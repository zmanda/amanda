/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amxfer.h"
#include "element-glue.h"
#include "amanda.h"

static GObjectClass *parent_class = NULL;

/*
 * Utility functions, etc.
 */

static void
make_pipe(
    XferElementGlue *self)
{
    if (pipe(self->pipe) < 0)
	g_critical(_("Could not create pipe: %s"), strerror(errno));
}

static void
send_xfer_done(
    XferElementGlue *self)
{
    xfer_queue_message(XFER_ELEMENT(self)->xfer,
	    xmsg_new((XferElement *)self, XMSG_DONE, 0));
}

#define GLUE_BUFFER_SIZE 32768
#define GLUE_RING_BUFFER_SIZE 32

/*
 * Worker threads
 *
 * At most one of these runs in a given instance, as selected in setup_impl
 */

static gpointer
call_and_write_thread(
    gpointer data)
{
    XferElement *elt = XFER_ELEMENT(data);
    XferElementGlue *self = XFER_ELEMENT_GLUE(data);
    int *fdp = (self->pipe[1] == -1)? &elt->downstream->input_fd : &self->pipe[1];
    int fd = *fdp;

    while (!elt->cancelled) {
	size_t len;
	char *buf;

	/* get a buffer from upstream */
	buf = xfer_element_pull_buffer(elt->upstream, &len);
	if (!buf)
	    break;

	/* write it */
	if (full_write(fd, buf, len) < len) {
	    xfer_element_handle_error(elt,
		_("Error writing to fd %d: %s"), fd, strerror(errno));
	    amfree(buf);
	    break;
	}

	amfree(buf);
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_by_pulling(elt->upstream);

    /* close the fd we've been writing, as an EOF signal to downstream, and
     * set it to -1 to avoid accidental re-use */
    close(fd);
    *fdp = -1;

    send_xfer_done(self);

    return NULL;
}

static gpointer
read_and_write_thread(
    gpointer data)
{
    XferElement *elt = XFER_ELEMENT(data);
    XferElementGlue *self = XFER_ELEMENT_GLUE(data);
    int rfd = elt->upstream->output_fd;
    int wfd = elt->downstream->input_fd;

    /* dynamically allocate a buffer, in case this thread has
     * a limited amount of stack allocated */
    char *buf = g_malloc(GLUE_BUFFER_SIZE);

    while (!elt->cancelled) {
	size_t len;

	/* read from upstream */
	len = full_read(rfd, buf, GLUE_BUFFER_SIZE);
	if (len < GLUE_BUFFER_SIZE) {
	    if (errno) {
		xfer_element_handle_error(elt,
		    _("Error reading from fd %d: %s"), rfd, strerror(errno));
		break;
	    } else if (len == 0) { /* we only count a zero-length read as EOF */
		break;
	    }
	}

	/* write the buffer fully */
	if (full_write(wfd, buf, len) < len) {
	    xfer_element_handle_error(elt,
		_("Could not write to fd %d: %s"), wfd, strerror(errno));
	    break;
	}
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_by_pulling(elt->upstream);

    /* close the read fd, if it's at EOF, and set it to -1 to avoid accidental
     * re-use */
    if (!elt->cancelled || elt->expect_eof) {
	close(rfd);
	elt->upstream->output_fd = -1;
    }

    /* close the fd we've been writing, as an EOF signal to downstream, and
     * set it to -1 to avoid accidental re-use */
    close(wfd);
    elt->downstream->input_fd = -1;

    send_xfer_done(self);

    amfree(buf);
    return NULL;
}

static gpointer
read_and_call_thread(
    gpointer data)
{
    XferElement *elt = XFER_ELEMENT(data);
    XferElementGlue *self = XFER_ELEMENT_GLUE(data);
    int *fdp = (self->pipe[0] == -1)? &elt->upstream->output_fd : &self->pipe[0];
    int fd = *fdp;

    while (!elt->cancelled) {
	char *buf = g_malloc(GLUE_BUFFER_SIZE);
	size_t len;

	/* read a buffer from upstream */
	len = full_read(fd, buf, GLUE_BUFFER_SIZE);
	if (len < GLUE_BUFFER_SIZE) {
	    if (errno) {
		xfer_element_handle_error(elt,
		    _("Error reading from fd %d: %s"), fd, strerror(errno));
		break;
	    } else if (len == 0) { /* we only count a zero-length read as EOF */
		amfree(buf);
		break;
	    }
	}

	xfer_element_push_buffer(elt->downstream, buf, len);
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_by_reading(fd);

    /* send an EOF indication downstream */
    xfer_element_push_buffer(elt->downstream, NULL, 0);

    /* close the read fd, since it's at EOF, and set it to -1 to avoid accidental
     * re-use */
    close(fd);
    *fdp = -1;

    send_xfer_done(self);

    return NULL;
}

static gpointer
call_and_call_thread(
    gpointer data)
{
    XferElement *elt = XFER_ELEMENT(data);
    XferElementGlue *self = XFER_ELEMENT_GLUE(data);
    gboolean eof_sent = FALSE;

    /* TODO: consider breaking this into two cooperating threads: one to pull
     * buffers from upstream and one to push them downstream.  This would gain
     * parallelism at the cost of a lot of synchronization operations. */

    while (!elt->cancelled) {
	char *buf;
	size_t len;

	/* get a buffer from upstream */
	buf = xfer_element_pull_buffer(elt->upstream, &len);

	/* and push it downstream */
	xfer_element_push_buffer(elt->downstream, buf, len);

	if (!buf) {
	    eof_sent = TRUE;
	    break;
	}
    }

    if (elt->cancelled && elt->expect_eof)
	xfer_element_drain_by_pulling(elt->upstream);

    if (!eof_sent)
	xfer_element_push_buffer(elt->downstream, NULL, 0);

    send_xfer_done(self);

    return NULL;
}

/*
 * Implementation
 */

static void
setup_impl(
    XferElement *elt)
{
    XferElementGlue *self = (XferElementGlue *)elt;

    switch (elt->input_mech) {
    case XFER_MECH_READFD:
	switch (elt->output_mech) {
	case XFER_MECH_READFD:
	    g_assert_not_reached(); /* no glue needed */
	    break;

	case XFER_MECH_WRITEFD:
	    self->threadfunc = read_and_write_thread;
	    break;

	case XFER_MECH_PUSH_BUFFER:
	    self->threadfunc = read_and_call_thread;
	    break;

	case XFER_MECH_PULL_BUFFER:
	    break;

	case XFER_MECH_NONE:
	default:
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_WRITEFD:
	make_pipe(self);
	elt->input_fd = self->pipe[1];
	self->pipe[1] = -1; /* upstream will close this for us */

	switch (elt->output_mech) {
	case XFER_MECH_READFD:
	    elt->output_fd = self->pipe[0];
	    self->pipe[0] = -1; /* downstream will close this for us */
	    break;

	case XFER_MECH_WRITEFD:
	    g_assert_not_reached(); /* no glue needed */
	    break;

	case XFER_MECH_PUSH_BUFFER:
	    self->threadfunc = read_and_call_thread;
	    break;

	case XFER_MECH_PULL_BUFFER:
	    break;

	case XFER_MECH_NONE:
	default:
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_PUSH_BUFFER:
	switch (elt->output_mech) {
	case XFER_MECH_READFD:
	    make_pipe(self);
	    elt->output_fd = self->pipe[0];
	    self->pipe[0] = -1; /* downstream will close this for us */
	    break;

	case XFER_MECH_WRITEFD:
	    break;

	case XFER_MECH_PUSH_BUFFER:
	    g_assert_not_reached(); /* no glue needed */
	    break;

	case XFER_MECH_PULL_BUFFER:
	    self->ring = g_malloc(sizeof(*self->ring) * GLUE_RING_BUFFER_SIZE);
	    self->ring_used_sem = semaphore_new_with_value(0);
	    self->ring_free_sem = semaphore_new_with_value(GLUE_RING_BUFFER_SIZE);
	    break;

	case XFER_MECH_NONE:
	default:
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_PULL_BUFFER:
	switch (elt->output_mech) {
	case XFER_MECH_READFD:
	    make_pipe(self);
	    elt->output_fd = self->pipe[0];
	    self->pipe[0] = -1; /* downstream will close this for us */
	    self->threadfunc = call_and_write_thread;
	    break;

	case XFER_MECH_WRITEFD:
	    self->threadfunc = call_and_write_thread;
	    break;

	case XFER_MECH_PUSH_BUFFER:
	    self->threadfunc = call_and_call_thread;
	    break;

	case XFER_MECH_PULL_BUFFER:
	    g_assert_not_reached(); /* no glue needed */
	    break;

	case XFER_MECH_NONE:
	default:
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_NONE:
    default:
	g_assert_not_reached();
	break;
    }
}

static gboolean
start_impl(
    XferElement *elt)
{
    XferElementGlue *self = (XferElementGlue *)elt;

    if (self->threadfunc) {
	self->thread = g_thread_create(self->threadfunc, (gpointer)self, FALSE, NULL);
    }

    /* we're active if we have a thread that will eventually die */
    return self->threadfunc? TRUE : FALSE;
}

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferElementGlue *self = XFER_ELEMENT_GLUE(elt);

    if (self->ring) {
	gpointer buf;

	if (elt->cancelled) {
	    /* The finalize method will empty the ring buffer */
	    *size = 0;
	    return NULL;
	}

	/* make sure there's at least one element available */
	semaphore_down(self->ring_used_sem);

	/* get it */
	buf = self->ring[self->ring_tail].buf;
	*size = self->ring[self->ring_tail].size;
	self->ring_tail = (self->ring_tail + 1) % GLUE_RING_BUFFER_SIZE;

	/* and mark this element as free to be overwritten */
	semaphore_up(self->ring_free_sem);

	return buf;
    } else {
	int *fdp = (self->pipe[0] == -1)? &elt->upstream->output_fd : &self->pipe[0];
	int fd = *fdp;
	char *buf = g_malloc(GLUE_BUFFER_SIZE);
	ssize_t len;

	if (elt->cancelled) {
	    if (elt->expect_eof)
		xfer_element_drain_by_reading(fd);

	    close(fd);
	    *fdp = -1;

	    *size = 0;
	    return NULL;
	}

	/* read from upstream */
	len = full_read(fd, buf, GLUE_BUFFER_SIZE);
	if (len < GLUE_BUFFER_SIZE) {
	    if (errno) {
		xfer_element_handle_error(elt,
		    _("Error reading from fd %d: %s"), fd, strerror(errno));

		/* return an EOF */
		amfree(buf);
		len = 0;

		/* and finish off the upstream */
		if (elt->expect_eof) {
		    xfer_element_drain_by_reading(fd);
		}
		close(fd);
		*fdp = -1;
	    } else if (len == 0) {
		/* EOF */
		g_free(buf);
		buf = NULL;
		*size = 0;

		/* signal EOF to downstream */
		close(fd);
		*fdp = -1;
	    }
	}

	*size = (size_t)len;
	return buf;
    }
}

static void
push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t len)
{
    XferElementGlue *self = (XferElementGlue *)elt;

    if (self->ring) {
	/* just drop packets if the transfer has been cancelled */
	if (elt->cancelled) {
	    amfree(buf);
	    return;
	}

	/* make sure there's at least one element free */
	semaphore_down(self->ring_free_sem);

	/* set it */
	self->ring[self->ring_head].buf = buf;
	self->ring[self->ring_head].size = len;
	self->ring_head = (self->ring_head + 1) % GLUE_RING_BUFFER_SIZE;

	/* and mark this element as available for reading */
	semaphore_up(self->ring_used_sem);

	return;
    } else {
	int *fdp = (self->pipe[1] == -1)? &elt->downstream->input_fd : &self->pipe[1];
	int fd = *fdp;

	if (elt->cancelled) {
	    if (!elt->expect_eof || !buf) {
		close(fd);
		*fdp = -1;

		/* hack to ensure we won't close the fd again, if we get another push */
		elt->expect_eof = TRUE;
	    }

	    amfree(buf);

	    return;
	}

	/* write the full buffer to the fd, or close on EOF */
	if (buf) {
	    if (full_write(fd, buf, len) < len) {
		xfer_element_handle_error(elt,
		    _("Error writing to fd %d: %s"), fd, strerror(errno));
		/* nothing special to do to handle the cancellation */
	    }
	    amfree(buf);
	} else {
	    close(fd);
	    *fdp = -1;
	}

	return;
    }
}

static void
instance_init(
    XferElementGlue *self)
{
    XferElement *elt = (XferElement *)self;
    elt->can_generate_eof = TRUE;
    self->pipe[0] = self->pipe[1] = -1;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferElementGlue *self = XFER_ELEMENT_GLUE(obj_self);

    /* close our pipes if they're still open (they shouldn't be!) */
    if (self->pipe[0] != -1) close(self->pipe[0]);
    if (self->pipe[1] != -1) close(self->pipe[1]);

    if (self->ring) {
	/* empty the ring buffer, ignoring syncronization issues */
	while (self->ring_used_sem->value) {
	    if (self->ring[self->ring_tail].buf)
		amfree(self->ring[self->ring_tail].buf);
	    self->ring_tail = (self->ring_tail + 1) % GLUE_RING_BUFFER_SIZE;
	}

	amfree(self->ring);
	semaphore_free(self->ring_used_sem);
	semaphore_free(self->ring_free_sem);
    }

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static xfer_element_mech_pair_t _pairs[] = {
    { XFER_MECH_READFD, XFER_MECH_WRITEFD, 2, 1 }, /* splice or copy */
    { XFER_MECH_READFD, XFER_MECH_PUSH_BUFFER, 1, 1 }, /* read and call */
    { XFER_MECH_READFD, XFER_MECH_PULL_BUFFER, 1, 0 }, /* read on demand */

    { XFER_MECH_WRITEFD, XFER_MECH_READFD, 0, 0 }, /* pipe */
    { XFER_MECH_WRITEFD, XFER_MECH_PUSH_BUFFER, 1, 1 }, /* pipe + read and call*/
    { XFER_MECH_WRITEFD, XFER_MECH_PULL_BUFFER, 1, 0 }, /* pipe + read on demand */

    { XFER_MECH_PUSH_BUFFER, XFER_MECH_READFD, 1, 0 }, /* write on demand + pipe */
    { XFER_MECH_PUSH_BUFFER, XFER_MECH_WRITEFD, 1, 0 }, /* write on demand */
    { XFER_MECH_PUSH_BUFFER, XFER_MECH_PULL_BUFFER, 0, 0 }, /* async queue */

    { XFER_MECH_PULL_BUFFER, XFER_MECH_READFD, 1, 1 }, /* call and write + pipe */
    { XFER_MECH_PULL_BUFFER, XFER_MECH_WRITEFD, 1, 1 }, /* call and write */
    { XFER_MECH_PULL_BUFFER, XFER_MECH_PUSH_BUFFER, 0, 1 }, /* call and call */

    /* terminator */
    { XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
};
xfer_element_mech_pair_t *xfer_element_glue_mech_pairs = _pairs;

static void
class_init(
    XferElementGlueClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);

    klass->setup = setup_impl;
    klass->start = start_impl;
    klass->push_buffer = push_buffer_impl;
    klass->pull_buffer = pull_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Element::Glue";
    klass->mech_pairs = xfer_element_glue_mech_pairs;

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_element_glue_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferElementGlueClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferElementGlue),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferElementGlue", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_element_glue(void)
{
    XferElementGlue *self = (XferElementGlue *)g_object_new(XFER_ELEMENT_GLUE_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    return elt;
}
