/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
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

static void
kill_and_wait_for_thread(
    XferElementGlue *self)
{
    if (self->thread) {
	self->prolong = FALSE;
	g_thread_join(self->thread);
	self->thread = NULL;
    }
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
    XferElementGlue *self = (XferElementGlue *)data;
    int *fdp = (self->pipe[1] == -1)? &XFER_ELEMENT(self)->downstream->input_fd : &self->pipe[1];
    int fd = *fdp;

    while (self->prolong) {
	size_t len;
	char *buf;

	/* get a buffer from upstream */
	buf = xfer_element_pull_buffer(XFER_ELEMENT(self)->upstream, &len);
	if (!buf)
	    break;

	/* write it */
	if (full_write(fd, buf, len) < len) {
	    g_critical(_("Could not write to fd %d: %s"), fd, strerror(errno));
	}
    }

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
    XferElementGlue *self = (XferElementGlue *)data;
    int rfd = XFER_ELEMENT(self)->upstream->output_fd;
    int wfd = XFER_ELEMENT(self)->downstream->input_fd;

    /* dynamically allocate a buffer, in case this thread has
     * a limited amount of stack allocated */
    char *buf = g_malloc(GLUE_BUFFER_SIZE);

    while (self->prolong) {
	size_t len;

	/* read from upstream */
	len = full_read(rfd, buf, GLUE_BUFFER_SIZE);
	if (len < GLUE_BUFFER_SIZE) {
	    if (errno) {
		g_critical(_("Error reading from fd %d: %s"), rfd, strerror(errno));
	    } else if (len == 0) { /* we only count a zero-length read as EOF */
		break;
	    }
	}

	/* write the buffer fully */
	if (full_write(wfd, buf, len) < len)
	    g_critical(_("Could not write to fd %d: %s"), wfd, strerror(errno));
    }

    /* close the read fd, since it's at EOF, and set it to -1 to avoid accidental
     * re-use */
    close(rfd);
    XFER_ELEMENT(self)->upstream->output_fd = -1;

    /* close the fd we've been writing, as an EOF signal to downstream, and
     * set it to -1 to avoid accidental re-use */
    close(wfd);
    XFER_ELEMENT(self)->downstream->input_fd = -1;

    send_xfer_done(self);

    amfree(buf);
    return NULL;
}

static gpointer
read_and_call_thread(
    gpointer data)
{
    XferElementGlue *self = (XferElementGlue *)data;
    int *fdp = (self->pipe[0] == -1)? &XFER_ELEMENT(self)->upstream->output_fd : &self->pipe[0];
    int fd = *fdp;

    while (self->prolong) {
	char *buf = g_malloc(GLUE_BUFFER_SIZE);
	size_t len;

	/* read a buffer from upstream */
	len = full_read(fd, buf, GLUE_BUFFER_SIZE);
	if (len < GLUE_BUFFER_SIZE) {
	    if (errno) {
		g_critical(_("Error reading from fd %d: %s"), fd, strerror(errno));
	    } else if (len == 0) { /* we only count a zero-length read as EOF */
		amfree(buf);
		break;
	    }
	}

	xfer_element_push_buffer(XFER_ELEMENT(self)->downstream, buf, len);
    }

    /* send an EOF indication downstream */
    xfer_element_push_buffer(XFER_ELEMENT(self)->downstream, NULL, 0);

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
    XferElementGlue *self = (XferElementGlue *)data;

    /* TODO: consider breaking this into two cooperating threads: one to pull
     * buffers from upstream and one to push them downstream.  This would gain
     * parallelism at the cost of a lot of synchronization operations. */

    while (self->prolong) {
	char *buf;
	size_t len;

	/* get a buffer from upstream */
	buf = xfer_element_pull_buffer(XFER_ELEMENT(self)->upstream, &len);

	/* and push it downstream */
	xfer_element_push_buffer(XFER_ELEMENT(self)->downstream, buf, len);

	if (!buf) break;
    }

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

    switch (XFER_ELEMENT(self)->input_mech) {
    case XFER_MECH_READFD:
	switch (XFER_ELEMENT(self)->output_mech) {
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
	    /* nothing to set up */
	    break;

	case XFER_MECH_NONE:
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_WRITEFD:
	make_pipe(self);
	XFER_ELEMENT(self)->input_fd = self->pipe[1];
	self->pipe[1] = -1; /* upstream will close this for us */

	switch (XFER_ELEMENT(self)->output_mech) {
	case XFER_MECH_READFD:
	    XFER_ELEMENT(self)->output_fd = self->pipe[0];
	    self->pipe[0] = -1; /* downstream will close this for us */
	    break;

	case XFER_MECH_WRITEFD:
	    g_assert_not_reached(); /* no glue needed */
	    break;

	case XFER_MECH_PUSH_BUFFER:
	    self->threadfunc = read_and_call_thread;
	    break;

	case XFER_MECH_PULL_BUFFER:
	    /* nothing to set up */
	    break;

	case XFER_MECH_NONE:
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_PUSH_BUFFER:
	switch (XFER_ELEMENT(self)->output_mech) {
	case XFER_MECH_READFD:
	    make_pipe(self);
	    XFER_ELEMENT(self)->output_fd = self->pipe[0];
	    self->pipe[0] = -1; /* downstream will close this for us */
	    break;

	case XFER_MECH_WRITEFD:
	    /* nothing to set up */
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
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_PULL_BUFFER:
	switch (XFER_ELEMENT(self)->output_mech) {
	case XFER_MECH_READFD:
	    make_pipe(self);
	    XFER_ELEMENT(self)->output_fd = self->pipe[0];
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
	    g_assert_not_reached();
	    break;
	}
	break;

    case XFER_MECH_NONE:
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
	self->prolong = TRUE;
	self->thread = g_thread_create(self->threadfunc, (gpointer)self, TRUE, NULL);
    }

    /* we're active if we have a thread that will eventually die */
    return self->threadfunc? TRUE : FALSE;
}

static void
abort_impl(
    XferElement *elt)
{
    XferElementGlue *self = (XferElementGlue *)elt;

    /* kill and wait for our child thread, if it's still around */
    kill_and_wait_for_thread(self);
}

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferElementGlue *self = (XferElementGlue *)elt;

    if (self->ring) {
	gpointer buf;

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
	int *fdp = (self->pipe[0] == -1)? &XFER_ELEMENT(self)->upstream->output_fd : &self->pipe[0];
	int fd = *fdp;
	char *buf = g_malloc(GLUE_BUFFER_SIZE);
	ssize_t len;

	/* read from upstream */
	len = full_read(fd, buf, GLUE_BUFFER_SIZE);
	if (len < GLUE_BUFFER_SIZE) {
	    if (errno) {
		g_critical(_("Error reading from fd %d: %s"), fd, strerror(errno));
	    } else if (len == 0) {
		/* EOF */
		g_free(buf);
		buf = NULL;

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
	int *fdp = (self->pipe[1] == -1)? &XFER_ELEMENT(self)->downstream->input_fd : &self->pipe[1];
	int fd = *fdp;

	/* write the full buffer to the fd, or close on EOF */
	if (buf) {
	    if (full_write(fd, buf, len) < len) {
		g_critical(_("Could not write to fd %d: %s"), fd, strerror(errno));
	    }
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

    /* kill and wait for our child thread, if it's still around */
    kill_and_wait_for_thread(self);

    if (self->ring) {
	/* empty the ring buffer, ignoring syncronization issues */
	while (self->ring_used_sem->value) {
	    if (self->ring[self->ring_tail].buf)
		amfree(self->ring[self->ring_tail].buf);
	    self->ring_tail = (self->ring_tail + 1) % GLUE_RING_BUFFER_SIZE;
	    semaphore_down(self->ring_used_sem);
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
    klass->abort = abort_impl;
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
