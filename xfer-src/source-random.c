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
#include "amanda.h"

#if USE_RAND
/* If the C library does not define random(), try to use rand() by
   defining USE_RAND, but then make sure you are not using hardware
   compression, because the low-order bits of rand() may not be that
   random... :-( */
#define random() rand()
#define srandom(seed) srand(seed)
#endif

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_random() references
 * it directly.
 */

GType xfer_source_random_get_type(void);
#define XFER_SOURCE_RANDOM_TYPE (xfer_source_random_get_type())
#define XFER_SOURCE_RANDOM(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_random_get_type(), XferSourceRandom)
#define XFER_SOURCE_RANDOM_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_random_get_type(), XferSourceRandom const)
#define XFER_SOURCE_RANDOM_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_random_get_type(), XferSourceRandomClass)
#define IS_XFER_SOURCE_RANDOM(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_random_get_type ())
#define XFER_SOURCE_RANDOM_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_random_get_type(), XferSourceRandomClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceRandom {
    XferElement __parent__;

    GThread *thread;
    gboolean prolong;

    size_t length;
    gboolean text_only;
} XferSourceRandom;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceRandomClass;

/*
 * Utilities
 */

static void
kill_and_wait_for_thread(
    XferSourceRandom *self)
{
    if (self->thread) {
	self->prolong = FALSE;
	g_thread_join(self->thread);
	self->thread = NULL;
    }
}

static void
write_gibberish(
    char *buf,
    size_t len,
    gboolean text_only)
{
    size_t i;

    /* fill in some new random data.  This is less than "efficient". */
    if (text_only) {
	/* printable randomness */
	for (i = 0; i < len; i++) {
	    buf[i] = 'a' + (char)(random() % 26);
	}
    } else {
	/* truly random binary data */
	for (i = 0; i < len; i++) {
	    buf[i] = (char)random();
	}
    }
}

/*
 * The actual data source, as a GThreadFunc, used for XFER_MECH_WRITEFD output
 */

static gpointer
random_write_thread(
    gpointer data)
{
    XferSourceRandom *self = (XferSourceRandom *)data;
    char buf[10240];
    size_t buf_used = 10240;
    XMsg *msg;
    size_t length = self->length;
    gboolean text_only = self->text_only;
    int fd = XFER_ELEMENT(self)->downstream->input_fd;

    while (self->prolong && length) {
	ssize_t written;

	/* overwrite any portion of the buffer that has already been written */
	write_gibberish(buf, buf_used, text_only);

	/* and try to write it to the fd */
	if ((written = write(fd, buf, min(sizeof(buf), length))) < 0) {
	    error("error in write(): %s", strerror(errno));
	}

	length -= written;
	buf_used = written;
    }

    /* close the write side of the pipe to propagate the EOF */
    close(fd);
    XFER_ELEMENT(self)->downstream->input_fd = -1;

    /* and send an XMSG_DONE */
    msg = xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0);
    xfer_queue_message(XFER_ELEMENT(self)->xfer, msg);

    return NULL;
}

/*
 * Implementation
 */

static gboolean
start_impl(
    XferElement *elt)
{
    XferSourceRandom *self = (XferSourceRandom *)elt;

    if (XFER_ELEMENT(self)->output_mech == XFER_MECH_WRITEFD) {
	self->prolong = TRUE;
	self->thread = g_thread_create(random_write_thread, (gpointer)self, TRUE, NULL);
	return TRUE;
    } else {
	/* we'll generate random data on demand */
	return FALSE;
    }
}

static void
abort_impl(
    XferElement *elt)
{
    XferSourceRandom *self = (XferSourceRandom *)elt;

    kill_and_wait_for_thread(self);
}

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourceRandom *self = (XferSourceRandom *)elt;
    char *buf;

    if (self->length == 0) {
	*size = 0;
	return NULL;
    }

    *size = MIN(10240, self->length);
    buf = g_malloc(*size);
    write_gibberish(buf, *size, self->text_only);

    self->length -= *size;

    return buf;
}

static void
instance_init(
    XferSourceRandom *self)
{
    self->thread = NULL;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferSourceRandom *self = XFER_SOURCE_RANDOM(obj_self);

    /* kill and wait for our thread, if it's still around */
    kill_and_wait_for_thread(self);

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferSourceRandomClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    GObjectClass *goc = G_OBJECT_CLASS(klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_WRITEFD, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, 1, 0},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    xec->start = start_impl;
    xec->abort = abort_impl;
    xec->pull_buffer = pull_buffer_impl;

    xec->perl_class = "Amanda::Xfer::Source::Random";
    xec->mech_pairs = mech_pairs;

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(klass);
}

GType
xfer_source_random_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceRandomClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceRandom),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceRandom", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_random(
    size_t length, 
    gboolean text_only)
{
    XferSourceRandom *xsr = (XferSourceRandom *)g_object_new(XFER_SOURCE_RANDOM_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xsr);

    xsr->length = length;
    xsr->text_only = text_only;

    return elt;
}
