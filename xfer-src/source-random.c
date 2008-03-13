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

/*
 * Main object structure
 */

typedef struct XferSourceRandom {
    XferSource __parent__;

    size_t length;
    gboolean text_only;

    /* ordinarily an element wouldn't make its own pipe, but we want to support
     * all mechanisms for testing purposes */
    int pipe[2];
} XferSourceRandom;

/*
 * Class definition
 */

typedef struct {
    XferSourceClass __parent__;
} XferSourceRandomClass;

/*
 * The actual data source, as a GThreadFunc
 */
static gpointer
random_write_thread(
    gpointer data)
{
    char buf[10240];
    size_t buf_used = 10240;
    XMsg *msg;

    XferSourceRandom *xs = (XferSourceRandom *)data;
    size_t length = xs->length;
    gboolean text_only = xs->text_only;
    int fd = xs->pipe[1];

    while (length) {
	size_t i;
	ssize_t written;

	/* fill in some new random data.  This is less than "efficient". */
	if (text_only) {
	    /* printable randomness */
	    for (i = 0; i < buf_used; i++) {
		buf[i] = 'a' + (char)(random() % 26);
	    }
	} else {
	    /* truly random binary data */
	    for (i = 0; i < buf_used; i++) {
		buf[i] = (char)random();
	    }
	}

	/* try to write some data */
	if ((written = write(fd, buf, min(sizeof(buf), length))) < 0) {
	    error("error in write(): %s", strerror(errno));
	}

	length -= written;
	buf_used = written;
    }

    /* close the write side of the pipe to propagate the EOF */
    close(xs->pipe[1]);
    xs->pipe[1] = -1;

    /* and send an XMSG_DONE */
    msg = xmsg_new((XferElement *)xs, XMSG_DONE, 0);
    xfer_queue_message(XFER_ELEMENT(xs)->xfer, msg);

    return NULL;
}

/*
 * Implementation
 */

static void
start_impl(
    XferElement *elt)
{
    XferSourceRandom *xs = (XferSourceRandom *)elt;
    GThread *th;

    /* we'd better have a fd to write to. */
    g_assert(xs->pipe[1] != -1);

    th = g_thread_create(random_write_thread, (gpointer)xs, FALSE, NULL);
}

static void
abort_impl(
    XferElement *elt G_GNUC_UNUSED)
{
    /* TODO: set a 'prolong' flag to FALSE */
}

static void
setup_output_impl(
    XferElement *elt,
    xfer_output_mech mech,
    int *fdp)
{
    XferSourceRandom *xsr = (XferSourceRandom *)elt;

    switch (mech) {
	case MECH_OUTPUT_WRITE_GIVEN_FD:
	    /* we've got an fd, so tuck it away; we won't need to close
	     * anything later. */
	    xsr->pipe[0] = -1; /* read end of the pipe isn't our problem */
	    xsr->pipe[1] = *fdp;
	    break;

	case MECH_OUTPUT_HAVE_READ_FD:
	    if (pipe(xsr->pipe) != 0) {
		error("could not create pipe: %s", strerror(errno));
		/* NOTREACHED */
	    }
	    *fdp = xsr->pipe[0];
	    break;

	default:
	    g_assert_not_reached();
    }
}

static void
instance_init(
    XferSourceRandom *xsr)
{
    XFER_ELEMENT(xsr)->input_mech = 0;
    XFER_ELEMENT(xsr)->output_mech =
	  MECH_OUTPUT_WRITE_GIVEN_FD
	| MECH_OUTPUT_HAVE_READ_FD;

    xsr->pipe[0] = xsr->pipe[1] = -1;
}

static void
finalize_impl(
    GObject * obj_self)
{
    GObjectClass *goc;
    XferSourceRandom *xsr = XFER_SOURCE_RANDOM(obj_self);

    /* close our pipes */
    if (xsr->pipe[0] != -1) close(xsr->pipe[0]);
    if (xsr->pipe[1] != -1) close(xsr->pipe[1]);

    /* kill and wait for our thread, if it's still around */
    /* TODO */

    /* chain up */
    goc = G_OBJECT_CLASS(g_type_class_peek(G_TYPE_OBJECT));
    if (goc->finalize) goc->finalize(obj_self);
}

static void
class_init(
    XferSourceRandomClass * xsrc)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(xsrc);
    GObjectClass *goc = G_OBJECT_CLASS(xsrc);

    xec->start = start_impl;
    xec->abort = abort_impl;
    xec->setup_output = setup_output_impl;

    goc->finalize = finalize_impl;
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

        type = g_type_register_static (XFER_SOURCE_TYPE, "XferSourceRandom", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_random(
    size_t length, 
    gboolean text_only,
    xfer_output_mech mechanisms)
{
    XferSourceRandom *xsr = (XferSourceRandom *)g_object_new(XFER_SOURCE_RANDOM_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xsr);

    xsr->length = length;
    xsr->text_only = text_only;
    elt->output_mech = mechanisms;

    return elt;
}
