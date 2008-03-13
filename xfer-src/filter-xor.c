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

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_filter_xor() references
 * it directly.
 */

GType xfer_filter_xor_get_type(void);
#define XFER_FILTER_XOR_TYPE (xfer_filter_xor_get_type())
#define XFER_FILTER_XOR(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_xor_get_type(), XferFilterXor)
#define XFER_FILTER_XOR_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_xor_get_type(), XferFilterXor const)
#define XFER_FILTER_XOR_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_filter_xor_get_type(), XferFilterXorClass)
#define IS_XFER_FILTER_XOR(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_filter_xor_get_type ())
#define XFER_FILTER_XOR_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_filter_xor_get_type(), XferFilterXorClass)

/*
 * Main object structure
 */

typedef struct XferFilterXor {
    XferFilter __parent__;

    char xor_key;

    /* ordinarily an element wouldn't make its own pipes, but we want to support
     * all mechanisms for testing purposes */
    int input_pipe[2];
    int output_pipe[2];
} XferFilterXor;

/*
 * Class definition
 */

typedef struct {
    XferFilterClass __parent__;
} XferFilterXorClass;

/*
 * The actual data destination
 */

static gpointer
filter_data_thread(
    gpointer data)
{
    char buf[256];
    char *p;
    XMsg *msg;

    XferFilterXor *xs = (XferFilterXor *)data;
    int input_fd = xs->input_pipe[0];
    int output_fd = xs->output_pipe[1];
    char xor_key = xs->xor_key;

    while (1) {
	ssize_t len, written;
	ssize_t i;

	/* try to read some data */
	if ((len = read(input_fd, buf, sizeof(buf))) < 0) {
	    error("xor filter: error in read(): %s", strerror(errno));
	} else if (len == 0) {
	    break;
	}

	/* Apply XOR.  This is a pretty sophisticated encryption algorithm! */
	for (i = 0; i < len; i++) {
	    buf[i] ^= xor_key;
	}

	/* now we have to be sure we write *all* of that data */
	p = buf;
	while (len) {
	    if ((written = write(output_fd, p, len)) < 0) {
		error("xor filter: error in write(): %s", strerror(errno));
	    }
	    p += written;
	    len -= written;
	}
    }

    /* close our fd's */
    close(xs->input_pipe[0]);
    xs->input_pipe[0] = -1;
    close(xs->output_pipe[1]);
    xs->output_pipe[1] = -1;

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
    XferFilterXor *xs = (XferFilterXor *)elt;
    GThread *th;

    /* we'd better have our fd's */
    g_assert(xs->input_pipe[0] != -1);
    g_assert(xs->output_pipe[1] != -1);

    th = g_thread_create(filter_data_thread, (gpointer)xs, FALSE, NULL);
}

static void
abort_impl(
    XferElement *elt G_GNUC_UNUSED)
{
    /* TODO */
}

static void
setup_input_impl(
    XferElement *elt,
    xfer_input_mech mech,
    int *fdp)
{
    XferFilterXor *xfx = (XferFilterXor *)elt;

    switch (mech) {
	case MECH_INPUT_READ_GIVEN_FD:
	    /* we've got an fd, so tuck it away; we won't need to close
	     * anything later. */
	    xfx->input_pipe[0] = *fdp;
	    xfx->input_pipe[1] = -1; /* write end of the pipe isn't our problem */
	    break;

	case MECH_INPUT_HAVE_WRITE_FD:
	    if (pipe(xfx->input_pipe) != 0) {
		error("could not create input_pipe: %s", strerror(errno));
		/* NOTREACHED */
	    }
	    *fdp = xfx->input_pipe[1];
	    break;

	default:
	    g_assert_not_reached();
    }
}

static void
setup_output_impl(
    XferElement *elt,
    xfer_output_mech mech,
    int *fdp)
{
    XferFilterXor *xfx = (XferFilterXor *)elt;

    switch (mech) {
	case MECH_OUTPUT_WRITE_GIVEN_FD:
	    /* we've got an fd, so tuck it away; we won't need to close
	     * anything later. */
	    xfx->output_pipe[0] = -1; /* read end of the pipe isn't our problem */
	    xfx->output_pipe[1] = *fdp;
	    break;

	case MECH_OUTPUT_HAVE_READ_FD:
	    if (pipe(xfx->output_pipe) != 0) {
		error("could not create output_pipe: %s", strerror(errno));
		/* NOTREACHED */
	    }
	    *fdp = xfx->output_pipe[0];
	    break;

	default:
	    g_assert_not_reached();
    }
}

static void
instance_init(
    XferFilterXor *xfx)
{
    XFER_ELEMENT(xfx)->input_mech =
	  MECH_INPUT_READ_GIVEN_FD
	| MECH_INPUT_HAVE_WRITE_FD;
    XFER_ELEMENT(xfx)->output_mech =
	  MECH_OUTPUT_WRITE_GIVEN_FD
	| MECH_OUTPUT_HAVE_READ_FD;

    xfx->input_pipe[0] = xfx->input_pipe[1] = -1;
    xfx->output_pipe[0] = xfx->output_pipe[1] = -1;
}

static void
finalize_impl(
    GObject * obj_self)
{
    GObjectClass *goc;
    XferFilterXor *xfx = XFER_FILTER_XOR(obj_self);

    /* close our pipes */
    if (xfx->input_pipe[0] != -1) close(xfx->input_pipe[0]);
    if (xfx->input_pipe[1] != -1) close(xfx->input_pipe[1]);
    if (xfx->output_pipe[0] != -1) close(xfx->output_pipe[0]);
    if (xfx->output_pipe[1] != -1) close(xfx->output_pipe[1]);

    /* kill and wait for our child, if it's still around */
    /* TODO */

    /* chain up */
    goc = G_OBJECT_CLASS(g_type_class_peek(G_TYPE_OBJECT));
    if (goc->finalize) goc->finalize(obj_self);
}

static void
class_init(
    XferFilterXorClass * xfxc)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(xfxc);
    GObjectClass *goc = G_OBJECT_CLASS(xfxc);

    xec->start = start_impl;
    xec->abort = abort_impl;
    xec->setup_input = setup_input_impl;
    xec->setup_output = setup_output_impl;

    goc->finalize = finalize_impl;
}

GType
xfer_filter_xor_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferFilterXorClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferFilterXor),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_FILTER_TYPE, "XferFilterXor", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_filter_xor(
    xfer_input_mech input_mechanisms,
    xfer_output_mech output_mechanisms,
    char xor_key)
{
    XferFilterXor *xfx = (XferFilterXor *)g_object_new(XFER_FILTER_XOR_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xfx);

    elt->input_mech = input_mechanisms;
    elt->output_mech = output_mechanisms;

    xfx->xor_key = xor_key;

    return elt;
}
