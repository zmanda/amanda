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
 * This declaration is entirely private; nothing but xfer_dest_null() references
 * it directly.
 */

GType xfer_dest_null_get_type(void);
#define XFER_DEST_NULL_TYPE (xfer_dest_null_get_type())
#define XFER_DEST_NULL(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_null_get_type(), XferDestNull)
#define XFER_DEST_NULL_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_null_get_type(), XferDestNull const)
#define XFER_DEST_NULL_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_null_get_type(), XferDestNullClass)
#define IS_XFER_DEST_NULL(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_null_get_type ())
#define XFER_DEST_NULL_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_null_get_type(), XferDestNullClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferDestNull {
    XferElement __parent__;

    gboolean debug_print;

    /* ordinarily an element wouldn't make its own pipe, but we want to support
     * all mechanisms for testing purposes */
    int pipe[2];
} XferDestNull;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferDestNullClass;

/*
 * The actual data destination
 */

static gpointer
read_data_thread(gpointer data)
{
    char buf[70];
    XMsg *msg;

    XferDestNull *xs = (XferDestNull *)data;
    int fd = xs->pipe[0];
    gboolean debug_print = xs->debug_print;

    while (1) {
	ssize_t len;

	/* try to read some data */
	if ((len = read(fd, buf, sizeof(buf))) < 0) {
	    error("error in read(): %s", strerror(errno));
	} else if (len == 0) {
	    break;
	}

	if (debug_print) {
	    ssize_t i;
	    g_printf("data: ");
	    for (i = 0; i < len; i++) {
		if (isprint((int)buf[i]))
		    g_printf("%c", buf[i]);
		else
		    g_printf("\\x%02x", (int)buf[i]);
	    }
	    g_printf("\n");
	}
    }

    /* send an XMSG_DONE */
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
    XferDestNull *self = (XferDestNull *)elt;
    GThread *th;
    XMsg *msg;

    /* we'd better have a fd to read from. */
    g_assert(self->pipe[0] != -1);

    xfer_will_send_xmsg_done(XFER_ELEMENT(self)->xfer);
    th = g_thread_create(read_data_thread, (gpointer)self, FALSE, NULL);

    /* send a superfluous message (this is a testing XferElement,
     * after all) */
    msg = xmsg_new((XferElement *)self, XMSG_INFO, 0);
    msg->message = stralloc("Is this thing on?");
    xfer_queue_message(XFER_ELEMENT(self)->xfer, msg);
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
    XferDestNull *xdn = (XferDestNull *)elt;

    switch (mech) {
	case MECH_INPUT_READ_GIVEN_FD:
	    /* we've got an fd, so tuck it away; we won't need to close
	     * anything later. */
	    xdn->pipe[0] = *fdp;
	    xdn->pipe[1] = -1; /* write end of the pipe isn't our problem */
	    break;

	case MECH_INPUT_HAVE_WRITE_FD:
	    if (pipe(xdn->pipe) != 0) {
		error("could not create pipe: %s", strerror(errno));
		/* NOTREACHED */
	    }
	    *fdp = xdn->pipe[1];
	    break;

	default:
	    g_assert_not_reached();
    }
}

static void
instance_init(
    XferDestNull *xdn)
{
    XFER_ELEMENT(xdn)->input_mech =
	  MECH_INPUT_READ_GIVEN_FD
	| MECH_INPUT_HAVE_WRITE_FD;
    XFER_ELEMENT(xdn)->output_mech = 0;

    xdn->pipe[0] = xdn->pipe[1] = -1;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferDestNull *xdn = XFER_DEST_NULL(obj_self);

    /* close our pipes */
    if (xdn->pipe[0] != -1) close(xdn->pipe[0]);
    if (xdn->pipe[1] != -1) close(xdn->pipe[1]);

    /* kill and wait for our child, if it's still around */
    /* TODO */

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferDestNullClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);
    GObjectClass *goc = G_OBJECT_CLASS(klass);

    xec->start = start_impl;
    xec->abort = abort_impl;
    xec->setup_input = setup_input_impl;

    xec->perl_class = "Amanda::Xfer::Dest::Null";

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(klass);
}

GType
xfer_dest_null_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestNullClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestNull),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestNull", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_dest_null(
    gboolean debug_print,
    xfer_input_mech mechanisms)
{
    XferDestNull *xdn = (XferDestNull *)g_object_new(XFER_DEST_NULL_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xdn);

    /* mechansims == 0 means 'default' */
    if (mechanisms)
	elt->input_mech = mechanisms;

    xdn->debug_print = debug_print;

    return elt;
}
