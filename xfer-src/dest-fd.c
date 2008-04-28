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
 * This declaration is entirely private; nothing but xfer_dest_fd() references
 * it directly.
 */

GType xfer_dest_fd_get_type(void);
#define XFER_DEST_FD_TYPE (xfer_dest_fd_get_type())
#define XFER_DEST_FD(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_fd_get_type(), XferDestFd)
#define XFER_DEST_FD_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_fd_get_type(), XferDestFd const)
#define XFER_DEST_FD_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_fd_get_type(), XferDestFdClass)
#define IS_XFER_DEST_FD(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_fd_get_type ())
#define XFER_DEST_FD_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_fd_get_type(), XferDestFdClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferDestFd {
    XferElement __parent__;

    int fd;
} XferDestFd;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferDestFdClass;

/*
 * Implementation
 */

static void
setup_input_impl(
    XferElement *elt,
    xfer_input_mech mech,
    int *fdp)
{
    XferDestFd *self = (XferDestFd *)elt;

    switch (mech) {
	case MECH_INPUT_HAVE_WRITE_FD:
	    *fdp = self->fd;
	    break;

	default:
	    g_assert_not_reached();
    }
}

static void
instance_init(
    XferDestFd *self)
{
    XFER_ELEMENT(self)->input_mech = MECH_INPUT_HAVE_WRITE_FD;

    self->fd = -1;
}

static void
class_init(
    XferDestFdClass * klass)
{
    XferElementClass *xec = XFER_ELEMENT_CLASS(klass);

    xec->setup_input = setup_input_impl;

    xec->perl_class = "Amanda::Xfer::Dest::Fd";

    parent_class = g_type_class_peek_parent(klass);
}

GType
xfer_dest_fd_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestFdClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestFd),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestFd", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_dest_fd(
    int fd)
{
    XferDestFd *self = (XferDestFd *)g_object_new(XFER_DEST_FD_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    self->fd = fd;

    return elt;
}
