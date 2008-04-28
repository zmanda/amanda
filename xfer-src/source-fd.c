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
 * This declaration is entirely private; nothing but xfer_source_fd() references
 * it directly.
 */

GType xfer_source_fd_get_type(void);
#define XFER_SOURCE_FD_TYPE (xfer_source_fd_get_type())
#define XFER_SOURCE_FD(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_fd_get_type(), XferSourceFd)
#define XFER_SOURCE_FD_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_fd_get_type(), XferSourceFd const)
#define XFER_SOURCE_FD_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_fd_get_type(), XferSourceFdClass)
#define IS_XFER_SOURCE_FD(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_fd_get_type ())
#define XFER_SOURCE_FD_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_fd_get_type(), XferSourceFdClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceFd {
    XferElement __parent__;

    int fd;
} XferSourceFd;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceFdClass;

/*
 * Implementation
 */

static void
setup_output_impl(
    XferElement *elt,
    xfer_output_mech mech,
    int *fdp)
{
    XferSourceFd *self = (XferSourceFd *)elt;

    switch (mech) {
	case MECH_OUTPUT_HAVE_READ_FD:
	    *fdp = self->fd;
	    break;

	default:
	    g_assert_not_reached();
    }
}

static void
instance_init(
    XferSourceFd *self)
{
    XFER_ELEMENT(self)->output_mech = MECH_OUTPUT_HAVE_READ_FD;
    self->fd = -1;
}

static void
class_init(
    XferSourceFdClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);

    klass->setup_output = setup_output_impl;

    klass->perl_class = "Amanda::Xfer::Source::Fd";

    parent_class = g_type_class_peek_parent(klass);
}

GType
xfer_source_fd_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceFdClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceFd),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceFd", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_fd(
    int fd)
{
    XferSourceFd *self = (XferSourceFd *)g_object_new(XFER_SOURCE_FD_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    self->fd = fd;

    return elt;
}
