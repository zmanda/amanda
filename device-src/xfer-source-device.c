/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

#include "amanda.h"
#include "amxfer.h"
#include "device.h"
#include "property.h"
#include "xfer-device.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_device() references
 * it directly.
 */

GType xfer_source_device_get_type(void);
#define XFER_SOURCE_DEVICE_TYPE (xfer_source_device_get_type())
#define XFER_SOURCE_DEVICE(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_device_get_type(), XferSourceDevice)
#define XFER_SOURCE_DEVICE_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_device_get_type(), XferSourceDevice const)
#define XFER_SOURCE_DEVICE_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_device_get_type(), XferSourceDeviceClass)
#define IS_XFER_SOURCE_DEVICE(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_device_get_type ())
#define XFER_SOURCE_DEVICE_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_device_get_type(), XferSourceDeviceClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceDevice {
    XferElement __parent__;

    Device *device;
    size_t block_size;
    gboolean cancelled;
} XferSourceDevice;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceDeviceClass;

/*
 * Implementation
 */

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourceDevice *self = (XferSourceDevice *)elt;
    gpointer buf = NULL;
    int result;
    int devsize;

    /* indicate EOF on an cancel */
    if (elt->cancelled) {
	*size = 0;
	return NULL;
    }

    /* get the device block size */
    if (self->block_size == 0) {
	self->block_size = self->device->block_size;
    }

    do {
	buf = g_malloc(self->block_size);
	devsize = (int)self->block_size;
	result = device_read_block(self->device, buf, &devsize);
	*size = devsize;

	/* if the buffer was too small, loop around again */
	if (result == 0) {
	    g_assert(*size > self->block_size);
	    self->block_size = devsize;
	    amfree(buf);
	}
    } while (result == 0);

    if (result < 0) {
	amfree(buf);

	/* if we're not at EOF, it's an error */
	if (!self->device->is_eof) {
	    xfer_cancel_with_error(elt,
		_("error reading from %s: %s"),
		self->device->device_name,
		device_error_or_status(self->device));
	    wait_until_xfer_cancelled(elt->xfer);
	}

	*size = 0;
	return NULL;
    }

    return buf;
}

static void
instance_init(
    XferElement *elt)
{
    elt->can_generate_eof = TRUE;
}

static void
class_init(
    XferSourceDeviceClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, 0, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    klass->pull_buffer = pull_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Source::Device";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_source_device_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceDeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceDevice", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-device.h */
XferElement *
xfer_source_device(
    Device *device)
{
    XferSourceDevice *self = (XferSourceDevice *)g_object_new(XFER_SOURCE_DEVICE_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    g_assert(device != NULL);

    self->device = device;

    return elt;
}
