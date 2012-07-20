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
 * This declaration is entirely private; nothing but xfer_dest_device() references
 * it directly.
 */

GType xfer_dest_device_get_type(void);
#define XFER_DEST_DEVICE_TYPE (xfer_dest_device_get_type())
#define XFER_DEST_DEVICE(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_device_get_type(), XferDestDevice)
#define XFER_DEST_DEVICE_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_device_get_type(), XferDestDevice const)
#define XFER_DEST_DEVICE_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_device_get_type(), XferDestDeviceClass)
#define IS_XFER_DEST_DEVICE(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_device_get_type ())
#define XFER_DEST_DEVICE_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_device_get_type(), XferDestDeviceClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferDestDevice {
    XferElement __parent__;

    Device *device;

    gboolean cancel_at_leom;

    gpointer partial;
    gsize block_size;
    gsize partial_length;
} XferDestDevice;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferDestDeviceClass;

/*
 * Implementation
 */

static gboolean
do_block(
    XferDestDevice *self,
    guint size,
    gpointer data)
{
    XferElement *elt = XFER_ELEMENT(self);

    if (!device_write_block(self->device, size, data)) {
	xfer_cancel_with_error(elt, "%s: %s",
		self->device->device_name, device_error_or_status(self->device));
	wait_until_xfer_cancelled(elt->xfer);
	return FALSE;
    }

    /* check for LEOM */
    if (self->cancel_at_leom && self->device->is_eom) {
	xfer_cancel_with_error(elt, "%s: LEOM detected", self->device->device_name);
	wait_until_xfer_cancelled(elt->xfer);
	return FALSE;
    }

    return TRUE;
}

static void
push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t len)
{
    XferDestDevice *self = XFER_DEST_DEVICE(elt);
    gpointer to_free = buf;

    /* Handle EOF */
    if (!buf) {
	/* write out the partial buffer, if there's anything in it */
	if (self->partial_length) {
	    if (!do_block(self, self->block_size, self->partial)) {
		return;
	    }
	    self->partial_length = 0;
	}

	device_finish_file(self->device);
	return;
    }

    /* set up the block buffer, now that we can depend on having a blocksize
     * from the device */
    if (!self->partial) {
	self->partial = g_malloc(self->device->block_size);
	self->block_size = self->device->block_size;
	self->partial_length = 0;
    }

    /* if we already have data in the buffer, add the new data to it */
    if (self->partial_length != 0) {
	gsize to_copy = min(self->block_size - self->partial_length, len);
	memmove(self->partial + self->partial_length, buf, to_copy);
	buf = (gpointer)(to_copy + (char *)buf);
	len -= to_copy;
	self->partial_length += to_copy;
    }

    /* and if the buffer is now full, write the block */
    if (self->partial_length == self->block_size) {
	if (!do_block(self, self->block_size, self->partial)) {
	    g_free(to_free);
	    return;
	}
	self->partial_length = 0;
    }

    /* write any whole blocks directly from the push buffer */
    while (len >= self->block_size) {
	if (!do_block(self, self->block_size, buf)) {
	    g_free(to_free);
	    return;
	}

	buf = (gpointer)(self->block_size + (char *)buf);
	len -= self->block_size;
    }

    /* and finally store any leftover data in the partial buffer */
    if (len) {
	memmove(self->partial, buf, len);
	self->partial_length = len;
    }

    g_free(to_free);
}

static void
instance_init(
    XferElement *elt)
{
    XferDestDevice *self = XFER_DEST_DEVICE(elt);
    self->partial = NULL;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferDestDevice *self = XFER_DEST_DEVICE(obj_self);

    if (self->partial) {
	g_free(self->partial);
    }
}

static void
class_init(
    XferDestDeviceClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = (GObjectClass*) klass;
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_PUSH_BUFFER, XFER_MECH_NONE, 0, 0},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    klass->push_buffer = push_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Dest::Device";
    klass->mech_pairs = mech_pairs;

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_dest_device_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestDeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestDevice", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-device.h */
XferElement *
xfer_dest_device(
    Device *device,
    gboolean cancel_at_leom)
{
    XferDestDevice *self = (XferDestDevice *)g_object_new(XFER_DEST_DEVICE_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    g_assert(device != NULL);

    self->device = device;
    self->cancel_at_leom = cancel_at_leom;

    return elt;
}
