/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
#include "amanda.h"
#include "device.h"
#include "property.h"
#include "xfer-device.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_taper() references
 * it directly.
 */

GType xfer_source_taper_get_type(void);
#define XFER_SOURCE_TAPER_TYPE (xfer_source_taper_get_type())
#define XFER_SOURCE_TAPER(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_taper_get_type(), XferSourceTaper)
#define XFER_SOURCE_TAPER_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_taper_get_type(), XferSourceTaper const)
#define XFER_SOURCE_TAPER_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_taper_get_type(), XferSourceTaperClass)
#define IS_XFER_SOURCE_TAPER(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_taper_get_type ())
#define XFER_SOURCE_TAPER_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_taper_get_type(), XferSourceTaperClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceTaper {
    XferElement __parent__;

    /* this mutex in this condition variable governs all variables below */
    GCond *start_part_cond;
    GMutex *start_part_mutex;

    /* is this device currently paused and awaiting a new part? */
    gboolean paused;

    /* Already-positioned device to read from */
    Device *device;

    /* and the block size for that device (reset to zero at the start of each
     * part) */
    size_t block_size;

    /* part size (potentially including any zero-padding from the
     * device) */
    guint64 part_size;

    /* timer for the duration; NULL while paused or cancelled */
    GTimer *part_timer;
} XferSourceTaper;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;

    /* start reading the part at which DEVICE is positioned, sending an
     * XMSG_PART_DONE when the part has been read */
    void (*start_part)(XferSourceTaper *self, Device *device);
} XferSourceTaperClass;

/*
 * Implementation
 */

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourceTaper *self = (XferSourceTaper *)elt;
    gpointer buf = NULL;
    int result;
    int devsize;
    XMsg *msg;

    g_mutex_lock(self->start_part_mutex);

    while (1) {
	/* make sure we have a device */
	while (self->paused && !elt->cancelled)
	    g_cond_wait(self->start_part_cond, self->start_part_mutex);

	/* indicate EOF on an cancel or when there are no more parts */
	if (elt->cancelled || !self->device) {
            goto error;
	}

	/* start the timer if appropriate */
	if (!self->part_timer)
	    self->part_timer = g_timer_new();

	/* loop until we read a full block, in case the blocks are larger than
	 * expected */
	if (self->block_size == 0)
	    self->block_size = (size_t)self->device->block_size;

	do {
	    buf = g_malloc(self->block_size);
	    devsize = (int)self->block_size;
	    result = device_read_block(self->device, buf, &devsize);
	    *size = devsize;

	    if (result == 0) {
		g_assert(*size > self->block_size);
		self->block_size = devsize;
		amfree(buf);
	    }
	} while (result == 0);

	/* if this block was successful, return it */
	if (result > 0) {
	    self->part_size += *size;
	    break;
	}

	if (result < 0) {
	    amfree(buf);

	    /* if we're not at EOF, it's an error */
	    if (!self->device->is_eof) {
		xfer_element_handle_error(elt,
		    _("error reading from %s: %s"),
		    self->device->device_name,
		    device_error_or_status(self->device));
                goto error;
	    }

	    /* the device has signalled EOF (really end-of-part), so clean up instance
	     * variables and report the EOP to the caller in the form of an xmsg */
	    msg = xmsg_new(XFER_ELEMENT(self), XMSG_PART_DONE, 0);
	    msg->size = self->part_size;
	    msg->duration = g_timer_elapsed(self->part_timer, NULL);
	    msg->partnum = 0;
	    msg->fileno = self->device->file;
	    msg->successful = TRUE;
	    msg->eof = FALSE;
	    xfer_queue_message(elt->xfer, msg);

	    self->paused = TRUE;
	    self->device = NULL;
	    self->part_size = 0;
	    self->block_size = 0;
	    if (self->part_timer) {
		g_timer_destroy(self->part_timer);
		self->part_timer = NULL;
	    }
	}
    }

    g_mutex_unlock(self->start_part_mutex);

    return buf;
error:
    g_mutex_unlock(self->start_part_mutex);
    *size = 0;
    return NULL;
}

static void
start_part_impl(
    XferSourceTaper *self,
    Device *device)
{
    g_assert(!device || device->in_file);

    g_mutex_lock(self->start_part_mutex);

    g_assert(self->paused);
    self->paused = FALSE;
    self->device = device;

    g_cond_broadcast(self->start_part_cond);
    g_mutex_unlock(self->start_part_mutex);
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferSourceTaper *self = XFER_SOURCE_TAPER(obj_self);

    g_cond_free(self->start_part_cond);
    g_mutex_free(self->start_part_mutex);
}

static void
instance_init(
    XferElement *elt)
{
    XferSourceTaper *self = XFER_SOURCE_TAPER(elt);

    elt->can_generate_eof = TRUE;
    self->paused = 1;
    self->start_part_cond = g_cond_new();
    self->start_part_mutex = g_mutex_new();
}

static void
class_init(
    XferSourceTaperClass * xst_klass)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(xst_klass);
    GObjectClass *gobject_klass = G_OBJECT_CLASS(xst_klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, 0, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    klass->pull_buffer = pull_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Source::Taper";
    klass->mech_pairs = mech_pairs;

    xst_klass->start_part = start_part_impl;

    gobject_klass->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(xst_klass);
}

GType
xfer_source_taper_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceTaperClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceTaper),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceTaper", &info, 0);
    }

    return type;
}

/*
 * Public methods and stubs
 */

void
xfer_source_taper_start_part(
    XferElement *elt,
    Device *device)
{
    XferSourceTaperClass *klass;
    g_assert(IS_XFER_SOURCE_TAPER(elt));

    klass = XFER_SOURCE_TAPER_GET_CLASS(elt);
    klass->start_part(XFER_SOURCE_TAPER(elt), device);
}

/* create an element of this class; prototype is in xfer-device.h */
XferElement *
xfer_source_taper(void)
{
    XferSourceTaper *self = (XferSourceTaper *)g_object_new(XFER_SOURCE_TAPER_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    return elt;
}
