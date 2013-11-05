/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
#include "amutil.h"
#include "amxfer.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_filter_crc() references
 * it directly.
 */

GType xfer_filter_crc_get_type(void);
#define XFER_FILTER_CRC_TYPE (xfer_filter_crc_get_type())
#define XFER_FILTER_CRC(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_crc_get_type(), XferFilterCrc)
#define XFER_FILTER_CRC_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_filter_crc_get_type(), XferFilterCrc const)
#define XFER_FILTER_CRC_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_filter_crc_get_type(), XferFilterCrcClass)
#define IS_XFER_FILTER_CRC(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_filter_crc_get_type ())
#define XFER_FILTER_CRC_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_filter_crc_get_type(), XferFilterCrcClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferFilterCrc {
    XferElement __parent__;

} XferFilterCrc;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferFilterCrcClass;


/*
 * Implementation
 */

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferFilterCrc *self = (XferFilterCrc *)elt;
    char *buf;
    XMsg *msg;

    if (elt->cancelled) {
	/* drain our upstream only if we're expecting an EOF */
	if (elt->expect_eof) {
	    xfer_element_drain_buffers(XFER_ELEMENT(self)->upstream);
	}

	/* return an EOF */
	*size = 0;
	return NULL;
    }

    /* get a buffer from upstream, crc it, and hand it back */
    buf = xfer_element_pull_buffer(XFER_ELEMENT(self)->upstream, size);
    if (buf) {
	crc32_add((uint8_t *)buf, *size, &elt->crc);
    } else {
	g_debug("sending XMSG_CRC message");
	g_debug("crc pull_buffer CRC: %08x",
		crc32_finish(&elt->crc));
	msg = xmsg_new(elt, XMSG_CRC, 0);
	msg->crc = crc32_finish(&elt->crc);
	msg->size = elt->crc.size;
	xfer_queue_message(elt->xfer, msg);
    }
    return buf;
}

static void
push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t len)
{
    XferFilterCrc *self = (XferFilterCrc *)elt;
    XMsg *msg;

    /* drop the buffer if we've been cancelled */
    if (elt->cancelled) {
	amfree(buf);
	return;
    }

    /* crc the given buffer and pass it downstream */
    if (buf) {
	crc32_add((uint8_t *)buf, len, &elt->crc);
    } else {
	g_debug("sending XMSG_CRC message to %p", elt);
	g_debug("crc push_buffer CRC: %08x",
		crc32_finish(&elt->crc));
	msg = xmsg_new(elt, XMSG_CRC, 0);
	msg->crc = crc32_finish(&elt->crc);
	msg->size = elt->crc.size;
	xfer_queue_message(elt->xfer, msg);
    }
    xfer_element_push_buffer(XFER_ELEMENT(self)->downstream, buf, len);
}

static void
instance_init(
    XferElement *elt)
{
    elt->can_generate_eof = TRUE;
    crc32_init(&elt->crc);
}

static void
class_init(
    XferFilterCrcClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_PULL_BUFFER, XFER_MECH_PULL_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0) },
	{ XFER_MECH_PUSH_BUFFER, XFER_MECH_PUSH_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    klass->push_buffer = push_buffer_impl;
    klass->pull_buffer = pull_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Filter::Crc";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_filter_crc_get_type (void)
{
    static GType type = 0;

    if (G_UNLIKELY(type == 0)) {
        static const GTypeInfo info = {
            sizeof (XferFilterCrcClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferFilterCrc),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferFilterCrc", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_filter_crc(void)
{
    XferFilterCrc *xfx = (XferFilterCrc *)g_object_new(XFER_FILTER_CRC_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xfx);

    return elt;
}
