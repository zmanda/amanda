/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "amxfer.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_dest_buffer() references
 * it directly.
 */

GType xfer_dest_buffer_get_type(void);
#define XFER_DEST_BUFFER_TYPE (xfer_dest_buffer_get_type())
#define XFER_DEST_BUFFER(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_buffer_get_type(), XferDestBuffer)
#define XFER_DEST_BUFFER_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_buffer_get_type(), XferDestBuffer const)
#define XFER_DEST_BUFFER_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_buffer_get_type(), XferDestBufferClass)
#define IS_XFER_DEST_BUFFER(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_buffer_get_type ())
#define XFER_DEST_BUFFER_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_buffer_get_type(), XferDestBufferClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferDestBuffer {
    XferElement __parent__;

    gsize max_size;

    gpointer buf;
    gsize len;
    gsize allocated;
} XferDestBuffer;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;

    void (*get)(XferDestBuffer *self, gpointer *buf, gsize *size);
} XferDestBufferClass;

/*
 * Implementation
 */

static void
get_impl(
    XferDestBuffer *self,
    gpointer *buf,
    gsize *size)
{
    if (size)
	*size = self->len;

    if (buf)
	*buf = self->buf;
}

static void
push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t len)
{
    XferDestBuffer *self = (XferDestBuffer *)elt;

    if (!buf)
	return;

    /* make sure this isn't too much data */
    if (self->max_size && self->len + len > self->max_size) {
	xfer_cancel_with_error(elt,
	    _("illegal attempt to transfer more than %zd bytes"), self->max_size);
	wait_until_xfer_cancelled(elt->xfer);
	amfree(buf);
	return;
    }

    /* expand the buffer if necessary */
    if (self->len + len > self->allocated) {
	gsize new_size = self->allocated * 2;
	if (new_size < self->len+len)
	    new_size = self->len+len;
	if (self->max_size && new_size > self->max_size)
	    new_size = self->max_size;

	self->buf = g_realloc(self->buf, new_size);
	self->allocated = new_size;
    }

    g_memmove(((guint8 *)self->buf)+self->len, buf, len);
    self->len += len;

    amfree(buf);
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferDestBuffer *self = XFER_DEST_BUFFER(obj_self);

    if (self->buf)
	g_free(self->buf);
    self->buf = NULL;

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferDestBufferClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_PUSH_BUFFER, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    selfc->get = get_impl;
    klass->push_buffer = push_buffer_impl;
    goc->finalize = finalize_impl;

    klass->perl_class = "Amanda::Xfer::Dest::Buffer";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_dest_buffer_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestBufferClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestBuffer),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestBuffer", &info, 0);
    }

    return type;
}

void
xfer_dest_buffer_get(
    XferElement *elt,
    gpointer *buf,
    gsize *size)
{
    XferDestBufferClass *klass;
    g_assert(IS_XFER_DEST_BUFFER(elt));

    klass = XFER_DEST_BUFFER_GET_CLASS(elt);
    klass->get(XFER_DEST_BUFFER(elt), buf, size);
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_dest_buffer(
    gsize max_size)
{
    XferDestBuffer *self = (XferDestBuffer *)g_object_new(XFER_DEST_BUFFER_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    self->max_size = max_size;

    return elt;
}
