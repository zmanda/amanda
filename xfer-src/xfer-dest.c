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

#define selfp (self->_priv)

#include "taper-source.h"
#include "taper-file-source.h"
#include "taper-port-source.h"
#include "taper-disk-port-source.h"
#include "taper-mem-port-source.h"

/* here are local prototypes */
static void xfer_source_init (XferSource * o);
static void xfer_source_class_init (XferSourceClass * c);

/* pointer to the class of our parent */
static GObjectClass *parent_class = NULL;

GType
xfer_source_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) xfer_source_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSource),
            0 /* n_preallocs */,
            (GInstanceInitFunc) xfer_source_init,
            NULL
        };

        type = g_type_register_static (G_TYPE_OBJECT, "XferSource", &info,
                                       (GTypeFlags)G_TYPE_FLAG_ABSTRACT);
    }

    return type;
}

static void xfer_source_finalize(GObject * obj_self) {
    XferSource * self = XFER_SOURCE(obj_self);

    if (G_OBJECT_CLASS(parent_class)->finalize)
        G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
xfer_source_init (XferSource * o) {
    o->end_of_data = FALSE;
}

static void
xfer_source_class_init (XferSourceClass * c) {
    GObjectClass *g_object_class = (GObjectClass*) c;

    parent_class = g_type_class_ref (G_TYPE_OBJECT);

    c->read = NULL;

    g_object_class->finalize = xfer_source_finalize;
}


/* The rest of these functions are vtable dispatch stubs. */

ssize_t
xfer_source_read (XferSource * self, void * buf, size_t count)
{
    XferSourceClass *klass;
    g_return_val_if_fail (self != NULL, (ssize_t )-1);
    g_return_val_if_fail (IS_XFER_SOURCE (self), (ssize_t )-1);
    g_return_val_if_fail (buf != NULL, (ssize_t )-1);
    g_return_val_if_fail (count > 0, (ssize_t )-1);

    if (self->end_of_data || self->end_of_part) {
        return 0;
    }

    klass = XFER_SOURCE_GET_CLASS(self);

    if(klass->read)
        return (*klass->read)(self,buf,count);
    else
        return (ssize_t )(-1);
}

