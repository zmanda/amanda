/*
 * Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as 
 * published by the Free Software Foundation.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 * 
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "device.h"
#include "null-device.h"

#define NULL_DEVICE_MIN_BLOCK_SIZE (1)
#define NULL_DEVICE_MAX_BLOCK_SIZE SHRT_MAX

/* here are local prototypes */
static void null_device_init (NullDevice * o);
static void null_device_class_init (NullDeviceClass * c);
static gboolean null_device_start (Device * self, DeviceAccessMode mode,
                                   char * label, char * timestamp);
static gboolean null_device_write_block (Device * self, guint size,
                                         gpointer data, gboolean last);
static Device* null_device_factory(char * device_type,
                                   char * device_name);

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

void null_device_register(void) {
    static const char * device_prefix_list[] = { "null", NULL };
    register_device(null_device_factory, device_prefix_list);
}

GType
null_device_get_type (void)
{
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (NullDeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) null_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (NullDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) null_device_init,
            NULL
        };
        
        type = g_type_register_static (TYPE_DEVICE, "NullDevice", &info,
                                       (GTypeFlags)0);
    }

    return type;
}

static void 
null_device_init (NullDevice * self)
{
    Device * o;
    DeviceProperty prop;
    GValue response;

    o = (Device*)(self);
    bzero(&response, sizeof(response));

    /* Register properties */
    prop.base = &device_property_concurrency;
    prop.access = PROPERTY_ACCESS_GET_MASK;
    g_value_init(&response, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(&response, CONCURRENCY_PARADIGM_RANDOM_ACCESS);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
    

    prop.base = &device_property_streaming;
    g_value_init(&response, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(&response, STREAMING_REQUIREMENT_NONE);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
    
    prop.base = &device_property_block_size;
    g_value_init(&response, G_TYPE_INT);
    g_value_set_int(&response, -1);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
    
    prop.base = &device_property_min_block_size;
    g_value_init(&response, G_TYPE_UINT);
    g_value_set_uint(&response, NULL_DEVICE_MIN_BLOCK_SIZE);
    device_add_property(o, &prop, &response);

    prop.base = &device_property_max_block_size;
    g_value_set_uint(&response, NULL_DEVICE_MAX_BLOCK_SIZE);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_appendable;
    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_add_property(o, &prop, &response);

    prop.base = &device_property_partial_deletion;
    device_add_property(o, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_canonical_name;
    g_value_init(&response, G_TYPE_STRING);
    g_value_set_static_string(&response, "null:");
    device_add_property(o, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_medium_access_type;
    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_WRITE_ONLY);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
}

static void 
null_device_class_init (NullDeviceClass * c G_GNUC_UNUSED)
{
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->start = null_device_start;
    device_class->write_block = null_device_write_block;
}


static Device* null_device_factory(char * device_type,
                                   char * device_name G_GNUC_UNUSED) {
    g_assert(0 == strcmp(device_type, "null"));
    return DEVICE(g_object_new(TYPE_NULL_DEVICE, NULL));
    
}

/* Begin virtual function overrides */

static gboolean 
null_device_start (Device * pself, DeviceAccessMode mode,
                   char * label, char * timestamp) {
    NullDevice * self;
    self = NULL_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);

    if (mode == ACCESS_WRITE) {
        if (parent_class->start) {
            return parent_class->start((Device*)self, mode, label, timestamp);
        } else {
            return TRUE;
        }
    } else {
        g_fprintf(stderr, "Can't open NULL device for reading or appending.\n");
        return FALSE;
    }
}

static gboolean
null_device_write_block (Device * pself, guint size, gpointer data,
                         gboolean last_block) {
    NullDevice * self;
    self = NULL_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);
    g_return_val_if_fail (data != NULL, FALSE);
    
    if ((size < NULL_DEVICE_MIN_BLOCK_SIZE && !last_block) ||
        size > NULL_DEVICE_MAX_BLOCK_SIZE) {
        return FALSE;
    } else {
        if (parent_class->write_block) {
            /* Calls device_finish_file(). */
            parent_class->write_block((Device*)self, size, data, last_block);
        }
        return TRUE;
    }

    return FALSE;
}
