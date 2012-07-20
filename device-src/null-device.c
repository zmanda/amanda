/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "device.h"

#define NULL_DEVICE_MIN_BLOCK_SIZE (1)
#define NULL_DEVICE_MAX_BLOCK_SIZE (INT_MAX)
#define NULL_DEVICE_DEFAULT_BLOCK_SIZE DISK_BLOCK_BYTES

/*
 * Type checking and casting macros
 */
#define TYPE_NULL_DEVICE	(null_device_get_type())
#define NULL_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), null_device_get_type(), NullDevice)
#define NULL_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), null_device_get_type(), NullDevice const)
#define NULL_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), null_device_get_type(), NullDeviceClass)
#define IS_NULL_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), null_device_get_type ())
#define NULL_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), null_device_get_type(), NullDeviceClass)
static GType null_device_get_type (void);

/*
 * Main object structure
 */
typedef struct _NullDevice NullDevice;
struct _NullDevice {
	Device __parent__;
};

/*
 * Class definition
 */
typedef struct _NullDeviceClass NullDeviceClass;
struct _NullDeviceClass {
    DeviceClass __parent__;
};

void null_device_register(void);

/* here are local prototypes */
static void null_device_init (NullDevice * o);
static void null_device_class_init (NullDeviceClass * c);
static void null_device_base_init (NullDeviceClass * c);
static DeviceStatusFlags null_device_read_label(Device * dself);
static void null_device_open_device(Device * self, char *device_name,
		                    char * device_type, char * device_node);
static gboolean null_device_start (Device * self, DeviceAccessMode mode,
                                   char * label, char * timestamp);
static gboolean null_device_finish (Device * pself);
static gboolean null_device_start_file(Device * self, dumpfile_t * jobInfo);
static gboolean null_device_write_block (Device * self, guint size, gpointer data);
static gboolean null_device_finish_file(Device * self);
static Device* null_device_factory(char * device_name, char * device_type, char * device_node);

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

void null_device_register(void) {
    static const char * device_prefix_list[] = { "null", NULL };
    register_device(null_device_factory, device_prefix_list);
}

static GType
null_device_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (NullDeviceClass),
            (GBaseInitFunc) null_device_base_init,
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
    Device * dself;
    GValue response;

    dself = (Device*)(self);
    bzero(&response, sizeof(response));

    /* Register properties */
    g_value_init(&response, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(&response, CONCURRENCY_PARADIGM_RANDOM_ACCESS);
    device_set_simple_property(dself, PROPERTY_CONCURRENCY,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(&response, STREAMING_REQUIREMENT_NONE);
    device_set_simple_property(dself, PROPERTY_STREAMING,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_APPENDABLE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_PARTIAL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_FULL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_LEOM,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    /* this device's canonical name is always "null:", regardless of
     * the name the user supplies; note that we install the simple
     * getter in null_device_class_init. */
    g_value_init(&response, G_TYPE_STRING);
    g_value_set_static_string(&response, "null:");
    device_set_simple_property(dself, PROPERTY_CANONICAL_NAME,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);

    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_WRITE_ONLY);
    device_set_simple_property(dself, PROPERTY_MEDIUM_ACCESS_TYPE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);
}

static void
null_device_class_init (NullDeviceClass * c)
{
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->read_label = null_device_read_label;
    device_class->open_device = null_device_open_device;
    device_class->start = null_device_start;
    device_class->finish = null_device_finish;
    device_class->start_file = null_device_start_file;
    device_class->write_block = null_device_write_block;
    device_class->finish_file = null_device_finish_file;
}

static void
null_device_base_init (NullDeviceClass * c)
{
    DeviceClass *device_class = (DeviceClass *)c;

    /* Our canonical name is simpler than most devices' */
    device_class_register_property(device_class, PROPERTY_CANONICAL_NAME,
	    PROPERTY_ACCESS_GET_MASK,
	    device_simple_property_get_fn,
	    device_simple_property_set_fn);
}


static Device* null_device_factory(char * device_name, char * device_type, char * device_node) {
    Device * device;
    g_assert(0 == strcmp(device_type, "null"));
    device = DEVICE(g_object_new(TYPE_NULL_DEVICE, NULL));
    device_open_device(device, device_name, device_type, device_node);
    return device;
}

/* Begin virtual function overrides */

static DeviceStatusFlags
null_device_read_label(Device * dself) {
    if (device_in_error(dself)) return FALSE;

    device_set_error(dself,
	stralloc(_("Can't open NULL device for reading or appending.")),
	DEVICE_STATUS_DEVICE_ERROR);
    return FALSE;
}

static void
null_device_open_device(Device * pself, char *device_name,
			char * device_type, char * device_node)
{
    pself->min_block_size = NULL_DEVICE_MIN_BLOCK_SIZE;
    pself->max_block_size = NULL_DEVICE_MAX_BLOCK_SIZE;
    pself->block_size = NULL_DEVICE_DEFAULT_BLOCK_SIZE;

    if (parent_class->open_device) {
        parent_class->open_device(pself, device_name, device_type, device_node);
    }
}

static gboolean
null_device_start (Device * pself, DeviceAccessMode mode,
                   char * label, char * timestamp) {
    NullDevice * self;
    self = NULL_DEVICE(pself);

    if (device_in_error(self)) return FALSE;

    pself->access_mode = mode;
    g_mutex_lock(pself->device_mutex);
    pself->in_file = FALSE;
    g_mutex_unlock(pself->device_mutex);

    if (mode == ACCESS_WRITE) {
        pself->volume_label = newstralloc(pself->volume_label, label);
        pself->volume_time = newstralloc(pself->volume_time, timestamp);
	pself->header_block_size = 32768;
	return TRUE;
    } else {
	device_set_error(pself,
	    stralloc(_("Can't open NULL device for reading or appending.")),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }
}

/* This default implementation does very little. */
static gboolean
null_device_finish (Device * pself) {
    pself->access_mode = ACCESS_NULL;

    if (device_in_error(pself)) return FALSE;

    return TRUE;
}

static gboolean
null_device_start_file(Device * d_self,
		    dumpfile_t * jobInfo G_GNUC_UNUSED)
{
    g_mutex_lock(d_self->device_mutex);
    d_self->in_file = TRUE;
    g_mutex_unlock(d_self->device_mutex);
    d_self->is_eom = FALSE;
    d_self->block = 0;
    if (d_self->file <= 0)
        d_self->file = 1;
    else
        d_self->file ++;

    return TRUE;
}

static gboolean
null_device_write_block (Device * pself, guint size G_GNUC_UNUSED,
	    gpointer data G_GNUC_UNUSED) {
    NullDevice * self;
    self = NULL_DEVICE(pself);

    if (device_in_error(self)) return FALSE;

    pself->block++;

    return TRUE;
}

static gboolean
null_device_finish_file(Device * pself) {
    if (device_in_error(pself)) return FALSE;

    g_mutex_lock(pself->device_mutex);
    pself->in_file = FALSE;
    g_mutex_unlock(pself->device_mutex);
    return TRUE;
}
