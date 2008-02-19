/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* The Device API abstracts device workings, interaction, properties, and
 * capabilities from the rest of the Amanda code base. It supports
 * pluggable modules for different kinds of devices. */

#include "amanda.h"
#include "conffile.h"

#include <regex.h>

#include "device.h"
#include "queueing.h"
#include "property.h"

#include "null-device.h"
#include "timestamp.h"
#include "vfs-device.h"
#include "util.h"
#ifdef WANT_TAPE_DEVICE
#include "tape-device.h"
#endif
#include "rait-device.h"
#ifdef WANT_S3_DEVICE
  #include "s3-device.h"
#endif

static GHashTable* driverList = NULL;

void device_api_init(void) {
    g_type_init();
    amanda_thread_init();
    device_property_init();
    driverList = g_hash_table_new(g_str_hash, g_str_equal);

    /* register other types and devices. */
    null_device_register();
    vfs_device_register();
#ifdef WANT_TAPE_DEVICE
    tape_device_register();
#endif
    rait_device_register();
#ifdef WANT_S3_DEVICE
    s3_device_register();
#endif
}

void register_device(DeviceFactory factory,
                     const char ** device_prefix_list) {
    char ** tmp;
    g_assert(driverList != NULL);
    g_assert(factory != NULL);
    g_return_if_fail(device_prefix_list != NULL);
    g_return_if_fail(*device_prefix_list != NULL);

    tmp = (char**)device_prefix_list;
    while (*tmp != NULL) {
        g_hash_table_insert(driverList, *tmp, (gpointer)factory);
        tmp ++;
    }
}

static DeviceFactory lookup_device_factory(const char *device_name) {
    gpointer key, value;
    g_assert(driverList != NULL);

    if (g_hash_table_lookup_extended(driverList, device_name, &key, &value)) {
        return (DeviceFactory)value;
    } else {
        return NULL;
    }
}

static const GFlagsValue read_label_status_flags_values[] = {
    { READ_LABEL_STATUS_SUCCESS,
      "READ_LABEL_STATUS_SUCCESS",
      "Success" },
    { READ_LABEL_STATUS_DEVICE_MISSING,
      "READ_LABEL_STATUS_DEVICE_MISSING",
      "Device not found" },
    { READ_LABEL_STATUS_DEVICE_ERROR,
      "READ_LABEL_STATUS_DEVICE_ERROR",
      "Device error" },
    { READ_LABEL_STATUS_VOLUME_MISSING,
      "READ_LABEL_STATUS_VOLUME_MISSING",
      "Volume not found" },
    { READ_LABEL_STATUS_VOLUME_UNLABELED,
      "READ_LABEL_STATUS_VOLUME_UNLABELED",
      "Volume not labeled" },
    { READ_LABEL_STATUS_VOLUME_ERROR,
      "READ_LABEL_STATUS_VOLUME_ERROR",
      "Volume error" },
    { 0, NULL, NULL }
};

GType read_label_status_flags_get_type(void) {
    static GType type = 0;
    if (G_UNLIKELY(type == 0)) {
        type = g_flags_register_static("ReadLabelStatusFlags",
                                       read_label_status_flags_values);
    }
    return type;
}

/* Device class definition starts here. */

struct DevicePrivate_s {
    /* This is the return value of the device_get_property_list()
       method. */
    GArray *property_list;
    GHashTable * property_response;
};

/* This holds the default response to a particular property. */
typedef struct {
    PropertyAccessFlags access;
    GValue response;
} PropertyResponse;

#define selfp (self->private)

/* here are local prototypes, so we can make function pointers. */
static void device_init (Device * o) G_GNUC_UNUSED;
static void device_class_init (DeviceClass * c) G_GNUC_UNUSED;

static void property_response_free(PropertyResponse *o);

static gboolean default_device_open_device(Device * self, char * device_name);
static gboolean default_device_finish(Device * self);
static gboolean default_device_start(Device * self, DeviceAccessMode mode,
                                     char * label, char * timestamp);
static gboolean default_device_start_file (Device * self,
                                           const dumpfile_t * jobinfo);
static gboolean default_device_write_block (Device * self, guint size,
                                            gpointer data, gboolean last);
static gboolean default_device_write_from_fd(Device *self, int fd);
static gboolean default_device_finish_file (Device * self);
static dumpfile_t* default_device_seek_file (Device * self, guint file);
static gboolean default_device_seek_block (Device * self, guint64 block);
static int default_device_read_block (Device * self, gpointer buffer,
                                      int * size);
static gboolean default_device_read_to_fd(Device *self, int fd);
static gboolean default_device_property_get(Device * self, DevicePropertyId ID,
                                            GValue * value);

/* pointer to the class of our parent */
static GObjectClass *parent_class = NULL;

GType
device_get_type (void)
{
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (DeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (Device),
            0 /* n_preallocs */,
            (GInstanceInitFunc) device_init,
            NULL
        };
        
        type = g_type_register_static (G_TYPE_OBJECT, "Device", &info,
                                       (GTypeFlags)G_TYPE_FLAG_ABSTRACT);
    }
    
    return type;
}

static void device_finalize(GObject *obj_self) {
    Device *self G_GNUC_UNUSED = DEVICE (obj_self);
    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    /* Here we call device_finish() if it hasn't been done
       yet. Subclasses may need to do this same check earlier. */
    if (self->access_mode != ACCESS_NULL) {
        device_finish(self);
    }

    amfree(self->device_name);
    amfree(self->volume_label);
    amfree(self->volume_time);
    g_array_free(selfp->property_list, TRUE);
    g_hash_table_destroy(selfp->property_response);
    amfree(self->private);
}

static void 
device_init (Device * self G_GNUC_UNUSED)
{
    self->private = malloc(sizeof(DevicePrivate));
    self->device_name = NULL;
    self->access_mode = ACCESS_NULL;
    self->is_eof = FALSE;
    self->file = -1;
    self->block = 0;
    self->in_file = FALSE;
    self->volume_label = NULL;
    self->volume_time = NULL;
    selfp->property_list = g_array_new(TRUE, FALSE, sizeof(DeviceProperty));
    selfp->property_response =
        g_hash_table_new_full(g_direct_hash,
                              g_direct_equal,
                              NULL,
                              (GDestroyNotify) property_response_free);
}

static void 
device_class_init (DeviceClass * c G_GNUC_UNUSED)
{
    GObjectClass *g_object_class G_GNUC_UNUSED = (GObjectClass*) c;
    
    parent_class = g_type_class_ref (G_TYPE_OBJECT);
    
    c->open_device = default_device_open_device;
    c->finish = default_device_finish;
    c->read_label = NULL;
    c->start = default_device_start;
    c->start_file = default_device_start_file;
    c->write_block = default_device_write_block;
    c->write_from_fd = default_device_write_from_fd;
    c->finish_file = default_device_finish_file;
    c->seek_file = default_device_seek_file;
    c->seek_block = default_device_seek_block;
    c->read_block = default_device_read_block;
    c->read_to_fd = default_device_read_to_fd;
    c->property_get = default_device_property_get;
    c->property_set = NULL;
    c->recycle_file = NULL;
    g_object_class->finalize = device_finalize;
}

static void property_response_free(PropertyResponse * resp) {
    g_value_unset(&(resp->response));
    amfree(resp);
}

static char *
regex_message(int result, regex_t *regex) {
    char * rval;
    size_t size;

    size = regerror(result, regex, NULL, 0);
    rval = malloc(size);
    regerror(result, regex, rval, size);

    return rval;
}

static gboolean
handle_device_regex(const char * user_name, char ** driver_name,
                    char ** device) {
    regex_t regex;
    int reg_result;
    regmatch_t pmatch[3];
    static const char * regex_string = "^([a-z0-9]+):(.*)$";

    bzero(&regex, sizeof(regex));

    reg_result = regcomp(&regex, regex_string, REG_EXTENDED | REG_ICASE);
    if (reg_result != 0) {
        char * message = regex_message(reg_result, &regex);
        g_fprintf(stderr, "Error compiling regular expression \"%s\": %s\n",
               regex_string, message);
        amfree(message);
        return FALSE;
    }

    reg_result = regexec(&regex, user_name, 3, pmatch, 0);
    if (reg_result != 0 && reg_result != REG_NOMATCH) {
        char * message = regex_message(reg_result, &regex);
        g_fprintf(stderr, "Error applying regular expression \"%s\" to string \"%s\":\n"
               "%s\n", user_name, regex_string, message);
        regfree(&regex);
        return FALSE;
    } else if (reg_result == REG_NOMATCH) {
#ifdef WANT_TAPE_DEVICE
        g_fprintf(stderr, "\"%s\" uses deprecated device naming convention; \n"
                "using \"tape:%s\" instead.\n",
                user_name, user_name);
        *driver_name = stralloc("tape");
        *device = stralloc(user_name);
#else /* !WANT_TAPE_DEVICE */
        g_fprintf(stderr, "\"%s\" is not a valid device name.\n", user_name);
	regfree(&regex);
	return FALSE;
#endif /* WANT_TAPE_DEVICE */
    } else {
        *driver_name = find_regex_substring(user_name, pmatch[1]);
        *device = find_regex_substring(user_name, pmatch[2]);
    }
    regfree(&regex);
    return TRUE;
}

Device* 
device_open (char * device_name)
{
    char *device_driver_name = NULL;
    char *device_node_name = NULL;
    DeviceFactory factory;
    Device *device;

    g_return_val_if_fail (device_name != NULL, NULL);

    if (driverList == NULL) {
        g_log(G_LOG_DOMAIN, G_LOG_LEVEL_ERROR,
              "device_open() called without device_api_init()!\n");
        g_assert_not_reached();
    }

    if (!handle_device_regex(device_name, &device_driver_name, &device_node_name)) {
        amfree(device_driver_name);
        amfree(device_node_name);
        return NULL;
    }

    factory = lookup_device_factory(device_driver_name);

    if (factory == NULL) {
        g_fprintf(stderr, "Device driver %s is not known.\n",
                device_driver_name);
        amfree(device_driver_name);
        amfree(device_node_name);
        return NULL;
    }

    device = factory(device_driver_name, device_node_name);
    amfree(device_driver_name);
    amfree(device_node_name);
    return device;
}

void 
device_add_property (Device * self, DeviceProperty * prop, GValue * response)
{
    unsigned int i;
    g_return_if_fail (self != NULL);
    g_return_if_fail (IS_DEVICE (self));
    g_assert(selfp->property_list != NULL);
    g_assert(selfp->property_response != NULL);

    /* Delete it if it already exists. */
    for(i = 0; i < selfp->property_list->len; i ++) {
        if (g_array_index(selfp->property_list,
                          DeviceProperty, i).base->ID == prop->base->ID) {
            g_array_remove_index_fast(selfp->property_list, i);
            break;
        }
    }

    g_array_append_val(selfp->property_list, *prop);
    
    if (response != NULL) {
        PropertyResponse * property_response;
        
        g_return_if_fail(G_IS_VALUE(response));
        
        property_response = malloc(sizeof(*property_response));
        property_response->access = prop->access;
        bzero(&(property_response->response),
              sizeof(property_response->response));
        g_value_init(&(property_response->response),
                     G_VALUE_TYPE(response));
        g_value_copy(response, &(property_response->response));
        
        g_hash_table_insert(selfp->property_response,
                            GINT_TO_POINTER(prop->base->ID),
                            property_response);
    }
}

const DeviceProperty * 
device_property_get_list (Device * self)
{
	g_return_val_if_fail (self != NULL, (const DeviceProperty * )0);
	g_return_val_if_fail (IS_DEVICE (self), (const DeviceProperty * )0);

        return (const DeviceProperty*) selfp->property_list->data;
}

guint device_write_min_size(Device * self) {
    GValue g_tmp;
    int block_size, min_block_size;
    
    bzero(&g_tmp, sizeof(g_tmp));
    device_property_get(self, PROPERTY_BLOCK_SIZE, &g_tmp);
    block_size = g_value_get_int(&g_tmp);
    g_value_unset(&g_tmp);
    if (block_size > 0) {
        return block_size;
    }

    /* variable block size */
    device_property_get(self, PROPERTY_MIN_BLOCK_SIZE, &g_tmp);
    min_block_size = g_value_get_uint(&g_tmp);
    g_value_unset(&g_tmp);
    return min_block_size;
}

guint device_write_max_size(Device * self) {
    GValue g_tmp;
    int block_size, max_block_size;
    
    bzero(&g_tmp, sizeof(g_tmp));
    device_property_get(self, PROPERTY_BLOCK_SIZE, &g_tmp);
    block_size = g_value_get_int(&g_tmp);
    g_value_unset(&g_tmp);
    if (block_size > 0) {
        return block_size;
    }

    /* variable block size */
    device_property_get(self, PROPERTY_MAX_BLOCK_SIZE, &g_tmp);
    max_block_size = g_value_get_uint(&g_tmp);
    g_value_unset(&g_tmp);
    return max_block_size;
}

guint device_read_max_size(Device * self) {
    GValue g_tmp;
    
    bzero(&g_tmp, sizeof(g_tmp));
    if (device_property_get(self, PROPERTY_READ_BUFFER_SIZE, &g_tmp)) {
        guint rval = g_value_get_uint(&g_tmp);
        g_value_unset(&g_tmp);
        return rval;
    } else {
        return device_write_max_size(self);
    }
}

char * device_build_amanda_header(Device * self, const dumpfile_t * info,
                                  int * size, gboolean * oneblock) {
    char *amanda_header;
    unsigned int min_header_length;
    unsigned int header_buffer_size;

    min_header_length = device_write_min_size(self);
    amanda_header = build_header(info, min_header_length);
    header_buffer_size = MAX(min_header_length, strlen(amanda_header)+1);
    if (size != NULL)
        *size = header_buffer_size;
    if (oneblock != NULL)
        *oneblock = (header_buffer_size <=  device_write_max_size(self));
    return amanda_header;
}

dumpfile_t * make_tapestart_header(Device * self, char * label,
                                   char * timestamp) {
    dumpfile_t * rval;

    g_return_val_if_fail(label != NULL, NULL);

    rval = malloc(sizeof(*rval));
    fh_init(rval);
    rval->type = F_TAPESTART;
    amfree(self->volume_time);
    if (get_timestamp_state(timestamp) == TIME_STATE_REPLACE) {
        self->volume_time = get_proper_stamp_from_time(time(NULL));
    } else {
        self->volume_time = g_strdup(timestamp);
    }
    strncpy(rval->datestamp, self->volume_time, sizeof(rval->datestamp));
    strncpy(rval->name, label, sizeof(rval->name));

    return rval;
}

dumpfile_t * make_tapeend_header(void) {
    dumpfile_t * rval;
    char * timestamp;

    rval = malloc(sizeof(*rval));
    rval->type = F_TAPEEND;
    timestamp = get_timestamp_from_time(time(NULL));
    strncpy(rval->datestamp, timestamp, sizeof(rval->datestamp));
    amfree(timestamp);
    return rval;
}

/* Try setting max/fixed blocksize on a device. Check results, fallback, and
 * print messages for problems. */
static void try_set_blocksize(Device * device, guint blocksize,
                              gboolean try_max_first) {
    GValue val;
    gboolean success;
    bzero(&val, sizeof(val));
    g_value_init(&val, G_TYPE_UINT);
    g_value_set_uint(&val, blocksize);
    if (try_max_first) {
        success = device_property_set(device,
                                      PROPERTY_MAX_BLOCK_SIZE,
                                      &val);
        if (!success) {
            g_fprintf(stderr, "Setting MAX_BLOCK_SIZE to %u "
                    "not supported for device %s.\n"
                    "trying BLOCK_SIZE instead.\n",
                    blocksize, device->device_name);
        } else {
            g_value_unset(&val);
            return;
        }
    }

    g_value_unset(&val);
    g_value_init(&val, G_TYPE_INT);
    g_value_set_int(&val, blocksize);
    success = device_property_set(device,
                                  PROPERTY_BLOCK_SIZE,
                                  &val);
    if (!success) {
        g_fprintf(stderr, "Setting BLOCK_SIZE to %u "
                "not supported for device %s.\n",
                blocksize, device->device_name);
    }
    g_value_unset(&val);
}

/* A GHFunc (callback for g_hash_table_foreach) */
static void set_device_property(gpointer key_p, gpointer value_p,
                                   gpointer user_data_p) {
    char * property_s = key_p;
    char * value_s = value_p;
    Device * device = user_data_p;
    const DevicePropertyBase* property_base;
    GValue property_value;

    g_return_if_fail(IS_DEVICE(device));
    g_return_if_fail(property_s != NULL);
    g_return_if_fail(value_s != NULL);

    property_base = device_property_get_by_name(property_s);
    if (property_base == NULL) {
        /* Nonexistant property name. */
        g_fprintf(stderr, _("Unknown device property name %s.\n"), property_s);
        return;
    }
    
    bzero(&property_value, sizeof(property_value));
    g_value_init(&property_value, property_base->type);
    if (!g_value_set_from_string(&property_value, value_s)) {
        /* Value type could not be interpreted. */
        g_fprintf(stderr,
                _("Could not parse property value %s for property type %s.\n"),
                value_s, g_type_name(property_base->type));
        return;
    } else {
        g_assert (G_VALUE_HOLDS(&property_value, property_base->type));
    }

    if (!device_property_set(device, property_base->ID, &property_value)) {
        /* Device rejects property. */
        g_fprintf(stderr, _("Could not set property %s to %s on device %s.\n"),
                property_base->name, value_s, device->device_name);
        return;
    }
}

/* Set up first-run properties, including DEVICE_MAX_VOLUME_USAGE property
 * based on the tapetype. */
void device_set_startup_properties_from_config(Device * device) {
    char * tapetype_name = getconf_str(CNF_TAPETYPE);
    if (tapetype_name != NULL) {
        tapetype_t * tapetype = lookup_tapetype(tapetype_name);
        if (tapetype != NULL) {
            GValue val;
            guint64 length;
            guint blocksize_kb;
            gboolean success;

            bzero(&val, sizeof(GValue));

            if (tapetype_seen(tapetype, TAPETYPE_LENGTH)) {
		length = tapetype_get_length(tapetype);
                g_value_init(&val, G_TYPE_UINT64);
                g_value_set_uint64(&val, length * 1024);
                /* If this fails, it's not really an error. */
                device_property_set(device, PROPERTY_MAX_VOLUME_USAGE, &val);
                g_value_unset(&val);
            }

            if (tapetype_seen(tapetype, TAPETYPE_READBLOCKSIZE)) {
		blocksize_kb = tapetype_get_readblocksize(tapetype);
                g_value_init(&val, G_TYPE_UINT);
                g_value_set_uint(&val, blocksize_kb * 1024);
                success = device_property_set(device,
                                              PROPERTY_READ_BUFFER_SIZE,
                                              &val);
                g_value_unset(&val);
                if (!success) {
                    g_fprintf(stderr, "Setting READ_BUFFER_SIZE to %llu "
                            "not supported for device %s.\n",
                            1024*(long long unsigned int)blocksize_kb,
			    device->device_name);
                }
            }

            if (tapetype_seen(tapetype, TAPETYPE_BLOCKSIZE)) {
		blocksize_kb = tapetype_get_blocksize(tapetype);
                try_set_blocksize(device, blocksize_kb * 1024,
                                  !tapetype_get_file_pad(tapetype));
            }
        }
    }

    g_hash_table_foreach(getconf_proplist(CNF_DEVICE_PROPERTY),
                         set_device_property, device);
}

void device_clear_volume_details(Device * device) {
    if (device == NULL || device->access_mode != ACCESS_NULL) {
        return;
    }

    amfree(device->volume_label);
    amfree(device->volume_time);
}

/* Here we put default implementations of virtual functions. Since
   this class is virtual, many of these functions offer at best
   incomplete functionality. But they do offer the useful commonality
   that all devices can expect to need. */

/* This function only updates access_mode, volume_label, and volume_time. */
static gboolean
default_device_start (Device * self, DeviceAccessMode mode, char * label,
                      char * timestamp) {
    if (mode != ACCESS_WRITE && self->volume_label == NULL) {
        if (device_read_label(self) != READ_LABEL_STATUS_SUCCESS)
            return FALSE;
    } else if (mode == ACCESS_WRITE) {
        self->volume_label = newstralloc(self->volume_label, label);
        self->volume_time = newstralloc(self->volume_time, timestamp);
    }
    self->access_mode = mode;

    return TRUE;
}

static gboolean default_device_open_device(Device * self,
                                           char * device_name) {
    DeviceProperty prop;
    guint i;

    self->device_name = stralloc(device_name);

    prop.base = &device_property_canonical_name;
    prop.access = PROPERTY_ACCESS_GET_MASK;

    for(i = 0; i < selfp->property_list->len; i ++) {
        if (g_array_index(selfp->property_list,
                          DeviceProperty, i).base->ID == prop.base->ID) {
            return TRUE;
        }
    }
    /* If we got here, the property was not registered. */
    device_add_property(self, &prop, NULL);

    return TRUE;
}

/* This default implementation does very little. */
static gboolean
default_device_finish (Device * self) {
    self->access_mode = ACCESS_NULL;
    return TRUE;
}

/* This function updates the file, in_file, and block attributes. */
static gboolean
default_device_start_file (Device * self,
                           const dumpfile_t * jobInfo G_GNUC_UNUSED) {
    self->in_file = TRUE;
    if (self->file <= 0)
        self->file = 1;
    else
        self->file ++;
    self->block = 0;
    return TRUE;
}

/* This function lies: It updates the block number and maybe calls
   device_finish_file(), but returns FALSE. */
static gboolean
default_device_write_block(Device * self, guint size G_GNUC_UNUSED,
                           gpointer data G_GNUC_UNUSED, gboolean last_block) {
    self->block ++;
    if (last_block)
        device_finish_file(self);
    return FALSE;
}

/* This function lies: It updates the block number, but returns
   -1. */
static int
default_device_read_block(Device * self, gpointer buf G_GNUC_UNUSED,
                          int * size G_GNUC_UNUSED) {
    self->block ++;
    return -1;
}

/* This function just updates the in_file field. */
static gboolean
default_device_finish_file(Device * self) {
    self->in_file = FALSE;
    return TRUE;
}

/* This function just updates the file number. */
static dumpfile_t *
default_device_seek_file(Device * self, guint file) {
    self->in_file = TRUE;
    self->file = file;
    return NULL;
}

/* This function just updates the block number. */
static gboolean
default_device_seek_block(Device * self, guint64 block) {
    self->block = block;
    return TRUE;
}

/* This default implementation serves up static responses, and
   implements a default response to the "canonical name" property. */

static gboolean
default_device_property_get(Device * self, DevicePropertyId ID,
                            GValue * value) {
    const PropertyResponse * resp;

    resp = (PropertyResponse*)g_hash_table_lookup(selfp->property_response,
                                                  GINT_TO_POINTER(ID));
    if (resp == NULL) {
        if (ID == PROPERTY_CANONICAL_NAME) {
            g_value_unset_init(value, G_TYPE_STRING);
            g_value_set_string(value, self->device_name);
	    return TRUE;
        } else {
            return FALSE;
        }
    }

    g_value_unset_copy(&resp->response, value);

    return TRUE;
}

static gboolean
default_device_read_to_fd(Device *self, int fd) {
    return do_consumer_producer_queue(device_read_producer,
                                      self,
                                      fd_write_consumer,
                                      GINT_TO_POINTER(fd));
}

static gboolean
default_device_write_from_fd(Device *self, int fd) {
    return do_consumer_producer_queue(fd_read_producer,
                                      GINT_TO_POINTER(fd),
                                      device_write_consumer,
                                      self);
}

/* XXX WARNING XXX
 * All the functions below this comment are stub functions that do nothing
 * but implement the virtual function table. Call these functions and they
 * will do what you expect vis-a-vis virtual functions. But don't put code
 * in them beyond error checking and VFT lookup. */

gboolean 
device_open_device (Device * self, char * device_name)
{
        DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
	g_return_val_if_fail (device_name != NULL, FALSE);
	klass = DEVICE_GET_CLASS(self);

	if(klass->open_device)
            return (*klass->open_device)(self,device_name);
	else
		return FALSE;
}

ReadLabelStatusFlags device_read_label(Device * self) {
    DeviceClass * klass;
    g_return_val_if_fail(self != NULL, FALSE);
    g_return_val_if_fail(IS_DEVICE(self), FALSE);
    g_return_val_if_fail(self->access_mode == ACCESS_NULL, FALSE);

    klass = DEVICE_GET_CLASS(self);
    if (klass->read_label) {
        return (klass->read_label)(self);
    } else {
        return ~ READ_LABEL_STATUS_SUCCESS;
    }
}

gboolean
device_finish (Device * self) {
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);

        if (self->access_mode == ACCESS_NULL)
            return TRUE;

	klass = DEVICE_GET_CLASS(self);
        if (klass->finish) {
            return (*klass->finish)(self);
        } else {
            return FALSE;
        }
}

/* For a good combination of synchronization and public simplicity,
   this stub function does not take a timestamp, but the actual
   implementation function does. We generate the timestamp here with
   time(). */
gboolean 
device_start (Device * self, DeviceAccessMode mode,
              char * label, char * timestamp)
{
	DeviceClass *klass;

	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
        g_return_val_if_fail (mode != ACCESS_NULL, FALSE);
        g_return_val_if_fail (mode != ACCESS_WRITE || label != NULL,
                              FALSE);
	klass = DEVICE_GET_CLASS(self);

	if(klass->start) {
	    char * local_timestamp = NULL;
	    gboolean rv;

	    /* fill in a timestamp if none was given */
	    if (mode == ACCESS_WRITE &&
		get_timestamp_state(timestamp) == TIME_STATE_REPLACE) {
		local_timestamp = timestamp = 
		    get_proper_stamp_from_time(time(NULL));
	    }

            rv = (*klass->start)(self, mode, label, timestamp);
	    amfree(local_timestamp);
	    return rv;
        } else {
            return FALSE;
        }
}

gboolean
device_write_block (Device * self, guint size, gpointer block,
                    gboolean short_block)
{
    DeviceClass *klass;
    g_return_val_if_fail (self != NULL, FALSE);
    g_return_val_if_fail (IS_DEVICE (self), FALSE);
    g_return_val_if_fail (size > 0, FALSE);
    g_return_val_if_fail (short_block ||
                          size >= device_write_min_size(self), FALSE);
    g_return_val_if_fail (size <= device_write_max_size(self), FALSE);
    g_return_val_if_fail (block != NULL, FALSE);
    g_return_val_if_fail (IS_WRITABLE_ACCESS_MODE(self->access_mode),
                          FALSE);

    klass = DEVICE_GET_CLASS(self);
    
    if(klass->write_block)
        return (*klass->write_block)(self,size, block, short_block);
    else
        return FALSE;
}

gboolean 
device_write_from_fd (Device * self, int fd)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
	g_return_val_if_fail (fd >= 0, FALSE);
        g_return_val_if_fail (IS_WRITABLE_ACCESS_MODE(self->access_mode),
                              FALSE);

	klass = DEVICE_GET_CLASS(self);

	if(klass->write_from_fd)
		return (*klass->write_from_fd)(self,fd);
	else
		return FALSE;
}

gboolean
device_start_file (Device * self, const dumpfile_t * jobInfo) {
    DeviceClass * klass;
    g_return_val_if_fail (self != NULL, FALSE);
    g_return_val_if_fail (IS_DEVICE (self), FALSE);
    g_return_val_if_fail (!(self->in_file), FALSE);
    g_return_val_if_fail (jobInfo != NULL, FALSE);

    klass = DEVICE_GET_CLASS(self);
    
    if(klass->start_file)
        return (*klass->start_file)(self, jobInfo );
    else
        return FALSE;
}

gboolean 
device_finish_file (Device * self)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
        g_return_val_if_fail (IS_WRITABLE_ACCESS_MODE(self->access_mode),
                              FALSE);
        g_return_val_if_fail (self->in_file, FALSE);

	klass = DEVICE_GET_CLASS(self);

	if(klass->finish_file)
		return (*klass->finish_file)(self);
	else
		return FALSE;
}

dumpfile_t*
device_seek_file (Device * self, guint file)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, NULL);
	g_return_val_if_fail (IS_DEVICE (self), NULL);
        g_return_val_if_fail (self->access_mode == ACCESS_READ,
                              NULL);

	klass = DEVICE_GET_CLASS(self);

	if(klass->seek_file)
		return (*klass->seek_file)(self,file);
	else
		return FALSE;
}

gboolean 
device_seek_block (Device * self, guint64 block)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
        g_return_val_if_fail (self->access_mode == ACCESS_READ,
                              FALSE);
        g_return_val_if_fail (self->in_file, FALSE);

	klass = DEVICE_GET_CLASS(self);

	if(klass->seek_block)
		return (*klass->seek_block)(self,block);
	else
		return FALSE;
}

int
device_read_block (Device * self, gpointer buffer, int * size)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, -1);
	g_return_val_if_fail (IS_DEVICE (self), -1);
	g_return_val_if_fail (size != NULL, -1);
        g_return_val_if_fail (self->access_mode == ACCESS_READ, -1);
        if (*size != 0) {
            g_return_val_if_fail (buffer != NULL, -1);
        }

        /* Do a quick check here, so fixed-block subclasses don't have to. */
        if (*size == 0 &&
            device_write_min_size(self) == device_write_max_size(self)) {
            *size = device_write_min_size(self);
            return 0;
        }

	klass = DEVICE_GET_CLASS(self);

	if(klass->read_block)
            return (*klass->read_block)(self,buffer,size);
	else
            return -1;
}

gboolean 
device_read_to_fd (Device * self, int fd)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
	g_return_val_if_fail (fd >= 0, FALSE);
        g_return_val_if_fail (self->access_mode == ACCESS_READ, FALSE);

	klass = DEVICE_GET_CLASS(self);

	if(klass->read_to_fd)
		return (*klass->read_to_fd)(self,fd);
	else
		return FALSE;
}


gboolean 
device_property_get (Device * self, DevicePropertyId id, GValue * val)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
        g_return_val_if_fail (device_property_get_by_id(id) != NULL, FALSE);

	klass = DEVICE_GET_CLASS(self);

        /* FIXME: Check access flags? */

	if(klass->property_get)
		return (*klass->property_get)(self,id,val);
	else
		return FALSE;
}

gboolean 
device_property_set (Device * self, DevicePropertyId id, GValue * val)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);

	klass = DEVICE_GET_CLASS(self);

        /* FIXME: Check access flags? */

	if(klass->property_set)
		return (*klass->property_set)(self,id,val);
	else
		return FALSE;
}

gboolean 
device_recycle_file (Device * self, guint filenum)
{
	DeviceClass *klass;
	g_return_val_if_fail (self != NULL, FALSE);
	g_return_val_if_fail (IS_DEVICE (self), FALSE);
        g_return_val_if_fail (self->access_mode == ACCESS_APPEND, FALSE);

	klass = DEVICE_GET_CLASS(self);

	if(klass->recycle_file)
		return (*klass->recycle_file)(self,filenum);
	else
		return FALSE;
}

