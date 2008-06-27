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

/* The Device API abstracts device workings, interaction, properties, and
 * capabilities from the rest of the Amanda code base. It supports
 * pluggable modules for different kinds of devices. */

#include "amanda.h"
#include "conffile.h"

#include <regex.h>

#include "device.h"
#include "queueing.h"
#include "device-queueing.h"
#include "property.h"

#include "timestamp.h"
#include "util.h"

/*
 * Prototypes for subclass registration functions
 */

void    null_device_register    (void);
void	rait_device_register	(void);
#ifdef WANT_S3_DEVICE
void    s3_device_register    (void);
#endif
#ifdef WANT_TAPE_DEVICE
void    tape_device_register    (void);
#endif
void    vfs_device_register     (void);

/*
 * Registration infrastructure
 */

static GHashTable* driverList = NULL;

void device_api_init(void) {
    glib_init();
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

static DeviceFactory lookup_device_factory(const char *device_type) {
    gpointer key, value;
    g_assert(driverList != NULL);

    if (g_hash_table_lookup_extended(driverList, device_type, &key, &value)) {
        return (DeviceFactory)value;
    } else {
        return NULL;
    }
}

static const GFlagsValue device_status_flags_values[] = {
    { DEVICE_STATUS_SUCCESS,
      "DEVICE_STATUS_SUCCESS",
      "Success" },
    { DEVICE_STATUS_DEVICE_ERROR,
      "DEVICE_STATUS_DEVICE_ERROR",
      "Device error" },
    { DEVICE_STATUS_DEVICE_BUSY,
      "DEVICE_STATUS_DEVICE_BUSY",
      "Device busy" },
    { DEVICE_STATUS_VOLUME_MISSING,
      "DEVICE_STATUS_VOLUME_MISSING",
      "Volume not found" },
    { DEVICE_STATUS_VOLUME_UNLABELED,
      "DEVICE_STATUS_VOLUME_UNLABELED",
      "Volume not labeled" },
    { DEVICE_STATUS_VOLUME_ERROR,
      "DEVICE_STATUS_VOLUME_ERROR",
      "Volume error" },
    { 0, NULL, NULL }
};

GType device_status_flags_get_type(void) {
    static GType type = 0;
    if (G_UNLIKELY(type == 0)) {
        type = g_flags_register_static("DeviceStatusFlags",
                                       device_status_flags_values);
    }
    return type;
}

/* Device class definition starts here. */

struct DevicePrivate_s {
    /* This is the return value of the device_get_property_list()
       method. */
    GArray *property_list;
    GHashTable * property_response;

    /* Holds an error message if the function returned an error. */
    char * errmsg;

    /* temporary holding place for device_status_error() */
    char * statusmsg;
    DeviceStatusFlags last_status;
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

static void default_device_open_device(Device * self, char * device_name,
				    char * device_type, char * device_node);
static gboolean default_device_write_from_fd(Device *self,
					     queue_fd_t *queue_fd);
static gboolean default_device_read_to_fd(Device *self, queue_fd_t *queue_fd);
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
    amfree(self->volume_header);
    amfree(selfp->errmsg);
    amfree(selfp->statusmsg);
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
    self->status = DEVICE_STATUS_SUCCESS;
    selfp->errmsg = NULL;
    selfp->statusmsg = NULL;
    selfp->last_status = 0;
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
    c->write_from_fd = default_device_write_from_fd;
    c->read_to_fd = default_device_read_to_fd;
    c->property_get = default_device_property_get;
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
                    char ** device, char **errmsg) {
    regex_t regex;
    int reg_result;
    regmatch_t pmatch[3];
    static const char * regex_string = "^([a-z0-9]+):(.*)$";

    bzero(&regex, sizeof(regex));

    reg_result = regcomp(&regex, regex_string, REG_EXTENDED | REG_ICASE);
    if (reg_result != 0) {
        char * message = regex_message(reg_result, &regex);
	*errmsg = newvstrallocf(*errmsg, "Error compiling regular expression \"%s\": %s\n",
			      regex_string, message);
	amfree(message);
        return FALSE;
    }

    reg_result = regexec(&regex, user_name, 3, pmatch, 0);
    if (reg_result != 0 && reg_result != REG_NOMATCH) {
        char * message = regex_message(reg_result, &regex);
	*errmsg = newvstrallocf(*errmsg,
			"Error applying regular expression \"%s\" to string \"%s\": %s\n",
			user_name, regex_string, message);
	amfree(message);
        regfree(&regex);
        return FALSE;
    } else if (reg_result == REG_NOMATCH) {
#ifdef WANT_TAPE_DEVICE
	g_warning(
		"\"%s\" uses deprecated device naming convention; \n"
                "using \"tape:%s\" instead.\n",
                user_name, user_name);
        *driver_name = stralloc("tape");
        *device = stralloc(user_name);
#else /* !WANT_TAPE_DEVICE */
	errmsg = newvstrallocf(errmsg, "\"%s\" is not a valid device name.\n", user_name);
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

/* helper function for device_open */
static Device *
make_null_error(char *errmsg, DeviceStatusFlags status)
{
    DeviceFactory factory;
    Device *device;

    factory = lookup_device_factory("null");
    g_assert(factory != NULL);

    device = factory("null:", "null", "");
    device_set_error(device, errmsg, status);

    return device;
}

Device* 
device_open (char * device_name)
{
    char *device_type = NULL;
    char *device_node = NULL;
    char *errmsg = NULL;
    DeviceFactory factory;
    Device *device;


    g_assert(device_name != NULL);

    if (driverList == NULL) {
        g_critical("device_open() called without device_api_init()!");
        g_assert_not_reached();
    }

    if (device_name == NULL)
	return make_null_error(stralloc(_("No device name specified")), DEVICE_STATUS_DEVICE_ERROR);

    if (!handle_device_regex(device_name, &device_type, &device_node,
			     &errmsg)) {
        amfree(device_type);
        amfree(device_node);
	return make_null_error(errmsg, DEVICE_STATUS_DEVICE_ERROR);
    }

    factory = lookup_device_factory(device_type);

    if (factory == NULL) {
	Device *nulldev = make_null_error(vstrallocf(_("Device type %s is not known."),
	    device_type), DEVICE_STATUS_DEVICE_ERROR);
	amfree(device_type);
	amfree(device_node);
	return nulldev;
    }

    device = factory(device_name, device_type, device_node);
    g_assert(device != NULL); /* factories must always return a device */

    amfree(device_type);
    amfree(device_node);
    return device;
}

char *
device_error(Device * self)
{
    if (selfp->errmsg)
	return selfp->errmsg;
    return "Unknown Device error";
}

char *
device_status_error(Device * self)
{
    char **status_strv;
    char *statusmsg;

    /* reuse a previous statusmsg, if it was for the same status */
    if (selfp->statusmsg && selfp->last_status == self->status)
	return selfp->statusmsg;

    amfree(selfp->statusmsg);

    status_strv = g_flags_nick_to_strv(self->status, DEVICE_STATUS_FLAGS_TYPE);
    g_assert(g_strv_length(status_strv) > 0);
    if (g_strv_length(status_strv) == 1) {
	statusmsg = stralloc(*status_strv);
    } else {
	char * status_list = g_english_strjoinv(status_strv, "or");
	statusmsg = g_strdup_printf("one of %s", status_list);
	amfree(status_list);
    }
    g_strfreev(status_strv);

    selfp->statusmsg = statusmsg;
    selfp->last_status = self->status;
    return statusmsg;
}

char *
device_error_or_status(Device * self)
{
    if (selfp->errmsg)
	return selfp->errmsg;
    else
	return device_status_error(self);
}

void
device_set_error(Device *self, char *errmsg, DeviceStatusFlags new_flags)
{
    char **flags_strv;
    char *flags_str;
    char *device_name;

    if (!self) {
	g_warning("device_set_error called with a NULL device: '%s'", errmsg? errmsg:"(NULL)");
	amfree(errmsg);
	return;
    }

    device_name = self->device_name? self->device_name : "(unknown device)";

    if (errmsg && (!selfp->errmsg || strcmp(errmsg, selfp->errmsg) != 0))
	g_debug("Device %s error = '%s'", device_name, errmsg);

    amfree(selfp->errmsg);
    selfp->errmsg = errmsg;

    if (new_flags != DEVICE_STATUS_SUCCESS) {
	flags_strv = g_flags_name_to_strv(new_flags, DEVICE_STATUS_FLAGS_TYPE);
	g_assert(g_strv_length(flags_strv) > 0);
	flags_str = g_english_strjoinv(flags_strv, "and");
	g_debug("Device %s setting status flag(s): %s", device_name, flags_str);
	amfree(flags_str);
	g_strfreev(flags_strv);
    }

    self->status = new_flags;
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
	g_assert(IS_DEVICE(self));

        return (const DeviceProperty*) selfp->property_list->data;
}

size_t device_write_min_size(Device * self) {
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

size_t device_write_max_size(Device * self) {
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

size_t device_read_max_size(Device * self) {
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
    size_t min_header_length;
    size_t header_buffer_size;

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

    g_assert(label != NULL);

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
    char   * property_s = key_p;
    GSList * value_s = value_p;
    Device * device = user_data_p;
    const DevicePropertyBase* property_base;
    GValue property_value;
    char   * value;

    g_return_if_fail(IS_DEVICE(device));
    g_return_if_fail(property_s != NULL);
    g_return_if_fail(value_s != NULL);

    property_base = device_property_get_by_name(property_s);
    if (property_base == NULL) {
        /* Nonexistant property name. */
        g_fprintf(stderr, _("Unknown device property name %s.\n"), property_s);
        return;
    }
    if (g_slist_length(value_s) > 1) {
	g_fprintf(stderr,
		  _("Multiple value for property name %s.\n"), property_s);
	return;
    }
    
    bzero(&property_value, sizeof(property_value));
    g_value_init(&property_value, property_base->type);
    value = value_s->data;
    if (!g_value_set_from_string(&property_value, value)) {
        /* Value type could not be interpreted. */
        g_fprintf(stderr,
                _("Could not parse property value %s for property type %s.\n"),
                value, g_type_name(property_base->type));
        return;
    } else {
        g_assert (G_VALUE_HOLDS(&property_value, property_base->type));
    }

    if (!device_property_set(device, property_base->ID, &property_value)) {
        /* Device rejects property. */
        g_fprintf(stderr, _("Could not set property %s to %s on device %s.\n"),
                property_base->name, value, device->device_name);
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

static void default_device_open_device(Device * self, char * device_name,
		    char * device_type G_GNUC_UNUSED, char * device_node G_GNUC_UNUSED) {
    DeviceProperty prop;
    guint i;

    /* Set the device_name property */
    self->device_name = stralloc(device_name);

    /* And add canonical_name to the property_list if it's not
     * already present */
    prop.base = &device_property_canonical_name;
    prop.access = PROPERTY_ACCESS_GET_MASK;
    for(i = 0; i < selfp->property_list->len; i ++) {
        if (g_array_index(selfp->property_list,
                          DeviceProperty, i).base->ID == prop.base->ID) {
            break;
        }
    }
    if (i >= selfp->property_list->len)
	/* not found, so add it */
	device_add_property(self, &prop, NULL);
}

/* This default implementation serves up static responses, and
   implements a default response to the "canonical name" property. */

static gboolean
default_device_property_get(Device * self, DevicePropertyId ID,
                            GValue * value) {
    const PropertyResponse * resp;
    if (device_in_error(self)) return FALSE;

    /* look up any static responses in property_response */
    resp = (PropertyResponse*)g_hash_table_lookup(selfp->property_response,
                                                  GINT_TO_POINTER(ID));

    /* if we didn't find anything, and the request was for something
     * we can generate, do so */
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
default_device_read_to_fd(Device *self, queue_fd_t *queue_fd) {
    GValue val;
    StreamingRequirement streaming_mode;

    if (device_in_error(self)) return FALSE;

    /* Get the device's parameters */
    bzero(&val, sizeof(val));
    if (!device_property_get(self, PROPERTY_STREAMING, &val)
	|| !G_VALUE_HOLDS(&val, STREAMING_REQUIREMENT_TYPE)) {
	streaming_mode = STREAMING_REQUIREMENT_REQUIRED;
    } else {
	streaming_mode = g_value_get_enum(&val);
    }

    return QUEUE_SUCCESS ==
	do_consumer_producer_queue_full(
	    device_read_producer,
	    self,
	    fd_write_consumer,
	    queue_fd,
	    device_read_max_size(self),
	    DEFAULT_MAX_BUFFER_MEMORY,
	    streaming_mode);
}

static gboolean
default_device_write_from_fd(Device *self, queue_fd_t *queue_fd) {
    GValue val;
    StreamingRequirement streaming_mode;

    if (device_in_error(self)) return FALSE;

    /* Get the device's parameters */
    bzero(&val, sizeof(val));
    if (!device_property_get(self, PROPERTY_STREAMING, &val)
	|| !G_VALUE_HOLDS(&val, STREAMING_REQUIREMENT_TYPE)) {
	streaming_mode = STREAMING_REQUIREMENT_REQUIRED;
    } else {
	streaming_mode = g_value_get_enum(&val);
    }

    return QUEUE_SUCCESS ==
	do_consumer_producer_queue_full(
	    fd_read_producer,
	    queue_fd,
	    device_write_consumer,
	    self,
	    device_write_max_size(self),
	    DEFAULT_MAX_BUFFER_MEMORY,
	    streaming_mode);
}

/* XXX WARNING XXX
 * All the functions below this comment are stub functions that do nothing
 * but implement the virtual function table. Call these functions and they
 * will do what you expect vis-a-vis virtual functions. But don't put code
 * in them beyond error checking and VFT lookup. */

void
device_open_device (Device * self, char * device_name,
	char * device_type, char * device_node)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE(self));
    g_assert(device_name != NULL);

    klass = DEVICE_GET_CLASS(self);
    if (klass->open_device)
	(klass->open_device)(self, device_name, device_type, device_node);
}

DeviceStatusFlags device_read_label(Device * self) {
    DeviceClass * klass;

    g_assert(self != NULL);
    g_assert(IS_DEVICE(self));
    g_assert(self->access_mode == ACCESS_NULL);

    klass = DEVICE_GET_CLASS(self);
    if (klass->read_label) {
        return (klass->read_label)(self);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
        return ~ DEVICE_STATUS_SUCCESS;
    }
}

gboolean
device_finish (Device * self) {
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));

    klass = DEVICE_GET_CLASS(self);
    if (klass->finish) {
	return (klass->finish)(self);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean 
device_start (Device * self, DeviceAccessMode mode,
              char * label, char * timestamp)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(mode != ACCESS_NULL);
    g_assert(mode != ACCESS_WRITE || label != NULL);

    klass = DEVICE_GET_CLASS(self);
    if(klass->start) {
	char * local_timestamp = NULL;
	gboolean rv;

	/* For a good combination of synchronization and public simplicity,
	   this stub function does not require a timestamp, but the actual
	   implementation function does. We generate the timestamp here with
	   time(). */
	if (mode == ACCESS_WRITE &&
	    get_timestamp_state(timestamp) == TIME_STATE_REPLACE) {
	    local_timestamp = timestamp =
		get_proper_stamp_from_time(time(NULL));
	}

	rv = (klass->start)(self, mode, label, timestamp);
	amfree(local_timestamp);
	return rv;
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean
device_write_block (Device * self, guint size, gpointer block,
                    gboolean short_block)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(size > 0);

    /* these are all things that the caller should take care to
     * guarantee, so we just assert them here */
    g_assert(short_block || size >= device_write_min_size(self));
    g_assert(size <= device_write_max_size(self));
    g_assert(block != NULL);
    g_assert(IS_WRITABLE_ACCESS_MODE(self->access_mode));
    g_assert(self->in_file);

    klass = DEVICE_GET_CLASS(self);
    if(klass->write_block) {
        return (*klass->write_block)(self,size, block, short_block);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean 
device_write_from_fd (Device * self, queue_fd_t * queue_fd)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(queue_fd->fd >= 0);
    g_assert(IS_WRITABLE_ACCESS_MODE(self->access_mode));

    klass = DEVICE_GET_CLASS(self);
    if(klass->write_from_fd) {
	return (klass->write_from_fd)(self,queue_fd);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean
device_start_file (Device * self, const dumpfile_t * jobInfo) {
    DeviceClass * klass;

    g_assert(IS_DEVICE (self));
    g_assert(!(self->in_file));
    g_assert(jobInfo != NULL);

    klass = DEVICE_GET_CLASS(self);
    if(klass->start_file) {
        return (klass->start_file)(self, jobInfo );
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean 
device_finish_file (Device * self)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(IS_WRITABLE_ACCESS_MODE(self->access_mode));
    g_assert(self->in_file);

    klass = DEVICE_GET_CLASS(self);
    if(klass->finish_file) {
	return (klass->finish_file)(self);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

dumpfile_t*
device_seek_file (Device * self, guint file)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(self->access_mode == ACCESS_READ);

    klass = DEVICE_GET_CLASS(self);
    if(klass->seek_file) {
	return (klass->seek_file)(self,file);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean 
device_seek_block (Device * self, guint64 block)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(self->access_mode == ACCESS_READ);
    g_assert(self->in_file);

    klass = DEVICE_GET_CLASS(self);
    if(klass->seek_block) {
	return (klass->seek_block)(self,block);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

int
device_read_block (Device * self, gpointer buffer, int * size)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(size != NULL);
    g_assert(self->access_mode == ACCESS_READ);

    if (*size != 0) {
	g_assert(buffer != NULL);
    }

    klass = DEVICE_GET_CLASS(self);
    if(klass->read_block) {
	return (klass->read_block)(self,buffer,size);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return -1;
    }
}

gboolean 
device_read_to_fd (Device * self, queue_fd_t *queue_fd)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(queue_fd->fd >= 0);
    g_assert(self->access_mode == ACCESS_READ);

    klass = DEVICE_GET_CLASS(self);
    if(klass->read_to_fd) {
	return (klass->read_to_fd)(self,queue_fd);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}


gboolean 
device_property_get (Device * self, DevicePropertyId id, GValue * val)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));
    g_assert(device_property_get_by_id(id) != NULL);

    klass = DEVICE_GET_CLASS(self);

    if(klass->property_get) {
	return (klass->property_get)(self,id,val);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean 
device_property_set (Device * self, DevicePropertyId id, GValue * val)
{
    DeviceClass *klass;

    g_assert(IS_DEVICE (self));

    klass = DEVICE_GET_CLASS(self);

    if(klass->property_set) {
	return (klass->property_set)(self,id,val);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

gboolean 
device_recycle_file (Device * self, guint filenum)
{
    DeviceClass *klass;

    g_assert(self != NULL);
    g_assert(IS_DEVICE (self));
    g_assert(self->access_mode == ACCESS_APPEND);
    g_assert(!self->in_file);

    klass = DEVICE_GET_CLASS(self);

    if(klass->recycle_file) {
	return (klass->recycle_file)(self,filenum);
    } else {
	device_set_error(self,
	    stralloc(_("Unimplemented method")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
}

