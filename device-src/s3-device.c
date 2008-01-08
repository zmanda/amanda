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

/* An S3 device uses Amazon's S3 service (http://www.amazon.com/s3) to store 
 * data.  It stores data in keys named with a user-specified prefix, inside a
 * user-specified bucket.  Data is stored in the form of numbered (large) 
 * blocks. 
 */

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <regex.h>
#include <time.h>
#include "util.h"
#include "amanda.h"
#include "conffile.h"
#include "device.h"
#include "s3-device.h"
#include <curl/curl.h>
#ifdef HAVE_OPENSSL_HMAC_H
# include <openssl/hmac.h>
#else
# ifdef HAVE_CRYPTO_HMAC_H
#  include <crypto/hmac.h>
# else
#  ifdef HAVE_HMAC_H
#   include <hmac.h>
#  endif
# endif
#endif

/*
 * Constants and static data
 */

/* Maximum key length as specified in the S3 documentation
 * (*excluding* null terminator) */
#define S3_MAX_KEY_LENGTH 1024

#define S3_DEVICE_MIN_BLOCK_SIZE 1024
#define S3_DEVICE_MAX_BLOCK_SIZE (10*1024*1024)

/* This goes in lieu of file number for metadata. */
#define SPECIAL_INFIX "special-"

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

/*
 * prototypes
 */

/* 
 * utility functions */

/* Given file and block numbers, return an S3 key.
 * 
 * @param self: the S3Device object
 * @param file: the file number
 * @param block: the block within that file
 * @returns: a newly allocated string containing an S3 key.
 */
static char * 
file_and_block_to_key(S3Device *self, 
                      int file, 
                      guint64 block);

/* Given the name of a special file (such as 'tapestart'), generate
 * the S3 key to use for that file.
 *
 * @param self: the S3Device object
 * @param special_name: name of the special file
 * @param file: a file number to include; omitted if -1
 * @returns: a newly alocated string containing an S3 key.
 */
static char * 
special_file_to_key(S3Device *self, 
                    char *special_name, 
                    int file);
/* Write an amanda header file to S3.
 *
 * @param self: the S3Device object
 * @param label: the volume label
 * @param timestamp: the volume timestamp
 */
static gboolean 
write_amanda_header(S3Device *self, 
                    char *label, 
                    char * timestamp);

/* "Fast forward" this device to the end by looking up the largest file number
 * present and setting the current file number one greater.
 *
 * @param self: the S3Device object
 */
static gboolean 
seek_to_end(S3Device *self);

/* Find the number of the last file that contains any data (even just a header). 
 *
 * @param self: the S3Device object
 * @returns: the last file, or -1 in event of an error
 */
static int 
find_last_file(S3Device *self);

/* Delete all blocks in the given file, including the filestart block
 *
 * @param self: the S3Device object
 * @param file: the file to delete
 */
static gboolean 
delete_file(S3Device *self, 
            int file);

/* Set up self->s3 as best as possible.  Unless SILENT is TRUE,
 * any problems will generate warnings (with g_warning).  Regardless,
 * the return value is TRUE iff self->s3 is useable.
 *
 * @param self: the S3Device object
 * @param silent: silence warnings
 * @returns: TRUE if the handle is set up
 */
static gboolean 
setup_handle(S3Device * self, 
	     gboolean ignore_problems);

/* 
 * class mechanics */

static void
s3_device_init(S3Device * o);

static void
s3_device_class_init(S3DeviceClass * c);

static void
s3_device_finalize(GObject * o);

static Device*
s3_device_factory(char * device_type,
                  char * device_name);

/* 
 * virtual functions */

static gboolean
s3_device_open_device(Device *pself, 
                      char *device_name);

static ReadLabelStatusFlags s3_device_read_label(Device * self);

static gboolean 
s3_device_start(Device * self, 
                DeviceAccessMode mode, 
                char * label, 
                char * timestamp);

static gboolean 
s3_device_start_file(Device * self,
                     const dumpfile_t * jobInfo);

static gboolean 
s3_device_write_block(Device * self, 
                      guint size, 
                      gpointer data, 
                      gboolean last);

static gboolean 
s3_device_finish_file(Device * self);

static dumpfile_t* 
s3_device_seek_file(Device *pself, 
                    guint file);

static gboolean 
s3_device_seek_block(Device *pself, 
                     guint64 block);

static gboolean 
s3_device_read_block(Device * pself, 
                     gpointer data, 
                     int *size_req);

static gboolean 
s3_device_recycle_file(Device *pself, 
                       guint file);

static gboolean s3_device_property_set(Device * p_self, DevicePropertyId id,
                                       GValue * val);
static gboolean s3_device_property_get(Device * p_self, DevicePropertyId id,
                                       GValue * val);
/*
 * Private functions
 */

/* {{{ file_and_block_to_key */
static char *
file_and_block_to_key(S3Device *self, 
                      int file, 
                      guint64 block)
{
    char *s3_key = g_strdup_printf("%sf%08x-b%016llx.data",
                                   self->prefix, file, (long long unsigned int)block);
    g_assert(strlen(s3_key) <= S3_MAX_KEY_LENGTH);
    return s3_key;
}
/* }}} */

/* {{{ special_file_to_key */
static char *
special_file_to_key(S3Device *self, 
                    char *special_name, 
                    int file)
{
    if (file == -1)
        return g_strdup_printf("%s" SPECIAL_INFIX "%s", self->prefix, special_name);
    else
        return g_strdup_printf("%sf%08x-%s", self->prefix, file, special_name);
}
/* }}} */

/* {{{ write_amanda_header */
static gboolean
write_amanda_header(S3Device *self, 
                    char *label, 
                    char * timestamp)
{
    char * amanda_header = NULL;
    char * key = NULL;
    int header_size;
    gboolean header_fits, result;
    dumpfile_t * dumpinfo = NULL;

    /* build the header */
    dumpinfo = make_tapestart_header(DEVICE(self), label, timestamp);
    amanda_header = device_build_amanda_header(DEVICE(self), dumpinfo, 
                                               &header_size, &header_fits);
    if (!header_fits) {
        fprintf(stderr,
                _("Amanda tapestart header won't fit in a single block!\n"));
	g_free(amanda_header);
	return FALSE;
    }

    /* write out the header and flush the uploads. */
    key = special_file_to_key(self, "tapestart", -1);
    result = s3_upload(self->s3, self->bucket, key, amanda_header, header_size);
    g_free(amanda_header);
    g_free(key);

    if (!result) {
        fprintf(stderr, _("While writing amanda header: %s\n"),
                s3_strerror(self->s3));
    }
    return result;
}
/* }}} */

/* {{{ seek_to_end */
static gboolean
seek_to_end(S3Device *self) {
    int last_file;

    Device *pself = DEVICE(self);

    last_file = find_last_file(self);
    if (last_file < 0)
        return FALSE;

    pself->file = last_file;

    return TRUE;
}
/* }}} */

/* Convert an object name into a file number, assuming the given prefix
 * length. Returns -1 if the object name is invalid, or 0 if the object name
 * is a "special" key. */
static int key_to_file(guint prefix_len, const char * key) {
    int file;
    int i;
    
    /* skip the prefix */
    g_return_val_if_fail(strlen(key) > prefix_len, -1);

    key += prefix_len;

    if (strncmp(key, SPECIAL_INFIX, strlen(SPECIAL_INFIX)) == 0) {
        return 0;
    }
    
    /* check that key starts with 'f' */
    g_return_val_if_fail(key[0] == 'f', -1);
    key++;
    
    /* check that key is of the form "%08x-" */
    for (i = 0; i < 8; i++) {
        if (!(key[i] >= '0' && key[i] <= '9') &&
            !(key[i] >= 'a' && key[i] <= 'f') &&
            !(key[i] >= 'A' && key[i] <= 'F')) break;
    }
    if (key[i] != '-') return -1;
    if (i < 8) return -1;

    /* convert the file number */
    errno = 0;
    file = strtoul(key, NULL, 16);
    if (errno != 0) {
        g_warning(_("unparseable file number '%s'"), key);
        return -1;
    }
    
    return file;
}

/* {{{ find_last_file */
/* Find the number of the last file that contains any data (even just a header). 
 * Returns -1 in event of an error
 */
static int
find_last_file(S3Device *self) {
    gboolean result;
    GSList *keys;
    unsigned int prefix_len = strlen(self->prefix);
    int last_file = 0;

    /* list all keys matching C{PREFIX*-*}, stripping the C{-*} */
    result = s3_list_keys(self->s3, self->bucket, self->prefix, "-", &keys);
    if (!result) {
        fprintf(stderr, _("While listing S3 keys: %s\n"),
                s3_strerror(self->s3));
        return -1;
    }

    for (; keys; keys = g_slist_remove(keys, keys->data)) {
        int file = key_to_file(prefix_len, keys->data);

        /* and if it's the last, keep it */
        if (file > last_file)
            last_file = file;
    }

    return last_file;
}
/* }}} */

/* {{{ find_next_file */
/* Find the number of the file following the requested one, if any. 
 * Returns 0 if there is no such file or -1 in event of an error
 */
static int
find_next_file(S3Device *self, int last_file) {
    gboolean result;
    GSList *keys;
    unsigned int prefix_len = strlen(self->prefix);
    int next_file = 0;

    /* list all keys matching C{PREFIX*-*}, stripping the C{-*} */
    result = s3_list_keys(self->s3, self->bucket, self->prefix, "-", &keys);
    if (!result) {
        fprintf(stderr, _("While listing S3 keys: %s\n"),
                s3_strerror(self->s3));
        return -1;
    }

    for (; keys; keys = g_slist_remove(keys, keys->data)) {
        int file;

        file = key_to_file(prefix_len, (char*)keys->data);

        if (file < 0) {
            /* Set this in case we don't find a next file; this is not a
             * hard error, so if we can find a next file we'll return that
             * instead. */
            next_file = -1;
        }

        if (file < next_file && file > last_file) {
            next_file = file;
        }
    }

    return last_file;
}
/* }}} */

/* {{{ delete_file */
static gboolean
delete_file(S3Device *self,
            int file)
{
    gboolean result;
    GSList *keys;
    char *my_prefix = g_strdup_printf("%sf%08x-", self->prefix, file);
    
    result = s3_list_keys(self->s3, self->bucket, my_prefix, NULL, &keys);
    if (!result) {
        fprintf(stderr, _("While listing S3 keys: %s\n"),
                s3_strerror(self->s3));
        return FALSE;
    }

    /* this will likely be a *lot* of keys */
    for (; keys; keys = g_slist_remove(keys, keys->data)) {
        if (self->verbose) g_debug(_("Deleting %s"), (char*)keys->data);
        if (!s3_delete(self->s3, self->bucket, keys->data)) {
            fprintf(stderr, _("While deleting key '%s': %s\n"),
                    (char*)keys->data, s3_strerror(self->s3));
            g_slist_free(keys);
            return FALSE;
        }
    }

    return TRUE;
}
/* }}} */

/*
 * Class mechanics
 */

/* {{{ s3_device_register */
void 
s3_device_register(void)
{
    static const char * device_prefix_list[] = { "s3", NULL };
    g_assert(s3_init());
    register_device(s3_device_factory, device_prefix_list);
}
/* }}} */

/* {{{ s3_device_get_type */
GType
s3_device_get_type(void)
{
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (S3DeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) s3_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (S3Device),
            0 /* n_preallocs */,
            (GInstanceInitFunc) s3_device_init,
            NULL
        };
        
        type = g_type_register_static (TYPE_DEVICE, "S3Device", &info,
                                       (GTypeFlags)0);
    }

    return type;
}
/* }}} */

/* {{{ s3_device_init */
static void 
s3_device_init(S3Device * self)
{
    Device * o;
    DeviceProperty prop;
    GValue response;

    self->initializing = TRUE;

    /* Register property values */
    o = (Device*)(self);
    bzero(&response, sizeof(response));

    prop.base = &device_property_concurrency;
    prop.access = PROPERTY_ACCESS_GET_MASK;
    g_value_init(&response, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(&response, CONCURRENCY_PARADIGM_SHARED_READ);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
    
    prop.base = &device_property_streaming;
    g_value_init(&response, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(&response, STREAMING_REQUIREMENT_NONE);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
    
    prop.base = &device_property_block_size;
    g_value_init(&response, G_TYPE_INT);
    g_value_set_int(&response, -1); /* indicates a variable block size; see below */
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
    
    prop.base = &device_property_min_block_size;
    g_value_init(&response, G_TYPE_UINT);
    g_value_set_uint(&response, S3_DEVICE_MIN_BLOCK_SIZE);
    device_add_property(o, &prop, &response);

    prop.base = &device_property_max_block_size;
    g_value_set_uint(&response, S3_DEVICE_MAX_BLOCK_SIZE);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_appendable;
    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE);
    device_add_property(o, &prop, &response);

    prop.base = &device_property_partial_deletion;
    g_value_set_boolean(&response, TRUE);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_canonical_name;
    g_value_init(&response, G_TYPE_STRING);
    g_value_set_static_string(&response, "s3:");
    device_add_property(o, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_medium_access_type;
    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_READ_WRITE);
    device_add_property(o, &prop, &response);
    g_value_unset(&response);
    
    prop.access = PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START;
    prop.base = &device_property_s3_secret_key;
    device_add_property(o, &prop, NULL);
    prop.base = &device_property_s3_access_key;
    device_add_property(o, &prop, NULL);
#ifdef WANT_DEVPAY
    prop.base = &device_property_s3_user_token;
    device_add_property(o, &prop, NULL);
#endif
}
/* }}} */

/* {{{ s3_device_class_init */
static void 
s3_device_class_init(S3DeviceClass * c G_GNUC_UNUSED)
{
    GObjectClass *g_object_class = (GObjectClass*) c;
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->open_device = s3_device_open_device;
    device_class->read_label = s3_device_read_label;
    device_class->start = s3_device_start;

    device_class->start_file = s3_device_start_file;
    device_class->write_block = s3_device_write_block;
    device_class->finish_file = s3_device_finish_file;

    device_class->seek_file = s3_device_seek_file;
    device_class->seek_block = s3_device_seek_block;
    device_class->read_block = s3_device_read_block;
    device_class->recycle_file = s3_device_recycle_file;

    device_class->property_set = s3_device_property_set;
    device_class->property_get = s3_device_property_get;

    g_object_class->finalize = s3_device_finalize;
}
/* }}} */

/* {{{ s3_device_factory */
static Device* 
s3_device_factory(char * device_type,
                  char * device_name)
{
    Device *rval;
    S3Device * s3_rval;
    g_assert(0 == strcmp(device_type, "s3"));
    rval = DEVICE(g_object_new(TYPE_S3_DEVICE, NULL));
    s3_rval = (S3Device*)rval;

    if (!device_open_device(rval, device_name)) {
        g_object_unref(rval);
        return NULL;
    } else {
        s3_rval->initializing = FALSE;
        return rval;
    }
    
}
/* }}} */

/*
 * Virtual function overrides
 */

/* {{{ s3_device_open_device */
static gboolean 
s3_device_open_device(Device *pself, 
                      char *device_name)
{
    S3Device *self = S3_DEVICE(pself);
    char * name_colon;

    g_return_val_if_fail(self != NULL, FALSE);

    /* Device name may be bucket/prefix, to support multiple volumes in a
     * single bucket. */
    name_colon = index(device_name, '/');
    if (name_colon == NULL) {
        self->bucket = g_strdup(device_name);
        self->prefix = g_strdup("");
    } else {
        self->bucket = g_strndup(device_name, name_colon - device_name);
        self->prefix = g_strdup(name_colon + 1);
    }
    
    if (self->bucket == NULL || self->bucket[0] == '\0') {
        fprintf(stderr, _("Empty bucket name in device %s.\n"), device_name);
        amfree(self->bucket);
        amfree(self->prefix);
        return FALSE;
    }

    g_debug(_("S3 driver using bucket '%s', prefix '%s'"), self->bucket, self->prefix);

    /* default value */
    self->verbose = FALSE;

    if (parent_class->open_device) {
        parent_class->open_device(pself, device_name);
    }

    return TRUE;
}
/* }}} */

/* {{{ s3_device_finalize */
static void s3_device_finalize(GObject * obj_self) {
    S3Device *self = S3_DEVICE (obj_self);

    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    if(self->s3) s3_free(self->s3);
    if(self->bucket) g_free(self->bucket);
    if(self->prefix) g_free(self->prefix);
}
/* }}} */

static gboolean setup_handle(S3Device * self, G_GNUC_UNUSED gboolean silent) {
    if (self->s3 == NULL) {
        if (self->access_key == NULL) {
	    if (!silent) fprintf(stderr, _("No S3 access key specified\n"));
            return FALSE;
	}
	if (self->secret_key == NULL) {
	    if (!silent) fprintf(stderr, _("No S3 secret key specified\n"));
            return FALSE;
	}
#ifdef WANT_DEVPAY
	if (self->user_token == NULL) {
	    if (!silent) fprintf(stderr, _("No S3 user token specified\n"));
            return FALSE;
	}
#endif
        self->s3 = s3_open(self->access_key, self->secret_key
#ifdef WANT_DEVPAY
                           , self->user_token
#endif
                           );
        if (self->s3 == NULL) {
            fprintf(stderr, "Internal error creating S3 handle.\n");
            return FALSE;
        }
    }

    s3_verbose(self->s3, self->verbose);

    return TRUE;
}

/* {{{ s3_device_read_label */
static ReadLabelStatusFlags
s3_device_read_label(Device *pself) {
    S3Device *self = S3_DEVICE(pself);
    char *key;
    gpointer buf;
    guint buf_size;
    dumpfile_t amanda_header;
    
    if (!setup_handle(self, self->initializing))
        return READ_LABEL_STATUS_DEVICE_ERROR;

    key = special_file_to_key(self, "tapestart", -1);
    if (!s3_read(self->s3, self->bucket, key, &buf, &buf_size, S3_DEVICE_MAX_BLOCK_SIZE)) {
        guint response_code;
        s3_error_code_t s3_error_code;
        s3_error(self->s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it's an expected error (not found), just return FALSE */
        if (response_code == 404 && 
             (s3_error_code == S3_ERROR_NoSuchKey || s3_error_code == S3_ERROR_NoSuchBucket)) {
            g_debug(_("Amanda header not found while reading tapestart header (this is expected for empty tapes)"));
            return READ_LABEL_STATUS_VOLUME_UNLABELED;
        }

        /* otherwise, log it and return */
        fprintf(stderr, _("While trying to read tapestart header: %s\n"),
                s3_strerror(self->s3));
        return READ_LABEL_STATUS_DEVICE_ERROR;
    }

    g_assert(buf != NULL);
    fh_init(&amanda_header);
    parse_file_header(buf, &amanda_header, buf_size);

    g_free(buf);

    if (amanda_header.type != F_TAPESTART) {
        fprintf(stderr, _("Invalid amanda header\n"));
        return READ_LABEL_STATUS_VOLUME_ERROR;
    }

    amfree(pself->volume_label);
    pself->volume_label = g_strdup(amanda_header.name);
    amfree(pself->volume_time);
    pself->volume_time = g_strdup(amanda_header.datestamp);

    return READ_LABEL_STATUS_SUCCESS;
}
/* }}} */

/* {{{ s3_device_start */
static gboolean 
s3_device_start (Device * pself, DeviceAccessMode mode,
                 char * label, char * timestamp) {
    S3Device * self;
    int file, last_file;

    self = S3_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);

    if (!setup_handle(self, FALSE))
        return FALSE;

    /* try creating the bucket, in case it doesn't exist */
    if (mode != ACCESS_READ && !s3_make_bucket(self->s3, self->bucket)) {
        guint response_code;
        s3_error_code_t s3_error_code;
        s3_error(self->s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it isn't an expected error (bucket already exists),
         * return FALSE */
        if (response_code != 409 ||
            s3_error_code != S3_ERROR_BucketAlreadyExists) {
            fprintf(stderr, _("While creating new S3 bucket: %s\n"),
                    s3_strerror(self->s3));
            return FALSE;
        }
    }

    /* call up to the parent (Device) to set access_mode, volume_label,
     * and volume_time, either from the arguments (ACCESS_WRITE) or by 
     * reading from the 0th file (otherwise)
     */
    if (parent_class->start) 
        if (!parent_class->start((Device*)self, mode, label, timestamp))
            return FALSE;

    /* take care of any dirty work for this mode */
    switch (mode) {
        case ACCESS_READ:
            break;

        case ACCESS_WRITE:
            /* delete all files */
            last_file = find_last_file(self);
            if (last_file < 0) return FALSE;
            for (file = 0; file <= last_file; file++) {
                if (!delete_file(self, file)) return FALSE;
            }

            /* write a new amanda header */
            if (!write_amanda_header(self, label, timestamp)) {
                return FALSE;
            }
            break;

        case ACCESS_APPEND:
            return seek_to_end(self);
            break;
        case ACCESS_NULL:
            g_assert_not_reached();
    }

    g_assert(pself->access_mode == mode);

    return TRUE;
}
/* }}} */

static gboolean s3_device_property_get(Device * p_self, DevicePropertyId id,
                                       GValue * val) {
    S3Device * self;
    const DevicePropertyBase * base;

    self = S3_DEVICE(p_self);
    g_return_val_if_fail(self != NULL, FALSE);

    base = device_property_get_by_id(id);
    g_return_val_if_fail(self != NULL, FALSE);
    
    g_value_unset_init(val, base->type);
    
    if (id == PROPERTY_S3_SECRET_KEY) {
        if (self->secret_key != NULL) {
            g_value_set_string(val, self->secret_key);
            return TRUE;
        } else {
            return FALSE;
        }
    } else if (id == PROPERTY_S3_ACCESS_KEY) {
        if (self->access_key != NULL) {
            g_value_set_string(val, self->access_key);
            return TRUE;
        } else {
            return FALSE;
        }
    }
#ifdef WANT_DEVPAY
    else if (id == PROPERTY_S3_USER_TOKEN) {
        if (self->user_token != NULL) {
            g_value_set_string(val, self->user_token);
            return TRUE;
        } else {
            return FALSE;
        }
    }
#endif /* WANT_DEVPAY */
    else if (id == PROPERTY_VERBOSE) {
        g_value_set_boolean(val, self->verbose);
        return TRUE;
    } else {
        /* chain up */
        if (parent_class->property_get) {
            return (parent_class->property_get)(p_self, id, val);
        } else {
            return FALSE;
        }
    }

    g_assert_not_reached();
}

static gboolean s3_device_property_set(Device * p_self, DevicePropertyId id,
                                       GValue * val) {
    S3Device * self;
    const DevicePropertyBase * base;

    self = S3_DEVICE(p_self);
    g_return_val_if_fail(self != NULL, FALSE);

    base = device_property_get_by_id(id);
    g_return_val_if_fail(self != NULL, FALSE);

    g_return_val_if_fail(G_VALUE_HOLDS(val, base->type), FALSE);

    if (id == PROPERTY_S3_SECRET_KEY) {
        if (p_self->access_mode != ACCESS_NULL)
            return FALSE;
        amfree(self->secret_key);
        self->secret_key = g_value_dup_string(val);
        device_clear_volume_details(p_self);
        return TRUE;
    } else if (id == PROPERTY_S3_ACCESS_KEY) {
        if (p_self->access_mode != ACCESS_NULL)
            return FALSE;
        amfree(self->access_key);
        self->access_key = g_value_dup_string(val);
        device_clear_volume_details(p_self);
        return TRUE;
    }
#ifdef WANT_DEVPAY
    else if (id == PROPERTY_S3_USER_TOKEN) {
        if (p_self->access_mode != ACCESS_NULL)
            return FALSE;
        amfree(self->user_token);
        self->user_token = g_value_dup_string(val);
        device_clear_volume_details(p_self);
        return TRUE;
    }
#endif /* WANT_DEVPAY */
    else if (id == PROPERTY_VERBOSE) {
        self->verbose = g_value_get_boolean(val);
	/* Our S3 handle may not yet have been instantiated; if so, it will
	 * get the proper verbose setting when it is created */
	if (self->s3)
	    s3_verbose(self->s3, self->verbose);
	return TRUE;
    } else {
        if (parent_class->property_set) {
            return (parent_class->property_set)(p_self, id, val);
        } else {
            return FALSE;
        }
    }

    g_assert_not_reached();
}

/* functions for writing */

/* {{{ s3_device_start_file */

static gboolean
s3_device_start_file (Device *pself, const dumpfile_t *jobInfo) {
    S3Device *self = S3_DEVICE(pself);
    char *amanda_header;
    int header_size;
    gboolean header_fits, result;
    char *key;

    g_return_val_if_fail (self != NULL, FALSE);

    /* Build the amanda header. */
    amanda_header = device_build_amanda_header(pself, jobInfo,
                                               &header_size, &header_fits);
    g_return_val_if_fail(amanda_header != NULL, FALSE);
    g_return_val_if_fail(header_fits, FALSE);

    /* set the file and block numbers correctly */
    pself->file = (pself->file > 0)? pself->file+1 : 1;
    pself->block = 0;
    pself->in_file = TRUE;

    /* write it out as a special block (not the 0th) */
    key = special_file_to_key(self, "filestart", pself->file);
    result = s3_upload(self->s3, self->bucket, key, amanda_header, header_size);
    g_free(amanda_header);
    g_free(key);
    if (!result) {
        fprintf(stderr, _("While writing filestart header: %s\n"),
                s3_strerror(self->s3));
        return FALSE;
    }

    return TRUE;
}
/* }}} */

/* {{{ s3_device_write_block */
static gboolean
s3_device_write_block (Device * pself, guint size, gpointer data,
                         gboolean last_block) {
    gboolean result;
    char *filename;
    S3Device * self = S3_DEVICE(pself);;

    g_assert (self != NULL);
    g_assert (data != NULL);
    
    filename = file_and_block_to_key(self, pself->file, pself->block);

    result = s3_upload(self->s3, self->bucket, filename, data, size);
    g_free(filename);
    if (!result) {
        fprintf(stderr, _("While writing data block to S3: %s\n"),
                s3_strerror(self->s3));
        return FALSE;
    }

    pself->block++;

    /* if this is the last block, finish the file */
    if (last_block) {
        return s3_device_finish_file(pself);
    }

    return TRUE;
}
/* }}} */

/* {{{ s3_device_finish_file */
static gboolean
s3_device_finish_file (Device * pself) {
    /* we're not in a file anymore */
    pself->in_file = FALSE;

    return TRUE;
}
/* }}} */

/* {{{ s3_device_recycle_file */
static gboolean
s3_device_recycle_file(Device *pself, guint file) {
    S3Device *self = S3_DEVICE(pself);

    return delete_file(self, file);
}
/* }}} */

/* functions for reading */

/* {{{ s3_device_seek_file */
static dumpfile_t*
s3_device_seek_file(Device *pself, guint file) {
    S3Device *self = S3_DEVICE(pself);
    gboolean result;
    char *key;
    gpointer buf;
    guint buf_size;
    dumpfile_t *amanda_header;

    pself->file = file;
    pself->block = 0;
    pself->in_file = TRUE;

    /* read it in */
    key = special_file_to_key(self, "filestart", pself->file);
    result = s3_read(self->s3, self->bucket, key, &buf, &buf_size, S3_DEVICE_MAX_BLOCK_SIZE);
    g_free(key);
 
    if (!result) {
        guint response_code;
        s3_error_code_t s3_error_code;
        s3_error(self->s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it's an expected error (not found), check what to do. */
        if (response_code == 404 && s3_error_code == S3_ERROR_NoSuchKey) {
            int next_file;
            pself->file = -1;
            pself->in_file = FALSE;
            next_file = find_next_file(self, pself->file);
            if (next_file > 0) {
                /* Note short-circut of dispatcher. */
                return s3_device_seek_file(pself, next_file);
            } else if (next_file == 0) {
                /* No next file. Check if we are one past the end. */
                key = special_file_to_key(self, "filestart", pself->file - 1);
                result = s3_read(self->s3, self->bucket, key, &buf, &buf_size,
                                 S3_DEVICE_MAX_BLOCK_SIZE);
                g_free(key);
                if (result) {
                    return make_tapeend_header();
                } else {
                    return NULL;
                }
            }
        } else {
            /* An error occured finding out if we are the last file. */
            return NULL;
        }
    }
   
    /* and make a dumpfile_t out of it */
    g_assert(buf != NULL);
    amanda_header = g_new(dumpfile_t, 1);
    fh_init(amanda_header);
    parse_file_header(buf, amanda_header, buf_size);
    g_free(buf);

    switch (amanda_header->type) {
        case F_DUMPFILE:
        case F_CONT_DUMPFILE:
        case F_SPLIT_DUMPFILE:
            return amanda_header;

        default:
            fprintf(stderr,
                    _("Invalid amanda header while reading file header\n"));
            g_free(amanda_header);
            return NULL;
    }
}
/* }}} */

/* {{{ s3_device_seek_block */
static gboolean
s3_device_seek_block(Device *pself, guint64 block) {
    pself->block = block;
    return TRUE;
}
/* }}} */

/* {{{ s3_device_read_block */
static int
s3_device_read_block (Device * pself, gpointer data, int *size_req) {
    S3Device * self = S3_DEVICE(pself);
    char *key;
    gpointer buf;
    gboolean result;
    guint buf_size;

    g_assert (self != NULL);

    /* get the file*/
    key = file_and_block_to_key(self, pself->file, pself->block);
    g_assert(key != NULL);
    if (self->cached_key && (0 == strcmp(key, self->cached_key))) {
        /* use the cached copy and clear the cache */
        buf = self->cached_buf;
        buf_size = self->cached_size;

        self->cached_buf = NULL;
        g_free(self->cached_key);
        self->cached_key = NULL;
    } else {
        /* clear the cache and actually download the file */
        if (self->cached_buf) {
            g_free(self->cached_buf);
            self->cached_buf = NULL;
        }
        if (self->cached_key) {
            g_free(self->cached_key);
            self->cached_key = NULL;
        }

        result = s3_read(self->s3, self->bucket, key, &buf, &buf_size, S3_DEVICE_MAX_BLOCK_SIZE);
        if (!result) {
            guint response_code;
            s3_error_code_t s3_error_code;
            s3_error(self->s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

            g_free(key);
            key = NULL;

            /* if it's an expected error (not found), just return -1 */
            if (response_code == 404 && s3_error_code == S3_ERROR_NoSuchKey) {
                pself->is_eof = TRUE;
                return -1;
            }

            /* otherwise, log it and return FALSE */
            fprintf(stderr, _("While reading data block from S3: %s\n"),
                    s3_strerror(self->s3));
            return -1;
        }
    }

    /* INVARIANT: cache is NULL */
    g_assert(self->cached_buf == NULL);
    g_assert(self->cached_key == NULL);

    /* now see how the caller wants to deal with that */
    if (data == NULL || *size_req < 0 || buf_size > (guint)*size_req) {
        /* A size query or short buffer -- load the cache and return the size*/
        self->cached_buf = buf;
        self->cached_key = key;
        self->cached_size = buf_size;

        *size_req = buf_size;
        return 0;
    } else {
        /* ok, all checks are passed -- copy the data */
        *size_req = buf_size;
        g_memmove(data, buf, buf_size);
        g_free(key);
        g_free(buf);

        /* move on to the next block */
        pself->block++;

        return buf_size;
    }
}
/* }}} */
