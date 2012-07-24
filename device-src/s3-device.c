/*
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* An S3 device uses Amazon's S3 service (http://www.amazon.com/s3) to store
 * data.  It stores data in keys named with a user-specified prefix, inside a
 * user-specified bucket.  Data is stored in the form of numbered (large)
 * blocks.
 */

#include "amanda.h"
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <regex.h>
#include <time.h>
#include "util.h"
#include "conffile.h"
#include "device.h"
#include "s3.h"
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
 * Type checking and casting macros
 */
#define TYPE_S3_DEVICE	(s3_device_get_type())
#define S3_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), s3_device_get_type(), S3Device)
#define S3_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), s3_device_get_type(), S3Device const)
#define S3_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), s3_device_get_type(), S3DeviceClass)
#define IS_S3_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), s3_device_get_type ())

#define S3_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), s3_device_get_type(), S3DeviceClass)
static GType	s3_device_get_type	(void);

/*
 * Main object structure
 */
typedef struct _S3MetadataFile S3MetadataFile;
typedef struct _S3Device S3Device;

typedef struct _S3_by_thread S3_by_thread;
struct _S3_by_thread {
    S3Handle * volatile          s3;
    CurlBuffer volatile          curl_buffer;
    guint volatile               buffer_len;
    int volatile                 idle;
    int volatile                 eof;
    int volatile                 done;
    char volatile * volatile     filename;
    DeviceStatusFlags volatile   errflags;	/* device_status */
    char volatile * volatile     errmsg;	/* device error message */
    GMutex			*now_mutex;
    guint64			 dlnow, ulnow;
};

struct _S3Device {
    Device __parent__;

    /* The "easy" curl handle we use to access Amazon S3 */
    S3_by_thread *s3t;

    /* S3 access information */
    char *bucket;
    char *prefix;

    /* The S3 access information. */
    char *secret_key;
    char *access_key;
    char *user_token;

    /* The Openstack swift information. */
    char *swift_account_id;
    char *swift_access_key;

    char *username;
    char *password;
    char *tenant_id;
    char *tenant_name;

    char *bucket_location;
    char *storage_class;
    char *host;
    char *service_path;
    char *server_side_encryption;
    char *proxy;

    char *ca_info;

    /* a cache for unsuccessful reads (where we get the file but the caller
     * doesn't have space for it or doesn't want it), where we expect the
     * next call will request the same file.
     */
    char *cached_buf;
    char *cached_key;
    int cached_size;

    /* Produce verbose output? */
    gboolean verbose;

    /* create the bucket? */
    gboolean create_bucket;

    /* Use SSL? */
    gboolean use_ssl;
    S3_api s3_api;

    /* Throttling */
    guint64 max_send_speed;
    guint64 max_recv_speed;

    gboolean leom;
    guint64 volume_bytes;
    guint64 volume_limit;
    gboolean enforce_volume_limit;
    gboolean use_subdomain;
    gboolean use_s3_multi_delete;

    int          nb_threads;
    int          nb_threads_backup;
    int          nb_threads_recovery;
    GThreadPool *thread_pool_delete;
    GThreadPool *thread_pool_write;
    GThreadPool *thread_pool_read;
    GCond       *thread_idle_cond;
    GMutex      *thread_idle_mutex;
    int          next_block_to_read;
    GSList      *keys;

    guint64      dltotal;
    guint64      ultotal;

    /* google OAUTH2 */
    char        *client_id;
    char        *client_secret;
    char        *refresh_token;
    char        *project_id;

    gboolean	 reuse_connection;
};

/*
 * Class definition
 */
typedef struct _S3DeviceClass S3DeviceClass;
struct _S3DeviceClass {
    DeviceClass __parent__;
};


/*
 * Constants and static data
 */

#define S3_DEVICE_NAME "s3"

/* Maximum key length as specified in the S3 documentation
 * (*excluding* null terminator) */
#define S3_MAX_KEY_LENGTH 1024

/* Note: for compatability, min can only be decreased and max increased */
#define S3_DEVICE_MIN_BLOCK_SIZE 1024
#define S3_DEVICE_MAX_BLOCK_SIZE (3*1024*1024*1024ULL)
#define S3_DEVICE_DEFAULT_BLOCK_SIZE (10*1024*1024)
#define EOM_EARLY_WARNING_ZONE_BLOCKS 4

/* This goes in lieu of file number for metadata. */
#define SPECIAL_INFIX "special-"

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

/*
 * device-specific properties
 */

/* Authentication information for Amazon S3. Both of these are strings. */
static DevicePropertyBase device_property_s3_access_key;
static DevicePropertyBase device_property_s3_secret_key;
#define PROPERTY_S3_SECRET_KEY (device_property_s3_secret_key.ID)
#define PROPERTY_S3_ACCESS_KEY (device_property_s3_access_key.ID)

/* Authentication information for Openstack Swift. Both of these are strings. */
static DevicePropertyBase device_property_swift_account_id;
static DevicePropertyBase device_property_swift_access_key;
#define PROPERTY_SWIFT_ACCOUNT_ID (device_property_swift_account_id.ID)
#define PROPERTY_SWIFT_ACCESS_KEY (device_property_swift_access_key.ID)

/* Authentication information for Openstack Swift. Both of these are strings. */
static DevicePropertyBase device_property_username;
static DevicePropertyBase device_property_password;
static DevicePropertyBase device_property_tenant_id;
static DevicePropertyBase device_property_tenant_name;
#define PROPERTY_USERNAME (device_property_username.ID)
#define PROPERTY_PASSWORD (device_property_password.ID)
#define PROPERTY_TENANT_ID (device_property_tenant_id.ID)
#define PROPERTY_TENANT_NAME (device_property_tenant_name.ID)

/* Host and path */
static DevicePropertyBase device_property_s3_host;
static DevicePropertyBase device_property_s3_service_path;
#define PROPERTY_S3_HOST (device_property_s3_host.ID)
#define PROPERTY_S3_SERVICE_PATH (device_property_s3_service_path.ID)

/* Same, but for S3 with DevPay. */
static DevicePropertyBase device_property_s3_user_token;
#define PROPERTY_S3_USER_TOKEN (device_property_s3_user_token.ID)

/* Location constraint for new buckets created on Amazon S3. */
static DevicePropertyBase device_property_s3_bucket_location;
#define PROPERTY_S3_BUCKET_LOCATION (device_property_s3_bucket_location.ID)

/* Storage class */
static DevicePropertyBase device_property_s3_storage_class;
#define PROPERTY_S3_STORAGE_CLASS (device_property_s3_storage_class.ID)

/* Server side encryption */
static DevicePropertyBase device_property_s3_server_side_encryption;
#define PROPERTY_S3_SERVER_SIDE_ENCRYPTION (device_property_s3_server_side_encryption.ID)

/* proxy */
static DevicePropertyBase device_property_proxy;
#define PROPERTY_PROXY (device_property_proxy.ID)

/* Path to certificate authority certificate */
static DevicePropertyBase device_property_ssl_ca_info;
#define PROPERTY_SSL_CA_INFO (device_property_ssl_ca_info.ID)

/* Which strotage api to use. */
static DevicePropertyBase device_property_storage_api;
#define PROPERTY_STORAGE_API (device_property_storage_api.ID)

/* Whether to use openstack protocol. */
/* DEPRECATED */
static DevicePropertyBase device_property_openstack_swift_api;
#define PROPERTY_OPENSTACK_SWIFT_API (device_property_openstack_swift_api.ID)

/* Whether to use SSL with Amazon S3. */
static DevicePropertyBase device_property_s3_ssl;
#define PROPERTY_S3_SSL (device_property_s3_ssl.ID)

/* Whether to re-use connection. */
static DevicePropertyBase device_property_reuse_connection;
#define PROPERTY_REUSE_CONNECTION (device_property_reuse_connection.ID)

/* Speed limits for sending and receiving */
static DevicePropertyBase device_property_max_send_speed;
static DevicePropertyBase device_property_max_recv_speed;
#define PROPERTY_MAX_SEND_SPEED (device_property_max_send_speed.ID)
#define PROPERTY_MAX_RECV_SPEED (device_property_max_recv_speed.ID)

/* Whether to use subdomain */
static DevicePropertyBase device_property_s3_subdomain;
#define PROPERTY_S3_SUBDOMAIN (device_property_s3_subdomain.ID)

/* Number of threads to use */
static DevicePropertyBase device_property_nb_threads_backup;
#define PROPERTY_NB_THREADS_BACKUP (device_property_nb_threads_backup.ID)
static DevicePropertyBase device_property_nb_threads_recovery;
#define PROPERTY_NB_THREADS_RECOVERY (device_property_nb_threads_recovery.ID)

/* If the s3 server have the multi-delete functionality */
static DevicePropertyBase device_property_s3_multi_delete;
#define PROPERTY_S3_MULTI_DELETE (device_property_s3_multi_delete.ID)

/* The client_id for OAUTH2 */
static DevicePropertyBase device_property_client_id;
#define PROPERTY_CLIENT_ID (device_property_client_id.ID)

/* The client_secret for OAUTH2 */
static DevicePropertyBase device_property_client_secret;
#define PROPERTY_CLIENT_SECRET (device_property_client_secret.ID)

/* The refresh token for OAUTH2 */
static DevicePropertyBase device_property_refresh_token;
#define PROPERTY_REFRESH_TOKEN (device_property_refresh_token.ID)

/* The PROJECT ID */
static DevicePropertyBase device_property_project_id;
#define PROPERTY_PROJECT_ID (device_property_project_id.ID)

/* The PROJECT ID */
static DevicePropertyBase device_property_create_bucket;
#define PROPERTY_CREATE_BUCKET (device_property_create_bucket.ID)

/*
 * prototypes
 */

void s3_device_register(void);

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


/* Delete all files in the given device
 *
 * @param self: the S3Device object
 */
static gboolean
delete_all_files(S3Device *self);

/* Set up self->s3t as best as possible.
 *
 * The return value is TRUE iff self->s3t is useable.
 *
 * @param self: the S3Device object
 * @returns: TRUE if the handle is set up
 */
static gboolean
setup_handle(S3Device * self);

static void
s3_wait_thread_delete(S3Device *self);

/*
 * class mechanics */

static void
s3_device_init(S3Device * o);

static void
s3_device_class_init(S3DeviceClass * c);

static void
s3_device_finalize(GObject * o);

static Device*
s3_device_factory(char * device_name, char * device_type, char * device_node);

/*
 * Property{Get,Set}Fns */

static gboolean s3_device_set_access_key_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_secret_key_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_swift_account_id_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_swift_access_key_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_username(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_password(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_tenant_id(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_tenant_name(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_user_token_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_bucket_location_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_storage_class_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_server_side_encryption_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_proxy_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_ca_info_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_verbose_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_create_bucket_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_storage_api(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_openstack_swift_api_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_s3_multi_delete_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_ssl_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_reuse_connection_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_max_send_speed_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_max_recv_speed_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_nb_threads_backup(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_nb_threads_recovery(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_max_volume_usage_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean property_set_leom_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_enforce_max_volume_usage_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_use_subdomain_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_host_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_service_path_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_client_id_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_client_secret_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_refresh_token_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_project_id_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static void s3_thread_read_block(gpointer thread_data,
				 gpointer data);
static void s3_thread_write_block(gpointer thread_data,
				  gpointer data);
static gboolean make_bucket(Device * pself);


/* Wait that all threads are done */
static void reset_thread(S3Device *self);

/*
 * virtual functions */

static void
s3_device_open_device(Device *pself, char *device_name,
		  char * device_type, char * device_node);

static DeviceStatusFlags s3_device_read_label(Device * self);

static gboolean
s3_device_start(Device * self,
                DeviceAccessMode mode,
                char * label,
                char * timestamp);

static gboolean
s3_device_finish(Device * self);

static guint64
s3_device_get_bytes_read(Device * self);

static guint64
s3_device_get_bytes_written(Device * self);

static gboolean
s3_device_start_file(Device * self,
                     dumpfile_t * jobInfo);

static gboolean
s3_device_write_block(Device * self,
                      guint size,
                      gpointer data);

static gboolean
s3_device_finish_file(Device * self);

static dumpfile_t*
s3_device_seek_file(Device *pself,
                    guint file);

static gboolean
s3_device_seek_block(Device *pself,
                     guint64 block);

static int
s3_device_read_block(Device * pself,
                     gpointer data,
                     int *size_req);

static gboolean
s3_device_recycle_file(Device *pself,
                       guint file);

static gboolean
s3_device_erase(Device *pself);

static gboolean
check_at_leom(S3Device *self,
                guint64 size);

static gboolean
check_at_peom(S3Device *self,
                guint64 size);

/*
 * Private functions
 */

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

static gboolean
write_amanda_header(S3Device *self,
                    char *label,
                    char * timestamp)
{
    CurlBuffer amanda_header = {NULL, 0, 0, 0};
    char * key = NULL;
    gboolean result;
    dumpfile_t * dumpinfo = NULL;
    Device *d_self = DEVICE(self);
    size_t header_size;

    /* build the header */
    header_size = 0; /* no minimum size */
    dumpinfo = make_tapestart_header(DEVICE(self), label, timestamp);
    amanda_header.buffer = device_build_amanda_header(DEVICE(self), dumpinfo,
        &header_size);
    if (amanda_header.buffer == NULL) {
	device_set_error(d_self,
	    stralloc(_("Amanda tapestart header won't fit in a single block!")),
	    DEVICE_STATUS_DEVICE_ERROR);
	dumpfile_free(dumpinfo);
	g_free(amanda_header.buffer);
	return FALSE;
    }

    if(check_at_leom(self, header_size))
        d_self->is_eom = TRUE;

    if(check_at_peom(self, header_size)) {
        d_self->is_eom = TRUE;
        device_set_error(d_self,
            stralloc(_("No space left on device")),
            DEVICE_STATUS_DEVICE_ERROR);
        g_free(amanda_header.buffer);
        return FALSE;
    }

    /* write out the header and flush the uploads. */
    key = special_file_to_key(self, "tapestart", -1);
    g_assert(header_size < G_MAXUINT); /* for cast to guint */
    amanda_header.buffer_len = (guint)header_size;
    result = s3_upload(self->s3t[0].s3, self->bucket, key, S3_BUFFER_READ_FUNCS,
                       &amanda_header, NULL, NULL);
    g_free(amanda_header.buffer);
    g_free(key);

    if (!result) {
	device_set_error(d_self,
	    vstrallocf(_("While writing amanda header: %s"), s3_strerror(self->s3t[0].s3)),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
	dumpfile_free(dumpinfo);
    } else {
	dumpfile_free(d_self->volume_header);
	d_self->volume_header = dumpinfo;
        self->volume_bytes += header_size;
    }
    d_self->header_block_size = header_size;
    return result;
}

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

/* Convert an object name into a file number, assuming the given prefix
 * length. Returns -1 if the object name is invalid, or 0 if the object name
 * is a "special" key. */
static int key_to_file(guint prefix_len, const char * key) {
    int file;
    int i;

    /* skip the prefix */
    if (strlen(key) <= prefix_len)
	return -1;

    key += prefix_len;

    if (strncmp(key, SPECIAL_INFIX, strlen(SPECIAL_INFIX)) == 0) {
        return 0;
    }

    /* check that key starts with 'f' */
    if (key[0] != 'f')
	return -1;
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

/* Find the number of the last file that contains any data (even just a header).
 * Returns -1 in event of an error
 */
static int
find_last_file(S3Device *self) {
    gboolean result;
    GSList *keys;
    unsigned int prefix_len = strlen(self->prefix);
    int last_file = 0;
    Device *d_self = DEVICE(self);

    /* list all keys matching C{PREFIX*-*}, stripping the C{-*} */
    result = s3_list_keys(self->s3t[0].s3, self->bucket, self->prefix, "-", &keys, NULL);
    if (!result) {
	device_set_error(d_self,
	    vstrallocf(_("While listing S3 keys: %s"), s3_strerror(self->s3t[0].s3)),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
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

/* Find the number of the file following the requested one, if any.
 * Returns 0 if there is no such file or -1 in event of an error
 */
static int
find_next_file(S3Device *self, int last_file) {
    gboolean result;
    GSList *keys;
    unsigned int prefix_len = strlen(self->prefix);
    int next_file = 0;
    Device *d_self = DEVICE(self);

    /* list all keys matching C{PREFIX*-*}, stripping the C{-*} */
    result = s3_list_keys(self->s3t[0].s3, self->bucket, self->prefix, "-",
			  &keys, NULL);
    if (!result) {
	device_set_error(d_self,
	    vstrallocf(_("While listing S3 keys: %s"), s3_strerror(self->s3t[0].s3)),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
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

    return next_file;
}

static gboolean
delete_file(S3Device *self,
            int file)
{
    int thread = -1;

    gboolean result;
    GSList *keys;
    guint64 total_size = 0;
    Device *d_self = DEVICE(self);
    char *my_prefix;

    if (file == -1) {
	my_prefix = g_strdup_printf("%sf", self->prefix);
    } else {
	my_prefix = g_strdup_printf("%sf%08x-", self->prefix, file);
    }

    result = s3_list_keys(self->s3t[0].s3, self->bucket, my_prefix, NULL,
			  &keys, &total_size);
    if (!result) {
	device_set_error(d_self,
		g_strdup_printf(_("While listing S3 keys: %s"),
		    s3_strerror(self->s3t[0].s3)),
		DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
	return FALSE;
    }

    g_mutex_lock(self->thread_idle_mutex);
    if (!self->keys) {
	self->keys = keys;
    } else {
	self->keys = g_slist_concat(self->keys, keys);
    }

    // start the threads
    for (thread = 0; thread < self->nb_threads; thread++)  {
	if (self->s3t[thread].idle == 1) {
	    /* Check if the thread is in error */
	    if (self->s3t[thread].errflags != DEVICE_STATUS_SUCCESS) {
		device_set_error(d_self,
				 (char *)self->s3t[thread].errmsg,
				 self->s3t[thread].errflags);
		self->s3t[thread].errflags = DEVICE_STATUS_SUCCESS;
		self->s3t[thread].errmsg = NULL;
		g_mutex_unlock(self->thread_idle_mutex);
		s3_wait_thread_delete(self);
		return FALSE;
	    }
	    self->s3t[thread].idle = 0;
	    self->s3t[thread].done = 0;
	    g_thread_pool_push(self->thread_pool_delete, &self->s3t[thread],
			       NULL);
	}
    }
    g_cond_wait(self->thread_idle_cond, self->thread_idle_mutex);
    g_mutex_unlock(self->thread_idle_mutex);

    self->volume_bytes = total_size;

    s3_wait_thread_delete(self);

    return TRUE;
}

static void
s3_thread_delete_block(
    gpointer thread_data,
    gpointer data)
{
    static int count = 0;
    S3_by_thread *s3t = (S3_by_thread *)thread_data;
    Device *pself = (Device *)data;
    S3Device *self = S3_DEVICE(pself);
    int result = 1;
    char *filename;

    g_mutex_lock(self->thread_idle_mutex);
    while (result && self->keys) {
	if (self->use_s3_multi_delete) {
	    char **filenames = g_new(char *, 1001);
	    char **f = filenames;
	    int  n = 0;
	    while (self->keys && n<1000) {
		*f++ = self->keys->data;
		self->keys = g_slist_remove(self->keys, self->keys->data);
		n++;
	    }
	    *f++ = NULL;
	    g_mutex_unlock(self->thread_idle_mutex);
	    result = s3_multi_delete(s3t->s3, (const char *)self->bucket,
					      (const char **)filenames);
	    if (result != 1) {
		char **f;

		if (result == 2) {
		    g_debug("Deleting multiple keys not implemented");
		} else { /* result == 0 */
		    g_debug("Deleteing multiple keys failed: %s",
			    s3_strerror(s3t->s3));
		}

		self->use_s3_multi_delete = 0;
		/* re-add all filenames */
		f = filenames;
		g_mutex_lock(self->thread_idle_mutex);
		while(*f) {
		    self->keys = g_slist_prepend(self->keys, *f++);
		}
		g_mutex_unlock(self->thread_idle_mutex);
		g_free(filenames);
		result = 1;
		g_mutex_lock(self->thread_idle_mutex);
		continue;
	    }
	    f = filenames;
	    while(*f) {
		g_free(*f++);
	    }
	    g_free(filenames);
	} else {
	    filename = self->keys->data;
	    self->keys = g_slist_remove(self->keys, self->keys->data);
	    count++;
	    if (count >= 1000) {
		g_debug("Deleting %s ...", filename);
		count = 0;
	    }
	    g_mutex_unlock(self->thread_idle_mutex);
	    result = s3_delete(s3t->s3, (const char *)self->bucket,
					(const char *)filename);
	    if (!result) {
		s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
		s3t->errmsg = g_strdup_printf(_("While deleting key '%s': %s"),
					  filename, s3_strerror(s3t->s3));
	    }
	    g_free(filename);
	}
	g_mutex_lock(self->thread_idle_mutex);
    }
    s3t->idle = 1;
    s3t->done = 1;
    g_cond_broadcast(self->thread_idle_cond);
    g_mutex_unlock(self->thread_idle_mutex);
}

static void
s3_wait_thread_delete(S3Device *self)
{
    Device *d_self = (Device *)self;
    int idle_thread = 0;
    int thread;

    g_mutex_lock(self->thread_idle_mutex);
    while (idle_thread != self->nb_threads) {
	idle_thread = 0;
	for (thread = 0; thread < self->nb_threads; thread++)  {
	    if (self->s3t[thread].idle == 1) {
		idle_thread++;
	    }
	    /* Check if the thread is in error */
	    if (self->s3t[thread].errflags != DEVICE_STATUS_SUCCESS) {
		device_set_error(d_self, (char *)self->s3t[thread].errmsg,
					     self->s3t[thread].errflags);
		self->s3t[thread].errflags = DEVICE_STATUS_SUCCESS;
		self->s3t[thread].errmsg = NULL;
	    }
	}
	if (idle_thread != self->nb_threads) {
	    g_cond_wait(self->thread_idle_cond, self->thread_idle_mutex);
	}
    }
    g_mutex_unlock(self->thread_idle_mutex);
}


static gboolean
delete_all_files(S3Device *self)
{
    return delete_file(self, -1);
}

/*
 * Class mechanics
 */

void
s3_device_register(void)
{
    static const char * device_prefix_list[] = { S3_DEVICE_NAME, NULL };
    g_assert(s3_init());

    /* set up our properties */
    device_property_fill_and_register(&device_property_s3_secret_key,
                                      G_TYPE_STRING, "s3_secret_key",
       "Secret access key to authenticate with Amazon S3");
    device_property_fill_and_register(&device_property_s3_access_key,
                                      G_TYPE_STRING, "s3_access_key",
       "Access key ID to authenticate with Amazon S3");
    device_property_fill_and_register(&device_property_swift_account_id,
                                      G_TYPE_STRING, "swift_account_id",
       "Account ID to authenticate with openstack swift");
    device_property_fill_and_register(&device_property_swift_access_key,
                                      G_TYPE_STRING, "swift_access_key",
       "Access key to authenticate with openstack swift");
    device_property_fill_and_register(&device_property_username,
                                      G_TYPE_STRING, "username",
       "Username to authenticate with");
    device_property_fill_and_register(&device_property_password,
                                      G_TYPE_STRING, "password",
       "password to authenticate with");
    device_property_fill_and_register(&device_property_tenant_id,
                                      G_TYPE_STRING, "tenant_id",
       "tenant_id to authenticate with");
    device_property_fill_and_register(&device_property_tenant_name,
                                      G_TYPE_STRING, "tenant_name",
       "tenant_name to authenticate with");
    device_property_fill_and_register(&device_property_s3_host,
                                      G_TYPE_STRING, "s3_host",
       "hostname:port of the server");
    device_property_fill_and_register(&device_property_s3_service_path,
                                      G_TYPE_STRING, "s3_service_path",
       "path to add in the url");
    device_property_fill_and_register(&device_property_s3_user_token,
                                      G_TYPE_STRING, "s3_user_token",
       "User token for authentication Amazon devpay requests");
    device_property_fill_and_register(&device_property_s3_bucket_location,
                                      G_TYPE_STRING, "s3_bucket_location",
       "Location constraint for buckets on Amazon S3");
    device_property_fill_and_register(&device_property_s3_storage_class,
                                      G_TYPE_STRING, "s3_storage_class",
       "Storage class as specified by Amazon (STANDARD or REDUCED_REDUNDANCY)");
    device_property_fill_and_register(&device_property_s3_server_side_encryption,
                                      G_TYPE_STRING, "s3_server_side_encryption",
       "Serve side encryption as specified by Amazon (AES256)");
    device_property_fill_and_register(&device_property_proxy,
                                      G_TYPE_STRING, "proxy",
       "The proxy");
    device_property_fill_and_register(&device_property_ssl_ca_info,
                                      G_TYPE_STRING, "ssl_ca_info",
       "Path to certificate authority certificate");
    device_property_fill_and_register(&device_property_storage_api,
                                      G_TYPE_STRING, "storage_api",
       "Which cloud API to use.");
    device_property_fill_and_register(&device_property_openstack_swift_api,
                                      G_TYPE_STRING, "openstack_swift_api",
       "Whether to use openstack protocol");
    device_property_fill_and_register(&device_property_client_id,
                                      G_TYPE_STRING, "client_id",
       "client_id for use with oauth2");
    device_property_fill_and_register(&device_property_client_secret,
                                      G_TYPE_STRING, "client_secret",
       "client_secret for use with oauth2");
    device_property_fill_and_register(&device_property_refresh_token,
                                      G_TYPE_STRING, "refresh_token",
       "refresh_token for use with oauth2");
    device_property_fill_and_register(&device_property_project_id,
                                      G_TYPE_STRING, "project_id",
       "project id for use with google");
    device_property_fill_and_register(&device_property_s3_ssl,
                                      G_TYPE_BOOLEAN, "s3_ssl",
       "Whether to use SSL with Amazon S3");
    device_property_fill_and_register(&device_property_reuse_connection,
                                      G_TYPE_BOOLEAN, "reuse_connection",
       "Whether to reuse connection");
    device_property_fill_and_register(&device_property_create_bucket,
                                      G_TYPE_BOOLEAN, "create_bucket",
       "Whether to create/delete bucket");
    device_property_fill_and_register(&device_property_s3_subdomain,
                                      G_TYPE_BOOLEAN, "s3_subdomain",
       "Whether to use subdomain");
    device_property_fill_and_register(&device_property_max_send_speed,
                                      G_TYPE_UINT64, "max_send_speed",
       "Maximum average upload speed (bytes/sec)");
    device_property_fill_and_register(&device_property_max_recv_speed,
                                      G_TYPE_UINT64, "max_recv_speed",
       "Maximum average download speed (bytes/sec)");
    device_property_fill_and_register(&device_property_nb_threads_backup,
                                      G_TYPE_UINT64, "nb_threads_backup",
       "Number of writer thread");
    device_property_fill_and_register(&device_property_nb_threads_recovery,
                                      G_TYPE_UINT64, "nb_threads_recovery",
       "Number of reader thread");
    device_property_fill_and_register(&device_property_s3_multi_delete,
                                      G_TYPE_BOOLEAN, "s3_multi_delete",
       "Whether to use multi-delete");

    /* register the device itself */
    register_device(s3_device_factory, device_prefix_list);
}

static GType
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

static void
s3_device_init(S3Device * self)
{
    Device * dself = DEVICE(self);
    GValue response;

    self->s3_api = S3_API_S3;
    self->volume_bytes = 0;
    self->volume_limit = 0;
    self->leom = TRUE;
    self->enforce_volume_limit = FALSE;
    self->use_subdomain = FALSE;
    self->nb_threads = 1;
    self->nb_threads_backup = 1;
    self->nb_threads_recovery = 1;
    self->thread_pool_delete = NULL;
    self->thread_pool_write = NULL;
    self->thread_pool_read = NULL;
    self->thread_idle_cond = NULL;
    self->thread_idle_mutex = NULL;
    self->use_s3_multi_delete = 1;

    /* Register property values
     * Note: Some aren't added until s3_device_open_device()
     */
    bzero(&response, sizeof(response));

    g_value_init(&response, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(&response, CONCURRENCY_PARADIGM_SHARED_READ);
    device_set_simple_property(dself, PROPERTY_CONCURRENCY,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(&response, STREAMING_REQUIREMENT_NONE);
    device_set_simple_property(dself, PROPERTY_STREAMING,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE);
    device_set_simple_property(dself, PROPERTY_APPENDABLE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE);
    device_set_simple_property(dself, PROPERTY_PARTIAL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE);
    device_set_simple_property(dself, PROPERTY_FULL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE); /* well, there *is* no EOM on S3 .. */
    device_set_simple_property(dself, PROPERTY_LEOM,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_ENFORCE_MAX_VOLUME_USAGE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_S3_SUBDOMAIN,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_COMPRESSION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_READ_WRITE);
    device_set_simple_property(dself, PROPERTY_MEDIUM_ACCESS_TYPE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

}

static void
s3_device_class_init(S3DeviceClass * c G_GNUC_UNUSED)
{
    GObjectClass *g_object_class = (GObjectClass*) c;
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->open_device = s3_device_open_device;
    device_class->read_label = s3_device_read_label;
    device_class->start = s3_device_start;
    device_class->finish = s3_device_finish;
    device_class->get_bytes_read = s3_device_get_bytes_read;
    device_class->get_bytes_written = s3_device_get_bytes_written;

    device_class->start_file = s3_device_start_file;
    device_class->write_block = s3_device_write_block;
    device_class->finish_file = s3_device_finish_file;

    device_class->seek_file = s3_device_seek_file;
    device_class->seek_block = s3_device_seek_block;
    device_class->read_block = s3_device_read_block;
    device_class->recycle_file = s3_device_recycle_file;

    device_class->erase = s3_device_erase;

    g_object_class->finalize = s3_device_finalize;

    device_class_register_property(device_class, PROPERTY_S3_ACCESS_KEY,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_access_key_fn);

    device_class_register_property(device_class, PROPERTY_S3_SECRET_KEY,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_secret_key_fn);

    device_class_register_property(device_class, PROPERTY_SWIFT_ACCOUNT_ID,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_swift_account_id_fn);

    device_class_register_property(device_class, PROPERTY_SWIFT_ACCESS_KEY,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_swift_access_key_fn);

    device_class_register_property(device_class, PROPERTY_USERNAME,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_username);

    device_class_register_property(device_class, PROPERTY_PASSWORD,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_password);

    device_class_register_property(device_class, PROPERTY_TENANT_ID,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_tenant_id);

    device_class_register_property(device_class, PROPERTY_TENANT_NAME,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_tenant_name);

    device_class_register_property(device_class, PROPERTY_S3_HOST,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_host_fn);

    device_class_register_property(device_class, PROPERTY_S3_SERVICE_PATH,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_service_path_fn);

    device_class_register_property(device_class, PROPERTY_S3_USER_TOKEN,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_user_token_fn);

    device_class_register_property(device_class, PROPERTY_S3_BUCKET_LOCATION,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_bucket_location_fn);

    device_class_register_property(device_class, PROPERTY_S3_STORAGE_CLASS,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_storage_class_fn);

    device_class_register_property(device_class, PROPERTY_S3_SERVER_SIDE_ENCRYPTION,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_server_side_encryption_fn);

    device_class_register_property(device_class, PROPERTY_PROXY,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_proxy_fn);

    device_class_register_property(device_class, PROPERTY_SSL_CA_INFO,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_ca_info_fn);

    device_class_register_property(device_class, PROPERTY_VERBOSE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_verbose_fn);

    device_class_register_property(device_class, PROPERTY_CREATE_BUCKET,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_create_bucket_fn);

    device_class_register_property(device_class, PROPERTY_STORAGE_API,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_storage_api);

    device_class_register_property(device_class, PROPERTY_OPENSTACK_SWIFT_API,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_openstack_swift_api_fn);

    device_class_register_property(device_class, PROPERTY_S3_MULTI_DELETE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_s3_multi_delete_fn);

    device_class_register_property(device_class, PROPERTY_S3_SSL,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_ssl_fn);

    device_class_register_property(device_class, PROPERTY_REUSE_CONNECTION,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_reuse_connection_fn);

    device_class_register_property(device_class, PROPERTY_MAX_SEND_SPEED,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_max_send_speed_fn);

    device_class_register_property(device_class, PROPERTY_MAX_RECV_SPEED,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_max_recv_speed_fn);

    device_class_register_property(device_class, PROPERTY_NB_THREADS_BACKUP,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_nb_threads_backup);

    device_class_register_property(device_class, PROPERTY_NB_THREADS_RECOVERY,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_nb_threads_recovery);

    device_class_register_property(device_class, PROPERTY_COMPRESSION,
	    PROPERTY_ACCESS_GET_MASK,
	    device_simple_property_get_fn,
	    NULL);

    device_class_register_property(device_class, PROPERTY_LEOM,
            PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
            device_simple_property_get_fn,
            property_set_leom_fn);

    device_class_register_property(device_class, PROPERTY_MAX_VOLUME_USAGE,
            (PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_MASK) &
                (~ PROPERTY_ACCESS_SET_INSIDE_FILE_WRITE),
            device_simple_property_get_fn,
            s3_device_set_max_volume_usage_fn);

    device_class_register_property(device_class, PROPERTY_ENFORCE_MAX_VOLUME_USAGE,
            (PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_MASK) &
                (~ PROPERTY_ACCESS_SET_INSIDE_FILE_WRITE),
            device_simple_property_get_fn,
            s3_device_set_enforce_max_volume_usage_fn);

    device_class_register_property(device_class, PROPERTY_S3_SUBDOMAIN,
            PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
            device_simple_property_get_fn,
            s3_device_set_use_subdomain_fn);

    device_class_register_property(device_class, PROPERTY_CLIENT_ID,
            PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
            device_simple_property_get_fn,
            s3_device_set_client_id_fn);

    device_class_register_property(device_class, PROPERTY_CLIENT_SECRET,
            PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
            device_simple_property_get_fn,
            s3_device_set_client_secret_fn);

    device_class_register_property(device_class, PROPERTY_REFRESH_TOKEN,
            PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
            device_simple_property_get_fn,
            s3_device_set_refresh_token_fn);

    device_class_register_property(device_class, PROPERTY_PROJECT_ID,
            PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
            device_simple_property_get_fn,
            s3_device_set_project_id_fn);
}

static gboolean
s3_device_set_access_key_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->access_key);
    self->access_key = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_secret_key_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->secret_key);
    self->secret_key = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_swift_account_id_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->swift_account_id);
    self->swift_account_id = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_swift_access_key_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->swift_access_key);
    self->swift_access_key = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_username(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->username);
    self->username = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_password(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->password);
    self->password = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_tenant_id(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->tenant_id);
    self->tenant_id = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_tenant_name(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->tenant_name);
    self->tenant_name = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_host_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->host);
    self->host = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_service_path_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->service_path);
    self->service_path = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_user_token_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->user_token);
    self->user_token = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_bucket_location_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    char *str_val = g_value_dup_string(val);

    if (str_val[0] && self->use_ssl && !s3_curl_location_compat()) {
	device_set_error(p_self, stralloc(_(
		"Location constraint given for Amazon S3 bucket, "
		"but libcurl is too old support wildcard certificates.")),
	    DEVICE_STATUS_DEVICE_ERROR);
        goto fail;
    }

    if (str_val[0] && !s3_bucket_location_compat(self->bucket)) {
	device_set_error(p_self, g_strdup_printf(_(
		"Location constraint given for Amazon S3 bucket, "
		"but the bucket name (%s) is not usable as a subdomain."),
		self->bucket),
	    DEVICE_STATUS_DEVICE_ERROR);
        goto fail;
    }

    amfree(self->bucket_location);
    self->bucket_location = str_val;
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
fail:
    g_free(str_val);
    return FALSE;
}

static gboolean
s3_device_set_storage_class_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    char *str_val = g_value_dup_string(val);

    amfree(self->storage_class);
    self->storage_class = str_val;
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_server_side_encryption_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    char *str_val = g_value_dup_string(val);

    amfree(self->server_side_encryption);
    self->server_side_encryption = str_val;
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_proxy_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    char *str_val = g_value_dup_string(val);

    amfree(self->proxy);
    self->proxy = str_val;
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_ca_info_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->ca_info);
    self->ca_info = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_verbose_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    int       thread;

    self->verbose = g_value_get_boolean(val);
    /* Our S3 handle may not yet have been instantiated; if so, it will
     * get the proper verbose setting when it is created */
    if (self->s3t) {
	for (thread = 0; thread < self->nb_threads; thread++) {
	    if (self->s3t[thread].s3)
		s3_verbose(self->s3t[thread].s3, self->verbose);
	}
    }

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_create_bucket_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    int       thread;

    self->create_bucket = g_value_get_boolean(val);
    /* Our S3 handle may not yet have been instantiated; if so, it will
     * get the proper verbose setting when it is created */
    if (self->s3t) {
	for (thread = 0; thread < self->nb_threads; thread++) {
	    if (self->s3t[thread].s3)
		s3_verbose(self->s3t[thread].s3, self->verbose);
	}
    }

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_storage_api(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    const char *storage_api = g_value_get_string(val);
    if (g_str_equal(storage_api, "S3")) {
	self->s3_api = S3_API_S3;
    } else if (g_str_equal(storage_api, "SWIFT-1.0")) {
	self->s3_api = S3_API_SWIFT_1;
    } else if (g_str_equal(storage_api, "SWIFT-2.0")) {
	self->s3_api = S3_API_SWIFT_2;
    } else if (g_str_equal(storage_api, "OAUTH2")) {
	self->s3_api = S3_API_OAUTH2;
    } else {
	g_debug("Invalid STORAGE_API, using \"S3\".");
	self->s3_api = S3_API_S3;
    }

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_openstack_swift_api_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{

    const gboolean openstack_swift_api = g_value_get_boolean(val);
    if (openstack_swift_api) {
	GValue storage_api_val;
	g_value_init(&storage_api_val, G_TYPE_STRING);
	g_value_set_static_string(&storage_api_val, "SWIFT-1.0");
	return s3_device_set_storage_api(p_self, base, &storage_api_val,
					 surety, source);
    }
    return TRUE;
}

static gboolean
s3_device_set_s3_multi_delete_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->use_s3_multi_delete = g_value_get_boolean(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_ssl_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    gboolean new_val;
    int      thread;

    new_val = g_value_get_boolean(val);
    /* Our S3 handle may not yet have been instantiated; if so, it will
     * get the proper use_ssl setting when it is created */
    if (self->s3t) {
	for (thread = 0; thread < self->nb_threads; thread++) {
	    if (self->s3t[thread].s3 && !s3_use_ssl(self->s3t[thread].s3, new_val)) {
		device_set_error(p_self, g_strdup_printf(_(
	                "Error setting S3 SSL/TLS use "
	                "(tried to enable SSL/TLS for S3, but curl doesn't support it?)")),
		    DEVICE_STATUS_DEVICE_ERROR);
	        return FALSE;
	    }
	}
    }
    self->use_ssl = new_val;

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_reuse_connection_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->reuse_connection = g_value_get_boolean(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_max_send_speed_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    guint64 new_val;
    int     thread;

    new_val = g_value_get_uint64(val);
    if (self->s3t) {
	for (thread = 0; thread < self->nb_threads; thread++) {
	    if (self->s3t[thread].s3 && !s3_set_max_send_speed(self->s3t[thread].s3, new_val)) {
		device_set_error(p_self,
			g_strdup("Could not set S3 maximum send speed"),
			DEVICE_STATUS_DEVICE_ERROR);
	        return FALSE;
	    }
	}
    }
    self->max_send_speed = new_val;

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_max_recv_speed_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    guint64 new_val;
    int     thread;

    new_val = g_value_get_uint64(val);
    if (self->s3t) {
	for (thread = 0; thread < self->nb_threads; thread++) {
	    if (self->s3t[thread].s3 &&
		!s3_set_max_recv_speed(self->s3t[thread].s3, new_val)) {
		device_set_error(p_self,
			g_strdup("Could not set S3 maximum recv speed"),
			DEVICE_STATUS_DEVICE_ERROR);
	        return FALSE;
	    }
	}
    }
    self->max_recv_speed = new_val;

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_nb_threads_backup(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    guint64 new_val;

    new_val = g_value_get_uint64(val);
    self->nb_threads_backup = new_val;
    if (self->nb_threads_backup > self->nb_threads) {
	self->nb_threads = self->nb_threads_backup;
    }

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_nb_threads_recovery(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    guint64 new_val;

    new_val = g_value_get_uint64(val);
    self->nb_threads_recovery = new_val;
    if (self->nb_threads_recovery > self->nb_threads) {
	self->nb_threads = self->nb_threads_recovery;
    }

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_max_volume_usage_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->volume_limit = g_value_get_uint64(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);

}

static gboolean
s3_device_set_enforce_max_volume_usage_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->enforce_volume_limit = g_value_get_boolean(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);

}

static gboolean
s3_device_set_use_subdomain_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->use_subdomain = g_value_get_boolean(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
property_set_leom_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->leom = g_value_get_boolean(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_client_id_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->client_id);
    self->client_id = g_value_dup_string(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_client_secret_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->client_secret);
    self->client_secret = g_value_dup_string(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_refresh_token_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->refresh_token);
    self->refresh_token = g_value_dup_string(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_project_id_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->project_id);
    self->project_id = g_value_dup_string(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static Device*
s3_device_factory(char * device_name, char * device_type, char * device_node)
{
    Device *rval;
    g_assert(0 == strcmp(device_type, S3_DEVICE_NAME));
    rval = DEVICE(g_object_new(TYPE_S3_DEVICE, NULL));

    device_open_device(rval, device_name, device_type, device_node);
    return rval;
}

/*
 * Virtual function overrides
 */

static void
s3_device_open_device(Device *pself, char *device_name,
			char * device_type, char * device_node)
{
    S3Device *self = S3_DEVICE(pself);
    char * name_colon;
    GValue tmp_value;

    pself->min_block_size = S3_DEVICE_MIN_BLOCK_SIZE;
    pself->max_block_size = S3_DEVICE_MAX_BLOCK_SIZE;
    pself->block_size = S3_DEVICE_DEFAULT_BLOCK_SIZE;

    /* Device name may be bucket/prefix, to support multiple volumes in a
     * single bucket. */
    name_colon = strchr(device_node, '/');
    if (name_colon == NULL) {
        self->bucket = g_strdup(device_node);
        self->prefix = g_strdup("");
    } else {
        self->bucket = g_strndup(device_node, name_colon - device_node);
        self->prefix = g_strdup(name_colon + 1);
    }

    if (self->bucket == NULL || self->bucket[0] == '\0') {
	device_set_error(pself,
	    vstrallocf(_("Empty bucket name in device %s"), device_name),
	    DEVICE_STATUS_DEVICE_ERROR);
        amfree(self->bucket);
        amfree(self->prefix);
        return;
    }

    g_debug(_("S3 driver using bucket '%s', prefix '%s'"), self->bucket, self->prefix);

    /* default values */
    self->verbose = FALSE;
    self->s3_api = S3_API_S3;

    /* use SSL if available */
    self->use_ssl = s3_curl_supports_ssl();
    bzero(&tmp_value, sizeof(GValue));
    g_value_init(&tmp_value, G_TYPE_BOOLEAN);
    g_value_set_boolean(&tmp_value, self->use_ssl);
    device_set_simple_property(pself, device_property_s3_ssl.ID,
	&tmp_value, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT);

    /* reuse connection */
    self->reuse_connection = TRUE;
    bzero(&tmp_value, sizeof(GValue));
    g_value_init(&tmp_value, G_TYPE_BOOLEAN);
    g_value_set_boolean(&tmp_value, self->reuse_connection);
    device_set_simple_property(pself, device_property_reuse_connection.ID,
	&tmp_value, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT);

    /* Set default create_bucket */
    self->create_bucket = TRUE;
    bzero(&tmp_value, sizeof(GValue));
    g_value_init(&tmp_value, G_TYPE_BOOLEAN);
    g_value_set_boolean(&tmp_value, self->create_bucket);
    device_set_simple_property(pself, device_property_create_bucket.ID,
	&tmp_value, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT);

    if (parent_class->open_device) {
        parent_class->open_device(pself, device_name, device_type, device_node);
    }
}

static void s3_device_finalize(GObject * obj_self) {
    S3Device *self = S3_DEVICE (obj_self);
    int thread;

    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    if (self->thread_pool_delete) {
	g_thread_pool_free(self->thread_pool_delete, 1, 1);
	self->thread_pool_delete = NULL;
    }
    if (self->thread_pool_write) {
	g_thread_pool_free(self->thread_pool_write, 1, 1);
	self->thread_pool_write = NULL;
    }
    if (self->thread_pool_read) {
	g_thread_pool_free(self->thread_pool_read, 1, 1);
	self->thread_pool_read = NULL;
    }
    if (self->thread_idle_mutex) {
	g_mutex_free(self->thread_idle_mutex);
	self->thread_idle_mutex = NULL;
    }
    if (self->thread_idle_cond) {
	g_cond_free(self->thread_idle_cond);
	self->thread_idle_cond = NULL;
    }
    if (self->s3t) {
	for (thread = 0; thread < self->nb_threads; thread++) {
	    g_mutex_free(self->s3t[thread].now_mutex);
            if(self->s3t[thread].s3) s3_free(self->s3t[thread].s3);
	    g_free(self->s3t[thread].curl_buffer.buffer);
	}
	g_free(self->s3t);
    }
    if(self->bucket) g_free(self->bucket);
    if(self->prefix) g_free(self->prefix);
    if(self->access_key) g_free(self->access_key);
    if(self->secret_key) g_free(self->secret_key);
    if(self->swift_account_id) g_free(self->swift_account_id);
    if(self->swift_access_key) g_free(self->swift_access_key);
    if(self->username) g_free(self->username);
    if(self->password) g_free(self->password);
    if(self->tenant_id) g_free(self->tenant_id);
    if(self->tenant_name) g_free(self->tenant_name);
    if(self->host) g_free(self->host);
    if(self->service_path) g_free(self->service_path);
    if(self->user_token) g_free(self->user_token);
    if(self->bucket_location) g_free(self->bucket_location);
    if(self->storage_class) g_free(self->storage_class);
    if(self->server_side_encryption) g_free(self->server_side_encryption);
    if(self->proxy) g_free(self->proxy);
    if(self->ca_info) g_free(self->ca_info);
}

static gboolean setup_handle(S3Device * self) {
    Device *d_self = DEVICE(self);
    int thread;
    guint response_code;
    s3_error_code_t s3_error_code;
    CURLcode curl_code;

    if (self->s3t == NULL) {
	if (self->s3_api == S3_API_S3) {
	    if (self->access_key == NULL || self->access_key[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("No Amazon access key specified")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }

	    if (self->secret_key == NULL || self->secret_key[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("No Amazon secret key specified")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	} else if (self->s3_api == S3_API_SWIFT_1) {
	    if (self->swift_account_id == NULL ||
		self->swift_account_id[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("No Swift account id specified")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
            if (self->swift_access_key == NULL ||
		self->swift_access_key[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("No Swift access key specified")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	} else if (self->s3_api == S3_API_SWIFT_2) {
	    if (!((self->username && self->password && self->tenant_id) ||
		  (self->username && self->password && self->tenant_name) ||
		  (self->access_key && self->secret_key && self->tenant_id) ||
		  (self->access_key && self->secret_key && self->tenant_name))) {
		device_set_error(d_self,
		    g_strdup(_("Missing authorization properties")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	} else if (self->s3_api == S3_API_OAUTH2) {
	    if (self->client_id == NULL ||
		self->client_id[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("Missing client_id properties")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	    if (self->client_secret == NULL ||
		self->client_secret[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("Missing client_secret properties")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	    if (self->refresh_token == NULL ||
		self->refresh_token[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("Missing refresh_token properties")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	    if (self->project_id == NULL ||
		self->project_id[0] == '\0') {
		device_set_error(d_self,
		    g_strdup(_("Missing project_id properties")),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	}

	self->s3t = g_new0(S3_by_thread, self->nb_threads);
	if (self->s3t == NULL) {
	    device_set_error(d_self,
		g_strdup(_("Can't allocate S3Handle array")),
		DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
	}

	self->thread_idle_cond = g_cond_new();
	self->thread_idle_mutex = g_mutex_new();

	for (thread = 0; thread < self->nb_threads; thread++) {
	    self->s3t[thread].idle = 1;
	    self->s3t[thread].done = 1;
	    self->s3t[thread].eof = FALSE;
	    self->s3t[thread].errflags = DEVICE_STATUS_SUCCESS;
	    self->s3t[thread].errmsg = NULL;
	    self->s3t[thread].filename = NULL;
	    self->s3t[thread].curl_buffer.buffer = NULL;
	    self->s3t[thread].curl_buffer.buffer_len = 0;
	    self->s3t[thread].now_mutex = g_mutex_new();
            self->s3t[thread].s3 = s3_open(self->access_key, self->secret_key,
					   self->swift_account_id,
					   self->swift_access_key,
					   self->host, self->service_path,
					   self->use_subdomain,
					   self->user_token, self->bucket_location,
					   self->storage_class, self->ca_info,
					   self->server_side_encryption,
					   self->proxy,
					   self->s3_api,
					   self->username,
					   self->password,
					   self->tenant_id,
					   self->tenant_name,
					   self->client_id,
					   self->client_secret,
					   self->refresh_token,
					   self->reuse_connection);
            if (self->s3t[thread].s3 == NULL) {
	        device_set_error(d_self,
		    stralloc(_("Internal error creating S3 handle")),
		    DEVICE_STATUS_DEVICE_ERROR);
		self->nb_threads = thread+1;
                return FALSE;
	    }
        }

	g_debug("Create %d threads", self->nb_threads);
	self->thread_pool_delete = g_thread_pool_new(s3_thread_delete_block,
						     self, self->nb_threads, 0,
						     NULL);
	self->thread_pool_write = g_thread_pool_new(s3_thread_write_block, self,
					      self->nb_threads, 0, NULL);
	self->thread_pool_read = g_thread_pool_new(s3_thread_read_block, self,
					      self->nb_threads, 0, NULL);

	for (thread = 0; thread < self->nb_threads; thread++) {
	    s3_verbose(self->s3t[thread].s3, self->verbose);

	    if (!s3_use_ssl(self->s3t[thread].s3, self->use_ssl)) {
		device_set_error(d_self, g_strdup_printf(_(
	                "Error setting S3 SSL/TLS use "
	                "(tried to enable SSL/TLS for S3, but curl doesn't support it?)")),
		        DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }

	    if (self->max_send_speed &&
		!s3_set_max_send_speed(self->s3t[thread].s3,
				       self->max_send_speed)) {
		device_set_error(d_self,
			g_strdup("Could not set S3 maximum send speed"),
			DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }

	    if (self->max_recv_speed &&
		!s3_set_max_recv_speed(self->s3t[thread].s3,
				       self->max_recv_speed)) {
		device_set_error(d_self,
			g_strdup("Could not set S3 maximum recv speed"),
			DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    }
	}

	for (thread = 0; thread < self->nb_threads; thread++) {
	    if (!s3_open2(self->s3t[thread].s3)) {
		if (self->s3_api == S3_API_SWIFT_1 ||
		    self->s3_api == S3_API_SWIFT_2) {
		    s3_error(self->s3t[0].s3, NULL, &response_code,
			     &s3_error_code, NULL, &curl_code, NULL);
		    device_set_error(d_self,
			    g_strdup_printf(_("s3_open2 failed: %s"),
					    s3_strerror(self->s3t[0].s3)),
			    DEVICE_STATUS_DEVICE_ERROR);
		    self->nb_threads = thread+1;
		    return FALSE;
		} else {
		    device_set_error(d_self,
				     g_strdup("s3_open2 failed"),
				     DEVICE_STATUS_DEVICE_ERROR);
		    return FALSE;
		}
	    }
	}
    }

    return TRUE;
}

static gboolean
make_bucket(
    Device * pself)
{
    S3Device *self = S3_DEVICE(pself);
    guint response_code;
    s3_error_code_t s3_error_code;
    CURLcode curl_code;

    if (s3_is_bucket_exists(self->s3t[0].s3, self->bucket, self->project_id)) {
	return TRUE;
    }

    s3_error(self->s3t[0].s3, NULL, &response_code, &s3_error_code, NULL, &curl_code, NULL);

    if (response_code == 0 && s3_error_code == 0 &&
	(curl_code == CURLE_COULDNT_CONNECT ||
	 curl_code == CURLE_COULDNT_RESOLVE_HOST)) {
	device_set_error(pself,
	    g_strdup_printf(_("While connecting to S3 bucket: %s"),
			    s3_strerror(self->s3t[0].s3)),
		DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (!self->create_bucket) {
	device_set_error(pself,
	    g_strdup_printf(_("Can't list bucket: %s"),
			    s3_strerror(self->s3t[0].s3)),
		DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (!s3_make_bucket(self->s3t[0].s3, self->bucket, self->project_id)) {
        s3_error(self->s3t[0].s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it isn't an expected error (bucket already exists),
         * return FALSE */
        if (response_code != 409 ||
            (s3_error_code != S3_ERROR_BucketAlreadyExists &&
	     s3_error_code != S3_ERROR_BucketAlreadyOwnedByYou)) {
	    device_set_error(pself,
		g_strdup_printf(_("While creating new S3 bucket: %s"), s3_strerror(self->s3t[0].s3)),
		DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
        }
    }
    return TRUE;
}

static int progress_func(
    void *thread_data,
    double dltotal G_GNUC_UNUSED,
    double dlnow,
    double ultotal G_GNUC_UNUSED,
    double ulnow)
{
    S3_by_thread *s3t = (S3_by_thread *)thread_data;

    g_mutex_lock(s3t->now_mutex);
    s3t->dlnow = dlnow;
    s3t->ulnow = ulnow;
    g_mutex_unlock(s3t->now_mutex);

    return 0;
}

static DeviceStatusFlags
s3_device_read_label(Device *pself) {
    S3Device *self = S3_DEVICE(pself);
    char *key;
    CurlBuffer buf = {NULL, 0, 0, S3_DEVICE_MAX_BLOCK_SIZE};
    dumpfile_t *amanda_header;
    /* note that this may be called from s3_device_start, when
     * self->access_mode is not ACCESS_NULL */

    amfree(pself->volume_label);
    amfree(pself->volume_time);
    dumpfile_free(pself->volume_header);
    pself->volume_header = NULL;

    if (device_in_error(self)) return pself->status;

    if (!setup_handle(self)) {
        /* setup_handle already set our error message */
	return pself->status;
    }
    reset_thread(self);

    key = special_file_to_key(self, "tapestart", -1);

    if (!make_bucket(pself)) {
	return pself->status;
    }

    if (!s3_read(self->s3t[0].s3, self->bucket, key, S3_BUFFER_WRITE_FUNCS, &buf, NULL, NULL)) {
        guint response_code;
        s3_error_code_t s3_error_code;
        s3_error(self->s3t[0].s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it's an expected error (not found), just return FALSE */
        if (response_code == 404 &&
             (s3_error_code == S3_ERROR_None ||
              s3_error_code == S3_ERROR_Unknown ||
	      s3_error_code == S3_ERROR_NoSuchKey ||
	      s3_error_code == S3_ERROR_NoSuchEntity ||
	      s3_error_code == S3_ERROR_NoSuchBucket)) {
            g_debug(_("Amanda header not found while reading tapestart header (this is expected for empty tapes)"));
	    device_set_error(pself,
		stralloc(_("Amanda header not found -- unlabeled volume?")),
		  DEVICE_STATUS_DEVICE_ERROR
		| DEVICE_STATUS_VOLUME_ERROR
		| DEVICE_STATUS_VOLUME_UNLABELED);
            return pself->status;
        }

        /* otherwise, log it and return */
	device_set_error(pself,
	    vstrallocf(_("While trying to read tapestart header: %s"), s3_strerror(self->s3t[0].s3)),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
        return pself->status;
    }

    /* handle an empty file gracefully */
    if (buf.buffer_len == 0) {
	device_set_error(pself, stralloc(_("Empty header file")), DEVICE_STATUS_VOLUME_ERROR);
        return pself->status;
    }

    pself->header_block_size = buf.buffer_len;
    g_assert(buf.buffer != NULL);
    amanda_header = g_new(dumpfile_t, 1);
    parse_file_header(buf.buffer, amanda_header, buf.buffer_pos);
    pself->volume_header = amanda_header;
    g_free(buf.buffer);

    if (amanda_header->type != F_TAPESTART) {
	device_set_error(pself, stralloc(_("Invalid amanda header")), DEVICE_STATUS_VOLUME_ERROR);
        return pself->status;
    }

    pself->volume_label = g_strdup(amanda_header->name);
    pself->volume_time = g_strdup(amanda_header->datestamp);
    /* pself->volume_header is already set */

    device_set_error(pself, NULL, DEVICE_STATUS_SUCCESS);

    return pself->status;
}

static gboolean
s3_device_start (Device * pself, DeviceAccessMode mode,
                 char * label, char * timestamp) {
    S3Device * self;
    GSList *keys;
    guint64 total_size = 0;
    gboolean result;

    self = S3_DEVICE(pself);

    if (device_in_error(self)) return FALSE;

    if (!setup_handle(self)) {
        /* setup_handle already set our error message */
	return FALSE;
    }

    reset_thread(self);
    pself->access_mode = mode;
    g_mutex_lock(pself->device_mutex);
    pself->in_file = FALSE;
    g_mutex_unlock(pself->device_mutex);

    /* try creating the bucket, in case it doesn't exist */
    if (!make_bucket(pself)) {
	return FALSE;
    }

    /* take care of any dirty work for this mode */
    switch (mode) {
        case ACCESS_READ:
	    if (pself->volume_label == NULL && s3_device_read_label(pself) != DEVICE_STATUS_SUCCESS) {
		/* s3_device_read_label already set our error message */
		return FALSE;
	    }
            break;

        case ACCESS_WRITE:
            if (!delete_all_files(self)) {
		return FALSE;
	    }

            /* write a new amanda header */
            if (!write_amanda_header(self, label, timestamp)) {
                return FALSE;
            }

	    pself->volume_label = newstralloc(pself->volume_label, label);
	    pself->volume_time = newstralloc(pself->volume_time, timestamp);

	    /* unset the VOLUME_UNLABELED flag, if it was set */
	    device_set_error(pself, NULL, DEVICE_STATUS_SUCCESS);
            break;

        case ACCESS_APPEND:
	    if (pself->volume_label == NULL && s3_device_read_label(pself) != DEVICE_STATUS_SUCCESS) {
		/* s3_device_read_label already set our error message */
		return FALSE;
	    } else {
                result = s3_list_keys(self->s3t[0].s3, self->bucket, NULL, NULL, &keys, &total_size);
                if(!result) {
                    device_set_error(pself,
                                 vstrallocf(_("While listing S3 keys: %s"), s3_strerror(self->s3t[0].s3)),
                                 DEVICE_STATUS_DEVICE_ERROR|DEVICE_STATUS_VOLUME_ERROR);
                    return FALSE;
                } else {
                    self->volume_bytes = total_size;
                }
            }
            return seek_to_end(self);
            break;

        case ACCESS_NULL:
            g_assert_not_reached();
    }

    return TRUE;
}

static gboolean
s3_device_finish (
    Device * pself)
{
    S3Device *self = S3_DEVICE(pself);

    reset_thread(self);

    /* we're not in a file anymore */
    pself->access_mode = ACCESS_NULL;

    if (device_in_error(pself)) return FALSE;

    return TRUE;
}

/* functions for writing */


static guint64
s3_device_get_bytes_read(
    Device * pself)
{
    S3Device *self = S3_DEVICE(pself);
    int       thread;
    guint64   dltotal;

    g_mutex_unlock(pself->device_mutex);
    /* Add per thread */
    g_mutex_lock(self->thread_idle_mutex);
    dltotal = self->dltotal;
    for (thread = 0; thread < self->nb_threads_recovery; thread++) {
	g_mutex_lock(self->s3t[thread].now_mutex);
	dltotal += self->s3t[thread].dlnow;
	g_mutex_unlock(self->s3t[thread].now_mutex);
    }
    g_mutex_unlock(self->thread_idle_mutex);
    g_mutex_lock(pself->device_mutex);

    return dltotal;
}


static guint64
s3_device_get_bytes_written(
    Device * pself)
{
    S3Device *self = S3_DEVICE(pself);
    int       thread;
    guint64   ultotal;

    g_mutex_unlock(pself->device_mutex);
    /* Add per thread */
    g_mutex_lock(self->thread_idle_mutex);
    ultotal = self->ultotal;
    for (thread = 0; thread < self->nb_threads_backup; thread++) {
	g_mutex_lock(self->s3t[thread].now_mutex);
	ultotal += self->s3t[thread].ulnow;
	g_mutex_unlock(self->s3t[thread].now_mutex);
    }
    g_mutex_unlock(self->thread_idle_mutex);
    g_mutex_lock(pself->device_mutex);

    return ultotal;
}


static gboolean
s3_device_start_file (Device *pself, dumpfile_t *jobInfo) {
    S3Device *self = S3_DEVICE(pself);
    CurlBuffer amanda_header = {NULL, 0, 0, 0};
    gboolean result;
    size_t header_size;
    char  *key;
    int    thread;

    if (device_in_error(self)) return FALSE;

    reset_thread(self);
    pself->is_eom = FALSE;

    /* Set the blocksize to zero, since there's no header to skip (it's stored
     * in a distinct file, rather than block zero) */
    jobInfo->blocksize = 0;

    /* Build the amanda header. */
    header_size = 0; /* no minimum size */
    amanda_header.buffer = device_build_amanda_header(pself, jobInfo,
        &header_size);
    if (amanda_header.buffer == NULL) {
	device_set_error(pself,
	    stralloc(_("Amanda file header won't fit in a single block!")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
    amanda_header.buffer_len = header_size;

    if(check_at_leom(self, header_size))
        pself->is_eom = TRUE;

    if(check_at_peom(self, header_size)) {
        pself->is_eom = TRUE;
        device_set_error(pself,
            stralloc(_("No space left on device")),
            DEVICE_STATUS_DEVICE_ERROR);
        g_free(amanda_header.buffer);
        return FALSE;
    }

    for (thread = 0; thread < self->nb_threads; thread++)  {
	self->s3t[thread].idle = 1;
	self->s3t[thread].ulnow = 0;
    }

    /* set the file and block numbers correctly */
    pself->file = (pself->file > 0)? pself->file+1 : 1;
    pself->block = 0;
    g_mutex_lock(pself->device_mutex);
    pself->in_file = TRUE;
    pself->bytes_written = 0;
    g_mutex_unlock(pself->device_mutex);
    g_mutex_lock(self->thread_idle_mutex);
    self->ultotal = 0;
    g_mutex_unlock(self->thread_idle_mutex);
    /* write it out as a special block (not the 0th) */
    key = special_file_to_key(self, "filestart", pself->file);
    result = s3_upload(self->s3t[0].s3, self->bucket, key, S3_BUFFER_READ_FUNCS,
                       &amanda_header, NULL, NULL);
    g_free(amanda_header.buffer);
    g_free(key);
    if (!result) {
	device_set_error(pself,
	    vstrallocf(_("While writing filestart header: %s"), s3_strerror(self->s3t[0].s3)),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
        return FALSE;
    }

    self->volume_bytes += header_size;

    return TRUE;
}

static gboolean
s3_device_write_block (Device * pself, guint size, gpointer data) {
    char *filename;
    S3Device * self = S3_DEVICE(pself);
    int idle_thread = 0;
    int thread = -1;
    int first_idle = -1;

    g_assert (self != NULL);
    g_assert (data != NULL);
    if (device_in_error(self)) return FALSE;

    if(check_at_leom(self, size))
        pself->is_eom = TRUE;

    if(check_at_peom(self, size)) {
        pself->is_eom = TRUE;
        device_set_error(pself,
            stralloc(_("No space left on device")),
            DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    filename = file_and_block_to_key(self, pself->file, pself->block);

    g_mutex_lock(self->thread_idle_mutex);
    while (!idle_thread) {
	idle_thread = 0;
	for (thread = 0; thread < self->nb_threads_backup; thread++)  {
	    if (self->s3t[thread].idle == 1) {
		idle_thread++;
		/* Check if the thread is in error */
		if (self->s3t[thread].errflags != DEVICE_STATUS_SUCCESS) {
		    device_set_error(pself, (char *)self->s3t[thread].errmsg,
				     self->s3t[thread].errflags);
		    self->s3t[thread].errflags = DEVICE_STATUS_SUCCESS;
		    self->s3t[thread].errmsg = NULL;
		    g_mutex_unlock(self->thread_idle_mutex);
		    return FALSE;
		}
		if (first_idle == -1) {
		    first_idle = thread;
		    break;
		}
	    }
	}
	if (!idle_thread) {
	    g_cond_wait(self->thread_idle_cond, self->thread_idle_mutex);
	}
    }
    thread = first_idle;

    self->s3t[thread].idle = 0;
    self->s3t[thread].done = 0;
    if (self->s3t[thread].curl_buffer.buffer &&
	self->s3t[thread].curl_buffer.buffer_len < size) {
	g_free((char *)self->s3t[thread].curl_buffer.buffer);
	self->s3t[thread].curl_buffer.buffer = NULL;
	self->s3t[thread].curl_buffer.buffer_len = 0;
	self->s3t[thread].buffer_len = 0;
    }
    if (self->s3t[thread].curl_buffer.buffer == NULL) {
	self->s3t[thread].curl_buffer.buffer = g_malloc(size);
	self->s3t[thread].curl_buffer.buffer_len = size;
	self->s3t[thread].buffer_len = size;
    }
    memcpy((char *)self->s3t[thread].curl_buffer.buffer, data, size);
    self->s3t[thread].curl_buffer.buffer_pos = 0;
    self->s3t[thread].curl_buffer.buffer_len = size;
    self->s3t[thread].curl_buffer.max_buffer_size = 0;
    self->s3t[thread].filename = filename;
    g_thread_pool_push(self->thread_pool_write, &self->s3t[thread], NULL);
    g_mutex_unlock(self->thread_idle_mutex);

    pself->block++;
    self->volume_bytes += size;
    return TRUE;
}

static void
s3_thread_write_block(
    gpointer thread_data,
    gpointer data)
{
    S3_by_thread *s3t = (S3_by_thread *)thread_data;
    Device *pself = (Device *)data;
    S3Device *self = S3_DEVICE(pself);
    gboolean result;

    result = s3_upload(s3t->s3, self->bucket, (char *)s3t->filename,
		       S3_BUFFER_READ_FUNCS, (CurlBuffer *)&s3t->curl_buffer,
		       progress_func, s3t);
    g_free((void *)s3t->filename);
    s3t->filename = NULL;
    if (!result) {
	s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
	s3t->errmsg = g_strdup_printf(_("While writing data block to S3: %s"), s3_strerror(s3t->s3));
    }
    g_mutex_lock(self->thread_idle_mutex);
    s3t->idle = 1;
    s3t->done = 1;
    if (result)
	self->ultotal += s3t->curl_buffer.buffer_len;
    s3t->curl_buffer.buffer_len = s3t->buffer_len;
    s3t->ulnow = 0;
    g_cond_broadcast(self->thread_idle_cond);
    g_mutex_unlock(self->thread_idle_mutex);
}

static gboolean
s3_device_finish_file (Device * pself) {
    S3Device *self = S3_DEVICE(pself);

    /* Check all threads are done */
    int idle_thread = 0;
    int thread;

    g_mutex_lock(self->thread_idle_mutex);
    while (idle_thread != self->nb_threads) {
	idle_thread = 0;
	for (thread = 0; thread < self->nb_threads; thread++)  {
	    if (self->s3t[thread].idle == 1) {
		idle_thread++;
	    }
	    /* check thread status */
	    if (self->s3t[thread].errflags != DEVICE_STATUS_SUCCESS) {
		device_set_error(pself, (char *)self->s3t[thread].errmsg,
				 self->s3t[thread].errflags);
		self->s3t[thread].errflags = DEVICE_STATUS_SUCCESS;
		self->s3t[thread].errmsg = NULL;
	    }
	}
	if (idle_thread != self->nb_threads) {
	    g_cond_wait(self->thread_idle_cond, self->thread_idle_mutex);
	}
    }
    self->ultotal = 0;
    g_mutex_unlock(self->thread_idle_mutex);

    if (device_in_error(pself)) return FALSE;

    /* we're not in a file anymore */
    g_mutex_lock(pself->device_mutex);
    pself->in_file = FALSE;
    pself->bytes_written = 0;;
    g_mutex_unlock(pself->device_mutex);

    return TRUE;
}

static gboolean
s3_device_recycle_file(Device *pself, guint file) {
    S3Device *self = S3_DEVICE(pself);
    if (device_in_error(self)) return FALSE;

    reset_thread(self);
    delete_file(self, file);
    s3_wait_thread_delete(self);
    return !device_in_error(self);
    /* delete_file already set our error message if necessary */
}

static gboolean
s3_device_erase(Device *pself) {
    S3Device *self = S3_DEVICE(pself);
    char *key = NULL;
    const char *errmsg = NULL;
    guint response_code;
    s3_error_code_t s3_error_code;

    if (!setup_handle(self)) {
        /* error set by setup_handle */
        return FALSE;
    }

    reset_thread(self);
    key = special_file_to_key(self, "tapestart", -1);
    if (!s3_delete(self->s3t[0].s3, self->bucket, key)) {
        s3_error(self->s3t[0].s3, &errmsg, NULL, NULL, NULL, NULL, NULL);
	device_set_error(pself,
	    stralloc(errmsg),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }
    g_free(key);

    dumpfile_free(pself->volume_header);
    pself->volume_header = NULL;

    if (!delete_all_files(self))
        return FALSE;

    device_set_error(pself, g_strdup("Unlabeled volume"),
		     DEVICE_STATUS_VOLUME_UNLABELED);

    if (self->create_bucket &&
	!s3_delete_bucket(self->s3t[0].s3, self->bucket)) {
        s3_error(self->s3t[0].s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

        /*
         * ignore the error if the bucket isn't empty (there may be data from elsewhere)
         * or the bucket not existing (already deleted perhaps?)
         */
        if (!(
                (response_code == 409 && s3_error_code == S3_ERROR_BucketNotEmpty) ||
                (response_code == 404 && s3_error_code == S3_ERROR_NoSuchBucket))) {

            device_set_error(pself,
	        stralloc(errmsg),
	        DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
        }
    }
    self->volume_bytes = 0;
    return TRUE;
}

/* functions for reading */

static dumpfile_t*
s3_device_seek_file(Device *pself, guint file) {
    S3Device *self = S3_DEVICE(pself);
    gboolean result;
    char *key;
    CurlBuffer buf = {NULL, 0, 0, S3_DEVICE_MAX_BLOCK_SIZE};
    dumpfile_t *amanda_header;
    const char *errmsg = NULL;
    int thread;

    if (device_in_error(self)) return NULL;

    reset_thread(self);

    pself->file = file;
    pself->is_eof = FALSE;
    pself->block = 0;
    g_mutex_lock(pself->device_mutex);
    pself->in_file = FALSE;
    pself->bytes_read = 0;
    g_mutex_unlock(pself->device_mutex);
    self->next_block_to_read = 0;
    g_mutex_lock(self->thread_idle_mutex);
    self->dltotal = 0;
    g_mutex_unlock(self->thread_idle_mutex);

    /* read it in */
    key = special_file_to_key(self, "filestart", pself->file);
    result = s3_read(self->s3t[0].s3, self->bucket, key, S3_BUFFER_WRITE_FUNCS,
        &buf, NULL, NULL);
    g_free(key);

    if (!result) {
        guint response_code;
        s3_error_code_t s3_error_code;
        s3_error(self->s3t[0].s3, &errmsg, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it's an expected error (not found), check what to do. */
        if (response_code == 404 &&
            (s3_error_code == S3_ERROR_None ||
	     s3_error_code == S3_ERROR_NoSuchKey ||
	     s3_error_code == S3_ERROR_NoSuchEntity)) {
            int next_file;
            next_file = find_next_file(self, pself->file);
            if (next_file > 0) {
                /* Note short-circut of dispatcher. */
                return s3_device_seek_file(pself, next_file);
            } else if (next_file == 0) {
                /* No next file. Check if we are one past the end. */
                key = special_file_to_key(self, "filestart", pself->file - 1);
                result = s3_read(self->s3t[0].s3, self->bucket, key,
                    S3_BUFFER_WRITE_FUNCS, &buf, NULL, NULL);
                g_free(key);
                if (result) {
		    /* pself->file, etc. are already correct */
                    return make_tapeend_header();
                } else {
		    device_set_error(pself,
			stralloc(_("Attempt to read past tape-end file")),
			DEVICE_STATUS_SUCCESS);
                    return NULL;
                }
            }
        } else {
            /* An unexpected error occured finding out if we are the last file. */
	    device_set_error(pself,
		stralloc(errmsg),
		DEVICE_STATUS_DEVICE_ERROR);
            return NULL;
        }
    }

    /* and make a dumpfile_t out of it */
    g_assert(buf.buffer != NULL);
    amanda_header = g_new(dumpfile_t, 1);
    fh_init(amanda_header);
    parse_file_header(buf.buffer, amanda_header, buf.buffer_pos);
    g_free(buf.buffer);

    switch (amanda_header->type) {
        case F_DUMPFILE:
        case F_CONT_DUMPFILE:
        case F_SPLIT_DUMPFILE:
            break;

        default:
	    device_set_error(pself,
		stralloc(_("Invalid amanda header while reading file header")),
		DEVICE_STATUS_VOLUME_ERROR);
            g_free(amanda_header);
            return NULL;
    }

    for (thread = 0; thread < self->nb_threads; thread++)  {
	self->s3t[thread].idle = 1;
	self->s3t[thread].eof = FALSE;
	self->s3t[thread].ulnow = 0;
    }
    g_mutex_lock(pself->device_mutex);
    pself->in_file = TRUE;
    g_mutex_unlock(pself->device_mutex);
    return amanda_header;
}

static gboolean
s3_device_seek_block(Device *pself, guint64 block) {
    S3Device * self = S3_DEVICE(pself);
    if (device_in_error(pself)) return FALSE;

    reset_thread(self);
    pself->block = block;
    self->next_block_to_read = block;
    return TRUE;
}

static int
s3_device_read_block (Device * pself, gpointer data, int *size_req) {
    S3Device * self = S3_DEVICE(pself);
    char *key;
    int thread;
    int done = 0;

    g_assert (self != NULL);
    if (device_in_error(self)) return -1;

    g_mutex_lock(self->thread_idle_mutex);
    /* start a read ahead for each thread */
    for (thread = 0; thread < self->nb_threads_recovery; thread++) {
	S3_by_thread *s3t = &self->s3t[thread];
	if (s3t->idle) {
	    key = file_and_block_to_key(self, pself->file, self->next_block_to_read);
	    s3t->filename = key;
	    s3t->done = 0;
	    s3t->idle = 0;
	    s3t->eof = FALSE;
	    s3t->dlnow = 0;
	    s3t->ulnow = 0;
	    s3t->errflags = DEVICE_STATUS_SUCCESS;
	    if (self->s3t[thread].curl_buffer.buffer &&
		(int)self->s3t[thread].curl_buffer.buffer_len < *size_req) {
		g_free(self->s3t[thread].curl_buffer.buffer);
		self->s3t[thread].curl_buffer.buffer = NULL;
		self->s3t[thread].curl_buffer.buffer_len = 0;
		self->s3t[thread].buffer_len = 0;
	    }
	    if (!self->s3t[thread].curl_buffer.buffer) {
		self->s3t[thread].curl_buffer.buffer = g_malloc(*size_req);
		self->s3t[thread].curl_buffer.buffer_len = *size_req;
		self->s3t[thread].buffer_len = *size_req;
	    }
	    s3t->curl_buffer.buffer_pos = 0;
	    s3t->curl_buffer.max_buffer_size = S3_DEVICE_MAX_BLOCK_SIZE;
	    self->next_block_to_read++;
	    g_thread_pool_push(self->thread_pool_read, s3t, NULL);
	}
    }

    /* get the file*/
    key = file_and_block_to_key(self, pself->file, pself->block);
    g_assert(key != NULL);
    while (!done) {
	/* find which thread read the key */
	for (thread = 0; thread < self->nb_threads_recovery; thread++) {
	    S3_by_thread *s3t;
	    s3t = &self->s3t[thread];
	    if (!s3t->idle &&
		s3t->done &&
		strcmp(key, (char *)s3t->filename) == 0) {
		if (s3t->eof) {
		    /* return eof */
		    g_free(key);
		    pself->is_eof = TRUE;
		    g_mutex_lock(pself->device_mutex);
		    pself->in_file = FALSE;
		    g_mutex_unlock(pself->device_mutex);
		    device_set_error(pself, stralloc(_("EOF")),
				     DEVICE_STATUS_SUCCESS);
		    g_mutex_unlock(self->thread_idle_mutex);
		    return -1;
		} else if (s3t->errflags != DEVICE_STATUS_SUCCESS) {
		    /* return the error */
		    device_set_error(pself, (char *)s3t->errmsg, s3t->errflags);
		    g_free(key);
		    g_mutex_unlock(self->thread_idle_mutex);
		    return -1;

		} else if ((guint)*size_req >= s3t->curl_buffer.buffer_pos) {
		    /* return the buffer */
		    g_mutex_unlock(self->thread_idle_mutex);
		    memcpy(data, s3t->curl_buffer.buffer,
				 s3t->curl_buffer.buffer_pos);
		    *size_req = s3t->curl_buffer.buffer_pos;
		    g_free(key);
		    s3t->idle = 1;
		    g_free((char *)s3t->filename);
		    pself->block++;
		    done = 1;
		    g_mutex_lock(self->thread_idle_mutex);
		    break;
		} else { /* buffer not enough large */
		    *size_req = s3t->curl_buffer.buffer_len;
		    g_free(key);
		    g_mutex_unlock(self->thread_idle_mutex);
		    return 0;
		}
	    }
	}
	if (!done) {
	    g_cond_wait(self->thread_idle_cond, self->thread_idle_mutex);
	}
    }

    /* start a read ahead for the thread */
    for (thread = 0; thread < self->nb_threads_recovery; thread++) {
	S3_by_thread *s3t = &self->s3t[thread];
	if (s3t->idle) {
	    key = file_and_block_to_key(self, pself->file, self->next_block_to_read);
	    s3t->filename = key;
	    s3t->done = 0;
	    s3t->idle = 0;
	    s3t->eof = FALSE;
	    s3t->dlnow = 0;
	    s3t->ulnow = 0;
	    s3t->errflags = DEVICE_STATUS_SUCCESS;
	    if (!self->s3t[thread].curl_buffer.buffer) {
		self->s3t[thread].curl_buffer.buffer = g_malloc(*size_req);
		self->s3t[thread].curl_buffer.buffer_len = *size_req;
	    }
	    s3t->curl_buffer.buffer_pos = 0;
	    self->next_block_to_read++;
	    g_thread_pool_push(self->thread_pool_read, s3t, NULL);
	}
    }
    g_mutex_unlock(self->thread_idle_mutex);

    return *size_req;

}

static void
s3_thread_read_block(
    gpointer thread_data,
    gpointer data)
{
    S3_by_thread *s3t = (S3_by_thread *)thread_data;
    Device *pself = (Device *)data;
    S3Device *self = S3_DEVICE(pself);
    gboolean result;

    result = s3_read(s3t->s3, self->bucket, (char *)s3t->filename,
	S3_BUFFER_WRITE_FUNCS,
	(CurlBuffer *)&s3t->curl_buffer, progress_func, s3t);

    g_mutex_lock(self->thread_idle_mutex);
    if (!result) {
	guint response_code;
	s3_error_code_t s3_error_code;
	s3_error(s3t->s3, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);
	/* if it's an expected error (not found), just return -1 */
	if (response_code == 404 &&
            (s3_error_code == S3_ERROR_None ||
	     s3_error_code == S3_ERROR_Unknown ||
	     s3_error_code == S3_ERROR_NoSuchKey ||
	     s3_error_code == S3_ERROR_NoSuchEntity)) {
	    s3t->eof = TRUE;
	} else {

	    /* otherwise, log it and return FALSE */
	    s3t->errflags = DEVICE_STATUS_VOLUME_ERROR;
	    s3t->errmsg = g_strdup_printf(_("While reading data block from S3: %s"),
					  s3_strerror(s3t->s3));
	}
    } else {
	self->dltotal += s3t->curl_buffer.buffer_len;
    }
    s3t->dlnow = 0;
    s3t->ulnow = 0;
    s3t->done = 1;
    g_cond_broadcast(self->thread_idle_cond);
    g_mutex_unlock(self->thread_idle_mutex);

    return;
}

static gboolean
check_at_peom(S3Device *self, guint64 size)
{
    if(self->enforce_volume_limit && (self->volume_limit > 0)) {
        guint64 newtotal = self->volume_bytes + size;
        if(newtotal > self->volume_limit) {
            return TRUE;
        }
    }
    return FALSE;
}

static gboolean
check_at_leom(S3Device *self, guint64 size)
{
    guint64 block_size = DEVICE(self)->block_size;
    guint64 eom_warning_buffer = block_size *
		(EOM_EARLY_WARNING_ZONE_BLOCKS + self->nb_threads);

    if(!self->leom)
        return FALSE;

    if(self->enforce_volume_limit && (self->volume_limit > 0)) {
        guint64 newtotal = self->volume_bytes + size + eom_warning_buffer;
        if(newtotal > self->volume_limit) {
           return TRUE;
        }
    }
    return FALSE;
}

static void
reset_thread(
    S3Device *self)
{
    int thread;
    int nb_done = 0;

    if (self->thread_idle_mutex) {
	g_mutex_lock(self->thread_idle_mutex);
	while(nb_done != self->nb_threads) {
	    nb_done = 0;
	    for (thread = 0; thread < self->nb_threads; thread++)  {
		if (self->s3t[thread].done == 1)
		    nb_done++;
	    }
	    if (nb_done != self->nb_threads) {
		g_cond_wait(self->thread_idle_cond, self->thread_idle_mutex);
	    }
	}
	g_mutex_unlock(self->thread_idle_mutex);
    }
}
