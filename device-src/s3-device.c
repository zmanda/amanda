/* Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

/* An S3 device uses Amazon's S3 service (http://www.amazon.com/s3) to store
 * data.  It stores data in keys named with a user-specified prefix, inside a
 * user-specified bucket.  Data is stored in the form of numbered (large)
 * blocks.
 */
#ifdef HAVE_CONFIG_H
#include "../config/config.h"
/* use a relative path here to avoid conflicting with Perl's config.h. */
#endif

#include "s3-device.h"
#include "s3.h"
#include "s3-util.h"

#include "conffile.h"
#include "device.h"

#ifdef HAVE_UTIL_H
#include "amutil.h"
#endif
#ifdef HAVE_AMANDA_H
#include "amanda.h"
#include "sockaddr-util.h"
#endif
#include "amjson.h"

#include <string.h>

#ifdef HAVE_REGEX_H
#include <regex.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#include <curl/curl.h>

#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

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

#ifdef HAVE_TIME_H
#include <regex.h>
#endif

#include <openssl/md5.h>

// arbitrarily copied from linux 3.10.0 headers
#define TMPFS_MAGIC   0x01021994

#define __API_FXN_INIT__(x) do { \
    g_debug("API %s << api-device-init >>",__FUNCTION__) \
} while(0)

#define __API_FXN_CALLED__(s3dev) do { \
    if ( (s3dev)->verbose )                                        \
        debug_api_call(s3dev, __FUNCTION__, ""); \
} while(0)

#define __API_LONG_FXN_CHECK__(s3dev,msg) do { \
    if ( (s3dev)->verbose )                                        \
        debug_api_call(s3dev, __FUNCTION__, msg); \
} while(0)

#define __API_LONG_FXN_ERR_RETURN__(s3dev,msg) do { \
    if ( (s3dev)->verbose )                                        \
        debug_api_call(s3dev, __FUNCTION__, msg); \
} while(0)


#ifdef HAVE_SYS_STATFS_H
# include <sys/statfs.h>
#endif

// struct definitions local to this file
//     (opaque outside).
struct _S3_by_thread {
    S3Handle            *s3;
    CurlBuffer           curl_buffer;
    MD5_CTX              md5_ctx;
    volatile guint8    	 at_final; // pseudo-atomic:   eof of upload/download of file was reached
    volatile guint8      done;         // pseudo-atomic:  thread is reusable now
    volatile guint8    	 ahead;        // pseudo-atomic: thread can be cancelled

    objbytes_t		 object_uploadNum;

    const char          *object_subkey; // used for real s3 url
    const char		*object_uploadId;
    objbytes_t	         object_offset;    // current object's offset (for range downloads only)
    GTree               *mp_etag_tree_ref;

    objbytes_t	         xfer_begin;
    objbytes_t	         xfer_end;

    DeviceStatusFlags    errflags;	/* device_status */
    char                *errmsg;	/* device error message */
    GMutex		*now_mutex;
    GMutex		 _now_mutex_store;
    objbytes_t	         dlnow, ulnow; // atomic
    objbytes_t		 dlnow_last, ulnow_last;
    time_t		 time_expired;
    GTree               *gcp_object_tree_ref;
};

typedef char         *(S3CreateObjkey)(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp, guint file, objbytes_t block);
typedef S3_by_thread *(S3WriteThreadCTOR)(S3Device *self, char *objkey, objbytes_t pos);
// typedef void          (*S3WriteSignalEOD)(S3Device *self, S3_by_thread *s3t, objbytes_t eod);
typedef s3_result_t   (S3WriteComplete)(const S3Device *self, S3_by_thread *s3t, char *etag);
typedef s3_result_t   (S3WriteFinish)(S3Device *self, S3_by_thread *s3t);

struct _S3Device {
    Device __parent__;

    char *catalog_filename;
    char *catalog_label;
    char *catalog_header;

    S3_by_thread *s3t;
    S3_by_thread *s3t_temp_free; // use for a free curl instance
    S3_by_thread *volatile xfer_s3t;  // quick-retry for next xfer

    /* S3 access information */
    char *bucket;
    char *prefix;

    /* The S3 access information. */
    char *secret_key;
    char *access_key;
    char *session_token;
    char *user_token;

    /* The Openstack swift information. */
    char *swift_account_id; // swift-v1 only
    char *swift_access_key; // swift-v1 only

    char *username;
    char *password;
    char *tenant_id;
    char *tenant_name;
    char *project_name;
    char *domain_name;

    char *bucket_location;
    char *storage_class;
    char *host;
    char *service_path;
    char *server_side_encryption;
    char *proxy;

    char *ca_info;

    /* Produce verbose output? */
    gboolean verbose;

    /* create the bucket? */
    gboolean create_bucket;

    /* Use SSL? */
    gboolean use_ssl;
    S3_api s3_api;

    /* Throttling */
    objbytes_t max_send_speed;
    objbytes_t max_recv_speed;

    // no write-read race conditions among these
    gboolean leom:1;
    gboolean enforce_volume_limit:1;
    gboolean use_subdomain:1;

    gboolean use_s3_multi_delete:1;          // can try-and-reset
    gboolean use_s3_multi_part_upload:1;     // allowed-and-required both
    gboolean use_padded_stream_uploads:1;    // allowed-and-required both
    gboolean use_chunked:1;

    gboolean allow_s3_multi_part_upload:1;   // prevented in some cases
    gboolean allow_s3_multi_part_copy:1;
    gboolean allow_padded_stream_uploads:1;   // must create full checksum before upload
    gboolean allow_gcp_compose:1;

    guint16      nb_threads;
    guint16      nb_threads_backup;
    guint16      nb_threads_recovery;

    objbytes_t  volume_bytes;
    objbytes_t  volume_limit;

    GThreadPool *thread_pool_delete;
    GThreadPool *thread_pool_write;
    GThreadPool *thread_pool_read;
    GCond       *thread_list_cond;
    GMutex      *thread_list_mutex;
    GCond       _thread_list_cond_store;
    GMutex      _thread_list_mutex_store;
    GSList      *volatile delete_objlist;

    objbytes_t	 next_xbyte;  // next byte to copy full bytes through..
    objbytes_t	 next_ahead_byte; // next byte to prepare a thread for

    objbytes_t	 curr_object_offset;   // current upload-key base (to help range downloads)

    xferbytes_t	 buff_blksize; // memory-sensitive max-size for all buffers
    xferbytes_t	 dev_blksize;     // expected max-size for session reads/writes
    objbytes_t	 end_xbyte;   // key is shorter than this size

    char        *file_objkey; // only if object is entire file

    GTree       *mp_etag_tree; // used for all block uploads
    char        *mp_uploadId;
    guint16	 mp_next_uploadNum; // maximum number allowed is 10_000!!!

    GTree       *mpcopy_etag_tree; // use only for CopyPart blocks 
    const char  *mpcopy_uploadId;  // use only for CopyPart blocks 

    // s3_device_write_block: for creating object-keys 
    S3CreateObjkey    *fxn_create_curr_objkey;
    S3WriteThreadCTOR *fxn_thread_write_factory;
    S3WriteComplete   *fxn_thread_write_complete;
    S3WriteFinish     *fxn_thread_write_finish;

    gboolean	 bucket_made;

    objbytes_t      dltotal;
    objbytes_t      ultotal;

    /* google OAUTH2 */
    char        *client_id;
    char        *client_secret;
    char        *refresh_token;
    char        *project_id;

    gboolean	 reuse_connection;

    gboolean	 read_from_glacier;
    int		 transition_to_glacier; // upload -> days (after 1) until transition
    char         *transition_to_class;  // class ('GLACIER' by default)
    long	 timeout;

    /* CAStor */
    char        *reps;
    char        *reps_bucket;

    GTree       *gcp_object_tree; // used for all store all temporary uploads in GCP
};

/*
 * Class definition
 */
struct _S3DeviceClass {
    DeviceClass __parent__;
};

/*
 * Constants and static data
 */

#define S3_DEVICE_NAME "s3"

#define EOM_EARLY_WARNING_ZONE_BLOCKS 4

/* This goes in lieu of file number for metadata. */
#define SPECIAL_INFIX            "special-"   // found in s3_list values
#define TAPESTART_OBJECT         SPECIAL_INFIX "tapestart"
#define FILESTART_SUFFIX         "-filestart"
#define DATA_OBJECT_SUFFIX       ".data"
#define MULTIPART_OBJECT_SUFFIX  "mp" DATA_OBJECT_SUFFIX
#define MULTIPART_TEMP_SUFFIX    "mp.tocopy" DATA_OBJECT_SUFFIX
#define TEMP_COMPOSE_PART_OBJECT_SUFFIX "cp.tocompose" DATA_OBJECT_SUFFIX 
#define COMPOSE_PART_OBJECT_SUFFIX "cp" DATA_OBJECT_SUFFIX 

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

/*
 * device-specific properties
 */

/* Authentication information for Amazon S3. Both of these are strings. */
DevicePropertyBase device_property_s3_access_key;
DevicePropertyBase device_property_s3_secret_key;
static DevicePropertyBase device_property_s3_session_token;
//#define PROPERTY_S3_SECRET_KEY (device_property_s3_secret_key.ID)
//#define PROPERTY_S3_ACCESS_KEY (device_property_s3_access_key.ID)
#define PROPERTY_S3_SESSION_TOKEN (device_property_s3_session_token.ID)

/* Authentication information for Openstack Swift (swift v1 only). Both of these are strings. */
static DevicePropertyBase device_property_swift_account_id;
static DevicePropertyBase device_property_swift_access_key;
#define PROPERTY_SWIFT_ACCOUNT_ID (device_property_swift_account_id.ID)
#define PROPERTY_SWIFT_ACCESS_KEY (device_property_swift_access_key.ID)

/* Authentication information for Openstack Swift. Both of these are strings. */
static DevicePropertyBase device_property_username;
static DevicePropertyBase device_property_password;
static DevicePropertyBase device_property_tenant_id;
static DevicePropertyBase device_property_tenant_name;
static DevicePropertyBase device_property_project_name;
static DevicePropertyBase device_property_domain_name;
#define PROPERTY_USERNAME (device_property_username.ID)
#define PROPERTY_PASSWORD (device_property_password.ID)
#define PROPERTY_TENANT_ID (device_property_tenant_id.ID)
#define PROPERTY_TENANT_NAME (device_property_tenant_name.ID)
#define PROPERTY_PROJECT_NAME (device_property_project_name.ID)
#define PROPERTY_DOMAIN_NAME (device_property_domain_name.ID)

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

/* Which strotage api to use. */
static DevicePropertyBase device_property_storage_api;
#define PROPERTY_STORAGE_API (device_property_storage_api.ID)

/* Whether to use openstack protocol. */
/* DEPRECATED */
static DevicePropertyBase device_property_openstack_swift_api;
#define PROPERTY_OPENSTACK_SWIFT_API (device_property_openstack_swift_api.ID)

/* Whether to use chunked transfer-encoding */
static DevicePropertyBase device_property_chunked;
#define PROPERTY_CHUNKED (device_property_chunked.ID)

/* Whether to use SSL with Amazon S3. */
static DevicePropertyBase device_property_s3_ssl;
#define PROPERTY_S3_SSL (device_property_s3_ssl.ID)

/* Whether to use subdomain */
static DevicePropertyBase device_property_s3_subdomain;
#define PROPERTY_S3_SUBDOMAIN (device_property_s3_subdomain.ID)

/* Whether to use s3 multi-part upload */
static DevicePropertyBase device_property_s3_multi_part_upload;
#define PROPERTY_S3_MULTI_PART_UPLOAD (device_property_s3_multi_part_upload.ID)

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

/* glacier */
static DevicePropertyBase device_property_read_from_glacier;
#define PROPERTY_READ_FROM_GLACIER (device_property_read_from_glacier.ID)
static DevicePropertyBase device_property_transition_to_glacier;
#define PROPERTY_TRANSITION_TO_GLACIER (device_property_transition_to_glacier.ID)

/* Adding transition to class property. This should be either GLACIER or DEEP_ARCHIVE. Only
 * used for lifecycle rule
 */
static DevicePropertyBase device_property_transition_to_class;
#define PROPERTY_TRANSITION_TO_CLASS (device_property_transition_to_class.ID)

static DevicePropertyBase device_property_timeout;
#define PROPERTY_TIMEOUT (device_property_timeout.ID)

/* CAStor replication values for objects and buckets */
static DevicePropertyBase device_property_s3_reps;
#define PROPERTY_S3_REPS (device_property_s3_reps.ID)
#define S3_DEVICE_REPS_DEFAULT "2"
static DevicePropertyBase device_property_s3_reps_bucket;
#define PROPERTY_S3_REPS_BUCKET (device_property_s3_reps_bucket.ID)
#define S3_DEVICE_REPS_BUCKET_DEFAULT "4"
/* Properties defined in property.h
 * PROPERTY_COMMENT
 * PROPERTY_LEOM
 * PROPERTY_BLOCK_SIZE
 * PROPERTY_REUSE_CONNECTION
 * PROPERTY_VERBOSE
 * PROPERTY_MAX_SEND_SPEED
 * PROPERTY_MAX_RECV_SPEED
 * PROPERTY_NB_THREADS_BACKUP
 * PROPERTY_NB_THREADS_RECOVERY
 * PROPERTY_SSL_CA_INFO
 * PROPERTY_ENFORCE_MAX_VOLUME_USAGE
*/

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
 * @returns: a newly allocated string containing the prefix for the file.
 */
static char *
file_to_prefix(const S3Device *self,
               guint file);

/* Given file and block numbers, return an S3 key.
 *
 * @param self: the S3Device object
 * @param file: the file number
 * @param block: the block within that file
 * @returns: a newly allocated string containing an S3 key.
 */
static char *
file_and_block_to_key(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                      guint file,
                      objbytes_t block);

/* Given file, return an S3 key.
 *
 * @param self: the S3Device object
 * @param file: the file number
 * @returns: a newly allocated string containing an S3 key.
 */
static char *
file_to_single_file_objkey(const S3Device *self, guint file);

static char *
file_to_single_compose_file_objkey(const S3Device *self, guint file);


/* Given file, return an S3 key.
 *
 * @param self: the S3Device object
 * @param file: the file number
 * @returns: a newly allocated string containing an S3 key.
 */
static char *
get_write_file_objkey_offset(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                       guint file,
                       objbytes_t unused G_GNUC_UNUSED);

/* Given file, return an S3 key.
 *
 * @param self: the S3Device object
 * @param file: the file number
 * @returns: a newly allocated string containing an S3 key.
 */
static char *
get_read_file_objkey_offset(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                       guint file,
                       objbytes_t unused G_GNUC_UNUSED);

/* Given the name of a special file (such as 'tapestart'), generate
 * the S3 key to use for that file.
 *
 * @param self: the S3Device object
 * @returns: a newly alocated string containing the S3 special-tapestart key.
 */
static char *
tapestart_key(S3Device *self);

/* Given the name of a special file (such as 'tapestart'), generate
 * the S3 key to use for that file.
 *
 * @param self: the S3Device object
 * @param file: the file number
 * @returns: a newly alocated string containing the S3 file#/filestart key.
 */
static char *
filestart_key(S3Device *self, guint file);

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

/* Find the number of the last file that contains any data (even just a header).
 *
 * @param self: the S3Device object
 * @returns: the last file, or -1 in event of an error
 */
static guint
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

static guint
reset_idle_thread (S3_by_thread *s3t, gboolean verbose);

static void
reset_file_state(S3Device *self);

static S3_by_thread  *
claim_free_thread(const S3Device * self, guint16 next_ind, guint16 ctxt_limit, gboolean wait_flag);

static guint
setup_active_thread(S3Device *self, S3_by_thread *s3t, const char *newkey, objbytes_t pos, signed_xferbytes_t down_bytes);

static gboolean
catalog_open(S3Device *self);

static gboolean
catalog_reset(S3Device *self,
              char     *header,
              char     *label);

static gboolean
catalog_remove(S3Device *self);

static gboolean
catalog_close(S3Device *self);

/*
 * class mechanics */

static void
s3_device_init(S3Device * o, gpointer data G_GNUC_UNUSED);

static void
s3_device_class_init(S3DeviceClass * c, gpointer data G_GNUC_UNUSED);

static void
s3_device_base_init(S3DeviceClass *c);

static void
s3_device_at_finalize(GObject * o);

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

static gboolean s3_device_set_session_token_fn(Device *self,
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

static gboolean s3_device_set_project_name(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_domain_name(Device *self,
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

static gboolean s3_device_set_read_from_glacier_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_transition_to_glacier_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_transtion_to_class_fn(Device *self,
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

static gboolean s3_device_set_chunked_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_ssl_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_reuse_connection_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_timeout_fn(Device *self,
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

static gboolean s3_device_set_s3_multi_part_upload(Device *self,
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

static gboolean s3_device_set_reps_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean s3_device_set_reps_bucket_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static void s3_thread_read_session(gpointer thread_data, 
				   gpointer data);

static void s3_thread_write_session(gpointer thread_data, 
				    gpointer data);

static void s3_thread_delete_session( gpointer thread_data, gpointer data );

static s3_result_t s3_thread_multi_delete(Device *pself, S3_by_thread *s3t, GSList **d_objectsp);


static int s3_thread_xferinfo_func( void *thread_data,
				    curl_off_t dltotal, curl_off_t dlnow,
				    curl_off_t ultotal, curl_off_t ulnow);

static int s3_thread_progress_func( void *thread_data,
				    double dltotal, double dlnow,
				    double ultotal, double ulnow);

static int start_read_ahead(Device * pself, 
			       int max_reads);

static S3_by_thread *find_active_file_thread(S3Device * self, int file, objbytes_t pos, char **pkey);
static gboolean make_bucket(Device * pself);


/* Wait that all threads are done */
static void s3_wait_threads_done(const S3Device *self);
static void s3_cancel_busy_threads(S3Device *self, gboolean do_truncate);

/*
 * virtual functions for device API */

static void
s3_device_open_device(Device *pself, char *device_name,
		  char * device_type, char * device_node);

static gboolean
s3_device_create(Device *pself);

static DeviceStatusFlags 
s3_device_read_label(Device * self);

static gboolean
s3_device_start(Device * self,
                DeviceAccessMode mode,
                char * label,
                char * timestamp);

static gboolean
s3_device_finish(Device * self);

static objbytes_t
s3_device_get_bytes_read(Device * self);

static objbytes_t
s3_device_get_bytes_written(Device * self);

static gboolean
s3_device_start_file(Device * self,
                     dumpfile_t * jobInfo);

static DeviceWriteResult
s3_device_write_block(Device * self,
                      guint size,
                      gpointer data);
static gboolean
s3_device_finish_file(Device * self);

static gboolean
s3_device_get_keys_restored(Device *pself,
                         guint file);

static dumpfile_t*
s3_device_seek_file(Device *pself,
                    guint file);

static gboolean
s3_device_seek_block(Device *pself,
                     objbytes_t block);

static int
s3_device_read_block(Device   *pself,
                     gpointer  data,
                     int    *size_req,
                     int     max_block);

static gboolean
s3_device_recycle_file(Device *pself,
                       guint file);

static gboolean
s3_device_erase(Device *pself);

static gboolean
s3_device_set_reuse(Device *pself);

static gboolean
s3_device_set_no_reuse(Device *pself,
		       char   *label,
		       char   *datestamp);

/*
 * Private functions
 */

static S3_by_thread *
thread_device_write_factory(S3Device *self, char *objkey, objbytes_t pos);

static S3_by_thread *
thread_buffer_write_factory(S3Device *self, char *objkey, objbytes_t pos);

static S3_by_thread *
thread_write_init_mp_factory(S3Device *self, char *objkey, objbytes_t pos, objbytes_t partlen);

static void
init_block_xfer_hooks(S3Device *self);

static s3_result_t
initiate_multi_part_upload(const S3Device *self, S3_by_thread *s3t);

static gboolean
check_at_leom(S3Device *self,
                objbytes_t size);

static gboolean
check_at_peom(S3Device *self,
                objbytes_t size);

static gint 
gptr_cmp(gconstpointer a, gconstpointer b, gpointer data);

#define MIX_RESPCODE_AND_S3ERR(rsp,err)  ( (rsp)<<16 | (int)err )

static char *
file_to_prefix(const S3Device *self,
               guint file)
{
    char *prefix = g_strdup_printf("%sf%08x-", self->prefix, file);
    g_assert(strlen(prefix) <= S3_MAX_KEY_LENGTH);
    return prefix;
}

static char *
file_and_block_to_compose_temp_key(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                      guint file,
                      objbytes_t pos)
{
    objbytes_t block = pos/self->buff_blksize;
    char *s3_key = g_strdup_printf("%sf%08x-c%010lx-" TEMP_COMPOSE_PART_OBJECT_SUFFIX,
                                   self->prefix, file, block);
    if (offsetp) *offsetp = block*self->buff_blksize; // end of key (+2**23--2**29 max)
    if (eodp)    *eodp = (block+1)*self->buff_blksize; // end of key (+2**23--2**29 max)
    g_assert(strlen(s3_key) <= S3_MAX_KEY_LENGTH);
    return s3_key;
}

static char *
file_and_block_to_key(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                      guint file,
                      objbytes_t pos)
{
    objbytes_t block = pos/self->dev_blksize;
    char *s3_key = g_strdup_printf("%sf%08x-b%016llx" DATA_OBJECT_SUFFIX,
                                   self->prefix, file, (long long unsigned int)block);
    if (self->dev_blksize >= G_MAXINT64) block = 0; // protect from crazy calculations
    if (offsetp) *offsetp = block*self->dev_blksize; // end of key (+2**16--2**29 max)
    if (eodp)    *eodp = (block+1)*self->dev_blksize; // end of key (+2**16--2**29 max)
    g_assert(strlen(s3_key) <= S3_MAX_KEY_LENGTH);
    return s3_key;
}

static char *
file_and_block_to_temp_key(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                      guint file,
                      objbytes_t pos)
{
    objbytes_t block = pos/self->dev_blksize;
    char *s3_key = g_strdup_printf("%sf%08x-b%010llx-" MULTIPART_TEMP_SUFFIX,
                                   self->prefix, file, (long long unsigned int)block);
    if (self->dev_blksize >= G_MAXINT64) block = 0; // protect from crazy calculations
    if (offsetp) *offsetp = block*self->dev_blksize; // end of key (+2**23--2**29 max)
    if (eodp)    *eodp = (block+1)*self->dev_blksize; // end of key (+2**23--2**29 max)
    g_assert(strlen(s3_key) <= S3_MAX_KEY_LENGTH);
    return s3_key;
}

static char *
file_to_single_file_objkey(const S3Device *self, guint file)
    { return g_strdup_printf("%sf%08x-" MULTIPART_OBJECT_SUFFIX, self->prefix, file); }

static char *
file_to_single_compose_file_objkey(const S3Device *self, guint file)
    { return g_strdup_printf("%sf%08x-" COMPOSE_PART_OBJECT_SUFFIX, self->prefix, file); }

static char *
get_write_file_objkey_offset(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                       guint file_unused G_GNUC_UNUSED,
                       objbytes_t pos_unused G_GNUC_UNUSED)
{
    if (offsetp) *offsetp = 0;
    if (eodp)    *eodp = G_MAXINT64;
    return g_strdup(self->file_objkey);
}

static char *
get_read_file_objkey_offset(const S3Device *self, objbytes_t *offsetp, objbytes_t *eodp,
                               guint file_unused G_GNUC_UNUSED,
                               objbytes_t pos_unused G_GNUC_UNUSED)
{
    char *s3_key = get_write_file_objkey_offset(self, offsetp, NULL, -1, 0);
    if (eodp && eodp != &self->end_xbyte)
        *eodp = self->end_xbyte; // retain original end
    g_assert(strlen(s3_key) <= S3_MAX_KEY_LENGTH);
    return s3_key;
}


static char *
tapestart_key(S3Device *self)
{
    char *s3_key = g_strdup_printf("%s" TAPESTART_OBJECT, self->prefix);
    g_assert(strlen(s3_key) <= S3_MAX_KEY_LENGTH);
    return s3_key;
}

static char *
filestart_key(S3Device *self, guint file)
{
    char *s3_key = g_strdup_printf("%sf%08x" FILESTART_SUFFIX, self->prefix, file);
    g_assert(strlen(s3_key) <= S3_MAX_KEY_LENGTH);
    return s3_key;
}


static gint
gptr_cmp(
    gconstpointer a,
    gconstpointer b,
    gpointer data G_GNUC_UNUSED)
{
    return (a < b ? -1 : (a > b) );
}

static S3Handle  *
get_temp_curl(const S3Device * self)
{
    S3_by_thread *s3t;

    g_mutex_lock(self->thread_list_mutex);
    if (self->s3t_temp_free) {
	g_mutex_unlock(self->thread_list_mutex);
    	return self->s3t_temp_free->s3;
    }
    s3t = claim_free_thread(self, 0, self->nb_threads, TRUE);
    // reset back to idle
    if (!s3t) {
        g_mutex_unlock(self->thread_list_mutex);
        return NULL;
    }

    s3t->done = TRUE; // curl-interface can be used at next chance..

    if (device_in_error(self)) {
        // notify all that claimed-thread will not be used at all..
        g_cond_broadcast(self->thread_list_cond);
        g_mutex_unlock(self->thread_list_mutex);
        return NULL;
    }

    // adjust for cache (reset under lock)...
    ((S3Device *)self)->s3t_temp_free = s3t;
    g_mutex_unlock(self->thread_list_mutex);

    return s3t->s3;
}


struct { 
    S3CreateObjkey * const xferkey_fxn; 
    const char *xfertype; 
} const s_xfertypes[] = {
    { &file_and_block_to_key, "Blocks" },
    { &get_write_file_objkey_offset, "WrMPart" },
    { &file_and_block_to_temp_key, "WrMPartCopy" },
    { &get_read_file_objkey_offset, "RdWhole" },
    { NULL, "" },
};

static void debug_api_call(S3Device *self, const char *fxn, const char *msg)
{
    const Device *pself = DEVICE(self);
    const char *eof = ( pself->is_eof ? " RdEOF" : "" );
    const char *eom = ( pself->is_eom ? " WrEOM" : "" );
    const char *stat = ( pself->status ? " !Err!" : "" );
    const char *mode = ( IS_WRITABLE_ACCESS_MODE(pself->access_mode) ? "Up" : 
                            pself->access_mode == ACCESS_READ ? "Dn" :
                            "_" );
    const char *glacier = ( self->read_from_glacier ? " ChkGlac" : 
                            self->transition_to_glacier >= 0 ? " ToGlac" : 
                            "" );
    const char *endpoint = ( self->end_xbyte < G_MAXINT64           ? " Fixlen" : "" );
    const char *xfertype = "";

    // before 'start_file' or 'seek_file' or after finish_file
    if ( ! pself->in_file && ! pself->access_mode ) {
        // char buff[40] = { 0 };

        // if ( self->max_send_speed || self->max_recv_speed )
        //     snprintf(buff,sizeof(buff),"upmax=%lukps dnmax=%lukps",(self->max_send_speed>>10),(self->max_recv_speed>>10))
        // buff,
        // self->storage_class,

        g_debug("----- API[%d] << %s() >> :%s%s blk=%.1fM<->%.1fM [%s%s%s%s%s%s%s] %s",
              (int) self->s3_api, 
              fxn,
              ( self->host ? " " : "" ),
              ( self->host ? : "" ),
              (DEVICE(self)->block_size / 1024.0f / 1024.0f), (self->buff_blksize / 1024.0f / 1024.0f),

              eof, eom, stat, glacier,
              
              ( ! self->bucket_made && self->create_bucket ? " Newbkt" : 
                ! self->bucket_made ? " Oldbkt" : 
                 "" ),
              ( self->proxy ? " Proxy" : ""  ),
              ( self->reuse_connection ? "" : " Discon" ),
              msg);
              ;
        return;
    }

    // assign xfertype to all strings in table until fxn ptr matches or last is found
    for(int i=0 ; *(xfertype=s_xfertypes[i].xfertype) ; ++i) 
        { if (s_xfertypes[i].xferkey_fxn == self->fxn_create_curr_objkey) 
            break; 
        }

    if ( *xfertype && pself->file > 0 && self->next_ahead_byte )
    {
        // inside file *after* I/O begun

        g_debug("----- API-%s << %s() >> : F#%04x:B#%04lx [%s%s%s%s%s%s:%s] %s",
              mode, fxn, pself->file, pself->block, 
                   eof, eom, stat, glacier,
                  ( self->buff_blksize < self->dev_blksize ? "<" : "" ),
                  endpoint,
                  xfertype,
              msg);
        return;
    } 

    // only after 'start_file' or 'seek_file' but before first read/write or finish_file

    g_debug("----- API#%d-%s << %s() >> : #%d [%s%s%s%s%s%s%s%s%s%s%s] %s",
          self->s3_api, mode, fxn, pself->file,
               eof, eom, stat, glacier,
              ( self->use_ssl ? "" : " Insec" ),
              ( self->leom && self->enforce_volume_limit && self->volume_limit ? " Lim" : "" ),
              ( self->use_subdomain ? " Dom" : "" ),
              ( self->use_s3_multi_delete ? " =MDel" : "" ),
              ( self->use_s3_multi_part_upload && self->allow_s3_multi_part_copy ? " =MCopy" : 
                    self->allow_s3_multi_part_upload && self->allow_s3_multi_part_copy ? " mpcopy" : 
                    self->use_s3_multi_part_upload ? " =MP" : 
                    self->allow_s3_multi_part_upload ? " mp" : 
                    "" ),
              ( self->use_padded_stream_uploads ? " =Pad" : 
                    self->allow_padded_stream_uploads ? " pad" : 
                    "" ),
              ( self->use_chunked ? " =Chunk" : "" ),
          msg);
}

static gboolean
write_amanda_header(S3Device *self,
                    char *label,
                    char * timestamp)
{
    CurlBuffer amanda_header_tmp = { 0 }; // to load once only
    CurlBuffer *s3buf_hdr_tmp = &amanda_header_tmp;
    char * key = NULL;
    gboolean result;
    dumpfile_t * dumpinfo = NULL;
    Device *d_self = DEVICE(self);
    size_t header_size;
    char *hdr;
    char *errmsg = NULL;
    DeviceStatusFlags errflags = DEVICE_STATUS_SUCCESS;

    /* build the header */
    header_size = 0; /* no minimum size */
    dumpinfo = make_tapestart_header(DEVICE(self), label, timestamp);

    hdr = device_build_amanda_header(DEVICE(self), dumpinfo, &header_size);

    if (!s3_buffer_load(s3buf_hdr_tmp, hdr, header_size, self->verbose)) {
        errmsg = g_strdup(_("Amanda tapestart header won't fit in a single block!"));
        errflags = DEVICE_STATUS_DEVICE_ERROR;
        result = FALSE;
	goto cleanup;
    }

    // flag to cancel/split if enabled
    if(check_at_leom(self, header_size))
        d_self->is_eom = TRUE;

    // flag to cancel/split with error now
    if(check_at_peom(self, header_size)) {
        d_self->is_eom = TRUE;
        errmsg = g_strdup(_("No space left on device")),
        errflags = DEVICE_STATUS_DEVICE_ERROR;
        result = FALSE;
        goto cleanup;
    }
 
    g_assert(header_size < G_MAXUINT); /* for cast to guint */

    /* write out the header and flush the uploads. */
    catalog_reset(self, s3_buffer_data_func(s3buf_hdr_tmp), label);

    key = tapestart_key(self); // prefix+"special"+"-tapestart"
    result = s3_upload(get_temp_curl(self), self->bucket, key, FALSE,
		       S3_BUFFER_READ_FUNCS, s3buf_hdr_tmp,
                       NULL, NULL);
    if (!result) {
        errmsg = g_strdup_printf(_("While writing amanda header: %s"), s3_strerror(get_temp_curl(self))); 
        errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
        // result = FALSE;
	goto cleanup;
    }
    // result = TRUE;

    self->volume_bytes += header_size;
    d_self->header_block_size = header_size; // save this size

    {
        // swap new for old...
        dumpfile_t *t = dumpinfo;
        dumpinfo = d_self->volume_header; // free this old one...
        d_self->volume_header = t;
    }

cleanup:
    g_free(key);
    if (errflags != DEVICE_STATUS_SUCCESS)
        device_set_error(d_self, errmsg, errflags);
    dumpfile_free(dumpinfo); // free the one *not* in d_self->volume_header
    s3_buffer_destroy(s3buf_hdr_tmp);
    return result;
}

/* Convert an object name into a file number, assuming the given prefix
 * length. Returns -1 if the object name is invalid, or 0 if the object name
 * is a "special" key. */
static guint
key_to_file(const char *preprefix, const char * key) {
    int file;
    int i;
    int n = strlen(preprefix);

    /* skip any non-matching prefix */
    if (strncmp(preprefix,key,n) != 0)
	return ~0U;

    key += n;

    // <match-prefix> + "special-" ... as it stops at '-'
    if (g_str_has_prefix(key, SPECIAL_INFIX)) {
        return 0;
    }

    /* check that key starts with 'f' */
    if (key[0] != 'f')
	return G_MAXUINT;
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

static s3_result_t
initiate_multi_part_upload(const S3Device *self, S3_by_thread *s3t)
{
    Device *pself = DEVICE(self);
    char *last_uploadID;
    char *errmsg = NULL;
    DeviceStatusFlags errflags = DEVICE_STATUS_SUCCESS;

    if (s3t->object_uploadId || s3t->object_uploadNum) {
        errmsg = g_strdup_printf(_("Cannot interrupt thread multipart upload: part#%d uploadId=%s"),
                                   (int) s3t->object_uploadNum, s3t->object_uploadId);
        errflags = DEVICE_STATUS_DEVICE_ERROR;
        goto error;
    }
    if (!s3t->object_subkey) { // must be filled
        errmsg = g_strdup_printf(_("Thread has no object key for multipart upload"));
        errflags = DEVICE_STATUS_DEVICE_ERROR;
        goto error;
    }

    {
        g_mutex_unlock(self->thread_list_mutex); // allow unlocked
        // use "object_uploadNum" for ordered writes
        last_uploadID = s3_initiate_multi_part_upload(s3t->s3, self->bucket, s3t->object_subkey);
        g_mutex_lock(self->thread_list_mutex); // allow unlocked
    }

    if (!last_uploadID) {
        errmsg = g_strdup_printf(_("No uploadID was parsed for new multi-part upload: %s"), s3t->object_subkey);
        errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
        goto error;
    }

    s3t->object_uploadId = g_strdup(last_uploadID);// use "object_uploadNum" for ordered writes
    s3t->object_uploadNum = 1;

    s3t->mp_etag_tree_ref = ( self->mp_etag_tree ? : g_tree_new_full(gptr_cmp, NULL, NULL, g_free) );
    g_tree_ref(s3t->mp_etag_tree_ref); // count this reference

    return S3_RESULT_OK;

error:
    // and ignore the pself->block_size
    if (errflags != DEVICE_STATUS_SUCCESS)
        device_set_error(pself, errmsg, errflags);
    return S3_RESULT_FAIL;
}


typedef struct { 
    gconstpointer orig_pos; // cast to objbytes_t
    gconstpointer next_pos; // cast to objbytes_t
} TreeNextEtag;

static int
cmp_least_upper_bound(gconstpointer pos, gconstpointer userdata)
{
    TreeNextEtag *last = (TreeNextEtag *) userdata;
    if (pos <= last->orig_pos)
        return 1; // seek higher pos values
    last->next_pos = min(last->next_pos,pos);
    return -1; // seek lower (nearest) pos values
}

static int
cmp_greatest_lower_bound(gconstpointer pos, gconstpointer userdata)
{
    TreeNextEtag *last = (TreeNextEtag *) userdata;
    if (pos >= last->orig_pos)
        return -1; // seek lower pos values
    ++pos;            // roll over MAXUINT64, no optim allowed
    ++last->next_pos; // roll over MAXUINT64, no optim allowed
    last->next_pos = max(last->next_pos,pos); 
    --last->next_pos; // roll back to MAXUINT64, no optim allowed
    return 1; // seek higher (nearest) pos values
}

static objbytes_t
get_least_next_elt(GTree *tree, objbytes_t pos)
{
    TreeNextEtag srch = { GSIZE_TO_POINTER(pos), GSIZE_TO_POINTER(G_MAXUINT64) };
    g_tree_search(tree, cmp_least_upper_bound, &srch); // only goes to leaf
    return GPOINTER_TO_SIZE(srch.next_pos);
}

static objbytes_t
get_greatest_prev_elt(GTree *tree, objbytes_t pos)
{
    TreeNextEtag srch = { GSIZE_TO_POINTER(pos), GSIZE_TO_POINTER(G_MAXUINT64) };
    g_tree_search(tree, cmp_greatest_lower_bound, &srch); // only goes to leaf
    return GPOINTER_TO_SIZE(srch.next_pos);
}

static int
get_remaining_parts(GTree *const tree, objbytes_t blk, objbytes_t lastpos, int missing, objbytes_t *pblk)
{
    objbytes_t blksz;
    
    if (!missing) {
        *pblk = blk;
        return missing; // nothing to find...
    }
    
    blksz = ( lastpos - blk ) / missing;   // divide range up exactly..
    g_assert( ( lastpos - blk ) % missing == 0 ); // [range must divide cleanly]

    // quickly confirm base-block is ready (might be last?)
    if (!g_tree_lookup(tree, GSIZE_TO_POINTER(blk)))
        return missing; // no blocks were found at all

    // 1) mark position checked, return if at/beyond end
    // 3) detect next ... and return at current pos if missing
    // 4) record find, 5) advance to next position
    for ( ; (*pblk=blk) < lastpos && get_least_next_elt(tree, blk) == blk + blksz
           ; --missing, (blk+=blksz) )
        { }

    return missing;
}

static int
get_remaining_mp_parts(const S3_by_thread *s3t, objbytes_t *pblk)
{
    return get_remaining_parts(s3t->mp_etag_tree_ref, 
                                  s3t->object_offset,  // start of first block
                                  s3t->xfer_begin,     // end of earlier blocks (constant size blocks)
                                  s3t->object_uploadNum-1, // count of earlier blocks 
                                  pblk); // position return
}

static int
get_remaining_cp_parts(const S3_by_thread *s3t, objbytes_t *pblk)
{
    objbytes_t blocknum = s3t->object_uploadNum;
    objbytes_t start = blocknum - ( (blocknum-1) % GCP_COMPOSE_COUNT );
    return get_remaining_parts(s3t->gcp_object_tree_ref, 
                                  start,        // blocknum of first block
                                  blocknum,     // blocknum of final block (current thread)
                                  blocknum - start, // count of earlier blocks 
                                  pblk); // position return
}

static s3_result_t thread_write_close_compose_append(const S3Device *self, S3_by_thread *s3t)
{
    Device *pself = DEVICE(self);
    GSList *delete_objlist = NULL;
    int retries = GCP_COMPOSE_COUNT*2;
    int last_compose_retries = retries;
    int missing_block_retries = retries;
    GTree *const tree = s3t->gcp_object_tree_ref;
    objbytes_t blocknum = s3t->object_uploadNum;
    const objbytes_t firstNewBlk = blocknum - ((blocknum-1) % GCP_COMPOSE_COUNT); 
    int missing = 0;
    GString *buf;
    objbytes_t pos;

    g_assert( ! g_mutex_trylock(self->thread_list_mutex) );  // confirm lock in effect

    while ( (pos=get_greatest_prev_elt(tree,firstNewBlk)) < G_MAXINT64 && --last_compose_retries ) {
        if (self->verbose)
            g_debug("%s: Previous Compose request pending....previous key %#010lx: Wait to complete",__FUNCTION__,pos);
        g_cond_wait_until(self->thread_list_cond, self->thread_list_mutex, g_get_monotonic_time () + 5 * G_TIME_SPAN_SECOND);
    }

    if ( !last_compose_retries ) {
        g_warning("%s: ABORTED: Previous Compose request pending too long....[pos=%#010lx]",__FUNCTION__,pos);
        return S3_RESULT_FAIL;
    }

    if( self->verbose)
        g_debug("%s: No previous compose request pending",__FUNCTION__);

    buf = g_string_new("<ComposeRequest>\n");

    if (blocknum > GCP_COMPOSE_COUNT) { 
        g_string_append_printf(buf, "<Component>\n    <Name>%s</Name>\n   </Component>\n",self->file_objkey);
    }

    // always returns at least one 'found' in number
    while( (missing=get_remaining_cp_parts(s3t, &pos)) && --missing_block_retries)
    {
        objbytes_t missingblk = 1 + (pos/self->buff_blksize);
        if (self->verbose)
            g_debug("%s: (n=%d) Waiting for key=@%#010lx in gcp_object_tree_ref",__FUNCTION__, missing, missingblk);
        g_cond_wait_until(self->thread_list_cond, self->thread_list_mutex, g_get_monotonic_time () + 5 * G_TIME_SPAN_SECOND);
    }

    if (!missing_block_retries) {
        g_warning("%s: ABORTED: Required entries waiting too long....[pos=%#010lx blk=%#010lx]",__FUNCTION__,pos, 1 + (pos/self->buff_blksize));
        return S3_RESULT_FAIL;
    }

    for( objbytes_t partNum = firstNewBlk ; partNum <= blocknum ; ++partNum )
    {
        s3_object *object = g_new0(s3_object, 1);

        object->key = g_strdup((char*)g_tree_lookup(tree,GSIZE_TO_POINTER(partNum))); 

        g_assert(object->key);
        g_string_append_printf(buf, "<Component>\n   <Name>%s</Name>\n   </Component>\n", object->key);

        if( self->verbose)  
            g_debug("%s: Fetch from gcp object tree key=%010llx,Value=%s", __FUNCTION__,(long long unsigned int)partNum,object->key);

        delete_objlist = g_slist_append(delete_objlist,object);
        g_tree_remove(tree, GSIZE_TO_POINTER(partNum));
    }

    g_string_append_printf(buf,"</ComposeRequest>\n");

    {
        int    nIndex;
        GSList *node;
        CurlBuffer upload_tmp = { 0 }; // to load once only
        CurlBuffer *s3buf_tmp = &upload_tmp;
        s3_result_t result = S3_RESULT_OK;

        s3_buffer_load_string(s3buf_tmp, g_string_free(buf, FALSE), self->verbose);

        g_mutex_unlock(self->thread_list_mutex);
        result = s3_compose_append_upload(s3t->s3,
                                         self->bucket, self->file_objkey,
                                         S3_BUFFER_READ_FUNCS, s3buf_tmp);
        g_mutex_lock(self->thread_list_mutex);

        if (result != S3_RESULT_OK) {
            if (device_in_error(pself))
                return result;
            s3t->errflags = DEVICE_STATUS_DEVICE_ERROR;
            s3t->errmsg = g_strdup_printf(_("Compose Append failed: '%s'"), self->file_objkey);
            g_slist_free_full(delete_objlist, free_s3_object);
            return result;
        }

        for (nIndex = 0; (node=g_slist_nth(delete_objlist, nIndex)) ; nIndex++) 
        {
            s3_object *object1 = (s3_object *)node->data;

            g_mutex_unlock(self->thread_list_mutex);
            result = s3_delete(s3t->s3, self->bucket, object1->key);
            g_mutex_lock(self->thread_list_mutex);
           
            if (result != S3_RESULT_OK) {
                g_debug("%s: Delete key failed: %s", __FUNCTION__, s3_strerror(s3t->s3));
                g_slist_free_full(delete_objlist, free_s3_object);
                return S3_RESULT_FAIL;
            }
        }
    }

    g_slist_free_full(delete_objlist, free_s3_object);
    return S3_RESULT_OK;
}

//
// NOTE: do *not* modify globals/self-fields ... as it causes race conditions
//
static s3_result_t
thread_mp_upload_combine(const S3Device *self, S3_by_thread *s3t)
{
    CurlBuffer upload_tmp = { 0 }; // to load once only
    CurlBuffer *s3buf_tmp = &upload_tmp;
    Device *pself = DEVICE(self);
    s3_result_t result = S3_RESULT_OK;
    objbytes_t blkOff = 0;
    int missing = 0;
    int retries = self->nb_threads_backup * self->nb_threads_backup; 
    // preserve before lock is first released...

    // called with thread_list_lock
    g_assert( ! g_mutex_trylock(self->thread_list_mutex) ); // must be locked now...
    g_assert( s3t->xfer_begin < s3t->xfer_end ); // last part cannot be zero-length

    // if called from thread context.. use all local info

    if (device_in_error(self))
        return S3_RESULT_FAIL;

    if (!s3t || !s3t->at_final) {
        s3t->errflags = DEVICE_STATUS_DEVICE_ERROR;
        s3t->errmsg = g_strdup_printf(_("Multi-part thread is not at EOF: '%s'"), s3t->object_subkey);
        return S3_RESULT_FAIL; // bad state
    }

    // one of the block-uploads reported a global error
    if (device_in_error(pself))
        return S3_RESULT_FAIL;

    // always returns at least one 'found' in number
    while( (missing=get_remaining_mp_parts(s3t, &blkOff)) )
    {
        // failed once yet?
        if ( !--retries ) {
            g_error("%s: (left=%d) FAILED: missing completion-tree need %ld/%d step pos=#%ld/<=%#lx range=[%#lx-%#lx] key=%s uploadid=[...%s]", __FUNCTION__, 
                  missing, 
                  s3t->object_uploadNum, g_tree_nnodes(s3t->mp_etag_tree_ref), 
                  s3t->object_uploadNum - missing, blkOff,
		  s3t->object_offset, s3t->xfer_begin,
                  s3t->object_subkey,
                  s3t->object_uploadId + strlen(s3t->object_uploadId)-5);
            return S3_RESULT_FAIL;                                             ////// RETURN SUCCESS
        }

        if (self->verbose)
            g_debug("%s: (left=%d) WAITING: missing completion-tree need %ld/%d step pos=#%ld/<=%#lx?? range=[%#lx-%#lx] key=%s uploadid=[...%s]", __FUNCTION__, 
                  missing, 
                  s3t->object_uploadNum, g_tree_nnodes(s3t->mp_etag_tree_ref), 
                  s3t->object_uploadNum - missing, blkOff,
		  s3t->object_offset, s3t->xfer_begin,
                  s3t->object_subkey,
                  s3t->object_uploadId + strlen(s3t->object_uploadId)-5);

        // wait for notice of new thread completing
        g_cond_wait(self->thread_list_cond, self->thread_list_mutex); 
    }

    ////////////////////////////////////////// safe to upload the parts (when all present)

    {
	GString *buf = g_string_new("<CompleteMultipartUpload>\n");
        int partNum;
        objbytes_t blkOff;

        // add all etag entries to string
        // [ always must re-create correct 1-10,000 (max) numbering of parts ]
        for( partNum=1, blkOff=s3t->object_offset
               ; partNum <= (int) s3t->object_uploadNum
                  ; ++partNum, blkOff=get_least_next_elt(s3t->mp_etag_tree_ref, blkOff) )
        {
            g_string_append_printf(buf,
                   "  <Part>\n    <PartNumber>%d</PartNumber>\n    <ETag>%s</ETag>\n  </Part>\n",
                   partNum, (char*)g_tree_lookup(s3t->mp_etag_tree_ref, GSIZE_TO_POINTER(blkOff)));

            g_debug("%s: delivered from completion-tree pos=#%d/%#lx key=%s uploadid=[...%s]", __FUNCTION__, 
                  partNum, blkOff, s3t->object_subkey,
                  s3t->object_uploadId + strlen(s3t->object_uploadId)-5);
            g_tree_remove(s3t->mp_etag_tree_ref, GSIZE_TO_POINTER(blkOff));
        }

	g_string_append_printf(buf, "</CompleteMultipartUpload>\n");

        s3_buffer_load_string(s3buf_tmp, g_string_free(buf, FALSE), self->verbose);
    }

    /////////////////////////////// prepared with string: now unlock
    {
        g_mutex_unlock(self->thread_list_mutex); // allow unlocked
        result = s3_complete_multi_part_upload(s3t->s3,
                             self->bucket, s3t->object_subkey, s3t->object_uploadId,
                             S3_BUFFER_READ_FUNCS, s3buf_tmp);
       g_mutex_lock(self->thread_list_mutex); // allow unlocked
    }
    /////////////////////////////// prepared with string: now unlock

    if (result != S3_RESULT_OK) {
        if (device_in_error(pself)) 
            return result;
        s3t->errflags = DEVICE_STATUS_DEVICE_ERROR;
        s3t->errmsg = g_strdup_printf(_("Multi-part completion failed: '%s'"), s3t->object_subkey);
        return result;
    }
    return S3_RESULT_OK;
}



static int
abort_partial_upload(S3Device *self)
{
    Device *pself = DEVICE(self);
    gboolean result;
    GSList *partlist;
    s3_object *part = NULL;
    DeviceAccessMode oldmode = pself->access_mode;

    /////// WARNING... LOCKING AND MESSING WITH THE DEVICE SETTINGS...
    /////// CALL ONLY FROM TOP API THREAD

    // immediately cancel ALL threads instantly... [no flushing allowed]
    {
        g_mutex_lock(pself->device_mutex);
        pself->access_mode = ACCESS_READ; // STOP IMMEDIATELY
        s3_device_finish_file(pself);
        pself->access_mode = oldmode;     // restore mode as before
        g_mutex_unlock(pself->device_mutex);
    }

    // all multipart-uploads from "?uploads" ... for EVERY file AND temporary
    result = s3_list_keys(get_temp_curl(self), self->bucket, "uploads", self->prefix, NULL, 0,
                          &partlist, NULL);

    if (!result) {
        if (device_in_error(pself)) 
            return result;
	device_set_error(pself,
	    g_strdup_printf(_("While listing partial upload: %s"), s3_strerror(get_temp_curl(self))),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
        return FALSE;
    }

    for (; partlist; partlist = g_slist_remove(partlist, partlist->data)) {
	part = (s3_object *)partlist->data;
	g_debug("%s: abort of %s / %s", __FUNCTION__, part->key, part->mp_uploadId);
	s3_abort_multi_part_upload(get_temp_curl(self), self->bucket, part->key, part->mp_uploadId);
	free_s3_object(part);
    }
    return TRUE;
}

/* Find the number of the last file that contains any data (even just a header).
 * Returns -1 in event of an error
 */
static guint
find_last_file(S3Device *self)
{
    gboolean result;
    GSList *keys;
    guint last_file = 0;
    // Device *d_self = DEVICE(self);

    // get common-prefixes from <prefix>*-* subkeys
    result = s3_list_keys(get_temp_curl(self), self->bucket, NULL, self->prefix, "-", 0,
                          &keys, NULL);
    if (!result) {
	// device_set_error(d_self,
	//     g_strdup_printf(_("While listing S3 keys: %s"), s3_strerror(get_temp_curl(self))),
	//     DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
        return G_MAXUINT;
    }

    for (; keys; keys = g_slist_remove(keys, keys->data)) 
    {
	s3_object *object = keys->data;
        guint file = key_to_file(self->prefix, ( object->prefix ? : object->key ));

        /* and if it's the last, keep it */
        if (file > last_file)
            last_file = file;
    }

    return last_file;
}

/* Find the number of the file following the requested one, if any.
 * Returns 0 if there is no such file or -1 in event of an error
 */
static guint
find_next_file(S3Device *self, guint last_file) {
    gboolean result;
    GSList *keylist;
    guint next_file = G_MAXUINT;
    // Device *d_self = DEVICE(self);

    // get common-prefixes from <prefix>*-* subkeys
    result = s3_list_keys(get_temp_curl(self), self->bucket, NULL, self->prefix, "-", 0,
                          &keylist, NULL);
    if (!result) {
	// device_set_error(d_self,
	//     g_strdup_printf(_("While listing S3 keys: %s"), s3_strerror(get_temp_curl(self))),
	//     DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
        return G_MAXUINT;
    }

    // "...special-tapestart" is always zeroth file
    // else, decode hex file# in "...fXXXXXXXX-*"
    for (; keylist; keylist = g_slist_remove(keylist, keylist->data)) 
    {
        guint file;
	s3_object *object = keylist->data;

        file = key_to_file(self->prefix, ( object->prefix ? : object->key ));

        if (file == G_MAXUINT) {
            continue; // ignore confusing key-names as unrelated ...
            /* Set this in case we don't find a next file; this is not a
             * hard error, so if we can find a next file we'll return that
             * instead. */
            // next_file = -1;
        }

	// update next-file until its the closest one > last_file
        if (last_file < file && file < next_file)
            next_file = file;
    }

    return next_file;
}

static gboolean
delete_file(S3Device *self,
            int file)
{
    gboolean result;
    GSList *keylist;
    guint64 file_bytes = 0;
    Device *pself = DEVICE(self);
    char *my_prefix = file_to_prefix(self, file); // prefix+"fXXXXXXXX-"
    guint16 next_ind = 0;
    S3_by_thread *s3t = NULL;

    if (file == -1) {
	my_prefix[strlen(self->prefix)+sizeof("f")-1] = '\0'; // leave off file#
    }

    // all keys for file# file
    result = s3_list_keys(get_temp_curl(self), self->bucket, NULL, my_prefix, NULL, 0,
			  &keylist, &file_bytes);
    g_free(my_prefix);
    if (!result) 
    {
	guint response_code;
	s3_error_code_t s3_error_code;
	CURLcode curl_code;

	s3_error(get_temp_curl(self), NULL, &response_code,
		 &s3_error_code, NULL, &curl_code, NULL);

        switch ( MIX_RESPCODE_AND_S3ERR(response_code,s3_error_code) )
        {
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchBucket):
               break;
            default:
                // device_set_error(pself,
                //     g_strdup_printf(_("While listing S3 keys: %s"), s3_strerror(get_temp_curl(self))),
                //     DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
                return FALSE;
	}

        return TRUE;
    }

    // open threads up to help delete
    s3_cancel_busy_threads(self,TRUE);
    
    s3_wait_threads_done(self); // all previous uploading threads need to finish

    { // lock context
	g_mutex_lock(self->thread_list_mutex);

        if (!self->delete_objlist) {
            self->delete_objlist = keylist;
        } else {
            self->delete_objlist = g_slist_concat(self->delete_objlist, keylist);
        }

        if (!self->delete_objlist)
            goto success_easy;

	// just get a thread to use its curl-context
	s3t = claim_free_thread(self, next_ind, self->nb_threads, TRUE); // wait for at least one
	if (!s3t)
	    goto failed;
	if (device_in_error(self))
	    goto failed;

	{ /// UNLOCK AND RELOCK
	    g_mutex_unlock(self->thread_list_mutex);
	    // single test of multi_delete
	    result = s3_thread_multi_delete(pself,s3t,&self->delete_objlist);
	    g_mutex_lock(self->thread_list_mutex);
	} /// UNLOCK AND RELOCK

	// ignore failure result .. but notice device failure
	if (device_in_error(self))
	    goto failed;

        if (!self->delete_objlist) {
	    reset_idle_thread(s3t, self->verbose); // clean up any earlier state
            goto success_easy;
	}

	// use the claimed thread as our first
	do
	{
	    s3t->done = FALSE;
	    g_thread_pool_push(self->thread_pool_delete, s3t, NULL); // a thread uses the curl context on the objects

	    next_ind = (s3t - self->s3t) + 1; // skip beyond any previous thread we started!
	    s3t = claim_free_thread(self, next_ind, self->nb_threads, FALSE); // *never* wait..
	    if (device_in_error(self))
		goto failed;
	} while(s3t);

success_easy:
	g_mutex_unlock(self->thread_list_mutex);
    } // lock context

    // NOTE: WASABI took 181 seconds to complete a response...
    // if (next_ind)
    // 	s3_wait_threads_done(self); // all the threads need to finish

    self->volume_bytes -= min(self->volume_bytes,file_bytes);

    return TRUE;

failed:
    if (s3t && !s3t->done) {
        s3t->done = TRUE;
        g_cond_broadcast(self->thread_list_cond); // notify new idle thread present..
    }
    // exit lock .. and perform wait-thread-delete
    g_mutex_unlock(self->thread_list_mutex);
    s3_cancel_busy_threads(self,TRUE);
    s3_wait_threads_done(self);
    return FALSE;
}

static gboolean
delete_all_files(S3Device *self)
{
    return delete_file(self, -1);
}

static s3_result_t 
s3_thread_multi_delete(Device *pself, S3_by_thread *s3t, GSList **d_objectsp)
{
    S3Handle *hndlN = s3t->s3;
    const S3Device *self = S3_DEVICE_CONST(pself);
    s3_result_t result = S3_RESULT_OK;
    GSList *mreq_objects = NULL; // 
    GSList *d_objects = g_atomic_pointer_get(d_objectsp);
    GSList *d_objects_prev = d_objects;
    gboolean xchg_ok;

    if (!self->use_s3_multi_delete)
	return S3_RESULT_NOTIMPL;

    g_mutex_lock(self->thread_list_mutex);

    g_debug("%s: using s3t#%ld ...", __FUNCTION__, s3t - self->s3t);
   
    // one iteration only...
    do
    {
	int  n;

        d_objects_prev = d_objects;

	for (n = 0 ; d_objects && n<1000 ; n++ ) 
	{
	    s3_object *object = d_objects->data;
	    d_objects = g_slist_remove(d_objects, object);
	    mreq_objects = g_slist_prepend(mreq_objects, object);
	}

	xchg_ok = g_atomic_pointer_compare_and_exchange(d_objectsp,
							d_objects_prev,
							d_objects);  // put new subset back for others
	g_assert(xchg_ok);

	{ /// UNLOCK AND RELOCK
	    g_mutex_unlock(self->thread_list_mutex);
	    g_debug("%s: Delete multiple keys n=%d",__FUNCTION__, n);
	    result = s3_multi_delete(hndlN, (const char *)self->bucket, mreq_objects);
	    g_mutex_lock(self->thread_list_mutex);
	} /// UNLOCK AND RELOCK

	if (result == S3_RESULT_NOTIMPL) {
	    g_debug("%s: Deleting multiple keys not implemented",__FUNCTION__);
	    break;
	}

	if (result != S3_RESULT_OK) {
	    g_debug("%s: Deleting multiple keys failed: %s", __FUNCTION__, s3_strerror(hndlN));
	    break;
	}

	slist_free_full(mreq_objects, free_s3_object); // delete all objects correctly
	mreq_objects = NULL;
    } while(FALSE); 
    // one iteration only...

    if (!mreq_objects) // task was completed?
	goto done;

    d_objects = g_atomic_pointer_get(d_objectsp);
    ((S3Device*)self)->use_s3_multi_delete = FALSE; // continue one by one

    // objects were left over? re-add all object-names
    d_objects_prev = d_objects;
    while (mreq_objects) {
	s3_object *object = mreq_objects->data;
	mreq_objects = g_slist_remove(mreq_objects, object);
	d_objects = g_slist_prepend(d_objects, object);
    }

    xchg_ok = g_atomic_pointer_compare_and_exchange(d_objectsp,
						    d_objects_prev,
						    d_objects);  // put new subset back for others
    g_assert(xchg_ok);

done:
    g_mutex_unlock(self->thread_list_mutex);
    return result;
}

static void
s3_thread_delete_session(
    gpointer thread_data,
    gpointer data)
{
    static int count = 0; // used to suppress block deletion messages
    S3_by_thread *s3t = (S3_by_thread *)thread_data;
    S3Handle *hndlN = s3t->s3;
    Device *pself = (Device *)data;
    const S3Device *self = S3_DEVICE_CONST(pself);
    s3_result_t result = S3_RESULT_OK;
    s3_object *object;
    GSList *d_objects = NULL; // multi-
    GSList **objlistp = &((S3Device*)self)->delete_objlist;

    {
        char *tname = g_strdup_printf("d#%lx",s3t - self->s3t);
        (void) pthread_setname_np(pthread_self(),tname);
        g_debug("[%p] delete worker thread ctxt#%ld",&s3t->curl_buffer,s3t - self->s3t);
        g_free(tname);
    }
 
    while (self->use_s3_multi_delete && *objlistp && result == S3_RESULT_OK ) {
    	result = s3_thread_multi_delete(pself,s3t,objlistp);
	// get another set to delete
    }

    g_mutex_lock(self->thread_list_mutex);

    // try a second time if not done...
    if (!*objlistp) 
	goto done;

    d_objects = g_atomic_pointer_get(objlistp);
    g_debug("%s: using s3t#%ld ...", __FUNCTION__, s3t - self->s3t);

    // if any were left... handle one-at-a-time
    while (d_objects) {
        object = d_objects->data;
	d_objects = g_slist_remove(d_objects, object);

	g_atomic_pointer_set(objlistp, d_objects); // place back for others to see

        { /// UNLOCK AND RELOCK
            g_mutex_unlock(self->thread_list_mutex);

            count = (count+1) % 1000;
            // if (!count) 
	    g_debug("%s: Deleting %s ...", __FUNCTION__, object->key);

            result = s3_delete(hndlN, (const char *)self->bucket,
                                        (const char *)object->key);
            g_mutex_lock(self->thread_list_mutex);
        } /// UNLOCK AND RELOCK

	d_objects = g_atomic_pointer_get(objlistp);

        if (result != S3_RESULT_OK) {
            s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
            s3t->errmsg = g_strdup_printf(_("While deleting key '%s': %s"),
                                      object->key, s3_strerror(hndlN));
        }
        free_s3_object(object);
    }

    g_atomic_pointer_set(objlistp, d_objects);

done:
    s3t->done = TRUE;
    g_cond_broadcast(self->thread_list_cond); // notify new idle thread present..
    g_mutex_unlock(self->thread_list_mutex);
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
    device_property_fill_and_register(&device_property_s3_session_token,
                                      G_TYPE_STRING, "s3_session_token",
       "Session token to authenticate with Amazon S3");
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
    device_property_fill_and_register(&device_property_project_name,
                                      G_TYPE_STRING, "project_name",
       "project_name to authenticate with");
    device_property_fill_and_register(&device_property_domain_name,
                                      G_TYPE_STRING, "domain_name",
       "domain_name to authenticate with");
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
    device_property_fill_and_register(&device_property_chunked,
                                      G_TYPE_BOOLEAN, "chunked",
       "Whether to use chunked transfer-encoding");
    device_property_fill_and_register(&device_property_s3_ssl,
                                      G_TYPE_BOOLEAN, "s3_ssl",
       "Whether to use SSL with Amazon S3");

    device_property_fill_and_register(&device_property_create_bucket,
                                      G_TYPE_BOOLEAN, "create_bucket",
       "Whether to create/delete bucket");
    device_property_fill_and_register(&device_property_read_from_glacier,
                                      G_TYPE_BOOLEAN, "read_from_glacier",
       "Whether to add code to read from glacier storage class");
    device_property_fill_and_register(&device_property_transition_to_glacier,
                                      G_TYPE_UINT64, "transition_to_glacier",
       "The number of days to wait before migrating to glacier after set to no-reuse");
    device_property_fill_and_register(&device_property_transition_to_class,
                                      G_TYPE_STRING, "transition_to_class",
       "The storage class the transition should happen either GLACIER or DEEP_ARCHIVE");
    device_property_fill_and_register(&device_property_s3_subdomain,
                                      G_TYPE_BOOLEAN, "s3_subdomain",
       "Whether to use subdomain");
    device_property_fill_and_register(&device_property_s3_multi_delete,
                                      G_TYPE_BOOLEAN, "s3_multi_delete",
       "Whether to use multi-delete");
    device_property_fill_and_register(&device_property_s3_reps,
                                      G_TYPE_STRING, "reps",
       "Number of replicas for data objects in CAStor");
    device_property_fill_and_register(&device_property_s3_reps_bucket,
                                      G_TYPE_STRING, "reps_bucket",
       "Number of replicas for automatically created buckets in CAStor");
    device_property_fill_and_register(&device_property_s3_multi_part_upload,
                                      G_TYPE_BOOLEAN, "s3_multi_part_upload",
       "If multi part upload must be used");

    device_property_fill_and_register(&device_property_timeout,
                                      G_TYPE_UINT64, "timeout",
       "The timeout for one tranfer");

    /* register the device itself */
    register_device(s3_device_factory, device_prefix_list);
}

GType
s3_device_get_type(void)
{
    static GType type = 0;

    if (G_UNLIKELY(type == 0)) {
        static const GTypeInfo info = {
            sizeof (S3DeviceClass),
            (GBaseInitFunc) s3_device_base_init,
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
s3_device_init(S3Device * self, gpointer data G_GNUC_UNUSED)
{
    Device * dself = DEVICE(self);
    GValue response;

    self->s3_api = S3_API_UNKNOWN;
    self->volume_bytes = 0;
    self->volume_limit = 0;
    self->leom = TRUE;
    self->enforce_volume_limit = FALSE;
    self->use_subdomain = FALSE;
    self->nb_threads = 1;
    self->nb_threads_backup = 1;
    self->nb_threads_recovery = 1;
    self->use_s3_multi_part_upload = FALSE;// must know in advance if intended
    self->use_padded_stream_uploads = FALSE;	   // can test and failover
    self->use_s3_multi_delete = TRUE;	   // can test and failover
    self->allow_s3_multi_part_upload = TRUE;
    self->allow_s3_multi_part_copy = TRUE;
    self->allow_padded_stream_uploads = TRUE;

    self->thread_pool_delete = NULL;
    self->thread_pool_write = NULL;
    self->thread_pool_read = NULL;
    self->thread_list_cond = NULL;
    self->thread_list_mutex = NULL;
    self->reps = NULL;
    self->reps_bucket = NULL;
    self->transition_to_glacier = -1;
    self->transition_to_class = NULL;
    
    self->s3t = NULL;
    self->s3t_temp_free = NULL;
    self->xfer_s3t = NULL;

    self->fxn_create_curr_objkey    = NULL; // require API start
    self->fxn_thread_write_factory  = NULL; // require API start
    self->fxn_thread_write_complete = NULL; // do nothing...
    self->fxn_thread_write_finish = NULL; // do nothing...

    // set these up only once...
    self->thread_list_cond = &self->_thread_list_cond_store;
    self->thread_list_mutex = &self->_thread_list_mutex_store;

    g_cond_init(self->thread_list_cond);
    g_mutex_init(self->thread_list_mutex);

    // cannot use pself/dself yet

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
s3_device_class_init(S3DeviceClass * c G_GNUC_UNUSED, gpointer data G_GNUC_UNUSED)
{
    GObjectClass *g_object_class = (GObjectClass*) c;
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->open_device = s3_device_open_device;
    device_class->create = s3_device_create;
    device_class->read_label = s3_device_read_label;
    device_class->start = s3_device_start;
    device_class->finish = s3_device_finish;
    device_class->get_bytes_read = s3_device_get_bytes_read;
    device_class->get_bytes_written = s3_device_get_bytes_written;

    device_class->start_file = s3_device_start_file;
    device_class->write_block = s3_device_write_block;
    device_class->finish_file = s3_device_finish_file;

    device_class->init_seek_file = s3_device_get_keys_restored;
    device_class->seek_file = s3_device_seek_file;
    device_class->seek_block = s3_device_seek_block;
    device_class->read_block = s3_device_read_block;
    device_class->recycle_file = s3_device_recycle_file;

    device_class->erase = s3_device_erase;
    device_class->set_reuse = s3_device_set_reuse;
    device_class->set_no_reuse = s3_device_set_no_reuse;

    g_object_class->finalize = s3_device_at_finalize;
}

static void
s3_device_base_init(
    S3DeviceClass *c)
{
    DeviceClass *device_class = (DeviceClass *)c;

    device_class_register_property(device_class, PROPERTY_S3_ACCESS_KEY,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_access_key_fn);

    device_class_register_property(device_class, PROPERTY_S3_SECRET_KEY,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_secret_key_fn);

    device_class_register_property(device_class, PROPERTY_S3_SESSION_TOKEN,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_session_token_fn);

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

    device_class_register_property(device_class, PROPERTY_PROJECT_NAME,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_project_name);

    device_class_register_property(device_class, PROPERTY_DOMAIN_NAME,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_domain_name);

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

    device_class_register_property(device_class, PROPERTY_READ_FROM_GLACIER,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_read_from_glacier_fn);

    device_class_register_property(device_class, PROPERTY_TRANSITION_TO_GLACIER,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_transition_to_glacier_fn);

    device_class_register_property(device_class, PROPERTY_TRANSITION_TO_CLASS ,
            PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
            device_simple_property_get_fn,
            s3_device_set_transtion_to_class_fn);

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

    device_class_register_property(device_class, PROPERTY_CHUNKED,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_chunked_fn);

    device_class_register_property(device_class, PROPERTY_S3_SSL,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_ssl_fn);

    device_class_register_property(device_class, PROPERTY_REUSE_CONNECTION,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_reuse_connection_fn);

    device_class_register_property(device_class, PROPERTY_TIMEOUT,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_timeout_fn);

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

    device_class_register_property(device_class, PROPERTY_S3_MULTI_PART_UPLOAD,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_s3_multi_part_upload);

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

    device_class_register_property(device_class, PROPERTY_S3_REPS,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_reps_fn);

    device_class_register_property(device_class, PROPERTY_S3_REPS_BUCKET,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    s3_device_set_reps_bucket_fn);
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
s3_device_set_session_token_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->session_token);
    self->session_token = g_value_dup_string(val);
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
s3_device_set_project_name(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->project_name);
    self->project_name = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_domain_name(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->domain_name);
    self->domain_name = g_value_dup_string(val);
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
	device_set_error(p_self, g_strdup(_(
		"Location constraint given for Amazon S3 bucket, "
		"but libcurl is too old support wildcard certificates.")),
	    DEVICE_STATUS_DEVICE_ERROR);
        goto fail;
    }

    if (str_val[0] && !s3_bucket_name_compat(self->bucket)) {
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

    self->verbose = g_value_get_boolean(val);
    /* Our S3 handle may not yet have been instantiated; if so, it will
     * get the proper verbose setting when it is created */
    if (self->s3t)
    {
        S3_by_thread *s3t = NULL;
        S3_by_thread *s3t_begin = self->s3t;
        S3_by_thread *s3t_end = s3t_begin + self->nb_threads;

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t) {
	    if (s3t->s3) s3_verbose(s3t->s3, self->verbose);
	}
    }

    __API_FXN_CALLED__(self);
    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_create_bucket_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->create_bucket = g_value_get_boolean(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_read_from_glacier_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->read_from_glacier = g_value_get_boolean(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_transition_to_glacier_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->transition_to_glacier = g_value_get_uint64(val);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_transtion_to_class_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    amfree(self->transition_to_class);
    self->transition_to_class = g_value_dup_string(val);
    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_storage_api(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    static const char *apis[] = { 
    	"__UNKNOWN_API__", // [S3_API_UNKNOWN
    	"S3", 		   // [S3_API_S3
    	"SWIFT-1.0", 	   // S3_API_SWIFT_1
    	"SWIFT-2.0", 	   // S3_API_SWIFT_2
    	"SWIFT-3", 	   // S3_API_SWIFT_3
    	"OAUTH2", 	   // S3_API_OAUTH2
    	"CASTOR", 	   // S3_API_CASTOR
    	"AWS4",   	   // S3_API_AWS4
    	NULL,
    };
    S3Device *self = S3_DEVICE(p_self);
    const char *storage_api = g_value_get_string(val);

    for(self->s3_api = S3_API_S3 
    	    ; apis[self->s3_api] && !g_str_equal(storage_api, apis[self->s3_api]) 
                ; ++self->s3_api)
	{ }

    if (self->s3_api > S3_API_AWS4)
	self->s3_api = S3_API_UNKNOWN;
    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_openstack_swift_api_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{

    const gboolean openstack_swift_api = g_value_get_boolean(val);
    if (openstack_swift_api) {
	GValue storage_api_val;
	bzero(&storage_api_val, sizeof(GValue));
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

    new_val = g_value_get_boolean(val);
    /* Our S3 handle may not yet have been instantiated; if so, it will
     * get the proper use_ssl setting when it is created */
    if (self->s3t) {
        S3_by_thread *s3t = NULL;
        S3_by_thread *s3t_begin = self->s3t;
        S3_by_thread *s3t_end = s3t_begin + self->nb_threads;

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t) {
            if (!s3t->s3)
                continue;
	    if (!s3_use_ssl(s3t->s3, new_val))
                break;
	}

        if ( s3t != s3t_end ) {
            device_set_error(p_self, g_strdup_printf(_(
                    "Error setting S3 SSL/TLS use "
                    "(tried to enable SSL/TLS for S3, but curl doesn't support it?)")),
                DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
        }
    }
    self->use_ssl = new_val;

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_chunked_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    gboolean new_val;

    new_val = g_value_get_boolean(val);

    self->use_chunked = new_val;

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
s3_device_set_timeout_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->timeout = g_value_get_uint64(val);
    if (self->timeout <= 0 || self->timeout > 300)
	self->timeout = 300;

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_max_send_speed_fn(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);
    guint64 new_val;

    new_val = g_value_get_uint64(val);
    if (new_val && new_val < 5120) {
        device_set_error(p_self,
            g_strdup("MAX-SEND-SPEED property is too low (minimum value is 5120)"),
            DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (self->s3t)
    {
        S3_by_thread *s3t = NULL;
        S3_by_thread *s3t_begin = self->s3t;
        S3_by_thread *s3t_end = s3t_begin + self->nb_threads;

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t) {
            if (!s3t->s3)
                continue;
	    if (!s3_set_max_send_speed(s3t->s3, new_val))
                break;
	}

        if ( s3t != s3t_end ) {
            device_set_error(p_self,
                    g_strdup("Could not set S3 maximum send speed"),
                    DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
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

    new_val = g_value_get_uint64(val);
    if (new_val && new_val < 5120) {
        device_set_error(p_self,
                    g_strdup("MAX-RECV-SPEED property is too low (minimum value is 5120)"),
                    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (self->s3t)
    {
        S3_by_thread *s3t = NULL;
        S3_by_thread *s3t_begin = self->s3t;
        S3_by_thread *s3t_end = s3t_begin + self->nb_threads;

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t) {
            if (!s3t->s3)
                continue;
	    if (!s3_set_max_recv_speed(s3t->s3, new_val))
                break;
	}

        if (s3t != s3t_end) {
            device_set_error(p_self,
                    g_strdup("Could not set S3 maximum recv speed"),
                    DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
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
    self->nb_threads = max(self->nb_threads, self->nb_threads_backup);

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
    self->nb_threads = max(self->nb_threads, self->nb_threads_recovery);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_s3_multi_part_upload(Device *p_self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    self->use_s3_multi_part_upload = g_value_get_boolean(val);
    self->use_s3_multi_part_upload = self->use_s3_multi_part_upload 
                                        && self->allow_s3_multi_part_upload;

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

    if (self->use_subdomain && !s3_bucket_name_compat(self->bucket)) {
	device_set_error(p_self, g_strdup_printf(_(
		"S3-SUBDOMAIN is set, "
		"but the bucket name (%s) is not usable as a subdomain."),
		self->bucket),
	    DEVICE_STATUS_DEVICE_ERROR);
	self->use_subdomain = FALSE;
	return FALSE;
    }

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

static gboolean
s3_device_set_reps_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->reps);
    self->reps = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
s3_device_set_reps_bucket_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    S3Device *self = S3_DEVICE(p_self);

    amfree(self->reps_bucket);
    self->reps_bucket = g_value_dup_string(val);
    device_clear_volume_details(p_self);

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static Device*
s3_device_factory(char * device_name, char * device_type, char * device_node)
{
    Device *rval;
    g_assert(g_str_equal(device_type, S3_DEVICE_NAME));
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

    pself->min_block_size = S3_DEVICE_MIN_BLOCK_SIZE; // must be 64k
    pself->max_block_size = S3_DEVICE_MAX_BLOCK_SIZE;
    pself->block_size = self->buff_blksize; // assign most ideal at first

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
	    g_strdup_printf(_("Empty bucket name in device %s"), device_name),
	    DEVICE_STATUS_DEVICE_ERROR);
        amfree(self->bucket);
        amfree(self->prefix);
        return;
    }

    if (self->reps == NULL) {
        self->reps = g_strdup(S3_DEVICE_REPS_DEFAULT);
    }

    if (self->reps_bucket == NULL) {
        self->reps_bucket = g_strdup(S3_DEVICE_REPS_BUCKET_DEFAULT);
    }

    g_debug(_("S3 driver using bucket '%s', prefix '%s'"), self->bucket, self->prefix);
    g_debug("curl version: %s", curl_version());
    #ifdef LIBCURL_USE_OPENSSL
	g_debug("curl compiled for OPENSSL");
    #else
    #if defined LIBCURL_USE_GNUTLS
	g_debug("curl compiled for GNUTLS");
    #else
	g_debug("curl compiled for NSS");
    #endif
    #endif

    /* default values */
    self->verbose = FALSE;
    self->s3_api = S3_API_UNKNOWN;

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

    /* timeout */
    self->timeout = 0;
    bzero(&tmp_value, sizeof(GValue));
    g_value_init(&tmp_value, G_TYPE_UINT64);
    g_value_set_uint64(&tmp_value, self->timeout);
    device_set_simple_property(pself, device_property_timeout.ID,
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

    __API_FXN_CALLED__(self);  // [never verbose?] 
}

static gboolean
s3_device_create(Device *pself)
{
    S3Device *self = S3_DEVICE(pself);

    // clear from start
    device_set_error(pself, NULL, DEVICE_STATUS_SUCCESS);

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (!setup_handle(self)) {
        /* setup_handle already set our error message */
	return FALSE;
    }

    if (!make_bucket(pself)) {
	return FALSE;
    }

    if (parent_class->create) {
        return parent_class->create(pself);
    }
    return TRUE;
}

static void 
s3_device_at_finalize(GObject * obj_self) {
    S3Device *self = S3_DEVICE (obj_self);
    S3Device self_copy;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    s3_cancel_busy_threads(self,TRUE);
    s3_wait_threads_done(self);

    __API_LONG_FXN_CHECK__(self,"[idled-before-clear]");

    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    // attempt to quickly remove original from shared use
    if (self->thread_pool_delete)
	g_thread_pool_free(self->thread_pool_delete, 1, 1);
    if (self->thread_pool_write)
	g_thread_pool_free(self->thread_pool_write, 1, 1);
    if (self->thread_pool_read)
	g_thread_pool_free(self->thread_pool_read, 1, 1);
    if (self->thread_list_mutex)
	g_mutex_clear(self->thread_list_mutex);
    if (self->thread_list_cond)
	g_cond_clear(self->thread_list_cond);

    // MUST perform full clear *after* mutex/cond pointers used above 

    self_copy = *self; // copy all values and clear shared object first
    memset((char*)self + sizeof(self->__parent__),'\0',sizeof(*self) - sizeof(self->__parent__));

    if (self_copy.s3t)
    {
        S3_by_thread *s3t = NULL;
        S3_by_thread *s3t_begin = self_copy.s3t;
        S3_by_thread *s3t_end = s3t_begin + self_copy.nb_threads;

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t)
        {
	    reset_idle_thread(s3t,self->verbose);
            s3_free(s3t->s3);
	    s3_buffer_destroy(&s3t->curl_buffer);
	}
	g_free(self_copy.s3t);
    }
    if (self_copy.catalog_filename) {
	catalog_close(self);
    }
    g_free(self_copy.bucket);
    g_free(self_copy.prefix);
    g_free(self_copy.access_key);
    g_free(self_copy.secret_key);
    g_free(self_copy.session_token);
    g_free(self_copy.swift_account_id);
    g_free(self_copy.swift_access_key);
    g_free(self_copy.username);
    g_free(self_copy.password);
    g_free(self_copy.tenant_id);
    g_free(self_copy.tenant_name);
    g_free(self_copy.project_name);
    g_free(self_copy.domain_name);
    g_free(self_copy.host);
    g_free(self_copy.service_path);
    g_free(self_copy.user_token);
    g_free(self_copy.bucket_location);
    g_free(self_copy.storage_class);
    g_free(self_copy.server_side_encryption);
    g_free(self_copy.proxy);
    g_free(self_copy.ca_info);
    g_free(self_copy.reps);
    g_free(self_copy.reps_bucket);

}

static gboolean
catalog_open(
    S3Device *self)
{
    char   *filename;
    char   *dirname;
    FILE   *file;
    char    line[1025];

    /* create the directory */
    filename = g_strdup_printf("bucket-%s", self->bucket);
    dirname  = config_dir_relative(filename);
    if (mkdir(dirname, 0700) == -1 && errno != EEXIST) {
	g_debug(_("Can't create catalog directory '%s': %s"), dirname, strerror(errno));
	return FALSE;
    }
    amfree(filename);
    amfree(dirname);

    filename = g_strdup_printf("bucket-%s/%s", self->bucket, self->prefix);
    g_free(self->catalog_filename);
    self->catalog_filename = config_dir_relative(filename);
    g_free(filename);
    file = fopen(self->catalog_filename, "r");
    if (!file) {
	g_free(self->catalog_label);
	g_free(self->catalog_header);
	self->catalog_label = NULL;
	self->catalog_header = NULL;
	return TRUE;
    }
    if (!fgets(line, 1024, file)) {
	fclose(file);
	return FALSE;
    }
    if (line[strlen(line)-1] == '\n')
	line[strlen(line)-1] = '\0';
    g_free(self->catalog_label);
    self->catalog_label = g_strdup(line+7);
    if (!fgets(line, 1024, file)) {
	fclose(file);
	return FALSE;
    }
    if (line[strlen(line)-1] == '\n')
	line[strlen(line)-1] = '\0';
    g_free(self->catalog_header);
    self->catalog_header = g_strdup(line+8);
    fclose(file);
    return TRUE;
}

static gboolean
write_catalog(
    const S3Device *self)
{
    FILE *file;

    if (!self->catalog_label || !self->catalog_header)
	return TRUE;

    file = fopen(self->catalog_filename, "w");
    if (!file) {
        return FALSE;
    }
    g_fprintf(file,"LABEL: %s\n", self->catalog_label);
    g_fprintf(file,"HEADER: %s\n", self->catalog_header);
    fclose(file);
    return TRUE;
}

static gboolean
catalog_reset(
    S3Device *self,
    char     *header,
    char     *label)
{
    g_free(self->catalog_header);
    self->catalog_header = quote_string(header);
    g_free(self->catalog_label);
    self->catalog_label = g_strdup(label);

    return write_catalog(self);
}

static gboolean
catalog_remove(
    S3Device *self)
{
    unlink(self->catalog_filename);
    amfree(self->catalog_filename); // sets NULL
    amfree(self->catalog_label); // sets NULL
    amfree(self->catalog_header); // sets NULL
    return TRUE;
}

static gboolean
catalog_close(
    S3Device *self)
{
    gboolean result;

    result = write_catalog(self);
    amfree(self->catalog_filename); // sets NULL
    amfree(self->catalog_label); // sets NULL
    amfree(self->catalog_header); // sets NULL
    return result;
}

static gboolean
setup_handle(S3Device * self) {
    Device *d_self = DEVICE(self);
    guint response_code;
    s3_error_code_t s3_error_code;
    CURLcode curl_code;
    DeviceStatusFlags errflag = DEVICE_STATUS_DEVICE_ERROR;
    char *errmsg;

    switch (self->s3_api) {
    	case S3_API_SWIFT_1:
    	case S3_API_SWIFT_2:
	    self->allow_s3_multi_part_upload = FALSE; // not implemented in older cases
	    self->allow_s3_multi_part_copy = FALSE; // not implemented in older cases
	    self->use_s3_multi_delete = FALSE;      // not implemented
	    self->use_s3_multi_part_upload = FALSE; // not implemented in older cases
	    break;

    	case S3_API_SWIFT_3:
	    self->allow_s3_multi_part_upload = FALSE; // not implemented in older cases
	    self->allow_s3_multi_part_copy = FALSE; // not implemented in older cases
	    self->use_s3_multi_part_upload = FALSE; // not implemented in older cases
	    break;

        // GCP...
    	case S3_API_OAUTH2:
	    self->use_s3_multi_delete = FALSE;    // not implemented
	    self->allow_s3_multi_part_copy = FALSE; // mp-copy not implemented
	    break;

    	case S3_API_AWS4:
	    self->allow_padded_stream_uploads = FALSE; // must supply leading checksums 
	    if (self->host && g_str_has_suffix(self->host, GCP_TOP_DOMAIN) ) {
		self->use_s3_multi_delete = FALSE;    // not implemented
		self->allow_s3_multi_part_copy = FALSE; // mp-copy not implemented
	    }
            break;

    	case S3_API_CASTOR:
#if LIBCURL_VERSION_NUM >= 0x071301
        {
            curl_version_info_data *info;
            /* check the runtime version too */
            info = curl_version_info(CURLVERSION_NOW);
            if (info->version_num < 0x071301) {
#endif
                device_set_error(d_self, g_strdup_printf(_(
                            "Error setting STORAGE-API to castor "
                            "(You must install libcurl 7.19.1 or newer)")),
                        DEVICE_STATUS_DEVICE_ERROR);
                return FALSE;
#if LIBCURL_VERSION_NUM >= 0x071301
            }
            break;
        }
#endif
        
    	case S3_API_S3:
	case S3_API_UNKNOWN:
	    self->s3_api = S3_API_S3;

            if (self->host && g_str_has_suffix(self->host, AMZ_TOP_DOMAIN) ) {
	    	// don't use AWS4 by default 
	    }
	    if (self->host && g_str_has_suffix(self->host, GCP_TOP_DOMAIN) ) {
		self->use_s3_multi_delete = FALSE;    // not implemented
		self->allow_s3_multi_part_copy = FALSE; // mp-copy not implemented
		self->allow_gcp_compose = TRUE;
	    }
	    break;

        default:
            g_error(_("%s: Invalid STORAGE_API, using \"S3\"."),__FUNCTION__);
            break;
    }

    catalog_open(self);

    // already allocated
    if (self->s3t)
           return TRUE;

    // for all the below going wrong... DEVICE_ERROR is default

    switch ( self->s3_api ) {
      case S3_API_S3:
        if (self->access_key == NULL || self->access_key[0] == '\0')
        {
            errmsg = g_strdup(_("No Amazon access key specified"));
            goto fail;
        }

        if (self->secret_key == NULL || self->secret_key[0] == '\0')
        {
            errmsg = g_strdup(_("No Amazon secret key specified"));
            goto fail;
        }
        break;

      case S3_API_SWIFT_1:
        if (self->swift_account_id == NULL ||
            self->swift_account_id[0] == '\0')
        {
            errmsg = g_strdup(_("No Swift account id specified"));
            goto fail;
        }
        if (self->swift_access_key == NULL ||
            self->swift_access_key[0] == '\0')
        {
            errmsg = g_strdup(_("No Swift access key specified"));
            goto fail;
        }
        break;

      case S3_API_SWIFT_2:
        if (!((self->username && self->password && self->tenant_id) ||
              (self->username && self->password && self->tenant_name) ||
              (self->access_key && self->secret_key && self->tenant_id) ||
              (self->access_key && self->secret_key && self->tenant_name)))
        {
            errmsg = g_strdup(_("Missing authorization properties"));
            goto fail;
        }
        break;

      // self->project_name & self->domain_name have default value
      case S3_API_SWIFT_3:
        if (!(self->username && self->password))
        {
            errmsg = g_strdup(_("Missing authorization properties"));
            goto fail;
        }
        break;

      case S3_API_OAUTH2:
        if (self->client_id == NULL || self->client_id[0] == '\0')
        {
            errmsg = g_strdup(_("Missing client_id properties"));
            goto fail;
        }
        if (self->client_secret == NULL || self->client_secret[0] == '\0')
        {
            errmsg = g_strdup(_("Missing client_secret properties"));
            goto fail;
        }
        if (self->refresh_token == NULL || self->refresh_token[0] == '\0')
        {
            errmsg = g_strdup(_("Missing refresh_token properties"));
            goto fail;
        }
        if (self->project_id == NULL || self->project_id[0] == '\0')
        {
            errmsg = g_strdup(_("Missing project_id properties"));
            goto fail;
        }
        break;

      case S3_API_CASTOR:
        self->use_s3_multi_delete = 0;
        self->use_subdomain = FALSE;
        if(self->service_path) {
            g_free(self->service_path);
            self->service_path = NULL;
        }
      default:
           break; // FIXME: no error currently
    }

    self->s3t = g_new0(S3_by_thread, self->nb_threads);
    self->s3t_temp_free = NULL; // this one is ready..
    self->xfer_s3t = NULL;

    if (self->s3t == NULL)
    {
        errmsg = g_strdup(_("Can't allocate S3Handle array"));
        goto fail;
    }

    {
        S3_by_thread *s3t = NULL;
        S3_by_thread *s3t_begin = self->s3t;
        S3_by_thread *s3t_end = s3t_begin + self->nb_threads;

        g_mutex_lock(self->thread_list_mutex);

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t) {
            // zero memory and init the mutex and cond
            s3_buffer_ctor(&s3t->curl_buffer,0, self->verbose);
            reset_idle_thread(s3t, self->verbose); // ignore any buffer
            // s3t->done = TRUE;  // in the above
            // s3t->ahead = TRUE;  // can be cancelled

            s3t->s3 = s3_open(self->access_key, self->secret_key,
                                           self->session_token,
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
                                           self->project_name,
                                           self->domain_name,
                                           self->client_id,
                                           self->client_secret,
                                           self->refresh_token,
                                           self->reuse_connection,
                                           self->read_from_glacier,
                                           self->timeout,
                                           self->reps, self->reps_bucket);
            if (s3t->s3 == NULL) {
                self->nb_threads = s3t - s3t_begin; // halt before current thread
                errmsg = g_strdup(_("Internal error creating S3 handle"));
                goto fail_unlock;
            }
        }
       
        g_debug("Create %d threads", self->nb_threads);
        self->thread_pool_delete = g_thread_pool_new(s3_thread_delete_session,
                                                     self, self->nb_threads, 0,
                                                     NULL);
        self->thread_pool_write = g_thread_pool_new(s3_thread_write_session,
                                                     self, self->nb_threads, 0,
                                                     NULL);
        self->thread_pool_read = g_thread_pool_new(s3_thread_read_session,
                                                     self, self->nb_threads, 0,
                                                     NULL);

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t) {
            S3Handle *hndlN = s3t->s3;
            s3_verbose(hndlN, self->verbose);

            if (!s3_use_ssl(hndlN, self->use_ssl)) {
                errmsg = g_strdup_printf(_( "Error setting S3 SSL/TLS use "
                        "(tried to enable SSL/TLS for S3, but curl doesn't support it?)"));
                goto fail_unlock;
            }

            if (self->max_send_speed && self->max_send_speed < 5120) {
                errmsg = g_strdup("MAX-SEND-SPEED property is too low (minimum value is 5120)");
                goto fail_unlock;
            }
            if (self->max_send_speed && !s3_set_max_send_speed(hndlN, self->max_send_speed)) {
                errmsg = g_strdup("Could not set S3 maximum send speed");
                goto fail_unlock;
            }

            if (self->max_recv_speed && self->max_recv_speed < 5120) {
                errmsg = g_strdup("MAX-RECV-SPEED property is too low (minimum value is 5120)");
                goto fail_unlock;
            }
            if (self->max_recv_speed && !s3_set_max_recv_speed(hndlN, self->max_recv_speed)) {
                errmsg = g_strdup("Could not set S3 maximum recv speed");
                goto fail_unlock;
            }
        }

        for(s3t = s3t_begin; s3t != s3t_end ; ++s3t) {
            S3Handle *hndlN = s3t->s3;
            if ( !s3_open2(hndlN) ) break;
        }

        if ( s3t != s3t_end ) {
            S3Handle *hndlN = s3t->s3;
            if (self->s3_api == S3_API_SWIFT_1 ||
                self->s3_api == S3_API_SWIFT_2 ||
                self->s3_api == S3_API_SWIFT_3) {
                self->nb_threads = s3t - s3t_begin; // disable current thread and later
                s3_error(hndlN, NULL, &response_code,
                         &s3_error_code, NULL, &curl_code, NULL);
                errmsg = g_strdup_printf(_("s3_open2 failed: %s"),
                                        s3_strerror(hndlN));
                goto fail_unlock;
            }

            errmsg = g_strdup("s3_open2 failed");
            goto fail_unlock;
        }

        self->s3t_temp_free = &self->s3t[0]; // this one is a useful start

        g_mutex_unlock(self->thread_list_mutex);
        goto done;

    fail_unlock:
        g_mutex_unlock(self->thread_list_mutex);
        goto fail;
    }

done:
    return TRUE;

fail:
    device_set_error(d_self, errmsg, errflag);
    return FALSE;
}

static gboolean
make_bucket(
    Device * pself)
{
    S3Device *self = S3_DEVICE(pself);
    guint response_code;
    s3_error_code_t s3_error_code;
    CURLcode curl_code;

    if (self->bucket_made) {
	return TRUE;
    }

    if (s3_is_bucket_exists(get_temp_curl(self), self->bucket, self->prefix, self->project_id)) {
	self->bucket_made = TRUE;
	abort_partial_upload(self);
	return TRUE;
    }

    s3_error(get_temp_curl(self), NULL, &response_code, &s3_error_code, NULL, &curl_code, NULL);

    if (response_code == 0 && s3_error_code == 0 &&
	(curl_code == CURLE_COULDNT_CONNECT ||
	 curl_code == CURLE_COULDNT_RESOLVE_HOST)) {
	device_set_error(pself,
	    g_strdup_printf(_("While connecting to %s bucket: %s"),
			    S3_name[self->s3_api], s3_strerror(get_temp_curl(self))),
		DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (!self->create_bucket) {
	// device_set_error(pself,
	//     g_strdup_printf(_("Can't list bucket: %s"),
	// 		    s3_strerror(get_temp_curl(self))),
	// 	DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (!s3_make_bucket(get_temp_curl(self), self->bucket, self->project_id)) {
        s3_error(get_temp_curl(self), NULL, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it isn't an expected error (bucket already exists),
         * return FALSE */
        if (response_code != 409 ||
            (s3_error_code != S3_ERROR_BucketAlreadyExists &&
	     s3_error_code != S3_ERROR_BucketAlreadyOwnedByYou)) {
	    // device_set_error(pself,
	    // 	g_strdup_printf(_("While creating new S3 bucket: %s"), s3_strerror(get_temp_curl(self))),
	    // 	DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
        }
    }

    self->bucket_made = TRUE;
    abort_partial_upload(self);
    return TRUE;
}

static int 
s3_thread_xferinfo_func(
    void *thread_data,
    curl_off_t dltotal, curl_off_t dlnow,
    curl_off_t ultotal, curl_off_t ulnow)
{
    S3_by_thread *s3t = (S3_by_thread *)thread_data;
    CurlBuffer *s3buf = &s3t->curl_buffer;
    gint64 now = g_get_monotonic_time ();
    curl_off_t eodleft;
    time_t last_now = s3t->time_expired - 300*G_TIME_SPAN_SECOND;
    // curl_off_t speed, speed;

    // ignore if nothing interesting happening yet...
    if (!dlnow && !ulnow) {
	s3t->dlnow_last = 0;
	s3t->ulnow_last = 0;
	s3t->time_expired = now + 300*G_TIME_SPAN_SECOND;
       return 0; // no-abort
    }

    g_mutex_lock(s3t->now_mutex);

    do {
        // update the timer?
        if (dlnow && s3t->dlnow_last != (objbytes_t) dlnow) break;
        if (ulnow && s3t->ulnow_last != (objbytes_t) ulnow) break;

	if ( s3buf->opt_verbose && dltotal && now - last_now > 2*G_TIME_SPAN_SECOND ) {
            g_debug("[%p] %s:%d: halted download progress pos/end=%#lx+%#lx [%lds]",
                       s3buf, __FUNCTION__, __LINE__, 
                       s3t->xfer_begin + dlnow,
                       s3t->xfer_begin + dltotal,
                       (now - last_now)/G_TIME_SPAN_SECOND);
        }
	if ( s3buf->opt_verbose && ultotal && now - last_now > 2*G_TIME_SPAN_SECOND ) {
            g_debug("[%p] %s:%d: halted upload progress pos/end=%#lx+%#lx [%lds]%s",
                       s3buf, __FUNCTION__, __LINE__, 
                       s3t->xfer_begin + ulnow,
                       s3t->xfer_begin + ultotal,
                       (now - last_now)/G_TIME_SPAN_SECOND,
                       ( s3t->at_final ? " (part-final)" : ""));
        }

        // no timeout for now?
        if (!s3t->time_expired) goto cleanup;
        if (now <= s3t->time_expired) goto cleanup;

        // no progress for too long
        goto abort_download;
    } while(FALSE);

    (void) dltotal;
    (void) ultotal;

    //if (ultotal)
    //	res = curl_easy_getinfo(curl, CURLINFO_SPEED_UPLOAD_T, &speed);
    //if (dltotal)
    //	res = curl_easy_getinfo(curl, CURLINFO_SPEED_DOWNLOAD_T, &speed);

    eodleft = (curl_off_t)s3_buffer_read_size_func(s3buf);

    // change in the number of 8-meg xfers has occurred... then print
    if ( (s3t->dlnow_last >> 23) != ((objbytes_t)dlnow >>23) || 
        (s3t->ulnow_last >> 23) != ((objbytes_t)ulnow >>23))
    {
	gint s3t_filled = (xferbytes_t) g_atomic_int_get(&s3buf->bytes_filled);
	objbytes_t s3t_dlnow = (objbytes_t) g_atomic_pointer_get(&s3t->dlnow);
	objbytes_t s3t_ulnow = (objbytes_t) g_atomic_pointer_get(&s3t->ulnow);
        // s3t increments when each read is done
	curl_off_t ddlnow = dlnow - (s3t_dlnow + s3t_filled); // direct xfer performed
        // s3t increments when each write is done
	curl_off_t dulnow = (ulnow + s3t_filled) - s3t_ulnow; // direct xfer performed
        // measures reads in only
	curl_off_t ddltotal = eodleft - (dltotal - dlnow) - s3t_filled; // amt remaining to dload
	curl_off_t dultotal = eodleft - (ultotal - ulnow) + s3t_filled; // amt remaining to uload

        if (s3t_dlnow == 1) ++ddlnow;
        if (s3t_ulnow == 1) ++dulnow;

        // download not full
	if ( s3buf->opt_verbose && dltotal && ddltotal )
	{
	    g_debug("[%p] %s:%d: new download progress pos/end=%#lx+%#lx ~~ diffs %c%lx/%c%lx",
			   s3buf, __FUNCTION__, __LINE__,
			   s3t->xfer_begin + dlnow, 
                           s3t->xfer_begin + dltotal,
			   (ddlnow >= 0 ? '+' : '-' ), labs(ddlnow),
			   (ddltotal >= 0 ? '+' : '-' ), labs(ddltotal));
	}
        // upload not full
	if ( s3buf->opt_verbose && ultotal && dultotal && (curl_off_t) s3t_ulnow != ultotal ) {
		g_debug("[%p] %s:%d: new upload progress pos/end=%#lx/%#lx ~~ diffs %c%lx/%c%lx %s",
			   s3buf, __FUNCTION__, __LINE__,
			   s3t->xfer_begin + ulnow, 
			   s3t->xfer_begin + ultotal,
			   (dulnow >= 0 ? '+' : '-' ), labs(dulnow),
			   (dultotal >= 0 ? '+' : '-' ), labs(dultotal),
                           ( s3t->at_final ? " (mp-final)" : ""));
	}
        // upload but full
	if ( s3buf->opt_verbose && ultotal && dultotal && (curl_off_t) s3t_ulnow == ultotal ) {
		g_debug("[%p] %s:%d: new upload progress pos/end=%#lx/%#lx ~~ buffer-flush %s",
			   s3buf, __FUNCTION__, __LINE__,
			   s3t->xfer_begin + ulnow,
			   s3t->xfer_begin + ultotal,
                           ( s3t->at_final ? " (mp-final)" : ""));
	}
    }

    s3t->dlnow_last = dlnow;
    s3t->ulnow_last = ulnow;
    s3t->time_expired = now + 300*G_TIME_SPAN_SECOND;

 cleanup:
    g_mutex_unlock(s3t->now_mutex);
    return 0;

 abort_download:
    g_debug("%s: Abort due to timeout", __FUNCTION__);
    g_mutex_unlock(s3t->now_mutex);
    return -1;
}

static int 
s3_thread_progress_func(
    void *thread_data,
    double dltotal, double dlnow,
    double ultotal, double ulnow)
{
    return s3_thread_xferinfo_func(thread_data, (curl_off_t) dltotal, (curl_off_t) dlnow,
                                      (curl_off_t) ultotal, (curl_off_t) ulnow);
}

static DeviceStatusFlags
s3_device_read_label(Device *pself) {
    S3Device *self = S3_DEVICE(pself);
    char *key;
    CurlBuffer label_buffer_tmp = { 0 }; // to read in w/max of S3_DEVICE_DEFAULT_BLOCK_SIZE,
    CurlBuffer *s3buf_label_tmp = &label_buffer_tmp;
    dumpfile_t *amanda_header;
    gboolean result;

    /* note that this may be called from s3_device_start, when
     * self->access_mode is not ACCESS_NULL */

    __API_FXN_CALLED__(self);

    amfree(pself->volume_label);
    amfree(pself->volume_time);
    dumpfile_free(pself->volume_header);
    pself->volume_header = NULL;

    if (device_in_error(self))
        return pself->status;

    if (!setup_handle(self)) {
        /* setup_handle already set our error message */
	return pself->status;
    }

    // if no full reader/write needs to be shut down
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);

    if (self->catalog_label && self->catalog_header) {
	char *header_buf;

	header_buf = unquote_string(self->catalog_header);
	amanda_header = g_new(dumpfile_t, 1);
	fh_init(amanda_header);
	if (strlen(header_buf) > 0) {
	    parse_file_header(header_buf, amanda_header, strlen(header_buf));
	}

	pself->header_block_size = strlen(header_buf);
	g_free(header_buf);
	pself->volume_header = amanda_header;
    } else {
	if (!make_bucket(pself)) {
            __API_LONG_FXN_ERR_RETURN__(self,": make bucket failed");
	    return pself->status;
	}

	key = tapestart_key(self); // prefix+"special"+"-tapestart"

	s3_device_get_keys_restored(pself, 0);
        s3_buffer_init(s3buf_label_tmp, DISK_BLOCK_BYTES, self->verbose); // small-size buffer
	result = s3_read(get_temp_curl(self), self->bucket, key,
                          S3_BUFFER_WRITE_FUNCS, s3buf_label_tmp,
                          NULL, NULL);
	g_free(key);

	if (!result)
        {
            guint response_code;
            s3_error_code_t s3_error_code;

            s3_error(get_temp_curl(self), NULL, &response_code, &s3_error_code,
		     NULL, NULL, NULL);

	    s3_buffer_destroy(s3buf_label_tmp);

            /* if it's an expected error (not found), just return FALSE */
            switch ( MIX_RESPCODE_AND_S3ERR(response_code,s3_error_code) ) {
                case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_None):
                case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NotFound):
                case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_Unknown):
                case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchKey):
                case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchEntity):
                case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchBucket):
                   break;
                default:
                    /* otherwise, log it and return */
                    device_set_error(pself,
                        g_strdup_printf(_("While trying to read tapestart header: %s"),
                               s3_strerror(get_temp_curl(self))),
                               DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
                    return pself->status;
            }

            g_debug(_("Amanda header not found while reading tapestart header (this is expected for empty tapes)"));
                    device_set_error(pself,
				    g_strdup(_("Amanda header not found -- unlabeled volume?")),
				       DEVICE_STATUS_DEVICE_ERROR
				     | DEVICE_STATUS_VOLUME_ERROR
				     | DEVICE_STATUS_VOLUME_UNLABELED);
            return pself->status;
	}

	/* handle an empty file gracefully */
	if (s3_buffer_lsize_func(s3buf_label_tmp) == 0) {
	    device_set_error(pself, g_strdup(_("Empty header file")),
			     DEVICE_STATUS_VOLUME_ERROR);
	    s3_buffer_destroy(s3buf_label_tmp);
            return pself->status;
	}

	pself->header_block_size = s3_buffer_lsize_func(s3buf_label_tmp);
	g_assert(s3buf_label_tmp->buffer != NULL);

        {
            char *buf = s3_buffer_data_func(s3buf_label_tmp);

            amanda_header = g_new(dumpfile_t, 1);
            parse_file_header(buf, amanda_header, pself->header_block_size);
            pself->volume_header = amanda_header;

            s3_buffer_destroy(s3buf_label_tmp);
        }

	if (amanda_header->type != F_TAPESTART) {
	    device_set_error(pself, g_strdup(_("Invalid amanda header")),
			     DEVICE_STATUS_VOLUME_ERROR);
            return pself->status;
	}

	if (!self->catalog_label || self->catalog_header) {
	    size_t header_size = 0;
	    char *buf;
	    buf = device_build_amanda_header(DEVICE(self), amanda_header,
					     &header_size);
	    catalog_reset(self, buf, amanda_header->name);
	    g_free(buf);
	}
    }

    pself->volume_label = g_strdup(amanda_header->name);
    pself->volume_time = g_strdup(amanda_header->datestamp);
    /* pself->volume_header is already set */

    device_set_error(pself, NULL, DEVICE_STATUS_SUCCESS);

    __API_LONG_FXN_CHECK__(self,"[completed]");
    return pself->status;
}

static gboolean
s3_device_start(Device * pself, DeviceAccessMode mode,
                 char * label, char * timestamp) 
{
    S3Device * self = S3_DEVICE(pself);
    gboolean result;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (device_in_error(self))
        return FALSE;

    if (!setup_handle(self)) {
        /* setup_handle already set our error message */
	return FALSE;
    }

    // truncate is needed if we/re starting up
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);
    pself->access_mode = mode;
    g_mutex_lock(pself->device_mutex);
    pself->in_file = FALSE;
    g_mutex_unlock(pself->device_mutex);

    /* try creating the bucket, in case it doesn't exist */
    if (!make_bucket(pself)) {
        __API_LONG_FXN_ERR_RETURN__(self,": make bucket failed");
	return FALSE;
    }

    /* take care of any dirty work for this mode */
    switch (mode) {
        case ACCESS_READ:
            // no need to rescan?
	    if (pself->volume_label)
                return TRUE; 
            /* s3_device_read_label already set our error message */
            __API_LONG_FXN_CHECK__(self,"[ACCESS_READ]");
            return (s3_device_read_label(pself) == DEVICE_STATUS_SUCCESS);

        case ACCESS_WRITE:
	    s3_device_set_reuse(pself);
            if (!delete_all_files(self)) {
		return FALSE;
	    }

            /* write a new amanda header */
            if (!write_amanda_header(self, label, timestamp)) {
                return FALSE;
            }

	    g_free(pself->volume_label);
	    g_free(pself->volume_time);
	    pself->volume_label = g_strdup(label);
	    pself->volume_time = g_strdup(timestamp);

	    /* unset the VOLUME_UNLABELED flag, if it was set */
	    device_set_error(pself, NULL, DEVICE_STATUS_SUCCESS);
            break;

        // get total amount in volume (to reassess)
        case ACCESS_APPEND:
            {
                GSList *keys;
                guint last_file;
                objbytes_t total_size = 0;

                // need to scan?
                if (pself->volume_label == NULL && s3_device_read_label(pself) != DEVICE_STATUS_SUCCESS) {
                    /* s3_device_read_label already set our error message */
                    return FALSE;
                }

                last_file = find_last_file(self);
                if (last_file == G_MAXUINT) {
                    __API_LONG_FXN_ERR_RETURN__(self,": append failed");
                    return FALSE;
                }

                // arrived at last file correctly
                pself->file = last_file;

                g_info("renew volume_bytes count %s/%s", self->bucket, self->prefix);

                // XXX: all keys for all files .. may take a while.
                result = s3_list_keys(get_temp_curl(self), self->bucket, NULL, self->prefix, NULL, 0,
                                      &keys, &total_size);
                if(!result) {
                    // fault really blocks device
                    device_set_error(pself,
                        g_strdup_printf(_("While listing all S3 keys: %s"), s3_strerror(get_temp_curl(self))),
                        DEVICE_STATUS_DEVICE_ERROR|DEVICE_STATUS_VOLUME_ERROR);
                    return FALSE;
                }
                g_info("renew volume_bytes count %s/%s == %ld Kbytes", self->bucket, self->prefix, (total_size+1023)>>10);
                self->volume_bytes = total_size;
		slist_free_full(keys, free_s3_object);
                __API_LONG_FXN_CHECK__(self,"[ACCESS_APPEND]");
                return TRUE;
            }

        case ACCESS_NULL:
            g_assert_not_reached();
            return FALSE;
    }

    __API_LONG_FXN_CHECK__(self,"[ACCESS_WRITE]");
    return TRUE;
}

static gboolean
s3_device_finish (Device * pself)
{
    S3Device *self = S3_DEVICE(pself);

    __API_FXN_CALLED__(self);  // [never verbose?] 

    // if no full reader/write needs to be shut down
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);

    __API_LONG_FXN_CHECK__(self,"[done]");

    /* we're not in a file anymore */
    pself->access_mode = ACCESS_NULL;

    if (device_in_error(pself)) 
        return FALSE;

    return TRUE;
}

/* functions for writing */


static objbytes_t
s3_device_get_bytes_read(
    Device * pself)
{
    S3Device *self = S3_DEVICE(pself);
    S3_by_thread *s3t = NULL;
    S3_by_thread *s3t_begin = self->s3t;
    S3_by_thread *s3t_end_recovery = s3t_begin + self->nb_threads_recovery;
    objbytes_t   dltotal;

    g_mutex_unlock(pself->device_mutex);
    /* Add per thread */
    g_mutex_lock(self->thread_list_mutex);
    dltotal = self->dltotal;
    for(s3t = s3t_begin; s3t != s3t_end_recovery ; ++s3t) 
    {
    	objbytes_t s3t_dlnow = (objbytes_t) g_atomic_pointer_get(&s3t->dlnow);
        if (s3t_dlnow == 1) 
            continue;
	dltotal += s3t_dlnow;
    }
    g_mutex_unlock(self->thread_list_mutex);
    g_mutex_lock(pself->device_mutex);

    __API_FXN_CALLED__(self);  // [never verbose?] 

    return dltotal;
}


static objbytes_t
s3_device_get_bytes_written(
    Device * pself)
{
    S3Device *self = S3_DEVICE(pself);
    S3_by_thread *s3t = NULL;
    S3_by_thread *s3t_begin = self->s3t;
    S3_by_thread *s3t_end_backup = s3t_begin + self->nb_threads_backup;
    objbytes_t   ultotal;

    g_mutex_unlock(pself->device_mutex);
    /* Add per thread */
    {
        g_mutex_lock(self->thread_list_mutex);
        ultotal = self->ultotal;
        for(s3t = s3t_begin; s3t != s3t_end_backup ; ++s3t) 
        {
            objbytes_t ulnow = (objbytes_t) g_atomic_pointer_get(&s3t->ulnow);
            if (ulnow == 1) 
                continue;
            ultotal += ulnow;
        }
        g_mutex_unlock(self->thread_list_mutex);
    }
    g_mutex_lock(pself->device_mutex);

    __API_FXN_CALLED__(self);  // [never verbose?] 
    return ultotal;
}

static gboolean
thread_check_at_write_file_finish(const S3Device *self, const S3_by_thread *s3t)
{
    // cancel-threads marks detection of the *last* byte
    if ( self->next_ahead_byte != self->end_xbyte )   
        return FALSE;
    // this thread must be the last transfer to have run
    if ( s3t->xfer_end != self->next_xbyte )   
        return FALSE;
    return TRUE;
}


// single case: reset for each segment to cover
static S3_by_thread *
thread_range_factory(S3Device *self, char *objkey, objbytes_t pos, signed_xferbytes_t upbytes_minus_dn)
{
    S3_by_thread *s3t = NULL;
    guint r;

    // called in all cases where new thread is needed
    s3t = ( upbytes_minus_dn > 0 ? claim_free_thread(self, 0, self->nb_threads_backup, TRUE)
                                 : claim_free_thread(self, 0, self->nb_threads_recovery, TRUE) );

    if (!s3t)
        return NULL;
    if (device_in_error(self))
        return NULL;

    // set a range correctly (respects end_xbyte)
    r = setup_active_thread(self, s3t, objkey, pos, upbytes_minus_dn);
    if (!r)
        return NULL;

    self->next_ahead_byte = max(self->next_ahead_byte, s3t->xfer_end); // define end if we finish a file
    return s3t;
}

// single case: reset for each segment to cover
static S3_by_thread *
thread_device_write_factory(S3Device *self, char *objkey, objbytes_t wrpos)
{
    return thread_range_factory(self, objkey, wrpos, +self->dev_blksize);
    // standard inits with dev_blksize spacing (up to end_xbyte)
}

// single case: reset for each segment to cover
static S3_by_thread *
thread_buffer_write_factory(S3Device *self, char *objkey, objbytes_t wrpos)
{
    return thread_range_factory(self, objkey, wrpos, +self->buff_blksize);
    // standard inits with dev_blksize spacing (up to end_xbyte)
}


// single case: reset for each segment to cover
static S3_by_thread *
thread_read_base_factory(S3Device *self, char *objkey, objbytes_t pos)
{
    objbytes_t eodbytes = 0;
    // standard inits with buff_blksize spacing (up to end_xbyte)
    S3_by_thread *s3t = thread_range_factory(self, objkey, pos, -self->dev_blksize);
    CurlBuffer *s3buf = &s3t->curl_buffer;

    if (!s3t) return NULL;

    s3t->time_expired = G_MAXINT64;
    eodbytes = s3_buffer_read_size_func(s3buf); // expected to be "read" when filled in

    s3t->at_final = FALSE; // clear the EOF flag (waiting to detect)
    s3t->ahead = TRUE; // a speculative read [chg w/mutex prot.]
    // act as if progress was made to reset timeout
    // trigger a reset of timer [set up dtotal in advance]
    s3t->dlnow_last = 0;
    s3t->ulnow_last = 0;
    s3_thread_xferinfo_func(s3t, max(eodbytes, self->buff_blksize), s3t->dlnow_last+1, 0, 0);
    return s3t;
}

// two cases: initial case factory and re-use factory
static S3_by_thread *
thread_write_init_mp_factory(S3Device *self, char *objkey, objbytes_t pos, objbytes_t partlen)
{
    // standard inits with dev_blksize spacing (limited by end_xbyte)
    Device *pself = DEVICE(self);
    S3_by_thread *s3t = NULL;
    s3_result_t result;

    // dangerously close to the maximum!
    if (self->mp_uploadId && self->mp_next_uploadNum) 
    {
        if (self->mp_next_uploadNum >= S3_MULTIPART_UPLOAD_MAX)
            pself->is_eom = TRUE; // advise splitting tape NOW!

        // dangerously close to the maximum!
        if (self->mp_next_uploadNum > S3_MULTIPART_UPLOAD_MAX) {
            device_set_error(pself, g_strdup(_("Too many blocks for mp-upload!")), DEVICE_STATUS_DEVICE_ERROR);
            return NULL;
        }

        s3t = thread_range_factory(self, objkey, pos, +partlen);

        if (!s3t)
            return NULL;

        g_assert(self->mp_etag_tree);

        s3t->mp_etag_tree_ref = self->mp_etag_tree;
        g_tree_ref(s3t->mp_etag_tree_ref);
        s3t->object_uploadId = g_strdup(self->mp_uploadId);
        s3t->object_uploadNum = self->mp_next_uploadNum;

        if (s3t->at_final) ////// FINAL MP-THREAD PART-UPLOAD 
            goto at_final;                                           ////// --> clear global spawn-info

        ++self->mp_next_uploadNum; // 1..N a general ordering of range-writes
        return s3t;                                                  ////// NEW NON-FINAL PART-UPLOAD ACTIVE
    }

    s3t = thread_range_factory(self, objkey, pos, +partlen);

    if (!s3t)
        return NULL;

    /////////////////////////////////////////////////////////////////////////////////////
    // no mp-upload currently running

    // advance with next active multipart part
    // s3t must be block-aligned for new upload
    if (s3t->xfer_begin != s3t->object_offset) {
        g_warning("%s: EOF: xfer_begin mismatch key=%s and xfer_begin=@%#lx object_offset=@%#lx",__FUNCTION__,
                   s3t->object_subkey, s3t->xfer_begin, s3t->object_offset );
        return NULL;
    }

    g_assert(!self->mp_uploadId && !self->mp_next_uploadNum);

    result = initiate_multi_part_upload(self, s3t);
    if (result != S3_RESULT_OK)
        return NULL;
    if (device_in_error(self))
        return NULL;

    // if starting block is last block
    if (s3t->at_final)
        goto at_final;

    if (!self->mp_etag_tree) {
        self->mp_etag_tree = s3t->mp_etag_tree_ref;
        g_tree_ref(self->mp_etag_tree); // count this reference
    }

    self->mp_uploadId = g_strdup(s3t->object_uploadId); // XXX: maybe leave NULL instead?
    self->mp_next_uploadNum = s3t->object_uploadNum; // 1..N a general ordering of range-writes
    ++self->mp_next_uploadNum; // 1..N a general ordering of range-writes
    return s3t;

  at_final:
    g_free(self->mp_uploadId);
    self->mp_uploadId = NULL;
    self->mp_next_uploadNum = 0;
    return s3t;
}

// two cases: initial case factory and re-use factory
static S3_by_thread *
thread_write_mp_factory(S3Device *self, char *objkey, objbytes_t pos)
{
    // use_mp (streamed or resendable parts) --- get_write_file_objkey_offset
    guint max_blksize = max(self->dev_blksize, self->buff_blksize);

    // if it overflows... use other 
    if (max_blksize == G_MAXUINT)
        max_blksize = self->buff_blksize;
 
    return thread_write_init_mp_factory(self, objkey, pos, max_blksize);
}

static S3_by_thread *
thread_write_reliable_mp_factory(S3Device *self, char *objkey, objbytes_t pos)
    { return thread_write_init_mp_factory(self, objkey, pos, self->buff_blksize); }

// use largest buffer size as upload part (maybe streaming) to achieve 
static S3_by_thread *
thread_write_mpcopy_factory(S3Device *self, char *objkey, objbytes_t pos)
{
    Device *pself = DEVICE(self);
    const int blockNum = 1 + (pos / self->dev_blksize); // constant size blocks .. starting with 1

    // dangerously close to the maximum!
    if (blockNum >= S3_MULTIPART_UPLOAD_MAX)
        pself->is_eom = TRUE; // advise splitting tape NOW!

    // dangerously close to the maximum!
    if (blockNum > S3_MULTIPART_UPLOAD_MAX) {
        device_set_error(pself, g_strdup(_("Too many blocks for mp-upload!")), DEVICE_STATUS_DEVICE_ERROR);
        return NULL;
    }

    return thread_write_reliable_mp_factory(self, objkey, pos);
}

static S3_by_thread *
thread_write_cp_factory(S3Device *self, char *objkey, objbytes_t wrpos)
{
    // standard inits with dev_blksize spacing (limited by end_xbyte)
    S3_by_thread *s3t = NULL;
    objbytes_t blocknum = 1 + (wrpos/self->buff_blksize); // constant size blocks .. starting with 1

    // global tree setup (done with lock)
    if (!self->gcp_object_tree) {
        self->gcp_object_tree = g_tree_new_full(gptr_cmp, NULL, NULL, g_free); 
        g_tree_ref(self->gcp_object_tree); // count this reference
    }

    s3t = thread_buffer_write_factory(self, objkey, wrpos);

    if (!s3t)
        return NULL;

    // moved here from setup_active_thread
    s3t->object_uploadNum = blocknum;
    s3t->gcp_object_tree_ref = self->gcp_object_tree;
    g_tree_ref(s3t->gcp_object_tree_ref); // count this reference
    return s3t;
}

static s3_result_t
thread_write_close_compose(const S3Device *self, S3_by_thread *s3t, char *etag)
{
    s3_result_t result;
    CurlBuffer *s3buf = &s3t->curl_buffer;
    objbytes_t blocknum = s3t->object_uploadNum;

    amfree(etag); // not needed

    g_tree_insert(s3t->gcp_object_tree_ref, GSIZE_TO_POINTER(blocknum), g_strdup(s3t->object_subkey));
    g_debug("%s: (n=%d) added to primary-blks-tree key=#%010lx value=%s ", __FUNCTION__,
          g_tree_nnodes(s3t->gcp_object_tree_ref), 
          blocknum, s3t->object_subkey);

    // check both cases to prevent appending now
    if ( blocknum % GCP_COMPOSE_COUNT != 0 && !thread_check_at_write_file_finish(self,s3t) )
        return S3_RESULT_RETRY;

    // NOTE: waits until all parts have added
    result = thread_write_close_compose_append(self, s3t);
    if (result != S3_RESULT_OK) {
        // error received upon next write...
        if (!s3t->errmsg)
            s3t->errmsg = g_strdup_printf(_("While completing upload [key=%s r=%d] to %s: %s"),
                                         s3t->object_subkey, result,
                                         S3_name[self->s3_api], s3_strerror(s3t->s3));
        if (!s3t->errflags)
        s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
        g_warning("[%p] %s: s3-op error result ctxt#%ld: key=%s flag=%x mesg=%s", s3buf, __FUNCTION__, s3t - self->s3t,
                    s3t->object_subkey, s3t->errflags, s3t->errmsg);
    }
    return result;
}

static s3_result_t
thread_write_final_compose(S3Device *wrself, S3_by_thread *s3t)
{
    Device *pself = DEVICE(wrself);
    const S3Device *self = wrself;
    s3_result_t result = S3_RESULT_OK;
    objbytes_t blocknum = s3t->object_uploadNum;
    char        *file_objkey;
    GString *buf;

    // must be the at_final thread of one copypart set...

    if (! thread_check_at_write_file_finish(self,s3t))
        return S3_RESULT_RETRY;

    buf = g_string_new("<ComposeRequest>\n");
    g_string_append_printf(buf, "<Component>\n    <Name>%s</Name>\n   </Component>\n",self->file_objkey);
    g_string_append_printf(buf,"</ComposeRequest>\n");

    file_objkey = file_to_single_file_objkey(self, pself->file);

    g_debug("%s: Final compose request ..Total number of objects upload  count=%ju ",__FUNCTION__, (uintmax_t)blocknum);
    g_assert( ! g_mutex_trylock(self->thread_list_mutex) );
    {
        CurlBuffer upload_tmp = { 0 }; // to load once only
        CurlBuffer *s3buf_tmp = &upload_tmp;
        s3_result_t result = S3_RESULT_OK;

        s3_buffer_load_string(s3buf_tmp, g_string_free(buf, FALSE), self->verbose);

        g_mutex_unlock(self->thread_list_mutex); // allow unlocked
        result = s3_compose_append_upload(s3t->s3,
                                         self->bucket, file_objkey,
                                         S3_BUFFER_READ_FUNCS, s3buf_tmp);
        g_mutex_lock(self->thread_list_mutex); 

        if (result != S3_RESULT_OK) {
            if (device_in_error(pself))
                return result;
            s3t->errflags = DEVICE_STATUS_DEVICE_ERROR;
            s3t->errmsg = g_strdup_printf(_("Final Compose Append failed: '%s'"), file_objkey);
            g_free((char*)file_objkey);
            file_objkey = NULL;
            return result;
        }

        g_free((char*)file_objkey);
        file_objkey = NULL;

        g_mutex_unlock(self->thread_list_mutex); // allow unlocked
        result = s3_delete(s3t->s3, self->bucket, wrself->file_objkey);
        g_mutex_lock(self->thread_list_mutex); 

        if (result != S3_RESULT_OK) {
                g_debug("%s: Delete key failed: %s", __FUNCTION__, s3_strerror(s3t->s3));
                return S3_RESULT_FAIL;
        }
    }

    return result;
}

// flush current mp if possible
static s3_result_t
thread_write_close_mp(const S3Device *self, S3_by_thread *s3t, char *etag)
{
    s3_result_t result;
    CurlBuffer *s3buf = &s3t->curl_buffer;
    GTree *const blktree = s3t->mp_etag_tree_ref;

    if (!etag || !etag[0]) {
        s3t->errmsg = g_strdup_printf(_("No etag was received [key=%s] to %s: %s"), 
                                         s3t->object_subkey, 
                                         S3_name[self->s3_api], s3_strerror(s3t->s3));
        s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
        g_warning("[%p] %s: s3-op no etag returned ctxt#%ld: key=%s flag=%x mesg=%s", s3buf, __FUNCTION__, s3t - self->s3t,
                    s3t->object_subkey, s3t->errflags, s3t->errmsg);
        return S3_RESULT_FAIL;
    }

    g_tree_insert(blktree, GSIZE_TO_POINTER(s3t->xfer_begin), etag);
    g_debug("%s: (n=%d) added to primary-blks-tree pos=#%ld/%#lx key=%s uploadid=[...%s] etag=[...%s]", __FUNCTION__, 
          g_tree_nnodes(blktree), 
          s3t->object_uploadNum, s3t->xfer_begin, s3t->object_subkey,
          s3t->object_uploadId + strlen(s3t->object_uploadId)-5, etag + strlen(etag)-5);

    // only act on final mp-uploads
    if (!s3t->at_final)
        return S3_RESULT_RETRY;                                                ////// RETURN (RETRY)

    // NOTE: waits until all parts have added
    result = thread_mp_upload_combine(self, s3t);
    if (result != S3_RESULT_OK) {
        // error received upon next write...
        if (!s3t->errmsg)
            s3t->errmsg = g_strdup_printf(_("While completing upload [key=%s r=%d] to %s: %s"), 
                                         s3t->object_subkey, result,
                                         S3_name[self->s3_api], s3_strerror(s3t->s3));
        if (!s3t->errflags)
            s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;

        g_warning("[%p] %s: s3-op error result ctxt#%ld: key=%s flag=%x mesg=%s", s3buf, __FUNCTION__, s3t - self->s3t,
                    s3t->object_subkey, s3t->errflags, s3t->errmsg);
    }
    return result;
}

static s3_result_t
thread_write_mpblock_copypart(const S3Device *self, S3_by_thread *s3t, char *etag)
{
    s3_result_t result;
    const objbytes_t pos = s3t->object_offset;
    const int blocknum = 1 + (pos / self->dev_blksize); // constant size blocks!
    static const char *const FAKE_UPLOAD_ID = "___UPLOAD_ID_DELAY_LOCK___"; // signal who tries first

    g_assert(pos % self->dev_blksize == 0); // must divide cleanly

    // close the current block-upload
    result = thread_write_close_mp(self,s3t,etag);
    if (result != S3_RESULT_OK)
        return S3_RESULT_RETRY;                                                ////// RETURN (RETRY OR ERROR)

    etag = NULL;

    // initiate the group upload??
    if (!self->mpcopy_uploadId && self->file_objkey)
    {
        S3_by_thread fake_s3t_store = { 0 };
        S3_by_thread *fake_s3t = &fake_s3t_store;

        g_debug("%s: start whole-file copypart-upload: [%#lx-%#lx)", __FUNCTION__, 
                pos, s3t->xfer_end);

        //// get an uploadId for the whole file
        fake_s3t->s3 = s3t->s3;
        fake_s3t->object_subkey = g_strdup(self->file_objkey);

        {
            // NOTE: PERFORM REQUEST RELEASES thread_list_lock
            ((S3Device*)self)->mpcopy_uploadId = FAKE_UPLOAD_ID;       // make all other threads wait while this thread does not own lock...
            result = initiate_multi_part_upload(self, fake_s3t);
            ((S3Device*)self)->mpcopy_uploadId = NULL;                // reset to default NULL again in case of error
        }

        if ( result != S3_RESULT_OK ) {
            g_warning("%s: WARNING: failed top-multipart for multi-part-blocks: [%#lx-%#lx)", __FUNCTION__, 
                    s3t->xfer_begin, s3t->xfer_end);
            return result; // cannot do anything for this upload
        }

        // WARNING: magically changing global-Device values
        ((S3Device*)self)->mpcopy_uploadId = fake_s3t->object_uploadId;       // continue upload w/copypart

        g_cond_broadcast(self->thread_list_cond); // in case any thread is waiting
    }

    // quickly wait until SOME thread completes the global mpcopy upload
    while ( self->mpcopy_uploadId == FAKE_UPLOAD_ID )
        g_cond_wait_until(self->thread_list_cond, self->thread_list_mutex, g_get_monotonic_time () + 5 * G_TIME_SPAN_SECOND);

    g_assert(self->mpcopy_uploadId && self->file_objkey);

    // copy the finished part into the larger upload
    /////////////////////////////// prepared with string: now unlock
    {
        g_mutex_unlock(self->thread_list_mutex); // allow unlocked

        g_assert(s3t->xfer_begin != s3t->xfer_end);

        result = s3_copypart_upload(s3t->s3, self->bucket, self->file_objkey,
                    self->mpcopy_uploadId, blocknum, &etag,
                    s3t->object_subkey);

        g_mutex_lock(self->thread_list_mutex); // allow unlocked
    }

    if (result != S3_RESULT_OK) {
        if (device_in_error(self)) 
            return result;
        g_free(s3t->errmsg);
        s3t->errmsg = g_strdup_printf(_("While in block-copypart after upload"));
        s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
        return result;
    }

    // copy in an entry at the start-of-large-size-block
    g_tree_insert(self->mpcopy_etag_tree, GSIZE_TO_POINTER(pos), etag) ;
    g_debug("%s: (n=%d) added BLOCK-FLAG to secondary-blks-tree pos=#%d/%#lx wkey=%s uploadid=[...%s]", __FUNCTION__, 
          g_tree_nnodes(self->mpcopy_etag_tree), blocknum, pos, self->file_objkey,
          self->mpcopy_uploadId + strlen(self->mpcopy_uploadId)-5);

    /////////////////////////////// prepared with string: now unlock
    {
        g_mutex_unlock(self->thread_list_mutex); // allow unlocked

        result = s3_delete(s3t->s3, self->bucket, s3t->object_subkey);

        g_mutex_lock(self->thread_list_mutex); // allow unlocked
    }
    /////////////////////////////// prepared with string: now lock

    if (result != S3_RESULT_OK) {
        if (device_in_error(self)) 
            return result;
        g_warning("%s: WARNING: delete of temp-part failed: [%#lx-%#lx)", __FUNCTION__, 
                    s3t->xfer_begin, s3t->xfer_end);
    }

    return S3_RESULT_OK;                                             ////// RETURN SUCCESS
}


static s3_result_t
thread_write_mpblocks_combine(S3Device *wrself, S3_by_thread *s3t)
{
    const S3Device *self = wrself;
    objbytes_t lastblkoff = s3t->object_offset;
    s3_result_t result = S3_RESULT_OK;
    GTree *const blktree = s3t->mp_etag_tree_ref;
    GTree *const filetree = self->mpcopy_etag_tree;
    char *lastetag = NULL;

    // must be the at_final thread of one copypart set...

    if (! thread_check_at_write_file_finish(self,s3t))
        return S3_RESULT_RETRY;

    // GLOBAL: rule out if cancel_threads was not run yet
    if ( self->mp_uploadId || self->mp_next_uploadNum )
        return S3_RESULT_RETRY;

    if ( g_tree_nnodes(blktree) )
        return S3_RESULT_RETRY;
    // detected any later blocks uploaded?
    if ( get_least_next_elt(filetree, s3t->xfer_begin+1) != (objbytes_t) G_MAXUINT64 )
        return S3_RESULT_RETRY;

    ////////////////////////
    /// transform *this* thread into final-combine thread to complete mpblocks upload
    ////////////////////////

    lastetag = g_tree_lookup(filetree, GSIZE_TO_POINTER(lastblkoff));
    lastblkoff = get_greatest_prev_elt(filetree, s3t->xfer_end); // offset of final block

    g_assert(lastetag && get_greatest_prev_elt(filetree, s3t->xfer_end) == lastblkoff );

    if (blktree) g_tree_unref(blktree); // release old reference
    g_tree_ref(filetree); // add new reference
    g_free((char*)s3t->object_subkey);
    g_free((char*)s3t->object_uploadId);

    s3t->mp_etag_tree_ref = filetree; // use the alternate large tree 
    s3t->object_subkey = self->file_objkey; // move out of global
    s3t->object_uploadId = self->mpcopy_uploadId; // move out of global
    s3t->object_offset = 0; // reset to base of file
    s3t->object_uploadNum = 1 + ( lastblkoff / self->dev_blksize ); // needed count of blocks (including final)

    g_debug("%s: completing mpblocks combination s3t%ld key=%s uploadId=[....%s]", __FUNCTION__, s3t - self->s3t, 
          s3t->object_subkey, s3t->object_uploadId + strlen(s3t->object_uploadId)-5);
    
    // now close the whole-object block-upload
    result = thread_write_close_mp(self,s3t, g_strdup(lastetag));

    wrself->mpcopy_uploadId = NULL;         // mpcopy upload is done
    wrself->file_objkey = NULL;             // mpcopy upload is done

    if (result != S3_RESULT_OK) {
        if (device_in_error(self))
            return result;
        g_free(s3t->errmsg);
        s3t->errmsg = g_strdup_printf(_("While in mpblocks-combine upon finish_file"));
        s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
    }

    return result;
}


void
init_block_xfer_hooks(S3Device *self)
{
    Device *pself = DEVICE(self);
    gboolean read_segments      = ( pself->access_mode == ACCESS_READ );            // API-device_start for downloads
    gboolean oversize_blks      = self->dev_blksize > self->buff_blksize;                // if a written block cannot fit
    gboolean oversize_mpblocks  = ( oversize_blks && self->allow_s3_multi_part_upload ); // need more checks...
    gboolean oversize_padded    = ( oversize_blks && self->allow_padded_stream_uploads );
    gboolean oversize_copied    = ( oversize_mpblocks && self->allow_s3_multi_part_copy );

    g_free((char*)self->file_objkey);
    self->file_objkey = NULL;

    self->fxn_create_curr_objkey    = NULL;
    self->fxn_thread_write_factory  = NULL; 
    self->fxn_thread_write_complete = NULL; // do nothing...
    self->fxn_thread_write_finish = NULL; // do nothing...

    if ( ! pself->in_file ) {
        return;
    }

    // detect if single object-key is used for reading
    if ( read_segments && self->end_xbyte < G_MAXINT64 ) {
        self->file_objkey = file_to_single_file_objkey(self, pself->file);
        self->fxn_create_curr_objkey = &get_read_file_objkey_offset; // reuse key on each read
        return;
    }

    // detect if multiple object-keys (blocks) are used for reading
    if ( read_segments ) {
        self->fxn_create_curr_objkey = &file_and_block_to_key;
        return;
    }

    self->fxn_create_curr_objkey    = &file_and_block_to_key;
    self->fxn_thread_write_factory  = &thread_device_write_factory; 

    // if we cannot stream, nor multi-part copy, all xfers must be the same small size
    if (oversize_blks && !oversize_padded && !oversize_mpblocks) {
        self->dev_blksize = min(DEVICE(self)->block_size, self->buff_blksize); // limit size of pself->block_size
        return init_block_xfer_hooks(self); // recurse, and not again
    }

    // determine if required and allowed
    self->use_padded_stream_uploads = FALSE;

    if (self->use_chunked) {
        // end_xbyte is set to infinite
        self->file_objkey = file_to_single_file_objkey(self, pself->file);
        self->fxn_create_curr_objkey = &get_write_file_objkey_offset; // reuse key on each write
        // standard init
        self->fxn_thread_write_factory = &thread_device_write_factory; 
        // standard EOF in any write
        // self->fxn_thread_write_complete = NULL; // nothing
        return;
    }

    if (self->use_s3_multi_part_upload && oversize_copied ) {
        self->file_objkey = file_to_single_file_objkey(self, pself->file);
        self->mpcopy_etag_tree = g_tree_new_full(gptr_cmp, NULL, NULL, g_free);   // continue upload w/copypart
        g_tree_ref(self->mpcopy_etag_tree); // count this reference

        // end_xbyte == offset + dev_blksize for each temp
        self->fxn_create_curr_objkey = &file_and_block_to_temp_key; // only temp block-keys 
        self->fxn_thread_write_factory = &thread_write_mpcopy_factory;
        // call thread_write_close_mp()
        //    + initial global file-mp [if missing]
        //    + perform copypart
        //    + close global file-mp [if present]
        self->fxn_thread_write_complete = &thread_write_mpblock_copypart;
        self->fxn_thread_write_finish = &thread_write_mpblocks_combine;
        return;
    }

    if (self->allow_gcp_compose && oversize_blks) {
        self->file_objkey = file_to_single_compose_file_objkey(self, pself->file);
        // end_xbyte == offset + buff_blksize for each temp
        self->fxn_create_curr_objkey = &file_and_block_to_compose_temp_key;
        self->fxn_thread_write_factory = &thread_write_cp_factory;
        self->fxn_thread_write_complete = &thread_write_close_compose;
        self->fxn_thread_write_finish = &thread_write_final_compose;
        return;
    }

    if (self->use_s3_multi_part_upload) {
        // end_xbyte is set to infinite
        self->use_padded_stream_uploads = oversize_padded;
        self->file_objkey = file_to_single_file_objkey(self, pself->file);
        self->fxn_create_curr_objkey = &get_write_file_objkey_offset; // reuse on each write
        self->fxn_thread_write_factory = &thread_write_mp_factory;
        // flush current mp 
        self->fxn_thread_write_complete = thread_write_close_mp;
        return;
    }

    if ( oversize_mpblocks ) {
        // end_xbyte is dev_blksize
        self->fxn_create_curr_objkey = &file_and_block_to_key;
        self->fxn_thread_write_factory = &thread_write_reliable_mp_factory;
        // flush current mp
        self->fxn_thread_write_complete = thread_write_close_mp;
        return;
    }

    self->use_padded_stream_uploads = oversize_padded; // dont use if not needed
    // end_xbyte is dev_blksize
    self->fxn_create_curr_objkey = &file_and_block_to_key;
    // standard init [with padding allowed]
    self->fxn_thread_write_factory = &thread_device_write_factory; 
    // self->fxn_thread_write_complete = NULL; // nothing
}
 
static gboolean
s3_device_start_file(Device *pself, dumpfile_t *jobInfo)
{
    S3Device *self = S3_DEVICE(pself);
    CurlBuffer amanda_header_tmp = { 0 }; // to load once only
    CurlBuffer *s3buf_hdr_tmp = &amanda_header_tmp;
    gboolean result;
    size_t header_size;
    char  *key = NULL;
    char *errmsg = NULL;
    DeviceStatusFlags errflags = DEVICE_STATUS_SUCCESS;
    int thread;
    char *hdrbuf;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    // reset position to start and wait for done
    if (!s3_device_seek_block(pself, 0))
        return FALSE;
    
    pself->is_eom = FALSE;

    /* Set the blocksize to zero, since there's no header to skip (it's stored
     * in a distinct file, rather than block zero) */
    jobInfo->blocksize = 0;

    /* Build the amanda header. */
    header_size = 0; /* no minimum size */
    hdrbuf = device_build_amanda_header(pself, jobInfo, &header_size);

    if (!s3_buffer_load(s3buf_hdr_tmp, hdrbuf, header_size, self->verbose)) {
        g_free(hdrbuf);
        errmsg = g_strdup(_("Amanda file header won't fit in a single block!"));
        errflags = DEVICE_STATUS_DEVICE_ERROR;
        goto error;
    }

    // flag to cancel/split if enabled
    if(check_at_leom(self, header_size))
        pself->is_eom = TRUE;

    // flag to cancel/split with error now
    if(check_at_peom(self, header_size)) {
        pself->is_eom = TRUE;
        errmsg = g_strdup(_("No space left on device"));
        errflags = DEVICE_STATUS_DEVICE_ERROR;
        goto error;
    }

    reset_file_state(self);

    {
        g_mutex_lock(pself->device_mutex);
        /* set the file and block numbers correctly */
        pself->file = max(pself->file,0);
        pself->file++; // increment on start [next file]
        pself->in_file = TRUE;
        g_mutex_unlock(pself->device_mutex);
    }

    {
        g_mutex_lock(self->thread_list_mutex);
        for(thread = 0; thread != self->nb_threads; ++thread) {
            self->s3t[thread].done = TRUE;
            self->s3t[thread].dlnow = 0;
            self->s3t[thread].ulnow = 0;
        }
        // assess all current setup
        init_block_xfer_hooks(self);
        
        g_mutex_unlock(self->thread_list_mutex);
    }

    /* write it out as a special block (not the 0th) */
    key = filestart_key(self, pself->file); // prefix+"fXXXXXXXX-filestart" (file#)

    result = s3_upload(get_temp_curl(self), self->bucket, key, FALSE,
		       S3_BUFFER_READ_FUNCS, s3buf_hdr_tmp,
                       NULL, NULL);
    g_free(key);
    key = NULL;

    if (!result) {
        errmsg = g_strdup_printf(_("While writing filestart header: %s"), s3_strerror(get_temp_curl(self)));
        errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
        goto error;
    }

    self->volume_bytes += header_size;

    s3_buffer_destroy(s3buf_hdr_tmp);

    __API_LONG_FXN_CHECK__(self,"[hdr-written]");
    return TRUE;

error:
    s3_buffer_destroy(s3buf_hdr_tmp);
    g_free(key);
    if (errflags != DEVICE_STATUS_SUCCESS) 
        device_set_error(pself, errmsg, errflags);

    __API_LONG_FXN_CHECK__(self,"[hdr-write-failed]");
    return FALSE;
}


static S3_by_thread  *
claim_free_thread(const S3Device * self, guint16 start_index, guint16 ctxt_limit, gboolean wait_flag)
{
    Device *pself = DEVICE(self);
    S3_by_thread *s3t = NULL;
    S3_by_thread *const s3t_end = self->s3t + self->nb_threads;        // fixed endpoint
    S3_by_thread *const s3t_begin = s3t_end - ctxt_limit;              // fixed start of non-reserve threads
    S3_by_thread *s3t_scan = s3t_begin + (start_index % ctxt_limit);   // rotate to all tried threads
    int max_blkspan = self->nb_threads*self->nb_threads / 2;

    // called with thread_list_lock held

    g_assert( ! g_mutex_trylock(self->thread_list_mutex) ); // must be locked now...


    // reset scan to full if we repeat
    for(;; s3t_scan = self->s3t)
    {
        int busy_cnt = 0;

        for ( s3t = s3t_scan ; s3t < s3t_end ; ++s3t, ++busy_cnt)
        {
            CurlBuffer *const s3buf = &s3t->curl_buffer;

            /* Check if the thread is in error from an earlier task */
            if ( s3t->errflags != DEVICE_STATUS_SUCCESS ) {
                device_set_error(pself,
                                 (char *)s3t->errmsg,
                                 s3t->errflags);
                s3t->errmsg = NULL;
                s3t->errflags = DEVICE_STATUS_SUCCESS;
                g_warning("%s: stopped free thread search file#%d and pos=@%#lx",__FUNCTION__,
                     pself->file, self->next_ahead_byte);
                return NULL;					                  /////// RETURN FAILURE
            }

            if ( !s3t->done ) 
                continue;
                
            // failed to lock it??   skip thread
            if ( ! s3_buffer_trylock(s3buf) ) {
                if (self->verbose)
                    g_warning("%s: busy locked-buffer s3t#%ld done=%d...", __FUNCTION__, s3t - self->s3t, s3t->done);
                continue;
            }

            if (s3t == self->s3t_temp_free)
                ((S3Device *)self)->s3t_temp_free = NULL; // not free any longer
            s3t->done = FALSE; // mark as busy now
            s3_buffer_unlock(s3buf);
            return s3t; // passed all the checks!		/////// RETURN SUCCESS
        } 

        // unsuccessful search is over
        if (!wait_flag) {
            g_debug("%s: NULL for no-wait thread search file#%d and pos=@%#lx",__FUNCTION__,
                 pself->file, self->next_ahead_byte);
            return NULL;					/////// RETURN FAILURE
        }

        /////// reloop from here....

        if (self->verbose)
            g_debug("%s: WAITING [busy>=%d] for free thread ctxt file#%d and pos=@%#lx",__FUNCTION__,
                 busy_cnt, pself->file, self->next_ahead_byte);

        // wait on a new change... and retry in 5 seconds just in case
        do 
        {
            S3_by_thread *earliest = NULL;
            objbytes_t blksize = 0;

            g_cond_wait_until(self->thread_list_cond, self->thread_list_mutex, g_get_monotonic_time () + 5 * G_TIME_SPAN_SECOND);

            // detect the OLDEST lagging transfer
            for ( s3t=s3t_begin ; s3t < s3t_end ; ++s3t ) {
                if (s3t->done) continue;   // not active?
                if (s3t->ahead) continue;  // no data from API yet?

                // record earliest transfer
                if (!earliest || s3t->xfer_begin < earliest->xfer_begin)
                    earliest = s3t;

                // record 'normal' block size to measure one xfer thread of lag
                blksize = max(blksize, s3t->xfer_end - s3t->xfer_begin);
            }

            // oldest thread is N^2/2 blocks behind newest thread?  [upload case]
            if ( earliest && earliest->xfer_begin + blksize*max_blkspan < self->next_ahead_byte ) {
                if (self->verbose)
                    g_warning("%s: WAITING for OLDEST byte-range file#%d and limit=%#lx oldest=@%#lx",__FUNCTION__,
                         pself->file, blksize*max_blkspan, earliest->xfer_begin);
                continue; // delay new threads until span shrinks
            }

            // detect signaled+periodic checking for free threads
            for ( s3t=s3t_begin ; s3t < s3t_end && !s3t->done ; ++s3t) 
              { }
        } while (s3t == s3t_end);

        if (self->verbose)
            g_debug("%s: RETRY find thread file#%d and pos=@%#lx",__FUNCTION__,
                 pself->file, self->next_ahead_byte);
    } // for(;;) ... 

    // NEVER BREAKS LOOP
    __builtin_unreachable();
}

static S3_by_thread  *
find_active_file_thread(S3Device * self, int file, objbytes_t pos, char **pkey)
{
    S3_by_thread *s3t = NULL;
    S3_by_thread *s3t_begin = self->s3t;
    S3_by_thread *s3t_end = s3t_begin + self->nb_threads;
    char *dummy_key = NULL;

    // either find an identical key to the S3Device ... (i.e. multipart)

    g_assert(s3t_begin);

    if (!pkey)
        pkey = &dummy_key; // ignore passed pkey arg

    *pkey = (*self->fxn_create_curr_objkey)(self, NULL, NULL, file, pos);

    for(s3t = s3t_begin; s3t < s3t_end ; ++s3t) {
        if ( s3t->done )
            continue; // not-active so don't look...
        if ( s3t->curl_buffer.cancel )
            continue; // must find a thread with upload-able or download-able bytes left

	g_assert( s3t->object_subkey );

        // NO-MATCH: if no key-prefix (or whole) match
        if ( ! g_str_has_prefix(s3t->object_subkey,*pkey) )
            continue; 

        // MATCH: range match means full multipart match
        if (s3t->xfer_begin <= pos && pos < s3t->xfer_end)
            break;
    }

    g_free(dummy_key);
    dummy_key = NULL;

    // could not find... so return new key if desired
    if (s3t == s3t_end)
        return NULL;

    // no new key as result was found
    g_free(*pkey);
    *pkey = NULL;

    return s3t; // found a matching thread
}

static void
reset_file_state(S3Device *self)
{
    Device *pself = DEVICE(self);
    // similar expected max-size for a weeensy 512MB-memory system
    objbytes_t blksize = S3_DEVICE_DEFAULT_BLOCK_SIZE; 

#ifdef __linux__
    {
        struct statfs st;
        int r = statfs("/dev/shm",&st); // get info from (*required*) tmpfs system

        if (!r && st.f_type == TMPFS_MAGIC) 
        {
           objbytes_t half_of_memory = st.f_bsize * st.f_bavail; // likely around %50 of physical memory
           
           blksize = half_of_memory >> 3;  // divide to 1/16th *all* physical mem...
           blksize = blksize / ( self->nb_threads + 1 );  // divide up by threads running plus reader/writer
           blksize = ( blksize + (1ULL<<21)-1 ) >> 21; // align correctly against 2M hugeblocks
           blksize <<= 21; // up to next 2M hugeblocks
           blksize = max(blksize, S3_DEVICE_DEFAULT_BLOCK_SIZE); // S3 minimum part size
           blksize = min(blksize, S3_DEVICE_MAX_BLOCK_SIZE);     // prevents brittle streaming-uploads & restarts
        }
    }
#endif

    /* we're not in a file anymore */
    {
        g_mutex_lock(pself->device_mutex);
        pself->is_eof = FALSE;
        pself->block = 0;
        pself->in_file = FALSE;
        pself->bytes_read = 0;
        pself->bytes_written = 0;;
        g_mutex_unlock(pself->device_mutex);
    }

    {
        g_mutex_lock(self->thread_list_mutex);
        self->ultotal = 0;
        self->dltotal = 0;
        self->next_xbyte = 0;
        self->next_ahead_byte = 0;
        self->end_xbyte = G_MAXINT64; 	 // infinite length is too far...
        self->curr_object_offset = 0; 	// updated for every newly-made r/w thread.. [as created]
	self->mp_next_uploadNum = 0;
        g_free(self->file_objkey);
        self->file_objkey = NULL;
        g_free(self->mp_uploadId);
        self->mp_uploadId = NULL;
        if (self->mp_etag_tree) {
            g_tree_destroy(self->mp_etag_tree);  // strings freed automatically
            self->mp_etag_tree = NULL;
        }
        if (self->mpcopy_etag_tree) {
            g_tree_destroy(self->mpcopy_etag_tree); // strings freed automatically
            self->mpcopy_etag_tree = NULL;
        }
        if (self->gcp_object_tree) {
            g_tree_destroy(self->gcp_object_tree); // no references left?
            self->gcp_object_tree = NULL;
        }

        self->dev_blksize = (pself->block_size ? : blksize); // requested block size [maybe oversize]
        self->buff_blksize = blksize;           // use most ideal size

        // setup fxn pointers if called
        init_block_xfer_hooks(self);
        
        g_mutex_unlock(self->thread_list_mutex);
    }
}

static guint
reset_idle_thread (S3_by_thread *s3t, gboolean verbose)
{
    CurlBuffer *s3buf = &s3t->curl_buffer;
    int r;

    s3t->curl_buffer.cancel = TRUE;  // done with any operations
    s3t->done = TRUE;                // done with any operations
    s3t->ahead = TRUE;               // done with any operations

    // halt all others...
    if (s3t->now_mutex) {
        g_mutex_lock(&s3t->_now_mutex_store); // block up waiters
        s3t->now_mutex = NULL;
        g_mutex_unlock(&s3t->_now_mutex_store); // detect abort
    }

    if (s3buf->mutex) {
        g_mutex_lock(&s3buf->_mutex_store);     // block up lockers
        s3buf->mutex = NULL;
        if (s3buf->cond) 
           g_cond_broadcast(s3buf->cond);   // wake up waiters to attempt locks
        s3buf->cond = NULL;
        g_mutex_unlock(&s3buf->_mutex_store);
    }

    g_thread_yield(); // allow others to complete work

    // retain s3t->s3 as is...
    g_free((void *)s3t->object_subkey);
    g_free((void *)s3t->object_uploadId);
    g_free((void *)s3t->errmsg);
    if (s3t->mp_etag_tree_ref)
        g_tree_unref(s3t->mp_etag_tree_ref);

    if (s3t->gcp_object_tree_ref)
        g_tree_unref(s3t->gcp_object_tree_ref);

    {
        // zero all other fields...
        s3t->object_uploadNum = 0;
        s3t->ulnow = 0;
        s3t->dlnow = 0;
        s3t->xfer_begin = 0;
        s3t->xfer_end = 0;
        s3t->at_final = FALSE;
        s3t->time_expired = 0;

        s3t->object_subkey = NULL;
        s3t->object_uploadId = NULL;
        s3t->errflags = DEVICE_STATUS_SUCCESS; // value == zero
        s3t->errmsg = NULL;
        s3t->mp_etag_tree_ref = NULL;
        s3t->gcp_object_tree_ref = NULL;
    }

    s3t->now_mutex = &s3t->_now_mutex_store;

    // test lock is free and clear buffer state
    r = s3_buffer_init(s3buf, 0, verbose);

    g_assert( g_mutex_trylock(&s3t->_now_mutex_store) );
    g_mutex_unlock(&s3t->_now_mutex_store);
    g_mutex_clear(&s3t->_now_mutex_store); // attempt to free resources
    g_mutex_init(&s3t->_now_mutex_store); // reset to free

    s3_buffer_reset_eod_func(s3buf,0); // empty eod ....
    return r;
}
    

static guint
setup_active_thread(S3Device *self, S3_by_thread *s3t, const char *newkey, objbytes_t pos, signed_xferbytes_t up_bytes)
{
    int r; 
    const Device *pself = DEVICE(self);
    CurlBuffer *s3buf = &s3t->curl_buffer;
    signed_xferbytes_t down_bytes = ( up_bytes < 0 ? -up_bytes : 0 );
    signed_xferbytes_t n_bytes = ( down_bytes ? : (guint)up_bytes );
    objbytes_t eodbytes = 0;
    objbytes_t xfer_end_xbyte = G_MAXINT64; // no limit by default

    reset_idle_thread(s3t, self->verbose);

    newkey = (*self->fxn_create_curr_objkey)(self,
                            &s3t->object_offset, &xfer_end_xbyte, // find range of objkey [or 0-infinite]
                            pself->file, pos);

    xfer_end_xbyte = min(xfer_end_xbyte, self->end_xbyte); // compute actual limit on xfer (if not infinite)
    s3t->xfer_begin = pos;
    s3t->xfer_end = min(pos + n_bytes, xfer_end_xbyte); // cap this key-end if needed
    eodbytes = s3t->xfer_end - s3t->xfer_begin;

    // must act on obj-complete limit for write-thread?
    if (self->fxn_thread_write_complete && s3t->xfer_end == xfer_end_xbyte)
        s3t->at_final = TRUE; // signal write-thread-session call xfer-complete is needed

    // reset with actual buffer now... [kept to smaller size to avoid crayz allocs!]
    r = s3_buffer_init(s3buf, min(eodbytes,self->buff_blksize), self->verbose);

    // out of memory!!
    if ( r == 0 ) {
	g_warning("WARNING: %s: thread creation failed ctxt#%ld [XXXXXX]: [%#lx:%#lx) %s-key=%s",
		    __FUNCTION__, s3t - self->s3t, pos, pos + n_bytes,
		    ( s3t->object_uploadNum ? "mp" : "block" ), newkey);
	device_set_error(DEVICE(self), g_strdup(_("Failed to allocate memory")), DEVICE_STATUS_DEVICE_ERROR);
        return 0;
    }
   
    /////////////////////// grab current byte config needed ///////////////////////

    if (s3t->at_final) {
        g_debug("%s: [%p] xfer_end has at_final ctxt#%ld [%#lx-%#lx)", __FUNCTION__, s3buf, 
              s3t - self->s3t, s3t->xfer_begin, s3t->xfer_end);
    }

    s3_buffer_reset_eod_func(s3buf, eodbytes); // auto eod instantly...
    s3buf->opt_padded = self->use_padded_stream_uploads;

    s3t->done = FALSE;   // thread is active
    s3t->ahead = FALSE;  // thread has in-use data

    s3t->object_subkey = g_strdup(newkey);
    g_free((char*)newkey); // created it above

    // keep a simple signature ready
    s3t->ulnow = ( up_bytes > 0 );      // 1 == Upload bytes coming
    s3t->dlnow = ( down_bytes > 0 );    // 1 == Download bytes coming

    return r;
}

//
// main s3/curl thread is started here
//    - spawned according to newly-writable block#
//    - main task is to create locally-started reads into circular-buffer
//    - calling/creating thread then writes what it can to fulfill write
//
static void
s3_thread_write_session(gpointer thread_data, gpointer data)
{
    S3_by_thread *s3t = (S3_by_thread *)thread_data;
    CurlBuffer *s3buf = &s3t->curl_buffer;
    const Device *pself = (Device *)data;
    S3Device *self = S3_DEVICE(pself);
    const int myind = s3t - self->s3t;
    gboolean result = FALSE;
    char *etag = NULL;
    char *mykey_tmp = s3t->object_subkey ? strdupa(s3t->object_subkey) : "";
    objbytes_t eodbytes;

    {
        g_mutex_lock(self->thread_list_mutex);
        if (s3t->errflags || s3t->errmsg || device_in_error(self)) {
            g_warning("[%p] %s: pre-existing s3-context error blocks progress ctxt#%ld: key=%s flag=%x mesg=%s", s3buf, __FUNCTION__, s3t - self->s3t,
                        s3t->object_subkey, s3t->errflags, s3t->errmsg);
            goto failed_locked;
        }
        g_mutex_unlock(self->thread_list_mutex);
    }

    // was cancelled before started...
    if (!mykey_tmp[0]) {
        if (self->verbose)
           g_debug("%s: pre-flushed [bgd-upload] write-session ctxt#%d [%p]: key=<erased> ",
              __FUNCTION__, myind, s3buf);
        // allow to continue...
    }

    {
        char *tname = NULL;
        char *keysuff = strrchr(s3t->object_subkey,'.');
        if ( self->fxn_create_curr_objkey == file_and_block_to_temp_key ) {
            tname = g_strdup_printf("wp%x.%02lx.%02lx", pself->file, s3t->object_offset/self->dev_blksize, (s3t->object_uploadNum-1)&0xff);
        }
        else if ( self->fxn_create_curr_objkey == &get_write_file_objkey_offset ) {
            tname = g_strdup_printf("wp%x.%02lx   ", pself->file, s3t->object_uploadNum-1);
        }
        else if ( self->fxn_create_curr_objkey == file_and_block_to_key && s3t->object_uploadId ) {
            tname = g_strdup_printf("wb%x:%c%c.%02lx", pself->file, keysuff[-2],keysuff[-1], s3t->object_uploadNum-1);
        }
        else if ( self->fxn_create_curr_objkey == file_and_block_to_key ) {
            tname = g_strdup_printf("wb%x:%c%c   ", pself->file, keysuff[-2],keysuff[-1]);
        }
        else if ( self->fxn_create_curr_objkey == file_and_block_to_compose_temp_key) {
            tname = g_strdup_printf("wc%x:%02lx", pself->file, s3t->object_offset/self->buff_blksize);
        }
        (void) pthread_setname_np(pthread_self(),tname);
        g_debug("[%p] upload worker thread ctxt#%ld [%s] [%#lx-%#lx)",&s3t->curl_buffer,s3t - self->s3t, 
					mykey_tmp, s3t->xfer_begin, s3t->xfer_end);
        g_free(tname);
    }

    // act as if progress was made to reset timeout
    s3t->time_expired = G_MAXINT64;
    eodbytes = s3_buffer_read_size_func(s3buf);

    // trigger a reset of timer with new ultotal estimate
    s3t->ulnow_last = 0;
    s3t->dlnow_last = 0;
    s3_thread_xferinfo_func(s3t, 0, 0, eodbytes, s3t->ulnow_last+1);

    // NOTE: s3_upload / s3_buffer_read_func must block
    //    as external "buffer-writes" push the data in

    // NOTE: wait for a full buffer... if we require sha256 or md5 in advance...
    if (!s3buf->opt_padded && eodbytes && eodbytes <= s3buf->max_buffer_size ) {

        if (s3buf->opt_verbose) 
	    g_debug("[%p] %s:%d: s3-op waiting for buffer-fill ctxt#%ld: [%#lx-(%#lx->%#lx)) key=%s",
	       s3buf, __FUNCTION__, __LINE__, s3t - self->s3t, s3t->xfer_begin,
               s3t->xfer_begin + s3_buffer_size_func(s3buf), 
               s3t->xfer_begin + eodbytes, s3t->object_subkey);

        s3_buffer_fill_wait(0, s3buf);
        eodbytes = s3_buffer_read_size_func(s3buf); // couild be changed by a reset

        if (s3buf->opt_verbose) 
	    g_debug("[%p] %s:%d: s3-op reached buffer-full ctxt#%ld: [%#lx-(%#lx->%#lx)) key=%s",
	       s3buf, __FUNCTION__, __LINE__, s3t - self->s3t, s3t->xfer_begin,
               s3t->xfer_begin + s3_buffer_size_func(s3buf), 
               s3t->xfer_begin + eodbytes, s3t->object_subkey);
    }

    // perform new part upload if data is nonempty
    if (eodbytes && s3t->object_uploadId)
    {
	result = s3_part_upload(s3t->s3, self->bucket, (char *)s3t->object_subkey,
				(char *)s3t->object_uploadId, s3t->object_uploadNum, &etag,
				S3_BUFFER_READ_FUNCS, s3buf,
                                s3_thread_progress_func, s3t);
        if (self->verbose)
            g_debug("[%p] %s:%d: s3-op completed ctxt#%ld: [%#lx-%#lx) @%#lx key=%s etag=%s",
               s3buf, __FUNCTION__, __LINE__, s3t - self->s3t, s3t->xfer_begin, s3t->xfer_end, 
               s3t->object_offset, s3t->object_subkey, etag + strlen(etag)-5);
    // perform new block upload if data is nonempty
    } else if (eodbytes) {
	result = s3_upload(s3t->s3, self->bucket, (char *)s3t->object_subkey,
			   self->use_chunked,
			   S3_BUFFER_READ_FUNCS, s3buf,
                           s3_thread_progress_func, s3t);
        if (self->verbose)
           g_debug("[%p] %s:%d: s3-op completed ctxt#%ld: key=%s", s3buf, __FUNCTION__, __LINE__, s3t - self->s3t,
               s3t->object_subkey);
    }

    // cleared if it was owned
    g_atomic_pointer_compare_and_exchange((void**)&self->xfer_s3t, s3t, NULL);

    {
        g_mutex_lock(self->thread_list_mutex);

        if (s3t->errflags || s3t->errmsg || device_in_error(self)) {
            g_warning("[%p] %s: s3-op error result blocks progress ctxt#%ld: key=%s flag=%x mesg=%s", s3buf, __FUNCTION__, s3t - self->s3t,
                        s3t->object_subkey, s3t->errflags, s3t->errmsg);
            goto failed_locked;
        }

        if (!result) {
            // error received upon next write...
            s3t->errflags = DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR;
            s3t->errmsg = g_strdup_printf(_("While writing data block to %s: %s"), S3_name[self->s3_api], s3_strerror(s3t->s3));

            g_debug("[%p] %s: s3-op error result ctxt#%ld: key=%s flag=%x mesg=%s", s3buf, __FUNCTION__, s3t - self->s3t,
                        s3t->object_subkey, s3t->errflags, s3t->errmsg);
            goto failed_locked;
        }

        if (self->fxn_thread_write_complete) 
        {
            s3_result_t result = S3_RESULT_OK;

            result = (*self->fxn_thread_write_complete)(self,s3t, etag);
            if (result == S3_RESULT_OK && self->fxn_thread_write_finish)
                result = (*self->fxn_thread_write_finish)(self,s3t);
            if (result == S3_RESULT_RETRY) 
                result = S3_RESULT_OK;
            if (result != S3_RESULT_OK) 
               goto failed_locked;
        }

        if (self->verbose)
           g_debug("[%p] %s:%d: thread idle-and-completed ctxt#%ld", s3buf, __FUNCTION__, __LINE__, s3t - self->s3t);

        // define as complete
        s3t->done = TRUE;
        g_cond_broadcast(self->thread_list_cond); // notify change in thread
        g_mutex_unlock(self->thread_list_mutex);
    }
    return;

failed_locked:
    {
        // drop through if no lock
        if (s3buf->mutex) {
            g_mutex_lock(s3buf->mutex);
            s3buf->cancel = TRUE; // failed... so do not allow more writes at all!
            g_cond_broadcast(s3buf->cond); // wakeup
            g_mutex_unlock(s3buf->mutex);
        }

        // mark thread as done
        s3t->done = TRUE;
        g_cond_broadcast(self->thread_list_cond); // notify change in thread
        g_mutex_unlock(self->thread_list_mutex);
    }
}



static gboolean
s3_device_finish_file(Device * pself)
{
    S3Device *self = S3_DEVICE(pself);
    // threadsafe write ops only

    __API_FXN_CALLED__(self);  // [never verbose?] 

    // no need if not in file
    if (!pself->in_file)
	return TRUE;

    // no need if no mutex
    if (!self->thread_list_mutex)
	return TRUE;

    // must truncate if we have a partial-write waiting...
    // automatically sets self->end_xbyte = self->next_xbyte;
    s3_cancel_busy_threads(self,TRUE);

    // session thread(s?) should be complete ...
    s3_wait_threads_done(self);

    __API_LONG_FXN_CHECK__(self,"[finish-complete]");

    reset_file_state(self);
    return (pself->status == DEVICE_STATUS_SUCCESS);
}

static gboolean
s3_device_recycle_file(Device *pself, guint file)
{
    S3Device *self = S3_DEVICE(pself);

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (device_in_error(self))
        return FALSE;

    // only one file can be cancelled at a time now
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);
    delete_file(self, file);
    s3_wait_threads_done(self);

    __API_LONG_FXN_CHECK__(self,"[done]");
    return !device_in_error(self);
    /* delete_file already set our error message if necessary */
}

static gboolean
s3_device_erase(Device *pself)
{
    S3Device *self = S3_DEVICE(pself);
    char *key = NULL;
    const char *errmsg = NULL;
    guint response_code;
    s3_error_code_t s3_error_code;
    s3_result_t result;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (!setup_handle(self)) {
        /* error set by setup_handle */
        return FALSE;
    }

    // only one file can be cancelled at a time now
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);

    key = tapestart_key(self); // prefix+"special"+"-tapestart"

    result = s3_delete(get_temp_curl(self), self->bucket, key);
    if (result != S3_RESULT_OK) {
        s3_error(get_temp_curl(self), &errmsg, NULL, NULL, NULL, NULL, NULL);
	device_set_error(pself,
	    g_strdup(errmsg),
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

    // must be free after the delete_all_files above!
    if (self->create_bucket && !s3_delete_bucket(get_temp_curl(self), self->bucket))
    {
        s3_error(get_temp_curl(self), &errmsg, &response_code, &s3_error_code, NULL, NULL, NULL);

        /*
         * ignore the error if the bucket isn't empty (there may be data from elsewhere)
         * or the bucket not existing (already deleted perhaps?)
         */
        switch ( MIX_RESPCODE_AND_S3ERR(response_code,s3_error_code) ) {
            case MIX_RESPCODE_AND_S3ERR(409,S3_ERROR_BucketNotEmpty):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchBucket):
               break;
            default:
                device_set_error(pself,
                    g_strdup(errmsg),
                    DEVICE_STATUS_DEVICE_ERROR);
                return FALSE;
        }
    }
    self->bucket_made = FALSE;
    self->volume_bytes = 0;
    catalog_remove(self);

    if (self->create_bucket)
        __API_LONG_FXN_CHECK__(self,"[bucket-erased]");
    else
        __API_LONG_FXN_CHECK__(self,"[files-erased]");

    return TRUE;
}

static gboolean
s3_device_set_reuse(
    Device *dself)
{
    S3Device *self = S3_DEVICE(dself);
    GSList *lifecycle = NULL, *life;
    lifecycle_rule *rule;
    gboolean removed = FALSE;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (self->transition_to_glacier < 0 && !self->read_from_glacier) {
	return TRUE;
    }

    if (device_in_error(self))
        return dself->status;

    if (!setup_handle(self)) {
        /* setup_handle already set our error message */
	return dself->status;
    }


    // only one file can be cancelled at a time now
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);

    s3_get_lifecycle(get_temp_curl(self), self->bucket, &lifecycle);

    /* remove it if it exist */
    for (life = lifecycle; life != NULL; life = life->next) {
	rule = (lifecycle_rule *)life->data;

	if (g_str_equal(rule->id, dself->volume_label)) {
	    removed = TRUE;
	    lifecycle = g_slist_delete_link(lifecycle, life);
	    free_lifecycle_rule(rule);
	    break;
	}
    }
    if (removed) {
	s3_put_lifecycle(get_temp_curl(self), self->bucket, lifecycle);
    }
    return TRUE;
}

static gboolean
s3_device_set_no_reuse(
    Device *dself,
    char   *label,
    char   *datestamp)
{
    S3Device *self = S3_DEVICE(dself);
    GSList *lifecycle = NULL, *life, *next_life, *prev_life = NULL;
    lifecycle_rule *rule;
    guint count = 0;
    GSList *to_remove = NULL;
    char *lifecycle_datestamp = NULL;
    time_t t;
    struct tm tmp;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (self->transition_to_glacier < 0) {
	return TRUE;
    }

    if (!label || !datestamp) {
	s3_device_read_label(dself);
	label = dself->volume_label;
	datestamp = dself->volume_time;
    }

    if (device_in_error(self)) return dself->status;

    if (!setup_handle(self)) {
        /* setup_handle already set our error message */
	return dself->status;
    }

    // only one file can be cancelled at a time now
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);

    s3_get_lifecycle(get_temp_curl(self), self->bucket, &lifecycle);

    /* remove it if it exist */
    for (life = lifecycle; life != NULL; life = next_life) {
	next_life = life->next;

	rule = (lifecycle_rule *)life->data;

	if (g_str_equal(rule->id, label)) {
	    free_lifecycle_rule(rule);
	    if (prev_life == NULL) {
		lifecycle = next_life;
	    } else {
		prev_life->next = next_life;
	    }
	    //g_free(life);
	} else {
	    if (!to_remove ||
		 strcmp(datestamp, lifecycle_datestamp) < 0) {
		to_remove = life;
		g_free(lifecycle_datestamp);
		lifecycle_datestamp = g_strdup(datestamp);
	    }
	    prev_life = life;
	    count++;
	}
    }

    /* remove a lifecycle */
    if (count >= 999) {
	rule = (lifecycle_rule *)to_remove->data;
	free_lifecycle_rule(rule);
	lifecycle = g_slist_delete_link(lifecycle, to_remove);
    }

    /* add it */
    rule = g_new0(lifecycle_rule, 1);
    rule->id = g_strdup(label);

    // prefix+"fXXXXXXXX-"
    rule->prefix = file_to_prefix(self, 0);
    rule->prefix[strlen(self->prefix)+sizeof("f")-1] = '\0'; // prefix+"f"

    rule->status = g_strdup("Enabled");
    rule->transition = g_new0(lifecycle_action, 1);
    rule->transition->days = 0;
    t = time(NULL) + ((self->transition_to_glacier+1) * 86400);

    if (!gmtime_r(&t, &tmp)) 
        perror("localtime");

    rule->transition->date = g_strdup_printf(
		"%04d-%02d-%02dT00:00:00.000Z",
		1900+tmp.tm_year, tmp.tm_mon+1, tmp.tm_mday);
    if (self->transition_to_class != NULL) {
        rule->transition->storage_class = g_strdup(self->transition_to_class);
    } else {
        rule->transition->storage_class = g_strdup("GLACIER");
    }

    lifecycle = g_slist_append(lifecycle, rule);
    s3_put_lifecycle(get_temp_curl(self), self->bucket, lifecycle);

    __API_LONG_FXN_CHECK__(self,"[lcycle-uploaded]");

    return TRUE;
}

/* functions for reading */

static gboolean
s3_device_get_keys_restored(
    Device *pself,
    guint file)
{
    S3Device *self = S3_DEVICE(pself);
    gboolean result;
    GSList *objects;
    char *prefix;
    const char *errmsg = NULL;
    s3_object *object = NULL;

    if (!self->read_from_glacier)
	return TRUE;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    /* get a list of all objects */
    if (file == 0) {
	prefix = tapestart_key(self); // prefix+"special-tapestart"
    } else {
	prefix = file_to_prefix(self, file); // prefix+"fXXXXXXXX-"
    }
    // all keys
    result = s3_list_keys(get_temp_curl(self), self->bucket, NULL, prefix, NULL, 0,
			  &objects, NULL);
    g_free(prefix);

    if (!result) {
        guint response_code;
        s3_error_code_t s3_error_code;
        s3_error(get_temp_curl(self), &errmsg, &response_code, &s3_error_code,
		 NULL, NULL, NULL);

	device_set_error(pself,
		g_strdup_printf(_("failed to list objects: %s"), errmsg),
		DEVICE_STATUS_SUCCESS);

	return FALSE;
    }

    /* perform s3_init_restore on all non-restored GLACIER objects */
    for( ; objects
            ; (objects=g_slist_remove(objects, objects->data)),
              free_s3_object(object) )
    {
	s3_head_t *head;
	object = (s3_object *)objects->data;

	if (object->storage_class != S3_SC_GLACIER && object->storage_class != S3_SC_DEEP_ARCHIVE)
              continue;

        /* HEAD object */
        head = s3_head(get_temp_curl(self), self->bucket, object->key);
        if (!head) {
            guint response_code;
            s3_error_code_t s3_error_code;
            s3_error(get_temp_curl(self), &errmsg, &response_code,
                     &s3_error_code, NULL, NULL, NULL);

            device_set_error(pself,
                    g_strdup_printf(
                            _("failed to get head of objects '%s': %s"),
                            object->key, errmsg),
                    DEVICE_STATUS_SUCCESS);

            slist_free_full(objects, free_s3_object);
            return FALSE;
        }

        /* skip if it is restored already ..*/
        if (head->x_amz_restore)
            continue;

        free_s3_head(head),

        /* init restore */
        result = s3_init_restore(get_temp_curl(self), self->bucket, object->key);
        if (!result) {
            guint response_code;
            s3_error_code_t s3_error_code;

            s3_error(get_temp_curl(self), &errmsg, &response_code,
                     &s3_error_code, NULL, NULL, NULL);

            device_set_error(pself,
                g_strdup_printf(_("failed to list objects: %s"),
                                 errmsg),
                DEVICE_STATUS_SUCCESS);

            slist_free_full(objects, free_s3_object);
            return FALSE;
        }
    }
    __API_LONG_FXN_CHECK__(self,"[glacier-keys-pulled]");

    return TRUE;
}

static dumpfile_t*
s3_device_seek_file(Device *pself, guint file) 
{
    S3Device *self = S3_DEVICE(pself);
    CurlBuffer buffer_tmp = { 0 }; // max alloc of S3_DEVICE_DEFAULT_BLOCK_SIZE
    CurlBuffer *s3buf_tmp = &buffer_tmp;
    char *headerkey = NULL;
    char *fileprefix = NULL;
    dumpfile_t *amanda_header;
    const char *errmsg = NULL;
    GSList *objects;
    char *buff = NULL;
    xferbytes_t bufflen = 0;
    s3_object *fileobj = NULL;

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (device_in_error(self))
        return NULL;

    // only one file can be cancelled at a time now
    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);

    reset_file_state(self);

    for(;;)
    {
        guint response_code;
        s3_error_code_t s3_error_code;
	gboolean result;

        // arg passed in or directly from find_next_file() ...
        if (file) {
            g_mutex_lock(pself->device_mutex);
            pself->file = file; // successful read sets value
            g_mutex_unlock(pself->device_mutex);
        }

	do {
            // test valid number or just-previous bound if in error...
	    headerkey = filestart_key(self, ( (gint) file ? : pself->file-1));  // prefix+"fXXXXXXXX-filestart"
	    result = s3_device_get_keys_restored(pself, ( (gint) file ? : pself->file-1) );
	    /* read it in */

	    if (!result) break; // failed

	    s3_buffer_init(s3buf_tmp, DISK_BLOCK_BYTES, self->verbose);
	    result = s3_read(get_temp_curl(self), self->bucket, headerkey, 
                            S3_BUFFER_WRITE_FUNCS, s3buf_tmp,
                            NULL, NULL);

	    g_free(headerkey);
	    headerkey = NULL;

	    if (!result) break; // failed

	    // successfully tested actual boundary
 
	    if (!file) {
		g_debug("%s: completing with successful confirmation of %d file limit", __FUNCTION__, pself->file);
		s3_buffer_destroy(s3buf_tmp);
		/* pself->file, etc. are already correct */
                __API_LONG_FXN_CHECK__(self,"[past-limit-seek]");
		return make_tapeend_header();
	    }
	} while(FALSE);
	
	// success .. so escape the loop
	if (result) break; // success!

	////////////////////////////////////// failed so retry

	s3_buffer_destroy(s3buf_tmp);

        if (file <= 0) {
	    g_debug("%s: completing after reading past tape-end %d file limit", __FUNCTION__, pself->file);
            device_set_error(pself,
                g_strdup(_("Attempt to read past tape-end file")),
                DEVICE_STATUS_SUCCESS);
            return NULL;
        }

        // something failed...

        s3_error(get_temp_curl(self), &errmsg, &response_code, &s3_error_code, NULL, NULL, NULL);

        /* if it's an expected error (not found), check what to do. */
        switch ( MIX_RESPCODE_AND_S3ERR(response_code,s3_error_code) ) {

            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_None):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NotFound):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchKey):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchEntity):
                break;
            default:
                /* An unexpected error occured finding out if we are the last file. */
                device_set_error(pself, g_strdup(errmsg), DEVICE_STATUS_DEVICE_ERROR);
		g_debug("%s: completing after failure before find_next_file %d", __FUNCTION__, pself->file);
                return NULL;
        }

        file = find_next_file(self, file);
	if (device_in_error(self)) {
	    g_debug("%s: completing after failure during find_next_file %d", __FUNCTION__, pself->file);
            return NULL; // device_set_error() was already called
	}

 	if (file >= G_MAXINT)
	    file = 0;
        // try again.... (may include failed w/file <= 0)
    } //for(;;)

    /* and make a dumpfile_t out of it */
    g_assert(s3buf_tmp->buffer != NULL);

    amanda_header = g_new(dumpfile_t, 1);
    fh_init(amanda_header);

    buff = s3_buffer_data_func(s3buf_tmp);
    bufflen = s3_buffer_lsize_func(s3buf_tmp);

    parse_file_header(buff, amanda_header, bufflen);

    s3_buffer_destroy(s3buf_tmp);

    switch (amanda_header->type) {
        case F_DUMPFILE:
        case F_CONT_DUMPFILE:
        case F_SPLIT_DUMPFILE:
            break;

        default:
            device_set_error(pself,
                g_strdup(_("Invalid amanda header while reading file header")),
                DEVICE_STATUS_VOLUME_ERROR);
            g_free(amanda_header);
            return NULL;
    }

    // look for multi-part objects
    fileprefix = file_to_prefix(self, pself->file); // prefix+"fXXXXXXXX-"
    // get lowest 2 [sorted] that match.. [maybe *-filestart + *-mp.data]
    s3_list_keys(get_temp_curl(self), self->bucket, NULL, fileprefix, NULL, 2,
          &objects, NULL);
    g_free(fileprefix);

    for (; objects; objects = g_slist_remove(objects, objects->data))
    {
        fileobj = (s3_object *)objects->data;
        if (g_str_has_suffix(fileobj->key, DATA_OBJECT_SUFFIX) && fileobj->size)
            break;

        free_s3_object(fileobj);
        fileobj = NULL;
    }

    // no data keys found at all!
    if (!fileobj || !fileobj->key) {
	g_free(amanda_header);
	slist_free_full(objects, free_s3_object); // just in case...?
        device_set_error(pself, g_strdup(_("EOF")), DEVICE_STATUS_SUCCESS);
        __API_LONG_FXN_CHECK__(self,"[seek-found-nothing]");
        return NULL;
    }

    // found a single-key uploaded file
    if (g_str_has_suffix(fileobj->key, MULTIPART_OBJECT_SUFFIX)) {
        // keep end_xbyte as found below
	self->dev_blksize = min(self->dev_blksize,fileobj->size); // single block or buffer-tuned block
	self->end_xbyte = fileobj->size; // set size.. and S3CreateObjkey will re-use it
    } else {
        // set end_xbyte for end of each block
	self->dev_blksize = fileobj->size; // set correct fixed size
	self->end_xbyte = G_MAXINT64; // ideally infinite
    }

    self->fxn_thread_write_factory = NULL;
    self->fxn_thread_write_complete = NULL;

    // leave read block size fixed ....

    slist_free_full(objects, free_s3_object);

    {
        g_mutex_lock(pself->device_mutex);
        pself->in_file = TRUE; // prevent reads from device code
        g_mutex_unlock(pself->device_mutex);
    }

    // setup fxn pointers for read-in-file
    init_block_xfer_hooks(self);

    __API_LONG_FXN_CHECK__(self,"[seek-succeeded]");
    return amanda_header;
}

static gboolean
s3_device_seek_block(Device *pself, objbytes_t block) {
    S3Device * self = S3_DEVICE(pself);
    objbytes_t pos = block * pself->block_size; // start with expectation of user

    __API_FXN_CALLED__(self);  // [never verbose?] 

    if (device_in_error(self))
        return FALSE;
   
    // looking for block-sized objects?  use real block-division
    if ( self->fxn_create_curr_objkey == &file_and_block_to_key) {
    	if (self->dev_blksize && self->dev_blksize < G_MAXINT64)
	    pos = block * self->dev_blksize; // use actual discovered block size
    }

    if (self->end_xbyte < pos)
        return FALSE; // cannot seek past the one object

    s3_cancel_busy_threads(self,TRUE);

    s3_wait_threads_done(self);

    self->next_xbyte = pos;              // set to location for new reads to start
    self->next_ahead_byte = pos;

    // only used to show location was reached
    pself->block = block;

    // MUST use pself->block_size as default object_size
    __API_LONG_FXN_CHECK__(self,"[block-pos-set]");
    return TRUE;
}

// NOTE: called with thread_list_mutex held
static int
start_read_ahead(Device * pself, int max_reads)
{
    S3Device * self = S3_DEVICE(pself);
    S3_by_thread *s3t = NULL;
    S3_by_thread *s3t_begin = self->s3t;
    S3_by_thread *s3t_end_recovery = s3t_begin + self->nb_threads_recovery;
    int busy_reads = 0;

    // limit or not...
    char *objkey = NULL;
    int reads_started = 0;

    if (max_reads <= 0)
        goto cleanup;

    // still not the best alloc size
    // guint allocate = ( self->use_chunked ? size_req*2 + 1 : size_req );

    // limit total reads started
    do
    {
        objbytes_t ulnow = 0ULL;
        objbytes_t dlnow = 0ULL;

        busy_reads = 0;

        for (s3t = s3t_begin ; s3t != s3t_end_recovery ; s3t++)
        {
            if (!s3t->dlnow) continue;  // thread is an old upload?
            if (s3t->done) continue;	// thread is not active
            if (!s3t->ahead) continue;  // thread is in-use by API read?
            // download-ready && active && unreached-by-API call
            ++busy_reads;
        }

        // prevent re-retrieving the same block if it was pre-emptively read
	g_free(objkey);
        objkey = NULL;

        s3t = find_active_file_thread(self, pself->file, self->next_ahead_byte, &objkey);

        if (s3t) {
            ulnow = (objbytes_t) g_atomic_pointer_get(&s3t->ulnow);
            dlnow = (objbytes_t) g_atomic_pointer_get(&s3t->dlnow);

            if (s3t && !s3t->ahead) {
                g_debug("[%p] %s: FOUND IN-USE thread s3t#%ld and pos=@%#lx n=%#lx/%#lx key=%s", 
                     &s3t->curl_buffer, __FUNCTION__,
                     s3t - s3t_begin, s3t->xfer_begin, dlnow, ulnow, s3t->object_subkey);
            } else {
                g_debug("[%p] %s: FOUND thread s3t#%ld and pos=@%#lx n=%#lx/%#lx key=%s", 
                     &s3t->curl_buffer, __FUNCTION__,
                     s3t - s3t_begin, s3t->xfer_begin, dlnow, ulnow, s3t->object_subkey);
            }

            // if READ-POSITION is matching... jump to start/find next valid thread
            if (s3t->xfer_begin == self->next_ahead_byte) {
                self->next_ahead_byte = s3t->xfer_end; // advance to end...
                continue; // match and plan as "read-ahead"
            }
        }
	
	// drop through... because thread did not match or thread was not found

	////////////////////////////////////////////////////////////////
	///// new thread needed.....
	////////////////////////////////////////////////////////////////

        if (self->next_ahead_byte >= self->end_xbyte)
            break; // no more

        g_assert(!s3t || !objkey);

        // thread found ... but multipart was not a match
	if (s3t) {
	    objkey = g_strdup(s3t->object_subkey);
            s3t = NULL;
	}

	if (self->verbose)
	   g_debug("%s: start download-thread: ahead=[%#lx+%#lx) key=%s",__FUNCTION__,
		    self->next_ahead_byte, 
                    min(self->next_ahead_byte + self->buff_blksize, self->end_xbyte), objkey);

        s3t = thread_read_base_factory(self, objkey, self->next_ahead_byte);
	objkey = NULL; // now owned by s3t

        // EOD set (for return-sync readers) before thread is spawned
        g_assert(s3t && s3t->object_subkey != NULL);

        // key/settings define the new thread
        g_thread_pool_push(self->thread_pool_read, s3t, NULL);

        s3t = NULL; // dont wake waiters *here* if thread has failed

        ++busy_reads; // account for new one
        ++reads_started;

    // some must be busy to stop!
    } while( busy_reads && busy_reads < max_reads && reads_started < max_reads );

    if (self->verbose)
        g_debug("%s: created %d/%d new threads: not-trying file#%d nextpos=@%#lx key=%s",
             __FUNCTION__, reads_started, busy_reads, pself->file, self->next_ahead_byte, objkey);

cleanup:
    if (device_in_error(self) && s3t && !s3t->done) {
        s3t->done = TRUE;
        g_cond_broadcast(self->thread_list_cond); // notify new idle thread present..
    }
    g_free(objkey);
    return reads_started; // return amount created!
}


// NOTE: return 0 with *size_req increased, if *size_req is too small to return
// NOTE: return -1 for error or EOF
// (assumes max_block must be >= 1 to imply a limit
static int
s3_device_read_block(Device *pself, gpointer dataout, int *size_req, int max_block)
{
    S3Device *self = S3_DEVICE(pself);
    char *newkey = NULL;
    S3_by_thread *s3t = NULL;
    char *errmsg = NULL;
    DeviceStatusFlags errflags = DEVICE_STATUS_SUCCESS;
    int bytes_desired = *size_req;
    int n;
    int r = -1;

    g_assert(self->dev_blksize);

    __API_FXN_CALLED__(self);  // [never verbose?] 

    // need only set a reasonably large buffer size.. is all
    if (*size_req < S3_DEVICE_DEFAULT_BLOCK_SIZE) {
        // all chunked-reads must be big enough to be useful
        *size_req = max(S3_DEVICE_DEFAULT_BLOCK_SIZE, self->dev_blksize);
        return 0;
    }

    // repeat until eof or until size is zero
    do
    {
        // can read from incomplete thread...
        g_free(newkey);
        newkey = NULL;

        // device_set_error() was called already...
        if (device_in_error(self)) {
            g_warning("%s: ERROR: transfer ended with device error file#%d and pos=@%#lx",__FUNCTION__,
                       pself->file, self->next_xbyte );
            return -1;
        }

        ///////////////////////////////// search under lock ////////////////
        {
            g_mutex_lock(self->thread_list_mutex);
            // try a short-circuit (avoid deadlock too)
            s3t = find_active_file_thread(self, pself->file, self->next_xbyte, &newkey);

            // new threads are free?
            if (!s3t)
            {
                int max_block_now = ( max_block<0 ? self->nb_threads_recovery : max_block );

                // stop looking already!
                if (self->next_xbyte >= self->end_xbyte) {
                    g_mutex_unlock(self->thread_list_mutex);
                    goto eof;
                }

                // need at least one more thread...
                n = start_read_ahead(pself, max_block_now);
		if (device_in_error(self)) {
                    g_warning("%s: ERROR: transfer ended with device error file#%d and pos=@%#lx",__FUNCTION__,
                               pself->file, self->next_xbyte );
                    g_mutex_unlock(self->thread_list_mutex);
		    return -1;
                }

		g_free(newkey);
		newkey = NULL;
		s3t = find_active_file_thread(self, pself->file, self->next_xbyte, &newkey);
            }

            // claim thread as essential if found
            if (s3t)
                s3t->ahead = FALSE; // thread is in-use now

            g_mutex_unlock(self->thread_list_mutex);
        }
        ///////////////////////////////// search under lock ////////////////

        // device_set_error() was called already...
        if (device_in_error(self)) {
            g_warning("%s: ERROR: transfer ended with device error file#%d and xfer_begin=@%#lx pos=@%#lx",__FUNCTION__,
                       pself->file, s3t->xfer_begin, self->next_xbyte );
            return -1; // error during read_ahead
        }

        if (!s3t) {
            if (self->verbose)
               g_debug("%s: EOF: no reading thread read-ahead=%d file#%d and pos=@%#lx and key=%s",__FUNCTION__,
                     n, pself->file, self->next_xbyte, newkey);
            goto eof;
        }

        // s3t is busy reading...
        // located a valid/usable thread for the correct segment/block ... (newkey == NULL)

        // in case changed too far
        g_assert( s3t->xfer_begin <= s3t->xfer_end );

        // byte range [if any] must match exactly at start..
        if (s3t->xfer_begin != self->next_xbyte) {
            errflags = DEVICE_STATUS_DEVICE_ERROR;
            errmsg = g_strdup_printf(_("EOF: xfer_begin mismatch: xfer_begin=@%#lx pos@%#lx with %s"),
                       s3t->xfer_begin, self->next_xbyte, s3t->object_subkey);
            g_warning("%s: EOF: xfer_begin mismatch file#%d and xfer_begin=@%#lx pos=@%#lx",__FUNCTION__,
                       pself->file, s3t->xfer_begin, self->next_xbyte );
            goto eof;
        }

        // session (earlier) failed?
        if (s3t->errflags != DEVICE_STATUS_SUCCESS) {
            /* return the error */
            errmsg = s3t->errmsg;
            errflags = s3t->errflags;
            s3t->errmsg = NULL;
            s3t->errflags = DEVICE_STATUS_SUCCESS;

            g_debug("%s: ERR: errflag on read ctxt#%ld [%p]: tocopy=%#x key=%s",__FUNCTION__, s3t - self->s3t,
               &s3t->curl_buffer, bytes_desired, ( s3t->object_subkey ? : "<null>" ));

            goto cleanup;
        }

        ////////////////////////////////////////////////////////////////////

        // found a valid/reading thread-context
        {
            const int myind = s3t - self->s3t;
            // S3Handle *hndl = s3t->s3;
            CurlBuffer *s3buf = &s3t->curl_buffer;
            int toread = ( s3t->xfer_end - s3t->xfer_begin );
            int nread;

            if (self->verbose)
                g_debug("%s: read-thread is found ctxt#%d [%p]: [%#lx-%#lx) tocopy=%#x/%#x file-key=%s",__FUNCTION__, myind,
                      s3buf, s3t->xfer_begin, s3t->xfer_end, min(toread,bytes_desired), bytes_desired, s3t->object_subkey);

            ///////////////////////////////////////////

            // perform maximal read possible into buffer
            nread = s3_buffer_read_func(dataout, min(toread,bytes_desired), 1, s3buf);

            // error?  ignore if number of bytes could not go this high
            if ( (bytes_desired >= CURL_READFUNC_ABORT && nread == 0) || 
                 (bytes_desired < CURL_READFUNC_ABORT && nread == CURL_READFUNC_ABORT) ) 
             {
	    	// quit because data was read-blocked/read-cancelled?
	    	 if ( (!s3t->at_final && s3_buffer_read_size_func(s3buf)) || (!s3t->at_final && s3_buffer_size_func(s3buf)) ) 
		 {
		    // quit because data to read was halted...
		     errflags = DEVICE_STATUS_DEVICE_ERROR;
		     errmsg = g_strdup_printf(_("[%p] %s: deadlock in copy buffer: ctxt#%d : tocopy=%#x key=%s"),
		 	     s3buf, __FUNCTION__, myind, bytes_desired, s3t->object_subkey);
		     goto cleanup;
		 }

                 g_debug("%s: [%p] at_final-read-block ctxt#%ld [%#lx-%#lx)", __FUNCTION__, s3buf, 
                          s3t - self->s3t, s3t->xfer_begin, s3t->xfer_end);
                 // nothing was left in buffer ... so read-EOF was reached
		 s3t->at_final = TRUE;
		 nread = 0;
            }

            dataout += nread;
            bytes_desired -= nread;
            // NOTE: top thread only must change this
            s3t->xfer_begin += nread; 
            r = *size_req - bytes_desired;

	    // atomic-update dlnow to correct value ...
            if ( ! g_atomic_pointer_compare_and_exchange(&s3t->dlnow,GSIZE_TO_POINTER(1), GSIZE_TO_POINTER(nread)) ) {
                g_atomic_pointer_add(&s3t->dlnow, nread);
            }

            { /// detect eof flag if flipped on
                g_mutex_lock(self->thread_list_mutex);

		//////////////////////// MUTEX on self fields...
                self->dltotal += nread;
                // ignore self->volume_bytes for read
                self->next_xbyte += nread; // shift to new block-key if needed

                // must start a limit on any later read_aheads...
                if (s3t->at_final) 
                    self->end_xbyte = min(self->end_xbyte, self->next_xbyte);

                g_mutex_unlock(self->thread_list_mutex);
            }

            // empty of any more read bytes... send out notice that read from thread is done
            if ( ! s3_buffer_read_size_func(s3buf) || s3t->at_final )
            {
		// NOTE: buffer-read is complete -->
		//       buffer-write must be complete -->
		//       s3_read() MIGHT be ended -->
		//       session thread MIGHT be waiting to reset and exit
		if (s3buf->mutex && s3buf->cond) {
		    g_mutex_lock(s3buf->mutex); // ensure only cond_wait segments get this...
		    g_cond_broadcast(s3buf->cond); // notify buffer is freed ...

		    if (self->verbose)
			g_debug("%s: read is completing ctxt#%d [%p]: tocopy=%#x key=%s",__FUNCTION__, myind,
			      s3buf, bytes_desired, ( s3t->object_subkey ? : "<null>" ));
		    g_mutex_unlock(s3buf->mutex);
		}

                // handle no-waiting case... if present
		if (!s3buf->mutex || !s3buf->cond) {
		    if (self->verbose)
			g_debug("%s: read is completing [NO-WAKEUP] ctxt#%d [%p]: tocopy=%#x key=%s",__FUNCTION__, myind,
			      s3buf, bytes_desired, ( s3t->object_subkey ? : "<null>" ));
		}

		// did the session for this block not get any data?
		if (s3t->at_final) {
		    if (self->verbose)
		       g_debug("DONE/EOF: s3t-complete settings file#%d and next_xbyte=@%#lx and key=%s",
			       pself->file, self->next_xbyte, s3t->object_subkey);
		    goto eof;
		}

		continue;  // complete in another thread .. or read from new thread context
            }

            // bytes left before empty??  ---> read thread is busy or waiting

            if (bytes_desired)
                g_debug("%s: continue after read ctxt#%d last-nread=%#x [%p]: tocopy=%#x key=%s",__FUNCTION__, myind,
                      nread, s3buf, bytes_desired, ( s3t->object_subkey ? : "<null>" ));

            // buffer may not not be filled yet... so continue
        }
    } while(bytes_desired);

    if (self->verbose) 
        g_debug("%s: s3t-complete file#%d next_xbyte=@%#lx r=[%#x/%#x]/%#x",__FUNCTION__,
               pself->file, self->next_xbyte, r, bytes_desired, *size_req);

    ++pself->block; // total read_block calls succeeded 

    // no eof signal found... so allow more calls
    __API_LONG_FXN_CHECK__(self,"[block-read]");
    return r;

eof:
    // received actual zero bytes?
    pself->is_eof = TRUE;
    pself->in_file = FALSE;
    device_set_error(pself, g_strdup(_("EOF")), DEVICE_STATUS_SUCCESS);

    // drop through...

cleanup:
    if (self->verbose)
       g_debug("%s: eof/error s3t-complete file#%d next_xbyte=@%#lx r=[%#x/%#x]/%#x",__FUNCTION__,
               pself->file, self->next_xbyte, r, bytes_desired, *size_req);

    // empty... send out notice that thread is done
    if (s3t)
    {
        g_mutex_lock(self->thread_list_mutex);
        // NOTE: buffer-read is complete -->
        //       buffer-write must be complete -->
        //       s3_read() must be complete -->
        //       session thread is done

        // reset_idle_thread(s3t, self->verbose); // first read/write sets the buffer size
        s3t->curl_buffer.cancel = TRUE;  // abnormal end operation
        s3t->ahead = TRUE;               // abnormal end operation
        s3t->done = TRUE;                // abnormal end operation

        g_cond_broadcast(self->thread_list_cond); // notify new done thread present..
        g_mutex_unlock(self->thread_list_mutex);
    }
    g_free(newkey);

    if (errflags != DEVICE_STATUS_SUCCESS)
        device_set_error(pself, errmsg, errflags);
    __API_LONG_FXN_CHECK__(self,"[block-read-incomplete]");
    return r;
}


static DeviceWriteResult
s3_device_write_block(Device * pself, guint bytes_ready, gpointer data)
{
    S3Device *self = S3_DEVICE(pself);
    S3_by_thread *s3t = NULL;
    char *nxtkey = NULL;
    objbytes_t nwritten = 0;
    char *errmsg = NULL;
    DeviceStatusFlags errflags = DEVICE_STATUS_SUCCESS;

    __API_FXN_CALLED__(self);

    // NOTE: only one thread (no write_ahead yet) can be for the next write

    // create more threads while bytes_ready is unsatisfied... but always try once!
    do // do while (bytes_ready)
    {
        // get out? ...
        if (device_in_error(self)) {
            __API_LONG_FXN_CHECK__(self,"[device-error-end]");
            return WRITE_FAILED;
        }

        // flag to cancel/split if enabled
        if(check_at_leom(self, bytes_ready))
            pself->is_eom = TRUE;

        // flag to cancel/split with error now
        if(check_at_peom(self, bytes_ready)) {   
            pself->is_eom = TRUE;
            errmsg = g_strdup(_("No space left on device"));
            errflags = DEVICE_STATUS_DEVICE_ERROR;
            goto error;                                         ////////// RETURN WRITE_FULL
        }

        //////// thread is needed to read >1 bytes data from buffer
        { // lock scope
            g_mutex_lock(self->thread_list_mutex);

            // do while(FALSE)... if-chain
            do 
            {
                // find a key with full match to current-xfer-byte now...
		g_free(nxtkey);
                nxtkey = NULL;
                s3t = find_active_file_thread(self, pself->file, self->next_xbyte, &nxtkey);

                // chunked writes cause increment when re-found
                if (s3t && self->use_chunked)
                    ++pself->block; // total write_block calls.. needs to be set early

                // accept a partly-written one [MUST have been the same!]
                // XXX: much safer if xfer_begin could track with write...
                if (s3t)
                    break;

                ////////////////////////////////////////////////////////////////
                ///// new thread needed.....
                ////////////////////////////////////////////////////////////////

                s3t = (*self->fxn_thread_write_factory)(self, nxtkey, self->next_xbyte);
                if (!s3t)
                    goto threadlock_error;

                // calls s3_thread_write_session next... starting the curl operation
                g_thread_pool_push(self->thread_pool_write, s3t, NULL); // start write of a single block

                if (self->verbose)
                   g_debug("%s: spawned %s-thread ctxt#%ld [%p]: write=[%#lx+%#lx) towrite=%#lx key=%s",__FUNCTION__,
                            ( self->mp_next_uploadNum ? "mp" : "block" ), s3t - self->s3t,
                           &s3t->curl_buffer, s3t->xfer_begin, s3t->xfer_end, bytes_ready - (s3t->xfer_end - s3t->xfer_begin) , s3t->object_subkey);

                //////// new thread set with rdbytes_to_eod from wrblksize

            } while(FALSE); // do-while single-thru

            s3t->ahead = FALSE; // write cannot be truncated by cancel now

            g_mutex_unlock(self->thread_list_mutex);
        } // lock scope

        // if applies only once, when bytes_ready==0 as passed argument
        if (!bytes_ready)
            break;

        ////////////////////////
        // XXX: Probably best to wait for the "100 Continue" or at least a read
        //      read from the s3_upload before proceeding
        //      onwards (i.e. not an error)
        ////////////////////////

        // must be bytes left to write if we reach here...
        g_assert(s3t && (self->next_xbyte < s3t->xfer_end));

        {
            objbytes_t wrstop = min(self->next_xbyte + bytes_ready, s3t->xfer_end);

            wrstop -= self->next_xbyte;

            // perform write of bytes into write_session
            nwritten = s3_buffer_write_func(data, wrstop, 1, &s3t->curl_buffer);
            // NOTE: cannot change xfer_begin, to identify start of range
        }

        // if unsigned 'negative' ... something went wrong...
        if ( (signed_xferbytes_t) nwritten < 0 || ! nwritten ) {
            errflags = DEVICE_STATUS_DEVICE_ERROR;
            errmsg = g_strdup_printf(_("[%p] %s: deadlock in copy buffer: ctxt#%ld : left=%#x pos=%#lx"),
                         &s3t->curl_buffer, __FUNCTION__, s3t - self->s3t, bytes_ready, self->next_xbyte);
            goto error;
        }

        data += nwritten;
        bytes_ready -= nwritten;

        if ( ! g_atomic_pointer_compare_and_exchange(&s3t->ulnow, GSIZE_TO_POINTER(1), GSIZE_TO_POINTER(nwritten)) ) {
            g_atomic_pointer_add(&s3t->ulnow, nwritten);
        }

        {
            g_mutex_lock(self->thread_list_mutex);

            self->ultotal += nwritten;
            self->volume_bytes += nwritten; // add new data bytes to total
            self->next_xbyte += nwritten; // shift to new block-key as needed

            if (self->verbose)
               g_debug("%s: buffered-data ctxt#%ld [%p]: xfer=[%#lx,%#lx) left=%#x key=%s",__FUNCTION__, s3t - self->s3t,
                     &s3t->curl_buffer, self->next_xbyte - nwritten, self->next_xbyte, bytes_ready, ( s3t->object_subkey ? : "<null>" ));
		     
            // DONT allow "flush" to truncate writes
            s3t->ahead = FALSE;
            g_mutex_unlock(self->thread_list_mutex);
        }

        if ( s3t->errflags != DEVICE_STATUS_SUCCESS ) {
            // handle an idle thread in error.. so reset it after a message
            errmsg = s3t->errmsg;
            errflags = s3t->errflags;

            s3t->errmsg = NULL;
            s3t->errflags = DEVICE_STATUS_SUCCESS;
            goto error;                           ////////// GOTO ERROR
        }
    // repeat if block-writes short with no errors ...
    } while (bytes_ready);

    // successfully written bytes to thread...
    if (!self->use_chunked && (signed_xferbytes_t) nwritten > 0)
        ++pself->block; // total write_block calls.. needs to be set after success

    // thread is in background still writing...
    g_free(nxtkey); 
    __API_LONG_FXN_CHECK__(self,"[block-written]");
    return WRITE_SUCCEED;

 threadlock_error:
    // free up context as needed
    if (s3t && !s3t->done) {
        s3t->done = TRUE;
	g_cond_broadcast(self->thread_list_cond); // announce change of thread state...
    }
    g_mutex_unlock(self->thread_list_mutex);
    // fall through...
 error:
    if (errmsg)
        device_set_error(pself, errmsg, errflags);
    g_free(nxtkey); 
    __API_LONG_FXN_CHECK__(self,"[block-write-incomplete]");
    return WRITE_FAILED;
}

//
// main s3/curl thread is started here
//    - spawned according to newly-needed block#
//    - main task is to create locally-started writes into circular-buffer
//    - calling/creating thread then reads what it needs to fulfill read
//
static void
s3_thread_read_session(gpointer thread_data, gpointer data)
{
    S3_by_thread *s3t = (S3_by_thread *)thread_data;
    CurlBuffer *const s3buf = &s3t->curl_buffer;
    const Device *pself = (Device *)data;
    const S3Device *self = S3_DEVICE_CONST(pself);

    const int myind = s3t - self->s3t;
    S3Handle *hndl = s3t->s3;
    gboolean result;
    char *mykey_tmp = s3t->object_subkey ? strdupa(s3t->object_subkey) : "";
    objbytes_t objsize = G_MAXINT64;

    // was cancelled before started...
    if (!mykey_tmp[0] || s3buf->cancel) {
        if (self->verbose)
            g_debug("%s: pre-cancelled [unused] read-session ctxt#%d [%p]: [%#lx-%#lx) key=<erased> ",
               __FUNCTION__, myind, s3buf, s3t->xfer_begin, s3t->xfer_end);
        goto cancel;
    }

    {
        char *tname = NULL;
        char *keysuff = strrchr(mykey_tmp,'.');

        if ( self->fxn_create_curr_objkey == &file_and_block_to_key)
            tname = g_strdup_printf("rb%x:%c%c.%02lx", pself->file, keysuff[-2],keysuff[-1], 
                                   ( s3t->xfer_begin - s3t->object_offset ) / self->dev_blksize);

        if ( self->fxn_create_curr_objkey == &get_read_file_objkey_offset )
            tname = g_strdup_printf("rp%x.%02lx", pself->file, 1 + ( s3t->xfer_begin / self->dev_blksize) );

        (void) pthread_setname_np(pthread_self(),tname);
        g_debug("[%p] download worker thread ctxt#%ld [%s] [%#lx-%#lx)",s3buf,s3t - self->s3t,
              mykey_tmp, s3t->xfer_begin, s3t->xfer_end);
        g_free(tname);
    }

    // NOTE: s3_read / s3_buffer_write_func must block
    //    as external "buffer-reads" pull the data out

    // use all the time... as blocks may have strange lengths
    {
        const objbytes_t rmin = s3t->xfer_begin - s3t->object_offset;
        const objbytes_t rmax = s3t->xfer_end - s3t->object_offset -1;

	result = s3_read_range(hndl, self->bucket, mykey_tmp, rmin, rmax,
			       S3_BUFFER_WRITE_FUNCS, s3buf,
                               s3_thread_progress_func, s3t,
                               &objsize);
    }

    // cleared if it was owned
    g_atomic_pointer_compare_and_exchange((void**)&self->xfer_s3t, s3t, NULL);

    // use the objsize found if non-empty
    if ( s3t->xfer_end == self->end_xbyte && s3t->xfer_end >= s3t->object_offset + objsize )
    {
        g_mutex_lock(self->thread_list_mutex); // block reset of s3t until done...

        s3t->xfer_end = s3t->object_offset + objsize; 
        s3t->at_final = TRUE; // had to truncate.. so object-read-end was reached
        *(objbytes_t*)&self->end_xbyte = s3t->xfer_end;

        g_debug("%s: read includes object end s3t%d [%p]: [%#lx-%#lx) key=%s",
                 __FUNCTION__, myind, s3buf, s3t->xfer_begin, s3t->xfer_end, mykey_tmp);
        g_mutex_unlock(self->thread_list_mutex); // block reset of s3t until done...
    }

    if (!result)
	goto cancel;

    if (s3buf->mutex && s3buf->cond && !s3t->curl_buffer.cancel)
    {
	///////// MUTEX on buffer for error or draining-read-buffer
	g_mutex_lock(s3buf->mutex);

        if (s3_buffer_read_size_func(s3buf) && self->verbose)
            g_debug("[%p] %s WAITING on main thread reader ctxt#%d: [%#lx-%#lx) key=%s",
                  s3buf, __FUNCTION__, myind, s3t->xfer_begin, s3t->xfer_end, mykey_tmp);

        s3buf->waiting = 2 + FALSE;  // waiting as download-side
           
	// block and cond-wait until buffer-read is fully complete
	while ( s3_buffer_read_size_func(s3buf) && !s3t->curl_buffer.cancel ) {
	    g_cond_wait_until(s3buf->cond, s3buf->mutex, g_get_monotonic_time () + 5 * G_TIME_SPAN_SECOND);
	}

        if (s3buf->waiting == 2 + FALSE) 
           s3buf->waiting = FALSE; // clear downloader flag

        if (s3buf->mutex)
            g_mutex_unlock(s3buf->mutex);
    }

    // simply handle a wakeup-and-cancel reader cleanup
    {
        g_mutex_lock(self->thread_list_mutex); // block reset of s3t until done...
	// give thread back now as reset-and-ready
	// reset_idle_thread(s3t, self->verbose); // first read/write sets the buffer size
        s3t->curl_buffer.cancel = TRUE;  // done with operation
        s3t->done = TRUE;                // done with operation
        s3t->ahead = TRUE;               // done with operation
        
	g_cond_broadcast(self->thread_list_cond); // announce change of thread state...

        if (self->verbose)
            g_debug("%s: completing read-session ctxt#%d [%p]: [%#lx-%#lx) key=%s ",
                 __FUNCTION__, myind, s3buf, s3t->xfer_begin, s3t->xfer_end, mykey_tmp);

        g_mutex_unlock(self->thread_list_mutex); // block reset of s3t until done...

        // don't touch object again!
    }

    return;

 cancel:
    //
    // FAILED S3_READ CALL.
    //
    {
        guint response_code;
        s3_error_code_t s3_error_code;

        g_mutex_lock(self->thread_list_mutex); // block reset of s3t until done...

        s3_error(hndl, NULL, &response_code, &s3_error_code, NULL, NULL, NULL);
        if (response_code - response_code % 100 == 200 && s3_error_code == S3_ERROR_None) {
           g_debug("%s: completing read-session with \"result\" failure ctxt#%d [%p]: [%#lx-%#lx) key=%s ",
                 __FUNCTION__, myind, s3buf, s3t->xfer_begin, s3t->xfer_end, mykey_tmp);
           g_mutex_unlock(self->thread_list_mutex);
           return;
        }

        /* if it's an expected error (not found), just return -1 */
        switch ( MIX_RESPCODE_AND_S3ERR(response_code,s3_error_code) )
        {
            case MIX_RESPCODE_AND_S3ERR(0,S3_ERROR_None): // cancelled read has no effect
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_None):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NotFound):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_Unknown):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchKey):
            case MIX_RESPCODE_AND_S3ERR(404,S3_ERROR_NoSuchEntity):
            case MIX_RESPCODE_AND_S3ERR(416,S3_ERROR_InvalidRange):
                 break; // creates EOF condition [maybe after partial-read]
            default:
                /* otherwise, log it and fail the WRITE later */
                s3t->errflags = DEVICE_STATUS_VOLUME_ERROR;
                s3t->errmsg = g_strdup_printf(_("While reading data block from S3: %s"),
                                              s3_strerror(hndl));
		g_debug("[%p] %s FAILURE ctxt#%d: [%#lx-%#lx) key=%s",
		      s3buf, __FUNCTION__, myind, s3t->xfer_begin, s3t->xfer_end, mykey_tmp);
		break;
        }

        // unblock read on buffer and detect eod condition
        if (s3buf->mutex && s3buf->cond) // XXX: avoid grabbing two locks at once!
        {
            g_mutex_lock(s3buf->mutex);
            s3_buffer_reset_eod_func(s3buf,0); // auto eod instantly...
            s3buf->cancel = TRUE;      // block all new copy loops
            g_cond_broadcast(s3buf->cond);
            g_mutex_unlock(s3buf->mutex);
        }

        // NOTE: context is *not* reset automatically.. but the write needs interrupting

        g_debug("%s: [r=%3d,err=%d] at_final completing read ctxt#%d [%p]: [%#lx-%#lx) key=%s",
                 __FUNCTION__, response_code, s3_error_code, myind, s3buf, s3t->xfer_begin, s3t->xfer_end, mykey_tmp);

        s3t->at_final = TRUE; // hold the EOF state
        s3t->done = TRUE; // allow resetting when wait_threads_done is called

        // NOTE: messing in-thread with the self->end_xbyte .. 
        //       to *immediately* stop more read-aheads from spawning.
        ((S3Device*)self)->end_xbyte = min(self->end_xbyte, s3t->xfer_begin); 

        // NOTE: allow reset of thread-ctxt now
        g_cond_broadcast(self->thread_list_cond); // notify new idle thread ...
        g_mutex_unlock(self->thread_list_mutex);
    }
}

static gboolean
check_at_peom(S3Device *self, objbytes_t size)
{
    if ( !self->enforce_volume_limit) return FALSE;
    if ( !self->volume_limit ) return FALSE;

    return (self->volume_bytes + size > self->volume_limit);
}

static gboolean
check_at_leom(S3Device *self, objbytes_t size)
{
    objbytes_t eom_warn;

    // disabled?
    if (!self->leom) return FALSE;
    
    eom_warn = EOM_EARLY_WARNING_ZONE_BLOCKS;  // advance warning by small amount
    eom_warn += self->nb_threads;              // and each thread may deliver new blocks

    eom_warn *= DEVICE(self)->block_size; // change to bytes

    // return TRUE if enabled AND past limit...
    return check_at_peom(self, size + eom_warn);
}


static void
s3_wait_threads_done(
    const S3Device *self)
{
    Device *pself = DEVICE(self);
    S3_by_thread *s3t_begin = self->s3t;
    S3_by_thread *s3t_end = s3t_begin + self->nb_threads;
    S3_by_thread *s3t;

    if (!self->thread_list_mutex)
        return;
    if (!s3t_begin || !self->nb_threads)
        return;

    // complete when all done
    g_mutex_lock(self->thread_list_mutex);
    for (s3t = s3t_begin ; s3t != s3t_end ; s3t++)  {

        /* Check if the thread is in error */
        if (s3t->errflags != DEVICE_STATUS_SUCCESS) {
            device_set_error(pself, (char *)s3t->errmsg, s3t->errflags);
            s3t->errflags = DEVICE_STATUS_SUCCESS;
            s3t->errmsg = NULL;
        }
            
        // find non-done threads
        if ( s3t->done )
            continue;

        // non-done thread was found.. so wait in another scan

        // speculative thread has not been read-from / written-to yet?
        g_cond_wait(self->thread_list_cond, self->thread_list_mutex);

        // recheck from start...
        s3t = s3t_begin;
        s3t--;
    }

    // clean up all thread contexts [all are done]
    for (s3t = s3t_begin; s3t != s3t_end; s3t++) {
        // *any* upload/download buffers must be "at_final"
        s3t->at_final = TRUE; 
        // no new bytes should be counted.  
        // FIXME: should they be tallied?
        s3t->ulnow = 0;
        s3t->dlnow = 0;
    }
    g_mutex_unlock(self->thread_list_mutex);
}


static void
s3_cancel_busy_threads(S3Device *self, gboolean end_all_xfers)
{
    Device *pself = DEVICE(self);
    S3_by_thread *s3t = NULL;
    S3_by_thread *s3t_begin = self->s3t;
    S3_by_thread *s3t_end = s3t_begin + self->nb_threads;

    if (!self->thread_list_mutex)
        return;
    if (!s3t_begin || !self->nb_threads)
        return;

    g_mutex_lock(self->thread_list_mutex);

    // clear *any* global state that creates new multipart threads
    // [but do NOT remove file_objkey!]
    // HALT ADDING ANY MULTIPART UPLOADED PARTS
    {
        g_free(self->mp_uploadId);
        self->mp_uploadId = NULL;
        self->mp_next_uploadNum = 0;
    }

    if (end_all_xfers && self->next_xbyte < self->end_xbyte)
        self->end_xbyte = self->next_ahead_byte; // end_xbyte becomes smaller or non-infinite

    // locate thread context that S3Device is busy inside...
    for(s3t = s3t_begin; s3t != s3t_end ; ++s3t)
    {
        CurlBuffer *s3buf = &s3t->curl_buffer;
        const objbytes_t ulnow = (objbytes_t) g_atomic_pointer_get(&s3t->ulnow);
        const objbytes_t dlnow = (objbytes_t) g_atomic_pointer_get(&s3t->dlnow);
	gboolean read_cancel = ( pself->access_mode == ACCESS_READ );            // API-device_start for downloads
	gboolean write_cancel = IS_WRITABLE_ACCESS_MODE(pself->access_mode); // API-device_start for uploads

        if (s3t->done)
            continue; // done already
        if (!s3buf->mutex)
            continue; // [no thread is involved]

        if (!end_all_xfers && !s3t->ahead)
            continue; // cancel in-use threads if end_all_xfers is TRUE

        // thread is active ...

        if ( pself->access_mode == ACCESS_NULL ) {
            read_cancel = (!ulnow && dlnow); // dubious test for download threads
            write_cancel = (ulnow && !dlnow); // dubious test for upload threads
        }

        // for end_all_xfers: also enable read cancel if ulnow and dlnow are both zero [??or both not??]
	read_cancel = read_cancel || (end_all_xfers && !write_cancel);

        // disable write cancel if if flushing *final* buffer now
	write_cancel = write_cancel && ( s3_buffer_read_size_func(s3buf) != s3_buffer_size_func(s3buf) );

        if (!read_cancel && !write_cancel)
            continue; // nothing to do...

        // perform any cancel using at_final flag always
        s3t->at_final = TRUE;

        // cause cancel to reads and/or *flush* to writes
        if (self->verbose)
           g_debug("[%p] %s ahead-thread s3t#%ld file#%d and [%#lx-%#lx) key=%s",
              s3buf, (read_cancel ? "cancelling" : "flushing"), s3t - self->s3t, 
              pself->file, s3t->xfer_begin, s3t->xfer_end, s3t->object_subkey);

        // set cancel for a downloading thread [when current copy is done]
        if (read_cancel) {
            s3_buffer_lock(s3buf);
            s3buf->cancel = TRUE;      // block ALL copying...
            g_cond_broadcast(s3buf->cond); // wake a curl-writer thread
            s3_buffer_unlock(s3buf);
            continue; // no write_cancel even matters now..
        }

        // set truncate&flush for an uploading thread [when current copy is done]
        {
            s3_buffer_lock(s3buf);

            g_debug("%s: [%p] finalize-write-block ctxt#%ld [%#lx-%#lx)", __FUNCTION__, s3buf, 
                  s3t - self->s3t, s3t->xfer_begin, s3t->xfer_end);

            // reset eodbytes down to amount uploaded and declare as "final" block [if active at all]
            s3_buffer_reset_eod_func(s3buf, s3_buffer_size_func(s3buf));

            // truncate xfer_end to nearest valid point
            s3t->xfer_end = max(s3t->xfer_begin, self->next_xbyte);

            g_cond_broadcast(s3buf->cond); // wake the reading thread if asleep

            // flush is before upload-data even written? 
            if (ulnow <= 1) {
                // MUST wait for a curl-reader thread to complete
                while ( !s3buf->cancel && !s3t->done )
                    g_cond_wait_until(s3buf->cond, s3buf->mutex, g_get_monotonic_time () + 200 * G_TIME_SPAN_MILLISECOND);
            }

            s3_buffer_unlock(s3buf);
        }
    } // for
    g_mutex_unlock(self->thread_list_mutex);
}
