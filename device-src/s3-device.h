/*
 * Copyright (c) 2008-2014 Zmanda, Inc.  All Rights Reserved.
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

/* An S3 device uses Amazon's S3 service (http://www.amazon.com/s3) to store
 * data.  It stores data in keys named with a user-specified prefix, inside a
 * user-specified bucket.  Data is stored in the form of numbered (large)
 * blocks.
 */

#ifndef __S3_DEVICE_H__
#define __S3_DEVICE_H__

#include "device.h"
#include "s3.h"
 /* Type checking and casting macros
 */
#define TYPE_S3_DEVICE	(s3_device_get_type())
#define S3_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), s3_device_get_type(), S3Device)
#define S3_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), s3_device_get_type(), S3Device const)
#define S3_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), s3_device_get_type(), S3DeviceClass)
#define IS_S3_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), s3_device_get_type ())

#define S3_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), s3_device_get_type(), S3DeviceClass)
GType	s3_device_get_type	(void);

extern DevicePropertyBase device_property_s3_access_key;
extern DevicePropertyBase device_property_s3_secret_key;
#define PROPERTY_S3_SECRET_KEY (device_property_s3_secret_key.ID)
#define PROPERTY_S3_ACCESS_KEY (device_property_s3_access_key.ID)
/*
 * Main object structure
 */
typedef struct _S3MetadataFile S3MetadataFile;
typedef struct _S3Device S3Device;

typedef struct _S3_by_thread S3_by_thread;
struct _S3_by_thread {
    S3Handle            *s3;
    CurlBuffer           curl_buffer;
    guint                buffer_len;
    int                  idle;
    int                  eof;
    int                  done;
    char                *filename;
    char		*uploadId;
    int			 partNumber;
    guint64		 range_min;
    guint64		 range_max;
    DeviceStatusFlags    errflags;	/* device_status */
    char                *errmsg;	/* device error message */
    GMutex		*now_mutex;
    guint64		 dlnow, ulnow;
    time_t		 timeout;
};

struct _S3Device {
    Device __parent__;

    char *catalog_filename;
    char *catalog_label;
    char *catalog_header;

    /* The "easy" curl handle we use to access Amazon S3 */
    S3_by_thread *s3t;

    /* S3 access information */
    char *bucket;
    char *prefix;

    /* The S3 access information. */
    char *secret_key;
    char *access_key;
    char *session_token;
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
    gboolean set_s3_multi_delete;
    char        *uploadId;
    GTree       *part_etag;
    char        *filename;

    int          nb_threads;
    int          nb_threads_backup;
    int          nb_threads_recovery;
    gboolean     use_s3_multi_part_upload;
    GThreadPool *thread_pool_delete;
    GThreadPool *thread_pool_write;
    GThreadPool *thread_pool_read;
    GCond       *thread_idle_cond;
    GMutex      *thread_idle_mutex;
    int		 last_byte_read;
    int          next_block_to_read;
    int		 next_byte_to_read;
    GSList      *objects;
    guint64	 object_size;
    gboolean	 bucket_made;

    guint64      dltotal;
    guint64      ultotal;

    /* google OAUTH2 */
    char        *client_id;
    char        *client_secret;
    char        *refresh_token;
    char        *project_id;

    gboolean	 reuse_connection;
    gboolean	 chunked;

    gboolean	 read_from_glacier;
    int		 transition_to_glacier;
    long	 timeout;

    /* CAStor */
    char        *reps;
    char        *reps_bucket;
};

/*
 * Class definition
 */
typedef struct _S3DeviceClass S3DeviceClass;
struct _S3DeviceClass {
    DeviceClass __parent__;
};

#endif

