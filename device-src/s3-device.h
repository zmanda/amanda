/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

#ifndef __S3_DEVICE_H__
#define __S3_DEVICE_H__

#include "device.h"
#include "s3.h"
#include "s3-util.h"
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

/* Maximum key length as specified in the S3 documentation
 * (*excluding* null terminator) */
#define S3_MAX_KEY_LENGTH 1024

/* Note: for compatability, min can only be decreased and max increased */
// minimum allows 64k * 10000 == 640M > 512MB uploads
#define S3_DEVICE_MIN_BLOCK_SIZE 0x10000      
// maximum allows 512MiB * 10,000 == 5.36TB > 5.0TB max for AWS (a.k.a 4.88TiB) 
#define S3_DEVICE_MAX_BLOCK_SIZE (512 * 1024 * 1024)
#define S3_DEVICE_DEFAULT_BLOCK_SIZE BUFFER_SIZE_MAX_DEFAULT

/*
 * Main object structure
 */
typedef struct _S3MetadataFile S3MetadataFile;
typedef struct _S3Device S3Device;
typedef struct _S3_by_thread S3_by_thread;
typedef const struct _S3DeviceClass S3DeviceClass;

#endif

