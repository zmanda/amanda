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

/* The VFS device is the driver formerly known as the vtape driver or
 * the file driver. It uses a directory on the UNIX filesystem as a
 * data store. */

#include <glib.h>
#include <glib-object.h>
#include "device.h"
#include <dirent.h>

#ifndef VFS_DEVICE_H
#define VFS_DEVICE_H

#define VFS_DEVICE_MIN_BLOCK_SIZE (1)
#define VFS_DEVICE_MAX_BLOCK_SIZE (INT_MAX)
#define VFS_DEVICE_DEFAULT_BLOCK_SIZE (MAX_TAPE_BLOCK_BYTES)
#define VFS_DEVICE_LABEL_SIZE (32768)

/* This looks dangerous, but is actually modified by the umask. */
#define VFS_DEVICE_CREAT_MODE 0666

/*
 * Type checking and casting macros
 */
#define TYPE_VFS_DEVICE	(vfs_device_get_type())
#define VFS_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), vfs_device_get_type(), VfsDevice)
#define VFS_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), vfs_device_get_type(), VfsDevice const)
#define VFS_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), vfs_device_get_type(), VfsDeviceClass)
#define IS_VFS_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), vfs_device_get_type ())

#define VFS_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), vfs_device_get_type(), VfsDeviceClass)

/*
 * Main object structure
 */
typedef struct {
    Device __parent__;

    /*< private >*/
    DIR * dir_handle;
    char * dir_name;
    char * file_name;
    int file_lock_fd;
    char * file_lock_name;
    int volume_lock_fd;
    char * volume_lock_name;
    int open_file_fd;
    
    /* Properties */
    int block_size;
    guint64 volume_bytes;
    guint64 volume_limit;
} VfsDevice;

/*
 * Class definition
 */
typedef struct {
    DeviceClass __parent__;
} VfsDeviceClass;


/*
 * Public methods
 */
GType	vfs_device_get_type	(void);
void    vfs_device_register     (void);

#endif

