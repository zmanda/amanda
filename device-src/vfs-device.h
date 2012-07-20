/*
 * Copyright (c) 2005-2012 Zmanda Inc.  All Rights Reserved.
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

#ifndef __VFS_DEVICE_H__
#define __VFS_DEVICE_H__

#include "device.h"

/*
 * Type checking and casting macros
 */
#define TYPE_VFS_DEVICE	(vfs_device_get_type())
#define VFS_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), vfs_device_get_type(), VfsDevice)
#define VFS_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), vfs_device_get_type(), VfsDevice const)
#define VFS_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), vfs_device_get_type(), VfsDeviceClass)
#define IS_VFS_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), vfs_device_get_type ())

#define VFS_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), vfs_device_get_type(), VfsDeviceClass)

GType	vfs_device_get_type	(void);

/*
 * Main object structure
 */
typedef struct {
    Device __parent__;

    /*< private >*/
    char * dir_name;
    char * file_name;
    int open_file_fd;
    gboolean leom;

    /* Properties */
    guint64 volume_bytes;
    guint64 volume_limit;
    gboolean enforce_volume_limit;

    /* should we monitor free space? (controlled by MONITOR_FREE_SPACE property) */
    gboolean monitor_free_space;

    /* how many bytes were free at last check */
    guint64 checked_fs_free_bytes;

    /* when was that check performed? */
    time_t checked_fs_free_time;

    /* and how many bytes have been written since the last check? */
    guint64 checked_bytes_used;
} VfsDevice;

/*
 * Class definition
 */
typedef struct {
    DeviceClass __parent__;
} VfsDeviceClass;

/* Implementation functions */
void delete_vfs_files(VfsDevice * self);

#endif
