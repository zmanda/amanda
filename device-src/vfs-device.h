/*
 * Copyright (c) 2005-2012 Zmanda Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
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

/* This looks dangerous, but is actually modified by the umask. */
#define VFS_DEVICE_CREAT_MODE 0666

#define VFS_DEVICE_LABEL_SIZE (32768)

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

    /* should the data subdir must be use */
    int use_data;

    /* how many bytes were free at last check */
    guint64 checked_fs_free_bytes;

    /* when was that check performed? */
    time_t checked_fs_free_time;

    /* and how many bytes have been written since the last check? */
    guint64 checked_bytes_used;
    gboolean (* clear_and_prepare_label)(Device *dself, char *label, char *timestamp);
    void (* release_file)(Device *dself);
    void (* update_volume_size)(Device *dself);
    gboolean (* device_start_file_open)(Device *dself, dumpfile_t *ji);
    gboolean (* validate)(Device *dself);
} VfsDevice;

/*
 * Class definition
 */
typedef struct {
    DeviceClass __parent__;
} VfsDeviceClass;

/* Possible (abstracted) results from a system I/O operation. */
typedef enum {
    RESULT_SUCCESS,
    RESULT_ERROR,        /* Undefined error. */
    RESULT_NO_DATA,      /* End of File, while reading */
    RESULT_NO_SPACE,     /* Out of space. Sometimes we don't know if
                            it was this or I/O error, but this is the
                            preferred explanation. */
    RESULT_MAX
} IoResult;

/* Implementation functions */
void delete_vfs_files(VfsDevice * self);

IoResult vfs_device_robust_write(VfsDevice *self, char *buf, int count);
IoResult vfs_device_robust_read(VfsDevice *self, char *buf, int *count);
gboolean vfs_write_amanda_header(VfsDevice *self, const dumpfile_t *header);
#endif
