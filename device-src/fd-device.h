/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2005 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

/* The FdDevice is another abstract class (inherited from plain
   Device), which implements functionality common to all devices that
   use a UNIX file descriptor. */

#ifndef FD_DEVICE_H
#define FD_DEVICE_H

#include <glib.h>
#include <glib-object.h>
#include "device.h"

/* Possible (abstracted) results from a system I/O operation. */
typedef enum {
    RESULT_SUCCESS,
    RESULT_ERROR,        /* Undefined error. */
    RESULT_SMALL_BUFFER, /* Tried to read with a buffer that is too
                            small. */
    RESULT_NO_DATA,      /* End of File, while reading */
    RESULT_NO_SPACE,     /* Out of space. Sometimes we don't know if
                            it was this or I/O error, but this is the
                            preferred explanation. */
    RESULT_MAX
} FdDeviceResult;

/*
 * Type checking and casting macros
 */
#define TYPE_FD_DEVICE	(fd_device_get_type())
#define FD_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), fd_device_get_type(), FdDevice)
#define FD_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), fd_device_get_type(), FdDevice const)
#define FD_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), fd_device_get_type(), FdDeviceClass)
#define IS_FD_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), fd_device_get_type ())

#define FD_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), fd_device_get_type(), FdDeviceClass)

/*
 * Main object structure
 */
typedef struct {
    Device __parent__;
    /*< private >*/
    /* Child classes: Modify this at will.
     * The rest of the world: look but don't touch. */
    int fd;
} FdDevice;

/*
 * Class definition
 */
typedef struct _FdDeviceClass FdDeviceClass;
struct _FdDeviceClass {
    DeviceClass __parent__;
    /* These virtual functions have default implementations, so
     * reimplementation is not required. They do the equivalent of a
     * standard UNIX read() or write(), except that they guarantee a
     * full block of data is read or written. This is the place to
     * handle any wierd deviceness in reading or writing.
     *
     * Note that these functions will be called during device setup,
     * so don't make assumptions about labels etc. yet.
     */
    /* count should be filled in with the actual number of bytes read. */
    FdDeviceResult (* robust_read) (FdDevice * self, void * buf, int * count);
    FdDeviceResult (* robust_write)(FdDevice * self, void * buf, int count);

    /* This function returns the range of possible sizes (in bytes) of the
     * tapestart header. The default implementation uses the actual block
     * size range. The dispatch function is private to fd-device.c . */
    void (*label_size_range)(FdDevice * self, guint * min, guint * max);

    /* This function is called when fd_device_read_block reaches EOF. You
       can do whatever you please, but if you want to change the
       return value of fd_device_read_block, then you must also
       override device_read_block.

       The default implementation closes the fd. */
    void (* found_eof)(FdDevice * self);
};

GType	fd_device_get_type	(void);
void fd_device_register(void);

#endif
