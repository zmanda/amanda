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

#ifndef TAPE_DEVICE_H
#define TAPE_DEVICE_H

#include <device.h>

/* Unlike other Device classes, this class is implemented across multiple source
 * files, so its class declaration is placed in a header file.
 */

/*
 * Type checking and casting macros
 */

#define TYPE_TAPE_DEVICE	(tape_device_get_type())
#define TAPE_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), tape_device_get_type(), TapeDevice)
#define TAPE_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), tape_device_get_type(), TapeDevice const)
#define TAPE_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), tape_device_get_type(), TapeDeviceClass)
#define IS_TAPE_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), tape_device_get_type ())
#define TAPE_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), tape_device_get_type(), TapeDeviceClass)
GType	tape_device_get_type	(void);

/*
 * Main object structure
 */
typedef struct TapeDevicePrivate_s TapeDevicePrivate;
typedef struct _TapeDevice {
    Device __parent__;

    /* It should go without saying that all this stuff is
     * look-but-don't-touch. */
    guint min_block_size, max_block_size, fixed_block_size, read_block_size;
    FeatureSupportFlags fsf, bsf, fsr, bsr, eom, bsf_after_eom;
    int final_filemarks;
    gboolean compression, broken_gmt_online;
    /* 0 if we opened with O_RDWR; error otherwise. */
    gboolean write_open_errno;
    gboolean first_file; /* Is this the first file in append mode? */
    int fd;

    TapeDevicePrivate * private;
} TapeDevice;

/*
 * Class definition
 */
typedef struct _TapeDeviceClass TapeDeviceClass;
struct _TapeDeviceClass {
	DeviceClass __parent__;
};

#endif
