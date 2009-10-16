/*
 * Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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

    /* characteristics of the device */
    gboolean fsf, bsf, fsr, bsr, eom, bsf_after_eom, broken_gmt_online;
    gboolean nonblocking_open, fsf_after_filemark;
    int final_filemarks;

    /* 0 if we opened with O_RDWR; error otherwise. */
    gboolean write_open_errno;
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

/* Tape device properties. These properties do not exist on non-linear
   devices. */
extern DevicePropertyBase device_property_broken_gmt_online;
#define PROPERTY_BROKEN_GMT_ONLINE (device_property_broken_gmt_online.ID)

extern DevicePropertyBase device_property_fsf;
#define PROPERTY_FSF (device_property_fsf.ID)

extern DevicePropertyBase device_property_fsf_after_filemark;
#define PROPERTY_FSF_AFTER_FILEMARK (device_property_fsf_after_filemark.ID)

extern DevicePropertyBase device_property_bsf;
#define PROPERTY_BSF (device_property_bsf.ID)

extern DevicePropertyBase device_property_fsr;
#define PROPERTY_FSR (device_property_fsr.ID)

extern DevicePropertyBase device_property_bsr;
#define PROPERTY_BSR (device_property_bsr.ID)

/* Is EOM supported? Must be able to read file number afterwards as
   well. */
extern DevicePropertyBase device_property_eom;
#define PROPERTY_EOM (device_property_eom.ID)

/* Is it necessary to perform a BSF after EOM? */
extern DevicePropertyBase device_property_bsf_after_eom;
#define PROPERTY_BSF_AFTER_EOM (device_property_bsf_after_eom.ID)

/* Should the device be opened with O_NONBLOCK */
extern DevicePropertyBase device_property_nonblocking_open;
#define PROPERTY_NONBLOCKING_OPEN (device_property_nonblocking_open.ID)

/* How many filemarks to write at EOD? (Default is 2).
 * This property is a G_TYPE_UINT, but can only really be set to 1 or 2. */
extern DevicePropertyBase device_property_final_filemarks;
#define PROPERTY_FINAL_FILEMARKS (device_property_final_filemarks.ID)

/* useful callback for tape ops */
void tape_device_set_capabilities(TapeDevice *self,
	gboolean fsf, PropertySurety fsf_surety, PropertySource fsf_source,
	gboolean fsf_after_filemark, PropertySurety faf_surety, PropertySource faf_source,
	gboolean bsf, PropertySurety bsf_surety, PropertySource bsf_source,
	gboolean fsr, PropertySurety fsr_surety, PropertySource fsr_source,
	gboolean bsr, PropertySurety bsr_surety, PropertySource bsr_source,
	gboolean eom, PropertySurety eom_surety, PropertySource eom_source,
	gboolean bsf_after_eom, PropertySurety bae_surety, PropertySource bae_source,
	guint final_filemarks, PropertySurety ff_surety, PropertySource ff_source);

#endif
