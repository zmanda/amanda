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

/* The NULL device accepts data and sends it to the bit bucket. Like
   /dev/null, you cannot read from the NULL device -- only
   write. While useful for testing, the NULL device is incredibly
   dangerous in practice (because it eats your data). So it will
   generate warnings whenever you use it. */

#include <glib.h>
#include <glib-object.h>
#ifndef __NULL_DEVICE_H__
#define __NULL_DEVICE_H__

/* This header file is very boring, because the class just overrides
   existing methods. */

/*
 * Type checking and casting macros
 */
#define TYPE_NULL_DEVICE	(null_device_get_type())
#define NULL_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), null_device_get_type(), NullDevice)
#define NULL_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), null_device_get_type(), NullDevice const)
#define NULL_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), null_device_get_type(), NullDeviceClass)
#define IS_NULL_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), null_device_get_type ())

#define NULL_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), null_device_get_type(), NullDeviceClass)

/*
 * Main object structure
 */
#ifndef __TYPEDEF_NULL_DEVICE__
#define __TYPEDEF_NULL_DEVICE__
typedef struct _NullDevice NullDevice;
#endif
struct _NullDevice {
	Device __parent__;
};

/*
 * Class definition
 */
typedef struct _NullDeviceClass NullDeviceClass;
struct _NullDeviceClass {
    DeviceClass __parent__;
    gboolean in_file;
};


/*
 * Public methods
 */
GType	null_device_get_type	(void);
void    null_device_register    (void);

#endif
