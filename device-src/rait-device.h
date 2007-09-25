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

/* The RAIT device encapsulates some number of other devices into a single
 * redundant device. */

#ifndef RAIT_DEVICE_H
#define RAIT_DEVICE_H

#include <glib.h>
#include <glib-object.h>
#include "device.h"

/*
 * Type checking and casting macros
 */
#define TYPE_RAIT_DEVICE	(rait_device_get_type())
#define RAIT_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), rait_device_get_type(), RaitDevice)
#define RAIT_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), rait_device_get_type(), RaitDevice const)
#define RAIT_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), rait_device_get_type(), RaitDeviceClass)
#define IS_RAIT_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), rait_device_get_type ())

#define RAIT_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), rait_device_get_type(), RaitDeviceClass)

/*
 * Main object structure
 */
typedef struct RaitDevicePrivate_s RaitDevicePrivate;
typedef struct RaitDevice_s {
    Device __parent__;

    RaitDevicePrivate * private;
} RaitDevice;

/*
 * Class definition
 */
typedef struct _RaitDeviceClass RaitDeviceClass;
struct _RaitDeviceClass {
    DeviceClass __parent__;
};


/*
 * Public methods
 */
GType	rait_device_get_type	(void);
Device * rait_device_factory	(char * type,
                                 char * name);
/* Pass this factory a NULL-terminated array of Devices, and it will make a
   RAIT out of them. The returned device refss the passed devices, so unref
   them yourself. */
Device * rait_device_new_from_devices(Device ** devices);
void 	rait_device_register	(void);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
