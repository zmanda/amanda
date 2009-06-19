/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* The taper disk port source is a taper source (see taper-source.h)
   used for the case where we are reading directly from a client
   (PORT-WRITE) and are using a disk buffer to hold split dump parts. */

#include <glib.h>
#include <glib-object.h>

#include "taper-port-source.h"

#ifndef __TAPER_DISK_PORT_SOURCE_H__
#define __TAPER_DISK_PORT_SOURCE_H__


/*
 * Type checking and casting macros
 */
#define TAPER_TYPE_DISK_PORT_SOURCE	(taper_disk_port_source_get_type())
#define TAPER_DISK_PORT_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), taper_disk_port_source_get_type(), TaperDiskPortSource)
#define TAPER_DISK_PORT_SOURCE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), taper_disk_port_source_get_type(), TaperDiskPortSource const)
#define TAPER_DISK_PORT_SOURCE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), taper_disk_port_source_get_type(), TaperDiskPortSourceClass)
#define TAPER_IS_DISK_PORT_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), taper_disk_port_source_get_type ())

#define TAPER_DISK_PORT_SOURCE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), taper_disk_port_source_get_type(), TaperDiskPortSourceClass)

/* Private structure type */
typedef struct _TaperDiskPortSourcePrivate TaperDiskPortSourcePrivate;

/*
 * Main object structure
 */
#ifndef __TYPEDEF_TAPER_DISK_PORT_SOURCE__
#define __TYPEDEF_TAPER_DISK_PORT_SOURCE__
typedef struct _TaperDiskPortSource TaperDiskPortSource;
#endif
struct _TaperDiskPortSource {
    TaperPortSource __parent__;
    /*< private >*/
    guint64 fallback_buffer_size;
    char * buffer_dir_name;
    TaperDiskPortSourcePrivate *_priv;
};

/*
 * Class definition
 */
typedef struct _TaperDiskPortSourceClass TaperDiskPortSourceClass;
struct _TaperDiskPortSourceClass {
	TaperPortSourceClass __parent__;
};

/*
 * Public methods
 */
GType	taper_disk_port_source_get_type	(void);

#endif
