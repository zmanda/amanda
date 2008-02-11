/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2005-2008 Zmanda Inc.
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

/* The taper file source is a taper source (see taper-source.h)
   used for the case where we are reading from the holding disk
   (FILE-WRITE mode). */

#include <glib.h>
#include <glib-object.h>

#include "taper-source.h" 

#ifndef __TAPER_FILE_SOURCE_H__
#define __TAPER_FILE_SOURCE_H__

/*
 * Type checking and casting macros
 */
#define TAPER_TYPE_FILE_SOURCE	(taper_file_source_get_type())
#define TAPER_FILE_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), taper_file_source_get_type(), TaperFileSource)
#define TAPER_FILE_SOURCE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), taper_file_source_get_type(), TaperFileSource const)
#define TAPER_FILE_SOURCE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), taper_file_source_get_type(), TaperFileSourceClass)
#define TAPER_IS_FILE_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), taper_file_source_get_type ())

#define TAPER_FILE_SOURCE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), taper_file_source_get_type(), TaperFileSourceClass)

/* Private structure type */
typedef struct _TaperFileSourcePrivate TaperFileSourcePrivate;

/*
 * Main object structure
 */
#ifndef __TYPEDEF_TAPER_FILE_SOURCE__
#define __TYPEDEF_TAPER_FILE_SOURCE__
typedef struct _TaperFileSource TaperFileSource;
#endif
struct _TaperFileSource {
    TaperSource __parent__;
    /*< private >*/
    char * holding_disk_file;
    TaperFileSourcePrivate *_priv;
};

/*
 * Class definition
 */
typedef struct _TaperFileSourceClass TaperFileSourceClass;
struct _TaperFileSourceClass {
    TaperSourceClass __parent__;
};


/*
 * Public methods
 */
GType	taper_file_source_get_type	(void);

#endif
