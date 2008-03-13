/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008 Zmanda Inc.
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

/* The taper source object abstracts the different ways that taper can
 * retrieve and buffer data on its way to the device. It handles all
 * splitting up and re-reading of split-tape parts, as well as all
 * holding-disk related actions. */

#include <glib.h>
#include <glib-object.h>

#include <amanda.h>
#include "server_util.h"
#include "fileheader.h"
#include "queueing.h"

#ifndef __XFER_SOURCE_H__
#define __XFER_SOURCE_H__


/*
 * Type checking and casting macros
 */
#define XFER_SOURCE_TYPE	(xfer_source_get_type())
#define XFER_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_get_type(), XferSource)
#define XFER_SOURCE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_get_type(), XferSource const)
#define XFER_SOURCE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_get_type(), XferSourceClass)
#define IS_XFER_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_get_type ())

#define XFER_SOURCE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_get_type(), XferSourceClass)

/*
 * Main object structure
 */
typedef struct {
    GObject __parent__;
    /*< private >*/
    gboolean end_of_data; /* protected */
} XferSource ;

/*
 * Class definition
 */
typedef struct {
    GObjectClass __parent__;
    ssize_t (* read) (XferSource * self, void * buf, size_t count);
} XferSourceClass ;


/*
 * Public methods
 */
GType xfer_source_get_type(void);

ssize_t xfer_source_read(XferSource * self, void * buf, size_t count);

/* This function is how you get a taper source. Call it with the
   relevant parameters, and the return value is yours to
   keep. Arguments must be consistant (e.g., if you specify FILE_WRITE
   mode, then you must provide a holding disk file). Input strings are
   copied internally. */
XferSource * xfer_source_new(char * handle,
                               cmd_t mode, char * holding_disk_file,
                               int socket_fd, int control_fd,
                               char * split_disk_buffer,
                               guint64 splitsize,
                               guint64 fallback_splitsize);

#endif
