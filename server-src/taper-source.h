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

#ifndef __TAPER_SOURCE_H__
#define __TAPER_SOURCE_H__


/*
 * Type checking and casting macros
 */
#define TAPER_SOURCE_TYPE	(taper_source_get_type())
#define TAPER_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), taper_source_get_type(), TaperSource)
#define TAPER_SOURCE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), taper_source_get_type(), TaperSource const)
#define TAPER_SOURCE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), taper_source_get_type(), TaperSourceClass)
#define IS_TAPER_SOURCE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), taper_source_get_type ())

#define TAPER_SOURCE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), taper_source_get_type(), TaperSourceClass)

/*
 * Main object structure
 */
#ifndef __TYPEDEF_TAPER_SOURCE__
#define __TYPEDEF_TAPER_SOURCE__
typedef struct _TaperSource TaperSource;
#endif
struct _TaperSource {
    GObject __parent__;
    /*< private >*/
    gboolean end_of_data; /* protected */
    gboolean end_of_part; /* protected */
    guint64 max_part_size; /* protected */
    dumpfile_t * first_header;
    char * driver_handle;
    char * errmsg;
};

/*
 * Class definition
 */
typedef struct _TaperSourceClass TaperSourceClass;
struct _TaperSourceClass {
    GObjectClass __parent__;
    ssize_t (* read) (TaperSource * self, void * buf, size_t count);
    gboolean (* seek_to_part_start) (TaperSource * self);
    void (* start_new_part) (TaperSource * self);
    gboolean (* is_partial) (TaperSource * self);
    gboolean (* get_end_of_data)(TaperSource * self);
    gboolean (* get_end_of_part)(TaperSource * self);
    dumpfile_t * (* get_first_header)(TaperSource * self);
    char * (* get_errmsg)(TaperSource * self);
    int (* predict_parts)(TaperSource * self);
};


/*
 * Public methods
 */
GType	taper_source_get_type	(void);
ssize_t 	taper_source_read	(TaperSource * self,
					void * buf,
					size_t count);
gboolean 	taper_source_get_end_of_data	(TaperSource * self);
gboolean 	taper_source_get_end_of_part	(TaperSource * self);
dumpfile_t *    taper_source_get_first_header   (TaperSource * self);
char *          taper_source_get_errmsg         (TaperSource * self);
/* Returns -1 for an unknown number of splits, or a positive integer if the
 * number of splits is exactly known. Should never return zero. */
int             taper_source_predict_parts      (TaperSource * self);

/* You can only call this function (and expect to get an accurate
   result) if taper_source_get_end_of_data() has already returned
   TRUE. */
gboolean        taper_source_is_partial         (TaperSource * self);

gboolean 	taper_source_seek_to_part_start	(TaperSource * self);
void 	taper_source_start_new_part	(TaperSource * self);

/* This function is how you get a taper source. Call it with the
   relevant parameters, and the return value is yours to
   keep. Arguments must be consistant (e.g., if you specify FILE_WRITE
   mode, then you must provide a holding disk file). Input strings are
   copied internally. */
TaperSource * taper_source_new(char * handle,
                               cmd_t mode, char * holding_disk_file,
                               int socket_fd,
                               char * split_disk_buffer,
                               guint64 splitsize,
                               guint64 fallback_splitsize);

/* This function is a ProducerFunctor, as that type is defined in
   device-src/queueing.h. */
producer_result_t taper_source_producer(gpointer taper_source,
                                        queue_buffer_t * buffer,
                                        size_t hint_size);

#endif
