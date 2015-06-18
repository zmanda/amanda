/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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

#ifndef XFER_SERVER_H
#define XFER_SERVER_H

#include "amxfer.h"
#include "fileheader.h"

/* A transfer source that reads from a holding file, following CONT_FILENAME
 * from one chunk to the next.  If the downstream element is an XferDestTaper,
 * this source will call its cache_inform method for each chunk.
 *
 * Implemented in xfer-source-holding.c
 *
 * @param filename: holding filename
 * @return: new element
 */
XferElement *xfer_source_holding(
    const char *filename);

void
xfer_source_holding_start_recovery(
    XferElement *elt);

guint64
xfer_source_holding_get_bytes_read(
    XferElement *elt);

/* A transfer destination that writes to holding file.
 *
 * Implemented in xfer-dest-holding.c
 *
 * @param max_memory: total amount of memory to use for buffers, or zero
 *		      for a reasonable default.
 * @return: new element
 */
XferElement *xfer_dest_holding(
    size_t max_memory);

void
xfer_dest_holding_start_chunk(
    XferElement *elt,
    dumpfile_t *chunk_header,
    char *filename,
    guint64 use_bytes);

void
xfer_dest_holding_finish_chunk(
    XferElement *elt);

#endif
