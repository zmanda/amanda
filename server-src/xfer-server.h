/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

#ifndef XFER_SERVER_H
#define XFER_SERVER_H

#include "amxfer.h"

/* A transfer source that reads from a holding file, following CONT_FILENAME
 * from one chunk to the next.  If the downstream element is an XferDestTaper,
 * this source will call its cache_inform method for each chunk.
 *
 * Implemented in xfer-source-holding.c
 *
 * @param device: holding filename
 * @param send_cache_inform: TRUE if this element should call cache_inform on
 *	the xfer's destination element
 * @return: new element
 */
XferElement *xfer_source_holding(
    const char *filename);

guint64
xfer_source_holding_get_bytes_read(
    XferElement *elt);

#endif
