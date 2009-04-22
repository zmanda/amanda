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


#endif
