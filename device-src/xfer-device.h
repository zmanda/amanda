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

#ifndef XFER_DEVICE_H
#define XFER_DEVICE_H

#include "amxfer.h"
#include "device.h"
#include "xfer-dest-taper.h"

/* A transfer source that reads from a Device. The device must be positioned
 * at the start of a file before the transfer is started.  The transfer will
 * continue until the end of the file.
 *
 * Implemented in xfer-source-device.c
 *
 * @param device: Device object to read from
 * @return: new element
 */
XferElement *xfer_source_device(
    Device *device);

/* A transfer destination that writes bytes to a Device.  The device should have a
 * file started, ready for a device_write_block call.  On completion of the transfer,
 * the file will be finished.  If a device error occurs, the transfer will be cancelled.
 *
 * If cancel_at_leom is true, then the transfer will also be cancelled on LEOM, with
 * the error message containing the string "LEOM detected" (this is used by amtapetype).
 *
 * Implemented in xfer-dest-device.c
 *
 * @param device: the Device to write to, with a file started
 * @param canel_at_leom: if true, the element will cancel the transfer at LEOM.
 * @return: new element
 */
XferElement *xfer_dest_device(
    Device *device,
    gboolean canel_at_leom);

/* Constructor for XferDestTaperSplitter, which writes data to devices block by
 * block and splitting parts but no caching.
 *
 * @param first_device: the first device that will be used with this xfer, used
 *                      to calculate some internal parameters
 * @param max_memory: total amount of memory to use for buffers, or zero
 *                    for a reasonable default.
 * @param part_size: the desired size of each part
 * @param expect_cache_inform: TRUE if this element will get cache_inform messages
 * @return: new element
 */
XferElement *
xfer_dest_taper_splitter(
    Device *first_device,
    size_t max_memory,
    guint64 part_size,
    gboolean expect_cache_inform);

/* Constructor for XferDestTaperCacher, which writes data to devices block by
 * block and handles caching and splitting parts.
 *
 * @param first_device: the first device that will be used with this xfer, used
 *                      to calculate some internal parameters
 * @param max_memory: total amount of memory to use for buffers, or zero
 *                    for a reasonable default.
 * @param part_size: the desired size of each part
 * @param use_mem_cache: if true, use the memory cache
 * @param disk_cache_dirname: if not NULL, this is the directory in which the disk
 *		      cache should be created
 * @return: new element
 */
XferElement *
xfer_dest_taper_cacher(
    Device *first_device,
    size_t max_memory,
    guint64 part_size,
    gboolean use_mem_cache,
    const char *disk_cache_dirname);

/* Constructor for XferDestTaperDirectTCP, which uses DirectTCP to transfer data
 * to devices (which must support the feature).
 *
 * @param first_device: the first device that will be used with this xfer, used
 *                      to calculate some internal parameters
 * @param part_size: the desired size of each part
 * @return: new element
 */
XferElement *
xfer_dest_taper_directtcp(
    Device *first_device,
    guint64 part_size);

/*
 * XferSourceRecovery
 */

/* Create a new XferSourceRecovery object.  Like XferDestTaper instances, this object
 * will not start transferring data until xfer_source_recovery_start_part is called to
 * give the device from which the data should flow.
 *
 * @param first device: teh first device that will be used with this xfer
 * @returns: new element
 */
XferElement *
xfer_source_recovery(
    Device *first_device);

/* Start an XferSourceRecovery reading from a particular, pre-positioned device
 *
 * @param self: XferSourceRecovery object
 * @param device: device to read from
 */
void
xfer_source_recovery_start_part(
    XferElement *elt,
    Device *device);

/* Prepare to read subsequent parts from the given device.  The device must
 * not be started yet.  It is not necessary to call this method for the first
 * device used in a transfer.
 *
 * @param self: the XferSourceRecovery object
 * @param device: the device
 */
void xfer_source_recovery_use_device(
    XferElement *self,
    Device *device);

guint64
xfer_source_recovery_get_bytes_read(
    XferElement *elt);

#endif
