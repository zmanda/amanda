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

#ifndef XFER_DEVICE_H
#define XFER_DEVICE_H

#include "amxfer.h"
#include "device.h"

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
 * the file will be finished.
 *
 * Implemented in xfer-dest-device.c
 *
 * @param device: the Device to write to, with a file started
 * @param max_memory: total amount of memory to use for buffers, or zero
 *                    for a reasonable default.
 * @return: new element
 */
XferElement *xfer_dest_device(
    Device *device,
    size_t max_memory);

#endif
