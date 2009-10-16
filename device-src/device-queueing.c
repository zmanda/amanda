/*
 * Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

#include "amanda.h"
#include "device-queueing.h"
#include "device.h"

producer_result_t device_read_producer(gpointer devicep,
                                       queue_buffer_t *buffer,
                                       size_t hint_size G_GNUC_UNUSED) {
    Device* device;

    device = (Device*) devicep;
    g_assert(IS_DEVICE(device));

    buffer->offset = 0;
    for (;;) {
        int result, read_size;
        read_size = buffer->alloc_size;
        result = device_read_block(device, buffer->data, &read_size);
        if (result > 0) {
            buffer->data_size = read_size;
            return PRODUCER_MORE;
        } else if (result == 0) {
	    /* unfortunately, the best "memory" we have of needing a larger
	     * block size is the next time this buffer comes around, and even
	     * this is incomplete as buffers may be resized periodically.  So
	     * we'll end up calling read_block with small buffers more often
	     * than strictly necessary. */
            buffer->data = realloc(buffer->data, read_size);
            buffer->alloc_size = read_size;
        } else if (device->is_eof) {
            return PRODUCER_FINISHED;
        } else {
            buffer->data_size = 0;
            return PRODUCER_ERROR;
        }
    }
}

ssize_t device_write_consumer(gpointer devicep, queue_buffer_t *buffer) {
    Device* device;
    size_t write_size;
    gsize block_size;

    device = DEVICE(devicep);

    block_size = device->block_size;
    write_size = MIN(buffer->data_size, block_size);

    /* we assume that the queueing module is providing us with
     * appropriately-sized blocks until the last block. */
    if (device_write_block(device, write_size,
                           buffer->data + buffer->offset)) {
        /* Success! */
        return write_size;
    } else {
        /* Nope, really an error. */
        return -1;
    }
}

