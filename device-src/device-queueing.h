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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#ifndef DEVICE_QUEUEING_H
#define DEVICE_QUEUEING_H

/* some utilities for using queueing with device */

#include "queueing.h"

/* These functions will call device_read_block and device_write_block
 * respectively. The user data should be a Device*.
 *
 * device_write_consumer assumes that the block_size passed to
 * do_consumer_producer_queue_full is at least device->block_size();
 * do_consumer_thread() will not pass a buffer of less than block_size
 * to the consumer unless it has received EOF from the producer thread.
 *
 * Similarly, device_read_producer works similarly, but will expand its
 * buffers if the device encounters larger blocks - this is Amanda's
 * ability to read volumes written with larger block sizes.
 */
producer_result_t device_read_producer(gpointer device,
                                       queue_buffer_t *buffer,
                                       size_t hint_size);
ssize_t device_write_consumer(gpointer device, queue_buffer_t *buffer);

#endif /* DEVICE_QUEUEING_H */
