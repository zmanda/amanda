/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as 
 * published by the Free Software Foundation.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 * 
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#ifndef QUEUEING_H
#define QUEUEING_H

/* This file contains the code for fast threaded reading and writing to/from
 * media, for devices that don't require any special handling. Some
 * devices (e.g., CD-ROM) may use a different method for bulk reads or
 * writes. */

#include <glib.h>
#include "property.h"

#define DEFAULT_MAX_BUFFER_MEMORY (1*1024*1024)

/* Valid data in this structure starts at data + offset, and has size
 * data_size. Allocation starts at data and has size alloc_size. */
typedef struct {
    char *data;
    guint alloc_size;
    guint data_size;
    guint offset;
} queue_buffer_t;

void free_buffer(queue_buffer_t*);

typedef enum {
    PRODUCER_MORE,     /* Means the producer should be run again. */
    PRODUCER_FINISHED, /* Means that no error occured, but the
                          producer should not be run again. */
    PRODUCER_ERROR     /* Means an error occured, and the producer
                          should not be run again. */
} producer_result_t;

typedef enum {
    QUEUE_SUCCESS = 0,
    QUEUE_PRODUCER_ERROR = 1 << 0,
    QUEUE_CONSUMER_ERROR = 1 << 1,
    QUEUE_INTERNAL_ERROR = 1 << 2
} queue_result_flags;

/* The producer takes the given buffer (which is not itself NULL, but
 * may contain a NULL data segment), and fills it with data. The
 * producer should feel free to allocate or reallocate data as
 * necessary; the queueing system will free it when necessary. The
 * result of the production operation is specified in the return
 * value, but if the buffer is left without data, then that is
 * interpreted as PRODUCER_ERROR. It is preferred (but not required)
 * that the producer produce hint_size bytes of data, 
 *
 * The consumer takes the given buffer (which will not be NULL, nor
 * contain a NULL data segment) and processess it. If there is a
 * problem consuming data (such that no further data should be
 * consumed), the consumer may return -1. Otherwise, the consumer
 * should return the number of bytes actually consumed.
 * If an error occurs, return -1, regardless of the number of bytes consumed.
 * If the amount of data written is not a full block, then this is the
 * last (partial block) of data. The consumer should do whatever is
 * appropriate in that case.
 *
 * Note that the handling of the queue_buffer_t is different between
 * the two functions: The producer should update queue_buffer_t as
 * necessary to corespond to read data, while the consumer should
 * leave the queue_buffer_t unadjusted: The queueing framework will
 * invalidate data in the buffer according to the return value of the
 * consumer.*/
typedef producer_result_t (* ProducerFunctor)(gpointer user_data,
                                              queue_buffer_t* buffer,
                                              int hint_size);
typedef int (* ConsumerFunctor)(gpointer user_data,
                                queue_buffer_t* buffer);


/* These functions make the magic happen. The first one assumes
   reasonable defaults, the second one provides more options.
   % producer           : A function that provides data to write.
   % producer_user_data : A pointer to pass to that function.
   % consumer           : A function that writes data out.
   % consumer_user_data : A pointer to pass to that function.
   % block_size         : Size of chunks to write out to consumer. If
                          nonpositive, data will be written in
                          variable-sized chunks.
   % max_memory         : Amount of memory to be used for buffering.
                          (default is DEFAULT_MAX_BUFFER_MEMORY).
   % streaming_mode     : Describes streaming mode.
         STREAMING_REQUIREMENT_NONE:     Data will be written as fast
                                         as possible. No prebuffering
                                         will be done.
         STREAMING_REQUIREMENT_DESIRED:  max_memory bytes of data will
                                         be prebuffered, and if the
                                         buffer ever empties, no data
                                         will be written until it
                                         fills again.
         STREAMING_REQUIREMENT_REQUIRED: max_memory bytes of data will
                                         be prebuffered, and
                                         thereafter data will be
                                         written as fast as possible.
*/
gboolean
do_consumer_producer_queue(ProducerFunctor producer,
                           gpointer producer_user_data,
                           ConsumerFunctor consumer,
                           gpointer consumer_user_data);
queue_result_flags
do_consumer_producer_queue_full(ProducerFunctor producer,
                                gpointer producer_user_data,
                                ConsumerFunctor consumer,
                                gpointer consumer_user_data,
                                int block_size,
                                size_t max_memory,
                                StreamingRequirement streaming_mode);

/* Some commonly-useful producers and consumers.*/

/* These functions will call device_read_block and device_write_block
 * respectively. The user data should be a Device*.
 *
 * device_write_consumer assumes that the block_size passed to
 * do_consumer_producer_queue_full is at least device_write_min_size();
 * do_consumer_thread() will not pass a buffer of less than block_size
 * to the consumer unless it has received EOF from the producer thread.
 */
producer_result_t device_read_producer(gpointer device,
                                       queue_buffer_t *buffer,
                                       int hint_size);
int device_write_consumer(gpointer device, queue_buffer_t *buffer);

/* These functions will call read() or write() respectively. The user
   data should be a file descriptor stored with GINT_TO_POINTER. */
producer_result_t fd_read_producer(gpointer fd, queue_buffer_t *buffer,
                                   int hint_size);
int fd_write_consumer(gpointer fd, queue_buffer_t *buffer);



#endif /* QUEUEING_H */
