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

#ifndef QUEUEING_H
#define QUEUEING_H

/* This file contains the code for fast threaded reading and writing to/from
 * media, for devices that don't require any special handling. Some
 * devices (e.g., CD-ROM) may use a different method for bulk reads or
 * writes. */

#include "amanda.h"
#include <glib.h>

#define DEFAULT_MAX_BUFFER_MEMORY (1*1024*1024)

typedef enum {
    STREAMING_REQUIREMENT_NONE,
    STREAMING_REQUIREMENT_DESIRED,
    STREAMING_REQUIREMENT_REQUIRED
} StreamingRequirement;

/* Valid data in this structure starts at data + offset, and has size
 * data_size. Allocation starts at data and has size alloc_size. */
typedef struct {
    char *data;
    size_t alloc_size;
    size_t data_size;
    size_t offset;
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

/* The producer takes the given buffer (which is not itself NULL, but may contain
 * a NULL data segment), and fills it with data. The producer can allocate or
 * reallocate the buffer's 'data' element as necessary; the queueing system will
 * free it when necessary. The result of the production operation is specified in
 * the return value, but if the buffer is left without data, then that is
 * interpreted as PRODUCER_ERROR. For optimal performance, the producer should
 * supply exactly hint_size bytes of data in each call, but this is not required.
 *
 * The consumer is given a buffer (which will not be NULL, nor contain a NULL data
 * segment), and is expected to process some or all of the data in that buffer. If
 * there is a problem consuming data (such that no further data can be consumed),
 * the consumer may return -1. Otherwise, the consumer should return the number of
 * bytes actually consumed.  If an error occurs, it should return -1, regardless
 * of the number of bytes consumed.  The queueing framework will ensure that all
 * blocks have at least hint_size bytes, except the last.  For optimal
 * performance, the consumer should consume the entire buffer at each call, but
 * this is not required.
 *
 * Note that the handling of the queue_buffer_t is different between the two
 * functions: The producer should update queue_buffer_t as necessary to corespond
 * to read data, while the consumer should leave the queue_buffer_t unadjusted:
 * The queueing framework will invalidate data in the buffer according to the
 * return value of the consumer.*/

typedef producer_result_t (* ProducerFunctor)(gpointer user_data,
                                              queue_buffer_t* buffer,
                                              size_t hint_size);
typedef ssize_t (* ConsumerFunctor)(gpointer user_data,
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
                                size_t block_size,
                                size_t max_memory,
                                StreamingRequirement streaming_mode);

/* Some commonly-useful producers and consumers.*/

/* These functions will call read() or write() respectively. The user
   data should be a pointer to an queue_fd_t, with fd set to the device
   descriptor and errmsg set to NULL. */

typedef struct {
    int fd;
    char *errmsg;
} queue_fd_t;

queue_fd_t *queue_fd_new(int fd, char *errmsg);
int queue_fd_fd(queue_fd_t *queue_fd);
char *queue_fd_errmsg(queue_fd_t *queue_fd);

producer_result_t fd_read_producer(gpointer queue_fd, queue_buffer_t *buffer,
                                   size_t hint_size);
ssize_t fd_write_consumer(gpointer queue_fd, queue_buffer_t *buffer);



#endif /* QUEUEING_H */
