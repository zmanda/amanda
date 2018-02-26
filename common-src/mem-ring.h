/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2016-2016 Carbonite, Inc.  All Rights Reserved.
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
*/

/*
 *
 * memory ring buffer
 */

#ifndef MEMRING_H
#define MEMRING_H

#include <glib.h>
#include <stream.h>

typedef struct mem_ring_s {
    uint64_t write_offset;	/* where to write */
    uint64_t written;		/* nb bytes written to the ring */
    gboolean eof_flag;
    char     padding1[256 - 2*sizeof(off_t) - sizeof(gboolean)];
    uint64_t read_offset;	/* where to read */
    uint64_t readx;		/* nb bytes written to the ring */
    char     padding2[256 - 2*sizeof(off_t)];
    char    *buffer;
    uint64_t ring_size;
    GCond   *add_cond;		/* some data was added to the ring */
    GCond   *free_cond;		/* some data was freed from the ring */
    GMutex  *mutex;
    size_t   consumer_block_size;
    size_t   producer_block_size;
    uint64_t consumer_ring_size;
    uint64_t producer_ring_size;
    size_t   data_avail;
} mem_ring_t;

mem_ring_t *create_mem_ring(void);
void init_mem_ring(mem_ring_t *mem_ring, size_t ring_size, size_t block_size);
void mem_ring_consumer_set_size(mem_ring_t *mem_ring, size_t ring_size, size_t block_size);
void mem_ring_producer_set_size(mem_ring_t *mem_ring, size_t ring_size, size_t block_size);
void close_mem_ring(mem_ring_t *mem_ring);

#endif
