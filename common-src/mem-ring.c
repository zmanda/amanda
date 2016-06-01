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
 * memory ring buffer
 */

#include <config.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <glib.h>

#include "amanda.h"
#include "glib.h"
#include "conffile.h"
#include "mem-ring.h"

#define DEFAULT_MEM_RING_BLOCK_SIZE (NETWORK_BLOCK_BYTES)
#define DEFAULT_MEM_RING_SIZE (DEFAULT_MEM_RING_BLOCK_SIZE*8)

static void alloc_mem_ring(mem_ring_t *mem_ring);

mem_ring_t *
create_mem_ring(void)
{
    mem_ring_t *mem_ring = g_new0(mem_ring_t, 1);

    mem_ring->mutex = g_mutex_new();
    mem_ring->add_cond = g_cond_new();
    mem_ring->free_cond = g_cond_new();
    mem_ring->write_offset = 0;
    mem_ring->written = 0;
    mem_ring->read_offset = 0;
    mem_ring->readx = 0;
    mem_ring->eof_flag = FALSE;

    return mem_ring;
}

void
mem_ring_producer_set_size(
    mem_ring_t *mem_ring,
    size_t ring_size,
    size_t block_size)
{
    g_mutex_lock(mem_ring->mutex);
    mem_ring->producer_block_size = block_size;
    mem_ring->producer_ring_size = ring_size;
    while (mem_ring->consumer_block_size == 0 ||
	   mem_ring->consumer_ring_size == 0) {
	g_cond_wait(mem_ring->add_cond, mem_ring->mutex);
    }
    alloc_mem_ring(mem_ring);
    g_cond_broadcast(mem_ring->free_cond);
    g_mutex_unlock(mem_ring->mutex);
}

void
mem_ring_consumer_set_size(
    mem_ring_t *mem_ring,
    size_t ring_size,
    size_t block_size)
{
    g_mutex_lock(mem_ring->mutex);
    mem_ring->consumer_block_size = block_size;
    mem_ring->consumer_ring_size = ring_size;
    g_cond_broadcast(mem_ring->add_cond);
    g_cond_wait(mem_ring->free_cond, mem_ring->mutex);
    g_mutex_unlock(mem_ring->mutex);
}

void init_mem_ring(
    mem_ring_t *mem_ring,
    size_t ring_size,
    size_t block_size)
{
    g_mutex_lock(mem_ring->mutex);
    mem_ring->consumer_block_size = block_size;
    mem_ring->producer_block_size = block_size;
    mem_ring->consumer_ring_size = ring_size;
    mem_ring->producer_ring_size = ring_size;
    mem_ring->ring_size = ring_size;
    alloc_mem_ring(mem_ring);
    g_mutex_unlock(mem_ring->mutex);
}

static void
alloc_mem_ring(
    mem_ring_t *mem_ring)
{
    uint64_t best_ring_size;

    if (mem_ring->producer_ring_size > mem_ring->consumer_ring_size) {
	best_ring_size = mem_ring->producer_ring_size;
	if (best_ring_size < mem_ring->producer_block_size * 2)
	    best_ring_size = mem_ring->producer_block_size * 2;
    } else {
	best_ring_size =  mem_ring->consumer_ring_size;
	if (best_ring_size < mem_ring->consumer_block_size * 2)
	    best_ring_size = mem_ring->consumer_block_size * 2;
    }

    if (best_ring_size % mem_ring->producer_block_size != 0) {
        best_ring_size = ((best_ring_size % mem_ring->producer_block_size)+1) * mem_ring->producer_block_size;
    }

    while (best_ring_size % mem_ring->consumer_block_size != 0) {
        best_ring_size += mem_ring->producer_block_size;
    }

    mem_ring->ring_size = best_ring_size;
    mem_ring->buffer = malloc(mem_ring->ring_size);
}

void
close_mem_ring(
    mem_ring_t *mem_ring)
{
    g_mutex_free(mem_ring->mutex);
    g_cond_free(mem_ring->add_cond);
    g_cond_free(mem_ring->free_cond);
    g_free(mem_ring->buffer);
    g_free(mem_ring);
}
