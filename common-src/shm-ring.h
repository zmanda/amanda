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
 * shared memory ring buffer
 */

#ifndef SHM_RING_H
#define SHM_RING_H

#include <sys/mman.h>
#include <sys/stat.h>
#include <glib.h>
#include <semaphore.h>
#include "conffile.h"

#define SHM_RING_BLOCK_SIZE NETWORK_BLOCK_BYTES
#define SHM_RING_SIZE (SHM_RING_BLOCK_SIZE * 32)
#define SHM_RING_NAME_LENGTH 50
#define SHM_RING_MAX_PID 10

typedef struct shm_ring_control_t {
    uint64_t write_offset;
    uint64_t written;
    gboolean eof_flag;
    char     padding1[64 - 2*sizeof(uint64_t) - sizeof(gboolean)];
    uint64_t read_offset;
    uint64_t readx;
    char     padding2[64 - 2*sizeof(uint64_t)];
    gboolean cancelled;
    gboolean need_sem_ready;
    uint64_t ring_size;
    pid_t    pids[SHM_RING_MAX_PID];
    char     sem_write_name[SHM_RING_NAME_LENGTH];
    char     sem_read_name[SHM_RING_NAME_LENGTH];
    char     sem_ready_name[SHM_RING_NAME_LENGTH];
    char     sem_start_name[SHM_RING_NAME_LENGTH];
    char     shm_data_name[SHM_RING_NAME_LENGTH];
    size_t   consumer_block_size;
    size_t   producer_block_size;
    uint64_t consumer_ring_size;
    uint64_t producer_ring_size;
} shm_ring_control_t;

typedef struct shm_ring_t {
    shm_ring_control_t *mc;
    int             shm_control;
    int             shm_data;
    off_t	    shm_data_mmap_size;
    sem_t          *sem_write;
    sem_t          *sem_read;
    sem_t          *sem_ready;
    sem_t          *sem_start;
    char           *data;
    char           *data2;
    char           *shm_control_name;
    size_t         ring_size;	/* shm_ring desired size */
    size_t         block_size;
    size_t         data_avail;
} shm_ring_t;

#include "security.h"
#include "stream.h"

extern GMutex *shm_ring_mutex;

int shm_ring_sem_wait(shm_ring_t *shm_ring, sem_t *sem);
shm_ring_t *shm_ring_create(char **errmsg);
shm_ring_t *shm_ring_link(char *name);
void shm_ring_to_security_stream(shm_ring_t *shm_ring, struct security_stream_t *netfd, crc_t *crc);
void shm_ring_consumer_set_size(shm_ring_t *shm_ring, ssize_t ring_size, ssize_t block_size);
void shm_ring_producer_set_size(shm_ring_t *shm_ring, ssize_t ring_size, ssize_t block_size);

void close_producer_shm_ring(shm_ring_t *shm_ring);
void close_consumer_shm_ring(shm_ring_t *shm_ring);
void clean_shm_ring(void);
void cleanup_shm_ring(void);
void fd_to_shm_ring(int fd, shm_ring_t *shm_ring, crc_t *crc);
void shm_ring_to_fd(shm_ring_t *shm_ring, int fd, crc_t *crc);

#endif
