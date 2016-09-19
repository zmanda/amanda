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
 * shared memory ring buffer
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
#include <semaphore.h>
#include <glob.h>

#include "amanda.h"
#include "glib.h"
#include "conffile.h"
#include "security.h"
#include "shm-ring.h"

#define DEFAULT_SHM_RING_BLOCK_SIZE (NETWORK_BLOCK_BYTES)
#define DEFAULT_SHM_RING_SIZE (DEFAULT_SHM_RING_BLOCK_SIZE*8)

#define SHM_CONTROL_NAME "/amanda_shm_control-%d-%d"
#define SHM_DATA_NAME "/amanda_shm_data-%d-%d"
#define SEM_WRITE_NAME "/amanda_sem_write-%d-%d"
#define SEM_READ_NAME "/amanda_sem_read-%d-%d"
#define SEM_READY_NAME "/amanda_sem_ready-%d-%d"
#define SEM_START_NAME "/amanda_sem_start-%d-%d"

static int shm_ring_id = 0;
static GMutex *shm_ring_mutex = NULL;

static void alloc_shm_ring(shm_ring_t *shm_ring);

static int
get_next_shm_ring_id(void)
{
    int id;

    if (shm_ring_mutex == NULL) {
	shm_ring_mutex = g_mutex_new();
    }
    g_mutex_lock(shm_ring_mutex);
    id = shm_ring_id++;
    g_mutex_unlock(shm_ring_mutex);

    return id;
}

void
clean_shm_ring(void)
{
    if (shm_ring_mutex) {
	g_mutex_free(shm_ring_mutex);
    }
}

void
cleanup_shm_ring(void)
{
    glob_t globbuf;
    char **aglob;
    int r;
    time_t now = time(NULL) - 300;
    GHashTable *names;
    names = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);

    r = glob("/dev/shm/amanda_shm_control-*-*", GLOB_NOSORT, NULL, &globbuf);
    if (r == 0) {
	for (aglob = globbuf.gl_pathv; *aglob != NULL; aglob++) {
	    int cfd;

	    g_hash_table_insert(names, g_strdup(*aglob), GINT_TO_POINTER(1));
	    g_debug("cleanup_shm_ring: control_name: %s", *aglob);
	    cfd = shm_open(*aglob+8, O_RDONLY, 0);
	    if (cfd >= 0) {
		struct stat statbuf;
		if (fstat(cfd, &statbuf) == 0 &&
		    statbuf.st_atime < now &&
		    statbuf.st_mtime < now &&
		    statbuf.st_ctime < now &&
		    statbuf.st_size == sizeof(shm_ring_control_t)) {
		    shm_ring_control_t *mc;
		    mc = mmap(NULL, sizeof(shm_ring_control_t),
			      PROT_READ, MAP_SHARED, cfd, 0);
		    if (mc != MAP_FAILED) {
			gboolean all_dead = TRUE;
			int i;

			g_hash_table_insert(names, g_strdup(mc->sem_write_name), GINT_TO_POINTER(1));
			g_hash_table_insert(names, g_strdup(mc->sem_read_name), GINT_TO_POINTER(1));
			g_hash_table_insert(names, g_strdup(mc->sem_ready_name), GINT_TO_POINTER(1));
			g_hash_table_insert(names, g_strdup(mc->sem_start_name), GINT_TO_POINTER(1));
			g_hash_table_insert(names, g_strdup(mc->shm_data_name), GINT_TO_POINTER(1));

			for (i=0; i<SHM_RING_MAX_PID; i++) {
			    if (mc->pids[i] != 0) {
				if (kill(mc->pids[i], 0) == -1) {
				    if (errno != ESRCH) {
					all_dead = FALSE;
				    }
				} else {
				    all_dead = FALSE;
				}
			    }
			}
			// check all pids
			if (all_dead) {
			    g_debug("sem_unlink %s", mc->sem_write_name);
			    g_debug("sem_unlink %s", mc->sem_read_name);
			    g_debug("sem_unlink %s", mc->sem_ready_name);
			    g_debug("sem_unlink %s", mc->sem_start_name);
			    g_debug("shm_unlink %s", mc->shm_data_name);
			    sem_unlink(mc->sem_write_name);
			    sem_unlink(mc->sem_read_name);
			    sem_unlink(mc->sem_ready_name);
			    sem_unlink(mc->sem_start_name);
			    shm_unlink(mc->shm_data_name);
			    munmap(mc, sizeof(shm_ring_control_t));
			    g_debug("shm_unlink %s", *aglob+8);
			    shm_unlink(*aglob+8);
			} else {
			    munmap(mc, sizeof(shm_ring_control_t));
			}
		    } else {
			g_debug("mmap failed '%s': %s", *aglob+8, strerror(errno));
		    }
		} else {
		    g_debug("fstat failed '%s': %s", *aglob+8, strerror(errno));
		}
		close(cfd);
	    } else {
		g_debug("shm_open failed '%s': %s", *aglob+8, strerror(errno));
	    }
	}
    } else if (r == GLOB_NOSPACE) {
	g_debug("glob failed because no space");
    } else if (r == GLOB_ABORTED) {
	g_debug("glob aborted");
    } else if (r == GLOB_NOMATCH) {
	// good
    } else {
	// impossible
    }

    globfree(&globbuf);

    r = glob("/dev/shm/*amanda*", GLOB_NOSORT, NULL, &globbuf);
    if (r == 0) {
	int one_day_old = time(NULL) - 60*60*24;
	for (aglob = globbuf.gl_pathv; *aglob != NULL; aglob++) {
	    if (!g_hash_table_lookup(names, *aglob)) {
		struct stat statbuf;
		if (stat(*aglob, &statbuf) == 0 &&
		    statbuf.st_mtime < one_day_old) {
		    g_debug("unlink unknown old file: %s", *aglob);
		    unlink(*aglob);
		}
	    }
	}
    }

    globfree(&globbuf);
    g_hash_table_destroy(names);
}

int
shm_ring_sem_wait(
    shm_ring_t *shm_ring,
    sem_t      *sem)
{

    int i;
    while(1) {
	struct timespec tv = {time(NULL)+300, 0};

	if (sem_timedwait(sem, &tv) == 0)
	    return 0;

	if (shm_ring->mc->cancelled) {
	    g_debug("shm_ring_sem_wait: shm-ring is cancelled");
	    return -1;
	}

	if (shm_ring->mc->cancelled) {
	    g_debug("shm_ring_sem_wait: shm-ring is cancelled");
	    return -1;
	}

	if (errno == EINTR)
	    continue;

	if (errno != ETIMEDOUT) {
	    goto failed_sem_wait;
	}

	/* Check all pids */
	for (i=0; i<SHM_RING_MAX_PID; i++) {
	    if (shm_ring->mc->pids[i] != 0) {
		if (kill(shm_ring->mc->pids[i], 0) == -1) {
		    if (errno == ESRCH) {
			goto failed_sem_wait;
		    }
		}
	    }
	}
    }

failed_sem_wait:
    g_debug("shm_ring_sem_wait: failed_sem_wait: %s", strerror(errno));
    shm_ring->mc->cancelled = 1;
    sem_post(shm_ring->sem_read);
    sem_post(shm_ring->sem_write);
    sem_post(shm_ring->sem_ready);
    sem_post(shm_ring->sem_start);
    return -1;
}

void
fd_to_shm_ring(
    int fd,
    shm_ring_t *shm_ring,
    crc_t *crc)
{
    uint64_t write_offset;
    uint64_t written;
    uint64_t readx;
    uint64_t shm_ring_size;
    struct iovec iov[2];
    int          iov_count;
    ssize_t      n;
    size_t      consumer_block_size;

    g_debug("fd_to_shm_ring");

    shm_ring_size = shm_ring->mc->ring_size;
    consumer_block_size = shm_ring->mc->consumer_block_size;
    crc32_init(crc);

    while (!shm_ring->mc->cancelled) {
        write_offset = shm_ring->mc->write_offset;
        written = shm_ring->mc->written;
	while (!shm_ring->mc->cancelled) {
            readx = shm_ring->mc->readx;
	    if (shm_ring_size - (written - readx) >= shm_ring->block_size)
		break;
            if (shm_ring_sem_wait(shm_ring, shm_ring->sem_write) != 0) {
		break;
	    }
	}

	if (shm_ring->mc->cancelled)
	    break;

        iov[0].iov_base = shm_ring->data + write_offset;
        if (write_offset + shm_ring->block_size <= shm_ring_size) {
            iov[0].iov_len = shm_ring->block_size;
            iov_count = 1;
        } else {
            iov[0].iov_len = shm_ring_size - write_offset;
            iov[1].iov_base = shm_ring->data;
            iov[1].iov_len = shm_ring->block_size - iov[0].iov_len;
            iov_count = 2;
        }

        n = readv(fd, iov, iov_count);
        if (n > 0) {
	    if (shm_ring->mc->written == 0 && shm_ring->mc->need_sem_ready) {
		sem_post(shm_ring->sem_ready);
		if (shm_ring_sem_wait(shm_ring, shm_ring->sem_start) != 0) {
		    break;
		}
	    }
            write_offset += n;
            write_offset %= shm_ring_size;
            shm_ring->mc->write_offset = write_offset;
            shm_ring->mc->written += n;
            shm_ring->data_avail += n;
            if (shm_ring->data_avail >= consumer_block_size) {
                sem_post(shm_ring->sem_read);
                shm_ring->data_avail -= consumer_block_size;
            }
            if (n <= (ssize_t)iov[0].iov_len) {
                crc32_add((uint8_t *)iov[0].iov_base, n, crc);
            } else {
                crc32_add((uint8_t *)iov[0].iov_base, iov[0].iov_len, crc);
                crc32_add((uint8_t *)iov[1].iov_base, n - iov[0].iov_len, crc);
            }
        } else {
            shm_ring->mc->eof_flag = TRUE;
            break;
        }
    }

    sem_post(shm_ring->sem_read);
    sem_post(shm_ring->sem_read);

    // wait for the consumer to read everything
    while (!shm_ring->mc->cancelled &&
	   (shm_ring->mc->written != shm_ring->mc->readx ||
	    !shm_ring->mc->eof_flag)) {
	if (shm_ring_sem_wait(shm_ring, shm_ring->sem_write) != 0) {
	    break;
	}
    }
}

void
close_producer_shm_ring(
    shm_ring_t *shm_ring)
{
    if (!shm_ring->mc->eof_flag) {
	shm_ring->mc->eof_flag = TRUE;
    }
    sem_post(shm_ring->sem_ready);
    sem_post(shm_ring->sem_start);
    sem_post(shm_ring->sem_write);
    sem_post(shm_ring->sem_read);
    if (sem_close(shm_ring->sem_write) == -1) {
	g_debug("sem_close(sem_write) failed: %s", strerror(errno));
    }
    if (sem_close(shm_ring->sem_read) == -1) {
	g_debug("sem_close(sem_read) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_close(shm_ring->sem_ready) == -1) {
	g_debug("sem_close(sem_ready) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_close(shm_ring->sem_start) == -1) {
	g_debug("sem_close(sem_start) failed: %s", strerror(errno));
	exit(1);
    }
    if (shm_ring->shm_data_mmap_size > 0 && shm_ring->data) {
	if (munmap(shm_ring->data, shm_ring->shm_data_mmap_size) == -1) {;
	    g_debug("munmap(data) failed: %s", strerror(errno));
	    exit(0);
	}
    }
    if (munmap(shm_ring->mc, sizeof(shm_ring_control_t)) == -1) {
	g_debug("munmap(mc) failed: %s", strerror(errno));
	exit(1);
    }
    aclose(shm_ring->shm_data);
    aclose(shm_ring->shm_control);
    g_free(shm_ring->shm_control_name);
    g_free(shm_ring);
}

void
shm_ring_to_security_stream(
    shm_ring_t *shm_ring,
    struct security_stream_t *netfd,
    crc_t *crc)
{
    uint64_t     read_offset;
    uint64_t     shm_ring_size;
    gsize        usable = 0;
    gboolean     eof_flag = FALSE;

    g_debug("shm_ring_to_security_stream");
    shm_ring_size = shm_ring->mc->ring_size;

    sem_post(shm_ring->sem_write);
    while (!shm_ring->mc->cancelled) {
	do {
	    if (shm_ring_sem_wait(shm_ring, shm_ring->sem_read) != 0) {
		break;
	    }
	    usable = shm_ring->mc->written - shm_ring->mc->readx;
	    eof_flag = shm_ring->mc->eof_flag;
	} while (!shm_ring->mc->cancelled &&
		 usable < shm_ring->block_size && !eof_flag);
	read_offset = shm_ring->mc->read_offset;

	while (usable >= shm_ring->block_size || eof_flag) {
	    gsize to_write = usable;
	    if (to_write > shm_ring->block_size)
		to_write = shm_ring->block_size;

	    if (to_write + read_offset <= shm_ring_size) {
		security_stream_write(netfd, shm_ring->data + read_offset,
				      to_write);
		if (crc) {
		    crc32_add((uint8_t *)shm_ring->data + read_offset, to_write,
			      crc);
		}
	    } else {
		security_stream_write(netfd, shm_ring->data + read_offset,
				      shm_ring_size - read_offset);
		security_stream_write(netfd, shm_ring->data,
				      to_write - shm_ring_size + read_offset);
		if (crc) {
		    crc32_add((uint8_t *)shm_ring->data + read_offset, shm_ring_size - read_offset, crc);
		    crc32_add((uint8_t *)shm_ring->data, usable - shm_ring_size + read_offset, crc);
		}
	    }
	    if (to_write) {
		read_offset += to_write;
		if (read_offset >= shm_ring_size)
		    read_offset -= shm_ring_size;
		shm_ring->mc->read_offset = read_offset;
		shm_ring->mc->readx += to_write;
		sem_post(shm_ring->sem_write);
		usable -= to_write;
	    }
	    if (shm_ring->mc->write_offset == shm_ring->mc->read_offset &&
		shm_ring->mc->eof_flag) {
		// notify the producer that everything is read
		sem_post(shm_ring->sem_write);
		return;
	    }
	}
    }
}

void
shm_ring_to_fd(
    shm_ring_t *shm_ring,
    int fd,
    crc_t *crc)
{
    uint64_t     read_offset;
    uint64_t     shm_ring_size;
    gsize        usable = 0;
    gboolean     eof_flag = FALSE;

    g_debug("shm_ring_to_fd");
    shm_ring_size = shm_ring->mc->ring_size;

    sem_post(shm_ring->sem_write);
    while (!shm_ring->mc->cancelled) {
	do {
	    if (shm_ring_sem_wait(shm_ring, shm_ring->sem_read) != 0) {
		break;
	    }
	    usable = shm_ring->mc->written - shm_ring->mc->readx;
	    eof_flag = shm_ring->mc->eof_flag;
	} while (!shm_ring->mc->cancelled &&
		 usable < shm_ring->block_size && !eof_flag);
	read_offset = shm_ring->mc->read_offset;

	while (usable >= shm_ring->block_size || eof_flag) {
	    gsize to_write = usable;
	    if (to_write > shm_ring->block_size)
		to_write = shm_ring->block_size;

	    if (to_write + read_offset <= shm_ring_size) {
		if (full_write(fd, shm_ring->data + read_offset, to_write) != to_write) {
		    g_debug("full_write failed: %s", strerror(errno));
		    shm_ring->mc->cancelled = TRUE;
		    sem_post(shm_ring->sem_write);
		    return;
		}
		if (crc) {
		    crc32_add((uint8_t *)shm_ring->data + read_offset, to_write,
			      crc);
		}
	    } else {
		if (full_write(fd, shm_ring->data + read_offset,
			   shm_ring_size - read_offset) != shm_ring_size - read_offset) {
		    g_debug("full_write failed: %s", strerror(errno));
		    shm_ring->mc->cancelled = TRUE;
		    sem_post(shm_ring->sem_write);
		    return;
		}
		if (full_write(fd, shm_ring->data,
			   to_write - shm_ring_size + read_offset) != to_write - shm_ring_size + read_offset) {
		    g_debug("full_write failed: %s", strerror(errno));
		    shm_ring->mc->cancelled = TRUE;
		    sem_post(shm_ring->sem_write);
		    return;
		}
		if (crc) {
		    crc32_add((uint8_t *)shm_ring->data + read_offset, shm_ring_size - read_offset, crc);
		    crc32_add((uint8_t *)shm_ring->data, usable - shm_ring_size + read_offset, crc);
		}
	    }
	    if (to_write) {
		read_offset += to_write;
		if (read_offset >= shm_ring_size)
		    read_offset -= shm_ring_size;
		shm_ring->mc->read_offset = read_offset;
		shm_ring->mc->readx += to_write;
		sem_post(shm_ring->sem_write);
		usable -= to_write;
	    }
	    if (shm_ring->mc->write_offset == shm_ring->mc->read_offset &&
		shm_ring->mc->eof_flag) {
		// notify the producer that everythinng is read
		sem_post(shm_ring->sem_write);
		return;
	    }
	}
    }
}

void
shm_ring_producer_set_size(
    shm_ring_t *shm_ring,
    ssize_t      ring_size,
    ssize_t      block_size)
{

    g_debug("shm_ring_producer_set_size");
    shm_ring->ring_size = ring_size;
    shm_ring->block_size = block_size;
    shm_ring->mc->producer_ring_size = ring_size;
    shm_ring->mc->producer_block_size = block_size;

    if (shm_ring_sem_wait(shm_ring, shm_ring->sem_write) == -1) {
	exit(1);
    }

    alloc_shm_ring(shm_ring);

    if (ftruncate(shm_ring->shm_data, shm_ring->mc->ring_size) == -1) {
	g_debug("ftruncate of shm_data failed: %s", strerror(errno));
	exit(1);
    }
    shm_ring->shm_data_mmap_size = shm_ring->mc->ring_size;
    shm_ring->data = mmap(NULL, shm_ring->shm_data_mmap_size,
			   PROT_READ|PROT_WRITE, MAP_SHARED,
			   shm_ring->shm_data, 0);
    if (shm_ring->data == MAP_FAILED) {
	g_debug("shm_ring shm_ring->data failed: %s", strerror(errno));
	exit(1);
    }
    sem_post(shm_ring->sem_read);
}

static void
alloc_shm_ring(
    shm_ring_t *shm_ring)
{

    uint64_t best_ring_size;

    if (shm_ring->mc->producer_ring_size > shm_ring->mc->consumer_ring_size) {
	best_ring_size = shm_ring->mc->producer_ring_size;
	if (best_ring_size < shm_ring->mc->producer_block_size * 2)
	    best_ring_size = shm_ring->mc->producer_block_size * 2;
    } else {
	best_ring_size =  shm_ring->mc->consumer_ring_size;
	if (best_ring_size < shm_ring->mc->consumer_block_size * 2)
	    best_ring_size = shm_ring->mc->consumer_block_size * 2;
    }

    if (best_ring_size % shm_ring->mc->producer_block_size != 0) {
	best_ring_size = ((best_ring_size % shm_ring->mc->producer_block_size)+1) * shm_ring->mc->producer_block_size;
    }

    while (best_ring_size % shm_ring->mc->consumer_block_size != 0) {
	best_ring_size += shm_ring->mc->producer_block_size;
    }

    shm_ring->ring_size = best_ring_size;
    shm_ring->mc->ring_size = shm_ring->ring_size;
}

shm_ring_t *
shm_ring_create(void)
{
    shm_ring_t *shm_ring = g_new0(shm_ring_t, 1);

    g_debug("shm_ring_create");
    shm_ring->shm_control_name = g_strdup_printf(SHM_CONTROL_NAME, getpid(), get_next_shm_ring_id());
    shm_unlink(shm_ring->shm_control_name);
    shm_ring->shm_control = shm_open(shm_ring->shm_control_name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (shm_ring->shm_control == -1) {
	g_debug("shm_control failed '%s': %s", shm_ring->shm_control_name, strerror(errno));
	exit(1);
    }
    if (ftruncate(shm_ring->shm_control, sizeof(shm_ring_control_t)) == -1) {
	g_debug("ftruncate of shm_control failed '%s': %s", shm_ring->shm_control_name, strerror(errno));
	exit(1);
    }
    shm_ring->mc = mmap(NULL, sizeof(shm_ring_control_t), PROT_READ|PROT_WRITE,
			 MAP_SHARED, shm_ring->shm_control, 0);
    if (shm_ring->mc == MAP_FAILED) {
	g_debug("shm_ring shm_ring.mc failed '%s': %s", shm_ring->shm_control_name, strerror(errno));
	exit(1);
    }
    shm_ring->mc->write_offset = 0;
    shm_ring->mc->read_offset = 0;
    shm_ring->mc->eof_flag = FALSE;
    shm_ring->mc->pids[0] = getpid();;

    g_snprintf(shm_ring->mc->sem_write_name,
	       sizeof(shm_ring->mc->sem_write_name),
	       SEM_WRITE_NAME, getpid(), get_next_shm_ring_id());
    g_snprintf(shm_ring->mc->sem_read_name,
	       sizeof(shm_ring->mc->sem_read_name),
	       SEM_READ_NAME, getpid(), get_next_shm_ring_id());
    g_snprintf(shm_ring->mc->sem_ready_name,
	       sizeof(shm_ring->mc->sem_ready_name),
	       SEM_READY_NAME, getpid(), get_next_shm_ring_id());
    g_snprintf(shm_ring->mc->sem_start_name,
	       sizeof(shm_ring->mc->sem_start_name),
	       SEM_START_NAME, getpid(), get_next_shm_ring_id());
    g_snprintf(shm_ring->mc->shm_data_name,
	       sizeof(shm_ring->mc->shm_data_name),
	       SHM_DATA_NAME, getpid(), get_next_shm_ring_id());

    shm_unlink(shm_ring->mc->shm_data_name);
    shm_ring->shm_data = shm_open(shm_ring->mc->shm_data_name,
			O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
    if (shm_ring->shm_data == -1) {
	g_debug("shm_data failed '%s': %s", shm_ring->mc->shm_data_name,strerror(errno));
	exit(1);
    }
    sem_unlink(shm_ring->mc->sem_write_name);
    shm_ring->sem_write = sem_open(shm_ring->mc->sem_write_name,
				   O_CREAT | O_EXCL, S_IRUSR | S_IWUSR, 0);
    if (shm_ring->sem_write == SEM_FAILED) {
	g_debug("sem_open shm_ring->sem_write failed '%s': %s", shm_ring->mc->sem_write_name,strerror(errno));
	exit(1);
    }
    sem_unlink(shm_ring->mc->sem_read_name);
    shm_ring->sem_read  = sem_open(shm_ring->mc->sem_read_name,
				   O_CREAT | O_EXCL, S_IRUSR | S_IWUSR, 0);
    if (shm_ring->sem_read == SEM_FAILED) {
	g_debug("sem_open shm_ring->sem_read failed '%s': %s", shm_ring->mc->sem_read_name, strerror(errno));
	exit(1);
    }
    sem_unlink(shm_ring->mc->sem_ready_name);
    shm_ring->sem_ready  = sem_open(shm_ring->mc->sem_ready_name,
				   O_CREAT | O_EXCL, S_IRUSR | S_IWUSR, 0);
    if (shm_ring->sem_ready == SEM_FAILED) {
	g_debug("sem_open shm_ring->sem_ready failed '%s': %s", shm_ring->mc->sem_ready_name, strerror(errno));
	exit(1);
    }
    sem_unlink(shm_ring->mc->sem_start_name);
    shm_ring->sem_start  = sem_open(shm_ring->mc->sem_start_name,
				   O_CREAT | O_EXCL, S_IRUSR | S_IWUSR, 0);
    if (shm_ring->sem_start == SEM_FAILED) {
	g_debug("sem_open shm_ring->sem_start failed '%s': %s", shm_ring->mc->sem_start_name, strerror(errno));
	exit(1);
    }
    g_debug("shm_data: %s", shm_ring->mc->shm_data_name);
    g_debug("sem_write: %s", shm_ring->mc->sem_write_name);
    g_debug("sem_read: %s", shm_ring->mc->sem_read_name);
    g_debug("sem_ready: %s", shm_ring->mc->sem_ready_name);
    g_debug("sem_start: %s", shm_ring->mc->sem_start_name);

    return shm_ring;
}

void
shm_ring_consumer_set_size(
    shm_ring_t *shm_ring,
    ssize_t         ring_size,     /* shm_ring desired size */
    ssize_t         block_size)
{
    g_debug("shm_ring_consumer_set_size");

    shm_ring->ring_size = ring_size;
    shm_ring->block_size = block_size;
    shm_ring->mc->consumer_ring_size = ring_size;
    shm_ring->mc->consumer_block_size = block_size;
    sem_post(shm_ring->sem_write);
    if (shm_ring_sem_wait(shm_ring, shm_ring->sem_read) == -1) {
	g_debug("shm_ring_consumer_set_size: fail shm_ring_sem_wait");
	return;
    }
    if (shm_ring->mc->cancelled) {
	g_debug("shm_ring_consumer_set_size: cancelled");
	return;
    }

    if (shm_ring->mc->ring_size == 0) {
	g_debug("shm_ring_consumer_set_size: ring_size == 0");
	shm_ring->mc->cancelled = TRUE;
	sem_post(shm_ring->sem_read);
	sem_post(shm_ring->sem_write);
	sem_post(shm_ring->sem_ready);
	sem_post(shm_ring->sem_start);
	return;
    }
    shm_ring->ring_size = shm_ring->mc->ring_size;

    shm_ring->shm_data_mmap_size = shm_ring->mc->ring_size;
    shm_ring->data = mmap(NULL, shm_ring->shm_data_mmap_size,
			   PROT_READ|PROT_WRITE, MAP_SHARED,
			   shm_ring->shm_data, 0);
    if (shm_ring->data == MAP_FAILED) {
	g_debug("shm_ring shm_ring->data failed (%lld): %s", (long long)shm_ring->shm_data_mmap_size, strerror(errno));
	g_debug("shm_ring->ring_size %lld", (long long)shm_ring->ring_size);
	g_debug("shm_ring->block_size %lld", (long long)shm_ring->block_size);
	g_debug("shm_ring->mc->consumer_ring_size %lld", (long long)shm_ring->mc->consumer_ring_size);
	g_debug("shm_ring->mc->producer_ring_size %lld", (long long)shm_ring->mc->producer_ring_size);
	g_debug("shm_ring->mc->consumer_block_size %lld", (long long)shm_ring->mc->consumer_block_size);
	g_debug("shm_ring->mc->producer_block_size %lld", (long long)shm_ring->mc->producer_block_size);
	g_debug("shm_ring->mc->ring_size %lld", (long long)shm_ring->mc->ring_size);
	exit(1);
    }
}

shm_ring_t *
shm_ring_link(
    char *name)
{
    shm_ring_t *shm_ring = g_new0(shm_ring_t, 1);
    int i;

    g_debug("shm_ring_link %s", name);
    shm_ring->shm_control_name = g_strdup(name);
    shm_ring->shm_control = shm_open(shm_ring->shm_control_name, O_RDWR, S_IRUSR | S_IWUSR);
    if (shm_ring->shm_control == -1) {
	g_debug("shm_control failed '%s': %s", shm_ring->shm_control_name, strerror(errno));
	exit(1);
    }
    shm_ring->mc = mmap(NULL, sizeof(shm_ring_control_t), PROT_READ|PROT_WRITE, MAP_SHARED,
              shm_ring->shm_control, 0);
    if (shm_ring->mc == MAP_FAILED) {
	g_debug("shm_ring shm_ring.mc failed '%s': %s", shm_ring->shm_control_name, strerror(errno));
	exit(1);
    }
    shm_ring->shm_data = shm_open(shm_ring->mc->shm_data_name, O_RDWR, S_IRUSR | S_IWUSR);
    if (shm_ring->shm_data == -1) {
	g_debug("shm_data failed '%s': %s", shm_ring->mc->shm_data_name, strerror(errno));
	exit(1);
    }
    shm_ring->shm_data_mmap_size = 0;
    shm_ring->sem_write = sem_open(shm_ring->mc->sem_write_name, 0);
    if (shm_ring->sem_write == SEM_FAILED) {
	g_debug("sem_open sem_write failed '%s': %s", shm_ring->mc->sem_write_name, strerror(errno));
	exit(1);
    }
    shm_ring->sem_read  = sem_open(shm_ring->mc->sem_read_name, 0);
    if (shm_ring->sem_read == SEM_FAILED) {
	g_debug("sem_open sem_read failed '%s': %s", shm_ring->mc->sem_read_name, strerror(errno));
	exit(1);
    }
    shm_ring->sem_ready  = sem_open(shm_ring->mc->sem_ready_name, 0);
    if (shm_ring->sem_ready == SEM_FAILED) {
	g_debug("sem_open sem_ready failed '%s': %s", shm_ring->mc->sem_ready_name, strerror(errno));
	exit(1);
    }
    shm_ring->sem_start  = sem_open(shm_ring->mc->sem_start_name, 0);
    if (shm_ring->sem_start == SEM_FAILED) {
	g_debug("sem_open sem_start failed '%s': %s", shm_ring->mc->sem_start_name, strerror(errno));
	exit(1);
    }
    for (i=1; i < SHM_RING_MAX_PID; i++) {
	if (shm_ring->mc->pids[i] == 0) {
	    shm_ring->mc->pids[i] = getpid();
	    break;
	}
    }
    return shm_ring;
}

void
close_consumer_shm_ring(
    shm_ring_t *shm_ring)
{
    if (sem_close(shm_ring->sem_write) == -1) {
	g_debug("sem_close(sem_write) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_close(shm_ring->sem_read) == -1) {
	g_debug("sem_close(sem_read) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_close(shm_ring->sem_ready) == -1) {
	g_debug("sem_close(sem_ready) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_close(shm_ring->sem_start) == -1) {
	g_debug("sem_close(sem_start) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_unlink(shm_ring->mc->sem_write_name) == -1 && errno != ENOENT) {
	g_debug("sem_unlink(sem_write_name) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_unlink(shm_ring->mc->sem_read_name) == -1 && errno != ENOENT) {
	g_debug("sem_unlink(sem_read_name) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_unlink(shm_ring->mc->sem_ready_name) == -1 && errno != ENOENT) {
	g_debug("sem_unlink(sem_ready_name) failed: %s", strerror(errno));
	exit(1);
    }
    if (sem_unlink(shm_ring->mc->sem_start_name) == -1 && errno != ENOENT) {
	g_debug("sem_unlink(sem_start_name) failed: %s", strerror(errno));
	exit(1);
    }
    if (shm_ring->shm_data_mmap_size > 0 && shm_ring->data) {
	if (munmap(shm_ring->data, shm_ring->shm_data_mmap_size) == -1) {
	    g_debug("munmap(data) failed: %s", strerror(errno));
	    exit(1);
	}
    }
    if (shm_unlink(shm_ring->mc->shm_data_name) == -1 && errno != ENOENT) {
	g_debug("shm_unlink(shm_ring_data_name) failed: %s", strerror(errno));
	exit(1);
    }
    if (munmap(shm_ring->mc, sizeof(shm_ring_control_t)) == -1) {
	g_debug("munmap(mc) failed: %s", strerror(errno));
	exit(1);
    }
    if (shm_unlink(shm_ring->shm_control_name) == -1 && errno != ENOENT) {
	g_debug("shm_unlink(shm_ring_control_name) failed: %s", strerror(errno));
	exit(1);
    }
    aclose(shm_ring->shm_data);
    aclose(shm_ring->shm_control);
    g_free(shm_ring->shm_control_name);
    g_free(shm_ring);
}
