/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "amutil.h"
#include "xfer-server.h"
#include "xfer-device.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_holding() references
 * it directly.
 */

GType xfer_source_holding_get_type(void);
#define XFER_SOURCE_HOLDING_TYPE (xfer_source_holding_get_type())
#define XFER_SOURCE_HOLDING(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_holding_get_type(), XferSourceHolding)
#define XFER_SOURCE_HOLDING_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_holding_get_type(), XferSourceHolding const)
#define XFER_SOURCE_HOLDING_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_holding_get_type(), XferSourceHoldingClass)
#define IS_XFER_SOURCE_HOLDING(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_holding_get_type ())
#define XFER_SOURCE_HOLDING_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_holding_get_type(), XferSourceHoldingClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceHolding {
    XferElement __parent__;

    /* this mutex in this condition variable governs all variables below */
    GCond  *start_recovery_cond;
    GMutex *start_recovery_mutex;

    int fd;
    char *first_filename;
    char *next_filename;
    guint64 bytes_read;
    gint64 current_offset;
    gint64 offset_file;
    off_t fsize;
    gboolean paused;

    GThread *holding_thread;
    GMutex     *state_mutex;
    GCond      *state_cond;

    mem_ring_t *mem_ring;
    gboolean mem_ring_ready;

    XferElement *dest_taper;
} XferSourceHolding;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;

    void (*start_recovery)(XferSourceHolding *self);
} XferSourceHoldingClass;

static gboolean start_new_chunk(XferSourceHolding *self);

/*
 * Implementation
 */

#define HOLDING_BLOCK_BYTES DISK_BLOCK_BYTES

/*
 * Debug logging
 */

#define DBG(LEVEL, ...) if (debug_chunker >= LEVEL) { _xsh_dbg(__VA_ARGS__); }
static void
_xsh_dbg(const char *fmt, ...)
{
    va_list argp;
    gchar *msg;

    arglist_start(argp, fmt);
    msg = g_strdup_vprintf(fmt, argp);
    arglist_end(argp);
    g_debug("XSH: %s", msg);
    g_free(msg);
}


static gpointer
holding_thread(
    gpointer data)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(data);
    XferElement *elt = XFER_ELEMENT(self);
    XMsg *msg;
    GTimer *timer = g_timer_new();
    uint64_t write_offset;
    uint64_t readx;
    uint64_t written;
    uint64_t producer_block_size;
    uint64_t consumer_block_size;
    uint64_t mem_ring_size;
    ssize_t  to_read_size;
    size_t   bytes_read;

    DBG(1, "(this is the holding thread)");

    g_mutex_lock(self->start_recovery_mutex);
    g_mutex_lock(self->state_mutex);
    self->mem_ring = create_mem_ring();
    self->mem_ring_ready = TRUE;
    g_cond_broadcast(self->state_cond);
    g_mutex_unlock(self->state_mutex);
    mem_ring_producer_set_size(self->mem_ring, HOLDING_BLOCK_BYTES*32, HOLDING_BLOCK_BYTES);
    mem_ring_size = self->mem_ring->ring_size;
    producer_block_size = self->mem_ring->producer_block_size;
    consumer_block_size = self->mem_ring->consumer_block_size;

    g_mutex_lock(self->state_mutex);
    while (1) {
	g_mutex_lock(self->mem_ring->mutex);
	write_offset = self->mem_ring->write_offset;
	written = self->mem_ring->written;
        readx = self->mem_ring->readx;

	// wait for mem_ring space;
	while (mem_ring_size - (written - readx) < producer_block_size) {
	    if (elt->cancelled) {
		g_mutex_unlock(self->mem_ring->mutex);
		goto return_eof;
	    }
	    g_cond_wait(self->mem_ring->free_cond, self->mem_ring->mutex);
	    write_offset = self->mem_ring->write_offset;
	    written = self->mem_ring->written;
            readx = self->mem_ring->readx;
	}
	g_mutex_unlock(self->mem_ring->mutex);

	if (self->fd == -1) {
	   if (!start_new_chunk(self))
		goto return_eof;
	}

	while (self->paused && !elt->cancelled)
	    g_cond_wait(self->start_recovery_cond, self->start_recovery_mutex);
	if (elt->cancelled) {
	    goto return_eof;
	}

	//read to mem ring;
	to_read_size = MIN(HOLDING_BLOCK_BYTES, self->mem_ring->ring_size - write_offset);
	bytes_read = read_fully(self->fd, self->mem_ring->buffer + write_offset, to_read_size, NULL);
	if (bytes_read > 0) {
	    if (elt->size >= 0 && bytes_read > (guint64)elt->size) {
		bytes_read = elt->size;
	    }
	    elt->size -= bytes_read;
	    elt->offset += bytes_read;
	    self->current_offset += bytes_read;
	    self->bytes_read += bytes_read;
	    crc32_add((uint8_t *)self->mem_ring->buffer + self->mem_ring->write_offset, bytes_read, &elt->crc);
	    write_offset += bytes_read;
	    write_offset %= mem_ring_size;
	    g_mutex_lock(self->mem_ring->mutex);
	    self->mem_ring->data_avail += bytes_read;
	    self->mem_ring->written += bytes_read;
	    self->mem_ring->write_offset = write_offset;
	    if (self->mem_ring->data_avail >= consumer_block_size) {
		g_cond_broadcast(self->mem_ring->add_cond);
		self->mem_ring->data_avail -= consumer_block_size;
	    }
	    g_mutex_unlock(self->mem_ring->mutex);
	} else {
	    if (errno != 0) {
		xfer_cancel_with_error(XFER_ELEMENT(self),
			"while reading holding file: %s", strerror(errno));
		wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
		goto return_eof;
	    }

	    if (!start_new_chunk(self))
		goto return_eof;

	}
    }

return_eof:
    g_mutex_unlock(self->state_mutex);

    /* send an EOF indication downstream */
    g_mutex_lock(self->mem_ring->mutex);
    self->mem_ring->eof_flag = TRUE;
    g_cond_broadcast(self->mem_ring->add_cond);
    g_mutex_unlock(self->mem_ring->mutex);

    g_debug("sending XMSG_CRC message");
    g_debug("xfer-source-holding CRC: %08x     size: %lld",
            crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(XFER_ELEMENT(self), XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    g_debug("xfer-source-holding sending XMSG_DONE message");
    msg = xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0);
    msg->duration = g_timer_elapsed(timer, NULL);
    g_timer_destroy(timer);
    /* tell the main thread we're done */
    xfer_queue_message(elt->xfer, msg);

    g_mutex_unlock(self->start_recovery_mutex);

    return NULL;
}

static gboolean
start_new_chunk(
    XferSourceHolding *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    char *hdrbuf = NULL;
    dumpfile_t hdr;
    size_t bytes_read;
    struct stat finfo;
    gboolean seek_done = FALSE;

    while (!seek_done &&
	   (self->fd == -1 ||
	    elt->offset < self->offset_file ||
	    elt->offset >= self->offset_file + self->fsize)) {

	/* open a new file if the offset is not in the current file */
	if (self->fd != -1 &&
	    (elt->offset < self->offset_file ||
	     elt->offset >= self->offset_file + self->fsize)) {
	    if (close(self->fd) < 0) {
		xfer_cancel_with_error(XFER_ELEMENT(self),
			"while closing holding file: %s", strerror(errno));
		wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
		return FALSE;
	    }
	    self->fd = -1;
	}

	if (elt->offset < self->offset_file || self->offset_file == -1) {
	    self->current_offset = 0;
	    self->offset_file = 0;
	    self->fsize = 0;
	    g_free(self->next_filename);
	    self->next_filename = g_strdup(self->first_filename);
	}

	if (self->fd == -1) {
	    /* if we have no next filename, then we're at EOF */
	    if (!self->next_filename) {
		g_debug("no next_filename");
		return FALSE;
	    }

	    /* otherwise, open up the next file */
	    self->fd = open(self->next_filename, O_RDONLY);
	    if (self->fd < 0) {
		xfer_cancel_with_error(XFER_ELEMENT(self),
			"while opening holding file '%s': %s",
			self->next_filename, strerror(errno));
		wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
		return FALSE;
	    }

	}


	/* get a downstream XferDestTaper, if one exists.  This check happens
	 * for each chunk, but chunks are large, so that's OK. */
	if (!self->dest_taper) {
	    XferElement *elt = (XferElement *)self;

	    /* the xfer may have inserted glue between this element and
	     * the XferDestTaper. Glue does not change the bytestream, so
	     * it does not interfere with cache_inform calls. */
	    XferElement *iter = elt->downstream;
	    while (iter && IS_XFER_ELEMENT_GLUE(iter)) {
		iter = iter->downstream;
	    }
	    if (IS_XFER_DEST_TAPER(iter))
		self->dest_taper = iter;
        }

	/* tell a XferDestTaper about the new file */
	if (self->dest_taper) {
	    struct stat st;
	    if (fstat(self->fd, &st) < 0) {
		xfer_cancel_with_error(XFER_ELEMENT(self),
		    "while finding size of holding file '%s': %s",
		    self->next_filename, strerror(errno));
		wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
		return FALSE;
	    }

	    xfer_dest_taper_cache_inform(self->dest_taper,
		self->next_filename,
		DISK_BLOCK_BYTES,
		st.st_size - DISK_BLOCK_BYTES);
	}

	/* read the header from the file and determine the size and
	 * filename of the next chunk
	 */
	hdrbuf = g_malloc(DISK_BLOCK_BYTES);
	bytes_read = read_fully(self->fd, hdrbuf, DISK_BLOCK_BYTES, NULL);
	if (bytes_read < DISK_BLOCK_BYTES) {
	    g_free(hdrbuf);
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"while reading header from holding file '%s': %s",
		self->next_filename, strerror(errno));
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    return FALSE;
	}

	parse_file_header(hdrbuf, &hdr, DISK_BLOCK_BYTES);
	g_free(hdrbuf);
	hdrbuf = NULL;

	if (hdr.type != F_DUMPFILE && hdr.type != F_CONT_DUMPFILE) {
	    if (hdr.type == F_SPLIT_DUMPFILE) {
		g_debug("Reading a SPLIT_DUMPFILE) from holding disk");
	    } else {
		dumpfile_free_data(&hdr);
		xfer_cancel_with_error(XFER_ELEMENT(self),
			"unexpected header type %d in holding file '%s'",
			hdr.type, self->next_filename);
		wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
		return FALSE;
	    }
	}

	if (fstat(self->fd, &finfo) == -1) {
	    dumpfile_free_data(&hdr);
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"Can't stat holding file '%s': %s",
		self->next_filename, strerror(errno));
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    return FALSE;
	}

	self->current_offset = self->offset_file += self->fsize;	/* fsize of previous chunk */
	self->fsize = finfo.st_size - DISK_BLOCK_BYTES;

	g_free(self->next_filename);
	if (hdr.cont_filename[0]) {
	    self->next_filename = g_strdup(hdr.cont_filename);
	} else {
	    self->next_filename = NULL;
	}
	dumpfile_free_data(&hdr);
    };

    if (lseek(self->fd, elt->offset - self->offset_file + DISK_BLOCK_BYTES, SEEK_SET) == -1) {
	xfer_cancel_with_error(XFER_ELEMENT(self),
		"Can't lseek holding file '%s': %s",
		self->next_filename, strerror(errno));
	wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	return FALSE;
    }
    self->current_offset = elt->offset;

    return TRUE;
}

/* pick an arbitrary block size for reading */
#define HOLDING_BLOCK_SIZE (1024*128)

static mem_ring_t *
get_mem_ring_impl(
    XferElement *elt)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(elt);

    g_mutex_lock(self->state_mutex);
    while (!self->mem_ring_ready) {
	g_cond_wait(self->state_cond, self->state_mutex);
    }
    g_mutex_unlock(self->state_mutex);

    return self->mem_ring;
}

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(elt);
    XMsg *msg;
    char *buf = NULL;
    size_t bytes_read;

    g_mutex_lock(self->start_recovery_mutex);

    if (elt->cancelled)
	goto return_eof;

    if (elt->size == 0) {
	if (elt->offset == 0 && elt->orig_size == 0) {
	    self->paused = TRUE;
	} else {
	    g_debug("pull_buffer hit EOF; sending XMSG_SEGMENT_DONE");
	    msg = xmsg_new(XFER_ELEMENT(self), XMSG_SEGMENT_DONE, 0);
	    msg->successful = TRUE;
	    msg->eof = FALSE;

	    self->paused = TRUE;
	    xfer_queue_message(elt->xfer, msg);
	}
    }

    if (self->fd == -1) {
	if (!start_new_chunk(self))
	    goto return_eof;
    }

    buf = g_malloc(HOLDING_BLOCK_SIZE);

    if (elt->offset == 0 && elt->orig_size == 0) {
    }

    while (1) {
	while (self->paused && !elt->cancelled)
	   g_cond_wait(self->start_recovery_cond, self->start_recovery_mutex);
	if (elt->cancelled) {
	    goto return_eof;
	}

	bytes_read = read_fully(self->fd, buf, HOLDING_BLOCK_SIZE, NULL);
	if (bytes_read > 0) {
	    if (elt->size >= 0 && bytes_read > (guint64)elt->size) {
		bytes_read = elt->size;
	    }
	    elt->size -= bytes_read;
	    elt->offset += bytes_read;
	    self->current_offset += bytes_read;
	    *size = bytes_read;
	    self->bytes_read += bytes_read;
	    crc32_add((uint8_t *)buf, bytes_read, &elt->crc);
	    g_mutex_unlock(self->start_recovery_mutex);
	    return buf;
	}

	/* did an error occur? */
	if (errno != 0) {
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"while reading holding file: %s", strerror(errno));
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    goto return_eof;
	}

	if (!start_new_chunk(self))
	    goto return_eof;
    }

return_eof:
    g_debug("sending XMSG_CRC message");
    g_debug("xfer-source-holding CRC: %08x     size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(XFER_ELEMENT(self), XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    g_mutex_unlock(self->start_recovery_mutex);
    g_free(buf);
    *size = 0;
    return NULL;
}

static gpointer
pull_buffer_static_impl(
    XferElement *elt,
    gpointer buf,
    size_t block_size,
    size_t *size)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(elt);
    XMsg *msg;
    size_t bytes_read;
    size_t to_read_size;

    g_mutex_lock(self->start_recovery_mutex);

    if (elt->cancelled)
	goto return_eof;

    if (elt->size == 0) {
	if (elt->offset == 0 && elt->orig_size == 0) {
	    self->paused = TRUE;
	} else {
	    g_debug("pull_buffer_static hit EOF; sending XMSG_SEGMENT_DONE");
	    msg = xmsg_new(XFER_ELEMENT(self), XMSG_SEGMENT_DONE, 0);
	    msg->successful = TRUE;
	    msg->eof = FALSE;

	    self->paused = TRUE;
	    xfer_queue_message(elt->xfer, msg);
	}
    }

    if (self->fd == -1) {
	if (!start_new_chunk(self))
	    goto return_eof;
    }

    if (elt->offset == 0 && elt->orig_size == 0) {
    }

    while (1) {
	while (self->paused && !elt->cancelled)
	   g_cond_wait(self->start_recovery_cond, self->start_recovery_mutex);
	if (elt->cancelled) {
	    goto return_eof;
	}

	to_read_size = MIN(block_size, HOLDING_BLOCK_SIZE);
	bytes_read = read_fully(self->fd, buf, to_read_size, NULL);
	if (bytes_read > 0) {
	    if (elt->size >= 0 && bytes_read > (guint64)elt->size) {
		bytes_read = elt->size;
	    }
	    elt->size -= bytes_read;
	    elt->offset += bytes_read;
	    self->current_offset += bytes_read;
	    *size = bytes_read;
	    self->bytes_read += bytes_read;
	    crc32_add((uint8_t *)buf, bytes_read, &elt->crc);
	    g_mutex_unlock(self->start_recovery_mutex);
	    return buf;
	}

	/* did an error occur? */
	if (errno != 0) {
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"while reading holding file: %s", strerror(errno));
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    goto return_eof;
	}

	if (!start_new_chunk(self))
	    goto return_eof;
    }

return_eof:
    g_debug("sending XMSG_CRC message");
    g_debug("xfer-source-holding CRC: %08x     size %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(XFER_ELEMENT(self), XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    g_mutex_unlock(self->start_recovery_mutex);
    *size = 0;
    return NULL;
}

static gboolean
start_impl(
    XferElement *elt)
{
    XferSourceHolding *self = (XferSourceHolding *)elt;
    GError *error = NULL;

    if (elt->output_mech == XFER_MECH_MEM_RING) {
	self->holding_thread = g_thread_create(holding_thread, (gpointer)self, FALSE, &error);
	if (!self->holding_thread) {
            g_critical(_("Error creating new thread: %s (%s)"),
	            error->message, errno? strerror(errno) : _("no error code"));
	}
	return TRUE;
    }

    return FALSE;
}

static gboolean
cancel_impl(
    XferElement *elt,
    gboolean expect_eof G_GNUC_UNUSED)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(elt);
    elt->cancelled = TRUE;

    if (elt->shm_ring) {
	elt->shm_ring->mc->cancelled = TRUE;
	sem_post(elt->shm_ring->sem_ready);
	sem_post(elt->shm_ring->sem_start);
	sem_post(elt->shm_ring->sem_read);
	sem_post(elt->shm_ring->sem_write);
    }

    if (self->mem_ring) {
	g_mutex_lock(self->mem_ring->mutex);
	self->mem_ring->eof_flag = TRUE;
	g_cond_broadcast(self->mem_ring->add_cond);
        g_cond_broadcast(self->mem_ring->free_cond);
        g_mutex_unlock(self->mem_ring->mutex);
    }

    /* trigger the condition variable, in case the thread is waiting on it */
    g_mutex_lock(self->start_recovery_mutex);
    g_cond_broadcast(self->start_recovery_cond);
    g_mutex_unlock(self->start_recovery_mutex);

    return TRUE;
}

static void
start_recovery_impl(
    XferSourceHolding *self)
{
    g_debug("start_recovery called");

    g_mutex_lock(self->start_recovery_mutex);
    if (!start_new_chunk(self)) {
	// MUST CANCEL
	g_debug("start_new_chunk failed");
	g_mutex_unlock(self->start_recovery_mutex);
	return;
    }
    self->paused = FALSE;
    g_cond_broadcast(self->start_recovery_cond);
    g_mutex_unlock(self->start_recovery_mutex);
}

static void
instance_init(
    XferElement *elt)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(elt);

    self->state_mutex = g_mutex_new();
    self->state_cond = g_cond_new();

    elt->can_generate_eof = TRUE;
    self->fd = -1;
    self->paused = TRUE;
    self->current_offset = 0;
    self->offset_file = -1;
    self->fsize = -1;
    self->start_recovery_cond = g_cond_new();
    self->start_recovery_mutex = g_mutex_new();
    crc32_init(&elt->crc);
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(obj_self);

    g_mutex_lock(self->start_recovery_mutex);
    g_mutex_free(self->state_mutex);
    g_cond_free(self->state_cond);

    if (self->first_filename)
	g_free(self->first_filename);
    if (self->next_filename)
	g_free(self->next_filename);

    g_cond_free(self->start_recovery_cond);
    g_mutex_unlock(self->start_recovery_mutex);
    g_mutex_free(self->start_recovery_mutex);
    if (self->fd != -1)
	close(self->fd); /* ignore error; we were probably already cancelled */

    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferSourceHoldingClass * xsh_klass)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(xsh_klass);
    GObjectClass *goc = G_OBJECT_CLASS(xsh_klass);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER_STATIC, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) },
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(1) },
	{ XFER_MECH_NONE, XFER_MECH_MEM_RING, XFER_NROPS(1), XFER_NTHREADS(1), XFER_NALLOC(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(1), XFER_NTHREADS(0), XFER_NALLOC(0) }
    };

    klass->get_mem_ring = get_mem_ring_impl;
    klass->pull_buffer = pull_buffer_impl;
    klass->pull_buffer_static = pull_buffer_static_impl;
    klass->start = start_impl;
    klass->cancel = cancel_impl;
    klass->perl_class = "Amanda::Xfer::Source::Holding";
    klass->mech_pairs = mech_pairs;

    xsh_klass->start_recovery = start_recovery_impl;

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(xsh_klass);
}

GType
xfer_source_holding_get_type (void)
{
    static GType type = 0;

    if (G_UNLIKELY(type == 0)) {
        static const GTypeInfo info = {
            sizeof (XferSourceHoldingClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceHolding),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceHolding", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_holding(
    const char *filename)
{
    XferSourceHolding *self = (XferSourceHolding *)g_object_new(XFER_SOURCE_HOLDING_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    self->first_filename = g_strdup(filename);
    self->next_filename = g_strdup(filename);
    self->bytes_read = 0;

    return elt;
}

void
xfer_source_holding_start_recovery(
    XferElement *elt)
{
    XferSourceHoldingClass *klass;
    g_assert(IS_XFER_SOURCE_HOLDING(elt));

    klass = XFER_SOURCE_HOLDING_GET_CLASS(elt);
    klass->start_recovery(XFER_SOURCE_HOLDING(elt));
}

guint64
xfer_source_holding_get_bytes_read(
    XferElement *elt)
{
    XferSourceHolding *self = XFER_SOURCE_HOLDING(elt);

    return self->bytes_read;
}

