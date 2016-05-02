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
#include "fileheader.h"
#include "amxfer.h"
#include "xfer-server.h"
#include "conffile.h"
#include "holding.h"

/* A transfer destination that writes an entire dumpfile to one or more files
 * on one or more holding disks */

/* installcheck will fail if HOLDING_BLOCK_BYTES is increased because the
 * fake ENOSPC will no be on the same byte */
#define HEADER_BLOCK_BYTES  DISK_BLOCK_BYTES
#define HOLDING_BLOCK_BYTES DISK_BLOCK_BYTES

/*
 * Xfer Dest Holding
 */

static GObjectClass *parent_class = NULL;

typedef struct XferDestHolding {
    XferElement __parent__;

    /* object parameters
     *
     * These values are supplied to the constructor, and can be assumed
     * constant for the lifetime of the element.
     */

    char       *first_filename;
    /* The thread doing the actual writes to tape; this also handles buffering
     * for streaming */
    GThread *holding_thread;

    /* Ring Buffer
     *
     * This buffer holds MAX_MEMORY bytes of data (rounded up to the next
     * blocksize), and serves as the interface between the holding_thread and
     * the thread calling push_buffer.  Ring_length is the total length of the
     * buffer in bytes, while ring_count is the number of data bytes currently
     * in the buffer.  The ring_add_cond is signalled when data is added to the
     * buffer, while ring_free_cond is signalled when data is removed.  Both
     * are governed by ring_mutex, and both are signalled when the transfer is
     * cancelled.
     */

    GMutex *ring_mutex;
    GCond *ring_add_cond, *ring_free_cond;
    gchar *ring_buffer;
    gsize ring_length, ring_count;
    gsize ring_head, ring_tail;
    gboolean ring_head_at_eof;

    /* Element State
     *
     * "state" includes all of the variables below (including holding
     * parameters).  Note that the holding_thread holdes this mutex for the
     * entire duration of writing a chunk.
     *
     * state_mutex should always be locked before ring_mutex, if both are to be
     * held simultaneously.
     */
    GMutex     *state_mutex;
    GCond      *state_cond;
    gboolean    paused;
    char       *filename;
    char       *new_filename;
    dumpfile_t *chunk_header;
    int         fd;
    guint64     use_bytes;
    guint64     data_bytes_written;   /* bytes written in the current call */
    guint64     header_bytes_written;
    guint64     chunk_offset;         /* bytes written to the current */
				      /* chunk, including header      */

    enum { CHUNK_OK, CHUNK_EOF, CHUNK_EOC, CHUNK_NO_ROOM } chunk_status;
} XferDestHolding;

static GType xfer_dest_holding_get_type(void);
#define XFER_DEST_HOLDING_TYPE (xfer_dest_holding_get_type())
#define XFER_DEST_HOLDING(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_holding_get_type(), XferDestHolding)
#define XFER_DEST_HOLDING_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_holding_get_type(), XferDestHolding const)
#define XFER_DEST_HOLDING_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_holding_get_type(), XferDestHoldingClass)
#define IS_XFER_DEST_HOLDING(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_holding_get_type ())
#define XFER_DEST_HOLDING_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_holding_get_type(), XferDestHoldingClass)

typedef struct {
    XferElementClass __parent__;

    void (*start_chunk)(XferDestHolding *self, dumpfile_t *chunk_header, char *filename, guint64 use_bytes);
    void (*finish_chunk)(XferDestHolding *self);
    guint64 (*get_chunk_bytes_written)(XferDestHolding *self);

} XferDestHoldingClass;

/* local functions */
static void close_chunk(XferDestHolding *xdh, char *cont_filename);
static ssize_t write_header(XferDestHolding *xdh, int fd);
static size_t full_write_with_fake_enospc(int fd, const void *buf, size_t count);

/* we use a function pointer for full_write, so that we can "shim" in
 * full_write_with_fake_enospc for testing
 */
size_t (*db_full_write)(int fd, const void *buf, size_t count);
static off_t fake_enospc_at_byte = -1;

/*
 * Debug logging
 */

#define DBG(LEVEL, ...) if (debug_chunker >= LEVEL) { _xdh_dbg(__VA_ARGS__); }
static void
_xdh_dbg(const char *fmt, ...)
{
    va_list argp;
    gchar *msg;

    arglist_start(argp, fmt);
    msg = g_strdup_vprintf(fmt, argp);
    arglist_end(argp);
    g_debug("XDH: %s", msg);
    g_free(msg);
}


/*
 * Holding Thread
 */

/* Wait for at least one block, or EOF, to be available in the ring buffer.
 * Called with the ring mutex held. */
static gsize
holding_thread_wait_for_block(
    XferDestHolding *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    gsize bytes_needed = HOLDING_BLOCK_BYTES;
    gsize usable;

    while (1) {
	/* are we ready? */
	if (elt->cancelled)
	    break;

	if (self->ring_count >= bytes_needed)
	    break;

	if (self->ring_head_at_eof)
	    break;

	/* nope - so wait */
	g_cond_wait(self->ring_add_cond, self->ring_mutex);
    }

    usable = MIN(self->ring_count, bytes_needed);

    return usable;
}

/* Mark WRITTEN bytes as free in the ring buffer.  Called with the ring mutex
 * held. */
static void
holding_thread_consume_block(
    XferDestHolding *self,
    gsize written)
{
    self->ring_count -= written;
    self->ring_tail += written;
    if (self->ring_tail >= self->ring_length)
	self->ring_tail -= self->ring_length;
    g_cond_broadcast(self->ring_free_cond);
}

/* Write an entire chunk.  Called with the state_mutex held */
static gboolean
holding_thread_write_chunk(
    XferDestHolding *self)
{
    XferElement *elt = XFER_ELEMENT(self);

    self->chunk_status = CHUNK_OK;

    g_mutex_lock(self->ring_mutex);
    while (1) {
	gsize to_write;
	size_t count;

	/* wait for at least one block, and (if necessary) prebuffer */
	to_write = holding_thread_wait_for_block(self);
	to_write = MIN(to_write, HOLDING_BLOCK_BYTES);
	if (elt->cancelled)
	    break;
	if (to_write == 0) {
	    self->chunk_status = CHUNK_EOF;
	    break;
	}
	if (self->chunk_status == CHUNK_EOC) {
	    break;
	}
	to_write = MIN(to_write, self->use_bytes);

	DBG(8, "writing %ju bytes to holding", (uintmax_t)to_write);

	/* note that it's OK to reference these ring_* vars here, as they
	 * are static at this point */
	g_mutex_unlock(self->ring_mutex);
	count = db_full_write(self->fd, self->ring_buffer + self->ring_tail,
			      (guint)to_write);
	g_mutex_lock(self->ring_mutex);

	if (count != to_write) {
	    if (count > 0) {
		if (ftruncate(self->fd, self->chunk_offset) != 0) {
		    g_debug("ftruncate failed: %s", strerror(errno));
		    g_mutex_unlock(self->ring_mutex);
		    return FALSE;
		}
	    }
	    self->chunk_status = CHUNK_NO_ROOM;
	    break;
	}
	crc32_add((uint8_t *)(self->ring_buffer + self->ring_tail),
			 to_write, &elt->crc);
	self->chunk_offset += count;

	self->data_bytes_written += count;
	self->use_bytes -= count;
	holding_thread_consume_block(self, count);

	if (self->use_bytes <= 0) {
	    self->chunk_status = CHUNK_EOC;
	    /* loop to see if more data is available
	     * chunk_status might become CHUNK_EOF if at end of input file
	     */
	}
    }
    g_mutex_unlock(self->ring_mutex);

    /* if we write all of the blocks, but the finish_file fails, then likely
     * there was some buffering going on in the holding driver, and the blocks
     * did not all make it to permanent storage -- so it's a failed part.  Note
     * that we try to finish_file even if the part failed, just to be thorough.
     */
    if (elt->cancelled) {
	return FALSE;
    }

    return TRUE;
}

static gpointer
holding_thread(
    gpointer data)
{
    XferDestHolding *self = XFER_DEST_HOLDING(data);
    XferElement *elt = XFER_ELEMENT(self);
    XMsg *msg;
    gchar *mesg = NULL;
    GTimer *timer = g_timer_new();

    DBG(1, "(this is the holding thread)");

    /* This is the outer loop, that loops once for each holding file or
     * CONTINUE command */
    g_mutex_lock(self->state_mutex);
    while (1) {
	gboolean done;
	/* wait until the main thread un-pauses us, and check that we have
	 * the relevant holding info available */
	while (self->paused && !elt->cancelled) {
	    DBG(9, "waiting to be unpaused");
	    g_cond_wait(self->state_cond, self->state_mutex);
	}
	DBG(9, "holding_thread done waiting");

        if (elt->cancelled)
	    break;

	self->data_bytes_written = 0;
	self->header_bytes_written = 0;

	/* new holding file */
	if (self->filename == NULL ||
	    strcmp(self->filename, self->new_filename) != 0) {
	    char    *tmp_filename;
	    char    *pc;
	    int      fd;
	    ssize_t  write_header_size;

	    if (self->use_bytes < HEADER_BLOCK_BYTES) {
		self->chunk_status = CHUNK_NO_ROOM;
		goto no_room;
	    }

	    tmp_filename = g_strjoin(NULL, self->new_filename, ".tmp", NULL);
	    pc = strrchr(tmp_filename, '/');
	    g_assert(pc != NULL);
	    *pc = '\0';
	    mkholdingdir(tmp_filename);
	    *pc = '/';

	    fd = open(tmp_filename, O_RDWR|O_CREAT|O_TRUNC, 0600);
	    if (fd < 0) {
		self->chunk_status = CHUNK_NO_ROOM;
		g_free(mesg);
		mesg = g_strdup_printf("Failed to open '%s': %s",
				       tmp_filename, strerror(errno));
		g_free(tmp_filename);
		goto no_room;
	    }
	    if (self->filename == NULL) {
		self->chunk_header->type = F_DUMPFILE;
	    } else {
		self->chunk_header->type = F_CONT_DUMPFILE;
	    }
	    self->chunk_header->cont_filename[0] = '\0';

	    write_header_size = write_header(self, fd);
	    if (write_header_size != HEADER_BLOCK_BYTES) {
		self->chunk_status = CHUNK_NO_ROOM;
		mesg = g_strdup_printf("Failed to write header to '%s': %s",
				       tmp_filename, strerror(errno));
		close(fd);
		unlink(tmp_filename);
		g_free(tmp_filename);
		goto no_room;
	    }
	    g_free(tmp_filename);
	    self->use_bytes -= HEADER_BLOCK_BYTES;

	    /* rewrite old_header */
	    if (self->filename &&
		strcmp(self->filename, self->new_filename) != 0) {
		close_chunk(self, self->new_filename);
	    }
	    self->filename = self->new_filename;
	    self->new_filename = NULL;
	    self->fd = fd;
	    self->header_bytes_written = HEADER_BLOCK_BYTES;
	    self->chunk_offset = HEADER_BLOCK_BYTES;
	}

	DBG(2, "beginning to write chunk");
	done = holding_thread_write_chunk(self);
	DBG(2, "done writing chunk");

	if (!done) /* cancelled */
	    break;

no_room:
	msg = xmsg_new(XFER_ELEMENT(self), XMSG_CHUNK_DONE, 0);
	msg->header_size = self->header_bytes_written;
	msg->data_size = self->data_bytes_written;
	msg->no_room = (self->chunk_status == CHUNK_NO_ROOM);
	if (mesg) {
	    msg->message = mesg;
	    mesg = NULL;
	}

	xfer_queue_message(elt->xfer, msg);

	/* pause ourselves and await instructions from the main thread */
	self->paused = TRUE;

	/* if this is the last part, we're done with the chunk loop */
	if (self->chunk_status == CHUNK_EOF) {
	    break;
	}
    }
    g_mutex_unlock(self->state_mutex);

    g_debug("sending XMSG_CRC message");
    g_debug("xfer-dest-holding CRC: %08x     size: %lld",
	    crc32_finish(&elt->crc), (long long)elt->crc.size);
    msg = xmsg_new(XFER_ELEMENT(self), XMSG_CRC, 0);
    msg->crc = crc32_finish(&elt->crc);
    msg->size = elt->crc.size;
    xfer_queue_message(elt->xfer, msg);

    msg = xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0);
    msg->duration = g_timer_elapsed(timer, NULL);
    g_timer_destroy(timer);
    /* tell the main thread we're done */
    xfer_queue_message(elt->xfer, msg);

    return NULL;
}

/*
 * Class mechanics
 */

static void
push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t size)
{
    XferDestHolding *self = XFER_DEST_HOLDING(elt);
    gchar *p = buf;

    DBG(3, "push_buffer(%p, %ju)", buf, (uintmax_t)size);

    /* do nothing if cancelled */
    if (G_UNLIKELY(elt->cancelled)) {
        goto free_and_finish;
    }

    /* handle EOF */
    if (G_UNLIKELY(buf == NULL)) {
	/* indicate EOF to the holding thread */
	g_mutex_lock(self->ring_mutex);
	self->ring_head_at_eof = TRUE;
	g_cond_broadcast(self->ring_add_cond);
	g_mutex_unlock(self->ring_mutex);

	return;
    }

    /* push the block into the ring buffer, in pieces if necessary */
    while (size > 0) {
	gsize avail;

	g_mutex_lock(self->ring_mutex);

	/* wait for some space */
	while (self->ring_count == self->ring_length && !elt->cancelled) {
	    DBG(9, "waiting for any space to buffer pushed data");
	    g_cond_wait(self->ring_free_cond, self->ring_mutex);
	}
	DBG(9, "holding_thread done waiting");

	if (elt->cancelled) {
	    g_mutex_unlock(self->ring_mutex);
	    break;
	}

	/* only copy to the end of the buffer, if the available space wraps
	 * around to the beginning */
	avail = MIN(size, self->ring_length - self->ring_count);
	avail = MIN(avail, self->ring_length - self->ring_head);

	/* copy AVAIL bytes into the ring buf (knowing it's contiguous) */
	memmove(self->ring_buffer + self->ring_head, p, avail);

	/* reset the ring variables to represent this state */
	self->ring_count += avail;
	self->ring_head += avail; /* will, at most, hit ring_length */
	if (self->ring_head == self->ring_length)
	    self->ring_head = 0;
	p = (gpointer)((guchar *)p + avail);
	size -= avail;

	/* and give the holding thread a notice that data is ready */
	g_cond_broadcast(self->ring_add_cond);

	g_mutex_unlock(self->ring_mutex);
    }


free_and_finish:
    g_free(buf);
}

/*
 * Element mechanics
 */

static gboolean
start_impl(
    XferElement *elt)
{
    XferDestHolding *self = (XferDestHolding *)elt;
    GError *error = NULL;

    self->holding_thread = g_thread_create(holding_thread, (gpointer)self, FALSE, &error);
    if (!self->holding_thread) {
        g_critical(_("Error creating new thread: %s (%s)"),
            error->message, errno? strerror(errno) : _("no error code"));
    }

    return TRUE;
}

static gboolean
cancel_impl(
    XferElement *elt,
    gboolean expect_eof)
{
    XferDestHolding *self = XFER_DEST_HOLDING(elt);
    gboolean rv;

    /* chain up first */
    rv = XFER_ELEMENT_CLASS(parent_class)->cancel(elt, expect_eof);

    /* then signal all of our condition variables, so that threads waiting on them
     * wake up and see elt->cancelled. */
    g_mutex_lock(self->ring_mutex);
    g_cond_broadcast(self->ring_add_cond);
    g_cond_broadcast(self->ring_free_cond);
    g_mutex_unlock(self->ring_mutex);

    g_mutex_lock(self->state_mutex);
    g_cond_broadcast(self->state_cond);
    g_mutex_unlock(self->state_mutex);

    return rv;
}

static void
start_chunk_impl(
    XferDestHolding *xdh,
    dumpfile_t *chunk_header,
    char *filename,
    guint64 use_bytes)
{
    XferDestHolding *self = XFER_DEST_HOLDING(xdh);

    g_assert(chunk_header != NULL);

    DBG(1, "start_chunk(%s)", filename);

    g_mutex_lock(self->state_mutex);
    g_assert(self->paused);

    self->chunk_header = chunk_header;
    self->use_bytes = use_bytes;
    self->new_filename = g_strdup(filename);
    if (!self->first_filename) {
	self->first_filename = g_strdup(filename);
    }

    DBG(1, "unpausing");
    self->paused = FALSE;
    g_cond_broadcast(self->state_cond);

    g_mutex_unlock(self->state_mutex);
}

static void
finish_chunk_impl(
    XferDestHolding *xdh)
{
    XferDestHolding *self = XFER_DEST_HOLDING(xdh);

    g_mutex_lock(self->state_mutex);
    close_chunk(self, NULL);
    g_mutex_unlock(self->state_mutex);
}

static void
close_chunk(
    XferDestHolding *xdh,
    char *cont_filename)
{
    XferDestHolding *self = XFER_DEST_HOLDING(xdh);

    lseek(self->fd, 0L, SEEK_SET);
    if (strcmp(self->filename, self->first_filename) == 0) {
	self->chunk_header->type = F_DUMPFILE;
    } else {
	self->chunk_header->type = F_CONT_DUMPFILE;
    }
    if (cont_filename) {
	strncpy(self->chunk_header->cont_filename, cont_filename, sizeof(self->chunk_header->cont_filename));
	self->chunk_header->cont_filename[sizeof(self->chunk_header->cont_filename)-1] = '\0';
    } else {
	self->chunk_header->cont_filename[0] = '\0';
    }
    write_header(self, self->fd);
    close(self->fd);
    self->fd = -1;
    g_free(self->filename);
    self->filename = NULL;
}

static guint64
get_chunk_bytes_written_impl(
    XferDestHolding *xdhself)
{
    XferDestHolding *self = XFER_DEST_HOLDING(xdhself);

    /* NOTE: this access is unsafe and may return inconsistent results (e.g, a
     * partial write to the 64-bit value on a 32-bit system).  This is ok for
     * the moment, as it's only informational, but be warned. */
    return self->data_bytes_written;
}

static void
instance_init(
    XferElement *elt)
{
    XferDestHolding *self = XFER_DEST_HOLDING(elt);
    elt->can_generate_eof = FALSE;

    self->state_mutex = g_mutex_new();
    self->state_cond = g_cond_new();
    self->ring_mutex = g_mutex_new();
    self->ring_add_cond = g_cond_new();
    self->ring_free_cond = g_cond_new();

    self->fd = -1;
    self->use_bytes = 0;
    self->paused = TRUE;
    self->chunk_header = NULL;
    self->filename = NULL;
    self->first_filename = NULL;
    self->new_filename = NULL;
    self->data_bytes_written = 0;
    self->header_bytes_written = 0;
    crc32_init(&elt->crc);
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferDestHolding *self = XFER_DEST_HOLDING(obj_self);

    g_mutex_free(self->state_mutex);
    g_cond_free(self->state_cond);

    g_mutex_free(self->ring_mutex);
    g_cond_free(self->ring_add_cond);
    g_cond_free(self->ring_free_cond);

    if (self->ring_buffer)
	g_free(self->ring_buffer);
    self->chunk_header = NULL;

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferDestHoldingClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    XferDestHoldingClass *xdh_klass = XFER_DEST_HOLDING_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_PUSH_BUFFER, XFER_MECH_NONE, XFER_NROPS(2), XFER_NTHREADS(1), XFER_NALLOC(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0), XFER_NALLOC(0) }
    };

    klass->start = start_impl;
    klass->cancel = cancel_impl;
    klass->push_buffer = push_buffer_impl;
    xdh_klass->start_chunk = start_chunk_impl;
    xdh_klass->finish_chunk = finish_chunk_impl;
    xdh_klass->get_chunk_bytes_written = get_chunk_bytes_written_impl;
    goc->finalize = finalize_impl;

    klass->perl_class = "Amanda::Xfer::Dest::Holding";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

static GType
xfer_dest_holding_get_type (void)
{
    static GType type = 0;

    if (G_UNLIKELY(type == 0)) {
        static const GTypeInfo info = {
            sizeof (XferDestHoldingClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestHolding),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static(XFER_ELEMENT_TYPE, "XferDestHolding",
				      &info, 0);
    }

    return type;
}

/*
 * Constructor
 */

XferElement *
xfer_dest_holding(
    size_t max_memory)
{
    XferDestHolding *self = (XferDestHolding *)g_object_new(XFER_DEST_HOLDING_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);
    char *env;

    /* max_memory get rounded up to the next multiple of block_size */
    max_memory = ((max_memory + HOLDING_BLOCK_BYTES - 1)
			/ HOLDING_BLOCK_BYTES) * HOLDING_BLOCK_BYTES;

    self->paused = TRUE;

    /* set up a ring buffer of size max_memory */
    self->ring_length = max_memory;
    self->ring_buffer = g_malloc(max_memory);
    self->ring_head = self->ring_tail = 0;
    self->ring_count = 0;
    self->ring_head_at_eof = 0;

    /* set up a fake ENOSPC for testing purposes.  Note that this counts
     * headers as well as data written to disk. */
    env = getenv("CHUNKER_FAKE_ENOSPC_AT");
    if (env) {
	fake_enospc_at_byte = (off_t)atoi(env); /* these values are never > MAXINT */
	db_full_write = full_write_with_fake_enospc;
	DBG(1,"will trigger fake ENOSPC at byte %d", (int)fake_enospc_at_byte);
    } else {
	db_full_write = full_write;
    }

    return elt;
}

/*
 * Send an Amanda dump header to the output file and set file->blocksize
 */
static ssize_t
write_header(
    XferDestHolding *self,
    int fd)
{
    char *buffer;
    size_t written;

    self->chunk_header->blocksize = HEADER_BLOCK_BYTES;
    if (debug_chunker > 1)
        dump_dumpfile_t((dumpfile_t *)self->chunk_header);
    buffer = build_header((dumpfile_t *)self->chunk_header, NULL, HEADER_BLOCK_BYTES);
    if (!buffer) /* this shouldn't happen */
        error(_("header does not fit in %zd bytes"), (size_t)HEADER_BLOCK_BYTES);

    written = db_full_write(fd, buffer, HEADER_BLOCK_BYTES);
    g_free(buffer);
    if(written == HEADER_BLOCK_BYTES) return HEADER_BLOCK_BYTES;

    /* fake ENOSPC when we get a short write without errno set */
    if(errno == 0)
        errno = ENOSPC;

    return (ssize_t)-1;
}

static size_t
full_write_with_fake_enospc(
    int fd,
    const void *buf,
    size_t count)
{
    size_t rc;

    //DBG(1,"HERE %zd %zd", count, (size_t)fake_enospc_at_byte);

    if (count <= (size_t)fake_enospc_at_byte) {
	fake_enospc_at_byte -= count;
	return full_write(fd, buf, count);
    }

    /* if we get here, the caller has requested a size that is less
     * than fake_enospc_at_byte. */
    count = fake_enospc_at_byte;
    DBG(1,"returning fake ENOSPC");

    if (fake_enospc_at_byte) {
	rc = full_write(fd, buf, fake_enospc_at_byte);
	if (rc == (size_t)fake_enospc_at_byte) {
	    /* full_write succeeded, so fake a failure */
	    errno = ENOSPC;
	}
    } else {
	/* no bytes to write; just fake an error */
	errno = ENOSPC;
	rc = 0;
    }

    /* switch back to calling full_write directly */
    fake_enospc_at_byte = -1;
    db_full_write = full_write;
    return rc;
}

void
xfer_dest_holding_start_chunk(
    XferElement *elt,
    dumpfile_t *chunk_header,
    char *filename,
    guint64 use_bytes)
{
    XferDestHoldingClass *klass;
    g_assert(IS_XFER_DEST_HOLDING(elt));

    klass = XFER_DEST_HOLDING_GET_CLASS(elt);
    klass->start_chunk(XFER_DEST_HOLDING(elt), chunk_header, filename,
                                         use_bytes);
}

void
xfer_dest_holding_finish_chunk(
    XferElement *elt)
{
    XferDestHoldingClass *klass;
    g_assert(IS_XFER_DEST_HOLDING(elt));

    klass = XFER_DEST_HOLDING_GET_CLASS(elt);
    klass->finish_chunk(XFER_DEST_HOLDING(elt));
}

