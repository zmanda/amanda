/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009, 2010 Zmanda, Inc.  All Rights Reserved.
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

#include "amxfer.h"
#include "amanda.h"
#include "xfer-device.h"
#include "arglist.h"
#include "conffile.h"

/* A transfer destination that writes an entire dumpfile to one or more files
 * on one or more devices, without any caching - this assumes that once a block
 * is written, it is on media and no longer needed by Amanda.   This is
 * similar to the XferDestTaperCacher, but much simpler. */

/*
 * Xfer Dest Taper
 */

static GObjectClass *parent_class = NULL;

typedef struct XferDestTaperSplitter {
    XferDestTaper __parent__;

    /* object parameters
     *
     * These values are supplied to the constructor, and can be assumed
     * constant for the lifetime of the element.
     */

    /* Maximum size of each part (bytes) */
    guint64 part_size;

    /* the device's need for streaming (it's assumed that all subsequent devices
     * have the same needs) */
    StreamingRequirement streaming;

    /* block size expected by the target device */
    gsize block_size;

    /* The thread doing the actual writes to tape; this also handles buffering
     * for streaming */
    GThread *device_thread;

    /* Ring Buffer
     *
     * This buffer holds MAX_MEMORY bytes of data (rounded up to the next
     * blocksize), and serves as the interface between the device_thread and
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
     * "state" includes all of the variables below (including device
     * parameters).  Note that the device_thread holdes this mutex for the
     * entire duration of writing a part.
     *
     * state_mutex should always be locked before ring_mutex, if both are to be
     * held simultaneously.
     */
    GMutex *state_mutex;
    GCond *state_cond;
    volatile gboolean paused;

    /* The device to write to, and the header to write to it */
    Device *volatile device;
    dumpfile_t *volatile part_header;

    /* part number in progress */
    volatile guint64 partnum;

    /* if true, the main thread should *not* call start_part */
    volatile gboolean no_more_parts;

    /* total bytes written in the current part */
    volatile guint64 part_bytes_written;
} XferDestTaperSplitter;

static GType xfer_dest_taper_splitter_get_type(void);
#define XFER_DEST_TAPER_SPLITTER_TYPE (xfer_dest_taper_splitter_get_type())
#define XFER_DEST_TAPER_SPLITTER(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_taper_splitter_get_type(), XferDestTaperSplitter)
#define XFER_DEST_TAPER_SPLITTER_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_taper_splitter_get_type(), XferDestTaperSplitter const)
#define XFER_DEST_TAPER_SPLITTER_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_taper_splitter_get_type(), XferDestTaperSplitterClass)
#define IS_XFER_DEST_TAPER_SPLITTER(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_taper_splitter_get_type ())
#define XFER_DEST_TAPER_SPLITTER_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_taper_splitter_get_type(), XferDestTaperSplitterClass)

typedef struct {
    XferDestTaperClass __parent__;

} XferDestTaperSplitterClass;

/*
 * Debug logging
 */

#define DBG(LEVEL, ...) if (debug_taper >= LEVEL) { _xdt_dbg(__VA_ARGS__); }
static void
_xdt_dbg(const char *fmt, ...)
{
    va_list argp;
    char msg[1024];

    arglist_start(argp, fmt);
    g_vsnprintf(msg, sizeof(msg), fmt, argp);
    arglist_end(argp);
    g_debug("XDT thd-%p: %s", g_thread_self(), msg);
}

/*
 * Device Thread
 */

/* Wait for at least one block, or EOF, to be available in the ring buffer.
 * Called with the ring mutex held. */
static gsize
device_thread_wait_for_block(
    XferDestTaperSplitter *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    gsize bytes_needed = self->device->block_size;
    gsize usable;

    /* for any kind of streaming, we need to fill the entire buffer before the
     * first byte */
    if (self->part_bytes_written == 0 && self->streaming != STREAMING_REQUIREMENT_NONE)
	bytes_needed = self->ring_length;

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

	/* in STREAMING_REQUIREMENT_REQUIRED, once we decide to wait for more bytes,
	 * we need to wait for the entire buffer to fill */
	if (self->streaming == STREAMING_REQUIREMENT_REQUIRED)
	    bytes_needed = self->ring_length;
    }

    usable = MIN(self->ring_count, bytes_needed);
    if (self->part_size)
       usable = MIN(usable, self->part_size - self->part_bytes_written);

    DBG(8, "using %ju bytes in ring buffer starting at %ju",
	    (uintmax_t)usable, (uintmax_t)self->ring_tail);

    return usable;
}

/* Mark WRITTEN bytes as free in the ring buffer.  Called with the ring mutex
 * held. */
static void
device_thread_consume_block(
    XferDestTaperSplitter *self,
    gsize written)
{
    DBG(8, "consuming %ju bytes starting at %ju in ring buffer",
	    (uintmax_t)written, (uintmax_t)self->ring_tail);
    self->ring_count -= written;
    self->ring_tail += written;
    if (self->ring_tail >= self->ring_length)
	self->ring_tail -= self->ring_length;
    g_cond_broadcast(self->ring_free_cond);
}

/* Write an entire part.  Called with the state_mutex held */
static XMsg *
device_thread_write_part(
    XferDestTaperSplitter *self)
{
    GTimer *timer = g_timer_new();
    XferElement *elt = XFER_ELEMENT(self);
    enum { PART_EOF, PART_LEOM, PART_EOP, PART_FAILED } part_status = PART_FAILED;
    int fileno = 0;
    XMsg *msg;

    self->part_bytes_written = 0;

    g_timer_start(timer);

    /* write the header; if this fails or hits LEOM, we consider this a
     * successful 0-byte part */
    if (!device_start_file(self->device, self->part_header) || self->device->is_eom) {
	part_status = PART_LEOM;
	goto part_done;
    }

    fileno = self->device->file;
    g_assert(fileno > 0);

    /* free the header, now that it's written */
    dumpfile_free(self->part_header);
    self->part_header = NULL;

    g_mutex_lock(self->ring_mutex);
    while (1) {
	gsize to_write;
	gboolean ok;

	/* wait for at least one block, and (if necessary) prebuffer */
	to_write = device_thread_wait_for_block(self);
	if (elt->cancelled)
	    break;

	if (to_write == 0) {
	    part_status = PART_EOF;
	    break;
	}

	g_mutex_unlock(self->ring_mutex);
	DBG(8, "writing %ju bytes to device", (uintmax_t)to_write);

	/* note that it's OK to reference these ring_* vars here, as they
	 * are static at this point */
	ok = device_write_block(self->device, (guint)to_write,
		self->ring_buffer + self->ring_tail);
	g_mutex_lock(self->ring_mutex);

	if (!ok) {
	    part_status = PART_FAILED;
	    break;
	}

	self->part_bytes_written += to_write;
	device_thread_consume_block(self, to_write);

	if (self->part_size && self->part_bytes_written >= self->part_size) {
	    part_status = PART_EOP;
	    break;
	} else if (self->device->is_eom) {
	    part_status = PART_LEOM;
	    break;
	}
    }
    g_mutex_unlock(self->ring_mutex);
part_done:

    if (elt->cancelled) {
	g_timer_destroy(timer);
	return NULL;
    }

    /* if we write all of the blocks, but the finish_file fails, then likely
     * there was some buffering going on in the device driver, and the blocks
     * did not all make it to permanent storage -- so it's a failed part.  Note
     * that we try to finish_file even if the part failed, just to be thorough. */
    if (self->device->in_file) {
	if (!device_finish_file(self->device))
	    part_status = PART_FAILED;
    }

    g_timer_stop(timer);

    msg = xmsg_new(XFER_ELEMENT(self), XMSG_PART_DONE, 0);
    msg->size = self->part_bytes_written;
    msg->duration = g_timer_elapsed(timer, NULL);
    msg->partnum = self->partnum;
    msg->fileno = fileno;
    msg->successful = part_status != PART_FAILED;
    msg->eom = (part_status == PART_LEOM || !msg->successful);
    msg->eof = part_status == PART_EOF;

    /* time runs backward on some test boxes, so make sure this is positive */
    if (msg->duration < 0) msg->duration = 0;

    self->partnum++;
    self->no_more_parts = msg->eof || !msg->successful;

    g_timer_destroy(timer);

    return msg;
}

static gpointer
device_thread(
    gpointer data)
{
    XferDestTaperSplitter *self = XFER_DEST_TAPER_SPLITTER(data);
    XferElement *elt = XFER_ELEMENT(self);
    XMsg *msg;

    DBG(1, "(this is the device thread)");

    /* This is the outer loop, that loops once for each split part written to
     * tape. */
    g_mutex_lock(self->state_mutex);
    while (1) {
	/* wait until the main thread un-pauses us, and check that we have
	 * the relevant device info available (block_size) */
	while (self->paused && !elt->cancelled) {
	    DBG(9, "waiting to be unpaused");
	    g_cond_wait(self->state_cond, self->state_mutex);
	}
	DBG(9, "done waiting");

        if (elt->cancelled)
	    break;

	DBG(2, "beginning to write part");
	msg = device_thread_write_part(self);
	DBG(2, "done writing part");

	if (!msg) /* cancelled */
	    break;

	xfer_queue_message(elt->xfer, msg);

	/* pause ourselves and await instructions from the main thread */
	self->paused = TRUE;

	/* if this is the last part, we're done with the part loop */
	if (self->no_more_parts)
	    break;
    }
    g_mutex_unlock(self->state_mutex);

    /* tell the main thread we're done */
    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

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
    XferDestTaperSplitter *self = (XferDestTaperSplitter *)elt;
    gchar *p = buf;

    DBG(3, "push_buffer(%p, %ju)", buf, (uintmax_t)size);

    /* do nothing if cancelled */
    if (G_UNLIKELY(elt->cancelled)) {
        goto free_and_finish;
    }

    /* handle EOF */
    if (G_UNLIKELY(buf == NULL)) {
	/* indicate EOF to the device thread */
	g_mutex_lock(self->ring_mutex);
	self->ring_head_at_eof = TRUE;
	g_cond_broadcast(self->ring_add_cond);
	g_mutex_unlock(self->ring_mutex);
	goto free_and_finish;
    }

    /* push the block into the ring buffer, in pieces if necessary */
    g_mutex_lock(self->ring_mutex);
    while (size > 0) {
	gsize avail;

	/* wait for some space */
	while (self->ring_count == self->ring_length && !elt->cancelled) {
	    DBG(9, "waiting for any space to buffer pushed data");
	    g_cond_wait(self->ring_free_cond, self->ring_mutex);
	}
	DBG(9, "done waiting");

	if (elt->cancelled)
	    goto unlock_and_free_and_finish;

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

	/* and give the device thread a notice that data is ready */
	g_cond_broadcast(self->ring_add_cond);
    }

unlock_and_free_and_finish:
    g_mutex_unlock(self->ring_mutex);

free_and_finish:
    if (buf)
        g_free(buf);
}

/*
 * Element mechanics
 */

static gboolean
start_impl(
    XferElement *elt)
{
    XferDestTaperSplitter *self = (XferDestTaperSplitter *)elt;
    GError *error = NULL;

    self->device_thread = g_thread_create(device_thread, (gpointer)self, FALSE, &error);
    if (!self->device_thread) {
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
    XferDestTaperSplitter *self = XFER_DEST_TAPER_SPLITTER(elt);
    gboolean rv;

    /* chain up first */
    rv = XFER_ELEMENT_CLASS(parent_class)->cancel(elt, expect_eof);

    /* then signal all of our condition variables, so that threads waiting on them
     * wake up and see elt->cancelled. */
    g_mutex_lock(self->state_mutex);
    g_cond_broadcast(self->state_cond);
    g_mutex_unlock(self->state_mutex);

    g_mutex_lock(self->ring_mutex);
    g_cond_broadcast(self->ring_add_cond);
    g_cond_broadcast(self->ring_free_cond);
    g_mutex_unlock(self->ring_mutex);

    return rv;
}

static void
start_part_impl(
    XferDestTaper *xdtself,
    gboolean retry_part,
    dumpfile_t *header)
{
    XferDestTaperSplitter *self = XFER_DEST_TAPER_SPLITTER(xdtself);

    g_assert(self->device != NULL);
    g_assert(!self->device->in_file);
    g_assert(header != NULL);

    DBG(1, "start_part()");

    /* this shouldn't happen, but just to double-check */
    if (retry_part) {
	xfer_cancel_with_error(XFER_ELEMENT(self),
	    _("Previous part failed completely; cannot retry"));
	return;
    }

    g_mutex_lock(self->state_mutex);
    g_assert(self->paused);
    g_assert(!self->no_more_parts);

    if (self->part_header)
	dumpfile_free(self->part_header);
    self->part_header = dumpfile_copy(header);

    DBG(1, "unpausing");
    self->paused = FALSE;
    g_cond_broadcast(self->state_cond);

    g_mutex_unlock(self->state_mutex);
}

static void
use_device_impl(
    XferDestTaper *xdtself,
    Device *device)
{
    XferDestTaperSplitter *self = XFER_DEST_TAPER_SPLITTER(xdtself);
    StreamingRequirement newstreaming;
    GValue val;

    DBG(1, "use_device(%s)%s", device->device_name, (device == self->device)? " (no change)":"");

    /* short-circuit if nothing is changing */
    if (self->device == device)
	return;

    g_mutex_lock(self->state_mutex);
    if (self->device)
	g_object_unref(self->device);
    self->device = device;
    g_object_ref(device);

    /* get this new device's streaming requirements */
    bzero(&val, sizeof(val));
    if (!device_property_get(self->device, PROPERTY_STREAMING, &val)
        || !G_VALUE_HOLDS(&val, STREAMING_REQUIREMENT_TYPE)) {
        g_warning("Couldn't get streaming type for %s", self->device->device_name);
    } else {
        newstreaming = g_value_get_enum(&val);
	if (newstreaming != self->streaming)
	    g_warning("New device has different streaming requirements from the original; "
		    "ignoring new requirement");
    }
    g_value_unset(&val);

    /* check that the blocksize hasn't changed */
    if (self->block_size != device->block_size) {
        g_mutex_unlock(self->state_mutex);
        xfer_cancel_with_error(XFER_ELEMENT(self),
            _("All devices used by the taper must have the same block size"));
        return;
    }
    g_mutex_unlock(self->state_mutex);
}

static guint64
get_part_bytes_written_impl(
    XferDestTaper *xdtself)
{
    XferDestTaperSplitter *self = XFER_DEST_TAPER_SPLITTER(xdtself);

    /* NOTE: this access is unsafe and may return inconsistent results (e.g, a
     * partial write to the 64-bit value on a 32-bit system).  This is ok for
     * the moment, as it's only informational, but be warned. */
    return self->part_bytes_written;
}

static void
instance_init(
    XferElement *elt)
{
    XferDestTaperSplitter *self = XFER_DEST_TAPER_SPLITTER(elt);
    elt->can_generate_eof = FALSE;

    self->state_mutex = g_mutex_new();
    self->state_cond = g_cond_new();
    self->ring_mutex = g_mutex_new();
    self->ring_add_cond = g_cond_new();
    self->ring_free_cond = g_cond_new();

    self->device = NULL;
    self->paused = TRUE;
    self->part_header = NULL;
    self->partnum = 1;
    self->part_bytes_written = 0;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferDestTaperSplitter *self = XFER_DEST_TAPER_SPLITTER(obj_self);

    g_mutex_free(self->state_mutex);
    g_cond_free(self->state_cond);

    g_mutex_free(self->ring_mutex);
    g_cond_free(self->ring_add_cond);
    g_cond_free(self->ring_free_cond);

    if (self->ring_buffer)
	g_free(self->ring_buffer);

    if (self->part_header)
	dumpfile_free(self->part_header);

    if (self->device)
	g_object_unref(self->device);

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferDestTaperSplitterClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    XferDestTaperClass *xdt_klass = XFER_DEST_TAPER_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_PUSH_BUFFER, XFER_MECH_NONE, 1, 1},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    klass->start = start_impl;
    klass->cancel = cancel_impl;
    klass->push_buffer = push_buffer_impl;
    xdt_klass->start_part = start_part_impl;
    xdt_klass->use_device = use_device_impl;
    xdt_klass->get_part_bytes_written = get_part_bytes_written_impl;
    goc->finalize = finalize_impl;

    klass->perl_class = "Amanda::Xfer::Dest::Taper::Splitter";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

static GType
xfer_dest_taper_splitter_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestTaperSplitterClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestTaperSplitter),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_DEST_TAPER_TYPE, "XferDestTaperSplitter", &info, 0);
    }

    return type;
}

/*
 * Constructor
 */

XferElement *
xfer_dest_taper_splitter(
    Device *first_device,
    size_t max_memory,
    guint64 part_size)
{
    XferDestTaperSplitter *self = (XferDestTaperSplitter *)g_object_new(XFER_DEST_TAPER_SPLITTER_TYPE, NULL);
    GValue val;

    /* max_memory and part_size get rounded up to the next multiple of
     * block_size */
    max_memory = ((max_memory + first_device->block_size - 1)
			/ first_device->block_size) * first_device->block_size;
    if (part_size)
	part_size = ((part_size + first_device->block_size - 1)
			    / first_device->block_size) * first_device->block_size;

    self->part_size = part_size;
    self->partnum = 1;
    self->device = first_device;
    g_object_ref(self->device);
    self->block_size = first_device->block_size;
    self->paused = TRUE;
    self->no_more_parts = FALSE;

    /* set up a ring buffer of size max_memory */
    self->ring_length = max_memory;
    self->ring_buffer = g_malloc(max_memory);
    self->ring_head = self->ring_tail = 0;
    self->ring_count = 0;
    self->ring_head_at_eof = 0;

    /* get this new device's streaming requirements */
    bzero(&val, sizeof(val));
    if (!device_property_get(self->device, PROPERTY_STREAMING, &val)
        || !G_VALUE_HOLDS(&val, STREAMING_REQUIREMENT_TYPE)) {
        g_warning("Couldn't get streaming type for %s", self->device->device_name);
        self->streaming = STREAMING_REQUIREMENT_REQUIRED;
    } else {
        self->streaming = g_value_get_enum(&val);
    }
    g_value_unset(&val);

    return XFER_ELEMENT(self);
}
