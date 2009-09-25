/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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

#include "amxfer.h"
#include "amanda.h"
#include "xfer-device.h"
#include "arglist.h"

/* A transfer destination that writes and entire dumpfile to one or more files on one
 * or more devices.   This is designed to work in concert with Amanda::Taper::Scribe. */

/* Future Plans:
 * - capture EOF early enough to avoid wasting a tape when the part size is an even multiple of the volume size - maybe reader thread can just go back and tag previous slab with EOF in that case?
 * - use mmap to make the disk-cacher thread unnecessary, if supported, by simply mapping slabs into the disk cache file
 * - can we find a way to fall back to mem_cache when the disk cache gets ENOSPC? Does it even make sense to try, since this would change the part size?
 * - distinguish some permanent device errors and do not retry the part? (this will be a change of behavior)
 */

/*
 * Slabs
 *
 * Slabs are larger than blocks, and are the unit on which the element
 * operates.  They are designed to be a few times larger than a block, to
 * achieve a corresponding reduction in the number of locks and unlocks used
 * per block, and similar reduction in the the amount of memory overhead
 * required.
 */

typedef struct Slab {
    struct Slab *next;

    /* counts incoming pointers: the preceding slab's 'next' pointer, and pointers
     * from any processes operating on the slab */
    gint refcount;

    /* number of this slab in the sequence, global to this element's lifetime.
     * Since this counts slabs, which are about 1M, this can address 16
     * yottabytes of data before wrapping. */
    guint64 serial;

    /* slab size; this is only less than the element's slab size if the
     * transfer is at EOF. */
    gsize size;

    /* base of the slab_size buffer */
    gpointer base;
} Slab;

/*
 * File Slices
 *
 * These objects are arranged in a linked list, and describe the files in which
 * the disk cache is stored.  Note that we assume that slices are added *before*
 * they are needed: the xfer element will fail if it tries to rewind and does not
 * find a suitable slice.
 */

typedef struct FileSlice {
    struct FileSlice *next;

    /* fully-qualified filename to read from, or NULL to read from disk_cache_read_fd */
    char *filename;

    /* offset in file to start at */
    off_t offset;

    /* length of data to read */
    gsize length;
} FileSlice;

/*
 * Xfer Dest Taper
 */

static GObjectClass *parent_class = NULL;

typedef struct XferDestTaper {
    XferElement __parent__;

    /* object parameters
     *
     * These values are supplied to the constructor, and can be assumed
     * constant for the lifetime of the element.
     */

    /* maximum buffer space to use for streaming; this is unrelated to the
     * fallback_splitsize */
    gsize max_memory;

    /* split buffering info; if we're doing memory buffering, use_mem_cache is
     * true; if we're doing disk buffering, disk_cache_dirname is non-NULL
     * and contains the (allocated) filename of the cache file.  Either way,
     * part_size gives the desired cache size.  If part_size is zero, then
     * no splitting takes place (so part_size is effectively infinite) */
    gboolean use_mem_cache;
    char *disk_cache_dirname;
    guint64 part_size; /* (bytes) */

    /*
     * threads
     */

    /* The thread doing the actual writes to tape; this also handles buffering
     * for streaming */
    GThread *device_thread;

    /* The thread writing slabs to the disk cache, if any */
    GThread *disk_cache_thread;

    /* slab train
     *
     * All in-memory data is contained in a linked list called the "slab
     * train".  Various components are operating simultaneously at different
     * points in this train.  Data from the upstream XferElement is appended to
     * the head of the train, and the device thread follows along behind,
     * writing data to the device.  When caching parts in memory, the slab
     * train just grows to eventually contain the whole part.  When using an
     * on-disk cache, the disk cache thread writes the tail of the train to
     * disk, freeing slabs to be re-used at the head of the train.  Some
     * careful coordination of these components allows them to operate as
     * independently as possible within the limits of the user's configuration.
     *
     * Slabs are rarely, if ever, freed: the oldest_slab reference generally
     * ensures that all slabs have refcount > 0, and this pointer is only
     * advanced when re-using slabs that have been flushed to the disk cache or
     * when freeing slabs after completion of the transfer. */

    /* pointers into the slab train are all protected by this mutex.  Note that
     * the slabs themselves can be manipulated without this lock; it's only
     * when changing the pointers that the mutex must be held.  Furthermore, a
     * foo_slab variable which is not NULL will not be changed except by its
     * controlling thread (disk_cacher_slab is controlled by disk_cache_thread,
     * and device_slab is controlled by device_thread).  This means that a
     * controlling thread can drop the slab_mutex once it has ensured its slab
     * is non-NULL.
     *
     * Slab_cond is notified when a new slab is made available from the reader.
     * Slab_free_cond is notified when a slab becomes available for
     * reallocation.
     *
     * Any thread waiting on either condition variable should also check
     * elt->cancelled, and act appropriately if awakened in a cancelled state.
     */
    GMutex *slab_mutex; GCond *slab_cond; GCond *slab_free_cond;

    /* slabs in progress by each thread, or NULL if the thread is waiting on
     * slab_cond.  These can only be changed by their respective threads, except
     * when they are NULL (in which case the reader will point them to a new
     * slab and signal the slab_cond). */
    Slab *volatile disk_cacher_slab;
    Slab *volatile mem_cache_slab;
    Slab *volatile device_slab;

    /* tail and head of the slab train */
    Slab *volatile oldest_slab;
    Slab *volatile newest_slab;

    /* thread-specific information
     *
     * These values are only used by one thread, and thus are not
     * subject to any locking or concurrency constraints.
     */

    /* slab in progress by the reader (not in the slab train) */
    Slab *reader_slab;

    /* the serial to be assigned to reader_slab */
    guint64 next_serial;

    /* bytes written to the device in this part */
    guint64 bytes_written;

    /* element state
     *
     * "state" includes all of the variables below (including device
     * parameters).  Note that the device_thread reads state values when
     * paused is false without locking the mutex.  No other thread should
     * change state when the element is not paused.
     *
     * If there is every any reason to lock both mutexes, acquire this one
     * first.
     *
     * Any thread waiting on this condition variable should also check
     * elt->cancelled, and act appropriately if awakened in a cancelled state.
     */
    GMutex *state_mutex;
    GCond *state_cond;
    volatile gboolean paused;

    /* The device to write to, and the header to write to it */
    Device *volatile device;
    dumpfile_t *volatile part_header;

    /* If true, when unpaused, the device should begin at the beginning of the
     * cache; if false, it should proceed to the next part. */
    volatile gboolean retry_part;

    /* If true, the previous part was completed successfully; only used for
     * assertions */
    volatile gboolean last_part_successful;

    /* part number in progress */
    volatile guint64 partnum;

    /* if true, the main thread should *not* call start_part */
    volatile gboolean no_more_parts;

    /* the first serial in this part, and the serial to stop at */
    volatile guint64 part_first_serial, part_stop_serial;

    /* file slices for the current part */
    FileSlice *volatile part_slices;

    /* read and write file descriptors for the disk cache file, in use by the
     * disk_cache_thread.  If these are -1, wait on state_cond until they are
     * not; once the value is set, it will not change. */
    volatile int disk_cache_read_fd;
    volatile int disk_cache_write_fd;

    /* device parameters
     *
     * Note that these values aren't known until we begin writing to the
     * device; if block_size is zero, threads should block on state_cond until
     * it is nonzero, at which point all of the dependent fields will have
     * their correct values.  Note that, since this value never changes after
     * it has been set, it is safe to read block_size without acquiring the
     * mutext first. */

    /* this device's need for streaming */
    StreamingRequirement streaming;

    /* block size expected by the target device */
    gsize block_size;

    /* Size of a slab - some multiple of the block size */
    gsize slab_size;

    /* maximum number of slabs allowed, rounded up to the next whole slab.  If
     * using mem cache, this is the equivalent of part_size bytes; otherwise,
     * it is equivalent to max_memory bytes. */
    guint64 max_slabs;

    /* number of slabs in a part */
    guint64 slabs_per_part;
} XferDestTaper;

typedef struct {
    XferElementClass __parent__;

    /* see xfer-device.h for details of these methods */
    void (*start_part)(XferDestTaper *self, gboolean retry_part, Device *device,
		       dumpfile_t *header);
    void (*cache_inform)(XferDestTaper *self, const char *filename, off_t offset,
			 off_t length);
} XferDestTaperClass;

/*
 * Error Handling
 */

/* This is similar to xfer_element_handle_error, except that in this element
 * we don't want to wait around for cancellation to complete. */
static void
send_xmsg_error_and_cancel(
    XferDestTaper *self,
    const char *fmt,
    ...)
{
    XferElement *elt = XFER_ELEMENT(self);
    va_list argp;
    XMsg *msg;

    g_assert(elt != NULL);
    g_assert(elt->xfer != NULL);

    msg = xmsg_new(elt, XMSG_ERROR, 0);

    arglist_start(argp, fmt);
    msg->message = g_strdup_vprintf(fmt, argp);
    arglist_end(argp);

    xfer_queue_message(elt->xfer, msg);

    xfer_cancel(elt->xfer);
}

/*
 * Slab handling
 */

/* called with the slab_mutex held, this gets a new slab to write into, with
 * refcount 1.  It will block if max_memory slabs are already in use, and mem
 * caching is not in use, although allocation may be forced with the 'force'
 * parameter.
 *
 * If the memory allocation cannot be satisfied due to system constraints,
 * this function will send an XMSG_ERROR, wait for the transfer to cancel, and
 * return NULL.  If the transfer is cancelled by some other means while this
 * function is blocked awaiting a free slab, it will return NULL.
 *
 * @param self: the xfer element
 * @param force: allocate a slab even if it would exceed max_memory
 * @returns: a new slab, or NULL if the xfer is cancelled
 */
static Slab *
alloc_slab(
    XferDestTaper *self,
    gboolean force)
{
    XferElement *elt = XFER_ELEMENT(self);
    Slab *rv;

    if (!force) {
	/* throttle based on maximum number of extant slabs */
	while (G_UNLIKELY(
            !elt->cancelled &&
	    self->oldest_slab &&
	    self->newest_slab &&
	    self->oldest_slab->refcount > 1 &&
	    (self->newest_slab->serial - self->oldest_slab->serial + 1) >= self->max_slabs))
	    g_cond_wait(self->slab_free_cond, self->slab_mutex);

        if (elt->cancelled)
            return NULL;
    }

    /* if the oldest slab doesn't have anything else pointing to it, just use
     * that */
    if (self->oldest_slab && self->oldest_slab->refcount == 1) {
	rv = self->oldest_slab;
	self->oldest_slab = rv->next;
    } else {
	rv = g_new0(Slab, 1);
	rv->refcount = 1;
	rv->base = g_try_malloc(self->slab_size);
	if (!rv->base) {
	    g_free(rv);
	    send_xmsg_error_and_cancel(self,
		_("Could not allocate %zu bytes of memory"), self->slab_size);
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    return NULL;
	}
    }

    rv->next = NULL;
    rv->size = 0;
    return rv;
}

/* called with the slab_mutex held, this frees the given slave entirely.  The
 * reference count is not consulted.
 *
 * @param slab: slab to free
 */
static void
free_slab(
    Slab *slab)
{
    if (slab) {
	if (slab->base)
	    g_free(slab->base);
	g_free(slab);
    }
}

/* called with the slab_mutex held, this decrements the refcount of the
 * given slab
 *
 * @param self: xfer element
 * @param slab: slab to free
 */
static inline void
unref_slab(
    XferDestTaper *self,
    Slab *slab)
{
    g_assert(slab->refcount > 1);
    slab->refcount--;
    if (G_UNLIKELY(slab->refcount == 1 && slab == self->oldest_slab)) {
	g_cond_broadcast(self->slab_free_cond);
    } else if (G_UNLIKELY(slab->refcount == 0)) {
	free_slab(slab);
    }
}

/* called with the slab_mutex held, this sets *slabp to *slabp->next,
 * adjusting refcounts appropriately, and returns the new value
 *
 * @param self: xfer element
 * @param slabp: slab pointer to advance
 * @returns: new value of *slabp
 */
static inline Slab *
next_slab(
    XferDestTaper *self,
    Slab * volatile *slabp)
{
    Slab *next;

    if (!slabp || !*slabp)
	return NULL;

    next = (*slabp)->next;
    if (next)
	next->refcount++;
    if (*slabp)
	unref_slab(self, *slabp);
    *slabp = next;

    return next;
}

/*
 * Disk Cache
 *
 * The disk cache thread's job is simply to follow along the slab train at
 * maximum speed, writing slabs to the disk cache file. */

static gboolean
open_disk_cache_fds(
    XferDestTaper *self)
{
    char * filename;

    g_assert(self->disk_cache_read_fd == -1);
    g_assert(self->disk_cache_write_fd == -1);

    g_mutex_lock(self->state_mutex);
    filename = g_strdup_printf("%s/amanda-split-buffer-XXXXXX",
                               self->disk_cache_dirname);

    self->disk_cache_write_fd = g_mkstemp(filename);
    if (self->disk_cache_write_fd < 0) {
	g_mutex_unlock(self->state_mutex);
	send_xmsg_error_and_cancel(self,
	    _("Error creating cache file in '%s': %s"), self->disk_cache_dirname,
	    strerror(errno));
	g_free(filename);
	return FALSE;
    }

    /* open a separate copy of the file for reading */
    self->disk_cache_read_fd = open(filename, O_RDONLY);
    if (self->disk_cache_read_fd < 0) {
	g_mutex_unlock(self->state_mutex);
	send_xmsg_error_and_cancel(self,
	    _("Error opening cache file in '%s': %s"), self->disk_cache_dirname,
	    strerror(errno));
	g_free(filename);
	return FALSE;
    }

    /* signal anyone waiting for this value */
    g_cond_broadcast(self->state_cond);
    g_mutex_unlock(self->state_mutex);

    /* errors from unlink are not fatal */
    if (unlink(filename) < 0) {
	g_warning("While unlinking '%s': %s (ignored)", filename, strerror(errno));
    }

    g_free(filename);
    return TRUE;
}

static gpointer
disk_cache_thread(
    gpointer data)
{
    XferDestTaper *self = XFER_DEST_TAPER(data);
    XferElement *elt = XFER_ELEMENT(self);

    /* open up the disk cache file first */
    if (!open_disk_cache_fds(self))
	return NULL;

    while (!elt->cancelled) {
	gboolean eof, eop;
	guint64 stop_serial;
	Slab *slab;

	/* rewind to the begining of the disk cache file */
	if (lseek(self->disk_cache_write_fd, 0, SEEK_SET) == -1) {
	    send_xmsg_error_and_cancel(self,
		_("Error seeking disk cache file in '%s': %s"), self->disk_cache_dirname,
		strerror(errno));
	    return NULL;
	}

	/* we need to sit and wait for the next part to begin, first making sure
	 * we have a slab .. */
	g_mutex_lock(self->slab_mutex);
	while (!self->disk_cacher_slab && !elt->cancelled)
	    g_cond_wait(self->slab_cond, self->slab_mutex);
	g_mutex_unlock(self->slab_mutex);

	if (elt->cancelled)
	    break;

	/* this slab is now fixed until this thread changes it */
	g_assert(self->disk_cacher_slab != NULL);

	/* and then making sure we're ready to write that slab. */
	g_mutex_lock(self->state_mutex);
	while (self->disk_cacher_slab
		    && self->disk_cacher_slab->serial > self->part_first_serial
		    && !elt->cancelled)
	    g_cond_wait(self->state_cond, self->state_mutex);
	stop_serial = self->part_stop_serial;
	g_mutex_unlock(self->state_mutex);

	g_mutex_lock(self->slab_mutex);
	slab = self->disk_cacher_slab;
	eop = eof = FALSE;
	while (!eop && !eof) {
	    /* if we're at the head of the slab train, wait for more data */
	    while (!self->disk_cacher_slab && !elt->cancelled)
		g_cond_wait(self->slab_cond, self->slab_mutex);

            if (elt->cancelled)
                break;

	    /* drop the lock long enough to write the slab; the refcount
	     * protects the slab during this time */
	    slab = self->disk_cacher_slab;
	    g_mutex_unlock(self->slab_mutex);

	    if (full_write(self->disk_cache_write_fd, slab->base, slab->size) < slab->size) {
		send_xmsg_error_and_cancel(self,
		    _("Error writing to disk cache file in '%s': %s"), self->disk_cache_dirname,
		    strerror(errno));
		return NULL;
	    }

	    eof = slab->size < self->slab_size;
	    eop = (slab->serial + 1 == stop_serial);

	    g_mutex_lock(self->slab_mutex);
	    next_slab(self, &self->disk_cacher_slab);
	}
	g_mutex_unlock(self->slab_mutex);

	if (eof) {
	    /* this very thread should have just set this value to NULL, and since it's
	     * EOF, there should not be any 'next' slab */
	    g_assert(self->disk_cacher_slab == NULL);
	    break;
	}
    }

    return NULL;
}

/*
 * Device Thread
 *
 * The device thread's job is to write slabs to self->device, applying whatever
 * streaming algorithms are required.  It does this by alternately getting the
 * next slab from a "slab source" and writing that slab to the device.  Most of
 * the slab source functions assume that self->slab_mutex is held, but may
 * release the mutex (either explicitly or via a g_cond_wait), so it is not
 * valid to assume that any slab pointers remain unchanged after a slab_source
 * function invication.
 */

/* This struct tracks the current state of the slab source */
typedef struct slab_source_state {
    /* temporary slab used for reading from disk */
    Slab *tmp_slab;

    /* current source slice */
    FileSlice *slice;

    /* open fd in current slice, or -1 */
    int slice_fd;

    /* next serial to read from disk */
    guint64 next_serial;

    /* bytes remaining in this slice */
    gsize slice_remaining;
} slab_source_state;

/* Called without the slab_mutex held, this function pre-buffers enough data into the slab
 * train to meet the device's streaming needs. */
static gboolean
slab_source_prebuffer(
    XferDestTaper *self)
{
    XferElement *elt = XFER_ELEMENT(self);
    guint64 prebuffer_slabs = (self->max_memory + self->slab_size - 1) / self->slab_size;
    guint64 i;
    Slab *slab;

    /* pre-buffering is not necessary if we're reading from a disk cache */
    if (self->retry_part && self->part_slices)
	return TRUE;

    g_mutex_lock(self->slab_mutex);

    /* pre-buffering means waiting until we have at least prebuffer_slabs in the
     * slab train ahead of the device_slab, or the newest slab is at EOF. */
    while (!elt->cancelled) {
	gboolean eof_or_eop = FALSE;

	/* see if there's enough data yet */
	for (i = 0, slab = self->device_slab;
	     i < prebuffer_slabs && slab != NULL;
	     i++, slab = slab->next) {
	    eof_or_eop = (slab->size < self->slab_size)
		|| (slab->serial + 1 == self->part_stop_serial);
	}
	if (i == prebuffer_slabs || eof_or_eop)
	    break;

	g_cond_wait(self->slab_cond, self->slab_mutex);
    }

    g_mutex_unlock(self->slab_mutex);

    if (elt->cancelled) {
	self->last_part_successful = FALSE;
	self->no_more_parts = TRUE;
	return FALSE;
    }

    return TRUE;
}

/* Called without the slab_mutex held, this function sets up a new slab_source_state
 * object based on the configuratino of the Xfer Element. */
static inline gboolean
slab_source_setup(
    XferDestTaper *self,
    slab_source_state *state)
{
    state->tmp_slab = NULL;
    state->slice_fd = -1;
    state->slice = NULL;
    state->slice_remaining = 0;
    state->next_serial = G_MAXUINT64;

    /* if we're to retry the part, rewind to the beginning */
    if (self->retry_part) {
	if (self->use_mem_cache) {
	    /* rewind device_slab to point to the mem_cache_slab */
	    g_mutex_lock(self->slab_mutex);
	    if (self->device_slab)
		unref_slab(self, self->device_slab);
	    self->device_slab = self->mem_cache_slab;
	    if(self->device_slab != NULL)
		self->device_slab->refcount++;
	    g_mutex_unlock(self->slab_mutex);
	} else {
	    g_assert(self->part_slices);

	    g_mutex_lock(self->slab_mutex);

	    /* we're going to read from the disk cache until we get to the oldest useful
	     * slab in memory, so it had best exist */
	    g_assert(self->oldest_slab != NULL);

	    /* point device_slab at the oldest slab we have */
	    self->oldest_slab->refcount++;
	    if (self->device_slab)
		unref_slab(self, self->device_slab);
	    self->device_slab = self->oldest_slab;

	    /* and increment it until it is at least the slab we want to start from */
	    while (self->device_slab->serial < self->part_first_serial) {
		next_slab(self, &self->device_slab);
	    }

	    /* get a new, temporary slab for use while reading */
	    state->tmp_slab = alloc_slab(self, TRUE);

	    g_mutex_unlock(self->slab_mutex);

	    if (!state->tmp_slab) {
		self->last_part_successful = FALSE;
		self->no_more_parts = TRUE;
		return FALSE;
	    }

	    state->tmp_slab->size = self->slab_size;
	    state->slice = self->part_slices;
	    state->next_serial = self->part_first_serial;
	}
    }

    /* if the streaming mode requires it, pre-buffer */
    if (self->streaming == STREAMING_REQUIREMENT_DESIRED ||
	self->streaming == STREAMING_REQUIREMENT_REQUIRED) {
	if (!slab_source_prebuffer(self))
	    return FALSE;
    }

    return TRUE;
}

/* Called with the slab_mutex held, this does the work of slab_source_get when
 * reading from the disk cache.  Note that this explicitly releases the
 * slab_mutex during execution - do not depend on any protected values across a
 * call to this function.  The mutex is held on return. */
static Slab *
slab_source_get_from_disk(
    XferDestTaper *self,
    slab_source_state *state,
    guint64 serial)
{
    XferElement *elt = XFER_ELEMENT(self);
    gsize bytes_needed = self->slab_size;
    gsize slab_offset = 0;

    /* NOTE: slab_mutex is held, but we don't need it here, so release it for the moment */
    g_mutex_unlock(self->slab_mutex);

    g_assert(state->next_serial == serial);

    while (bytes_needed > 0) {
	gsize read_size, bytes_read;

	if (state->slice_fd < 0) {
	    g_assert(state->slice);
	    if (state->slice->filename) {
		/* regular cache_inform file - just open it */
		state->slice_fd = open(state->slice->filename, O_RDONLY, 0);
		if (state->slice_fd < 0) {
		    send_xmsg_error_and_cancel(self, _("Could not open '%s' for reading: %s"),
			state->slice->filename, strerror(errno));
		    goto fatal_error;
		}
	    } else {
		/* wait for the disk_cache_thread to open the disk_cache_read_fd, and then copy it */
		g_mutex_lock(self->state_mutex);
		while (self->disk_cache_read_fd == -1 && !elt->cancelled)
		    g_cond_wait(self->state_cond, self->state_mutex);
		state->slice_fd = self->disk_cache_read_fd;
		g_mutex_unlock(self->state_mutex);
	    }

	    if (lseek(state->slice_fd, state->slice->offset, SEEK_SET) == -1) {
		send_xmsg_error_and_cancel(self, _("Could not seek '%s' for reading: %s"),
		    state->slice->filename? state->slice->filename : "(cache file)",
		    strerror(errno));
		goto fatal_error;
	    }

	    state->slice_remaining = state->slice->length;
	}

	read_size = MIN(state->slice_remaining, bytes_needed);
	bytes_read = full_read(state->slice_fd,
			       state->tmp_slab->base + slab_offset,
			       read_size);
	if (bytes_read < read_size) {
            send_xmsg_error_and_cancel(self, _("Error reading '%s': %s"),
		state->slice->filename? state->slice->filename : "(cache file)",
		errno? strerror(errno) : _("Unexpected EOF"));
            goto fatal_error;
	}

	state->slice_remaining -= bytes_read;
	if (state->slice_remaining == 0) {
	    if (close(state->slice_fd) < 0) {
		send_xmsg_error_and_cancel(self, _("Could not close fd %d: %s"),
		    state->slice_fd, strerror(errno));
		goto fatal_error;
	    }
	    state->slice_fd = -1;
	    state->slice = state->slice->next;
	}

	bytes_needed -= bytes_read;
	slab_offset += bytes_read;
    }

    state->tmp_slab->serial = state->next_serial++;

    g_mutex_lock(self->slab_mutex);
    return state->tmp_slab;

fatal_error:
    g_mutex_lock(self->slab_mutex);

    self->last_part_successful = FALSE;
    self->no_more_parts = TRUE;
    return NULL;
}

/* Called with the slab_mutex held, this function gets the slab with the given
 * serial number, waiting if necessary for that slab to be available.  Note
 * that the slab_mutex may be released during execution, although it is always
 * held on return. */
static inline Slab *
slab_source_get(
    XferDestTaper *self,
    slab_source_state *state,
    guint64 serial)
{
    XferElement *elt = (XferElement *)self;

    /* device_slab is only NULL if we're following the slab train, so wait for
     * a new slab */
    if (!self->device_slab) {
	/* if the streaming mode requires it, pre-buffer */
	if (self->streaming == STREAMING_REQUIREMENT_DESIRED) {
	    if (!slab_source_prebuffer(self))
		return NULL;
	} else {
	    while (self->device_slab == NULL && !elt->cancelled)
		g_cond_wait(self->slab_cond, self->slab_mutex);
	}

	if (elt->cancelled)
	    goto fatal_error;
    }

    /* device slab is now set, and only this thread can change it */
    g_assert(self->device_slab);

    /* if the next item in the device slab is the one we want, then the job is
     * pretty easy */
    if (G_LIKELY(serial == self->device_slab->serial))
	return self->device_slab;

    /* otherwise, we're reading from disk */
    g_assert(serial < self->device_slab->serial);
    return slab_source_get_from_disk(self, state, serial);

fatal_error:
    self->last_part_successful = FALSE;
    self->no_more_parts = TRUE;
    return NULL;
}

/* Called without the slab_mutex held, this frees any resources assigned
 * to the slab source state */
static inline void
slab_source_free(
    XferDestTaper *self,
    slab_source_state *state)
{
    if (state->slice_fd != -1)
	close(state->slice_fd);

    if (state->tmp_slab) {
	g_mutex_lock(self->slab_mutex);
	free_slab(state->tmp_slab);
	g_mutex_unlock(self->slab_mutex);
    }
}

/* Called without the slab_mutex, this writes the given slab to the device */
static gboolean
write_slab_to_device(
    XferDestTaper *self,
    Slab *slab)
{
    XferElement *elt = XFER_ELEMENT(self);
    gpointer buf = slab->base;
    gsize remaining = slab->size;

    while (remaining && !elt->cancelled) {
	gsize write_size = MIN(self->block_size, remaining);
	gboolean ok;
	ok = device_write_block(self->device, write_size, buf);
	if (!ok) {
            self->bytes_written += slab->size - remaining;

	    self->last_part_successful = FALSE;
	    self->no_more_parts = FALSE;
	    return FALSE;
	}

	buf += write_size;
	remaining -= write_size;
    }

    if (elt->cancelled) {
	self->last_part_successful = FALSE;
	self->no_more_parts = TRUE;
        return FALSE;
    }

    self->bytes_written += slab->size;
    return TRUE;
}

static XMsg *
device_thread_write_part(
    XferDestTaper *self)
{
    GTimer *timer = g_timer_new();
    XMsg *msg;
    slab_source_state src_state;
    guint64 serial, stop_serial;
    gboolean eof = FALSE;
    int fileno = 0;

    self->last_part_successful = FALSE;
    self->bytes_written = 0;

    if (!device_start_file(self->device, self->part_header))
	goto part_done;

    dumpfile_free(self->part_header);
    self->part_header = NULL;

    fileno = self->device->file;

    if (!slab_source_setup(self, &src_state))
	goto part_done;

    g_timer_start(timer);

    stop_serial = self->part_stop_serial;
    g_mutex_lock(self->slab_mutex);
    for (serial = self->part_first_serial; serial < stop_serial && !eof; serial++) {
	Slab *slab = slab_source_get(self, &src_state, serial);
	g_mutex_unlock(self->slab_mutex);
	if (!slab)
	    goto part_done;

	eof = slab->size < self->slab_size;

	if (!write_slab_to_device(self, slab))
	    goto part_done;

	g_mutex_lock(self->slab_mutex);

	/* if we're reading from the slab train, advance self->device_slab. */
	if (slab == self->device_slab) {
	    next_slab(self, &self->device_slab);
	}
    }
    g_mutex_unlock(self->slab_mutex);

    /* if we write all of the blocks, but the finish_file fails, then likely
     * there was some buffering going on in the device driver, and the blocks
     * did not all make it to permanent storage -- so it's a failed part. */
    if (!device_finish_file(self->device))
	goto part_done;

    slab_source_free(self, &src_state);

    self->last_part_successful = TRUE;
    self->no_more_parts = eof;

part_done:
    g_timer_stop(timer);

    msg = xmsg_new(XFER_ELEMENT(self), XMSG_PART_DONE, 0);
    msg->size = self->bytes_written;
    msg->duration = g_timer_elapsed(timer, NULL);
    msg->partnum = self->partnum;
    msg->fileno = fileno;
    msg->successful = self->last_part_successful;
    msg->eof = self->no_more_parts;

    if (self->last_part_successful)
	self->partnum++;

    g_timer_destroy(timer);

    return msg;
}

/* Called with the status_mutex held, this frees any cached data for
 * a successful part */
static void
release_part_cache(
    XferDestTaper *self)
{
    if (self->use_mem_cache && self->mem_cache_slab) {
	/* move up the mem_cache_slab to point to the first slab in
	 * the next part (probably NULL at this point), so that the
	 * reader can continue reading data into the new mem cache
	 * immediately. */
	g_mutex_lock(self->slab_mutex);
	unref_slab(self, self->mem_cache_slab);
	self->mem_cache_slab = self->device_slab;
	if (self->mem_cache_slab)
	    self->mem_cache_slab->refcount++;
	g_mutex_unlock(self->slab_mutex);
    }

    /* the disk_cache_thread takes care of freeing its cache */
    else if (self->disk_cache_dirname)
	return;

    /* if we have part_slices, fast-forward them. Note that we should have a
     * full part's worth of slices by now. */
    else if (self->part_slices) {
	guint64 bytes_remaining = self->slabs_per_part * self->slab_size;
	FileSlice *slice = self->part_slices;

	/* consume slices until we've eaten the whole part */
	while (bytes_remaining > 0) {
	    if (slice == NULL)
		g_critical("Not all data in part was represented to cache_inform");

	    if (slice->length <= bytes_remaining) {
		bytes_remaining -= slice->length;

		self->part_slices = slice->next;
		g_free(slice->filename);
		g_free(slice);
		slice = self->part_slices;
	    } else {
		slice->length -= bytes_remaining;
		slice->offset += bytes_remaining;
		break;
	    }
	}
    }
}

static gpointer
device_thread(
    gpointer data)
{
    XferDestTaper *self = XFER_DEST_TAPER(data);
    XferElement *elt = XFER_ELEMENT(self);
    XMsg *msg;

    if (self->disk_cache_dirname) {
        GError *error = NULL;
	self->disk_cache_thread = g_thread_create(disk_cache_thread, (gpointer)self, TRUE, &error);
        if (!self->disk_cache_thread) {
            g_critical(_("Error creating new thread: %s (%s)"),
                error->message, errno? strerror(errno) : _("no error code"));
        }
    }

    /* This is the outer loop, that loops once for each split part written to
     * tape. */
    g_mutex_lock(self->state_mutex);
    while (1) {
	/* wait until the main thread un-pauses us, and check that we have
	 * the relevant device info available (block_size) */
	while ((self->paused || !self->block_size) && !elt->cancelled)
	    g_cond_wait(self->state_cond, self->state_mutex);

        if (elt->cancelled)
	    break;

        g_mutex_unlock(self->state_mutex);
	msg = device_thread_write_part(self);
        g_mutex_lock(self->state_mutex);

	/* release any cache of a successful part, but don't bother at EOF */
	if (msg->successful && !msg->eof)
	    release_part_cache(self);

	xfer_queue_message(elt->xfer, msg);

	/* if this is the last part, we're done with the part loop */
	if (self->no_more_parts)
	    break;

	/* pause ourselves and await instructions from the main thread */
	self->paused = TRUE;
    }

    g_mutex_unlock(self->state_mutex);

    /* make sure the other thread is done before we send XMSG_DONE */
    if (self->disk_cache_thread)
        g_thread_join(self->disk_cache_thread);

    /* tell the main thread we're done */
    xfer_queue_message(XFER_ELEMENT(self)->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

/*
 * Incoming data handling
 */

/* called with the slab_mutex held, this adds the reader_slab to the head of
 * the slab train and signals the condition variable. */
static void
add_reader_slab_to_train(
    XferDestTaper *self)
{
    Slab *slab = self->reader_slab;

    if (self->newest_slab) {
	self->newest_slab->next = slab;
	slab->refcount++;

	self->newest_slab->refcount--;
    }

    self->newest_slab = slab; /* steal reader_slab's ref */
    self->reader_slab = NULL;

    /* steal reader_slab's reference for newest_slab */

    /* if any of the other pointers are waiting for this slab, update them */
    if (self->disk_cache_dirname && !self->disk_cacher_slab) {
	self->disk_cacher_slab = slab;
	slab->refcount++;
    }
    if (self->use_mem_cache && !self->mem_cache_slab) {
	self->mem_cache_slab = slab;
	slab->refcount++;
    }
    if (!self->device_slab) {
	self->device_slab = slab;
	slab->refcount++;
    }
    if (!self->oldest_slab) {
	self->oldest_slab = slab;
	slab->refcount++;
    }

    g_cond_broadcast(self->slab_cond);
}

static void
push_buffer_impl(
    XferElement *elt,
    gpointer buf,
    size_t size)
{
    XferDestTaper *self = (XferDestTaper *)elt;
    gpointer p;

    /* wait for device info, so we know how large slabs are, etc. */
    if (G_UNLIKELY(!self->block_size)) {
	g_mutex_lock(self->state_mutex);
	while (!self->block_size && !elt->cancelled)
	    g_cond_wait(self->state_cond, self->state_mutex);
	g_mutex_unlock(self->state_mutex);
    }

    /* do nothing if cancelled */
    if (G_UNLIKELY(elt->cancelled)) {
        goto free_and_finish;
    }

    /* handle EOF */
    if (G_UNLIKELY(buf == NULL)) {
	/* send off the last, probably partial slab */
	g_mutex_lock(self->slab_mutex);

	/* create a new, empty slab if necessary */
	if (!self->reader_slab) {
	    self->reader_slab = alloc_slab(self, FALSE);
            if (!self->reader_slab) {
                /* we've been cancelled while waiting for a slab */
                g_mutex_unlock(self->slab_mutex);
                goto free_and_finish;
            }
	    self->reader_slab->serial = self->next_serial++;
	}

	add_reader_slab_to_train(self);
	g_mutex_unlock(self->slab_mutex);

	goto free_and_finish;
    }

    p = buf;
    while (1) {
	gsize copy_size;

	/* get a fresh slab, if needed */
	if (G_UNLIKELY(!self->reader_slab) || self->reader_slab->size == self->slab_size) {
	    g_mutex_lock(self->slab_mutex);
	    if (self->reader_slab)
		add_reader_slab_to_train(self);
	    self->reader_slab = alloc_slab(self, FALSE);
            if (!self->reader_slab) {
                /* we've been cancelled while waiting for a slab */
                g_mutex_unlock(self->slab_mutex);
                goto free_and_finish;
            }
	    self->reader_slab->serial = self->next_serial++;
	    g_mutex_unlock(self->slab_mutex);
	}

	if (size == 0)
	    break;

	copy_size = MIN(self->slab_size - self->reader_slab->size, size);
	memcpy(self->reader_slab->base+self->reader_slab->size, p, copy_size);

	self->reader_slab->size += copy_size;
	p += copy_size;
	size -= copy_size;
    }

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
    XferDestTaper *self = (XferDestTaper *)elt;
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
    XferDestTaper *self = XFER_DEST_TAPER(elt);
    gboolean rv;

    /* chain up first */
    rv = XFER_ELEMENT_CLASS(parent_class)->cancel(elt, expect_eof);

    /* then signal all of our condition variables, so that threads waiting on them
     * wake up and see elt->cancelled. */
    g_mutex_lock(self->state_mutex);
    g_cond_broadcast(self->state_cond);
    g_mutex_unlock(self->state_mutex);

    g_mutex_lock(self->slab_mutex);
    g_cond_broadcast(self->slab_cond);
    g_cond_broadcast(self->slab_free_cond);
    g_mutex_unlock(self->slab_mutex);

    return rv;
}

static void
start_part_impl(
    XferDestTaper *self,
    gboolean retry_part,
    Device *device,
    dumpfile_t *header)
{
    g_assert(device != NULL);
    g_assert(!device->in_file);
    g_assert(header != NULL);

    g_mutex_lock(self->state_mutex);
    g_assert(self->paused);
    g_assert(!self->no_more_parts);

    self->device = device;
    if (self->part_header)
	dumpfile_free(self->part_header);
    self->part_header = dumpfile_copy(header);

    /* calculate or verify the device-dependent parameters, now that we have
     * a device */
    if (!self->block_size) {
	GValue val;
	self->block_size = self->device->block_size;

	/* The slab size should be large enough to justify the overhead of all
	 * of the mutexes, but it needs to be small enough to have a few slabs
	 * available so that the threads are not constantly waiting on one
	 * another.  The choice is sixteen blocks, not more than a quarter of
	 * the part size, and not more than 10MB.  If we're not using the mem
	 * cache, then avoid exceeding max_memory by keeping the slab size less
	 * than a quarter of max_memory. */

	self->slab_size = self->block_size * 16;
	if (self->part_size)
	    self->slab_size = MIN(self->slab_size, self->part_size / 4);
	self->slab_size = MIN(self->slab_size, 10*1024*1024);
	if (!self->use_mem_cache)
	    self->slab_size = MIN(self->slab_size, self->max_memory / 4);

	/* round slab size up to the nearest multiple of the block size */
	self->slab_size =
	    ((self->slab_size + self->block_size - 1) / self->block_size) * self->block_size;

	/* round part size up to a multiple of the slab size */
	if (self->part_size != 0) {
	    self->slabs_per_part = (self->part_size + self->slab_size - 1) / self->slab_size;
	    self->part_size = self->slabs_per_part * self->slab_size;
	} else {
	    self->slabs_per_part = 0;
	}

	/* fill in the file slice's length, now that we know the real part size */
	if (self->disk_cache_dirname)
	    self->part_slices->length = self->part_size;

	if (self->use_mem_cache) {
	    self->max_slabs = self->slabs_per_part;
	} else {
	    self->max_slabs = (self->max_memory + self->slab_size - 1) / self->slab_size;
	}

	/* Note that max_slabs == 1 will cause deadlocks, due to some assumptions in
	 * alloc_slab, so we check here that it's at least 2. */
	if (self->max_slabs < 2)
	    self->max_slabs = 2;

	g_debug("XferDestTaper using slab_size %zu and max_slabs %ju",
	    self->slab_size, (uintmax_t)self->max_slabs);

        bzero(&val, sizeof(val));
        if (!device_property_get(self->device, PROPERTY_STREAMING, &val)
            || !G_VALUE_HOLDS(&val, STREAMING_REQUIREMENT_TYPE)) {
            g_warning("Couldn't get streaming type for %s", self->device->device_name);
            self->streaming = STREAMING_REQUIREMENT_REQUIRED;
        } else {
            self->streaming = g_value_get_enum(&val);
        }
	g_value_unset(&val);
    } else {
	if (self->block_size != device->block_size) {
	    g_mutex_unlock(self->state_mutex);
	    send_xmsg_error_and_cancel(self,
		_("All devices used by the taper must have the same block size"));
	    return;
	}
    }

    if (retry_part) {
	if (!self->use_mem_cache && !self->part_slices) {
	    g_mutex_unlock(self->state_mutex);
	    send_xmsg_error_and_cancel(self,
		_("Failed part was not cached; cannot retry"));
	    return;
	}
	g_assert(!self->last_part_successful);
	self->retry_part = TRUE;
    } else {
	g_assert(self->last_part_successful);
	self->retry_part = FALSE;
	self->part_first_serial = self->part_stop_serial;
	if (self->part_size != 0) {
	    self->part_stop_serial = self->part_first_serial + self->slabs_per_part;
	} else {
	    /* set part_stop_serial to an effectively infinite value */
	    self->part_stop_serial = G_MAXUINT64;
	}
    }

    self->paused = FALSE;
    g_cond_broadcast(self->state_cond);
    g_mutex_unlock(self->state_mutex);
}

static void
cache_inform_impl(
    XferDestTaper *self,
    const char *filename,
    off_t offset,
    off_t length)
{
    FileSlice *slice, *iter;

    /* do we even need this info? */
    if (self->disk_cache_dirname || self->use_mem_cache || self->part_size == 0)
	return;

    /* handle the (admittedly unlikely) event that length is larger than gsize.
     * Hopefully if sizeof(off_t) = sizeof(gsize), this will get optimized out */
    while (sizeof(off_t) > sizeof(gsize) && length > (off_t)SIZE_MAX) {
	cache_inform_impl(self, filename, offset, (off_t)SIZE_MAX);
	offset += (off_t)SIZE_MAX;
	length -= (off_t)SIZE_MAX;
    }

    slice = g_new0(FileSlice, 1);
    slice->filename = g_strdup(filename);
    slice->offset = offset;
    slice->length = (gsize)length;

    g_mutex_lock(self->state_mutex);
    if (self->part_slices) {
	for (iter = self->part_slices; iter->next; iter = iter->next) {}
	iter->next = slice;
    } else {
	self->part_slices = slice;
    }
    g_mutex_unlock(self->state_mutex);
}

static void
instance_init(
    XferElement *elt)
{
    XferDestTaper *self = XFER_DEST_TAPER(elt);
    elt->can_generate_eof = FALSE;

    self->state_mutex = g_mutex_new();
    self->state_cond = g_cond_new();
    self->slab_mutex = g_mutex_new();
    self->slab_cond = g_cond_new();
    self->slab_free_cond = g_cond_new();

    self->last_part_successful = TRUE;
    self->paused = TRUE;
    self->part_stop_serial = 0;
    self->disk_cache_read_fd = -1;
    self->disk_cache_write_fd = -1;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferDestTaper *self = XFER_DEST_TAPER(obj_self);
    Slab *slab, *next_slab;
    FileSlice *slice, *next_slice;

    if (self->disk_cache_dirname)
	g_free(self->disk_cache_dirname);

    g_mutex_free(self->state_mutex);
    g_cond_free(self->state_cond);

    g_mutex_free(self->slab_mutex);
    g_cond_free(self->slab_cond);
    g_cond_free(self->slab_free_cond);

    /* free the slab train, without reference to the refcounts */
    for (slab = self->oldest_slab; slab != NULL; slab = next_slab) {
        next_slab = slab->next;
        free_slab(slab);
    }
    self->disk_cacher_slab = NULL;
    self->mem_cache_slab = NULL;
    self->device_slab = NULL;
    self->oldest_slab = NULL;
    self->newest_slab = NULL;

    if (self->reader_slab) {
        free_slab(self->reader_slab);
        self->reader_slab = NULL;
    }

    for (slice = self->part_slices; slice; slice = next_slice) {
	next_slice = slice->next;
	g_free(slice->filename);
	g_free(slice);
    }

    if (self->part_header)
	dumpfile_free(self->part_header);

    if (self->disk_cache_read_fd != -1)
	close(self->disk_cache_read_fd); /* ignore error */
    if (self->disk_cache_write_fd != -1)
	close(self->disk_cache_write_fd); /* ignore error */

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferDestTaperClass * selfc)
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
    xdt_klass->cache_inform = cache_inform_impl;
    goc->finalize = finalize_impl;

    klass->perl_class = "Amanda::Xfer::Dest::Taper";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_dest_taper_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestTaperClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestTaper),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestTaper", &info, 0);
    }

    return type;
}

/*
 * Public methods and stubs
 */

void
xfer_dest_taper_start_part(
    XferElement *elt,
    gboolean retry_part,
    Device *device,
    dumpfile_t *header)
{
    XferDestTaperClass *klass;
    g_assert(IS_XFER_DEST_TAPER(elt));

    klass = XFER_DEST_TAPER_GET_CLASS(elt);
    klass->start_part(XFER_DEST_TAPER(elt), retry_part, device, header);
}

void
xfer_dest_taper_cache_inform(
    XferElement *elt,
    const char *filename,
    off_t offset,
    off_t length)
{
    XferDestTaperClass *klass;
    g_assert(IS_XFER_DEST_TAPER(elt));

    klass = XFER_DEST_TAPER_GET_CLASS(elt);
    klass->cache_inform(XFER_DEST_TAPER(elt), filename, offset, length);
}

XferElement *
xfer_dest_taper(
    size_t max_memory,
    guint64 part_size,
    gboolean use_mem_cache,
    const char *disk_cache_dirname)
{
    XferDestTaper *self = (XferDestTaper *)g_object_new(XFER_DEST_TAPER_TYPE, NULL);

    self->max_memory = max_memory;
    self->part_size = part_size;
    self->partnum = 1;

    /* pick only one caching mechanism, caller! */
    g_assert(!use_mem_cache || !disk_cache_dirname);

    /* and if part size is zero, then we don't do any caching */
    if (part_size == 0) {
	g_assert(!use_mem_cache && !disk_cache_dirname);
    }

    self->use_mem_cache = use_mem_cache;
    if (disk_cache_dirname) {
	self->disk_cache_dirname = g_strdup(disk_cache_dirname);

	self->part_slices = g_new0(FileSlice, 1);
	self->part_slices->filename = NULL; /* indicates "use disk_cache_read_fd" */
	self->part_slices->offset = 0;
	self->part_slices->length = 0; /* will be filled in in start_part */
    }

    return XFER_ELEMENT(self);
}
