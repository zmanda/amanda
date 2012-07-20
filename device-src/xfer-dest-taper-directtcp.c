/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

#include "amanda.h"
#include "amxfer.h"
#include "xfer-device.h"
#include "arglist.h"
#include "conffile.h"

/* A transfer destination that writes and entire dumpfile to one or more files
 * on one or more devices via DirectTCP, handling the work of spanning a
 * directtcp connection over multiple devices.  Note that this assumes the
 * devices support early EOM warning. */

/*
 * Xfer Dest Taper DirectTCP
 */

static GType xfer_dest_taper_directtcp_get_type(void);
#define XFER_DEST_TAPER_DIRECTTCP_TYPE (xfer_dest_taper_directtcp_get_type())
#define XFER_DEST_TAPER_DIRECTTCP(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_taper_directtcp_get_type(), XferDestTaperDirectTCP)
#define XFER_DEST_TAPER_DIRECTTCP_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_taper_directtcp_get_type(), XferDestTaperDirectTCP const)
#define XFER_DEST_TAPER_DIRECTTCP_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_taper_directtcp_get_type(), XferDestTaperDirectTCPClass)
#define IS_XFER_DEST_TAPER_DIRECTTCP(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_taper_directtcp_get_type ())
#define XFER_DEST_TAPER_DIRECTTCP_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_taper_directtcp_get_type(), XferDestTaperDirectTCPClass)

static GObjectClass *parent_class = NULL;

typedef struct XferDestTaperDirectTCP {
    XferDestTaper __parent__;

    /* constructor parameters */
    guint64 part_size; /* (bytes) */

    /* thread */
    GThread *worker_thread;

    /* state (governs everything below) */
    GMutex *state_mutex;

    /* part parameters */
    Device *volatile device; /* device to write to (refcounted) */
    dumpfile_t *volatile part_header;

    /* did the device listen proceed without error? */
    gboolean listen_ok;

    /* part number in progress */
    volatile guint64 partnum;

    /* connection we're writing to (refcounted) */
    DirectTCPConnection *conn;

    /* is the element paused, waiting to start a new part? this is set to FALSE
     * by the main thread to start a part, and the worker thread waits on the
     * corresponding condition variable. */
    volatile gboolean paused;
    GCond *paused_cond;
    GCond *abort_accept_cond; /* condition to trigger to abort an accept */

} XferDestTaperDirectTCP;

typedef struct {
    XferDestTaperClass __parent__;
} XferDestTaperDirectTCPClass;

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
    g_debug("XDT: %s", msg);
}
/*
 * Worker Thread
 */

static gpointer
worker_thread(
    gpointer data)
{
    XferElement *elt = (XferElement *)data;
    XferDestTaperDirectTCP *self = (XferDestTaperDirectTCP *)data;
    GTimer *timer = g_timer_new();
    int result;

    /* This thread's job is to accept() an incoming connection, then call
     * write_from_connection for each part, and then close the connection */

    /* If the device_listen failed, then we will soon be cancelled, so wait
     * for that to occur and then send XMSG_DONE */
    if (!self->listen_ok) {
	DBG(2, "listen failed; waiting for cancellation without attempting an accept");
	wait_until_xfer_cancelled(elt->xfer);
	goto send_xmsg_done;
    }

    g_mutex_lock(self->state_mutex);

    /* first, accept a new connection from the device */
    DBG(2, "accepting DirectTCP connection on device %s", self->device->device_name);
    result = device_accept_with_cond(self->device, &self->conn,
				     self->state_mutex,
				     self->abort_accept_cond);
    if (result == 2) {
	xfer_cancel_with_error(XFER_ELEMENT(self),
	    "accepting DirectTCP connection: %s",
	    device_error_or_status(self->device));
	g_mutex_unlock(self->state_mutex);
	return NULL;
    } else if (result == 1) {
	g_mutex_unlock(self->state_mutex);
	return NULL;
    }

    DBG(2, "connection accepted; sending XMSG_READY");
    xfer_queue_message(elt->xfer, xmsg_new(elt, XMSG_READY, 0));

    /* round the part size up to the next multiple of the block size */
    if (self->part_size) {
	self->part_size += self->device->block_size-1;
	self->part_size -= self->part_size % self->device->block_size;
    }

    /* now loop until we're out of parts */
    while (1) {
	guint64 size;
	int fileno;
	XMsg *msg = NULL;
	gboolean eom, eof;

	/* wait to be un-paused */
	while (!elt->cancelled && self->paused) {
	    DBG(9, "waiting to be un-paused");
	    g_cond_wait(self->paused_cond, self->state_mutex);
	}
	DBG(9, "done waiting");

	if (elt->cancelled)
	    break;

	DBG(2, "writing part to %s", self->device->device_name);
	if (!device_start_file(self->device, self->part_header) || self->device->is_eom) {
	    /* this is not fatal to the transfer, since no data was lost.  We
	     * just need a new device.  The scribe special-cases 0-byte parts, and will
	     * not record this in the catalog. */

	    /* clean up */
	    dumpfile_free(self->part_header);
	    self->part_header = NULL;

	    goto empty_part;
	}

	dumpfile_free(self->part_header);
	self->part_header = NULL;

	fileno = self->device->file;
	g_assert(fileno > 0);

	/* write the part */
	g_timer_start(timer);
	if (!device_write_from_connection(self->device,
		self->part_size, &size)) {
	    /* even if this is just a physical EOM, we may have lost data, so
	     * the whole transfer is dead. */
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"Error writing from DirectTCP connection: %s",
		device_error_or_status(self->device));
	    goto cancelled;
	}
	g_timer_stop(timer);

	eom = self->device->is_eom;
	eof = self->device->is_eof;

	/* finish the file, even if we're at EOM, but if this fails then we may
	 * have lost data */
	if (!device_finish_file(self->device)) {
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"Error finishing tape file: %s",
		device_error_or_status(self->device));
	    goto cancelled;
	}

	/* if we wrote zero bytes and reached EOM, then this is an empty part */
	if (eom && !eof && size == 0) {
	    goto empty_part;
	}

	msg = xmsg_new(XFER_ELEMENT(self), XMSG_PART_DONE, 0);
	msg->size = size;
	msg->duration = g_timer_elapsed(timer, NULL);
	msg->partnum = self->partnum;
	msg->fileno = fileno;
	msg->successful = TRUE;
	msg->eom = eom;
	msg->eof = eof;

	/* time runs backward on some test boxes, so make sure this is positive */
	if (msg->duration < 0) msg->duration = 0;

	xfer_queue_message(elt->xfer, msg);

        self->partnum++;

	/* we're done at EOF */
	if (eof)
	    break;

	/* wait to be unpaused again */
	self->paused = TRUE;
	continue;

empty_part:
	msg = xmsg_new(XFER_ELEMENT(self), XMSG_PART_DONE, 0);
	msg->size = 0;
	msg->duration = 0;
	msg->partnum = 0;
	msg->fileno = 0;
	msg->successful = TRUE;
	msg->eom = TRUE;
	msg->eof = FALSE;
	xfer_queue_message(elt->xfer, msg);

	/* wait to be unpaused again */
	self->paused = TRUE;
	continue;

cancelled:
	/* drop the mutex and wait until all elements have been cancelled
	 * before closing the connection */
	g_mutex_unlock(self->state_mutex);
	wait_until_xfer_cancelled(elt->xfer);
	g_mutex_lock(self->state_mutex);
	break;
    }

    /* close the DirectTCP connection */
    directtcp_connection_close(self->conn);
    g_object_unref(self->conn);
    self->conn = NULL;

    g_mutex_unlock(self->state_mutex);
    g_timer_destroy(timer);

send_xmsg_done:
    xfer_queue_message(elt->xfer, xmsg_new(XFER_ELEMENT(self), XMSG_DONE, 0));

    return NULL;
}

/*
 * Element mechanics
 */

static gboolean
setup_impl(
    XferElement *elt)
{
    XferDestTaperDirectTCP *self = (XferDestTaperDirectTCP *)elt;

    /* start the device listening, and get the addresses */
    if (!device_listen(self->device, TRUE, &elt->input_listen_addrs)) {
	elt->input_listen_addrs = NULL;
	xfer_cancel_with_error(XFER_ELEMENT(self),
	    "Error starting DirectTCP listen: %s",
	    device_error_or_status(self->device));
	self->listen_ok = FALSE;
	return FALSE;
    }

    self->listen_ok = TRUE;
    return TRUE;
}

static gboolean
start_impl(
    XferElement *elt)
{
    XferDestTaperDirectTCP *self = (XferDestTaperDirectTCP *)elt;
    GError *error = NULL;

    self->paused = TRUE;

    /* start up the thread */
    self->worker_thread = g_thread_create(worker_thread, (gpointer)self, TRUE, &error);
    if (!self->worker_thread) {
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
    XferDestTaperDirectTCP *self = XFER_DEST_TAPER_DIRECTTCP(elt);
    gboolean rv;

    /* chain up first */
    rv = XFER_ELEMENT_CLASS(parent_class)->cancel(elt, expect_eof);

    /* signal all of the condition variables to realize that we're no
     * longer paused */
    g_mutex_lock(self->state_mutex);
    g_cond_broadcast(self->paused_cond);
    g_cond_broadcast(self->abort_accept_cond);
    g_mutex_unlock(self->state_mutex);

    return rv;
}

static void
start_part_impl(
    XferDestTaper *xdtself,
    gboolean retry_part,
    dumpfile_t *header)
{
    XferDestTaperDirectTCP *self = XFER_DEST_TAPER_DIRECTTCP(xdtself);

    /* the only way self->device can become NULL is if use_device fails, in
     * which case an error is already queued up, so just return silently */
    if (self->device == NULL)
	return;

    g_assert(!self->device->in_file);
    g_assert(header != NULL);

    DBG(1, "start_part(retry_part=%d)", retry_part);

    g_mutex_lock(self->state_mutex);
    g_assert(self->paused);

    if (self->part_header)
	dumpfile_free(self->part_header);
    self->part_header = dumpfile_copy(header);

    DBG(1, "unpausing");
    self->paused = FALSE;
    g_cond_broadcast(self->paused_cond);

    g_mutex_unlock(self->state_mutex);
}

static void
use_device_impl(
    XferDestTaper *xdtself,
    Device *device)
{
    XferDestTaperDirectTCP *self = XFER_DEST_TAPER_DIRECTTCP(xdtself);

    /* short-circuit if nothing is changing */
    if (self->device == device)
	return;

    g_mutex_lock(self->state_mutex);

    if (self->device)
	g_object_unref(self->device);
    self->device = NULL;

    /* if we already have a connection, then make this device use it */
    if (self->conn) {
	if (!device_use_connection(device, self->conn)) {
	    /* queue up an error for later, and leave the device NULL.
	     * start_part will see this and fail silently */
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		_("Failed part was not cached; cannot retry"));
	    return;
	}
    }

    self->device = device;
    g_object_ref(device);

    g_mutex_unlock(self->state_mutex);
}

static guint64
get_part_bytes_written_impl(
    XferDestTaper *xdtself G_GNUC_UNUSED)
{
    /* This operation is not supported for this taper dest.  Maybe someday. */
    return 0;
}

static void
instance_init(
    XferElement *elt)
{
    XferDestTaperDirectTCP *self = XFER_DEST_TAPER_DIRECTTCP(elt);
    elt->can_generate_eof = FALSE;

    self->worker_thread = NULL;
    self->paused = TRUE;
    self->conn = NULL;
    self->state_mutex = g_mutex_new();
    self->paused_cond = g_cond_new();
    self->abort_accept_cond = g_cond_new();
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferDestTaperDirectTCP *self = XFER_DEST_TAPER_DIRECTTCP(obj_self);

    if (self->conn)
	g_object_unref(self->conn);
    self->conn = NULL;

    if (self->device)
	g_object_unref(self->device);
    self->device = NULL;

    if (self->device)
	g_object_unref(self->device);
    self->device = NULL;

    g_mutex_free(self->state_mutex);
    g_cond_free(self->paused_cond);
    g_cond_free(self->abort_accept_cond);

    if (self->part_header)
	dumpfile_free(self->part_header);
    self->part_header = NULL;

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferDestTaperDirectTCPClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    XferDestTaperClass *xdt_klass = XFER_DEST_TAPER_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_DIRECTTCP_LISTEN, XFER_MECH_NONE, 0, 0},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    klass->start = start_impl;
    klass->setup = setup_impl;
    klass->cancel = cancel_impl;
    xdt_klass->start_part = start_part_impl;
    xdt_klass->use_device = use_device_impl;
    xdt_klass->get_part_bytes_written = get_part_bytes_written_impl;
    goc->finalize = finalize_impl;

    klass->perl_class = "Amanda::Xfer::Dest::Taper::DirectTCP";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

static GType
xfer_dest_taper_directtcp_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestTaperDirectTCPClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestTaperDirectTCP),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_DEST_TAPER_TYPE, "XferDestTaperDirectTCP", &info, 0);
    }

    return type;
}

/*
 * Constructor
 */

XferElement *
xfer_dest_taper_directtcp(Device *first_device, guint64 part_size)
{
    XferDestTaperDirectTCP *self = (XferDestTaperDirectTCP *)g_object_new(XFER_DEST_TAPER_DIRECTTCP_TYPE, NULL);

    g_assert(device_directtcp_supported(first_device));

    self->part_size = part_size;
    self->device = first_device;
    self->partnum = 1;
    g_object_ref(self->device);

    return XFER_ELEMENT(self);
}
