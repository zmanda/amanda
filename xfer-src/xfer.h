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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* An Xfer abstracts an active data transfer through the Amanda core.
 */

#ifndef XFER_H
#define XFER_H

#include <glib.h>

/* An Xfer represents a flow of data from a source, via zero or more filters,
 * to a destination.  Sources, filters, and destinations are "transfer elements".
 * The job of the Xfer is glue together a sequence of elements, and provide a
 * dispatch point for messages from those elements to the caller.
 *
 * Xfers are not implemented as GObjects because there is no reason to subclass an
 * Xfer or apply any of the other features that come with GObject.
 */

/* The moment-to-moment state of a transfer */
typedef enum {
    XFER_INIT = 1,	/* initializing */
    XFER_START = 2,	/* starting */
    XFER_RUNNING = 3,	/* data flowing */
    XFER_CANCELLING = 4,/* cancellation begun */
    XFER_CANCELLED = 5, /* all elements cancelled; draining data */
    XFER_DONE = 6,	/* data no longer flowing */
} xfer_status;

/* forward declarations */
struct XferElement;
struct XMsgSource;
struct XMsg;

/*
 * "Class" declaration
 */

struct Xfer {
    /* The current status of this transfer.  This is read-only, and 
     * must only be accessed from the main thread or with status_mutex
     * held. */
    xfer_status status;

    /* lock this while checking status in a thread
     * other than the main thread */
    GMutex *status_mutex;

    /* and wait on this for status changes */
    GCond *status_cond;

    /* -- remaining fields are private -- */

    gint refcount;

    /* All transfer elements for this transfer, in order from
     * source to destination.  This is initialized when the Xfer is
     * created. */
    GPtrArray *elements;

    /* temporary string for a representation of this transfer */
    char *repr;

    /* GSource and queue for incoming messages */
    struct XMsgSource *msg_source;
    GAsyncQueue *queue;

    /* Number of active elements remaining (a.k.a. the number of
     * XMSG_DONE messages to expect) */
    gint num_active_elements;
};

typedef struct Xfer Xfer;

/* Note that all functions must be called from the main thread unless
 * otherwise noted */

/* Create a new Xfer object, which should later be freed with xfref_free.
 *
 * This function adds a reference to each element.  The caller should
 * unreference the elements if it does not intend to use them directly.
 * The Xfer returned has a refcount of one.
 *
 * @param elements: array of pointers to transfer elements, in order from source
 *     to destination
 * @param nelements: length of 'elements'
 * @returns: new Xfer object
 */
Xfer *xfer_new(struct XferElement **elements, unsigned int nelements);

/* Increase the reference count of a transfer.
 *
 * @param xfer: the transfer
 */
void xfer_ref(Xfer *xfer);

/* Decrease the reference count of a transfer, possibly freeing it.  A running
 * transfer (state neither XFER_INIT nor XFER_DONE) will not be freed.
 *
 * @param xfer: the transfer
 */
void xfer_unref(Xfer *xfer);

/* Get a GSource which will produce events corresponding to messages from
 * this transfer.  This is a "peek" operation, so the reference count for the
 * GSource is not affected.  Note that the same GSource is returned on every
 * call for a particular transfer.
 *
 * @returns: GSource object
 */
GSource *xfer_get_source(Xfer *xfer);

/* Typedef for the callback to be set on the GSource returned from
 * xfer_get_source.
 */
typedef void (*XMsgCallback)(gpointer data, struct XMsg *msg, Xfer *xfer);

/* Queue a message for delivery via this transfer's GSource.  This can
 * be called in any thread.
 *
 * @param xfer: the transfer
 * @param msg: the message to queue
 */
void xfer_queue_message(Xfer *xfer, struct XMsg *msg);

/* Get a representation of this transfer.  The string belongs to the transfer, and
 * will be freed when the transfer is freed.
 *
 * @param xfer: the Xfer object
 * @returns: statically allocated string
 */
char *xfer_repr(Xfer *xfer);

/* Start a transfer.  This function will fail with an error message if it is
 * unable to set up the transfer (e.g., if the elements cannot be connected
 * correctly).
 *
 * @param xfer: the Xfer object
 */
void xfer_start(Xfer *xfer);

/* Abort a running transfer.  This essentially tells the source to stop
 * producing data and allows the remainder of the transfer to "drain".  Thus
 * the transfer will signal its completion "normally" some time after
 * xfer_cancel returns.  In particular, the state transitions will occur
 * as follows:
 *
 * - XFER_RUNNING
 * - xfer_cancel()  (note state may still be XFER_RUNNING on return)
 * - XFER_CANCELLING
 * - (individual elements' cancel() methods are invoked)
 * - XFER_CANCELLED
 * - (data drains from the transfer)
 * - XFER_DONE
 *
 * This function can be called from any thread at any time.  It will return
 * without blocking.
 *
 * @param xfer: the Xfer object
 */
void xfer_cancel(Xfer *xfer);

#endif /* XFER_H */
