/*
 * Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as
 * published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
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
    XFER_DONE = 4,	/* data no longer flowing */
} xfer_status;

/* forward declarations */
struct XferElement;
struct XMsg;

/* Opaque type for an Xfer */
typedef struct Xfer Xfer;

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

/* Queue a message for delivery via this transfer's GSource.
 *
 * @param xfer: the transfer
 * @param msg: the message to queue
 */
void xfer_queue_message(Xfer *xfer, struct XMsg *msg);

/* Get the transfer's status.  The easiest way to determine that a transfer
 * has finished is to call this function for every XMSG_DONE message; when the
 * status is XFER_DONE, then all elements have finished.
 *
 * @return: transfer status
 */
xfer_status xfer_get_status(Xfer *xfer);

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

/* Register something that is actively processing the datastream, and will
 * eventually send XMSG_DONE.  "Something" can be a thread, a process, or
 * even an asynchronous event handler.  When all registered processing has
 * sent XMSG_DONE, then the transfer itself is done.
 *
 * This function should be called in the main thread, usually from a transfer's
 * start() method.
 *
 * @param xfer: the Xfer object
 */
void xfer_will_send_xmsg_done(Xfer *xfer);

#endif /* XFER_H */
