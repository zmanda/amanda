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
 * In the most common case, the given transfer elements were just created, and
 * thus have a reference count of one.  This function "steals" that reference,
 * so the caller must not g_object_unref the elements.
 *
 * @param elements: array of pointers to transfer elements, in order from source
 *     to destination
 * @param nelements: length of 'elements'
 * @returns: new Xfer object
 */
Xfer *xfer_new(struct XferElement **elements, unsigned int nelements);

/* Free a transfer.  This is a fatal error if the transfer is not in state
 * XFER_INIT or XFER_DONE, as asynchronous operations may still be in progress.
 *
 * @param xfer: the Xfer object
 */
void xfer_free(Xfer *xfer);

/* Set the function to be called when messages arrive from elements of
 * this transfer.
 *
 * @param xfer: the transfer
 * @param callback: function to call
 * @param data: data to be passed to callback
 */
typedef void (*XferMsgCallback)(gpointer data, struct XMsg *msg, Xfer *xfer);
void xfer_set_callback(Xfer *xfer, XferMsgCallback callback, gpointer data);

/* Queue a message for delivery via this transfer.
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

#endif /* XFER_H */
