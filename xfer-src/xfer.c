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

#include "amxfer.h"
#include "amanda.h"

/*
 * "Class" declaration
 */

struct Xfer {
    /* All transfer elements for this transfer, in order from
     * source to destination.  This is initialized when the Xfer is
     * created. */
    GPtrArray *elements;

    /* The current status of this transfer */
    xfer_status status;

    /* temporary string for a representation of this transfer */
    char *repr;

    /* GSource for incoming messages */
    struct XMsgSource *msg_source;

    /* Callback for messages */
    XferMsgCallback msg_callback;
    gpointer msg_callback_data;

    /* Number of active elements */
    gint num_active_elements;
};

/* Callback for incoming XMsgs.  This gives an xfer a chance to intercept and
 * track incoming messages, before sending them on to the xfer's own
 * XferMsgCallback.  To be clear, there are two layers of callbacks here;
 * this function is registered with xmsgsource_set_callback, and it in turn
 * calls the function registered with xfer_set_callback.
 */
static void
xmsg_callback(
    gpointer data,
    XMsg *msg)
{
    Xfer *xfer = (Xfer *)data;

    /* ignore the message if the xfer isn't running yet */
    if (xfer->status != XFER_RUNNING) return;

    /* do what we like with the message */
    switch (msg->type) {
	/* Intercept and count DONE messages so that we can determine when
	 * the entire transfer is finished. */
	case XMSG_DONE:
	    xfer->num_active_elements--;
	    if (xfer->num_active_elements == 0)
		xfer->status = XFER_DONE;
	    break;

	default:
	    break;  /* nothing interesting to do */
    }

    /* and then hand it off to our own callback */
    if (xfer->msg_callback) {
	xfer->msg_callback(xfer->msg_callback_data, msg, xfer);
    }
}

Xfer *
xfer_new(
    XferElement **elements,
    unsigned int nelements)
{
    Xfer *xfer = g_new0(Xfer, 1);
    unsigned int i;

    g_assert(elements);
    g_assert(nelements >= 2);

    xfer->status = XFER_INIT;
    xfer->repr = NULL;

    /* Create and attach our message source */
    xfer->msg_source = xmsgsource_new();
    g_source_attach((GSource *)xfer->msg_source, NULL);
    xmsgsource_set_callback(xfer->msg_source,
	xmsg_callback, (gpointer)xfer);

    /* copy the elements in, verifying that they're all XferElement objects */
    xfer->elements = g_ptr_array_sized_new(nelements);
    for (i = 0; i < nelements; i++) {
	g_assert(elements[i] != NULL);
	g_assert(IS_XFER_ELEMENT(elements[i]));

	g_ptr_array_add(xfer->elements, (gpointer)elements[i]);
	elements[i]->xfer = xfer;
    }

    return xfer;
}

void
xfer_free(
    Xfer *xfer)
{
    unsigned int i;

    g_assert(xfer != NULL);
    g_assert(xfer->status == XFER_INIT || xfer->status == XFER_DONE);

    /* We "borrowed" references to each of these elements in xfer_new; now
     * we're going to give up those references, and also set the 'xfer'
     * attribute of each to NULL. */
    for (i = 0; i < xfer->elements->len; i++) {
	XferElement *elt = (XferElement *)g_ptr_array_index(xfer->elements, i);

	elt->xfer = NULL;
	g_object_unref(elt);
    }

    xmsgsource_destroy(xfer->msg_source);

    g_free(xfer);
}

void
xfer_set_callback(
    Xfer *xfer,
    XferMsgCallback callback,
    gpointer data)
{
    xfer->msg_callback = callback;
    xfer->msg_callback_data = data;
}

void
xfer_queue_message(
    Xfer *xfer, 
    XMsg *msg)
{
    g_assert(xfer != NULL);
    g_assert(msg != NULL);

    xmsgsource_queue_message(xfer->msg_source, msg);
}

xfer_status
xfer_get_status(Xfer *xfer)
{
    return xfer->status;
}

char *
xfer_repr(
    Xfer *xfer)
{
    unsigned int i;

    if (!xfer->repr) {
	xfer->repr = newvstrallocf(xfer->repr, "<Xfer@%p (", xfer);
	for (i = 0; i < xfer->elements->len; i++) {
	    XferElement *elt = (XferElement *)g_ptr_array_index(xfer->elements, i);
	    xfer->repr = newvstralloc(xfer->repr,
		xfer->repr, (i==0)?"":" -> ", xfer_element_repr(elt), NULL);
	}
	xfer->repr = newvstralloc(xfer->repr, xfer->repr, ")>", NULL);
    }

    return xfer->repr;
}

void
xfer_start(
    Xfer *xfer)
{
    unsigned int i;

    g_assert(xfer != NULL);
    g_assert(xfer->status == XFER_INIT);
    g_assert(xfer->elements->len >= 2);

    xfer->status = XFER_START;

    /* check that the first element is an XferSource and the last is an XferDest.
     * Any non-filters in the middle will be un-linkable later on, so we don't
     * check them explicitly. */
    if (!IS_XFER_SOURCE((XferElement *)g_ptr_array_index(xfer->elements, 0)))
	error("Transfer element 0 is not a transfer source");
    if (!IS_XFER_DEST((XferElement *)g_ptr_array_index(xfer->elements, xfer->elements->len-1)))
	error("Last transfer element is not a transfer destination");

    /* link each of the elements together, in order from destination to source */
    for (i = xfer->elements->len - 1; i >= 1; i--) {
	XferElement *xe = (XferElement *)g_ptr_array_index(xfer->elements, i-1);
	XferElement *next_xe = (XferElement *)g_ptr_array_index(xfer->elements, i);
	if (!xfer_element_link_to(xe, next_xe)) {
	    error("Cannot link transfer elements at indices %d and %d", i-1, i);
	    /* NOTREACHED */
	}
    }

    /* now tell them all to start, again from destination to start */
    for (i = xfer->elements->len; i >= 1; i--) {
	XferElement *xe = (XferElement *)g_ptr_array_index(xfer->elements, i-1);
	xfer_element_start(xe);
    }

    xfer->num_active_elements = xfer->elements->len;
    xfer->status = XFER_RUNNING;
}
