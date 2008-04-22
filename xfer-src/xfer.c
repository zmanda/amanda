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
    gint refcount;

    /* All transfer elements for this transfer, in order from
     * source to destination.  This is initialized when the Xfer is
     * created. */
    GPtrArray *elements;

    /* The current status of this transfer */
    xfer_status status;

    /* temporary string for a representation of this transfer */
    char *repr;

    /* GSource and queue for incoming messages */
    struct XMsgSource *msg_source;
    GAsyncQueue *queue;

    /* Number of active elements */
    gint num_active_elements;
};

/* XMsgSource objects are GSource "subclasses" which manage
 * a queue of messages, delivering those messages via callback
 * in the mainloop.  Messages can be *sent* from any thread without
 * any concern for locking, but must only be received in the main
 * thread, in the default GMainContext.
 *
 * An XMsgSource pointer can be cast to a GSource pointer as
 * necessary.
 */
typedef struct XMsgSource {
    GSource source; /* must be the first element of the struct */
    Xfer *xfer;
} XMsgSource;

/* forward prototype */
static XMsgSource *xmsgsource_new(Xfer *xfer);

Xfer *
xfer_new(
    XferElement **elements,
    unsigned int nelements)
{
    Xfer *xfer = g_new0(Xfer, 1);
    unsigned int i;

    g_assert(elements);
    g_assert(nelements >= 2);

    xfer->refcount = 1;
    xfer->status = XFER_INIT;
    xfer->repr = NULL;

    /* Create our message source and corresponding queue */
    xfer->msg_source = xmsgsource_new(xfer);
    g_source_ref((GSource *)xfer->msg_source);
    xfer->queue = g_async_queue_new();

    /* copy the elements in, verifying that they're all XferElement objects */
    xfer->elements = g_ptr_array_sized_new(nelements);
    for (i = 0; i < nelements; i++) {
	g_assert(elements[i] != NULL);
	g_assert(IS_XFER_ELEMENT(elements[i]));
	g_assert(elements[i]->xfer == NULL);

	g_ptr_array_add(xfer->elements, (gpointer)elements[i]);

	g_object_ref(elements[i]);
	elements[i]->xfer = xfer;
    }

    return xfer;
}

void
xfer_ref(
    Xfer *xfer)
{
    ++xfer->refcount;
}

void
xfer_unref(
    Xfer *xfer)
{
    unsigned int i;
    XMsg *msg;

    if (!xfer) return; /* be friendly to NULLs */

    if (--xfer->refcount > 0) return;

    g_assert(xfer != NULL);
    g_assert(xfer->status == XFER_INIT || xfer->status == XFER_DONE);

    /* Divorce ourselves from the message source */
    xfer->msg_source->xfer = NULL;
    g_source_unref((GSource *)xfer->msg_source);
    xfer->msg_source = NULL;

    /* Try to empty the message queue */
    while ((msg = (XMsg *)g_async_queue_try_pop(xfer->queue))) {
	g_warning("Dropping XMsg from %s because the XMsgSource is being destroyed", 
	    xfer_element_repr(msg->elt));
	xmsg_free(msg);
    }
    g_async_queue_unref(xfer->queue);

    /* Free our references to the elements, and also set the 'xfer'
     * attribute of each to NULL, making them "unattached" (although 
     * subsequent reuse of elements is untested). */
    for (i = 0; i < xfer->elements->len; i++) {
	XferElement *elt = (XferElement *)g_ptr_array_index(xfer->elements, i);

	elt->xfer = NULL;
	g_object_unref(elt);
    }

    g_free(xfer);
}

GSource *
xfer_get_source(
    Xfer *xfer)
{
    return (GSource *)xfer->msg_source;
}

void
xfer_queue_message(
    Xfer *xfer, 
    XMsg *msg)
{
    g_assert(xfer != NULL);
    g_assert(msg != NULL);

    g_async_queue_push(xfer->queue, (gpointer)msg);

    /* TODO: don't do this if we're in the main thread */
    g_main_context_wakeup(NULL);
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
    XferElement *xe;

    g_assert(xfer != NULL);
    g_assert(xfer->status == XFER_INIT);
    g_assert(xfer->elements->len >= 2);

    /* set the status to XFER_START and add a reference to our count, so that
     * we are not freed while still in operation.  We'll drop this reference
     * when the status becomes XFER_DONE. */
    xfer_ref(xfer);
    xfer->status = XFER_START;

    /* check that the first element is an XferSource and the last is an XferDest.
     * A source is identified by having no input mechanisms. */
    xe = (XferElement *)g_ptr_array_index(xfer->elements, 0);
    if (xe->input_mech != 0)
	error("Transfer element 0 is not a transfer source");

    /* Similarly, a destination has no output mechanisms. */
    xe = (XferElement *)g_ptr_array_index(xfer->elements, xfer->elements->len-1);
    if (xe->output_mech != 0)
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

static gboolean
xmsgsource_prepare(
    GSource *source,
    gint *timeout_)
{
    XMsgSource *xms = (XMsgSource *)source;

    *timeout_ = -1;
    return xms->xfer && g_async_queue_length(xms->xfer->queue) > 0;
}

static gboolean
xmsgsource_check(
    GSource *source)
{
    XMsgSource *xms = (XMsgSource *)source;

    return xms->xfer && g_async_queue_length(xms->xfer->queue) > 0;
}

static gboolean
xmsgsource_dispatch(
    GSource *source G_GNUC_UNUSED,
    GSourceFunc callback,
    gpointer user_data)
{
    XMsgSource *xms = (XMsgSource *)source;
    XMsgCallback my_cb = (XMsgCallback)callback;
    XMsg *msg;

    /* we're calling perl within this loop, so we have to check that everything is
     * ok on each iteration of the loop. */
    while (xms->xfer 
        && xms->xfer->status == XFER_RUNNING
        && (msg = (XMsg *)g_async_queue_try_pop(xms->xfer->queue))) {
	/* We get first crack at interpreting messages, before calling the
	 * designated callback. */

	switch (msg->type) {
	    /* Intercept and count DONE messages so that we can determine when
	     * the entire transfer is finished. */
	    case XMSG_DONE:
		if (--xms->xfer->num_active_elements <= 0) {
		    /* mark the transfer as done, and decrement the refcount
		     * by one to balance the increment in xfer_start */
		    xms->xfer->status = XFER_DONE;
		    xfer_unref(xms->xfer);
		}
		break;

	    default:
		break;  /* nothing interesting to do */
	}

	if (my_cb) {
	    my_cb(user_data, msg, xms->xfer);
	} else {
	    g_warning("Dropping %s because no callback is set", xmsg_repr(msg));
	}
	xmsg_free(msg);
    }

    /* Never automatically un-queue the event source */
    return TRUE;
}

XMsgSource *
xmsgsource_new(
    Xfer *xfer)
{
    static GSourceFuncs *xmsgsource_funcs = NULL;
    GSource *src;
    XMsgSource *xms;

    /* initialize these here to avoid a compiler warning */
    if (!xmsgsource_funcs) {
	xmsgsource_funcs = g_new0(GSourceFuncs, 1);
	xmsgsource_funcs->prepare = xmsgsource_prepare;
	xmsgsource_funcs->check = xmsgsource_check;
	xmsgsource_funcs->dispatch = xmsgsource_dispatch;
    }

    src = g_source_new(xmsgsource_funcs, sizeof(XMsgSource));
    xms = (XMsgSource *)src;
    xms->xfer = xfer;

    return xms;
}
