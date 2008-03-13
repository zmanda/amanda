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
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "amxfer.h"
#include "amanda.h"

/* TODO: use glib chunk allocator */

/* NOTE TO IMPLEMENTERS:
 *
 * When adding a new attribute, make changes in the following places:
 *  - add the attribute to the XMsg struct in xmsg.h
 *  - add the attribute to the comments for the appropriate xmsg_types
 *  - free the attribute in xmsg_free.
 */

/*
 * Methods
 */

XMsg *
xmsg_new(
    XferElement *src,
    xmsg_type type,
    int version)
{
    XMsg *msg = g_new0(XMsg, 1);
    msg->src = src;
    msg->type = type;
    msg->version = version;

    /* messages hold a reference to the XferElement, to avoid dangling
     * pointers. */
    g_object_ref((GObject *)src);

    return msg;
}

void
xmsg_free(
    XMsg *msg)
{
    /* unreference the source */
    g_object_unref((GObject *)msg->src);

    /* and free any allocated attributes */
    if (msg->repr) g_free(msg->repr);
    if (msg->message) g_free(msg->message);

    /* then free the XMsg itself */
    g_free(msg);
}

char *
xmsg_repr(
    XMsg *msg)
{
    /* this just shows the "header" fields for now */
    if (!msg->repr) {
	char *typ = NULL;
	switch (msg->type) {
	    case XMSG_INFO: typ = "INFO"; break;
	    case XMSG_ERROR: typ = "ERROR"; break;
	    case XMSG_DONE: typ = "DONE"; break;
	    default: typ = "**UNKNOWN**"; break;
	}

	msg->repr = vstrallocf("<XMsg@%p type=XMSG_%s src=%s version=%d>",
	    msg, typ, xfer_element_repr(msg->src), msg->version);
    }

    return msg->repr;
}

/*
 * XMsgSource
 */

static gboolean
xmsgsource_prepare(
    GSource *source,
    gint *timeout_)
{
    XMsgSource *xms = (XMsgSource *)source;

    *timeout_ = -1;
    return g_async_queue_length(xms->queue) > 0;
}

static gboolean
xmsgsource_check(
    GSource *source)
{
    XMsgSource *xms = (XMsgSource *)source;

    return g_async_queue_length(xms->queue) > 0;
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

    while ((msg = (XMsg *)g_async_queue_try_pop(xms->queue))) {
	if (my_cb) {
	    my_cb(user_data, msg);
	} else {
	    g_warning("Dropping XMsg from %s because no callback is set", 
		xfer_element_repr(msg->src));
	}
	xmsg_free(msg);
    }

    /* Never automatically un-queue the event source */
    return TRUE;
}

XMsgSource *
xmsgsource_new(void)
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

    xms->queue = g_async_queue_new();

    return xms;
}

void
xmsgsource_destroy(
    XMsgSource *xms)
{
    XMsg *msg;

    /* First, try to empty the queue */
    while ((msg = (XMsg *)g_async_queue_try_pop(xms->queue))) {
	g_warning("Dropping XMsg from %s because the XMsgSource is being destroyed", 
	    xfer_element_repr(msg->src));
	xmsg_free(msg);
    }

    /* Now, unreference the queue.  */
    g_async_queue_unref(xms->queue);

    /* Finally, destroy the GSource itself, so that the callback is no longer
     * called. */
    g_source_destroy((GSource *)xms);
}

void
xmsgsource_set_callback(
    XMsgSource *xms,
    XMsgCallback callback,
    gpointer data)
{
    g_source_set_callback((GSource *)xms, (GSourceFunc)callback, data, NULL);
}

void
xmsgsource_queue_message(
    XMsgSource *xms,
    XMsg *msg)
{
    g_assert(xms != NULL);
    g_assert(msg != NULL);

    g_async_queue_push(xms->queue, (gpointer)msg);

    /* TODO: don't do this if we're in the main thread */
    g_main_context_wakeup(NULL);
}
