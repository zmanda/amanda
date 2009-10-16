/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
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

#include "amxfer.h"
#include "amanda.h"
#include "arglist.h"

/* parent class for XferElement */
static GObjectClass *parent_class = NULL;

/* parent class for XferDest, XferFilter, and XferSource */
static XferElementClass *xfer_element_class = NULL;

/***********************
 * XferElement */

static void
xfer_element_init(
    XferElement *xe)
{
    xe->xfer = NULL;
    xe->output_mech = XFER_MECH_NONE;
    xe->input_mech = XFER_MECH_NONE;
    xe->upstream = xe->downstream = NULL;
    xe->input_fd = xe->output_fd = -1;
    xe->repr = NULL;
}

static void
xfer_element_setup_impl(
    XferElement *elt G_GNUC_UNUSED)
{
}

static gboolean
xfer_element_start_impl(
    XferElement *elt G_GNUC_UNUSED)
{
    return FALSE;
}

static gboolean
xfer_element_cancel_impl(
    XferElement *elt,
    gboolean expect_eof)
{
    elt->cancelled = TRUE;
    elt->expect_eof = expect_eof;
    return elt->can_generate_eof;
}

static gpointer
xfer_element_pull_buffer_impl(
    XferElement *elt G_GNUC_UNUSED,
    size_t *size G_GNUC_UNUSED)
{
    return NULL;
}

static void
xfer_element_push_buffer_impl(
    XferElement *elt G_GNUC_UNUSED,
    gpointer buf G_GNUC_UNUSED,
    size_t size G_GNUC_UNUSED)
{
}

static char *
xfer_element_repr_impl(
    XferElement *elt)
{
    if (!elt->repr) {
	elt->repr = newvstrallocf(elt->repr, "<%s@%p>", 
		G_OBJECT_TYPE_NAME(G_OBJECT(elt)),
		elt);
    }

    return elt->repr;
}

static void
xfer_element_finalize(
    GObject * obj_self)
{
    XferElement *elt = XFER_ELEMENT(obj_self);

    /* free the repr cache */
    if (elt->repr) g_free(elt->repr);

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
xfer_element_class_init(
    XferElementClass * klass)
{
    GObjectClass *goc = (GObjectClass*) klass;

    klass->repr = xfer_element_repr_impl;
    klass->setup = xfer_element_setup_impl;
    klass->start = xfer_element_start_impl;
    klass->cancel = xfer_element_cancel_impl;
    klass->pull_buffer = xfer_element_pull_buffer_impl;
    klass->push_buffer = xfer_element_push_buffer_impl;

    goc->finalize = xfer_element_finalize;

    klass->perl_class = NULL;

    parent_class = g_type_class_peek_parent(klass);
    xfer_element_class = klass;
}

GType
xfer_element_get_type(void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferElementClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) xfer_element_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferElement),
            0 /* n_preallocs */,
            (GInstanceInitFunc) xfer_element_init,
            NULL
        };

        type = g_type_register_static (G_TYPE_OBJECT, "XferElement", &info,
                                       (GTypeFlags)G_TYPE_FLAG_ABSTRACT);
    }

    return type;
}

/*
 * Method stubs
 */

void
xfer_element_unref(
    XferElement *elt)
{
    if (elt) g_object_unref(elt);
}

char *
xfer_element_repr(
    XferElement *elt)
{
    return XFER_ELEMENT_GET_CLASS(elt)->repr(elt);
}

void
xfer_element_setup(
    XferElement *elt)
{
    XFER_ELEMENT_GET_CLASS(elt)->setup(elt);
}

gboolean
xfer_element_start(
    XferElement *elt)
{
    return XFER_ELEMENT_GET_CLASS(elt)->start(elt);
}

gboolean
xfer_element_cancel(
    XferElement *elt,
    gboolean expect_eof)
{
    return XFER_ELEMENT_GET_CLASS(elt)->cancel(elt, expect_eof);
}

gpointer
xfer_element_pull_buffer(
    XferElement *elt,
    size_t *size)
{
    return XFER_ELEMENT_GET_CLASS(elt)->pull_buffer(elt, size);
}

void
xfer_element_push_buffer(
    XferElement *elt,
    gpointer buf,
    size_t size)
{
    XFER_ELEMENT_GET_CLASS(elt)->push_buffer(elt, buf, size);
}

/****
 * Utilities
 */

void
xfer_element_drain_by_pulling(
    XferElement *upstream)
{
    gpointer buf;
    size_t size;

    while ((buf =xfer_element_pull_buffer(upstream, &size))) {
	amfree(buf);
    }
}

void
xfer_element_drain_by_reading(
    int fd)
{
    size_t len;
    char buf[1024];

    while (1) {
	len = full_read(fd, buf, sizeof(buf));
	if (len < sizeof(buf))
	    return;
    }
}

xfer_status
wait_until_xfer_cancelled(
    Xfer *xfer)
{
    xfer_status seen_status;
    g_assert(xfer != NULL);

    g_mutex_lock(xfer->status_mutex);
    while (xfer->status != XFER_CANCELLED && xfer->status != XFER_DONE)
	g_cond_wait(xfer->status_cond, xfer->status_mutex);
    seen_status = xfer->status;
    g_mutex_unlock(xfer->status_mutex);

    return seen_status;
}

void
xfer_element_handle_error(
    XferElement *elt,
    const char *fmt,
    ...)
{
    va_list argp;
    XMsg *msg;

    g_assert(elt != NULL);
    g_assert(elt->xfer != NULL);

    msg = xmsg_new(elt, XMSG_ERROR, 0);

    arglist_start(argp, fmt);
    msg->message = g_strdup_vprintf(fmt, argp);
    arglist_end(argp);

    /* send the XMSG_ERROR */
    xfer_queue_message(elt->xfer, msg);

    /* cancel the transfer */
    xfer_cancel(elt->xfer);

    /* and wait for the cancellation to take effect */
    wait_until_xfer_cancelled(elt->xfer);
}
