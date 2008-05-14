/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#include "amxfer.h"
#include "amanda.h"

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

static void
xfer_element_abort_impl(
    XferElement *elt G_GNUC_UNUSED)
{
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
	/* TODO: find a way to get the class name */
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
    klass->abort = xfer_element_abort_impl;
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

void
xfer_element_abort(
    XferElement *elt)
{
    XFER_ELEMENT_GET_CLASS(elt)->abort(elt);
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
