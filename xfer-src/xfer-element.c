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
    xe->output_mech = 0;
    xe->input_mech = 0;

    xe->pipe[0] = xe->pipe[1] = -1;
    xe->repr = NULL;
}

static gboolean
xfer_element_link_to_impl(
    XferElement *elt, 
    XferElement *downstream)
{
    xfer_output_mech output = elt->output_mech;
    xfer_input_mech input = downstream->input_mech;

    g_debug("Linking %s to %s", xfer_element_repr(elt), xfer_element_repr(downstream));

    /* check the possibilities for linking ELT to DOWNSTREAM, in order of 
     * efficiency */

    /* can we simply connect two fd's to one another? */
    if (output & MECH_OUTPUT_HAVE_READ_FD && input & MECH_INPUT_READ_GIVEN_FD) {
	int fd = 0;
	XFER_ELEMENT_GET_CLASS(elt)->setup_output(elt, MECH_OUTPUT_HAVE_READ_FD, &fd);
	XFER_ELEMENT_GET_CLASS(downstream)->setup_input(downstream, MECH_INPUT_READ_GIVEN_FD, &fd);
	g_debug(".. %s provides fd %d to %s.",
	    xfer_element_repr(elt), fd, xfer_element_repr(downstream));

    } else if (output & MECH_OUTPUT_WRITE_GIVEN_FD && input & MECH_INPUT_HAVE_WRITE_FD) {
	int fd = 0;
	XFER_ELEMENT_GET_CLASS(downstream)->setup_input(downstream, MECH_INPUT_HAVE_WRITE_FD, &fd);
	XFER_ELEMENT_GET_CLASS(elt)->setup_output(elt, MECH_OUTPUT_WRITE_GIVEN_FD, &fd);
	g_debug(".. %s provides fd %d to %s.",
	    xfer_element_repr(downstream), fd, xfer_element_repr(elt));

    /* maybe we can use a pipe? */
    } else if (output & MECH_OUTPUT_WRITE_GIVEN_FD && input & MECH_INPUT_READ_GIVEN_FD) {
	if (pipe(elt->pipe) != 0) {
	    g_critical("Cannot open pipe: %s", strerror(errno));
	    /* NOTREACHED */
	}
	XFER_ELEMENT_GET_CLASS(elt)->setup_output(elt, 
	    MECH_OUTPUT_WRITE_GIVEN_FD, &elt->pipe[1]);
	XFER_ELEMENT_GET_CLASS(downstream)->setup_input(downstream, 
	    MECH_INPUT_READ_GIVEN_FD, &elt->pipe[0]);
	g_debug(".. new pipe created (r: %d, w: %d)", elt->pipe[0], elt->pipe[1]);

    /* TODO:
    try a consumer/producer queue
    } else if (output & MECH_OUTPUT_PRODUCER && input & MECH_INPUT_CONSUMER) {
	g_critical("not imp");

    can we wrap the input with an fd_read_consumer? 
    } else if (output & MECH_OUTPUT_PRODUCER && input & MECH_INPUT_READ_GIVEN_FD) {
	g_critical("not imp");

    } else if (output & MECH_OUTPUT_PRODUCER && input & MECH_INPUT_HAVE_WRITE_FD) {
	g_critical("not imp");

    can we wrap the output with an fd_write_producer?
    } else if (output & MECH_OUTPUT_WRITE_GIVEN_FD && input & MECH_INPUT_CONSUMER) {
	g_critical("not imp");

    } else if (output & MECH_OUTPUT_HAVE_READ_FD && input & MECH_INPUT_CONSUMER) {
	g_critical("not imp");

    ok, let's try both an fd_read_consumer and an fd_write_producer; this is
    the least efficient option.
    } else if (output & MECH_OUTPUT_HAVE_READ_FD && input & MECH_INPUT_HAVE_WRITE_FD) {
	g_critical("not imp");
    */
    } else {
	g_debug(".. failed");
	return FALSE;
    }

    return TRUE;
}

/* empty method standing in for start() and abort() */
static void
xfer_element_empty_method(
    XferElement *elt G_GNUC_UNUSED)
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

    /* close our pipe, if we've been using one */
    if (elt->pipe[0] != -1) close(elt->pipe[0]);
    if (elt->pipe[1] != -1) close(elt->pipe[1]);

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

    klass->link_to = xfer_element_link_to_impl;
    klass->repr = xfer_element_repr_impl;
    klass->start = xfer_element_empty_method;
    klass->abort = xfer_element_empty_method;
    klass->setup_output = NULL;
    klass->setup_input = NULL;

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

gboolean
xfer_element_link_to(
    XferElement *elt,
    XferElement *successor)
{
    return XFER_ELEMENT_GET_CLASS(elt)->link_to(elt, successor);
}

char *
xfer_element_repr(
    XferElement *elt)
{
    return XFER_ELEMENT_GET_CLASS(elt)->repr(elt);
}

void
xfer_element_start(
    XferElement *elt)
{
    XFER_ELEMENT_GET_CLASS(elt)->start(elt);
}

void
xfer_element_abort(
    XferElement *elt)
{
    XFER_ELEMENT_GET_CLASS(elt)->abort(elt);
}
