/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "sockaddr-util.h"
#include "amxfer.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_directtcp_listen() references
 * it directly.
 */

static GType xfer_source_directtcp_listen_get_type(void);
#define XFER_SOURCE_DIRECTTCP_LISTEN_TYPE (xfer_source_directtcp_listen_get_type())
#define XFER_SOURCE_DIRECTTCP_LISTEN(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_directtcp_listen_get_type(), XferSourceDirectTCPListen)
#define XFER_SOURCE_DIRECTTCP_LISTEN_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_directtcp_listen_get_type(), XferSourceDirectTCPListen const)
#define XFER_SOURCE_DIRECTTCP_LISTEN_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_directtcp_listen_get_type(), XferSourceDirectTCPListenClass)
#define IS_XFER_SOURCE_DIRECTTCP_LISTEN(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_directtcp_listen_get_type ())
#define XFER_SOURCE_DIRECTTCP_LISTEN_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_directtcp_listen_get_type(), XferSourceDirectTCPListenClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceDirectTCPListen {
    XferElement __parent__;
} XferSourceDirectTCPListen;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceDirectTCPListenClass;

/*
 * Implementation
 */

static gboolean
start_impl(
    XferElement *elt)
{
    /* this is really all we do -- copy the input addresses from our downstream
     * neighbor.  This seems trivial, but downstream may be a glue element, which
     * means we get the listening socket for free. */
    elt->input_listen_addrs = elt->downstream->input_listen_addrs;

    return FALSE;
}

static void
instance_init(
    XferElement *elt)
{
    elt->can_generate_eof = FALSE;
}

static void
class_init(
    XferSourceDirectTCPListenClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_DIRECTTCP_LISTEN, XFER_NROPS(1), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    klass->start = start_impl;
    klass->perl_class = "Amanda::Xfer::Source::DirectTCPListen";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_source_directtcp_listen_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceDirectTCPListenClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceDirectTCPListen),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceDirectTCPListen", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_directtcp_listen(void)
{
    XferSourceDirectTCPListen *xsr = (XferSourceDirectTCPListen *)g_object_new(XFER_SOURCE_DIRECTTCP_LISTEN_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xsr);

    return elt;
}
