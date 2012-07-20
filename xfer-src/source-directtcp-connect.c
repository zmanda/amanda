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
#include "amxfer.h"
#include "sockaddr-util.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_directtcp_connect() references
 * it directly.
 */

static GType xfer_source_directtcp_connect_get_type(void);
#define XFER_SOURCE_DIRECTTCP_CONNECT_TYPE (xfer_source_directtcp_connect_get_type())
#define XFER_SOURCE_DIRECTTCP_CONNECT(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_directtcp_connect_get_type(), XferSourceDirectTCPConnect)
#define XFER_SOURCE_DIRECTTCP_CONNECT_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_directtcp_connect_get_type(), XferSourceDirectTCPConnect const)
#define XFER_SOURCE_DIRECTTCP_CONNECT_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_directtcp_connect_get_type(), XferSourceDirectTCPConnectClass)
#define IS_XFER_SOURCE_DIRECTTCP_CONNECT(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_directtcp_connect_get_type ())
#define XFER_SOURCE_DIRECTTCP_CONNECT_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_directtcp_connect_get_type(), XferSourceDirectTCPConnectClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceDirectTCPConnect {
    XferElement __parent__;

    /* Addresses to connect to */
    DirectTCPAddr *addrs;
} XferSourceDirectTCPConnect;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceDirectTCPConnectClass;

/*
 * Implementation
 */

static gboolean
setup_impl(
    XferElement *elt)
{
    XferSourceDirectTCPConnect *self = (XferSourceDirectTCPConnect *)elt;

    g_assert(self->addrs && SU_GET_FAMILY(self->addrs) != 0);
    elt->output_listen_addrs = self->addrs;

    return TRUE;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferSourceDirectTCPConnect *self = XFER_SOURCE_DIRECTTCP_CONNECT(obj_self);

    if (self->addrs)
	g_free(self->addrs);
    self->addrs = NULL;

    /* chain up */
    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
instance_init(
    XferElement *elt)
{
    elt->can_generate_eof = FALSE;
}

static void
class_init(
    XferSourceDirectTCPConnectClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_DIRECTTCP_CONNECT, XFER_NROPS(1), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    klass->setup = setup_impl;
    goc->finalize = finalize_impl;

    klass->perl_class = "Amanda::Xfer::Source::DirectTCPConnect";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_source_directtcp_connect_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceDirectTCPConnectClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceDirectTCPConnect),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceDirectTCPConnect", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_directtcp_connect(DirectTCPAddr *addrs)
{
    XferSourceDirectTCPConnect *self = (XferSourceDirectTCPConnect *)
	    g_object_new(XFER_SOURCE_DIRECTTCP_CONNECT_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);
    int i;

    g_assert(addrs != NULL);

    for (i = 0; SU_GET_FAMILY(&addrs[i]) != 0; i++);
    self->addrs = g_memdup(addrs, (i+1) * sizeof(*addrs));

    return elt;
}
