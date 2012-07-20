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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "amxfer.h"
#include "sockaddr-util.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_dest_directtcp_listen() references
 * it directly.
 */

GType xfer_dest_directtcp_listen_get_type(void);
#define XFER_DEST_DIRECTTCP_LISTEN_TYPE (xfer_dest_directtcp_listen_get_type())
#define XFER_DEST_DIRECTTCP_LISTEN(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_directtcp_listen_get_type(), XferDestDirectTCPListen)
#define XFER_DEST_DIRECTTCP_LISTEN_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_dest_directtcp_listen_get_type(), XferDestDirectTCPListen const)
#define XFER_DEST_DIRECTTCP_LISTEN_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_dest_directtcp_listen_get_type(), XferDestDirectTCPListenClass)
#define IS_XFER_DEST_DIRECTTCP_LISTEN(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_dest_directtcp_listen_get_type ())
#define XFER_DEST_DIRECTTCP_LISTEN_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_dest_directtcp_listen_get_type(), XferDestDirectTCPListenClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferDestDirectTCPListen {
    XferElement __parent__;
} XferDestDirectTCPListen;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferDestDirectTCPListenClass;

/*
 * Implementation
 */

static gboolean
start_impl(
    XferElement *elt)
{
    elt->output_listen_addrs = elt->upstream->output_listen_addrs;

    return FALSE;
}

static void
class_init(
    XferDestDirectTCPListenClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_DIRECTTCP_CONNECT, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    klass->start = start_impl;

    klass->perl_class = "Amanda::Xfer::Dest::DirectTCPListen";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_dest_directtcp_listen_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestDirectTCPListenClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestDirectTCPListen),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestDirectTCPListen", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_dest_directtcp_listen(void)
{
    XferDestDirectTCPListen *self = (XferDestDirectTCPListen *)
                g_object_new(XFER_DEST_DIRECTTCP_LISTEN_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    return elt;
}
