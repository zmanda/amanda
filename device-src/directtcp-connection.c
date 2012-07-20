/*
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "directtcp-connection.h"
#include <glib-object.h>

/*
 * DirectTCPConnection class implementation
 */

static GObjectClass *parent_class = NULL;

static void
directtcp_connection_finalize(GObject *goself)
{
    DirectTCPConnection *self = DIRECTTCP_CONNECTION(goself);

    /* close this socket if necessary, failing fatally if that doesn't work */
    if (!self->closed) {
        char *errmsg;

        g_warning("connection freed without being closed first; any error will be fatal");
        errmsg = directtcp_connection_close(self);
        if (errmsg)
            error("while closing directtcp connection: %s", errmsg);
    }

    G_OBJECT_CLASS(parent_class)->finalize(goself);
}

static void
directtcp_connection_class_init(
        DirectTCPConnectionClass * c)
{
    GObjectClass *goc = (GObjectClass *)c;

    goc->finalize =  directtcp_connection_finalize;

    parent_class = g_type_class_peek_parent(c);
}

GType
directtcp_connection_get_type(void)
{
    static GType type = 0;
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (DirectTCPConnectionClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) directtcp_connection_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (DirectTCPConnection),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (G_TYPE_OBJECT, "DirectTCPConnection", &info,
                                       (GTypeFlags)G_TYPE_FLAG_ABSTRACT);
    }
    return type;
}

char *
directtcp_connection_close(
    DirectTCPConnection *self)
{
    DirectTCPConnectionClass *klass = DIRECTTCP_CONNECTION_GET_CLASS(self);
    char *rv;

    /* Note that this also tracks the 'closed' value, which is used by finalize
     * to ensure that the connection has been closed */

    g_assert(!self->closed);

    g_assert(klass->close);
    rv = klass->close(self);
    self->closed = TRUE;
    return rv;
}

/*
 * DirectTCPConnSocket class implementation
 */

DirectTCPConnectionSocket *
directtcp_connection_socket_new(
    int socket)
{
    DirectTCPConnectionSocket *conn = DIRECTTCP_CONNECTION_SOCKET(g_object_new(TYPE_DIRECTTCP_CONNECTION_SOCKET, NULL));
    conn->socket = socket;
    return conn;
}

static char *
directtcp_connection_socket_close(DirectTCPConnection *dself)
{
    DirectTCPConnectionSocket *self = DIRECTTCP_CONNECTION_SOCKET(dself);
    if (self->socket >= 0 && close(self->socket)) {
        return g_strdup_printf("while closing socket: %s", strerror(errno));
    }
    self->socket = -1;

    return NULL;
}

static void
directtcp_connection_socket_init(DirectTCPConnectionSocket *self)
{
    self->socket = -1;
}

static void
directtcp_connection_socket_class_init(DirectTCPConnectionSocketClass * c)
{
    DirectTCPConnectionClass *connc = (DirectTCPConnectionClass *)c;

    connc->close = directtcp_connection_socket_close;
}

GType
directtcp_connection_socket_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (DirectTCPConnectionSocketClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) directtcp_connection_socket_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (DirectTCPConnectionSocket),
            0 /* n_preallocs */,
            (GInstanceInitFunc) directtcp_connection_socket_init,
            NULL
        };

        type = g_type_register_static(TYPE_DIRECTTCP_CONNECTION,
                                "DirectTCPConnectionSocket", &info, (GTypeFlags)0);
    }

    return type;
}
