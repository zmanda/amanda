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

#ifndef DIRECTTCP_CONNECTION_H
#define DIRECTTCP_CONNECTION_H

#include <glib.h>
#include <glib-object.h>

GType	directtcp_connection_get_type	(void);
#define TYPE_DIRECTTCP_CONNECTION	(directtcp_connection_get_type())
#define DIRECTTCP_CONNECTION(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), directtcp_connection_get_type(), DirectTCPConnection)
#define IS_DIRECTTCP_CONNECTION(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), directtcp_connection_get_type ())
#define DIRECTTCP_CONNECTION_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), directtcp_connection_get_type(), DirectTCPConnectionClass)

/*
 * Parent class for connections
 */

typedef struct DirectTCPConnection_ {
    GObject __parent__;

    gboolean closed;
} DirectTCPConnection;

typedef struct DirectTCPConnectionClass_ {
    GObjectClass __parent__;

    /* The DirectTCPConnection object allows a particular connection to "span"
     * multiple devices -- the caller gets the connection from one device,
     * reads or writes as desired, then creates a new device and passes the
     * connection to that device.  If the new device cannot use the old
     * connection, then it generates a suitable error message.
     */

    /* call this to close the connection (even if the Device that created
     * it is long gone).  Note that this will be called automatically by
     * finalize, but it is a programming error to allow this to happen as
     * any error will be fatal.
     *
     * @param self: object
     * @returns: error message on error, NULL for no error (caller should
     *  free the error message)
     */
    char *(* close)(struct DirectTCPConnection_ *self);
} DirectTCPConnectionClass;

/* Method Stubs */

char *directtcp_connection_close(
    DirectTCPConnection *self);

/*
 * A simple connection subclass containing a local TCP socket, useful for testing
 */

#define TYPE_DIRECTTCP_CONNECTION_SOCKET	(directtcp_connection_socket_get_type())
#define DIRECTTCP_CONNECTION_SOCKET(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), directtcp_connection_socket_get_type(), DirectTCPConnectionSocket)
#define DIRECTTCP_CONNECTION_SOCKET_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), directtcp_connection_socket_get_type(), DirectTCPConnectionSocket const)
#define DIRECTTCP_CONNECTION_SOCKET_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), directtcp_connection_socket_get_type(), DirectTCPConnectionSocketClass)
#define IS_DIRECTTCP_CONNECTION_SOCKET(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), directtcp_connection_socket_get_type ())
#define DIRECTTCP_CONNECTION_SOCKET_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), directtcp_connection_socket_get_type(), DirectTCPConnectionSocketClass)
GType directtcp_connection_socket_get_type(void);

typedef struct DirectTCPConnectionSocket_ {
    DirectTCPConnection __parent__;
    int socket;
} DirectTCPConnectionSocket;

typedef struct DirectTCPConnectionSocketClass_ {
    DirectTCPConnectionClass __parent__;
} DirectTCPConnectionSocketClass;

/* Method Stubs */

DirectTCPConnectionSocket *directtcp_connection_socket_new(int socket);

#endif /* DIRECTTCP_CONNECTION_H */
