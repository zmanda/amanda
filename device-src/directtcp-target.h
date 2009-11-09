/*
 * Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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

/* An interface for devices that support direct-tcp access to a the device */

#ifndef DIRECTTCP_TARGET_H
#define DIRECTTCP_TARGET_H

#include "device.h"
#include "directtcp.h"

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
 * A simple connection subclass containing a local TCP socket
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

/*
 * Interface definition
 */

GType	directtcp_target_get_type	(void);
#define TYPE_DIRECTTCP_TARGET	(directtcp_target_get_type())
#define DIRECTTCP_TARGET(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), directtcp_target_get_type(), DirectTCPTarget)
#define IS_DIRECTTCP_TARGET(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), directtcp_target_get_type ())
#define DIRECTTCP_TARGET_GET_INTERFACE(obj) G_TYPE_INSTANCE_GET_INTERFACE((obj), directtcp_target_get_type(), DirectTCPTargetInterface)

typedef gboolean (* ProlongProc)(gpointer data);

typedef struct DirectTCPTargetInterface_ DirectTCPTargetInterface;
struct DirectTCPTargetInterface_ {
    GTypeInterface __parent__;

    /* This is an extension of the Device class, so all methods take a Device
     * object as the "self" parameter, and error handling takes place using the
     * usual Device mechanisms.
     *
     * The DirectTCPConnection object allows a particular connection to "span"
     * multiple devices -- the caller gets the connection from one device,
     * reads or writes as desired, then creates a new device and passes the
     * connection to that device.  If the new device cannot use the old
     * connection, then it generates a suitable error message.
     */

    /* Begin listening for incoming connections, and return a list of listening
     * addresses.  This method should be called when the device is not yet
     * started, and must be followed by an accept() call.  The list of
     * addresses is the property of the device, and will remain unchanged at
     * least until accept() returns.
     *
     * @param self: device object
     * @param addrs (output): 0-terminated list of destination addresses
     * @returns: FALSE on error
     */
    gboolean (* listen)(Device *self, DirectTCPAddr **addrs);

    /* accept an incoming connection and return an opaque identifier for the
     * connection, but do not begin reading data.  If the operation is aborted
     * before the incoming connection is made, this method returns FALSE but
     * does not set an error status.
     *
     * @param self: device object
     * @param conn (output): new connection object
     * @param prolong: a ProlongProc called periodically; accept is aborted if
     *   this returns FALSE (can be NULL)
     * @param prolong_data: data passed to prolong
     * @returns: FALSE on error
     */
    gboolean (* accept)(Device *self, DirectTCPConnection **conn,
			ProlongProc prolong, gpointer prolong_data);

    /* Write to the device using data from the connection, writing at most
     * SIZE bytes of data.
     *
     * @param self: device object
     * @param conn: connection object from which to read data
     * @param size: number of bytes to transfer (must be a multiple of block size), or
     * zero for unlimited
     * @param actual_size (out): bytes actually written
     * @returns: FALSE on error
     */
    gboolean (* write_from_connection)(Device *self, DirectTCPConnection *conn,
		    gsize size, gsize *actual_size);

    /* send a fixed amount of data to the given connection from the device
     *
     * @param self: device object
     * @param conn: connection object from which to read data
     * @param size: number of bytes to transfer (must be a multiple of block
     * size), or zero for unlimited
     * @param actual_size (out): bytes actually read
     * @returns: FALSE on error
     */
    gboolean (* read_to_connection)(Device *self, DirectTCPConnection *conn,
		    gsize size, gsize *actual_size);

    /* verify that the device can use a given connection
     *
     * @param self: device object
     * @param conn: conn to verify
     * @returns: TRUE if this device can interact with the given connection
     */
    gboolean (* can_use_connection)(Device *self, DirectTCPConnection *conn);
};

/* Method Stubs */

gboolean directtcp_target_listen(Device *self, DirectTCPAddr **addrs);
gboolean directtcp_target_accept(Device *self, DirectTCPConnection **conn, ProlongProc prolong, gpointer prolong_data);
gboolean directtcp_target_write_from_connection(Device *self, DirectTCPConnection *conn,
		gsize size, gsize *actual_size);
gboolean directtcp_target_read_to_connection(Device *self, DirectTCPConnection *conn,
		gsize size, gsize *actual_size);
gboolean directtcp_target_can_use_connection(Device *self, DirectTCPConnection *conn);

#endif /* DIRECTTCP_TARGET_H */
