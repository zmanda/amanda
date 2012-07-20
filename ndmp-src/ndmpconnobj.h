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

/* An interface for devices that support direct-tcp access to a the device */

#ifndef NDMCONNOBJ_H
#define NDMCONNOBJ_H

#include "amanda.h"
#include "ndmlib.h"
#include "directtcp.h"
#include <glib-object.h>

GType	ndmp_connection_get_type	(void);
#define TYPE_NDMP_CONNECTION	(ndmp_connection_get_type())
#define NDMP_CONNECTION(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), ndmp_connection_get_type(), NDMPConnection)
#define IS_NDMP_CONNECTION(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), ndmp_connection_get_type ())
#define NDMP_CONNECTION_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), ndmp_connection_get_type(), NDMPConnectionClass)

/*
 * Parent class for connections
 */

typedef struct NDMPConnection_ {
    GObject __parent__;

    /* the ndmconn - interface to ndmlib */
    struct ndmconn *conn;

    /* integer to identify this connection */
    int connid;

    /* received notifications; note that this does not include all possible
     * notifications, and that only one "queued" version of each notification
     * is possible.  Each reason is 0 if no such notification has been
     * received.  */
    ndmp9_data_halt_reason data_halt_reason;
    ndmp9_mover_halt_reason mover_halt_reason;
    ndmp9_mover_pause_reason mover_pause_reason;
    guint64 mover_pause_seek_position;

    /* log state, if using verbose logging (private) */
    gpointer log_state;

    /* error info */
    int last_rc;
    gchar *startup_err;
} NDMPConnection;

typedef struct NDMPConnectionClass_ {
    GObjectClass __parent__;

    /* NOTE: this class is not subclassed, so its "methods" are not
     * implemented via the usual GObject function-pointer mechanism,
     * but are simple C functions. */
} NDMPConnectionClass;

/* Error handling */

/* Get the last NDMP error on this connection; returns NDMP4_NO_ERR (0)
 * if no error occurred, or if there was a communications error.  This
 * will also detect errors from the constructor.
 *
 * @param self: object
 * @returns: error code
 */
ndmp4_error ndmp_connection_err_code(
    NDMPConnection *self);

/* Get the error message describing the most recent error on this object.
 * This will always return a non-NULL string.  The result must be freed
 * by the caller.
 *
 * @param self: object
 * @returns: heap-allocated error message
 */
gchar *ndmp_connection_err_msg(
    NDMPConnection *self);

/* Set the verbose flag for this connection
 *
 * @param self: object
 * @param verbose: TRUE for verbose logging
 */
void ndmp_connection_set_verbose(
    NDMPConnection *self,
    gboolean verbose);

/*
 * basic NDMP protocol operations
 *
 * All of these functions return TRUE on success.
 */

gboolean ndmp_connection_scsi_open(
	NDMPConnection *self,
	gchar *device);

gboolean ndmp_connection_scsi_close(
	NDMPConnection *self);

gboolean ndmp_connection_scsi_execute_cdb(
	NDMPConnection *self,
	guint32 flags, /* bitmask: NDMP9_SCSI_DATA_DIR_{IN,OUT}; OUT = to device */
	guint32 timeout, /* in ms */
	gpointer cdb,
	gsize cdb_len,
	gpointer dataout,
	gsize dataout_len,
	gsize *actual_dataout_len, /* output */
	gpointer datain, /* output */
	gsize datain_max_len, /* output buffer size */
	gsize *actual_datain_len, /* output */
	guint8 *status, /* output */
	gpointer ext_sense, /* output */
	gsize ext_sense_max_len, /* output buffer size */
	gsize *actual_ext_sense_len /* output */
	);

gboolean ndmp_connection_tape_open(
	NDMPConnection *self,
	gchar *device,
	ndmp9_tape_open_mode mode);

gboolean ndmp_connection_tape_close(
	NDMPConnection *self);

gboolean ndmp_connection_tape_mtio(
	NDMPConnection *self,
	ndmp9_tape_mtio_op tape_op,
	gint count,
	guint *resid_count);

gboolean ndmp_connection_tape_write(
	NDMPConnection *self,
	gpointer buf,
	guint64 len,
	guint64 *count); /* output */

gboolean ndmp_connection_tape_read(
	NDMPConnection *self,
	gpointer buf, /* output */
	guint64 count, /* buffer size/requested read size */
	guint64 *out_count); /* bytes read */

gboolean ndmp_connection_tape_get_state(
	NDMPConnection *self,
	guint64 *blocksize, /* 0 if not supported */
	guint64 *file_num, /* all 1's if not supported */
	guint64 *blockno); /* all 1's if not supported */
	/* (other state variables should be added as needed) */

gboolean ndmp_connection_mover_set_record_size(
	NDMPConnection *self,
	guint32 record_size);

gboolean ndmp_connection_mover_set_window(
	NDMPConnection *self,
	guint64 offset,
	guint64 length);

gboolean ndmp_connection_mover_read(
	NDMPConnection *self,
	guint64 offset,
	guint64 length);

gboolean ndmp_connection_mover_continue(
	NDMPConnection *self);

gboolean ndmp_connection_mover_listen(
	NDMPConnection *self,
	ndmp9_mover_mode mode,
	ndmp9_addr_type addr_type,
	DirectTCPAddr **addrs);

gboolean ndmp_connection_mover_connect(
	NDMPConnection *self,
	ndmp9_mover_mode mode,
	DirectTCPAddr *addrs);

gboolean ndmp_connection_mover_abort(
	NDMPConnection *self);

gboolean ndmp_connection_mover_stop(
	NDMPConnection *self);

gboolean ndmp_connection_mover_close(
	NDMPConnection *self);

gboolean ndmp_connection_mover_get_state(
	NDMPConnection *self,
	ndmp9_mover_state *state,
	guint64 *bytes_moved,
	guint64 *window_offset,
	guint64 *window_length);
	/* (other state variables should be added as needed) */

/* Synchronous notification interface.  This handles all types of notification,
 * returning the result in the appropriate output parameter. */
gboolean ndmp_connection_wait_for_notify(
	NDMPConnection *self,
	/* NDMP_NOTIFY_DATA_HALTED */
	ndmp9_data_halt_reason *data_halt_reason,
	/* NDMP_NOTIFY_MOVER_HALTED */
	ndmp9_mover_halt_reason *mover_halt_reason,
	/* NDMP_NOTIFY_MOVER_PAUSED */
	ndmp9_mover_pause_reason *mover_pause_reason,
	guint64 *mover_pause_seek_position);

/* Synchronous notification interface.  This handles all types of notification,
 * returning the result in the appropriate output parameter. */
gboolean ndmp_connection_wait_for_notify_with_cond(
	NDMPConnection *self,
	/* NDMP_NOTIFY_DATA_HALTED */
	ndmp9_data_halt_reason *data_halt_reason,
	/* NDMP_NOTIFY_MOVER_HALTED */
	ndmp9_mover_halt_reason *mover_halt_reason,
	/* NDMP_NOTIFY_MOVER_PAUSED */
	ndmp9_mover_pause_reason *mover_pause_reason,
	guint64 *mover_pause_seek_position,
	GMutex *abort_mutex,
	GCond *abort_cond);

/*
 * Constructor
 */

/* Get a new NDMP connection. If an error occurs, this returns an empty object
 * with its err_msg and err_code set appropriately.
 *
 * @param hostname: hostname to connect to
 * @param port: port to connect on
 * @param username: username to login with
 * @param password: password to login with
 * @param auth: authentication method to use ("MD5", "TEXT", "NONE", or "VOID")
 * @returns: NDMPConnection object
 */
NDMPConnection *
ndmp_connection_new(
    gchar *hostname,
    gint port,
    gchar *username,
    gchar *password,
    gchar *auth);

#endif
