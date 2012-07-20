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
#include "event.h"
#include "sockaddr-util.h"
#include "ndmpconnobj.h"

/*
 * NDMPConnection class implementation
 */

/* level at which to snoop when VERBOSE is set; 8 = everything but hexdumps,
 * and 5 = packets without details */
#define SNOOP_LEVEL 7

static GObjectClass *parent_class = NULL;

/* and equipment to ensure we only talk to ndmlib in one thread at a time, even
 * using multiple connections.  The ndmlib code is not necessarily reentrant,
 * so this is better safe than sorry. */
static GStaticMutex ndmlib_mutex = G_STATIC_MUTEX_INIT;

/* macros like those in ndmlib.h, but designed for use in this class */
/* (copied from ndmp-src/ndmlib.h; see that file for copyright and license) */

#define NDMP_TRANS(SELF, TYPE) \
  { \
	struct ndmp_xa_buf *	xa = &(SELF)->conn->call_xa_buf; \
	TYPE##_request * request; \
	TYPE##_reply   * reply; \
	request = &xa->request.body.TYPE##_request_body; \
	reply = &xa->reply.body.TYPE##_reply_body; \
	NDMOS_MACRO_ZEROFILL (xa); \
	xa->request.protocol_version = NDMP4VER; \
	xa->request.header.message = (ndmp0_message) MT_##TYPE; \
	g_static_mutex_lock(&ndmlib_mutex); \
     {

#define NDMP_TRANS_NO_REQUEST(SELF, TYPE) \
  { \
	struct ndmp_xa_buf *	xa = &(SELF)->conn->call_xa_buf; \
	TYPE##_reply   * reply; \
	reply = &xa->reply.body.TYPE##_reply_body; \
	NDMOS_MACRO_ZEROFILL (xa); \
	xa->request.protocol_version = NDMP4VER; \
	xa->request.header.message = (ndmp0_message) MT_##TYPE; \
	g_static_mutex_lock(&ndmlib_mutex); \
     {

#define NDMP_CALL(SELF) \
    do { \
	(SELF)->last_rc = (*(SELF)->conn->call)((SELF)->conn, xa); \
	if ((SELF)->last_rc) { \
	    NDMP_FREE(); \
	    g_static_mutex_unlock(&ndmlib_mutex); \
	    return FALSE; \
	} \
    } while (0);

#define NDMP_FREE() ndmconn_free_nmb(NULL, &xa->reply)

#define NDMP_END \
	g_static_mutex_unlock(&ndmlib_mutex); \
    } }

/*
 * Methods
 */

static void
finalize_impl(GObject *goself)
{
    NDMPConnection *self = NDMP_CONNECTION(goself);

    /* chain up first */
    G_OBJECT_CLASS(parent_class)->finalize(goself);

    g_debug("closing conn#%d", self->connid);

    /* close this connection if necessary */
    if (self->conn) {
	ndmconn_destruct(self->conn);
	self->conn = NULL;
    }

    if (self->log_state) {
	g_free(self->log_state);
	self->log_state = NULL;
    }
}

/*
 * Error handling
 */

ndmp4_error
ndmp_connection_err_code(
    NDMPConnection *self)
{
    if (self->startup_err) {
	return NDMP4_IO_ERR;
    } else if (self->last_rc == NDMCONN_CALL_STATUS_REPLY_ERROR) {
	return self->conn->last_reply_error;
    } else {
	return NDMP4_NO_ERR;
    }
}

gchar *
ndmp_connection_err_msg(
    NDMPConnection *self)
{
    if (self->startup_err) {
	return g_strdup(self->startup_err);
    } else if (self->last_rc == NDMCONN_CALL_STATUS_REPLY_ERROR) {
	return g_strdup_printf("Error from NDMP server: %s",
		    ndmp9_error_to_str(self->conn->last_reply_error));
    } else if (self->last_rc) {
	return g_strdup_printf("ndmconn error %d: %s",
		    self->last_rc, ndmconn_get_err_msg(self->conn));
    } else {
	return g_strdup_printf("No error");
    }
}

static void
ndmp_connection_ndmlog_deliver(
    struct ndmlog *log,
    char *tag,
    int lev G_GNUC_UNUSED,
    char *msg)
{
    NDMPConnection *self = NDMP_CONNECTION(log->cookie);
    g_debug("conn#%d: %s: %s", self->connid, tag, msg);
}

void
ndmp_connection_set_verbose(
    NDMPConnection *self,
    gboolean verbose)
{
    struct ndmlog *device_ndmlog;
    g_assert(!self->startup_err);

    device_ndmlog = g_new0(struct ndmlog, 1);

    self->log_state = (gpointer)device_ndmlog;
    device_ndmlog->deliver = ndmp_connection_ndmlog_deliver;
    device_ndmlog->cookie = self;

    if (verbose) {
	ndmconn_set_snoop(self->conn,
	    device_ndmlog,
	    SNOOP_LEVEL);
    } else {
	ndmconn_clear_snoop(self->conn);
    }
}

/*
 * Operations
 */

gboolean
ndmp_connection_scsi_open(
	NDMPConnection *self,
	gchar *device)
{
    g_assert(!self->startup_err);

    NDMP_TRANS(self, ndmp4_scsi_open)
	request->device = device;
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_scsi_close(
	NDMPConnection *self)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_scsi_close)
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_scsi_execute_cdb(
	NDMPConnection *self,
	guint32 flags, /* NDMP4_SCSI_DATA_{IN,OUT}; OUT = to device */
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
	)
{
    g_assert(!self->startup_err);

    if (status)
	*status = 0;
    if (actual_dataout_len)
	*actual_dataout_len = 0;
    if (actual_datain_len)
	*actual_datain_len = 0;
    if (actual_ext_sense_len)
	*actual_ext_sense_len = 0;

    NDMP_TRANS(self, ndmp4_scsi_execute_cdb)
	request->flags = flags;
	request->timeout = timeout;
	request->datain_len = datain_max_len;
	request->cdb.cdb_len = cdb_len;
	request->cdb.cdb_val = cdb;
	request->dataout.dataout_len = dataout_len;
	request->dataout.dataout_val = dataout;

	NDMP_CALL(self);

	if (status)
	    *status = reply->status;
	if (actual_dataout_len)
	    *actual_dataout_len = reply->dataout_len;

	reply->datain.datain_len = MIN(datain_max_len, reply->datain.datain_len);
	if (actual_datain_len)
	    *actual_datain_len = reply->datain.datain_len;
	if (datain_max_len && datain)
	    g_memmove(datain, reply->datain.datain_val, reply->datain.datain_len);

	reply->ext_sense.ext_sense_len = MIN(ext_sense_max_len, reply->ext_sense.ext_sense_len);
	if (actual_ext_sense_len)
	    *actual_ext_sense_len = reply->ext_sense.ext_sense_len;
	if (ext_sense_max_len && ext_sense)
	    g_memmove(ext_sense, reply->ext_sense.ext_sense_val, reply->ext_sense.ext_sense_len);

	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_tape_open(
	NDMPConnection *self,
	gchar *device,
	ndmp9_tape_open_mode mode)
{
    g_assert(!self->startup_err);

    NDMP_TRANS(self, ndmp4_tape_open)
	request->device = device;
	request->mode = mode;
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_tape_close(
	NDMPConnection *self)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_tape_close)
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_tape_mtio(
	NDMPConnection *self,
	ndmp9_tape_mtio_op tape_op,
	gint count,
	guint *resid_count)
{
    g_assert(!self->startup_err);

    NDMP_TRANS(self, ndmp4_tape_mtio)
	request->tape_op = tape_op;
	request->count = count;
	NDMP_CALL(self);
	*resid_count = reply->resid_count;
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_tape_write(
	NDMPConnection *self,
	gpointer buf,
	guint64 len,
	guint64 *count)
{
    g_assert(!self->startup_err);

    *count = 0;

    NDMP_TRANS(self, ndmp4_tape_write)
	request->data_out.data_out_val = buf;
	request->data_out.data_out_len = len;
	NDMP_CALL(self);
	*count = reply->count;
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_tape_read(
	NDMPConnection *self,
	gpointer buf,
	guint64 count,
	guint64 *out_count)
{
    g_assert(!self->startup_err);

    *out_count = 0;

    NDMP_TRANS(self, ndmp4_tape_read)
	request->count = count;
	NDMP_CALL(self);
	*out_count = reply->data_in.data_in_len;
	g_memmove(buf, reply->data_in.data_in_val, *out_count);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_tape_get_state(
	NDMPConnection *self,
	guint64 *blocksize,
	guint64 *file_num,
	guint64 *blockno)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_tape_get_state)
	NDMP_CALL(self);

	if (reply->unsupported & NDMP4_TAPE_STATE_BLOCK_SIZE_UNS)
	    *blocksize = 0;
	else
	    *blocksize = reply->block_size;

	if (reply->unsupported & NDMP4_TAPE_STATE_FILE_NUM_UNS)
	    *file_num = G_MAXUINT64;
	else
	    *file_num = reply->file_num;

	if (reply->unsupported & NDMP4_TAPE_STATE_BLOCKNO_UNS)
	    *blockno = G_MAXUINT64;
	else
	    *blockno = reply->blockno;

	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_set_record_size(
	NDMPConnection *self,
	guint32 record_size)
{
    g_assert(!self->startup_err);

    NDMP_TRANS(self, ndmp4_mover_set_record_size)
	/* this field is "len" in ndmp4, but "record_size" in ndmp9 */
	request->len = record_size;
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_set_window(
	NDMPConnection *self,
	guint64 offset,
	guint64 length)
{
    g_assert(!self->startup_err);

    NDMP_TRANS(self, ndmp4_mover_set_window)
	request->offset = offset;
	request->length = length;
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_read(
	NDMPConnection *self,
	guint64 offset,
	guint64 length)
{
    g_assert(!self->startup_err);

    NDMP_TRANS(self, ndmp4_mover_read)
	request->offset = offset;
	request->length = length;
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_continue(
	NDMPConnection *self)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_mover_continue)
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_listen(
	NDMPConnection *self,
	ndmp9_mover_mode mode,
	ndmp9_addr_type addr_type,
	DirectTCPAddr **addrs)
{
    unsigned int naddrs, i;
    *addrs = NULL;

    g_assert(!self->startup_err);

    NDMP_TRANS(self, ndmp4_mover_listen)
	request->mode = mode;
	request->addr_type = addr_type;
	NDMP_CALL(self);

	if (request->addr_type != reply->connect_addr.addr_type) {
	    g_warning("MOVER_LISTEN addr_type mismatch; got %d", reply->connect_addr.addr_type);
	}

	if (reply->connect_addr.addr_type == NDMP4_ADDR_TCP) {
	    naddrs = reply->connect_addr.ndmp4_addr_u.tcp_addr.tcp_addr_len;
	    *addrs = g_new0(DirectTCPAddr, naddrs+1);
	    for (i = 0; i < naddrs; i++) {
		ndmp4_tcp_addr *na = &reply->connect_addr.ndmp4_addr_u.tcp_addr.tcp_addr_val[i];
		(*addrs)[i].sin.sin_family = AF_INET;
		(*addrs)[i].sin.sin_addr.s_addr = htonl(na->ip_addr);
		SU_SET_PORT(addrs[i], na->port);
	    }
	}
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

ndmp_connection_mover_connect(
	NDMPConnection *self,
	ndmp9_mover_mode mode,
	DirectTCPAddr *addrs)
{
    unsigned int naddrs, i;
    ndmp4_tcp_addr *na;

    g_assert(!self->startup_err);

    /* count addrs */
    g_assert(addrs);
    for (naddrs = 0; SU_GET_FAMILY(&addrs[naddrs]) != 0; naddrs++) ;

    /* convert addrs to an ndmp4_tcp_addr */
    na = g_new0(ndmp4_tcp_addr, naddrs);
    for (i = 0; i < naddrs; i++) {
	na[i].ip_addr = ntohl(addrs[i].sin.sin_addr.s_addr);
	na[i].port = SU_GET_PORT(&addrs[i]);
    }


    NDMP_TRANS(self, ndmp4_mover_connect)
	request->mode = mode;
	request->addr.addr_type = NDMP4_ADDR_TCP;
	request->addr.ndmp4_addr_u.tcp_addr.tcp_addr_len = naddrs;
	request->addr.ndmp4_addr_u.tcp_addr.tcp_addr_val = na;
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_abort(
	NDMPConnection *self)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_mover_abort)
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_stop(
	NDMPConnection *self)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_mover_stop)
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean
ndmp_connection_mover_close(
	NDMPConnection *self)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_mover_close)
	NDMP_CALL(self);
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

gboolean ndmp_connection_mover_get_state(
	NDMPConnection *self,
	ndmp9_mover_state *state,
	guint64 *bytes_moved,
	guint64 *window_offset,
	guint64 *window_length)
{
    g_assert(!self->startup_err);

    NDMP_TRANS_NO_REQUEST(self, ndmp4_mover_get_state)
	NDMP_CALL(self);
	if (state) *state = reply->state;
	if (bytes_moved) *bytes_moved = reply->bytes_moved;
	if (window_offset) *window_offset = reply->window_offset;
	if (window_length) *window_length = reply->window_length;
	NDMP_FREE();
    NDMP_END
    return TRUE;
}

static gboolean
ndmconn_handle_notify(
    NDMPConnection *self,
    struct ndmp_msg_buf *nmb)
{
    g_assert(!self->startup_err);

    if (nmb->header.message_type == NDMP0_MESSAGE_REQUEST) {
	switch (nmb->header.message) {
	    case NDMP4_NOTIFY_DATA_HALTED: {
		ndmp4_notify_data_halted_post *post =
		    &nmb->body.ndmp4_notify_data_halted_post_body;
		self->data_halt_reason = post->reason;
		break;
	    }

	    case NDMP4_NOTIFY_MOVER_HALTED: {
		ndmp4_notify_mover_halted_post *post =
		    &nmb->body.ndmp4_notify_mover_halted_post_body;
		self->mover_halt_reason = post->reason;
		break;
	    }

	    case NDMP4_NOTIFY_MOVER_PAUSED: {
		ndmp4_notify_mover_paused_post *post =
		    &nmb->body.ndmp4_notify_mover_paused_post_body;
		self->mover_pause_reason = post->reason;
		self->mover_pause_seek_position = post->seek_position;
		break;
	    }

            case NDMP4_LOG_FILE:
            case NDMP4_LOG_MESSAGE:
            case NDMP4_LOG_NORMAL:
            case NDMP4_LOG_DEBUG:
            case NDMP4_LOG_ERROR:
            case NDMP4_LOG_WARNING: {
                ndmp4_log_message_post *post =
                    &nmb->body.ndmp4_log_message_post_body;
                g_debug("%s", post->entry);
                break;
            }

	    default:
		self->last_rc = NDMCONN_CALL_STATUS_REPLY_ERROR;
		self->conn->last_reply_error = NDMP4_ILLEGAL_STATE_ERR;
		return FALSE;
	}
    } else {
	self->last_rc = NDMCONN_CALL_STATUS_REPLY_ERROR;
	self->conn->last_reply_error = NDMP4_ILLEGAL_STATE_ERR;
	return FALSE;
    }

    return TRUE;
}

/* handler for "unexpected" messages.  This handles notifications which happen
 * to arrive while the connection is reading the socket looking for a reply. */
static void
ndmconn_unexpected_impl (struct ndmconn *conn, struct ndmp_msg_buf *nmb)
{
    NDMPConnection *self = NDMP_CONNECTION(conn->context);

    if (!ndmconn_handle_notify(self, nmb)) {
	g_warning("ignoring unrecognized, unexpected packet");
    }

    ndmconn_free_nmb(NULL, nmb);
}

gboolean
ndmp_connection_wait_for_notify(
	NDMPConnection *self,
	ndmp9_data_halt_reason *data_halt_reason,
	ndmp9_mover_halt_reason *mover_halt_reason,
	ndmp9_mover_pause_reason *mover_pause_reason,
	guint64 *mover_pause_seek_position)
{
    struct ndmp_msg_buf nmb;

    g_assert(!self->startup_err);

    /* initialize output parameters */
    if (data_halt_reason)
	*data_halt_reason = NDMP4_DATA_HALT_NA;
    if (mover_halt_reason)
	*mover_halt_reason = NDMP4_MOVER_HALT_NA;
    if (mover_pause_reason)
	*mover_pause_reason = NDMP4_MOVER_PAUSE_NA;
    if (mover_pause_seek_position)
	*mover_pause_seek_position = 0;

    while (1) {
	gboolean found = FALSE;
	int fd;
	SELECT_ARG_TYPE readset;
	int nfound;

	/* if any desired notifications have been received, then we're
	 * done */
	if (data_halt_reason && self->data_halt_reason) {
	    found = TRUE;
	    *data_halt_reason = self->data_halt_reason;
	    self->data_halt_reason = NDMP4_DATA_HALT_NA;
	}

	if (mover_halt_reason && self->mover_halt_reason) {
	    found = TRUE;
	    *mover_halt_reason = self->mover_halt_reason;
	    self->mover_halt_reason = NDMP4_MOVER_HALT_NA;
	}

	if (mover_pause_reason && self->mover_pause_reason) {
	    found = TRUE;
	    *mover_pause_reason = self->mover_pause_reason;
	    if (mover_pause_seek_position)
		*mover_pause_seek_position = self->mover_pause_seek_position;
	    self->mover_pause_reason = NDMP4_MOVER_PAUSE_NA;
	    self->mover_pause_seek_position = 0;
	}

	if (found)
	    return TRUE;

	/* otherwise, wait for an incoming packet and handle it, then try
	 * again.  There's some select trickery here to avoid hogging the
	 * ndmlib_mutex - basically, we want to block as long as possible
	 * outside of the ndmlib_mutex critical section.  This will also be
	 * useful to allow the wait to be aborted. */
	fd = self->conn->chan.fd;
	FD_ZERO(&readset);
	FD_SET(fd, &readset);
	nfound = select(fd+1, &readset, NULL, NULL, NULL);

	/* fall on through, blind to any errors - presumably the same error
	 * condition will be caught by ndmconn_recv_nmb. */

	g_static_mutex_lock(&ndmlib_mutex);
	NDMOS_MACRO_ZEROFILL(&nmb);
	nmb.protocol_version = NDMP4VER;
	self->last_rc = ndmconn_recv_nmb(self->conn, &nmb);
	g_static_mutex_unlock(&ndmlib_mutex);

	if (self->last_rc) {
	    /* (nothing to free) */
	    return FALSE;
	}

	ndmconn_handle_notify(self, &nmb);
    }
}

typedef struct notify_data {
    NDMPConnection *self;
    ndmp9_data_halt_reason *data_halt_reason;
    ndmp9_mover_halt_reason *mover_halt_reason;
    ndmp9_mover_pause_reason *mover_pause_reason;
    guint64 *mover_pause_seek_position;
    GMutex *abort_mutex;
    GCond *abort_cond;
    int status;
    event_handle_t *read_event;
} notify_data;

static void handle_notify(void *cookie);

int
ndmp_connection_wait_for_notify_with_cond(
	NDMPConnection *self,
	ndmp9_data_halt_reason *data_halt_reason,
	ndmp9_mover_halt_reason *mover_halt_reason,
	ndmp9_mover_pause_reason *mover_pause_reason,
	guint64 *mover_pause_seek_position,
	GMutex *abort_mutex,
	GCond *abort_cond)
{
    struct ndmp_msg_buf nmb;
    notify_data ndata;
    gboolean found = FALSE;

    ndata.self = self;
    ndata.data_halt_reason= data_halt_reason;
    ndata.mover_halt_reason= mover_halt_reason;
    ndata.mover_pause_reason= mover_pause_reason;
    ndata.mover_pause_seek_position = mover_pause_seek_position;
    ndata.abort_mutex = abort_mutex;
    ndata.abort_cond = abort_cond;
    ndata.status = 2;

    g_assert(!self->startup_err);

    /* initialize output parameters */
    if (data_halt_reason)
	*data_halt_reason = NDMP4_DATA_HALT_NA;
    if (mover_halt_reason)
	*mover_halt_reason = NDMP4_MOVER_HALT_NA;
    if (mover_pause_reason)
	*mover_pause_reason = NDMP4_MOVER_PAUSE_NA;
    if (mover_pause_seek_position)
	*mover_pause_seek_position = 0;

    /* if any desired notifications have been received, then we're
     * done */
    if (data_halt_reason && self->data_halt_reason) {
	found = TRUE;
	*data_halt_reason = self->data_halt_reason;
	self->data_halt_reason = NDMP4_DATA_HALT_NA;
    }

    if (mover_halt_reason && self->mover_halt_reason) {
	found = TRUE;
	*mover_halt_reason = self->mover_halt_reason;
	self->mover_halt_reason = NDMP4_MOVER_HALT_NA;
    }

    if (mover_pause_reason && self->mover_pause_reason) {
	found = TRUE;
	*mover_pause_reason = self->mover_pause_reason;
	if (mover_pause_seek_position)
	    *mover_pause_seek_position = self->mover_pause_seek_position;
	self->mover_pause_reason = NDMP4_MOVER_PAUSE_NA;
	self->mover_pause_seek_position = 0;
    }

    if (found)
	return TRUE;

	/* otherwise, wait for an incoming packet and handle it, then try
	 * again.  There's some select trickery here to avoid hogging the
	 * ndmlib_mutex - basically, we want to block as long as possible
	 * outside of the ndmlib_mutex critical section.  This will also be
	 * useful to allow the wait to be aborted. */

    ndata.read_event = event_register(self->conn->chan.fd,
				      EV_READFD, handle_notify, &ndata);

    g_cond_wait(abort_cond, abort_mutex);

    if (ndata.read_event) {
	event_release(ndata.read_event);
    }
    if (ndata.status == 2) {
	ndmp_connection_mover_abort(self);
	ndmp_connection_mover_stop(self);
    }
    return ndata.status;

}

static void
handle_notify(void *cookie)
{
    notify_data *ndata = cookie;
    struct ndmp_msg_buf nmb;
    gboolean found = FALSE;

    g_mutex_lock(ndata->abort_mutex);

    event_release(ndata->read_event);
    ndata->read_event = NULL;

    g_static_mutex_lock(&ndmlib_mutex);
    NDMOS_MACRO_ZEROFILL(&nmb);
    nmb.protocol_version = NDMP4VER;
    ndata->self->last_rc = ndmconn_recv_nmb(ndata->self->conn, &nmb);
    g_static_mutex_unlock(&ndmlib_mutex);

    if (ndata->self->last_rc) {
	/* (nothing to free) */
	ndata->status = 1;
	goto notify_done;
    }

    ndmconn_handle_notify(ndata->self, &nmb);


    /* if any desired notifications have been received, then we're
     * done */
    if (ndata->data_halt_reason && ndata->self->data_halt_reason) {
	found = TRUE;
	*ndata->data_halt_reason = ndata->self->data_halt_reason;
	ndata->self->data_halt_reason = NDMP4_DATA_HALT_NA;
    }

    if (ndata->mover_halt_reason && ndata->self->mover_halt_reason) {
	found = TRUE;
	*ndata->mover_halt_reason = ndata->self->mover_halt_reason;
	ndata->self->mover_halt_reason = NDMP4_MOVER_HALT_NA;
    }

    if (ndata->mover_pause_reason && ndata->self->mover_pause_reason) {
	found = TRUE;
	*ndata->mover_pause_reason = ndata->self->mover_pause_reason;
	if (ndata->mover_pause_seek_position)
	    *ndata->mover_pause_seek_position = ndata->self->mover_pause_seek_position;
	ndata->self->mover_pause_reason = NDMP4_MOVER_PAUSE_NA;
	ndata->self->mover_pause_seek_position = 0;
    }

    if (!found) {
	ndata->read_event = event_register(ndata->self->conn->chan.fd,
					   EV_READFD, handle_notify, ndata);
	g_mutex_unlock(ndata->abort_mutex);
	return;
    }

    ndata->status = 0;
notify_done:
    g_cond_broadcast(ndata->abort_cond);
    g_mutex_unlock(ndata->abort_mutex);
}
/*
 * Class Mechanics
 */

static void
ndmp_connection_class_init(
        NDMPConnectionClass * c)
{
    GObjectClass *goc = (GObjectClass *)c;

    goc->finalize =  finalize_impl;

    parent_class = g_type_class_peek_parent(c);
}

GType
ndmp_connection_get_type(void)
{
    static GType type = 0;
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (NDMPConnectionClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) ndmp_connection_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (NDMPConnection),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (G_TYPE_OBJECT, "NDMPConnection", &info,
                                       (GTypeFlags)0);
    }
    return type;
}

/* Method stubs */

/*
 * Constructor
 */

NDMPConnection *
ndmp_connection_new(
    gchar *hostname,
    gint port,
    gchar *username,
    gchar *password,
    gchar *auth)
{
    NDMPConnection *self = NULL;
    gchar *key = NULL;
    gchar *errmsg = NULL;
    struct ndmconn *conn = NULL;
    int rc;
    static int next_connid = 1;
    static GStaticMutex next_connid_mutex = G_STATIC_MUTEX_INIT;

    conn = ndmconn_initialize(NULL, "amanda-server");
    if (!conn) {
	errmsg = "could not initialize ndmconn";
	goto out;
    }

    /* set up a handler for unexpected messages, which should generally
     * be notifications */
    conn->unexpected = ndmconn_unexpected_impl;

    if (ndmconn_connect_host_port(conn, hostname, port, 0) != 0) {
	errmsg = ndmconn_get_err_msg(conn);
	ndmconn_destruct(conn);
	goto out;
    }

    if (0 == g_ascii_strcasecmp(auth, "void")) {
	rc = 0; /* don't authenticate */
    } else if (0 == g_ascii_strcasecmp(auth, "none")) {
	rc = ndmconn_auth_none(conn);
    } else if (0 == g_ascii_strcasecmp(auth, "md5")) {
	rc = ndmconn_auth_md5(conn, username, password);
    } else if (0 == g_ascii_strcasecmp(auth, "text")) {
	rc = ndmconn_auth_text(conn, username, password);
    } else {
	errmsg = "invalid auth type";
	goto out;
    }

    if (rc != 0) {
	errmsg = ndmconn_get_err_msg(conn);
	ndmconn_destruct(conn);
	goto out;
    }

    if (conn->protocol_version != NDMP4VER) {
	errmsg = g_strdup_printf("Only NDMPv4 is supported; got NDMPv%d",
	    conn->protocol_version);
	ndmconn_destruct(conn);
	goto out;
    }

    self = NDMP_CONNECTION(g_object_new(TYPE_NDMP_CONNECTION, NULL));
    self->conn = conn;
    g_static_mutex_lock(&next_connid_mutex);
    self->connid = next_connid++;
    g_static_mutex_unlock(&next_connid_mutex);
    conn->context = (void *)self;
    g_debug("opening new NDMPConnection #%d: to %s:%d", self->connid, hostname, port);

out:
    /* make a "fake" error connection if we have an error message.  Note that
     * this object is not added to the instances hash */
    if (errmsg) {
	self = NDMP_CONNECTION(g_object_new(TYPE_NDMP_CONNECTION, NULL));
	self->startup_err = errmsg;
	errmsg = NULL;
    }

    return self;
}
