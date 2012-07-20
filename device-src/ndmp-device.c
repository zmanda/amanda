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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "util.h"
#include "device.h"
#include "directtcp.h"
#include "stream.h"
#include "ndmlib.h"
#include "ndmpconnobj.h"
#include "sockaddr-util.h"

/*
 * Type checking and casting macros
 */
#define TYPE_NDMP_DEVICE	(ndmp_device_get_type())
#define NDMP_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), ndmp_device_get_type(), NdmpDevice)
#define NDMP_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), ndmp_device_get_type(), NdmpDevice const)
#define NDMP_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), ndmp_device_get_type(), NdmpDeviceClass)
#define IS_NDMP_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), ndmp_device_get_type ())
#define NDMP_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), ndmp_device_get_type(), NdmpDeviceClass)
static GType	ndmp_device_get_type	(void);

/*
 * Main object structure
 */

typedef struct NdmpDevice_ NdmpDevice;
struct NdmpDevice_ {
    Device __parent__;

    NDMPConnection *ndmp;

    /* true if tape service is open on the NDMP server */
    gboolean tape_open;

    /* addresses the object is listening on, and how the connection
     * was opened */
    DirectTCPAddr *listen_addrs;
    gboolean for_writing;

    /* support for IndirectTCP */
    int indirecttcp_sock; /* -1 if not in use */
    int indirect;

    /* Current DirectTCPConnectionNDMP */
    struct DirectTCPConnectionNDMP_ *directtcp_conn;

    /* constructor parameters and properties */
    gchar	 *ndmp_hostname;
    gint	 ndmp_port;
    gchar        *ndmp_device_name;
    gchar	 *ndmp_username;
    gchar	 *ndmp_password;
    gchar	 *ndmp_auth;
    gboolean	 verbose;
    gsize	 read_block_size;
};

/*
 * Class definition
 */

typedef struct NdmpDeviceClass_ NdmpDeviceClass;
struct NdmpDeviceClass_ {
    DeviceClass __parent__;
};

/*
 * A directtcp connection subclass representing a running mover on the other end of
 * the given NDMP connection
 */

#define TYPE_DIRECTTCP_CONNECTION_NDMP	(directtcp_connection_ndmp_get_type())
#define DIRECTTCP_CONNECTION_NDMP(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), directtcp_connection_ndmp_get_type(), DirectTCPConnectionNDMP)
#define DIRECTTCP_CONNECTION_NDMP_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), directtcp_connection_ndmp_get_type(), DirectTCPConnectionNDMP const)
#define DIRECTTCP_CONNECTION_NDMP_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), directtcp_connection_ndmp_get_type(), DirectTCPConnectionNDMPClass)
#define IS_DIRECTTCP_CONNECTION_NDMP(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), directtcp_connection_ndmp_get_type ())
#define DIRECTTCP_CONNECTION_NDMP_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), directtcp_connection_ndmp_get_type(), DirectTCPConnectionNDMPClass)
GType directtcp_connection_ndmp_get_type(void);

typedef struct DirectTCPConnectionNDMP_ {
    DirectTCPConnection __parent__;

    /* NDMP connection controlling the mover */
    NDMPConnection *ndmp;

    /* mode for this operation */
    ndmp9_mover_mode mode;

    /* last reported mover position in the datastream */
    guint64 offset;
} DirectTCPConnectionNDMP;

typedef struct DirectTCPConnectionNDMPClass_ {
    DirectTCPConnectionClass __parent__;
} DirectTCPConnectionNDMPClass;

static DirectTCPConnectionNDMP *directtcp_connection_ndmp_new(
	NDMPConnection *ndmp,
	ndmp9_mover_mode mode);

/*
 * Constants and static data
 */

#define NDMP_DEVICE_NAME "ndmp"

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

/* robust_write results */
typedef enum {
    ROBUST_WRITE_OK,
    ROBUST_WRITE_OK_LEOM,
    ROBUST_WRITE_ERROR, /* device error already set */
    ROBUST_WRITE_NO_SPACE
} robust_write_result;

/*
 * device-specific properties
 */

/* Authentication information for NDMP agent. Both of these are strings. */
static DevicePropertyBase device_property_ndmp_username;
static DevicePropertyBase device_property_ndmp_password;
static DevicePropertyBase device_property_ndmp_auth;
static DevicePropertyBase device_property_indirect;
#define PROPERTY_NDMP_USERNAME (device_property_ndmp_username.ID)
#define PROPERTY_NDMP_PASSWORD (device_property_ndmp_password.ID)
#define PROPERTY_NDMP_AUTH (device_property_ndmp_auth.ID)
#define PROPERTY_INDIRECT (device_property_indirect.ID)


/*
 * prototypes
 */

void ndmp_device_register(void);
static void set_error_from_ndmp(NdmpDevice *self);

#define ndmp_device_read_size(self) \
    (((NdmpDevice *)(self))->read_block_size? \
	((NdmpDevice *)(self))->read_block_size : ((Device *)(self))->block_size)

/*
 * Utility functions
 */

static gboolean
open_connection(
	NdmpDevice *self)
{
    if (!self->ndmp) {
	self->ndmp = ndmp_connection_new(
	    self->ndmp_hostname,
	    self->ndmp_port,
	    self->ndmp_username,
	    self->ndmp_password,
	    self->ndmp_auth);

	if (ndmp_connection_err_code(self->ndmp)) {
	    char *errmsg = ndmp_connection_err_msg(self->ndmp);
	    device_set_error(DEVICE(self),
		g_strdup_printf("could not connect to ndmp-server '%s:%d': %s",
		    self->ndmp_hostname, self->ndmp_port, errmsg),
		DEVICE_STATUS_DEVICE_ERROR);
	    g_object_unref(self->ndmp);
	    self->ndmp = NULL;
	    return FALSE;
	}

	if (self->verbose)
	    ndmp_connection_set_verbose(self->ndmp, TRUE);

	self->tape_open = FALSE;
    }

    return TRUE;
}

static void
close_connection(
	NdmpDevice *self)
{
    /* note that this does not send NDMP_TAPE_CLOSE, as it's used in error
     * situations too */
    if (self->ndmp) {
	g_object_unref(self->ndmp);
	self->ndmp = NULL;
	self->tape_open = FALSE;
    }
}

static gboolean
open_tape_agent(
    NdmpDevice *self)
{
    guint64 file_num, blockno, blocksize;

    /* if already open, stop now */
    if (self->tape_open) {
	return TRUE;
    }

    if (!open_connection(self)) {
	/* error message set by open_connection */
	return FALSE;
    }

    g_debug("opening tape device '%s' on NDMP server '%s:%d'",
	self->ndmp_device_name, self->ndmp_hostname, self->ndmp_port);

    /* send NDMP_TAPE_OPEN, using RAW mode so that it will open even with no tape */
    if (!ndmp_connection_tape_open(self->ndmp,
		self->ndmp_device_name, NDMP9_TAPE_RAW_MODE)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    /* check that the block sizes match */
    if (!ndmp_connection_tape_get_state(self->ndmp,
	&blocksize, &file_num, &blockno)) {
	set_error_from_ndmp(self);
	return FALSE;
    }
    if (blocksize != 0 && blocksize != DEVICE(self)->block_size) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("NDMP device has fixed block size %ju, but Amanda "
		    "device is configured with blocksize %ju", (uintmax_t)blocksize,
		    (uintmax_t)(DEVICE(self)->block_size)),
	    DEVICE_STATUS_DEVICE_ERROR);
    }

    self->tape_open = TRUE;

    return TRUE;
}

static gboolean
close_tape_agent(
	NdmpDevice *self)
{
    if (self->tape_open) {
	g_debug("closing tape device '%s' on NDMP server '%s:%d'",
	    self->ndmp_device_name, self->ndmp_hostname, self->ndmp_port);
	self->tape_open = FALSE; /* count it as closed even if there is an error */
	if (!ndmp_connection_tape_close(self->ndmp)) {
	    set_error_from_ndmp(self);
	    return FALSE;
	}
    }

    return TRUE;
}

static gboolean
single_ndmp_mtio(
    NdmpDevice *self,
    ndmp9_tape_mtio_op tape_op)
{
    guint resid;

    if (!ndmp_connection_tape_mtio(self->ndmp, tape_op, 1, &resid)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (resid != 0) {
	device_set_error(DEVICE(self),
		g_strdup_printf("NDMP MTIO operation %d did not complete", tape_op),
		DEVICE_STATUS_DEVICE_ERROR);
    }

    return TRUE;
}

/* get the tape state straight from the device; we try to track these things
 * accurately in the device, but sometimes it's good to check. */
static gboolean
ndmp_get_state(
    NdmpDevice *self)
{
    Device *dself = DEVICE(self);
    guint64 file_num, blockno, blocksize;

    if (!ndmp_connection_tape_get_state(self->ndmp,
	&blocksize, &file_num, &blockno)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    g_assert(file_num < INT_MAX);
    dself->file = (int)file_num;
    dself->block = blockno;

    return TRUE;
}

static robust_write_result
robust_write(
    NdmpDevice *self,
    char *buf,
    guint64 count)
{
    guint64 actual;
    robust_write_result subresult;

    if (!ndmp_connection_tape_write(self->ndmp, buf, count, &actual)) {
	switch (ndmp_connection_err_code(self->ndmp)) {
	    case NDMP9_IO_ERR:
		/* We encountered PEOM; this only happens when the caller ignores
		 * LEOM */
		return ROBUST_WRITE_NO_SPACE;

	    case NDMP9_EOM_ERR:
		/* We encountered LEOM; retry the write (which should succeed) */
		subresult = robust_write(self, buf, count);
		if (subresult != ROBUST_WRITE_OK)
		    return subresult;
		g_debug("ndmp device hit logical EOM");
		return ROBUST_WRITE_OK_LEOM;

	    default:
		set_error_from_ndmp(self);
		return ROBUST_WRITE_ERROR;
	}
    }

    g_assert(count == actual);
    return ROBUST_WRITE_OK;
}

static void
set_error_from_ndmp(
    NdmpDevice *self)
{
    /* translate some error codes to the corresponding Device API status */
    switch (ndmp_connection_err_code(self->ndmp)) {
	case NDMP9_NO_TAPE_LOADED_ERR:
	    device_set_error(DEVICE(self),
		    g_strdup(_("no tape loaded")),
			    DEVICE_STATUS_VOLUME_MISSING);
	    break;

	case NDMP9_DEVICE_BUSY_ERR:
	    device_set_error(DEVICE(self),
		    g_strdup(_("device busy")),
			    DEVICE_STATUS_DEVICE_BUSY);
	    break;

	case NDMP9_IO_ERR:
	    device_set_error(DEVICE(self),
		    g_strdup(_("IO error")),
			    DEVICE_STATUS_VOLUME_UNLABELED |
			    DEVICE_STATUS_VOLUME_ERROR |
			    DEVICE_STATUS_DEVICE_ERROR);
	    break;

	default:
	    device_set_error(DEVICE(self),
		    ndmp_connection_err_msg(self->ndmp),
		    DEVICE_STATUS_DEVICE_ERROR);
	    break;
	}
    close_connection(self);
}

/*
 * Virtual function overrides
 */

static void
ndmp_device_open_device(
    Device *dself,
    char   *device_name,
    char   *device_type,
    char   *device_node)
{
    NdmpDevice          *self = NDMP_DEVICE(dself);
    char *colon, *at;

    /* first, extract the various parts of the device_node:
     * HOST[:PORT]@DEVICE */
    colon = strchr(device_node, ':');
    at = strchr(device_node, '@');
    if (colon > at)
	colon = NULL; /* :PORT only counts if it's before the device name */
    if (!at) {
	device_set_error(dself,
			 g_strdup_printf("invalid ndmp device name '%s'", device_name),
			 DEVICE_STATUS_DEVICE_ERROR);
	return;
    }

    if (colon) {
	char *p = NULL;
	long port = strtol(colon+1, &p, 10);

	if (port < 0 || port >= 65536 || p != at || (!port && EINVAL == errno)) {
	    device_set_error(dself,
			    g_strdup_printf("invalid ndmp port in device name '%s'",
					    device_name),
			    DEVICE_STATUS_DEVICE_ERROR);
	    return;
	}
	self->ndmp_port = (gint)port;
	self->ndmp_hostname = g_strndup(device_node, colon-device_node);
    } else {
	self->ndmp_port = 0; /* (use ndmjob's default, 10000) */
	self->ndmp_hostname = g_strndup(device_node, at-device_node);
    }
    self->ndmp_device_name = g_strdup(at+1);

    if (parent_class->open_device) {
        parent_class->open_device(dself, device_name, device_type, device_node);
    }
}

static void ndmp_device_finalize(GObject * obj_self)
{
    NdmpDevice       *self = NDMP_DEVICE (obj_self);

    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    (void)close_tape_agent(self); /* ignore any error */

    if (self->directtcp_conn)
	g_object_unref(self->directtcp_conn);

    if (self->listen_addrs)
	g_free(self->listen_addrs);

    close_connection(self);

    if (self->ndmp_hostname)
	g_free(self->ndmp_hostname);
    if (self->ndmp_device_name)
	g_free(self->ndmp_device_name);
    if (self->ndmp_username)
	g_free(self->ndmp_username);
    if (self->ndmp_password)
	g_free(self->ndmp_password);
    if (self->ndmp_auth)
	g_free(self->ndmp_auth);
    if (self->indirecttcp_sock != -1)
	close(self->indirecttcp_sock);
}

static DeviceStatusFlags
ndmp_device_read_label(
    Device *dself)
{
    NdmpDevice       *self = NDMP_DEVICE(dself);
    dumpfile_t       *header = NULL;
    gpointer buf = NULL;
    guint64 buf_size = 0;
    gsize read_block_size = 0;

    amfree(dself->volume_label);
    amfree(dself->volume_time);
    dumpfile_free(dself->volume_header);
    dself->volume_header = NULL;

    if (device_in_error(self)) return dself->status;

    if (!open_tape_agent(self)) {
	/* error status was set by open_tape_agent */
	return dself->status;
    }

    if (!single_ndmp_mtio(self, NDMP9_MTIO_REW)) {
	/* error message, if any, is set by single_ndmp_mtio */
	return dself->status;
    }

    /* read the tape header from the NDMP server */
    dself->status = 0;
    read_block_size = ndmp_device_read_size(self);
    buf = g_malloc(read_block_size);
    if (!ndmp_connection_tape_read(self->ndmp,
	buf,
	read_block_size,
	&buf_size)) {

	/* handle known errors */
	switch (ndmp_connection_err_code(self->ndmp)) {
	    case NDMP9_NO_TAPE_LOADED_ERR:
		device_set_error(dself,
			g_strdup(_("no tape loaded")),
				DEVICE_STATUS_VOLUME_MISSING);
		goto read_err;

	    case NDMP9_IO_ERR:
		device_set_error(dself,
			g_strdup(_("IO error reading tape label")),
				DEVICE_STATUS_VOLUME_UNLABELED |
				DEVICE_STATUS_VOLUME_ERROR |
				DEVICE_STATUS_DEVICE_ERROR);
		goto read_err;

	    case NDMP9_EOM_ERR:
	    case NDMP9_EOF_ERR:
		device_set_error(dself,
			g_strdup(_("no tape label found")),
				DEVICE_STATUS_VOLUME_UNLABELED);
		header = dself->volume_header = g_new(dumpfile_t, 1);
		fh_init(header);
		goto read_err;

	    default:
		set_error_from_ndmp(self);
		goto read_err;
	    }
	}

	header = dself->volume_header = g_new(dumpfile_t, 1);
	fh_init(header);
	parse_file_header(buf, header, buf_size);

read_err:
    g_free(buf);

    if (dself->status != 0) {
	/* error already set above */
	return dself->status;
    }

    /* handle a "weird" label */
    if (header->type != F_TAPESTART) {
	device_set_error(dself,
		stralloc(_("No tapestart header -- unlabeled device?")),
			 DEVICE_STATUS_VOLUME_UNLABELED);
	return dself->status;
    }
    dself->volume_label = g_strdup(header->name);
    dself->volume_time = g_strdup(header->datestamp);
    /* dself->volume_header is already set */

    /* note: connection is left open, as well as the tape device */

    device_set_error(dself, NULL, DEVICE_STATUS_SUCCESS);

    return dself->status;
}


static gboolean
ndmp_device_start(
    Device           *dself,
    DeviceAccessMode  mode,
    char             *label,
    char             *timestamp)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    dumpfile_t *header;
    char       *header_buf;

    self = NDMP_DEVICE(dself);

    if (device_in_error(self)) return FALSE;

    if (!open_tape_agent(self)) {
	/* error status was set by open_tape_agent */
	return FALSE;
    }

    if (mode != ACCESS_WRITE && dself->volume_label == NULL) {
	if (ndmp_device_read_label(dself) != DEVICE_STATUS_SUCCESS)
	    /* the error was set by ndmp_device_read_label */
	    return FALSE;
    }

    dself->access_mode = mode;
    g_mutex_lock(dself->device_mutex);
    dself->in_file = FALSE;
    g_mutex_unlock(dself->device_mutex);

    if (!single_ndmp_mtio(self, NDMP9_MTIO_REW)) {
	/* single_ndmp_mtio already set our error message */
	return FALSE;
    }

    /* Position the tape */
    switch (mode) {
    case ACCESS_APPEND:
	device_set_error(dself,
	    g_strdup("operation not supported"),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
	break;

    case ACCESS_READ:
	dself->file = 0;
	break;

    case ACCESS_WRITE:
	header = make_tapestart_header(dself, label, timestamp);
	g_assert(header != NULL);

	header_buf = device_build_amanda_header(dself, header, NULL);
	if (header_buf == NULL) {
	    device_set_error(dself,
		stralloc(_("Tapestart header won't fit in a single block!")),
		DEVICE_STATUS_DEVICE_ERROR);
	    dumpfile_free(header);
	    return FALSE;
	}

	switch (robust_write(self, header_buf, dself->block_size)) {
	    case ROBUST_WRITE_OK_LEOM:
		dself->is_eom = TRUE;
		/* fall through */
	    case ROBUST_WRITE_OK:
		break;

	    case ROBUST_WRITE_NO_SPACE:
		/* this would be an odd error to see writing the tape label, but
		 * oh well */
		device_set_error(dself,
		    stralloc(_("No space left on device")),
		    DEVICE_STATUS_VOLUME_ERROR);
		dself->is_eom = TRUE;
		/* fall through */

	    case ROBUST_WRITE_ERROR:
		/* error was set by robust_write or above */
		dumpfile_free(header);
		amfree(header_buf);
		return FALSE;

	}
	amfree(header_buf);

	if (!single_ndmp_mtio(self, NDMP9_MTIO_EOF)) {
	    /* error was set by single_ndmp_mtio */
	    dumpfile_free(header);
	    return FALSE;
	}

	dself->volume_label = newstralloc(dself->volume_label, label);
	dself->volume_time = newstralloc(dself->volume_time, timestamp);
	dumpfile_free(dself->volume_header);
	dself->volume_header = header;

	/* unset the VOLUME_UNLABELED flag, if it was set */
	device_set_error(dself, NULL, DEVICE_STATUS_SUCCESS);
	dself->file = 0;
	break;

    default:
	g_assert_not_reached();
    }

    return TRUE;
}

static gboolean
ndmp_device_finish(
    Device *dself)
{
    gboolean rval;

    NdmpDevice *self = NDMP_DEVICE(dself);
    rval = !device_in_error(dself);

    /* we're not in a file anymore */
    dself->access_mode = ACCESS_NULL;

    if (!close_tape_agent(self)) {
	/* error is set by close_tape_agent */
	rval = FALSE;
    }

    if (self->ndmp)
	close_connection(self);

    return rval;
}

static gboolean
ndmp_device_eject(
    Device *dself)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    if (device_in_error(dself)) return FALSE;

    if (!single_ndmp_mtio(self, NDMP9_MTIO_OFF)) {
	/* error was set by single_ndmp_mtio */
	return FALSE;
    }

    return TRUE;
}


/* functions for writing */

static gboolean
ndmp_device_start_file(
    Device     *dself,
    dumpfile_t *header)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    char *header_buf;

    if (device_in_error(self)) return FALSE;

    dself->is_eof = FALSE;
    dself->is_eom = FALSE;
    g_mutex_lock(dself->device_mutex);
    dself->bytes_written = 0;
    g_mutex_unlock(dself->device_mutex);

    /* set the blocksize in the header properly */
    header->blocksize = dself->block_size;

    header_buf = device_build_amanda_header(dself, header, NULL);
    if (header_buf == NULL) {
	device_set_error(dself,
	    stralloc(_("Amanda file header won't fit in a single block!")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    switch (robust_write(self, header_buf, dself->block_size)) {
	case ROBUST_WRITE_OK_LEOM:
	    dself->is_eom = TRUE;
	    /* fall through */

	case ROBUST_WRITE_OK:
	    break;

	case ROBUST_WRITE_NO_SPACE:
	    /* this would be an odd error to see writing the tape label, but
	     * oh well */
	    device_set_error(dself,
		stralloc(_("No space left on device")),
		DEVICE_STATUS_VOLUME_ERROR);
	    dself->is_eom = TRUE;
	    /* fall through */

	case ROBUST_WRITE_ERROR:
	    /* error was set by robust_write or above */
	    amfree(header_buf);
	    return FALSE;
    }
    amfree(header_buf);

    /* arrange the file numbers correctly */
    g_mutex_lock(dself->device_mutex);
    dself->in_file = TRUE;
    g_mutex_unlock(dself->device_mutex);
    if (!ndmp_get_state(self)) {
	/* error already set by ndmp_get_state */
	return FALSE;
    }

    /* double-check that the tape agent gave us a non-bogus file number */
    g_assert(dself->file > 0);

    return TRUE;
}

static gboolean
ndmp_device_write_block(
    Device   *dself,
    guint     size,
    gpointer  data)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    gpointer replacement_buffer = NULL;

    if (device_in_error(self)) return FALSE;

    /* zero out to the end of a short block -- tape devices only write
     * whole blocks. */
    if (size < dself->block_size) {
        replacement_buffer = malloc(dself->block_size);
        memcpy(replacement_buffer, data, size);
        bzero(replacement_buffer+size, dself->block_size-size);

        data = replacement_buffer;
        size = dself->block_size;
    }

    switch (robust_write(self, data, size)) {
	case ROBUST_WRITE_OK_LEOM:
	    dself->is_eom = TRUE;
	    /* fall through */

	case ROBUST_WRITE_OK:
	    break;

	case ROBUST_WRITE_NO_SPACE:
	    /* this would be an odd error to see writing the tape label, but
	     * oh well */
	    device_set_error(dself,
		stralloc(_("No space left on device")),
		DEVICE_STATUS_VOLUME_ERROR);
	    dself->is_eom = TRUE;
	    /* fall through */

	case ROBUST_WRITE_ERROR:
	    /* error was set by robust_write or above */
	    if (replacement_buffer) g_free(replacement_buffer);
	    return FALSE;
    }

    dself->block++;
    g_mutex_lock(dself->device_mutex);
    dself->bytes_written += size;
    g_mutex_unlock(dself->device_mutex);

    if (replacement_buffer) g_free(replacement_buffer);
    return TRUE;
}

static gboolean
ndmp_device_finish_file(
    Device *dself)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    if (device_in_error(dself)) return FALSE;

    /* we're not in a file anymore */
    g_mutex_lock(dself->device_mutex);
    dself->in_file = FALSE;
    g_mutex_unlock(dself->device_mutex);

    if (!single_ndmp_mtio(self, NDMP9_MTIO_EOF)) {
	/* error was set by single_ndmp_mtio */
        dself->is_eom = TRUE;
	return FALSE;
    }

    return TRUE;
}

/* functions for reading */

static dumpfile_t*
ndmp_device_seek_file(
    Device *dself,
    guint   file)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    gint delta;
    guint resid;
    gpointer buf;
    guint64 buf_size;
    dumpfile_t *header;
    gsize read_block_size = 0;

    if (device_in_error(dself)) return FALSE;

    /* file 0 is the tape header, and isn't seekable as a distinct
     * Device-API-level file */
    if (file == 0) {
	device_set_error(dself,
	    g_strdup("cannot seek to file 0"),
	    DEVICE_STATUS_DEVICE_ERROR);
	return NULL;
    }

    /* first, make sure the file and block numbers are correct */
    if (!ndmp_get_state(self)) {
	/* error already set by ndmp_get_state */
	return FALSE;
    }

    /* now calculate the file delta */
    delta = file - dself->file;

    if (delta <= 0) {
	/* Note that this algorithm will rewind to the beginning of
	 * the current part, too */

	/* BSF *past* the filemark we want to seek to */
	if (!ndmp_connection_tape_mtio(self->ndmp, NDMP9_MTIO_BSF, -delta + 1, &resid)) {
	    set_error_from_ndmp(self);
	    return NULL;
	}
	if (resid != 0)
	    goto incomplete_bsf;

	/* now we are on the BOT side of the filemark, but we want to be
	 * on the EOT side of it.  An FSF will get us there.. */
	if (!ndmp_connection_tape_mtio(self->ndmp, NDMP9_MTIO_FSF, 1, &resid)) {
	    set_error_from_ndmp(self);
	    return NULL;
	}

	if (resid != 0) {
incomplete_bsf:
	    device_set_error(dself,
		g_strdup_printf("BSF operation failed to seek by %d files", resid),
		DEVICE_STATUS_DEVICE_ERROR);
	    return NULL;
	}
    } else /* (delta > 0) */ {
	if (!ndmp_connection_tape_mtio(self->ndmp, NDMP9_MTIO_FSF, delta, &resid)) {
	    set_error_from_ndmp(self);
	    return FALSE;
	}

	/* if we didn't seek all the way there, then we're past the tapeend */
	if (resid > 0) {
	    device_set_error(dself,
		vstrallocf(_("Could not seek forward to file %d"), file),
		DEVICE_STATUS_VOLUME_ERROR);
	    return NULL;
	}
    }

    /* fix up status */
    g_mutex_lock(dself->device_mutex);
    dself->in_file = TRUE;
    g_mutex_unlock(dself->device_mutex);
    dself->file = file;
    dself->block = 0;
    g_mutex_lock(dself->device_mutex);
    dself->bytes_read = 0;
    g_mutex_unlock(dself->device_mutex);

    /* now read the header */
    read_block_size = ndmp_device_read_size(self);
    buf = g_malloc(read_block_size);
    if (!ndmp_connection_tape_read(self->ndmp,
		buf, read_block_size, &buf_size)) {
	switch (ndmp_connection_err_code(self->ndmp)) {
	    case NDMP9_EOF_ERR:
	    case NDMP9_EOM_ERR:
		return make_tapeend_header();

	    default:
		set_error_from_ndmp(self);
		g_free(buf);
		return NULL;
	}
    }

    header = g_new(dumpfile_t, 1);
    fh_init(header);
    parse_file_header(buf, header, buf_size);
    g_free(buf);

    return header;
}

static gboolean
ndmp_device_seek_block(
    Device  *dself,
    guint64  block)
{
    if (device_in_error(dself)) return FALSE;

    dself->block = block;

    device_set_error(dself, g_strdup("operation not supported"), DEVICE_STATUS_DEVICE_ERROR);
    return FALSE;
}

static int
ndmp_device_read_block (Device * dself, gpointer data, int *size_req) {
    NdmpDevice *self = NDMP_DEVICE(dself);
    guint64 requested, actual;
    gsize read_block_size = ndmp_device_read_size(self);

    /* We checked the NDMP device's blocksize when the device was opened, which should
     * catch any misalignent of server block size and Amanda block size */

    g_assert(read_block_size < INT_MAX); /* check data type mismatch */
    if (!data || *size_req < (int)(read_block_size)) {
	*size_req = (int)(read_block_size);
	return 0;
    }

    requested = *size_req;
    if (!ndmp_connection_tape_read(self->ndmp,
	data,
	requested,
	&actual)) {

	/* handle known errors */
	switch (ndmp_connection_err_code(self->ndmp)) {
	    case NDMP9_EOM_ERR:
	    case NDMP9_EOF_ERR:
		dself->is_eof = TRUE;
		return -1;

	    default:
		set_error_from_ndmp(self);
		return -1;
	}
    }

    *size_req = (int)actual; /* cast is OK - requested size was < INT_MAX too */
    g_mutex_lock(dself->device_mutex);
    dself->bytes_read += actual;
    g_mutex_unlock(dself->device_mutex);

    return *size_req;
}

static gboolean
indirecttcp_listen(
    NdmpDevice *self,
    DirectTCPAddr **addrs)
{
    in_port_t port;

    self->indirecttcp_sock = stream_server(AF_INET, &port, 0, STREAM_BUFSIZE, 0);
    if (self->indirecttcp_sock < 0) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("Could not bind indirecttcp socket: %s", strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    /* An IndirectTCP address is 255.255.255.255:$port */
    self->listen_addrs = *addrs = g_new0(DirectTCPAddr, 2);
    addrs[0]->sin.sin_family = AF_INET;
    addrs[0]->sin.sin_addr.s_addr = htonl(0xffffffff);
    SU_SET_PORT(addrs[0], port);

    return TRUE;
}

static gboolean
listen_impl(
    Device *dself,
    gboolean for_writing,
    DirectTCPAddr **addrs)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    if (device_in_error(self)) return FALSE;

    /* check status */
    g_assert(!self->listen_addrs);

    if (!open_tape_agent(self)) {
	/* error message was set by open_tape_agent */
	return FALSE;
    }

    self->for_writing = for_writing;

    /* first, set the window to an empty span so that the mover doesn't start
     * reading or writing data immediately.  NDMJOB tends to reset the record
     * size periodically (in direct contradiction to the spec), so we reset it
     * here as well. */
    if (!ndmp_connection_mover_set_record_size(self->ndmp,
		DEVICE(self)->block_size)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (for_writing) {
	/* if we're forcing indirecttcp, just do it */
	if (self->indirect) {
	    return indirecttcp_listen(self, addrs);
	}
	if (!ndmp_connection_mover_set_window(self->ndmp, 0, 0)) {
	    /* NDMP4_ILLEGAL_ARGS_ERR means the NDMP server doesn't like a zero-byte
	     * mover window, so we'll ignore it */
	    if (ndmp_connection_err_code(self->ndmp) != NDMP4_ILLEGAL_ARGS_ERR) {
		set_error_from_ndmp(self);
		return FALSE;
	    }

	    g_debug("NDMP Device: cannot set zero-length mover window; "
		    "falling back to IndirectTCP");
	    /* In this case, we need to set up IndirectTCP */
	    return indirecttcp_listen(self, addrs);
	}
    } else {
	/* For reading, set the window to the second mover record, so that the
	 * mover will pause immediately when it wants to read the first mover
	 * record. */
	if (!ndmp_connection_mover_set_window(self->ndmp,
					      dself->block_size,
					      dself->block_size)) {
	    set_error_from_ndmp(self);
	    return FALSE;
	}
    }

    /* then tell it to start listening */
    if (!ndmp_connection_mover_listen(self->ndmp,
		for_writing? NDMP9_MOVER_MODE_READ : NDMP9_MOVER_MODE_WRITE,
		NDMP9_ADDR_TCP,
		addrs)) {
	set_error_from_ndmp(self);
	return FALSE;
    }
    self->listen_addrs = *addrs;

    return TRUE;
}

static gboolean
accept_impl(
    Device *dself,
    DirectTCPConnection **dtcpconn,
    ProlongProc prolong,
    gpointer prolong_data)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    ndmp9_mover_state state;
    guint64 bytes_moved;
    ndmp9_mover_mode mode;
    ndmp9_mover_pause_reason reason;
    guint64 seek_position;

    if (device_in_error(self)) return FALSE;

    g_assert(self->listen_addrs);

    *dtcpconn = NULL;

    /* TODO: support aborting this operation - maybe just always poll? */
    prolong = prolong;
    prolong_data = prolong_data;

    if (!self->for_writing) {
	/* when reading, we don't get any kind of notification that the
	 * connection has been established, but we can't call NDMP_MOVER_READ
	 * until the mover is active.  So we have to poll, waiting for ACTIVE.
	 * This is ugly. */
	gulong backoff = G_USEC_PER_SEC/20; /* 5 msec */
	while (1) {
	    if (!ndmp_connection_mover_get_state(self->ndmp,
		    &state, &bytes_moved, NULL, NULL)) {
		set_error_from_ndmp(self);
		return FALSE;
	    }

	    if (state != NDMP9_MOVER_STATE_LISTEN)
		break;

	    /* back off a little bit to give the other side time to breathe,
	     * but not more than one second */
	    g_usleep(backoff);
	    backoff *= 2;
	    if (backoff > G_USEC_PER_SEC)
		backoff = G_USEC_PER_SEC;
	}

	/* double-check state */
	if (state != NDMP9_MOVER_STATE_ACTIVE) {
	    device_set_error(DEVICE(self),
		g_strdup("mover did not enter the ACTIVE state as expected"),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}

	/* now, we need to get this into the PAUSED state, since right now we
	 * aren't allowed to perform any tape movement commands.  So we issue a
	 * MOVER_READ request for the whole darn image stream after setting the
	 * usual empty window. Note that this means the whole dump will be read
	 * in one MOVER_READ operation, even if it does not begin at the
	 * beginning of a part. */
	if (!ndmp_connection_mover_read(self->ndmp, 0, G_MAXUINT64)) {
	    set_error_from_ndmp(self);
	    return FALSE;
	}

	/* now we should expect a notice that the mover has paused */
    } else {
	/* when writing, the mover will pause as soon as the first byte comes
         * in, so there's no need to do anything to trigger the pause.
         *
         * Well, sometimes it won't - specifically, when it does not allow a
         * zero-byte mover window, which means we've set up IndirectTCP.  But in
         * that case, there's nothing interesting to do here.*/
    }

    if (self->indirecttcp_sock == -1) {
	/* NDMJOB sends NDMP9_MOVER_PAUSE_SEEK to indicate that it wants to write
	 * outside the window, while the standard specifies .._EOW, instead.  When
	 * reading to a connection, we get the appropriate .._SEEK.  It's easy
	 * enough to handle both. */

	if (!ndmp_connection_wait_for_notify(self->ndmp,
		NULL,
		NULL,
		&reason, &seek_position)) {
	    set_error_from_ndmp(self);
	    return FALSE;
	}

	if (reason != NDMP9_MOVER_PAUSE_SEEK && reason != NDMP9_MOVER_PAUSE_EOW) {
	    device_set_error(DEVICE(self),
		g_strdup_printf("got NOTIFY_MOVER_PAUSED, but not because of EOW or SEEK"),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}
    }

    /* at this point, if we're doing directtcp, the mover is paused and ready
     * to go, and the listen addrs are no longer required; if we're doing
     * indirecttcp, then the other end may not even know of our listen_addrs
     * yet, so we can't free them.
     */

    if (self->indirecttcp_sock == -1) {
	g_free(self->listen_addrs);
	self->listen_addrs = NULL;
    }

    if (self->for_writing)
	mode = NDMP9_MOVER_MODE_READ;
    else
	mode = NDMP9_MOVER_MODE_WRITE;

    /* set up the new directtcp connection */
    if (self->directtcp_conn)
	g_object_unref(self->directtcp_conn);
    self->directtcp_conn =
	directtcp_connection_ndmp_new(self->ndmp, mode);
    *dtcpconn = DIRECTTCP_CONNECTION(self->directtcp_conn);

    /* reference it for the caller */
    g_object_ref(*dtcpconn);

    return TRUE;
}

static int
accept_with_cond_impl(
    Device *dself,
    DirectTCPConnection **dtcpconn,
    GMutex *abort_mutex,
    GCond *abort_cond)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    ndmp9_mover_state state;
    guint64 bytes_moved;
    ndmp9_mover_mode mode;
    ndmp9_mover_pause_reason reason;
    guint64 seek_position;
    int result;

    if (device_in_error(self)) return 1;

    g_assert(self->listen_addrs);

    *dtcpconn = NULL;

    if (!self->for_writing) {
	/* when reading, we don't get any kind of notification that the
	 * connection has been established, but we can't call NDMP_MOVER_READ
	 * until the mover is active.  So we have to poll, waiting for ACTIVE.
	 * This is ugly. */
	gulong backoff = G_USEC_PER_SEC/20; /* 5 msec */
	while (1) {
	    if (!ndmp_connection_mover_get_state(self->ndmp,
		    &state, &bytes_moved, NULL, NULL)) {
		set_error_from_ndmp(self);
		return 1;
	    }

	    if (state != NDMP9_MOVER_STATE_LISTEN)
		break;

	    /* back off a little bit to give the other side time to breathe,
	     * but not more than one second */
	    g_usleep(backoff);
	    backoff *= 2;
	    if (backoff > G_USEC_PER_SEC)
		backoff = G_USEC_PER_SEC;
	}

	/* double-check state */
	if (state != NDMP9_MOVER_STATE_ACTIVE) {
	    device_set_error(DEVICE(self),
		g_strdup("mover did not enter the ACTIVE state as expected"),
		DEVICE_STATUS_DEVICE_ERROR);
	    return 1;
	}

	/* now, we need to get this into the PAUSED state, since right now we
	 * aren't allowed to perform any tape movement commands.  So we issue a
	 * MOVER_READ request for the whole darn image stream after setting the
	 * usual empty window. Note that this means the whole dump will be read
	 * in one MOVER_READ operation, even if it does not begin at the
	 * beginning of a part. */
	if (!ndmp_connection_mover_read(self->ndmp, 0, G_MAXUINT64)) {
	    set_error_from_ndmp(self);
	    return 1;
	}

	/* now we should expect a notice that the mover has paused */
    } else {
	/* when writing, the mover will pause as soon as the first byte comes
	 * in, so there's no need to do anything to trigger the pause. */
    }

    if (self->indirecttcp_sock == -1) {
	/* NDMJOB sends NDMP9_MOVER_PAUSE_SEEK to indicate that it wants to
	 * write outside the window, while the standard specifies .._EOW,
	 * instead.  When reading to a connection, we get the appropriate
	 * .._SEEK.  It's easy enough to handle both. */
	result = ndmp_connection_wait_for_notify_with_cond(self->ndmp,
			NULL,
			NULL,
			&reason, &seek_position,
			abort_mutex, abort_cond);

	if (result == 1) {
	    set_error_from_ndmp(self);
	    return 1;
	} else if (result == 2) {
	    return 2;
	}

	if (reason != NDMP9_MOVER_PAUSE_SEEK &&
	    reason != NDMP9_MOVER_PAUSE_EOW) {
	    device_set_error(DEVICE(self),
	    g_strdup_printf(
		"got NOTIFY_MOVER_PAUSED, but not because of EOW or SEEK"),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}
    }

    /* NDMJOB sends NDMP9_MOVER_PAUSE_SEEK to indicate that it wants to write
     * outside the window, while the standard specifies .._EOW, instead.  When
     * reading to a connection, we get the appropriate .._SEEK.  It's easy
     * enough to handle both. */

    if (self->indirecttcp_sock == -1) {
	g_free(self->listen_addrs);
	self->listen_addrs = NULL;
    }

    if (self->for_writing)
	mode = NDMP9_MOVER_MODE_READ;
    else
	mode = NDMP9_MOVER_MODE_WRITE;

    /* set up the new directtcp connection */
    if (self->directtcp_conn)
	g_object_unref(self->directtcp_conn);
    self->directtcp_conn =
	directtcp_connection_ndmp_new(self->ndmp, mode);
    *dtcpconn = DIRECTTCP_CONNECTION(self->directtcp_conn);

    /* reference it for the caller */
    g_object_ref(*dtcpconn);

    return 0;
}

static gboolean
connect_impl(
    Device *dself,
    gboolean for_writing,
    DirectTCPAddr *addrs,
    DirectTCPConnection **dtcpconn,
    ProlongProc prolong,
    gpointer prolong_data)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    ndmp9_mover_mode mode;
    ndmp9_mover_pause_reason reason;
    guint64 seek_position;

    g_assert(!self->listen_addrs);

    *dtcpconn = NULL;
    self->for_writing = for_writing;

    /* TODO: support aborting this operation - maybe just always poll? */
    prolong = prolong;
    prolong_data = prolong_data;

    if (!open_tape_agent(self)) {
	/* error message was set by open_tape_agent */
	return FALSE;
    }

    /* first, set the window to an empty span so that the mover doesn't start
     * reading or writing data immediately.  NDMJOB tends to reset the record
     * size periodically (in direct contradiction to the spec), so we reset it
     * here as well. */
    if (!ndmp_connection_mover_set_record_size(self->ndmp,
		DEVICE(self)->block_size)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (!ndmp_connection_mover_set_window(self->ndmp, 0, 0)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (self->for_writing)
	mode = NDMP9_MOVER_MODE_READ;
    else
	mode = NDMP9_MOVER_MODE_WRITE;

    if (!ndmp_connection_mover_connect(self->ndmp, mode, addrs)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (!self->for_writing) {
	/* The agent is in the ACTIVE state, and will remain so until we tell
	 * it to do something else.  The thing we want to is for it to start
	 * reading data from the tape, which will immediately trigger an EOW or
	 * SEEK pause. */
	if (!ndmp_connection_mover_read(self->ndmp, 0, G_MAXUINT64)) {
	    set_error_from_ndmp(self);
	    return FALSE;
	}

	/* now we should expect a notice that the mover has paused */
    } else {
	/* when writing, the mover will pause as soon as the first byte comes
	 * in, so there's no need to do anything to trigger the pause. */
    }

    /* NDMJOB sends NDMP9_MOVER_PAUSE_SEEK to indicate that it wants to write
     * outside the window, while the standard specifies .._EOW, instead.  When
     * reading to a connection, we get the appropriate .._SEEK.  It's easy
     * enough to handle both. */

    if (!ndmp_connection_wait_for_notify(self->ndmp,
	    NULL,
	    NULL,
	    &reason, &seek_position)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (reason != NDMP9_MOVER_PAUSE_SEEK && reason != NDMP9_MOVER_PAUSE_EOW) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("got NOTIFY_MOVER_PAUSED, but not because of EOW or SEEK"),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (self->listen_addrs) {
	g_free(self->listen_addrs);
	self->listen_addrs = NULL;
    }

    /* set up the new directtcp connection */
    if (self->directtcp_conn)
	g_object_unref(self->directtcp_conn);
    self->directtcp_conn =
	directtcp_connection_ndmp_new(self->ndmp, mode);
    *dtcpconn = DIRECTTCP_CONNECTION(self->directtcp_conn);

    /* reference it for the caller */
    g_object_ref(*dtcpconn);

    return TRUE;
}

static gboolean
connect_with_cond_impl(
    Device *dself,
    gboolean for_writing,
    DirectTCPAddr *addrs,
    DirectTCPConnection **dtcpconn,
    GMutex *abort_mutex,
    GCond *abort_cond)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    ndmp9_mover_mode mode;
    ndmp9_mover_pause_reason reason;
    guint64 seek_position;
    int result;

    g_assert(!self->listen_addrs);

    *dtcpconn = NULL;
    self->for_writing = for_writing;

    if (!open_tape_agent(self)) {
	/* error message was set by open_tape_agent */
	return 1;
    }

    /* first, set the window to an empty span so that the mover doesn't start
     * reading or writing data immediately.  NDMJOB tends to reset the record
     * size periodically (in direct contradiction to the spec), so we reset it
     * here as well. */
    if (!ndmp_connection_mover_set_record_size(self->ndmp,
		DEVICE(self)->block_size)) {
	set_error_from_ndmp(self);
	return 1;
    }

    if (!ndmp_connection_mover_set_window(self->ndmp, 0, 0)) {
	set_error_from_ndmp(self);
	return 1;
    }

    if (self->for_writing)
	mode = NDMP9_MOVER_MODE_READ;
    else
	mode = NDMP9_MOVER_MODE_WRITE;

    if (!ndmp_connection_mover_connect(self->ndmp, mode, addrs)) {
	set_error_from_ndmp(self);
	return 1;
    }

    if (!self->for_writing) {
	/* The agent is in the ACTIVE state, and will remain so until we tell
	 * it to do something else.  The thing we want to is for it to start
	 * reading data from the tape, which will immediately trigger an EOW or
	 * SEEK pause. */
	if (!ndmp_connection_mover_read(self->ndmp, 0, G_MAXUINT64)) {
	    set_error_from_ndmp(self);
	    return 1;
	}

	/* now we should expect a notice that the mover has paused */
    } else {
	/* when writing, the mover will pause as soon as the first byte comes
	 * in, so there's no need to do anything to trigger the pause. */
    }

    /* NDMJOB sends NDMP9_MOVER_PAUSE_SEEK to indicate that it wants to write
     * outside the window, while the standard specifies .._EOW, instead.  When
     * reading to a connection, we get the appropriate .._SEEK.  It's easy
     * enough to handle both. */

    result = ndmp_connection_wait_for_notify_with_cond(self->ndmp,
	    NULL,
	    NULL,
	    &reason, &seek_position,
	    abort_mutex, abort_cond);

    if (result == 1) {
	set_error_from_ndmp(self);
	return 1;
    } else if (result == 2) {
	return 2;
    }

    if (reason != NDMP9_MOVER_PAUSE_SEEK && reason != NDMP9_MOVER_PAUSE_EOW) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("got NOTIFY_MOVER_PAUSED, but not because of EOW or SEEK"),
	    DEVICE_STATUS_DEVICE_ERROR);
	return 1;
    }

    if (self->listen_addrs) {
	g_free(self->listen_addrs);
	self->listen_addrs = NULL;
    }

    /* set up the new directtcp connection */
    if (self->directtcp_conn)
	g_object_unref(self->directtcp_conn);
    self->directtcp_conn =
	directtcp_connection_ndmp_new(self->ndmp, mode);
    *dtcpconn = DIRECTTCP_CONNECTION(self->directtcp_conn);

    /* reference it for the caller */
    g_object_ref(*dtcpconn);

    return 0;
}

static gboolean
indirecttcp_start_writing(
	NdmpDevice *self)
{
    DirectTCPAddr *real_addrs, *iter;
    int conn_sock;

    /* The current state is that the other end is trying to connect to
     * indirecttcp_sock.  The mover remains IDLE, although its window is set
     * correctly for the part we are about to write. */

    g_debug("indirecttcp_start_writing, ready to accept");
    conn_sock = accept(self->indirecttcp_sock, NULL, NULL);
    if (conn_sock < 0) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("Could not accept indirecttcp socket: %s", strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
    g_debug("indirecttcp_start_writing, accepted");

    close(self->indirecttcp_sock);
    self->indirecttcp_sock = -1;

    /* tell mover to start listening */
    g_assert(self->for_writing);
    if (!ndmp_connection_mover_listen(self->ndmp,
		NDMP4_MOVER_MODE_READ,
		NDMP4_ADDR_TCP,
		&real_addrs)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    /* format the addresses and send them down the socket */
    for (iter = real_addrs; iter && SU_GET_FAMILY(iter) != 0; iter++) {
	char inet[INET_ADDRSTRLEN];
	const char *addr;
	char *addrspec;

	addr = inet_ntop(AF_INET, &iter->sin.sin_addr.s_addr, inet, 40);

	addrspec = g_strdup_printf("%s:%d%s", addr, SU_GET_PORT(iter),
		SU_GET_FAMILY(iter+1) !=0? " ":"");
    g_debug("indirecttcp_start_writing, send %s", addrspec);
	if (full_write(conn_sock, addrspec, strlen(addrspec)) < strlen(addrspec)) {
	    device_set_error(DEVICE(self),
		g_strdup_printf("writing to indirecttcp socket: %s", strerror(errno)),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}
    }

    /* close the socket for good.  This ensures that the next call to
     * write_from_connection_impl will not go through the mover setup process.
     * */
    if (close(conn_sock) < 0) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("closing indirecttcp socket: %s", strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
    conn_sock = -1;

    /* and free the listen_addrs, since we didn't free them in accept_impl */
    if (self->listen_addrs) {
	g_free(self->listen_addrs);
	self->listen_addrs = NULL;
    }

    /* Now it's up to the remote end to connect to the mover and start sending
     * data.  We won't get any notification when this happens, although we could
     * in principle poll for such a thing. */
    return TRUE;
}

static gboolean
write_from_connection_impl(
    Device *dself,
    guint64 size,
    guint64 *actual_size)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    DirectTCPConnectionNDMP *nconn = self->directtcp_conn;
    gboolean eom = FALSE, eof = FALSE, eow = FALSE;
    ndmp9_mover_state mover_state;
    ndmp9_mover_halt_reason halt_reason;
    ndmp9_mover_pause_reason pause_reason;
    guint64 bytes_moved_before, bytes_moved_after;
    gchar *err;

    if (device_in_error(self)) return FALSE;

    g_debug("write_from_connection_impl");
    if (actual_size)
	*actual_size = 0;

    /* if this is false, then the caller did not use use_connection correctly */
    g_assert(self->directtcp_conn != NULL);
    g_assert(self->ndmp == nconn->ndmp);
    g_assert(nconn->mode == NDMP9_MOVER_MODE_READ);

    if (!ndmp_connection_mover_get_state(self->ndmp,
		&mover_state, &bytes_moved_before, NULL, NULL)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (self->indirecttcp_sock != -1) {
	/* If we're doing IndirectTCP, then we've deferred the whole
	 * mover_set_window mover_listen process.. until now.
	 * So the mover should be IDLE.
	 */
	g_assert(mover_state == NDMP9_MOVER_STATE_IDLE);
    } else {
	/* the mover had best be PAUSED right now */
	g_assert(mover_state == NDMP9_MOVER_STATE_PAUSED);
    }

    /* we want to set the window regardless of whether this is directtcp or
     * indirecttcp
     */
    if (!ndmp_connection_mover_set_window(self->ndmp,
		nconn->offset,
		size? size : G_MAXUINT64 - nconn->offset)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    /* for DirectTCP, we just tell the mover to continue; IndirectTCP is more complicated. */
    if (self->indirecttcp_sock != -1) {
	if (!indirecttcp_start_writing(self)) {
	    return FALSE;
	}
    } else {
	if (!ndmp_connection_mover_continue(self->ndmp)) {
	    set_error_from_ndmp(self);
	    return FALSE;
	}
    }

    /* now wait for the mover to pause itself again, or halt on EOF or an error */
    if (!ndmp_connection_wait_for_notify(self->ndmp,
	    NULL,
	    &halt_reason,
	    &pause_reason, NULL)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    err = NULL;
    if (pause_reason) {
	switch (pause_reason) {
	    case NDMP9_MOVER_PAUSE_EOM:
		eom = TRUE;
		break;

	    /* ndmjob sends .._SEEK when it should send .._EOW, so deal with
		* both equivalently */
	    case NDMP9_MOVER_PAUSE_EOW:
	    case NDMP9_MOVER_PAUSE_SEEK:
		eow = TRUE;
		break;

	    default:
		err = "got NOTIFY_MOVER_PAUSED, but not because of EOW or SEEK";
		break;
	}
    } else if (halt_reason) {
	switch (halt_reason) {
	    case NDMP9_MOVER_HALT_CONNECT_CLOSED:
		eof = TRUE;
		break;

	    default:
	    case NDMP9_MOVER_HALT_ABORTED:
	    /* case NDMP9_MOVER_HALT_MEDIA_ERROR: <-- not in ndmjob */
	    case NDMP9_MOVER_HALT_INTERNAL_ERROR:
	    case NDMP9_MOVER_HALT_CONNECT_ERROR:
		err = "unexpected NDMP_NOTIFY_MOVER_HALTED";
		break;
	}
    }

    if (err) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("waiting for accept: %s", err),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    /* no error, so the mover stopped due to one of EOM (volume out of space),
     * EOF (data connection is done), or EOW (maximum part size was written).
     * In any case, we want to know how many bytes were written. */

    if (!ndmp_connection_mover_get_state(self->ndmp,
		&mover_state, &bytes_moved_after, NULL, NULL)) {
	set_error_from_ndmp(self);
	return FALSE;
    }
    size = bytes_moved_after - bytes_moved_before;
    nconn->offset += size;

    if (actual_size) {
	*actual_size = bytes_moved_after - bytes_moved_before;
    }

    if (eow) {
        ; /* mover finished the whole part -- nothing to report! */
    } else if (eof) {
        DEVICE(self)->is_eof = TRUE;
    } else if (eom) {
        /* this is a *lossless* EOM, so no need to set error, but
         * we do need to figure out the actual size */
        DEVICE(self)->is_eom = TRUE;
    } else {
        error("not reached");
    }

    return TRUE;
}

static gboolean
read_to_connection_impl(
    Device *dself,
    guint64 size,
    guint64 *actual_size)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    DirectTCPConnectionNDMP *nconn = self->directtcp_conn;
    gboolean eom = FALSE, eof = FALSE, eow = FALSE;
    ndmp9_mover_state mover_state;
    ndmp9_mover_halt_reason halt_reason;
    ndmp9_mover_pause_reason pause_reason;
    guint64 bytes_moved_before, bytes_moved_after;
    gchar *err;

    if (actual_size)
	*actual_size = 0;

    if (device_in_error(self)) return FALSE;

    /* read_to_connection does not support IndirectTCP */
    g_assert(self->indirecttcp_sock == -1);

    /* if this is false, then the caller did not use use_connection correctly */
    g_assert(nconn != NULL);
    g_assert(self->ndmp == nconn->ndmp);
    g_assert(nconn->mode == NDMP9_MOVER_MODE_WRITE);

    if (!ndmp_connection_mover_get_state(self->ndmp,
		&mover_state, &bytes_moved_before, NULL, NULL)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    /* the mover had best be PAUSED right now */
    g_assert(mover_state == NDMP9_MOVER_STATE_PAUSED);

    if (!ndmp_connection_mover_set_window(self->ndmp,
		nconn->offset,
		size? size : G_MAXUINT64 - nconn->offset)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    if (!ndmp_connection_mover_continue(self->ndmp)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    /* now wait for the mover to pause itself again, or halt on EOF or an error */
    if (!ndmp_connection_wait_for_notify(self->ndmp,
	    NULL,
	    &halt_reason,
	    &pause_reason, NULL)) {
	set_error_from_ndmp(self);
	return FALSE;
    }

    err = NULL;
    if (pause_reason) {
	switch (pause_reason) {
	    case NDMP9_MOVER_PAUSE_EOF:
		eof = TRUE;
		break;

	    /* ndmjob sends .._SEEK when it should send .._EOW, so deal with
		* both equivalently */
	    case NDMP9_MOVER_PAUSE_EOW:
	    case NDMP9_MOVER_PAUSE_SEEK:
		eow = TRUE;
		break;

	    default:
		err = "got NOTIFY_MOVER_PAUSED, but not because of EOW or SEEK";
		break;
	}
    } else if (halt_reason) {
	switch (halt_reason) {
	    case NDMP9_MOVER_HALT_CONNECT_CLOSED:
		eof = TRUE;
		break;

	    default:
	    case NDMP9_MOVER_HALT_ABORTED:
	    /* case NDMP9_MOVER_HALT_MEDIA_ERROR: <-- not in ndmjob */
	    case NDMP9_MOVER_HALT_INTERNAL_ERROR:
	    case NDMP9_MOVER_HALT_CONNECT_ERROR:
		err = "unexpected NDMP_NOTIFY_MOVER_HALTED";
		break;
	}
    }

    if (err) {
	device_set_error(DEVICE(self),
	    g_strdup_printf("waiting for accept: %s", err),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    /* no error, so the mover stopped due to one of EOM (volume out of space),
     * EOF (data connection is done), or EOW (maximum part size was written).
     * In any case, we want to know how many bytes were written. */

    if (!ndmp_connection_mover_get_state(self->ndmp,
		&mover_state, &bytes_moved_after, NULL, NULL)) {
	set_error_from_ndmp(self);
	return FALSE;
    }
    size = bytes_moved_after - bytes_moved_before;
    nconn->offset += size;

    if (actual_size) {
	*actual_size = bytes_moved_after - bytes_moved_before;
    }

    if (eow) {
        ; /* mover finished the whole part -- nothing to report! */
    } else if (eof) {
        DEVICE(self)->is_eof = TRUE;
    } else if (eom) {
        /* this is a *lossless* EOM, so no need to set error, but
         * we do need to figure out the actual size */
        DEVICE(self)->is_eom = TRUE;
    } else {
        error("not reached");
    }

    return TRUE;
}

static gboolean
use_connection_impl(
    Device *dself,
    DirectTCPConnection *conn)
{
    NdmpDevice *self = NDMP_DEVICE(dself);
    DirectTCPConnectionNDMP *nconn;

    /* the device_use_connection_impl wrapper already made sure we're in
     * ACCESS_NULL, but we may have opened the tape service already to read
     * a label - so close it to be sure */
    if (!close_tape_agent(self)) {
	/* error was already set by close_tape_agent */
	return FALSE;
    }

    /* we had best not be listening when this is called */
    g_assert(!self->listen_addrs);

    if (!IS_DIRECTTCP_CONNECTION_NDMP(conn)) {
	device_set_error(DEVICE(self),
	    g_strdup("existing DirectTCPConnection is not compatible with this device"),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    if (self->directtcp_conn)
	g_object_unref(self->directtcp_conn);
    self->directtcp_conn = nconn = DIRECTTCP_CONNECTION_NDMP(conn);
    g_object_ref(self->directtcp_conn);

    /* if this is a different connection, use it */
    if (nconn->ndmp != self->ndmp) {
	if (self->ndmp)
	    close_connection(self);
	self->ndmp = nconn->ndmp;
	g_object_ref(self->ndmp);
    }

    return TRUE;
}

/*
 * Class mechanics
 */

static gboolean
ndmp_device_set_username_fn(Device *dself,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    amfree(self->ndmp_username);
    self->ndmp_username = g_value_dup_string(val);
    device_clear_volume_details(dself);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
ndmp_device_set_password_fn(Device *dself,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    amfree(self->ndmp_password);
    self->ndmp_password = g_value_dup_string(val);
    device_clear_volume_details(dself);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
ndmp_device_set_auth_fn(Device *dself,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    amfree(self->ndmp_auth);
    self->ndmp_auth = g_value_dup_string(val);
    device_clear_volume_details(dself);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
ndmp_device_set_verbose_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    NdmpDevice *self = NDMP_DEVICE(p_self);

    self->verbose = g_value_get_boolean(val);

    /* if the connection is active, set up verbose logging or turn it off */
    if (self->ndmp) {
	ndmp_connection_set_verbose(self->ndmp, self->verbose);
    }


    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
ndmp_device_set_read_block_size_fn(Device *p_self, DevicePropertyBase *base G_GNUC_UNUSED,
    GValue *val, PropertySurety surety, PropertySource source)
{
    NdmpDevice *self = NDMP_DEVICE(p_self);
    gsize read_block_size = g_value_get_uint(val);

    if (read_block_size != 0 &&
	    ((gsize)read_block_size < p_self->block_size ||
	     (gsize)read_block_size > p_self->max_block_size)) {
	device_set_error(p_self,
	    g_strdup_printf("Error setting READ-BLOCk-SIZE property to '%zu', it must be between %zu and %zu", read_block_size, p_self->block_size, p_self->max_block_size),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    self->read_block_size = read_block_size;

    /* use the READ_BLOCK_SIZE, even if we're invoked to get the old READ_BUFFER_SIZE */
    return device_simple_property_set_fn(p_self, base,
					val, surety, source);
}

static gboolean
ndmp_device_set_indirect_fn(Device *dself,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    self->indirect = g_value_get_boolean(val);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static void
ndmp_device_class_init(NdmpDeviceClass * c G_GNUC_UNUSED)
{
    GObjectClass *g_object_class = (GObjectClass*) c;
    DeviceClass *device_class = (DeviceClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->open_device = ndmp_device_open_device;
    device_class->read_label = ndmp_device_read_label;
    device_class->start = ndmp_device_start;
    device_class->finish = ndmp_device_finish;
    device_class->eject = ndmp_device_eject;

    device_class->start_file = ndmp_device_start_file;
    device_class->write_block = ndmp_device_write_block;
    device_class->finish_file = ndmp_device_finish_file;

    device_class->seek_file = ndmp_device_seek_file;
    device_class->seek_block = ndmp_device_seek_block;
    device_class->read_block = ndmp_device_read_block;

    device_class->directtcp_supported = TRUE;
    device_class->listen = listen_impl;
    device_class->accept = accept_impl;
    device_class->accept_with_cond = accept_with_cond_impl;
    device_class->connect = connect_impl;
    device_class->connect_with_cond = connect_with_cond_impl;
    device_class->write_from_connection = write_from_connection_impl;
    device_class->read_to_connection = read_to_connection_impl;
    device_class->use_connection = use_connection_impl;

    g_object_class->finalize = ndmp_device_finalize;

    device_class_register_property(device_class, PROPERTY_NDMP_USERNAME,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    ndmp_device_set_username_fn);

    device_class_register_property(device_class, PROPERTY_NDMP_PASSWORD,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    ndmp_device_set_password_fn);

    device_class_register_property(device_class, PROPERTY_NDMP_AUTH,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    ndmp_device_set_auth_fn);

    device_class_register_property(device_class, PROPERTY_VERBOSE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_MASK,
	    device_simple_property_get_fn,
	    ndmp_device_set_verbose_fn);

    device_class_register_property(device_class, PROPERTY_INDIRECT,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_MASK,
	    device_simple_property_get_fn,
	    ndmp_device_set_indirect_fn);

    device_class_register_property(device_class, PROPERTY_READ_BLOCK_SIZE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    ndmp_device_set_read_block_size_fn);
}

static void
ndmp_device_init(NdmpDevice *self)
{
    Device *dself = DEVICE(self);
    GValue response;

    /* begin unconnected */
    self->ndmp = NULL;

    /* decent defaults */
    dself->block_size = 32768;
    dself->min_block_size = 32768;
    dself->max_block_size = SIZE_MAX;

    bzero(&response, sizeof(response));

    g_value_init(&response, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(&response, CONCURRENCY_PARADIGM_EXCLUSIVE);
    device_set_simple_property(dself, PROPERTY_CONCURRENCY,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(&response, STREAMING_REQUIREMENT_DESIRED);
    device_set_simple_property(dself, PROPERTY_STREAMING,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_APPENDABLE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_PARTIAL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_FULL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE);
    device_set_simple_property(dself, PROPERTY_LEOM,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_READ_WRITE);
    device_set_simple_property(dself, PROPERTY_MEDIUM_ACCESS_TYPE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    self->read_block_size = 0;
    g_value_init(&response, G_TYPE_UINT);
    g_value_set_uint(&response, self->read_block_size);
    device_set_simple_property(dself, PROPERTY_READ_BLOCK_SIZE,
            &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_STRING);
    g_value_set_string(&response, "ndmp");
    device_set_simple_property(dself, PROPERTY_NDMP_USERNAME,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);
    self->ndmp_username = g_strdup("ndmp");

    g_value_init(&response, G_TYPE_STRING);
    g_value_set_string(&response, "ndmp");
    device_set_simple_property(dself, PROPERTY_NDMP_PASSWORD,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);
    self->ndmp_password = g_strdup("ndmp");

    g_value_init(&response, G_TYPE_STRING);
    g_value_set_string(&response, "md5");
    device_set_simple_property(dself, PROPERTY_NDMP_AUTH,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);
    self->ndmp_auth = g_strdup("md5");

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(dself, PROPERTY_INDIRECT,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);
    self->indirect = FALSE;

    self->indirecttcp_sock = -1;
}

static GType
ndmp_device_get_type(void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (NdmpDeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) ndmp_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (NdmpDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) ndmp_device_init,
            NULL
        };

        type = g_type_register_static (TYPE_DEVICE, "NdmpDevice", &info,
                                       (GTypeFlags)0);
    }

    return type;
}

static Device*
ndmp_device_factory(
    char *device_name,
    char *device_type,
    char *device_node)
{
    Device *rval;
    g_assert(0 == strcmp(device_type, NDMP_DEVICE_NAME));
    rval = DEVICE(g_object_new(TYPE_NDMP_DEVICE, NULL));

    device_open_device(rval, device_name, device_type, device_node);
    return rval;
}

void
ndmp_device_register(void)
{
    static const char * device_prefix_list[] = { NDMP_DEVICE_NAME, NULL };

    /* register the device itself */
    register_device(ndmp_device_factory, device_prefix_list);

    device_property_fill_and_register(&device_property_ndmp_username,
                                      G_TYPE_STRING, "ndmp_username",
       "Username for access to the NDMP agent");
    device_property_fill_and_register(&device_property_ndmp_password,
                                      G_TYPE_STRING, "ndmp_password",
       "Password for access to the NDMP agent");
    device_property_fill_and_register(&device_property_ndmp_auth,
                                      G_TYPE_STRING, "ndmp_auth",
       "Authentication method for the NDMP agent - md5 (default), text, none, or void");
    device_property_fill_and_register(&device_property_indirect,
                                      G_TYPE_BOOLEAN, "indirect",
       "Use Indirect TCP mode, even if the NDMP server supports "
       "window length 0");
}

/*
 * DirectTCPConnectionNDMP implementation
 */

static char *
directtcp_connection_ndmp_close(DirectTCPConnection *dself)
{
    DirectTCPConnectionNDMP *self = DIRECTTCP_CONNECTION_NDMP(dself);
    char *rv = NULL;
    ndmp9_mover_state state;
    guint64 bytes_moved;
    ndmp9_mover_halt_reason reason;
    gboolean expect_notif = FALSE;

    /* based on the current state, we may need to abort or stop the
     * mover before closing it */
    if (!ndmp_connection_mover_get_state(self->ndmp, &state,
				    &bytes_moved, NULL, NULL)) {
	rv = ndmp_connection_err_msg(self->ndmp);
	goto error;
    }

    switch (state) {
	case NDMP9_MOVER_STATE_HALTED:
	    break; /* nothing to do but ndmp_mover_close, below */

	case NDMP9_MOVER_STATE_PAUSED:
	    if (!ndmp_connection_mover_close(self->ndmp)) {
		rv = ndmp_connection_err_msg(self->ndmp);
		goto error;
	    }
	    expect_notif = TRUE;
	    break;

	case NDMP9_MOVER_STATE_ACTIVE:
	default:
	    if (!ndmp_connection_mover_abort(self->ndmp)) {
		rv = ndmp_connection_err_msg(self->ndmp);
		goto error;
	    }
	    expect_notif = TRUE;
	    break;
    }

    /* the spec isn't entirely clear that mover_close and mover_abort should
     * generate a NOTIF_MOVER_HALTED, but ndmjob does it */
    if (expect_notif) {
	if (!ndmp_connection_wait_for_notify(self->ndmp,
		NULL,
		&reason, /* value is ignored.. */
		NULL, NULL)) {
	    goto error;
	}
    }

    if (!ndmp_connection_mover_stop(self->ndmp)) {
	rv = ndmp_connection_err_msg(self->ndmp);
	goto error;
    }

error:
    if (self->ndmp) {
	g_object_unref(self->ndmp);
	self->ndmp = NULL;
    }

    return rv;
}

static void
directtcp_connection_ndmp_class_init(DirectTCPConnectionNDMPClass * c)
{
    DirectTCPConnectionClass *connc = (DirectTCPConnectionClass *)c;

    connc->close = directtcp_connection_ndmp_close;
}

GType
directtcp_connection_ndmp_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (DirectTCPConnectionNDMPClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) directtcp_connection_ndmp_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (DirectTCPConnectionNDMP),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static(TYPE_DIRECTTCP_CONNECTION,
                                "DirectTCPConnectionNDMP", &info, (GTypeFlags)0);
    }

    return type;
}

static DirectTCPConnectionNDMP *
directtcp_connection_ndmp_new(
    NDMPConnection *ndmp,
    ndmp9_mover_mode mode)
{
    DirectTCPConnectionNDMP *dcn = DIRECTTCP_CONNECTION_NDMP(
	    g_object_new(TYPE_DIRECTTCP_CONNECTION_NDMP, NULL));

    /* hang onto a copy of this NDMP connection */
    g_object_ref(ndmp);
    dcn->ndmp = ndmp;
    dcn->mode = mode;
    dcn->offset = 0;

    return dcn;
}
