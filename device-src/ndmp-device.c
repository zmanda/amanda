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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "util.h"
#include "device.h"
#include "ndmlib.h"

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
typedef struct _NdmpMetadataFile NdmpMetadataFile;

typedef struct _NdmpDevice NdmpDevice;
struct _NdmpDevice {
    Device __parent__;

    /* TODO: make this settable via a property */
    struct ndmconn *conn;
    gboolean tape_open;

    /* constructor parameters and properties */
    gchar	 *ndmp_hostname;
    gint	 ndmp_port;
    gchar        *ndmp_device_name;
    gchar	 *ndmp_username;
    gchar	 *ndmp_password;
    gboolean	 verbose;
};

/*
 * Class definition
 */
typedef struct _NdmpDeviceClass NdmpDeviceClass;
struct _NdmpDeviceClass {
    DeviceClass __parent__;
};


/*
 * Constants and static data
 */

#define NDMP_DEVICE_NAME "ndmp"

/* level at which to snoop when VERBOSE is set; 8 = everything but hexdumps,
 * and 5 = packets without details */
#define SNOOP_LEVEL 7

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

/* logging information (for VERBOSE) */
static struct ndmlog device_ndmlog;

/*
 * device-specific properties
 */

/* Authentication information for NDMP agent. Both of these are strings. */
static DevicePropertyBase device_property_ndmp_username;
static DevicePropertyBase device_property_ndmp_password;
#define PROPERTY_NDMP_PASSWORD (device_property_ndmp_password.ID)
#define PROPERTY_NDMP_USERNAME (device_property_ndmp_username.ID)

/*
 * prototypes
 */

void ndmp_device_register(void);
static gboolean handle_conn_failure(NdmpDevice *self, int rc);

/*
 * Utility functions
 */

static gboolean
open_connection(
	NdmpDevice *self)
{
    if (!self->conn) {
	struct ndmconn *conn = ndmconn_initialize(NULL, "amanda-server");
	if (!conn) {
	    device_set_error(DEVICE(self),
		g_strdup("could not initialize ndmconn"),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}

	if (self->verbose) {
	    ndmconn_set_snoop(conn, &device_ndmlog, SNOOP_LEVEL);
	}

	if (ndmconn_connect_host_port(conn, self->ndmp_hostname, self->ndmp_port, 0) != 0) {
	    device_set_error(DEVICE(self),
		g_strdup_printf("could not connect to ndmp-server '%s:%d': %s",
		    self->ndmp_hostname, self->ndmp_port, ndmconn_get_err_msg(conn)),
		DEVICE_STATUS_DEVICE_ERROR);
	    ndmconn_destruct(conn);
	    return FALSE;
	}

	if (ndmconn_auth_md5(conn, self->ndmp_username, self->ndmp_password) != 0) {
	    device_set_error(DEVICE(self),
		g_strdup_printf("could not authenticate to ndmp-server '%s:%d': %s",
		    self->ndmp_hostname, self->ndmp_port, ndmconn_get_err_msg(conn)),
		DEVICE_STATUS_DEVICE_ERROR);
	    ndmconn_destruct(conn);
	    return FALSE;
	}

	self->conn = conn;
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
    if (self->conn) {
	ndmconn_destruct(self->conn);
	self->conn = NULL;
	self->tape_open = FALSE;
    }
}

static gboolean
open_ndmp_device(
    NdmpDevice *self)
{
    struct ndmconn *conn;
    int rc;

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
    /* send NDMP_TAPE_OPEN */
    conn = self->conn;
    NDMC_WITH(ndmp9_tape_open, self->conn->protocol_version)
	request->device = self->ndmp_device_name;
	request->mode = NDMP9_TAPE_RDWR_MODE;
	rc = NDMC_CALL(self->conn);
	if (!handle_conn_failure(self, rc)) {
	    NDMC_FREE_REPLY();
	    return FALSE;
	}
	NDMC_FREE_REPLY();
    NDMC_ENDWITH

    self->tape_open = TRUE;

    return TRUE;
}

static gboolean
close_ndmp_device(
	NdmpDevice *self)
{
    struct ndmconn *conn = self->conn;
    int rc;

    if (self->tape_open) {
	g_debug("closing tape device '%s' on NDMP server '%s:%d'",
	    self->ndmp_device_name, self->ndmp_hostname, self->ndmp_port);
	NDMC_WITH_VOID_REQUEST(ndmp9_tape_close, self->conn->protocol_version)
	    rc = NDMC_CALL(self->conn);
	    if (!handle_conn_failure(self, rc)) {
		NDMC_FREE_REPLY();
		return FALSE;
	    }
	    NDMC_FREE_REPLY();
	NDMC_ENDWITH
	self->tape_open = FALSE;
    }

    return TRUE;
}

static gboolean
ndmp_mtio(
    NdmpDevice *self,
    int tape_op,
    int count)
{
    struct ndmconn *conn = self->conn;
    int rc;

    NDMC_WITH(ndmp9_tape_mtio, self->conn->protocol_version)
	request->tape_op = tape_op;
	request->count = count;
	rc = NDMC_CALL(self->conn);
	if (!handle_conn_failure(self, rc)) {
	    NDMC_FREE_REPLY();
	    return FALSE;
	}
	NDMC_FREE_REPLY();
    NDMC_ENDWITH

    return TRUE;
}

static gboolean
ndmp_mtio_eof(
    NdmpDevice *self)
{
    return ndmp_mtio(self, NDMP9_MTIO_EOF, 1);
}

static gboolean
ndmp_mtio_rewind(
    NdmpDevice *self)
{
    return ndmp_mtio(self, NDMP9_MTIO_REW, 1);
}

static gboolean
robust_write(
    NdmpDevice *self,
    char *buf,
    gsize count)
{
    int rc;
    struct ndmconn *conn = self->conn;

    /* TODO: handle EOM better; use IoResult? */
    NDMC_WITH(ndmp9_tape_write, self->conn->protocol_version)
	request->data_out.data_out_val = buf;
	request->data_out.data_out_len = count;
	rc = NDMC_CALL(self->conn);

	if (!handle_conn_failure(self, rc)) {
	    NDMC_FREE_REPLY();
	    return FALSE;
	}
	/* TODO: check this and handle it better */
	g_assert(reply->count == count);
	NDMC_FREE_REPLY();
    NDMC_ENDWITH
    return TRUE;
}

static gboolean
handle_conn_failure(
    NdmpDevice *self,
    int rc)
{
    if (rc) {
	if (rc == NDMCONN_CALL_STATUS_REPLY_ERROR) {
	    device_set_error(DEVICE(self),
		g_strdup_printf("Error from NDMP server: %s",
			ndmp9_error_to_str(self->conn->last_reply_error)),
		DEVICE_STATUS_DEVICE_ERROR);
	} else {
	    device_set_error(DEVICE(self),
		g_strdup_printf("ndmconn error %d: %s", rc, ndmconn_get_err_msg(self->conn)),
		DEVICE_STATUS_DEVICE_ERROR);
	}
	close_connection(self);
	return FALSE;
    }
    return TRUE;
}

static void
ndmp_device_ndmlog_deliver(
    struct ndmlog *log G_GNUC_UNUSED,
    char *tag,
    int lev G_GNUC_UNUSED,
    char *msg)
{
    g_debug("%s: %s", tag, msg);
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

    g_debug("ndmp_device_open_device: %s : %s : %s", device_name, device_type, device_node);

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

    self->ndmp_hostname = g_strndup(device_node, colon-device_node);
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
    } else {
	self->ndmp_port = 0; /* (use ndmjob's default, 10000) */
    }
    self->ndmp_device_name = g_strdup(at+1);

    /* these should be changed by properties */
    self->ndmp_username = g_strdup("ndmp");
    self->ndmp_password = g_strdup("ndmp");

    if (parent_class->open_device) {
        parent_class->open_device(dself, device_name, device_type, device_node);
    }
}

static void ndmp_device_finalize(GObject * obj_self)
{
    NdmpDevice       *self = NDMP_DEVICE (obj_self);

    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    close_connection(self);

    if (self->ndmp_hostname)
	g_free(self->ndmp_hostname);
    if (self->ndmp_device_name)
	g_free(self->ndmp_device_name);
    if (self->ndmp_username)
	g_free(self->ndmp_username);
    if (self->ndmp_password)
	g_free(self->ndmp_password);
}

static DeviceStatusFlags
ndmp_device_read_label(
    Device *dself)
{
    NdmpDevice       *self = NDMP_DEVICE(dself);
    dumpfile_t       *header;
    struct ndmconn *conn;
    int rc;

    amfree(dself->volume_label);
    amfree(dself->volume_time);
    amfree(dself->volume_header);

    if (device_in_error(self)) return dself->status;

    header = dself->volume_header = g_new(dumpfile_t, 1);
    fh_init(header);

    if (!open_ndmp_device(self)) {
	/* error status was set by open_ndmp_device */
	return dself->status;
    }

    if (!ndmp_mtio_rewind(self)) {
	/* error message, if any, is set by ndmp_mtio_rewind */
	return dself->status;
    }

    /* read the tape header from the NDMP server */
    dself->status = 0;
    conn = self->conn;
    NDMC_WITH(ndmp9_tape_read, self->conn->protocol_version)
	request->count = dself->block_size;
	rc = NDMC_CALL(self->conn);

	/* handle known errors */
	if (rc == NDMCONN_CALL_STATUS_REPLY_ERROR) {
	    switch (self->conn->last_reply_error) {
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
		goto read_err;

	    default:
		break;
	    }
	}
	if (!handle_conn_failure(self, rc)) {
	    goto read_err;
	}
	parse_file_header(reply->data_in.data_in_val, header, reply->data_in.data_in_len);
read_err:
	NDMC_FREE_REPLY();
    NDMC_ENDWITH

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
    dumpfile_t *header;
    char       *header_buf;
    NdmpDevice *self = NDMP_DEVICE(dself);

    self = NDMP_DEVICE(dself);

    if (device_in_error(self)) return FALSE;

    if (!open_ndmp_device(self)) {
	/* error status was set by open_ndmp_device */
	return FALSE;
    }

    if (mode != ACCESS_WRITE && dself->volume_label == NULL) {
	if (ndmp_device_read_label(dself) != DEVICE_STATUS_SUCCESS)
	    /* the error was set by ndmp_device_read_label */
	    return FALSE;
    }

    dself->access_mode = mode;
    dself->in_file = FALSE;

    if (!ndmp_mtio_rewind(self)) {
	/* ndmp_mtio_rewind already set our error message */
	return FALSE;
    }

    /* Position the tape */
    switch (mode) {
    case ACCESS_APPEND:
	/* TODO: append support */
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
	    return FALSE;
	}
	amfree(header);

	if (!robust_write(self, header_buf, dself->block_size)) {
	    /* error was set by robust_write */
	    amfree(header_buf);
	    return FALSE;
	}

	amfree(header_buf);
	if (!ndmp_mtio_eof(self)) {
	    /* error was set by ndmp_mtio_eof */
	    return FALSE;
	}

	dself->volume_label = newstralloc(dself->volume_label, label);
	dself->volume_time = newstralloc(dself->volume_time, timestamp);

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
    NdmpDevice *self = NDMP_DEVICE(dself);
    if (device_in_error(dself)) return FALSE;

    /* we're not in a file anymore */
    dself->access_mode = ACCESS_NULL;

    if (!close_ndmp_device(self)) {
	/* error is set by close_ndmp_device */
	return FALSE;
    }

    return TRUE;
}

/* functions for writing */

static gboolean
ndmp_device_start_file(
    Device     *dself,
    dumpfile_t *jobInfo)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    self = self;
    jobInfo = jobInfo;

    device_set_error(dself, g_strdup("operation not supported"), DEVICE_STATUS_DEVICE_ERROR);
    return FALSE;
}

static gboolean
ndmp_device_write_block(
    Device   *dself,
    guint     size,
    gpointer  data)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    self = self;
    size = size;
    data = data;

    device_set_error(dself, g_strdup("operation not supported"), DEVICE_STATUS_DEVICE_ERROR);
    return FALSE;
}

static gboolean
ndmp_device_finish_file(
    Device *dself)
{
    if (device_in_error(dself)) return FALSE;

    /* we're not in a file anymore */
    dself->in_file = FALSE;

    device_set_error(dself, g_strdup("operation not supported"), DEVICE_STATUS_DEVICE_ERROR);
    return FALSE;
}

/* functions for reading */

static dumpfile_t*
ndmp_device_seek_file(
    Device *dself,
    guint   file)
{
    NdmpDevice *self = NDMP_DEVICE(dself);

    self = self;
    file = file;

    device_set_error(dself, g_strdup("operation not supported"), DEVICE_STATUS_DEVICE_ERROR);
    return NULL;
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
    self = self;
    data = data;
    size_req = size_req;

    device_set_error(dself, g_strdup("operation not supported"), DEVICE_STATUS_DEVICE_ERROR);
    return -1;
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
ndmp_device_set_verbose_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    NdmpDevice *self = NDMP_DEVICE(p_self);

    self->verbose = g_value_get_boolean(val);

    /* if the connection is active, set up verbose logging or turn it off */
    if (self->conn) {
	if (self->verbose)
	    ndmconn_set_snoop(self->conn, &device_ndmlog, SNOOP_LEVEL);
	else
	    ndmconn_clear_snoop(self->conn);
    }


    return device_simple_property_set_fn(p_self, base, val, surety, source);
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

    device_class->start_file = ndmp_device_start_file;
    device_class->write_block = ndmp_device_write_block;
    device_class->finish_file = ndmp_device_finish_file;

    device_class->seek_file = ndmp_device_seek_file;
    device_class->seek_block = ndmp_device_seek_block;
    device_class->read_block = ndmp_device_read_block;

    g_object_class->finalize = ndmp_device_finalize;

    device_class_register_property(device_class, PROPERTY_NDMP_USERNAME,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    ndmp_device_set_username_fn);

    device_class_register_property(device_class, PROPERTY_NDMP_PASSWORD,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    ndmp_device_set_password_fn);

    device_class_register_property(device_class, PROPERTY_VERBOSE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_MASK,
	    device_simple_property_get_fn,
	    ndmp_device_set_verbose_fn);
}

static void
ndmp_device_init(NdmpDevice *self)
{
    Device *dself = DEVICE(self);
    GValue response;

    /* begin unconnected */
    self->conn = NULL;

    /* TODO: allow other block sizes */
    dself->block_size = 32768;
    dself->min_block_size = 32768;
    dself->max_block_size = 32768;

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

    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_READ_WRITE);
    device_set_simple_property(dself, PROPERTY_MEDIUM_ACCESS_TYPE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

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
    NdmpDevice * ndmp_rval;
    g_assert(0 == strcmp(device_type, NDMP_DEVICE_NAME));
    rval = DEVICE(g_object_new(TYPE_NDMP_DEVICE, NULL));
    ndmp_rval = (NdmpDevice *)rval;

    device_open_device(rval, device_name, device_type, device_node);
    return rval;
}

void
ndmp_device_register(void)
{
    static const char * device_prefix_list[] = { NDMP_DEVICE_NAME, NULL };

    /* set up logging */
    device_ndmlog.deliver = ndmp_device_ndmlog_deliver;
    device_ndmlog.cookie = NULL; /* unused */

    /* register the device itself */
    register_device(ndmp_device_factory, device_prefix_list);

    device_property_fill_and_register(&device_property_ndmp_username,
                                      G_TYPE_STRING, "ndmp_username",
       "Username for access to the NDMP agent");
    device_property_fill_and_register(&device_property_ndmp_password,
                                      G_TYPE_STRING, "ndmp_password",
       "Password for access to the NDMP agent");
}

