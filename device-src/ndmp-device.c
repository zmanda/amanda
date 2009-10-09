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

/* An Ndmp device uses Amazon's Ndmp service (http://www.amazon.com/ndmp) to store
 * data.  It stores data in keys named with a user-specified prefix, inside a
 * user-specified bucket.  Data is stored in the form of numbered (large)
 * blocks.
 */

#include "amanda.h"
#include "util.h"
#include "device.h"
#include "ndmp-proxy.h"

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

    ipc_binary_channel_t *proxy_chan;
    int proxy_sock; /* -1 if not connected */

    /* constructor parameters and properties */
    gchar	 *ndmp_hostname;
    gint	 ndmp_port;
    gchar        *ndmp_device_name;
    gchar	 *ndmp_username;
    gchar	 *ndmp_password;
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

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

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

/*
 * utility functions */

static gboolean try_open_ndmp_device(NdmpDevice *nself);
static gboolean close_ndmp_device(NdmpDevice *nself);
static gboolean get_generic_reply(NdmpDevice *nself);
static void set_proxy_comm_err(Device *dself, int saved_errno);

static int ndmp_mtio_eod(NdmpDevice *nself);
static int ndmp_mtio_eof(NdmpDevice *nself);
static int ndmp_mtio_rewind(NdmpDevice *nself);
static int ndmp_mtio(NdmpDevice *nself, char *cmd, int count);
static int ndmp_device_robust_write(NdmpDevice *nself, char *buf, int count);

/*
 * class mechanics */

static void
ndmp_device_init(NdmpDevice * o);

static void
ndmp_device_class_init(NdmpDeviceClass *c);

static void
ndmp_device_finalize(GObject *o);

static Device*
ndmp_device_factory(char *device_name, char *device_type, char *device_node);

static gboolean ndmp_device_set_username_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean ndmp_device_set_password_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

/*
 * virtual functions */

static void
ndmp_device_open_device(Device *dself, char *device_name,
		  char * device_type, char * device_node);

static DeviceStatusFlags ndmp_device_read_label(Device *dself);

static gboolean
ndmp_device_start(Device *dself,
                DeviceAccessMode mode,
                char * label,
                char * timestamp);

static gboolean
ndmp_device_finish(Device *dself);

static gboolean
ndmp_device_start_file(Device *dself,
                     dumpfile_t * jobInfo);

static gboolean
ndmp_device_write_block(Device *dself,
                      guint size,
                      gpointer data);

static gboolean
ndmp_device_finish_file(Device *dself);

static dumpfile_t*
ndmp_device_seek_file(Device *dself,
                    guint file);

static gboolean
ndmp_device_seek_block(Device *dself,
                     guint64 block);

static int
ndmp_device_read_block(Device * dself,
                     gpointer data,
                     int *size_req);

/*
 * Class mechanics
 */

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

static void
ndmp_device_init(NdmpDevice *nself)
{
    Device *dself = DEVICE(nself);
    GValue response;

    nself->proxy_chan = NULL;
    nself->proxy_sock = -1;

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

}

static gboolean
ndmp_device_set_username_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    NdmpDevice *nself = NDMP_DEVICE(self);

    amfree(nself->ndmp_username);
    nself->ndmp_username = g_value_dup_string(val);
    device_clear_volume_details(self);

    return device_simple_property_set_fn(self, base, val, surety, source);
}

static gboolean
ndmp_device_set_password_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source)
{
    NdmpDevice *nself = NDMP_DEVICE(self);

    amfree(nself->ndmp_password);
    nself->ndmp_password = g_value_dup_string(val);
    device_clear_volume_details(self);

    return device_simple_property_set_fn(self, base, val, surety, source);
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
    NdmpDevice          *nself = NDMP_DEVICE(dself);
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

    nself->ndmp_hostname = g_strndup(device_node, colon-device_node);
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
	nself->ndmp_port = (gint)port;
    } else {
	nself->ndmp_port = 0; /* (use ndmjob's default, 10000) */
    }
    nself->ndmp_device_name = g_strdup(at+1);

    /* these should be changed by properties */
    nself->ndmp_username = g_strdup("ndmp");
    nself->ndmp_password = g_strdup("ndmp");

    if (parent_class->open_device) {
        parent_class->open_device(dself, device_name, device_type, device_node);
    }
}

static void ndmp_device_finalize(GObject * obj_self)
{
    NdmpDevice       *nself = NDMP_DEVICE (obj_self);

    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    if (nself->proxy_chan)
	ipc_binary_free_channel(nself->proxy_chan);
    if (nself->proxy_sock != -1)
	robust_close(nself->proxy_sock);

    if (nself->ndmp_hostname)
	g_free(nself->ndmp_hostname);
    if (nself->ndmp_device_name)
	g_free(nself->ndmp_device_name);
    if (nself->ndmp_username)
	g_free(nself->ndmp_username);
    if (nself->ndmp_password)
	g_free(nself->ndmp_password);
}

static DeviceStatusFlags
ndmp_device_read_label(
    Device *dself)
{
    NdmpDevice       *nself = NDMP_DEVICE(dself);
    dumpfile_t       *header;
    ipc_binary_message_t *msg;
    char *errcode, *errstr;

    amfree(dself->volume_label);
    amfree(dself->volume_time);
    amfree(dself->volume_header);

    if (device_in_error(nself)) return dself->status;

    header = dself->volume_header = g_new(dumpfile_t, 1);
    fh_init(header);

    if (!try_open_ndmp_device(nself)) {
	/* error status was set by try_open_ndmp_device */
	return dself->status;
    }

    if (!ndmp_mtio_rewind(nself)) {
	/* error message, if any, is set by ndmp_mtio_rewind */
	return dself->status;
    }

    msg = ipc_binary_new_message(nself->proxy_chan, NDMP_PROXY_CMD_TAPE_READ);
    ipc_binary_add_arg(msg, NDMP_PROXY_COUNT, 0, "32768", 0); /* TODO: use variable block size */
    if (ipc_binary_write_message(nself->proxy_chan, nself->proxy_sock, msg) < 0) {
	set_proxy_comm_err(dself, errno);
	return dself->status;
    }

    g_debug("Sent NDMP_PROXY_CMD_TAPE_READ to ndmp-proxy");
    if (!(msg = ipc_binary_read_message(nself->proxy_chan, nself->proxy_sock))) {
	if (errno) {
	    set_proxy_comm_err(dself, errno);
	} else {
	    device_set_error(dself,
		    vstrallocf(_("EOF from ndmp-proxy")),
		    DEVICE_STATUS_DEVICE_ERROR);
	}

	return dself->status;
    }

    errcode = (char *)msg->args[NDMP_PROXY_ERRCODE].data;
    errstr = (char *)msg->args[NDMP_PROXY_ERROR].data;
    if (errcode) {
	/* EOF gets translated to an unlabeled volume */
	if (0 == strcmp(errcode, "NDMP9_EOF_ERR")) {
	    device_set_error(dself,
		    g_strdup(_("unlabeled volume")), DEVICE_STATUS_VOLUME_UNLABELED);
	} else {
	    device_set_error(dself,
		    g_strdup_printf(_("Error reading label: %s"), errstr),
		    DEVICE_STATUS_DEVICE_ERROR);
		    /* TODO: could this also be a volume error? */
	}
	ipc_binary_free_message(msg);
	return dself->status;
    }

    parse_file_header(msg->args[NDMP_PROXY_DATA].data, header, msg->args[NDMP_PROXY_DATA].len);
    ipc_binary_free_message(msg);

    if (header->type != F_TAPESTART) {
	device_set_error(dself,
		stralloc(_("No tapestart header -- unlabeled device?")),
			 DEVICE_STATUS_VOLUME_UNLABELED);
	return dself->status;
    }
    dself->volume_label = g_strdup(header->name);
    dself->volume_time = g_strdup(header->datestamp);
    /* dself->volume_header is already set */

    /* close the connection now, so as not to reserve the tape device longer than
     * necessary; TODO: be less aggressive about this */
    if (!close_ndmp_device(nself))
	/* error is set by close_ndmp_device */
	return FALSE;

    device_set_error(dself, NULL, DEVICE_STATUS_SUCCESS);

    return dself->status;
}

/* Just a helper function for ndmp_device_start(). */
static gboolean
write_tapestart_header(
    NdmpDevice *nself,
    char       *label,
    char       *timestamp)
{
    int         result;
    Device     *dself = (Device*)nself;
    dumpfile_t *header;
    char       *header_buf;

    if (!ndmp_mtio_rewind(nself)) {
	/* error message, if any, is set by ndmp_mtio_rewind */
	return FALSE;
    }

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

    result = ndmp_device_robust_write(nself, header_buf, dself->block_size);
    if (!result) {
	/* error was set by ndmp_device_robust_write */
	amfree(header_buf);
	return FALSE;
    }

    amfree(header_buf);
    if (!ndmp_mtio_eof(nself)) {
	/* error was set by ndmp_mtio_eof */
	return FALSE;
    }

    return TRUE;
}


static gboolean
ndmp_device_start(
    Device           *dself,
    DeviceAccessMode  mode,
    char             *label,
    char             *timestamp)
{
    NdmpDevice *nself = NDMP_DEVICE(dself);

    nself = NDMP_DEVICE(dself);

    if (device_in_error(nself)) return FALSE;

    if (!try_open_ndmp_device(nself)) {
	/* error status was set by try_open_ndmp_device */
	return FALSE;
    }

    if (mode != ACCESS_WRITE && dself->volume_label == NULL) {
	if (ndmp_device_read_label(dself) != DEVICE_STATUS_SUCCESS)
	    /* the error was set by ndmp_device_read_label */
	    return FALSE;
    }

    dself->access_mode = mode;
    dself->in_file = FALSE;

    if (!ndmp_mtio_rewind(nself)) {
	/* ndmp_mtio_rewind already set our error message */
	return FALSE;
    }

    /* Position the tape */
    switch (mode) {
    case ACCESS_APPEND:
	if (!ndmp_mtio_eod(nself)) {
	    /* ndmp_mtio_eod already set our error message */
	    return FALSE;
	}
	break;

    case ACCESS_READ:
	dself->file = 0;
	break;

    case ACCESS_WRITE:
	if (!write_tapestart_header(nself, label, timestamp)) {
	    /* write_tapestart_header already set the error status */
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
    NdmpDevice *nself = NDMP_DEVICE(dself);
    if (device_in_error(dself)) return FALSE;

    /* we're not in a file anymore */
    dself->access_mode = ACCESS_NULL;

    if (!close_ndmp_device(nself))
	/* error is set by close_ndmp_device */
	return FALSE;

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
    NdmpDevice *nself = NDMP_DEVICE(dself);

    nself = nself;
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
    NdmpDevice *nself = NDMP_DEVICE(dself);

    nself = nself;
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
    NdmpDevice *nself = NDMP_DEVICE(dself);
    nself = nself;
    data = data;
    size_req = size_req;

    device_set_error(dself, g_strdup("operation not supported"), DEVICE_STATUS_DEVICE_ERROR);
    return -1;
}

/*
 * Utility functions
 */

static gboolean
try_open_ndmp_device(
    NdmpDevice *nself)
{
    Device              *dself = DEVICE(nself);
    ipc_binary_message_t *msg;
    char *errmsg = NULL;

    /* if already open, stop now */
    if (nself->proxy_sock != -1)
	return TRUE;

    /* now try to connect to the proxy */
    nself->proxy_sock = connect_to_ndmp_proxy(&errmsg);
    if (nself->proxy_sock <= 0) {
	device_set_error(dself, errmsg, DEVICE_STATUS_DEVICE_ERROR);
	goto error;
    }

    nself->proxy_chan = ipc_binary_new_channel(get_ndmp_proxy_proto());

    /* select the DEVICE service from the proxy */
    msg = ipc_binary_new_message(nself->proxy_chan, NDMP_PROXY_CMD_SELECT_SERVICE);
    ipc_binary_add_arg(msg, NDMP_PROXY_SERVICE, 0, "DEVICE", 0);
    if (ipc_binary_write_message(nself->proxy_chan, nself->proxy_sock, msg) < 0) {
	set_proxy_comm_err(dself, errno);
	goto error;
    }
    g_debug("Sent NDMP_PROXY_SELECT_SERVICE to ndmp-proxy");

    if (!get_generic_reply(nself)) {
	/* error message is set by get_generic_reply */
	goto error;
    }

    /* now send a NDMP_PROXY_TAPE_OPEN */
    msg = ipc_binary_new_message(nself->proxy_chan, NDMP_PROXY_CMD_TAPE_OPEN);
    /* TODO: support read-only and write-only */
    ipc_binary_add_arg(msg, NDMP_PROXY_MODE, 0, "RDRW", 0);
    ipc_binary_add_arg(msg, NDMP_PROXY_HOST, 0, nself->ndmp_hostname, 0);
    ipc_binary_add_arg(msg, NDMP_PROXY_PORT, 0,
	    g_strdup_printf("%d", nself->ndmp_port), 1);
    ipc_binary_add_arg(msg, NDMP_PROXY_FILENAME, 0, nself->ndmp_device_name, 0);
    ipc_binary_add_arg(msg, NDMP_PROXY_USERNAME, 0, nself->ndmp_username, 0);
    ipc_binary_add_arg(msg, NDMP_PROXY_PASSWORD, 0, nself->ndmp_password, 0);
    if (ipc_binary_write_message(nself->proxy_chan, nself->proxy_sock, msg) < 0) {
	set_proxy_comm_err(dself, errno);
	goto error;
    }
    g_debug("Sent NDMP_PROXY_TAPE_OPEN to ndmp-proxy");

    if (!get_generic_reply(nself)) {
	/* error message is set by get_generic_reply */
	goto error;
    }
    g_debug("get reply from ndmp-proxy");

    return TRUE;

error:
    if (nself->proxy_chan) {
	ipc_binary_free_channel(nself->proxy_chan);
	nself->proxy_chan = NULL;
    }
    if (nself->proxy_sock != -1) {
	close(nself->proxy_sock);
	nself->proxy_sock = -1;
    }

    return FALSE;
}

static gboolean
close_ndmp_device(NdmpDevice *nself) {
    ipc_binary_message_t *msg;
    gboolean success = TRUE;

    if (nself->proxy_sock == -1 || !nself->proxy_chan)
	return TRUE;

    g_debug("closing tape service");

    /* select the DEVICE service from the proxy */
    msg = ipc_binary_new_message(nself->proxy_chan, NDMP_PROXY_CMD_TAPE_CLOSE);
    if (ipc_binary_write_message(nself->proxy_chan, nself->proxy_sock, msg) < 0) {
	set_proxy_comm_err(DEVICE(nself), errno);
	return FALSE;
    }
    g_debug("Sent NDMP_PROXY_CMD_TAPE_CLOSE to ndmp-proxy");

    if (!get_generic_reply(nself)) {
	/* error message is set by get_generic_reply */
	success = FALSE;
    }

    close(nself->proxy_sock);
    nself->proxy_sock = -1;

    ipc_binary_free_channel(nself->proxy_chan);
    nself->proxy_chan = NULL;

    return success;
}

static void
set_proxy_comm_err(
	Device *dself,
	int saved_errno) {
    device_set_error(dself,
	g_strdup_printf(_("failed to communicate with ndmp-proxy: %s"), strerror(saved_errno)),
	DEVICE_STATUS_DEVICE_ERROR);
}

static gboolean
get_generic_reply(
	NdmpDevice *nself) {
    char *errcode = NULL, *errstr = NULL;
    ipc_binary_message_t *msg;
    Device *dself = DEVICE(nself);

    if (!(msg = ipc_binary_read_message(nself->proxy_chan, nself->proxy_sock))) {
	if (errno) {
	    set_proxy_comm_err(dself, errno);
	} else {
	    device_set_error(dself,
		    vstrallocf(_("EOF from ndmp-proxy")),
		    DEVICE_STATUS_DEVICE_ERROR);
	}

	return FALSE;
    }

    if (msg->cmd_id != NDMP_PROXY_REPLY_GENERIC) {
	device_set_error(dself,
		vstrallocf(_("incorrect generic reply from ndmp-proxy")),
		DEVICE_STATUS_DEVICE_ERROR);
    }

    errcode = (char *)msg->args[NDMP_PROXY_ERRCODE].data;
    errstr = (char *)msg->args[NDMP_PROXY_ERROR].data;
    g_debug("got NDMP_PROXY_REPLY_GENERIC, errcode=%s errstr=%s",
	errcode?errcode:"(null)", errstr?errstr:"(null)");
    if (errcode || errstr) {
	device_set_error(dself,
		g_strdup_printf("Error from ndmp-proxy: %s (%s)", errstr, errcode),
		DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    return TRUE;
}

static int
ndmp_mtio_eod(
    NdmpDevice *nself)
{
    return ndmp_mtio(nself, "EOD", 1);
}

static int
ndmp_mtio_eof(
    NdmpDevice *nself)
{
    return ndmp_mtio(nself, "EOF", 1);
}

static int
ndmp_mtio_rewind(
    NdmpDevice *nself)
{
    return ndmp_mtio(nself, "REWIND", 1);
}

static int
ndmp_mtio(
    NdmpDevice *nself,
    char       *command,
    int         count)
{
    Device           *dself = DEVICE(nself);
    ipc_binary_message_t *msg;

    msg = ipc_binary_new_message(nself->proxy_chan, NDMP_PROXY_CMD_TAPE_MTIO);
    ipc_binary_add_arg(msg, NDMP_PROXY_COMMAND, 0, command, FALSE);
    ipc_binary_add_arg(msg, NDMP_PROXY_COUNT, 0, g_strdup_printf("%d", count), TRUE);
    if (ipc_binary_write_message(nself->proxy_chan, nself->proxy_sock, msg) < 0) {
	set_proxy_comm_err(dself, errno);
	return FALSE;
    }

    if (!get_generic_reply(nself)) {
	/* error message is set by get_generic_reply */
	return FALSE;
    }

    return TRUE;
}

static int
ndmp_device_robust_write(
    NdmpDevice  *nself,
    char        *buf,
    int          count)
{
    Device *dself = (Device*)nself;
    ipc_binary_message_t *msg;

    msg = ipc_binary_new_message(nself->proxy_chan, NDMP_PROXY_CMD_TAPE_WRITE);
    ipc_binary_add_arg(msg, NDMP_PROXY_DATA, count, buf, 0);
    if (ipc_binary_write_message(nself->proxy_chan, nself->proxy_sock, msg) < 0) {
	set_proxy_comm_err(dself, errno);
	return FALSE;
    }
    g_debug("Sent CMD_TAPE_WRITE to ndmp-proxy");

    if (!get_generic_reply(nself)) {
	/* error message is set by get_generic_reply */
	return FALSE;
    }

    return TRUE;
}
