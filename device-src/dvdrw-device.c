/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009 University of Maryland at College Park
 * Copyright (c) 2007-2013 Zmanda, Inc.  All Rights Reserved.
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Author: Sam Couter <sam@couter.id.au>
 */

#include "amanda.h"
#include "vfs-device.h"

/*
 * Type checking and casting macros
 */
#define TYPE_DVDRW_DEVICE	(dvdrw_device_get_type())
#define DVDRW_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), TYPE_DVDRW_DEVICE, DvdRwDevice)
#define DVDRW_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), TYPE_DVDRW_DEVICE, DvdRwDevice const)
#define DVDRW_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), TYPE_DVDRW_DEVICE, DvdRwDeviceClass)
#define IS_DVDRW_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), TYPE_DVDRW_DEVICE)
#define DVDRW_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), TYPE_DVDRW_DEVICE, DvdRwDeviceClass)

/* Forward declaration */
static GType dvdrw_device_get_type(void);

/*
 * Main object structure
 */
typedef struct _DvdRwDevice DvdRwDevice;
struct _DvdRwDevice {
    VfsDevice __parent__;

    gchar *dvdrw_device;
    gchar *cache_dir;
    gchar *cache_data;
    gchar *mount_point;
    gchar *mount_data;
    gboolean mounted;
    gboolean keep_cache;
    gboolean unlabelled_when_unmountable;
    gchar *growisofs_command;
    gchar *mount_command;
    gchar *umount_command;
};

/*
 * Class definition
 */
typedef struct _DvdRwDeviceClass DvdRwDeviceClass;
struct _DvdRwDeviceClass {
    VfsDeviceClass __parent__;
};

/* Where the DVD-RW can be mounted */
static DevicePropertyBase device_property_dvdrw_mount_point;
#define PROPERTY_DVDRW_MOUNT_POINT (device_property_dvdrw_mount_point.ID)

/* Should the on-disk version be kept after the optical disc has been written? */
static DevicePropertyBase device_property_dvdrw_keep_cache;
#define PROPERTY_DVDRW_KEEP_CACHE (device_property_dvdrw_keep_cache.ID)

/* Should a mount failure (eg, a freshly formatted disc) when reading a label be treated like an unlabelled volume? */
static DevicePropertyBase device_property_dvdrw_unlabelled_when_unmountable;
#define PROPERTY_DVDRW_UNLABELLED_WHEN_UNMOUNTABLE (device_property_dvdrw_unlabelled_when_unmountable.ID)

/* Where to find the growisofs command */
static DevicePropertyBase device_property_dvdrw_growisofs_command;
#define PROPERTY_DVDRW_GROWISOFS_COMMAND (device_property_dvdrw_growisofs_command.ID)

/* Where to find the filesystem mount command */
static DevicePropertyBase device_property_dvdrw_mount_command;
#define PROPERTY_DVDRW_MOUNT_COMMAND (device_property_dvdrw_mount_command.ID)

/* Where to find the filesystem unmount command */
static DevicePropertyBase device_property_dvdrw_umount_command;
#define PROPERTY_DVDRW_UMOUNT_COMMAND (device_property_dvdrw_umount_command.ID)

/* GObject housekeeping */
void
dvdrw_device_register(void);

static Device*
dvdrw_device_factory(char *device_name, char *device_type, char *device_node);

static void
dvdrw_device_class_init (DvdRwDeviceClass *c);

static void
dvdrw_device_init (DvdRwDevice *self);

/* Properties */
static gboolean
dvdrw_device_set_mount_point_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean
dvdrw_device_set_keep_cache_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean
dvdrw_device_set_unlabelled_when_unmountable_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean
dvdrw_device_set_growisofs_command_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean
dvdrw_device_set_mount_command_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

static gboolean
dvdrw_device_set_umount_command_fn(Device *self,
    DevicePropertyBase *base, GValue *val,
    PropertySurety surety, PropertySource source);

/* Methods */
static void
dvdrw_device_open_device(Device *dself, char *device_name, char *device_type, char *device_node);

static DeviceStatusFlags
dvdrw_device_read_label(Device *dself);

static gboolean
dvdrw_device_start(Device *dself, DeviceAccessMode mode, char *label, char *timestamp);

static gboolean
dvdrw_device_finish(Device *dself);

static void
dvdrw_device_finalize(GObject *gself);

/* Helper functions */
static gboolean
check_access_mode(DvdRwDevice *self, DeviceAccessMode mode);

static gboolean
check_readable(DvdRwDevice *self);

static DeviceStatusFlags
mount_disc(DvdRwDevice *self, gboolean report_error);

static void
unmount_disc(DvdRwDevice *self);

static gboolean
burn_disc(DvdRwDevice *self);

static DeviceStatusFlags
execute_command(DvdRwDevice *self, gchar **argv, gint *status);

static GType
dvdrw_device_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (VfsDeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) dvdrw_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (VfsDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) dvdrw_device_init,
            NULL
        };

        type = g_type_register_static (TYPE_VFS_DEVICE, "DvdRwDevice",
                                       &info, (GTypeFlags)0);
    }

    return type;
}

void
dvdrw_device_register(void)
{
    const char *device_prefix_list[] = { "dvdrw", NULL };

    device_property_fill_and_register(&device_property_dvdrw_mount_point,
	G_TYPE_STRING, "dvdrw_mount_point",
	"Directory to mount DVD-RW for reading");

    device_property_fill_and_register(&device_property_dvdrw_keep_cache,
	G_TYPE_BOOLEAN, "dvdrw_keep_cache",
	"Keep on-disk cache after DVD-RW has been written");

    device_property_fill_and_register(&device_property_dvdrw_unlabelled_when_unmountable,
	G_TYPE_BOOLEAN, "dvdrw_unlabelled_when_unmountable",
	"Treat unmountable volumes as unlabelled when reading label");

    device_property_fill_and_register(&device_property_dvdrw_growisofs_command,
	G_TYPE_BOOLEAN, "dvdrw_growisofs_command",
	"The location of the growisofs command used to write the DVD-RW");

    device_property_fill_and_register(&device_property_dvdrw_mount_command,
	G_TYPE_BOOLEAN, "dvdrw_mount_command",
	"The location of the mount command used to mount the DVD-RW filesystem for reading");

    device_property_fill_and_register(&device_property_dvdrw_umount_command,
	G_TYPE_BOOLEAN, "dvdrw_umount_command",
	"The location of the umount command used to unmount the DVD-RW filesystem after reading");

    register_device(dvdrw_device_factory, device_prefix_list);
}

static Device *
dvdrw_device_factory(char *device_name, char *device_type, char *device_node)
{
    Device *device;

    g_assert(0 == strncmp(device_type, "dvdrw", strlen("dvdrw")));

    device = DEVICE(g_object_new(TYPE_DVDRW_DEVICE, NULL));
    device_open_device(device, device_name, device_type, device_node);

    return device;
}

static void
dvdrw_device_class_init (DvdRwDeviceClass *c)
{
    DeviceClass *device_class = DEVICE_CLASS(c);
    GObjectClass *g_object_class = G_OBJECT_CLASS(c);

    device_class->open_device = dvdrw_device_open_device;
    device_class->read_label = dvdrw_device_read_label;
    device_class->start = dvdrw_device_start;
    device_class->finish = dvdrw_device_finish;

    g_object_class->finalize = dvdrw_device_finalize;

    device_class_register_property(device_class, PROPERTY_DVDRW_MOUNT_POINT,
	PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	device_simple_property_get_fn,
	dvdrw_device_set_mount_point_fn);

    device_class_register_property(device_class, PROPERTY_DVDRW_KEEP_CACHE,
	PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	device_simple_property_get_fn,
	dvdrw_device_set_keep_cache_fn);

    device_class_register_property(device_class, PROPERTY_DVDRW_UNLABELLED_WHEN_UNMOUNTABLE,
	PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	device_simple_property_get_fn,
	dvdrw_device_set_unlabelled_when_unmountable_fn);

    device_class_register_property(device_class, PROPERTY_DVDRW_GROWISOFS_COMMAND,
	PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	device_simple_property_get_fn,
	dvdrw_device_set_growisofs_command_fn);

    device_class_register_property(device_class, PROPERTY_DVDRW_MOUNT_COMMAND,
	PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	device_simple_property_get_fn,
	dvdrw_device_set_mount_command_fn);

    device_class_register_property(device_class, PROPERTY_DVDRW_UMOUNT_COMMAND,
	PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	device_simple_property_get_fn,
	dvdrw_device_set_umount_command_fn);
}

static void
dvdrw_device_init (DvdRwDevice *self)
{
    Device *dself = DEVICE(self);
    GValue val;

    self->dvdrw_device = NULL;
    self->cache_dir = NULL;
    self->cache_data = NULL;
    self->mount_point = NULL;
    self->mount_data = NULL;
    self->mounted = FALSE;
    self->keep_cache = FALSE;
    self->growisofs_command = NULL;
    self->mount_command = NULL;
    self->umount_command = NULL;

    bzero(&val, sizeof(val));

    g_value_init(&val, G_TYPE_BOOLEAN);
    g_value_set_boolean(&val, FALSE);
    device_set_simple_property(dself, PROPERTY_APPENDABLE,
	&val, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&val);

    g_value_init(&val, G_TYPE_BOOLEAN);
    g_value_set_boolean(&val, FALSE);
    device_set_simple_property(dself, PROPERTY_PARTIAL_DELETION,
	&val, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&val);

    g_value_init(&val, G_TYPE_BOOLEAN);
    g_value_set_boolean(&val, FALSE);
    device_set_simple_property(dself, PROPERTY_FULL_DELETION,
	&val, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&val);

    g_value_init(&val, G_TYPE_BOOLEAN);
    g_value_set_boolean(&val, TRUE);
    device_set_simple_property(dself, PROPERTY_LEOM,
	&val, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&val);
}

static gboolean
dvdrw_device_set_mount_point_fn(Device *dself, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);

    amfree(self->mount_point);
    amfree(self->mount_data);

    self->mount_point = g_value_dup_string(val);
    self->mount_data = g_strconcat(self->mount_point, "/data/", NULL);

    device_clear_volume_details(dself);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
dvdrw_device_set_keep_cache_fn(Device *dself, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);

    self->keep_cache = g_value_get_boolean(val);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
dvdrw_device_set_unlabelled_when_unmountable_fn(Device *dself, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);

    self->unlabelled_when_unmountable = g_value_get_boolean(val);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
dvdrw_device_set_growisofs_command_fn(Device *dself, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);

    self->growisofs_command = g_value_dup_string(val);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
dvdrw_device_set_mount_command_fn(Device *dself, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);

    self->mount_command = g_value_dup_string(val);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static gboolean
dvdrw_device_set_umount_command_fn(Device *dself, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);

    self->umount_command = g_value_dup_string(val);

    return device_simple_property_set_fn(dself, base, val, surety, source);
}

static void
dvdrw_device_open_device(Device *dself, char *device_name, char *device_type, char *device_node)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);
    DeviceClass *parent_class = DEVICE_CLASS(g_type_class_peek_parent(DVDRW_DEVICE_GET_CLASS(dself)));
    GValue val;
    char *colon;

    g_debug("Opening device: %s", device_node);

    bzero(&val, sizeof(val));

    colon = index(device_node, ':');
    if (!colon) {
	device_set_error(dself,
	    stralloc(_("DVDRW device requires cache directory and DVD-RW device separated by a colon (:) in tapedev")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return;
    }

    self->cache_dir = g_strndup(device_node, colon - device_node);
    self->cache_data = g_strconcat(self->cache_dir, "/data/", NULL);
    self->dvdrw_device = g_strdup(colon + 1);

    parent_class->open_device(dself, device_name, device_type, device_node);
}

static DeviceStatusFlags
dvdrw_device_read_label(Device *dself)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);
    VfsDevice *vself = VFS_DEVICE(dself);
    gboolean mounted = FALSE;
    DeviceStatusFlags status;
    struct stat dir_status;
    DeviceClass *parent_class = DEVICE_CLASS(g_type_class_peek_parent(DVDRW_DEVICE_GET_CLASS(dself)));

    g_debug("Reading label from media at %s", self->mount_point);

    if (device_in_error(dself)) return DEVICE_STATUS_DEVICE_ERROR;
    if (!check_readable(self)) return DEVICE_STATUS_DEVICE_ERROR;

    if (!self->mounted) {
	status = mount_disc(self, !self->unlabelled_when_unmountable);
	if (status != DEVICE_STATUS_SUCCESS) {
	    /* Not mountable. May be freshly formatted or corrupted, drive may be empty. */
	    return (self->unlabelled_when_unmountable)
		? DEVICE_STATUS_VOLUME_UNLABELED
		: status;
	}
	mounted = TRUE;
    }

    if ((stat(self->mount_data, &dir_status) < 0) && (errno == ENOENT)) {
	/* No data directory, consider the DVD unlabelled */
	g_debug("Media contains no data directory and therefore no label");
	unmount_disc(self);

	return DEVICE_STATUS_VOLUME_UNLABELED;
    }

    amfree(vself->dir_name);
    vself->dir_name = g_strdup(self->mount_data);
    status = parent_class->read_label(dself);

    if (mounted) {
	unmount_disc(self);
    }

    return status;
}

static gboolean
dvdrw_device_start(Device *dself, DeviceAccessMode mode, char *label, char *timestamp)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);
    VfsDevice *vself = VFS_DEVICE(dself);
    DeviceClass *parent_class = DEVICE_CLASS(g_type_class_peek_parent(DVDRW_DEVICE_GET_CLASS(dself)));

    g_debug("Start DVDRW device");

    if (device_in_error(dself)) return FALSE;
    if (!check_access_mode(self, mode)) return FALSE;

    dself->access_mode = mode;

    /* We'll replace this with our own value */
    amfree(vself->dir_name);

    if (mode == ACCESS_READ) {
	if (mount_disc(self, TRUE) != DEVICE_STATUS_SUCCESS) {
	    return FALSE;
	}

	vself->dir_name = g_strdup(self->mount_data);
    } else if (mode == ACCESS_WRITE) {
	vself->dir_name = g_strdup(self->cache_data);
    }

    return parent_class->start(dself, mode, label, timestamp);
}

static gboolean
dvdrw_device_finish(Device *dself)
{
    DvdRwDevice *self = DVDRW_DEVICE(dself);
    VfsDevice *vself = VFS_DEVICE(dself);
    gboolean result;
    DeviceClass *parent_class = DEVICE_CLASS(g_type_class_peek_parent(DVDRW_DEVICE_GET_CLASS(dself)));
    DeviceAccessMode mode;

    g_debug("Finish DVDRW device");

    /* Save access mode before parent class messes with it */
    mode = dself->access_mode;

    result = parent_class->finish(dself);

    if (mode == ACCESS_READ) {
	unmount_disc(self);
    }

    if (!result || device_in_error(dself)) {
	return FALSE;
    }

    if (mode == ACCESS_WRITE) {
	result = burn_disc(self);

	if (result && !self->keep_cache) {
	    delete_vfs_files(vself);
	}

	return result;
    }

    return TRUE;
}

static gboolean
burn_disc(DvdRwDevice *self)
{
    gint status;

    char *burn_argv[] = {NULL, "-use-the-force-luke",
	"-Z", self->dvdrw_device,
	"-J", "-R", "-pad", "-quiet",
	self->cache_dir, NULL};

    if (self->growisofs_command == NULL) {
	burn_argv[0] = "growisofs";
    } else {
	burn_argv[0] = self->growisofs_command;
    }

    g_debug("Burning media in %s", self->dvdrw_device);
    if (execute_command(self, burn_argv, &status) != DEVICE_STATUS_SUCCESS) {
	return FALSE;
    }
    g_debug("Burn completed successfully");

    return TRUE;
}

static void
dvdrw_device_finalize(GObject *gself)
{
    DvdRwDevice *self = DVDRW_DEVICE(gself);
    GObjectClass *parent_class = G_OBJECT_CLASS(g_type_class_peek_parent(DVDRW_DEVICE_GET_CLASS(gself)));

    if (parent_class->finalize) {
	parent_class->finalize(gself);
    }

    amfree(self->dvdrw_device);

    amfree(self->cache_dir);
    amfree(self->cache_data);
    amfree(self->mount_point);
    amfree(self->mount_data);
    amfree(self->growisofs_command);
    amfree(self->mount_command);
    amfree(self->umount_command);
}

static gboolean
check_access_mode(DvdRwDevice *self, DeviceAccessMode mode)
{
    Device *dself = DEVICE(self);

    if (mode == ACCESS_READ) {
	return check_readable(self);
    } else if (mode == ACCESS_WRITE) {
	return TRUE;
    }

    device_set_error(dself,
	stralloc(_("DVDRW device can only be opened in READ or WRITE mode")),
	DEVICE_STATUS_DEVICE_ERROR);

    return FALSE;
}

static gboolean
check_readable(DvdRwDevice *self)
{
    Device *dself = DEVICE(self);
    GValue value;
    bzero(&value, sizeof(value));

    if (! device_get_simple_property(dself, PROPERTY_DVDRW_MOUNT_POINT, &value, NULL, NULL)) {
	device_set_error(dself,
	    stralloc(_("DVDRW device requires DVDRW_MOUNT_POINT to open device for reading")),
	    DEVICE_STATUS_DEVICE_ERROR);

	return FALSE;
    }

    return TRUE;
}

static DeviceStatusFlags
mount_disc(DvdRwDevice *self, gboolean report_error)
{
    Device *dself = DEVICE(self);
    gchar *mount_argv[] = { NULL, self->mount_point, NULL };
    DeviceStatusFlags status;

    if (self->mounted) {
	return DEVICE_STATUS_SUCCESS;
    }

    if (self->mount_command == NULL) {
	mount_argv[0] = "mount";
    } else {
	mount_argv[0] = self->mount_command;
    }

    g_debug("Mounting media at %s", self->mount_point);
    status = execute_command(report_error ? self : NULL, mount_argv, NULL);
    if (status != DEVICE_STATUS_SUCCESS) {
	/* Wait a few seconds and try again - The tray may still be out after burning */
	sleep(3);
	if (execute_command(report_error ? self : NULL, mount_argv, NULL) == DEVICE_STATUS_SUCCESS) {
	    /* Clear error */
	    device_set_error(dself, NULL, DEVICE_STATUS_SUCCESS);
	    self->mounted = TRUE;
	    return DEVICE_STATUS_SUCCESS;
	} else {
	    return status;
	}
    }

    self->mounted = TRUE;
    return DEVICE_STATUS_SUCCESS;
}

static void
unmount_disc(DvdRwDevice *self)
{
    gchar *unmount_argv[] = { NULL, self->mount_point, NULL };
    DeviceStatusFlags status;

    if (! self->mounted) {
	return;
    }

    if (self->umount_command == NULL) {
	unmount_argv[0] = "umount";
    } else {
	unmount_argv[0] = self->umount_command;
    }

    g_debug("Unmounting media at %s", self->mount_point);
    status = execute_command(NULL, unmount_argv, NULL);
    if (status == DEVICE_STATUS_SUCCESS) {
	self->mounted = FALSE;
    }
}

static DeviceStatusFlags
execute_command(DvdRwDevice *self, gchar **argv, gint *result)
{
    Device *dself = DEVICE(self);
    gchar *std_output = NULL;
    gchar *std_error = NULL;
    gint errnum = 0;
    GError *error = NULL;
    gboolean success;

    /* g_debug("Executing: %s", argv[0]); */

    g_spawn_sync(NULL, argv, NULL, G_SPAWN_SEARCH_PATH, NULL, NULL,
	&std_output, &std_error, &errnum, &error);

    /* g_debug("Execution complete"); */

    if (WIFSIGNALED(errnum)) {
	success = FALSE;
    } else if (WIFEXITED(errnum)) {
	success = (WEXITSTATUS(errnum) == 0);
    } else {
	success = FALSE;
    }

    if (!success) {
	gchar *error_message = vstrallocf(_("DVDRW device cannot execute '%s': %s (status: %d) (stderr: %s)"),
	    argv[0], error ? error->message : _("Unknown error"), errnum, std_error ? std_error: "No stderr");

	if (dself != NULL) {
	    device_set_error(dself, error_message, DEVICE_STATUS_DEVICE_ERROR);
	}

	if (std_output) {
	    g_free(std_output);
	}

	if (std_error) {
	    g_free(std_error);
	}

	if (error) {
	    g_error_free(error);
	}

	if (result != NULL) {
	    *result = errnum;
	}

	return DEVICE_STATUS_DEVICE_ERROR;
    }

    return DEVICE_STATUS_SUCCESS;
}
