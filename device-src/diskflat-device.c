/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
#define TYPE_DISKFLAT_DEVICE	(diskflat_device_get_type())
#define DISKFLAT_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), TYPE_DISKFLAT_DEVICE, DiskflatDevice)
#define DISKFLAT_DEVICE_CONST(obj)	G_TYPE_CHECK_INSTANCE_CAST((obj), TYPE_DISKFLAT_DEVICE, DiskflatDevice const)
#define DISKFLAT_DEVICE_CLASS(klass)	G_TYPE_CHECK_CLASS_CAST((klass), TYPE_DISKFLAT_DEVICE, DiskflatDeviceClass)
#define IS_DISKFLAT_DEVICE(obj)	G_TYPE_CHECK_INSTANCE_TYPE((obj), TYPE_DISKFLAT_DEVICE)
#define DISKFLAT_DEVICE_GET_CLASS(obj)	G_TYPE_INSTANCE_GET_CLASS((obj), TYPE_DISKFLAT_DEVICE, DiskflatDeviceClass)

/* Forward declaration */
static GType diskflat_device_get_type(void);

/*
 * Main object structure
 */
typedef struct _DiskflatDevice DiskflatDevice;
struct _DiskflatDevice {
    VfsDevice __parent__;

    char *filename;
};

/*
 * Class definition
 */
typedef struct _DiskflatDeviceClass DiskflatDeviceClass;
struct _DiskflatDeviceClass {
    VfsDeviceClass __parent__;
};


/* GObject housekeeping */
void
diskflat_device_register(void);

static Device*
diskflat_device_factory(char *device_name, char *device_type, char *device_node);

static void
diskflat_device_class_init (DiskflatDeviceClass *c);

static void
diskflat_device_init (DiskflatDevice *self);

/* Methods */
static void
diskflat_device_open_device(Device *dself, char *device_name, char *device_type, char *device_node);

static dumpfile_t *
diskflat_device_seek_file (Device * dself, guint requested_file);

static gboolean
diskflat_device_seek_block (Device * dself, guint64 block);

static gboolean
diskflat_device_erase (Device * dself);

static gboolean
diskflat_device_finish(Device *dself);

static void
diskflat_device_finalize(GObject *gself);

static gboolean
diskflat_device_start_file_open(Device *dself, dumpfile_t *ji);

static void
diskflat_update_volume_size(Device *dself);

static void
diskflat_release_file(Device *dself);

static gboolean
diskflat_clear_and_prepare_label(Device *dself, char *label, char *timestamp);

static gboolean
diskflat_validate(Device *dself);

static GType
diskflat_device_get_type (void)
{
    static GType type = 0;

    if (G_UNLIKELY(type == 0)) {
        static const GTypeInfo info = {
            sizeof (DiskflatDeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) diskflat_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (DiskflatDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) diskflat_device_init,
            NULL
        };

        type = g_type_register_static (TYPE_VFS_DEVICE, "DiskflatDevice",
                                       &info, (GTypeFlags)0);
    }

    return type;
}

void
diskflat_device_register(void)
{
    const char *device_prefix_list[] = { "diskflat", NULL };

    register_device(diskflat_device_factory, device_prefix_list);
}

static Device *
diskflat_device_factory(
    char *device_name,
    char *device_type,
    char *device_node)
{
    Device *device;

    g_assert(g_str_has_prefix(device_type, "diskflat"));

    device = DEVICE(g_object_new(TYPE_DISKFLAT_DEVICE, NULL));
    device_open_device(device, device_name, device_type, device_node);

    return device;
}

static void
diskflat_device_class_init (
    DiskflatDeviceClass *c)
{
    DeviceClass *device_class = DEVICE_CLASS(c);
    GObjectClass *g_object_class = G_OBJECT_CLASS(c);

    device_class->open_device = diskflat_device_open_device;
    device_class->seek_file = diskflat_device_seek_file;
    device_class->seek_block = diskflat_device_seek_block;
    device_class->erase = diskflat_device_erase;
    device_class->finish = diskflat_device_finish;

    g_object_class->finalize = diskflat_device_finalize;
}

static void
diskflat_device_init (
    DiskflatDevice *self)
{
    Device *dself = DEVICE(self);
    VfsDevice *vself = VFS_DEVICE(self);
    GValue val;

    vself->device_start_file_open = &diskflat_device_start_file_open;
    vself->update_volume_size = &diskflat_update_volume_size;
    vself->release_file = &diskflat_release_file;
    vself->clear_and_prepare_label = &diskflat_clear_and_prepare_label;
    vself->validate = &diskflat_validate;

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


static void
diskflat_device_open_device(
    Device *dself,
    char *device_name,
    char *device_type,
    char *device_node)
{
    DiskflatDevice *self = DISKFLAT_DEVICE(dself);
    VfsDevice *vself = VFS_DEVICE(dself);
    char *d;
    DeviceClass *parent_class = DEVICE_CLASS(g_type_class_peek_parent(DISKFLAT_DEVICE_GET_CLASS(dself)));

    self->filename = g_strdup(device_node);
    g_debug("device_node: %s", self->filename);

    parent_class->open_device(dself, device_name, device_type, device_node);

    /* retrieve the directory */
    if ((d = rindex(vself->dir_name, '/')) != NULL) {
	*d = '\0';
	if ((d = rindex(vself->dir_name, '/')) != NULL) {
	    *d = '\0';
	}
    }
}

static gboolean
diskflat_device_start_file_open(
    Device     *dself,
    dumpfile_t *ji G_GNUC_UNUSED)
{
    if (dself->file >= 1) {
        device_set_error(dself,
            g_strdup_printf(_("Can't write more than one file to the diskflat device")),
            DEVICE_STATUS_VOLUME_ERROR);
	return FALSE;
    }

    dself->file++;
    return TRUE;
}

static dumpfile_t *
diskflat_device_seek_file(
    Device *dself,
    guint requested_file)
{
    VfsDevice     *vself = VFS_DEVICE(dself);
    DiskflatDevice *self  = DISKFLAT_DEVICE(dself);
    dumpfile_t *rval;
    char header_buffer[VFS_DEVICE_LABEL_SIZE];
    int header_buffer_size = sizeof(header_buffer);
    IoResult result;
    off_t result_seek;

    if (device_in_error(dself)) return NULL;
    if (requested_file > 1) {
        device_set_error(dself,
            g_strdup_printf(_("Can't seek to file number above 1")),
            DEVICE_STATUS_VOLUME_ERROR);
	return NULL;
    }

    /* read_label before start */
    if (requested_file == 0 && vself->open_file_fd == -1) {
	vself->open_file_fd = robust_open(self->filename,
                                          O_RDONLY, 0);
	if (vself->open_file_fd < 0) {
	    device_set_error(dself,
		g_strdup_printf(_("Couldn't open file %s: %s"), self->filename, strerror(errno)),
			DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
	    return NULL;
	}
    }

    dself->is_eof = FALSE;
    dself->block = 0;
    g_mutex_lock(dself->device_mutex);
    dself->in_file = FALSE;
    dself->bytes_read = 0;
    g_mutex_unlock(dself->device_mutex);

    result_seek = lseek(vself->open_file_fd, requested_file * DISK_BLOCK_BYTES,
			SEEK_SET);
    if (result_seek == -1) {
        device_set_error(dself,
            g_strdup_printf(_("Error seeking within file: %s"), strerror(errno)),
            DEVICE_STATUS_DEVICE_ERROR);
        return NULL;
    }

    result = vfs_device_robust_read(vself, header_buffer,
                                    &header_buffer_size);
    if (result != RESULT_SUCCESS) {
        device_set_error(dself,
            g_strdup_printf(_("Problem reading Amanda header: %s"), device_error(dself)),
            DEVICE_STATUS_VOLUME_ERROR);
        return NULL;
    }

    rval = g_new(dumpfile_t, 1);
    parse_file_header(header_buffer, rval, header_buffer_size);
    switch (rval->type) {
	case F_DUMPFILE:
	case F_CONT_DUMPFILE:
	case F_SPLIT_DUMPFILE:
	    break;

	case F_TAPESTART:
	    /* file 0 should have a TAPESTART header; diskflat_device_read_label
	        * uses this */
	    if (requested_file == 0)
		break;
	    /* FALLTHROUGH */

	default:
	    device_set_error(dself,
		g_strdup(_("Invalid amanda header while reading file header")),
		DEVICE_STATUS_VOLUME_ERROR);
	    amfree(rval);
	    return NULL;
    }

    /* update our state */
    if (requested_file == 0) {
	dself->header_block_size = header_buffer_size;
    } else {
	g_mutex_lock(dself->device_mutex);
	dself->in_file = TRUE;
	g_mutex_unlock(dself->device_mutex);
    }
    dself->file = requested_file;

    return rval;
}

static gboolean
diskflat_device_seek_block(
    Device *dself,
    guint64 block)
{
    DiskflatDevice *self;
    VfsDevice *vself;
    off_t result;

    self = DISKFLAT_DEVICE(dself);
    vself = VFS_DEVICE(dself);

    g_assert(vself->open_file_fd >= 0);
    g_assert(sizeof(off_t) >= sizeof(guint64));
    if (device_in_error(self)) return FALSE;

    /* Pretty simple. We figure out the blocksize and use that. */
    result = lseek(vself->open_file_fd,
                   (block) * dself->block_size + 2 * VFS_DEVICE_LABEL_SIZE,
                   SEEK_SET);

    dself->block = block;

    if (result == (off_t)(-1)) {
        device_set_error(dself,
            g_strdup_printf(_("Error seeking within file: %s"), strerror(errno)),
            DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    return TRUE;
}

static gboolean
diskflat_device_erase(
    Device *dself)
{
    DiskflatDevice *self = DISKFLAT_DEVICE(dself);
    VfsDevice *vself = VFS_DEVICE(dself);

    if (vself->open_file_fd < 0) {
	vself->open_file_fd = robust_open(self->filename,
					  O_CREAT | O_RDWR,
					  VFS_DEVICE_CREAT_MODE);
	if (vself->open_file_fd < 0) {
	    device_set_error(dself,
		g_strdup_printf(_("Can't open file %s: %s"), self->filename, strerror(errno)),
			DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
	    return FALSE;
	}
    }
    if (ftruncate(vself->open_file_fd, 0) == -1)  {
	g_debug("ftruncate failed: %s", strerror(errno));
	return FALSE;
    }
    vself->release_file(dself);

    dumpfile_free(dself->volume_header);
    dself->volume_header = NULL;
    device_set_error(dself, g_strdup("Unlabeled volume"),
                     DEVICE_STATUS_VOLUME_UNLABELED);

    return TRUE;
}

static gboolean
diskflat_device_finish(
    Device *dself)
{
    VfsDevice *vself = VFS_DEVICE(dself);
    gboolean result;
    DeviceClass *parent_class = DEVICE_CLASS(g_type_class_peek_parent(DISKFLAT_DEVICE_GET_CLASS(dself)));

    g_debug("Finish DISKFLAT device");

    /* Save access mode before parent class messes with it */
    if (vself->open_file_fd != -1) {
	robust_close(vself->open_file_fd);
	vself->open_file_fd = -1;
    }

    result = parent_class->finish(dself);

    if (!result || device_in_error(dself)) {
	return FALSE;
    }

    return TRUE;
}

static void
diskflat_device_finalize(
    GObject *gself)
{
    DiskflatDevice *self  = DISKFLAT_DEVICE(gself);

    GObjectClass *parent_class = G_OBJECT_CLASS(g_type_class_peek_parent(DISKFLAT_DEVICE_GET_CLASS(gself)));

    if (parent_class->finalize) {
	parent_class->finalize(gself);
    }
    amfree(self->filename);
}

static void 
diskflat_release_file(
    Device *dselfi G_GNUC_UNUSED)
{
    return;
}

void
diskflat_update_volume_size(
    Device *dself)
{
    VfsDevice     *vself = VFS_DEVICE(dself);
    DiskflatDevice *self  = DISKFLAT_DEVICE(dself);
    struct stat stat_buf;

    /* stat the file */
    if (stat(self->filename, &stat_buf) < 0) {
	g_warning("Couldn't stat file %s: %s", self->filename, strerror(errno));
	return;
    }

    vself->volume_bytes += stat_buf.st_size;

    return;
}

static gboolean
diskflat_clear_and_prepare_label(
    Device *dself,
    char *label,
    char *timestamp)
{
    dumpfile_t *label_header;
    VfsDevice *vself = VFS_DEVICE(dself);
    DiskflatDevice *self = DISKFLAT_DEVICE(dself);

    vself->open_file_fd = robust_open(self->filename,
                                     O_CREAT | O_WRONLY,
                                     VFS_DEVICE_CREAT_MODE);
    if (vself->open_file_fd < 0) {
	device_set_error(dself,
	    g_strdup_printf(_("Can't open file %s: %s"), self->filename, strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
	return FALSE;
    }

    label_header = make_tapestart_header(dself, label, timestamp);
    if (!vfs_write_amanda_header(vself, label_header)) {
	/* vfs_write_amanda_header sets error status if necessary */
	dumpfile_free(label_header);
	return FALSE;
    }
    dumpfile_free(dself->volume_header);
    dself->header_block_size = VFS_DEVICE_LABEL_SIZE;
    dself->volume_header = label_header;
    dself->file = 0;
    vself->volume_bytes = VFS_DEVICE_LABEL_SIZE;
    return TRUE;
}

static gboolean
diskflat_validate(
    Device *dself G_GNUC_UNUSED)
{
    return TRUE;
}
