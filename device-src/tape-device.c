/*
 * Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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

#include <string.h> /* memset() */
#include "util.h"
#include "tape-device.h"
#include "tape-ops.h"

/* This is equal to 2*1024*1024*1024 - 16*1024*1024 - 1, but written
   explicitly to avoid overflow issues. */
#define RESETOFS_THRESHOLD (0x7effffff)

/* Largest possible block size on SCSI systems. */
#define LARGEST_BLOCK_ESTIMATE (16 * 1024 * 1024)

struct TapeDevicePrivate_s {
    /* This holds the total number of bytes written to the device,
       modulus RESETOFS_THRESHOLD. */
    int write_count;
    char * device_filename;
    gsize read_block_size;
};

/* Possible (abstracted) results from a system I/O operation. */
typedef enum {
    RESULT_SUCCESS,
    RESULT_ERROR,        /* Undefined error (*errmsg set) */
    RESULT_SMALL_BUFFER, /* Tried to read with a buffer that is too
                            small. */
    RESULT_NO_DATA,      /* End of File, while reading */
    RESULT_NO_SPACE,     /* Out of space. Sometimes we don't know if
                            it was this or I/O error, but this is the
                            preferred explanation. */
    RESULT_MAX
} IoResult;

/*
 * Our device-specific properties.  These are not static because they are
 * accessed from the OS-specific tape-*.c files.
 */
DevicePropertyBase device_property_broken_gmt_online;
DevicePropertyBase device_property_fsf;
DevicePropertyBase device_property_fsf_after_filemark;
DevicePropertyBase device_property_bsf;
DevicePropertyBase device_property_fsr;
DevicePropertyBase device_property_bsr;
DevicePropertyBase device_property_eom;
DevicePropertyBase device_property_bsf_after_eom;
DevicePropertyBase device_property_nonblocking_open;
DevicePropertyBase device_property_final_filemarks;
DevicePropertyBase device_property_read_buffer_size; /* old name for READ_BLOCK_SIZE */

void tape_device_register(void);

#define tape_device_read_size(self) \
    (((TapeDevice *)(self))->private->read_block_size? \
	((TapeDevice *)(self))->private->read_block_size : ((Device *)(self))->block_size)

/* here are local prototypes */
static void tape_device_init (TapeDevice * o);
static void tape_device_class_init (TapeDeviceClass * c);
static void tape_device_base_init (TapeDeviceClass * c);
static gboolean tape_device_set_feature_property_fn(Device *p_self, DevicePropertyBase *base,
				    GValue *val, PropertySurety surety, PropertySource source);
static gboolean tape_device_set_final_filemarks_fn(Device *p_self, DevicePropertyBase *base,
				    GValue *val, PropertySurety surety, PropertySource source);
static gboolean tape_device_set_compression_fn(Device *p_self, DevicePropertyBase *base,
				    GValue *val, PropertySurety surety, PropertySource source);
static gboolean tape_device_get_read_block_size_fn(Device *p_self, DevicePropertyBase *base,
				    GValue *val, PropertySurety *surety, PropertySource *source);
static gboolean tape_device_set_read_block_size_fn(Device *p_self, DevicePropertyBase *base,
				    GValue *val, PropertySurety surety, PropertySource source);
static void tape_device_open_device (Device * self, char * device_name, char * device_type, char * device_node);
static Device * tape_device_factory (char * device_name, char * device_type, char * device_node);
static DeviceStatusFlags tape_device_read_label(Device * self);
static gboolean tape_device_write_block(Device * self, guint size, gpointer data);
static int tape_device_read_block(Device * self,  gpointer buf,
                                       int * size_req);
static gboolean tape_device_start (Device * self, DeviceAccessMode mode,
                                   char * label, char * timestamp);
static gboolean tape_device_start_file (Device * self, dumpfile_t * ji);
static gboolean tape_device_finish_file (Device * self);
static dumpfile_t * tape_device_seek_file (Device * self, guint file);
static gboolean tape_device_seek_block (Device * self, guint64 block);
static gboolean tape_device_finish (Device * self);
static IoResult tape_device_robust_read (TapeDevice * self, void * buf,
                                               int * count, char **errmsg);
static IoResult tape_device_robust_write (TapeDevice * self, void * buf, int count, char **errmsg);
static gboolean tape_device_fsf (TapeDevice * self, guint count);
static gboolean tape_device_bsf (TapeDevice * self, guint count, guint file);
static gboolean tape_device_fsr (TapeDevice * self, guint count);
static gboolean tape_device_bsr (TapeDevice * self, guint count, guint file, guint block);
static gboolean tape_device_eod (TapeDevice * self);

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

GType tape_device_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (TapeDeviceClass),
            (GBaseInitFunc) tape_device_base_init,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) tape_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (TapeDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) tape_device_init,
            NULL
        };

        type = g_type_register_static (TYPE_DEVICE, "TapeDevice",
                                       &info, (GTypeFlags)0);
    }

    return type;
}

static void
tape_device_init (TapeDevice * self) {
    Device * d_self;
    GValue response;

    d_self = DEVICE(self);
    bzero(&response, sizeof(response));

    self->private = g_new0(TapeDevicePrivate, 1);

    /* Clear all fields. */
    d_self->block_size = 32768;
    d_self->min_block_size = 32768;
    d_self->max_block_size = LARGEST_BLOCK_ESTIMATE;
    self->broken_gmt_online = FALSE;

    self->fd = -1;

    /* set all of the feature properties to an unsure default of FALSE */
    self->broken_gmt_online = FALSE;
    self->fsf = FALSE;
    self->bsf = FALSE;
    self->fsr = FALSE;
    self->bsr = FALSE;
    self->eom = FALSE;
    self->bsf_after_eom = FALSE;

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(d_self, PROPERTY_BROKEN_GMT_ONLINE,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    device_set_simple_property(d_self, PROPERTY_FSF,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    device_set_simple_property(d_self, PROPERTY_FSF_AFTER_FILEMARK,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    device_set_simple_property(d_self, PROPERTY_BSF,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    device_set_simple_property(d_self, PROPERTY_FSR,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    device_set_simple_property(d_self, PROPERTY_BSR,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    device_set_simple_property(d_self, PROPERTY_EOM,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    device_set_simple_property(d_self, PROPERTY_BSF_AFTER_EOM,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);

#ifdef DEFAULT_TAPE_NON_BLOCKING_OPEN
    self->nonblocking_open = TRUE;
#else
    self->nonblocking_open = FALSE;
#endif
    g_value_set_boolean(&response, self->nonblocking_open);
    device_set_simple_property(d_self, PROPERTY_NONBLOCKING_OPEN,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);

    self->final_filemarks = 2;
    g_value_init(&response, G_TYPE_UINT);
    g_value_set_uint(&response, self->final_filemarks);
    device_set_simple_property(d_self, PROPERTY_FINAL_FILEMARKS,
	    &response, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);

    self->private->read_block_size = 0;
    g_value_init(&response, G_TYPE_UINT);
    g_value_set_uint(&response, self->private->read_block_size);
    device_set_simple_property(d_self, PROPERTY_READ_BLOCK_SIZE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT);
    g_value_unset(&response);

    self->private->write_count = 0;
    self->private->device_filename = NULL;

    /* Static properites */
    g_value_init(&response, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(&response, CONCURRENCY_PARADIGM_EXCLUSIVE);
    device_set_simple_property(d_self, PROPERTY_CONCURRENCY,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(&response, STREAMING_REQUIREMENT_DESIRED);
    device_set_simple_property(d_self, PROPERTY_STREAMING,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE);
    device_set_simple_property(d_self, PROPERTY_APPENDABLE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(d_self, PROPERTY_PARTIAL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, FALSE);
    device_set_simple_property(d_self, PROPERTY_FULL_DELETION,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);

    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_READ_WRITE);
    device_set_simple_property(d_self, PROPERTY_MEDIUM_ACCESS_TYPE,
	    &response, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
    g_value_unset(&response);
}

static void tape_device_finalize(GObject * obj_self) {
    TapeDevice * self = TAPE_DEVICE(obj_self);

    if(G_OBJECT_CLASS(parent_class)->finalize) \
           (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    robust_close(self->fd);
    self->fd = -1;
    amfree(self->private->device_filename);
    amfree(self->private);
}

static void
tape_device_class_init (TapeDeviceClass * c)
{
    DeviceClass *device_class = (DeviceClass *)c;
    GObjectClass *g_object_class = (GObjectClass *)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->open_device = tape_device_open_device;
    device_class->read_label = tape_device_read_label;
    device_class->write_block = tape_device_write_block;
    device_class->read_block = tape_device_read_block;
    device_class->start = tape_device_start;
    device_class->start_file = tape_device_start_file;
    device_class->finish_file = tape_device_finish_file;
    device_class->seek_file = tape_device_seek_file;
    device_class->seek_block = tape_device_seek_block;
    device_class->finish = tape_device_finish;

    g_object_class->finalize = tape_device_finalize;
}

static void
tape_device_base_init (TapeDeviceClass * c)
{
    DeviceClass *device_class = (DeviceClass *)c;

    device_class_register_property(device_class, PROPERTY_BROKEN_GMT_ONLINE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_FSF,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_FSF_AFTER_FILEMARK,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_BSF,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_FSR,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_BSR,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_EOM,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_BSF_AFTER_EOM,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_NONBLOCKING_OPEN,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_feature_property_fn);

    device_class_register_property(device_class, PROPERTY_FINAL_FILEMARKS,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    device_simple_property_get_fn,
	    tape_device_set_final_filemarks_fn);

    /* We don't (yet?) support reading the device's compression state, so not
     * gettable. */
    device_class_register_property(device_class, PROPERTY_COMPRESSION,
	    PROPERTY_ACCESS_SET_MASK,
	    NULL,
	    tape_device_set_compression_fn);

    device_class_register_property(device_class, PROPERTY_READ_BLOCK_SIZE,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    tape_device_get_read_block_size_fn,
	    tape_device_set_read_block_size_fn);

    device_class_register_property(device_class, device_property_read_buffer_size.ID,
	    PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START,
	    tape_device_get_read_block_size_fn,
	    tape_device_set_read_block_size_fn);
}

static gboolean
tape_device_set_feature_property_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    TapeDevice *self = TAPE_DEVICE(p_self);
    GValue old_val;
    gboolean old_bool, new_bool;
    PropertySurety old_surety;
    PropertySource old_source;

    new_bool = g_value_get_boolean(val);

    /* get the old source and surety and see if we're willing to make this change */
    bzero(&old_val, sizeof(old_val));
    if (device_get_simple_property(p_self, base->ID, &old_val, &old_surety, &old_source)) {
	old_bool = g_value_get_boolean(&old_val);

	if (old_surety == PROPERTY_SURETY_GOOD && old_source == PROPERTY_SOURCE_DETECTED) {
	    if (new_bool != old_bool) {
		device_set_error(p_self, vstrallocf(_(
			   "Value for property '%s' was autodetected and cannot be changed"),
			   base->name),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    } else {
		/* pretend we set it, but don't change surety/source */
		return TRUE;
	    }
	}
    }

    /* (note: PROPERTY_* are not constants, so we can't use switch) */
    if (base->ID == PROPERTY_BROKEN_GMT_ONLINE)
	self->broken_gmt_online = new_bool;
    else if (base->ID == PROPERTY_FSF)
	self->fsf = new_bool;
    else if (base->ID == PROPERTY_FSF_AFTER_FILEMARK)
	self->fsf_after_filemark = new_bool;
    else if (base->ID == PROPERTY_BSF)
	self->bsf = new_bool;
    else if (base->ID == PROPERTY_FSR)
	self->fsr = new_bool;
    else if (base->ID == PROPERTY_BSR)
	self->bsr = new_bool;
    else if (base->ID == PROPERTY_EOM)
	self->eom = new_bool;
    else if (base->ID == PROPERTY_BSF_AFTER_EOM)
	self->bsf_after_eom = new_bool;
    else if (base->ID == PROPERTY_NONBLOCKING_OPEN)
	self->nonblocking_open = new_bool;
    else
	return FALSE; /* shouldn't happen */

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
tape_device_set_final_filemarks_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    TapeDevice *self = TAPE_DEVICE(p_self);
    GValue old_val;
    gboolean old_int, new_int;
    PropertySurety old_surety;
    PropertySource old_source;

    new_int = g_value_get_uint(val);

    /* get the old source and surety and see if we're willing to make this change */
    bzero(&old_val, sizeof(old_val));
    if (device_get_simple_property(p_self, base->ID, &old_val, &old_surety, &old_source)) {
	old_int = g_value_get_uint(&old_val);

	if (old_surety == PROPERTY_SURETY_GOOD && old_source == PROPERTY_SOURCE_DETECTED) {
	    if (new_int != old_int) {
		device_set_error(p_self, vstrallocf(_(
			   "Value for property '%s' was autodetected and cannot be changed"),
			   base->name),
		    DEVICE_STATUS_DEVICE_ERROR);
		return FALSE;
	    } else {
		/* pretend we set it, but don't change surety/source */
		return TRUE;
	    }
	}
    }

    self->final_filemarks = new_int;

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
tape_device_set_compression_fn(Device *p_self, DevicePropertyBase *base,
    GValue *val, PropertySurety surety, PropertySource source)
{
    TapeDevice *self = TAPE_DEVICE(p_self);
    gboolean request = g_value_get_boolean(val);

    /* We allow this property to be set at any time. This is mostly
     * because setting compression is a hit-and-miss proposition
     * at any time; some drives accept the mode setting but don't
     * actually support compression, while others do support
     * compression but do it via density settings or some other
     * way. Set this property whenever you want, but all we'll do
     * is report whether or not the ioctl succeeded. */
    if (tape_setcompression(self->fd, request)) {
	/* looks good .. let's start the device over, though */
	device_clear_volume_details(p_self);
    } else {
	return FALSE;
    }

    return device_simple_property_set_fn(p_self, base, val, surety, source);
}

static gboolean
tape_device_get_read_block_size_fn(Device *p_self, DevicePropertyBase *base G_GNUC_UNUSED,
    GValue *val, PropertySurety *surety, PropertySource *source)
{
    /* use the READ_BLOCK_SIZE, even if we're invoked to get the old READ_BUFFER_SIZE */
    return device_simple_property_get_fn(p_self, &device_property_read_block_size,
					val, surety, source);
}

static gboolean
tape_device_set_read_block_size_fn(Device *p_self, DevicePropertyBase *base G_GNUC_UNUSED,
    GValue *val, PropertySurety surety, PropertySource source)
{
    TapeDevice *self = TAPE_DEVICE(p_self);
    guint read_block_size = g_value_get_uint(val);

    if (read_block_size != 0 &&
	    ((gsize)read_block_size < p_self->block_size ||
	     (gsize)read_block_size > p_self->max_block_size))
	return FALSE;

    self->private->read_block_size = read_block_size;

    /* use the READ_BLOCK_SIZE, even if we're invoked to get the old READ_BUFFER_SIZE */
    return device_simple_property_set_fn(p_self, &device_property_read_block_size,
					val, surety, source);
}

void tape_device_register(void) {
    static const char * device_prefix_list[] = { "tape", NULL };

    /* First register tape-specific properties */
    device_property_fill_and_register(&device_property_broken_gmt_online,
                                      G_TYPE_BOOLEAN, "broken_gmt_online",
      "Does this drive support the GMT_ONLINE macro?");

    device_property_fill_and_register(&device_property_fsf,
                                      G_TYPE_BOOLEAN, "fsf",
      "Does this drive support the MTFSF command?");

    device_property_fill_and_register(&device_property_fsf_after_filemark,
                                      G_TYPE_BOOLEAN, "fsf_after_filemark",
      "Does this drive needs a FSF if a filemark is already read?");

    device_property_fill_and_register(&device_property_bsf,
                                      G_TYPE_BOOLEAN, "bsf",
      "Does this drive support the MTBSF command?" );

    device_property_fill_and_register(&device_property_fsr,
                                      G_TYPE_BOOLEAN, "fsr",
      "Does this drive support the MTFSR command?");

    device_property_fill_and_register(&device_property_bsr,
                                      G_TYPE_BOOLEAN, "bsr",
      "Does this drive support the MTBSR command?");

    device_property_fill_and_register(&device_property_eom,
                                      G_TYPE_BOOLEAN, "eom",
      "Does this drive support the MTEOM command?");

    device_property_fill_and_register(&device_property_bsf_after_eom,
                                      G_TYPE_BOOLEAN,
                                      "bsf_after_eom",
      "Does this drive require an MTBSF after MTEOM in order to append?" );

    device_property_fill_and_register(&device_property_nonblocking_open,
                                      G_TYPE_BOOLEAN,
                                      "nonblocking_open",
      "Does this drive require a open with O_NONBLOCK?" );

    device_property_fill_and_register(&device_property_final_filemarks,
                                      G_TYPE_UINT, "final_filemarks",
      "How many filemarks to write after the last tape file?" );

    device_property_fill_and_register(&device_property_read_buffer_size,
                                      G_TYPE_UINT, "read_buffer_size",
      "(deprecated name for READ_BLOCK_SIZE)");

    /* Then the device itself */
    register_device(tape_device_factory, device_prefix_list);
}

/* Open the tape device, trying various combinations of O_RDWR and
   O_NONBLOCK.  Returns -1 and calls device_set_error for errors
   On Linux, with O_NONBLOCK, the kernel just checks the state once,
   whereas it checks it every second for ST_BLOCK_SECONDS if O_NONBLOCK is
   not given.  Amanda already have the code to poll, we want open to check
   the state only once. */

static int try_open_tape_device(TapeDevice * self, char * device_filename) {
    int fd;
    int save_errno;
    DeviceStatusFlags new_status;

#ifdef O_NONBLOCK
    int nonblocking = 0;

    if (self->nonblocking_open) {
	nonblocking = O_NONBLOCK;
    }
#endif

#ifdef O_NONBLOCK
    fd  = robust_open(device_filename, O_RDWR | nonblocking, 0);
    save_errno = errno;
    if (fd < 0 && nonblocking && (save_errno == EWOULDBLOCK || save_errno == EINVAL)) {
        /* Maybe we don't support O_NONBLOCK for tape devices. */
        fd = robust_open(device_filename, O_RDWR, 0);
	save_errno = errno;
    }
#else
    fd = robust_open(device_filename, O_RDWR, 0);
    save_errno = errno;
#endif
    if (fd >= 0) {
        self->write_open_errno = 0;
    } else {
        if (errno == EACCES || errno == EPERM
#ifdef EROFS
			    || errno == EROFS
#endif
	   ) {
            /* Device is write-protected. */
            self->write_open_errno = errno;
#ifdef O_NONBLOCK
            fd = robust_open(device_filename, O_RDONLY | nonblocking, 0);
	    save_errno = errno;
            if (fd < 0 && nonblocking && (save_errno == EWOULDBLOCK || save_errno == EINVAL)) {
                fd = robust_open(device_filename, O_RDONLY, 0);
		save_errno = errno;
            }
#else
            fd = robust_open(device_filename, O_RDONLY, 0);
	    save_errno = errno;
#endif
        }
    }
#ifdef O_NONBLOCK
    /* Clear O_NONBLOCK for operations from now on. */
    if (fd >= 0 && nonblocking)
	fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) & ~O_NONBLOCK);
    errno = save_errno;
    /* function continues after #endif */

#endif /* O_NONBLOCK */

    if (fd < 0) {
	DeviceStatusFlags status_flag = 0;
	if (errno == EBUSY)
	    status_flag = DEVICE_STATUS_DEVICE_BUSY;
	else
	    status_flag = DEVICE_STATUS_DEVICE_ERROR;
	device_set_error(DEVICE(self),
	    vstrallocf(_("Can't open tape device %s: %s"), self->private->device_filename, strerror(errno)),
	    status_flag);
        return -1;
    }

    /* Check that this is actually a tape device. */
    new_status = tape_is_tape_device(fd);
    if (new_status & DEVICE_STATUS_DEVICE_ERROR) {
	device_set_error(DEVICE(self),
	    vstrallocf(_("File %s is not a tape device"), self->private->device_filename),
	    new_status);
        robust_close(fd);
        return -1;
    }
    if (new_status & DEVICE_STATUS_VOLUME_MISSING) {
	device_set_error(DEVICE(self),
	    vstrallocf(_("Tape device %s is not ready or is empty"), self->private->device_filename),
	    new_status);
        robust_close(fd);
        return -1;
    }

    new_status = tape_is_ready(fd, self);
    if (new_status & DEVICE_STATUS_VOLUME_MISSING) {
	device_set_error(DEVICE(self),
	    vstrallocf(_("Tape device %s is empty"), self->private->device_filename),
	    new_status);
        robust_close(fd);
        return -1;
    }
    if (new_status != DEVICE_STATUS_SUCCESS) {
	device_set_error(DEVICE(self),
	    vstrallocf(_("Tape device %s is not ready or is empty"), self->private->device_filename),
	    new_status);
        robust_close(fd);
        return -1;
    }

    return fd;
}

static void
tape_device_open_device (Device * d_self, char * device_name,
			char * device_type, char * device_node) {
    TapeDevice * self;

    self = TAPE_DEVICE(d_self);

    self->fd = -1;
    self->private->device_filename = stralloc(device_node);

    /* Get tape drive/OS info */
    tape_device_detect_capabilities(self);

    /* Chain up */
    if (parent_class->open_device) {
        parent_class->open_device(d_self, device_name, device_type, device_node);
    }
}

void
tape_device_set_capabilities(TapeDevice *self,
	gboolean fsf, PropertySurety fsf_surety, PropertySource fsf_source,
	gboolean fsf_after_filemark, PropertySurety faf_surety, PropertySource faf_source,
	gboolean bsf, PropertySurety bsf_surety, PropertySource bsf_source,
	gboolean fsr, PropertySurety fsr_surety, PropertySource fsr_source,
	gboolean bsr, PropertySurety bsr_surety, PropertySource bsr_source,
	gboolean eom, PropertySurety eom_surety, PropertySource eom_source,
	gboolean bsf_after_eom, PropertySurety bae_surety, PropertySource bae_source,
	guint final_filemarks, PropertySurety ff_surety, PropertySource ff_source)
{
    Device *dself = DEVICE(self);
    GValue val;

    /* this function is called by tape_device_detect_capabilities, and basically
     * exists to take care of the GValue mechanics in one place */

    g_assert(final_filemarks == 1 || final_filemarks == 2);

    bzero(&val, sizeof(val));
    g_value_init(&val, G_TYPE_BOOLEAN);

    self->fsf = fsf;
    g_value_set_boolean(&val, fsf);
    device_set_simple_property(dself, PROPERTY_FSF, &val, fsf_surety, fsf_source);

    self->fsf_after_filemark = fsf_after_filemark;
    g_value_set_boolean(&val, fsf_after_filemark);
    device_set_simple_property(dself, PROPERTY_FSF_AFTER_FILEMARK, &val, faf_surety, faf_source);

    self->bsf = bsf;
    g_value_set_boolean(&val, bsf);
    device_set_simple_property(dself, PROPERTY_BSF, &val, bsf_surety, bsf_source);

    self->fsr = fsr;
    g_value_set_boolean(&val, fsr);
    device_set_simple_property(dself, PROPERTY_FSR, &val, fsr_surety, fsr_source);

    self->bsr = bsr;
    g_value_set_boolean(&val, bsr);
    device_set_simple_property(dself, PROPERTY_BSR, &val, bsr_surety, bsr_source);

    self->eom = eom;
    g_value_set_boolean(&val, eom);
    device_set_simple_property(dself, PROPERTY_EOM, &val, eom_surety, eom_source);

    self->bsf_after_eom = bsf_after_eom;
    g_value_set_boolean(&val, bsf_after_eom);
    device_set_simple_property(dself, PROPERTY_BSF_AFTER_EOM, &val, bae_surety, bae_source);

    g_value_unset(&val);
    g_value_init(&val, G_TYPE_UINT);

    self->final_filemarks = final_filemarks;
    g_value_set_uint(&val, final_filemarks);
    device_set_simple_property(dself, PROPERTY_FINAL_FILEMARKS, &val, ff_surety, ff_source);

    g_value_unset(&val);
}

static DeviceStatusFlags tape_device_read_label(Device * dself) {
    TapeDevice * self;
    char * header_buffer;
    int buffer_len;
    IoResult result;
    dumpfile_t *header;
    DeviceStatusFlags new_status;
    char *msg = NULL;

    self = TAPE_DEVICE(dself);

    amfree(dself->volume_label);
    amfree(dself->volume_time);
    amfree(dself->volume_header);

    if (device_in_error(self)) return dself->status;

    header = dself->volume_header = g_new(dumpfile_t, 1);
    fh_init(header);

    if (self->fd == -1) {
        self->fd = try_open_tape_device(self, self->private->device_filename);
	/* if the open failed, then try_open_tape_device already set the
	 * approppriate error status */
	if (self->fd == -1)
	    return dself->status;
    }

    /* Rewind it. */
    if (!tape_rewind(self->fd)) {
	device_set_error(dself,
	    vstrallocf(_("Error rewinding device %s: %s"),
		    self->private->device_filename, strerror(errno)),
	      DEVICE_STATUS_DEVICE_ERROR
	    | DEVICE_STATUS_VOLUME_ERROR);
        robust_close(self->fd);
        return dself->status;
    }

    buffer_len = tape_device_read_size(self);
    header_buffer = malloc(buffer_len);
    result = tape_device_robust_read(self, header_buffer, &buffer_len, &msg);

    if (result != RESULT_SUCCESS) {
        free(header_buffer);
        tape_rewind(self->fd);
        /* I/O error. */
	switch (result) {
	case RESULT_NO_DATA:
	    msg = stralloc(_("no data"));
            new_status = (DEVICE_STATUS_VOLUME_ERROR |
	                  DEVICE_STATUS_VOLUME_UNLABELED);
	    break;

	case RESULT_SMALL_BUFFER:
	    msg = stralloc(_("block size too small"));
            new_status = (DEVICE_STATUS_DEVICE_ERROR |
	                  DEVICE_STATUS_VOLUME_ERROR);
	    break;

	default:
	    msg = stralloc(_("unknown error"));
	case RESULT_ERROR:
            new_status = (DEVICE_STATUS_DEVICE_ERROR |
	                  DEVICE_STATUS_VOLUME_ERROR |
	                  DEVICE_STATUS_VOLUME_UNLABELED);
	    break;
        }
	device_set_error(dself,
		 g_strdup_printf(_("Error reading Amanda header: %s"),
			msg? msg : _("unknown error")),
		 new_status);
	amfree(msg);
	return dself->status;
    }

    parse_file_header(header_buffer, header, buffer_len);
    amfree(header_buffer);
    if (header->type != F_TAPESTART) {
	device_set_error(dself,
		stralloc(_("No tapestart header -- unlabeled device?")),
		DEVICE_STATUS_VOLUME_UNLABELED);
        return dself->status;
    }

    dself->volume_label = g_strdup(header->name);
    dself->volume_time = g_strdup(header->datestamp);
    /* dself->volume_header is already set */

    device_set_error(dself, NULL, DEVICE_STATUS_SUCCESS);

    return dself->status;
}

static gboolean
tape_device_write_block(Device * pself, guint size, gpointer data) {
    TapeDevice * self;
    char *replacement_buffer = NULL;
    IoResult result;
    char *msg = NULL;

    self = TAPE_DEVICE(pself);

    g_assert(self->fd >= 0);
    if (device_in_error(self)) return FALSE;

    /* zero out to the end of a short block -- tape devices only write
     * whole blocks. */
    if (size < pself->block_size) {
        replacement_buffer = malloc(pself->block_size);
        memcpy(replacement_buffer, data, size);
        bzero(replacement_buffer+size, pself->block_size-size);

        data = replacement_buffer;
        size = pself->block_size;
    }

    result = tape_device_robust_write(self, data, size, &msg);
    amfree(replacement_buffer);

    switch (result) {
	case RESULT_SUCCESS:
	    break;

	case RESULT_NO_SPACE:
	    device_set_error(pself,
		stralloc(_("No space left on device")),
		DEVICE_STATUS_VOLUME_ERROR);
	    pself->is_eof = TRUE;
	    return FALSE;

	default:
	    msg = stralloc(_("unknown error"));
	case RESULT_ERROR:
	    device_set_error(pself,
		g_strdup_printf(_("Error writing block: %s"), msg),
		DEVICE_STATUS_DEVICE_ERROR);
	    amfree(msg);
	    return FALSE;
    }

    pself->block++;

    return TRUE;
}

static int tape_device_read_block (Device * pself, gpointer buf,
                                   int * size_req) {
    TapeDevice * self;
    int size;
    IoResult result;
    gssize read_block_size = tape_device_read_size(pself);
    char *msg = NULL;

    self = TAPE_DEVICE(pself);

    g_assert(self->fd >= 0);
    if (device_in_error(self)) return -1;

    g_assert(read_block_size < INT_MAX); /* data type mismatch */
    if (buf == NULL || *size_req < (int)read_block_size) {
        /* Just a size query. */
        *size_req = (int)read_block_size;
        return 0;
    }

    size = *size_req;
    result = tape_device_robust_read(self, buf, &size, &msg);
    switch (result) {
    case RESULT_SUCCESS:
        *size_req = size;
        pself->block++;
        return size;
    case RESULT_SMALL_BUFFER: {
        gsize new_size;
	GValue newval;

        /* If this happens, it means that we have:
         *     (next block size) > (buffer size) >= (read_block_size)
         * The solution is to ask for an even bigger buffer. We also play
         * some games to refrain from reading above the SCSI limit or from
         * integer overflow.  Note that not all devices will tell us about
	 * this problem -- some will just discard the "extra" data. */
        new_size = MIN(INT_MAX/2 - 1, *size_req) * 2;
        if (new_size > LARGEST_BLOCK_ESTIMATE &&
            *size_req < LARGEST_BLOCK_ESTIMATE) {
            new_size = LARGEST_BLOCK_ESTIMATE;
        }
        g_assert (new_size > (gsize)*size_req);

	g_info("Device %s indicated blocksize %zd was too small; using %zd.",
	    pself->device_name, (gsize)*size_req, new_size);
	*size_req = (int)new_size;
	self->private->read_block_size = new_size;

	bzero(&newval, sizeof(newval));
	g_value_init(&newval, G_TYPE_UINT);
	g_value_set_uint(&newval, self->private->read_block_size);
	device_set_simple_property(pself, PROPERTY_READ_BLOCK_SIZE,
		&newval, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DETECTED);
	g_value_unset(&newval);

	return 0;
    }
    case RESULT_NO_DATA:
        pself->is_eof = TRUE;
	pself->in_file = FALSE;
	device_set_error(pself,
	    stralloc(_("EOF")),
	    DEVICE_STATUS_SUCCESS);
        return -1;

    default:
	msg = stralloc(_("unknown error"));
    case RESULT_ERROR:
	device_set_error(pself,
	    vstrallocf(_("Error reading from tape device: %s"), msg),
	    DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
	amfree(msg);
        return -1;
    }

    g_assert_not_reached();
}

/* Just a helper function for tape_device_start(). */
static gboolean write_tapestart_header(TapeDevice * self, char * label,
                                       char * timestamp) {
     IoResult result;
     dumpfile_t * header;
     char * header_buf;
     Device * d_self = (Device*)self;
     char *msg = NULL;

     tape_rewind(self->fd);

     header = make_tapestart_header(d_self, label, timestamp);
     g_assert(header != NULL);
     header_buf = device_build_amanda_header(d_self, header, NULL);
     if (header_buf == NULL) {
	 device_set_error(d_self,
	    stralloc(_("Tapestart header won't fit in a single block!")),
	    DEVICE_STATUS_DEVICE_ERROR);
         return FALSE;
     }
     amfree(header);

     result = tape_device_robust_write(self, header_buf, d_self->block_size, &msg);
     if (result != RESULT_SUCCESS) {
	 device_set_error(d_self,
	    g_strdup_printf(_("Error writing tapestart header: %s"),
			(result == RESULT_ERROR)? msg : _("out of space")),
	    DEVICE_STATUS_DEVICE_ERROR);
	amfree(msg);
	amfree(header_buf);
	return FALSE;
     }

     amfree(header_buf);

     if (!tape_weof(self->fd, 1)) {
	device_set_error(d_self,
			 vstrallocf(_("Error writing filemark: %s"),
				    strerror(errno)),
			 DEVICE_STATUS_DEVICE_ERROR|DEVICE_STATUS_VOLUME_ERROR);
	return FALSE;
     }

     return TRUE;

}

static gboolean
tape_device_start (Device * d_self, DeviceAccessMode mode, char * label,
                   char * timestamp) {
    TapeDevice * self;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return FALSE;

    if (self->fd == -1) {
        self->fd = try_open_tape_device(self, self->private->device_filename);
	/* if the open failed, then try_open_tape_device already set the
	 * approppriate error status */
	if (self->fd == -1)
	    return FALSE;
    }

    if (mode != ACCESS_WRITE && d_self->volume_label == NULL) {
	/* we need a labeled volume for APPEND and READ */
	if (tape_device_read_label(d_self) != DEVICE_STATUS_SUCCESS)
	    return FALSE;
    }

    d_self->access_mode = mode;
    d_self->in_file = FALSE;

    if (IS_WRITABLE_ACCESS_MODE(mode)) {
        if (self->write_open_errno != 0) {
            /* We tried and failed to open the device in write mode. */
	    device_set_error(d_self,
		vstrallocf(_("Can't open tape device %s for writing: %s"),
			    self->private->device_filename, strerror(self->write_open_errno)),
		DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
            return FALSE;
        } else if (!tape_rewind(self->fd)) {
	    device_set_error(d_self,
		vstrallocf(_("Couldn't rewind device: %s"), strerror(errno)),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
        }
    }

    /* Position the tape */
    switch (mode) {
    case ACCESS_APPEND:
	if (d_self->volume_label == NULL && device_read_label(d_self) != DEVICE_STATUS_SUCCESS) {
	    /* device_read_label already set our error message */
            return FALSE;
	}

        if (!tape_device_eod(self)) {
	    device_set_error(d_self,
		vstrallocf(_("Couldn't seek to end of tape: %s"), strerror(errno)),
		DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
	}
        break;

    case ACCESS_READ:
	if (d_self->volume_label == NULL && device_read_label(d_self) != DEVICE_STATUS_SUCCESS) {
	    /* device_read_label already set our error message */
            return FALSE;
	}

        if (!tape_rewind(self->fd)) {
	    device_set_error(d_self,
		vstrallocf(_("Couldn't rewind device: %s"), strerror(errno)),
		DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
        }
        d_self->file = 0;
        break;

    case ACCESS_WRITE:
        if (!write_tapestart_header(self, label, timestamp)) {
	    /* write_tapestart_header already set the error status */
            return FALSE;
        }

        d_self->volume_label = newstralloc(d_self->volume_label, label);
        d_self->volume_time = newstralloc(d_self->volume_time, timestamp);

	/* unset the VOLUME_UNLABELED flag, if it was set */
	device_set_error(d_self, NULL, DEVICE_STATUS_SUCCESS);
        d_self->file = 0;
        break;

    default:
        g_assert_not_reached();
    }

    return TRUE;
}

static gboolean tape_device_start_file(Device * d_self,
                                       dumpfile_t * info) {
    TapeDevice * self;
    IoResult result;
    char * amanda_header;
    char *msg = NULL;

    self = TAPE_DEVICE(d_self);

    g_assert(self->fd >= 0);
    if (device_in_error(self)) return FALSE;

    /* set the blocksize in the header properly */
    info->blocksize = d_self->block_size;

    /* Make the Amanda header suitable for writing to the device. */
    /* Then write the damn thing. */
    amanda_header = device_build_amanda_header(d_self, info, NULL);
    if (amanda_header == NULL) {
	device_set_error(d_self,
	    stralloc(_("Amanda file header won't fit in a single block!")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }

    result = tape_device_robust_write(self, amanda_header, d_self->block_size, &msg);
    if (result != RESULT_SUCCESS) {
	device_set_error(d_self,
	    vstrallocf(_("Error writing file header: %s"),
			(result == RESULT_ERROR)? msg : _("out of space")),
	    DEVICE_STATUS_DEVICE_ERROR);
	amfree(amanda_header);
	amfree(msg);
        return FALSE;
    }
    amfree(amanda_header);

    /* arrange the file numbers correctly */
    d_self->in_file = TRUE;
    d_self->block = 0;
    if (d_self->file >= 0)
        d_self->file ++;
    return TRUE;
}

static gboolean
tape_device_finish_file (Device * d_self) {
    TapeDevice * self;

    self = TAPE_DEVICE(d_self);
    if (device_in_error(d_self)) return FALSE;

    if (!tape_weof(self->fd, 1)) {
	device_set_error(d_self,
		vstrallocf(_("Error writing filemark: %s"), strerror(errno)),
		DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
        return FALSE;
    }

    d_self->in_file = FALSE;
    return TRUE;
}

static dumpfile_t *
tape_device_seek_file (Device * d_self, guint file) {
    TapeDevice * self;
    int difference;
    char * header_buffer;
    dumpfile_t * rval;
    int buffer_len;
    IoResult result;
    char *msg;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return NULL;

    difference = file - d_self->file;

    /* Check if we already read a filemark. */
    /* If we already read a filemark and the drive automaticaly goes to the
       next file, then we must reduce the difference by one. */
    if (d_self->is_eof && !self->fsf_after_filemark) {
        difference --;
    }

    d_self->in_file = FALSE;
    d_self->is_eof = FALSE;
    d_self->block = 0;

reseek:
    if (difference > 0) {
        /* Seeking forwards */
        if (!tape_device_fsf(self, difference)) {
            tape_rewind(self->fd);
	    device_set_error(d_self,
		vstrallocf(_("Could not seek forward to file %d"), file),
		DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
            return NULL;
        }
    } else if (difference < 0) {
        /* Seeking backwards */
        if (!tape_device_bsf(self, -difference, d_self->file)) {
            tape_rewind(self->fd);
	    device_set_error(d_self,
		vstrallocf(_("Could not seek backward to file %d"), file),
		DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
            return NULL;
        }
    }

    buffer_len = tape_device_read_size(d_self);
    header_buffer = malloc(buffer_len);
    d_self->is_eof = FALSE;
    result = tape_device_robust_read(self, header_buffer, &buffer_len, &msg);

    if (result != RESULT_SUCCESS) {
        free(header_buffer);
        tape_rewind(self->fd);
	switch (result) {
	case RESULT_NO_DATA:
            /* If we read 0 bytes, that means we encountered a double
             * filemark, which indicates end of tape. This should
             * work even with QIC tapes on operating systems with
             * proper support. */
	    d_self->file = file; /* other attributes are already correct */
            return make_tapeend_header();

	case RESULT_SMALL_BUFFER:
	    msg = stralloc(_("block size too small"));
	    break;

	default:
	    msg = stralloc(_("unknown error"));
	case RESULT_ERROR:
	    break;
        }
	device_set_error(d_self,
	    g_strdup_printf(_("Error reading Amanda header: %s"), msg),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
	amfree(msg);
        return NULL;
    }

    rval = g_new(dumpfile_t, 1);
    parse_file_header(header_buffer, rval, buffer_len);
    amfree(header_buffer);
    switch (rval->type) {
    case F_DUMPFILE:
    case F_CONT_DUMPFILE:
    case F_SPLIT_DUMPFILE:
        break;

    case F_NOOP:
	/* a NOOP is written on QIC tapes to avoid writing two sequential
	 * filemarks when closing a device in WRITE or APPEND mode.  In this
	 * case, we just seek to the next file. */
	amfree(rval);
	file++;
	difference = 1;
	goto reseek;

    default:
        tape_rewind(self->fd);
	device_set_error(d_self,
	    stralloc(_("Invalid amanda header while reading file header")),
	    DEVICE_STATUS_VOLUME_ERROR);
        amfree(rval);
        return NULL;
    }

    d_self->in_file = TRUE;
    d_self->file = file;

    return rval;
}

static gboolean
tape_device_seek_block (Device * d_self, guint64 block) {
    TapeDevice * self;
    int difference;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return FALSE;

    difference = block - d_self->block;

    if (difference > 0) {
        if (!tape_device_fsr(self, difference)) {
	    device_set_error(d_self,
		vstrallocf(_("Could not seek forward to block %ju: %s"), (uintmax_t)block, strerror(errno)),
		DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
	}
    } else if (difference < 0) {
        if (!tape_device_bsr(self, difference, d_self->file, d_self->block)) {
	    device_set_error(d_self,
		vstrallocf(_("Could not seek backward to block %ju: %s"), (uintmax_t)block, strerror(errno)),
		DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
	}
    }

    d_self->block = block;
    return TRUE;
}

static gboolean
tape_device_finish (Device * d_self) {
    TapeDevice * self;
    char *msg = NULL;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return FALSE;

    if (d_self->access_mode == ACCESS_NULL)
	return TRUE;

    /* Polish off this file, if relevant. */
    if (d_self->in_file && IS_WRITABLE_ACCESS_MODE(d_self->access_mode)) {
        if (!device_finish_file(d_self))
            return FALSE;
    }

    /* Straighten out the filemarks.  We already wrote one in finish_file, and
     * the device driver will write another filemark when we rewind.  This means
     * that, if we do nothing, we'll get two filemarks.  If final_filemarks is
     * 1, this would be wrong, so in this case we insert a F_NOOP header between
     * the two filemarks. */
    if (self->final_filemarks == 1 &&
        IS_WRITABLE_ACCESS_MODE(d_self->access_mode)) {
	dumpfile_t file;
	char *header;
	int result;

	/* write a F_NOOP header */
	fh_init(&file);
	file.type = F_NOOP;
	header = device_build_amanda_header(d_self, &file, NULL);
	if (!header) {
	    device_set_error(d_self,
		stralloc(_("Amanda file header won't fit in a single block!")),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}

	result = tape_device_robust_write(self, header, d_self->block_size, &msg);
	if (result != RESULT_SUCCESS) {
	    device_set_error(d_self,
		vstrallocf(_("Error writing file header: %s"),
			    (result == RESULT_ERROR)? msg : _("out of space")),
		DEVICE_STATUS_DEVICE_ERROR);
	    amfree(header);
	    amfree(msg);
	    return FALSE;
	}
	amfree(header);
    }

    /* Rewind (the kernel will write a filemark first) */
    if (!tape_rewind(self->fd)) {
	device_set_error(d_self,
	    vstrallocf(_("Couldn't rewind device: %s"), strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    d_self->is_eof = FALSE;
    d_self->access_mode = ACCESS_NULL;

    return TRUE;
}

/* Works just like read(), except for the following:
 * 1) Retries on EINTR & friends.
 * 2) Stores count in parameter, not return value.
 * 3) Provides explicit return result.
 * *errmsg is only set on RESULT_ERROR.
 */
static IoResult
tape_device_robust_read (TapeDevice * self, void * buf, int * count, char **errmsg) {
    Device * d_self;
    int result;

    d_self = (Device*)self;

    /* Callers should ensure this. */
    g_assert(*count >= 0);

    for (;;) {
        result = read(self->fd, buf, *count);
        if (result > 0) {
            /* Success. By definition, we read a full block. */
            d_self->is_eof = FALSE;
            *count = result;
            return RESULT_SUCCESS;
        } else if (result == 0) {
            d_self->is_eof = TRUE;
            return RESULT_NO_DATA;
        } else {
            if (0
#ifdef EAGAIN
                || errno == EAGAIN
#endif
#ifdef EWOULDBLOCK
                || errno == EWOULDBLOCK
#endif
#ifdef EINTR
                || errno == EINTR
#endif
                ) {
                /* Interrupted system call */
                continue;
            } else if ((0
#ifdef ENOMEM
                        || errno == ENOMEM /* bad user-space buffer */
#endif
#ifdef EOVERFLOW
                        || errno == EOVERFLOW /* bad kernel-space buffer */
#endif
#ifdef EINVAL
                        || errno == EINVAL /* ??? */
#endif
                        )) {
                /* Buffer too small. */
		g_warning("Buffer is too small (%d bytes) from %s: %s",
			*count, self->private->device_filename, strerror(errno));
                return RESULT_SMALL_BUFFER;
            } else {
		*errmsg = g_strdup_printf(_("Error reading %d bytes from %s: %s"),
			*count, self->private->device_filename, strerror(errno));
                return RESULT_ERROR;
            }
        }
    }

    g_assert_not_reached();
}

/* Kernel workaround: If needed, poke the kernel so it doesn't fail.
   at the 2GB boundry. Parameters are G_GNUC_UNUSED in case NEED_RESETOFS
   is not defined. */
static void check_resetofs(TapeDevice * self G_GNUC_UNUSED,
                           int count G_GNUC_UNUSED) {
#ifdef NEED_RESETOFS
    Device * d_self;
    int result;

    d_self = (Device*)self;

    self->private->write_count += count;
    if (self->private->write_count < RESETOFS_THRESHOLD) {
        return;
    }

    result = lseek(self->fd, 0, SEEK_SET);
    if (result < 0) {
	g_warning(_("lseek() failed during kernel 2GB workaround: %s"),
	       strerror(errno));
    }
#endif
}

/* *errmsg is only set on RESULT_ERROR */
static IoResult
tape_device_robust_write (TapeDevice * self, void * buf, int count, char **errmsg) {
    Device * d_self;
    int result;

    d_self = (Device*)self;

    check_resetofs(self, count);

    for (;;) {
        result = write(self->fd, buf, count);

        if (result == count) {
            /* Success. */

            self->private->write_count ++;
            return RESULT_SUCCESS;
        } else if (result >= 0) {
            /* write() returned a short count. This should not happen. */
	    *errmsg = g_strdup_printf("Mysterious short write on tape device: Tried %d, got %d",
				count, result);
            return RESULT_ERROR;
        } else if (0
#ifdef EAGAIN
                   || errno == EAGAIN
#endif
#ifdef EWOULDBLOCK
                   || errno == EWOULDBLOCK
#endif
#ifdef EINTR
                   || errno == EINTR
#endif
                   ) {
                /* Interrupted system call */
            continue;
        } else if (0
#ifdef ENOSPC
                   || errno == ENOSPC
#endif
#ifdef EIO
                   || errno == EIO
#endif
                   ) {
            /* Probably EOT. Print a message if we got EIO. */
#ifdef EIO
            if (errno == EIO) {
		g_warning(_("Got EIO on %s, assuming end of tape"),
			self->private->device_filename);
            }
#endif
            return RESULT_NO_SPACE;
        } else {
            /* WTF */
	    *errmsg = vstrallocf(_("Kernel gave unexpected write() result of \"%s\" on device %s"),
			    strerror(errno), self->private->device_filename);
            return RESULT_ERROR;
        }
    }

    g_assert_not_reached();
}

/* Reads some number of tape blocks into the bit-bucket. If the count
   is negative, then we read the rest of the entire file. Returns the
   number of blocks read, or -1 if an error occured. If we encounter
   EOF (as opposed to some other error) we return the number of blocks
   actually read. */
static int drain_tape_blocks(TapeDevice * self, int count) {
    char * buffer;
    gsize buffer_size;
    int i;

    buffer_size = tape_device_read_size(self);

    buffer = malloc(buffer_size);

    for (i = 0; i < count || count < 0;) {
        int result;

        result = read(self->fd, buffer, buffer_size);
        if (result > 0) {
            i ++;
            continue;
        } else if (result == 0) {
            amfree(buffer);
            return i;
        } else {
            /* First check for interrupted system call. */
            if (0
#ifdef EAGAIN
                || errno == EAGAIN
#endif
#ifdef EWOULDBLOCK
                || errno == EWOULDBLOCK
#endif
#ifdef EINTR
                || errno == EINTR
#endif
                ) {
                /* Interrupted system call */
                continue;
            } else if (0
#ifdef ENOSPC
                       || errno == ENOSPC /* bad user-space buffer */
#endif
#ifdef EOVERFLOW
                       || errno == EOVERFLOW /* bad kernel-space buffer */
#endif
#ifdef EINVAL
                       || errno == EINVAL /* ??? */
#endif
                       ) {
                /* The buffer may not be big enough. But the OS is not
                   100% clear. We double the buffer and try again, but
                   in no case allow a buffer bigger than 32 MB. */
                buffer_size *= 2;

                if (buffer_size > 32*1024*1024) {
                    amfree(buffer);
                    return -1;
                } else {
                    buffer = realloc(buffer, buffer_size);
                    continue;
                }
            }
        }
    }

    amfree(buffer);
    return count;
}

/* FIXME: Make sure that there are no cycles in reimplementation
   dependencies. */

static gboolean
tape_device_fsf (TapeDevice * self, guint count) {
    if (self->fsf) {
        return tape_fsf(self->fd, count);
    } else {
        guint i;
        for (i = 0; i < count; i ++) {
            if (drain_tape_blocks(self, -1) < 0)
                return FALSE;
        }
        return TRUE;
    }
}

/* Seek back over count + 1 filemarks to the start of the given file. */
static gboolean
tape_device_bsf (TapeDevice * self, guint count, guint file) {
    if (self->bsf) {
        /* The BSF operation is not very smart; it includes the
           filemark of the present file as part of the count, and seeks
           to the wrong (BOT) side of the filemark. We compensate for
           this by seeking one filemark too many, then FSFing back over
           it.

           If this procedure fails for some reason, we can still try
           the backup plan. */
        if (tape_bsf(self->fd, count + 1) &&
            tape_device_fsf(self, 1))
            return TRUE;
    } /* Fall through to backup plan. */

    /* We rewind the tape, then seek forward the given number of
       files. */
    if (!tape_rewind(self->fd))
        return FALSE;

    return tape_device_fsf(self, file);
}


static gboolean
tape_device_fsr (TapeDevice * self, guint count) {
    if (self->fsr) {
        return tape_fsr(self->fd, count);
    } else {
        int result = drain_tape_blocks(self, count);
        return result > 0 && (int)count == result;
    }
}

/* Seek back the given number of blocks to block number block within
 * the current file, numbered file. */

static gboolean
tape_device_bsr (TapeDevice * self, guint count, guint file, guint block) {
    if (self->bsr) {
        return tape_bsr(self->fd, count);
    } else {
        /* We BSF, then FSR. */
        if (!tape_device_bsf(self, 0, file))
            return FALSE;

        return tape_device_fsr(self, block);
    }
    g_assert_not_reached();
}

/* Go to the right place to write more data, and update the file
   number if possible. */
static gboolean
tape_device_eod (TapeDevice * self) {
    Device * d_self;
    int count;

    d_self = (Device*)self;

    if (self->eom) {
        int result;
        result = tape_eod(self->fd);
        if (result == TAPE_OP_ERROR) {
            return FALSE;
        } else if (result != TAPE_POSITION_UNKNOWN) {
	    /* great - we just fast-forwarded to EOD, but don't know where we are, so
	     * now we have to rewind and drain all of that data.  Warn the user so that
	     * we can skip the fast-forward-rewind stage on the next run */
	    g_warning("Seek to end of tape does not give an accurate tape position; set "
		      "the EOM property to 0 to avoid useless tape movement.");
	    /* and set the property so that next time *this* object is opened for
	     * append, we skip this stage */
	    self->eom = FALSE;
            /* fall through to draining blocks, below */
        } else {
            /* We drop by 1 because Device will increment the first
               time the user does start_file. */
            d_self->file = result - 1;
	    return TRUE;
        }
    }

    if (!tape_rewind(self->fd))
	return FALSE;

    count = 0;
    for (;;) {
	/* We alternately read a block and FSF. If the read is
	   successful, then we are not there yet and should FSF
	   again. */
	int result;
	result = drain_tape_blocks(self, 1);
	if (result == 1) {
	    /* More data, FSF. */
	    tape_device_fsf(self, 1);
	    count ++;
	} else if (result == 0) {
	    /* Finished. */
	    d_self->file = count - 1;
	    return TRUE;
	} else {
	    return FALSE;
	}
    }
}

static Device *
tape_device_factory (char * device_name, char * device_type, char * device_node) {
    Device * rval;
    g_assert(0 == strcmp(device_type, "tape"));
    rval = DEVICE(g_object_new(TYPE_TAPE_DEVICE, NULL));
    device_open_device(rval, device_name, device_type, device_node);
    return rval;
}
