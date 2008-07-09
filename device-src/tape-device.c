/*
 * Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as 
 * published by the Free Software Foundation.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 * 
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
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
};

/* Possible (abstracted) results from a system I/O operation. */
typedef enum {
    RESULT_SUCCESS,
    RESULT_ERROR,        /* Undefined error. */
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
DevicePropertyBase device_property_bsf;
DevicePropertyBase device_property_fsr;
DevicePropertyBase device_property_bsr;
DevicePropertyBase device_property_eom;
DevicePropertyBase device_property_bsf_after_eom;
DevicePropertyBase device_property_final_filemarks;

void tape_device_register(void);

/* here are local prototypes */
static void tape_device_init (TapeDevice * o);
static void tape_device_class_init (TapeDeviceClass * c);
static void tape_device_open_device (Device * self, char * device_name, char * device_type, char * device_node);
static Device * tape_device_factory (char * device_name, char * device_type, char * device_node);
static DeviceStatusFlags tape_device_read_label(Device * self);
static gboolean tape_device_write_block(Device * self, guint size,
                                        gpointer data, gboolean short_block);
static int tape_device_read_block(Device * self,  gpointer buf,
                                       int * size_req);
static gboolean tape_device_start (Device * self, DeviceAccessMode mode,
                                   char * label, char * timestamp);
static gboolean tape_device_start_file (Device * self, const dumpfile_t * ji);
static gboolean tape_device_finish_file (Device * self);
static dumpfile_t * tape_device_seek_file (Device * self, guint file);
static gboolean tape_device_seek_block (Device * self, guint64 block);
static gboolean tape_device_property_get (Device * self, DevicePropertyId id,
                                          GValue * val);
static gboolean tape_device_property_set (Device * self, DevicePropertyId id,
                                          GValue * val);
static gboolean tape_device_finish (Device * self);
static IoResult tape_device_robust_read (TapeDevice * self, void * buf,
                                               int * count);
static IoResult tape_device_robust_write (TapeDevice * self, void * buf, int count);
static gboolean tape_device_fsf (TapeDevice * self, guint count);
static gboolean tape_device_bsf (TapeDevice * self, guint count, guint file);
static gboolean tape_device_fsr (TapeDevice * self, guint count);
static gboolean tape_device_bsr (TapeDevice * self, guint count, guint file, guint block);
static gboolean tape_device_eod (TapeDevice * self);
static void tape_validate_properties (TapeDevice * self);

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

GType tape_device_get_type (void)
{
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (TapeDeviceClass),
            (GBaseInitFunc) NULL,
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
    Device * device_self;
    DeviceProperty prop;
    GValue response;

    device_self = (Device*)self;
    bzero(&response, sizeof(response));

    self->private = g_new0(TapeDevicePrivate, 1);

    /* Clear all fields. */
    self->min_block_size = self->fixed_block_size = 32768;
    self->max_block_size = self->read_block_size = MAX_TAPE_BLOCK_BYTES;
    self->broken_gmt_online = FALSE;

    self->fd = -1;
    
    self->fsf = self->bsf = self->fsr = self->bsr = self->eom =
        self->bsf_after_eom = self->compression = self->first_file = 0;
    self->final_filemarks = 2;

    self->private->write_count = 0;
    self->private->device_filename = NULL;

    /* Register properites */
    prop.base = &device_property_concurrency;
    prop.access = PROPERTY_ACCESS_GET_MASK;
    g_value_init(&response, CONCURRENCY_PARADIGM_TYPE);
    g_value_set_enum(&response, CONCURRENCY_PARADIGM_EXCLUSIVE);
    device_add_property(device_self, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_streaming;
    g_value_init(&response, STREAMING_REQUIREMENT_TYPE);
    g_value_set_enum(&response, STREAMING_REQUIREMENT_DESIRED);
    device_add_property(device_self, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_appendable;
    g_value_init(&response, G_TYPE_BOOLEAN);
    g_value_set_boolean(&response, TRUE);
    device_add_property(device_self, &prop, &response);

    prop.base = &device_property_partial_deletion;
    g_value_set_boolean(&response, FALSE);
    device_add_property(device_self, &prop, &response);
    g_value_unset(&response);

    prop.base = &device_property_medium_access_type;
    g_value_init(&response, MEDIA_ACCESS_MODE_TYPE);
    g_value_set_enum(&response, MEDIA_ACCESS_MODE_READ_WRITE);
    device_add_property(device_self, &prop, &response);
    g_value_unset(&response);

    prop.access = PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_MASK;
    prop.base = &device_property_compression;
    device_add_property(device_self, &prop, NULL);

    prop.access = PROPERTY_ACCESS_GET_MASK | PROPERTY_ACCESS_SET_BEFORE_START;
    prop.base = &device_property_min_block_size;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_max_block_size;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_block_size;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_broken_gmt_online;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_fsf;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_bsf;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_fsr;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_bsr;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_eom;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_bsf_after_eom;
    device_add_property(device_self, &prop, NULL);
    prop.base = &device_property_final_filemarks;
    device_add_property(device_self, &prop, NULL);
    
    prop.access = PROPERTY_ACCESS_GET_MASK;
    prop.base = &device_property_canonical_name;
    device_add_property(device_self, &prop, NULL);
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
    device_class->property_get = tape_device_property_get;
    device_class->property_set = tape_device_property_set;
    device_class->finish = tape_device_finish;
    
    g_object_class->finalize = tape_device_finalize;
}

void tape_device_register(void) {
    static const char * device_prefix_list[] = { "tape", NULL };

    /* First register tape-specific properties */
    device_property_fill_and_register(&device_property_broken_gmt_online,
                                      G_TYPE_BOOLEAN, "broken_gmt_online",
      "Does this drive support the GMT_ONLINE macro?");

    device_property_fill_and_register(&device_property_fsf,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "fsf",
      "Does this drive support the MTFSF command?");

    device_property_fill_and_register(&device_property_bsf,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "bsf",
      "Does this drive support the MTBSF command?" );

    device_property_fill_and_register(&device_property_fsr,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "fsr",
      "Does this drive support the MTFSR command?");

    device_property_fill_and_register(&device_property_bsr,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "bsr",
      "Does this drive support the MTBSR command?");

    /* FIXME: Is this feature even useful? */
    device_property_fill_and_register(&device_property_eom,
                                      FEATURE_SUPPORT_FLAGS_TYPE, "eom",
      "Does this drive support the MTEOM command?");

    device_property_fill_and_register(&device_property_bsf_after_eom,
                                      FEATURE_SUPPORT_FLAGS_TYPE,
                                      "bsf_after_eom",
      "Does this drive require an MTBSF after MTEOM in order to append?" );

    device_property_fill_and_register(&device_property_final_filemarks,
                                      G_TYPE_UINT, "final_filemarks",
      "How many filemarks to write after the last tape file?" );

    /* Then the device itself */
    register_device(tape_device_factory, device_prefix_list);
}

/* Open the tape device, trying various combinations of O_RDWR and
   O_NONBLOCK.  Returns -1 and calls device_set_error for errors */
static int try_open_tape_device(TapeDevice * self, char * device_filename) {
    int fd;
    int save_errno;
    DeviceStatusFlags new_status;

#ifdef O_NONBLOCK
    fd  = robust_open(device_filename, O_RDWR | O_NONBLOCK, 0);
    save_errno = errno;
    if (fd < 0 && (save_errno == EWOULDBLOCK || save_errno == EINVAL)) {
        /* Maybe we don't support O_NONBLOCK for tape devices. */
        fd = robust_open(device_filename, O_RDWR, 0);
	save_errno = errno;
    }
#else
    fd = robust_open(device_filename, O_RDWR);
    save_errno = errno;
#endif
    if (fd >= 0) {
        self->write_open_errno = 0;
    } else {
        if (errno == EACCES || errno == EPERM) {
            /* Device is write-protected. */
            self->write_open_errno = errno;
#ifdef O_NONBLOCK
            fd = robust_open(device_filename, O_RDONLY | O_NONBLOCK, 0);
	    save_errno = errno;
            if (fd < 0 && (save_errno == EWOULDBLOCK || save_errno == EINVAL)) {
                fd = robust_open(device_filename, O_RDONLY, 0);
		save_errno = errno;
            }
#else
            fd = robust_open(device_filename, O_RDONLY);
	    save_errno = errno;
#endif
        }
    }
#ifdef O_NONBLOCK
    /* Clear O_NONBLOCK for operations from now on. */
    if (fd >= 0)
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
tape_device_open_device (Device * d_self, char * device_name G_GNUC_UNUSED,
			char * device_type G_GNUC_UNUSED, char * device_node) {
    TapeDevice * self;

    self = TAPE_DEVICE(d_self);

    self->fd = -1;
    self->private->device_filename = stralloc(device_node);

    /* Get tape drive/OS info */
    tape_device_set_capabilities(self);

    /* And verify the above. */
    g_assert(feature_support_flags_is_valid(self->fsf));
    g_assert(feature_support_flags_is_valid(self->bsf));
    g_assert(feature_support_flags_is_valid(self->fsr));
    g_assert(feature_support_flags_is_valid(self->bsr));
    g_assert(feature_support_flags_is_valid(self->eom));
    g_assert(feature_support_flags_is_valid(self->bsf_after_eom));
    g_assert(self->final_filemarks == 1 ||
             self->final_filemarks == 2);

    /* Chain up */
    if (parent_class->open_device) {
        parent_class->open_device(d_self, device_node, device_type, device_node);
    }
}

static DeviceStatusFlags tape_device_read_label(Device * dself) {
    TapeDevice * self;
    char * header_buffer;
    int buffer_len;
    IoResult result;
    dumpfile_t *header;
    DeviceStatusFlags new_status;

    self = TAPE_DEVICE(dself);

    amfree(dself->volume_label);
    amfree(dself->volume_time);
    amfree(dself->volume_header);

    if (device_in_error(self)) return dself->status;

    header = dself->volume_header = g_new(dumpfile_t, 1);
    fh_init(header);

    tape_validate_properties(self);

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
	    vstrallocf(_("Error rewinding device %s"), self->private->device_filename),
	      DEVICE_STATUS_DEVICE_ERROR
	    | DEVICE_STATUS_VOLUME_ERROR);
        robust_close(self->fd);
        return dself->status;
    }

    buffer_len = self->read_block_size;
    header_buffer = malloc(buffer_len);
    result = tape_device_robust_read(self, header_buffer, &buffer_len);

    if (result != RESULT_SUCCESS) {
        free(header_buffer);
        tape_rewind(self->fd);
        /* I/O error. */
        if (result == RESULT_NO_DATA) {
            new_status = (DEVICE_STATUS_VOLUME_ERROR |
	                  DEVICE_STATUS_VOLUME_UNLABELED);
        } else {
            new_status = (DEVICE_STATUS_DEVICE_ERROR |
	                  DEVICE_STATUS_VOLUME_ERROR |
	                  DEVICE_STATUS_VOLUME_UNLABELED);
        }
	device_set_error(dself, stralloc(_("Error reading Amanda header")), new_status);
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
tape_device_write_block(Device * pself, guint size,
                        gpointer data, gboolean short_block) {
    TapeDevice * self;
    char *replacement_buffer = NULL;
    IoResult result;

    self = TAPE_DEVICE(pself);

    g_assert(self->fd >= 0);
    if (device_in_error(self)) return FALSE;

    /* zero out to the end of a short block -- tape devices only write
     * whole blocks. */
    if (short_block && self->min_block_size > size) {
        replacement_buffer = malloc(self->min_block_size);
        memcpy(replacement_buffer, data, size);
        bzero(replacement_buffer+size, self->min_block_size-size);

        data = replacement_buffer;
        size = self->min_block_size;
    }

    result = tape_device_robust_write(self, data, size);
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
	case RESULT_ERROR:
	    device_set_error(pself,
		vstrallocf(_("Error writing block: %s"), strerror(errno)),
		DEVICE_STATUS_DEVICE_ERROR);
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
    
    self = TAPE_DEVICE(pself);

    g_assert(self->fd >= 0);
    if (device_in_error(self)) return -1;

    if (buf == NULL || *size_req < (int)self->read_block_size) {
        /* Just a size query. */
        *size_req = self->read_block_size;
        return 0;
    }

    size = *size_req;
    result = tape_device_robust_read(self, buf, &size);
    switch (result) {
    case RESULT_SUCCESS:
        *size_req = size;
        pself->block++;
        return size;
    case RESULT_SMALL_BUFFER: {
        int new_size;
        /* If this happens, it means that we have:
         *     (next block size) > (buffer size) >= (read_block_size)
         * The solution is to ask for an even bigger buffer. We also play
         * some games to refrain from reading above the SCSI limit or from
         * integer overflow. */
        new_size = MIN(INT_MAX/2 - 1, *size_req) * 2;
        if (new_size > LARGEST_BLOCK_ESTIMATE &&
            *size_req < LARGEST_BLOCK_ESTIMATE) {
            new_size = LARGEST_BLOCK_ESTIMATE;
        }
        g_assert (new_size > *size_req);
	*size_req = new_size;
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
	device_set_error(pself,
	    vstrallocf(_("Error reading from tape device: %s"), strerror(errno)),
	    DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
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
     int header_size;
     gboolean header_fits;
     Device * d_self = (Device*)self;
     tape_rewind(self->fd);
    
     header = make_tapestart_header(d_self, label, timestamp);
     g_assert(header != NULL);
     header_buf = device_build_amanda_header(d_self, header, &header_size,
                                             &header_fits);
     amfree(header);
     g_assert(header_buf != NULL);

     if (!header_fits) {
         amfree(header_buf);
	 device_set_error(d_self,
	    stralloc(_("Tapestart header won't fit in a single block!")),
	    DEVICE_STATUS_DEVICE_ERROR);
         return FALSE;
     }

     g_assert(header_size >= (int)self->min_block_size);
     result = tape_device_robust_write(self, header_buf, header_size);
     if (result == RESULT_SUCCESS) {
	amfree(header_buf);
	return TRUE;
     } else {
	 device_set_error(d_self,
	    vstrallocf(_("Error writing tapestart header: %s"), strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR);
	amfree(header_buf);
	return FALSE;
     }
}

static gboolean 
tape_device_start (Device * d_self, DeviceAccessMode mode, char * label,
                   char * timestamp) {
    TapeDevice * self;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return FALSE;

    tape_validate_properties(self);

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

    d_self->access_mode = mode;

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
        self->first_file = TRUE;
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
        self->first_file = TRUE;

        d_self->volume_label = newstralloc(d_self->volume_label, label);
        d_self->volume_time = newstralloc(d_self->volume_time, timestamp);

	/* unset the VOLUME_UNLABELED flag, if it was set */
	device_set_error(d_self, NULL, DEVICE_STATUS_SUCCESS);
        break;

    default:
        g_assert_not_reached();
    }

    return TRUE;
}

static gboolean tape_device_start_file(Device * d_self,
                                       const dumpfile_t * info) {
    TapeDevice * self;
    IoResult result;
    char * amanda_header;
    int header_size;
    gboolean header_fits;

    self = TAPE_DEVICE(d_self);

    g_assert(self->fd >= 0);
    if (device_in_error(self)) return FALSE;

    if (!(d_self->access_mode == ACCESS_APPEND && self->first_file)) {
        if (!tape_weof(self->fd, 1)) {
	    device_set_error(d_self,
		vstrallocf(_("Error writing filemark: %s"), strerror(errno)),
		DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
            return FALSE;
        }
    }

    self->first_file = FALSE;

    /* Make the Amanda header suitable for writing to the device. */
    /* Then write the damn thing. */
    amanda_header = device_build_amanda_header(d_self, info,
                                               &header_size, &header_fits);
    if (!header_fits) {
	device_set_error(d_self,
	    stralloc(_("Amanda file header won't fit in a single block!")),
	    DEVICE_STATUS_DEVICE_ERROR);
	return FALSE;
    }
    result = tape_device_robust_write(self, amanda_header, header_size);
    if (result != RESULT_SUCCESS) {
	device_set_error(d_self,
	    vstrallocf(_("Error writing file header: %s"), strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR);
	amfree(amanda_header);
        return FALSE;
    }
    amfree(amanda_header);

    /* arrange the file numbers correctly */
    d_self->in_file = TRUE;
    d_self->block = 0;
    if (d_self->file <= 0)
        d_self->file = 1;
    else
        d_self->file ++;
    return TRUE;
}

static gboolean
tape_device_finish_file (Device * d_self) {
    if (device_in_error(d_self)) return FALSE;

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

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return NULL;

    d_self->in_file = FALSE;
    d_self->is_eof = FALSE;
    d_self->block = 0;

    difference = file - d_self->file;

    /* Check if we already read a filemark. */
    if (d_self->is_eof) {
        difference --;
    }

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

    buffer_len = self->read_block_size;
    header_buffer = malloc(buffer_len);
    d_self->is_eof = FALSE;
    result = tape_device_robust_read(self, header_buffer, &buffer_len);

    if (result != RESULT_SUCCESS) {
        free(header_buffer);
        tape_rewind(self->fd);
        if (result == RESULT_NO_DATA) {
            /* If we read 0 bytes, that means we encountered a double
             * filemark, which indicates end of tape. This should
             * work even with QIC tapes on operating systems with
             * proper support. */
	    d_self->file = file; /* other attributes are already correct */
            return make_tapeend_header();
        }
        /* I/O error. */
	device_set_error(d_self,
	    stralloc(_("Error reading Amanda header")),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
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
		vstrallocf(_("Could not seek forward to block %ju"), (uintmax_t)block),
		DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
	}
    } else if (difference < 0) {
        if (!tape_device_bsr(self, difference, d_self->file, d_self->block)) {
	    device_set_error(d_self,
		vstrallocf(_("Could not seek backward to block %ju"), (uintmax_t)block),
		DEVICE_STATUS_VOLUME_ERROR | DEVICE_STATUS_DEVICE_ERROR);
            return FALSE;
	}
    }

    d_self->block = block;
    return TRUE;
}

/* Just checks that the flag is valid before setting it. */
static gboolean get_feature_flag(GValue * val, FeatureSupportFlags f) {
    if (feature_support_flags_is_valid(f)) {
        g_value_set_flags(val, f);
        return TRUE;
    } else {
        return FALSE;
    }
}

static gboolean 
tape_device_property_get (Device * d_self, DevicePropertyId id, GValue * val) {
    TapeDevice * self;
    const DevicePropertyBase * base;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(d_self)) return FALSE;

    /* clear error status in case we return FALSE */
    device_set_error(d_self, NULL, DEVICE_STATUS_SUCCESS);

    base = device_property_get_by_id(id);

    g_value_unset_init(val, base->type);

    if (id == PROPERTY_COMPRESSION) {
        g_value_set_boolean(val, self->compression);
        return TRUE;
    } else if (id == PROPERTY_MIN_BLOCK_SIZE) {
        g_value_set_uint(val, self->min_block_size);
        return TRUE;
    } else if (id == PROPERTY_MAX_BLOCK_SIZE) {
        g_value_set_uint(val, self->max_block_size);
        return TRUE;
    } else if (id == PROPERTY_BLOCK_SIZE) {
        if (self->fixed_block_size == 0) {
            g_value_set_int(val, -1);
        } else {
            g_value_set_int(val, self->fixed_block_size);
        }
        return TRUE;
    } else if (id == PROPERTY_BROKEN_GMT_ONLINE) {
        g_value_set_boolean(val, self->broken_gmt_online);
	return TRUE;
    } else if (id == PROPERTY_FSF) {
        return get_feature_flag(val, self->fsf);
    } else if (id == PROPERTY_BSF) {
        return get_feature_flag(val, self->bsf);
    } else if (id == PROPERTY_FSR) {
        return get_feature_flag(val, self->fsr);
    } else if (id == PROPERTY_BSR) {
        return get_feature_flag(val, self->bsr);
    } else if (id == PROPERTY_EOM) {
        return get_feature_flag(val, self->eom);
    } else if (id == PROPERTY_BSF_AFTER_EOM) {
        return get_feature_flag(val, self->bsf_after_eom);
    } else if (id == PROPERTY_FINAL_FILEMARKS) {
        g_value_set_uint(val, self->final_filemarks);
        return TRUE;
    } else {
        /* Chain up */
        if (parent_class->property_get) {
            return (parent_class->property_get)(d_self, id, val);
        } else {
            return FALSE;
        }
    }

    g_assert_not_reached();
}

/* We don't allow overriding of flags with _GOOD surety. That way, if
   e.g., a feature has no matching IOCTL on a given platform, we don't
   ever try to set it. */
static gboolean flags_settable(FeatureSupportFlags request,
                               FeatureSupportFlags existing) {
    if (!feature_support_flags_is_valid(request))
        return FALSE;
    else if (!feature_support_flags_is_valid(existing))
        return TRUE;
    else if (request == existing)
        return TRUE;
    else if (existing & FEATURE_SURETY_GOOD)
        return FALSE;
    else
        return TRUE;
}

/* If the access listed is NULL, and the provided flags can override the
   existing ones, then do it and return TRUE. */
static gboolean try_set_feature(DeviceAccessMode mode,
                                FeatureSupportFlags request,
                                FeatureSupportFlags * existing) {
    if (mode != ACCESS_NULL) {
        return FALSE;
    } else if (flags_settable(request, *existing)) {
        *existing = request;
        return TRUE;
    } else {
        return FALSE;
    }
}
 
static gboolean 
tape_device_property_set (Device * d_self, DevicePropertyId id, GValue * val) {
    TapeDevice * self;
    FeatureSupportFlags feature_request_flags = 0;
    const DevicePropertyBase * base;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return FALSE;

    base = device_property_get_by_id(id);

    g_assert(G_VALUE_HOLDS(val, base->type));

    if (base->type == FEATURE_SUPPORT_FLAGS_TYPE) {
        feature_request_flags = g_value_get_flags(val);

	/* assume the user is sure if not specified */
	if ((feature_request_flags & FEATURE_SUPPORT_FLAGS_SURETY_MASK) == 0)
	    feature_request_flags |= FEATURE_SURETY_GOOD;

	/* assume the user is the source if not specified */
	if ((feature_request_flags & FEATURE_SUPPORT_FLAGS_SOURCE_MASK) == 0)
	    feature_request_flags |= FEATURE_SOURCE_USER;

        if (!feature_support_flags_is_valid(feature_request_flags)) {
	    device_set_error(d_self,
		stralloc(_("Invalid feature support flags")),
		DEVICE_STATUS_DEVICE_ERROR);
	    return FALSE;
	}
    }

    if (id == PROPERTY_COMPRESSION) {
        /* We allow this property to be set at any time. This is mostly
         * because setting compression is a hit-and-miss proposition
         * at any time; some drives accept the mode setting but don't
         * actually support compression, while others do support
         * compression but do it via density settings or some other
         * way. Set this property whenever you want, but all we'll do
         * is report whether or not the ioctl succeeded. */
        gboolean request = g_value_get_boolean(val);
        if (tape_setcompression(self->fd, request)) {
            self->compression = request;
            device_clear_volume_details(d_self);
            return TRUE;
        } else {
            return FALSE;
        }
    } else if (id == PROPERTY_MIN_BLOCK_SIZE) {
        if (d_self->access_mode != ACCESS_NULL)
            return FALSE;
        self->min_block_size = g_value_get_uint(val);
        device_clear_volume_details(d_self);
        return TRUE;
    } else if (id == PROPERTY_MAX_BLOCK_SIZE) {
        if (d_self->access_mode != ACCESS_NULL)
            return FALSE;
        self->max_block_size = g_value_get_uint(val);
        device_clear_volume_details(d_self);
        return TRUE;
    } else if (id == PROPERTY_BLOCK_SIZE) {
        if (d_self->access_mode != ACCESS_NULL)
            return FALSE;

        self->fixed_block_size = g_value_get_int(val);
        device_clear_volume_details(d_self);
        return TRUE;
    } else if (id == PROPERTY_READ_BUFFER_SIZE) {
        if (d_self->access_mode != ACCESS_NULL)
            return FALSE;
        self->read_block_size = g_value_get_uint(val);
        device_clear_volume_details(d_self);
        return TRUE;
    } else if (id == PROPERTY_BROKEN_GMT_ONLINE) {
	self->broken_gmt_online = g_value_get_boolean(val);
	return TRUE;
    } else if (id == PROPERTY_FSF) {
        return try_set_feature(d_self->access_mode,
                               feature_request_flags,
                               &(self->fsf));
    } else if (id == PROPERTY_BSF) {
        return try_set_feature(d_self->access_mode,
                               feature_request_flags,
                               &(self->bsf));
    } else if (id == PROPERTY_FSR) {
        return try_set_feature(d_self->access_mode,
                               feature_request_flags,
                               &(self->fsr));
    } else if (id == PROPERTY_BSR) {
        return try_set_feature(d_self->access_mode,
                               feature_request_flags,
                               &(self->bsr));
    } else if (id == PROPERTY_EOM) {
        /* Setting this to disabled also clears BSF after EOM. */
        if (try_set_feature(d_self->access_mode,
                            feature_request_flags,
                            &(self->eom))) {
            feature_request_flags &= ~FEATURE_SUPPORT_FLAGS_STATUS_MASK;
            feature_request_flags |= FEATURE_STATUS_DISABLED;
            self->bsf_after_eom = feature_request_flags;
            return TRUE;
        } else {
            return FALSE;
        }
    } else if (id == PROPERTY_BSF_AFTER_EOM) {
        /* You can only set this if EOM is enabled. */
        if (self->bsf | FEATURE_STATUS_DISABLED)
            return FALSE;
        else
            return try_set_feature(d_self->access_mode,
                                   feature_request_flags,
                                   &(self->bsf_after_eom));
    } else if (id == PROPERTY_FINAL_FILEMARKS) {
        guint request = g_value_get_uint(val);
        if (request == 1 || request == 2) {
            self->final_filemarks = request;
            return TRUE;
        } else {
            return FALSE;
        }
    } else {
        /* Chain up */
        if (parent_class->property_set) {
            return (parent_class->property_set)(d_self, id, val);
        } else {
            return FALSE;
        }
    }

    g_assert_not_reached();
}

static gboolean 
tape_device_finish (Device * d_self) {
    TapeDevice * self;

    self = TAPE_DEVICE(d_self);

    if (device_in_error(self)) return FALSE;

    if (d_self->access_mode == ACCESS_NULL)
	return TRUE;

    /* Polish off this file, if relevant. */
    if (d_self->in_file && IS_WRITABLE_ACCESS_MODE(d_self->access_mode)) {
        if (!device_finish_file(d_self))
            return FALSE;
    }

    /* Write an extra filemark, if needed. The OS will give us one for
       sure. */
    if (self->final_filemarks > 1 &&
        IS_WRITABLE_ACCESS_MODE(d_self->access_mode)) {
        if (!tape_weof(self->fd, 1)) {
	device_set_error(d_self,
	    vstrallocf(_("Error writing final filemark: %s"), strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
            return FALSE;
        }
    }

    /* Rewind. */
    if (!tape_rewind(self->fd)) {
	device_set_error(d_self,
	    vstrallocf(_("Couldn't rewind device: %s"), strerror(errno)),
	    DEVICE_STATUS_DEVICE_ERROR);
        return FALSE;
    }

    d_self->access_mode = ACCESS_NULL;

    return TRUE;
}

/* Works just like read(), except for the following:
 * 1) Retries on EINTR & friends.
 * 2) Stores count in parameter, not return value.
 * 3) Provides explicit return result. */
static IoResult
tape_device_robust_read (TapeDevice * self, void * buf, int * count) {
    Device * d_self;
    int result;

    d_self = (Device*)self;

    /* Callers should ensure this. */
    g_assert(*count >= 0);
    g_assert((guint)(*count) <= self->read_block_size);

    for (;;) {
        result = read(self->fd, buf, *count);
        if (result > 0) {
            /* Success. By definition, we read a full block. */
            *count = result;
            return RESULT_SUCCESS;
        } else if (result == 0) {
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
            } else if ((self->fixed_block_size == 0) &&
                       (0
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
                return RESULT_SMALL_BUFFER;
            } else {
		device_set_error(d_self,
		    vstrallocf(_("Error reading %d bytes from %s: %s"),
				*count, self->private->device_filename, strerror(errno)),
		    DEVICE_STATUS_DEVICE_ERROR | DEVICE_STATUS_VOLUME_ERROR);
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

static IoResult 
tape_device_robust_write (TapeDevice * self, void * buf, int count) {
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
	    device_set_error(d_self,
		     vstrallocf(_("Mysterious short write on tape device: Tried %d, got %d"),
				count, result),
		    DEVICE_STATUS_DEVICE_ERROR);
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
	    device_set_error(d_self,
		    vstrallocf(_("Kernel gave unexpected write() result of \"%s\" on device %s"),
					strerror(errno), self->private->device_filename),
		    DEVICE_STATUS_DEVICE_ERROR);
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
    int buffer_size;
    int i;

    buffer_size = self->read_block_size;

    buffer = malloc(sizeof(buffer_size));

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
    if (self->fsf & FEATURE_STATUS_ENABLED) {
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
    if (self->bsf & FEATURE_STATUS_ENABLED) {
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
    if (self->fsr & FEATURE_STATUS_ENABLED) {
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
    if (self->bsr & FEATURE_STATUS_ENABLED) {
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
    d_self = (Device*)self;

    if (self->eom & FEATURE_STATUS_ENABLED) {
        int result;
        result = tape_eod(self->fd); 
        if (result == TAPE_OP_ERROR) {
            return FALSE;
        } else if (result == TAPE_POSITION_UNKNOWN) {
            d_self->file = -1;
        } else {
            /* We drop by 1 because Device will increment the first
               time the user does start_file. */
            d_self->file = result - 1;
        }
        return TRUE;
    } else {
        int count = 0;
        if (!tape_rewind(self->fd))
            return FALSE;
        
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
                d_self->file = count;
                return TRUE;
            } else {
                return FALSE;
            }
        }
    }
}

static void
tape_validate_properties(TapeDevice * self) {
    if (self->read_block_size < self->fixed_block_size)
	self->read_block_size = self->fixed_block_size;
    if (self->read_block_size < self->max_block_size)
	self->read_block_size = self->max_block_size;
}

static Device *
tape_device_factory (char * device_name, char * device_type, char * device_node) {
    Device * rval;
    g_assert(0 == strcmp(device_type, "tape"));
    rval = DEVICE(g_object_new(TYPE_TAPE_DEVICE, NULL));
    device_open_device(rval, device_name, device_type, device_node);
    return rval;
}
