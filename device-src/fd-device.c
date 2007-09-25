/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#define selfp (self->_priv)

#include "amanda.h"
#include "fd-device.h"
#include "timestamp.h"

/* here are local prototypes */
static void fd_device_init (FdDevice * o);
static void fd_device_class_init (FdDeviceClass * c);
static void fd_device_finalize(GObject * o);
static gboolean fd_device_read_label(Device * self);
static gboolean fd_device_start(Device * self, DeviceAccessMode mode,
                                char * label, char * timestamp);
static gboolean fd_device_finish(Device * self);
static gboolean fd_device_write_block (Device * self, guint size,
                                       gpointer data, gboolean last);
static gboolean fd_device_start_file (Device * self, const dumpfile_t * ji);
static dumpfile_t * fd_device_seek_file (Device * self, guint file);
static int fd_device_read_block (Device * self, gpointer buf, int * size);
static FdDeviceResult fd_device_robust_read (FdDevice * self, void * buf,
                                             int * count);
static FdDeviceResult default_fd_device_robust_read (FdDevice * self,
                                                     void * buf,
                                                     int * count);
static FdDeviceResult fd_device_robust_write (FdDevice * self, void * buf,
                                              int count);
static FdDeviceResult default_fd_device_robust_write (FdDevice * self,
                                                      void * buf,
                                                      int count);
static void fd_device_found_eof (FdDevice * self);
static void default_fd_device_found_eof(FdDevice * self);
static void fd_device_label_size_range(FdDevice * self, guint*, guint*);
static void default_fd_device_label_size_range(FdDevice * self,
                                                guint*, guint*);

/* pointer to the class of our parent */
static DeviceClass *parent_class = NULL;

GType
fd_device_get_type (void) {
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (FdDeviceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) fd_device_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (FdDevice),
            0 /* n_preallocs */,
            (GInstanceInitFunc) fd_device_init,
            NULL
        };
        
        type = g_type_register_static (TYPE_DEVICE, "FdDevice", &info,
                                       (GTypeFlags)G_TYPE_FLAG_ABSTRACT);
    }
    
    return type;
}

static void 
fd_device_init (FdDevice * o G_GNUC_UNUSED)
{
	o->fd = -1;
}

static void 
fd_device_class_init (FdDeviceClass * c G_GNUC_UNUSED)
{
    DeviceClass *device_class = (DeviceClass *)c;
    GObjectClass * g_object_class = (GObjectClass*)c;

    parent_class = g_type_class_ref (TYPE_DEVICE);

    device_class->write_block = fd_device_write_block;
    device_class->start_file = fd_device_start_file;
    device_class->read_label = fd_device_read_label;
    device_class->start = fd_device_start;
    device_class->finish = fd_device_finish;
    device_class->seek_file = fd_device_seek_file;
    device_class->read_block = fd_device_read_block;

    c->robust_read = default_fd_device_robust_read;
    c->robust_write = default_fd_device_robust_write;
    c->found_eof = default_fd_device_found_eof;
    c->label_size_range = default_fd_device_label_size_range;

    g_object_class->finalize = fd_device_finalize;
}   

static void fd_device_finalize(GObject * obj_self) {
    FdDevice *self;
    Device * d_self;

    self = FD_DEVICE (obj_self);
    g_return_if_fail(self != NULL);
    d_self = (Device*)self;

    if (d_self->access_mode != ACCESS_NULL) {
        device_finish(d_self);
    }

    if (self->fd >= 0) {
        aclose(self->fd);
    }

    /* Chain up */
    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

}

/* Just a small helper function */
static gboolean fd_device_read_label(Device * pself) {
    FdDevice * self;
    char * buf;
    guint buf_size;
    int buf_size_int;
    dumpfile_t amanda_header;
    FdDeviceResult read_result;

    self = FD_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);

    if (self->fd < 0)
        return FALSE;

    fd_device_label_size_range(self, NULL, &buf_size);
    buf = malloc(buf_size);
    buf_size_int = (int)buf_size;
    read_result = fd_device_robust_read(self, buf, &buf_size_int);
    if (read_result != RESULT_SUCCESS)
        return FALSE;

    fh_init(&amanda_header);
    parse_file_header(buf, &amanda_header, buf_size_int);
    if (amanda_header.type != F_TAPESTART) {
        fprintf(stderr, "Got a bad volume label:\n%s\n", buf);
        amfree(buf);
        return FALSE;
    }
    amfree(buf);
    pself->volume_label = strdup(amanda_header.name);
    pself->volume_time = strdup(amanda_header.datestamp);
    
    return TRUE;
}

static gboolean 
fd_device_write_block (Device * pself, guint size,
                       gpointer data, gboolean short_block)
{
    FdDevice * self;
    guint min_block_size;
    gpointer replacement_buffer = NULL;
    self = FD_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);
   
    min_block_size = device_write_min_size(pself);
    if (short_block && min_block_size > size) {
        replacement_buffer = malloc(min_block_size);
        memcpy(replacement_buffer, data, size);
        bzero(replacement_buffer+size, min_block_size-size);
        
        data = replacement_buffer;
        size = min_block_size;
    }

    if (self->fd >= 0) {
        FdDeviceResult result;
        result = fd_device_robust_write(self, data, size);
        if (result == RESULT_SUCCESS) {
            if (parent_class->write_block) {
                (parent_class->write_block)(pself, size, data, short_block);
            }
            amfree(replacement_buffer);
            return TRUE;
        } else {
            amfree(replacement_buffer);
            return FALSE;
        }
    }
    
    amfree(replacement_buffer);
    return FALSE;
}


static int 
fd_device_read_block (Device * pself, gpointer buf,
                      int * size_req) {
    FdDevice * self;
    int size;
    FdDeviceResult result;
    
    self = FD_DEVICE(pself);
    g_return_val_if_fail (self != NULL, -1);

    if (buf == NULL) {
        /* Just a size query. */
        *size_req = device_read_max_size(pself);
        return 0;
    }

    size = *size_req;
    result = fd_device_robust_read(self, buf, &size);
    switch (result) {
    case RESULT_SUCCESS:
        *size_req = size;
        return size;
    case RESULT_SMALL_BUFFER:
        *size_req = device_read_max_size(pself);
        return 0;
    case RESULT_NO_DATA:
        pself->is_eof = TRUE;
        fd_device_found_eof(self);
        /* FALLTHROUGH */
    default:
        return -1;
    }

    g_assert_not_reached();
}

/* Just a small helper function */
static gboolean fd_device_start_chain_up(Device * self,
                                     MediaAccessMode mode,
                                     char * label, char * timestamp) {
    if (parent_class->start) {
        return parent_class->start(self, mode, label, timestamp);
    } else {
        return TRUE;
    }
}

static char * make_fd_device_tapestart_header(FdDevice * self, char * label,
                                              char * timestamp, int * size) {
    dumpfile_t * dumpinfo;
    guint min_header_length, max_header_length, header_buffer_size;
    char * rval;
    Device * pself = (Device*)self;

    dumpinfo = make_tapestart_header(pself, label, timestamp);

    fd_device_label_size_range(self, &min_header_length, &max_header_length);

    rval = build_header(dumpinfo, min_header_length);
    amfree(dumpinfo);
    header_buffer_size = MAX(min_header_length, strlen(rval)+1);

    if (header_buffer_size > max_header_length) {
        amfree(rval);
        return NULL;
    }

    if (size != NULL)
        *size = header_buffer_size;

    return rval;
}

static gboolean
fd_device_start (Device * pself, DeviceAccessMode mode, char * label,
                 char * timestamp) {
    FdDevice * self;
    /* We don't need to read the label here (we did that already in 
     * fd_device_open_device), but we may need to write it. */
    self = FD_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);

    if (mode != ACCESS_WRITE) {
        /* I have nothing to say. */
        return fd_device_start_chain_up(pself, mode, label, timestamp);
    }

    if (self->fd < 0) {
        /* Nothing to do; label will be written by other means. */
        return fd_device_start_chain_up(pself, mode, label, timestamp);
    } else {
        FdDeviceResult result;
        char * amanda_header;
        int header_size;

        amanda_header = make_fd_device_tapestart_header(self, label, timestamp,
                                                        &header_size);

        if (amanda_header == NULL) {
            fprintf(stderr, "Tapestart header won't fit in a single block!\n");
            return FALSE;
        }
        result = fd_device_robust_write(self, amanda_header, header_size);
        amfree(amanda_header);
        if (result == RESULT_SUCCESS) {
            return fd_device_start_chain_up(pself, mode, label, timestamp);
        } else {
            return FALSE;
        }
    }
}

static gboolean
fd_device_finish (Device * pself) {
    FdDevice * self;
    FdDeviceResult result;
    char * amanda_header;
    int header_size;
    gboolean header_fits;
    dumpfile_t * dumpinfo;

    self = FD_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);

    if (self->fd >= 0 && IS_WRITABLE_ACCESS_MODE(pself->access_mode)) {
        dumpinfo = make_tapeend_header();
        amanda_header = device_build_amanda_header(pself, dumpinfo,
                                                   &header_size, &header_fits);
        amfree(dumpinfo);
        g_return_val_if_fail(amanda_header != NULL, FALSE);
        if (!header_fits) {
            fprintf(stderr, "Tape-end header won't fit in a single block!\n");
            amfree(amanda_header);
            return FALSE;
        }
        result = fd_device_robust_write(self, amanda_header, header_size);
        amfree(amanda_header);
        if (result != RESULT_SUCCESS)
            return FALSE;
    }

    if (parent_class->finish)
        (parent_class->finish)(pself);
    
    return TRUE;
}

static gboolean 
fd_device_start_file (Device * pself, const dumpfile_t * ji) {
    FdDevice * self;
    FdDeviceResult result;
    char * amanda_header;
    int header_size;
    gboolean header_fits;
    self = FD_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);
    g_return_val_if_fail (self->fd >= 0, FALSE);

    /* Make the Amanda header suitable for writing to the device. */
    /* Then write the damn thing. */
    amanda_header = device_build_amanda_header(pself, ji,
                                               &header_size, &header_fits);
    g_return_val_if_fail(amanda_header != NULL, FALSE);
    g_return_val_if_fail(header_fits, FALSE);
    result = fd_device_robust_write(self, amanda_header, header_size);
    amfree(amanda_header);
    if (result == RESULT_SUCCESS) {
        /* Chain up. */
        if (parent_class->start_file) {
            parent_class->start_file(pself, ji);
        }
        return TRUE;
    } else {
        return FALSE;
    }
} 

static dumpfile_t * 
fd_device_seek_file (Device * pself, guint file) {
    FdDevice * self;
    char * buffer;
    dumpfile_t * rval;
    int buffer_len;
    FdDeviceResult result;
    self = FD_DEVICE(pself);
    g_return_val_if_fail (self != NULL, FALSE);

    /* By the time we get here, we assume all the seeking has already been
     * done. We just read a block, and get the header. */
    buffer_len = device_read_max_size(pself);
    buffer = malloc(buffer_len);
    result = fd_device_robust_read(self, buffer, &buffer_len);
    if (result != RESULT_SUCCESS) {
        fprintf(stderr, "Problem reading Amanda header.\n");
        free(buffer);
        return FALSE;
    }
    
    rval = malloc(sizeof(*rval));
    parse_file_header(buffer, rval, buffer_len);
    free(buffer);
    switch (rval->type) {
    case F_DUMPFILE:
    case F_CONT_DUMPFILE:
    case F_SPLIT_DUMPFILE:
        /* Chain up. */
        if (parent_class->seek_file) {
            parent_class->seek_file(pself, file);
        }
        return rval;
    default:
        amfree(rval);
        return NULL;
    }
}

/* Default implementations of FdDevice virtual functions */

/* These default implementations are good for any file descriptor that
 * doesn't have special semantics: That is, regular files, FIFOs,
 * pipes (named and otherwise), sockets, etc., but not tapes. We
 * enforce a fixed block size on top of the file (which is itself
 * inherently unblocked). */
static FdDeviceResult default_fd_device_robust_read(FdDevice * self, void *buf,
                                         int *count) {
    int fd = self->fd;
    int want = *count, got = 0;

    while (got < want) {
        int result;
        result = read(fd, buf + got, want - got);
        if (result > 0) {
            got += result;
        } else if (result == 0) {
            /* end of file */
            if (got == 0) {
                return RESULT_NO_DATA;
            } else {
                *count = got;
                return RESULT_SUCCESS;
            }
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
            /* Try again. */
            continue;
        } else {
            /* Error occured. */
            fprintf(stderr, "Error reading fd %d: %s\n", fd, strerror(errno));
            *count = got;
            return -1;
        }
    }

    *count = got;
    return RESULT_SUCCESS;
}

static FdDeviceResult default_fd_device_robust_write(FdDevice * self,
                                                     void *buf,
                                                     int count) {
    int fd = self->fd;
    int rval = 0;

    while (rval < count) {
        int result;
        result = write(fd, buf + rval, count - rval);
        if (result > 0) {
            rval += result;
            continue;
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
            /* Try again. */
            continue;
        } else if (0
#ifdef EFBIG
                   || errno == EFBIG
#endif
#ifdef ENOSPC
                   || errno == ENOSPC
#endif
                   ) {
            /* We are definitely out of space. */
            return RESULT_NO_SPACE;
        } else {
            /* Error occured. Note that here we handle EIO as an error. */
            fprintf(stderr, "Error writing device fd %d: %s\n",
                    fd, strerror(errno));
            
            return RESULT_ERROR;
        }
    }
    return RESULT_SUCCESS;
}

static void default_fd_device_found_eof(FdDevice * self) {
    close(self->fd);
    self->fd = -1;
}

static void default_fd_device_label_size_range(FdDevice * self,
                                               guint * min, guint * max) {
    Device * dself = DEVICE(self);
    if (min) {
        *min = device_write_min_size(dself);
    }
    if (max) {
        *max = device_write_max_size(dself);
    }
}

/* XXX WARNING XXX
 * All the functions below this comment are stub functions that do nothing
 * but implement the virtual function table. Call these functions and they
 * will do what you expect vis-a-vis virtual functions. But don't put code
 * in them beyond error checking and VFT lookup. */

static FdDeviceResult
fd_device_robust_read (FdDevice * self, void * buf, int * count)
{
        FdDeviceClass *klass;
        g_assert (self != NULL);
        g_assert (IS_FD_DEVICE (self));
        g_assert (buf != NULL);
        g_assert (count != NULL);
        g_assert (*count > 0);
        klass = FD_DEVICE_GET_CLASS(self);

        if(klass->robust_read)
                return (*klass->robust_read)(self,buf,count);
        else
                return (FdDeviceResult )(0);
}

static FdDeviceResult
fd_device_robust_write (FdDevice * self, void * buf, int count)
{
        FdDeviceClass *klass;
        g_assert (self != NULL);
        g_assert (IS_FD_DEVICE (self));
        g_assert (buf != NULL);
        g_assert (count > 0);
        klass = FD_DEVICE_GET_CLASS(self);

        if(klass->robust_write)
                return (*klass->robust_write)(self,buf,count);
        else
                return (FdDeviceResult )(0);
}

static void
fd_device_found_eof (FdDevice * self) {
        FdDeviceClass *klass;
        g_assert (self != NULL);
        g_assert (IS_FD_DEVICE (self));

        klass = FD_DEVICE_GET_CLASS(self);

        if(klass->found_eof)
            (*klass->found_eof)(self);
}

static void fd_device_label_size_range(FdDevice * self,
                                        guint * min, guint * max) {
        FdDeviceClass *klass;
        g_assert (self != NULL);
        g_assert (IS_FD_DEVICE (self));

        klass = FD_DEVICE_GET_CLASS(self);

        if(klass->label_size_range)
            (*klass->label_size_range)(self, min, max);
}
