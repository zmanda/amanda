/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2005-2008 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#define selfp (self->_priv)

#include "taper-disk-port-source.h"
#include "taper-mem-port-source.h"

struct _TaperDiskPortSourcePrivate {
    gboolean retry_mode;
    int buffer_fd;
    TaperSource * fallback;
    gboolean disk_problem;
    guint64 disk_buffered_bytes;
    
    /* This is for extra data we picked up by accident. */
    char * excess_buffer;
    size_t excess_buffer_size;

    guint64 retry_data_written;
};

/* here are local prototypes */
static void taper_disk_port_source_init (TaperDiskPortSource * o);
static void taper_disk_port_source_class_init (TaperDiskPortSourceClass * c);
static ssize_t taper_disk_port_source_read (TaperSource * pself, void * buf,
                                            size_t count);
static gboolean taper_disk_port_source_seek_to_part_start
    (TaperSource * pself);
static void taper_disk_port_source_start_new_part (TaperSource * pself);
static gboolean taper_disk_port_source_get_end_of_data(TaperSource * pself);
static gboolean taper_disk_port_source_get_end_of_part(TaperSource * pself);
static int taper_disk_port_source_predict_parts(TaperSource * pself);

/* pointer to the class of our parent */
static TaperSourceClass * source_parent_class = NULL;
static TaperPortSourceClass *parent_class = NULL;

GType taper_disk_port_source_get_type (void) {
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (TaperDiskPortSourceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) taper_disk_port_source_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (TaperDiskPortSource),
            0 /* n_preallocs */,
            (GInstanceInitFunc) taper_disk_port_source_init,
            NULL
        };
        
        type = g_type_register_static (TAPER_TYPE_PORT_SOURCE,
                                       "TaperDiskPortSource", &info,
                                       (GTypeFlags)0);
    }
    
    return type;
}

static void taper_disk_port_source_dispose(GObject * obj_self) {
    TaperDiskPortSource *self = TAPER_DISK_PORT_SOURCE (obj_self);
    if (G_OBJECT_CLASS (parent_class)->dispose)
        (* G_OBJECT_CLASS (parent_class)->dispose) (obj_self);
    
    if(self->_priv->fallback) {
        g_object_unref (self->_priv->fallback);
    }
}

static void
taper_disk_port_source_finalize(GObject *obj_self)
{
    TaperDiskPortSource *self = TAPER_DISK_PORT_SOURCE (obj_self);
    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);

    amfree(self->buffer_dir_name);
    amfree(self->_priv->excess_buffer);
    amfree(self->_priv);
}

static void 
taper_disk_port_source_init (TaperDiskPortSource * o G_GNUC_UNUSED)
{
        o->_priv = malloc(sizeof(TaperDiskPortSourcePrivate));
	o->_priv->retry_mode = FALSE;
        o->_priv->buffer_fd = -1;
	o->_priv->fallback = NULL;
        o->_priv->disk_problem = FALSE;
        o->_priv->disk_buffered_bytes = 0;
        o->_priv->excess_buffer = NULL;
        o->_priv->excess_buffer_size = 0;
}

static void 
taper_disk_port_source_class_init (TaperDiskPortSourceClass * c G_GNUC_UNUSED)
{
    GObjectClass *g_object_class = (GObjectClass*) c;
    TaperSourceClass *taper_source_class = (TaperSourceClass *)c;
    
    parent_class = g_type_class_ref (TAPER_TYPE_PORT_SOURCE);
    source_parent_class = (TaperSourceClass*)parent_class;
    
    taper_source_class->read = taper_disk_port_source_read;
    taper_source_class->seek_to_part_start =
        taper_disk_port_source_seek_to_part_start;
    taper_source_class->start_new_part = taper_disk_port_source_start_new_part;
    taper_source_class->get_end_of_data =
        taper_disk_port_source_get_end_of_data;
    taper_source_class->get_end_of_part =
        taper_disk_port_source_get_end_of_part;
    taper_source_class->predict_parts = taper_disk_port_source_predict_parts;

    g_object_class->dispose = taper_disk_port_source_dispose;
    g_object_class->finalize = taper_disk_port_source_finalize;
}


static gboolean taper_disk_port_source_get_end_of_data(TaperSource * pself) {
    TaperDiskPortSource * self = TAPER_DISK_PORT_SOURCE(pself);
    g_return_val_if_fail(self != NULL, TRUE);

    if (self->_priv->fallback != NULL) {
        return taper_source_get_end_of_data(self->_priv->fallback);
    } else {
        return (source_parent_class->get_end_of_data)(pself);
    }
}
static gboolean taper_disk_port_source_get_end_of_part(TaperSource * pself) {
    TaperDiskPortSource * self = TAPER_DISK_PORT_SOURCE(pself);
    g_return_val_if_fail(self != NULL, TRUE);

    if (self->_priv->fallback != NULL) {
        return taper_source_get_end_of_part(self->_priv->fallback);
    } else {
        return (source_parent_class->get_end_of_part)(pself);
    }
}
  
static int taper_disk_port_source_predict_parts(TaperSource * pself) {
    TaperDiskPortSource * self = TAPER_DISK_PORT_SOURCE(pself);
    g_return_val_if_fail(self != NULL, -1);

    return -1;
}

static TaperSource * make_fallback_source(TaperDiskPortSource * self) {
    TaperSource * rval;
    TaperPortSource * port_rval;
    rval = (TaperSource*)
        g_object_new(TAPER_TYPE_MEM_PORT_SOURCE, NULL);
    port_rval = (TaperPortSource*)rval;
    
    if (rval == NULL)
        return NULL;

    port_rval->socket_fd = ((TaperPortSource*)self)->socket_fd;
    rval->max_part_size = self->fallback_buffer_size;

    return rval;
}

/* Open the buffer file. We create the file and then immediately
   unlink it, to improve security and ease cleanup. */
static gboolean open_buffer_file(TaperDiskPortSource * self) {
    int fd;
    char * filename;
    mode_t old_umask;
    TaperSource * pself = (TaperSource *)self;

    g_return_val_if_fail(self != NULL, FALSE);
    g_return_val_if_fail(self->buffer_dir_name != NULL, FALSE);

    filename = g_strdup_printf("%s/amanda-split-buffer-XXXXXX",
                               self->buffer_dir_name);
    /* This is not thread-safe. :-( */
    old_umask = umask(0);
    fd = g_mkstemp(filename);
    umask(old_umask);
    if (fd < 0) {
	pself->errmsg = newvstrallocf(pself->errmsg,
        	"Couldn't open temporary file with template %s: %s",
                filename, strerror(errno));
        return FALSE;
    }

    /* If it fails, that's annoying, but no great loss. */
    if (unlink(filename) != 0) {
        g_fprintf(stderr, "Unlinking %s failed: %s\n", filename,
                strerror(errno));
    }

    free(filename);
    selfp->buffer_fd = fd;
    return TRUE;
}

/* An error has occured with the disk buffer; store the extra data in
   memory until we can recover. */
static void store_excess(TaperDiskPortSource * self, char * buf,
                         size_t attempted_size, size_t disk_size) {
    TaperSource * pself = (TaperSource*)self;
    g_return_if_fail(attempted_size > 0);
    g_return_if_fail(disk_size < attempted_size);
    g_return_if_fail(buf != NULL);
    g_return_if_fail(selfp->excess_buffer == NULL);

    selfp->excess_buffer_size = attempted_size - disk_size;
    selfp->excess_buffer = malloc(selfp->excess_buffer_size);
    memcpy(selfp->excess_buffer, buf + disk_size, attempted_size - disk_size);

    selfp->disk_buffered_bytes += disk_size;
    pself->max_part_size = MIN(pself->max_part_size,
                               selfp->disk_buffered_bytes);
}

/* Handle the output of the small amount of saved in-memory data. */
static size_t handle_excess_buffer_read(TaperDiskPortSource * self,
                                        void * buf, size_t count) {
    guint64 offset;

    /* First, do we have anything left? */
    if (selfp->retry_data_written >=
        (selfp->disk_buffered_bytes + selfp->excess_buffer_size)) {
        return 0;
    }
    
    count = MIN(count,
                (selfp->disk_buffered_bytes + selfp->excess_buffer_size)
                - selfp->retry_data_written);

    offset = selfp->disk_buffered_bytes + selfp->excess_buffer_size
        - selfp->retry_data_written;
    g_assert(offset + count <= selfp->excess_buffer_size);
    memcpy(buf, selfp->excess_buffer + offset, count);

    selfp->retry_data_written += count;

    return count;
}
    
/* Write data out to the disk buffer, and handle any problems that
   crop up along the way. */
static ssize_t write_disk_buffer(TaperDiskPortSource * self, char * buf,
                                 size_t read_size) {
    size_t bytes_written = 0;
    while (bytes_written < read_size) {
        int write_result = write(selfp->buffer_fd, buf + bytes_written,
                                 read_size - bytes_written);
        if (write_result > 0) {
            bytes_written += write_result;
            continue;
        } else if (write_result == 0) {
            g_fprintf(stderr, "Writing disk buffer: Wrote 0 bytes.\n");
            continue;
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
                /* Out of space */
                store_excess(self, buf, read_size, bytes_written);
                return read_size;
            } else {
                /* I/O error. */
                store_excess(self, buf, read_size, bytes_written);
                selfp->disk_problem = TRUE;
                TAPER_SOURCE(self)->end_of_part = TRUE;
                return read_size;
            }
        }
        g_assert_not_reached();
    }

    selfp->disk_buffered_bytes += bytes_written;
    return read_size;
}

static ssize_t 
taper_disk_port_source_read (TaperSource * pself, void * buf, size_t count) {
    TaperDiskPortSource * self = (TaperDiskPortSource*)pself;
    int read_result;
    int result;

    g_return_val_if_fail (self != NULL, -1);
    g_return_val_if_fail (TAPER_IS_DISK_PORT_SOURCE (self), -1);
    g_return_val_if_fail (buf != NULL, -1);
    g_return_val_if_fail (count > 0, -1);
    g_assert(selfp->disk_buffered_bytes <= pself->max_part_size);
	
    if (selfp->fallback != NULL) {
        return taper_source_read(selfp->fallback, buf, count);
    } else if (selfp->buffer_fd < 0) {
        if (!open_buffer_file(self)) {
            /* Buffer file failed; go immediately to failover mode. */
            selfp->fallback = make_fallback_source(self);
            if (selfp->fallback != NULL) {
                return taper_source_read(selfp->fallback, buf, count);
            } else {
                /* Even the fallback source failed! */
                return -1;
            }
        }
    }

    if (selfp->retry_mode) {
        /* Read from disk buffer. */

        if (selfp->retry_data_written < selfp->disk_buffered_bytes) {
            /* Read from disk. */
            count = MIN(count, selfp->disk_buffered_bytes -
                               selfp->retry_data_written);
            result = read(selfp->buffer_fd, buf, count);
            if (result <= 0) {
                /* This should not happen. */
                return -1;
            } else {
                selfp->retry_data_written += result;
                return result;
            }
        } else if (selfp->excess_buffer != NULL) {
            /* We are writing out the last bit of buffer. Handle that. */
            result = handle_excess_buffer_read(self, buf, count);
	    if (result) {
		return result;
	    }
        }

	/* No more cached data -- start reading from the part again */
	selfp->retry_mode = FALSE;
    }

    /* Read from port. */
    count = MIN(count, pself->max_part_size - selfp->disk_buffered_bytes);
    if (count == 0) /* It was nonzero before. */ {
	pself->end_of_part = TRUE;
	return 0;
    }

    read_result = source_parent_class->read(pself, buf, count);
    /* Parent handles EOF and other goodness. */
    if (read_result <= 0) {
	return read_result;
    }

    /* Now write to disk buffer. */
    return write_disk_buffer(self, buf, read_result);
}

/* Try seeking back to byte 0. If that fails, then we mark ourselves
   as having a disk problem. Returns FALSE in that case. */
static gboolean try_rewind(TaperDiskPortSource * self) {
    gint64 result;
    TaperSource * pself = (TaperSource *)self;
    result = lseek(selfp->buffer_fd, 0, SEEK_SET);
    if (result != 0) {
	pself->errmsg = newvstrallocf(pself->errmsg,
        	"Couldn't seek split buffer: %s", strerror(errno));
        selfp->disk_problem = TRUE;
        return FALSE;
    } else {
        return TRUE;
    }
}

static gboolean 
taper_disk_port_source_seek_to_part_start (TaperSource * pself) {
    TaperDiskPortSource * self = TAPER_DISK_PORT_SOURCE(pself);

    if (self->_priv->fallback != NULL) {
        return taper_source_seek_to_part_start(selfp->fallback);
    }

    if (selfp->disk_problem && selfp->disk_buffered_bytes) {
        /* The disk buffer is screwed; nothing to do. */
        return FALSE;
    }

    if (!selfp->disk_problem) {
        if (!try_rewind(self)) {
            return FALSE;
        }
    }

    selfp->retry_mode = TRUE;
    selfp->retry_data_written = 0;

    if (source_parent_class->seek_to_part_start) {
        return source_parent_class->seek_to_part_start(pself);
    } else {
        return TRUE;
    }
}

static void 
taper_disk_port_source_start_new_part (TaperSource * pself) {
    TaperDiskPortSource * self = (TaperDiskPortSource*)pself;
    g_return_if_fail (self != NULL);
    g_return_if_fail (TAPER_IS_DISK_PORT_SOURCE (pself));
	
    if (self->_priv->fallback != NULL) {
        taper_source_start_new_part(self->_priv->fallback);
        return;
    }

    selfp->retry_mode = FALSE;
    if (!selfp->disk_problem) {
        try_rewind(self); /* If this fails it will set disk_problem to
                             TRUE. */
    }

    if (selfp->disk_problem && selfp->fallback == NULL) {
        selfp->fallback = make_fallback_source(self);
    }
    selfp->disk_buffered_bytes = 0;
    amfree(selfp->excess_buffer);
    selfp->excess_buffer_size = selfp->retry_data_written = 0;

    if (source_parent_class->start_new_part) {
        source_parent_class->start_new_part(pself);
    }
}
