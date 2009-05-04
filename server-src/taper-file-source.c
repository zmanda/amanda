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

#include "taper-file-source.h"

#include "fileheader.h"
#include "holding.h"

#define HOLDING_DISK_OPEN_FLAGS (O_NOCTTY | O_RDONLY)

struct _TaperFileSourcePrivate {
    /* How many bytes have we written from the current part? */
    guint64 current_part_pos;
    /* Information about the files at the start of this part. */
    dumpfile_t part_start_chunk_header;
    int part_start_chunk_fd;
    /* Where is the start of this part with respect to the first chunk
       of the part? */
    guint64 part_start_chunk_offset;
    /* These may be the same as their part_start_chunk_ counterparts. */
    dumpfile_t current_chunk_header;
    int current_chunk_fd;
    /* Current position of current_chunk_fd. */
    guint64 current_chunk_position;
    /* Expected number of split parts. */
    int predicted_splits;
};
/* here are local prototypes */
static void taper_file_source_init (TaperFileSource * o);
static void taper_file_source_class_init (TaperFileSourceClass * c);
static ssize_t taper_file_source_read (TaperSource * pself, void * buf,
                                            size_t count);
static gboolean taper_file_source_seek_to_part_start (TaperSource * pself);
static void taper_file_source_start_new_part (TaperSource * pself);
static int taper_file_source_predict_parts(TaperSource * pself);
static dumpfile_t * taper_file_source_get_first_header(TaperSource * pself);
static gboolean first_time_setup(TaperFileSource * self);

/* pointer to the class of our parent */
static TaperSourceClass *parent_class = NULL;

GType taper_file_source_get_type (void) {
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (TaperFileSourceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) taper_file_source_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (TaperFileSource),
            0 /* n_preallocs */,
            (GInstanceInitFunc) taper_file_source_init,
            NULL
        };
        
        type = g_type_register_static (TAPER_SOURCE_TYPE, "TaperFileSource",
                                       &info, (GTypeFlags)0);
    }
    
    return type;
}

static void
taper_file_source_finalize(GObject *obj_self)
{
    TaperFileSource *self = TAPER_FILE_SOURCE (obj_self);
    gpointer priv G_GNUC_UNUSED = self->_priv;
    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);
    if(self->_priv->part_start_chunk_fd >= 0) {
        close (self->_priv->part_start_chunk_fd);
    }
    if(self->_priv->current_chunk_fd >= 0) {
        close (self->_priv->current_chunk_fd);
    }
    dumpfile_free_data(&(self->_priv->part_start_chunk_header));
    dumpfile_free_data(&(self->_priv->current_chunk_header));
    amfree(self->_priv);
}

static void 
taper_file_source_init (TaperFileSource * o G_GNUC_UNUSED)
{
    o->_priv = malloc(sizeof(TaperFileSourcePrivate));
    o->_priv->part_start_chunk_fd = -1;
    o->_priv->current_chunk_fd = -1;
    o->_priv->predicted_splits = -1;
    fh_init(&o->_priv->part_start_chunk_header);
    fh_init(&o->_priv->current_chunk_header);
    o->holding_disk_file = NULL;
}

static void  taper_file_source_class_init (TaperFileSourceClass * c) {
    GObjectClass *g_object_class = (GObjectClass*) c;
    TaperSourceClass *taper_source_class = (TaperSourceClass *)c;

    parent_class = g_type_class_ref (TAPER_SOURCE_TYPE);

    taper_source_class->read = taper_file_source_read;
    taper_source_class->seek_to_part_start =
        taper_file_source_seek_to_part_start;
    taper_source_class->start_new_part = taper_file_source_start_new_part;
    taper_source_class->get_first_header = taper_file_source_get_first_header;
    taper_source_class->predict_parts = taper_file_source_predict_parts;

    g_object_class->finalize = taper_file_source_finalize;
}

static void compute_splits(TaperFileSource * self) {
    guint64 total_kb;
    int predicted_splits;
    TaperSource * pself = (TaperSource*)self;

    if (selfp->predicted_splits > 0) {
        return;
    }

    if (pself->max_part_size <= 0) {
        selfp->predicted_splits = 1;
        return;
    }

    total_kb = holding_file_size(self->holding_disk_file, TRUE);
    if (total_kb <= 0) {
        g_fprintf(stderr, "taper: %lld KB holding file makes no sense, not precalculating splits\n",
		(long long)total_kb);
        fflush(stderr);
        selfp->predicted_splits = -1;
        return;
    }
    
    g_fprintf(stderr, "taper: Total dump size should be %jukb, part size is %ju bytes\n",
            (uintmax_t)total_kb, (uintmax_t)pself->max_part_size);

    /* always add one here; if the max_part_size evenly divides the total
     * dump size, taper will write an empty final part */
    predicted_splits = (total_kb * 1024) / pself->max_part_size + 1;
    g_fprintf(stderr, "taper: predicting %d split parts\n", predicted_splits);
    selfp->predicted_splits = predicted_splits;
}

static int taper_file_source_predict_parts(TaperSource * pself) {
    TaperFileSource * self = TAPER_FILE_SOURCE(pself);
    g_return_val_if_fail(self != NULL, -1);

    compute_splits(self);

    return selfp->predicted_splits;
}

static dumpfile_t * taper_file_source_get_first_header(TaperSource * pself) {
    TaperFileSource * self = TAPER_FILE_SOURCE(pself);
    g_return_val_if_fail(self != NULL, NULL);

    first_time_setup(self);

    if (parent_class->get_first_header) {
        return (parent_class->get_first_header)(pself);
    } else {
        return NULL;
    }
}

/* Open a holding disk and parse the header. Returns TRUE if
   everything went OK. Writes the fd into fd_pointer and the header
   into header_pointer. Both must be non-NULL. */
static gboolean open_holding_file(char * filename, int * fd_pointer,
                                  dumpfile_t * header_pointer, char **errmsg) {
    int fd;
    size_t read_result;
    char * header_buffer;

    g_return_val_if_fail(filename != NULL, FALSE);
    g_return_val_if_fail(fd_pointer != NULL, FALSE);
    g_return_val_if_fail(header_pointer != NULL, FALSE);

    fd = robust_open(filename, O_NOCTTY | O_RDONLY, 0);
    if (fd < 0) {
	*errmsg = newvstrallocf(*errmsg,
		"Could not open holding disk file \"%s\": %s",
                filename, strerror(errno));
        return FALSE;
    }

    header_buffer = malloc(DISK_BLOCK_BYTES);
    read_result = full_read(fd, header_buffer, DISK_BLOCK_BYTES);
    if (read_result < DISK_BLOCK_BYTES) {
	if (errno != 0) {
	    *errmsg = newvstrallocf(*errmsg,
		    "Could not read header from holding disk file %s: %s",
		    filename, strerror(errno));
	} else {
	    *errmsg = newvstrallocf(*errmsg,
		    "Could not read header from holding disk file %s: got EOF",
		    filename);
	}
        aclose(fd);
	amfree(header_buffer);
        return FALSE;
    }

    dumpfile_free_data(header_pointer);
    parse_file_header(header_buffer, header_pointer, DISK_BLOCK_BYTES);
    amfree(header_buffer);
    
    if (!(header_pointer->type == F_DUMPFILE ||
          header_pointer->type == F_CONT_DUMPFILE)) {
	*errmsg = newvstrallocf(*errmsg,
        	"Got strange header from file %s",
                filename);
        aclose(fd);
        return FALSE;
    }
    
    *fd_pointer = fd;
    return TRUE;
}

/* Copy fd and header information from first chunk fields to current
   chunk. Returns FALSE if an error occurs (unlikely). */
static gboolean copy_chunk_data(int * from_fd, int* to_fd,
                                dumpfile_t * from_header,
                                dumpfile_t * to_header,
				char **errmsg) {
    g_return_val_if_fail(from_fd != NULL, FALSE);
    g_return_val_if_fail(to_fd != NULL, FALSE);
    g_return_val_if_fail(from_header != NULL, FALSE);
    g_return_val_if_fail(to_header != NULL, FALSE);
    g_return_val_if_fail(*to_fd < 0, FALSE);
    
    *to_fd = dup(*from_fd);
    if (*to_fd < 0) {
	*errmsg = newvstrallocf(*errmsg, "dup(%d) failed!", *from_fd);
        return FALSE;
    }

    dumpfile_free_data(to_header);
    dumpfile_copy_in_place(to_header, from_header);

    return TRUE;
}


static gboolean first_time_setup(TaperFileSource * self) {
    TaperSource * pself = (TaperSource*)self;

    if (selfp->part_start_chunk_fd >= 0) {
        return TRUE;
    }

    g_return_val_if_fail(self->holding_disk_file != NULL, FALSE);

    if (!open_holding_file(self->holding_disk_file, 
                           &(selfp->part_start_chunk_fd),
                           &(selfp->part_start_chunk_header),
			   &(pself->errmsg))) {
        return FALSE;
    }

    /* We are all set; just copy the "start chunk" datums into the
       "current chunk" fields. */
    if (!copy_chunk_data(&(selfp->part_start_chunk_fd),
                         &(selfp->current_chunk_fd),
                         &(selfp->part_start_chunk_header),
                         &(selfp->current_chunk_header),
			 &(pself->errmsg))) {
        aclose(selfp->part_start_chunk_fd);
        return FALSE;
    }

    dumpfile_free(pself->first_header);
    pself->first_header = dumpfile_copy(&(selfp->part_start_chunk_header));

    /* Should not be necessary. You never know! */
    selfp->current_part_pos = selfp->part_start_chunk_offset =
        selfp->current_chunk_position = 0;

    return TRUE;
}

static int retry_read(int fd, void * buf, size_t count) {
    for (;;) {
        int read_result = read(fd, buf, count);
        if (read_result < 0 && (0
#ifdef EAGAIN
                                || errno == EAGAIN
#endif
#ifdef EWOULDBLOCK
                                || errno == EWOULDBLOCK
#endif
#ifdef EINTR
                                || errno == EINTR
#endif
                  )) {
            /* Try again. */
            continue;
        } else {
            if (read_result < 0) {
                g_fprintf(stderr, "Error reading holding disk: %s\n",
                        strerror(errno));
            }
            return read_result;
        }
    }
}

/* If another chunk is available, load it. Returns TRUE if there are
   no more chunks or the next chunk is loaded, or FALSE if an error
   occurs. */
static gboolean get_next_chunk(TaperFileSource * self) {
    char * cont_filename = NULL;
    TaperSource * pself = (TaperSource*)self;

    if (selfp->current_chunk_header.cont_filename[0] != '\0') {
        cont_filename =
            g_strdup(selfp->current_chunk_header.cont_filename);
    } else {
        /* No more data. */
        aclose(selfp->current_chunk_fd);
	dumpfile_free_data(&(selfp->current_chunk_header));
        bzero(&(selfp->current_chunk_header),
              sizeof(selfp->current_chunk_header));
        return TRUE;
    }

    /* More data. */

    aclose(selfp->current_chunk_fd);

    if (!open_holding_file(cont_filename,
                           &(selfp->current_chunk_fd),
                           &(selfp->current_chunk_header),
			   &(pself->errmsg))) {
        amfree(cont_filename);
	dumpfile_free_data(&(selfp->current_chunk_header));
        bzero(&(selfp->current_chunk_header),
              sizeof(selfp->current_chunk_header));
        aclose(selfp->current_chunk_fd);
        return FALSE;
    }

    amfree(cont_filename);
    selfp->current_chunk_position = 0;

    return TRUE;
}

static ssize_t 
taper_file_source_read (TaperSource * pself, void * buf, size_t count) {
    TaperFileSource * self = (TaperFileSource*) pself;
    int read_result;

    g_return_val_if_fail (self != NULL, -1);
    g_return_val_if_fail (TAPER_IS_FILE_SOURCE (self), -1);
    g_return_val_if_fail (buf != NULL, -1);
    g_return_val_if_fail (count > 0, -1);
    
    if (!first_time_setup(self))
        return -1;

    if (pself->max_part_size > 0) {
        count = MIN(count, pself->max_part_size - selfp->current_part_pos);
    }
    if (count <= 0) {
        /* Was positive before. Thus we are at EOP. */
        pself->end_of_part = TRUE;
        return 0;
    }

    /* We don't use full_read, because we would rather return a partial
     * read ASAP. */
    read_result = retry_read(selfp->current_chunk_fd, buf, count);
    if (read_result < 0) {
        /* Nothing we can do. */
	pself->errmsg = newvstrallocf(pself->errmsg,
		"Error reading holding disk '%s': %s'",
		 self->holding_disk_file, strerror(errno));
        return read_result;
    } else if (read_result == 0) {
        if (!get_next_chunk(self)) {
            return -1; 
        }

        if (selfp->current_chunk_fd >= 0) {
            /* Try again with the next chunk. */
            return taper_file_source_read(pself, buf, count);
        } else {
            pself->end_of_data = TRUE;
            return 0;
        }
    } else {
        /* Success. */
        selfp->current_part_pos += read_result;
        selfp->current_chunk_position += read_result;
        return read_result;
    }
}

static gboolean taper_file_source_seek_to_part_start (TaperSource * pself) {
    TaperFileSource * self = (TaperFileSource*)pself;
    off_t lseek_result;

    g_return_val_if_fail (self != NULL, FALSE);
    g_return_val_if_fail (TAPER_IS_FILE_SOURCE (self), FALSE);

    aclose(selfp->current_chunk_fd);
    if (!copy_chunk_data(&(selfp->part_start_chunk_fd),
                         &(selfp->current_chunk_fd),
                         &(selfp->part_start_chunk_header),
                         &(selfp->current_chunk_header),
			 &(pself->errmsg))) {
        return FALSE;
    }

    selfp->current_chunk_position = selfp->part_start_chunk_offset;

    lseek_result = lseek(selfp->current_chunk_fd,
                         DISK_BLOCK_BYTES + selfp->current_chunk_position,
                         SEEK_SET);
    if (lseek_result < 0) {
	pself->errmsg = newvstrallocf(pself->errmsg,
        	"Could not seek holding disk file: %s\n",
                strerror(errno));
        return FALSE;
    }

    selfp->current_part_pos = 0;

    if (parent_class->seek_to_part_start)
        return parent_class->seek_to_part_start(pself);
    else
        return TRUE;
}

static void taper_file_source_start_new_part (TaperSource * pself) {
    TaperFileSource * self = (TaperFileSource*)pself;
    g_return_if_fail (self != NULL);
    g_return_if_fail (TAPER_IS_FILE_SOURCE (self));

    aclose(selfp->part_start_chunk_fd);
    if (!copy_chunk_data(&(selfp->current_chunk_fd),
                         &(selfp->part_start_chunk_fd),
                         &(selfp->current_chunk_header),
                         &(selfp->part_start_chunk_header),
			 &(pself->errmsg))) {
        /* We can't return FALSE. :-( Instead, we set things up so
           they will fail on the next read(). */
        aclose(selfp->current_chunk_fd);
        aclose(selfp->part_start_chunk_fd);
        return;
    }

    selfp->part_start_chunk_offset = selfp->current_chunk_position;
    selfp->current_part_pos = 0;

    if (parent_class->start_new_part)
        parent_class->start_new_part(pself);
}
    
