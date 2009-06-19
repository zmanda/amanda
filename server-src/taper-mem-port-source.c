/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
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

#define selfp (self->_priv)

#include "taper-mem-port-source.h"

struct _TaperMemPortSourcePrivate {
    /* Actual size of this buffer is given by max_part_size in TaperSource. */
    char * retry_buffer;
    guint64 buffer_offset; /* Bytes read from buffer. */
    guint64 buffer_len;    /* Bytes written to buffer. */
    gboolean retry_mode;
};
/* here are local prototypes */
static void taper_mem_port_source_init (TaperMemPortSource * o);
static void taper_mem_port_source_class_init (TaperMemPortSourceClass * c);
static ssize_t taper_mem_port_source_read (TaperSource * pself, void * buf,
                                           size_t count);
static gboolean taper_mem_port_source_seek_to_part_start (TaperSource * pself);
static void taper_mem_port_source_start_new_part (TaperSource * pself);
static int taper_mem_port_source_predict_parts(TaperSource * pself);

/* pointer to the class of our parent */
static TaperSourceClass * source_parent_class = NULL;
static TaperPortSourceClass *parent_class = NULL;

GType
taper_mem_port_source_get_type (void)
{
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (TaperMemPortSourceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) taper_mem_port_source_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (TaperMemPortSource),
            0 /* n_preallocs */,
            (GInstanceInitFunc) taper_mem_port_source_init,
            NULL
        };
        
        type = g_type_register_static (TAPER_TYPE_PORT_SOURCE,
                                       "TaperMemPortSource", &info,
                                       (GTypeFlags)0);
    }
    
    return type;
}

static void
taper_mem_port_source_finalize(GObject *obj_self) {
    TaperMemPortSource *self = TAPER_MEM_PORT_SOURCE (obj_self);
    if(G_OBJECT_CLASS(parent_class)->finalize)
        (* G_OBJECT_CLASS(parent_class)->finalize)(obj_self);
    amfree (self->_priv->retry_buffer);
    amfree (self->_priv);
}

static void 
taper_mem_port_source_init (TaperMemPortSource * o) {
    o->_priv = malloc(sizeof(TaperMemPortSourcePrivate));
    o->_priv->retry_buffer = NULL;
    o->_priv->retry_mode = FALSE;
    o->_priv->buffer_offset = o->_priv->buffer_len = 0;
}

static void 
taper_mem_port_source_class_init (TaperMemPortSourceClass * c) {
    GObjectClass *g_object_class = (GObjectClass*) c;
    TaperSourceClass *taper_source_class = (TaperSourceClass *)c;
    
    parent_class = g_type_class_ref (TAPER_TYPE_PORT_SOURCE);
    source_parent_class = (TaperSourceClass*)parent_class;
    
    taper_source_class->read = taper_mem_port_source_read;
    taper_source_class->seek_to_part_start =
        taper_mem_port_source_seek_to_part_start;
    taper_source_class->start_new_part = taper_mem_port_source_start_new_part;
    taper_source_class->predict_parts = taper_mem_port_source_predict_parts;

    g_object_class->finalize = taper_mem_port_source_finalize;
}

static int taper_mem_port_source_predict_parts(TaperSource * pself) {
    TaperMemPortSource * self = TAPER_MEM_PORT_SOURCE(pself);
    g_return_val_if_fail(self != NULL, -1);

    return -1;
}

/* Allocate buffer space, if it hasn't been done yet. */
static gboolean
setup_retry_buffer(TaperMemPortSource * self) {
    TaperSource *pself = TAPER_SOURCE(self);
    guint64 alloc_size;
    if (selfp->retry_buffer != NULL)
        return TRUE;

    alloc_size = pself->max_part_size;
    if (alloc_size > SIZE_MAX) {
        g_fprintf(stderr, "Fallback split size of %lld is greater that system maximum of %lld.\n",
                (long long)alloc_size, (long long)SIZE_MAX);
        alloc_size = SIZE_MAX;
    }
    
    if (alloc_size < DISK_BLOCK_BYTES * 10) {
        g_fprintf(stderr, "Fallback split size of %ju is smaller than 10 blocks (%u bytes).\n",
		  (uintmax_t)alloc_size, DISK_BLOCK_BYTES * 10);
        alloc_size = DISK_BLOCK_BYTES * 10;
    }
    
    pself->max_part_size = alloc_size;
    selfp->retry_buffer = malloc(alloc_size);

    if (selfp->retry_buffer == NULL) {
	pself->errmsg = g_strdup_printf(_("Can't allocate %ju bytes of memory for split buffer"),
					(uintmax_t)pself->max_part_size);
	return FALSE;
    }

    return TRUE;
}

static ssize_t 
taper_mem_port_source_read (TaperSource * pself, void * buf, size_t count) {
    TaperMemPortSource * self = (TaperMemPortSource*)pself;
    g_return_val_if_fail (self != NULL, -1);
    g_return_val_if_fail (TAPER_IS_MEM_PORT_SOURCE (self), -1);
    g_return_val_if_fail (buf != NULL, -1);
    g_return_val_if_fail (count > 0, -1);
    
    if (selfp->retry_mode) {
        g_assert(selfp->retry_buffer != NULL && selfp->buffer_len > 0);
        count = MIN(count, selfp->buffer_len - selfp->buffer_offset);

        if (count == 0) {
            /* It was not before. */
            pself->end_of_part = TRUE;
            return 0;
        }

        memcpy(buf, selfp->retry_buffer + selfp->buffer_offset, count);
        selfp->buffer_offset += count;

	/* cancel retry mode if we're at the end of the retry buffer */
	if (selfp->buffer_offset == selfp->buffer_len) {
	    selfp->retry_mode = 0;
	}

        return count;
    } else {
        int read_result;
        if (selfp->retry_buffer == NULL) {
            if (!setup_retry_buffer(self))
		return -1;
        }

        count = MIN(count, pself->max_part_size - selfp->buffer_len);
        if (count == 0) /* it was nonzero before */ {
            pself->end_of_part = TRUE;
            return 0;
        }
        
        read_result = source_parent_class->read(pself, buf, count);
        /* TaperPortSource handles EOF and other goodness. */
        if (read_result <= 0) {
            return read_result;
        }

        /* All's well in the world. */
        memcpy(selfp->retry_buffer + selfp->buffer_len,
               buf, read_result);
        selfp->buffer_len += read_result;

        return read_result;
    }

    g_assert_not_reached();
}

static gboolean 
taper_mem_port_source_seek_to_part_start (TaperSource * pself) {
    TaperMemPortSource * self = (TaperMemPortSource*)pself;
    g_return_val_if_fail (self != NULL, FALSE);
    g_return_val_if_fail (TAPER_IS_MEM_PORT_SOURCE (self), FALSE);
    g_return_val_if_fail (selfp->buffer_len > 0, FALSE);

    selfp->retry_mode = TRUE;
    selfp->buffer_offset = 0;

    if (source_parent_class->seek_to_part_start)
        return source_parent_class->seek_to_part_start(pself);
    else
        return TRUE;
}

static void 
taper_mem_port_source_start_new_part (TaperSource * pself) {
    TaperMemPortSource * self = (TaperMemPortSource*)pself;
    g_return_if_fail (self != NULL);
    g_return_if_fail (TAPER_IS_MEM_PORT_SOURCE (self));

    selfp->buffer_offset = selfp->buffer_len = 0;
    selfp->retry_mode = FALSE;

    if (source_parent_class->start_new_part)
        source_parent_class->start_new_part(pself);
}
