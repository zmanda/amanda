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

#include "taper-source.h"
#include "taper-file-source.h"
#include "taper-port-source.h"
#include "taper-disk-port-source.h"
#include "taper-mem-port-source.h"

/* here are local prototypes */
static void taper_source_init (TaperSource * o);
static void taper_source_class_init (TaperSourceClass * c);
static void default_taper_source_start_new_part(TaperSource * self);
static gboolean default_taper_source_is_partial(TaperSource * self);
static gboolean default_taper_source_seek_to_part_start(TaperSource * self);
static gboolean default_taper_source_get_end_of_data(TaperSource * self);
static gboolean default_taper_source_get_end_of_part(TaperSource * self);
static dumpfile_t * default_taper_source_get_first_header(TaperSource * self);
static char* default_taper_source_get_errmsg(TaperSource * self);

/* pointer to the class of our parent */
static GObjectClass *parent_class = NULL;

GType
taper_source_get_type (void)
{
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (TaperSourceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) taper_source_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (TaperSource),
            0 /* n_preallocs */,
            (GInstanceInitFunc) taper_source_init,
            NULL
        };
        
        type = g_type_register_static (G_TYPE_OBJECT, "TaperSource", &info,
                                       (GTypeFlags)G_TYPE_FLAG_ABSTRACT);
    }
    
    return type;
}

static void taper_source_finalize(GObject * obj_self) {
    TaperSource * self = TAPER_SOURCE(obj_self);
    
    if (G_OBJECT_CLASS(parent_class)->finalize)
        G_OBJECT_CLASS(parent_class)->finalize(obj_self);

    dumpfile_free(self->first_header);
    amfree(self->driver_handle);
    amfree(self->errmsg);
}

static void 
taper_source_init (TaperSource * o) {
    o->end_of_data = FALSE;
    o->end_of_part = FALSE;
    o->max_part_size = G_MAXUINT64;
    o->first_header = NULL;
    o->errmsg = NULL;
}

static void 
taper_source_class_init (TaperSourceClass * c) {
    GObjectClass *g_object_class = (GObjectClass*) c;

    parent_class = g_type_class_ref (G_TYPE_OBJECT);

    c->read = NULL;
    c->seek_to_part_start = default_taper_source_seek_to_part_start;
    c->start_new_part = default_taper_source_start_new_part;
    c->is_partial = default_taper_source_is_partial;
    c->get_end_of_data = default_taper_source_get_end_of_data;
    c->get_end_of_part = default_taper_source_get_end_of_part;
    c->get_first_header = default_taper_source_get_first_header;
    c->get_errmsg = default_taper_source_get_errmsg;
    c->predict_parts = NULL;

    g_object_class->finalize = taper_source_finalize;
}

TaperSource * taper_source_new(char * handle,
                               cmd_t mode, char * holding_disk_file,
                               int socket_fd,
                               char * split_disk_buffer,
                               guint64 splitsize,
                               guint64 fallback_splitsize) {
    TaperSource * source_rval;
    g_return_val_if_fail(mode == FILE_WRITE || mode == PORT_WRITE, NULL);
    if (mode == FILE_WRITE) {
        TaperFileSource * file_rval;
        g_return_val_if_fail(holding_disk_file != NULL, NULL);
        g_return_val_if_fail(holding_disk_file[0] != '\0', NULL);

        /* Return a TaperFileSource. */
        
        source_rval = (TaperSource*)
            g_object_new(TAPER_TYPE_FILE_SOURCE, NULL);
        file_rval = (TaperFileSource*) source_rval;

        if (file_rval == NULL)
            return NULL;

        file_rval->holding_disk_file = g_strdup(holding_disk_file);
        source_rval->max_part_size = splitsize;
    } else {
        TaperPortSource * port_rval;
        g_return_val_if_fail(socket_fd >= 0, NULL);

        if (split_disk_buffer != NULL) {
            TaperDiskPortSource * disk_rval;
            g_return_val_if_fail(split_disk_buffer[0] != '\0', NULL);
            g_return_val_if_fail(splitsize > 0, NULL);
            
            /* Return a TaperDiskPortSource. */
            source_rval = (TaperSource*)
                g_object_new(TAPER_TYPE_DISK_PORT_SOURCE, NULL);
            disk_rval = (TaperDiskPortSource*) source_rval;
            port_rval = (TaperPortSource*) source_rval;

            if (disk_rval == NULL)
                return NULL;

            disk_rval->buffer_dir_name = g_strdup(split_disk_buffer);
            disk_rval->fallback_buffer_size = fallback_splitsize;
            source_rval->max_part_size = splitsize;
        } else {
            if (splitsize != 0) {
                TaperMemPortSource * mem_rval;
                /* Return a TaperMemPortSource. */
                if (fallback_splitsize == 0)
                    fallback_splitsize = splitsize;
                source_rval = (TaperSource*)
                    g_object_new(TAPER_TYPE_MEM_PORT_SOURCE, NULL);
                mem_rval = (TaperMemPortSource*) source_rval;
                port_rval = (TaperPortSource*) source_rval;

                if (mem_rval == NULL)
                    return NULL;
                
                source_rval->max_part_size = fallback_splitsize;
            } else {
                /* Return a TaperPortSource. */
                source_rval = (TaperSource*)
                    g_object_new(TAPER_TYPE_PORT_SOURCE, NULL);
                port_rval = (TaperPortSource*) source_rval;

                if (source_rval == NULL)
                    return NULL;
            } 
        }
        
        port_rval->socket_fd = socket_fd;
    }

    /* If we got here, we have a return value. */
    source_rval->driver_handle = strdup(handle);
    return source_rval;
}

/* Default implementations of virtual functions. */
static void
default_taper_source_start_new_part(TaperSource * self) {
    self->end_of_part = FALSE;
}

static gboolean
default_taper_source_seek_to_part_start(TaperSource * self) {
    self->end_of_data = self->end_of_part = FALSE;

    return self->max_part_size > 0;
}

static gboolean
default_taper_source_is_partial(TaperSource * self) {
    return self->first_header->is_partial;
}

static gboolean default_taper_source_get_end_of_data(TaperSource * self) {
    return self->end_of_data;
}
static gboolean default_taper_source_get_end_of_part(TaperSource * self) {
    return self->end_of_part;
}
static dumpfile_t* default_taper_source_get_first_header(TaperSource * self) {
    if (self->first_header == NULL)
	return NULL;
    return dumpfile_copy(self->first_header);
}

static char* default_taper_source_get_errmsg(TaperSource * self) {
    return self->errmsg;
}

/* The rest of these functions are vtable dispatch stubs. */

ssize_t 
taper_source_read (TaperSource * self, void * buf, size_t count)
{
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, (ssize_t )-1);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), (ssize_t )-1);
    g_return_val_if_fail (buf != NULL, (ssize_t )-1);
    g_return_val_if_fail (count > 0, (ssize_t )-1);

    if (self->end_of_data || self->end_of_part) {
        return 0;
    }

    klass = TAPER_SOURCE_GET_CLASS(self);
    
    if(klass->read)
        return (*klass->read)(self,buf,count);
    else
        return (ssize_t )(-1);
}

gboolean 
taper_source_get_end_of_data (TaperSource * self)
{
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, TRUE);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), TRUE);

    klass = TAPER_SOURCE_GET_CLASS(self);
    
    g_return_val_if_fail(klass->get_end_of_data != NULL, TRUE);

    return (*klass->get_end_of_data)(self);
}

gboolean 
taper_source_get_end_of_part (TaperSource * self)
{
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, TRUE);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), TRUE);

    klass = TAPER_SOURCE_GET_CLASS(self);
    
    g_return_val_if_fail(klass->get_end_of_part != NULL, TRUE);

    return (*klass->get_end_of_part)(self);
}

dumpfile_t *
taper_source_get_first_header (TaperSource * self)
{
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, NULL);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), NULL);

    klass = TAPER_SOURCE_GET_CLASS(self);
    
    g_return_val_if_fail(klass->get_first_header != NULL, NULL);

    return (*klass->get_first_header)(self);
}

char *
taper_source_get_errmsg (TaperSource * self)
{
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, NULL);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), NULL);

    klass = TAPER_SOURCE_GET_CLASS(self);
    
    g_return_val_if_fail(klass->get_errmsg != NULL, NULL);

    return (*klass->get_errmsg)(self);
}

int taper_source_predict_parts(TaperSource * self) {
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, -1);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), -1);

    klass = TAPER_SOURCE_GET_CLASS(self);
    
    if (klass->predict_parts != NULL) {
        return (*klass->predict_parts)(self);
    } else {
        return -1;
    }
}

gboolean 
taper_source_seek_to_part_start (TaperSource * self)
{
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, (gboolean )0);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), (gboolean )0);
    klass = TAPER_SOURCE_GET_CLASS(self);
    
    if(klass->seek_to_part_start)
        return (*klass->seek_to_part_start)(self);
    else
        return (gboolean )(0);
}

void 
taper_source_start_new_part (TaperSource * self)
{
    TaperSourceClass *klass;
    g_return_if_fail (self != NULL);
    g_return_if_fail (IS_TAPER_SOURCE (self));
    klass = TAPER_SOURCE_GET_CLASS(self);
    
    if(klass->start_new_part)
        (*klass->start_new_part)(self);
}

gboolean
taper_source_is_partial (TaperSource * self)
{
    TaperSourceClass *klass;
    g_return_val_if_fail (self != NULL, FALSE);
    g_return_val_if_fail (IS_TAPER_SOURCE (self), FALSE);
    g_return_val_if_fail (taper_source_get_end_of_data(self), FALSE);
    klass = TAPER_SOURCE_GET_CLASS(self);
    
    if(klass->is_partial)
        return (*klass->is_partial)(self);
    else
        return FALSE;
}

producer_result_t taper_source_producer(gpointer data,
                                        queue_buffer_t * buffer,
                                        size_t hint_size) {
    TaperSource * source;
    ssize_t result;

    source = data;
    g_assert(IS_TAPER_SOURCE(source));

    buffer->offset = 0;
    if (buffer->data == NULL) {
        buffer->data = malloc(hint_size);
        /* This allocation is more likely than most to fail. */
        g_return_val_if_fail(buffer->data != NULL, PRODUCER_ERROR);
        buffer->alloc_size = hint_size;
    }

    result = taper_source_read(source, buffer->data, buffer->alloc_size);
    if (result > 0) {
        buffer->data_size = result;
        return PRODUCER_MORE;
    } else if (result == 0) {
        /* EOF or EOC? We are done here either way. */
        return PRODUCER_FINISHED;
    } else {
        return PRODUCER_ERROR;
    }

    g_assert_not_reached();
}

