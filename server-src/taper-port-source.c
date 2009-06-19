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

#include "taper-port-source.h"

/* here are local prototypes */
static void taper_port_source_class_init (TaperPortSourceClass * c);
static ssize_t taper_port_source_read (TaperSource * pself, void * buf,
                                       size_t count);
static void taper_port_source_init (TaperPortSource * self);
static gboolean taper_port_source_is_partial(TaperSource * self);
static int taper_port_source_predict_parts(TaperSource * pself);
static dumpfile_t * taper_port_source_get_first_header(TaperSource * pself);


/* pointer to the class of our parent */
static TaperSourceClass *parent_class = NULL;

GType
taper_port_source_get_type (void)
{
    static GType type = 0;
    
    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (TaperPortSourceClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) taper_port_source_class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (TaperPortSource),
            0 /* n_preallocs */,
            (GInstanceInitFunc) taper_port_source_init,
            NULL
        };
        
        type = g_type_register_static (TAPER_SOURCE_TYPE, "TaperPortSource",
                                       &info, (GTypeFlags)0);
    }
    
    return type;
}

static void taper_port_source_finalize(GObject * obj_self) {
    TaperPortSource *self = TAPER_PORT_SOURCE(obj_self);
    if (self->socket_fd >= 0) {
        aclose(self->socket_fd);
    }
    
    G_OBJECT_CLASS (parent_class)->finalize (obj_self);
}

static void taper_port_source_class_init (TaperPortSourceClass * c) {
    TaperSourceClass *taper_source_class = (TaperSourceClass *)c;
    GObjectClass *g_object_class = (GObjectClass*)c;
    
    parent_class = g_type_class_ref (TAPER_SOURCE_TYPE);

    taper_source_class->read = taper_port_source_read;
    taper_source_class->is_partial = taper_port_source_is_partial;
    taper_source_class->get_first_header = taper_port_source_get_first_header;
    taper_source_class->predict_parts = taper_port_source_predict_parts;

    g_object_class->finalize = taper_port_source_finalize;
}

/* Check if the header has been read; if not, read and parse it. */
static void check_first_header(TaperPortSource * self) {
    TaperSource * pself = (TaperSource*)self;
    char buf[DISK_BLOCK_BYTES];
    size_t result;
    dumpfile_t * rval;
    
    if (G_LIKELY(pself->first_header != NULL)) {
        return;
    }
    
    result = full_read(self->socket_fd, buf, DISK_BLOCK_BYTES);
    if (result != DISK_BLOCK_BYTES) {
        return;
    }
    rval = malloc(sizeof(dumpfile_t));
    parse_file_header(buf, rval, DISK_BLOCK_BYTES);
    pself->first_header = rval;
}

static int taper_port_source_predict_parts(TaperSource * pself) {
    TaperPortSource * self = TAPER_PORT_SOURCE(pself);
    g_return_val_if_fail(self != NULL, -1);

    return 1;
}

static dumpfile_t * taper_port_source_get_first_header(TaperSource * pself) {
    TaperPortSource * self = TAPER_PORT_SOURCE(pself);
    g_return_val_if_fail(self != NULL, NULL);

    check_first_header(self);
    
    if (parent_class->get_first_header) {
        return (parent_class->get_first_header)(pself);
    } else {
        return NULL;
    }
}

static void taper_port_source_init (TaperPortSource * self) {
    /* Subclasses may do as they please, but if we are the final word,
       then there will be no rewinding. */
    if (G_TYPE_FROM_INSTANCE(self) == TAPER_TYPE_PORT_SOURCE) {
        TAPER_SOURCE(self)->max_part_size = 0;
    }
    self->socket_fd = -1;
}

static ssize_t taper_port_source_read (TaperSource * pself, void * buf,
                                       size_t count) {
    TaperPortSource * self = (TaperPortSource*)pself;
    int read_result;
    g_return_val_if_fail (self != NULL, -1);
    g_return_val_if_fail (TAPER_IS_PORT_SOURCE (pself), -1);
    g_return_val_if_fail (buf != NULL, -1);
    g_return_val_if_fail (count > 0, -1);
    
    check_first_header(self);

    for (;;) {
        read_result = read(self->socket_fd, buf, count);
        if (read_result > 0) {
            return read_result;
        } else if (read_result == 0) {
            pself->end_of_data = TRUE;
            aclose(self->socket_fd);
            return 0;
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
            return read_result;
        }
    }
    
    g_assert_not_reached();
}

static gboolean
taper_port_source_is_partial(TaperSource * pself) {
    struct cmdargs *cmdargs;
    gboolean result;
    TaperPortSource * self = (TaperPortSource*)pself;

    if (self->socket_fd >= 0)
	return FALSE;

    /* Query DRIVER about partial dump. */
    putresult(DUMPER_STATUS, "%s\n", pself->driver_handle);
    cmdargs = getcmd();
    if (cmdargs->cmd == FAILED) {
        result = TRUE;
    } else if (cmdargs->cmd == DONE) {
        result = FALSE;
    } else {
        error("Driver gave invalid response "
              "to query DUMPER-STATUS.\n");
        g_assert_not_reached();
    }

    free_cmdargs(cmdargs);
    return result;
}
