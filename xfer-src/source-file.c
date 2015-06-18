/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "amutil.h"
#include "amxfer.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_file() references
 * it directly.
 */

GType xfer_source_file_get_type(void);
#define XFER_SOURCE_FILE_TYPE (xfer_source_file_get_type())
#define XFER_SOURCE_FILE(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_file_get_type(), XferSourceFile)
#define XFER_SOURCE_FILE_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_file_get_type(), XferSourceFile const)
#define XFER_SOURCE_FILE_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_file_get_type(), XferSourceFileClass)
#define IS_XFER_SOURCE_FILE(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_file_get_type ())
#define XFER_SOURCE_FILE_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_file_get_type(), XferSourceFileClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceFile {
    XferElement __parent__;

    gint fd;
} XferSourceFile;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceFileClass;

/*
 * Implementation
 */

static gboolean
setup_impl(
    XferElement *elt G_GNUC_UNUSED)
{
    XferSourceFile *self = XFER_SOURCE_FILE(elt);

    g_assert(xfer_element_swap_output_fd(elt, dup(self->fd)) == -1);

    return TRUE;
}

static gboolean
start_impl(
    XferElement *elt)
{
    XferSourceFile *self = XFER_SOURCE_FILE(elt);

    lseek(self->fd, (off_t)0, SEEK_SET);

    /* We keep a *copy* of this fd, because our caller will close it to
     * indicate EOF */

    return FALSE;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferSourceFile *self = XFER_SOURCE_FILE(obj_self);

    close(self->fd);
    self->fd = -1;
}

static void
instance_init(
    XferElement *elt)
{
    make_crc_table();
    crc32_init(&elt->crc);
}

static void
class_init(
    XferSourceFileClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_READFD, XFER_NROPS(0), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    klass->start = start_impl;
    klass->setup = setup_impl;
    goc->finalize = finalize_impl;

    klass->perl_class = "Amanda::Xfer::Source::File";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_source_file_get_type (void)
{
    static GType type = 0;

    if (G_UNLIKELY(type == 0)) {
        static const GTypeInfo info = {
            sizeof (XferSourceFileClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceFile),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceFile", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_file(
    char *filename)
{
    XferSourceFile *self = (XferSourceFile *)g_object_new(XFER_SOURCE_FILE_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    g_assert(filename);

    self->fd = open(filename, O_RDONLY);

    return elt;
}
