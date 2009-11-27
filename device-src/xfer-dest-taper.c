/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
#include "xfer-device.h"

static void
class_init(
    XferDestTaperClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);

    klass->perl_class = "Amanda::Xfer::Dest::Taper";
}

GType
xfer_dest_taper_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferDestTaperClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferDestTaper),
            0 /* n_preallocs */,
            (GInstanceInitFunc) NULL,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferDestTaper", &info, 0);
    }

    return type;
}

/*
 * Method stubs
 */

void
xfer_dest_taper_start_part(
    XferElement *elt,
    gboolean retry_part,
    Device *device,
    dumpfile_t *header)
{
    XferDestTaperClass *klass;
    g_assert(IS_XFER_DEST_TAPER(elt));

    klass = XFER_DEST_TAPER_GET_CLASS(elt);
    klass->start_part(XFER_DEST_TAPER(elt), retry_part, device, header);
}

void
xfer_dest_taper_cache_inform(
    XferElement *elt,
    const char *filename,
    off_t offset,
    off_t length)
{
    XferDestTaperClass *klass;
    g_assert(IS_XFER_DEST_TAPER(elt));

    klass = XFER_DEST_TAPER_GET_CLASS(elt);
    klass->cache_inform(XFER_DEST_TAPER(elt), filename, offset, length);
}

guint64
xfer_dest_taper_get_part_bytes_written(
    XferElement *elt G_GNUC_UNUSED)
{
    XferDestTaperClass *klass;
    g_assert(IS_XFER_DEST_TAPER(elt));

    klass = XFER_DEST_TAPER_GET_CLASS(elt);
    if (klass->get_part_bytes_written)
	return klass->get_part_bytes_written(XFER_DEST_TAPER(elt));
    else
	return 0;
}
