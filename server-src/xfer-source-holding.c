/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "xfer-server.h"
#include "xfer-device.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_holding() references
 * it directly.
 */

GType xfer_source_holding_get_type(void);
#define XFER_SOURCE_HOLDING_TYPE (xfer_source_holding_get_type())
#define XFER_SOURCE_HOLDING(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_holding_get_type(), XferSourceHolding)
#define XFER_SOURCE_HOLDING_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_holding_get_type(), XferSourceHolding const)
#define XFER_SOURCE_HOLDING_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_holding_get_type(), XferSourceHoldingClass)
#define IS_XFER_SOURCE_HOLDING(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_holding_get_type ())
#define XFER_SOURCE_HOLDING_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_holding_get_type(), XferSourceHoldingClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceHolding {
    XferElement __parent__;

    int fd;
    char *next_filename;
    guint64 bytes_read;

    XferElement *dest_taper;
} XferSourceHolding;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourceHoldingClass;

/*
 * Implementation
 */

static gboolean
start_new_chunk(
    XferSourceHolding *self)
{
    char *hdrbuf = NULL;
    dumpfile_t hdr;
    size_t bytes_read;

    /* try to close an already-open file */
    if (self->fd != -1) {
	if (close(self->fd) < 0) {
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"while closing holding file: %s", strerror(errno));
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    return FALSE;
	}

	self->fd = -1;
    }

    /* if we have no next filename, then we're at EOF */
    if (!self->next_filename) {
	return FALSE;
    }

    /* otherwise, open up the next file */
    self->fd = open(self->next_filename, O_RDONLY);
    if (self->fd < 0) {
	xfer_cancel_with_error(XFER_ELEMENT(self),
	    "while opening holding file '%s': %s",
	    self->next_filename, strerror(errno));
	wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	return FALSE;
    }

    /* get a downstream XferDestTaper, if one exists.  This check happens
     * for each chunk, but chunks are large, so that's OK. */
    if (!self->dest_taper) {
	XferElement *elt = (XferElement *)self;

	/* the xfer may have inserted glue between this element and
	* the XferDestTaper. Glue does not change the bytestream, so
	* it does not interfere with cache_inform calls. */
	XferElement *iter = elt->downstream;
	while (iter && IS_XFER_ELEMENT_GLUE(iter)) {
	    iter = iter->downstream;
	}
	if (IS_XFER_DEST_TAPER(iter))
	    self->dest_taper = iter;
    }

    /* tell a XferDestTaper about the new file */
    if (self->dest_taper) {
	struct stat st;
	if (fstat(self->fd, &st) < 0) {
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"while finding size of holding file '%s': %s",
		self->next_filename, strerror(errno));
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    return FALSE;
	}

	xfer_dest_taper_cache_inform(self->dest_taper,
	    self->next_filename,
	    DISK_BLOCK_BYTES,
	    st.st_size - DISK_BLOCK_BYTES);
    }

    /* read the header from the file and determine the filename of the next chunk */
    hdrbuf = g_malloc(DISK_BLOCK_BYTES);
    bytes_read = full_read(self->fd, hdrbuf, DISK_BLOCK_BYTES);
    if (bytes_read < DISK_BLOCK_BYTES) {
	g_free(hdrbuf);
	xfer_cancel_with_error(XFER_ELEMENT(self),
	    "while reading header from holding file '%s': %s",
	    self->next_filename, strerror(errno));
	wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	return FALSE;
    }

    parse_file_header(hdrbuf, &hdr, DISK_BLOCK_BYTES);
    g_free(hdrbuf);
    hdrbuf = NULL;

    if (hdr.type != F_DUMPFILE && hdr.type != F_CONT_DUMPFILE) {
	dumpfile_free_data(&hdr);
	xfer_cancel_with_error(XFER_ELEMENT(self),
	    "unexpected header type %d in holding file '%s'",
	    hdr.type, self->next_filename);
	wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	return FALSE;
    }

    g_free(self->next_filename);
    if (hdr.cont_filename[0]) {
	self->next_filename = g_strdup(hdr.cont_filename);
    } else {
	self->next_filename = NULL;
    }
    dumpfile_free_data(&hdr);

    return TRUE;
}

/* pick an arbitrary block size for reading */
#define HOLDING_BLOCK_SIZE (1024*128)

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourceHolding *self = (XferSourceHolding *)elt;
    char *buf = NULL;
    size_t bytes_read;

    if (elt->cancelled)
	goto return_eof;

    if (self->fd == -1) {
	if (!start_new_chunk(self))
	    goto return_eof;
    }

    buf = g_malloc(HOLDING_BLOCK_SIZE);

    while (1) {
	bytes_read = full_read(self->fd, buf, HOLDING_BLOCK_SIZE);
	if (bytes_read > 0) {
	    *size = bytes_read;
	    self->bytes_read += bytes_read;
	    return buf;
	}

	/* did an error occur? */
	if (errno != 0) {
	    xfer_cancel_with_error(XFER_ELEMENT(self),
		"while reading holding file: %s", strerror(errno));
	    wait_until_xfer_cancelled(XFER_ELEMENT(self)->xfer);
	    goto return_eof;
	}

	if (!start_new_chunk(self))
	    goto return_eof;
    }

return_eof:
    g_free(buf);
    *size = 0;
    return NULL;
}

static void
instance_init(
    XferElement *elt)
{
    XferSourceHolding *self = (XferSourceHolding *)elt;

    elt->can_generate_eof = TRUE;
    self->fd = -1;
}

static void
finalize_impl(
    GObject * obj_self)
{
    XferSourceHolding *self = (XferSourceHolding *)obj_self;

    if (self->next_filename)
	g_free(self->next_filename);

    if (self->fd != -1)
	close(self->fd); /* ignore error; we were probably already cancelled */

    G_OBJECT_CLASS(parent_class)->finalize(obj_self);
}

static void
class_init(
    XferSourceHoldingClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    GObjectClass *goc = G_OBJECT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, 1, 0},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    klass->pull_buffer = pull_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Source::Holding";
    klass->mech_pairs = mech_pairs;

    goc->finalize = finalize_impl;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_source_holding_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceHoldingClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceHolding),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceHolding", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_holding(
    const char *filename)
{
    XferSourceHolding *self = (XferSourceHolding *)g_object_new(XFER_SOURCE_HOLDING_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(self);

    self->next_filename = g_strdup(filename);
    self->bytes_read = 0;

    return elt;
}

guint64
xfer_source_holding_get_bytes_read(
    XferElement *elt)
{
    XferSourceHolding *self = (XferSourceHolding *)elt;

    return self->bytes_read;
}

