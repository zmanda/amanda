/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008 Zmanda Inc.
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

#include "amxfer.h"
#include "amanda.h"
#include "simpleprng.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_pattern() references
 * it directly.
 */

GType xfer_source_pattern_get_type(void);
#define XFER_SOURCE_PATTERN_TYPE (xfer_source_pattern_get_type())
#define XFER_SOURCE_PATTERN(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_pattern_get_type(), XferSourcePattern)
#define XFER_SOURCE_PATTERN_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_pattern_get_type(), XferSourcePattern const)
#define XFER_SOURCE_PATTERN_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_pattern_get_type(), XferSourcePatternClass)
#define IS_XFER_SOURCE_PATTERN(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_pattern_get_type ())
#define XFER_SOURCE_PATTERN_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_pattern_get_type(), XferSourcePatternClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourcePattern {
    XferElement __parent__;

    gboolean limited_length;
    guint64 length;
    size_t pattern_orig_length;
    size_t pattern_buffer_length;
    size_t current_offset;
    char * pattern;
} XferSourcePattern;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;
} XferSourcePatternClass;

/*
 * Implementation
 */

static void resize_pattern_buffer(XferSourcePattern * self,
                                  size_t min_new_length) {
    size_t new_length;
    g_assert(self->pattern_buffer_length % self->pattern_orig_length == 0);

    new_length = min_new_length;
    if (new_length % self->pattern_orig_length != 0) {
        /* Extend also to include full pattern. */
        new_length = new_length * (new_length % self->pattern_orig_length)
            + self->pattern_orig_length;
    }

    self->pattern = realloc(self->pattern, new_length);
    while (self->pattern_buffer_length < new_length) {
        /* memcpy is really fast, so rather than copy things ourself,
         * we play games with calls to memcpy of exponentially greater
         * length. */
        size_t data_to_copy;
        if (self->pattern_buffer_length * 2 > new_length) {
            data_to_copy = new_length - self->pattern_buffer_length;
        } else {
            data_to_copy = self->pattern_buffer_length;
        }
        memcpy(self->pattern, self->pattern + self->pattern_buffer_length,
               data_to_copy);
        self->pattern_buffer_length += data_to_copy;
    }

    g_assert(self->pattern_buffer_length == new_length);
    g_assert(self->pattern_buffer_length % self->pattern_orig_length == 0);
}

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourcePattern *self = (XferSourcePattern *)elt;
    char *rval;

    /* indicate EOF on an cancel */
    if (elt->cancelled) {
	*size = 0;
	return NULL;
    }

    if (self->limited_length) {
        if (self->length == 0) {
            *size = 0;
            return NULL;
        }

        *size = MIN(10240, self->length);
        self->length -= *size;
    }

    rval = malloc(*size);
    resize_pattern_buffer(self, *size);

    memcpy(rval, self->pattern, *size);

    self->current_offset =
        (*size + self->current_offset) % self->pattern_orig_length;

    return rval;
}

static void
instance_init(
    XferElement *elt)
{
    elt->can_generate_eof = TRUE;
}

static void
class_init(
    XferSourcePatternClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, 1, 0},
	{ XFER_MECH_NONE, XFER_MECH_NONE, 0, 0},
    };

    klass->pull_buffer = pull_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Source::Pattern";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_source_pattern_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourcePatternClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourcePattern),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourcePattern", &info, 0);
    }

    return type;
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement * xfer_source_pattern(guint64 length, void * pattern,
                                  size_t pattern_length) {
    XferSourcePattern *xsp =
        (XferSourcePattern *)g_object_new(XFER_SOURCE_PATTERN_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xsp);

    xsp->length = length;
    xsp->pattern = g_memdup(pattern, pattern_length);
    xsp->pattern_buffer_length = xsp->pattern_orig_length = pattern_length;

    return elt;
}
