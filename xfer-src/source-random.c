/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "amxfer.h"
#include "simpleprng.h"

/*
 * Class declaration
 *
 * This declaration is entirely private; nothing but xfer_source_random() references
 * it directly.
 */

GType xfer_source_random_get_type(void);
#define XFER_SOURCE_RANDOM_TYPE (xfer_source_random_get_type())
#define XFER_SOURCE_RANDOM(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_random_get_type(), XferSourceRandom)
#define XFER_SOURCE_RANDOM_CONST(obj) G_TYPE_CHECK_INSTANCE_CAST((obj), xfer_source_random_get_type(), XferSourceRandom const)
#define XFER_SOURCE_RANDOM_CLASS(klass) G_TYPE_CHECK_CLASS_CAST((klass), xfer_source_random_get_type(), XferSourceRandomClass)
#define IS_XFER_SOURCE_RANDOM(obj) G_TYPE_CHECK_INSTANCE_TYPE((obj), xfer_source_random_get_type ())
#define XFER_SOURCE_RANDOM_GET_CLASS(obj) G_TYPE_INSTANCE_GET_CLASS((obj), xfer_source_random_get_type(), XferSourceRandomClass)

static GObjectClass *parent_class = NULL;

/*
 * Main object structure
 */

typedef struct XferSourceRandom {
    XferElement __parent__;

    gboolean limited_length;
    guint64 length;
    simpleprng_state_t prng;
} XferSourceRandom;

/*
 * Class definition
 */

typedef struct {
    XferElementClass __parent__;

    guint32 (*get_seed)(XferSourceRandom *elt);
} XferSourceRandomClass;

/*
 * Implementation
 */

static guint32
get_seed_impl(
    XferSourceRandom *self)
{
    return simpleprng_get_seed(&self->prng);
}

static gpointer
pull_buffer_impl(
    XferElement *elt,
    size_t *size)
{
    XferSourceRandom *self = (XferSourceRandom *)elt;
    char *buf;

    if (elt->cancelled || (self->limited_length && self->length == 0)) {
	*size = 0;
	return NULL;
    }

    if (self->limited_length) {
        *size = MIN(10240, self->length);
        self->length -= *size;
    } else {
	*size = 10240;
    }

    buf = g_malloc(*size);
    simpleprng_fill_buffer(&self->prng, buf, *size);

    return buf;
}

static void
instance_init(
    XferElement *elt)
{
    elt->can_generate_eof = TRUE;
}

static void
class_init(
    XferSourceRandomClass * selfc)
{
    XferElementClass *klass = XFER_ELEMENT_CLASS(selfc);
    static xfer_element_mech_pair_t mech_pairs[] = {
	{ XFER_MECH_NONE, XFER_MECH_PULL_BUFFER, XFER_NROPS(1), XFER_NTHREADS(0) },
	{ XFER_MECH_NONE, XFER_MECH_NONE, XFER_NROPS(0), XFER_NTHREADS(0) },
    };

    selfc->get_seed = get_seed_impl;
    klass->pull_buffer = pull_buffer_impl;

    klass->perl_class = "Amanda::Xfer::Source::Random";
    klass->mech_pairs = mech_pairs;

    parent_class = g_type_class_peek_parent(selfc);
}

GType
xfer_source_random_get_type (void)
{
    static GType type = 0;

    if G_UNLIKELY(type == 0) {
        static const GTypeInfo info = {
            sizeof (XferSourceRandomClass),
            (GBaseInitFunc) NULL,
            (GBaseFinalizeFunc) NULL,
            (GClassInitFunc) class_init,
            (GClassFinalizeFunc) NULL,
            NULL /* class_data */,
            sizeof (XferSourceRandom),
            0 /* n_preallocs */,
            (GInstanceInitFunc) instance_init,
            NULL
        };

        type = g_type_register_static (XFER_ELEMENT_TYPE, "XferSourceRandom", &info, 0);
    }

    return type;
}

guint32
xfer_source_random_get_seed(
    XferElement *elt)
{
    XferSourceRandomClass *klass;
    g_assert(IS_XFER_SOURCE_RANDOM(elt));

    klass = XFER_SOURCE_RANDOM_GET_CLASS(elt);
    return klass->get_seed(XFER_SOURCE_RANDOM(elt));
}

/* create an element of this class; prototype is in xfer-element.h */
XferElement *
xfer_source_random(
    guint64 length,
    guint32 prng_seed)
{
    XferSourceRandom *xsr = (XferSourceRandom *)g_object_new(XFER_SOURCE_RANDOM_TYPE, NULL);
    XferElement *elt = XFER_ELEMENT(xsr);

    xsr->length = length;
    xsr->limited_length = (length != 0);
    simpleprng_seed(&xsr->prng, prng_seed);

    return elt;
}
