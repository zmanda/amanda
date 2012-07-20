/*
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

#include "amglue.h"

/* GSources are tricky to bind to perl for a few reasons:
 *  - they have a one-way state machine: once attached and detached, they
 *    cannot be re-attached
 *  - different "kinds" of GSources require C-level callbacks with
 *    different signatures
 *  - an attached GSource should continue running, even if not referenced
 *    from perl, while a detached GSource should free all its resources
 *    when no longer referenced.
 *
 * To accomplish all of this, this file implements a "glue object" called
 * amglue_Source.  There are zero or one amglue_Source objects for each
 * GSource object, so they serve as a place to store "extra" data about a
 * GSource.  In particular, they store:
 *  - a pointer to a C callback function that can trigger a Perl callback
 *  - a pointer to an SV representing the perl callback to run
 *  - a reference count
 * Any number of Perl SV's may reference the amglue_Source -- it tracks this
 * via its reference count.
 *
 * Let's look at this arrangement as it follows a typical usage scenario.  The
 * numbers in brackets are reference counts.
 *
 * -- my $src = Amanda::MainLoop::new_foo_source();
 * GSrc[1] <----) amSrc[1] <---- $src[1] <--- perl-stack
 *
 * The lexical $src contains a reference to the amglue_Source object, which is
 * referencing the underlying GSource object.  Pretty simple.  The amglue_Source
 * only counts one reference because the GSource isn't yet attached.  Think of
 * the ')' in the diagram as a weak reference.  If the perl scope were to end
 * now, all of these objects would be freed immediately.
 *
 * -- $src->set_callback(\&cb);
 *                              ,--> &cb[1]
 * GMainLoop --> GSrc[2] <---> amSrc[2]
 *                              ^--- $src[1] <--- perl-stack
 *
 * The GSource has been attached, so GMainLoop holds a reference to it.  The
 * amglue_Source incremented its own reference count, making the previous weak
 * reference a full reference, because the link from the GSource will be used
 * when a callback occurs.  The amglue_Source object also keeps a reference to
 * the callback coderef.
 *
 * -- return;
 *                              ,--> &cb[1]
 * GMainLoop --> GSrc[2] <---> amSrc[1]
 *
 * When the perl scope ends, the lexical $src is freed, reducing the reference
 * count on the amglue_Source to 1.  At this point, the object is not accessible
 * from perl, but it is still accessible from the GSource via a callback.
 *
 * -- # in callback
 *                              ,--> &cb[1]
 * GMainLoop --> GSrc[2] <---> amSrc[2] <--- $self[1] <--- perl-stack
 *
 * When the callback is invoked, a reference to the amglue_Source is placed on
 * the perl stack, so it is once again referenced twice.
 *
 * -- $self->remove();
 *               GSrc[1] <---) amSrc[1] <--- $self[1] <--- perl-stack
 *
 * Now the callback itself has called remove().  The amglue_Source object removes
 * the GSource from the MainLoop and drops its reference to the perl callback, and
 * decrements its refcount to again weaken the reference from the GSource.  The
 * amglue_Source is now useless, but since it is still in scope, it remains
 * allocated and accessible.
 *
 * -- return;
 *
 * When the callback returns, the last reference to SV is destroyed, reducing
 * the reference count to the amglue_Source to zero, reducing the reference to
 * the GSource to zero.  Everything is gone.
 */

/* We use a glib 'dataset' to attach an amglue_Source to each GSource
 * object.  This requires a Quark to describe the kind of data being
 * attached.
 *
 * We define a macro and corresponding global to support access
 * to our quark.  The compiler will optimize out all but the first
 * conditional in each function, which is just as we want it. */
static GQuark _quark = 0;
#define AMGLUE_SOURCE_QUARK \
    ( _quark?_quark:(_quark = g_quark_from_static_string("amglue_Source")) )

amglue_Source *
amglue_source_get(
    GSource *gsrc,
    GSourceFunc callback)
{
    amglue_Source *src;
    g_assert(gsrc != NULL);

    src = (amglue_Source *)g_dataset_id_get_data(gsrc, AMGLUE_SOURCE_QUARK);

    if (!src)
	src = amglue_source_new(gsrc, callback);
    else
	amglue_source_ref(src);

    return src;
}

amglue_Source *
amglue_source_new(
    GSource *gsrc,
    GSourceFunc callback)
{
    amglue_Source *src = g_new0(amglue_Source, 1);
    g_source_ref(gsrc);
    src->src = gsrc;
    src->callback = callback;
    src->state = AMGLUE_SOURCE_NEW;
    src->refcount = 1;
    g_dataset_id_set_data(gsrc, AMGLUE_SOURCE_QUARK, (gpointer)src);

    return src;
}

void
amglue_source_free(
    amglue_Source *self)
{
    /* if we're attached, we hold a circular reference to ourselves,
     * so we shouldn't be at refcount=0 */
    g_assert(self->state != AMGLUE_SOURCE_ATTACHED);
    g_assert(self->callback_sv == NULL);

    g_dataset_id_remove_data(self->src, AMGLUE_SOURCE_QUARK);
    g_source_unref(self->src);
    g_free(self);
}
