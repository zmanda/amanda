/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as
 * published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "amglue.h"

/* TODO: use a slice allocator */
amglue_Source *
amglue_new_source(
    GSource *gsrc, 
    GSourceFunc callback)
{
    amglue_Source *src = g_new0(amglue_Source, 1);
    src->src = gsrc;
    src->callback = callback;
    src->state = AMGLUE_SOURCE_NEW;

    return src;
}

void
amglue_source_free(
    amglue_Source *self)
{
    if (self->callback_sv)
	SvREFCNT_dec(self->callback_sv);
    if (self->state == AMGLUE_SOURCE_ATTACHED)
	g_source_destroy(self->src);
    g_free(self);
}

gboolean
amglue_source_callback_simple(
    gpointer *data)
{
    dSP;
    amglue_Source *src = (amglue_Source *)data;
    g_assert(src->callback_sv != NULL);

    PUSHMARK(SP);
    call_sv(src->callback_sv, G_EVAL|G_DISCARD|G_NOARGS);

    /* 'src' may have been freed at this point! */
    src = NULL;

    /* check for an uncaught 'die'.  If we don't do this, then Perl will longjmp()
     * over the GMainLoop mechanics, leaving GMainLoop in an inconsistent (locked)
     * state. */
    if (SvTRUE(ERRSV)) {
	/* We handle this just the way the default 'die' handler in Amanda::Debug 
	 * does, but since Amanda's debug support may not yet be running, we back
	 * it up with an exit() */
	g_critical("%s", SvPV_nolen(ERRSV));
	exit(1);
    }

    return TRUE;
}
