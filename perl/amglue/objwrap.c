/*
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

/* Functions to wrap arbitrary C objects into perl, with a blessing.  These
 * functions are used when we totally skip SWIG's object-wrapping and build
 * our own Perl methods to wrap a class -- currently only in AManda::Xfer.
 */

#include "amglue.h"

SV *
new_sv_for_c_obj(
    gpointer c_obj,
    const char *perl_class)
{
    SV *sv = newSV(0);

    /* Make an SV that contains a pointer to the object, and bless it
     * with the appropriate class. */
    sv_setref_pv(sv, perl_class, c_obj);

    return sv;
}

gpointer
c_obj_from_sv(
    SV *sv,
    const char *derived_from)
{
    SV *referent;
    IV tmp;

    if (!sv) return NULL;
    if (!SvOK(sv)) return NULL;

    /* Peel back the layers.  The sv should be a blessed reference to a PV,
     * and we check the class against derived_from to ensure we have the right
     * stuff. */
    if (!sv_isobject(sv) || !sv_derived_from(sv, derived_from)) {
	croak("Value is not an object of type %s", derived_from);
	return NULL;
    }

    referent = (SV *)SvRV(sv);
    tmp = SvIV(referent);
    return INT2PTR(gpointer, tmp);
}

