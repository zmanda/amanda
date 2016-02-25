/*
 * Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

/* Utility functions for packages that create Amanda::Xfer objects */

#include "amglue.h"
#include "amxfer.h"

SV *
new_sv_for_xfer(
    Xfer *xfer)
{
    if (!xfer) return &PL_sv_undef;

    xfer_ref(xfer);
    return new_sv_for_c_obj(xfer, "Amanda::Xfer::Xfer");
}

SV *
new_sv_for_xfer_element(
    XferElement *xe)
{
    const char *perl_class;

    if (!xe) return &PL_sv_undef;

    perl_class = XFER_ELEMENT_GET_CLASS(xe)->perl_class;
    if (!perl_class) die("Attempt to wrap an XferElementClass with no perl class!");
    g_object_ref(xe);
    return new_sv_for_c_obj(xe, perl_class);
}

Xfer *
xfer_from_sv(
    SV *sv)
{
    return (Xfer *)c_obj_from_sv(sv, "Amanda::Xfer::Xfer");
}

XferElement *
xfer_element_from_sv(
    SV *sv)
{
    return (XferElement *)c_obj_from_sv(sv, "Amanda::Xfer::Element");
}
