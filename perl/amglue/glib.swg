/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

/* Typedefs for a few glib basic types that SWIG doesn't recognize
 * automatically.  Integers are handled specially; see bigint.{swg,c}
 */

%{
#include "amglue.h"
%}

typedef int gboolean;
%typemap(in) gboolean {
    $1 = SvTRUE($input);
}

%typemap(out) gboolean {
    if ($1)
	$result = &PL_sv_yes;
    else
	$result = &PL_sv_no;
    argvi++;
}

typedef void *gpointer;
typedef void *gconstpointer;

typedef char gchar;
typedef unsigned char guchar;

typedef float gfloat;
typedef double gdouble;
