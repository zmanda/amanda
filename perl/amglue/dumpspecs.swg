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

%{
#include "cmdline.h"
%}

/*
 * Typemaps for lists of dumpspecs
 *
 * To use:
 *   %include "amglue/dumpspecs.swg"
 *   %import "Amanda/Logfile.swg"
 * and declare functions as either taking or returning "amglue_dumpspec_list *".
 */

%inline %{
typedef GSList amglue_dumpspec_list;
%}

/* Typemap to convert a GSList of dumpspec_t's into an array of same. This uses
 * some SWIG trickery to manage to convert the GSList itself to an array, while
 * leaving the dumpspecs as C objects.
 */
%types(dumpspec_t *);
%typemap(out) amglue_dumpspec_list * {
    if ($1) {
	GSList *iter;
	EXTEND(SP, g_slist_length($1)); /* make room for return values */

	iter = $1;
	while (iter) {
	    /* Let SWIG take ownership of the object; we'll free the GSList momentarily */
	    $result = SWIG_NewPointerObj(iter->data, $descriptor(dumpspec_t*), SWIG_OWNER | SWIG_SHADOW);
	    argvi++;
	    iter = iter->next;
	}

	/* Now free the GSList, but *not* its contents (which are now "owned" by SWIG) */
	g_slist_free($1);
    }
}

/* Typemap to convert an arrayref of dumpspecs into a GSList of same.  This assumes
 * that the dumpspecs are all C objects.  This borrows references to the underlying
 * dumpspec objects, which remain owned by SWIG.
 */
%typemap(in) amglue_dumpspec_list * {
    AV *av;
    int len;
    int i;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception_fail(SWIG_TypeError, "Expected an arrayref of dumpspecs");
    }
    av = (AV *)SvRV($input);

    len = av_len(av)+1;
    $1 = NULL;
    for (i = 0; i < len; i++) {
	dumpspec_t *ds = NULL;
	SV **elt = av_fetch(av, i, 0);
	if (elt)
	    SWIG_ConvertPtr(*elt, (void **)&ds, $descriptor(dumpspec_t *), 0);
	if (!ds)
	    SWIG_exception_fail(SWIG_TypeError, "Expected an arrayref of dumpspecs");
	$1 = g_slist_append($1, ds);
    }
}

%typemap(freearg) amglue_dumpspec_list * {
    /* Free the GSList, but not its contents (which are still owned by SWIG) */
    g_slist_free($1);
}
