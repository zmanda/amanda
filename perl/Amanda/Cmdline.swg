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

%module "Amanda::Cmdline"
%include "amglue/amglue.swg"
%include "exception.i"
%include "amglue/dumpspecs.swg"

%include "Amanda/Cmdline.pod"

%{
#include <glib.h>
#include "amanda.h"
#include "cmdline.h"
#include "fileheader.h"
%}

/* Add a few methods to make this type act like a class */
typedef struct dumpspec_t {
    %immutable;
    char *host;
    char *disk;
    char *datestamp;
    char *level;
    char *write_timestamp;
    %mutable;

    %extend {
	/* constructor */
	dumpspec_t(char *host, char *disk, char *datestamp, char *level, char *write_timestamp) {
	    return dumpspec_new(host, disk, datestamp, level, write_timestamp);
	}

	~dumpspec_t() {
	    dumpspec_free(self);
	}

	%newobject format;
	char *format() {
	    return cmdline_format_dumpspec(self);
	}
    }
} dumpspec_t;

%rename(format_dumpspec_components) cmdline_format_dumpspec_components;
char *cmdline_format_dumpspec_components(char *host, char *disk, char *datestamp, char *level);

/* Typemap to convert a perl list of strings to the strv that 
 * cmdline_parse_dumpspecs expects.
 */
%typemap(in, numinputs=1) (int argc, char **argv) {
    AV *av;
    int i;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception(SWIG_TypeError, "Expected an arrayref");
    }
    av = (AV *)SvRV($input);

    $1 = av_len(av)+1; /* av_len(av) is like $#av */
    $2 = malloc(sizeof(char *) * $1);
    for (i = 0; i < $1; i++) {
	SV **elt = av_fetch(av, i, 0);
	if (!elt || !SvPOK(*elt)) {
	    SWIG_exception(SWIG_TypeError, "Non-string in arrayref");
	}
	$2[i] = SvPV_nolen(*elt); /* TODO: handle unicode here */
    }
}

/* Free the space allocated by the previous typemap */
%typemap(freearg) (int argc, char **argv) {
    free($2);
}

amglue_add_flag_tag_fns(cmdline_parse_dumpspecs_flags);
amglue_add_constant(CMDLINE_PARSE_DATESTAMP, cmdline_parse_dumpspecs_flags);
amglue_add_constant(CMDLINE_PARSE_LEVEL, cmdline_parse_dumpspecs_flags);
amglue_add_constant(CMDLINE_EMPTY_TO_WILDCARD, cmdline_parse_dumpspecs_flags);
amglue_copy_to_tag(cmdline_parse_dumpspecs_flags, constants);

%rename(parse_dumpspecs) cmdline_parse_dumpspecs;
amglue_dumpspec_list *cmdline_parse_dumpspecs(int argc, char **argv, int flags);

/* TODO:
 * convert AV back to GSList as input, convert resulting GSList into an AV of strings
 * on output
 */
/* amglue_dumpspec_list * cmdline_match_holding(amglue_dumpspec_list *dumpspec_list); */

%inline %{
gboolean header_matches_dumpspecs(dumpfile_t *dumpfile, amglue_dumpspec_list *dumpspecs) {
    char level_str[100];

    /* ignore anything that's not a (split) dumpfile */
    if(dumpfile->type != F_DUMPFILE && dumpfile->type != F_SPLIT_DUMPFILE)
	return FALSE;

    g_snprintf(level_str, sizeof(level_str), "%d", dumpfile->dumplevel);

    while (dumpspecs) {
	dumpspec_t *ds = (dumpspec_t *)dumpspecs->data;
	dumpspecs = g_slist_next(dumpspecs);

	if (ds->host && *ds->host
	    && !match_host(ds->host, dumpfile->name))
	    continue;

	if (ds->disk && *ds->disk
	    && !match_disk(ds->disk, dumpfile->disk))
	    continue;

	if (ds->datestamp && *ds->datestamp
	    && !match_datestamp(ds->datestamp, dumpfile->datestamp))
	    continue;

	if (ds->level && *ds->level
	    && !match_level(ds->level, level_str))
	    continue;

	/* passed all the checks, so it's a match */
	return TRUE;
    }

    return FALSE;
}
%}

amglue_export_ok(header_matches_dumpspecs parse_dumpspecs format_dumpspec_components);
