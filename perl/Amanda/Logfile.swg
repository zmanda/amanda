/*
 * Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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

%module "Amanda::Logfile"
%include "amglue/amglue.swg"
%include "exception.i"
%include "amglue/dumpspecs.swg"
%import "Amanda/Cmdline.swg"

%include "Amanda/Logfile.pod"

%{
#include <glib.h>
#include "logfile.h"
#include "find.h"
#include "diskfile.h" /* for the gross hack, below */
%}

amglue_export_ok(
    open_logfile get_logline close_logfile
    log_add
);


amglue_add_enum_tag_fns(logtype_t);
amglue_add_constant(L_BOGUS, logtype_t);
amglue_add_constant(L_FATAL, logtype_t);
amglue_add_constant(L_ERROR, logtype_t);
amglue_add_constant(L_WARNING, logtype_t);
amglue_add_constant(L_INFO, logtype_t);
amglue_add_constant(L_SUMMARY, logtype_t);
amglue_add_constant(L_START, logtype_t);
amglue_add_constant(L_FINISH, logtype_t);
amglue_add_constant(L_DISK, logtype_t);
amglue_add_constant(L_DONE, logtype_t);
amglue_add_constant(L_PART, logtype_t);
amglue_add_constant(L_PARTPARTIAL, logtype_t);
amglue_add_constant(L_SUCCESS, logtype_t);
amglue_add_constant(L_PARTIAL, logtype_t);
amglue_add_constant(L_FAIL, logtype_t);
amglue_add_constant(L_STRANGE, logtype_t);
amglue_add_constant(L_CHUNK, logtype_t);
amglue_add_constant(L_CHUNKSUCCESS, logtype_t);
amglue_add_constant(L_STATS, logtype_t);
amglue_add_constant(L_MARKER, logtype_t);
amglue_add_constant(L_CONT, logtype_t);

amglue_add_enum_tag_fns(program_t);
amglue_add_constant(P_UNKNOWN, program_t);
amglue_add_constant(P_PLANNER, program_t);
amglue_add_constant(P_DRIVER, program_t);
amglue_add_constant(P_REPORTER, program_t);
amglue_add_constant(P_DUMPER, program_t);
amglue_add_constant(P_CHUNKER, program_t);
amglue_add_constant(P_TAPER, program_t);
amglue_add_constant(P_AMFLUSH, program_t);
amglue_add_constant(P_AMDUMP, program_t);
amglue_add_constant(P_AMIDXTAPED, program_t);
amglue_add_constant(P_AMFETCHDUMP, program_t);
amglue_add_constant(P_AMCHECKDUMP, program_t);

/* TODO: support for writing logfiles is omitted for the moment. */

%inline %{
/* open_ and close_logfile are both simple wrappers around fopen/fclose. */
typedef FILE loghandle;

loghandle *open_logfile(char *filename) {
    return fopen(filename, "r");
}
%}

%inline %{
void close_logfile(loghandle *logfile) {
    if (logfile) fclose(logfile);
}
%}

/* We fake the return type of get_logline, and use a typemap to
 * slurp curstr, curprog, and curlog into a return value.  */
%{
typedef int LOGLINE_RETURN;
%}
%typemap(out) LOGLINE_RETURN {
    if ($1 != 0) {
	EXTEND(SP, 3);
	$result = sv_2mortal(newSViv(curlog));
	argvi++;
	$result = sv_2mortal(newSViv(curprog));
	argvi++;
	$result = sv_2mortal(newSVpv(curstr, 0));
	argvi++;
    }
    /* otherwise (end of logfile) return an empty list */
}
LOGLINE_RETURN get_logline(FILE *logfile);

%rename(log_add) log_add_;
%inline %{
static void log_add_(logtype_t typ, char *message)
{
    log_add(typ, "%s", message);
}
%}

typedef struct {
    %extend {
	/* destructor */
	~find_result_t() {
	    find_result_t *selfp = self;
	    free_find_result(&selfp);
	}
    }

    %immutable;
    char *timestamp;
    char *hostname;
    char *diskname;
    int level;
    char *label;
    off_t filenum;
    char *status;
    int partnum;
    int totalparts;
    double sec;
    size_t kb;
    %mutable;
} find_result_t;

/* This typemap is used in a few functions.  It converts a linked list of find_result_t's
 * into an array of same, de-linking the list in the process.  This gives ownership of the
 * objects to perl, which is consistent with the C interface to this module.
 */
%typemap(out) find_result_t * {
    find_result_t *iter;
    int len;

    /* measure the list and make room on the perl stack */
    for (len=0, iter=$1; iter; iter=iter->next) len++;
    EXTEND(SP, len);

    iter = $1;
    while (iter) {
	find_result_t *next;
	/* Let SWIG take ownership of the object */
	$result = SWIG_NewPointerObj(iter, $descriptor(find_result_t *), SWIG_OWNER | SWIG_SHADOW);
	argvi++;

	/* null out the 'next' field */
	next = iter->next;
	iter->next = NULL;
	iter = next;
    }
}

/* Similarly, on input we link an array full of find_result_t's.  The list is then
 * unlinked on return.  Note that the array is supplied as an arrayref (since it's 
 * usually the first argument).
 */
%typemap(in) find_result_t * {
    AV *av;
    I32 len, i;
    find_result_t *head = NULL, *tail = NULL;

    if (!SvROK($input) || SvTYPE(SvRV($input)) != SVt_PVAV) {
	SWIG_exception(SWIG_TypeError, "expected an arrayref of find_result_t's");
    }

    av = (AV *)SvRV($input);
    len = av_len(av) + 1;

    for (i = 0; i < len; i++) {
	SV **val = av_fetch(av, i, 0);
	find_result_t *r;

	if (!val || SWIG_ConvertPtr(*val, (void **)&r, $descriptor(find_result_t *), 0) == -1) {
	    SWIG_exception(SWIG_TypeError, "array member is not a find_result_t");
	}

	if (!head) {
	    head = tail = r;
	} else {
	    tail->next = r;
	    tail = r;
	}

	tail->next = NULL;
    }

    /* point to the head of that list */
    $1 = head;
}

%typemap(freearg) find_result_t * {
    find_result_t *iter = $1, *next;

    /* undo all the links we added earlier */
    while (iter) {
	next = iter->next;
	iter->next = NULL;
	iter = next;
    }
}

%typemap(out) char ** {
    char **iter;
    int len, i;
    
    /* measure the length of the array and make sure perl has enough room */
    for (len=0, iter=$1; *iter; iter++) len++;
    EXTEND(SP, len);

    /* now copy it to the perl stack */
    for (i=0, iter=$1; *iter; iter++, i++) {
	$result = sv_2mortal(newSVpv(*iter, 0));
	argvi++;
    }
}

amglue_export_ok(
    find_log search_logfile dumps_match
);

char **find_log(void);

%rename(search_logfile) search_logfile_wrap;
%inline %{
find_result_t *search_logfile_wrap(char *label, char *datestamp, 
				   char *logfile, int add_missing_disks) {
    find_result_t *rv = NULL;

    /* We use a static variable to collect any unrecognized disks */
    static disklist_t unrecognized_disks = { NULL, NULL };

    search_logfile(&rv, label, datestamp, logfile, 
	add_missing_disks? &unrecognized_disks : NULL);

    return rv;
}
%}

find_result_t *dumps_match(find_result_t *output_find, char *hostname,
			   char *diskname, char *datestamp, char *level, int ok);

find_result_t *dumps_match_dumpspecs(find_result_t *output_find,
    amglue_dumpspec_list *dumpspecs,
    gboolean ok);

%immutable;
amanda_log_handler_t *amanda_log_trace_log;
%mutable;
amglue_export_ok(
    $amanda_log_trace_log
);
