/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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
#include "amindex.h"
%}

amglue_export_ok(
    open_logfile get_logline close_logfile
    log_add log_add_full log_start_multiline log_end_multiline
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
amglue_add_constant(L_CONT, logtype_t);
amglue_add_constant(L_RETRY, logtype_t);
amglue_add_constant(L_MARKER, logtype_t);
amglue_copy_to_tag(logtype_t, constants);

amglue_add_enum_tag_fns(program_t);
amglue_add_constant(P_UNKNOWN, program_t);
amglue_add_constant(P_PLANNER, program_t);
amglue_add_constant(P_DRIVER, program_t);
amglue_add_constant(P_AMREPORT, program_t);
amglue_add_constant(P_DUMPER, program_t);
amglue_add_constant(P_CHUNKER, program_t);
amglue_add_constant(P_TAPER, program_t);
amglue_add_constant(P_AMFLUSH, program_t);
amglue_add_constant(P_AMDUMP, program_t);
amglue_add_constant(P_AMIDXTAPED, program_t);
amglue_add_constant(P_AMFETCHDUMP, program_t);
amglue_add_constant(P_AMCHECKDUMP, program_t);
amglue_add_constant(P_AMVAULT, program_t);
amglue_add_constant(P_AMCLEANUP, program_t);
amglue_add_constant(P_AMBACKUPD, program_t);
amglue_add_constant(P_AMTRMIDX, program_t);
amglue_add_constant(P_AMTRMLOG, program_t);
amglue_copy_to_tag(program_t, constants);

char *get_logtype_str(logtype_t logtype);
char *get_program_str(program_t program);

/* TODO: support for writing logfiles is omitted for the moment. */

%inline %{
/* open_ and close_logfile are both simple wrappers around fopen/fclose. */
typedef FILE loghandle;

static loghandle *open_logfile(char *filename) {
    return fopen(filename, "r");
}
%}

%inline %{
static void close_logfile(loghandle *logfile) {
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
%rename(log_add_full) log_add_full_;
%inline %{
static void log_add_(logtype_t typ, char *message)
{
    log_add(typ, "%s", message);
}
static void log_add_full_(logtype_t typ, char *pname, char *message)
{
    log_add_full(typ, pname, "%s", message);
}
%}

char *make_logname(char *process, char *datestamp);
%newobject get_logname;
char *get_logname(void);
void set_logname(char *filename);
void log_rename(char *datestamp);
void log_start_multiline(void);
void log_end_multiline(void);

%typemap(in) crc_t {
    parse_crc(SvPV_nolen($input), &$1);
}

%typemap(out) crc_t {
    char *s_crc;

    s_crc = g_strdup_printf("%08x:%lld", $1.crc, (long long)$1.size);
    $result = sv_2mortal(newSVpv(s_crc, 0));
    g_free(s_crc);
    argvi++;
}

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
    char *write_timestamp;
    char *hostname;
    char *diskname;
    char *storage;
    int   storage_id;
    char *pool;
    int level;
    char *label;
    off_t filenum;
    char *status;
    char *dump_status;
    char *message;
    int partnum;
    int totalparts;
    double sec;
    off_t bytes;
    off_t kb;
    off_t orig_kb;
    crc_t native_crc;
    crc_t client_crc;
    crc_t server_crc;
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
    find_log search_logfile dumps_match log_rename
);

char **find_log(void);

%rename(search_logfile) search_logfile_wrap;
%inline %{
static find_result_t *search_logfile_wrap(char *label, char *datestamp,
				   char *logfile, int add_missing_disks,
				   int added_todo) {
    find_result_t *rv = NULL;

    /* We use a static variable to collect any unrecognized disks */
    static disklist_t unrecognized_disks = { NULL, NULL };

    search_logfile(&rv, label, datestamp, logfile, 
	add_missing_disks? &unrecognized_disks : NULL, added_todo);

    return rv;
}
%}

%rename(search_holding_disk) search_holding_disk_wrap;
%inline %{
static find_result_t *search_holding_disk_wrap(int added_todo) {
    find_result_t *rv = NULL;
    static disklist_t unrecognized_disks = { NULL, NULL };
    search_holding_disk(&rv, &unrecognized_disks, added_todo);
    return rv;
}
%}

find_result_t *dumps_match(find_result_t *output_find, char *hostname,
			   char *diskname, char *datestamp, char *level, int ok);

find_result_t *dumps_match_dumpspecs(find_result_t *output_find,
    amglue_dumpspec_list *dumpspecs,
    gboolean ok);

%newobject getheaderfname;
char *getheaderfname(char *host, char *disk, char *date, int level);
%newobject getstatefname;
char *getstatefname(char *host, char *disk, char *date, int level);
%newobject getindexfname;
char *getindexfname(char *host, char *disk, char *date, int level);
%newobject getindex_unsorted_fname;
char *getindex_unsorted_fname(char *host, char *disk, char *date, int level);
%newobject getindex_unsorted_gz_fname;
char *getindex_unsorted_gz_fname(char *host, char *disk, char *date, int level);
%newobject getindex_sorted_fname;
char *getindex_sorted_fname(char *host, char *disk, char *date, int level);
%newobject getindex_sorted_gz_fname;
char *getindex_sorted_gz_fname(char *host, char *disk, char *date, int level);

%immutable;
amanda_log_handler_t *amanda_log_trace_log;
%mutable;
amglue_export_ok(
    $amanda_log_trace_log
);


amglue_export_ok(
    find_all_logs find_latest_log
    get_current_log_timestamp
    make_stats
    make_chunker_stats
);

%perlcode %{

use Amanda::Config qw ( :init :getconf config_dir_relative );
use Amanda::Debug;

sub find_all_logs
{
    my $logdir = shift @_ || config_dir_relative(getconf($CNF_LOGDIR));

    opendir my $logdh, $logdir or die("can't read $logdir");
    my @logfiles = sort grep { m{^log\.\d+\.\d+$} } readdir $logdh;

    return @logfiles;
}

sub find_latest_log
{
    my $logdir = shift @_;
    my @logs = find_all_logs($logdir || ());
    return $logs[-1];
}

sub get_current_log_timestamp
{
    my $logfile = config_dir_relative(getconf($CNF_LOGDIR)) . "/log";
    if (! -f $logfile) {
	Amanda::Debug::warning("no current logfile '$logfile'");
	return undef;
    }

    my $logh = open_logfile("$logfile");
    if (!$logh) {
	Amanda::Debug::warning("could not open logfile '$logfile'");
	return undef;
    }
    while (my ($type, $prog, $str) = get_logline($logh)) {
	if ($type == $L_START) {
	    my ($ts) = ($str =~ /date (\d+)/);
	    return $ts if $ts;
	}
    }

    # no timestamp, apparently
    Amanda::Debug::warning("no current timestamp found in logfile");
    return undef;
}

sub make_stats {
    my ($size, $duration, $orig_kb) = @_;

    $duration = 0.1 if $duration <= 0;  # prevent division by zero
    my $kb = $size/1024;
    my $kps = "$kb.0"/$duration; # Perlish cast from BigInt to float

    if (defined $orig_kb) {
	return sprintf("[sec %f bytes %s kps %f orig-kb %s]", $duration, $size, $kps, $orig_kb);
    } else {
	return sprintf("[sec %f bytes %s kps %f]", $duration, $size, $kps);
    }
}

sub make_chunker_stats {
    my ($size, $duration) = @_;

    $duration = 0.1 if $duration <= 0;  # prevent division by zero
    my $kb = $size/1024;
    my $kps = "$kb.0"/$duration; # Perlish cast from BigInt to float

    return sprintf("[sec %f kb %s kps %f]", $duration, $kb, $kps);
}

%}

