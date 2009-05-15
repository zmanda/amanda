/*
 * Copyright (c) Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
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

%module "Amanda::Logfile"
%include "amglue/amglue.swg"
%include "exception.i"
%include "amglue/dumpspecs.swg"
%import "Amanda/Cmdline.swg"

%{
#include <glib.h>
#include "logfile.h"
#include "find.h"
#include "diskfile.h" /* for the gross hack, below */
%}

%perlcode %{
=head1 NAME

Amanda::Logfile - manage Amanda trace logs

=head1 SYNOPSIS

  use Amanda::Logfile qw(:logtype_t);
  use Amanda::Config qw( :getconf config_dir_relative );

  for my $logfile (Amanda::Logfile::find_log()) {
    $logfile = config_dir_relative(getconf($CNF_LOGDIR)) . "/" . $logfile;

    my $hdl = Amanda::Logfile::open_logfile($logfile);
    while (my ($type, $prog, $str) = Amanda::Logfile::get_logline($hdl)) {
      if ($type == $L_INFO) {
        my $pname = Amanda::Logfile::program_t_to_string($prog);
        print "Found info line from $pname: $str\n";
      }
    }
    Amanda::Logfile::close_logfile($log);

    my @dumps = Amanda::Logfile::search_logfile("TapeLabel-001", "19780615", $logfile, 1);

    my @matching = Amanda::Logfile::dumps_match([@dumps], "myhost", "/usr", undef, undef, 0);
    for my $dump (@matching) {
      print "$dump->{'label'}:$dump->{'filenum'} = $dump->{'hostname'}:$dump->{'disk'}\n";
    }
  }

=head1 API STATUS

Stabilizing

=head1 RAW LOGFILE ACCESS

This section corresponds to the C C<logfile> module. 

Raw access to logfiles is accomplished by opening a logfile and
fetching log lines one by one via the C<get_logline> function.

A log line is represented by a list C<($type, $prog, $string)>
where C<$type> is one of the C<L_*> constants (available in export
tag C<logtype_t>), C<$prog> is one of the C<P_*> constants (available
in export tag C<program_t>), and C<$str> is the remainder of the line.

Both families of constants can be converted to symbolic names with
C<logtype_t_to_string> and C<program_t_to_string>, respectively.

=head2 FUNCTIONS

Use these functions to read a logfile:

=over

=item C<open_logfile($filename)>

Opens a logfile for reading, returning an opaque log file handle.

=item C<close_logfile($handle)>

Closes a log file handle.

=item C<get_logline($handle)>

Returns a list as described above representing the next log line in
C<$handle>, or nothing at the end of the logfile. 

=back

To write a logfile, call C<log_add($logtype, $string)>.  On the first
call, this function opens and locks C<$logdir/log>; subsequent calls just
append to this file.  As such, this function is only appropriate for situations
where C<amlogroll> will be invoked later to rename C<$logdir/log> to
C<$logdir/log.$timestamp.$n>.

All of the functions in this section can be imported by name if desired.

=head2 Amanda::Find::find_result_t objects

These objects contain information about dumps, as read from logfiles.
Instance variables are:

=over

=item C<timestamp>

=item C<hostname>

=item C<diskname>

=item C<level>

=item C<label>

=item C<filenum>

=item C<status>

=item C<partnum>

=item C<sec>

=item C<kb>

=back

Note that the format for these variables are based on that found in the
logfiles.  In particular, C<partnum> is a string, usually with a slash (C</>)
in it.  Also, C<timestamp> is the timestamp for the run in which the client
dump took place, and not for the timestamp of the logfile.

=head1 HIGHER-LEVEL FUNCTIONS

Functions in this section extract information from logfiles.

=over

=item C<find_log()>

Return a list of logfiles for active tapes.  The tapelist must be loaded before
this function is called (see L<Amanda::Tapelist>).

=item C<search_logfile($label, $datestamp, $logfile, $add_missing_disks)>

Return all results in C<$logfile> matching C<$label> and C<$datestamp>.
If C<$add_missing_disks> is true, then any disks in the logfile
not present in the disklist are added to the disklist; otherwise,
such dumps are skipped.

=item C<dumps_match([@results], $hostname, $diskname, $datestamp, $level, $ok)>

Return a filtered version of C<@results> containing only results that match the 
given expressions.  If C<$ok> is true, don't match partial results.  Note that
C<$level> is given as a string, since it is a match expression.

All of these functions can be imported by name.

=back

=head1 DEBUG LOGGING HANDLER

This package provides C<$amanda_log_trace_log>, which sends C<die> messages
(and any C<g_error> or C<g_critical> calls from C) to the trace log.  Use it
like this:

  use Amanda::Logfile qw( $amanda_log_trace_log );
  # ...
  Amanda::Debug::add_amanda_log_handler($amanda_log_trace_log);

=cut
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
    char *partnum;
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
