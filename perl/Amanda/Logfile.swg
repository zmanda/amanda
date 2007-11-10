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
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::Logfile"
%include "amglue/exports.swg"
%include "amglue/constants.swg"

%{
#include <glib.h>
#include "logfile.h"
%}

%perlcode %{
=head1 NAME

Amanda::Logfile - read Amanda trace logs

=head1 SYNOPSIS

  use Amanda::Logfile qw(:logtype_t);

  my $log = Amanda::Logfile::open_logfile($filename);
  while (Amanda::Logfile::get_logline($log)) {
    if ($Amanda::Logfile::curlog == $L_INFO) {
	print "Found some info: $Amanda::Logfile::curstr\n";
    }
  }
  Amanda::Logfile::close_logfile($filename);

=head1 API STATUS

Incomplete -- will change

=cut
%}

amglue_export_ok(
    open_logfile get_logline close_logfile
);

%perlcode %{
=head1 CONSTANTS

Constant familiy C<logtype_t> (available in export tag C<logtype_t>)
defines the C<L_*> constants which identify the "type" of log line.
Similarly, constant familiy C<program_t> defines the C<P_*> constants
which program that generated a log line.  Both families of constants
can be converted to symbolic names with C<logtype_t_to_string> and
C<program_t_to_string>, respectively.

=cut
%}

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

/* TODO: support *writing* logs */

%perlcode %{
=head1 FUNCTIONS

=head2 open_logfile($filename)

Opens a logfile for reading, returning an opaque log file handle.

=head2 close_logfile($lf)

Closes a log file handle.

=head2 get_logline($lf)

Loads C<$curlog>, C<$curprog>, and C<$curstr> based on the next logline.  Returns
false at the end of the logfile.

=cut
%}

%inline %{
/* Simple wrappers around fopen/fclose. */
FILE *open_logfile(char *filename) {
    return fopen(filename, "r");
}

void close_logfile(FILE *logfile) {
    if (logfile) fclose(logfile);
}
%}

logtype_t get_logline(FILE *logfile);

%perlcode %{
=head1 PACKAGE VARIABLES

TODO: these should go away

=over

=item C<$curlog>

The C<logtype_t> for the most recent log line

=item C<$curprog>

The C<program_t> for the most recent log line

=item C<$curstr>

The remainder of the most recent log line

=back

=cut
%}

extern logtype_t curlog;
extern program_t curprog;
extern char *curstr;
