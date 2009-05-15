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

%module "Amanda::Debug"
%include "amglue/amglue.swg"
%include "exception.i"

%{
#include <glib.h>
#include "debug.h"
%}

%perlcode %{
=head1 NAME

Amanda::Debug - support for debugging Amanda applications

=head1 SYNOPSIS

  use Amanda::Util qw( :constants );

  Amanda::Util::setup_application("amcooltool", "server", $CONTEXT_CMDLINE);

  debug("this is a debug message");
  die("Unable to frobnicate the ergonator");

See C<debug.h> for a more in-depth description of the logging functionality of
this module.

=head1 API STATUS

Stable

=head1 DEBUG LOGGING

Several debug logging functions, each taking a single string, are
available:

=over

=item C<error> - also aborts the program to produce a core dump

=item C<critical> - exits the program with C<$error_exit_status>

=item C<warning>

=item C<message>

=item C<info>

=item C<debug>

=back

Perl's built-in C<die> and C<warn> functions are patched to call C<critical>
and C<warning>, respectively. 

All of the debug logging functions are available via the export tag
C<:logging>.

Applications can adjust the handling of log messages with
C<add_amanda_log_handler($hdlr)> where C<$hdlr> is a predefined log
destination.  The following destinations are available in this package.  See
L<Amanda::Logfile> for C<$amanda_log_trace_log>.

  $amanda_log_null
  $amanda_log_stderr
  $amanda_log_syslog

=head1 ADVANCED USAGE

Most applications should use L<Amanda::Util>'s C<setup_application>
to initialize the debug libraries.  The initialization functions
available from this module are thus considered "advanced", and the
reader is advised to consult the C header, C<debug.h>, for details.

Briefly, the functions C<dbopen> and C<dbrename> are used to
open a debug file whose pathname includes all of the relevant
information. C<dbclose> and C<dbreopen> are used to close that debug
file before transferring control to another process.

C<$error_exit_status> is the exit status with which C<critical>
will exit.

All of the initialization functions and variables are available via
the export tag C<:init>.

The current debug file's integer file descriptor (I<not> a Perl
filehandle) is available from C<dbfd()>.  Likewise, C<dbfn()> returns
the filename of the current debug file.

C<debug_dup_stderr_to_debug()> redirects, at the file-descriptor level,
C<STDERR> into the debug file.  This is useful when running external
applications which may produce error output.

=cut
%}

/*
 * Initialization
 */

amglue_export_tag(init,
    debug_init dbopen dbreopen dbrename dbclose
    $error_exit_status
);

void	debug_init(void);
void	dbopen(char *subdir);
void	dbreopen(char *file, char *notation);
void	dbrename(char *config, char *subdir);
void	dbclose(void);

int error_exit_status;

/*
 * Override die() and warn()
 */
%perlcode %{
sub _my_die {
    # $^S: (from perlvar)
    #  undef -> parsing module/eval
    #  1 -> executing an eval
    #  0 -> otherwise
    # we *only* want to call critical() in the "otherwise" case
    if (!defined($^S) or $^S == 1) {
	die(@_);
    } else {
	my ($msg) = @_;
	chomp $msg;
	suppress_error_traceback();
	critical(@_);
    }
};
$SIG{__DIE__} = \&_my_die;

sub _my_warn {
    my ($msg) = @_;
    chomp $msg;
    warning(@_);
};
$SIG{__WARN__} = \&_my_warn;

# utility function for test scripts, which want to use the regular
# perl mechanisms
sub disable_die_override {
    delete $SIG{__DIE__};
    delete $SIG{__WARN__};
}
%}

/*
 * Logging
 */

%rename(error) error__; /* error() is a macro defined in debug.h .. just avoid that */
%inline %{
void error__(char *msg) { g_error("%s", msg); }
void critical(char *msg) { g_critical("%s", msg); }
void warning(char *msg) { g_warning("%s", msg); }
void message(char *msg) { g_message("%s", msg); }
void info(char *msg) { g_info("%s", msg); }
void debug(char *msg) { g_debug("%s", msg); }
%}

amglue_export_tag(logging,
    error critical warning message info debug
);

void add_amanda_log_handler(amanda_log_handler_t *handler);
/* these functions are written as simple global variables, since they are just
 * function pointers used in add_amanda_log_handler.  Note that the functions
 * then appear as e.g., $amanda_log_null. */
%immutable;
amanda_log_handler_t *amanda_log_stderr, *amanda_log_syslog, *amanda_log_null;
%mutable;

amglue_export_tag(logging,
    add_amanda_log_handler
    $amanda_log_stderr $amanda_log_syslog $amanda_log_null
);

/* used to suppress the traceback when calling from perl */
void suppress_error_traceback(void);

/*
 * Advanced
 */

int	dbfd(void);
char *	dbfn(void);
void debug_dup_stderr_to_debug(void);
