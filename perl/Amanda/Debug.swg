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

%module "Amanda::Debug"
%include "amglue/amglue.swg"
%include "exception.i"

%include "Amanda/Debug.pod"

%{
#include <glib.h>
#include "debug.h"
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
