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

%module "Amanda::Util"
%include "amglue/amglue.swg"
%include "exception.i"

%{
#include "debug.h"
/* use a relative path here to avoid conflicting with Perl's util.h. */
#include "../common-src/util.h"
#include "file.h"
%}

%perlcode %{
use Amanda::Debug qw(:init);
use Amanda::Config qw(:getconf);
use Carp;
use POSIX qw( :fcntl_h strftime );

=head1 NAME

Amanda::Util - Runtime support for Amanda applications

=head1 Application Initialization

Application initialization generally looks like this:

  use Amanda::Config qw( :init );
  use Amanda::Util qw( :constants );
  use Amanda::Debug;

  Amanda::Util::setup_application("myapp", "server", $CONTEXT_CMDLINE);
  # .. command-line processing ..
  Amanda::Config::config_init(...);
  Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

=over

=item C<setup_application($name, $type, $context)>

Set up the operating environment for an application, without requiring any
configuration.

C<$name> is the name of the application, used in log messages, etc.  C<$type>
is usualy one of "server" or "client".  It specifies the subdirectory in which
debug logfiles will be created.  C<$context> indicates the usual manner in
which this application is invoked; one of C<$CONTEXT_CMDLINE> for a
user-invoked command-line utility (e.g., C<amadmin>) which should send
human-readable error messages to stderr; C<$CONTEXT_DAEMON> for a program
started by C<amandad>, e.g., C<sendbackup>; or C<$CONTEXT_SCRIPTUTIL> for a
small program used from shell scripts, e.g., C<amgetconf>

Based on C<$type> and C<$context>, this function does the following:

=over

=item *

sets up debug logging;

=item *

configures internationalization

=item *

sets the umask;

=item *

sets the current working directory to the debug or temporary directory;

=item *

closes any unnecessary file descriptors as a security meaasure;

=item *

ignores C<SIGPIPE>; and

=item *

sets the appropriate target for error messages.

=back

=cut

# private package variables
my $_pname;
my $_ptype;
my $_pcontext;

sub setup_application {
    my ($name, $type, $context) = @_;

    # sanity check
    croak("no name given") unless ($name);
    croak("no type given") unless ($type);
    croak("no context given") unless ($context);

    # store these as perl values
    $_pname = $name;
    $_ptype = $type;
    $_pcontext = $context;

    # and let the C side know about them too
    set_pname($name);
    set_ptype($type);
    set_pcontext($context);

    safe_cd(); # (also sets umask)
    check_std_fds();

    # set up debugging, now that we have a name, type, and context
    debug_init();

    # ignore SIGPIPE
    $SIG{'PIPE'} = 'IGNORE';
}

=item C<finish_setup($running_as_flags)>

Perform final initialization tasks that require a loaded configuration.
Specifically, move the debug log into a configuration-specific
subdirectory, and check that the current userid is appropriate for
this applciation.

The user is specified by one of the following flags, which are
available in export tag C<:check_running_as_flags>:

  $RUNNING_AS_ANY                 # any user is OK
  $RUNNING_AS_ROOT                # root
  $RUNNING_AS_DUMPUSER            # dumpuser, from configuration
  $RUNNING_AS_DUMPUSER_PREFERRED  # dumpuser, but client_login is OK too
  $RUNNING_AS_CLIENT_LOGIN        # client_login (--with-user at build time)

If the flag C<$RUNNING_AS_UID_ONLY> is bit-or'd into C<$running_as_flags>, then
the euid is ignored; this is used for programs that expect to be setuid-root.

=cut

sub finish_setup {
    my ($running_as) = @_;

    my $config_name = Amanda::Config::get_config_name();

    if ($config_name) {
	dbrename($config_name, $_ptype);
    }

    check_running_as($running_as);
}

=item C<get_original_cwd()>

Return the original current directory with C<get_original_cwd>.

=cut
%}
char *get_original_cwd(void);
amglue_export_tag(util, get_original_cwd);

%perlcode %{
=head1 Miscellaneous Utilities

=item C<safe_env()>

Return a "safe" environment hash.  For non-setuid programs, this means filtering out any
localization variables.

=cut

sub safe_env {
    my %rv = %ENV;

    delete @rv{qw(IFS CDPATH ENV BASH_ENV LANG)};

    # delete all LC_* variables
    for my $var (grep /^LC_/, keys %rv) {
        delete $rv{$var};
    }

    return %rv;
}

%}

amglue_add_flag_tag_fns(running_as_flags);
amglue_add_constant(RUNNING_AS_ANY, running_as_flags);
amglue_add_constant(RUNNING_AS_ROOT, running_as_flags);
amglue_add_constant(RUNNING_AS_DUMPUSER, running_as_flags);
amglue_add_constant(RUNNING_AS_DUMPUSER_PREFERRED, running_as_flags);
amglue_add_constant(RUNNING_AS_CLIENT_LOGIN, running_as_flags);
amglue_add_constant(RUNNING_AS_UID_ONLY, running_as_flags);
amglue_copy_to_tag(running_as_flags, constants);

amglue_add_enum_tag_fns(pcontext_t);
amglue_add_constant(CONTEXT_DEFAULT, pcontext_t);
amglue_add_constant(CONTEXT_CMDLINE, pcontext_t);
amglue_add_constant(CONTEXT_DAEMON, pcontext_t);
amglue_add_constant(CONTEXT_SCRIPTUTIL, pcontext_t);
amglue_copy_to_tag(pcontext_t, constants);

%perlcode %{
=item C<quote_string($str)>

Quote a string using Amanda's quoting algorithm.  Strings with no whitespace,
control, or quote characters are returned unchanged.  An empty string is
represented as the two-character string C<"">.  Otherwise, tab, newline,
carriage return, form-feed, backslash, and double-quote (C<">) characters are
escaped with a backslash and the string is surrounded by double quotes.

=item C<unquote_string($str)>

Unquote a string as quoted with C<quote_string>.

=item C<skip_quoted_string($str)>

my($q, $remaider) = skip_quoted_string($str)

Return the first quoted string and the remainder of the string.

Both C<quote_string>, C<unquote_string> and C<skip_quoted_string> are
available under the export tag C<:quoting>.

=cut

sub skip_quoted_string {
    my $str = shift;

    chomp $str;
    my $iq = 0;
    my $i = 0;
    my $c = substr $str, $i, 1;
    while ($c ne "" && !($iq == 0 && $c =~ /\s/)) {
	if ($c eq '"') {
	    $iq = !$iq;
	} elsif ($c eq '\\') {
	    $i++;
	}
	$i++;
	$c = substr $str, $i, 1;
    }
    my $quoted_string = substr $str, 0, $i;
    my $remainder     = substr $str, $i+1;

    return ($quoted_string, $remainder);
}

%}

char *sanitise_filename(char *inp);
char *quote_string(char *);
char *unquote_string(char *);
amglue_export_tag(quoting, quote_string unquote_string skip_quoted_string sanitise_filename);

%perlcode %{
=item C<generate_timestamp()>

Generate a timestamp from the current time, obeying the 'USETIMESTAMPS'
config parameter.  The Amanda configuration must already be loaded.

=cut

sub generate_timestamp {
    # this corresponds to common-src/timestamp.c's get_proper_stamp_from_time
    if (getconf($CNF_USETIMESTAMPS)) {
	return strftime "%Y%m%d%H%M%S", localtime;
    } else {
	return strftime "%Y%m%d", localtime;
    }
}
%}

/* -------------------------------------------------------------------------
 * Functions below this line are only meant to be called within this module;
 * do not call them externally. */

void set_pname(char *name);
void set_ptype(char *type);
void set_pcontext(pcontext_t context);
void safe_cd(void);

void check_running_as(running_as_flags who);

/* Check that fd's 0, 1, and 2 are open, calling critical() if not.
 */
%perlcode %{
sub check_std_fds {
    fcntl(STDIN, F_GETFD, 0) or critical("Standard input is not open");
    fcntl(STDOUT, F_GETFD, 0) or critical("Standard output is not open");
    fcntl(STDERR, F_GETFD, 0) or critical("Standard error is not open");
}

=back

=cut
%}
