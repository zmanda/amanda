#! @PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

package main::Interactivity;
use POSIX qw( :errno_h );
use Amanda::MainLoop qw( :GIOCondition );
use vars qw( @ISA );
@ISA = qw( Amanda::Interactivity );

sub new {
    my $class = shift;

    if (!-r STDIN) {
	return undef;
    }
    my $stdin;
    if (!open $stdin, "<&STDIN") {
	return undef;
    }
    close $stdin;

    my $self = {
	input_src => undef};
    return bless ($self, $class);
}

sub abort() {
    my $self = shift;

    if ($self->{'input_src'}) {
	$self->{'input_src'}->remove();
	$self->{'input_src'} = undef;
    }
}

sub user_request {
    my $self = shift;
    my %params = @_;
    my %subs;
    my $buffer = "";

    my $message  = $params{'message'};
    my $label    = $params{'label'};
    my $err      = $params{'err'};
    my $chg_name = $params{'chg_name'};

    $subs{'data_in'} = sub {
	my $b;
	my $n_read = POSIX::read(0, $b, 1);
	if (!defined $n_read) {
	    return if ($! == EINTR);
	    $self->abort();
	    return $params{'request_cb'}->(
		Amanda::Changer::Error->new('fatal',
			message => "Fail to read from stdin"));
	} elsif ($n_read == 0) {
	    $self->abort();
	    return $params{'request_cb'}->(
		Amanda::Changer::Error->new('fatal',
			message => "Aborted by user"));
	} else {
	    $buffer .= $b;
	    if ($b eq "\n") {
		my $line = $buffer;
		chomp $line;
		$buffer = "";
		$self->abort();
		return $params{'request_cb'}->(undef, $line);
	    }
	}
    };

    print STDERR "$err\n";
    print STDERR "Insert volume labeled '$label' in $chg_name\n";
    print STDERR "and press enter, or ^D to abort.\n";

    $self->{'input_src'} = Amanda::MainLoop::fd_source(0, $G_IO_IN|$G_IO_HUP|$G_IO_ERR);
    $self->{'input_src'}->set_callback($subs{'data_in'});
    return;
};


package main;

use Amanda::Config qw( :init :getconf );
use Amanda::Debug qw( :logging );
use Amanda::Util qw( :constants );
use Getopt::Long;
use Amanda::Cmdline qw( :constants parse_dumpspecs );
use Amanda::Vault;

sub usage {
    my ($msg) = @_;

    print STDERR <<EOF;
**NOTE** this interface is under development and will change in future releases!

Usage: amvault [-o configoption...] [-q] [--quiet] [-n] [--dry-run]
	   [--fulls-only] [--latest-fulls] [--incrs-only] [--export]
	   [--src-timestamp src-timestamp] [--exact-match]
	   config
	   [hostname [ disk [ date [ level [ hostname [...] ] ] ] ]]

    -o: configuration override (see amanda(8))
    -q: quiet progress messages
    --fulls-only: only copy full (level-0) dumps
    --latest-fulls: copy the latest full of every dle
    --incrs-only: only copy incremental (level > 0) dumps
    --export: move completed destination volumes to import/export slots
    --src-timestamp: the timestamp of the Amanda run that should be vaulted

Copies data from the run with timestamp <src-timestamp> onto volumes using
the storage <amvault-storage>.  If <src-timestamp> is "latest", then the
most recent run of amdump or amflush will be used.  If any dumpspecs are
included (<host-expr> and so on), then only dumps matching those dumpspecs
will be dumped.  At least one of --fulls-only, --src-timestamp, or a dumpspec
must be specified.

EOF
    if ($msg) {
	print STDERR "ERROR: $msg\n";
    }
    exit(1);
}

Amanda::Util::setup_application("amvault", "server", $CONTEXT_CMDLINE, "amanda", "amanda");

my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;
my $opt_quiet = 0;
my $opt_dry_run = 0;
my $opt_fulls_only = 0;
my $opt_latest_fulls = 0;
my $opt_incrs_only = 0;
my $opt_exact_match = 0;
my $opt_export = 0;
my $opt_src_write_timestamp;
my $opt_interactivity = 1;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw{ bundling });
GetOptions(
    'o=s' => sub {
	push @config_overrides_opts, "-o" . $_[1];
	add_config_override_opt($config_overrides, $_[1]);
    },
    'q|quiet' => \$opt_quiet,
    'n|dry-run' => \$opt_dry_run,
    'fulls-only' => \$opt_fulls_only,
    'latest-fulls' => \$opt_latest_fulls,
    'incrs-only' => \$opt_incrs_only,
    'exact-match' => \$opt_exact_match,
    'export' => \$opt_export,
    'label-template=s' => sub {
	usage("--label-templaple is deprecated, use autolabel from the 'amvault-storage'"); },
    'autolabel=s' => sub {
	usage("--autolabel is deprecated, use autolabel from the 'amvault-storage'"); },
    'dst-changer=s' => sub {
	usage("--dst-changer is deprecated, use tpchanger from the 'amvault-storage'"); },
    'src-timestamp=s' => \$opt_src_write_timestamp,
    'interactivity!' => \$opt_interactivity,
    'version' => \&Amanda::Util::version_opt,
    'help' => \&usage,
) or usage("usage error");

usage("not enough arguments") unless (@ARGV >= 1);

my $config_name = shift @ARGV;
my $cmd_flags = $CMDLINE_PARSE_DATESTAMP|$CMDLINE_PARSE_LEVEL;
$cmd_flags |= $CMDLINE_EXACT_MATCH if $opt_exact_match;
my @opt_dumpspecs = parse_dumpspecs(\@ARGV, $cmd_flags)
    if (@ARGV);

usage("specify something to select the source dumps") unless
    $opt_src_write_timestamp or $opt_fulls_only or $opt_latest_fulls or
    $opt_incrs_only or @opt_dumpspecs;

usage("The following options are incompatible: --fulls-only, --latest-fulls and --incrs-only") if
      ($opt_fulls_only + $opt_latest_fulls + $opt_incrs_only) > 1;

set_config_overrides($config_overrides);
config_init_with_global($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	print STDERR "errors processing config file\n";
	exit(1);
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# and the disklist
my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
$cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
if ($cfgerr_level >= $CFGERR_ERRORS) {
    print STDERR "errors processing disklist\n";
    exit(1);
}

my $exit_status;
my $exit_cb = sub {
    ($exit_status) = @_;
    Amanda::MainLoop::quit();
};

my $is_tty = -t STDOUT;
my $last_is_size = 0;
my $delay;
if ($is_tty) {
    $delay = 1000; # 1 second
} else {
    $delay = 15000; # 15 seconds
}

sub user_msg {
    my $msg = shift;

    if ($msg->{'code'} == 2400008) {
	if ($is_tty) {
	    if (!$last_is_size) {
		print STDOUT "\n";
		$last_is_size = 1;
	    }
	    print STDOUT "\r" . $msg . " ";
	}
    } else {
	if ($is_tty) {
	    if ($last_is_size) {
		print STDOUT "\n";
		$last_is_size = 0;
	    }
	}
	print STDOUT $msg . "\n";
    }
}

my $interactivity;
if ($opt_interactivity) {
    $interactivity = main::Interactivity->new();
}

my @messages;
(my $vault, @messages) = Amanda::Vault->new(
    config => $config_name,
    src_write_timestamp => $opt_src_write_timestamp,
    dst_write_timestamp => Amanda::Util::generate_timestamp(),
    opt_dumpspecs => @opt_dumpspecs? \@opt_dumpspecs : undef,
    opt_dry_run => $opt_dry_run,
    quiet => $opt_quiet,
    fulls_only => $opt_fulls_only,
    latest_fulls=> $opt_latest_fulls,
    incrs_only => $opt_incrs_only,
    opt_export => $opt_export,
    interactivity => $interactivity,
    config_overrides_opts => \@config_overrides_opts,
    user_msg => \&user_msg,
    delay => $delay,
    is_tty => $is_tty
);


Amanda::MainLoop::call_later(sub { $vault->run($exit_cb) });
Amanda::MainLoop::run();

Amanda::Util::finish_application();
exit($exit_status);
