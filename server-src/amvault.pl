#! @PERL@
# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
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
	input_src => undef
    };
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
    my $storage_name = $params{'storage_name'};
    my $chg_name = $params{'chg_name'};

    $subs{'data_in'} = sub {
	my $b;
	my $n_read = POSIX::read(0, $b, 1);
	if (!defined $n_read) {
	    return if ($! == EINTR);
	    $self->abort();
	    return $params{'request_cb'}->(
		Amanda::Changer::Error->new('fatal',
			storage_name => $storage_name,
			chg_name => $chg_name,
			dev => "stdin",
			code => 1110000));
	} elsif ($n_read == 0) {
	    $self->abort();
	    return $params{'request_cb'}->(
		Amanda::Changer::Error->new('fatal',
			storage_name => $storage_name,
			chg_name => $chg_name,
			code => 1110001));
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
use Amanda::DB::Catalog2;
use Amanda::Vault;
use Amanda::Amdump;

sub usage {
    my ($msg) = @_;

    print STDERR <<EOF;
**NOTE** this interface is under development and will change in future releases!

Usage: amvault [-o configoption...] [-q] [--quiet] [-n] [--dry-run]
	   [--exact-match] [--export] [--nointeractivity]
	   [--src-labelstr labelstr] [--src-storage storage]
	   [--no-uniq] [--delayed] [run-delayed] [--dest-storage storage]
	   [--fulls-only] [--latest-fulls] [--incrs-only]
	   [--src-timestamp src-timestamp]
	   config
	   [hostname [ disk [ date [ level [ hostname [...] ] ] ] ]]

    -o: configuration override (see amanda(8))
    -q/--quiet: quiet progress messages

    --dest-storage: destination storage for vaulting operation

    --exact-match: parse host and disk as exact values
    --export: move completed destination volumes to import/export slots
    --src-labelstr: only copy dumps from volumes matching labelstr
    --src-storage: only copy dumps from specified storage

    --fulls-only: only copy full (level-0) dumps
    --latest-fulls: copy the latest full of every dle
    --incrs-only: only copy incremental (level > 0) dumps
    --src-timestamp: the timestamp of the Amanda run that should be vaulted
    --no-uniq: Vault a dump even if a copy is already in the dest-storage
    --uniq: Do not vault something that is already in the dest-storage
    --delayed: Schedule the vault to be run later
    --run-delayed: Run the delayed vault

Copies dumps selected by the specified filters onto volumes on the storage
<dest-storage>.  If <src-timestamp> is "latest", then the most recent run of
amdump or amflush will be used.  If any dumpspecs are included (<host-expr> and
so on), then only dumps matching those dumpspecs will be dumped.  At least one
of --fulls-only, --latest-fulls, --incrs-only, --src-timestamp, or a dumpspec
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
my $opt_src_labelstr;
my $opt_src_storage_name;
my $opt_dest_storage_name;
my $opt_interactivity = 1;
my $opt_uniq = undef;
my $opt_delayed = 0;
my $opt_run_delayed = 0;

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
	usage("--label-templaple is deprecated, use autolabel from the 'dest-storage'"); },
    'autolabel=s' => sub {
	usage("--autolabel is deprecated, use autolabel from the 'dest-storage'"); },
    'dst-changer=s' => sub {
	usage("--dst-changer is deprecated, use tpchanger from the 'dest-storage'"); },
    'src-timestamp=s' => \$opt_src_write_timestamp,
    'src-labelstr=s' => \$opt_src_labelstr,
    'src-storage=s' => \$opt_src_storage_name,
    'dest-storage=s' => \$opt_dest_storage_name,
    'interactivity!' => \$opt_interactivity,
    'uniq!' => \$opt_uniq,
    'delayed!' => \$opt_delayed,
    'run-delayed' => \$opt_run_delayed,
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

my $catalog = Amanda::DB::Catalog2->new();

my $exit_status = 0;
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
$|++;

sub user_msg {
    my $msg = shift;

    if ($msg->{'code'} == 2500008) {
	if ($is_tty) {
	    if (!$last_is_size) {
		#print STDOUT "\n";
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

my $messages;
(my $vault, $messages) = Amanda::Vault->new(
    config => $config_name,
    catalog => $catalog,
    src_write_timestamp => $opt_src_write_timestamp,
    dst_write_timestamp => Amanda::Util::generate_timestamp(),
    src_labelstr => $opt_src_labelstr,
    opt_dumpspecs => @opt_dumpspecs? \@opt_dumpspecs : undef,
    opt_dry_run => $opt_dry_run,
    quiet => $opt_quiet,
    fulls_only => $opt_fulls_only,
    latest_fulls=> $opt_latest_fulls,
    incrs_only => $opt_incrs_only,
    opt_export => $opt_export,
    interactivity => $interactivity,
    uniq => $opt_uniq,
    delayed => $opt_delayed,
    run_delayed => $opt_run_delayed,
    config_overrides_opts => \@config_overrides_opts,
    user_msg => \&user_msg,
    delay => $delay,
    is_tty => $is_tty,
    src_storage_name => $opt_src_storage_name,
    dest_storage_name => $opt_dest_storage_name,
);

if (!$vault) {
    foreach my $message (@$messages) {
	print $message, "\n";
    }
    $exit_status = 1;
} else {
    Amanda::MainLoop::call_later(sub { $vault->run($exit_cb) });
    Amanda::MainLoop::run();
}

$catalog->quit();

Amanda::Util::finish_application();
exit($exit_status);
