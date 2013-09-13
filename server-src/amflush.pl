#! @PERL@
# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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

use Getopt::Long;
use POSIX qw( WIFEXITED WEXITSTATUS strftime :termios_h);
use File::Glob qw( :glob );

use Amanda::Config qw( :init :getconf );
use Amanda::Util qw( :constants match_datestamp );
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Debug qw( debug );
use Amanda::Paths;
use Amanda::Amflush;
use Amanda::Tapelist;

##
# Main

sub usage {
    my ($msg) = @_;
    print STDERR <<EOF;
Usage: amflush <conf> [-b] [-f] [-D <datestamps>]* [--exact-match] [-o configoption]* [host/disk]*
EOF
    print STDERR "$msg\n" if $msg;
    exit 1;
}

Amanda::Util::setup_application("amflush", "server", $CONTEXT_DAEMON);

my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;

my $opt_no_taper    = 0;
my $opt_from_client = 0;
my $opt_exact_match = 0;
my $opt_batch       = 0;
my $opt_foreground  = 0;
my @opt_datestamps;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'no-taper' => \$opt_no_taper,
    'from-client' => \$opt_from_client,
    'exact-match' => \$opt_exact_match,
    'b' => \$opt_batch,
    'f' => \$opt_foreground,
    'D=s' => sub { push @opt_datestamps, $_[1]; },
    'o=s' => sub {
	push @config_overrides_opts, "-o" . $_[1];
	add_config_override_opt($config_overrides, $_[1]);
    },
) or usage();

usage("No config specified") if (@ARGV < 1);

my $config_name = shift @ARGV;
set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
$cfgerr_level = Amanda::Disklist::read_disklist('filename' => $diskfile);
if ($cfgerr_level >= $CFGERR_ERRORS) {
    die "Errors processing disklist";
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

Amanda::Disklist::add_holding_to_disklist();

sub user_msg {
    my $msg = shift;

    if ($msg->{'code'} != 2000000 and
	$msg->{'code'} != 2000001) {
	print STDOUT $msg->message() . "\n";
    }
}

sub pick_datestamp {
    my @ts = @_;

    my @datestamp;
    while (!@datestamp) {
	print "\nMultiple Amanda runs in holding disks; please pick one by letter:\n";
	my $char = 'A';
	foreach my $ts (@ts) {
	    print "  $char. $ts\n";
	    $char = chr(ord($char)+1);
	    last if $char gt 'Z';
	}
	$char = chr(ord($char)-1);

	print "Select datestamps to flush [A.." . $char ." or <enter> for all]: ";
	my $answer = <STDIN>;

	if (!$answer) {
	    exit(0);
	}
	chomp $answer;
	if ($answer eq "" || $answer eq "ALL") {
	    return @ts;
	}

	for my $c (split //, $answer) {
	    next if $c eq ' ' || $c eq '\t' || $c eq ',';
	    $c = uc $c;
	    if ($c lt 'A' || $c gt $char) {
		undef @datestamp;
		last;
	    }
	    push @datestamp, $ts[$char-'A'];
	}
    }
    return @datestamp;
}

sub confirm {
    my @datestamps = @_;

    my $storages = getconf($CNF_STORAGE);
    foreach my $storage_n (@{$storages}) {
	print "Flushing dumps from " . join(', ', @datestamps);
	my $storage = lookup_storage($storage_n);
	my $tpchanger = storage_getconf($storage, $STORAGE_TPCHANGER);
	print " using storage \"$storage_n\", tape changer \"$tpchanger\".\n";

	my $policy_n = storage_getconf($storage, $STORAGE_POLICY);
	my $labelstr = storage_getconf($storage, $STORAGE_LABELSTR);
	my $l_template = $labelstr->{'template'};
	my $tapepool = storage_getconf($storage, $STORAGE_TAPEPOOL);
	my $retention_tapes;
	my $retention_days;
	my $retention_recover;
	my $retention_full;
	if ($policy_n) {
	    my $policy = lookup_policy($policy_n);
	    if (policy_seen($policy, $POLICY_RETENTION_TAPES)) {
	        $retention_tapes = policy_getconf($policy, $POLICY_RETENTION_TAPES);
	    } else {
	        $retention_tapes = getconf($CNF_TAPECYCLE);
	    }
	    $retention_days = policy_getconf($policy, $POLICY_RETENTION_DAYS);
	    $retention_recover = policy_getconf($policy, $POLICY_RETENTION_RECOVER);
	    $retention_full = policy_getconf($policy, $POLICY_RETENTION_FULL);
	} else {
	    $retention_tapes = getconf($CNF_TAPECYCLE);
	    $retention_days = 0;
	    $retention_recover = 0;
	    $retention_full = 0;
	}
	my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
	my ($tapelist, $message) = Amanda::Tapelist->new($tlf);
	my $tp = Amanda::Tapelist::get_last_reusable_tape_label($l_template,
	#my $tp = $tapelist->get_last_reusable_tape_label($l_template,
			$tapepool, $storage_n, $retention_tapes,
			$retention_days, $retention_recover,
			$retention_full, 0);
	if ($tp) {
	    my $tle = $tapelist->lookup_tapelabel($tp);
	    if ($tle) {
		print "To volume $tp or a new volume,";
	    } else {
		print "To a new volume.\n";
	    }
	    $tle = $tapelist->lookup_tapepos(1);
	    if ($tp) {
		print "  (The last dumps were to volume $tle->{'label'}\n";
	    }
	}
    }

    while (1) {
	$| = 1;
	print "\nAre you sure you want to do this [yN]? ";
	my $term = POSIX::Termios->new();
	my $fd_stdin = fileno(STDIN);
	$term->getattr($fd_stdin);
	my $oterm = $term->getlflag();
	my $echo     = ECHO | ECHOK | ICANON;
	my $noecho   = $oterm & ~$echo;

	$term->setlflag($noecho);
	$term->setcc(VTIME, 1);
	$term->setattr($fd_stdin, TCSANOW);
	my $key = '';
	my $r = sysread(STDIN, $key, 1);
	$term->setlflag($oterm);
	$term->setcc(VTIME, 0);
	$term->setattr($fd_stdin, TCSANOW);

	return 1 if $r == 0;
	return 0 if $r != 1;

	$key = uc $key;
	print "$key\n";

	return 1 if $key eq 'Y';
	return 0 if $key eq 'N' || $key eq "\0" || $key eq "\n";
    }

    return 0;
}


Amanda::Disklist::match_disklist(
	user_msg    => \&user_msg,
	exact_match => $opt_exact_match,
	args        => \@ARGV);

my $hostdisk = \@ARGV;
my ($amflush, @messages) = Amanda::Amflush->new(
				config           => $config_name,
				exact_match      => $opt_exact_match,
				config_overrides => \@config_overrides_opts,
				hostdisk         => $hostdisk,
				user_msg         => \&user_msg);

my @ts = Amanda::Holding::get_all_datestamps();
my @datestamps;
if (@opt_datestamps) {
    foreach my $ts (@ts) {
	foreach my $datestamp (@opt_datestamps) {
	    if (match_datestamp($datestamp, $ts)) {
		push @datestamps, $ts;
		last;
	    }
	}
    }
} elsif ($opt_batch || @ts < 2) {
    @datestamps = @ts;
} else {
    @datestamps = pick_datestamp(@ts);
}

if (!@datestamps) {
    print "Could not find any Amanda directories to flush.\n";
    exit(1);
}

if (!$opt_batch && !confirm(@datestamps)) {
    print "Ok, quitting.  Run amflush again when you are ready.\n";
    log_add($L_INFO, "pid-done $$");
    exit(1);
}

open STDERR, ">>&", $amflush->{'amdump_log'} || die("stdout: $!");
my $to_flushs = $amflush->get_flush(datestamps => \@datestamps);

if (!$to_flushs || !@$to_flushs) {
    print "Could not find any valid dump image, check directory.\n";
    exit(1);
}

if (!$opt_foreground) {
    my $pid = POSIX::fork();
    if ($pid != 0) {
	print STDOUT "Running in background, you can log off now.\n";
	print STDOUT "You'll get mail when amflush is finished.\n";
	# parent exit;
	exit(0);
    }
    open STDIN, '/dev/null';
    open STDOUT, ">>&", $amflush->{'amdump_log'} || die("stdout: $!");
    POSIX::setsid();
}
my $exit_code = $amflush->run(1, $to_flushs);
debug("exiting with code $exit_code");
exit($exit_code);

