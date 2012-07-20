#! @PERL@
# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
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
use POSIX qw(WIFEXITED WEXITSTATUS strftime);

use Amanda::Config qw( :init :getconf );
use Amanda::Util qw( :constants );
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Debug qw( debug );
use Amanda::Paths;

##
# Main

sub usage {
    my ($msg) = @_;
    print STDERR <<EOF;
Usage: amdump <conf> [--no-taper] [--from-client] [-o configoption]* [host/disk]*
EOF
    print STDERR "$msg\n" if $msg;
    exit 1;
}

Amanda::Util::setup_application("amdump", "server", $CONTEXT_DAEMON);

my $config_overrides = new_config_overrides($#ARGV+1);
my @config_overrides_opts;

my $opt_no_taper = 0;
my $opt_from_client = 0;

debug("Arguments: " . join(' ', @ARGV));
Getopt::Long::Configure(qw(bundling));
GetOptions(
    'version' => \&Amanda::Util::version_opt,
    'help|usage|?' => \&usage,
    'no-taper' => \$opt_no_taper,
    'from-client' => \$opt_from_client,
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

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# useful info for below
my @hostdisk = @ARGV;
my $logdir = getconf($CNF_LOGDIR);
my @now = localtime;
my $longdate = strftime "%a %b %e %H:%M:%S %Z %Y", @now;
my $timestamp = strftime "%Y%m%d%H%M%S", @now;
my $datestamp = strftime "%Y%m%d", @now;
my $starttime_locale_independent = strftime "%Y-%m-%d %H:%M:%S %Z", @now;
my $trace_log_filename = "$logdir/log";
my $amdump_log_filename = "$logdir/amdump";
my $exit_code = 0;
my $amdump_log = \*STDERR;

##
# subs for below

sub amdump_log {
    print $amdump_log "amdump: ", @_, "\n";
}

sub check_exec {
    my ($prog) = @_;
    return if -x $prog;

    log_add($L_ERROR, "Can't execute $prog");
}

sub run_subprocess {
    my ($proc, @args) = @_;
    check_exec($proc);

    my $pid = POSIX::fork();
    if ($pid == 0) {
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($null, 0);
	POSIX::dup2($null, 1);
	POSIX::dup2(fileno($amdump_log), 2);
	close($amdump_log);
	exec $proc, @args;
	die "Could not exec $proc: $!";
    }
    waitpid($pid, 0);
    my $s = $? >> 8;
    debug("$proc exited with code $s");
    if ($?) {
	if ($exit_code == 0) {
	    debug("ignoring failing exit code $s from $proc");
	} else {
	    debug("recording failing exit code $s from $proc for amdump exit");
	    $exit_code = $s;
	}
    }
}

sub wait_for_hold {
    my $holdfile = "$CONFIG_DIR/$config_name/hold";
    if (-f $holdfile) {
	debug("waiting for hold file '$holdfile' to be removed");
	while (-f $holdfile) {
	    sleep(60);
	}
    }
}

sub bail_already_running {
    my $msg = "An Amanda process is already running - please run amcleanup manually";
    debug($msg);
    amdump_log($msg);

    # put together a fake logfile and send an amreport
    my $fakelogfile = "$AMANDA_TMPDIR/fakelog.$$";
    open(my $fakelog, ">", $fakelogfile)
	or die("cannot open a fake log to send an report - situation is dire");
    print $fakelog <<EOF;
INFO amdump amdump pid $$
START planner date $timestamp
START driver date $timestamp
ERROR amdump $msg
EOF
    run_subprocess("$sbindir/amreport", $config_name, '--from-amdump', '-l', $fakelogfile, @config_overrides_opts);
    unlink($fakelogfile);

    # and we're done here
    exit 1;
}

sub do_amcleanup {
    return unless -f $amdump_log_filename || -f $trace_log_filename;

    # logfiles are still around.  First, try an amcleanup -p to see if
    # the actual processes are already dead
    debug("runing amcleanup -p");
    run_subprocess("$sbindir/amcleanup", '-p', $config_name, @config_overrides_opts);

    # and check again
    return unless -f $amdump_log_filename || -f $trace_log_filename;

    bail_already_running();
}

sub start_logfiles {
    debug("beginning trace log");
    # start the trace log by simply writing an INFO line to it
    log_add($L_INFO, "amdump pid $$");

    # but not so fast!  What if another process has also appended such a line?
    open(my $tl, "<", $trace_log_filename)
	or die("could not open trace log file '$trace_log_filename': $!");
    if (<$tl> !~ /^INFO amdump amdump pid $$/) {
	# we didn't get there first, so bail out
	debug("another amdump raced with this one, and won");
	bail_already_running();
    }
    close($tl);

    # redirect the amdump_log to the proper filename instead of stderr
    # note that perl will overwrite STDERR if we don't set $amdump_log to
    # undef first.. stupid perl.
    debug("beginning amdump log");
    $amdump_log = undef;
    # Must be opened in append so that all subprocess can write to it.
    open($amdump_log, ">>", $amdump_log_filename)
	or die("could not open amdump log file '$amdump_log_filename': $!");
}

sub planner_driver_pipeline {
    my $planner = "$amlibexecdir/planner";
    my $driver = "$amlibexecdir/driver";
    my @no_taper = $opt_no_taper? ('--no-taper'):();
    my @from_client = $opt_from_client? ('--from-client'):();

    check_exec($planner);
    check_exec($driver);

    # Perl's open3 is an embarassment to the language.  We'll do this manually.
    debug("invoking planner | driver");
    my ($rpipe, $wpipe) = POSIX::pipe();

    my $pl_pid = POSIX::fork();
    if ($pl_pid == 0) {
	## child
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($null, 0);
	POSIX::close($null);
	POSIX::dup2($wpipe, 1);
	POSIX::close($rpipe);
	POSIX::close($wpipe);
	POSIX::dup2(fileno($amdump_log), 2);
	close($amdump_log);
	exec $planner,
	    # note that @no_taper must follow --starttime
	    $config_name, '--starttime', $timestamp, @no_taper, @from_client, @config_overrides_opts, @hostdisk;
	die "Could not exec $planner: $!";
    }
    debug(" planner: $pl_pid");

    my $dr_pid = POSIX::fork();
    if ($dr_pid == 0) {
	## child
	my $null = POSIX::open("/dev/null", POSIX::O_RDWR);
	POSIX::dup2($rpipe, 0);
	POSIX::close($rpipe);
	POSIX::close($wpipe);
	POSIX::dup2(fileno($amdump_log), 1); # driver does lots of logging to stdout..
	POSIX::close($null);
	POSIX::dup2(fileno($amdump_log), 2);
	close($amdump_log);
	exec $driver,
	    $config_name, @no_taper, @from_client, @config_overrides_opts;
	die "Could not exec $driver: $!";
    }
    debug(" driver: $dr_pid");

    POSIX::close($rpipe);
    POSIX::close($wpipe);

    my $first_bad_exit = 0;
    for (my $i = 0; $i < 2; $i++) {
	my $dead = wait();
	die("Error waiting: $!") if ($dead <= 0);
	my $s = $? >> 8;
	debug("planner finished with exit code $s") if $dead == $pl_pid;
	debug("driver finished with exit code $s") if $dead == $dr_pid;
	my $exit = WIFEXITED($?)? WEXITSTATUS($?) : 1;
	$first_bad_exit = $exit if ($exit && !$first_bad_exit)
    }
    $exit_code |= $first_bad_exit;
}

sub do_amreport {
    debug("running amreport");
    run_subprocess("$sbindir/amreport", $config_name, '--from-amdump', @config_overrides_opts);
}

sub roll_trace_logs {
    my $t = getconf($CNF_USETIMESTAMPS)? $timestamp : $datestamp;
    debug("renaming trace log");
    Amanda::Logfile::log_rename($t)
}

sub trim_trace_logs {
    debug("trimming old trace logs");
    run_subprocess("$amlibexecdir/amtrmlog", $config_name, @config_overrides_opts);
}

sub trim_indexes {
    debug("trimming old indexes");
    run_subprocess("$amlibexecdir/amtrmidx", $config_name, @config_overrides_opts);
}

sub roll_amdump_logs {
    debug("renaming amdump log and trimming old amdump logs (beyond tapecycle+2)");

    # rename all the way along the tapecycle
    my $days = getconf($CNF_TAPECYCLE) + 2;
    for (my $i = $days-1; $i >= 1; $i--) {
	next unless -f "$amdump_log_filename.$i";
	rename("$amdump_log_filename.$i", "$amdump_log_filename.".($i+1));
    }

    # now swap the current logfile in
    rename("$amdump_log_filename", "$amdump_log_filename.1");
}

# now do the meat of the amdump work; these operations are ported directly
# from the old amdump.sh script

# wait for $confdir/hold to disappear
wait_for_hold();

# look for a current logfile, and if found run amcleanup -p, and if that fails
# bail out
do_amcleanup();

my $crtl_c = 0;
$SIG{INT} = \&interrupt;

sub interrupt {
    $crtl_c = 1;
}

# start up the log file
start_logfiles();

# amstatus needs a lot of forms of the time, I guess
amdump_log("start at $longdate");
amdump_log("datestamp $datestamp");
amdump_log("starttime $timestamp");
amdump_log("starttime-locale-independent $starttime_locale_independent");

# run the planner and driver, the one piped to the other
planner_driver_pipeline();

if ($crtl_c == 1) {
    print "Caught a ctrl-c\n";
    log_add($L_FATAL, "amdump killed by ctrl-c");
    debug("Caught a ctrl-c");
    $exit_code = 1;
}
$SIG{INT} = 'DEFAULT';

my $end_longdate = strftime "%a %b %e %H:%M:%S %Z %Y", localtime;
amdump_log("end at $end_longdate");

# send the dump report
do_amreport();

# do some house-keeping
roll_trace_logs();
trim_trace_logs();
trim_indexes();
roll_amdump_logs();

debug("exiting with code $exit_code");
exit($exit_code);
