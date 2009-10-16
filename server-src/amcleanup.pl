#!@PERL@
# Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
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

use Getopt::Long;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( :constants );
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Process;

my $kill_enable=0;
my $process_alive=0;
my $verbose=0;

sub usage() {
    print "Usage: amcleanup [-k] [-v] [-p] conf\n";
    exit 1;
}

Amanda::Util::setup_application("amcleanup", "server", $CONTEXT_SCRIPTUTIL);

my $config_overrides = new_config_overrides($#ARGV+1);

Getopt::Long::Configure(qw(bundling));
GetOptions(
    'k' => \$kill_enable,
    'p' => \$process_alive,
    'v' => \$verbose,
    'help|usage' => \&usage,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

my $config_name = shift @ARGV;

if ($kill_enable && $process_alive) {
    die "amcleanup: Can't use -k and -p simultaneously\n";
}

config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
apply_config_overrides($config_overrides);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $logdir=config_dir_relative(getconf($CNF_LOGDIR));
my $logfile = "$logdir/log";
my $amreport="$sbindir/amreport";
my $amlogroll="$amlibexecdir/amlogroll";
my $amtrmidx="$amlibexecdir/amtrmidx";
my $amcleanupdisk="$amlibexecdir/amcleanupdisk";

if ( ! -e "$CONFIG_DIR/$config_name" ) {
    die "Configuration directory '$CONFIG_DIR/$config_name' doesn't exist\n";
}
if ( ! -d "$CONFIG_DIR/$config_name" ) {
    die "Configuration directory '$CONFIG_DIR/$config_name' is not a directory\n";
}

my $Amanda_process = Amanda::Process->new($verbose);
$Amanda_process->load_ps_table();

if (-f "$logfile") {
    $Amanda_process->scan_log($logfile);
} elsif (!$process_alive) {
    #check amdump/amflush process
    foreach my $pname ("amdump", "amflush") {
	my $pid = `ps -ef|grep -w ${pname}|grep -w ${config_name}| grep -v grep | awk '{print \$2}'`;
	chomp $pid;
	if ($pid ne "") {
	    $Amanda_process->set_master($pname, $pid);
	}
    }
}

$Amanda_process->add_child();

my $nb_amanda_process = $Amanda_process->count_process();
#if amanda processes are running
if ($nb_amanda_process > 0) {
    if ($process_alive) {
	exit 0;
    } elsif (!$kill_enable) {
	print "amcleanup: ", $Amanda_process->{master_pname}, " Process is running at PID ", $Amanda_process->{master_pid}, " for $config_name configuration.\n";
	print "amcleanup: Use -k option to stop all the process...\n";
	print "Usage: amcleanup [-k] conf\n";
	exit 0;
    } else { #kill the processes
	$Amanda_process->kill_process("SIGTERM");
	my $count = 5;
	my $pp;
	while ($count > 0) {
	   $pp = $Amanda_process->process_running();
	   if ($pp > 0) {
		$count--;
		sleep 1;
	   } else {
		$count = 0;
	   }
	}
	if ($pp > 0) {
	    $Amanda_process->kill_process("SIGKILL");
	    sleep 2;
	    $pp = $Amanda_process->process_running();
	}
	print "amcleanup: ", $nb_amanda_process, " Amanda processes were found running.\n";
	print "amcleanup: $pp processes failed to terminate.\n";
    }
}

# rotate log
if (-f $logfile) {
    system $amreport, $config_name;
    system $amlogroll, $config_name;
    system $amtrmidx, $config_name;
} else {
    print "amcleanup: no unprocessed logfile to clean up.\n";
}

my $tapecycle = getconf($CNF_TAPECYCLE);

# cleanup logfiles
chdir "$CONFIG_DIR/$config_name";
foreach my $pname ("amdump", "amflush") {
    my $errfile = "$logdir/$pname";
    if (-f $errfile) {
	print "amcleanup: $errfile exists, renaming it.\n";

	# Keep debug log through the tapecycle plus a couple days
	my $maxdays=$tapecycle + 2;

	my $days=1;
	# First, find out the last existing errfile,
	# to avoid ``infinite'' loops if tapecycle is infinite
	while ($days < $maxdays  && -f "$errfile.$days") {
	    $days++;
	}

	# Now, renumber the existing log files
	while ($days >= 2) {
	    my $ndays = $days - 1;
	    rename("$errfile.$ndays", "$errfile.$days");
	    $days=$ndays;
	}
	rename($errfile, "$errfile.1");
    }
}

if ($verbose) {
    system $amcleanupdisk, "-v", $config_name;
} else {
    system $amcleanupdisk, $config_name;
}
