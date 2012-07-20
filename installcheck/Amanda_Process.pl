# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 11;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Amanda::Paths;
use Amanda::Debug;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Process;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $Amanda_process = Amanda::Process->new(0);

$Amanda_process->load_ps_table();

like($Amanda_process->{pstable}->{$$}, qr/(Amanda_Process|perl)/,
   "find program name for perl script");
is($Amanda_process->{ppid}->{$$}, getppid,
   "load_ps_table get correct ppid for Amanda_Process");

#override works done by load_ps_table, override pstable
$Amanda_process->{pstable} = {
	1     => "init",
	1001  => "bash",
	30072 => "amdump",
	30093 => "driver",
	30099 => "taper",
	30100 => "dumper",
	30101 => "dumper",
	30102 => "foobar",
	30103 => "dumper",
	30104 => "dumper",
	30527 => "chunker",
	30538 => "gzip",
	30539 => "gzip",
};

#override works done by load_ps_table, override ppid
$Amanda_process->{ppid} = {
	1     => 1,
	1001  => 1,
	30072 => 1001,
	30093 => 30072,
	30099 => 30093,
	30100 => 30093,
	30101 => 30093,
	30102 => 1,
	30103 => 30093,
	30104 => 30093,
	30527 => 30093,
	30538 => 30100,
	30539 => 30100,
};

#create a log file
my $log_filename = "$Installcheck::TMP/Amanda_Logfile_test.log";
open my $logfile, ">", $log_filename or die("Could not create temporary log file '$log_filename': $!");
print $logfile <<LOGFILE;
INFO amdump amdump pid 30072
INFO driver driver pid 30093
INFO planner planner pid 30092
INFO dumper dumper pid 30100
INFO taper taper pid 30099
INFO dumper dumper pid 30103
INFO dumper dumper pid 30104
INFO dumper dumper pid 30101
INFO planner pid-done 30092
INFO chunker chunker pid 30475
INFO dumper gzip pid 30500
INFO dumper gzip pid 30501
INFO dumper pid-done 30500
INFO chunker pid-done 30475
INFO dumper pid-done 30501
INFO chunker chunker pid 30527
LOGFILE
close $logfile;

#parse the log file
$Amanda_process->scan_log($log_filename);
is_deeply($Amanda_process->{pids},
	{30072 => "amdump",
	 30093 => "driver",
	 30099 => "taper",
	 30100 => "dumper",
	 30101 => "dumper",
	 30103 => "dumper",
	 30104 => "dumper",
	 30527 => "chunker"},
	"scan_log works");
is($Amanda_process->{master_pname}, "amdump",
   "master_name is set to 'amdump'");
is($Amanda_process->{master_pid}, "30072",
   "master_pid is set to '30072'");

$Amanda_process->add_child();
is_deeply($Amanda_process->{amprocess},
	{30072 => 30072,
	 30093 => 30093,
	 30099 => 30099,
	 30100 => 30100,
	 30101 => 30101,
	 30103 => 30103,
	 30104 => 30104,
	 30527 => 30527,
	 30538 => 30538,
	 30539 => 30539},
	"add_child add the 2 gzip process");

is($Amanda_process->process_alive(30100, "dumper"), 1,
   "process_alive return if pname match");
is($Amanda_process->process_alive(30100, "driver"), '',
   "process_alive return '' if pname doesn't match");
is($Amanda_process->process_alive(30100), 1,
   "process_alive return 1 without pname for amanda process");
is($Amanda_process->process_alive(30102), 1,
   "process_alive return 1 without pname for any process");
is($Amanda_process->process_alive(30105), '',
   "process_alive return '' if the process is dead");
