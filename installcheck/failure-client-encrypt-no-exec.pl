# Copyright (c) 2014-2016 Carbonite, Inc.  All Rights Reserved.
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

use Test::More;
use File::Path;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag check_amreport check_amstatus);
use Installcheck::Catalogs;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::Constants;
use Cwd qw (getcwd);

eval 'use Installcheck::Rest;';
if ($@) {
    plan skip_all => "Can't load Installcheck::Rest: $@";
    exit 1;
}

eval "require Time::HiRes;";

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $rest = Installcheck::Rest->new();
if ($rest->{'error'}) {
   plan skip_all => "Can't start JSON Rest server: $rest->{'error'}: see " . Amanda::Debug::dbfn();
   exit 1;
}
plan tests => 40;

my $reply;

my $config_dir = $Amanda::Paths::CONFIG_DIR;
my $amperldir = $Amanda::Paths::amperldir;
my $testconf;
my $diskfile;
my $infodir;
my $timestamp;
my $tracefile;
my $logfile;
my $hostname = `hostname`;
chomp $hostname;

$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
$testconf->add_param('columnspec', '"Dumprate=1:-8:1,TapeRate=1:-8:1"');
my $cwd = getcwd();
$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
        plugin "amrandom"
        property "SIZE" "1075200"
    }
    encrypt client
    client-encrypt "$cwd/amcat-error-no-exec"
}
EODLE
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
$diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
$infodir = getconf($CNF_INFOFILE);

my $post_data = '';
if ($Amanda::Constants::FAILURE_CODE && rand(2) > 1) {
    $post_data = '{"FAILURE":{"DISABLE_NETWORK_SHM":"1"}}';
}
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs/amdump", $post_data);
foreach my $message (@{$reply->{'body'}}) {
    if (defined $message and defined $message->{'code'}) {
        if ($message->{'code'} == 2000003) {
            $timestamp = $message->{'timestamp'};
        }
        if ($message->{'code'} == 2000001) {
            $tracefile = $message->{'tracefile'};
        }
        if ($message->{'code'} == 2000000) {
            $logfile = $message->{'logfile'};
        }
    }
}

#wait until it is done
do {
    Time::HiRes::sleep(0.5);
    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs");
    } while ($reply->{'body'}[0]->{'code'} == 2000004 and
             $reply->{'body'}[0]->{'status'} ne 'done');

# get REST report
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$logfile");

is($reply->{'body'}->[0]->{'severity'}, 'success', 'severity is success');
is($reply->{'body'}->[0]->{'code'}, '1900001', 'code is 1900001');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'hostname'}, $hostname , 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'exit_status'}, '4' , 'exit_status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'status'}, 'done' , 'status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'org'}, 'DailySet1' , 'org is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'config_name'}, 'TESTCONF' , 'config_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'timestamp'}, $timestamp , 'timestamp is correct');
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[1], '  planner: Adding new disk localhost:diskname2.' , 'notes[1] is correct');
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[2], '  taper: Slot 1 without label can be labeled' , 'notes[2] is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'notes'}->[3], 'no notes[3]');
is_deeply($reply->{'body'}->[0]->{'report'}->{'failure_summary'}, [
        "  localhost diskname2 lev 0  FAILED [exec $cwd/amcat-error-no-exec: Permission denied]",
        "  localhost diskname2 lev 0  FAILED [Got empty header]",
        "  localhost diskname2 lev 0  FAILED [exec $cwd/amcat-error-no-exec: Permission denied]",
        "  localhost diskname2 lev 0  FAILED [Got empty header]",
        ], "failure_summary is correct") || diag("failure_summary: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'failure_summary'}));

ok(!exists $reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0], 'no tape');
is_deeply($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}, {
	'storage' => { 'TESTCONF' => { 'new'  => 1,
				       'next_to_use' => 1
				     }
		     }
        }, 'tapeinfo is correct') || diag("tapeinfo: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}));
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tape_size'}, { 'full' => '0',
									     'total' => '0',
									     'incr' => '0' }, 'tape_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'parts_taped'}, { 'full' => '0',
									       'total' => '0',
									       'incr' => '0' }, 'parts_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_taped'}, { 'full' => '0',
									      'total' => '0',
									      'incr' => '0' }, 'dles_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_dumped'}, { 'full' => '0',
									       'total' => '0',
									       'incr' => '0' }, 'dles_dumped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'original_size'}, { 'full' => '0',
									         'total' => '0',
									         'incr' => '0' }, 'original_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'output_size'}, { 'full' => '0',
									       'total' => '0',
									       'incr' => '0' }, 'output_size is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dumpdisks'}, '', 'dumpdisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapedisks'}, '', 'tapedisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapeparts'}, '', 'tapeparts is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'backup_level'}, '0', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'disk_name'}, 'diskname2', 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'hostname'}, 'localhost', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_orig_kb'}, '', 'dump_orig_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_out_kb'}, '', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dle_status'}, 'nodump-PARTIAL', 'dle_status is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'summary'}->[1], 'Only one summary');

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?tracefile=$tracefile");
is($reply->{'body'}->[0]->{'severity'}, 'info', 'severity is info');
is($reply->{'body'}->[0]->{'code'}, '1800000', 'code is 1800000');
is($reply->{'body'}->[0]->{'status'}->{'dead_run'}, '1', 'dead_run is correct');
is($reply->{'body'}->[0]->{'status'}->{'exit_status'}, '4', 'exit_status is correct');
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'chunk_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'dump_time'} = undef;
is_deeply($reply->{'body'}->[0]->{'status'}->{'dles'},
    {
	'localhost' => {
		'diskname2' => {
			$timestamp => {
				'retry' => '0',
				'size' => '0',
				'esize' => '1075200',
				'retry_level' => '-1',
				'message' => "dump failed: [exec $cwd/amcat-error-no-exec: Permission denied]",
				'chunk_time' => undef,
				'dsize' => '0',
				'status' => '12',
				'level' => '0',
				'dump_time' => undef,
				'error' => "[exec $cwd/amcat-error-no-exec: Permission denied]",
				'holding_file' => "$Installcheck::TMP/holding/$timestamp/localhost.diskname2.0",
				'degr_level' => '-1',
				'flush' => '0',
				'will_retry' => '0'
			}
		}
	}
    },
    'dles is correct') || diag("dles: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'status'}->{'dles'}));

is_deeply($reply->{'body'}->[0]->{'status'}->{'taper'},
    {
	'taper0' => {
		'worker' => {
			'worker0-0' => {
				'status' => '0',
			}
		},
		'tape_size' => '31457280',
		'storage' => 'TESTCONF',
		'nb_tape' => '0',
	}
    },
    'taper is correct') ||diag('status taper'. Data::Dumper::Dumper($reply->{'body'}->[0]->{'status'}->{'taper'}));

is($reply->{'body'}->[0]->{'status'}->{'storage'}->{'TESTCONF'}->{'taper'}, 'taper0', 'taper is correct');
is_deeply($reply->{'body'}->[0]->{'status'}->{'stat'},
    {
	'flush' => {
		'name' => 'flush'
	},
	'writing_to_tape' => {
		'name' => 'writing to tape'
	},
	'wait_for_dumping' => {
		'name' => 'wait for dumping',
		'real_size' => undef,
		'estimated_stat' => '0',
		'nb' => '0',
		'estimated_size' => '0'
	},
	'failed_to_tape' => {
		'name' => 'failed to tape'
	},
	'dumping_to_tape' => {
		'name' => 'dumping to tape',
		'estimated_stat' => '0',
		'real_size' => '0',
		'real_stat' => '0',
		'estimated_size' => '0',
		'nb' => '0'
	},
	'wait_for_writing' => {
		'name' => 'wait for writing'
	},
	'wait_to_flush' => {
		'name' => 'wait_to_flush'
	},
	'dump_failed' => {
		'name' => 'dump failed',
		'estimated_stat' => '0',
		'real_size' => undef,
		'nb' => '1',
		'estimated_size' => '1075200'
	},
	'estimated' => {
		'name' => 'estimated',
		'real_size' => undef,
		'estimated_size' => '1075200',
		'nb' => '1'
	},
	'taped' => {
		'name' => 'taped',
	},
	'dumped' => {
		'name' => 'dumped',
		'estimated_stat' => '0',
		'real_size' => '0',
		'nb' => '0',
		'real_stat' => '0',
		'estimated_size' => '0'
	},
	'disk' => {
		'name' => 'disk',
		'nb' => '1',
		'estimated_size' => undef,
		'real_size' => undef
	},
	'dumping' => {
		'name' => 'dumping',
		'nb' => '0',
		'real_stat' => '0',
		'estimated_size' => '0',
		'real_size' => '0',
		'estimated_stat' => '0'
	}
    },
    'stat is correct') || diag("stat: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'status'}->{'stat'}));


# amreport
#
my $report = <<"END_REPORT";
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : June 22, 2016

The next tape Amanda expects to use is: 1 new tape.


FAILURE DUMP SUMMARY:
  localhost diskname2 lev 0  FAILED [exec $cwd/amcat-error-no-exec: Permission denied]
  localhost diskname2 lev 0  FAILED [Got empty header]
  localhost diskname2 lev 0  FAILED [exec $cwd/amcat-error-no-exec: Permission denied]
  localhost diskname2 lev 0  FAILED [Got empty header]


STATISTICS:
                          Total       Full      Incr.   Level:#
                        --------   --------   --------  --------
Estimate Time (hrs:min)     0:00
Run Time (hrs:min)          0:00
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            0.0        0.0        0.0
Original Size (meg)          0.0        0.0        0.0
Avg Compressed Size (%)      --         --         --
DLEs Dumped                    0          0          0
Avg Dump Rate (k/s)          --         --         --

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)              0.0        0.0        0.0
Tape Used (%)                0.0        0.0        0.0
DLEs Taped                     0          0          0
Parts Taped                    0          0          0
Avg Tp Write Rate (k/s)      --         --         --


FAILED DUMP DETAILS:
  /-- localhost diskname2 lev 0 FAILED [exec $cwd/amcat-error-no-exec: Permission denied]
  sendbackup: info BACKUP=APPLICATION
  sendbackup: info APPLICATION=amrandom
  sendbackup: info RECOVER_CMD=$Amanda::Paths::APPLICATION_DIR/amrandom restore [./file-to-restore]+
  sendbackup: info end
  sendbackup: size 1050
  sendbackup: error [exec $cwd/amcat-error-no-exec: Permission denied]
  sendbackup: error [encrypt (PID) encrypt returned 1]
  sendbackup: native-CRC 00000000:0
  sendbackup: client-CRC 00000000:0
  sendbackup: end
  \\--------
  /-- localhost diskname2 lev 0 FAILED [exec $cwd/amcat-error-no-exec: Permission denied]
  sendbackup: info BACKUP=APPLICATION
  sendbackup: info APPLICATION=amrandom
  sendbackup: info RECOVER_CMD=$Amanda::Paths::APPLICATION_DIR/amrandom restore [./file-to-restore]+
  sendbackup: info end
  sendbackup: size 1050
  sendbackup: error [exec $cwd/amcat-error-no-exec: Permission denied]
  sendbackup: error [encrypt (PID) encrypt returned 1]
  sendbackup: native-CRC 00000000:0
  sendbackup: client-CRC 00000000:0
  sendbackup: end
  \\--------


NOTES:
  planner: tapecycle (2) <= runspercycle (10)
  planner: Adding new disk localhost:diskname2.
  taper: Slot 1 without label can be labeled


DUMP SUMMARY:
                                                    DUMPER STATS     TAPER STATS
HOSTNAME     DISK        L ORIG-KB  OUT-KB  COMP%  MMM:SS     KB/s MMM:SS     KB/s
-------------------------- ---------------------- ---------------- ---------------
localhost    diskname2   0                    --      PARTIAL                     

(brought to you by Amanda version 4.0.0alpha.git.00388ecf)
END_REPORT

check_amreport($report, $timestamp, "amreport first amdump");

# amstatus

my $status = <<"END_STATUS";
Using: /amanda/h1/etc/amanda/TESTCONF/log/amdump.1
From Wed Jun 22 08:22:28 EDT 2016

localhost:diskname2 $timestamp 0         0k dump failed: [exec $cwd/amcat-error-no-exec: Permission denied] (00:00:00)

SUMMARY           dle       real  estimated
                            size       size
---------------- ----  ---------  ---------
disk            :   1
estimated       :   1                 1050k
flush
dump failed     :   1                 1050k           (  0.00%)
wait for dumping:   0                    0k           (  0.00%)
dumping to tape :   0         0k         0k (  0.00%) (  0.00%)
dumping         :   0         0k         0k (  0.00%) (  0.00%)
dumped          :   0         0k         0k (100.00%) (100.00%)
wait for writing
wait to flush
writing to tape
dumping to tape
failed to tape
taped

2 dumpers idle  : no-dumpers
TESTCONF    qlen: 0
               0:

network free kps: 80000
holding space   : 25k (100.00%)
chunker0 busy   : 00:00:00  ( 83.54%)
 dumper0 busy   : 00:00:00  (  5.99%)
 0 dumpers busy : 00:00:00  ( 99.56%)
 1 dumper busy  : 00:00:00  (  0.44%)
END_STATUS

check_amstatus($status, $tracefile, "amstatus first amdump");

#diag("reply: " . Data::Dumper::Dumper($reply));
#$rest->stop();
#exit;

$rest->stop();

Installcheck::Run::cleanup();
