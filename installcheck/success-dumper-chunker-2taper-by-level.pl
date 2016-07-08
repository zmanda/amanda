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
plan tests => 104;

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
Installcheck::Run::setup_storage($testconf, 1, 2, 'dump-selection', "\"random\" FULL");
Installcheck::Run::setup_storage($testconf, 2, 2, 'dump-selection', "\"random\" INCR");
$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    tag "random"
    application {
        plugin "amrandom"
        property "SIZE" "1075200"
        property "SIZE-LEVEL-1" "102400"
    }
}
EODLE
$testconf->add_param('storage', '"storage-1" "storage-2"');
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
$diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
$infodir = getconf($CNF_INFOFILE);

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs/amdump","");
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
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'exit_status'}, '0' , 'exit_status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'status'}, 'done' , 'status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'org'}, 'DailySet1' , 'org is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'config_name'}, 'TESTCONF' , 'config_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'timestamp'}, $timestamp , 'timestamp is correct');
my @sorted_notes = sort @{$reply->{'body'}->[0]->{'report'}->{'notes'}};
is($sorted_notes[0], '  planner: Adding new disk localhost:diskname2.' , 'notes[0] is correct');
is($sorted_notes[1], '  planner: tapecycle (2) <= runspercycle (10)', 'notes[1] is correct');
is($sorted_notes[2], '  taper: Slot 1 without label can be labeled' , 'notes[2] is correct');
is($sorted_notes[3], '  taper: Slot 1 without label can be labeled' , 'notes[3] is correct');
is($sorted_notes[4], '  taper: tape STO-1-00001 kb 1050 fm 1 [OK]' , 'notes[4] is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'notes'}->[5], 'no notes[5]');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'failure_summary'}, 'no failure_summary');
my @sorted_usage_by_tape = sort { $a->{'tape_label'} cmp $b->{'tape_label'}} @{$reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}};
is($sorted_usage_by_tape[0]->{'nb'}, '1' , 'one dle on tape 0');
is($sorted_usage_by_tape[0]->{'nc'}, '1' , 'one part on tape 0');
is($sorted_usage_by_tape[0]->{'tape_label'}, 'STO-1-00001' , 'label tape_label on tape 0');
is($sorted_usage_by_tape[0]->{'size'}, '1050' , 'size 1050  on tape 0');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[1], 'use one tape');
is($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}->{'TESTCONF'}, undef, 'storage TESTCONF is not set');
is_deeply($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}->{'storage-1'}->{'use'}, [ 'STO-1-00001'], 'use storage-1');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tape_size'}, { 'full' => '1050',
									     'total' => '1050',
									     'incr' => '0' }, 'tape_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'parts_taped'}, { 'full' => '1',
									       'total' => '1',
									       'incr' => '0' }, 'parts_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_taped'}, { 'full' => '1',
									      'total' => '1',
									      'incr' => '0' }, 'dles_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_dumped'}, { 'full' => '1',
									       'total' => '1',
									       'incr' => '0' }, 'dles_dumped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'original_size'}, { 'full' => '1050',
									         'total' => '1050',
									         'incr' => '0' }, 'original_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'output_size'}, { 'full' => '1050',
									       'total' => '1050',
									       'incr' => '0' }, 'output_size is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dumpdisks'}, '', 'dumpdisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapedisks'}, '', 'tapedisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapeparts'}, '', 'tapeparts is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'backup_level'}, '0', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'disk_name'}, 'diskname2', 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'hostname'}, 'localhost', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_orig_kb'}, '1050', 'dump_orig_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_out_kb'}, '1050', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dle_status'}, 'full', 'dle_status is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'summary'}->[1], 'Only one summary');

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?tracefile=$tracefile");
is($reply->{'body'}->[0]->{'severity'}, 'info', 'severity is info');
is($reply->{'body'}->[0]->{'code'}, '1800000', 'code is 1800000');
is($reply->{'body'}->[0]->{'status'}->{'dead_run'}, '1', 'dead_run is correct');
is($reply->{'body'}->[0]->{'status'}->{'exit_status'}, '0', 'exit_status is correct');
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'chunk_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'dump_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'storage'}->{'storage-1'}->{'taper_time'} = undef;
is_deeply($reply->{'body'}->[0]->{'status'}->{'dles'},
    {
	'localhost' => {
		'diskname2' => {
			$timestamp => {
				'taped' => '1',
				'retry' => '0',
				'size' => '1075200',
				'esize' => '1075200',
				'retry_level' => '-1',
				'message' => 'dump done',
				'chunk_time' => undef,
				'dsize' => '1075200',
				'status' => '20',
				'partial' => '0',
				'level' => '0',
				'dump_time' => undef,
				'error' => '',
				'holding_file' => "$Installcheck::TMP/holding/$timestamp/localhost.diskname2.0",
				'degr_level' => '-1',
				'storage' => {
					'storage-1' => {
						'will_retry' => '0',
						'status' => '22',
						'dsize' => '1075200',
						'taper_time' => undef,
						'error' => '',
						'taped_size' => '1075200',
						'message' => 'written',
						'size' => '1075200',
						'partial' => '0'
					},
				},
				'flush' => '0',
				'will_retry' => '0'
			}
		}
	}
    },
    'dles is correct');

is_deeply($reply->{'body'}->[0]->{'status'}->{'taper'},
    {
	'taper0' => {
		'worker' => {
			'worker0-0' => {
				'status' => '0',
				'no_tape' => '0'
			}
		},
		'tape_size' => '31457280',
		'storage' => 'storage-1',
		'nb_tape' => '1',
		'stat' => [
			{
				'size' => '1075200',
				'esize' => '1075200',
				'nb_dle' => '1',
				'nb_part' => '1',
				'label' => 'STO-1-00001',
				'percent' => '3.41796875'
			}
		]
	},
	'taper1' => {
		'worker' => {
			'worker1-0' => {
				'status' => '0',
			}
		},
		'tape_size' => '31457280',
		'storage' => 'storage-2',
		'nb_tape' => '0',
	}
    },
    'taper is correct');

is($reply->{'body'}->[0]->{'status'}->{'storage'}->{'storage-1'}->{'taper'}, 'taper0', 'taper is correct');
is($reply->{'body'}->[0]->{'status'}->{'storage'}->{'storage-2'}->{'taper'}, 'taper1', 'taper is correct');
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
		'nb' => '0',
		'estimated_size' => '0'
	},
	'estimated' => {
		'name' => 'estimated',
		'real_size' => undef,
		'estimated_size' => '1075200',
		'nb' => '1'
	},
	'taped' => {
		'name' => 'taped',
		'estimated_size' => '1075200',
		'storage' => {
			'storage-1' => {
				'estimated_stat' => '100',
				'real_size' => '1075200',
				'nb' => '1',
				'real_stat' => '100',
				'estimated_size' => '1075200'
			},
		},
	},
	'dumped' => {
		'name' => 'dumped',
		'estimated_stat' => '100',
		'real_size' => '1075200',
		'nb' => '1',
		'real_stat' => '100',
		'estimated_size' => '1075200'
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
    'stat is correct');


# amreport
#
my $report = <<'END_REPORT';
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : June 22, 2016

These dumps to storage 'storage-1' were to tape STO-1-00001.
The next tape Amanda expects to use for storage 'storage-1' is: 1 new tape.
The next tape Amanda expects to use for storage 'storage-2' is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.   Level:#
                        --------   --------   --------  --------
Estimate Time (hrs:min)     0:00
Run Time (hrs:min)          0:00
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            1.0        1.0        0.0
Original Size (meg)          1.0        1.0        0.0
Avg Compressed Size (%)    100.0      100.0        --
DLEs Dumped                    1          1          0
Avg Dump Rate (k/s)     999999.9   999999.9        --

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)              1.0        1.0        0.0
Tape Used (%)                3.4        3.4        0.0
DLEs Taped                     1          1          0
Parts Taped                    1          1          0
Avg Tp Write Rate (k/s) 999999.9   999999.9        --


USAGE BY TAPE:
  Label                 Time         Size      %  DLEs Parts
  STO-1-00001           0:00        1050K    3.4     1     1


NOTES:
  planner: Adding new disk localhost:diskname2.
  planner: tapecycle (2) <= runspercycle (10)
  taper: Slot 1 without label can be labeled
  taper: Slot 1 without label can be labeled
  taper: tape STO-1-00001 kb 1050 fm 1 [OK]


DUMP SUMMARY:
                                                    DUMPER STATS     TAPER STATS
HOSTNAME     DISK        L ORIG-KB  OUT-KB  COMP%  MMM:SS     KB/s MMM:SS     KB/s
-------------------------- ---------------------- ---------------- ---------------
localhost    diskname2   0    1050    1050    --     0:00 999999.9   0:00 999999.9

(brought to you by Amanda version 4.0.0alpha.git.00388ecf)
END_REPORT

check_amreport($report, $timestamp, "amreport first amdump", 1);

# amstatus

my $status = <<"END_STATUS";
Using: /amanda/h1/etc/amanda/TESTCONF/log/amdump.1
From Wed Jun 22 08:22:28 EDT 2016

localhost:diskname2 $timestamp 0      1050k dump done (00:00:00), (storage-1) written (00:00:00)

SUMMARY           dle       real  estimated
                            size       size
---------------- ----  ---------  ---------
disk            :   1
estimated       :   1                 1050k
flush
dump failed     :   0                    0k           (  0.00%)
wait for dumping:   0                    0k           (  0.00%)
dumping to tape :   0         0k         0k (  0.00%) (  0.00%)
dumping         :   0         0k         0k (  0.00%) (  0.00%)
dumped          :   1      1050k      1050k (100.00%) (100.00%)
wait for writing
wait to flush
writing to tape
dumping to tape
failed to tape
taped
  storage-1     :   1      1050k      1050k (100.00%) (100.00%)
    tape 1      :   1      1050k      1050k (  3.42%) STO-1-00001 (1 parts)

2 dumpers idle  : no-dumpers
storage-1   qlen: 0
               0:  (:)

storage-2   qlen: 0
               0:  (:)

network free kps: 80000
holding space   : 25k (100.00%)
 chunker0 busy  : 00:00:00  ( 43.90%)
  dumper0 busy  : 00:00:00  (  5.99%)
storage-1 busy  : 00:00:00  (  1.77%)
 0 dumpers busy : 00:00:00  ( 99.56%)
 1 dumper busy  : 00:00:00  (  0.44%)
END_STATUS

check_amstatus($status, $tracefile, "amstatus first amdump");


$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=diskname2&force_level_1=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Curinfo.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'host' => 'localhost',
                'disk' => 'diskname2',
                'message' => "localhost:diskname2 is set to a forced level 1 at next run.",
                'process' => 'Amanda::Rest::Dles',
                'running_on' => 'amanda-server',
                'component' => 'rest-server',
                'module' => 'amanda',
                'code' => '1300023'
          },
        ],
      http_code => 200,
    },
    "get runs") || diag("reply: " . Data::Dumper::Dumper($reply));


$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs/amdump","");
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
    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs?logfile=$logfile");
    } while ($reply->{'body'}[0]->{'code'} == 2000004 and
             $reply->{'body'}[0]->{'status'} ne 'done');

# get REST report
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$logfile");

is($reply->{'body'}->[0]->{'severity'}, 'success', 'severity is success');
is($reply->{'body'}->[0]->{'code'}, '1900001', 'code is 1900001');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'hostname'}, $hostname , 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'exit_status'}, '0' , 'exit_status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'status'}, 'done' , 'status is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'org'}, 'DailySet1' , 'org is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'config_name'}, 'TESTCONF' , 'config_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'head'}->{'timestamp'}, $timestamp , 'timestamp is correct');
@sorted_notes = sort @{$reply->{'body'}->[0]->{'report'}->{'notes'}};
is($sorted_notes[0], '  planner: Forcing level 1 of localhost:diskname2 as directed.' , 'notes[0] is correct');
is($sorted_notes[1], '  planner: Last full dump of localhost:diskname2 on tape STO-1-00001 overwritten in 1 run.' , 'notes[1] is correct');
is($sorted_notes[2], '  planner: tapecycle (2) <= runspercycle (10)', 'notes[2] is correct');
is($sorted_notes[3], '  taper: Slot 1 with label STO-1-00001 is not reusable' , 'notes[3] is correct');
is($sorted_notes[4], '  taper: Slot 1 without label can be labeled' , 'notes[4] is correct');
is($sorted_notes[5], '  taper: Slot 2 without label can be labeled' , 'notes[5] is correct');
is($sorted_notes[6], '  taper: tape STO-2-00001 kb 100 fm 1 [OK]' , 'notes[6] is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'notes'}->[7], 'no notes[7]');
@sorted_usage_by_tape = sort { $a->{'tape_label'} cmp $b->{'tape_label'}} @{$reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}};
is($sorted_usage_by_tape[0]->{'nb'}, '1' , 'one dle on tape 0');
is($sorted_usage_by_tape[0]->{'nc'}, '1' , 'one part on tape 0');
is($sorted_usage_by_tape[0]->{'tape_label'}, 'STO-2-00001' , 'label tape_label on tape 0');
is($sorted_usage_by_tape[0]->{'size'}, '100' , 'size 100  on tape 0');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[1], 'use one tape');
is($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}->{'TESTCONF'}, undef, 'storage TESTCONF is not set');
is_deeply($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}->{'storage-2'}->{'use'}, [ 'STO-2-00001'], 'use storage-2');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tape_size'}, { 'full' => '0',
									     'total' => '100',
									     'incr' => '100' }, 'tape_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'parts_taped'}, { 'full' => '0',
									       'total' => '1',
									       'incr' => '1' }, 'parts_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_taped'}, { 'full' => '0',
									      'total' => '1',
									      'incr' => '1' }, 'dles_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_dumped'}, { 'full' => '0',
									       'total' => '1',
									       'incr' => '1' }, 'dles_dumped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'original_size'}, { 'full' => '0',
									         'total' => '100',
									         'incr' => '100' }, 'original_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'output_size'}, { 'full' => '0',
									       'total' => '100',
									       'incr' => '100' }, 'output_size is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dumpdisks'}, '1:1', 'dumpdisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapedisks'}, '1:1', 'tapedisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapeparts'}, '1:1', 'tapeparts is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'backup_level'}, '1', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'disk_name'}, 'diskname2', 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'hostname'}, 'localhost', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_orig_kb'}, '100', 'dump_orig_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_out_kb'}, '100', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dle_status'}, 'full', 'dle_status is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'summary'}->[1], 'Only one summary');

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?tracefile=$tracefile");
is($reply->{'body'}->[0]->{'severity'}, 'info', 'severity is info');
is($reply->{'body'}->[0]->{'code'}, '1800000', 'code is 1800000');
is($reply->{'body'}->[0]->{'status'}->{'dead_run'}, '1', 'dead_run is correct');
is($reply->{'body'}->[0]->{'status'}->{'exit_status'}, '0', 'exit_status is correct');
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'chunk_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'dump_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{'diskname2'}->{$timestamp}->{'storage'}->{'storage-2'}->{'taper_time'} = undef;
is_deeply($reply->{'body'}->[0]->{'status'}->{'dles'},
    {
	'localhost' => {
		'diskname2' => {
			$timestamp => {
				'taped' => '1',
				'retry' => '0',
				'size' => '102400',
				'esize' => '102400',
				'retry_level' => '-1',
				'message' => 'dump done',
				'chunk_time' => undef,
				'dsize' => '102400',
				'status' => '20',
				'partial' => '0',
				'level' => '1',
				'dump_time' => undef,
				'error' => '',
				'holding_file' => "$Installcheck::TMP/holding/$timestamp/localhost.diskname2.1",
				'degr_level' => '-1',
				'storage' => {
					'storage-2' => {
						'will_retry' => '0',
						'status' => '22',
						'dsize' => '102400',
						'taper_time' => undef,
						'error' => '',
						'taped_size' => '102400',
						'message' => 'written',
						'size' => '102400',
						'partial' => '0'
					},
				},
				'flush' => '0',
				'will_retry' => '0'
			}
		}
	}
    },
    'dles is correct');

is_deeply($reply->{'body'}->[0]->{'status'}->{'taper'},
    {
	'taper0' => {
		'worker' => {
			'worker0-0' => {
				'status' => '0',
			}
		},
		'tape_size' => '31457280',
		'storage' => 'storage-1',
		'nb_tape' => '0',
	},
	'taper1' => {
		'worker' => {
			'worker1-0' => {
				'status' => '0',
				'no_tape' => '0',
			}
		},
		'tape_size' => '31457280',
		'storage' => 'storage-2',
		'nb_tape' => '1',
		'stat' => [
			{
				'size' => '102400',
				'esize' => '102400',
				'nb_dle' => '1',
				'nb_part' => '1',
				'label' => 'STO-2-00001',
				'percent' => '0.325520833333333',
			}
		]
	}
    },
    'taper is correct');

is($reply->{'body'}->[0]->{'status'}->{'storage'}->{'storage-1'}->{'taper'}, 'taper0', 'taper is correct');
is($reply->{'body'}->[0]->{'status'}->{'storage'}->{'storage-2'}->{'taper'}, 'taper1', 'taper is correct');
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
		'nb' => '0',
		'estimated_size' => '0'
	},
	'estimated' => {
		'name' => 'estimated',
		'real_size' => undef,
		'estimated_size' => '102400',
		'nb' => '1'
	},
	'taped' => {
		'name' => 'taped',
		'estimated_size' => '102400',
		'storage' => {
			'storage-2' => {
				'estimated_stat' => '100',
				'real_size' => '102400',
				'nb' => '1',
				'real_stat' => '100',
				'estimated_size' => '102400'
			},
		},
	},
	'dumped' => {
		'name' => 'dumped',
		'estimated_stat' => '100',
		'real_size' => '102400',
		'nb' => '1',
		'real_stat' => '100',
		'estimated_size' => '102400'
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
    'stat is correct');


# amreport
#
$report = <<'END_REPORT';
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : June 22, 2016

These dumps to storage 'storage-2' were to tape STO-2-00001.
The next tape Amanda expects to use for storage 'storage-1' is: 1 new tape.
The next tape Amanda expects to use for storage 'storage-2' is: 1 new tape.


STATISTICS:
                          Total       Full      Incr.   Level:#
                        --------   --------   --------  --------
Estimate Time (hrs:min)     0:00
Run Time (hrs:min)          0:00
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            0.1        0.0        0.1
Original Size (meg)          0.1        0.0        0.1
Avg Compressed Size (%)    100.0        --       100.0
DLEs Dumped                    1          0          1  1:1
Avg Dump Rate (k/s)     999999.9        --    999999.9

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)              0.1        0.0        0.1
Tape Used (%)                0.4        0.0        0.4
DLEs Taped                     1          0          1  1:1
Parts Taped                    1          0          1  1:1
Avg Tp Write Rate (k/s) 999999.9        --    999999.9


USAGE BY TAPE:
  Label                 Time         Size      %  DLEs Parts
  STO-2-00001           0:00         100K    0.4     1     1


NOTES:
  planner: Forcing level 1 of localhost:diskname2 as directed.
  planner: Last full dump of localhost:diskname2 on tape STO-1-00001 overwritten in 1 run.
  planner: tapecycle (2) <= runspercycle (10)
  taper: Slot 1 with label STO-1-00001 is not reusable
  taper: Slot 1 without label can be labeled
  taper: Slot 2 without label can be labeled
  taper: tape STO-2-00001 kb 100 fm 1 [OK]


DUMP SUMMARY:
                                                    DUMPER STATS     TAPER STATS
HOSTNAME     DISK        L ORIG-KB  OUT-KB  COMP%  MMM:SS     KB/s MMM:SS     KB/s
-------------------------- ---------------------- ---------------- ---------------
localhost    diskname2   1     100     100    --     0:00 999999.9   0:00 999999.9

(brought to you by Amanda version 4.0.0alpha.git.00388ecf)
END_REPORT

check_amreport($report, $timestamp, "amreport second amdump", 1);

# amstatus

$status = <<"END_STATUS";
Using: /amanda/h1/etc/amanda/TESTCONF/log/amdump.1
From Wed Jun 22 08:22:28 EDT 2016

localhost:diskname2 $timestamp 1       100k dump done (00:00:00), (storage-2) written (00:00:00)

SUMMARY           dle       real  estimated
                            size       size
---------------- ----  ---------  ---------
disk            :   1
estimated       :   1                  100k
flush
dump failed     :   0                    0k           (  0.00%)
wait for dumping:   0                    0k           (  0.00%)
dumping to tape :   0         0k         0k (  0.00%) (  0.00%)
dumping         :   0         0k         0k (  0.00%) (  0.00%)
dumped          :   1       100k       100k (100.00%) (100.00%)
wait for writing
wait to flush
writing to tape
dumping to tape
failed to tape
taped
  storage-2     :   1       100k       100k (100.00%) (100.00%)
    tape 1      :   1       100k       100k (  3.42%) STO-2-00001 (1 parts)

2 dumpers idle  : no-dumpers
storage-1   qlen: 0
               0:  (:)

storage-2   qlen: 0
               0:  (:)

network free kps: 80000
holding space   : 25k (100.00%)
 chunker0 busy  : 00:00:00  ( 43.90%)
  dumper0 busy  : 00:00:00  (  5.99%)
storage-2 busy  : 00:00:00  (  1.77%)
 0 dumpers busy : 00:00:00  ( 99.56%)
 1 dumper busy  : 00:00:00  (  0.44%)
END_STATUS

check_amstatus($status, $tracefile, "amstatus second amdump");

#diag("reply: " . Data::Dumper::Dumper($reply));
#$rest->stop();
#exit;

$rest->stop();

Installcheck::Run::cleanup();
