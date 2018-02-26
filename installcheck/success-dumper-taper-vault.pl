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
use Amanda::DB::Catalog2;

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
plan tests => 84;

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

my $diskname2 = $diskname;
my $diskname3 = "$diskname/AA";
my $diskname2X = sprintf "%-130s", $diskname2;
my $diskname3X = sprintf "%-130s", $diskname3;
my $diskname2S = ' ' x length($diskname2);
my $diskname3S = ' ' x length($diskname3);
my $disknameSS = ' ' x 130;
$testconf = Installcheck::Run::setup();
Installcheck::Run::setup_storage($testconf, '1', 3, vault => '"storage-2" 0', runtapes => 3 );
Installcheck::Run::setup_storage($testconf, '2', 3, runtapes => 3 );
$testconf->add_param('columnspec', '"Disk=1:130:130"');
$testconf->add_param('storage', '"storage-1"');
$testconf->add_param('vault-storage', '"storage-2"');
$testconf->add_dle(<<EODLE);
localhost $diskname2 {
    installcheck-test
    program "APPLICATION"
    application {
        plugin "amrandom"
        property "SIZE" "1075200"
    }
    holdingdisk never
}
EODLE
$testconf->add_dle(<<EODLE);
localhost $diskname3 {
    installcheck-test
    program "APPLICATION"
    application {
        plugin "amrandom"
        property "SIZE" "1075200"
    }
    holdingdisk never
}
EODLE
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();
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
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[1], "  planner: Adding new disk localhost:$diskname2." , 'notes[1] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[2], "  planner: Adding new disk localhost:$diskname3." , 'notes[2] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[3], '  taper: Storage \'storage-1\': slot 1: without label can be labeled' , 'notes[3] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[4], '  taper: tape STO-1-00001 kb 1050 fm 1 [OK]' , 'notes[4] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[5], '  taper: Storage \'storage-1\': slot 2: without label can be labeled' , 'notes[5] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[6], '  taper: tape STO-1-00002 kb 1050 fm 1 [OK]' , 'notes[6] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[7], '  taper: Storage \'storage-2\': slot 1: without label can be labeled' , 'notes[7] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[8], '  taper: tape STO-2-00001 kb 1050 fm 1 [OK]' , 'notes[8] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[9], '  taper: Storage \'storage-2\': slot 2: without label can be labeled' , 'notes[9] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
is($reply->{'body'}->[0]->{'report'}->{'notes'}->[10], '  taper: tape STO-2-00002 kb 1050 fm 1 [OK]' , 'notes[10] is correct') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
ok(!exists $reply->{'body'}->[0]->{'report'}->{'notes'}->[11], 'no notes[11]') || diag("notes: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'notes'}));
ok(!exists $reply->{'body'}->[0]->{'report'}->{'failure_summary'}, 'no failure_summary');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'nb'}, '1' , 'one dle on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'nc'}, '1' , 'one part on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'tape_label'}, 'STO-1-00001' , 'label tape_label on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[0]->{'size'}, '1050' , 'size 1050  on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[1]->{'nb'}, '1' , 'one dle on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[1]->{'nc'}, '1' , 'one part on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[1]->{'tape_label'}, 'STO-1-00002' , 'label tape_label on tape 1');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[1]->{'size'}, '1050' , 'size 1050  on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[2]->{'nb'}, '1' , 'one dle on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[2]->{'nc'}, '1' , 'one part on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[2]->{'tape_label'}, 'STO-2-00001' , 'label tape_label on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[2]->{'size'}, '1050' , 'size 1050  on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[3]->{'nb'}, '1' , 'one dle on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[3]->{'nc'}, '1' , 'one part on tape 0');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[3]->{'tape_label'}, 'STO-2-00002' , 'label tape_label on tape 1');
is($reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[3]->{'size'}, '1050' , 'size 1050  on tape 0');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'usage_by_tape'}->[4], '4 tapes used');
is_deeply($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}->{'storage-1'}->{'use'}, [ 'STO-1-00001', 'STO-1-00002'], 'use storage-1');
is_deeply($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}->{'storage-2'}->{'use'}, [ 'STO-2-00001', 'STO-2-00002'], 'use storage-2') or diag ("storage: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'tapeinfo'}->{'storage'}));
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tape_size'}, { 'full' => '4200',
									     'total' => '4200',
									     'incr' => '0' }, 'tape_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'parts_taped'}, { 'full' => '4',
									       'total' => '4',
									       'incr' => '0' }, 'parts_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_taped'}, { 'full' => '4',
									      'total' => '4',
									      'incr' => '0' }, 'dles_taped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dles_dumped'}, { 'full' => '2',
									       'total' => '2',
									       'incr' => '0' }, 'dles_dumped is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'original_size'}, { 'full' => '2100',
									         'total' => '2100',
									         'incr' => '0' }, 'original_size is correct');
is_deeply($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'output_size'}, { 'full' => '2100',
									       'total' => '2100',
									       'incr' => '0' }, 'output_size is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'dumpdisks'}, '', 'dumpdisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapedisks'}, '', 'tapedisks is correct');
is($reply->{'body'}->[0]->{'report'}->{'statistic'}->{'tapeparts'}, '', 'tapeparts is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'backup_level'}, '0', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'disk_name'}, "$diskname2", 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'hostname'}, 'localhost', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_orig_kb'}, '1050', 'dump_orig_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dump_out_kb'}, '1050', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[0]->{'dle_status'}, 'full', 'dle_status is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[1]->{'backup_level'}, '', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[1]->{'disk_name'}, "", 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[1]->{'hostname'}, '', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[1]->{'dump_orig_kb'}, '', 'dump_orig_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[1]->{'dump_out_kb'}, '', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[1]->{'dle_status'}, 'nodump-VAULT', 'dle_status is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[2]->{'backup_level'}, '0', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[2]->{'disk_name'}, "$diskname3", 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[2]->{'hostname'}, 'localhost', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[2]->{'dump_orig_kb'}, '1050', 'dump_orig_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[2]->{'dump_out_kb'}, '1050', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[2]->{'dle_status'}, 'full', 'dle_status is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[3]->{'backup_level'}, '', 'backup_level is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[3]->{'disk_name'}, "", 'disk_name is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[3]->{'hostname'}, '', 'hostname is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[3]->{'dump_orig_kb'}, '', 'dump_orig_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[3]->{'dump_out_kb'}, '', 'dump_out_kb is correct');
is($reply->{'body'}->[0]->{'report'}->{'summary'}->[3]->{'dle_status'}, 'nodump-VAULT', 'dle_status is correct');
ok(!exists $reply->{'body'}->[0]->{'report'}->{'summary'}->[9], 'four summary') or diag("summary: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'report'}->{'summary'}));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?tracefile=$tracefile");
is($reply->{'body'}->[0]->{'severity'}, 'info', 'severity is info');
is($reply->{'body'}->[0]->{'code'}, '1800000', 'code is 1800000');
is($reply->{'body'}->[0]->{'status'}->{'dead_run'}, '1', 'dead_run is correct');
is($reply->{'body'}->[0]->{'status'}->{'exit_status'}, '0', 'exit_status is correct');
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname2}->{$timestamp}->{'chunk_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname2}->{$timestamp}->{'dump_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname3}->{$timestamp}->{'chunk_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname3}->{$timestamp}->{'dump_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname2}->{$timestamp}->{'storage'}->{'storage-1'}->{'taper_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname2}->{$timestamp}->{'storage'}->{'storage-2'}->{'taper_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname3}->{$timestamp}->{'storage'}->{'storage-1'}->{'taper_time'} = undef;
$reply->{'body'}->[0]->{'status'}->{'dles'}->{'localhost'}->{$diskname3}->{$timestamp}->{'storage'}->{'storage-2'}->{'taper_time'} = undef;
is_deeply($reply->{'body'}->[0]->{'status'}->{'dles'},
    {
	'localhost' => {
		$diskname2 => {
			$timestamp => {
				'taped' => '1',
				'retry' => '0',
				'size' => '1075200',
				'esize' => '1075200',
				'retry_level' => '-1',
				#'message' => 'dump to tape done',
				'chunk_time' => undef,
				'dsize' => '1075200',
				'status' => '28',
				#'partial' => '0',
				'level' => '0',
				'dump_time' => undef,
				'degr_level' => '-1',
				'storage' => {
					'storage-1' => {
						'will_retry' => '0',
						'status' => '21',
						'dsize' => '1075200',
						'taper_time' => undef,
						'taped_size' => '1075200',
						'message' => 'dump to tape done',
						'size' => '1075200',
						'partial' => '0'
					},
					'storage-2' => {
						'will_retry' => '0',
						'status' => '28',
						'dsize' => '1075200',
						'taper_time' => undef,
						'taped_size' => '1075200',
						'message' => 'vaulted',
						'size' => '1075200',
						'partial' => '0',
						'vaulting' =>'1',
						'src_storage' => 'storage-1',
						'src_label' => 'STO-1-00002',
						'src_pool' => 'POOL-1',
					}
				},
				'dump_to_tape_storage' => 'storage-1',
				'flush' => '0',
				'will_retry' => '0'
			}
		},
		$diskname3 => {
			$timestamp => {
				'taped' => '1',
				'retry' => '0',
				'size' => '1075200',
				'esize' => '1075200',
				'retry_level' => '-1',
				#'message' => 'dump to tape done',
				'chunk_time' => undef,
				'dsize' => '1075200',
				'status' => '28',
				#'partial' => '0',
				'level' => '0',
				'dump_time' => undef,
				'degr_level' => '-1',
				'storage' => {
					'storage-1' => {
						'will_retry' => '0',
						'status' => '21',
						'dsize' => '1075200',
						'taper_time' => undef,
						'taped_size' => '1075200',
						'message' => 'dump to tape done',
						'size' => '1075200',
						'partial' => '0'
					},
					'storage-2' => {
						'will_retry' => '0',
						'status' => '28',
						'dsize' => '1075200',
						'taper_time' => undef,
						'taped_size' => '1075200',
						'message' => 'vaulted',
						'size' => '1075200',
						'partial' => '0',
						'vaulting' =>'1',
						'src_storage' => 'storage-1',
						'src_label' => 'STO-1-00001',
						'src_pool' => 'POOL-1',
					}
				},
				'dump_to_tape_storage' => 'storage-1',
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
				'no_tape' => '1'
			}
		},
		'tape_size' => '31457280',
		'storage' => 'storage-1',
		'nb_tape' => '2',
		'stat' => [
			{
				'size' => '1075200',
				'esize' => '1075200',
				'nb_dle' => '1',
				'nb_part' => '1',
				'label' => 'STO-1-00001',
				'percent' => '3.41796875'
			},
			{
				'size' => '1075200',
				'esize' => '1075200',
				'nb_dle' => '1',
				'nb_part' => '1',
				'label' => 'STO-1-00002',
				'percent' => '3.41796875'
			}
		]
	},
	'taper1' => {
		'worker' => {
			'worker1-0' => {
				'status' => '0',
				'no_tape' => '1',
				'message' => 'Idle'
			}
		},
		'tape_size' => '31457280',
		'storage' => 'storage-2',
		'nb_tape' => '2',
		'stat' => [
			{
				'size' => '1075200',
				'esize' => '1075200',
				'nb_dle' => '1',
				'nb_part' => '1',
				'label' => 'STO-2-00001',
				'percent' => '3.41796875'
			},
			{
				'size' => '1075200',
				'esize' => '1075200',
				'nb_dle' => '1',
				'nb_part' => '1',
				'label' => 'STO-2-00002',
				'percent' => '3.41796875'
			}
		]
	}
    },
    'taper is correct') or diag("taper: " . Data::Dumper::Dumper($reply->{'body'}->[0]->{'status'}->{'taper'}));

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
		'estimated_size' => '2150400',
		'nb' => '2'
	},
	'taped' => {
		'name' => 'taped',
		'estimated_size' => '2150400',
		'storage' => {
			'storage-1' => {
				'estimated_stat' => '100',
				'real_size' => '2150400',
				'nb' => '2',
				'real_stat' => '100',
				'estimated_size' => '2150400'
			},
			'storage-2' => {
				'estimated_stat' => '100',
				'real_size' => '2150400',
				'nb' => '2',
				'real_stat' => '100',
				'estimated_size' => '2150400'
			}
		},
	},
	'dumped' => {
		'name' => 'dumped',
		'estimated_stat' => '100',
		'real_size' => '2150400',
		'nb' => '2',
		'real_stat' => '100',
		'estimated_size' => '2150400'
	},
	'disk' => {
		'name' => 'disk',
		'nb' => '2',
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
	},
	'wait_to_vault' => {
		'name' => 'wait_to_vault'
	},
	'vaulting' => {
		'name' => 'vaulting'
	},
	'vaulted' => {
		'name' => 'vaulted',
		'nb' => '2',
		'estimated_size' => '2150400',
		'real_size' => '2150400',
	}
    },
    'stat is correct');

# amreport

my $report = <<"END_REPORT";
Hostname: localhost.localdomain
Org     : DailySet1
Config  : TESTCONF
Date    : June 21, 2016

These dumps to storage 'storage-1' were to tapes STO-1-00001, STO-1-00002.
These dumps to storage 'storage-2' were to tapes STO-2-00001, STO-2-00002.
The next 3 tapes Amanda expects to use for storage 'storage-1' are: 1 new tape, STO-1-00001, STO-1-00002.
The next 3 tapes Amanda expects to use for storage 'storage-2' are: 1 new tape, STO-2-00001, STO-2-00002.


STATISTICS:
                          Total       Full      Incr.   Level:#
                        --------   --------   --------  --------
Estimate Time (hrs:min)     0:00
Run Time (hrs:min)          0:00
Dump Time (hrs:min)         0:00       0:00       0:00
Output Size (meg)            2.1        2.1        0.0
Original Size (meg)          2.1        2.1        0.0
Avg Compressed Size (%)    100.0      100.0        --
DLEs Dumped                    2          2          0
Avg Dump Rate (k/s) 999999.9 999999.9 *--

Tape Time (hrs:min)         0:00       0:00       0:00
Tape Size (meg)              4.1        4.1        0.0
Tape Used (%)               13.8       13.8        0.0
DLEs Taped                     4          4          0
Parts Taped                    4          4          0
Avg Tp Write Rate (k/s) 999999.9 999999.9 *--


USAGE BY TAPE:
  Label                 Time         Size      %  DLEs Parts
  STO-1-00001           0:00        1050K    3.4     1     1
  STO-1-00002           0:00        1050K    3.4     1     1
  STO-2-00001           0:00        1050K    3.4     1     1
  STO-2-00002           0:00        1050K    3.4     1     1


NOTES:
  planner: tapecycle (2) <= runspercycle (10)
  planner: Adding new disk localhost:$diskname2.
  planner: Adding new disk localhost:$diskname3.
  taper: Storage 'storage-1': slot 1: without label can be labeled
  taper: tape STO-1-00001 kb 1050 fm 1 [OK]
  taper: Storage 'storage-1': slot 2: without label can be labeled
  taper: tape STO-1-00002 kb 1050 fm 1 [OK]
  taper: Storage 'storage-2': slot 1: without label can be labeled
  taper: tape STO-2-00001 kb 1050 fm 1 [OK]
  taper: Storage 'storage-2': slot 2: without label can be labeled
  taper: tape STO-2-00002 kb 1050 fm 1 [OK]


DUMP SUMMARY:
                                                                                                                                                                          DUMPER STATS   TAPER STATS
HOSTNAME     DISK                                                                                                                               L ORIG-KB  OUT-KB  COMP%  MMM:SS   KB/s MMM:SS    KB/s
------------------------------------------------------------------------------------------------------------------------------------------------- ---------------------- -------------- --------------
localhost    $diskname2X 0    1050    1050    --     0:00 999999.9   0:00 999999.9
             $disknameSS                              VAULT        0:00 999999.9
localhost    $diskname3X 0    1050    1050    --     0:00 999999.9   0:00 999999.9
             $disknameSS                              VAULT        0:00 999999.9

(brought to you by Amanda version x.y.z)
END_REPORT

check_amreport($report, $timestamp, "amreport first amdump");

# amstatus

my $status = <<"END_STATUS";
Using: /amanda/h1/etc/amanda/TESTCONF/log/amdump.1
From Wed Jun 22 08:01:00 EDT 2016

localhost:$diskname2    $timestamp 0      1050k (storage-1) dump to tape done (00:00:00)
          $diskname2S                                (storage-2) vaulted (00:00:00)
localhost:$diskname3 $timestamp 0      1050k (storage-1) dump to tape done (00:00:00)
          $diskname3S                             (storage-2) vaulted (00:00:00)

SUMMARY           dle       real  estimated
                            size       size
---------------- ----  ---------  ---------
disk            :   2
estimated       :   2                 2100k
flush
dump failed     :   0                    0k           (  0.00%)
wait for dumping:   0                    0k           (  0.00%)
dumping to tape :   0         0k         0k (  0.00%) (  0.00%)
dumping         :   0         0k         0k (  0.00%) (  0.00%)
dumped          :   2      2100k      2100k (100.00%) (100.00%)
wait for writing
wait to flush
writing to tape
dumping to tape
failed to tape
taped
  storage-1     :   2      2100k      2100k (100.00%) (100.00%)
    tape 1      :   1      1050k      1050k (  3.42%) STO-1-00001 (1 parts)
    tape 2      :   1      1050k      1050k (  3.42%) STO-1-00002 (1 parts)
  storage-2     :   2      2100k      2100k (100.00%) (100.00%)
    tape 1      :   1      1050k      1050k (  3.42%) STO-2-00001 (1 parts)
    tape 2      :   1      1050k      1050k (  3.42%) STO-2-00002 (1 parts)

2 dumpers idle  : no-dumpers
storage-1   qlen: 0
               0:

storage-2   qlen: 0
               0: Idle

network free kps: 80000
holding space   : 25k (100.00%)
  dumper0 busy  : 00:00:00  ( 52.93%)
storage-1 busy  : 00:00:00  ( 53.46%)
storage-2 busy  : 00:00:00  (199.47%)
 0 dumpers busy : 00:00:00  ( 98.99%)no-dumpers: 00:00:00  ( 68.19%)
 1 dumper busy  : 00:00:00  (  1.01%)
END_STATUS

check_amstatus($status, $tracefile, "amstatus first amdump");

#diag("reply: " . Data::Dumper::Dumper($reply));
#$rest->stop();
#exit;

$rest->stop();

#Installcheck::Run::cleanup();
