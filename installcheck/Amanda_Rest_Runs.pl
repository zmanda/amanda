# Copyright (c) 2014 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More;
use File::Path;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
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
plan tests => 29;

# write a fake holding file to holding disk, for amflush to flush
#  from amflush.pl
sub write_holding_file {
    my ($host, $disk) = @_;

    my $datestamp = "20100102030405";
    my $filename = "$Installcheck::Run::holdingdir/$datestamp/$host-somefile";

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = $datestamp;
    $hdr->{'dumplevel'} = 0;
    $hdr->{'name'} = $host;
    $hdr->{'disk'} = $disk;
    $hdr->{'program'} = "INSTALLCHECK";

    mkpath($Installcheck::Run::holdingdir);
    mkpath("$Installcheck::Run::holdingdir/$datestamp");
    open(my $fh, ">", $filename) or die("opening '$filename': $!");
    print $fh $hdr->to_string(32768,32768);
    print $fh "some data!\n";
    close($fh);
}

my $reply;

my $config_dir = $Amanda::Paths::CONFIG_DIR;
my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
# one AMGTAR dle
$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
        plugin "amgtar"
        property "ATIME-PRESERVE" "NO"
    }
}
EODLE
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
my $infodir = getconf($CNF_INFOFILE);

my $timestamp;
my $amdump_log;
my $trace_log;

#CODE 2000000, 2000001, 2000002, 2000003
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs/amdump","");
foreach my $message (@{$reply->{'body'}}) {
    if (defined $message and defined $message->{'code'}) {
	if ($message->{'code'} == 2000003) {
	    $timestamp = $message->{'timestamp'};
	}
	if ($message->{'code'} == 2000001) {
	    $amdump_log = $message->{'amdump_log'};
	}
	if ($message->{'code'} == 2000000) {
	    $trace_log = $message->{'trace_log'};
	}
    }
}
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Amdump.pm",
		'severity' => '2',
		'message' => "The timestamp is '$timestamp'",
		'timestamp' => $timestamp,
		'process' => 'amdump',
		'running_on' => 'amanda-server',
		'code' => '2000003'
	  },
          {	'source_filename' => "$amperldir/Amanda/Amdump.pm",
		'severity' => '2',
		'message' => "The amdump log file is '$amdump_log'",
		'amdump_log' => $amdump_log,
		'process' => 'amdump',
		'running_on' => 'amanda-server',
		'code' => '2000001'
	  },
          {	'source_filename' => "$amperldir/Amanda/Amdump.pm",
		'severity' => '2',
		'message' => "The trace log file is '$trace_log'",
		'trace_log' => $trace_log,
		'process' => 'amdump',
		'running_on' => 'amanda-server',
		'code' => '2000000'
	  },
          {	'source_filename' => "$amperldir/Amanda/Rest/Runs.pm",
		'severity' => '2',
		'message' => 'Running a dump',
		'process' => 'amdump',
		'running_on' => 'amanda-server',
		'code' => '2000002'
	  }
        ],
      http_code => 202,
    },
    "post amdump") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1500001
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/BADCONF/runs/amdump","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => '16',
		'cfgerror' => "parse error: could not open conf file '$config_dir/BADCONF/amanda.conf': No such file or directory",
		'message' => "config error: parse error: could not open conf file '$config_dir/BADCONF/amanda.conf': No such file or directory",
		'process' => 'Amanda::Rest::Runs',
		'running_on' => 'amanda-server',
		'code' => '1500001'
	  },
        ],
      http_code => 200,
    },
    "post amdump bad config") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 2000004
#wait for the run to end
do {
    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs");
} while ($reply->{'body'}[0]->{'code'} == 2000004 and
	 $reply->{'body'}[0]->{'status'} ne 'done');
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Runs.pm",
		'severity' => '16',
		'message' => "one run",
		'run_type' => "amdump",
		'timestamp' => $timestamp,
		'amdump_log' => $amdump_log,
		'trace_log' => $trace_log,
		'status' => 'done',
		'process' => 'Amanda::Rest::Runs',
		'running_on' => 'amanda-server',
		'code' => '2000004'
	  },
        ],
      http_code => 200,
    },
    "get runs") || diag("reply: " . Data::Dumper::Dumper($reply));

my $cat;
$testconf = Installcheck::Run::setup();
$testconf->write();

$cat = Installcheck::Catalogs::load('normal');
$cat->install();

my $current_time;
my $starttime;
my $datestamp;
my $chunk_time;
my $dump_time;
my $logdir = getconf($CNF_LOGDIR);
$amdump_log = "$logdir/amdump";
$trace_log = "$logdir/log";
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?amdump_log=$amdump_log");
$starttime = $reply->{body}[0]{status}{starttime};
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'source_filename' => "$amperldir/Amanda/Status.pm",
                         'status' => {
                                       'free_space' => '868352',
                                       'current_time' => '6.418',
                                       'taper' => {
                                                    'taper0' => {
                                                                  'tape_size' => '102400',
                                                                  'worker' => {
                                                                                'worker0-0' => {
                                                                                                 'status' => 0,
                                                                                                 'no_tape' => 0,
                                                                                                 'message' => 'Idle'
                                                                                               }
                                                                              },
                                                                  'nb_tape' => 1,
                                                                  'storage' => 'TESTCONF',
                                                                  'stat' => [
                                                                              {
                                                                                'esize' => 0,
                                                                                'percent' => 0,
                                                                                'nb_part' => 0,
                                                                                'nb_dle' => 1,
                                                                                'label' => 'Conf-001',
                                                                                'size' => 0
                                                                              }
                                                                            ]
                                                                }
                                                  },
                                       'dead_run' => 1,
                                       'starttime' => $starttime,
                                       'holding_space' => 868352,
                                       'free_kps' => '600',
                                       'qlen' => {
                                                   'tapeq' => {
                                                                'taper0' => 0
                                                              },
                                                   'roomq' => '0',
                                                   'runq' => '0'
                                                 },
                                       'datestamp' => '20080618130147',
                                       'exit_status' => 0,
                                       'stat' => {
                                                   'dumping' => {
                                                                  'real_size' => 0,
                                                                  'nb' => 0,
                                                                  'real_stat' => 0,
                                                                  'estimated_size' => 0,
                                                                  'name' => 'dumping',
                                                                  'estimated_stat' => 0
                                                                },
                                                   'dumping_to_tape' => {
                                                                          'real_size' => 0,
                                                                          'nb' => 0,
                                                                          'real_stat' => 0,
                                                                          'estimated_size' => 0,
                                                                          'name' => 'dumping to tape',
                                                                          'estimated_stat' => 0
                                                                        },
                                                   'dump_failed' => {
                                                                      'real_size' => undef,
                                                                      'nb' => 0,
                                                                      'estimated_size' => 0,
                                                                      'name' => 'dump failed',
                                                                      'estimated_stat' => 0
                                                                    },
                                                   'disk' => {
                                                               'real_size' => undef,
                                                               'nb' => 1,
                                                               'estimated_size' => undef,
                                                               'name' => 'disk'
                                                             },
                                                   'wait_to_flush' => {
                                                                        'name' => 'wait_to_flush'
                                                                      },
                                                   'writing_to_tape' => {
                                                                          'name' => 'writing to tape'
                                                                        },
                                                   'taped' => {
                                                                'estimated_size' => 100,
                                                                'name' => 'taped',
                                                                'storage' => {
                                                                               'TESTCONF' => {
                                                                                               'real_size' => 100,
                                                                                               'nb' => 1,
                                                                                               'real_stat' => 100,
                                                                                               'estimated_size' => 100,
                                                                                               'estimated_stat' => 100
                                                                                             }
                                                                             }
                                                              },
                                                   'dumped' => {
                                                                 'real_size' => 100,
                                                                 'nb' => 1,
                                                                 'real_stat' => 100,
                                                                 'estimated_size' => 100,
                                                                 'name' => 'dumped',
                                                                 'estimated_stat' => 100
                                                               },
                                                   'wait_for_writing' => {
                                                                           'name' => 'wait for writing'
                                                                         },
                                                   'failed_to_tape' => {
                                                                         'name' => 'failed to tape'
                                                                       },
                                                   'flush' => {
                                                                'name' => 'flush'
                                                              },
                                                   'wait_for_dumping' => {
                                                                           'real_size' => undef,
                                                                           'nb' => 0,
                                                                           'estimated_size' => 0,
                                                                           'name' => 'wait for dumping',
                                                                           'estimated_stat' => 0
                                                                         },
                                                   'estimated' => {
                                                                    'real_size' => undef,
                                                                    'nb' => 1,
                                                                    'estimated_size' => 100,
                                                                    'name' => 'estimated'
                                                                  }
                                                 },
                                       'dles' => {
                                                   'clienthost' => {
                                                                     '/some/dir' => {
                                                                                      '20080618130147' => {
                                                                                                            'degr_level' => -1,
                                                                                                            'status' => 20,
                                                                                                            'taped' => 1,
                                                                                                            'partial' => 0,
                                                                                                            'size' => '100',
                                                                                                            'message' => 'dump done',
                                                                                                            'esize' => '100',
                                                                                                            'level' => '0',
                                                                                                            'flush' => 0,
                                                                                                            'holding_file' => '/holding/20080618130147/clienthost._some_dir.0',
                                                                                                            'error' => '',
                                                                                                            'chunk_time' => '6.408',
                                                                                                            'dsize' => '100',
                                                                                                            'storage' => {
                                                                                                                           'TESTCONF' => {
                                                                                                                                           'taped_size' => 0,
                                                                                                                                           'taper_time' => '6.415',
                                                                                                                                           'status' => 22,
                                                                                                                                           'error' => '',
                                                                                                                                           'dsize' => '100',
                                                                                                                                           'partial' => 0,
                                                                                                                                           'size' => '100',
                                                                                                                                           'message' => 'written'
                                                                                                                                         }
                                                                                                                         },
                                                                                                            'dump_time' => '6.408'
                                                                                                          }
                                                                                    }
                                                                   },
                                                 },
                                       'filename' => $amdump_log,
                                       'busy_dumper' => {
                                                          '1' => {
                                                                   'time' => 0,
                                                                   'percent' => 0
                                                                 },
                                                          '0' => {
                                                                   'time' => '5.103',
                                                                   'status' => {
                                                                                 'not-idle' => {
                                                                                                 'time' => '5.096',
                                                                                                 'percent' => '99.8628257887517'
                                                                                               }
                                                                               },
                                                                   'percent' => '80.2484667400535'
                                                                 }
                                                        },
                                       'status_driver' => 'no-dumpers',
                                       'storage' => {
                                                      'TESTCONF' => {
                                                                      'taper' => 'taper0'
                                                                    }
                                                    },
                                       'busy' => {
                                                   'taper0' => {
                                                                 'time' => '0.00499999999999989',
                                                                 'percent' => '0.0786287152067918',
                                                                 'type' => 'taper',
                                                                 'storage' => 'TESTCONF'
                                                               },
                                                   'chunker0' => {
                                                                   'time' => '5.094',
                                                                   'percent' => '80.1069350526813',
                                                                   'type' => 'chunker'
                                                                 },
                                                   'dumper0' => {
                                                                  'time' => '5.078',
                                                                  'percent' => '79.8553231640195',
                                                                  'type' => 'dumper'
                                                                }
                                                 },
                                       'idle_dumpers' => '4'
                                     },
                         'severity' => '16',
                         'message' => 'The status',
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1800000
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 1371742094,
                                                                         'full' => 1371742094
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 9,
                                                                           'full' => 9
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 9,
                                                                          'full' => 9
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '5.306',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '608307843.014652',
                                                                                    'full' => '608307843.014652'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '14.265',
                                                                         'full' => '14.265'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '4465306.52994792',
                                                                         'full' => '4465306.52994792'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '2.255013',
                                                                         'full' => '2.255013'
                                                                       },
                                                        'estimate_time' => '0.084',
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 1865445810,
                                                                             'full' => 1865445810
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 1371742094,
                                                                           'full' => 1371742094
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 9,
                                                                           'full' => 9
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '96161380.5818437',
                                                                             'full' => '96161380.5818437'
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'February 25, 2009',
                                                   'hostname' => 'localhost.localdomain'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot1',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '16',
                                                        'dump_comp' => 75,
                                                        'dump_out_kb' => '12'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot2',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '167',
                                                        'dump_comp' => '73.6526946107784',
                                                        'dump_out_kb' => '123'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot3',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '1678',
                                                        'dump_comp' => '73.5399284862932',
                                                        'dump_out_kb' => '1234'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot4',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '16789',
                                                        'dump_comp' => '73.5302876883674',
                                                        'dump_out_kb' => '12345'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot5',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '167890',
                                                        'dump_comp' => '73.5338614569063',
                                                        'dump_out_kb' => '123456'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot6',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '1678901',
                                                        'dump_comp' => '73.5342345975135',
                                                        'dump_out_kb' => '1234567'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot7',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '16789012',
                                                        'dump_comp' => '73.5342734879217',
                                                        'dump_out_kb' => '12345678'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot8',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '167890123',
                                                        'dump_comp' => '73.5342775345992',
                                                        'dump_out_kb' => '123456789'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.250557',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '156611.070535',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24748.4',
                                                        'disk_name' => '/boot9',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1.585',
                                                        'dump_orig_kb' => '1678901234',
                                                        'dump_comp' => '73.534277359403',
                                                        'dump_out_kb' => '1234567890'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'DIRO-TEST-003'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  taper: tape DIRO-TEST-003 kb 39240 fm 10 [OK]'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '4465306.42578125',
                                                              'nb' => 9,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DIRO-TEST-003',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '2.255013',
                                                              'nc' => 9,
                                                              'size' => 1371742094
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

# now test a file with spaces and other funny characters in filenames
$cat = Installcheck::Catalogs::load('quoted');
$cat->install();
$amdump_log = "$logdir/amdump";
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?amdump_log=$amdump_log");
$starttime = $reply->{body}[0]{status}{starttime};
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'source_filename' => "$amperldir/Amanda/Status.pm",
                         'status' => {
                                       'free_space' => '868352',
                                       'current_time' => '6.418',
                                       'taper' => {
                                                    'taper0' => {
                                                                  'tape_size' => '102400',
                                                                  'worker' => {
                                                                                'worker0-0' => {
                                                                                                 'status' => 0,
                                                                                                 'no_tape' => 0,
                                                                                                 'message' => 'Idle'
                                                                                               }
                                                                              },
                                                                  'nb_tape' => 1,
                                                                  'storage' => 'TESTCONF',
                                                                  'stat' => [
                                                                              {
                                                                                'esize' => 100,
                                                                                'percent' => '0.09765625',
                                                                                'nb_part' => 1,
                                                                                'nb_dle' => 1,
                                                                                'label' => 'Conf-001',
                                                                                'size' => 100
                                                                              }
                                                                            ]
                                                                }
                                                  },
                                       'dead_run' => 1,
                                       'starttime' => $starttime,
                                       'holding_space' => 868352,
                                       'free_kps' => '600',
                                       'qlen' => {
                                                   'tapeq' => {
                                                                'taper0' => 0
                                                              },
                                                   'roomq' => '0',
                                                   'runq' => '0'
                                                 },
                                       'datestamp' => '20080618130147',
                                       'exit_status' => 0,
                                       'stat' => {
                                                   'dumping' => {
                                                                  'real_size' => 0,
                                                                  'nb' => 0,
                                                                  'real_stat' => 0,
                                                                  'estimated_size' => 0,
                                                                  'name' => 'dumping',
                                                                  'estimated_stat' => 0
                                                                },
                                                   'dumping_to_tape' => {
                                                                          'real_size' => 0,
                                                                          'nb' => 0,
                                                                          'real_stat' => 0,
                                                                          'estimated_size' => 0,
                                                                          'name' => 'dumping to tape',
                                                                          'estimated_stat' => 0
                                                                        },
                                                   'dump_failed' => {
                                                                      'real_size' => undef,
                                                                      'nb' => 0,
                                                                      'estimated_size' => 0,
                                                                      'name' => 'dump failed',
                                                                      'estimated_stat' => 0
                                                                    },
                                                   'disk' => {
                                                               'real_size' => undef,
                                                               'nb' => 1,
                                                               'estimated_size' => undef,
                                                               'name' => 'disk'
                                                             },
                                                   'wait_to_flush' => {
                                                                        'name' => 'wait_to_flush'
                                                                      },
                                                   'writing_to_tape' => {
                                                                          'name' => 'writing to tape'
                                                                        },
                                                   'taped' => {
                                                                'estimated_size' => 100,
                                                                'name' => 'taped',
                                                                'storage' => {
                                                                               'TESTCONF' => {
                                                                                               'real_size' => 100,
                                                                                               'nb' => 1,
                                                                                               'real_stat' => 100,
                                                                                               'estimated_size' => 100,
                                                                                               'estimated_stat' => 100
                                                                                             }
                                                                             }
                                                              },
                                                   'dumped' => {
                                                                 'real_size' => 100,
                                                                 'nb' => 1,
                                                                 'real_stat' => 100,
                                                                 'estimated_size' => 100,
                                                                 'name' => 'dumped',
                                                                 'estimated_stat' => 100
                                                               },
                                                   'wait_for_writing' => {
                                                                           'name' => 'wait for writing'
                                                                         },
                                                   'failed_to_tape' => {
                                                                         'name' => 'failed to tape'
                                                                       },
                                                   'flush' => {
                                                                'name' => 'flush'
                                                              },
                                                   'wait_for_dumping' => {
                                                                           'real_size' => undef,
                                                                           'nb' => 0,
                                                                           'estimated_size' => 0,
                                                                           'name' => 'wait for dumping',
                                                                           'estimated_stat' => 0
                                                                         },
                                                   'estimated' => {
                                                                    'real_size' => undef,
                                                                    'nb' => 1,
                                                                    'estimated_size' => 100,
                                                                    'name' => 'estimated'
                                                                  }
                                                 },
                                       'dles' => {
                                                   'clienthost' => {
                                                                     'C:\\Some Dir\\' => {
                                                                                           '20080618130147' => {
                                                                                                                 'degr_level' => -1,
                                                                                                                 'status' => 20,
                                                                                                                 'taped' => 1,
                                                                                                                 'partial' => 0,
                                                                                                                 'size' => '100',
                                                                                                                 'message' => 'dump done',
                                                                                                                 'esize' => '100',
                                                                                                                 'level' => '0',
                                                                                                                 'flush' => 0,
                                                                                                                 'holding_file' => '/holding/20080618130147/clienthost._some_dir.0',
                                                                                                                 'error' => '',
                                                                                                                 'chunk_time' => '6.408',
                                                                                                                 'dsize' => '100',
                                                                                                                 'storage' => {
                                                                                                                                'TESTCONF' => {
                                                                                                                                                'taped_size' => 100,
                                                                                                                                                'taper_time' => '6.415',
                                                                                                                                                'status' => 22,
                                                                                                                                                'error' => '',
                                                                                                                                                'dsize' => '100',
                                                                                                                                                'partial' => 0,
                                                                                                                                                'size' => '100',
                                                                                                                                                'message' => 'written'
                                                                                                                                              }
                                                                                                                              },
                                                                                                                 'dump_time' => '6.408'
                                                                                                               }
                                                                                         }
                                                                   }
                                                 },
                                       'filename' => $amdump_log,
                                       'busy_dumper' => {
                                                          '1' => {
                                                                   'time' => 0,
                                                                   'percent' => 0
                                                                 },
                                                          '0' => {
                                                                   'time' => '5.103',
                                                                   'status' => {
                                                                                 'not-idle' => {
                                                                                                 'time' => '5.096',
                                                                                                 'percent' => '99.8628257887517'
                                                                                               }
                                                                               },
                                                                   'percent' => '80.2484667400535'
                                                                 }
                                                        },
                                       'status_driver' => 'no-dumpers',
                                       'storage' => {
                                                      'TESTCONF' => {
                                                                      'taper' => 'taper0'
                                                                    }
                                                    },
                                       'busy' => {
                                                   'taper0' => {
                                                                 'time' => '0.00499999999999989',
                                                                 'percent' => '0.0786287152067918',
                                                                 'type' => 'taper',
                                                                 'storage' => 'TESTCONF'
                                                               },
                                                   'chunker0' => {
                                                                   'time' => '5.094',
                                                                   'percent' => '80.1069350526813',
                                                                   'type' => 'chunker'
                                                                 },
                                                   'dumper0' => {
                                                                  'time' => '5.078',
                                                                  'percent' => '79.8553231640195',
                                                                  'type' => 'dumper'
                                                                }
                                                 },
                                       'idle_dumpers' => '4'
                                     },
                         'severity' => '16',
                         'message' => 'The status',
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1800000
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

# now test a chunker partial result
$cat = Installcheck::Catalogs::load('chunker-partial');
$cat->install();
$amdump_log = "$logdir/amdump";
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?amdump_log=$amdump_log");
$starttime = $reply->{body}[0]{status}{starttime};
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'source_filename' => "$amperldir/Amanda/Status.pm",
                         'status' => {
                                       'free_space' => '1215488',
                                       'current_time' => '114.944',
                                       'taper' => {
                                                    'taper0' => {
                                                                  'tape_size' => '122880',
                                                                  'worker' => {
                                                                                'worker0-0' => {
                                                                                                 'status' => 0,
                                                                                                 'no_tape' => 0,
                                                                                                 'message' => 'Idle'
                                                                                               }
                                                                              },
                                                                  'nb_tape' => 1,
                                                                  'storage' => 'TESTCONF',
                                                                  'stat' => [
                                                                              {
                                                                                'esize' => 80917,
                                                                                'percent' => '65.8504231770833',
                                                                                'nb_part' => 1,
                                                                                'nb_dle' => 1,
                                                                                'label' => 'maitreyee-010',
                                                                                'size' => 80917
                                                                              }
                                                                            ]
                                                                }
                                                  },
                                       'dead_run' => 1,
                                       'starttime' => $starttime,
                                       'holding_space' => 1215488,
                                       'free_kps' => '8000',
                                       'qlen' => {
                                                   'tapeq' => {
                                                                'taper0' => 0
                                                              },
                                                   'roomq' => '0',
                                                   'runq' => '0'
                                                 },
                                       'datestamp' => '20090410074759',
                                       'exit_status' => 4,
                                       'stat' => {
                                                   'dumping' => {
                                                                  'real_size' => 0,
                                                                  'nb' => 0,
                                                                  'real_stat' => 0,
                                                                  'estimated_size' => 0,
                                                                  'name' => 'dumping',
                                                                  'estimated_stat' => 0
                                                                },
                                                   'dumping_to_tape' => {
                                                                          'real_size' => 0,
                                                                          'nb' => 0,
                                                                          'real_stat' => 0,
                                                                          'estimated_size' => 0,
                                                                          'name' => 'dumping to tape',
                                                                          'estimated_stat' => 0
                                                                        },
                                                   'dump_failed' => {
                                                                      'real_size' => undef,
                                                                      'nb' => 1,
                                                                      'estimated_size' => 80822,
                                                                      'name' => 'dump failed',
                                                                      'estimated_stat' => 0
                                                                    },
                                                   'disk' => {
                                                               'real_size' => undef,
                                                               'nb' => 1,
                                                               'estimated_size' => undef,
                                                               'name' => 'disk'
                                                             },
                                                   'wait_to_flush' => {
                                                                        'name' => 'wait_to_flush'
                                                                      },
                                                   'writing_to_tape' => {
                                                                          'name' => 'writing to tape'
                                                                        },
                                                   'taped' => {
                                                                'estimated_size' => 80822,
                                                                'name' => 'taped',
                                                                'storage' => {
                                                                               'TESTCONF' => {
                                                                                               'real_size' => 80917,
                                                                                               'nb' => 1,
                                                                                               'real_stat' => '100.117542253347',
                                                                                               'estimated_size' => 80822,
                                                                                               'estimated_stat' => '100.117542253347'
                                                                                             }
                                                                             }
                                                              },
                                                   'dumped' => {
                                                                 'real_size' => 0,
                                                                 'nb' => 0,
                                                                 'real_stat' => 0,
                                                                 'estimated_size' => 0,
                                                                 'name' => 'dumped',
                                                                 'estimated_stat' => 0
                                                               },
                                                   'wait_for_writing' => {
                                                                           'name' => 'wait for writing'
                                                                         },
                                                   'failed_to_tape' => {
                                                                         'name' => 'failed to tape'
                                                                       },
                                                   'flush' => {
                                                                'name' => 'flush'
                                                              },
                                                   'wait_for_dumping' => {
                                                                           'real_size' => undef,
                                                                           'nb' => 0,
                                                                           'estimated_size' => 0,
                                                                           'name' => 'wait for dumping',
                                                                           'estimated_stat' => 0
                                                                         },
                                                   'estimated' => {
                                                                    'real_size' => undef,
                                                                    'nb' => 1,
                                                                    'estimated_size' => 80822,
                                                                    'name' => 'estimated'
                                                                  }
                                                 },
                                       'dles' => {
                                                   'localhost' => {
                                                                    '/etc' => {
                                                                                '20090410074759' => {
                                                                                                      'degr_level' => -1,
                                                                                                      'status' => 12,
                                                                                                      'taped' => 1,
                                                                                                      'partial' => 1,
                                                                                                      'size' => '80917',
                                                                                                      'message' => 'dump failed: dumper: [/usr/sbin/tar returned error]',
                                                                                                      'esize' => '80822',
                                                                                                      'level' => '0',
                                                                                                      'flush' => 0,
                                                                                                      'holding_file' => '/var/lib/amanda/staging/20090410074759/localhost._etc.0',
                                                                                                      'error' => 'dumper: [/usr/sbin/tar returned error]',
                                                                                                      'chunk_time' => '84.745',
                                                                                                      'dsize' => '80917',
                                                                                                      'storage' => {
                                                                                                                     'TESTCONF' => {
                                                                                                                                     'taped_size' => 80917,
                                                                                                                                     'taper_time' => '114.911',
                                                                                                                                     'status' => 22,
                                                                                                                                     'error' => '',
                                                                                                                                     'dsize' => '80917',
                                                                                                                                     'partial' => 0,
                                                                                                                                     'size' => '80917',
                                                                                                                                     'message' => 'written'
                                                                                                                                   }
                                                                                                                   },
                                                                                                      'dump_time' => '84.740'
                                                                                                    }
                                                                              }
                                                                  }
                                                 },
                                       'filename' => $amdump_log,
                                       'busy_dumper' => {
                                                          '1' => {
                                                                   'time' => '81.442',
                                                                   'status' => {
                                                                                 'no-dumpers' => {
                                                                                                   'time' => '81.442',
                                                                                                   'percent' => 100
                                                                                                 }
                                                                               },
                                                                   'percent' => '70.8647303482241'
                                                                 },
                                                          '0' => {
                                                                   'time' => '30.564',
                                                                   'status' => {
                                                                                 'no-dumpers' => {
                                                                                                   'time' => '30.467',
                                                                                                   'percent' => '99.6826331631985'
                                                                                                 }
                                                                               },
                                                                   'percent' => '26.5945042897169'
                                                                 }
                                                        },
                                       'status_driver' => 'no-dumpers',
                                       'storage' => {
                                                      'TESTCONF' => {
                                                                      'taper' => 'taper0'
                                                                    }
                                                    },
                                       'busy' => {
                                                   'taper0' => {
                                                                 'time' => '30.021',
                                                                 'percent' => '26.1220263473888',
                                                                 'type' => 'taper',
                                                                 'storage' => 'TESTCONF'
                                                               },
                                                   'chunker0' => {
                                                                   'time' => '81.603',
                                                                   'percent' => '71.0048204931869',
                                                                   'type' => 'chunker'
                                                                 },
                                                   'dumper0' => {
                                                                  'time' => '81.5',
                                                                  'percent' => '70.9151976054157',
                                                                  'type' => 'dumper'
                                                                }
                                                 },
                                       'idle_dumpers' => '4'
                                     },
                         'severity' => '16',
                         'message' => 'The status',
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1800000
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

# now test a taper-parallel-write > 1
$cat = Installcheck::Catalogs::load('taper-parallel-write');
$cat->install();
$amdump_log = "$logdir/amdump";
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/status?amdump_log=$amdump_log");
$starttime = $reply->{body}[0]{status}{starttime};
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'source_filename' => "$amperldir/Amanda/Status.pm",
                         'status' => {
                                       'free_space' => '0',
                                       'current_time' => '82.698',
                                       'taper' => {
                                                    'taper0' => {
                                                                  'tape_size' => '2445312',
                                                                  'worker' => {
                                                                                'worker0-1' => {
                                                                                                 'status' => 0,
                                                                                                 'no_tape' => 2,
                                                                                                 'message' => 'Idle'
                                                                                               },
                                                                                'worker0-0' => {
                                                                                                 'status' => 0,
                                                                                                 'no_tape' => 0,
                                                                                                 'message' => 'Idle'
                                                                                               }
                                                                              },
                                                                  'nb_tape' => 3,
                                                                  'storage' => 'TESTCONF',
                                                                  'stat' => [
                                                                              {
                                                                                'esize' => 715072,
                                                                                'percent' => '29.242567001675',
                                                                                'nb_part' => 1,
                                                                                'nb_dle' => 1,
                                                                                'label' => 'amstatus_test_3-AA-001',
                                                                                'size' => 715072
                                                                              },
                                                                              {
                                                                                'esize' => 35584,
                                                                                'percent' => '1.45519262981575',
                                                                                'nb_part' => 1,
                                                                                'nb_dle' => 1,
                                                                                'label' => 'amstatus_test_3-AA-002',
                                                                                'size' => 35584
                                                                              },
                                                                              {
                                                                                'esize' => 142336,
                                                                                'percent' => '5.82077051926298',
                                                                                'nb_part' => 1,
                                                                                'nb_dle' => 1,
                                                                                'label' => 'amstatus_test_3-AA-003',
                                                                                'size' => 142336
                                                                              }
                                                                            ]
                                                                }
                                                  },
                                       'dead_run' => 1,
                                       'starttime' => $starttime,
                                       'holding_space' => undef,
                                       'free_kps' => '2000000',
                                       'qlen' => {
                                                   'tapeq' => {
                                                                'taper0' => 0
                                                              },
                                                   'roomq' => '0',
                                                   'runq' => '0'
                                                 },
                                       'datestamp' => '20120919143530',
                                       'exit_status' => 0,
                                       'stat' => {
                                                   'dumping' => {
                                                                  'real_size' => 0,
                                                                  'nb' => 0,
                                                                  'real_stat' => 0,
                                                                  'estimated_size' => 0,
                                                                  'name' => 'dumping',
                                                                  'estimated_stat' => 0
                                                                },
                                                   'dumping_to_tape' => {
                                                                          'real_size' => 0,
                                                                          'nb' => 0,
                                                                          'real_stat' => 0,
                                                                          'estimated_size' => 0,
                                                                          'name' => 'dumping to tape',
                                                                          'estimated_stat' => 0
                                                                        },
                                                   'dump_failed' => {
                                                                      'real_size' => undef,
                                                                      'nb' => 0,
                                                                      'estimated_size' => 0,
                                                                      'name' => 'dump failed',
                                                                      'estimated_stat' => 0
                                                                    },
                                                   'disk' => {
                                                               'real_size' => undef,
                                                               'nb' => 3,
                                                               'estimated_size' => undef,
                                                               'name' => 'disk'
                                                             },
                                                   'wait_to_flush' => {
                                                                        'name' => 'wait_to_flush'
                                                                      },
                                                   'writing_to_tape' => {
                                                                          'name' => 'writing to tape'
                                                                        },
                                                   'taped' => {
                                                                'estimated_size' => 892929,
                                                                'name' => 'taped',
                                                                'storage' => {
                                                                               'TESTCONF' => {
                                                                                               'real_size' => 892992,
                                                                                               'nb' => 3,
                                                                                               'real_stat' => '100.007055432179',
                                                                                               'estimated_size' => 892929,
                                                                                               'estimated_stat' => '100.007055432179'
                                                                                             }
                                                                             }
                                                              },
                                                   'dumped' => {
                                                                 'real_size' => 892992,
                                                                 'nb' => 3,
                                                                 'real_stat' => '100.007055432179',
                                                                 'estimated_size' => 892929,
                                                                 'name' => 'dumped',
                                                                 'estimated_stat' => '100.007055432179'
                                                               },
                                                   'wait_for_writing' => {
                                                                           'name' => 'wait for writing'
                                                                         },
                                                   'failed_to_tape' => {
                                                                         'name' => 'failed to tape'
                                                                       },
                                                   'flush' => {
                                                                'name' => 'flush'
                                                              },
                                                   'wait_for_dumping' => {
                                                                           'real_size' => undef,
                                                                           'nb' => 0,
                                                                           'estimated_size' => 0,
                                                                           'name' => 'wait for dumping',
                                                                           'estimated_stat' => 0
                                                                         },
                                                   'estimated' => {
                                                                    'real_size' => undef,
                                                                    'nb' => 3,
                                                                    'estimated_size' => 892929,
                                                                    'name' => 'estimated'
                                                                  }
                                                 },
                                       'dles' => {
                                                   'qa-debian6-x64-anuj' => {
                                                                              '/root/testfile/146mb' => {
                                                                                                          '20120919143530' => {
                                                                                                                                'degr_level' => -1,
                                                                                                                                'status' => 21,
                                                                                                                                'taped' => 1,
                                                                                                                                'size' => '142336',
                                                                                                                                'message' => 'dump to tape done',
                                                                                                                                'esize' => '142298',
                                                                                                                                'level' => '0',
                                                                                                                                'flush' => 0,
                                                                                                                                'error' => '',
                                                                                                                                'dsize' => '142336',
                                                                                                                                'dump_to_tape_storage' => 'TESTCONF',
                                                                                                                                'storage' => {
                                                                                                                                               'TESTCONF' => {
                                                                                                                                                               'taped_size' => 142336,
                                                                                                                                                               'taper_time' => '82.694',
                                                                                                                                                               'status' => 21,
                                                                                                                                                               'error' => '',
                                                                                                                                                               'dsize' => '142336',
                                                                                                                                                               'partial' => 0,
                                                                                                                                                               'size' => 142336,
                                                                                                                                                               'message' => 'dump to tape done'
                                                                                                                                                             }
                                                                                                                                             },
                                                                                                                                'dump_time' => '82.692'
                                                                                                                              }
                                                                                                        }
                                                                            },
                                                   'centos6-43-client' => {
                                                                            '/root/testfile/36mb' => {
                                                                                                       '20120919143530' => {
                                                                                                                             'degr_level' => -1,
                                                                                                                             'status' => 21,
                                                                                                                             'taped' => 1,
                                                                                                                             'size' => '35584',
                                                                                                                             'message' => 'dump to tape done',
                                                                                                                             'esize' => '35576',
                                                                                                                             'level' => '0',
                                                                                                                             'flush' => 0,
                                                                                                                             'error' => '',
                                                                                                                             'dsize' => '35584',
                                                                                                                             'dump_to_tape_storage' => 'TESTCONF',
                                                                                                                             'storage' => {
                                                                                                                                            'TESTCONF' => {
                                                                                                                                                            'taped_size' => 35584,
                                                                                                                                                            'taper_time' => '6.243',
                                                                                                                                                            'status' => 21,
                                                                                                                                                            'error' => '',
                                                                                                                                                            'dsize' => '35584',
                                                                                                                                                            'partial' => 0,
                                                                                                                                                            'size' => 35584,
                                                                                                                                                            'message' => 'dump to tape done'
                                                                                                                                                          }
                                                                                                                                          },
                                                                                                                             'dump_time' => '6.226'
                                                                                                                           }
                                                                                                     }
                                                                          },
                                                   'localhost' => {
                                                                    '/root/testfile/732mb' => {
                                                                                                '20120919143530' => {
                                                                                                                      'degr_level' => -1,
                                                                                                                      'status' => 21,
                                                                                                                      'taped' => 1,
                                                                                                                      'size' => '715072',
                                                                                                                      'message' => 'dump to tape done',
                                                                                                                      'esize' => '715055',
                                                                                                                      'level' => '0',
                                                                                                                      'flush' => 0,
                                                                                                                      'error' => '',
                                                                                                                      'dsize' => '715072',
                                                                                                                      'dump_to_tape_storage' => 'TESTCONF',
                                                                                                                      'storage' => {
                                                                                                                                     'TESTCONF' => {
                                                                                                                                                     'taped_size' => 715072,
                                                                                                                                                     'taper_time' => '42.944',
                                                                                                                                                     'status' => 21,
                                                                                                                                                     'error' => '',
                                                                                                                                                     'dsize' => '715072',
                                                                                                                                                     'partial' => 0,
                                                                                                                                                     'size' => 715072,
                                                                                                                                                     'message' => 'dump to tape done'
                                                                                                                                                   }
                                                                                                                                   },
                                                                                                                      'dump_time' => '42.900'
                                                                                                                    }
                                                                                              }
                                                                  }
                                                 },
                                       'filename' => $amdump_log,
                                       'busy_dumper' => {
                                                          '1' => {
                                                                   'time' => '44.543',
                                                                   'status' => {
                                                                                 'no-dumpers' => {
                                                                                                   'time' => '44.543',
                                                                                                   'percent' => 100
                                                                                                 }
                                                                               },
                                                                   'percent' => '53.894178997931'
                                                                 },
                                                          '0' => {
                                                                   'time' => '2.52899999999999',
                                                                   'status' => {
                                                                                 'not-idle' => {
                                                                                                 'time' => '2.35',
                                                                                                 'percent' => '92.9221035982605'
                                                                                               }
                                                                               },
                                                                   'percent' => '3.05992812980192'
                                                                 },
                                                          '2' => {
                                                                   'time' => '35.574',
                                                                   'status' => {
                                                                                 'no-dumpers' => {
                                                                                                   'time' => '35.574',
                                                                                                   'percent' => 100
                                                                                                 }
                                                                               },
                                                                   'percent' => '43.0422630642839'
                                                                 }
                                                        },
                                       'status_driver' => 'no-dumpers',
                                       'storage' => {
                                                      'TESTCONF' => {
                                                                      'taper' => 'taper0'
                                                                    }
                                                    },
                                       'busy' => {
                                                   'taper0' => {
                                                                 'time' => '120.6',
                                                                 'percent' => '145.918280922939',
                                                                 'type' => 'taper',
                                                                 'storage' => 'TESTCONF'
                                                               },
                                                   'dumper1' => {
                                                                  'time' => '40.487',
                                                                  'percent' => '48.9866786047018',
                                                                  'type' => 'dumper'
                                                                },
                                                   'dumper0' => {
                                                                  'time' => '80.009',
                                                                  'percent' => '96.8057689748212',
                                                                  'type' => 'dumper'
                                                                }
                                                 },
                                       'idle_dumpers' => '2'
                                     },
                         'severity' => '16',
                         'message' => 'The status',
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1800000
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));


$cat = Installcheck::Catalogs::load('doublefailure');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 54929,
                                                                         'full' => 54929
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '77506.015',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '15450.8260702627',
                                                                                    'full' => '15450.8260702627'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '178.831380208333',
                                                                         'full' => '178.831380208333'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '3.555085',
                                                                         'full' => '3.555085'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 109860,
                                                                           'full' => 109860
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'failure_summary' => [
                                                              '  ns-new.slikon.local /opt/var lev 0  FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]',
                                                              '  ns-new.slikon.local /opt/var lev 0  FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]',
                                                              '  ns-new.slikon.local /opt/var lev 0  partial taper: successfully taped a partial dump'
                                                            ],
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-PARTIAL',
                                                        'tape_duration' => '3.555085',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'ns-new.slikon.local',
                                                        'tape_rate' => '15451.027696',
                                                        'dump_partial' => ' PARTIAL',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/opt/var',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '54929'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-13'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  planner: Forcing full dump of ns-new.slikon.local:/opt/var as directed.'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '178.831380208333',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-13',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '3.555085',
                                                              'nc' => 1,
                                                              'size' => 54929
                                                            }
                                                          ],
                                       'failure_details' => [
                                                              '  /-- ns-new.slikon.local /opt/var lev 0 FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]',
                                                              '  sendbackup: info BACKUP=APPLICATION',
                                                              '  sendbackup: info APPLICATION=amgtar',
                                                              '  sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -',
                                                              '  sendbackup: info COMPRESS_SUFFIX=.gz',
                                                              '  sendbackup: info end',
                                                              '  ? /bin/tar: ./gdm: Cannot savedir: Permission denied',
                                                              '  | Total bytes written: 943831040 (901MiB, 4.9MiB/s)',
                                                              '  | /bin/tar: Error exit delayed from previous errors',
                                                              '  sendbackup: error [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134405000.debug]',
                                                              '  sendbackup: size 921710',
                                                              '  sendbackup: end',
                                                              '  \\--------',
                                                              '  /-- ns-new.slikon.local /opt/var lev 0 FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]',
                                                              '  sendbackup: info BACKUP=APPLICATION',
                                                              '  sendbackup: info APPLICATION=amgtar',
                                                              '  sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -',
                                                              '  sendbackup: info COMPRESS_SUFFIX=.gz',
                                                              '  sendbackup: info end',
                                                              '  ? /bin/tar: ./gdm: Cannot savedir: Permission denied',
                                                              '  | Total bytes written: 943851520 (901MiB, 7.4MiB/s)',
                                                              '  | /bin/tar: Error exit delayed from previous errors',
                                                              '  sendbackup: error [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326134714000.debug]',
                                                              '  sendbackup: size 921730',
                                                              '  sendbackup: end',
                                                              '  \\--------'
                                                            ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('strontium');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 1630,
                                                                         'total' => 1650,
                                                                         'full' => 20
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 3,
                                                                           'total' => 4,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 3,
                                                                          'total' => 4,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '1:3',
                                                        'run_time' => '49.037',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => '151811.492968241',
                                                                                    'total' => '143966.495070238',
                                                                                    'full' => '27624.3093922652'
                                                                                  },
                                                        'tapeparts' => '1:3',
                                                        'dump_time' => {
                                                                         'incr' => '0.912',
                                                                         'total' => '1.061',
                                                                         'full' => '0.149'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => '5.38411458333333',
                                                                         'total' => '5.47526041666667',
                                                                         'full' => '0.0911458333333333'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => '0.010737',
                                                                         'total' => '0.011461',
                                                                         'full' => '0.000724'
                                                                       },
                                                        'estimate_time' => '2.344',
                                                        'original_size' => {
                                                                             'incr' => 1630,
                                                                             'total' => 1650,
                                                                             'full' => 20
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 1630,
                                                                           'total' => 1650,
                                                                           'full' => 20
                                                                         },
                                                        'tapedisks' => '1:3',
                                                        'dles_dumped' => {
                                                                           'incr' => 3,
                                                                           'total' => 4,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => 100,
                                                                               'total' => 100,
                                                                               'full' => 100
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => '1787.28070175439',
                                                                             'total' => '1555.13666352498',
                                                                             'full' => '134.228187919463'
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'January 7, 2010',
                                                   'hostname' => 'advantium'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.001916',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'strontium',
                                                        'tape_rate' => '140918.580376',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '1146.3',
                                                        'disk_name' => '/etc',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '0.235',
                                                        'dump_orig_kb' => '270',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '270'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.001107',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'strontium',
                                                        'tape_rate' => '9033.423668',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '65.6',
                                                        'disk_name' => '/home/elantra',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '0.152',
                                                        'dump_orig_kb' => '10',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '10'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.000724',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'strontium',
                                                        'tape_rate' => '27624.309392',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '133.9',
                                                        'disk_name' => '/local',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '0.149',
                                                        'dump_orig_kb' => '20',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '20'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.007714',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'strontium',
                                                        'tape_rate' => '175006.481722',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '2568.5',
                                                        'disk_name' => '/zones/data/strontium.example.com/repositories/repository_13',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '0.525',
                                                        'dump_orig_kb' => '1350',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '1350'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'metals-013'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  taper: tape metals-013 kb 1650 fm 4 [OK]'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '5.43619791666667',
                                                              'nb' => 4,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'metals-013',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.011461',
                                                              'nc' => 4,
                                                              'size' => 1650
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('amflush');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 37289,
                                                                         'total' => 37289,
                                                                         'full' => 0
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 2,
                                                                           'total' => 2,
                                                                           'full' => 0
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 2,
                                                                          'total' => 2,
                                                                          'full' => 0
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '177.708',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => '8920.79845799301',
                                                                                    'total' => '8920.79845799301',
                                                                                    'full' => undef
                                                                                  },
                                                        'tapeparts' => '1:2',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => '121.435546875',
                                                                         'total' => '121.435546875',
                                                                         'full' => 0
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => '4.180007',
                                                                         'total' => '4.180007',
                                                                         'full' => 0
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '1:2',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'June 22, 2009',
                                                   'hostname' => 'centralcity.zmanda.com'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-NOT FLUSHED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/home',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-NOT FLUSHED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/opt',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '1.675693',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '184.632684',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/usr/lib',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '309'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-NOT FLUSHED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/usr/local',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '2.504314',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '14766.518895',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/var/mysql',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '36980'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Flushy-017'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '121.422526041667',
                                                              'nb' => 2,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Flushy-017',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '4.180007',
                                                              'nc' => 2,
                                                              'size' => 37289
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('resultsmissing');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 17246,
                                                                         'full' => 17246
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 2,
                                                                           'full' => 2
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 2,
                                                                          'full' => 2
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '77506.015',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '23070.7385820178',
                                                                                    'full' => '23070.7385820178'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '4.295',
                                                                         'full' => '4.295'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '56.19140625',
                                                                         'full' => '56.19140625'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '0.747527',
                                                                         'full' => '0.747527'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 20700,
                                                                             'full' => 20700
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 17247,
                                                                           'full' => 17247
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 2,
                                                                           'full' => 2
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '4015.59953434226',
                                                                             'full' => '4015.59953434226'
                                                                           }
                                                      },
                                       'failure_summary' => [
                                                              '  cnc.slikon.local / RESULTS MISSING',
                                                              '  ns-new.slikon.local /home RESULTS MISSING',
                                                              '  ns-new.slikon.local /boot lev 0  FAILED [planner failed]'
                                                            ],
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'missing',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'cnc.slikon.local',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.742831',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'cnc.slikon.local',
                                                        'tape_rate' => '23216.462699',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '4052.7',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '4.255',
                                                        'dump_orig_kb' => '20670',
                                                        'dump_comp' => '83.430091920658',
                                                        'dump_out_kb' => '17245'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.004696',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'ns-new.slikon.local',
                                                        'tape_rate' => '153.471705',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '24.6',
                                                        'disk_name' => '//usr/local',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '0.040',
                                                        'dump_orig_kb' => '30',
                                                        'dump_comp' => '3.33333333333333',
                                                        'dump_out_kb' => '1'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'ns-new.slikon.local',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'missing',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'ns-new.slikon.local',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/home',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-36'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '56.1783854166667',
                                                              'nb' => 2,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-36',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.747527',
                                                              'nc' => 2,
                                                              'size' => 17246
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('shortstrange');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 2293470,
                                                                         'full' => 2293470
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '77506.015',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '14596.58566789',
                                                                                    'full' => '14596.58566789'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '1775.514',
                                                                         'full' => '1775.514'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '7465.74869791667',
                                                                         'full' => '7465.74869791667'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '157.123731',
                                                                         'full' => '157.123731'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 5401240,
                                                                             'full' => 5401240
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 2293471,
                                                                           'full' => 2293471
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '1291.72228436385',
                                                                             'full' => '1291.72228436385'
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '157.123731',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'bsdfw.slikon.local',
                                                        'tape_rate' => '14596.586283',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '1305.4',
                                                        'disk_name' => '/',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1775.514',
                                                        'dump_orig_kb' => '5401240',
                                                        'dump_comp' => '42.4619161525872',
                                                        'dump_out_kb' => '2293470'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-36'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'strange_summary' => [
                                                              '  bsdfw.slikon.local / lev 0  STRANGE (see below)'
                                                            ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '7465.74869791667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-36',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '157.123731',
                                                              'nc' => 1,
                                                              'size' => 2293470
                                                            }
                                                          ],
                                       'strange_details' => [
                                                              '  /-- bsdfw.slikon.local / lev 0 STRANGE',
                                                              '  sendbackup: info BACKUP=APPLICATION',
                                                              '  sendbackup: info APPLICATION=amgtar',
                                                              '  sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -',
                                                              '  sendbackup: info COMPRESS_SUFFIX=.gz',
                                                              '  sendbackup: info end',
                                                              '  | /bin/tar: ./tmp/.gdm_socket: socket ignored',
                                                              '  | /bin/tar: ./tmp/.X11-unix/X0: socket ignored',
                                                              '  | /bin/tar: ./tmp/.font-unix/fs7100: socket ignored',
                                                              '  ? /bin/tar: ./var/log/messages: file changed as we read it',
                                                              '  | /bin/tar: ./var/run/acpid.socket: socket ignored',
                                                              '  | /bin/tar: ./var/run/dbus/system_bus_socket: socket ignored',
                                                              '  | Total bytes written: 5530869760 (5.2GiB, 3.0MiB/s)',
                                                              '  sendbackup: size 5401240',
                                                              '  sendbackup: end',
                                                              '  \\--------'
                                                            ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('longstrange');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 2293470,
                                                                         'full' => 2293470
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '77506.015',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '14596.58566789',
                                                                                    'full' => '14596.58566789'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '1775.514',
                                                                         'full' => '1775.514'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '7465.74869791667',
                                                                         'full' => '7465.74869791667'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '157.123731',
                                                                         'full' => '157.123731'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 5401240,
                                                                             'full' => 5401240
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 2293471,
                                                                           'full' => 2293471
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '1291.72228436385',
                                                                             'full' => '1291.72228436385'
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '157.123731',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'bsdfw.slikon.local',
                                                        'tape_rate' => '14596.586283',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '1305.4',
                                                        'disk_name' => '/',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '1775.514',
                                                        'dump_orig_kb' => '5401240',
                                                        'dump_comp' => '42.4619161525872',
                                                        'dump_out_kb' => '2293470'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-36'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'strange_summary' => [
                                                              '  bsdfw.slikon.local / lev 0  STRANGE (see below)'
                                                            ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '7465.74869791667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-36',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '157.123731',
                                                              'nc' => 1,
                                                              'size' => 2293470
                                                            }
                                                          ],
                                       'strange_details' => [
                                                              '  /-- bsdfw.slikon.local / lev 0 STRANGE',
                                                              '  sendbackup: info BACKUP=APPLICATION',
                                                              '  sendbackup: info APPLICATION=amgtar',
                                                              '  sendbackup: info RECOVER_CMD=/usr/bin/gzip -dc |amgtar -f... -',
                                                              '  sendbackup: info COMPRESS_SUFFIX=.gz',
                                                              '  sendbackup: info end',
                                                              '  | /bin/tar: ./tmp/.gdm_socket: socket ignored',
                                                              '  | /bin/tar: ./tmp/.X11-unix/X0: socket ignored',
                                                              '  | /bin/tar: ./tmp/.font-unix/fs7100: socket ignored',
                                                              '  ? /bin/tar: ./var/log/messages: file changed as we read it',
                                                              '  | /bin/tar: ./var/run/acpid.socket: socket ignored',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  | x',
                                                              '  \\--------',
                                                              '  913 lines follow, see the corresponding log.* file for the complete list',
                                                              '  \\--------'
                                                            ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('bigestimate');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 50917369,
                                                                         'full' => 50917369
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 3,
                                                                           'full' => 3
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '77506.015',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '27287.776782761',
                                                                                    'full' => '27287.776782761'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '2816.52',
                                                                         'full' => '2816.52'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '165746.695963542',
                                                                         'full' => '165746.695963542'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '1865.940542',
                                                                         'full' => '1865.940542'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 72987320,
                                                                             'full' => 72987320
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 50917370,
                                                                           'full' => 50917370
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '18078.1141266527',
                                                                             'full' => '18078.1141266527'
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '1865.940542',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'home.slikon.local',
                                                        'tape_rate' => '27287.777030',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '18078.1',
                                                        'disk_name' => '/opt/public',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '2816.520',
                                                        'dump_orig_kb' => '72987320',
                                                        'dump_comp' => '69.7619381010291',
                                                        'dump_out_kb' => '50917369'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-36'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  big estimate: home.slikon.local /opt/public 0',
                                                    '                  est: 80286112k    out 50917370k'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '165746.695963542',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-36',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '1865.940542',
                                                              'nc' => 3,
                                                              'size' => 50917369
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('retried');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 268356,
                                                                         'full' => 268356
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '77506.015',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '17213.5582974616',
                                                                                    'full' => '17213.5582974616'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '53.356',
                                                                         'full' => '53.356'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '873.580729166667',
                                                                         'full' => '873.580729166667'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '15.589804',
                                                                         'full' => '15.589804'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 2985670,
                                                                             'full' => 2985670
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 536715,
                                                                           'full' => 536715
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '10059.1311192743',
                                                                             'full' => '10059.1311192743'
                                                                           }
                                                      },
                                       'failure_summary' => [
                                                              '  jamon.slikon.local /var lev 0  FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326133640000.debug]',
                                                              '  jamon.slikon.local /var lev 0  was successfully retried'
                                                            ],
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '15.589804',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'jamon.slikon.local',
                                                        'tape_rate' => '17213.595632',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '5029.5',
                                                        'disk_name' => '/var',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '53.356',
                                                        'dump_orig_kb' => '2985670',
                                                        'dump_comp' => '8.98813331680996',
                                                        'dump_out_kb' => '268356'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-36'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  planner: disk jamon.slikon.local:/var, estimate of level 1 failed.'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '873.580729166667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-36',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '15.589804',
                                                              'nc' => 1,
                                                              'size' => 268356
                                                            }
                                                          ],
                                       'failure_details' => [
                                                              '  /-- jamon.slikon.local /var lev 0 FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326133640000.debug]',
                                                              '  blah blah blah',
                                                              '  \\--------'
                                                            ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('retried-strange');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'August 24, 2010',
                                                   'hostname' => 'molybdenum.zmanda.com'
                                                 },
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'metals-003-000105'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'failure_summary' => [
                                                              '  lead.zmanda.com /tensile-measurements lev 0  FAILED [data read: recv error: Connection timed out]',
                                                              '  lead.zmanda.com /tensile-measurements lev 0  was successfully retried'
                                                            ],
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 258237053,
                                                                         'full' => 258237053
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 13,
                                                                           'full' => 13
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '97784.977',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '75156.3018044238',
                                                                                    'full' => '75156.3018044238'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '49067.76',
                                                                         'full' => '49067.76'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '840615.589192708',
                                                                         'full' => '840615.589192708'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 3436,
                                                                         'full' => 3436
                                                                       },
                                                        'estimate_time' => '1053.509',
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 539140000,
                                                                             'full' => 539140000
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 466158894,
                                                                           'full' => 466158894
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '9500.30924582659',
                                                                             'full' => '9500.30924582659'
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '3436.000000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'lead.zmanda.com',
                                                        'tape_rate' => '75156.301804',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '10987.7',
                                                        'disk_name' => '/tensile-measurements',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '49067.760',
                                                        'dump_orig_kb' => '539140000',
                                                        'dump_comp' => '47.8979584152539',
                                                        'dump_out_kb' => '258237053'
                                                      }
                                                    ],
                                       'notes' => [
                                                    '  taper: tape metals-003-000105 kb 258237053 fm 13 [OK]'
                                                  ],
                                       'strange_summary' => [
                                                              '  lead.zmanda.com /tensile-measurements lev 0  STRANGE (see below)'
                                                            ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '840615.589192708',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'metals-003-000105',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '3401.029391',
                                                              'nc' => 13,
                                                              'size' => 258237053
                                                            }
                                                          ],
                                       'failure_details' => [
                                                              '  /-- lead.zmanda.com /tensile-measurements lev 0 FAILED [data read: recv error: Connection timed out]',
                                                              '  sendbackup: info BACKUP=APPLICATION',
                                                              '  sendbackup: info APPLICATION=amgtar',
                                                              '  sendbackup: info RECOVER_CMD=/usr/lib64/amanda/application/amgtar restore [./file-to-restore]+',
                                                              '  sendbackup: info COMPRESS_SUFFIX=.gz',
                                                              '  sendbackup: info end',
                                                              '  \\--------'
                                                            ],
                                       'strange_details' => [
                                                              '  /-- lead.zmanda.com /tensile-measurements lev 0 STRANGE',
                                                              '  sendbackup: info BACKUP=APPLICATION',
                                                              '  sendbackup: info APPLICATION=amgtar',
                                                              '  sendbackup: info RECOVER_CMD=/usr/lib64/amanda/application/amgtar restore [./file-to-restore]+',
                                                              '  sendbackup: info COMPRESS_SUFFIX=.gz',
                                                              '  sendbackup: info end',
                                                              '  ? /bin/tar: ./foundry.mpg: file changed as we read it',
                                                              '  \\--------'
                                                            ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('retried-nofinish');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-36'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'failure_summary' => [
                                                              '  jamon.slikon.local /var lev 0  FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326133640000.debug]',
                                                              '  jamon.slikon.local /var lev 0  was successfully retried'
                                                            ],
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 268356,
                                                                         'full' => 268356
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '15.589804',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '17213.5582974616',
                                                                                    'full' => '17213.5582974616'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '53.356',
                                                                         'full' => '53.356'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '873.580729166667',
                                                                         'full' => '873.580729166667'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '15.589804',
                                                                         'full' => '15.589804'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 2985670,
                                                                             'full' => 2985670
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 536715,
                                                                           'full' => 536715
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '10059.1311192743',
                                                                             'full' => '10059.1311192743'
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '15.589804',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'jamon.slikon.local',
                                                        'tape_rate' => '17213.595632',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '5029.5',
                                                        'disk_name' => '/var',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '53.356',
                                                        'dump_orig_kb' => '2985670',
                                                        'dump_comp' => '8.98813331680996',
                                                        'dump_out_kb' => '268356'
                                                      }
                                                    ],
                                       'notes' => [
                                                    '  planner: disk jamon.slikon.local:/var, estimate of level 1 failed.'
                                                  ],
                                       'failure_details' => [
                                                              '  /-- jamon.slikon.local /var lev 0 FAILED [/bin/tar exited with status 2: see /var/log/amanda/client/Daily/amgtar.20090326133640000.debug]',
                                                              '  blah blah blah',
                                                              '  \\--------'
                                                            ],
                                       'header' => [
                                                     '*** THE DUMPS DID NOT FINISH PROPERLY!'
                                                   ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '873.580729166667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-36',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '15.589804',
                                                              'nc' => 1,
                                                              'size' => 268356
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));


$cat = Installcheck::Catalogs::load('taperr');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 0,
                                                                          'full' => 0
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '2.247',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => undef,
                                                                                    'full' => undef
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '0.04',
                                                                         'full' => '0.04'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'estimate_time' => '1.137',
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 100,
                                                                             'full' => 100
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 100,
                                                                           'full' => 100
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => 2500,
                                                                             'full' => 2500
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'March 3, 2010',
                                                   'hostname' => 'euclid'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'euclid',
                                                        'tape_rate' => '      ',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '2491.0',
                                                        'disk_name' => '/A/p/etc',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '0.040',
                                                        'dump_orig_kb' => '100',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '100'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'hdisk' => {
                                                                    'size' => 0
                                                                  },
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'header' => [
                                                     '*** A TAPE ERROR OCCURRED: [Virtual-tape directory /A/p/vtapes does not exist.].'
                                                   ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('spanned');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 21830,
                                                                         'full' => 21830
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 6,
                                                                           'full' => 6
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '7.391',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '244169.78916168',
                                                                                    'full' => '244169.78916168'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '0.264',
                                                                         'full' => '0.264'
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '71.15234375',
                                                                         'full' => '71.15234375'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '0.089405',
                                                                         'full' => '0.089405'
                                                                       },
                                                        'estimate_time' => '5.128',
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 21830,
                                                                             'full' => 21830
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 21830,
                                                                           'full' => 21830
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => '82689.3939393939',
                                                                             'full' => '82689.3939393939'
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'March 3, 2010',
                                                   'hostname' => 'euclid'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'full',
                                                        'tape_duration' => '0.089405',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'euclid',
                                                        'tape_rate' => '244169.966680',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '82641.8',
                                                        'disk_name' => '/A/b/server-src',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '0.264',
                                                        'dump_orig_kb' => '21830',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '21830'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Conf-001',
                                                                                                 'Conf-002'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  taper: Will request retry of failed split part.',
                                                    '  taper: tape Conf-001 kb 15360 fm 4 [OK]',
                                                    '  taper: tape Conf-002 kb 6470 fm 2 [OK]'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '66.2109375',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Conf-001',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.079896',
                                                              'nc' => 4,
                                                              'size' => 20320
                                                            },
                                                            {
                                                              'percent_use' => '21.1002604166667',
                                                              'nb' => 0,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Conf-002',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.026898',
                                                              'nc' => 2,
                                                              'size' => 6470
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('fatal');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 0,
                                                                          'full' => 0
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '8.150',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => undef,
                                                                                    'full' => undef
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'March 3, 2010',
                                                   'hostname' => 'localhost.localdomain'
                                                 },
                                       'failure_summary' => [
                                                              '  planner: FATAL cannot fit anything on tape, bailing out',
                                                              '  localhost /boot lev 0  FAILED [dump larger than available tape space, 83480 KB, but cannot incremental dump new disk]'
                                                            ],
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  planner: tapecycle (3) <= runspercycle (10)',
                                                    '  planner: Forcing full dump of localhost:/boot as directed.',
                                                    '  planner: disk localhost:/boot, full dump (83480KB) will be larger than available tape space',
                                                    '  driver: WARNING: got empty schedule from planner'
                                                  ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('flush-origsize');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 83480,
                                                                         'full' => 83480
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '1.966',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '169009.750251045',
                                                                                    'full' => '169009.750251045'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '271.770833333333',
                                                                         'full' => '271.770833333333'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '0.493936',
                                                                         'full' => '0.493936'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'March 3, 2010',
                                                   'hostname' => 'localhost.localdomain'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '0.493936',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '169009.900121',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '148870',
                                                        'dump_comp' => '56.0757708067441',
                                                        'dump_out_kb' => '83480'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'TESTCONF02'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  taper: tape TESTCONF02 kb 83480 fm 9 [OK]'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '271.770833333333',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'TESTCONF02',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.493936',
                                                              'nc' => 1,
                                                              'size' => 83480
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('flush-noorigsize');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 83480,
                                                                         'full' => 83480
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 1,
                                                                           'full' => 1
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 1,
                                                                          'full' => 1
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '1.966',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => '169009.750251045',
                                                                                    'full' => '169009.750251045'
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => '271.770833333333',
                                                                         'full' => '271.770833333333'
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => '0.493936',
                                                                         'full' => '0.493936'
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'March 3, 2010',
                                                   'hostname' => 'localhost.localdomain'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '0.493936',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '0',
                                                        'hostname' => 'localhost',
                                                        'tape_rate' => '169009.900121',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '83480'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'TESTCONF02'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  taper: tape TESTCONF02 kb 83480 fm 9 [OK]'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '271.770833333333',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'TESTCONF02',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.493936',
                                                              'nc' => 1,
                                                              'size' => 83480
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('plannerfail');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 0,
                                                                          'full' => 0
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '2114.332',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => undef,
                                                                                    'full' => undef
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'estimate_time' => '2113.308',
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'March 13, 2010',
                                                   'hostname' => 'advantium'
                                                 },
                                       'failure_summary' => [
                                                              '  1.2.3.4 SystemState lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]',
                                                              '  1.2.3.4 "C:/" lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]',
                                                              '  1.2.3.4 "E:/Replication/Scripts" lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]',
                                                              '  1.2.3.4 "G:/" lev 0  FAILED [Request to 1.2.3.4 failed: recv error: Connection reset by peer]'
                                                            ],
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => '1.2.3.4',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '"C:/"',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => '1.2.3.4',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '"E:/Replication/Scripts"',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => '1.2.3.4',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '"G:/"',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => '1.2.3.4',
                                                        'tape_rate' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => 'SystemState',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  driver: WARNING: got empty schedule from planner',
                                                    '  taper: Will write new label `winsafe-002\' to new tape'
                                                  ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('skipped');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 0,
                                                                          'total' => 0,
                                                                          'full' => 0
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '77506.015',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => undef,
                                                                                    'total' => undef,
                                                                                    'full' => undef
                                                                                  },
                                                        'tapeparts' => '',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'skipped',
                                                        'tape_duration' => '',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '',
                                                        'hostname' => 'ns-new.slikon.local',
                                                        'tape_rate' => '',
                                                        'dump_partial' => undef,
                                                        'dump_rate' => '',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '',
                                                        'dump_comp' => '',
                                                        'dump_out_kb' => ''
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'Daily-36'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '0.0130208333333333',
                                                              'nb' => 0,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'Daily-36',
                                                              'configuration_id' => 1,
                                                              'time_duration' => 0,
                                                              'nc' => 0,
                                                              'size' => 0
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('filesystemstaped');
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 30720,
                                                                         'total' => 30720,
                                                                         'full' => 0
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 3,
                                                                           'total' => 3,
                                                                           'full' => 0
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 2,
                                                                          'total' => 2,
                                                                          'full' => 0
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '2.534',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => 153600,
                                                                                    'total' => 153600,
                                                                                    'full' => undef
                                                                                  },
                                                        'tapeparts' => '1:2 2:1',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => '100.065104166667',
                                                                         'total' => '100.065104166667',
                                                                         'full' => 0
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => '0.2',
                                                                         'total' => '0.2',
                                                                         'full' => 0
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '1:1 2:1',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'July 13, 2010',
                                                   'hostname' => 'localhost.localdomain'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '0.100000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '446100.000000',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '20480',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '20480'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '0.100000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '2',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '446100.000000',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/boot',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '10240',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '10240'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'next' => [
                                                                                                  'TESTCONF38'
                                                                                                ],
                                                                                      'use' => [
                                                                                                 'DAILY-37'
                                                                                               ],
                                                                                      'next_to_use' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  taper: tape DAILY-37 kb 30720 fm 3 [OK]'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '100.052083333333',
                                                              'nb' => 2,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DAILY-37',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.272815',
                                                              'nc' => 3,
                                                              'size' => 30720
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

$cat = Installcheck::Catalogs::load('multi-taper');
$cat->install();
$trace_log = "$logdir/log.20100908110856.0";

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'report' => {
                                       'statistic' => {
                                                        'tape_size' => {
                                                                         'incr' => 223050,
                                                                         'total' => 223050,
                                                                         'full' => 0
                                                                       },
                                                        'parts_taped' => {
                                                                           'incr' => 28,
                                                                           'total' => 28,
                                                                           'full' => 0
                                                                         },
                                                        'dles_taped' => {
                                                                          'incr' => 5,
                                                                          'total' => 5,
                                                                          'full' => 0
                                                                        },
                                                        'dumpdisks' => '',
                                                        'run_time' => '3.390',
                                                        'Avg_tape_write_speed' => {
                                                                                    'incr' => 44610,
                                                                                    'total' => 44610,
                                                                                    'full' => undef
                                                                                  },
                                                        'tapeparts' => '1:28',
                                                        'dump_time' => {
                                                                         'incr' => 0,
                                                                         'total' => 0,
                                                                         'full' => 0
                                                                       },
                                                        'tape_used' => {
                                                                         'incr' => '726.50390625',
                                                                         'total' => '726.50390625',
                                                                         'full' => 0
                                                                       },
                                                        'tape_time' => {
                                                                         'incr' => 5,
                                                                         'total' => 5,
                                                                         'full' => 0
                                                                       },
                                                        'estimate_time' => 0,
                                                        'original_size' => {
                                                                             'incr' => 0,
                                                                             'total' => 0,
                                                                             'full' => 0
                                                                           },
                                                        'output_size' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'tapedisks' => '1:5',
                                                        'dles_dumped' => {
                                                                           'incr' => 0,
                                                                           'total' => 0,
                                                                           'full' => 0
                                                                         },
                                                        'avg_compression' => {
                                                                               'incr' => undef,
                                                                               'total' => undef,
                                                                               'full' => undef
                                                                             },
                                                        'avg_dump_rate' => {
                                                                             'incr' => undef,
                                                                             'total' => undef,
                                                                             'full' => undef
                                                                           }
                                                      },
                                       'head' => {
                                                   'org' => 'DailySet1',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => 'September 8, 2010',
                                                   'hostname' => 'localhost.localdomain'
                                                 },
                                       'summary' => [
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '1.000000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '44610.000000',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/bootAMGTAR',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '44610',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '44610'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '1.000000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '44610.000000',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/bootAMGTAR',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '44610',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '44610'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '1.000000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '44610.000000',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/bootAMGTAR',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '44610',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '44610'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '1.000000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '44610.000000',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/bootAMGTAR',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '44610',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '44610'
                                                      },
                                                      {
                                                        'last_tape_label' => undef,
                                                        'dle_status' => 'nodump-FLUSH',
                                                        'tape_duration' => '1.000000',
                                                        'configuration_id' => 1,
                                                        'backup_level' => '1',
                                                        'hostname' => 'localhost.localdomain',
                                                        'tape_rate' => '44610.000000',
                                                        'dump_partial' => '',
                                                        'dump_rate' => '',
                                                        'disk_name' => '/bootAMGTAR',
                                                        'dump_timestamp' => undef,
                                                        'dump_duration' => '',
                                                        'dump_orig_kb' => '44610',
                                                        'dump_comp' => undef,
                                                        'dump_out_kb' => '44610'
                                                      }
                                                    ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'use' => [
                                                                                                 'DIRO-TEST-002',
                                                                                                 'DIRO-TEST-003',
                                                                                                 'DIRO-TEST-004',
                                                                                                 'DIRO-TEST-005',
                                                                                                 'DIRO-TEST-006',
                                                                                                 'DIRO-TEST-001'
                                                                                               ],
                                                                                      'next_to_use' => 1,
                                                                                      'new' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'notes' => [
                                                    '  taper: tape DIRO-TEST-003 kb 41696 fm 5 [OK]',
                                                    '  taper: tape DIRO-TEST-002 kb 41696 fm 5 [OK]',
                                                    '  taper: tape DIRO-TEST-004 kb 41696 fm 5 [OK]',
                                                    '  taper: tape DIRO-TEST-005 kb 41666 fm 5 [OK]',
                                                    '  taper: tape DIRO-TEST-006 kb 41666 fm 5 [OK]',
                                                    '  taper: tape DIRO-TEST-001 kb 14630 fm 3 [OK]'
                                                  ],
                                       'usage_by_tape' => [
                                                            {
                                                              'percent_use' => '135.807291666667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DIRO-TEST-002',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.455176',
                                                              'nc' => 5,
                                                              'size' => 41696
                                                            },
                                                            {
                                                              'percent_use' => '135.807291666667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DIRO-TEST-003',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.382599',
                                                              'nc' => 5,
                                                              'size' => 41696
                                                            },
                                                            {
                                                              'percent_use' => '135.807291666667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DIRO-TEST-004',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.394526',
                                                              'nc' => 5,
                                                              'size' => 41696
                                                            },
                                                            {
                                                              'percent_use' => '135.709635416667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DIRO-TEST-005',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.308434',
                                                              'nc' => 5,
                                                              'size' => 41666
                                                            },
                                                            {
                                                              'percent_use' => '135.709635416667',
                                                              'nb' => 1,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DIRO-TEST-006',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.323978',
                                                              'nc' => 5,
                                                              'size' => 41666
                                                            },
                                                            {
                                                              'percent_use' => '47.67578125',
                                                              'nb' => 0,
                                                              'dump_timestamp' => undef,
                                                              'tape_label' => 'DIRO-TEST-001',
                                                              'configuration_id' => 1,
                                                              'time_duration' => '0.084708',
                                                              'nc' => 3,
                                                              'size' => 14630
                                                            }
                                                          ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'severity' => '16',
                         'message' => 'The report',
                         'logfile' => $trace_log,
			 'process' => 'Amanda::Rest::Runs',
			 'running_on' => 'amanda-server',
                         'code' => 1900001
                       }
                     ],
      http_code => 200,
    },
    "status") || diag("reply: " . Data::Dumper::Dumper($reply));

Installcheck::Run::cleanup();
$testconf = Installcheck::Run::setup();
$testconf->add_param("autolabel", '"TESTCONF%%" any');
$testconf->add_dle("localhost $diskname installcheck-test");
$testconf->write();

# add a holding file that's in the disklist
write_holding_file("localhost", $Installcheck::Run::diskname);

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs/amflush","");
foreach my $message (@{$reply->{'body'}}) {
    if (defined $message and defined $message->{'code'}) {
	if ($message->{'code'} == 2200006) {
	    $timestamp = $message->{'timestamp'};
	}
	if ($message->{'code'} == 2200001) {
	    $amdump_log = $message->{'amdump_log'};
	}
	if ($message->{'code'} == 2200000) {
	    $trace_log = $message->{'trace_log'};
	}
    }
}
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body => [
                       {
                         'source_filename' => "$amperldir/Amanda/Amflush.pm",
			 'timestamp' => $timestamp,
                         'severity' => '2',
                         'message' => "The timestamp is '$timestamp'",
			 'process' => 'amflush',
			 'running_on' => 'amanda-server',
                         'code' => '2200006'
                       },
		       {
                         'source_filename' => "$amperldir/Amanda/Amflush.pm",
                         'amdump_log' => $amdump_log,
                         'severity' => '2',
                         'message' => "The amdump log file is '$amdump_log'",
			 'process' => 'amflush',
			 'running_on' => 'amanda-server',
                         'code' => '2200001'
                       },
                       {
                         'source_filename' => "$amperldir/Amanda/Amflush.pm",
                         'trace_log' => $trace_log,
                         'severity' => '2',
                         'message' => "The trace log file is '$trace_log'",
			 'process' => 'amflush',
			 'running_on' => 'amanda-server',
                         'code' => '2200000'
                       },
                       {
                         'source_filename' => "$amperldir/Amanda/Rest/Runs.pm",
                         'severity' => '2',
                         'message' => 'Running a flush',
			 'process' => 'amflush',
			 'running_on' => 'amanda-server',
                         'code' => '2200005'
                       }
                     ],
      http_code => 202,
    },
    "amflush") || diag("reply: " . Data::Dumper::Dumper($reply));

#wait until it is done
do {
    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs");
} while ($reply->{'body'}[0]->{'code'} == 2000004 and
	 $reply->{'body'}[0]->{'status'} ne 'done');
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Runs.pm",
		'severity' => '16',
		'message' => "one run",
		'run_type' => "amflush",
		'timestamp' => $timestamp,
		'amdump_log' => $amdump_log,
		'trace_log' => $trace_log,
		'status' => 'done',
		'process' => 'Amanda::Rest::Runs',
		'running_on' => 'amanda-server',
		'code' => '2000004'
	  },
        ],
      http_code => 200,
    },
    "get amflush run") || diag("reply: " . Data::Dumper::Dumper($reply));


$rest->stop();

Installcheck::Run::cleanup();
exit;
