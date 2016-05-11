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

use Test::More tests => 16;

use lib '@amperldir@';
use Cwd qw(abs_path getcwd);
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
use Amanda::Config qw( :init );
use Amanda::Paths;
use warnings;
use strict;

# send xfer logging somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $reply;
my $config_dir = $Amanda::Paths::CONFIG_DIR;
my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

# Run amdump with client- and server-side scripts

my $templog = $Installcheck::TMP . "/check-script." . $$;

sub verify_log {
    my $msg = shift;
    my @exp = @_;
    my @got;
    my ($exp, $got);
    my $logfile;

    if (!open($logfile, "<", $templog)) {
	fail($msg);
	diag("Logfile '$templog' does not exist.");
	return;
    }

    my $linenum = 1;
    foreach $exp (@exp) {
	chomp $exp;
	$got = <$logfile>;
	chomp $got;
	push @got, $got;
	if (!$got) {
	    fail($msg);
	    diag("    Line: $linenum");
	    diag("Expected: '$exp'");
	    diag("     Got: EOF");
	    diag($exp);
	    for $got (<$logfile>) {
		chomp $got;
		push @got, $got;
	    }
	    diag("exp: " . join("\n     ",@exp) . "\ngot: " . join("\n     ", @got) . "\n");
	    return;
	}
	$got =~ s/ *$//g;
	if ($got ne $exp) {
	    fail($msg);
	    diag("    Line: $linenum");
	    diag("Expected: '$exp'");
	    diag("     Got: '$got'");
	    for $got (<$logfile>) {
		chomp $got;
		push @got, $got;
	    }
	    diag("exp: " . join("\n     ",@exp) . "\ngot: " . join("\n     ", @got) . "\n");
	    return;
	}
	$linenum++;
    }
    $got = <$logfile>;
    if ($got) {
        chomp $got;
	fail($msg);
	diag("    Line: $linenum");
	diag("Expected: EOF");
	diag("     Got: '$got'");
	push @got, $got;
	for $got (<$logfile>) {
	    chomp $got;
	    push @got, $got;
	}
	diag("exp: " . join("\n     ",@exp) . "\ngot: " . join("\n     ", @got) . "\n");
	return;
    }
    pass($msg);
};

# check script on client
$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');

$testconf->add_dle(<<EODLE);
localhost diskname1 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amgtar"
	property "atime_preserve" "no" # note underscore
    }
    script {
	plugin "amlog-script"
	execute-where client
	execute-on pre-dle-amcheck, post-dle-amcheck, pre-dle-estimate, post-dle-estimate, pre-dle-backup, post-dle-backup
	property "logfile" "$templog"
    }
}
EODLE
$testconf->write();

unlink $templog;
ok(run('amcheck', '-c', 'TESTCONF'), "amcheck runs successfully for client scripts.");

verify_log("amcheck invokes correct script commands",
    "check TESTCONF pre-dle-amcheck client localhost diskname1 $diskname",
    "check TESTCONF post-dle-amcheck client localhost diskname1 $diskname",
);

unlink $templog;
ok(run('amdump', 'TESTCONF'), "amdump runs successfully for client scripts.")
    or amdump_diag();

verify_log("amdump invokes correct script commands",
    "estimate TESTCONF pre-dle-estimate client localhost diskname1 $diskname 0",
    "estimate TESTCONF post-dle-estimate client localhost diskname1 $diskname 0",
    "backup TESTCONF pre-dle-backup client localhost diskname1 $diskname 0",
    "backup TESTCONF post-dle-backup client localhost diskname1 $diskname 0",
);

Installcheck::Run::cleanup();

#check script on server
$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');

$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amgtar"
	property "atime-preserve" "no"
    }
    script {
	plugin "amlog-script"
	single-execution yes
	execute-where server
	execute-on pre-host-amcheck, post-host-amcheck, pre-host-estimate, post-host-estimate, pre-host-backup, post-host-backup
	property "logfile" "$templog"
    }
}
EODLE
$testconf->add_dle(<<EODLE);
localhost diskname3 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amgtar"
	property "atime-preserve" "no"
    }
    script {
	plugin "amlog-script"
	single-execution yes
	execute-where server
	execute-on pre-host-amcheck, post-host-amcheck, pre-host-estimate, post-host-estimate, pre-host-backup, post-host-backup
	property "logfile" "$templog"
    }
}
EODLE
$testconf->write();

unlink $templog;
ok(run('amcheck', '-c', 'TESTCONF'), "amcheck runs successfully for server scripts.");

verify_log("amcheck invokes correct script commands",
    "check TESTCONF pre-host-amcheck server localhost diskname3 $diskname",
    "check TESTCONF post-host-amcheck server localhost diskname3 $diskname",
);

unlink $templog;
ok(run('amdump', 'TESTCONF'), "amdump runs successfully for server scripts.")
    or amdump_diag();

verify_log("amdump invokes correct script commands",
    "estimate TESTCONF pre-host-estimate server localhost diskname3 $diskname",
    "estimate TESTCONF post-host-estimate server localhost diskname3 $diskname",
    "backup TESTCONF pre-host-backup server localhost diskname3 $diskname",
    "backup TESTCONF post-host-backup server localhost diskname3 $diskname",
);

unlink $templog;
Installcheck::Run::cleanup();

#check order script
$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');

$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amgtar"
	property "atime-preserve" "no"
    }
    script {
	plugin "amlog-script"
	execute-where client
	execute-on pre-host-amcheck
	property "logfile" "$templog"
	property "TEXT" "50"
	order 50
    }
    script {
	plugin "amlog-script"
	execute-where client
	execute-on pre-host-amcheck
	property "logfile" "$templog"
	property "TEXT" "60"
	order 60
    }
    script {
	plugin "amlog-script"
	execute-where client
	execute-on pre-host-amcheck
	property "logfile" "$templog"
	property "TEXT" "40"
	order 40
    }
}
EODLE
$testconf->write();

unlink $templog;
ok(run('amcheck', '-c', 'TESTCONF'), "amcheck runs successfully for ordered scripts.");

verify_log("amcheck invokes script in correct order",
    "check TESTCONF pre-host-amcheck client localhost diskname2 $diskname  40",
    "check TESTCONF pre-host-amcheck client localhost diskname2 $diskname  50",
    "check TESTCONF pre-host-amcheck client localhost diskname2 $diskname  60",
);

unlink $templog;
SKIP: {
    eval 'use Installcheck::Rest;';
    skip "Can't load Installcheck::Rest: $@", 6 if $@;

    my $rest = Installcheck::Rest->new();

    skip "Can't start JSON Rest server: $rest->{'error'}: see " . Amanda::Debug::dbfn(), 6 if $rest->{'error'};

    my $timestamp;
    my $amdump_log;
    my $trace_log;

    $testconf = Installcheck::Run::setup();
    $testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
    $testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
  installcheck-test
  program "APPLICATION"
  application {
    plugin "amgtar"
    property "ATIME-PRESERVE" "NO"
  }
  script {
    plugin "script-fail"
    execute-where client
    execute-on pre-host-estimate
  }
}
EODLE
    $testconf->add_dle(<<EODLE);
localhost diskname3 $diskname {
  installcheck-test
  program "APPLICATION"
  application {
    plugin "amgtar"
    property "ATIME-PRESERVE" "NO"
  }
}
EODLE
    $testconf->write();

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
    is ($reply->{http_code}, 202, "correct http_code")
        or diag("reply: " . Data::Dumper::Dumper($reply));

    #wait for the run to end
    do {
        $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs");
        } while ($reply->{'body'}[0]->{'code'} == 2000004 and
                 $reply->{'body'}[0]->{'status'} ne 'done');

    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
    is_deeply (Installcheck::Rest::cleanup_for_amdump(Installcheck::Rest::remove_source_line($reply)),
    { body => [
	{
                         'message' => 'The report',
                         'process' => 'Amanda::Rest::Report',
                         'report' => {
                                       'head' => {
                                                   'hostname' => 'localhost.localdomain',
                                                   'org' => 'DailySet1',
                                                   'exit_status' => '5',
                                                   'config_name' => 'TESTCONF',
                                                   'date' => undef,
                                                   'timestamp' => $timestamp,
                                                 },
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'new' => '1',
                                                                                      'next_to_use' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'statistic' => {
                                                        'dumpdisks' => '',
                                                        'output_size' => {
                                                                           'total' => '0',
                                                                           'full' => '0',
                                                                           'incr' => '0'
                                                                         },
                                                        'Avg_tape_write_speed' => undef,
                                                        'avg_dump_rate' => undef,
                                                        'estimate_time' => undef,
                                                        'parts_taped' => {
                                                                           'incr' => '0',
                                                                           'total' => '0',
                                                                           'full' => '0'
                                                                         },
                                                        'dles_dumped' => {
                                                                           'incr' => '0',
                                                                           'full' => '0',
                                                                           'total' => '0'
                                                                         },
                                                        'avg_compression' => {
                                                                               'full' => undef,
                                                                               'total' => undef,
                                                                               'incr' => undef
                                                                             },
                                                        'tape_time' => undef,
                                                        'original_size' => {
                                                                             'incr' => '0',
                                                                             'full' => '0',
                                                                             'total' => '0'
                                                                           },
                                                        'dump_time' => undef,
                                                        'run_time' => undef,
                                                        'tapedisks' => '',
                                                        'dles_taped' => {
                                                                          'full' => '0',
                                                                          'total' => '0',
                                                                          'incr' => '0'
                                                                        },
                                                        'tape_used' => {
                                                                         'incr' => '0',
                                                                         'total' => '0',
                                                                         'full' => '0'
                                                                       },
                                                        'tapeparts' => '',
                                                        'tape_size' => {
                                                                         'incr' => '0',
                                                                         'full' => '0',
                                                                         'total' => '0'
                                                                       }
                                                      },
                                       'notes' => [
                                                    '  planner: tapecycle (2) <= runspercycle (10)',
                                                    '  planner: Adding new disk localhost:diskname2.',
                                                    '  planner: Adding new disk localhost:diskname3.',
                                                    '  driver: WARNING: got empty schedule from planner',
                                                    '  taper: Slot 1 without label can be labeled'
                                                  ],
                                       'failure_summary' => [
                                                              '  planner: ERROR localhost:  "Script \'script-fail\' command \'PRE-HOST-ESTIMATE\': stderr error:  PRE-HOST-ESTIMATE"',
                                                              '  localhost diskname2 lev 0  FAILED [localhost:  "Script \'script-fail\' command \'PRE-HOST-ESTIMATE\': stderr error:  PRE-HOST-ESTIMATE"]',
                                                              '  localhost diskname3 lev 0  FAILED [localhost:  "Script \'script-fail\' command \'PRE-HOST-ESTIMATE\': stderr error:  PRE-HOST-ESTIMATE"]'
                                                            ],
                                       'summary' => [
                                                      {
                                                        'dump_duration' => undef,
                                                        'dump_out_kb' => '',
                                                        'hostname' => 'localhost',
                                                        'tape_duration' => undef,,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'configuration_id' => '1',
                                                        'dump_orig_kb' => '',
                                                        'dump_timestamp' => undef,
                                                        'tape_rate' => undef,
                                                        'last_tape_label' => undef,
                                                        'backup_level' => '',
                                                        'dump_rate' => undef,
                                                        'dump_partial' => '',
                                                        'dump_comp' => '',
                                                        'disk_name' => 'diskname2'
                                                      },
                                                      {
                                                        'configuration_id' => '1',
                                                        'dle_status' => 'nodump-FAILED',
                                                        'tape_duration' => undef,
                                                        'hostname' => 'localhost',
                                                        'dump_out_kb' => '',
                                                        'dump_duration' => undef,
                                                        'last_tape_label' => undef,
                                                        'tape_rate' => undef,
                                                        'dump_timestamp' => undef,
                                                        'dump_orig_kb' => '',
                                                        'backup_level' => '',
                                                        'disk_name' => 'diskname3',
                                                        'dump_comp' => '',
                                                        'dump_partial' => '',
                                                        'dump_rate' => undef
                                                      }
                                                    ]
                                     },
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'component' => 'rest-server',
                         'logfile' =>  $trace_log,
                         'code' => '1900001',
                         'severity' => 'success',
                         'running_on' => 'amanda-server',
                         'module' => 'amanda'
                       }
      ],
      http_code => 200,
    },
    "report") or diag("reply: " . Data::Dumper::Dumper($reply));

    $testconf = Installcheck::Run::setup();
    $testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
    $testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
  installcheck-test
  program "APPLICATION"
  application {
    plugin "amgtar"
    property "ATIME-PRESERVE" "NO"
  }
  script {
    plugin "script-fail"
    execute-where client
    execute-on pre-dle-estimate
  }
}
EODLE
    $testconf->add_dle(<<EODLE);
localhost diskname3 $diskname {
  installcheck-test
  program "APPLICATION"
  application {
    plugin "amgtar"
    property "ATIME-PRESERVE" "NO"
  }
}
EODLE
    $testconf->write();

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
    is ($reply->{http_code}, 202, "correct http_code")
        or diag("reply: " . Data::Dumper::Dumper($reply));

    #wait for the run to end
    do {
        $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs");
        } while ($reply->{'body'}[0]->{'code'} == 2000004 and
                 $reply->{'body'}[0]->{'status'} ne 'done');

    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
    is_deeply (Installcheck::Rest::cleanup_for_amdump(Installcheck::Rest::remove_source_line($reply)),
    { body => [
                       {
                         'running_on' => 'amanda-server',
                         'module' => 'amanda',
                         'component' => 'rest-server',
                         'message' => 'The report',
                         'code' => '1900001',
                         'severity' => 'success',
                         'process' => 'Amanda::Rest::Report',
                         'logfile' => $trace_log,
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'report' => {
                                       'notes' => [
                                                    '  planner: tapecycle (2) <= runspercycle (10)',
                                                    '  planner: Adding new disk localhost:diskname2.',
                                                    '  planner: Adding new disk localhost:diskname3.',
                                                    '  taper: Slot 1 without label can be labeled',
                                                    '  taper: tape TESTCONF01 kb 1050 fm 1 [OK]'
                                                  ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'new' => '1',
                                                                                      'use' => [
                                                                                                 'TESTCONF01'
                                                                                               ],
                                                                                      'next_to_use' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'statistic' => {
                                                        'dles_dumped' => {
                                                                           'incr' => '0',
                                                                           'total' => '1',
                                                                           'full' => '1'
                                                                         },
                                                        'output_size' => {
                                                                           'full' => '1050',
                                                                           'incr' => '0',
                                                                           'total' => '1050'
                                                                         },
                                                        'Avg_tape_write_speed' => undef,
                                                        'tape_time' => undef,
                                                        'dump_time' => undef,
                                                        'run_time' => undef,
                                                        'avg_dump_rate' => undef,
                                                        'tape_used' => {
                                                                         'incr' => '0',
                                                                         'total' => '3.44401041666667',
                                                                         'full' => '3.44401041666667'
                                                                       },
                                                        'tapeparts' => '',
                                                        'dles_taped' => {
                                                                          'total' => '1',
                                                                          'incr' => '0',
                                                                          'full' => '1'
                                                                        },
                                                        'tape_size' => {
                                                                         'full' => '1050',
                                                                         'total' => '1050',
                                                                         'incr' => '0'
                                                                       },
                                                        'avg_compression' => {
                                                                               'full' => '100',
                                                                               'incr' => undef,
                                                                               'total' => '100'
                                                                             },
                                                        'dumpdisks' => '',
                                                        'estimate_time' => undef,
                                                        'parts_taped' => {
                                                                           'incr' => '0',
                                                                           'total' => '1',
                                                                           'full' => '1'
                                                                         },
                                                        'tapedisks' => '',
                                                        'original_size' => {
                                                                             'incr' => '0',
                                                                             'total' => '1050',
                                                                             'full' => '1050'
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'dump_timestamp' => undef,
                                                        'dump_comp' => '',
                                                        'hostname' => 'localhost',
                                                        'dump_out_kb' => '',
                                                        'last_tape_label' => undef,
                                                        'configuration_id' => '1',
                                                        'dump_rate' => undef,
                                                        'dle_status' => 'nodump-FAILED',
                                                        'disk_name' => 'diskname2',
                                                        'backup_level' => '',
                                                        'tape_duration' => undef,
                                                        'tape_rate' => undef,
                                                        'dump_duration' => undef,
                                                        'dump_partial' => '',
                                                        'dump_orig_kb' => ''
                                                      },
                                                      {
                                                        'tape_duration' => undef,
                                                        'tape_rate' => undef,
                                                        'backup_level' => '0',
                                                        'dump_partial' => '',
                                                        'dump_orig_kb' => '1050',
                                                        'dump_duration' => undef,
                                                        'hostname' => 'localhost',
                                                        'dump_timestamp' => undef,
                                                        'dump_comp' => undef,
                                                        'dle_status' => 'full',
                                                        'dump_rate' => undef,
                                                        'configuration_id' => '1',
                                                        'disk_name' => 'diskname3',
                                                        'dump_out_kb' => '1050',
                                                        'last_tape_label' => undef
                                                      }
                                                    ],
                                       'failure_summary' => [
                                                              '  localhost diskname2 lev 0  FAILED [Script \'script-fail\' command \'PRE-DLE-ESTIMATE\': stderr error:  PRE-DLE-ESTIMATE]'
                                                            ],
                                       'usage_by_tape' => [
                                                            {
                                                              'nb' => '1',
                                                              'time_duration' => undef,
                                                              'size' => '1050',
                                                              'tape_label' => 'TESTCONF01',
                                                              'configuration_id' => '1',
                                                              'nc' => '1',
                                                              'percent_use' => '3.44401041666667',
                                                              'dump_timestamp' => undef
                                                            }
                                                          ],
                                       'head' => {
                                                   'exit_status' => '4',
                                                   'hostname' => 'localhost.localdomain',
                                                   'date' => undef,
                                                   'timestamp' => $timestamp,
                                                   'config_name' => 'TESTCONF',
                                                   'org' => 'DailySet1'
                                                 }
                                     }
                       }
      ],
      http_code => 200,
    },
    "report") or diag("reply: " . Data::Dumper::Dumper($reply));

    $testconf = Installcheck::Run::setup();
    $testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
    $testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
  installcheck-test
  program "APPLICATION"
  application {
    plugin "amgtar"
    property "ATIME-PRESERVE" "NO"
  }
  script {
    plugin "script-fail"
    execute-where client
    execute-on pre-dle-backup
  }
}
EODLE
    $testconf->add_dle(<<EODLE);
localhost diskname3 $diskname {
  installcheck-test
  program "APPLICATION"
  application {
    plugin "amgtar"
    property "ATIME-PRESERVE" "NO"
  }
}
EODLE
    $testconf->write();

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
    is ($reply->{http_code}, 202, "correct http_code")
        or diag("reply: " . Data::Dumper::Dumper($reply));

    #wait for the run to end
    do {
        $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/runs");
        } while ($reply->{'body'}[0]->{'code'} == 2000004 and
                 $reply->{'body'}[0]->{'status'} ne 'done');

    $reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/report?logfile=$trace_log");
    is_deeply (Installcheck::Rest::cleanup_for_amdump(Installcheck::Rest::remove_source_line($reply)),
    { body => [
                       {
                         'running_on' => 'amanda-server',
                         'module' => 'amanda',
                         'component' => 'rest-server',
                         'message' => 'The report',
                         'code' => '1900001',
                         'severity' => 'success',
                         'process' => 'Amanda::Rest::Report',
                         'logfile' => $trace_log,
                         'source_filename' => "$amperldir/Amanda/Rest/Report.pm",
                         'report' => {
                                       'notes' => [
                                                    '  planner: tapecycle (2) <= runspercycle (10)',
                                                    '  planner: Adding new disk localhost:diskname2.',
                                                    '  planner: Adding new disk localhost:diskname3.',
                                                    '  taper: Slot 1 without label can be labeled',
                                                    '  taper: tape TESTCONF01 kb 1050 fm 1 [OK]'
                                                  ],
                                       'tapeinfo' => {
                                                       'storage' => {
                                                                      'TESTCONF' => {
                                                                                      'new' => '1',
                                                                                      'use' => [
                                                                                                 'TESTCONF01'
                                                                                               ],
                                                                                      'next_to_use' => 1
                                                                                    }
                                                                    }
                                                     },
                                       'statistic' => {
                                                        'dles_dumped' => {
                                                                           'incr' => '0',
                                                                           'total' => '1',
                                                                           'full' => '1'
                                                                         },
                                                        'output_size' => {
                                                                           'full' => '1050',
                                                                           'incr' => '0',
                                                                           'total' => '1050'
                                                                         },
                                                        'Avg_tape_write_speed' => undef,
                                                        'tape_time' => undef,
                                                        'dump_time' => undef,
                                                        'run_time' => undef,
                                                        'avg_dump_rate' => undef,
                                                        'tape_used' => {
                                                                         'incr' => '0',
                                                                         'total' => '3.44401041666667',
                                                                         'full' => '3.44401041666667'
                                                                       },
                                                        'tapeparts' => '',
                                                        'dles_taped' => {
                                                                          'total' => '1',
                                                                          'incr' => '0',
                                                                          'full' => '1'
                                                                        },
                                                        'tape_size' => {
                                                                         'full' => '1050',
                                                                         'total' => '1050',
                                                                         'incr' => '0'
                                                                       },
                                                        'avg_compression' => {
                                                                               'full' => '100',
                                                                               'incr' => undef,
                                                                               'total' => '100'
                                                                             },
                                                        'dumpdisks' => '',
                                                        'estimate_time' => undef,
                                                        'parts_taped' => {
                                                                           'incr' => '0',
                                                                           'total' => '1',
                                                                           'full' => '1'
                                                                         },
                                                        'tapedisks' => '',
                                                        'original_size' => {
                                                                             'incr' => '0',
                                                                             'total' => '1050',
                                                                             'full' => '1050'
                                                                           }
                                                      },
                                       'summary' => [
                                                      {
                                                        'dump_timestamp' => undef,
                                                        'dump_comp' => undef,
                                                        'hostname' => 'localhost',
                                                        'dump_out_kb' => '',
                                                        'last_tape_label' => undef,
                                                        'configuration_id' => '1',
                                                        'dump_rate' => undef,
                                                        'dle_status' => 'nodump-PARTIAL',
                                                        'disk_name' => 'diskname2',
                                                        'backup_level' => '0',
                                                        'tape_duration' => undef,
                                                        'tape_rate' => undef,
                                                        'dump_duration' => undef,
                                                        'dump_partial' => '',
                                                        'dump_orig_kb' => ''
                                                      },
                                                      {
                                                        'tape_duration' => undef,
                                                        'tape_rate' => undef,
                                                        'backup_level' => '0',
                                                        'dump_partial' => '',
                                                        'dump_orig_kb' => '1050',
                                                        'dump_duration' => undef,
                                                        'hostname' => 'localhost',
                                                        'dump_timestamp' => undef,
                                                        'dump_comp' => undef,
                                                        'dle_status' => 'full',
                                                        'dump_rate' => undef,
                                                        'configuration_id' => '1',
                                                        'disk_name' => 'diskname3',
                                                        'dump_out_kb' => '1050',
                                                        'last_tape_label' => undef
                                                      }
                                                    ],
                                       'failure_summary' => [
                                                              '  localhost diskname2 lev 0  FAILED [Script \'script-fail\' command \'PRE-DLE-BACKUP\': stderr error:  PRE-DLE-BACKUP]',
                                                              '  localhost diskname2 lev 0  FAILED Got empty header',
                                                              '  localhost diskname2 lev 0  FAILED [Script \'script-fail\' command \'PRE-DLE-BACKUP\': stderr error:  PRE-DLE-BACKUP]',
                                                              '  localhost diskname2 lev 0  FAILED Got empty header'
                                                            ],
                                       'usage_by_tape' => [
                                                            {
                                                              'nb' => '1',
                                                              'time_duration' => undef,
                                                              'size' => '1050',
                                                              'tape_label' => 'TESTCONF01',
                                                              'configuration_id' => '1',
                                                              'nc' => '1',
                                                              'percent_use' => '3.44401041666667',
                                                              'dump_timestamp' => undef
                                                            }
                                                          ],
				       'failure_details' => [
                                                             '  /-- localhost diskname2 lev 0 FAILED [Script \'script-fail\' command \'PRE-DLE-BACKUP\': stderr error:  PRE-DLE-BACKUP]',
                                                             '  sendbackup: error [Script \'script-fail\' command \'PRE-DLE-BACKUP\': stderr error:  PRE-DLE-BACKUP]',
                                                             '  sendbackup: error [Script \'script-fail\' command \'PRE-DLE-BACKUP\' exited with status 1: see /tmp/amanda/client/TESTCONF/sendbackup.DATESTAMP.debug]',
                                                             '  \\--------',
                                                             '  /-- localhost diskname2 lev 0 FAILED [Script \'script-fail\' command \'PRE-DLE-BACKUP\': stderr error:  PRE-DLE-BACKUP]',
                                                             '  sendbackup: error [Script \'script-fail\' command \'PRE-DLE-BACKUP\': stderr error:  PRE-DLE-BACKUP]',
                                                             '  sendbackup: error [Script \'script-fail\' command \'PRE-DLE-BACKUP\' exited with status 1: see /tmp/amanda/client/TESTCONF/sendbackup.DATESTAMP.debug]',
                                                             '  \\--------'
                                                            ],
                                       'head' => {
                                                   'exit_status' => '4',
                                                   'hostname' => 'localhost.localdomain',
                                                   'date' => undef,
                                                   'timestamp' => $timestamp,
                                                   'config_name' => 'TESTCONF',
                                                   'org' => 'DailySet1'
                                                 }
                                     }
                       }
      ],
      http_code => 200,
    },
    "report") or diag("reply: " . Data::Dumper::Dumper($reply));


    $rest->stop();
}

Installcheck::Run::cleanup();
