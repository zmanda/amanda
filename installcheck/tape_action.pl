# Copyright (c) 2016-2016 Carbonite, Inc.  All Rights Reserved.
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

use Test::More tests => 12;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname $holdingdir $exit_code $taperoot amdump_diag clean_taperoot);
use Amanda::Config qw( :init );
use Amanda::Debug;
use Amanda::Paths;
use File::Path;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;

# Just run amdump.

$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');

# Two amrandom dle
$testconf->add_dle(<<EODLE);
localhost diskname1 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "1075200"
    }
}
EODLE

$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "1075200"
    }
}
EODLE

$testconf->add_taperscan("lexical", [ "plugin" => '"lexical"' ]);
$testconf->add_param("taperscan", '"lexical"');
$testconf->add_tapetype("tape-1500",
			[ length => "1500k", filemark => "0k" ]);
$testconf->rm_param("tapetype");
$testconf->add_param("tapetype", '"tape-1500"');
$testconf->add_param("device-property", "\"ENFORCE_MAX_VOLUME_USAGE\" \"TRUE\"");
$testconf->write();

#ok(!run('amdump', 'TESTCONF'), "amdump failed with runtapes=1")
#    or amdump_diag();
#is($exit_code, 16, "exit code is not 16");
#is(check_logs("1 tapes filled; runtapes=1 does not allow additional tapes"), 2, "good error");
#
#run('amadmin', 'TESTCONF', 'force', 'localhost');
#if (-d $holdingdir) {
#    rmtree("$holdingdir/*");
#}
#clean_taperoot(3);
$testconf->add_param("runtapes", "2");
$testconf->write();

#ok(run('amdump', 'TESTCONF'), "amdump runs successfully")
#    or amdump_diag();
#is($exit_code, 0, "exit code is not 0");
#is(check_logs("INFO taper tape TESTCONF01 kb 1306 fm 2", "INFO taper tape TESTCONF02 kb 794 fm 1"), 2, "2 vtapes used");
#
#run('amadmin', 'TESTCONF', 'force', 'localhost');
#if (-d $holdingdir) {
#    rmtree("$holdingdir/*");
#}
#clean_taperoot(5);
$testconf->rm_param("tapecycle");
$testconf->add_param("tapecycle", "5");
$testconf->add_tapetype("tape-800",
			[ length => "800k", filemark => "0k" ]);
$testconf->rm_param("tapetype");
$testconf->add_param("tapetype", '"tape-800"');
$testconf->rm_param("runtapes", "2");
$testconf->add_param("runtapes", "4");
$testconf->add_param("taper-parallel-write", "2");
$testconf->write();
#ok(run('amdump', 'TESTCONF'), "amdump runs successfully")
#    or amdump_diag();
#is($exit_code, 0, "exit code is not 0");
#is(check_logs("INFO taper tape TESTCONF01 kb 640 fm 1", "INFO taper tape TESTCONF02 kb 602 fm 2", "INFO taper tape TESTCONF03 kb 640 fm 1", "INFO taper tape TESTCONF04 kb 218 fm 1"), 4, "4 vtapes used");
#
#run('amadmin', 'TESTCONF', 'force', 'localhost');
#if (-d $holdingdir) {
#    rmtree("$holdingdir/*");
#}
#clean_taperoot(5);
$testconf->add_param("device-property", "\"SLOW_WRITE\" \"YES\"");
$testconf->add_param("debug-driver", "9");
$testconf->write();
#ok(run('amdump', 'TESTCONF'), "amdump runs successfully")
#    or amdump_diag();
#is($exit_code, 0, "exit code is not 0");
#is(check_logs("INFO taper tape TESTCONF01 kb 640 fm 1", "INFO taper tape TESTCONF02 kb 640 fm 1", "INFO taper tape TESTCONF03 kb 410 fm 1", "INFO taper tape TESTCONF04 kb 410 fm 1"), 4, "4 vtapes used");
#
#run('amadmin', 'TESTCONF', 'force', 'localhost');
#if (-d $holdingdir) {
#    rmtree("$holdingdir/*");
#}
#clean_taperoot(5);
$testconf->add_tapetype("tape-900",
			[ length => "900k", filemark => "0k" ]);
$testconf->rm_param("tapetype");
$testconf->add_param("tapetype", '"tape-900"');
$testconf->add_param("device-property", "\"SLOW_WRITE\" \"YES\"");
$testconf->add_param("debug-driver", "9");
$testconf->write();
my $result;

ok(run('amdump', 'TESTCONF'), "amdump runs successfully")
    or amdump_diag();
is($exit_code, 0, "exit code is not 0");
is(check_logs("INFO taper tape TESTCONF01 kb 736 fm 1", "INFO taper tape TESTCONF02 kb 736 fm 1", "INFO taper tape TESTCONF03 kb 628 fm 2"), 3, "3 vtapes used");
$result = check_amdump(
	[
		"to taper0: START-TAPER taper0 worker0-0 TESTCONF",
		"from taper0: TAPER-OK worker0-0 ALLOW-TAKE-SCRIBE-FROM",
		"to taper0: FILE-WRITE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: REQUEST-NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: START-SCAN worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01",
		"from taper0: READY worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTDONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01 1 736",
		"from taper0: REQUEST-NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: START-SCAN worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF03",
		"from taper0: PARTDONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF03 1 314",
		"from taper0: DONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d INPUT-GOOD TAPE-GOOD",
		"to taper0: TAKE-SCRIBE-FROM worker0-1 \\d\\d-\\d\\d\\d\\d\\d worker0-0",
		"to taper0: QUIT"
	],
	[
		"to taper0: START-TAPER taper0 worker0-1 TESTCONF",
		"from taper0: TAPER-OK worker0-1 ALLOW-TAKE-SCRIBE-FROM",
		"to taper0: FILE-WRITE worker0-1 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: REQUEST-NEW-TAPE worker0-1 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: START-SCAN worker0-1 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: NEW-TAPE worker0-1 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: NEW-TAPE worker0-1 \\d\\d-\\d\\d\\d\\d\\d TESTCONF02",
		"from taper0: READY worker0-1 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTDONE worker0-1 \\d\\d-\\d\\d\\d\\d\\d TESTCONF02 1 736",
		"from taper0: REQUEST-NEW-TAPE worker0-1 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: TAKE-SCRIBE-FROM worker0-1 \\d\\d-\\d\\d\\d\\d\\d worker0-0",
		"from taper0: READY worker0-1 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTDONE worker0-1 \\d\\d-\\d\\d\\d\\d\\d TESTCONF03 2 314",
		"from taper0: DONE worker0-1 \\d\\d-\\d\\d\\d\\d\\d INPUT-GOOD TAPE-GOOD",
		"to taper0: CLOSE-VOLUME worker0-1",
		"from taper0: CLOSED-VOLUME worker0-1",
		"to taper0: QUIT"
	]);
ok($result == 34, "amdump is good") ||
    dump_amdump($result);

$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');

# Two amrandom dle
$testconf->add_dle(<<EODLE);
localhost diskname1 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "1471488"
    }
}
EODLE

$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "204800"
    }
}
EODLE

$testconf->add_taperscan("lexical", [ "plugin" => '"lexical"' ]);
$testconf->add_param("taperscan", '"lexical"');
$testconf->add_tapetype("tape-1500",
			[ length => "1500k", filemark => "0k" ]);
$testconf->rm_param("tapetype");
$testconf->add_param("tapetype", '"tape-1500"');
$testconf->add_param("device-property", "\"ENFORCE_MAX_VOLUME_USAGE\" \"TRUE\"");
$testconf->add_param("taperalgo", 'smallest');
$testconf->add_param("flush_threshold_dumped", '500');
$testconf->add_param("flush_threshold_scheduled", '500');
$testconf->add_param("taperflush", '100');
$testconf->add_param("runtapes", '5');
$testconf->add_param("debug-driver", '9');
$testconf->write();
ok(run('amdump', 'TESTCONF'), "amdump runs successfully")
    or amdump_diag();
is($exit_code, 0, "exit code is not 0");
is(check_logs("INFO taper tape TESTCONF01 kb 1288 fm 2", "INFO taper tape TESTCONF02 kb 349 fm 1"), 2, "2 vtapes used");
$result = check_amdump(
	[
		"to taper0: START-TAPER taper0 worker0-0 TESTCONF",
		"from taper0: TAPER-OK worker0-0 ALLOW-TAKE-SCRIBE-FROM",
		"to taper0: FILE-WRITE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: REQUEST-NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: START-SCAN worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01",
		"from taper0: READY worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTDONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01 1 200",
		"from taper0: DONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d INPUT-GOOD TAPE-GOOD",
		"to taper0: FILE-WRITE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: READY worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTDONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01 2 1088",
		"from taper0: REQUEST-NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: START-SCAN worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF02",
		"from taper0: PARTDONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF02 1 349",
		"from taper0: DONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d INPUT-GOOD TAPE-GOOD",
		"to taper0: QUIT"
	]);
ok($result == 20, "amdump is good") ||
    dump_amdump($result);


$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');

# Two amrandom dle
$testconf->add_dle(<<EODLE);
localhost diskname1 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "1469440"
    }
}
EODLE

$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "204800"
    }
}
EODLE

$testconf->add_taperscan("lexical", [ "plugin" => '"lexical"' ]);
$testconf->add_param("taperscan", '"lexical"');
$testconf->add_tapetype("tape-1500",
			[ length => "1500k", filemark => "0k" ]);
$testconf->rm_param("tapetype");
$testconf->add_param("tapetype", '"tape-1500"');
$testconf->add_param("device-property", "\"ENFORCE_MAX_VOLUME_USAGE\" \"TRUE\"");
$testconf->add_param("taperalgo", 'smallest');
$testconf->add_param("flush_threshold_dumped", '500');
$testconf->add_param("flush_threshold_scheduled", '500');
$testconf->add_param("taperflush", '100');
$testconf->add_param("runtapes", '5');
$testconf->add_param("debug-driver", '9');
$testconf->write();
ok(!run('amdump', 'TESTCONF'), "amdump exited with no zero")
    or amdump_diag();
is($exit_code, 16, "exit code is not 16");
is(check_logs("INFO taper tape TESTCONF01 kb 1288 fm 2"), 1, "1 vtapes used");
$result = check_amdump(
	[
		"to taper0: START-TAPER taper0 worker0-0 TESTCONF",
		"from taper0: TAPER-OK worker0-0 ALLOW-TAKE-SCRIBE-FROM",
		"to taper0: FILE-WRITE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: REQUEST-NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: START-SCAN worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01",
		"from taper0: READY worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTDONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01 1 200",
		"from taper0: DONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d INPUT-GOOD TAPE-GOOD",
		"to taper0: FILE-WRITE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: READY worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTDONE worker0-0 \\d\\d-\\d\\d\\d\\d\\d TESTCONF01 2 1088",
		"from taper0: REQUEST-NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: START-SCAN worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"to taper0: NO-NEW-TAPE worker0-0 \\d\\d-\\d\\d\\d\\d\\d",
		"from taper0: PARTIAL worker0-0 \\d\\d-\\d\\d\\d\\d\\d INPUT-GOOD TAPE-CONFIG",
		"to taper0: QUIT"
	]);
ok($result == 18, "amdump is good") ||
    dump_amdump($result);


Installcheck::Run::cleanup();

sub check_logs {
    my @lines = @_;
    my $good = 0;

    open(my $logfile, "<", "$CONFIG_DIR/TESTCONF/log/log")
	or die("opening log: $!");
    foreach my $logline (<$logfile>) {
	foreach my $line (@lines) {
	    $good++ if ($logline =~ /$line/);
	}
    }
    close($logfile);
    return $good;
}

sub check_amdump {
    my @plines = @_;
    my $good = 0;

    open(my $amdump, "<", "$CONFIG_DIR/TESTCONF/log/amdump.1")
	or die("opening amdump $!");
    foreach my $logline (<$amdump>) {
	#my $a = 0;
	foreach my $lines (@plines) {
	    my $line = @$lines[0];
	    if (defined $line && $logline =~ /$line/) {
		$good++;
		#$a++;
		shift @$lines;
	    }
	}
	#diag("$a: $good: $logline");
    }
    close($amdump);
    return $good;
}

sub dump_amdump {
    my $rline;
    for my $result (@_) {
	$rline .= " result=$result";
    }
    diag("got $rline\n");
    open(my $amdump, "<", "$CONFIG_DIR/TESTCONF/log/amdump.1")
	or die("opening amdump $!");
    foreach my $logline (<$amdump>) {
	diag($logline) if $logline =~ /from taper0/;
	diag($logline) if $logline =~ /to taper0/;
    }
    close($amdump);
}
