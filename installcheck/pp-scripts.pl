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

use Test::More tests => 10;

use lib "@amperldir@";
use Cwd qw(abs_path getcwd);
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
use Amanda::Config qw( :init );
use Amanda::Paths;
use warnings;
use strict;

my $testconf;

# Run amdump with client- and server-side scripts

my $templog = $Installcheck::TMP . "/check-script." . $$;

sub verify_log {
    my $msg = shift;
    my @exp = @_;
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
	if (!$got) {
	    fail($msg);
	    diag("    Line: $linenum");
	    diag("Expected: '$exp'");
	    diag("     Got: EOF");
	    diag($exp);
	    return;
	}
	$got =~ s/ *$//g;
	if ($got ne $exp) {
	    fail($msg);
	    diag("    Line: $linenum");
	    diag("Expected: '$exp'");
	    diag("     Got: '$got'");
	    return;
	}
	$linenum++;
    }
    $got = <$logfile>;
    if ($got) {
	fail($msg);
	diag("    Line: $linenum");
	diag("Expected: EOF");
	diag("     Got: '$got'");
	diag($got);
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
Installcheck::Run::cleanup();
