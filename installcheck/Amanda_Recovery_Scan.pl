# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 95;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::Recovery::Scan;
use Installcheck::Run qw(run run_get run_err vtape_dir);

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# --------
# Interactivity package

package Amanda::Interactivity::Installcheck;
use vars qw( @ISA );
@ISA = qw( Amanda::Interactivity );

sub new {
    my $class = shift;
    my $self = {};
    return bless ($self, $class);
}
sub abort() {};
sub user_request {
    my $self = shift;
    my %params = @_;

    Amanda::Debug::debug("Change changer to multi-changer");
    $params{'request_cb'}->(undef, "multi-changer");
};

# --------
# back to the perl tests..

package main;

my $testconf = Installcheck::Config->new();

my $taperoot_disk = "$Installcheck::TMP/Amanda_Recovery_Scan_Disk";
#create a disk changer with 3 slots
{
    if (-d $taperoot_disk) {
        rmtree($taperoot_disk);
    }
    mkpath($taperoot_disk);

    for my $slot (1 .. 3) {
        mkdir("$taperoot_disk/slot$slot")
            or die("Could not mkdir: $!");
    }

    $testconf->add_changer("disk-changer", [
	'tpchanger' => "\"chg-disk:$taperoot_disk\"",
    ]);
}

my $taperoot_multi = "$Installcheck::TMP/Amanda_Recovery_Scan_Multi";
#create a multi changer
{
    if (-d $taperoot_multi) {
        rmtree($taperoot_multi);
    }
    mkpath($taperoot_multi);

    my @names;
    for my $slot (1 .. 3) {
        mkdir("$taperoot_multi/slot$slot")
            or die("Could not mkdir: $!");
        mkdir("$taperoot_multi/slot$slot/data")
            or die("Could not mkdir: $!");
	push @names, $slot;
    }

    my $chg_name = "chg-multi:file:$taperoot_multi/slot{".join(',', @names)."}";
    $testconf->add_changer("multi-changer", [
	'tpchanger' => "\"$chg_name\"",
	'changerfile' => "\"$Installcheck::TMP/Amanda_Recovery_Scan_Multi_status\"",
    ]);
}

my $taperoot_compat = "$Installcheck::TMP/Amanda_Recovery_Scan_Compat";
my $changerfile = "$Installcheck::TMP/scan-changerfile";

#create a compat changer
{
    if (-d $taperoot_compat) {
        rmtree($taperoot_compat);
    }
    mkpath($taperoot_compat);

    my @names;
    for my $slot (1 .. 3) {
        mkdir("$taperoot_compat/slot$slot")
            or die("Could not mkdir: $!");
        #mkdir("$taperoot_compat/slot$slot/data")
        #    or die("Could not mkdir: $!");
	push @names, $slot;
    }

    open (CONF, ">$changerfile");
    print CONF "firstslot=1\n";
    print CONF "lastslot=3\n";
    close CONF;

    $testconf->add_changer("compat-changer", [
	'tpchanger' => '"chg-disk"',
	'tapedev'   => "\"file:$taperoot_compat\"",
	'changerfile' => "\"$changerfile\"",
    ]);
}

my $taperoot_single = "$Installcheck::TMP/Amanda_Recovery_Scan_Single";
#create a single changer
{
    if (-d $taperoot_single) {
        rmtree($taperoot_single);
    }
    mkpath($taperoot_single);
    mkdir("$taperoot_single/data");

    $testconf->add_changer("single-changer", [
	'tpchanger' => "\"chg-single:file:$taperoot_single\"",
    ]);
}

$testconf->write();

Amanda::Config::config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");

# sub to label a slot in a changer
sub amlabel {
    my ($chg, $chg_name, $slot, $label, $finished_cb) = @_;
    my $res;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$chg->load(slot => $slot, res_cb => $steps->{'res_cb'});
    };

    step res_cb => sub {
	(my $err, $res) = @_;
	die "$err" if $err;

	$res->{'device'}->start($ACCESS_WRITE, $label, "20100201010203");
	$res->set_label(label => $label, finished_cb => $steps->{'set_label_finished'});
    };

    step set_label_finished => sub {
	my ($err) = @_;
	die "$err" if $err;

	$res->release(finished_cb => $steps->{'finished_cb'});
    };

    step finished_cb => sub {
	my ($err) = @_;
	die "$err" if $err;
	pass("label slot $slot of $chg_name with label '$label'");
	$finished_cb->();
    };
}

sub amlabel_sync {
    my ($chg, $chg_name, $slot, $label) = @_;

    amlabel($chg, $chg_name, $slot, $label,
	make_cb(finished_cb => sub { Amanda::MainLoop::quit(); }));
    Amanda::MainLoop::run();
}

# searching tests
sub test_searching {
    my ($chg, $chg_name, $finished_cb) = @_;
    my $scan;
    my $res01;
    my $res02;
    my $res03;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $scan->quit() };

    step start => sub {
	$scan = Amanda::Recovery::Scan->new(chg => $chg);
	$steps->{'find_04'}->();
    };

    step find_04 => sub {
	$scan->find_volume(label  => "TESTCONF04",
                           res_cb => $steps->{'res_cb_04'});
    };

    step res_cb_04 => sub {
	my ($err, $res) = @_;

	ok(!$res, "$chg_name didn't find TESTCONF04");
	ok($err->notfound, "$chg_name: TESTCONF04 error is notfound");

	$scan->find_volume(label  => "TESTCONF02",
                           res_cb => $steps->{'res_cb_02'});
    };

    step res_cb_02 => sub {
	(my $err, $res02) = @_;

	ok(!$err, "$chg_name found TESTCONF02");
	ok($res02, "$chg_name: TESTCONF02 give a reservation");

	$scan->find_volume(label  => "TESTCONF02",
			   res_cb => $steps->{'res_cb_02_volinuse'});
    };

    step res_cb_02_volinuse => sub {
	my ($err, $res) = @_;

	ok(!$res, "$chg_name doesn't reserve an already reserved slot");
	if ($chg_name eq "compat-changer" ||
	    $chg_name eq "single-changer") {
	    ok($err->driveinuse, "$chg_name: TESTCONF02 is driveinuse") ||
		    diag("$chg_name:".Dumper($err));
	} else {
	    ok($err->volinuse, "$chg_name: TESTCONF02 is volinuse") ||
		    diag("$chg_name:".Dumper($err));
	}

	$scan->find_volume(label  => "TESTCONF03",
			   res_cb => $steps->{'res_cb_03'});
    };

    step res_cb_03 => sub {
	(my $err, $res03) = @_;

	if ($chg_name eq "compat-changer" ||
	    $chg_name eq "single-changer") {
	    ok($err, "$chg_name doesn't found TESTCONF03");
	    ok($err->driveinuse, "$chg_name TESTCONF03 is driveinuse") ||
		diag($err."\n");
	    ok(!$res03, "$chg_name: TESTCONF03 give no reservation");
	} else {
	    ok(!$err, "$chg_name found TESTCONF03");
	    ok($res03, "$chg_name: TESTCONF03 give a reservation");
	}
	$scan->find_volume(label  => "TESTCONF01",
			   res_cb => $steps->{'res_cb_01'});
    };

    step res_cb_01 => sub {
	(my $err, $res01) = @_;

	if ($chg_name eq "compat-changer" ||
	    $chg_name eq "single-changer") {
	    ok($err, "$chg_name doesn't found TESTCONF01");
	    ok($err->driveinuse, "$chg_name TESTCONF01 is driveinuse") ||
		diag($err."\n");
	    ok(!$res01, "$chg_name: TESTCONF01 give no reservation");
	} else {
	    ok(!$err, "$chg_name found TESTCONF01");
	    ok($res01, "$chg_name: TESTCONF01 give a reservation");
	}
	$scan->find_volume(label  => "TESTCONF05",
			   res_cb => $steps->{'res_cb_05'});
    };

    step res_cb_05 => sub {
	my ($err, $res) = @_;

	if ($chg_name eq "compat-changer" ||
	    $chg_name eq "single-changer") {
	    ok($err, "$chg_name doesn't found TESTCONF05");
	    ok($err->driveinuse, "$chg_name TESTCONF05 is driveinuse") ||
		diag($err."\n");
	    ok(!$res, "$chg_name: TESTCONF05 give no reservation");
	} else {
	    ok(!$res, "$chg_name doesn't found TESTCONF05");
	    ok($err->notfound, "$chg_name: TESTCONF05 is notfound");
	}
	$scan->find_volume(label  => "TESTCONF01",
			   res_cb => $steps->{'res_cb_01_volinuse'});
    };

    step res_cb_01_volinuse => sub {
	my ($err, $res) = @_;

	ok($err, "$chg_name doesn't found TESTCONF01");
	if ($chg_name eq "compat-changer" ||
	    $chg_name eq "single-changer") {
	    ok($err->driveinuse, "$chg_name TESTCONF01 is driveinuse") ||
		diag($err."\n");
	} else {
	    ok($err->volinuse, "$chg_name TESTCONF01 is volinuse") ||
		diag($err."\n");
	}
	ok(!$res, "$chg_name: TESTCONF01 give no reservation");
	$steps->{'release01'}->();
    };

    step release01 => sub {
	if ($res01) {
	    $res01->release(finished_cb => $steps->{'release02'});
	} else {
	    $steps->{'release02'}->();
	}
    };

    step release02 => sub {
	$res02->release(finished_cb => $steps->{'release03'});
    };

    step release03 => sub {
	if ($res03) {
	    $res03->release(finished_cb => $steps->{'done'});
	} else {
	    $steps->{'done'}->();
	}
    };

    step done => sub {
	pass("done with searching test on $chg_name");
	$finished_cb->();
    };
}

foreach my $chg_name ("disk-changer", "multi-changer", "compat-changer",
		      "single-changer") {
    # amlabel has to be done outside of Amanda::MainLoop
    my $chg = Amanda::Changer->new($chg_name);
    if ($chg_name eq "single-changer") {
	amlabel_sync($chg, $chg_name, 1, 'TESTCONF02');
    } else {
	amlabel_sync($chg, $chg_name, 1, 'TESTCONF01');
	amlabel_sync($chg, $chg_name, 2, 'TESTCONF02');
	amlabel_sync($chg, $chg_name, 3, 'TESTCONF03');
    }

    test_searching($chg, $chg_name, \&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
}

#test SCAN_POLL
sub test_scan_poll {
    my ($chg_name, $finished_cb) = @_;

    my $scan;
    my $chg;
    my $res04;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $scan->quit() };

    step start => sub {
	$chg = Amanda::Changer->new($chg_name);
	$scan = Amanda::Recovery::Scan->new(chg => $chg);
	$scan->{'scan_conf'}->{'notfound'} = Amanda::Recovery::Scan::SCAN_POLL;
	$scan->{'scan_conf'}->{'volinuse'} = Amanda::Recovery::Scan::SCAN_POLL;
	$scan->{'scan_conf'}->{'poll_delay'} = 10; # 10 ms

	$steps->{'find_04'}->();
    };

    step find_04 => sub {
	Amanda::MainLoop::call_after(100, $steps->{'label_04'});
	$scan->find_volume(label  => "TESTCONF04",
			   res_cb => $steps->{'res_cb_04'});
	pass("began searching for TESTCONF04");
    };

    step label_04 => sub {
	# this needs to be run on a different process.
	ok(run('amlabel', '-f', "-otpchanger=$chg_name", 'TESTCONF',
	       'TESTCONF04', 'slot', '3'),
	   "label slot 3 of $chg_name with label TESTCONF04");
	# note: nothing to do in the amlabel callback
    };

    step res_cb_04 => sub {
	(my $err, $res04) = @_;

	ok(!$err, "$chg_name found TESTCONF04 after POLL");
	ok($res04, "$chg_name: TESTCONF04 give a reservation after POLL");

	$res04->release(finished_cb => $steps->{'done'});
    };

    step done => sub {
	pass("done with SCAN_POLL on $chg_name");
        $finished_cb->();
    };
}

foreach my $chg_name ("disk-changer", "multi-changer") {
    test_scan_poll($chg_name, \&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
}

#test SCAN_ASK_POLL which change the changer.
#label TESTCONF05 in multi-changer
#start the scan on disk-changer
#interactivity module change changer to multi-changer
sub test_scan_ask_poll {
    my ($finished_cb) = @_;
    my $scan;
    my $res05;

    my $chg_name = "multi-changer";
    my $chg = Amanda::Changer->new($chg_name);
    amlabel_sync($chg, $chg_name, 2, 'TESTCONF05');
    $chg->quit();
    $chg = Amanda::Changer->new("disk-changer");

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $scan->quit() };

    step start => sub {
	my $interactivity = Amanda::Interactivity::Installcheck->new();
	$scan = Amanda::Recovery::Scan->new(chg =>         $chg,
					    interactivity => $interactivity);
	$scan->{'scan_conf'}->{'poll_delay'} = 10; # 10 ms

	$steps->{'find_05'}->();
    };

    step find_05 => sub {
	$scan->find_volume(label  => "TESTCONF05",
			   res_cb => $steps->{'res_cb_05'});
    };

    step res_cb_05 => sub {
	(my $err, $res05) = @_;

	ok(!$err, "found TESTCONF05 on changer multi");
	ok($res05, "TESTCONF05 give a reservation after interactivity");
	is($res05->{'chg'}->{'chg_name'}, $chg_name,
	   "found TESTCONF05 on correct changer: $chg_name");

	$res05->release(finished_cb => $steps->{'done'});
    };

    step done => sub {
	pass("done with SCAN_ASK_POLL");
        $finished_cb->();
    };
}
test_scan_ask_poll(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

rmtree($taperoot_disk);
rmtree($taperoot_multi);
rmtree($taperoot_compat);
rmtree($taperoot_single);
unlink($changerfile);
