# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 30;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Config;
use Installcheck::Changer;
use Installcheck::Mock qw( setup_mock_mtx $mock_mtx_path );
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::Storage;
use Amanda::Taper::Scan;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $taperoot = "$Installcheck::TMP/Amanda_Taper_Scan_lexical";
my $tapelist_filename = "$Installcheck::TMP/tapelist";
`cat /dev/null > $Installcheck::TMP/tapelist`;
my ($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);

# vtape support
my %slot_label;

sub reset_taperoot {
    my ($nslots) = @_;

    if (-d $taperoot) {
	rmtree($taperoot);
    }
    mkpath($taperoot);

    for my $slot (1 .. $nslots) {
	mkdir("$taperoot/slot$slot")
	    or die("Could not mkdir: $!");
    }

    # clear out the tapefile
    open(my $fh, ">", $tapelist_filename) or die("opening tapelist_filename: $!");
    %slot_label = ();
}

sub label_slot {
    my ($slot, $label, $stamp, $reuse, $update_tapelist) = @_;

    my $drivedir = "$taperoot/tmp";
    -d $drivedir and rmtree($drivedir);
    mkpath($drivedir);
    symlink("$taperoot/slot$slot", "$drivedir/data");

    my $dev = Amanda::Device->new("file:$drivedir");
    die $dev->error_or_status() unless $dev->status == $DEVICE_STATUS_SUCCESS;

    if (defined $label){
	if (!$dev->start($ACCESS_WRITE, $label, $stamp)) {
	    die $dev->error_or_status();
	}
    } else {
	$dev->erase();
    }

    rmtree($drivedir);

    if ($update_tapelist) {
	if (exists $slot_label{$slot}) {
	    $tapelist->remove_tapelabel($slot_label{$slot});
	    delete $slot_label{$slot};
	}
	# tapelist uses '0' for new tapes; devices use 'X'..
	$stamp = '0' if ($stamp eq 'X');
	$reuse = $reuse ne 'no-reuse';
	$tapelist->remove_tapelabel($label);
	$tapelist->add_tapelabel($stamp, $label, "", $reuse);
	$tapelist->write();
	$slot_label{$slot} = $label;
    }
}

sub label_mtx_slot {
    my ($slot, $label, $stamp, $reuse, $update_tapelist) = @_;

    my $drivedir = "$taperoot/tmp";
    -d $drivedir and rmtree($drivedir);
    mkpath($drivedir);
    mkdir("$taperoot/slot$slot/data");
    symlink("$taperoot/slot$slot/data", "$drivedir/data");

    my $dev = Amanda::Device->new("file:$drivedir");
    die $dev->error_or_status() unless $dev->status == $DEVICE_STATUS_SUCCESS;

    if (defined $label){
	if (!$dev->start($ACCESS_WRITE, $label, $stamp)) {
	    die $dev->error_or_status();
	}
    } else {
	$dev->erase();
    }

    rmtree($drivedir);

    if ($update_tapelist) {
	if (exists $slot_label{$slot}) {
	    $tapelist->remove_tapelabel($slot_label{$slot});
	    delete $slot_label{$slot};
	}
	# tapelist uses '0' for new tapes; devices use 'X'..
	$stamp = '0' if ($stamp eq 'X');
	$reuse = $reuse ne 'no-reuse';
	$tapelist->remove_tapelabel($label);
	$tapelist->add_tapelabel($stamp, $label, "", $reuse);
	$tapelist->write();
	$slot_label{$slot} = $label;
    }
}


# run the mainloop around a scan
sub run_scan {
    my ($ts) = @_;
    my ($error, $res, $label, $mode);

    my $result_cb = make_cb(result_cb => sub {
	($error, $res, $label, $mode) = @_;

	$error = "$error" if defined $error;
	if ($res) {
	    $res->release(finished_cb => sub {
		Amanda::MainLoop::quit();
	    });
	} else {
	    Amanda::MainLoop::quit();
	}
    });

    $ts->scan(result_cb => $result_cb);
    Amanda::MainLoop::run();
    return $error, $label, $mode;
}

# set the current slot on the changer
sub set_current_slot {
    my ($slot) = @_;

    unlink("$taperoot/data");
    symlink("slot$slot", "$taperoot/data");
}

# set up and load a config
my $testconf = Installcheck::Config->new();
$testconf->add_param("tapelist", "\"$tapelist\"");
$testconf->add_policy("test_policy", [ retention_tapes => 4 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
				 policy    => "\"test_policy\"",
				 labelstr  => "\"TEST-[0-9]+\"" ]);
$testconf->add_param("storage", "\"disk\"");
$testconf->write();
my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

reset_taperoot(6);
$tapelist->reset_tapelist();
$tapelist->write();

label_slot(1, "TEST-10", "20090424173011", "reuse", 1);
label_slot(2, "TEST-20", "20090424173022", "reuse", 1);
label_slot(3, "TEST-30", "20090424173033", "reuse", 1);

my $storage;
my $chg;
my $taperscan;
my @results;

# set up a lexical taperscan
$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ "No acceptable volumes found", undef, undef ],
	  "no reusable tapes -> error")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 2 ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-10", $ACCESS_WRITE ],
	  "finds the best reusable tape")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 1 ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
die "$storage" if $storage->isa("Amanda::Changer::Error");
set_current_slot(2);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-10", $ACCESS_WRITE ],
	  "finds the first reusable tape")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 1 ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

label_slot(4, "TEST-40", "20090424173030", "reuse", 1);
$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "finds the first reusable tape")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 2 ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

label_slot(4, "TEST-40", "X", "reuse", 1);
label_slot(5, "TEST-35", "X", "reuse", 1);
$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-35", $ACCESS_WRITE ],
	  "finds the first reusable tape")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 2 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
				 policy    => "\"test_policy\"",
				 labelstr  => "\"TEST-[0-9]+\"",
				 autolabel  => "\"TEST-%0\" empty volume-error" ]);

$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-50", $ACCESS_WRITE ],
	  "labels new tapes in blank slots")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 2 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
                                 policy    => "\"test_policy\"",
                                 labelstr  => "\"TEST-[0-9]+\"" ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
# simulate "amlabel"
label_slot(1, "TEST-60", "X", "reuse", 1);
$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
set_current_slot(2);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-35", $ACCESS_WRITE ],
	  "scans for volumes, even with a newly labeled volume available")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 1 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
                                 policy    => "\"test_policy\"",
                                 labelstr  => "\"TEST-[0-9]+\"",
				 autolabel  => "\"TEST-%0\" empty volume-error" ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# test new_labeled with autolabel
reset_taperoot(5);
$tapelist->reset_tapelist();
$tapelist->write();

label_slot(1, "TEST-10", "20090424173011", "reuse", 1);
label_slot(2, "TEST-20", "20090424173033", "reuse", 1);
label_slot(4, "TEST-30", "20090424173022", "reuse", 1);
label_slot(3, "TEST-15", "X", "reuse", 1);
label_slot(5, "TEST-25", "X", "reuse", 1);
set_current_slot(2);

set_current_slot(1);

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
$taperscan->{'scan_conf'}->{'new_labeled'} = 'soon';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-25", $ACCESS_WRITE ],
	  "autolabel soon")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'order';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-25", $ACCESS_WRITE ],
	  "autolabel order")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'last';
$taperscan->{'scan_conf'}->{'new_volume'} = 'last';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-30", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 1 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
                                 policy    => "\"test_policy\"",
                                 labelstr  => "\"TEST-[0-9]+\"",
				 autolabel  => "\"TEST-%0\" empty volume-error" ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# test new_labeled with autolabel
reset_taperoot(5);
$tapelist->reset_tapelist();
$tapelist->write();

label_slot(1, "TEST-10", "20090424173011", "reuse", 1);
label_slot(2, "TEST-20", "20090424173033", "reuse", 1);
label_slot(4, "TEST-30", "20090424173022", "reuse", 1);
label_slot(3, "TEST-15", "X", "reuse", 1);
set_current_slot(2);

set_current_slot(1);

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
$taperscan->{'scan_conf'}->{'new_labeled'} = 'soon';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-15", $ACCESS_WRITE ],
	  "autolabel soon")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'order';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-30", $ACCESS_WRITE ],
	  "autolabel order")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'last';
$taperscan->{'scan_conf'}->{'new_volume'} = 'last';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-30", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'last';
$taperscan->{'scan_conf'}->{'new_volume'} = 'order';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-30", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 1 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
                                 policy    => "\"test_policy\"",
                                 labelstr  => "\"TEST-[0-9]+\"",
				 autolabel  => "\"TEST-%0\" empty volume-error" ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# test new_labeled with autolabel
reset_taperoot(5);
$tapelist->reset_tapelist();
$tapelist->write();

label_slot(1, "TEST-10", "20090424173011", "reuse", 1);
label_slot(2, "TEST-20", "20090424173022", "reuse", 1);
label_slot(4, "TEST-30", "20090424173033", "reuse", 1);
label_slot(3, "TEST-15", "X", "reuse", 1);
set_current_slot(2);

set_current_slot(1);

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
$taperscan->{'scan_conf'}->{'new_labeled'} = 'soon';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-15", $ACCESS_WRITE ],
	  "autolabel soon")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'order';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel order")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'last';
$taperscan->{'scan_conf'}->{'new_volume'} = 'last';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-10", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'last';
$taperscan->{'scan_conf'}->{'new_volume'} = 'order';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 1 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
                                 policy    => "\"test_policy\"",
                                 labelstr  => "\"TEST-[0-9]+\"",
				 autolabel  => "\"TEST-%0\" empty volume-error" ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# test new_volume with autolabel
reset_taperoot(5);
$tapelist->reset_tapelist();
$tapelist->write();

label_slot(1, "TEST-10", "20090424173011", "reuse", 1);
label_slot(2, "TEST-20", "20090424173033", "reuse", 1);
label_slot(4, "TEST-30", "20090424173022", "reuse", 1);
set_current_slot(2);

set_current_slot(1);

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
$taperscan->{'scan_conf'}->{'new_labeled'} = 'soon';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel soon")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'order';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-30", $ACCESS_WRITE ],
	  "autolabel order")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'last';
$taperscan->{'scan_conf'}->{'new_volume'} = 'last';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-30", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 4 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
                                 policy    => "\"test_policy\"",
                                 labelstr  => "\"TEST-[0-9]+\"",
				 autolabel  => "\"TEST-%0\" empty volume-error" ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
$taperscan->{'scan_conf'}->{'new_labeled'} = 'soon';
$taperscan->{'scan_conf'}->{'new_volume'} = 'soon';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel soon")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'order';
$taperscan->{'scan_conf'}->{'new_volume'} = 'order';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel order")
	  or diag(Dumper(\@results));

$taperscan->{'seen'} = {};
$taperscan->{'scan_conf'}->{'new_labeled'} = 'last';
$taperscan->{'scan_conf'}->{'new_volume'} = 'last';
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-40", $ACCESS_WRITE ],
	  "autolabel last")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

$testconf->add_policy("test_policy", [ retention_tapes => 1 ]);
$testconf->add_storage("disk", [ tpchanger => "\"chg-disk:$taperoot\"",
                                 policy    => "\"test_policy\"",
                                 labelstr  => "\"TEST-[0-9]+\"" ]);
$testconf->write();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# test skipping no-reuse tapes
reset_taperoot(5);
$tapelist->reset_tapelist();
$tapelist->write();

label_slot(1, "TEST-1", "20090424173001", "no-reuse", 1);
label_slot(2, "TEST-2", "20090424173002", "reuse", 1);
label_slot(3, "TEST-3", "20090424173003", "reuse", 1);
label_slot(4, "TEST-4", "20090424173004", "reuse", 1);

set_current_slot(1);

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-2", $ACCESS_WRITE ],
	  "skips a no-reuse volume")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

rmtree($taperoot);
unlink($tapelist);

# test do not use no-reuse with a datestamp of 0
reset_taperoot(5);
$tapelist->reset_tapelist();
$tapelist->write();

label_slot(1, "TEST-1", "X", "no-reuse", 1);
label_slot(2, "TEST-2", "X", "no-reuse", 1);
label_slot(3, "TEST-3", "X", "reuse", 1);
label_slot(4, "TEST-4", "X", "no-reuse", 1);
label_slot(5, "TEST-5", "X", "no-reuse", 1);

$storage = Amanda::Storage->new(storage_name => "disk", tapelist => $tapelist);
set_current_slot(1);

$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "lexical",
    storage => $storage);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-3", $ACCESS_WRITE ],
	  "skips a no-reuse volume")
	  or diag(Dumper(\@results));
$taperscan->quit();
$storage->quit();

rmtree($taperoot);
unlink($tapelist);

# test invalid because slot loaded in invalid drive
my $chg_state_file = "$Installcheck::TMP/chg-robot-state";
unlink($chg_state_file) if -f $chg_state_file;
my $vtape_root = $taperoot;
my $mtx_state_file = setup_mock_mtx (
	 num_slots => 5,
	 num_ie => 0,
	 barcodes => 1,
	 track_orig => 1,
	 num_drives => 2,
	 vtape_root => $vtape_root,
	 loaded_slots => {
		1 => '11111',
		2 => '22222',
		3 => '33333',
		4 => '44444',
		5 => '55555',
	  },
	 first_slot => 1,
	 first_drive => 0,
	 first_ie => 6,
	);

$testconf = Installcheck::Config->new();
$testconf->add_param("tapelist", "\"$tapelist_filename\"");
$testconf->add_param("labelstr", "\"TEST-[0-9]+\"");
$testconf->add_changer('robo1', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
	changerfile => "\"$chg_state_file\"",

	# point to the two vtape "drives" that mock/mtx will set up
	property => "\"tape-device\" \"1=file:$vtape_root/drive1\"",

	# an point to the mock mtx
	property => "\"mtx\" \"$mock_mtx_path\"",
]);
$testconf->add_changer('robo2', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
	changerfile => "\"$chg_state_file\"",

	# point to the two vtape "drives" that mock/mtx will set up
	property => "\"tape-device\" \"0=file:$vtape_root/drive0\" \"1=file:$vtape_root/drive1\"",

	# an point to the mock mtx
	property => "\"mtx\" \"$mock_mtx_path\"",
]);
$testconf->add_policy("test_policy", [ retention_tapes => 2 ]);
$testconf->add_storage("robo1", [ tpchanger => "\"robo1\"",
				  policy => "\"test_policy\"",
				  labelstr  => "\"TEST-[0-9]+\"" ]);
$testconf->add_storage("robo2", [ tpchanger => "\"robo2\"",
				  policy => "\"test_policy\"",
				  labelstr  => "\"TEST-[0-9]+\"" ]);
$testconf->write();
reset_taperoot(5);
$tapelist->reset_tapelist();
$tapelist->write();

label_mtx_slot(1, "TEST-1", "20090424173001", "reuse", 1);
label_mtx_slot(2, "TEST-2", "20090424173002", "reuse", 1);
label_mtx_slot(3, "TEST-3", "20090424173003", "reuse", 1);
label_mtx_slot(4, "TEST-4", "20090424173004", "reuse", 1);

$testconf->remove_param("tpchanger");
$testconf->add_param("storage", "\"robo2\"");
$testconf->write();

$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
}

$chg = Amanda::Changer->new("robo2", tapelist => $tapelist);
die "$chg" if $chg->isa("Amanda::Changer::Error");

sub test_robot {
    my $finished_cb = shift;

    my $steps = define_steps
        cb_ref => \$finished_cb;

    step setup => sub {
        $chg->load(slot => 1, res_cb => $steps->{'loaded_slot'});
    };

    step loaded_slot => sub {
        my ($err, $res1) = @_;
        die $err if $err;

        is($res1->{'device'}->device_name, "file:$vtape_root/drive0",
            "first load returns drive-0 device");

        $res1->release(finished_cb => $steps->{'released'});
    };

    step released => sub {
        $chg->quit();
	$storage = Amanda::Storage->new(storage_name => "robo1",
					tapelist => $tapelist);
        $taperscan = Amanda::Taper::Scan->new(
	    tapelist  => $tapelist,
	    algorithm => "lexical",
	    storage => $storage);
        @results = run_scan($taperscan);
        is_deeply([ @results ],
	    [ undef, "TEST-2", $ACCESS_WRITE ],
	      "skip a slot loaded in an inaccessible drive")
	      or diag(Dumper(\@results));
        $chg->quit();
        $taperscan->quit();
	$storage->quit();
    };

};

test_robot(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

unlink($chg_state_file) if -f $chg_state_file;
unlink($mtx_state_file) if -f $mtx_state_file;
unlink($tapelist);

