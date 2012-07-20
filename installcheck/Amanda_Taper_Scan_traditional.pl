# Copyright (c) 2010-2012 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 9;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Changer;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::Taper::Scan;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $taperoot = "$Installcheck::TMP/Amanda_Taper_Scan_traditional";
my $tapelist_filename = "$Installcheck::TMP/tapelist";
my $tapelist = Amanda::Tapelist->new($tapelist_filename);

# vtape support

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
	# tapelist uses '0' for new tapes; devices use 'X'..
	$stamp = '0' if ($stamp eq 'X');
	open(my $fh, ">>", $tapelist_filename) or die("opening tapelist_filename: $!");
	print $fh "$stamp $label $reuse\n";
	close($fh);
    }
}

# run the mainloop around a scan
sub run_scan {
    my ($ts) = @_;
    my ($error, $res, $label, $mode);

    my $result_cb = make_cb(result_cb => sub {
	($error, $res, $label, $mode) = @_;

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
$testconf->add_param("labelstr", "\"TEST-[0-9]+\"");
$testconf->write();
my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

reset_taperoot(5);
label_slot(1, "TEST-1", "20090424173001", "reuse", 1);
label_slot(2, "TEST-2", "20090424173002", "reuse", 1);
label_slot(3, "TEST-3", "20090424173003", "reuse", 1);

my $chg;
my $taperscan;
my @results;

# set up a traditional taperscan
$chg = Amanda::Changer->new("chg-disk:$taperoot", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 4,
    changer => $chg);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ "No acceptable volumes found", undef, undef ],
	  "no reusable tapes -> error")
	  or diag(Dumper(\@results));
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot", tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 3,
    changer => $chg);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-1", $ACCESS_WRITE ],
	  "finds the best reusable tape")
	  or diag(Dumper(\@results));
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot", tapelist => $tapelist);
$chg->{'support_fast_search'} = 0; # no fast search -> skip stage 1
set_current_slot(2); # slot 2 is acceptable, so it should be returned
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 1,
    changer => $chg);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-2", $ACCESS_WRITE ],
	  "finds the first reusable tape when fast_search is false")
	  or diag(Dumper(\@results));
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot", tapelist => $tapelist);
$chg->{'support_fast_search'} = 1;

label_slot(1); # remove TEST-1
label_slot(4, "TEST-4", "20090424183004", "reuse", 1);
set_current_slot(1);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 2,
    changer => $chg);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-2", $ACCESS_WRITE ],
	  "uses the first usable tape it finds when oldest is missing")
	  or diag(Dumper(\@results));
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot", tapelist => $tapelist);

set_current_slot(3);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 2,
    changer => $chg);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-3", $ACCESS_WRITE ],
	  "starts sequential scan at 'current'")
	  or diag(Dumper(\@results));
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot",
			tapelist => $tapelist,
			autolabel => { 'template'     => "TEST-%",
			               'empty'        => 1,
			               'volume_error' => 1});
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 2,
    changer => $chg);
set_current_slot(5);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-5", $ACCESS_WRITE ],
	  "labels new tapes in blank slots")
	  or diag(Dumper(\@results));
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot",
			tapelist => $tapelist,
			autolabel => { });
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 1,
    changer => $chg);
set_current_slot(6);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-2", $ACCESS_WRITE ],
	  "handles an invalid current slot by going to the next")
	  or diag(Dumper(\@results));
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot",
			tapelist => $tapelist);
# simulate "amlabel"
label_slot(1, "TEST-6", "X", "reuse", 1);
$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 2,
    changer => $chg);
set_current_slot(2);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-2", $ACCESS_WRITE ],
	  "scans for volumes, even with a newly labeled volume available")
	  or diag(Dumper(\@results));

# test skipping no-reuse tapes
reset_taperoot(5);
label_slot(1, "TEST-1", "20090424173001", "no-reuse", 1);
label_slot(2, "TEST-2", "20090424173002", "reuse", 1);
label_slot(3, "TEST-3", "20090424173003", "reuse", 1);
label_slot(4, "TEST-4", "20090424173004", "reuse", 1);
$taperscan->quit();

$chg = Amanda::Changer->new("chg-disk:$taperoot", tapelist => $tapelist);
set_current_slot(1);

$taperscan = Amanda::Taper::Scan->new(
    tapelist  => $tapelist,
    algorithm => "traditional",
    tapecycle => 2,
    changer => $chg);
@results = run_scan($taperscan);
is_deeply([ @results ],
	  [ undef, "TEST-2", $ACCESS_WRITE ],
	  "skips a no-reuse volume")
	  or diag(Dumper(\@results));
$taperscan->quit();

rmtree($taperoot);
unlink($tapelist);
