# Copyright (c) Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 311;
use File::Path;
use Data::Dumper;
use strict;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Changer;
use Installcheck::Mock qw( setup_mock_mtx $mock_mtx_path );
use Amanda::Device;
use Amanda::Debug;
use Amanda::Paths;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $chg_state_file = "$Installcheck::TMP/chg-robot-state";
unlink($chg_state_file) if -f $chg_state_file;

my $mtx_state_file = setup_mock_mtx (
	 num_slots => 5,
	 num_ie => 1,
	 barcodes => 1,
	 track_orig => 1,
	 num_drives => 2,
	 loaded_slots => {
	    1 => '11111',
	    2 => '22222',
	    3 => '33333',
	    4 => '44444',
	    # slot 5 is empty
         },
	 first_slot => 1,
	 first_drive => 0,
	 first_ie => 6,
       );

sub check_inventory {
    my ($chg, $barcodes, $next_step, $expected, $msg) = @_;

    $chg->inventory(inventory_cb => make_cb(sub {
	my ($err, $inv) = @_;
	die $err if $err;

	# strip barcodes from both $expected and $inv
	if (!$barcodes) {
	    for (@$expected, @$inv) {
		delete $_->{'barcode'};
	    }
	}

	is_deeply($inv, $expected, $msg)
	    or diag("Got:\n" . Dumper($inv));

	$next_step->();
    }));
}

##
# test the "interface" package

{
    my $testconf = Installcheck::Config->new();
    $testconf->add_changer('robo', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
	changerfile => "\"$chg_state_file\"",

	# point to the two vtape "drives" that mock/mtx will set up
	property => "\"tape-device\" \"0=null:drive0\"",

	# an point to the mock mtx
	property => "\"mtx\" \"$mock_mtx_path\"",
    ]);
    $testconf->write();

    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }

    my $chg = Amanda::Changer->new("robo");
    die "$chg" if $chg->isa("Amanda::Changer::Error");
    my $interface = $chg->{'interface'};
    my %subs;

    $subs{'start'} = sub {
	$interface->inquiry(sub {
	    my ($error, $info) = @_;

	    die $error if $error;

	    is_deeply($info, {
		'revision' => '0416',
		'product id' => 'SSL2000 Series',
		'attached changer' => 'No',
		'vendor id' => 'COMPAQ',
		'product type' => 'Medium Changer'
		}, "robot::Interface inquiry() info is correct");

	    $subs{'status1'}->();
	});
    };

    $subs{'status1'} = sub {
	$interface->status(sub {
	    my ($error, $status) = @_;

	    die $error if $error;

	    is_deeply($status, {
		drives => {
		    0 => undef,
		    1 => undef,
		},
		slots => {
		    1 => { 'barcode' => '11111', ie => 0 },
		    2 => { 'barcode' => '22222', ie => 0 },
		    3 => { 'barcode' => '33333', ie => 0 },
		    4 => { 'barcode' => '44444', ie => 0 },
		    5 => { empty => 1, ie => 0 },
		    6 => { empty => 1, ie => 1 },
		},
	    }, "robot::Interface status() output is correct (no drives loaded)");

	    $subs{'load0'}->();
	});
    };

    $subs{'load0'} = sub {
	$interface->load(2, 0, sub {
	    my ($error) = @_;

	    die $error if $error;

	    pass("load");
	    $subs{'status2'}->();
	});
    };

    $subs{'status2'} = sub {
	$interface->status(sub {
	    my ($error, $status) = @_;

	    die $error if $error;

	    is_deeply($status, {
		drives => {
		    0 => { barcode => '22222', 'orig_slot' => 2 },
		    1 => undef,
		},
		slots => {
		    1 => { 'barcode' => '11111', ie => 0 },
		    2 => { empty => 1, ie => 0 },
		    3 => { 'barcode' => '33333', ie => 0 },
		    4 => { 'barcode' => '44444', ie => 0 },
		    5 => { empty => 1, ie => 0 },
		    6 => { empty => 1, ie => 1 },
		},
	    }, "robot::Interface status() output is correct (one drive loaded)");

	    $subs{'load1'}->();
	});
    };

    $subs{'load1'} = sub {
	$interface->load(4, 1, sub {
	    my ($error) = @_;

	    die $error if $error;

	    pass("load");
	    $subs{'status3'}->();
	});
    };

    $subs{'status3'} = sub {
	$interface->status(sub {
	    my ($error, $status) = @_;

	    die $error if $error;

	    is_deeply($status, {
		drives => {
		    0 => { barcode => '22222', 'orig_slot' => 2 },
		    1 => { barcode => '44444', 'orig_slot' => 4 },
		},
		slots => {
		    1 => { 'barcode' => '11111', ie => 0 },
		    2 => { empty => 1, ie => 0 },
		    3 => { 'barcode' => '33333', ie => 0 },
		    4 => { empty => 1, ie => 0 },
		    5 => { empty => 1, ie => 0 },
		    6 => { empty => 1, ie => 1 },
		},
	    }, "robot::Interface status() output is correct (two drives loaded)");

	    $subs{'transfer'}->();
	});
    };

    $subs{'transfer'} = sub {
	$interface->transfer(3, 6, sub {
	    my ($error) = @_;

	    die $error if $error;

	    pass("transfer");
	    $subs{'status4'}->();
	});
    };

    $subs{'status4'} = sub {
	$interface->status(sub {
	    my ($error, $status) = @_;

	    die $error if $error;

	    is_deeply($status, {
		drives => {
		    0 => { barcode => '22222', 'orig_slot' => 2 },
		    1 => { barcode => '44444', 'orig_slot' => 4 },
		},
		slots => {
		    1 => { 'barcode' => '11111', ie => 0 },
		    2 => { empty => 1, ie => 0 },
		    3 => { empty => 1, ie => 0 },
		    4 => { empty => 1, ie => 0 },
		    5 => { empty => 1, ie => 0 },
		    6 => { 'barcode' => '33333', ie => 1 },
		},
	    }, "robot::Interface status() output is correct after transfer");

	    $subs{'quit'}->();
	});
    };

    $subs{'quit'} = sub {
	Amanda::MainLoop::quit();
    };

    Amanda::MainLoop::call_later($subs{'start'});
    Amanda::MainLoop::run();
}

{
    my $testconf = Installcheck::Config->new();
    $testconf->add_changer('no-tape-device', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
	changerfile => "\"$chg_state_file\"",
    ]);
    $testconf->add_changer('bad-property', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
	changerfile => "\"$chg_state_file\"",
	property => "\"fast-search\" \"maybe\"",
	property => "\"tape-device\" \"0=null:foo\"",
    ]);
    $testconf->add_changer('no-fast-search', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
	changerfile => "\"$chg_state_file\"",
	property => "\"use-slots\" \"1-3,9\"",
	property => "append \"use-slots\" \"8,5-6\"",
	property => "\"fast-search\" \"no\"",
	property => "\"tape-device\" \"0=null:foo\"",
    ]);
    $testconf->add_changer('delays', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
        # no changerfile property
	property => "\"tape-device\" \"0=null:foo\"",
	property => "\"status-interval\" \"1m\"",
	property => "\"eject-delay\" \"1s\"",
	property => "\"unload-delay\" \"2M\"",
	property => "\"load-poll\" \"2s POLl 3s uNtil 1m\"",
    ]);
    $testconf->write();

    config_uninit();
    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }

    # test the changer constructor and properties
    my $err = Amanda::Changer->new("chg-robot:does/not/exist");
    chg_err_like($err,
	{ message => "'does/not/exist' not found",
	  type => 'fatal' },
	"check for device existence works");

    $err = Amanda::Changer->new("no-tape-device");
    chg_err_like($err,
	{ message => "no 'tape-device' property specified",
	  type => 'fatal' },
	"tape-device property is required");

    $err = Amanda::Changer->new("bad-property");
    chg_err_like($err,
	{ message => "invalid 'fast-search' value",
	  type => 'fatal' },
	"invalid boolean value handled correctly");

    my $chg = Amanda::Changer->new("delays");
    die "$chg" if $chg->isa("Amanda::Changer::Error");
    is($chg->{'status_interval'}, 60, "status-interval parsed");
    is($chg->{'eject_delay'}, 1, "eject-delay parsed");
    is($chg->{'unload_delay'}, 120, "unload-delay parsed");
    is_deeply($chg->{'load_poll'}, [ 2, 3, 60 ], "load-poll parsed");

    # check out the statefile filename generation
    my $dashed_mtx_state_file = $mtx_state_file;
    $dashed_mtx_state_file =~ tr/a-zA-Z0-9/-/cs;
    $dashed_mtx_state_file =~ s/^-*//;
    is($chg->{'statefile'}, "$libexecdir/lib/amanda/chg-robot-$dashed_mtx_state_file",
        "statefile calculated correctly");

    # test no-fast-search
    $chg = Amanda::Changer->new("no-fast-search");
    die "$chg" if $chg->isa("Amanda::Changer::Error");
    $chg->info(
	    info => ['fast_search'],
	    info_cb => make_cb(info_cb => sub {
	my ($err, %info) = @_;
	ok(!$info{'fast_search'}, "fast-search property works");
	Amanda::MainLoop::quit();
    }));
    Amanda::MainLoop::run();

    # test use-slots
    my @allowed = map { $chg->_is_slot_allowed($_) } (0 .. 10);
    is_deeply([ @allowed ], [ 0, 1, 1, 1, 0, 1, 1, 0, 1, 1, 0 ],
	"_is_slot_allowed parses multiple properties and behaves as expected");
}

##
# Test the real deal

# These tests are run over a number of different mtx configurations, to ensure
# that the behavior is identical regardless of the changer/mtx characteristics
for my $mtx_config (
    { barcodes => 1, track_orig => 1, },
    { barcodes => 0, track_orig => 1, },
    { barcodes => 1, track_orig => -1, },
    { barcodes => 0, track_orig => 0, },
    { barcodes => -1, track_orig => 0, },
) {
    my %subs;
    my $chg;
    my ($res1, $res2);

    my $pfx = "BC=$mtx_config->{barcodes}; TORIG=$mtx_config->{track_orig}";

    # clean up
    unlink($chg_state_file) if -f $chg_state_file;
    unlink($mtx_state_file) if -f $mtx_state_file;
    %Amanda::Changer::changers_by_uri_cc = ();

    # set up some vtapes
    my $vtape_root = "$Installcheck::TMP/chg-robot-vtapes";
    rmtree($vtape_root);
    mkpath($vtape_root);

    my @ignore_barcodes = ( property => "\"ignore-barcodes\" \"y\"")
	if ($mtx_config->{'barcodes'} == -1);

    my $testconf = Installcheck::Config->new();
    $testconf->add_changer('robo', [
	tpchanger => "\"chg-robot:$mtx_state_file\"",
	changerfile => "\"$chg_state_file\"",

	# point to the two vtape "drives" that mock/mtx will set up
	property => "\"tape-device\" \"0=file:$vtape_root/drive0\"",
	property => "append \"tape-device\" \"1=file:$vtape_root/drive1\"",
	property => "\"use-slots\" \"1-5\"",
	property => "\"mtx\" \"$mock_mtx_path\"",
	@ignore_barcodes,
    ]);
    $testconf->write();

    config_uninit();
    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }

    # reset the mock mtx
    my $mtx_state_file = setup_mock_mtx (
	     %$mtx_config,
	     num_slots => 6,
	     num_ie => 1,
	     num_drives => 2,
	     loaded_slots => {
		1 => '11111',
		2 => '22222',
		3 => '33333',
		4 => '44444',
		# slot 5 is empty
		6 => '66666', # slot 6 is full, but not in use-slots
	     },
	     first_slot => 1,
	     first_drive => 0,
	     first_ie => 6,
	     vtape_root => $vtape_root,
	   );


    $subs{'start'} = sub {
	$chg = Amanda::Changer->new("robo");
	ok(!$chg->isa("Amanda::Changer::Error"),
	    "$pfx: Create working chg-robot instance")
	    or die("no sense going on");

	$chg->info(info => [qw(vendor_string num_slots fast_search)], info_cb => $subs{'info_cb'});
    };

    $subs{'info_cb'} = sub {
	my ($err, %info) = @_;
	die $err if $err;

	is_deeply({ %info }, {
	    num_slots => 5,
	    fast_search => 1,
	    vendor_string => "COMPAQ SSL2000 Series",
	}, "$pfx: info keys num_slots, fast_search, vendor_string are correct");

	$subs{'inventory1'}->();
    };

    $subs{'inventory1'} = sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $subs{'load_slot_1'}, [
	    { slot => 1, barcode => '11111', label => undef },
	    { slot => 2, barcode => '22222', label => undef },
	    { slot => 3, barcode => '33333', label => undef },
	    { slot => 4, barcode => '44444', label => undef },
	    { slot => 5, empty => 1, label => undef },
	], "$pfx: inventory is correct on start-up");
    };

    $subs{'load_slot_1'} = sub {
	$chg->load(slot => 1, res_cb => $subs{'loaded_slot_1'});
    };

    $subs{'loaded_slot_1'} = sub {
	(my $err, $res1) = @_;
	die $err if $err;

	is($res1->{'device'}->device_name, "file:$vtape_root/drive0",
	    "$pfx: first load returns drive-0 device");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{1}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
	    }, {
		loaded_in => 0,
		orig_slot => 1,
	    }, "$pfx: slot 1 'loaded_in' and drive 0 'orig_slot' are correct");

	$subs{'load_slot_2'}->();
    };

    $subs{'load_slot_2'} = sub {
	$chg->load(slot => 2, res_cb => $subs{'loaded_slot_2'});
    };

    $subs{'loaded_slot_2'} = sub {
	(my $err, $res2) = @_;
	die $err if $err;

	is($res2->{'device'}->device_name, "file:$vtape_root/drive1",
	    "$pfx: second load returns drive-1 device");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{1}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
	    }, {
		loaded_in => 0,
		orig_slot => 1,
	    }, "$pfx: slot 1 'loaded_in' and drive 0 'orig_slot' are still correct");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{2}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
	    }, {
		loaded_in => 1,
		orig_slot => 2,
	    }, "$pfx: slot 2 'loaded_in' and drive 1 'orig_slot' are correct");

	$subs{'check_loads'}->();
    };

    $subs{'check_loads'} = sub {
	# peek into the interface to check that things are loaded correctly
	$chg->{'interface'}->status(sub {
	    my ($error, $status) = @_;

	    die $error if $error;

	    # only perform these checks when barcodes are enabled
	    if ($mtx_config->{'barcodes'} > 0) {
		is_deeply($status->{'drives'}, {
		    0 => { barcode => '11111', 'orig_slot' => 1 },
		    1 => { barcode => '22222', 'orig_slot' => 2 },
		}, "$pfx: double-check: loading drives with the changer gets the right drives loaded");
	    }

	    $subs{'inventory2'}->();
	});
    };

    $subs{'inventory2'} = sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $subs{'load_slot_3'}, [
	    { slot => 1, barcode => '11111', reserved => 1,
	      label => '', loaded_in => 0 },
	    { slot => 2, barcode => '22222', reserved => 1,
	      label => '', loaded_in => 1 },
	    { slot => 3, barcode => '33333', label => undef },
	    { slot => 4, barcode => '44444', label => undef },
	    { slot => 5, empty => 1, label => undef },
	], "$pfx: inventory is updated when slots are loaded");
    };

    $subs{'load_slot_3'} = sub {
	$chg->load(slot => 3, res_cb => $subs{'loaded_slot_3'});
    };

    $subs{'loaded_slot_3'} = sub {
	my ($err, $no_res) = @_;

	chg_err_like($err,
	    { message => "no drives available",
	      reason => 'inuse',
	      type => 'failed' },
	    "$pfx: trying to load a third slot fails with 'no drives available'");

	$subs{'label_tape_1'}->();
    };

    $subs{'label_tape_1'} = sub {
	$res1->{'device'}->start($Amanda::Device::ACCESS_WRITE, "TAPE-1", undef);
	$res1->{'device'}->finish();

	$res1->set_label(label => "TAPE-1", finished_cb => $subs{'label_tape_2'});
    };

    $subs{'label_tape_2'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: labeled TAPE-1 in drive 0");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{1}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{1}->{'label'},
		drive_label => $chg->{'__last_state'}->{'drives'}->{0}->{'label'},
	    }, {
		loaded_in => 0,
		orig_slot => 1,
		slot_label => 'TAPE-1',
		drive_label => 'TAPE-1',
	    }, "$pfx: label is correctly reflected in changer state");

	is_deeply({
		slot_2_loaded_in => $chg->{'__last_state'}->{'slots'}->{2}->{'loaded_in'},
		slot_1_loaded_in => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
	    }, {
		slot_2_loaded_in => 1,
		slot_1_loaded_in => 2,
	    },
	    "$pfx: slot 2 'loaded_in' and drive 1 'orig_slot' are correct");

	$res2->{'device'}->start($Amanda::Device::ACCESS_WRITE, "TAPE-2", undef);
	$res2->{'device'}->finish();

	$res2->set_label(label => "TAPE-2", finished_cb => $subs{'release1'});
    };

    $subs{'release1'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: labeled TAPE-2 in drive 1");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{2}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{2}->{'label'},
		drive_label => $chg->{'__last_state'}->{'drives'}->{1}->{'label'},
	    }, {
		loaded_in => 1,
		orig_slot => 2,
		slot_label => 'TAPE-2',
		drive_label => 'TAPE-2',
	    }, "$pfx: label is correctly reflected in changer state");

	$res2->release(finished_cb => $subs{'inventory3'});
    };

    $subs{'inventory3'} = sub {
	my ($err) = @_;
	die "$err" if $err;
	pass("$pfx: slot 2/drive 1 released");

	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $subs{'check_state_after_release1'}, [
	    { slot => 1, barcode => '11111', label => 'TAPE-1',
	      loaded_in => 0, reserved => 1 },
	    { slot => 2, barcode => '22222', label => 'TAPE-2',
	      loaded_in => 1 },
	    { slot => 3, barcode => '33333', label => undef },
	    { slot => 4, barcode => '44444', label => undef },
	    { slot => 5, empty => 1, label => undef },
	], "$pfx: inventory is still up to date");
    };

    $subs{'check_state_after_release1'} = sub {
	is($chg->{'__last_state'}->{'drives'}->{1}->{'res_info'}, undef,
		"$pfx: drive is not reserved");
	is($chg->{'__last_state'}->{'drives'}->{1}->{'label'}, 'TAPE-2',
		"$pfx: tape is still in drive");

	$subs{'load_current_1'}->();
    };

    $subs{'load_current_1'} = sub {
	$chg->load(relative_slot => "current", res_cb => $subs{'loaded_current_1'});
    };

    $subs{'loaded_current_1'} = sub {
	my ($err, $res) = @_;

	chg_err_like($err,
	    { message => "the requested volume is in use (drive 0)",
	      reason => 'inuse',
	      type => 'failed' },
	    "$pfx: loading 'current' when set_current hasn't been used yet gets slot 1 (which is in use)");

	$subs{'load_slot_4'}->();
    };

    # this should unload what's in drive 1 and load the empty volume in slot 4
    $subs{'load_slot_4'} = sub {
	$chg->load(slot => 4, set_current => 1, res_cb => $subs{'loaded_slot_4'});
    };

    $subs{'loaded_slot_4'} = sub {
	(my $err, $res2) = @_;
	die "$err" if $err;

	is($res2->{'device'}->device_name, "file:$vtape_root/drive1",
	    "$pfx: loaded slot 4 into drive 1 (and set current to slot 4)");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{2}->{'loaded_in'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{2}->{'label'},
	    }, {
		loaded_in => undef,
		slot_label => 'TAPE-2',
	    }, "$pfx: slot 2 (which was just unloaded) still tracked correctly");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{1}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
	    }, {
		loaded_in => 0,
		orig_slot => 1,
	    }, "$pfx: slot 1 'loaded_in' and drive 0 'orig_slot' are *still* correct");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{4}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
	    }, {
		loaded_in => 1,
		orig_slot => 4,
	    }, "$pfx: slot 4 'loaded_in' and drive 1 'orig_slot' are correct");

	$subs{'label_tape_4'}->();
    };

    $subs{'label_tape_4'} = sub {
	$res2->{'device'}->start($Amanda::Device::ACCESS_WRITE, "TAPE-4", undef);
	$res2->{'device'}->finish();

	$res2->set_label(label => "TAPE-4", finished_cb => $subs{'inventory4'});
    };

    $subs{'inventory4'} = sub {
	my ($err) = @_;
	die "$err" if $err;
	pass("$pfx: labeled TAPE-4 in drive 1");

	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $subs{'release2'}, [
	    { slot => 1, barcode => '11111', label => 'TAPE-1',
	      reserved => 1, loaded_in => 0 },
	    { slot => 2, barcode => '22222', label => 'TAPE-2' },
	    { slot => 3, barcode => '33333', label => undef },
	    { slot => 4, barcode => '44444', label => 'TAPE-4',
	      reserved => 1, loaded_in => 1 },
	    { slot => 5, empty => 1, label => undef },
	], "$pfx: inventory is up to date after more labelings");
    };

    $subs{'release2'} = sub {
	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{4}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{4}->{'label'},
		drive_label => $chg->{'__last_state'}->{'drives'}->{1}->{'label'},
	    }, {
		loaded_in => 1,
		orig_slot => 4,
		slot_label => 'TAPE-4',
		drive_label => 'TAPE-4',
	    }, "$pfx: label is correctly reflected in changer state");

	$res1->release(finished_cb => $subs{'release2_done'});
    };

    $subs{'release2_done'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 1/drive 0 released");

	is($chg->{'__last_state'}->{'drives'}->{0}->{'label'}, 'TAPE-1',
		"$pfx: tape is still in drive");

	$subs{'release3'}->();
    };

    $subs{'release3'} = sub {
	my ($err) = @_;
	die $err if $err;

	$res2->release(finished_cb => $subs{'release3_done'});
    };

    $subs{'release3_done'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 4/drive 0 released");

	is($chg->{'__last_state'}->{'drives'}->{1}->{'label'},
		'TAPE-4', "$pfx: tape is still in drive");

	$subs{'load_preloaded_by_slot'}->();
    };

    # try loading a slot that's already in a drive
    $subs{'load_preloaded_by_slot'} = sub {
	$chg->load(slot => 1, res_cb => $subs{'loaded_preloaded_by_slot'});
    };

    $subs{'loaded_preloaded_by_slot'} = sub {
	(my $err, $res1) = @_;
	die $err if $err;

	is($res1->{'device'}->device_name, "file:$vtape_root/drive0",
	    "$pfx: loading a tape (by slot) that's already in a drive returns that drive");

	$res1->release(finished_cb => $subs{'load_preloaded_by_label'});
    };

    # try again, this time by label
    $subs{'load_preloaded_by_label'} = sub {
	pass("$pfx: slot 1/drive 0 released");

	$chg->load(label => 'TAPE-4', res_cb => $subs{'loaded_preloaded_by_label'});
    };

    $subs{'loaded_preloaded_by_label'} = sub {
	(my $err, $res1) = @_;
	die $err if $err;

	is($res1->{'device'}->device_name, "file:$vtape_root/drive1",
	    "$pfx: loading a tape (by label) that's already in a drive returns that drive");

	$res1->release(finished_cb => $subs{'load_unloaded_by_label'});
    };

    # test out searching by label
    $subs{'load_unloaded_by_label'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 4/drive 1 released");

	$chg->load(label => 'TAPE-2', res_cb => $subs{'loaded_unloaded_by_label'});
    };

    $subs{'loaded_unloaded_by_label'} = sub {
	(my $err, $res1) = @_;
	die $err if $err;

	$res1->{'device'}->read_label();
	is($res1->{'device'}->volume_label, "TAPE-2",
	    "$pfx: loading a tape (by label) that's *not* already in a drive returns " .
	    "the correct device");

	$subs{'release4'}->();
    };

    $subs{'release4'} = sub {
	$res1->release(finished_cb => $subs{'release4_done'}, eject => 1);
    };

    $subs{'release4_done'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 2/drive 0 released");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{2}->{'loaded_in'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{2}->{'label'},
		drive_label => $chg->{'__last_state'}->{'drives'}->{0}->{'label'},
	    }, {
		loaded_in => undef,
		slot_label => 'TAPE-2',
		drive_label => undef,
	    }, "$pfx: and TAPE-2 ejected");

	$subs{'load_current_2'}->();
    };

    $subs{'load_current_2'} = sub {
	$chg->load(relative_slot => "current", res_cb => $subs{'loaded_current_2'});
    };

    $subs{'loaded_current_2'} = sub {
	(my $err, $res1) = @_;
	die $err if $err;

	$res1->{'device'}->read_label();
	is($res1->{'device'}->volume_label, "TAPE-4",
	    "$pfx: loading 'current' returns the correct device");

	$subs{'release5'}->();
    };

    $subs{'release5'} = sub {
	$res1->release(finished_cb => $subs{'load_slot_next'});
    };

    $subs{'load_slot_next'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 4/drive 1 released");

	$chg->load(relative_slot => "next", res_cb => $subs{'loaded_slot_next'});
    };

    $subs{'loaded_slot_next'} = sub {
	(my $err, $res1) = @_;
	die $err if $err;

	$res1->{'device'}->read_label();
	is($res1->{'device'}->volume_label, "TAPE-1",
	    "$pfx: loading 'next' returns the correct slot, skipping slot 5 and " .
		    "looping around to the beginning");

	$subs{'load_res1_next_slot'}->();
    };

    $subs{'load_res1_next_slot'} = sub {
	$chg->load(relative_slot => "next", slot => $res1->{'this_slot'},
		   res_cb => $subs{'loaded_res1_next_slot'});
    };

    $subs{'loaded_res1_next_slot'} = sub {
	(my $err, $res2) = @_;
	die $err if $err;

	$res2->{'device'}->read_label();
	is($res2->{'device'}->volume_label, "TAPE-2",
	    "$pfx: \$res->{this_slot} + 'next' returns the correct slot, too");
        if ($mtx_config->{'barcodes'} == 1) {
            is($res2->{'barcode'}, '22222',
                "$pfx: result has a barcode");
        }

	$subs{'release6'}->();
    };

    $subs{'release6'} = sub {
	$res1->release(finished_cb => $subs{'release7'});
    };

    $subs{'release7'} = sub {
	my ($err) = @_;
	die "$err" if $err;

	pass("$pfx: slot 1 released");

	$res2->release(finished_cb => $subs{'load_disallowed_slot'});
    };

    $subs{'load_disallowed_slot'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 2 released");

	$chg->load(slot => 6, res_cb => $subs{'loaded_disallowed_slot'});
    };

    $subs{'loaded_disallowed_slot'} = sub {
	(my $err, $res1) = @_;

	chg_err_like($err,
	    { message => "slot 6 not in use-slots (1-5)",
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: loading a disallowed slot fails propertly");

	$subs{'inventory5'}->();
    };

    $subs{'inventory5'} = sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $subs{'try_update'}, [
	    { slot => 1, barcode => '11111', label => 'TAPE-1', loaded_in => 1 },
	    { slot => 2, barcode => '22222', label => 'TAPE-2', loaded_in => 0 },
	    { slot => 3, barcode => '33333', label => undef },
	    { slot => 4, barcode => '44444', label => 'TAPE-4' },
	    { slot => 5, empty => 1, label => undef },
	], "$pfx: inventory still accurate");
    };

    $subs{'try_update'} = sub {
	# first, add a label in slot 3, which hasn't been written
	# to yet
	my $dev = Amanda::Device->new("file:$vtape_root/slot3");
	die $dev->error_or_status()
	    unless $dev->status == 0;
	die "error writing label"
	    unless $dev->start($Amanda::Device::ACCESS_WRITE, "TAPE-3", undef);
	$dev->finish();

	# now update that slot
	$chg->update(changed => "2-4", finished_cb => $subs{'update_finished'});
    };

    $subs{'update_finished'} = sub {
	my ($err) = @_;
	die "$err" if $err;

	# verify that slots 2, 3, and 4 have correct info now
	is_deeply({
		slot_2 => $chg->{'__last_state'}->{'slots'}->{2}->{'label'},
		slot_3 => $chg->{'__last_state'}->{'slots'}->{3}->{'label'},
		slot_4 => $chg->{'__last_state'}->{'slots'}->{4}->{'label'},
	    }, {
		slot_2 => 'TAPE-2',
		slot_3 => 'TAPE-3',
		slot_4 => 'TAPE-4',
	    }, "$pfx: update correctly finds new label in slot 3");

	# and check barcodes otherwise
	if ($mtx_config->{'barcodes'} > 0) {
	    is_deeply({
		    barcode_2 => $chg->{'__last_state'}->{'bc2lb'}->{'22222'},
		    barcode_3 => $chg->{'__last_state'}->{'bc2lb'}->{'33333'},
		    barcode_4 => $chg->{'__last_state'}->{'bc2lb'}->{'44444'},
		}, {
		    barcode_2 => 'TAPE-2',
		    barcode_3 => 'TAPE-3',
		    barcode_4 => 'TAPE-4',
		}, "$pfx: bc2lb is correct, too");
	}

	$subs{'try_update2'}->();
    };

    $subs{'try_update2'} = sub {
	# lie about slot 2
	$chg->update(changed => "2=SURPRISE!", finished_cb => $subs{'update_finished2'});
    };

    $subs{'update_finished2'} = sub {
	my ($err) = @_;
	die "$err" if $err;

	# verify the new slot info
	is_deeply({
		slot_2 => $chg->{'__last_state'}->{'slots'}->{2}->{'label'},
	    }, {
		slot_2 => 'SURPRISE!',
	    }, "$pfx: assignment-style update correctly sets new label in slot 2");

	# and check barcodes otherwise
	if ($mtx_config->{'barcodes'} > 0) {
	    is_deeply({
		    barcode_2 => $chg->{'__last_state'}->{'bc2lb'}->{'22222'},
		}, {
		    barcode_2 => 'SURPRISE!',
		}, "$pfx: bc2lb is correct, too");
	}

	$subs{'try_update3'}->();
    };

    $subs{'try_update3'} = sub {
	# lie about slot 2
	$chg->update(changed => "5=NO!", finished_cb => $subs{'update_finished3'});
    };

    $subs{'update_finished3'} = sub {
	my ($err) = @_;
	chg_err_like($err,
	    { message => "slot 5 is empty",
	      reason => 'unknown',
	      type => 'failed' },
	    "$pfx: assignment-style update of an empty slot gives error");

	$subs{'inventory6'}->();
    };

    $subs{'inventory6'} = sub {
	# note that the loading behavior of update() is not required, so the loaded_in
	# keys here may change if update() gets smarter
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $subs{'move1'}, [
	    { slot => 1, barcode => '11111', label => 'TAPE-1' },
	    { slot => 2, barcode => '22222', label => 'SURPRISE!' },
	    { slot => 3, barcode => '33333', label => 'TAPE-3', loaded_in => 1 },
	    { slot => 4, barcode => '44444', label => 'TAPE-4', loaded_in => 0 },
	    { slot => 5, empty => 1, label => undef },
	], "$pfx: inventory reflects updates");
    };

    $subs{'move1'} = sub {
	# move to a full slot
	$chg->move(from_slot => 2, to_slot => 1, finished_cb => $subs{'moved1'});
    };

    $subs{'moved1'} = sub {
	my ($err) = @_;

	chg_err_like($err,
	    { message => "slot 1 is not empty",
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: moving to a full slot is an error");

	$subs{'move2'}->();
    };

    $subs{'move2'} = sub {
	# move to a full slot that's loaded (so there's not *actually* a tape
	# in the slot)
	$chg->move(from_slot => 2, to_slot => 3, finished_cb => $subs{'moved2'});
    };

    $subs{'moved2'} = sub {
	my ($err) = @_;

	chg_err_like($err,
	    { message => "slot 3 is not empty",
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: moving to a full slot is an error even if that slot is loaded");

	$subs{'move3'}->();
    };

    $subs{'move3'} = sub {
	# move from an empty slot
	$chg->move(from_slot => 5, to_slot => 3, finished_cb => $subs{'moved3'});
    };

    $subs{'moved3'} = sub {
	my ($err) = @_;

	chg_err_like($err,
	    { message => "slot 5 is empty", # note that this depends on the order of checks..
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: moving from an empty slot is an error");

	$subs{'move4'}->();
    };

    $subs{'move4'} = sub {
	# move from a loaded slot to an empty slot
	$chg->move(from_slot => 4, to_slot => 5, finished_cb => $subs{'moved4'});
    };

    $subs{'moved4'} = sub {
	my ($err) = @_;

	chg_err_like($err,
	    { message => "slot 4 is currently loaded",
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: moving from a loaded slot is an error");

	$subs{'move5'}->();
    };

    $subs{'move5'} = sub {
	$chg->move(from_slot => 2, to_slot => 5, finished_cb => $subs{'inventory7'});
    };


    $subs{'inventory7'} = sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: move succeeds");

	# note that the loading behavior of update() is not required, so the loaded_in
	# keys here may change if update() gets smarter
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $subs{'start_scan'}, [
	    { slot => 1, barcode => '11111', label => 'TAPE-1' },
	    { slot => 2, empty => 1, label => undef },
	    { slot => 3, barcode => '33333', label => 'TAPE-3', loaded_in => 1 },
	    { slot => 4, barcode => '44444', label => 'TAPE-4', loaded_in => 0 },
	    { slot => 5, barcode => '22222', label => 'SURPRISE!' },
	], "$pfx: inventory reflects the move");
    };

    # test a scan, using except_slots
    my %except_slots;

    $subs{'start_scan'} = make_cb(start_scan => sub {
	$chg->load(relative_slot => "current", except_slots => { %except_slots },
		   res_cb => $subs{'loaded_for_scan'});
    });

    $subs{'loaded_for_scan'} = make_cb(loaded_for_scan => sub {
        (my $err, $res1) = @_;
	my $slot;
	if ($err) {
	    if ($err->notfound) {
		return $subs{'scan_done'}->();
	    } elsif ($err->inuse and defined $err->{'slot'}) {
		$slot = $err->{'slot'};
	    } else {
		die $err;
	    }
	} else {
	    $slot = $res1->{'this_slot'};
	}

	$except_slots{$slot} = 1;

	$res1->release(finished_cb => $subs{'released_for_scan'});
    });

    $subs{'released_for_scan'} = make_cb(released_for_scan => sub {
	my ($err) = @_;
	die $err if $err;

        $chg->load(relative_slot => 'next', slot => $res1->{'this_slot'},
		   except_slots => { %except_slots },
		   res_cb => $subs{'loaded_for_scan'});
    });

    $subs{'scan_done'} = make_cb(scan_done => sub {
	is_deeply({ %except_slots }, { 4=>1, 5=>1, 1=>1, 3=>1 },
		"$pfx: scanning with except_slots works");
	$subs{'quit'}->();
    });

    $subs{'quit'} = sub {
	Amanda::MainLoop::quit();
    };

    Amanda::MainLoop::call_later($subs{'start'});
    Amanda::MainLoop::run();

    unlink($chg_state_file) if -f $chg_state_file;
    unlink($mtx_state_file) if -f $mtx_state_file;
    rmtree($vtape_root);
}
