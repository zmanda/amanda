# Copyright (c) 2009-2012 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 324;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Changer;
use Installcheck::Mock qw( setup_mock_mtx $mock_mtx_path );
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::Paths;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();
Installcheck::log_test_output();

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

sub test_interface {
    my ($finished_cb) = @_;
    my ($interface, $chg);

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() };

    step start => sub {
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

	$chg = Amanda::Changer->new("robo");
	die "$chg" if $chg->isa("Amanda::Changer::Error");
	is($chg->have_inventory(), '1', "changer have inventory");
	$interface = $chg->{'interface'};

	$interface->inquiry($steps->{'inquiry_cb'});
    };

    step inquiry_cb => sub {
	my ($error, $info) = @_;

	die $error if $error;

	is_deeply($info, {
	    'revision' => '0416',
	    'product id' => 'SSL2000 Series',
	    'attached changer' => 'No',
	    'vendor id' => 'COMPAQ',
	    'product type' => 'Medium Changer'
	    }, "robot::Interface inquiry() info is correct");

	$steps->{'status1'}->();
    };

    step status1 => sub {
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
	    $steps->{'load0'}->();
	});
    };

    step load0 => sub {
	$interface->load(2, 0, sub {
	    my ($error) = @_;

	    die $error if $error;

	    pass("load");
	    $steps->{'status2'}->();
	});
    };

    step status2 => sub {
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

	    $steps->{'load1'}->();
	});
    };

    step load1 => sub {
	$interface->load(4, 1, sub {
	    my ($error) = @_;

	    die $error if $error;

	    pass("load");
	    $steps->{'status3'}->();
	});
    };

    step status3 => sub {
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

	    $steps->{'transfer'}->();
	});
    };

    step transfer => sub {
	$interface->transfer(3, 6, sub {
	    my ($error) = @_;

	    die $error if $error;

	    pass("transfer");
	    $steps->{'status4'}->();
	});
    };

    step status4 => sub {
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

	    $finished_cb->();
	});
    };
}
test_interface(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

{
    my $testconf = Installcheck::Config->new();
    $testconf->add_changer('bum-scsi-dev', [
	tpchanger => "\"chg-robot:does/not/exist\"",
	property => "\"tape-device\" \"0=null:foo\"",
	changerfile => "\"$chg_state_file\"",
    ]);
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
    my $err = Amanda::Changer->new("bum-scsi-dev");
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
    is($chg->have_inventory(), '1', "changer have inventory");
    is($chg->{'status_interval'}, 60, "status-interval parsed");
    is($chg->{'eject_delay'}, 1, "eject-delay parsed");
    is($chg->{'unload_delay'}, 120, "unload-delay parsed");
    is_deeply($chg->{'load_poll'}, [ 2, 3, 60 ], "load-poll parsed");

    # check out the statefile filename generation
    my $dashed_mtx_state_file = $mtx_state_file;
    $dashed_mtx_state_file =~ tr/a-zA-Z0-9/-/cs;
    $dashed_mtx_state_file =~ s/^-*//;
    is($chg->{'statefile'}, "$localstatedir/amanda/chg-robot-$dashed_mtx_state_file",
        "statefile calculated correctly");
    $chg->quit();

    # test no-fast-search
    $chg = Amanda::Changer->new("no-fast-search");
    die "$chg" if $chg->isa("Amanda::Changer::Error");
    is($chg->have_inventory(), '1', "changer have inventory");
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
    $chg->quit();
}

##
# Test the real deal

sub test_changer {
    my ($mtx_config, $finished_cb) = @_;
    my $chg;
    my ($res1, $res2, $mtx_state_file);
    my $pfx = "BC=$mtx_config->{barcodes}; TORIG=$mtx_config->{track_orig}";
    my $vtape_root = "$Installcheck::TMP/chg-robot-vtapes";

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() };

    step setup => sub {
	# clean up
	unlink($chg_state_file) if -f $chg_state_file;

	# set up some vtapes
	rmtree($vtape_root);
	mkpath($vtape_root);

	# reset the mock mtx
	$mtx_state_file = setup_mock_mtx (
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

	$steps->{'start'}->();
    };

    step start => sub {
	$chg = Amanda::Changer->new("robo");
	ok(!$chg->isa("Amanda::Changer::Error"),
	    "$pfx: Create working chg-robot instance")
	    or die("no sense going on: $chg");

	$chg->info(info => [qw(vendor_string num_slots fast_search)], info_cb => $steps->{'info_cb'});
    };

    step info_cb => sub {
	my ($err, %info) = @_;
	die $err if $err;

	is_deeply({ %info }, {
	    num_slots => 5,
	    fast_search => 1,
	    vendor_string => "COMPAQ SSL2000 Series",
	}, "$pfx: info keys num_slots, fast_search, vendor_string are correct");

	$steps->{'inventory1'}->();
    };

    step inventory1 => sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'load_slot_1'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111', current => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 2, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 5, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory is correct on start-up");
    };

    step load_slot_1 => sub {
	$chg->load(slot => 1, res_cb => $steps->{'loaded_slot_1'});
    };

    step loaded_slot_1 => sub {
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

	$steps->{'load_slot_2'}->();
    };

    step load_slot_2 => sub {
	$chg->load(slot => 2, res_cb => $steps->{'loaded_slot_2'});
    };

    step loaded_slot_2 => sub {
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

	$steps->{'check_loads'}->();
    };

    step check_loads => sub {
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

	    $steps->{'inventory2'}->();
	});
    };

    step inventory2 => sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'load_slot_3'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111', reserved => 1, loaded_in => 0, current => 1,
	      device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	      device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 2, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222', reserved => 1, loaded_in => 1,
	      device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	      device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 5, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory is updated when slots are loaded");
    };

    step load_slot_3 => sub {
	$chg->load(slot => 3, res_cb => $steps->{'loaded_slot_3'});
    };

    step loaded_slot_3 => sub {
	my ($err, $no_res) = @_;

	chg_err_like($err,
	    { message => "no drives available",
	      reason => 'driveinuse',
	      type => 'failed' },
	    "$pfx: trying to load a third slot fails with 'no drives available'");

	$steps->{'label_tape_1'}->();
    };

    step label_tape_1 => sub {
	$res1->{'device'}->start($Amanda::Device::ACCESS_WRITE, "TAPE-1", undef);
	$res1->{'device'}->finish();

	$res1->set_label(label => "TAPE-1", finished_cb => $steps->{'label_tape_2'});
    };

    step label_tape_2 => sub {
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

	$res2->set_label(label => "TAPE-2", finished_cb => $steps->{'release1'});
    };

    step release1 => sub {
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

	$res2->release(finished_cb => $steps->{'inventory3'});
    };

    step inventory3 => sub {
	my ($err) = @_;
	die "$err" if $err;
	pass("$pfx: slot 2/drive 1 released");

	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'check_state_after_release1'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111', reserved => 1, loaded_in => 0, current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
	    { slot => 2, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222', loaded_in => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-2' },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 5, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory is still up to date");
    };

    step check_state_after_release1 => sub {
	is($chg->{'__last_state'}->{'drives'}->{1}->{'res_info'}, undef,
		"$pfx: drive is not reserved");
	is($chg->{'__last_state'}->{'drives'}->{1}->{'label'}, 'TAPE-2',
		"$pfx: tape is still in drive");

	$steps->{'load_current_1'}->();
    };

    step load_current_1 => sub {
	$chg->load(relative_slot => "current", res_cb => $steps->{'loaded_current_1'});
    };

    step loaded_current_1 => sub {
	my ($err, $res) = @_;

	chg_err_like($err,
	    { message => "the requested volume is in use (drive 0)",
	      reason => 'volinuse',
	      type => 'failed' },
	    "$pfx: loading 'current' when set_current hasn't been used yet gets slot 1 (which is in use)");

	$steps->{'load_slot_4'}->();
    };

    # this should unload what's in drive 1 and load the empty volume in slot 4
    step load_slot_4 => sub {
	$chg->load(slot => 4, set_current => 1, res_cb => $steps->{'loaded_slot_4'});
    };

    step loaded_slot_4 => sub {
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

	$steps->{'label_tape_4'}->();
    };

    step label_tape_4 => sub {
	$res2->{'device'}->start($Amanda::Device::ACCESS_WRITE, "TAPE-4", undef);
	$res2->{'device'}->finish();

	$res2->set_label(label => "TAPE-4", finished_cb => $steps->{'inventory4'});
    };

    step inventory4 => sub {
	my ($err) = @_;
	die "$err" if $err;
	pass("$pfx: labeled TAPE-4 in drive 1");

	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'release2'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111',
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1',
	      reserved => 1, loaded_in => 0 },
	    { slot => 2, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222',
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-2' },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444', reserved => 1, loaded_in => 1, current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	    { slot => 5, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory is up to date after more labelings");
    };

    step release2 => sub {
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

	$res1->release(finished_cb => $steps->{'release2_done'});
    };

    step release2_done => sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 1/drive 0 released");

	is($chg->{'__last_state'}->{'drives'}->{0}->{'label'}, 'TAPE-1',
		"$pfx: tape is still in drive");

	$steps->{'release3'}->();
    };

    step release3 => sub {
	my ($err) = @_;
	die $err if $err;

	$res2->release(finished_cb => $steps->{'release3_done'});
    };

    step release3_done => sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 4/drive 0 released");

	is($chg->{'__last_state'}->{'drives'}->{1}->{'label'},
		'TAPE-4', "$pfx: tape is still in drive");

	$steps->{'load_preloaded_by_slot'}->();
    };

    # try loading a slot that's already in a drive
    step load_preloaded_by_slot => sub {
	$chg->load(slot => 1, res_cb => $steps->{'loaded_preloaded_by_slot'});
    };

    step loaded_preloaded_by_slot => sub {
	(my $err, $res1) = @_;
	die $err if $err;

	is($res1->{'device'}->device_name, "file:$vtape_root/drive0",
	    "$pfx: loading a tape (by slot) that's already in a drive returns that drive");

	$res1->release(finished_cb => $steps->{'load_preloaded_by_label'});
    };

    # try again, this time by label
    step load_preloaded_by_label => sub {
	pass("$pfx: slot 1/drive 0 released");

	$chg->load(label => 'TAPE-4', res_cb => $steps->{'loaded_preloaded_by_label'});
    };

    step loaded_preloaded_by_label => sub {
	(my $err, $res1) = @_;
	die $err if $err;

	is($res1->{'device'}->device_name, "file:$vtape_root/drive1",
	    "$pfx: loading a tape (by label) that's already in a drive returns that drive");

	$res1->release(finished_cb => $steps->{'load_unloaded_by_label'});
    };

    # test out searching by label
    step load_unloaded_by_label => sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 4/drive 1 released");

	$chg->load(label => 'TAPE-2', res_cb => $steps->{'loaded_unloaded_by_label'});
    };

    step loaded_unloaded_by_label => sub {
	(my $err, $res1) = @_;
	die $err if $err;

	$res1->{'device'}->read_label();
	is($res1->{'device'}->volume_label, "TAPE-2",
	    "$pfx: loading a tape (by label) that's *not* already in a drive returns " .
	    "the correct device");

	$steps->{'release4'}->();
    };

    step release4 => sub {
	$res1->release(finished_cb => $steps->{'release4_done'}, eject => 1);
    };

    step release4_done => sub {
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

	$steps->{'load_current_2'}->();
    };

    step load_current_2 => sub {
	$chg->load(relative_slot => "current", res_cb => $steps->{'loaded_current_2'});
    };

    step loaded_current_2 => sub {
	(my $err, $res1) = @_;
	die $err if $err;

	$res1->{'device'}->read_label();
	is($res1->{'device'}->volume_label, "TAPE-4",
	    "$pfx: loading 'current' returns the correct device");

	$steps->{'release5'}->();
    };

    step release5 => sub {
	$res1->release(finished_cb => $steps->{'load_slot_next'});
    };

    step load_slot_next => sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 4/drive 1 released");

	$chg->load(relative_slot => "next", res_cb => $steps->{'loaded_slot_next'});
    };

    step loaded_slot_next => sub {
	(my $err, $res1) = @_;
	die $err if $err;

	$res1->{'device'}->read_label();
	is($res1->{'device'}->volume_label, "TAPE-1",
	    "$pfx: loading 'next' returns the correct slot, skipping slot 5 and " .
		    "looping around to the beginning");

	$steps->{'load_res1_next_slot'}->();
    };

    step load_res1_next_slot => sub {
	$chg->load(relative_slot => "next", slot => $res1->{'this_slot'},
		   res_cb => $steps->{'loaded_res1_next_slot'});
    };

    step loaded_res1_next_slot => sub {
	(my $err, $res2) = @_;
	die $err if $err;

	$res2->{'device'}->read_label();
	is($res2->{'device'}->volume_label, "TAPE-2",
	    "$pfx: \$res->{this_slot} + 'next' returns the correct slot, too");
        if ($mtx_config->{'barcodes'} == 1) {
            is($res2->{'barcode'}, '22222',
                "$pfx: result has a barcode");
        }

	$steps->{'release6'}->();
    };

    step release6 => sub {
	$res1->release(finished_cb => $steps->{'release7'});
    };

    step release7 => sub {
	my ($err) = @_;
	die "$err" if $err;

	pass("$pfx: slot 1 released");

	$res2->release(finished_cb => $steps->{'load_disallowed_slot'});
    };

    step load_disallowed_slot => sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: slot 2 released");

	$chg->load(slot => 6, res_cb => $steps->{'loaded_disallowed_slot'});
    };

    step loaded_disallowed_slot => sub {
	(my $err, $res1) = @_;

	chg_err_like($err,
	    { message => "slot 6 not in use-slots (1-5)",
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: loading a disallowed slot fails propertly");

	$steps->{'inventory5'}->();
    };

    step inventory5 => sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'try_update'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111', loaded_in => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
	    { slot => 2, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222', loaded_in => 0,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-2' },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444', current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	    { slot => 5, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory still accurate");
    };

    step try_update => sub {
	# first, add a label in slot 3, which hasn't been written
	# to yet
	my $dev = Amanda::Device->new("file:$vtape_root/slot3");
	die $dev->error_or_status()
	    unless $dev->status == 0;
	die "error writing label"
	    unless $dev->start($Amanda::Device::ACCESS_WRITE, "TAPE-3", undef);
	$dev->finish();

	# now update that slot
	$chg->update(changed => "2-4", finished_cb => $steps->{'update_finished'});
    };

    step update_finished => sub {
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

	$steps->{'try_update2'}->();
    };

    step try_update2 => sub {
	# lie about slot 2
	$chg->update(changed => "2=SURPRISE!", finished_cb => $steps->{'update_finished2'});
    };

    step update_finished2 => sub {
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

	$steps->{'try_update3'}->();
    };

    step try_update3 => sub {
	# lie about slot 2
	$chg->update(changed => "5=NO!", finished_cb => $steps->{'update_finished3'});
    };

    step update_finished3 => sub {
	my ($err) = @_;
	chg_err_like($err,
	    { message => "slot 5 is empty",
	      reason => 'unknown',
	      type => 'failed' },
	    "$pfx: assignment-style update of an empty slot gives error");

	$steps->{'inventory6'}->();
    };

    step inventory6 => sub {
	# note that the loading behavior of update() is not required, so the loaded_in
	# keys here may change if update() gets smarter
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'move1'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111',
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
	    { slot => 2, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222',
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'SURPRISE!' },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333', loaded_in => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, 
	      device_error => undef,
	      f_type => undef, label => 'TAPE-3' },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444', loaded_in => 0, current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	    { slot => 5, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory reflects updates");
    };

    step move1 => sub {
	# move to a full slot
	$chg->move(from_slot => 2, to_slot => 1, finished_cb => $steps->{'moved1'});
    };

    step moved1 => sub {
	my ($err) = @_;

	chg_err_like($err,
	    { message => "slot 1 is not empty",
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: moving to a full slot is an error");

	$steps->{'move2'}->();
    };

    step move2 => sub {
	# move to a full slot that's loaded (so there's not *actually* a tape
	# in the slot)
	$chg->move(from_slot => 2, to_slot => 3, finished_cb => $steps->{'moved2'});
    };

    step moved2 => sub {
	my ($err) = @_;

	chg_err_like($err,
	    { message => "slot 3 is not empty",
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: moving to a full slot is an error even if that slot is loaded");

	$steps->{'move3'}->();
    };

    step move3 => sub {
	# move from an empty slot
	$chg->move(from_slot => 5, to_slot => 3, finished_cb => $steps->{'moved3'});
    };

    step moved3 => sub {
	my ($err) = @_;

	chg_err_like($err,
	    { message => "slot 5 is empty", # note that this depends on the order of checks..
	      reason => 'invalid',
	      type => 'failed' },
	    "$pfx: moving from an empty slot is an error");

	$steps->{'move4'}->();
    };

    step move4 => sub {
	# move from a loaded slot to an empty slot
	$chg->move(from_slot => 4, to_slot => 5, finished_cb => $steps->{'moved4'});
    };

    step moved4 => sub {
	my ($err) = @_;
	die "$err" if $err;

	pass("$pfx: move of a loaded volume succeeds");

	$steps->{'move5'}->();
    };

    step move5 => sub {
	$chg->move(from_slot => 2, to_slot => 4, finished_cb => $steps->{'inventory7'});
    };


    step inventory7 => sub {
	my ($err) = @_;
	die $err if $err;

	pass("$pfx: move succeeds");

	# note that the loading behavior of update() is not required, so the loaded_in
	# keys here may change if update() gets smarter
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'start_scan'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111',
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
	    { slot => 2, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333', loaded_in => 1,
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => undef, label => 'TAPE-3' },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222', current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'SURPRISE!' },
	    { slot => 5, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444',
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	], "$pfx: inventory reflects the move");
    };

    # test a scan, using except_slots
    my %except_slots;

    step start_scan => sub {
	$chg->load(relative_slot => "current", except_slots => { %except_slots },
		   res_cb => $steps->{'loaded_for_scan'});
    };

    step loaded_for_scan => sub {
        (my $err, $res1) = @_;
	my $slot;
	if ($err) {
	    if ($err->notfound) {
		return $steps->{'scan_done'}->();
	    } elsif ($err->volinuse and defined $err->{'slot'}) {
		$slot = $err->{'slot'};
	    } else {
		die $err;
	    }
	} else {
	    $slot = $res1->{'this_slot'};
	}

	$except_slots{$slot} = 1;

	$res1->release(finished_cb => $steps->{'released_for_scan'});
    };

    step released_for_scan => sub {
	my ($err) = @_;
	die $err if $err;

        $chg->load(relative_slot => 'next', slot => $res1->{'this_slot'},
		   except_slots => { %except_slots },
		   res_cb => $steps->{'loaded_for_scan'});
    };

    step scan_done => sub {
	is_deeply({ %except_slots }, { 4=>1, 5=>1, 1=>1, 3=>1 },
		"$pfx: scanning with except_slots works");
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'update_unknown'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_FULL,
	      barcode => '11111', loaded_in => 1,
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
	    { slot => 2, state => Amanda::Changer::SLOT_EMPTY,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => '33333', loaded_in => 0,
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => undef, label => 'TAPE-3' },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => '22222', current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-2' },
	    { slot => 5, state => Amanda::Changer::SLOT_FULL,
	      barcode => '44444',
	      device_status => $DEVICE_STATUS_SUCCESS,
	      device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	], "$pfx: inventory before updates with unknown state");
    };

    step update_unknown => sub {
	$chg->update(changed => "3-4=", finished_cb => $steps->{'update_unknown_finished'});
    };

    step update_unknown_finished => sub {
	my ($err) = @_;
	die "$err" if $err;

	if ($mtx_config->{'barcodes'} > 0) {
	    check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'quit'}, [
		{ slot => 1, state => Amanda::Changer::SLOT_FULL,
		  barcode => '11111', loaded_in => 1,
		  device_status => $DEVICE_STATUS_SUCCESS,
		  device_error => undef,
		  f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
		{ slot => 2, state => Amanda::Changer::SLOT_EMPTY,
		  device_status => undef, device_error => undef,
		  f_type => undef, label => undef },
		{ slot => 3, state => Amanda::Changer::SLOT_FULL,
		  barcode => '33333', loaded_in => 0,
		  device_status => $DEVICE_STATUS_SUCCESS,
		  device_error => undef,
		  f_type => undef, label => 'TAPE-3' },
		{ slot => 4, state => Amanda::Changer::SLOT_FULL,
		  barcode => '22222', current => 1,
		  device_status => $DEVICE_STATUS_SUCCESS,
		  device_error => undef,
		  f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-2' },
		{ slot => 5, state => Amanda::Changer::SLOT_FULL,
		  barcode => '44444',
		  device_status => $DEVICE_STATUS_SUCCESS,
		  device_error => undef,
		  f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	    ], "$pfx: inventory reflects updates with unknown state with barcodes");
	} else {
	    check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'quit'}, [
		{ slot => 1, state => Amanda::Changer::SLOT_FULL,
		  barcode => '11111', loaded_in => 1,
		  device_status => $DEVICE_STATUS_SUCCESS,
		  device_error => undef,
		  f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
		{ slot => 2, state => Amanda::Changer::SLOT_EMPTY,
		  device_status => undef, device_error => undef,
		  f_type => undef, label => undef },
		{ slot => 3, state => Amanda::Changer::SLOT_FULL,
		  barcode => '33333', loaded_in => 0,
		  device_status => undef, device_error => undef,
		  f_type => undef, label => undef },
		{ slot => 4, state => Amanda::Changer::SLOT_FULL,
		  barcode => '22222', current => 1,
		  device_status => undef, device_error => undef,
		  f_type => undef, label => undef },
		{ slot => 5, state => Amanda::Changer::SLOT_FULL,
		  barcode => '44444',
		  device_status => $DEVICE_STATUS_SUCCESS,
		  device_error => undef,
		  f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	    ], "$pfx: inventory reflects updates with unknown state without barcodes");
	}
    };

    step quit => sub {
	unlink($chg_state_file) if -f $chg_state_file;
	unlink($mtx_state_file) if -f $mtx_state_file;
	rmtree($vtape_root);

	$finished_cb->();
    };
}

# These tests are run over a number of different mtx configurations, to ensure
# that the behavior is identical regardless of the changer/mtx characteristics
for my $mtx_config (
    { barcodes => 1, track_orig => 1, },
    { barcodes => 0, track_orig => 1, },
    { barcodes => 1, track_orig => -1, },
    { barcodes => 0, track_orig => 0, },
    { barcodes => -1, track_orig => 0, },
    ) {
    test_changer($mtx_config, \&Amanda::MainLoop::quit);
    Amanda::MainLoop::run();
}
