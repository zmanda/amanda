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

use Test::More tests => 171;
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

my $ndmp = Installcheck::Mock::NdmpServer->new();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $chg_state_file = "$Installcheck::TMP/chg-ndmp-state";
unlink($chg_state_file) if -f $chg_state_file;

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
	finalize => sub { $chg->quit() if defined $chg};

    step start => sub {
	my $testconf = Installcheck::Config->new();
	$testconf->add_changer('robo', [
	    tpchanger => "\"chg-ndmp:127.0.0.1:$ndmp->{port}\@$ndmp->{changer}\"",
	    changerfile => "\"$chg_state_file\"",

	    property => "       \"tape-device\" \"0=ndmp:127.0.0.1:$ndmp->{port}\@$ndmp->{drive0}\"",
	    property => "append \"tape-device\" \"1=ndmp:127.0.0.1:$ndmp->{port}\@$ndmp->{drive1}\"",
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
	    'revision' => '1.0',
	    'product id' => 'FakeRobot',
	    'vendor id' => 'NDMJOB',
	    'product type' => 'Medium Changer'
	    }, "robot::Interface inquiry() info is correct");

	$steps->{'status1'}->();
    };

    step status1 => sub {
	$interface->status(sub {
	    my ($error, $status) = @_;

	    die $error if $error;

	    is_deeply($status, {
		'drives' => {
		    '0' => undef,
		    '1' => undef,
		},
		'slots' => {
		    1 => { ie => 1, empty => 1 },
		    2 => { ie => 1, empty => 1 },
		    3 => { barcode => 'PTAG00XX', },
		    4 => { barcode => 'PTAG01XX', },
		    5 => { barcode => 'PTAG02XX', },
		    6 => { barcode => 'PTAG03XX', },
		    7 => { barcode => 'PTAG04XX', },
		    8 => { barcode => 'PTAG05XX', },
		    9 => { barcode => 'PTAG06XX', },
		    10 => { barcode => 'PTAG07XX', },
		    11 => { barcode => 'PTAG08XX', },
		    12 => { barcode => 'PTAG09XX', },
		},
	    }, "robot::Interface status() output is correct (no drives loaded)")
		or die("robot does not look like I expect it to");

	    $steps->{'load0'}->();
	});
    };

    step load0 => sub {
	$interface->load(3, 0, sub {
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
		'drives' => {
		    '0' => { barcode => 'PTAG00XX', orig_slot => 3 },
		    '1' => undef,
		},
		'slots' => {
		    1 => { ie => 1, empty => 1 },
		    2 => { ie => 1, empty => 1 },
		    3 => { empty => 1 },
		    4 => { barcode => 'PTAG01XX', },
		    5 => { barcode => 'PTAG02XX', },
		    6 => { barcode => 'PTAG03XX', },
		    7 => { barcode => 'PTAG04XX', },
		    8 => { barcode => 'PTAG05XX', },
		    9 => { barcode => 'PTAG06XX', },
		    10 => { barcode => 'PTAG07XX', },
		    11 => { barcode => 'PTAG08XX', },
		    12 => { barcode => 'PTAG09XX', },
		},
	    }, "robot::Interface status() output is correct (one drive loaded)")
		or die("robot does not look like I expect it to");

	    $steps->{'load1'}->();
	});
    };

    step load1 => sub {
	$interface->load(12, 1, sub {
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
		'drives' => {
		    '0' => { barcode => 'PTAG00XX', orig_slot => 3 },
		    '1' => { barcode => 'PTAG09XX', orig_slot => 12 },
		},
		'slots' => {
		    1 => { ie => 1, empty => 1 },
		    2 => { ie => 1, empty => 1 },
		    3 => { empty => 1 },
		    4 => { barcode => 'PTAG01XX', },
		    5 => { barcode => 'PTAG02XX', },
		    6 => { barcode => 'PTAG03XX', },
		    7 => { barcode => 'PTAG04XX', },
		    8 => { barcode => 'PTAG05XX', },
		    9 => { barcode => 'PTAG06XX', },
		    10 => { barcode => 'PTAG07XX', },
		    11 => { barcode => 'PTAG08XX', },
		    12 => { empty => 1 },
		},
	    }, "robot::Interface status() output is correct (two drives loaded)")
		or die("robot does not look like I expect it to");

	    $steps->{'transfer'}->();
	});
    };

    step transfer => sub {
	$interface->transfer(5, 2, sub {
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
		'drives' => {
		    '0' => { barcode => 'PTAG00XX', orig_slot => 3 },
		    '1' => { barcode => 'PTAG09XX', orig_slot => 12 },
		},
		'slots' => {
		    1 => { ie => 1, empty => 1 },
		    2 => { ie => 1, barcode => 'PTAG02XX', },
		    3 => { empty => 1 },
		    4 => { barcode => 'PTAG01XX', },
		    5 => { empty => 1 },
		    6 => { barcode => 'PTAG03XX', },
		    7 => { barcode => 'PTAG04XX', },
		    8 => { barcode => 'PTAG05XX', },
		    9 => { barcode => 'PTAG06XX', },
		    10 => { barcode => 'PTAG07XX', },
		    11 => { barcode => 'PTAG08XX', },
		    12 => { empty => 1 },
		},
	    }, "robot::Interface status() output is correct after transfer")
		or die("robot does not look like I expect it to");

	    $finished_cb->();
	});
    };
}
test_interface(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

##
# Test the real deal

sub test_changer {
    my ($mtx_config, $finished_cb) = @_;
    my $chg;
    my ($res1, $res2);
    my ($drive0_name, $drive1_name);
    my $pfx = "BC=$mtx_config->{barcodes}; TORIG=$mtx_config->{track_orig}";

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() if defined $chg};

    # clean up
    step setup => sub {
	unlink($chg_state_file) if -f $chg_state_file;

	my @ignore_barcodes = ( property => "\"ignore-barcodes\" \"y\"")
	    if ($mtx_config->{'barcodes'} == -1);

	$drive0_name = "ndmp:127.0.0.1:$ndmp->{port}\@$ndmp->{drive0}";
	$drive1_name = "ndmp:127.0.0.1:$ndmp->{port}\@$ndmp->{drive1}";

	my $testconf = Installcheck::Config->new();
	$testconf->add_changer('robo', [
	    tpchanger => "\"chg-ndmp:127.0.0.1:$ndmp->{port}\@$ndmp->{changer}\"",
	    changerfile => "\"$chg_state_file\"",

	    property => "       \"tape-device\" \"0=$drive0_name\"",
	    property => "append \"tape-device\" \"1=$drive1_name\"",
	    property => "\"use-slots\" \"1-5\"",
	    property => "\"verbose\" \"1\"",
	    @ignore_barcodes,
	]);
	$testconf->write();

	config_uninit();
	my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
	if ($cfg_result != $CFGERR_OK) {
	    my ($level, @errors) = Amanda::Config::config_errors();
	    die(join "\n", @errors);
	}

	# reset the changer to its base state
	$ndmp->reset();

	$steps->{'start'}->();
    };

    step start => sub {
	$chg = Amanda::Changer->new("robo");
	ok(!$chg->isa("Amanda::Changer::Error"),
	    "$pfx: Create working chg-robot instance: $chg")
	    or die("no sense going on");

	is($chg->have_inventory(), '1', "changer have inventory");
	$chg->info(info => [qw(vendor_string num_slots fast_search)],
		    info_cb => $steps->{'info_cb'});
    };

    step info_cb => sub {
	my ($err, %info) = @_;
	die $err if $err;

	is_deeply({ %info }, {
	    num_slots => 5,
	    fast_search => 1,
	    vendor_string => "NDMJOB FakeRobot",
	}, "$pfx: info keys num_slots, fast_search, vendor_string are correct");

	$steps->{'inventory1'}->();
    };

    step inventory1 => sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'load_slot_1'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 2, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG00XX', current => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG01XX',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 5, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG02XX',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory is correct on start-up");
    };

    step load_slot_1 => sub {
	$chg->load(slot => 3, res_cb => $steps->{'loaded_slot_1'});
    };

    step loaded_slot_1 => sub {
	(my $err, $res1) = @_;
	die $err if $err;

	is($res1->{'device'}->device_name, $drive0_name,
	    "$pfx: first load returns drive-0 device");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{3}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
	    }, {
		loaded_in => 0,
		orig_slot => 3,
	    }, "$pfx: slot 3 'loaded_in' and drive 0 'orig_slot' are correct");

	$steps->{'load_slot_2'}->();
    };

    step load_slot_2 => sub {
	$chg->load(slot => 4, res_cb => $steps->{'loaded_slot_2'});
    };

    step loaded_slot_2 => sub {
	(my $err, $res2) = @_;
	die $err if $err;

	is($res2->{'device'}->device_name, $drive1_name,
	    "$pfx: second load returns drive-1 device");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{3}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
	    }, {
		loaded_in => 0,
		orig_slot => 3,
	    }, "$pfx: slot 3 'loaded_in' and drive 0 'orig_slot' are still correct");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{4}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
	    }, {
		loaded_in => 1,
		orig_slot => 4,
	    }, "$pfx: slot 4 'loaded_in' and drive 1 'orig_slot' are correct");

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
		    0 => { barcode => 'PTAG00XX', 'orig_slot' => 3 },
		    1 => { barcode => 'PTAG01XX', 'orig_slot' => 4 },
		}, "$pfx: double-check: loading drives with the changer gets the right drives loaded");
	    }

	    $steps->{'inventory2'}->();
	});
    };

    step inventory2 => sub {
	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'load_slot_3'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 2, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG00XX', reserved => 1, loaded_in => 0,
	      current => 1,
	      device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	      device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG01XX', reserved => 1, loaded_in => 1,
	      device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	      device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 5, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG02XX',
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	], "$pfx: inventory is updated when slots are loaded");
    };

    step load_slot_3 => sub {
	$chg->load(slot => 5, res_cb => $steps->{'loaded_slot_3'});
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
		loaded_in => $chg->{'__last_state'}->{'slots'}->{3}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{3}->{'label'},
		drive_label => $chg->{'__last_state'}->{'drives'}->{0}->{'label'},
	    }, {
		loaded_in => 0,
		orig_slot => 3,
		slot_label => 'TAPE-1',
		drive_label => 'TAPE-1',
	    }, "$pfx: label is correctly reflected in changer state");

	is_deeply({
		slot_2_loaded_in => $chg->{'__last_state'}->{'slots'}->{4}->{'loaded_in'},
		slot_1_loaded_in => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
	    }, {
		slot_2_loaded_in => 1,
		slot_1_loaded_in => 4,
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
		loaded_in => $chg->{'__last_state'}->{'slots'}->{4}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{4}->{'label'},
		drive_label => $chg->{'__last_state'}->{'drives'}->{1}->{'label'},
	    }, {
		loaded_in => 1,
		orig_slot => 4,
		slot_label => 'TAPE-2',
		drive_label => 'TAPE-2',
	    }, "$pfx: label is correctly reflected in changer state");

	$res2->release(finished_cb => $steps->{'inventory3'});
    };

    step inventory3 => sub {
	my ($err) = @_;
	die "$err" if $err;
	pass("$pfx: slot 4/drive 1 released");

	check_inventory($chg, $mtx_config->{'barcodes'} > 0, $steps->{'check_state_after_release1'}, [
	    { slot => 1, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 2, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG00XX', reserved => 1, loaded_in => 0,
	      current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG01XX', loaded_in => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-2' },
	    { slot => 5, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG02XX',
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
	$chg->load(slot => 5, set_current => 1, res_cb => $steps->{'loaded_slot_4'});
    };

    step loaded_slot_4 => sub {
	(my $err, $res2) = @_;
	die "$err" if $err;

	is($res2->{'device'}->device_name, $drive1_name,
	    "$pfx: loaded slot 5 into drive 1 (and set current to slot 5)");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{4}->{'loaded_in'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{4}->{'label'},
	    }, {
		loaded_in => undef,
		slot_label => 'TAPE-2',
	    }, "$pfx: slot 4 (which was just unloaded) still tracked correctly");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{3}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{0}->{'orig_slot'},
	    }, {
		loaded_in => 0,
		orig_slot => 3,
	    }, "$pfx: slot 1 'loaded_in' and drive 0 'orig_slot' are *still* correct");

	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{5}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
	    }, {
		loaded_in => 1,
		orig_slot => 5,
	    }, "$pfx: slot 5 'loaded_in' and drive 1 'orig_slot' are correct");

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
	    { slot => 1, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 2, state => Amanda::Changer::SLOT_EMPTY,
	      import_export => 1,
	      device_status => undef, device_error => undef,
	      f_type => undef, label => undef },
	    { slot => 3, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG00XX', reserved => 1, loaded_in => 0,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-1' },
	    { slot => 4, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG01XX',
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-2' },
	    { slot => 5, state => Amanda::Changer::SLOT_FULL,
	      barcode => 'PTAG02XX', reserved => 1, loaded_in => 1,
	      current => 1,
	      device_status => $DEVICE_STATUS_SUCCESS, device_error => undef,
	      f_type => $Amanda::Header::F_TAPESTART, label => 'TAPE-4' },
	], "$pfx: inventory is up to date after more labelings");
    };

    step release2 => sub {
	is_deeply({
		loaded_in => $chg->{'__last_state'}->{'slots'}->{5}->{'loaded_in'},
		orig_slot => $chg->{'__last_state'}->{'drives'}->{1}->{'orig_slot'},
		slot_label => $chg->{'__last_state'}->{'slots'}->{5}->{'label'},
		drive_label => $chg->{'__last_state'}->{'drives'}->{1}->{'label'},
	    }, {
		loaded_in => 1,
		orig_slot => 5,
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

	$steps->{'quit'}->();
    };

    # note that Amanda_Changer_robot performs a *lot* more tests; they're
    # duplicative for this changer, so they are omitted

    step quit => sub {
	unlink($chg_state_file) if -f $chg_state_file;
	$finished_cb->();
    };
    # ^^ remove final call to first sub XXX
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

$ndmp->cleanup();
