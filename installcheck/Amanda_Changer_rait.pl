# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 43;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Changer;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

my $tapebase = "$Installcheck::TMP/Amanda_Changer_rait_test";

sub reset_taperoot {
    for my $root (1 .. 3) {
	my $taperoot = "$tapebase/$root";
	if (-d $taperoot) {
	    rmtree($taperoot);
	}
	mkpath($taperoot);

	for my $slot (1 .. 4) {
	    mkdir("$taperoot/slot$slot")
		or die("Could not mkdir: $!");
	}
    }
}

sub label_vtape {
    my ($root, $slot, $label) = @_;
    mkpath("$tapebase/tmp");
    symlink("$tapebase/$root/slot$slot", "$tapebase/tmp/data");
    my $dev = Amanda::Device->new("file:$tapebase/tmp");
    $dev->start($Amanda::Device::ACCESS_WRITE, $label, undef)
        or die $dev->error_or_status();
    $dev->finish()
        or die $dev->error_or_status();
    rmtree("$tapebase/tmp");
}

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $testconf = Installcheck::Config->new();
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

reset_taperoot();
label_vtape(1,1,"mytape");
label_vtape(2,3,"mytape");
label_vtape(3,4,"mytape");
{
    my $err = Amanda::Changer->new("chg-rait:chg-disk:$tapebase/1");
    chg_err_like($err,
	{ message => "chg-rait needs at least two child changers",
	  type => 'fatal' },
	"single child device detected and handled");

    $err = Amanda::Changer->new("chg-rait:chg-disk:{$tapebase/13,$tapebase/14}");
    chg_err_like($err,
	{ message => qr/chg-disk.*13: directory.*; chg-disk.*14: directory.*/,
	  type => 'fatal' },
	"constructor errors in child devices detected and handled");
}

sub test_threeway {
    my ($finished_cb) = @_;

    my $chg = Amanda::Changer->new("chg-rait:chg-disk:$tapebase/{1,2,3}");
    pass("Create 3-way RAIT of vtapes");

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() };

    step get_info => sub {
        $chg->info(info_cb => $steps->{'check_info'},
	    info => [ 'num_slots', 'vendor_string', 'fast_search' ]);
    };

    step check_info => sub {
        my ($err, %results) = @_;
        die($err) if defined($err);

        is($results{'num_slots'}, 4,
	    "info() returns the correct num_slots");
        is($results{'vendor_string'}, '{chg-disk,chg-disk,chg-disk}',
	    "info() returns the correct vendor string");
        is($results{'fast_search'}, 1,
	    "info() returns the correct fast_search");

	$steps->{'do_load_current'}->();
    };

    step do_load_current => sub {
	$chg->load(relative_slot => "current", res_cb => $steps->{'got_res_current'});
    };

    step got_res_current => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot 'current'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,file:$tapebase/3/drive0}",
	    "returns correct device name");
	is($res->{'this_slot'}, '{1,1,1}',
	    "returns correct 'this_slot' name");

	$res->release(finished_cb => $steps->{'do_load_next'});
    };

    step do_load_next => sub {
	my ($err) = @_;
	die $err if $err;

	# (use a slot-relative 'next', rather than relative to current)
	$chg->load(relative_slot => "next", slot => '{1,1,1}', res_cb => $steps->{'got_res_next'});
    };

    step got_res_next => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot 'next'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,file:$tapebase/3/drive0}",
	    "returns correct device name");
	is($res->{'this_slot'}, '{2,2,2}',
	    "returns correct 'this_slot' name");

	$res->release(finished_cb => $steps->{'do_load_label'});
    };

    step do_load_label => sub {
	my ($err) = @_;
	die $err if $err;

	$chg->load(label => "mytape", res_cb => $steps->{'got_res_label'});
    };

    step got_res_label => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot 'label'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,file:$tapebase/3/drive0}",
	    "returns correct device name");
	is($res->{'this_slot'}, '{1,3,4}',
	    "returns correct 'this_slot' name, even with different slots");

	$res->release(finished_cb => $steps->{'do_load_slot'});
    };

    step do_load_slot => sub {
	my ($err) = @_;
	die $err if $err;

	$chg->load(slot => "{1,2,3}", res_cb => $steps->{'got_res_slot'});
    };

    step got_res_slot => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot '{1,2,3}'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,file:$tapebase/3/drive0}",
	    "returns correct device name");
	is($res->{'this_slot'}, '{1,2,3}',
	    "returns the 'this_slot' I requested");

	$res->release(finished_cb => $steps->{'do_load_slot_nobraces'});
    };

    step do_load_slot_nobraces => sub {
	my ($err) = @_;
	die $err if $err;

	# test the shorthand "2" -> "{2,2,2}"
	$chg->load(slot => "2", res_cb => $steps->{'got_res_slot_nobraces'});
    };

    step got_res_slot_nobraces => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot '2'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,file:$tapebase/3/drive0}",
	    "returns correct device name");
	is($res->{'this_slot'}, '{2,2,2}',
	    "returns an expanded 'this_slot' of {2,2,2} in response to the shorthand '2'");

	$res->release(finished_cb => $steps->{'do_load_slot_failure'});
    };

    step do_load_slot_failure => sub {
	my ($err) = @_;
	die $err if $err;

	$chg->load(slot => "{1,99,1}", res_cb => $steps->{'got_res_slot_failure'});
    };

    step got_res_slot_failure => sub {
	my ($err, $res) = @_;
	chg_err_like($err,
	    { message => qr/from chg-disk.*2: Slot 99 not found/,
	      type => 'failed',
	      reason => 'invalid' },
	    "failure of a child to load a slot is correctly propagated");

	$steps->{'do_load_slot_multifailure'}->();
    };

    step do_load_slot_multifailure => sub {
	my ($err) = @_;
	die $err if $err;

	$chg->load(slot => "{99,1,99}", res_cb => $steps->{'got_res_slot_multifailure'});
    };

    step got_res_slot_multifailure => sub {
	my ($err, $res) = @_;
	chg_err_like($err,
	    { message => qr/from chg-disk.*1: Slot 99 not found; from chg-disk.*3: /,
	      type => 'failed',
	      reason => 'invalid' },
	    "failure of multiple chilren to load a slot is correctly propagated");

	$steps->{'do_inventory'}->();
    };

    step do_inventory => sub {
	$chg->inventory(inventory_cb => $steps->{'got_inventory'});
    };

    step got_inventory => sub {
	my ($err, $inv) = @_;
	die $err if $err;

	is_deeply($inv,  [
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	    f_type => $Amanda::Header::F_EMPTY, label => undef, # undef because labels don't match
	    reserved => 0,
	    slot => '{1,1,1}', import_export => undef },
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	    f_type => $Amanda::Header::F_EMPTY, label => undef, # all blank
	    reserved => 0,
	    slot => '{2,2,2}', import_export => undef },
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	    f_type => $Amanda::Header::F_EMPTY, label => undef, # mismatched labels
	    reserved => 0,
	    slot => '{3,3,3}', import_export => undef },
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_VOLUME_UNLABELED,
	    f_type => $Amanda::Header::F_EMPTY, label => undef, # mismatched labels
	    reserved => 0,
	    slot => '{4,4,4}', import_export => undef } ,
        ], "inventory is correct");

	$finished_cb->();
    };
}
test_threeway(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

sub test_threeway_error {
    my ($finished_cb) = @_;

    my $chg = Amanda::Changer->new("chg-rait:{chg-disk:$tapebase/1,chg-disk:$tapebase/2,ERROR}");
    pass("Create 3-way RAIT of vtapes, with the third errored out");
    is($chg->have_inventory(), '1', "changer have inventory");

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() };

    step get_info => sub {
        $chg->info(info_cb => $steps->{'check_info'},
	    info => [ 'num_slots', 'fast_search' ]);
    };

    step check_info => sub {
        my $err = shift;
        my %results = @_;
        die($err) if defined($err);

        is($results{'num_slots'}, 4, "info() returns the correct num_slots");
        is($results{'fast_search'}, 1, "info() returns the correct fast_search");

	$steps->{'do_load_current'}->();
    };

    step do_load_current => sub {
	$chg->load(relative_slot => "current", res_cb => $steps->{'got_res_current'});
    };

    step got_res_current => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot 'current'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,MISSING}",
	    "returns correct device name");
	is($res->{'this_slot'}, '{1,1,ERROR}',
	    "returns correct 'this_slot' name");

	$res->release(finished_cb => $steps->{'do_load_label'});
    };

    step do_load_label => sub {
	my ($err) = @_;
	die $err if $err;

	$chg->load(label => "mytape", res_cb => $steps->{'got_res_label'});
    };

    step got_res_label => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot 'label'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,MISSING}",
	    "returns correct device name");
	is($res->{'this_slot'}, '{1,3,ERROR}',
	    "returns correct 'this_slot' name, even with different slots");

	$res->release(finished_cb => $steps->{'released'});
    };

    step released => sub {
	my ($err) = @_;
	die $err if $err;

	$steps->{'do_reset'}->();
    };

    # unfortunately, reset, clean, and update are pretty boring with vtapes, so
    # it's hard to test them effectively.

    step do_reset => sub {
	my ($err) = @_;
	die $err if $err;

	$chg->reset(finished_cb => $steps->{'finished_reset'});
    };

    step finished_reset => sub {
	my ($err) = @_;
	ok(!$err, "no error resetting");

	$finished_cb->();
    };
}
test_threeway_error(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

# test inventory under "normal" circumstances
sub test_normal_inventory {
    my ($finished_cb) = @_;

    my $chg = Amanda::Changer->new("chg-rait:chg-disk:$tapebase/{1,2,3}");
    pass("Create 3-way RAIT of vtapes with correctly-labeled children");

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() };

    step setup => sub {
	reset_taperoot();
	label_vtape(1,1,"mytape-1");
	label_vtape(2,1,"mytape-1");
	label_vtape(3,1,"mytape-1");
	label_vtape(1,2,"mytape-2");
	label_vtape(2,2,"mytape-2");
	label_vtape(3,2,"mytape-2");

	$steps->{'do_inventory'}->();
    };

    step do_inventory => sub {
	$chg->inventory(inventory_cb => $steps->{'got_inventory'});
    };

    step got_inventory => sub {
	my ($err, $inv) = @_;
	die $err if $err;

	is_deeply($inv,  [
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_SUCCESS, f_type => $Amanda::Header::F_TAPESTART, label => 'mytape-1', reserved => 0,
	    slot => '{1,1,1}', import_export => undef },
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_SUCCESS, f_type => $Amanda::Header::F_TAPESTART, label => 'mytape-2', reserved => 0,
	    slot => '{2,2,2}', import_export => undef },
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_VOLUME_UNLABELED, f_type => $Amanda::Header::F_EMPTY, label => undef, reserved => 0,
	    slot => '{3,3,3}', import_export => undef },
          { state => Amanda::Changer::SLOT_FULL, device_status => $DEVICE_STATUS_VOLUME_UNLABELED, f_type => $Amanda::Header::F_EMPTY, label => undef, reserved => 0,
	    slot => '{4,4,4}', import_export => undef } ,
        ], "second inventory is correct");

	$finished_cb->();
    };
}
test_normal_inventory(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

##
# Test configuring the device with device_property

$testconf = Installcheck::Config->new();
$testconf->add_changer("myrait", [
    tpchanger => "\"chg-rait:chg-disk:$tapebase/{1,2,3}\"",
    device_property => '"comment" "hello, world"',
]);
$testconf->write();

config_uninit();
$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}


sub test_properties {
    my ($finished_cb) = @_;

    my $chg = Amanda::Changer->new("myrait");
    ok($chg->isa("Amanda::Changer::rait"),
	"Create RAIT device from a named config subsection");

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() };

    step do_load_1 => sub {
	reset_taperoot();
	label_vtape(1,1,"mytape");
	label_vtape(2,2,"mytape");
	label_vtape(3,3,"mytape");

	$chg->load(slot => "1", res_cb => $steps->{'got_res_1'});
    };

    step got_res_1 => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot '1'")
	    or diag($err);
	is($res->{'device'}->device_name,
	   "rait:{file:$tapebase/1/drive0,file:$tapebase/2/drive0,file:$tapebase/3/drive0}",
	    "returns correct (full) device name");
	is($res->{'this_slot'}, '{1,1,1}',
	    "returns correct 'this_slot' name");
	is($res->{'device'}->property_get("comment"), "hello, world",
	    "property from device_property appears on RAIT device");

	$res->release(finished_cb => $steps->{'quit'});
    };

    step quit => sub {
	my ($err) = @_;
	die $err if $err;

	$finished_cb->();
    };
}
test_properties(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

# scan the changer using except_slots
sub test_except_slots {
    my ($finished_cb) = @_;
    my $slot;
    my %except_slots;
    my $chg;

    my $steps = define_steps
	cb_ref => \$finished_cb,
	finalize => sub { $chg->quit() if defined $chg };

    step start => sub {
	$chg = Amanda::Changer->new("myrait");
	die "error creating" unless $chg->isa("Amanda::Changer::rait");

	$chg->load(relative_slot => "current", except_slots => { %except_slots },
		   res_cb => $steps->{'loaded'});
    };

    step loaded => sub {
        my ($err, $res) = @_;
	if ($err) {
	    if ($err->notfound) {
		# this means the scan is done
		return $steps->{'quit'}->();
	    } elsif ($err->volinuse and defined $err->{'slot'}) {
		$slot = $err->{'slot'};
	    } else {
		die $err;
	    }
	} else {
	    $slot = $res->{'this_slot'};
	}

	$except_slots{$slot} = 1;

	if ($res) {
	    $res->release(finished_cb => $steps->{'released'});
	} else {
	    $steps->{'released'}->();
	}
    };

    step released => sub {
	my ($err) = @_;
	die $err if $err;

        $chg->load(relative_slot => 'next', slot => $slot,
		   except_slots => { %except_slots },
		   res_cb => $steps->{'loaded'});
    };

    step quit => sub {
	is_deeply({ %except_slots }, { "{1,1,1}"=>1, "{2,2,2}"=>1, "{3,3,3}"=>1, "{4,4,4}"=>1 },
		"scanning with except_slots works");
	$finished_cb->();
    };
}
test_except_slots(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

rmtree($tapebase);
