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

use Test::More tests => 31;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Run;
use Installcheck::Changer;
use Amanda::Paths;
use Amanda::Device;
use Amanda::Debug qw( :logging );
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $changer_filename = "$Installcheck::TMP/chg-test";
my $result_file = "$Installcheck::TMP/chg-test.result";

# Set up a 'test' changer; several of these are defined below.
sub setup_changer {
    my ($changer_script) = @_;

    open my $chg_test, ">", $changer_filename or die("Could not create test changer");

    $changer_script =~ s/\$Installcheck::TMP/$Installcheck::TMP/g;

    print $chg_test "#! /bin/sh\n";
    print $chg_test $changer_script;

    close $chg_test;
    chmod 0755, $changer_filename;
}

# slurp the $result_file
sub slurp_result {
    return '' unless (-r $result_file);

    open(my $fh, "<", $result_file) or die("open $result_file: $!");
    my $result = do { local $/; <$fh> };
    close($fh);

    return $result;
}

# Functions to invoke the changer and later verify the result
my ($check_res_cb, $check_finished_cb);
{
    my $expected_err_info;
    my $expected_dev;
    my $msg;
    my $quit;

    $check_res_cb = make_cb('check_res_cb' => sub {
	my ($err, $res) = @_;

	if ($err) {
	    if (defined($expected_err_info)) {
		chg_err_like($err, $expected_err_info, $msg);
	    } else {
		fail($msg);
		diag("Unexpected error: $err");
	    }
	} else {
	    if (defined($expected_dev)) {
		is($res->{'device'}->device_name, $expected_dev, $msg);
	    } else {
		fail($msg);
		diag("Unexpected reservation");
	    }
	}

	if ($res) {
	    $res->release(finished_cb => $quit);
	} else {
	    $quit->();
	}
    });

    $check_finished_cb = make_cb('check_finished_cb' => sub {
	my ($err, $res) = @_;

	if ($err) {
	    if (defined($expected_err_info)) {
		chg_err_like($err, $expected_err_info, $msg);
	    } else {
		fail($msg);
		diag("Unexpected error: $err");
	    }
	} else {
	    if (!defined($expected_err_info)) {
		pass($msg);
	    } else {
		fail($msg);
		diag("Unexpected success");
	    }
	}

	if ($res) {
	    $res->release(finished_cb => $quit);
	} else {
	    $quit->();
	}
    });

    $quit = make_cb(quit => sub {
	my ($err) = @_;
	die $err if $err;

	Amanda::MainLoop::quit();
    });

    sub try_run_changer {
	my $sub;
	($sub, $expected_err_info, $expected_dev, $msg) = @_;

	Amanda::MainLoop::call_later($sub);
	Amanda::MainLoop::run();
    }
}

# OK, let's get started with some simple stuff
setup_changer <<'EOC';
case "${1}" in
    -slot)
        case "${2}" in
            1) echo "1 null:fake1"; exit 0;;
            2) echo "<ignored> slot 2 is empty"; exit 1;;
            3) echo "1"; exit 0;; # test missing 'device' portion
	    4) echo "1 bogus:dev"; exit 0;;
	    5) echo "<error> multiline error"; echo "line 2"; exit 1;;
	    current) echo "1 null:current"; exit 0;;
	    next) echo "1 null:next"; exit 0;;
        esac;;
    -reset)
	echo "reset" > $Installcheck::TMP/chg-test.result
	echo "reset ignored";;
    -eject)
	echo "eject" > $Installcheck::TMP/chg-test.result
	echo "eject ignored";;
    -clean)
	echo "clean" > $Installcheck::TMP/chg-test.result
	echo "clean ignored";;
    -label)
        case "${2}" in
            foo?bar) echo "1 ok"; exit 0;;
            *) echo "<error> bad label"; exit 1;;
        esac;;
    -info) echo "7 10 1 1"; exit 0;;
    -search)
        case "${2}" in
            TAPE?01) echo "5 null:fakedev"; exit 0;;
	    fatal) echo "<error> game over"; exit 2;;
            *) echo "<error> not found"; exit 1;;
        esac;;
esac
EOC

# set up a config for this changer, implicitly using Amanda::Changer::Compat
my $testconf;
$testconf = Installcheck::Config->new();
$testconf->add_param("tpchanger", "\"$changer_filename\"");
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

my $chg = Amanda::Changer->new();
die($chg) if $chg->isa("Amanda::Changer::Error");

is($chg->have_inventory(), '', "changer have inventory");

try_run_changer(
    sub { $chg->load(label => 'TAPE-01', res_cb => $check_res_cb); },
    undef,
    "null:fakedev",
    "search by label succeeds");

try_run_changer(
    sub { $chg->load(label => 'TAPE-99', res_cb => $check_res_cb); },
    { message => "not found", type => 'failed', reason => 'notfound' },
    undef,
    "search by label; nonexistent tape");

try_run_changer(
    sub { $chg->load(slot => '1', res_cb => $check_res_cb); },
    undef,
    "null:fake1",
    "search by slot");

try_run_changer(
    sub { $chg->load(slot => '2', res_cb => $check_res_cb); },
    { message => "slot 2 is empty", type => 'failed', reason => 'notfound' },
    undef,
    "search by slot; empty slot");

try_run_changer(
    sub { $chg->load(slot => '3', res_cb => $check_res_cb); },
    { message => "changer script did not provide a device name", type => 'fatal' },
    undef,
    "search by slot; no device in response");

try_run_changer(
    sub { $chg->load(slot => '1', res_cb => $check_res_cb); },
    { message => "changer script did not provide a device name", type => 'fatal' },
    undef,
    "fatal error is sticky");

$chg->{'fatal_error'} = undef; # reset the fatal error

try_run_changer(
    sub { $chg->load(slot => '4', res_cb => $check_res_cb); },
    { message => "opening 'bogus:dev': Device type bogus is not known.",
      type => 'failed',
      reason => 'device' },
    undef,
    "search by slot; bogus device leads to 'failed' error");

$chg->{'fatal_error'} = undef; # reset the fatal error

try_run_changer(
    sub { $chg->load(slot => '5', res_cb => $check_res_cb); },
    { message => "multiline error\nline 2",
      type => 'failed',
      reason => 'notfound' },
    undef,
    "multiline error response captured in its entirety");

$chg->{'fatal_error'} = undef; # reset the fatal error

try_run_changer(
    sub { $chg->load(label => 'fatal', res_cb => $check_res_cb); },
    { message => "game over", type => 'fatal' },
    undef,
    "search by label with fatal error");

# reset the fatal error
$chg->{'fatal_error'} = undef;

try_run_changer(
    sub { $chg->eject(finished_cb => $check_finished_cb); },
    undef, undef, "chg->eject doesn't fail");
like(slurp_result(), qr/eject/, ".. and calls chg-test -eject");

try_run_changer(
    sub { $chg->reset(finished_cb => $check_finished_cb); },
    undef, undef, "chg->reset doesn't fail");
like(slurp_result(), qr/reset/, ".. and calls chg-test -reset");

try_run_changer(
    sub { $chg->clean(finished_cb => $check_finished_cb); },
    undef, undef, "chg->clean doesn't fail");
like(slurp_result(), qr/clean/, ".. and calls chg-test -clean");

try_run_changer(
    sub { $chg->update(finished_cb => $check_finished_cb); },
    undef, undef, "chg->update doesn't fail");

try_run_changer(
    sub { $chg->inventory(inventory_cb => $check_finished_cb); },
    { message => "'chg-compat:' does not support inventory",
	    type => 'failed', reason => 'notimpl' },
    undef,
    "inventory not implemented");


# make sure only one reservation can be held at once
{
    my $first_res;

    my ($load_1, $load_2, $check_load_2, $check_eject);

    $load_1 = make_cb('load_1' => sub {
        $chg->load(slot => 1, res_cb => $load_2);
    });

    $load_2 = make_cb('load_2' => sub {
        my ($err, $res) = @_;
        die $err if ($err);

        # keep this in scope through the next load
        $first_res = $res;

        $chg->load(slot => 2, res_cb => $check_load_2);
    });

    $check_load_2 = make_cb('check_load_2' => sub {
        my ($err, $res) = @_;

        like($err, qr/Changer is already reserved/,
            "mulitple simultaneous reservations not alowed");

        $first_res->release(eject => 1, finished_cb => $check_eject);
    });

    $check_eject = make_cb('check_eject' => sub {
        my ($err) = @_;

        ok(!defined $err, "release with eject succeeds");

	like(slurp_result(), qr/eject/, "..and calls chg-test -eject");

        Amanda::MainLoop::quit();
    });

    $load_1->();
    Amanda::MainLoop::run();
}

## check chg-disk

# Installcheck::Run sets up the whole chg-disk thing for us
$testconf = Installcheck::Run::setup();
$testconf->write();

$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$chg->quit();
$chg = Amanda::Changer->new();
die($chg) if $chg->isa("Amanda::Changer::Error");

{
    my $res;
    my ($get_info, $load_current, $label_current, $load_next,
        $released1, $release_next, $load_by_label, $check_by_label);

    $get_info = make_cb('get_info' => sub {
        $chg->info(info_cb => $load_current, info => [ 'num_slots', 'fast_search' ]);
    });

    $load_current = make_cb('load_current' => sub {
        my $err = shift;
        my %results = @_;
        die($err) if defined($err);

        is_deeply({ %results },
	    { num_slots => 3, fast_search => 0 }, # old chg-disk is not searchable
	    "info() returns the correct num_slots and fast_search");

        $chg->load(slot => "1", res_cb => $label_current);
    });

    $label_current = make_cb('label_current' => sub {
        (my $err, $res) = @_;
        die $err if ($err);

        pass("seek to current slot succeeded");

        my $dev = $res->{'device'};
        $dev->start($Amanda::Device::ACCESS_WRITE, "TESTCONF18", undef)
            or die $dev->error_or_status();
        $dev->finish()
            or die $dev->error_or_status();

        is($res->{'this_slot'}, "1", "this slot is '1'");
        $res->set_label(label => "TESTCONF18", finished_cb => $load_next);
    });

    $load_next = make_cb('load_next' => sub {
        my ($err) = @_;
        die $err if ($err);

        pass("set_label succeeded");

	$res->release(finished_cb => $released1);
    });

    $released1 = make_cb(released1 => sub {
	my ($err) = @_;
	die $err if $err;

        $chg->load(relative_slot => "next", res_cb => $release_next);
    });

    $release_next = make_cb('release_next' => sub {
        (my $err, $res) = @_;
        die $err if ($err);

        pass("load relative slot 'next' succeeded");

        $res->release(finished_cb => $load_by_label);
    });

    $load_by_label = make_cb('load_by_label' => sub {
        my ($err) = @_;
        die $err if ($err);

        pass("release loaded");

        $chg->load(label => "TESTCONF18", res_cb => $check_by_label);
    });

    $check_by_label = make_cb('check_by_label' => sub {
        (my $err, $res) = @_;
        die $err if ($err);

        pass("load by label succeeded");

        my $dev = $res->{'device'};
        $dev->read_label() == 0
            or die $dev->error_or_status();

        is($dev->volume_label(), "TESTCONF18",
            "..and finds the right volume");

	$res->release(finished_cb => sub {
	    my ($err) = @_;
	    die $err if $err;

	    Amanda::MainLoop::quit();
	});
    });

    $get_info->();
    Amanda::MainLoop::run();
}
$chg->quit();

# test two simultaneous invocations of info()

$chg = Amanda::Changer->new();
die($chg) if $chg->isa("Amanda::Changer::Error");

sub test_get_infos {
    my ($finished_cb) = @_;
    my $n_info_results = 0;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step get_infos => sub {
	# convince the changer that it has not gotten any info yet
	$chg->{'got_info'} = 0;

        $chg->info(info_cb => $steps->{'got_info_result'}, info => [ 'num_slots' ]);
        $chg->info(info_cb => $steps->{'got_info_result'}, info => [ 'fast_search' ]);
    };

    step got_info_result => sub {
	my ($err, %info) = @_;
	die $err if $err;
	++$n_info_results;
	if ($n_info_results >= 2) {
	    pass("two simultaneous info() invocations are successful");
	    $finished_cb->();
	}
    };
}
test_get_infos(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

# scan the changer using except_slots
sub test_except_slots {
    my ($finished_cb) = @_;
    my $slot;
    my %except_slots;

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step start => sub {
	$chg->load(relative_slot => "current",
		   except_slots => { %except_slots },
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
	is_deeply({ %except_slots }, { 1=>1, 2=>1, 3=>1 },
		"scanning with except_slots works");
	$finished_cb->();
    };
}
test_except_slots(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();
$chg->quit();

unlink($changer_filename);
unlink($result_file);
