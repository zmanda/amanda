# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 22;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Installcheck::Run;
use Amanda::Paths;
use Amanda::Device;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $changer_filename = "$AMANDA_TMPDIR/chg-test";
my $result_file = "$AMANDA_TMPDIR/chg-test.result";

# Set up a 'test' changer; several of these are defined below.
sub setup_changer {
    my ($changer_script) = @_;

    open my $chg_test, ">", $changer_filename or die("Could not create test changer");

    $changer_script =~ s/\$AMANDA_TMPDIR/$AMANDA_TMPDIR/g;

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
{
    my $expected_err_re;
    my $expected_dev;
    my $msg;

    sub check_res_cb {
	my ($err, $res) = @_;
	Amanda::MainLoop::quit();

	if ($err) {
	    if (defined($expected_err_re)) {
		like($err, $expected_err_re, $msg);
	    } else {
		fail($msg);
		debug("Unexpected error: $err");
	    }
	} else {
	    if (defined($expected_dev)) {
		is($res->{'device_name'}, $expected_dev, $msg);
	    } else {
		fail($msg);
		diag("Unexpected reservation");
	    }
	}
    }

    sub check_finished_cb {
	my ($err) = @_;
	Amanda::MainLoop::quit();

	if ($err) {
	    if (defined($expected_err_re)) {
		like($err, $expected_err_re, $msg);
	    } else {
		fail($msg);
		diag("Unexpected error: $err");
	    }
	} else {
	    if (!defined($expected_err_re)) {
		pass($msg);
	    } else {
		fail($msg);
		diag("Unexpected success");
	    }
	}
    }

    sub try_run_changer {
	my $sub;
	($sub, $expected_err_re, $expected_dev, $msg) = @_;

	Amanda::MainLoop::call_later($sub);
	Amanda::MainLoop::run();
    }
}

# OK, let's get started with some simple stuff
setup_changer <<'EOC';
case "${1}" in
    -slot)
        case "${2}" in
            1) echo "1 fake:1"; exit 0;;
            2) echo "<ignored> slot 2 is empty"; exit 1;;
            3) echo "1"; exit 0;; # test missing 'device' portion
        esac;;
    -reset)
	echo "reset" > @AMANDA_TMPDIR@/chg-test.result
	echo "reset ignored";;
    -eject)
	echo "eject" > @AMANDA_TMPDIR@/chg-test.result
	echo "eject ignored";;
    -clean)
	echo "clean" > @AMANDA_TMPDIR@/chg-test.result
	echo "clean ignored";;
    -label)
        case "${2}" in
            foo?bar) echo "1 ok"; exit 0;;
            *) echo "<error> bad label"; exit 1;;
        esac;;
    -info) echo "7 10 1 1"; exit 0;;
    -search)
        case "${2}" in
            TAPE?01) echo "5 fakedev"; exit 0;;
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
try_run_changer(
    sub { $chg->load(label => 'TAPE-01', res_cb => \&check_res_cb); },
    undef, "fakedev", "search by label");

try_run_changer(
    sub { $chg->load(label => 'TAPE-99', res_cb => \&check_res_cb); },
    qr/^not found$/, undef, "search by label; nonexistent tape");

try_run_changer(
    sub { $chg->load(slot => '1', res_cb => \&check_res_cb); },
    undef, "fake:1", "search by slot");

try_run_changer(
    sub { $chg->load(slot => '2', res_cb => \&check_res_cb); },
    qr/^slot 2 is empty$/, undef, "search by slot; empty slot");

# TODO: what *should* happen here?
#try_run_changer(
#    sub { $chg->load(slot => '3', res_cb => \&check_res_cb); },
#    undef, undef, "search by slot; invalid response");

try_run_changer(
    sub { $chg->eject(finished_cb => \&check_finished_cb); },
    undef, undef, "chg->eject doesn't fail");
like(slurp_result(), qr/eject/, ".. and calls chg-test -eject");

try_run_changer(
    sub { $chg->reset(finished_cb => \&check_finished_cb); },
    undef, undef, "chg->reset doesn't fail");
like(slurp_result(), qr/reset/, ".. and calls chg-test -reset");

try_run_changer(
    sub { $chg->clean(finished_cb => \&check_finished_cb); },
    undef, undef, "chg->clean doesn't fail");
like(slurp_result(), qr/clean/, ".. and calls chg-test -clean");

# TODO test update()

# make sure only one reservation can be held at once
{
    my $first_res;

    my ($load_1, $load_2, $check_load_2, $check_eject);

    $load_1 = sub {
        $chg->load(slot => 1, res_cb => $load_2);
    };

    $load_2 = sub {
        my ($err, $res) = @_;
        die $err if ($err);

        # keep this in scope through the next load
        $first_res = $res;

        $chg->load(slot => 2, res_cb => $check_load_2);
    };

    $check_load_2 = sub {
        my ($err, $res) = @_;

        like($err, qr/Changer is already reserved/,
            "mulitple simultaneous reservations not alowed");

        $first_res->release(eject => 1, finished_cb => $check_eject);
    };

    $check_eject = sub {
        my ($err) = @_;

        ok(!defined $err, "release with eject succeeds");

	like(slurp_result(), qr/eject/, "..and calls chg-test -eject");

        Amanda::MainLoop::quit();
    };

    Amanda::MainLoop::call_later($load_1);
    Amanda::MainLoop::run();
}

## check chg-disk

# Installcheck::Run sets up the whole chg-disk thing for us
$testconf = Installcheck::Run->setup();
$testconf->write();

$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$chg = Amanda::Changer->new();

{
    my ($get_info, $load_current, $label_current, $load_next,
        $release_next, $load_by_label, $check_by_label);

    $get_info = sub {
        $chg->info(info_cb => $load_current, info => [ 'num_slots' ]);
    };

    $load_current = sub {
        my $err = shift;
        my %results = @_;
        die($err) if defined($err);

        is($results{'num_slots'}, 15, "info() returns the correct num_slots");

        $chg->load(slot => "1", res_cb => $label_current);
    };

    $label_current = sub {
        my ($err, $res) = @_;
        die $err if ($err);

        pass("seek to current slot succeeded");

        my $dev = Amanda::Device->new($res->{'device_name'});
        $dev->start($Amanda::Device::ACCESS_WRITE, "TESTCONF18", undef)
            or die $dev->error_or_status();
        $dev->finish()
            or die $dev->error_or_status();

        is($res->{'this_slot'}, "1", "this slot is '1'");
        is($res->{'next_slot'}, "next", "next slot is 'next'");
        $res->set_label(label => "TESTCONF18", finished_cb => $load_next);
    };

    $load_next = sub {
        my ($err) = @_;
        die $err if ($err);

        pass("set_label succeeded");

        $chg->load(slot => "next", res_cb => $release_next);
    };

    $release_next = sub {
        my ($err, $res) = @_;
        die $err if ($err);

        pass("load 'next' succeeded");

        $res->release(finished_cb => $load_by_label);
    };

    $load_by_label = sub {
        my ($err) = @_;
        die $err if ($err);

        pass("release loaded");

        $chg->load(label => "TESTCONF18", res_cb => $check_by_label);
    };

    $check_by_label = sub {
        my ($err, $res) = @_;
        die $err if ($err);

        pass("load by label succeeded");

        my $dev = Amanda::Device->new($res->{'device_name'});
        $dev->read_label() == 0
            or die $dev->error_or_status();

        is($dev->volume_label(), "TESTCONF18",
            "..and finds the right volume");

        Amanda::MainLoop::quit();
    };

    Amanda::MainLoop::call_later($get_info);
    Amanda::MainLoop::run();
}
