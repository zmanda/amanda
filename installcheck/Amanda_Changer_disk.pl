# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 16;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Changer;
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

my $taperoot = "$Installcheck::TMP/Amanda_Changer_Disk_test";

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
}

sub is_pointing_to {
    my ($res, $slot, $msg) = @_;

    my ($datalink) = ($res->{'device'}->device_name =~ /file:(.*)/);
    $datalink .= "/data";
    is(readlink($datalink), "../slot$slot", $msg);
}

# Build a configuration that specifies Amanda::Changer::Disk
my $testconf = Installcheck::Config->new();
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

reset_taperoot(5);

# first try an error
my $chg = Amanda::Changer->new("chg-disk:$taperoot/foo");
chg_err_like($chg,
    { message => qr/directory '.*' does not exist/,
      type => 'fatal' },
    "detects nonexistent directory");

$chg = Amanda::Changer->new("chg-disk:$taperoot");
die($chg) if $chg->isa("Amanda::Changer::Error");
{
    my @slots = ( 1, 3, 5 );
    my @reservations = ();
    my $getres;

    $getres = make_cb('getres' => sub {
	my $slot = pop @slots;

	$chg->load(slot => $slot,
                   set_current => ($slot == 5),
		   res_cb => make_cb(sub {
	    my ($err, $reservation) = @_;
	    ok(!$err, "no error loading slot $slot")
		or diag($err);

	    # keep this reservation
	    if ($reservation) {
		push @reservations, $reservation;
	    }

	    # and start on the next
	    if (@slots) {
		$getres->();
		return;
	    } else {
		# try to load an already-reserved slot
		$chg->load(slot => 3,
			   res_cb => sub {
		    my ($err, $reservation) = @_;
		    chg_err_like($err,
			{ message => qr/Slot 3 is already in use by drive/,
			  type => 'failed',
			  reason => 'inuse' },
			"error when requesting already-reserved slot");
		    Amanda::MainLoop::quit();
		});
	    }
	}));
    });

    # start the loop
    $getres->();
    Amanda::MainLoop::run();

    # ditch the reservations and do it all again
    @reservations = ();
    @slots = ( 4, 2, 3 );
    $getres->();
    Amanda::MainLoop::run();

    @reservations = ();
}

# check "current" and "next" functionality
{
    # load the "current" slot, which should be 3
    my ($load_current, $check_current_cb, $check_next_cb, $reset_finished_cb, $check_reset_cb);

    $load_current = make_cb('load_current' => sub {
	$chg->load(slot => "current", res_cb => $check_current_cb);
    });

    $check_current_cb = make_cb('check_current_cb' => sub {
        my ($err, $res) = @_;
        die $err if $err;

        is_pointing_to($res, 5, "'current' is slot 5");

        $chg->load(slot => $res->{'next_slot'}, res_cb => $check_next_cb);
    });

    $check_next_cb = make_cb('check_next_cb' => sub {
        my ($err, $res) = @_;
        die $err if $err;

        is_pointing_to($res, 1, "'next' from there is slot 1");

        $chg->reset(finished_cb => $reset_finished_cb);
    });

    $reset_finished_cb = make_cb('reset_finished_cb' => sub {
        my ($err) = @_;
        die $err if $err;

	$chg->load(slot => "current", res_cb => $check_reset_cb);
    });

    $check_reset_cb = make_cb('check_reset_cb' => sub {
        my ($err, $res) = @_;
        die $err if $err;

        is_pointing_to($res, 1, "after reset, 'current' is slot 1");

        Amanda::MainLoop::quit();
    });

    $load_current->();
    Amanda::MainLoop::run();
}

# test loading slot "next"
{
    my $load_next = make_cb('load_next' => sub {
        $chg->load(slot => "next",
            res_cb => sub {
                my ($err, $res) = @_;
                die $err if $err;

                is_pointing_to($res, 2, "loading slot 'next' loads the correct slot");

                Amanda::MainLoop::quit();
            }
        );
    });

    $load_next->();
    Amanda::MainLoop::run();
}

# eject is not implemented
{
    my $try_eject = make_cb('try_eject' => sub {
        $chg->eject(finished_cb => make_cb(sub {
	    my ($err, $res) = @_;
	    chg_err_like($err,
		{ type => 'failed', reason => 'notimpl' },
		"eject returns a failed/notimpl error");

	    Amanda::MainLoop::quit();
	}));
    });

    $try_eject->();
    Amanda::MainLoop::run();
}

# check num_slots and loading by label
{
    my ($get_info, $load_label, $check_load_cb) = @_;

    $get_info = make_cb('get_info' => sub {
        $chg->info(info_cb => $load_label, info => [ 'num_slots', 'fast_search' ]);
    });

    $load_label = make_cb('load_label' => sub {
        my $err = shift;
        my %results = @_;
        die($err) if defined($err);

        is_deeply({ %results },
	    { num_slots => 5, fast_search => 1 },
	    "info() returns the correct num_slots and fast_search");

        # note use of a glob metacharacter in the label name
        $chg->load(label => "FOO?BAR", res_cb => $check_load_cb);
    });

    $check_load_cb = make_cb('check_load_cb' => sub {
        my ($err, $res) = @_;
        die $err if $err;

        is_pointing_to($res, 4, "labeled volume found in slot 4");

        Amanda::MainLoop::quit();
    });

    # label slot 4, using our own symlink
    mkpath("$taperoot/tmp");
    symlink("../slot4", "$taperoot/tmp/data") or die "While symlinking: $!";
    my $dev = Amanda::Device->new("file:$taperoot/tmp");
    $dev->start($Amanda::Device::ACCESS_WRITE, "FOO?BAR", undef)
        or die $dev->error_or_status();
    $dev->finish()
        or die $dev->error_or_status();
    rmtree("$taperoot/tmp");

    $get_info->();
    Amanda::MainLoop::run();
}

rmtree($taperoot);
