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

use Test::More tests => 5;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
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

my $chg = Amanda::Changer->new("chg-null:");
is($chg->have_inventory(), '', "changer have inventory");
{
    my ($get_info, $check_info, $do_load, $got_res);

    $get_info = make_cb('get_info' => sub {
        $chg->info(info_cb => $check_info, info => [ 'num_slots', 'fast_search' ]);
    });

    $check_info = make_cb('check_info' => sub {
        my $err = shift;
        my %results = @_;
        die($err) if defined($err);

        is_deeply({ %results },
	    { num_slots => 1, fast_search => 1 },
	    "info() returns the correct num_slots and fast_search");

	$do_load->();
    });

    $do_load = make_cb('do_load' => sub {
	$chg->load(relative_slot => "current",
		   res_cb => $got_res);
    });

    $got_res = make_cb('got_res' => sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading relative slot 'current'")
	    or diag($err);
	is($res->{'device'}->device_name, 'null:',
	    "returns correct device name");

	$res->release(finished_cb => sub {
	    Amanda::MainLoop::quit();
	});
    });

    # start the loop
    $get_info->();
    Amanda::MainLoop::run();
}

# eject is not implemented
{
    my $try_eject = make_cb('try_eject' => sub {
        $chg->eject(finished_cb => sub {
	    my ($err, $res) = @_;
	    chg_err_like($err,
		{ type => 'failed', reason => 'notimpl' },
		"eject returns a failed/notimpl error");

	    Amanda::MainLoop::quit();
	});
    });

    $try_eject->();
    Amanda::MainLoop::run();
}

$chg->quit();
