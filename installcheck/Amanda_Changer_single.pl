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

use Test::More tests => 4;
use File::Path;
use strict;

use lib "@amperldir@";
use Installcheck::Config;
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

my $testconf = Installcheck::Config->new();
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

my $chg = Amanda::Changer->new("chg-single:tape:/foo");

{
    my ($held_res);
    my ($get_info, $get_res, $got_res, $got_second_res);

    $get_info = sub {
        $chg->info(info_cb => $get_res, info => [ 'num_slots' ]);
    };

    $get_res = sub {
        my $err = shift;
        my %results = @_;
        die($err) if defined($err);

        is($results{'num_slots'}, 1, "info() returns the correct num_slots");

	$chg->load(slot => "current",
		   res_cb => $got_res);
    };

    $got_res = sub {
	my ($err, $res) = @_;
	ok(!$err, "no error loading slot 'current'")
	    or diag($err);
	is($res->{'device_name'}, 'tape:/foo',
	    "returns correct device name");

	$held_res = $res; # hang onto it while loading another slot

	$chg->load(label => "FOO!",
		   res_cb => $got_second_res);
    };

    $got_second_res = sub {
	my ($err, $res) = @_;
	ok($err, "second simultaneous reservation rejected");

	Amanda::MainLoop::quit();
    };

    # start the loop
    Amanda::MainLoop::call_later($get_info);
    Amanda::MainLoop::run();
}
