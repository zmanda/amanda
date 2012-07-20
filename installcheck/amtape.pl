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

use Test::More tests => 46;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Installcheck::Run qw(run run_err run_get load_vtape vtape_dir);
use Amanda::Device qw( :constants );
use Amanda::Config qw( :init :getconf );
use Amanda::Paths;
use Amanda::Debug;
use Amanda::Tapelist;

my $testconf;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

$testconf = Installcheck::Run::setup();
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    BAIL_OUT("config errors");
}

# label slot 2 with "MyTape", slot 3 with "TESTCONF13", and add
# the latter to the tapelist
sub setup_vtapes {
    my ($devdir, $dev);

    $devdir = load_vtape(2);
    $dev = Amanda::Device->new("file:$devdir");
    ($dev && $dev->status == $DEVICE_STATUS_SUCCESS)
        or BAIL_OUT("device error");

    $dev->start($ACCESS_WRITE, "MyTape", undef)
        or BAIL_OUT("device error");
    $dev->finish()
        or BAIL_OUT("device error");


    $devdir = load_vtape(3);
    $dev = Amanda::Device->new("file:$devdir");
    ($dev && $dev->status == $DEVICE_STATUS_SUCCESS)
        or BAIL_OUT("device error");

    $dev->start($ACCESS_WRITE, "TESTCONF13", undef)
        or BAIL_OUT("device error");
    $dev->finish()
        or BAIL_OUT("device error");

    my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
    my $tl = Amanda::Tapelist->new($tlf, 1);
    $tl->add_tapelabel("0", "TESTCONF13", "test tape");
    $tl->write($tlf);
}

like(run_err('amtape'),
    qr/^Usage:/,
    "bare 'amtape' gives usage message");

# in general, output goes to stderr, so we can't use run_get.  These checks
# accomplish two things: ensure that amtape can invoke the changer functions
# correctly (and not generate an error), and ensure that it gives the
# appropriate responses.  It does not exercise the changer itself -- most
# of these operations are meaningless for vtapes, anyway.

setup_vtapes();

ok(run('amtape', 'TESTCONF', 'reset'),
    "'amtape TESTCONF reset'");
like($Installcheck::Run::stderr,
    qr/changer is reset/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'eject'),
    "'amtape TESTCONF eject'");
like($Installcheck::Run::stderr,
    qr/drive ejected/,
    "..result correct");

# TODO: chg-disk doesn't support "clean"

ok(run('amtape', 'TESTCONF', 'slot', '2'),
    "'amtape TESTCONF slot 2'");
like($Installcheck::Run::stderr,
    qr/changed to slot 2/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'slot', 'current'),
    "'amtape TESTCONF slot current'");
like($Installcheck::Run::stderr,
    qr/changed to slot 2/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'slot', 'next'),
    "'amtape TESTCONF slot next'");
like($Installcheck::Run::stderr,
    qr/changed to slot 3/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'slot', 'next'),
    "'amtape TESTCONF slot next'");
like($Installcheck::Run::stderr,
    qr/changed to slot 1/, # loop around to slot 1
    "..result correct");

ok(run('amtape', 'TESTCONF', 'label', 'MyTape'),
    "'amtape TESTCONF label MyTape'");
like($Installcheck::Run::stderr,
    qr/slot +2:.*label MyTape/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'current'),
    "'amtape TESTCONF current'");
like($Installcheck::Run::stderr,
    qr/slot +2:.*label MyTape/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'update'),
    "'amtape TESTCONF update'");
like($Installcheck::Run::stderr,
    qr/update complete/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'show'),
    "'amtape TESTCONF show'");
like($Installcheck::Run::stderr,
    qr/slot +2:.*label MyTape\nslot +3/,
    "'amtape TESTCONF show' ..result correct");

ok(run('amtape', 'TESTCONF', 'show', '2'),
    "'amtape TESTCONF show'");
like($Installcheck::Run::stderr,
    qr/^slot +2:.*label MyTape$/,
    "'amtape TESTCONF show 2' ..result correct");

ok(run('amtape', 'TESTCONF', 'show', '1,3'),
    "'amtape TESTCONF show'");
like($Installcheck::Run::stderr,
    qr/^slot +1: unlabeled volume\nslot +3: date \d{14} label TESTCONF13$/,
#    qr/slot +1: unlabeled volume\nslot +3: date 20111121133419 label TESTCONF13/,
    "'amtape TESTCONF show 1,3' ..result correct");

ok(run('amtape', 'TESTCONF', 'taper'),
    "'amtape TESTCONF taper'");
like($Installcheck::Run::stderr,
    qr/Will write to volume 'TESTCONF13' in slot 3/,
    "'amtape TESTCONF taper' ..result correct");

###
## shift to using the new Amanda::Changer::disk

$testconf->remove_param("tapedev");
$testconf->remove_param("tpchanger");
$testconf->add_param("tpchanger", "\"chg-disk:" . vtape_dir(). "\"");
$testconf->write();

setup_vtapes();

ok(run('amtape', 'TESTCONF', 'reset'),
    "'amtape TESTCONF reset'");
like($Installcheck::Run::stderr,
    qr/changer is reset/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'slot', '2'),
    "'amtape TESTCONF slot 2'");
like($Installcheck::Run::stderr,
    qr/changed to slot 2/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'slot', 'current'),
    "'amtape TESTCONF slot current'");
like($Installcheck::Run::stderr,
    qr/changed to slot 2/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'slot', 'next'),
    "'amtape TESTCONF slot next'");
like($Installcheck::Run::stderr,
    qr/changed to slot 3/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'label', 'MyTape'),
    "'amtape TESTCONF label MyTape'");
like($Installcheck::Run::stderr,
    qr/label MyTape is now loaded from slot 2/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'current'),
    "'amtape TESTCONF current'");
like($Installcheck::Run::stderr,
    qr/slot +2:.*label MyTape/,
    "..result correct");

like(run_err('amtape', 'TESTCONF', 'update'),
    qr/does not support update/,
    "'amtape TESTCONF update' fails gracefully");

ok(run('amtape', 'TESTCONF', 'show'),
    "'amtape TESTCONF show'");
like($Installcheck::Run::stderr,
    qr/slot +2:.*label MyTape\nslot +3/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'inventory'),
    "'amtape TESTCONF inventory'");
like($Installcheck::Run::stdout,
    qr/slot +1: blank\nslot +2: label MyTape \(current\)\nslot +3/,
    "..result correct");

ok(run('amtape', 'TESTCONF', 'taper'),
    "'amtape TESTCONF taper'");
like($Installcheck::Run::stderr,
    qr/Will write to volume 'TESTCONF13' in slot 3/,
    "'amtape TESTCONF taper' ..result correct");

Installcheck::Run::cleanup();
