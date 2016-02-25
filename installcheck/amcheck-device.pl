# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 4;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck::Run qw(run run_get run_err);
use Amanda::Config qw( :init :getconf );
use Amanda::Paths;
use Amanda::Debug;

my $testconf;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

$testconf = Installcheck::Run::setup();
$testconf->add_param("autolabel", "\"TESTCONF%%\" empty volume_error");
$testconf->write();

like(run_get("$amlibexecdir/amcheck-device", "TESTCONF", "TESTCONF"),
    qr/Will write label 'TESTCONF01' to new volume/,
    "a run of amcheck-device on a new config succeeds");

ok(!run("$amlibexecdir/amcheck-device", "TESTCONF", "TESTCONF", "-o", "autolabel="),
    "accepts config_overrides, returns exit status on failure");

like(run_get("$amlibexecdir/amcheck-device", "TESTCONF", "TESTCONF", "-w"),
    qr/Volume 'TESTCONF01' is writeable/,
    "tests for writeability with -w");

$testconf = Installcheck::Run::setup();
$testconf->add_param("autolabel", "\"TESTCONF-\$4s\" empty volume_error");
$testconf->write();

like(run_get("$amlibexecdir/amcheck-device", "TESTCONF", "TESTCONF"),
    qr/Will write label 'TESTCONF-0001' to new volume in slot 1/,
    "a run with incompatible autolabel and labelstr");

