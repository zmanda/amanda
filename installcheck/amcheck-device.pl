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

use Test::More tests => 4;
use strict;
use warnings;

use lib "@amperldir@";
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

like(run_get("$amlibexecdir/amcheck-device", "TESTCONF"),
    qr/Will write label 'TESTCONF01' to new volume/,
    "a run of amcheck-device on a new config succeeds");

ok(!run("$amlibexecdir/amcheck-device", "TESTCONF", "-o", "autolabel="),
    "accepts config_overrides, returns exit status on failure");

like(run_get("$amlibexecdir/amcheck-device", "TESTCONF", "-w"),
    qr/Volume 'TESTCONF01' is writeable/,
    "tests for writeability with -w");

$testconf = Installcheck::Run::setup();
$testconf->add_param("autolabel", "\"TESTCONF-\$4s\" empty volume_error");
$testconf->write();

like(run_err("$amlibexecdir/amcheck-device", "TESTCONF"),
    qr/ERROR: Newly-generated label 'TESTCONF-0001' does not match labelstr 'TESTCONF\[0-9\]\[0-9\]'/m,
    "a run with incompatible autolabel and labelstr");

