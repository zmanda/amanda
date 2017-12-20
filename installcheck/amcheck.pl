# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 12;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Config;
use Installcheck::Dumpcache;
use Installcheck::Run qw(run run_get run_err $diskname);
use Amanda::Debug;
use Amanda::Paths;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;

##
# First, try amgetconf out without a config

ok(!run('amcheck'),
    "amcheck with no arguments returns an error exit status");
like($Installcheck::Run::stdout, qr/\AUSAGE:/i,
    ".. and gives usage message");

like(run_err('amcheck', 'this-probably-doesnt-exist'),
    qr(could not open conf file)i,
    "run with non-existent config fails with an appropriate error message.");

##
# Now use a fresh new config

$testconf = Installcheck::Run::setup();
$testconf->add_param("autolabel", "\"TESTCONF%%\" empty volume_error");
$testconf->add_dle("localhost $diskname installcheck-test");
$testconf->write();

like(run_get('amcheck', 'TESTCONF'),
    qr/Amanda Tape Server Host Check/,
    "amcheck with a new config succeeds");
like(run_get('amcheck', '-s', 'TESTCONF'),
    qr/Amanda Tape Server Host Check/,
    "amcheck -s works");

like(run_get('amcheck', '-l', 'TESTCONF'),
    qr/Amanda Tape Server Host Check/,
    "amcheck -l works");

like(run_get('amcheck', '-t', 'TESTCONF'),
    qr/Amanda Tape Server Host Check/,
    "amcheck -t works");

like(run_get('amcheck', '-c', 'TESTCONF'),
    qr/Amanda Backup Client Hosts Check/,
    "amcheck -c works");

like(run_get('amcheck', '-c', 'TESTCONF', 'localhost', "$diskname"),
    qr/Amanda Backup Client Hosts Check/,
    "amcheck -c works with a hostname and diskname");

ok(!run('amcheck', '-o', 'autolabel=', 'TESTCONF'),
    "amcheck -o configoption works");

# do this after the other tests, above, since it writes to the tape
like(run_get('amcheck', '-sw', 'TESTCONF'),
    qr/Volume 'TESTCONF01' is writeable/,
    "amcheck -w works (with -s)");

Installcheck::Dumpcache::load("basic");
ok(run('amcheck', 'TESTCONF'),
    "amcheck on a cached dump run works");

Installcheck::Run::cleanup();
