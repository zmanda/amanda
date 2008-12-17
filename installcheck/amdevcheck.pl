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

use Test::More tests => 10;

use lib "@amperldir@";
use Installcheck::Config;
use Installcheck::Run qw(run run_get run_err $diskname);
use Amanda::Paths;

my $testconf;

##
# First, try amgetconf out without a config

ok(!run('amdevcheck'),
    "'amdevcheck' with no arguments returns an error exit status");
like($Installcheck::Run::stdout, qr(\AUsage: )i, 
    ".. and gives usage message on stdout");

like(run_err('amdevcheck', 'this-probably-doesnt-exist'), qr(could not open conf file)i, 
    "if the configuration doesn't exist, fail with the correct message");

##
# Next, work against a basically empty config

# this is re-created for each test
$testconf = Installcheck::Config->new();
$testconf->add_param("tapedev", '"/dev/null"');
$testconf->write();

# test some defaults
ok(run('amdevcheck', 'TESTCONF'), "run succeeds with a /dev/null tapedev");
is_deeply([ sort split "\n", $Installcheck::Run::stdout],
	  [ sort "MESSAGE File /dev/null is not a tape device", "DEVICE_ERROR"],
	  "Fail with correct message for a /dev/null tapedev");

##
# Now use a config with a vtape

# this is re-created for each test
$testconf = Installcheck::Run::setup();
$testconf->add_param('label_new_tapes', '"TESTCONF%%"');
$testconf->add_dle("localhost $diskname installcheck-test");
$testconf->write();

ok(run('amdevcheck', 'TESTCONF'), "run succeeds with an unlabeled tape");
is_deeply([ sort split "\n", $Installcheck::Run::stdout],
	  [ sort "MESSAGE Error loading device header -- unlabeled volume?", "VOLUME_UNLABELED", "DEVICE_ERROR", "VOLUME_ERROR"],
	  "..and output is correct");

ok(run('amdevcheck', 'TESTCONF', "/dev/null"),
    "can override device on the command line");
is_deeply([ sort split "\n", $Installcheck::Run::stdout],
	  [ sort "MESSAGE File /dev/null is not a tape device", "DEVICE_ERROR"],
    ".. and produce a corresponding error message");

BAIL_OUT("amdump failed")
    unless run('amdump', 'TESTCONF');

is_deeply([ sort split "\n", run_get('amdevcheck', 'TESTCONF') ],
	  [ sort "SUCCESS" ],
    "used vtape described as SUCCESS");

Installcheck::Run::cleanup();
