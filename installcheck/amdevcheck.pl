# Copyright (c) 2007,2008 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 17;

use lib "@amperldir@";
use Installcheck::Config;
use Installcheck::Run qw(run run_get run_err $diskname);
use Installcheck::Dumpcache;
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

ok(run('amdevcheck', 'TESTCONF', "--properties"),
    "can list properties with --properties option");

ok(run('amdevcheck', 'TESTCONF', "--properties", "BLOCK_SIZE"),
    "check block_size property value");
is_deeply([ sort split "\n", $Installcheck::Run::stdout],
	  [ sort "BLOCK_SIZE=32768"],
    ".. and confirm it is default value");

ok(run('amdevcheck', 'TESTCONF', "--properties", "CANONICAL_NAME"),
    "check canonical_name property value");
is_deeply([ sort split "\n", $Installcheck::Run::stdout],
	  [ sort "CANONICAL_NAME=file:" . Installcheck::Run::vtape_dir() ],
    ".. and confirm it is set to default value");

ok(run('amdevcheck', 'TESTCONF', "--properties", "BLOCK_SIZE,CANONICAL_NAME"),
    "check a list of properties");
is_deeply([ sort split "\n", $Installcheck::Run::stdout],
	  [ sort "BLOCK_SIZE=32768",
	  	 "CANONICAL_NAME=file:" . Installcheck::Run::vtape_dir() ],
    ".. with correct results");

ok(run('amdevcheck', 'TESTCONF', '/dev/null'),
    "can override device on the command line");
is_deeply([ sort split "\n", $Installcheck::Run::stdout],
	  [ sort "MESSAGE File /dev/null is not a tape device", "DEVICE_ERROR"],
    ".. and produce a corresponding error message");

Installcheck::Dumpcache::load("basic");

is_deeply([ sort split "\n", run_get('amdevcheck', 'TESTCONF') ],
	  [ sort "SUCCESS" ],
    "used vtape described as SUCCESS");

Installcheck::Run::cleanup();
