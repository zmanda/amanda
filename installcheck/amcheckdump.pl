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

use Test::More tests => 8;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck::Config;
use Installcheck::Dumpcache;
use Installcheck::Run qw(run run_get run_err $diskname);
use Amanda::Paths;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();


my $testconf;

##
# First, try amgetconf out without a config

ok(!run('amcheckdump'),
    "amcheckdump with no arguments returns an error exit status");
like($Installcheck::Run::stdout, qr/\AUSAGE:/i, 
    ".. and gives usage message");

like(run_err('amcheckdump', 'this-probably-doesnt-exist'), qr(could not open conf file)i, 
    "run with non-existent config fails with an appropriate error message.");

##
# Now use a config with a vtape and without usetimestamps

$testconf = Installcheck::Run::setup();
$testconf->write();

ok(!run('amcheckdump', 'TESTCONF'),
    "amcheckdump with a new config succeeds");
like($Installcheck::Run::stdout, qr(No matching dumps found)i,
     "..but finds no dumps.");

##
# and check command-line handling

Installcheck::Dumpcache::load("basic");

like(run_get('amcheckdump', 'TESTCONF', '-oorg=installcheck'), qr(Validating),
    "amcheckdump accepts '-o' options on the command line");

##
# Try with usetimestamps enabled

like(run_get('amcheckdump', 'TESTCONF'), qr(Validating),
    "amcheckdump succeeds, claims to validate something (usetimestamps=yes)");

##
# now try zeroing out the dumps

my $vtape1 = Installcheck::Run::vtape_dir(1);
opendir(my $vtape_dir, $vtape1) || die "can't opendir $vtape1: $!";
my @dump1 = grep { /^0+1/ } readdir($vtape_dir);
closedir $vtape_dir;

for my $dumpfile (@dump1) {
    open(my $dumpfh, "+<", "$vtape1/$dumpfile");
    sysseek($dumpfh, 32768, 0); # jump past the header
    syswrite($dumpfh, "\0" x 100); # and write some zeroes
    close($dumpfh);
}

ok(!run('amcheckdump', 'TESTCONF'),
    "amcheckdump detects a failure from a zeroed-out dumpfile");

Installcheck::Run::cleanup();
