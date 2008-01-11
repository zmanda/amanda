# Copyright (c) 2006 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

use Test::More qw(no_plan);

use Amconfig;
use lib "@amperldir@";
use Amanda::Paths;

# wrapper to call amgetconf and return the results
sub amgetconf {
    # open amgetconf and read from it
    my $cmd = "$sbindir/amgetconf " . join(" ", @_) . " 2>&1";
    my $result = `$cmd`;
    chomp $result;
    return $result;
}

# this is re-created for each test
my $testconf;

##
# First, try amgetconf out without a config

like(amgetconf(), qr(\AUsage: )i, 
    "bare 'amgetconf' gives usage message");
like(amgetconf("this-probably-doesnt-exist"), qr(could not open conf file)i, 
    "error message when configuration parameter doesn't exist");

##
# Next, work against a basically empty config

$testconf = Amconfig->new();
$testconf->write();

# test some defaults
is(amgetconf('TESTCONF', "reserve"), "100", 
    "reserve defaults to 100");
is(amgetconf('TESTCONF', "tapelist"), "tapelist", 
    "tapelist defaults to 'tapelist'");
is(amgetconf('TESTCONF', "usetimestamps"), "yes", 
    "usetimestamps defaults to 'yes'");

# test a nonexistent parameter
like(amgetconf('TESTCONF', "foos_per_bar"), qr/no such parameter/, 
    "handles nonexistent parameters");

# test build parameters (just the most common)
is(amgetconf('TESTCONF', "build.bindir"), $bindir, "build.bindir is correct");
is(amgetconf('TESTCONF', "build.sbindir"), $sbindir, "build.sbindir is correct");
is(amgetconf('TESTCONF', "build.amlibexecdir"), $amlibexecdir, "build.amlibexecdir is correct");
is(amgetconf('TESTCONF', "build.mandir"), $mandir, "build.mandir is correct");
is(amgetconf('TESTCONF', "build.AMANDA_DBGDIR"), $AMANDA_DBGDIR, "build.AMANDA_DBGDIR is correct");
is(amgetconf('TESTCONF', "build.AMANDA_TMPDIR"), $AMANDA_TMPDIR, "build.AMANDA_TMPDIR is correct");
is(amgetconf('TESTCONF', "build.CONFIG_DIR"), $CONFIG_DIR, "build.CONFIG_DIR is correct");

# dbopen, dbclose
my $dbfile = amgetconf('TESTCONF', "dbopen.foo");
like($dbfile, qr(^$AMANDA_DBGDIR/server/foo.[0-9]*.debug$),
    "'amgetconf dbopen.foo' returns a proper debug filename");
ok(-f $dbfile,
    "'amgetconf dbopen.foo' creates the debug file");
like(amgetconf('TESTCONF', "dbclose.foo"), qr/cannot parse/,
    "dbclose without filename fails");
is(amgetconf('TESTCONF', "dbclose.foo:$dbfile"), $dbfile, 
    "'amgetconf dbclose.foo:<filename>' returns the debug filename");

##
# Test an invalid config file

$testconf = Amconfig->new();
$testconf->add_param("foos_per_bar", "10");
$testconf->write();

like(amgetconf('TESTCONF', "foos_per_bar"), qr/errors processing config file/, 
    "gives error on invalid configuration");

##
# Now let's fill in some interesting values

$testconf = Amconfig->new();
$testconf->add_param("reserved-udp-port", '100,200');
$testconf->add_param("printer", '"/dev/lp"');
$testconf->add_param("reserve", '27');
$testconf->write();

is(amgetconf('TESTCONF', "reserved-udp-port"), "100,200", 
    "correctly returns intrange parameters from the file");
is(amgetconf('TESTCONF', "printer"), "/dev/lp", 
    "correctly returns string parameters from the file");
is(amgetconf('TESTCONF', "reserve"), "27", 
    "correctly returns integer parameters from the file");
is(amgetconf('TESTCONF', "rEsErVe"), "27", 
    "is case-insensitive");

##
# device_property can appear multiple times

$testconf = Amconfig->new();
$testconf->add_param("device_property", '"power" "on"');
$testconf->add_param("device_property", '"turbo" "engaged"');
$testconf->write();

is_deeply([sort(split(qr/\n/, amgetconf('TESTCONF', 'device_property')))],
	  [sort('"power" "on"', '"turbo" "engaged"')],
    "device_property can have multiple values");

##
# Subsections

$testconf = Amconfig->new();
$testconf->add_tapetype("cassette", [ length => "32 k" ]);
$testconf->add_tapetype("reel2reel", [ length => "1 M" ]);
$testconf->add_tapetype("scotch", [ length => "500 bytes" ]); # (use a sharpie)
$testconf->add_dumptype("testdump", [ comment => '"testdump-dumptype"' ]);
$testconf->add_interface("testiface", [ use => '10' ]);
$testconf->add_holdingdisk("hd17", [ chunksize => '128' ]);
$testconf->write();

is_deeply([sort(split(/\n/, amgetconf('TESTCONF', '--list', 'tapetype')))],
	  [sort("cassette", "reel2reel", "scotch", "TEST-TAPE")],
	"--list returns correct set of tapetypes");
is(amgetconf('TESTCONF', 'tapetype:scotch:length'), '500', 
    "returns tapetype parameter correctly");

ok(grep { $_ eq 'testdump' } split(/\n/, amgetconf('TESTCONF', '--list', 'dumptype')),
	"--list returns a test dumptype among the default dumptypes");
is(amgetconf('TESTCONF', 'dumptype:testdump:comment'), 'testdump-dumptype', 
    "returns dumptype parameter correctly");

is_deeply([sort(split(/\n/, amgetconf('TESTCONF', '--list', 'interface')))], 
          [sort("testiface", "default")],
	"--list returns correct set of interfaces");
is(amgetconf('TESTCONF', 'interface:testiface:use'), '10', 
    "returns interface parameter correctly");

is_deeply([sort(split(/\n/, amgetconf('TESTCONF', '--list', 'holdingdisk')))], 
	  [sort("hd17")], 
	"--list returns correct set of holdingdisks");
is(amgetconf('TESTCONF', 'holdingdisk:hd17:chunksize'), '128',
    "returns holdingdisk parameter correctly");

# non-existent subsection types, names, and parameters
like(amgetconf('TESTCONF', 'NOSUCHTYPE:testiface:comment'), qr/no such parameter/, 
    "handles bad subsection type");
like(amgetconf('TESTCONF', 'dumptype:NOSUCHDUMP:comment'), qr/no such parameter/, 
    "handles bad dumptype namek");
like(amgetconf('TESTCONF', 'dumptype:testdump:NOSUCHPARAM'), qr/no such parameter/, 
    "handles bad dumptype parameter name");

##
# exclude lists are a bit funny, too

$testconf = Amconfig->new();
$testconf->add_dumptype("testdump", [
    "exclude file optional" => '"f1"', # this optional will have no effect
    "exclude file append" => '"f2"',
    "exclude list" => '"l1"',
    "exclude list append" => '"l2"',
    "include file" => '"ifo"',
    "include list optional" => '"ilo"',
    ]);
$testconf->write();

is_deeply([sort(split(qr/\n/, amgetconf('TESTCONF', 'dumptype:testdump:exclude')))],
	  [sort('FILE "f1" "f2"',
	        'LIST "l1" "l2"')],
    "exclude files and lists displayed correctly; a non-final optional is ignored");

is_deeply([sort(split(qr/\n/, amgetconf('TESTCONF', 'dumptype:testdump:include')))],
	  [sort('FILE OPTIONAL "ifo"',
	        'LIST OPTIONAL "ilo"')],
    "a final 'OPTIONAL' makes the whole include/exclude optional")

