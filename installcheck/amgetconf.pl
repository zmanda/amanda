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

use Test::More tests => 48;

use lib "@amperldir@";
use Installcheck::Config;
use Installcheck::Run qw(run run_get run_err);
use Amanda::Paths;
use Cwd;

# this is re-created for each test
my $testconf;

##
# First, try amgetconf out without a config

ok(!run('amgetconf'), "bare amgetconf fails");
like($Installcheck::Run::stdout, qr(\AUsage: )i, 
    ".. and gives usage message on stdout");
like(run_err('amgetconf', 'this-probably-doesnt-exist', 'tapedev'),
    qr(could not open conf file)i, 
    "error message when configuration parameter doesn't exist");

##
# Next, work against a basically empty config

$testconf = Installcheck::Config->new();
$testconf->write();

# test some defaults
is(run_get('amgetconf', 'TESTCONF', "reserve"), "100", 
    "reserve defaults to 100");
is(run_get('amgetconf', 'TESTCONF', "tapelist"), "tapelist", 
    "tapelist defaults to 'tapelist'");
is(run_get('amgetconf', 'TESTCONF', "usetimestamps"), "yes", 
    "usetimestamps defaults to 'yes'");

# test command-line parsing
is(run_get('amgetconf', 'TESTCONF', '-o', 'reserve=50', 'reserve'), "50",
    "-o reserve=50");
is(run_get('amgetconf', 'TESTCONF', '-oreserve=50', 'reserve'), "50",
    "-oreserve=50");
is(run_get('amgetconf', '-o', 'reserve=50', 'TESTCONF', 'reserve'), "50",
    "-oreserve=50 before config name");
is(run_get('amgetconf', 'TESTCONF', 'reserve', 'a', 'table', 'for', 'two', '-o', 'reserve=50'), "50",
    "extra command-line arguments are ignored");

# test a nonexistent parameter
like(run_err('amgetconf', 'TESTCONF', "foos_per_bar"), qr/no such parameter/, 
    "handles nonexistent parameters as an error");
like(run_err('amgetconf', 'TESTCONF', "build.foos_per_bar"), qr/no such parameter/, 
    "handles nonexistent build parameters");

# Test build parameters that we can determine easily.  Testing all parameters
# would be more of a maintenance bother than a help.
is(run_get('amgetconf', 'TESTCONF', "build.bindir"), $bindir,
    "build.bindir is correct");
is(run_get('amgetconf', 'TESTCONF', "build.sbindir"), $sbindir,
    "build.sbindir is correct");
is(run_get('amgetconf', 'TESTCONF', "build.libexecdir"), $libexecdir,
    "build.libexecdir is correct");
is(run_get('amgetconf', 'TESTCONF', "build.amlibexecdir"), $amlibexecdir,
    "build.amlibexecdir is correct");
is(run_get('amgetconf', 'TESTCONF', "build.mandir"), $mandir,
    "build.mandir is correct");
is(run_get('amgetconf', 'TESTCONF', "build.AMANDA_DBGDIR"), $AMANDA_DBGDIR,
    "build.AMANDA_DBGDIR is correct");
is(run_get('amgetconf', 'TESTCONF', "build.AMANDA_TMPDIR"), $AMANDA_TMPDIR,
    "build.AMANDA_TMPDIR is correct");
is(run_get('amgetconf', 'TESTCONF', "build.CONFIG_DIR"), $CONFIG_DIR,
    "build.CONFIG_DIR is correct");
is(run_get('amgetconf', 'TESTCONF', "build.__empty"), "",
    "empty build variables handled correctly");

like(run_err('amgetconf', 'TESTCONF', "build.bogus-param"), qr(no such parameter),
    "bogus build parameters result in an error");

is(run_get('amgetconf', 'TESTCONF', "build.config_dir"), $CONFIG_DIR, 
    "build parameters are case-insensitive");

is(run_get('amgetconf', "build.bindir"), $bindir, "build variables are available without a config");

# dbopen, dbclose
my $dbfile = run_get('amgetconf', 'TESTCONF', "dbopen.foo");
chomp $dbfile;
like($dbfile, qr(^$AMANDA_DBGDIR/server/foo.[0-9]*.debug$),
    "'amgetconf dbopen.foo' returns a proper debug filename");
SKIP: {
    skip "dbopen didn't work, so I'll skip the rest", 2
	unless (-f $dbfile);
    ok(!run('amgetconf', 'TESTCONF', "dbclose.foo"),
	"dbclose without filename fails");
    is(run_get('amgetconf', 'TESTCONF', "dbclose.foo:$dbfile"), $dbfile, 
	"'amgetconf dbclose.foo:<filename>' returns the debug filename");
}

##
# Test an invalid config file

$testconf = Installcheck::Config->new();
$testconf->add_param("foos_per_bar", "10");
$testconf->write();

like(run_err('amgetconf', 'TESTCONF', "foos_per_bar"), qr/errors processing config file/, 
    "gives error on invalid configuration");

##
# Now let's fill in some interesting values

$testconf = Installcheck::Config->new();
$testconf->add_param("reserved-udp-port", '100,200');
$testconf->add_param("printer", '"/dev/lp"');
$testconf->add_param("reserve", '27');
$testconf->write();

is(run_get('amgetconf', 'TESTCONF', "reserved-udp-port"), "100,200", 
    "correctly returns intrange parameters from the file");
is(run_get('amgetconf', 'TESTCONF', "printer"), "/dev/lp", 
    "correctly returns string parameters from the file");
is(run_get('amgetconf', 'TESTCONF', "reserve"), "27", 
    "correctly returns integer parameters from the file");
is(run_get('amgetconf', 'TESTCONF', "rEsErVe"), "27", 
    "is case-insensitive");

# check runs without a config
my $olddir = getcwd();
chdir("$CONFIG_DIR/TESTCONF") or die("Could not 'cd' to TESTCONF directory");
is(run_get('amgetconf', "printer"), "/dev/lp", 
    "uses current directory when no configuration name is given");
chdir($olddir) or die("Could not 'cd' back to my original directory");

##
# device_property can appear multiple times

$testconf = Installcheck::Config->new();
$testconf->add_param("device_property", '"power" "on"');
$testconf->add_param("device_property", '"turbo" "engaged"');
$testconf->write();

is_deeply([sort(split(qr/\n/, run_get('amgetconf', 'TESTCONF', 'device_property')))],
	  [sort('"power" "on"', '"turbo" "engaged"')],
    "device_property can have multiple values");

##
# Subsections

$testconf = Installcheck::Config->new();
$testconf->add_tapetype("cassette", [ length => "32 k" ]);
$testconf->add_tapetype("reel2reel", [ length => "1 M" ]);
$testconf->add_tapetype("scotch", [ length => "500 bytes" ]); # (use a sharpie)
$testconf->add_dumptype("testdump", [ comment => '"testdump-dumptype"' ]);
$testconf->add_interface("testiface", [ use => '10' ]);
$testconf->add_holdingdisk("hd17", [ chunksize => '128' ]);
$testconf->write();

is_deeply([sort(split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'tapetype')))],
	  [sort("cassette", "reel2reel", "scotch", "TEST-TAPE")],
	"--list returns correct set of tapetypes");
is(run_get('amgetconf', 'TESTCONF', 'tapetype:scotch:length'), '500', 
    "returns tapetype parameter correctly");

ok(scalar(grep { $_ eq 'testdump' } 
	split(/\n/, 
	    run_get('amgetconf', 'TESTCONF', '--list', 'dumptype'))),
	"--list returns a test dumptype among the default dumptypes");
is(run_get('amgetconf', 'TESTCONF', 'dumptype:testdump:comment'), 'testdump-dumptype', 
    "returns dumptype parameter correctly");

is_deeply([sort(split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'interface')))], 
          [sort("testiface", "default")],
	"--list returns correct set of interfaces");
is(run_get('amgetconf', 'TESTCONF', 'interface:testiface:use'), '10', 
    "returns interface parameter correctly");

is_deeply([sort(split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'holdingdisk')))], 
	  [sort("hd17")], 
	"--list returns correct set of holdingdisks");
is(run_get('amgetconf', 'TESTCONF', 'holdingdisk:hd17:chunksize'), '128',
    "returns holdingdisk parameter correctly");

like(run_get('amgetconf', 'TESTCONF', '--list', 'build'), qr(.*version.*),
	"'--list build' lists build variables");

# non-existent subsection types, names, and parameters
like(run_err('amgetconf', 'TESTCONF', 'NOSUCHTYPE:testiface:comment'), qr/no such parameter/, 
    "handles bad subsection type");
like(run_err('amgetconf', 'TESTCONF', 'dumptype:NOSUCHDUMP:comment'), qr/no such parameter/, 
    "handles bad dumptype namek");
like(run_err('amgetconf', 'TESTCONF', 'dumptype:testdump:NOSUCHPARAM'), qr/no such parameter/, 
    "handles bad dumptype parameter name");

##
# exclude lists are a bit funny, too

$testconf = Installcheck::Config->new();
$testconf->add_dumptype("testdump", [
    "exclude file optional" => '"f1"', # this optional will have no effect
    "exclude file append" => '"f2"',
    "exclude list" => '"l1"',
    "exclude list append" => '"l2"',
    "include file" => '"ifo"',
    "include list optional" => '"ilo"',
    ]);
$testconf->write();

is_deeply([sort(split(qr/\n/, run_get('amgetconf', 'TESTCONF', 'dumptype:testdump:exclude')))],
	  [sort('FILE "f1" "f2"',
	        'LIST "l1" "l2"')],
    "exclude files and lists displayed correctly; a non-final optional is ignored");

is_deeply([sort(split(qr/\n/, run_get('amgetconf', 'TESTCONF', 'dumptype:testdump:include')))],
	  [sort('FILE OPTIONAL "ifo"',
	        'LIST OPTIONAL "ilo"')],
    "a final 'OPTIONAL' makes the whole include/exclude optional")

