# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 86;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
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
is(run_get('amgetconf', 'TESTCONF', "send_amreport_on"), "ALL",
    "send_amreport_on defaults to 'ALL'"); # (enum value is 0)
is(run_get('amgetconf', 'TESTCONF', "taperalgo"), "FIRST",
    "taperalgo defaults to 'ALL'"); # (enum value is 0)
is(run_get('amgetconf', 'TESTCONF', "printer"), "",
    "printer defaults to empty string, which is not an error");

# test command-line parsing
is(run_get('amgetconf', 'TESTCONF', '--execute-where', 'client', 'amandates'),
   $Amanda::Constants::DEFAULT_AMANDATES_FILE,
    "--execute-where client");
is(run_get('amgetconf', 'TESTCONF', '--execute-where=client', 'amandates'),
   $Amanda::Constants::DEFAULT_AMANDATES_FILE,
    "--execute-where=client");
is(run_get('amgetconf', 'TESTCONF', '--client', 'amandates'),
   $Amanda::Constants::DEFAULT_AMANDATES_FILE,
    "--client");

is(run_get('amgetconf', 'TESTCONF', '--execute-where', 'server', 'reserve'), "100",
    "--execute-where server");
is(run_get('amgetconf', 'TESTCONF', '--execute-where=server', 'reserve'), "100",
    "--execute-where=server");
is(run_get('amgetconf', 'TESTCONF', '--execute-where=server', '--execute-where=server', 'reserve'), "100",
    "--execute-where=server --execute-where=server");
is(run_get('amgetconf', 'TESTCONF', '--execute-where=client', '--execute-where=client', 'amandates'),
   $Amanda::Constants::DEFAULT_AMANDATES_FILE,
    "--execute-where=client --execute-where=client");

like(run_err('amgetconf', 'TESTCONF', '--execute-where=server', '--execute-where=client'),
    qr/conflicts with/,
    "handles conflict --execute-where=server --execute-where=client");
like(run_err('amgetconf', 'TESTCONF', '--execute-where=client', '--execute-where=server'),
    qr/conflicts with/,
    "handles conflict --execute-where=client --execute-where=server");
like(run_err('amgetconf', 'TESTCONF', '--execute-where=server', '--client'),
     qr/conflicts with/,
    "handles conflict --execute-where=server --client");
like(run_err('amgetconf', 'TESTCONF', '--client', '--execute-where=server'),
    qr/conflicts with/, 
    "handles conflict --client --execute-where=server");

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
    "handles nonexistent build parameters as an error");

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
    "build.AMDNA_TMPDIR is correct");
is(run_get('amgetconf', 'TESTCONF', "build.CONFIG_DIR"), $CONFIG_DIR,
    "build.CONFIG_DIR is correct");
is(run_get('amgetconf', 'TESTCONF', "build.__empty"), "",
    "empty build variables handled correctly");

like(run_err('amgetconf', 'TESTCONF', "build.bogus-param"), qr(no such parameter),
    "bogus build parameters result in an error");

is(run_get('amgetconf', 'TESTCONF', "build.config_dir"), $CONFIG_DIR, 
    "build parameters are case-insensitive");

is(run_get('amgetconf', "build.bindir"), $bindir, "build variables are available without a config");

# empty --list should return nothing
is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'holdingdisk')))], [ ],
	"--list returns an empty list when there's nothing to return");

# dbopen, dbclose
my $dbfile = run_get('amgetconf', 'TESTCONF', "dbopen.foo");
chomp $dbfile;
like($dbfile, qr(^\Q$AMANDA_DBGDIR\E/server/foo.[0-9]*.debug$),
    "'amgetconf dbopen.foo' returns a proper debug filename");
SKIP: {
    skip "dbopen didn't work, so I'll skip the rest", 3
	unless (-f $dbfile);
    ok(!run('amgetconf', 'TESTCONF', "dbclose.foo"),
	"dbclose without filename fails");
    is(run_get('amgetconf', 'TESTCONF', "dbclose.foo:$dbfile"), $dbfile, 
	"'amgetconf dbclose.foo:<filename>' returns the debug filename");

    # sometimes shell scripts pass a full path as appname..
    $dbfile = run_get('amgetconf', 'TESTCONF', 'dbopen./sbin/foo');
    like($dbfile, qr(^\Q$AMANDA_DBGDIR\E/server/_sbin_foo.[0-9]*.debug$),
	"'amgetconf dbopen./sbin/foo' doesn't get confused by the slashes");
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
is(run_get('amgetconf', 'TESTCONF', "reserved_udp_port"), "100,200", 
    "treats _ and - identically");

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

is_deeply([sort(+split(qr/\n/, run_get('amgetconf', 'TESTCONF', 'device_property')))],
	  [sort('"power" "on"', '"turbo" "engaged"')],
    "device_property can have multiple values");

##
# Subsections

$testconf = Installcheck::Config->new();
$testconf->add_tapetype("cassette", [ length => "32 k" ]);
$testconf->add_tapetype("reel2reel", [ length => "1 M" ]);
$testconf->add_tapetype("scotch", [ length => "512000 bytes" ]);
$testconf->add_dumptype("testdump", [ comment => '"testdump-dumptype"',
				      auth => '"bsd"' ]);
$testconf->add_dumptype("testdump1", [ inherit => 'testdump' ]);
$testconf->add_interface("testiface", [ use => '10' ]);
$testconf->add_holdingdisk("hd17", [ chunksize => '128' ]);
$testconf->add_application('app_amgtar', [ plugin => '"amgtar"' ]);
$testconf->add_application('app_amstar', [ plugin => '"amstar"' ]);
$testconf->add_script('my_script', [ "execute-on" => 'pre-dle-amcheck', 'plugin' => '"foo"' ]);
$testconf->add_device('my_device', [ "tapedev" => '"foo:/bar"' ]);
$testconf->write();

is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'tapetype')))],
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

is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'interface')))], 
          [sort("testiface", "default")],
	"--list returns correct set of interfaces");
is(run_get('amgetconf', 'TESTCONF', 'interface:testiface:use'), '10', 
    "returns interface parameter correctly");

is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'holdingdisk')))], 
	  [sort("hd17")], 
	"--list returns correct set of holdingdisks");
is(run_get('amgetconf', 'TESTCONF', 'holdingdisk:hd17:chunksize'), '128',
    "returns holdingdisk parameter correctly");

like(run_get('amgetconf', 'TESTCONF', '--list', 'build'), qr(.*version.*),
	"'--list build' lists build variables");

is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'application')))],
          [sort("app_amgtar", "app_amstar")],
        "--list returns correct set of applications");

is(run_get('amgetconf', 'TESTCONF', 'application-tool:app_amgtar:plugin'), 'amgtar',
    "returns application-tool parameter correctly");

is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'script')))],
          [sort("my_script")],
        "--list returns correct set of scripts");

# test the old names
is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'script-tool')))],
          [sort("my_script")],
        "--list returns correct set of scripts, using the name script-tool");

is_deeply([sort(+split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'application-tool')))],
          [sort("app_amgtar", "app_amstar")],
        "--list returns correct set of applications, using the name 'application-tool'");

is(run_get('amgetconf', 'TESTCONF', 'script-tool:my_script:execute-on'), 'PRE-DLE-AMCHECK',
    "returns script-tool parameter correctly");
is(run_get('amgetconf', 'TESTCONF', 'script_tOOl:my_script:execute-on'), 'PRE-DLE-AMCHECK',
    "insensitive to case in subsec_type");
is(run_get('amgetconf', 'TESTCONF', 'script-tool:my_script:execute-on'), 'PRE-DLE-AMCHECK',
    "insensitive to -/_ in subsec_type");
is(run_get('amgetconf', 'TESTCONF', 'script_tOOl:my_script:eXECute-on'), 'PRE-DLE-AMCHECK',
    "insensitive to case in subsec_key");
is(run_get('amgetconf', 'TESTCONF', 'script-tool:my_script:execute_on'), 'PRE-DLE-AMCHECK',
    "insensitive to -/_ in subsec_key");
is(run_get('amgetconf', 'TESTCONF', 'dumptype:testdump1:auth', '-odumptype:testdump:auth=SSH'), 'SSH',
    "inherited setting are overrided");
is(run_get('amgetconf', 'TESTCONF', 'dumptype:testdump1:compress', '-odumptype:testdump:compress=SERVER BEST'), 'SERVER BEST',
    "inherited default are overrided");

is_deeply([sort(split(/\n/, run_get('amgetconf', 'TESTCONF', '--list', 'device')))],
          [sort("my_device")],
        "--list returns correct set of devices");

is(run_get('amgetconf', 'TESTCONF', 'device:my_device:tapedev'), 'foo:/bar',
    "returns device parameter correctly");

# non-existent subsection types, names, and parameters
like(run_err('amgetconf', 'TESTCONF', 'NOSUCHTYPE:testiface:comment'), qr/no such parameter/, 
    "handles bad subsection type");
like(run_err('amgetconf', 'TESTCONF', 'dumptype:NOSUCHDUMP:comment'), qr/no such parameter/, 
    "handles bad dumptype namek");
like(run_err('amgetconf', 'TESTCONF', 'dumptype:testdump:NOSUCHPARAM'), qr/no such parameter/, 
    "handles bad dumptype parameter name");
like(run_err('amgetconf', 'TESTCONF', 'application-tool:app_amgtar:NOSUCHPARAM'), qr/no such parameter/, 
    "handles bad application-tool parameter name");
like(run_err('amgetconf', 'TESTCONF', 'script-tool:my-script:NOSUCHPARAM'), qr/no such parameter/, 
    "handles bad script-tool parameter name");

like(run_err('amgetconf', 'TESTCONF', '--list', 'frogs'), qr/no such parameter/,
        "--list fails given an invalid subsection name");

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

is_deeply([sort(+split(qr/\n/, run_get('amgetconf', 'TESTCONF', 'dumptype:testdump:exclude')))],
	  [sort('FILE "f1" "f2"',
	        'LIST "l1" "l2"')],
    "exclude files and lists displayed correctly; a non-final optional is ignored");

is_deeply([sort(+split(qr/\n/, run_get('amgetconf', 'TESTCONF', 'dumptype:testdump:include')))],
	  [sort('FILE OPTIONAL "ifo"',
	        'LIST OPTIONAL "ilo"')],
    "a final 'OPTIONAL' makes the whole include/exclude optional");

$testconf = Installcheck::Config->new();
$testconf->add_param("property", '"prop1" "value1"');
$testconf->add_param("property", '"prop2" "value2"');
$testconf->add_param("property", '"prop3" "value3"');
$testconf->write();

is(run_get('amgetconf', 'TESTCONF', "property:prop1"), "value1", 
    "correctly returns property prop1 from the file");
is(run_get('amgetconf', 'TESTCONF', "property:prop2"), "value2", 
    "correctly returns property prop2 from the file");
is(run_get('amgetconf', 'TESTCONF', "property:prop3"), "value3", 
    "correctly returns property prop3 from the file");
is(run_get('amgetconf', 'TESTCONF', "property"), "\"prop1\" \"value1\"\n\"prop2\" \"value2\"\n\"prop3\" \"value3\"", 
    "correctly returns all propertiss from the file");

