# Unit tests for Amanda::Logfile

use Test::More qw(no_plan);
use Installdirs;
use Amconfig;
use strict;

use lib "@amperldir@";
use Amanda::Config qw( 
    config_init	    :config_init_flags 
    :global	    :confparm_key   
    :tapetype	    :tapetype_key   
    :dumptype	    :dumptype_key   
    :interface	    :interface_key  
    :holdingdisk    :holdingdisk_key

    :dump_holdingdisk_t
    :comp_t
    :encrypt_t
    :strategy_t
    :estimate_t
    :taperalgo_t
);

my $testconf;

##
# Try starting with no configuration at all
ok(config_init(0, ''), "Initialize with no configuration");

##
# Parse up a basic configuration

# 0xA000B000C000 / 1024
my $bignum = '171801575472';
$testconf = Amconfig->new();
$testconf->add_param('reserve', '75');
$testconf->add_param('autoflush', 'yes');
$testconf->add_param('tapedev', '"/dev/foo"');
$testconf->add_param('bumpsize', $bignum);
$testconf->add_param('bumpmult', '1.4');
$testconf->add_param('reserved-udp-port', '100,200');
$testconf->add_param('device_output_buffer_size', $bignum);
$testconf->add_param('taperalgo', 'last');
$testconf->add_param('device_property', '"foo" "bar"');
$testconf->add_param('device_property', '"blue" "car"');
$testconf->add_param('displayunit', '"m"');
$testconf->add_param('debug_auth', '1');
$testconf->add_tapetype('mytapetype', [
    'comment' => '"mine"',
    'length' => '128 M',
]);
$testconf->add_dumptype('mydumptype', [
    'comment' => '"mine"',
    'priority' => 'high',  # == 2
    'bumpsize' => $bignum,
    'bumpmult' => 1.75,
    'starttime' => 1829,
    'holdingdisk' => 'required',
    'compress' => 'client best',
    'encrypt' => 'server',
    'strategy' => 'incronly',
    'comprate' => '0.25,0.75',
    'exclude list' => '"foo" "bar"',
    'exclude list append' => '"true" "star"',
    'exclude file' => '"foolist"',
    'include list' => '"bing" "ting"',
    'include list append' => '"string" "fling"',
    'include file optional' => '"rhyme"',
]);
$testconf->add_interface('inyoface', [
    'comment' => '"mine"',
    'use' => '100',
]);
$testconf->add_interface('inherface', [
    'comment' => '"empty"',
]);
$testconf->add_holdingdisk('hd1', [
    'comment' => '"mine"',
    'directory' => '"/mnt/hd1"',
    'use' => '100M',
    'chunksize' => '1024k',
]);
$testconf->add_holdingdisk('hd2', [
    'comment' => '"empty"',
]);
$testconf->write();

my $cfg_ok = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
ok($cfg_ok, "Load test configuration");

SKIP: {
    skip "error loading config", unless $cfg_ok;

    is($Amanda::Config::config_name, "TESTCONF", 
	"config_name set");
    is($Amanda::Config::config_dir, "$prefix/etc/amanda/TESTCONF", 
	"config_dir set");
    is($Amanda::Config::config_filename,
	"$prefix/etc/amanda/TESTCONF/amanda.conf", 
	"config_filename set");
}

SKIP: { # global parameters
    skip "error loading config", unless $cfg_ok;

    is(getconf($CNF_RESERVE), 75,
	"integer global confparm");
    is(getconf($CNF_BUMPSIZE), 171801575472,
	"am64 global confparm");
    isnt(getconf($CNF_BUMPSIZE), 2883632,
	"am64 isn't truncated to 32 bits");
    is(getconf($CNF_TAPEDEV), "/dev/foo",
	"string global confparm");
    is(getconf($CNF_DEVICE_OUTPUT_BUFFER_SIZE), 171801575472,
	"size global confparm");
    isnt(getconf($CNF_DEVICE_OUTPUT_BUFFER_SIZE), 2883632,
	"size isn't truncated to 32 bits");
    ok(getconf($CNF_AUTOFLUSH),
	"boolean global confparm");
    is(getconf($CNF_TAPERALGO), $Amanda::Config::ALGO_LAST,
	"taperalgo global confparam");
    is_deeply([getconf($CNF_RESERVED_UDP_PORT)], [100,200],
	"intrange global confparm");
    is_deeply(getconf($CNF_DEVICE_PROPERTY),
	      { "foo" => "bar", "blue" => "car" },
	    "proplist global confparm");

    ok(getconf_seen($CNF_TAPEDEV),
	"'tapedev' parm was seen");
    ok(!getconf_seen($CNF_NETUSAGE),
	"'netusage' parm was not seen");
}

SKIP: { # derived values
    skip "error loading config", unless $cfg_ok;

    is(Amanda::Config::getconf_unit_divisor(), 1024, 
	"correct unit divisor (from displayunit -> KB)");
    ok($Amanda::Config::debug_auth, 
	"debug_auth setting reflected in global variable");
    ok(!$Amanda::Config::debug_amandad, 
	"debug_amandad defaults to false");
}

SKIP: { # tapetypes
    skip "error loading config", unless $cfg_ok;
    my $ttyp = lookup_tapetype("mytapetype");
    ok($ttyp, "found mytapetype");
    is(tapetype_getconf($ttyp, $TAPETYPE_COMMENT), 'mine', 
	"tapetype comment");
    is(tapetype_getconf($ttyp, $TAPETYPE_LENGTH), 128 * 1024, 
	"tapetype comment");

    ok(tapetype_seen($ttyp, $TAPETYPE_COMMENT),
	"tapetype comment was seen");
    ok(!tapetype_seen($ttyp, $TAPETYPE_LBL_TEMPL),
	"tapetype lbl_templ was not seen");

    is_deeply([ sort(getconf_list("tapetype")) ],
	      [ sort("mytapetype", "TEST-TAPE") ],
	"getconf_list lists all tapetypes");
}

SKIP: { # dumptypes
    skip "error loading config", unless $cfg_ok;

    my $dtyp = lookup_dumptype("mydumptype");
    ok($dtyp, "found mydumptype");
    is(dumptype_getconf($dtyp, $DUMPTYPE_COMMENT), 'mine', 
	"dumptype string");
    is(dumptype_getconf($dtyp, $DUMPTYPE_PRIORITY), 2, 
	"dumptype priority");
    is(dumptype_getconf($dtyp, $DUMPTYPE_BUMPSIZE), 171801575472,
	"dumptype size");
    isnt(dumptype_getconf($dtyp, $DUMPTYPE_BUMPSIZE), 2883632,
	"dumptype size isn't truncated to 32 bits");
    is(dumptype_getconf($dtyp, $DUMPTYPE_BUMPMULT), 1.75,
	"dumptype real");
    is(dumptype_getconf($dtyp, $DUMPTYPE_STARTTIME), 1829,
	"dumptype time");
    is(dumptype_getconf($dtyp, $DUMPTYPE_HOLDINGDISK), $HOLD_REQUIRED,
	"dumptype holdingdisk");
    is(dumptype_getconf($dtyp, $DUMPTYPE_COMPRESS), $COMP_BEST,
	"dumptype compress");
    is(dumptype_getconf($dtyp, $DUMPTYPE_ENCRYPT), $ENCRYPT_SERV_CUST,
	"dumptype encrypt");
    is(dumptype_getconf($dtyp, $DUMPTYPE_STRATEGY), $DS_INCRONLY,
	"dumptype strategy");
    is_deeply([dumptype_getconf($dtyp, $DUMPTYPE_COMPRATE)], [0.25, 0.75],
	"dumptype comprate");
    is_deeply(dumptype_getconf($dtyp, $DUMPTYPE_INCLUDE),
	{ 'file' => [ 'rhyme' ],
	  'list' => [ 'bing', 'ting', 'string', 'fling' ],
	  'optional' => 1 },
	"dumptype include list");
    is_deeply(dumptype_getconf($dtyp, $DUMPTYPE_EXCLUDE),
	{ 'file' => [ 'foolist' ],
	  'list' => [ 'foo', 'bar', 'true', 'star' ],
	  'optional' => 0 },
	"dumptype exclude list");

    ok(dumptype_seen($dtyp, $DUMPTYPE_EXCLUDE),
	"'exclude' parm was seen");
    ok(!dumptype_seen($dtyp, $DUMPTYPE_RECORD),
	"'record' parm was not seen");

    is_deeply([ sort(getconf_list("dumptype")) ],
	      [ sort(qw(
	        mydumptype
	        NO-COMPRESS COMPRESS-FAST COMPRESS-BEST COMPRESS-CUST
		SRVCOMPRESS BSD-AUTH KRB4-AUTH NO-RECORD NO-HOLD
		NO-FULL
		)) ],
	"getconf_list lists all dumptypes (including defaults)");
}

SKIP: { # interfaces
    skip "error loading config" unless $cfg_ok;
    my $iface = lookup_interface("inyoface");
    ok($iface, "found inyoface");
    is(interface_name($iface), "inyoface",
	"interface knows its name");
    is(interface_getconf($iface, $INTER_COMMENT), 'mine', 
	"interface comment");
    is(interface_getconf($iface, $INTER_MAXUSAGE), 100, 
	"interface maxusage");

    $iface = lookup_interface("inherface");
    ok($iface, "found inherface");
    ok(interface_seen($iface, $INTER_COMMENT),
	"seen set for parameters that appeared");
    ok(!interface_seen($iface, $INTER_MAXUSAGE),
	"seen not set for parameters that did not appear");

    is_deeply([ sort(getconf_list("interface")) ],
	      [ sort('inyoface', 'inherface', 'default') ],
	"getconf_list lists all interfaces (in any order)");
}

SKIP: { # holdingdisks
    skip "error loading config" unless $cfg_ok;
    my $hdisk = lookup_holdingdisk("hd1");
    ok($hdisk, "found hd1");
    is(holdingdisk_name($hdisk), "hd1",
	"hd1 knows its name");
    is(holdingdisk_getconf($hdisk, $HOLDING_COMMENT), 'mine', 
	"holdingdisk comment");
    is(holdingdisk_getconf($hdisk, $HOLDING_DISKDIR), '/mnt/hd1',
	"holdingdisk diskdir (directory)");
    is(holdingdisk_getconf($hdisk, $HOLDING_DISKSIZE), 100*1024, 
	"holdingdisk disksize (use)");
    is(holdingdisk_getconf($hdisk, $HOLDING_CHUNKSIZE), 1024, 
	"holdingdisk chunksize");

    $hdisk = lookup_holdingdisk("hd2");
    ok($hdisk, "found hd2");
    ok(holdingdisk_seen($hdisk, $HOLDING_COMMENT),
	"seen set for parameters that appeared");
    ok(!holdingdisk_seen($hdisk, $HOLDING_CHUNKSIZE),
	"seen not set for parameters that did not appear");

    # only holdingdisks have this linked-list structure
    # exposed
    $hdisk = getconf_holdingdisks();
    like(holdingdisk_name($hdisk), qr/hd[12]/,
	"one disk is first in list of holdingdisks");
    $hdisk = holdingdisk_next($hdisk);
    like(holdingdisk_name($hdisk), qr/hd[12]/,
	"another is second in list of holdingdisks");
    ok(!holdingdisk_next($hdisk),
	"no third holding disk");

    is_deeply([ sort(getconf_list("holdingdisk")) ],
	      [ sort('hd1', 'hd2') ],
	"getconf_list lists all holdingdisks (in any order)");
}

##
# Explore a quirk of exinclude parsing.  Only the last
# exclude (or include) directive affects the 'optional' flag.
# We may want to change this, but we should do so intentionally.

$testconf = Amconfig->new();
$testconf->add_dumptype('mydumptype', [
    'exclude list' => '"foo" "bar"',
    'exclude list optional append' => '"true" "star"',
    'exclude list append' => '"true" "star"',
]);
$testconf->write();

$cfg_ok = config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
SKIP: {
    skip "error loading config", unless $cfg_ok;

    my $dtyp = lookup_dumptype("mydumptype");
    ok($dtyp, "found mydumptype");
    is(dumptype_getconf($dtyp, $DUMPTYPE_EXCLUDE)->{'optional'}, 0,
	"'optional' has no effect when not on the last occurrence");
}

$testconf = Amconfig->new();
$testconf->add_dumptype('mydumptype', [
    'exclude file' => '"foo" "bar"',
    'exclude file optional append' => '"true" "star"',
    'exclude list append' => '"true" "star"',
]);
$testconf->write();

$cfg_ok = config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
SKIP: {
    skip "error loading config", unless $cfg_ok;

    my $dtyp = lookup_dumptype("mydumptype");
    ok($dtyp, "found mydumptype");
    is(dumptype_getconf($dtyp, $DUMPTYPE_EXCLUDE)->{'optional'}, 0,
	"'optional' has no effect when not on the last occurrence of 'file'");
}

# TODO:
# overwrites
# inheritance
# more init
