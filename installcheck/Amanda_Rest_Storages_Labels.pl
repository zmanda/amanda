# Copyright (c) 2014 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More;
use File::Path;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

eval 'use Installcheck::Rest;';
if ($@) {
    plan skip_all => "Can't load Installcheck::Rest: $@";
    exit 1;
}

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $rest = Installcheck::Rest->new();
if ($rest->{'error'}) {
   plan skip_all => "Can't start JSON Rest server: see " . Amanda::Debug::dbfn();
   exit 1;
}
plan tests => 17;

my $reply;

my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

my $taperoot = "$Installcheck::TMP/Amanda_Changer_Diskflat_test";
mkdir $taperoot;

$testconf = Installcheck::Run::setup();
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");

#CODE 1600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKLFAT/labels");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'tles' => [],
		'severity' => '16',
		'message' => 'List of labels',
		'code' => '1600001'
	  },
        ],
      http_code => 200,
    },
    "No config") || diag("reply: " . Data::Dumper::Dumper($reply));

my $tapelist_data = <<EOF;
20140527000001 DISKFLAT-001 reuse BLOCKSIZE:32 POOL:DISKFLAT STORAGE:DISKFLAT CONFIG:TESTCONF #comment001
20140527000002 DISKFLAT-002 reuse BLOCKSIZE:32 POOL:DISKFLAT STORAGE:DISKFLAT CONFIG:TESTCONF #comment002
20140527000003 DISKFLAT-003 reuse BLOCKSIZE:32 POOL:DISKFLAT STORAGE:DISKFLAT CONFIG:TESTCONF #comment003
20140527000004 DISKFLAT-004 no-reuse BLOCKSIZE:32 POOL:DISKFLAT STORAGE:DISKFLAT CONFIG:OTHER #comment004
20140527000005 DISKFLAT-005 no-reuse BLOCKSIZE:32 POOL:DISKFLAT STORAGE:DISKFLAT CONFIG:TESTCONF #comment005
20140527000006 DISKFLAT-006 no-reuse BLOCKSIZE:32 POOL:DISKFLAT STORAGE:DISKFLAT CONFIG:TESTCONF #comment006
20140527000100 robot-BAR100 reuse BARCODE:BAR100 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000101 robot-BAR101 reuse BARCODE:BAR101 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000102 robot-BAR102 no-reuse BARCODE:BAR102 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000103 robot-BAR103 no-reuse BARCODE:BAR103 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000200 tape-200 reuse BLOCKSIZE:32 POOL:my_tape STORAGE:my_tape CONFIG:TESTCONF2
20140527000201 tape-201 reuse BLOCKSIZE:32 POOL:my_tape STORAGE:my_tape CONFIG:TESTCONF2
EOF

my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
sub write_tapelist {
    open TAPELIST, ">$tlf";
    print TAPELIST $tapelist_data;
    close TAPELIST;
}

write_tapelist();

#CODE 1600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'tles' => [
		  { 'position' => 7,
		    'datestamp' => 20140527000006,
		    'label' => 'DISKFLAT-006',
		    'reuse' => '0',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment006', },
		  { 'position' => 8,
		    'datestamp' => 20140527000005,
		    'label' => 'DISKFLAT-005',
		    'reuse' => '0',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment005', },
		  { 'position' => 9,
		    'datestamp' => 20140527000004,
		    'label' => 'DISKFLAT-004',
		    'reuse' => '0',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'OTHER',
		    'comment' => 'comment004', },
		  { 'position' => 10,
		    'datestamp' => 20140527000003,
		    'label' => 'DISKFLAT-003',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment003', },
		  { 'position' => 11,
		    'datestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment002', },
		  { 'position' => 12,
		    'datestamp' => 20140527000001,
		    'label' => 'DISKFLAT-001',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment001', },
		],
		'severity' => '16',
		'message' => 'List of labels',
		'code' => '1600001'
	  },
        ],
      http_code => 200,
    },
    "All Dles") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?config=TESTCONF");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'tles' => [
		  { 'position' => 7,
		    'datestamp' => 20140527000006,
		    'label' => 'DISKFLAT-006',
		    'reuse' => '0',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment006', },
		  { 'position' => 8,
		    'datestamp' => 20140527000005,
		    'label' => 'DISKFLAT-005',
		    'reuse' => '0',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment005', },
		  { 'position' => 10,
		    'datestamp' => 20140527000003,
		    'label' => 'DISKFLAT-003',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment003', },
		  { 'position' => 11,
		    'datestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment002', },
		  { 'position' => 12,
		    'datestamp' => 20140527000001,
		    'label' => 'DISKFLAT-001',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment001', },
		],
		'severity' => '16',
		'message' => 'List of labels',
		'code' => '1600001'
	  },
        ],
      http_code => 200,
    },
    "config=TESTCONF2") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?reuse=1");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'tles' => [
		  { 'position' => 10,
		    'datestamp' => 20140527000003,
		    'label' => 'DISKFLAT-003',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment003', },
		  { 'position' => 11,
		    'datestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment002', },
		  { 'position' => 12,
		    'datestamp' => 20140527000001,
		    'label' => 'DISKFLAT-001',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment001', },
		],
		'severity' => '16',
		'message' => 'List of labels',
		'code' => '1600001'
	  },
        ],
      http_code => 200,
    },
    "reuse=1") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'tles' => [
		  { 'position' => 11,
		    'datestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'comment002', },
		],
		'severity' => '16',
		'message' => 'List of labels',
		'code' => '1600001'
	  },
        ],
      http_code => 200,
    },
    "LABEL=DISKFLAT-002") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1150002
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Storage.pm",
		'severity' => '16',
		'type' => 'fatal',
		'storage' => 'DISKFLAT',
		'message' => 'Storage \'DISKFLAT\' not found',
		'code' => '1150002'
	  },
        ],
      http_code => 200,
    },
    "set 1") || diag("reply: " . Data::Dumper::Dumper($reply));

$testconf->add_policy("DISKFLAT", [
	retention_tapes => 10,
]);
$testconf->add_changer("DISKFLAT", [
	tpchanger => "\"chg-diskflat:$taperoot\"",
	property  => '"num-slot" "7"',
]);
$testconf->add_storage("DISKFLAT", [
	policy    => '"DISKFLAT"',
	tpchanger => '"DISKFLAT"',
	autolabel => '"DISKFLAT-$3s" any',
	labelstr  => 'MATCH-AUTOLABEL',
]);
$testconf->write();
write_tapelist();

#CODE 1000035 1000061
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-012?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'tapelist_filename' => $tlf,
		'label' => 'DISKFLAT-012',
		'message' => "label \'DISKFLAT-012\' not found in tapelist file \'$tlf\'.",
		'code' => '1000035'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-012',
		'message' => 'No label matching \'DISKFLAT-012\' in the tapelist file',
		'code' => '1000061'
	  },
        ],
      http_code => 200,
    },
    "set 2") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000047 1000003 1000004
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-002',
		'message' => 'marking tape \'DISKFLAT-002\' as not reusable.',
		'code' => '1000047'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-002',
		'pool' => 'DISKFLAT',
		'message' => 'DISKFLAT-002: Can\'t assign pool without force, old pool is \'DISKFLAT\'',
		'code' => '1000003'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-002',
		'storage' => 'DISKFLAT',
		'message' => 'DISKFLAT-002: Can\'t assign storage without force, old storage is \'DISKFLAT\'',
		'code' => '1000004'
	  },
        ],
      http_code => 200,
    },
    "set 3") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000048
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment&force=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-002',
		'message' => 'tape \'DISKFLAT-002\' already not reusable.',
		'code' => '1000048'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-002',
		'message' => 'Setting DISKFLAT-002',
		'code' => '1000006'
	  },
        ],
      http_code => 200,
    },
    "set 3") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/NEW-DISKFLAT/labels/DISKFLAT-002");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'tles' => [
		  { 'position' => 11,
		    'datestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '0',
		    'meta' => undef,
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'NEW-POOL',
		    'storage' => 'NEW-DISKFLAT',
		    'config' => 'TESTCONF',
		    'comment' => 'newcomment', },
		],
		'severity' => '16',
		'message' => 'List of labels',
		'code' => '1600001'
	  },
        ],
      http_code => 200,
    },
    "LABEL=DISKFLAT-002") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000008 1000009 1100057
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'message' => 'Reading label...',
		'code' => '1000008'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'message' => 'Found an empty tape.',
		'code' => '1000009'
	  },
          {	'source_filename' => "$amperldir/Amanda/Changer.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-001',
		'message' => 'Label \'DISKFLAT-001\' already exists',
		'code' => '1100057'
	  },
        ],
      http_code => 200,
    },
    "label") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000008 1100033
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?slot=55","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'message' => 'Reading label...',
		'code' => '1000008'
	  },
          {	'source_filename' => "$amperldir/Amanda/Changer/diskflat.pm",
		'severity' => '16',
		'reason' => 'invalid',
		'type' => 'failed',
		'slot' => '55',
		'message' => 'Slot 55 not found',
		'code' => '1100033'
	  },
        ],
      http_code => 200,
    },
    "label slot 55") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000008 1000009 1000020 1000021 1000022
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?slot=7","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'message' => 'Reading label...',
		'code' => '1000008'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'message' => 'Found an empty tape.',
		'code' => '1000009'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-007',
		'message' => 'Writing label \'DISKFLAT-007\'...',
		'code' => '1000020'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'message' => 'Checking label...',
		'code' => '1000021'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'message' => 'Success!',
		'code' => '1000022'
	  },
        ],
      http_code => 200,
    },
    "label slot 7") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1500015
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'severity' => '16',
		'message' => 'No label specified',
		'code' => '1500015'
	  },
        ],
      http_code => 200,
    },
    "DELETE no label") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000035
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-008","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-008',
		'tapelist_filename' =>$tlf,
		'message' => "label \'DISKFLAT-008\' not found in tapelist file '$tlf'.",
		'code' => '1000035'
	  },
        ],
      http_code => 200,
    },
    "DELETE DISKFLAT-008") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000035
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-006","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-006',
		'tapelist_filename' =>$tlf,
		'message' => "Removed label 'DISKFLAT-006' from tapelist file.",
		'code' => '1000052'
	  },
        ],
      http_code => 200,
    },
    "DELETE DISKFLAT-008") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000049 1000052
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-005?erase=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-005',
		'message' => 'Erased volume with label \'DISKFLAT-005\'.',
		'code' => '1000049'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => '16',
		'label' => 'DISKFLAT-005',
		'tapelist_filename' =>$tlf,
		'message' => "Removed label 'DISKFLAT-005' from tapelist file.",
		'code' => '1000052'
	  },
        ],
      http_code => 200,
    },
    "DELETE DISKFLAT-005") || diag("reply: " . Data::Dumper::Dumper($reply));

$rest->stop();

rmtree $taperoot;

1;
