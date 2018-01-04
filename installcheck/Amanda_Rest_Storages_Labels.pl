# Copyright (c) 2014-2016 Carbonite, Inc.  All Rights Reserved.
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
   plan skip_all => "Can't start JSON Rest server: $rest->{'error'}: see " . Amanda::Debug::dbfn();
   exit 1;
}
plan tests => 17;

my $reply;

my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

my $taperoot = "$Installcheck::TMP/Amanda_Changer_Diskflat_test";
rmtree $taperoot;
mkdir $taperoot;

$testconf = Installcheck::Run::setup();
$testconf->write( do_catalog => 0 );
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKLFAT/labels");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'volumes' => [],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
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
    #unlink "$Amanda::Paths::CONFIG_DIR/TESTCONF/SQLite.db";
    system("$sbindir/amcatalog", "TESTCONF", "create", "drop_tables");
}
write_tapelist();

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000006,
		    'label' => 'DISKFLAT-006',
		    'reuse' => '0',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'no-reuse',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000005,
		    'label' => 'DISKFLAT-005',
		    'reuse' => '0',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'no-reuse',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000004,
		    'label' => 'DISKFLAT-004',
		    'reuse' => '0',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'other config',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'OTHER',},
		  {
		    'write_timestamp' => 20140527000003,
		    'label' => 'DISKFLAT-003',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'DISKFLAT-001',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "All Dles") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?config=TESTCONF");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000006,
		    'label' => 'DISKFLAT-006',
		    'reuse' => '0',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'no-reuse',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000005,
		    'label' => 'DISKFLAT-005',
		    'reuse' => '0',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'no-reuse',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000003,
		    'label' => 'DISKFLAT-003',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'DISKFLAT-001',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "config=TESTCONF2") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?reuse=1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000003,
		    'label' => 'DISKFLAT-003',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'DISKFLAT-001',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "reuse=1") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '1',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'retention-tapes',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'DISKFLAT',
		    'storage' => 'DISKFLAT',
		    'config' => 'TESTCONF',},
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "LABEL=DISKFLAT-002") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1150002
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Storage.pm",
		'severity' => $Amanda::Message::ERROR,
		'type' => 'fatal',
		'storage_name' => 'DISKFLAT',
		'message' => "Storage 'DISKFLAT': not found",
		'changer_message' => 'not found',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'changer',
		'module' => 'amanda',
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
	tapepool  => '"DISKFLAT"',
	tpchanger => '"DISKFLAT"',
	autolabel => '"DISKFLAT-$3s" any',
	labelstr  => 'MATCH-AUTOLABEL',
]);
$testconf->write( do_catalog => 0 );
write_tapelist();

#CODE 1000035 1000061
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-012?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/DB/Catalog2/SQL.pm",
		'severity' => $Amanda::Message::ERROR,
		'catalog_name' => 'my_catalog',
		'label' => 'DISKFLAT-012',
		'pool'  => 'DISKFLAT',
		'message' => 'volume \'DISKFLAT:DISKFLAT-012\' not found in catalog \'my_catalog\'.',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000035'
	  },
        ],
      http_code => 200,
    },
    "set 2") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000047 1000003 1000004
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/DB/Catalog2/SQL.pm",
		'severity' => $Amanda::Message::ERROR,
		'label' => 'DISKFLAT-002',
		'pool'  => 'DISKFLAT',
		'catalog_name' => 'my_catalog',
		'storage' => 'DISKFLAT',
		'message' => 'volume \'DISKFLAT:DISKFLAT-002\': Can\'t assign storage without force, old storage is \'DISKFLAT\'',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000004'
	  },
          {	'source_filename' => "$amperldir/Amanda/DB/Catalog2/SQL.pm",
		'severity' => $Amanda::Message::ERROR,
		'label' => 'DISKFLAT-002',
		'pool'  => 'DISKFLAT',
		'catalog_name' => 'my_catalog',
		'message' => 'volume \'DISKFLAT:DISKFLAT-002\': Can\'t assign pool without force, old pool is \'DISKFLAT\'',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000003'
	  },
          {	'source_filename' => "$amperldir/Amanda/DB/Catalog2/SQL.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'label' => 'DISKFLAT-002',
		'pool'  => 'DISKFLAT',
		'catalog_name' => 'my_catalog',
		'message' => 'marking volume \'DISKFLAT:DISKFLAT-002\' as not reusable.',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000047'
	  },
        ],
      http_code => 200,
    },
    "set 3") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000048
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-002?reuse=0&storage=NEW-DISKFLAT&pool=NEW-POOL&comment=newcomment&force=1","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/DB/Catalog2/SQL.pm",
		'severity' => $Amanda::Message::INFO,
		'label' => 'DISKFLAT-002',
		'pool' => 'DISKFLAT',
		'catalog_name' => 'my_catalog',
		'message' => 'volume \'DISKFLAT:DISKFLAT-002\' already not reusable.',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000048'
	  },
          {	'source_filename' => "$amperldir/Amanda/DB/Catalog2/SQL.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'label' => 'DISKFLAT-002',
		'pool' => 'DISKFLAT',
		'catalog_name' => 'my_catalog',
		'message' => 'Setting volume \'DISKFLAT:DISKFLAT-002\'',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000006'
	  },
        ],
      http_code => 200,
    },
    "set 3") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/NEW-DISKFLAT/labels/DISKFLAT-002");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'DISKFLAT-002',
		    'reuse' => '0',
		    'retention_days' => '0',
		    'retention_full' => '0',
		    'retention_recover' => '0',
		    'retention_tape' => '1',
		    'retention_name' => 'no-reuse',
		    'meta' => '',
		    'barcode' => undef,
		    'blocksize' => '32',
		    'pool' => 'NEW-POOL',
		    'storage' => 'NEW-DISKFLAT',
		    'config' => 'TESTCONF',},
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "LABEL=DISKFLAT-002") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000008 1100066
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::INFO,
		'message' => 'Reading label...',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000008'
	  },
	  {
		'slot_file' => "$Installcheck::TMP/Amanda_Changer_Diskflat_test/DISKFLAT-001",
		'reason' => 'invalid',
		'message' => "Storage 'DISKFLAT': label 'DISKFLAT-001' already in tapelist and slot file '$Installcheck::TMP/Amanda_Changer_Diskflat_test/DISKFLAT-001' do not exists",
		'changer_message' => "label 'DISKFLAT-001' already in tapelist and slot file '$Installcheck::TMP/Amanda_Changer_Diskflat_test/DISKFLAT-001' do not exists",
		'source_filename' => "$amperldir/Amanda/Changer/diskflat.pm",
		'error' => 'No such file or directory',
		'type' => 'failed',
		'severity' => $Amanda::Message::ERROR,,
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'changer',
		'module' => 'Amanda::Changer::diskflat',
		'code' => 1100066,
		'storage_name' => 'DISKFLAT',
		'label' => 'DISKFLAT-001',
		'slot' => '1'
	  }
        ],
      http_code => 200,
    },
    "label") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000008 1100033
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?slot=55","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::INFO,
		'message' => 'Reading label...',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000008'
	  },
          {	'source_filename' => "$amperldir/Amanda/Changer/diskflat.pm",
		'severity' => $Amanda::Message::ERROR,
		'reason' => 'invalid',
		'type' => 'failed',
		'slot' => '55',
		'message' => "Storage 'DISKFLAT': Slot 55 not found",
		'changer_message' => 'Slot 55 not found',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'changer',
		'module' => 'Amanda::Changer::diskflat',
		'storage_name' => 'DISKFLAT',
		'code' => '1100033'
	  },
        ],
      http_code => 200,
    },
    "label slot 55") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000008 1000009 1000020 1000021 1000022
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels?slot=7","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::INFO,
		'message' => 'Reading label...',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000008'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::INFO,
		'message' => 'Found an empty tape.',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000009'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::INFO,
		'label' => 'DISKFLAT-007',
		'message' => 'Writing label \'DISKFLAT-007\'...',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000020'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::INFO,
		'message' => 'Checking label...',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000021'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Success!',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000022'
	  },
        ],
      http_code => 200,
    },
    "label slot 7") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1500015
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Storages/Labels.pm",
		'severity' => $Amanda::Message::ERROR,
		'message' => 'No label specified',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500015'
	  },
        ],
      http_code => 200,
    },
    "DELETE no label") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000035
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-008","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::ERROR,
		'label' => 'DISKFLAT-008',
		'pool' => 'DISKFLAT',
		'catalog_name' => 'my_catalog',
		'message' => "volume \'DISKFLAT:DISKFLAT-008\' not found in catalog 'my_catalog'.",
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000035'
	  },
        ],
      http_code => 200,
    },
    "DELETE DISKFLAT-008") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000052
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-006","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'pool' => 'DISKFLAT',
		'label' => 'DISKFLAT-006',
		'catalog_name' => 'my_catalog',
		'message' => 'Removed volume \'DISKFLAT:DISKFLAT-006\' from catalog \'my_catalog\'',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000052'
	  },
        ],
      http_code => 200,
    },
    "DELETE DISKFLAT-006") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1000049 1000052
$reply = $rest->delete("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/labels/DISKFLAT-007?erase=1","");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'pool' => 'DISKFLAT',
		'label' => 'DISKFLAT-007',
		'message' => 'Erased volume with label \'DISKFLAT-007\'.',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000049'
	  },
          {	'source_filename' => "$amperldir/Amanda/Label.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'pool' => 'DISKFLAT',
		'label' => 'DISKFLAT-007',
		'catalog_name' => 'my_catalog',
		'message' => 'Removed volume \'DISKFLAT:DISKFLAT-007\' from catalog \'my_catalog\'',
		'process' => 'Amanda::Rest::Storages::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1000052'
	  },
        ],
      http_code => 200,
    },
    "DELETE DISKFLAT-007") || diag("reply: " . Data::Dumper::Dumper($reply));

$rest->stop();

rmtree $taperoot;

1;
