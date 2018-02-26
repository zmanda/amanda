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
use Installcheck::DBCatalog2;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;
use Amanda::DB::Catalog2;

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
plan tests => 12;

my $reply;

my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

$testconf = Installcheck::Run::setup();
$testconf->write( do_catalog => 0 );
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "No label") || diag("reply: ".Data::Dumper::Dumper($reply));


my $tapelist_data = <<EOF;
20140527000000 vtape-AA-000 reuse META:AA BLOCKSIZE:32 POOL:my_vtapes STORAGE:my_vtapes CONFIG:TESTCONF #comment000
20140527000001 vtape-AA-001 reuse META:AA BLOCKSIZE:32 POOL:my_vtapes STORAGE:my_vtapes CONFIG:TESTCONF #comment001
20140527000002 vtape-AB-002 reuse META:AB BLOCKSIZE:32 POOL:my_vtapes STORAGE:my_vtapes CONFIG:TESTCONF #comment002
20140527000003 vtape-AA-003 no-reuse META:AA BLOCKSIZE:32 POOL:my_vtapes STORAGE:my_vtapes CONFIG:TESTCONF #comment003
20140527000004 vtape-AA-004 no-reuse META:AA BLOCKSIZE:32 POOL:my_vtapes STORAGE:my_vtapes CONFIG:TESTCONF #comment004
20140527000005 vtape-AB-005 no-reuse META:AB BLOCKSIZE:32 POOL:my_vtapes STORAGE:my_vtapes CONFIG:TESTCONF #comment005
20140527000100 robot-BAR100 reuse BARCODE:BAR100 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000101 robot-BAR101 reuse BARCODE:BAR101 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000102 robot-BAR102 no-reuse BARCODE:BAR102 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000103 robot-BAR103 no-reuse BARCODE:BAR103 BLOCKSIZE:32 POOL:my_robot STORAGE:my_robot CONFIG:TESTCONF
20140527000200 tape-200 reuse BLOCKSIZE:32 POOL:my_tape STORAGE:my_tape CONFIG:TESTCONF2
20140527000201 tape-201 reuse BLOCKSIZE:32 POOL:my_tape STORAGE:my_tape CONFIG:TESTCONF2
EOF

my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
open TAPELIST, ">$tlf";
print TAPELIST $tapelist_data;
close TAPELIST;
$catalog->quit();
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();
#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000201,
		    'label' => 'tape-201',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_tape',
		    'storage' => 'my_tape',
		    'config' => 'TESTCONF2',
		    'retention_name' => 'other config',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000200,
		    'label' => 'tape-200',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_tape',
		    'storage' => 'my_tape',
		    'config' => 'TESTCONF2',
		    'retention_name' => 'other config',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000103,
		    'label' => 'robot-BAR103',
		    'reuse' => '0',
		    'barcode' => 'BAR103',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000102,
		    'label' => 'robot-BAR102',
		    'reuse' => '0',
		    'barcode' => 'BAR102',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000101,
		    'label' => 'robot-BAR101',
		    'reuse' => '1',
		    'barcode' => 'BAR101',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000100,
		    'label' => 'robot-BAR100',
		    'reuse' => '1',
		    'barcode' => 'BAR100',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000005,
		    'label' => 'vtape-AB-005',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AB',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000004,
		    'label' => 'vtape-AA-004',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000003,
		    'label' => 'vtape-AA-003',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'vtape-AB-002',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AB',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'vtape-AA-001',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000000,
		    'label' => 'vtape-AA-000',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "All Dles") || diag("reply: " . Data::Dumper::Dumper(Installcheck::Config::remove_source_line($reply)));;

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?config=TESTCONF2&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000201,
		    'label' => 'tape-201',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_tape',
		    'storage' => 'my_tape',
		    'config' => 'TESTCONF2',
		    'retention_name' => 'other config',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000200,
		    'label' => 'tape-200',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_tape',
		    'storage' => 'my_tape',
		    'config' => 'TESTCONF2',
		    'retention_name' => 'other config',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "config=TESTCONF2");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?storage=my_robot&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000103,
		    'label' => 'robot-BAR103',
		    'reuse' => '0',
		    'barcode' => 'BAR103',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000102,
		    'label' => 'robot-BAR102',
		    'reuse' => '0',
		    'barcode' => 'BAR102',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000101,
		    'label' => 'robot-BAR101',
		    'reuse' => '1',
		    'barcode' => 'BAR101',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000100,
		    'label' => 'robot-BAR100',
		    'reuse' => '1',
		    'barcode' => 'BAR100',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "storage=my_robot");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?meta=AA&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000004,
		    'label' => 'vtape-AA-004',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000003,
		    'label' => 'vtape-AA-003',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'vtape-AA-001',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000000,
		    'label' => 'vtape-AA-000',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		    'config' => 'TESTCONF',
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "meta=AA");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?pool=my_vtapes&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000005,
		    'label' => 'vtape-AB-005',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AB',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000004,
		    'label' => 'vtape-AA-004',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		    'config' => 'TESTCONF',
		  },
		  {
		    'write_timestamp' => 20140527000003,
		    'label' => 'vtape-AA-003',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'vtape-AB-002',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AB',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'vtape-AA-001',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000000,
		    'label' => 'vtape-AA-000',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		    'config' => 'TESTCONF',
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "pool=my_vtapes");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?reuse=1&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000201,
		    'label' => 'tape-201',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_tape',
		    'storage' => 'my_tape',
		    'config' => 'TESTCONF2',
		    'retention_name' => 'other config',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000200,
		    'label' => 'tape-200',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_tape',
		    'storage' => 'my_tape',
		    'config' => 'TESTCONF2',
		    'retention_name' => 'other config',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000101,
		    'label' => 'robot-BAR101',
		    'reuse' => '1',
		    'barcode' => 'BAR101',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000100,
		    'label' => 'robot-BAR100',
		    'reuse' => '1',
		    'barcode' => 'BAR100',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'vtape-AB-002',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AB',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'vtape-AA-001',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000000,
		    'label' => 'vtape-AA-000',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "reuse=1");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?reuse=0&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000103,
		    'label' => 'robot-BAR103',
		    'reuse' => '0',
		    'barcode' => 'BAR103',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000102,
		    'label' => 'robot-BAR102',
		    'reuse' => '0',
		    'barcode' => 'BAR102',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000005,
		    'label' => 'vtape-AB-005',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AB',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000004,
		    'label' => 'vtape-AA-004',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000003,
		    'label' => 'vtape-AA-003',
		    'reuse' => '0',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "reuse=0") || diag(Data::Dumper::Dumper($reply));

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?storage=my_vtapes&config=TESTCONF&reuse=1&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000002,
		    'label' => 'vtape-AB-002',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AB',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'vtape-AA-001',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000000,
		    'label' => 'vtape-AA-000',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "storage=my_vtapes&config=TESTCONF&reuse=1");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?storage=my_vtapes&config=TESTCONF&reuse=1&meta=AA&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000001,
		    'label' => 'vtape-AA-001',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000000,
		    'label' => 'vtape-AA-000',
		    'reuse' => '1',
		    'barcode' => undef,
		    'meta' => 'AA',
		    'blocksize' => '32',
		    'pool' => 'my_vtapes',
		    'storage' => 'my_vtapes',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "storage=my_vtapes&config=TESTCONF&reuse=1&meta=AA");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?pool=my_robot&config=TESTCONF&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		  {
		    'write_timestamp' => 20140527000103,
		    'label' => 'robot-BAR103',
		    'reuse' => '0',
		    'barcode' => 'BAR103',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000102,
		    'label' => 'robot-BAR102',
		    'reuse' => '0',
		    'barcode' => 'BAR102',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'no-reuse',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000101,
		    'label' => 'robot-BAR101',
		    'reuse' => '1',
		    'barcode' => 'BAR101',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		  {
		    'write_timestamp' => 20140527000100,
		    'label' => 'robot-BAR100',
		    'reuse' => '1',
		    'barcode' => 'BAR100',
		    'meta' => '',
		    'blocksize' => '32',
		    'pool' => 'my_robot',
		    'storage' => 'my_robot',
		    'config' => 'TESTCONF',
		    'retention_name' => 'retention-tapes',
		    'retention_days' => 0,
		    'retention_full' => 0,
		    'retention_recover' => 0,
		    'retention_tape' => 1,
		  },
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "pool=my_robot&config=TESTCONF");

#CODE 2600001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/labels?pool=my_robot&config=TESTCONF2&order_write_timestamp=-1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Labels.pm",
		'volumes' => [
		],
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'Volumes list',
		'process' => 'Amanda::Rest::Labels',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2600001'
	  },
        ],
      http_code => 200,
    },
    "pool=my_robot&config=TESTCONF2");

#diag("reply: " . Data::Dumper::Dumper($reply));

$rest->stop();
