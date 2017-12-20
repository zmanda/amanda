# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Run;
use Installcheck::Catalogs;
use Amanda::Paths;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::DB::Catalog;
use Amanda::Cmdline;
use Amanda::Xfer qw( :constants );

eval 'use Installcheck::Rest;';
if ($@) {
    plan skip_all => "Can't load Installcheck::Rest: $@";
    exit 1;
}

# send xfer logging somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $rest = Installcheck::Rest->new();
if ($rest->{'error'}) {
   plan skip_all => "Can't start JSON Rest server: $rest->{'error'}: see " . Amanda::Debug::dbfn();
   exit 1;
}
plan tests => 21;

my $taperoot = "$Installcheck::TMP/Amanda_Changer_Diskflat_test";

# set up and load a simple config
my $testconf = Installcheck::Run::setup();
$testconf->write();

my $reply;
my $amperldir = $Amanda::Paths::amperldir;

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/create","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Storage.pm",
                'severity' => $Amanda::Message::ERROR,
		'type'     => 'fatal',
                'message' => 'Storage \'DISKFLAT\': not found',
                'changer_message' => 'not found',
		'storage' => 'DISKFLAT',
		'storage_name' => 'DISKFLAT',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1150002'
          },
        ],
      http_code => 404,
    },
    "Create storage DISKFLAT - 1150002") || diag("reply: " .Data::Dumper::Dumper($reply));

$testconf = Installcheck::Run::setup();
$testconf->remove_param("tpchanger");
$testconf->remove_param("tapedev");
$testconf->add_param("tapedev",'"/dev/nst0"');
$testconf->add_policy("DISKFLAT", [
	retention_tapes => 10,
]);
$testconf->add_changer("DISKFLAT", [
]);
$testconf->add_storage("DISKFLAT", [
	policy => '"DISKFLAT"',
	runtapes => 4,
]);
$testconf->write();

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/create","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::ERROR,
		'type'     => 'failed',
                'message' => 'Storage \'DISKFLAT\': \'chg-single\' does not support create',
                'changer_message' => '\'chg-single\' does not support create',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage' => 'DISKFLAT',
		'storage_name' => 'DISKFLAT',
		'chg_name' => '/dev/nst0',
		'chg_type' => 'chg-single',
		'module' => 'amanda',
		'reason' => 'notimpl',
		'op' => 'create',
                'code' => '1100048'
          },
        ],
      http_code => 404,
    },
    "Create storage DISKFLAT - 1100048") || diag("reply: " .Data::Dumper::Dumper($reply));

$testconf->remove_param("tapedev");
$testconf->add_param("tapedev", '""');
$testconf->write();

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/create","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Storage.pm",
                'severity' => $Amanda::Message::ERROR,
		'type'     => 'fatal',
                'message' => 'Storage \'DISKFLAT\': You must specify the \'tapedev\' or \'tpchanger\'',
                'changer_message' => 'You must specify the \'tapedev\' or \'tpchanger\'',
		'storage' => 'DISKFLAT',
		'storage_name' => 'DISKFLAT',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1150003'
          },
        ],
      http_code => 404,
    },
    "Create storage DISKFLAT - 1150003") || diag("reply: " .Data::Dumper::Dumper($reply));

$testconf->add_storage("DISKFLAT", [
	policy    => '"DISKFLAT"',
	tpchanger => '"DISKFLAT"',
	runtapes => 4,
]);
$testconf->write();

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/create","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::ERROR,
		'type'     => 'fatal',
                'message' => 'Storage \'TESTCONF\': You must specify one of \'tapedev\' or \'tpchanger\'',
                'changer_message' => 'You must specify one of \'tapedev\' or \'tpchanger\'',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'changer',
		'module' => 'changer',
		'storage' => 'TESTCONF',
		'storage_name' => 'TESTCONF',
                'code' => '1100029'
          },
        ],
      http_code => 404,
    },
    "Create storage DISKFLAT - 1100029") || diag("reply: " .Data::Dumper::Dumper($reply));

rmtree $taperoot;
$testconf->add_changer("DISKFLAT", [
	tpchanger => "\"chg-diskflat:$taperoot\"",
	property  => '"num-slot" "5"',
	property  => '"auto-create-slot" "yes"'
]);
$testconf->write();

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/create","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer/diskflat.pm",
                'severity' => $Amanda::Message::SUCCESS,
		'dir'     => $taperoot,
                'message' => "Storage 'DISKFLAT': Created vtape root '$taperoot'",
                'changer_message' => "Created vtape root '$taperoot'",
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
                'code' => '1100027'
          },
        ],
      http_code => 200,
    },
    "Create storage DISKFLAT - 1100027") || diag("reply: " .Data::Dumper::Dumper($reply));
ok(-d $taperoot, "DIR EXISTS");

#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/inventory","");
#is_deeply (Installcheck::Rest::remove_source_line($reply),
#    { body =>
#        [ {     'source_filename' => "$amperldir/Amanda/Changer/diskflat.pm",
#                'severity' => $Amanda::Message::ERROR,
#                'message' => 'Can\'t compute label for slot \'1\': template is not set, you must set autolabel',
#		'process' => 'Amanda::Rest::Storages',
#		'running_on' => 'amanda-server',
#		'component' => 'rest-server',
#		'module' => 'amanda',
#                'code'  => '1100042',
#		'slot'  => 1,
#		'type'  => 'fatal',
#		'error' => {
#			'source_filename' => "$amperldir/Amanda/Changer.pm",
#			'severity' => $Amanda::Message::ERROR,
#			'message' => 'template is not set, you must set autolabel',
#			'code' => '1100050'
#		},
#          },
#        ],
#      http_code => 404,
#    },
#    "inventory DISKFLAT - 1100042") || diag("reply: " .Data::Dumper::Dumper($reply));

$testconf->add_storage("DISKFLAT", [
	policy    => '"DISKFLAT"',
	tpchanger => '"DISKFLAT"',
	autolabel => '"DISKFLAT-$3s" any',
	labelstr  => 'MATCH-AUTOLABEL',
	runtapes  => '5',
]);
$testconf->write();

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/inventory","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Storages.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Storage \'DISKFLAT\': The inventory',
                'changer_message' => 'The inventory',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100000',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'inventory' => [
				 {'current' => 1,
				  'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '1'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '2'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '3'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '4'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '5'
				 },
			       ],
          },
        ],
      http_code => 200,
    },
    "empty inventory DISKFLAT - 1100000") || diag("reply: " .Data::Dumper::Dumper($reply));

#manualy label a slot
my $file = $taperoot ."/DISKFLAT-001";
open AA, ">$taperoot/DISKFLAT-001";
print AA "AMANDA: TAPESTART DATE 20140509113436 TAPE DISKFLAT-001\n";
close AA;

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/inventory","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Storages.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Storage \'DISKFLAT\': The inventory',
                'changer_message' => 'The inventory',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100000',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'inventory' => [
				 {'current' => 1,
				  'label' => "DISKFLAT-001",
				  'label_match' => '1',
				  'device_status' => '0',
				  'reserved' => 0,
				  'f_type' => '1',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '1'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '2'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '3'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '4'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'retention_type' => 'retention-no',
				  'slot' => '5'
				 },
			       ],
          },
        ],
      http_code => 200,
    },
    "one slot inventory DISKFLAT - 1100000") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/load?slot=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Storages.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Storage \'DISKFLAT\': load result',
                'changer_message' => 'load result',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100002',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'load_result' => {
			'label' => 'DISKFLAT-001',
			'device_status' => 0,
			'f_type' => 1,
			'datestamp' => '20140509113436',
		},
          },
        ],
      http_code => 200,
    },
    "load slot 1 - 1100002") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/load?slot=2","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Storages.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'Storage \'DISKFLAT\': load result',
                'changer_message' => 'load result',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100002',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'load_result' => {
			'device_status_error' => 'Volume not labeled',
			'device_status' => 8,
			'device_error' => "Couldn't open file $taperoot/DISKFLAT-002: No such file or directory (unlabeled)"
		},
          },
        ],
      http_code => 200,
    },
    "load slot 2 - 1100002") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/load?label=DISKFLAT-001","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Storages.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Storage \'DISKFLAT\': load result',
                'changer_message' => 'load result',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100002',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'load_result' => {
			'label' => 'DISKFLAT-001',
			'device_status' => 0,
			'f_type' => 1,
			'datestamp' => '20140509113436',
		},
          },
        ],
      http_code => 200,
    },
    "load label DISKFLAT-001 - 1100002") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/load?label=DISKFLAT-002","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer/diskflat.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'Storage \'DISKFLAT\': Label \'DISKFLAT-002\' not found',
                'changer_message' => 'Label \'DISKFLAT-002\' not found',
		'label' => 'DISKFLAT-002',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100035',
		'storage' => 'DISKFLAT',
		'storage_name' => 'DISKFLAT',
		'type'  => 'failed',
		'reason' => 'notfound'
          },
        ],
      http_code => 200,
    },
    "load label DISKFLAT-002 - 1100002") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/show","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'Storage \'DISKFLAT\': scanning all 5 slots in changer:',
                'changer_message' => 'scanning all 5 slots in changer:',
		'num_slots' => '5',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
		'code' => '1100010',
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'Storage \'DISKFLAT\': slot   1: date 20140509113436 label DISKFLAT-001',
                'changer_message' => 'slot   1: date 20140509113436 label DISKFLAT-001',
		'label' => 'DISKFLAT-001',
		'datestamp' => '20140509113436',
		'write_protected' => '',
		'label_match' => '1',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
		'code' => '1100015',
		'slot' => '1'
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'Storage \'DISKFLAT\': slot   2: unlabeled volume',
                'changer_message' => 'slot   2: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
		'code' => '1100016',
		'slot' => '2',
		'write_protected' => ''
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'Storage \'DISKFLAT\': slot   3: unlabeled volume',
                'changer_message' => 'slot   3: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
		'code' => '1100016',
		'slot' => '3',
		'write_protected' => ''
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'Storage \'DISKFLAT\': slot   4: unlabeled volume',
                'changer_message' => 'slot   4: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
		'code' => '1100016',
		'slot' => '4',
		'write_protected' => ''
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'Storage \'DISKFLAT\': slot   5: unlabeled volume',
                'changer_message' => 'slot   5: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
		'code' => '1100016',
		'slot' => '5',
		'write_protected' => ''
          },
        ],
      http_code => 200,
    },
    "shot - 1100010 - 1100015 - 1100017") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/reset","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Storages.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Storage \'DISKFLAT\': Changer is reset',
                'changer_message' => 'Changer is reset',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
                'code'  => '1100003',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
          },
        ],
      http_code => 200,
    },
    "reset - 1100003") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/clean","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'Storage \'DISKFLAT\': \'chg-diskflat\' does not support clean',
                'changer_message' => '\'chg-diskflat\' does not support clean',
		'reason' => 'notimpl',
		'type'   => 'failed',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage' => 'DISKFLAT',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
                'code'  => '1100048',
		'storage_name' => 'DISKFLAT',
		'op'    => 'clean',
		'chg_type' => 'chg-diskflat',
          },
        ],
      http_code => 405,
    },
    "clean - 1100048") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/verify","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'Storage \'DISKFLAT\': \'chg-diskflat\' does not support verify',
                'changer_message' => '\'chg-diskflat\' does not support verify',
		'reason' => 'notimpl',
		'type'   => 'failed',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage' => 'DISKFLAT',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
                'code'  => '1100048',
		'storage_name' => 'DISKFLAT',
		'op'    => 'verify',
		'chg_type' => 'chg-diskflat',
          },
        ],
      http_code => 405,
    },
    "verify - 1100048") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/update","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'Storage \'DISKFLAT\': \'chg-diskflat\' does not support update',
                'changer_message' => '\'chg-diskflat\' does not support update',
		'reason' => 'notimpl',
		'type'   => 'failed',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'storage' => 'DISKFLAT',
		'storage_name' => 'DISKFLAT',
		'chg_name' => 'DISKFLAT',
		'module' => 'amanda',
                'code'  => '1100048',
		'storage_name' => 'DISKFLAT',
		'op'    => 'update',
		'chg_type' => 'chg-diskflat',
          },
        ],
      http_code => 405,
    },
    "update - 1100048") || diag("reply: " .Data::Dumper::Dumper($reply));

rmtree $taperoot;

# chg-aggregate with 2 chg-disk
my $taperoot0 = "$Installcheck::TMP/Amanda_Changer_Disk_test0";
my $taperoot1 = "$Installcheck::TMP/Amanda_Changer_Disk_test1";
$testconf = Installcheck::Config->new();
$testconf->add_changer("disk0", [
        tpchanger => "\"chg-disk:$taperoot0\"",
        property  => '"num-slot" "3"',
]);
$testconf->add_changer("disk1", [
        tpchanger => "\"chg-disk:$taperoot1\"",
        property  => '"num-slot" "3"',
]);
my $aggregate_statefile = "$CONFIG_DIR/TESTCONF/aggregate.state";
$testconf->add_changer("aggregateC", [
        tpchanger => "\"chg-aggregate:{disk0,disk1}\"",
        property  => '"allow-missing-changer" "yes"',
        property  => "\"state-filename\" \"$aggregate_statefile\""
]);
$testconf->add_storage("aggregate", [
        tpchanger => '"aggregateC"',
]);
$testconf->add_param("storage", '"aggregate"');
$testconf->write();

unlink $aggregate_statefile;
rmtree($taperoot0);
mkpath($taperoot0);
rmtree($taperoot1);
mkpath($taperoot1);
mkdir("$taperoot1/slot1");
mkdir("$taperoot1/slot2");
mkdir("$taperoot1/slot3");

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/aggregate/show","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
	[ {	'code' => '1100010',
		'num_slots' => '3',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'message' => 'Storage \'aggregate\': scanning all 3 slots in changer:',
		'changer_message' => 'scanning all 3 slots in changer:'
	  },
	  {
		'slot' => '1:1',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:1: unlabeled volume',
		'changer_message' => 'slot 1:1: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'slot' => '1:2',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:2: unlabeled volume',
		'changer_message' => 'slot 1:2: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'message' => 'Storage \'aggregate\': slot 1:3: unlabeled volume',
		'changer_message' => 'slot 1:3: unlabeled volume',
		'module' => 'amanda',
		'severity' => 'info',
		'running_on' => 'amanda-server',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'slot' => '1:3',
		'code' => '1100016'
	  }
	],
      http_code => 200,
    },
    "show - 1100010, 1100016") || diag("reply: " .Data::Dumper::Dumper($reply));

# chg-aggregate with 2 chg-disk (first missing)
rmtree($taperoot0);
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/aggregate/show","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
	[ {	'code' => '1100010',
		'num_slots' => '3',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'message' => 'Storage \'aggregate\': scanning all 3 slots in changer:',
		'changer_message' => 'scanning all 3 slots in changer:'
	  },
	  {
		'slot' => '1:1',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:1: unlabeled volume',
		'changer_message' => 'slot 1:1: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'slot' => '1:2',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:2: unlabeled volume',
		'changer_message' => 'slot 1:2: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateC',
		'message' => 'Storage \'aggregate\': slot 1:3: unlabeled volume',
		'changer_message' => 'slot 1:3: unlabeled volume',
		'module' => 'amanda',
		'severity' => 'info',
		'running_on' => 'amanda-server',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'slot' => '1:3',
		'code' => '1100016'
	  }
	],
      http_code => 200,
    },
    "show - 1100010, 1100014") || diag("reply: " .Data::Dumper::Dumper($reply));

# chg-aggregate with 2 chg-diskflat
$taperoot0 = "$Installcheck::TMP/Amanda_Changer_Disk_test0";
$taperoot1 = "$Installcheck::TMP/Amanda_Changer_Disk_test1";
$testconf = Installcheck::Config->new();
$testconf->add_changer("diskflat0", [
        tpchanger => "\"chg-diskflat:$taperoot0\"",
        property  => '"num-slot" "3"',
]);
$testconf->add_changer("diskflat1", [
        tpchanger => "\"chg-diskflat:$taperoot1\"",
        property  => '"auto-create-slot" "yes"',
        property  => '"num-slot" "3"',
]);
$testconf->add_changer("aggregateB", [
        tpchanger => "\"chg-aggregate:{diskflat0,diskflat1}\"",
        property  => '"allow-missing-changer" "yes"',
        property  => "\"state-filename\" \"$aggregate_statefile\""
]);
$testconf->add_storage("aggregate", [
        tpchanger => '"aggregateB"',
        'meta-autolabel' => '"!!"',
        autolabel => '"$m-$1s" empty'
]);
$testconf->add_param("storage", '"aggregate"');
$testconf->write();

unlink $aggregate_statefile;
rmtree($taperoot0);
mkpath($taperoot0);
rmtree($taperoot1);
mkpath($taperoot1);
open AA, ">$taperoot1/AA-1"; close AA;
open AA, ">$taperoot1/AA-2"; close AA;
open AA, ">$taperoot1/AA-3"; close AA;

$cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/aggregate/show","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
	[ {	'code' => '1100010',
		'num_slots' => '3',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'message' => 'Storage \'aggregate\': scanning all 3 slots in changer:',
		'changer_message' => 'scanning all 3 slots in changer:'
	  },
	  {
		'slot' => '1:1',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:1: unlabeled volume',
		'changer_message' => 'slot 1:1: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'slot' => '1:2',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:2: unlabeled volume',
		'changer_message' => 'slot 1:2: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'message' => 'Storage \'aggregate\': slot 1:3: unlabeled volume',
		'changer_message' => 'slot 1:3: unlabeled volume',
		'module' => 'amanda',
		'severity' => 'info',
		'running_on' => 'amanda-server',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'slot' => '1:3',
		'code' => '1100016'
	  }
	],
      http_code => 200,
    },
    "show - 1100010, 1100016") || diag("reply: " .Data::Dumper::Dumper($reply));

# chg-aggregate with 2 chg-diskflat (first missing)
rmtree($taperoot0);
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/aggregate/show","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
	[ {	'code' => '1100010',
		'num_slots' => '3',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'message' => 'Storage \'aggregate\': scanning all 3 slots in changer:',
		'changer_message' => 'scanning all 3 slots in changer:'
	  },
	  {
		'slot' => '1:1',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:1: unlabeled volume',
		'changer_message' => 'slot 1:1: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'slot' => '1:2',
		'code' => '1100016',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'module' => 'amanda',
		'running_on' => 'amanda-server',
		'severity' => 'info',
		'message' => 'Storage \'aggregate\': slot 1:2: unlabeled volume',
		'changer_message' => 'slot 1:2: unlabeled volume',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'process' => 'Amanda::Rest::Storages'
	  },
	  {
		'process' => 'Amanda::Rest::Storages',
		'component' => 'rest-server',
		'storage_name' => 'aggregate',
		'chg_name' => 'aggregateB',
		'message' => 'Storage \'aggregate\': slot 1:3: unlabeled volume',
		'changer_message' => 'slot 1:3: unlabeled volume',
		'module' => 'amanda',
		'severity' => 'info',
		'running_on' => 'amanda-server',
		'source_filename' => "$amperldir/Amanda/Changer.pm",
		'write_protected' => '',
		'slot' => '1:3',
		'code' => '1100016'
	  }
	],
      http_code => 200,
    },
    "show - 1100010, 1100014") || diag("reply: " .Data::Dumper::Dumper($reply));

$rest->stop();

#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/reset","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/eject","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/clean","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/verify","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/show","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/show?slots=3..5","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/update","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/update?changed=3..5","");

1;
