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
plan tests => 16;

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
                'message' => 'Storage \'DISKFLAT\' not found',
		'storage' => 'DISKFLAT',
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
        [ {     'source_filename' => "$amperldir/Amanda/Storage.pm",
                'severity' => $Amanda::Message::ERROR,
		'type'     => 'fatal',
                'message' => 'You must specify the storage \'tpchanger\'',
		'storage' => 'DISKFLAT',
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
                'message' => 'You must specify one of \'tapedev\' or \'tpchanger\'',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'changer',
		'module' => 'changer',
                'code' => '1100029'
          },
        ],
      http_code => 404,
    },
    "Create storage DISKFLAT - 1100029") || diag("reply: " .Data::Dumper::Dumper($reply));

rmtree $taperoot;
$testconf->add_changer("DISKFLAT", [
	tpchanger => "\"chg-diskflat:$taperoot\"",
	property  => '"num-slot" "5"'
]);
$testconf->write();

$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/create","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Changer/diskflat.pm",
                'severity' => $Amanda::Message::SUCCESS,
		'dir'     => $taperoot,
                'message' => "Created vtape root '$taperoot'",
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
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
                'message' => 'The inventory',
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
				  'slot' => '1'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'slot' => '2'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'slot' => '3'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'slot' => '4'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
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
                'message' => 'The inventory',
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
				  'device_status' => '0',
				  'reserved' => 0,
				  'f_type' => '1',
				  'state' => 1,
				  'slot' => '1'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'slot' => '2'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'slot' => '3'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
				  'slot' => '4'
				 },
				 {'label' => undef,
				  'device_status' => '8',
				  'reserved' => 0,
				  'f_type' => '-2',
				  'state' => 1,
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
                'message' => 'load result',
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
                'message' => 'load result',
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
                'message' => 'load result',
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
                'message' => 'Label \'DISKFLAT-002\' not found',
		'label' => 'DISKFLAT-002',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100035',
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
                'message' => 'scanning all 5 slots in changer:',
		'num_slots' => '5',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1100010',
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'slot   1: date 20140509113436 label DISKFLAT-001',
		'label' => 'DISKFLAT-001',
		'datestamp' => '20140509113436',
		'write_protected' => '',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1100015',
		'slot' => '1'
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'slot   2: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1100016',
		'slot' => '2',
		'write_protected' => ''
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'slot   3: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1100016',
		'slot' => '3',
		'write_protected' => ''
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'slot   4: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1100016',
		'slot' => '4',
		'write_protected' => ''
          },
          {     'source_filename' => "$amperldir/Amanda/Changer.pm",
                'severity' => $Amanda::Message::INFO,
                'message' => 'slot   5: unlabeled volume',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
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
                'message' => 'Changer is reset',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
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
                'message' => '\'chg-diskflat\' does not support clean',
		'reason' => 'notimpl',
		'type'   => 'failed',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100048',
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
                'message' => '\'chg-diskflat\' does not support verify',
		'reason' => 'notimpl',
		'type'   => 'failed',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100048',
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
                'message' => '\'chg-diskflat\' does not support update',
		'reason' => 'notimpl',
		'type'   => 'failed',
		'process' => 'Amanda::Rest::Storages',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code'  => '1100048',
		'op'    => 'update',
		'chg_type' => 'chg-diskflat',
          },
        ],
      http_code => 405,
    },
    "update - 1100048") || diag("reply: " .Data::Dumper::Dumper($reply));


$rest->stop();

rmtree $taperoot;

#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/reset","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/eject","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/clean","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/verify","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/show","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/show?slots=3..5","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/update","");
#$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/DISKFLAT/update?changed=3..5","");

1;
