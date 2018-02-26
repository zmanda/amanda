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
use Installcheck::DBCatalog2;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::DB::Catalog2;
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
plan tests => 2;

# set up and load a simple config
my $testconf = Installcheck::Run::setup();
$testconf->write( do_catalog => 0 );
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

my $reply;
my $amperldir = $Amanda::Paths::amperldir;

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps");
is_deeply (sort_reply(Installcheck::Config::remove_source_line($reply)),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => "The dumps",
		'dumps' => [],
		'process' => 'Amanda::Rest::Dumps',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '2600000'
          },
        ],
      http_code => 200,
    },
    "No dumps");

my $cat = Installcheck::Catalogs::load("bigdb");
$cat->install();
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

sub sort_dumps {
    my $dumps = shift;

    my @sorted = sort { $a->{'hostname'} cmp $b->{'hostname'} ||
			$a->{'diskname'} cmp $b->{'diskname'} ||
			$b->{'dump_timestamp'} cmp $a->{'dump_timestamp'} ||
			$b->{'write_timestamp'} cmp $a->{'write_timestamp'}} @{$dumps};
    return \@sorted;
}

sub sort_reply {
    my $reply = shift;

    if (defined $reply->{'body'}[0]->{'dumps'}) {
	$reply->{'body'}[0]->{'dumps'} = sort_dumps($reply->{'body'}[0]->{'dumps'});
    }
    return $reply;
}

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps?parts=1");
my $a =  { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
            'dumps' => [
#0
                                      {
                                        'diskname' => '/home/ada',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20100722000000',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-009',
                                                       'status' => 'OK',
                                                       'filenum' => 1,
                                                       'kb' => 166976,
						       'part_offset' => 0,
						       'part_size' => 170983424,
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '166976',
                                        'orig_kb' => '0',
                                        'hostname' => 'lovelace',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'level' => 3,
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '1',
                                        'write_timestamp' => '20100722000000'
                                      },
#1
                                      {
                                        'write_timestamp' => '00000000000000',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '2',
                                        'level' => '0',
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'hostname' => 'oldbox',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '1329152',
                                        'kb' => 1298,
                                        'native_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 1298,
                                                       'pool' => 'HOLDING',
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/oldbox._opt",
                                                       'status' => 'OK',
                                                       'partnum' => '1',
						       'part_offset' => 0,
						       'part_size' => 1329152,
                                                     }
                                                   ],
                                        'message' => '',
                                        'dump_timestamp' => '20080414144444',
                                        'nparts' => '1',
                                        'diskname' => '/opt'
                                      },
#2
                                      {
                                        'nparts' => '1',
                                        'diskname' => '/direct',
                                        'dump_timestamp' => '20080515155555',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 190,
                                                       'filenum' => 14,
                                                       'status' => 'OK',
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 1,
						       'part_offset' => 0,
						       'part_size' => 194560,
                                                     }
                                                   ],
                                        'message' => '',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '190',
                                        'orig_kb' => 350,
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'level' => '0',
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '3',
                                      },
#3
                                      {
                                        'level' => '0',
                                        'orig_kb' => 190,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '7',
                                        'dump_timestamp' => '20080511151555',
                                        'nparts' => '1',
                                        'diskname' => '/lib',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '190',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 13,
                                                       'kb' => 190,
                                                       'status' => 'OK',
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
						       'part_offset' => 0,
						       'part_size' => 194560,
                                                     }
                                                   ],
                                        'message' => ''
                                      },
#4
                                      {
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'kb' => '32',
                                        'native_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 1,
                                                       'kb' => 32,
                                                       'status' => 'PARTIAL',
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-005',
						       'part_offset' => 0,
						       'part_size' => 32 * 1024,
                                                     }
                                                   ],
                                        'message' => 'full-up',
                                        'dump_timestamp' => '20080414144444',
                                        'diskname' => '/lib',
                                        'nparts' => '0',
                                        'write_timestamp' => '20080414144444',
                                        'status' => 'PARTIAL',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'PARTIAL',
                                        'copy_id' => '5',
                                        'level' => 1,
                                        'orig_kb' => '0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'otherbox',
                                        'client_crc' => '00000000:0'
                                      },
#5
                                      {
                                        'level' => 1,
                                        'hostname' => 'otherbox',
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'client_crc' => '00000000:0',
                                        'orig_kb' => '0',
                                        'write_timestamp' => '00000000000000',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '6',
                                        'dump_timestamp' => '20080414144444',
                                        'diskname' => '/lib',
                                        'nparts' => '1',
                                        'kb' => 256,
                                        'native_crc' => '00000000:0',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '262144',
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'pool' => 'HOLDING',
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                                       'status' => 'OK',
                                                       'kb' => 256,
                                                       'partnum' => '1',
						       'part_offset' => 0,
						       'part_size' => 256 * 1024,
                                                     }
                                                   ]
                                      },
#6
                                      {
                                        'dump_timestamp' => '20080313133333',
                                        'nparts' => '1',
                                        'diskname' => '/lib',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '190',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 190,
                                                       'filenum' => 14,
                                                       'status' => 'OK',
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
						       'part_offset' => 0,
						       'part_size' => 190 * 1024,
                                                     }
                                                   ],
                                        'message' => '',
                                        'level' => '0',
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '4',
                                        'write_timestamp' => '20080313133333'
                                      },
#7
                                      {
                                        'orig_kb' => 240,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'level' => '0',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '10',
                                        'write_timestamp' => '20080515155555',
                                        'nparts' => '1',
                                        'diskname' => '/usr/bin',
                                        'dump_timestamp' => '20080511151155',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'status' => 'OK',
                                                       'kb' => 240,
                                                       'filenum' => 12,
						       'part_offset' => 0,
						       'part_size' => 245760,
                                                     }
                                                   ],
                                        'server_crc' => '00000000:0',
                                        'kb' => '240',
					'message' => '',
					'bytes' => '0',
                                        'native_crc' => '00000000:0'
                                      },
#8
                                      {
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 1,
                                                       'status' => 'OK',
                                                       'filenum' => 12,
                                                       'kb' => 240,
						       'part_offset' => 0,
						       'part_size' => 240 * 1024,
                                                     }
                                                   ],
                                        'kb' => '240',
                                        'native_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'diskname' => '/usr/bin',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20080311132233',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '9',
                                        'write_timestamp' => '20080313133333',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'otherbox',
                                        'orig_kb' => '0',
                                        'level' => '0'
                                      },
#9
                                      {
                                        'kb' => '240',
                                        'native_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'status' => 'OK',
                                                       'kb' => 240,
                                                       'filenum' => 13,
						       'part_offset' => 0,
						       'part_size' => 240 * 1024,
                                                     }
                                                   ],
                                        'dump_timestamp' => '20080311131133',
                                        'nparts' => '1',
                                        'diskname' => '/usr/bin',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '8',
                                        'write_timestamp' => '20080313133333',
                                        'level' => '0',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'orig_kb' => '0'
                                      },
#10
                                      {
                                        'write_timestamp' => '20080616166666',
                                        'status' => 'FAIL',
                                        'dump_status' => 'FAIL',
                                        'copy_status' => 'FAIL',
                                        'copy_id' => '15',
                                        'orig_kb' => 20,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'somebox',
                                        'client_crc' => '00000000:0',
                                        'level' => 1,
                                        'parts' => [
                                                     {},
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-007',
                                                       'partnum' => 1,
                                                       'filenum' => 2,
                                                       'kb' => 20,
                                                       'status' => 'OK',
						       'part_offset' => 0,
						       'part_size' => 20 * 1024,
                                                     }
                                                   ],
                                        'message' => '',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'kb' => '20',
                                        'native_crc' => '00000000:0',
                                        'diskname' => '/lib',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20080616166666',
                                      },
#11
                                      {
                                        'nparts' => '10',
                                        'diskname' => '/lib',
                                        'dump_timestamp' => '20080515155555',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'status' => 'OK',
                                                       'filenum' => 2,
                                                       'kb' => 1024,
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
						       'part_offset' => 0,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'kb' => 1024,
                                                       'filenum' => 3,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 2,
						       'part_offset' => 1 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'partnum' => 3,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'status' => 'OK',
                                                       'filenum' => 4,
                                                       'kb' => 1024,
						       'part_offset' => 2 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 4,
                                                       'kb' => 1024,
                                                       'filenum' => 5,
                                                       'status' => 'OK',
						       'part_offset' => 3 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'filenum' => 6,
                                                       'kb' => 1024,
                                                       'partnum' => 5,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
						       'part_offset' => 4 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'kb' => 1024,
                                                       'filenum' => 7,
                                                       'status' => 'OK',
                                                       'partnum' => 6,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
						       'part_offset' => 5 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 7,
                                                       'status' => 'OK',
                                                       'filenum' => 8,
                                                       'kb' => 1024,
						       'part_offset' => 6 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'partnum' => 8,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'status' => 'OK',
                                                       'kb' => 1024,
                                                       'filenum' => 9,
						       'part_offset' => 7 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'filenum' => 10,
                                                       'kb' => 1024,
                                                       'status' => 'OK',
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 9,
						       'part_offset' => 8 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'partnum' => 10,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'filenum' => 11,
                                                       'kb' => 284,
                                                       'status' => 'OK',
						       'part_offset' => 9 * 1024 * 1024,
						       'part_size' => 284 * 1024,
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '9500',
                                        'orig_kb' => 9500,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'somebox',
                                        'level' => '0',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '14',
                                        'write_timestamp' => '20080515155555'
                                      },
#12
                                      {
                                        'parts' => [
                                                     {},
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 1,
                                                       'status' => 'OK',
                                                       'filenum' => 2,
                                                       'kb' => 1024,
						       'part_offset' => 0,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'partnum' => 2,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'status' => 'OK',
                                                       'filenum' => 3,
                                                       'kb' => 1024,
						       'part_offset' => 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'partnum' => 3,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'kb' => 1024,
                                                       'filenum' => 4,
                                                       'status' => 'OK',
						       'part_offset' => 2 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 4,
                                                       'status' => 'OK',
                                                       'filenum' => 5,
                                                       'kb' => 1024,
						       'part_offset' => 3 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'kb' => 1024,
                                                       'filenum' => 6,
                                                       'status' => 'OK',
                                                       'partnum' => 5,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
						       'part_offset' => 4 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'filenum' => 7,
                                                       'kb' => 1024,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 6,
						       'part_offset' => 5 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'filenum' => 8,
                                                       'kb' => 1024,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 7,
						       'part_offset' => 6 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'filenum' => 9,
                                                       'kb' => 1024,
                                                       'partnum' => 8,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
						       'part_offset' => 7 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'kb' => 1024,
                                                       'filenum' => 10,
                                                       'status' => 'OK',
                                                       'partnum' => 9,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
						       'part_offset' => 8 * 1024 * 1024,
						       'part_size' => 1024 * 1024,
                                                     },
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 10,
                                                       'filenum' => 11,
                                                       'kb' => 284,
                                                       'status' => 'OK',
						       'part_offset' => 9 * 1024 * 1024,
						       'part_size' => 284 * 1024,
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '9500',
                                        'nparts' => '10',
                                        'diskname' => '/lib',
                                        'dump_timestamp' => '20080313133333',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '13',
                                        'write_timestamp' => '20080313133333',
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'somebox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'level' => '0'
                                      },
#13
                                      {
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'somebox',
                                        'level' => '0',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '12',
                                        'write_timestamp' => '20080222222222',
                                        'diskname' => '/lib',
                                        'nparts' => '2',
                                        'dump_timestamp' => '20080222222222',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'status' => 'OK',
                                                       'kb' => 100,
                                                       'filenum' => 1,
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-002',
						       'part_offset' => 0,
						       'part_size' => 100 * 1024,
                                                     },
                                                     {
                                                       'partnum' => 2,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-002',
                                                       'kb' => 72,
                                                       'filenum' => 2,
                                                       'status' => 'OK',
						       'part_offset' => 100 * 1024,
						       'part_size' => 72 * 1024,
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'kb' => '172',
                                        'native_crc' => '00000000:0'
                                      },
#14
                                      {
                                        'diskname' => '/lib',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20080111000000',
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'status' => 'OK',
                                                       'kb' => 419,
                                                       'filenum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-001',
                                                       'partnum' => 1,
						       'part_offset' => 0,
						       'part_size' => 419 * 1024,
                                                     }
                                                   ],
                                        'native_crc' => '00000000:0',
                                        'kb' => '419',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'somebox',
                                        'orig_kb' => '0',
                                        'level' => '0',
                                        'write_timestamp' => '20080111000000',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '11',
                                      },
#15
                                      {
                                        'write_timestamp' => '20080616166666',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '18',
                                        'hostname' => 'somebox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'orig_kb' => 20,
                                        'level' => 1,
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 20,
                                                       'filenum' => 1,
                                                       'status' => 'OK',
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-007',
                                                       'partnum' => 1,
						       'part_offset' => 0,
						       'part_size' => 20 * 1024,
                                                     }
                                                   ],
                                        'kb' => '20',
                                        'native_crc' => '00000000:0',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'diskname' => '/usr/bin',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20080616166666',
                                      },
#16
                                      {
                                        'level' => 1,
                                        'hostname' => 'somebox',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'orig_kb' => 20,
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '17',
                                        'dump_timestamp' => '20080515155555',
                                        'nparts' => '1',
                                        'diskname' => '/usr/bin',
                                        'kb' => '20',
                                        'native_crc' => '00000000:0',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'status' => 'OK',
                                                       'kb' => 20,
                                                       'filenum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 1,
						       'part_offset' => 0,
						       'part_size' => 20 * 1024,
                                                     }
                                                   ]
                                      },
#17
                                      {
                                        'dump_timestamp' => '20080313133333',
                                        'nparts' => '1',
                                        'diskname' => '/usr/bin',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '20',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'status' => 'OK',
                                                       'filenum' => 1,
                                                       'kb' => 20,
						       'part_offset' => 0,
						       'part_size' => 20 * 1024,
                                                     }
                                                   ],
                                        'message' => '',
                                        'level' => 1,
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'somebox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080313133333',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '16',
                                      },
                                    ],
            'severity' => $Amanda::Message::SUCCESS,
            'message' => 'The dumps',
	    'process' => 'Amanda::Rest::Dumps',
	    'running_on' => 'amanda-server',
	    'component' => 'rest-server',
	    'module' => 'amanda',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    };
is_deeply (sort_reply(Installcheck::Config::remove_source_line($reply)), $a,
    "All hosts") || diag(Data::Dumper::Dumper(Installcheck::Config::remove_source_line($reply)));
$rest->stop();
exit;

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/otherbox?parts=1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
                         'dumps' => [
#0
                                      {
                                        'message' => '',
                                        'dump_timestamp' => '20080515155555',
                                        'hostname' => 'otherbox',
                                        'level' => '0',
                                        'orig_kb' => 350,
                                        'kb' => '190',
                                        'nparts' => '1',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'kb' => 190,
                                                       'status' => 'OK',
                                                       'partnum' => 1,
                                                       'filenum' => 14,
						       'part_offset' => 0,
						       'part_size' => 190 * 1024,
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'diskname' => '/direct',
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_id' => '3',
                                        'copy_status' => 'OK',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF'
                                      },
#1
                                      {
                                        'hostname' => 'otherbox',
                                        'message' => '',
                                        'dump_timestamp' => '20080511151555',
                                        'kb' => '190',
                                        'level' => '0',
                                        'orig_kb' => 190,
                                        'nparts' => '1',
                                        'client_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 13,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 1,
                                                       'kb' => 190,
                                                       'status' => 'OK',
						       'part_offset' => 0,
						       'part_size' => 190 * 1024,
                                                     }
                                                   ],
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'diskname' => '/lib',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '7',
                                        'write_timestamp' => '20080515155555',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'native_crc' => '00000000:0'
                                      },
#2
                                      {
                                        'server_crc' => '00000000:0',
                                        'diskname' => '/lib',
                                        'bytes' => '0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-005',
                                                       'partnum' => 1,
                                                       'kb' => 32,
                                                       'status' => 'PARTIAL',
						       'part_offset' => 0,
						       'part_size' => 32 * 1024,
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080414144444',
                                        'status' => 'PARTIAL',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'PARTIAL',
                                        'copy_id' => '5',
                                        'message' => 'full-up',
                                        'dump_timestamp' => '20080414144444',
                                        'hostname' => 'otherbox',
                                        'nparts' => '0',
                                        'orig_kb' => '0',
                                        'level' => 1,
                                        'kb' => '32'
                                      },
#3
                                      {
                                        'server_crc' => '00000000:0',
                                        'bytes' => '262144',
                                        'diskname' => '/lib',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => '1',
                                                       'kb' => 256,
                                                       'status' => 'OK',
                                                       'pool' => 'HOLDING',
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
						       'part_offset' => 0,
						       'part_size' => 256 * 1024,
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'write_timestamp' => '00000000000000',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '6',
                                        'message' => '',
                                        'dump_timestamp' => '20080414144444',
                                        'hostname' => 'otherbox',
                                        'nparts' => '1',
                                        'level' => 1,
                                        'orig_kb' => '0',
                                        'kb' => 256
                                      },
#4
                                      {
                                        'bytes' => '0',
                                        'diskname' => '/lib',
                                        'server_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'status' => 'OK',
                                                       'kb' => 190,
                                                       'partnum' => 1,
                                                       'filenum' => 14,
						       'part_offset' => 0,
						       'part_size' => 190 * 1024,
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080313133333',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '4',
                                        'dump_timestamp' => '20080313133333',
                                        'message' => '',
                                        'hostname' => 'otherbox',
                                        'nparts' => '1',
                                        'level' => '0',
                                        'orig_kb' => '0',
                                        'kb' => '190'
                                      },
#5
                                      {
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '10',
                                        'server_crc' => '00000000:0',
                                        'diskname' => '/usr/bin',
                                        'bytes' => '0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 240,
                                                       'status' => 'OK',
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'filenum' => 12,
						       'part_offset' => 0,
						       'part_size' => 240 * 1024,
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'nparts' => '1',
                                        'level' => '0',
                                        'orig_kb' => 240,
                                        'kb' => '240',
                                        'message' => '',
                                        'dump_timestamp' => '20080511151155',
                                        'hostname' => 'otherbox',
                                      },
#6
                                      {
                                        'kb' => '240',
                                        'orig_kb' => '0',
                                        'level' => '0',
                                        'nparts' => '1',
                                        'hostname' => 'otherbox',
                                        'dump_timestamp' => '20080311132233',
                                        'message' => '',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '9',
                                        'write_timestamp' => '20080313133333',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'native_crc' => '00000000:0',
                                        'client_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 12,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 1,
                                                       'status' => 'OK',
                                                       'kb' => 240,
						       'part_offset' => 0,
						       'part_size' => 240 * 1024,
                                                     }
                                                   ],
                                        'bytes' => '0',
                                        'diskname' => '/usr/bin',
                                        'server_crc' => '00000000:0'
                                      },
#7
                                      {
                                        'write_timestamp' => '20080313133333',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '8',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 13,
                                                       'status' => 'OK',
                                                       'kb' => 240,
                                                       'partnum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
						       'part_offset' => 0,
						       'part_size' => 240 * 1024,
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'diskname' => '/usr/bin',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'level' => '0',
                                        'orig_kb' => '0',
                                        'kb' => '240',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20080311131133',
                                        'message' => '',
                                        'hostname' => 'otherbox'
                                      },
                                    ],
            'severity' => $Amanda::Message::SUCCESS,
            'message' => 'The dumps',
	    'process' => 'Amanda::Rest::Dumps',
	    'running_on' => 'amanda-server',
	    'component' => 'rest-server',
	    'module' => 'amanda',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    },
    "One host, all disk==1") || diag(Data::Dumper::Dumper($reply));;

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/otherbox?disk=/lib&parts=1");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
                         'dumps' => [
#0
                                      {
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'diskname' => '/lib',
                                        'orig_kb' => 190,
                                        'message' => '',
                                        'dump_timestamp' => '20080511151555',
                                        'hostname' => 'otherbox',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 190,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 1,
                                                       'filenum' => 13,
                                                       'status' => 'OK',
						       'part_offset' => 0,
						       'part_size' => 190 * 1024,
                                                     }
                                                   ],
                                        'level' => '0',
                                        'server_crc' => '00000000:0',
                                        'nparts' => '1',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '7',
                                        'kb' => '190',
                                        'native_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'write_timestamp' => '20080515155555',
                                      },
#1
                                      {
                                        'native_crc' => '00000000:0',
                                        'kb' => '32',
                                        'write_timestamp' => '20080414144444',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'nparts' => '0',
                                        'status' => 'PARTIAL',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'PARTIAL',
                                        'copy_id' => '5',
                                        'dump_timestamp' => '20080414144444',
                                        'message' => 'full-up',
                                        'orig_kb' => '0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => 1,
                                                       'status' => 'PARTIAL',
                                                       'filenum' => 1,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-005',
                                                       'kb' => 32,
						       'part_offset' => 0,
						       'part_size' => 32 * 1024,
                                                     }
                                                   ],
                                        'hostname' => 'otherbox',
                                        'level' => 1,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'diskname' => '/lib',
                                        'client_crc' => '00000000:0'
                                      },
#2
                                      {
                                        'orig_kb' => '0',
                                        'message' => '',
                                        'dump_timestamp' => '20080414144444',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => '1',
                                                       'pool' => 'HOLDING',
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                                       'status' => 'OK',
                                                       'kb' => 256,
						       'part_offset' => 0,
						       'part_size' => 256 * 1024,
                                                     }
                                                   ],
                                        'hostname' => 'otherbox',
                                        'level' => 1,
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'client_crc' => '00000000:0',
                                        'diskname' => '/lib',
                                        'kb' => 256,
                                        'native_crc' => '00000000:0',
                                        'bytes' => '262144',
                                        'write_timestamp' => '00000000000000',
                                        'server_crc' => '00000000:0',
                                        'nparts' => '1',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '6',
                                      },
#3
                                      {
                                        'orig_kb' => '0',
                                        'message' => '',
                                        'dump_timestamp' => '20080313133333',
                                        'hostname' => 'otherbox',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 190,
                                                       'pool' => 'TESTCONF',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 1,
                                                       'status' => 'OK',
                                                       'filenum' => 14,
						       'part_offset' => 0,
						       'part_size' => 190 * 1024,
                                                     }
                                                   ],
                                        'level' => '0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'diskname' => '/lib',
                                        'kb' => '190',
                                        'native_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'write_timestamp' => '20080313133333',
                                        'server_crc' => '00000000:0',
                                        'nparts' => '1',
                                        'status' => 'OK',
                                        'dump_status' => 'OK',
                                        'copy_status' => 'OK',
                                        'copy_id' => '4',
                                      },
                                    ],
            'severity' => $Amanda::Message::SUCCESS,
            'message' => 'The dumps',
	    'process' => 'Amanda::Rest::Dumps',
	    'running_on' => 'amanda-server',
	    'component' => 'rest-server',
	    'module' => 'amanda',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    },
    "One host, one disk") || diag(Data::Dumper::Dumper($reply));;

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/nohost&parts=1");
is_deeply (sort_reply(Installcheck::Config::remove_source_line($reply)),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
	    'dumps' => [],
            'severity' => $Amanda::Message::SUCCESS,
            'message' => 'The dumps',
	    'process' => 'Amanda::Rest::Dumps',
	    'running_on' => 'amanda-server',
	    'component' => 'rest-server',
	    'module' => 'amanda',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    },
    "no host");


$rest->stop();

1;
