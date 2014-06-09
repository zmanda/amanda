# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
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
plan tests => 5;

# set up and load a simple config
my $testconf = Installcheck::Run::setup();
$testconf->write();

my $reply;
my $amperldir = $Amanda::Paths::amperldir;

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
                'severity' => '16',
                'message' => "The dumps",
		'dumps' => [],
                'code' => '2600000'
          },
        ],
      http_code => 200,
    },
    "No dumps");

my $cat = Installcheck::Catalogs::load("bigdb");
$cat->install();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
            'dumps' => [
                         {
                           'bytes' => 0,
                           'status' => 'OK',
                           'diskname' => '/lib',
                           'hostname' => 'otherbox',
                           'native_crc' => '00000000:0',
                           'dump_timestamp' => '20080414144444',
                           'write_timestamp' => '00000000000000',
                           'client_crc' => '00000000:0',
                           'kb' => 256,
                           'message' => '',
                           'level' => 1,
                           'sec' => 0,
                           'orig_kb' => 0,
                           'parts' => [
                                        {},
                                        {
                                          'status' => 'OK',
                                          'client_crc' => '00000000:0',
                                          'kb' => 256,
                                          'native_crc' => '00000000:0',
                                          'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                          'sec' => 0,
                                          'orig_kb' => 0,
                                          'server_crc' => '00000000:0',
                                          'partnum' => 1
                                        }
                                      ],
                           'server_crc' => '00000000:0',
                           'nparts' => 1,
                           'storage' => 'HOLDING'
                         },
                         {
                          'bytes' => 0,
                           'status' => 'OK',
                           'diskname' => '/opt',
                           'hostname' => 'oldbox',
                           'native_crc' => '00000000:0',
                           'dump_timestamp' => '20080414144444',
                           'write_timestamp' => '00000000000000',
                           'client_crc' => '00000000:0',
                           'kb' => 1298,
                           'message' => '',
                           'level' => 0,
                           'sec' => 0,
                           'orig_kb' => 0,
                           'parts' => [
                                        {},
                                        {
                                          'status' => 'OK',
                                          'client_crc' => '00000000:0',
                                          'kb' => 1298,
                                          'native_crc' => '00000000:0',
                                          'holding_file' => "$Installcheck::TMP/holding/20080414144444/oldbox._opt",
                                          'sec' => 0,
                                          'orig_kb' => 0,
                                          'server_crc' => '00000000:0',
                                          'partnum' => 1
                                        }
                                      ],
                           'server_crc' => '00000000:0',
                           'nparts' => 1,
                           'storage' => 'HOLDING'
                         }
                       ],
            'severity' => '16',
            'message' => 'The dumps',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    },
    "All hosts");

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/otherbox");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
            'dumps' => [
                         {
                           'bytes' => 0,
                           'status' => 'OK',
                           'diskname' => '/lib',
                           'hostname' => 'otherbox',
                           'native_crc' => '00000000:0',
                           'dump_timestamp' => '20080414144444',
                           'write_timestamp' => '00000000000000',
                           'client_crc' => '00000000:0',
                           'kb' => 256,
                           'message' => '',
                           'level' => 1,
                           'sec' => 0,
                           'orig_kb' => 0,
                           'parts' => [
                                        {},
                                        {
                                          'status' => 'OK',
                                          'client_crc' => '00000000:0',
                                          'kb' => 256,
                                          'native_crc' => '00000000:0',
                                          'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                          'sec' => 0,
                                          'orig_kb' => 0,
                                          'server_crc' => '00000000:0',
                                          'partnum' => 1
                                        }
                                      ],
                           'server_crc' => '00000000:0',
                           'nparts' => 1,
                           'storage' => 'HOLDING'
                         }
                       ],
            'severity' => '16',
            'message' => 'The dumps',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    },
    "One host, all disk==1");

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/otherbox?disk=/lib");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
            'dumps' => [
                         {
                           'bytes' => 0,
                           'status' => 'OK',
                           'diskname' => '/lib',
                           'hostname' => 'otherbox',
                           'native_crc' => '00000000:0',
                           'dump_timestamp' => '20080414144444',
                           'write_timestamp' => '00000000000000',
                           'client_crc' => '00000000:0',
                           'kb' => 256,
                           'message' => '',
                           'level' => 1,
                           'sec' => 0,
                           'orig_kb' => 0,
                           'parts' => [
                                        {},
                                        {
                                          'status' => 'OK',
                                          'client_crc' => '00000000:0',
                                          'kb' => 256,
                                          'native_crc' => '00000000:0',
                                          'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                          'sec' => 0,
                                          'orig_kb' => 0,
                                          'server_crc' => '00000000:0',
                                          'partnum' => 1
                                        }
                                      ],
                           'server_crc' => '00000000:0',
                           'nparts' => 1,
                           'storage' => 'HOLDING'
                         }
                       ],
            'severity' => '16',
            'message' => 'The dumps',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    },
    "One host, one disk");

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/nohost");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
	    'dumps' => [],
            'severity' => '16',
            'message' => 'The dumps',
            'code' => '2600000'
          }
         ],
      http_code => 200,
    },
    "no host");

#diag("reply: " .Data::Dumper::Dumper($reply));

$rest->stop();

1;
