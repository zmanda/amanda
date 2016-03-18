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
plan tests => 5;

# set up and load a simple config
my $testconf = Installcheck::Run::setup();
$testconf->write();

my $reply;
my $amperldir = $Amanda::Paths::amperldir;

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps");
is_deeply (sort_reply(Installcheck::Rest::remove_source_line($reply)),
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

sub sort_dumps {
    my $dumps = shift;

    my @sorted = sort { $a->{'hostname'} cmp $b->{'hostname'} ||
			$a->{'diskname'} cmp $b->{'diskname'} ||
			$a->{'dump_timestamp'} cmp $b->{'dump_timestamp'} ||
			$a->{'write_timestamp'} cmp $b->{'write_timestamp'}} @{$dumps};
    return \@sorted;
}

sub sort_reply {
    my $reply = shift;

    if (defined $reply->{'body'}[0]->{'dumps'}) {
	$reply->{'body'}[0]->{'dumps'} = sort_dumps($reply->{'body'}[0]->{'dumps'});
    }
    return $reply;
}

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps");
is_deeply (sort_reply(Installcheck::Rest::remove_source_line($reply)),
    sort_reply({ body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
            'dumps' => [
                                      {
                                        'diskname' => '/home/ada',
                                        'nparts' => '1',
                                        'sec' => '0.883',
                                        'dump_timestamp' => '20100722000000',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'partnum' => 1,
                                                       'label' => 'Conf-009',
                                                       'sec' => '0.883',
                                                       'status' => 'OK',
                                                       'filenum' => 1,
                                                       'kb' => 166976,
                                                       'native_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0'
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
                                        'write_timestamp' => '20100722000000'
                                      },
                                      {
                                        'write_timestamp' => '00000000000000',
                                        'status' => 'OK',
                                        'level' => '0',
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'hostname' => 'oldbox',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'kb' => 1298,
                                        'native_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 1298,
                                                       'native_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0',
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/oldbox._opt",
                                                       'status' => 'OK',
                                                       'partnum' => '1',
                                                       'sec' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0'
                                                     }
                                                   ],
                                        'message' => '',
                                        'dump_timestamp' => '20080414144444',
                                        'sec' => '0',
                                        'nparts' => '1',
                                        'diskname' => '/opt'
                                      },
                                      {
                                        'orig_kb' => 240,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'level' => '0',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080515155555',
                                        'nparts' => '1',
                                        'diskname' => '/usr/bin',
                                        'sec' => '0.002733',
                                        'dump_timestamp' => '20080511151155',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 240,
                                                       'partnum' => 1,
                                                       'label' => 'Conf-006',
                                                       'sec' => '0.002733',
                                                       'status' => 'OK',
                                                       'kb' => 240,
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 12,
                                                       'server_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'kb' => '240',
                                        'native_crc' => '00000000:0'
                                      },
                                      {
                                        'level' => '0',
                                        'orig_kb' => 190,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'sec' => '0.001733',
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
                                                       'server_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 13,
                                                       'kb' => 190,
                                                       'status' => 'OK',
                                                       'sec' => '0.001733',
                                                       'partnum' => 1,
                                                       'label' => 'Conf-006',
                                                       'orig_kb' => 190,
                                                       'client_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'message' => ''
                                      },
                                      {
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'kb' => '32',
                                        'native_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 1,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 32,
                                                       'status' => 'PARTIAL',
                                                       'sec' => '0.00054',
                                                       'partnum' => 1,
                                                       'label' => 'Conf-005',
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'message' => 'full-up',
                                        'sec' => '0.00054',
                                        'dump_timestamp' => '20080414144444',
                                        'diskname' => '/lib',
                                        'nparts' => '0',
                                        'write_timestamp' => '20080414144444',
                                        'status' => 'PARTIAL',
                                        'level' => 1,
                                        'orig_kb' => '0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'otherbox',
                                        'client_crc' => '00000000:0'
                                      },
                                      {
                                        'diskname' => '/boot',
                                        'nparts' => '0',
                                        'sec' => '0',
                                        'dump_timestamp' => '20080414144444',
                                        'message' => 'error no-space',
                                        'kb' => '0',
                                        'native_crc' => undef,
                                        'bytes' => '0',
                                        'server_crc' => undef,
                                        'hostname' => 'otherbox',
                                        'client_crc' => undef,
                                        'storage' => 'TESTCONF',
#                                        'pool' => 'TESTCONF',
                                        'orig_kb' => undef,
                                        'level' => '0',
                                        'status' => 'FAILED',
                                        'write_timestamp' => '20080414144444'
                                      },
                                      {
                                        'sec' => '0.001733',
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
                                                       'native_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'partnum' => 1,
                                                       'label' => 'Conf-003',
                                                       'sec' => '0.001733',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0'
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
                                        'write_timestamp' => '20080313133333'
                                      },
                                      {
                                        'kb' => '240',
                                        'native_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'partnum' => 1,
                                                       'label' => 'Conf-003',
                                                       'sec' => '0.002733',
                                                       'status' => 'OK',
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 240,
                                                       'filenum' => 13,
                                                       'server_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'sec' => '0.002733',
                                        'dump_timestamp' => '20080311131133',
                                        'nparts' => '1',
                                        'diskname' => '/usr/bin',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080313133333',
                                        'level' => '0',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'otherbox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'orig_kb' => '0'
                                      },
                                      {
                                        'nparts' => '1',
                                        'diskname' => '/direct',
                                        'sec' => '0.001',
                                        'dump_timestamp' => '20080515155555',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'server_crc' => '00000000:0',
                                                       'kb' => 190,
                                                       'filenum' => 14,
                                                       'native_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'sec' => '0.001',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 1,
                                                       'orig_kb' => 350,
                                                       'client_crc' => '00000000:0'
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
                                        'status' => 'OK'
                                      },
                                      {
                                        'level' => 1,
                                        'hostname' => 'otherbox',
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'client_crc' => '00000000:0',
                                        'orig_kb' => '0',
                                        'write_timestamp' => '00000000000000',
                                        'status' => 'OK',
                                        'sec' => '0',
                                        'dump_timestamp' => '20080414144444',
                                        'diskname' => '/lib',
                                        'nparts' => '1',
                                        'kb' => 256,
                                        'native_crc' => '00000000:0',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                                       'status' => 'OK',
                                                       'kb' => 256,
                                                       'native_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'partnum' => '1',
                                                       'sec' => '0'
                                                     }
                                                   ]
                                      },
                                      {
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 1,
                                                       'sec' => '0.002733',
                                                       'status' => 'OK',
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 12,
                                                       'kb' => 240,
                                                       'server_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'kb' => '240',
                                        'native_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'diskname' => '/usr/bin',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20080311132233',
                                        'sec' => '0.002733',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080313133333',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'otherbox',
                                        'orig_kb' => '0',
                                        'level' => '0'
                                      },
                                      {
                                        'level' => 1,
                                        'hostname' => 'somebox',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'orig_kb' => 20,
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'dump_timestamp' => '20080515155555',
                                        'sec' => '0.00037',
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
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 1,
                                                       'server_crc' => '00000000:0',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 20,
                                                       'label' => 'Conf-006',
                                                       'partnum' => 1,
                                                       'sec' => '0.00037'
                                                     }
                                                   ]
                                      },
                                      {
                                        'dump_timestamp' => '20080313133333',
                                        'sec' => '0.00037',
                                        'nparts' => '1',
                                        'diskname' => '/usr/bin',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '20',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'partnum' => 1,
                                                       'label' => 'Conf-003',
                                                       'sec' => '0.00037',
                                                       'status' => 'OK',
                                                       'filenum' => 1,
                                                       'kb' => 20,
                                                       'native_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0'
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
                                        'status' => 'OK'
                                      },
                                      {
                                        'diskname' => '/lib',
                                        'nparts' => '1',
                                        'sec' => '4.813543',
                                        'dump_timestamp' => '20080111000000',
                                        'message' => '',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'kb' => 419,
                                                       'filenum' => 1,
                                                       'native_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '4.813543',
                                                       'label' => 'Conf-001',
                                                       'partnum' => 1
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
                                        'status' => 'OK'
                                      },
                                      {
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'somebox',
                                        'level' => '0',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080222222222',
                                        'diskname' => '/lib',
                                        'nparts' => '2',
                                        'sec' => '0.001161',
                                        'dump_timestamp' => '20080222222222',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'status' => 'OK',
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 100,
                                                       'filenum' => 1,
                                                       'server_crc' => '00000000:0',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'partnum' => 1,
                                                       'label' => 'Conf-002',
                                                       'sec' => '0.000733'
                                                     },
                                                     {
                                                       'partnum' => 2,
                                                       'label' => 'Conf-002',
                                                       'sec' => '0.000428',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 72,
                                                       'filenum' => 2,
                                                       'server_crc' => '00000000:0',
                                                       'status' => 'OK'
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'kb' => '172',
                                        'native_crc' => '00000000:0'
                                      },
                                      {
                                        'write_timestamp' => '20080616166666',
                                        'status' => 'OK',
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
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 20,
                                                       'filenum' => 1,
                                                       'server_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'label' => 'Conf-007',
                                                       'partnum' => 1,
                                                       'sec' => '0.00037',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 20
                                                     }
                                                   ],
                                        'kb' => '20',
                                        'native_crc' => '00000000:0',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'diskname' => '/usr/bin',
                                        'nparts' => '1',
                                        'dump_timestamp' => '20080616166666',
                                        'sec' => '0.00037'
                                      },
                                      {
                                        'parts' => [
                                                     {},
                                                     {
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.005621',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 1,
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 2,
                                                       'kb' => 1024
                                                     },
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'partnum' => 2,
                                                       'label' => 'Conf-003',
                                                       'sec' => '0.006527',
                                                       'status' => 'OK',
                                                       'filenum' => 3,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'server_crc' => '00000000:0'
                                                     },
                                                     {
                                                       'sec' => '0.005854',
                                                       'partnum' => 3,
                                                       'label' => 'Conf-003',
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'filenum' => 4,
                                                       'native_crc' => '00000000:0',
                                                       'status' => 'OK'
                                                     },
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 4,
                                                       'sec' => '0.007344',
                                                       'status' => 'OK',
                                                       'filenum' => 5,
                                                       'kb' => 1024,
                                                       'native_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0'
                                                     },
                                                     {
                                                       'server_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 6,
                                                       'status' => 'OK',
                                                       'sec' => '0.007344',
                                                       'partnum' => 5,
                                                       'label' => 'Conf-003',
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0'
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 7,
                                                       'kb' => 1024,
                                                       'native_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.007344',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 6
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 8,
                                                       'kb' => 1024,
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.007344',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 7
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 9,
                                                       'kb' => 1024,
                                                       'native_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.007344',
                                                       'partnum' => 8,
                                                       'label' => 'Conf-003'
                                                     },
                                                     {
                                                       'server_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'filenum' => 10,
                                                       'native_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'sec' => '0.007344',
                                                       'partnum' => 9,
                                                       'label' => 'Conf-003',
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0'
                                                     },
                                                     {
                                                       'sec' => '0.001919',
                                                       'label' => 'Conf-003',
                                                       'partnum' => 10,
                                                       'orig_kb' => '0',
                                                       'client_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 11,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 284,
                                                       'status' => 'OK'
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '3156',
                                        'nparts' => '10',
                                        'diskname' => '/lib',
                                        'sec' => '0.051436',
                                        'dump_timestamp' => '20080313133333',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080313133333',
                                        'orig_kb' => '0',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'somebox',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'level' => '0'
                                      },
                                      {
                                        'nparts' => '10',
                                        'diskname' => '/lib',
                                        'sec' => '0.051436',
                                        'dump_timestamp' => '20080515155555',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'status' => 'OK',
                                                       'filenum' => 2,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'server_crc' => '00000000:0',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 3156,
                                                       'partnum' => 1,
                                                       'label' => 'Conf-006',
                                                       'sec' => '0.005621'
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'filenum' => 3,
                                                       'orig_kb' => 3156,
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.006527',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 2
                                                     },
                                                     {
                                                       'orig_kb' => 3156,
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.005854',
                                                       'partnum' => 3,
                                                       'label' => 'Conf-006',
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 4,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 1024
                                                     },
                                                     {
                                                       'label' => 'Conf-006',
                                                       'partnum' => 4,
                                                       'sec' => '0.007344',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 3156,
                                                       'kb' => 1024,
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 5,
                                                       'server_crc' => '00000000:0',
                                                       'status' => 'OK'
                                                     },
                                                     {
                                                       'status' => 'OK',
                                                       'filenum' => 6,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'server_crc' => '00000000:0',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 3156,
                                                       'partnum' => 5,
                                                       'label' => 'Conf-006',
                                                       'sec' => '0.007344'
                                                     },
                                                     {
                                                       'kb' => 1024,
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 7,
                                                       'server_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'partnum' => 6,
                                                       'label' => 'Conf-006',
                                                       'sec' => '0.007344',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 3156
                                                     },
                                                     {
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 3156,
                                                       'label' => 'Conf-006',
                                                       'partnum' => 7,
                                                       'sec' => '0.007344',
                                                       'status' => 'OK',
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 8,
                                                       'kb' => 1024,
                                                       'server_crc' => '00000000:0'
                                                     },
                                                     {
                                                       'orig_kb' => 3156,
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.007344',
                                                       'partnum' => 8,
                                                       'label' => 'Conf-006',
                                                       'status' => 'OK',
                                                       'server_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'filenum' => 9,
                                                       'native_crc' => '00000000:0'
                                                     },
                                                     {
                                                       'filenum' => 10,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 1024,
                                                       'server_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'label' => 'Conf-006',
                                                       'partnum' => 9,
                                                       'sec' => '0.007344',
                                                       'client_crc' => '00000000:0',
                                                       'orig_kb' => 3156
                                                     },
                                                     {
                                                       'sec' => '0.001919',
                                                       'partnum' => 10,
                                                       'label' => 'Conf-006',
                                                       'orig_kb' => 3156,
                                                       'client_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 11,
                                                       'kb' => 284,
                                                       'status' => 'OK'
                                                     }
                                                   ],
                                        'message' => '',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'kb' => '3156',
                                        'orig_kb' => 3156,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'client_crc' => '00000000:0',
                                        'hostname' => 'somebox',
                                        'level' => '0',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080515155555'
                                      },
                                      {
                                        'write_timestamp' => '20080616166666',
                                        'status' => 'FAIL',
                                        'orig_kb' => 20,
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'hostname' => 'somebox',
                                        'client_crc' => '00000000:0',
                                        'level' => 1,
                                        'parts' => [
                                                     {},
                                                     {
                                                       'sec' => '0.00037',
                                                       'label' => 'Conf-007',
                                                       'partnum' => 1,
                                                       'orig_kb' => 20,
                                                       'client_crc' => '00000000:0',
                                                       'server_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'filenum' => 2,
                                                       'kb' => 20,
                                                       'status' => 'OK'
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
                                        'sec' => '0.00037'
                                      }
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
    }),
    "All hosts") || diag(Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/otherbox");
is_deeply (sort_reply(Installcheck::Rest::remove_source_line($reply)),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
                         'dumps' => [
                                      {
                                        'message' => 'error no-space',
                                        'dump_timestamp' => '20080414144444',
                                        'hostname' => 'otherbox',
                                        'sec' => '0',
                                        'nparts' => '0',
                                        'orig_kb' => undef,
                                        'level' => '0',
                                        'kb' => '0',
                                        'server_crc' => undef,
                                        'diskname' => '/boot',
                                        'bytes' => '0',
                                        'client_crc' => undef,
                                        'native_crc' => undef,
                                        'storage' => 'TESTCONF',
#                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080414144444',
                                        'status' => 'FAILED'
                                      },
                                      {
                                        'sec' => '0.001',
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
                                                       'label' => 'Conf-006',
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 190,
                                                       'status' => 'OK',
                                                       'partnum' => 1,
                                                       'orig_kb' => 350,
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 14,
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.001'
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'diskname' => '/direct',
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF'
                                      },
                                      {
                                        'bytes' => '0',
                                        'diskname' => '/lib',
                                        'server_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'label' => 'Conf-003',
                                                       'native_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'kb' => 190,
                                                       'partnum' => 1,
                                                       'orig_kb' => '0',
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 14,
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.001733'
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080313133333',
                                        'status' => 'OK',
                                        'dump_timestamp' => '20080313133333',
                                        'message' => '',
                                        'hostname' => 'otherbox',
                                        'sec' => '0.001733',
                                        'nparts' => '1',
                                        'level' => '0',
                                        'orig_kb' => '0',
                                        'kb' => '190'
                                      },
                                      {
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'diskname' => '/lib',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => '1',
                                                       'kb' => 256,
                                                       'status' => 'OK',
                                                       'orig_kb' => '0',
                                                       'native_crc' => '00000000:0',
                                                       'client_crc' => '00000000:0',
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                                       'sec' => '0',
                                                       'server_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'write_timestamp' => '00000000000000',
                                        'status' => 'OK',
                                        'message' => '',
                                        'dump_timestamp' => '20080414144444',
                                        'hostname' => 'otherbox',
                                        'sec' => '0',
                                        'nparts' => '1',
                                        'level' => 1,
                                        'orig_kb' => '0',
                                        'kb' => 256
                                      },
                                      {
                                        'server_crc' => '00000000:0',
                                        'diskname' => '/lib',
                                        'bytes' => '0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 1,
                                                       'server_crc' => '00000000:0',
                                                       'sec' => '0.00054',
                                                       'client_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'label' => 'Conf-005',
                                                       'orig_kb' => '0',
                                                       'partnum' => 1,
                                                       'kb' => 32,
                                                       'status' => 'PARTIAL'
                                                     }
                                                   ],
                                        'client_crc' => '00000000:0',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080414144444',
                                        'status' => 'PARTIAL',
                                        'message' => 'full-up',
                                        'dump_timestamp' => '20080414144444',
                                        'hostname' => 'otherbox',
                                        'sec' => '0.00054',
                                        'nparts' => '0',
                                        'orig_kb' => '0',
                                        'level' => 1,
                                        'kb' => '32'
                                      },
                                      {
                                        'sec' => '0.001733',
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
                                                       'server_crc' => '00000000:0',
                                                       'sec' => '0.001733',
                                                       'client_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'label' => 'Conf-006',
                                                       'orig_kb' => 190,
                                                       'partnum' => 1,
                                                       'kb' => 190,
                                                       'status' => 'OK'
                                                     }
                                                   ],
                                        'server_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'diskname' => '/lib',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080515155555',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'native_crc' => '00000000:0'
                                      },
                                      {
                                        'write_timestamp' => '20080313133333',
                                        'status' => 'OK',
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'sec' => '0.002733',
                                                       'client_crc' => '00000000:0',
                                                       'filenum' => 13,
                                                       'server_crc' => '00000000:0',
                                                       'orig_kb' => '0',
                                                       'status' => 'OK',
                                                       'kb' => 240,
                                                       'partnum' => 1,
                                                       'label' => 'Conf-003',
                                                       'native_crc' => '00000000:0'
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
                                        'sec' => '0.002733',
                                        'dump_timestamp' => '20080311131133',
                                        'message' => '',
                                        'hostname' => 'otherbox'
                                      },
                                      {
                                        'kb' => '240',
                                        'orig_kb' => '0',
                                        'level' => '0',
                                        'nparts' => '1',
                                        'sec' => '0.002733',
                                        'hostname' => 'otherbox',
                                        'dump_timestamp' => '20080311132233',
                                        'message' => '',
                                        'status' => 'OK',
                                        'write_timestamp' => '20080313133333',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'native_crc' => '00000000:0',
                                        'client_crc' => '00000000:0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'filenum' => 12,
                                                       'server_crc' => '00000000:0',
                                                       'sec' => '0.002733',
                                                       'client_crc' => '00000000:0',
                                                       'native_crc' => '00000000:0',
                                                       'label' => 'Conf-003',
                                                       'orig_kb' => '0',
                                                       'partnum' => 1,
                                                       'status' => 'OK',
                                                       'kb' => 240
                                                     }
                                                   ],
                                        'bytes' => '0',
                                        'diskname' => '/usr/bin',
                                        'server_crc' => '00000000:0'
                                      },
                                      {
                                        'native_crc' => '00000000:0',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'write_timestamp' => '20080515155555',
                                        'status' => 'OK',
                                        'server_crc' => '00000000:0',
                                        'diskname' => '/usr/bin',
                                        'bytes' => '0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'kb' => 240,
                                                       'status' => 'OK',
                                                       'partnum' => 1,
                                                       'orig_kb' => 240,
                                                       'label' => 'Conf-006',
                                                       'native_crc' => '00000000:0',
                                                       'client_crc' => '00000000:0',
                                                       'sec' => '0.002733',
                                                       'server_crc' => '00000000:0',
                                                       'filenum' => 12
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
                                        'sec' => '0.002733'
                                      }
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

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/otherbox?disk=/lib");
is_deeply (sort_reply(Installcheck::Rest::remove_source_line($reply)),
    { body =>
        [ { 'source_filename' => "$amperldir/Amanda/Rest/Dumps.pm",
                         'dumps' => [
                                      {
                                        'orig_kb' => '0',
                                        'message' => '',
                                        'dump_timestamp' => '20080313133333',
                                        'hostname' => 'otherbox',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'orig_kb' => '0',
                                                       'kb' => 190,
                                                       'native_crc' => '00000000:0',
                                                       'label' => 'Conf-003',
                                                       'server_crc' => '00000000:0',
                                                       'sec' => '0.001733',
                                                       'partnum' => 1,
                                                       'status' => 'OK',
                                                       'client_crc' => '00000000:0',
                                                       'filenum' => 14
                                                     }
                                                   ],
                                        'level' => '0',
                                        'sec' => '0.001733',
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
                                        'status' => 'OK'
                                      },
                                      {
                                        'orig_kb' => '0',
                                        'message' => '',
                                        'dump_timestamp' => '20080414144444',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => '1',
                                                       'server_crc' => '00000000:0',
                                                       'sec' => '0',
                                                       'holding_file' => "$Installcheck::TMP/holding/20080414144444/otherbox._lib",
                                                       'client_crc' => '00000000:0',
                                                       'status' => 'OK',
                                                       'orig_kb' => '0',
                                                       'kb' => 256,
                                                       'native_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'hostname' => 'otherbox',
                                        'level' => 1,
                                        'sec' => '0',
                                        'storage' => 'HOLDING',
                                        'pool' => 'HOLDING',
                                        'client_crc' => '00000000:0',
                                        'diskname' => '/lib',
                                        'kb' => 256,
                                        'native_crc' => '00000000:0',
                                        'bytes' => '0',
                                        'write_timestamp' => '00000000000000',
                                        'server_crc' => '00000000:0',
                                        'nparts' => '1',
                                        'status' => 'OK'
                                      },
                                      {
                                        'native_crc' => '00000000:0',
                                        'kb' => '32',
                                        'write_timestamp' => '20080414144444',
                                        'bytes' => '0',
                                        'server_crc' => '00000000:0',
                                        'nparts' => '0',
                                        'status' => 'PARTIAL',
                                        'dump_timestamp' => '20080414144444',
                                        'message' => 'full-up',
                                        'orig_kb' => '0',
                                        'parts' => [
                                                     {},
                                                     {
                                                       'partnum' => 1,
                                                       'server_crc' => '00000000:0',
                                                       'sec' => '0.00054',
                                                       'client_crc' => '00000000:0',
                                                       'status' => 'PARTIAL',
                                                       'filenum' => 1,
                                                       'orig_kb' => '0',
                                                       'label' => 'Conf-005',
                                                       'kb' => 32,
                                                       'native_crc' => '00000000:0'
                                                     }
                                                   ],
                                        'hostname' => 'otherbox',
                                        'level' => 1,
                                        'sec' => '0.00054',
                                        'storage' => 'TESTCONF',
                                        'pool' => 'TESTCONF',
                                        'diskname' => '/lib',
                                        'client_crc' => '00000000:0'
                                      },
                                      {
                                        'sec' => '0.001733',
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
                                                       'orig_kb' => 190,
                                                       'native_crc' => '00000000:0',
                                                       'kb' => 190,
                                                       'label' => 'Conf-006',
                                                       'sec' => '0.001733',
                                                       'server_crc' => '00000000:0',
                                                       'partnum' => 1,
                                                       'filenum' => 13,
                                                       'client_crc' => '00000000:0',
                                                       'status' => 'OK'
                                                     }
                                                   ],
                                        'level' => '0',
                                        'server_crc' => '00000000:0',
                                        'nparts' => '1',
                                        'status' => 'OK',
                                        'kb' => '190',
                                        'native_crc' => '00000000:0',
                                        'write_timestamp' => '20080515155555',
                                        'bytes' => '0'
                                      }
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
    "One host, one disk");

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dumps/hosts/nohost");
is_deeply (sort_reply(Installcheck::Rest::remove_source_line($reply)),
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
