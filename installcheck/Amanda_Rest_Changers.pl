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

my $taperoot = "$Installcheck::TMP/Amanda_Changer_Diskflat_test";

# set up and load a simple config
my $testconf = Installcheck::Run::setup();
$testconf->add_changer("DISKFLAT", [
    tpchanger => '"chg-disk:/amanda/h1/vtapes"',
    changerfile => '"/tmp/changerfile"'
]);
$testconf->write();

my $reply;
my $amperldir = $Amanda::Paths::amperldir;

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Changers.pm",
                'severity' => '16',
                'message' => "Defined changer",
		'changer' => ['DISKFLAT'],
                'code' => '1500021'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/TEST");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Changers.pm",
                'severity' => '16',
                'message' => 'Changer \'TEST\' not found',
		'changer' => 'TEST',
                'code' => '1500017'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/TEST?fields");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Changers.pm",
                'severity' => '16',
                'message' => 'Changer \'TEST\' not found',
		'changer' => 'TEST',
                'code' => '1500017'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/DISKFLAT?fields");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Changers.pm",
                'severity' => '16',
                'message' => 'No fields specified',
		'changer' => 'DISKFLAT',
                'code' => '1500009'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/DISKFLAT?fields=tpchanger,changerfile,pool");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Changers.pm",
                'severity' => '16',
                'message' => 'Not existant parameters in changer \'DISKFLAT\'',
		'changer' => 'DISKFLAT',
		'parameters' => [ 'pool' ],
                'code' => '1500018'
          },
          {     'source_filename' => "$amperldir/Amanda/Rest/Changers.pm",
                'severity' => '16',
                'message' => 'Parameters values for changer \'DISKFLAT\'',
		'changer' => 'DISKFLAT',
		'result' => { 'tpchanger' => 'chg-disk:/amanda/h1/vtapes',
			      'changerfile' => '/tmp/changerfile'
			    },
                'code' => '1500019'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));


$rest->stop();

1;
