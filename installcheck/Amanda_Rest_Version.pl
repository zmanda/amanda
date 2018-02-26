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
plan tests => 1;
my $reply;

my $amperldir = $Amanda::Paths::amperldir;

#CODE 1550000
$reply = $rest->get("http://localhost:5001/amanda/v1.0");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Version.pm",
		'BUILT_DATE' => $Amanda::Constants::BUILT_DATE,
		'BUILT_REV' => $Amanda::Constants::BUILT_REV,
		'BUILT_BRANCH' => $Amanda::Constants::BUILT_BRANCH,
		'VERSION' => $Amanda::Constants::VERSION,
		'severity' => $Amanda::Message::SUCCESS,
		'message' => 'The version',
		'process' => 'Amanda::Rest::Version',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1550000'
	  },
        ],
      http_code => 200,
    },
    "get version");

$rest->stop();
