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

use Test::More tests => 1;
use File::Path;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Rest;
use Amanda::Paths;
use Amanda::Device qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $rest = Installcheck::Rest->new();
my $reply;

my $amperldir = $Amanda::Paths::amperldir;

#CODE 1550000
$reply = $rest->get("http://localhost:5000/amanda/v1.0");
is_deeply ($reply,
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Version.pm",
		'source_line' => '63',
		'BUILT_DATE' => $Amanda::Constants::BUILT_DATE,
		'BUILT_REV' => $Amanda::Constants::BUILT_REV,
		'BUILT_BRANCH' => $Amanda::Constants::BUILT_BRANCH,
		'VERSION' => $Amanda::Constants::VERSION,
		'severity' => '16',
		'message' => 'The version',
		'code' => '1550000'
	  },
        ],
      http_code => 200,
    },
    "get version");

$rest->stop();
