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

if (!Amanda::Util::built_with_component("client")) {
    plan skip_all => "Not build with client";
    exit 1;
}

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
plan tests => 6;

my $reply;

my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

$testconf = Installcheck::Run::setup();
$testconf->write( do_catalog => 0 );

#CODE 3700012
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Services.pm",
		'severity' => $Amanda::Message::ERROR,
		'message' => "No application argument specified",
		'process' => 'Amanda::Rest::Services',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '3100004'
	  },
        ],
      http_code => 200,
    },
    "no application") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 3100000
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?auth=local&application=amgtar");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Services.pm",
		'severity' => $Amanda::Message::ERROR,
		'message' => "No host argument specified",
		'process' => 'Amanda::Rest::Services',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '3100000'
	  },
        ],
      http_code => 200,
    },
    "no host") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 3100001
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&application=amgtar");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Services.pm",
		'severity' => $Amanda::Message::ERROR,
		'message' => "No auth argument specified",
		'process' => 'Amanda::Rest::Services',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '3100001'
	  },
        ],
      http_code => 200,
    },
    "no auth") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 3100002
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=bad_auth&application=amgtar");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Services.pm",
		'severity' => $Amanda::Message::ERROR,
		'message' => 'amservice failed: Could not find security driver "bad_auth".',
		'errmsg' => 'Could not find security driver "bad_auth".',
		'process' => 'Amanda::Rest::Services',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '3100002'
	  },
        ],
      http_code => 200,
    },
    "bad auth") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 2900000
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local&application=bad_app");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => 'senddiscover.c',
		'severity' => $Amanda::Message::ERROR,
		'message' => "The Application 'bad_app' failed: senddiscover: error [exec $APPLICATION_DIR/bad_app: No such file or directory]",
		'errmsg' => "senddiscover: error [exec $APPLICATION_DIR/bad_app: No such file or directory]",
		'service' => 'senddiscover',
		'application' => 'bad_app',
		'process' => 'senddiscover',
		'running_on' => 'amanda-client',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2900000'
	  },
	  {
		'source_filename' => 'senddiscover.c',
		'severity' => 'error',
		'message' => 'The Application \'bad_app\' failed: exited with status 1',
		'errmsg' => 'exited with status 1',
		'service' => 'senddiscover',
		'application' => 'bad_app',
		'process' => 'senddiscover',
		'running_on' => 'amanda-client',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2900000'
	  }
        ],
      http_code => 200,
    },
    "bad_app") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 2900002
$reply = $rest->get("http://localhost:5001/amanda/v1.0/services/discover?host=localhost&auth=local&application=amraw");
is_deeply (Installcheck::Config::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => 'senddiscover.c',
		'severity' => $Amanda::Message::ERROR,
		'message' => "The application 'amraw' does not support the 'discover' method",
		'service' => 'senddiscover',
		'application' => 'amraw',
		'method' => 'discover',
		'process' => 'senddiscover',
		'running_on' => 'amanda-client',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2900002'
	  },
        ],
      http_code => 200,
    },
    "amraw") || diag("reply: " . Data::Dumper::Dumper($reply));

$rest->stop();
