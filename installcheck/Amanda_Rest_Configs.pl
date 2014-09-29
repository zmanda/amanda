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
plan tests => 9;

my $reply;

my $amperldir = $Amanda::Paths::amperldir;
my $config_dir = $Amanda::Paths::CONFIG_DIR;
my $testconf;

#CODE 1500000
$testconf = Installcheck::Run::setup();
$testconf->add_param('AMRECOVER_DO_FSF', 'YES');
$testconf->write();
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF?fields=amrecover_do_fsf");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'cfgerror' => "'$Amanda::Paths::CONFIG_DIR/TESTCONF/amanda.conf', line 9: warning: Keyword AMRECOVER_DO_FSF is deprecated.",
		'severity' => '16',
		'message' => "config warning: '$Amanda::Paths::CONFIG_DIR/TESTCONF/amanda.conf', line 9: warning: Keyword AMRECOVER_DO_FSF is deprecated.",
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500000'
	  },
        ],
      http_code => 200,
    },
    "Get no fields");

#CODE 1500001
$testconf = Installcheck::Run::setup();
$testconf->write();
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/FOOBAR?fields=runtapes");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'cfgerror' => "parse error: could not open conf file '$Amanda::Paths::CONFIG_DIR/FOOBAR/amanda.conf': No such file or directory",
		'severity' => '16',
		'message' => "config error: parse error: could not open conf file '$CONFIG_DIR/FOOBAR/amanda.conf': No such file or directory",
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500001'
	  },
        ],
      http_code => 200,
    },
    "Get runtapes");

#CODE 1500003
$testconf = Installcheck::Run::setup();
$testconf->write();
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs");

my @conf = <$config_dir/*/amanda.conf>;
ok (@conf > 0, "More than one config");
#get the list of config directories
my @newconf;
foreach my $conf (@conf) {
    $conf =~ s/\/amanda.conf//g;
    $conf =~ s/.*\///g;
    push @newconf, $conf;
}
@newconf = sort @newconf;

#Sort the config in the reply
if (defined $reply->{'body'}[0]->{'config'}) {
    @{$reply->{'body'}[0]->{'config'}} = sort @{$reply->{'body'}[0]->{'config'}};
}

is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'config' => [@newconf],
		'severity' => '16',
		'message' => 'config name',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500003'
          }
        ],
      http_code => 200,
    },
    "Get config list");

# CODE 1500003 or 1500004
$testconf->cleanup();
# Get the list of configs (can be zero config)
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs");
@conf = <$config_dir/*/amanda.conf>;
if (@conf > 0) {
    #get the list of config directories
    my @newconf;
    foreach my $conf (@conf) {
	$conf =~ s/\/amanda.conf//g;
	$conf =~ s/.*\///g;
	push @newconf, $conf;
    }
    @newconf = sort @newconf;

    #Sort the config in the reply
    if (defined $reply->{'body'}[0]->{'config'}) {
	@{$reply->{'body'}[0]->{'config'}} = sort @{$reply->{'body'}[0]->{'config'}};
    }

    is_deeply (Installcheck::Rest::remove_source_line($reply),
        { body =>
            [ { 'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'config' => [@newconf],
		'severity' => '16',
		'message' => 'config name',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500003'
              }
            ],
          http_code => 200,
        },
        "Get config list");
} else {
    is_deeply (Installcheck::Rest::remove_source_line($reply),
        { body =>
            [ { 'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => '16',
		'message' => 'no config',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500004'
              }
            ],
          http_code => 404,
        },
        "Get config list");
}

#CODE 1500006
$testconf = Installcheck::Run::setup();
$testconf->write();
chmod 0000, $config_dir;
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs");

is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => '16',
		'errno'    => 'Permission denied',
		'message' => "Can't open config directory '$Amanda::Paths::CONFIG_DIR': Permission denied",
		'dir' => $Amanda::Paths::CONFIG_DIR,
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500006'
          }
        ],
      http_code => 404,
    },
    "Get config list error (Permission denied)");
chmod 0700, $config_dir;


#CODE 1500007 and 1500008
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF?fields=foobar,tapecycle");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => '16',
		'parameters' => [ 'foobar' ],
		'message' => 'Not existant parameters',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500007'
	  },
          {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => '16',
		'result' => {
			'tapecycle' => 3 },
		'message' => 'Parameters values',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500008'
          }
        ],
      http_code => 200,
    },
    "Get invalid fields (foobar,tapecycle)");

#CODE 1500008
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF?fields=runtapes,tapecycle");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => '16',
		'result' => {
			'tapecycle' => 3,
			'runtapes' => 1},
		'message' => 'Parameters values',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500008'
          }
        ],
      http_code => 200,
    },
    "Get valid fields (runtapes,tapecycle)");

#CODE 1500009
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => '16',
		'message' => 'No fields specified',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500009'
	  },
        ],
      http_code => 200,
    },
    "Get no fields");

#CODE 1500002  # no way with Rest API
#CODE 1500005  # invalid error code
#CODE 1500010  # Checked in Amanda_Rest_Storages
#CODE 1500011  # Checked in Amanda_Rest_Storages
#CODE 1500012  # Checked in Amanda_Rest_Storages
#CODE 1500013  # Checked in Amanda_Rest_Storages
#CODE 1500014  # Checked in Amanda_Rest_Storages
#CODE 1500015  # Checked in Amanda_Rest_Storages and/or Amanda_Rest_Storages_Label

$rest->stop();
