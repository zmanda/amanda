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
plan tests => 1;

my $reply;

my $amperldir = $Amanda::Paths::amperldir;
my $testconf;

$testconf = Installcheck::Run::setup();
$testconf->add_dumptype("mytype", [
    "compress" => "server",
    "starttime" => "1830",
    "amandad_path" => "\"/path/to/amandad\"",
]);
$testconf->add_dle("localhost /home mytype");
$testconf->add_dle(<<EOF);
localhost /home-incronly {
    mytype
    strategy incronly
}
localhost /etc {
    mytype
}
EOF

$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
my $infodir = getconf($CNF_INFOFILE);

#CODE 1500001
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/amcheck","");
$reply->{'body'}[2]->{'message'} = "Holding disk : disk space available, using as requested";
$reply->{'body'}[14]->{'message'} = "Server check took 1.00 seconds";
$reply->{'body'}[18]->{'message'} = "Client check: 1 host checked in 1.00 seconds.  1 problem found.";
$reply->{'body'}[19]->{'message'} = "(brought to you by Amanda x.y.z)";
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "Amanda Tape Server Host Check",
		'code' => '2800027'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "-----------------------------",
		'code' => '2800028'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "Holding disk : disk space available, using as requested",
		'code' => '2800073'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "slot 1: contains an empty volume",
		'code' => '123'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "slot 2: contains an empty volume",
		'code' => '123'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "slot 3: contains an empty volume",
		'code' => '123'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => ' volume \'\'',
		'code' => '123'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'Taper scan algorithm did not find an acceptable volume.',
		'code' => '123'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => '    (expecting a new volume)',
		'code' => '123'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'ERROR: No acceptable volumes found',
		'code' => '123'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'NOTE: host info dir /amanda/h1/etc/amanda/TESTCONF/curinfo/localhost does not exist',
		'code' => '2800100'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'NOTE: it will be created on the next run.',
		'code' => '2800101'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'NOTE: index dir /amanda/h1/etc/amanda/TESTCONF/index/localhost does not exist',
		'code' => '2800126'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'NOTE: it will be created on the next run.',
		'code' => '2800127'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'Server check took 1.00 seconds',
		'code' => '2800160'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'Amanda Backup Client Hosts Check',
		'code' => '2800202'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => '--------------------------------',
		'code' => '2800203'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'ERROR: localhost: Could not access /home-incronly (/home-incronly): No such file or directory',
		'code' => '2800211'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'Client check: 1 host checked in 1.00 seconds.  1 problem found.',
		'code' => '2800204'
	  },
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => '(brought to you by Amanda x.y.z)',
		'code' => '2800016'
	  },
	  {}
        ],
      http_code => 200,
    },
    "No config") || diag(Data::Dumper::Dumper($reply));
$rest->stop();
exit;

#CODE 1400007
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Dles.pm",
		'severity' => '16',
		'message' => 'Required \'disk\' argument is not provided.',
		'code' => '1400009'
	  },
        ],
      http_code => 404,
    },
    "host exists in disklist");


#CODE 140009
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/Localhost?disk=/home","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Dles.pm",
		'diskfile' => $diskfile,
		'host' => 'Localhost',
		'severity' => '16',
		'message' => 'No such host \'Localhost\' in disklist.',
		'code' => '1400007'
	  },
        ],
      http_code => 200,
    },
    "No such host in disklist");

#CODE 140009
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Dles.pm",
		'severity' => '16',
		'message' => 'No command specified: force, force_level_1, force_bump, force_no_bump.',
		'code' => '1300030'
	  },
        ],
      http_code => 200,
    },
    "No command");

#CODE 1300003
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to a forced level 0 at next run.',
		'code' => '1300003'
	  },
        ],
      http_code => 200,
    },
    "first force=1");

#CODE 1300003
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to a forced level 0 at next run.',
		'code' => '1300003'
	  },
        ],
      http_code => 200,
    },
    "second force=1");

#CODE 1300019
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'force command for localhost:/home cleared.',
		'code' => '1300019'
	  },
        ],
      http_code => 200,
    },
    "first force=0");

#CODE 1300021
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'no force command outstanding for localhost:/home, unchanged.',
		'code' => '1300021'
	  },
        ],
      http_code => 200,
    },
    "second force=0");


#CODE 1300031
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force=1&force_level_1=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Dles.pm",
		'severity' => '16',
		'message' => 'Only one command allowed.',
		'code' => '1300031'
	  },
        ],
      http_code => 200,
    },
    "force=1&force_level_1=1");

#CODE 1300023
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_level_1=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to a forced level 1 at next run.',
		'code' => '1300023'
	  },
        ],
      http_code => 200,
    },
    "first force_level_1=1");

#CODE 1300023
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_level_1=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to a forced level 1 at next run.',
		'code' => '1300023'
	  },
        ],
      http_code => 200,
    },
    "second force_level_1=1");

#CODE 1300020
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_level_1=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'force-level-1 command for localhost:/home cleared.',
		'code' => '1300020'
	  },
        ],
      http_code => 200,
    },
    "first force_level_1=0");

#CODE 1300021
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_level_1=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'no force command outstanding for localhost:/home, unchanged.',
		'code' => '1300021'
	  },
        ],
      http_code => 200,
    },
    "second force_level_1=0");

#CODE 1300023
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_level_1=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to a forced level 1 at next run.',
		'code' => '1300023'
	  },
        ],
      http_code => 200,
    },
    "third force_level_1=1");

#CODE 1300000 and 1300003
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home FORCE-LEVEL-1 command was cleared',
		'code' => '1300000'
	  },
          {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to a forced level 0 at next run.',
		'code' => '1300003'
	  },
        ],
      http_code => 200,
    },
    "first force=1");

#CODE 1300022 and 1300025
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_bump=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home FORCE command was cleared',
		'code' => '1300022'
	  },
          {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to bump at next run.',
		'code' => '1300025'
	  },
        ],
      http_code => 200,
    },
    "first force_bump=1") || diag("reply: " .Data::Dumper::Dumper($reply));

#CODE 1300025
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_bump=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to bump at next run.',
		'code' => '1300025'
	  },
        ],
      http_code => 200,
    },
    "second force_bump=1");

#CODE 1300027
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_bump=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'bump command for localhost:/home cleared.',
		'code' => '1300027'
	  },
        ],
      http_code => 200,
    },
    "first force_bump=0");

#CODE 1300028
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_bump=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'no bump command outstanding for localhost:/home, unchanged.',
		'code' => '1300028'
	  },
        ],
      http_code => 200,
    },
    "second force_bump=0");

#CODE 1300025
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_bump=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to bump at next run.',
		'code' => '1300025'
	  },
        ],
      http_code => 200,
    },
    "third force_bump=1");

#CODE 1300001 and 1300026
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_no_bump=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home FORCE-BUMP command was cleared',
		'code' => '1300001'
	  },
          {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to not bump at next run.',
		'code' => '1300026'
	  },
        ],
      http_code => 200,
    },
    "first force_no_bump=1");

#CODE 1300026
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_no_bump=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to not bump at next run.',
		'code' => '1300026'
	  },
        ],
      http_code => 200,
    },
    "second force_no_bump=1");

#CODE 1300026
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_no_bump=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'bump command for localhost:/home cleared.',
		'code' => '1300027'
	  },
        ],
      http_code => 200,
    },
    "first force_no_bump=0");

#CODE 1300028
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_no_bump=0","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'no bump command outstanding for localhost:/home, unchanged.',
		'code' => '1300028'
	  },
        ],
      http_code => 200,
    },
    "second force_no_bump=0");

#CODE 1300002
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home-incronly&force=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home-incronly',
		'severity' => '16',
		'message' => 'localhost:/home-incronly full dump done offline, next dump will be at level 1.',
		'code' => '1300002'
	  },
        ],
      http_code => 200,
    },
    "first force=1 for /home-incronly");

#CODE 1300026
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_no_bump=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to not bump at next run.',
		'code' => '1300026'
	  },
        ],
      http_code => 200,
    },
    "third force_no_bump=1");

#CODE 1300024 and 1300025
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/dles/hosts/localhost?disk=/home&force_bump=1","");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home FORCE-NO-BUMP command was cleared.',
		'code' => '1300024'
	  },
          {	'source_filename' => "$amperldir/Amanda/Curinfo.pm",
		'host' => 'localhost',
		'disk' => '/home',
		'severity' => '16',
		'message' => 'localhost:/home is set to bump at next run.',
		'code' => '1300025'
	  },
        ],
      http_code => 200,
    },
    "third force_bump=1");

#diag("reply: " . Data::Dumper::Dumper($reply));

$rest->stop();
