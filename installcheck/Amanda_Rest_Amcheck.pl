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
$testconf->add_dle("localhost /home installcheck-test");
$testconf->add_dle(<<EOF);
localhost /home-incronly {
    installcheck-test
}
localhost /etc {
    installcheck-test
}
EOF

$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $diskfile = Amanda::Config::config_dir_relative(getconf($CNF_DISKFILE));
my $infodir = getconf($CNF_INFOFILE);

#CODE 28* 123
$reply = $rest->post("http://localhost:5001/amanda/v1.0/configs/TESTCONF/amcheck","");
foreach my $message (@{$reply->{'body'}}) {
    if (defined $message and defined $message->{'message'}) {
	$message->{'message'} =~ s/^NOTE: host info dir .*$/NOTE: host info dir/;
	$message->{'message'} =~ s/^NOTE: index dir .*$/NOTE: index dir/;
	$message->{'message'} =~ s/^Holding disk .*$/Holding disk : disk space available, using as requested/;
	$message->{'message'} =~ s/^Server check took .*$/Server check took 1.00 seconds/;
	$message->{'message'} =~ s/^Client check: 1 host checked in \d+.\d+ seconds.  1 problem found.$/Client check: 1 host checked in 1.00 seconds.  1 problem found./;
	$message->{'message'} =~ s/^\(brought to you by Amanda .*$/(brought to you by Amanda x.y.z)/;
    }
    if ($message->{'code'} == 2800073) {
	$message->{'avail'} = '9999';
	$message->{'requested'} = '9999';
    } elsif ($message->{'code'} == 2800160) {
	$message->{'seconds'} = '1.00';
    } elsif ($message->{'code'} == 2800204) {
	$message->{'seconds'} = '1.00';
    }
}
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
#0
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Amcheck.pm",
		'severity' => '16',
		'exit_code' => '1',
		'process' => 'amrest-server',
		'running_on' => 'amanda-server',
		'message' => 'Amcheck exit code is \'1\'',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '2850000'
	  },
#1
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'message' => "Amanda Tape Server Host Check",
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800027'
	  },
#2
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => "Holding disk : disk space available, using as requested",
		'avail' => '9999',
		'requested' => '9999',
		'holding_dir' => "$Installcheck::TMP/holding",
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800073'
	  },
#3
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "slot 1: contains an empty volume",
		'errstr' => "slot 1: contains an empty volume",
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '123'
	  },
#4
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "slot 2: contains an empty volume",
		'errstr' => "slot 2: contains an empty volume",
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '123'
	  },
#5
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => "slot 3: contains an empty volume",
		'errstr' => "slot 3: contains an empty volume",
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '123'
	  },
#6
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => ' volume \'\'',
		'errstr' => ' volume \'\'',
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '123'
	  },
#7
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'Taper scan algorithm did not find an acceptable volume.',
		'errstr' => 'Taper scan algorithm did not find an acceptable volume.',
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '123'
	  },
#8
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => '    (expecting a new volume)',
		'errstr' => '    (expecting a new volume)',
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '123'
	  },
#9
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => 'ERROR: No acceptable volumes found',
		'errstr' => 'ERROR: No acceptable volumes found',
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '123'
	  },
#10
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => 'NOTE: host info dir',
		'hint'    => '      It will be created on the next run',
		'hostinfodir' => "$Amanda::Paths::CONFIG_DIR/TESTCONF/curinfo/localhost",
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800100'
	  },
#11
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => 'NOTE: index dir',
		'hint'    => '      it will be created on the next run',
		'hostindexdir' => "$Amanda::Paths::CONFIG_DIR/TESTCONF/index/localhost",
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800126'
	  },
#12
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => 'Server check took 1.00 seconds',
		'seconds' => '1.00',
		'process' => 'amcheck-server',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800160'
	  },
#13
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => 'Amanda Backup Client Hosts Check',
		'process' => 'amcheck-clients',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800202'
	  },
#14
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => '--------------------------------',
		'process' => 'amcheck-clients',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800203'
	  },
#15
          {	'source_filename' => "amcheck.c",
		'severity' => '16',
		'message' => Amanda::Util::built_with_component("client")
                            ? 'ERROR: localhost: Could not access /home-incronly (/home-incronly): No such file or directory'
                            : "ERROR: NAK localhost: execute access to '$Amanda::Paths::amlibexecdir/amanda/noop' denied",
		'errstr' => Amanda::Util::built_with_component("client")
                            ? 'Could not access /home-incronly (/home-incronly): No such file or directory'
                            : "NAK localhost: execute access to '$Amanda::Paths::amlibexecdir/amanda/noop' denied",
		'hostname' => 'localhost',
		'type' => '',
		'process' => 'amcheck-clients',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800211'
	  },
#16
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => 'Client check: 1 host checked in 1.00 seconds.  1 problem found.',
		'hostcount' => 1,
		'remote_errors' => 1,
		'seconds' => '1.00',
		'process' => 'amcheck-clients',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800204'
	  },
#17
          {	'source_filename' => "amcheck.c",
		'severity' => '2',
		'message' => '(brought to you by Amanda x.y.z)',
		'version' => $Amanda::Constants::VERSION,
		'process' => 'amcheck',
		'running_on' => 'amanda-server',
		'component' => 'amanda',
		'module' => 'amanda',
		'code' => '2800016'
	  },
        ],
      http_code => 200,
    },
    "No config") || diag("reply: " . Data::Dumper::Dumper($reply));

$rest->stop();
