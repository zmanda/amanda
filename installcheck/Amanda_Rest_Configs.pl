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
plan tests => 19;

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
		'cfgerror' => "'$Amanda::Paths::CONFIG_DIR/TESTCONF/amanda.conf', line 10: warning: Keyword AMRECOVER_DO_FSF is deprecated.",
		'severity' => $Amanda::Message::WARNING,
		'message' => "config warning: '$Amanda::Paths::CONFIG_DIR/TESTCONF/amanda.conf', line 10: warning: Keyword AMRECOVER_DO_FSF is deprecated.",
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
		'severity' => $Amanda::Message::ERROR,
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
		'severity' => $Amanda::Message::SUCCESS,
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
		'severity' => $Amanda::Message::SUCCESS,
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
		'severity' => $Amanda::Message::ERROR,
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
		'severity' => $Amanda::Message::ERROR,
		'errno'    => 13,
		'errnostr' => 'Permission denied',
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

config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
#CODE 1500007 and 1500008
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF?fields=foobar&fields=tapecycle");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => $Amanda::Message::ERROR,
		'field' => 'FOOBAR',
		'message' => 'invalid \'FOOBAR\' field specified',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500058'
	  },
          {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'result' => {
			'TAPECYCLE' => 3 },
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
    "Get invalid fields (foobar,tapecycle)") || diag("reply: " . Data::Dumper::Dumper($reply));

#CODE 1500008
$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF?fields=runtapes&fields=tapecycle");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'result' => {
			'TAPECYCLE' => 3,
			'RUNTAPES' => 1},
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
    "Get valid fields (runtapes,tapecycle)") || diag("reply: " . Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {	'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
		'severity' => $Amanda::Message::SUCCESS,
		'result' => {
			'DEBUG-CHUNKER' => 0,
			'BUMPMULT' => '1.5',
			'DEBUG-PROTOCOL' => 0,
			'KRB5KEYTAB' => '/.amanda-v5-keytab',
			'COLUMNSPEC' => '',
			'AMRECOVER-DO-FSF' => 'YES',
			'LABEL-NEW-TAPES' => '',
			'HOLDINGDISK' => [
				'hd1'
			],
			'REST-API-PORT' => 0,
			'MAXDUMPSIZE' => -1,
			'DEBUG-TAPER' => 0,
			'REPORT-FORMAT' => [],
			'TAPETYPE' => 'TEST-TAPE',
			'DEVICE-PROPERTY' => {},
			'VAULT-STORAGE' => [],
			'RESERVED-TCP-PORT' => [
				'512',
				'1023'
			],
			'TAPECYCLE' => 3,
			'DEBUG-RECOVERY' => 1,
			'ETIMEOUT' => 300,
			'DEBUG-SENDBACKUP' => 0,
			'REPORT-USE-MEDIA' => 'YES',
			'DEBUG-AMINDEXD' => 0,
			'MAILER' => getconf($CNF_MAILER),
			'INTERACTIVITY' => undef,
			'AMRECOVER-CHECK-LABEL' => 'YES',
			'UNRESERVED-TCP-PORT' => [
				'1024',
				'65535'
			],
			'DEBUG-SENDSIZE' => 0,
			'MAX-DLE-BY-VOLUME' => 1000000000,
			'RUNTAPES' => 1,
			'DEBUG-DRIVER' => 0,
			'INPARALLEL' => 2,
			'BUMPSIZE' => 10240,
			'DEBUG-AMIDXTAPED' => 0,
			'FLUSH-THRESHOLD-SCHEDULED' => 0,
			'TAPER-PARALLEL-WRITE' => 1,
			'NETUSAGE' => 80000,
			'ORG' => 'DailySet1',
			'CHANGERDEV' => undef,
			'FLUSH-THRESHOLD-DUMPED' => 0,
			'RESERVED-UDP-PORT' => [
				'512',
				'1023'
			],
			'REPORT-NEXT-MEDIA' => 'YES',
			'DUMPCYCLE' => 10,
			'DEVICE-OUTPUT-BUFFER-SIZE' => 1310720,
			'DEBUG-AUTH' => 0,
			'TAPERSCAN' => undef,
			'AUTOLABEL' => {
				'empty' => 'NO',
				'other_config' => 'NO',
				'non_amanda' => 'NO',
				'template' => undef,
				'volume_error' => 'NO'
			},
			'STORAGE' => [
				'TESTCONF'
			],
			'RECOVERY-LIMIT' => [],
			'TAPEDEV' => getconf($CNF_TAPEDEV),
			'TAPELIST' => 'tapelist',
			'META-AUTOLABEL' => undef,
			'MAILTO' => '',
			'COMPRESS-INDEX' => 'YES',
			'USETIMESTAMPS' => 'YES',
			'DEBUG-PLANNER' => 0,
			'COMMAND-FILE' => 'command_file',
			'DEBUG-AMRECOVER' => 0,
			'DTIMEOUT' => 1800,
			'PRINTER' => '',
			'EJECT-VOLUME' => 'NO',
			'DUMPORDER' => 'ttt',
			'LABELSTR' => {
				'template' => 'TESTCONF[0-9][0-9]',
				'match_autolabel' => 'NO'
			},
			'RESERVE' => 100,
			'DEBUG-EVENT' => 0,
			'TAPERALGO' => 'FIRST',
			'DISPLAYUNIT' => 'K',
			'PROPERTY' => {},
			'REP-TRIES' => 5,
			'BUMPPERCENT' => 0,
			'DEBUG-HOLDING' => 0,
			'DEBUG-DUMPER' => 0,
			'CHANGERFILE' => 'changer',
			'DEBUG-SELFCHECK' => 0,
			'MAXDUMPS' => 1,
			'AUTOFLUSH' => 'NO',
			'CONNECT-TRIES' => 3,
			'RUNSPERCYCLE' => 0,
			'KRB5PRINCIPAL' => 'service/amanda',
			'BUMPDAYS' => 2,
			'SEND-AMREPORT-ON' => 'ALL',
			'DISKFILE' => 'disklist',
			'TAPERFLUSH' => 0,
			'SORT-INDEX' => 'NO',
			'REST-SSL-KEY' => undef,
			'REST-SSL-CERT' => undef,
			'CTIMEOUT' => 30,
			'REQ-TRIES' => 3,
			'AMRECOVER-CHANGER' => '',
			'DEBUG-AMANDAD' => 0,
			'TPCHANGER' => getconf($CNF_TPCHANGER),
			'SSL-DIR' => getconf($CNF_SSL_DIR),
			'INFOFILE' => getconf($CNF_INFOFILE),
			'TMPDIR' => getconf($CNF_TMPDIR),
			'INDEXDIR' => getconf($CNF_INDEXDIR),
			'LOGDIR' => getconf($CNF_LOGDIR),
			'DUMPUSER' => getconf($CNF_DUMPUSER),
                },

		'message' => 'Parameters values',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
		'code' => '1500008'
	  },
        ],
      http_code => 200,
    },
    "Get all fields") || diag("reply: " . Data::Dumper::Dumper($reply));

# set up and load a simple config
$testconf = Installcheck::Run::setup();
$testconf->add_changer("DISKFLAT", [
    tpchanger => '"chg-disk:/amanda/h1/vtapes"',
    changerfile => '"/tmp/changerfile"'
]);
$testconf->write();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => "Changer list",
		'changers_list' => ['DISKFLAT'],
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500026'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/TEST");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'No \'TEST\' changer',
		'changer' => 'TEST',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500051'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/TEST?fields");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'No \'TEST\' changer',
		'changer' => 'TEST',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500051'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/DISKFLAT?fields");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Changers \'DISKFLAT\' parameters values',
		'changer' => 'DISKFLAT',
		'result'  => {},
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500041'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/changers/DISKFLAT?fields=tpchanger&fields=changerfile&fields=pool");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
       [  {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Changers \'DISKFLAT\' parameters values',
		'changer' => 'DISKFLAT',
		'result' => { 'TPCHANGER' => 'chg-disk:/amanda/h1/vtapes',
			      'CHANGERFILE' => '/tmp/changerfile'
			    },
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500041'
          },
        ],
      http_code => 200,
    },
    "List changer") || diag("reply: " .Data::Dumper::Dumper($reply));

my $taperoot = "$Installcheck::TMP/Amanda_Changer_Diskflat_test";

# set up and load a simple config
 $testconf = Installcheck::Run::setup();
$testconf->write();

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => "Storage list",
		'storages_list' => ['TESTCONF'],
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500027'
          },
        ],
      http_code => 200,
    },
    "List storage") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/TEST");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'No \'TEST\' storage',
		'storage' => 'TEST',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500048'
          },
        ],
      http_code => 200,
    },
    "List storage") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/TEST?fields");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::ERROR,
                'message' => 'No \'TEST\' storage',
		'storage' => 'TEST',
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500048'
          },
        ],
      http_code => 200,
    },
    "List storage") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/TESTCONF?fields");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Storage \'TESTCONF\' parameters values',
		'storage' => 'TESTCONF',
		'result'  => {},
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500036'
          },
        ],
      http_code => 200,
    },
    "List storage") || diag("reply: " .Data::Dumper::Dumper($reply));

$reply = $rest->get("http://localhost:5001/amanda/v1.0/configs/TESTCONF/storages/TESTCONF?fields=tpchanger&fields=runtapes&fields=pool&fields=tapepool");
is_deeply (Installcheck::Rest::remove_source_line($reply),
    { body =>
        [ {     'source_filename' => "$amperldir/Amanda/Rest/Configs.pm",
                'severity' => $Amanda::Message::SUCCESS,
                'message' => 'Storage \'TESTCONF\' parameters values',
		'storage' => 'TESTCONF',
		'result' => { 'TAPEPOOL' => 'TESTCONF',
			      'RUNTAPES' => 1,
			      'TPCHANGER' => "chg-disk:$Installcheck::TMP/vtapes"
			    },
		'process' => 'Amanda::Rest::Configs',
		'running_on' => 'amanda-server',
		'component' => 'rest-server',
		'module' => 'amanda',
                'code' => '1500036'
          },
        ],
      http_code => 200,
    },
    "List storage") || diag("reply: " .Data::Dumper::Dumper($reply));

$rest->stop();

rmtree $taperoot;


$rest->stop();
