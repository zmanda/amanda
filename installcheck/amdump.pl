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

use Test::More tests => 10;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
use Installcheck::DBCatalog2 qw( create_db_catalog2 check_db_catalog2 );
use Amanda::Config qw( :init );
use Amanda::Debug;
use Amanda::Paths;
use Amanda::Util;
use Amanda::DB::Catalog2;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;
my $log;

# Just run amdump.

$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
#$testconf->add_catalog('my_catalog', [
#  'comment' => '"SQLite catalog"',
#  'plugin'  => '"SQLite"',
#  'property' => "\"dbname\" \"$CONFIG_DIR/TESTCONF/SQLite.db\""
#]);
#$testconf->add_param('catalog','"my_catalog"');

# one program "GNUTAR"
$testconf->add_dle(<<EODLE);
localhost diskname1 $diskname {
    installcheck-test
    program "GNUTAR"
}
EODLE

# and one with the amgtar application
$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    property "a#s" "5%#8"
    application {
	plugin "amgtar"
	property "ATIME-PRESERVE" "NO"
    }
}
EODLE
$testconf->write( do_catalog => 0 );
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

ok(run('amdump', 'TESTCONF'), "amdump runs successfully")
    or amdump_diag();
check_db_catalog2('TESTCONF', 'amdump.catalog.1', $diskname, $diskname);

# Dump a nonexistant client, and see amdump fail.
$testconf = Installcheck::Run::setup();
$testconf->add_dle('does-not-exist.example.com / installcheck-test');
$testconf->write( do_catalog => 0 );
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

ok(!run('amdump', 'TESTCONF'), "amdump fails with nonexistent client");

#check failure in validate_optstr.
$testconf = Installcheck::Run::setup();
$testconf->add_dle(<<EODLE);
localhost diskname2 $diskname {
    installcheck-test
    program "APPLICATION"
    application {
        plugin "amgtar"
        property "ATIME-PRESERVE" "NO"
    }
    compress client custom
}
EODLE
$testconf->write( do_catalog => 0 );
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

ok(!run("$amlibexecdir/planner", 'TESTCONF', '--log-filename', "$CONFIG_DIR/TESTCONF/log/log"), "amdump fails in validate_optstr");
open(my $logfile, "<", "$CONFIG_DIR/TESTCONF/log/log")
	or die("opening log: $!");
my $logline = grep(/^\S+ planner localhost diskname2 \d* 0 \[client custom compression with no compression program specified\]/, <$logfile>);
ok($logline, "planner fail without 'client custom compression with no compression program specified'");


$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
$testconf->add_tapetype('tape-5m',
			[ length => '5000k', filemark => '0k' ]);
$testconf->add_param("tapetype", '"tape-5m"');
$testconf->add_param("device-property", '"ENFORCE_MAX_VOLUME_USAGE" "TRUE"');
$testconf->add_param("device-property", '"MAX_VOLUME_USAGE" "1m"');
$testconf->add_param("device-property", '"LEOM" "FALSE"');
$testconf->add_param("runtapes", '10');
$testconf->add_param("mailto", '"martinea"');

$testconf->add_dle(<<EODLE);
localhost diskname1 $diskname {
    installcheck-test
    holdingdisk never
    retry-dump 1
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "3145728"
    }
}
EODLE

$testconf->write( do_catalog => 0 );
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

ok(!run('amdump', 'TESTCONF'), "amdump fail on no more space")
    or amdump_diag();
$log = Amanda::Util::slurp("$CONFIG_DIR/TESTCONF/log/log");
ok($log =~ /No space left on device: more than MAX_VOLUME_USAGE bytes written, splitting not enabled/, "report No space left on device") or amdump_diag();


$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');
$testconf->add_tapetype('tape-5m',
			[ length => '5000k', filemark => '0k' ]);
$testconf->add_param("tapetype", '"tape-5m"');
$testconf->add_param("device-property", '"ENFORCE_MAX_VOLUME_USAGE" "TRUE"');
$testconf->add_param("device-property", '"MAX_VOLUME_USAGE" "1m"');
$testconf->add_param("device-property", '"LEOM" "FALSE"');
$testconf->add_param("runtapes", '10');
$testconf->add_param("mailto", '"martinea"');

$testconf->add_dle(<<EODLE);
localhost diskname1 $diskname {
    installcheck-test
    holdingdisk required
    retry-dump 1
    program "APPLICATION"
    application {
	plugin "amrandom"
	property "SIZE" "3145728"
    }
}
EODLE

$testconf->write( do_catalog => 0 );
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

ok(!run('amdump', 'TESTCONF'), "amdump succeed on flush no more space")
    or amdump_diag();
$log = Amanda::Util::slurp("$CONFIG_DIR/TESTCONF/log/log");
ok($log =~ /No space left on device: more than MAX_VOLUME_USAGE bytes written, splitting not enabled/, "report No space left on device") or amdump_diag();

Installcheck::Run::cleanup();
