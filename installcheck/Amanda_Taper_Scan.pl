# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 34;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Run;
use Installcheck::Config;
use Installcheck::Changer;
use Installcheck::DBCatalog2;
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Taper::Scan;
use Amanda::Storage;
use Amanda::DB::Catalog2;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $testconf_dir = "$CONFIG_DIR/TESTCONF";
my $tapelist_filename = "$testconf_dir/tapelist";
#my $DBCatalog2;
#my $tapelist_filename = "$Installcheck::TMP/tapelist";
#sub set_tapelist {
    #my ($content) = @_;
    #open(my $fh, ">", $tapelist_filename) or die("opening '$tapelist_filename': $!");
    #print $fh $content;
    #close($fh);

    #for my $line (split "\n", $content) {
	#my ($datestamp, $label, $reuse) = split ' ', $line;
	#$DBCatalog2->add_volume('TESTCONF', $label, $datestamp, 'TESTCONF');
    #}
#}

my $testconf = Installcheck::Run::setup();
$testconf->write( do_catalog => 0, tapelist=><<TAPELIST
20090424173004 TEST-4 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173003 TEST-3 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173002 TEST-2 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173001 TEST-1 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424172000 CONF-4 reuse POOL:CONFCONF
TAPELIST
);
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
my $catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);

# we use a "traditional" Amanda::Taper::Scan object, only because instantiating
# the parent class alone is difficult.  We never call scan(), so traditional's
# methods are never invoked in this test.
my $storage = Amanda::Storage->new(catalog => $catalog);
my $taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    catalog => $catalog,
    storage => $storage,
    );

$taperscan->quit();
$storage->quit();
$catalog->quit();

$testconf->remove_param('labelstr');
$testconf->add_param('labelstr', '"TEST-[0-9]"');
$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 2);
$testconf->write( do_catalog => 0, tapelist=><<TAPELIST
20090424173004 TEST-4 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173003 TEST-3 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173002 TEST-2 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173001 TEST-1 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424172000 CONF-4 reuse POOL:CONFCONF
TAPELIST
);
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173004, 0, "TESTCONF", "TESTCONF", "TEST-4", 1, 0, 0, 0);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173003, 0, "TESTCONF", "TESTCONF", "TEST-3", 1, 0, 0, 0);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173002, 0, "TESTCONF", "TESTCONF", "TEST-2", 1, 0, 0, 0);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173001, 0, "TESTCONF", "TESTCONF", "TEST-1", 1, 0, 0, 0);
my $temp_filename1 =  "$Amanda::Paths::AMANDA_TMPDIR/Amanda_Taper_Scan.$$.1_" . rand();
my $temp_filename2 =  "$Amanda::Paths::AMANDA_TMPDIR/Amanda_Taper_Scan.$$.2_" . rand();
system("$sbindir/amcatalog TESTCONF export $temp_filename1");
$catalog->compute_retention();
system("$sbindir/amcatalog TESTCONF export $temp_filename2");
$storage = Amanda::Storage->new(catalog => $catalog);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    catalog => $catalog,
    storage => $storage);

is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-1",
   "simple tapelist, tapecycle = 2: oldest_reusable_volume correct");

ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();
$catalog->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 3);
$testconf->write( do_catalog => 0, tapelist=><<TAPELIST
20090424173004 TEST-4 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173003 TEST-3 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173002 TEST-2 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173001 TEST-1 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424172000 CONF-4 reuse POOL:CONFCONF
TAPELIST
);
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173004, 0, "TESTCONF", "TESTCONF", "TEST-4", 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173003, 0, "TESTCONF", "TESTCONF", "TEST-3", 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173002, 0, "TESTCONF", "TESTCONF", "TEST-2", 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173001, 0, "TESTCONF", "TESTCONF", "TEST-1", 1);
$catalog->compute_retention();
$storage = Amanda::Storage->new(catalog => $catalog);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    catalog => $catalog,
    storage => $storage);

is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-1",
   "simple tapelist, tapecycle = 3: oldest_reusable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();
$catalog->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 5);
$testconf->write( do_catalog => 0, tapelist=><<TAPELIST
20090424173004 TEST-4 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173003 TEST-3 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173002 TEST-2 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173001 TEST-1 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424172000 CONF-4 reuse POOL:CONFCONF
TAPELIST
);
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173004, 0, "TESTCONF", "TESTCONF", "TEST-4", 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173003, 0, "TESTCONF", "TESTCONF", "TEST-3", 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173002, 0, "TESTCONF", "TESTCONF", "TEST-2", 1);
$catalog->add_simple_dump("localhost","/boot","/boot", 20090424173001, 0, "TESTCONF", "TESTCONF", "TEST-1", 1);
$catalog->compute_retention();
$storage = Amanda::Storage->new(catalog => $catalog);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    catalog => $catalog,
    storage => $storage);

is($taperscan->oldest_reusable_volume(new_label_ok => 1), undef,
   "simple tapelist, tapecycle = 5: oldest_reusable_volume correct (undef)");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();
$catalog->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 2);
$testconf->write( do_catalog => 0, tapelist =><<TAPELIST
20090424173004 TEST-4 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173003 TEST-3 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173002 TEST-2 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173001 TEST-1 no-reuse POOL:TESTCONF STORAGE:TESTCONF
TAPELIST
);
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$storage = Amanda::Storage->new(catalog => $catalog);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    catalog => $catalog,
    storage => $storage);

is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-2",
   "no-reuse in tapelist, tapecycle = 2: oldest_reusable_volume correct");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();
$catalog->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 4);
$testconf->write( do_catalog => 0, tapelist =><<TAPELIST
20090424173004 TEST-4 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173003 TEST-3 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173002 TEST-2 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173001 TEST-1 no-reuse POOL:TESTCONF STORAGE:TESTCONF
TAPELIST
);
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$storage = Amanda::Storage->new(catalog => $catalog);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    catalog => $catalog,
    storage => $storage);
is($taperscan->oldest_reusable_volume(new_label_ok => 1), undef,
   "no-reuse in tapelist, tapecycle = 4: oldest_reusable_volume correct (undef)");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();
$catalog->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 3);
$testconf->write( do_catalog => 0, tapelist=><<TAPELIST
20090424173003 TEST-3 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173002 TEST-2 reuse POOL:TESTCONF STORAGE:TESTCONF
20090424173001 TEST-1 reuse POOL:TESTCONF STORAGE:TESTCONF
0 TEST-4 reuse POOL:TESTCONF
TAPELIST
);
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$storage = Amanda::Storage->new(catalog => $catalog);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    catalog => $catalog,
    retention_tapes => 2,
    storage => $storage);
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 reusable");

is($taperscan->oldest_reusable_volume(new_label_ok => 0), "TEST-1",
   "newly labeled in tapelist, retention_tapes = 2, !new_label_ok: oldest_reusable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 0), " TEST-1 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 0), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 0), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 0), " TEST-4 not reusable");

$taperscan->quit();
$storage->quit();
$catalog->quit();

unlink $temp_filename1;
unlink $temp_filename2;
