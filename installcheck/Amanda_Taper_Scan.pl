# Copyright (c) Zmanda Inc.  All Rights Reserved.
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
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Taper::Scan;
use Amanda::Storage;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $tapelist_filename = "$Installcheck::TMP/tapelist";
sub set_tapelist {
    my ($content) = @_;
    open(my $fh, ">", $tapelist_filename) or die("opening '$tapelist_filename': $!");
    print $fh $content;
    close($fh);
}

my $testconf = Installcheck::Run::setup();
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');

# we use a "traditional" Amanda::Taper::Scan object, only because instantiating
# the parent class alone is difficult.  We never call scan(), so traditional's
# methods are never invoked in this test.
my ($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);
my $storage = Amanda::Storage->new(tapelist => $tapelist);
my $taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    tapelist => $tapelist,
    storage => $storage,
    );

set_tapelist(<<EOF);
20090424173004 TEST-4 reuse
20090424173003 TEST-3 reuse
20090424173002 TEST-2 reuse
20090424173001 TEST-1 reuse
20090424172000 CONF-4 reuse
EOF
$taperscan->quit();
$storage->quit();

$testconf->add_param('tapelist', "\"$tapelist_filename\"");
$testconf->remove_param('labelstr');
$testconf->add_param('labelstr', '"TEST-[0-9]"');
$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 2);
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);
$storage = Amanda::Storage->new(tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    tapelist => $tapelist,
    storage => $storage);
$taperscan->read_tapelist();

is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-1",
   "simple tapelist, tapecycle = 2: oldest_reusable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 3);
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);
$storage = Amanda::Storage->new(tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    tapelist => $tapelist,
    storage => $storage);
$taperscan->read_tapelist();

is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-1",
   "simple tapelist, tapecycle = 3: oldest_reusable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 5);
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);
$storage = Amanda::Storage->new(tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    tapelist => $tapelist,
    storage => $storage);
$taperscan->read_tapelist();

is($taperscan->oldest_reusable_volume(new_label_ok => 1), undef,
   "simple tapelist, tapecycle = 5: oldest_reusable_volume correct (undef)");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();

set_tapelist(<<EOF);
20090424173004 TEST-4 reuse
20090424173003 TEST-3 reuse
20090424173002 TEST-2 reuse
20090424173001 TEST-1 no-reuse
EOF

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 2);
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);
$storage = Amanda::Storage->new(tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    tapelist => $tapelist,
    storage => $storage);
$taperscan->read_tapelist();

is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-2",
   "no-reuse in tapelist, tapecycle = 2: oldest_reusable_volume correct");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$taperscan->quit();
$storage->quit();

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 4);
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);
$storage = Amanda::Storage->new(tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    tapelist => $tapelist,
    storage => $storage);
$taperscan->read_tapelist();
is($taperscan->oldest_reusable_volume(new_label_ok => 1), undef,
   "no-reuse in tapelist, tapecycle = 4: oldest_reusable_volume correct (undef)");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");
$storage->quit();

set_tapelist(<<EOF);
20090424173003 TEST-3 reuse
20090424173002 TEST-2 reuse
20090424173001 TEST-1 reuse
0 TEST-4 reuse
EOF

$testconf->remove_param('tapecycle');
$testconf->add_param('tapecycle', 3);
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
($tapelist, $message) = Amanda::Tapelist->new($tapelist_filename);
$storage = Amanda::Storage->new(tapelist => $tapelist);
$taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => undef, # (not used)
    tapelist => $tapelist,
    retention_tapes => 2,
    storage => $storage);
$taperscan->read_tapelist();
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

