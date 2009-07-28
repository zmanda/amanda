# Copyright (c) Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 35;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Changer;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Taper::Scan;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

my $tapelist = "$Installcheck::TMP/tapelist";
sub set_tapelist {
    my ($content) = @_;
    open(my $fh, ">", $tapelist) or die("opening '$tapelist': $!");
    print $fh $content;
    close($fh);
}

# we use a "traditional" Amanda::Taper::Scan object, only because instantiating
# the parent class alone is difficult.  We never call scan(), so traditional's
# methods are never invoked in this test.
my $taperscan = Amanda::Taper::Scan->new(
    algorithm => "traditional",
    changer => {}, # (not used)
    tapelist_filename => $tapelist,
    tapecycle => 1, # will be changed periodically below
    labelstr => "TEST-[0-9]",
    label_new_tapes => "TEST-%",
    );

set_tapelist(<<EOF);
20090424173001 TEST-1 reuse
20090424173002 TEST-2 reuse
20090424173003 TEST-3 reuse
20090424173004 TEST-4 reuse
EOF
$taperscan->read_tapelist();

$taperscan->{'tapecycle'} = 2;
is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-1",
   "simple tapelist, tapecycle = 2: oldest_resuable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");

$taperscan->{'tapecycle'} = 3;
is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-1",
   "simple tapelist, tapecycle = 3: oldest_resuable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");

$taperscan->{'tapecycle'} = 5;
is($taperscan->oldest_reusable_volume(new_label_ok => 1), undef,
   "simple tapelist, tapecycle = 5: oldest_resuable_volume correct (undef)");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");

set_tapelist(<<EOF);
20090424173001 TEST-1 no-reuse
20090424173002 TEST-2 reuse
20090424173003 TEST-3 reuse
20090424173004 TEST-4 reuse
EOF
$taperscan->read_tapelist();

$taperscan->{'tapecycle'} = 2;
is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-2",
   "no-reuse in tapelist, tapecycle = 2: oldest_resuable_volume correct");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");

$taperscan->{'tapecycle'} = 4;
is($taperscan->oldest_reusable_volume(new_label_ok => 1), undef,
   "no-reuse in tapelist, tapecycle = 3: oldest_resuable_volume correct (undef)");
ok(!$taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 not reusable");

set_tapelist(<<EOF);
20090424173001 TEST-1 reuse
20090424173002 TEST-2 reuse
20090424173003 TEST-3 reuse
0 TEST-4 reuse
EOF
$taperscan->read_tapelist();

$taperscan->{'tapecycle'} = 3;
is($taperscan->oldest_reusable_volume(new_label_ok => 1), "TEST-4",
   "newly labeled in tapelist, tapecycle = 3, new_label_ok: oldest_resuable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 1), " TEST-1 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 1), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 1), " TEST-3 not reusable");
ok( $taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 1), " TEST-4 reusable");

$taperscan->{'tapecycle'} = 3;
is($taperscan->oldest_reusable_volume(new_label_ok => 0), "TEST-1",
   "newly labeled in tapelist, tapecycle = 3, !new_label_ok: oldest_resuable_volume correct");
ok( $taperscan->is_reusable_volume(label => "TEST-1", new_label_ok => 0), " TEST-1 reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-2", new_label_ok => 0), " TEST-2 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-3", new_label_ok => 0), " TEST-3 not reusable");
ok(!$taperscan->is_reusable_volume(label => "TEST-4", new_label_ok => 0), " TEST-4 not reusable");

