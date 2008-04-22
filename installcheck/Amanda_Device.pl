# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 28;
use File::Path;
use strict;

use lib "@amperldir@";
use Installcheck::Run;
use Amanda::Debug;
use Amanda::Device qw( :constants );
use Amanda::Config qw( :getconf :init );
use Amanda::Types;

my $dev;
my ($input, $output);
my $testconf;

# make up a fake dumpfile_t to write with
my $dumpfile = Amanda::Types::dumpfile_t->new();
$dumpfile->{type} = $Amanda::Types::F_DUMPFILE;
$dumpfile->{datestamp} = "20070102030405";
$dumpfile->{name} = "localhost";
$dumpfile->{disk} = "/home";

# get stuff set up
$testconf = Installcheck::Run::setup();
$testconf->write();
config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF') == $CFGERR_OK
    or die("Could not load configuration");

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");

## first, test out the 'null' device.

ok($dev = Amanda::Device->new("null:"), "create null device");
ok($dev->start($ACCESS_WRITE, "NULL1", "19780615010203"), "start in write mode");

# check some info methods
isnt($dev->write_min_size(), 0, "write_min_size > 0");
isnt($dev->write_max_size(), 0, "write_max_size > 0");
isnt($dev->read_max_size(), 0, "read_max_size > 0");

# and properties
my $plist = $dev->property_list();
ok($plist, "got some properties");
is($dev->property_get("canonical_name"), "null:", "property_get");

ok($dev->start_file($dumpfile), "start file");
open($input, "<", Amanda::Config::get_config_filename()) or die("Could not open amanda.conf: $!");
my $queue_fd_1 = Amanda::Device::queue_fd_t->new(fileno($input));
ok($dev->write_from_fd($queue_fd_1), "write some data");
close($input) or die("Error closing amanda.conf");
# ok($dev->finish_file(), "finish file");

ok($dev->finish(), "finish device");

## now let's try a vfs device -- the one Installcheck::Run set it up

ok($dev = Amanda::Device->new(getconf($CNF_TAPEDEV)), "create vfs device");
$dev->set_startup_properties_from_config();

## write a copy of the config file to the tape, three times

ok($dev->start($ACCESS_WRITE, "TESTCONF13", "19780602010203"), "start in write mode");

for (my $i = 1; $i <= 3; $i++) {
    ok($dev->start_file($dumpfile), "start file $i");

    open($input, "<", Amanda::Config::get_config_filename())
	or die("Could not open amanda.conf: $!");
    my $queue_fd_2 = Amanda::Device::queue_fd_t->new(fileno($input));
    ok($dev->write_from_fd($queue_fd_2), "write some data for file $i");
    close($input) or die("Error closing amanda.conf");

    # Device API automatically finishes the file when a write of < write_block_size
    # is made, so don't do it ourselves. TODO: change this
    # ok($dev->finish_file(), "finish file $i");
}

ok($dev->finish(), "finish device");

## append one more copy

ok($dev->start($ACCESS_APPEND, undef, undef), "start in append mode");

ok($dev->start_file($dumpfile), "start file 4");

open($input, "<", Amanda::Config::get_config_filename())
    or die("Could not open amanda.conf: $!");
my $queue_fd_3 = Amanda::Device::queue_fd_t->new(fileno($input));
ok($dev->write_from_fd($queue_fd_3), "write some data for file 4");
close($input) or die("Error closing amanda.conf");

# Device API automatically finishes the file when a write of < write_block_size
# is made, so don't do it ourselves. TODO: change this
# ok($dev->finish_file(), "finish file $i");

ok($dev->finish(), "finish device");

## try reading that back

ok($dev->start($ACCESS_READ, undef, undef), "start in read mode");

ok($dumpfile = $dev->seek_file(3), "seek to file 3");
is($dumpfile->{name}, "localhost", "header looks familiar");

open($output, ">", Amanda::Config::get_config_filename() . "~")
    or die("Could not open amanda.conf~: $!");
my $queue_fd_4 = Amanda::Device::queue_fd_t->new(fileno($output));
ok($dev->read_to_fd($queue_fd_4), "write data from file 3");
close($output) or die("Error closing amanda.conf~");

ok($dev->finish(), "finish device");
