# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 13;
use strict;
use warnings;

use lib "@amperldir@";
use Amanda::Config qw( :init :getconf );
use Amanda::Disklist;
use Installcheck::Config;

my $testconf;

$testconf = Installcheck::Config->new();
$testconf->add_dumptype("mytype", [
    "compress" => "server",
    "starttime" => "1830",
    "amandad_path" => "\"/path/to/amandad\"",
]);
$testconf->add_interface("eth1", []);
$testconf->add_dle("otherbox /home mytype");
$testconf->add_dle("otherbox /disk1 mytype");
$testconf->add_dle("otherbox /disk2 mytype");
$testconf->add_dle(<<EOF);
myhost /mydisk {
    auth "bsd"
} -1 eth1
EOF
$testconf->write();

if (config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") != $CFGERR_OK) {
    config_print_errors();
    die("config errors");
}

is(Amanda::Disklist::read_disklist(), $CFGERR_OK,
    "read_disklist returns CFGERR_OK")
    or die("Error loading disklist");

my ($x, $d, @list);

$x = Amanda::Disklist::get_host("otherbox");
ok($x, "get_host returns a host");
is($x->{'auth'}, 'BSDTCP', "..host has correct auth");
is_deeply([ sort @{$x->{'disks'}} ],
	  [ sort "/disk1", "/disk2", "/home" ],
	  "..and three disks");
is(interface_name($x->{'interface'}->{'config'}), "default", "..and correct interface");

$d = $x->get_disk("/home");
is($d->{'name'}, "/home", "host->get_disk() works");

@list = $x->all_disks();
is_deeply([ sort map { $_->{'name'} } @list ],
	  [ sort "/disk1", "/disk2", "/home" ],
	  "host->all_disks returns all disk objects");

@list = Amanda::Disklist::all_hosts();
is_deeply([ sort( map { $_->{'hostname'} } @list ) ],
	[ sort qw(myhost otherbox) ],
	"all_hosts returns correct hosts");

$x = Amanda::Disklist::get_disk("myhost", "/mydisk");
ok($x, "get_disk returns a disk");
is($x->{'name'}, "/mydisk", "..and it's the right one");

@list = Amanda::Disklist::all_disks();
is(scalar @list, 4, "all_lists returns 4 disks");

$x = Amanda::Disklist::get_interface("eth1");
is(interface_name($x->{'config'}), "eth1", "get_interface returns an interface");

@list = Amanda::Disklist::all_interfaces();
is(scalar @list, 2, "all_interfaces returns two interfaces");
