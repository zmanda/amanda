# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 13;
use strict;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Mock;
use Installcheck::Config;
use Amanda::Device qw( :constants );
use Amanda::Config qw( :init );

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $ndmp_port = Installcheck::get_unused_port();
my $ndmp_proxy_port = Installcheck::get_unused_port();

my $testconf = Installcheck::Config->new();
$testconf->add_param("ndmp-proxy-port", $ndmp_proxy_port);
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

my $tapefile = Installcheck::Mock::run_ndmjob($ndmp_port);
pass("started ndmjob in daemon mode");

my $dev = Amanda::Device->new("ndmp:localhost:9i1\@foo");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device fails with invalid port");

$dev = Amanda::Device->new("ndmp:localhost:90000\@foo");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device fails with too-large port");

$dev = Amanda::Device->new("ndmp:localhost:$ndmp_port");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device fails without ..\@device_name");

$dev = Amanda::Device->new("ndmp:localhost:$ndmp_port\@$tapefile");
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device succeeds with correct syntax");

ok($dev->property_set("ndmp_username", "foo"),
    "set ndmp_username property");
is($dev->property_get("ndmp_username"), "foo",
    "..and get the value back");
ok($dev->property_set("ndmp_password", "bar"),
    "set ndmp_password property");
is($dev->property_get("ndmp_password"), "bar",
    "..and get the value back");

# set 'em back to the defaults
$dev->property_set("ndmp_username", "ndmp");
$dev->property_set("ndmp_password", "ndmp");

# ok, let's fire the thing up
ok($dev->start($ACCESS_WRITE, "TEST1", "20090915000000"),
    "start device in write mode")
    or diag $dev->error_or_status();

# TODO: device doesn't write files or blocks yet

ok($dev->finish(),
    "finish device")
    or diag $dev->error_or_status();

is($dev->read_label(), $DEVICE_STATUS_SUCCESS,
    "read label from device")
    or diag $dev->error_or_status();

is($dev->volume_label, "TEST1",
    "volume label read back correctly");
