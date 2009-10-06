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

use Test::More tests => 2;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Amanda::NDMP;
use Amanda::Debug;
use Amanda::Config qw( :init );

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

sub try_connect {
    my ($port) = @_;
    my $testconf = Installcheck::Config->new();
    $testconf->add_param("ndmp-proxy-port", $port);
    $testconf->write();

    my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
    if ($cfg_result != $CFGERR_OK) {
	my ($level, @errors) = Amanda::Config::config_errors();
	die(join "\n", @errors);
    }

    my $fd = Amanda::NDMP::connect_to_ndmp_proxy();

    pass("successfully connected to ndmp-proxy");
    # dropping the connection should kill the proxy..
}

# first try it successfully
try_connect(Installcheck::get_unused_port());

# and then unsuccessfully (with an illegal port number)
eval {
    try_connect(1);
    fail("succeeded in connecting on port 1?!");
};
like($@, qr/^ndmp-proxy failed:/, "proxy startup error is properly detected");
