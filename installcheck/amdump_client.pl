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

use Test::More tests => 3;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
use Amanda::Config qw( :init );
use Amanda::Paths;
use Sys::Hostname;

my $testconf;

# Just run amdump.

$testconf = Installcheck::Run::setup();
$testconf->add_param('autolabel', '"TESTCONF%%" empty volume_error');

my $hostname = hostname;
$testconf->add_client_param('auth', '"local"');
$testconf->add_client_param('amdump_server', "\"$hostname\"");

# and one with the amgtar application
$testconf->add_dle(<<EODLE);
$hostname diskname $diskname {
    installcheck-test
    program "APPLICATION"
    dump-limit server
    application {
	plugin "amgtar"
	property "ATIME-PRESERVE" "NO"
    }
}
EODLE
$testconf->write();

ok(run('amdump_client', '--config', 'TESTCONF', 'list'), "'amdump_client list' runs successfully");
is($Installcheck::Run::stdout,
    "config: TESTCONF\ndiskname\n",
    "'amdump_client list' list diskname");
ok(run('amdump_client', '--config', 'TESTCONF', 'dump'), "'amdump_client dump' runs successfully")
    or amdump_diag();

Installcheck::Run::cleanup();
