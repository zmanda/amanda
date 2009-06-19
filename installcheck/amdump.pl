# Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
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

use lib "@amperldir@";
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
use Amanda::Config qw( :init );
use Amanda::Paths;

my $testconf;

# Just run amdump.

$testconf = Installcheck::Run::setup();
$testconf->add_param('label_new_tapes', '"TESTCONF%%"');

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
    application {
	plugin "amgtar"
	property "ATIME-PRESERVE" "NO"
    }
}
EODLE
$testconf->write();

ok(run('amdump', 'TESTCONF'), "amdump runs successfully")
    or amdump_diag();

# Dump a nonexistant client, and see amdump fail.
$testconf = Installcheck::Run::setup();
$testconf->add_dle('does-not-exist.example.com / installcheck-test');
$testconf->write();

ok(!run('amdump', 'TESTCONF'), "amdump fails with nonexistent client");

Installcheck::Run::cleanup();
