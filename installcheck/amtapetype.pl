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

use Test::More tests => 5;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck::Run qw(run run_get run_err vtape_dir);

use File::Path qw(rmtree);

##
# First, check that the script runs -- this is essentially a syntax/strict
# check of the script.

ok(!run('amtapetype'),
    "'amtapetype' with no arguments returns an error exit status");
like($Installcheck::Run::stderr, qr(\AUsage: )i,
    ".. and gives usage message on stderr");

##
# Set up a small vtape to write to

my $testconf = Installcheck::Run::setup();
$testconf->add_device("smallvtape", [
    "tapedev" => '"file:' . vtape_dir() . '/drive0"',
    "device_property" => '"MAX_VOLUME_USAGE" "2m"', # need at least 1M
]);
$testconf->write();
mkdir vtape_dir() . '/drive0';
symlink "../slot1",  vtape_dir() . '/drive0/data';

like(run_get('amtapetype', 'TESTCONF', 'smallvtape'),
    qr/define tapetype unknown-tapetype.*blocksize 32 kbytes/s,
    "amtapetype runs successfully on a small vtape");

ok(run_err('amtapetype', 'TESTCONF', 'smallvtape'),
    "a second run on the same device fails because -f isn't used") or die;

like(run_get('amtapetype', 'TESTCONF', '-f', '-b', '33000', 'smallvtape'),
    qr/add device-property/,
    "with a non-kilobyte block size, directs user to add a device_property");
rmtree(vtape_dir() . '/drive0');
