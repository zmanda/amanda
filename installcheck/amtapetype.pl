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

use Test::More tests => 5;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Run qw(run run_get run_err vtape_dir);

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
    "tapedev" => '"file:' . vtape_dir() . '"',
    "device_property" => '"MAX_VOLUME_USAGE" "2m"', # need at least 1M
]);
$testconf->write();

like(run_get('amtapetype', 'TESTCONF', 'smallvtape'),
    qr/define tapetype unknown-tapetype.*blocksize 32 kbytes/s,
    "amtapetype runs successfully on a small vtape");

ok(run_err('amtapetype', 'TESTCONF', 'smallvtape'),
    "a second run on the same device fails because -f isn't used") or die;

like(run_get('amtapetype', 'TESTCONF', '-f', '-b', '33000', 'smallvtape'),
    qr/add device-property/,
    "with a non-kilobyte block size, directs user to add a device_property");
