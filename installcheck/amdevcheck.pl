# Copyright (c) 2006 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

use Test::More qw( no_plan );

use Amconfig;
use lib "@amperldir@";
use Amanda::Paths;

sub amdevcheck {
    my $cmd = "$sbindir/amdevcheck " . join(" ", @_) . " 2>&1";
    my $result = `$cmd`;
    chomp $result;
    return $result;
}

my $testconf;

##
# First, try amgetconf out without a config

like(amdevcheck(), qr(\AUsage: )i, 
    "bare 'amdevcheck' gives usage message");
like(amdevcheck("this-probably-doesnt-exist"), qr(could not open conf file)i, 
    "error message when configuration parameter doesn't exist");

##
# Next, work against a basically empty config

# this is re-created for each test
$testconf = Amconfig->new();
$testconf->add_param("tapedev", '"/dev/null"');
$testconf->write();

# test some defaults
like(amdevcheck('TESTCONF'), qr{File /dev/null is not a tape device},
    "uses tapedev by default");

##
# Now use a config with a vtape

# this is re-created for each test
$testconf = Amconfig->new();
$testconf->setup_vtape();
$testconf->write();

is_deeply([ sort split "\n", amdevcheck('TESTCONF') ],
	  [ sort "VOLUME_UNLABELED", "VOLUME_ERROR", "DEVICE_ERROR" ],
    "empty vtape described as VOLUME_UNLABELED, VOLUME_ERROR, DEVICE_ERROR");

like(amdevcheck('TESTCONF', "/dev/null"), qr{File /dev/null is not a tape device},
    "can override device on the command line");
