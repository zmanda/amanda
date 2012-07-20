#! @PERL@
# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

# This utility is useful for setting up a long-running NDMP tape service while
# developing.  It's not used in normal Amanda operations, nor even during
# installchecks.  Note that you will need to run the =setupcache installcheck
# first, to generate the cached NDMP dump.

use lib "@top_srcdir@/installcheck";
use lib "@amperldir@";

use Installcheck;
use Installcheck::Mock;
use Installcheck::Config;
use Installcheck::Dumpcache;

die "not built with ndmp" unless
    Amanda::Util::built_with_component("ndmp");

Installcheck::Dumpcache::load("ndmp");
my $ndmp = Installcheck::Mock::NdmpServer->new(no_reset => 1);
$ndmp->edit_config();

print "NDMP test daemon running for config TESTCONF; put this script in\n";
print "the background and kill it when you are finished.\n";

while (1) { sleep(100); }
