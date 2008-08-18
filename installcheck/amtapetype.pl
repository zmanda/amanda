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

use Test::More tests => 2;

use lib "@amperldir@";
use Installcheck::Run qw(run run_get run_err);

##
# First, check that the script runs -- this is essentially a syntax/strict
# check of the script.

ok(!run('amtapetype'),
    "'amtapetype' with no arguments returns an error exit status");
like($Installcheck::Run::stderr, qr(\AUsage: )i,
    ".. and gives usage message on stderr");

# amtapetype demands far more resources than we can allow it to use in a
# test situation, so for now this is the  best we can do.
