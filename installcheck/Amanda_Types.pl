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

use Test::More tests => 4;
use strict;

use lib "@amperldir@";
use Amanda::Types;

# Not much to test, but we can at least exercise the constructor and destructor,
# and the SWIG getters and setters:
ok(my $df = Amanda::Types::dumpfile_t->new(), "can create a dumpfile_t");
is($df->{'datestamp'}, '', "newly created dumpfile_t has empty datestamp");
ok($df->{'name'} = "myhost", "can write to a string in the header");
is($df->{'name'}, "myhost", "..and get it back");
