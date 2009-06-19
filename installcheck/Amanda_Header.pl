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

use Test::More tests => 7;
use strict;

use lib "@amperldir@";
use Amanda::Header;
use Amanda::Debug;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");

# Not much to test, but we can at least exercise the constructor and destructor,
# and the SWIG getters and setters:
ok(my $hdr = Amanda::Header->new(), "can create a dumpfile_t");
is($hdr->{'datestamp'}, '', "newly created dumpfile_t has empty datestamp");
ok($hdr->{'name'} = "TAPE17", "can write to a string in the header");
is($hdr->{'name'}, "TAPE17", "..and get it back");

# set some other attributes so to_string will work
$hdr->{'type'} = $Amanda::Header::F_TAPESTART;
$hdr->{'datestamp'} = '20090102030405';

my $block = $hdr->to_string(32768, 32768);
like($block,
     qr/^AMANDA: TAPESTART DATE 20090102030405 TAPE TAPE17/,
     "generated header looks OK");
is(length($block), 32768, "generated header has correct length");

my $hdr2 = Amanda::Header->from_string($block);
is($hdr2->{'name'}, "TAPE17",
   "from_string gives a reasonable-looking object");
