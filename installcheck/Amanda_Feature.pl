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

use Test::More tests => 31;
use strict;
use warnings;

use lib "@amperldir@";
use Amanda::Feature;
use Amanda::Debug;
use Installcheck;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# some round-trip tests
for my $str (qw(
		0
		00
		000
		0000
		ff
		ffff
		000f
		00ff
		0fff
		abaa
		aaaaaa
		aaaaaaaa
		aaaaaaaaaa
		aaaaaaaaaaaaaa
		aaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
		aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
		)) {
    # (note that odd-length strings will trigger a warning)
    my $feat = Amanda::Feature::Set->from_string($str);
    my $str2 = $feat->as_string();
    my $feat2 = Amanda::Feature::Set->from_string($str2);
    my $str3 = $feat2->as_string();
    is($str2, $str3, "round-trip '$str' -> '$str2'")
}

# check the various constructors
like((Amanda::Feature::Set->old())->as_string(),
    qr/^[0-9a-f]+$/,
    "old constructor");
like((Amanda::Feature::Set->mine())->as_string(),
    qr/^[0-9a-f]+$/,
    "mine constructor");

# and some bit flags
my $feat = Amanda::Feature::Set->mine();
ok($feat->has($Amanda::Feature::fe_amrecover_feedme_tape),
    "'mine' features include fe_amrecover_feedme_tape");
$feat->remove($Amanda::Feature::fe_amrecover_feedme_tape);
ok(!$feat->has($Amanda::Feature::fe_amrecover_feedme_tape),
    "fe_amrecover_feedme_tape removed");

$feat = Amanda::Feature::Set->old();
ok(!$feat->has($Amanda::Feature::fe_req_xml),
    "old set does not have fe_req_xml");
$feat->add($Amanda::Feature::fe_req_xml);
ok($feat->has($Amanda::Feature::fe_req_xml),
    "fe_req_xml added");

