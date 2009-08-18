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

use Test::More tests => 25;
use strict;

use lib "@amperldir@";
use Amanda::Header;
use Amanda::Debug;
use Amanda::Cmdline;
use Installcheck;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

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

$hdr = Amanda::Header->from_string($block);
is($hdr->{'name'}, "TAPE17",
   "from_string gives a reasonable-looking object");

# test matching
$hdr = Amanda::Header->new();
$hdr->{'name'} = 'foo.bar.baz';
$hdr->{'disk'} = '/foo/bar/baz';
$hdr->{'datestamp'} = '20090102030405';
$hdr->{'dumplevel'} = 31;
$hdr->{'type'} = $Amanda::Header::F_DUMPFILE;

ok(!$hdr->matches_dumpspecs([]), "header doesn't match empty list of dumpspecs");

ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new('foo.bar.baz', undef, undef, undef)]),
   'header matches exact host dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new('foo', undef, undef, undef)]),
   'header matches partial host dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new('?a*', undef, undef, undef)]),
   'header matches host pattern dumpspec');

ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, '/foo/bar/baz', undef, undef)]),
   'header matches exact disk dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, 'bar', undef, undef)]),
   'header matches partial disk dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, '*a?', undef, undef)]),
   'header matches disk pattern dumpspec');

ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, '20090102030405', undef)]),
   'header matches exact datestamp dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, '2009', undef)]),
   'header matches partial datestamp dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, '20090102030404-20090102030406', undef)]),
   'header matches datestamp range dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, '2009-2010', undef)]),
   'header matches datestamp year-only range dumpspec');
ok(!$hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, '20090102030406-20090102030407', undef)]),
   "header doesn't match datestamp range dumpspec that it's outside of");

ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, '31')]),
   'header matches exact level dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, '30-32')]),
   'header matches small level range dumpspec');
ok($hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, '4-50')]),
   'header matches large level range dumpspec');
ok(!$hdr->matches_dumpspecs([Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, '32-50')]),
   "header doesn't match level range it's outside of");

ok($hdr->matches_dumpspecs([
    Amanda::Cmdline::dumpspec_t->new('foo.bar.baz', undef, undef, undef),
    Amanda::Cmdline::dumpspec_t->new(undef, '/foo/bar/baz', undef, undef),
  ]),
  'header matches when two dumpspecs are possible matches');
ok(!$hdr->matches_dumpspecs([
    Amanda::Cmdline::dumpspec_t->new(undef, undef, '20090102030406-20090102030407', undef),
    Amanda::Cmdline::dumpspec_t->new(undef, undef, undef, '32-50'),
  ]),
  'header matches when two dumpspecs are given and neither should match');
