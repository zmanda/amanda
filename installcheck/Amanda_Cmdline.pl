# Copyright (c) 2007,2008 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 24;

use lib "@amperldir@";
use Amanda::Paths;
use Amanda::Cmdline;

# convert a dumpspec_t object to an array, for easy is_deeply() comparisons
sub ds2av {
    my ($ds) = @_;
    return (
	$ds->{'host'},
	$ds->{'disk'},
	$ds->{'datestamp'},
	$ds->{'level'},
    );
}

# test dumpspec_t objects

is_deeply([ ds2av(Amanda::Cmdline::dumpspec_t->new("h", "di", "ds", "l")) ],
	  [ "h", "di", "ds", "l" ],
	  "dumpspec_t constructor returns a valid dumpspec");

is_deeply([ ds2av(Amanda::Cmdline::dumpspec_t->new("h", "di", "ds", undef)) ],
	  [ "h", "di", "ds", undef ],
	  "dumpspec_t constructor returns a valid dumpspec with only 3 args");

is_deeply([ ds2av(Amanda::Cmdline::dumpspec_t->new("h", "di", undef, undef)) ],
	  [ "h", "di", undef, undef ],
	  "dumpspec_t constructor returns a valid dumpspec with only 2 args");

is_deeply([ ds2av(Amanda::Cmdline::dumpspec_t->new("h", undef, undef, undef)) ],
	  [ "h", undef, undef, undef ],
	  "dumpspec_t constructor returns a valid dumpspec with only 1 arg");

# TODO: test parse_dumpspecs
my @specs;

@specs = Amanda::Cmdline::parse_dumpspecs(["h1", "d1", "h2", "d2"], 0);
is(@specs, 2, "parse of four elements with no flags yields 2 specs");
is_deeply([ ds2av($specs[0]) ], [ "h1", "d1", undef, undef ], "..first spec is correct");
is_deeply([ ds2av($specs[1]) ], [ "h2", "d2", undef, undef ], "..second spec is correct");

@specs = Amanda::Cmdline::parse_dumpspecs(["h1", "d1", "ds1", "h2", "d2", "ds2" ], $Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP);
is(@specs, 2, "parse of six elements with CMDLINE_PARSE_DATESTAMP yields 2 specs");
is_deeply([ ds2av($specs[0]) ], [ "h1", "d1", "ds1", undef ], "..first spec is correct");
is_deeply([ ds2av($specs[1]) ], [ "h2", "d2", "ds2", undef ], "..second spec is correct");

@specs = Amanda::Cmdline::parse_dumpspecs(["h1", "d1", "ds1", "lv1", "h2", "d2", "ds2", "lv2" ],
		$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP | $Amanda::Cmdline::CMDLINE_PARSE_LEVEL);
is(@specs, 2, "parse of eight elements with CMDLINE_PARSE_DATESTAMP and CMDLINE_PARSE_LEVEL yields 2 specs");
is_deeply([ ds2av($specs[0]) ], [ "h1", "d1", "ds1", "lv1" ], "..first spec is correct");
is_deeply([ ds2av($specs[1]) ], [ "h2", "d2", "ds2", "lv2" ], "..second spec is correct");

@specs = Amanda::Cmdline::parse_dumpspecs(["h1", "d1", "ds1", "lv1" ],
		$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP | $Amanda::Cmdline::CMDLINE_PARSE_LEVEL);
is(@specs, 1, "parse of four elements with CMDLINE_PARSE_DATESTAMP and CMDLINE_PARSE_LEVEL yields one spec");
is_deeply([ ds2av($specs[0]) ], [ "h1", "d1", "ds1", "lv1" ], "..which is correct");

@specs = Amanda::Cmdline::parse_dumpspecs(["h1", "d1", "ds1" ],
		$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP | $Amanda::Cmdline::CMDLINE_PARSE_LEVEL);
is(@specs, 1, "parse of three elements with CMDLINE_PARSE_DATESTAMP and CMDLINE_PARSE_LEVEL yields one spec");
is_deeply([ ds2av($specs[0]) ], [ "h1", "d1", "ds1", undef ], "..which is correct");

@specs = Amanda::Cmdline::parse_dumpspecs(["h1", "d1" ],
		$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP | $Amanda::Cmdline::CMDLINE_PARSE_LEVEL);
is(@specs, 1, "parse of two elements with CMDLINE_PARSE_DATESTAMP and CMDLINE_PARSE_LEVEL yields one spec");
is_deeply([ ds2av($specs[0]) ], [ "h1", "d1", undef, undef ], "..which is correct");

@specs = Amanda::Cmdline::parse_dumpspecs(["h1" ],
		$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP | $Amanda::Cmdline::CMDLINE_PARSE_LEVEL);
is(@specs, 1, "parse of one element with CMDLINE_PARSE_DATESTAMP and CMDLINE_PARSE_LEVEL yields one spec");
is_deeply([ ds2av($specs[0]) ], [ "h1", undef, undef, undef ], "..which is correct");

@specs = Amanda::Cmdline::parse_dumpspecs([],
		$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP | $Amanda::Cmdline::CMDLINE_PARSE_LEVEL);
is(@specs, 0, "parse of no elements with CMDLINE_PARSE_DATESTAMP and CMDLINE_PARSE_LEVEL yields no specs");

@specs = Amanda::Cmdline::parse_dumpspecs([],
		$Amanda::Cmdline::CMDLINE_PARSE_DATESTAMP | $Amanda::Cmdline::CMDLINE_PARSE_LEVEL
		| $Amanda::Cmdline::CMDLINE_EMPTY_TO_WILDCARD);
is(@specs, 1, "parse of no elements with CMDLINE_EMPTY_TO_WILDCARD yields one spec");

# test format_dumpspec_components

is(Amanda::Cmdline::format_dumpspec_components("h", "di", "ds", "l"),
   "h di ds l",
   "format_dumpspec_components works ok");
