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

use Test::More tests => 28;

use lib "@amperldir@";
use Data::Dumper;
use Amanda::Util;

# Data::Dumper is used to output strings with control characters
# in them, below
$Data::Dumper::Useqq = 1;  # quote strings
$Data::Dumper::Terse = 1;  # no $VAR1 = ..
$Data::Dumper::Indent = 0; # no newlines

# most of Amanda::Util is tested via running applications that use it

# Tests for quote_string and unquote string.  First, some fuzzing of the
# quote + unquote round-trip.
my @fuzzstrs = (
    '',
    'abcd',
    '"',
    '""',
    '\\',
    "\t", "\r", "\n", "\f",
    '\\\\\\\\', # memory overflow?
    'backslash\nletter',
    'backslash\tletter',
    '"quoted"',
    "line\nanother", # real newline
    "ends with slash\\",
    '"starts with quote',
    'ends with quote"',
    "single'quote",
);

for my $fuzzstr (@fuzzstrs) {
    is(Amanda::Util::unquote_string(Amanda::Util::quote_string($fuzzstr)), $fuzzstr,
	"fuzz " . Dumper($fuzzstr));
}

# since users often provide quoted strings (e.g., in config files), test that chosen
# quoted strings are correctly unquoted.  The need to quote the quoted strings for perl
# makes this a little hard to read..
my %unquote_checks = (
    '""' => '',
    'abcd' => 'abcd',
    '"abcd"' => 'abcd',
    '"\t"' => "\t",
    '"\r"' => "\r",
    '"\n"' => "\n",
    '"\f"' => "\f",
    '"\t"' => "\t",
    '"\\\\n"' => '\n', # literal \
    '"\\\\"' => "\\",
    '"\""' => "\"",
);

while (my ($qstr, $uqstr) = each %unquote_checks) {
    is(Amanda::Util::unquote_string($qstr), $uqstr,
	"unquote " . Dumper($qstr));
}
