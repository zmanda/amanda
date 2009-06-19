# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 75;

use lib "@amperldir@";
use warnings;
use strict;
use Data::Dumper;
use Amanda::Util;
use Installcheck;
use POSIX;

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

for my $a (keys %unquote_checks) {
    for my $b ("unquoted", "\"quoted str\"") {
	my ($a_out, $b_out) = Amanda::Util::skip_quoted_string("$a $b");
	is_deeply([$a_out, $b_out], [$a, $b],
	    "skip_quoted string over " . Dumper("$a $b"));
    }
}

{
    my ($a, $b) = Amanda::Util::skip_quoted_string("foobar");
    is($a, "foobar",
       "skip_quoted_string with one quoted string (first argument)");
    is($b, undef,
       "skip_quoted_string with one quoted string (second argument)");
}

my @try_bracing = (
    [ 'abc' ],
    [ 'abc', 'def' ],
    [ 'abc', 'def', 'ghi' ],
    [ 'a,b', 'c' ],
    [ 'a', 'b,c' ],
    [ 'a', 'b,c', 'd' ],
    [ 'a{b', 'c' ],
    [ 'a', 'b{c' ],
    [ 'a', 'b{c', 'd' ],
    [ 'a}b', 'c' ],
    [ 'a', 'b}c' ],
    [ 'a', 'b}c', 'd' ],
    [ 'a\\,b', 'c\\{d', 'e\\}f' ],
);

for my $strs (@try_bracing) {
    my $rt = [ Amanda::Util::expand_braced_alternates(
		    Amanda::Util::collapse_braced_alternates($strs)) ];
    is_deeply($rt, $strs,
	      "round-trip of " . Dumper($strs));
}

## test full_read and full_write

my $testfile = "$Installcheck::TMP/Amanda_Util";
my $fd;
my $buf;

# set up a 1K test file
{
    open (my $fh, ">", $testfile) or die("Opening $testfile: $!");
    print $fh 'abcd' x 256;
    close($fh);
}

$! = 0;
my $rv = Amanda::Util::full_read(-1, 13);
isnt($!, '', "bad full_read gives a nonzero errno ($!)");

$! = 0;
$rv = Amanda::Util::full_write(-1, "hello", 5);
isnt($!, '', "bad full_write gives a nonzero errno ($!)");

$fd = POSIX::open($testfile, POSIX::O_RDONLY);
die "Could not open '$testfile'" unless defined $fd;

$! = 0;
$buf = Amanda::Util::full_read($fd, 1000);
is(length($buf), 1000, "a valid read gets the right number of bytes");
is(substr($buf, 0, 8), "abcdabcd", "..and what looks like the right data");
is($!, '', "..and no error");

$! = 0;
$buf = Amanda::Util::full_read($fd, 1000);
is(length($buf), 24, "a second read, to EOF, gets the right number of bytes");
is(substr($buf, 0, 8), "abcdabcd", "..and what looks like the right data");
is($!, '', "..and no error");

POSIX::close($fd);

$fd = POSIX::open($testfile, POSIX::O_WRONLY);
die "Could not open '$testfile'" unless defined $fd;

$! = 0;
$rv = Amanda::Util::full_write($fd, "swank!", 6);
is($rv, 6, "full_write returns number of bytes written");
is($!, '', "..and no error");

POSIX::close($fd);

unlink($testfile);

# just a quick check for split_quoted_strings - thorough checks are done in
# common-src/quoting-test.c.
is_deeply([ Amanda::Util::split_quoted_strings('one "T W O" thr\ ee'), ],
          [ "one", "T W O", "thr ee" ],
          "split_quoted_strings seems to work");

# check out get_fs_usage
my $fs_usage = Amanda::Util::get_fs_usage(POSIX::getcwd);
if ($fs_usage) {
    ok($fs_usage->{'blocks'}, "get_fs_usage returns something");
} else {
    fail("get_fs_usage fails: $!");
}
