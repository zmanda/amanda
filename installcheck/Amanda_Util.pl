# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 130;

use lib "@amperldir@";
use warnings;
use strict;
use Data::Dumper;
use Amanda::Util qw(slurp burp safe_overwrite_file);
use Installcheck;
use POSIX;

# Data::Dumper is used to output strings with control characters
# in them, below
$Data::Dumper::Useqq = 1;  # quote strings
$Data::Dumper::Terse = 1;  # no $VAR1 = ..
$Data::Dumper::Indent = 0; # no newlines

# most of Amanda::Util is tested via running applications that use it

# Test hexencode/hexdecode lightly (they have a "make check" test)
is(Amanda::Util::hexencode("hi"), "hi", "no encoding needed");
is(Amanda::Util::hexencode("hi!"), "hi%21", "encoding");
is(Amanda::Util::hexdecode("hi%21"), "hi!", "decoding");
ok(!eval {Amanda::Util::hexdecode("%"); 1}, "decoding error throws exception");

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

is_deeply([ Amanda::Util::skip_quoted_string("foobar") ],
	  [ "foobar", undef ],
   "skip_quoted_string with one quoted string");

is_deeply([ Amanda::Util::skip_quoted_string("foo  bar") ],
	  [ "foo", " bar" ],
   "skip_quoted_string with two spaces keeps second space");

is_deeply([ Amanda::Util::skip_quoted_string("foo\tbar") ],
	  [ "foo", "bar" ],
   "skip_quoted_string with a tab still splits");

is_deeply([ Amanda::Util::split_quoted_string_friendly("a b c d") ],
	  [ qw(a b c d) ],
	  "split_quoted_string_friendly with a basic split");

is_deeply([ Amanda::Util::split_quoted_string_friendly("\ta   b\nc \t \td   ") ],
	  [ qw(a b c d) ],
	  "split_quoted_string_friendly with extra whitespace");

is_deeply([ Amanda::Util::split_quoted_string_friendly("") ],
	  [ ],
	  "split_quoted_string_friendly with empty string");

is_deeply([ Amanda::Util::split_quoted_string_friendly("\n\t ") ],
	  [ ],
	  "split_quoted_string_friendly with just whitespace");

is_deeply([ Amanda::Util::split_quoted_string_friendly("\n\"hi there\"\t ") ],
	  [ 'hi there' ],
	  "split_quoted_string_friendly with one string (containing whitespace)");

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

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{0..3,5}") ],
    [ qw(t0 t1 t2 t3 t5) ],
    "expand_braced_alternates('t{0..3,5}')");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{13..12}") ],
    [ qw(t13..12) ],
    "expand_braced_alternates('t{13..12}') (sequence not parsed)");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{999..999}") ],
    [ qw(t999) ],
    "expand_braced_alternates('t{999..999}')");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{0..3}") ],
    [ qw(t0 t1 t2 t3) ],
    "expand_braced_alternates('t{0..3}')");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{10..13}") ],
    [ qw(t10 t11 t12 t13) ],
    "expand_braced_alternates('t{10..13}')");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{9..13}") ],
    [ qw(t9 t10 t11 t12 t13) ],
    "expand_braced_alternates('t{9..13}')");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{09..13}") ],
    [ qw(t09 t10 t11 t12 t13) ],
    "expand_braced_alternates('t{09..13}')");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{009..13}") ],
    [ qw(t009 t010 t011 t012 t013) ],
    "expand_braced_alternates('t{009..13}') (ldigits > rdigits)");

is_deeply(
    [ sort(+Amanda::Util::expand_braced_alternates("x{001..004}y{1..2}z")) ],
    [ sort(qw( x001y1z x002y1z x003y1z x004y1z x001y2z x002y2z x003y2z x004y2z )) ],
    "expand_braced_alternates('x{001..004}y{1..2}z')");

is_deeply(
    [ Amanda::Util::expand_braced_alternates("t{1..100}e") ],
    [ map { "t$_"."e" } (1 .. 100) ],
    "expand_braced_alternates('t{1..100}e')");

my @try_sanitise = (
    [ '', '' ],
    [ 'foo', 'foo' ],
    [ '/', '_' ],
    [ ':', '_' ],
    [ '\\', '_' ],
    [ 'foo/bar:baz', 'foo_bar_baz' ],
);

for my $strs (@try_sanitise) {
    my ($in, $exp) = @{$strs};
    is(Amanda::Util::sanitise_filename($in), $exp, "sanitise " . $in);
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

## tests for slurp and burp

my $corpus = <<EOF;

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean id
neque interdum ligula euismod cursus at vel tortor. Praesent interdum
molestie felis, nec vehicula lorem luctus quis. Suspendisse in laoreet
diam. Maecenas fringilla lectus vel libero vehicula
condimentum. Aenean ac luctus nulla. Nullam sagittis lacinia orci, et
consectetur nunc malesuada sed. Nulla eu felis ipsum. Duis feugiat
risus a lectus blandit lobortis. Fusce quis neque neque. Class aptent
taciti sociosqu ad litora torquent per conubia nostra, per inceptos
himenaeos.

Nulla at auctor mi. Mauris vestibulum ante vel metus auctor at iaculis
neque semper. Nullam ipsum lorem, convallis ullamcorper ornare in,
lacinia eu magna. Vivamus vulputate fermentum quam, quis pulvinar eros
varius at. Phasellus ac diam nec erat elementum facilisis et ac
est. Nunc nec nulla nec quam tristique dignissim at ut arcu. Integer
accumsan tincidunt nisi non consectetur. Donec nec massa sed dui
auctor sodales eget ac elit. Aliquam luctus sollicitudin nibh, eu
volutpat augue tempor sed. Mauris ac est et neque mollis iaculis vel
in libero. Duis molestie felis ultrices elit fringilla varius. In eget
turpis dignissim sem varius sagittis eget vel neque.

EOF

my $burp_corpus_fname = "$Installcheck::TMP/burp_corpus";

ok( burp( $burp_corpus_fname, $corpus ), "burp round-trip test" );
is( slurp($burp_corpus_fname), $corpus, "slurp round-trip test" );

# test safe_overwrite_file

my $sof_data = <<EOF;
DISK planner somebox /lib
START planner date 20080111
START driver date 20080111
STATS driver hostname somebox
STATS driver startup time 0.051
FINISH planner date 20080111 time 82.721
START taper datestamp 20080111 label Conf-001 tape 1
SUCCESS dumper somebox /lib 20080111 0 [sec 0.209 kb 1970 kps 9382.2 orig-kb 1970]
SUCCESS chunker somebox /lib 20080111 0 [sec 0.305 kb 420 kps 1478.7]
STATS driver estimate somebox /lib 20080111 0 [sec 1 nkb 2002 ckb 480 kps 385]
PART taper Conf-001 1 somebox /lib 20080111 1/1 0 [sec 4.813543 kb 419 kps 87.133307]
DONE taper somebox /lib 20080111 1 0 [sec 4.813543 kb 419 kps 87.133307]
FINISH driver date 20080111 time 2167.581
EOF

ok(safe_overwrite_file($burp_corpus_fname, $sof_data),
    "safe_overwrite_file success");
is(slurp($burp_corpus_fname), $sof_data,
    "safe_overwrite_file round-trip check");

# check out get_fs_usage
my $fs_usage = Amanda::Util::get_fs_usage(POSIX::getcwd);
if ($fs_usage) {
    ok($fs_usage->{'blocks'}, "get_fs_usage returns something");
} else {
    fail("get_fs_usage fails: $!");
}

# check file_lock -- again, full checks are in common-src/amflock-test.c
my $filename = "$Installcheck::TMP/testlock";
unlink($filename);
my $fl = Amanda::Util::file_lock->new($filename);
is($fl->data, undef, "data is initially undefined");
$fl->lock();
is($fl->data, undef, "data is undefined even after lock");
$fl->write("THIS IS MY DATA");
is($fl->data, "THIS IS MY DATA", "data is set correctly after write()");
$fl->unlock();

# new lock object
$fl = Amanda::Util::file_lock->new($filename);
is($fl->data, undef, "data is initially undefined");
$fl->lock();
is($fl->data, "THIS IS MY DATA", "data is set correctly after lock");

## check (un)marshal_tapespec

my @tapespecs = (
    "FOO:1,2;BAR:3" => [ FOO => [ 1, 2 ], BAR => [ 3 ] ],
    "SE\\;MI:0;COL\\:ON:3" => [ 'SE;MI' => [0], 'COL:ON' => [3] ],
    "CO\\,MMA:88,99;BACK\\\\SLASH:3" => [ 'CO,MMA' => [88,99], 'BACK\\SLASH' => [3] ],
    "FUNNY\\;:1;CHARS\\::2;AT\\,:3;END\\\\:4" =>
	[ 'FUNNY;' => [ 1 ], 'CHARS:' => [ 2 ], 'AT,' => [ 3 ], 'END\\' => [ 4 ], ],
    "\\;FUNNY:1;\\:CHARS:2;\\,AT:3;\\\\BEG:4" =>
	[ ';FUNNY' => [ 1 ], ':CHARS' => [ 2 ], ',AT' => [ 3 ], '\\BEG' => [ 4 ], ],
);

while (@tapespecs) {
    my $tapespec = shift @tapespecs;
    my $filelist = shift @tapespecs;
    is(Amanda::Util::marshal_tapespec($filelist), $tapespec,
	    "marshal '$tapespec'");
    is_deeply(Amanda::Util::unmarshal_tapespec($tapespec), $filelist,
	    "unmarshal '$tapespec'");
}

is_deeply(Amanda::Util::unmarshal_tapespec("x:100,99"), [ x => [99,100] ],
    "filenums are sorted when unmarshalled");

is_deeply(Amanda::Util::marshal_tapespec([ x => [100, 99] ]), "x:100,99",
    "un-sorted filenums are NOT sorted when marshalled");

is_deeply(Amanda::Util::unmarshal_tapespec("x:34,34"), [ x => [34, 34] ],
    "duplicate filenums are NOT collapsed when unmarshalled");

is_deeply(Amanda::Util::marshal_tapespec([ x => [34, 34] ]), "x:34,34",
    "duplicate filenums are NOT collapsed when marshalled");

is_deeply(Amanda::Util::unmarshal_tapespec("sim\\\\ple\\:quoted\\;file\\,name"),
    [ "sim\\ple:quoted;file,name" => [0] ],
    "simple non-tapespec string translated like string:0");

is_deeply(Amanda::Util::unmarshal_tapespec("tricky\\,tricky\\:1,2,3"),
    [ "tricky,tricky:1,2,3" => [0] ],
    "tricky non-tapespec string also translated to string:0");

is_deeply(Amanda::Util::unmarshal_tapespec("\\:3"), # one slash
    [ ":3" => [0] ],
    "one slash escapes the colon");

is_deeply(Amanda::Util::unmarshal_tapespec("\\\\:3"), # two slashes
    [ "\\" => [3] ],
    "two slashes escape to one");

is_deeply(Amanda::Util::unmarshal_tapespec("\\\\\\:3"), # three slashes
    [ "\\:3" => [0] ],
    "three slashes escape to a slash and a colon");

is_deeply(Amanda::Util::unmarshal_tapespec("\\\\\\\\:3"), # four slashes
    [ "\\\\" => [3] ],
    "four slashes escape to two");
