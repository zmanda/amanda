# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 39;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Data::Dumper;
use Amanda::Paths;
use Amanda::Tapelist;
use Amanda::Cmdline;
use Amanda::Util;
use Amanda::Debug qw( :logging );
use Amanda::Logfile qw(:logtype_t :program_t open_logfile get_logline
		close_logfile log_add $amanda_log_trace_log );
use Amanda::Config qw( :init :getconf config_dir_relative );

Amanda::Debug::dbopen("installcheck");

my $log_filename = "$Installcheck::TMP/Amanda_Logfile_test.log";

# write a logfile and return the filename
sub write_logfile {
    my ($contents) = @_;

    if (!-e $Installcheck::TMP) {
	mkpath($Installcheck::TMP);
    }

    open my $logfile, ">", $log_filename or die("Could not create temporary log file '$log_filename': $!");
    print $logfile $contents;
    close $logfile;

    return $log_filename;
}

####
## RAW LOGFILE ACCESS

my $logfile;
my $logdata;

##
# Test out the constant functions

is(logtype_t_to_string($L_MARKER), "L_MARKER", "logtype_t_to_string works");
is(program_t_to_string($P_DRIVER), "P_DRIVER", "program_t_to_string works");

##
# Regression test for previously missing program types
is( program_t_to_string($P_AMDUMP),
    "P_AMDUMP", "program type for amdump defined" );
is( program_t_to_string($P_AMIDXTAPED),
    "P_AMIDXTAPED", "program type for amidxtaped defined" );
is( program_t_to_string($P_AMFETCHDUMP),
    "P_AMFETCHDUMP", "program type for amfetchdump defined" );
is( program_t_to_string($P_AMCHECKDUMP),
    "P_AMCHECKDUMP", "program type for amcheckdump defined" );

##
# Test a simple logfile

$logdata = <<END;
START planner date 20071026183200
END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a simple logfile");
is_deeply([ get_logline($logfile) ],
	  [ $L_START, $P_PLANNER, "date 20071026183200" ],
	  "reads START line correctly");
ok(!get_logline($logfile), "no second line");
close_logfile($logfile);

##
# Test continuation lines

$logdata = <<END;
INFO chunker line1
  line2
END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a logfile containing continuation lines");
is_deeply([ get_logline($logfile) ],
	  [ $L_INFO, $P_CHUNKER, "line1" ],
	  "can read INFO line");
is_deeply([ get_logline($logfile) ],
	  [ $L_CONT, $P_CHUNKER, "line2" ],
	  "can read continuation line");
ok(!get_logline($logfile), "no third line");
close_logfile($logfile);

##
# Test skipping blank lines

# (retain the two blank lines in the following:)
$logdata = <<END;

STATS taper foo

END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a logfile containing blank lines");
is_deeply([ get_logline($logfile) ],
	  [ $L_STATS, $P_TAPER, "foo" ],
	  "reads non-blank line correctly");
ok(!get_logline($logfile), "no second line");
close_logfile($logfile);

##
# Test BOGUS values and short lines

$logdata = <<END;
SOMETHINGWEIRD somerandomprog bar
MARKER amflush
MARKER amflush put something in curstr
PART
END

$logfile = open_logfile(write_logfile($logdata));
ok($logfile, "can open a logfile containing bogus entries");
is_deeply([ get_logline($logfile) ],
	  [ $L_BOGUS, $P_UNKNOWN, "bar" ],
	  "can read line with bogus program and logtype");
is_deeply([ get_logline($logfile) ],
	  [ $L_MARKER, $P_AMFLUSH, "" ],
	  "can read line with an empty string");
ok(get_logline($logfile), "can read third line (to fill in curstr with some text)");
is_deeply([ get_logline($logfile) ],
	  [ $L_PART, $P_UNKNOWN, "" ],
	  "can read a one-word line, with P_UNKNOWN");
ok(!get_logline($logfile), "no next line");
close_logfile($logfile);

## HIGHER-LEVEL FUNCTIONS

# a utility function for is_deeply checks, below.  Converts a hash to
# an array, for more succinct comparisons
sub res2arr {
    my ($res) = @_;
    return [
	$res->{'timestamp'},
	$res->{'hostname'},
	$res->{'diskname'},
	"$res->{'level'}",
	$res->{'label'},
	"$res->{'filenum'}",
	$res->{'status'},
	$res->{'dump_status'},
	$res->{'message'},
	"$res->{'partnum'}",
	"$res->{'totalparts'}"
    ];
}

# set up a basic config
my $testconf = Installcheck::Config->new();
$testconf->add_param("tapecycle", "20");
$testconf->write();

# load the config
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
    or die("Could not load config");
my $tapelist = config_dir_relative("tapelist");
my $logdir = $testconf->{'logdir'};

# test log_add
{
    my $filename = "$logdir/log";

    -f "$filename" and unlink("$filename");
    log_add($L_INFO, "This is my info");
    log_add($L_START, "blah blah blah date 20300405060708 blah blah");

    open(my $fh, "<", $filename) or die("open $filename: $!");
    my $logdata = do { local $/; <$fh> };
    close($fh);

    like($logdata, qr/^INFO Amanda_Logfile This is my info/, "log_add works");

    is(Amanda::Logfile::get_current_log_timestamp(), "20300405060708",
	"get_current_log_timestamp finds a timestamp");

    Amanda::Logfile::log_rename("20300405060708");

    ok(! -f $filename, "after log_rename, /log is gone");
    ok(-f "$filename.20300405060708.0", "..and log.20300405060708.0 exists");
}

# set up and read the tapelist (we don't use Amanda::Tapelist to write this,
# in case it's broken)
open my $tlf, ">", $tapelist or die("Could not write tapelist");
print $tlf "20071111010002 TESTCONF006 reuse\n";
print $tlf "20071110010002 TESTCONF005 reuse\n";
print $tlf "20071109010002 TESTCONF004 reuse\n";
print $tlf "20071109010002 TESTCONF003 reuse\n";
print $tlf "20071109010002 TESTCONF002 reuse\n";
print $tlf "20071108010001 TESTCONF001 reuse\n";
close $tlf;
Amanda::Tapelist->new($tapelist);

# set up a number of logfiles in logdir.
my $logf;

# (an old log file that should be ignored)
open $logf, ">", "$logdir/log.20071106010002.0" or die("Could not write logfile");
print $logf "START taper datestamp 20071107010002 label TESTCONF017 tape 1\n";
close $logf;

# (a logfile with two tapes)
open $logf, ">", "$logdir/log.20071106010002.0" or die("Could not write logfile");
print $logf "START taper datestamp 20071106010002 label TESTCONF018 tape 1\n";
print $logf "START taper datestamp 20071106010002 label TESTCONF019 tape 2\n";
close $logf;

open $logf, ">", "$logdir/log.20071108010001.0" or die("Could not write logfile");
print $logf "START taper datestamp 20071108010001 label TESTCONF001 tape 1\n";
close $logf;

# a logfile with some detail, to run search_logfile against
open $logf, ">", "$logdir/log.20071109010002.0" or die("Could not write logfile");
print $logf <<EOF;
START taper datestamp 20071109010002 label TESTCONF002 tape 1
PART taper TESTCONF002 1 clihost /usr 20071109010002 1 0 [regular single part PART]
DONE taper clihost /usr 20071109010002 1 0 [regular single part DONE]
PART taper TESTCONF002 2 clihost "/my documents" 20071109010002 1 0 [diskname quoting]
DONE taper clihost "/my documents" 20071109010002 1 0 [diskname quoting]
PART taper TESTCONF002 3 thatbox /var 1 [regular 'old style' PART]
DONE taper thatbox /var 1 [regular 'old style' DONE]
PART taper TESTCONF002 4 clihost /home 20071109010002 1/5 0 [multi-part dump]
PART taper TESTCONF002 5 clihost /home 20071109010002 2/5 0 [multi-part dump]
PART taper TESTCONF002 6 clihost /home 20071109010002 3/5 0 [multi-part dump]
PART taper TESTCONF002 7 clihost /home 20071109010002 4/5 0 [multi-part dump]
PART taper TESTCONF002 8 clihost /home 20071109010002 5/5 0 [multi-part dump]
DONE taper clihost /home 20071109010002 5 0 [multi-part dump]
PART taper TESTCONF002 9 thatbox /u_lose 20071109010002 1/4 2 [multi-part failure]
PART taper TESTCONF002 10 thatbox /u_lose 20071109010002 2/4 2 [multi-part failure]
PARTPARTIAL taper TESTCONF002 11 thatbox /u_lose 20071109010002  3/4 2 [multi-part retry]
START taper datestamp 20071109010002 label TESTCONF003 tape 1
PART taper TESTCONF003 1 thatbox /u_lose 20071109010002 3/4 2 [multi-part failure]
FAIL taper thatbox /u_lose 20071109010002 2 error "Oh no!"
PART taper TESTCONF003 2 thatbox /u_win 20071109010002 1/4 3 [multi-part retry]
PART taper TESTCONF003 3 thatbox /u_win 20071109010002 2/4 3 [multi-part retry]
PARTPARTIAL taper TESTCONF003 4 thatbox /u_win 20071109010002  3/4 3 [multi-part retry]
START taper datestamp 20071109010002 label TESTCONF004 tape 1
PART taper TESTCONF004 1 thatbox /u_win 20071109010002 3/4 3 [multi-part retry]
PART taper TESTCONF004 2 thatbox /u_win 20071109010002 4/4 3 [multi-part retry]
DONE taper thatbox /u_win 20071109010002 4 3 [multi-part retry]
EOF
close $logf;

# "old-style amflush log"
open $logf, ">", "$logdir/log.20071110010002.amflush" or die("Could not write logfile");
print $logf "START taper datestamp 20071110010002 label TESTCONF005 tape 1\n";
close $logf;

# "old-style main log"
open $logf, ">", "$logdir/log.20071111010002" or die("Could not write logfile");
print $logf "START taper datestamp 20071111010002 label TESTCONF006 tape 1\n";
close $logf;

is_deeply([ Amanda::Logfile::find_log() ],
	  [ "log.20071111010002", "log.20071110010002.amflush",
	    "log.20071109010002.0", "log.20071108010001.0" ],
	  "find_log returns correct logfiles in the correct order");

my @results;
my @results_arr;

@results = Amanda::Logfile::search_logfile(undef, "20071109010002",
					   "$logdir/log.20071109010002.0", 1);
is($#results+1, 17, "search_logfile returned 17 results");

# sort by filenum so we can compare each to what it should be
@results = sort { $a->{'label'} cmp $b->{'label'} ||
		  $a->{'filenum'} <=> $b->{'filenum'} } @results;

# and convert the hashes to arrays for easy comparison
@results_arr = map { res2arr($_) } @results;
is_deeply(\@results_arr,
	[
	  [ '20071109010002', 'clihost', '/usr',	    0, 'TESTCONF002', 1,  'OK',       'OK',   '',         1, 1 ],
	  [ '20071109010002', 'clihost', '/my documents',   0, 'TESTCONF002', 2,  'OK',       'OK',   '',         1, 1 ],
	  [ '20071109010002', 'thatbox', '/var',	    1, 'TESTCONF002', 3,  'OK',       'OK',   '',         1, 1 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 4,  'OK',       'OK',   '',         1, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 5,  'OK',       'OK',   '',         2, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 6,  'OK',       'OK',   '',         3, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 7,  'OK',       'OK',   '',         4, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 8,  'OK',       'OK',   '',         5, 5 ],
	  [ '20071109010002', 'thatbox', '/u_lose',         2, 'TESTCONF002', 9,  'OK',       'FAIL', '"Oh no!"', 1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',         2, 'TESTCONF002', 10, 'OK',       'FAIL', '"Oh no!"', 2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',         2, 'TESTCONF002', 11, 'PARTIAL',  'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',         2, 'TESTCONF003', 1,  'OK',       'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',          3, 'TESTCONF003', 2,  'OK',       'OK',   '',         1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',          3, 'TESTCONF003', 3,  'OK',       'OK',   '',         2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',          3, 'TESTCONF003', 4,  'PARTIAL',  'OK',   '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',          3, 'TESTCONF004', 1,  'OK',       'OK',   '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',          3, 'TESTCONF004', 2,  'OK',       'OK',   '',         4, 4 ],
	], "results are correct");

my @filtered;
my @filtered_arr;

@filtered = Amanda::Logfile::dumps_match([@results], "thatbox", undef, undef, undef, 0);
is($#filtered+1, 10, "ten results match 'thatbox'");
@filtered = sort { $a->{'label'} cmp $b->{'label'} ||
		   $a->{'filenum'} <=> $b->{'filenum'} } @filtered;

@filtered_arr = map { res2arr($_) } @filtered;

is_deeply(\@filtered_arr,
	[
	  [ '20071109010002', 'thatbox', '/var',      1, 'TESTCONF002', 3,  'OK',      'OK',   '',         1, 1 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 9,  'OK',      'FAIL', '"Oh no!"', 1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 10, 'OK',      'FAIL', '"Oh no!"', 2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 11, 'PARTIAL', 'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF003', 1,  'OK',      'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 2,  'OK',      'OK',   '',         1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 3,  'OK',      'OK',   '',         2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 4,  'PARTIAL', 'OK',   '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF004', 1,  'OK',      'OK',   '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF004', 2,  'OK',      'OK',   '',         4, 4 ],
	], "results are correct");

@filtered = Amanda::Logfile::dumps_match([@results], "thatbox", "/var", undef, undef, 0);
is($#filtered+1, 1, "only one result matches 'thatbox:/var'");

@filtered = Amanda::Logfile::dumps_match([@results], undef, undef, "20071109010002", undef, 0);
is($#filtered+1, 17, "all 17 results match '20071109010002'");

@filtered = Amanda::Logfile::dumps_match([@results], undef, undef, "20071109010002", undef, 1);
is($#filtered+1, 12, "of those, 12 results are 'OK'");

@filtered = Amanda::Logfile::dumps_match([@results], undef, undef, undef, "2", 0);
is($#filtered+1, 4, "4 results are at level 2");

# test dumps_match_dumpspecs

my @dumpspecs;

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["thatbox", "/var"], 0);
@filtered = Amanda::Logfile::dumps_match_dumpspecs([@results], [@dumpspecs], 0);
is_deeply([ map { res2arr($_) } @filtered ],
	[
	  [ '20071109010002', 'thatbox', '/var',	    1, 'TESTCONF002', 3,  'OK', 'OK', '', 1, 1 ],
	], "filter with dumpspecs 'thatbox /var'");

@dumpspecs = Amanda::Cmdline::parse_dumpspecs(["thatbox", "/var", "clihost"], 0);
@filtered = Amanda::Logfile::dumps_match_dumpspecs([@results], [@dumpspecs], 0);
@filtered = sort { $a->{'label'} cmp $b->{'label'} ||
		   $a->{'filenum'} <=> $b->{'filenum'} } @filtered;
is_deeply([ map { res2arr($_) } @filtered ],
	[
	  [ '20071109010002', 'clihost', '/usr',	    0, 'TESTCONF002', 1,  'OK', 'OK', '', 1, 1 ],
	  [ '20071109010002', 'clihost', '/my documents',   0, 'TESTCONF002', 2,  'OK', 'OK', '', 1, 1 ],
	  [ '20071109010002', 'thatbox', '/var',	    1, 'TESTCONF002', 3,  'OK', 'OK', '', 1, 1 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 4,  'OK', 'OK', '', 1, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 5,  'OK', 'OK', '', 2, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 6,  'OK', 'OK', '', 3, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 7,  'OK', 'OK', '', 4, 5 ],
	  [ '20071109010002', 'clihost', '/home',	    0, 'TESTCONF002', 8,  'OK', 'OK', '', 5, 5 ],
	], "filter with dumpspecs 'thatbox /var clihost' (union of two disjoint sets)");

# if multiple dumpspecs specify the same dump, it will be included in the output multiple times
@dumpspecs = Amanda::Cmdline::parse_dumpspecs([".*", "/var", "thatbox"], 0);
@filtered = Amanda::Logfile::dumps_match_dumpspecs([@results], [@dumpspecs], 0);
@filtered = sort { $a->{'label'} cmp $b->{'label'} ||
		   $a->{'filenum'} <=> $b->{'filenum'} } @filtered;
is_deeply([ map { res2arr($_) } @filtered ],
	[
	  [ '20071109010002', 'thatbox', '/var',      1, 'TESTCONF002', 3,  'OK',      'OK'  , '',         1, 1 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 9,  'OK',      'FAIL', '"Oh no!"', 1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 10, 'OK',      'FAIL', '"Oh no!"', 2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 11, 'PARTIAL', 'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF003', 1,  'OK',      'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 2,  'OK',      'OK'  , '',         1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 3,  'OK',      'OK'  , '',         2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 4,  'PARTIAL', 'OK'  , '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF004', 1,  'OK',      'OK'  , '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF004', 2,  'OK',      'OK'  , '',         4, 4 ],
	], "filter with dumpspecs '.* /var thatbox' (union of two overlapping sets includes dupes)");

@dumpspecs = Amanda::Cmdline::dumpspec_t->new('thatbox', undef, undef, undef, '20071109010002');
@filtered = Amanda::Logfile::dumps_match_dumpspecs([@results], [@dumpspecs], 0);
@filtered = sort { $a->{'label'} cmp $b->{'label'} ||
		   $a->{'filenum'} <=> $b->{'filenum'} } @filtered;
is_deeply([ map { res2arr($_) } @filtered ],
	[
	  [ '20071109010002', 'thatbox', '/var',      1, 'TESTCONF002', 3,  'OK',      'OK'  , '',         1, 1 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 9,  'OK',      'FAIL', '"Oh no!"', 1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 10, 'OK',      'FAIL', '"Oh no!"', 2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF002', 11, 'PARTIAL', 'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_lose',   2, 'TESTCONF003', 1,  'OK',      'FAIL', '"Oh no!"', 3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 2,  'OK',      'OK'  , '',         1, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 3,  'OK',      'OK'  , '',         2, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF003', 4,  'PARTIAL', 'OK'  , '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF004', 1,  'OK',      'OK'  , '',         3, 4 ],
	  [ '20071109010002', 'thatbox', '/u_win',    3, 'TESTCONF004', 2,  'OK',      'OK'  , '',         4, 4 ],
	], "filter with dumpspecs with host 'thatbox' and a write_timestamp");
unlink($log_filename);

# search_holding_disk and match_* are tested via Amanda::DB::Catalog's installcheck
