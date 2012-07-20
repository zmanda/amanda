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

use Test::More tests => 33;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Run qw(run run_get run_err $diskname);
use Installcheck::Dumpcache;
use File::Path qw(rmtree mkpath);
use Amanda::Paths;
use Amanda::Header;
use Amanda::Debug;
use Cwd;
use warnings;
use strict;
no strict 'subs';

unless ($Installcheck::Run::have_expect) {
    SKIP: {
        skip("Expect.pm not available", Test::More->builder->expected_tests);
    }
    exit 0;
}

## NOTE:
#
# Not all features of amfetchdump can be tested without a lot of extra work:
# --header-fd: Expect doesn't pass through nonstandard fd's
# -p: Expect would have to deal with the dumpfile data, which it won't like

my $testconf;
my $dumpok;
my @filenames;
my $exp;
my @results;
my $fok;
my $last_file_size;

my $testdir = "$Installcheck::TMP/amfetchdump-installcheck/files";
rmtree($testdir);
mkpath($testdir);

my $origdir = getcwd;
chdir($testdir);

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

sub cleandir {
    for my $filename (<$testdir/*>) {
	unlink($filename);
    }
}

sub got_files {
    my ($count, $msg) = @_;
    my $ok = 1;

    my @filenames = <localhost.*>;

    # check for .tmp files and empty files
    for my $fn (@filenames) {
	if ($fn =~ /\.tmp$/ || -z "$testdir/$fn") {
	    $ok = 0;
	}
    }

    if (scalar @filenames != $count) {
	diag("expected $count files");
	$ok = 0;
    }

    # capture file size if there's only one file
    if (@filenames == 1) {
	$last_file_size = -s $filenames[0];
    }

    ok($ok, $msg) or diag(`ls -l $testdir`);
}

Installcheck::Dumpcache::load("basic");

like(run_err('amfetchdump', 'TESTCONF'),
    qr{^Usage:},
    "'amfetchdump TESTCONF' gives usage message on stderr");

like(run_err('amfetchdump', '-b', '65536', 'TESTCONF', 'localhost'),
    qr{ERROR: The -b option is no longer},
    "-b option gives a warning stderr");

##
# plain vanilla

cleandir();

$exp = Installcheck::Run::run_expect('amfetchdump', 'TESTCONF', 'localhost');
$exp->log_stdout(0);

@results = ();
$exp->expect(60,
    [ qr{1 (tape|volume)\(s\) needed for restoration}, sub {
	push @results, "tape-count";
	exp_continue;
    } ],
    [ qr{The following (tapes|volumes) are needed: TESTCONF01}, sub {
	push @results, "tapes-needed";
	exp_continue;
    } ],
    [ qr{amfetchdump: 1: restoring split dumpfile: date [[:digit:]]+ host localhost disk .*},
    sub {
	push @results, "restoring";
	exp_continue;
    } ],
    [ 'Press enter when ready', sub {
	push @results, "press-enter";
	$exp->send("\n");
	exp_continue;
    }, ],
    [ 'eof', sub {
	push @results, "eof";
    }, ],
);
is_deeply([ @results ], [ "tape-count", "tapes-needed", "press-enter", "restoring", "eof" ],
	  "simple restore follows the correct steps");

got_files(1, "..and restored file is present in testdir");

##
# -a (assume)

cleandir();

ok(run('amfetchdump', '-a', '-l', 'TESTCONF', 'localhost'),
    "run with -a and -l successful");

got_files(1, "..and restored file is present in testdir ($last_file_size bytes)");
my $uncomp_size = $last_file_size;

##
# -C (should make output file smaller)

cleandir();

ok(run('amfetchdump', '-a', '-C', 'TESTCONF', 'localhost'),
    "run with -a and -C successful");

got_files(1, "..and restored file is present in testdir");

ok($last_file_size < $uncomp_size,
    "..and is smaller than previous run ($last_file_size bytes)");

##
# -O

cleandir();
chdir($Installcheck::TMP);

$exp = Installcheck::Run::run_expect('amfetchdump', '-O', $testdir, 'TESTCONF', 'localhost');
$exp->log_stdout(0);

@results = ();
$exp->expect(60,
    [ qr{1 (tape|volume)\(s\) needed for restoration}, sub {
	push @results, "tape-count";
	exp_continue;
    } ],
    [ qr{The following (tapes|volumes) are needed: TESTCONF01}, sub {
	push @results, "tapes-needed";
	exp_continue;
    } ],
    [ qr{amfetchdump: 1: restoring split dumpfile: date [[:digit:]]+ host localhost disk .*},
    sub {
	push @results, "restoring";
	exp_continue;
    } ],
    [ 'Press enter when ready', sub {
	push @results, "press-enter";
	$exp->send("\n");
	exp_continue;
    }, ],
    [ 'eof', sub {
	push @results, "eof";
    }, ],
);
is_deeply([ @results ], [ "tape-count", "tapes-needed", "press-enter", "restoring", "eof" ],
	  "restore with -O follows the correct steps");

chdir($testdir);
got_files(1, "..and restored file is present in testdir");

##
# -h

cleandir();

$exp = Installcheck::Run::run_expect('amfetchdump', '-h', 'TESTCONF', 'localhost');
$exp->log_stdout(0);

@results = ();
$exp->expect(60,
    [ qr{1 (tape|volume)\(s\) needed for restoration}, sub {
	push @results, "tape-count";
	exp_continue;
    } ],
    [ qr{The following (tapes|volumes) are needed: TESTCONF01}, sub {
	push @results, "tapes-needed";
	exp_continue;
    } ],
    [ qr{amfetchdump: 1: restoring split dumpfile: date [[:digit:]]+ host localhost disk .*},
    sub {
	push @results, "restoring";
	exp_continue;
    } ],
    [ 'Press enter when ready', sub {
	push @results, "press-enter";
	$exp->send("\n");
	exp_continue;
    }, ],
    [ 'eof', sub {
	push @results, "eof";
    }, ],
);
is_deeply([ @results ], [ "tape-count", "tapes-needed", "press-enter", "restoring", "eof" ],
	  "restore with -h follows the correct steps");

$fok = got_files(1, "..and restored file is present in testdir");

# check that it starts with a header
if ($fok) {
    my @filenames = <localhost.*>;
    open(my $fh, "<", $filenames[0]) or die "error opening: $!";
    sysread($fh, my $hdr_dat, 32768) or die "error reading: $!";
    close($fh);
    my $hdr = Amanda::Header->from_string($hdr_dat);
    is($hdr->{type}+0, $Amanda::Header::F_SPLIT_DUMPFILE,
	"..dumpfile begins with a split dumpfile header");
} else {
    fail();
}

##
# --header-file

cleandir();

$exp = Installcheck::Run::run_expect('amfetchdump', '--header-file', 'hdr',
					'TESTCONF', 'localhost');
$exp->log_stdout(0);

@results = ();
$exp->expect(60,
    [ qr{1 (tape|volume)\(s\) needed for restoration}, sub {
	push @results, "tape-count";
	exp_continue;
    } ],
    [ qr{The following (tapes|volumes) are needed: TESTCONF01}, sub {
	push @results, "tapes-needed";
	exp_continue;
    } ],
    [ qr{amfetchdump: 1: restoring split dumpfile: date [[:digit:]]+ host localhost disk .*},
    sub {
	push @results, "restoring";
	exp_continue;
    } ],
    [ 'Press enter when ready', sub {
	push @results, "press-enter";
	$exp->send("\n");
	exp_continue;
    }, ],
    [ 'eof', sub {
	push @results, "eof";
    }, ],
);
is_deeply([ @results ], [ "tape-count", "tapes-needed", "press-enter", "restoring", "eof" ],
	  "restore with --header-file follows the correct steps");

$fok = got_files(1, "..and restored file is present in testdir");

# check that it starts with a header
if ($fok) {
    my @filenames = <localhost.*>;
    open(my $fh, "<", "$testdir/hdr") or die "error opening: $!";
    sysread($fh, my $hdr_dat, 32768) or die "error reading: $!";
    close($fh);
    my $hdr = Amanda::Header->from_string($hdr_dat);
    is($hdr->{type}+0, $Amanda::Header::F_SPLIT_DUMPFILE,
	"..and the header file contains the right header");
} else {
    fail();
}

##
# -d and prompting for volumes one at a time

cleandir();

my $vfsdev = 'file:' . Installcheck::Run::vtape_dir();
Installcheck::Run::load_vtape(3); # wrong vtape
$exp = Installcheck::Run::run_expect('amfetchdump', '-d', $vfsdev,
					'TESTCONF', 'localhost');
$exp->log_stdout(0);

@results = ();
$exp->expect(60,
    [ qr{1 (tape|volume)\(s\) needed for restoration}, sub {
	push @results, "tape-count";
	exp_continue;
    } ],
    [ qr{The following (tapes|volumes) are needed: TESTCONF01}, sub {
	push @results, "tapes-needed";
	exp_continue;
    } ],
    [ 'Press enter when ready', sub {
	push @results, "press-enter";
	$exp->send("\n");
	exp_continue;
    }, ],
    [ qr{Insert (tape|volume) labeled '?TESTCONF01'? in .*\n.*to abort}, sub {
	push @results, "insert-tape";
	Installcheck::Run::load_vtape(1); # right vtape
	$exp->send("\n");
	exp_continue;
    }, ],
    [ qr{amfetchdump: 1: restoring split dumpfile: date [[:digit:]]+ host localhost disk .*},
    sub {
	push @results, "restoring";
	exp_continue;
    } ],
    [ 'eof', sub {
	push @results, "eof";
    }, ],
);
is_deeply([ @results ], [ "tape-count", "tapes-needed", "press-enter",
			  "insert-tape", "restoring", "eof" ],
	  "restore with an explicit device follows the correct steps, prompting for each");

got_files(1, "..and restored file is present in testdir");

##
# -n (using a multipart dump)

Installcheck::Dumpcache::load("parts");
cleandir();

$exp = Installcheck::Run::run_expect('amfetchdump', '-n', 'TESTCONF', 'localhost');
$exp->log_stdout(0);

@results = ();
$exp->expect(60,
    [ qr{1 (tape|volume)\(s\) needed for restoration}, sub {
	push @results, "tape-count";
	exp_continue;
    } ],
    [ qr{The following (tapes|volumes) are needed: TESTCONF01}, sub {
	push @results, "tapes-needed";
	exp_continue;
    } ],
    [ qr{amfetchdump: (\d+): restoring split dumpfile: date [[:digit:]]+ host localhost disk .*},
    sub {
	push @results, "restoring";
	exp_continue;
    } ],
    [ 'Press enter when ready', sub {
	push @results, "press-enter";
	$exp->send("\n");
	exp_continue;
    }, ],
    [ 'eof', sub {
	push @results, "eof";
    }, ],
);
is_deeply([ @results ], [ "tape-count", "tapes-needed", "press-enter",
			  ("restoring",)x9, "eof" ],
	  "restore with -n follows the correct steps");

got_files(9, "..and restored file is present in testdir");

##
# -l, no options, and -c for compressed dumps

Installcheck::Dumpcache::load("compress");
cleandir();

ok(run('amfetchdump', '-a', 'TESTCONF', 'localhost'),
    "run with -a successful (should uncompress)");

got_files(1, "..and restored file is present in testdir ($last_file_size bytes)");
$uncomp_size = $last_file_size;

cleandir();

ok(run('amfetchdump', '-a', '-l', 'TESTCONF', 'localhost'),
    "run with -a and -l successful (should not uncompress)");

got_files(1, "..and restored file is present in testdir");

ok($last_file_size < $uncomp_size,
    "..and is smaller than previous run ($last_file_size bytes)");

cleandir();

ok(run('amfetchdump', '-a', '-c', 'TESTCONF', 'localhost'),
    "run with -a and -c successful (should not uncompress)");

got_files(1, "..and restored file is present in testdir");

ok($last_file_size < $uncomp_size,
    "..and is smaller than previous run ($last_file_size bytes)");

Installcheck::Dumpcache::load("multi");
cleandir();

$exp = Installcheck::Run::run_expect('amfetchdump', 'TESTCONF', 'localhost');
$exp->log_stdout(0);

@results = ();
$exp->expect(60,
    [ qr{2 (tape|volume)\(s\) needed for restoration}, sub {
	push @results, "tape-count";
	exp_continue;
    } ],
    [ qr{The following (tapes|volumes) are needed: TESTCONF01 TESTCONF02.*}, sub {
	push @results, "tapes-needed";
	exp_continue;
    } ],
    [ qr{2 holding file\(s\) needed for restoration}, sub {
	push @results, "holding-count";
	exp_continue;
    } ],
    [ qr{Reading .*\nFILE: date [[:digit:]]+ host localhost disk .*},
    sub {
	push @results, "reading";
	exp_continue;
    } ],
    [ 'Press enter when ready', sub {
	push @results, "press-enter";
	$exp->send("\n");
	exp_continue;
    }, ],
    [ 'eof', sub {
	push @results, "eof";
    }, ],
);
is_deeply([ @results ], [ "tape-count", "tapes-needed", "holding-count",
			  "press-enter", "reading", "reading", "eof" ],
	  "restore from holding follows the correct steps");

got_files(6, "..and all restored files are present in testdir");


SKIP: {
    skip "Expect not installed or not built with ndmp and server", 2 unless
	Amanda::Util::built_with_component("ndmp") and
	Amanda::Util::built_with_component("server") and
	$Installcheck::Run::have_expect;


    Installcheck::Dumpcache::load("ndmp");
    my $ndmp = Installcheck::Mock::NdmpServer->new(no_reset => 1);
    $ndmp->edit_config();

    cleandir();

    $exp = Installcheck::Run::run_expect('amfetchdump', 'TESTCONF', 'localhost');
    $exp->log_stdout(0);

    @results = ();
    $exp->expect(60,
	[ qr{1 (tape|volume)\(s\) needed for restoration}, sub {
	    push @results, "tape-count";
	    exp_continue;
	} ],
	[ qr{The following (tapes|volumes) are needed: TESTCONF01}, sub {
	    push @results, "tapes-needed";
	    exp_continue;
	} ],
	[ qr{amfetchdump: 1: restoring split dumpfile: date [[:digit:]]+ host localhost disk .*},
	sub {
	    push @results, "restoring";
	    exp_continue;
	} ],
	[ 'Press enter when ready', sub {
	    push @results, "press-enter";
	    $exp->send("\n");
	    exp_continue;
	}, ],
	[ 'eof', sub {
	    push @results, "eof";
	}, ],
    );
    is_deeply([ @results ], [ "tape-count", "tapes-needed", "press-enter", "restoring", "eof" ],
	      "ndmp restore follows the correct steps");

    got_files(1, "..and restored file is present in testdir");
}

chdir("$testdir/..");
rmtree($testdir);
