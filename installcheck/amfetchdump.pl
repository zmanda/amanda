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

use Test::More tests => 7;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Run qw(run run_get run_err $diskname);
use Installcheck::Dumpcache;
use File::Path qw(rmtree mkpath);
use Amanda::Paths;
use Cwd;

my $testconf;
my $dumpok;

my $testdir = "$Installcheck::TMP/amfetchdump-installcheck";
rmtree($testdir);
mkpath($testdir);

my $origdir = getcwd;
chdir($testdir);

sub cleandir {
    for my $filename (<$testdir/*>) {
	unlink($filename);
    }
}

Installcheck::Dumpcache::load("basic");

like(run_err('amfetchdump', 'TESTCONF'),
    qr{^Usage:},
    "'amfetchdump TESTCONF' gives usage message on stderr");

SKIP: {
    skip "Expect.pm not installed", 2
	unless $Installcheck::Run::have_expect;

    cleandir();

    my $exp = Installcheck::Run::run_expect('amfetchdump', 'TESTCONF', 'localhost');
    $exp->log_stdout(0);

    my @results;
    $exp->expect(60,
	[ qr{1 tape\(s\) needed for restoration}, sub {
	    push @results, "tapes-needed";
	    exp_continue;
	} ],
	[ qr{amfetchdump: 1: restoring FILE: date [[:digit:]]+ host localhost disk .*},
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
    is_deeply([ @results ], [ "tapes-needed", "press-enter", "restoring", "eof" ],
	      "simple restore follows the correct steps");

    my @filenames = <localhost.*>;
    is(scalar @filenames, 1, "..and restored file is present in testdir")
	or diag(join("\n", @filenames));
}

{
    cleandir();

    ok(run('amfetchdump', '-a', 'TESTCONF', 'localhost'),
	"run with -a successful");

    my @filenames = <localhost.*>;
    is(scalar @filenames, 1, "..and restored file is present in testdir")
	or diag(join("\n", @filenames));
}

SKIP: {
    skip "Expect.pm not installed", 2
	unless $Installcheck::Run::have_expect;

    cleandir();
    chdir($Installcheck::TMP);

    my $exp = Installcheck::Run::run_expect('amfetchdump', '-O', $testdir, 'TESTCONF', 'localhost');
    $exp->log_stdout(0);

    my @results;
    $exp->expect(60,
	[ qr{1 tape\(s\) needed for restoration}, sub {
	    push @results, "tapes-needed";
	    exp_continue;
	} ],
	[ qr{amfetchdump: 1: restoring FILE: date [[:digit:]]+ host localhost disk .*},
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
    is_deeply([ @results ], [ "tapes-needed", "press-enter", "restoring", "eof" ],
	      "restore with -O follows the correct steps");

    chdir($testdir);
    my @filenames = <localhost.*>;
    is(scalar @filenames, 1, "..and restored file is present in testdir")
	or diag(join("\n", @filenames));
}

# TODO:
# - test piping (-p),
# - test compression (-c and -C)
# - test a specified device (-d)
# - test splits (regular, -w, -n)

END {
    chdir("$testdir/..");
    rmtree($testdir);
}
