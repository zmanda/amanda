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

use Test::More tests => 3;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Installcheck::Run qw(run run_get run_err $diskname);
use Installcheck::Dumpcache;

use File::Path qw(rmtree mkpath);
use Data::Dumper;
use Sys::Hostname;

use Amanda::Paths;
use Amanda::Header;
use Amanda::Debug;
use warnings;
use strict;
no strict 'subs';

if (!$Installcheck::Run::have_expect) {
    SKIP: {
        skip("Expect.pm not available",
	    Test::More->builder->expected_tests);
    }
    exit 0;
}

# amrecover can successfully set the host if its hostname is localhost
my $hostname = hostname;
my $set_host_succeed = 0;
$set_host_succeed=1 if (   ($hostname eq "localhost")
		        or ($hostname =~ /localhost\./));

my $debug = !exists $ENV{'HARNESS_ACTIVE'};
diag("logging amrecover conversations to stdout because Test::Harness not in use")
    if $debug;

my @results;
my $testdir = "$Installcheck::TMP/amfetchdump-installcheck/files";
rmtree($testdir);
mkpath($testdir);
chdir($testdir);

sub cleandir {
    for my $filename (<$testdir/*>) {
	unlink($filename);
    }
}

sub mkcont($) {
    my ($msg) = @_;
    sub {
	push @results, $msg;
	exp_continue;
    };
}

sub run_amrecover {
    my %params = @_;
    my ($exp, $continued);
    my @commands = @{$params{'commands'}};

    cleandir();
    my @h_opt = ('-h', 'localhost') unless $set_host_succeed;
    $exp = Installcheck::Run::run_expect('amrecover', 'TESTCONF',
	@h_opt, '-s', 'localhost', '-t', 'localhost', '-o', 'auth=local');
    $exp->log_stdout($debug);

    @results = ();
    $exp->expect(60,
	[ qr{220.*ready\.}, mkcont "server-ready" ],
	[ qr{200 Config set to TESTCONF\.}, mkcont "config-set" ],
	[ qr{Use the sethost command}, mkcont "use-sethost" ],
	[ qr{syntax error}, mkcont "syntax-err" ],
	[ qr{Invalid command:}, mkcont "invalid-cmd" ],
	[ qr{200 Dump host set}, mkcont "host-set" ],
	[ qr{200 Disk set to}, mkcont "disk-set" ],
	[ qr{Added file /1kilobyte}, mkcont "added-file" ],
	[ qr{The following tapes are needed:}, mkcont "tapes-needed" ],
	[ qr{Load tape \S+ now}, mkcont "load-tape" ],
	[ qr{Restoring files into directory}, mkcont "restoring-into" ],
	[ qr{All existing files.*can be deleted}, mkcont "can-delete" ],
	[ qr{\./1kilobyte}, mkcont "restored-file" ],
	[ qr{200 Good bye}, mkcont "bye" ],

	[ qr{amrecover> }, sub {
	    my $cmd = shift @commands or die "out of commands!";
	    push @results, "> $cmd";
	    $exp->send("$cmd\n");
	    exp_continue;
	} ],
	[ qr{Continue \[\?/Y/n/s/d\]\?}, sub {
	    die "multiple Continue requests" if $continued;
	    $continued = 1;

	    push @results, "> continue-Y";
	    $exp->send("Y\n");
	    exp_continue;
	} ],
	[ qr{Continue \[\?/Y/n\]\?}, sub { # the "all files" question
	    push @results, "> deletall-Y";
	    $exp->send("Y\n");
	    exp_continue;
	} ],
    );
    return @results;
}

## plain vanilla amrecover run

Installcheck::Dumpcache::load("basic");

run_amrecover(
	commands => [
	    "sethost localhost",
	    "setdisk $diskname",
	    "add 1kilobyte",
	    "extract",
	    "quit",
	]);

is_deeply([ @results ], [
	'server-ready', 'config-set',
	'host-set',
	'> sethost localhost',
	'host-set',
	"> setdisk $diskname",
	'disk-set',
	'> add 1kilobyte',
	'added-file',
	'> extract',
	'tapes-needed', 'load-tape',
	'> continue-Y',
	'restoring-into', 'can-delete',
	'> deletall-Y',
	'restored-file',
	'> quit',
	'bye'
    ],
    "simple restore follows the correct steps")
    or diag Dumper([@results]);

ok((-f "1kilobyte" and ! -z "1kilobyte"),
    "..restored file appears in current directory");

## parser check

run_amrecover(
	commands => [
	    "sethost localhost ", # <-- note extra space
	    "sethost localhost",
	    "quit",
	]);

is_deeply([ @results ], [
	'server-ready', 'config-set',
	'host-set',
	'> sethost localhost ',
	'host-set',
	'> sethost localhost',
	'host-set',
	'> quit',
	'bye'
    ],
    "check trailling space")
    or die Dumper([@results]);

## cleanup

chdir("/");
rmtree $testdir;
Installcheck::Run::cleanup();
