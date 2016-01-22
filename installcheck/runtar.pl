# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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

use Test::More tests => 8;
use File::Path;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck::Run qw(run run_get run_err);
use Amanda::Paths;
use Amanda::Constants;

my $testconf;
my $runtar = "$amlibexecdir/runtar";
##
# First, try amgetconf out without a config

ok(!run($runtar),
    "runtar with no arguments returns an error exit status");
like($Installcheck::Run::stderr, qr/runtar: Need at least 3 arguments/i,
    ".. and gives error message on stderr");

ok(!run($runtar, 'NOCONFIG', $Amanda::Constants::GNUTAR, '--extract'),
    "runtar with --extract returns an error exit status");
like($Installcheck::Run::stderr, qr/runtar: Can only be used to create tar archives/i,
    ".. and gives error message on stderr");

ok(!run($runtar, 'NOCONFIG', $Amanda::Constants::GNUTAR, '--create', '--rsh-command=/toto'),
    "runtar with --rsh-command returns an error exit status");
like($Installcheck::Run::stderr, qr/runtar: error \[runtar invalid option: --rsh-command=\/toto\]/i,
    ".. and gives error message on stderr");

my $test_dir;
if (File::Temp->can('newdir')) {
    $test_dir = File::Temp->newdir('test_runtarXXXXXX',
                                   DIR      => '/tmp',
                                   CLEANUP  => 1);
} else {
    $test_dir = "/tmp/test_runtar_data_$$";
    rmtree $test_dir;
    mkdir $test_dir;
}

my $test_dir2;
if (File::Temp->can('newdir')) {
    $test_dir2 = File::Temp->newdir('test_runtarXXXXXX',
                                   DIR      => '/tmp',
                                   CLEANUP  => 1);
} else {
    $test_dir2 = "/tmp/test_runtar_setup_$$";
    rmtree $test_dir2;
    mkdir $test_dir2;
}
open AA, ">$test_dir2/listed-incremental";
close AA;
open AA, ">$test_dir2/files-from";
print AA ".\n";
close AA;
open AA, ">$test_dir2/exclude-from";
print AA "/foo\n";
close AA;

ok(run($runtar, 'NOCONFIG', $Amanda::Constants::GNUTAR,
	'--create',
	'--totals',
	'--dereference',
	'--no-recursion',
	'--one-file-system',
	'--incremental',
	'--atime-preserve',
	'--sparse',
	'--ignore-failed-read',
	'--numeric-owner',
	'--blocking-factor', '64',
	'--file', "$test_dir2/aa.tar",
	'--directory', $test_dir,
	'--exclude', "/toto",
	'--transform', "s/foo/bar/",
	'--listed-incremental', "$test_dir2/listed-incremental",
#	'--newer', "$test_dir2/files-from",
	'--exclude-from', "$test_dir2/exclude-from",
	'--files-from', "$test_dir2/files-from",
	),
   "runtar accept all option amanda use") || diag("$Installcheck::Run::stdout\n$Installcheck::Run::stderr");

ok(run($runtar, 'NOCONFIG', $Amanda::Constants::GNUTAR,
	'--create',
	'--totals',
	'--dereference',
	'--no-recursion',
	'--one-file-system',
	'--incremental',
	'--atime-preserve',
	'--sparse',
	'--ignore-failed-read',
	'--numeric-owner',
	'--blocking-factor', '64',
	'--file', "$test_dir2/aa.tar",
	'--directory', $test_dir,
	'--exclude', "/toto",
	'--transform', "s/foo/bar/",
#	'--listed-incremental', "$test_dir2/listed-incremental",
	'--newer', "$test_dir2/files-from",
	'--exclude-from', "$test_dir2/exclude-from",
	'--files-from', "$test_dir2/files-from",
	),
   "runtar accept all option amanda use") || diag("$Installcheck::Run::stdout\n$Installcheck::Run::stderr");

rmtree $test_dir;
rmtree $test_dir2;

Installcheck::Run::cleanup();
