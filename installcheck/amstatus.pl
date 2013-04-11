# Copyright (c) 2008-2013 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 12;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Run qw( run run_get );
use Installcheck::Catalogs;
use Amanda::Paths;
use Amanda::Constants;

my $cat;
my $testconf = Installcheck::Run::setup();
$testconf->write();

## try a few various options with a pretty normal logfile

$cat = Installcheck::Catalogs::load('normal');
$cat->install();

ok(run('amstatus', 'TESTCONF'),
    "plain amstatus runs without error");
like($Installcheck::Run::stdout,
    qr{clienthost:/some/dir\s*0\s*100k\s*finished\s*\(13:01:53\)},
    "output is reasonable");

ok(run('amstatus', 'TESTCONF', '--summary'),
    "amstatus --summary runs without error");
unlike($Installcheck::Run::stdout,
    qr{clienthost:/some/dir\s*0\s*100k\s*finished\s*\(13:01:53\)},
    "output does not contain the finished dump");
like($Installcheck::Run::stdout,
    qr{taped\s+:\s+1\s+},
    "output contains summary info");

## now test a file with spaces and other funny characters in filenames

$cat = Installcheck::Catalogs::load('quoted');
$cat->install();

ok(run('amstatus', 'TESTCONF'),
    "amstatus runs without error with quoted disknames");
like($Installcheck::Run::stdout,
    # note that amstatus' output is quoted, so backslashes are doubled
    qr{clienthost:"C:\\\\Some Dir\\\\"\s*0\s*100k\s*finished\s*\(13:01:53\)},
    "output is correct");

## now test a chunker partial result

$cat = Installcheck::Catalogs::load('chunker-partial');
$cat->install();

ok(!run('amstatus', 'TESTCONF'),
    "amstatus return error with chunker partial");
is($Installcheck::Run::exit_code, 4,
    "correct exit code for chunker partial");
like($Installcheck::Run::stdout,
    qr{localhost:/etc 0\s*80917k dump failed: dumper: \[/usr/sbin/tar returned error\], finished \(7:49:53\)},
    "output is correct");

## now test a taper-parallel-write > 1

$cat = Installcheck::Catalogs::load('taper-parallel-write');
$cat->install();

ok(run('amstatus', 'TESTCONF'),
    "amstatus with taper-parallel-write runs without error");
like($Installcheck::Run::stdout,
    qr{\s*tape 3\s*:\s*1\s*142336k\s*142336k \(  5.82\%\) amstatus_test_3-AA-003 \(1 chunks\)},
    "output is correct");
