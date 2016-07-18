# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 20;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::Run qw( run run_get );
use Installcheck::Catalogs;
use Amanda::Paths;
use Amanda::Constants;

my $cat;
my $testconf = Installcheck::Run::setup();
$testconf->write();

## try number formating

$cat = Installcheck::Catalogs::load('number');
$cat->install();

ok(run('amstatus', 'TESTCONF', '-odisplayunit=k'),
    "plain amstatus runs without error with -odisplayunit=k");
like($Installcheck::Run::stdout,
    qr{localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*155584k\s*flushed\s*\(11:41:56\)
localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*155584k\s*dump done, written\s*\(11:55:09\)
localhost.localdomain:1000g\s*[0-9]{14}\s*0\s*1048580000k\s*dump to tape done\s*\(11:42:07\)
localhost.localdomain:100g\s*[0-9]{14}\s*0\s*104858000k\s*dump to tape done\s*\(11:42:22\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*102410k\s*flushed\s*\(11:41:56\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*102410k\s*dump done, written\s*\(11:55:03\)
localhost.localdomain:100t\s*[0-9]{14}\s*0\s*104857700000k\s*dump to tape done\s*\(11:49:22\)
localhost.localdomain:10g\s*[0-9]{14}\s*0\s*10486016k\s*dump to tape done\s*\(11:54:59\)
localhost.localdomain:10m\s*[0-9]{14}\s*0\s*10250k\s*dump done, written\s*\(11:41:58\)
localhost.localdomain:10t\s*[0-9]{14}\s*0\s*10485770000k\s*dump to tape done\s*\(11:53:14\)
localhost.localdomain:1g\s*[0-9]{14}\s*0\s*1048755k\s*dump to tape done\s*\(11:42:54\)
localhost.localdomain:2ga\s*[0-9]{14}\s*0\s*2097160k\s*dump to tape done\s*\(11:43:35\)
localhost.localdomain:5ga\s*[0-9]{14}\s*0\s*5242890k\s*dump to tape done\s*\(11:45:36\)},
    "output is reasonable with -odisplayunit=k");

ok(run('amstatus', 'TESTCONF', '-odisplayunit=m'),
    "plain amstatus runs without error with -odisplayunit=m");
like($Installcheck::Run::stdout,
    qr{localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*151m\s*flushed\s*\(11:41:56\)
localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*151m\s*dump done, written\s*\(11:55:09\)
localhost.localdomain:1000g\s*[0-9]{14}\s*0\s*1024003m\s*dump to tape done\s*\(11:42:07\)
localhost.localdomain:100g\s*[0-9]{14}\s*0\s*102400m\s*dump to tape done\s*\(11:42:22\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*100m\s*flushed\s*\(11:41:56\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*100m\s*dump done, written\s*\(11:55:03\)
localhost.localdomain:100t\s*[0-9]{14}\s*0\s*102400097m\s*dump to tape done\s*\(11:49:22\)
localhost.localdomain:10g\s*[0-9]{14}\s*0\s*10240m\s*dump to tape done\s*\(11:54:59\)
localhost.localdomain:10m\s*[0-9]{14}\s*0\s*10m\s*dump done, written\s*\(11:41:58\)
localhost.localdomain:10t\s*[0-9]{14}\s*0\s*10240009m\s*dump to tape done\s*\(11:53:14\)
localhost.localdomain:1g\s*[0-9]{14}\s*0\s*1024m\s*dump to tape done\s*\(11:42:54\)
localhost.localdomain:2ga\s*[0-9]{14}\s*0\s*2048m\s*dump to tape done\s*\(11:43:35\)
localhost.localdomain:5ga\s*[0-9]{14}\s*0\s*5120m\s*dump to tape done\s*\(11:45:36\)},
    "output is reasonable with -odisplayunit=m");

ok(run('amstatus', 'TESTCONF', '-odisplayunit=g'),
    "plain amstatus runs without error with -odisplayunit=g");
like($Installcheck::Run::stdout,
    qr{localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*0g\s*flushed\s*\(11:41:56\)
localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*0g\s*dump done, written\s*\(11:55:09\)
localhost.localdomain:1000g\s*[0-9]{14}\s*0\s*1000g\s*dump to tape done\s*\(11:42:07\)
localhost.localdomain:100g\s*[0-9]{14}\s*0\s*100g\s*dump to tape done\s*\(11:42:22\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*0g\s*flushed\s*\(11:41:56\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*0g\s*dump done, written\s*\(11:55:03\)
localhost.localdomain:100t\s*[0-9]{14}\s*0\s*100000g\s*dump to tape done\s*\(11:49:22\)
localhost.localdomain:10g\s*[0-9]{14}\s*0\s*10g\s*dump to tape done\s*\(11:54:59\)
localhost.localdomain:10m\s*[0-9]{14}\s*0\s*0g\s*dump done, written\s*\(11:41:58\)
localhost.localdomain:10t\s*[0-9]{14}\s*0\s*10000g\s*dump to tape done\s*\(11:53:14\)
localhost.localdomain:1g\s*[0-9]{14}\s*0\s*1g\s*dump to tape done\s*\(11:42:54\)
localhost.localdomain:2ga\s*[0-9]{14}\s*0\s*2g\s*dump to tape done\s*\(11:43:35\)
localhost.localdomain:5ga\s*[0-9]{14}\s*0\s*5g\s*dump to tape done\s*\(11:45:36\)},
    "output is reasonable with -odisplayunit=g");

ok(run('amstatus', 'TESTCONF', '-odisplayunit=t'),
    "plain amstatus runs without error with -odisplayunit=t");
like($Installcheck::Run::stdout,
    qr{localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*0t\s*flushed\s*\(11:41:56\)
localhost.localdomain:/bootAMGTAR\s*[0-9]{14}\s*0\s*0t\s*dump done, written\s*\(11:55:09\)
localhost.localdomain:1000g\s*[0-9]{14}\s*0\s*0t\s*dump to tape done\s*\(11:42:07\)
localhost.localdomain:100g\s*[0-9]{14}\s*0\s*0t\s*dump to tape done\s*\(11:42:22\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*0t\s*flushed\s*\(11:41:56\)
localhost.localdomain:100m\s*[0-9]{14}\s*0\s*0t\s*dump done, written\s*\(11:55:03\)
localhost.localdomain:100t\s*[0-9]{14}\s*0\s*97t\s*dump to tape done\s*\(11:49:22\)
localhost.localdomain:10g\s*[0-9]{14}\s*0\s*0t\s*dump to tape done\s*\(11:54:59\)
localhost.localdomain:10m\s*[0-9]{14}\s*0\s*0t\s*dump done, written\s*\(11:41:58\)
localhost.localdomain:10t\s*[0-9]{14}\s*0\s*9t\s*dump to tape done\s*\(11:53:14\)
localhost.localdomain:1g\s*[0-9]{14}\s*0\s*0t\s*dump to tape done\s*\(11:42:54\)
localhost.localdomain:2ga\s*[0-9]{14}\s*0\s*0t\s*dump to tape done\s*\(11:43:35\)
localhost.localdomain:5ga\s*[0-9]{14}\s*0\s*0t\s*dump to tape done\s*\(11:45:36\)},
    "output is reasonable with -odisplayunit=t");

## try a few various options with a pretty normal logfile

$cat = Installcheck::Catalogs::load('normal');
$cat->install();

ok(run('amstatus', 'TESTCONF'),
    "plain amstatus runs without error");
like($Installcheck::Run::stdout,
    qr{clienthost:/some/dir [0-9]{14}\s*0\s*100k\s*dump done, written\s*\(13:01:53\)},
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
    qr{clienthost:"C:\\\\Some Dir\\\\" [0-9]{14}\s*0\s*100k\s*dump done, written\s*\(13:01:53\)},
    "output is correct");

## now test a chunker partial result

$cat = Installcheck::Catalogs::load('chunker-partial');
$cat->install();

ok(!run('amstatus', 'TESTCONF'),
    "amstatus return error with chunker partial");
is($Installcheck::Run::exit_code, 4,
    "correct exit code for chunker partial");
like($Installcheck::Run::stdout,
    qr{localhost:/etc [0-9]{14} 0\s*80917k dump failed: \[/usr/sbin/tar returned error\], written \( 7:49:53\)},
    "output is correct");

## now test a taper-parallel-write > 1

$cat = Installcheck::Catalogs::load('taper-parallel-write');
$cat->install();

ok(run('amstatus', 'TESTCONF'),
    "amstatus with taper-parallel-write runs without error");
like($Installcheck::Run::stdout,
    qr{\s*tape 3\s*:\s*1\s*142336k\s*142336k \(  5.82\%\) amstatus_test_3-AA-003 \(1 parts\)},
    "output is correct");
