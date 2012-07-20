# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S Mathilda Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 18;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Installcheck::Run qw(run run_err run_get load_vtape vtape_dir);
use Amanda::Device qw( :constants );
use Amanda::Config qw( :init :getconf );
use Amanda::Paths;
use Amanda::Debug;
use Amanda::Constants;
use Amanda::Tapelist;

my $testconf;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

$testconf = Installcheck::Run::setup();
$testconf->add_dle("localhost \"\\\\\\\\windows\\\\share\" installcheck-test");
$testconf->add_dle("localhost \"\\\\\\\\windows\\\\share-a\" installcheck-test");
$testconf->add_dle("localhost \"\\\\\\\\windows\\\\share-b\" installcheck-test");
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    BAIL_OUT("config errors");
}

#Check ARGV argument, extra quoting because of perl.
like(run_get('amadmin', 'TESTCONF', 'force', 'localhost', '\\\\\\\\\\\\\\\windows\\\\\\\\share'),
    qr/Argument '\\\\\\\\\\\\\\\\windows\\\\\\\\share' matches neither a host nor a disk; quoting may not be correct./,
    "argv 1");

like(run_get('amadmin', 'TESTCONF', 'force', 'localhost', '\\\\\\\\windows\\\\share'),
    qr/amadmin: localhost:\\\\windows\\share is set to a forced level 0 at next run./,
    "argv 2");

like(run_get('amadmin', 'TESTCONF', 'force', 'localhost', '\\\\windows\\share'),
    qr/Argument '\\\\windows\\share' matches neither a host nor a disk; quoting may not be correct./,
    "argv 3");

like(run_get('amadmin', 'TESTCONF', 'force', 'localhost', '\\windows\share'),
    qr/Argument '\\windows\\share' matches neither a host nor a disk; quoting may not be correct./,
    "argv 4");

#Check sheel quoting, extra quoting because of perl
like(run_get('amadmin TESTCONF force localhost \\\\\\\\\\\\\\\\windows\\\\\\\\share'),
   qr/amadmin: localhost:\\\\windows\\share is set to a forced level 0 at next run./,
   "shell 1");

like(run_get('amadmin TESTCONF force localhost "\\\\\\\\\\\\\\\\windows\\\\\\\\share"'),
   qr/amadmin: localhost:\\\\windows\\share is set to a forced level 0 at next run./,
   "shell 2");

like(run_get('amadmin TESTCONF force localhost \'\\\\\\\\\\\\\\\\windows\\\\\\\\share\''),
   qr/\\\\\\\\\\\\\\\windows\\\\\\\\share' matches neither a host nor a disk; quoting may not be correct./,
   "shell 3");

like(run_get('amadmin TESTCONF force localhost \\\\\\\\windows\\\\share'),
   qr/Argument '\\\\windows\\share' matches neither a host nor a disk; quoting may not be correct./,
   "shell 4");

like(run_get('amadmin TESTCONF force localhost "\\\\\\\\windows\\\\share"'),
   qr/\\\\windows\\share' matches neither a host nor a disk; quoting may not be correct./,
   "shell 5");

like(run_get('amadmin TESTCONF force localhost \'\\\\\\\\windows\\\\share\''),
   qr/amadmin: localhost:\\\\windows\\share is set to a forced level 0 at next run./,
   "shell 6");

like(run_get('amadmin TESTCONF force localhost \\\\windows\\share'),
   qr/Argument '\\windowsshare' matches neither a host nor a disk; quoting may not be correct./,
   "shell 7");

like(run_get('amadmin TESTCONF force localhost "\\\\windows\\share"'),
   qr/Argument '\\windows\\share' matches neither a host nor a disk; quoting may not be correct./,
   "shell 8");

like(run_get('amadmin TESTCONF force localhost \'\\\\windows\\share\''),
   qr/Argument '\\\\windows\\share' matches neither a host nor a disk; quoting may not be correct./,
   "shell 9");

like(run_get('amadmin TESTCONF force localhost share'),
   qr/amadmin: localhost:\\\\windows\\share is set to a forced level 0 at next run./,
   "shell 10");

like(run_get('amadmin TESTCONF force localhost windows'),
   qr/amadmin: localhost:\\\\windows\\share is set to a forced level 0 at next run.
amadmin: localhost:\\\\windows\\share-a is set to a forced level 0 at next run.
amadmin: localhost:\\\\windows\\share-b is set to a forced level 0 at next run.$/,
   "shell 11");

like(run_get('amadmin TESTCONF force localhost share-\*'),
   qr/amadmin: localhost:\\\\windows\\share-a is set to a forced level 0 at next run.
amadmin: localhost:\\\\windows\\share-b is set to a forced level 0 at next run.$/,
   "shell 12");

like(run_get('amadmin TESTCONF force localhost share-a share-a'),
   qr/^amadmin: localhost:\\\\windows\\share-a is set to a forced level 0 at next run.$/,
   "shell 13");

like(run_get('amadmin TESTCONF balance --days 12'),
   qr/No data to report on yet.$/,
   "shell 13");
