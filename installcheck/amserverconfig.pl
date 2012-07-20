# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 10;

use lib "@amperldir@";
use File::Path;
use Installcheck;
use Installcheck::Run qw(run run_err $diskname);
use Amanda::Paths;
use Amanda::Constants;
use Amanda::Util qw( slurp burp );
use Amanda::Config qw( :init );

# this basically gets one run of amserverconfig in for each template, and then
# checks that the config loads correctly

sub config_ok {
    my ($msg) = @_;

    config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");

    my ($cfgerr_level, @cfgerr_errors) = config_errors();
    ok($cfgerr_level < $CFGERR_WARNINGS, $msg || "..config is OK") or
	config_print_errors();

    config_uninit();
}

Installcheck::Run::cleanup();
ok(run("$sbindir/amserverconfig", 'TESTCONF', '--template', 'S3'),
    "amserverconfig with S3 template")
    or diag($Installcheck::Run::stdout);
config_ok();

Installcheck::Run::cleanup();
ok(run("$sbindir/amserverconfig", 'TESTCONF', '--template', 'harddisk'),
    "amserverconfig with harddisk template")
    or diag($Installcheck::Run::stdout);
config_ok();

Installcheck::Run::cleanup();
mkpath(Installcheck::Run::vtape_dir());
ok(run("$sbindir/amserverconfig", 'TESTCONF', '--template', 'harddisk',
		    '--tapecycle', '2',
		    '--tapedev', Installcheck::Run::vtape_dir()),
    "amserverconfig with harddisk template and tapedev and tapecycle")
    or diag($Installcheck::Run::stdout);
config_ok();

Installcheck::Run::cleanup();
ok(run("$sbindir/amserverconfig", 'TESTCONF', '--template', 'single-tape'),
    "amserverconfig with single-tape template")
    or diag($Installcheck::Run::stdout);
config_ok();

SKIP: {
    skip "tape-changer template requires mtx", 2
	unless $Amanda::Constants::MTX && -x $Amanda::Constants::MTX;
    Installcheck::Run::cleanup();
    ok(run("$sbindir/amserverconfig", 'TESTCONF', '--template', 'tape-changer'),
	"amserverconfig with tape-changer template")
	or diag($Installcheck::Run::stdout);
    config_ok();
}

Installcheck::Run::cleanup();
