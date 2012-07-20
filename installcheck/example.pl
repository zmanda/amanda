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

use Test::More tests => 2;

use lib "@amperldir@";
use File::Path;
use Installcheck;
use Installcheck::Config;
use Installcheck::Run;
use Amanda::Paths;
use Amanda::Util qw( slurp burp );
use Amanda::Config qw( :init );

# set up a basic TESTCONF, and then burp the example configs over amanda.conf
my $testconf = Installcheck::Config->new();
$testconf->write();
my $example_dir = "$amdatadir/example";
my $testconf_dir = "$CONFIG_DIR/TESTCONF";
my ($cfgerr_level, @cfgerr_errors);

burp("$testconf_dir/amanda.conf", slurp("$example_dir/amanda.conf"));
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
($cfgerr_level, @cfgerr_errors) = config_errors();
ok($cfgerr_level < $CFGERR_WARNINGS, "example/amanda.conf parses without warnings or errors") or
    config_print_errors();
config_uninit();

burp("$testconf_dir/amanda-client.conf", slurp("$example_dir/amanda-client.conf"));
config_init($CONFIG_INIT_EXPLICIT_NAME|$CONFIG_INIT_CLIENT, "TESTCONF");
($cfgerr_level, @cfgerr_errors) = config_errors();
ok($cfgerr_level < $CFGERR_WARNINGS, "example/amanda-client.conf parses without warnings or errors") or
    config_print_errors();
config_uninit();

Installcheck::Run::cleanup();
