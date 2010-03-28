# Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
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
use Installcheck::Dumpcache;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname amdump_diag);
use Amanda::Config qw( :init );
use Amanda::Paths;

my $vtape_root = "$Installcheck::TMP/tertiary";
sub setup_chg_disk {
    rmtree $vtape_root if -d $vtape_root;
    mkpath "$vtape_root/slot1";
    return "chg-disk:$vtape_root";
}

# set up a basic dump
Installcheck::Dumpcache::load("basic");

# and then set up a new vtape to vault onto
my $tertiary_chg = setup_chg_disk();

ok(run("$sbindir/amvault", 'TESTCONF', 'latest', $tertiary_chg, "TESTCONF%%"),
    "amvault runs!")
    or diag($Installcheck::Run::stderr);

my @tert_files = glob("$vtape_root/slot1/0*");
ok(@tert_files > 0,
    "..and files appear on the tertiary volume!");

rmtree $vtape_root;
Installcheck::Run::cleanup();
