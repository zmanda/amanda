# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 4;
use strict;
use warnings;

use lib '@amperldir@';
use File::Path;
use Data::Dumper;
use Installcheck;
use Installcheck::Config;
use Installcheck::Run qw(run run_err $diskname);
use Amanda::Paths;
use Amanda::Header;
use Amanda::Debug;
use Amanda::Config qw( :init );
use Amanda::DB::Catalog2;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $testconf;
my $vtape_dir;

# write a fake holding file to holding disk, for amflush to flush
sub write_holding_file {
    my ($host, $disk) = @_;

    my $datestamp = "20100102030405";
    my $filename = "$Installcheck::Run::holdingdir/$datestamp/$host-somefile";

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'datestamp'} = $datestamp;
    $hdr->{'dumplevel'} = 0;
    $hdr->{'name'} = $host;
    $hdr->{'disk'} = $disk;
    $hdr->{'program'} = "INSTALLCHECK";

    mkpath($Installcheck::Run::holdingdir);
    mkpath("$Installcheck::Run::holdingdir/$datestamp");
    open(my $fh, ">", $filename) or die("opening '$filename': $!");
    print $fh $hdr->to_string(32768,32768);
    print $fh "some data!\n";
    close($fh);
}

Installcheck::Run::cleanup();
$testconf = Installcheck::Run::setup();
$testconf->add_param("autolabel", '"TESTCONF%%" any');
$testconf->add_dle("localhost $diskname installcheck-test");
$testconf->write( do_catalog => 0 );

# add a holding file that's in the disklist
write_holding_file("localhost", $Installcheck::Run::diskname);
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my $catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

ok(run("$sbindir/amflush", '-f', '-b', 'TESTCONF'),
    "amflush runs successfully")
    or diag($Installcheck::Run::stderr);

# check that there's a vtape file where we expect to see one
$vtape_dir = Installcheck::Run::vtape_dir(1);
ok(<$vtape_dir/00001.*>, "..and dump appears on vtapes");

Installcheck::Run::cleanup();
$testconf = Installcheck::Run::setup();
$testconf->add_param("autolabel", '"TESTCONF%%" any');
# don't add anything to the disklist; it should still flush it
$testconf->write( do_catalog => 0 );

# add a holding file that's not in the disklist
write_holding_file("localhost", $Installcheck::Run::diskname);
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
$catalog = Amanda::DB::Catalog2->new(undef, create => 1, drop_tables => 1, load => 1);
$catalog->quit();

ok(run("$sbindir/amflush", '-f', '-b', 'TESTCONF'),
    "amflush runs successfully")
    or diag($Installcheck::Run::stderr);

# check that there's a vtape file where we expect to see one
$vtape_dir = Installcheck::Run::vtape_dir(1);
ok(<$vtape_dir/00001.*>, "..and dump appears on vtapes");

#Installcheck::Run::cleanup();
