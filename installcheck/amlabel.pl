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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 24;
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

$testconf = Installcheck::Run::setup(1, 4);
$testconf->add_param('autolabel', '"TESTCONF%%" any');
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    BAIL_OUT("config errors");
}

# label slot 2 with "MyTape", slot 3 with "TESTCONF13", and add
# the latter to the tapelist
my ($devdir, $dev);

$devdir = load_vtape(2);
$dev = Amanda::Device->new("file:$devdir");
($dev && $dev->status == $DEVICE_STATUS_SUCCESS)
    or BAIL_OUT("device error");

$dev->start($ACCESS_WRITE, "MyTape", undef)
    or BAIL_OUT("device error");
$dev->finish()
    or BAIL_OUT("device error");

my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
my $tl = Amanda::Tapelist->new($tlf, 1);
$tl->add_tapelabel("0", "TESTCONF13", "test tape");
$tl->write($tlf);

like(run_err('amlabel'),
    qr/^Usage:/,
    "bare 'amlabel' gives usage message");

like(run_get('amlabel', '--version'),
    qr/^amlabel-\Q$Amanda::Constants::VERSION\E/,
    "'amlabel --version' gives version");

like(run_get('amlabel', 'TESTCONF', 'TESTCONF92'),
    qr/Writing label 'TESTCONF92'/,
    "amlabel labels the current slot by default");

$tl->reload();
is_deeply($tl->{'tles'}->[0], {
       'reuse' => 1,
       'barcode' => undef,
       'meta' => undef,
       'blocksize' => '32',
       'comment' => undef,
       'position' => 1,
       'label' => 'TESTCONF92',
       'datestamp' => '0'
     },
    "tapelist correctly updated");

$devdir = load_vtape(1);
$dev = Amanda::Device->new("file:$devdir");
die "read_label failed" unless $dev->read_label() == $DEVICE_STATUS_SUCCESS;
is($dev->volume_label, "TESTCONF92", "volume is actually labeled");

ok(!run('amlabel', 'TESTCONF', 'TESTCONF93'),
    "amlabel refuses to re-label a labeled volume");
like($Installcheck::Run::stdout,
    qr/Volume with label 'TESTCONF92' is active and contains data from this configuration/,
    "with correct message");

ok(!run('amlabel', 'TESTCONF', 'SomeTape'),
    "amlabel refuses to write a non-matching label");
like($Installcheck::Run::stderr,
    qr/Label 'SomeTape' doesn't match labelstr '.*'/,
    "with correct message on stderr");

ok(!run('amlabel', '-f', 'TESTCONF', 'SomeTape'),
    "amlabel will not write a non-matching label even with -f");

ok(!run('amlabel', 'TESTCONF', 'TESTCONF13', 'slot', '3'),
    "amlabel refuses to write a label already in the tapelist (and recognizes 'slot xx')");
like($Installcheck::Run::stderr,
    qr/Label 'TESTCONF13' already on a volume/,
    "with correct message on stderr");

ok(run('amlabel', '-f', 'TESTCONF', 'TESTCONF13', 'slot', '3'),
    "amlabel will write a label already in the tapelist with -f");
like($Installcheck::Run::stdout,
    qr/Writing label 'TESTCONF13'/,
    "with correct message on stdout");

ok(!run('amlabel', 'TESTCONF', 'TESTCONF88', 'slot', '2'),
    "amlabel refuses to overwrite a non-matching label");
like($Installcheck::Run::stdout,
    qr/Found label 'MyTape', but it is not from configuration 'TESTCONF'\./,
    "with correct message on stdout");

ok(run('amlabel', '-f', 'TESTCONF', 'TESTCONF88', 'slot', '2'),
    "amlabel will overwrite a non-matching label with -f");
like($Installcheck::Run::stdout,
    qr/Found label 'MyTape', but it is not from configuration 'TESTCONF'\.
Writing label 'TESTCONF88'/,
    "with correct message on stdout");

ok(run('amlabel', 'TESTCONF', 'TESTCONF88', '-f', 'slot', '2'),
    "-f option doesn't have to follow 'amlabel'");

ok(run('amlabel', 'TESTCONF', 'TESTCONF88', '--meta', 'meta-01', '--barcode', 'bar-01', '--assign'),
    "--assign works");

$tl->reload();
is_deeply($tl->{'tles'}->[0], {
       'reuse' => 1,
       'barcode' => 'bar-01',
       'meta' => 'meta-01',
       'blocksize' => undef,
       'comment' => undef,
       'position' => 1,
       'label' => 'TESTCONF88',
       'datestamp' => '0'
     },
    "tapelist correctly updated after --assign");

ok(run('amlabel', 'TESTCONF', 'slot', '4'),
    "amlabel works without a label");
like($Installcheck::Run::stdout,
     qr/Reading label\.\.\.
Found an empty tape\.
Writing label 'TESTCONF01'\.\.\.
Checking label\.\.\.
Success!/,
     "amlabel without label use autolabel");

$tl->reload();
is_deeply($tl->{'tles'}->[0], {
       'reuse' => 1,
       'barcode' => undef,
       'meta' => 'meta-01',
       'blocksize' => '32',
       'comment' => undef,
       'position' => 1,
       'label' => 'TESTCONF01',
       'datestamp' => '0'
     },
    "tapelist correctly updated after autolabel");

