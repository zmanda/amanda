# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 38;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck::Config;
use Installcheck::Run qw(run run_err run_get load_vtape load_vtape_res );
use Amanda::Device qw( :constants );
use Amanda::Config qw( :init :getconf );
use Amanda::Paths;
use Amanda::Debug;
use Amanda::Constants;
use Amanda::Tapelist;
use Amanda::Changer;

my $testconf;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

$testconf = Installcheck::Run::setup(4);
$testconf->add_param('autolabel', '"TESTCONF%%" any');
$testconf->write();

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF");
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    BAIL_OUT("config errors");
}

my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
my ($tl, $message) = Amanda::Tapelist->new($tlf, 1);
my $chg;
my $dev;

# label slot 2 with "MyTape", slot 3 with "TESTCONF13", and add
# the latter to the tapelist
{
    $chg = Amanda::Changer->new();
    $chg->load(slot => 2,
        res_cb => sub {
            my ($err, $reservation) = @_;
            if ($err) {
                BAIL_OUT("device error 1");
            }
            $dev = $reservation->{'device'};

            ($dev && ($dev->status == $DEVICE_STATUS_SUCCESS ||
                      $dev->status == $DEVICE_STATUS_VOLUME_UNLABELED))
                or BAIL_OUT("device error 2");

            $dev->start($ACCESS_WRITE, "MyTape", undef)
                or BAIL_OUT("device error 3");
            $dev->finish()
                or BAIL_OUT("device error 4");
            $reservation->release(
                finished_cb => sub {
		     Amanda::MainLoop::quit(); return;
                });
        });

    Amanda::MainLoop::run();

    $chg->quit();
    $tl->add_tapelabel("0", "TESTCONF13", "test tape");
    $tl->write($tlf);
}



load_vtape(1);

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
       'datestamp' => '0',
       'pool' => 'TESTCONF',
       'storage' => 'TESTCONF',
       'config' => 'TESTCONF'
     },
    "tapelist correctly updated");

$chg = Amanda::Changer->new();
my $res = load_vtape_res($chg, 1);
$dev = $res->{'device'};
die "read_label failed" unless $dev->read_label() == $DEVICE_STATUS_SUCCESS;
is($dev->volume_label, "TESTCONF92", "volume is actually labeled");

$res->release(finished_cb => sub { Amanda::MainLoop::quit() });
Amanda::MainLoop::run();
$chg->quit();

ok(!run('amlabel', 'TESTCONF', 'TESTCONF93'),
    "amlabel refuses to re-label a labeled volume");
like($Installcheck::Run::stdout,
    qr/Volume with label 'TESTCONF92' is active and contains data from this configuration/,
    "with correct message");

ok(!run('amlabel', 'TESTCONF', 'SomeTape'),
    "amlabel refuses to write on a  tape already labeled");
like($Installcheck::Run::stdout,
    qr/Reading label...\nLabel 'TESTCONF92' doesn't match the labelstr 'TESTCONF\[0-9\]\[0-9\]'/,
    "with correct message on stdout");
like($Installcheck::Run::stderr,
    qr/Not writing label./,
    "with correct message on stderr");

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
    qr/Found label 'MyTape' but it is not in the tapelist file./,
    "with correct message on stdout");

ok(run('amlabel', '-f', 'TESTCONF', 'TESTCONF88', 'slot', '2'),
    "amlabel will overwrite a non-matching label with -f");
like($Installcheck::Run::stdout,
     qr/Found label 'MyTape' but it is not in the tapelist file.
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
       'blocksize' => 32,
       'comment' => undef,
       'position' => 1,
       'label' => 'TESTCONF88',
       'datestamp' => '0',
       'pool' => 'TESTCONF',
       'storage' => "TESTCONF",
       'config' => "TESTCONF"
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
       'datestamp' => '0',
       'pool' => 'TESTCONF',
       'storage' => "TESTCONF",
       'config' => "TESTCONF"
     },
    "tapelist correctly updated after autolabel");

ok(!run('amlabel', 'TESTCONF', 'TESTCONF88', '--meta', 'meta-02', '--barcode', 'bar-02', '--pool', 'pool1', '--assign'),
    "--assign meta fail without -f");
like($Installcheck::Run::stdout,
     qr/TESTCONF88: Can't assign meta-label without force, old meta-label is 'meta-01'/,
     "amlabel --assign without -f (meta)");

ok(!run('amlabel', 'TESTCONF', 'TESTCONF88', '--barcode', 'bar-02', '--pool', 'pool1', '--assign'),
    "--assign barcode fail without -f");
like($Installcheck::Run::stdout,
     qr/TESTCONF88: Can't assign barcode without force, old barcode is 'bar-01'/,
     "amlabel --assign without -f (barcode)");

ok(!run('amlabel', 'TESTCONF', 'TESTCONF88', '--pool', 'pool1', '--assign'),
    "--assign pool fail without -f");
like($Installcheck::Run::stdout,
     qr/TESTCONF88: Can't assign pool without force, old pool is 'TESTCONF'/,
     "amlabel --assign without -f (pool)");

ok(run('amlabel', '-f', 'TESTCONF', 'TESTCONF88', '--meta', 'meta-02', '--barcode', 'bar-02', '--pool', 'pool-02', '--assign'),
    "--assign pool succeed with -f");

$tl->reload();
is_deeply($tl->{'tles'}->[1], {
       'reuse' => 1,
       'barcode' => 'bar-02',
       'meta' => 'meta-02',
       'blocksize' => 32,
       'comment' => undef,
       'position' => 2,
       'label' => 'TESTCONF88',
       'datestamp' => '0',
       'pool' => 'pool-02',
       'storage' => "TESTCONF",
       'config' => "TESTCONF"
     },
    "tapelist correctly updated after -f --assign");

$tl->reload(1);
$tl->{'tles'}->[1]->{'config'} = "TESTCONF2";
$tl->write();
ok(!run('amlabel', '-f', 'TESTCONF', 'TESTCONF88', '--meta', 'meta-03', '--barcode', 'bar-03', '--pool', 'pool-03', '--assign'),
    "--assign failed for another config");

like($Installcheck::Run::stdout,
     qr/TESTCONF88: Can't assign because it is is the 'TESTCONF2' config/,
     "correct stdout  for amlabel --assign for another config");

ok(!run('amlabel', 'TESTCONF', 'TESTCONF89', 'slot', '2'),
    "label for another config fail");
like($Installcheck::Run::stdout,
     qr/Found label 'TESTCONF88' but it is from config 'TESTCONF2'/,
     "label for another config fail with correct stdout");

$tl->reload(1);
$tl->{'tles'}->[1]->{'config'} = "TESTCONF";
$tl->write();

ok(!run('amlabel', 'TESTCONF', 'TESTCONF89', 'slot', '2'),
   "label for another pool fail");
like($Installcheck::Run::stdout,
     qr/Found label 'TESTCONF88' but it is from tape pool 'pool-02'/,
     "label for another pool fail with correct stdout");
