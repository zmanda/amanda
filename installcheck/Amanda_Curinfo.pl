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

use Test::More tests => 18;
use strict;
use warnings;

use File::Path qw(mkpath);
use Data::Dumper;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Util qw( slurp burp sanitise_filename );

use Amanda::Curinfo;
use Amanda::Curinfo::Info;

my $use_diff;

BEGIN {
    eval "use Text::Diff;";
    $use_diff = !$@;
}

my $testconf = Installcheck::Config->new();
$testconf->write();

my $infodir = "$Installcheck::TMP/infodir";

config_init( $CONFIG_INIT_EXPLICIT_NAME, "TESTCONF" ) == $CFGERR_OK
  or die("config_init failed");

my $data = <<EOF;
version: 0
command: 0
full-rate: 2974.060606 2886.529412 2973.878788
full-comp: 0.471483 0.471497 0.471477
incr-rate: 27.800000 3062.406250 1.000000
incr-comp: 0.133333 0.133333 0.133333
stats: 0 208160 98144 33 1211438831 16 S3012
stats: 1 30 4 0 1212380683 8 S3023
last_level: 1 11
history: 1 30 4 1212380683 0
history: 1 30 4 1212294141 0
history: 1 30 4 1212207722 0
history: 1 30 4 1212121208 0
history: 1 30 4 1212034786 0
history: 1 30 4 1211948729 0
history: 1 30 4 1211862004 0
history: 1 30 4 1211775583 0
history: 1 30 4 1211690046 0
history: 1 30 4 1211603671 0
history: 1 30 4 1211526232 0
history: 0 208160 98144 1211438831 33
history: 1 50 5 1211344512 0
history: 1 30 4 1210912587 0
history: 1 30 4 1210826040 0
history: 1 30 4 1210739734 0
history: 1 30 4 1210653066 0
history: 1 30 4 1210567105 0
history: 1 30 4 1210480489 0
history: 1 30 4 1210393557 0
history: 1 30 4 1210307156 0
history: 1 30 4 1210220759 0
history: 1 30 4 1210134427 0
history: 1 30 4 1210047948 0
history: 1 30 4 1209961521 0
history: 1 30 4 1209875114 0
history: 1 30 4 1209788730 0
history: 1 30 4 1209702327 0
history: 1 30 4 1209615918 0
history: 1 30 4 1209529659 0
history: 1 30 4 1209443026 0
history: 1 30 4 1209356534 0
history: 1 30 4 1209270139 0
history: 1 30 4 1209183732 0
history: 1 30 4 1209097323 0
history: 1 30 4 1209010914 0
history: 1 30 4 1208924687 0
history: 1 30 4 1208838244 0
history: 1 30 4 1208751683 0
history: 1 30 4 1208665357 0
history: 0 208150 98142 1208579385 34
history: 1 50 10 1208492851 0
history: 1 50 10 1208406237 0
history: 1 50 10 1208319724 0
history: 1 50 10 1208233394 0
history: 1 50 10 1208146982 0
history: 1 50 10 1208060529 0
history: 1 50 10 1207974095 0
history: 1 50 10 1207887693 0
history: 1 50 10 1207801290 0
history: 1 50 10 1207714857 0
history: 1 50 10 1207628549 0
history: 1 50 10 1207541982 0
history: 1 50 10 1207455648 0
history: 1 50 10 1207369359 0
history: 1 50 10 1207282921 0
history: 1 50 10 1207196370 0
history: 1 50 10 1207109992 0
history: 1 50 10 1207023639 0
history: 1 50 10 1206937266 0
history: 1 50 10 1206850809 0
history: 1 50 10 1206764417 0
history: 1 50 10 1206678095 0
history: 1 50 10 1206591577 0
history: 1 50 10 1206505188 0
history: 1 50 10 1206418784 0
history: 1 30 4 1206332424 0
history: 1 30 4 1206245966 0
history: 1 30 4 1206159612 0
history: 0 208150 98138 1206073398 33
history: 1 70 11 1205986945 0
history: 1 70 11 1205900387 0
history: 1 70 11 1205814007 0
history: 1 70 11 1205727544 0
history: 1 70 11 1205641330 0
history: 1 70 11 1205554904 0
history: 1 70 11 1205468423 0
history: 1 70 11 1205381996 0
history: 1 70 11 1205295616 0
history: 1 70 11 1205209178 0
history: 1 70 11 1205122964 0
history: 1 70 11 1205039969 0
history: 1 70 11 1204953604 0
history: 1 70 11 1204867108 0
history: 1 70 11 1204780854 0
history: 1 70 11 1204694432 0
history: 1 70 11 1204608080 0
history: 1 70 11 1204521528 0
history: 1 30 4 1204435294 0
history: 1 30 4 1204348825 0
history: 1 30 4 1204176062 0
history: 1 30 4 1204003405 0
history: 1 30 4 1203916794 0
history: 1 30 4 1203830409 0
history: 1 30 4 1203744032 0
history: 1 30 4 1203657632 0
history: 0 208120 98130 1203571350 33
history: 2 220 139 1203312046 0
history: 2 220 139 1203139220 0
history: 2 220 139 1203053015 0
//
EOF

## set up the temporary infofile to read from
my $tmp_infofile = "$Installcheck::TMP/temporary-infofile";
burp $tmp_infofile, $data;

my $host   = "fakehost";
my $disk   = "/home/fakeuser/disk";
my $host_q = sanitise_filename($host);
my $disk_q = sanitise_filename($disk);

my $curinfo_file = "$infodir/$host_q/$disk_q/info";

## start running tests
ok(my $ci = Amanda::Curinfo->new($infodir),
    "create the Amanda::Curinfo object");

is_deeply(
    $ci,
    bless({ 'infodir' => $infodir }, 'Amanda::Curinfo'),
    "Amanda::Curinfo object check"
);

ok(my $info = Amanda::Curinfo::Info->new($tmp_infofile),
    "create the Amanda::Curinfo::Info object");
ok($ci->put_info($host, $disk, $info),
    "test writing the Info object to the Curinfo database");
ok(-f $curinfo_file, "Info object installed in the correct location");

## re-load Amanda::Curinfo::Info and check each field separately

undef $info;
ok($info = $ci->get_info($host, $disk),
    "Amanda::Curinfo::Info constructor check");

## Test components of Amanda::Curinfo::Info separately

is( $info->{command}, "0", "Amanda::Curinfo::Info->command check" );

is_deeply(
    $info->{incr},
    bless(
        {
            'rate' => [ '27.800000', '3062.406250', '1.000000' ],
            'comp' => [ '0.133333',  '0.133333',    '0.133333' ]
        },
        'Amanda::Curinfo::Perf'
    ),
    'Amanda::Curinfo::Perf ($info->{incr}) check'
);

is_deeply(
    $info->{full},
    bless(
        {
            'rate' => [ '2974.060606', '2886.529412', '2973.878788' ],
            'comp' => [ '0.471483',    '0.471497',    '0.471477' ]
        },
        'Amanda::Curinfo::Perf'
    ),
    'Amanda::Curinfo::Perf ($info->{full}) check'
);

is_deeply(
    $info->{inf},
    [
        bless(
            {
                'level'   => '0',
                'date'    => '1211438831',
                'csize'   => '98144',
                'label'   => 'S3012',
                'filenum' => '16',
                'secs'    => '33',
                'size'    => '208160'
            },
            'Amanda::Curinfo::Stats'
        ),
        bless(
            {
                'level'   => '1',
                'date'    => '1212380683',
                'csize'   => '4',
                'label'   => 'S3023',
                'filenum' => '8',
                'secs'    => '0',
                'size'    => '30'
            },
            'Amanda::Curinfo::Stats'
        )
    ],
    'Amanda::Curinfo::Stats ($info->{inf}) check'
);

is_deeply(
    $info->{history},
    [
        bless(
            {
                'level' => '1',
                'date'  => '1212380683',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1212294141',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1212207722',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1212121208',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1212034786',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1211948729',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1211862004',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1211775583',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1211690046',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1211603671',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1211526232',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '0',
                'date'  => '1211438831',
                'csize' => '98144',
                'secs'  => '33',
                'size'  => '208160'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1211344512',
                'csize' => '5',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210912587',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210826040',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210739734',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210653066',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210567105',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210480489',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210393557',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210307156',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210220759',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210134427',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1210047948',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209961521',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209875114',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209788730',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209702327',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209615918',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209529659',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209443026',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209356534',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209270139',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209183732',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209097323',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1209010914',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208924687',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208838244',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208751683',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208665357',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '0',
                'date'  => '1208579385',
                'csize' => '98142',
                'secs'  => '34',
                'size'  => '208150'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208492851',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208406237',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208319724',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208233394',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208146982',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1208060529',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207974095',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207887693',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207801290',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207714857',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207628549',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207541982',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207455648',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207369359',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207282921',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207196370',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207109992',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1207023639',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206937266',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206850809',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206764417',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206678095',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206591577',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206505188',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206418784',
                'csize' => '10',
                'secs'  => '0',
                'size'  => '50'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206332424',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206245966',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1206159612',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '0',
                'date'  => '1206073398',
                'csize' => '98138',
                'secs'  => '33',
                'size'  => '208150'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205986945',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205900387',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205814007',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205727544',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205641330',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205554904',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205468423',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205381996',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205295616',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205209178',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205122964',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1205039969',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204953604',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204867108',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204780854',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204694432',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204608080',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204521528',
                'csize' => '11',
                'secs'  => '0',
                'size'  => '70'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204435294',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204348825',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204176062',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1204003405',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1203916794',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1203830409',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1203744032',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '1',
                'date'  => '1203657632',
                'csize' => '4',
                'secs'  => '0',
                'size'  => '30'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '0',
                'date'  => '1203571350',
                'csize' => '98130',
                'secs'  => '33',
                'size'  => '208120'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '2',
                'date'  => '1203312046',
                'csize' => '139',
                'secs'  => '0',
                'size'  => '220'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '2',
                'date'  => '1203139220',
                'csize' => '139',
                'secs'  => '0',
                'size'  => '220'
            },
            'Amanda::Curinfo::History'
        ),
        bless(
            {
                'level' => '2',
                'date'  => '1203053015',
                'csize' => '139',
                'secs'  => '0',
                'size'  => '220'
            },
            'Amanda::Curinfo::History'
        )
    ],
    'Amanda::Curinfo::history ($info->{history}) check'
);

is( $info->{last_level}, 1, "Amanda::Curinfo::Info last_level check" );

is( $info->{consecutive_runs},
    11, "Amanda::Curinfo::Info consecutive_runs check" );

## test the accessor functions
is( $info->get_dumpdate(2),
    "2008:6:2:4:24:43", 'Amanda::Curinfo::Info->get_dumpdate check' );

is( $info->get_dumpdate(1),
    "2008:5:22:6:47:11", 'Amanda::Curinfo::Info->get_dumpdate check' );

## delete the file

$ci->del_info($host, $disk);
ok(!-f "$infodir/$host_q/$disk_q/info", "infofile successfully deleted");

## rewrite it using the built-in

ok( $ci->put_info($host, $disk, $info), "Amanda::Curinfo->put_info check");

## compare the two files

sub diff_wi
{
    my ( $from, $to ) = @_;

    $$from =~ s{\s+}{ }g;
    $$to   =~ s{\s+}{ }g;

    return diff($from, $to);
}

my $filedata = slurp $curinfo_file;

if ($use_diff) {

    my $diff_txt = diff_wi(\$filedata, \$data);

    is($diff_txt, "", "file writing functional.")
      or diag("original and written infofile different.  diff:\n"
          . diff_wi(\$filedata, \$data));

} else {

    $filedata =~ s{\s+}{ }g;
    $data     =~ s{\s+}{ }g;

    is($filedata, $data, "file writing functional. ")
      or diag("original and written infofile different.");
}
