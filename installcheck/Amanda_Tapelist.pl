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

use Test::More tests => 54;
use strict;
use warnings;

use lib '@amperldir@';
use Installcheck::Config;
use Amanda::Tapelist;
use Amanda::Config qw( :init :getconf config_dir_relative );
use POSIX ":sys_wait_h";
use POSIX qw( strftime );
use Data::Dumper;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $message;
my $tl;
my $tl_ok;
my $line;
my @lines;

# First try reading a tapelist

my $testconf = Installcheck::Config->new();
$testconf->write( do_catalog => 0 );

config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
    or die("config_init failed");
my $tapelist = config_dir_relative("tapelist");

sub mktapelist {
    my ($filename, @lines) = @_;
    open(my $fh, ">", $filename) or die("Could not make tapelist '$filename'");
    for my $line (@lines) {
	print $fh $line;
    }
    close($fh);
}

sub readtapelist {
    my ($filename) = @_;
    open(my $fh, "<", $filename) or die("Could not read tapelist '$filename'");
    my @reread_lines = <$fh>;
    close($fh);
    return @reread_lines;
}

@lines = (
    "20071111010002 TESTCONF004 reuse META:META1\n",
    "20071110010002 TESTCONF003 reuse BARCODE:BAR-003 BLOCKSIZE:32\n",
    "20071109010002 TESTCONF002 reuse BARCODE:BAR-002 META:META2 BLOCKSIZE:64 #comment 2\n",
    "20071108010001 TESTCONF001 no-reuse #comment 1\n",
    "20071107110002 TESTCONF015\n",
    "20071107010002 TESTCONF006 no-reuse\n",
    "20071106010002 TESTCONF005 reuse\n",
    "20071105010002 TESTCONF007 reuse BARCODE:BAR-002 META:META2 BLOCKSIZE:64 POOL:POOL2 #comment 2\n",
    "20071104010002 TESTCONF008 reuse BARCODE:BAR-002 META:META2 BLOCKSIZE:64 POOL:POOL2 CONFIG:CONFIG2 #comment 2\n",
);
mktapelist($tapelist, @lines);

($tl, $message) = Amanda::Tapelist->new($tapelist);
$tl_ok = is_deeply($tl,	{
 filename => $tapelist,
 lockname => $tapelist . ".lock",
 last_write => $tapelist . ".last_write",
 tles => [
  { 'datestamp' => '20071111010002', 'label' => 'TESTCONF004',
    'reuse' => 1, 'position' => 1, 'blocksize' => undef,
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => undef, 'meta' => 'META1', 'comment' => undef },
  { 'datestamp' => '20071110010002', 'label' => 'TESTCONF003',
    'reuse' => 1, 'position' => 2, 'blocksize' => '32',
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => 'BAR-003', 'meta' => undef, 'comment' => undef },
  { 'datestamp' => '20071109010002', 'label' => 'TESTCONF002',
    'reuse' => 1, 'position' => 3, 'blocksize' => '64',
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
  { 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
    'reuse' => 0, 'position' => 4, 'blocksize' => undef,
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => 'comment 1' },
  { 'datestamp' => '20071107110002', 'label' => 'TESTCONF015',
    'reuse' => 1, 'position' => 5, 'blocksize' => undef,
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => undef },
  { 'datestamp' => '20071107010002', 'label' => 'TESTCONF006',
    'reuse' => 0, 'position' => 6, 'blocksize' => undef,
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => undef },
  { 'datestamp' => '20071106010002', 'label' => 'TESTCONF005',
    'reuse' => 1, 'position' => 7, 'blocksize' => undef,
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => undef },
  { 'datestamp' => '20071105010002', 'label' => 'TESTCONF007',
    'reuse' => 1, 'position' => 8, 'blocksize' => '64',
    'pool' => 'POOL2', 'storage' => undef, 'config' => undef,
    'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
  { 'datestamp' => '20071104010002', 'label' => 'TESTCONF008',
    'reuse' => 1, 'position' => 9, 'blocksize' => '64',
    'pool' => 'POOL2', 'storage' => undef, 'config' => 'CONFIG2',
    'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
 ],
 tle_hash_barcode => {
  'BAR-003' => { 'datestamp' => '20071110010002', 'label' => 'TESTCONF003',
	         'reuse' => 1, 'position' => 2, 'blocksize' => '32',
	         'pool' => undef, 'storage' => undef, 'config' => undef,
	         'barcode' => 'BAR-003', 'meta' => undef, 'comment' => undef },
  'BAR-002' => { 'datestamp' => '20071104010002', 'label' => 'TESTCONF008',
	         'reuse' => 1, 'position' => 9, 'blocksize' => '64',
	         'pool' => 'POOL2', 'storage' => undef, 'config' => 'CONFIG2',
	         'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
 },
 tle_hash_label => {
    'TESTCONF004' => { 'datestamp' => '20071111010002', 'label' => 'TESTCONF004',
		       'reuse' => 1, 'position' => 1, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => 'META1', 'comment' => undef },
    'TESTCONF003' => { 'datestamp' => '20071110010002', 'label' => 'TESTCONF003',
		       'reuse' => 1, 'position' => 2, 'blocksize' => '32',
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => 'BAR-003', 'meta' => undef, 'comment' => undef },
    'TESTCONF002' => { 'datestamp' => '20071109010002', 'label' => 'TESTCONF002',
		       'reuse' => 1, 'position' => 3, 'blocksize' => '64',
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
    'TESTCONF001' => { 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
		       'reuse' => 0, 'position' => 4, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => 'comment 1' },
    'TESTCONF015' => { 'datestamp' => '20071107110002', 'label' => 'TESTCONF015',
		       'reuse' => 1, 'position' => 5, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => undef },
    'TESTCONF006' => { 'datestamp' => '20071107010002', 'label' => 'TESTCONF006',
		       'reuse' => 0, 'position' => 6, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => undef },
    'TESTCONF005' => { 'datestamp' => '20071106010002', 'label' => 'TESTCONF005',
		       'reuse' => 1, 'position' => 7, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => undef },
    'TESTCONF007' => { 'datestamp' => '20071105010002', 'label' => 'TESTCONF007',
		       'reuse' => 1, 'position' => 8, 'blocksize' => '64',
		       'pool' => 'POOL2', 'storage' => undef, 'config' => undef,
		       'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
    'TESTCONF008' => { 'datestamp' => '20071104010002', 'label' => 'TESTCONF008',
		       'reuse' => 1, 'position' => 9, 'blocksize' => '64',
		       'pool' => 'POOL2', 'storage' => undef, 'config' => 'CONFIG2',
		       'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
 },
 tle_hash_pool_label => {
  ''=> {
    'TESTCONF004' => { 'datestamp' => '20071111010002', 'label' => 'TESTCONF004',
		       'reuse' => 1, 'position' => 1, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => 'META1', 'comment' => undef },
    'TESTCONF003' => { 'datestamp' => '20071110010002', 'label' => 'TESTCONF003',
		       'reuse' => 1, 'position' => 2, 'blocksize' => '32',
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => 'BAR-003', 'meta' => undef, 'comment' => undef },
    'TESTCONF002' => { 'datestamp' => '20071109010002', 'label' => 'TESTCONF002',
		       'reuse' => 1, 'position' => 3, 'blocksize' => '64',
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
    'TESTCONF001' => { 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
		       'reuse' => 0, 'position' => 4, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => 'comment 1' },
    'TESTCONF015' => { 'datestamp' => '20071107110002', 'label' => 'TESTCONF015',
		       'reuse' => 1, 'position' => 5, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => undef },
    'TESTCONF006' => { 'datestamp' => '20071107010002', 'label' => 'TESTCONF006',
		       'reuse' => 0, 'position' => 6, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => undef },
    'TESTCONF005' => { 'datestamp' => '20071106010002', 'label' => 'TESTCONF005',
		       'reuse' => 1, 'position' => 7, 'blocksize' => undef,
		       'pool' => undef, 'storage' => undef, 'config' => undef,
		       'barcode' => undef, 'meta' => undef, 'comment' => undef }
  },
  'POOL2' => {
    'TESTCONF007' => { 'datestamp' => '20071105010002', 'label' => 'TESTCONF007',
		       'reuse' => 1, 'position' => 8, 'blocksize' => '64',
		       'pool' => 'POOL2', 'storage' => undef, 'config' => undef,
		       'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
    'TESTCONF008' => { 'datestamp' => '20071104010002', 'label' => 'TESTCONF008',
		       'reuse' => 1, 'position' => 9, 'blocksize' => '64',
		       'pool' => 'POOL2', 'storage' => undef, 'config' => 'CONFIG2',
		       'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' }
   }
 }
}, "A simple tapelist is parsed correctly") || diag(Data::Dumper::Dumper($tl));

SKIP: {
    skip "Tapelist is parsed incorrectly, so these tests are unlikely to work", 15,
	unless $tl_ok;

    # now try writing it out and check that the results are the same
    $tl->reload(1);
    $tl->write("$tapelist-new");
    my @reread_lines = readtapelist("$tapelist-new");
    chomp($lines[4]);
    $lines[4] .= " reuse\n"; #'reuse' is automatically written
    is_deeply(\@reread_lines, \@lines, "Lines of freshly written tapelist match the original");

    is_deeply($tl->lookup_tapelabel('TESTCONF002'),
	{ 'datestamp' => '20071109010002', 'label' => 'TESTCONF002',
	  'reuse' => 1, 'position' => 3, 'blocksize' => '64',
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
	"lookup_tapelabel works");

    is_deeply($tl->lookup_tapelabel('TESTCONF009'), undef,
	"lookup_tapelabel returns undef on an unknown label");

    is_deeply($tl->lookup_tapepos(4),
	{ 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
	  'reuse' => 0, 'position' => 4, 'blocksize' => undef,
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => undef, 'meta' => undef, 'comment' => 'comment 1' },
	"lookup_tapepos works");

    is_deeply($tl->lookup_tapepos(10), undef,
	"lookup_tapepos returns undef on an unknown position");

    is_deeply($tl->lookup_tapedate('20071110010002'),
	{ 'datestamp' => '20071110010002', 'label' => 'TESTCONF003',
	  'reuse' => 1, 'position' => 2, 'blocksize' => '32',
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => 'BAR-003', 'meta' => undef, 'comment' => undef },
	"lookup_tapedate works");

    is_deeply($tl->lookup_tapedate('12345678'), undef,
	"lookup_tapedate returns undef on an unknown datestamp");

    # try some edits
    $tl->add_tapelabel("20080112010203", "TESTCONF007", "seven", 1, 'META3', 'BAR-007');
    is(scalar @{$tl->{'tles'}}, 10, "add_tapelabel adds a new element to the tapelist");

    is_deeply($tl->lookup_tapepos(1),
	{ 'datestamp' => '20080112010203', 'label' => 'TESTCONF007',
	  'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => 'BAR-007', 'meta' => 'META3', 'comment' => 'seven' },
	".. lookup_tapepos finds it at the beginning");

    is_deeply($tl->lookup_tapelabel("TESTCONF007"),
	{ 'datestamp' => '20080112010203', 'label' => 'TESTCONF007',
	  'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => 'BAR-007', 'meta' => 'META3' , 'comment' => 'seven' },
	".. lookup_tapelabel finds it");

    is_deeply($tl->lookup_tapedate("20080112010203"),
	{ 'datestamp' => '20080112010203', 'label' => 'TESTCONF007',
	  'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => 'BAR-007', 'meta' => 'META3', 'comment' => 'seven' },
	".. lookup_tapedate finds it");

    # try some edits
    $tl->add_tapelabel("20080112010204", "TESTCONF008", "eight", 0, undef, undef, 128, 'POOL3', 'STORAGE3', 'CONFIG3');
    is(scalar @{$tl->{'tles'}}, 11, "add_tapelabel adds a new element to the tapelist no-reuse");

    is_deeply($tl->lookup_tapelabel("TESTCONF008"),
	{ 'datestamp' => '20080112010204', 'label' => 'TESTCONF008',
	  'reuse' => 0, 'position' => 1, 'blocksize' => '128',
	  'pool' => 'POOL3', 'storage' => 'STORAGE3', 'config' => 'CONFIG3',
	  'barcode' => undef, 'meta' => undef, 'comment' => 'eight' },
	".. lookup_tapelabel finds it no-reuse");

    $tl->remove_tapelabel("TESTCONF008");
    is(scalar @{$tl->{'tles'}}, 10, "remove_tapelabel removes an element from the tapelist, no-reuse");

    $tl->remove_tapelabel("TESTCONF002");
    is(scalar @{$tl->{'tles'}}, 9, "remove_tapelabel removes an element from the tapelist");

    is_deeply($tl->lookup_tapepos(4), # used to be in position 5
	{ 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
	  'reuse' => 0, 'position' => 4, 'blocksize' => undef,
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => undef, 'meta' => undef, 'comment' => 'comment 1' },
	".. tape positions are adjusted correctly");

    is_deeply($tl->lookup_tapelabel("TESTCONF002"), undef,
	".. lookup_tapelabel no longer finds it");

    is_deeply($tl->lookup_tapedate("20071109010002"), undef,
	".. lookup_tapedate no longer finds it");

    # insert in the middle of the list.
    $tl->add_tapelabel("20071109010204", "TESTCONF009", "nine", 1);

    is_deeply($tl->lookup_tapepos(4),
	{ 'datestamp' => '20071109010204', 'label' => 'TESTCONF009',
	  'reuse' => '1', 'position' => 4, 'blocksize' => undef,
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => undef, 'meta' => undef, 'comment' => 'nine' },
	".. tape positions are adjusted correctly");

    is_deeply($tl->lookup_tapelabel('TESTCONF009'),
	{ 'datestamp' => '20071109010204', 'label' => 'TESTCONF009',
	  'reuse' => '1', 'position' => 4, 'blocksize' => undef,
	  'pool' => undef, 'storage' => undef, 'config' => undef,
	  'barcode' => undef, 'meta' => undef, 'comment' => 'nine' },
	".. tape positions are adjusted correctly");

    ## set tapecycle to 0 to perform the next couple tests
    config_uninit();
    my $cor = new_config_overrides(1);
    add_config_override_opt($cor, "tapecycle=1");
    set_config_overrides($cor);
    config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
	or die("config_init failed");

    is( Amanda::Tapelist::get_last_reusable_tape_label(getconf($CNF_LABELSTR)->{'template'}, "TESTCONF", "TESTCONF", getconf($CNF_TAPECYCLE), 0, 0, 0, 0),
        'TESTCONF005', ".. get_last_reusable_tape_labe for skip=0" );

    is( Amanda::Tapelist::get_last_reusable_tape_label(getconf($CNF_LABELSTR)->{'template'}, "TESTCONF", "TESTCONF", getconf($CNF_TAPECYCLE), 0, 0, 0, 1),
        'TESTCONF015', ".. get_last_reusable_tape_labe for skip=1" );

    is( Amanda::Tapelist::get_last_reusable_tape_label(getconf($CNF_LABELSTR)->{'template'}, "TESTCONF", "TESTCONF", getconf($CNF_TAPECYCLE), 0, 0, 0, 2),
        'TESTCONF009', ".. get_last_reusable_tape_labe for skip=2" );
}

# try parsing various invalid lines
@lines = (
    "2006123456 FOO reuse\n", # valid
#    "TESTCONF003 290385098 reuse\n", # invalid
#    "20071109010002 TESTCONF002 re-use\n", # invalid
#    "20071108010001 TESTCONF001\n", # invalid
#    "20071108010001 TESTCONF001 #comment\n", # invalid
#    "#comment\n", # invalid
);
mktapelist($tapelist, @lines);

($tl, $message) = Amanda::Tapelist->new($tapelist);
is_deeply($tl, {
  filename => $tapelist,
  lockname => $tapelist . ".lock",
  last_write => $tapelist . ".last_write",
  tles => [
  { 'datestamp' => '2006123456', 'label' => 'FOO',
    'reuse' => 1, 'position' => 1, 'blocksize' => undef,
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => undef },
  ],
  tle_hash_barcode => undef,
  tle_hash_label => {
    'FOO' => { 'datestamp' => '2006123456', 'label' => 'FOO',
	       'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	       'pool' => undef, 'storage' => undef, 'config' => undef,
	       'barcode' => undef, 'meta' => undef, 'comment' => undef },
  },
  tle_hash_pool_label => {
   ''=> {
    'FOO' => { 'datestamp' => '2006123456', 'label' => 'FOO',
	       'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	       'pool' => undef, 'storage' => undef, 'config' => undef,
	       'barcode' => undef, 'meta' => undef, 'comment' => undef },
   }
  }
}, "Invalid lines are ignored")  || diag(Data::Dumper::Dumper($tl));

# make sure clear_tapelist is empty
$tl->clear_tapelist();
is_deeply($tl,	{ filename => $tapelist,
                  lockname => $tapelist . ".lock",
		  last_write => $tapelist . ".last_write",
		  tles => [],
		  tle_hash_barcode => undef,
		  tle_hash_label => undef,
		  tle_hash_pool_label => undef }, "clear_tapelist returns an empty tapelist")  || diag(Data::Dumper::Dumper($tl));;

$tl->reload();
delete $tl->{'mtime_tapelist'};
is_deeply($tl, {
  filename => $tapelist,
  lockname => $tapelist . ".lock",
  last_write => $tapelist . ".last_write",
  tles => [
  { 'datestamp' => '2006123456', 'label' => 'FOO',
    'reuse' => 1, 'position' => 1, 'blocksize' => undef,
    'pool' => undef, 'storage' => undef, 'config' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => undef },
  ],
  tle_hash_barcode => undef,
  tle_hash_label => {
    'FOO' => { 'datestamp' => '2006123456', 'label' => 'FOO',
	       'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	       'pool' => undef, 'storage' => undef, 'config' => undef,
	       'barcode' => undef, 'meta' => undef, 'comment' => undef },
  },
  tle_hash_pool_label => {
   '' => {
    'FOO' => { 'datestamp' => '2006123456', 'label' => 'FOO',
	       'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	       'pool' => undef, 'storage' => undef, 'config' => undef,
	       'barcode' => undef, 'meta' => undef, 'comment' => undef },
   }
  }
}, "reload works")  || diag(Data::Dumper::Dumper($tl));

# test retention_tapes
$testconf = Installcheck::Config->new();
$testconf->add_param("tapecycle", 1 );
$testconf->add_policy("test_policy", [ retention_tapes => 2 ]);
$testconf->add_storage("STORAGE1", [ policy => "\"test_policy\"",
				     tapepool => "\"POOL1\"" ]);
$testconf->add_storage("STORAGE2", [ policy => "\"test_policy\"",
				     tapepool => "\"POOL2\"" ]);
$testconf->write( do_catalog => 0 );

config_uninit();
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
    or die("config_init failed");
@lines = (
    "20071121010002 TESTCONF1-001 reuse BARCODE:BAR1-001 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "20071122010002 TESTCONF1-002 reuse BARCODE:BAR1-002 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "20071123010002 TESTCONF1-003 reuse BARCODE:BAR1-003 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "20071124010002 TESTCONF1-004 reuse BARCODE:BAR1-004 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "0  TESTCONF1-005 reuse BARCODE:BAR1-005 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 #comment 2\n",
    "0  TESTCONF1-006 reuse BARCODE:BAR1-006 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 #comment 2\n",
    "20071021010002 TESTCONF2-001 reuse BARCODE:BAR2-001 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "20071022010002 TESTCONF2-002 reuse BARCODE:BAR2-002 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "20071023010002 TESTCONF2-003 reuse BARCODE:BAR2-003 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "20071024010002 TESTCONF2-004 reuse BARCODE:BAR2-004 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "0 TESTCONF2-005 reuse BARCODE:BAR2--005 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 #comment 2\n",
    "20070921010002 TESTCONF2-011 reuse BARCODE:BAR2-011 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "20070922010002 TESTCONF2-012 reuse BARCODE:BAR2-012 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "20070923010002 TESTCONF2-013 reuse BARCODE:BAR2-013 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "20070928010002 TESTCONF2-014 reuse BARCODE:BAR2-014 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "0 TESTCONF2-015 reuse BARCODE:BAR2-015 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 #comment 2\n",
);
mktapelist($tapelist, @lines);
$tl->reload();

my @lr = Amanda::Tapelist::list_retention();
is_deeply(\@lr,
          [ 'TESTCONF1-004', 'TESTCONF1-003', 'TESTCONF2-004', 'TESTCONF2-003' ],
	  "list_retention") || diag(Data::Dumper::Dumper(\@lr));

@lr = Amanda::Tapelist::list_no_retention();
is_deeply(\@lr,
          [ 'TESTCONF1-002', 'TESTCONF1-001', 'TESTCONF2-002', 'TESTCONF2-001' ],
	  "list_no_retention") || diag(Data::Dumper::Dumper(\@lr));

@lr = Amanda::Tapelist::list_new_tapes("STORAGE1", 1);
is_deeply(\@lr,
          [ 'TESTCONF1-006' ],
	  "list_new_tapes STORAGE1 1") || diag(Data::Dumper::Dumper(\@lr));

@lr = Amanda::Tapelist::list_new_tapes("STORAGE1", 2);
is_deeply(\@lr,
          [ 'TESTCONF1-006', 'TESTCONF1-005' ],
	  "list_new_tapes STORAGE1 2") || diag(Data::Dumper::Dumper(\@lr));

@lr = Amanda::Tapelist::list_new_tapes("STORAGE2", 2);
is_deeply(\@lr,
          [ 'TESTCONF2-015', 'TESTCONF2-005' ],
	  "list_new_tapes STORAGE2 2") || diag(Data::Dumper::Dumper(\@lr));


config_uninit();
my $config_overrides = new_config_overrides( 2 );
add_config_override_opt( $config_overrides, 'storage=STORAGE2' );
set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
	or die("config_init failed");

@lr = Amanda::Tapelist::list_retention();
is_deeply(\@lr,
          [ 'TESTCONF2-004', 'TESTCONF2-003' ],
	  "list_retention") || diag(Data::Dumper::Dumper(\@lr));

@lr = Amanda::Tapelist::list_no_retention();
is_deeply(\@lr,
          [ 'TESTCONF2-002', 'TESTCONF2-001' ],
	  "list_no_retention") || diag(Data::Dumper::Dumper(\@lr));

# test retention_days
$testconf = Installcheck::Config->new();
$testconf->add_param("tapecycle", 1 );
$testconf->add_policy("test_policy", [ retention_days => 2 ]);
$testconf->add_storage("STORAGE1", [ policy => "\"test_policy\"",
				     tapepool => "\"POOL1\"" ]);
$testconf->add_storage("STORAGE2", [ policy => "\"test_policy\"",
				     tapepool => "\"POOL2\"" ]);
$testconf->write( do_catalog => 0 );

config_uninit();
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
    or die("config_init failed");

my $epoc = time();
my $today_0 = strftime("%Y%m%d%H%M%S", localtime($epoc -3600));
my $today_1 = strftime("%Y%m%d%H%M%S", localtime($epoc - 1*24*60*60 - 3600));
my $today_2 = strftime("%Y%m%d%H%M%S", localtime($epoc - 2*24*60*60 - 3600));
my $today_3 = strftime("%Y%m%d%H%M%S", localtime($epoc - 3*24*60*60 - 3600));

@lines = (
    "$today_0 TESTCONF1-001 reuse BARCODE:BAR1-001 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "$today_1 TESTCONF1-002 reuse BARCODE:BAR1-002 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "$today_2 TESTCONF1-003 reuse BARCODE:BAR1-003 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "$today_3 TESTCONF1-004 reuse BARCODE:BAR1-004 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "0  TESTCONF1-005 reuse BARCODE:BAR1-005 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "0  TESTCONF1-006 reuse BARCODE:BAR1-006 META:META1 BLOCKSIZE:64 POOL:POOL1 STORAGE:STORAGE1 CONFIG:TESTCONF #comment 2\n",
    "$today_0 TESTCONF2-001 reuse BARCODE:BAR2-001 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "$today_1 TESTCONF2-002 reuse BARCODE:BAR2-002 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "$today_2 TESTCONF2-003 reuse BARCODE:BAR2-003 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "$today_3 TESTCONF2-004 reuse BARCODE:BAR2-004 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "0 TESTCONF2-005 reuse BARCODE:BAR2-005 META:META2 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF #comment 2\n",
    "$today_0 TESTCONF2-011 reuse BARCODE:BAR3-011 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "$today_1 TESTCONF2-012 reuse BARCODE:BAR3-012 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "$today_2 TESTCONF2-013 reuse BARCODE:BAR3-013 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "$today_3 TESTCONF2-014 reuse BARCODE:BAR3-014 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
    "0 TESTCONF2-015 reuse BARCODE:BAR3-015 META:META3 BLOCKSIZE:64 POOL:POOL2 STORAGE:STORAGE2 CONFIG:TESTCONF2 #comment 2\n",
);
mktapelist($tapelist, @lines);
$tl->reload();

@lr = Amanda::Tapelist::list_retention();
is_deeply(\@lr,
          [ 'TESTCONF1-001', 'TESTCONF2-001', 'TESTCONF1-002', 'TESTCONF2-002' ],
	  "list_retention") || diag(Data::Dumper::Dumper(\@lr));

@lr = Amanda::Tapelist::list_no_retention();
is_deeply(\@lr,
          [ 'TESTCONF1-003', 'TESTCONF2-003', 'TESTCONF1-004', 'TESTCONF2-004' ],
	  "list_no_retention") || diag(Data::Dumper::Dumper(\@lr));


config_uninit();
$config_overrides = new_config_overrides( 2 );
add_config_override_opt( $config_overrides, 'storage=STORAGE2' );
set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
	or die("config_init failed");

@lr = Amanda::Tapelist::list_retention();
is_deeply(\@lr,
          [ 'TESTCONF2-001', 'TESTCONF2-002' ],
	  "list_retention") || diag(Data::Dumper::Dumper(\@lr));

@lr = Amanda::Tapelist::list_no_retention();
is_deeply(\@lr,
          [ 'TESTCONF2-003', 'TESTCONF2-004' ],
	  "list_no_retention") || diag(Data::Dumper::Dumper(\@lr));

is(Amanda::Tapelist::volume_is_reusable("TESTCONF1-001"), 0, "volume_is_reusable TESTCONF1-001");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF1-002"), 0, "volume_is_reusable TESTCONF1-002");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF1-003"), 1, "volume_is_reusable TESTCONF1-003");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF1-004"), 1, "volume_is_reusable TESTCONF1-004");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF1-005"), 1, "volume_is_reusable TESTCONF1-005");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF1-006"), 1, "volume_is_reusable TESTCONF1-006");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-001"), 0, "volume_is_reusable TESTCONF2-001");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-002"), 0, "volume_is_reusable TESTCONF2-002");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-003"), 1, "volume_is_reusable TESTCONF2-003");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-004"), 1, "volume_is_reusable TESTCONF2-004");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-005"), 1, "volume_is_reusable TESTCONF2-005");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-011"), 0, "volume_is_reusable TESTCONF2-011");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-012"), 0, "volume_is_reusable TESTCONF2-012");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-013"), 0, "volume_is_reusable TESTCONF2-013");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-014"), 0, "volume_is_reusable TESTCONF2-014");
is(Amanda::Tapelist::volume_is_reusable("TESTCONF2-015"), 1, "volume_is_reusable TESTCONF2-015");
