# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 26;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Config;
use Amanda::Tapelist;
use Amanda::Config qw( :init :getconf config_dir_relative );
use POSIX ":sys_wait_h";
use Data::Dumper;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $tl;
my $tl_ok;
my $line;
my @lines;

# First try reading a tapelist

my $testconf = Installcheck::Config->new();
$testconf->write();

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
);
mktapelist($tapelist, @lines);

$tl = Amanda::Tapelist->new($tapelist);
$tl_ok = is_deeply($tl,	{
 filename => $tapelist,
 lockname => $tapelist . ".lock",
 tles => [
  { 'datestamp' => '20071111010002', 'label' => 'TESTCONF004',
    'reuse' => 1, 'position' => 1, 'blocksize' => undef,
    'barcode' => undef, 'meta' => 'META1', 'comment' => undef },
  { 'datestamp' => '20071110010002', 'label' => 'TESTCONF003',
    'reuse' => 1, 'position' => 2, 'blocksize' => '32',
    'barcode' => 'BAR-003', 'meta' => undef, 'comment' => undef },
  { 'datestamp' => '20071109010002', 'label' => 'TESTCONF002',
    'reuse' => 1, 'position' => 3, 'blocksize' => '64',
    'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
  { 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
    'reuse' => '', 'position' => 4, 'blocksize' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => 'comment 1' },
] }, "A simple tapelist is parsed correctly");

SKIP: {
    skip "Tapelist is parsed incorrectly, so these tests are unlikely to work", 15,
	unless $tl_ok;

    # now try writing it out and check that the results are the same
    $tl->write("$tapelist-new");
    my @reread_lines = readtapelist("$tapelist-new");
    is_deeply(\@reread_lines, \@lines, "Lines of freshly written tapelist match the original");

    is_deeply($tl->lookup_tapelabel('TESTCONF002'),
	{ 'datestamp' => '20071109010002', 'label' => 'TESTCONF002',
	  'reuse' => 1, 'position' => 3, 'blocksize' => '64',
	  'barcode' => 'BAR-002', 'meta' => 'META2', 'comment' => 'comment 2' },
	"lookup_tapelabel works");

    is_deeply($tl->lookup_tapelabel('TESTCONF009'), undef,
	"lookup_tapelabel returns undef on an unknown label");

    is_deeply($tl->lookup_tapepos(4),
	{ 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
	  'reuse' => '', 'position' => 4, 'blocksize' => undef,
	  'barcode' => undef, 'meta' => undef, 'comment' => 'comment 1' },
	"lookup_tapepos works");

    is_deeply($tl->lookup_tapepos(9), undef,
	"lookup_tapepos returns undef on an unknown position");

    is_deeply($tl->lookup_tapedate('20071110010002'),
	{ 'datestamp' => '20071110010002', 'label' => 'TESTCONF003',
	  'reuse' => 1, 'position' => 2, 'blocksize' => '32',
	  'barcode' => 'BAR-003', 'meta' => undef, 'comment' => undef },
	"lookup_tapedate works");

    is_deeply($tl->lookup_tapedate('12345678'), undef,
	"lookup_tapedate returns undef on an unknown datestamp");

    # try some edits
    $tl->add_tapelabel("20080112010203", "TESTCONF007", "seven", 1, 'META3', 'BAR-007');
    is(scalar @{$tl->{'tles'}}, 5, "add_tapelabel adds a new element to the tapelist");

    is_deeply($tl->lookup_tapepos(1),
	{ 'datestamp' => '20080112010203', 'label' => 'TESTCONF007',
	  'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	  'barcode' => 'BAR-007', 'meta' => 'META3', 'comment' => 'seven' },
	".. lookup_tapepos finds it at the beginning");

    is_deeply($tl->lookup_tapelabel("TESTCONF007"),
	{ 'datestamp' => '20080112010203', 'label' => 'TESTCONF007',
	  'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	  'barcode' => 'BAR-007', 'meta' => 'META3' , 'comment' => 'seven' },
	".. lookup_tapelabel finds it");

    is_deeply($tl->lookup_tapedate("20080112010203"),
	{ 'datestamp' => '20080112010203', 'label' => 'TESTCONF007',
	  'reuse' => 1, 'position' => 1, 'blocksize' => undef,
	  'barcode' => 'BAR-007', 'meta' => 'META3', 'comment' => 'seven' },
	".. lookup_tapedate finds it");

    # try some edits
    $tl->add_tapelabel("20080112010204", "TESTCONF008", "eight", 0, undef, undef, 128);
    is(scalar @{$tl->{'tles'}}, 6, "add_tapelabel adds a new element to the tapelist no-reuse");

    is_deeply($tl->lookup_tapelabel("TESTCONF008"),
	{ 'datestamp' => '20080112010204', 'label' => 'TESTCONF008',
	  'reuse' => 0, 'position' => 1, 'blocksize' => '128',
	   'barcode' => undef, 'meta' => undef, 'comment' => 'eight' },
	".. lookup_tapelabel finds it no-reuse");

    $tl->remove_tapelabel("TESTCONF008");
    is(scalar @{$tl->{'tles'}}, 5, "remove_tapelabel removes an element from the tapelist, no-reuse");

    $tl->remove_tapelabel("TESTCONF002");
    is(scalar @{$tl->{'tles'}}, 4, "remove_tapelabel removes an element from the tapelist");

    is_deeply($tl->lookup_tapepos(4), # used to be in position 5
	{ 'datestamp' => '20071108010001', 'label' => 'TESTCONF001',
	  'reuse' => '', 'position' => 4, 'blocksize' => undef,
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
	  'barcode' => undef, 'meta' => undef, 'comment' => 'nine' },
	".. tape positions are adjusted correctly");

    is_deeply($tl->lookup_tapelabel('TESTCONF009'),
	{ 'datestamp' => '20071109010204', 'label' => 'TESTCONF009',
	  'reuse' => '1', 'position' => 4, 'blocksize' => undef,
	  'barcode' => undef, 'meta' => undef, 'comment' => 'nine' },
	".. tape positions are adjusted correctly");

    ## set tapecycle to 0 to perform the next couple tests
    config_uninit();
    my $cor = new_config_overrides(1);
    add_config_override_opt($cor, "tapecycle=1");
    set_config_overrides($cor);
    config_init($CONFIG_INIT_EXPLICIT_NAME, "TESTCONF") == $CFGERR_OK
	or die("config_init failed");

    is( Amanda::Tapelist::get_last_reusable_tape_label(0),
        'TESTCONF002', ".. get_last_reusable_tape_labe for skip=0" );

    is( Amanda::Tapelist::get_last_reusable_tape_label(2),
        'TESTCONF004', ".. get_last_reusable_tape_labe for skip=2" );
}

# try parsing various invalid lines
@lines = (
    "2006123456 FOO reuse\n", # valid
    "TESTCONF003 290385098 reuse\n", # invalid
    "20071109010002 TESTCONF002 re-use\n", # invalid
    "20071108010001 TESTCONF001\n", # invalid
    "20071108010001 TESTCONF001 #comment\n", # invalid
    "#comment\n", # invalid
);
mktapelist($tapelist, @lines);

$tl = Amanda::Tapelist->new($tapelist);
is_deeply($tl, {
  filename => $tapelist,
  lockname => $tapelist . ".lock",
  tles => [
  { 'datestamp' => '2006123456', 'label' => 'FOO',
    'reuse' => 1, 'position' => 1, 'blocksize' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => undef },
] }, "Invalid lines are ignored");

# make sure clear_tapelist is empty
$tl->clear_tapelist();
is_deeply($tl,	{ filename => $tapelist,
                  lockname => $tapelist . ".lock",
		  tles => [] }, "clear_tapelist returns an empty tapelist");

$tl->reload();
is_deeply($tl, {
  filename => $tapelist,
  lockname => $tapelist . ".lock",
  tles => [
  { 'datestamp' => '2006123456', 'label' => 'FOO',
    'reuse' => 1, 'position' => 1, 'blocksize' => undef,
    'barcode' => undef, 'meta' => undef, 'comment' => undef },
] }, "reload works");

