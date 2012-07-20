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

use Test::More tests => 31;
use File::Path;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Mock qw( setup_mock_mtx );
use Installcheck::Run qw( run run_get run_err );

my $vtape_root = "$Installcheck::TMP/mock_mtx_vtapes";
my $mtx_state_file = setup_mock_mtx (
	 barcodes => 1,
	 track_orig => 1,
	 num_slots => 5,
	 num_drives => 2,
	 num_ie => 1,
	 first_slot => 1,
	 first_drive => 0,
	 first_ie => 6,
	 vtape_root => $vtape_root,
	 loaded_slots => {
	     1 => '023984',
	     3 => '978344',
         },
       );

unless(like(run_get('mock/mtx', '-f', $mtx_state_file, 'inquiry'),
        qr/Product Type: Medium Changer/,
        "mtx inquiry")) {
  SKIP: {
      skip("mock/mtx seems broken; calling off the test early",
           Test::More->builder->expected_tests-1);
    }
    exit 0;
}

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 6 Slots \( 1 Import/Export \)
Data Transfer Element 0:Empty
Data Transfer Element 1:Empty
      Storage Element 1:Full :VolumeTag=023984
      Storage Element 2:Empty
      Storage Element 3:Full :VolumeTag=978344
      Storage Element 4:Empty
      Storage Element 5:Empty
      Storage Element 6 IMPORT/EXPORT:Empty},
    "mtx status (+BARCODES, +TRACK ORIG)");

like(run_err('mock/mtx', '-f', $mtx_state_file, 'load', '2', '0'),
    qr/source Element Address 2 is Empty/,
    "mtx load 2 0 (slot empty)");

like(run_err('mock/mtx', '-f', $mtx_state_file, 'unload', '5', '1'),
    qr/Unloading Data Transfer Element into Storage Element 5...source Element Address.*/,
    "mtx unload 5 1 (drive empty)");

like(run_err('mock/mtx', '-f', $mtx_state_file, 'unload', '3', '0'),
    qr/Storage Element 3 is Already Full/,
    "mtx unload 5 1 (slot full)");

ok(run('mock/mtx', '-f', $mtx_state_file, 'load', '1', '0'),
    "mtx load 1 0");

ok(-d "$vtape_root/drive0/data", "fake vfs drive loaded");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 6 Slots \( 1 Import/Export \)
Data Transfer Element 0:Full \(Storage Element 1 Loaded\):VolumeTag=023984
Data Transfer Element 1:Empty
      Storage Element 1:Empty
      Storage Element 2:Empty
      Storage Element 3:Full :VolumeTag=978344
      Storage Element 4:Empty
      Storage Element 5:Empty
      Storage Element 6 IMPORT/EXPORT:Empty},
    "mtx status shows results");

like(run_err('mock/mtx', '-f', $mtx_state_file, 'load', '3', '0'),
    qr/Drive 0 Full \(Storage Element 1 Loaded\)/,
    "mtx load 3 0 (drive full)");

ok(run('mock/mtx', '-f', $mtx_state_file, 'unload', '2', '0'),
    "mtx unload 2 0");

ok(! -d "$vtape_root/drive0/data", "fake vfs drive unloaded");
ok(-d "$vtape_root/slot2/data", "fake vfs slot re-populated");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 6 Slots \( 1 Import/Export \)
Data Transfer Element 0:Empty
Data Transfer Element 1:Empty
      Storage Element 1:Empty
      Storage Element 2:Full :VolumeTag=023984
      Storage Element 3:Full :VolumeTag=978344
      Storage Element 4:Empty
      Storage Element 5:Empty
      Storage Element 6 IMPORT/EXPORT:Empty},
    "mtx status shows results");

like(run_err('mock/mtx', '-f', $mtx_state_file, 'transfer', '1', '4'),
    qr/source Element Address 1 is Empty/,
    "mtx transfer 1 4 (source empty)");

like(run_err('mock/mtx', '-f', $mtx_state_file, 'transfer', '2', '3'),
    qr/destination Element Address 3 is Already Full/,
    "mtx transfer 1 4 (dest full)");

ok(run('mock/mtx', '-f', $mtx_state_file, 'transfer', '2', '4'),
    "mtx transfer 2 4");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 6 Slots \( 1 Import/Export \)
Data Transfer Element 0:Empty
Data Transfer Element 1:Empty
      Storage Element 1:Empty
      Storage Element 2:Empty
      Storage Element 3:Full :VolumeTag=978344
      Storage Element 4:Full :VolumeTag=023984
      Storage Element 5:Empty
      Storage Element 6 IMPORT/EXPORT:Empty},
    "mtx status shows results");

rmtree($vtape_root);

##
# Without barcodes, with track orig

$mtx_state_file = setup_mock_mtx (
	 barcodes => 0,
	 track_orig => 1,
	 num_slots => 5,
	 num_drives => 1,
	 first_slot => 1,
	 first_drive => 0,
	 loaded_slots => {
	     1 => '023984',
	     2 => '376524',
	     3 => '983754',
         },
       );

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:1 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Empty
      Storage Element 1:Full
      Storage Element 2:Full
      Storage Element 3:Full
      Storage Element 4:Empty
      Storage Element 5:Empty},
    "mtx status (-BARCODES, +TRACK ORIG)");

ok(run('mock/mtx', '-f', $mtx_state_file, 'load', '1', '0'),
    "mtx load 1 0");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:1 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Full \(Storage Element 1 Loaded\)
      Storage Element 1:Empty
      Storage Element 2:Full
      Storage Element 3:Full
      Storage Element 4:Empty
      Storage Element 5:Empty},
    "mtx status shows results");

##
# Without barcodes, without track orig

$mtx_state_file = setup_mock_mtx (
	 barcodes => 0,
	 track_orig => 0,
	 num_slots => 5,
	 num_drives => 1,
	 first_slot => 1,
	 first_drive => 0,
	 loaded_slots => {
	     1 => '023984',
	     2 => '376524',
	     3 => '983754',
         },
       );

ok(run('mock/mtx', '-f', $mtx_state_file, 'load', '1', '0'),
    "mtx load 1 0 (-BARCODES, -TRACK ORIG)");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:1 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Full
      Storage Element 1:Empty
      Storage Element 2:Full
      Storage Element 3:Full
      Storage Element 4:Empty
      Storage Element 5:Empty},
    "mtx status shows results");

##
# With barcodes, without track orig

$mtx_state_file = setup_mock_mtx (
	 barcodes => 1,
	 track_orig => 0,
	 num_slots => 5,
	 num_drives => 2,
	 first_slot => 1,
	 first_drive => 0,
	 loaded_slots => {
	     2 => '023984',
	     3 => '376524',
	     4 => '983754',
         },
       );

ok(run('mock/mtx', '-f', $mtx_state_file, 'load', '2', '1'),
    "mtx load 2 0 (+BARCODES, -TRACK ORIG)");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Empty
Data Transfer Element 1:Full :VolumeTag=023984
      Storage Element 1:Empty
      Storage Element 2:Empty
      Storage Element 3:Full :VolumeTag=376524
      Storage Element 4:Full :VolumeTag=983754
      Storage Element 5:Empty},
    "mtx status shows results");

ok(run('mock/mtx', '-f', $mtx_state_file, 'load', '4', '0'),
    "mtx load 2 0");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Full :VolumeTag=983754
Data Transfer Element 1:Full :VolumeTag=023984
      Storage Element 1:Empty
      Storage Element 2:Empty
      Storage Element 3:Full :VolumeTag=376524
      Storage Element 4:Empty
      Storage Element 5:Empty},
    "mtx status shows results");

##
# With barcodes, with weird track orig behavior

$mtx_state_file = setup_mock_mtx (
	 barcodes => 1,
	 track_orig => -1,
	 num_slots => 5,
	 num_drives => 2,
	 first_slot => 1,
	 first_drive => 0,
	 loaded_slots => {
	     2 => '023984',
	     3 => '376524',
	     4 => '983754',
         },
       );

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Empty
Data Transfer Element 1:Empty
      Storage Element 1:Empty
      Storage Element 2:Full :VolumeTag=023984
      Storage Element 3:Full :VolumeTag=376524
      Storage Element 4:Full :VolumeTag=983754
      Storage Element 5:Empty},
    "mtx status (+BARCODES, IMAGINED TRACK ORIG)");

ok(run('mock/mtx', '-f', $mtx_state_file, 'load', '2', '1'),
    "mtx load 2 0");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Empty
Data Transfer Element 1:Full \(Storage Element 1 Loaded\):VolumeTag=023984
      Storage Element 1:Empty
      Storage Element 2:Empty
      Storage Element 3:Full :VolumeTag=376524
      Storage Element 4:Full :VolumeTag=983754
      Storage Element 5:Empty},
    "mtx status shows results");

ok(run('mock/mtx', '-f', $mtx_state_file, 'load', '4', '0'),
    "mtx load 2 0");

like(run_get('mock/mtx', '-f', $mtx_state_file, 'status'),
    qr{  Storage Changer .*:2 Drives, 5 Slots \( 0 Import/Export \)
Data Transfer Element 0:Full \(Storage Element 1 Loaded\):VolumeTag=983754
Data Transfer Element 1:Full \(Storage Element 2 Loaded\):VolumeTag=023984
      Storage Element 1:Empty
      Storage Element 2:Empty
      Storage Element 3:Full :VolumeTag=376524
      Storage Element 4:Empty
      Storage Element 5:Empty},
    "mtx status shows results");
