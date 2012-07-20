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

use Test::More tests => 16;
use strict;
use warnings;
use File::Path;
use Data::Dumper;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Config;
use Amanda::Holding;
use Amanda::Header;
use Amanda::Debug;
use Amanda::Config qw( :init );
use Amanda::Disklist;

# put the debug messages somewhere
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $holding1 = "$Installcheck::TMP/holding1";
my $holding2 = "$Installcheck::TMP/holding2";
my $holding3 = "$Installcheck::TMP/holding3";

# set up a demo holding disk
sub make_holding_file {
    my ($hdir, $ts, $host, $disk, $nchunks) = @_;

    my $dir = "$hdir/$ts";
    mkpath($dir);

    my $safe_disk = $disk;
    $safe_disk =~ tr{/}{_};
    my $base_filename = "$dir/$host.$safe_disk";

    for my $i (0 .. $nchunks-1) {
	my $chunk_filename = $base_filename;
	$chunk_filename .= ".$i" if ($i);

	my $hdr = Amanda::Header->new();
	$hdr->{'type'} = ($i == 0)? $Amanda::Header::F_DUMPFILE : $Amanda::Header::F_CONT_DUMPFILE;
	$hdr->{'datestamp'} = $ts;
	$hdr->{'dumplevel'} = 0;
	$hdr->{'name'} = $host;
	$hdr->{'disk'} = $disk;
	$hdr->{'program'} = "INSTALLCHECK";
	if ($i != $nchunks-1) {
	    $hdr->{'cont_filename'} = "$base_filename." . ($i+1);
	}

	open(my $fh, ">", $chunk_filename) or die("opening '$chunk_filename': $!");
	print $fh $hdr->to_string(32768,32768);
	print $fh "some data!\n";
	close($fh);
    }
}

sub make_holding {
    my @files = @_;

    rmtree($holding1);
    rmtree($holding2);
    for my $file (@files) {
	make_holding_file(@$file);
    }
}

my $testconf = Installcheck::Config->new();
$testconf->add_holdingdisk("holding1", [
    "directory", '"' . $holding1 . '"',
    "use", "10m",
]);
$testconf->add_holdingdisk("holding2", [
    "directory", '"' . $holding2 . '"',
    "use", "10m",
]);
# note: this holding disk is not active -- just a definition
$testconf->add_holdingdisk_def("holding3", [
    "directory", '"' . $holding3 . '"',
    "use", "10m",
]);
$testconf->add_dle("videoserver /video/a no-compress");
# note no /video/b
$testconf->add_dle("audio /usr no-compress");
$testconf->add_dle("audio /var no-compress");
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
$cfg_result = Amanda::Disklist::read_disklist();
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

# Let's get started!

make_holding(
    [ $holding1, '20070303000000', 'videoserver', '/video/a', 2 ],
    [ $holding1, '20070306123456', 'videoserver', '/video/a', 1 ],
    [ $holding1, '20070306123456', 'videoserver', '/video/b', 2 ],
    [ $holding2, '20070306123456', 'audio', '/var', 3 ],
    [ $holding2, '20070306123456', 'audio', '/usr', 1 ],
    [ $holding3, '20070303000000', 'olfactory', '/perfumes', 1 ],
    [ $holding3, '20070306123456', 'olfactory', '/stinky', 1 ],
);

is_deeply([ sort(+Amanda::Holding::disks()) ],
    [ sort($holding1, $holding2) ],
    "all active holding disks, but not inactive (defined but not used) disks");

is_deeply([ sort(+Amanda::Holding::files()) ],
    [ sort(
	"$holding1/20070303000000/videoserver._video_a",
	"$holding1/20070306123456/videoserver._video_a",
	"$holding1/20070306123456/videoserver._video_b",
	"$holding2/20070306123456/audio._usr",
	"$holding2/20070306123456/audio._var",
    ) ],
    "all files");

is_deeply([ sort(+Amanda::Holding::file_chunks("$holding2/20070306123456/audio._var")) ],
    [ sort(
	"$holding2/20070306123456/audio._var",
	"$holding2/20070306123456/audio._var.1",
	"$holding2/20070306123456/audio._var.2",
    ) ],
    "chunks for a chunked file");

is(Amanda::Holding::file_size("$holding2/20070306123456/audio._usr", 1), 1,
    "size of a single-chunk file, without headers");
is(Amanda::Holding::file_size("$holding2/20070306123456/audio._usr", 0), 32+1,
    "size of a single-chunk file, with headers");
is(Amanda::Holding::file_size("$holding2/20070306123456/audio._var", 1), 3,
    "size of a chunked file, without headers");
is(Amanda::Holding::file_size("$holding2/20070306123456/audio._var", 0), 32*3+1*3,
    "size of a chunked file, with headers");

my $hdr = Amanda::Holding::get_header("$holding2/20070306123456/audio._usr");
is_deeply([ $hdr->{'name'}, $hdr->{'disk'} ],
	  [ 'audio', '/usr' ],
	  "get_header gives a reasonable header");

is_deeply([ Amanda::Holding::get_all_datestamps() ],
	  [ sort("20070303000000", "20070306123456") ],
	  "get_all_datestamps");

is_deeply([ sort(+Amanda::Holding::get_files_for_flush("023985")) ],
	  [ sort() ],
	  "get_files_for_flush with no matching datestamps returns no files");
is_deeply([ Amanda::Holding::get_files_for_flush("20070306123456") ],
	  [ sort(
		"$holding2/20070306123456/audio._usr",
		"$holding2/20070306123456/audio._var",
		"$holding1/20070306123456/videoserver._video_a",
	  )],
	  "get_files_for_flush gets only files listed in disklist (no _video_b)");
is_deeply([ Amanda::Holding::get_files_for_flush() ],
	  [ sort(
		"$holding1/20070303000000/videoserver._video_a",
		"$holding2/20070306123456/audio._usr",
		"$holding2/20070306123456/audio._var",
		"$holding1/20070306123456/videoserver._video_a",
	  )],
	  "get_files_for_flush with no datestamps returns all files");

ok(Amanda::Holding::file_unlink("$holding2/20070306123456/audio._var"),
    "unlink a holding file");
ok(!-f "$holding2/20070306123456/audio._var", "..first chunk gone");
ok(!-f "$holding2/20070306123456/audio._var.1", "..second chunk gone");
ok(!-f "$holding2/20070306123456/audio._var.2", "..third chunk gone");

rmtree($holding1);
rmtree($holding2);
rmtree($holding3);
