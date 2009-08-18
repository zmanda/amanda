# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 17;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck::Run;
use Amanda::Xfer qw( :constants );
use Amanda::Device qw( :constants );
use Amanda::Header;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Paths;
use Amanda::Config;
use Amanda::Changer;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# initialize configuration for the device API
Amanda::Config::config_init(0, undef);

# exercise device source and destination
{
    my $RANDOM_SEED = 0xFACADE;
    my $xfer;

    my $quit_cb = make_cb(quit_cb => sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    });

    # set up vtapes
    my $testconf = Installcheck::Run::setup();
    $testconf->write();

    # set up a device for slot 1
    my $device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape(1));
    die("Could not open VFS device: " . $device->error())
	unless ($device->status() == $DEVICE_STATUS_SUCCESS);

    # write to it
    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'name'} = "installcheck";
    $hdr->{'disk'} = "/";
    $hdr->{'datestamp'} = "20080102030405";

    $device->finish();
    $device->start($ACCESS_WRITE, "TESTCONF01", "20080102030405");
    $device->start_file($hdr);

    $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024, $RANDOM_SEED),
	Amanda::Xfer::Dest::Device->new($device, $device->block_size() * 10),
    ]);

    $xfer->start($quit_cb);

    Amanda::MainLoop::run();
    pass("write to a device (completed succesfully; data may not be correct)");

    # finish up the file and device
    ok(!$device->in_file(), "not in_file");
    ok($device->finish(), "finish");

    # now turn around and read from it
    $device->start($ACCESS_READ, undef, undef);
    $device->seek_file(1);

    $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Device->new($device),
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    ]);

    $xfer->start($quit_cb);

    Amanda::MainLoop::run();
    pass("read from a device succeeded, too, and data was correct");
}

my $disk_cache_dir = "$Installcheck::TMP";
my $RANDOM_SEED = 0xFACADE;

# extra params:
#   cancel_after_partnum - after this partnum is completed, cancel the xfer
#   do_not_retry - do not retry a failed part - cancel the xfer instead
sub test_taper_dest {
    my ($src, $dest, $expected_messages, $msg_prefix, %params) = @_;
    my $xfer;
    my $device;

    # set up vtapes
    my $testconf = Installcheck::Run::setup();
    $testconf->write();

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'name'} = "installcheck";
    $hdr->{'disk'} = "/";
    $hdr->{'datestamp'} = "20080102030405";

    $xfer = Amanda::Xfer->new([ $src, $dest ]);

    my $vtape_num = 1;
    my @messages;
    my $start_new_part = sub {
	my ($successful, $eof, $partnum) = @_;

	if (exists $params{'cancel_after_partnum'}
		and $params{'cancel_after_partnum'} == $partnum) {
	    push @messages, "CANCEL";
	    $xfer->cancel();
	    return;
	}

	if (!$device || !$successful) {
	    # set up a device and start writing a part to it
	    $device->finish() if $device;
	    $device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($vtape_num++));
	    die("Could not open VFS device: " . $device->error())
		unless ($device->status() == $DEVICE_STATUS_SUCCESS);
	    $device->start($ACCESS_WRITE, "TESTCONF01", "20080102030405");
	    $device->property_set("MAX_VOLUME_USAGE", 1024*1024*2.5);
	}

	# bail out if we shouldn't retry this part
	if (!$successful and $params{'do_not_retry'}) {
	    push @messages, "NOT-RETRYING";
	    $xfer->cancel();
	    return;
	}

	if (!$eof) {
	    if ($successful) {
		$dest->start_part(0, $device, $hdr);
	    } else {
		$dest->start_part(1, $device, $hdr);
	    }
	}
    };

    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;

	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	} elsif ($msg->{'type'} == $XMSG_PART_DONE) {
	    push @messages, "PART-" . $msg->{'partnum'} . '-' . ($msg->{'successful'}? "OK" : "FAILED");
	    $start_new_part->($msg->{'successful'}, $msg->{'eof'}, $msg->{'partnum'});
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    push @messages, "DONE";
	} elsif ($msg->{'type'} == $XMSG_CANCEL) {
	    push @messages, "CANCELLED";
	} else {
	    push @messages, "$msg";
	}

	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    });

    Amanda::MainLoop::call_later(sub { $start_new_part->(1, 0, -1); });
    Amanda::MainLoop::run();

    use Data::Dumper;
    is_deeply([@messages],
	$expected_messages,
	"$msg_prefix: element produces the correct series of messages")
    or diag(Dumper([@messages]));
}

sub test_taper_source {
    my ($src, $dest, $files, $expected_messages) = @_;
    my $device;
    my @filenums;
    my @messages;
    my %subs;
    my $xfer;
    my $dev;

    $subs{'setup'} = sub {
	$xfer = Amanda::Xfer->new([ $src, $dest ]);

	$xfer->start($subs{'got_xmsg'});

	$subs{'load_slot'}->();
    };

    $subs{'load_slot'} = sub {
	if (!@$files) {
	    return $src->start_part(undef);
	    # (will trigger an XMSG_DONE; see below)
	}

	my $slot = shift @$files;
	@filenums = @{ shift @$files };

	$dev = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($slot));
	if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    die $dev->error_or_status();
	}
	if (!$dev->start($ACCESS_READ, undef, undef)) {
	    die $dev->error_or_status();
	}

	$subs{'seek_file'}->();
    };

    $subs{'seek_file'} = sub {
	if (!@filenums) {
	    return $subs{'load_slot'}->();
	}

	my $hdr = $dev->seek_file(shift @filenums);
	if (!$hdr) {
	    die $dev->error_or_status();
	}

	push @messages, "PART";

	$src->start_part($dev);
    };

    $subs{'got_xmsg'} = sub {
	my ($src, $msg, $xfer) = @_;

	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	} elsif ($msg->{'type'} == $XMSG_PART_DONE) {
	    push @messages, "KB-" . ($msg->{'size'}/1024);
	    $subs{'seek_file'}->();
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    push @messages, "DONE";
	} elsif ($msg->{'type'} == $XMSG_CANCEL) {
	    push @messages, "CANCELLED";
	}

	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    };

    Amanda::MainLoop::call_later($subs{'setup'});
    Amanda::MainLoop::run();

    is_deeply([@messages],
	$expected_messages,
	"files read back and verified successfully with Amanda::Xfer::Taper::Source")
    or diag(Dumper([@messages]));
}

my $holding_base = "$Installcheck::TMP/source-holding";
my $holding_file;
# create a sequence of holding chunks, each 2MB.
sub make_holding_files {
    my ($nchunks) = @_;
    my $block = 'a' x 32768;

    rmtree($holding_base);
    mkpath($holding_base);
    for (my $i = 0; $i < $nchunks; $i++) {
	my $filename = "$holding_base/file$i";
	open(my $fh, ">", "$filename");

	my $hdr = Amanda::Header->new();
	$hdr->{'type'} = ($i == 0)?
	    $Amanda::Header::F_DUMPFILE : $Amanda::Header::F_CONT_DUMPFILE;
	$hdr->{'datestamp'} = "20070102030405";
	$hdr->{'dumplevel'} = 0;
	$hdr->{'compressed'} = 1;
	$hdr->{'name'} = "localhost";
	$hdr->{'disk'} = "/home";
	$hdr->{'program'} = "INSTALLCHECK";
	if ($i != $nchunks-1) {
	    $hdr->{'cont_filename'} = "$holding_base/file" . ($i+1);
	}

	print $fh $hdr->to_string(32768,32768);

	for (my $b = 0; $b < 64; $b++) {
	    print $fh $block;
	}
	close($fh);
    }

    return "$holding_base/file0";
}

# run this test in each of a few different cache permutations
test_taper_dest(
    Amanda::Xfer::Source::Random->new(1024*1024*4.1, $RANDOM_SEED),
    Amanda::Xfer::Dest::Taper->new(128*1024, 1024*1024, 1, undef),
    [ "PART-1-OK", "PART-2-OK", "PART-3-FAILED",
      "PART-3-OK", "PART-4-OK", "PART-5-OK",
      "DONE" ],
    "mem cache");
test_taper_source(
    Amanda::Xfer::Source::Taper->new(),
    Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    [ 1 => [ 1, 2 ], 2 => [ 1, 2, 3 ], ],
    [
      'PART',
      'KB-1024',
      'PART',
      'KB-1024',
      'PART',
      'KB-1024',
      'PART',
      'KB-1024',
      'PART',
      'KB-102',
      'DONE'
    ]);

test_taper_dest(
    Amanda::Xfer::Source::Random->new(1024*1024*4.1, $RANDOM_SEED),
    Amanda::Xfer::Dest::Taper->new(128*1024, 1024*1024, 0, $disk_cache_dir),
    [ "PART-1-OK", "PART-2-OK", "PART-3-FAILED",
      "PART-3-OK", "PART-4-OK", "PART-5-OK",
      "DONE" ],
    "disk cache");
test_taper_source(
    Amanda::Xfer::Source::Taper->new(),
    Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    [ 1 => [ 1, 2 ], 2 => [ 1, 2, 3 ], ],
    [
      'PART',
      'KB-1024',
      'PART',
      'KB-1024',
      'PART',
      'KB-1024',
      'PART',
      'KB-1024',
      'PART',
      'KB-102',
      'DONE'
    ]);

test_taper_dest(
    Amanda::Xfer::Source::Random->new(1024*1024*2, $RANDOM_SEED),
    Amanda::Xfer::Dest::Taper->new(128*1024, 1024*1024, 0, undef),
    [ "PART-1-OK", "PART-2-OK", "PART-3-OK",
      "DONE" ],
    "no cache (no failed parts; exact multiple of part size)");
test_taper_source(
    Amanda::Xfer::Source::Taper->new(),
    Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    [ 1 => [ 1, 2, 3 ], ],
    [
      'PART',
      'KB-1024',
      'PART',
      'KB-1024',
      'PART',
      'KB-0',
      'DONE'
    ]);

test_taper_dest(
    Amanda::Xfer::Source::Random->new(1024*1024*2, $RANDOM_SEED),
    Amanda::Xfer::Dest::Taper->new(128*1024, 0, 0, undef),
    [ "PART-1-OK", "DONE" ],
    "no splitting (fits on volume)");
test_taper_source(
    Amanda::Xfer::Source::Taper->new(),
    Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    [ 1 => [ 1 ], ],
    [
      'PART',
      'KB-2048',
      'DONE'
    ]);

test_taper_dest(
    Amanda::Xfer::Source::Random->new(1024*1024*4.1, $RANDOM_SEED),
    Amanda::Xfer::Dest::Taper->new(128*1024, 0, 0, undef),
    [ "PART-1-FAILED", "NOT-RETRYING", "CANCELLED", "DONE" ],
    "no splitting (doesn't fit on volume -> fails)",
    do_not_retry => 1);

test_taper_dest(
    Amanda::Xfer::Source::Random->new(1024*1024*4.1, $RANDOM_SEED),
    Amanda::Xfer::Dest::Taper->new(128*1024, 1024*1024, 0, $disk_cache_dir),
    [ "PART-1-OK", "PART-2-OK", "PART-3-FAILED",
      "PART-3-OK", "PART-4-OK", "CANCEL",
      "CANCELLED", "DONE" ],
    "cancellation after success",
    cancel_after_partnum => 4);

# set up a few holding chunks and read from those
$holding_file = make_holding_files(3);
test_taper_dest(
    Amanda::Xfer::Source::Holding->new($holding_file),
    Amanda::Xfer::Dest::Taper->new(128*1024, 1024*1024, 0, undef),
    [ "PART-1-OK", "PART-2-OK", "PART-3-FAILED",
      "PART-3-OK", "PART-4-OK", "PART-5-FAILED",
      "PART-5-OK", "PART-6-OK", "PART-7-OK",
      "DONE" ],
    "Amanda::Xfer::Source::Holding acts as a source and supplies cache_inform");

##
# test the cache_inform method

sub test_taper_dest_cache_inform {
    my %params = @_;
    my $xfer;
    my $device;
    my $fh;
    my $part_size = 1024*1024;
    my $file_size = $part_size * 4 + 100 * 1024;
    my $cache_file = "$Installcheck::TMP/cache_file";

    # set up our "cache", cleverly using an Amanda::Xfer::Dest::Fd
    open($fh, ">", "$cache_file") or die("Could not open '$cache_file' for writing");
    $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new($file_size, $RANDOM_SEED),
	Amanda::Xfer::Dest::Fd->new(fileno($fh)),
    ]);

    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    });
    Amanda::MainLoop::run();
    close($fh);

    # create a list of holding chuunks, some slab-aligned, some part-aligned,
    # some not
    my @holding_chunks;
    if (!$params{'omit_chunks'}) {
	my $offset = 0;
	my $do_chunk = sub {
	    my ($break) = @_;
	    die unless $break > $offset;
	    push @holding_chunks, [ $cache_file, $offset, $break - $offset ];
	    $offset = $break;
	};
	$do_chunk->(277);
	$do_chunk->($part_size);
	$do_chunk->($part_size+128*1024);
	$do_chunk->($part_size*3);
	$do_chunk->($part_size*3+1024);
	$do_chunk->($part_size*3+1024*2);
	$do_chunk->($part_size*3+1024*3);
	$do_chunk->($part_size*4);
	$do_chunk->($part_size*4 + 77);
	$do_chunk->($file_size - 1);
	$do_chunk->($file_size);
    }

    # set up vtapes
    my $testconf = Installcheck::Run::setup();
    $testconf->write();

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'name'} = "installcheck";
    $hdr->{'disk'} = "/";
    $hdr->{'datestamp'} = "20080102030405";

    open($fh, "<", "$cache_file") or die("Could not open '$cache_file' for reading");
    my $dest = Amanda::Xfer::Dest::Taper->new(128*1024, 1024*1024, 0, undef);
    $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Fd->new(fileno($fh)),
	$dest,
    ]);

    my $vtape_num = 1;
    my $start_new_part = sub {
	my ($successful, $eof, $last_partnum) = @_;

	if (!$device || !$successful) {
	    # set up a device and start writing a part to it
	    $device->finish() if $device;
	    $device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($vtape_num++));
	    die("Could not open VFS device: " . $device->error())
		unless ($device->status() == $DEVICE_STATUS_SUCCESS);
	    $device->start($ACCESS_WRITE, "TESTCONF01", "20080102030405");
	    $device->property_set("MAX_VOLUME_USAGE", 1024*1024*2.5);
	}

	# feed enough chunks to cache_inform
	my $upto = ($last_partnum+2) * $part_size;
	while (@holding_chunks and $holding_chunks[0]->[1] < $upto) {
	    my ($filename, $offset, $length) = @{shift @holding_chunks};
	    $dest->cache_inform($filename, $offset, $length);
	}

	if (!$eof) {
	    if ($successful) {
		$dest->start_part(0, $device, $hdr);
	    } else {
		$dest->start_part(1, $device, $hdr);
	    }
	}
    };

    my @messages;
    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;

	if ($msg->{'type'} == $XMSG_ERROR) {
	    push @messages, "ERROR: $msg->{message}";
	} elsif ($msg->{'type'} == $XMSG_PART_DONE) {
	    push @messages, "PART-" . ($msg->{'successful'}? "OK" : "FAILED");
	    $start_new_part->($msg->{'successful'}, $msg->{'eof'}, $msg->{'partnum'});
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    push @messages, "DONE";
	} elsif ($msg->{'type'} == $XMSG_CANCEL) {
	    push @messages, "CANCELLED";
	} else {
	    push @messages, $msg->{'type'};
	}

	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    });

    Amanda::MainLoop::call_later(sub { $start_new_part->(1, 0, -1); });
    Amanda::MainLoop::run();

    unlink($cache_file);

    return @messages;
}

is_deeply([ test_taper_dest_cache_inform() ],
    [ "PART-OK", "PART-OK", "PART-FAILED",
      "PART-OK", "PART-OK", "PART-OK",
      "DONE" ],
    "cache_inform: element produces the correct series of messages");

is_deeply([ test_taper_dest_cache_inform(omit_chunks => 1) ],
    [ "PART-OK", "PART-OK", "PART-FAILED",
      "ERROR: Failed part was not cached; cannot retry", "CANCELLED",
      "DONE" ],
    "cache_inform: element produces the correct series of messages when a chunk is missing");

rmtree($holding_base);
