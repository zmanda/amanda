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

use Test::More tests => 46;
use File::Path;
use Data::Dumper;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Run;
use Installcheck::Mock;
use Amanda::Xfer qw( :constants );
use Amanda::Header;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Paths;
use Amanda::Config;
use Amanda::Constants;

# get Amanda::Device only when we're building for server
BEGIN {
    use Amanda::Util;
    if (Amanda::Util::built_with_component("server")) {
	eval "use Amanda::Device;";
	die $@ if $@;
    }
}

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

{
    my $RANDOM_SEED = 0xD00D;

    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024, $RANDOM_SEED),
	Amanda::Xfer::Filter::Xor->new(0), # key of 0 -> no change, so random seeds match
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    ]);

    pass("Creating a transfer doesn't crash"); # hey, it's a start..

    my $got_msg = "(not received)";
    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	}
	if ($msg->{type} == $XMSG_INFO) {
	    $got_msg = $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    Amanda::MainLoop::quit();
	}
    });
    Amanda::MainLoop::run();
    pass("A simple transfer runs to completion");
    is($got_msg, "Is this thing on?",
	"XMSG_INFO from Amanda::Xfer::Dest::Null has correct message");
}

{
    my $RANDOM_SEED = 0xDEADBEEF;

    my $xfer1 = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024, $RANDOM_SEED),
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    ]);
    my $xfer2 = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024*3, $RANDOM_SEED),
	Amanda::Xfer::Filter::Xor->new(0xf0),
	Amanda::Xfer::Filter::Xor->new(0xf0),
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    ]);

    my $cb = sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    if  ($xfer1->get_status() == $Amanda::Xfer::XFER_DONE
	     and $xfer2->get_status() == $Amanda::Xfer::XFER_DONE) {
		Amanda::MainLoop::quit();
	    }
	}
    };

    $xfer1->start($cb);
    $xfer2->start($cb);
}
# let the already-started transfers go out of scope before they 
# complete, as a memory management test..
Amanda::MainLoop::run();
pass("Two simultaneous transfers run to completion");

{
    my $RANDOM_SEED = 0xD0DEEDAA;
    my @elts;

    # note that, because the Xor filter is flexible, assembling
    # long pipelines can take an exponentially long time.  A 10-elt
    # pipeline exercises the linking algorithm without wasting
    # too many CPU cycles

    push @elts, Amanda::Xfer::Source::Random->new(1024*1024, $RANDOM_SEED);
    for my $i (1 .. 4) {
	push @elts, Amanda::Xfer::Filter::Xor->new($i);
	push @elts, Amanda::Xfer::Filter::Xor->new($i);
    }
    push @elts, Amanda::Xfer::Dest::Null->new($RANDOM_SEED);
    my $xfer = Amanda::Xfer->new(\@elts);

    my $cb = sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    Amanda::MainLoop::quit();
	}
    };

    $xfer->start($cb);

    Amanda::MainLoop::run();
    pass("One 10-element transfer runs to completion");
}


{
    my $read_filename = "$Installcheck::TMP/xfer-junk-src.tmp";
    my $write_filename = "$Installcheck::TMP/xfer-junk-dest.tmp";
    my ($rfh, $wfh);

    mkdir($Installcheck::TMP) unless (-e $Installcheck::TMP);

    # fill the file with some stuff
    open($wfh, ">", $read_filename) or die("Could not open '$read_filename' for writing");
    for my $i (1 .. 100) { print $wfh "line $i\n"; }
    close($wfh);

    open($rfh, "<", $read_filename) or die("Could not open '$read_filename' for reading");
    open($wfh, ">", "$write_filename") or die("Could not open '$write_filename' for writing");

    # now run a transfer out of it
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Fd->new(fileno($rfh)),
	Amanda::Xfer::Filter::Xor->new(0xde),
	Amanda::Xfer::Filter::Xor->new(0xde),
	Amanda::Xfer::Dest::Fd->new(fileno($wfh)),
    ]);

    my $cb = sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    Amanda::MainLoop::quit();
	}
    };

    $xfer->start($cb);

    Amanda::MainLoop::run();

    close($wfh);
    close($rfh);

    # now verify the file contents are identical
    open($rfh, "<", $read_filename);
    my $src = do { local $/; <$rfh> };

    open($rfh, "<", $write_filename);
    my $dest = do { local $/; <$rfh> };

    is($src, $dest, "Source::Fd and Dest::Fd read and write files");

    unlink($read_filename);
    unlink($write_filename);
}

{
    my $RANDOM_SEED = 0x5EAF00D;

    # build a transfer that will keep going forever
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(0, $RANDOM_SEED),
	Amanda::Xfer::Filter::Xor->new(14),
	Amanda::Xfer::Filter::Xor->new(14),
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    ]);

    my $got_timeout = 0;
    Amanda::MainLoop::timeout_source(200)->set_callback(sub {
	my ($src) = @_;
	$got_timeout = 1;
	$src->remove();
	$xfer->cancel();
    });
    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    Amanda::MainLoop::quit();
	}
    });
    Amanda::MainLoop::run();
    ok($got_timeout, "A neverending transfer finishes after being cancelled");
    # (note that this does not test all of the cancellation possibilities)
}

{
    # build a transfer that will write to a read-only fd
    my $read_filename = "$Installcheck::TMP/xfer-junk-src.tmp";
    my $rfh;

    # create the file
    open($rfh, ">", $read_filename) or die("Could not open '$read_filename' for writing");

    # open it for reading
    open($rfh, "<", $read_filename) or die("Could not open '$read_filename' for reading");;

    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(0, 1),
	Amanda::Xfer::Dest::Fd->new(fileno($rfh)),
    ]);

    my $got_error = 0;
    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    $got_error = 1;
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    Amanda::MainLoop::quit();
	}
    });
    Amanda::MainLoop::run();
    ok($got_error, "A transfer with an error cancels itself after sending an error");

    unlink($read_filename);
}

# test the Process filter
{
    my $RANDOM_SEED = 0xD00D;

    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024, $RANDOM_SEED),
	Amanda::Xfer::Filter::Process->new(
	    [ $Amanda::Constants::COMPRESS_PATH, $Amanda::Constants::COMPRESS_BEST_OPT ], 0),
	Amanda::Xfer::Filter::Process->new(
	    [ $Amanda::Constants::UNCOMPRESS_PATH, $Amanda::Constants::UNCOMPRESS_OPT ], 0),
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
    ]);

    $xfer->get_source()->set_callback(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    $xfer->start();
    Amanda::MainLoop::run();
    pass("compress | uncompress gets back the original stream");
}

{
    my $RANDOM_SEED = 0x5EAF00D;

    # build a transfer that will keep going forever, using a source that
    # cannot produce an EOF, so Filter::Process is forced to kill the
    # compress process

    open(my $zerofd, "<", "/dev/zero")
	or die("could not open /dev/zero: $!");
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Fd->new($zerofd),
	Amanda::Xfer::Filter::Process->new(
	    [ $Amanda::Constants::COMPRESS_PATH, $Amanda::Constants::COMPRESS_BEST_OPT ], 0),
	Amanda::Xfer::Dest::Null->new(0),
    ]);

    my $got_timeout = 0;
    Amanda::MainLoop::timeout_source(200)->set_callback(sub {
	my ($src) = @_;
	$got_timeout = 1;
	$src->remove();
	$xfer->cancel();
    });
    $xfer->get_source()->set_callback(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    $xfer->start();
    Amanda::MainLoop::run();
    ok($got_timeout, "Amanda::Xfer::Filter::Process can be cancelled");
    # (note that this does not test all of the cancellation possibilities)
}

# Test Amanda::Xfer::Dest::Buffer
{
    my $dest = Amanda::Xfer::Dest::Buffer->new(1025);
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Pattern->new(1024, "ABCDEFGH"),
	$dest,
    ]);

    $xfer->get_source()->set_callback(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    $xfer->start();
    Amanda::MainLoop::run();

    is($dest->get(), 'ABCDEFGH' x 128,
	"buffer captures the right bytes");
}

# Test that Amanda::Xfer::Dest::Buffer terminates an xfer early
{
    my $dest = Amanda::Xfer::Dest::Buffer->new(100);
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Pattern->new(1024, "ABCDEFGH"),
	$dest,
    ]);

    my $got_err = 0;
    $xfer->get_source()->set_callback(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    $got_err = 1;
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    $xfer->start();
    Amanda::MainLoop::run();

    ok($got_err, "buffer stops the xfer if it doesn't have space");
}

SKIP: {
    skip "not built with server", 25 unless Amanda::Util::built_with_component("server");

    my $disk_cache_dir = "$Installcheck::TMP";
    my $RANDOM_SEED = 0xFACADE;

    # exercise device source and destination
    {
	my $RANDOM_SEED = 0xFACADE;
	my $xfer;

	my $quit_cb = make_cb(quit_cb => sub {
	    my ($src, $msg, $xfer) = @_;
	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		Amanda::MainLoop::quit();
	    }
	});

	# set up vtapes
	my $testconf = Installcheck::Run::setup();
	$testconf->write();

	# set up a device for slot 1
	my $device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape(1));
	die("Could not open VFS device: " . $device->error())
	    unless ($device->status() == $Amanda::Device::DEVICE_STATUS_SUCCESS);

	# write to it
	my $hdr = Amanda::Header->new();
	$hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
	$hdr->{'name'} = "installcheck";
	$hdr->{'disk'} = "/";
	$hdr->{'datestamp'} = "20080102030405";
	$hdr->{'program'} = "INSTALLCHECK";

	$device->finish();
	$device->start($Amanda::Device::ACCESS_WRITE, "TESTCONF01", "20080102030405");
	$device->start_file($hdr);

	$xfer = Amanda::Xfer->new([
	    Amanda::Xfer::Source::Random->new(1024*1024, $RANDOM_SEED),
	    Amanda::Xfer::Dest::Device->new($device, 0),
	]);

	$xfer->start($quit_cb);

	Amanda::MainLoop::run();
	pass("write to a device (completed succesfully; data may not be correct)");

	# finish up the file and device
	ok(!$device->in_file(), "not in_file");
	ok($device->finish(), "finish");

	# now turn around and read from it
	$device->start($Amanda::Device::ACCESS_READ, undef, undef);
	$device->seek_file(1);

	$xfer = Amanda::Xfer->new([
	    Amanda::Xfer::Source::Device->new($device),
	    Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	]);

	$xfer->start($quit_cb);

	Amanda::MainLoop::run();
	pass("read from a device succeeded, too, and data was correct");
    }

    # extra params:
    #   cancel_after_partnum - after this partnum is completed, cancel the xfer
    #   do_not_retry - do not retry a failed part - cancel the xfer instead
    sub test_taper_dest {
	my ($src, $dest_sub, $expected_messages, $msg_prefix, %params) = @_;
	my $xfer;
	my $device;
	my $vtape_num = 1;
	my @messages;

	# set up vtapes
	my $testconf = Installcheck::Run::setup();
	$testconf->write();

	my $hdr = Amanda::Header->new();
	$hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
	$hdr->{'name'} = "installcheck";
	$hdr->{'disk'} = "/";
	$hdr->{'datestamp'} = "20080102030405";
	$hdr->{'program'} = "INSTALLCHECK";

	# set up a device for the taper dest
	$device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($vtape_num++));
	die("Could not open VFS device: " . $device->error())
	    unless ($device->status() == $Amanda::Device::DEVICE_STATUS_SUCCESS);
	$device->property_set("MAX_VOLUME_USAGE", 1024*1024*2.5);
	$device->property_set("LEOM", $params{'disable_leom'}? 0 : 1);
	$device->start($Amanda::Device::ACCESS_WRITE, "TESTCONF01", "20080102030405");
	my $dest = $dest_sub->($device);

	# and create the xfer
	$xfer = Amanda::Xfer->new([ $src, $dest ]);

	my $start_new_part = sub {
	    my ($successful, $eof, $partnum, $eom) = @_;

	    if (exists $params{'cancel_after_partnum'}
		    and $params{'cancel_after_partnum'} == $partnum) {
		push @messages, "CANCEL";
		$xfer->cancel();
		return;
	    }

	    if (!$device || $eom) {
		# set up a device and start writing a part to it
		$device->finish() if $device;
		$device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($vtape_num++));
		die("Could not open VFS device: " . $device->error())
		    unless ($device->status() == $Amanda::Device::DEVICE_STATUS_SUCCESS);
		$dest->use_device($device);
		$device->property_set("LEOM", $params{'disable_leom'}? 0 : 1);
		$device->property_set("MAX_VOLUME_USAGE", 1024*1024*2.5);
		$device->start($Amanda::Device::ACCESS_WRITE, "TESTCONF01", "20080102030405");
	    }

	    # bail out if we shouldn't retry this part
	    if (!$successful and $params{'do_not_retry'}) {
		push @messages, "NOT-RETRYING";
		$xfer->cancel();
		return;
	    }

	    if (!$eof) {
		if ($successful) {
		    $dest->start_part(0, $hdr);
		} else {
		    $dest->start_part(1, $hdr);
		}
	    }
	};

	$xfer->start(sub {
	    my ($src, $msg, $xfer) = @_;

	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_PART_DONE) {
		push @messages, "PART-" . $msg->{'partnum'} . '-' . ($msg->{'successful'}? "OK" : "FAILED");
		push @messages, "EOM" if $msg->{'eom'};
		$start_new_part->($msg->{'successful'}, $msg->{'eof'}, $msg->{'partnum'}, $msg->{'eom'});
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		push @messages, "DONE";
		Amanda::MainLoop::quit();
	    } elsif ($msg->{'type'} == $XMSG_CANCEL) {
		push @messages, "CANCELLED";
	    } else {
		push @messages, "$msg";
	    }
	});

	Amanda::MainLoop::call_later(sub { $start_new_part->(1, 0, -1); });
	Amanda::MainLoop::run();

	is_deeply([@messages],
	    $expected_messages,
	    "$msg_prefix: element produces the correct series of messages")
	or diag(Dumper([@messages]));
    }

    sub run_recovery_source {
	my ($dest, $files, $expected_messages, $finished_cb) = @_;
	my $device;
	my @filenums;
	my @messages;
	my $xfer;
	my $dev;
	my $src;

	my $steps = define_steps
	    cb_ref => \$finished_cb;

	step setup => sub {
	    # we need a device up front, so sneak a peek into @$files
	    $dev = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($files->[0]));
	    $src = Amanda::Xfer::Source::Recovery->new($dev);
	    $xfer = Amanda::Xfer->new([ $src, $dest ]);

	    $xfer->start($steps->{'got_xmsg'});
	    # got_xmsg will call got_ready when the element is ready
	};

	step got_ready => sub {
	    $steps->{'load_slot'}->();
	};

	step load_slot => sub {
	    if (!@$files) {
		return $src->start_part(undef);
		# (will trigger an XMSG_DONE; see below)
	    }

	    my $slot = shift @$files;
	    @filenums = @{ shift @$files };

	    $dev = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($slot));
	    if ($dev->status != $Amanda::Device::DEVICE_STATUS_SUCCESS) {
		die $dev->error_or_status();
	    }

	    $src->use_device($dev);

	    if (!$dev->start($Amanda::Device::ACCESS_READ, undef, undef)) {
		die $dev->error_or_status();
	    }

	    $steps->{'seek_file'}->();
	};

	step seek_file => sub {
	    if (!@filenums) {
		return $steps->{'load_slot'}->();
	    }

	    my $hdr = $dev->seek_file(shift @filenums);
	    if (!$hdr) {
		die $dev->error_or_status();
	    }

	    push @messages, "PART";

	    $src->start_part($dev);
	};

	step got_xmsg => sub {
	    my ($src, $msg, $xfer) = @_;

	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    } elsif ($msg->{'type'} == $XMSG_PART_DONE) {
		push @messages, "KB-" . ($msg->{'size'}/1024);
		$steps->{'seek_file'}->();
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		push @messages, "DONE";
		$steps->{'quit'}->();
	    } elsif ($msg->{'type'} == $XMSG_READY) {
		push @messages, "READY";
		$steps->{'got_ready'}->();
	    } elsif ($msg->{'type'} == $XMSG_CANCEL) {
		push @messages, "CANCELLED";
	    }
	};

	step quit => sub {
	    is_deeply([@messages],
		$expected_messages,
		"files read back and verified successfully with Amanda::Xfer::Recovery::Source")
	    or diag(Dumper([@messages]));

	    $finished_cb->();
	};
    }

    sub test_recovery_source {
	run_recovery_source(@_, \&Amanda::MainLoop::quit);
	Amanda::MainLoop::run();
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

    # first, test the simpler Splitter class
    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1951, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Splitter->new($first_dev, 128*1024,
						     520*1024, 0);
	},
	[ "PART-1-OK", "PART-2-OK", "PART-3-OK", "PART-4-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Splitter - simple splitting");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1, 2, 3, 4 ], ],
	[
	  'READY',
	  'PART',
	  'KB-544',
	  'PART',
	  'KB-544',
	  'PART',
	  'KB-544',
	  'PART',
	  'KB-319',
	  'DONE'
	]);

    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1024*3.1, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Splitter->new($first_dev, 128*1024,
						     1024*1024, 0);
	},
	[ "PART-1-OK", "PART-2-OK", "PART-3-OK", "EOM",
	  "PART-4-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Splitter - splitting and spanning with LEOM");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1, 2, 3 ], 2 => [ 1, ], ],
	[
	  'READY',
	  'PART',
	  'KB-1024',
	  'PART',
	  'KB-1024',
	  'PART',
	  'KB-288',
	  'PART',
	  'KB-838',
	  'DONE'
	]);

    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1024*1.5, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Splitter->new($first_dev, 128*1024,
						     0, 0);
	},
	[ "PART-1-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Splitter - no splitting");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1, ], ],
	[
	  'READY',
	  'PART',
	  'KB-1536',
	  'DONE'
	]);

    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1024*3.1, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Splitter->new($first_dev, 128*1024,
						     2368*1024, 0);
	},
	[ "PART-1-OK", "PART-2-OK", "EOM",
	  "PART-3-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Splitter - LEOM hits in file 2 header");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1, 2 ], 2 => [ 1, ], ],
	[
	  'READY',
	  'PART',
	  'KB-2368',
	  'PART',
	  'KB-0', # this wouldn't be in the catalog, but it's on the vtape
	  'PART',
	  'KB-806',
	  'DONE'
	]);

    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1024*3.1, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Splitter->new($first_dev, 128*1024,
						     2368*1024, 0);
	},
	[ "PART-1-OK", "PART-2-FAILED", "EOM",
	  "NOT-RETRYING", "CANCELLED", "DONE" ],
	"Amanda::Xfer::Dest::Taper::Splitter - LEOM fails, PEOM => failure",
	disable_leom => 1, do_not_retry => 1);

    # run A::X::Dest::Taper::Cacher test in each of a few different cache permutations
    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1024*4.1, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Cacher->new($first_dev, 128*1024,
						     1024*1024, 1, undef),
	},
	[ "PART-1-OK", "PART-2-OK", "PART-3-FAILED", "EOM",
	  "PART-3-OK", "PART-4-OK", "PART-5-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Cacher - mem cache");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1, 2 ], 2 => [ 1, 2, 3 ], ],
	[
	  'READY',
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
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Cacher->new($first_dev, 128*1024,
					      1024*1024, 0, $disk_cache_dir),
	},
	[ "PART-1-OK", "PART-2-OK", "PART-3-FAILED", "EOM",
	  "PART-3-OK", "PART-4-OK", "PART-5-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Cacher - disk cache");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1, 2 ], 2 => [ 1, 2, 3 ], ],
	[
	  'READY',
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
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Cacher->new($first_dev, 128*1024,
						    1024*1024, 0, undef),
	},
	[ "PART-1-OK", "PART-2-OK", "PART-3-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Cacher - no cache (no failed parts; exact multiple of part size)");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1, 2, 3 ], ],
	[
	  'READY',
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
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Cacher->new($first_dev, 128*1024, 0, 0, undef),
	},
	[ "PART-1-OK", "DONE" ],
	"Amanda::Xfer::Dest::Taper::Cacher - no splitting (fits on volume)");
    test_recovery_source(
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED),
	[ 1 => [ 1 ], ],
	[
	  'READY',
	  'PART',
	  'KB-2048',
	  'DONE'
	]);

    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1024*4.1, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Cacher->new($first_dev, 128*1024, 0, 0, undef),
	},
	[ "PART-1-FAILED", "EOM",
	  "NOT-RETRYING", "CANCELLED", "DONE" ],
	"Amanda::Xfer::Dest::Taper::Cacher - no splitting (doesn't fit on volume -> fails)",
	do_not_retry => 1);

    test_taper_dest(
	Amanda::Xfer::Source::Random->new(1024*1024*4.1, $RANDOM_SEED),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Cacher->new($first_dev, 128*1024,
					    1024*1024, 0, $disk_cache_dir),
	},
	[ "PART-1-OK", "PART-2-OK", "PART-3-FAILED", "EOM",
	  "PART-3-OK", "PART-4-OK", "CANCEL",
	  "CANCELLED", "DONE" ],
	"Amanda::Xfer::Dest::Taper::Cacher - cancellation after success",
	cancel_after_partnum => 4);

    # set up a few holding chunks and read from those

    $holding_file = make_holding_files(3);

    test_taper_dest(
	Amanda::Xfer::Source::Holding->new($holding_file),
	sub {
	    my ($first_dev) = @_;
	    Amanda::Xfer::Dest::Taper::Splitter->new($first_dev, 128*1024,
					    1024*1024, 1);
	},
	[ "PART-1-OK", "PART-2-OK", "PART-3-FAILED", "EOM",
	  "PART-3-OK", "PART-4-OK", "PART-5-FAILED", "EOM",
	  "PART-5-OK", "PART-6-OK", "PART-7-OK",
	  "DONE" ],
	"Amanda::Xfer::Dest::Taper::Splitter - Amanda::Xfer::Source::Holding "
	. "acts as a source and supplies cache_inform",
	disable_leom => 1);

    ##
    # test the cache_inform method

    sub test_taper_dest_splitter_cache_inform {
	my %params = @_;
	my $xfer;
	my $device;
	my $fh;
	my $part_size = 1024*1024;
	my $file_size = $part_size * 4 + 100 * 1024;
	my $cache_file = "$Installcheck::TMP/cache_file";
	my $vtape_num = 1;

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
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		Amanda::MainLoop::quit();
	    }
	});
	Amanda::MainLoop::run();
	close($fh);

	# create a list of holding chuunks, some slab-aligned, some part-aligned,
	# some not
	my @holding_chunks;
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

	# set up vtapes
	my $testconf = Installcheck::Run::setup();
	$testconf->write();

	my $hdr = Amanda::Header->new();
	$hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
	$hdr->{'name'} = "installcheck";
	$hdr->{'disk'} = "/";
	$hdr->{'datestamp'} = "20080102030405";
	$hdr->{'program'} = "INSTALLCHECK";

	# set up the cache file
	open($fh, "<", "$cache_file") or die("Could not open '$cache_file' for reading");

	# set up a device for writing
	$device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($vtape_num++));
	die("Could not open VFS device: " . $device->error())
	    unless ($device->status() == $Amanda::Device::DEVICE_STATUS_SUCCESS);
	$device->property_set("MAX_VOLUME_USAGE", 1024*1024*2.5);
	$device->property_set("LEOM", 0);
	$device->start($Amanda::Device::ACCESS_WRITE, "TESTCONF01", "20080102030405");

	my $dest = Amanda::Xfer::Dest::Taper::Splitter->new($device, 128*1024,
						    1024*1024, 1);
	$xfer = Amanda::Xfer->new([
	    Amanda::Xfer::Source::Fd->new(fileno($fh)),
	    $dest,
	]);

	my $start_new_part = sub {
	    my ($successful, $eof, $last_partnum) = @_;

	    if (!$device || !$successful) {
		# set up a device and start writing a part to it
		$device->finish() if $device;
		$device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape($vtape_num++));
		die("Could not open VFS device: " . $device->error())
		    unless ($device->status() == $Amanda::Device::DEVICE_STATUS_SUCCESS);
		$dest->use_device($device);
		$device->property_set("LEOM", 0);
		$device->property_set("MAX_VOLUME_USAGE", 1024*1024*2.5);
		$device->start($Amanda::Device::ACCESS_WRITE, "TESTCONF01", "20080102030405");
	    }

	    # feed enough chunks to cache_inform
	    my $upto = ($last_partnum+2) * $part_size;
	    while (@holding_chunks and $holding_chunks[0]->[1] < $upto) {
		my ($filename, $offset, $length) = @{shift @holding_chunks};
		$dest->cache_inform($filename, $offset, $length);
	    }

	    if (!$eof) {
		if ($successful) {
		    $dest->start_part(0, $hdr);
		} else {
		    $dest->start_part(1, $hdr);
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
		Amanda::MainLoop::quit();
	    } elsif ($msg->{'type'} == $XMSG_CANCEL) {
		push @messages, "CANCELLED";
	    } else {
		push @messages, $msg->{'type'};
	    }
	});

	Amanda::MainLoop::call_later(sub { $start_new_part->(1, 0, -1); });
	Amanda::MainLoop::run();

	unlink($cache_file);

	return @messages;
    }

    is_deeply([ test_taper_dest_splitter_cache_inform() ],
	[ "PART-OK", "PART-OK", "PART-FAILED",
	  "PART-OK", "PART-OK", "PART-OK",
	  "DONE" ],
	"cache_inform: splitter element produces the correct series of messages");

    rmtree($holding_base);
}

# test Amanda::Xfer::Dest::Taper::DirectTCP; do it twice, once with a cancellation
SKIP: {
    skip "not built with ndmp and server", 3 unless
	Amanda::Util::built_with_component("ndmp") and Amanda::Util::built_with_component("server");

    my $RANDOM_SEED = 0xFACADE;

    # make XDT output fairly verbose
    $Amanda::Config::debug_taper = 2;

    my $ndmp = Installcheck::Mock::NdmpServer->new();
    my $ndmp_port = $ndmp->{'port'};
    my $drive = $ndmp->{'drive'};

    my $mkdevice = sub {
	my $dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port\@$drive");
	die "can't create device" unless $dev->status() == $Amanda::Device::DEVICE_STATUS_SUCCESS;
	$dev->property_set("verbose", 1) or die "can't set VERBOSE";
	$dev->property_set("ndmp_username", "ndmp") or die "can't set username";
	$dev->property_set("ndmp_password", "ndmp") or die "can't set password";

	return $dev;
    };

    my $hdr = Amanda::Header->new();
    $hdr->{'type'} = $Amanda::Header::F_DUMPFILE;
    $hdr->{'name'} = "installcheck";
    $hdr->{'disk'} = "/";
    $hdr->{'datestamp'} = "20080102030405";
    $hdr->{'program'} = "INSTALLCHECK";

    for my $do_cancel (0, 'later', 'in_setup') {
	my $dev;
	my $xfer;
	my @messages;

	# make a starting device
	$dev = $mkdevice->();

	# and create the xfer
	my $src = Amanda::Xfer::Source::Random->new(32768*34-7, $RANDOM_SEED);
	# note we ask for slightly less than 15 blocks; the dest should round up
	my $dest = Amanda::Xfer::Dest::Taper::DirectTCP->new($dev, 32768*16-99);
	$xfer = Amanda::Xfer->new([ $src, $dest ]);

	my $start_new_part; # forward declaration
	my $xmsg_cb = sub {
	    my ($src, $msg, $xfer) = @_;

	    if ($msg->{'type'} == $XMSG_ERROR) {
		# if this is an expected error, don't die
		if ($do_cancel eq 'in_setup' and $msg->{'message'} =~ /operation not supported/) {
		    push @messages, "ERROR";
		} else {
		    die $msg->{'elt'} . " failed: " . $msg->{'message'};
		}
	    } elsif ($msg->{'type'} == $XMSG_READY) {
		push @messages, "READY";

		# get ourselves a new (albeit identical) device, just to prove that the connections
		# are a little bit portable
		$dev->finish();
		$dev = $mkdevice->();
		$dest->use_device($dev);
		$dev->start($Amanda::Device::ACCESS_WRITE, "TESTCONF02", "20080102030406");

		$start_new_part->(1, 0); # start first part
	    } elsif ($msg->{'type'} == $XMSG_PART_DONE) {
		push @messages, "PART-" . $msg->{'partnum'} . '-' . ($msg->{'successful'}? "OK" : "FAILED");
		if ($do_cancel and $msg->{'partnum'} == 2) {
		    $xfer->cancel();
		} else {
		    $start_new_part->($msg->{'successful'}, $msg->{'eof'});
		}
	    } elsif ($msg->{'type'} == $XMSG_DONE) {
		push @messages, "DONE";
		Amanda::MainLoop::quit();
	    } elsif ($msg->{'type'} == $XMSG_CANCEL) {
		push @messages, "CANCELLED";
	    } else {
		push @messages, "$msg";
	    }
	};

	# trigger an error in the xfer dest's setup method by putting the device
	# in an error state.  NDMP devices do not support append, so starting in
	# append mode should trigger the failure.
	if ($do_cancel eq 'in_setup') {
	    if ($dev->start($Amanda::Device::ACCESS_APPEND, "MYLABEL", undef)) {
		die "successfully started NDMP device in ACCESS_APPEND?!";
	    }
	}

	$xfer->start($xmsg_cb);

	$start_new_part = sub {
	    my ($successful, $eof) = @_;

	    die "this dest shouldn't have unsuccessful parts" unless $successful;

	    if (!$eof) {
		$dest->start_part(0, $hdr);
	    }
	};

	Amanda::MainLoop::run();

	$dev->finish();

	if (!$do_cancel) {
	    is_deeply([@messages],
		[ 'READY', 'PART-1-OK', 'PART-2-OK', 'PART-3-OK', 'DONE' ],
		"Amanda::Xfer::Dest::Taper::DirectTCP element produces the correct series of messages")
	    or diag(Dumper([@messages]));
	} elsif ($do_cancel eq 'in_setup') {
	    is_deeply([@messages],
		[ 'ERROR', 'CANCELLED', 'DONE' ],
		"Amanda::Xfer::Dest::Taper::DirectTCP element produces the correct series of messages when cancelled during setup")
	    or diag(Dumper([@messages]));
	} else {
	    is_deeply([@messages],
		[ 'READY', 'PART-1-OK', 'PART-2-OK', 'CANCELLED', 'DONE' ],
		"Amanda::Xfer::Dest::Taper::DirectTCP element produces the correct series of messages when cancelled in mid-xfer")
	    or diag(Dumper([@messages]));
	}
    }

    # Amanda::Xfer::Source::Recovery's directtcp functionality is not
    # tested here, as to do so would basically require re-implementing
    # Amanda::Recovery::Clerk; the xfer source is adequately tested by
    # the Amanda::Recovery::Clerk tests.

    $ndmp->cleanup();
}

# directtcp stuff

{
    my $RANDOM_SEED = 0x13131313; # 13 is bad luck, right?

    # we want this to look like:
    # A: [ Random -> DirectTCPConnect ]
    #        --dtcp-->
    # B: [ DirectTCPListen -> filter -> DirectTCPListen ]
    #        --dtcp-->
    # C: [ DirectTCPConnect -> filter -> Null ]
    #
    # this tests both XFER_MECH_DIRECTTCP_CONNECT and
    # XFER_MECH_DIRECTTCP_LISTEN, as well as some of the glue
    # used to attach those to filters.
    #
    # that means we need to start transfer B, since it has all of the
    # addresses, before creating A or C.

    my $done = { };
    my $handle_msg = sub {
	my ($letter, $src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	} elsif ($msg->{'type'} == $XMSG_DONE) {
	    $done->{$letter} = 1;
	}
	if ($done->{'A'} and $done->{'B'} and $done->{'C'}) {
	    Amanda::MainLoop::quit();
	}
    };

    my %cbs;
    for my $letter ('A', 'B', 'C') {
	$cbs{$letter} = sub { $handle_msg->($letter, @_); };
    }

    my $src_listen = Amanda::Xfer::Source::DirectTCPListen->new();
    my $dst_listen = Amanda::Xfer::Dest::DirectTCPListen->new();
    my $xferB = Amanda::Xfer->new([
	$src_listen,
	Amanda::Xfer::Filter::Xor->new(0x13),
	$dst_listen
    ]);

    $xferB->start($cbs{'B'});

    my $xferA = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024*3, $RANDOM_SEED),
	Amanda::Xfer::Dest::DirectTCPConnect->new($src_listen->get_addrs())
    ]);

    $xferA->start($cbs{'A'});

    my $xferC = Amanda::Xfer->new([
	Amanda::Xfer::Source::DirectTCPConnect->new($dst_listen->get_addrs()),
	Amanda::Xfer::Filter::Xor->new(0x13),
	Amanda::Xfer::Dest::Null->new($RANDOM_SEED)
    ]);

    $xferC->start($cbs{'C'});

    # let the already-started transfers go out of scope before they 
    # complete, as a memory management test..
    Amanda::MainLoop::run();
    pass("Three xfers interlinked via DirectTCP complete successfully");
}

# try cancelling a DirectTCP xfer while it's waiting in accept()
{
    my $xfer_src = Amanda::Xfer::Source::DirectTCPListen->new();
    my $xfer_dst = Amanda::Xfer::Dest::Null->new(0);
    my $xfer = Amanda::Xfer->new([ $xfer_src, $xfer_dst ]);

    # start up the transfer, which starts a thread which will accept
    # soon after that.
    $xfer->start(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{'type'} == $XMSG_DONE) {
	    Amanda::MainLoop::quit();
	}
    });

    sleep(1);

    # Now, ideally we'd wait until the accept() is running, maybe testing it
    # with a SYN or something like that.  This is not terribly critical,
    # because the element glue does not check for cancellation before it begins
    # accepting.
    $xfer->cancel();

    Amanda::MainLoop::run();
    pass("A DirectTCP accept operation can be cancelled");
}

# test element comparison
{
    my $a = Amanda::Xfer::Filter::Xor->new(0);
    my $b = Amanda::Xfer::Filter::Xor->new(1);
    ok($a == $a, "elements compare equal to themselves");
    ok(!($a == $b), ".. and not to other elements");
    ok(!($a != $a), "elements do not compare != to themselves");
    ok($a != $b, ".. but are != to other elements");
}
