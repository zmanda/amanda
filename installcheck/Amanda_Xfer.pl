# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 10;
use File::Path;
use strict;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Run;
use Amanda::Xfer qw( :constants );
use Amanda::Header;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Paths;
use Amanda::Config;
use Amanda::Constants;

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
	}
	elsif ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
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
	}
	if  ($xfer1->get_status() == $Amanda::Xfer::XFER_DONE
	 and $xfer2->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
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
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
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
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
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
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
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
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
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
	}
	elsif ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
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
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    $xfer->start();
    Amanda::MainLoop::run();
    ok($got_timeout, "Amanda::Xfer::Filter::Process can be cancelled");
    # (note that this does not test all of the cancellation possibilities)
}

