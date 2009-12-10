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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 55;
use IO::Socket;
use strict;

use lib "@amperldir@";
use Installcheck;
use Installcheck::Mock;
use Installcheck::Config;
use Amanda::Xfer qw( :constants );
use Amanda::Device qw( :constants );
use Amanda::Config qw( :init );
use Amanda::Header qw( :constants );
use Amanda::MainLoop;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

my $dev;
my $ndmp_port = Installcheck::get_unused_port();

my $testconf = Installcheck::Config->new();
$testconf->write();

my $cfg_result = config_init($CONFIG_INIT_EXPLICIT_NAME, 'TESTCONF');
if ($cfg_result != $CFGERR_OK) {
    my ($level, @errors) = Amanda::Config::config_errors();
    die(join "\n", @errors);
}

my $tapefile = Installcheck::Mock::run_ndmjob($ndmp_port);
pass("started ndmjob in daemon mode; tapefile=$tapefile");

# set up a header for use below
my $hdr = Amanda::Header->new();
$hdr->{type} = $Amanda::Header::F_DUMPFILE;
$hdr->{datestamp} = "20070102030405";
$hdr->{dumplevel} = 0;
$hdr->{compressed} = 1;
$hdr->{name} = "localhost";
$hdr->{disk} = "/home";
$hdr->{program} = "INSTALLCHECK";

$dev = Amanda::Device->new("ndmp:127.0.0.1:9i1\@foo");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device fails with invalid port");

$dev = Amanda::Device->new("ndmp:127.0.0.1:90000\@foo");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device fails with too-large port");

$dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port");
isnt($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device fails without ..\@device_name");

$dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port\@$tapefile");
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device succeeds with correct syntax");

ok($dev->property_set("ndmp_username", "foo"),
    "set ndmp_username property");
is($dev->property_get("ndmp_username"), "foo",
    "..and get the value back");
ok($dev->property_set("ndmp_password", "bar"),
    "set ndmp_password property");
is($dev->property_get("ndmp_password"), "bar",
    "..and get the value back");

ok($dev->property_set("verbose", 1),
    "set VERBOSE");

# set 'em back to the defaults
$dev->property_set("ndmp_username", "ndmp");
$dev->property_set("ndmp_password", "ndmp");

# ok, let's fire the thing up
ok($dev->start($ACCESS_WRITE, "TEST1", "20090915000000"),
    "start device in write mode")
    or diag $dev->error_or_status();

ok($dev->start_file($hdr),
    "start_file");

{   # write to the file
    my $xfer = Amanda::Xfer->new([
	    Amanda::Xfer::Source::Random->new(32768*21, 0xBEEFEE00),
	    Amanda::Xfer::Dest::Device->new($dev, 32768*5) ]);
    $xfer->start(make_cb(xmsg_cb => sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    }));

    Amanda::MainLoop::run();
    pass("wrote 21 blocks");
}

ok($dev->finish(),
    "finish device")
    or diag $dev->error_or_status();

is($dev->read_label(), $DEVICE_STATUS_SUCCESS,
    "read label from (same) device")
    or diag $dev->error_or_status();

is($dev->volume_label, "TEST1",
    "volume label read back correctly");

## label a device and check the label, but open a new device in between

# Write a label
$dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port\@$tapefile");
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device succeeds with correct syntax");
$dev->property_set("ndmp_username", "ndmp");
$dev->property_set("ndmp_password", "ndmp");
$dev->property_set("verbose", 1);

# Write the label
ok($dev->start($ACCESS_WRITE, "TEST2", "20090915000000"),
    "start device in write mode")
    or diag $dev->error_or_status();
ok($dev->finish(),
    "finish device")
    or diag $dev->error_or_status();

# Read the label with a new device.
$dev = Amanda::Device->new("ndmp:127.0.0.1:$ndmp_port\@$tapefile");
is($dev->status(), $DEVICE_STATUS_SUCCESS,
    "creation of an ndmp device succeeds with correct syntax");
$dev->property_set("ndmp_username", "ndmp");
$dev->property_set("ndmp_password", "ndmp");
$dev->property_set("verbose", 1);

# read the label
is($dev->read_label(), $DEVICE_STATUS_SUCCESS,
    "read label from device")
    or diag $dev->error_or_status();
is($dev->volume_label, "TEST2",
    "volume label read back correctly");
ok($dev->finish(),
    "finish device")
    or diag $dev->error_or_status();

#
# test the directtcp-target implementation
#

{
    ok($dev->directtcp_supported(), "is a directtcp target");

    ok($dev->start($ACCESS_WRITE, "TEST2", "20090915000000"),
	"start device in write mode")
	or diag $dev->error_or_status();

    my $addrs = $dev->listen(1);
    ok($addrs, "listen returns successfully") or die($dev->error_or_status());

    # set up an xfer to write to the device
    my $xfer = Amanda::Xfer->new([
	    Amanda::Xfer::Source::Random->new(32768*34, 0xB00),
	    Amanda::Xfer::Dest::DirectTCPConnect->new($addrs) ]);

    my @messages;
    $xfer->start(make_cb(xmsg_cb => sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    }));

    # write files from the connection until EOF
    my $num_files;
    my $conn;
    my ($call_accept, $write_file_cb);

    $call_accept = make_cb(call_accept => sub {
	$conn = $dev->accept();
	Amanda::MainLoop::call_later($write_file_cb);
    });

    $write_file_cb = make_cb(write_file_cb => sub {
	++$num_files < 20 or die "I seem to be in a loop!";

	ok($dev->start_file($hdr), "start file $num_files for writing");
	is($dev->file, $num_files, "..file number is correct");

	my ($ok, $size) = $dev->write_from_connection($conn, 32768*15);
	push @messages, sprintf("WRITE-%s-%d-%s-%s",
	    $ok?"OK":"ERR", $size,
	    $dev->is_eof()? "EOF":"!eof",
	    $dev->is_eom()? "EOM":"!eom");
	ok($ok, "..write from connection succeeds");
	my $eof = $dev->is_eof();

	ok($dev->finish_file(), "..finish file after writing");

	if (!$eof) {
	    Amanda::MainLoop::call_later($write_file_cb);
	}
    });

    Amanda::MainLoop::call_later($call_accept);
    Amanda::MainLoop::run();
    is_deeply([@messages], [
	    'WRITE-OK-491520-!eof-!eom',
	    'WRITE-OK-491520-!eof-!eom',
	    'WRITE-OK-131072-EOF-!eom',
	],
	"a sequence of write_from_connection calls works correctly");

    $dev->finish();

    if (my $err = $conn->close()) {
	die $err;
    }
}

# now try reading that back piece by piece

{
    my $filename = "$Installcheck::TMP/Amanda_Device_ndmp.tmp";
    open(my $dest_fh, ">", $filename);

    ok($dev->start($ACCESS_READ, undef, undef),
	"start device in read mode")
	or diag $dev->error_or_status();

    my $file;
    for ($file = 1; $file <= 3; $file++) {
	ok($dev->seek_file($file),
	    "seek_file $file");
	is($dev->file, $file, "..file num is correct");
	is($dev->block, 0, "..block num is correct");

	# read the file, writing to our temp file.  We'll check that the byte
	# sequence is correct later
	my $xfer = Amanda::Xfer->new([
		Amanda::Xfer::Source::Device->new($dev),
		Amanda::Xfer::Dest::Fd->new($dest_fh) ]);

	$xfer->start(make_cb(xmsg_cb => sub {
	    my ($src, $msg, $xfer) = @_;
	    if ($msg->{'type'} == $XMSG_ERROR) {
		die $msg->{'elt'} . " failed: " . $msg->{'message'};
	    }
	    if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
		Amanda::MainLoop::quit();
	    }
	}));
	Amanda::MainLoop::run();

	pass("read back file " . $file);
    }

    $dev->finish();
    close $dest_fh;

    # now read back and verify that file
    open(my $src_fh, "<", $filename);
    my $xfer = Amanda::Xfer->new([
	    Amanda::Xfer::Source::Fd->new($src_fh),
	    Amanda::Xfer::Dest::Null->new(0xB00) ]);

    $xfer->start(make_cb(xmsg_cb => sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    Amanda::MainLoop::quit();
	}
    }));
    Amanda::MainLoop::run();

    pass("data in the three parts is correct");
    unlink $filename;
}

####
# Test read_to_connection
#
# This requires something that can connect to a device and read from
# it; the XFA does not have an XFER_MECH_DIRECTTCP_CONNECT, so we fake
# it by manually connecting and then setting up an xfer with a regular
# XferSourceFd.  This works because the NDMP server will accept an
# incoming connection before the Device API accept() method is called;
# this trick may not work with other DirectTCP-capable devices.  Also,
# this doesn't work so well if there's an error in the xfer (e.g., a
# random value mismatch).  But tests are supposed to succeed!

{
    my @events;
    ok($dev->start($ACCESS_READ, undef, undef),
	"start device in read mode")
	or diag $dev->error_or_status();

    my $addrs = $dev->listen(0);

    # now connect to that 
    my $sock = IO::Socket::INET->new(
	Proto => "tcp",
	PeerHost => $addrs->[0][0],
	PeerPort => $addrs->[0][1],
	Blocking => 1,
    );
    
    # and set up a transfer to read from that socket
    my $xfer = Amanda::Xfer->new([
	    Amanda::Xfer::Source::Fd->new($sock),
	    Amanda::Xfer::Dest::Null->new(0xB00) ]);

    $xfer->start(make_cb(xmsg_cb => sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{'type'} == $XMSG_ERROR) {
	    die $msg->{'elt'} . " failed: " . $msg->{'message'};
	}
	if ($msg->{'type'} == $XMSG_DONE) {
	    push @events, "DONE";
	    Amanda::MainLoop::quit();
	}
    }));
    
    # set up to accept and then read_to_connection for each part
    my $file = 1;
    my $conn;
    my %subs;

    $subs{'accept'} = make_cb(accept => sub {
	$conn = $dev->accept();
	die $dev->error_or_status() unless ($conn);

	Amanda::MainLoop::call_later($subs{'read_part_cb'});
    });

    $subs{'read_part_cb'} = make_cb(read_part_cb => sub {
	my $hdr = $dev->seek_file($file);
	die $dev->error_or_status() unless ($hdr);
	my $size = $dev->read_to_connection($conn, 0);
	push @events, "READ-$size";

	if (++$file <= 3) {
	    Amanda::MainLoop::call_later($subs{'read_part_cb'});
	} else {
	    # close the connection, which will end the xfer, which will
	    # result in a call to Amanda::MainLoop::quit.  So there.
	    push @events, "CLOSE";
	    $conn->close();
	}
    });

    Amanda::MainLoop::call_later($subs{'accept'});
    Amanda::MainLoop::run();

    close $sock or die "close: $!";

    is_deeply([@events], 
	[ "READ-491520", "READ-491520", "READ-131072", "CLOSE", "DONE" ],
	"sequential read_to_connection operations read the right amounts and bytestream matches");
}

unlink $tapefile;
