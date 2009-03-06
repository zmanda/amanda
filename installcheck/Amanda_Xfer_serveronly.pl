# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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

use Test::More tests => 4;
use File::Path;
use strict;

use lib "@amperldir@";
use Installcheck::Run;
use Amanda::Xfer qw( :constants );
use Amanda::Device qw( :constants );
use Amanda::Header;
use Amanda::Debug;
use Amanda::MainLoop;
use Amanda::Paths;
use Amanda::Config;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# initialize configuration for the device API
Amanda::Config::config_init(0, undef);

# exercise device source and destination
{
    my $RANDOM_SEED = 0xFACADE;
    my $xfer;

    my $quit_cb = sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_ERROR) {
	    die $msg->{elt} . " failed: " . $msg->{message};
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    $xfer->get_source()->remove();
	    Amanda::MainLoop::quit();
	}
    };

    # set up vtapes
    my $testconf = Installcheck::Run::setup();
    $testconf->write();

    # set up a device for slot 1
    my $device = Amanda::Device->new("file:" . Installcheck::Run::load_vtape(1));
    die("Could not open VFS device: " . $device->error())
	unless ($device->status() == $DEVICE_STATUS_SUCCESS);

    # write to it
    my $hdr = Amanda::Header->new();
    $hdr->{type} = $Amanda::Header::F_DUMPFILE;
    $hdr->{name} = "installcheck";
    $hdr->{disk} = "/";
    $hdr->{datestamp} = "20080102030405";

    $device->finish();
    $device->start($ACCESS_WRITE, "TESTCONF01", "20080102030405");
    $device->start_file($hdr);

    $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024, $RANDOM_SEED),
	Amanda::Xfer::Dest::Device->new($device, $device->block_size() * 10),
    ]);

    $xfer->get_source()->set_callback($quit_cb);
    $xfer->start();

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

    $xfer->get_source()->set_callback($quit_cb);
    $xfer->start();

    Amanda::MainLoop::run();
    pass("read from a device succeeded, too, and data was correct");
}
