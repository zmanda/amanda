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

use Test::More tests => 5;
use strict;

use lib "@amperldir@";
use Amanda::Xfer qw( :constants );
use Amanda::Debug;
use Amanda::MainLoop;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

{
    my $xfer = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024, 1, 0),
	Amanda::Xfer::Filter::Xor->new(0xde, 0, 0),
	Amanda::Xfer::Dest::Null->new(0, 0),
    ]);

    pass("Creating a transfer doesn't crash"); # hey, it's a start..

    $xfer->get_source()->set_callback(sub {
	my ($src, $msg, $xfer) = @_;
	if ($msg->{type} == $XMSG_INFO) {
	    is($msg->{message}, "Is this thing on?",
		"XMSG_INFO from Amanda::Xfer::Dest::Null has correct message");
	}
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    $src->remove();
	    Amanda::MainLoop::quit();
	}
    });
    $xfer->start();
    Amanda::MainLoop::run();
    pass("A simple transfer runs to completion");
}


{
    my $xfer1 = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024, 1, 0),
	Amanda::Xfer::Dest::Null->new(0, 0),
    ]);
    my $xfer2 = Amanda::Xfer->new([
	Amanda::Xfer::Source::Random->new(1024*1024*3, 1, 0),
	Amanda::Xfer::Filter::Xor->new(0xde, 0, 0),
	Amanda::Xfer::Dest::Null->new(0, 0),
    ]);

    my $cb = sub {
	my ($src, $msg, $xfer) = @_;
	if  ($xfer1->get_status() == $Amanda::Xfer::XFER_DONE
	 and $xfer2->get_status() == $Amanda::Xfer::XFER_DONE) {
	    $xfer1->get_source()->remove();
	    $xfer2->get_source()->remove();
	    Amanda::MainLoop::quit();
	}
    };

    $xfer1->get_source()->set_callback($cb);
    $xfer2->get_source()->set_callback($cb);

    $xfer1->start();
    $xfer2->start();
}
# let the transfers go out of scope before they complete, as
# a memory management test..
Amanda::MainLoop::run();
pass("Two simultaneous transfers run to completion");


{
    my @elts;

    push @elts, Amanda::Xfer::Source::Random->new(1024*1024, 1, 0);
    for my $i (0 .. 10) {
	push @elts, Amanda::Xfer::Filter::Xor->new($i, 0, 0);
    }
    push @elts, Amanda::Xfer::Dest::Null->new(0, 0);
    my $xfer = Amanda::Xfer->new(\@elts);

    my $cb = sub {
	my ($src, $msg, $xfer) = @_;
	if ($xfer->get_status() == $Amanda::Xfer::XFER_DONE) {
	    $xfer->get_source()->remove();
	    Amanda::MainLoop::quit();
	}
    };

    $xfer->get_source()->set_callback($cb);
    $xfer->start();

    Amanda::MainLoop::run();
    pass("One very long transfer runs to completion");
}
