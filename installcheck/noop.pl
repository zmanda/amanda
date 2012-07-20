# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

use Test::More tests => 6;

use strict;
use warnings;

use lib '@amperldir@';
use Installcheck;
use Installcheck::ClientService;
use Amanda::Debug;
use Amanda::MainLoop;

Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

{
    my $service;
    my ($got_options, $got_eof, $process_done);

    $got_options = sub {
	pass("got OPTIONS");

	$service->expect('main',
	    [ eof => $got_eof ]);
    };

    # note that this gets called four times, in arbitrary order
    $got_eof = sub {
	pass("got EOF");
    };

    $process_done = sub {
	my ($exitstatus) = @_;
	is($exitstatus, 0,
	    "exit status 0");
	Amanda::MainLoop::quit();
    };

    $service = Installcheck::ClientService->new(
	    service => 'noop',
	    emulate => 'amandad',
	    process_done => $process_done);
    # no REQ packet body
    $service->close('main', 'w');
    $service->expect('main',
	[ re => qr/OPTIONS features=[0-9a-f]+;\n/, $got_options ]);
    $service->expect('stream1', [ eof => $got_eof ]);
    $service->expect('stream2', [ eof => $got_eof ]);
    $service->expect('stream3', [ eof => $got_eof ]);
    Amanda::MainLoop::run();
}
