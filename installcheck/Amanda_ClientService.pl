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

use Test::More tests => 30;
use strict;
use warnings;

use Data::Dumper;

use lib "@amperldir@";
use Amanda::ClientService;
use Amanda::Constants;
use Amanda::Util;
use Amanda::Debug;
use Amanda::Config qw( :init );
use Amanda::MainLoop;
use Socket;

config_init(0, undef);
Amanda::Debug::dbopen('installcheck');

# test connect_streams
{
    # that these tests assume that DATA_FD_OFFSET and DATA_FD_COUNT have these values
    is($Amanda::Constants::DATA_FD_OFFSET, 50, "DATA_FD_OFFSET is what I think it is");
    is($Amanda::Constants::DATA_FD_COUNT, 3, "DATA_FD_COUNT is what I think it is");

    sub test_connect_streams {
	my ($args, $exp_line, $exp_closed_fds, $exp_streams, $msg) = @_;

	my $cs = Amanda::ClientService->new();
	$cs->{'_dont_use_real_fds'} = 1;
	$cs->{'_argv'} = [ 'amandad' ];
	my $got_line = $cs->connect_streams(@$args);

	is($got_line, $exp_line, "$msg (CONNECT line)");

	is_deeply([ sort @{$cs->{'_would_have_closed_fds'}} ],
		  [ sort @$exp_closed_fds ],
		  "$msg (closed fds)");

	# get the named streams and their fd's
	my %streams;
	while (@$args) {
	    my $name = shift @$args;
	    my $dirs = shift @$args;
	    $streams{$name} = [ $cs->rfd($name), $cs->wfd($name) ];
	}

	is_deeply(\%streams, $exp_streams, "$msg (streams)");
    }

    test_connect_streams(
	[ 'DATA' => 'rw' ],
	'CONNECT DATA 50',
	[ 52, 53, 54, 55 ],
	{ 'DATA' => [ 51, 50 ] },
	"simple read/write DATA stream");

    test_connect_streams(
	[ 'DATA' => 'r' ],
	'CONNECT DATA 50',
	[ 50, 52, 53, 54, 55 ],
	{ 'DATA' => [ 51, -1 ] },
	"read-only stream");

    test_connect_streams(
	[ 'DATA' => 'w' ],
	'CONNECT DATA 50',
	[ 51, 52, 53, 54, 55 ],
	{ 'DATA' => [ -1, 50 ] },
	"write-only stream");

    test_connect_streams(
	[ 'DATA' => 'rw', 'RD' => 'r', 'WR' => 'w' ],
	'CONNECT DATA 50 RD 51 WR 52',
	[ 52, 55 ],
	{ 'DATA' => [ 51, 50 ],
	  'RD' => [ 53, -1 ],
	  'WR' => [ -1, 54 ] },
	"three streams");
}

# test from_inetd and friends
{
    my $cs;

    $cs = Amanda::ClientService->new();
    $cs->{'_argv'} = [];
    ok($cs->from_inetd, "no argv[0] interpreted as a run from inetd");

    $cs = Amanda::ClientService->new();
    $cs->{'_argv'} = [ 'installcheck' ];
    ok($cs->from_inetd, "argv[0] = 'installcheck' also interpreted as a run from inetd");

    $cs = Amanda::ClientService->new();
    $cs->{'_argv'} = [ 'amandad' ];
    ok(!$cs->from_inetd, "argv[0] = 'amandad' interpreted as a run from amandad");

    $cs = Amanda::ClientService->new();
    $cs->{'_argv'} = [ 'amandad', 'bsdgre' ];
    is($cs->amandad_auth, "bsdgre",
	"argv[1] = 'bsdgre' interpreted as auth");

    $cs = Amanda::ClientService->new();
    $cs->{'_argv'} = [ 'amandad' ];
    is($cs->amandad_auth, undef,
	"amandad_auth interpreted as undef if missing");
}

# test add_connection and half-close operations
sub test_connections {
    my ($finished_cb) = @_;

    my $port;
    my $cs = Amanda::ClientService->new();
    $cs->{'_argv'} = [ ];

    my $steps = define_steps
	cb_ref => \$finished_cb;

    step listen => sub {
	$port = $cs->connection_listen('FOO', 0);

	$steps->{'fork'}->();
    };

    step fork => sub {
	# fork off a child to connect to and write to that port
	if (fork() == 0) {
	    socket(my $foo, PF_INET, SOCK_STREAM, getprotobyname('tcp'))
		or die "error creating connect socket: $!";
	    connect($foo, sockaddr_in($port, inet_aton("127.0.0.1")))
		or die "error connecting: $!";
	    my $info = <$foo>;
	    print $foo "GOT[$info]";
	    close($foo);
	    exit(0);
	} else {
	    $steps->{'accept'}->();
	}
    };

    step accept => sub {
	$cs->connection_accept('FOO', 90, $steps->{'accept_finished'});
    };

    step accept_finished => sub {
	# write a message to the fd and read back the result; this is
	# synchronous
	my $msg = "HELLO WORLD";
	Amanda::Util::full_write($cs->wfd('FOO'), $msg, length($msg))
	    or die "full write: $!";
	$cs->close('FOO', 'w');
	is($cs->wfd('FOO'), -1,
	    "FOO is closed for writing");

	my $input = Amanda::Util::full_read($cs->rfd('FOO'), 1024);
	$cs->close('FOO', 'r');
	is_deeply([ keys %{$cs->{'_streams'}} ], [ 'main' ],
	    "FOO stream is deleted when completely closed");

	is($input, "GOT[HELLO WORLD]",
	    "both directions of the FOO stream work");

	$finished_cb->();
    };
}
test_connections(\&Amanda::MainLoop::quit);
Amanda::MainLoop::run();

# check rfd and wfd
{
    my $cs = Amanda::ClientService->new();
    is($cs->rfd('main'), 0,
	"main rfd is stdin");
    is($cs->wfd('main'), 1,
	"main wfd is stdout");
    is($cs->wfd('none'), -1,
	"wfd returns -1 for invalid stream");
    is($cs->rfd('none'), -1,
	"rfd returns -1 for invalid stream");
}

# check check_bsd_security
{
    # note that we can't completely test this, because BSD security entails checking
    # DNS and privileged ports, neither of which are controllable from the installcheck
    # environment.  However, we can at least call the method.

    my $cs = Amanda::ClientService->new();
    $cs->{'_argv'} = [ 'installcheck' ]; # basically neuters check_bsd_security

    ok(!$cs->check_bsd_security('main', "USER bart"),
	"check_bsd_security returns undef");
}

# check parse_req
{
    my $cs = Amanda::ClientService->new();
    my $req_str;

    # is_deeply doesn't like objects very much
    sub strip_features {
	my ($x) = @_;
	#use Data::Dumper;
	#print Dumper($x);
	return $x unless defined $x->{'features'};
	$x->{'features'} = "featureset";
	return $x;
    }

    $req_str = <<ENDREQ;
OPTIONS auth=passport;features=f0039;
FOO
ENDREQ
    is_deeply(strip_features($cs->parse_req($req_str)), {
	lines => [ 'OPTIONS auth=passport;features=f0039;', 'FOO' ],
	options => {
	    auth => 'passport',
	    features => 'f0039',
	},
	errors => [],
	features => "featureset",
    }, "parse_req parses a request properly");

    $req_str = <<ENDREQ;
OPTIONS auth=bsd;no-features;yes=no;
ENDREQ
    is_deeply(strip_features($cs->parse_req($req_str)), {
	lines => [ 'OPTIONS auth=bsd;no-features;yes=no;' ],
	options => {
	    auth => 'bsd',
	    yes => 'no',
	    'no-features' => 1,
	},
	errors => [],
	features => undef,
    }, "parse_req parses a request with boolean options");

    $req_str = <<ENDREQ;
OPTIONS turn=left;
OPTIONS turn=right;
ENDREQ
    is_deeply(strip_features($cs->parse_req($req_str)), {
	lines => [ 'OPTIONS turn=left;', 'OPTIONS turn=right;' ],
	options => {
	    turn => 'left',
	},
	errors => [ 'got multiple OPTIONS lines' ],
	features => undef,
    }, "parse_req detects multiple OPTIONS lines as an error");
}
