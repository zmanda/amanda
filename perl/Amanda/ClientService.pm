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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::ClientService;

=head1 NAME

Amanda::ClientService -- support for writing amandad and inetd services

=head1 SYNOPSIS

    package main::Service;
    use base qw( Amanda::ClientService );

    sub start {
	my $self = shift;
	if ($self->from_inetd) {
	    $self->check_bsd_security('main');
	}
	# ...
    }
    #...

    package main;
    use Amanda::MainLoop;
    my $svc = main::Service->new();
    Amanda::MainLoop::run();

=head1 NOTE

Note that, despite the name, some client services are actually run on the
server - C<amidxtaped> and C<amindexd> in particular.

=head1 INTERFACE

This package is used as a parent class, and is usually subclassed as
C<main::Service>.  Its constructor takes no arguments, and automatically
configures the MainLoop to call the C<start> method:

    my $svc = main::Service->new();
    Amanda::MainLoop::start();

The object is a blessed hashref. And all of the keys used by this class are
private, and begin with an underscore.  Subclasses are free to use any key
names that do not begin with an underscore.

=head2 Invocation Type

Some client services can be invoked directly from inetd for backward
compatibility.  This package will automatically detect this, and act
accordingly.  Subclasses can determine if they were invoked from inetd with the
boolean C<from_inetd> and C<from_amandad> methods.  If C<from_amandad> is true,
then the authentication specified is available from the C<amandad_auth> method
(and may be C<undef>).

    print "hi, inetd"
	if $self->from_inetd();
    print "hi, amandadd w/ ", ($self->amandad_auth() || "none")
	if $self->from_amandad();

=head2 Streams

This package manages the available data streams as pairs of file descriptors,
providing convenience methods to hide some of the complexity of dealing with
them.  Note that the class does not handle asynchronous reading or writing to
these file descriptors, and in fact most of the operations are entirely
synchronous, as they are non-blocking.  File descriptors for streams are
available from the C<rfd> and C<wfd> methods, which both take a stream name:

    Amanda::Util::full_write($self->wfd('MESG'), $buf, length($buf));

Each bidirectional stream has a name.  At startup, stdin and stdout constitute
the 'main' stream.  For amandad invocations, this stream should be used to read
the REQ packet and write the PREP and REP packets (see below).  For inetd
invocations, this is the primary means of communicating with the other end of
the connection.

For amandad invocations, the C<create_streams> method will create named streams
back to amandad.  Each stream name is paired with direction indicators: C<'r'>
for read, C<'w'> for write, or C<'rw'> for both.  Any unused file descriptors
will be closed.  The method will return a C<CONNECT> line suitable for
inclusion in a REP packet, without a newline terminator, giving the streams in
the order they were specified.

    push @replines, $self->connect_streams(
	    'DATA' => 'w', 'MESG' => 'rw', 'INDEX' => 'w');

For inetd invocations, the C<connection_listen> method will open a (privileged,
if C<$priv> is true) listening socket and return the port.  You should then
write a C<CONNECT $port\n> to the main stream and call C<connection_acecpt> to
wait for a connection to the listening port.  The latter method calls
C<finished_cb> with C<undef> on success or an error message on failure.  The
C<$timeout> is specified in seconds.  On success, the stream named C<$name> is
then available for use.  Note that this method does not check security on the
connection.  Also note that this process requires that the Amanda configuration
be initialized.

    my $port = $self->connection_listen($name, $priv);
    # send $port to the other side
    $self->connection_accept($name, $timeout, $finished_cb);

To close a stream, use C<close>.  This takes an optional second argument which
can be used to half-close connections which support it.  Note that the
underlying file descriptor is closed immediately, without regard to any
outstanding asynchronous reads or writes.

    $self->close('MESG');       # complete close ('rw' is OK too)
    $self->close('INDEX', 'r'); # close for reading
    $self->close('DATA', 'w');  # close for writing

=head2 Security

Invocations from inetd require a BSD-style security check.  The
C<check_bsd_security> method takes the stream and the authentication string,
I<without> the C<"SECURITY "> prefix or trailing newline, and performs the
necesary checks.  To be clear, this string usually has the form C<"USER root">.
The method returns C<undef> if the check succeeds, and an error message if it
fails.

    if ($self->check_bsd_security($stream, $authstr)) { .. }

Not that the security check is skipped if the service is being run from an
installcheck, since BSD security can't be tested by installchecks.

=head2 REQ packets

When invoked from amandad, a REQ packet is available on stdin, and amanadad
expects a REP packet on stdout.  The C<parse_req> method takes the entire REP
packet, splits it into lines without trailing whitespace, exracts any OPTIONS
line into a hash, and decodes any features in the OPTIONS line.  If no OPTIONS
appear, or the OPTIONS do not include features, then the C<features> key of the
result will be undefined.  If there are format errors in the REQ, then the
C<errors> key will contain a list of error messages.

    my $req_info = parse_req($req_str);
    if (@{$req_info->{'errors'}}) {
	print join("\n", @{$req_info->{'errors'}}), "\n";
	exit;
    }
    print $req_info->{'options'}{'auth'}; # access to options
    print "yes!" if $req_info->{'features'}->has($fe_whatever);
    for my $line (@{$req_info->{'lines'}) {
	print "got REQ line '$line'\n";
    }

Note that the general format of OPTION strings is unknown at this time, so this
method may change significantly as more client services are added.

=cut

use strict;
use warnings;
use Data::Dumper;
use IO::Handle;

use Amanda::Util qw( :constants stream_server stream_accept );
use Amanda::Debug qw( debug );
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Feature;

sub new {
    my $class = shift;

    my $self = bless {
	_streams => {},
	_listen_sockets => {},
	_argv => [ @ARGV ],
    }, $class;

    $self->_add_stream('main', 0, 1);
    return $self;
}

sub from_inetd {
    my $self = shift;
    return 1 if (!defined $self->{'_argv'}[0] or $self->{'_argv'}[0] eq 'installcheck');
}

sub from_amandad {
    my $self = shift;
    return 1 if defined $self->{'_argv'}[0] and $self->{'_argv'}[0] eq 'amandad';
}

sub from_installcheck {
    my $self = shift;
    return 1 if defined $self->{'_argv'}[0] and $self->{'_argv'}[0] eq 'installcheck';
}

sub amandad_auth {
    my $self = shift;
    return undef unless $self->from_amandad();
    return $self->{'_argv'}[1];
}

sub connect_streams {
    my $self = shift;
    my $connect_line = "CONNECT";
    my $fd = $Amanda::Constants::DATA_FD_OFFSET;
    my $handle = $Amanda::Constants::DATA_FD_OFFSET;
    my @fds_to_close;

    # NOTE: while $handle and $fd both start counting in the same place, they
    # are not the same thing!  $fd counts real file descriptors - two per
    # stream.  $handle only increases by one for each stream.

    if (@_ > $Amanda::Constants::DATA_FD_COUNT * 2) {
	die "too many streams!";
    }

    die "not using amandad" if $self->from_inetd();

    while (@_) {
	my $name = shift;
	my $dirs = shift;
	my ($wfd, $rfd) = (-1, -1);

	# lower-numbered fd is for writing
	if ($dirs =~ /w/) {
	    $wfd = $fd;
	} else {
	    push @fds_to_close, $fd;
	}
	$fd++;

	# higher-numbered fd is for reading
	if ($dirs =~ /r/) {
	    $rfd = $fd;
	} else {
	    push @fds_to_close, $fd;
	}
	$fd++;

	$self->_add_stream($name, $rfd, $wfd);
	$connect_line .= " $name $handle";
	$handle++;
    }

    while ($fd < $Amanda::Constants::DATA_FD_OFFSET
		 + 2 * $Amanda::Constants::DATA_FD_COUNT) {
	push @fds_to_close, $fd++;
    }

    # _dont_use_real_fds indicats that we should mock the close operation
    if (!$self->{'_dont_use_real_fds'}) {
	for $fd (@fds_to_close) {
	    if (!POSIX::close($fd)) {
		Amanda::Debug::warning("Error closing fd $fd: $!");
	    }
	}
	# let the stupid OpenBSD threads library know we're using these fd's
	Amanda::Util::openbsd_fd_inform();
    } else {
	$self->{'_would_have_closed_fds'} = \@fds_to_close;
    }

    return $connect_line;
}

sub connection_listen {
    my $self = shift;
    my ($name, $priv) = @_;

    die "not using inetd" unless $self->from_inetd();
    die "stream $name already exists" if exists $self->{'_streams'}{$name};
    die "stream $name is already listening" if exists $self->{'_listen_sockets'}{$name};

    # first open a socket
    my ($lsock, $port) = stream_server(
	    $AF_INET, $STREAM_BUFSIZE, $STREAM_BUFSIZE, $priv);
    return 0 if ($lsock < 0);

    $self->{'_listen_sockets'}{$name} = $lsock;
    return $port;
}


sub connection_accept {
    my $self = shift;
    my ($name, $timeout, $finished_cb) = @_;

    my $lsock = $self->{'_listen_sockets'}{$name};
    die "stream $name is not listening" unless defined $lsock;

    # set up a fd source *and* a timeout source
    my ($fd_source, $timeout_source);
    my $fired = 0;

    # accept is a "read" operation on a listening socket
    $fd_source = Amanda::MainLoop::fd_source($lsock, $Amanda::MainLoop::G_IO_IN);
    $fd_source->set_callback(sub {
	return if $fired;
	$fired = 1;
	$fd_source->remove();
	$timeout_source->remove();

	my $errmsg;
	my $datasock = stream_accept($lsock, 1, $STREAM_BUFSIZE, $STREAM_BUFSIZE);
	if ($datasock < 0) {
	    $errmsg = "error from stream_accept: $!";
	}

	# clean up
	POSIX::close($lsock);
	delete $self->{'_listen_sockets'}{$name};

	if ($datasock >= 0) {
	    $self->_add_stream($name, $datasock, $datasock);
	}

	return $finished_cb->($errmsg);
    });

    $timeout_source = Amanda::MainLoop::timeout_source(1000*$timeout);
    $timeout_source->set_callback(sub {
	return if $fired;
	$fired = 1;
	$fd_source->remove();
	$timeout_source->remove();

	# clean up
	POSIX::close($lsock);
	delete $self->{'_listen_sockets'}{$name};

	return $finished_cb->("timeout while waiting for incoming TCP connection");
    });
}

sub rfd {
    my $self = shift;
    my ($name) = @_;

    return -1 unless exists $self->{'_streams'}{$name};
    my $fd = $self->{'_streams'}{$name}{'rfd'};
    return defined($fd)? $fd : -1;
}

sub wfd {
    my $self = shift;
    my ($name) = @_;

    return -1 unless exists $self->{'_streams'}{$name};
    my $fd = $self->{'_streams'}{$name}{'wfd'};
    return defined($fd)? $fd : -1;
}

sub close {
    my $self = shift;
    my ($name, $dir) = @_;

    $dir = 'rw' unless defined $dir;
    my $rfd = $self->rfd($name);
    my $wfd = $self->wfd($name);

    # sockets will have the read and write fd's, and are handled differently.  If
    # one end of a socket has been closed, then we can treat it like a regular fd
    if ($rfd == $wfd) {
	die "stream is already closed?" if ($rfd == -1);

	if ($dir eq 'rw') {
	    POSIX::close($rfd);
	    $rfd = $wfd = -1;
	} elsif ($dir eq 'r') {
	    # perl doesn't provide a fd-compatible shutdown, but luckily shudown
	    # affects dup'd file descriptors, too!  So create a new handle and shut
	    # it down.  When the handle is garbage collected, it will be closed,
	    # but that will not affect the original.  This will look strange in an
	    # strace, but it works without SWIGging shutdown()
	    shutdown(IO::Handle->new_from_fd(POSIX::dup($rfd), "r"), 0);
	    $rfd = -1;
	} elsif ($dir eq 'w') {
	    shutdown(IO::Handle->new_from_fd(POSIX::dup($wfd), "w"), 1);
	    $wfd = -1;
	}
    } else {
	if ($dir =~ /r/ and $rfd != -1) {
	    POSIX::close($rfd);
	    $rfd = -1;
	}
	if ($dir =~ /w/ and $wfd != -1) {
	    POSIX::close($wfd);
	    $wfd = -1;
	}
    }

    if ($rfd == -1 and $wfd == -1) {
	delete $self->{'_streams'}{$name};
    } else {
	$self->{'_streams'}{$name}{'rfd'} = $rfd;
	$self->{'_streams'}{$name}{'wfd'} = $wfd;
    }
}

sub check_bsd_security {
    my $self = shift;
    my ($name, $authstr) = @_;

    # find the open file descriptor
    my $fd = $self->rfd($name);
    $fd = $self->wfd($name) if $fd < 0;
    die "stream '$name' not open" if $fd < 0;

    # don't invoke check_security if we're run as an installcheck;
    # installchecks are incompatible with BSD security
    return undef if $self->from_installcheck();

    return Amanda::Util::check_security($fd, $authstr);
}

sub parse_req {
    my $self = shift;
    my ($req_str) = @_;
    my $rv = {
	lines => [],
	options => {},
	features => undef,
	errors => [],
    };

    # split into lines, split by '\n', filtering empty lines
    my @req_lines = grep /.+/, (split(/\n/, $req_str));
    $rv->{'lines'} = [ @req_lines ];

    # find and parse the options line
    my @opt_lines = grep /^OPTIONS /, @req_lines;
    if (@opt_lines > 1) {
	push @{$rv->{'errors'}}, "got multiple OPTIONS lines";
	# (and use the first OPTIONS line anyway)
    }
    if (@opt_lines) {
	my ($line) = $opt_lines[0] =~ /^OPTIONS (.*)$/;
	my @opts = grep /.+/, (split(/;/, $line));
	for my $opt (@opts) {
	    if ($opt =~ /^([^=]+)=(.*)$/) {
		$rv->{'options'}{$1} = $2;
	    } else {
		$rv->{'options'}{$opt} = 1;
	    }
	}
    }

    # features
    if ($rv->{'options'}{'features'}) {
	$rv->{'features'} = Amanda::Feature::Set->from_string($rv->{'options'}{'features'});
    }

    return $rv;
}

# private methods

sub _add_stream {
    my $self = shift;
    my ($stream, $rfd, $wfd) = @_;

    $self->{'_streams'}{$stream} = {
	rfd => $rfd,
	wfd => $wfd,
    };
}

1;
