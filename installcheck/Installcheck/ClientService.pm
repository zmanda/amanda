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

package Installcheck::ClientService;

=head1 NAME

Installcheck::ClientService - a harness for testing client services like
sendbackup or selfcheck.

=head1 SYNOPSIS

    use Installcheck::ClientService;

    # fire up a fake amandad
    my $service;
    my $process_done = sub {
	my ($wait_status) = @_;
	Amanda::MainLoop::quit();
    };
    $service = Installcheck::ClientService->new(
			    service => 'amindexd',
			    emulate => 'amandad',
			    auth => 'bsdtcp',
			    auth_peer => 'localhost',
			    process_done => $process_done);
    # or
    $service = Installcheck::ClientService->new(
			    service => 'amindexd',
			    emulate => 'inetd',
			    args => [ @args ],
			    process_done => $process_done);
    $service->expect('main',
	[ re => qr/^CONNECT (\d+)\n/, $handle_connect ],
	[ re => qr/^ERROR (.*)\r\n/, $handle_error ]);
    $service->expect('stream1',
	[ eof => $handle_eof ]);
    $service->expect('stream2',
	[ header => $handle_header ]);
    $service->expect('stream3',
	[ data => $handle_data ]);
    Amanda::MainLoop::run();

=head1 DESCRIPTION

The C<Installcheck::ClientService> class re-implements the service-facing side
of amandad and inetd.  It strips away all of the service-specific hacks and the
security API portions.  It handles multiple, simultaneous, named, bidirectional
data streams with an expect-like interface.

When emulating amandad, the service is run with the usual high-numbered file
descriptors pre-piped, and with 'amandad' in C<argv[1]> and the C<auth>
parameter (which defaults to 'bsdtcp') in C<argv[2]>.  The service's stdout and
stdin are connected to the 'main' stream, and stderr is available as 'stderr'.
The three bidirectional streams on the high-numbered pipes are available as
'stream1', 'stream2', and 'stream3'.  You should send a request packet on the
'main' stream and close it for writing, and read the reply from 'main'.  Note
that you should omit the 'SERVICE' line of the request, as it is ordinarily
consumed by amandad itself.

When emulating inetd, the service is run with a TCP socket attached to its
stdin and stdout, and 'installcheck' in C<argv[1]>.  Additional arguments can
be provided in the C<args> parameter.  The TCP socket is available as stream
'main'.

=head2 Constructor

See the SYNOPSIS for examples.  The constructor's C<service> parameter gives
the name of the service to run.  The C<emulate> parameter determines how the
service is invoked.  The C<args> and C<auth> parameters are described above.
The C<process_done> parameter gives a sub which is called with the service's
wait status when the service exits and all of its file descriptors have been
drained.  The C<auth_peer> parameter gives the value for
C<$AMANDA_AUTHENTICATED_PEER> when emulating amandad.

=head2 Killing Subprocess

To kill the subprocess, call

  $service->kill();

this will send a SIGINT.  Process termination proceeds as normal -
C<process_done> will be called.

=head2 Handling Streams

Streams have simple strings as names; the standard names are described in the
DESCRIPTION section.

To send data on a stream, use C<send>:

    $service->send('main', 'Hello, service!\n');

Note that this method does not block until the data is sent.

To close a stream, use C<close>.  It takes a stream name and direction, and
only closes that direction.  For TCP connections, this means half-open
connections, while for file descriptors only one of the descriptors is closed.

    $service->close('data', 'w'); # close for reading
    $service->close('data', 'r'); # close for writing
    $service->close('data', 'b'); # close for both

When emulating inetd, the C<connect> method can open a new connection to the
service, given a port number and a name for the new stream:

    $service->connect('index', $idx_port);

=head2 Handling Incoming Data

The read side of each stream has a set of I<expectations>: expected events and
subs to call when those events occur.  Each expectation comes in the form of an
arrayref, and starts with a string indicating its type.  The simplest is a
regular expression:

    [ re => qr/^200 OK/, $got_ok ]

In this case the C<$got_ok> sub will be called with the matched text.  An
expected EOF is written

    [ eof => $got_eof ]

To capture a stream of data, and call C<$got_data> on EOF with the number of
bytes consumed, use

    [ bytes_to_eof => $got_eof ]

To capture a specific amount of data - in this case 32k - and pass it to
C<$got_header>, use

    [ bytes => 32768, $got_header ]

The set of expectations for a stream is set with the C<expect> method.  This
method completely replaces any previous expectations.

    $service->expect('data',
	[ re => qr/^200 OK.*\n/, $got_ok ],
	[ re => qr/^4\d\d .*\n/, $got_err ]);

=cut

use base qw( Exporter );
use warnings;
use strict;
use Amanda::Constants;
use Amanda::MainLoop;
use Amanda::Paths;
use Amanda::Util;
use Amanda::Debug qw( debug );
use POSIX qw( :fcntl_h );
use POSIX;
use Data::Dumper;
use IO::Handle;
use Socket;

use constant DATA_FD_OFFSET => $Amanda::Constants::DATA_FD_OFFSET;
use constant DATA_FD_COUNT => $Amanda::Constants::DATA_FD_COUNT;
our @EXPORT_OK = qw(DATA_FD_OFFSET DATA_FD_COUNT);
our %EXPORT_TAGS = ( constants => [ @EXPORT_OK ] );

sub new {
    my $class = shift;
    my %params = @_;

    my $self = bless {
	emulate => $params{'emulate'},
	service => $params{'service'},
	process_done => $params{'process_done'},
	auth => $params{'auth'} || 'bsdtcp',
	args => $params{'args'} || [],
	auth_peer => $params{'auth_peer'},

	# all hashes keyed by stream name
	stream_fds => {},
	outstanding_writes => {},
	close_after_write => {},

	read_buf => {},
	got_eof => {},

	expectations => {},
    }, $class;

    if ($self->{'emulate'} eq 'amandad') {
	$self->_start_process_amandad();
    } elsif ($self->{'emulate'} eq 'inetd') {
	$self->_start_process_inetd();
    } else {
	die "invalid 'emulate' parameter";
    }

    return $self;
}

sub send {
    my $self = shift;
    my ($name, $data) = @_;

    my $fd = $self->{'stream_fds'}{$name}[1];
    die "stream '$name' is not writable"
	unless defined $fd and $fd != -1;

    return if $data eq '';

    $self->{'outstanding_writes'}{$name}++;
    Amanda::MainLoop::async_write(
	fd => $fd,
	data => $data,
	async_write_cb => sub {
	    my ($err, $bytes_written) = @_;
	    die "on stream $name: $err" if $err;

	    $self->_log_data(">>", $name, $data);

	    $self->{'outstanding_writes'}{$name}--;
	    if ($self->{'close_after_write'}{$name}
		    and $self->{'outstanding_writes'}{$name} == 0) {
		$self->_do_close_write($name);
	    }
	});
}

sub connect {
    my $self = shift;
    my ($name, $port) = @_;

    socket(my $child, PF_INET, SOCK_STREAM, getprotobyname('tcp'))
	or die "error creating connect socket: $!";
    connect($child, sockaddr_in($port, inet_aton("127.0.0.1")))
	or die "error connecting: $!";

    # get our own fd for the socket that Perl won't close for us, and
    # close the perl socket
    my $fd = dup(fileno($child));
    close($child);

    $self->_add_stream($name, $fd, $fd);
}

sub close {
    my $self = shift;
    my ($name, $for) = @_;

    die "stream '$name' does not exist"
	unless exists $self->{'stream_fds'}{$name};

    # translate 'b'oth into 'r'ead and 'w'rite
    if ($for eq 'b') {
	$self->close($name, 'r');
	$self->close($name, 'w');
	return;
    }

    if ($for eq 'w') {
	if ($self->{'outstanding_writes'}{$name}) {
	    # close when the writes are done
	    $self->{'close_after_write'}{$name} = 1;
	} else {
	    $self->_do_close_write($name);
	}
    } else {
	$self->_do_close_read($name);
    }
}

sub expect {
    my $self = shift;
    my ($name, @expectations) = @_;

    for my $exp (@expectations) {
	# set up a byte counter for bytes_to_eof
	if ($exp->[0] eq 'bytes_to_eof') {
	    $exp->[2] = 0;
	}
    }

    $self->{'expectations'}{$name} = [ @expectations ];

    $self->_check_expectations($name);
}

sub kill {
    my $self = shift;

    kill 'INT', $self->{'pid'};
}

# private methods

sub _start_process_amandad {
    my $self = shift;
    my $i;

    my $service = "$amlibexecdir/$self->{service}";
    die "service '$service' does not exist" unless -x $service;

    # we'll need some pipes:
    my ($stdin_c, $stdin_p) = POSIX::pipe();
    my ($stdout_p, $stdout_c) = POSIX::pipe();
    my ($stderr_p, $stderr_c) = POSIX::pipe();
    my @data_fdpairs;
    for ($i = 0; $i < DATA_FD_COUNT; $i++) {
	my ($in_c, $in_p) = POSIX::pipe();
	my ($out_p, $out_c) = POSIX::pipe();
	push @data_fdpairs, [ $in_c, $in_p, $out_p, $out_c ];
    }

    # fork and execute!
    $self->{'pid'} = POSIX::fork();
    die "could not fork: $!" if (!defined $self->{'pid'} || $self->{'pid'} < 0);
    if ($self->{'pid'} == 0) {
	# child

	my $fd;
	my $fdpair;

	# First, close all of the fd's we don't need.
	POSIX::close($stdin_p);
	POSIX::close($stdout_p);
	POSIX::close($stderr_p);
	for $fdpair (@data_fdpairs) {
	    my ($in_c, $in_p, $out_p, $out_c) = @$fdpair;
	    POSIX::close($in_p);
	    POSIX::close($out_p);
	}

	# dup our in/out fd's appropriately
	POSIX::dup2($stdin_c, 0);
	POSIX::dup2($stdout_c, 1);
	POSIX::dup2($stderr_c, 2);
	POSIX::close($stdin_c);
	POSIX::close($stdout_c);
	POSIX::close($stderr_c);

	# then make sure everything is greater than the highest
	# fd we'll need
	my @fds_to_close;
	for $fdpair (@data_fdpairs) {
	    my ($in_c, $in_p, $out_p, $out_c) = @$fdpair;
	    while ($in_c < DATA_FD_OFFSET + DATA_FD_COUNT * 2) {
		push @fds_to_close, $in_c;
		$in_c = POSIX::dup($in_c);
	    }
	    while ($out_c < DATA_FD_OFFSET + DATA_FD_COUNT * 2) {
		push @fds_to_close, $out_c;
		$out_c = POSIX::dup($out_c);
	    }
	    $fdpair->[0] = $in_c;
	    $fdpair->[3] = $out_c;
	}

	# close all of the leftovers
	for $fd (@fds_to_close) {
	    POSIX::close($fd);
	}

	# and now use dup2 to move everything to its final location (whew!)
	for ($i = 0; $i < DATA_FD_COUNT; $i++) {
	    my ($in_c, $in_p, $out_p, $out_c) = @{$data_fdpairs[$i]};
	    POSIX::dup2($out_c, DATA_FD_OFFSET + $i*2);
	    POSIX::dup2($in_c, DATA_FD_OFFSET + $i*2 + 1);
	    POSIX::close($out_c);
	    POSIX::close($in_c);
	}

	delete $ENV{'AMANDA_AUTHENTICATED_PEER'};
	$ENV{'AMANDA_AUTHENTICATED_PEER'} = $self->{'auth_peer'} if $self->{'auth_peer'};

	# finally, execute!
	# braces avoid warning
	{ exec { $service } $service, 'amandad', $self->{'auth'}; }
	my $err = "could not execute $service; $!\n";
	POSIX::write(2, $err, length($err));
	exit 2;
    }

    # parent

    # watch for the child to die
    Amanda::MainLoop::call_on_child_termination($self->{'pid'},
	    sub { $self->_process_done(@_); });

    # close all of the fd's we don't need, and make notes of the fd's
    # we want to keep around

    POSIX::close($stdin_c);
    POSIX::close($stdout_c);
    $self->_add_stream('main', $stdout_p, $stdin_p);

    POSIX::close($stderr_c);
    $self->_add_stream('stderr', $stderr_p, -1);

    for ($i = 0; $i < DATA_FD_COUNT; $i++) {
	my ($in_c, $in_p, $out_p, $out_c) = @{$data_fdpairs[$i]};
	POSIX::close($in_c);
	POSIX::close($out_c);

	$self->_add_stream('stream'.($i+1), $out_p, $in_p);
    }
}

sub _start_process_inetd {
    my $self = shift;
    my $i;

    # figure out the service
    my $service = "$amlibexecdir/$self->{service}";
    die "service '$service' does not exist" unless -x $service;

    # set up and bind a listening socket on localhost
    socket(SERVER, PF_INET, SOCK_STREAM, getprotobyname('tcp'))
	or die "creating socket: $!";
    bind(SERVER, sockaddr_in(0, inet_aton("127.0.0.1")))
	or die "binding socket: $!";
    listen(SERVER, 1);
    my ($port, $addr) = sockaddr_in(getsockname(SERVER));

    # fork and execute!
    $self->{'pid'} = POSIX::fork();
    die "could not fork: $!" if ($self->{'pid'} < 0);
    if ($self->{'pid'} == 0) {
	# child

	# send stderr to debug
	Amanda::Debug::debug_dup_stderr_to_debug();

	# wait for a connection on the socket, waiting a long time
	# but not forever..
	alarm 60*60*24; # one day
	my $paddr = accept(CLIENT, SERVER);
	CORE::close(SERVER);
	alarm 0;

	# dup that into stdio
	POSIX::dup2(fileno(CLIENT), 0);
	POSIX::dup2(fileno(CLIENT), 1);
	CORE::close(CLIENT);

	# finally, execute!
	# braces avoid warning
	{ exec { $service } $service, 'installcheck', @{$self->{'args'}}; }
	my $err = "could not execute $service; $!\n";
	POSIX::write(2, $err, length($err));
	exit 2;
    }

    # parent

    # watch for the child to die
    Amanda::MainLoop::call_on_child_termination($self->{'pid'},
	    sub { $self->_process_done(@_); });

    # close the server socket
    CORE::close(SERVER);

    # connect to the child
    $self->connect('main', $port);
}

sub _add_stream {
    my $self = shift;
    my ($name, $rfd, $wfd) = @_;

    if (exists $self->{'stream_fds'}{$name}) {
	die "stream $name already exists";
    }

    $self->{'stream_fds'}{$name} = [ $rfd, $wfd ];
    $self->{'read_sources'}{$name} = undef;
    $self->{'outstanding_writes'}{$name} = 0;
    $self->{'close_after_write'}{$name} = 0;

    # start an async read on every read_fd we set up, after making it not-blocking
    if ($rfd != -1) {
	my $async_read_cb;

	Amanda::Util::set_blocking($rfd, 0);
	$self->{'read_buf'}{$name} = '';
	$self->{'got_eof'}{$name} = 0;

	$async_read_cb = sub {
	    my ($err, $data) = @_;
	    die "on stream $name: $err" if $err;

	    # log it
	    $self->_log_data("<<", $name, $data);

	    # prep for next time
	    if ($data) {
		$self->{'read_sources'}{$name} =
		    Amanda::MainLoop::async_read(
			fd => $rfd,
			async_read_cb => $async_read_cb);
	    } else {
		delete $self->{'read_sources'}{$name};
		$self->_do_close_read($name);
	    }

	    # add the data to the buffer, or signal EOF
	    if ($data) {
		$self->{'read_buf'}{$name} .= $data;
	    } else {
		$self->{'got_eof'}{$name} = 1;
	    }

	    # and call the user function
	    $self->_check_expectations($name);
	};

	$self->{'read_sources'}{$name} =
	    Amanda::MainLoop::async_read(
		fd => $rfd,
		async_read_cb => $async_read_cb);
    }

    # set all the write_fd's to non-blocking too.
    if ($wfd != -1) {
	Amanda::Util::set_blocking($wfd, 0);
    }
}

sub _do_close_read {
    my $self = shift;
    my ($name) = @_;

    my $fds = $self->{'stream_fds'}{$name};

    if ($fds->[0] == -1) {
	die "$name is already closed for reading";
    }

    debug("XX closing $name for reading");

    # remove any ongoing reads
    if ($self->{'read_sources'}{$name}) {
	$self->{'read_sources'}{$name}->remove();
	delete $self->{'read_sources'}{$name};
    }

    # if both fd's are the same, then this is probably a socket, so shut down
    # the read side
    if ($fds->[0] == $fds->[1]) {
	# perl doesn't provide a fd-compatible shutdown, but luckily shudown
	# affects dup'd file descriptors, too!  So create a new handle and shut
	# it down.  When the handle is garbage collected, it will be closed,
	# but that will not affect the original.  This will look strange in an
	# strace, but it works without SWIGging shutdown()
	shutdown(IO::Handle->new_from_fd(POSIX::dup($fds->[0]), "r"), 0);
    } else {
	POSIX::close($fds->[0]);
    }
    $fds->[0] = -1;

    if ($fds->[1] == -1) {
	delete $self->{'stream_fds'}{$name};
    }
}

sub _do_close_write {
    my $self = shift;
    my ($name, $for) = @_;

    my $fds = $self->{'stream_fds'}{$name};

    if ($fds->[1] == -1) {
	die "$name is already closed for writing";
    }

    debug("XX closing $name for writing");

    # if both fd's are the same, then this is probably a socket, so shut down
    # the write side
    if ($fds->[1] == $fds->[0]) {
	# (see above)
	shutdown(IO::Handle->new_from_fd(POSIX::dup($fds->[1]), "w"), 1);
    } else {
	POSIX::close($fds->[1]);
    }
    $fds->[1] = -1;

    if ($fds->[0] == -1) {
	delete $self->{'stream_fds'}{$name};
    }
    delete $self->{'outstanding_writes'}{$name};
    delete $self->{'close_after_write'}{$name};
}

sub _process_done {
    my $self = shift;
    my ($exitstatus) = @_;

    debug("service exit: $exitstatus");

    # delay this to the next trip around the MainLoop, in case data is available
    # on any fd's
    Amanda::MainLoop::call_later(\&_do_process_done, $self, $exitstatus);
}

sub _do_process_done {
    my $self = shift;
    my ($exitstatus) = @_;

    $self->{'process_done_loops'} = ($self->{'process_done_loops'} || 0) + 1;

    # defer with call_after if there are still read fd's open or data in a read
    # buffer.  Since the process just died, presumably these will close in this
    # trip around the MainLoop, so this will be a very short busywait.  The upper
    # bound on the wait is 1 second.
    if ($self->{'process_done_loops'} < 100) {
	my $still_busy = 0;
	for my $name (keys %{$self->{'stream_fds'}}) {
	    my $fds = $self->{'stream_fds'}{$name};
	    # if we're still expecting something on this stream..
	    if ($self->{'expectations'}{$name}) {
		$still_busy = 1;
	    }
	    # or the stream's not closed yet..
	    if ($fds->[0] != -1) {
		$still_busy = 1;
	    }
	}
	if ($still_busy) {
	    return Amanda::MainLoop::call_after(10, \&_do_process_done, $self, $exitstatus);
	}
    }

    # close all of the write_fd's.  If there are pending writes, they
    # were going to get a SIGPIPE anyway.
    for my $name (keys %{$self->{'stream_fds'}}) {
	my $fds = $self->{'stream_fds'}{$name};
	if ($fds->[1] != -1) {
	    $self->_do_close_write($name);
	}
    }

    $self->{'process_done'}->($exitstatus);
}

sub _log_data {
    my $self = shift;
    my ($dir, $name, $data) = @_;

    if ($data) {
	if (length($data) < 300) {
	    my $printable = $data;
	    $printable =~ s/[^\r\n[:print:]]+//g;
	    $printable =~ s/\n/\\n/g;
	    $printable =~ s/\r/\\r/g;
	    debug("$dir$name: [$printable]");
	} else {
	    debug(sprintf("$dir$name: %d bytes", length($data)));
	}
    } else {
	debug("$dir$name: EOF");
    }
}

sub _check_expectations {
    my $self = shift;
    my ($name) = @_;

    my $expectations = $self->{'expectations'}{$name};
    return unless defined $expectations and @$expectations;

    my $cb = undef;
    my @args = undef;
    # if we got EOF and have no more pending data, look for a matching
    # expectation
    if ($self->{'got_eof'}{$name} and !$self->{'read_buf'}{$name}) {
	for my $exp (@$expectations) {
	    if ($exp->[0] eq 'eof') {
		$cb = $exp->[1];
		@args = ();
		last;
	    } elsif ($exp->[0] eq 'bytes_to_eof') {
		$cb = $exp->[1];
		@args = ($exp->[2],); # byte count
		last;
	    }
	}

	if (!$cb) {
	    debug("Expected on $name: " . Dumper($expectations));
	    die "Unexpected EOF on $name";
	}
    } elsif ($self->{'read_buf'}{$name}) {
	my $buf = $self->{'read_buf'}{$name};

	for my $exp (@$expectations) {
	    if ($exp->[0] eq 'eof') {
		die "Expected EOF but got data on $name";
	    } elsif ($exp->[0] eq 'bytes_to_eof') {
		# store the ongoing byte count in the expectation itself
		$exp->[2] = ($exp->[2] || 0) + length($buf);
		$self->{'read_buf'}{$name} = '';
		# and if this stream *also* has EOF, call back
		if ($self->{'got_eof'}{$name}) {
		    $cb = $exp->[1];
		    @args = ($exp->[2],); # byte count
		}
		last;
	    } elsif ($exp->[0] eq 'bytes') {
		if (length($buf) >= $exp->[1]) {
		    $cb = $exp->[2];
		    @args = (substr($buf, 0, $exp->[1]),);
		    $self->{'read_buf'}{$name} = substr($buf, $exp->[1]);
		}
		last; # done searching, even if we don't call a sub
	    } elsif ($exp->[0] eq 're') {
		if ($buf =~ $exp->[1]) {
		    $cb = $exp->[2];
		    @args = ($&,); # matched section of $buf
		    $self->{'read_buf'}{$name} = $'; # remainder of $buf
		    last;
		}
	    }
	}
    }

    # if there's a callback to make, then remove the expectations *before*
    # calling it
    if ($cb) {
	delete $self->{'expectations'}{$name};
	$cb->(@args);
    }
}

1;
