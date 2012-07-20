# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::IPC::LineProtocol;
=head1 NAME

Amanda::IPC::LineProtocol -- parent class for line-based protocols

=head1 SYNOPSIS

Define your protocol:

    packge MyProtocol;
    use Amanda::IPC::LineProtocol;
    use base "Amanda::IPC::LineProtocol";

    use constant SETSTATUS => message("SETSTATUS",
	match => qr/^FOO$/i,
	format => [ qw( param1 param2 optional? list* ) ],
    );
    use constant PING => message("PING",
	match => qr/^PING$/i,
	format => [ qw( id ) ],
    );
    use constant PONG => message("PONG",
	match => qr/^PONG$/i,
	format => [ qw( id ) ],
    );
    # ...

    # And use the protocol
    package main;
    my $input_fh = IO::Handle->new(...);
    my $output_fh = IO::Handle->new(...);
    my $proto;

    my $ping_cb = make_cb(ping_cb => sub {
	my ($msg, %args) = @_;
	$proto->send(MyProtocol::PONG, id => $args{'id'});
    });

    my $message_cb = make_cb(message_cb => sub {
	my ($msg, %args) = @_;
	if (!$msg) {
	    die $args{'error'};
	}
    });

    $proto = MyProtocol->new(
	    rx_fh => $input_fh,
	    tx_fh => $output_fh,
	    message_cb => $message_cb);

    # and add callbacks
    $proto->set_message_cb(MyProtocol::PING, $ping_cb);
    $proto->set_message_cb(MyProtocol::PONG, $pong_cb);

    # or send messages to an object, with method names based on
    # the message name
    sub msg_PONG {
	my $self = shift;
	my ($msg, %args) = @_;
    }
    # ..
    $proto = MyProtocol->new( # ..
	    message_obj => $self);

    # send a message
    $proto->send(MyProtocol::SETSTATUS,
	param1 => "x",
	param2 => "y",
	);

    # shut down the protocol, flushing any messages waiting to
    # be sent first
    my $finished_cb = make_cb(finished_cb => sub {
	my ($err) = @_;
	# ...
    });
    $proto->stop(finished_cb => $finished_cb);

=head1 DESCRIPTION

This library is used to implement communications between Amanda processes.
Amanda has historically implemented a number of distinct text-based protocols
for communications between various components, and this library servces to
abstract and centralize the implementation of those protocols.

The package supports point-to-point, message-based, symmetric protocols.  Two
communicating processes exchange discrete messages, and in principle either
process can send a message at any time, although this is limited by the (often
unwritten) rules of the protocol.

In protocols based on this package, each message is a single text line,
terminated with a newline and consisting of a sequence of quoted strings.  The
first string determines the type of message.  For example:

  SEND-MORE-MONEY $150.00 "Books and pencils"
  ORDER-PIZZA Large Mushrooms Olives Onions "Green Peppers"

The package is asynchronous (see L<Amanda::MainLoop>), triggering callbacks for
incoming messages rather than implementing a C<get_message> method or the like.
If necessary, outgoing messages are queued for later transmission, thus
avoiding deadlocks from full pipe buffers.  This allows processing to continue
unhindered in both processes while messages are in transit in either direction.

=head2 DEFINING A PROTOCOL

There are two parts to any use of this package.  First, define the protocol by
creating a subclass and populating it using the C<message> package method.
This begins with something like

  package CollegeProtocol;
  use base "Amanda::IPC::LineProtocol";
  use Amanda::IPC::LineProtocol;

The usual trick for specifying messages is to simultaneously define a series of
constants, using the following idiom:

  use constant ORDER_PIZZA => message("ORDER-PIZZA",
    match => qr/^ORDER-PIZZA$/,
    format => [ qw( size toppings* ) ],
  );

The first argument to C<message> is the word with which this particular message
type will be sent.  The C<match> parameter gives a regular expression which
will be used to recognize incoming messages of this type.   If this parameter
is not specified, the default is to match the first argument with a
case-insensitive regexp.

The C<format> parameter describes the format of the arguments for this message
type.  A format parameter with the C<*> suffix gathers all remaining arguments
into a list.  A C<?> suffix indicates an optional parameter.  Note that it is
quite possible to specify ambiguous formats which will not work like you
expect.  The default format is an empty list (taking no arguments).

The optional C<on_eof> parameter will cause a a message of this type to be
generated on EOF.  For example, with:

  use constant DROP_OUT => message("DROP-OUT",
    on_eof => 1,
  );

when an EOF is detected, a C<DROP_OUT> message will be generated.

The protocol class should contain, in POD, a full description of the syntax of
the protcol -- which messages may be sent when, and what they mean.  No
facility is provided to encode this description in perl.

In general, protocols are expected to be symmetrical -- any message can either
be sent or received.  However, some existing protocols use different formats in
different directions.  In this case, specify C<format> as a hashref with keys
C<in> and C<out> pointing to the two different formats:

  use constant ERROR => message("ERROR",
    match => qr/^ERROR$/,
    format => { in => [ qw( message severity ) ],
		out => [ qw( component severity message ) ] },
  );

=head2 USING A PROTOCOL

Once a protocol is defined, it forms a class which can be used to run the
protocol.  Multiple instances of this class can be created to handle
simultaneous uses of the protocol over different channels.

The constructor, C<new>, takes two C<IO::Handle> objects -- one to read from
(C<rx_fh>) and one to write to (C<tx_fh>).  In some cases (e.g., a socket),
these may be the same handle.  It takes an optional callback, C<message_cb>,
which will be called for any received messages not handled by a more specific
callback.  Any other parameters are considered message-type-specific callbacks.

For example, given a socket handle C<$sockh>, the following will start the
C<CollegeProtocol> running on that socket:

  my $proto = CollegeProtocol->new(
    rx_fh => $sockh,
    tx_fh => $sockh,
  );
  $proto->set_message_cb(CollegeProtocol::PIZZA_DELIVERY, $pizza_delivery_cb);

For protocols with a lot of message types, it may be useful to have the
protocol call methods on an object.  This is done with the C<message_obj>
argument to the protocol constructor:

  $proto = CollegeProtocol->new( # ..
    message_obj => $obj);

The methods are named C<msg_$msgname>, where $msgname has all non-identifier
characters translated to an underscore (C<_>).  For situations where the meaning
of a message can change dynamically, it may be useful to set a callback after
the object has been crated:

  $proto->set_message_cb(CollegeProtocol::MIDTERM,
    sub { ... });

The constructor also takes a 'debug' argument; if given, then all incoming and
outgoing messages will be written to the debug log with this argument as
prefix.

All message callbacks have the same signature:

  my $pizza_delivery_cb = make_cb(pizza_delivery_cb => sub {
    # (note that object methods will get the usual $self)
    my ($msgtype, %params) = @_;
  });

where C<%params> contains all of the arguments to the message, keyed by the
argument names given in the message's C<format>.  Note that parameters
specified with the C<*> suffix will appear as arrayrefs.

Callbacks specified with C<set_message_cb> take precedence over other
specifications; next are message-specific callbacks given to the constructor,
followed by C<message_obj>, and finally C<message_cb>.

In case of an error, the C<message_cb> (if specified) is called with
C<$msgtype> undefined and with a single parameter named C<error> giving the
error message.  This generally indicates either an unknown or badly-formatted
message.

To send a message, use the C<send> method, which takes the same arguments as a
message callback:

  $proto->send(CollegeProtocol::SEND_MORE_MONEY,
    how_much => "$150.00",
    what_for => "Books and pencils");

=cut

use Exporter ();
our @ISA = qw( Exporter );
our @EXPORT = qw( message new );

use IO::Handle;
use POSIX qw( :errno_h );
use strict;
use warnings;
use Carp;

use Amanda::Debug qw( debug );
use Amanda::MainLoop qw( :GIOCondition make_cb );
use Amanda::Util;

##
# Package methods to support protocol definition

my %msgspecs_by_protocol;
sub message {
    my ($name, @params) = @_;

    my $msgspec = $msgspecs_by_protocol{caller()}->{$name} = { @params };

    # do some parameter sanity checks
    my $param;
    my @allowed_params = qw( match format on_eof );
    for $param (keys %$msgspec) {
	die "invalid message() parameter '$param'"
	    unless grep { $_ eq $param } @allowed_params;
    }

    # normalize the results a little bit
    $msgspec->{'name'} = $name;

    if (!exists $msgspec->{'match'}) {
	$msgspec->{'match'} = qr/^$msgspec->{'name'}$/i;
    }
    if (!exists $msgspec->{'format'}) {
	$msgspec->{'format'} = [];
    }

    # calculate a method name
    my $methname = "msg_$name";
    $methname =~ tr/a-zA-Z0-9/_/c;
    $msgspec->{'methname'} = $methname;

    return $name;
}

##
# class methods

sub new {
    my $class = shift;
    my %params = @_;

    my $self = bless {
	stopped => 0,
	debug => $params{'debug'},

	rx_fh => $params{'rx_fh'},
	rx_fh_tty => 0,
	rx_buffer => '',
	rx_source => undef,

	tx_fh => $params{'tx_fh'},
	tx_fh_tty => 0,
	tx_source => undef,
	tx_finished_cb => undef,
	tx_outstanding_writes => 0,

	cmd_cbs => {},
	message_obj => $params{'message_obj'},
	default_cb => $params{'message_cb'},

	# a ref to the existing structure
	msgspecs => $msgspecs_by_protocol{$class},
    }, $class;

    # set nonblocking mode on both file descriptor, but only for non-tty
    # handles -- non-blocking tty's don't work well at all.
    if (POSIX::isatty($self->{'rx_fh'}->fileno())) {
	$self->{'rx_fh_tty'} = 1;
    } else {
	if (!defined($self->{'rx_fh'}->blocking(0))) {
	    die("Could not make protocol filehandle non-blocking");
	}
    }

    if (POSIX::isatty($self->{'tx_fh'}->fileno())) {
	$self->{'tx_fh_tty'} = 1;
    } else {
	if (!defined($self->{'tx_fh'}->blocking(0))) {
	    die("Could not make protocol filehandle non-blocking");
	}
    }

    # start reading..
    $self->{'rx_source'} = Amanda::MainLoop::async_read(
	fd => $self->{'rx_fh'}->fileno(),
	async_read_cb => sub { $self->_async_read_cb(@_); });

    return $self;
}

sub set_message_cb {
    my $self = shift;
    my ($name, $message_cb) = @_;

    $self->{'cmd_cbs'}->{$name} = $message_cb;
}

sub stop {
    my $self = shift;
    my %params = @_;

    $self->{'stopped'} = 1;

    # abort listening for incoming data
    if (defined $self->{'rx_source'}) {
	$self->{'rx_source'}->remove();
    }

    # and flush any outgoing messages
    if ($self->{'tx_outstanding_writes'} > 0) {
	$self->{'tx_finished_cb'} = $params{'finished_cb'};
    } else {
	$params{'finished_cb'}->();
    }
}

sub send {
    my $self = shift;
    my ($name, %info) = @_;

    my $msgspec = $self->{'msgspecs'}->{$name};
    die "No message spec for '$name'" unless defined($msgspec);

    my @line = $msgspec->{'name'};

    my $format = $msgspec->{'format'};
    $format = $format->{'out'} if (ref $format eq "HASH");

    for my $elt (@$format) {
	my ($name, $kind)= ($elt =~ /^(.*?)([*?]?)$/);
	my $val = $info{$name};
	if (!defined $val) {
	    croak "Value for '$name' is undefined";
	}

	if ($kind eq "*") {
	    croak "message key '$name' must be an array"
		unless defined $val and ref($val) eq "ARRAY";
	    push @line, @$val;
	} else {
	    croak "message key '$name' is required"
		unless defined $val or $kind eq "?";
	    push @line, $val if defined $val;
	}
    }

    my $line = join(" ", map { Amanda::Util::quote_string("$_") } @line);
    debug($self->{'debug'} . " >> $line") if ($self->{'debug'});
    $line .= "\n";

    ++$self->{'tx_outstanding_writes'};
    my $write_done_cb = make_cb(write_done_cb => sub {
	my ($err, $nbytes) = @_;

	if ($err) {
	    # TODO: handle this better
	    die $err;
	}

	# call the protocol's finished_cb if necessary
	if (--$self->{'tx_outstanding_writes'} == 0 and $self->{'tx_finished_cb'}) {
	    $self->{'tx_finished_cb'}->();
	}
    });
    $self->{'tx_source'} = Amanda::MainLoop::async_write(
	fd => $self->{'tx_fh'}->fileno(),
	data => $line,
	async_write_cb => $write_done_cb);
}

##
# Handle incoming messages

sub _find_msgspec {
    my $self = shift;
    my ($cmdstr) = @_;

    for my $msgspec (values %{$self->{'msgspecs'}}) {
	my $match = $msgspec->{'match'};
	next unless defined($match);
	return $msgspec if ($cmdstr =~ $match);
    }

    return undef;
}

sub _parse_line {
    my $self = shift;
    my ($msgspec, @line) = @_;

    # parse the message according to the "in" format
    my $format = $msgspec->{'format'};
    $format = $format->{'in'} if (ref $format eq "HASH");

    my $args = {};
    for my $elt (@$format) {
	my ($name, $kind)= ($elt =~ /^(.*?)([*?]?)$/);

	if ($kind eq "*") {
	    $args->{$name} = [ @line ];
	    @line = ();
	    last;
	}

	next if ($kind eq "?" and !@line);

	if (!@line) {
	    return "too few arguments to '$msgspec->{name}': first missing argument is $name";
	}

	$args->{$name} = shift @line;
    }

    if (@line) {
	return "too many arguments to '$msgspec->{name}': first unmatched argument is '$line[0]'";
    }

    return (undef, $args);
}

sub _call_message_cb {
    my $self = shift;
    my ($msgspec, $line, $args) = @_;

    # after the user calls stop(), don't call any more callbacks
    return if $self->{'stopped'};

    # send a bogus line message to the default_cb if there's no msgspec
    if (!defined $msgspec) {
	if ($self->{'default_cb'}) {
	    $self->{'default_cb'}->(undef, %$args);
	} else {
	    debug("IPC: " . ($args->{'error'} or "bogus line '$line'"));
	}
	return;
    }

    # otherwise, call the relevant callback
    if (exists $self->{'cmd_cbs'}{$msgspec->{'name'}}) {
	return $self->{'cmd_cbs'}{$msgspec->{'name'}}->($msgspec->{'name'}, %$args);
    }
    
    if (defined $self->{'message_obj'} and $self->{'message_obj'}->can($msgspec->{'methname'})) {
	my $methname = $msgspec->{'methname'};
	return $self->{'message_obj'}->$methname($msgspec->{'name'}, %$args);
    } 
    
    if ($self->{'default_cb'}) {
	return $self->{'default_cb'}->($msgspec->{'name'}, %$args);
    }

    warn "IPC: Ignored unhandled line '$line'";
}

sub _incoming_line {
    my $self = shift;
    my ($line) = @_;

    $line =~ s/\n//g;
    return unless $line;

    debug($self->{'debug'} . " << $line") if ($self->{'debug'});

    # turn the line into a list of strings..
    my @line = Amanda::Util::split_quoted_strings($line);
    return unless @line;

    # get the specification for this message
    my $msgspec = $self->_find_msgspec(shift @line);
    if (!defined $msgspec) {
	$self->_call_message_cb(undef, $line, {error => 'unknown command'});
	return;
    }

    my ($parserr, $args) = $self->_parse_line($msgspec, @line);
    if ($parserr) {
	$self->_call_message_cb(undef, $line, {error => $parserr});
	return;
    }

    $self->_call_message_cb($msgspec, $line, $args);
}

sub _incoming_eof {
    my $self = shift;

    # handle a final line, even without a newline (is this wise?)
    if ($self->{'rx_buffer'} ne '') {
	$self->_incoming_line($self->{'rx_buffer'} . "\n");
    }

    # find the EOF msgspec and call it
    for my $msgspec (values %{$self->{'msgspecs'}}) {
	if ($msgspec->{'on_eof'}) {
	    $self->_call_message_cb($msgspec, "(EOF)", {});
	    last;
	}
    }
}

sub _async_read_cb {
    my $self = shift;
    my ($err, $data) = @_;

    if (defined $err) {
	# TODO: call an error_handler given to new()?
	die $err;
    }

    if (!$data) {
	$self->_incoming_eof();
	return;
    }

    # set up to read the next chunk
    $self->{'rx_source'} = Amanda::MainLoop::async_read(
	fd => $self->{'rx_fh'}->fileno(),
	async_read_cb => sub { $self->_async_read_cb(@_); });

    # and process this data
    $self->{'rx_buffer'} .= $data;

    while ($self->{'rx_buffer'} =~ /\n/) {
	my ($line, $rest) = split '\n', $self->{'rx_buffer'}, 2;
	$self->{'rx_buffer'} = $rest;
	$self->_incoming_line($line);
    }
}

1;
