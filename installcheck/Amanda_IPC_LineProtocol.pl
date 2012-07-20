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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use Test::More tests => 6;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Amanda::IPC::LineProtocol;
use IO::Handle;
use Amanda::MainLoop;
use Amanda::Debug;
use Data::Dumper;
use Carp;

##
# Define a test protocol

package TestProtocol;
use base "Amanda::IPC::LineProtocol";
use Amanda::IPC::LineProtocol;

use constant SIMPLE => message("SIMPLE",
    format => [ qw( ) ],
);

use constant FOO => message("FOO",
    format => [ qw( name? nicknames* ) ],
);

use constant FO => message("FO",  # prefix of "FOO"
    format => [ qw( ) ],
);

use constant ASSYM => message("ASSYM",
    format => {
	in => [ qw( a b ) ],
	out => [ qw( x ) ],
    },
);

use constant BAR => message("BAR",
    match => qr/^BA[Rh]$/i, # more elaborate regex
    format => [ qw( mandatory optional? ) ],
);

use constant QUIT => message("QUIT",
    match => qr/^QUIT$/i,
    on_eof => 1,
    format => [ qw( reason? ) ],
);

package main;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

# run $code in a separate process, with read and write handles hooked up, and returns
# read and write handles.
sub in_fork {
    my ($code) = @_;

    my ($parent_read, $child_write) = POSIX::pipe();
    my ($child_read, $parent_write) = POSIX::pipe();

    my $pid = fork();
    if (!defined($pid) or $pid < 0) {
        die("Can't fork: $!");
    }

    if (!$pid) {
        ## child

	# get our file-handle house in order
	POSIX::close($parent_read);
	POSIX::close($parent_write);

	$code->(IO::Handle->new_from_fd($child_read, "r"),
		IO::Handle->new_from_fd($child_write, "w"));
	POSIX::exit(0);
    }

    ## parent

    POSIX::close($child_read);
    POSIX::close($child_write);

    return (IO::Handle->new_from_fd($parent_read, "r"),
	    IO::Handle->new_from_fd($parent_write, "w"),
	    $pid);
}

# generic "die" message_cb
my $message_cb = make_cb(message_cb => sub {
    my ($msgtype, %params) = @_;
    if (defined $msgtype) {
	diag(Dumper(\%params));
	die("unhandled message: $msgtype");
    } else {
	die("IPC error: $params{'error'}");
    }
});

##
# Run some tests

my $proto;
my @events;
my ($rx_fh, $tx_fh, $pid);

# on QUIT, stop the protocol and quit the mainloop
my $quit_cb = make_cb(quit_cb => sub {
    push @events, [ @_ ];
    $proto->stop(finished_cb => sub {
	Amanda::MainLoop::quit();
    });
});


#
# test a simple "QUIT"

@events = ();
($rx_fh, $tx_fh, $pid) = in_fork(sub {
    my ($rdh, $wrh) = @_;
    $wrh->autoflush(1);

    $rdh->getline(); # get 'start\n'
    $wrh->write("QUIT \"just because\"");
});

$proto = TestProtocol->new(
    rx_fh => $rx_fh, tx_fh => $tx_fh,
    message_cb => $message_cb);
$proto->set_message_cb(TestProtocol::QUIT, $quit_cb);
Amanda::MainLoop::call_later(sub {
    $tx_fh->autoflush(1);
    $tx_fh->write("start\n");
});
Amanda::MainLoop::run();
waitpid($pid, 0);

is_deeply([ @events ],
    [
	[ "QUIT", reason => "just because" ],
	[ "QUIT" ],
    ],
    "correct events for a simple 'QUIT \"just because\"")
    or diag(Dumper(\@events));


##
# test a bogus message

@events = ();
($rx_fh, $tx_fh, $pid) = in_fork(sub {
    my ($rdh, $wrh) = @_;
    $wrh->autoflush(1);

    $rdh->getline(); # get 'start\n'
    $wrh->write("SNARSBLAT, yo");
});

$proto = TestProtocol->new(
    rx_fh => $rx_fh, tx_fh => $tx_fh,
    message_cb => sub { push @events, [ @_ ]; });
$proto->set_message_cb(TestProtocol::QUIT, $quit_cb);
Amanda::MainLoop::call_later(sub {
    $tx_fh->autoflush(1);
    $tx_fh->write("start\n");
});
Amanda::MainLoop::run();
waitpid($pid, 0);

is_deeply([ @events ],
    [
	[ undef, 'error' => 'unknown command' ],
	[ "QUIT" ], # from EOF
    ],
    "bogus message handled correctly")
    or diag(Dumper(\@events));


##
# a more complex conversation

@events = ();
($rx_fh, $tx_fh, $pid) = in_fork(sub {
    my ($rdh, $wrh) = @_;
    $wrh->autoflush(1);

    $wrh->write("FOO\n");
    $rdh->getline() =~ /SIMPLE/ or die("bad response");

    $wrh->write("FOO one\n");
    $rdh->getline() =~ /SIMPLE/ or die("bad response");

    $wrh->write("FOO one \"t w o\"\n");
    $rdh->getline() =~ /SIMPLE/ or die("bad response");

    $wrh->write("FOO one \"t w o\" three\n");
    $rdh->getline() =~ /SIMPLE/ or die("bad response");
});

$proto = TestProtocol->new(
    rx_fh => $rx_fh, tx_fh => $tx_fh,
    message_cb => $message_cb);
$proto->set_message_cb(TestProtocol::QUIT, $quit_cb);
$proto->set_message_cb(TestProtocol::FOO, sub {
    push @events, [ shift @_, { @_ } ];
    $proto->send(TestProtocol::SIMPLE);
});
Amanda::MainLoop::run();
waitpid($pid, 0);

is_deeply([ @events ],
    [
	[ "FOO", { nicknames => [] } ],
	[ "FOO", { nicknames => [], name => "one" } ],
	[ "FOO", { nicknames => [ "t w o" ], name => "one" } ],
	[ "FOO", { nicknames => [ "t w o", "three" ], name => "one" } ],
	[ "QUIT" ],
    ],
    "correct events for a few conversation steps, parsing")
    or diag(Dumper(\@events));

##
# Asymmetrical formats

@events = ();
($rx_fh, $tx_fh, $pid) = in_fork(sub {
    my ($rdh, $wrh) = @_;
    $wrh->autoflush(1);

    $wrh->write("ASSYM 1 2\n");
    $rdh->getline() =~ /ASSYM a/ or die("bad response");
});

$proto = TestProtocol->new(
    rx_fh => $rx_fh, tx_fh => $tx_fh,
    message_cb => $message_cb);
$proto->set_message_cb(TestProtocol::QUIT, $quit_cb);
$proto->set_message_cb(TestProtocol::ASSYM, sub {
	push @events, [ shift @_, { @_ } ];
        $proto->send(TestProtocol::ASSYM, x => "a");
    });
Amanda::MainLoop::run();
waitpid($pid, 0);

is_deeply([ @events ],
    [
	[ "ASSYM", { a => "1", b => "2" } ],
	[ "QUIT" ],
    ],
    "correct events for asymmetric message format")
    or diag(Dumper(\@events));


##
# test queueing up of messages on writing.

# The idea here is to write more than a pipe buffer can hold, while the child
# process does not read that data, and then to signal the child process,
# causing it to read all of that data, write a reply, and exit.  Recent linuxes
# have a pipe buffer of 64k, so we exceed that threshold.  We use an 'alarm' to
# fail in the case that this blocks.

my $NMSGS = 10000;

@events = ();
($rx_fh, $tx_fh, $pid) = in_fork(sub {
    my ($rdh, $wrh) = @_;
    $wrh->autoflush(1);

    # on USR1, read lots of inputs
    $SIG{'USR1'} = sub {
	for (my $i = 0; $i < $NMSGS; $i++) {
	    $rdh->getline();
	}

	# send a message that the parent can hope to get
	$wrh->write("BAR \"got your inputs\"\n");

	# and bail out
	POSIX::exit(0);
    };

    $wrh->write("SIMPLE\n");

    # and sleep forever, or until killed.
    while (1) { sleep(100); }
});

$proto = TestProtocol->new(
    rx_fh => $rx_fh, tx_fh => $tx_fh,
    message_cb => $message_cb);
$proto->set_message_cb(TestProtocol::QUIT, $quit_cb);
$proto->set_message_cb(TestProtocol::SIMPLE, sub {
	push @events, [ shift @_ ];
	# send $NMSGS messages to the child, which isn't listening yet!
	for (my $i = 0; $i < $NMSGS; $i++) {
	    $proto->send(TestProtocol::SIMPLE);
	}
	# and then send it SIGUSR1, so it reads those
	kill USR1 => $pid;
    });
$proto->set_message_cb(TestProtocol::BAR, sub {
	push @events, [ shift @_, { @_ } ];
    });

# die after 10 minutes
alarm 600;

Amanda::MainLoop::run();
waitpid($pid, 0);
alarm 0; # cancel the alarm

is_deeply([ @events ],
    [
	[ "SIMPLE" ],
	[ "BAR", { mandatory => "got your inputs" } ],
	[ "QUIT" ],
    ],
    "write buffering handled correctly")
    or diag(Dumper(\@events));

##
# test the message_obj functionality

package main::MessageObj;

sub msg_FOO {
    my $self = shift;
    push @{$self}, [ shift @_, { @_ } ];
    $proto->send(TestProtocol::SIMPLE);
}

sub msg_BAR {
    my $self = shift;
    push @{$self}, [ shift @_, { @_ } ];
    $proto->send(TestProtocol::SIMPLE);
}

package main;

@events = ();
($rx_fh, $tx_fh, $pid) = in_fork(sub {
    my ($rdh, $wrh) = @_;
    $wrh->autoflush(1);

    $wrh->write("FOO\n");
    $rdh->getline() =~ /SIMPLE/ or die("bad response");

    $wrh->write("BAR one\n");
    $rdh->getline() =~ /SIMPLE/ or die("bad response");

    $wrh->write("BAH one \"t w o\"\n"); # note alternate spelling "BAH"
    $rdh->getline() =~ /SIMPLE/ or die("bad response");

    $wrh->write("FOO one \"t w o\" three\n");
    $rdh->getline() =~ /SIMPLE/ or die("bad response");
});

$proto = TestProtocol->new(
    rx_fh => $rx_fh, tx_fh => $tx_fh,
    message_obj => bless(\@events, "main::MessageObj"));
$proto->set_message_cb(TestProtocol::QUIT, $quit_cb);
Amanda::MainLoop::run();
waitpid($pid, 0);

is_deeply([ @events ],
    [ [ 'FOO', { 'nicknames' => [] } ],
      [ 'BAR', { 'mandatory' => 'one' } ],
      [ 'BAR', { 'mandatory' => 'one', 'optional' => 't w o' } ],
      [ 'FOO', { 'name' => 'one', 'nicknames' => [ 't w o', 'three' ] } ],
      [ 'QUIT' ],
    ],
    "message_obj works")
    or diag(Dumper(\@events));

