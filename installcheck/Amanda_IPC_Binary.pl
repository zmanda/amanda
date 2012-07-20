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

use Test::More tests => 15;
use strict;
use warnings;

use lib "@amperldir@";
use Installcheck;
use Amanda::IPC::Binary;
use IO::Handle;
use Amanda::Debug;
use Data::Dumper;
use Carp;
use POSIX;

##
# Define a test protocol

package TestProtocol;
use base "Amanda::IPC::Binary";
use Amanda::IPC::Binary;

# cmd_id's
use constant SIMPLE => 1;
use constant FOO => 2;
use constant BAR => 3;

# arg_id's
use constant NAME => 1;
use constant NICKNAME => 2;
use constant MANDATORY => 3;
use constant OPTIONAL => 4;

magic(0x1234);

command(SIMPLE);

command(FOO,
    NAME, $IPC_BINARY_STRING,
    NICKNAME, $IPC_BINARY_STRING);

command(BAR,
    MANDATORY, 0,
    OPTIONAL, $IPC_BINARY_OPTIONAL);

package main;

# set up debugging so debug output doesn't interfere with test results
Amanda::Debug::dbopen("installcheck");
Installcheck::log_test_output();

# and disable Debug's die() and warn() overrides
Amanda::Debug::disable_die_override();

sub to_bytes {
    my @result;
    for my $byte (@_) {
	if (length($byte) == 1) {
	    push @result, $byte;
	} else {
	    push @result, chr(hex($byte));
	}
    }
    return join('', @result);
}

sub to_hex {
    my ($bytes) = @_;
    my @result;
    for my $byte (split //, $bytes) {
	if ((ord($byte) >= ord('a') and ord($byte) <= ord('z')) || $byte eq '-') {
	    push @result, $byte;
	} else {
	    push @result, sprintf("%02x", ord($byte));
	}
    }

    return join(" ", @result);
}

# first, try reading a pre-defined sequence of bytes; this lets us make sure
# byte-ordering is right on all platforms.
my $fh;
my $tmpfile = "$Installcheck::TMP/ipc-binary-test";
my ($chan, $msg);

open($fh, ">", $tmpfile);
print $fh to_bytes(
    qw(12 34), # magic
    qw(00 01), # cmd_id = SIMPLE
    qw(00 00 00 0A), # length
    qw(00 00), # count

    qw(12 34), # magic
    qw(00 02), # cmd_id = FOO
    qw(00 00 00 22), # length
    qw(00 02), # n_args
    qw(00 00 00 07), # length
    qw(00 01), # arg_id = NAME
    qw(n i k o l a s), # data
    qw(00 00 00 05), # length
    qw(00 02), # arg_id = NICKNAME
    qw(a t r u s), # data

    qw(12 34), # magic
    qw(00 03), # cmd_id = BAR
    qw(00 00 00 1f), # length
    qw(00 01), # n_args
    qw(00 00 00 0f), # length
    qw(00 03), # arg_id = MANDATORY
    qw(v e r b o d e n - v r u c h t), # data

    qw(12 34), # magic
    qw(00 03), # cmd_id = BAR
    qw(00 00 00 29), # length
    qw(00 02), # n_args
    qw(00 00 00 0a), # length
    qw(00 03), # arg_id = MANDATORY
    qw(o u d e - g e u z e), # data
    qw(00 00 00 09), # length
    qw(00 04), # arg_id = OPTIONAL
    qw(r o d e n b a c h), # data
);
close($fh);

open($fh, "<", $tmpfile);

$chan = TestProtocol->new();

$msg = $chan->read_message($fh);
is($msg->{'cmd_id'}, TestProtocol::SIMPLE,
    "got SIMPLE");

$msg = $chan->read_message($fh);
is($msg->{'cmd_id'}, TestProtocol::FOO,
    "got FOO");
is($msg->{'args'}[TestProtocol::NAME], "nikolas",
    "got NAME arg");
is($msg->{'args'}[TestProtocol::NICKNAME], "atrus",
    "got NICKNAME arg");

$msg = $chan->read_message($fh);
is($msg->{'cmd_id'}, TestProtocol::BAR,
    "got BAR");
is($msg->{'args'}[TestProtocol::MANDATORY], "verboden-vrucht",
    "got MANDATORY arg");
is($msg->{'args'}[TestProtocol::OPTIONAL], undef,
    "got no OPTIONAL arg");

$msg = $chan->read_message($fh);
is($msg->{'cmd_id'}, TestProtocol::BAR,
    "got BAR");
is($msg->{'args'}[TestProtocol::MANDATORY], "oude-geuze",
    "got MANDATORY arg");
is($msg->{'args'}[TestProtocol::OPTIONAL], "rodenbach",
    "got OPTIONAL arg");

$msg = $chan->read_message($fh);
is($msg, undef, "no more messages");

close($fh);

# now try writing a set of messages, and check that the result is what it should be
open($fh, ">", $tmpfile);
$chan = TestProtocol->new();

ok($chan->write_message($fh, $chan->message(
	TestProtocol::FOO,
	TestProtocol::NAME, "james",
	TestProtocol::NICKNAME, "jimmy")),
    "wrote FOO message");

ok($chan->write_message($fh, $chan->message(
	TestProtocol::BAR,
	TestProtocol::MANDATORY, "absolutely",
	TestProtocol::OPTIONAL, "maybe")),
    "wrote BAR message with optional arg");

ok($chan->write_message($fh, $chan->message(
	TestProtocol::BAR,
	TestProtocol::MANDATORY, "yessir")),
    "wrote BAR message without optional arg");

$chan->close();
close($fh);

my $bytes_expected = to_bytes(
    qw(12 34), # magic
    qw(00 02), # cmd_id = FOO
    qw(00 00 00 20), # length
    qw(00 02), # n_args
    qw(00 00 00 05), # length
    qw(00 01), # arg_id = NAME
    qw(j a m e s), # data
    qw(00 00 00 05), # length
    qw(00 02), # arg_id = NICKNAME
    qw(j i m m y), # data

    qw(12 34), # magic
    qw(00 03), # cmd_id = BAR
    qw(00 00 00 25), # length
    qw(00 02), # n_args
    qw(00 00 00 0a), # length
    qw(00 03), # arg_id = MANDATORY
    qw(a b s o l u t e l y), # data
    qw(00 00 00 05), # length
    qw(00 04), # arg_id = OPTIONAL
    qw(m a y b e), # data

    qw(12 34), # magic
    qw(00 03), # cmd_id = BAR
    qw(00 00 00 16), # length
    qw(00 01), # n_args
    qw(00 00 00 06), # length
    qw(00 03), # arg_id = MANDATORY
    qw(y e s s i r), # data
);

# slurp the contents of the temp file and see if it matches
open($fh, "<", $tmpfile);
my $bytes_written = do { local $/; <$fh> };
close($fh);

is(to_hex($bytes_written),
   to_hex($bytes_expected),
    "got the expected bytes");
