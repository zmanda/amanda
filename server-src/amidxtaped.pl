#! @PERL@
# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;
use warnings;

##
# Interactivity class

package Amanda::Interactivity::amidxtaped;
use base 'Amanda::Interactivity';
use Amanda::Util qw( weaken_ref );
use Amanda::MainLoop;
use Amanda::Feature;
use Amanda::Debug qw( debug );
use Amanda::Config qw( :getconf );
use Amanda::Recovery::Scan qw( $DEFAULT_CHANGER );

sub new {
    my $class = shift;
    my %params = @_;

    my $self = {
	clientservice => $params{'clientservice'},
    };

    # (weak ref here to eliminate reference loop)
    weaken_ref($self->{'clientservice'});

    return bless ($self, $class);
}

sub abort() {
    my $self = shift;

    debug("ignoring spurious Amanda::Recovery::Scan abort call");
}

sub user_request {
    my $self = shift;
    my %params = @_;
    my $buffer = "";

    my $steps = define_steps
	cb_ref => \$params{'request_cb'};

    step send_message => sub {
	if ($params{'err'}) {
	    $self->{'clientservice'}->sendmessage("$params{err}");
	}

	$steps->{'check_fe_feedme'}->();
    };

    step check_fe_feedme => sub {
	# note that fe_amrecover_FEEDME implies fe_amrecover_splits
	if (!$self->{'clientservice'}->{'their_features'}->has(
				    $Amanda::Feature::fe_amrecover_FEEDME)) {
	    return $params{'request_cb'}->("remote cannot prompt for volumes", undef);
	}
	$steps->{'send_feedme'}->();
    };

    step send_feedme => sub {
	$self->{'clientservice'}->sendctlline("FEEDME $params{label}\r\n", $steps->{'read_response'});
    };

    step read_response => sub {
	my ($err, $written) = @_;
	return $params{'request_cb'}->($err, undef) if $err;

	$self->{'clientservice'}->getline_async(
		$self->{'clientservice'}->{'ctl_stream'}, $steps->{'got_response'});
    };

    step got_response => sub {
	my ($err, $line) = @_;
	return $params{'request_cb'}->($err, undef) if $err;

	if ($line eq "OK\r\n") {
	    return $params{'request_cb'}->(undef, undef); # carry on as you were
	} elsif ($line =~ /^TAPE (.*)\r\n$/) {
	    my $tape = $1;
	    if ($tape eq getconf($CNF_AMRECOVER_CHANGER)) {
		$tape = $Amanda::Recovery::Scan::DEFAULT_CHANGER;
	    }
	    return $params{'request_cb'}->(undef, $tape); # use this device
	} else {
	    return $params{'request_cb'}->("got invalid response from remote", undef);
	}
    };
};

##
# main driver

package main;

use Amanda::Debug qw( debug );
use Amanda::Util qw( :constants );
use Amanda::Config qw( :init );
use Amanda::Service::Amidxtaped;

our $exit_status = 0;

Amanda::Util::setup_application("amidxtaped", "server", $CONTEXT_DAEMON, "amanda", "amanda");
config_init($CONFIG_INIT_GLOBAL, undef);

my $amidxtaped = Amanda::Service::Amidxtaped->new();
Amanda::MainLoop::call_later(sub { $amidxtaped->run(); });
Amanda::MainLoop::run();

debug("exiting with $exit_status");
Amanda::Util::finish_application();

exit($exit_status);
