# Copyright (c) 2009-2012 Zmanda Inc.  All Rights Reserved.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

=head1 NAME

Amanda::Taper::Controller

=head1 DESCRIPTION

This package is a component of the Amanda taper, and is not intended for use by
other scripts or applications.

The controller interfaces with the driver (via L<Amanda::Taper::Protocol>) and
controls one or more workers (L<Amanda::Taper::Worker>).

The controller create an L<Amanda::Taper::Worker> object for each
START_TAPER command it receive. It dispatch the following commands
to the correct worker.

=cut

use strict;
use warnings;

package Amanda::Taper::Controller;

use POSIX qw( :errno_h );
use Amanda::Policy;
use Amanda::Debug qw( debug );
use Amanda::Storage;
use Amanda::Changer;
use Amanda::Config qw( :getconf config_dir_relative );
use Amanda::Header;
use Amanda::Holding;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::MainLoop;
use Amanda::Taper::Protocol;
use Amanda::Taper::Scan;
use Amanda::Taper::Worker;
use Amanda::Interactivity;
use Amanda::Logfile qw( :logtype_t log_add );
use Amanda::Xfer qw( :constants );
use Amanda::Util qw( quote_string );
use File::Temp;

sub new {
    my $class = shift;
    my %params = @_;

    my $self = bless {

	# filled in at start
	proto => undef,
	catalog => $params{'catalog'},
	storage_name => $params{'storage_name'},
	worker => {},
    }, $class;
    return $self;
}

# The feedback object mediates between messages from the driver and the ongoing
# action with the taper.  This is made a little bit complicated because the
# driver conversation is fairly contextual, with some responses answering
# "questions" asked earlier.  This is modeled with the following taper
# "states":
#
# init:
#   waiting for START-TAPER command
# starting:
#   warming up devices; TAPER-OK not sent yet
# idle:
#   not currently dumping anything
# making_xfer:
#   setting up a transfer for a new dump
# getting_header:
#   getting the header before beginning a new dump
# writing:
#   in the middle of writing a file (self->{'handle'} set)
# error:
#   a fatal error has occurred, so this object won't do anything

sub start {
    my $self = shift;

    my $message_cb = make_cb(message_cb => sub {
	my ($msgtype, %params) = @_;
	my $msg;
	if (defined $msgtype) {
	    $msg = "unhandled command '$msgtype'";
	} else {
	    $msg = $params{'error'};
	}
	log_add($L_ERROR, "$msg");
	print STDERR "$msg\n";
	$self->{'proto'}->send(Amanda::Taper::Protocol::BAD_COMMAND,
	    message => $msg);
    });
    $self->{'proto'} = Amanda::Taper::Protocol->new(
	rx_fh => *STDIN,
	tx_fh => *STDOUT,
	message_cb => $message_cb,
	message_obj => $self,
	debug => $Amanda::Config::debug_taper?'taper/driver':'',
	no_read => 1,
    );

    my ($storage) = Amanda::Storage->new(
					storage_name => $self->{'storage_name'},
					catalog => $self->{'catalog'});
    if ($storage->isa("Amanda::Changer::Error")) {
	$self->{'proto'}->send(Amanda::Taper::Protocol::TAPE_ERROR,
		worker_name => "SETUP",
		message => "$storage");

	# log the error (note that the message is intentionally not quoted)
	log_add($L_ERROR, "no-tape error [$storage]");

	# wait for it to be transmitted, then exit
	$self->{'proto'}->stop(finished_cb => sub {
	    Amanda::MainLoop::quit();
	});

	# don't finish start()ing
	return;
    }
    if ($storage->{'erase_volume'}) {
	$storage->erase_no_retention();
    }
    my $changer = $storage->{'chg'};
    if ($changer->isa("Amanda::Changer::Error")) {
	# send a TAPE_ERROR right away
	$self->{'proto'}->send(Amanda::Taper::Protocol::TAPE_ERROR,
		worker_name => "SETUP",
		message => "$changer");

	# log the error (note that the message is intentionally not quoted)
	log_add($L_ERROR, "no-tape error [$changer]");

	# wait for it to be transmitted, then exit
	$self->{'proto'}->stop(finished_cb => sub {
	    Amanda::MainLoop::quit();
	});

	$storage->quit();

	# don't finish start()ing
	return;
    }

    my $interactivity = Amanda::Interactivity->new(
					name => $storage->{'interactivity'},
					storage_name => $storage->{'storage'},
					changer_name => $changer->{'chg_name'});
    my $scan_name = $storage->{'taperscan_name'};
    $self->{'storage'} = $storage;
    $self->{'taperscan'} = Amanda::Taper::Scan->new(algorithm => $scan_name,
					    storage => $storage,
					    changer => $changer,
					    interactivity => $interactivity,
					    catalog => $self->{'catalog'});
    $self->{'proto'}->start_read();
}

sub quit {
    my $self = shift;
    my %params = @_;
    my @errors = ();
    my @worker = ();
    my $worker;

    my $steps = define_steps
	cb_ref => \$params{'finished_cb'};

    step init => sub {
	@worker = values %{$self->{'worker'}};
	delete $self->{'worker'};
	$steps->{'quit_scribe'}->();
    };

    step quit_scribe => sub {
	$worker = shift @worker;
	if (defined $worker) {
	    if (defined $worker->{'scribe'}) {
		return $worker->{'scribe'}->quit(finished_cb => sub {
	            my ($err) = @_;
	            push @errors, $err if ($err);

	            $steps->{'quit_clerk'}->();
		});
	    }
	    $steps->{'quit_clerk'}->();
	}
	$steps->{'stop_proto'}->();
    };

    step quit_clerk => sub {
	if (defined $worker->{'src'}->{'clerk'}) {
	    return $worker->{'src'}->{'clerk'}->quit(finished_cb => sub {
	            my ($err) = @_;
	            push @errors, $err if ($err);
		    if (defined $worker->{'src'}->{'storage'}) {
			$worker->{'src'}->{'storage'}->quit();
		    }

	            $steps->{'quit_scribe'}->();
	    });
	}
	$steps->{'quit_scribe'}->();
    };

    step stop_proto => sub {
	$self->{'proto'}->stop(finished_cb => sub {
	    my ($err) = @_;
	    push @errors, $err if ($err);

	    $steps->{'done'}->();
	});
    };

    step done => sub {
	$self->{'taperscan'}->quit() if defined $self->{'taperscan'};
	$self->{'storage'}->quit() if defined $self->{'storage'};
	if (@errors) {
	    $params{'finished_cb'}->(join("; ", @errors));
	} else {
	    $params{'finished_cb'}->();
	}
    };
}

##
# Driver commands

sub msg_START_TAPER {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = new Amanda::Taper::Worker($params{'taper_name'}, $params{'worker_name'}, $self,
				  $params{'timestamp'});

    $self->{'worker'}->{$params{'worker_name'}} = $worker;

    $self->{'timestamp'} = $params{'timestamp'};
}

# defer both PORT_ and FILE_WRITE to a common method
sub msg_FILE_WRITE {
    my $self = shift;
    my ($msgtype, %params) = @_;

debug("msg_FILE_WRITE: " . Data::Dumper::Dumper(\%params));
    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->FILE_WRITE(@_);
}

sub msg_PORT_WRITE {
    my $self = shift;
    my ($msgtype, %params) = @_;

debug("msg_PORT_WRITE " . Data::Dumper::Dumper(\%params));
    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->PORT_WRITE(@_);
}

sub msg_SHM_WRITE {
    my $self = shift;
    my ($msgtype, %params) = @_;

debug("msg_SHM_WRITE " . Data::Dumper::Dumper(\%params));
    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->SHM_WRITE(@_);
}

sub msg_VAULT_WRITE {
    my $self = shift;
    my ($msgtype, %params) = @_;

debug("msg_VAULT_WRITE " . Data::Dumper::Dumper(\%params));
    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->VAULT_WRITE(@_);
}

sub msg_START_SCAN {
    my $self = shift;
    my ($msgtype, %params) = @_;

debug("msg_START_SCAN " . Data::Dumper::Dumper(\%params));
    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->START_SCAN(@_);
}

sub msg_NEW_TAPE {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->NEW_TAPE(@_);
}

sub msg_NO_NEW_TAPE {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->NO_NEW_TAPE(@_);
}

sub msg_DONE {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->DONE(@_);
}

sub msg_FAILED {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->FAILED(@_);
}

sub msg_CLOSE_VOLUME {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->CLOSE_VOLUME(@_);
}

sub msg_CLOSE_SOURCE_VOLUME {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    $worker->CLOSE_SOURCE_VOLUME(@_);
}

sub msg_TAKE_SCRIBE_FROM {
    my $self = shift;
    my ($msgtype, %params) = @_;

    my $worker = $self->{'worker'}->{$params{'worker_name'}};
    my $worker1 = $self->{'worker'}->{$params{'from_worker_name'}};
    $worker->TAKE_SCRIBE_FROM($worker1, @_);
    delete $self->{'worker'}->{$params{'from_worker_name'}};
}

sub msg_QUIT {
    my $self = shift;
    my ($msgtype, %params) = @_;
    my $read_cb;

    # because the driver hangs up on us immediately after sending QUIT,
    # and EOF also means QUIT, we tend to get this command repeatedly.
    # So check to make sure this is only called once
    return if $self->{'quitting'};
    $self->{'quitting'} = 1;

    my $finished_cb = make_cb(finished_cb => sub {
	my $err = shift;
	if ($err) {
	    Amanda::Debug::debug("Quit error: $err");
	}
	Amanda::MainLoop::quit();
    });
    $self->quit(finished_cb => $finished_cb);
};

1;
