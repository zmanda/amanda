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

package Amanda::Recovery::Scan;

use strict;
use warnings;
use Carp;
use POSIX ();
use Data::Dumper;
use vars qw( @ISA );
use base qw(Exporter);
our @EXPORT_OK = qw($DEFAULT_CHANGER);

use Amanda::Paths;
use Amanda::Util;
use Amanda::Device qw( :constants );
use Amanda::Debug qw( debug );
use Amanda::Changer;
use Amanda::MainLoop;
use Amanda::Interactivity;

use constant SCAN_ASK  => 1;     # call Amanda::Interactivity module
use constant SCAN_POLL => 2;     # wait 'poll_delay' and retry the scan.
use constant SCAN_FAIL => 3;     # abort
use constant SCAN_CONTINUE => 4; # continue to the next step
use constant SCAN_ASK_POLL => 5; # call Amanda::Interactivity module and
				 # poll at the same time.

=head1 NAME

Amanda::Recovery::Scan -- interface to scan algorithm

=head1 SYNOPSIS

    use Amanda::Recovey::Scan;

    # scan the default changer with no interactivity
    my $scan = Amanda::Recovery::Scan->new();
    # ..or scan the changer $chg, using $interactivity for interactivity
    $scan = Amanda::Recovery::Scan->new(chg => $chg,
                                        interactivity => $interactivity);

    $scan->find_volume(
	label => "TAPE-012",
	res_cb => sub {
	    my ($err, $reservation) = @_;
	    if ($err) {
		die "$err";
	    }
	    $dev = $reservation->{device};
	    # use device..
	});

    # later..
    $reservation->release(finished_cb => $start_next_volume);

    # later..
    $scan->quit(); # also quit the changer

    
=head1 OVERVIEW

This package provides a way for programs that need to read data from volumes
(loosely called "recovery" programs) to find the volumes they need in a
configurable way.  It takes care of prompting for volumes when they are not
available, juggling multiple changers, and any other unpredictabilities.

=head1 INTERFACE

Like L<Amanda::Changer>, this package operates asynchronously, and thus
requires that the caller use L<Amanda::MainLoop> to poll for events.

A new Scan object is created with the C<new> function as follows:

  my $scan = Amanda::Recovery::Scan->new(scan_conf     => $scan_conf,
                                         chg           => $chg,
                                         interactivity => $interactivity);

C<scan_conf> is the configuration for the scan, which at this point should be
omitted, as configuration is not yet supported.  The C<chg> parameter specifies
the changer to start the scan with. The default changer is used if C<chg> is
omitted. The C<interactivity> parameter gives an C<Amanda::Interactivity> object.

=head2 CALLBACKS

Many of the callbacks used by this package are identical to the callbacks of
the same name in L<Amanda::Changer>.

When a callback is called with an error, it is an object of type
C<Amanda::Changer::Error>.  The C<volinuse> reason has a different meaning: it
means that the volume with that label is present in the changer, but is in use
by another program.

=head2 Scan object

=head3 find_volume

  $scan->find_volume(label       => $label,
                     res_cb      => $res_cb,
                     user_msg_fn => $user_msg_fn,
                     set_current => 0)

Find the volume labelled C<$label> and call C<$res_cb>.  C<$user_msg_fn> is
used to send progress information, The argumnet it takes are describe in
the next section.  As with the C<load> method
of the changer API, C<set_current> should be set to 1 if you want the scan to
set the current slot.

=head3 quit

  $scan->quit()

The cleanly terminate a scan objet, the changer quit is also called.

=head3 user_msg_fn

The user_msg_fn take various arguments

Initiate the scan of the slot $slot:
  $self->user_msg_fn(scan_slot => 1,
                     slot      => $slot);

Initiate the scan of the slot $slot which should have the label $label:
  $self->user_msg_fn(scan_slot => 1,
                     slot      => $slot,
                     label     => $label);   

The result of scanning slot $slot:
  $self->user_msg_fn(slot_result => 1,
                     slot        => $slot,
                     err         => $err,
                     res         => $res);

Other options can be added at any time.  The function can ignore them.

=cut

our $DEFAULT_CHANGER = {};

sub new {
    my $class = shift;
    my %params = @_;
    my $scan_conf = $params{'scan_conf'};
    my $chg = $params{'chg'};
    my $interactivity = $params{'interactivity'};

    #until we have a config for it.
    $scan_conf = Amanda::Recovery::Scan::Config->new();
    $chg = Amanda::Changer->new() if !defined $chg;
    return $chg if $chg->isa("Amanda::Changer::Error");

    my $self = {
	initial_chg   => $chg,
	chg           => $chg,
	scan_conf     => $scan_conf,
        interactivity => $interactivity,
    };
    return bless ($self, $class);
}

sub DESTROY {
    my $self = shift;

    die("Recovery::Scan detroyed without quit") if defined $self->{'scan_conf'};
}

sub quit {
    my $self = shift;

    $self->{'chg'}->quit() if defined $self->{'chg'};

    foreach (keys %$self) {
	delete $self->{$_};
    }

}

sub find_volume {
    my $self = shift;
    my %params = @_;

    my $label = $params{'label'};
    my $user_msg_fn = $params{'user_msg_fn'} || \&_user_msg_fn;
    my $res;
    my %seen = ();
    my $inventory;
    my $current;
    my $new_slot;
    my $poll_src;
    my $scan_running = 0;
    my $interactivity_running = 0;
    my $restart_scan = 0;
    my $abort_scan = undef;
    my $last_err = undef; # keep the last meaningful error, the one reported
			  # to the user, most scan end with the notfound error,
			  # it's more interesting to report an error from the
			  # device or ...
    my $slot_scanned;
    my $remove_undef_state = 0;
    my $load_for_label = 0; # 1 = Try to load the slot with the correct label
                            # 0 = Load a slot with an unknown label

    my $steps = define_steps
	cb_ref => \$params{'res_cb'};

    step get_first_inventory => sub {
	Amanda::Debug::debug("find_volume labeled '$label'");

	$scan_running = 1;
	$self->{'chg'}->inventory(inventory_cb => $steps->{'got_first_inventory'});
    };

    step got_first_inventory => sub {
	(my $err, $inventory) = @_;

	if ($err && $err->notimpl) {
	    #inventory not implemented
	    return $self->_find_volume_no_inventory(%params);
	} elsif ($err) {
	    #inventory fail
	    return $steps->{'call_res_cb'}->($err, undef);
	}

	# find current slot and keep a private copy of the value
	for my $i (0..(scalar(@$inventory)-1)) {
	    if ($inventory->[$i]->{current}) {
		$current = $inventory->[$i]->{slot};
		last;
	    }
	}

	if (!defined $current) {
	    if (scalar(@$inventory) == 0) {
		$current = 0;
	    } else {
		$current = $inventory->[0]->{slot};
	    }
	}

	# continue parsing the inventory
	$steps->{'parse_inventory'}->($err, $inventory);
    };

    step restart_scan => sub {
	$restart_scan = 0;
	return $steps->{'get_inventory'}->();
    };

    step get_inventory => sub {
	$self->{'chg'}->inventory(inventory_cb => $steps->{'parse_inventory'});
    };

    step parse_inventory => sub {
	(my $err, $inventory) = @_;

	if ($err && $err->notimpl) {
	    #inventory not implemented
	    return $self->_find_volume_no_inventory(%params);
	}
	return $steps->{'handle_error'}->($err, undef) if $err;

	# throw out the inventory result and move on if the situation has
	# changed while we were waiting
	return $steps->{'abort_scan'}->() if $abort_scan;
	return $steps->{'restart_scan'}->() if $restart_scan;

	# check if label is in the inventory
	for my $i (0..(scalar(@$inventory)-1)) {
	    my $sl = $inventory->[$i];
	    if (defined $sl->{'label'} and
		$sl->{'label'} eq $label and
		!defined $seen{$sl->{'slot'}}) {
		$slot_scanned = $sl->{'slot'};
		if ($sl->{'reserved'}) {
		    return $steps->{'handle_error'}->(
			    Amanda::Changer::Error->new('failed',
				reason => 'volinuse',
				message => "Volume '$label' in slot $slot_scanned is reserved"),
			    undef);
		}
		Amanda::Debug::debug("parse_inventory: load slot $slot_scanned with label '$label'");
		$user_msg_fn->(scan_slot => 1,
			       slot      => $slot_scanned,
			       label     => $label);
		$seen{$slot_scanned} = { device_status => $sl->{'device_status'},
					 f_type        => $sl->{'f_type'},
					 label         => $sl->{'label'} };
		$load_for_label = 1;
		return $self->{'chg'}->load(slot => $slot_scanned,
				  res_cb => $steps->{'slot_loaded'},
				  set_current => $params{'set_current'});
	    }
	}

	# Remove from seen all slot that have state == SLOT_UNKNOWN
	# It is done when as scan is restarted from interactivity object.
	if ($remove_undef_state) {
	    for my $i (0..(scalar(@$inventory)-1)) {
		my $slot = $inventory->[$i]->{slot};
		if (exists($seen{$slot}) &&
		    !defined($inventory->[$i]->{state})) {
		    delete $seen{$slot}
		}
	    }
	    $remove_undef_state = 0;
	}

	# remove any slots where the state has changed from the list of seen slots
	for my $i (0..(scalar(@$inventory)-1)) {
	    my $sl = $inventory->[$i];
	    my $slot = $sl->{slot};
	    if ($seen{$slot} &&
		defined($sl->{'state'}) &&
		(($seen{$slot}->{'device_status'} != $sl->{'device_status'}) ||
		 (defined $seen{$slot}->{'device_status'} &&
		  $seen{$slot}->{'device_status'} == $DEVICE_STATUS_SUCCESS &&
		  $seen{$slot}->{'f_type'} != $sl->{'f_type'}) ||
		 (defined $seen{$slot}->{'device_status'} &&
		  $seen{$slot}->{'device_status'} == $DEVICE_STATUS_SUCCESS &&
		  defined $seen{$slot}->{'f_type'} &&
		  $seen{$slot}->{'f_type'} == $Amanda::Header::F_TAPESTART &&
		  $seen{$slot}->{'label'} ne $sl->{'label'}))) {
		delete $seen{$slot};
	    }
	}

	# scan any unseen slot already in a drive, if configured to do so
	if ($self->{'scan_conf'}->{'scan_drive'}) {
	    for my $sl (@$inventory) {
		my $slot = $sl->{'slot'};
		if (defined $sl->{'loaded_in'} &&
		    !$sl->{'reserved'} &&
		    !$seen{$slot}) {
		    $slot_scanned = $slot;
		    $user_msg_fn->(scan_slot => 1, slot => $slot_scanned);
		    $seen{$slot_scanned} = { device_status => $sl->{'device_status'},
					     f_type        => $sl->{'f_type'},
					     label         => $sl->{'label'} };
		    $load_for_label = 0;
		    return $self->{'chg'}->load(slot => $slot_scanned,
				      res_cb => $steps->{'slot_loaded'},
				      set_current => $params{'set_current'});
		}
	    }
	}

	# scan slot
	if ($self->{'scan_conf'}->{'scan_unknown_slot'}) {
	    #find index for current slot
	    my $current_index = undef;
	    for my $i (0..(scalar(@$inventory)-1)) {
		my $slot = $inventory->[$i]->{slot};
		if ($slot eq $current) {
                    $current_index = $i;
		}
	    }

	    #scan next slot to scan
	    $current_index = 0 if !defined $current_index;
	    for my $i ($current_index..(scalar(@$inventory)-1), 0..($current_index-1)) {
		my $sl = $inventory->[$i];
		my $slot = $sl->{slot};
		# skip slots we've seen
		next if defined($seen{$slot});
		# skip slots that are empty
		next if defined $sl->{'state'} &&
			$sl->{'state'} == Amanda::Changer::SLOT_EMPTY;
		# skip slots for which we have a known label, since it's not the
		# one we want
		next if defined $sl->{'f_type'} &&
			$sl->{'f_type'} == $Amanda::Header::F_TAPESTART;
		next if defined $sl->{'label'};

		# found a slot to check - reset our current slot
		$current = $slot;
		$slot_scanned = $current;
		Amanda::Debug::debug("parse_inventory: load slot $current");
		$user_msg_fn->(scan_slot => 1, slot => $slot_scanned);
		$seen{$slot_scanned} = { device_status => $sl->{'device_status'},
					 f_type        => $sl->{'f_type'},
					 label         => $sl->{'label'} };
		$load_for_label = 0;
		return $self->{'chg'}->load(slot => $slot_scanned,
				res_cb => $steps->{'slot_loaded'},
				set_current => $params{'set_current'});
	    }
        }

	#All slots are seen or empty.
	if ($last_err) {
	    return $steps->{'handle_error'}->($last_err, undef);
	} else {
	    return $steps->{'handle_error'}->(
		    Amanda::Changer::Error->new('failed',
			    reason => 'notfound',
			    message => "Volume '$label' not found"),
		    undef);
	}
    };

    step slot_loaded => sub {
	(my $err, $res) = @_;

	# we don't responsd to abort_scan or restart_scan here, since we
	# have an open reservation that we should deal with.

	$user_msg_fn->(slot_result => 1,
		       slot => $slot_scanned,
		       err  => $err,
		       res  => $res);
	if ($res) {
	    if ($res->{device}->status == $DEVICE_STATUS_SUCCESS &&
		$res->{device}->volume_label &&
		$res->{device}->volume_label eq $label) {
		my $volume_label = $res->{device}->volume_label;
		return $steps->{'call_res_cb'}->(undef, $res);
	    }
	    my $f_type;
	    if (defined $res->{device}->volume_header) {
		$f_type = $res->{device}->volume_header->{type};
	    } else {
		$f_type = undef;
	    }

	    # The slot did not contain the volume we wanted, so mark it
	    # as seen and try again.
	    $seen{$slot_scanned} = {
			device_status => $res->{device}->status,
			f_type => $f_type,
			label  => $res->{device}->volume_label
	    };

	    # notify the user
	    if ($res->{device}->status == $DEVICE_STATUS_SUCCESS) {
		$last_err = undef;
	    } else {
		$last_err = Amanda::Changer::Error->new('fatal',
				message => $res->{device}->error_or_status());
	    }
	    return $res->release(finished_cb => $steps->{'load_released'});
	} else {
	    if ($load_for_label == 0 && $err->volinuse) {
		# Scan semantics for volinuse is different than changer.
		# If a slot with unknown label is loaded then we map
		# volinuse to driveinuse.
		$err->{reason} = "driveinuse";
	    }
	    $last_err = $err if $err->fatal || !$err->notfound;
	    if ($load_for_label == 1 && $err->failed && $err->volinuse) {
		# volinuse is an error
		return $steps->{'handle_error'}->($err, $steps->{'load_released'});
	    }
	    return $steps->{'load_released'}->();
	}
    };

    step load_released => sub {
	my ($err) = @_;

	# TODO: handle error

	$res = undef;

	# throw out the inventory result and move on if the situation has
	# changed while we were loading a volume
	return $steps->{'abort_scan'}->() if $abort_scan;
	return $steps->{'restart_scan'}->() if $restart_scan;

	$new_slot = $current;
	$steps->{'get_inventory'}->();
    };

    step handle_error => sub {
	my ($err, $continue_cb) = @_;

	my $scan_method = undef;
	$scan_running = 0;
	my $message;


	$poll_src->remove() if defined $poll_src;
	$poll_src = undef;

	# prefer to use scan method for $last_err, if present
	if ($last_err && $err->failed && $err->notfound) {
	    $message = "$last_err";
	
	    if ($last_err->isa("Amanda::Changer::Error")) {
		if ($last_err->fatal) {
		    $scan_method = $self->{'scan_conf'}->{'fatal'};
		} else {
		    $scan_method = $self->{'scan_conf'}->{$last_err->{'reason'}};
		}
	    } elsif ($continue_cb) {
		$scan_method = SCAN_CONTINUE;
	    }
	}

	#use scan method for $err
	if (!defined $scan_method) {
	    if ($err) {
		$message = "$err" if !defined $message;
		if ($err->fatal) {
		    $scan_method = $self->{'scan_conf'}->{'fatal'};
		} else {
		    $scan_method = $self->{'scan_conf'}->{$err->{'reason'}};
		}
	    } else {
		confess("error not defined");
		$scan_method = SCAN_ASK_POLL;
	    }
	}

	## implement the desired scan method

	if ($scan_method == SCAN_CONTINUE && !defined $continue_cb) {
	    $scan_method = $self->{'scan_conf'}->{'notfound'};
	    if ($scan_method == SCAN_CONTINUE) {
		$scan_method = SCAN_FAIL;
	    }
	}

	if ($scan_method == SCAN_ASK && !defined $self->{'interactivity'}) {
	    $scan_method = SCAN_FAIL;
	}

	if ($scan_method == SCAN_ASK_POLL && !defined $self->{'interactivity'}) {
	    $scan_method = SCAN_FAIL;
	}

	if ($scan_method == SCAN_ASK) {
	    return $steps->{'scan_interactivity'}->("$message");
	} elsif ($scan_method == SCAN_POLL) {
	    $poll_src = Amanda::MainLoop::call_after(
				$self->{'scan_conf'}->{'poll_delay'},
				$steps->{'after_poll'});
	    return;
	} elsif ($scan_method == SCAN_ASK_POLL) {
	    $steps->{'scan_interactivity'}->("$message\n");
	    $poll_src = Amanda::MainLoop::call_after(
				$self->{'scan_conf'}->{'poll_delay'},
				$steps->{'after_poll'});
	    return;
	} elsif ($scan_method == SCAN_FAIL) {
	    return $steps->{'call_res_cb'}->($err, undef);
	} elsif ($scan_method == SCAN_CONTINUE) {
	    return $continue_cb->($err, undef);
	} else {
	    confess("Invalid SCAN_* value:$err:$err->{'reason'}:$scan_method");
	}
    };

    step after_poll => sub {
	$poll_src->remove() if defined $poll_src;
	$poll_src = undef;
	return $steps->{'restart_scan'}->();
    };

    step scan_interactivity => sub {
	my ($err_message) = @_;
	if (!$interactivity_running) {
	    $interactivity_running = 1;
	    my $message = "$err_message\nInsert volume labeled '$label' in changer and type <enter>\nor type \"^D\" to abort\n";
	    $self->{'interactivity'}->user_request(
				message     => $message,
				label       => $label,
				err         => "$err_message",
				chg_name    => $self->{'chg'}->{'chg_name'},
				request_cb  => $steps->{'scan_interactivity_cb'});
	}
	return;
    };

    step scan_interactivity_cb => sub {
	my ($err, $message) = @_;
	$interactivity_running = 0;
	$poll_src->remove() if defined $poll_src;
	$poll_src = undef;

	if ($err) {
	    if ($scan_running) {
		$abort_scan = $err;
		return;
	    } else {
		return $steps->{'call_res_cb'}->($err, undef);
	    }
	}

	if ($message ne '') {
	    # use a new changer
	    my $new_chg;
	    if (ref($message) eq 'HASH' and $message == $DEFAULT_CHANGER) {
		$new_chg = Amanda::Changer->new();
	    } else {
		$new_chg = Amanda::Changer->new($message);
	    }
	    if ($new_chg->isa("Amanda::Changer::Error")) {
		return $steps->{'scan_interactivity'}->("$new_chg");
	    }
	    $last_err = undef;
	    $self->{'chg'}->quit();
	    $self->{'chg'} = $new_chg;
	    %seen = ();
	} else {
	    $remove_undef_state = 1;
	}

	if ($scan_running) {
	    $restart_scan = 1;
	    return;
	} else {
	    return $steps->{'restart_scan'}->();
	}
    };

    step abort_scan => sub {
	$steps->{'call_res_cb'}->($abort_scan, undef);
    };

    step call_res_cb => sub {
	(my $err, $res) = @_;

	# TODO: what happens if the search was aborted or
	# restarted in the interim?

	$abort_scan = undef;
	$poll_src->remove() if defined $poll_src;
	$poll_src = undef;
	$interactivity_running = 0;
	$self->{'interactivity'}->abort() if defined $self->{'interactivity'};
	$params{'res_cb'}->($err, $res);
    };
}

#
sub _find_volume_no_inventory {
    my $self = shift;
    my %params = @_;

    my $label = $params{'label'};
    my $res;
    my %seen_slots = ();
    my $inventory;
    my $current;
    my $new_slot;
    my $last_slot;

    my $steps = define_steps
	cb_ref => \$params{'res_cb'};

    step load_label => sub {
	return $self->{'chg'}->load(relative_slot => "current",
				    res_cb => $steps->{'load_label_cb'});
    };

    step load_label_cb => sub {
	(my $err, $res) = @_;

	if ($err) {
	    if ($err->failed && $err->notfound) {
		if ($err->{'message'} eq "all slots have been loaded") {
		    $err->{'message'} = "label '$label' not found";
		}
		return $params{'res_cb'}->($err, undef);
	    } elsif ($err->failed && $err->volinuse and defined $err->{'slot'}) {
		$last_slot = $err->{'slot'};
	    } else {
		#no interactivity yet.
		return $params{'res_cb'}->($err, undef);
	    }
	} else {
	    $last_slot = $res->{'this_slot'}
	}

	$seen_slots{$last_slot} = 1 if defined $last_slot;
	if ($res) {
	    my $dev = $res->{'device'};
	    if (defined $dev->volume_label && $dev->volume_label eq $label) {
		return $params{'res_cb'}->(undef, $res);
	    }
	    return $res->release(finished_cb => $steps->{'released'});
	} else {
	    return $steps->{'released'}->()
	}
    };

    step released => sub {
	$self->{'chg'}->load(relative_slot => "next",
		   except_slots => \%seen_slots,
		   res_cb => $steps->{'load_label_cb'},
		   set_current => 1);
    };
}

sub _user_msg_fn {
    my %params = @_;
}

package Amanda::Recovery::Scan::Config;

sub new {
    my $class = shift;
    my ($cc) = @_;

    my $self = bless {}, $class;

    $self->{'scan_drive'} = 0;
    $self->{'scan_unknown_slot'} = 1;
    $self->{'poll_delay'} = 10000; #10 seconds

    $self->{'fatal'} = Amanda::Recovery::Scan::SCAN_CONTINUE;
    $self->{'driveinuse'} = Amanda::Recovery::Scan::SCAN_ASK_POLL;
    $self->{'volinuse'} = Amanda::Recovery::Scan::SCAN_ASK_POLL;
    $self->{'notfound'} = Amanda::Recovery::Scan::SCAN_ASK_POLL;
    $self->{'unknown'} = Amanda::Recovery::Scan::SCAN_FAIL;
    $self->{'notimpl'} = Amanda::Recovery::Scan::SCAN_FAIL;
    $self->{'invalid'} = Amanda::Recovery::Scan::SCAN_CONTINUE;

    return $self;
}

1;
