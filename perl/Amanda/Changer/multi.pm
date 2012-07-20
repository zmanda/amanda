# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Changer::multi;

use strict;
use warnings;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer );

use File::Glob qw( :glob );
use File::Path;
use Amanda::Config qw( :getconf );
use Amanda::Debug;
use Amanda::Changer;
use Amanda::MainLoop;
use Amanda::Device qw( :constants );

=head1 NAME

Amanda::Changer::multi

=head1 DESCRIPTION

This changer operates with a list of device, specified in the tpchanger
string.

See the amanda-changers(7) manpage for usage information.

=cut

# STATE
#
# The device state is shared between all changers accessing the same changer.
# It is a hash with keys:
#   current_slot - the unaliased device name of the current slot
#   slots - see below
#
# The 'slots' key is a hash, with unaliased device name as keys and hashes
# as values.  Each slot's hash has keys:
#   pid           - the pid that reserved that slot.
#   state         - SLOT_FULL/SLOT_EMPTY/SLOT_UNKNOWN
#   device_status - the status of the device after the open or read_label
#   device_error  - error message from the device
#   f_type        - the F_TYPE of the fileheader.
#   label         - the label, if known, of the volume in this slot

# $self is a hash with keys:
#   slot           : slot number of the current slot
#   slots	   : An array with all slot names
#   unaliased	   : A hash with slot number as keys and unaliased device name
#                    as value
#   slot_name      : A hash with slot number as keys and device name as value
#   number         : A hash with unaliased device name as keys and slot number
#                    as value
#   config         : The Amanda::Changer::Config for this changer
#   state_filename : The filename of the state file
#   first_slot     : The number of the first slot
#   last_slot      : The number of the last slot + 1

sub new {
    my $class = shift;
    my ($config, $tpchanger) = @_;
    my $devices = $tpchanger;
    $devices =~ s/^chg-multi://g;
    my (@slots) = Amanda::Util::expand_braced_alternates($devices);

    unless (scalar @slots != 0) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "no devices specified");
    }

    my $properties = $config->{'properties'};
    my $first_slot = 1;
    if (exists $properties->{'first-slot'}) {
	$first_slot = @{$properties->{'first-slot'}->{'values'}}[0];
    }

    my %number = ();
    my %unaliased = ();
    my %slot_name = ();
    my $last_slot = $first_slot;
    foreach my $slot_name (@slots) {
	my $unaliased_name = Amanda::Device::unaliased_name($slot_name);
	$number{$unaliased_name} = $last_slot;
	$unaliased{$last_slot} = $unaliased_name;
	$slot_name{$last_slot} = $slot_name;
	$last_slot++;
    }

    if (!defined $config->{changerfile} ||
	$config->{changerfile} eq "") {
	return Amanda::Changer->make_error("fatal", undef,
	    reason => "invalid",
	    message => "no changerfile specified for changer '$config->{name}'");
    }

    my $state_filename = Amanda::Config::config_dir_relative($config->{'changerfile'});
    Amanda::Debug::debug("Using state file: $state_filename");

    my $self = {
	slots => \@slots,
	unaliased => \%unaliased,
	slot_name => \%slot_name,
	number => \%number,
	config => $config,
	state_filename => $state_filename,
	first_slot => $first_slot,
	last_slot => $last_slot,
    };

    bless ($self, $class);
    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;
    my $old_res_cb = $params{'res_cb'};
    my $state;

    $self->validate_params('load', \%params);

    return if $self->check_error($params{'res_cb'});

    $self->with_locked_state($self->{'state_filename'},
				     $params{'res_cb'}, sub {
	my ($state, $res_cb) = @_;

	$params{'state'} = $state;
	# overwrite the callback for _load_by_xxx
	$params{'res_cb'} = $res_cb;

	if (exists $params{'slot'} or exists $params{'relative_slot'}) {
	    $self->_load_by_slot(%params);
	} elsif (exists $params{'label'}) {
	    $self->_load_by_label(%params);
	}
    });
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;

    return if $self->check_error($params{'info_cb'});

    # no need for synchronization -- all of these values are static

    if ($key eq 'num_slots') {
	$results{$key} = $self->{last_slot} - $self->{first_slot};
    } elsif ($key eq 'vendor_string') {
	$results{$key} = 'chg-multi'; # mostly just for testing
    } elsif ($key eq 'fast_search') {
	$results{$key} = 0;
    }

    $params{'info_cb'}->(undef, %results) if $params{'info_cb'};
}

sub reset {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'finished_cb'});

    $self->with_locked_state($self->{'state_filename'},
				     $params{'finished_cb'}, sub {
	my ($state, $finished_cb) = @_;
	my $slot;

	$params{state} = $state;
	$slot = $self->{first_slot};
	$self->{slot} = $slot;
	$self->_set_current($state, $slot);

	$finished_cb->();
    });
}

sub eject {
    my $self = shift;
    my %params = @_;
    my $slot;

    return if $self->check_error($params{'finished_cb'});

    $self->with_locked_state($self->{'state_filename'},
				     $params{'finished_cb'}, sub {
	my ($state, $finished_cb) = @_;
	my $drive;

	$params{state} = $state;
	if (!exists $params{'drive'}) {
	    $drive = $self->_get_current($params{state});
	} else {
	    $drive = $params{'drive'};
	}
	if (!defined $self->{unaliased}->{$drive}) {
	    return $self->make_error("failed", $finished_cb,
		reason => "invalid",
		message => "Invalid slot '$drive'");
	}

	Amanda::Debug::debug("ejecting drive $drive");
	my $device = Amanda::Device->new($self->{slot_name}->{$drive});
	if ($device->status() != $DEVICE_STATUS_SUCCESS) {
	    return $self->make_error("failed", $finished_cb,
		reason => "device",
		message => $device->error_or_status);
	}
	if (my $err = $self->{'config'}->configure_device($device)) {
	    return $self->make_error("failed", $params{'res_cb'},
			reason => "device",
			message => $err);
	}
	$device->eject();
	if ($device->status() != $DEVICE_STATUS_SUCCESS) {
	    return $self->make_error("failed", $finished_cb,
		reason => "invalid",
		message => $device->error_or_status);
	}
	undef $device;

	$finished_cb->();
    });
}

sub update {
    my $self = shift;
    my %params = @_;
    my @slots_to_check;
    my $state;
    my $set_to_unknown = 0;

    my $user_msg_fn = $params{'user_msg_fn'};
    $user_msg_fn ||= sub { Amanda::Debug::info("chg-multi: " . $_[0]); };

    my $steps = define_steps
	cb_ref => \$params{'finished_cb'};

    step lock => sub {
	$self->with_locked_state($self->{'state_filename'},
				 $params{'finished_cb'}, sub {
	    my ($state, $finished_cb) = @_;

	    $params{state} = $state;
	    $params{'finished_cb'} = $finished_cb;

	    $steps->{'handle_assignment'}->();
	});
    };

    step handle_assignment => sub {
	$state = $params{state};
	# check for the SL=LABEL format, and handle it here
	if (exists $params{'changed'} and
	    $params{'changed'} =~ /^\d+=\S+$/) {
	    my ($slot, $label) = ($params{'changed'} =~ /^(\d+)=(\S+)$/);

	    # let's list the reasons we *can't* do what the user has asked
	    my $whynot;
	    if (!exists $self->{unaliased}->{$slot}) {
		$whynot = "slot $slot does not exist";
	    }

	    if ($whynot) {
		return $self->make_error("failed", $params{'finished_cb'},
			reason => "unknown", message => $whynot);
	    }

	    $user_msg_fn->("recording volume '$label' in slot $slot");
	    # ok, now erase all knowledge of that label
	    while (my ($sl, $inf) = each %{$state->{'slots'}}) {
		if ($inf->{'label'} and $inf->{'label'} eq $label) {
		    $inf->{'label'} = undef;
		}
	    }

	    # and add knowledge of the label to the given slot
	    my $unaliased = $self->{unaliased}->{$slot};
	    $state->{'slots'}->{$unaliased}->{'label'} = $label;

	    # that's it -- no changer motion required
	    return $params{'finished_cb'}->(undef);
	} elsif (exists $params{'changed'} and
	       $params{'changed'} =~ /^(.+)=$/) {
	    $params{'changed'} = $1;
	    $set_to_unknown = 1;
	    $steps->{'calculate_slots'}->();
	} else {
	    $steps->{'calculate_slots'}->();
	}
    };

    step calculate_slots => sub {
	if (exists $params{'changed'}) {
	    # parse the string just like use-slots, using a hash for uniqueness
	    my %changed;
	    for my $range (split ',', $params{'changed'}) {
		my ($first, $last) = ($range =~ /(\d+)(?:-(\d+))?/);
		$last = $first unless defined($last);
		for ($first .. $last) {
		    $changed{$_} = undef;
		}
	    }

	    @slots_to_check = keys %changed;
	    @slots_to_check = grep { exists $self->{'unaliased'}->{$_} } @slots_to_check;
	} else {
	    @slots_to_check = keys %{ $self->{unaliased} };
	}

	# sort them so we don't confuse the user with a "random" order
	@slots_to_check = sort @slots_to_check;

	$steps->{'update_slot'}->();
    };

    # TODO: parallelize, we have one drive by slot

    step update_slot => sub {
	return $steps->{'done'}->() if (!@slots_to_check);
	my $slot = shift @slots_to_check;
	if ($self->_is_slot_in_use($state, $slot)) {
	    $user_msg_fn->("Slot $slot is already in use");
	    return $steps->{'update_slot'}->();
	}

	if ($set_to_unknown == 1) {
	    $user_msg_fn->("removing entry for slot $slot");
	    my $unaliased = $self->{unaliased}->{$slot};
	    delete $state->{slots}->{$unaliased};
	    return $steps->{'update_slot'}->();
	} else {
	    $user_msg_fn->("scanning slot $slot");
	    $params{'slot'} = $slot;
	    $params{'res_cb'} = $steps->{'slot_loaded'};
	    $self->_load_by_slot(%params);
	}
    };

    step slot_loaded => sub {
	my ($err, $res) = @_;
	if ($err) {
	    return $params{'finished_cb'}->($err);
	}

	my $slot = $res->{'this_slot'};
	my $dev = $res->{device};
	$self->_update_slot_state(state => $state, dev => $dev, slot =>$slot);
	if ($dev->status() == $DEVICE_STATUS_SUCCESS) {
	    my $label = $dev->volume_label;
	    $user_msg_fn->("recording volume '$label' in slot $slot");
	} else {
	    my $status = $dev->error_or_status;
	    $user_msg_fn->("recording device error '" . $status . "' in slot $slot");
	}
	$res->release(
	    finished_cb => $steps->{'released'},
	    unlocked => 1,
	    state => $state);
    };

    step released => sub {
	my ($err) = @_;
	if ($err) {
	    return $params{'finished_cb'}->($err);
	}

	$steps->{'update_slot'}->();
    };

    step done => sub {
	$params{'finished_cb'}->(undef);
    };
}

sub inventory {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'inventory_cb'});

    $self->with_locked_state($self->{'state_filename'},
			     $params{'inventory_cb'}, sub {
	my ($state, $inventory_cb) = @_;

	my @inventory;
	my $current = $self->_get_current($state);
	foreach ($self->{first_slot} .. ($self->{last_slot} - 1)) {
	    my $slot = "$_";
	    my $unaliased = $self->{unaliased}->{$slot};
	    my $s = { slot => $slot,
		      state => $state->{slots}->{$unaliased}->{state} || Amanda::Changer::SLOT_UNKNOWN,
		      reserved => $self->_is_slot_in_use($state, $slot) };
	    if (defined $state->{slots}->{$unaliased} and
		exists $state->{slots}->{$unaliased}->{device_status}) {
		$s->{'device_status'} =
			      $state->{slots}->{$unaliased}->{device_status};
		if ($s->{'device_status'} != $DEVICE_STATUS_SUCCESS) {
		    $s->{'device_error'} =
			      $state->{slots}->{$unaliased}->{device_error};
		} else {
		    $s->{'device_error'} = undef;
		}
		$s->{'f_type'} = $state->{slots}->{$unaliased}->{f_type};
		$s->{'label'} = $state->{slots}->{$unaliased}->{label};
	    } else {
		$s->{'device_status'} = undef;
		$s->{'device_error'} = undef;
		$s->{'f_type'} = undef;
		$s->{'label'} = undef;
	    }
	    if ($slot eq $current) {
		$s->{'current'} = 1;
	    }
	    push @inventory, $s;
	}
	$inventory_cb->(undef, \@inventory);
    })
}

sub _load_by_slot {
    my $self = shift;
    my %params = @_;
    my $slot;

    if (exists $params{'relative_slot'}) {
	if ($params{'relative_slot'} eq "current") {
	    $slot = $self->_get_current($params{state});
	} elsif ($params{'relative_slot'} eq "next") {
	    if (exists $params{'slot'}) {
		$slot = $params{'slot'};
	    } else {
		$slot = $self->_get_current($params{state});
	    }
	    $slot = $self->_get_next($slot);
	    $self->{slot} = $slot if ($params{'set_current'});
	    $self->_set_current($params{state}, $slot) if ($params{'set_current'});
	} else {
	    return $self->make_error("failed", $params{'res_cb'},
		reason => "invalid",
		message => "Invalid relative slot '$params{relative_slot}'");
	}
    } else {
	$slot = $params{'slot'};
    }

    if (exists $params{'except_slots'} and exists $params{'except_slots'}->{$slot}) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "notfound",
	    message => "all slots have been loaded");
    }

    if (!$self->_slot_exists($slot)) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "notfound",
	    message => "Slot $slot not defined");
    }

    if ($self->_is_slot_in_use($params{state}, $slot)) {
	my $unaliased = $self->{unaliased}->{$slot};
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "volinuse",
	    slot => $slot,
	    message => "Slot $slot is already in use by process '$params{state}->{slots}->{$unaliased}->{pid}'");
    }

    $self->{slot} = $slot if ($params{'set_current'});
    $self->_set_current($params{state}, $slot) if ($params{'set_current'});

    $self->_make_res($params{state}, $params{'res_cb'}, $slot);
}

sub _load_by_label {
    my $self = shift;
    my %params = @_;
    my $label = $params{'label'};
    my $slot;
    my $slot_name;
    my $state = $params{state};

    foreach $slot (keys %{$state->{slots}}) {
	if (defined $state->{slots}->{$slot} &&
	    $state->{slots}->{$slot}->{label} &&
	    $state->{slots}->{$slot}->{label} eq $label) {
	    $slot_name = $slot;
	    last;
	}
    }

    if (defined $slot_name &&
	$state->{slots}->{$slot_name}->{label} eq $label) {

	$slot = $self->{number}->{$slot_name};
	delete $params{'label'};
	$params{'slot'} = $slot;
	$self->_load_by_slot(%params);
    } else {
	return $self->make_error("failed", $params{'res_cb'},
				reason => "notfound",
				message => "Label '$label' not found");
    }
}


sub _make_res {
    my $self = shift;
    my ($state, $res_cb, $slot) = @_;
    my $res;

    my $unaliased = $self->{unaliased}->{$slot};
    my $slot_name = $self->{slot_name}->{$slot};
    my $device = Amanda::Device->new($slot_name);
    if ($device->status != $DEVICE_STATUS_SUCCESS) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => "opening '$slot': " . $device->error_or_status());
    }

    if (my $err = $self->{'config'}->configure_device($device)) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => $err);
    }

    $res = Amanda::Changer::multi::Reservation->new($self, $device, $slot);
    $state->{slots}->{$unaliased}->{pid} = $$;
    $device->read_label();

    $self->_update_slot_state(state => $state, dev => $res->{device}, slot => $slot);
    $res_cb->(undef, $res);
}


# Internal function to determine whether a slot exists.
sub _slot_exists {
    my ($self, $slot) = @_;

    return 1 if defined $self->{unaliased}->{$slot};
    return 0;
}

sub _update_slot_state {
    my $self = shift;
    my %params = @_;
    my $state = $params{state};
    my $dev = $params{dev};
    my $slot = $params{slot};
    my $unaliased = $self->{unaliased}->{$slot};
    $state->{slots}->{$unaliased}->{device_status} = "".scalar($dev->status);
    if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	$state->{slots}->{$unaliased}->{device_error} = $dev->error;
    } else {
	$state->{slots}->{$unaliased}->{device_error} = undef;
    }
    my $label = $dev->volume_label;
    $state->{slots}->{$unaliased}->{state} = Amanda::Changer::SLOT_FULL;
    $state->{slots}->{$unaliased}->{label} = $label;
    my $volume_header = $dev->volume_header;
    if (defined $volume_header) {
	$state->{slots}->{$unaliased}->{f_type} = "".scalar($volume_header->{type});
    } else {
	delete $state->{slots}->{$unaliased}->{f_type};
    }
}
# Internal function to determine if a slot (specified by number) is in use by a
# drive, and return the path for that drive if so.
sub _is_slot_in_use {
    my ($self, $state, $slot) = @_;

    return 0 if !defined $state;
    return 0 if !defined $state->{slots};
    return 0 if !defined $self->{unaliased}->{$slot};
    my $unaliased = $self->{unaliased}->{$slot};
    return 0 if !defined $state->{slots}->{$unaliased};
    return 0 if !defined $state->{slots}->{$unaliased}->{pid};

    #check if PID is still alive
    my $pid = $state->{slots}->{$unaliased}->{pid};
    if (Amanda::Util::is_pid_alive($pid) == 1) {
	return 1;
    }

    delete $state->{slots}->{$unaliased}->{pid};
    return 0;
}

# Internal function to get the next slot after $slot.
# skip over except_slot and slot in use.
sub _get_next {
    my ($self, $slot, $except_slot) = @_;
    my $next_slot;

    $next_slot = $slot + 1;
    $next_slot = $self->{'first_slot'} if $next_slot >= $self->{'last_slot'};

    return $next_slot;
}

# Get the 'current' slot
sub _get_current {
    my ($self, $state) = @_;

    return $self->{slot} if defined $self->{slot};
    if (defined $state->{current_slot}) {
	my $slot = $self->{number}->{$state->{current_slot}};
	# return the slot if it exist.
	return $slot if $slot >= $self->{'first_slot'} && $slot < $self->{'last_slot'};
	Amanda::Debug::debug("statefile current_slot is not configured");
    }
    # return the first slot
    return $self->{first_slot};
}

# Set the 'current' slot
sub _set_current {
    my ($self, $state, $slot) = @_;

    $self->{slot} = $slot;
    $state->{current_slot} = $self->{unaliased}->{$slot};
}

package Amanda::Changer::multi::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );
use Amanda::Device qw( :constants );

sub new {
    my $class = shift;
    my ($chg, $device, $slot) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;
    $self->{'device'} = $device;
    $self->{'this_slot'} = $slot;

    return $self;
}

sub set_label {
    my $self = shift;
    my %params = @_;

    my $chg = $self->{chg};
    $chg->with_locked_state($chg->{'state_filename'},
			    $params{'finished_cb'}, sub {
	my ($state, $finished_cb) = @_;
	my $label = $params{'label'};
	my $slot = $self->{'this_slot'};
	my $unaliased = $chg->{unaliased}->{$slot};
	my $dev = $self->{'device'};

	$state->{slots}->{$unaliased}->{label} =  $label;
	$state->{slots}->{$unaliased}->{device_status} =
				"".$dev->status;
	if ($dev->status != $DEVICE_STATUS_SUCCESS) {
	    $state->{slots}->{$unaliased}->{device_error} = $dev->error;
	} else {
	    $state->{slots}->{$unaliased}->{device_error} = undef;
	}
	my $volume_header = $dev->volume_header;
	if (defined $volume_header) {
	    $state->{slots}->{$unaliased}->{f_type} =
				"".$volume_header->{type};
	} else {
	    $state->{slots}->{$unaliased}->{f_type} = undef;
	}
	$finished_cb->();
    });
}

sub do_release {
    my $self = shift;
    my %params = @_;

    # if we're in global cleanup and the changer is already dead,
    # then never mind
    return unless $self->{'chg'};

    $self->{'device'}->eject() if (exists $self->{'device'} and
				   exists $params{'eject'} and
				   $params{'eject'});

    # unref the device, for good measure
    $self->{'device'} = undef;

    if (exists $params{'unlocked'}) {
	my $state = $params{state};
	my $slot = $self->{'this_slot'};
	my $unaliased = $self->{chg}->{unaliased}->{$slot};
	delete $state->{slots}->{$unaliased}->{pid};
	return $params{'finished_cb'}->();
    }

    $self->{chg}->with_locked_state($self->{chg}->{'state_filename'},
				    $params{'finished_cb'}, sub {
	my ($state, $finished_cb) = @_;
	$params{state} = $state;
	my $slot = $self->{'this_slot'};
	my $unaliased = $self->{chg}->{unaliased}->{$slot};
	delete $state->{slots}->{$unaliased}->{pid};
	$finished_cb->();
    });
}
