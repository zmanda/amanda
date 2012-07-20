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

package Amanda::ScanInventory;

=head1 NAME

Amanda::ScanInventory

=head1 SYNOPSIS

This package implements a base class for all scan that use the inventory.
see C<amanda-taperscan(7)>.

=cut

use strict;
use warnings;
use Amanda::Tapelist;
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

use constant SCAN_ASK      => 1; # call Amanda::Interactivity module
use constant SCAN_POLL     => 2; # wait 'poll_delay' and retry the scan.
use constant SCAN_FAIL     => 3; # abort
use constant SCAN_CONTINUE => 4; # continue to the next step
use constant SCAN_ASK_POLL => 5; # call Amanda::Interactivity module and
				 # poll at the same time.
use constant SCAN_LOAD     => 6; # load a slot
use constant SCAN_DONE     => 7; # successful scan

our $DEFAULT_CHANGER = {};

sub new {
    my $class = shift;
    my %params = @_;
    my $scan_conf = $params{'scan_conf'};
    my $tapelist = $params{'tapelist'};
    my $chg = $params{'changer'};
    my $interactivity = $params{'interactivity'};

    #until we have a config for it.
    $scan_conf = Amanda::ScanInventory::Config->new();
    $chg = Amanda::Changer->new(undef, tapelist => $tapelist) if !defined $chg;

    my $self = {
	initial_chg => $chg,
	chg         => $chg,
	scanning    => 0,
	scan_conf   => $scan_conf,
	tapelist    => $tapelist,
        interactivity => $interactivity,
	seen        => {},
	scan_num    => 0
    };
    return bless ($self, $class);
}


sub scan {
    my $self = shift;
    my %params = @_;

    die "Can only run one scan at a time" if $self->{'scanning'};
    $self->{'scanning'} = 1;
    $self->{'user_msg_fn'} = $params{'user_msg_fn'} || sub {};

    # refresh the tapelist at every scan
    $self->read_tapelist();

    # count the number of scans we do, so we can only load 'current' on the
    # first scan
    $self->{'scan_num'}++;

    $self->_scan(%params);
}

sub _user_msg {
    my $self = shift;
    my %params = @_;
    $self->{'user_msg_fn'}->(%params);
}

sub _scan {
    my $self = shift;
    my %params = @_;

    my $user_msg_fn = $params{'user_msg_fn'} || \&_user_msg_fn;
    my $action;
    my $action_slot;
    my $res;
    my $label;
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
    my $result_cb = $params{'result_cb'};

    my $steps = define_steps
	cb_ref => \$result_cb;

    step get_first_inventory => sub {
	$scan_running = 1;
	$self->{'chg'}->inventory(inventory_cb => $steps->{'got_first_inventory'});
    };

    step got_first_inventory => sub {
	(my $err, $inventory) = @_;

	if ($err && $err->notimpl) {
	    #inventory not implemented
	    die("no inventory");
	} elsif ($err and $err->fatal) {
	    #inventory fail
	    return $steps->{'call_result_cb'}->($err, undef);
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
	    die("no inventory");
	}
	return $steps->{'handle_error'}->($err, undef) if $err;

	# throw out the inventory result and move on if the situation has
	# changed while we were waiting
	return $steps->{'abort_scan'}->() if $abort_scan;
	return $steps->{'restart_scan'}->() if $restart_scan;

	# Remove from seen all slot that have state == SLOT_UNKNOWN
	# It is done when a scan is restarted from interactivity object.
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
		!defined ($seen{$slot}->{'failed'}) &&
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

	($action, $action_slot) = $self->analyze($inventory, \%seen, $res);

	if ($action == Amanda::ScanInventory::SCAN_DONE) {
	    return $steps->{'call_result_cb'}->(undef, $res);
	}

	if (defined $res) {
	    $res->release(finished_cb => $steps->{'released'});
	} else {
	    $steps->{'released'}->();
	}
    };

    step released => sub {
	if ($action == Amanda::ScanInventory::SCAN_LOAD) {
	    $slot_scanned = $action_slot;
	    $self->_user_msg(scan_slot => 1,
			     slot => $slot_scanned);
	    return $self->{'changer'}->load(
			slot => $slot_scanned,
			set_current => $params{'set_current'},
			res_cb => $steps->{'slot_loaded'});
	}

	my $err;
	if ($last_err) {
	    $err = $last_err;
	} else {
	    $err = Amanda::Changer::Error->new('failed',
				reason => 'notfound',
				message => "No acceptable volumes found");
	}

	if ($action == Amanda::ScanInventory::SCAN_FAIL) {
	    return $steps->{'handle_error'}->($err, undef);
	}
	$scan_running = 0;
	$steps->{'scan_next'}->($action, $err);
    };

    step slot_loaded => sub {
	(my $err, $res) = @_;

	# we don't responsd to abort_scan or restart_scan here, since we
	# have an open reservation that we should deal with.

	my $label;
	if ($res && defined $res->{device} &&
	    $res->{device}->status == $DEVICE_STATUS_SUCCESS) {
	    $label = $res->{device}->volume_label;
	}
	my $relabeled = !defined($label) || $label !~ /$self->{'labelstr'}/;
	$self->_user_msg(slot_result => 1,
			 slot => $slot_scanned,
			 label => $label,
			 err  => $err,
			 relabeled => $relabeled,
			 res  => $res);
	if ($res) {
	    my $f_type;
	    if (defined $res->{device}->volume_header) {
		$f_type = $res->{device}->volume_header->{type};
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
	} else {
	    $seen{$slot_scanned} = { failed => 1 };
	    if ($err->volinuse) {
		# Scan semantics for volinuse is different than changer.
		# If a slot with unknown label is loaded then we map
		# volinuse to driveinuse.
		$err->{reason} = "driveinuse";
	    }
	    $last_err = $err if $err->fatal || !$err->notfound;
	}
	return $steps->{'load_released'}->();
    };

    step load_released => sub {
	my ($err) = @_;

	# TODO: handle error

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
		die("error not defined");
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
	$steps->{'scan_next'}->($scan_method, $err, $continue_cb);
    };

    step scan_next => sub {
	my ($scan_method, $err, $continue_cb) = @_;

	if ($scan_method == SCAN_ASK && !defined $self->{'interactivity'}) {
	    $scan_method = SCAN_FAIL;
	}

	if ($scan_method == SCAN_ASK_POLL && !defined $self->{'interactivity'}) {
	    $scan_method = SCAN_FAIL;
	}

	if ($scan_method == SCAN_ASK) {
	    return $steps->{'scan_interactivity'}->("$err");
	} elsif ($scan_method == SCAN_POLL) {
	    $poll_src = Amanda::MainLoop::call_after(
				$self->{'scan_conf'}->{'poll_delay'},
				$steps->{'after_poll'});
	    return;
	} elsif ($scan_method == SCAN_ASK_POLL) {
	    $steps->{'scan_interactivity'}->("$err\n");
	    $poll_src = Amanda::MainLoop::call_after(
				$self->{'scan_conf'}->{'poll_delay'},
				$steps->{'after_poll'});
	    return;
	} elsif ($scan_method == SCAN_FAIL) {
	    return $steps->{'call_result_cb'}->($err, undef);
	} elsif ($scan_method == SCAN_CONTINUE) {
	    return $continue_cb->($err, undef);
	} else {
	    die("Invalid SCAN_* value:$err:$err->{'reason'}:$scan_method");
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
	    my $message = "$err_message\n";
	    if ($self->{'most_prefered_label'}) {
		$message .= "Insert volume labeled '$self->{'most_prefered_label'}'";
	    } else {
		$message .= "Insert a new volume";
	    }
	    $message .= " in changer and type <enter>\nor type \"^D\" to abort\n";
	    $self->{'interactivity'}->user_request(
				message     => $message,
				label       => $self->{'most_prefered_label'},
				new_volume  => !$self->{'most_prefered_label'},
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
	$last_err = undef;

	if ($err) {
	    if ($scan_running) {
		$abort_scan = $err;
		return;
	    } else {
		return $steps->{'call_result_cb'}->($err, undef);
	    }
	}

	if ($message ne '') {
	    # use a new changer
	    my $new_chg;
	    if (ref($message) eq 'HASH' and $message == $DEFAULT_CHANGER) {
		$message = undef;
	    }
	    $new_chg = Amanda::Changer->new($message,
					    tapelist => $self->{'tapelist'});
	    if ($new_chg->isa("Amanda::Changer::Error")) {
		return $steps->{'scan_interactivity'}->("$new_chg");
	    }
	    $self->{'chg'}->quit() if $self->{'chg'} != $self->{'initial_chg'};
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
	if (defined $res) {
	    $res->released(finished_cb => $steps->{'abort_scan_released'});
	} else {
	    $steps->{'abort_scan_released'}->();
	}
    };

    step abort_scan_released => sub {
	$steps->{'call_result_cb'}->($abort_scan, undef);
    };

    step call_result_cb => sub {
	(my $err, $res) = @_;

	# TODO: what happens if the search was aborted or
	# restarted in the interim?

	$abort_scan = undef;
	$poll_src->remove() if defined $poll_src;
	$poll_src = undef;
	$interactivity_running = 0;
	$self->{'interactivity'}->abort() if defined $self->{'interactivity'};
	$self->{'chg'}->quit() if $self->{'chg'} != $self->{'initial_chg'} and !$res;
	if ($err) {
	    $self->{'scanning'} = 0;
	    return $result_cb->($err, $res);
	}
	$label = $res->{'device'}->volume_label;
	if (!defined($label) || $label !~ /$self->{'labelstr'}/) {
	    $res->get_meta_label(finished_cb => $steps->{'got_meta_label'});
	    return;
	}
	$self->{'scanning'} = 0;
	return $result_cb->(undef, $res, $label, $ACCESS_WRITE);
    };

    step got_meta_label => sub {
	my ($err, $meta) = @_;
	if (defined $err) {
	    return $result_cb->($err, $res);
	}
	($label, my $make_err) = $res->make_new_tape_label(meta => $meta);
	if (!defined $label) {
	    # make this fatal, rather than silently skipping new tapes
	    $self->{'scanning'} = 0;
	    return $result_cb->($make_err, $res);
	}
	$self->{'scanning'} = 0;
	return $result_cb->(undef, $res, $label, $ACCESS_WRITE, 1);
    };
}

sub volume_is_labelable {
    my $self = shift;
    my $sl = shift;
    my $dev_status  = $sl->{'device_status'};
    my $f_type = $sl->{'f_type'};
    my $label = $sl->{'label'};
    my $slot = $sl->{'slot'};
    my $chg = $self->{'chg'};
    my $autolabel = $chg->{'autolabel'};

    if (!defined $dev_status) {
	return 0;
    } elsif ($dev_status & $DEVICE_STATUS_VOLUME_UNLABELED and
	     defined $f_type and
	     $f_type == $Amanda::Header::F_EMPTY) {
	if (!$autolabel->{'empty'}) {
	    $self->_user_msg(slot_result  => 1,
			     empty        => 1,
			     slot         => $slot);
	    return 0;
	}
    } elsif ($dev_status & $DEVICE_STATUS_VOLUME_UNLABELED and
	     defined $f_type and
	     $f_type == $Amanda::Header::F_WEIRD) {
	if (!$autolabel->{'non_amanda'}) {
	    $self->_user_msg(slot_result  => 1,
			     non_amanda   => 1,
			     slot         => $slot);
	    return 0;
	}
    } elsif ($dev_status & $DEVICE_STATUS_VOLUME_ERROR) {
	if (!$autolabel->{'volume_error'}) {
	    $self->_user_msg(slot_result  => 1,
			     volume_error => 1,
			     slot         => $slot);
	    return 0;
	}
    } elsif ($dev_status != $DEVICE_STATUS_SUCCESS) {
	    $self->_user_msg(slot_result  => 1,
			     not_success  => 1,
			     err          => $sl->{'device_error'},
			     slot         => $slot);
	return 0;
    } elsif ($dev_status == $DEVICE_STATUS_SUCCESS and
	     $f_type == $Amanda::Header::F_TAPESTART and
	     $label !~ /$self->{'labelstr'}/) {
	if (!$autolabel->{'other_config'}) {
	    $self->_user_msg(slot_result  => 1,
			     label        => $label,
			     does_not_match_labelstr => 1,
			     slot         => $slot);
	    return 0;
	}
    }

    return 1;
}
package Amanda::ScanInventory::Config;

sub new {
    my $class = shift;
    my ($cc) = @_;

    my $self = bless {}, $class;

    $self->{'poll_delay'} = 10000; #10 seconds

    $self->{'fatal'} = Amanda::ScanInventory::SCAN_CONTINUE;
    $self->{'driveinuse'} = Amanda::ScanInventory::SCAN_ASK_POLL;
    $self->{'volinuse'} = Amanda::ScanInventory::SCAN_ASK_POLL;
    $self->{'notfound'} = Amanda::ScanInventory::SCAN_ASK_POLL;
    $self->{'unknown'} = Amanda::ScanInventory::SCAN_FAIL;
    $self->{'invalid'} = Amanda::ScanInventory::SCAN_CONTINUE;

    $self->{'scan'} = 1;
    $self->{'ask'} = 1;
    $self->{'new_labeled'} = 'order';
    $self->{'new_volume'} = 'order';

    return $self;
}

1;
