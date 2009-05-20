# Copyright (c) 2005-2008 Zmanda, Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License version 2.1 as
# published by the Free Software Foundation.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
#
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Changer::rait;

use strict;
use warnings;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer );

use File::Glob qw( :glob );
use File::Path;
use Amanda::Config qw( :getconf );
use Amanda::Debug;
use Amanda::Util qw( :alternates );
use Amanda::Changer;
use Amanda::MainLoop;
use Amanda::Device qw( :constants );

=head1 NAME

Amanda::Changer::rait

=head1 DESCRIPTION

This changer operates several child changers, returning RAIT devices composed of
the devices produced by the child changers.  It's modeled on the RAIT device.

See the amanda-changers(7) manpage for usage information.

=cut

sub new {
    my $class = shift;
    my ($config, $tpchanger) = @_;
    my ($kidspecs) = ( $tpchanger =~ /chg-rait:(.*)/ );

    my @kidspecs = Amanda::Util::expand_braced_alternates($kidspecs);
    if (@kidspecs < 2) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "chg-rait needs at least two child changers");
    }

    my @children = map {
	($_ eq "ERROR")? "ERROR" : Amanda::Changer->new($_)
    } @kidspecs;

    if (grep { $_->isa("Amanda::Changer::Error") } @children) {
	my @annotated_errs;
	for my $i (0 .. @children-1) {
	    next unless $children[$i]->isa("Amanda::Changer::Error");
	    push @annotated_errs,
		[ $kidspecs[$i], $children[$i] ];
	}
	return Amanda::Changer->make_combined_error(
		"fatal", [ @annotated_errs ]);
    }

    my $self = {
	config => $config,
	child_names => \@kidspecs,
	children => \@children,
	num_children => scalar @children,
    };
    bless ($self, $class);
    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'res_cb'});

    my $release_on_error = sub {
	my ($kid_results) = @_;

	# an error has occurred, so we have to release all of the *non*-error
	# reservations (and handle errors in those releases!), then construct
	# and return a combined error message.

	my $releases_outstanding = 1; # start at one, in case the releases are immediate
	my @release_errors = ( undef ) x $self->{'num_children'};
	my $releases_maybe_done = sub {
	    return if (--$releases_outstanding);

	    # gather up the errors and combine them for return to our caller
	    my @annotated_errs;
	    for my $i (0 .. $self->{'num_children'}-1) {
		my $child_name = $self->{'child_names'}[$i];
		if ($kid_results->[$i][0]) {
		    push @annotated_errs,
			[ "from $child_name", $kid_results->[$i][0] ];
		}
		if ($release_errors[$i]) {
		    push @annotated_errs,
			[ "while releasing $child_name reservation",
			  $kid_results->[$i][0] ];
		}
	    }

	    return $self->make_combined_error(
		$params{'res_cb'}, [ @annotated_errs ]);
	};

	for my $i (0 .. $self->{'num_children'}-1) {
	    next unless (my $res = $kid_results->[$i][1]);
	    $releases_outstanding++;
	    $res->release(finished_cb => sub {
		$release_errors[$i] = $_[0];
		$releases_maybe_done->();
	    });
	}

	# we started $releases_outstanding at 1, so decrement it now
	$releases_maybe_done->();
    };

    my $all_kids_done_cb = sub {
	my ($kid_results) = @_;
	my $result;

	# first, let's see if any changer gave an error
	if (!grep { defined($_->[0]) } @$kid_results) {
	    # no error .. combine the reservations and return a RAIT reservation
	    return $self->_make_res($params{'res_cb'}, [ map { $_->[1] } @$kid_results ]);
	} else {
	    return $release_on_error->($kid_results);
	}
    };

    if (exists $params{'slot'}) {
	my $slot = $params{'slot'};

	# calculate the slots for each child
	my @kid_slots;
	if ($slot eq "current" or $slot eq "next") {
	    @kid_slots = ( $slot ) x $self->{'num_children'};
	} else {
	    @kid_slots = expand_braced_alternates($slot);
	    if (@kid_slots != $self->{'num_children'}) {
		# as a convenience, expand a single slot into the same slot for each child
		if (@kid_slots == 1) {
		    @kid_slots = ( $kid_slots[0] ) x $self->{'num_children'};
		} else {
		    return $self->make_error("failed", $params{'res_cb'},
			    reason => "invalid",
			    message => "slot '$slot' does not specify " .
					"$self->{num_children} child slots");
		}
	    }
	}

	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb, $kid_slot) = @_;
		my %kid_params = %params;
		$kid_params{'slot'} = $kid_slot;
		$kid_params{'res_cb'} = $kid_cb;
		$kid_chg->load(%kid_params);
	    },
	    errsub => sub {
		my ($kid_chg, $kid_cb, $kid_slot) = @_;
		$kid_cb->(undef, "ERROR");
	    },
	    parent_cb => $all_kids_done_cb,
	    args => \@kid_slots,
	);
    } elsif (exists $params{'label'}) {
	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb) = @_;
		my %kid_params = %params;
		$kid_params{'res_cb'} = $kid_cb;
		$kid_chg->load(%kid_params);
	    },
	    errsub => sub {
		my ($kid_chg, $kid_cb, $kid_slot) = @_;
		$kid_cb->(undef, "ERROR");
	    },
	    parent_cb => $all_kids_done_cb,
	);
    } else {
	return $self->make_error("failed", $params{'res_cb'},
		reason => "invalid",
		message => "Invalid parameters to 'load'");
    }
}

sub _make_res {
    my $self = shift;
    my ($res_cb, $kid_reservations) = @_;
    my @kid_devices = map { ($_ ne "ERROR") ? $_->{'device'} : undef } @$kid_reservations;

    my $rait_device = Amanda::Device->new_rait_from_children(@kid_devices);
    if ($rait_device->status() != $DEVICE_STATUS_SUCCESS) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => $rait_device->error_or_status());
    }

    if (my $err = $self->{'config'}->configure_device($rait_device)) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => $err);
    }

    my $combined_res = Amanda::Changer::rait::Reservation->new(
	$kid_reservations, $rait_device);
    $res_cb->(undef, $combined_res);
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;

    return if $self->check_error($params{'info_cb'});

    my $check_and_report_errors = sub {
	my ($kid_results) = @_;

	if (grep { defined($_->[0]) } @$kid_results) {
	    # we have errors, so collect them and make a "combined" error.
	    my @annotated_errs;
	    for my $i (0 .. $self->{'num_children'}-1) {
		my $kr = $kid_results->[$i];
		next unless defined($kr->[0]);
		push @annotated_errs,
		    [ $self->{'child_names'}[$i], $kr->[0] ];
	    }
	    $self->make_combined_error(
		$params{'info_cb'}, [ @annotated_errs ]);
	    return 1;
	}
    };

    if ($key eq 'num_slots') {
	my $all_kids_done_cb = sub {
	    my ($kid_results) = @_;
	    return if ($check_and_report_errors->($kid_results));

	    # aggregate the results: the consensus if the children agree,
	    # otherwise -1
	    my $num_slots;
	    for (@$kid_results) {
		my ($err, %kid_info) = @$_;
		next unless exists($kid_info{'num_slots'});
		my $kid_num_slots = $kid_info{'num_slots'};
		if (defined $num_slots and $num_slots != $kid_num_slots) {
		    $num_slots = -1;
		} else {
		    $num_slots = $kid_num_slots;
		}
	    }
	    $params{'info_cb'}->(undef, num_slots => $num_slots) if $params{'info_cb'};
	};

	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb) = @_;
		$kid_chg->info(info => [ 'num_slots' ], info_cb => $kid_cb);
	    },
	    errsub => undef,
	    parent_cb => $all_kids_done_cb,
	);
    } elsif ($key eq "vendor_string") {
	my $all_kids_done_cb = sub {
	    my ($kid_results) = @_;
	    return if ($check_and_report_errors->($kid_results));

	    my @kid_vendors =
		grep { defined($_) }
		map { my ($e, %r) = @$_; $r{'vendor_string'} }
		@$kid_results;
	    my $vendor_string;
	    if (@kid_vendors) {
		$vendor_string = collapse_braced_alternates([@kid_vendors]);
		$params{'info_cb'}->(undef, vendor_string => $vendor_string) if $params{'info_cb'};
	    } else {
		$params{'info_cb'}->(undef) if $params{'info_cb'};
	    }
	};

	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb) = @_;
		$kid_chg->info(info => [ 'vendor_string' ], info_cb => $kid_cb);
	    },
	    errsub => undef,
	    parent_cb => $all_kids_done_cb,
	);
    } elsif ($key eq 'fast_search') {
	my $all_kids_done_cb = sub {
	    my ($kid_results) = @_;
	    return if ($check_and_report_errors->($kid_results));

	    my @kid_fastness =
		grep { defined($_) }
		map { my ($e, %r) = @$_; $r{'fast_search'} }
		@$kid_results;
	    if (@kid_fastness) {
		my $fast_search = 1;
		# conduct a logical AND of all child fastnesses
		for my $f (@kid_fastness) {
		    $fast_search = $fast_search && $f;
		}
		$params{'info_cb'}->(undef, fast_search => $fast_search) if $params{'info_cb'};
	    } else {
		$params{'info_cb'}->(undef, fast_search => 0) if $params{'info_cb'};
	    }
	};

	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb) = @_;
		$kid_chg->info(info => [ 'fast_search' ], info_cb => $kid_cb);
	    },
	    errsub => undef,
	    parent_cb => $all_kids_done_cb,
	);
    }
}

# reset, clean, etc. are all *very* similar to one another, so we create them
# generically
sub _mk_simple_op {
    my ($op) = @_;
    sub {
	my $self = shift;
	my %params = @_;

	return if $self->check_error($params{'finished_cb'});

	my $all_kids_done_cb = sub {
	    my ($kid_results) = @_;
	    if (grep { defined($_->[0]) } @$kid_results) {
		# we have errors, so collect them and make a "combined" error.
		my @annotated_errs;
		for my $i (0 .. $self->{'num_children'}-1) {
		    my $kr = $kid_results->[$i];
		    next unless defined($kr->[0]);
		    push @annotated_errs,
			[ $self->{'child_names'}[$i], $kr->[0] ];
		}
		$self->make_combined_error(
		    $params{'finished_cb'}, [ @annotated_errs ]);
		return 1;
	    }
	    $params{'finished_cb'}->() if $params{'finished_cb'};
	};

	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb) = @_;
		$kid_chg->$op(%params, finished_cb => $kid_cb);
	    },
	    errsub => undef,
	    parent_cb => $all_kids_done_cb,
	);
    };
}

*reset = _mk_simple_op("reset");
*update = _mk_simple_op("update");
*clean = _mk_simple_op("clean");
*eject = _mk_simple_op("eject");

# Takes keyword parameters 'oksub', 'errsub', 'parent_cb', and 'args'.  For
# each child, runs $oksub (or, if the child is "ERROR", $errsub), passing it
# the changer, an aggregating callback, and the corresponding element from
# @$args (if specified).  The callback combines its results with the results
# from other changers, and when all results are available, calls $parent_cb.
#
# This forms a kind of "AND" combinator for a parallel operation on multiple
# changers, providing the caller with a simple collection of the results of
# the operation. The parent_cb is called as
#   $parent_cb->([ [ <chg_1_results> ], [ <chg_2_results> ], .. ]).
sub _for_each_child {
    my $self = shift;
    my %params = @_;
    my ($oksub, $errsub, $parent_cb, $args) =
	($params{'oksub'}, $params{'errsub'}, $params{'parent_cb'}, $params{'args'});

    if (defined($args)) {
	die "number of args did not match number of children"
	    unless (@$args == $self->{'num_children'});
    } else {
	$args = [ ( undef ) x $self->{'num_children'} ];
    }

    my $remaining = $self->{'num_children'};
    my @results = ( undef ) x $self->{'num_children'};
    my $maybe_done = sub {
	return if (--$remaining);
	$parent_cb->([ @results ]);
    };

    for my $i (0 .. $self->{'num_children'}-1) {
	my $child = $self->{'children'}[$i];
	my $arg = @$args? $args->[$i] : undef;

	my $child_cb = sub {
	    $results[$i] = [ @_ ];
	    $maybe_done->();
	};

	if ($child eq "ERROR") {
	    if (defined $errsub) {
		$errsub->("ERROR", $child_cb, $arg);
	    } else {
		# no errsub; just call $child_cb directly
		$child_cb->(undef) if $child_cb;
	    }
	} else {
	    $oksub->($child, $child_cb, $arg) if $oksub;
	}
    }
}

package Amanda::Changer::rait::Reservation;

use Amanda::Util qw( :alternates );
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

# utility function to act like 'map', but pass "ERROR" straight through
# (this has to appear before it is used, because it has a prototype)
sub errmap (&@) {
    my $sub = shift;
    return map { ($_ ne "ERROR")? $sub->($_) : "ERROR" } @_;
}

sub new {
    my $class = shift;
    my ($child_reservations, $rait_device) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    # note that $child_reservations may contain "ERROR" in place of a reservation

    $self->{'child_reservations'} = $child_reservations;

    my @device_names = errmap { $_->{'device_name'} } @$child_reservations;
    $self->{'device'} = $rait_device;

    my @slot_names;
    @slot_names = errmap { $_->{'this_slot'} } @$child_reservations;
    $self->{'this_slot'} = collapse_braced_alternates(\@slot_names);
    @slot_names = errmap { $_->{'next_slot'} } @$child_reservations;
    $self->{'next_slot'} = collapse_braced_alternates(\@slot_names);

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;
    my $remaining = @{$self->{'child_reservations'}};
    my @outer_errors;

    my $maybe_finished = sub {
	my ($err) = @_;
	push @outer_errors, $err if ($err);
	return if (--$remaining);

	my $errstr;
	if (@outer_errors) {
	    $errstr = join("; ", @outer_errors);
	}

	$params{'finished_cb'}->($errstr) if $params{'finished_cb'};
    };

    for my $res (@{$self->{'child_reservations'}}) {
	# short-circuit an "ERROR" reservation
	if ($res eq "ERROR") {
	    $maybe_finished->(undef);
	    next;
	}
	$res->release(%params, finished_cb => $maybe_finished);
    }
}

