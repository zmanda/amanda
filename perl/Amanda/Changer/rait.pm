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

=head1 NAME

Amanda::Changer::rait

=head1 DESCRIPTION

This changer operates several child changers, returning RAIT devices composed of
the devices produced by the child changers.  It's modeled on the RAIT device.

=head1 USAGE

Specify this changer as C<chg-rait:{changer1,changer2,..}>, much like the RAIT
device.  The child devices are specified using a shell-like syntax, where
alternatives are enclosed in braces and separated by commas.

The string C<ERROR>, if given for a changer, will be specified directly to the
RAIT device, causing it to assume that child is missing and operate in degraded
mode.  This changer does not automatically detect and exclude failed child
changers.  If a tape robot breaks, you must explicitly specify ERROR for that
changer.

The slots used for this changer are comma-separated strings containing the
slots from each child device.

=cut

sub new {
    my $class = shift;
    my ($cc, $tpchanger) = @_;
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

    my $all_kids_done_cb = sub {
	my ($kid_results) = @_;
	my $result;

	# first, let's see if any changer gave an error
	if (!grep { defined($_->[0]) } @$kid_results) {
	    # no error .. combine the reservations and return a RAIT reservation
	    my $combined_res = Amanda::Changer::rait::Reservation->new(
		[ map { $_->[1] } @$kid_results ]);
	    $params{'res_cb'}->(undef, $combined_res);
	    return;
	}

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

	$self->_for_each_child(sub {
	    my ($kid_chg, $kid_cb, $kid_slot) = @_;
	    my %kid_params = %params;
	    $kid_params{'slot'} = $kid_slot;
	    $kid_params{'res_cb'} = $kid_cb;
	    $kid_chg->load(%kid_params);
	}, undef, $all_kids_done_cb, \@kid_slots);
    } elsif (exists $params{'label'}) {
	$self->_for_each_child(sub {
	    my ($kid_chg, $kid_cb) = @_;
	    my %kid_params = %params;
	    $kid_params{'res_cb'} = $kid_cb;
	    $kid_chg->load(%kid_params);
	}, undef, $all_kids_done_cb);
    } else {
	return $self->make_error("failed", $params{'res_cb'},
		reason => "invalid",
		message => "Invalid parameters to 'load'");
    }
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
	    Amanda::MainLoop::call_later($params{'info_cb'}, undef,
		num_slots => $num_slots);
	};

	$self->_for_each_child(sub {
	    my ($kid_chg, $kid_cb) = @_;
	    $kid_chg->info(info => [ 'num_slots' ], info_cb => $kid_cb);
	}, undef, $all_kids_done_cb, undef);
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
		Amanda::MainLoop::call_later($params{'info_cb'}, undef,
		    vendor_string => $vendor_string);
	    } else {
		Amanda::MainLoop::call_later($params{'info_cb'}, undef);
	    }
	};

	$self->_for_each_child(sub {
	    my ($kid_chg, $kid_cb) = @_;
	    $kid_chg->info(info => [ 'vendor_string' ], info_cb => $kid_cb);
	}, undef, $all_kids_done_cb, undef);
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
	    Amanda::MainLoop::call_later($params{'finished_cb'});
	};

	$self->_for_each_child(sub {
	    my ($kid_chg, $kid_cb) = @_;
	    $kid_chg->$op(%params, finished_cb => $kid_cb);
	}, undef, $all_kids_done_cb, undef);
    };
}

*reset = _mk_simple_op("reset");
*update = _mk_simple_op("update");
*clean = _mk_simple_op("clean");
*eject = _mk_simple_op("eject");

# for each child, run $sub (or, if the child is "ERROR", $errsub), passing it
# the changer, an aggregating callback, and the corresponding element from
# @$args (if specified).  The callback combines its results with the results
# from other changes, and when all results are available, calls $parent_cb.
#
# The call is $parent_cb->([ [ <chg_1_results> ], [ <chg_2_results> ], .. ]).
sub _for_each_child {
    my $self = shift;
    my ($sub, $errsub, $parent_cb, $args) = @_;

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
		Amanda::MainLoop::call_later($errsub, "ERROR", $child_cb, $arg);
	    } else {
		# no errsub; just call $child_cb directly
		Amanda::MainLoop::call_later($child_cb, undef);
	    }
	} else {
	    Amanda::MainLoop::call_later($sub, $child, $child_cb, $arg);
	}
    }
}

package Amanda::Changer::rait::Reservation;

use Amanda::Util qw( :alternates );
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($child_reservations) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    # filter out ay undefined reservations (e.g., from ERROR devices)
    $child_reservations = [ grep { defined($_) } @$child_reservations ];
    $self->{'child_reservations'} = $child_reservations;

    my @device_names = map { $_->{'device_name'} } @$child_reservations;
    $self->{'device_name'} = "rait:" . collapse_braced_alternates(\@device_names);

    my @slot_names;
    @slot_names = map { $_->{'this_slot'} } @$child_reservations;
    $self->{'this_slot'} = collapse_braced_alternates(\@slot_names);
    @slot_names = map { $_->{'next_slot'} } @$child_reservations;
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

	if (exists $params{'finished_cb'}) {
	    my $errstr;
	    if (@outer_errors) {
		$errstr = join("; ", @outer_errors);
	    }

	    Amanda::MainLoop::call_later($params{'finished_cb'}, $errstr);
	}
    };

    for my $res (@{$self->{'child_reservations'}}) {
	$res->release(%params, finished_cb => $maybe_finished);
    }
}
