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

package Amanda::Changer::rait;

use strict;
use warnings;
use Carp;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer );

use File::Glob qw( :glob );
use File::Path;
use Amanda::Config qw( :getconf );
use Amanda::Debug qw( debug warning );
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
	    if ($children[$i]->isa("Amanda::Changer::Error")) {
		push @annotated_errs,
		    [ $kidspecs[$i], $children[$i] ];
	    } elsif ($children[$i]->isa("Amanda::Changer")) {
		$children[$i]->quit();
	    }
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

sub quit {
    my $self = shift;

    # quit each child
    foreach my $child (@{$self->{'children'}}) {
	$child->quit() if $child ne "ERROR";
    }

    $self->SUPER::quit();
}

# private method to help handle slot input
sub _kid_slots_ok {
    my ($self, $res_cb, $slot, $kid_slots_ref, $err_ref) = @_;
    @{$kid_slots_ref} = expand_braced_alternates($slot);
    return 1 if (@{$kid_slots_ref} == $self->{'num_children'});

    if (@{$kid_slots_ref} == 1) {
        @{$kid_slots_ref} = ( $slot ) x $self->{'num_children'};
        return 1;
    }
    ${$err_ref} = $self->make_error("failed", $res_cb,
                                    reason => "invalid",
                                    message => "slot string '$slot' does not specify " .
                                    "$self->{num_children} child slots");
    return 0;
}

sub load {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'res_cb'});

    $self->validate_params('load', \%params);

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

    # make a template for params for the children
    my %kid_template = %params;
    delete $kid_template{'res_cb'};
    delete $kid_template{'slot'};
    delete $kid_template{'except_slots'};
    # $kid_template{'label'} is passed directly to children
    # $kid_template{'relative_slot'} is passed directly to children
    # $kid_template{'mode'} is passed directly to children

    # and make a copy for each child
    my @kid_params;
    for (0 .. $self->{'num_children'}-1) {
	push @kid_params, { %kid_template };
    }

    if (exists $params{'slot'}) {
	my $slot = $params{'slot'};

	# calculate the slots for each child
	my (@kid_slots, $err);
        return $err unless $self->_kid_slots_ok($params{'res_cb'}, $slot, \@kid_slots, \$err);
	if (@kid_slots != $self->{'num_children'}) {
	    # as a convenience, expand a single slot into the same slot for each child
	    if (@kid_slots == 1) {
		@kid_slots = ( $slot ) x $self->{'num_children'};
	    } else {
		return $self->make_error("failed", $params{'res_cb'},
			reason => "invalid",
			message => "slot '$slot' does not specify " .
				    "$self->{num_children} child slots");
	    }
	}
	for (0 .. $self->{'num_children'}-1) {
	    $kid_params[$_]->{'slot'} = $kid_slots[$_];
	}
    }

    # each slot in except_slots needs to get broken down, and the appropriate slot
    # given to each child
    if (exists $params{'except_slots'}) {
	for (0 .. $self->{'num_children'}-1) {
	    $kid_params[$_]->{'except_slots'} = {};
	}

	# for each slot, split it up, then apportion the result to each child
	for my $slot ( keys %{$params{'except_slots'}} ) {
	    my (@kid_slots, $err);
            return $err unless $self->_kid_slots_ok($params{'res_cb'}, $slot, \@kid_slots, \$err);
	    for (0 .. $self->{'num_children'}-1) {
		$kid_params[$_]->{'except_slots'}->{$kid_slots[$_]} = 1;
	    }
	}
    }

    $self->_for_each_child(
	oksub => sub {
	    my ($kid_chg, $kid_cb, $kid_params) = @_;
	    $kid_params->{'res_cb'} = $kid_cb;
	    $kid_chg->load(%$kid_params);
	},
	errsub => sub {
	    my ($kid_chg, $kid_cb, $kid_slot) = @_;
	    $kid_cb->(undef, "ERROR");
	},
	parent_cb => $all_kids_done_cb,
	args => \@kid_params,
    );
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
    $rait_device->read_label();

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
	    my @err_slots;
	    for my $i (0 .. $self->{'num_children'}-1) {
		my $kr = $kid_results->[$i];
		next unless defined($kr->[0]);
		push @annotated_errs,
		    [ $self->{'child_names'}[$i], $kr->[0] ];
		push @err_slots, $kr->[0]->{'slot'}
		    if (defined $kr->[0] and defined $kr->[0]->{'slot'});
	    }

	    my @slotarg;
	    if (@err_slots == $self->{'num_children'}) {
		@slotarg = (slot => collapse_braced_alternates([@err_slots]));
	    }

	    $self->make_combined_error(
		$params{'info_cb'}, [ @annotated_errs ],
		@slotarg);
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
		    debug("chg-rait: children have different slot counts!");
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
    my ($op, $has_drive) = @_;
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

	# get the drives for the kids, if necessary
	my @kid_args;
	if ($has_drive and $params{'drive'}) {
	    my $drive = $params{'drive'};
	    my @kid_drives = expand_braced_alternates($drive);

	    if (@kid_drives == 1) {
		@kid_drives = ( $kid_drives[0] ) x $self->{'num_children'};
	    }

	    if (@kid_drives != $self->{'num_children'}) {
		return $self->make_error("failed", $params{'finished_cb'},
			reason => "invalid",
			message => "drive string '$drive' does not specify " .
			"$self->{num_children} child drives");
	    }

	    @kid_args = map { { drive => $_ } } @kid_drives;
	    delete $params{'drive'};
	} else {
	    @kid_args = ( {} ) x $self->{'num_children'};
	}

	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb, $args) = @_;
		$kid_chg->$op(%params, finished_cb => $kid_cb, %$args);
	    },
	    errsub => undef,
	    parent_cb => $all_kids_done_cb,
	    args => \@kid_args,
	);
    };
}

{
    # perl doesn't like that these symbols are only mentioned once
    no warnings;

    *reset = _mk_simple_op("reset", 0);
    *update = _mk_simple_op("update", 0);
    *clean = _mk_simple_op("clean", 1);
    *eject = _mk_simple_op("eject", 1);
}

sub inventory {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'inventory_cb'});

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
	    return $self->make_combined_error(
		$params{'inventory_cb'}, [ @annotated_errs ]);
	}

	my $inv = $self->_merge_inventories($kid_results);
	if (!defined $inv) {
	    return $self->make_error("failed", $params{'inventory_cb'},
		    reason => "notimpl",
		    message => "could not generate consistent inventory from rait child changers");
	}

	$params{'inventory_cb'}->(undef, $inv);
    };

    $self->_for_each_child(
	oksub => sub {
	    my ($kid_chg, $kid_cb) = @_;
	    $kid_chg->inventory(inventory_cb => $kid_cb);
	},
	errsub => undef,
	parent_cb => $all_kids_done_cb,
    );
}

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
	confess "number of args did not match number of children"
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

sub _merge_inventories {
    my $self = shift;
    my ($kid_results) = @_;

    my @combined;
    for my $kid_result (@$kid_results) {
	my $kid_inv = $kid_result->[1];

	if (!@combined) {
	    for my $x (@$kid_inv) {
		push @combined, {
		    state => Amanda::Changer::SLOT_FULL,
		    device_status => undef, f_type => undef,
		    label => undef, barcode => [],
		    reserved => 0, slot => [],
		    import_export => 1, loaded_in => [],
		};
	    }
	}

	# if the results have different lengths, then we'll just call it
	# not implemented; otherwise, we assume that the order of the slots
	# in each child changer is the same.
	if (scalar @combined != scalar @$kid_inv) {
	    warning("child changers returned different-length inventories; cannot merge");
	    return undef;
	}

	my $i;
	for ($i = 0; $i < @combined; $i++) {
	    my $c = $combined[$i];
	    my $k = $kid_inv->[$i];
	    # mismatches here are just warnings
	    if (defined $c->{'label'}) {
		if (defined $k->{'label'} and $c->{'label'} ne $k->{'label'}) {
		    warning("child changers have different labels in slot at index $i");
		    $c->{'label_mismatch'} = 1;
		    $c->{'label'} = undef;
		} elsif (!defined $k->{'label'}) {
		    $c->{'label_mismatch'} = 1;
		    $c->{'label'} = undef;
		}
	    } else {
		if (!$c->{'label_mismatch'} && !$c->{'label_set'}) {
		    $c->{'label'} = $k->{'label'};
		}
	    }
	    $c->{'label_set'} = 1;

	    $c->{'device_status'} |= $k->{'device_status'}
		if defined $k->{'device_status'};

	    if (!defined $c->{'f_type'} ||
		$k->{'f_type'} != $Amanda::Header::F_TAPESTART) {
		$c->{'f_type'} = $k->{'f_type'};
	    }
	    # a slot is empty if any of the child slots are empty
	    $c->{'state'} = Amanda::Changer::SLOT_EMPTY
			if $k->{'state'} == Amanda::Changer::SLOT_EMPTY;

	    # a slot is reserved if any of the child slots are reserved
	    $c->{'reserved'} = $c->{'reserved'} || $k->{'reserved'};

	    # a slot is import-export if all of the child slots are import_export
	    $c->{'import_export'} = $c->{'import_export'} && $k->{'import_export'};

	    # barcodes, slots, and loaded_in are lists
	    push @{$c->{'slot'}}, $k->{'slot'};
	    push @{$c->{'barcode'}}, $k->{'barcode'};
	    push @{$c->{'loaded_in'}}, $k->{'loaded_in'};
	}
    }

    # now post-process the slots, barcodes, and loaded_in into braced-alternates notation
    my $i;
    for ($i = 0; $i < @combined; $i++) {
	my $c = $combined[$i];

	delete $c->{'label_mismatch'} if $c->{'label_mismatch'};
	delete $c->{'label_set'} if $c->{'label_set'};

	$c->{'slot'} = collapse_braced_alternates([ @{$c->{'slot'}} ]);

	if (grep { !defined $_ } @{$c->{'barcode'}}) {
	    delete $c->{'barcode'};
	} else {
	    $c->{'barcode'} = collapse_braced_alternates([ @{$c->{'barcode'}} ]);
	}

	if (grep { !defined $_ } @{$c->{'loaded_in'}}) {
	    delete $c->{'loaded_in'};
	} else {
	    $c->{'loaded_in'} = collapse_braced_alternates([ @{$c->{'loaded_in'}} ]);
	}
    }

    return [ @combined ];
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

    $self->{'device'} = $rait_device;

    my @slot_names;
    @slot_names = errmap { "" . $_->{'this_slot'} } @$child_reservations;
    $self->{'this_slot'} = collapse_braced_alternates(\@slot_names);

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

	# unref the device, for good measure
	$self->{'device'} = undef;

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

