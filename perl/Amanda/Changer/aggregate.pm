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

package Amanda::Changer::aggregate;

use strict;
use warnings;
use Carp;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer );

use File::Glob qw( :glob );
use File::Path;
use Amanda::Config qw( :getconf );
use Amanda::Paths;
use Amanda::Debug qw( debug warning );
use Amanda::Util qw( :alternates );
use Amanda::Changer;
use Amanda::MainLoop;
use Amanda::Device qw( :constants );

=head1 NAME

Amanda::Changer::aggregate

=head1 DESCRIPTION

This changer operates several child changers.
Slot are numbered:
  0:0  changer 0 slot 0
  0:1  changer 0 slot 1
  1:0  changer 1 slot 0
  3:4  changer 3 slot 4

See the amanda-changers(7) manpage for usage information.

=cut

sub new {
    my $class = shift;
    my ($config, $tpchanger) = @_;
    my ($kidspecs) = ( $tpchanger =~ /chg-aggregate:(.*)/ );

    my @kidspecs = Amanda::Util::expand_braced_alternates($kidspecs);
    if (@kidspecs < 2) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "chg-aggregate needs at least two child changers");
    }

    my @children = map {
	($_ eq "ERROR")? "ERROR" : Amanda::Changer->new($_)
    } @kidspecs;

    my $fail_on_error;
    if (defined $config->{'properties'}->{'fail-on-error'}) {
	$fail_on_error = string_to_boolean($config->{'properties'}->{'fail-on-error'}->{'values'}[0]);
    } else {
	$fail_on_error = 1;
    }

    if (grep { $_->isa("Amanda::Changer::Error") } @children) {
	my @annotated_errs;
	my $valid = 0;
	for my $i (0 .. @children-1) {
	    if ($children[$i]->isa("Amanda::Changer::Error")) {
		push @annotated_errs,
		    [ $kidspecs[$i], $children[$i] ];
	    } else {
		$valid++;
	    }
	}
	if ($valid == 0 || $fail_on_error) {
	    return Amanda::Changer->make_combined_error(
		"fatal", [ @annotated_errs ]);
	}
    }

    my $state_filename;
    my $state_filename_prop = $config->{'properties'}->{'state_filename'};

    if (defined $state_filename_prop) {
	$state_filename = $state_filename_prop->{'values'}[0];
    }
    if (!defined $state_filename) {
	$state_filename = $Amanda::Paths::CONFIG_DIR . '/' . $config->{'name'} . ".state";
    }

    my $self = {
	config => $config,
	child_names => \@kidspecs,
	children => \@children,
	num_children => scalar @children,
	current_slot => undef,
	state_filename => $state_filename,
    };
    bless ($self, $class);
    return $self;
}

sub quit {
    my $self = shift;

    # quit each child
    foreach my $child (@{$self->{'children'}}) {
        $child->quit();
    }

    $self->SUPER::quit();
}

sub _get_current_slot
{
    my $self = shift;
    my $cb = shift;

    $self->with_locked_state($self->{'state_filename'}, $cb, sub {
	my ($state, $cb) = @_;
	$self->{'current_slot'} = $state->{'current_slot'};
	$self->{'current_slot'} = "0:first" if !defined $self->{'current_slot'};
	$cb->();
    });
}

sub _set_current_slot
{
    my $self = shift;
    my $cb = shift;

    $self->with_locked_state($self->{'state_filename'}, $cb, sub {
	my ($state, $cb) = @_;
	$state->{'current_slot'} = $self->{'current_slot'};
	$cb->();
    });
}

sub load {
    my $self = shift;
    my %params = @_;
    my $aggregate_res;

    return if $self->check_error($params{'res_cb'});

    my $res_cb = $params{'res_cb'};
    my $orig_slot = $params{'slot'};
    $self->validate_params('load', \%params);

    my $steps = define_steps
	cb_ref => \$res_cb;

    step which_slot => sub {
	if (exists $params{'relative_slot'} &&
	    $params{'relative_slot'} eq "current") {
	    if (defined $self->{'current_slot'}) {
		return $steps->{'set_from_current'}->();
	    } else {
		return $self->_get_current_slot($steps->{'set_from_current'});
	    }
	} elsif (exists $params{'relative_slot'} &&
		 $params{'relative_slot'} eq "next") {
	    if (defined $self->{'current_slot'}) {
		return $steps->{'get_inventory_next'}->();
	    } else {
		return $self->_get_current_slot($steps->{'get_inventory_next'});
	    }
	} elsif (exists $params{'label'}) {
	    return $self->inventory(inventory_cb => $steps->{'got_inventory_label'});
	}
	return $steps->{'slot_set'}->();
    };

    step get_inventory_next => sub {
	return $self->inventory(inventory_cb => $steps->{'got_inventory_next'})
    };

    step got_inventory_next => sub {
	my ($err, $inv) = @_;
	my $slot;
	if ($err) {
	    $res_cb->($err);
	}
	my $found = -1;
	for my $i (0.. scalar(@$inv)-1) {
	    $slot = @$inv[$i]->{'slot'};
	    if ($slot eq $self->{'current_slot'}) {
		$found = $i;
	    } elsif ($found >= 0 && (!exists $params{'except_slots'} ||
				!exists $params{'except_slots'}->{$slot})) {
		$orig_slot = $slot;
		return $steps->{'slot_set'}->();
	    }
	}
	if ($found >= 0) {
	    for my $i (0..($found-1)) {
		$slot = @$inv[$i]->{'slot'};
		if (!exists($params{'except_slots'}) ||
		    !exists($params{'except_slots'}->{$slot})) {
		    $orig_slot = $slot;
		    return $steps->{'slot_set'}->();
		}
	    }
	}
	return $self->make_error("failed", $res_cb,
		reason => "notfound",
		message => "all slots have been loaded");
    };

    step got_inventory_label => sub {
	my ($err, $inv) = @_;
	my $slot;
	if ($err) {
	    $res_cb->($err);
	}
	for my $i (0.. scalar(@$inv)-1) {
	    my $slot = @$inv[$i]->{'slot'};
	    my $label = @$inv[$i]->{'label'};
	    if ($label eq $params{'label'}) {
		$orig_slot = $slot;
		return $steps->{'slot_set'}->();
	    }
	}
	return $self->make_error("failed", $res_cb,
		reason => "notfound",
		message => "label $params{'label'} not found");
    };

    step set_from_current => sub {
	$orig_slot = $self->{'current_slot'};
	return $steps->{'slot_set'}->();
    };

    step slot_set => sub {
	my ($kid, $slot) = split(':', $orig_slot, 2);
	my $child = $self->{'children'}[$kid];
	if (!defined $child) {
	    return $self->make_error("failed", $res_cb,
		reason => "invalid",
		message => "no changer $kid");
	}
	delete $params{'relative_slot'};
	$params{'slot'} = $slot;
	$params{'res_cb'} = sub {
	    my ($err, $res) = @_;
	    if ($res) {
		if ($slot ne "first" && $res->{'this_slot'} != $slot) {
		    return $self->make_error("failed", $res_cb,
			reason => "invalid",
			message => "slot doesn't match: $res->{'this_slot'} != $slot");
		} else {
		    $self->{'current_slot'} = "$kid:$res->{'this_slot'}";
		    $aggregate_res = Amanda::Changer::aggregate::Reservation->new($self, $res, $self->{'current_slot'});
		    return $self->_set_current_slot($steps->{'done'});
		}
	    }
	    return $res_cb->($err, undef);
	};
	return $child->load(%params);
    };

    step done => sub {
	$res_cb->(undef, $aggregate_res);
    };
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

	    # Sum the result
	    my $num_slots;
	    for (@$kid_results) {
		my ($err, %kid_info) = @$_;
		next unless exists($kid_info{'num_slots'});
		my $kid_num_slots = $kid_info{'num_slots'};
		$num_slots += $kid_num_slots;
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

	if (exists $params{'drive'}) {
	    return $self->make_error("failed", $params{'finished_cb'},
		    reason => "notimpl",
		    message => "Can't specify drive fo $op command");
	}

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

{
    # perl doesn't like that these symbols are only mentioned once
    no warnings;

    *reset = _mk_simple_op("reset");
    *clean = _mk_simple_op("clean");
    *eject = _mk_simple_op("eject");
}

sub update {
    my $self = shift;
    my %params = @_;
    my %changed;
    my @kid_args;

    my $user_msg_fn = $params{'user_msg_fn'};
    $user_msg_fn ||= sub { Amanda::Debug::info("chg-aggregate: " . $_[0]); };

    if (exists $params{'changed'}) {
	for my $range (split ',', $params{'changed'}) {
	    my ($first, $last) = ($range =~ /([:\d]+)(?:-([:\d]+))?/);
	    $last = $first unless defined($last);
	    if ($first =~ /:/) {
		my ($f_kid, $f_slot) = split(':', $first, 2);
		my ($l_kid, $l_slot) = split(':', $last, 2);
		if ($f_kid != $l_kid) {
		    return;
		}
		if ($changed{$f_kid} != 1) {
		    for my $slot ($f_slot..$l_slot) {
			$changed{$f_kid}{$slot} = 1;
		    }
		}
	    } else {
		for my $kid ($first..$last) {
		    $changed{$kid} = 1;
		}
	    }
	}
	for my $kid (0..$self->{'num_children'}-1) {
	    if ($changed{$kid} == 1) {
		$kid_args[$kid] = "ALL";
	    } elsif (keys %{$changed{$kid}} > 0) {
		$kid_args[$kid] = { changed => join(',',sort(keys %{$changed{$kid}})) };
	    } else {
		$kid_args[$kid] = "NONE";
	    }
	}
    } else {
	for my $kid (0..$self->{'num_children'}-1) {
	    $kid_args[$kid] = "ALL";
	}
    }

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
	    my ($kid_chg, $kid_cb, $args) = @_;
	    if (ref($args) eq "HASH") {
		$kid_chg->update(%params, finished_cb => $kid_cb, %$args);
	    } elsif ($args eq "ALL") {
		$kid_chg->update(%params, finished_cb => $kid_cb);
	    } else {
		$kid_cb->();
	    }
	},
	errsub => undef,
	parent_cb => $all_kids_done_cb,
	args => \@kid_args,
    );
}

sub inventory {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'inventory_cb'});

    my $steps = define_steps
	cb_ref => \$params{'inventory_cb'};

    step get_current => sub {
	return $self->_get_current_slot($steps->{'got_current_slot'});
    };

    step got_current_slot => sub {
	$self->_for_each_child(
	    oksub => sub {
		my ($kid_chg, $kid_cb) = @_;
		$kid_chg->inventory(inventory_cb => $kid_cb);
	    },
	    errsub => undef,
	    parent_cb => $steps->{'all_kids_done_cb'},
	);
    };

    step all_kids_done_cb => sub {
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
		    message => "could not generate consistent inventory from aggregate child changers");
	}

	$params{'inventory_cb'}->(undef, $inv);
    };

}

sub set_meta_label {
    my $self = shift;
    my %params = @_;
    my $state;

    return if $self->check_error($params{'finished_cb'});

    my $finished_cb = $params{'finished_cb'};
    my $orig_slot = $params{'slot'};

    if (!defined $params{'slot'}) {
	return $self->make_error("failed", $finished_cb,
	    reason => "invalid",
	    message => "no 'slot' params set.");
    }

    if (!defined $params{'meta'}) {
	return $self->make_error("failed", $finished_cb,
	    reason => "invalid",
	    message => "no 'meta' params set.");
    }

    my ($kid, $slot) = split(':', $orig_slot, 2);
    my $child = $self->{'children'}[$kid];
    if (!defined $child) {
	return $self->make_error("failed", $finished_cb,
	    reason => "invalid",
	    message => "no changer $kid");
    }

    $params{'slot'} = $slot;
    return $child->set_meta_label(%params);
}

sub get_meta_label {
    my $self = shift;
    my %params = @_;
    my $state;

    return if $self->check_error($params{'finished_cb'});

    my $finished_cb = $params{'finished_cb'};
    my $orig_slot = $params{'slot'};

    if (!defined $params{'slot'}) {
	return $self->make_error("failed", $finished_cb,
	    reason => "invalid",
	    message => "no 'slot' params set.");
    }

    my ($kid, $slot) = split(':', $orig_slot, 2);
    my $child = $self->{'children'}[$kid];
    if (!defined $child) {
	return $self->make_error("failed", $finished_cb,
	    reason => "invalid",
	    message => "no changer $kid");
    }

    $params{'slot'} = $slot;
    return $child->get_meta_label(%params);
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
    my $nb = 0;
    for my $kid_result (@$kid_results) {
	my $kid_inv = $kid_result->[1];

	for my $x (@$kid_inv) {
	    my $slotname = "$nb:" . $x->{'slot'};
	    my $current = $slotname eq $self->{'current_slot'};
	    push @combined, {
		    state => $x->{'state'},
		    device_status => $x->{'device_status'},
		    f_type => $x->{'f_type'},
		    label => $x->{'label'},
		    barcode => $x->{'barcode'},
		    reserved => $x->{'reserved'},
		    slot => $slotname,
		    import_export => $x->{'import_export'},
		    loaded_in => $x->{'loaded_in'},
		    current => $current,
		};
	}
	$nb++;
    }

    return [ @combined ];
}

package Amanda::Changer::aggregate::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($chg, $kid_res, $slot) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;
    $self->{'kid_res'} = $kid_res;
    $self->{'device'} = $kid_res->{'device'};
    $self->{'barcode'} = $kid_res->{'barcode'};
    $self->{'this_slot'} = $slot;

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;

    # unref the device, for good measure
    $self->{'device'} = undef;

    $self->{'kid_res'}->release(%params);
    $self->{'kid_res'} = undef;
}

sub get_meta_label {
    my $self = shift;
    my %params = @_;

    $self->{'kid_res'}->get_meta_label(%params);
}

sub set_meta_label {
    my $self = shift;
    my %params = @_;

    $self->{'kid_res'}->set_meta_label(%params);
}
