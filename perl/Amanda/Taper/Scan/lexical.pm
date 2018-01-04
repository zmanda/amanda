# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94085, or: http://www.zmanda.com

package Amanda::Taper::Scan::lexical;

=head1 NAME

Amanda::Taper::Scan::lexical

=head1 SYNOPSIS

This package implements the "lexical" taperscan algorithm.  See
C<amanda-taperscan(7)>.

=cut

use strict;
use warnings;
use base qw( Amanda::ScanInventory Amanda::Taper::Scan );
use Carp;
use POSIX ();
use Data::Dumper;
use vars qw( @ISA );
use base qw(Exporter);
our @EXPORT_OK = qw($DEFAULT_CHANGER);

use Amanda::Paths;
use Amanda::Util qw( match_labelstr );
use Amanda::Device qw( :constants );
use Amanda::Debug qw( debug );
use Amanda::Changer;
use Amanda::MainLoop;
use Amanda::Interactivity;
use Amanda::Taper::Scan::traditional;

our $DEFAULT_CHANGER = {};

sub new {
    my $class = shift;
    my %params = @_;

    my $chg = $params{'changer'};
    if (!defined $chg) {
	$chg = Amanda::Changer->new();
	$params{'changer'} = $chg;
    }
    if (!$chg->have_inventory()) {
	return Amanda::Taper::Scan::traditional->new(%params);
    }
    my $self = Amanda::ScanInventory->new(%params);
    $self->{'handled-error'} = {};
    return bless ($self, $class);
}

sub last_use_label {
    my $self = shift;

    my $volumes = $self->{'catalog'}->find_volumes(
				pool => $self->{'storage'}->{'tapepool'},
				storage => $self->{'storage'}->{'storage_name'},
				order_write_timestamp => -1,
				max_volume => 1);
    return undef if !defined $volumes->[0];
    return $volumes->[0]->{'label'};
}

sub analyze {
    my $self = shift;
    my $inventory  = shift;
    my $seen  = shift;
    my $res = shift;

    my @reusable_before;
    my @reusable_after;
    my @new_volume_before;
    my @new_volume_after;
    my @new_labeled_before;
    my @new_labeled_after;
    my @unknown;
    my @error;

    my $last_label = $self->last_use_label();

    for my $i (0..(scalar(@$inventory)-1)) {
	my $sl = $inventory->[$i];
	next if $seen->{$sl->{slot}};

	if (!defined $sl->{'state'} ||
	    $sl->{'state'} == Amanda::Changer::SLOT_UNKNOWN) {
	    push @unknown, $sl
	} elsif ($sl->{'state'} == Amanda::Changer::SLOT_EMPTY) {
	} elsif (defined $sl->{'label'} &&
		 $sl->{device_status} == $DEVICE_STATUS_SUCCESS) {
	    if ($self->is_reusable_volume(label => $sl->{'label'})) {
		if ($last_label && $sl->{'label'} gt $last_label) {
		    push @reusable_after, $sl;
		} else {
		    push @reusable_before, $sl;
		}
	    } else {
		my $volume = $self->{'catalog'}->find_volume(
				$self->{'storage'}->{'tapepool'},
				$sl->{'label'});
		if ($volume) {
		    if ($self->volume_is_new_labelled($volume, $sl)) {
			if ($last_label && $sl->{'label'} gt $last_label) {
			    push @new_labeled_after, $sl;
			} else {
			    push @new_labeled_before, $sl;
			}
		    }
		} elsif ($self->volume_is_labelable($sl)) {
		    $sl->{'label'} = $self->{'chg'}->make_new_tape_label(
					barcode => $sl->{'barcode'},
					slot => $sl->{'slot'},
					meta => $sl->{'meta'});
		    if ($last_label && $sl->{'label'} gt $last_label) {
			push @new_volume_after, $sl;
		    } else {
			push @new_volume_before, $sl;
		    }
		}
	    }
	} elsif ($self->volume_is_labelable($sl)) {
	    $sl->{'label'} = $self->{'chg'}->make_new_tape_label(
					barcode => $sl->{'barcode'},
					slot => $sl->{'slot'},
					meta => $sl->{'meta'});
	    if ($last_label && $sl->{'label'} gt $last_label) {
		push @new_volume_after, $sl;
	    } else {
		push @new_volume_before, $sl;
	    }
	} elsif (!defined($sl->{device_status}) && !defined($sl->{label})) {
	    push @unknown, $sl;
	} elsif (defined($sl->{device_status}) and
		 ($sl->{'device_status'} & $DEVICE_STATUS_DEVICE_ERROR or
		  $sl->{'device_status'} & $DEVICE_STATUS_VOLUME_ERROR) and
		 not exists $self->{'handled-error'}->{$sl->{'device_error'}} and
		 not exists $self->{'new_error'}->{$sl->{'device_error'}}) {
	    push @error, $sl;
	} else {
	}
    }

    @reusable_before = sort { $a->{'label'} cmp $b->{'label'} } @reusable_before;
    @reusable_after = sort { $a->{'label'} cmp $b->{'label'} } @reusable_after;
    @new_labeled_before = sort { $a->{'label'} cmp $b->{'label'} } @new_labeled_before;
    @new_labeled_after = sort { $a->{'label'} cmp $b->{'label'} } @new_labeled_after;
    @new_volume_before = sort { $a->{'label'} cmp $b->{'label'} } @new_volume_before;
    @new_volume_after = sort { $a->{'label'} cmp $b->{'label'} } @new_volume_after;

    my @result;
    my @soon_before;
    my @soon_after;
    my @order_before;
    my @order_after;
    my @last_before;
    my @last_after;
    my $use;

    if (@new_labeled_after && $self->{'scan_conf'}->{'new_labeled'} eq 'soon') {
	push @soon_after, @new_labeled_after;
    }
    if (@new_labeled_before && $self->{'scan_conf'}->{'new_labeled'} eq 'soon') {
	push @soon_before, @new_labeled_before;
    }
    if (@new_volume_after && $self->{'scan_conf'}->{'new_volume'} eq 'soon') {
	push @soon_after, @new_volume_after;
    }
    if (@new_volume_before && $self->{'scan_conf'}->{'new_volume'} eq 'soon') {
	push @soon_before, @new_volume_before;
    }
    @soon_before = sort { $a->{'label'} cmp $b->{'label'} } @soon_before;
    @soon_after = sort { $a->{'label'} cmp $b->{'label'} } @soon_after;

    if (@new_labeled_before && $self->{'scan_conf'}->{'new_labeled'} eq 'order') {
	push @order_before, @new_labeled_before;
    }
    if (@new_labeled_after && $self->{'scan_conf'}->{'new_labeled'} eq 'order') {
	push @order_after, @new_labeled_after;
    }
    if (@new_volume_before && $self->{'scan_conf'}->{'new_volume'} eq 'order') {
	push @order_before, @new_volume_before;
    }
    if (@new_volume_after && $self->{'scan_conf'}->{'new_volume'} eq 'order') {
	push @order_after, @new_volume_after;
    }
    if (@reusable_after) {
	push @order_after, @reusable_after;
    }
    if (@reusable_before) {
	push @order_before, @reusable_before;
    }

    @order_before = sort { $a->{'label'} cmp $b->{'label'} } @order_before;
    @order_after = sort { $a->{'label'} cmp $b->{'label'} } @order_after;

    if (@new_labeled_after && $self->{'scan_conf'}->{'new_labeled'} eq 'last') {
	push @last_after, @new_labeled_after;
    }
    if (@new_labeled_before && $self->{'scan_conf'}->{'new_labeled'} eq 'last') {
	push @last_before, @new_labeled_before;
    }
    if (@new_volume_after && $self->{'scan_conf'}->{'new_volume'} eq 'last') {
	push @last_after, @new_volume_after;
    }
    if (@new_volume_before && $self->{'scan_conf'}->{'new_volume'} eq 'last') {
	push @last_before, @new_volume_before;
    }
    @last_before = sort { $a->{'label'} cmp $b->{'label'} } @last_before;
    @last_after = sort { $a->{'label'} cmp $b->{'label'} } @last_after;

    push @result, @soon_after, @soon_before, @order_after, @order_before, @last_after, @last_before, @unknown, @error;

    return \@result;
}

1;
