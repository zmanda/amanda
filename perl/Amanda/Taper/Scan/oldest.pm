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

package Amanda::Taper::Scan::oldest;

=head1 NAME

Amanda::Taper::Scan::oldest

=head1 SYNOPSIS

This package implements the "oldest" taperscan algorithm.  See
C<amanda-taperscan(7)>.

=cut

use strict;
use warnings;
use base qw( Amanda::ScanInventory Amanda::Taper::Scan );
use Amanda::Tapelist;
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

use sort 'stable';

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

    my $tles = $self->{'tapelist'}->{tles};
    return undef if !defined $tles->[0];
    return $tles->[0]->{'label'};
}

sub analyze {
    my $self = shift;
    my $inventory  = shift;
    my $seen  = shift;


    my @reusable;
    my @new_labeled;
    my @new_volume;
    my @unknown;
    my @error;

    for my $sl (@{$inventory}) {
	next if $seen->{$sl->{slot}};

	if (!defined $sl->{'state'} ||
	    $sl->{'state'} == Amanda::Changer::SLOT_UNKNOWN) {
	    push @unknown, $sl;
	} elsif ($sl->{'state'} == Amanda::Changer::SLOT_EMPTY) {
	} elsif (defined $sl->{'label'} &&
		 $sl->{device_status} == $DEVICE_STATUS_SUCCESS) {
	    my $vol_tle = $self->{'tapelist'}->lookup_tapelabel($sl->{'label'});
	    if ($self->is_reusable_volume(label => $sl->{'label'})) {
		push @reusable, $sl;
		if ($vol_tle){
		    $sl->{'datestamp'} = $vol_tle->{'datestamp'};
		}
	    } else {
		if ($vol_tle) {
		    $sl->{'datestamp'} = $vol_tle->{'datestamp'};
		    if ($self->volume_is_new_labelled($vol_tle, $sl)) {
			push @new_labeled, $sl;
		    }
		} elsif ($self->volume_is_labelable($sl)) {
		    $sl->{'label'} = $self->{'chg'}->make_new_tape_label(
					barcode => $sl->{'barcode'},
					slot => $sl->{'slot'},
					meta => $sl->{'meta'});
		    push @new_volume, $sl;
		}
	    }
	} elsif ($self->volume_is_labelable($sl)) {
	    $sl->{'label'} = $self->{'chg'}->make_new_tape_label(
					barcode => $sl->{'barcode'},
					slot => $sl->{'slot'},
					meta => $sl->{'meta'});
	    push @new_volume, $sl;
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

    @reusable = sort { $a->{'datestamp'} cmp $b->{'datestamp'} || $a->{'label'} cmp $b->{'label'} } @reusable;
    @new_labeled = sort { $a->{'datestamp'} cmp $b->{'datestamp'} || $a->{'label'} cmp $b->{'label'} } @new_labeled;
    @new_volume = sort { $a->{'label'} cmp $b->{'label'} } @new_volume;

debug("reusable: " . Data::Dumper::Dumper(\@reusable));
    my @result;
    if (@new_labeled && $self->{'scan_conf'}->{'new_labeled'} eq 'soon') {
	push @result, @new_labeled;
    }
    if (@new_volume && $self->{'scan_conf'}->{'new_volume'} eq 'soon') {
	push @result, @new_volume;
    }
    if (@new_labeled && $self->{'scan_conf'}->{'new_labeled'} eq 'order') {
	push @result, @new_labeled;
    }
    if (@new_volume && $self->{'scan_conf'}->{'new_volume'} eq 'order') {
	push @result, @new_volume;
    }
    if (@reusable) {
	push @result, @reusable;
    }
    if (@new_labeled && $self->{'scan_conf'}->{'new_labeled'} eq 'last') {
	push @result, @new_labeled;
    }
    if (@new_volume && $self->{'scan_conf'}->{'new_volume'} eq 'last') {
	push @result, @new_volume;
    }
    if (@unknown && $self->{'scan_conf'}->{'scan'}) {
	push @result, @unknown;
    }

    return \@result;
}
1;
