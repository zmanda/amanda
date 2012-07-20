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
use Amanda::Util;
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
    return bless ($self, $class);
}

sub most_prefered {
    my $self = shift;
    return $self->oldest_reusable_volume();
}

sub first_reusable_label {
    my $self = shift;

    my $label;

    for my $tle (@{$self->{'tapelist'}->{'tles'}}) {
	$label = $tle->{'label'} if $self->is_reusable_volume(label => $tle->{'label'});
    }
    return $label;
}

sub last_use_label {
    my $self = shift;

    my $tles = $self->{'tapelist'}->{tles};
    my $label = $tles->[0]->{'label'};
}

sub analyze {
    my $self = shift;
    my $inventory  = shift;
    my $seen  = shift;
    my $res = shift;

    my $most_prefered;
    my @reusable;
    my @new_labeled;
    my $first_new_volume;
    my $new_volume;
    my @new_volume;
    my $first_unknown;
    my $unknown;
    my $current;
    my $label = $self->most_prefered();
    $self->{'most_prefered_label'} = $label;

    for my $i (0..(scalar(@$inventory)-1)) {
	my $sl = $inventory->[$i];
	if ($sl->{current}) {
	    $current = $sl;
	}
	next if $seen->{$sl->{slot}} and (!$res || $res->{'this_slot'} ne $sl->{'slot'});

	if (!defined $sl->{'state'} ||
	    $sl->{'state'} == Amanda::Changer::SLOT_UNKNOWN) {
	    $first_unknown = $sl if !$first_unknown;
	    $unknown = $sl if $current && !$unknown;
	} elsif ($sl->{'state'} == Amanda::Changer::SLOT_EMPTY) {
	} elsif (defined $sl->{'label'}) {
	    if ($label && $sl->{'label'} eq $label) {
		$most_prefered = $sl;
	    } elsif ($self->is_reusable_volume(label => $sl->{'label'})) {
		push @reusable, $sl;
	    } else {
		my $vol_tle = $self->{'tapelist'}->lookup_tapelabel($sl->{'label'});
		if ($vol_tle) {
		    if ($vol_tle->{'datestamp'} eq '0') {
			push @new_labeled, $sl;
		    }
		} elsif ($self->volume_is_labelable($sl)) {
		    $first_new_volume = $sl if !$first_new_volume;
		    $new_volume = $sl if $current && !$new_volume;
		    push @new_volume, $sl;
		}
	    }
	} elsif ($self->volume_is_labelable($sl)) {
	    $first_new_volume = $sl if !$first_new_volume;
	    $new_volume = $sl if $current && !$new_volume;
	    push @new_volume, $sl;
	} elsif (!defined($sl->{device_status}) && !defined($sl->{label})) {
	    $first_unknown = $sl if !$first_unknown;
	    $unknown = $sl if $current && !$unknown;
	} else {
	}
    }
    $unknown = $first_unknown if !defined $unknown;
    $new_volume = $first_new_volume if !defined $new_volume;

    my $first_label = $self->first_reusable_label();
    my $last_label = $self->last_use_label();

    my $reusable;
    for my $sl (@reusable) {
	my $tle = $self->{'tapelist'}->lookup_tapelabel($sl->{'label'});
	$sl->{'datestamp'} = $tle->{'datestamp'};
	$reusable = $sl if !defined $reusable or
			   $sl->{'datestamp'} < $reusable->{'datestamp'};
    }

    my $new_labeled;
    for my $sl (@new_labeled) {
	$new_labeled = $sl if !defined $new_labeled or
			      (!$last_label and
			       $sl->{'label'} lt $new_labeled->{'label'}) or
			      ($last_label and
			       $sl->{'label'} gt $last_label and
			       $sl->{'label'} lt $new_labeled->{'label'}) or
			      ($last_label and
			       $sl->{'label'} gt $last_label and
			       $new_labeled->{'label'} lt $last_label) or
			      ($last_label and
			       $new_labeled->{'label'} lt $last_label and
			       $sl->{'label'} lt $new_labeled->{'label'});
    }

    for my $sl (@new_volume) {
	$sl->{'label'} = $self->{'chg'}->make_new_tape_label(
					barcode => $sl->{'barcode'},
					slot => $sl->{'slot'},
					meta => $sl->{'meta'});
	$new_volume = $sl if defined $last_label and
			     $new_volume->{'label'} ne $sl->{'label'} and
			     (($sl->{'label'} gt $last_label and
			       $sl->{'label'} lt $new_volume->{'label'}) or
			      ($sl->{'label'} gt $last_label and
			       $new_volume->{'label'} lt $last_label) or
			      ($new_volume->{'label'} lt $last_label and
			       $sl->{'label'} lt $new_volume->{'label'}));
    }

    my $use;
    if ($new_labeled && $self->{'scan_conf'}->{'new_labeled'} eq 'soon') {
	$use = $new_labeled;
    } elsif ($new_volume && $self->{'scan_conf'}->{'new_volume'} eq 'soon') {
	$use = $new_volume;
    } elsif ($new_labeled &&
	      $self->{'scan_conf'}->{'new_labeled'} eq 'order' and
	      (!$label || !$first_label || !$last_label || !$most_prefered or
	       ($last_label and $most_prefered and
		$new_labeled->{'label'} gt $last_label and
	        $new_labeled->{'label'} lt $most_prefered->{'label'}) or
	       ($last_label and $most_prefered and
		$new_labeled->{'label'} gt $last_label and
	        $most_prefered->{'label'} lt $last_label) or
	       ($first_label and $most_prefered and
		$new_labeled->{'label'} lt $first_label and
	        $new_labeled->{'label'} lt $most_prefered->{'label'}))) {
	$use = $new_labeled;
    } elsif ($new_volume and
	     $self->{'scan_conf'}->{'new_volume'} eq 'order' and
	     (!$label || !$first_label || !$last_label || !$most_prefered or
	      ($last_label and $most_prefered and
	       $new_volume->{'label'} gt $last_label and
	       $new_volume->{'label'} lt $most_prefered->{'label'}) or
	      ($last_label and $most_prefered and
	       $new_volume->{'label'} gt $last_label and
	       $most_prefered->{'label'} lt $last_label) or
	      ($first_label and $most_prefered and
	       $new_volume->{'label'} lt $first_label and
	       $new_volume->{'label'} lt $most_prefered->{'label'}))) {
	$use = $new_volume;
    } elsif (defined $most_prefered) {
	$use = $most_prefered;
    } elsif (defined $reusable) {
	$use = $reusable;
    } elsif ($new_labeled and $self->{'scan_conf'}->{'new_labeled'} eq 'last') {
	$use = $new_labeled;
    } elsif ($new_volume and $self->{'scan_conf'}->{'new_volume'} eq 'last') {
	$use = $new_volume;
    }

    if ($use) {
	if (defined $res and $res->{'this_slot'} eq $use->{'slot'}) {
	    return (Amanda::ScanInventory::SCAN_DONE);
	} else {
	    return (Amanda::ScanInventory::SCAN_LOAD,
		    $use->{'slot'});
	}
    } elsif ($unknown and $self->{'scan_conf'}->{'scan'}) {
	return (Amanda::ScanInventory::SCAN_LOAD,
		$unknown->{'slot'});
    } elsif ($self->{'scan_conf'}->{'ask'}) {
	return (Amanda::ScanInventory::SCAN_ASK_POLL);
    } else {
	return (Amanda::ScanInventory::SCAN_FAIL);
    }
}

1;
