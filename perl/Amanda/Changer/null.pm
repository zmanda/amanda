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

package Amanda::Changer::null;

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

=head1 NAME

Amanda::Changer::null

=head1 DESCRIPTION

This changer always returns reservations for null devices.  It is useful to add
a null device to a RAIT device configuration.  It takes no arguments.

Note that this changer's constructor is guaranteed not to return an error.

See the amanda-changers(7) manpage for usage information.

=cut

sub new {
    my $class = shift;
    my ($config, $tpchanger) = @_;

    return bless ({}, $class);
}

sub load {
    my $self = shift;
    my %params = @_;
    return if $self->check_error($params{'res_cb'});

    $params{'res_cb'}->(undef, Amanda::Changer::null::Reservation->new()) if $params{'res_cb'};
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;
    return if $self->check_error($params{'info_cb'});

    if ($key eq 'num_slots') {
	$results{$key} = 1;
    } elsif ($key eq 'fast_search') {
	$results{$key} = 1;
    }

    $params{'info_cb'}->(undef, %results) if $params{'info_cb'};
}

sub reset {
    my $self = shift;
    my %params = @_;
    return if $self->check_error($params{'finished_cb'});

    $params{'finished_cb'}->() if $params{'finished_cb'};
}

package Amanda::Changer::null::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'device'} = Amanda::Device->new("null:");
    $self->{'this_slot'} = "null";

    return $self;
}
