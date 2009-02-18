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

=head1 USAGE

Specify this changer as C<chg-null>.

=cut

sub new {
    my $class = shift;
    my ($cc, $tpchanger) = @_;

    return bless ({}, $class);
}

sub load {
    my $self = shift;
    my %params = @_;

    Amanda::MainLoop::call_later($params{'res_cb'},
            undef, Amanda::Changer::null::Reservation->new());
}

sub info {
    my $self = shift;
    my %params = @_;
    my %results;

    die "no info_cb supplied" unless (exists $params{'info_cb'});
    die "no info supplied" unless (exists $params{'info'});

    for my $inf (@{$params{'info'}}) {
        if ($inf eq 'num_slots') {
            $results{$inf} = 1;
        } else {
            warn "Ignoring request for info key '$inf'";
        }
    }

    Amanda::MainLoop::call_later($params{'info_cb'}, undef, %results);
}

sub reset {
    my $self = shift;
    my %params = @_;

    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'});
    }
}

package Amanda::Changer::null::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'device_name'} = "null:";
    $self->{'this_slot'} = "null";
    $self->{'next_slot'} = "null";

    return $self;
}
