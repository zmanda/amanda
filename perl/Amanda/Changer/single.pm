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

package Amanda::Changer::single;

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

Amanda::Changer::single

=head1 DESCRIPTION

This changer represents a single drive as a changer.  It may eventually morph
into something similar to the old C<chg-manual>.

Whatever you load, you get the volume in the drive.  The volume's either
reserved or not.  All pretty straightforward.

=head1 TODO

Support notifying the user that a tape is required, and some kind of "OK,
loaded" feedback mechanism -- perhaps a utility script of some sort, or an
amtape subcommand?

=cut

sub new {
    my $class = shift;
    my ($cc, $tpchanger) = @_;
    my ($device_name) = ($tpchanger =~ /chg-single:(.*)/);

    my $self = {
	device_name => $device_name,
	reserved => 0,
    };

    bless ($self, $class);
    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;

    die "no res_cb supplied" unless (exists $params{'res_cb'});

    if ($self->{'reserved'}) {
	Amanda::MainLoop::call_later($params{'res_cb'},
	    "'{$self->{device_name}}' is already reserved", undef);
    } else {
	Amanda::MainLoop::call_later($params{'res_cb'},
		undef, Amanda::Changer::single::Reservation->new($self));
    }
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

package Amanda::Changer::single::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($chg, $drive, $next_slot) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;

    $self->{'device_name'} = $chg->{'device_name'};
    $self->{'this_slot'} = '1';
    $self->{'next_slot'} = '1';
    $chg->{'reserved'} = 1;

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;

    $self->{'chg'}->{'reserved'} = 0;

    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
    }
}
