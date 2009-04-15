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
use Amanda::Device qw( :constants );

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

    # check that $device_name is an honest-to-goodness device
    my $tmpdev = Amanda::Device->new($device_name);
    if ($tmpdev->status() != $DEVICE_STATUS_SUCCESS) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "chg-single: error opening device '$device_name': " .
			$tmpdev->error_or_status());
    }

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

    return if $self->check_error($params{'res_cb'});

    die "no res_cb supplied" unless (exists $params{'res_cb'});

    if ($self->{'reserved'}) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "inuse",
	    message => "'$self->{device_name}' is already reserved");
    }

    $params{'res_cb'} and $params{'res_cb'}->(
	    undef, Amanda::Changer::single::Reservation->new($self));
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;

    return if $self->check_error($params{'info_cb'});

    if ($key eq 'num_slots') {
	$results{$key} = 1;
    }

    $params{'info_cb'}->(undef, %results) if $params{'info_cb'};
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

    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
}
