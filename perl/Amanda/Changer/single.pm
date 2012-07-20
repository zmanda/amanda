# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Changer::single;

use strict;
use warnings;
use Carp;
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

See the amanda-changers(7) manpage for usage information.

=cut

sub new {
    my $class = shift;
    my ($config, $tpchanger) = @_;
    my ($device_name) = ($tpchanger =~ /chg-single:(.*)/);

    # check that $device_name is an honest-to-goodness device
    my $tmpdev = Amanda::Device->new($device_name);
    if ($tmpdev->status() != $DEVICE_STATUS_SUCCESS) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "chg-single: error opening device '$device_name': " .
			$tmpdev->error_or_status());
    }

    my $self = {
	config => $config,
	device_name => $device_name,
	reserved => 0,
    };

    bless ($self, $class);
    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;
    $self->validate_params('load', \%params);

    return if $self->check_error($params{'res_cb'});

    confess "no res_cb supplied" unless (exists $params{'res_cb'});

    if ($self->{'reserved'}) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "volinuse",
	    message => "'$self->{device_name}' is already reserved");
    }

    if (keys %{$params{'except_slots'}} > 0) {
	return $self->make_error("failed", $params{'res_cb'},
		reason => "notfound",
		message => "all slots have been loaded");
    }

    my $device = Amanda::Device->new($self->{'device_name'});
    if ($device->status() != $DEVICE_STATUS_SUCCESS) {
	return $self->make_error("fatal", $params{'res_cb'},
	    message => "error opening device '$self->{device_name}': " . $device->error_or_status());
    }

    if (my $msg = $self->{'config'}->configure_device($device)) {
	# a failure to configure a device is fatal, since it's probably
	# a user configuration error (and thus unlikely to work for the
	# next device, either)
	return $self->make_error("fatal", $params{'res_cb'},
	    message => $msg);
    }

    my $res = Amanda::Changer::single::Reservation->new($self, $device);
    $device->read_label();
    $params{'res_cb'}->(undef, $res);
}

sub inventory {
    my $self = shift;
    my %params = @_;

    my @inventory;
    my $s = { slot => 0,
	      state => undef,
	      device_status => undef,
	      f_type => undef,
	      label => undef };
    push @inventory, $s;

    $params{'inventory_cb'}->(undef, \@inventory);
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;

    return if $self->check_error($params{'info_cb'});

    if ($key eq 'num_slots') {
	$results{$key} = 1;
    } elsif ($key eq 'fast_search') {
	# (asking the user for a specific label is faster than asking
	# for each "slot" in a sequential scan, so search is "fast")
	$results{$key} = 0;
    }

    $params{'info_cb'}->(undef, %results) if $params{'info_cb'};
}

package Amanda::Changer::single::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($chg, $device) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;

    $self->{'device'} = $device;

    $self->{'this_slot'} = '1';
    $chg->{'reserved'} = 1;

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;

    $self->{'device'}->eject() if (exists $self->{'device'} and
				   exists $params{'eject'} and
				   $params{'eject'});

    $self->{'chg'}->{'reserved'} = 0;

    # unref the device, for good measure
    $self->{'device'} = undef;

    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
}
