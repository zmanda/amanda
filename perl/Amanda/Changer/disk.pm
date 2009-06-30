# Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Changer::disk;

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

Amanda::Changer::disk

=head1 DESCRIPTION

This changer operates within a root directory, specified in the changer
string, which it arranges as follows:

  $dir -|
        |- drive0/ -|
        |           | data -> '../slot4'
        |- drive1/ -|
        |           | data -> '../slot1'
        |- current -> slot5
        |- slot1/
        |- slot2/
        |- ...
        |- slot$n/

The user should create the desired number of C<slot$n> subdirectories.  The
changer will take care of dynamically creating the drives as needed, and track
the "current" slot using the eponymous symlink.

Drives are dynamically allocated as Amanda applications request access to
particular slots.  Each drive is represented as a subdirectory containing a
'data' symlink pointing to the "loaded" slot.

See the amanda-changers(7) manpage for usage information.

=cut

# TODO:
# better locking (at least to work on a shared filesystem, if not NFS)

sub new {
    my $class = shift;
    my ($config, $tpchanger) = @_;
    my ($dir) = ($tpchanger =~ /chg-disk:(.*)/);

    unless (-d $dir) {
	return Amanda::Changer->make_error("fatal", undef,
	    message => "directory '$dir' does not exist");
    }

    # note that we don't track outstanding Reservation objects -- we know
    # they're gone when they delete their drive directory
    my $self = {
	dir => $dir,
	config => $config,

	# this is set to 0 by various test scripts,
	# notably Amanda_Taper_Scan_traditional
	support_fast_search => 1,
    };

    bless ($self, $class);
    return $self;
}

sub load {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'res_cb'});

    if (exists $params{'slot'}) {
        $self->_load_by_slot(%params);
    } elsif (exists $params{'label'}) {
        $self->_load_by_label(%params);
    } else {
	die "Invalid parameters to 'load'";
    }
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;

    return if $self->check_error($params{'info_cb'});

    if ($key eq 'num_slots') {
	my @slots = $self->_all_slots();
	$results{$key} = scalar @slots;
    } elsif ($key eq 'vendor_string') {
	$results{$key} = 'chg-disk'; # mostly just for testing
    } elsif ($key eq 'fast_search') {
	$results{$key} = $self->{'support_fast_search'};
    }

    $params{'info_cb'}->(undef, %results) if $params{'info_cb'};
}

sub reset {
    my $self = shift;
    my %params = @_;
    my $slot;
    my @slots = $self->_all_slots();

    return if $self->check_error($params{'finished_cb'});

    $slot = (scalar @slots)? $slots[0] : 0;
    $self->_set_current($slot);

    $params{'finished_cb'}->() if $params{'finished_cb'};
}

sub _load_by_slot {
    my $self = shift;
    my %params = @_;
    my $slot = $params{'slot'};
    my $drive;

    if ($slot eq "current") {
        $slot = $self->_get_current();
    } elsif ($slot eq "next") {
        $slot = $self->_get_current();
        $slot = $self->_get_next($slot);
    }

    if (!$self->_slot_exists($slot)) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "notfound",
	    message => "Slot $slot not found");
    }

    if ($drive = $self->_is_slot_in_use($slot)) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "inuse",
	    message => "Slot $slot is already in use by drive '$drive'");
    }

    $drive = $self->_alloc_drive();
    $self->_load_drive($drive, $slot);
    $self->_set_current($slot) if ($params{'set_current'});

    my $next_slot = $self->_get_next($slot);

    $self->_make_res($params{'res_cb'}, $drive, $slot, $next_slot);
}

sub _load_by_label {
    my $self = shift;
    my %params = @_;
    my $label = $params{'label'};
    my $slot;
    my $drive;

    $slot = $self->_find_label($label);
    if (!defined $slot) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "notfound",
	    message => "Label '$label' not found");
    }

    if ($drive = $self->_is_slot_in_use($slot)) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "notfound",
	    message => "Slot $slot, containing '$label', is already " .
			"in use by drive '$drive'");
    }

    $drive = $self->_alloc_drive();
    $self->_load_drive($drive, $slot);
    $self->_set_current($slot) if ($params{'set_current'});

    my $next_slot = $self->_get_next($slot);

    $self->_make_res($params{'res_cb'}, $drive, $slot, $next_slot);
}

sub _make_res {
    my $self = shift;
    my ($res_cb, $drive, $slot, $next_slot) = @_;
    my $res;

    my $device = Amanda::Device->new("file:$drive");
    if ($device->status != $DEVICE_STATUS_SUCCESS) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => "opening 'file:$drive': " . $device->error_or_status());
    }

    if (my $err = $self->{'config'}->configure_device($device)) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => $err);
    }

    $res = Amanda::Changer::disk::Reservation->new($self, $device, $drive, $slot, $next_slot);
    $res_cb->(undef, $res);
}

# Internal function to find an unused (nonexistent) driveN subdirectory and
# create it.  Note that this does not add a 'data' symlink inside the directory.
sub _alloc_drive {
    my ($self) = @_;
    my $n = 0;

    while (1) {
	my $drive = $self->{'dir'} . "/drive$n";
	$n++;

	warn "$drive is not a directory; please remove it" if (-e $drive and ! -d $drive);
	next if (-e $drive);
	next if (!mkdir($drive)); # TODO probably not a very effective locking mechanism..

	return $drive;
    }
}

# Internal function to enumerate all available slots.  Slots are described by
# integers.
sub _all_slots {
    my ($self) = @_;
    my $dir = _quote_glob($self->{'dir'});
    my @slots;

    for my $slotname (bsd_glob("$dir/slot*/")) {
	my $slot;
	next unless (($slot) = ($slotname =~ /.*slot([0-9]+)\/$/));
	push @slots, $slot + 0;
    }

    return sort @slots;
}

# Internal function to determine whether a slot exists.
sub _slot_exists {
    my ($self, $slot) = @_;
    return (-d $self->{'dir'} . "/slot$slot");
}

# Internal function to determine if a slot (specified by number) is in use by a
# drive, and return the path for that drive if so.
sub _is_slot_in_use {
    my ($self, $slot) = @_;
    my $dir = _quote_glob($self->{'dir'});

    for my $symlink (bsd_glob("$dir/drive*/data")) {
	if (! -l $symlink) {
	    warn "'$symlink' is not a symlink; please remove it";
	    next;
	}

	my $target = readlink($symlink);
	if (!$target) {
	    warn "could not read '$symlink': $!";
	    next;
	}

	my $tslot;
	if (!(($tslot) = ($target =~ /..\/slot([0-9]+)/))) {
	    warn "invalid changer symlink '$symlink' -> '$target'";
	    next;
	}

	if ($tslot+0 == $slot) {
	    $symlink =~ s{/data$}{}; # strip the trailing '/data'
	    return $symlink;
	}
    }

    return 0;
}

# Internal function to point a drive to a slot
sub _load_drive {
    my ($self, $drive, $slot) = @_;

    die "'$drive' does not exist" unless (-d $drive);
    if (-e "$drive/data") {
	unlink("$drive/data");
    }

    symlink("../slot$slot", "$drive/data");
    # TODO: read it to be sure??
}

# Internal function to return the slot containing a volume with the given
# label.  This takes advantage of the naming convention used by vtapes.
sub _find_label {
    my ($self, $label) = @_;
    my $dir = _quote_glob($self->{'dir'});
    $label = _quote_glob($label);

    my @tapelabels = bsd_glob("$dir/slot*/00000.$label");
    if (!@tapelabels) {
        return undef;
    }

    if (scalar @tapelabels > 1) {
        warn "Multiple slots with label '$label': " . (join ", ", @tapelabels);
    }

    my ($slot) = ($tapelabels[0] =~ qr{/slot([0-9]+)/00000.});
    return $slot;
}

# Internal function to get the next slot after $slot.
sub _get_next {
    my ($self, $slot) = @_;
    my $next_slot;

    # Try just incrementing the slot number
    $next_slot = $slot+1;
    return $next_slot if (-d $self->{'dir'} . "/slot$next_slot");

    # Otherwise, search through all slots
    my @all_slots = $self->_all_slots();
    my $prev = $all_slots[-1];
    for $next_slot (@all_slots) {
        return $next_slot if ($prev == $slot);
        $prev = $next_slot;
    }

    # not found? take a guess.
    return $all_slots[0];
}

# Get the 'current' slot, represented as a symlink named 'current'
sub _get_current {
    my ($self) = @_;
    my $curlink = $self->{'dir'} . "/current";

    if (-l $curlink) {
        my $target = readlink($curlink);
        if ($target =~ "^slot([0-9]+)/?") {
            return $1;
        }
    }

    # get the first slot as a default
    my @slots = $self->_all_slots();
    return 0 unless (@slots);
    return $slots[0];
}

# Set the 'current' slot
sub _set_current {
    my ($self, $slot) = @_;
    my $curlink = $self->{'dir'} . "/current";

    if (-e $curlink) {
        unlink($curlink)
            or warn("Could not unlink '$curlink'");
    }

    # TODO: locking
    symlink("slot$slot", $curlink);
}

# utility function
sub _quote_glob {
    my ($filename) = @_;
    $filename =~ s/([]{}\\?*[])/\\$1/g;
    return $filename;
}

package Amanda::Changer::disk::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($chg, $device, $drive, $slot, $next_slot) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;
    $self->{'drive'} = $drive;

    $self->{'device'} = $device;
    $self->{'this_slot'} = $slot;
    $self->{'next_slot'} = $next_slot;

    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;
    my $drive = $self->{'drive'};

    unlink("$drive/data")
	or warn("Could not unlink '$drive/data': $!");
    rmdir("$drive")
	or warn("Could not rmdir '$drive': $!");

    $params{'finished_cb'}->(undef) if $params{'finished_cb'};
}
