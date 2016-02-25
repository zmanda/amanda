# Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Changer::disk;

use strict;
use warnings;
use Carp;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer );

use File::Glob qw( :glob );
use File::Path;
use File::Basename;
use Amanda::Config qw( :getconf :init string_to_boolean );
use Amanda::Debug qw( debug warning );
use Amanda::Changer;
use Amanda::MainLoop;
use Amanda::Device qw( :constants );

=head1 NAME

Amanda::Changer::disk

=head1 DESCRIPTION

This changer operates within a root directory, specified in the changer
string, which it arranges as follows:

  $dir -|
        |- data -> slot5
        |- slot1/
        |- slot2/
        |- ...
        |- slot$n/

The user should create the desired number of C<slot$n> subdirectories.  The
changer track
the current slot using a "data" symlink.  This allows use of "file:$dir" as a
device operating on the current slot, although note that it is unlocked.

See the amanda-changers(7) manpage for usage information.

=cut

# STATE
#
# The device state is shared between all changers accessing the same changer.
# It is a hash with keys:
#   current - the slot directory of the current slot
#   current_slot->{'config'}->{$config_name}->{'storage'}->{$storage_name}->{'changer'}->{$changer}
#   meta    - meta label of the vtapes
#   drives  - see below
#
# The 'drives' key is a hash, with drive as keys and hashes
# as values.  Each drive's hash has keys:
#   slot - slot directory
#   pid  - the pid that reserved that drive.
#


sub new {
    my $class = shift;
    my ($config, $tpchanger, %params) = @_;
    my ($dir) = ($tpchanger =~ /chg-disk:(.*)/);
    my $properties = $config->{'properties'};

    my $self = {
	dir => $dir,
	config => $config,
	state_filename => "$dir/state",
	global_space => 1,

	# list of all reservations
	reservation => {},

	# this is set to 0 by various test scripts,
	# notably Amanda_Taper_Scan_traditional
	support_fast_search => 1,
    };

    bless ($self, $class);

    if ($config->{'changerfile'}) {
	$self->{'state_filename'} = Amanda::Config::config_dir_relative($config->{'changerfile'});
    }
    $self->{'lock-timeout'} = $config->get_property('lock-timeout');

    $self->{'num-slot'} = $config->get_property('num-slot');
    $self->{'auto-create-slot'} = $config->get_boolean_property(
					'auto-create-slot', 0);
    $self->{'removable'} = $config->get_boolean_property('removable', 0);
    $self->{'mount'} = $config->get_boolean_property('mount', 0);
    $self->{'umount'} = $config->get_boolean_property('umount', 0);
    $self->{'umount_lockfile'} = $config->get_property('umount-lockfile');
    $self->{'umount_idle'} = $config->get_property('umount-idle');
    if (defined $self->{'umount_lockfile'}) {
	$self->{'fl'} = Amanda::Util::file_lock->new($self->{'umount_lockfile'})
    }

    if (!$params{'no_validate'}) {
	$self->_validate();
    }
    debug("chg-disk: Dir $dir");
    debug("chg-disk: Using statefile '$self->{state_filename}'");
    return $self->{'fatal_error'} if defined $self->{'fatal_error'};

    return $self;
}

sub DESTROY {
    my $self = shift;

    $self->SUPER::DESTROY();
}

sub quit {
    my $self = shift;

    if (defined $self->{'dir'}) {
	$self->force_unlock();
	delete $self->{'fl'};
	delete $self->{'dir'};
	$self->SUPER::quit();
    }
}

sub create {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'finished_cb'});

    if (!mkdir($self->{'dir'}, 0700)) {
	return $self->make_error("failed", $params{'finished_cb'},
		source_filename => __FILE__,
		source_line     => __LINE__,
		code    => 1100026,
		severity => $Amanda::Message::ERROR,
		dir     => $self->{'dir'},
		error   => $!,
		reason  => "unknown");
    }
    return $params{'finished_cb'}->(undef, Amanda::Changer::Message->new(
		source_filename => __FILE__,
		source_line     => __LINE__,
		code    => 1100027,
		severity => $Amanda::Message::SUCCESS,
		dir     => $self->{'dir'}));
}

sub load {
    my $self = shift;
    my %params = @_;
    my $old_res_cb = $params{'res_cb'};
    my $state;

    $self->validate_params('load', \%params);

    return if $self->check_error($params{'res_cb'});

    $self->with_disk_locked_state($params{'res_cb'}, sub {
	my ($state, $res_cb) = @_;
	$params{'state'} = $state;

	# overwrite the callback for _load_by_xxx
	$params{'res_cb'} = $res_cb;

	if (exists $params{'slot'} or exists $params{'relative_slot'}) {
	    $self->_load_by_slot(%params);
	} elsif (exists $params{'label'}) {
	    $self->_load_by_label(%params);
	}
    });
}

sub info_key {
    my $self = shift;
    my ($key, %params) = @_;
    my %results;
    my $info_cb = $params{'info_cb'};

    return if $self->check_error($info_cb);

    my $steps = define_steps
	cb_ref => \$info_cb;

    step init => sub {
	$self->try_lock($steps->{'locked'});
    };

    step locked => sub {
	return if $self->check_error($info_cb);

	# no need for synchronization -- all of these values are static

	if ($key eq 'num_slots') {
	    my @slots = $self->_all_slots();
	    $results{$key} = scalar @slots;
	} elsif ($key eq 'slots') {
	    my @slots = $self->_all_slots();
	    $results{$key} = \@slots;
	} elsif ($key eq 'vendor_string') {
	    $results{$key} = 'chg-disk'; # mostly just for testing
	} elsif ($key eq 'fast_search') {
	    $results{$key} = $self->{'support_fast_search'};
	}

	$self->try_unlock();
	$info_cb->(undef, %results) if $info_cb;
    }
}

sub reset {
    my $self = shift;
    my %params = @_;
    my $slot;
    my @slots = $self->_all_slots();

    return if $self->check_error($params{'finished_cb'});

    $self->with_disk_locked_state($params{'finished_cb'}, sub {
	my ($state, $finished_cb) = @_;

	$slot = (scalar @slots)? $slots[0] : 0;
	$self->_set_current($state, $slot);

	$finished_cb->();
    });
}

sub inventory {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'inventory_cb'});

    $self->with_disk_locked_state($params{'inventory_cb'}, sub {
	my ($state, $finished_cb) = @_;
	my @inventory;

	my @slots = $self->_all_slots();
	my $current = $self->_get_current($state);
	for my $slot (@slots) {
	    my $s = { slot => $slot, state => Amanda::Changer::SLOT_FULL };
	    $s->{'reserved'} = $self->_is_slot_in_use($state, $slot);
	    my $label = $self->_get_slot_label($slot);
	    if ($label) {
		$s->{'label'} = $self->_get_slot_label($slot);
		$s->{'f_type'} = "".$Amanda::Header::F_TAPESTART;
		$s->{'device_status'} = "".$DEVICE_STATUS_SUCCESS;
	    } else {
		$s->{'label'} = undef;
		$s->{'f_type'} = "".$Amanda::Header::F_EMPTY;
		$s->{'device_status'} = "".$DEVICE_STATUS_VOLUME_UNLABELED;
	    }
	    $s->{'current'} = 1 if $slot eq $current;
	    push @inventory, $s;
	}
	$finished_cb->(undef, \@inventory);
    });
}

sub set_meta_label {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'finished_cb'});

    $self->with_disk_locked_state($params{'finished_cb'}, sub {
	my ($state, $finished_cb) = @_;

	$state->{'meta'} = $params{'meta'};
	$finished_cb->(undef);
    });
}

sub with_disk_locked_state {
    my $self = shift;
    my ($cb, $sub) = @_;

    my $steps = define_steps
	cb_ref => \$cb;

    step init => sub {
	$self->try_lock($steps->{'locked'});
    };

    step locked => sub {
	my $err = shift;
	return $cb->($err) if $err;
	$self->with_locked_state($self->{'state_filename'},
	    sub { my @args = @_;
		  $self->try_unlock();
		  $cb->(@args);
		},
	    $sub);
    };
}

sub get_meta_label {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'finished_cb'});

    $self->with_disk_locked_state($params{'finished_cb'}, sub {
	my ($state, $finished_cb) = @_;

	$finished_cb->(undef, $state->{'meta'});
    });
}

sub _load_by_slot {
    my $self = shift;
    my %params = @_;
    my $drive;
    my $slot;

    if (exists $params{'relative_slot'}) {
	if ($params{'relative_slot'} eq "current") {
	    $slot = $self->_get_current($params{'state'});
	} elsif ($params{'relative_slot'} eq "next") {
	    if (exists $params{'slot'}) {
		$slot = $params{'slot'};
	    } else {
		$slot = $self->_get_current($params{'state'});
	    }
	    $slot = $self->_get_next($slot);
	    $self->_set_current($params{'state'}, $slot) if ($params{'set_current'});
	} else {
	    return $self->make_error("failed", $params{'res_cb'},
		reason => "invalid",
		message => "Invalid relative slot '$params{relative_slot}'");
	}
    } else {
	$slot = $params{'slot'};
    }

    if (exists $params{'except_slots'} and exists $params{'except_slots'}->{$slot}) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "notfound",
	    message => "all slots have been loaded");
    }

    if (!$self->_slot_exists($slot)) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "invalid",
	    slot   => $slot,
	    message => "Slot $slot not found");
    }

    if ($drive = $self->_is_slot_in_use($params{'state'}, $slot)) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "volinuse",
	    slot => $slot,
	    message => "Slot $slot is already in use by drive '$drive' and process '$params{state}->{drives}->{$drive}->{pid}'");
    }

    $drive = $self->_alloc_drive($params{state});
    $self->_load_drive($params{state}, $drive, $slot);
    $self->_set_current($params{state}, $slot) if ($params{'set_current'});

    $self->_make_res($params{'state'}, $params{'res_cb'}, $drive, $slot);
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

    if ($drive = $self->_is_slot_in_use($params{'state'}, $slot)) {
	return $self->make_error("failed", $params{'res_cb'},
	    reason => "volinuse",
	    message => "Slot $slot, containing '$label', is already " .
			"in use by drive '$drive' and process '$params{state}->{drives}->{$drive}->{pid}'");
    }

    $drive = $self->_alloc_drive($params{state});
    $self->_load_drive($params{state}, $drive, $slot);
    $self->_set_current($params{state}, $slot) if ($params{'set_current'});

    $self->_make_res($params{'state'}, $params{'res_cb'}, $drive, $slot);
}

sub _make_res {
    my $self = shift;
    my ($state, $res_cb, $drive, $slot, $meta) = @_;
    my $res;

    my $slot_path = "$self->{'dir'}/slot$slot";
    my $device_name = "file:$slot_path";
    my $device = Amanda::Device->new($device_name);
    if ($device->status != $DEVICE_STATUS_SUCCESS) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => "opening '$device_name': " . $device->error_or_status());
    }
    my ($use_data, $surety, $source) = $device->property_get("USE-DATA");
    if ($source == $PROPERTY_SOURCE_DEFAULT) {
	$use_data = $device->property_set("USE-DATA", "NO");
    }
    if (my $err = $self->{'config'}->configure_device($device, $self->{'storage'})) {
	return $self->make_error("failed", $res_cb,
		reason => "device",
		message => $err);
    }

    $res = Amanda::Changer::disk::Reservation->new($self, $device, $drive, $slot, $state->{'meta'});
    $state->{drives}->{$drive}->{pid} = $$;
    $device->read_label();

    $res_cb->(undef, $res);
}

# Internal function to find an unused (nonexistent) driveN subdirectory and
# create it.  Note that this does not add a 'data' symlink inside the directory.
sub _alloc_drive {
    my ($self, $state) = @_;
    my $n = 0;

    while (1) {
	my $drive = "drive$n";
	$n++;

	next if exists $state->{'drives'}->{$drive};

	return $drive;
    }
}

# Internal function to enumerate all available slots.  Slots are described by
# strings.
sub _all_slots {
    my ($self) = @_;
    my $dir = _quote_glob($self->{'dir'});
    my @slots;

    for my $slotname (bsd_glob("$dir/slot*/")) {
	my $slot;
	next unless (($slot) = ($slotname =~ /.*slot([0-9]+)\/$/));
	push @slots, $slot + 0;
    }

    return map { "$_"} sort { $a <=> $b } @slots;
}

# Internal function to determine whether a slot exists.
sub _slot_exists {
    my ($self, $slot) = @_;
    return (-d $self->{'dir'} . "/slot$slot");
}

# Internal function to determine if a slot (specified by number) is in use by a
# drive, and return the path for that drive if so.
sub _is_slot_in_use {
    my ($self, $state, $slot) = @_;
    my $dir = _quote_glob($self->{'dir'});

    foreach my $drive (keys %{$state->{'drives'}}) {
	my $adrive = $state->{'drives'}->{$drive};
	if (!defined $adrive->{'slot'} or !defined $adrive->{'pid'}) {
	    delete $state->{'drives'}->{$drive};
	    next;
	}

	my $tslot = $adrive->{'slot'};
	if (!(($tslot) = ($adrive->{'slot'} =~ /slot([0-9]+)/))) {
	    warn "invalid slot '$adrive->{'slot'}' for drive '$drive'";
	    next;
	}

	if ($tslot+0 == $slot) {
	    #check if process is alive
	    my $pid = $state->{'drives'}->{$drive}->{'pid'};
	    if (!defined $pid or !Amanda::Util::is_pid_alive($pid)) {
		delete $state->{'drives'}->{$drive};
		next;
	    }
	    return $drive;
	}
    }

    return 0;
}

sub _get_slot_label {
    my ($self, $slot) = @_;
    my $dir = _quote_glob($self->{'dir'});

    for my $symlink (bsd_glob("$dir/slot$slot/00000.*")) {
	my ($label) = ($symlink =~ qr{\/00000\.([^/]*)$});
	return $label;
    }

    return ''; # known, but blank
}

# Internal function to point a drive to a slot
sub _load_drive {
    my ($self, $state, $drive, $slot) = @_;

    $state->{'drives'}->{$drive}->{'slot'} = "slot$slot";
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

# Get the 'current' slot, represented as a symlink named 'data'
sub _get_current {
    my $self = shift;
    my $state = shift;

    my $storage = $self->{'storage'}->{'storage_name'};
    my $changer = $self->{'chg_name'};
    my $current_slot = $state->{'current_slot'}->{'config'}->{get_config_name()}->{'storage'}->{$storage}->{'changer'}->{$changer};
    if (defined $current_slot) {
	if ($current_slot =~ "^slot([0-9]+)/?") {
	    return $1;
	}
    }

    if ($state->{'current'}) {
        if ($state->{'current'} =~ "^slot([0-9]+)/?") {
            return $1;
        }
    }

    my $curlink = $self->{'dir'} . "/data";

    # for 2.6.1-compatibility, also parse a "current" symlink
    my $oldlink = $self->{'dir'} . "/current";
    if (-l $oldlink and ! -e $curlink) {
	rename($oldlink, $curlink);
    }

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
    my $self  = shift;
    my $state = shift;
    my $slot  = shift;

    $state->{'current'} = "slot$slot";
    my $storage = $self->{'storage'}->{'storage_name'};
    my $changer = $self->{'chg_name'};
    $state->{'current_slot'}->{'config'}->{get_config_name()}->{'storage'}->{$storage}->{'changer'}->{$changer} = "slot$slot";
    my $curlink = $self->{'dir'} . "/data";

    if (-l $curlink or -e $curlink) {
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

sub _validate() {
    my $self = shift;
    my $dir = $self->{'dir'};

    unless (-d $dir) {
	return $self->make_error("fatal", undef,
	    message => "directory '$dir' does not exist");
    }

    if ($self->{'removable'}) {
	my ($dev, $ino) = stat $dir;
	my $parentdir = dirname $dir;
	my ($pdev, $pino) = stat $parentdir;
	if ($dev == $pdev) {
	    if ($self->{'mount'}) {
		system $Amanda::Constants::MOUNT, $dir;
		($dev, $ino) = stat $dir;
	    }
	}
	if ($dev == $pdev) {
	    return $self->make_error("failed", undef,
		reason => "notfound",
		message => "No removable disk mounted on '$dir'");
	}
    }

    if ($self->{'num-slot'}) {
	for my $i (1..$self->{'num-slot'}) {
	    my $slot_dir = "$dir/slot$i";
	    if (!-e $slot_dir) {
		if ($self->{'auto-create-slot'}) {
		    if (!mkdir ($slot_dir)) {
			return $self->make_error("fatal", undef,
			    message => "Can't create '$slot_dir': $!");
		    }
		} else {
		    return $self->make_error("fatal", undef,
			message => "slot $i doesn't exists '$slot_dir'");
		}
	    }
	}
    } else {
	if ($self->{'auto-create-slot'}) {
	    return $self->make_error("fatal", undef,
		message => "property 'auto-create-slot' set but property 'num-slot' is not set");
	}
    }
    return undef;
}

sub try_lock {
    my $self = shift;
    my $cb = shift;
    my $poll = 0; # first delay will be 0.1s; see below
    my $time;

    if (defined $self->{'lock-timeout'}) {
	$time = time() + $self->{'lock-timeout'};
    } else {
	$time = time() + 1000;
    }


    my $steps = define_steps
	cb_ref => \$cb;

    step init => sub {
	if ($self->{'mount'} && defined $self->{'fl'} &&
	    !$self->{'fl'}->locked()) {
	    return $steps->{'lock'}->();
	}
	$steps->{'lock_done'}->();
    };

    step lock => sub {
	my $rv = $self->{'fl'}->lock_rd();
	if ($rv == 1 && time() < $time) {
	    # loop until we get the lock, increasing $poll to 10s
	    $poll += 100 unless $poll >= 10000;
	    return Amanda::MainLoop::call_after($poll, $steps->{'lock'});
	} elsif ($rv == 1) {
	    return $self->make_error("fatal", $cb,
		message => "Timeout trying to lock '$self->{'umount_lockfile'}'");
	} elsif ($rv == -1) {
	    return $self->make_error("fatal", $cb,
		message => "Error locking '$self->{'umount_lockfile'}'");
	} elsif ($rv == 0) {
	    if (defined $self->{'umount_src'}) {
		$self->{'umount_src'}->remove();
		$self->{'umount_src'} = undef;
	    }
	    return $steps->{'lock_done'}->();
	}
    };

    step lock_done => sub {
	my $err = $self->_validate();
	$cb->($err);
    };
}

sub try_umount {
    my $self = shift;

    my $dir = $self->{'dir'};
    if ($self->{'removable'} && $self->{'umount'}) {
	my ($dev, $ino) = stat $dir;
	my $parentdir = dirname $dir;
	my ($pdev, $pino) = stat $parentdir;
	if ($dev != $pdev) {
	    system $Amanda::Constants::UMOUNT, $dir;
	}
    }
}

sub force_unlock {
    my $self = shift;

    if (keys( %{$self->{'reservation'}}) == 0 ) {
	if ($self->{'fl'}) {
	    if ($self->{'fl'}->locked()) {
		$self->{'fl'}->unlock();
	    }
	    if ($self->{'umount'}) {
		if (defined $self->{'umount_src'}) {
		    $self->{'umount_src'}->remove();
		    $self->{'umount_src'} = undef;
		}
		if ($self->{'fl'}->lock_wr() == 0) {
		    $self->try_umount();
		    $self->{'fl'}->unlock();
		}
	    }
	}
    }
}

sub try_unlock {
    my $self = shift;

    my $do_umount = sub {
	local $?;

	$self->{'umount_src'} = undef;
	if ($self->{'fl'}->lock_wr() == 0) {
	    $self->try_umount();
	    $self->{'fl'}->unlock();
	}
    };

    if (defined $self->{'umount_idle'}) {
	if ($self->{'umount_idle'} == 0) {
	    return $self->force_unlock();
	}
	if (defined $self->{'fl'}) {
	    if (keys( %{$self->{'reservation'}}) == 0 ) {
		if ($self->{'fl'}->locked()) {
		    $self->{'fl'}->unlock();
		}
		if ($self->{'umount'}) {
		    if (defined $self->{'umount_src'}) {
			$self->{'umount_src'}->remove();
			$self->{'umount_src'} = undef;
		    }
		    $self->{'umount_src'} = Amanda::MainLoop::call_after(
						0+$self->{'umount_idle'},
						$do_umount);
		}
	    }
	}
    }
}

package Amanda::Changer::disk::Reservation;
use vars qw( @ISA );
@ISA = qw( Amanda::Changer::Reservation );

sub new {
    my $class = shift;
    my ($chg, $device, $drive, $slot, $meta) = @_;
    my $self = Amanda::Changer::Reservation::new($class);

    $self->{'chg'} = $chg;
    $self->{'drive'} = $drive;

    $self->{'device'} = $device;
    $self->{'this_slot'} = $slot;
    $self->{'meta'} = $meta;

    $self->{'chg'}->{'reservation'}->{$slot} += 1;
    return $self;
}

sub do_release {
    my $self = shift;
    my %params = @_;
    my $drive = $self->{'drive'};

    # unref the device, for good measure
    $self->{'device'} = undef;
    my $slot = $self->{'this_slot'};

    my $finish = sub {
	$self->{'chg'}->{'reservation'}->{$slot} -= 1;
	delete $self->{'chg'}->{'reservation'}->{$slot} if
		$self->{'chg'}->{'reservation'}->{$slot} == 0;
	$self->{'chg'}->try_unlock();
	delete $self->{'chg'};
	$self = undef;
	return $params{'finished_cb'}->();
    };

    if (exists $params{'unlocked'}) {
        my $state = $params{state};
	delete $state->{drives}->{$drive}->{pid};
	return $finish->();
    }

    $self->{chg}->with_locked_state($self->{chg}->{'state_filename'},
				    $finish, sub {
	my ($state, $finished_cb) = @_;

	delete $state->{drives}->{$drive};

	$finished_cb->();
    });
}

sub get_meta_label {
    my $self = shift;
    my %params = @_;

    $params{'slot'} = $self->{'this_slot'};
    $self->{'chg'}->get_meta_label(%params);
}

sub set_meta_label {
    my $self = shift;
    my %params = @_;

    $params{'slot'} = $self->{'this_slot'};
    $self->{'chg'}->set_meta_label(%params);
    $self->{'meta'} = $params{'meta'};
}
