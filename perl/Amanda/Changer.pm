# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License version 2.1 as 
# published by the Free Software Foundation.
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
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Changer;

use strict;
use warnings;
use Carp;
use POSIX ();
use vars qw( @ISA );

use Amanda::Paths;
use Amanda::Util;
use Amanda::Config qw( :getconf );
use Amanda::Device qw( :constants );

=head1 NAME

Amanda::Changer -- interface to changer scripts

=head1 SYNOPSIS

    use Amanda::Changer;

    my $chg = Amanda::Changer->new(); # loads the default changer; OR
    $chg = Amanda::Changer->new("somechanger"); # references a defined changer in amanda.conf

    $chg->load(
	label => "TAPE-012",
	res_cb => sub {
	    my ($err, $reservation) = @_;
	    if ($err) {
		die $err->{message};
	    }
	    $dev = Amanda::Device->new($reservation->{device_name});
	    # use device..
	});

    # later..
    $reservation->release(finished_cb => $start_next_volume);

=head1 API STATUS

This interface will change before the next release.

=head1 INTERFACE

All operations in the module return immediately, and take as an argument a
callback function which will indicate completion of the changer operation -- a
kind of continuation.  The caller should run a main loop (see
L<Amanda::MainLoop>) to allow the interactions with the changer script to
continue.

A new object is created with the C<new> function as follows:

  my $chg = Amanda::Changer->new($changer);

to create a named changer (a name provided by the user, either specifying a
changer directly or specifying a changer definition), or

  my $chg = Amanda::Changer->new();

to run the default changer.  This function handles the many ways a user can
configure a changer.

=head2 CALLBACKS

A res_cb C<$cb> is called back as:

 $cb->($error, undef);

in the event of an error, or

 $cb->(undef, $reservation);

with a successful reservation. res_cb must always be specified.  A finished_cb
C<$cb> is called back as

 $cb->($error);

in the event of an error, or

 $cb->(undef);

on success. A finished_cb may be omitted if no notification of completion is
required.

=head2 CURRENT SLOT

Changers maintain a global concept of a "current" slot, for
compatibility with Amanda algorithms such as the taperscan.  However, it
is not compatible with concurrent use of the same changer, and may be
inefficient for some changers, so new algorithms should avoid using it,
preferring instead to load the correct tape immediately (with C<load>),
and to progress from tape to tape using the reservation objects'
C<next_slot> attribute.

=head2 CHANGER OBJECTS

=head3 $chg->load(res_cb => $cb, label => $label, set_current => $sc)

Load a volume with the given label. This may leverage any barcodes or other
indices that the changer has created, or may resort to a sequential scan of
media. If set_current is specified and true, then the changer's current slot
should be updated to correspond to $slot. If not, then the changer should not
update its current slot (but some changers will anyway - specifically,
chg-compat).

Note that the changer I<tries> to load the requested volume, but it's a mean
world out there, and you may not get what you want, so check the label on the
loaded volume before getting started.

=head3 $chg->load(res_cb => $cb, slot => "current")

Reserve the volume in the "current" slot. This is used by the sequential
taperscan algorithm to begin its search.

=head3 $chg->load(res_cb => $cb, slot => "next")

Reserve the volume that follows the current slot.  This may not be a
very efficient operation on all devices.

=head3 $chg->load(res_cb => $cb, slot => $slot, set_current => $sc)

Reserve the volume in the given slot. $slot must be a string that appeared in a
reservation's 'next_slot' field at some point, or a string from the user (e.g.,
an argument to amtape).

=head3 $chg->info(info_cb => $cb, info => [ $key1, $key2, .. ])

Query the changer for miscellaneous information.  Any number of keys may be
specified.  The C<info_cb> is called with C<$error> as the first argument,
much like a C<res_cb>, but the remaining arguments form a hash giving values
for all of the requested keys that are supported by the changer.  The preamble
to such a callback is usually

  info_cb => sub {
    my $error = shift;
    my %results = @_;
    # ..
  }

Supported keys are:

=over 2

=item num_slots

The total number of slots in the changer device.  If this key is not
present, then the device cannot determine its slot count (for example,
an archival device that names slots by timestamp could potentially run
until the heat-death of the universe).

=item vendor_string

A string describing the name and model of the changer device.

=back

=head3 $chg->reset(finished_cb => $cb)

Reset the changer to a "base" state. This will generally reset the "current"
slot to something the user would think of as the "first" tape, unload any
loaded drives, etc. It is an error to call this while any reservations are
outstanding.

=head3 $chg->clean(finished_cb => $cb, drive => $drivename)

Clean a drive, if the changer supports it. Drivename can be an empty string for
devices with only one drive, or can be an arbitrary string from the user (e.g.,
an amtape argument). Note that some changers cannot detect the completion of a
cleaning cycle; in this case, the user will just need to delay further Amanda
activities until the cleaning is complete.

=head3 $chg->eject(finished_cb => $cb, drive => $drivename)

Eject the volume in a drive, if the changer supports it.  Drivename is as
specified to C<clean>.  If possible, applications should prefer to eject a
reserved volume when finished with it (C<< $res->release(eject => 1) >>), to
ensure that the correct volume is ejected from a multi-drive changer.

=head3 $chg->update(finished_cb => $cb, changed => $changed)

The user has changed something -- loading or unloading tapes,
reconfiguring the changer, etc. -- that may have invalidated the
database.  C<$changed> is a changer-specific string indicating what has
changed; if it is omitted, the changer will check everything.

=head3 $chg->import(finished_cb => $cb, slots => $slots)

The user has placed volumes in the import/export slots, and would like the
changer to place them in storage slots. This is a very changer-specific
operation, and $slots should be supplied by the user for verbatim transmission
to the changer, and may specify which import/export slots, for example, contain
the new volumes.

=head3 $chg->export(finished_cb => $cb, slot => $slot)

=head3 $chg->export(finished_cb => $cb, label => $label)

Place the indicated volume (by $label, or in $slot) into an available
import/export slot. This, too, is a very changer-specific operation.

=head3 $chg->move(finished_cb => $cb, from_slot => $from, to_slot => $to)

Move a volume between two slots in the changer. These slots are provided by the
user, and have meaning for the changer.

=head2 RESERVATION OBJECTS

=head3 $res->{'device_name'}

This is the name of the device reserved by a reservation object.

=head3 $res->{'this_slot'}

This is the name of this slot.  It is an arbitrary string which will
have some meaning to the changer's C<load()> method. It is safe to
access this field after the reservation has been released.

=head3 $res->{'next_slot'}

This is the "next" slot after this one. It is safe to access this field,
too, after the reservation has been released (and, in changers with only
one "drive", this is the only way you will get to the next volume!)

=head3 $res->release(finished_cb => $cb, eject => $eject)

This is how an Amanda application indicates that it no longer needs the
reserved volume. The callback is called after any related operations are
complete -- possibly immediately. Some drives and changers have a notion of
"ejecting" a volume, and some don't. In particular, a manual changer can cause
the tape drive to eject the tape, while a tape robot can move a tape back to
storage, leaving the drive empty. If the eject parameter is given and true, it
indicates that Amanda is done with the volume and has reason to believe the
user is done with the volume, too -- for example, when a tape has been written
completely.

A reservation will be released automatically when the object is destroyed, but
in this case no finished_cb is given, so the release operation may not complete
before the process exits. Wherever possible, reservations should be explicitly
released.

=head3 $res->set_label(finished_cb => $cb, label => $label)

This is how Amanda indicates to the changer that the volume in the device has
been (re-)labeled. Changers can keep a database of volume labels by slot or by
barcode, or just ignore this function and call $cb immediately. Note that the
reservation must still be held when this function is called.

=head1 SEE ALSO

See the other changer packages, including:

=over 2

=item L<Amanda::Changer::disk>

=item L<Amanda::Changer::compat>

=item L<Amanda::Changer::single>

=back

=head1 TODO

 - support loading by barcode, showing barcodes in reservations
 - support deadlock avoidance by returning more information in load errors
 - Amanda::Changer::Single

=cut

# this is a "virtual" constructor which instantiates objects of different
# classes based on its argument.  Subclasses should not try to chain up!
sub new {
    shift eq 'Amanda::Changer'
	or die("Do not call the Amanda::Changer constructor from subclasses");
    my ($name) = @_;
    my ($uri, $cc);

    # creating a named changer is a bit easier
    if (defined($name)) {
	# first, is it a changer alias?
	if (($uri,$cc) = _changer_alias_to_uri($name)) {
	    return _new_from_uri($uri, $cc, $name);
	}

	# maybe a straight-up changer URI?
	if (_uri_to_pkgname($name)) {
	    return _new_from_uri($name, undef, $name);
	}

	# assume it's a device name or alias, and invoke the single-changer
	return _new_from_uri("chg-single:$name", undef, $name);
    } else { # !defined($name)
	if (getconf_seen($CNF_TPCHANGER)) {
	    my $tpchanger = getconf($CNF_TPCHANGER);

	    # first, is it an old changer script?
	    if ($uri = _old_script_to_uri($tpchanger)) {
		return _new_from_uri($uri, undef, $name);
	    }

	    # if not, then there had better be no tapdev
	    if (getconf_seen($CNF_TAPEDEV)) {
		die "Cannot specify both 'tapedev' and 'tpchanger' unless using an old-style changer script";
	    }

	    # maybe a changer alias?
	    if (($uri,$cc) = _changer_alias_to_uri($tpchanger)) {
		return _new_from_uri($uri, $cc, $name);
	    }

	    # maybe a straight-up changer URI?
	    if (_uri_to_pkgname($tpchanger)) {
		return _new_from_uri($tpchanger, undef, $name);
	    }

	    # assume it's a device name or alias, and invoke the single-changer
	    return _new_from_uri("chg-single:$tpchanger", undef, $name);
	} elsif (getconf_seen($CNF_TAPEDEV)) {
	    my $tapedev = getconf($CNF_TAPEDEV);

	    # first, is it a changer alias?
	    if (($uri,$cc) = _changer_alias_to_uri($tapedev)) {
		return _new_from_uri($uri, $cc, $name);
	    }

	    # maybe a straight-up changer URI?
	    if (_uri_to_pkgname($tapedev)) {
		return _new_from_uri($tapedev, undef, $name);
	    }

	    # assume it's a device name or alias, and invoke the single-changer
	    return _new_from_uri("chg-single:$tapedev", undef, $name);
	} else {
	    die "Must specify one of 'tapedev' or 'tpchanger'";
	}
    }
}

# helper functions for new

sub _changer_alias_to_uri {
    my ($name) = @_;

    my $cc = Amanda::Config::lookup_changer_config($name);
    if ($cc) {
	my $tpchanger = changer_config_getconf($cc, $CHANGER_CONFIG_TPCHANGER);
	if (my $uri = _old_script_to_uri($tpchanger)) {
	    return ($uri, $cc);
	} elsif (_uri_to_pkgname($tpchanger)) {
	    return ($tpchanger, $cc);
	} else {
	    die "Changer '$name' specifies invalid tpchanger '$tpchanger'";
	}
    }

    # not an alias
    return;
}

sub _old_script_to_uri {
    my ($name) = @_;

    if ((-x "$amlibexecdir/$name") or (($name =~ qr{^/}) and (-x $name))) {
	return "chg-compat:$name"
    }

    # not an old script
    return;
}

# try to load the package for the given URI.  $@ is set properly
# if this function returns a false value.
sub _uri_to_pkgname {
    my ($name) = @_;

    my ($type) = ($name =~ /^chg-([A-Za-z_]+):/);
    if (!defined $type) {
	$@ = "'$name' is not a changer URI";
	return 0;
    }

    $type =~ tr/A-Z-/a-z_/;

    # create a package name to see if it's already imported
    my $pkgname = "Amanda::Changer::$type";
    my $filename = $pkgname;
    $filename =~ s|::|/|g;
    $filename .= '.pm';
    return $pkgname if (exists $INC{$filename});

    # try loading it
    eval "use $pkgname;";
    if ($@) {
        my $err = $@;

        # determine whether the module doesn't exist at all, or if there was an
        # error loading it; die if we found a syntax error
        if (exists $INC{$filename}) {
            die($err);
        }

        return 0;
    }

    return $pkgname;
}

# already-instsantiated changer objects (using 'our' so that the installcheck
# and reset this list as necessary)
our %changers_by_uri_cc = ();

sub _new_from_uri { # (note: this sub is patched by the installcheck)
    my ($uri, $cc, $name) = @_;

    # make up a key for our hash of already-instantiated objects,
    # using a newline as a separator, since perl can't use tuples
    # as keys
    my $uri_cc = "$uri\n";
    if (defined $cc) {
	$uri_cc = $uri_cc . changer_config_name($cc);
    }

    # return a pre-existing changer, if possible

    if (exists($changers_by_uri_cc{$uri_cc})) {
	return $changers_by_uri_cc{$uri_cc};
    }

    # look up the type and load the class
    my $pkgname = _uri_to_pkgname($uri);
    if (!$pkgname) {
	die $@;
    }

    my $rv = $pkgname->new($cc, $uri);
    die "$pkgname->new did not return an Amanda::Changer object"
	unless ($rv->isa("Amanda::Changer"));

    # store this in our cache for next time
    $changers_by_uri_cc{$uri_cc} = $rv;

    return $rv;
}

# parent-class methods; mostly "unimplemented method"

sub load {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    $params{'res_cb'}->("$class does not support load()", undef);
}

sub reset {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->("$class does not support reset()");
    }
}

sub info {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'info_cb'}) {
	$params{'info_cb'}->("$class does not support info()");
    }
}

sub clean {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->("$class does not support clean()");
    }
}

sub eject {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->("$class does not support eject()");
    }
}

sub update {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->("$class does not support update()");
    }
}

sub import {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->("$class does not support import()");
    }
}

sub export {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->("$class does not support export()");
    }
}

sub move {
    my $self = shift;
    my %params = @_;

    my $class = ref($self);
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->("$class does not support move()");
    }
}

package Amanda::Changer::Reservation;

# this is a simple base class with stub method or two.

sub new {
    my $class = shift;
    my $self = {
	released => 0,
    };
    return bless ($self, $class)
}

sub DESTROY {
    my ($self) = @_;
    if (!$self->{'released'}) {
	$self->release(finished_cb => sub {
	    my ($err) = @_;
	    if (defined $err) {
		warn "While releasing reservation: $err";
	    }
	});
    }
}

sub set_label {
    my $self = shift;
    my %params = @_;

    # nothing to do: just call the finished callback
    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
    }
}

sub release {
    my $self = shift;
    my %params = @_;

    return if $self->{'released'};

    $self->{'released'} = 1;
    $self->do_release(%params);
}

sub do_release {
    my $self = shift;
    my %params = @_;

    # this is the one subclasses should override

    if (exists $params{'finished_cb'}) {
	Amanda::MainLoop::call_later($params{'finished_cb'}, undef);
    }
}

1;
