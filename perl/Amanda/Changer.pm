# Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Changer;

use strict;
use warnings;
use Carp;
use POSIX ();
use Fcntl qw( O_RDWR O_CREAT LOCK_EX LOCK_NB );
use Data::Dumper;
use vars qw( @ISA );

use Amanda::Paths;
use Amanda::Util;
use Amanda::Config qw( :getconf string_to_boolean );
use Amanda::Device qw( :constants );
use Amanda::Debug qw( debug );

=head1 NAME

Amanda::Changer -- interface to changer scripts

=head1 SYNOPSIS

    use Amanda::Changer;

    my $chg = Amanda::Changer->new(); # loads the default changer; OR
    $chg = Amanda::Changer->new("somechanger"); # references a defined changer in amanda.conf

    $chg->load(
	label => "TAPE-012",
	mode => "write",
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

=head1 INTERFACE

All operations in the module return immediately, and take as an argument a
callback function which will indicate completion of the changer operation -- a
kind of continuation.  The caller should run a main loop (see
L<Amanda::MainLoop>) to allow the interactions with the changer script to
continue.

A new object is created with the C<new> function as follows:

  my $chg = Amanda::Changer->new($changer_name);

to create a named changer (a name provided by the user, either specifying a
changer directly or specifying a changer definition), or

  my $chg = Amanda::Changer->new();

to run the default changer.  This function handles the many ways a user can
configure a changer.

If there is a problem creating the new object, then the resulting object will
be a fatal C<Error> object (described below).  Thus the usual recipe for
creating a new changer is

  my $chg = Amanda::Changer->new($changer_name);
  if ($chg->isa("Amanda::Changer::Error")) {
    die("Error creating changer $changer_name: $chg");
  }

=head2 CALLBACKS

All changer callbacks take an error object as the first parameter.  If no error
occurred, then this parameter is C<undef> and the remaining parameters are
defined.

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

Other callback types are defined below.

=head2 ERRORS

When a callback is made with an error, it is an object of type
C<Amanda::Changer::Error>.  When interpolated into a string, this object turns
into a simple error message.  However, it has some additional methods that can
be used to determine how to respond to the error.  First, the error message is
available explicitly as C<< $err->message >>.  The error type is available as
C<< $err->{'type'} >>, although checks for particular error types should use
the C<TYPE> methods instead, as perl is better able to detect typos with this
syntax:

  if ($err->failed) { ... }

The error types are:

  fatal      Changer is no longer useable
  failed     Operation failed, but the changer is OK

The API may add other error types in the future (for example, to indicate
that a required resource is already reserved).

Errors of the type C<fatal> indicate that the changer should not be used any
longer, and in most cases the caller should terminate abnormally.  For example,
configuration or hardware errors are generally fatal.

If an operation fails, but the changer remains viable, then the error type is
C<failed>.  The reason for the failure is usually clear to the user from the
message, but for callers who may need to distinguish, C<< $err->{'reason'} >>
has one of the following values:

  notfound          The requested volume was not found
  invalid           The caller's request was invalid (e.g., bad slot)
  notimpl           The requested operation is not supported
  inuse             A required resource is already in use
  unknown           Unknown reason

Like types, checks for particular reasons should use the methods, to avoid
undetected typos:

  if ($err->failed and $err->notimpl) { ... }

Other reasons may be added in the future, so a caller should check for the
reasons it expects, and treat any other failures as of unknown cause.

When the desired slot cannot be loaded because it is already in use, the
C<inuse> error comes with an extra parameter, C<slot>, giving the slot in
question.  This parameter is not defined for other cases, such as no available
drives.

=head2 CURRENT SLOT

Changers maintain a global concept of a "current" slot, for compatibility with
Amanda algorithms such as the taperscan.  However, it is not compatible with
concurrent use of the same changer, and may be inefficient for some changers,
so new algorithms should avoid using it, preferring instead to load the correct
tape immediately (with C<load>), and to progress from tape to tape using the
C<relative_slot> parameter to C<load>.

=head2 CHANGER OBJECTS

=head3 load

The most common operation with a tape changer is to load a volume.  The C<load>
method is heavily overloaded to support a number of different ways to specify a
volume.

In general, the method takes a C<res_cb> giving a callback that will receive
the reservation.  If set_current is specified and true, then the changer's
current slot should be updated to correspond to C<$slot>. If not, then the changer
should not update its current slot (but some changers will anyway -
specifically, chg-compat).

The optional C<mode> describes the intended use of the volume by the caller,
and should be one of C<"read"> (the default) or C<"write">.  Changers managing
WORM media may use this parameter to provide a fresh volume for writing, but to
search for already-written volumes when reading.

The load method has a number of permutations:

  $chg->load(res_cb => $cb,
	     label => $label,
	     mode => $mode,
	     set_current => $sc)

Load and reserve a volume with the given label. This may leverage any barcodes
or other indices that the changer has available.

Note that the changer I<tries> to load the requested volume, but it's a mean
world out there, and you may not get what you want, so check the label on the
loaded volume before getting started.

  $chg->load(res_cb => $cb,
	     slot => $slot,
	     mode => $mode,
	     set_current => $sc)

Load and reserve the volume in the given slot. C<$slot> is a string specifying the slot
to load, provided by the user or from some other invocation of this changer.
Note that slots are not necessarily numeric, so performing arithmetic on this
value is an error.

If the slot does not exist, C<res_cb> will be called with a C<notfound> error.
Empty slots are considered empty.

  $chg->load(res_cb => $cb,
	     relative_slot => "current",
	     mode => $mode)

Reserve the volume in the "current" slot. This is used by the traditional
taperscan algorithm to begin its search.

  $chg->load(res_cb => $cb,
	     relative_slot => "next",
	     slot => $slot,
	     except_slots => { %except_slots },
	     mode => $mode,
	     set_current => $sc)

Reserve the volume that follows the given slot or, if C<slot> is omitted, the
volume that follows the current slot.  This will skip empty slots as if they
were not present in the changer.

The optional C<except_slots> argument specifies a hash of slots that should
I<not> be loaded.  Keys are slot names, and the hash values are ignored.  This
is useful as a termination condition when scanning all of the slots in a
changer: keep a hash of all slots already loaded, and pass that hash in
C<except_slots>.  When the load operation returns a C<notfound> error, the scan
is complete.

=head3 info

  $chg->info(info_cb => $cb,
	     info => [ $key1, $key2, .. ])

Query the changer for miscellaneous information.  Any number of keys may be
specified.  The C<info_cb> is called with C<$error> as the first argument,
much like a C<res_cb>, but the remaining arguments form a hash giving values
for all of the requested keys that are supported by the changer.  The preamble
to such a callback is usually

  info_cb => sub {
    my ($error, %results) = @_;
    # ..
  }

Supported keys are:

=over 2

=item num_slots

The total number of slots in the changer device.  If this key is not present or
-1, then the device cannot determine its slot count (for example, an archival
device that names slots by timestamp could potentially run until the heat-death
of the universe).

=item vendor_string

A string describing the name and model of the changer device.

=item fast_search

If true, then this changer implements searching (loading by label) with
something more efficient than a sequential scan through the volumes.  This
information affects some taperscan algorithms and recovery programs, which may
choose to do their own manual scan instead of invoking many potentially slow
searches.

=back

=head3 reset

  $chg->reset(finished_cb => $cb)

Reset the changer to a "base" state. This will generally reset the "current"
slot to something the user would think of as the "first" tape, unload any
loaded drives, etc. It is an error to call this while any reservations are
outstanding.

=head3 clean

  $chg->clean(finished_cb => $cb,
	      drive => $drivename)

Clean a drive, if the changer supports it. Drivename can be omitted for devices
with only one drive, or can be an arbitrary string from the user (e.g., an
amtape argument). Note that some changers cannot detect the completion of a
cleaning cycle; in this case, the user will just need to delay further Amanda
activities until the cleaning is complete.

=head3 eject

  $chg->eject(finished_cb => $cb,
	      drive => $drivename)

Eject the volume in a drive, if the changer supports it.  Drivename is as
specified to C<clean>.  If possible, applications should prefer to eject a
reserved volume when finished with it (C<< $res->release(eject => 1) >>), to
ensure that the correct volume is ejected from a multi-drive changer.

=head3 update

  $chg->update(finished_cb => $cb,
	       user_msg_fn => $fn,
	       changed => $changed)

The user has changed something -- loading or unloading tapes, reconfiguring the
changer, etc. -- that may have invalidated the database.  C<$changed> is a
changer-specific string indicating what has changed; if it is omitted, the
changer will check everything.

Since updates can take a long time, and users often want to know what's going
on, the update method will call C<user_msg_fn>, if specified, with
user-oriented messages appropriate to the changer.

=head3 inventory

  $chg->inventory(inventory_cb => $cb)

The C<inventory_cb> is called with an error object as the first parameter, or
C<undef> if no error occurs.  The second parameter is an arrayref containing an
ordered list of information about the slots in the changer.  The order will
make some sense to the user, but may change from one invocation to another.
Note that not all changers support the C<inventory> method, and those that do
not will return a C<'notimpl'> failure.

Each slot is represented by a hash with the following keys:

=over 4

=item slot

The slot name

=item empty

Set to C<1> if the slot is empty -- meaning no media is available in the slot.
A blank or erased volume is not the same as an empty slot.

=item label

The label on the volume in this slot, or undef if the label is unknown.  If the
volume is known to be blank, then this field should contain an empty string.

=item barcode (optional)

The barcode for the volume in this slot, if barcodes are available.

=item reserved

Set to C<1> if this slot is reserved, either by this process or another
process.  This is only set for I<exclusive> reservations, meaning that loading
the slot would result in an C<inuse> error.  Devices which can support
concurrent access will never set this flag.

=item loaded_in (optional)

For changers which have distinct user-visible drives, this gives the drive
currently accessing the volume in this slot.

=item import_export (optional)

Set to C<1> if this is an import-export slot -- a slot in which the user can
easily add or remove volumes.  This information may be useful for operations to
bulk-import newly-inserted tapes or bulk-export a set of tapes.

=back

=head3 move

  $chg->move(finished_cb => $cb,
	     from_slot => $from,
	     to_slot => $to)

Move a volume between two slots in the changer. These slots are provided by the
user, and have meaning for the changer.

=head2 RESERVATION OBJECTS

=head3 $res->{'device'}

This is the fully configured device for the reserved volume.  The device is not
started.

=head3 $res->{'this_slot'}

This is the name of this slot.  It is an arbitrary string which will
have some meaning to the changer's C<load()> method. It is safe to
access this field after the reservation has been released.

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

=head1 SUBCLASS HELPERS

C<Amanda::Changer> implements some methods and attributes to help subclass
implementers.

=head2 INFO

Implementing the C<info> method can be tricky, because it can potentially request
a number of keys that require asynchronous access.  The C<info> implementation in
this class may make the process a bit easier.

First, if the method C<info_setup> is defined, C<info> calls it, passing it a
C<finished_cb> and the list of desired keys, C<info>.  This method is useful to
gather information that is useful for several info keys.

Next, for each requested key, C<info> calls C<< $self->info_key($key, %params)
>>, including a regular C<info_cb> callback.  The C<info> method will wait for
all C<info_key> invocations to finish, then collect the results or errors that
occur.

=head2 PROPERTY PARSING

Many properties are boolean, and Amanda has a habit of accepting a number of
different ways of writing boolean values.  The method C<<
$self->get_boolean_property($config, $prop, $default) >> will parse such a
property, returning 0 or 1 if the property is specified, C<$default> if it is
not specified, or C<undef> if the property cannot be parsed.

=head2 ERROR HANDLING

To create a new error object, use C<< $self->make_error($type, $cb, %args) >>.
This method will create a new C<Amanda::Changer::Error> object and optionally
invoke a callback with it.  If C<$type> is C<fatal>, then C<<
$chg->{'fatal_error'} >> is made a reference to the new error object.  The
callback C<$cb> (which should be made using C<make_cb()> from
C<Amanda::MainLoop>) is called with the new error object.  The C<%args> are
added to the new error object.  In use, this looks something like:

  if (!$success) {
    return $self->make_error("failed", $params{'res_cb'},
	    reason => "notfound",
	    message => "Volume '$label' not found");
  }

This method can also be called as a class method, e.g., from a constructor.
In this case, it returns the resulting error object, which should be fatal.

  if (!$config_ok) {
    return Amanda::Changer->make_error("fatal", undef,
	    message => "config error");
  }

For cases where a number of errors have occurred, it is helpful to make a
"combined" error.  The method C<make_combined_error> takes care of this
operation, given a callback and an array of tuples C<[ $description, $err ]>
for each error.  This method uses some heuristics to figure out the
appropriate type and reason for the combined error.

  if ($left_err and $right_err) {
    return $self->make_combined_error($params{'finished_cb'},
	[ [ "from the left", $left_err ],
	  [ "from the right", $right_err ] ]);
  }

Any additional keyword arguments to C<make_combined_error> are put into the
combined error; this is useful to set the C<slot> attribute.

The method C<< $self->check_error($cb) >> is a useful method for subclasses to
avoid doing anything after a fatal error.  This method checks C<<
$self->{'fatal_error'} >>.  If the error is defined, the method calls C<$cb>
and returns true.  The usual recipe is

  sub load {
    my $self = shift;
    my %params = @_;

    return if $self->check_error($params{'res_cb'});
    # ...
  }

=head2 CONFIG

C<Amanda::Changer->new> calls subclass constructors with two parameters: a
configuration object and a changer specification.  The changer specification is
the string that led to creation of this changer device.  The configuration
object is of type C<Amanda::Changer::Config>, and can be treated as a hashref
with the following keys:

  name                  -- name of the changer section (or "default")
  is_global             -- true if this changer is the default changer
  tapedev               -- tapedev parameter
  tpchanger             -- tpchanger parameter
  changerdev            -- changerdev parameter
  changerfile           -- changerfile parameter
  properties            -- all properties for this changer
  device_properties     -- device properties from this changer

The four parameters are just as supplied by the user, either in the global
config or in a changer section.  Changer authors are cautioned not to try to
override any of these parameters as previous changers have done (e.g.,
C<changerfile> specifying both configuration and state files).  Use properties
instead.

The C<properties> and C<device_properties> parameters are in the format
provided by C<Amanda::Config>.  If C<is_global> is true, then
C<device_properties> will include any device properties specified globally, as
well as properties culled from the global tapetype.

The C<configure_device> method generally takes care of the intricacies of
handling device properties.  Pass it a newly opened device and it will apply
the relevant properties, returning undef on success or an error message on
failure.

The C<get_property> method is a shortcut method to get the value of a changer
property, ignoring its the priority and other attributes.  In a list context,
it returns all values for the property; in a scalar context, it returns the
first value specified.

=head2 PERSISTENT STATE AND LOCKING

Many changer subclasses need to track state across invocations and between
different processes, and to ensure that the state is read and written
atomically.  The C<with_locked_state> provides this functionality by
locking a statefile, only unlocking it after any changes have been written back
to it.  Subclasses can use this method both for mutual exclusion (ensuring that
only one changer operation is in progress at any time) and for atomic state
storage.

The C<with_locked_state> method works like C<synchronized> (in
L<Amanda::MainLoop>), but with some extra arguments:

  $self->with_locked_state($filename, $some_cb, sub {
    # note: $some_cb shadows outer $some_cb; see Amanda::MainLoop::synchronized
    my ($state, $some_cb) = @_;
    # ... and eventually:
    $some_cb->(...);
  });

The callback C<$some_cb> is assumed to take a changer error as its first
argument, and if there are any errors locking the statefile, they will be
reported directly to this callback.  Otherwise, a wrapped version of
C<$some_cb> is passed to the inner C<sub>.  When this wrapper is invoked, the
state will be written to disk and unlocked before the original callback is
invoked.

The state itself begins as an empty hashref, but subclasses can add arbitrary
keys to the hash.  Serialization is currently handled with L<Data::Dumper>.

=head2 PARAMETER VALIDATION

The C<validate_params> method is useful to make sure that the proper parameters
are present for a particular method, dying if not.  Call it like this:

  $self->validate_params("load", \%params);

The method currently only supports the "load" method, but can be expanded to
cover other methods.

=head1 SEE ALSO

The Amanda Wiki (http://wiki.zmanda.com) has a higher-level description of the
changer model implemented by this package.

See amanda-changers(7) for user-level documentation of the changer implementations.

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
	if (getconf_seen($CNF_TPCHANGER) and getconf($CNF_TPCHANGER) ne '') {
	    my $tpchanger = getconf($CNF_TPCHANGER);

	    # first, is it an old changer script?
	    if ($uri = _old_script_to_uri($tpchanger)) {
		return _new_from_uri($uri, undef, $name);
	    }

	    # if not, then there had better be no tapdev
	    if (getconf_seen($CNF_TAPEDEV) and getconf($CNF_TAPEDEV) ne '') {
		return Amanda::Changer::Error->new('fatal',
		    message => "Cannot specify both 'tapedev' and 'tpchanger' " .
			"unless using an old-style changer script");
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
	} elsif (getconf_seen($CNF_TAPEDEV) and getconf($CNF_TAPEDEV) ne '') {
	    my $tapedev = getconf($CNF_TAPEDEV);

	    # first, is it a changer alias?
	    if (($uri,$cc) = _changer_alias_to_uri($tapedev)) {
		return _new_from_uri($uri, $cc, $name);
	    }

	    # maybe a straight-up changer URI?
	    if (_uri_to_pkgname($tapedev)) {
		return _new_from_uri($tapedev, undef, $name);
	    }

	    # assume it's a device name or alias, and invoke chg-single.
	    # chg-single will check the device immediately and error out
	    # if the device name is invalid.
	    return _new_from_uri("chg-single:$tapedev", undef, $name);
	} else {
	    return Amanda::Changer::Error->new('fatal',
		message => "You must specify one of 'tapedev' or 'tpchanger'");
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

    my $rv = $pkgname->new(Amanda::Changer::Config->new($cc), $uri);
    die "$pkgname->new did not return an Amanda::Changer object or an Amanda::Changer::Error"
	unless ($rv->isa("Amanda::Changer") or $rv->isa("Amanda::Changer::Error"));

    if ($rv->isa("Amanda::Changer")) {
	# add an instance variable or two
	$rv->{'fatal_error'} = undef;

	# store this in our cache for next time
	$changers_by_uri_cc{$uri_cc} = $rv;
    }

    return $rv;
}

# method stubs that return a "notimpl" error

sub _stubop {
    my ($op, $cbname, $self, %params) = @_;
    return if $self->check_error($params{$cbname});

    my $class = ref($self);
    my $chg_foo = "chg-" . ($class =~ /Amanda::Changer::(.*)/)[0];
    return $self->make_error("failed", $params{$cbname},
	reason => "notimpl",
	message => "'$chg_foo:' does not support $op");
}

sub load { _stubop("loading volumes", "res_cb", @_); }
sub reset { _stubop("reset", "finished_cb", @_); }
sub clean { _stubop("clean", "finished_cb", @_); }
sub eject { _stubop("eject", "finished_cb", @_); }
sub update { _stubop("update", "finished_cb", @_); }
sub inventory { _stubop("inventory", "inventory_cb", @_); }
sub move { _stubop("move", "finished_cb", @_); }

# info calls out to info_setup and info_key; see POD above
sub info {
    my $self = shift;
    my %params = @_;

    if (!$self->can('info_key')) {
	my $class = ref($self);
	$params{'info_cb'}->("$class does not support info()");
	return;
    }

    my ($do_setup, $start_keys, $all_done);

    $do_setup = sub {
	if ($self->can('info_setup')) {
	    $self->info_setup(info => $params{'info'},
			      finished_cb => sub {
		my ($err) = @_;
		if ($err) {
		    $params{'info_cb'}->($err);
		} else {
		    $start_keys->();
		}
	    });
	} else {
	    $start_keys->();
	}
    };

    $start_keys = sub {
	my $remaining_keys = 1;
	my %key_results;

	my $maybe_done = sub {
	    return if (--$remaining_keys);
	    $all_done->(%key_results);
	};

	for my $key (@{$params{'info'}}) {
	    $remaining_keys++;
	    $self->info_key($key, info_cb => sub {
		$key_results{$key} = [ @_ ];
		$maybe_done->();
	    });
	}

	# we started with $remaining_keys = 1, so decrement it now
	$maybe_done->();
    };

    $all_done = sub {
	my %key_results = @_;

	# if there are *any* errors, handle them
	my @annotated_errs =
	    map { [ sprintf("While getting info key '%s'", $_), $key_results{$_}->[0] ] }
	    grep { defined($key_results{$_}->[0]) }
	    keys %key_results;

	if (@annotated_errs) {
	    return $self->make_combined_error(
		$params{'info_cb'}, [ @annotated_errs ]);
	}

	# no errors, so combine the results and return them
	my %info;
	while (my ($key, $result) = each(%key_results)) {
	    my ($err, %key_info) = @$result;
	    if (exists $key_info{$key}) {
		$info{$key} = $key_info{$key};
	    } else {
		warn("No value available for $key");
	    }
	}

	$params{'info_cb'}->(undef, %info);
    };

    $do_setup->();
}

# subclass helpers

sub get_boolean_property {
    my ($self) = shift;
    my ($config, $propname, $default) = @_;

    return $default
	unless (exists $config->{'properties'}->{$propname});

    my $propinfo = $config->{'properties'}->{$propname};
    return undef unless @{$propinfo->{'values'}} == 1;
    return string_to_boolean($propinfo->{'values'}->[0]);
}

sub make_error {
    my $self = shift;
    my ($type, $cb, %args) = @_;

    my $classmeth = $self eq "Amanda::Changer";

    if ($classmeth and $type ne 'fatal') {
	cluck("type must be fatal when calling make_error as a class method");
	$type = 'fatal';
    }

    my $err = Amanda::Changer::Error->new($type, %args);

    if (!$classmeth) {
	$self->{'fatal_error'} = $err
	    if ($err->fatal);

	$cb->($err);
    }

    return $err;
}

sub make_combined_error {
    my $self = shift;
    my ($cb, $suberrors, %extra_args) = @_;
    my $err;

    if (@$suberrors == 0) {
	die("make_combined_error called with no errors");
    }

    my $classmeth = $self eq "Amanda::Changer";

    # if there's only one suberror, just use it directly
    if (@$suberrors == 1) {
	$err = $suberrors->[0][1];
	die("$err is not an Error object")
	    unless defined($err) and $err->isa("Amanda::Changer::Error");

	$err = Amanda::Changer::Error->new(
	    $err->{'type'},
	    reason => $err->{'reason'},
	    message => $suberrors->[0][0] . ": " . $err->{'message'});
    } else {
	my $fatal = $classmeth or grep { $err->[1]{'fatal'} } @$suberrors;

	my $reason;
	if (!$fatal) {
	    my %reasons =
		map { ($_->[1]{'reason'}, undef) }
		grep { $_->[1]{'reason'} }
		@$suberrors;
	    if ((keys %reasons) == 1) {
		$reason = (keys %reasons)[0];
	    } else {
		$reason = 'unknown'; # multiple or 0 "source" reasons
	    }
	}

	my $message = join("; ",
	    map { sprintf("%s: %s", @$_) }
	    @$suberrors);

	my %errargs = ( message => $message, %extra_args );
	$errargs{'reason'} = $reason unless ($fatal);
	$err = Amanda::Changer::Error->new(
	    $fatal? "fatal" : "failed",
	    %errargs);
    }

    if (!$classmeth) {
	$self->{'fatal_error'} = $err
	    if ($err->fatal);

	$cb->($err) if $cb;
    }

    return $err;
}

sub check_error {
    my $self = shift;
    my ($cb) = @_;

    if (defined $self->{'fatal_error'}) {
	$cb->($self->{'fatal_error'}) if $cb;
	return 1;
    }
}

sub lock_statefile {
    my $self = shift;
    my %params = @_;

    my $statefile = $params{'statefile_filename'};
    my $lock_cb = $params{'lock_cb'};
    Amanda::Changer::StateFile->new($statefile, $lock_cb);
}

sub with_locked_state {
    my $self = shift;
    my ($statefile, $cb, $sub) = @_;
    my %subs;
    my ($filelock, $STATE);
    my $poll = 0; # first delay will be 0.1s; see below

    $subs{'open'} = sub {
	$filelock = Amanda::Util::file_lock->new($statefile);

	$subs{'lock'}->();
    };

    $subs{'lock'} = sub {
	my $rv = $filelock->lock();
	if ($rv == 1) {
	    # loop until we get the lock, increasing $poll to 10s
	    $poll += 100 unless $poll >= 10000;
	    return Amanda::MainLoop::call_after($poll, $subs{'lock'});
	} elsif ($rv == -1) {
	    return $self->make_error("fatal", $cb,
		    message => "Error locking '$statefile'");
	}

	$subs{'read'}->();
    };

    $subs{'read'} = sub {
	my $contents = $filelock->data();
	if ($contents) {
	    eval $contents;
	    if ($@) {
		# $fh goes out of scope here, and is thus automatically
		# unlocked
		return $cb->("error reading '$statefile': $@", undef);
	    }
	    if (!defined $STATE or ref($STATE) ne 'HASH') {
		return $cb->("'$statefile' did not define \$STATE properly", undef);
	    }
	} else {
	    # initial state (blank file)
	    $STATE = {};
	}

	$sub->($STATE, $subs{'cb_wrap'});
    };

    $subs{'cb_wrap'} =  sub {
	my @args = @_;

	my $dumper = Data::Dumper->new([ $STATE ], ["STATE"]);
	$dumper->Purity(1);
	$filelock->write($dumper->Dump);
	$filelock->unlock();

	# call through to the original callback with the original
	# arguments
	$cb->(@args);
    };

    $subs{'open'}->();
}

sub validate_params {
    my ($self, $op, $params) = @_;

    if ($op eq 'load') {
        unless(exists $params->{'label'} || exists $params->{'slot'} ||
               exists $params->{'relative_slot'}) {
		confess "Invalid parameters to 'load'";
        }
    } else {
        confess "don't know how to validate '$op'";
    }
}

package Amanda::Changer::Error;
use Amanda::Debug qw( :logging );
use Carp qw( cluck );
use Amanda::Debug;
use overload
    '""' => sub { $_[0]->{'message'}; },
    'cmp' => sub { $_[0]->{'message'} cmp $_[1]; };

my %known_err_types = map { ($_, 1) } qw( fatal failed );
my %known_err_reasons = map { ($_, 1) } qw( notfound invalid notimpl inuse unknown device );

sub new {
    my $class = shift; # ignore class
    my ($type, %info) = @_;

    debug("new Amanda::Changer::Error: type='$type', message='$info{message}'");

    $info{'type'} = $type;

    # do some sanity checks.  Note that these sanity checks issue a warning
    # with cluck, but add default values to the error.  This is in the hope
    # that an unusual Amanda error is not obscured by a problem in the
    # make_error invocation.  The stack trace produced by cluck should help to
    # track down the bad make_error invocation.

    if (!exists $info{'message'}) {
	cluck("no message given to A::C::make_error");
	$info{'message'} = "unknown error";
    }

    if (!exists $known_err_types{$type}) {
	cluck("invalid Amanda::Changer::Error type '$type'");
	$type = 'fatal';
    }

    if ($type eq 'failed' and !exists $info{'reason'}) {
	cluck("no reason given to A::C::make_error");
	$info{'reason'} = "unknown";
    }

    if ($type eq 'failed' and !exists $known_err_reasons{$info{'reason'}}) {
	cluck("invalid Amanda::Changer::Error reason '$info{reason}'");
	$info{'reason'} = 'unknown';
    }

    return bless (\%info, $class);
}

# types
sub fatal { $_[0]->{'type'} eq 'fatal'; }
sub failed { $_[0]->{'type'} eq 'failed'; }

# reasons
sub notfound { $_[0]->{'reason'} eq 'notfound'; }
sub invalid { $_[0]->{'reason'} eq 'invalid'; }
sub notimpl { $_[0]->{'reason'} eq 'notimpl'; }
sub inuse { $_[0]->{'reason'} eq 'inuse'; }
sub unknown { $_[0]->{'reason'} eq 'unknown'; }

# slot accessor
sub slot { $_[0]->{'slot'}; }

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
	Amanda::Debug::warning("Changer reservation for slot '$self->{this_slot}' has " .
			       "gone out of scope without release");
    }
}

sub set_label {
    my $self = shift;
    my %params = @_;

    # nothing to do by default: just call the finished callback
    if (exists $params{'finished_cb'}) {
	$params{'finished_cb'}->(undef) if $params{'finished_cb'};
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
	$params{'finished_cb'}->(undef) if $params{'finished_cb'};
    }
}

package Amanda::Changer::Config;
use Amanda::Config qw( :getconf );
use Amanda::Device;

sub new {
    my $class = shift;
    my ($cc) = @_;

    my $self = bless {}, $class;

    if (defined $cc) {
	$self->{'name'} = changer_config_name($cc);
	$self->{'is_global'} = 0;

	$self->{'tapedev'} = changer_config_getconf($cc, $CHANGER_CONFIG_TAPEDEV);
	$self->{'tpchanger'} = changer_config_getconf($cc, $CHANGER_CONFIG_TPCHANGER);
	$self->{'changerdev'} = changer_config_getconf($cc, $CHANGER_CONFIG_CHANGERDEV);
	$self->{'changerfile'} = changer_config_getconf($cc, $CHANGER_CONFIG_CHANGERFILE);

	$self->{'properties'} = changer_config_getconf($cc, $CHANGER_CONFIG_PROPERTY);
	$self->{'device_properties'} = changer_config_getconf($cc, $CHANGER_CONFIG_DEVICE_PROPERTY);
    } else {
	$self->{'name'} = "default";
	$self->{'is_global'} = 1;

	$self->{'tapedev'} = getconf($CNF_TAPEDEV);
	$self->{'tpchanger'} = getconf($CNF_TPCHANGER);
	$self->{'changerdev'} = getconf($CNF_CHANGERDEV);
	$self->{'changerfile'} = getconf($CNF_CHANGERFILE);

	# no changer or device properties, since there's no changer definition to use
	$self->{'properties'} = {};
	$self->{'device_properties'} = {};
    }
    return $self;
}

sub configure_device {
    my $self = shift;
    my ($device) = @_;

    # we'll accumulate properties in this hash *overwriting* previous properties
    # instead of appending to them
    my %properties;

    # always use implicit properties
    %properties = ( %properties, %{ $self->_get_implicit_properties() } );

    # always use global properties
    %properties = ( %properties, %{ getconf($CNF_DEVICE_PROPERTY) } );

    # if this is a device alias, add properties from its device definition
    if (my $dc = lookup_device_config($device->device_name)) {
	%properties = ( %properties,
		%{ device_config_getconf($dc, $DEVICE_CONFIG_DEVICE_PROPERTY); } );
    }

    # finally, add any props from the changer config
    %properties = ( %properties, %{ $self->{'device_properties'} } );

    while (my ($propname, $propinfo) = each(%properties)) {
	for my $value (@{$propinfo->{'values'}}) {
	    if (!$device->property_set($propname, $value)) {
		my $msg = "Error setting '$propname' on device '".$device->device_name."'";
		if (exists $propinfo->{'optional'}) {
		    if ($propinfo->{'optional'} eq 'warn') {
			warn("$msg (ignored)");
		    }
		} else {
		    return $msg;
		}
	    }
	}
    }

    return undef;
}

sub get_property {
    my $self = shift;
    my ($property) = @_;

    my $prophash = $self->{'properties'}->{$property};
    return undef unless defined($prophash);

    return wantarray? @{$prophash->{'values'}} : $prophash->{'values'}->[0];
}

sub _get_implicit_properties {
    my $self = shift;
    my $props = {};

    my $tapetype_name = getconf($CNF_TAPETYPE);
    return unless defined($tapetype_name);

    my $tapetype = lookup_tapetype($tapetype_name);
    return unless defined($tapetype);

    # The property hashes used here add the 'optional' key, which indicates
    # that the property is implicit and that a failure to set it is not fatal.
    # The flag is used by configure_device.
    if (tapetype_seen($tapetype, $TAPETYPE_LENGTH)) {
	$props->{'max_volume_usage'} = {
	    optional => 1,
	    priority => 0,
	    append => 0,
	    values => [
		tapetype_getconf($tapetype, $TAPETYPE_LENGTH) * 1024,
	    ]};
    }

    if (tapetype_seen($tapetype, $TAPETYPE_READBLOCKSIZE)) {
	$props->{'read_block_size'} = {
	    optional => "warn", # optional, but give a warning
	    priority => 0,
	    append => 0,
	    values => [
		tapetype_getconf($tapetype, $TAPETYPE_READBLOCKSIZE) * 1024,
	    ]};
    }

    if (tapetype_seen($tapetype, $TAPETYPE_BLOCKSIZE)) {
	$props->{'block_size'} = {
	    optional => 0,
	    priority => 0,
	    append => 0,
	    values => [
		# convert the length from kb to bytes here
		tapetype_getconf($tapetype, $TAPETYPE_BLOCKSIZE) * 1024,
	    ]};
    }

    return $props;
}

1;
