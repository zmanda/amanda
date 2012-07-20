# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Taper::Scan;

=head1 NAME

Amanda::Taper::Scan

=head1 SYNOPSIS

This is an abstract base class for taperscan algorithms.

  # open the taperscan algorithm specified in the config
  my $taperscan = Amanda::Taperscan->new(
	changer => $changer);

  my $result_cb = make_cb(result_cb => sub {
    my ($err, $reservation, $label, $access_mode, $is_new) = @_;
    die $err if $err;
    # write to $reservation->{'device'}, using label $label, and opening
    # the device with $access_mode (one of $ACCESS_WRITE or $ACCESS_APPEND)
    # $is_new is set to 1 if the volume is not already labeled.
    # ..
  });
  my $user_msg_fn = sub {
    print "$_[0]\n";
  };
  $taperscan->scan(result_cb => $result_cb, user_msg_fn => $user_msg_fn);

  # later ..
  $taperscan->quit(); # also quit the changer

=head1 OVERVIEW

C<Amanda::Taper::Scan> subclasses represent algorithms used by
C<Amanda::Taper::Scribe> (see L<Amanda::Taper::Scribe>) to scan for and select
volumes for writing.

Call C<< Amanda::Taperscan->new() >> to create a new taperscan
algorithm.  The constructor takes the following keyword arguments:

    changer       Amanda::Changer object to use (required)
    algorithm     Taperscan algorithm to instantiate
    tapelist      Amanda::Tapelist
    tapecycle
    labelstr
    autolabel
    meta_autolabel

The changer object must always be provided, but C<algorithm> may be omitted, in
which case the class specified by the user in the Amanda configuration file is
instantiated.  The remaining options will be taken from the configuration file
if not specified.  Default values for all of these options are applied before a
subclass's constructor is called.

The autolabel option should look like the C<CNF_AUTOLABEL> hash - see
L<Amanda::Config>.

Subclasses must implement a single method: C<scan>.  It takes only one mandatory
parameter, C<result_cb>:

  $taperscan->scan(
    result_cb => $my_result_cb,
    user_msg_fn => $fn,
    );

If C<user_msg_fn> is specified, then it is called with user-oriented messages to
indicate the progress of the scan.

The C<result_cb> takes the following positional parameters:

  $error        an error message, or undef on success
  $reservation  Amanda::Changer::Reservation object
  $label        label to apply to the volume
  $access_mode  access mode with which to start the volume

The error message can be a simple string or an C<Amanda::Changer::Error> object
(see L<Amanda::Changer>).  The C<$label> and C<$access_mode> specify parameters
for starting the device contained in C<$reservation>.

To cleanly terminate an Amanda::Taper::Scan object:

  $taperscan->quit()

It also terminate the changer by caller $chg->quit().

=head1 SUBCLASS UTILITIES

There are a few common tasks for subclasses that are implemented as methods in
the parent class.  Note that this class assumes subclasses will be implemented
as blessed hashrefs, and sets keys corresponding to the constructor arguments.

To read the tapelist, call C<read_tapelist>.  This method caches the result in
C<< $self->{'tapelist'} >>, which will be used by the other functions here.  In
general, call C<read_tapelist> at most once per C<scan()> invocation.

To see if a volume is reusable, call the C<is_reusable_volume> method.  This takes
several keyword parameters:

    $self->is_reusable_volume(
        label => $label,         # label to check
        new_label_ok => $nlo,    # count newly labeled vols as reusable?
    );

Similarly, to calculate the oldest reusable volume, call
C<oldest_reusable_volume>:

    $self->oldest_reusable_volume(
        new_label_ok => $nlo,    # count newly labeled vols as reusable?
    );

=head2 user_msg_fn

This interface is temporary and will change in the next release.

Initiate a load by label:

  user_msg_fn(search_label => 1,
                   label        => $label);

The result of a load by label:

  user_msg_fn(search_result => 1,
                   res           => $res,
                   err           => $err);

Initiate the scan of the slot $slot:

  $self->user_msg_fn(scan_slot => 1,
                     slot      => $slot);

Initiate the scan of the slot $slot which should have the label $label:

  $self->user_msg_fn(scan_slot => 1,
                     slot      => $slot,
                     label     => $label);

The result of scanning slot $slot:

  $self->user_msg_fn(slot_result => 1,
                     slot        => $slot,
                     err         => $err,
                     res         => $res);

The result if the read label doesn't match the labelstr:

  user_msg_fn(slot_result             => 1,
                   does_not_match_labelstr => 1,
                   labelstr                => $labelstr,
                   slot                    => $slot,
                   res                     => $res);

The result if the read label is not in the tapelist:

  user_msg_fn(slot_result     => 1,
                   not_in_tapelist => 1,
                   slot            => $slot,
                   res             => $res);

The result if the read label can't be used because it is active:

  user_msg_fn(slot_result => 1,
                   active      => 1,
                   slot        => $slot,
                   res         => $res);

The result if the volume can't be labeled because autolabel is not set:

  user_msg_fn(slot_result => 1,
                   not_autolabel => 1,
                   slot          => $slot,
                   res           => $res);

The result if the volume is empty and can't be labeled because autolabel setting:

  user_msg_fn(slot_result => 1,
                   empty         => 1,
                   slot          => $slot,
                   res           => $res);

The result if the volume is a non-amanda volume and can't be labeled because autolabel setting:

  user_msg_fn(slot_result => 1,
                   non_amanda    => 1,
                   slot          => $slot,
                   res           => $res);

The result if the volume is in error and can't be labeled because autolabel setting:

  user_msg_fn(slot_result => 1,
                   volume_error  => 1,
		   err           => $err,
                   slot          => $slot,
                   res           => $res);

The result if the volume is in error and can't be labeled because autolabel setting:

  user_msg_fn(slot_result => 1,
                   not_success   => 1,
		   err           => $err,
                   slot          => $slot,
                   res           => $res);

The scan has failed, possibly with some additional information as to what the
scan was looking for.

  user_msg_fn(scan_failed => 1,
	      expected_label => $label, # optional
	      expected_new => 1); # optional

=cut

use strict;
use warnings;
use Amanda::Config qw( :getconf );
use Amanda::Tapelist;
use Amanda::Debug;

sub new {
    my $class = shift;
    my %params = @_;

    die "No changer given to Amanda::Taper::Scan->new"
	unless exists $params{'changer'};
    # fill in the optional parameters
    $params{'algorithm'} = "traditional"
	unless defined $params{'algorithm'} and $params{'algorithm'} ne '';
    $params{'tapecycle'} = getconf($CNF_TAPECYCLE)
	unless exists $params{'tapecycle'};
    $params{'labelstr'} = getconf($CNF_LABELSTR)
	unless exists $params{'labelstr'};
    $params{'autolabel'} = getconf($CNF_AUTOLABEL)
	unless exists $params{'autolabel'};
    $params{'meta_autolabel'} = getconf($CNF_META_AUTOLABEL)
	unless exists $params{'meta_autolabel'};

    my $plugin;
    if (!defined $params{'algorithm'} or $params{'algorithm'} eq '') {
	$params{'algorithm'} = "traditional";
	$plugin = "traditional";
    } else {
	my $taperscan = Amanda::Config::lookup_taperscan($params{'algorithm'});
	if ($taperscan) {
	    $plugin = Amanda::Config::taperscan_getconf($taperscan, $TAPERSCAN_PLUGIN);
	    $params{'properties'} = Amanda::Config::taperscan_getconf($taperscan, $TAPERSCAN_PROPERTY);
	} else {
	    $plugin = $params{'algorithm'};
	}
    }
    # load the package
    my $pkgname = "Amanda::Taper::Scan::" . $plugin;
    my $filename = $pkgname;
    $filename =~ s|::|/|g;
    $filename .= '.pm';
    if (!exists $INC{$filename}) {
	eval "use $pkgname;";
	if ($@) {
	    # handle compile errors
	    die($@) if (exists $INC{$filename});
	    die("No such taperscan algorithm '$plugin'");
	}
    }

    # instantiate it
    my $self = eval {$pkgname->new(%params);};
    if ($@ || !defined $self) {
	debug("Can't instantiate $pkgname");
	die("Can't instantiate $pkgname");
    }

    # and set the keys from the parameters
    $self->{'changer'} = $params{'changer'};
    $self->{'algorithm'} = $params{'algorithm'};
    $self->{'plugin'} = $params{'plugin'};
    $self->{'tapecycle'} = $params{'tapecycle'};
    $self->{'labelstr'} = $params{'labelstr'};
    $self->{'autolabel'} = $params{'autolabel'};
    $self->{'meta_autolabel'} = $params{'meta_autolabel'};
    $self->{'tapelist'} = $params{'tapelist'};

    return $self;
}

sub DESTROY {
    my $self = shift;

    die("Taper::Scan did not quit") if defined $self->{'changer'};
}

sub quit {
    my $self = shift;

    if (defined $self->{'chg'} && $self->{'chg'} != $self->{'initial_chg'}) {
	$self->{'chg'}->quit();
    }
    $self->{'changer'}->quit() if defined $self->{'changer'};
    foreach (keys %$self) {
        delete $self->{$_};
    }
}

sub scan {
    my $self = shift;
    my %params = @_;

    $params{'result_cb'}->("scan not implemented");
}

sub read_tapelist {
    my $self = shift;

    $self->{'tapelist'}->reload();
}

sub oldest_reusable_volume {
    my $self = shift;
    my %params = @_;

    my $best = undef;
    my $num_acceptable = 0;
    for my $tle (@{$self->{'tapelist'}->{'tles'}}) {
	next unless $tle->{'reuse'};
	next if $tle->{'datestamp'} eq '0' and !$params{'new_label_ok'};
	$num_acceptable++;
	$best = $tle;
    }

    # if we didn't find at least $tapecycle reusable tapes, then
    # there is no oldest reusable tape
    return undef unless $num_acceptable >= $self->{'tapecycle'};

    return $best->{'label'};
}

sub is_reusable_volume {
    my $self = shift;
    my %params = @_;

    my $vol_tle = $self->{'tapelist'}->lookup_tapelabel($params{'label'});
    return 0 unless $vol_tle;
    return 0 unless $vol_tle->{'reuse'};
    if ($vol_tle->{'datestamp'} eq '0') {
	return $params{'new_label_ok'};
    }

    # see if it's in the collection of reusable volumes
    my @tapelist = @{$self->{'tapelist'}->{'tles'}};
    my @reusable = @tapelist[$self->{'tapecycle'}-1 .. $#tapelist];
    for my $tle (@reusable) {
        return 1 if $tle eq $vol_tle;
    }

    return 0;
}

1;
