# Copyright (c) Zmanda, Inc.  All Rights Reserved.
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
    # ..
  });
  my $user_msg_fn = sub {
    print "$_[0]\n";
  };
  $taperscan->scan(result_cb => $result_cb, user_msg_fn => $user_msg_fn);

=head1 OVERVIEW

C<Amanda::Taper::Scan> subclasses represent algorithms used by
C<Amanda::Taper::Scribe> (see L<Amanda::Taper::Scribe>) to scan for and select
volumes for writing.

Call C<Amanda::Taperscan->new()> to create a new taperscan
algorithm.  The constructor takes the following keyword arguments:

    changer       Amanda::Changer object to use (required)
    algorithm     Taperscan algorithm to instantiate
    tapelist_filename
    tapecycle
    labelstr
    label_new_tapes

The changer object must always be provided, but C<algorithm> may be omitted, in
which case the class specified by the user in the Amanda configuration file is
instantiated.  The remaining options will be taken from the configuration file
if not specified.  Default values for all of these options are applied before a
subclass's constructor is called.

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

=head1 SUBCLASS UTILITIES

There are a few common tasks for subclasses that are implemented as methods in
the parent class.  Note that this class assumes subclasses will be implemented
as blessed hashrefs, and sets keys corresponding to the constructor arguments.

To read the tapelist, call C<read_tapelist>.  This method caches the result in
C<< $self->{'tapelist'} >>, which will be used by the other functions here.  In
general, call C<read_tapelist at most once per C<scan()> invocation.

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

Finally, to devise a new name for a volume, call C<make_new_tape_label>,
passing a tapelist, a labelstr, and a template.  This will return C<undef>
if no label could be created.

    $label = $self->make_new_tape_label(
	labelstr => "foo-[0-9]+",
        template => "foo-%%%%",
    );

If no C<template> is provided, the function uses the value of
C<label_new_tapes> specified when the object was constructed; similarly,
C<labelstr> defaults to the value specified at object construction.

=cut

use strict;
use warnings;
use Amanda::Config qw( :getconf );

sub new {
    my $class = shift;
    my %params = @_;

    die "No changer given to Amanda::Taper::Scan->new"
	unless exists $params{'changer'};

    # fill in the optional parameters
    $params{'algorithm'} = "traditional" # TODO: get from a configuration variable
	unless exists $params{'algorithm'};
    $params{'tapecycle'} = getconf($CNF_TAPECYCLE)
	unless exists $params{'tapecycle'};
    $params{'tapelist_filename'} =
	Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST))
	    unless exists $params{'tapelist_filename'};
    $params{'labelstr'} = getconf($CNF_LABELSTR)
	unless exists $params{'labelstr'};
    $params{'label_new_tapes'} = getconf($CNF_LABEL_NEW_TAPES)
	unless exists $params{'label_new_tapes'};

    # load the package
    my $pkgname = "Amanda::Taper::Scan::" . $params{'algorithm'};
    my $filename = $pkgname;
    $filename =~ s|::|/|g;
    $filename .= '.pm';
    if (!exists $INC{$filename}) {
	eval "use $pkgname;";
	if ($@) {
	    # handle compile errors
	    die($@) if (exists $INC{$filename});
	    die("No such taperscan algorithm '$params{algorithm}'");
	}
    }

    # instantiate it
    my $self = $pkgname->new(%params);

    # and set the keys from the parameters
    $self->{'changer'} = $params{'changer'};
    $self->{'algorithm'} = $params{'algorithm'};
    $self->{'tapecycle'} = $params{'tapecycle'};
    $self->{'tapelist_filename'} = $params{'tapelist_filename'};
    $self->{'labelstr'} = $params{'labelstr'};
    $self->{'label_new_tapes'} = $params{'label_new_tapes'};

    return $self;
}

sub scan {
    my $self = shift;
    my %params = @_;

    $params{'result_cb'}->("not implemented");
}

sub read_tapelist {
    my $self = shift;

    $self->{'tapelist'} = Amanda::Tapelist::read_tapelist($self->{'tapelist_filename'});
    return $self->{'tapelist'};
}

sub oldest_reusable_volume {
    my $self = shift;
    my %params = @_;

    my $best = undef;
    my $num_acceptable = 0;
    for my $tle (@{$self->{'tapelist'}}) {
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
    my @tapelist = @{$self->{'tapelist'}};
    my @reusable = @tapelist[$self->{'tapecycle'}-1 .. $#tapelist];
    for my $tle (@reusable) {
        return 1 if $tle eq $vol_tle;
    }


    return 0;
}

sub make_new_tape_label {
    my $self = shift;
    my %params = @_;
    my $template = exists $params{'template'}? $params{'template'} : $self->{'label_new_tapes'};
    my $labelstr = exists $params{'labelstr'}? $params{'labelstr'} : $self->{'labelstr'};

    (my $npercents =
	$template) =~ s/[^%]*(%+)[^%]*/length($1)/e;
    my $nlabels = 10 ** $npercents;

    # make up a sprintf pattern
    (my $sprintf_pat =
	$template) =~ s/(%+)/"%0" . length($1) . "d"/e;

    my %existing_labels =
	map { $_->{'label'} => 1 } @{$self->{'tapelist'}};

    my ($i, $label);
    for ($i = 1; $i < $nlabels; $i++) {
	$label = sprintf($sprintf_pat, $i);
	last unless (exists $existing_labels{$label});
    }

    # bail out if we didn't find an unused label
    return undef if ($i >= $nlabels);

    # verify $label matches $labelstr
    if ($label !~ /$labelstr/) {
        warn "Newly-generated label '$label' does not match labelstr '$labelstr'";
        return undef;
    }

    return $label;
}

1;
