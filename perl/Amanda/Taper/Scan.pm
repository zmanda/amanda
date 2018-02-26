# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
#* License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
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

    strorage      Amanda::Storage object to use
    changer       Amanda::Changer object to use (required)
    algorithm     Taperscan algorithm to instantiate
    catalog       Amanda::DB::Catalog2
    labelstr
    autolabel
    meta_autolabel
    retention_tapes
    retention_days
    retention_recover
    retention_full

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

To see if a volume is reusable, call the C<is_reusable_volume> method.  This takes
several keyword parameters:

    $self->is_reusable_volume(
        label => $label,         # label to check
        new_label_ok => $nlo,    # count newly labeled vols as reusable?
    );

Similarly, to calculate the oldest reusable volume, call
C<oldest_reusable_volume>:

    $self->oldest_reusable_volume(
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
use Amanda::Debug qw( debug );
use Amanda::Util qw( match_labelstr_template );

sub new {
    my $class = shift;
    my %params = @_;

    die "No storage given to Amanda::Taper::Scan->new"
	unless exists $params{'storage'};
    # fill in the optional parameters
    $params{'algorithm'} = "traditional"
	unless defined $params{'algorithm'} and $params{'algorithm'} ne '';
    if ($params{'storage'}) {
	$params{'labelstr'} = $params{'storage'}->{'labelstr'}
	    if !exists $params{'labelstr'};
	$params{'autolabel'} = $params{'storage'}->{'autolabel'}
	    if !exists $params{'autolabel'};
	$params{'meta_autolabel'} = $params{'storage'}->{'meta_autolabel'}
	    if !exists $params{'meta_autolabel'};
	$params{'tapepool'} = $params{'storage'}->{'tapepool'}
	    if !exists $params{'tapepool'};
	if ($params{'storage'}->{'policy'}) {
	    $params{'retention_tapes'} = $params{'storage'}->{'policy'}->{'retention_tapes'}
			unless exists $params{'retention_tapes'};
	    $params{'retention_days'} = $params{'storage'}->{'policy'}->{'retention_days'}
			unless exists $params{'retention_days'};
	    $params{'retention_recover'} = $params{'storage'}->{'policy'}->{'retention_recover'}
			unless exists $params{'retention_recover'};
	    $params{'retention_full'} = $params{'storage'}->{'policy'}->{'retention_full'}
			unless exists $params{'retention_full'};
	}
	$params{'changer'} = $params{'storage'}->{'chg'}
			unless exists $params{'changer'};
    }
    if ($params{'chg'}) {
	my $chg = $params{'changer'};
    }
    $params{'retention_tapes'} = getconf($CNF_TAPECYCLE)
	unless exists $params{'retention_tapes'};
    $params{'retention_days'} = 0
	unless defined $params{'retention_days'};
    $params{'retention_recover'} = 0
	unless defined $params{'retention_recover'};
    $params{'retention_full'} = 0
	unless defined $params{'retention_full'};
    $params{'labelstr'} = getconf($CNF_LABELSTR)
	unless exists $params{'labelstr'};
    $params{'autolabel'} = getconf($CNF_AUTOLABEL)
	unless exists $params{'autolabel'};
    $params{'meta_autolabel'} = getconf($CNF_META_AUTOLABEL)
	unless exists $params{'meta_autolabel'};
    $params{'tapepool'} = get_config_name()
	unless exists $params{'tapepool'};

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
	Amanda::Debug::debug("Can't instantiate $pkgname");
	die("Can't instantiate $pkgname");
    }

    # and set the keys from the parameters
    $self->{'changer'} = $params{'changer'};
    $self->{'algorithm'} = $params{'algorithm'};
    $self->{'plugin'} = $params{'plugin'};
    $self->{'retention_tapes'} = $params{'retention_tapes'};
    $self->{'retention_days'} = $params{'retention_days'};
    $self->{'retention_recover'} = $params{'retention_recover'};
    $self->{'retention_full'} = $params{'retention_full'};
    $self->{'labelstr'} = $params{'labelstr'};
    $self->{'autolabel'} = $params{'autolabel'};
    $self->{'meta_autolabel'} = $params{'meta_autolabel'};
    $self->{'storage'} = $params{'storage'};
    $self->{'tapepool'} = $params{'tapepool'};
    $self->{'catalog'} = $params{'catalog'};

    return $self;
}

sub DESTROY {
    my $self = shift;

    die("Taper::Scan did not quit") if defined $self->{'changer'};
}

sub quit {
    my $self = shift;

    foreach (keys %$self) {
        delete $self->{$_};
    }
}

sub set_write_timestamp {
    my $self = shift;
    my $write_timestamp = shift;

    $self->{'write_timestamp'} = $write_timestamp;
}

sub scan {
    my $self = shift;
    my %params = @_;

    $params{'result_cb'}->("scan not implemented");
}

sub oldest_reusable_volume {
    my $self = shift;
    my %params = @_;

    my $volumes = $self->{'catalog'}->get_last_reusable_volume($self->{'storage'});
    return undef if !defined $volumes->[0];
    return $volumes->[0]->{'label'};
}

sub is_reusable_volume {
    my $self = shift;
    my %params = @_;
    my $volume = $params{'volume'};
    if (!defined $volume) {
	$volume = $self->{'catalog'}->find_volume(
				$self->{'storage'}->{'tapepool'},
				$params{'label'})
    }

    return 0 unless $volume;
    return 0 unless $volume->{'reuse'};
    return 0 if $volume->{'retention'};
    return 0 if $volume->{'retention_tape'};
    return 0 if $volume->{'retention_days'};
    return 0 if $volume->{'retention_recover'};
    return 0 if $volume->{'retention_full'};
    return 0 if $volume->{'config'} ne '' &&
		$volume->{'config'} ne Amanda::Config::get_config_name();
    return 0 if $volume->{'pool'} ne '' &&
		$volume->{'pool'} ne $self->{'tapepool'};
    return 0 if !$volume->{'pool'} and
		!match_labelstr_template($self->{'labelstr'}->{'template'},
				$volume->{'label'}, $volume->{'barcode'},
				$volume->{'meta'}, $volume->{'storage'});

    if ($volume->{'write_timestamp'} == 0) {
	return $params{'new_label_ok'};
    }

    if (defined $self->{'write_timestamp'} and
	$volume->{'write_timestamp'} eq $self->{'write_timestamp'}) {
	return 0;
    }

    return 1;
}

1;
