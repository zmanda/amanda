# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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

    strorage      Amanda::Storage object to use
    changer       Amanda::Changer object to use (required)
    algorithm     Taperscan algorithm to instantiate
    tapelist      Amanda::Tapelist
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
    $self->{'tapelist'} = $params{'tapelist'};
    $self->{'storage'} = $params{'storage'};
    $self->{'tapepool'} = $params{'tapepool'};

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

sub read_tapelist {
    my $self = shift;

    $self->{'tapelist'}->reload();
}

sub oldest_reusable_volume {
    my $self = shift;
    my %params = @_;

    return Amanda::Tapelist::get_last_reusable_tape_label(
					$self->{'labelstr'}->{'template'},
					$self->{'tapepool'},
				        $self->{'storage'}->{'storage_name'},
					$self->{'retention_tapes'},
					$self->{'retention_days'},
					$self->{'retention_recover'},
					$self->{'retention_full'},
				        0);

    my $retention_tapes = $self->{'retention_tapes'};

    my $best = undef;
    my $num_acceptable = 0;
    for my $tle (@{$self->{'tapelist'}->{'tles'}}) {
	next unless $tle->{'reuse'};
	next if $tle->{'datestamp'} eq '0' and !$params{'new_label_ok'};
	next if $tle->{'config'} and
		$tle->{'config'} ne Amanda::Config::get_config_name();
	next if $tle->{'pool'} and
		$tle->{'pool'} ne $self->{'tapepool'};
	next if !$tle->{'pool'} &&
		!match_labelstr_template($self->{'labelstr'}->{'template'},
				$tle->{'label'}, $tle->{'barcode'},
				$tle->{'meta'});
	$num_acceptable++;
	$best = $tle;
    }

    # if we didn't find at least $tapecycle reusable tapes, then
    # there is no oldest reusable tape

    return undef unless $num_acceptable > $retention_tapes;

    return $best->{'label'};
}

sub is_reusable_volume {
    my $self = shift;
    my %params = @_;

    my $vol_tle = $self->{'tapelist'}->lookup_tapelabel($params{'label'});
    return 0 unless $vol_tle;
    return 0 unless $vol_tle->{'reuse'};
    return 0 if $vol_tle->{'config'} and
		$vol_tle->{'config'} ne Amanda::Config::get_config_name();
    return 0 if $vol_tle->{'pool'} and
		$vol_tle->{'pool'} ne $self->{'tapepool'};
    return 0 if !$vol_tle->{'pool'} and
		!match_labelstr_template($self->{'labelstr'}->{'template'},
				$vol_tle->{'label'}, $vol_tle->{'barcode'},
				$vol_tle->{'meta'});

    if ($vol_tle->{'datestamp'} eq '0') {
	return $params{'new_label_ok'};
    }

    if (defined $self->{'write_timestamp'} and
	$vol_tle->{'datestamp'} eq $self->{'write_timestamp'}) {
	return 0;
    }

    return Amanda::Tapelist::volume_is_reusable($params{'label'});
}

1;
