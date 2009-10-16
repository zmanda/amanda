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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Taper::Scan::traditional;

=head1 NAME

Amanda::Taper::Scan::traditional

=head1 SYNOPSIS

This package implements the "traditional" taperscan algorithm.  See
C<amanda-taperscan(7)>.

=cut

use strict;
use warnings;
use base qw( Amanda::Taper::Scan );
use Amanda::Tapelist;
use Amanda::Config qw( :getconf );
use Amanda::Device qw( :constants );
use Amanda::Header;
use Amanda::Debug qw( :logging );
use Amanda::MainLoop qw( make_cb );

sub new {
    my $class = shift;
    my %params = @_;

    # parent will set all of the $params{..} keys for us
    my $self = bless {
	scanning => 0,
        tapelist => undef,
	seen => {},
    }, $class;

    return $self;
}

sub scan {
    my $self = shift;
    my %params = @_;

    die "Can only run one scan at a time" if $self->{'scanning'};
    $self->{'scanning'} = 1;
    $self->{'result_cb'} = $params{'result_cb'};
    $self->{'user_msg_fn'} = $params{'user_msg_fn'};

    # refresh the tapelist at every scan
    $self->{'tapelist'} = $self->read_tapelist();

    $self->stage_1();
}

sub _user_msg {
    my $self = shift;
    my ($msg) = @_;
    if (defined $self->{'user_msg_fn'}) {
	$self->{'user_msg_fn'}->($msg);
    } else {
	Amanda::Debug::info("Amanda::Taper::Scan::traditional: $msg");
    }
}

sub scan_result {
    my $self = shift;
    my ($err, $res, $label, $mode) = @_;
    my @result = @_;

    if ($err) {
	debug("Amanda::Taper::Scan::traditional result: error=$err");
    } else {
	my $devname = $res->{'device'}->device_name;
	debug("Amanda::Taper::Scan::traditional result: '$label' on $devname, mode $mode");
    }
    $self->{'scanning'} = 0;
    $self->{'result_cb'}->(@result);
}

##
# stage 1: search for the oldest reusable volume

sub stage_1 {
    my $self = shift;
    my %subs;

    $self->_user_msg("stage 1: search for oldest reusable volume");
    my $oldest_reusable = $self->oldest_reusable_volume(
        new_label_ok => 0,      # stage 1 never selects new volumes
    );

    if (!defined $oldest_reusable) {
	$self->_user_msg("no oldest reusable volume");
	return $self->stage_2();
    }
    $self->_user_msg("oldest reusable volume is '$oldest_reusable'");

    # try loading that oldest volume, but only if the changer is fast-search capable

    $subs{'get_info'} = sub {
        $self->{'changer'}->info(
            info => [ "fast_search" ],
            info_cb => $subs{'got_info'},
        );
    };

    $subs{'got_info'} = sub {
        my ($error, %results) = @_;
        if ($error) {
            return $self->scan_result($error);
        }

        if ($results{'fast_search'}) {
	    $subs{'do_load'}->();
	} else {
	    # no fast search, so skip to stage 2
	    $self->_user_msg("changer is not fast-searchable; skipping to stage 2");
	    $self->stage_2();
	}
    };

    $subs{'do_load'} = sub {
	$self->{'changer'}->load(
	    label => $oldest_reusable,
	    res_cb => $subs{'load_done'});
    };

    $subs{'load_done'} = sub {
	my ($err, $res) = @_;

	if ($err) {
	    if ($err->failed and $err->notfound) {
		$self->_user_msg("oldest reusable volume not found");
		return $self->stage_2();
	    } else {
		return $self->scan_result($err);
	    }
	}

	$self->{'seen'}->{$res->{'this_slot'}} = 1;

        my $status = $res->{'device'}->read_label();
        if ($status != $DEVICE_STATUS_SUCCESS) {
            warning "Error reading label after searching for '$oldest_reusable'";
            return $self->release_and_stage_2($res);
        }

        # go on to stage 2 if we didn't get the expected volume
        my $label = $res->{'device'}->volume_label;
        my $labelstr = $self->{'labelstr'};
        if ($label !~ /$labelstr/) {
            warning "Searched for label '$oldest_reusable' but found a volume labeled '$label'";
            return $self->release_and_stage_2($res);
        }

	# great! -- volume found
	return $self->scan_result(undef, $res, $oldest_reusable, $ACCESS_WRITE, 0);
    };

    $subs{'get_info'}->();
}

sub try_volume {
    my $self = shift;
    my ($res) = @_;

    my $dev = $res->{'device'};
    my $status = $dev->read_label();
    my $labelstr = $self->{'labelstr'};
    my $label;

    if ($status == $DEVICE_STATUS_SUCCESS) {
        $label = $dev->volume_label;

        if ($label !~ /$labelstr/) {
            $self->_user_msg("Volume label '$label' does not match labelstr '$labelstr'");
            return 0;
        }

        # verify that the label is in the tapelist, if it's not new
	if ($dev->volume_time() ne "X") {
	    my $tle = $self->{'tapelist'}->lookup_tapelabel($label);
	    if (!$tle) {
		$self->_user_msg("Volume label '$label' is not in the tapelist");
		return 0;
	    }

	    # see if it's reusable
	    if (!$self->is_reusable_volume(label => $label, new_label_ok => 1)) {
		$self->_user_msg("Volume with label '$label' is still active and cannot be overwritten");
		return 0;
	    }
	}

        $self->scan_result(undef, $res, $label, $ACCESS_WRITE, 0);
        return 1;

    } elsif ($status & $DEVICE_STATUS_VOLUME_UNLABELED) {
        # unlabeled volume -- we can only use this if label_new_tapes is set
        if (!$self->{'label_new_tapes'}) {
            return 0;
        }

        if ($dev->volume_header and
            $dev->volume_header->{'type'} != $Amanda::Header::F_EMPTY) {
	    my $slot = $res->{'this_slot'};
            $self->_user_msg("Slot '$slot' contains a non-Amanda volume; check and " .
                 "relabel it with 'amlabel -f'");
            return 0;
        }

        $label = $self->make_new_tape_label();
        if (!defined $label) {
            # make this fatal, rather than silently skipping new tapes
            $self->scan_result("Could not invent new label for new volume");
            return 1;
        }

        $self->scan_result(undef, $res, $label, $ACCESS_WRITE, 1);
        return 1;
    }

    # nope - the device is useless to us
    return 0;
}

##
# stage 2: scan for any usable volume

sub release_and_stage_2 {
    my $self = shift;
    my ($res) = @_;

    $res->release(finished_cb => sub {
	my ($error) = @_;
	if ($error) {
	    $self->scan_result($error);
	} else {
	    $self->stage_2();
	}
    });
}

sub stage_2 {
    my $self = shift;

    my $last_slot;
    my %subs;

    $self->_user_msg("stage 2: scan for any reusable volume");

    $subs{'load'} = make_cb(load => sub {
	my ($err) = @_;

        # bail on an error releasing a reservation
        if ($err) {
            return $self->scan_result($err);
        }

        # load the current or next slot
	if (defined $last_slot) {
	    $self->{'changer'}->load(
		relative_slot => 'next',
		slot => $last_slot,
		set_current => 1,
		res_cb => $subs{'loaded'},
		except_slots => $self->{'seen'},
		mode => "write",
	    );
	} else {
	    $self->{'changer'}->load(
		relative_slot => "current",
		set_current => 1,
		res_cb => $subs{'loaded'},
		except_slots => $self->{'seen'},
		mode => "write",
	    );
	}
    });

    $subs{'loaded'} = make_cb(loaded => sub {
        my ($err, $res) = @_;

	if ($err and $err->failed and $err->notfound) {
            return $self->scan_result("No acceptable volumes found");
	}

        # if we have a fatal error or something other than "notfound",
        # bail out.
        if ($err) {
            return $self->scan_result($err);
        }

	$self->{'seen'}->{$res->{'this_slot'}} = 1;

        # we're done if try_volume calls result_cb (with success or an error)
	$self->_user_msg("trying slot $res->{this_slot}");
        return if ($self->try_volume($res));

        # no luck -- release this reservation and get the next
        $last_slot = $res->{'this_slot'};

        $res->release(finished_cb => $subs{'load'});
    });

    # kick the whole thing off
    $subs{'load'}->();
}

1;
