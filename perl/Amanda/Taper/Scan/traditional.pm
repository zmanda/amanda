# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
	scan_num => 0,
    }, $class;

    return $self;
}

sub _user_msg_fn {
}

sub scan {
    my $self = shift;
    my %params = @_;

    die "Can only run one scan at a time" if $self->{'scanning'};
    $self->{'scanning'} = 1;
    $self->{'result_cb'} = $params{'result_cb'};
    $self->{'user_msg_fn'} = $params{'user_msg_fn'} || \&_user_msg_fn;

    # refresh the tapelist at every scan
    $self->{'tapelist'} = $self->read_tapelist();

    # count the number of scans we do, so we can only load 'current' on the
    # first scan
    $self->{'scan_num'}++;

    $self->stage_1();
}

sub _user_msg {
    my $self = shift;
    my %params = @_;
    $self->{'user_msg_fn'}->(%params);
}

sub scan_result {
    my $self = shift;
    my ($err, $res, $label, $mode) = @_;
    my @result = @_;

    if ($err) {
	debug("Amanda::Taper::Scan::traditional result: error=$err");

	# if we already had a reservation when the error occurred, then we'll need
	# to release that reservation before signalling the error
	if ($res) {
	    my $finished_cb = make_cb(finished_cb => sub {
		my ($err) = @_;
		# if there was an error releasing, log it and ignore it
		Amanda::Debug::warn("while releasing reservation: $err") if $err;

		$self->{'scanning'} = 0;
		$self->{'result_cb'}->(@result);
	    });
	    return $res->release(finished_cb => $finished_cb);
	}
    } else {
	my $devname = $res->{'device'}->device_name;
	my $slot = $res->{this_slot};
	debug("Amanda::Taper::Scan::traditional result: '$label' on $devname slot $slot, mode $mode");
    }
    $self->{'scanning'} = 0;
    $self->{'result_cb'}->(@result);
}

##
# stage 1: search for the oldest reusable volume

sub stage_1 {
    my $self = shift;
    my %subs;

    debug("Amanda::Taper::Scan::traditional stage 1: search for oldest reusable volume");
    my $oldest_reusable = $self->oldest_reusable_volume(
        new_label_ok => 0,      # stage 1 never selects new volumes
    );

    if (!defined $oldest_reusable) {
	debug("Amanda::Taper::Scan::traditional no oldest reusable volume");
	return $self->stage_2();
    }
    debug("Amanda::Taper::Scan::traditional oldest reusable volume is '$oldest_reusable'");

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
	    debug("Amanda::Taper::Scan::traditional stage 1: searching oldest reusable volume '$oldest_reusable'");
	    $self->_user_msg(search_label => 1,
			     label        => $oldest_reusable);

	    $subs{'do_load'}->();
	} else {
	    # no fast search, so skip to stage 2
	    debug("Amanda::Taper::Scan::traditional changer is not fast-searchable; skipping to stage 2");
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

	$self->_user_msg(search_result => 1, res => $res, err => $err);
	if ($err) {
	    if ($err->failed and $err->notfound) {
		debug("Amanda::Taper::Scan::traditional oldest reusable volume not found");
		return $self->stage_2();
	    } else {
		return $self->scan_result($err, $res);
	    }
	}

	$self->{'seen'}->{$res->{'this_slot'}} = 1;

        my $status = $res->{'device'}->status;
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

    my $slot = $res->{'this_slot'};
    my $dev = $res->{'device'};
    my $status = $dev->status;
    my $labelstr = $self->{'labelstr'};
    my $label;
    my $autolabel = $self->{'autolabel'};

    if ($status == $DEVICE_STATUS_SUCCESS) {
        $label = $dev->volume_label;

        if ($label !~ /$labelstr/) {
	    if (!$autolabel->{'other_config'}) {
		$self->_user_msg(slot_result             => 1,
				 does_not_match_labelstr => 1,
				 labelstr                => $labelstr,
				 slot                    => $slot,
				 res                     => $res);
		return 0;
	    }
        } else {
	    # verify that the label is in the tapelist
	    my $tle = $self->{'tapelist'}->lookup_tapelabel($label);
	    if (!$tle) {
		$self->_user_msg(slot_result     => 1,
				 not_in_tapelist => 1,
				 slot            => $slot,
				 res             => $res);
		return 0;
	    }

	    # see if it's reusable
	    if (!$self->is_reusable_volume(label => $label, new_label_ok => 1)) {
		$self->_user_msg(slot_result => 1,
				 active      => 1,
				 slot        => $slot,
				 res         => $res);
		return 0;
	    }
	    $self->_user_msg(slot_result => 1,
			     slot        => $slot,
			     res         => $res);
	    $self->scan_result(undef, $res, $label, $ACCESS_WRITE, 0);
	    return 1;
	}
    }

    if (!defined $autolabel->{'template'} ||
	$autolabel->{'template'} eq "") {
	$self->_user_msg(slot_result => 1,
			 slot        => $slot,
			 res         => $res);
	return 0;
    }

    $self->_user_msg(slot_result => 1, slot => $slot, res => $res);

    if ($status & $DEVICE_STATUS_VOLUME_UNLABELED and
	$dev->volume_header and
	$dev->volume_header->{'type'} == $Amanda::Header::F_EMPTY and
	!$autolabel->{'empty'}) {
	return 0;
    }

    if ($status & $DEVICE_STATUS_VOLUME_UNLABELED and
	$dev->volume_header and
	$dev->volume_header->{'type'} == $Amanda::Header::F_WEIRD and
	!$autolabel->{'non_amanda'}) {
	return 0;
    }

    if ($status & $DEVICE_STATUS_VOLUME_ERROR and
	!$autolabel->{'volume_error'}) {
	return 0;
    }

    ($label, my $err) = $self->make_new_tape_label();
    if (!defined $label) {
        # make this fatal, rather than silently skipping new tapes
        $self->scan_result($err, $res);
        return 1;
    }

    $self->scan_result(undef, $res, $label, $ACCESS_WRITE, 1);
    return 1;
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

    debug("Amanda::Taper::Scan::traditional stage 2: scan for any reusable volume");

    $subs{'load'} = make_cb(load => sub {
	my ($err) = @_;

        # bail on an error releasing a reservation
        if ($err) {
            return $self->scan_result($err);
        }

        # load the current or next slot
	my @load_args;
	if (defined $last_slot) {
	    @load_args = (
		relative_slot => 'next',
		slot => $last_slot,
	    );
	} elsif ($self->{'scan_num'} > 1) {
	    # don't bother to load 'current' if this is our second scan
	    @load_args = (
		relative_slot => 'next',
	    );
	} else {
	    # load 'current' the first time through
	    @load_args = (
		relative_slot => 'current',
	    );
	}
	$self->{'changer'}->load(
	    @load_args,
	    set_current => 1,
	    res_cb => $subs{'loaded'},
	    except_slots => $self->{'seen'},
	    mode => "write",
	);
    });

    $subs{'loaded'} = make_cb(loaded => sub {
        my ($err, $res) = @_;

	if (defined $res) {
	    $self->_user_msg(scan_slot => 1, slot => $res->{'this_slot'});
	} elsif (defined $err->{'slot'}) {
	    $self->_user_msg(scan_slot => 1, slot => $err->{'slot'});
	} else {
	    $self->_user_msg(scan_slot => 1, slot => "?");
	}

	if ($err and $err->failed and $err->notfound) {
            return $self->scan_result("No acceptable volumes found", $res);
	}

	if ($err && $err->{volinuse} && $err->{slot}) {
	    $self->_user_msg(slot_result => 1, err => $err);
	    $last_slot = $err->{slot};
	    $self->{'seen'}->{$last_slot} = 1;

	    # Load the next
	    $subs{'load'}->(undef);
	    return;
	} elsif ($err) {
            # if we have a fatal error or something other than "notfound"
            # or "volinuse", bail out.
	    $self->_user_msg(slot_result => 1, err => $err);
            return $self->scan_result($err, $res);
        }

	$self->{'seen'}->{$res->{'this_slot'}} = 1;

        # we're done if try_volume calls result_cb (with success or an error)
        return if ($self->try_volume($res));

        # no luck -- release this reservation and get the next
        $last_slot = $res->{'this_slot'};

        $res->release(finished_cb => $subs{'load'});
    });

    # kick the whole thing off
    $subs{'load'}->();
}

1;
