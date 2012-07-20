# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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
use Amanda::MainLoop;

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

sub scan {
    my $self = shift;
    my %params = @_;

    die "Can only run one scan at a time" if $self->{'scanning'};
    $self->{'scanning'} = 1;
    $self->{'user_msg_fn'} = $params{'user_msg_fn'} || sub {};

    # refresh the tapelist at every scan
    $self->read_tapelist();

    # count the number of scans we do, so we can only load 'current' on the
    # first scan
    $self->{'scan_num'}++;

    $self->stage_1($params{'result_cb'});
}

sub _user_msg {
    my $self = shift;
    my %params = @_;

    $self->{'user_msg_fn'}->(%params);
}

sub scan_result {
    my $self = shift;
    my %params = @_;

    my @result = ($params{'error'}, $params{'res'}, $params{'label'},
		  $params{'mode'}, $params{'is_new'});

    if ($params{'error'}) {
	debug("Amanda::Taper::Scan::traditional result: error=$params{'error'}");

	# if we already had a reservation when the error occurred, then we'll need
	# to release that reservation before signalling the error
	if ($params{'res'}) {
	    my $finished_cb = make_cb(finished_cb => sub {
		my ($err) = @_;
		# if there was an error releasing, log it and ignore it
		Amanda::Debug::warn("while releasing reservation: $err") if $err;

		$self->{'scanning'} = 0;
		$params{'result_cb'}->(@result);
	    });
	    return $params{'res'}->release(finished_cb => $finished_cb);
	}
    } elsif ($params{'res'}) {
	my $devname = $params{'res'}->{'device'}->device_name;
	my $slot = $params{'res'}->{'this_slot'};
	debug("Amanda::Taper::Scan::traditional result: '$params{label}' " .
	      "on $devname slot $slot, mode $params{mode}");
    } else {
	debug("Amanda::Taper::Scan::traditional result: scan failed");

	# we may not ever have looked for this, the oldest reusable volume, if
	# the changer is not fast-searchable.  But we'll tell the user about it
	# anyway.
	my $oldest_reusable = $self->oldest_reusable_volume(new_label_ok => 0);
	$self->_user_msg(scan_failed => 1,
			 expected_label => $oldest_reusable,
			 expected_new => 1);
	@result = ("No acceptable volumes found");
    }

    $self->{'scanning'} = 0;
    $params{'result_cb'}->(@result);
}

##
# stage 1: search for the oldest reusable volume

sub stage_1 {
    my $self = shift;
    my ($result_cb) = @_;
    my $oldest_reusable;

    my $steps = define_steps
	cb_ref => \$result_cb;

    step setup => sub {
	debug("Amanda::Taper::Scan::traditional stage 1: search for oldest reusable volume");
	$oldest_reusable = $self->oldest_reusable_volume(
	    new_label_ok => 0,      # stage 1 never selects new volumes
	);

	if (!defined $oldest_reusable) {
	    debug("Amanda::Taper::Scan::traditional no oldest reusable volume");
	    return $self->stage_2($result_cb);
	}
	debug("Amanda::Taper::Scan::traditional oldest reusable volume is '$oldest_reusable'");

	# try loading that oldest volume, but only if the changer is fast-search capable
	$steps->{'get_info'}->();
    };

    step get_info => sub {
        $self->{'changer'}->info(
            info => [ "fast_search" ],
            info_cb => $steps->{'got_info'},
        );
    };

    step got_info => sub {
        my ($error, %results) = @_;
        if ($error) {
            return $self->scan_result(error => $error, result_cb => $result_cb);
        }

        if ($results{'fast_search'}) {
	    debug("Amanda::Taper::Scan::traditional stage 1: searching oldest reusable " .
		  "volume '$oldest_reusable'");
	    $self->_user_msg(search_label => 1,
			     label        => $oldest_reusable);

	    $steps->{'do_load'}->();
	} else {
	    # no fast search, so skip to stage 2
	    debug("Amanda::Taper::Scan::traditional changer is not fast-searchable; skipping to stage 2");
	    $self->stage_2($result_cb);
	}
    };

    step do_load => sub {
	$self->{'changer'}->load(
	    label => $oldest_reusable,
	    set_current => 1,
	    res_cb => $steps->{'load_done'});
    };

    step load_done => sub {
	my ($err, $res) = @_;

	$self->_user_msg(search_result => 1, res => $res, err => $err);
	if ($err) {
	    if ($err->failed and $err->notfound) {
		debug("Amanda::Taper::Scan::traditional oldest reusable volume not found");
		return $self->stage_2($result_cb);
	    } else {
		return $self->scan_result(error => $err,
			res => $res, result_cb => $result_cb);
	    }
	}

	$self->{'seen'}->{$res->{'this_slot'}} = 1;

        my $status = $res->{'device'}->status;
        if ($status != $DEVICE_STATUS_SUCCESS) {
            warning "Error reading label after searching for '$oldest_reusable'";
            return $self->release_and_stage_2($res, $result_cb);
        }

        # go on to stage 2 if we didn't get the expected volume
        my $label = $res->{'device'}->volume_label;
        my $labelstr = $self->{'labelstr'};
        if ($label !~ /$labelstr/) {
            warning "Searched for label '$oldest_reusable' but found a volume labeled '$label'";
            return $self->release_and_stage_2($res, $result_cb);
        }

	# great! -- volume found
	return $self->scan_result(res => $res, label => $oldest_reusable,
		    mode => $ACCESS_WRITE, is_new => 0, result_cb => $result_cb);
    };
}

##
# stage 2: scan for any usable volume

sub release_and_stage_2 {
    my $self = shift;
    my ($res, $result_cb) = @_;

    $res->release(finished_cb => sub {
	my ($error) = @_;
	if ($error) {
	    $self->scan_result(error => $error, result_cb => $result_cb);
	} else {
	    $self->stage_2($result_cb);
	}
    });
}

sub stage_2 {
    my $self = shift;
    my ($result_cb) = @_;

    my $last_slot;
    my $load_current = ($self->{'scan_num'} == 1);
    my $steps = define_steps
	cb_ref => \$result_cb;
    my $res;

    step load => sub {
	my ($err) = @_;

	debug("Amanda::Taper::Scan::traditional stage 2: scan for any reusable volume");

        # bail on an error releasing a reservation
        if ($err) {
            return $self->scan_result(error => $err, result_cb => $result_cb);
        }

        # load the current or next slot
	my @load_args;
	if ($load_current) {
	    # load 'current' the first time through
	    @load_args = (
		relative_slot => 'current',
	    );
	} else {
	    @load_args = (
		relative_slot => 'next',
		(defined $last_slot)? (slot => $last_slot) : (),
	    );
	}

	$self->{'changer'}->load(
	    @load_args,
	    set_current => 1,
	    res_cb => $steps->{'loaded'},
	    except_slots => $self->{'seen'},
	    mode => "write",
	);
    };

    step loaded => sub {
        (my $err, $res) = @_;
	my $loaded_current = $load_current;
	$load_current = 0; # don't load current a second time

	$self->_user_msg(search_result => 1, res => $res, err => $err);
	# bail out immediately if the scan is complete
	if ($err and $err->failed and $err->notfound) {
	    # no error, no reservation -> end of the scan
            return $self->scan_result(result_cb => $result_cb);
	}

	# tell user_msg which slot we're looking at..
	if (defined $res) {
	    $self->_user_msg(scan_slot => 1, slot => $res->{'this_slot'});
	} elsif (defined $err->{'slot'}) {
	    $self->_user_msg(scan_slot => 1, slot => $err->{'slot'});
	} else {
	    $self->_user_msg(scan_slot => 1, slot => "?");
	}

	# and then tell it the result if already known (error) or try
	# loading the volume.
	if ($err) {
	    my $ignore_error = 0;
	    # there are two "acceptable" errors: if the slot exists but the volume
	    # is already in use
	    $ignore_error = 1 if ($err->volinuse && $err->{slot});
	    # or if we loaded the 'current' slot and it was invalid (this happens if
	    # the user changes 'use-slots', for example
	    $ignore_error = 1 if ($loaded_current && $err->invalid);
	    $ignore_error = 1 if ($err->empty);

	    if ($ignore_error) {
		$self->_user_msg(slot_result => 1, err => $err);
		if ($err->{'slot'}) {
		    $last_slot = $err->{slot};
		    $self->{'seen'}->{$last_slot} = 1;
		}
		return $steps->{'load'}->(undef);
	    } else {
		# if we have a fatal error or something other than "notfound"
		# or "volinuse", bail out.
		$self->_user_msg(slot_result => 1, err => $err);
		return $self->scan_result(error => $err, res => $res,
					result_cb => $result_cb);
	    }
	}

	$self->{'seen'}->{$res->{'this_slot'}} = 1;

        $steps->{'try_volume'}->();
    };

    step try_volume => sub {
	my $slot = $res->{'this_slot'};
	my $dev = $res->{'device'};
	my $status = $dev->status;
	my $labelstr = $res->{'chg'}->{'labelstr'};
	my $label;
	my $autolabel = $res->{'chg'}->{'autolabel'};

	if ($status == $DEVICE_STATUS_SUCCESS) {
            $label = $dev->volume_label;

            if ($label !~ /$labelstr/) {
	        if (!$autolabel->{'other_config'}) {
		    $self->_user_msg(slot_result             => 1,
				     does_not_match_labelstr => 1,
				     labelstr                => $labelstr,
				     slot                    => $slot,
				     label                   => $label,
				     res                     => $res);
		    return $steps->{'try_continue'}->();
	        }
            } else {
	        # verify that the label is in the tapelist
	        my $tle = $self->{'tapelist'}->lookup_tapelabel($label);
	        if (!$tle) {
		    $self->_user_msg(slot_result     => 1,
				     not_in_tapelist => 1,
				     slot            => $slot,
				     label           => $label,
				     res             => $res);
		    return $steps->{'try_continue'}->();
	        }

	        # see if it's reusable
	        if (!$self->is_reusable_volume(label => $label, new_label_ok => 1)) {
		    $self->_user_msg(slot_result => 1,
				     active      => 1,
				     slot        => $slot,
				     label       => $label,
				     res         => $res);
		    return $steps->{'try_continue'}->();
	        }
	        $self->_user_msg(slot_result => 1,
			         slot        => $slot,
			         label       => $label,
			         res         => $res);
	        $self->scan_result(res => $res, label => $label,
				   mode => $ACCESS_WRITE, is_new => 0,
				   result_cb => $result_cb);
	        return;
	    }
	}

	if (!defined $autolabel->{'template'} ||
	    $autolabel->{'template'} eq "") {
	    if ($status & $DEVICE_STATUS_VOLUME_UNLABELED and
		$dev->volume_header and
		$dev->volume_header->{'type'} == $Amanda::Header::F_EMPTY) {
		$self->_user_msg(slot_result   => 1,
			         not_autolabel => 1,
				 empty         => 1,
			         slot          => $slot,
			         res           => $res);
	    } elsif ($status & $DEVICE_STATUS_VOLUME_UNLABELED and
		$dev->volume_header and
		$dev->volume_header->{'type'} == $Amanda::Header::F_WEIRD) {
		$self->_user_msg(slot_result   => 1,
			         not_autolabel => 1,
				 non_amanda    => 1,
			         slot          => $slot,
			         res           => $res);
	    } elsif ($status & $DEVICE_STATUS_VOLUME_ERROR) {
		$self->_user_msg(slot_result   => 1,
			         not_autolabel => 1,
				 volume_error  => 1,
				 err           => $dev->error_or_status(),
			         slot          => $slot,
			         res           => $res);
	    } elsif ($status != $DEVICE_STATUS_SUCCESS) {
		$self->_user_msg(slot_result   => 1,
			         not_autolabel => 1,
				 not_success   => 1,
				 err           => $dev->error_or_status(),
			         slot          => $slot,
			         res           => $res);
	    } else {
		$self->_user_msg(slot_result   => 1,
			         not_autolabel => 1,
			         slot          => $slot,
			         res           => $res);
	    }
	    return $steps->{'try_continue'}->();
	}

	if ($status & $DEVICE_STATUS_VOLUME_UNLABELED and
	    $dev->volume_header and
	    $dev->volume_header->{'type'} == $Amanda::Header::F_EMPTY) {
	    if (!$autolabel->{'empty'}) {
	        $self->_user_msg(slot_result  => 1,
			         empty        => 1,
			         slot         => $slot,
			         res          => $res);
	        return $steps->{'try_continue'}->();
	    }
	} elsif ($status & $DEVICE_STATUS_VOLUME_UNLABELED and
	    $dev->volume_header and
	    $dev->volume_header->{'type'} == $Amanda::Header::F_WEIRD) {
	    if (!$autolabel->{'non_amanda'}) {
	        $self->_user_msg(slot_result  => 1,
			         non_amanda   => 1,
			         slot         => $slot,
			         res          => $res);
	        return $steps->{'try_continue'}->();
	    }
	} elsif ($status & $DEVICE_STATUS_VOLUME_ERROR) {
	    if (!$autolabel->{'volume_error'}) {
	        $self->_user_msg(slot_result  => 1,
			         volume_error => 1,
			         err          => $dev->error_or_status(),
			         slot         => $slot,
			         res          => $res);
	        return $steps->{'try_continue'}->();
	    }
	} elsif ($status != $DEVICE_STATUS_SUCCESS) {
	    $self->_user_msg(slot_result  => 1,
			     not_success  => 1,
			     err          => $dev->error_or_status(),
			     slot         => $slot,
			     res          => $res);
	    return $steps->{'try_continue'}->();
	}

	$self->_user_msg(slot_result => 1, slot => $slot, res => $res);
	$res->get_meta_label(finished_cb => $steps->{'got_meta_label'});
	return;
    };

    step got_meta_label => sub {
	my ($err, $meta) = @_;

	if (defined $err) {
	    $self->scan_result(error => $err, res => $res,
			       result_cb => $result_cb);
	    return;
	}

	($meta, $err) = $res->make_new_meta_label() if !defined $meta;
	if (defined $err) {
	    $self->scan_result(error => $err, res => $res,
			       result_cb => $result_cb);
	    return;
	}

	(my $label, $err) = $res->make_new_tape_label(meta => $meta);
	

	if (!defined $label) {
            # make this fatal, rather than silently skipping new tapes
            $self->scan_result(error => $err, res => $res, result_cb => $result_cb);
            return;
	}

        $self->scan_result(res => $res, label => $label, mode => $ACCESS_WRITE,
			   is_new => 1, result_cb => $result_cb);
	return;
    };

    step try_continue => sub {
        # no luck -- release this reservation and get the next
        $last_slot = $res->{'this_slot'};

        $res->release(finished_cb => $steps->{'load'});
    };
}

1;
