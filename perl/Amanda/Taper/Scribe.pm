# Copyright (c) 2009, 2010 Zmanda, Inc.  All Rights Reserved.
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

package Amanda::Taper::Scribe;

use strict;
use warnings;
use Carp;

use Amanda::Xfer qw( :constants );
use Amanda::Device qw( :constants );
use Amanda::Header;
use Amanda::Debug qw( :logging );
use Amanda::MainLoop;

=head1 NAME

Amanda::Taper::Scribe

=head1 SYNOPSIS

  my $scribe = Amanda::Taper::Scribe->new(
	taperscan => $taperscan_algo,
        feedback => $feedback_obj);

  $subs{'start_scribe'} = make_cb(start_scribe => sub {
    $scribe->start($datestamp, finished_cb => $subs{'start_xfer'});
  });

  $subs{'start_xfer'} = make_cb(start_xfer => sub {
    my ($err) = @_;

    my $xfer_dest = $scribe->get_xfer_dest(
	max_memory => 64 * 1024,
	split_method => 'disk',
	part_size => 150 * 1024**2,
	disk_cache_dirname => "$tmpdir/splitbuffer");

    # .. set up the rest of the transfer ..

    $xfer->start(sub {
        my ($src, $msg, $xfer) = @_;
        $scribe->handle_xmsg($src, $msg, $xfer);
        # .. any other processing ..
    });

    # tell the scribe to start dumping via this transfer
    $scribe->start_dump(
	xfer => $xfer,
        dump_header => $hdr,
        dump_cb => $subs{'dump_cb'});
  });

  $subs{'dump_cb'} = make_cb(dump_cb => sub {
      my %params = @_;
      # .. handle dump results ..

      print "DONE\n";
      Amanda::MainLoop::quit();
  });


  $subs{'start_scribe'}->();
  Amanda::MainLoop::run();

=head1 OVERVIEW

This package provides a high-level abstraction of Amanda's procedure for
writing dumpfiles to tape.

Amanda writes a sequence of dumpfiles to a sequence of volumes.  The
volumes are supplied by a taperscan algorithm, which operates a changer
to find and load each volume.  As dumpfiles are written to volumes and
those volumes fill up, the taperscan algorithm supplies additional
volumes.

In order to reduce internal fragmentation within volumes, Amanda can "split"
dumpfiles into smaller pieces, so that the overall dumpfile can span multiple
volumes.  Each "part" is written to the volume in sequence.  If a device
encounters an error while writing a part, then that part is considered
"partial", and is rewritten from its beginning on the next volume.  Some
devices can reliably indicate that they are full (EOM), and for these devices
parts are simply truncated, and the Scribe starts the next part on the next
volume.

To facilitate rewriting parts on devices which cannot indicate EOM, Amanda must
retain all of the data in a part, even after that data is written to the
volume.  The Scribe provides several methods to support this: caching the part
in memory, caching the part in a special on-disk file, or relying on
pre-existing on-disk storage.  The latter method is used when reading from
holding disks.

The details of efficiently splitting dumpfiles and rewriting parts are handled
by the low-level C<Amanda::Xfer::Dest::Taper> subclasses.  The Scribe creates
an instance of the appropriate subclass and supplies it with volumes from an
C<Amanda::Taper::Scan> object.  It calls a number of
C<Amanda::Taper::Scribe::Feedback> methods to indicate the status of the dump
process and to request permission for each additional volume.

=head1 OPERATING A SCRIBE

The C<Amanda::Taper::Scribe> constructor takes two arguments:
C<taperscan> and C<feedback>.  The first specifies the taper scan
algorithm that the Scribe should use, and the second specifies the
C<Feedback> object that will receive notifications from the Scribe (see
below).

  my $scribe = Amanda::Taper::Scribe->new(
        taperscan => $my_taperscan,
        feedback => $my_feedback);

Once the object is in place, call its C<start> method.

=head2 START THE SCRIBE

Start the scribe's operation by calling its C<start> method.  This will invoke
the taperscan algorithm and scan for a volume.  The method takes two parameters:

  $scribe->start(
        write_timestamp => $ts,
	finished_cb => $start_finished_cb);

The timestamp will be written to each volume written by the Scribe.  The
C<finished_cb> will be called with a single argument - C<undef> or an error
message - when the Scribe is ready to start its first dump.  The Scribe is
"ready" when it has found a device to which it can write, although it does not
request permission to overwrite that volume, nor start overwriting it, until
the first dump begins (that is, until the first call to C<start_dump>).

=head2 SET UP A TRANSFER

Once the Scribe is started, begin transferring a dumpfile.  This is a
three-step process: first, get an C<Amanda::Xfer::Dest::Taper> object from the
Scribe, then start the transfer, and finally let the Scribe know that the
transfer has started.  Note that the Scribe supplies and manages the transfer
destination, but the transfer itself remains the responsibility of the caller.

=head3 Get a Transfer Destination

Call C<get_xfer_dest> to get the transfer element, supplying information on how
the dump should be split:

  $xdest = $scribe->get_xfer_dest(
        max_memory => $max_memory,
        # .. split parameters
        );

This method must be called after C<start> has completed, and will always return
a transfer element immediately.

The underlying C<Amanda::Xfer::Dest::Taper> handles device streaming
properly.  It uses C<max_memory> bytes of memory for this purpose.

The arguments to C<get_xfer_dest> differ for the various split methods.
For no splitting:

  $scribe->get_xfer_dest(
        # ...
        split_method => 'none');

For buffering the split parts in memory:

  $scribe->get_xfer_dest(
        # ...
        split_method => 'memory',
        part_size => $part_size);

For buffering the split parts on disk:

  $scribe->get_xfer_dest(
        # ...
        split_method => 'disk',
        part_size => $part_size,
        disk_cache_dirname => $disk_cache_dirname);

Finally, if the transfer source is capable of calling
C<Amanda::Xfer::Dest::Taper>'s C<cache_inform> method:

  $scribe->get_xfer_dest(
        # ...
        split_method => 'cache_inform',
        part_size => $part_size);

An C<Amanda::Taper::Scribe> object can only run one transfer at a time, so
do not call C<get_xfer_dest> until the C<dump_cb> for the previous C<start_dump>
has been called.

=head3 Start the Transfer

Armed with the element returned by C<get_xfer_dest>, the caller should create a
source element and a transfer object and start the transfer.  In order to
manage the splitting process, the Scribe needs to be informed, via its
C<handle_xmsg> method, of all transfer messages .  This is usually accomplished
with something like:

  $xfer->start(sub {
      my ($src, $msg, $xfer) = @_;
      $scribe->handle_xmsg($src, $msg, $xfer);
  });

=head3 Inform the Scribe

Once the transfer has started, the Scribe is ready to begin writing parts to
the volume.  This is the first moment at which the Scribe needs a header, too.
All of this is supplied to the C<start_dump> method:

  $scribe->start_dump(
      xfer => $xfer,
      dump_header => $hdr,
      dump_cb => $dump_cb);

The c<dump_header> here is the header that will be applied to all parts of the
dumpfile.  The only field in the header that the Scribe controls is the part
number.  The C<dump_cb> callback passed to C<start_dump> is called when the
dump is completely finished - either successfully or with a fatal error.
Unlike most callbacks, this one takes keyword arguments, since it has so many
parameters.

  $dump_cb->(
        result => $result,
        device_errors => $device_errors,
        size => $size,
        duration => $duration,
	total_duration => $total_duration);

All parameters will be present on every call.

The C<result> is one of C<"FAILED">, C<"PARTIAL">, or C<"DONE">.  Even when
C<dump_cb> reports a fatal error, C<result> may be C<"PARTIAL"> if some data
was written successfully.

The final parameters, C<size> (in bytes), C<duration>, and C<total_duration>
(in seconds) describe the total transfer, and are a sum of all of the parts
written to the device.  Note that C<duration> does not include time spent
operating the changer, while C<total_duration> reflects the time from the
C<start_dump> call to the invocation of the C<dump_cb>.

TODO: cancel_dump

=head2 QUIT

When all of the dumpfiles are transferred, call the C<quit> method to
release any resources and clean up.  This method takes a typical
C<finished_cb>.

  $scribe->quit(finished_cb => sub {
    print "ALL DONE!\n";
  });

=head2 GET_BYTES_WRITTEN

The C<get_bytes_written> returns the number of bytes written to the device at
the time of the call, and is meant to be used for status reporting.  This value
is updated at least as each part is finished; for some modes of operation, it
is updated continuously.  Notably, DirectTCP transfers do not update
continuously.

=head1 FEEDBACK

The C<Amanda::Taper::Scribe::Feedback> class is intended to be
subclassed by the user.  It provides a number of notification methods
that enable the historical logging and driver/taper interactions
required by Amanda.  The parent class does nothing of interest, but
allows subclasses to omit methods they do not need.

The C<request_volume_permission> method provides a means for the caller
to limit the number of volumes the Scribe consumes.  It is called as

  $fb->request_volume_permission(perm_cb => $cb);

where the C<perm_cb> is a callback which expects a single argument:
C<undef> if permission is granted, or reason (as a string) if permission
is denied.  The default implementation always calls C<< perm_cb->(undef) >>.

All of the remaining methods are notifications, and do not take a
callback.

  $fb->notif_new_tape(
        error => $error,
        volume_label => $volume_label);

The Scribe calls C<notif_new_tape> when a new volume is started.  If the
C<volume_label> is undefined, then the volume was not successfully
relabled, and its previous contents may still be available.  If C<error>
is defined, then no useful data was written to the volume.  Note that
C<error> and C<volume_label> may I<both> be defined if the previous
contents of the volume were erased, but no useful, new data was written
to the volume.

This method will be called exactly once for every call to
C<request_volume_permission> that calls C<< perm_cb->(undef) >>.

  $fb->notif_part_done(
        partnum => $partnum,
        fileno => $fileno,
        successful => $successful,
        size => $size,
        duration => $duration);

The Scribe calls C<notif_part_done> for each part written to the volume,
including partial parts.  If the part was not written successfully, then
C<successful> is false.  The C<size> is in bytes, and the C<duration> is
a floating-point number of seconds.  If a part fails before a new device
file is created, then C<fileno> may be zero.

Finally, the Scribe sends a few historically significant trace log messages
via C<notif_log_info>:

  $fb->notif_log_info(
	message => $message);

A typical Feedback subclass might begin like this:

  package main::Feedback;
  use base 'Amanda::Taper::Scribe::Feedback';

  sub request_volume_permission {
    my $self = shift;
    my %params = @_;

    $params{'perm_cb'}->("NO VOLUMES FOR YOU!");
  }

=cut

sub new {
    my $class = shift;
    my %params = @_;

    for my $rq_param qw(taperscan feedback) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }

    my $self = {
	feedback => $params{'feedback'},
	debug => $params{'debug'},
	write_timestamp => undef,
	started => 0,

	# device handling, and our current device and reservation
	devhandling => Amanda::Taper::Scribe::DevHandling->new(
	    taperscan => $params{'taperscan'},
	    feedback => $params{'feedback'},
	),
	reservation => undef,
	device => undef,
	device_size => undef,

        # callback passed to start_dump
	dump_cb => undef,

	# information for the current dumpfile
	dump_header => undef,
	split_method => undef,
	xfer => undef,
	xdt => undef,
	xdt_ready => undef,
	start_part_on_xdt_ready => 0,
	size => 0,
	duration => 0.0,
	dump_start_time => undef,
	last_part_successful => 0,
	started_writing => 0,
	device_errors => [],
    };

    return bless ($self, $class);
}

sub start {
    my $self = shift;
    my %params = @_;

    for my $rq_param qw(write_timestamp finished_cb) {
	croak "required parameter '$rq_param' missing"
	    unless exists $params{$rq_param};
    }

    die "scribe already started" if $self->{'started'};

    $self->dbg("starting");
    $self->{'write_timestamp'} = $params{'write_timestamp'};

    # start up the DevHandling object, making sure we know
    # when it's done with its startup process
    $self->{'devhandling'}->start(finished_cb => sub {
	$self->{'started'} = 1;
	$params{'finished_cb'}->(@_);
    });
}

sub quit {
    my $self = shift;
    my %params = @_;

    for my $rq_param qw(finished_cb) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }

    $self->_log_volume_done();

    # since there's little other option than to barrel on through the
    # quitting procedure, quit() just accumulates its error messages
    # and, if necessary, concantenates them for the finished_cb.
    my @errors;

    if ($self->{'xfer'}) {
	die "Scribe cannot quit while a transfer is active";
        # Supporting this would be complicated:
        # - cancel the xfer and wait for it to complete
        # - ensure that the taperscan not be started afterward
        # and isn't required for normal Amanda operation.
    }

    $self->dbg("quitting");

    my $cleanup_cb = make_cb(cleanup_cb => sub {
	my ($error) = @_;
	push @errors, $error if $error;

        if (@errors == 1) {
            $error = $errors[0];
        } elsif (@errors > 1) {
            $error = join("; ", @errors);
        }

        $params{'finished_cb'}->($error);
    });

    if ($self->{'reservation'}) {
	if ($self->{'device'}) {
	    if (!$self->{'device'}->finish()) {
		push @errors, $self->{'device'}->error_or_status();
	    }
	}

	$self->{'reservation'}->release(finished_cb => $cleanup_cb);
    } else {
	$cleanup_cb->(undef);
    }
}

# Get a transfer destination; does not use a callback
sub get_xfer_dest {
    my $self = shift;
    my %params = @_;

    for my $rq_param qw(max_memory split_method) {
	croak "required parameter '$rq_param' missing"
	    unless exists $params{$rq_param};
    }

    die "Scribe is not started yet" unless $self->{'started'};

    $self->dbg("get_xfer_dest(split_method=$params{split_method})");

    if ($params{'split_method'} ne 'none') {
        croak("required parameter 'part_size' missing")
            unless exists $params{'part_size'};
    }

    $self->{'split_method'} = $params{'split_method'};
    my ($part_size, $use_mem_cache, $disk_cache_dirname) = (0, 0, undef);
    if ($params{'split_method'} eq 'none') {
        $part_size = 0;
    } elsif ($params{'split_method'} eq 'memory') {
        $part_size = $params{'part_size'};
        $use_mem_cache = 1;
    } elsif ($params{'split_method'} eq 'disk') {
        $part_size = $params{'part_size'};
        croak("required parameter 'disk_cache_dirname' missing")
            unless exists $params{'disk_cache_dirname'};
        $disk_cache_dirname = $params{'disk_cache_dirname'};
    } elsif ($params{'split_method'} eq 'cache_inform') {
        $part_size = $params{'part_size'};
        $use_mem_cache = 0;
    } else {
        croak("invalid split_method $params{split_method}");
    }

    debug("Amanda::Taper::Scribe setting up a transfer with split method $params{split_method}");

    die "not yet started"
	unless ($self->{'write_timestamp'});
    die "xfer element already returned"
	if ($self->{'xdt'});
    die "xfer already running"
	if ($self->{'xfer'});

    $self->{'xfer'} = undef;
    $self->{'xdt'} = undef;
    $self->{'size'} = 0;
    $self->{'duration'} = 0.0;
    $self->{'dump_start_time'} = undef;
    $self->{'last_part_successful'} = 1;
    $self->{'started_writing'} = 0;
    $self->{'device_errors'} = [];

    # set the callback
    $self->{'dump_cb'} = undef;

    # to build an xfer destination, we need a device, although we don't necessarily
    # need permission to write to it yet.  So we can either use a device we already
    # have, or we "peek" at the DevHandling object's device.
    my $xdt_first_dev;
    if (defined $self->{'device'}) {
	$xdt_first_dev = $self->{'device'};
    } else {
	$xdt_first_dev = $self->{'devhandling'}->peek_device();
    }

    if (!defined $xdt_first_dev) {
	die "no device is available to create an xfer_dest";
    }

    # set the device to verbose logging if we're in debug mode
    if ($self->{'debug'}) {
	$xdt_first_dev->property_set("verbose", 1);
    }

    my $use_directtcp = $xdt_first_dev->directtcp_supported();

    my $xdt;
    if ($use_directtcp) {
	# note: using the current configuration scheme, the user must specify either
	# a disk cache or a fallback_splitsize in order to split a directtcp dump; the
	# fix is to use a better set of config params for splitting
	$xdt = Amanda::Xfer::Dest::Taper::DirectTCP->new(
	    $xdt_first_dev, $part_size);
	$self->{'xdt_ready'} = 0; # xdt isn't ready until we get XMSG_READY
    } else {
	$xdt = Amanda::Xfer::Dest::Taper::Splitter->new(
	    $xdt_first_dev, $params{'max_memory'}, $part_size,
	    $use_mem_cache, $disk_cache_dirname);
	$self->{'xdt_ready'} = 1; # xdt is ready immediately
    }
    $self->{'start_part_on_xdt_ready'} = 0;
    $self->{'xdt'} = $xdt;

    return $xdt;
}

sub start_dump {
    my $self = shift;
    my %params = @_;

    die "no xfer dest set up; call get_xfer_dest first"
        unless defined $self->{'xdt'};

    # get the header ready for writing (totalparts was set by the caller)
    $self->{'dump_header'} = $params{'dump_header'};
    $self->{'dump_header'}->{'partnum'} = 1;

    # set up the dump_cb for when this dump is done, and keep the xfer
    $self->{'dump_cb'} = $params{'dump_cb'};
    $self->{'xfer'} = $params{'xfer'};
    $self->{'dump_start_time'} = time;

    # and start the part
    $self->_start_part();
}

sub cancel_dump {
    my $self = shift;
    my %params = @_;

    die "no xfer dest set up; call get_xfer_dest first"
	unless defined $self->{'xdt'};

    # set up the dump_cb for when this dump is done, and keep the xfer
    $self->{'dump_cb'} = $params{'dump_cb'};
    $self->{'xfer'} = $params{'xfer'};

    # The cancel should call dump_cb, but the xfer stay hanged in accept.
    # That's why dump_cb is called and xdt and xfer are set to undef.
    $self->{'xfer'}->cancel();

    $self->{'dump_cb'}->(
	result => "FAILED",
	device_errors => [],
	size => 0,
	duration => 0.0,
	total_duration => 0);
    $self->{'xdt'} = undef;
    $self->{'xfer'} = undef;
}

sub get_bytes_written {
    my ($self) = @_;

    if (defined $self->{'xdt'}) {
	return $self->{'size'} + $self->{'xdt'}->get_part_bytes_written();
    } else {
	return $self->{'size'};
    }
}

sub _start_part {
    my $self = shift;

    $self->dbg("trying to start part");

    # if the xdt isn't ready yet, wait until it is; note that the XDT is still
    # using the device right now, so we can't even label it yet.
    if (!$self->{'xdt_ready'}) {
	$self->dbg("XDT not ready yet; waiting until it is");
	$self->{'start_part_on_xdt_ready'} = 1;
	return
    }

    # we need an actual, permitted device at this point, so if we don't have
    # one, then defer this start_part call until we do
    if (!$self->{'device'}) {
	# _get_new_volume calls _start_part when it has a new volume in hand
	return $self->_get_new_volume();
    }

    # if the dump wasn't successful, and we're not splitting, then bail out.  It's
    # up to higher-level components to re-try this dump on a new volume, if desired.
    # Note that this should be caught in the XMSG_PART_DONE handler -- this is just
    # here for backup.
    if (!$self->{'last_part_successful'} and $self->{'split_method'} eq 'none') {
	$self->_operation_failed("No space left on device");
	return;
    }

    # and start writing this part
    $self->{'started_writing'} = 1;
    $self->dbg("resuming transfer");
    $self->{'xdt'}->start_part(!$self->{'last_part_successful'},
			       $self->{'dump_header'});
}

sub handle_xmsg {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    if ($msg->{'type'} == $XMSG_DONE) {
	$self->_xmsg_done($src, $msg, $xfer);
	return;
    }

    # for anything else we only pay attention to messages from
    # our own element
    if ($msg->{'elt'} == $self->{'xdt'}) {
	$self->dbg("got msg from xfer dest: $msg");
	if ($msg->{'type'} == $XMSG_PART_DONE) {
	    $self->_xmsg_part_done($src, $msg, $xfer);
	} elsif ($msg->{'type'} == $XMSG_READY) {
	    $self->_xmsg_ready($src, $msg, $xfer);
	} elsif ($msg->{'type'} == $XMSG_ERROR) {
	    $self->_xmsg_error($src, $msg, $xfer);
	}
    }
}

sub _xmsg_part_done {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

	# this handles successful zero-byte parts as a special case - they
	# are an implementation detail of the splitting done by the transfer
	# destination.

    if ($msg->{'successful'} and $msg->{'size'} == 0) {
	$self->dbg("not notifying for empty, successful part");
    } else {
	# double-check partnum
	die "Part numbers do not match!"
	    unless ($self->{'dump_header'}->{'partnum'} == $msg->{'partnum'});

	# notify
	$self->{'feedback'}->notif_part_done(
	    partnum => $msg->{'partnum'},
	    fileno => $msg->{'fileno'},
	    successful => $msg->{'successful'},
	    size => $msg->{'size'},
	    duration => $msg->{'duration'});
    }

    $self->{'last_part_successful'} = $msg->{'successful'};

    if ($msg->{'successful'}) {
	$self->{'device_size'} += $msg->{'size'};
	$self->{'size'} += $msg->{'size'};
	$self->{'duration'} += $msg->{'duration'};
    }

    if (!$msg->{'eof'}) {
	# update the header for the next dumpfile, if this was a non-empty part
	if ($msg->{'successful'} and $msg->{'size'} != 0) {
	    $self->{'dump_header'}->{'partnum'}++;
	}

	if ($msg->{'eom'}) {
	    # if there's an error finishing the device, it's probably just carryover
	    # from the error the Xfer::Dest::Taper encountered while writing to the
	    # device, so we ignore it.
	    if (!$self->{'device'}->finish()) {
		my $devname = $self->{'device'}->device_name;
		my $errmsg = $self->{'device'}->error_or_status();
		$self->dbg("ignoring error while finishing device '$devname': $errmsg");
	    }

	    # if the part failed..
	    if (!$msg->{'successful'}) {
		# if no caching was going on, then the dump has failed
		if ($self->{'split_method'} eq 'none') {
		    my $msg = "No space left on device";
		    if ($self->{'device'}->status() != $DEVICE_STATUS_SUCCESS) {
			$msg = $self->{'device'}->error_or_status();
		    }
		    $self->_operation_failed($msg);
		    return;
		}

		# log a message for amreport
		$self->{'feedback'}->notif_log_info(
		    message => "Will request retry of failed split part.");
	    }

	    # get a new volume, then go on to the next part
	    $self->_get_new_volume();
	} else {
	    # if the part was unsuccessful, but the xfer dest has reason to believe
	    # this is not due to EOM, then the dump is done
	    if (!$msg->{'successful'}) {
		my $msg = "unknown error while dumping";
		if ($self->{'device'}->status() != $DEVICE_STATUS_SUCCESS) {
		    $msg = $self->{'device'}->error_or_status();
		}
		$self->_operation_failed($msg);
		return;
	    }

	    # no EOM -- go on to the next part
	    $self->_start_part();
	}
    }
}

sub _xmsg_ready {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    $self->dbg("XDT is ready");
    $self->{'xdt_ready'} = 1;
    if ($self->{'start_part_on_xdt_ready'}) {
	$self->{'start_part_on_xdt_ready'} = 0;
	$self->_start_part();
    }
}

sub _xmsg_error {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    # XMSG_ERROR from the XDT is always fatal
    $self->_operation_failed($msg->{'message'});
}

sub _xmsg_done {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    if ($msg->{'type'} == $XMSG_DONE) {
	$self->dbg("transfer is complete");
	$self->_dump_done();
    }
}

sub _dump_done {
    my $self = shift;

    my $result;

    # determine the correct final status - DONE if we're done, PARTIAL
    # if we've started writing to the volume, otherwise FAILED
    if (@{$self->{'device_errors'}}) {
	$result = $self->{'started_writing'}? 'PARTIAL' : 'FAILED';
    } else {
	$result = 'DONE';
    }

    my $dump_cb = $self->{'dump_cb'};
    my %dump_cb_args = (
	result => $result,
	device_errors => $self->{'device_errors'},
	size => $self->{'size'},
	duration => $self->{'duration'},
	total_duration => time - $self->{'dump_start_time'});

    # reset everything and let the original caller know we're done
    $self->{'xfer'} = undef;
    $self->{'xdt'} = undef;
    $self->{'dump_header'} = undef;
    $self->{'dump_cb'} = undef;
    $self->{'size'} = 0;
    $self->{'duration'} = 0.0;
    $self->{'dump_start_time'} = undef;
    $self->{'device_errors'} = [];

    # and call the callback
    $dump_cb->(%dump_cb_args);
}

sub _operation_failed {
    my $self = shift;
    my ($error) = @_;

    $self->dbg("operation failed: $error");

    push @{$self->{'device_errors'}}, $error;

    # cancelling the xdt will eventually cause an XMSG_DONE, which will notice
    # the error and set the result correctly; but if there's no xfer, then we
    # can just call _dump_done directly.
    if (defined $self->{'xfer'}) {
        $self->dbg("cancelling the transfer: $error");

	$self->{'xfer'}->cancel();
    } else {
        if (defined $self->{'dump_cb'}) {
            # _dump_done uses device_errors, set above
            $self->_dump_done();
        } else {
            die "error with no callback to handle it: $error";
        }
    }
}

sub _log_volume_done {
    my $self = shift;

    # if we've already written a volume, log it
    if ($self->{'device'} and defined $self->{'device'}->volume_label) {
	my $label = $self->{'device'}->volume_label();
	my $fm = $self->{'device'}->file();
	my $kb = $self->{'device_size'} / 1024;

	# log a message for amreport
	$self->{'feedback'}->notif_log_info(
	    message => "tape $label kb $kb fm $fm [OK]");
    }
}

# invoke the devhandling to get a new device, with all of the requisite
# notifications and checks and whatnot.  On *success*, call _start_dump; on
# failure, call other appropriate methods.
sub _get_new_volume {
    my $self = shift;

    $self->_log_volume_done();
    $self->{'device'} = undef;

    # release first, if necessary
    if ($self->{'reservation'}) {
	my $res = $self->{'reservation'};

	$self->{'reservation'} = undef;
	$self->{'device'} = undef;

	$res->release(finished_cb => sub {
	    my ($error) = @_;

	    if ($error) {
		$self->_operation_failed($error);
	    } else {
		$self->_get_new_volume();
	    }
	});

	return;
    }

    $self->{'devhandling'}->get_volume(volume_cb => sub { $self->_volume_cb(@_); });
}

sub _volume_cb  {
    my $self = shift;
    my ($scan_error, $request_denied_reason, $reservation,
	$new_label, $access_mode, $is_new) = @_;

    # note that we prefer the request_denied_reason over the scan error.  If
    # both occurred, then the results of the scan are immaterial -- we
    # shouldn't have been looking for a new volume anyway.

    if ($request_denied_reason) {
	$self->_operation_failed($request_denied_reason);
	return;
    }

    if ($scan_error) {
	# we had permission to use a tape, but didn't find a tape, so we need
	# to notify of such
	$self->{'feedback'}->notif_new_tape(
	    error => $scan_error,
	    volume_label => undef);

	$self->_operation_failed($scan_error);
	return;
    }

    $self->dbg("got new volume; writing new label");

    # from here on, if an error occurs, we must send notif_new_tape, and look
    # for a new volume
    $self->{'reservation'} = $reservation;
    $self->{'device_size'} = 0;
    my $device = $self->{'device'} = $reservation->{'device'};

    # turn on verbose logging now, if we need it
    if ($self->{'debug'}) {
	$reservation->{'device'}->property_set("verbose", 1);
    }

    # read the label once, to get a "before" snapshot (see below)
    my $old_label;
    my $old_timestamp;
    if (!$is_new) {
	if (($device->status & ~$DEVICE_STATUS_VOLUME_UNLABELED)
	    && !($device->status & $DEVICE_STATUS_VOLUME_UNLABELED)) {
	    $self->{'feedback'}->notif_new_tape(
		error => "while reading label on new volume: " . $device->error_or_status(),
		volume_label => undef);

	    return $self->_get_new_volume();
	}
	$old_label = $device->volume_label;
	$old_timestamp = $device->volume_time;
    }

    # inform the xdt about this new device before starting it
    $self->{'xdt'}->use_device($device);

    if (!$device->start($access_mode, $new_label, $self->{'write_timestamp'})) {
	# try reading the label to see whether we erased the tape
	my $erased = 0;
	CHECK_READ_LABEL: {
	    # don't worry about erasing new tapes
	    if ($is_new) {
		last CHECK_READ_LABEL;
	    }

	    $device->read_label();

	    # does the device think something is broken now?
	    if (($device->status & ~$DEVICE_STATUS_VOLUME_UNLABELED)
		and !($device->status & $DEVICE_STATUS_VOLUME_UNLABELED)) {
		$erased = 1;
		last CHECK_READ_LABEL;
	    }

	    # has the label changed?
	    my $vol_label = $device->volume_label;
	    if ((!defined $old_label and defined $vol_label)
		or (defined $old_label and !defined $vol_label)
		or (defined $old_label and $old_label ne $vol_label)) {
		$erased = 1;
		last CHECK_READ_LABEL;
	    }

	    # has the timestamp changed?
	    my $vol_timestamp = $device->volume_time;
	    if ((!defined $old_timestamp and defined $vol_timestamp)
		or (defined $old_timestamp and !defined $vol_timestamp)
		or (defined $old_timestamp and $old_timestamp ne $vol_timestamp)) {
		$erased = 1;
		last CHECK_READ_LABEL;
	    }
	}

	$self->{'feedback'}->notif_new_tape(
	    error => "while labeling new volume: " . $device->error_or_status(),
	    volume_label => $erased? $new_label : undef);

	return $self->_get_new_volume();
    }

    # success!
    $self->{'feedback'}->notif_new_tape(
	error => undef,
	volume_label => $new_label);

    # notify the changer that we've labeled the tape, and start the part.
    my $label_set_cb = make_cb(label_set_cb => sub {
	my ($err) = @_;
	if ($err) {
	    $self->{'feedback'}->notif_log_info(
		message => "Error from set_label: $err");
	    # fall through to start_part anyway...
	}
	return $self->_start_part();
    });
    $self->{'reservation'}->set_label(label => $new_label,
	finished_cb => $label_set_cb);
}

sub dbg {
    my ($self, $msg) = @_;
    if ($self->{'debug'}) {
	debug("Amanda::Taper::Scribe: $msg");
    }
}

##
## Feedback
##

package Amanda::Taper::Scribe::Feedback;

# request permission to use a volume.
#
# $params{'perm_cb'} - callback taking one argument: an error message or 'undef'
sub request_volume_permission {
    my $self = shift;
    my %params = @_;

    # sure, you can have as many volumes as you want!
    $params{'perm_cb'}->(undef);
}

sub notif_new_tape { }
sub notif_part_done { }
sub notif_log_info { }

##
## Device Handling
##

package Amanda::Taper::Scribe::DevHandling;

# This class handles scanning for volumes, requesting permission for those
# volumes (the driver likes to feel like it's in control), and providing those
# volumes to the scribe on request.  These can all happen independently, but
# the scribe cannot begin writing to a volume until all three have finished.
# That is: the scan is finished, the driver has given its permission, and the
# scribe has requested a volume.
#
# On start, the class starts scanning immediately, even though the scribe has
# not requested a volume.  Subsequently, a new scan does not begin until the
# scribe requests a volume.
#
# This class is "private" to Amanda::Taper::Scribe, so it is documented in
# comments, rather than POD.

# Create a new DevHandling object.  Params are taperscan and feedback.
sub new {
    my $class = shift;
    my %params = @_;

    my $self = {
	taperscan => $params{'taperscan'},
	feedback => $params{'feedback'},

	# is a scan currently running, or completed?
	scan_running => 0,
	scan_finished => 0,
	scan_error => undef,

	# scan results
	reservation => undef,
	device => undef,
	volume_label => undef,

	# requests for permissiont to use a new volume
	request_pending => 0,
	request_complete => 0,
	request_denied_reason => undef,

	volume_cb => undef, # callback for get_volume
	start_finished_cb => undef, # callback for start
    };

    return bless ($self, $class);
}

## public methods

# Called at scribe startup, this starts the instance off with a scan.
sub start {
    my $self = shift;
    my %params = @_;

    $self->{'start_finished_cb'} = $params{'finished_cb'};
    $self->_start_scanning();
}

# Get an open, started device and label to start writing to.  The
# volume_callback takes the following arguments:
#   $scan_error -- error message, or undef if no error occurred
#   $request_denied_reason -- reason volume request was denied, or undef
#   $reservation -- Amanda::Changer reservation
#   $device -- open, started device
# It is the responsibility of the caller to close the device and release the
# reservation when finished.  If $scan_error or $request_denied_reason are
# defined, then $reservation and $device will be undef.
sub get_volume {
    my $self = shift;
    my (%params) = @_;

    die "already processing a volume request"
	if ($self->{'volume_cb'});

    $self->{'volume_cb'} = $params{'volume_cb'};

    # kick off the relevant processes, if they're not already running
    $self->_start_scanning();
    $self->_start_request();

    $self->_maybe_callback();
}

# take a peek at the device we have, for which permission has not yet been
# granted.  This will be undefined before the taperscan completes AND after
# the volume_cb has been called.
sub peek_device {
    my $self = shift;

    return $self->{'device'};
}

## private methods

sub _start_scanning {
    my $self = shift;

    return if $self->{'scan_running'} or $self->{'scan_finished'};

    $self->{'scan_running'} = 1;

    $self->{'taperscan'}->scan(result_cb => sub {
	my ($error, $reservation, $volume_label, $access_mode, $is_new) = @_;

	$self->{'scan_running'} = 0;
	$self->{'scan_finished'} = 1;

	if ($error) {
	    $self->{'scan_error'} = $error;
	} else {
	    $self->{'reservation'} = $reservation;
	    $self->{'device'} = $reservation->{'device'};
	    $self->{'volume_label'} = $volume_label;
	    $self->{'access_mode'} = $access_mode;
	    $self->{'is_new'} = $access_mode;
	}

	if (!$error and $is_new) {
	    $self->{'feedback'}->notif_log_info(
		message => "Will write new label `$volume_label' to new tape");
	}

	$self->_maybe_callback();
    });
}

sub _start_request {
    my $self = shift;

    return if $self->{'request_pending'} or $self->{'request_complete'};

    $self->{'request_pending'} = 1;

    $self->{'feedback'}->request_volume_permission(perm_cb => sub {
	my ($refusal_reason) = @_;

	$self->{'request_pending'} = 0;
	$self->{'request_complete'} = 1;
	$self->{'request_denied_reason'} = $refusal_reason;

	$self->_maybe_callback();
    });
}

sub _maybe_callback {
    my $self = shift;

    # if we have any kind of error, release the reservation and come back
    # later
    if (($self->{'scan_error'} or $self->{'request_denied_reason'}) and $self->{'reservation'}) {
	$self->{'device'} = undef;

	$self->{'reservation'}->release(finished_cb => sub {
	    my ($error) = @_;

	    # so many errors, so little time..
	    if ($error) {
		if ($self->{'scan_error'}) {
		    warning("ignoring error releasing reservation ($error) after a scan error");
		} else {
		    $self->{'scan_error'} = $error;
		}
	    }

	    $self->{'reservation'} = undef;
	    $self->_maybe_callback();
	});

	return;
    }

    # if we are just starting up, call the finished_cb given to start()
    if (defined $self->{'start_finished_cb'} and $self->{'scan_finished'}) {
	my $cb = $self->{'start_finished_cb'};
	$self->{'start_finished_cb'} = undef;

	$cb->($self->{'scan_error'});
    }

    # if the volume_cb is good to get called, call it and reset to the ground state
    if ($self->{'volume_cb'} and $self->{'scan_finished'} and $self->{'request_complete'}) {
	# get the cb and its arguments lined up before calling it..
	my $volume_cb = $self->{'volume_cb'};
	my @volume_cb_args = (
	    $self->{'scan_error'},
	    $self->{'request_denied_reason'},
	    $self->{'reservation'},
	    $self->{'volume_label'},
	    $self->{'access_mode'},
	    $self->{'is_new'},
	);

	# reset everything and prepare for a new scan
	$self->{'scan_finished'} = 0;

	$self->{'reservation'} = undef;
	$self->{'device'} = undef;
	$self->{'volume_label'} = undef;

	$self->{'request_complete'} = 0;
	$self->{'volume_cb'} = undef;

	$volume_cb->(@volume_cb_args);
    }
}

1;
