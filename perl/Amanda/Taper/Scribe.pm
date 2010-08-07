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

=head1 NAME

Amanda::Taper::Scribe

=head1 SYNOPSIS

  step start_scribe => sub {
      my $scribe = Amanda::Taper::Scribe->new(
	    taperscan => $taperscan_algo,
	    feedback => $feedback_obj);
    $scribe->start(
	write_timestamp => $write_timestamp,
	finished_cb => $steps->{'start_xfer'});
  };

  step start_xfer => sub {
    my ($err) = @_;
    my $xfer_dest = $scribe->get_xfer_dest(
	max_memory => 64 * 1024,
	can_cache_inform => 0,
	part_size => 150 * 1024**2,
	part_cache_type => 'disk',
	part_cache_dir => "$tmpdir/splitbuffer",
	part_cache_max_size => 20 * 1024**2);
    # .. set up the rest of the transfer ..
    $xfer->start(sub {
        my ($src, $msg, $xfer) = @_;
        $scribe->handle_xmsg($src, $msg, $xfer);
        # .. any other processing ..
    };
    # tell the scribe to start dumping via this transfer
    $scribe->start_dump(
	xfer => $xfer,
        dump_header => $hdr,
        dump_cb => $steps->{'dump_cb'});
  };

  step dump_cb => sub {
      my %params = @_;
      # .. handle dump results ..
      print "DONE\n";
      $finished_cb->();
  };


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

=head3 Get device

Call C<get_device> to get the first device the xfer will be working with.

  $device = $scribe->get_device();

This method must be called after C<start> has completed.

=head3 Check device compatibily for the data path

Call C<check_data_path>, supplying the data_path requested by the user.

  if (my $err = $scribe->check_data_path($data_path)) {
      # handle error message
  }

This method must be called after C<start> has completed and before
C<get_xfer_dest> is called. It returns C<undef> on success or an error message
if the supplied C<data_path> is incompatible with the device.  This is mainly
used to detect when a DirectTCP dump is going to a non-DirectTCP device.

=head3 Get a Transfer Destination

Call C<get_xfer_dest> to get the transfer element, supplying information on how
the dump should be split:

  $xdest = $scribe->get_xfer_dest(
        max_memory => $max_memory,
        # .. splitting parameters
        );

This method must be called after C<start> has completed, and will always return
a transfer element immediately.  The underlying C<Amanda::Xfer::Dest::Taper>
handles device streaming properly.  It uses C<max_memory> bytes of memory for
this purpose.

The splitting parameters to C<get_xfer_dest> are:

=over 4

=item C<part_size>

the split part size to use, or 0 for no splitting

=item C<part_cache_type>

when caching, the kind of caching to perform ('disk', 'memory' or the default,
'none')

=item C<part_cache_dir>

the directory to use for disk caching

=item C<part_cache_max_size>

the maximum part size to use when caching

=item C<can_cache_inform>

true if the transfer source can call the destination's C<cache_inform> method
(e.g., C<Amanda::Xfer::Source::Holding>).

=back

The first four of these parameters correspond exactly to the eponymous tapetype
configuration parameters, and have the same default values (when omitted or
C<undef>).  The method will take this information, along with details of the
device it intends to use, and set up the transfer destination.

The utility function C<get_splitting_args_from_config> can determine the
appropriate C<get_xfer_dest> splitting parameters based on a
few Amanda configuration parameters.  If a parameter was not seen in the
configuration, it should be omitted or passed as C<undef>.  The function
returns a hash to pass to get_xfer_dest, although that hash may have an
C<warning> key containing a message if there is a problem that the user
should know about.

  use Amanda::Taper::Scribe qw( get_splitting_args_from_config );
  my %splitting_args = get_splitting_args_from_config(
    # Amanda dumptype configuration parameters,
    dle_tape_splitsize => ..,
    dle_split_diskbuffer => ..,
    dle_fallback_splitsize => ..,
    dle_allow_split => ..,
    # Amanda tapetype configuration parameters,
    part_size => ..,
    part_cache_type => ..,
    part_cache_dir => ..,
    part_cache_max_size => ..,
  );
  if ($splitting_args{'error'}) { .. }

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
	config_denial_message => $cdm,
        size => $size,
        duration => $duration,
	total_duration => $total_duration,
	nparts => $nparts);

All parameters will be present on every call, although the order is not
guaranteed.

The C<result> is one of C<"FAILED">, C<"PARTIAL">, or C<"DONE">.  Even when
C<dump_cb> reports a fatal error, C<result> may be C<"PARTIAL"> if some data
was written successfully.

The C<device_error> key points to a list of errors, each given as a string,
that describe what went wrong to cause the dump to fail.  The
C<config_denial_message> parrots the reason provided by C<$perm_cb> (see below)
for denying use of a new tape if the cause was 'config', and is C<undef>
otherwise.

The final parameters, C<size> (in bytes), C<duration>, C<total_duration> (in
seconds), and C<nparts> describe the total transfer, and are a sum of all of
the parts written to the device.  Note that C<nparts> does not include any
empty trailing parts.  Note that C<duration> does not include time spent
operating the changer, while C<total_duration> reflects the time from the
C<start_dump> call to the invocation of the C<dump_cb>.

=head3 Cancelling a Dump

After you have requested a transfer destination, the scribe is poised to begin the
transfer.  If you cannot actually perform the transfer for some reason, you'll need
to go through the motions all the same, but cancel the operation immediately.  That
can be done by calling C<cancel_dump>:

  $scribe->cancel_dump(
	xfer => $xfer,
	dump_cb => $dump_cb);

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

where the C<perm_cb> is a callback which expects two arguments.  Those
arguments should be C<undef> if permission is granted, or the cause ('config'
or 'error') and reason, e.g., 

  $perm_cb->('config', "only 3 tapes allowed by runtapes");
  $perm_cb->('error', "catalog access failed");

A cause of 'config' indicates that the denial is due to the user's
configuration, and thus should not be presented as an error.  The default
implementation always calls C<< perm_cb->(undef, undef) >>.

All of the remaining methods are notifications, and do not take a
callback.

  $fb->scribe_notif_new_tape(
        error => $error,
        volume_label => $volume_label);

The Scribe calls C<scribe_notif_new_tape> when a new volume is started.  If the
C<volume_label> is undefined, then the volume was not successfully
relabled, and its previous contents may still be available.  If C<error>
is defined, then no useful data was written to the volume.  Note that
C<error> and C<volume_label> may I<both> be defined if the previous
contents of the volume were erased, but no useful, new data was written
to the volume.

This method will be called exactly once for every call to
C<request_volume_permission> that calls back with C<< perm_cb->(undef, undef)
>>.

  $fb->scribe_notif_part_done(
        partnum => $partnum,
        fileno => $fileno,
        successful => $successful,
        size => $size,
        duration => $duration);

The Scribe calls C<scribe_notif_part_done> for each part written to the volume,
including partial parts.  If the part was not written successfully, then
C<successful> is false.  The C<size> is in bytes, and the C<duration> is
a floating-point number of seconds.  If a part fails before a new device
file is created, then C<fileno> may be zero.

Finally, the Scribe sends a few historically significant trace log messages
via C<scribe_notif_log_info>:

  $fb->scribe_notif_log_info(
	message => $message);

A typical Feedback subclass might begin like this:

  package main::Feedback;
  use base 'Amanda::Taper::Scribe::Feedback';

  sub request_volume_permission {
    my $self = shift;
    my %params = @_;

    $params{'perm_cb'}->("error", "NO VOLUMES FOR YOU!");
  }

=cut

package Amanda::Taper::Scribe;

use strict;
use warnings;
use Carp;

use Amanda::Xfer qw( :constants );
use Amanda::Device qw( :constants );
use Amanda::Header;
use Amanda::Debug qw( :logging );
use Amanda::MainLoop;
use Amanda::Config;
use base qw( Exporter );

our @EXPORT_OK = qw( get_splitting_args_from_config );

sub new {
    my $class = shift;
    my %params = @_;

    my $decide_debug = $Amanda::Config::debug_taper || $params{'debug'};
    for my $rq_param qw(taperscan feedback) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }

    my $self = {
	feedback => $params{'feedback'},
	debug => $decide_debug,
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
	device_at_eom => undef, # device still exists, but is full

        # callback passed to start_dump
	dump_cb => undef,

	# information for the current dumpfile
	dump_header => undef,
	retry_part_on_peom => undef,
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
	config_denial_message => undef,
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

    my $devhandling_cb = make_cb(devhandling_cb => sub {
	my ($error) = @_;
	push @errors, $error if $error;

	$error = join("; ", @errors) if @errors >= 1;
	$params{'finished_cb'}->($error);
    });

    my $cleanup_cb = make_cb(cleanup_cb => sub {
	my ($error) = @_;
	push @errors, $error if $error;

	$self->{'reservation'} = undef;
	$self->{'device'} = undef;
	$self->{'devhandling'}->quit(finished_cb => $devhandling_cb);
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

sub get_device {
    my $self = shift;

    # Can return a device we already have, or "peek" at the
    # DevHandling object's device.
    # It might not have right permission on the device.

    my $device;
    if (defined $self->{'device'}) {
	$device = $self->{'device'};
    } else {
	$device = $self->{'devhandling'}->peek_device();
    }
    return $device;
}

sub check_data_path {
    my $self = shift;
    my $data_path = shift;

    my $device = $self->get_device();

    if (!defined $device) {
	die "no device is available to check the data_path";
    }

    my $use_directtcp = $device->directtcp_supported();

    my $xdt;
    if (!$use_directtcp) {
	if ($data_path eq 'DIRECTTCP') {
	    return "Can't dump DIRECTTCP data-path dle to a device ('" .
		   $device->device_name .
		   "') that doesn't support it";
	}
    }
    return undef;
}

# Get a transfer destination; does not use a callback
sub get_xfer_dest {
    my $self = shift;
    my %params = @_;

    for my $rq_param qw(max_memory) {
	croak "required parameter '$rq_param' missing"
	    unless exists $params{$rq_param};
    }

    die "not yet started"
	unless $self->{'write_timestamp'} and $self->{'started'};
    die "xfer element already returned"
	if ($self->{'xdt'});
    die "xfer already running"
	if ($self->{'xfer'});

    $self->{'xfer'} = undef;
    $self->{'xdt'} = undef;
    $self->{'size'} = 0;
    $self->{'duration'} = 0.0;
    $self->{'nparts'} = undef;
    $self->{'dump_start_time'} = undef;
    $self->{'last_part_successful'} = 1;
    $self->{'started_writing'} = 0;
    $self->{'device_errors'} = [];
    $self->{'config_denial_message'} = undef;

    # set the callback
    $self->{'dump_cb'} = undef;
    $self->{'retry_part_on_peom'} = 1;
    $self->{'start_part_on_xdt_ready'} = 0;

    # start getting parameters together to determine what kind of splitting
    # and caching we're going to do
    my $part_size = $params{'part_size'} || 0;
    my ($use_mem_cache, $disk_cache_dirname) = (0, undef);
    my $can_cache_inform = $params{'can_cache_inform'};
    my $part_cache_type = $params{'part_cache_type'} || 'none';

    my $xdt_first_dev = $self->get_device();
    if (!defined $xdt_first_dev) {
	die "no device is available to create an xfer_dest";
    }
    my $leom_supported = $xdt_first_dev->property_get("leom");
    my $use_directtcp = $xdt_first_dev->directtcp_supported();

    # figure out the destination type we'll use, based on the circumstances
    my ($dest_type, $dest_text);
    if ($use_directtcp) {
	$dest_type = 'directtcp';
	$dest_text = "using DirectTCP";
    } elsif ($can_cache_inform && $leom_supported) {
	$dest_type = 'splitter';
	$dest_text = "using LEOM (falling back to holding disk as cache)";
    } elsif ($leom_supported) {
	$dest_type = 'splitter';
	$dest_text = "using LEOM detection (no caching)";
    } elsif ($can_cache_inform) {
	$dest_type = 'splitter';
	$dest_text = "using cache_inform";
    } elsif ($part_cache_type ne 'none') {
	$dest_type = 'cacher';

	# we'll be caching, so apply the maximum size
	my $part_cache_max_size = $params{'part_cache_max_size'} || 0;
	$part_size = $part_cache_max_size
	    if ($part_cache_max_size and $part_cache_max_size < $part_size);

	# and figure out what kind of caching to apply
	if ($part_cache_type eq 'memory') {
	    $use_mem_cache = 1;
	} else {
	    # note that we assume this has already been checked; if it's wrong,
	    # the xfer element will just fail immediately
	    $disk_cache_dirname = $params{'part_cache_dir'};
	}
	$dest_text = "using cache type '$part_cache_type'";
    } else {
	$dest_type = 'splitter';
	$dest_text = "using no cache (PEOM will be fatal)";

	# no directtcp, no caching, no cache_inform, and no LEOM, so a PEOM will be fatal
	$self->{'retry_part_on_peom'} = 0;
    }

    debug("Amanda::Taper::Scribe preparing to write, part size $part_size, "
	. "$dest_text ($dest_type) "
	. ($leom_supported? " (LEOM supported)" : " (no LEOM)"));

    # set the device to verbose logging if we're in debug mode
    if ($self->{'debug'}) {
	$xdt_first_dev->property_set("verbose", 1);
    }

    my $xdt;
    if ($dest_type eq 'directtcp') {
	$xdt = Amanda::Xfer::Dest::Taper::DirectTCP->new(
	    $xdt_first_dev, $part_size);
	$self->{'xdt_ready'} = 0; # xdt isn't ready until we get XMSG_READY
    } elsif ($dest_type eq 'splitter') {
	$xdt = Amanda::Xfer::Dest::Taper::Splitter->new(
	    $xdt_first_dev, $params{'max_memory'}, $part_size, $can_cache_inform);
	$self->{'xdt_ready'} = 1; # xdt is ready immediately
    } else {
	$xdt = Amanda::Xfer::Dest::Taper::Cacher->new(
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

    # XXX The cancel should call dump_cb, but right now the xfer stays hung in
    # accept.  So we leave the xfer to its hang, and dump_cb is called and xdt
    # and xfer are set to undef.  This should be fixed in 3.2.

    $self->{'xfer'}->cancel();

    $self->{'dump_cb'}->(
	result => "FAILED",
	device_errors => [],
	config_denial_message => undef,
	size => 0,
	duration => 0.0,
	total_duration => 0,
	nparts => 0);
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
    # one, then defer this start_part call until we do.  The device may still
    # exist, but be at EOM, if the last dump failed at EOM and was not retried
    # on a new volume.
    if (!$self->{'device'} or $self->{'device_at_eom'}) {
	# _get_new_volume calls _start_part when it has a new volume in hand
	return $self->_get_new_volume();
    }

    # if the dump wasn't successful, and we're not splitting, then bail out.  It's
    # up to higher-level components to re-try this dump on a new volume, if desired.
    # Note that this should be caught in the XMSG_PART_DONE handler -- this is just
    # here for backup.
    if (!$self->{'last_part_successful'} and !$self->{'retry_part_on_peom'}) {
	$self->_operation_failed(device_error => "No space left on device (uncaught)");
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
	$self->{'feedback'}->scribe_notif_part_done(
	    partnum => $msg->{'partnum'},
	    fileno => $msg->{'fileno'},
	    successful => $msg->{'successful'},
	    size => $msg->{'size'},
	    duration => $msg->{'duration'});

	# increment nparts here, so empty parts are not counted
	$self->{'nparts'} = $msg->{'partnum'};
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
		if (!$self->{'retry_part_on_peom'}) {
		    # mark this device as at EOM, since we are not going to look
		    # for another one yet
		    $self->{'device_at_eom'} = 1;

		    my $msg = "No space left on device";
		    if ($self->{'device'}->status() != $DEVICE_STATUS_SUCCESS) {
			$msg = $self->{'device'}->error_or_status();
		    }
		    $self->_operation_failed(device_error => $msg);
		    return;
		}

		# log a message for amreport
		$self->{'feedback'}->scribe_notif_log_info(
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
		$self->_operation_failed(device_error => $msg);
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
    $self->_operation_failed(device_error => $msg->{'message'});
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
    if (@{$self->{'device_errors'}} or $self->{'config_denial_message'}) {
	$result = $self->{'started_writing'}? 'PARTIAL' : 'FAILED';
    } else {
	$result = 'DONE';
    }

    my $dump_cb = $self->{'dump_cb'};
    my %dump_cb_args = (
	result => $result,
	device_errors => $self->{'device_errors'},
	config_denial_message => $self->{'config_denial_message'},
	size => $self->{'size'},
	duration => $self->{'duration'},
	total_duration => time - $self->{'dump_start_time'},
	nparts => $self->{'nparts'});

    # reset everything and let the original caller know we're done
    $self->{'xfer'} = undef;
    $self->{'xdt'} = undef;
    $self->{'dump_header'} = undef;
    $self->{'dump_cb'} = undef;
    $self->{'size'} = 0;
    $self->{'duration'} = 0.0;
    $self->{'nparts'} = undef;
    $self->{'dump_start_time'} = undef;
    $self->{'device_errors'} = [];
    $self->{'config_denial_message'} = undef;

    # and call the callback
    $dump_cb->(%dump_cb_args);
}

# keyword parameters are utilities to the caller: either specify
# device_error to add to the device_errors list or config_denial_message
# to set the corresponding key in $self.
sub _operation_failed {
    my $self = shift;
    my %params = @_;

    my $error_message = $params{'device_error'}
		     || $params{'config_denial_message'}
		     || 'no reason';
    $self->dbg("operation failed: $error_message");

    # tuck the message away as desired
    push @{$self->{'device_errors'}}, $params{'device_error'}
	if defined $params{'device_error'};
    $self->{'config_denial_message'} = $params{'config_denial_message'} 
	if $params{'config_denial_message'};

    # cancelling the xdt will eventually cause an XMSG_DONE, which will notice
    # the error and set the result correctly; but if there's no xfer, then we
    # can just call _dump_done directly.
    if (defined $self->{'xfer'}) {
        $self->dbg("cancelling the transfer: $error_message");

	$self->{'xfer'}->cancel();
    } else {
        if (defined $self->{'dump_cb'}) {
            # _dump_done constructs the dump_cb from $self parameters
            $self->_dump_done();
        } else {
            die "error with no callback to handle it: $error_message";
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
	$self->{'feedback'}->scribe_notif_log_info(
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
    $self->{'device_at_eom'} = 0;

    # release first, if necessary
    if ($self->{'reservation'}) {
	my $res = $self->{'reservation'};

	$self->{'reservation'} = undef;
	$self->{'device'} = undef;

	$res->release(finished_cb => sub {
	    my ($error) = @_;

	    if ($error) {
		$self->_operation_failed(device_error => $error);
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
    my ($scan_error, $config_denial_message, $error_denial_message, $reservation,
	$new_label, $access_mode, $is_new) = @_;

    # note that we prefer the request_denied_* info over the scan error.  If
    # both occurred, then the results of the scan are immaterial -- we
    # shouldn't have been looking for a new volume anyway.

    if ($config_denial_message) {
	$self->_operation_failed(config_denial_message => $config_denial_message);
	return;
    }

    if ($error_denial_message) {
	$self->_operation_failed(device_error => $error_denial_message);
	return;
    }

    if ($scan_error) {
	# we had permission to use a tape, but didn't find a tape, so we need
	# to notify of such
	$self->{'feedback'}->scribe_notif_new_tape(
	    error => $scan_error,
	    volume_label => undef);

	$self->_operation_failed(device_error => $scan_error);
	return;
    }

    $self->dbg("got new volume; writing new label");

    # from here on, if an error occurs, we must send scribe_notif_new_tape, and look
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
	    $self->{'feedback'}->scribe_notif_new_tape(
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

	$self->{'feedback'}->scribe_notif_new_tape(
	    error => "while labeling new volume: " . $device->error_or_status(),
	    volume_label => $erased? $new_label : undef);

	return $self->_get_new_volume();
    }

    # success!
    $self->{'feedback'}->scribe_notif_new_tape(
	error => undef,
	volume_label => $new_label);

    # notify the changer that we've labeled the tape, and start the part.
    my $label_set_cb = make_cb(label_set_cb => sub {
	my ($err) = @_;
	if ($err) {
	    $self->{'feedback'}->scribe_notif_log_info(
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

sub get_splitting_args_from_config {
    my %params = @_;

    use Data::Dumper;
    my %splitting_args;

    # if dle_splitting is false, then we don't split - easy.
    if (defined $params{'dle_allow_split'} and !$params{'dle_allow_split'}) {
	return ();
    }

    # utility for below
    my $have_space = sub {
	my ($dirname, $part_size) = @_;

	use Carp;
	my $fsusage = Amanda::Util::get_fs_usage($dirname);
	confess "$dirname" if (!$fsusage);

	my $avail = $fsusage->{'blocks'} * $fsusage->{'bavail'};
	if ($avail < $part_size) {
	    Amanda::Debug::debug("disk cache has $avail bytes available on $dirname, but " .
				 "needs $part_size");
	    return 0;
	} else {
	    return 1;
	}
    };

    # if any of the dle_* parameters are set, use those to set the part_*
    # parameters, which are emptied out first.
    if (defined $params{'dle_tape_splitsize'} or
	defined $params{'dle_split_diskbuffer'} or
	defined $params{'dle_fallback_splitsize'}) {

	$params{'part_size'} = $params{'dle_tape_splitsize'} || 0;
	$params{'part_cache_type'} = 'none';
	$params{'part_cache_dir'} = undef;
	$params{'part_cache_max_size'} = undef;

	# part cache type is memory unless we have a split_diskbuffer that fits the bill
	if ($params{'part_size'}) {
	    $params{'part_cache_type'} = 'memory';
	    if (defined $params{'dle_split_diskbuffer'}
		    and -d $params{'dle_split_diskbuffer'}) {
		if ($have_space->($params{'dle_split_diskbuffer'}, $params{'part_size'})) {
		    # disk cache checks out, so use it
		    $params{'part_cache_type'} = 'disk';
		    $params{'part_cache_dir'} = $params{'dle_split_diskbuffer'};
		} else {
		    my $msg = "falling back to memory buffer for splitting: " .
				"insufficient space in disk cache directory";
		    $splitting_args{'warning'} = $msg;
		}
	    }
	}

	if ($params{'part_cache_type'} eq 'memory') {
	    # fall back to 10M if fallback size is not given
	    $params{'part_cache_max_size'} = $params{'dle_fallback_splitsize'} || 10*1024*1024;
	}
    } else {
	my $ps = $params{'part_size'};
	my $pcms = $params{'part_cache_max_size'};
	$ps = $pcms if (!defined $ps or (defined $pcms and $pcms < $ps));

	# fail back from 'disk' to 'none' if the disk isn't set up correctly
	if (defined $params{'part_cache_type'} and
		    $params{'part_cache_type'} eq 'disk') {
	    my $warning;
	    if (!$params{'part_cache_dir'}) {
		$warning = "no part_cache_dir specified; "
			    . "using part_cache_type 'none'";
	    } elsif (!-d $params{'part_cache_dir'}) {
		$warning = "part_cache_dir '$params{part_cache_dir} "
			    . "does not exist; using part_cache_type 'none'";
	    } elsif (!$have_space->($params{'part_cache_dir'}, $ps)) {
		$warning = "part_cache_dir '$params{part_cache_dir} "
			    . "has insufficient space; using part_cache_type 'none'";
	    }

	    if (defined $warning) {
		$splitting_args{'warning'} = $warning;
		$params{'part_cache_type'} = 'none';
		delete $params{'part_cache_dir'};
	    }
	}
    }

    $splitting_args{'part_size'} = $params{'part_size'}
	if defined($params{'part_size'});
    $splitting_args{'part_cache_type'} = $params{'part_cache_type'}
	if defined($params{'part_cache_type'});
    $splitting_args{'part_cache_dir'} = $params{'part_cache_dir'}
	if defined($params{'part_cache_dir'});
    $splitting_args{'part_cache_max_size'} = $params{'part_cache_max_size'}
	if defined($params{'part_cache_max_size'});

    return %splitting_args;
}
##
## Feedback
##

package Amanda::Taper::Scribe::Feedback;

sub request_volume_permission {
    my $self = shift;
    my %params = @_;

    # sure, you can have as many volumes as you want!
    $params{'perm_cb'}->(undef, undef);
}

sub scribe_notif_new_tape { }
sub scribe_notif_part_done { }
sub scribe_notif_log_info { }

##
## Device Handling
##

package Amanda::Taper::Scribe::DevHandling;
use Amanda::MainLoop;
use Carp;

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
	request_denied => 0,
	config_denial_message => undef,
	error_denial_message => undef,

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

sub quit {
    my $self = shift;
    my %params = @_;

    for my $rq_param qw(finished_cb) {
	croak "required parameter '$rq_param' mising"
	    unless exists $params{$rq_param};
    }

    # since there's little other option than to barrel on through the
    # quitting procedure, quit() just accumulates its error messages
    # and, if necessary, concantenates them for the finished_cb.
    my @errors;

    my $cleanup_cb = make_cb(cleanup_cb => sub {
	my ($error) = @_;
	push @errors, $error if $error;

	$error = join("; ", @errors) if @errors >= 1;

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

# Get an open, started device and label to start writing to.  The
# volume_callback takes the following arguments:
#   $scan_error -- error message, or undef if no error occurred
#   $config_denial_reason -- config-related reason request was denied, or undef
#   $error_denial_reason -- error-related reason request was denied, or undef
#   $reservation -- Amanda::Changer reservation
#   $device -- open, started device
# It is the responsibility of the caller to close the device and release the
# reservation when finished.  If $scan_error or $request_denied_info are
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
	    $self->{'feedback'}->scribe_notif_log_info(
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
	my ($cause, $message) = @_;
	die "bad cause" unless !defined $cause or $cause =~ /^(error|config)$/;

	$self->{'request_pending'} = 0;
	$self->{'request_complete'} = 1;
	if (defined $cause) {
	    $self->{'request_denied'} = 1;
	    if ($cause eq 'config') {
		$self->{'config_denial_message'} = $message;
	    } elsif ($cause eq 'error') {
		$self->{'error_denial_message'} = $message;
	    }
	}

	$self->_maybe_callback();
    });
}

sub _maybe_callback {
    my $self = shift;

    # if we have any kind of error, release the reservation and come back
    # later
    if (($self->{'scan_error'} or $self->{'request_denied'}) and $self->{'reservation'}) {
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
	    $self->{'config_denial_message'},
	    $self->{'error_denial_message'},
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
	$self->{'request_denied'} = 0;
	$self->{'config_denial_message'} = undef;
	$self->{'error_denial_message'} = undef;
	$self->{'volume_cb'} = undef;

	$volume_cb->(@volume_cb_args);
    }
}

1;
