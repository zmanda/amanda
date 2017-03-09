# Copyright (c) 2009-2012 Zmanda, Inc.  All Rights Reserved.
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

=head1 NAME

Amanda::Chunker::Scribe

=head1 SYNOPSIS

  step start_scribe => sub {
      my $scribe = Amanda::Chunker::Scribe->new(
	    feedback => $feedback_obj);
    $scribe->start(
	write_timestamp => $write_timestamp);
  };

  step start_xfer => sub {
    my ($err) = @_;
    my $xfer_dest = $scribe->get_xfer_dest(
	max_memory => 32*1024*64);
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
        dump_cb => $steps->{'dump_cb'},
	filename => $filename,
	use_bytes => $use_bytes,
	chunk_size => $chunk_size);
  };

  step dump_cb => sub {
      my %params = @_;
      # .. handle dump results ..
      print "DONE\n";
      $finished_cb->();
  };


=head1 OVERVIEW

This package provides a high-level abstraction of Amanda's procedure for
writing dumpfiles to holding disk.

Amanda writes a sequence of chunkfile to a sequence of holding disk.

The details of efficiently write to holding disk are handled
by the low-level C<Amanda::Xfer::Dest::Chunker> subclasses.  The Scribe creates
an instance of the appropriate subclass. It calls a number of
C<Amanda::Chunker::Scribe::Feedback> methods to indicate the status of the dump
process and to request permission to use more holding disk.

=head1 OPERATING A SCRIBE

The C<Amanda::Chunker::Scribe> constructor takes one arguments:
C<feedback>.  It specifies the C<Feedback> object that will receive
notifications from the Scribe (see below).

  my $scribe = Amanda::Chunker::Scribe->new(
        feedback => $my_feedback);

Once the object is in place, call its C<start> method.

=head2 START THE SCRIBE

Start the scribe's operation by calling its C<start> method.
The method takes one parameters:

  $scribe->start(
        write_timestamp => $ts);

The timestamp will be written to each holding disk written by the Scribe

=head2 SET UP A TRANSFER

Once the Scribe is started, begin transferring a dumpfile.  This is a
three-step process: first, get an C<Amanda::Xfer::Dest::Chunker> object from the
Scribe, then start the transfer, and finally let the Scribe know that the
transfer has started.  Note that the Scribe supplies and manages the transfer
destination, but the transfer itself remains the responsibility of the caller.

The parameter to C<get_xfer_dest> are:

=over 4

=item C<max_memory>

maximun size to use on the holding disk.

=back

=head3 Start the Transfer

Armed with the element returned by C<get_xfer_dest>, the caller should create a
source element and a transfer object and start the transfer.  In order to
manage the holding chunk process, the Scribe needs to be informed, via its
C<handle_xmsg> method, of all transfer messages .  This is usually accomplished
with something like:

  $xfer->start(sub {
      my ($src, $msg, $xfer) = @_;
      $scribe->handle_xmsg($src, $msg, $xfer);
  });

=head3 Inform the Scribe

Once the transfer has started, the Scribe is ready to begin writing chunk to
holding disk.  This is the first moment at which the Scribe needs a header, too.
All of this is supplied to the C<start_dump> method:

  $scribe->start_dump(
      xfer => $xfer,
      dump_header => $hdr,
      filename => $filename,
      use_bytes => $use_bytes,
      chunk_size => $chunk_size,
      dump_cb => $dump_cb);

The c<dump_header> here is the header that will be applied to all chunk of the
dumpfile.  The only field in the header that the Scribe controls is the
cont_filename.  The C<dump_cb> callback passed to C<start_dump> is called when the
dump is completely finished - either successfully or with a fatal error.
Unlike most callbacks, this one takes keyword arguments, since it has so many
parameters.

  $dump_cb->(
        result => $result,
        header_size => $header_size,
        data_size => $data_size,
        total_duration => $total_duration);

All parameters will be present on every call, although the order is not
guaranteed.

The C<result> is one of C<"FAILED">, C<"PARTIAL">, or C<"DONE">.  Even when
C<dump_cb> reports a fatal error, C<result> may be C<"PARTIAL"> if some data
was written successfully.

The C<holding_error> key points to a list of errors, each given as a string,
that describe what went wrong to cause the dump to fail.

The final parameters, C<size> (in bytes), C<total_duration> (in
seconds), and C<nparts> describe the total transfer, and are a sum of all of
the parts written to the device.  C<total_duration> reflects the time from the
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

The C<Amanda::Chunker::Scribe::Feedback> class is intended to be
subclassed by the user.  It provides a number of notification methods
that enable the historical logging and driver/chunker interactions
required by Amanda.  The parent class does nothing of interest, but
allows subclasses to omit methods they do not need.

  $fb->notif_no_room($use_bytes, $mesg);

The C<request_more_disk> method provides a means for the caller
to ask for more holding disk.  It is called as

  $fb->request_more_disk(perm_cb => $cb);

All of the remaining methods are notifications, and do not take a
callback.

  $fb->scribe_notif_no_room();

The Scribe calls C<scribe_notif_no_room> when there is no more space allocated on the holding disk.

Finally, the Scribe sends a few historically significant trace log messages
via C<scribe_notif_log_info>:

  $fb->scribe_notif_log_info(
	message => $message);

A typical Feedback subclass might begin like this:

  package main::Feedback;
  use base 'Amanda::Chunker::Scribe::Feedback';

  sub request_more_disk {
    my $self = shift;
    my %params = @_;

    $self->{'proto'}->send(Amanda::Chunker::Protocol::RQ_MORE_DISK,
        handle => $self->{'handle'});
  }

=cut

package Amanda::Chunker::Scribe;

use strict;
use warnings;
use Carp;

use Amanda::Xfer qw( :constants );
use Amanda::Header;
use Amanda::Debug qw( :logging );
use Amanda::MainLoop;
use Amanda::Tapelist;
use Amanda::Config qw( :getconf config_dir_relative );
use base qw( Exporter );

sub new {
    my $class = shift;
    my %params = @_;

    my $decide_debug = $Amanda::Config::debug_chunker || $params{'debug'};
    for my $rq_param (qw(feedback)) {
	croak "required parameter '$rq_param' missing"
	    unless exists $params{$rq_param};
    }

    my $self = {
	feedback => $params{'feedback'},
	debug => $decide_debug,
	write_timestamp => undef,
	started => 0,

        # callback passed to start_dump
	dump_cb => undef,

	# information for the current dumpfile
	dump_header => undef,
	retry_part_on_peom => undef,
	allow_split => undef,
	xfer => undef,
	xdh => undef,
	xdh_ready => undef,
	size => 0,
	total_duration => 0.0,
	dump_start_time => undef,
	last_part_successful => 0,
	started_writing => 0,
	result => 'DONE',
    };

    return bless ($self, $class);
}

sub start {
    my $self = shift;
    my %params = @_;

    for my $rq_param (qw(write_timestamp)) {
	croak "required parameter '$rq_param' missing"
	    unless exists $params{$rq_param};
    }

    confess "scribe already started" if $self->{'started'};

    $self->dbg("starting");
    $self->{'write_timestamp'} = $params{'write_timestamp'};
    $self->{'started'} = 1;
}

sub quit {
    my $self = shift;
    my %params = @_;

    # since there's little other option than to barrel on through the
    # quitting procedure, quit() just accumulates its error messages
    # and, if necessary, concantenates them for the finished_cb.
    my @errors;

    my $steps = define_steps
	cb_ref => \$params{'finished_cb'};

    step setup => sub {
	$self->dbg("quitting");

	if ($self->{'xfer'}) {
	    $self->{'xfer'}->cancel();
	}

	$steps->{'quit'}->();
    };

    step quit => sub {
	my ($err) = @_;
	push @errors, "$err" if $err;

	my $errmsg = join("; ", @errors) if @errors >= 1;
	$params{'finished_cb'}->($errmsg);
    };
}

# Get a transfer destination; does not use a callback
sub get_xfer_dest {
    my $self = shift;
    my %params = @_;

    for my $rq_param (qw( max_memory )) {
	croak "required parameter '$rq_param' missing"
	    unless exists $params{$rq_param};
    }

    confess "not yet started"
	unless $self->{'write_timestamp'} and $self->{'started'};
    confess "xfer element already returned"
	if ($self->{'xdh'});
    confess "xfer already running"
	if ($self->{'xfer'});

    $self->{'xfer'} = undef;
    $self->{'xdh'} = undef;
    $self->{'header_size'} = 0;
    $self->{'data_size'} = 0;
    $self->{'total_duration'} = 0.0;
    $self->{'dump_start_time'} = undef;
    $self->{'started_writing'} = 0;

    # set the callback
    $self->{'dump_cb'} = undef;

    # start getting parameters together to determine what kind of splitting
    # and caching we're going to do

    debug("Amanda::Chunker::Scribe preparing to write, max_memory $params{'max_memory'}");

    my $xdh;
    $xdh = Amanda::Xfer::Dest::Holding->new(
		$params{'max_memory'});
    $self->{'xdh_ready'} = 0; # xdh isn't ready until we get XMSG_READY

    $self->{'xdh'} = $xdh;

    return $xdh;
}

sub start_dump {
    my $self = shift;
    my %params = @_;

    confess "no xfer dest set up; call get_xfer_dest first"
        unless defined $self->{'xdh'};

    # get the header ready for writing
    $self->{'dump_header'} = $params{'dump_header'};

    $self->{'filename'} = $params{'filename'};
    $self->{'use_bytes'} = $params{'use_bytes'};
    $self->{'chunk_size'} = $params{'chunk_size'};
    $self->{'chunk_bytes'} = 0;
    $self->{'seq'} = 0;

    # set up the dump_cb for when this dump is done, and keep the xfer
    $self->{'dump_cb'} = $params{'dump_cb'};
    $self->{'xfer'} = $params{'xfer'};
    $self->{'dump_start_time'} = time;

    # and start the part
    $self->_start_chunk();
}

sub cancel_dump {
    my $self = shift;
    my %params = @_;

    confess "no xfer dest set up; call get_xfer_dest first"
	unless defined $self->{'xdh'};

    # set up the dump_cb for when this dump is done, and keep the xfer
    $self->{'dump_cb'} = $params{'dump_cb'};
    $self->{'xfer'} = $params{'xfer'};

    # XXX The cancel should call dump_cb, but right now the xfer stays hung in
    # accept.  So we leave the xfer to its hang, and dump_cb is called and xdh
    # and xfer are set to undef.  This should be fixed in 3.2.

    $self->{'xfer'}->cancel();

    $self->{'dump_cb'}->(
	result => "FAILED",
	size => 0,
	total_duration => 0,
	nparts => 0);
    $self->{'xdh'} = undef;
    $self->{'xfer'} = undef;
}

sub get_bytes_written {
    my ($self) = @_;

    my $size = $self->{'data_size'};
    $size += $self->{'xdh'}->get_part_bytes_written() if defined $self->{'xdh'};

    return $size;
}

sub continue_chunk {
    my $self = shift;
    my %params = @_;

    $self->{'filename'} = $params{'filename'};
    $self->{'chunk_size'} = $params{'chunk_size'};
    $self->{'use_bytes'} = $params{'use_bytes'};

    $self->_start_chunk() if defined $self->{'xdh'};
}

sub _start_chunk {
    my $self = shift;

    $self->dbg("trying to start chunk");

    # we need an actual, permitted holding at this point, so if we don't have
    # one, then defer this start_chunk call until we do.
    if ($self->{'use_bytes'} <= 0) {
	return $self->_get_new_holding();
    }

    # start a new chunk file
    # if on a different holding disk or previous chunk is full
    if ((defined $self->{'old_filename'} and
	 $self->{'old_filename'} ne $self->{'filename'}) or
	$self->{'chunk_bytes'} >= $self->{'chunk_size'}) {
	$self->{'seq'}++;
	$self->{'chunk_bytes'} = 0;
    }
    $self->{'old_filename'} = $self->{'filename'};

    my $filename = $self->{'filename'};
    if ($self->{'seq'} > 0) {
	#build the filename
	$filename = $self->{'filename'} . '.' . $self->{'seq'};
    }

    my $use_bytes = $self->{'use_bytes'};
    if ($use_bytes > $self->{'chunk_size'} - $self->{'chunk_bytes'}) {
	$use_bytes = $self->{'chunk_size'} - $self->{'chunk_bytes'};
    }

    # and start writing this chunk
    $self->{'started_writing'} = 1;
    $self->dbg("resuming transfer $filename, $use_bytes");
    $self->{'xdh'}->start_chunk($self->{'dump_header'}, $filename,
				$use_bytes);
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
    if ($msg->{'elt'} == $self->{'xdh'}) {
	$self->dbg("got msg from xfer dest: $msg");
	if ($msg->{'type'} == $XMSG_CHUNK_DONE) {
	    $self->_xmsg_chunk_done($src, $msg, $xfer);
	} elsif ($msg->{'type'} == $XMSG_ERROR) {
	    $self->_xmsg_error($src, $msg, $xfer);
	}
    }
}

sub _xmsg_chunk_done {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    $self->{'header_size'} += $msg->{'header_size'};
    $self->{'data_size'} += $msg->{'data_size'};
    $self->{'chunk_bytes'} += $msg->{'header_size'} + $msg->{'data_size'};
    $self->{'use_bytes'} -= ($msg->{'header_size'} + $msg->{'data_size'});

    if ($msg->{'no_room'}) {
	$self->{'feedback'}->notify_no_room($self->{'use_bytes'},
					    $msg->{'message'});
	$self->{'use_bytes'} = 0;
	$self->{'feedback'}->request_more_disk();
    } else {
	$self->_start_chunk();
    }
}

sub _xmsg_done {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    my $mesg = $self->{'xdh'}->finish_chunk() if defined $self->{'xdh'};
    if ($mesg) {
	$self->{'result'} = 'FAILED';
	$self->{'feedback'}->notify_error($mesg);
    }
    if ($msg->{'type'} == $XMSG_DONE) {
	$self->{'total_duration'} = $msg->{'duration'};
	$self->dbg("transfer is complete");
	$self->_dump_done();
    }
}

sub _xmsg_error {
    my $self = shift;
    my ($src, $msg, $xfer) = @_;

    if ($msg->{'type'} == $XMSG_ERROR) {
	$self->dbg("transfer error");
	$self->{'result'} = 'FAILED';
	$self->{'feedback'}->notify_error($msg->{'message'});
    }
}

sub _dump_done {
    my $self = shift;

    my $dump_cb = $self->{'dump_cb'};
    my %dump_cb_args = (
        result => $self->{'result'},
        header_size => $self->{'header_size'},
        data_size => $self->{'data_size'},
        total_duration => $self->{'total_duration'});

    # reset everything and let the original caller know we're done
    $self->{'xfer'} = undef;
    $self->{'xdh'} = undef;
    $self->{'dump_header'} = undef;
    $self->{'dump_cb'} = undef;
    $self->{'header_size'} = 0;
    $self->{'data_size'} = 0;
    $self->{'total_duration'} = 0.0;
    $self->{'dump_start_time'} = undef;

    # and call the callback
    $dump_cb->(%dump_cb_args) if defined $dump_cb;
}

sub _get_new_holding {
    my $self = shift;

    $self->{'feedback'}->request_more_disk();
}

sub dbg {
    my ($self, $msg) = @_;
    if ($self->{'debug'}) {
	debug("Amanda::Chunker::Scribe: $msg");
    }
}

##
## Feedback
##

package Amanda::Chunker::Scribe::Feedback;

sub request_more_disk { }

sub scribe_notif_log_info { }
sub scribe_notif_done {
    my $self = shift;
    my %params = @_;

    $params{'finished_cb'}->();
}

1;
