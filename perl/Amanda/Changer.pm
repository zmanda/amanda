# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
#
# This library is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License version 2.1 as 
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

package Amanda::Changer;

use strict;
use warnings;
use Carp;
use POSIX ();
use Exporter;
use vars qw( @ISA @EXPORT_OK );
@ISA = qw( Exporter );

@EXPORT_OK = qw(
    reset clean eject label
    query loadslot find scan
);

use Amanda::Paths;
use Amanda::Util;
use Amanda::MainLoop qw( :GIOCondition );
use Amanda::Config;
use Amanda::Device qw( :constants );

=head1 NAME

Amanda::Changer -- interface to changer scripts

=head1 SYNOPSIS

  use Amanda::Changer;

  TODO: REWRITE

  my ($error, $slot) = Amanda::Changer::reset();

  my ($error, $slot, $nslots, $backwards, $searchable) = Amanda::Changer::query();

  my ($error, $tpslot, $tpdevice) = Amanda::Changer::find("TAPE018");

  my $slot_callback = sub {
    my ($slot, $device) = @_;
    if (!$error) print "Slot $slot: $device\n";
    return 0;
  }
  my $error = Amanda::Changer::scan($slot_callback);

=head1 API STATUS

Stable

=head1 INTERFACE

This module provides an object-oriented, event-based interface to changer
scripts, unlike the C changer module.

Objects correspond to an individual tape changer, so it is possible to control
several changers independently using this module.  Objects are parameterized on
the changer script, changer device, and changer file sa provided in the
configuration file.

All operations in the module return immediately, and take as an argument a
callback function which will indicate completion of the changer operation -- a
kind of continuation.  The caller should run a main loop (see
L<Amanda::MainLoop>) to allow the interactions with the changer script to
continue.  Between the initiation of an operation and the subsequent completion
callback, the changer object is considered "busy", and it is a programming
error to initiate a new operation during that time.

The completion callback functions' parameters vary depending on the operation,
but the first parameter is always C<$error>.  This parameter contains an error
string in the case of "benign" errors, corresponding to an exit status of 1 or
a slot named "<error>".  In the normal case, it is zero.  All operations
C<croak()> in the event of a serious error (problems running the changer
script, or an exit status of 2 or higher). 

Many callbacks also take a C<$slot> parameter.  This is the first word returned
from the changer script, and is usually a number, but occasionally a string
such as "<none>".

A new object is created with the C<new> function as follows:

  my $chg = Amanda::Changer->new($tpchanger);

This is done in anticipation of the ability to control multiple tape changers
simultaneously.  The current Changer API does not support this, as changer
scripts access C<amanda.conf> directly to determine their changerdev and
changerfile.  The following member functions are available.

=over

=item reset

  $chg->reset(sub {
    my ($error, $slot) = @_;
    # ...
  });

Resets the tape changer, if supported, by calling

  $tpchanger -reset

=item clean

  $chg->clean(sub {
    my ($error, $slot) = @_;
    # ...
  });

Triggers a cleaning cycle, if supported, by calling

  $tpchanger -clean

=item eject

  $chg->eject(sub {
    my ($error, $slot) = @_;
    # ...
  });

Ejects the tape in the current slot, if supported, by calling

  $tpchanger -eject

=item label

  $chg->label(sub {
    my ($error) = @_;
    # ...
  });

Inform the changer that the tape in the current slot is labeled C<$label>.
Calls

  $tpchanger -label $label

=item query

  $chg->query(sub {
    my ($error, $slot, $nslots, $backwards, $searchable) = @_;
    # ...
  });

Query the changer to determine the current slot (C<$slot>), the
number of slots (C<$nslots>), whether it can move backward through tapes
(C<$backwards>), and whether it is searchable (that is, has a barcode
reader; C<$searchable>).  A changer which cannot move backward through
tapes is also known as a gravity feeder.

This function runs

  $tpchanger -info

=item loadslot

  $chg->loadslot($desired_slot, sub {
    my ($error, $slot, $device) = @_;
    # ...
  });

Load the tape in the given slot, calling back with its slot and device.
C<$desired_slot> can be a numeric slot number or one of the symbolic names
defined by the changer API, e.g., "next", "current", or "first".

  $tpchanger -slot $slot

=item find

  $chg->find($label, sub {
    my ($error, $tpslot, $tpdevice) = @_;
    # ...
  });

Search the changer for a tape with the given label, returning with
C<$tpslot = "<none>"> if the given label is not found.

If the changer is searchable, this function calls

  $tpchanger -search $label

Otherwise it scans all slots in order, beginning with the current slot,
until it finds one with a label equal to C<$label> or exhausts all
slots.  Note that it is considered a fatal error if the label is not
found.

=item scan

  my $each_slot_cb = sub {
    my ($error, $slot, $device_name) = @_;
    # ...
  }
  my $scan_done_cb = sub {
    my ($error) = @_;
    # ...
  }
  scan($each_slot_cb, $scan_done_cb);

Call C<each_slot_cb> for all slots, beginning with the current slot, until
C<each_slot_cb> returns a true value or all slots are exhausted.
C<each_slot_cb> gets three arguments: an error value, a slot number, and a
device name for that slot.  Note that the callback may be called again after a
call with a non-empty $error.  When all slots are scanned, or the
C<each_slot_cb> returns true, C<scan_done_cb> is called to indicate completion
of the operation.

=cut

sub new {
    my $class = shift;
    my ($tpchanger) = @_;
    # extend the tape changer to a full path
    if ($tpchanger !~ qr(^/)) {
        $tpchanger = "$amlibexecdir/$tpchanger";
    }

    my $self = {
	tpchanger => $tpchanger,
	busy => 0,
    };
    bless ($self, $class);
    return $self;
}

sub reset {
    my $self = shift;
    my ($cb) = @_;

    $self->run_tpchanger(
	sub { $cb->(0, $_[0]); },
	mk_fail_cb($cb),
	"-reset");
}

sub clean {
    my $self = shift;
    my ($cb) = @_;

    $self->run_tpchanger(
	sub { $cb->(0, $_[0]); },
	mk_fail_cb($cb),
	"-clean");
}

sub eject {
    my $self = shift;
    my ($cb) = @_;

    $self->run_tpchanger(
	sub { $cb->(0, $_[0]); },
	mk_fail_cb($cb),
	"-eject");
}

sub label {
    my $self = shift;
    my ($label, $cb) = @_;

    $self->run_tpchanger(
	sub { $cb->(0); },
	mk_fail_cb($cb),
	"-label", $label);
}

sub query {
    my $self = shift;
    my ($cb) = @_;

    my $success_cb = sub {
	my ($slot, $rest) = @_;
	# old, unsearchable changers don't return the third result, so it's
	# optional in the regex
	$rest =~ /(\d+) (\d+) ?(\d+)?/ or 
	    croak("Malformed response from changer -seek: $rest");

	# callback params: error, nslots, curslot, backwards, searchable
	$cb->(0, $slot, $1, $2, $3?1:0);
    };
    $self->run_tpchanger($success_cb, mk_fail_cb($cb), "-info");
}

sub loadslot {
    my $self = shift;
    my ($desired_slot, $cb) = @_;

    $self->run_tpchanger(
	sub { $cb->(0, @_); },
	mk_fail_cb($cb), 
	"-slot", $desired_slot);
}

sub find {
    my $self = shift;
    my ($label, $cb) = @_;

    $self->query(sub {
	my ($error, $curslot, $nslots, $backwards, $searchable) = @_;
	if ($error) {
	    $cb->($error);
	    return;
	}

	if ($searchable) {
	    # search using the barcode reader, etc.
	    $self->run_tpchanger(
		sub { $cb->(0, @_); },
		mk_fail_cb($cb),
		"-search", $label);
	} else {
	    # search manually, starting with "current".  This is complicated, because
	    # it's an event-based loop.
	    my $nchecked = 0;
	    my $check_slot;

	    $check_slot = sub {
		my ($error, $slot, $devname) = @_;

		TRYSLOT: {
		    # ignore "benign" errors
		    next TRYSLOT if $error;

		    my $device = Amanda::Device->new($devname);
		    next TRYSLOT unless ($device->status() == $DEVICE_STATUS_SUCCESS);
		    next TRYSLOT unless ($device->read_label() == $DEVICE_STATUS_SUCCESS);
		    my $volume_label = $device->volume_label();
		    next TRYSLOT unless (defined $volume_label);
		    next TRYSLOT unless ($volume_label eq $label);

		    # we found the correct slot
		    $cb->(0, $slot, $devname);
		    return;
		}

		# on to the next slot
		if (++$nchecked >= $nslots) {
		    die("Label $label not found in any slot");
		} else {
		    # loop again with the next slot
		    $self->loadslot("next", $check_slot);
		}
	    };
	    # kick off the loop with the current slot
	    $self->loadslot("current", $check_slot);
	}
    });
}

sub scan {
    my $self = shift;
    my ($each_slot_cb, $scan_done_cb) = @_;

    $self->query(sub {
	my ($error, $curslot, $nslots, $backwards, $searchable) = @_;
	if ($error) {
	    # callback with an error
	    $scan_done_cb->($error);
	    return;
	}

	# set up an event-based loop to call the user's callback for
	# each slot
	my $loadslot_cb;
	my $nchecked = 0;

	$loadslot_cb = sub {
	    my ($error, $slot, $devname) = @_;
	    my $each_result = $each_slot_cb->($error, $slot, $devname);

	    if (!$each_result and ++$nchecked < $nslots) {
		# loop again with the next slot
		$self->loadslot("next", $loadslot_cb);
	    } else {
		# finished
		$scan_done_cb->(0);
	    }
	};
	# kick off the loop with the current slot
	$self->loadslot("current", $loadslot_cb);
    });
}

# Internal-use function to create a failure sub that will croak on
# a serious error and call the callback properly for benign errors
sub mk_fail_cb {
    my ($cb) = @_;
    return sub {
	my ($exitval, $message) = @_;
	croak($message) if ($exitval > 1);
	$cb->($message);
    }
}

# Internal-use function to actually invoke a changer script and parse 
# its output.  
#
# @param $success_cb: called with ($slot, $rest) on success
# @param $failure_cb: called with ($exitval, $message) on any failure
# @params @args: command-line arguments to follow the name of the changer
# @returns: array ($error, $slot, $rest), where $error is an error message if
#       a benign error occurred, or 0 if no error occurred
sub run_tpchanger {
    my $self = shift;
    my $success_cb = shift;
    my $failure_cb = shift;
    my @args = @_;

    if ($self->{'busy'}) {
	croak("Changer is already in use");
    }

    my ($readfd, $writefd) = POSIX::pipe();
    if (!defined($writefd)) {
	croak("Error creating pipe to run changer script: $!");
    }

    my $pid = fork();
    if (!defined($pid) or $pid < 0) {
        croak("Can't fork to run changer script: $!");
    }

    if (!$pid) {
        ## child

	# get our file-handle house in order
	POSIX::close($readfd);
	POSIX::dup2($writefd, 1);
	POSIX::close($writefd);

        # cd into the config dir, if one exists
        # TODO: construct a "fake" config dir including any "-o" overrides
        my $config_dir = Amanda::Config::get_config_dir();
        if ($config_dir) {
            if (!chdir($config_dir)) {
                print "<error> Could not chdir to '$config_dir'\n";
                exit(2);
            }
        }

        %ENV = Amanda::Util::safe_env();

	my $tpchanger = $self->{'tpchanger'};
        { exec { $tpchanger } $tpchanger, @args; } # braces protect against warning

	my $err = "<error> Could not exec $tpchanger: $!\n";
	POSIX::write($writefd, $err, length($err));
        exit 2;
    }

    ## parent

    # clean up file descriptors from the fork
    POSIX::close($writefd);

    # mark this object as "busy", so we can't begin another operation
    # until this one is finished.
    $self->{'busy'} = 1;

    # the callbacks that follow share these lexical variables
    my $child_eof = 0;
    my $child_output = '';
    my $child_dead = 0;
    my $child_exit_status = 0;
    my ($fdsrc, $cwsrc);
    my ($maybe_finished, $fd_source_cb, $child_watch_source_cb);

    # Perl note: we have to use anonymous subs here, as they are instantiated
    # at runtime, rather than at compile time.

    $maybe_finished = sub {
	return unless $child_eof;
	return unless $child_dead;

	# everything is finished -- process the results and invoke the callback
	chomp $child_output;

	# handle fatal errors
	if (!POSIX::WIFEXITED($child_exit_status) || POSIX::WEXITSTATUS($child_exit_status) > 1) {
	    $failure_cb->(POSIX::WEXITSTATUS($child_exit_status),
		"Fatal error from changer script: ".$child_output);
	}

	# parse the child's output
	my @child_output = split '\n', $child_output;
	$failure_cb->(2, "Malformed output from changer script -- no output")
	    if (@child_output < 1);
	$failure_cb->(2, "Malformed output from changer script -- too many lines")
	    if (@child_output > 1);
	$failure_cb->(2, "Malformed output from changer script: '$child_output[0]'")
	    if ($child_output[0] !~ /\s*([^\s]+)(?:\s+(.+))?/);
	my ($slot, $rest) = ($1, $2);

	# mark this object as no longer busy.  This frees the
	# object up to begin the next operation, which may happen
	# during the invocation of the callback
	$self->{'busy'} = 0;

	# let the callback take care of any further interpretation
	my $exitval = POSIX::WEXITSTATUS($child_exit_status);
	if ($exitval == 0) {
	    $success_cb->($slot, $rest);
	} else {
	    $failure_cb->($exitval, $rest);
	}
    };

    $fd_source_cb = sub {
	my ($fdsrc) = @_;
	my ($len, $bytes);
	$len = POSIX::read($readfd, $bytes, 1024);

	# if we got an EOF, shut things down.
	if ($len == 0) {
	    $child_eof = 1;
	    POSIX::close($readfd);
	    $fdsrc->remove();
	    $fdsrc = undef; # break a reference loop
	    $maybe_finished->();
	} else {
	    # otherwise, just keep the bytes
	    $child_output .= $bytes;
	}
    };
    $fdsrc = Amanda::MainLoop::fd_source($readfd, $G_IO_IN | $G_IO_ERR | $G_IO_HUP);
    $fdsrc->set_callback($fd_source_cb);

    $child_watch_source_cb = sub {
	my ($cwsrc, $got_pid, $got_status) = @_;
	$cwsrc->remove();
	$cwsrc = undef; # break a reference loop
	$child_dead = 1;
	$child_exit_status = $got_status;

	$maybe_finished->();
    };
    $cwsrc = Amanda::MainLoop::child_watch_source($pid);
    $cwsrc->set_callback($child_watch_source_cb);
}

1;
