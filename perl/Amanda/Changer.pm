# Copyright (c) 2006 Zmanda Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
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
# Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Changer;

use Carp;
use POSIX ();
use Exporter;
@ISA = qw( Exporter );

@EXPORT_OK = qw(
    reset clean eject label
    query loadslot find scan
);

use Amanda::Paths;
use Amanda::Util;
use Amanda::Device qw( :constants );
use Amanda::Config qw( :getconf );

=head1 NAME

Amanda::Changer -- interface to changer scripts

=head1 SYNOPSIS

  use Amanda::Changer;

  my ($error, $slot) = Amanda::Changer::reset();

  my ($nslots, $curslot, $backwards, $searchable) = Amanda::Changer::query();

  my ($tpslot, $tpdevice) = Amanda::Changer::find("TAPE018");

  sub slot_callback {
    my ($slot, $device, $error) = @_;
    if (!$error) print "Slot $slot: $device\n";
    return 0;
  }
  Amanda::Changer::scan(\&slot_callback);

=head1 API STATUS

Stable

=head1 FUNCTIONS

All of these functions return an array of values, beginning with
C<$error>, and containing any other results appropriate to the
operation.

The functions C<croak()> in the event of a serious error (problems
running the changer script, or an exit status of 2 or higher).
"Benign" errors, corresponding to an exit status of 1 or a slot named
"<error>", result in the return of a single-element array containing
the error message.  Error-handling for calls can be written

C<$error> and C<$slot>.  The first is false unless a "benign"
error, such as a positioning error, has occurred, in which case it
contains the message from the changer script, and the other results
are undefined.  C<$slot> is the first word returned from the changer
script, and is usually a number, but occasionally a string such as
"<none>".

=over

=item reset

  my ($error, $slot) = reset();

Resets the tape changer, if supported, by calling

  $tpchanger -reset

=item clean

  my ($error, $slot) = clean();

Triggers a cleaning cycle, if supported, by calling

  $tpchanger -clean

=item eject

  my ($error, $slot) = eject();

Ejects the tape in the current slot, if supported, by calling

  $tpchanger -eject

=item label

  my ($error) = label($label);

Inform the changer that the tape in the current slot is labeled C<$label>.  Calls

  $tpchanger -label $label

=item query

  my ($error, $slot, $nslots, $backwards, $searchable) = query();

Query the changer to determine the current slot (C<$slot>), the
number of slots (C<$nslots>), whether it can move backward through tapes
(C<$backwards>), and whether it is searchable (that is, has a barcode
reader; C<$searchable>).  A changer which cannot move backward through
tapes is also known as a gravity feeder.

This function runs

  $tpchanger -info

=item loadslot

  my ($error, $slot, $device) = loadslot($desired_slot);

Load the tape in the given slot, returning its slot and device.
C<$desired_slot> can be a numeric slot number or one of the symbolic
names defined by the changer API, e.g., "next", "current", or "first".

  $tpchanger -slot $slot

=item find

  my ($error, $tpslot, $tpdevice) = Amanda::Changer::find($label);

Search the changer for a tape with the given label, returning with
C<$tpslot = "<none>"> if the given label is not found.

If the changer is searchable, this function calls

  $tpchanger -search $label

Otherwise it scans all slots in order, beginning with the current slot,
until it finds one with a label equal to C<$label> or exhausts all
slots.  Note that it is considered a fatal error if the label is not
found.

=item scan

  my ($error) = Amanda::Changer::scan(\&slot_callback);

Call C<slot_callback> for all slots, beginning with the current slot,
until C<slot_callback> returns a nonzero value or all slots are
exhausted.  C<slot_callback> gets three arguments: a slot number, a
device name for that slot, and a boolean value which is true if the
changer successfully loaded the slot.

=cut

sub reset {
    my ($error, $slot, $rest) = run_tpchanger("-reset");
    return ($error) if $error;

    return (0, $slot);
}

sub clean {
    my ($error, $slot, $rest) = run_tpchanger("-clean");
    return ($error) if $error;

    return (0, $slot);
}

sub eject {
    my ($error, $slot, $rest) = run_tpchanger("-eject");
    return ($error) if $error;

    return (0, $slot);
}

sub label {
    my ($label) = @_;

    my ($error, $slot, $rest) = run_tpchanger("-label", $label);
    return ($error) if $error;

    return (0);
}

sub query {
    my ($error, $slot, $rest) = run_tpchanger("-info");
    return ($error) if $error;

    # old, unsearchable changers don't return the third result, so it's optional in the regex
    $rest =~ /(\d+) (\d+) ?(\d+)?/ or croak("Malformed response from changer -seek: $rest");

    # return array: error, nslots, curslot, backwards, searchable
    return (0, $slot, $1, $2, $3?1:0);
}

sub loadslot {
    my ($desired_slot) = @_;

    my ($error, $slot, $rest) = run_tpchanger("-slot", $desired_slot);
    return ($error) if $error;

    return (0, $slot, $rest);
}

sub find {
    my ($label) = @_;

    my ($error, $curslot, $nslots, $backwards, $searchable) = query();
    return ($error) if $error;

    if ($searchable) {
        # search using the barcode reader, etc.
        my ($error, $slot, $rest) = run_tpchanger("-search", $label);
        return ($error) if $error;
        return ($error, $slot, $rest);
    } else {
        # search manually, starting with "current"
        my $slotstr = "current";
        for (my $checked = 0; $checked < $nslots; $checked++) {
            my ($error, $slot, $rest) = run_tpchanger("-slot", $slotstr);
            $slotstr = "next";

            # ignore "benign" errors
            next if $error;

            my $device = Amanda::Device->new($rest);
            next if (!$device);
            next if ($device->read_label() != $READ_LABEL_STATUS_SUCESS);

            # we found it!
            if ($device->{'volume_label'} eq $label) {
                return (0, $slot, $rest);
            }
        }

        croak("Label $label not found in any slot");
    }
}

sub scan {
    my ($slot_callback) = @_;

    my ($error, $curslot, $nslots, $backwards, $searchable) = query();
    return ($error) if $error;

    my $slotstr = "current";
    my $done = 0;
    for (my $checked = 0; $checked < $nslots; $checked++) {
        my ($error, $slot, $rest) = run_tpchanger("-slot", $slotstr);
        $slotstr = "next";

        if ($error) {
            $done = $slot_callback->(undef, undef, $error);
        } else {
            $done = $slot_callback->($slot, $rest, 0);
        }

        last if $done;
    }
    
    return (0);
}

# Internal-use function to actually invoke a changer script and parse 
# its output.  If the script's exit status is neither 0 nor 1, or if an error
# occurs running the script, then run_tpchanger croaks with the error message.
#
# @params @args: command-line arguments to follow the name of the changer
# @returns: array ($error, $slot, $rest), where $error is an error message if
#       a benign error occurred, or 0 if no error occurred
sub run_tpchanger {
    my @args = @_;

    # get the tape changer and extend it to a full path
    my $tapechanger = getconf($CNF_TPCHANGER);
    if ($tapechanger !~ qr(^/)) {
        $tapechanger = "$libexecdir/$tapechanger";
    }

    my $pid = open(my $child, "-|");
    if (!defined($pid)) {
        croak("Can't fork to run changer script: $!");
    }

    if (!$pid) {
        # child

        # cd into the config dir, if one exists
        # TODO: construct a "fake" config dir including any "-o" overrides
        if ($Amanda::Config::config_dir) {
            if (!chdir($Amanda::Config::config_dir)) {
                print "<error> Could not chdir to '$Amanda::Config::config_dir'\n";
                exit(2);
            }
        }

        %ENV = Amanda::Util::safe_env();

        exec { $tapechanger } $tapechanger, @args or
            print "<error> Could not exec $tapechanger: $!\n";
        exit 2;
    }

    # parent
    my @child_output = <$child>;

    # close the child and get its exit status
    my $child_exit = 0;
    if (!close($child)) {
        if ($!) {
            croak("Error running changer script: $!");
        } else {
            $child_exit = $?;
        }
    }

    # parse the response
    croak("Malformed output from changer script -- no output")
        if (@child_output < 1);
    croak("Malformed output from changer script -- too many lines")
        if (@child_output > 1);
    croak("Malformed output from changer script: '$child_output[0]'")
        if ($child_output[0] !~ /\s*([^\s]+)\s+(.+)?/);
    my ($slot, $rest) = ($1, $2);

    if ($child_exit == 0) {
        return (0, $slot, $rest);
    } elsif (POSIX::WIFEXITED($child_exit) && POSIX::WEXITSTATUS($child_exit) == 1) {
        return ($rest); # non-fatal error
    } else {
        croak("Fatal error from changer script: $rest");
    }
}

1;
