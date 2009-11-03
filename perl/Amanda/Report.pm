# Copyright (c) 2009 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

package Amanda::Report;
use strict;
use warnings;

use Amanda::Disklist;
use Amanda::Logfile qw/:logtype_t :program_t/;
use Amanda::Util;

=head1 NAME

Amanda::Report -- module for representing report data from logfiles

=head1 SYNOPSIS

    use Amanda::Report;

    my $report = Amanda::Report->new($logfile);
    my @hosts  = keys %{$report->{data}{disklist}};

=head1 INTERFACE

This module reads the logfile passed to it and aggregates the data in
a format of nested hashes for convenient output.  All data read in is
stored in C<< $report->{data} >>.

=head2 my $report = Amanda::Report->new($logfile);

The constructor does no real actions aside from calling C<read_file>
on the log file that it is passed.  The value C<$logfile> is a
mandatory argument.

=head2 $report->read_file($logfile);

This is used by C<new> to read in the log file.  This function can
also be used to read in additional logfiles.  All existing data in
C<$report> is deleted from the object before reading C<$logfile>.

=head2 my @hosts = $report->get_hosts();

This method returns a list containing the hosts that have been seen in
a logfile.  In a scalar context, C<get_hosts> returns the number of
hosts seen.

=head2 my @disks = $report->get_disks($hostname);

This method returns a list of disks that were archived under the given
C<$hostname>.  In a scalar context, this method returns the number of
disks seen, belonging to the hostname.

=head2 my @dles = $report->get_dles();

This method returns a list of list references that point to hostname
and disk pairs.  The list returned by C<get_dles> contains all DLE
entries encountered during lo parsing in the following format:

@dles = (
    [ 'example1', '/home' ],
    [ 'example1', '/var/log' ],
    [ 'example2', '/etc' ],
    [ 'example2', '/home' ],
    [ 'example3', '/var/www' ],
);

=head2 my $dle = $report->get_dle_info($hostname, $disk[, $field]);

This method returns all the information stored in the per-DLE section
for the given C<$hostname> and C<disk>.  The returned value is a hash
reference to the data as it is stored in the internal data
structure. Modifying the return value will modify the values in the
C<Amanda::Report> object.

=head2 my $info = $report->get_program_info($program[, $field]);

This method returns a reference to the data for the given C<$program>.
If the optional argument C<$field> is provided, that field in the
indicated program is returned.  The returned value is a reference to
the internal C<Amanda::Report> data structure and will in turn modify
the C<$report> object.

=head2 if ( $report->get_flag($flag) ) { ... }

This method accesses a number of flags that represent the state of the
dump.  A true value is returned if the flag is set, and undef
otherwise.

=head1 DATA DESCRIPTION

The data in the logfile is stored in the module at C<< $report->{data}
>>.  Beyond that, there are a number of subdivisions that track both
global and per-host status of the given Amanda run that the logfile
represents.

=head2 $data->{programs}

the C<programs> key of the data points to a hash of global program
data, with one element per program.  A number of fields are common
across all of the different programs.

=over

=item C<start> - the numeric timestamp at which the process was
started.

=item C<time> - the length of time (in seconds) that the program ran.

=item C<notes> - a list which stores all notes reported to the logfile
by the corresponding program.

=item C<errors> - a list which stores all errors reported to the
logfile by the corresponding program.

=back

In the below, assume

  my $programs = $report->{data}{programs}

=head3 $programs->{planner}

The planner logs very little DLE-specific information other than
determining what will be backed up.  It has no special fields other
than those given above.

=head3 $programs->{driver}

The driver has one unique field that the other program-specific
entries do not:

=over

=item C<start_time> - the time it takes for the driver to start up.

=back

=head3 $programs->{amflush}

When amflush is present, it records what disklist entries need to be
processed instead of the planner.  It also has no special fields.

=head3 $programs->{amdump}

This program is a control program that spawns off dumper programs.  It
has no special fields.

=head3 $programs->{dumper} and $programs->{chunker}

Most of the chunker's output and the dumper's output can be tied to a
particular DLE, so their C<programs> hashes are limited to C<notes>
and C<errors>.

=head3 $programs->{taper}

The taper hash holds notes and errors for the per-instance runs of the
taper program, but also has a unique field which tracks the tapes seen
in the logfile:

=over

=item C<tapes>

The C<tapes> field is a hash reference keyed by the label of the tape.
each value of the key is another hash which stores date, size, and the
number of files seen by this backup on the tape.  Here is an example:

    $report->{data}{programs}{taper}{tapes} =
    {
	FakeTape01 => {
	    date  => "",
	    kb    => "",
	    files => "",
	},
	FakeTape02 => {
	    date  => "",
	    kb    => "",
	    files => "",
	},
    };

=back

=head2 $data->{boguses}

The C<boguses> key refers to a list of arrayrefs of the form C<[$prog,
$type, $str]>, as returned directly by
C<Amanda::Logfile::get_logline>.  These lines were not parseable
because they were not in a recognized format of loglines.

=head2 $data->{disklist}

The C<disklist> key points to a two-level hash of hostnames and
disknames as present in the logfile.  It looks something like this:

    $report->{data}{disklist} = {
	"server.example.org"      => {
	    "/home" => { ... },
	    "/var"  => { ... },
	},
	"workstation.example.org" = {
	    "/etc"     => { ... },
	    "/var/www" => { ... },
	},
    };

In the below, C<$dle> represents one disklist entry (C<{ ... }> in the
above).  Each DLE has three major components: estimates, tries, and
chunks.

=head3 Estimates

The value of C<< $dle->{estimate} >> describes the estimate given by
the planner.

    $dle->{estimate} = {
	level => "0",    # the level of the backup
	sec   => "20",   # estimated time to back up (seconds)
	nkb   => "2048", # expected uncompressed size (kb)
	ckb   => "",	 # expected compressed size (kb)
	kps   => "",     # speed of the backup (kb/sec)
    };

=head3 Tries

Tries are located at C<< $dle->{tries} >>.  This is a list of tries,
each of which is a hash that represents a specific attempt to back up
this DLE.  If an error occurs during the backup of a DLE and is
retried, a second try is pushed to the tries list.

A try is a hash with at least one dumper, taper, and/or chunker DLE
program as a key.  These entries contain the exit conditions of that
particular program for that particular try.

There are a number of common fields between all three elements:

=over

=item C<date> - a timestamp of when the program finished.

=item C<status> - the exit status of the program on this try.

=item C<level> - the incremental level of the backup.

=item C<sec> - the time in seconds for the program to finish.

=item C<kb> - the size of the data dumped in kb.

=item C<kps> - the rate at which the program was able to process data,
in kb/sec.

=back

The C<dumper> hash also has an C<orig-kb> field, giving the size of
the data dumped from the source, before any compression.  The C<taper>
hash contains all the exit status data given by the taper.  Because
the taper has timestamped chunks, the program itself does not have a
C<date> field.  Taper has one unique field, C<parts>, giving the
number of chunks (described in the next section) that were written to
tape.

=head3 Chunks

The list C<< $dle->{chunks} >> describes each of the chunks
that are written by the taper.  Each item in the
list is a hash reference with the following fields:

=over

=item C<label> - the name of the tape that the chunk was written to.

=item C<date> - the datestamp at which this chunk was written.

=item C<file> - the filename of the chunk.

=item C<part> - the sequence number of the chunk for the DLE that the
chunk is archiving.

=item C<sec> - the length of time, in seconds, that the chunk took to
be written.

=item C<kb> - the total size of the chunk.

=item C<kps> - the speed at which the chunk was written.

=back

=head1 FLAGS

During the reading of a logfile, the module will set and unset a
number of flags to indicate the state of the backup.  These are used
to indicate the type of backup or the conditions of success.

The following is a list of currently recognized flags:

=over

=item C<got_finish> - This flag is true when the driver finished
correctly.  It indicates that the dump run has finished and cleaned
up.

=item C<degraded_mode> - This flag is set if the taper encounters an
error that forces it into degraded mode.

=item C<amflush_run> - This flag is set if amflush is run instead of planner.

=item C<normal_run> - This flag is set when planner is run.  Its value
should be opposite of C<amflush_run>.

=back

=cut


sub new
{
    my $class = shift @_;
    my ($logfname) = @_;

    my $self = {
        data => {},
    };
    bless $self, $class;

    $self->read_file($logfname);
    return $self;
}


sub read_file
{
    my $self       = shift @_;
    my ($logfname) = @_;
    my $data       = $self->{data} = {};

    # clear the program and DLE data
    $data->{programs} = {};
    $data->{disklist} = {};
    $self->{cache}    = {};
    $self->{flags}    = {};

    my $logfh = Amanda::Logfile::open_logfile($logfname)
      or die "cannot open $logfname: $!";

    while ( my ( $type, $prog, $str ) = Amanda::Logfile::get_logline($logfh) ) {
        $self->read_line( $type, $prog, $str );
    }

    # set post-run flags
    if (
        !$self->get_flag("normal_run")
        && (   ( defined $self->get_program_info("amflush") )
            && ( scalar %{ $self->get_program_info("amflush") } ) )
      ) {
        $self->{flags}{amflush_run} = 1;
    }

    return;
}


sub read_line
{
    my $self = shift @_;
    my ( $type, $prog, $str ) = @_;

    if ( $prog == $P_PLANNER ) {
        return $self->_handle_planner_line( $type, $str );

    } elsif ( $prog == $P_DRIVER ) {
        return $self->_handle_driver_line( $type, $str );

    } elsif ( $prog == $P_DUMPER ) {
        return $self->_handle_dumper_line( $type, $str );

    } elsif ( $prog == $P_CHUNKER ) {
        return $self->_handle_chunker_line( $type, $str );

    } elsif ( $prog == $P_TAPER ) {
        return $self->_handle_taper_line( $type, $str );

    } elsif ( $prog == $P_AMFLUSH ) {
        return $self->_handle_amflush_line( $type, $str );

    } elsif ( $prog == $P_AMDUMP ) {
        return $self->_handle_amdump_line( $type, $str );

    } elsif ( $prog == $P_REPORTER ) {
        return $self->_handle_reporter_line( $type, $str );

    } else {
        return $self->_handle_bogus_line( $prog, $type, $str );
    }
}

sub get_hosts
{
    my $self  = shift @_;
    my $cache = $self->{cache};

    $cache->{hosts} = [ keys %{ $self->{data}{disklist} } ]
      if ( !defined $cache->{hosts} );

    return @{ $cache->{hosts} };
}

sub get_disks
{
    my $self = shift @_;
    my ($hostname) = @_;
    return keys %{ $self->{data}{disklist}{$hostname} };
}

sub get_dles
{
    my $self  = shift @_;
    my $cache = $self->{cache};
    my @dles;

    if ( !defined $cache->{dles} ) {
        foreach my $hostname ( $self->get_hosts() ) {
            map { push @dles, [ $hostname, $_ ] } $self->get_disks($hostname);
        }
        $cache->{dles} = \@dles;
    }
    return @{ $cache->{dles} };
}

sub get_dle_info
{
    my $self = shift @_;
    my ( $hostname, $disk, $field ) = @_;

    return ( defined $field )
      ? $self->{data}{disklist}{$hostname}{$disk}{$field}
      : $self->{data}{disklist}{$hostname}{$disk};
}

sub get_program_info
{
    my $self = shift @_;
    my ( $program, $field ) = @_;

    return ( defined $field )
      ? $self->{data}{programs}{$program}{$field}
      : $self->{data}{programs}{$program};
}

sub get_flag
{
    my ( $self, $flag ) = @_;
    return $self->{flags}{$flag};
}

sub _handle_planner_line
{
    my $self = shift @_;
    my ( $type, $str ) = @_;
    my $data     = $self->{data};
    my $programs = $data->{programs};
    my $disklist = $data->{disklist} ||= {};
    my $planner  = $programs->{planner} ||= {};

    if ( $type == $L_INFO ) {
        return $self->_handle_info_line( "planner", $str );

    } elsif ( $type == $L_START ) {

        $self->{flags}{normal_run} = 1;
        return $self->_handle_start_line( "planner", $str );

    } elsif ( $type == $L_FINISH ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        return $planner->{time} = $info[3];

    } elsif ( $type == $L_DISK ) {
        return $self->_handle_disk_line( "planner", $str );

    } elsif ( $type == $L_ERROR ) {
        return $self->_handle_error_line( "planner", $str );

    } elsif ( $type == $L_FAIL ) {

        # TODO: these are not like other failure messages: later
        # handle here
        return $self->_handle_fail_line( "planner", $str );

    } else {
        return $self->_handle_bogus_line( $P_PLANNER, $type, $str );
    }
}


sub _handle_driver_line
{
    my $self = shift @_;
    my ( $type, $str ) = @_;
    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};
    my $driver_p = $programs->{driver} ||= {};

    if ( $type == $L_INFO ) {
        return $self->_handle_info_line( "driver", $str );

    } elsif ( $type == $L_START ) {
        return $self->_handle_start_line( "driver", $str );

    } elsif ( $type == $L_FINISH ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        $self->{flags}{got_finish} = 1;
        return $driver_p->{time} = $info[3];

    } elsif ( $type == $L_STATS ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        if ( $info[0] eq "hostname" ) {

            # do nothing

        } elsif ( $info[0] eq "startup" ) {

            my @info = Amanda::Util::split_quoted_strings($str);
            return $driver_p->{start_time} = $info[2];

        } elsif ( $info[0] eq "estimate" ) {

            # estimate format:
            # STATS driver estimate <hostname> <disk> <timestamp>
            # <level> [sec <sec> nkb <nkb> ckb <ckb> jps <kps>]
	    # note that the [..] section is *not* quoted properly
            my ( $hostname, $disk, $timestamp, $level ) = @info[ 1 .. 4 ];
            my $dle;

            # if the planner didn't define the DLE then this is a bad
            # line
            unless ( $dle = $disklist->{$hostname}->{$disk} ) {
                return $self->_handle_bogus_line( $P_DRIVER, $type, $str );
            }

            my ( $sec, $nkb, $ckb, $kps ) = @info[ 6, 8, 10, 12 ];
            $kps =~ s{\]}{}; # strip trailing "]"

            $dle->{estimate} = {
                level => $level,
                sec   => $sec,
                nkb   => $nkb,
                ckb   => $ckb,
                kps   => $kps,
            };

        } else {
            return $self->_handle_bogus_line( $P_DRIVER, $type, $str );
        }

    } elsif ( $type == $L_WARNING ) {

        if ( $str eq "Taper protocol error" ) {

            # DUSTIN: I realize reporter.c does this, so just
            # duplicate its functionality, but please also file a bug
            # so that we're not looking for magic strings in the
            # logfile, and are instead indicating the protocol error
            # in some machine-parsable way
            #
            # TODO: left as comment, figure out what global these are
            # and implement correctly
            #
            # $self->{exit_status} |= $STATUS_TAPE;
            #
            # DUSTIN: rather than exit_status, this should be some
            # field or set of fields that give an overall_status for
            # the dump run.  This is related to the possible return
            # values for amreport, so consult the manpage to see what
            # those are.
        }
        return $self->_handle_warning_line( "driver", $str );

    } elsif ( $type == $L_FAIL ) {
        return $self->_handle_fail_line( "driver", $str );

    } else {
        return $self->_handle_bogus_line( $P_DRIVER, $type, $str );
    }
}


sub _handle_dumper_line
{
    my $self = shift @_;
    my ( $type, $str ) = @_;
    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};
    my $dumper_p = $programs->{dumper} ||= {};

    if ( $type == $L_INFO ) {
        return $self->_handle_info_line( "dumper", $str );

    } elsif ( $type == $L_STRANGE ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $hostname, $disk, $level ) = @info[ 0 .. 2 ];

        #either this:
        #
        # my $strange = join " ", @info[ 3 .. -1 ];
        # $strange =~ s{^\[}{};
        # $strange =~ s{\]$}{};
        #
        # or
        my $strange = $info[3];

        my $dle    = $disklist->{$hostname}->{$disk};
        my $try    = $self->_get_try( $dle, "dumper" );
        my $dumper = $try->{dumper} ||= {};

        # TODO: should this go on the program or DLE notes?
        my $stranges = $dumper->{stranges} ||= [];
        push @$stranges, $strange;

    } elsif ( $type == $L_WARNING ) {

        # TODO: assign note to the appropriate DLE
        my $notes = $dumper_p->{notes};
        push @$notes, $str;

    } elsif ( $type == $L_SUCCESS ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $hostname, $disk, $timestamp, $level ) = @info[ 0 .. 3 ];
        my ( $sec, $kb, $kps, $orig_kb ) = @info[ 5, 7, 9, 11 ];
        $orig_kb =~ s{\]$}{};

        my $dle    = $disklist->{$hostname}->{$disk};
        my $try    = $self->_get_try( $dle, "dumper" );
        my $dumper = $try->{dumper} ||= {};

        $dumper->{date}      = $timestamp;
        $dumper->{level}     = $level;
        $dumper->{sec}       = $sec;
        $dumper->{kb}        = $kb;
        $dumper->{kps}       = $kps;
        $dumper->{"orig-kb"} = $orig_kb;

        return $dumper->{status} = "success";

    } elsif ( $type == $L_FAIL ) {
        return $self->_handle_fail_line( "dumper", $str );

    } else {
        return $self->_handle_bogus_line( $P_DUMPER, $type, $str );
    }
}


sub _handle_chunker_line
{
    my $self = shift @_;
    my ( $type, $str ) = @_;
    my $data      = $self->{data};
    my $disklist  = $data->{disklist};
    my $programs  = $data->{programs};
    my $chunker_p = $programs->{chunker} ||= {};

    if ( $type == $L_INFO ) {
        return $self->_handle_info_line( "chunker", $str );

    } elsif ( $type == $L_SUCCESS || $L_PARTIAL ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $hostname, $disk, $timestamp, $level ) = @info[ 0 .. 3 ];
        my ( $sec, $kb, $kps ) = @info[ 5, 7, 9 ];
        $kps =~ s{\]$}{};

        my $dle     = $disklist->{$hostname}->{$disk};
        my $try     = $self->_get_try( $dle, "chunker" );
        my $chunker = $try->{chunker};

        $chunker->{date}  = $timestamp;
        $chunker->{level} = $level;
        $chunker->{sec}   = $sec;
        $chunker->{kb}    = $kb;
        $chunker->{kps}   = $kps;

        return $chunker->{status} =
          ( $type == $L_SUCCESS ) ? "success" : "partial";

    } elsif ( $type == $L_FAIL ) {
        return $self->_handle_fail_line( "chunker", $str );

    } else {
        return $self->_handle_bogus_line( $P_CHUNKER, $type, $str );
    }
}


sub _handle_taper_line
{
    my $self = shift @_;
    my ( $type, $str ) = @_;
    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};
    my $taper_p  = $programs->{taper} ||= {};

    if ( $type == $L_START ) {

        # format is:
        # START taper datestamp <start> label <label> tape <tapenum>
        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $datestamp, $label, $files ) = @info[ 1, 3, 5 ];
        my $tapes = $taper_p->{tapes} ||= {};
        my $tape  = $tapes->{$label}  ||= {};

        #
        # Nothing interesting is happening here, so return the label
        # name, just in case
        #
        return $tape->{date} = $datestamp;

    } elsif ( $type == $L_PART || $type == $L_PARTPARTIAL ) {

# format is:
# <tapevolume> <tapefile> <hostname> <disk> <timestamp> <currpart>/<predparts> <level> [sec <sec> kb <kb> kps <kps>]
#
# format for $L_PARTPARTIAL is the same as $L_PART, plus <err> at the end
        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $tapevol, $tapefile, $hostname, $disk, $timestamp ) =
          @info[ 0 .. 4 ];

        $info[5] =~ m{^(\d+)\/(-?\d+)$};
        my ( $currpart, $predparts ) = ( $1, $2 );

        my ( $level, $sec, $kb, $kps ) = @info[ 6, 8, 10, 12 ];
        $kps =~ s{\]$}{};

        my $dle    = $disklist->{$hostname}->{$disk};
        my $try    = $self->_get_try( $dle, "taper" );
        my $taper  = $try->{taper}  ||= {};
        my $chunks = $dle->{chunks} ||= [];

        my $chunk = {
            label => $tapevol,
            date  => $timestamp,
            file  => $tapefile,
            part  => $currpart,
            sec   => $sec,
            kb    => $kb,
            kps   => $kps,
        };

        push @$chunks, $chunk;

        if ( $type == $L_PARTPARTIAL ) {

            my @info   = Amanda::Util::split_quoted_strings($str);
            my $errors = $taper_p->{errors};
            my $error  = $info[13];
            push @$errors, $error;

            $taper->{status} = "part+partial";
        }

        return $chunk->{part};

    } elsif ( $type == $L_DONE || $type == $L_PARTIAL ) {

# format is:
# $type = DONE | PARTIAL
# $type taper <hostname> <disk> <timestamp> <part> <level> [sec <sec> kb <kb> kps <kps>]
        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $hostname, $disk, $timestamp, $parts, $level ) = @info[ 0 .. 4 ];
        my ( $sec, $kb, $kps ) = @info[ 6, 8, 10 ];
        $kps =~ s{\]$}{};

        my $dle    = $disklist->{$hostname}->{$disk};
        my $try    = $self->_get_try( $dle, "taper" );
        my $taper  = $try->{taper}  ||= {};

        $taper->{parts} = $parts;
        $taper->{level} = $level;
        $taper->{sec}   = $sec;
        $taper->{kb}    = $kb;
        $taper->{kps}   = $kps;
        return $taper->{status} = ( $type == $L_DONE ) ? "done" : "partial";

    } elsif ( $type == $L_INFO ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        if ( $info[0] eq "tape" ) {

            my ( $label, $kb, $files ) = @info[ 1, 3, 5 ];
            my $tapes = $taper_p->{tapes} ||= {};
            my $tape  = $tapes->{$label}  ||= {};

            # data here: label, size, taperfile
            $tape->{kb} = $kb;
            return $tape->{files} = $files;

        } else {
            return $self->_handle_info_line( "taper", $str );
        }

    } elsif ( $type == $L_WARNING ) {

        # TODO: change this to a handler call
        # TODO: add to DLE if possible
        my $notes = $taper_p->{notes} ||= [];
        push @$notes, $str;

    } elsif ( $type == $L_ERROR ) {

        $self->{flags}{degraded_mode} = 1;
        return $self->_handle_error_line( "taper", $str );

    } elsif ( $type == $L_FAIL ) {

        # TODO: taper fail line also includes level.  parse out at
        # later date.
        return $self->_handle_fail_line( "taper", $str );

    } else {
        return $self->_handle_bogus_line( $P_TAPER, $type, $str );
    }
}


sub _handle_amflush_line
{
    my $self = shift @_;
    my ( $type, $str ) = @_;
    my $data      = $self->{data};
    my $disklist  = $data->{disklist};
    my $programs  = $data->{programs};
    my $amflush_p = $programs->{amflush} ||= {};

    if ( $type == $L_DISK ) {
        return $self->_handle_disk_line( "amflush", $str );

    } elsif ( $type == $L_START ) {
        return $self->_handle_start_line( "amflush", $str );

    } elsif ( $type == $L_INFO ) {
        return $self->_handle_info_line( "amflush", $str );

    } else {
        return $self->_handle_bogus_line( $P_AMFLUSH, $type, $str );
    }
}


sub _handle_amdump_line
{
    my $self = shift;
    my ( $type, $str ) = @_;
    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};
    my $amdump = $programs->{amdump} ||= {};

    if ( $type == $L_INFO ) {
        $self->_handle_info_line("amdump", $str);

    } elsif ( $type == $L_START ) {
        $self->_handle_start_line("amdump", $str);

    } elsif ( $type == $L_ERROR ) {
        $self->_handle_error_line("amdump", $str);
    }
}


sub _handle_fail_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;
    my @info = Amanda::Util::split_quoted_strings($str);

    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};

    my ( $hostname, $disk, $date, $level ) = $info[ 0 .. 3 ];

    #either this
    # my $error = join " ", @info[ 4 .. -1 ];
    # $error =~ s{^\[}{};
    # $error =~ s{\]$}{};
    #
    # or this
    my $error = $info[4];

    #TODO: verify that this reaches the right try.  Also, DLE or
    #program?
    my $dle       = $disklist->{$hostname}->{$disk};
    my $try       = $self->_get_try( $dle, $program );
    my $program_d = $try->{$program};
    my $program_p = $programs->{$program};

    $program_d->{status} = "fail";

    my $errors_p = $program_p->{errors} ||= [];
    push @$errors_p, $error;
}


sub _handle_error_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;
    my @info = Amanda::Util::split_quoted_strings($str);

    my $data      = $self->{data};
    my $programs  = $data->{programs};
    my $program_p = $programs->{$program};
    my $errors_p  = $program_p->{errors} ||= [];

    push @$errors_p, $str;
}


sub _handle_start_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;

    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};

    my $program_p = $programs->{$program} ||= {};

    my @info = Amanda::Util::split_quoted_strings($str);
    $program_p->{start} = $info[1];
}


sub _handle_disk_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;

    my $data     = $self->{data};
    my $disklist = $data->{disklist};

    my @info = Amanda::Util::split_quoted_strings($str);
    my ( $hostname, $disk ) = @info;

    $disklist->{$hostname} ||= {};
    my $dle = $disklist->{$hostname}->{$disk} = {};

    $dle->{estimate} = undef;
    $dle->{tries}    = [];
}


sub _handle_info_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;

    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};

    my $program_p = $programs->{$program} ||= {};

    if ( $str =~ m/^\w+ pid / || $str =~ m/pid-done / ) {

        #do not report pid lines
        return;

    } else {
        my $notes = $program_p->{notes} ||= [];
        push @$notes, $str;
    }
}


sub _handle_bogus_line
{
    my $self = shift @_;
    my ( $prog, $type, $str ) = @_;

    my $data = $self->{data};
    my $boguses = $data->{boguses} ||= [];
    push @$boguses, [ $prog, $type, $str ];
}

#
# NOTE: there may be a complicated state diagram lurking in the midst
# of taper and chunker.  You have been warned.
#
sub _get_try
{
    my $self = shift @_;
    my ( $dle, $program ) = @_;
    my $tries = $dle->{tries} ||= [];

    if (
        !@$tries    # no tries
        || defined $tries->[-1]->{$program}->{status}
        && $self->_program_finished(    # program has finished
            $program, $tries->[-1]->{$program}->{status}
        )
      ) {
        push @$tries, {};
    }
    return $tries->[-1];
}


sub _program_finished
{
    my $self = shift @_;
    my ( $program, $status ) = @_;

    if ( $program eq "chunker" ) {

        if ( $status eq "partial" ) {
            return;
        } else {
            return 1;
        }

    } elsif ( $status eq "done"
        || $status eq "success"
        || $status eq "fail"
        || $status eq "partial" ) {
        return 1;

    } else {
        return 0;
    }
}

1;
