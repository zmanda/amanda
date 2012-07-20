# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
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
use Data::Dumper;

use Amanda::Disklist;
use Amanda::Logfile qw/:logtype_t :program_t/;
use Amanda::Util;
use Amanda::Debug qw( debug warning );

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

=head2 Creating a Report

  my $report = Amanda::Report->new($logfile, $historical);

The constructor reads the logfile and produces the report, which can then be
queried with the other methods.  C<$logfile> should specify the path to the
logfile from which the report is prepared.  If the logfile is not the "current"
logfile, then C<$historical> should be false.  Non-historical reports may draw
information from the current Amanda environment, e.g., holding disks and info
files.

=head2 Summary Information

Note that most of the data provided by these methods is simply a reference to
data stored within the report, and should thus be considered read-only.  For
example, do not use C<shift> or C<pop> to destructively consume lists.

  my $datestamp = $report->get_timestamp();

This returns the run timestamp for this dump run.  This is determined from one
of several START entries.  This returns a full 14-digit timestamp regardless of
the setting of C<usetimestamps> now or during the dump run.

  my @hosts = $report->get_hosts();

This method returns a list containing the hosts that have been seen in
a logfile.  In a scalar context, C<get_hosts> returns the number of
hosts seen.

  my @disks = $report->get_disks($hostname);

This method returns a list of disks that were archived under the given
C<$hostname>.  In a scalar context, this method returns the number of
disks seen, belonging to the hostname.

  my @dles = $report->get_dles();

This method returns a list of list references.  Each referenced list
contains a hostname & disk pair that has been reported by either the
planner or amflush.  The DLEs are stored in the order that they appear
in the logfile.

    @dles = (
        [ 'example1', '/home' ],
        [ 'example1', '/var/log' ],
        [ 'example2', '/etc' ],
        [ 'example2', '/home' ],
        [ 'example3', '/var/www' ],
    );

  if ( $report->get_flag($flag) ) { ... }

The C<get_flag> method accesses a number of flags that represent the state of
the dump.  A true value is returned if the flag is set, and undef otherwise.
The available flags are:

=over

=item C<got_finish>

This flag is true when the driver finished
correctly.  It indicates that the dump run has finished and cleaned
up.

=item C<degraded_mode>

This flag is set if the taper encounters an
error that forces it into degraded mode.

=item C<amflush_run>

This flag is set if amflush is run instead of planner.

=item C<amvault_run>

This flag is set if the run was by amvault.

=item C<normal_run>

This flag is set when planner is run.  Its value
should be opposite of C<amflush_run>.

=item C<dump_failed>

If a dump failed.

=item C<dump_strange>

If a dump end in strange result.

=item C<results_missing>

If this was a normal run, but some DLEs named by the
planner do not have any results, then this flag is set.  Users should look for
DLEs with an empty C<dump> key to enumerate the missing results.

=item C<historical>

This flag is set if this is a "historical" report.  It is
based on the value passed to the constructor.

=back

=head2 Report Data

  my $dle = $report->get_dle_info($hostname, $disk [,$field] );

This method returns the DLE information for the given C<$hostname> and C<disk>,
or if C<$field> is given, returns that field of the DLE information.  See the
DATA DESCRIPTION section for the format of this information.

  my $info = $report->get_program_info($program [,$field] );

This method returns the program information for the given C<$program>, or if
C<$field> is given, returns that field of the DLE information.  See the DATA
DESCRIPTION section for the format of this information.

=head1 DATA DESCRIPTION

=head2 Top Level

The data in the logfile is stored in the module at C<< $report->{data} >>.
Beneath that, there are a number of subdivisions that track both global and
per-host status of the given Amanda run that the logfile represents.  Note that
these subdivisions are usually accessed via C<get_dle_info> and
C<get_program_info>, as described above.

  $data->{programs}

the C<programs> key of the data points to a hash of global program
information, with one element per program.  See the Programs section, below.

  $data->{boguses}

The C<boguses> key refers to a list of arrayrefs of the form

  [$prog, $type, $str]

as returned directly by C<Amanda::Logfile::get_logline>.  These lines are not
in a recognized trace log format.

  $data->{disklist}

The C<disklist> key points to a two-level hash of hostnames and
disknames as present in the logfile.  It looks something like this:

    $report->{data}{disklist} = {
        "server.example.org" => {
            "/home" => {...},
            "/var"  => {...},
        },
        "workstation.example.org" => {
            "/etc"     => {...},
            "/var/www" => {...},
        },
    };

Each C<{...}> in the above contains information about the corresponding DLE.  See DLEs, below.

=head2 Programs

Each program involved in a dump has a hash giving information about its
performance during the run.  A number of fields are common across all of the
different programs:

=over

=item C<start>

the numeric timestamp at which the process was started.

=item C<time>

the length of time (in seconds) that the program ran.

=item C<notes>

a list which stores all notes reported to the logfile
by the corresponding program.

=item C<errors>

a list which stores all errors reported to the
logfile by the corresponding program.

=back

Program-specific fields are described in the following sections.

=head3 planner

The planner logs very little information other than determining what will be
backed up.  It has no special fields other than those given above.

=head3 driver

The driver has one field that the other program-specific
entries do not:

=over

=item C<start_time> - the time it takes for the driver to start up.

=back

=head3 amflush and amdump

No special fields.

=head3 dumper and chunker

Most of the chunker's output and the dumper's output can be tied to a
particular DLE, so their C<programs> hashes are limited to C<notes> and
C<errors>.

=head3 taper

The taper hash holds notes and errors for the per-instance runs of the taper
program, but also tracks the tapes seen in the logfile:

=over

=item C<tapes>

This field is a hash reference keyed by the label of the tape.
each value of the key is another hash which stores date, size, and the
number of files seen by this backup on the tape.  For example:

    $report->{data}{programs}{taper}{tapes} = {
        FakeTape01 => {
            label => "FakeTape01",
            date  => "20100318141930",
            kb    => 7894769,          # data written to tape this session
            files => 14,               # parts written to tape this session
            dle   => 13,               # number of dumps that begin on this tape
            time  => 2.857,            # time spent writing to this tape
        },
    };

=item C<tape_labels>

The C<tape_labels> field is a reference to a list which records the
order that the tapes have been seen.  This list should be used as an
ordered index for C<tapes>.

=back

=head2 DLEs

In the below, C<$dle> is the hash representing one disklist entry.

The C<estimate> key describes the estimate given by the planner.  For
example:

    $dle->{estimate} = {
	level => 0,     # the level of the backup
	sec   => 20,    # estimated time to back up (seconds)
	nkb   => 2048,  # expected uncompressed size (kb)
	ckb   => 1293,  # expected compressed size (kb)
	kps   => 934.1, # speed of the backup (kb/sec)
    };

Each dump of the DLE is represented in C<< $dle->{dumps} >>.  This is a hash,
keyed by dump timestamp with a list of tries as the value for each dump.  Each
try represents a specific attempt to finish writing this dump to a volume.  If
an error occurs during the backup of a DLE and is retried, a second try is
pushed to the tries list.  For example:

    $dle->{dumps} = {
	'20100317142122' => [ $try1 ],
	'20100318141930' => [ $try1, $try2 ],
    };

=head3 Tries

A try is a hash with at least one dumper, taper, and/or chunker DLE program as
a key.  These entries contain the results from the associated program during
try.

There are a number of common fields between all three elements:

=over

=item C<date>

a timestamp of when the program finished (if the program exited)

=item C<status>

the status of the dump at this program on this try ("success", "partial",
"done", or "failed").  The planner adds an extra "skipped" status which is
added when the planner decides to skip a DLE due to user configuration (e.g.,
C<skipincr>).

=item C<level>

the incremental level of the backup.

=item C<sec>

the time in seconds for the program to finish.

=item C<kb>

the size of the data dumped in kb.

=item C<kps>

the rate at which the program was able to process data,
in kb/sec.

=item C<error>

if the program fails, this field contains the error message

=back

The C<dumper> hash has an C<orig_kb> field, giving the size of the data dumped
from the source, before any compression. If encountered, the C<dumper> hash may
also contain a C<stranges> field, which is a list of all the messages of type
C<L_STRANGE> encountered during the process.

The C<taper> hash contains all the exit status data given by the taper.
Because the same taper process handles multiple dumps, it does not have a
C<date> field.  However, the taper does have an additional field, C<parts>,
containing a list of parts written for this dump.

=head3 Parts

Each item in the list of taper parts is a hash with the following
fields:

=over

=item C<label>

the name of the tape that the part was written to.

=item C<date>

the datestamp at which this part was written.

=item C<file>

the filename of the part.

=item C<part>

the sequence number of the part for the DLE that the
part is archiving.

=item C<sec>

the length of time, in seconds, that the part took to
be written.

=item C<kb>

the total size of the part.

=item C<kps>

the speed at which the part was written.

=back

=cut

use constant STATUS_STRANGE => 2;
use constant STATUS_FAILED  => 4;
use constant STATUS_MISSING => 8;
use constant STATUS_TAPE    => 16;

sub new
{
    my $class = shift @_;
    my ($logfname, $historical) = @_;

    my $self = {
        data => {},

	## inputs
	_logfname => $logfname,
	_historical => $historical,

	## logfile-parsing state

	# the tape currently being writen
	_current_tape => undef,
    };
    bless $self, $class;

    $self->read_file();
    return $self;
}


sub read_file
{
    my $self       = shift @_;
    my $data       = $self->{data} = {};
    my $logfname   = $self->{_logfname};

    # clear the program and DLE data
    $data->{programs} = {};
    $data->{disklist} = {};
    $self->{cache}    = {};
    $self->{flags}    = {};
    $self->{run_timestamp} = '00000000000000';

    my $logfh = Amanda::Logfile::open_logfile($logfname)
      or die "cannot open '$logfname': $!";

    $self->{flags}{exit_status} = 0;
    $self->{flags}{results_missing} = 0;
    $self->{flags}{dump_failed} = 0;
    $self->{flags}{dump_strange} = 0;

    while ( my ( $type, $prog, $str ) = Amanda::Logfile::get_logline($logfh) ) {
        $self->read_line( $type, $prog, $str );
    }

    ## set post-run flags

    $self->{flags}{historical} = $self->{_historical};
    $self->{flags}{amflush_run} = 0;
    $self->{flags}{amvault_run} = 0;
    if (!$self->get_flag("normal_run")) {
        if (   ( defined $self->get_program_info("amflush") )
            && ( scalar %{ $self->get_program_info("amflush") } ) ) {
	    debug("detected an amflush run");
	    $self->{flags}{amflush_run} = 1;
	} elsif (   ( defined $self->get_program_info("amvault") )
                 && ( scalar %{ $self->get_program_info("amvault") } ) ) {
	    debug("detected an amvault run");
	    $self->{flags}{amvault_run} = 1;
	}
    }

    # check for missing, fail and strange results
    $self->check_missing_fail_strange() if $self->get_flag('normal_run');

    # clean up any temporary values in the data
    $self->cleanup();
}

sub cleanup
{
    my $self = shift;

    #remove last_label field
    foreach my $dle ($self->get_dles()) {
        my $dle_info = $self->get_dle_info(@$dle);
        delete $dle_info->{last_label};
    }

    return;
}


sub read_line
{
    my $self = shift @_;
    my ( $type, $prog, $str ) = @_;

    if ( $type == $L_CONT ) {
	${$self->{nbline_ref}}++;
	if ($str =~ /^\|/) {
	    $self->{nb_strange}++;
	    push @{$self->{contline}}, $str if $self->{nb_strange} + $self->{nb_error} <= 100;
	} elsif ($str =~ /^\?/) {
	    $self->{nb_error}++;
	    push @{$self->{contline}}, $str if $self->{nb_error} <= 100;
	} else {
	    $self->{nb_normal}++;
	    push @{$self->{contline}}, $str if ${$self->{nbline_ref}} <= 100;
	}
	return;
    }
    $self->{contline} = undef;
    $self->{nb_normal} = 0;
    $self->{nb_strange} = 0;
    $self->{nb_error} = 0;

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

    } elsif ( $prog == $P_AMVAULT ) {
        return $self->_handle_amvault_line( $type, $str );

    } elsif ( $prog == $P_AMDUMP ) {
        return $self->_handle_amdump_line( $type, $str );

    } elsif ( $prog == $P_REPORTER ) {
        return $self->_handle_reporter_line( $type, $str );

    } else {
        return $self->_handle_bogus_line( $prog, $type, $str );
    }
}

sub get_timestamp
{
    my $self = shift;
    return $self->{'run_timestamp'};
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

sub xml_output
{
    my ( $self, $org, $config ) = @_;
    use Amanda::Report::xml;
    return Amanda::Report::xml::make_amreport_xml( $self, $org, $config );
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
    my ($self, $program, $field, $default) = @_;
    my $prog = $self->{data}{programs}{$program};

    $prog->{$field} = $default if (defined $field && !defined $prog->{$field});

    return (defined $field) ? $prog->{$field} : $prog;
}

sub get_tape
{
    my ($self, $label) = @_;

    my $taper       = $self->get_program_info("taper");
    my $tapes       = $taper->{tapes}       ||= {};
    my $tape_labels = $taper->{tape_labels} ||= [];

    if (!exists $tapes->{$label}) {
        push @$tape_labels, $label;
        $tapes->{$label} = {date => "",
			    kb => 0,
			    files => 0,
			    dle => 0,
			    time => 0};
    }

    return $tapes->{$label};
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

    } elsif ( $type == $L_WARNING ) {
        return $self->_handle_warning_line( "planner", $str );

    } elsif ( $type == $L_START ) {

        $self->{flags}{normal_run} = 1;
        return $self->_handle_start_line( "planner", $str );

    } elsif ( $type == $L_FINISH ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        return $planner->{time} = $info[3];

    } elsif ( $type == $L_DISK ) {
        return $self->_handle_disk_line( "planner", $str );

    } elsif ( $type == $L_SUCCESS ) {
        return $self->_handle_success_line( "planner", $str );

    } elsif ( $type == $L_ERROR ) {
        return $self->_handle_error_line( "planner", $str );

    } elsif ( $type == $L_FATAL ) {
        return $self->_handle_fatal_line( "planner", $str );

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

            return $self->{hostname} = $info[1];

        } elsif ( $info[0] eq "startup" ) {

            my @info = Amanda::Util::split_quoted_strings($str);
            return $driver_p->{start_time} = $info[2];

        } elsif ( $info[0] eq "estimate" ) {

            # estimate format:
            # STATS driver estimate <hostname> <disk> <timestamp>
            # <level> [sec <sec> nkb <nkb> ckb <ckb> jps <kps>]
            # note that the [..] section is *not* quoted properly
            my ($hostname, $disk, $timestamp, $level) = @info[ 1 .. 4 ];

            # if the planner didn't define the DLE then this is a bad
            # line
            unless (exists $disklist->{$hostname}{$disk}) {
                return $self->_handle_bogus_line($P_DRIVER, $type, $str);
            }

            my $dle = $self->get_dle_info($hostname, $disk);
            my ($sec, $nkb, $ckb, $kps) = @info[ 6, 8, 10, 12 ];
            $kps =~ s{\]}{};    # strip trailing "]"

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

        $self->{flags}{exit_status} |= STATUS_TAPE
          if ($str eq "Taper protocol error");

        return $self->_handle_warning_line("driver", $str);

    } elsif ( $type == $L_ERROR ) {
        return $self->_handle_error_line( "driver", $str );

    } elsif ( $type == $L_FATAL ) {
        return $self->_handle_fatal_line( "driver", $str );

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
        my ( $sec, $kb, $kps, $orig_kb ) = @info[ 4, 6, 8, 10 ];
	$kb = int($kb/1024) if $info[4] eq 'bytes';
        $orig_kb =~ s{\]$}{};

        my $dle    = $disklist->{$hostname}->{$disk};
        my $try    = $self->_get_try( $dle, "dumper", $self->{'run_timestamp'});
        my $dumper = $try->{dumper} ||= {};
	$dumper->{level} = $level;
	$dumper->{status} = 'strange';
        $dumper->{sec}       = $sec;
        $dumper->{kb}        = $kb;
        $dumper->{kps}       = $kps;
        $dumper->{orig_kb}   = $orig_kb;

	$self->{contline} = $dumper->{stranges} ||= [];
	$dumper->{nb_stranges} = 0;
	$self->{nbline_ref} = \$dumper->{nb_stranges};
	$self->{nb_normal} = 0;
	$self->{nb_strange} = 0;
	$self->{nb_error} = 0;

        return $self->{flags}{exit_status} |= STATUS_STRANGE

    } elsif ( $type == $L_WARNING ) {

	return $self->_handle_warning_line("dumper", $str);

    } elsif ( $type == $L_SUCCESS ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $hostname, $disk, $timestamp, $level ) = @info[ 0 .. 3 ];
        my ( $sec, $kb, $kps, $orig_kb ) = @info[ 5, 7, 9, 11 ];
	$kb = int($kb/1024) if $info[6] eq 'bytes';
        $orig_kb =~ s{\]$}{};

        my $dle    = $disklist->{$hostname}->{$disk};
        my $try    = $self->_get_try( $dle, "dumper", $timestamp );
        my $dumper = $try->{dumper} ||= {};

        $dumper->{date}      = $timestamp;
        $dumper->{level}     = $level;
        $dumper->{sec}       = $sec;
        $dumper->{kb}        = $kb;
        $dumper->{kps}       = $kps;
        $dumper->{orig_kb}   = $orig_kb;

        return $dumper->{status} = "success";

    } elsif ( $type == $L_ERROR ) {
        return $self->_handle_error_line( "dumper", $str );

    } elsif ( $type == $L_FATAL ) {
        return $self->_handle_fatal_line( "dumper", $str );

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

    } elsif ( $type == $L_SUCCESS || $type == $L_PARTIAL ) {

        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $hostname, $disk, $timestamp, $level ) = @info[ 0 .. 3 ];
        my ( $sec, $kb, $kps ) = @info[ 5, 7, 9 ];
	$kb = int($kb/1024) if $info[6] eq 'bytes';
        $kps =~ s{\]$}{};

        my $dle     = $disklist->{$hostname}->{$disk};
        my $try     = $self->_get_try( $dle, "chunker", $timestamp );
        my $chunker = $try->{chunker} ||= {};

        $chunker->{date}  = $timestamp;
        $chunker->{level} = $level;
        $chunker->{sec}   = $sec;
        $chunker->{kb}    = $kb;
        $chunker->{kps}   = $kps;

        return $chunker->{status} =
          ( $type == $L_SUCCESS ) ? "success" : "partial";

    } elsif ( $type == $L_ERROR ) {
        return $self->_handle_error_line( "chunker", $str );

    } elsif ( $type == $L_FATAL ) {
        return $self->_handle_fatal_line( "chunker", $str );

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
        my ($datestamp, $label, $tapenum) = @info[ 1, 3, 5 ];
        my $tape = $self->get_tape($label);
        $tape->{date} = $datestamp;
        $tape->{label} = $label;

	# keep this tape for later
	$self->{'_current_tape'} = $tape;

	# call through to the generic start line function
        $self->_handle_start_line( "taper", $str );
    } elsif ( $type == $L_PART || $type == $L_PARTPARTIAL ) {

# format is:
# <label> <tapefile> <hostname> <disk> <timestamp> <currpart>/<predparts> <level> [sec <sec> kb <kb> kps <kps>]
#
# format for $L_PARTPARTIAL is the same as $L_PART, plus <err> at the end
        my @info = Amanda::Util::split_quoted_strings($str);
        my ($label, $tapefile, $hostname, $disk, $timestamp) = @info[ 0 .. 4 ];

        $info[5] =~ m{^(\d+)\/(-?\d+)$};
        my ( $currpart, $predparts ) = ( $1, $2 );

        my ($level, $sec, $kb, $kps, $orig_kb) = @info[ 6, 8, 10, 12, 14 ];
	$kb = int($kb/1024) if $info[9] eq 'bytes';
        $kps =~ s{\]$}{};
        $orig_kb =~ s{\]$}{} if defined($orig_kb);

        my $dle   = $disklist->{$hostname}{$disk};
        my $try   = $self->_get_try($dle, "taper", $timestamp);
        my $taper = $try->{taper} ||= {};
        my $parts = $taper->{parts} ||= [];

        my $part = {
            label => $label,
            date  => $timestamp,
            file  => $tapefile,
            sec   => $sec,
            kb    => $kb,
            kps   => $kps,
            partnum  => $currpart,
        };

	$taper->{orig_kb} = $orig_kb;

        push @$parts, $part;

        my $tape = $self->get_tape($label);
	# count this as a filesystem if this is the first part
        $tape->{dle}++ if $currpart == 1;
        $tape->{kb}   += $kb;
        $tape->{time} += $sec;
        $tape->{files}++;

    } elsif ( $type == $L_DONE || $type == $L_PARTIAL ) {

# format is:
# $type = DONE | PARTIAL
# $type taper <hostname> <disk> <timestamp> <part> <level> [sec <sec> kb <kb> kps <kps>]
        my @info = Amanda::Util::split_quoted_strings($str);
        my ( $hostname, $disk, $timestamp, $part_ct, $level ) = @info[ 0 .. 4 ];
        my ( $sec, $kb, $kps, $orig_kb ) = @info[ 6, 8, 10, 12 ];
	$kb = int($kb/1024) if $info[7] eq 'bytes';
	my $error;
	if ($type == $L_PARTIAL) {
	    if ($kps =~ /\]$/) {
	        $error = join " ", @info[ 11 .. $#info ];
	    } else {
	        $error = join " ", @info[ 13 .. $#info ];
	    }
	}
        $kps =~ s{\]$}{};
        $orig_kb =~ s{\]$}{} if defined $orig_kb;

        my $dle   = $disklist->{$hostname}->{$disk};
        my $try   = $self->_get_try($dle, "taper", $timestamp);
        my $taper = $try->{taper} ||= {};
        my $parts = $taper->{parts};

        if ($part_ct - $#$parts != 1) {
            ## this should always be true; do nothing right now
        }

        $taper->{level} = $level;
        $taper->{sec}   = $sec;
        $taper->{kb}    = $kb;
        $taper->{kps}   = $kps;

        $taper->{status} = ( $type == $L_DONE ) ? "done" : "partial";
	$taper->{error} = $error if $type == $L_PARTIAL;

    } elsif ( $type == $L_INFO ) {
        $self->_handle_info_line("taper", $str);

    } elsif ( $type == $L_WARNING ) {
	$self->_handle_warning_line("taper", $str);

    } elsif ( $type == $L_ERROR ) {

        if ($str =~ m{^no-tape}) {

	    my @info = Amanda::Util::split_quoted_strings($str);
	    my $failure_from = $info[1];
	    my $error = join " ", @info[ 2 .. $#info ];

            $self->{flags}{exit_status} |= STATUS_TAPE;
            $self->{flags}{degraded_mode} = 1;
	    $taper_p->{failure_from} = $failure_from;
            $taper_p->{tape_error} = $error;

        } else {
            $self->_handle_error_line("taper", $str);
        }

    } elsif ( $type == $L_FATAL ) {
        return $self->_handle_fatal_line( "taper", $str );

    } elsif ( $type == $L_FAIL ) {
        $self->_handle_fail_line( "taper", $str );

    } else {
        $self->_handle_bogus_line( $P_TAPER, $type, $str );
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

sub _handle_amvault_line
{
    my $self = shift @_;
    my ( $type, $str ) = @_;
    my $data      = $self->{data};
    my $disklist  = $data->{disklist};
    my $programs  = $data->{programs};
    my $amvault_p = $programs->{amvault} ||= {};

    if ( $type == $L_START ) {
        return $self->_handle_start_line( "amvault", $str );

    } elsif ( $type == $L_INFO ) {
        return $self->_handle_info_line( "amvault", $str );

    } elsif ( $type == $L_ERROR ) {
        return $self->_handle_error_line( "amvault", $str );

    } elsif ( $type == $L_FATAL ) {
        return $self->_handle_fatal_line( "amvault", $str );

    } elsif ( $type == $L_DISK ) {
        return $self->_handle_disk_line( "amvault", $str );

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

    } elsif ( $type == $L_FATAL ) {
        return $self->_handle_fatal_line( "amdump", $str );

    } elsif ( $type == $L_ERROR ) {
        $self->_handle_error_line("amdump", $str);
    }
}


sub _handle_fail_line
{
    my ($self, $program, $str) = @_;

    my @info = Amanda::Util::split_quoted_strings($str);
    my ($hostname, $disk, $timestamp, $level) = @info;
    my $error;
    my $failure_from;
    if ($program eq 'taper') {
	$failure_from = $info[4];
	$error = join " ", @info[ 5 .. $#info ];
    } else {
	$error = join " ", @info[ 4 .. $#info ];
    }

    #TODO: verify that this reaches the right try.  Also, DLE or
    #program?
    my $dle = $self->get_dle_info($hostname, $disk);

    my $program_d;
    if ($program eq "planner" ||
        $program eq "driver") {
	$program_d = $dle->{$program} ||= {};
    } else {
        my $try = $self->_get_try($dle, $program, $timestamp);
        $program_d = $try->{$program} ||= {};
    }

    $program_d->{level}  = $level;
    $program_d->{status} = "fail";
    $program_d->{failure_from}  = $failure_from;
    $program_d->{error}  = $error;

    my $errors = $self->get_program_info("program", "errors", []);
    push @$errors, $error;

    $self->{flags}{exit_status} |= STATUS_FAILED;
    if ($program eq "dumper") {
        $self->{contline} = $program_d->{errors} ||= [];
	$program_d->{nb_errors} = 0;
	$self->{nbline_ref} = \$program_d->{nb_errors};
	$self->{nb_normal} = 0;
	$self->{nb_strange} = 0;
	$self->{nb_error} = 0;
    }
}


sub _handle_error_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;

    my $data      = $self->{data};
    my $programs  = $data->{programs};
    my $program_p = $programs->{$program};
    my $errors_p  = $program_p->{errors} ||= [];

    $self->{flags}{exit_status} |= 1;

    push @$errors_p, $str;
}


sub _handle_fatal_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;

    my $data      = $self->{data};
    my $programs  = $data->{programs};
    my $program_p = $programs->{$program};
    my $fatal_p  = $program_p->{fatal} ||= [];

    $self->{flags}{exit_status} |= 1;

    push @$fatal_p, $str;
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
    my $timestamp = $info[1];
    $program_p->{start} = $info[1];

    if ($self->{'run_timestamp'} ne '00000000000000'
		and $self->{'run_timestamp'} ne $timestamp) {
	warning("not all timestamps in this file are the same; "
		. "$self->{run_timestamp}; $timestamp");
    }
    $self->{'run_timestamp'} = $timestamp;
}


sub _handle_disk_line
{
    my $self = shift @_;
    my ($program, $str) = @_;

    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $hosts    = $self->{cache}{hosts} ||= [];
    my $dles     = $self->{cache}{dles}  ||= [];

    my @info = Amanda::Util::split_quoted_strings($str);
    my ($hostname, $disk) = @info;

    if (!exists $disklist->{$hostname}) {

        $disklist->{$hostname} = {};
        push @$hosts, $hostname;
    }

    if (!exists $disklist->{$hostname}{$disk}) {

        push @$dles, [ $hostname, $disk ];
        my $dle = $disklist->{$hostname}{$disk} = {};
        $dle->{'estimate'} = undef;
        $dle->{'dumps'}    = {};
    }
    return;
}

sub _handle_success_line
{
    my $self = shift @_;
    my ($program, $str) = @_;

    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $hosts    = $self->{cache}{hosts} ||= [];
    my $dles     = $self->{cache}{dles}  ||= [];

    my @info = Amanda::Util::split_quoted_strings($str);
    my ($hostname, $disk, $timestamp, $level, $stat1, $stat2) = @info;

    if ($stat1 =~ /skipped/) {
        $disklist->{$hostname}{$disk}->{$program}->{'status'} = 'skipped';
    }
    return;
}


sub _handle_info_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;

    my $data     = $self->{data};
    my $disklist = $data->{disklist};
    my $programs = $data->{programs};

    my $program_p = $programs->{$program} ||= {};

    if ( $str =~ m/^\w+ pid \d+/ || $str =~ m/^pid-done \d+/ ) {

        #do not report pid lines
        return;

    } else {
        my $notes = $program_p->{notes} ||= [];
        push @$notes, $str;
    }
}

sub _handle_warning_line
{
    my $self = shift @_;
    my ( $program, $str ) = @_;

    $self->_handle_info_line($program, $str);
}

sub _handle_bogus_line
{
    my $self = shift @_;
    my ( $prog, $type, $str ) = @_;

    my $data = $self->{data};
    my $boguses = $data->{boguses} ||= [];
    push @$boguses, [ $prog, $type, $str ];
}

sub check_missing_fail_strange
{
    my ($self) = @_;
    my @dles = $self->get_dles();

    foreach my $dle_entry (@dles) {
        my $alldumps = $self->get_dle_info(@$dle_entry, 'dumps');
	my $driver = $self->get_dle_info(@$dle_entry, 'driver');
	my $planner = $self->get_dle_info(@$dle_entry, 'planner');

	if ($planner && $planner->{'status'} eq 'fail') {
	    $self->{flags}{dump_failed} = 1;
	} elsif ($planner && $planner->{'status'} eq 'skipped') {
	    # We don't want these to be counted as missing below
	} elsif (!defined $alldumps->{$self->{'run_timestamp'}} and
		 !$driver and
		 !$planner) {
	    $self->{flags}{results_missing} = 1;
	    $self->{flags}{exit_status} |= STATUS_MISSING;
	} else {
	    #get latest try
	    my $tries = $alldumps->{$self->{'run_timestamp'}};
	    my $try = @$tries[-1];

	    if (exists $try->{dumper} && $try->{dumper}->{status} eq 'fail') {
		$self->{flags}{dump_failed} = 1;
	    } elsif ((defined($try->{'chunker'}) &&
		 $try->{'chunker'}->{status} eq 'success') ||
		(defined($try->{'taper'}) &&
		 $try->{'taper'}->{status} eq 'done')) {
		#chunker or taper success, use dumper status
		if (exists $try->{dumper} && $try->{dumper}->{status} eq 'strange') {
		    $self->{flags}{dump_strange} = 1;
		}
	    } else {
		#chunker or taper failed, the dump is not valid.
		$self->{flags}{dump_failed} = 1;
	    }
	}
    }
}

#
# NOTE: there may be a complicated state diagram lurking in the midst
# of taper and chunker.  You have been warned.
#
sub _get_try
{
    my $self = shift @_;
    my ( $dle, $program, $timestamp ) = @_;
    my $tries = $dle->{'dumps'}{$timestamp} ||= [];

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
