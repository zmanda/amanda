# Copyright (c) 2010 Zmanda, Inc.  All Rights Reserved.
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
# Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
# Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
#

package Amanda::Report::human;

use strict;
use warnings;

use POSIX;

use Amanda::Config qw(:getconf config_dir_relative);
use Amanda::Util  qw (:constants);
use Amanda::Holding;
use Amanda::Tapelist;

use Amanda::Report;

use constant {
    COLSPEC_NAME      => 0,    # column name, used internally
    COLSPEC_PRE_SPACE => 1,    # prefix spaces
    COLSPEC_WIDTH     => 2,    # column width
    COLSPEC_PREC      => 3,    # post-decimal precision
    COLSPEC_MAXWIDTH  => 4,    # resize if set
    COLSPEC_FORMAT    => 5,    # sprintf format
    COLSPEC_TITLE     => 6     # column title
};

## helper functions

sub divzero
{
    my ( $a, $b ) = @_;
    return
        ( $b == 0 )              ? "--"
      : ( $a / $b > 9999999.95 ) ? "#######"
      : ( $b > 99999.95 ) ? sprintf( "%7.0lf", $a / $b )
      :                     sprintf( "%7.1lf", $a / $b );
}

sub divzero_col
{
    my ( $a, $b, $col ) = @_;
    return ( $b == 0 )
      ? "--"
      : sprintf( $col->[5], $col->[2], $col->[3], ( $a / $b ) );
}

sub swrite
{
    my ( $format, @args ) = @_;
    local $^A = "";
    formline( $format, @args );
    return $^A;
}

sub max
{
    my ( $max, @args ) = @_;    # first element starts as max

    foreach my $elt (@args) {
        $max = $elt if $elt > $max;
    }
    return $max;
}

sub min
{
    my ( $min, @args ) = @_;    # first element starts as min

    foreach my $elt (@args) {
        $min = $elt if $elt < $min;
    }
    return $min;
}

sub hrmn
{
    my ($sec) = @_;
    my ( $hr, $mn ) = ( int( $sec / ( 60 * 60 ) ), int( $sec / 60 ) % 60 );
    return sprintf( '%d:%02d', $hr, $mn );
}

sub mnsc
{
    my ($sec) = @_;
    my ( $mn, $sc ) = ( int( $sec / (60) ), int( $sec % 60 ) );
    return sprintf( '%d:%02d', $mn, $sc );
}

## class functions

sub new
{
    my ( $class, $report, $fh, $config_name, $logfname ) = @_;

    my $self = {
        report      => $report,
        fh          => $fh,
        config_name => $config_name,
        logfname    => $logfname,

        ## config info
        disp_unit => getconf($CNF_DISPLAYUNIT),
        unit_div  => getconf_unit_divisor(),

        ## statistics
        incr_stats  => {},
        full_stats  => {},
        total_stats => {},
    };

    if ( defined $report ) {

        my (@errors, @stranges, @notes);

        @errors =
          map { @{ $report->get_program_info( $_, "errors" ) || [] }; }
          keys %{ $report->{data}{programs} };
        @stranges =
          map { @{ $report->get_program_info( $_, "stranges" ) || [] }; }
          keys %{ $report->{data}{programs} };
        @notes = map { @{ $report->get_program_info( $_, "notes" ) || [] }; }
          keys %{ $report->{data}{programs} };

        $self->{errors}   = \@errors;
        $self->{stranges} = \@stranges;
        $self->{notes}    = \@notes;

    }

    bless $self, $class;
    return $self;
}

sub calculate_stats
{
    my ($self) = @_;
    my $fh     = $self->{fh};
    my $report = $self->{report};

    # TODO: the hashes are a cheap fix.  fix these.
    my @dles        = $report->get_dles();
    my $full_stats  = $self->{full_stats};
    my $incr_stats  = $self->{incr_stats};
    my $total_stats = $self->{total_stats};

    ## initialize all relevant fields to 0
    map { $incr_stats->{$_} = $full_stats->{$_} = 0; }
      qw/dumpdisks tapedisks tapechunks outsize origsize tapesize
      coutsize corigsize taper_time dumper_time/;

    foreach my $dle_entry (@dles) {

        # $dle_entry = [$hostname, $disk]
        my $dle = $report->get_dle_info(@$dle_entry);
        my $stats =
          ( $dle->{estimate}{level} > 0 ) ? $incr_stats : $full_stats;

        $stats->{tapechunks} += @{ $dle->{chunks} }
          if ( exists $dle->{chunks} );

        foreach my $chunk ( @{ $dle->{chunks} } ) {
            $stats->{outsize} += $chunk->{kb};

            # TODO: if chunk is compressed, increment coutsize.  how?
        }

        # TODO: corigsize
        #
        # notes from reporter.c on corigsize:
        #
        # if the program is not the dumper, take orig-kb from the
        # dumper and use it.

        foreach my $try ( @{ $dle->{tries} } ) {

            if ( exists $try->{dumper}
                && $try->{dumper}{status} eq 'success' ) {

                $stats->{origsize}    += $try->{dumper}{kbytes};
                $stats->{dumper_time} += $try->{dumper}{sec};
                $stats->{dumpdisks}++;
            }

            if ( exists $try->{taper}
                && $try->{taper}{status} eq 'done' ) {

                $stats->{tapesize}   += $try->{taper}{kb};
                $stats->{taper_time} += $try->{taper}{sec};
                $stats->{tapedisks}++;
            }
        }
    }

    $total_stats->{planner_time} = $report->get_program_info("planner", "time");

    %$total_stats = map { $_ => $incr_stats->{$_} + $full_stats->{$_} }
      keys %$incr_stats;

    if ( $report->get_flag("got_finish") ) {
        $total_stats->{total_time} = $report->get_program_info( "driver", "time" )
          || $report->get_program_info( "amflush", "time" );
    } else {
        $total_stats->{total_time} =
          $total_stats->{taper_time} + $total_stats->{planner_time};
    }

    $total_stats->{idle_time} =
      ( $total_stats->{total_time} - $total_stats->{planner_time} ) -
      $total_stats->{taper_time};

    # TODO: tape info is very sparse.  There either needs to be a
    # function that collects and fills in tape info post-processing in
    # Amanda::Report, or it needs to be done here.
    return;
}

sub print_human_amreport
{
    my ( $self, $fh ) = @_;

    $fh ||= $self->{fh}
      || die "error: no file handle given to print_human_amreport\n";

    ## collect statistics
    $self->calculate_stats();

    ## print the basic info header
    $self->print_header();

    ## print out statements about past and predicted tape usage
    $self->output_tapeinfo();

    ## print out error messages from the run
    $self->output_error_summaries();

    ## print out aggregated statistics for the whole dump
    $self->output_stats();

    ## print out statistics for each tape used
    $self->output_tape_stats();

    ## print out all errors & comments
    $self->output_details();

    ## print out dump statistics per DLE
    $self->output_summary();

    ## footer
    print $fh
      "\n(brought to you by Amanda version $Amanda::Constants::VERSION)\n";

    return;
}

sub print_header
{
    my ($self) = @_;

    my $report      = $self->{report};
    my $fh          = $self->{fh};
    my $config_name = $self->{config_name};

    my @hosts    = $report->get_hosts();
    my $hostname = $hosts[-1];
    my $org      = getconf($CNF_ORG);

    my $datestamp =
      $report->get_program_info(
        $report->get_flag("amflush_run") ? "amflush" : "planner", "start" );
    $datestamp /= 1000000 if $datestamp > 99999999;
    $datestamp = int($datestamp);
    my $year  = int( $datestamp / 10000 );
    my $month = int( ( $datestamp / 100 ) % 100 );
    my $day   = int( $datestamp % 100 );
    my $date  = POSIX::strftime( '%B %d, %Y', 0, 0, 0, $day, $month, $year );

    my $header_format = <<EOF;
@<<<<<<<: @<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<...
EOF

    print $fh swrite( $header_format, "Hostname", $hostname );
    print $fh swrite( $header_format, "Org",      $org );
    print $fh swrite( $header_format, "Config",   $config_name );
    print $fh swrite( $header_format, "Date",     $date );
    print $fh "\n";

    return;
}

sub output_tapeinfo
{
    my ($self)   = @_;
    my $report   = $self->{report};
    my $fh       = $self->{fh};
    my $logfname = $self->{logfname};

    my $taper = $report->get_program_info("taper")
      or return;
    my $tapes = $report->get_program_info( "taper", "tapes" );

    my %full_stats  = %{ $self->{full_stats} };
    my %incr_stats  = %{ $self->{incr_stats} };
    my %total_stats = %{ $self->{total_stats} };

    if ( keys %$tapes > 0 ) {

        my $tapelist_str =
            "These dumps were "
          . ( $report->get_flag("amflush_run") ? "flushed "  : "" )
          . ( keys %$tapes > 1                 ? "to tapes " : "to tape " );

        # TODO: figure out the best way to add these.  there was a
        # call to plural on these
        $tapelist_str .= join( ", ", keys %$tapes ) . ".\n";
        print $fh $tapelist_str;
    }

    if ( $report->get_flag("degraded_mode") ) {

        my $tape_errors = $report->get_program_info( "taper", "errors" );
        print $fh "*** A TAPE ERROR OCCURRED: " . $tape_errors->[0] . ".\n";
    }

    ## if this is a historical report, do not generate holding disk
    ## information.  If this dump is the mst recent, output holding
    ## disk info.
    if ( $logfname eq config_dir_relative(getconf($CNF_LOGDIR))) {
        print $fh "Some dumps may have been left in the holding disk.\n\n"
          if $report->get_flag("degraded_mode")

    } else {

        my @holding_list = Amanda::Holding::get_files_for_flush();
        my ( $h_size, $mh_size );

        foreach my $holding_file (@holding_list) {
            # TODO: figure out API for Amanda::Holding
        }

        if ( $h_size > 0 ) {

            # TODO: du for perl?
            print $fh "There are "
              . du($h_size)
              . " of dumps left in the holding disk.\n";

            ( !!getconf($CNF_AUTOFLUSH) )
              ? print $fh "They will be flushed on the next run.\n\n"
              : print $fh "Run amflush to flush them to tape.\n\n";

        } elsif ( $report->get_flag("degraded_mode") ) {
            print $fh "No dumps are left in the holding disk. " . $h_size . "\n\n";
        }
    }

    my $nb_new_tape = 0;
    my $run_tapes   = getconf($CNF_RUNTAPES);

    if ($run_tapes) {
        ( $run_tapes > 1 )
          ? print $fh "The next tape Amanda expects to use is: "
          : print $fh "The next tapes Amanda expects to use are: ";
    }

    foreach my $i ( 0 .. ( $run_tapes - 1 ) ) {

        if ( my $tape_label =
            Amanda::Tapelist::get_last_reusable_tape_label($i) ) {

            print $fh "$nb_new_tape new tape"
              . ( $nb_new_tape > 1 ? "s, " : ", " )
              . $tape_label
              if $nb_new_tape > 0;
            $nb_new_tape = 0;

        } else {
            $nb_new_tape++;
        }
    }

    if ($nb_new_tape) {
        print $fh "$nb_new_tape new tape"
          . ( $nb_new_tape > 1 ? "s" : "" ) . ".\n";
    }

    print $fh Amanda::Tapelist::list_new_tapes( getconf($CNF_RUNTAPES) );
    print $fh "\n";
    return;
}

sub output_error_summaries
{
    my ($self)   = @_;
    my $fh       = $self->{fh};
    my $errors   = $self->{errors};
    my $stranges = $self->{stranges};

    if (@$errors) {
        print $fh "\nFAILURE DUMP SUMMARY:\n";
        map { print $fh "$_\n" } @$errors;
        print $fh "\n";
    }

    if (@$stranges) {
        print $fh "\nSTRANGE DUMP SUMMARY:\n";
        map { print $fh "$_\n" } @$stranges;
        print $fh "\n";
    }

    print $fh "\n";
    return;
}

sub output_stats
{
    my ($self) = @_;
    my $fh     = $self->{fh};
    my $report = $self->{report};

    my $header = <<EOF;
STATISTICS:
                          Total      Full     Incr.
                        --------  --------  --------
EOF

    my $st_format = <<EOF;
@<<<<<<<<<<<<<<<<<<<<...@>>>>>>>  @>>>>>>>  @>>>>>>>   @<<<<<<<<<<<<<<<<
EOF

    # TODO: the hashes are a cheap fix.  fix these.
    my $full_stats  = $self->{full_stats};
    my $incr_stats  = $self->{incr_stats};
    my $total_stats = $self->{total_stats};

    my ( $ttyp, $tt, $tapesize, $marksize );
    $ttyp = getconf($CNF_TAPETYPE);
    $tt = lookup_tapetype($ttyp) if $ttyp;

    if ( $ttyp && $tt ) {

        $tapesize = tapetype_getconf( $tt, $TAPETYPE_LENGTH );
        $marksize = tapetype_getconf( $tt, $TAPETYPE_FILEMARK );
    }

    # these values should never be zero; assign defaults
    $tapesize = 100 * 1024 * 1024 if !$tapesize;
    $marksize = 1 * 1024 * 1024   if !$marksize;

    print $fh $header;

    print $fh swrite(
        $st_format,
        "Estimate Time (hrs:min)",
        hrmn( $total_stats->{planner_time} ),
        "", "", ""
    );

    print $fh swrite(
        $st_format,
        "Run Time (hrs:min)",
        hrmn( $total_stats->{total_time} ),
        "", "", ""
    );

    print $fh swrite(
        $st_format,
        "Dump Time (hrs:min)",
        hrmn( $total_stats->{dumper_time} ),
        hrmn( $full_stats->{dumper_time} ),
        hrmn( $incr_stats->{dumper_time} ), ""
    );

    print $fh swrite(
        $st_format,
        "Output Size (meg)",
        sprintf( "%8.1lf", $total_stats->{outsize} ),
        sprintf( "%8.1lf", $full_stats->{outsize} ),
        sprintf( "%8.1lf", $incr_stats->{outsize} ),
        "",
    );

    print $fh swrite(
        $st_format,
        "Original Size (meg)",
        sprintf( "%8.1lf", $total_stats->{origsize} ),
        sprintf( "%8.1lf", $full_stats->{origsize} ),
        sprintf( "%8.1lf", $incr_stats->{origsize} ),
        "",
    );

    my $comp_size = sub {
        my ($stats) = @_;
        return divzero( $stats->{coutsize}, $stats->{origsize} );
    };

    print $fh swrite(
        $st_format,
        "Avg Compressed Size (%%)",
        $comp_size->($total_stats),
        $comp_size->($full_stats),
        $comp_size->($incr_stats),
        ( $full_stats->{dumpdisks} > 0 ? "(level:#disks ...)" : "" )
    );

    print $fh swrite(
        $st_format,
        "Filesystems Dumped",
        sprintf( "%4d", $total_stats->{dumpdisks} ),
        sprintf( "%4d", $full_stats->{dumpdisks} ),
        sprintf( "%4d", $incr_stats->{dumpdisks} ),
        ( $full_stats->{dumpdisks} > 0 ? "FIXME!" : "" )
    );

    print $fh swrite(
        $st_format,
        "Avg Dump Rate (k/s)",
        divzero( $total_stats->{outsize}, $total_stats->{dumper_time} ),
        divzero( $full_stats->{outsize},  $full_stats->{dumper_time} ),
        divzero( $incr_stats->{outsize},  $incr_stats->{dumper_time} ),
        ""
    );

    print $fh swrite(
        $st_format,
        "Tape Time (hrs:min)",
        hrmn( $total_stats->{taper_time} ),
        hrmn( $full_stats->{taper_time} ),
        hrmn( $incr_stats->{taper_time} ), ""
    );

    print $fh swrite(
        $st_format,
        "Tape Size (meg)",
        sprintf( "%8.1lf", $total_stats->{tapesize} ),
        sprintf( "%8.1lf", $full_stats->{tapesize} ),
        sprintf( "%8.1lf", $incr_stats->{tapesize} ),
        ""
    );

    my $tape_usage = sub {
        my ($stat_ref) = @_;
        return sprintf(
            "%3.1lf",
            (
                ( $stat_ref->{tapesize} + $marksize ) *
                  ( $stat_ref->{tapedisks} + $stat_ref->{tapechunks} )
              ) / $tapesize
        );
    };

    print $fh swrite(
        $st_format,
        "Tape Used (%%)",
        $tape_usage->($total_stats),
        $tape_usage->($full_stats),
        $tape_usage->($incr_stats),
        ( $incr_stats->{tapedisks} > 0 ? "(level:#disks ...)" : "" )
    );

    print $fh swrite(
        $st_format,
        "Filesystems Taped",
        $total_stats->{tapedisks},
        $full_stats->{tapedisks},
        $incr_stats->{tapedisks},
        ( $incr_stats->{tapedisks} ? "FIXME!" : "" )
    );

    print $fh swrite(
        $st_format,
        "Chunks Taped",
        sprintf( "%4d", $total_stats->{tapechunks} ),
        sprintf( "%4d", $full_stats->{tapechunks} ),
        sprintf( "%4d", $incr_stats->{tapechunks} ),
        ( $incr_stats->{tapechunks} > 0 ? "FIXME!" : "" )
    );

    print $fh swrite(
        $st_format,
        "Avg Tp Write Rate (k/s)",
        divzero( $total_stats->{tapesize}, $total_stats->{taper_time} ),
        divzero( $full_stats->{tapesize},  $full_stats->{taper_time} ),
        divzero( $incr_stats->{tapesize},  $incr_stats->{taper_time} ),
        ""
    );

    print $fh "\n";
    return;
}

sub output_tape_stats
{
    my ($self) = @_;
    my $fh     = $self->{fh};
    my $report = $self->{report};
    my $tapes = $report->get_program_info( "taper", "tapes" );

    my $header = <<EOF;
USAGE BY TAPE:
  Label         Time      Size      %%    Nb    Nc
EOF

    my $ts_format = <<EOF;
@<<<<<<<<<<< @>>>>> @>>>>>>>> @>>>>>> @>>>> @>>>
EOF

    print $fh $header;

    while ( my ( $label, $tape ) = each %$tapes ) {

        # TODO: finish this
        print $fh swrite(
            $ts_format,
            $label,
            "",                # time
            $tape->{kb},       # size
            "",                # % usage
            $tape->{files},    # # of chunks
        );
    }

    print $fh "\n";
    return;
}

sub output_details
{
    ## takes no arguments
    my ($self)   = @_;
    my $fh       = $self->{fh};
    my $errors   = $self->{errors};
    my $stranges = $self->{stranges};
    my $notes    = $self->{notes};

    if (@$errors) {

        print $fh "FAILED DUMP DETAILS:\n";
        map { print $fh "$_\n" } @$errors;
        print $fh "\n";
    }

    if (@$stranges) {

        print $fh "STRANGE DUMP DETAILS:\n";
        map { print $fh "$_\n" } @$stranges;
        print $fh "\n";
    }

    if (@$notes) {

        print $fh "NOTES:\n";
        map { print $fh "$_\n" } @$notes;
        print $fh "\n";
    }

    print "\n";
    return;
}

sub output_summary
{
    ## takes no arguments
    my ($self) = @_;
    my $fh     = $self->{fh};
    my $report = $self->{report};

    ## get the dles
    my @dles =
      sort { ( $a->[0] cmp $b->[0] ) || ( $a->[1] cmp $b->[1] ) }
      $report->get_dles();

    ## set the col_spec, which is the configuration for the summary
    ## output.
    my $col_spec = $self->set_col_spec();

    ## collect all the output lines
    my @summary_lines =
      map { [ get_summary_info( $_, $report, $col_spec ) ] } @dles;

    ## get the summary format. this is based on col_spec, but may
    ## expand maxwidth columns if they have large fields.
    my $summary_format = get_summary_format($col_spec, @summary_lines);

    ## print the header names
    print $fh swrite( $summary_format, map { $_->[COLSPEC_TITLE] } @$col_spec );

    ## write out each output line
    map { print $fh swrite( $summary_format, @$_ ) } @summary_lines;

    print $fh "\n";
    return;
}

## output_summary helper functions.  mostly for formatting, but some
## for data collection.

sub get_summary_info
{
    my ( $dle, $report, $col_spec ) = @_;
    my ( $hostname, $disk ) = @$dle;

    my $dle_info = $report->get_dle_info(@$dle);
    my $last_try = $dle_info->{tries}->[-1];

    my $level =
        exists $last_try->{taper}   ? $last_try->{taper}{level}
      : exists $last_try->{chunker} ? $last_try->{chunker}{level}
      :                               $last_try->{dumper}{level};

    my ($orig_size);
    if ( $report->get_flag("normal_run") ) {

        my $dumper = undef;

        # find the try with the successful dumper entry
        foreach my $try ( @{ $dle_info->{tries} } ) {
            if ( exists $try->{dumper}
                && $try->{dumper}{status} eq "success" ) {
                $dumper = $try->{dumper};
                last;
            }
        }
        $orig_size = $dumper->{'orig-kb'};

    } elsif ( $report->get_flag("amflush_run") ) {
        ## do nothing.  $orig_size will be set in the loop over the
        ## tries

    } else {
        # should not be reached
        $orig_size = 0.0;
    }

    my ( $out_size, $dump_time, $dump_rate, $tape_time, $tape_rate ) = (0) x 5;

    ## Use this loop to set values
    foreach my $try ( @{ $dle_info->{tries} } ) {

        ## find the outsize for the output summary

        if (
            exists $try->{taper}
            && (   $try->{taper}{status} eq "done"
                || $try->{taper}{status} eq "part+partial" )
          ) {
            $out_size  = $try->{taper}{kb};
            $tape_time = $try->{taper}{sec};
            $tape_rate = $try->{taper}{kps};

        } elsif (
            exists $try->{chunker}
            && (   $try->{chunker}{status} eq "success"
                || $try->{chunker}{status} eq "partial" )
          ) {
            $out_size = $try->{chunker}{kb}

        } elsif ( exists $try->{taper}
            && ( $try->{taper}{status} eq "partial" ) ) {

            $out_size  = $try->{taper}{kb};
            $tape_time = $try->{taper}{sec} if !$tape_time;
            $tape_rate = $try->{taper}{kps} if !$tape_rate;

        } elsif ( exists $try->{dumper} ) {
            $out_size = $try->{dumper}{kb};
        }

        if ( exists $try->{taper} && ( $try->{taper}{status} eq "fail" ) ) {
            $tape_time = undef;
            $tape_rate = undef;
        }

        ## if this is an amflush run, get the datestamp from the
        ## chunker or taper and look up the $orig_size in the info
        ## file.
        if (   $report->get_flag("amreport_run") eq "amflush_run"
            && !defined $orig_size
            && ( defined $try->{chunker} || defined $try->{taper} ) ) {

            my $prog    = $try->{chunker} || $try->{taper};
            my $date    = $prog->{date};
            my $infodir = getconf($CNF_INFOFILE);

            my $ci = Amanda::Curinfo->new($infodir);
            my $info = $ci->get_info( $hostname, $disk );

            my $stats = $info->{inf}->[$level];
            my ( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) =
              localtime $stats->{date};

            my $info_date =
                ( $stats->{date} )
              ? ( 1900 + $year ) . ( $mon + 1 ) . ($mday)
              : 19000101;

            $orig_size = ( $info_date == $date )    # is it the same backup?
              ? $info->{inf}->[$level]->{size}
              : 0.0;
        }

        ## find the dump time
        if ( exists $try->{dumper}
            && $try->{dumper}{status} eq "success" ) {

            # TODO: what happens on an amflush run?
            $dump_time = $try->{dumper}{sec};
            $dump_rate = $try->{dumper}{kps};
        }
    }

    my $compression =
      divzero_col( ( 100 * $out_size ), $orig_size, $col_spec->[5] );

    ## simple formatting macros

    my $col_format_field = sub {
        my ( $column, $data ) = @_;

        return sprintf(
            $col_spec->[$column]->[COLSPEC_FORMAT],
            $col_spec->[$column]->[COLSPEC_WIDTH],
            $col_spec->[$column]->[COLSPEC_PREC], $data
        );
    };

    return (
        $col_format_field->( 0, $hostname ),
        $col_format_field->( 1, $disk ),
        $col_format_field->( 2, $level ),
        $col_format_field->( 3, $orig_size ),
        $col_format_field->( 4, $out_size ),
        $col_format_field->( 5, $compression ),
        $col_format_field->( 6, mnsc($dump_time) ),
        $col_format_field->( 7, $dump_rate ),
        $col_format_field->(
            8, ( defined $tape_time ) ? mnsc($tape_time) : "FAILED"
        ),
        $col_format_field->(
            9, ( defined $tape_rate ) ? $tape_rate : "FAILED"
        ),
    );
}

sub get_summary_format
{
    my ( $col_spec, @summary_lines ) = @_;
    my @col_format = ();

    foreach my $i ( 0 .. ( @$col_spec - 1 ) ) {
        $col_format[$i] =
          get_summary_col_format( $col_spec->[$i],
            map { $_->[$i] } @summary_lines );
    }

    return join( "", @col_format ) . "\n";
}

sub get_summary_col_format
{
    my ( $col, @entries ) = @_;
    my ( $col_width, $col_char );
    ## get the longest string in the column

    if ( $col->[COLSPEC_FORMAT] =~ m/s$/ ) {
        $col_width = $col->[COLSPEC_WIDTH];    #string
        $col_char  = '<';

    } elsif ( $col->[COLSPEC_FORMAT] =~ m/d$/ ) {
        $col_width = $col->[COLSPEC_WIDTH];    # decimal
        $col_char  = '>';

    } else {
        $col_width = $col->[COLSPEC_WIDTH] + $col->[COLSPEC_PREC] + 1;   # float
        $col_char  = '>';
    }

    my $strmax = max( map { length $_ } @entries );
    $col_width = max( $strmax, $col_width ) if ( $col->[COLSPEC_MAXWIDTH] );

    return ' ' x $col->[COLSPEC_PRE_SPACE] . '@' . $col_char x $col_width;
}

## col_spec functions.  I want to deprecate this stuff so bad it hurts.

sub set_col_spec
{
    my ($self) = @_;
    my $report = $self->{report};

    ## these should exist within the amreport.pl context, but it's
    ## easier to get them through the API
    my $disp_unit = $self->{disp_unit};
    my $unit_div  = $self->{unit_div};

    my $col_spec = [
        [ "HostName", 0, 12, 12, 0, "%-*.*s", "HOSTNAME" ],
        [ "Disk",     1, 11, 11, 0, "%-*.*s", "DISK" ],
        [ "Level",    1, 1,  1,  0, "%*.*d",  "L" ],
        [ "OrigKB",   1, 7,  0,  1, "%*.*lf", "ORIG-" . $disp_unit . "B" ],
        [ "OutKB",    1, 7,  0,  1, "%*.*lf", "OUT-" . $disp_unit . "B" ],
        [ "Compress", 1, 6,  1,  1, "%*.*lf", "COMP%" ],
        [ "DumpTime", 1, 7,  7,  1, "%*.*s",  "MMM:SS" ],
        [ "DumpRate", 1, 6,  1,  1, "%*.*lf", "KB/s" ],
        [ "TapeTime", 1, 6,  6,  1, "%*.*s",  "MMM:SS" ],
        [ "TapeRate", 1, 6,  1,  1, "%*.*lf", "KB/s" ]
    ];

    $self->apply_col_spec_override();
    return $self->{col_spec} = $col_spec;
}

sub apply_col_spec_override
{
    my ($self) = @_;
    my $col_spec = $self->{col_spec};

    my %col_spec_override = read_col_spec_override() || return;

    foreach my $col (@$col_spec) {
        if ( my $col_override = $col_spec_override{ $col->[COLSPEC_NAME] } ) {

            my $override_col_val_if_def = sub {
                my ( $field, $or_num ) = @_;
                if ( defined $col_override->[$or_num]
                    && !( $col_override->[$or_num] eq "" ) ) {
                    $col->[$field] = $col_override->[$or_num];
                }
            };

            $override_col_val_if_def->( COLSPEC_PRE_SPACE, 0 );
            $override_col_val_if_def->( COLSPEC_WIDTH,     1 );
            $override_col_val_if_def->( COLSPEC_PREC,      2 );
        }
    }
}

sub read_col_spec_override
{
    ## takes no arguments
    my $col_spec_str = getconf($CNF_COLUMNSPEC) || return;
    my %col_spec_override = ();

    foreach ( split( ",", $col_spec_str ) ) {
        if (
            $_ =~ m/^(\w+)        # field name
                =(\d*)            # prefix spaces
                (?=:(\d*))        # width
                (?=:(\d*))        # precision
                $/x
          ) {
            $col_spec_override{$1} = [ $2, $3, $4 ];
        } else {
            die "error: malformed columnspec string:$col_spec_str";
        }
    }
    return %col_spec_override;
}

1;
__END__

=head1 NAME

human - Perl extension for blah blah blah

=head1 SYNOPSIS

   use human;
   blah blah blah

=head1 DESCRIPTION

Stub documentation for human,

Blah blah blah.

=head2 EXPORT

None by default.

=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Paul C Mantz, E<lt>pcmantz@centralcityE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Paul C Mantz

This program is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.2 or,
at your option, any later version of Perl 5 you may have available.

=head1 BUGS

None reported... yet.

=cut
