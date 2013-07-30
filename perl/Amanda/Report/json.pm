# Copyright (c) 2010-2012 Zmanda, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
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

=head1 NAME

Amanda::Report::json -- Generate an amanda report in json format.

=over

=item report format

 $report->{'head'}->{'config_name'} => $config;
                    {'org'}         => $org;
                    {'date'}        => "July 24, 2013";
                    {'hostname'}    => "localhost.localdomain"
 $report->{'tapeinfo'}->{'storage'}->{$storage}->{'use'}          => @labels  #label used for each storage
                                                 {'next'}         => @labels  #labels to use on next run
                                                 {'next_to_use'}  => $nb      #
                                                 {'new_labelled'} => @labels  # new labelled tapes.
 $report->{'usage_by_tape'}[]->{'tape_label'} => $label
                               {'nb'}
                               {'size'}
                               {'dump_timestamp'}
                               {'configuration_id'}
                               {'time_duration'}
                               {'nc'}
                               {'percent_use'}
 $report->{'notes'}[]  # array of text lines;
 $report->{'summary'}[]->{'last_tape_label'}
                         {'dle_status'}
                         {'tape_duration'}
                         {'backup_level'}
                         {'configuration_id'}
                         {'hostname'}
                         {'dump_partial'}
                         {'tape_rate'}
                         {'dump_rate'}
                         {'disk_name'}
                         {'dump_duration'}
                         {'dump_timestamp'}
                         {'dump_orig_kb'}
                         {'dump_comp'}
                         {'dump_out_kb'}
 $report->{'statistic'}->{'tape_size'}->{'incr'}
                                        {'full'}
                                        {'total'}
                         {'parts_taped'}->{'incr'}
                                          {'full'}
                                          {'total'}
                         {'dles_taped'}->{'incr'}
                                         {'full'}
                                         {'total'}
                         {'Avg_tape_write_speed'}->{'incr'}
                                                   {'full'}
                                                   {'total'}
                         {'dump_time'}->{'incr'}
                                        {'full'}
                                        {'total'}
                         {'tape_used'}->{'incr'}
                                        {'full'}
                                        {'total'}
                         {'tape_time'}->{'incr'}
                                        {'full'}
                                        {'total'}
                         {'original_size'}->{'incr'}
                                            {'full'}
                                            {'total'}
                         {'output_size'}->{'incr'}
                                          {'full'}
                                          {'total'}
                         {'dles_dumped'}->{'incr'}
                                          {'full'}
                                          {'total'}
                         {'avg_compression'}->{'incr'}
                                              {'full'}
                                              {'total'}
                         {'avg_dump_rate'}->{'incr'}
                                            {'full'}
                                            {'total'}
                         {'dumpdisks'} =>  "1:1"
                         {'tapeparts'} =>  "1:1"
                         {'tapedisks'} =>  "1:1"
                         {'run_time'}
                         {'estimate_time'}

=back

=cut

package Amanda::Report::json;

use strict;
use warnings;
use base qw( Amanda::Report::human );
use Carp;

use POSIX;
use Data::Dumper;
use JSON;

use Amanda::Config qw(:getconf config_dir_relative);
use Amanda::Util qw(:constants quote_string );
use Amanda::Holding;
use Amanda::Tapelist;
use Amanda::Debug qw( debug );
use Amanda::Util qw( quote_string );

use Amanda::Report;

## constants that define the column specification output format.

use constant COLSPEC_NAME      => 0;    # column name; used internally
use constant COLSPEC_PRE_SPACE => 1;    # prefix spaces
use constant COLSPEC_WIDTH     => 2;    # column width
use constant COLSPEC_PREC      => 3;    # post-decimal precision
use constant COLSPEC_MAXWIDTH  => 4;    # resize if set
use constant COLSPEC_FORMAT    => 5;    # sprintf format
use constant COLSPEC_TITLE     => 6;    # column title

my $opt_zmc_cid = 1;

## class functions

sub new
{
    my ($class, $report, $config_name, $logfname) = @_;

    my $self = {
        report      => $report,
        config_name => $config_name,
        logfname    => $logfname,

        ## config info
        disp_unit => getconf($CNF_DISPLAYUNIT),
        unit_div  => getconf_unit_divisor(),

        ## statistics
        incr_stats  => {},
        full_stats  => {},
        total_stats => {},
        dumpdisks   => [ 0, 0 ],    # full_count, incr_count
        tapedisks   => [ 0, 0 ],
        tapeparts   => [ 0, 0 ],
	sections    => {},
	section     => 'header',
    };

    if (defined $report) {

        my (@errors, @stranges, @notes);

        @errors =
          map { @{ $report->get_program_info($_, "errors", []) }; }
          Amanda::Report::human::PROGRAM_ORDER;
        ## prepend program name to notes lines.
        foreach my $program (Amanda::Report::human::PROGRAM_ORDER) {
            push @notes,
              map { "$program: $_" }
              @{ $report->get_program_info($program, "notes", []) };
        }

        $self->{errors} = \@errors;
        $self->{notes}  = \@notes;
    }

    bless $self, $class;
    return $self;
}

sub zprint
{
    my $self = shift;

    my $line = join('', @_);
    chomp $line;
    chomp $line;
    chomp $line;
    push @{$self->{'sections'}{$self->{'section'}}}, $line unless $line eq "";
}

sub zsprint
{
    my $self = shift;
    my $section = shift;

    $section = lc($section);
    $section =~ s/\s*:?\s*$//;
    $section =~ s/^\s+//;
    $section =~ s/\s/_/g;
    $section =~ s/_dump_/_/;
    $section =~ s/failed_details/failure_details/;

    $self->{'section'} = $section;

    #FAILURE DUMP SUMMARY => failure_summary
    #STRANGE DUMP SUMMARY => strange_summary
    #DUMP SUMMARY => dump_summary
    #FAILED DUMP DETAILS => failure_details
    #STRANGE DUMP DETAILS => strange_details
    #NOTES => notes
    #USAGE BY TAPE => usage_by_tape
    #statistics
}

sub generate_report
{
    my $self = shift;

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
}

sub write_report
{
    my $self = shift;
    my $fh   = shift;

    $fh || confess "error: no file handle given to Amanda::Report::human::write_report\n";
    $self->{fh} = $fh;

    $self->generate_report();

    my $json = JSON->new->allow_nonref;
    print {$self->{'fh'}} $json->pretty->encode($self->{'sections'});

    return;
}

sub print_header
{
    my ($self) = @_;

    my $report      = $self->{report};
    my $fh          = $self->{fh};
    my $config_name = $self->{config_name};

    my $hostname = $report->{hostname};
    my $org      = getconf($CNF_ORG);

    # TODO: this should be a shared method somewhere
    my $timestamp = $report->get_timestamp();
    my ($year, $month, $day) = ($timestamp =~ m/^(\d\d\d\d)(\d\d)(\d\d)/);
    my $date  = POSIX::strftime('%B %e, %Y', 0, 0, 0, $day, $month - 1, $year - 1900);
    $date =~ s/  / /g; # get rid of intervening space

    $self->zprint("*** THE DUMPS DID NOT FINISH PROPERLY!\n\n")
      unless ($report->{flags}{got_finish});

    if ($hostname) {
	$self->{'sections'}{'head'}{"hostname"} = $hostname;
	$self->{'sections'}{'head'}{"org"} = $org;
	$self->{'sections'}{'head'}{"config_name"} = $config_name;
	$self->{'sections'}{'head'}{"date"} = $date;
    }

    return;
}

sub output_tapeinfo
{
    my ($self)   = @_;
    my $report   = $self->{report};
    my $fh       = $self->{fh};
    my $logfname = $self->{logfname};

    my $taper       = $report->get_program_info("taper");
    my $tapes       = $taper->{tapes}       || {};
    my $tape_labels = $taper->{tape_labels} || [];

    my %full_stats  = %{ $self->{full_stats} };
    my %incr_stats  = %{ $self->{incr_stats} };
    my %total_stats = %{ $self->{total_stats} };

    for my $storage_n (@{$report->{'storage_list'}}) {
	my $st = Amanda::Config::lookup_storage($storage_n);
	if (!$st) {
	    debug("Storage '%s' not found", $storage_n);
	    next;
	}
	if (storage_getconf($st, $STORAGE_REPORT_USE_MEDIA)) {
	    # find and count label use for the storage
	    my @storage_tape_labels;
	    foreach my $tape_label (@$tape_labels) {
		my $tape = $tapes->{$tape_label};
		if ($tape->{'storage'} eq $storage_n) {
		    push @storage_tape_labels, $tape_label;
		}
	    }

	    if (@storage_tape_labels > 0) {
		$self->{'sections'}->{'tapeinfo'}->{'storage'}->{$storage_n}->{'use'} = \@storage_tape_labels;
	    }
	}
    }

    if (my $tape_error =
        $report->get_program_info("taper", "tape_error", undef)) {

        if ($report->get_program_info("taper", "failure_from", undef) eq "config") {
	    # remove leading [ and trailling ]
	                $tape_error =~ s/^\[//;
            $tape_error =~ s/\]$//;
            $self->zprint("Not using all tapes because $tape_error.\n");
        } else {
            $self->zprint("*** A TAPE ERROR OCCURRED: $tape_error.\n");
        }
	#$tape_error =~ s{^no-tape }{};
    }

    ## if this is a historical report, do not generate holding disk
    ## information.  If this dump is the most recent, output holding
    ## disk info.
    if ($report->get_flag("historical")) {
	$self->{'sections'}->{'tapeinfo'}->{'hdisk'}->{'some_dump'} = 1
          if $report->get_flag("degraded_mode")

    } else {

        my @holding_list = Amanda::Holding::get_files_for_flush();
        my $h_size = 0;
        foreach my $holding_file (@holding_list) {
            $h_size += (0 + Amanda::Holding::file_size($holding_file, 1));
        }

        if ($h_size > 0) {
	    $self->{'sections'}->{'tapeinfo'}->{'hdisk'}->{'size'} = $h_size;

            (getconf($CNF_AUTOFLUSH))
	      ? $self->{'sections'}->{'tapeinfo'}->{'hdisk'}->{'flush_next_run'} = 1
	      : $self->{'sections'}->{'tapeinfo'}->{'hdisk'}->{'run_amflush'} = 1;

        } elsif ($report->get_flag("degraded_mode")) {
	    $self->{'sections'}->{'tapeinfo'}->{'hdisk'}->{'size'} = 0;
        }
    }

    for my $storage_n (@{$report->{'storage_list'}}) {
        my $st = Amanda::Config::lookup_storage($storage_n);
        if (!$st) {
            debug("Storage '%s' not found", $storage_n);
            next;
        }
        if (storage_getconf($st, $STORAGE_REPORT_NEXT_MEDIA)) {
            my $run_tapes   = storage_getconf($st, $STORAGE_RUNTAPES);
            my $nb_new_tape = 0;

            my $for_storage = '';
            $for_storage = " for storage '$storage_n'" if @{$report->{'storage_list'}} > 1;
            my @tape_labels;

	    if ($run_tapes) {
		$self->{'sections'}->{'tapeinfo'}->{'storage'}->{$storage_n}->{'next_to_use'} = $run_tapes;
	    }

            my $tlf = Amanda::Config::config_dir_relative(getconf($CNF_TAPELIST));
            my $tl = Amanda::Tapelist->new($tlf);

            my $labelstr = storage_getconf($st, $STORAGE_LABELSTR);
            my $tapepool = storage_getconf($st, $STORAGE_TAPEPOOL);
            my $policy = Amanda::Policy->new(policy => storage_getconf($st, $STORAGE_POLICY));
            my $retention_tapes = $policy->{'retention_tapes'};
            my $retention_days = $policy->{'retention_days'};
            my $retention_recover = $policy->{'retention_recover'};
            my $retention_full = $policy->{'retention_full'};

            foreach my $i ( 0 .. ( $run_tapes - 1 ) ) {

                if ( my $tape_label =
                    Amanda::Tapelist::get_last_reusable_tape_label(
                                        $labelstr->{'template'},
                                        $tapepool,
                                        $storage_n,
                                        $retention_tapes,
                                        $retention_days,
                                        $retention_recover,
                                        $retention_full,
                                        $i) ) {

                    push @tape_labels, $tape_label;
                } else {
                    $nb_new_tape++;
                }
            }
            if (@tape_labels) {
		$self->{'sections'}->{'tapeinfo'}->{'storage'}->{$storage_n}->{'next'} = \@tape_labels;
            }

            if ($nb_new_tape) {
		$self->{'sections'}->{'tapeinfo'}->{'storage'}->{$storage_n}->{'new'} = $nb_new_tape;
            }

            my @new_tapes = Amanda::Tapelist::list_new_tapes(
                                                $storage_n,
                                                $run_tapes);
	    if (@new_tapes) {
		$self->{'sections'}->{'tapeinfo'}->{'storage'}->{$storage_n}->{'new_labelled'} = \@new_tapes;
	    }
        }
    }

    return;
}

sub output_stats
{
    my ($self) = @_;
    my $fh     = $self->{fh};
    my $report = $self->{report};

    $self->zsprint("STATISTICS:");

    # TODO: the hashes are a cheap fix.  fix these.
    my $full_stats  = $self->{full_stats};
    my $incr_stats  = $self->{incr_stats};
    my $total_stats = $self->{total_stats};

    my ( $ttyp, $tt, $tapesize, $marksize );
    $ttyp = getconf($CNF_TAPETYPE);
    $tt = lookup_tapetype($ttyp) if $ttyp;

    if ( $ttyp && $tt ) {

        $tapesize = "".tapetype_getconf( $tt, $TAPETYPE_LENGTH );
        $marksize = "".tapetype_getconf( $tt, $TAPETYPE_FILEMARK );
    }

    # these values should never be zero; assign defaults
    $tapesize = 100 * 1024 * 1024 if !$tapesize;
    $marksize = 1 * 1024 * 1024   if !$marksize;

    $self->{'sections'}{'statistic'}{'estimate_time'} = $total_stats->{planner_time};
    $self->{'sections'}{'statistic'}{'run_time'} = $total_stats->{total_time};

    $self->{'sections'}{'statistic'}{'dump_time'} = {
		total => $total_stats->{dumper_time},
		full  => $full_stats->{dumper_time},
		incr  => $incr_stats->{dumper_time}
    };

    $self->{'sections'}{'statistic'}{'output_size'} = {
		total => $total_stats->{outsize},
		full  => $full_stats->{outsize},
		incr  => $incr_stats->{outsize}
    };

    $self->{'sections'}{'statistic'}{'original_size'} = {
		total => $total_stats->{origsize},
		full  => $full_stats->{origsize},
		incr  => $incr_stats->{origsize}
    };

    my $comp_size = sub {
        my ($stats) = @_;
        return Amanda::Report::human::divzero(100 * $stats->{outsize}, $stats->{origsize});
    };

    $self->{'sections'}{'statistic'}{'avg_compression'} = {
		total => $comp_size->($incr_stats),
		full  => $comp_size->($incr_stats),
		incr  => $comp_size->($incr_stats)
    };

    $self->{'sections'}{'statistic'}{'dles_dumped'} = {
		total => $total_stats->{dumpdisk_count},
		full  => $full_stats->{dumpdisk_count},
		incr  => $incr_stats->{dumpdisk_count},
    };

    $self->{'sections'}{'statistic'}{'avg_dump_rate'} = {
		total => Amanda::Report::human::divzero_wide( $total_stats->{outsize}, $total_stats->{dumper_time} ),
		full  => Amanda::Report::human::divzero_wide( $full_stats->{outsize},  $full_stats->{dumper_time} ),
		incr  => Amanda::Report::human::divzero_wide( $incr_stats->{outsize},  $incr_stats->{dumper_time} )
    };

    $self->{'sections'}{'statistic'}{'dumpdisks'} =
		Amanda::Report::human::has_incrementals($self->{dumpdisks}) ? Amanda::Report::human::by_level_count($self->{dumpdisks}) : "";

    $self->{'sections'}{'statistic'}{'tape_time'} = {
		total => $total_stats->{taper_time},
		full  => $full_stats->{taper_time},
		incr  => $incr_stats->{taper_time}
    };

    $self->{'sections'}{'statistic'}{'tape_size'} = {
		total => $total_stats->{tapesize},
		full  => $full_stats->{tapesize},
		incr  => $incr_stats->{tapesize}
    };

    my $tape_usage = sub {
        my ($stat_ref) = @_;
        return Amanda::Report::human::divzero(
            100 * (
                $marksize *
                  ($stat_ref->{tapedisk_count} + $stat_ref->{tapepart_count}) +
                  $stat_ref->{tapesize}
            ),
            $tapesize
        );
    };

    $self->{'sections'}{'statistic'}{'tape_used'} = {
		total => $tape_usage->($total_stats),
		full  => $tape_usage->($full_stats),
		incr  => $tape_usage->($incr_stats)
    };

    my $nb_incr_dle = 0;
    my @incr_dle = @{$self->{tapedisks}};
    foreach my $level (1 .. $#incr_dle) {
	$nb_incr_dle += $incr_dle[$level];
    }

    $self->{'sections'}{'statistic'}{'dles_taped'} = {
		total => $self->{tapedisks}[0] + $nb_incr_dle,
		full  => $self->{tapedisks}[0],
		incr  => $nb_incr_dle
    };

    $self->{'sections'}{'statistic'}{'tapedisks'} =
		Amanda::Report::human::has_incrementals($self->{tapedisks}) ? Amanda::Report::human::by_level_count($self->{tapedisks}) : "";

    $self->{'sections'}{'statistic'}{'parts_taped'} = {
		total => $total_stats->{tapepart_count},
		full  => $full_stats->{tapepart_count},
		incr  => $incr_stats->{tapepart_count}
    };

    $self->{'sections'}{'statistic'}{'tapeparts'} =
		$self->{tapeparts}[1] > 0 ? Amanda::Report::human::by_level_count($self->{tapeparts}) : "";

    $self->{'sections'}{'statistic'}{'Avg_tape_write_speed'} = {
		total => Amanda::Report::human::divzero_wide( $total_stats->{tapesize}, $total_stats->{taper_time} ),
		full  => Amanda::Report::human::divzero_wide( $full_stats->{tapesize},  $full_stats->{taper_time} ),
		incr  => Amanda::Report::human::divzero_wide( $incr_stats->{tapesize},  $incr_stats->{taper_time} )
    };

    return;
}

sub output_tape_stats
{
    my ($self) = @_;
    my $fh     = $self->{fh};
    my $report = $self->{report};

    my $taper       = $report->get_program_info("taper");
    my $tapes       = $taper->{tapes}       || {};
    my $tape_labels = $taper->{tape_labels} || [];

    # if no tapes used, do nothing
    return if (!@$tape_labels);

    my $label_length = 19;
    foreach my $label (@$tape_labels) {
        $label_length = length($label) if length($label) > $label_length;
    }
    my $ts_format = "  @"
      . '<' x ($label_length - 1)
      . "@>>>> @>>>>>>>>>>> @>>>>> @>>>> @>>>>\n";

    $self->zsprint("USAGE BY TAPE:\n");

    my $tapetype_name = getconf($CNF_TAPETYPE);
    my $tapetype      = lookup_tapetype($tapetype_name);
    my $tapesize      = "" . tapetype_getconf($tapetype, $TAPETYPE_LENGTH);
    my $marksize      = "" . tapetype_getconf($tapetype, $TAPETYPE_FILEMARK);

    foreach my $label (@$tape_labels) {

        my $tape = $tapes->{$label};

	my $tapeused = $tape->{'kb'};
	$tapeused += $marksize * (1 + $tape->{'files'});

	push @{$self->{'sections'}{$self->{'section'}}}, {
			'configuration_id' => $opt_zmc_cid,
			'dump_timestamp' => $self->{'_current_tape'}->{'date'},
			'nb' => int($tape->{dle}),
			'nc' => int($tape->{files}),
			'percent_use' => Amanda::Report::human::divzero(100 * $tapeused, $tapesize),
			'size' => $tape->{kb},
			'tape_label' => $label,
			'time_duration' => Amanda::Report::human::hrmn($tape->{time}),
			};
    }
    return;
}

sub output_details
{
    ## takes no arguments
    my ($self)   = @_;
    my $fh       = $self->{fh};
    my $errors   = $self->{errors};
    my $notes    = $self->{notes};
    my $report   = $self->{report};
    my $stranges = $report->{stranges};

    my $disp_unit = $self->{disp_unit};

    my @failed_dump_details;
    my @strange_dump_details;

    my @dles = $report->get_dles();

    foreach my $dle_entry (@dles) {

        my ($hostname, $disk) = @$dle_entry;
        my $dle      = $report->get_dle_info(@$dle_entry);
        my $alldumps = $dle->{'dumps'} || {};
        my $qdisk    = quote_string($disk);
        my $outsize  = undef;

	while( my ($timestamp, $tries) = each %$alldumps ) {
	    foreach my $try (@$tries) {

		#
		# check for failed dumper details
		#
		if (defined $try->{dumper}
		    && $try->{dumper}->{status} eq 'fail') {

		    push @failed_dump_details,
    "/-- $hostname $qdisk lev $try->{dumper}->{level} FAILED $try->{dumper}->{error}",
		      @{ $try->{dumper}->{errors} },
		      "\\--------";

		    if ($try->{dumper}->{nb_errors} > 100) {
			my $nb = $try->{dumper}->{nb_errors} - 100;

			push @failed_dump_details,
    "$nb lines follow, see the corresponding log.* file for the complete list",
			  "\\--------";
		    }
		}

		#
		# check for strange dumper details
		#
		if (defined $try->{dumper}
		    && $try->{dumper}->{status} eq 'strange') {

		    push @strange_dump_details,
		      "/-- $hostname $qdisk lev $try->{dumper}->{level} STRANGE",
		      @{ $try->{dumper}->{stranges} },
		      "\\--------";

		    if ($try->{dumper}->{nb_stranges} > 100) {
			my $nb = $try->{dumper}->{nb_stranges} - 100;
			push @strange_dump_details,
    "$nb lines follow, see the corresponding log.* file for the complete list",
			  "\\--------";
		    }
		}

		# note: copied & modified from calculate_stats.
		if (
		    exists $try->{dumper}
		    && exists $try->{chunker}
		    && defined $try->{chunker}->{kb}
		    && (   $try->{chunker}{status} eq 'success'
			|| $try->{chunker}{status} eq 'partial')
		  ) {
		    $outsize = $try->{chunker}->{kb};
		} elsif (
		       exists $try->{dumper}
		    && exists $try->{taper}
		    && defined $try->{taper}->{kb}
		    && (   $try->{taper}{status} eq 'done'
			|| $try->{taper}{status} eq 'partial')
		  ) {
		    $outsize = $try->{taper}->{kb};
		}
	    }
	}

        #
        # check for bad estimates
        #

        if (exists $dle->{estimate} && defined $outsize) {
            my $est = $dle->{estimate};

            push @$notes,
              "big estimate: $hostname $qdisk $dle->{estimate}{level}",
              sprintf('                est: %.0f%s    out %.0f%s',
                $self->tounits($est->{ckb}), $disp_unit,
		$self->tounits($outsize), $disp_unit)
              if (defined $est->{'ckb'} && ($est->{ckb} * .9 > $outsize)
                && ($est->{ckb} - $outsize > 1.0e5));
        }
    }

    $self->print_if_def(\@failed_dump_details,  "FAILED DUMP DETAILS:");
    $self->print_if_def(\@strange_dump_details, "STRANGE DUMP DETAILS:");
    $self->print_if_def($notes,                 "NOTES:");

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

    ## collect all the output line specs (see get_summary_info)
    my @summary_linespecs = ();
    $self->{'sections'}{'summary'} = ();
    foreach my $dle (@dles) {
	my @records = $self->get_summary_info($dle, $self->{report}, $col_spec);
	foreach my $record (@records) {
	    push @{$self->{'sections'}{'summary'}},  {
		'dump_timestamp' => $$record[13],
		'configuration_id' => $opt_zmc_cid,
		'hostname' => $$record[1],
		'disk_name' => $$record[2],
		'dle_status' => $$record[0],
		'backup_level' => $$record[3],
		'dump_orig_kb' => $$record[4],
		'dump_out_kb' => $$record[5],
		'dump_comp' => $$record[6],
		'dump_duration' => int($$record[7]),
		'dump_rate' => $$record[8],
		'tape_duration' => int($$record[9]),
		'tape_rate' => $$record[10],
		'dump_partial' => $$record[11],
		'last_tape_label' => $$record[12]
	    }
	}
    }
    return;
}

1;
