# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94085, or: http://www.zmanda.com

package Amanda::Status::Message;
use strict;
use warnings;

use Amanda::Message;
use vars qw( @ISA );
@ISA = qw( Amanda::Message );

sub local_message {
    my $self = shift;

    if ($self->{'code'} == 1800000) {
        return "The status";
    } elsif ($self->{'code'} == 1800001) {
        return "failed to open the amdump_log file '$self->{'amdump_log'}: $self->{'errnostr'}";
    } elsif ($self->{'code'} == 1800002) {
        return "The amdump_log file '$self->{'amdump_log'}' is for an older version '$self->{'amdump_version'}' and you are running '$self->{'version'}'";
    }
}

package Amanda::Status;

use strict;
use warnings;
use utf8;
use open ':std', ':encoding(utf-8)';
use Encode;
use POSIX ();
use Fcntl qw( O_RDWR O_CREAT LOCK_EX LOCK_NB );
use Data::Dumper;
use vars qw( @ISA );
use Time::Local;
use Text::ParseWords;

use Amanda::Paths;
use Amanda::Util qw( match_labelstr );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Device qw( :constants );
use Amanda::Debug qw( debug );
use Amanda::MainLoop;
use Amanda::Process;

=head1 NAME

Amanda::Status -- Get the status of a running job.

=head1 SYNOPSIS

    use Amanda::Status;

 filename      => The amdump file used for this status
 dead_run      => 1 if the run is already finished/killed
                  0 if the run is running
 datestamp     => $datestamp "20080618130147"
 idle_dumpers  => number of idle dumpers
 status_driver => status of the driver
                    not-idle               working
                    no-dumpers             all dumpers active
                    start-wait             waiting for the startime of a dle
                    no-hold                dle with no holding disk, taper busy
                    client-constrained     can't open new connection to the client
                                             because of maxdumps or spindle
                    no-bandwidth           no bandwidth available
                    no-diskspace	   no holding disk space
 network_free_kps   => free network bandwidth
 holding_free_space => free holding disk space
 holding_space => total holding disk space
 starttime     => The time at start of job since epoc (1213808507)
 current_time  => The current time since starttime in second (12.34)
 exit_status   => The exit status
		  0  = success
                  1  = error
                  4  = a dle failed
                  8  = Don't know the status of a dle (RESULT_MISSING in the report)
                  16 = tape error or no more tape
 dles->{$host}->{$disk}->{$datestamp}->{'status'}            => $status
                                                                  $IDLE                    default
                                                                  $ESTIMATING              got no estimate result yet
                                                                  $ESTIMATE_PARTIAL        got estimate for a few level
                                                                  $ESTIMATE_FAILED         estimate failed
                                                                  $ESTIMATE_DONE           got estimate for all levels
                                                                  $WAIT_FOR_DUMPING        wait for dumping
                                                                  $DUMPING_INIT            dumping initialization phase
                                                                  $DUMPING                 dumping
                                                                  $DUMPING_DUMPER          dumping ending phase
                                                                  $DUMP_DONE		   dump done and succeeded
                                                                  $DUMP_FAILED		   dump failed
                                                                  $DUMPING_TO_TAPE_INIT    dumping to tape initialisation phase
                                                                  $DUMPING_TO_TAPE         dumping to tape
                                                                  $DUMPING_TO_TAPE_DUMPER  dumping to tape ending phase
                                                                  $DUMP_TO_TAPE_DONE	   dump done and succeeded
                                                                  $DUMP_TO_TAPE_FAILED     dump to tape failed
				       {'level'}             => $level
				       {'error'}             => latest error message for the dle
				       {'storage'}           => $storage_name  # when dump to tape.
				       {'message'}           => amstatus message
                                                                  getting estimate
                                                                  partial estimate
                                                                  estimate done
                                                                  estimate failed
                                                                  wait for dumping
                                                                  dumping
                                                                  dumping to tape
                                                                  dump failed: $error
                                                                  will retry at level $retry_level: $error
                                                                  will retry: $error
                                                                  dump to tape failed: $error
                                                                  dump done
                                                                  dump to tape done
				       {'holding_file'}      => holding disk file path
				       {'size'}              => real size
				       {'esize'}             => estimated size
				       {'wsize'}             => working size (when dumping)
				       {'dsize'}             => dumped size (when dumping done)
				       {'dump_time'}         => time the dump started or finished
				       {'chunk_time'}        => time the dump started or finished
				       {'retry'}             => if dumper tell us to retry (from application)
				       {'retry_level'}       => the level of the retry
				       {'will_retry'}        => if that dump will be retried
				       {'wait_holding_disk'} => 1 if dump to holding disk wait for more space
				       {'failed_to_tape'}    #internal use
				       {'taped'}             #internal use
				       {'flush'}             #internal use
				       {'writing_to_tape'}   #internal use
				       {'wait_fo_writing'}   #internal use
 dles->{$host}->{$disk}->{$datestamp}->{'storage'}->{$storage_name}->{'status'}          => $status
                                                                                              $IDLE
                                                                                              $WAIT_FOR_FLUSHING
                                                                                              $WAIT_FOR_WRITING
                                                                                              $FLUSHING
                                                                                              $WRITING
                                                                                              $DUMPING_TO_TAPE
                                                                                              $DUMPING_TO_TAPE_DUMPER
                                                                                              $DUMP_TO_TAPE_FAILED
                                                                                              $WRITE_FAILED
                                                                                              $FLUSH_FAILED
                                                                                              $FLUSH_DONE
                                                                                              $WRITE_DONE
                                                                                              $DUMP_TO_TAPE_DONE
                                                                                              $CONFIG_ERROR
						                     {'message'}         => amstatus message
                                                                                              Idle
                                                                                              wait for flushing
                                                                                              wait for writing
                                                                                              flushing
                                                                                              writing
                                                                                              dumping to tape
                                                                                              dump to tape failed
                                                                                              write failed
                                                                                              flush failed
                                                                                              partially flushed
                                                                                              flushed
                                                                                              partially written
                                                                                              written
                                                                                              dump to tape done
                                                                                              $tape_config
                                                                                              waiting for a tape
                                                                                              searching for a tape
						                     {'size'}            => real size
						                     {'dsize'}           => taped size (when flush done)
						                     {'wsize'}           => working size (when flushing)
						                     {'partial'}         => partial flush
						                     {'taper_time'}      => time the flush started or finished
						                     {'error'}           => tape or config error
						                     {'flushing'}        #internal use
						                     {'wait_for_tape'}   => 1 if waiting for a tape
						                     {'search_for_tape'} => 1 if searching for a tape
						                     {'tape_error'}      => tape error message
						                     {'tape_config'}     => config error message
								     {'will_retry'}      => if that dump_to_tape/flush/write/vault will be retried
 taper->{$taper}->{'storage'}   => $storage
 taper->{$taper}->{'tape_size'} => tape size for the storage
 taper->{$taper}->{'nb_tape'} => number of tape used
 taper->{$taper}->{'worker'}->{$worker}->{'status'}            => $status
					 {'taper_status_file'} => filename of status file for the flush
					 {'message'}           => amstatus message
					 {'error'}             => error message
					 {'host'}              => host actualy flushing
					 {'disk'}              => disk actualy flushing
					 {'datestamp'}         => datestamp actualy flushing
					 {'wait_for_tape'}     => 1 if worker wait for a tape
					 {'search_for_tape'}   => 1 if worker search for a tape
					 {'no_tape'}           => index in {taper->{$taper}->{'stat'}[]} of actualy writing tape
 taper->{$taper}->{'stat'}[]->{'label'}   => label of the tape
			      {'nb_dle'}  => number of dle
			      {'nb_part'} => number of part
			      {'size'}    => real size
			      {'esize'}   => estimated size
			      {'percent'} =>
 storage->{$storage}->{'taper'} = $taper;
 qlen->{'tapeq'}->{'taper'}->{$taper} => number of dle in queue
 qlen->{'tapeq'}->{'roomq'} => number of dle in queue
 qlen->{'tapeq'}->{'runq'}  => number of dle in queue
 busy->{$process}->{'type'}    => 'dumper' or 'chunker' or 'taper'
		   {'time'}    =>
		   {'percent'} =>
		   {'storage'} => $storage # for type eq 'taper'
 busy_dumper =>
 stat->{$status}->{'real_size'}      =>
		  {'real_stat'}      =>
		  {'estimated_size'} =>
		  {'estimated_stat'} =>
		  {'write_size'}     =>
		  {'name'}           => To print to user
    where $status can be  disk
                          estimated
                          wait_for_dumping
                          dumping
                          dumping_to_tape
                          dump_failed
                          dump_to_tape_failed
                          dumped
                          flush
                          wait_to_flush
                          wait_for_writing
                          writing_to_tape
                          failed_to_tape
                          taped
 stat->{'taped'}->{'storage'}->{$storage}->{'real_size'}      =>
					   {'real_stat'}      =>
					   {'estimated_size'} =>
					   {'estimated_stat'} =>
					   {'write_size'}     =>
					   {'nb'}             =>

 status
   $IDLE			 =  0;
   $ESTIMATING			 =  1;  # estimating the dle, got no estimate result yet
   $ESTIMATE_PARTIAL		 =  2;  # estimating the dle, got the estimate result for at least one level
   $ESTIMATE_DONE		 =  3;  # receive the estimate for all levels
   $ESTIMATE_FAILED		 =  4;  # estimate for all levels failed
   $WAIT_FOR_DUMPING		 =  5;  # wait for dumping
   $DUMPING_INIT		 =  6;  # dumping initialization phase
   $DUMPING			 =  7;  # dumping
   $DUMPING_DUMPER		 =  8;  # dumping ending phase
   $DUMPING_TO_TAPE_INIT	 =  9;  # dumping to tape initialisation phase
   $DUMPING_TO_TAPE		 = 10;  # dumping to tape
   $DUMPING_TO_TAPE_DUMPER	 = 11;  # dumping to tape ending phase
   $DUMP_FAILED			 = 12;  # dump failed
   $DUMP_TO_TAPE_FAILED		 = 13;  # dump to tape failed
   $WAIT_FOR_WRITING		 = 14;  # wait for writing
   $WRITING			 = 15;  # writing a dump from holding disk
   $WRITE_FAILED		 = 16;  # writing failed
   $WAIT_FOR_FLUSHING		 = 17;  # wait for flushing
   $FLUSHING			 = 18;  # flushing
   $FLUSH_FAILED		 = 19;  # flush failed
   $DUMP_DONE			 = 20;  # dump done and succeeded
   $DUMP_TO_TAPE_DONE		 = 21;  # dump to tape done and succeeded
   $WRITE_DONE			 = 22;  # write done and succeeded
   $FLUSH_DONE			 = 23;  # flush done and succeeded
   $VAULTING			 = 27;  # vaulting
   $VAULTING_DONE		 = 28;  # vaulting done and succeded
   $VAULTING_FAILED		 = 29;  # vaulting failed

 status only for worker
   $TAPE_ERROR			 = 50;  # failed because of tape error
   $CONFIG_ERROR		 = 51;  # failed because of config (runtapes, ...)

 dle status
  IDLE => ESTIMATING => ESTIMATE_FAILED
                     => ESTIMATE_PARTIAL => ESTIMATE_DONE => WAIT_FOR_DUMPING

  WAIT_FOR_DUMPING => DUMPING_INIT => DUMPING => DUMPING_DUMPER => DUMP_DONE
                                                                => DUMP_FAILED

  WAIT_FOR_DUMPING => DUMPING_TO_TAPE_INIT => DUMPING_TO_TAPE => DUMPING_TO_TAPE_DUMPER => DUMP_TO_TAPE_DONE
                                                                                        => DUMP_TO_TAPE_FAILED

 storage status
  dump to tape
   IDLE => DUMPING_TO_TAPE => DUMPING_TO_TAPE_DUMPER => DUMPING_TO_TAPE_DONE
                                                     => DUMP_TO_TAPE_FAILED
                                                     => CONFIG_ERROR
  flush
   IDLE => WAIT_FOR_FLUSHING => FLUSHING => FLUSH_DONE
                                         => FLUSH_FAILED
                                         => CONFIG_ERROR

  vault
   IDLE => WAIT_FOR_WRITING => WRITING => WRITE_DONE
                                       => WRITE_FAILED
                                       => CONFIG_ERROR
=cut

# status value:
#no warnings;
#no strict;
my $IDLE			 =  0;
my $ESTIMATING			 =  1;  # estimating the dle, got no estimate result yet
my $ESTIMATE_PARTIAL		 =  2;  # estimating the dle, got the estimate result for at least one level
my $ESTIMATE_DONE		 =  3;  # receive the estimate for all levels
my $ESTIMATE_FAILED		 =  4;  # estimate for all levels failed
my $WAIT_FOR_DUMPING		 =  5;  # wait for dumping
my $DUMPING_INIT		 =  6;  # dumping initialization phase
my $DUMPING			 =  7;  # dumping
my $DUMPING_DUMPER		 =  8;  # dumping ending phase
my $DUMPING_TO_TAPE_INIT	 =  9;  # dumping to tape initialisation phase
my $DUMPING_TO_TAPE		 = 10;  # dumping to tape
my $DUMPING_TO_TAPE_DUMPER	 = 11;  # dumping to tape ending phase
my $DUMP_FAILED			 = 12;  # dump failed
my $DUMP_TO_TAPE_FAILED		 = 13;  # dump to tape failed
my $WAIT_FOR_WRITING		 = 14;  # wait for writing
my $WRITING			 = 15;  # writing a dump from holding disk
my $WRITE_FAILED		 = 16;  # writing failed
my $WAIT_FOR_FLUSHING		 = 17;  # wait for flushing
my $FLUSHING			 = 18;  # flushing
my $FLUSH_FAILED		 = 19;  # flush failed
my $DUMP_DONE			 = 20;  # dump done and succeeded
my $DUMP_TO_TAPE_DONE		 = 21;  # dump to tape done and succeeded
my $WRITE_DONE			 = 22;  # write done and succeeded
my $FLUSH_DONE			 = 23;  # flush done and succeeded
my $VAULTING			 = 27;  # vaulting
my $VAULTING_DONE		 = 28;  # vaulting done and succeded
my $VAULTING_FAILED		 = 29;  # vaulting failed
my $TERMINATED_ESTIMATE		 = 30;  # terminated while estimate
my $TERMINATED_DUMPING		 = 31;  # terminated while dumping
my $TERMINATED_DUMPING_TO_TAPE	 = 32;  # terminated while dumping to tape
my $TERMINATED_WRITING		 = 33;  # terminated while writing
my $TERMINATED_FLUSHING		 = 34;  # terminated while flushing
my $TERMINATED_VAULTING		 = 35;  # terminated while vaulting
my $TERMINATED_WAIT_FOR_DUMPING	 = 36;  # terminated while wait for dumping
my $TERMINATED_WAIT_FOR_WRITING	 = 37;  # terminated while wait for writing
my $TERMINATED_WAIT_FOR_FLUSHING = 38;  # terminated while wait for flushing

# status only for worker
my $TAPE_ERROR			 = 50;
my $CONFIG_ERROR		 = 51;

my $STATUS_FAILED  =  4;
my $STATUS_MISSING =  8;
my $STATUS_TAPE    = 16;

sub new {
    my $class = shift;
    my %params = @_;

    my $filename = $params{'filename'};
    my $dead_run;
    my $logdir = config_dir_relative(Amanda::Config::getconf($CNF_LOGDIR));
    if (defined $filename) {
	if ($filename =~ m,^/, ) {
	} else {
	    $filename = "$logdir/$filename" if ! -f $filename;
	}
    } else {
	if (-f "$logdir/amdump") {
	    $filename = "$logdir/amdump"
	} elsif (-f "$logdir/amflush") {
	    $filename = "$logdir/amflush"
	} else {
	    $dead_run = 1;
	    if (-f "$logdir/amflush.1" && -f "$logdir/amdump.1" &&
		-M "$logdir/amflush.1"  < -M "$logdir/amdump.1") {
		$filename = "$logdir/amflush.1";
	    } else {
		$filename = "$logdir/amdump.1";
	    }
	}
    }
    my $fd;
    if (!open ($fd, "<", "$filename")) {
	return Amanda::Status::Message->new(
		source_filename => __FILE__,
		source_line     => __LINE__,
		code   => 1800001,
		severity => $Amanda::Message::ERROR,
		amdump_log => $filename,
		errno => $!);

    } ;
    binmode($fd, ":encoding(utf-8)");  # all bytes should be valid text-based UTF8
    my $self = {
	filename    => $filename,
	fd          => $fd,
	driver_finished => 0,
	dead_run => 0,
	second_read => 0,
    };
    $self->{'dead_run'} = $dead_run if $dead_run;

    bless $self, $class;
    return $self;
}

sub set_starttime() {
    my (@tl);
    my ($time);
    my ($date);

    # Preset an array of values in case some parts are not passed as
    # arguments.  This lets the date, etc, be omitted and default to
    # today.

    ($date)=@_;
    @tl = localtime;

    $tl[5] = substr($date,  0, 4)   if(length($date) >= 4);
    $tl[4] = substr($date,  4, 2)-1 if(length($date) >= 6);
    $tl[3] = substr($date,  6, 2)   if(length($date) >= 8);
    $tl[2] = substr($date,  8, 2)   if(length($date) >= 10);
    $tl[1] = substr($date, 10, 2)   if(length($date) >= 12);
    $tl[0] = substr($date, 12, 2)   if(length($date) >= 14);

    $time = &timelocal (@tl);

    return $time;
}


sub parse {
    my $self = shift;
    my %params = @_;

REREAD:
    my $user_msg = $params{'user_msg'};
    my @datestamp;
    my %datestamp;
    my $generating_schedule = 0;
    my %dles;
    my %dumper_to_serial;
    my %chunker_to_serial;
    my %running_dumper;
    my %worker_to_serial;

    $self->{'exit_status'} = 0;
    my $line;
    my $fd = $self->{'fd'};
    while ($line = <$fd>) {
	chomp $line;
	$line =~ s/[:\s]+$//g; #remove separator at end of line
	my @line = Amanda::Util::split_quoted_strings_for_amstatus($line);
	if (!defined $line[0]) {
	    @line = split(/[:\s]+/, $line);
	}
	next if !defined $line[0];

	if ($line[0] eq "amdump" || $line[0] eq "amflush" || $line[0] eq "amvault") {
	    if ($line[1] eq "start" && $line[2] eq "at") {
		$self->{'datestr'} = $line;
		$self->{'datestr'} =~ s/.*start at //g;
	    } elsif ($line[1] eq "datestamp") {
		$self->{'datestamp'} = $line[2];
		if (!defined $datestamp{$self->{'datestamp'}}) {
		    $datestamp{$self->{'datestamp'}} = 1;
		    push @datestamp, $self->{'datestamp'};
		}
	    } elsif ($line[1] eq "starttime") {
		$self->{'starttime'} = &set_starttime($line[2]);
	    } elsif ($line[1] eq "starttime-locale-independent") {
		$self->{'starttime-locale-independent'} = $line[2] . " " . $line[3] . ":" . $line[4] . ":" . $line[5] . " " . $line[6];
	    }
	    if ($line[0] eq "amvault") {
		if ($line[1] eq 'vaulting') {
		} elsif ($line[1] eq 'status' and $line[2] eq 'file') {
		} elsif ($line[1] eq 'Done' and $line[2] eq 'vaulting') {
		} elsif ($line[1] eq 'Partial' and $line[2] eq 'vaulting') {
		} elsif ($line[1] eq 'Failed' and $line[2] eq 'vaulting') {
		}
	    }
	} elsif ($line[0] eq "planner") {
	    if ($line[1] eq 'pid' &&
		$line[3] eq 'executable' &&
		$line[5] eq 'version') {
		my $planner_version = $line[6];

		my ($version_major, $version_minor, $version_patch, $version_comment) = $self->parse_version($planner_version);
		if (ref $version_major eq "Amanda::Status::Message") {
		    return $version_major;
		}
	    } elsif ($line[1] eq "timestamp") {
		$self->{'datestamp'} = $line[2];
		if (!defined $datestamp{$self->{'datestamp'}}) {
		    $datestamp{$self->{'datestamp'}} = 1;
		    push @datestamp, $self->{'datestamp'};
		}
	    } elsif ($line[1] eq "FAILED") {
		#2:host 3:disk 4:datestamp 5:level 6:errmsg
		my $host=$line[2];
		my $disk=$line[3];
		my $datestamp=$line[4];
		my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
		$dle->{'level'} = $line[5];
		$dle->{'status'} = $ESTIMATE_FAILED;
		$dle->{'error'} = $line[6];
	    } elsif ($line[1] eq "time") {
		if($line[3] eq "got") {
		    if($line[4] eq "result") {
			my $host = $line[7];
			my $disk = $line[9];
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}};
			$dle->{'status'} = $ESTIMATE_DONE;
			$dle->{'level'} = $line[10];
			$line[12] =~ /(\d+)K/;
			$dle->{'esize'} = $1 * 1024;
			#$getest{$hostpart} = "";
			$self->{'estimated'}++;
			$self->{'estimated_size'} = $dle->{'esize'};
		    } elsif($line[4] eq "partial") {
			my $host = $line[8];
			my $disk = $line[10];
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}};
			$dle->{'status'} = $ESTIMATE_PARTIAL;
			my $level1 = $line[11];
			$line[13] =~ /(-?\d+)K/;
			my $size1 = $1 * 1024;
			my $level2 = $line[14];
			$line[16] =~ /(-?\d+)K/;
			my $size2 = $1 * 1024;
			my $level3 = $line[17];
			$line[19] =~ /(-?\d+)K/;
			my $size3 = $1 * 1024;
			if ($size1 > 0 || $size2 > 0 || $size3 > 0) {
			    my $dle = $self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}};
			    $dle->{'level'} = $line[11];
			    $dle->{'esize'} = $size1;
			    #if ($size1 > 0) { $getest{$hostpart} =~ s/:$level1://; }
			    #if ($size2 > 0) { $getest{$hostpart} =~ s/:$level2://; }
			    #if ($size3 > 0) { $getest{$hostpart} =~ s/:$level3://; }
			    #if ($getest{$hostpart} eq "") {$partialestimate{$hostpart}=0;}
			}
		    }
		} elsif($line[3] eq "getting" &&
			$line[4] eq "estimates" &&
			$line[5] eq "took") {
		}
	    }
	} elsif ($line[0] eq "setup_estimate") {
	    my $host = $line[1];
	    my $disk = $line[2];
	    $self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}} = {} if !defined($self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}});;
	    my $dle = $self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}};
	    $dle->{'status'} = $ESTIMATING;
	    $dle->{'level'} = 0;
	    $dle->{'degr_level'} = -1;
	    $dle->{'esize'} = 0;
	    delete $dle->{'error'};
	    if ($line[7] eq "last_level") {
		#$getest{$hostpart}="";
		my $level1 = $line[15];
		my $level2 = $line[17];
		my $level3 = $line[19];
		#if ($level1 != -1) { $getest{$hostpart} .= ":$level1:" };
		#if ($level2 != -1) { $getest{$hostpart} .= ":$level2:" };
		#if ($level3 != -1) { $getest{$hostpart} .= ":$level3:" };
	    }
	} elsif ($line[0] eq "GENERATING" &&
		 $line[1] eq "SCHEDULE") {
	    $generating_schedule = 1;
	} elsif ($line[0] eq "--------") {
	    if ($generating_schedule == 1) {
		$generating_schedule = 2;
		$self->{'estimated1'} = 0;
		$self->{'estimated_size1'} = 0;
	    } elsif ($generating_schedule == 2) {
		$generating_schedule = 3;
		$self->{'estimated'} = $self->{'estimated1'};
		$self->{'estimated_size'} = $self->{'estimated_size1'};
	    }
	} elsif ($line[0] eq "DUMP") {
	    if ($generating_schedule == 2 ) {
		my $host = $line[1];
		my $disk = $line[3];
		my $datestamp = $line[4];
		$self->{'dles'}->{$host}->{$disk}->{$datestamp} = {} if !defined($self->{'dles'}->{$host}->{$disk}->{$datestamp});
		my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
		$dle->{'status'} = $WAIT_FOR_DUMPING;
		$dle->{'level'} = $line[6];
		my $esize = $line[14] * 1024; #compressed size
		$esize=32768 if $esize<32768;
		$dle->{'esize'} = $esize;
		if (!defined($line[25])) {
		    $dle->{'degr_level'} = -1;
		} else {
		    $dle->{'degr_level'} = $line[17];
		    $esize=$line[25] * 1024;   #compressed size
		    $esize=32768 if $esize<32768;
		    $dle->{'degr_size'} = $esize;
		}
		$self->{'estimated1'}++;
		$self->{'estimated_size1'} += $dle->{'esize'};
	    }
	} elsif ($line[0] eq "FLUSH") {
	    my $ids = $line[1];
	    my $host = $line[2];
	    my $disk = $line[3];
	    my $datestamp = $line[4];
	    my $level = $line[5];
	    my $holding_file = $line[6];
	    $self->{'dles'}->{$host}->{$disk}->{$datestamp} = {} if !defined($self->{'dles'}->{$host}->{$disk}->{$datestamp});
	    my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
	    $dle->{'flush'} = 0;
	    $dle->{'holding_file'} = $holding_file;
	    $dle->{'level'} = $level;
	    $dle->{'size'} = $dle->{'esize'} = $self->_dump_size($holding_file);
	    for my $id1 (split ',', $ids) {
		my ($id, $storage) = split ';', $id1;
		$dle->{'storage'}->{$storage} = { status   => $WAIT_FOR_FLUSHING,
						  flushing => 1 };
		for my $taper (keys %{$self->{'taper'}}) {
		    if ($self->{'taper'}->{$taper}->{'storage'} eq $storage) {
			$self->{'storage'}->{$storage}->{'taper'} = $taper;
		    }
		}
	    }
	    $dle->{'status'} = $IDLE;
	} elsif ($line[0] eq "driver") {
	    if ($line[1] eq "pid") {
		my $pid = $line[2];
		if ($line[3] eq 'executable' &&
		    $line[5] eq 'version') {
		    my $driver_version = $line[6];

		    my ($version_major, $version_minor, $version_patch, $version_comment) = $self->parse_version($driver_version);
		    if (ref $version_major eq "Amanda::Status::Message") {
			return $version_major;
		    }
		}
		if (!Amanda::Util::is_pid_alive($pid, 'driver')) {
		    if ($self->{'second_read'}) {
			$self->{'dead_run'} = 1;
		    } else {
			$self->{'second_read'} = 1;
			close $self->{'fd'};
			my $fd;
			if (!open ($fd, "<", $self->{'filename'})) {
			    return Amanda::Status::Message->new(
					source_filename => __FILE__,
					source_line     => __LINE__,
					code   => 1800001,
					severity => $Amanda::Message::ERROR,
					amdump_log => $self->{'filename'},
					errno => $!);
			}
			$self->{'fd'} = $fd;
			goto REREAD;
		    }
		}
	    } elsif ($line[1] eq "to" && $line[2] eq "write") {
		#1:to 2:write 3:host 4:$host 5:disk 6:$disk 7:date 8:$datestamp 9:on 10:storage 11:$storage
		my $host = $line[4];
		my $disk = $line[6];
		my $datestamp = $line[8];
		my $storage = $line[11];
		my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
		$dle->{'storage'}->{$storage} = { status => $WAIT_FOR_WRITING };
	    } elsif ($line[1] eq "start" && $line[2] eq "time") {
		$self->{'start_time'} = $line[3];
		$self->{'current_time'} = $line[3];
		$self->{'dumpers_actives'}[0] = 0;
		$self->{'dumpers_held'}[0] = {};
		$self->{'dumpers_active'} = 0;
	    } elsif ($line[1] eq "adding" &&
		     $line[2] eq "holding" &&
		     $line[3] eq "disk") {
		$self->{'holding_space'} += $line[8];
	    } elsif ($line[1] eq "taper" && $line[3] eq "storage") {
		#2:taper 4:storage 5:"tape_size" 6:tape_size
		my $taper = $line[2];
		my $storage = $line[4];
		my $tape_size = $line[6] * 1024;
		$self->{'taper'}->{$taper}->{'storage'} = $storage;
		$self->{'taper'}->{$taper}->{'tape_size'} = $tape_size;
		$self->{'storage'}->{$storage}->{'taper'} = $taper;
	    } elsif ($line[1] eq "requeue" && $line[2] eq "dump" && $line[3] eq "time") {
		#1:requeue #2:dump #3:time #4:$time #5:$host #6:$disk #7:$datestamp
		my $host = $line[5];
		my $disk = $line[6];
		my $datestamp = $line[7];
		my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
		$dle->{'will_retry'} = 1;
	    } elsif ($line[1] eq "requeue" && $line[2] eq "dump_to_tape" && $line[3] eq "time") {
		#1:requeue #2:dump_to_tape #3:time #4:$time #5:$host #6:$disk #7:$datestamp [#8:$storage]
		my $host = $line[5];
		my $disk = $line[6];
		my $datestamp = $line[7];
		my $storage = $line[8];
		my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
		$dle->{'will_retry'} = 1;
		if ($storage) {
		    my $dlet = $dle->{'storage'}->{$storage};
		    $dlet->{'will_retry'} = 1;
		}
	    } elsif ($line[1] eq "requeue" && $line[2] eq "write" && $line[3] eq "time") {
		#1:requeue #2:write #3:time #4:$time #5:$host #6:$disk #7:$datestamp #8:$storage
		my $host = $line[5];
		my $disk = $line[6];
		my $datestamp = $line[7];
		my $storage = $line[8];
		my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
		my $dlet = $dle->{'storage'}->{$storage};
		$dlet->{'will_retry'} = 1;
	    } elsif ($line[1] eq "send-cmd" && $line[2] eq "time") {
		$self->{'current_time'} = $line[3];
		if ($line[5] =~ /dumper\d*/) {
		    my $dumper = $line[5];
		    if ($line[6] eq "PORT-DUMP" ||
			$line[6] eq "SHM-DUMP") {
			#7:handle 8:port 9:interface 10:maxdumps 11:host 12:amfeatures 13:disk 14:device 15:level ...
			my $host = $line[11];
			my $disk = $line[13];
			my $serial=$line[7];
			$dumper_to_serial{$line[5]} = $serial;
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}};
			$dle->{'retry'} = 0;
			$dle->{'retry_level'} = -1;
			$dle->{'will_retry'} = 0;
			$dle->{'dump_time'} = $self->{'current_time'};
			if (      $dle->{'level'} != $line[15] &&
			     $dle->{'degr_level'} == $line[15]) {
			    $dle->{'level'} = $dle->{'degr_level'};
			    $dle->{'esize'} = $dle->{'degr_size'};
			} elsif ($dle->{'level'} != $line[15]) {
			    $dle->{'level'} = $line[15];
			}
			if (!defined($self->{'busy_time'}->{$dumper})) {
			    $self->{'busy_time'}->{$dumper}=0;
			}
			#$running_dumper{$dumper} = $hostpart;
			delete $dle->{'error'};
			$dle->{'size'} = 0;
			$self->{'dumpers_active'}++;
			if (!defined($self->{'dumpers_actives'}[$self->{'dumpers_active'}])) {
			    $self->{'dumpers_actives'}[$self->{'dumpers_active'}] = 0;
			}
			if (!defined($self->{'dumpers_held'}[$self->{'dumpers_active'}])) {
			    $self->{'dumpers_held'}[$self->{'dumpers_active'}] = {};
			}
			if ($dle->{'status'} == $DUMPING_INIT) {
			    $dle->{'status'} = $DUMPING;
			} elsif ($dle->{'status'} == $DUMPING_TO_TAPE_INIT) {
			    $dle->{'status'} = $DUMPING_TO_TAPE;
			} else {
			    die ("bad status on dumper PORT-DUMP: $dle->{'status'}");
			}
		    } elsif ($line[6] eq "ABORT") {
			#7:handle 8:message
			my $serial=$line[7];
			my $dle = $dles{$serial};
			$dle->{'status'} = $DUMP_FAILED;
		    }
		} elsif ($line[5] =~ /chunker\d*/) {
		    if ($line[6] eq "PORT-WRITE" ||
			$line[6] eq "SHM-WRITE") {
			my $serial=$line[7];
			my $host = $line[9];
			my $disk = $line[11];
			my $level = $line[12];
			$chunker_to_serial{$line[5]} = $serial;
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$self->{'datestamp'}};
			$dles{$serial} = $dle;
			$dle->{'retry'} = 0;
			$dle->{'retry_level'} = -1;
			$dle->{'will_retry'} = 0;
			$dle->{'holding_file'} = $line[8];
			$dle->{'chunk_time'} = $self->{'current_time'};
			$dle->{'size'} = 0;
			$dle->{'level'} = $level;
			if ($dle->{'status'} != $WAIT_FOR_DUMPING and
			    $dle->{'status'} != $DUMP_FAILED) {
			    die ("bad status on chunker $line[6]: $dle->{'status'}");
			}
			$dle->{'status'} = $DUMPING_INIT;
		    } elsif ($line[6] eq "CONTINUE") {
			#7:handle 8:filename 9:chunksize 10:use
			my $serial=$line[7];
			my $dle = $dles{$serial};
			if ($dle) {
			    delete $dle->{'wait_holding_disk'};
			}
		    } elsif ($line[6] eq "ABORT") {
			#7:handle 8:message
			my $serial=$line[7];
			my $dle = $dles{$serial};
			if ($dle) {
			    delete $dle->{'wait_holding_disk'};
			    $dle->{'status'} = $DUMP_FAILED;
			}
		    }
		} elsif ($line[5] =~ /taper\d*/) {
		    my $taper = $line[5];
		    if ($line[6] eq "START-TAPER") {
			#7:taper 8:worker 9:storage 10:timestamp
			my $worker = $line[8];
			my $storage=$line[9];
			my $datestamp=$line[10];
			if (!defined $datestamp{$datestamp}) {
			    $datestamp{$datestamp} = 1;
			    push @datestamp, $datestamp;
			}
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'search_for_tape'} = 1;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $IDLE;
			$self->{'taper'}->{$taper}->{'nb_tape'} = 0 if !defined $self->{'taper'}->{$taper}->{'nb_tape'};
			$self->{'storage'}->{$storage}->{'taper'} = $taper;
		    } elsif ($line[6] eq "START-SCAN") {
			#7:name 8:handle
			my $worker = $line[7];
			my $serial = $line[8];
			my $dle = $dles{$serial};
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'search_for_tape'} = 1;
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'wait_for_tape'};
			if ($dle) {
			    my $storage = $self->{'taper'}->{$taper}->{'storage'};
			    my $dlet = $dle->{'storage'}->{$storage};
			    if ($dlet->{'wait_for_tape'}) {
				$dlet->{'search_for_tape'} = 1;
				delete $dlet->{'wait_for_tape'};
			    } else {
				die ("not in wait_for_tape on START-SCAN: $dlet->{'status'}");
			    }
			}
		    } elsif ($line[6] eq "NEW-TAPE") {
			#7:name 8:handle
			my $worker = $line[7];
			my $serial = $line[8];
			my $dle = $dles{$serial};
			my $storage = $self->{'taper'}->{$taper}->{'storage'};
			my $dlet = $dle->{'storage'}->{$storage};
			delete $dlet->{'wait_for_tape'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'wait_for_tape'};
		    } elsif ($line[6] eq "NO-NEW-TAPE") {
			#7:name 8:handle 9:errmsg
			my $worker = $line[7];
			my $serial = $line[8];
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'} = $line[9];
			my $dle = $dles{$serial};
			my $storage = $self->{'taper'}->{$taper}->{'storage'};
			my $dlet = $dle->{'storage'}->{$storage};
			delete $dlet->{'wait_for_tape'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'wait_for_tape'};
		    } elsif ($line[6] eq "FILE-WRITE") {
			#7:name 8:handle 9:filename 10:host 11:disk 12:level 13:datestamp 14:splitsize
			my $worker = $line[7];
			my $serial = $line[8];
			my $host = $line[10];
			my $disk = $line[11];
			my $level = $line[12];
			my $datestamp = $line[13];
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $WRITING;
			#$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status_taper'} = "Writing $host:$disk";
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'host'} = $host;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'disk'} = $disk;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'datestamp'} = $datestamp;
			if (!defined $datestamp{$datestamp}) {
			    $datestamp{$datestamp} = 1;
			    push @datestamp, $datestamp;
			}
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
			$dles{$serial} = $dle;
			if(!defined $self->{'level'}) {
			    $dle->{'level'} = $level;
			}
			my $storage = $self->{'taper'}->{$taper}->{'storage'};
			$dle->{'storage'}->{$storage} = {} if !defined $dle->{'storage'}->{$storage};
			my $dlet = $dle->{'storage'}->{$storage};
			$dlet->{'will_retry'} = 0;
			if (defined $dlet->{'flushing'}) {
			    if ($dlet->{'status'} != $WAIT_FOR_FLUSHING &&
				$dlet->{'status'} != $FLUSH_FAILED) {
				die ("bad status on taper FILE-WRITE (flushing): $dlet->{'status'}");
			    }
			    $dlet->{'status'} = $FLUSHING;
			} else {
			    if (defined $dlet->{'status'} and 
				($dlet->{'status'} != $WAIT_FOR_WRITING &&
				 $dlet->{'status'} != $WRITE_FAILED)) {
				die ("bad status on taper FILE-WRITE (writing): $dlet->{'status'}");
			    }
			    $dlet->{'status'} = $WRITING;
			}
			$dlet->{'taper_time'} = $self->{'current_time'};
			$dlet->{'taped_size'} = 0;
			delete $dlet->{'error'};
			$worker_to_serial{$worker} = $serial;
		    } elsif ($line[6] eq "PORT-WRITE" ||
			     $line[6] eq "SHM-WRITE") {
			#7:name 8:handle 9:host 10:disk 11:level 12:datestamp 13:splitsize 14:diskbuffer 15:fallback_splitsize
			my $worker = $line[7];
			my $serial = $line[8];
			my $host = $line[9];
			my $disk = $line[10];
			my $level = $line[11];
			my $datestamp = $line[12];
			#$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status_taper'} = "Writing $host:$disk";
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $DUMPING_TO_TAPE;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'host'} = $host;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'disk'} = $disk;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'datestamp'} = $datestamp;
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
			$dle->{'level'} = $level;
			$dles{$serial} = $dle;
			$dle->{'retry'} = 0;
			$dle->{'retry_level'} = -1;
			$dle->{'will_retry'} = 0;
			my $storage_name = $self->{'taper'}->{$taper}->{'storage'};
			$dle->{'dump_to_tape_storage'} = $storage_name;
			$dle->{'storage'}->{$storage_name} = {} if !defined $dle->{'storage'}->{$storage_name};
			my $dlet = $dle->{'storage'}->{$storage_name};
			$dlet->{'will_retry'} = 0;
			if ($dle->{'status'} != $WAIT_FOR_DUMPING and
			    $dle->{'status'} != $DUMP_FAILED and
			    $dle->{'status'} != $DUMP_TO_TAPE_FAILED) {
			    die ("bad status on taper $line[6] (dumper): $dle->{'status'}");
			}
			if ($dlet->{'status'} and
			    $dlet->{'status'} != $WAIT_FOR_DUMPING and
			    $dlet->{'status'} != $DUMP_FAILED and
			    $dlet->{'status'} != $DUMP_TO_TAPE_FAILED) {
			    die ("bad status on taper $line[6] (taper): $dlet->{'status'}");
			}
			$dle->{'status'} = $DUMPING_TO_TAPE_INIT;
			$dlet->{'status'} = $DUMPING_TO_TAPE;
			$dlet->{'taper_time'} = $self->{'current_time'};
			$dlet->{'taped_size'} = 0;
			delete $dlet->{'error'};
			$worker_to_serial{$worker} = $serial;
		    } elsif ($line[6] eq "VAULT-WRITE") {
			#7:name 8:handle 9:src_storage 10:src_pool 11:src_label 12:host 13:disk 14:level 15:datestamp 16:splitsize 17:diskbuffer 18:fallback_splitsize
			my $worker = $line[7];
			my $serial = $line[8];
			my $src_storage = $line[9];
			my $src_pool = $line[10];
			my $src_label = $line[11];
			my $host = $line[12];
			my $disk = $line[13];
			my $level = $line[14];
			my $datestamp = $line[15];
			#$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status_taper'} = "Writing $host:$disk";
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $VAULTING;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'host'} = $host;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'disk'} = $disk;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'datestamp'} = $datestamp;
			$self->{'dles'}->{$host}->{$disk}->{$datestamp} = {} if !defined $self->{'dles'}->{$host}->{$disk}->{$datestamp};
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
			$dle->{'level'} = $level;
			$dles{$serial} = $dle;
			$dle->{'retry'} = 0;
			$dle->{'retry_level'} = -1;
			$dle->{'will_retry'} = 0;
			my $storage_name = $self->{'taper'}->{$taper}->{'storage'};
			$dle->{'storage'}->{$storage_name} = {} if !defined $dle->{'storage'}->{$storage_name};
			my $dlet = $dle->{'storage'}->{$storage_name};
			$dlet->{'will_retry'} = 0;
			if ($dle->{'status'} != $WAIT_FOR_DUMPING &&
			    $dle->{'status'} != $VAULTING_FAILED &&
			    $dle->{'status'} != $DUMP_TO_TAPE_FAILED) {
			    #die ("bad status on taper VAULT-WRITE (dumper): $dle->{'status'}");
			}
			if ($dlet->{'status'} &&
			    $dlet->{'status'} != $WAIT_FOR_DUMPING &&
			    $dlet->{'status'} != $VAULTING_FAILED &&
			    $dlet->{'status'} != $DUMP_TO_TAPE_FAILED) {
			    die ("bad status on taper VAULT-WRITE (taper): $dlet->{'status'}");
			}
			#$dle->{'status'} = $VAULTING if !defined $dle->{'status'};
			$dle->{'status'} = $VAULTING;
			$dlet->{'status'} = $VAULTING;
			$dlet->{'taper_time'} = $self->{'current_time'};
			$dlet->{'taped_size'} = 0;
			delete $dlet->{'error'};
			$dlet->{'vaulting'} = 1;
			$dlet->{'src_storage'} = $src_storage;
			$dlet->{'src_pool'} = $src_pool;
			$dlet->{'src_label'} = $src_label;
			$worker_to_serial{$worker} = $serial;
		    } elsif ($line[6] eq "TAKE-SCRIBE-FROM") {
			#7:name1 #8:handle #9:name2
			my $worker1 = $line[7];
			my $serial = $line[8];
			my $worker2 = $line[9];
			my $dle = $dles{$serial};
			#$taper_nb{$worker1} = $taper_nb{$worker2};
			#$taper_nb{$worker2} = 0;
			if (defined $dle) {
			    $dle->{'error'} = $dle->{'olderror'} if defined $dle->{'olderror'};
			    my $storage = $self->{'taper'}->{$taper}->{'storage'};
			    my $dlet = $dle->{'storage'}->{$storage};
			}
			$self->{'taper'}->{$taper}->{'worker'}->{$worker1}->{'no_tape'} = $self->{'taper'}->{$taper}->{'worker'}->{$worker2}->{'no_tape'};
			$self->{'taper'}->{$taper}->{'worker'}->{$worker2}->{'no_tape'} = 0;
		    }
		}
	    } elsif($line[1] eq "result" && $line[2] eq "time") {
		$self->{'current_time'} = $line[3];
		if ($line[5] =~ /dumper\d+/) {
		    if ($line[6] eq "(eof)") {
			$line[6] = "FAILED";
			$line[7] = $dumper_to_serial{$line[5]};
			$line[8] = "dumper CRASH";
		    }

		    if ($line[6] eq "FAILED" || $line[6] eq "TRY-AGAIN") {
			#7:handle 8:message
			my $serial = $line[7];
			my $error = $line[8];
			my $dle = $dles{$serial};
			if ($dle->{'status'} == $DUMPING ||
			    $dle->{'status'} == $DUMP_FAILED) {
			    $dle->{'status'} = $DUMP_FAILED;
			} elsif ($dle->{'status'} == $DUMPING_TO_TAPE ||
				 $dle->{'status'} == $DUMP_TO_TAPE_FAILED) {
			    $dle->{'status'} = $DUMP_TO_TAPE_FAILED;
			    my $storage_name = $dle->{'dump_to_tape_storage'};
			    my $dlet = $dle->{'storage'}->{$storage_name};
			    $dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			} else {
			    die ("bad status on dumper FAILED: $dle->{'status'}");
			}
			$self->{'busy_time'}->{$line[5]} += ($self->{'current_time'} - $dle->{'dump_time'});
			$running_dumper{$line[5]} = "0";
			$dle->{'dump_time'} = $self->{'current_time'};
			if (!$dle->{'taper_error'}) {
			    $dle->{'error'} = "$error";
			}
			$self->{'dumpers_active'}--;
		    } elsif ($line[6] eq "RETRY") {
			#7:handle 8:delay 9:level 10:message
			my $serial = $line[7];
			my $delay = $line[8];
			my $level = $line[9];
			my $error = $line[10];
			my $dle = $dles{$serial};
			$dle->{'error'} = $error;
			$dle->{'retry'} = 1;
			$dle->{'retry_level'} = $level;
			if ($dle->{'status'} == $DUMPING ||
			    $dle->{'status'} == $DUMP_FAILED) {
			    $dle->{'status'} = $DUMP_FAILED;
			} elsif ($dle->{'status'} == $DUMPING_TO_TAPE ||
				 $dle->{'status'} == $DUMP_TO_TAPE_FAILED) {
			    $dle->{'status'} = $DUMP_TO_TAPE_FAILED;
			    my $storage_name = $dle->{'dump_to_tape_storage'};
			    my $dlet = $dle->{'storage'}->{$storage_name};
			    $dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			} else {
			    die ("bad status on dumper RETRY: $dle->{'status'}");
			}
			$self->{'busy_time'}->{$line[5]} += ($self->{'current_time'} - $dle->{'dump_time'});
			$running_dumper{$line[5]} = "0";
			$dle->{'dump_time'} = $self->{'current_time'};
			$self->{'dumpers_active'}--;
		    } elsif ($line[6] eq "DONE") {
			#7:handle 8:origsize 9:size ...
			my $serial = $line[7];
			my $origsize = $line[8] * 1024;
			my $outputsize = $line[9] * 1024;
			my $dle = $dles{$serial};
			if ($dle->{'status'} == $DUMPING) {
			    $dle->{'status'} = $DUMPING_DUMPER;
			} elsif ($dle->{'status'} == $DUMPING_TO_TAPE) {
			    $dle->{'status'} = $DUMPING_TO_TAPE_DUMPER;
			    my $storage_name = $dle->{'dump_to_tape_storage'};
			    my $dlet = $dle->{'storage'}->{$storage_name};
			    $dlet->{'status'} = $DUMPING_TO_TAPE_DUMPER;
			} elsif ($dle->{'status'} == $DUMP_FAILED) {
			} elsif ($dle->{'status'} == $DUMP_TO_TAPE_FAILED) {
			    my $storage_name = $dle->{'dump_to_tape_storage'};
			    my $dlet = $dle->{'storage'}->{$storage_name};
			    $dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			} else {
			    die("bad status on dumper DONE: $dle->{'status'}");
			}
			$dle->{'size'} = $outputsize;
			$dle->{'dsize'} = $outputsize;
			$self->{'busy_time'}->{$line[5]} += ($self->{'current_time'} - $dle->{'dump_time'});
			$running_dumper{$line[5]} = "0";
			$dle->{'dump_time'} = $self->{'current_time'};
			#$dle->{'error'} = "";
			$self->{'dumpers_active'}--;
		    } elsif ($line[6] eq "ABORT-FINISHED") {
			#7:handle
			my $serial = $line[7];
			my $dle = $dles{$serial};
			#if (defined $dle->{'taper'} == 1) {
			#    $dle->{'dump_finished'}=-1;
			#} else {
			#    $dle->{'dump_finished'}=-3;
			#}
			$self->{'busy_time'}->{$line[5]} += ($self->{'current_time'} - $dle->{'dump_time'});
			$running_dumper{$line[5]} = "0";
			$dle->{'dump_time'} = $self->{'current_time'};
			$dle->{'error'} = "dumper: (aborted)";
			$self->{'dumpers_active'}--;
		    }
		} elsif ($line[5] =~ /chunker\d+/) {
		    if ($line[6] eq "(eof)") {
			$line[6] = "FAILED";
			$line[7] = $chunker_to_serial{$line[5]};
			$line[8] = "chunker CRASH";
		    }

		    if ($line[6] eq "DONE" || $line[6] eq "PARTIAL") {
			#7:handle 8:size
			my $serial = $line[7];
			my $outputsize = $line[8] * 1024;
			my $dle = $dles{$serial};
			$dle->{'size'} = $outputsize;
			$dle->{'dsize'} = $outputsize;
			$self->{'busy_time'}->{$line[5]} +=  ($self->{'current_time'} - $dle->{'chunk_time'});
			$running_dumper{$line[5]} = "0";
			$dle->{'chunk_time'} = $self->{'current_time'};
			if ($line[6] eq "PARTIAL") {
			    $dle->{'partial'} = 1;
			} else {
			    $dle->{'partial'} = 0;
			    delete $dle->{'error'};
			}
			if ($dle->{'status'} == $DUMPING_DUMPER) {
			    $dle->{'status'} = $DUMP_DONE;
			    $self->{'dumped'}->{'nb'}++;
			    $self->{'dumped'}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'dumped'}->{'real_size'} += $dle->{'size'};
			} elsif ($dle->{'status'} == $DUMP_FAILED) {
			} else {
			    die("bad status on chunker DONE/PARTIAL ($serial): $dle->{'status'}");
			}
		    } elsif ($line[6] eq "FAILED") {
			my $serial = $line[7];
			my $dle = $dles{$serial};
			if ($dle->{'status'} != $DUMPING &&
			    $dle->{'status'} != $DUMPING_DUMPER &&
			    $dle->{'status'} != $DUMPING_INIT &&
			    $dle->{'status'} != $DUMP_FAILED) {
			    die("bad status on chunker FAILED: $dle->{'status'}");
			}
			$dle->{'status'} = $DUMP_FAILED;
			if (!exists $dle->{'error'} ||
			    !defined $dle->{'error'} ||
			    $dle->{'error'} eq '') {
			    $dle->{'error'} = $line[8];
			}
			$self->{'busy_time'}->{$line[5]} += ($self->{'current_time'} - $dle->{'chunk_time'});
			$running_dumper{$line[5]} = "0";
			$dle->{'chunk_time'} = $self->{'current_time'};
		    } elsif ($line[6] eq "RQ-MORE-DISK") {
			#7:handle
			my $serial = $line[7];
			my $dle = $dles{$serial};
			$dle->{'wait_holding_disk'} = 1;
		    }
		} elsif ($line[5] =~ /taper\d*/) {
		    my $taper = $line[5];
		    if ($line[6] eq "(eof)") {
			my $error= "taper CRASH";
			$self->{'taper'}->{$taper}->{'error'} = $error;
			# all worker fail
			foreach my $worker (keys %worker_to_serial) {
			    my $serial = $worker_to_serial{$worker};
			    my $dle = $dles{$serial};
			    if (defined $dle) {
				my $storage = $self->{'taper'}->{$taper}->{'storage'};
				my $dlet = $dle->{'storage'}->{$storage};
				if ($dlet->{'status'} == $DUMPING_TO_TAPE ||
				    $dlet->{'status'} == $DUMPING_TO_TAPE_INIT ||
				    $dlet->{'status'} == $DUMP_TO_TAPE_FAILED ||
				    $dlet->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
				    $dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
				    $dle->{'status'} = $DUMP_TO_TAPE_FAILED;
				} elsif ($dlet->{'status'} == $WRITING ||
					 $dlet->{'status'} == $WRITE_FAILED) {
				    $dlet->{'status'} = $WRITE_FAILED;
				    $dle->{'status'} = $WRITE_FAILED;
				} elsif ($dlet->{'status'} == $FLUSHING ||
					 $dlet->{'status'} == $FLUSH_FAILED) {
				    $dlet->{'status'} = $FLUSH_FAILED;
				    $dle->{'status'} = $FLUSH_FAILED;
				} elsif ($dlet->{'status'} == $VAULTING ||
					 $dlet->{'status'} == $VAULTING_FAILED) {
				    $dlet->{'status'} = $VAULTING_FAILED;
				    $dle->{'status'} = $VAULTING_FAILED;
				} else {
				    die("bad status on taper eof: $dlet->{'status'}");
				}
				$dlet->{'taper_time'} = $self->{'current_time'};
				$dlet->{'error'} = "$error";
				$dle->{'error'} = "$error" if !defined $dle->{'error'};
				undef $worker_to_serial{$worker};
			    }
			}
		    } elsif ($line[6] eq "DONE" || $line[6] eq "PARTIAL") {
			#DONE:    7:worker 8:handle 9:INPUT-GOOD 10:TAPE-GOOD 11:CRC 12:errstr
			#PARTIAL: 7:worker 8:handle 9:INPUT-* 10:TAPE-* 11:CRC 12:errstr 13:INPUT-MSG 14:TAPE-MSG
			my $worker = $line[7];
			my $serial = $line[8];
			#$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status_taper'} = "Idle";
			my $dle = $dles{$serial};
			$line[12] =~ /sec (\S+) (kb|bytes) (\d+) kps/;
			my $size;
			if ($2 eq 'kb') {
			    $size = $3 * 1024;
			} else {
			    $size = $3;
			}
			my $storage = $self->{'taper'}->{$taper}->{'storage'};
			my $dlet = $dle->{'storage'}->{$storage};
			if ($line[6] eq "DONE") {
			    if ($dle->{'status'} == $IDLE) {
			    } elsif ($dle->{'status'} == $DUMP_DONE) {
			    } elsif ($dle->{'status'} == $DUMP_FAILED) {
			    } elsif ($dle->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
				$dle->{'status'} = $DUMP_TO_TAPE_DONE;
				$self->{'dumped'}->{'nb'}++;
				$self->{'dumped'}->{'estimated_size'} += $dle->{'esize'};
				$self->{'dumped'}->{'real_size'} += $dle->{'size'};
			    } elsif ($dle->{'status'} == $DUMP_TO_TAPE_FAILED) {
				$dle->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dle->{'status'} == $VAULTING) {
				$dle->{'status'} = $VAULTING_DONE;
			    } elsif ($dle->{'status'} == $DUMP_TO_TAPE_DONE) {
			    } else {
				die("bad status on dle taper DONE/PARTIAL: $dle->{'status'}");
			    }
			    if ($dlet->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
				$dlet->{'status'} = $DUMP_TO_TAPE_DONE;
			    } elsif ($dlet->{'status'} == $DUMP_TO_TAPE_FAILED) {
				$dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dlet->{'status'} == $WRITING) {
				$dlet->{'status'} = $WRITE_DONE;
			    } elsif ($dlet->{'status'} == $FLUSHING) {
				$dlet->{'status'} = $FLUSH_DONE;
			    } elsif ($dlet->{'status'} == $VAULTING) {
				$dlet->{'status'} = $VAULTING_DONE;
			    } elsif ($dlet->{'status'} == $DUMP_TO_TAPE_DONE) {
			    } else {
				die("bad status on dlet taper DONE/PARTIAL: $dlet->{'status'}");
			    }
			} else {
			    if ($dle->{'status'} == $IDLE) {
			    } elsif ($dle->{'status'} == $DUMP_DONE) {
			    } elsif ($dle->{'status'} == $DUMP_FAILED) {
			    } elsif ($dle->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
				$dle->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dle->{'status'} == $DUMPING_TO_TAPE) {
				$dle->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dle->{'status'} == $DUMP_TO_TAPE_FAILED) {
				$dle->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } else {
				die("bad status on dle taper DONE/PARTIAL: $dle->{'status'}");
			    }
			    if ($dlet->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
				$dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dlet->{'status'} == $DUMPING_TO_TAPE) {
				$dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dlet->{'status'} == $DUMP_TO_TAPE_FAILED) {
				$dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dlet->{'status'} == $WRITING) {
				$dlet->{'status'} = $WRITE_FAILED;
			    } elsif ($dlet->{'status'} == $FLUSHING) {
				$dlet->{'status'} = $FLUSH_FAILED;
			    } else {
				die("bad status on dlet taper DONE/PARTIAL: $dlet->{'status'}");
			    }
			}
			$self->{'busy_time'}->{$taper} += ($self->{'current_time'} - $dlet->{'taper_time'});
			$dlet->{'taper_time'} = $self->{'current_time'};
			$dlet->{'size'} = $size;
			if (!defined $dle->{'size'} or $dle->{'size'} == 0) {
			    $dle->{'size'} = $size;
			}
			my $ntape = $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'no_tape'};
			$self->{'taper'}->{$taper}->{'stat'}[$ntape]->{'nb_dle'} += 1;
			delete $dle->{'taper_status_file'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'taper_status_file'};
			if ($line[6] eq "DONE") {
			    delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'host'};
			    delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'disk'};
			    delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'datestamp'};
			    delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'};
			}
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $IDLE;

			if ($line[6] eq "PARTIAL") {
			    $dlet->{'partial'} = 1;
			    if ($line[10] eq "TAPE-ERROR") {
				$dlet->{'error'} = $line[14];
				$dlet->{'tape_error'} = $line[14];
				$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $TAPE_ERROR;
				$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'} = $line[14];
			    } elsif ($line[10] eq "TAPE-CONFIG") {
				$dlet->{'error'} = $line[14];
				$dlet->{'tape_config'} = $line[14];
				$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $CONFIG_ERROR;
				$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'} = $line[14];
			    }
			    if ($line[9] eq "INPUT-ERROR") {
				$dlet->{'error'} = $line[13] if !defined $dlet->{'error'};
			    }
			} else {
			     $dlet->{'partial'} = 0;
			}
			undef $worker_to_serial{$worker};
		    } elsif($line[6] eq "PARTDONE") {
			#7:worker 8:handle 9:label 10:filenum 11:ksize 12:errstr
			my $worker = $line[7];
			my $serial = $line[8];
			my $dle = $dles{$serial};
			my $size=$line[11] * 1024;
			my $storage = $self->{'taper'}->{$taper}->{'storage'};
			my $dlet = $dle->{'storage'}->{$storage};
			$dlet->{'taped_size'} += $size;
			$dlet->{'wsize'} = $dlet->{'taped_size'} if !defined $dlet->{'wsize'} ||
								    $dlet->{'taped_size'} > $dlet->{'wsize'};
			my $ntape = $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'no_tape'};
			$self->{'taper'}->{$taper}->{'stat'}[$ntape]->{'nb_part'}++;
			$self->{'taper'}->{$taper}->{'stat'}[$ntape]->{'size'} += $size;
			$self->{'taper'}->{$taper}->{'stat'}[$ntape]->{'esize'} += $size;
		    } elsif($line[6] eq "REQUEST-NEW-TAPE") {
			#7:worker 8:serial
			my $worker = $line[7];
			my $serial = $line[8];
			my $dle = $dles{$serial};
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'wait_for_tape'} = 1;
			if (defined $dle) {
			    my $storage = $self->{'taper'}->{$taper}->{'storage'};
			    my $dlet = $dle->{'storage'}->{$storage};

			    $dlet->{'wait_for_tape'} = 1;
			}
		    } elsif($line[6] eq "NEW-TAPE") {
			#7:worker 8:serial #9:label
			my $worker = $line[7];
			my $serial = $line[8];
			my $dle = $dles{$serial};
			my $storage = $self->{'taper'}->{$taper}->{'storage'};
			$self->{'stat'}->{'storage'}->{$storage}->{'taper'} = $taper;
			my $nb_tape = $self->{'taper'}->{$taper}->{'nb_tape'}++;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'no_tape'} = $nb_tape;
			$self->{'taper'}->{$taper}->{'stat'}[$nb_tape]->{'label'} = $line[9];
			$self->{'taper'}->{$taper}->{'stat'}[$nb_tape]->{'nb_dle'} = 0;
			$self->{'taper'}->{$taper}->{'stat'}[$nb_tape]->{'nb_part'} = 0;
			$self->{'taper'}->{$taper}->{'stat'}[$nb_tape]->{'size'} = 0;
			$self->{'taper'}->{$taper}->{'stat'}[$nb_tape]->{'esize'} = 0;
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'search_for_tape'};
			if (defined $dle) {
			    my $storage = $self->{'taper'}->{$taper}->{'storage'};
			    my $dlet = $dle->{'storage'}->{$storage};
			    delete $dlet->{'search_for_tape'};
			    $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $dlet->{'status'};
			}
		    } elsif($line[6] eq "TAPER-OK") {
			#7:worker #8:label
			my $worker = $line[7];
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $IDLE;
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'taper_status_file'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'wait_for_tape'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'search_for_tape'};
		    } elsif($line[6] eq "TAPE-ERROR") {
			#7:worker 8:errstr
			my $worker = $line[7];
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $TAPE_ERROR;
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'} = $line[8];
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'taper_status_file'};
			$self->{'exit_status'} |= $STATUS_TAPE;
		    } elsif($line[6] eq "FAILED") {
			#7:worker 8:handle 9:INPUT- 10:TAPE- 11:input_message 12:tape_message
			my $worker = $line[7];
			my $serial = $line[8];
			my $dle = $dles{$serial};
			delete $dle->{'taper_status_file'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'taper_status_file'};
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $IDLE;
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'host'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'disk'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'datestamp'};
			delete $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'};
			if (defined $dle) {
			    my $storage = $self->{'taper'}->{$taper}->{'storage'};
			    delete $dle->{'wait_for_tape'};
			    delete $dle->{'search_for_tape'};
			    if ($dle->{'status'} == $IDLE) {
			    } elsif ($dle->{'status'} == $DUMP_DONE) {
			    } elsif ($dle->{'status'} == $DUMPING_TO_TAPE ||
				$dle->{'status'} == $DUMPING_TO_TAPE_DUMPER ||
				$dle->{'status'} == $DUMPING_TO_TAPE_INIT ||
				$dle->{'status'} == $DUMP_TO_TAPE_FAILED) {
				$dle->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dle->{'status'} == $VAULTING) {
				$dle->{'status'} = $VAULTING_FAILED;
			    } else {
				die ("bad status on dle taper FAILED: $dle->{'status'}");
			    }
			    my $dlet = $dle->{'storage'}->{$storage};
			    delete $dlet->{'wait_for_tape'};
			    delete $dlet->{'search_for_tape'};
			    if ($dlet->{'status'} == $DUMPING_TO_TAPE ||
				$dlet->{'status'} == $DUMPING_TO_TAPE_DUMPER ||
				$dle->{'status'} == $DUMPING_TO_TAPE_INIT ||
				$dlet->{'status'} == $DUMP_TO_TAPE_FAILED) {
				$dlet->{'status'} = $DUMP_TO_TAPE_FAILED;
			    } elsif ($dlet->{'status'} == $WRITING ||
				     $dlet->{'status'} == $WRITE_FAILED) {
				$dlet->{'status'} = $WRITE_FAILED;
			    } elsif ($dlet->{'status'} == $FLUSHING ||
				     $dlet->{'status'} == $FLUSH_FAILED) {
				$dlet->{'status'} = $FLUSH_FAILED;
			    } elsif ($dlet->{'status'} == $VAULTING ||
				     $dlet->{'status'} == $VAULTING_FAILED) {
				$dlet->{'status'} = $VAULTING_FAILED;
			    } else {
				die ("bad status on dlet taper FAILED: $dlet->{'status'}");
			    }
			    my $error;
			    if ($line[10] eq "TAPE-ERROR") {
				$error=$line[12];
				$dlet->{'tape_error'} = $error;
				$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $TAPE_ERROR;
			    } elsif ($line[10] eq "TAPE-CONFIG") {
				$error=$line[12];
				$dlet->{'tape_config'} = $error;
				$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'} = $CONFIG_ERROR;
			    } else { # INPUT-ERROR
				$error = $line[11];
				$error = $dlet->{'error'} if defined $dlet->{'error'};
			    }
			    $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'} = $error;
			    $dlet->{'taper_time'} = $self->{'current_time'};
			    $dlet->{'error'} = $error;
			}
			undef $worker_to_serial{$worker};
		    }
		}
	    } elsif($line[1] eq "finished-cmd" && $line[2] eq "time") {
		$self->{'current_time'} = $line[3];
		if($line[4] =~ /dumper\d+/) {
		}
	    } elsif($line[1] eq "dump" && $line[2] eq "failed") {
		#3:handle 4: 5: 6:"too many dumper retry"
		my $serial = $line[3];
		my $dle = $dles{$serial};
		$dle->{'error'} .= "(" . $line[6] . ")";
	    } elsif($line[1] eq "tape" && $line[2] eq "failed") {
		#3:handle 4: 5: 6:"too many dumper retry"
		my $serial = $line[3];
		my $dle = $dles{$serial};
		$dle->{'error'} .= "(" . $line[6] . ")";
	    } elsif($line[1] eq "state" && $line[2] eq "time") {
		#3:time 4:"free" 5:"kps" 6:free 7:"space" 8:space 9:"taper" 10:taper 11:"idle-dumpers" 12:idle-dumpers 13:"qlen" 14:"tapeq" 15:taper_name 16:taper 17:vault 18:"runq" 19:runq 20:"directq" 21:directq 22:"roomq" 23:roomq 24:"wakeup" 25:wakeup 26:"driver-idle" 27:driver-idle
		$self->{'current_time'} = $line[3];
		$self->{'idle_dumpers'} = $line[12];

		$self->{'network_free_kps'} = $line[6];
		$self->{'holding_free_space'} = $line[8];
		my $i = 14;
		delete $self->{'qlen'}->{'tapeq'};
		while($line[$i] eq "tapeq") {
		    $self->{'qlen'}->{'tapeq'}->{$line[$i+1]} += $line[$i+2];
		    if ($line[$i+3] eq 'tapeq' || $line[$i+3] eq 'runq') {
			$i += 3;
		    } else {
			$i += 4;
		    }
		}
		$self->{'qlen'}->{'runq'} = $line[$i+1];
		$self->{'qlen'}->{'directq'} = $line[$i+3];
		$self->{'qlen'}->{'roomq'} = $line[$i+5];

		if (defined $self->{'dumpers_actives'}) {
		    if (defined $self->{'status_driver'} and $self->{'status_driver'} ne "") {
			$self->{'dumpers_actives'}[$self->{'dumpers_active_prev'}]
				+= $self->{'current_time'} - $self->{'state_time_prev'};
			$self->{'dumpers_held'}[$self->{'dumpers_active_prev'}]{$self->{'status_driver'}}
				+= $self->{'current_time'} - $self->{'state_time_prev'};
		    }
		}
		$self->{'state_time_prev'} = $self->{'current_time'};
		$self->{'dumpers_active_prev'} = $self->{'dumpers_active'};
		$self->{'status_driver'} = $line[$i+9];
		if (!defined($self->{'dumpers_held'}[$self->{'dumpers_active'}]{$self->{'status_driver'}})) {
		    $self->{'dumpers_held'}[$self->{'dumpers_active'}]{$self->{'status_driver'}}=0;
		}

	    } elsif($line[1] eq "FINISHED") {
		$self->{'driver_finished'} = 1;
	    }
	} elsif ($line[0] eq "dump") {
	    if ($line[1] eq "of" &&
		$line[2] eq "driver" &&
		$line[3] eq "schedule" &&
		$line[4] eq "after" &&
		$line[5] eq "start" &&
		$line[6] eq "degraded" &&
		$line[7] eq "mode") {
		$self->{'start_degraded_mode'} = 1;
	    }
	} elsif ($line[0] eq "taper") {
	    if ($line[1] eq "DONE") {
	    } elsif ($line[1] eq "status" && $line[2] eq "file") {
		#1:"status" #2:"file:" #3:taper #4:worker #5:hostname #6:diskname #7:filename
		my $taper = $line[3];
		my $worker = $line[4];
		$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'taper_status_file'} = $line[7];
		my $wworker = $self->{'taper'}->{$taper}->{'worker'}->{$worker};
		my $dle = $self->{'dles'}->{$wworker->{'host'}}->{$wworker->{'disk'}}->{$wworker->{'datestamp'}};
		$dle->{'taper_status_file'} = $line[7];
	    } elsif ($line[2] eq "worker" &&
	        $line[4] eq "wrote") {
		#1:taper 2:"worker" 3:worker 4:"wrote" 5:host 6:disk
		my $tape = $line[1];
		my $worker = $line[3];
	    }
	} elsif ($line[0] eq "splitting" &&
		 $line[1] eq "chunk" &&
		 $line[2] eq "that" &&
		 $line[3] eq "started" &&
		 $line[4] eq "at" &&
		 $line[6] eq "after") {
	    $line[7] =~ /(\d*)kb/;
	    my $size = $1 * 1024;
	} else {
	    #print "Ignoring: $line\n";
	}
    }

    return undef;
}

sub set_summary {
    my $self = shift;

    delete $self->{'stat'};

    $self->{'stat'}->{'estimated'}->{'nb'} = $self->{'estimated'};
    $self->{'stat'}->{'estimated'}->{'estimated_size'} = $self->{'estimated_size'};
    $self->{'stat'}->{'dumped'}->{'nb'} = $self->{'dumped'}->{'nb'};
    $self->{'stat'}->{'dumped'}->{'estimated_size'} = $self->{'dumped'}->{'estimated_size'};
    $self->{'stat'}->{'dumped'}->{'real_size'} = $self->{'dumped'}->{'real_size'};
    foreach my $host (sort keys %{$self->{'dles'}}) {
	foreach my $disk (sort keys %{$self->{'dles'}->{$host}}) {
            foreach my $datestamp (sort keys %{$self->{'dles'}->{$host}->{$disk}}) {
		my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
		delete $dle->{'message'};
		delete $dle->{'wsize'};
		delete $dle->{'dsize'};
		delete $dle->{'failed_to_tape'};
		delete $dle->{'taped'};

		if ($dle->{'status'} == $IDLE) {
		} elsif ($dle->{'status'} == $ESTIMATING) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $dle->{'message'} = "getting estimate";
		} elsif ($dle->{'status'} == $ESTIMATE_PARTIAL) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $dle->{'message'} = "partial estimate";
		} elsif ($dle->{'status'} == $ESTIMATE_DONE) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $dle->{'message'} = "estimate done";
		} elsif ($dle->{'status'} == $ESTIMATE_FAILED) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $dle->{'message'} = "estimate failed";
		    $self->{'exit_status'} |= $STATUS_FAILED;
		} elsif ($dle->{'status'} == $WAIT_FOR_DUMPING) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'wait_for_dumping'}->{'nb'}++;
		    $self->{'stat'}->{'wait_for_dumping'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'message'} = "wait for dumping";
		} elsif ($dle->{'status'} == $DUMPING_INIT) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dumping'}->{'nb'}++;
		    $self->{'stat'}->{'dumping'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'message'} = "dumping";
		} elsif ($dle->{'status'} == $DUMPING) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dumping'}->{'nb'}++;
		    $self->{'stat'}->{'dumping'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'message'} = "dumping";
		    $dle->{'wsize'} = $self->_dump_size($dle->{'holding_file'});
		} elsif ($dle->{'status'} == $DUMPING_DUMPER) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dumping'}->{'nb'}++;
		    $self->{'stat'}->{'dumping'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'message'} = "dumping";
		    $dle->{'wsize'} = $self->_dump_size($dle->{'holding_file'});
		} elsif ($dle->{'status'} == $DUMPING_TO_TAPE_INIT) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dumping_to_tape'}->{'nb'}++;
		    $self->{'stat'}->{'dumping_to_tape'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'message'} = "dumping to tape";
		} elsif ($dle->{'status'} == $DUMPING_TO_TAPE) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dumping_to_tape'}->{'nb'}++;
		    $self->{'stat'}->{'dumping_to_tape'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'message'} = "dumping to tape";
		    $self->_set_taper_size($dle);
		    $self->{'stat'}->{'dumping_to_tape'}->{'write_size'} += $dle->{'wsize'};
		} elsif ($dle->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dumping_to_tape'}->{'nb'}++;
		    $self->{'stat'}->{'dumping_to_tape'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'message'} = "dumping to tape";
		    $self->_set_taper_size($dle);
		    $self->{'stat'}->{'dumping_to_tape'}->{'write_size'} += $dle->{'wsize'};
		} elsif ($dle->{'status'} == $DUMP_FAILED) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dump_failed'}->{'nb'}++;
		    $self->{'stat'}->{'dump_failed'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'error'} = "unknown" if !defined $dle->{'error'};
		    $dle->{'dsize'} = $dle->{'size'};
		    if ($dle->{'will_retry'}) {
			if (defined $dle->{'retry'} and $dle->{'retry'} and
			    defined $dle->{'retry_level'} and $dle->{'retry_level'} != -1) {
			    $dle->{'message'} = "dump failed: $dle->{'error'}; will retry at level $dle->{'retry_level'}";
			} else {
			    $dle->{'message'} = "dump failed: $dle->{'error'}; will retry";
			}
		    } else {
			$dle->{'message'} = "dump failed: $dle->{'error'}";
		    }
		    $self->{'exit_status'} |= $STATUS_FAILED;
		} elsif ($dle->{'status'} == $DUMP_TO_TAPE_FAILED) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'dump_to_tape_failed'}->{'nb'}++;
		    $self->{'stat'}->{'dump_to_tape_failed'}->{'estimated_size'} += $dle->{'esize'};
		    $dle->{'error'} = "unknown" if !defined $dle->{'error'};
		    $dle->{'message'} = "dump to tape failed: $dle->{'error'}";
		    $dle->{'dsize'} = $dle->{'size'};
		    $self->{'exit_status'} |= $STATUS_FAILED;
		} elsif ($dle->{'status'} == $DUMP_DONE) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $dle->{'message'} = "dump done";
		    #$dle->{'wsize'} = $dle->{'size'};
		    $dle->{'dsize'} = $dle->{'size'};
		} elsif ($dle->{'status'} == $DUMP_TO_TAPE_DONE) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $dle->{'message'} = "dump to tape done";
		    #$dle->{'wsize'} = $dle->{'size'};
		    $dle->{'dsize'} = $dle->{'size'};
		} elsif ($dle->{'status'} == $VAULTING) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'vaulting'}->{'nb'}++;
		    $self->{'stat'}->{'vaulting'}->{'estimated_size'} += $dle->{'esize'};
		    $self->{'stat'}->{'vaulting'}->{'real_size'} += $dle->{'size'};
		    $dle->{'message'} = "vaulting";
		    #$dle->{'wsize'} = $dle->{'size'};
		    $dle->{'dsize'} = $dle->{'size'};
		} elsif ($dle->{'status'} == $VAULTING_DONE) {
		    $self->{'stat'}->{'disk'}->{'nb'}++;
		    $self->{'stat'}->{'vaulted'}->{'nb'}++;
		    $self->{'stat'}->{'vaulted'}->{'estimated_size'} += $dle->{'esize'};
		    $self->{'stat'}->{'vaulted'}->{'real_size'} += $dle->{'size'};
		    $dle->{'dsize'} = $dle->{'size'};
		} elsif ($dle->{'status'} == $FLUSH_FAILED) {
# JLM
		} elsif ($dle->{'status'} == $VAULTING_FAILED) {
# JLM
		} elsif ($dle->{'status'} == $WRITE_FAILED) {
# JLM
		} else {
		    die("Bad dle status: $dle->{'status'}");
		}

		$dle->{'flush'} = 0;
		if ($dle->{'storage'}) {
		    for my $storage (keys %{$dle->{'storage'}}) {
			my $dlet = $dle->{'storage'}->{$storage};
			delete $dlet->{'message'};
			delete $dlet->{'wsize'};
			delete $dlet->{'dsize'};
			my $status = $dlet->{'status'};

			if ($dlet->{'status'} == $IDLE) {
			    $dlet->{'message'} = "Idle";
			} elsif ($dlet->{'status'} == $WAIT_FOR_FLUSHING) {
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $self->{'stat'}->{'wait_to_flush'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'wait_to_flush'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'wait_to_flush'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $dlet->{'message'} = "wait for flushing";
			} elsif ($dlet->{'status'} == $WAIT_FOR_WRITING) {
			    $self->{'stat'}->{'wait_for_writing'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'wait_for_writing'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'wait_for_writing'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    if (!$dle->{'wait_for_writing'}) {
				$self->{'stat'}->{'wait_for_writing'}->{'estimated_size'} += $dle->{'esize'};
				$dle->{'wait_for_writing'} = 1;
			    }
			    $dlet->{'message'} = "wait for writing";
			} elsif ($dlet->{'status'} == $FLUSHING) {
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $dlet->{'message'} = "flushing";
			    $self->_set_taper_size($dle, $dlet);
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'write_size'} += $dlet->{'wsize'};
			} elsif ($dlet->{'status'} == $WRITING) {
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $dlet->{'message'} = "writing";
			    $self->_set_taper_size($dle, $dlet);
			    if (!$dle->{'writing_to_tape'}) {
				$self->{'stat'}->{'writing_to_tape'}->{'estimated_size'} += $dle->{'esize'};
				$self->{'stat'}->{'writing_to_tape'}->{'write_size'} += $dlet->{'wsize'};
				$dle->{'writing_to_tape'} = 1;
			    }
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'write_size'} += $dlet->{'wsize'};
			} elsif ($dlet->{'status'} == $VAULTING) {
			    $self->{'stat'}->{'vaulting'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'vaulting'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'vaulting'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $dlet->{'message'} = "vaulting";
			    $self->_set_taper_size($dle, $dlet);
			    if (!$dle->{'vaulting'}) {
				$self->{'stat'}->{'vaulting'}->{'estimated_size'} += $dle->{'esize'};
				$self->{'stat'}->{'vaulting'}->{'write_size'} += $dlet->{'wsize'};
				$dle->{'vaulting'} = 1;
			    }
			    $self->{'stat'}->{'writing_to_tape'}->{'storage'}->{$storage}->{'write_size'} += $dlet->{'wsize'};
			} elsif ($dlet->{'status'} == $DUMPING_TO_TAPE) {
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->_set_taper_size($dle, $dlet);
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dlet->{'wsize'};
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'write_size'} += $dlet->{'wsize'};
			    $dlet->{'message'} = "dumping to tape";
			} elsif ($dlet->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->_set_taper_size($dle, $dlet);
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dlet->{'wsize'};
			    $self->{'stat'}->{'dumping_to_tape'}->{'storage'}->{$storage}->{'write_size'} += $dlet->{'wsize'};
			    $dlet->{'message'} = "dumping to tape";
			} elsif ($dlet->{'status'} == $DUMP_TO_TAPE_FAILED) {
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    #$self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'esize'};
			    if (!$dle->{'failed_to_tape'}) {
				$self->{'stat'}->{'failed_to_tape'}->{'estimated_size'} += $dle->{'esize'};
				$dle->{'failed_to_tape'} = 1;
			    }
			    $dlet->{'message'} = "dump to tape failed";
			    $dlet->{'wsize'} = $dle->{'wsize'};
			    $self->{'exit_status'} |= $STATUS_FAILED;
			    $self->{'exit_status'} |= $STATUS_TAPE;
			} elsif ($dlet->{'status'} == $WRITE_FAILED) {
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    if (!$dle->{'failed_to_tape'}) {
				$self->{'stat'}->{'failed_to_tape'}->{'estimated_size'} += $dle->{'esize'};
				$dle->{'failed_to_tape'} = 1;
			    }
			    $dlet->{'message'} = "write failed";
			    if ($dlet->{'will_retry'}) {
				$dlet->{'message'} .= " (will retry)";
			    }
			    $self->{'exit_status'} |= $STATUS_TAPE;
			} elsif ($dlet->{'status'} == $FLUSH_FAILED) {
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $dlet->{'message'} = "flush failed";
			    if ($dlet->{'will_retry'}) {
				$dlet->{'message'} .= " (will retry)";
			    }
			    $self->{'exit_status'} |= $STATUS_TAPE;
			} elsif ($dlet->{'status'} == $FLUSH_DONE) {
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'flush'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    if ($dlet->{'partial'}) {
				$dlet->{'message'} = "partially flushed";
			    } else {
				$dlet->{'message'} = "flushed";
			    }
			    #$dlet->{'wsize'} = $dle->{'size'};
			    $dlet->{'dsize'} = $dle->{'size'};
			    $dle->{'dsize'} = $dle->{'size'};
			} elsif ($dlet->{'status'} == $WRITE_DONE) {
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    if (!$dle->{'taped'}) {
				$self->{'stat'}->{'taped'}->{'estimated_size'} += $dle->{'esize'};
				$dle->{'taped'} = 1;
			    }
			    if ($dlet->{'partial'}) {
				$dlet->{'message'} = "partially written";
			    } else {
				$dlet->{'message'} = "written";
			    }
			    #$dlet->{'wsize'} = $dle->{'size'};
			    $dlet->{'dsize'} = $dle->{'size'};
			    $dle->{'dsize'} = $dle->{'size'};
			} elsif ($dlet->{'status'} == $VAULTING_DONE) {
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    if (!$dle->{'taped'}) {
				$self->{'stat'}->{'taped'}->{'estimated_size'} += $dle->{'esize'};
				$dle->{'taped'} = 1;
			    }
			    if ($dlet->{'partial'}) {
				$dlet->{'message'} = "partially vaulted";
			    } else {
				$dlet->{'message'} = "vaulted";
			    }
			    #$dlet->{'wsize'} = $dle->{'size'};
			    $dlet->{'dsize'} = $dle->{'size'};
			    $dle->{'dsize'} = $dle->{'size'};
			} elsif ($dlet->{'status'} == $DUMP_TO_TAPE_DONE) {
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'taped'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    if (!$dle->{'taped'}) {
				$self->{'stat'}->{'taped'}->{'estimated_size'} += $dle->{'esize'};
				$dle->{'taped'} = 1;
			    }
			    $dlet->{'message'} = "dump to tape done";
			    #$dlet->{'wsize'} = $dle->{'size'};
			    $dlet->{'dsize'} = $dle->{'size'};
			    $dle->{'dsize'} = $dle->{'size'};
			} elsif ($dlet->{'status'} == $CONFIG_ERROR) {
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'nb'}++;
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'estimated_size'} += $dle->{'esize'};
			    $self->{'stat'}->{'failed_to_tape'}->{'storage'}->{$storage}->{'real_size'} += $dle->{'size'};
			    $dlet->{'message'} = $dlet->{'tape_config'};
			}

			if ($dlet->{'wait_for_tape'}) {
			    $dlet->{'message'} = "waiting for a tape";
			} elsif ($dlet->{'search_for_tape'}) {
			    $dlet->{'message'} = "searching for a tape";
			}
		    }
		}
	    }
	}
    }

    if (defined $self->{'qlen'}->{'tapeq'}) {
	for my $taper (keys %{$self->{'qlen'}->{'tapeq'}}) {
	    my $storage = $self->{'taper'}->{$taper}->{'storage'};

	    next if !$storage;

	    if (defined $self->{'taper'}->{$taper}->{'worker'}) {
		for my $worker (keys %{$self->{'taper'}->{$taper}->{'worker'}}) {
		    my $wstatus = $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'status'};

		    if ($wstatus == $IDLE) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "Idle";
		    } elsif ($wstatus == $TAPE_ERROR) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "tape error: $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'}";
		    } elsif ($wstatus == $CONFIG_ERROR) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "config error: $self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'error'}";
		    } elsif ($wstatus == $WRITING) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "writing";
		    } elsif ($wstatus == $FLUSHING) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "flushing";
		    } elsif ($wstatus == $VAULTING) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "vaulting";
		    } elsif ($wstatus == $DUMPING_TO_TAPE_INIT) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "dumping to tape";
		    } elsif ($wstatus == $DUMPING_TO_TAPE) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "dumping to tape";
		    } elsif ($wstatus == $DUMPING_TO_TAPE_DUMPER) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "dumping to tape";
		    } else {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "Unknown ($wstatus)";
		    }

		    if ($self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'wait_for_tape'}) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "waiting for a tape";
		    } elsif ($self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'search_for_tape'}) {
			$self->{'taper'}->{$taper}->{'worker'}->{$worker}->{'message'} = "searching for a tape";
		    }
		}
	    }
	}
    }

    $self->_summary('disk', 'disk', 0, 0, 0, 0);
    $self->_summary('estimated', 'estimated', 0, 1, 0, 0);
    $self->_summary_storage('flush', 'flush', 1, 0, 0, 0);
    $self->_summary('dump_failed', 'dump failed', 0, 1, 0, 1);
    $self->_summary('wait_for_dumping', 'wait for dumping', 0, 1, 0, 1);
    $self->_summary('dumping_to_tape', 'dumping to tape', 1, 1, 1, 1);
    $self->_summary('dumping', 'dumping', 1, 1, 1, 1);
    $self->_summary('dumped', 'dumped', 1, 1, 1, 1);
    $self->_summary_storage('wait_for_writing', 'wait for writing', 1, 1, 1, 1);
    $self->_summary_storage('wait_to_flush'   , 'wait_to_flush'   , 1, 1, 1, 1);
    $self->_summary_storage('wait_to_vault'   , 'wait_to_vault'   , 1, 1, 1, 1);
    $self->_summary_storage('writing_to_tape' , 'writing to tape' , 1, 1, 1, 1);
    $self->_summary_storage('dumping_to_tape' , 'dumping to tape' , 1, 1, 1, 1);
    $self->_summary_storage('vaulting'        , 'vaulting'        , 1, 1, 1, 1);
    $self->_summary_storage('failed_to_tape'  , 'failed to tape'  , 1, 1, 1, 1);
    $self->_summary_storage('taped'           , 'taped'           , 1, 1, 1, 1);
    $self->_summary_storage('vaulted'         , 'vaulted'         , 1, 1, 1, 1);

    delete $self->{'busy'};
    delete $self->{'busy_dumper'};

    if (defined $self->{'current_time'} and
	defined $self->{'start_time'} and
	$self->{'current_time'} != $self->{'start_time'}) {
	my $total_time = $self->{'current_time'} - $self->{'start_time'};

	foreach my $key (keys %{$self->{'busy_time'}}) {
	    my $type = $key;
	       $type =~ s/[0-9]*$//g;
	    my $name = $key;
	    if ($key =~ /^taper/) {
		$name = $self->{'taper'}->{$key}->{'storage'};
		$self->{'busy'}->{$key}->{'storage'} = $name;
	    }
	    $self->{'busy'}->{$key}->{'type'} = $type;
	    $self->{'busy'}->{$key}->{'time'} = $self->{'busy_time'}->{$key};
	    $self->{'busy'}->{$key}->{'percent'} =
		 ($self->{'busy_time'}->{$key} * 1.0 / $total_time) * 100;
	}

	if (defined $self->{'dumpers_actives'}) {
	    for (my $d = 0; $d < @{$self->{'dumpers_actives'}}; $d++) {
		$self->{'busy_dumper'}->{$d}->{'time'} =
			$self->{'dumpers_actives'}[$d];
		$self->{'busy_dumper'}->{$d}->{'percent'} =
			($self->{'dumpers_actives'}[$d] * 1.0 / $total_time) * 100;

		foreach my $key (keys %{$self->{'dumpers_held'}[$d]}) {
		    next unless $self->{'dumpers_held'}[$d]{$key} >= 1;
		    $self->{'busy_dumper'}->{$d}->{'status'}->{$key}->{'time'} =
			$self->{'dumpers_held'}[$d]{$key};
		    $self->{'busy_dumper'}->{$d}->{'status'}->{$key}->{'percent'} =
			($self->{'dumpers_held'}[$d]{$key} * 1.0 /
			 $self->{'dumpers_actives'}[$d]) * 100;
		}
	    }
	}
    }

    if ($self->{'dead_run'}) {
	if (!$self->{'driver_finished'}) {
	    $self->{'exit_status'} |= $STATUS_FAILED;
	}
	{
	    foreach my $host (sort keys %{$self->{'dles'}}) {
		foreach my $disk (sort keys %{$self->{'dles'}->{$host}}) {
	            foreach my $datestamp (sort keys %{$self->{'dles'}->{$host}->{$disk}}) {
			my $dle = $self->{'dles'}->{$host}->{$disk}->{$datestamp};
			if ($dle->{'status'} == $WAIT_FOR_DUMPING) {
			    $self->{'exit_status'} |= $STATUS_MISSING;
			    $dle->{'status'} = $TERMINATED_WAIT_FOR_DUMPING;
			    $dle->{'message'} = "terminated while waiting for dumping";
			} elsif ($dle->{'status'} == $ESTIMATING ||
				 $dle->{'status'} == $ESTIMATE_PARTIAL) {
			    $dle->{'status'} = $TERMINATED_ESTIMATE;
			    $dle->{'message'} = "terminated while estimating";
			} elsif ($dle->{'status'} == $DUMPING_INIT ||
				 $dle->{'status'} == $DUMPING ||
				 $dle->{'status'} == $DUMPING_DUMPER) {
			    $dle->{'status'} = $TERMINATED_DUMPING;
			    $dle->{'message'} = "terminated while dumping";
			} elsif ($dle->{'status'} == $DUMPING_TO_TAPE_INIT ||
				 $dle->{'status'} == $DUMPING_TO_TAPE ||
				 $dle->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
			    $dle->{'status'} = $TERMINATED_DUMPING_TO_TAPE;
			    $dle->{'message'} = "terminated while dumping to tape";
			} elsif ($dle->{'status'} == $WRITING) {
			    $dle->{'status'} = $TERMINATED_WRITING;
			    $dle->{'message'} = "terminated while writing to tape";
			} elsif ($dle->{'status'} == $FLUSHING) {
			    $dle->{'status'} = $TERMINATED_FLUSHING;
			    $dle->{'message'} = "terminated while flushing to tape";
			} elsif ($dle->{'status'} == $VAULTING) {
			    $dle->{'status'} = $TERMINATED_VAULTING;
			    $dle->{'message'} = "terminated while vaulting";
			}
			if (exists $dle->{'storage'}) {
			    foreach my $storage (values %{$dle->{'storage'}}) {
				if ($storage->{'status'} == $WAIT_FOR_DUMPING) {
				    $self->{'exit_status'} |= $STATUS_MISSING;
				    $storage->{'status'} = $TERMINATED_WAIT_FOR_DUMPING;
				    $storage->{'message'} = "terminated while waiting for dumping";
				} elsif ($storage->{'status'} == $WAIT_FOR_FLUSHING) {
				    $storage->{'status'} = $TERMINATED_WAIT_FOR_FLUSHING;
				    $storage->{'message'} = "terminated while waiting for flushing";
				} elsif ($storage->{'status'} == $WAIT_FOR_WRITING) {
				    $storage->{'status'} = $TERMINATED_WAIT_FOR_WRITING;
				    $storage->{'message'} = "terminated while waiting for writing";
				} elsif ($storage->{'status'} == $ESTIMATING ||
					 $storage->{'status'} == $ESTIMATE_PARTIAL) {
				    $storage->{'status'} = $TERMINATED_ESTIMATE;
				    $storage->{'message'} = "terminated while estimating";
				} elsif ($storage->{'status'} == $DUMPING_INIT ||
					 $storage->{'status'} == $DUMPING ||
					 $storage->{'status'} == $DUMPING_DUMPER) {
				    $storage->{'status'} = $TERMINATED_DUMPING;
				    $storage->{'message'} = "terminated while dumping";
				} elsif ($storage->{'status'} == $DUMPING_TO_TAPE_INIT ||
					 $storage->{'status'} == $DUMPING_TO_TAPE ||
					 $storage->{'status'} == $DUMPING_TO_TAPE_DUMPER) {
				    $storage->{'status'} = $TERMINATED_DUMPING_TO_TAPE;
				    $storage->{'message'} = "terminated while dumping to tape";
				} elsif ($storage->{'status'} == $WRITING) {
				    $storage->{'status'} = $TERMINATED_WRITING;
				    $storage->{'message'} = "terminated while writing to tape";
				} elsif ($storage->{'status'} == $FLUSHING) {
				    $storage->{'status'} = $TERMINATED_FLUSHING;
				    $storage->{'message'} = "terminated while flushing to tape";
				} elsif ($storage->{'status'} == $VAULTING) {
				    $storage->{'status'} = $TERMINATED_VAULTING;
				    $storage->{'message'} = "terminated while vaulting";
				}
			   }
                       }
		    }
		}
	    }
	}
    }
}

sub _summary {
    my $self = shift;
    my $key = shift;
    my $name = shift;
    my $set_real_size = shift;
    my $set_estimated_size = shift;
    my $set_real_stat = shift;
    my $set_estimated_stat = shift;

    my $nb = $self->{'stat'}->{$key}->{'nb'};
    if (!defined $nb) {
	$nb = $self->{'stat'}->{$key}->{'nb'} = 0;
    }
    $self->{'stat'}->{$key}->{'name'} = $name;
    my $real_size;
    $real_size = $self->{'stat'}->{$key}->{'real_size'} || 0 if $set_real_size;
    $self->{'stat'}->{$key}->{'real_size'} = $real_size;
    $real_size ||= 0;

    my $estimated_size;
    $estimated_size = $self->{'stat'}->{$key}->{'estimated_size'} || 0 if $set_estimated_size;
    $self->{'stat'}->{$key}->{'estimated_size'} = $estimated_size;
    $estimated_size ||= 0;

    my $est_size = $self->{'stat'}->{'estimated'}->{'estimated_size'};

    delete $self->{'stat'}->{$key}->{'real_stat'};
    $self->{'stat'}->{$key}->{'real_stat'} = $estimated_size ? ($real_size * 1.0 / $estimated_size) * 100 : 0.0 || 0 if $set_real_stat;
    delete $self->{'stat'}->{$key}->{'estimated_stat'};
    $self->{'stat'}->{$key}->{'estimated_stat'} = $est_size ? ($real_size * 1.0 / $est_size) * 100 : 0.0 || 0 if $set_estimated_stat;
}


sub _summary_storage {
    my $self = shift;
    my $key = shift;
    my $name = shift;
    my $set_real_size = shift;
    my $set_estimated_size = shift;
    my $set_real_stat = shift;
    my $set_estimated_stat = shift;

    #return if $nb_storage == 0;
    $self->{'stat'}->{$key}->{'name'} = $name;

    return if !$self->{'stat'}->{$key}->{'storage'};
    for my $storage (sort keys %{$self->{'stat'}->{$key}->{'storage'}}) {
	my $nb = $self->{'stat'}->{$key}->{'storage'}->{$storage}->{'nb'};
	my $real_size;
	$real_size = $self->{'stat'}->{$key}->{'storage'}->{$storage}->{'real_size'} || 0 if $set_real_size;
	$self->{'stat'}->{$key}->{'storage'}->{$storage}->{'real_size'} = $real_size;

	my $estimated_size;
	$estimated_size = $self->{'stat'}->{$key}->{'storage'}->{$storage}->{'estimated_size'} || 0 if $set_estimated_size;
	$self->{'stat'}->{$key}->{'storage'}->{$storage}->{'estimated_size'} = $estimated_size;

	my $est_size = $self->{'stat'}->{'estimated'}->{'estimated_size'};

	$self->{'stat'}->{$key}->{'storage'}->{$storage}->{'real_stat'} = $estimated_size ? ($real_size * 1.0 / $estimated_size) * 100 : 0.0 || 0 if $set_real_stat;
	$self->{'stat'}->{$key}->{'storage'}->{$storage}->{'estimated_stat'} = $est_size ? ($real_size * 1.0 / $est_size) * 100 : 0.0 || 0 if $set_estimated_stat;

	my $i = 0;
	my $taper = $self->{'storage'}->{$storage}->{'taper'};
	while ($i < $self->{'taper'}->{$taper}->{'nb_tape'}) {
	    my $percent = (1.0 * $self->{'taper'}->{$taper}->{'stat'}[$i]->{'size'}) / $self->{'taper'}->{$taper}->{'tape_size'} * 100.0;
	    $self->{'taper'}->{$taper}->{'stat'}[$i]->{'percent'} = $percent;
	    $i++;
	}
    }
}

sub current {
    my $self = shift;
    my %params = @_;

    my $message = $self->parse();
    return $message if defined $message;

    $self->set_summary();

    my $data = {
		 filename      => $self->{'filename'},
		 dead_run      => $self->{'dead_run'},
		 aborted       => !$self->{'driver_finished'},
		 datestamp     => $self->{'datestamp'},
		 dles          => $self->{'dles'},
		 stat          => $self->{'stat'},
		 taper         => $self->{'taper'},
		 idle_dumpers  => $self->{'idle_dumpers'},
		 status_driver => $self->{'status_driver'},
		 storage       => $self->{'storage'},
		 qlen          => $self->{'qlen'},
		 network_free_kps      => $self->{'network_free_kps'},
		 holding_free_space    => $self->{'holding_free_space'},
		 holding_space => $self->{'holding_space'},
		 busy          => $self->{'busy'},
		 busy_dumper   => $self->{'busy_dumper'},
		 starttime     => $self->{'starttime'},
		 current_time  => $self->{'current_time'},
		 exit_status   => $self->{'exit_status'}
	       };

    return Amanda::Status::Message->new(
		source_filename => __FILE__,
		source_line     => __LINE__,
		code   => 1800000,
		severity => $Amanda::Message::INFO,
		status => $data);
}

#sub stream {
#    my $self = shift;
#    my %params = @_;
#
#    $self->parse(user_msg => $params{'user_msg'});
#}

sub _dump_size() {
    my $self = shift;
    my $filename = shift;
    my $dsize = 0;
    my($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
       $atime,$mtime,$ctime,$blksize,$blocks);

    while ($filename ne "") {
	$filename = "$filename.tmp" if (!(-e "$filename"));
	$filename = "/dev/null" if (!(-e "$filename"));
	($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
	 $atime,$mtime,$ctime,$blksize,$blocks) = stat($filename);
	$size=$size-32768 if $size > 32768;
	$dsize += $size;
	open(DUMP,$filename);
	$filename = "";
	while(<DUMP>) {
	    if(/^CONT_FILENAME=(.*)$/) { $filename = $1; last }
	    last if /^To restore, position tape at start of file and run/;
	}
	close(DUMP);
    }
    return $dsize;
}

sub _set_taper_size {
    my $self = shift;
    my $dle = shift;
    my $dlet = shift;

    $dle->{'wsize'} = 0;
    $dlet->{'wsize'} = 0 if defined $dlet;
    if ($dle->{'taper_status_file'} and -f $dle->{'taper_status_file'} and
	open FF, "<$dle->{'taper_status_file'}") {
	my $line = <FF>;
	if (defined $line) {
	    chomp $line;
	    my $value = $line;
	    if ($value) {
		if (defined $dlet) {
		    $dlet->{'wsize'} = $value if (!defined $dlet->{'wsize'} || $value > $dlet->{'wsize'});
		} else {
		    $dle->{'wsize'} = $value if (!defined $dle->{'wsize'} || $value > $dle->{'wsize'});
		}
	    }
	}
	close FF;
    }
    $dlet->{'wsize'} = $dlet->{'taped_size'} if defined $dlet->{'taped_size'} &&
							$dlet->{'taped_size'} > $dlet->{'wsize'};
}

sub show_time {
    my $status = shift;
    my $delta = shift;
    my $oneday = 24*60*60;

    my $starttime = $status->{'starttime'};
    my @starttime = localtime($starttime);
    my @now = localtime($starttime+$delta);
    my $now_yday = $now[7];
    my $result;

    if ($starttime[5] < $now[5]) {
	my $days_in_year = 364;
	my $startime1 = $starttime;
	while ($startime1 < $starttime+$delta) {
	    my @starttime1 = localtime($starttime);
	    if ($starttime1[7] > $days_in_year) {
		$days_in_year = $starttime1[7];
	    }
	    $startime1 += $oneday;
	}
	$now_yday += $days_in_year+1;
    }

    if ($starttime[7] < $now_yday) {
	$result = sprintf("%d+", $now_yday - $starttime[7]);
    } else {
	$result = "";
    }
    $result .= sprintf("%2d:%02d:%02d",$now[2],$now[1],$now[0]);
    return $result;
}

sub parse_version {
    my $self = shift;
    my $version = shift;

    my ($version_major, $version_minor, $version_patch, $version_comment);

    if ($version =~ /^(\d*)\.(\d*)\.(\d*)(.*)$/) {
	$version_major = $1;
	$version_minor = $2;
	$version_patch = $3;
	$version_comment = $4;
    } elsif ($version =~ /^(\d*)\.(\d*)(.*)$/) {
	$version_major = $1;
	$version_minor = $2;
	$version_patch = 0;
	$version_comment = $3;
    } elsif ($version =~ /^(\d*)(.*)$/) {
	$version_major = $1;
	$version_minor = 0;
	$version_patch = 0;
	$version_comment = $2;
    }

    if ($version_major ne $Amanda::Constants::VERSION_MAJOR ||
	$version_minor ne $Amanda::Constants::VERSION_MINOR) {
	return Amanda::Status::Message->new(
					source_filename => __FILE__,
					source_line     => __LINE__,
					code   => 1800002,
					severity => $Amanda::Message::ERROR,
					amdump_log => $self->{'filename'},
					version => $Amanda::Constants::VERSION,
					amdump_version => $version);
    }

    return ($version_major, $version_minor, $version_patch, $version_comment);
}
1;
