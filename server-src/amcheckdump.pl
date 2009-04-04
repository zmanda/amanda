#! @PERL@
# Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;

use File::Basename;
use Getopt::Long;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Logfile;
use Amanda::Util qw( :constants );
use Amanda::Changer;
use Amanda::Constants;

# Have all images been verified successfully so far?
my $all_success = 1;

sub usage {
    print <<EOF;
USAGE:	amcheckdump config [ --timestamp|-t timestamp ] [-o configoption]*
    amcheckdump validates Amanda dump images by reading them from storage
volume(s), and verifying archive integrity if the proper tool is locally
available. amcheckdump does not actually compare the data located in the image
to anything; it just validates that the archive stream is valid.
    Arguments:
	config       - The Amanda configuration name to use.
	-t timestamp - The run of amdump or amflush to check. By default, check
			the most recent dump; if this parameter is specified,
			check the most recent dump matching the given
			date- or timestamp.
	-o configoption	- see the CONFIGURATION OVERRIDE section of amanda(8)
EOF
    exit(1);
}

# Find the most recent logfile name matching the given timestamp
sub find_logfile_name($) {
    my $timestamp = shift @_;
    my $rval;
    my $config_dir = config_dir_relative(getconf($CNF_LOGDIR));
    # First try log.$datestamp.$seq
    for (my $seq = 0;; $seq ++) {
        my $logfile = sprintf("%s/log.%s.%u", $config_dir, $timestamp, $seq);
        if (-f $logfile) {
            $rval = $logfile;
        } else {
            last;
        }
    }
    return $rval if defined $rval;

    # Next try log.$datestamp.amflush
    $rval = sprintf("%s/log.%s.amflush", $config_dir, $timestamp);

    return $rval if -f $rval;

    # Finally try log.datestamp.
    $rval = sprintf("%s/log.%s.amflush", $config_dir, $timestamp);
    
    return $rval if -f $rval;

    # No dice.
    return undef;
}

## Device management

my $changer;
my $reservation;
my $current_device;
my $current_device_label;

sub find_next_device {
    my $label = shift;
    my $reset_done_cb;
    my $find_done_cb;
    my ($slot, $tapedev);

    # if the changer hasn't been created yet, set it up
    if (!$changer) {
	$changer = Amanda::Changer->new();
    }

    my $load_sub = sub {
	my ($err) = @_;
	if ($err) {
	    print STDERR $err, "\n";
	    exit 1;
	}

	$changer->load(
	    label => $label,
	    res_cb => sub {
		(my $err, $reservation) = @_;
		if ($err) {
		    print STDERR $err, "\n";
		    exit 1;
		}
		Amanda::MainLoop::quit();
	    },
	);
    };

    if (defined $reservation) {
	$reservation->release(finished_cb => $load_sub);
    } else {
	$load_sub->(undef);
    }

    # let the mainloop run until the find is done.  This is a temporary
    # hack until all of amcheckdump is event-based.
    Amanda::MainLoop::run();

    return $reservation->{device_name};
}

# Try to open a device containing a volume with the given label.  Returns undef
# if there is a problem.
sub try_open_device {
    my ($label) = @_;

    # can we use the same device as last time?
    if ($current_device_label eq $label) {
	return $current_device;
    }

    # nope -- get rid of that device
    close_device();

    my $device_name = find_next_device($label);
    if ( !$device_name ) {
	print "Could not find a device for label '$label'.\n";
        return undef;
    }

    my $device = Amanda::Device->new($device_name);
    if ($device->status() != $DEVICE_STATUS_SUCCESS) {
	print "Could not open device $device_name: ",
	      $device->error(), ".\n";
	return undef;
    }
    if (!$device->configure(1)) {
	print "Could not configure device $device_name: ",
	      $device->error(), ".\n";
	return undef;
    }

    my $label_status = $device->read_label();
    if ($label_status != $DEVICE_STATUS_SUCCESS) {
	if ($device->error() ) {
	    print "Could not read device $device_name: ",
		  $device->error(), ".\n";
	} else {
	    print "Could not read device $device_name: one of ",
	         join(", ", DevicestatusFlags_to_strings($label_status)),
	         "\n";
	}
	return undef;
    }

    if ($device->volume_label() ne $label) {
	printf("Labels do not match: Expected '%s', but the device contains '%s'.\n",
		     $label, $device->volume_label());
	return undef;
    }

    if (!$device->start($ACCESS_READ, undef, undef)) {
	printf("Error reading device %s: %s.\n", $device_name,
	       $device->error_message());
	return undef;
    }

    $current_device = $device;
    $current_device_label = $device->volume_label();

    return $device;
}

sub close_device {
    $current_device = undef;
    $current_device_label = undef;
}

## Validation application

my ($current_validation_pid, $current_validation_pipeline, $current_validation_image);

# Return a filehandle for the validation application that will handle this
# image.  This function takes care of split dumps.  At the moment, we have
# a single "current" validation application, and as such assume that split dumps
# are stored contiguously and in order on the volume.
sub open_validation_app {
    my ($image, $header) = @_;

    # first, see if this is the same image we were looking at previously
    if (defined($current_validation_image)
	and $current_validation_image->{timestamp} eq $image->{timestamp}
	and $current_validation_image->{hostname} eq $image->{hostname}
	and $current_validation_image->{diskname} eq $image->{diskname}
	and $current_validation_image->{level} == $image->{level}) {
	# TODO: also check that the part number is correct
        print "Continuing with previously started validation process.\n";
	return $current_validation_pipeline;
    }

    # nope, new image.  close the previous pipeline
    close_validation_app();
	
    my $validation_command = find_validation_command($header);
    print "  using '$validation_command'.\n";
    $current_validation_pid = open($current_validation_pipeline, "|-", $validation_command);
        
    if (!$current_validation_pid) {
	print "Can't execute validation command: $!\n";
	undef $current_validation_pid;
	undef $current_validation_pipeline;
	return undef;
    }

    $current_validation_image = $image;
    return $current_validation_pipeline;
}

# Close any running validation app, checking its exit status for errors.  Sets
# $all_success to false if there is an error.
sub close_validation_app {
    if (!defined($current_validation_pipeline)) {
	return;
    }

    # first close the applications standard input to signal it to stop
    if (!close($current_validation_pipeline)) {
	my $exit_value = $? >> 8;
	print "Validation process returned $exit_value (full status $?)\n";
	$all_success = 0; # flag this as a failure
    }

    $current_validation_pid = undef;
    $current_validation_pipeline = undef;
    $current_validation_image = undef;
}

# Given a dumpfile_t, figure out the command line to validate.
sub find_validation_command {
    my ($header) = @_;

    # We base the actual archiver on our own table, but just trust
    # whatever is listed as the decrypt/uncompress commands.
    my $program = uc(basename($header->{program}));

    my $validation_program;

    if ($program ne "APPLICATION") {
        my %validation_programs = (
            "STAR" => "$Amanda::Constants::STAR -t -f -",
            "DUMP" => "$Amanda::Constants::RESTORE tbf 2 -",
            "VDUMP" => "$Amanda::Constants::VRESTORE tf -",
            "VXDUMP" => "$Amanda::Constants::VXRESTORE tbf 2 -",
            "XFSDUMP" => "$Amanda::Constants::XFSRESTORE -t -v silent -",
            "TAR" => "$Amanda::Constants::GNUTAR tf -",
            "GTAR" => "$Amanda::Constants::GNUTAR tf -",
            "GNUTAR" => "$Amanda::Constants::GNUTAR tf -",
            "SMBCLIENT" => "$Amanda::Constants::GNUTAR tf -",
        );
        $validation_program = $validation_programs{$program};
    } else {
	if (!defined $header->{application}) {
            print STDERR "Application not set; ".
	                 "Will send dumps to /dev/null instead.";
            $validation_program = "cat > /dev/null";
	} else {
	    my $program_path = $Amanda::Paths::APPLICATION_DIR . "/" .
                               $header->{application};
            if (!-x $program_path) {
                print STDERR "Application '" , $header->{application},
			     "($program_path)' not available on the server; ".
	                     "Will send dumps to /dev/null instead.";
                $validation_program = "cat > /dev/null";
	    } else {
	        $validation_program = $program_path . " validate";
	    }
	}
    }
    if (!defined $validation_program) {
        print STDERR "Could not determine validation for dumper $program; ".
	             "Will send dumps to /dev/null instead.";
        $validation_program = "cat > /dev/null";
    } else {
        # This is to clean up any extra output the program doesn't read.
        $validation_program .= " > /dev/null && cat > /dev/null";
    }
    
    my $cmdline = "";
    if (defined $header->{decrypt_cmd} && 
        length($header->{decrypt_cmd}) > 0) {
        $cmdline .= $header->{decrypt_cmd};
    }
    if (defined $header->{uncompress_cmd} && 
        length($header->{uncompress_cmd}) > 0) {
        $cmdline .= $header->{uncompress_cmd};
    }
    $cmdline .= $validation_program;

    return $cmdline;
}

## Application initialization

Amanda::Util::setup_application("amcheckdump", "server", $CONTEXT_CMDLINE);

my $timestamp = undef;
my $config_overwrites = new_config_overwrites($#ARGV+1);

Getopt::Long::Configure(qw(bundling));
GetOptions(
    'timestamp|t=s' => \$timestamp,
    'help|usage|?' => \&usage,
    'o=s' => sub { add_config_overwrite_opt($config_overwrites, $_[1]); },
) or usage();

usage() if (@ARGV < 1);

my $timestamp_argument = 0;
if (defined $timestamp) { $timestamp_argument = 1; }

my $config_name = shift @ARGV;
config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
apply_config_overwrites($config_overwrites);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# If we weren't given a timestamp, find the newer of
# amdump.1 or amflush.1 and extract the datestamp from it.
if (!defined $timestamp) {
    my $amdump_log = config_dir_relative(getconf($CNF_LOGDIR)) . "/amdump.1";
    my $amflush_log = config_dir_relative(getconf($CNF_LOGDIR)) . "/amflush.1";
    my $logfile;
    if (-f $amflush_log && -f $amdump_log &&
         -M $amflush_log  < -M $amdump_log) {
         $logfile=$amflush_log;
    } elsif (-f $amdump_log) {
         $logfile=$amdump_log;
    } elsif (-f $amflush_log) {
         $logfile=$amflush_log;
    } else {
	print "Could not find amdump.1 or amflush.1 files.\n";
	exit;
    }

    # extract the datestamp from the dump log
    open (AMDUMP, "<$logfile") || die();
    while(<AMDUMP>) {
	if (/^amdump: starttime (\d*)$/) {
	    $timestamp = $1;
	}
	elsif (/^amflush: starttime (\d*)$/) {
	    $timestamp = $1;
	}
	elsif (/^planner: timestamp (\d*)$/) {
	    $timestamp = $1;
	}
    }
    close AMDUMP;
}

# Find all logfiles matching our timestamp
my $logfile_dir = config_dir_relative(getconf($CNF_LOGDIR));
my @logfiles =
    grep { $_ =~ /^log\.$timestamp(?:\.[0-9]+|\.amflush)?$/ }
    Amanda::Logfile::find_log();

# Check log file directory if find_log didn't find tape written
# on that tapestamp
if (!@logfiles) {
    opendir(DIR, $logfile_dir) || die "can't opendir $logfile_dir: $!";
    @logfiles = grep { /^log.$timestamp\..*/ } readdir(DIR);
    closedir DIR;

    if (!@logfiles) {
	if ($timestamp_argument) {
	    print STDERR "Can't find any logfiles with timestamp $timestamp.\n";
	} else {
	    print STDERR "Can't find the logfile for last run.\n";
	}
	exit 1;
    }
}

# compile a list of *all* dumps in those logfiles
my @images;
for my $logfile (@logfiles) {
    chomp $logfile;
    push @images, Amanda::Logfile::search_logfile(undef, $timestamp,
                                                  "$logfile_dir/$logfile", 1);
}

# filter only "ok" dumps, removing partial and failed dumps
@images = Amanda::Logfile::dumps_match([@images],
	undef, undef, undef, undef, 1);

if (!@images) {
    if ($timestamp_argument) {
	print STDERR "No backup written on timestamp $timestamp.\n";
    } else {
	print STDERR "No backup written on latest run.\n";
    }
    exit 1;
}

# Find unique tapelist, using a hash to filter duplicate tapes
my %tapes = map { ($_->{label}, undef) } @images;
my @tapes = sort { $a cmp $b } keys %tapes;

if (!@tapes) {
    print STDERR "Could not find any matching dumps.\n";
    exit 1;
}

printf("You will need the following tape%s: %s\n", (@tapes > 1) ? "s" : "",
       join(", ", @tapes));

# Now loop over the images, verifying each one.  

IMAGE:
for my $image (@images) {
    # Currently, L_PART results will be n/x, n >= 1, x >= -1
    # In the past (before split dumps), L_PART could be --
    # Headers can give partnum >= 0, where 0 means not split.
    my $logfile_part = 1; # assume this is not a split dump
    if ($image->{partnum} =~ m$(\d+)/(-?\d+)$) {
        $logfile_part = $1;
    }

    printf("Validating image %s:%s datestamp %s level %s part %s on tape %s file #%d\n",
           $image->{hostname}, $image->{diskname}, $image->{timestamp},
           $image->{level}, $logfile_part, $image->{label}, $image->{filenum});

    # note that if there is a device failure, we may try the same device
    # again for the next image.  That's OK -- it may give a user with an
    # intermittent drive some indication of such.
    my $device = try_open_device($image->{label});
    if (!defined $device) {
	# error message already printed
	$all_success = 0;
	next IMAGE;
    }

    # Now get the header from the device
    my $header = $device->seek_file($image->{filenum});
    if (!defined $header) {
        printf("Could not seek to file %d of volume %s.\n",
                     $image->{filenum}, $image->{label});
	$all_success = 0;
        next IMAGE;
    }

    # Make sure that the on-device header matches what the logfile
    # told us we'd find.

    my $volume_part = $header->{partnum};
    if ($volume_part == 0) {
        $volume_part = 1;
    }

    if ($image->{timestamp} ne $header->{datestamp} ||
        $image->{hostname} ne $header->{name} ||
        $image->{diskname} ne $header->{disk} ||
        $image->{level} != $header->{dumplevel} ||
        $logfile_part != $volume_part) {
        printf("Details of dump at file %d of volume %s do not match logfile.\n",
                     $image->{filenum}, $image->{label});
	$all_success = 0;
        next IMAGE;
    }
    
    # get the validation application pipeline that will process this dump.
    my $pipeline = open_validation_app($image, $header);

    # send the datastream from the device straight to the application
    my $queue_fd = Amanda::Device::queue_fd_t->new(fileno($pipeline));
    if (!$device->read_to_fd($queue_fd)) {
        print "Error reading device or writing data to validation command.\n";
	$all_success = 0;
	next IMAGE;
    }
}

if (defined $reservation) {
    $reservation->release();
}

# clean up
close_validation_app();
close_device();

exit($all_success? 0 : 1);
