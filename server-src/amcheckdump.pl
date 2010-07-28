#! @PERL@
# Copyright (c) 2007, 2008, 2009, 2010 Zmanda, Inc.  All Rights Reserved.
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
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

use lib '@amperldir@';
use strict;

use File::Basename;
use Getopt::Long;
use IPC::Open3;
use Symbol;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Tapelist;
use Amanda::Logfile;
use Amanda::Util qw( :constants );
use Amanda::Changer;
use Amanda::Recovery::Scan;
use Amanda::Constants;
use Amanda::MainLoop;

# Have all images been verified successfully so far?
my $all_success = 1;
my $verbose = 0;

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

## Device management

my $scan;
my $reservation;
my $current_device;
my $current_device_label;
my $current_command;

sub find_next_device {
    my $label = shift;
    my $reset_done_cb;
    my $find_done_cb;
    my ($slot, $tapedev);

    # if the scan hasn't been created yet, set it up
    if (!$scan) {
	my $inter = Amanda::Interactive->new(name => 'stdin');
	$scan = Amanda::Recovery::Scan->new(interactive => $inter);
	if ($scan->isa("Amanda::Changer::Error")) {
	    print "$scan\n";
	    exit 1;
	}
    }

    my $load_sub = make_cb(load_sub => sub {
	my ($err) = @_;
	if ($err) {
	    print STDERR $err, "\n";
	    exit 1;
	}

	$scan->find_volume(
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
    });

    my $start = make_cb(start => sub {
	if (defined $reservation) {
	    $reservation->release(finished_cb => $load_sub);
	} else {
	    $load_sub->(undef);
	}
    });

    # let the mainloop run until the find is done.  This is a temporary
    # hack until all of amcheckdump is event-based.
    Amanda::MainLoop::call_later($start);
    Amanda::MainLoop::run();

    return $reservation->{'device'};
}

# Try to open a device containing a volume with the given label.
# return ($device, undef) on success
# return (undef, $err) on error
sub try_open_device {
    my ($label, $timestamp) = @_;

    # can we use the same device as last time?
    if ($current_device_label eq $label) {
	return $current_device;
    }

    # nope -- get rid of that device
    close_device();

    my $device = find_next_device($label);
    my $device_name = $device->device_name;

    my $label_status = $device->status;
    if ($label_status != $DEVICE_STATUS_SUCCESS) {
	if ($device->error_or_status() ) {
	    return (undef, "Could not read device $device_name: " .
			    $device->error_or_status());
	} else {
	    return (undef, "Could not read device $device_name: one of " .
	            join(", ", DevicestatusFlags_to_strings($label_status)));
	}
    }

    my $start = make_cb(start => sub {
	$reservation->set_label(label => $device->volume_label(),
				finished_cb => sub {
					Amanda::MainLoop::quit();
				});
    });

    Amanda::MainLoop::call_later($start);
    Amanda::MainLoop::run();

    if ($device->volume_label() ne $label) {
	return (undef, "Labels do not match: Expected '$label', but the " .
		       "device contains '" . $device->volume_label() . "'");
    }

    if ($device->volume_time() ne $timestamp) {
	return (undef, "Timestamps do not match: Expected '$timestamp', " .
		       "but the device contains '" .
		       $device->volume_time() . "'");
    }

    if (!$device->start($ACCESS_READ, undef, undef)) {
	return (undef, "Error reading device $device_name: " .
		       $device->error_or_status());
	return undef;
    }

    $current_device = $device;
    $current_device_label = $device->volume_label();

    return ($device, undef);
}

sub close_device {
    $current_device = undef;
    $current_device_label = undef;
}

## Validation application

my ($current_validation_pid, $current_validation_pipeline, $current_validation_image);

sub is_part_of_same_image {
    my ($image, $header) = @_;

    return ($image->{timestamp} eq $header->{datestamp}
        and $image->{hostname} eq $header->{name}
        and $image->{diskname} eq $header->{disk}
        and $image->{level} == $header->{dumplevel});
}

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
        Amanda::Debug::debug("Continuing with previously started validation process");
	return $current_validation_pipeline, $current_command;
    }

    my @command = find_validation_command($header);

    if ($#command == 0) {
	$command[0]->{fd} = Symbol::gensym;
	open(NULL, ">", "/dev/null") or die "couldn't open /dev/null for writing";
	$command[0]->{pid} = open3($current_validation_pipeline, ">&NULL", $command[0]->{stderr}, $command[0]->{pgm});
    } else {
	my $nb = $#command;
	$command[$nb]->{fd} = "VAL_GLOB_$nb";
	$command[$nb]->{stderr} = Symbol::gensym;
	open(NULL, ">", "/dev/null") or die "couldn't open /dev/null for writing";
	$command[$nb]->{pid} = open3($command[$nb]->{fd}, ">&NULL", $command[$nb]->{stderr}, $command[$nb]->{pgm});
	close($command[$nb]->{stderr});
	while ($nb-- > 1) {
	    $command[$nb]->{fd} = "VAL_GLOB_$nb";
	    $command[$nb]->{stderr} = Symbol::gensym;
	    $command[$nb]->{pid} = open3($command[$nb]->{fd}, ">&". $command[$nb+1]->{fd}, $command[$nb]->{stderr}, $command[$nb]->{pgm});
	    close($command[$nb+1]->{fd});
	}
	$command[$nb]->{stderr} = Symbol::gensym;
	$command[$nb]->{pid} = open3($current_validation_pipeline, ">&".$command[$nb+1]->{fd}, $command[$nb]->{stderr}, $command[$nb]->{pgm});
	close($command[$nb+1]->{fd});
    }

    my @com;
    for my $i (0..$#command) {
	push @com, $command[$i]->{pgm};
    }
    my $validation_command = join (" | ", @com);
    Amanda::Debug::debug("  using '$validation_command'");
    print "  using '$validation_command'\n" if $verbose;
        
    $current_validation_image = $image;
    return $current_validation_pipeline, \@command;
}

# Close any running validation app, checking its exit status for errors.  Sets
# $all_success to false if there is an error.
sub close_validation_app {
    my $command = shift;

    if (!defined($current_validation_pipeline)) {
	return;
    }

    # first close the applications standard input to signal it to stop
    close($current_validation_pipeline);
    my $result = 0;
    while (my $cmd = shift @$command) {
	#read its stderr
	my $fd = $cmd->{stderr};
	while(<$fd>) {
	    print $_;
	    $result++;
	}
	waitpid $cmd->{pid}, 0;
	my $err = $?;
	my $res = $!;

	if ($err == -1) {
	    Amanda::Debug::debug("failed to execute $cmd->{pgm}: $res");
	    print "failed to execute $cmd->{pgm}: $res\n";
	    $result++;
	} elsif ($err & 127) {
	    Amanda::Debug::debug(sprintf("$cmd->{pgm} died with signal %d, %s coredump",
		($err & 127), ($err & 128) ? 'with' : 'without'));
	    printf "$cmd->{pgm} died with signal %d, %s coredump\n",
		($err & 127), ($err & 128) ? 'with' : 'without';
	    $result++;
	} elsif ($err > 0) {
	    Amanda::Debug::debug(sprintf("$cmd->{pgm} exited with value %d", $err >> 8));
	    printf "$cmd->{pgm} exited with value %d %d\n", $err >> 8, $err;
	    $result++;
	}

    }

    if ($result) {
	Amanda::Debug::debug("Image was not successfully validated");
	print "Image was not successfully validated\n\n";
	$all_success = 0; # flag this as a failure
    } else {
        Amanda::Debug::debug("Image was successfully validated");
        print("Image was successfully validated.\n\n") if $verbose;
    }

    $current_validation_pipeline = undef;
    $current_validation_image = undef;
}

# Given a dumpfile_t, figure out the command line to validate.
# return an array of command to execute
sub find_validation_command {
    my ($header) = @_;

    my @result = ();

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
	if (!defined $validation_program) {
	    Amanda::Debug::debug("Unknown program '$program'");
	    print "Unknown program '$program'.\n" if $program ne "PKZIP";
	}
    } else {
	if (!defined $header->{application}) {
	    Amanda::Debug::debug("Application not set");
            print "Application not set\n";
	} else {
	    my $program_path = $Amanda::Paths::APPLICATION_DIR . "/" .
                               $header->{application};
            if (!-x $program_path) {
                Amanda::Debug::debug("Application '" . $header->{application}.
			     "($program_path)' not available on the server; ".
	                     "Will send dumps to /dev/null instead.");
		Amanda::Debug::debug("Application '$header->{application}' in path $program_path not available on server");
	    } else {
	        $validation_program = $program_path . " validate";
	    }
	}
    }

    if (defined $header->{decrypt_cmd} && 
        length($header->{decrypt_cmd}) > 0) {
	if ($header->{dle_str} =~ /<encrypt>CUSTOM/) {
	    # Can't decrypt client encrypted image
	    my $cmd;
            $cmd->{pgm} = "cat";
	    push @result, $cmd;
	    return @result;
	}
	my $cmd;
	$cmd->{pgm} = $header->{decrypt_cmd};
	$cmd->{pgm} =~ s/ *\|$//g;
	push @result, $cmd;
    }
    if (defined $header->{uncompress_cmd} && 
        length($header->{uncompress_cmd}) > 0) {
	#If the image is not compressed, the decryption is here
	if ((!defined $header->{decrypt_cmd} ||
	     length($header->{decrypt_cmd}) == 0 ) and
	    $header->{dle_str} =~ /<encrypt>CUSTOM/) {
	    # Can't decrypt client encrypted image
	    my $cmd;
            $cmd->{pgm} = "cat";
	    push @result, $cmd;
	    return @result;
	}
	my $cmd;
	$cmd->{pgm} = $header->{uncompress_cmd};
	$cmd->{pgm} =~ s/ *\|$//g;
	push @result, $cmd;
    }

    my $command;
    if (!defined $validation_program) {
        $command->{pgm} = "cat";
    } else {
	$command->{pgm} = $validation_program;
    }

    push @result, $command;

    return @result;
}

## Application initialization

Amanda::Util::setup_application("amcheckdump", "server", $CONTEXT_CMDLINE);

my $timestamp = undef;
my $config_overrides = new_config_overrides($#ARGV+1);

Getopt::Long::Configure(qw(bundling));
GetOptions(
    'timestamp|t=s' => \$timestamp,
    'verbose|v'     => \$verbose,
    'help|usage|?'  => \&usage,
    'o=s' => sub { add_config_override_opt($config_overrides, $_[1]); },
) or usage();

usage() if (@ARGV < 1);

my $timestamp_argument = 0;
if (defined $timestamp) { $timestamp_argument = 1; }

my $config_name = shift @ARGV;
set_config_overrides($config_overrides);
config_init($CONFIG_INIT_EXPLICIT_NAME, $config_name);
my ($cfgerr_level, @cfgerr_errors) = config_errors();
if ($cfgerr_level >= $CFGERR_WARNINGS) {
    config_print_errors();
    if ($cfgerr_level >= $CFGERR_ERRORS) {
	die("errors processing config file");
    }
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

my $tapelist_file = config_dir_relative(getconf($CNF_TAPELIST));
my $tl = Amanda::Tapelist::read_tapelist($tapelist_file);

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
my $nb_images = @images;

# filter only "ok" dumps, removing partial and failed dumps
@images = Amanda::Logfile::dumps_match([@images],
	undef, undef, undef, undef, 1);

if (!@images) {
    if ($nb_images == 0) {
        if ($timestamp_argument) {
	    print STDERR "No backup written on timestamp $timestamp.\n";
        } else {
	    print STDERR "No backup written on latest run.\n";
        }
    } else {
        if ($timestamp_argument) {
	    print STDERR "No complete backup available on timestamp $timestamp.\n";
        } else {
	    print STDERR "No complete backup available on latest run.\n";
        }
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
print "Press enter when ready\n";
<STDIN>;

# Now loop over the images, verifying each one.  

my $header;

IMAGE:
for my $image (@images) {
    my $check = sub {
	my ($ok, $msg) = @_;
	if (!$ok) {
	    $all_success = 0;
	    Amanda::Debug::debug("Image was not successfully validated: $msg");
	    print "Image was not successfully validated: $msg.\n";
	    next IMAGE;
	}
    };

    # If it's a new image
    my $new_image = !(defined $header);
    if (!$new_image) {
	if (!is_part_of_same_image($image, $header)) {
	close_validation_app($current_command);
	$new_image = 1;
}
    }

    Amanda::Debug::debug("Validating image " . $image->{hostname} . ":" .
	$image->{diskname} . " datestamp " . $image->{timestamp} . " level ".
	$image->{level} . " part " . $image->{partnum} . "/" .
	$image->{totalparts} . "on tape " . $image->{label} . " file #" .
	$image->{filenum});

    if ($new_image) {
    printf("Validating image %s:%s datestamp %s level %s part %d/%d on tape %s file #%d\n",
           $image->{hostname}, $image->{diskname}, $image->{timestamp},
           $image->{level}, $image->{partnum}, $image->{totalparts},
	   $image->{label}, $image->{filenum});
    } else {
    printf("           part  %s:%s datestamp %s level %s part %d/%d on tape %s file #%d\n",
           $image->{hostname}, $image->{diskname}, $image->{timestamp},
           $image->{level}, $image->{partnum}, $image->{totalparts},
	   $image->{label}, $image->{filenum});
    }

    # note that if there is a device failure, we may try the same device
    # again for the next image.  That's OK -- it may give a user with an
    # intermittent drive some indication of such.
    my ($device, $err) = try_open_device($image->{label}, $timestamp);
    $check->(defined $device, "Could not open device: $err");

    # Now get the header from the device
    $header = $device->seek_file($image->{filenum});
    $check->(defined $header,
      "Could not seek to file $image->{filenum} of volume $image->{label}: " .
        $device->error_or_status());

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
        $image->{partnum} != $volume_part) {
        printf("Volume image is %s:%s datestamp %s level %s part %s\n",
               $header->{name}, $header->{disk}, $header->{datestamp},
               $header->{dumplevel}, $volume_part);
        $check->(0, sprintf("Details of dump at file %d of volume %s do not match logfile",
                     $image->{filenum}, $image->{label}));
    }
    
    # get the validation application pipeline that will process this dump.
    (my $pipeline, $current_command) = open_validation_app($image, $header);

    # send the datastream from the device straight to the application
    my $queue_fd = Amanda::Device::queue_fd_t->new(fileno($pipeline));
    my $read_ok = $device->read_to_fd($queue_fd);
    $check->($device->status() == $DEVICE_STATUS_SUCCESS,
      "Error reading device: " . $device->error_or_status());
    # if we make it here, the device was ok, but the read perhaps wasn't
    if (!$read_ok) {
        my $errmsg = $queue_fd->{errmsg};
        if (defined $errmsg && length($errmsg) > 0) {
            $check->($read_ok, "Error writing data to validation command: $errmsg");
	} else {
            $check->($read_ok, "Error writing data to validation command: Unknown reason");
	}
    }
}

if (defined $reservation) {
    my $release = make_cb(start => sub {
	$reservation->release(finished_cb => sub {
				Amanda::MainLoop::quit()});
    });

    Amanda::MainLoop::call_later($release);
    Amanda::MainLoop::run();
}

# clean up
close_validation_app($current_command);
close_device();

if ($all_success) {
    Amanda::Debug::debug("All images successfully validated");
    print "All images successfully validated\n";
} else {
    Amanda::Debug::debug("Some images failed to be correclty validated");
    print "Some images failed to be correclty validated.\n";
}

Amanda::Util::finish_application();
exit($all_success? 0 : 1);
