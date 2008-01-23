#! @PERL@
use lib '@amperldir@';
use strict;

use File::Basename;
use IPC::Open2;
use Getopt::Long;

use Amanda::Device qw( :constants );
use Amanda::Debug qw( :logging );
use Amanda::Config qw( :init :getconf config_dir_relative );
use Amanda::Logfile;
use Amanda::Util qw( :running_as_flags );
use Amanda::Tapelist;
use Amanda::Changer;

# Try to open the device and set properties. Does not read the label.
sub try_open_device($) {
    my ($device_name) = @_;

    if ( !$device_name ) {
        return undef;
    }

    my $device = Amanda::Device->new($device_name);
    if ( !$device ) {
        return undef;
    }

    $device->set_startup_properties_from_config();

    return $device;
}

sub usage {
    print <<EOF;
USAGE:	amcheckdump config [ --timestamp|-t timestamp ]
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

# Given a dumpfile_t, figure out the command line to validate.
sub find_validation_command($) {
    my $header = shift @_;
    # We base the actual archiver on our own table, but just trust
    # whatever is listed as the decrypt/uncompress commands.
    my $program = uc(basename($header->{program}));
    
    # TODO: Is there a way to access the configured variables?
    my $validation_program;
    my %validation_programs = (
        "STAR" => "star -t -f -",
        "DUMP" => "restore tbf 2 -",
        "VDUMP" => "vrestore tf -",
        "VXDUMP" => "vxrestore tbf 2 -",
        "XFSDUMP" => "xfsrestore -t -v silent -",
        "TAR" => "tar tf -",
        "GTAR" => "tar tf -",
        "GNUTAR" => "tar tf -",
        "SMBCLIENT" => "tar tf -",
    );

    $validation_program = $validation_programs{$program};
    if (!defined $validation_program) {
        warn("Could not determine validation for dumper $program;\n\t".
             "Will send dumps to /dev/null instead.\n");
        $validation_program = "cat > /dev/null";
    } else {
        # This is to clean up any extra output the program doesn't read.
        $validation_program .= " > /dev/null; cat > /dev/null";
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

# Figures out the next device to use, either prompting the user or
# accessing the changer. The argument is the desired volume label.
{ 
    # This is a static identifying if the changer has been initialized.
    my $changer_init_done = 0;
    sub find_next_device($) {
        my $label = shift;
        if (getconf_seen($CNF_TPCHANGER)) {
            # Changer script.
            if (!$changer_init_done) {
                my $error = (Amanda::Changer::reset())[0];
                die($error) if $error;
                $changer_init_done = 1;
            }
            my ($error, $slot, $tapedev) = Amanda::Changer::find($label);
            if ($error) {
                die("Error operating changer: $error\n");
            } elsif ($slot eq "<none>") {
                die("Could not find tape label $label in changer.\n");
            } else {
                return $tapedev;
            }
        } else {
            # Meatware changer.
            my $device_name = getconf_seen($CNF_TAPEDEV);
            printf("Insert volume with label %s in device %s and press ENTER: ",
                   $label, $device_name);
            <>;
            return $device_name;
        }
    }
}

## Application initialization

Amanda::Util::setup_application("amcheckdump", "server", "cmdline");

my $timestamp = undef;
GetOptions('timestamp|t=s' => \$timestamp,
           'help|usage|?' => \&usage);

if (@ARGV < 1) {
    usage();
}

my $config_name = shift @ARGV;
if (!config_init($CONFIG_INIT_EXPLICIT_NAME |
                 $CONFIG_INIT_FATAL, $config_name)) {
    die('errors processing config file "' . 
               Amanda::Config::get_config_filename() . '"');
}

Amanda::Util::finish_setup($RUNNING_AS_DUMPUSER);

# Read the tape list.
my $tl = Amanda::Tapelist::read_tapelist(config_dir_relative(getconf($CNF_TAPELIST)));

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
	print "Could not find any log file\n";
	exit;
    }

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

# Find logfiles matching our timestamp and scan them.
my @images;
my $logfile_dir = config_dir_relative(getconf($CNF_LOGDIR));
opendir(LOGFILE_DIR, $logfile_dir) || die("Can't opendir() $logfile_dir.\n");
my @logfiles =
    grep { $_ =~ /^log\.$timestamp(?:\.[0-9]+|\.amflush)?$/ }
    readdir(LOGFILE_DIR);
unless (@logfiles) {
    die("Can't find any logfiles with timestamp $timestamp.\n");
}
for my $logfile (@logfiles) {
    push @images, Amanda::Logfile::search_logfile(undef, $timestamp,
                                                  "$logfile_dir/$logfile", 1);
}
closedir(LOGFILE_DIR);

# filter only "ok" dumps
@images = Amanda::Logfile::dumps_match([@images],
	undef, undef, undef, undef, 1);

if (!@images) {
    print "Could not find any matching dumps\n";
    exit;
}

# Find unique tapelist. This confusing expression uses an anonymous hash.
my @tapes = sort { $a cmp $b } keys %{{map { ($_->{label}, undef) } @images}};

if (!@tapes) {
    die("Did not find any dumps to check!\n");
}

printf("You will need the following tape%s: %s\n", (@tapes > 1) ? "s" : "",
       join(", ", @tapes));

my $device;
my $validation_pid;
my $image;
while (1) {
    $image = $images[0];
    if (!defined $device) {
        my $device_name = find_next_device($image->{label});
        $device = try_open_device($device_name);
        if (!defined $device) {
            warn "Could not open device $device_name.\n";
            next;
        }

        my $label_status = $device->read_label;
        if ($label_status != $READ_LABEL_STATUS_SUCCESS) {
            warn("Could not read device $device_name: one of " .
                 join(", ", ReadLabelStatusFlags_to_strings($label_status)).
                 "\n");
            undef $device;
            next;
        }

        if ($device->{volume_label} ne $image->{label}) {
            warn(sprintf("Labels do not match: Expected %s, got %s.\n",
                         $image->{label}, $device->volume_label));
            undef $device;
            next;
        }

        if (!$device->start($ACCESS_READ, undef, undef)) {
            warn(sprintf("Error reading device %s.\n", $device_name));
            undef $device;
            next;
        }
    }

    # Errors below this point are problems with the image, not just the
    # device or volume.
    shift @images;

    # Now get the dump information.
    my $image_header = $device->seek_file($image->{filenum});
    if (!defined $image_header) {
        warn(sprintf("Could not seek to file %d of volume %s.\n",
                     $image->{filenum}, $image->{label}));
        next;
    }

    # Currently, L_PART results will be n/x, n >= 1, x >= -1
    # In the past (before split dumps), L_PART could be --
    # Headers can give partnum >= 0, where 0 means not split.
    my $logfile_part;
    if ($image->{partnum} =~ m$(\d+)/(-?\d+)$) {
        $logfile_part = $1;
    } else {
        # Not split, I guess.
        $logfile_part = 1;
    }

    my $volume_part = $image_header->{partnum};
    if ($volume_part == 0) {
        $volume_part = 1;
    }

    if ($image->{timestamp} ne $image_header->{datestamp} ||
        $image->{hostname} ne $image_header->{name} ||
        $image->{diskname} ne $image_header->{disk} ||
        $image->{level} != $image_header->{dumplevel} ||
        $logfile_part != $volume_part) {
        warn(sprintf("Details of dump at file %d of volume %s ".
                     "do not match logfile.\n",
                     $image->{filenum}, $image->{label}));
        next;
    }
    
    printf("Validating image %s/%s/%s/%s:%s on image %s:%d\n",
           $image->{hostname}, $image->{diskname}, $image->{timestamp},
           $image->{level}, $logfile_part, $image->{label}, $image->{filenum});
    if ($validation_pid) {
        print "Using previously started command.\n"
    } else {
        # Skipped if we are reusing from previous part.
        my $validation_command = find_validation_command($image_header);
        print "Using command $validation_command.\n";
        $validation_pid = open(VALIDATION_PIPELINE, "|-",
                               $validation_command);
        
        unless ($validation_pid) {
            die("Can't execute validation command: $!\n");
            next;
        }
    }
    
    if (!$device->read_to_fd(fileno(VALIDATION_PIPELINE))) {
        die("Error reading device or writing data to validation command.\n");
    }

    last unless @images;
} continue {
    # Check if device should be closed.
    my $next_image = $images[0]; # Could be the same.
    if ($image->{label} ne $next_image->{label}) {
        undef $device;
    }
    # Check if validation pipeline should be closed.
    if ($validation_pid &&
        ($image->{timestamp} ne $next_image->{timestamp} ||
         $image->{hostname} ne $next_image->{hostname} ||
         $image->{diskname} ne $next_image->{diskname} ||
         $image->{level} != $next_image->{level})) {
        if (!close(VALIDATION_PIPELINE)) {
            die("Error closing validation pipeline.\n");
        }
        undef $validation_pid;
    }
}

exit 0;
