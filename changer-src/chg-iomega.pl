#! @PERL@ -w

# Catch for sh/csh on systems without #! ability.
eval '(exit $?0)' && eval 'exec @PERL@ -S $0 ${1+"$@"}'
        & eval 'exec @PERL@ -S $0 $argv:q'
                if 0;

# 
# This changer script is designed for IOMEGA or JAZZ disks of various sizes
# as well as any other removable disk media.
#
# This is a PURELY MANUAL changer. It requests insertion of disk media via
# messages on /dev/tty. So it cannot be used via crontab.
#
# Make sure you comply with any of the following.
# - Add statements 
#         tpchanger "chg-iomega"
#         tapedev "file:<mount_point_of_removable_disk>"
#         # (e.g. tapedev "file:/mnt/iomega" )
#         tapetype IOMEGA      
#
#         
#         define tapetype IOMEGA {
#             comment "IOMega 250 MB floppys"
#             length 250 mbytes
#             filemark 100 kbytes
#             speed 1 mbytes
#         }
#   to your /etc/amanda/<backup_set>/amanda.conf file
# - Add entry to /etc/fstab to specify mount point of removable disk
#   and make this disk mountable by any user.
# - Format all disks, add a "data" sub directory and label all disks
#   by a call to amlabel.
# - Be aware that as of version 2.4.4p1, amanda can't handle backups that are
#   larger than the size of the removable disk media. So make sure
#   /etc/amanda/<backup_set>/disklist specifies chunks smaller than the 
#   disk size.
#
# This script is built up out of bits and pieces of other scripts, in
# particular chg-chio.pl. That script was written by 
# Nick Hibma - nick.hibma@jrc.it
# 
# Permission to freely use and distribute is granted (by me and was granted by
# the original authors).
#
# Christoph Pospiech <pospiech@de.ibm.com>
#

require 5.001;

($progname = $0) =~ s#/.*/##;

use English;
use Getopt::Long;

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV', 'PATH'};
$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin";

$| = 1;

if (-d "@AMANDA_DBGDIR@") {
	$logfile = "@AMANDA_DBGDIR@/changer.debug";
} else {
	$logfile = "/dev/null";
}
die "$progname: cannot open $logfile: $ERRNO\n"
	unless (open (LOG, ">> $logfile"));


#
# get the information from the configuration file
#

$prefix="@prefix@";
$prefix=$prefix;		# avoid warnings about possible typo
$exec_prefix="@exec_prefix@";
$exec_prefix=$exec_prefix;	# Ditto
$sbindir="@sbindir@";

chomp ($tapeDevice = `$sbindir/amgetconf tapedev 2>&1`);
die "tapedev not found in amanda.conf"
	if !$tapeDevice or $tapeDevice eq "" or
	    $tapeDevice =~ m/no such parameter/;
chomp ($changerDevice = `$sbindir/amgetconf changerdev 2>&1`);
chomp $changerDevice;
die "changerdev not found in amanda.conf"
	if !$changerDevice or $changerDevice eq "" or
	    $changerDevice =~ m/no such parameter/;

#
# Initialise a few global variables
#

$current_label = "";
#$current_slot = 0;
$max_slot = 1;

@dow = ("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat");
@moy = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
	"Jul", "Aug", "Sep", "Oct", "Nov", "Dec");

sub do_time {
	my (@t);
	my ($r);

	###
	# Get the local time for the value.
	###

	@t = localtime (time ());

	###
	# Return the result.
	###

	$r = sprintf "%s %s %2d %2d:%02d:%02d %4d",
	  $dow[$t[6]],
	  $moy[$t[4]],
	  $t[3],
	  $t[2], $t[1], $t[0],
	  1900 + $t[5];

	return $r;
}


sub is_mounted {
    my $device = shift @_;
    my ($directory) = ($device =~ m/file\:(.*)$/);
    if ( -d "$directory/data" ) { return 1;}
    else {return 0;}
}

sub request {
    my $label = shift @_;
    my $answer;
    open (TTY, "+</dev/tty") or die "Can't open tty.\n";
    print TTY "Insert Disk with label $label\n";
    read TTY,$answer,1;
    close TTY;
    return $answer;
}

sub print_label {
    my $label = shift @_;
    open (TTY, "+</dev/tty") or die "Can't open tty.\n";
    print TTY "The current Disk has label $label\n";
    close TTY;
}

sub get_label {
    my $device = shift @_;
    my ($directory) = ($device =~ m/file\:(.*)$/);
    my @dir_list =  glob("$directory/data/*");
    while ( ($_= shift(@dir_list)) ) {
	if ( /0+\.([\w\d]+)/ ) {return $1;}
    }
    return "no label";
}


sub Load {
    my $device = shift @_;
    my ($directory) = ($device =~ m/file\:(.*)$/);
    my $label;
    print LOG &do_time(), ": enter: Load $device\n";
    if ( ! &is_mounted($device) ) {
	print LOG &do_time(), ": mounting $directory\n";
	system "mount $directory";
    }
    $label = get_label $device;
    &print_label($label);
    print LOG &do_time(), ": current label: $label\n";
    print LOG &do_time(), ": leave: Load\n";

}

sub Unload {
    my $device = shift @_;
    my ($directory) = ($device =~ m/file\:(.*)$/);
    print LOG &do_time(), ": enter: Unload $device\n";
    if ( &is_mounted($device) ) {
	print LOG &do_time(), ": ejecting $directory\n";
	system "eject $directory";
    }
    print LOG &do_time(), ": leave: Unload\n";
}



#
# Main program
#

#
# Initialise
#


$opt_slot = 0;					# perl -w fodder
$opt_info = 0;					# perl -w fodder
$opt_reset = 0;					# perl -w fodder
$opt_eject = 0;					# perl -w fodder
$opt_search = 0;       				# perl -w fodder
$opt_label = 0;					# perl -w fodder

GetOptions("slot=s", "info", "reset", "eject", "search=s", "label=s"); 


if ( $opt_slot ) {
    print LOG &do_time(), ": Loading slot $opt_slot requested\n";
    if ( ! &is_mounted ($tapeDevice) ) {
	&request ("any");
	&Load ($tapeDevice);
    }
    $current_label = &get_label ($tapeDevice);
    print LOG &do_time(), ": current label: $current_label\n";
    print LOG &do_time(), ": 1 $tapeDevice\n";
    print "1 $tapeDevice\n";
    exit 0;
}

if ( $opt_info ) {
    print LOG &do_time(), ": info requested\n";
    $current_label = &get_label ($tapeDevice);
    print LOG &do_time(), ": current label: $current_label\n";
    print LOG &do_time(), ": 1 $max_slot 1 1\n";
    print "1 $max_slot 1 1\n";
    exit 0;
}

if ( $opt_reset ) {
    print LOG &do_time(), ": reset requested\n";
    &Unload ($tapeDevice);
    &request ("any");
    &Load ($tapeDevice);
    $current_label = &get_label ($tapeDevice);
    print LOG &do_time(), ": current label: $current_label\n";
    print LOG &do_time(), ": 1 $tapeDevice\n";
    print "1 $tapeDevice\n";
    exit 0;
}

if ( $opt_eject ) {
    print LOG &do_time(), ": eject requested\n";
    &Unload ($tapeDevice);
    print LOG &do_time(), ": 1 $tapeDevice\n";
    print "1 $tapeDevice\n";
    exit 0;
}

if ( $opt_search ) {
    print LOG &do_time(), ": search label $opt_search requested\n";
    $retry = 0;
    $current_label = &get_label ($tapeDevice);
    print LOG &do_time(), ": current label: $current_label\n";
    while ( $opt_search ne $current_label && ++$retry < 5) {
	&Unload ($tapeDevice);
	&request ($opt_search);
	&Load ($tapeDevice);
	$current_label = &get_label ($tapeDevice);
	print LOG &do_time(), ": search label: $opt_search\n";
	print LOG &do_time(), ": current label: $current_label\n";
    }
    if ( $retry >= 5) {
	print LOG &do_time(), ": disk not found\n";
	print "disk not found\n";
	exit 1;
    } else {
	print LOG &do_time(), ": 1 $tapeDevice\n";
	print "1 $tapeDevice\n";
	exit 0;
    }
}

if ( $opt_label ) {
    print LOG &do_time(), ": label $opt_label requested\n";
    # no operation
    print LOG &do_time(), ": 1 $tapeDevice\n";
    print "1 $tapeDevice\n";
    exit 0;
}

print "$progname: No command was received.  Exiting.\n";
exit 1;
