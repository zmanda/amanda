#! @PERL@ -w

# Catch for sh/csh on systems without #! ability.
eval '(exit $?0)' && eval 'exec @PERL@ -S $0 ${1+"$@"}'
        & eval 'exec @PERL@ -S $0 $argv:q'
                if 0;

# 
# This changer script controls tape libraries on operating systems that have a
# chgio program
#	DSL 7000 on FreeBSD is an example
#
# The changer being used is a n tape juke, that can be used with 1, n-1 or n
# tapes in the juke. The special slot is slot n. The script does not
# make assumptions about the number of slots, except that the special slot
# is the highest number. The slot is special in the sense that it contains the
# the only tape if the juke contains 1 tape and contains no tape if the juke
# contains n-1 tapes. See getCurrentTape.
#
# Furthermore, the script uses drive 0 and assumes that the device is able to
# figure itself how to move a type from slot m to drive 0 if asked to do so and
# multiple pickers are present.
#
# The numbering of the slots is by the way from 1 to n with slots. The chio
# program returns the slot numbers numbered from 0 to n-1 however.
# 
# This script is built up out of bits and pieces of the other scripts
# and no credits are claimed. Most notably the chg-rth.pl script was used. That
# script was written by Erik Frederick, <edf@tyrell.mc.duke.edu>.
# 
# Permission to freely use and distribute is granted (by me and was granted by
# the original authors).
#
# Nick Hibma - nick.hibma@jrc.it
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

@slots = ();
@drives = ();
$max_slot = 0;
$max_drive = 0;
$nr_tapes = 0;

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

sub getCurrentTape {
	print LOG &do_time(), ": enter: getCurrentTape\n";

	#
	# Determines the slot number for the tape that is currently in the
	# drive. getTapeParams and getTapeStatus should have been called.
	# If there is no tape in the drive, no current tape, 0 is returned.
	#

	my($slot, $i);

	if ( !$drives[0] ) {		# drive empty
		$i = 0;
	} elsif ( $nr_tapes == 1 ) {	# one tape -> comes from slot max_slot
		$i = $max_slot;
	} else {			# find first empty slot
		$i = 0;
		while ( $i < $#slots and $slots[$i] ) {
			$i++
		}
		$i++;
	}

	print LOG &do_time(), ": leave: getCurrentTape: $i\n";
	return $i;
}

sub getTapeStatus {
	print LOG &do_time(), ": enter: getTapeStatus\n";

	#
	# Sets $nr_tapes, @slots, @drives, $current_tape
	#

	my($type,$num,$status);

	print LOG &do_time(), ": running: @CHIO@ -f $changerDevice status\n";
	if ( !(open(FH,"@CHIO@ -f $changerDevice status|")) ) {
		print "$progname: '@CHIO@ -f $changerDevice status' failed, $!\n";
		exit(2);
	}

	#
	# This routine requires the format of the output of 'chio status' to 
	# be as follows:
	#   picker 0: 
	#   slot 0: <ACCESS>
	#   slot 1: <ACCESS,FULL>
	#   slot 2: <ACCESS,FULL>
	#   (etc.)
	#   drive 0: <ACCESS,FULL>


	@slots=();
	@drives=();

	while( defined ($line = <FH>) ) {
		chomp( $line );
		print LOG &do_time(), ": $line\n";
		next unless $line =~ m/(\w+)\s+(\d+):\s*<([^>]+)>/;
		($type,$num,$status) = ($1,$2,$3);
		if ( $type =~ m/slot/i ) {
			$slots[$num] = ( $status =~ m/full/i ) ? 1 : 0;
			if ($slots[ $num ]) { $nr_tapes++ }
		} elsif ( $type =~ m/drive/i ) {
			$drives[$num] = 0;
			if (  $status =~ m/full/i ) {
				$drives[$num] = 1;
				$nr_tapes++;
			}
		} else {
			# ignore 'picker', empty ones, etc...
		}
	}
	close(FH);

	if ( $nr_tapes == 0 ) {
		print "$progname: No tapes in changer!\n";
		exit(2);
	}

	$currentTape = &getCurrentTape(); 

	print LOG &do_time(), ": leave: getTapeStatus: $nr_tapes\n";
	return($nr_tapes);
}

sub getTapeParams {
	print LOG &do_time(), ": enter: getTapeParams\n";
  
	#
	# Requests information on the number of slots, pickers and drives
	# from the changer.
	#

	my($max_slot,$max_drive,$max_picker);
  
	print LOG &do_time(), ": running: @CHIO@ -f $changerDevice params\n";
	if ( !open(FH,"@CHIO@ -f $changerDevice params|") ) {
		print "$progname: '@CHIO@ -f $changerDevice params' failed, $!\n";
		exit(2);
	}
  
	#
	# the format of the output of 'chio params' should be
	#  /dev/ch0: 8 slots, 1 drive, 1 picker
	#  /dev/ch0: current picker: 0
	#

	$max_slot = 0;
	$max_picker = -1;
	$max_drive = 0;

	while( defined ($line = <FH>) ) {
		chomp $line;
		print LOG &do_time(), ": $line\n";
		$max_slot 	= $1 if $line =~ m/(\d+) slot/i;
		$max_drive	= $1 if $line =~ m/(\d+) drive/i;
		$max_picker 	= $1 if $line =~ m/(\d+) picker/i;

	}
	close(FH);
	if ( $max_drive == 0 or $max_picker == -1 ) {
		print "$progname: No drive or picker ? ($max_drive/$max_picker)\n";
		exit(2);
	}

	print LOG &do_time(), ": leave: getTapeParams: $max_slot, $max_drive, $max_picker\n";
	return ($max_slot, $max_drive, $max_picker);
}

sub testTape {
	my($tape) = @_;

	#
	# Check a few parameters to avoid the most serious problems
	#

	return
		if $currentTape == $tape;

	if( $slots[$tape-1] == 0 ) {
		print "<none> $progname: no tape in slot requested\n";
		exit(1);
	}
	if( $tape > $max_slot ) {
		print $tape," $progname: requested a tape > $max_slot\n";
		exit(2);
	}
	if( $tape < 1 ) {
		print $tape," $progname: requested a tape < 1\n";
		exit(2);
	}
	return;
}

sub Load {
	my($tape) = @_;
	print LOG &do_time(), ": enter: Load: $tape\n";

	#
	# Load tape $tape into drive 0
	#

	print LOG &do_time(), ": running: @CHIO@ -f $changerDevice move slot ", $tape - 1, " drive 0\n";
	if ( system("@CHIO@ -f $changerDevice move slot ".($tape-1)." drive 0") ) {
		print "$progname: cannot '@CHIO@ -f $changerDevice move' tape $tape into drive 0\n";
		exit(2);
	}

	# wait for tape to load
	$count = 1800;
	while ( $count > 0 &&
		system("$MT $MTF $tapeDevice status > /dev/null 2>&1" ) ) {
		print LOG &do_time(), ": waiting for tape to load\n";
		sleep 30;
		$count -= 30;
	}

	print LOG &do_time(), ": leave: Load\n";
}

sub Unload {
	my($tape) = @_;
	print LOG &do_time(), ": enter: Unload: $tape\n";

	#
	# Unload the tape from drive 0 and put it into the slot specified by
	# $tape.
	#

	#
	# Ecrix AutoPAK devices (based on the Spectra Logics 215 changer)
	# can lock up if you try to move a tape from a drive to an open slot
	# without first rewinding and ejecting the tape.  This appears to
	# occur when the operation times out and the ch driver sends a device
	# or bus reset. Ecrix claims this is about to be fixed with a new
	# firmware rev but for now it's safest to just explicitly eject
	# the tape before moving the cartridge.
	#
	if ( system ("$MT $MTF $tapeDevice offline") ) {
		print "$progname: Warning, failed to eject the tape with '$MT $MTF $tapeDevice offline'\n";
		# NB: not fatal; let chio try it's thing
	}

	if ( system("@CHIO@ -f $changerDevice move drive 0 slot ".($tape-1)." ") ) {
		print "$progname: cannot '@CHIO@ -f $changerDevice move' tape $tape from drive 0\n";
		exit(2);
	}
	print LOG &do_time(), ": leave: Unload\n";
}

sub changeTape {
	my($tape) = @_;
	print LOG &do_time(), ": enter: changeTape: $tape\n";

	#
	# Unload current tape and load a new tape from slot $tape.
	#

	if ($tape != $currentTape) {

		&testTape($tape);

		if( $currentTape != 0 ) {
			&Unload($currentTape);
		}
		&Load($tape);
		$currentTape = $tape;
	}
	print LOG &do_time(), ": leave: changeTape\n";
}


#
# Main program
#

#
# Initialise
#

($max_slot, $max_drive) = &getTapeParams();

$opt_slot = 0;					# perl -w fodder
$opt_info = 0;					# perl -w fodder
$opt_reset = 0;					# perl -w fodder
$opt_eject = 0;					# perl -w fodder

GetOptions("slot=s", "info", "reset", "eject"); 

$nr_tapes = &getTapeStatus();

#
# Before we do anything with the tape changer we'll have to rewind the tape
#

if (-x "$sbindir/ammt") {
	$MT="$sbindir/ammt";
	$MTF="-f";
} elsif (-x "@MT@") {
	$MT="@MT@";
	$MTF="@MT_FILE_FLAG@";
} else {
	print LOG &do_time(), ": mt program not found\n";
	print "<none> mt program not found\n";
	exit(1);
}
print LOG &do_time(), ": MT -> $MT $MTF\n";

system ("$MT $MTF $tapeDevice rewind")
	unless $currentTape == 0;


if ( $opt_slot ) {
	if ( $opt_slot =~ /first/ ) {
		&changeTape(1);
		print LOG &do_time(), ": $currentTape $tapeDevice\n";
		print "$currentTape $tapeDevice\n";
	}
	if ( $opt_slot =~ /last/ ) {
		&changeTape($max_slot);
		print LOG &do_time(), ": $currentTape $tapeDevice\n";
		print "$currentTape $tapeDevice\n";
	}
	if ( $opt_slot =~ /current/ ) {
		&changeTape($currentTape);
		print LOG &do_time(), ": $currentTape $tapeDevice\n";
		print "$currentTape $tapeDevice\n";
	}
	if ( $opt_slot =~ /next/ ) {
		$tape = $currentTape+1;
		if ( $tape > $max_slot ) {
			$tape = 1;
		}
		while ( $slots[$tape-1] == 0 ) {        # there is at least 1 
			if ( ++$tape > $max_slot ) {
				$tape = 1;
			}
		}
		&changeTape($tape);
		print LOG &do_time(), ": $currentTape $tapeDevice\n";
		print "$currentTape $tapeDevice\n";
	}
	if ( $opt_slot =~ /prev/ ) {
		$tape = $currentTape-1;
		if ( $tape < 1 ) {
			$tape = $max_slot;
		}
		while ( $slots[$tape-1] == 0 ) {        # there is at least 1
			if ( --$tape < 1 ) {
				$tape = $max_slot;
			}
		}
		&changeTape($tape);
		print LOG &do_time(), ": $currentTape $tapeDevice\n";
		print "$currentTape $tapeDevice\n";
	}
	if ( $opt_slot =~ /^\d+$/ ) {
		&changeTape($opt_slot);
		print LOG &do_time(), ": $currentTape $tapeDevice\n";
		print "$currentTape $tapeDevice\n";
	}
	if ( $opt_slot =~ /advance/ ) {
		$tape=$currentTape+1;
		if ( $tape > $max_slot ) {
			$tape = 1;
		}
		if ( $currentTape ) { 
			&Unload($currentTape);
		}
		print LOG &do_time(), ": $currentTape $tapeDevice\n";
		print "$currentTape , /dev/null\n";
	}

	exit 0;
}

if ( $opt_info ) {
	if ( $currentTape == 0 ) {
		&Load(1);			# load random tape
		$currentTape = 1;
	}

	print LOG &do_time(), ": $currentTape $max_slot 1\n";
	print "$currentTape $max_slot 1\n";
	exit 0;
}

if ( $opt_reset ) {
	&changeTape(1);
	print LOG &do_time(), ": $currentTape $tapeDevice\n";
	print "$currentTape $tapeDevice\n";
	exit 0;
}

if ( $opt_eject ) {
	if ( $currentTape ) { 
		&Unload($currentTape);
		print "0 $tapeDevice\n";
		exit 0;
	} else {
		print "$progname: drive was not loaded\n";
		exit 1;
	}
}

print "$progname: No command was received.  Exiting.\n";
exit 1;
