#!@PERL@

# Catch for sh/csh on systems without #! ability.
eval '(exit $?0)' && eval 'exec @PERL@ -S $0 ${1+"$@"}'
	& eval 'exec @PERL@ -S $0 $argv:q'
		if 0;

# rth-changer - 
#   A tape changer script for the Robotic Tape Handling system OEM'd
#   by Andataco (RTH-406) for use with Amanda, the Advanced Maryland
#   Network Disk Archiver.
#
#    Author: Erik Frederick 1/10/97
#            edf@tyrell.mc.duke.edu
#
# This changer script controls the HP c1553 tape drive via a
# Peripheral Vision Inc. SCSI control subsystem that interprets
# commands sent on the SCSI bus.  It may work with other tape drives
# containing the PVI board.
#
# Permission to freely use and distribute is granted.
#

require 5.001;

use Getopt::Long;

delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV', 'PATH'};
$ENV{'PATH'} = "/usr/bin:/usr/sbin:/sbin:/bin";

$pname = "rth-changer";

$prefix="@prefix@";
$prefix=$prefix;		# avoid warnings about possible typo
$exec_prefix="@exec_prefix@";
$exec_prefix=$exec_prefix;	# ditto
$sbindir="@sbindir@";
$amlibexecdir="@amlibexecdir@";

if (-x "$sbindir/ammt") {
	$MT="$sbindir/ammt";
	$MTF="-f";
} elsif (-x "@MT@") {
	$MT="@MT@";
	$MTF="@MT_FILE_FLAG@";
} else {
	print "<none> $pname: mt program not found\n";
	exit(1);
}

$tapeDevice=`$sbindir/amgetconf tapedev`;
die "tapedev not found in amanda.conf"
	if !$tapeDevice or $tapeDevice eq "" or
	    $tapeDevice =~ m/no such parameter/;

sub getCurrentTape {

  if (!sysopen(RTH, $tapeDevice, 2)) {
    print "$currentTape $pname: error in opening `$tapeDevice' for getting current tape: $!\n";
    exit(2);
  }
  if (syswrite(RTH, "Rd_ElS", 6) != 6) {
    print "$currentTape $pname: error in writing `Rd_ElS' to `$tapeDevice': $!\n";
    exit(2);
  }
  if (!close(RTH)) {
    print "$currentTape $pname: error in closing `$tapeDevice' for getting current tape: $!\n";
    exit(2);
  }

  if (!sysopen(RTH, $tapeDevice, 2)) {
    print "$currentTape $pname: error in opening `$tapeDevice' for getting current tape: $!\n";
    exit(2);
  }
  if (sysread(RTH, $status, 136) != 136) {
    print "$currentTape $pname: error in reading rth status.\n";
    exit(2);
  }
  if (!close(RTH)) {
    print "$currentTape $pname: error in closing `$tapeDevice' for getting current tape: $!\n";
    exit(2);
  }

  @statusBits=unpack("c*",$status);

  if( ($statusBits[18] == 0x1) || ($statusBits[18]== 0x9)) {
    return ($statusBits[27]-1);
  }

  return (0);
}


sub getTapeStatus {

  if (!sysopen(RTH, $tapeDevice, 2)) {
    print "$currentTape $pname: error in opening `$tapeDevice' for getting tape status: $!\n";
    exit(2);
  }
  if (syswrite(RTH, "Rd_ElS", 6) != 6) {
    print "$currentTape $pname: error in writing `Rd_ElS' to `$tapeDevice': $!\n";
    exit(2);
  }
  if (!close(RTH)) {
    print "$currentTape $pname: error in closing `$tapeDevice' for getting tpae status: $!\n";
    exit(2);
  }

  if (!sysopen(RTH, $tapeDevice, 2)) {
    print "$currentTape $pname: error in opening `$tapeDevice' for getting tape status: $!\n";
    exit(2);
  }
  if (sysread(RTH, $status, 136) != 136) {
    print "$currentTape $pname: error in reading rth status for tape $currentTape.\n";
    exit(2);
  }
  if (!close(RTH)) {
    print "$currentTape $pname: error in closing `$tapeDevice' for getting tape status: $!\n";
    exit(2);
  }

  @statusBits=unpack("c*",$status);

  $curTape=0;
  for($i=42;$i<187;$i+=16) {
    if($statusBits[$i] == 0x9) {
      $slots[$curTape] = 1;
    }
    else {
      $slots[$curTape] = 0;
    }
    $curTape++;
  }

  return (@slots);
}

sub rthLoad {
  my($tape) = @_;

  $command = sprintf "GeT%d", $tape;
  if (!sysopen(RTH, $tapeDevice, 2)) {
    print "$currentTape $pname: error in opening `$tapeDevice' for loading tape: $!\n";
    exit(2);
  }
  if (syswrite(RTH, $command, 4) != 4) {
    print "$currentTape $pname: error in loading tape by writing `$command' to `$tapeDevice': $!\n";
    exit(2);
  }
  if (!close (RTH)) {
    print "$currentTape $pname: error in closing `$tapeDevice' when trying to load tape: $!\n";
    exit(2);
  }
}

sub rthUnload {
  my($tape) = @_;

  $command = sprintf "PuT%d", $tape;
  if (!sysopen(RTH, $tapeDevice, 2)) {
    print "$currentTape $pname: error in opening `$tapeDevice' for unloading tape: $!\n";
    exit(2);
  }
  if (syswrite(RTH, $command, 4) != 4) {
    print "$currentTape $pname: error in unloading tape by writing `$command' to `$tapeDevice': $!\n";
    exit(2);
  }
  if (!close (RTH)) {
    print "$currentTape $pname: error in closing `$tapeDevice' when trying to unload tape: $!\n";
    exit(2);
  }
}

sub testTape {
  my($tape) = @_;

  @slots=getTapeStatus();

  if($currentTape == $tape) {
    return;
  }

  if($slots[$tape-1] == 0) {
    print "<none> $pname: no tape in slot requested\n";
    exit(1);
  }
  if($tape>6) {
    print $tape," $pname: requested a tape > 6\n";
    exit(2);
  }
  if($tape<1) {
    print $tape," $pname: requested a tape < 1\n";
    exit(2);
  }
  return;
}

sub changeTape {
  my($tape) = @_;

  if($tape==$currentTape) {
    return;
  }

  testTape($tape);

  if($currentTape==0) {
    rthLoad($tape);
    $currentTape=$tape;
    return;
  }
  else {
    rthUnload($currentTape);
    rthLoad($tape);
    $currentTape=$tape;
  }
}
    

$result = &GetOptions("slot=s", "info", "reset", "eject"); 

system($MT, 'rewind');

$nSlots=6;
$firstTape=1;
$lastTape=6;
$currentTape=getCurrentTape(); 

if($opt_slot) {
  if($opt_slot =~ /first/) {
    changeTape(1);
    print $currentTape, " ", $tapeDevice, "\n";
  }
  if($opt_slot =~ /last/) {
    changeTape(6);
    print $currentTape, " ", $tapeDevice, "\n";
  }
  if($opt_slot =~ /current/) {
    changeTape($currentTape);
    print $currentTape, " ", $tapeDevice, "\n";
  }
  if($opt_slot =~ /next/) {
    $tape=$currentTape+1;
    if ($tape>6) {
      $tape=1;
    }
    changeTape($tape);
    print $currentTape, " ", $tapeDevice,"\n";
  }
  if($opt_slot =~ /prev/) {
    $tape=$currentTape-1;
    if($tape<1) {
      $tape=6;
    }
    changeTape($tape);
    print $currentTape, " ", $tapeDevice,"\n";
  }
  if($opt_slot =~ /\d/) {
    changeTape($opt_slot);
    print $currentTape, " ", $tapeDevice,"\n";
  }
  if($opt_slot =~ /advance/) {
    $tape=$currentTape+1;
    if ($tape>6) {
      $tape=1;
    }
    if($currentTape) { 
      rthUnload($currentTape);
    }
    print $currentTape, " ", "/dev/null","\n";
  }

  exit 0;
}

if($opt_info) {
 
  print $currentTape, " 6 1\n";

  exit 0;
}

if($opt_reset) {
  changeTape(1);
  print $currentTape, " ",$tapeDevice,"\n";
  exit 0;
}

if($opt_eject) {
  if($currentTape) { 
    rthUnload($currentTape);
    print "0 ",$tapeDevice,"\n";
    exit 0;
  } 
  else {
    print "0 $pname: drive was not loaded\n";
    exit 1;
  }
}

print "$pname: No command was received.  Exiting.\n";
exit 1;
