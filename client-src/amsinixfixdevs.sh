
eval '(exit $?0)' && eval 'exec perl -x -S $0 ${1+"$@"}'
	& eval 'exec perl -x -S $0 $argv:q'
		if 0;

#!perl

# Check whether we're on a SINIX system.
$uname=`uname`;
chomp $uname;
if ( $uname !~ /SINIX/ ) {
  die("Sorry, this script only works for SINIX systems!\n");
}

# Check whether the user is root.
$id=`id -un`;
chomp $id;
if ( $id ne "root" ) {
  die("Sorry, this script needs to be run by the superuser!\n");
}

# Determine all filesystems currently mounted.
print "\nDetermining all filesystems currently mounted...\n";
open(VD, "/usr/bin/mount -p 2>/dev/null |") or
  die("$0: unable to open mount pipe: $!\n");
while ( <VD> ) {
  if ( m!^(/dev/\S*)\s+-\s+(\S+)\s+(vxfs|ufs)\s+! ) {
    $v = $1;
    print "Found filesystem $v\n";
    $vd{$v}++;
  }
}
close VD or
  warn "$0: error in closing mount pipe: $!\n";

# Determine all virtual disks.
undef($v);
print "\nDetermining all virtual disks...\n";
open(VD, "/sbin/dkconfig -lA 2>/dev/null |") or
  die("$0: unable to open dkconfig pipe: $!\n");
while ( <VD> ) {
  if ( m!^(/dev/\S*):\s+\d+\s+blocks! ) {
    $v = $1;
    print "Found virtual disk $v\n";
    $vd{$v}++;
  }
}
close VD or
  warn "$0: error in closing dkconfig pipe: $!\n";

# Check whether our target directories are present.
foreach $d ( "/dev/dsk", "/dev/rdsk" ) {
  if ( ! -x $d ) {
    if ( ! mkdir($d, 0755) ) {
      die("Failed to create directory $d!\n");
    }
  }
}

# Now fix the device entries for all virtual disks.
print "\nFixing device entries...\n";
foreach $v ( keys(%vd) ) {
  # determine the basename of the device
  ( $name ) = $v =~ m!^/dev/(.+)$!;
  if ( $name =~ m!/! ) {
    ( $p, $dev_orig ) = $name =~ m!(.*/)(.*)!;
  }
  else {
    $p = "";
    $dev_orig = $name;
  }

  # replace all slashes with _'s
  $dev_new = $name;
  $dev_new =~ s!/!_!g;

  # First the link for the block device.
  if ( ! -e "/dev/dsk/$dev_new" ) {
    print "Creating link for /dev/dsk/$dev_new...";
    if ( ! symlink($v, "/dev/dsk/$dev_new") ) {
      print "FAILED\n";
      next;
    }
    print "done\n";
  }

  # Now the link for the raw devive.
  if ( ! -e "/dev/rdsk/$dev_new" ) {
    print "Creating link for /dev/rdsk/$dev_new...";
    if ( ! symlink("/dev/${p}r$dev_orig", "/dev/rdsk/$dev_new") ) {
      print "FAILED\n";
      next;
    }
    print "done\n";
  }
}
