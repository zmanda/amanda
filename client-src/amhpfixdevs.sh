
eval '(exit $?0)' && eval 'exec perl -x -S $0 ${1+"$@"}'
	& eval 'exec perl -x -S $0 $argv:q'
		if 0;

#!perl

# Check whether we're on a HP-UX system.
$uname=`uname`;
chomp $uname;
if ( $uname ne "HP-UX" ) {
	print "Sorry, this script only works for HP-UX systems!\n";
	exit 1;
}

# Check whether the user is root.
$id=`id -un`;
chomp $id;
if ( $id ne "root" ) {
	print "Sorry, this script needs to be run by the superuser!\n";
	exit 1;
}

# Determine all volume groups and the logical volumes in these volume groups.
print "\n\nScanning volume groups...\n";
open(LV, "vgdisplay -v 2>/dev/null |") or
  die "$0: unable to open vgdisplay pipe: $!\n";
while ( <LV> ) {
	if ( m!^(VG Name\s+/dev/)(.*)! ) {
		print "\n" if $v;
		$v = $2;
		print "The volume group $v contains the following logical volumes:\n";
	}
	elsif ( m!(\s+LV Name\s+/dev/$v/)(.*)! ) {
		print "\t$2\n";
		$vg{$v} .= "$2 ";
	}
}
close LV or
  warn "$0: error in closing vgdisplay pipe: $!\n";

# Now fix the device entries for all logical volumes.
print "\n\nFixing device entries...\n";
foreach $v ( keys(%vg) ) {
	foreach $w ( split(/[\s]+/, $vg{$v} ) ) {
		# First the link for the block device.
		if ( ! -e "/dev/dsk/${v}_$w" ) {
			print "Creating link for /dev/dsk/${v}_$w...";
			if ( ! symlink("/dev/$v/$w", "/dev/dsk/${v}_$w") ) {
				print "FAILED\n";
				next;
			}
			print "done\n";
		}

		# Now the link for the raw devive.
		if ( ! -e "/dev/rdsk/${v}_$w" ) {
			print "Creating link for /dev/rdsk/${v}_$w...";
			if ( ! symlink("/dev/$v/r$w", "/dev/rdsk/${v}_$w") ) {
				print "FAILED\n";
				next;
			}
			print "done\n";
		}
	}
}
