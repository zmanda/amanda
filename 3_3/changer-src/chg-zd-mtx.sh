#!@SHELL@ 
#
# Exit Status:
# 0 Alles Ok
# 1 Illegal Request
# 2 Fatal Error
#
# Contributed by Eric DOUTRELEAU <Eric.Doutreleau@int-evry.fr>
# This is supposed to work with Zubkoff/Dandelion version of mtx
#
# Modified by Joe Rhett <jrhett@isite.net>
# to work with MTX 1.2.9 by Eric Lee Green http://mtx.sourceforge.net
#
# Modified by Jason Hollinden <jhollind@sammg.com> on 13-Feb-2001
# to work with MTX 1.2.10, >9 slots, has barcode support, and works with
# multiple configs at once.
# NOTE:  Only tested the 2 additions with an ADIC Scalar 100.

################################################################################
# Here are the things you need to do and know to configure this script:
#
#   * Figure out what the robot device name is and what the tape drive
#     device name is.  They will be different!
#
#     You cannot send robot commands to a tape drive and vice versa.
#     Both should respond to "mtx -f /dev/... inquiry".  Hopefully,
#     that output will make it obvious which is which.
#
#     For instance, here is what mtx has to say about my current robot:
#
#       Product Type: Medium Changer
#       Vendor ID: 'ATL     '
#       Product ID: 'ACL2640 206     '
#       Revision: '2A5A'
#       Attached Changer: No
#
#     and here is what it says about a tape drive:
#
#       Product Type: Tape Drive
#       Vendor ID: 'Quantum '
#       Product ID: 'DLT4000         '
#       Revision: 'CD50'
#       Attached Changer: No
#
#     Note the "Product Type" value makes it clear which is which.
#
#     If it is not obvious, "mf -f /dev/... rewind" should be happy when
#     talking to a (loaded) tape drive but the changer should give some
#     kind of error.  Similarly, "mtx -f /dev/... status" should show good
#     results with the changer but fail with a tape drive device name.
#
#     Once you have this figured out, set "changerdev" in amanda.conf
#     to the changer device and "tapedev" to the tape device.
#
#   * Find out what the first and last storage slots are.  Running
#     "mtx -f /dev/... status" should give you something like this
#     (although the output will vary widely based on the version of mtx
#     and the specifics of your robot):
#
#	  Storage Changer /dev/changer:1 Drives, 9 Slots ( 0 Import/Export )
#	Data Transfer Element 0:Empty
#	      Storage Element 1:Full :VolumeTag=SR0001
#	      Storage Element 2:Full :VolumeTag=SR0002
#	      Storage Element 3:Full :VolumeTag=SR0003
#	      Storage Element 4:Full :VolumeTag=SR0004
#	      Storage Element 5:Full :VolumeTag=SR0005
#	      Storage Element 6:Full :VolumeTag=SR0006
#	      Storage Element 7:Full :VolumeTag=SR0007
#	      Storage Element 8:Full :VolumeTag=SR0008
#	      Storage Element 9:Full :VolumeTag=SR0009
#	      Storage Element 10 IMPORT/EXPORT:Full :VolumeTag=SR0009
#
#     This says the first storage slot (element) is "1" and the last
#     is "9".  If you allocate the entire robot to Amanda, you do not need
#     to set the "firstslot" or "lastslot" configuration file variables --
#     the script will compute these values for you.
#
#     You do not have to allocate all of the slots for Amanda use,
#     but whatever slots you use must be contiguous (i.e. 4 through 9
#     in the above would be OK but 1, 2, 5, 6, 9 would not).  The one
#     exception to this is that if one of the slots contains a cleaning
#     cartridge, it may be in any slot (Amanda will just skip over it if
#     it is between firstslot and lastslot).
#
#   * Speaking of cleaning cartridges, if you have a storage slot dedicated
#     to one, figure out what slot it is in.  That slot number will go in
#     the "cleanslot" variable.
#
#     Also, decide if you want the changer script to automatically run
#     the cleaning tape through the drive after every so many mounts,
#     and how many mounts you want to do between cleanings.  If you
#     want the script to do this, set the "autoclean" variable to 1 and
#     the "autocleancount" to the number of mounts between cleanings.
#     If you do not want to do automatic cleanings (including not having
#     a cleaning cartridge in the robot), set "autoclean" to 0.
#
#     Note that only a count of mounts is used to determine when it is
#     time to clean.  The script does not try to detect if the drive is
#     requesting cleaning, or how much the drive was used on a given
#     mount.
#
#   * If you tell Amanda about a cleaning cartridge, whether for automatic
#     operation or manual (amtape <config> clean), you must also tell
#     the script how long it takes to run the cleaning cycle.  It is
#     impossible for the script to determine when the cleaning operation
#     is done, so the "cleancycle" variable is the number of seconds
#     the longest cleaning operation takes (you'll just have to figure
#     this out by watching it a few times, or maybe finding it in a tape
#     drive hardware manual).  The script will sleep for this length of
#     time whenever the cleaning tape is referenced.  The default is 120
#     seconds (two minutes).
#
#   * Figure out the drive slot number.  By default, it is set to 0.
#     In the example above, the tape drive ("Data Transfer Element")
#     is in slot 0. If your drive slot is not 0, you
#     need to set the drive slot number with the "driveslot" variable.
#
#   * Figure out whether your robot has a barcode reader and whether
#     your version of mtx supports it.  If you see "VolumeTag" entries
#     in the "mtx -f /dev/xxx status" output you did above, you have
#     a reader and mtx can work with it, so you may set the "havereader"
#     variable to 1.  The default is 0 (do not use a reader).
#
#   * Pick any tape to load and then determine if the robot can put it
#     away directly or whether an "offline" must be done first.
#
#     With the tape still mounted and ready, try to put the tape away
#     with "mtx".  If you get some kind of error, which is the most
#     common response, try "mt -f /dev/... offline", wait for the drive
#     to unload and make sure the robot takes no action on its own to
#     store the tape.  Assuming it does not, try the "mtx" command again
#     to store the tape.
#
#     If you had to issue the "mt -f /dev/... offline" before you could
#     use "mtx" to store the tape, set the "offline_before_unload"
#     variable to 1.  If "mtx" unloaded the drive and put the tape away
#     all by itself, set it to 0.
#
#   * Some drives and robots require a small delay between unloading the
#     tape and instructing the robot to move it back to storage.
#     For instance, if you try to grab the tape too soon on an ATL robot
#     with DLT tape drives, it will rip the leader out of the drive and
#     require sincerely painful hardware maintenance.
#
#     If you need a little delay, set the "unloadpause" variable to
#     the number of seconds to wait before trying to take a tape from
#     a drive back to storage.  The default is 0.
#
#   * Some drives also require a short pause after loading, or the drive
#     will return an I/O error during a test to see if it's online (which
#     this script uses "mt rewind" to test).  My drives don't recover from
#     this, and must be reloaded before they will come online after failing
#     such a test.  For this reason there is an "initial_poll_delay"
#     variable which will pause for a certain number of seconds before
#     looping through the online test for the first time.  The default is 0.
####

####
# Now you are ready to set up the variables in the changer configuration
# file.
#
# All variables are in "changerfile".conf where "changerfile" is set
# in amanda.conf.  For example, if amanda.conf has:
#
#	changerfile="/etc/amanda/Dailyset1/CHANGER"
#    or changerfile="/etc/amanda/Dailyset1/CHANGER.conf"
#
# the variables must be in "/etc/amanda/Dailyset1/CHANGER.conf".
# The ".conf" is appended only if it's not there".
#
# If "changerfile" is a relative path, it is relative to the directory
# that contains amanda.conf.  That also happens to be the directory Amanda
# makes current before running this script.
#
# Here is a commented out example file with all the variables and showing
# their default value (if any):
####
# firstslot=?		    #### First storage slot (element) -- required
# lastslot=?		    #### Last storage slot (element) -- required
# cleanslot=-1		    #### Slot with cleaner tape -- default is "-1"
#			    #### Set negative to indicate no cleaner available
# driveslot=0		    #### Drive slot number.  Defaults to 0
#			    #### Use the 'Data Transfer Element' you want
#
#   # Do you want to clean the drive after a certain number of accesses?
#   # NOTE - This is unreliable, since 'accesses' aren't 'uses', and we
#   #        have no reliable way to count this.  A single amcheck could
#   #        generate as many accesses as slots you have, plus 1.
#   # ALSO NOTE - many modern tape loaders handle this automatically.
#
# autoclean=0		    #### Set to '1' or greater to enable
#
# autocleancount=99	    #### Number of access before a clean.
#
# havereader=0		    #### If you have a barcode reader, set to 1.
#
# offline_before_unload=0   #### Does your robot require an
#			    #### 'mt offline' before mtx unload?
#
# poll_drive_ready=NN	    #### Time (seconds) between tests to see if
#			    #### the tape drive has gone ready (default: 3).
#
# max_drive_wait=NN	    #### Maximum time (seconds) to wait for the
#			    #### tape drive to become ready (default: 120).
#
# initial_poll_delay=NN	    #### initial delay after load before polling for
#			    #### readiness
#
# slotinfofile=FILENAME	    #### record slot information to this file, in
#			    #### the line-based format "SLOT LABEL\n"
#
####

####
# Now it is time to test the setup.  Do all of the following in the
# directory that contains the amanda.conf file, and do all of it as
# the Amanda user.
#
#   * Run this:
#
#       .../chg-zd-mtx -info
#       echo $?             #### (or "echo $status" if you use csh/tcsh)
#
#     You should get a single line from the script like this (the actual
#     numbers will vary):
#
#       5 9 1 1
#
#     The first number (5) is the "current" slot.  This may or may not be
#     the slot actually loaded at the moment (if any).  It is the slot
#     Amanda will try to use next.
#
#     The second number (9) is the number of slots.
#
#     The third number will always be "1" and indicates the changer is
#     capable of going backward.
#
#     The fourth number is optional.  If you set $havereader to 1, it
#     will be "1", otherwise it will not be present.
#
#     The exit code ($? or $status) should be zero.
#
#   * Run this:
#
#       .../chg-zd-mtx -reset
#       echo $?
#
#     The script should output a line like this:
#
#       1 /dev/rmt/0mn
#
#     The number at the first should match $firstslot.  The device name
#     after that should be your tape device.
#
#     The exit code ($? or $status) should be zero.
#
#   * Run this:
#
#       .../chg-zd-mtx -slot next
#       echo $?
#
#     The script should output a line like this:
#
#       2 /dev/rmt/0mn
#
#     The number at the first should be one higher than $firstslot.
#     The device name after that should be your tape device.
#
#     The exit code ($? or $status) should be zero.
#
#   * Run this:
#
#       .../chg-zd-mtx -slot current
#       echo $?
#
#     Assuming the tape is still loaded from the previous test, the
#     robot should not move and the script should report the same thing
#     the previous command did.
#
#   * If you continue to run "-slot next" commands, the robot should load
#     each tape in turn then wrap back around to the first when it
#     reaches $lasttape.  If $cleanslot is within the $firstslot to
#     $lastslot range, the script will skip over that entry.
#
#   * Finally, try some of the amtape commands and make sure they work:
#
#       amtape <config> reset
#       amtape <config> slot next
#       amtape <config> slot current
#
#   * If you set $havereader non-zero, now would be a good time to create
#     the initial barcode database:
#
#       amtape <config> update
####

################################################################################
# To debug this script, first look in @AMANDA_DBGDIR@.  The script
# uses one of two log files there, depending on what version of Amanda
# is calling it.  It may be chg-zd-mtx.YYYYMMDD*.debug, or it may be
# changer.debug.driveN where 'N' is the drive number.
#
# If the log file does not help, try running the script, **as the Amanda
# user**, in the amanda.conf directory with whatever set of args the log
# said were used when you had a problem.  If nothing else useful shows up
# in the output, try running the script with the DEBUG environment variable
# set non-null, e.g.:
#
#	env DEBUG=yes .../chg-zd-mtx ...
################################################################################

# source utility functions and values from configure
prefix=@prefix@
exec_prefix=@exec_prefix@
amlibexecdir=@amlibexecdir@
. ${amlibexecdir}/chg-lib.sh

test -n "$DEBUG" && set -x
TMPDIR="@AMANDA_TMPDIR@"
DBGDIR="@AMANDA_DBGDIR@"

argv0=$0
myname=`expr "$argv0" : '.*/\(.*\)'`

config=`pwd 2>/dev/null`
config=`expr "$config" : '.*/\(.*\)'`

###
# Functions to write a new log file entry and append more log information.
###

ds=`date '+%H:%M:%S' 2>/dev/null`
if [ $? -eq 0  -a  -n "$ds" ]; then
	logprefix=`echo "$ds" | sed 's/./ /g'`
else
	logprefix=""
fi

LogAppend() {
	if [ -z "$logprefix" ]; then
		echo "$@" >> $DBGFILE
	else
		echo "$logprefix" "$@" >> $DBGFILE
	fi
}

Log() {
	if [ -z "$logprefix" ]; then
		echo "===" "`date`" "===" >> $DBGFILE
		echo "$@" >> $DBGFILE
	else
		ds=`date '+%H:%M:%S' 2>/dev/null`
		echo "$ds" "$@" >> $DBGFILE
	fi
}

###
# Common exit function.
#
#   $1 = exit code
#   $2 = slot result
#   $3 = additional information (error message, tape devive, etc)
###

internal_call=0
Exit() {
	if [ $internal_call -gt 0 ]; then
		call_type=Return
	else
		call_type=Exit
	fi
	code=$1
	shift
	exit_slot=$1
	shift
	exit_answer="$@"
	Log $call_type "($code) -> $exit_slot $@"
	echo "$exit_slot" "$@"
	if [ $call_type = Return ]; then
		return $code
	fi
	amgetconf dbclose.$myname:$DBGFILE > /dev/null 2>&1
	exit $code
}

###
# Function to run another command and log it.
###

Run() {
	Log `_ 'Running: %s' "$*"`
	rm -f $stdout $stderr
	"$@" > $stdout 2> $stderr
	exitcode=$?
	Log `_ 'Exit code: %s' "$exitcode"`
	if [ -s $stdout ]
	then
		LogAppend Stdout:
		cat $stdout >> $DBGFILE
	fi
	if [ -s $stderr ]
	then
		LogAppend Stderr:
		cat $stderr >> $DBGFILE
	fi
	cat $stdout
	cat $stderr 1>&2
	return $exitcode
}

###
# Return success if the arg is numeric.
###

IsNumeric() {
	test -z "$1" && return 1
	x="`expr -- "$1" : "\([-0-9][0-9]*\)" 2>/dev/null`"
	return `expr X"$1" != X"$x"`
}

###
# Run $MTX status unless the previous output is still valid.
###

mtx_status_valid=0
get_mtx_status() {
	test -n "$DEBUG" && set -x
	if [ $mtx_status_valid -ne 0 ]; then
		return 0
	fi
	rm -f $mtx_status
	Run $MTX status > $mtx_status 2>&1
	status=$?
	if [ $status -eq 0 ]; then
		mtx_status_valid=1
	fi

	# shim this in here so that we get a completely new slotinfofile
	# every time we run mtx status
	regenerate_slotinfo_from_mtx

	return $status
}

###
# Determine the slot currently loaded.  Set $loadedslot to the slot
# currently loaded, or "-1", and $loadedbarcode to the corresponding
# barcode (or nothing).
###

get_loaded_info() {
	test -n "$DEBUG" && set -x
	get_mtx_status
	if [ $mtx_status_valid -eq 0 ]; then
		Exit 2 \
		     `_ '<none>'` \
		     `head -1 $mtx_status`
		return $?
	fi

	set x `sed -n '
/^Data Transfer Element:Empty/                          {
    s/.*/-1/p
    q
}
/^Data Transfer Element '$driveslot':Empty/             {
    s/.*/-1/p
    q
}
/^Data Transfer Element:Full (Storage Element \([0-9][0-9]*\) Loaded):VolumeTag *= *\([^     ]*\)/               {
    s/.*(Storage Element \([0-9][0-9]*\) Loaded):VolumeTag *= *\([^     ]*\)/\1 \2/p
    q
}
/^Data Transfer Element '$driveslot':Full (Storage Element \([0-9][0-9]*\) Loaded):VolumeTag *= *\([^     ]*\)/  {
    s/.*(Storage Element \([0-9][0-9]*\) Loaded):VolumeTag *= *\([^     ]*\)/\1 \2/p
    q
}
/^Data Transfer Element '$driveslot':Full (Unknown Storage Element Loaded):VolumeTag *= *\([^     ]*\)/	{
    s/.*:VolumeTag *= *\([^     ]*\)/-2 \1/p
    q
}
/^Data Transfer Element:Full (Storage Element \([0-9][0-9]*\) Loaded)/                           {
    s/.*(Storage Element \([0-9][0-9]*\) Loaded).*/\1/p
    q
}
/^Data Transfer Element '$driveslot':Full (Storage Element \([0-9][0-9]*\) Loaded)/              {
    s/.*Storage Element \([0-9][0-9]*\) Loaded.*/\1/p
    q
}
/^Data Transfer Element '$driveslot':Full (Unknown Storage Element Loaded)/	{
    s/.*/-2/p
    q
}
' < $mtx_status 2>&1`
	shift					# get rid of the "x"
	loadedslot=$1
	loadedbarcode=$2
	if [ -z "$loadedslot" ]; then
		Exit 2 \
		    `_ '<none>'` \
		    "could not determine current slot, are you sure your drive slot is $driveslot"
		return $?			# in case we are internal
	fi

	#Use the current slot if it's empty and we don't know which slot is loaded'
	if [ $loadedslot -eq -2 ]; then
		set x `sed -n '
{
    /^.*Storage Element '$currentslot':Empty/ {
	s/.*Storage Element \([0-9][0-9]*\):Empty/\1/p
        q
    }
    /^.*Storage Element '$currentslot':Full/ {
	s/.*Storage Element \([0-9][0-9]*\):Full/-2/p
        q
    }
    /^.*Storage Element '$currentslot' IMPORT\/EXPORT:Empty/ {
	s/.*Storage Element \([0-9][0-9]*\) IMPORT\/EXPORT:Empty/\1/p
        q
    }
    /^.*Storage Element '$currentslot' IMPORT\/EXPORT:Full/ {
	s/.*Storage Element \([0-9][0-9]*\) IMPORT\/EXPORT:Full/-2/p
        q
    }
}
' < $mtx_status 2>& 1`
		shift				# get rid of the "x"
		loadedslotx=$1
		if [ ! -z $loadedslotx ]; then
			loadedslot=$loadedslotx
		fi
	fi

	#Use the first empty slot if we don't know which slot is loaded'
	if [ $loadedslot -eq -2 ]; then
		set x `sed -n '
{
    /^.*Storage Element \([0-9][0-9]*\):Empty/ {
	s/.*Storage Element \([0-9][0-9]*\):Empty/\1/p
        q
    }
    /^.*Storage Element \([0-9][0-9]*\) IMPORT\/EXPORT:Empty/ {
	s/.*Storage Element \([0-9][0-9]*\) IMPORT\/EXPORT:Empty/\1/p
        q
    }
}
' < $mtx_status 2>& 1`
		shift				# get rid of the "x"
		loadedslot=$1
	fi

	if IsNumeric "$loadedslot" ; then
		:
	else
		Exit 2 \
		     `_ '<none>'` \
		     "currently loaded slot ($loadedslot) not numeric"
		return $?			# in case we are internal
	fi
	Log       `_ 'STATUS   -> currently loaded slot = %s' "$loadedslot"`
	LogAppend `_ '         -> currently loaded barcode = "%s"' "$loadedbarcode"`
}

###
# Get a list of slots between $firstslot and $lastslot, if they are set.
# If they are not set, set them to the first and last slot seen on the
# assumption the entire robot is to be used (???).
###

slot_list=
get_slot_list() {
	test -n "$DEBUG" && set -x
	if [ -n "$slot_list" ]; then
		return
	fi
	get_mtx_status
	if [ $mtx_status_valid -eq 0 ]; then
		Exit 2 \
		     `_ '<none>'` \
		     `head -1 $mtx_status`
		return $?
	fi
	slot_list=`sed -n '
/^Data Transfer Element:Full (Storage Element \([0-9][0-9]*\) Loaded)/ {
    s/.*(Storage Element \([0-9][0-9]*\) Loaded).*/\1/p
}
/^Data Transfer Element '$driveslot':Full (Storage Element \([0-9][0-9]*\) Loaded)/ {
    s/.*Storage Element \([0-9][0-9]*\) Loaded.*/\1/p
}
/^Data Transfer Element '$driveslot':Full (Unknown Storage Element Loaded)/ {
    : loop
    n
    /^.*Storage Element \([0-9][0-9]*\):Full/ {
        s/.*Storage Element \([0-9][0-9]*\):Full.*/\1/p
        b loop
    }
    /^.*Storage Element \([0-9][0-9]*\):Empty/ {
	s/.*Storage Element \([0-9][0-9]*\):Empty/\1/p
    }
}
/^.*Storage Element \([0-9][0-9]*\):Full/ {
    s/.*Storage Element \([0-9][0-9]*\):Full.*/\1/p
}
/^.*Storage Element \([0-9][0-9]*\) IMPORT\/EXPORT:Full/ {
    s/.*Storage Element \([0-9][0-9]*\) IMPORT\/EXPORT:Full.*/\1/p
}
' < $mtx_status 2>&1 | grep -v "^${cleanslot}\$" | sort -n`
	slot_list=`echo $slot_list`		# remove the newlines
	if [ $firstslot -lt 0 -o $lastslot -lt 0 ]; then
		last=$lastslot
		for slot in $slot_list; do
			if [ $firstslot -lt 0 ]; then
				Log `_ 'SLOTLIST -> firstslot set to %s' "$slot"`
				firstslot=$slot
			fi
			if [ $lastslot -lt 0 ]; then
				last=$slot
			fi
		done
		if [ $lastslot -lt 0 -a $last -ge 0 ]; then
			Log `_ 'SLOTLIST -> lastslot set to %s' "$last"`
			lastslot=$last
		fi
		if [ $firstslot -lt 0 ]; then
			Exit 2 \
			     `_ '<none>'` \
			     `_ 'cannot determine first slot'`
			return $?		# in case we are internal
		elif [ $lastslot -lt 0 ]; then
			Exit 2 \
			     `_ '<none>'` \
			     `_ 'cannot determine last slot'`
			return $?		# in case we are internal
		fi
	fi
	amanda_slot_list=
	for slot in $slot_list; do
		if [ $slot -ge $firstslot -a $slot -le $lastslot ]; then
			amanda_slot_list="$amanda_slot_list $slot"
		fi
	done
	if [ -z "$amanda_slot_list" ]; then
		Exit 2 \
		     `_ '<none>'` \
		     "no slots available"
		return $?			# in case we are internal
	fi
	slot_list="$amanda_slot_list"
}

###
# Read the labelfile and scan for a particular entry.
###

read_labelfile() {
	labelfile_entry_found=0
	labelfile_label=
	labelfile_barcode=

	lbl_search=$1
	bc_search=$2

	line=0
	while read lbl bc junk; do
		line=`expr $line + 1`
		if [ -z "$lbl" -o -z "$bc" -o -n "$junk" ]; then
			Log       `_ 'ERROR    -> Line %s malformed: %s %s %s' "$line" "$lbl" "$bc" "$junk"`
			LogAppend `_ '         -> Remove %s and run "%s %s update"' "$labelfile" "$sbindir/amtape" "$config"`
			Exit 2 \
			     `_ '<none>'` \
			     `_ 'Line %s malformed in %s: %s %s %s' "$line" "$labelfile" "$lbl" "$bc" "$junk"`
			return $?		# in case we are internal
		fi
		if [ $lbl = "$lbl_search" -o $bc = "$bc_search" ]; then
			if [ $labelfile_entry_found -ne 0 ]; then
				Log       `_ 'ERROR    -> Duplicate entries: %s line %s' "$labelfile" "$line"`
				LogAppend `_ '         -> Remove %s and run "%s %s update"' "$labelfile" "$sbindir/amtape" "$config"`
				Exit 2 \
				     `_ '<none>'` \
				     `_ 'Duplicate entries: %s line %s' "$labelfile" "$line"`
				return $?	# in case we are internal
			fi
			labelfile_entry_found=1
			labelfile_label=$lbl
			labelfile_barcode=$bc
		fi
	done
}

lookup_label_by_barcode() {
    [ -z "$1" ] && return
    read_labelfile "" "$1" < $labelfile
    echo "$labelfile_label"
}

lookup_barcode_by_label() {
    [ -z "$1" ] && return
    read_labelfile "$1" "" < $labelfile
    echo "$labelfile_barcode"
}

remove_from_labelfile() {
	labelfile=$1
	lbl_search=$2
	bc_search=$3

	internal_remove_from_labelfile "$lbl_search" "$bc_search" < $labelfile >$labelfile.new
	if [ $labelfile_entry_found -ne 0 ]; then
		mv -f $labelfile.new $labelfile
		LogAppend `_ 'Removed Entry "%s %s" from barcode database' "$labelfile_label" "$labelfile_barcode"`
	fi
}

internal_remove_from_labelfile() {
	labelfile_entry_found=0
	labelfile_label=
	labelfile_barcode=

	lbl_search=$1
	bc_search=$2

	line=0
	while read lbl bc junk; do
		line=`expr $line + 1`
		if [ -z "$lbl" -o -z "$bc" -o -n "$junk" ]; then
			Log       `_ 'ERROR    -> Line %s malformed: %s %s %s' "$line" "$lbl" "$bc" "$junk"`
			LogAppend `_ '         -> Remove %s and run "%s %s update"' "$labelfile" "$sbindir/amtape" "$config"`
			Exit 2 \
			     `_ '<none>'` \
			     `_ 'Line %s malformed in %s: %s %s %s' "$line" "$labelfile" "$lbl" "$bc" "$junk"`
			return $?		# in case we are internal
		fi
		if [ $lbl = "$lbl_search" -o $bc = "$bc_search" ]; then
			if [ $labelfile_entry_found -ne 0 ]; then
				Log       `_ 'ERROR    -> Duplicate entries: %s line %s' "$labelfile" "$line"`
				LogAppend `_ '         -> Remove %s and run "%s %s update"' "$labelfile" "$sbindir/amtape" "$config"`
				Exit 2 \
				     `_ '<none>'` \
				     `_ 'Duplicate entries: %s line %s' "$labelfile" "$line"`
				return $?	# in case we are internal
			fi
			labelfile_entry_found=1
			labelfile_label=$lbl
			labelfile_barcode=$bc
		else
			echo $lbl $bc
		fi
	done
}

###
# Add a new slot -> label correspondance to the slotinfo file, removing any previous
# information about that slot.
###

record_label_in_slot() {
    [ -z "$slotinfofile" ] && return
    newlabel="$1"
    newslot="$2"

    (
	if [ -f "$slotinfofile" ]; then
		grep -v "^$newslot " < "$slotinfofile"
	fi
	echo "$newslot $newlabel"
    ) > "$slotinfofile~"
    mv "$slotinfofile~" "$slotinfofile"
}

###
# Remove a slot from the slotinfo file
###

remove_slot_from_slotinfo() {
    [ -z "$slotinfofile" ] && return
    emptyslot="$1"

    (
	if [ -f "$slotinfofile" ]; then
		grep -v "^$emptyslot " < "$slotinfofile"
	fi
    ) > "$slotinfofile~"
    mv "$slotinfofile~" "$slotinfofile"
}

###
# Assuming get_mtx_status has been run,
# - if we have barcodes, regenerate the slotinfo file completely by
#   mapping barcodes in the status into labels using the labelfile
# - otherwise, remove all empty slots from the slotinfo file
###

regenerate_slotinfo_from_mtx() {
    [ -z "$slotinfofile" ] && return
    [ "$mtx_status_valid" = "1" ] || return

    if [ "$havereader" = "1" ]; then
	# rewrite slotinfo entirely based on the status, since it has barcodes
	:> "$slotinfofile~"
	sed -n '/.*Storage Element \([0-9][0-9]*\).*VolumeTag *= *\([^ ]*\) *$/{
s/.*Storage Element \([0-9][0-9]*\).*VolumeTag *= *\([^ ]*\) *$/\1 \2/
p
}' < $mtx_status | while read newslot newbarcode; do
		newlabel=`lookup_label_by_barcode "$newbarcode"`
		if [ -n "$newlabel" ]; then
		    echo "$newslot $newlabel" >> "$slotinfofile~"
		fi
	    done
	mv "$slotinfofile~" "$slotinfofile"
    else
	# just remove empty slots from slotinfo

	# first determine which slots are not really empty, but are
	# loaded into a data transfer element
loadedslots=`sed -n '/.*(Storage Element \([0-9][0-9]*\) Loaded).*/{
s/.*(Storage Element \([0-9][0-9]*\) Loaded).*/\1/g
p
}' < $mtx_status`

	# now look for any slots which are empty, but which aren't
	# in the set of loaded slots
	sed -n '/.*Storage Element \([0-9][0-9]*\): *Empty.*/{
s/.*Storage Element \([0-9][0-9]*\): *Empty.*/\1/g
p
}' < $mtx_status | while read emptyslot; do
	    reallyempty=1
	    if [ -n "$loadedslots" ]; then
		for loadedslot in $loadedslots; do
		    [ "$loadedslot" = "$emptyslot" ] && reallyempty=0
		done
	    fi
	    if [ "$reallyempty" = "1" ]; then
		remove_slot_from_slotinfo "$emptyslot"
	    fi
	done
    fi
}

DBGFILE=`amgetconf dbopen.$myname 2>/dev/null`
if [ -z "$DBGFILE" ]
then
	DBGFILE=/dev/null			# will try this again below
fi

changerfile=`amgetconf changerfile 2>/dev/null`
if [ -z "$changerfile" ]; then
	Exit 2 \
	     `_ '<none>'` \
	     "changerfile must be specified in amanda.conf"
fi

rawtape=`amgetconf tapedev 2>/dev/null`
if [ -z "$rawtape" ]; then
	Exit 2 \
	     `_ '<none>'` \
	     "tapedev may not be empty"
fi
tape=`tape_device_filename "$rawtape"`
if [ -z "$tape" ]; then
        Exit 2 \
             ` _ '<none>'` \
             "tapedev $rawtape is not a tape device."
elif [ $tape = "/dev/null" -o `expr "$tape" : 'null:'` -eq 5 ]; then
	Exit 2 \
	     `_ '<none>'` \
	     "tapedev ($tape) may not be the null device"
fi
# Confusingly, TAPE is the name of the changer device...
TAPE=`amgetconf changerdev 2>/dev/null`
if [ -z "$TAPE" ]; then
	Exit 2 \
	     `_ '<none>'` \
	     "changerdev may not be empty"
elif [ $TAPE = "/dev/null" ]; then
	Exit 2 \
	     `_ '<none>'` \
	     "changerdev ($TAPE) may not be the null device"
fi
export TAPE					# for mtx command

CHANGER=$TAPE 
export CHANGER					# for mtx command

#### Set up the various config files.

conf_match=`expr "$changerfile" : .\*\.conf\$`
if [ $conf_match -ge 6 ]; then
	configfile=$changerfile
	changerfile=`echo $changerfile | sed 's/.conf$//g'`
else
	configfile=$changerfile.conf
fi

if [ ! -e $configfile ]; then
	Exit 2 \
	     `_ '<none>'` \
	     "configuration file \"$configfile\" doesn't exist"
fi
if [ ! -f $configfile ]; then
	Exit 2 \
	     `_ '<none>'` \
	     "configuration file \"$configfile\" is not a file"
fi

cleanfile=$changerfile-clean
accessfile=$changerfile-access
slotfile=$changerfile-slot
labelfile=$changerfile-barcodes
slotinfofile=""
[ ! -s $cleanfile ] && echo 0 > $cleanfile
[ ! -s $accessfile ] && echo 0 > $accessfile
[ ! -s $slotfile ] && echo -1 > $slotfile
[ ! -f $labelfile ] && > $labelfile
cleancount=`cat $cleanfile`
accesscount=`cat $accessfile`

#### Dig out of the config file what is needed

varlist=
varlist="$varlist firstslot"
varlist="$varlist lastslot"
varlist="$varlist cleanslot"
varlist="$varlist cleancycle"
varlist="$varlist OFFLINE_BEFORE_UNLOAD"	# old name
varlist="$varlist offline_before_unload"
varlist="$varlist unloadpause"
varlist="$varlist AUTOCLEAN"			# old name
varlist="$varlist autoclean"
varlist="$varlist autocleancount"
varlist="$varlist havereader"
varlist="$varlist driveslot"
varlist="$varlist poll_drive_ready"
varlist="$varlist initial_poll_delay"
varlist="$varlist max_drive_wait"
varlist="$varlist slotinfofile"

for var in $varlist
do
	val="`cat $configfile 2>/dev/null | sed -n '
# Ignore comment lines (anything starting with a #).
/^[ 	]*#/d
# Find the first var=val line in the file, print the value and quit.
/^[ 	]*'$var'[ 	]*=[ 	]*\([^ 	][^ 	]*\).*/	{
	s/^[ 	]*'$var'[ 	]*=[ 	]*\([^ 	][^ 	]*\).*/\1/p
	q
}
'`"
	eval $var=\"$val\"
done

# Deal with driveslot first so we can get DBGFILE set if we are still
# using the old amgetconf.

if [ -z "$driveslot" ]; then
	driveslot=0;
fi

# Get DBGFILE set if it is not already.

if [ $DBGFILE = /dev/null ]; then
	if [ -d "$DBGDIR" ]; then
		DBGFILE=$DBGDIR/changer.debug.drive$driveslot
	else
		DBGFILE=/dev/null
	fi
	Log `_ '=== Start %s ===' "\`date\`"`
fi

stdout=$TMPDIR/$myname.1.$$
stderr=$TMPDIR/$myname.2.$$
mtx_status=$TMPDIR/$myname.status.$$
trap "rm -f $stdout $stderr $mtx_status" 0	# exit cleanup

Log `_ 'Using config file %s' "$configfile"`

# Log the argument list.

Log `_ "Arg info:"`
LogAppend "\$# = $#"
i=0
LogAppend "\$$i = \"$argv0\""
for arg in "$@"; do
	i=`expr $i + 1`
	LogAppend "\$$i = \"$arg\""
done

# Set the default config values for those not in the file.  Log the
# results and make sure each is valid (numeric).

firstslot=${firstslot:-'-1'}				# default: mtx status
lastslot=${lastslot:-'-1'}				# default: mtx status
cleanslot=${cleanslot:-'-1'}				# default: -1
cleancycle=${cleancycle:-'120'}				# default: two minutes
if [ -z "$offline_before_unload" -a -n "$OFFLINE_BEFORE_UNLOAD" ]; then
	offline_before_unload=$OFFLINE_BEFORE_UNLOAD	# (old name)
fi
offline_before_unload=${offline_before_unload:-'0'}	# default: 0
unloadpause=${unloadpause:-'0'}				# default: 0
if [ -z "$autoclean" -a -n "$AUTOCLEAN" ]; then
	autoclean=$AUTOCLEAN				# (old name)
fi
autoclean=${autoclean:-'0'}				# default: 0
autocleancount=${autocleancount:-'99'}			# default: 99
havereader=${havereader:-'0'}				# default: 0
poll_drive_ready=${poll_drive_ready:-'3'}		# default: three seconds
initial_poll_delay=${initial_poll_delay:-'0'}		# default: zero zeconds
max_drive_wait=${max_drive_wait:-'120'}			# default: two minutes

# check MT and MTX for sanity
if test "${MTX%${MTX#?}}" = "/"; then
    if ! test -f "${MTX}"; then
	Exit 2 \
	    `_ '<none>'` \
	    `_ "mtx binary at '%s' not found" "$MTX"`
    fi
    if ! test -x "${MTX}"; then
	Exit 2 \
	    `_ '<none>'` \
	    `_ "mtx binary at '%s' is not executable" "$MTX"`
    fi
else
    # try running it to see if the shell can find it
    "$MTX" >/dev/null 2>/dev/null
    if test $? -eq 127 -o $? -eq 126; then
	Exit 2 \
	    `_ '<none>'` \
	    `_ "Could not run mtx binary at '%s'" "$MTX"`
    fi
fi

error=`try_find_mt`
if test $? -ne 0; then
    Exit 2 '<none>' $error
fi

get_slot_list

Log `_ "Config info:"`
for var in $varlist; do
	if [ $var = "OFFLINE_BEFORE_UNLOAD" ]; then
		continue			# old name
	elif [ $var = "AUTOCLEAN" ]; then
		continue			# old name
	elif [ $var = "slotinfofile" ]; then
		continue			# not numeric
	fi
	eval val=\"'$'$var\"
	if [ -z "$val" ]; then
		Exit 2 \
		     `_ '<none>'` \
		     `_ '%s missing in %s' "$var" "$configfile"`
	fi
	if IsNumeric "$val" ; then
		:
	else
		Exit 2 \
		     `_ '<none>'` \
		     `_ '%s (%s) not numeric in %s' "$var" "$val" "$configfile"`
	fi
	LogAppend $var = \"$val\"
done

# Run the rest of the config file sanity checks.

if [ $firstslot -gt $lastslot ]; then
	Exit 2 \
	     `_ '<none>'` \
	     `_ 'firstslot (%s) greater than lastslot (%s) in %s' "$firstslot" "$lastslot" "$configfile"`
fi
if [ $autoclean -ne 0 -a $cleanslot -lt 0 ]; then
	Exit 2 \
	     `_ '<none>'` \
	     `_ 'autoclean set but cleanslot not valid (%s)' "$cleanslot"`
fi

# Set up the current slot

currentslot=`cat $slotfile`
if IsNumeric "$currentslot" ; then
	if [ $currentslot -lt $firstslot ]; then
		Log `_ 'SETUP    -> current slot %s less than %s ... resetting to %s' "$currentslot" "$firstslot" "$firstslot"`
		currentslot=$firstslot
	elif [ $currentslot -gt $lastslot ]; then
		Log `_ 'SETUP    -> current slot %s greater than %s ... resetting to %s' "$currentslot" "$lastslot" "$lastslot"`
		currentslot=$lastslot
	fi
else
	Log `_ 'SETUP    -> contents of %s (%s) invalid, setting current slot to first slot (%s)' "$slotfile" "$currentslot" "$firstslot"`
	currentslot=$firstslot
fi

found_current=0
first_slot_in_list=-1
next_slot_after_current=-1
for slot in $slot_list; do
	if [ $first_slot_in_list -lt 0 ]; then
		first_slot_in_list=$slot	# in case $firstslot is missing
	fi
	if [ $slot -eq $currentslot ]; then
		found_current=1
		break
	elif [ $slot -gt $currentslot ]; then
		next_slot_after_current=$slot	# $currentslot is missing
		break
	fi
done
if [ $found_current -eq 0 ]; then
	if [ $next_slot_after_current -lt 0 ]; then
		new_currentslot=$first_slot_in_list
	else
		new_currentslot=$next_slot_after_current
	fi
	Log `_ 'WARNING  -> current slot %s not available, setting current slot to next slot (%s)' "$currentslot" "$new_currentslot"`
	currentslot=$new_currentslot
fi

# More routines.

###
# Eject the current tape and put it away.
###

eject() {
	test -n "$DEBUG" && set -x
	Log `_ 'EJECT    -> ejecting tape from %s' "$tape"`
	get_loaded_info 
	if [ $loadedslot -gt 0 ]; then
		Log `_ 'EJECT    -> moving tape from drive %s to storage slot %s' "$driveslot" "$loadedslot"`
		if [ $offline_before_unload -ne 0 ]; then
                        Run try_eject_device $tape
		fi
		sleep $unloadpause
		result=`Run $MTX unload $loadedslot $driveslot 2>&1`
		status=$?
		Log `_ '         -> status %s, result "%s"' "$status" "$result"`
		mtx_status_valid=0
		if [ $status -ne 0 ]; then
			answer="$result"
			code=2
		else
			answer="$rawtape"
			code=0
		fi
	else
		answer=`_ 'Drive was not loaded'`
		code=1
	fi
	Exit $code "$loadedslot" "$answer"
	return $?				# in case we are internal
}

###
# Reset the robot back to the first slot.
###

reset() {
	test -n "$DEBUG" && set -x
	Log `_ 'RESET    -> loading tape from slot %s to drive %s (%s)' "$firstslot" "$driveslot" "$tape"`
	# Call loadslot without doing it as an internal and let it finish
	# things up.
	loadslot $firstslot
	# NOTREACHED
	Exit 2 `_ '<none>'` `_ 'reset: should not get here'`
	return $?				# in case we are internal
}

###
# Unload the current tape (if necessary) and load a new one (unless
# "advance").  If no tape is loaded, get the value of "current" from
# $slotfile.
###

loadslot() {
	test -n "$DEBUG" && set -x
	if [ $# -lt 1 ]; then
		Exit 2 `_ '<none>'` `_ 'Missing -slot argument'`
		return $?			# in case we are internal
	fi
	whichslot=$1
	Log `_ 'LOADSLOT -> load drive %s (%s) from slot %s' "$driveslot" "$tape" "$whichslot"`

	numeric=`echo $whichslot | sed 's/[^0-9]//g'`
	case $whichslot in
	current|prev|next|advance)
		find_slot=$currentslot
		;;
	first)
		find_slot=$firstslot
		;;
	last)
		find_slot=$lastslot
		;;
	$numeric)
		find_slot=$numeric
		;;
	clean)
		find_slot=$cleanslot
		;;
	*)
		Exit 2 `_ '<none>'` `_ 'Illegal slot: "%s"' "$whichslot"`
		return $?			# in case we are internal
		;;
	esac

	# Find the requested slot in the slot list.  By loading the "set"
	# command with multiple copies, we guarantee that if the slot is
	# found, we can look both forward and backward without running
	# off the end.	Putting $cleanslot at the end allows us to find
	# that slot since it is not in $slot_list.
	get_slot_list
	set x $slot_list $slot_list $slot_list $cleanslot
	shift					# get rid of the "x"
	prev_slot=$1
	shift
	while [ $# -gt 0 ]; do
		if [ $1 -eq $find_slot ]; then
			break
		fi
		prev_slot=$1
		shift
	done
	if [ $# -le 0 ]; then
		if [ $find_slot -ge $firstslot -a $find_slot -le $lastslot ]; then
			Exit 2 \
			     `_ '<none>'` \
			     `_ 'Cannot find a tape in slot %s' "$find_slot "`
			return $?			# in case we are internal
		else
			Exit 2 \
			     `_ '<none>'` \
			     `_ 'Cannot find slot %s in slot list (%s)' "$find_slot " "$slot_list"`
			return $?			# in case we are internal
		fi
	fi

	# Determine the slot to load.
	case $whichslot in
	next|advance)
		shift
		loadslot=$1
		;;
	prev)
		loadslot=$prev_slot
		;;
	*)
		loadslot=$find_slot
	esac

	# If the desired slot is already loaded, we are done.  Only update
	# current slot if this is not the cleaning slot.
	get_loaded_info
	if [ $loadslot = $loadedslot ]; then
		if [ $loadslot -ne $cleanslot ]; then
			rm -f $slotfile
			echo $loadslot > $slotfile
		fi
		Exit 0 "$loadedslot" "$rawtape"
		return $?			# in case we are internal
	fi
	if [ $loadedslot -eq -2 ]; then
		Exit 0 "$loadedslot" "$rawtape"
		return $?			# in case we are internal
        fi

	# If we are loading the cleaning tape, bump the cleaning count
	# and reset the access count.  Otherwise, bump the access count
	# and see if it is time to do a cleaning.
	if [ $loadslot = $cleanslot ]; then
		rm -f $cleanfile $accessfile
		expr $cleancount + 1 > $cleanfile
		echo 0 > $accessfile
	else
		rm -f $accessfile
		expr $accesscount + 1 > $accessfile
		if [ $autoclean -ne 0 -a $accesscount -gt $autocleancount ]
		then
			internal_call=`expr $internal_call + 1`
			loadslot clean > /dev/null 2>&1
			status=$?
			internal_call=`expr $internal_call - 1`
			if [ $status -ne 0 ]; then
				Exit $status "$loadslot" "$exit_answer"
				return $?	# in case we are internal
			fi

			# Slot $cleanslot might contain an ordinary tape
			# rather than a cleaning tape.  A cleaning tape
			# *MIGHT* auto-eject; an ordinary tape does not.
			# We therefore have to read the status again to
			# check what actually happened.
			mtx_status_valid=0
			get_loaded_info
		fi
	fi

	# Unload whatever tape is in the drive.
	internal_call=`expr $internal_call + 1`
	eject > /dev/null 2>&1
	status=$?
	internal_call=`expr $internal_call - 1`
	if [ $status -gt 1 ]; then
		Exit $status "$exit_slot" "$exit_answer"
		return $?			# in case we are internal
	fi

	# If we were doing an "advance", we are done.
	if [ $whichslot = advance ]; then
		if [ $loadslot -ne $cleanslot ]; then
			rm -f $slotfile
			echo $loadslot > $slotfile
		fi
		Exit 0 "$loadslot" "/dev/null"
		return $?			# in case we are internal
	fi

	# Load the tape, finally!
	Log `_ "LOADSLOT -> loading tape from slot %s to drive %s (%s)" "$loadslot" "$driveslot" "$tape"`
	result=`Run $MTX load $loadslot $driveslot 2>&1`
	status=$?
	Log `_ '         -> status %s, result "%s"' "$status" "$result"`
	mtx_status_valid=0
	if [ $status -ne 0 ]; then
		Exit 2 "$loadslot" "$result"
		return $?			# in case we are internal
	fi

	###
	# Cleaning tapes never go "ready", so instead we just sit here
	# for "long enough" (as determined empirically by the user),
	# then return success.
	###
	if [ $loadslot -eq $cleanslot ]; then
		Run sleep $cleancycle
		Exit 0 "$loadslot" "$rawtape"
		return $?			# in case we are internal
	fi

	###
	# Wait for the drive to go online.
	###
	waittime=0
	ready=0
	sleep $initial_poll_delay
	while [ $waittime -lt $max_drive_wait ]; do
                amdevcheck_status $tape
		if [ $? -eq 0 ]; then
			ready=1
			break
		fi
		sleep $poll_drive_ready
		waittime=`expr $waittime + $poll_drive_ready`
	done
	if [ $ready -eq 0 ]; then
		Exit 2 "$loadslot" `_ 'Drive not ready after %s seconds: %s' "$max_drive_wait" "$amdevcheck_message"`
		return $?			# in case we are internal
	fi

	if [ $loadslot -ne $cleanslot ]; then
		rm -f $slotfile
		echo $loadslot > $slotfile
	fi
	Exit 0 "$loadslot" "$rawtape"
	return $?				# in case we are internal
}

###
# Return information about how the changer is configured and the current
# state of the robot.
###

info() {
	test -n "$DEBUG" && set -x
	get_loaded_info
	get_slot_list
	Log       `_ 'INFO     -> first slot: %s' "$firstslot"`
	LogAppend `_ '         -> current slot: %s' "$currentslot"`
	LogAppend `_ '         -> loaded slot: %s' "$loadedslot"`
	LogAppend `_ '         -> last slot: %s' "$lastslot"`
	LogAppend `_ '         -> slot list: %s' "$slot_list"`
	LogAppend `_ '         -> can go backwards: 1'`
	LogAppend `_ '         -> havereader: %s' "$havereader"`

        ###
	# Check if a barcode reader is configured or not.  If so, it
	# passes the 4th item in the echo back to amtape signifying it
	# can search based on barcodes.
	###
	reader=
        if [ $havereader -eq 1 ]; then
		reader=1
        fi

	if [ $currentslot -lt $firstslot -o $currentslot -gt $lastslot ]; then
		currentslot=$firstslot		# what "current" will get
	fi
	numslots=`expr $lastslot - $firstslot + 1`
	Exit 0 "$currentslot" "$numslots 1 $reader"
	return $?				# in case we are internal
}

###
# Adds the label and barcode for the currently loaded tape to the
# barcode file.  Return an error if the database is messed up.
###

addlabel() {
	test -n "$DEBUG" && set -x
	if [ $# -lt 1 ]; then
		Exit 2 `_ '<none>'` `_ 'Missing -label argument'`
		return $?			# in case we are internal
	fi
        tapelabel=$1
        get_loaded_info
	if [ $loadedslot -lt 0 ]; then
		Exit 1 `_ '<none>'` `_ 'No tape currently loaded'`
		return $?			# in case we are internal
	fi
	record_label_in_slot "$tapelabel" "$loadedslot"
	if [ $havereader -eq 0 ]; then
		Exit 0 "$loadedslot" "$rawtape"	# that's all we needed
		return $?			# in case we are internal
	fi
	if [ -z "$loadedbarcode" ]; then
		Exit 1 `_ '<none>'` `_ 'No barcode found for tape %s.' $tapelabel`
		return $?			# in case we are internal
	fi
	Log       `_ 'LABEL    -> Adding label "%s" with barcode "%s" for slot %s into %s' "$tapelabel" "$loadedbarcode" "$loadedslot" "$labelfile"`
	read_labelfile "$tapelabel" "$loadedbarcode" < $labelfile
	if [ $labelfile_entry_found -ne 0 ]; then
		lf_val=
		if [ "$labelfile_barcode" != "$loadedbarcode" ]; then
			lf_type=label
			lf_val=$tapelabel
			val_type=barcode
			old_val=$labelfile_barcode
			new_val=$loadedbarcode
		elif [ "$labelfile_label" != "$tapelabel" ]; then
			lf_type=barcode
			lf_val=$loadedbarcode
			val_type=label
			old_val=$labelfile_label
			new_val=$tapelabel
		fi
		if [ -n "$lf_val" ]; then
			if [ "$val_type" = "barcode" ]; then
				remove_from_labelfile $labelfile "" "$old_val"
			else
				remove_from_labelfile $labelfile "$old_val" ""
			fi
			echo "$tapelabel $loadedbarcode" >> $labelfile
			LogAppend `_ '         -> appended %s entry: %s %s' "$labelfile" "$tapelabel" "$loadedbarcode"`
		else
			LogAppend `_ "         -> already synced"`
		fi
	else
		echo "$tapelabel $loadedbarcode" >> $labelfile
		LogAppend `_ '         -> appended %s entry: %s %s' "$labelfile" "$tapelabel" "$loadedbarcode"`
	fi
	Exit 0 "$loadedslot" "$rawtape"
	return $?				# in case we are internal
}

###
# Look for a label in the barcode file.  If found, locate the slot it's
# in by looking for the barcode in the mtx output, then load that tape.
###

searchtape() {
	test -n "$DEBUG" && set -x
	if [ $# -lt 1 ]; then
		Exit 2 `_ '<none>'` `_ 'Missing -search argument'`
		return $?			# in case we are internal
	fi
        tapelabel=$1
	if [ $havereader -eq 0 ]; then
		Exit 2 `_ '<none>'` `_ 'Not configured with barcode reader'`
		return $?			# in case we are internal
	fi
	Log `_ 'SEARCH   -> Hunting for label "%s"' "$tapelabel"`
	read_labelfile "$tapelabel" "" < $labelfile
	if [ $labelfile_entry_found -eq 0 ]; then
		LogAppend `_ '         -> !!! label "%s" not found in %s !!!' "$tapelabel" "$labelfile"`
		LogAppend `_ '         -> Remove %s and run "%s %s update"' "$labelfile" "$sbindir/amtape" "$config"`
		Exit 1 \
		     `_ '<none>'` \
		     `_ '%s: label "%s" not found in %s' "$tapelabel" "$tapelabel" "$labelfile"`
		return $?			# in case we are internal
	fi
	LogAppend `_ '         -> barcode is "%s"' "$labelfile_barcode"`
	get_mtx_status
	if [ $mtx_status_valid -eq 0 ]; then
		Exit 2 \
		     `_ '<none>'` \
		     `head -1 $mtx_status`
		return $?
	fi
	foundslot=`sed -n '
/VolumeTag *= *'$labelfile_barcode' *$/			{
	s/.*Storage Element \([0-9][0-9]*\).*/\1/p
	q
}
' < $mtx_status`
	LogAppend `_ '         -> foundslot is %s' "$foundslot"`
	if [ -z "$foundslot" ]; then
		LogAppend `_ 'ERROR    -> !!! Could not find slot for barcode "%s"!!!' "$labelfile_barcode"`
		LogAppend `_ '         -> Remove %s and run "%s %s update"' "$labelfile" "$sbindir/amtape" "$config"`
		Exit 1 \
		     `_ '<none>'` \
		     `_ 'barcode "%s" not found in mtx status output' "$labelfile_barcode"`
		return $?			# in case we are internal
	fi
	# Call loadslot without doing it as an internal and let it finish
	# things up.
	loadslot $foundslot
	# NOTREACHED
	Exit 2 `_ '<none>'` `_ 'searchtape: should not get here'`
	return $?				# in case we are internal
}

###
# Program invocation begins here
###

if [ $# -lt 1 ]; then
	Exit 2 `_ '<none>'` `_ 'Usage: %s -command args' "$myname"`
fi
cmd=$1
shift
case "$cmd" in
-slot)
	loadslot "$@"
	;;
-info)
	info "$@"
	;;
-reset)
	reset "$@"
	;;
-eject)
	eject "$@"
	;;
-label) 
	addlabel "$@"
	;;
-search)
	searchtape "$@"
	;;
-clean)
	loadslot clean
	;;
*)
	Exit 2 `_ '<none>'` `_ 'unknown option: %s' "$cmd"`
	;;
esac

Exit 2 `_ '<none>'` `_ '%s: should not get here' "$myname"`
