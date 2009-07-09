#!@SHELL@
#
# Amanda, The Advanced Maryland Automatic Network Disk Archiver
# Copyright (c) 1991-1998 University of Maryland at College Park
# All Rights Reserved.
#
# Permission to use, copy, modify, distribute, and sell this software and its
# documentation for any purpose is hereby granted without fee, provided that
# the above copyright notice appear in all copies and that both that
# copyright notice and this permission notice appear in supporting
# documentation, and that the name of U.M. not be used in advertising or
# publicity pertaining to distribution of the software without specific,
# written prior permission.  U.M. makes no representations about the
# suitability of this software for any purpose.  It is provided "as is"
# without express or implied warranty.
#
# U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
# BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
# OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# Author: James da Silva, Systems Design and Analysis Group
#			   Computer Science Department
#			   University of Maryland at College Park
#

#
# chg-chs.sh - chs tape changer script
#

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

pname="chg-chs"

# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

if [ -d "@AMANDA_DBGDIR@" ]; then
	logfile=@AMANDA_DBGDIR@/changer.debug
else
	logfile=/dev/null
fi

CHS=@CHS@

ourconf=`amgetconf changerfile`
changerdev=`amgetconf changerdev`
if test -n "$changerdev" && test x"$changerdev" != x/dev/null; then
	CHS="$CHS -f$changerdev"
fi

# read in some config parameters

if [ \! -f $ourconf ]; then
	answer=`_ '<none> %s: %s does not exist' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

firstslot=`awk '$1 == "firstslot" {print $2}' $ourconf 2>/dev/null`
if [ "$firstslot" = "" ]; then
	answer=`_ '<none> %s: firstslot not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

lastslot=`awk '$1 == "lastslot" {print $2}' $ourconf 2>/dev/null`
if [ "$lastslot" = "" ]; then
	answer=`_ '<none> %s: lastslot not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

nslots=`expr $lastslot - $firstslot + 1`

gravity=`awk '$1 == "gravity" {print $2}' $ourconf 2>/dev/null`
if [ "$gravity" = "" ]; then
	answer=`_ '<none> %s: gravity not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

needeject=`awk '$1 == "needeject" {print $2}' $ourconf 2>/dev/null`
if [ "$needeject" = "" ]; then
	answer=`_ '<none> %s: needeject not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

multieject=`awk '$1 == "multieject" {print $2}' $ourconf 2>/dev/null`
if [ "$multieject" = "" ]; then
	echo `_ 'Note -> multieject not specified in %s' "$ourconf"` >> $logfile
	multieject=0
fi

ejectdelay=`awk '$1 == "ejectdelay" {print $2}' $ourconf 2>/dev/null`
if [ "$ejectdelay" = "" ]; then
	echo `_ 'Note -> ejectdelay not specified in %s' "$ourconf"` >> $logfile
	ejectdelay=0
fi

ourstate=`awk '$1 == "statefile" {print $2}' $ourconf 2>/dev/null`
if [ "$ourstate" = "" ]; then
	answer=`_ '<none> %s: statefile not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

# read in state: only curslot and curloaded at the present time

curslot=`awk '$1 == "curslot" {print $2}' $ourstate 2>/dev/null`
if [ "$curslot" = "" ]; then
	curslot=$firstslot
fi

curloaded=`awk '$1 == "curloaded" {print $2}' $ourstate 2>/dev/null`
if [ "$curloaded" = "" ]; then
	curloaded=0
fi


# process the command-line

# control vars to avoid code duplication: not all shells have functions!
usage=0
checkgravity=0
ejectslot=0
loadslot=0
slotempty=0

if [ $# -ge 1 ]; then command=$1; else command="-usage"; fi

case "$command" in

-info) # return basic information about changer

	backwards=`expr 1 - $gravity`
	answer="$curslot $nslots $backwards"
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 0
	;;

-reset) # reset changer

	checkgravity=0
	loadslot=1
	newslot=$firstslot

	# XXX put changer-specific reset here, if applicable
	;;

-eject) # eject tape if loaded

	checkgravity=0
	loadslot=0
	newslot=$curslot
	ejectslot=1

	if [ $curloaded -eq 0 ]; then
		answer=`_ '%s %s: slot already empty' "$curslot" "$pname"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 1
	fi
	;;

-slot)	# change to slot

	checkgravity=1
	loadslot=1

	slotparm=$2
	case "$slotparm" in
	[0-9]*)	
		newslot=$slotparm
		if [ \( $newslot -gt $lastslot \) -o \
		     \( $newslot -lt $firstslot \) ]; then
			answer =`_ '%s %s: no slot %s: legal range is %s ... %s' "$newslot" "$pname" "$newslot" "$firstslot" "$lastslot"`
			echo `_ 'Exit ->'` $answer >> $logfile
			echo $answer
			exit 1
		fi
		;;
	current)
		newslot=$curslot
		;;
	first)
		newslot=$firstslot
		;;
	last)
		newslot=$lastslot
		;;
	next|advance)
		newslot=`expr $curslot + 1`
		if [ $newslot -gt $lastslot ]; then
			newslot=$firstslot
		fi
		if [ $slotparm = advance ]; then
			loadslot=0
		fi
		;;
	prev)
		newslot=`expr $curslot - 1`
		if [ $newslot -lt $firstslot ]; then
			newslot=$lastslot
		fi
		;;
	*)
		answer=`_ '<none> %s: bad slot name "%s"' "$pname" "$slotparm"`
		echo `_ 'Exit ->'` "$answer" >> $logfile
		echo $answer
		exit 1
		;;
	esac
	;;
*)
	usage=1
	;;
esac


if [ $usage -eq 1 ]; then
	answer=`_ '<none> usage: %s {-reset | -slot [<slot-number>|current|next|prev|advance]}' "$pname"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi


# check for legal move

if [ \( $checkgravity -eq 1 \) -a \( $gravity -ne 0 \) ]; then
	if [ \( $newslot -lt $curslot \) -o \( "$slotparm" = "prev" \) ]
	then
		answer=`_ '%s %s: cannot go backwards in gravity stacker' "$newslot" "$pname"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 1
	fi
fi

# get tape device name

device=`awk '$1 == "slot" && $2 == '$newslot' {print $3}' $ourconf 2>/dev/null`
if [ "$device" = "" ]; then
	answer=`_ '%s %s: slot %s device not specified in %s' "$newslot" "$pname" "$newslot" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

# check if load needs an eject first

if [ \( $needeject -eq 1 \) -a \( $loadslot -eq 1 \) -a \
     \( $curloaded -eq 1 \) -a \( $newslot -ne $curslot \) ]; then
	ejectslot=1
fi


if [ $ejectslot -eq 1 ]; then	# eject the tape from the drive

	# XXX put changer-specific load command here, if applicable

	curloaded=0		# unless something goes wrong
	slotempty=0

	# generically, first check that the device is there

	if [ ! -c $device ]; then
		answer=`_ '%s %s: %s: not a device file' "$newslot" "$pname" "$device"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	fi

	# if multiple eject is required, do it now
	if [ $multieject -eq 1 ]; then
		loopslot=$curslot
		while [ $loopslot -lt $newslot ]; do
                        try_eject_device $device
			if [ $? -ne 0 ]; then
				answer=`_ '%s %s: %s: unable to change slot %s' "$newslot" "$pname" "$device" "$loopslot"`
				echo `_ 'Exit ->'` $answer >> $logfile
				echo $answer
				exit 2
			fi
			loopslot=`/usr/bin/expr $loopslot + 1`
		done
	fi
  
	# second, try to unload the device
        try_eject_device $device
	$CHS deselect -d1 -s$curslot >/dev/null 2>&1
	if [ $? -ne 0 ]; then
		#
		# XXX if the changer-specific eject command can distinguish
		# betweeen "slot empty" and more serious errors, return 1
		# for the first case, 2 for the second case.  Generically,
		# we just presume an error signifies an empty slot.
		#
		#slotempty=1
		answer=`_ '<none> %s: tape unload to slot %s failed' "$pname" "$curslot"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	else
		sleep $ejectdelay
	fi
fi

if [ \( $loadslot -eq 1 \) -a \( \( $curloaded -ne 1 \) -o \( \( $curloaded -eq 1 \) -a \( $newslot -ne $curslot \) \) \) ]; then	# load the tape from the slot

	# XXX put changer-specific load command here, if applicable

	curloaded=1		# unless something goes wrong
	slotempty=0

	# generically, first check that the device is there

	if [ ! -c $device ]; then
		answer=`_ '%s %s: %s: not a device file' "$newslot" "$pname" "$device"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	fi

	$CHS select -s$newslot -d1 >/dev/null 2>&1
	if [ $? -ne 0 ]; then
		answer=`_ '<none> %s: tape load from slot %s failed' "$pname" "$newslot"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	fi
	sleep 60

	# second, try to rewind the device
        amdevcheck_status $device
	if [ $? -ne 0 ]; then
		#
		# XXX if the changer-specific load command can distinguish
		# betweeen "slot empty" and more serious errors, return 1
		# for the first case, 2 for the second case.  Generically,
		# we just presume an error signifies an empty slot.
		#
		slotempty=1
		curloaded=0
	fi
fi

# update state

echo "# $pname state cache: DO NOT EDIT!"	>  $ourstate
echo curslot $newslot 				>> $ourstate
echo curloaded $curloaded			>> $ourstate

# return slot info

if [ $slotempty -eq 1 ]; then
	answer=`_ '<none> %s: %s slot is empty: %s' "$pname" "$newslot" "$amdevcheck_message"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 1
fi

if [ "$command" = -slot -a "$slotparm" = advance ]; then
	device=/dev/null
fi

answer="$newslot $device"
echo `_ 'Exit ->'` $answer >> $logfile
echo $answer
exit 0
