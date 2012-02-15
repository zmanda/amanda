#! @SHELL@
#
# Amanda, The Advanced Maryland Automatic Network Disk Archiver
# Copyright (c) 1991-1999 University of Maryland at College Park
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
# chg-multi.sh - generic tape changer script
#

# source utility functions and values from configure
prefix=@prefix@
exec_prefix=@exec_prefix@
amlibexecdir=@amlibexecdir@
. ${amlibexecdir}/chg-lib.sh

pname="chg-multi"

if [ -d "@AMANDA_DBGDIR@" ]; then
	logfile=@AMANDA_DBGDIR@/changer.debug
else
	logfile=/dev/null
fi

echo `_ "arguments ->"` "$@" >> $logfile

ourconf=`amgetconf changerfile`

if ! error=try_find_mt; then
    echo <none> $error
    exit 2
fi

EXPR=expr
# EXPR=/usr/local/bin/expr # in case you need a more powerful expr...

# read in some config parameters

if [ ! -f "$ourconf" ]; then
	answer=`_ '<none> %s: %s does not exist' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

firstslot=`awk '$1 == "firstslot" {print $2}' $ourconf 2>/dev/null`
if [ -z "$firstslot" ]; then
	answer=`_ '<none> %s: firstslot not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

lastslot=`awk '$1 == "lastslot" {print $2}' $ourconf 2>/dev/null`
if [ -z "$lastslot" ]; then
	answer=`_ '<none> %s: lastslot not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

nslots=`$EXPR $lastslot - $firstslot + 1`

gravity=`awk '$1 == "gravity" {print $2}' $ourconf 2>/dev/null`
if [ -z "$gravity" ]; then
	answer=`_ '<none> %s: gravity not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

needeject=`awk '$1 == "needeject" {print $2}' $ourconf 2>/dev/null`
if [ -z "$needeject" ]; then
	answer=`_ '<none> %s: needeject not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

multieject=`awk '$1 == "multieject" {print $2}' $ourconf 2>/dev/null`
if [ -z "$multieject" ]; then
	echo `_ 'Note: setting multieject to a default of zero'` >> $logfile
	multieject=0
fi

ejectdelay=`awk '$1 == "ejectdelay" {print $2}' $ourconf 2>/dev/null`
if [ -z "$ejectdelay" ]; then
	echo `_ 'Note: setting ejectdelay to a default of zero'` >> $logfile
	ejectdelay=0
fi

posteject=`awk '$1 == "posteject" {print $2}' $ourconf 2>/dev/null`
if [ -z "$posteject" ]; then
	echo `_ 'Note: setting posteject to a default of "true"'` >> $logfile
	posteject=true
fi

ourstate=`awk '$1 == "statefile" {print $2}' $ourconf 2>/dev/null`
if [ -z "$ourstate" ]; then
	answer=`_ '<none> %s: statefile not specified in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

if [ -f "$ourstate" -a ! -r "$ourstate" ]; then
	answer=`_ "<none> %s: Can't read the statefile %s" "$pname" "$ourstate"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

if [ -f "$ourstate" -a ! -w "$ourstate" ]; then
	answer=`_ "<none> %s: Can't write the statefile %s" "$pname" "$ourstate"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

dirstate=`dirname $ourstate`
if [ ! -e "$dirstate" ]; then
	answer=`_ "<none> %s: Directory %s doesn't exist" "$pname" "$dirstate"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

if [ ! -d "$dirstate" ]; then
	answer=`_ '<none> %s: %s must be a directory' "$pname" "$dirstate"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

if [ ! -w "$dirstate" ]; then
	answer=`_ "<none> %s: Can't write to %s directory" "$pname" "$dirstate"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

# needeject and multieject are incompatible
if [ $needeject -eq 1 ] && [ $multieject -eq 1 ] ; then
	answer=`_ '<none> %s: needeject and multieject cannot be both enabled in %s' "$pname" "$ourconf"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi

# read in state: only curslot and curloaded at the present time

curslot=`awk '$1 == "curslot" {print $2}' $ourstate 2>/dev/null`
if [ -z "$curslot" ]; then
	curslot=$firstslot
fi

curloaded=`awk '$1 == "curloaded" {print $2}' $ourstate 2>/dev/null`
if [ -z "$curloaded" ]; then
	curloaded=0
fi


# process the command-line

# control vars to avoid code duplication: not all shells have functions!
usage=0
checkgravity=0
ejectslot=0
loadslot=0
slotempty=0
ejectonly=0

if [ $# -ge 1 ]; then command=$1; else command="-usage"; fi

case "$command" in

-info) # return basic information about changer

	backwards=`$EXPR 1 - $gravity`
	answer="$curslot $nslots $backwards"
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 0
	;;

-reset) # reset changer. Actually, we only reset changer state. We
	# trust that the operator has reloaded a stack and reset the
	# hardware. In most cases, we do not want to actually do
	# anything: if the operator has done something with the
	# hardware, we have no way to know what the actual current
	# slot is. If the hardware state has not changed, and what is
	# really wanted is to load the first slot, use "slot first"
	# instead 

	checkgravity=0
	loadslot=1
	newslot=$firstslot
	curslot=$firstslot
	# XXX put changer-specific reset here, if applicable
	;;

-eject) # eject tape if loaded. Note that if multieject is set, this
        # only can make sense if the position is last and gravity 1

	checkgravity=0
	loadslot=0
	newslot=$curslot
	ejectslot=1
	ejectonly=1
	if [ $multieject -eq 1 ] && \
	    ([ $gravity -eq 0 ] || [ $curslot -ne $lastslot ]) ; then 
		# Can't do this: if we eject, the stacker is going to
		# load the next tape, and our state will be botched
		answer=`_ '%s %s: Cannot use -eject with multieject/nogravity/notlastslot' "$curslot" "$pname"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 1
	fi    
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
		if [ $newslot -gt $lastslot ] || \
		     [ $newslot -lt $firstslot ] ; then
			answer=`_ '%s %s: no slot %s: legal range is %s ... %s' "$newslot" "$pname" "$newslot" "$firstslot" "$lastslot"`
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
		newslot=`$EXPR $curslot + 1`
		if [ $newslot -gt $lastslot ]; then
			newslot=$firstslot
		fi
		if [ $slotparm = advance ]; then
			loadslot=0
		fi
		;;
	prev)
		newslot=`$EXPR $curslot - 1`
		if [ $newslot -lt $firstslot ]; then
			newslot=$lastslot
		fi
		;;
	*)
		answer=`_ '<none> %s: bad slot name "%s"' "$pname" "$slotparm"`
		echo `_ 'Exit ->'` $answer >> $logfile
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
	answer=`_ '<none> usage: %s {-reset | -slot [<slot-number>|current|next|prev|advance] | -info | -eject}' "$pname"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
fi


# check for legal move

if [ $checkgravity -eq 1 ] && [ $gravity -ne 0 ] ; then
	if [ $newslot -lt $curslot ] || [ "$slotparm" = "prev" ] ; then
		answer=`_ '%s %s: cannot go backwards in gravity stacker' "$newslot" "$pname"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 1
	fi
fi

# Do the 'mt offline' style of stacker control if applicable
if [ $multieject -eq 1 ] && [ $loadslot -eq 1 ] && [ $newslot -ne $curslot ]
then
	# XXX put changer-specific load command here, if applicable

	curloaded=0		# unless something goes wrong
	slotempty=0

	while [ $curslot -ne $newslot ]; do
	    device=`awk '$1 == "slot" && $2 == '$curslot' {print $3}' $ourconf 2>/dev/null`
	    if [ "$device" = "" ]; then
		answer=`_ '%s %s: slot %s device not specified in %s' "$curslot" "$pname" "$curslot" "$ourconf"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	    fi
	    echo `_ '     -> offline'` "$device" >> $logfile
            if ! try_eject_device $device; then
		answer=`_ '%s %s: %s: unable to change to slot %s' "$newslot" "$pname" "$device" "$curslot"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	    fi
	    [ $ejectdelay -gt 0 ] && sleep $ejectdelay
	    echo `_ '     -> running'` $posteject $device >> $logfile
	    $posteject $device >> $logfile 2>&1
	    status=$?
	    if [ $status -ne 0 ]; then
		answer=`_ '%s %s: %s %s failed: %s' "$newslot" "$pname" "$posteject" "$device" "$status"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	    fi
	    curslot=`$EXPR $curslot + 1`
	    if [ $curslot -gt $lastslot ] ; then
		curslot=$firstslot
	    fi
	done
fi

if [ $ejectonly -eq 1 ] \
     || ([ $needeject -eq 1 ] \
	    && [ $loadslot -eq 1 ] \
	    && [ $curloaded -eq 1 ] \
	    && [ $newslot -ne $curslot ])
then
	# XXX put changer-specific load command here, if applicable

	curloaded=0		# unless something goes wrong
	slotempty=0

	# try to unload the current device
	device=`awk '$1 == "slot" && $2 == '$curslot' {print $3}' $ourconf 2>/dev/null`
	if [ "$device" = "" ]; then
		answer=`_ '%s %s: slot %s device not specified in %s' "$curslot" "$pname" "$curslot" "$ourconf"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	fi
	echo `_ '     -> offline'` $device >> $logfile
        try_eject_device $device
	if [ $? -ne 0 ]; then
		#
		# XXX if the changer-specific eject command can distinguish
		# betweeen "slot empty" and more serious errors, return 1
		# for the first case, 2 for the second case.  Generically,
		# we just presume an error signifies an empty slot.
		#
		slotempty=1
	else
		[ $ejectonly -eq 0 ] && [ $ejectdelay -gt 0 ] && sleep $ejectdelay
		echo `_ '     -> running '` $posteject $device >> $logfile
		$posteject $device >> $logfile 2>&1
		status=$?
		if [ $status -ne 0 ]; then
			answer=`_ '%s %s: %s %s failed: %s' "$newslot" "$pname" "$posteject" "$device" "$status"`
			echo `_ 'Exit ->'` $answer >> $logfile
			echo $answer
			exit 2
		fi
	fi
fi

if [ $loadslot -eq 1 ]; then	# load the tape from the slot

	# XXX put changer-specific load command here, if applicable

	curloaded=1		# unless something goes wrong
	slotempty=0
	curslot=$newslot

	# try to rewind the device
	device=`awk '$1 == "slot" && $2 == '$curslot' {print $3}' $ourconf 2>/dev/null`
	if [ "$device" = "" ]; then
		answer=`_ '%s %s: slot %s device not specified in %s' "$curslot" "$pname" "$curslot" "$ourconf"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	fi
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

echo `_ '# multi-changer state cache: DO NOT EDIT!'` >  $ourstate
echo curslot $newslot 				 >> $ourstate
echo curloaded $curloaded			 >> $ourstate

# return slot info

if [ $slotempty -eq 1 ]; then
	answer=`_ '%s %s: slot is empty: %s' "$newslot" "$pname" "$amdevcheck_message"`
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
