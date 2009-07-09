#!@SHELL@

# chg-juke
#
# This assumes we have possibly rait-striped drives in several
# jukeboxes, controlled by the Fermilab "juke" package
#
# So we could have 3 drives in 3 jukeboxes:
#   changerscript="chg-juke"
#   changerfile=/some/file
#   tapedev="rait:/dev/nst{1,2,3}"
#   changerdev="myjuke{0,1,2}"
# Or, if the jukebox has multiple drives:
#   changerscript="chg-juke"
#   changerfile=/some/file
#   tapedev="rait:/dev/nst{1,2,3}"
#   changerdev="myjuke"
# We need therefore to generate lists with csh to expand the tapedev 
# and changerdev, and deal with the possibility that there are several 
# jukeboxes and several drives in each jukebox involved.

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"


#
# debugging...
#
if [ -d "@AMANDA_DBGDIR@" ]; then
	DBGFILE=@AMANDA_DBGDIR@/changer.debug
else
	DBGFILE=/dev/null
fi
exec 2>$DBGFILE
echo `_ 'args:'` $0 $* >&2
set -x 

#
# the usual config fun
#

getconf=$sbindir/amgetconf

#
# make sure we can find JUKE later
#
JUKE_DIR=/usr/local
# Fermilab specific
if [ -f /usr/local/etc/setups.sh ]
then
    . /usr/local/etc/setups.sh
    setup juke
fi

# add sbin, ucb, and the JUKE_DIR to PATH
PATH="$PATH:$JUKE_DIR/bin:/usr/sbin:/sbin:/usr/ucb"
export PATH JUKE_DIR

build_drivelists() {
    #
    # figure out which drives are in which jukebox
    #
    count=0
    for juke in $jlist
    do
	for d in $dlist
	do
	    if juke list -j $juke drive $d | grep 'drive [0-9]' >&2
	    then
		eval "drives_in_$juke=\"\$drives_in_$juke $d\""
	    fi
	done
    done
}

unload_drive_n_clean() {

    #
    # $1 is whether to clean it
    #
    cleanit=$1

    #
    # if the drive is ONLINE, mt unload it
    #
    if amdevcheck_status $tapedev; then
        try_eject_device $tapedev
    fi

    #
    # unload any tapes present, maybe load/unload a cleaning cartridge
    #
    for juke in $jlist
    do
	eval "jdlist=\"\$drives_in_$juke\""
	for drive in $jdlist
	do
	    juke unload -j $juke drive $drive >&2 || true
	    if juke list -j $juke drive $drive | grep '(empty)' >&2
	    then
		:
	    else
		echo `_ '%s %s unable to empty preceding tape from drive %s' "$slot" "$tapedev" "$drive"`
		exit 1
	    fi

	    if $cleanit
            then
                juke load -j $juke drive $drive clean
		sleep 120
                juke unload -j $juke drive $drive
            fi
	done
    done
}

load_drives() {
    #
    # load slots.  If it's a stripe, load several...
    #
    for juke in $jlist
    do
	eval "jdlist=\"\$drives_in_$juke\""
	jndrives=`echo $jdlist | wc -w`
	count=0
	for drive in $jdlist
	do
	    rslot=`expr $newslot '*' $jndrives + $count`
	    juke load -j $changerdev drive $drive slot $rslot >&2
	    if juke list -j $changerdev drive $drive | grep '(empty)' >&2
	    then
		echo `_ '$slot $tapedev unable to load tape into drive' "$slot" "$tapedev"`
		exit 1
	    fi
	    count=`expr $count + 1`
	done
    done

    #
    # wait for drive(s) to come online
    #
    count=0
    until amdevcheck_status $tapedev; do
	count=`expr $count + 1`
	if [ $count -gt 24 ] 
	then
	    echo `_ '%s %s never came online: %s' "$slot" "$tapedev" "$amdevcheck_message"`
	    exit 1
	fi
	sleep 5
    done
}


ONLINEREGEX="ONLINE|READY|sense[_ ]key[(]0x0[)]|sense key error = 0|^er=0$"

#
# get config variables
#
changerfile=`$getconf changerfile`
    tapedev=`$getconf tapedev`
 changerdev=`$getconf changerdev`
      dlist=`csh -c "echo $tapedev" | sed -e 's/rait://g' -e 's/tape://g'`
    ndrives=`echo $dlist | wc -w`
      jlist=`csh -c "echo $changerdev"`
     njukes=`echo $jlist | wc -w`
 totalslots=`for juke in $jlist ; do juke list -j $juke; done | 
		grep -v '^clean' | 
		grep 'slot [0-9]' | 
		wc -l`

if [ $ndrives -gt 1 ]
then
   #
   # if it's a 3 tape stripe and we have 30 actual slots
   # we only have 10 virtual slots...
   #
   totalslots=`expr $totalslots / $ndrives`
fi

build_drivelists

#
# get current slot if we have one
#
if [ -f "$changerfile" ] 
then
    slot="`cat $changerfile`"
else
    slot=0
    echo $slot > $changerfile
fi

#
# We treat -reset just like -slot 0
#
if [ x$1 = 'x-reset' ]
then
    set : -slot 0
    shift
fi

case x$1 in

x-slot) 

    #
    # handle special slots...
    #
    case "$2" in
    current)	newslot=$slot 		; load=true;;
    next)	newslot=`expr $slot + 1`; load=true;;
    advance)	newslot=`expr $slot + 1`; load=false;;
    prev)	newslot=`expr $slot - 1`; load=true;;
    first)	newslot=0		; load=true;;
    last)	newslot=-1		; load=true;;
    *)		newslot=$2		; load=true;;
    esac

    if [ $newslot = "clean" ]
    then
	unload_drive_n_clean true
    else 
	if [ 0 -gt $newslot ]
	then
	    newslot=`expr $totalslots - 1`
	fi

	if [ $totalslots -le  $newslot ]
	then
	    newslot=0
	fi

	echo $newslot > $changerfile
	slot=$newslot

	if $load
	then
	    unload_drive_n_clean false
	    load_drives
	fi
    fi

    echo $slot $tapedev
    ;;

x-info)
    echo $slot $totalslots 1
    exit 0
    ;;

x-eject)
    unload_drive_n_clean false
    echo $slot $tapedev
    ;;
esac

exit $rc
