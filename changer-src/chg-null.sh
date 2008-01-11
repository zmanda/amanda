#!@SHELL@ 
#
# Exit Status:
# 0 Alles Ok
# 1 Illegal Request
# 2 Fatal Error
#

# try to hit all the possibilities here
prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

debugdir=@AMANDA_DBGDIR@

# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

if [ -d "$debugdir" ]
then
	logfile=$debugdir/changer.debug
else
	logfile=/dev/null
fi
exec 2> $logfile
set -x

USE_VERSION_SUFFIXES="@USE_VERSION_SUFFIXES@"
if test "$USE_VERSION_SUFFIXES" = "yes"; then
	SUF="-@VERSION@"
else
	SUF=
fi

myname=$0

EGREP='@EGREP@'

firstslot=1
totalslots=200

changerfile=`amgetconf$SUF changerfile`

tapedev="null:/dev/xxx$$"

cleanfile=$changerfile-clean
accessfile=$changerfile-access
slotfile=$changerfile-slot
[ ! -f $cleanfile ] && echo 0 > $cleanfile
[ ! -f $accessfile ] && echo 0 > $accessfile
[ ! -f $slotfile ] && echo $firstslot > $slotfile
cleancount=`cat $cleanfile`
accesscount=`cat $accessfile`
slot=`cat $slotfile`

rc=0

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

    if [ 0 -gt $newslot ]
    then
	newslot=`expr $totalslots - 1`
    fi

    if [ $totalslots -le  $newslot ]
    then
	newslot=0
    fi
    echo $newslot > $changerfile-slot
    slot=$newslot
    echo $slot $tapedev
    ;;

x-info)
    echo $slot $totalslots 1
    ;;

x-eject)
    echo $slot $tapedev
    ;;
esac

exit $rc
