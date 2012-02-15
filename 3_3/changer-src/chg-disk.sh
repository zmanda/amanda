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
# Author: Jean-Christian SIMONETTI, System and Network Engineer
#				    Wanadoo Portails
#			 	    Sophia Antipolis, France
#
#	This changer script is based on a directory structure like:
#	slot_root_dir -|
#	               |- info
#	               |- data -> slot1
#	               |- slot1
#	               |- slot2
#	               |- ...
#	               |- slotn
#	where 'slot_root_dir' is the tapedev 'file:xxx' parameter and 'n'
#	is the LASTSLOT value of your changerfile config file. If LASTSLOT is
#	not defined, the value of the tapecycle parameter is used.
#
#	To use this driver, just put the line 'tpchanger "chg-disk"' in your
#	amanda.conf.
#
#	Example of use (amanda.conf):
#	--- cut here ---
#	tapedev  "file:/BACKUP2/slots/"
#	changerdev "/dev/null"
#	changerfile "chg-disk"
#	tpchanger "chg-disk"
#	changerfile "/usr/local/amanda/etc/changer"
#	tapetype HARD-DISK
#	define tapetype HARD-DISK {
#	    length 12000 mbytes
#	}
#	--- cut here ---
#
#	Example changerfile (chg-disk.conf):
#	--- cut here ---
#	LASTSLOT=12
#	--- cut here ---
#
#	The number of slot is equal to your LASTSLOT or tapecycle.
#	You must create the slots and data directory.
#


prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"
 
# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

MYNAME=$0

TAPE=`amgetconf tapedev`
if test X"$TAPE" = X""; then
    echo `_ '<none> tapedev not specified in amanda.conf.'`
    exit 2
fi
SLOTDIR=`echo $TAPE | sed 's/^file://'`

isinteger() {
	    # should be exactly one arg
	[ $# = 1 ]  || return 1
	    # if arg is null, no expr needed
	[ "${1}" = '' ] && return 1
	    # expr will return 0 on match
	expr "$1" : '[0-9][0-9]*$' > /dev/null 2>&1
}

# Need rwx access to the virtual tape itself.
if ! test -d $SLOTDIR; then
    echo `_ '<none> Virtual-tape directory %s does not exist.' "$SLOTDIR"`
    exit 2
fi
if ! test -w $SLOTDIR; then
    echo `_ '<none> Virtual-tape directory %s is not writable.' "$SLOTDIR"`
    exit 2
fi


# need rwx access to directory of changer file
CHANGERFILE=`amgetconf changerfile`
conf_match=`expr "$CHANGERFILE" : .\*\.conf\$`
if [ $conf_match -ge 6 ]; then
        CONFIGFILE=$CHANGERFILE
        CHANGERFILE=`echo $CHANGERFILE | sed 's/.conf$//g'`
else
        CONFIGFILE=$CHANGERFILE.conf
fi

CFDir=`dirname ${CHANGERFILE}`
[ -d ${CFDir} -a -r ${CFDir} -a -w ${CFDir} -a -x ${CFDir} ] ||
	{ echo `_ "<none> %s: need 'rwx' access to '%s'" "$MYNAME" "$CFDir"` ; exit 2 ; }

# check or create changer metadata files
ACCESSFILE=$CHANGERFILE-access
[ -f $ACCESSFILE -a -r $ACCESSFILE -a -w $ACCESSFILE ] ||
	echo 0 > $ACCESSFILE ||
	{ echo `_ "<none> %s: could not access or create '%s'" "$MYNAME" "$ACCESSFILE"` ; exit 2; }
CLEANFILE=$CHANGERFILE-clean
[ -f $CLEANFILE -a -r $CLEANFILE -a -w $CLEANFILE ] ||
	echo 0 > $CLEANFILE ||
	{ echo `_ "<none> %s: could not access or create '%s'" "$MYNAME" "$CLEANFILE"` ; exit 2 ; }
SLOTFILE=$CHANGERFILE-slot
[ -f $SLOTFILE -a -r $SLOTFILE -a -w $SLOTFILE ] ||
	echo 0 > $SLOTFILE ||
	{ echo `_ "<none> %s: could not access or create '%s'" "$MYNAME" "$SLOTFILE"` ; exit 2; }

# read and check metadata
ACCESSCOUNT=`cat $ACCESSFILE`
isinteger $ACCESSCOUNT || { ACCESSCOUNT=0 ; echo 0 > $ACCESSFILE ; }
CLEANCOUNT=`cat $CLEANFILE`
isinteger $CLEANCOUNT || { CLEANCOUNT=0 ; echo 0 > $CLEANFILE ; }

FIRSTSLOT=1
LASTSLOT=`amgetconf tapecycle`
if test -r $CONFIGFILE; then
    . $CONFIGFILE
fi
CURSLOT=0
CLEANSLOT=$LASTSLOT
NSLOT=`expr $LASTSLOT - $FIRSTSLOT + 1`

load() {
  WHICHSLOT=$1;
  # unload should have been called, but just in case ...
  [ -h $SLOTDIR/data ] && unload
  ln -s $SLOTDIR/slot$WHICHSLOT $SLOTDIR/data
  echo $WHICHSLOT > $SLOTFILE
}

unload() {
  rm -f $SLOTDIR/data
  echo "0" > $SLOTFILE
}

readstatus() {
  CURSLOT=`cat $SLOTFILE`
}

loadslot() {
  WHICHSLOT=$1

  TYPE=string	# default if not numeric
  isinteger $WHICHSLOT && TYPE=digit

  readstatus
  NEWSLOT=0
  if [ $WHICHSLOT = "current" ]; then
    if [ $CURSLOT -le 0 ]; then
      load $FIRSTSLOT
      echo "$FIRSTSLOT $TAPE"
      exit 0
    else
      echo "$CURSLOT $TAPE"
      exit 0
    fi
  elif [ $WHICHSLOT = "next" -o $WHICHSLOT = "advance" ]; then
    NEWSLOT=`expr $CURSLOT + 1`
    [ $NEWSLOT -gt $LASTSLOT ] && NEWSLOT=$FIRSTSLOT
  elif [ $WHICHSLOT = "prev" ]; then
      NEWSLOT=`expr $CURSLOT - 1`
      [ $NEWSLOT -lt $FIRSTSLOT ] && NEWSLOT=$LASTSLOT
  elif [ $WHICHSLOT = "first" ]; then
      NEWSLOT=$FIRSTSLOT
  elif [ $WHICHSLOT = "last" ]; then
      NEWSLOT=$LASTSLOT
  elif [ $TYPE = "digit" ]; then
    if [ $WHICHSLOT -ge $FIRSTSLOT -a $WHICHSLOT -le $LASTSLOT ]; then
      NEWSLOT=$WHICHSLOT
    else
      echo `_ '%s illegal slot' "$WHICHSLOT"`
      exit 1
    fi
  elif [ $WHICHSLOT = "clean" ]; then
    NEWSLOT=$CLEANSLOT
  else
    echo `_ '%s illegal request' "$WHICHSLOT"`
    exit 1
  fi
  if [ $NEWSLOT = $CURSLOT ]; then
    echo "$CURSLOT $TAPE"
    exit 0
  fi
  if [ $NEWSLOT = $CLEANSLOT ]; then
    expr ${CLEANCOUNT:=0} + 1 > $CLEANFILE
    echo 0 > $ACCESSFILE
  else
    expr ${ACCESSCOUNT:=0} + 1 > $ACCESSFILE
    if [ $ACCESSCOUNT -gt $LASTSLOT ]; then
      $MYNAME -slot clean >/dev/null
    fi
  fi

  readstatus
  if [ $CURSLOT -ne 0 ]; then
    unload
  fi

  if [ $WHICHSLOT = "advance" ]; then
    echo "$NEWSLOT /dev/null"
    exit 0
  fi
  load $NEWSLOT
  echo "$NEWSLOT $TAPE"
  exit 0
}

info() {
  readstatus
  echo "$CURSLOT $NSLOT 1"
  exit 0
}

reset() {
  readstatus
  [ $CURSLOT -gt 0 ] && unload
  load $FIRSTSLOT
  echo "$FIRSTSLOT $tape"
  exit 0
}

eject() {
  readstatus
  if [ $CURSLOT -le 0 ]; then
    echo `_ '0 Drive was not loaded'`
    exit 1
  else
    unload
    echo $CURSLOT
    exit 0
  fi
}


while [ $# -ge 1 ];do
  case $1 in
    -slot)
           shift
           loadslot $*
           ;;
    -clean)
           shift
           loadslot clean
           ;;
    -current)
           shift
           loadslot current
           ;;
    -next)
           shift
           loadslot next
           ;;
    -advance)
           shift
           loadslot advance
           ;;
    -prev)
           shift
           loadslot prev
           ;;
    -first)
           shift
           loadslot first
           ;;
    -last)
           shift
           loadslot last
           ;;
    -info)
           shift
           info
           ;;
    -reset)
            shift
            reset
            ;;
    -eject)
            shift
            eject
            ;;
    *)
       echo `_ '<none> Unknown option %s' "$1"`
       exit 2
       ;;
  esac
done

