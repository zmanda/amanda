#!@SHELL@ 
#
# Author: Robert Dege
#
#
# version 1.2
# -----------
# fixed last_cleaned file so that if it doesn't exist, it gets created with current date, not '0,0'
# fixed a bug that was reporting the wrong slot # to amcheck
#
# version 1.1
# -----------
# amverify was failing when using -slot current.  Fixed exit $code from 1 -> 0.
# removed useless $current variables from movetape() function.
#
#
#
# Exit Status:
# 0 Alles Ok
# 1 Illegal Request
# 2 Fatal Error
#


#
# Set Path so that it includes Amanda binaries, and access to tapechanger & drive programs
#
prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH


#
# Load configuration data from the config file
#

ourconf=`amgetconf changerfile`
myname=$0


if [ ! -f "$ourconf" ]; then
        code=2
        echo `_ 'Command Line ->'` $myname $@
	echo `_ 'Exit(%s): %s not found as listed in amanda.conf' "$code" "$ourconf"` 1>&2
        exit $code
fi


# grab mcutil info
tmpval1=`grep ^mcutil $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^mcutil $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
	echo `_ 'Exit(%s): mcutil not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	MCUTIL=$tmpval2
else
	MCUTIL=$tmpval1
fi


# grab tape info
tmpval1=`grep ^tape $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^tape $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
	echo `_ 'Exit(%s): tape not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	tape=$tmpval2
else
	tape=$tmpval1
fi


# grab firstslot info
tmpval1=`grep ^firstslot $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^firstslot $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
        code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): firstslot not specified in %s' "$code" "$ourconf"` 1>&2
        exit $code
elif [ -z "$tmpval1" ]; then
        firstslot=$tmpval2
else
        firstslot=$tmpval1
fi


# grab lastslot info
tmpval1=`grep ^lastslot $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^lastslot $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
        code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): lastslot not specified in %s' "$code" "$ourconf"` 1>&2
elif [ -z "$tmpval1" ]; then
        lastslot=$tmpval2
else
        lastslot=$tmpval1
fi


# grab use_cleaning info
tmpval1=`grep ^use_cleaning $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^use_cleaning $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
        code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): use_cleaning not specified in %s' "$code" "$ourconf"` 1>&2
        exit $code
elif [ -z "$tmpval1" ]; then
        use_cleaning=$tmpval2
else
        use_cleaning=$tmpval1
fi


# grab cleanslot info
tmpval1=`grep ^cleanslot $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^cleanslot $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
        code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): cleanslot not specified in %s' "$code" "$ourconf"` 1>&2
        exit $code
elif [ -z "$tmpval1" ]; then
        cleanslot=$tmpval2
else
        cleanslot=$tmpval1
fi


# grab cleansleep info
tmpval1=`grep ^cleansleep $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^cleansleep $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): cleansleep not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	cleansleep=$tmpval2
else
	cleansleep=$tmpval1
fi


# grab cleanme info
tmpval1=`grep ^cleanme $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^cleanme $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): cleanme not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	cleanme=$tmpval2
else
	cleanme=$tmpval1
fi


# grab cleanfile info
tmpval1=`grep ^cleanfile $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^cleanfile $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
        code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): cleanfile not specified in %s' "$code" "$ourconf"` 1>&2
        exit $code
elif [ -z "$tmpval1" ]; then
        cleanfile=$tmpval2
else
        cleanfile=$tmpval1
fi


# grab lastfile info
tmpval1=`grep ^lastfile $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^lastfile $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): lastfile not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	lastfile=$tmpval2
else
	lastfile=$tmpval1
fi


# grab currentslot info
tmpval1=`grep ^currentslot $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^currentslot $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): currentslot not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	currentslot=$tmpval2
else
	currentslot=$tmpval1
fi


# grab logfile info
tmpval1=`grep ^logfile $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^logfile $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): logfile not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	logfile=$tmpval2
else
	logfile=$tmpval1
fi

[ ! -w $logfile ] && logfile=/dev/null


# grab slot0source info
tmpval1=`grep ^slot0source $ourconf | awk -F\  '{print $2}'`
tmpval2=`grep ^slot0source $ourconf | awk -F= '{print $2}'`
if [ -z "$tmpval1" ] && [ -z "$tmpval2" ]; then
	code=2
        echo `_ 'Command Line ->'` $myname $@
        echo `_ 'Exit(%s): slot0source not specified in %s' "$code" "$ourconf"` 1>&2
	exit $code
elif [ -z "$tmpval1" ]; then
	slot0source=$tmpval2
else
	slot0source=$tmpval1
fi



#
# Verify currentslot contains a value
#
if [ ! -f $currentslot ] || [ `cat $currentslot` -lt $firstslot ];then
   readstatus
   echo $used > $currentslot
fi

current=`cat $currentslot`


# Start logging to $logfile
echo "\n\n==== `date` ====" >> $logfile
echo `_ 'Command Line ->'` $myname $@ >> $logfile


#
# is Use Cleaning activated?
#
if [ $use_cleaning -eq 1 ]; then
   curday=`date +%j`
   curyear=`date +%Y`

   [ ! -f $cleanfile ] && echo 0 > $cleanfile
   [ ! -f $lastfile ] && echo $curday,$curyear > $lastfile


#
# Check to see when tape drive was last cleaned
# output warning message if it's been too long
# Currently, if it's been more than 45days, then
# an error message is displayed everytime the
# script is called, until the clean parameter
# is run
#
   cleaned=`cat $cleanfile`
   lastcleaned=`cut -d, -f1 $lastfile`
   yearcleaned=`cut -d, -f2 $lastfile`

  if [ `expr $curday - $lastcleaned`  -lt 0 ];then
     diffday=`expr $curday - $lastcleaned + 365`
     diffyear=`expr $curyear - $yearcleaned - 1`
  else
     diffday=`expr $curday - $lastcleaned`
     diffyear=`expr $curyear - $yearcleaned`
  fi

  if [ $diffday -gt $cleanme ] || [ $diffyear -ge 1 ];then
     if [ $diffyear -ge 1 ];then
	  echo `_ "Warning, it's been %s year(s) & %s day(s) since you last cleaned the tape drive!" "$diffyear" "$diffday"`
     else
	  echo `_ "Warning, it's been %s day(s) since you last cleaned the tape drive!" "$diffday"`
     fi
  fi

fi


#
# Read if there is a tape in the tape drive
# If so, determine what slot is the tape from
#
readstatus() {
  echo `_ "querying tape drive....."` >> $logfile
  used=`expr \`$MCUTIL -e drive | tr = \] | cut -d\] -f2\` - $slot0source`
  echo `_ " Done"` >> $logfile

  # Give changer a chance to reset itself
  sleep 3
}


#
# If tape is in the drive, eject it
#
eject() {
  echo `_ "tape drive eject was called"` >> $logfile

  readstatus 

  if [ $used -ge $firstslot ];then
    $MCUTIL -m drive slot:$used
    code=$?
  else
    code=1
  fi

  if [ $code -eq 0 ];then
    answer=`_ 'Cartridge %s successfully ejected from %s' "$used" "$tape"`
    echo `_ "Exit(%s): %s" "$code" "$answer"` >> $logfile
    echo $current $answer	#For amtape output  
    return $code
  elif [ $code -eq 1 ];then
    answer=`_ "No Cartridge in Tape Drive"`
    echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
    echo $current $answer	#For amtape output
    exit $code
  else
    answer=`_ 'Tape abnormally failed'`
    echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
    echo $current $answer	#For amtape output
    exit $code
  fi
}


#
# reset tape drive to a current state.
# This involves ejecting the current tape (if occupied)
# and inserting the tape in $firstslot
#
reset() {
  echo `_ 'tape drive reset was called'` >> $logfile

  readstatus

  if [ $used -ge $firstslot ];then
     eject
  fi

  res=`$MCUTIL -m slot:$firstslot drive`
  code=$?


  if [ $code -eq 0 ];then
    echo $firstslot > $currentslot
    answer=`_ '%s - Tape drive was successfully reset' "$firstslot"`
  elif [ $code -eq 1 ];then
    answer=`_ '%s - Tape drive reset failed\nCommand -> %s' "$firstslot" "$res"`
  else
    code=2
    answer=`_ '%s - Tape abnormally failed -> %s' "$firstslot" "$res"`
  fi

  echo `_ 'Exit(%s): slot %s' "$code" "$answer"` >> $logfile
  echo $firstslot	#For amtape output 
  exit $code
}




#
# Load a specific cartridge into the changer
#
loadslot() {
  echo `_ "loadslot was called"` >> $logfile

  readstatus

  whichslot=$1

  case $whichslot in
    current)
	if [ $current -ge $firstslot ];then
	   load=$current
	else
	   load=$used
	fi

	movetape
	;;
    next|advance)
	  [ $used -lt $firstslot ] && used=$current

	  load=`expr $used + 1`
	  [ $load -gt $lastslot ] && load=$firstslot

	  if [ $whichslot = advance ];then
	     echo $load > $currentslot
	     code=0
	     answer=`_ 'advancing to slot %s' "$load"`
	     echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
	     echo $load $code
	     exit $code
	  else
	     movetape
	  fi
	  ;;
    prev)
	  [ $used -lt $firstslot ] && used=$current

	  load=`expr $used - 1`
	  [ $load -lt $firstslot ] && load=$lastslot
	  movetape
	  ;;
    first)
	  load=$firstslot
	  movetape
	  ;;
    last)
	  load=$lastslot
	  movetape
	  ;;
    [$firstslot-$lastslot])
	     load=$1
	     movetape
	  ;;
    clean)
	  if [ use_cleaning -eq 1 ];then
	     current=$cleanslot
	     eject
	     $MCUTIL slot:$cleanslot drive
	     sleep $cleansleep
	     echo "$curday,$curyear" > $lastfile
	     echo `expr $cleaned + 1` > $cleanfile
	     reset
	  else
	     code=1
	     answer=`_ "Cleaning not enabled in config"`
	     echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
	     echo $cleanslot $answer
	     exit $code
	  fi
	  ;;
    *)
       code=1
       answer=`_ '"%s" invalid menu option' "$whichslot"`
       echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
       echo "$answer"
       exit $code
       ;;
    esac
}


#
# sub-function that slot calls to actually eject the tape
# & load in the correct slot cartridge
#
movetape() {

    # If the requested slot is already loaded in the tape drive
    if [ $load -eq $used ]; then
        code=0
	answer=`_ 'slot %s is already loaded' "$load"`
        echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
        echo $load $tape	# For amtape output
        exit $code
    elif [ $used -ge $firstslot ];then
	current=$load
	eject
    else
	echo $load $tape 	# For amtape output
    fi

    echo `_ 'Loading slot %s into Tape drive' "$load"` >> $logfile
    $MCUTIL -m  slot:$load drive
    code=$?

    if [ $code -eq 0 ];then
	echo $load > $currentslot
	answer=`_ 'Cartridge %s successfully loaded in Tape drive' "$load"`
    else
	answer=`_ 'Cartridge %s failed to load in Tape drive' "$load"`
    fi
    echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
    exit $code
}


info() {
  echo `_ 'tape drive info was called'` >> $logfile

  readstatus

  if [ $used -lt 0 ];then
    used=0
  fi

  code=0
  answer="$used $lastslot 1"
  echo `_ 'Exit(%s): %s' "$code" "$answer"` >> $logfile
  echo "$answer"
  exit $code
}


  case $1 in
    -slot)
	   shift
	   loadslot $*
	   ;;
    -device)
	   echo $tape
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
    --help|-help)
	    echo `_ '-slot {current|next|previous|first|last|%s-%s|clean}' "$firstslot" "$lastslot"`
	    echo `_ '	current  - show contents of current slot'`
	    echo `_ '	next     - load tape from next slot'`
	    echo `_ '	previous - load tape from previous slot'`
	    echo `_ '	first	 - load tape from first slot'`
	    echo `_ '	last	 - load tape from last slot'`
	    echo `_ '	%s - %s	 - load tape from slot <slot #>' "$firstslot" "$lastslot"`
	    echo `_ '	clean	 - Clean the drive'`
	    echo `_ '-device   : Show current tape device'`
	    echo `_ '-reset    : Reset changer to known state'`
	    echo `_ '-eject    : Eject current tape from drive'`
	    echo `_ '-info     : Output {current slot | # of slots | can changer go backwards}'`
	    echo `_ '-help     : Display this help'`
	    ;;
    *)
       echo `_ "<usage> %s -{slot|device|reset|eject|help}" "$myname"`
       ;;
 esac
