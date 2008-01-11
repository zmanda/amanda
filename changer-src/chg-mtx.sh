#!@SHELL@ 
#
# Exit Status:
# 0 Alles Ok
# 1 Illegal Request
# 2 Fatal Error
#

# source utility functions and values from configure
prefix=@prefix@
exec_prefix=@exec_prefix@
amlibexecdir=@amlibexecdir@
. ${amlibexecdir}/chg-lib.sh

if [ -d "@AMANDA_DBGDIR@" ]; then
	logfile=@AMANDA_DBGDIR@/changer.debug
else
	logfile=/dev/null
fi

myname=$0

tape=`amgetconf tapedev`
if [ -z "$tape" ]; then
  echo "<none> tapedev not specified in amanda.conf";
  exit 2;
fi

TAPE=`amgetconf changerdev`; export TAPE # for mtx command
if [ -z "$TAPE" ]; then
  echo "<none> changerdev not specified in amanda.conf";
  exit 2;
fi

if [ "$tape" = "/dev/null" -o "$TAPE" = "/dev/null" ]; then
  echo "<none> Both tapedev and changerdev must be specified in config file";
  exit 2;
fi

firstslot=1
lastslot=5
# counted from 1 !!!
cleanslot=6

changerfile=`amgetconf changerfile`

cleanfile=$changerfile-clean
accessfile=$changerfile-access
[ ! -f $cleanfile ] && echo 0 > $cleanfile
[ ! -f $accessfile ] && echo 0 > $accessfile
cleancount=`cat $cleanfile`
accesscount=`cat $accessfile`
#

readstatus() {
  used=`$MTX -s |
    sed -n 's/Drive: No tape Loaded/-1/p;s/Drive: tape \(.\) loaded/\1/p'`

  if [ -z "$used" ]; then
    used="-1";
  fi
}


eject() {
  readstatus 
  if [ $used -gt 0 ];then
    $MTX -u $used
    answer="0 $tape"
    echo `_ 'Exit ->'` $answer >> $logfile
    echo $answer
    exit 0
  else
    answer=`_ '<none> %s: Drive was not loaded' "$myname"`	
    echo `_ 'Exit ->'` $answer >> $logfile
    echo $answer
    exit 1
  fi
}

reset() {
  readstatus
  if [ $used -gt 0 ];then
    $MTX -u $used
  fi
  res=`$MTX -l 1`
  if [ $? -eq 0 ];then
    answer="1 $tape"
    echo `_ 'Exit ->'` $answer >> $logfile
    echo $answer
    exit 0
  else
    answer="1 $res"
    echo `_ 'Exit ->'` $answer >> $logfile
    echo $answer
    exit 1
  fi
}
#
#
loadslot() {
  readstatus
  echo "     -> loaded $used" >> $logfile
  whichslot=$1
  case $whichslot in
    current)
	     if [ $used -lt 0 ];then
	       $MTX -l 1
	       used=1
	     fi
	     answer="$used $tape"
	     echo `_ 'Exit ->'` $answer >> $logfile
	     echo $answer
	     exit 0
	     ;;
    next|advance)
	  load=`expr $used + 1`
	  [ $load -gt $lastslot ] && load=$firstslot
	  ;;
    prev)
	  load=`expr $used - 1`
	  [ $load -lt $firstslot ] && load=$lastslot
	  ;;
    first)
	  load=$firstslot
	  ;;
    last)
	  load=$lastslot
	  ;;
    [$firstslot-$lastslot])
	  load=$1
	  ;;
    clean)
	  load=$cleanslot
	  ;;
    *)
       answer=`_ '<none> %s: illegal request: "%s"' "$myname" "$whichslot"`
       echo `_ 'Exit ->'` $answer >> $logfile
       echo $answer
       exit 1
       ;;
    esac

    if [ $load = $used ]; then
        answer="$used $tape"
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 0
    fi

    if [ $load = $cleanslot ]; then
	expr $cleancount + 1 > $cleanfile
	echo 0 > $accessfile
    else
	expr $accesscount + 1 > $accessfile
	if [ $accesscount -gt 9 ]; then
		$myname -slot clean >/dev/null
	fi
    fi

    # Slot 6 might contain an ordinary tape rather than a cleaning
    # tape. A cleaning tape auto-ejects; an ordinary tape does not.
    # We therefore have to read the status again to check what
    # actually happened.
    readstatus
	

    if [ $used -gt 0 ];then
      echo "     -> unload $used" >> $logfile
      res=`$MTX -u $used`
      status=$?
      echo "     -> status $status" >> $logfile
      echo "     -> res    $res" >> $logfile
      if [ $status -ne 0 ];then
        answer=`_ '<none> %s: %s' "$myname" "$res"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
      fi
    fi
    if [ $whichslot = advance ];then
      answer="$load /dev/null"
      echo `_ 'Exit ->'` $answer >> $logfile
      echo $answer
      exit 0
    fi
    echo `_ '     -> load   %s' "$load"` >> $logfile
    res=`$MTX -l $load`
    status=$?
    echo `_ '     -> status %s' "$status"` >> $logfile
    echo `_ '     -> result %s' "$res"` >> $logfile
    if [ $status -eq 0 ];then
      amdevcheck_status $tape
      answer="$load $tape"
      code=0
    else
      answer="$load $res"
      code=2
    fi
    echo `_ 'Exit ->'` $answer >> $logfile
    echo $answer
    exit $code
}
#
info() {
  readstatus
  echo "     -> info   $used" >> $logfile
  if [ $used -lt 0 ];then
    used=0
  fi
  answer="$used $lastslot 1"
  echo `_ 'Exit ->'` $answer >> $logfile
  echo $answer
  exit 0
}
#
echo Args "->" "$@" >> $logfile
while [ $# -ge 1 ];do
  case $1 in
    -slot)
	   shift
	   loadslot $*
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
       answer=`_ '<none> %s: Unknown option %s' "$myname" "$1"`
       echo `_ 'Exit ->'` $answer >> $logfile
       echo $answer
       exit 2
       ;;
  esac
done
