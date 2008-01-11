#!@SHELL@
#

echo "amverifyrun is deprecated -- use amcheckdump" >& 2

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

USE_VERSION_SUFFIXES="@USE_VERSION_SUFFIXES@"
if [ "$USE_VERSION_SUFFIXES" = "yes" ]; then
        SUF="-@VERSION@"
else
        SUF=
fi

getparm() {
        $AMGETCONF $CONFIG $1 2>/dev/null
}

CONFIG=$1
amlibexecdir=$amlibexecdir  
sbindir=$sbindir
AMGETCONF=$sbindir/amgetconf$SUF
AMVERIFY=$sbindir/amverify$SUF
LOGDIR=`getparm logdir`
AMDUMPLOG=${LOGDIR}/amdump.1
AMFLUSHLOG=${LOGDIR}/amflush.1
if [ -f $AMDUMPLOG ]; then
  if [ -f $AMFLUSHLOG ]; then
    if [ $AMDUMPLOG -nt $AMFLUSHLOG ]; then
      AMLOG=$AMDUMPLOG
    else
      AMLOG=$AMFLUSHLOG
    fi
  else
    AMLOG=$AMDUMPLOG
  fi
else
  if [ -f $AMFLUSHLOG ]; then
    AMLOG=$AMFLUSHLOG
  else
    echo `_ 'Nothing to verify'`
    exit 1;
  fi
fi


FIRST_SLOT=`grep "taper: slot" $AMLOG | fgrep 'exact label match
new tape
first labelstr match' | sed 1q | sed 's/://g' | awk '{print $3}'`
if [ X"$FIRST_SLOT" = X"" ]; then
  FIRST_SLOT=`grep "taper: slot: .* wrote label" $AMLOG | sed 1q | sed 's/://g' | awk '{print $3}'`
  if [ X"$FIRST_SLOT" = X"" ]; then
    FIRST_SLOT='-1'
  fi
fi

NBTAPES=`grep -c "taper: .*wrote label " $AMLOG`

if [ X"$NBTAPES" != X"0" ]; then
  if ln -s $AMLOG $LOGDIR/log ; then
    $AMVERIFY $CONFIG $FIRST_SLOT $NBTAPES
    if [ -L $LOGDIR/log ] ; then rm $LOGDIR/log ; fi
  else
    echo "amdump or amflush is already running, or you must run amcleanup"
  fi
else
  echo `_ 'Nothing to verify'`
fi
