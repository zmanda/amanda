#!@SHELL@ 
#
# Exit Status:
# 0 Alles Ok
# 1 Illegal Request
# 2 Fatal Error
#

prefix=@prefix@
exec_prefix=@exec_prefix@
amlibexecdir=@amlibexecdir@
. ${amlibexecdir}/chg-lib.sh

#
#	Changer config file (changerfile)
#
#	resend_mail=900		# 15 minutes
#	timeout_mail=604800	# 7 days
#	request="tty"		# Use the tty to ask the user to change tape.
#				# Can't be use by cron
#	request="email"		# Send an email to ask the user to change tape.
#	request="tty_email"	# Use the tty if it exist or send an email.
#			#Default is "tty_email"
#       mtx_binary="/path/to/mtx" # path of 'mtx'; default is value discovered by
#                               # configure
#
#

if [ -d "@AMANDA_DBGDIR@" ]; then
	logfile=@AMANDA_DBGDIR@/changer.debug
else
	logfile=/dev/null
fi

myname=`basename $0`

EGREP='@EGREP@'

if ! error=`try_find_mt`; then
    echo <none> $error
    exit 2
fi

ONLINEREGEX="ONLINE|READY|sense[_ ]key[(]0x0[)]|sense key error = 0|^er=0$|, mt_erreg: 0x0|^Current Driver State: at rest$"
REPORTTO=`amgetconf mailto`
MAILER=`amgetconf mailer`
tape=`amgetconf tapedev`

if [ -z "$tape" ]; then
  echo `_ '<none> tapedev not specified in amanda.conf.'`
  exit 2
fi

ORG=`amgetconf ORG`

firstslot=1
lastslot=99
resend_mail=900		# 15 minutes
timeout_mail=604800 	# 7 days
abort_file="chg-manual.abort"
abort_dir=`pwd`

changerfile=`amgetconf changerfile`

conf_match=`expr "$changerfile" : .\*\.conf\$`
if [ $conf_match -ge 6 ]; then
        configfile=$changerfile
        changerfile=`echo $changerfile | sed 's/.conf$//g'`
else
        configfile=$changerfile.conf
fi

cleanfile=$changerfile-clean
accessfile=$changerfile-access
slotfile=$changerfile-slot
[ ! -f $cleanfile ] && echo 0 > $cleanfile
[ ! -f $accessfile ] && echo 0 > $accessfile
[ ! -f $slotfile ] && echo $firstslot > $slotfile
cleancount=`cat $cleanfile`
accesscount=`cat $accessfile`
slot=`cat $slotfile`

# define these functions early so that they can be overridden in changerfile.conf

request_tty() {
	if > /dev/tty; then
		echo "$amdevcheck_message" >> /dev/tty
		# message parsed by ZMC:
		echo `_ 'Insert tape into slot %s and press return' "$1"` > /dev/tty
		echo `_ ' or type "NONE" to abort'` > /dev/tty
		read ANSWER < /dev/tty
		if [ X"$ANSWER" = X"NONE" ]; then
			echo `_ 'Aborting by user request'` > /dev/tty
			answer=`_ '<none> Aborting by user request'`
			echo `_ 'Exit ->'` $answer >> $logfile
			echo $answer
			exit 2
		fi
	else
		answer=`_ '<none> no /dev/tty to ask to change tape'`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	fi
}

###
# If $changerfile exists, source it into this script.  One reason is to
# override the request() function above which gets called to request
# that a tape be mounted.  Here is an alternate versions of request()
# that does things more asynchronous:
#
request_email() {
	# Send E-mail about the mount request and wait for the drive
	# to go ready by checking the status once a minute.  Repeat
	# the E-mail once an hour in case it gets lost.
	timeout=0
	gtimeout=$timeout_mail
	rm -f $abort_filename
	while true;do
	    if [ $gtimeout -le 0 ]; then
		answer=`_ '%s %s: timeout waiting for tape online' "$load" "$myname"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2;
	    fi
	    if [ -f $abort_filename ]; then
		rm -f $abort_filename
		answer=`_ '<none> Aborting by user request'`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 2
	    fi
	    if [ $timeout -le 0 ]; then
		msg=`_ '%s\nInsert Amanda tape into slot %s (%s)\nor \`touch %s\` to abort.' "$amdevcheck_message" "$1" "$tape" "$abort_filename"` 
		subject=`_ '%s AMANDA TAPE MOUNT REQUEST FOR SLOT %s' "$ORG" "$1"`
		echo "$msg" | $MAILER -s "$subject" $REPORTTO
		timeout=$resend_mail
	    fi
            echo `_ '     -> status %s' "$tape"` >> $logfile
            if amdevcheck_status $tape; then
		break
	    fi
	    sleep 60
	    timeout=`expr $timeout - 60`
	    gtimeout=`expr $gtimeout - 60`
	done
}

request_tty_email() {
	if > /dev/tty; then
		request_tty "$1"
	else
		request_email "$1"
	fi
}

request() {
	if [ X"$request" = X"tty" ]; then
		request_tty "$1"
	else if [ X"$request" = X"email" ]; then
		request_email "$1"
	else
		request_tty_email "$1"
	fi
	fi
}

# source the changer configuration file (see description, top of file)
if [ -f $configfile ]; then
	. $configfile
fi

# adjust MTX, if necessary
test -n "${mtx_binary}" && MTX="${mtx_binary}"

# check that MAILER is defined
if test -z "$MAILER"; then
    if test x"$request" = x"email" || test x"$request" = x"tty-email"; then
	answer=`_ "<none> %s: Can't send email because MAILER is not defined" "$myname"`
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 2
    fi
fi

#

eject() { 
	echo `_ '     -> status %s' "$tape"` >> $logfile
        if amdevcheck_status $tape; then
	    echo `_ '     -> offline %s' "$tape"` >> $logfile
            try_eject_device $tape
	    answer="$slot $tape"
	    code=0
	else
	    answer=`_ '<none> %s: %s' "$myname" "$amdevcheck_message"`
	    code=1
	fi
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit $code
}

abort_filename="$abort_dir/$abort_file"

#

reset() {
	echo `_ '     -> status %s' "$tape"` >> $logfile
        if amdevcheck_status $tape; then
		answer="$slot $tape"
	else
		answer="0 $tape $amdevcheck_message"
	fi
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 0
}

# load #

loadslot() {
	echo `_ '     -> status %s' "$tape"` >> $logfile
        # amdevcheck returns zero if the tape exists.
        amdevcheck_status $tape;
        tape_status=$?

	whichslot=$1
	case $whichslot in
	current)
		load=$slot
		;;
	next|advance)
		load=`expr $slot + 1`
		[ $load -gt $lastslot ] && load=$firstslot
		;;
	prev)
		load=`expr $slot - 1`
		[ $load -lt $firstslot ] && load=$lastslot
		;;
	first)
		load=$firstslot
		;;
	last)
		load=$lastslot
		;;
	[0-9]|[0-9][0-9])
		if [ $1 -lt $firstslot -o $1 -gt $lastslot ]; then
			answer=`_ '<none> %s: slot must be %s .. %s' "$myname" "firstslot" "$lastslot"`
			echo `_ 'Exit ->'` $answer >> $logfile
			echo $answer
			exit 1
		fi
		load=$1
		;;
	*)
		answer=`_ '<none> %s: illegal slot: %s' "$myname" "$1"`
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 1
		;;
	esac
	#
	if [ $tape_status -eq 0 -a $load = $slot ];then
		# already loaded
		answer="$slot $tape"
		echo `_ 'Exit ->'` $answer >> $logfile
		echo $answer
		exit 0
	fi

	if [ X"$whichslot" = X"current" ]; then
		answer="<none> Current slot not loaded"
		echo `_ 'Exit ->'` $answer>> $logfile
		echo $answer
		exit 1
	fi

	expr $accesscount + 1 > $accessfile

	if [ $tape_status -eq 0 ]; then
		echo `_ "     -> offline %s" "$tape"` >> $logfile
                try_eject_device $tape
		tape_status=1
	fi
	if [ $whichslot = advance ]; then
		tape=/dev/null
	else
		echo `_ '     -> load   %s' "$load"` >> $logfile
		while true; do
			request $load
			echo `_ '     -> status %s' "$tape"` >> $logfile
                        if amdevcheck_status $tape; then
                            break;
                        fi
		done
	fi
	echo $load > $slotfile
	answer="$load $tape"
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 0
}

#

info() {
	echo `_ '     -> status %s' "$tape"` >> $logfile
        if amdevcheck_status $tape; then
		answer="$slot $lastslot 1"
	else
		answer="0 $lastslot 1"
	fi
	echo `_ 'Exit ->'` $answer >> $logfile
	echo $answer
	exit 0
}

#
# main part
#

echo `gettext "args ->"` "$@" >> $logfile
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
		fmt=`gettext "<none> %s: Unknown option %s\n"`
		printf $fmt $myname $1
		exit 2
		;;
	esac
done

exit 0
