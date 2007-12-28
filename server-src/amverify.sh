#! @SHELL@
#
#	$Id: amverify.sh.in,v 1.38 2006/07/25 19:00:56 martinea Exp $
#
# (C) 1996 by ICEM Systems GmbH
# Author: Axel Zinser (fifi@icem.de)
#
# amverify: check amanda tapes and report errors
#

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
libexecdir="@libexecdir@"
. "${libexecdir}/amanda-sh-lib.sh"

# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

USE_VERSION_SUFFIXES="@USE_VERSION_SUFFIXES@"
if [ "$USE_VERSION_SUFFIXES" = "yes" ]; then
	SUF="-@VERSION@"
else
	SUF=
fi

# If the shell/system echo support \r and \c, use them to write some
# status messages over the top of each other as things progress, otherwise
# use a normal echo and let them go on new lines.  Define $Echoe to be
# an echo that goes to stderr.  In the code, $Echoe is used and it may
# be either echoe or echone, the latter being code that knows about echon.

t=`echo "abc\r\c" | wc -c`
if [ $t -eq 4 ]; then
	Echon=echon
else
	Echon=echo
fi
Echoe=echoe
elen=0
echoe() {
	echo "$@" >&2
	Echoe=echoe
}
echon() {
        newelen=`expr "$1" : '.*'`
	blanks=
        while [ $newelen -lt $elen ]; do
		blanks="$blanks "
                elen=`expr $elen - 1`
        done
        echo "$1""$blanks\r\c"
        elen=$newelen
	Echoe=echone
}
echone() {
	echon
	echoe "$@"
	Echoe=echoe
}

report() {
	$Echoe "$@"
	echo "$@" >> $REPORT
}

getparm() {
	$AMGETCONF $CONFIG $1 2>/dev/null
}

sendreport() {
	if [ -f $REPORT -a X"$REPORTTO" != X"" ]; then
		(
		echo `_ 'Tapes: %s' "$TAPELIST"`
		if [ -s $DEFECTS ]; then
			echo `_ 'Errors found:'`
			cat $DEFECTS
		else
			echo `_ 'No errors found!'`
		fi
		echo

		[ -s $REPORT ] \
			&& cat $REPORT
		) | (
		    if test -n "$MAIL"; then
			$MAIL -s "$ORG AMANDA VERIFY REPORT FOR$TAPELIST" $REPORTTO
		    else
			cat >&2
		    fi
		)
	fi
}

###
# This function is called to process one dump image.  Standard input is
# the dump image.  We parse the header and decide if it is a GNU tar
# dump or a system dump.  Then we do a catalog operation to /dev/null
# and finally a "cat" to /dev/null to soak up whatever data is still in
# the pipeline.
#
# In the case of a system restore catalogue, this does not fully check
# the integrity of the dump image because system restore programs stop
# as soon as they are done with the directories, which are all at the
# beginning.  But the trailing cat will at least make sure the whole
# image is readable.
###

doonefile() {

	###
	# The goal here is to collect the first 32 KBytes and save the
	# first line.  But the pipe size coming in to us from amrestore
	# is highly system dependent and "dd" does not do reblocking.
	# So we pick a block size that is likely to always be available in
	# the pipe and a count to take it up to 32 KBytes.  Worst case,
	# this could be changed to "bs=1 count=32k".  We also have to
	# soak up the rest of the output after the "head" so an EPIPE
	# does not go back and terminate the "dd" early.
	###

        HEADER=`$DD bs=512 count=64 2>/dev/null | ( sed 1q ; cat > /dev/null )`
	CMD=
	result=1
	if [ X"$HEADER" = X"" ]; then
		echo `_ '** No header'` > $TEMP/errors
	else
		set X $HEADER
		# XXX meh, while[] is dangerous, what about a bad header?
		while [ X"$1" != X"program" ]; do shift; done
		if [ X"$1" = X"program" -a X"$2" != X"" ]; then
			if [ X"$TAR" != X"" \
			     -a \( X"`basename $2`" = X"`basename $TAR`" \
				   -o X"`basename $2`" = X"gtar" \
				   -o X"`basename $2`" = X"gnutar" \
				   -o X"`basename $2`" = X"tar" \) ]; then
				CMD=$TAR
				ARGS="tf -"
			elif [ X"$TAR" != X"" \
			       -a X"$SAMBA_CLIENT" != X"" \
			       -a X"$2" = X"$SAMBA_CLIENT" ]; then
				CMD=$TAR
				ARGS="tf -"
			elif [ X"$DUMP" != X"" -a X"$2" = X"$DUMP" ]; then
				CMD=$RESTORE
				if [ $IS_AIX -eq 1 ]; then
					ARGS=-tB
				else
					ARGS="tbf 2 -"
				fi
			elif [ X"$VDUMP" != X"" -a X"$2" = X"$VDUMP" ]; then
				CMD=$VRESTORE
				ARGS="tf -"
			elif [ X"$VXDUMP" != X"" -a X"$2" = X"$VXDUMP" ]; then
				CMD=$VXRESTORE
				ARGS="tbf 2 -"
			elif [ X"$XFSDUMP" != X"" -a X"$2" = X"$XFSDUMP" ]; then
				CMD=$XFSRESTORE
				ARGS="-t -v silent -"
			else
				echo `_ '** Cannot do %s dumps' "$2"` > $TEMP/errors
				result=999	# flag as not really an error
			fi
		else
			echo `_ '** Cannot find dump type'` > $TEMP/errors
		fi
	fi
	echo $CMD > $TEMP/onefile.cmd
	if [ X"`echo $HEADER | grep '^AMANDA: SPLIT_FILE'`" != X"" ]; then
	    result=500
	    set X $HEADER
	    shift 7
	    echo $1 | cut -f7 -d' ' > $TEMP/onefile.partnum
	elif [ X"$CMD" != X"" ]; then
		if [ -x $CMD ]; then
			$CMD $ARGS > /dev/null 2> $TEMP/errors
			result=$?
		else
			echo `_ '** Cannot execute %s' "$CMD"` > $TEMP/errors
		fi
	fi
	cat >/dev/null				# soak up the rest of the image
	echo $result
}

#
# some paths
#
#	CONFIG_DIR	directory in which the config file resides
#	AMRESTORE	full path name of amrestore
#	AMGETCONF	full path name of amgetconf
#	AMTAPE		full path name of amtape
#	TAR		ditto for GNU-tar
#	SAMBA_CLIENT	ditto for smbclient
#	DUMP		ditto for the system dump program
#	RESTORE		ditto for the system restore program
#	VDUMP		ditto for the system dump program
#	VRESTORE	ditto for the system restore program
#	VXDUMP		ditto for the system dump program
#	VXRESTORE	ditto for the system restore program
#	XFSDUMP		ditto for the system dump program
#	XFSRESTORE	ditto for the system restore program
#	DD		ditto for dd
#	MT		ditto for mt
#	MTF		flag given to MT to specify tape device: -f or -t
#	MAIL		mail program
#	IS_AIX		true if this is an AIX system

CONFIG_DIR=@CONFIG_DIR@
libexecdir=$libexecdir
sbindir=$sbindir
AMRESTORE=$sbindir/amrestore$SUF
AMGETCONF=$sbindir/amgetconf$SUF
AMTAPE=$sbindir/amtape$SUF
TAR=@GNUTAR@
SAMBA_CLIENT=@SAMBA_CLIENT@
DUMP=@DUMP@
RESTORE=@RESTORE@
VDUMP=@VDUMP@
VRESTORE=@VRESTORE@
VXDUMP=@VXDUMP@
VXRESTORE=@VXRESTORE@
XFSDUMP=@XFSDUMP@
XFSRESTORE=@XFSRESTORE@
MAIL=@MAILER@
DD=@DD@

. ${libexecdir}/chg-lib.sh

#
# config file
#
SLOT=0
CONFIG=$1
[ X"$CONFIG" = X"" ] \
	&& $Echoe "usage: amverify$SUF <config> [slot [ runtapes ] ]" \
	&& exit 1

AMCONFIG=$CONFIG_DIR/$CONFIG/amanda.conf
[ ! -f $AMCONFIG ] \
	&& $Echoe "Cannot find config file $AMCONFIG" \
	&& exit 1

TPCHANGER=`getparm tpchanger`
if [ X"$TPCHANGER" = X"" ]; then
	$Echoe "No tape changer..."
	DEVICE=`getparm tapedev`
	[ X"$DEVICE" = X"" ] \
		&& $Echoe "No tape device..." \
		&& exit 1
	$Echoe "Tape device is $DEVICE..."
	SLOTS=1
else
	CHANGER_SLOT=${2:-current}
	$Echoe "Tape changer is $TPCHANGER..."
	SLOTS=${3:-`getparm runtapes`}
	[ X"$SLOTS" = X"" ] && SLOTS=1
	if [ $SLOTS -eq 1 ]; then
		p=""
	else
		p=s
	fi
	$Echoe "$SLOTS slot${p}..."
	MAXRETRIES=2
fi

#
# check the accessability
#
[ X"$TAR" != X"" -a ! -x "$TAR" ] \
	&& $Echoe "GNU tar not found: $TAR"
[ X"$DUMP" != X"" -a \( X"$RESTORE" = X"" -o ! -x "$RESTORE" \) ] \
	&& $Echoe "System restore program not found: $RESTORE"
[ X"$VDUMP" != X"" -a \( X"$VRESTORE" = X"" -o ! -x "$VRESTORE" \) ] \
	&& $Echoe "System restore program not found: $VRESTORE"
[ X"$VXDUMP" != X"" -a \( X"$VXRESTORE" = X"" -o ! -x "$VXRESTORE" \) ] \
	&& $Echoe "System restore program not found: $VXRESTORE"
[ X"$XFSDUMP" != X"" -a \( X"$XFSRESTORE" = X"" -o ! -x "$XFSRESTORE" \) ] \
	&& $Echoe "System restore program not found: $XFSRESTORE"
[ ! -x $AMRESTORE ] \
	&& $Echoe "amrestore not found: $AMRESTORE" \
	&& exit 1

REPORTTO=`getparm mailto`
if [ X"$REPORTTO" = X"" ]; then
	$Echoe "No notification by mail!"
else
	$Echoe "Verify summary to $REPORTTO"
fi

ORG=`getparm org`
if [ X"$ORG" = X"" ]; then
	$Echoe "No org in amanda.conf -- using $CONFIG"
	ORG=$CONFIG
fi

#
# ok, let's do it
#
#	TEMP		directory for temporary tar archives and stderr
#	DEFECTS		defect list
#	REPORT		report for mail

if [ ! -d @AMANDA_TMPDIR@ ]; then
  $Echoe "amverify: directory @AMANDA_TMPDIR@ does not exist."
  exit 1
fi

cd @AMANDA_TMPDIR@ || exit 1

TEMP=@AMANDA_TMPDIR@/amverify.$$
trap 'rm -fr $TEMP' 0
if ( umask 077 ; mkdir $TEMP ) ; then
	:
else
	$Echoe "Cannot create $TEMP"
	exit 1
fi
DEFECTS=$TEMP/defects; rm -f $DEFECTS
REPORT=$TEMP/report; rm -f $REPORT
TAPELIST=
EXITSTAT=$TEMP/amrecover.exit; rm -rf $EXITSTAT

trap 'report "aborted!"; echo `_ 'aborted!'` >> $DEFECTS; sendreport; rm -fr $TEMP; exit 1' 1 2 3 4 5 6 7 8 10 12 13 14 15

$Echoe "Defects file is $DEFECTS"
report "amverify $CONFIG"
report "`date`"
report ""

# ----------------------------------------------------------------------------

SPLIT_DUMPS= # this will keep track of split dumps that we'll tally later
while [ $SLOT -lt $SLOTS ]; do
	SLOT=`expr $SLOT + 1`
	#
	# Tape Changer: dial slot
	#
	if [ X"$TPCHANGER" != X"" ]; then
		report "Loading ${CHANGER_SLOT} slot..."
		$AMTAPE $CONFIG slot $CHANGER_SLOT > $TEMP/amtape.out 2>&1
		THIS_SLOT=$CHANGER_SLOT
		CHANGER_SLOT=next
		RESULT=`grep "changed to slot" $TEMP/amtape.out`
		[ X"$RESULT" = X"" ] \
			&& report "** Error loading slot $THIS_SLOT" \
			&& report "`cat $TEMP/amtape.out`" \
			&& cat $TEMP/amtape.out >> $DEFECTS \
			&& continue
		DEVICE=`$AMTAPE $CONFIG device`
	fi
	report "Using device $DEVICE"
	$Echon "Waiting for device to go ready..."
	count=1800
        while true; do
            amdevcheck_output="`amdevcheck $CONFIG $DEVICE`"
            amdevcheck_status=$?
            if [ $amdevcheck_status -eq 0 ]; then
                break;
            else
                if echo $amdevcheck_output | grep UNLABELED > /dev/null; then
		    if [ count -lt 0 ]; then
		        report "Device not ready"
                        break;
		    fi
                    sleep 3
		    count=`expr $count - 3`
                else
                    report "Volume in $DEVICE unlabeled."
                    break;
                fi
            fi
	done
	$Echon "Processing label..."
        amtape_output="`amtape $CONFIG current 2>&1`";
        if echo "$amtape_output" | \
            egrep "^slot +[0-9]+: time [^ ]+ +label [^ ]+" > /dev/null; then
	    : # everything is fine
	else
            report "Error reading tape label using amtape."
            continue
        fi
        
        set X $amtape_output
        until [ "$1" = "time" ]; do
            shift
        done
        
	VOLUME=$4
	DWRITTEN=$2
	report "Volume $VOLUME, Date $DWRITTEN"
	[ X"$DWRITTEN" = X"0" -o X"$DWRITTEN" = X"X" ] \
		&& report "Fresh tape. Skipping..." \
		&& continue
	TAPELIST="$TAPELIST $VOLUME"

        FILENO=0
	ERG=0
	ERRORS=0
	while [ $ERG = 0 ]; do
	        FILENO=`expr $FILENO + 1`
#            { cat <<EOF; dd if=/dev/zero bs=32k count=1; } | doonefile
#AMANDA: FILE 20070925205916 localhost /boot  lev 0 comp N program /bin/tar
#To restore, position tape at start of file and run:
#        dd if=<tape> bs=32k skip=1 |      /bin/tar -xpGf - ...
#EOF
		RESULT=`$AMRESTORE -h -p -f $FILENO $DEVICE \
                            2> $TEMP/amrestore.out \
			| doonefile 2> $TEMP/onefile.errors`
		FILE=`grep restoring $TEMP/amrestore.out \
			| sed 's/^.*restoring //'`
		if [ X"$FILE" != X"" -a X"$RESULT" = X"0" ]; then
			report "Checked $FILE"
		elif [ X"$FILE" != X"" -a X"$RESULT" = X"500" ]; then
			report "Skipped `cat $TEMP/onefile.cmd` check on partial dump $FILE"
			dump="`echo $FILE | cut -d'.' -f'1,2,3,4'`"
			cat $TEMP/onefile.partnum >> $TEMP/$dump.parts
			if [ X"`echo $SPLIT_DUMPS | grep $dump`" = X"" ]; then
			    SPLIT_DUMPS="$dump $SPLIT_DUMPS"
			fi
		elif [ X"$FILE" != X"" -a X"$RESULT" = X"999" ]; then
			report "Skipped $FILE (`cat $TEMP/errors`)"
		elif [ -z "$FILE" ]; then
                        # Unless we went over, there is no extra output.
			report "End-of-Tape detected."
			break
		else
			report "** Error detected ($FILE)"
			echo "$VOLUME ($FILE):" >>$DEFECTS
			[ -s $TEMP/amrestore.out ] \
				&& report "`cat $TEMP/amrestore.out`" \
				&& cat $TEMP/amrestore.out >>$DEFECTS
			[ -s $TEMP/errors ] \
				&& report "`cat $TEMP/errors`" \
				&& cat $TEMP/errors >>$DEFECTS
			[ -s $TEMP/onefile.errors ] \
				&& report "`cat $TEMP/onefile.errors`" \
				&& cat $TEMP/onefile.errors >>$DEFECTS
			ERRORS=`expr $ERRORS + 1`
			[ $ERRORS -gt 5 ] \
				&& report "Too many errors." \
				&& break
		fi
	done
	rm -f $TEMP/header \
	      $TEMP/amtape.out \
	      $TEMP/amrestore.out \
	      $TEMP/errors \
	      $TEMP/onefile.cmd \
	      $TEMP/onefile.partnum \
	      $TEMP/onefile.errors
done

[ -s $DEFECTS ] \
	&& $Echoe "Errors found: " \
	&& cat $DEFECTS

# Work out whether any split dumps we saw had all their parts
for dump in $SPLIT_DUMPS;do
    report ""
    numparts=0
    max=0
    max_known=0
    missing=0
    # figure out 
    for part in `cat $TEMP/$dump.parts`;do
	cur="`echo $part | cut -d/ -f1`"
	max="`echo $part | cut -d/ -f2`"
	if [ $max != "UNKNOWN" ]; then
	    numparts=$max
	    max_known=1
	    break;
	fi
	if [ $cur -gt $numparts ]; then
	    numparts=$cur
	fi
    done
    report "Split dump $dump should have $numparts total pieces"
    if [ $max_known != 1 ]; then
	report "NOTE: Header field for total pieces was UNKNOWN, $numparts is best guess"
    fi
    part=1
    while [ $part -lt $numparts ];do
	part=`expr $part + 1`
	if [ X"`grep \"^$part/\" $TEMP/$dump.parts`" = X"" ];then
	    report "Spanning chunk part $part is missing!"
	    missing=`expr $missing + 1`
	fi
    done
    if [ $missing = 0 ];then
	report "All parts found"	
    fi
    rm -f $TEMP/$dump.parts
done

sendreport

exit 0
