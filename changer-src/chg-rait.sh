#!@SHELL@

# chg-rait
#
# This assumes we have rait-striped drives in several
# other amanda changer configs.
#
# so we have a changerfile that lists other changers and
# changer files.
#   nchangers=3
#   tpchanger_1="chg-mtx"
#   changerdev_1="/dev/mtx1"
#   changerfile_1="/some/file1"
#   tapedev_1="/some/dev"
#   tpchanger_2="chg-mtx"
#   changerdev_2="/dev/mtx2"
#   changerfile_2="/some/file2"
#   tapedev_2="/some/dev"
#   tpchanger_3="chg-mtx"
#   changerdev_3="/dev/mtx3"
#   changerfile_3="/some/file3"
#   tapedev_3="/some/dev"
#
# the tapedev_n entries are only needed if the changer script in question
# uses tapedev.
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
# debugging...
#
if [ -d "@AMANDA_DBGDIR@" ]; then
	DBGFILE=@AMANDA_DBGDIR@/rait-changer.debug
	KIDDEBUG=@AMANDA_DBGDIR@/changer.debug
	WORK=@AMANDA_DBGDIR@/chgwork$$
else
	DBGFILE=/dev/null
	KIDDEBUG=/dev/null
	WORK=/tmp/chgwork$$
fi

exec 2>$DBGFILE
echo `_ "arguments: "` $0 $* >&2
set -x 

USE_VERSION_SUFFIXES="@USE_VERSION_SUFFIXES@"
if test "$USE_VERSION_SUFFIXES" = "yes"; then
        SUF="-@VERSION@";
else
	SUF=
fi
getconf=$sbindir/amgetconf$SUF


changerfile=`$getconf changerfile`
. $changerfile

#
# get config items that other changers use to put in our
# fake amanda.conf files.
#
org=`$getconf ORG`
mailto=`$getconf mailto`

#
# make a working directory (with amanda.conf) for each changer, and start the
# changer script in background for each one
#

i=1
while [ $i -le $nchangers ]
do
   eval tpchanger=\$tpchanger_$i
   eval changerdev=\$changerdev_$i
   eval changerfile=\$changerfile_$i
   eval tapedev=\$tapedev_$i

   mkdir -p $WORK/$i
   (
       cd $WORK/$i

       cat >> amanda.conf <<EOF
org     	"$ORG"
mailto  	"$mailto"
tpchanger 	"$tpchanger"
changerdev 	"$changerdev"
changerfile 	"$changerfile"
tapedev 	"$tapedev"

define tapetype EXABYTE {
    comment "default tapetype"
    length 4200 mbytes
    filemark 48 kbytes
    speed 474 kbytes			
}
EOF

	(
	    $tpchanger "$@"
	    echo "$?"> exitcode
	)  > stdout 2>stderr &
    )

    i=`expr $i + 1`
done
wait

#
# once they've all finished, collect up the results
#

myexit=0
myslot=-1
mymax=65536
myflag=1
mydev=""
mysep="{"

case x$1 in
x-slot|x-reset|x-eject|x-search|x-label)

    #
    # read slot number and device from each
    # slot numbers must match(?!), and is our resulting slot number
    # resulting device is {dev1,dev2,...} from each one
    #
    i=1
    while [ $i -le $nchangers ]
    do
	read exitcode < $WORK/$i/exitcode
	read n dev < $WORK/$i/stdout
	echo -------------- >&2
        cat $WORK/$i/stderr >&2
	cat $KIDDEBUG >&2
	echo -------------- >&2

	if [ "$exitcode" != 0 ]
	then
	    myexit=$exitcode
	fi
	if [ $myslot = -1 ]
	then
	    myslot=$n
	fi
	if [ $n != $myslot ]
	then
	     # synch error!
	    myexit=1
	    echo `_ 'stackers are out of synch, issue a reset'` >&2
	fi
	mydev="$mydev$mysep$dev"
	mysep=","

	i=`expr $i + 1`
    done
    mydev="rait:$mydev}"
    echo $myslot $mydev
    ;;
x-info)
    #
    # read info from each
    # slot numbers must match(?!), and is our resulting slot number
    # minimum max slots is our resulting max slots
    # if any can't go backwards, the aggregate can't either
    #
    i=1
    while [ $i -le $nchangers ]
    do
	read exitcode < $WORK/$i/exitcode
	read n max flag < $WORK/$i/stdout
	echo -------------- >&2
        cat $WORK/$i/stderr >&2
	cat $KIDDEBUG >&2
	echo -------------- >&2

	if [ "$exitcode" != 0 ]
	then
	     myexit=$exitcode
	fi
	if [ $myslot = -1 ]
	then
	    myslot=$n
	fi
	if [ $n != $myslot ]
	then
	     # synch error!
	    myexit=1
	    echo `_ 'stackers are out of synch, issue a -reset'` >&2
	fi
	if [ $max -lt $mymax ]
	then
	    mymax=$max
	fi
	if [ $flag = 0 ]
	then
	    myflag=0
	fi
	i=`expr $i + 1`
    done
    echo $myslot $mymax $myflag

esac

#
# clean up work directories
#
rm -rf $WORK

exit $myexit
