#! @SHELL@
#
# patch inetd.conf and services
# originally by Axel Zinser (fifi@hiss.han.de)
#

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

SERVICE_SUFFIX="@SERVICE_SUFFIX@"

USER="@CLIENT_LOGIN@"

INETDCONF=/etc/inetd.conf
[ ! -f $INETDCONF ] && INETDCONF=/usr/etc/inetd.conf

SERVICES=/etc/services
[ ! -f $SERVICES ] && SERVICES=/usr/etc/services

ENABLE_AMANDAD=true

case `uname -n` in
"@DEFAULT_SERVER@" | "@DEFAULT_SERVER@".*)
    ENABLE_INDEX=true
    ENABLE_TAPE=true
    ;;
*)
    ENABLE_INDEX=false
    ENABLE_TAPE=false
    ;;
esac

CLIENT_PORT=10080
KCLIENT_PORT=10081
INDEX_PORT=10082
TAPE_PORT=10083

while [ $# != 0 ]; do
    case "$1" in
    --service-suffix=*)
	SERVICE_SUFFIX=`echo $1 | sed -e 's/[^=]*=//'`;;
    --version-suffix=*)
	SUF=`echo $1 | sed -e 's/[^=]*=//'`;;
    --inetd=*)
        INETDCONF=`echo $1 | sed -e 's/[^=]*=//' -e 's%^$%/dev/null%'`;;
    --services=*)
	SERVICES=`echo $1 | sed -e 's/[^=]*=//' -e 's%^$%/dev/null%'`;;
    --libexecdir=?*)
	libexecdir=`echo $1 | sed -e 's/[^=]*=//'`;;
    --user=?*)
	USER=`echo $1 | sed -e 's/[^=]*=//'`;;
    --enable-client)
	ENABLE_AMANDAD=true;;
    --disable-client)
	ENABLE_AMANDAD=false;;
    --enable-index)
	ENABLE_INDEX=true;;
    --disable-index)
	ENABLE_INDEX=false;;
    --enable-tape)
	ENABLE_TAPE=true;;
    --disable-tape)
	ENABLE_TAPE=false;;
    --client-port=?*)
	CLIENT_PORT=`echo $1 | sed -e 's/[^=]*=//'`;;
    --kclient-port=?*)
	KCLIENT_PORT=`echo $1 | sed -e 's/[^=]*=//'`;;
    --index-port=?*)
	INDEX_PORT=`echo $1 | sed -e 's/[^=]*=//'`;;
    --tape-port=?*)
	TAPE_PORT=`echo $1 | sed -e 's/[^=]*=//'`;;
    --usage | --help | -h)
	echo `_ 'call this script with zero or more of the following arguments:'`
	echo `_ '--version-suffix=<suffix>: deprecated option' ""`
	echo `_ '--service-suffix=<suffix>: append to service names [%s]' "$SERVICE_SUFFIX"`
	echo `_ '--libexecdir=<dirname>: where daemons should be looked for [%s]' "$libexecdir"`
	echo `_ '--inetd=<pathname>: full pathname of inetd.conf [%s]' "$INETDCONF"`
	echo `_ '--services=<pathname>: full pathname of services [%s]' "$SERVICES"`
	echo `_ '\tan empty pathname or /dev/null causes that file to be skipped'`
	echo `_ '--user=<username>: run deamons as this user [%s]' "$USER"`
	echo `_ '--enable/disable-client: enable/disable amandad [%s]' \`$ENABLE_AMANDAD && echo enabled || echo disabled\``
	echo `_ '--enable/disable-index: enable/disable index server [%s]' \`$ENABLE_INDEX && echo enabled || echo disabled\``
	echo `_ '--enable/disable-tape: enable/disable tape server [%s]' \`$ENABLE_TAPE && echo enabled || echo disabled\``
	echo `_ '--client-port=<num>: amandad port number [%s]' "$CLIENT_PORT"`
	echo `_ '--kclient-port=<num>: kamandad port number [%s]' "$KCLIENT_PORT"`
	echo `_ '--index-port=<num>: index server port number [%s]' "$INDEX_PORT"`
	echo `_ '--tape-port=<num>: tape server port number [%s]' "$TAPE_PORT"`
	exec true;;
    *)
	echo `_ '%s: invalid argument %s.  run with -h for usage\n' "$0" "$1"` >&2
	exec false;;
    esac
    shift
done

if [ "$SERVICES" = /dev/null ]; then :
elif [ -f "$SERVICES" ]; then
	TEMP="$SERVICES.new"
	{
	    egrep < "$SERVICES" -v "^(amanda|kamanda|amandaidx|amidxtape)${SERVICE_SUFFIX}[ 	]"
	    echo "amanda${SERVICE_SUFFIX} ${CLIENT_PORT}/udp"
	    echo "amanda${SERVICE_SUFFIX} ${CLIENT_PORT}/tcp"
	    echo "kamanda${SERVICE_SUFFIX} ${KCLIENT_PORT}/udp"
	    echo "amandaidx${SERVICE_SUFFIX} ${INDEX_PORT}/tcp"
	    echo "amidxtape${SERVICE_SUFFIX} ${TAPE_PORT}/tcp"
	} > "$TEMP"
	if diff "$SERVICES" "$TEMP" >/dev/null 2>/dev/null; then
		echo `_ '%s is up to date' "$SERVICES"`
	else
		cp "$TEMP" "$SERVICES" || echo `_ 'cannot patch %s' "$SERVICES"`
	fi
	rm -f "$TEMP"
else
	echo `_ '%s not found!' "$SERVICES"`
fi
if [ "$INETDCONF" = /dev/null ]; then :
elif [ -f "$INETDCONF" ]; then
	err=`_ 'warning: %s/amandad%s does not exist' "$libexecdir" ""`
	$ENABLE_AMANDAD && test ! -f $libexecdir/amandad && echo "$err" >&2
	err=`_ 'warning: %s/amindexd%s does not exist' "$libexecdir" ""`
	$ENABLE_INDEX && test ! -f $libexecdir/amindexd && echo "$err" >&2
	err=`_ 'warning: %s/amidxtaped%s does not exist' "$libexecdir" ""`
	$ENABLE_TAPE && test ! -f $libexecdir/amidxtaped && echo "$err" >&2
	TEMP="$INETDCONF.new"
	{
	    egrep < "$INETDCONF" -v "^(amanda|amandaidx|amidxtape)${SERVICE_SUFFIX}[ 	]"
	    $ENABLE_AMANDAD && echo "amanda${SERVICE_SUFFIX}    dgram  udp wait   $USER $libexecdir/amandad    amandad"
	    $ENABLE_INDEX && echo "amandaidx${SERVICE_SUFFIX} stream tcp nowait $USER $libexecdir/amindexd   amindexd"
	    $ENABLE_TAPE && echo "amidxtape${SERVICE_SUFFIX} stream tcp nowait $USER $libexecdir/amidxtaped amidxtaped"
	} > "$TEMP"
	if diff "$INETDCONF" "$TEMP" >/dev/null 2>/dev/null; then
		fmt="%s is up to date\n"
		printf $fmt $INETDCONF
	else
		fmt="cannot patch %s\n"
		cp "$TEMP" "$INETDCONF" || printf $fmt $INETDCONF
	fi
	rm -f "$TEMP"
else
	fmt="%s not found!\n"
	printf $fmt $INETDCONF
fi
