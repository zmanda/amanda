#! @SHELL@
#
# check tapelist against database and vice versa
#

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

ConfigDir=@CONFIG_DIR@

# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

Program=`basename $0`

log () {
	echo 1>&2 "$@"
	return 0
}

Config=$1
if [ "$Config" = "" ]; then
	log "usage: ${Program} <config>"
	exit 1
fi
shift;

#
# Check if the configuration directory exists.  Make sure that the
# necessary files can be found, such as amanda.conf and tapelist.
#
if [ ! -d ${ConfigDir}/${Config} ]; then
	log "${Program}: configuration directory ${ConfigDir}/${Config} does not exist."
	exit 1
fi
(cd ${ConfigDir}/${Config} >/dev/null 2>&1) || exit $?
cd ${ConfigDir}/${Config}
if [ ! -r amanda.conf ]; then
	log "${Program}: amanda.conf not found or is not readable in ${ConfigDir}."
	exit 1
fi

# Get the location and name of the tapelist filename.  If tapelist is not
# specified in the amanda.conf file, then use tapelist in the config
# directory.
TapeList=`amgetconf${SUF} $Config tapelist "@$"`
if [ ! "$TapeList" ]; then
	TapeList="$ConfigDir/$Config/tapelist"
fi
if [ ! -r $TapeList ]; then
	log "${Program}: $TapeList not found or is not readable."
	exit 1
fi

Amadmin=$sbindir/amadmin

[ ! -f $Amadmin ] \
	&& echo `_ '%s was not found' $Amadmin` >&2 \
	&& exit 1
[ ! -x $Amadmin ] \
	&& echo `_ '%s is not executable' $Amadmin` >&2 \
	&& exit 1

$Amadmin $Config export "$@"\
	| grep "^stats: " \
	| while read LINE; do
		[ "$LINE" = "" ] && continue
		set $LINE
		echo $8
	done \
	| sort -u \
	| while read TAPE; do
		[ "$TAPE" = "" ] && continue
		grep " $TAPE " $TapeList 2>/dev/null >/dev/null
		[ $? != 0 ] \
			&& echo `_ 'Tape %s missing in %s' "$TAPE" "$TapeList"`
	done

echo `_ 'Ready.'`

exit 0
