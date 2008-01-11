#!@SHELL@
#
# Original wrapper by Paul Bijnens
#
# worked by Stefan G. Weichinger
# to enable gpg-encrypted dumps via aespipe
# also worked by Matthieu Lochegnies for server-side encryption

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

# add sbin and ucb dirs, as well as csw (blastwave)
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb:/opt/csw/bin"
export PATH

AMANDA_HOME=~@CLIENT_LOGIN@
AM_AESPIPE=@sbindir@/amaespipe
AM_PASSPHRASE=$AMANDA_HOME/.am_passphrase

AESPIPE=`which aespipe`

if [ $? -ne 0 ] ; then
	echo `_ '%s: %s was not found in %s' "$0" "aespipe" "$PATH"` >&2
        exit 2
fi

if [ ! -x $AESPIPE ] ; then
	echo `_ '%s: %s is not executable' "$0" "aespipe"` >&2
        exit 2
fi

if [ ! -x $AM_AESPIPE ] ; then
        echo `_ '%s: %s was not found' "$0" "$AM_AESPIPE"` >&2
        exit 2
fi
if [ ! -x $AM_AESPIPE ] ; then
        echo `_ '%s: %s is not executable' "$0" "$AM_AESPIPE"` >&2
        exit 2
fi

$AM_AESPIPE "$@" 3< $AM_PASSPHRASE

rc=$?
exit $rc
