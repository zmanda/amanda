#!@SHELL@
#
# amrmtape.sh
# Time-stamp: <96/10/23 12:07:21 adrian>
# Copyright 1996, Adrian Filipi-Martin
#
# amrmtape
#
# Summary:  This script allow you to invalidate the contents of an
# existing backup tape within the Amanda current tape database.  This
# is meant as a recovery mecanism for when a good backup is damaged
# either by faulty hardware or user error, i.e. the tape is eaten by
# the tape drive, or the tape has been overwritten.
#
# To remove a tape you must specify the Amanda configuration to
# operate upon as well as the name of the tape. e.g.
#
# amrmtape nvl NVL-006
#
# N.B.  amrmtape must be run as a user that can read the tape database
# files and rewrite them.
#
# Usage: amrmtape [-n] [-v] [-q] [-d] <configuration> <label>
#          -n Do nothing to original files, leave new ones in --with-tmpdir
#	      directory.
#          -v Verbose mode.  Enabled by default.
#          -q Quiet (opposite of -v).
#          -d Enable debug tracing.
#
# Credits: The what-to-do algorithm was provided by Cedric Scott,
#          cedric.scott@sse.ie. 
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

USE_VERSION_SUFFIXES="@USE_VERSION_SUFFIXES@"
if test "$USE_VERSION_SUFFIXES" = "yes"; then
	SUF="-@VERSION@"
else
	SUF=
fi

Program=`basename $0`

CleanTapelist () {
  [ "xyes" = "x${DebugMode}" ] && set -x

  #
  # Check if the configuration directory exists.  Make sure that the
  # necessary files can be found, such as amanda.conf and tapelist.
  #
  if [ ! -d ${ConfigDir}/${Config} ]; then
    log `_ '%s: configuration directory %s does not exist.' "$0" "${ConfigDir}/${Config}"`
    return 1
  fi
  (cd ${ConfigDir}/${Config} >/dev/null 2>&1) || return $?
  cd ${ConfigDir}/${Config}
  if [ ! -r amanda.conf ]; then
    log `_ '%s: amanda.conf not found or is not readable in %s.' "$0" "${ConfigDir}"`
    return 1
  fi

  dumpuser=`amgetconf$SUF dumpuser`
  runuser=`whoami`
  if [ $runuser != $dumpuser ]; then
    log `_ '%s: must be run as user %s' "$0" "$dumpuser"`
    return 1
  fi

  # Get the location and name of the tapelist filename.  If tapelist is not
  # specified in the amanda.conf file, then use tapelist in the config
  # directory.
  TapeList=`amgetconf${SUF} tapelist`
  if [ ! "$TapeList" ]; then
    TapeList="$ConfigDir/$Config/tapelist"
  fi
  if [ ! -r $TapeList ]; then
    log `_ '%s: %s not found or is not readable.' "$0" "$TapeList"`
    return 1
  fi

  # Get the location and name of the database filename.
  InfoFile=`amgetconf${SUF} infofile`
  if [ ! "$InfoFile" ]; then
    log `_ '%s: unable to find name of infofile from %s.' "$0" "${ConfigDir}/${Config}/amanda.conf"`
    return 1
  fi
  VarDir=`echo "$InfoFile" | sed -e 's%^[^/]*$%.%' -e 's%/[^/]*$%%'`

  # Check that the database directory and files really exist.
  if [ ! -d "${VarDir}" ]; then
    log `_ '%s: %s does not exist or is not a directory.' "$0" "${VarDir}"`
    return 1
  fi
  if [ ! -r "${InfoFile}" ] && [ ! -d "${InfoFile}" ]; then
    log `_ '%s: %s does not exist or is not readable.' "${Program}" "${InfoFile}"`
    return 1
  fi

  if [ ! -d @AMANDA_TMPDIR@ ]; then
    log `_ '%s: directory %s does not exist.' "$0" "@AMANDA_TMPDIR@"`
    exit 1
  fi

  NewTapelist=@AMANDA_TMPDIR@/tapelist
  rm -f ${NewTapelist}
  awk "\$2 == \"${Tape}\" { next; } { print; }" \
      > ${NewTapelist} < $TapeList ||
  return $?
  if [ "xno" = "x${DoNothing}" ]; then
    lines=`wc -l < $TapeList`
    linesafter=`wc -l < $NewTapelist`
    if [ "$lines" -gt "$linesafter" ]; then
      cp -p $TapeList ${TapeList}~ && (
        if test "$lines" -gt 1; then
          [ -s ${NewTapelist} ] &&
            cp ${NewTapelist} $TapeList &&
            rm -f ${NewTapelist}
        else
          [ -f ${NewTapelist} ] &&
            cp ${NewTapelist} $TapeList &&
            rm -f ${NewTapelist}
        fi
      )
      log `_ '%s: remove label %s.' "$0" "${Tape}"`
    else
      log `_ '%s: no such tape: %s.' "$0" "${Tape}"`
      return 1
    fi
  fi
  
  return $?
}


CleanCurinfo_internal() {
  DeadLevel=10
  while read Line; do
    case ${Line} in
      CURINFO*|"#"*|command*|last_level*|consecutive_runs*|full*|incr*)
	echo "${Line}"
        ;;
      host*)
	set ${Line}
        Host=$2
	echo "${Line}"
        ;;
      disk*)
	set ${Line}
        Disk=$2
	echo "${Line}"
        ;;
      stats*)
	set ${Line}
	if [ $# -lt 6 ] || [ $# -gt 8 ]; then
	  log `_ '%s: unexpected number of fields in "stats" entry for %s.' "${Program}" "${Host}:${Disk}"`
	  log "${Line}"
	  return 1
	fi
	Level=$2
	CurrentTape=$8
	if [ "${CurrentTape}" = "${Tape}" ]; then
	  DeadLevel=${Level}
	  ${Verbose} "Discarding Host: ${Host}, Disk: ${Disk}, Level: ${Level}"
	elif [ $Level -gt $DeadLevel ]; then
	  ${Verbose} "Discarding Host: ${Host}, Disk: ${Disk}, Level: ${Level}"
	else
	  echo "${Line}"
	fi
	;;
      history*)
	set ${Line}
	echo "${Line}"
	;;
      //)
	echo "${Line}"
	DeadLevel=10
	;;
      *)
	log `_ 'Error: unrecognized line of input: "%s"' "${Line}"`
	return 1
    esac
  done
}

CleanCurinfo () {
  [ "xyes" = "x${DebugMode}" ] && set -x
  (cd ${VarDir} >/dev/null 2>&1) || return $?
  cd ${VarDir}
  InfoFileBase=`echo $InfoFile | sed -e 's%.*/%%g'`

  TmpSrc=$InfoFileBase.orig.$$
  TmpDest=$InfoFileBase.new.$$
  rm -f ${TmpSrc} ${TmpDest}
  amadmin${SUF} ${Config} export > ${TmpSrc} || return $?
  log `_ '%s: preserving original database in %s (exported).' "$0" "${TmpSrc}"`
  CleanCurinfo_internal < ${TmpSrc} > ${TmpDest} || return $?

  if [ "xno" = "x${DoNothing}" ]; then
    [ -s ${TmpDest} ] && 
    amadmin${SUF} ${Config} import < ${TmpDest} &&
    rm -f ${TmpDest}
  fi

  return $?
}


log () {
  echo 1>&2 "$@"
  return 0
}


usage () {
  echo `_ '%s [-n] [-v] [-q] [-d] <configuration> <label>' "${Program}"`
  echo `_ '  -n Do nothing to original files, leave new ones in database directory.'`
  echo `_ '  -v Verbose, list backups of hosts and disks that are being discarded.'`
  echo `_ '  -q Quiet, opposite of -v.'`
  echo `_ '  -d Enable debug tracing.'`
  echo `_ 'This program allows you to invalidate the contents of an existing
backup tape within the Amanda current tape database.  This is meant
as a recovery mecanism for when a good backup is damaged either by
faulty hardware or user error, i.e. the tape is eaten by the tape drive,
or the tape has been overwritten.'`
  return 0
}


Verbose="log "
DoNothing="no"
DebugMode="no"

set dummy ${1+"$@"}
while shift 2>/dev/null; do
  case "$1" in
    -q)
      Verbose=": "
      ;;
    -v)
      Verbose="log "
      ;;
    -n)
      DoNothing="yes"
      ;;
    -d)
      DebugMode="yes"
      ;;
    *)
      if [ $# = 2 ]; then
        Config=$1
        Tape=$2
	break
      else
        usage 1>&2
	exit 1
      fi

  esac
done

( CleanTapelist && CleanCurinfo )
exit $?
