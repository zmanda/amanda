#!/bin/ksh
# 
# Wrapper script to set environment and run dbbackup.tcl.
# 

#  user defined variables below
adm=/var/backup
mailuser=backup
dbhomescript=/opt/oracle/bin/dbhome

export ORACLE_SID=cc

#  no need to change anything below here
pgm=${0##*/}

if [[ ! -x ${adm}/dbbackup.tcl ]]
then
	msg="${pgm}: cannot execute ${adm}/dbbackup.tcl"
	/usr/bin/mailx -s "${msg}" ${mailuser} < /dev/null
	print -u2 ${msg}
	exit 1
fi

if [[ ! -x $dbhomescript ]]
then
	msg="${pgm}: cannot execute $dbhomescript"
	/usr/bin/mailx -s "${msg}" ${mailuser} < /dev/null
	print -u2 ${msg}
	exit 1
fi

timestamp=$(date "+%Y-%m-%d.%T")
log=${adm}/dbbackup.log.${timestamp}
err=${adm}/dbbackup.err.${timestamp}
rm -f ${log} ${err}

find ${adm}/. -name "dbbackup.log.*" -mtime +30 -print | xargs rm -f
find ${adm}/. -name "dbbackup.err.*" -mtime +30 -print | xargs rm -f

export ORACLE_HOME=$($dbhomescript "$ORACLE_SID")
export ORA_NLS=$ORACLE_HOME/ocommon/nls/admin/data
export LD_LIBRARY_PATH=$ORACLE_HOME/lib

( ${adm}/dbbackup.tcl 2>&1 || touch ${err} 2>&1 ) | tee $log

if [[ -f ${err} ]]
then
	msg="${pgm}: dbbackup.tcl failed"
	/usr/bin/mailx -s "${msg}" ${mailuser} < ${log}
	print -u2 ${msg}
	exit 1
fi

exit 0
