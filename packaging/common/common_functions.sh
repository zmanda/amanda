#!/bin/sh
# Common Functions

# Required variables:
# LOGFILE
# SYSCONFDIR
# os

logger() {
	# A non-annoying way to log stuff
	# ${@} is all the parameters, also known as the message.  Quoting the input
	# preserves whitespace.
	msg="`date +'%b %e %Y %T'`: ${@}"
	echo "${msg}" >> ${LOGFILE}
}

log_output_of() {
	# A non-annoying way to log output of commands
	# ${@} is all the parameters supplied to the function.  just execute it,
	# and capture the output in a variable.  then log that.
	output=`"${@}" 2>&1`
	ret=$?
	if [ -n "${output}" ] ; then
		logger "${1}: ${output}"
	fi
	return ${ret}
}

check_superserver() {
    # Check for the superserver $1 for the config $2
    case $1 in
	xinetd) check_xinetd $2; return $?;;
	inetd) check_inetd $2; return $?;;
	launchd) check_launchd $2; return $?;;
        smf) check_smf $2; return $?;;
    esac
}

check_xinetd() {
	# Checks for an xinetd install and a config name passed as the first
	# parameter.
	# Returns:
	#	 0 if the file exists,
	#	 1 if it does not,
	#	 2 if xinetd.d/ does not exist or is a file

	if [ -d ${SYSCONFDIR}/xinetd.d ] ; then
		if [ -f ${SYSCONFDIR}/xinetd.d/${1} ] ; then
			logger "Found existing xinetd config: ${1}"
			return 0
		else
			return 1
		fi
	else
		# No xinetd installation.
		return 2
	fi
}

check_inetd() {
    case $os in
        SunOS) inetd_conf=${SYSCONFDIR}/inet/inetd.conf ;;
        *) inetd_conf=${SYSCONFDIR}/inetd.conf ;;
    esac
    if [ -e ${inetd_conf} ] ; then
        if grep "${1}" ${inetd_conf} > /dev/null ; then
            logger "Found existing inetd config: ${1}"
            return 0
        else
            return 1
        fi
    else
        # No inetd installation.
            return 2
    fi
}

check_launchd() {
    # TODO: refactor OS X scripts.
    :
}

check_smf() {
    # Only for solaris! This check only notices if an amanda service is active,
    # it does not notice server vs client entries.
    log_output_of svcs -H "*amanda*" || { \
        logger "No amanda service found."; return 1; }
}

check_superserver_running() {
    # Check for the given superserver, $1, in the output of ps -ef, or on
    # mac/bsd ps ax.
    # Return codes:
    #  0: $1 is running
    #  1: $1 is not running
    #  2: $1 is not valid for this system
    case $1 in
	# Linux or Solaris.  This works despite sol10 using SMF.
	inetd) ps_flags='-e';;
	xinetd) ps_flags='-e';;
	# Mac OS X
	launchd) ps_flags='aux';;
	*) echo "Bad superserver."; return 2 ;;
    esac
    if [ "$1" = "launchd" ] && [ `uname` != 'Darwin' ]; then
	echo "Only darwin uses launchd"
	return 2
    fi
    if [ "$1" = "xinetd" ] && [ "$os" = 'SunOS' ] && \
       [ `uname -r` = "5.10" ]; then
        echo "Solaris 10 does not use xinetd."
        return 2
    fi
    # Search for $1, 
    PROC=`ps ${ps_flags} | grep -v 'grep'| grep "${1}"`
    if [ x"${PROC}" != x ]; then
	return 0
    else
	return 1
    fi
}

backup_xinetd() {
    log_output_of mv ${SYSCONFDIR}/xinetd.d/${1} ${AMANDAHOMEDIR}/example/xinetd.${1}.orig || \
	{ logger "WARNING:  Could not back up existing xinetd configuration '${1}'";
	return 1; }
    logger "Old xinetd config for '${1}' backed up to '${AMANDAHOMEDIR}/example/xinetd.${1}.orig'"
}

backup_inetd() {
    case $os in
        SunOS) inetd_conf=${SYSCONFDIR}/inet/inetd.conf ;;
        *) inetd_conf=${SYSCONFDIR}/inetd.conf ;;
    esac
    # Backs up any amanda configuration it finds
    log_output_of sed -n "/^amanda .* amandad/w ${AMANDAHOMEDIR}/example/inetd.orig" ${inetd_conf} || \
	{ logger "WARNING: could not write ${AMANDAHOMEDIR}/example/inetd.orig";
	return 1; }
    log_output_of sed "/^amanda .* amandad/d" ${inetd_conf} > \
	${inetd_conf}.tmp || \
	{ logger "WARNING: could not write ${inetd_conf}.tmp";
	return 1; }
    log_output_of mv ${inetd_conf}.tmp ${inetd_conf} || \
	{ logger "WARNING: could not overwrite ${inetd_conf}, old config not removed.";
	return 1; }
    logger "Old inetd config for amanda backed up to ${AMANDAHOMEDIR}/example/inetd.orig"
}

backup_smf() {
    # Solaris only. I *think* this should be consistent across all smf installs
    svccfg -s *amanda* > ${AMANDAHOMEDIR}/example/amanda_smf.xml.orig || {\
        logger "Warning: export of existing amanda service failed.";
        return 1; }

    log_output_of inetadm -d *amanda* || { \
        # Not critical, we don't need to return if this fails.
        logger "Warning: disabling existing amanda service failed."; }

    log_output_of svccfg delete -f *amanda* || { \
        logger "Error: removing the old amanda service failed.";
        return 1; }
}

install_xinetd() {
    log_output_of install -m 0644 ${AMANDAHOMEDIR}/example/xinetd.${1} ${SYSCONFDIR}/xinetd.d/${1} || \
	{ logger "WARNING:  Could not install xinetd configuration '${1}'" ; return 1; }		
    logger "Installed xinetd config for ${1}."
}

install_inetd() {
    case $os in
        SunOS) inetd_conf=${SYSCONFDIR}/inet/inetd.conf ;;
        *) inetd_conf=${SYSCONFDIR}/inetd.conf ;;
    esac
    # This one is hard to log because we're just appending.
    logger "Appending ${AMANDAHOMEDIR}/example/inetd.conf.${1} to ${inetd_conf}"
    cat ${AMANDAHOMEDIR}/example/inetd.conf.${1} >> ${inetd_conf}
}

install_smf() {
    # First parameter should be the name of the service to install
    # (amandaserver, or amandaclient).
    ver=`uname -r`
    case $ver in
      5.10)
        # Use inetadm and svcadm.
        log_output_of ${basedir}/usr/sbin/inetconv -f -i ${AMANDAHOMEDIR}/example/inetd.conf.${1} || { \
            logger "Warning: Failed to create Amanda SMF manifest. Check the system log.";
            return 1; }
        log_output_of ${basedir}/usr/sbin/inetadm -e svc:/network/amanda/tcp || { \
            logger "Warning: Failed to enable Amanda service. See system log for more information.";
            return 1; }
        log_output_of ${basedir}/usr/sbin/svcadm restart network/amanda/tcp || { \
            logger "Warning: Failed to restart Amanda service.  See system log for details.";
            return 1; }
      ;;

      5.8|5.9)
        logger "Solaris 8 and 9 use inetd, not SMF tools."
        return 1
      ;;
      
      *)
        # I don't know what to do...
        logger "ERROR: Unsupported and untested version of Solaris: $ver"
        return 1
      ;;
    esac
}

reload_xinetd() {
    # Default action is to try reload.
    if [ "x$1" = "x" ]; then
	action="reload"
    elif [ "$1" = "reload" ] || [ "$1" = "restart" ]; then
	action="$1"
    else
	logger "WARNING: bad argument to reload_xinetd: $1"
	return 1
    fi
    if [ "$action" = "reload" ] ; then
	logger "Reloading xinetd configuration..." 
	log_output_of ${SYSCONFDIR}/init.d/xinetd $action # Don't exit!
	if [ $? -ne 0 ] ; then
	    logger "xinetd reload failed.  Attempting restart..."
	    log_output_of ${SYSCONFDIR}/init.d/xinetd restart || \
		{ logger "WARNING:  restart failed." ; return 1; }
	fi
    else
	# Must be restart...
        logger "Restarting xinetd."
	log_output_of ${SYSCONFDIR}/init.d/xinetd $1 || \
	    { logger "WARNING:  ${1} failed." ; return 1; }
    fi
}

reload_inetd() {
    # Default action is to try reload.
    if [ "x$1" = "x" ]; then
	action="reload"
    elif [ "$1" = "reload" ] || [ "$1" = "restart" ]; then
	action="$1"
    else
	logger "WARNING: bad argument to reload_inetd: $1"
	return 1
    fi
    if [ "$1" = "reload" ] ; then
        logger "Reloading inetd configuration..."
        log_output_of ${SYSCONFDIR}/init.d/inetd $1 # Don't exit!
        if [ $? -ne 0 ] ; then
            logger "inetd reload failed.  Attempting restart..."
            log_output_of ${SYSCONFDIR}/init.d/inetd restart || \
                    { logger "WARNING:  restart failed." ; return 1; }
        fi
    else
	# Must be restart...
        logger "Restarting inetd."
        log_output_of ${SYSCONFDIR}/init.d/inetd $1 || \
		{ logger "WARNING:  ${1} failed." ; return 1; }
    fi
}
# End Common functions
