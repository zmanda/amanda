#!/bin/sh
# ------------- Begin Post Install Functions -----------------
# These functions are included by various different installers.

# We assume that the following variables are defined in the main script:
# amanda_user: the amanda account username
# amanda_group: the amanda account's group
# AMANDAHOMEDIR: a directory to use as amanda's home
# dist: used on linux for the distro.
# install_log: a log file we append to
# os: Linux, Mac, Solaris, etc...
# SYSCONFDIR: location of system config files (ie, /etc)
# LOGDIR: logging directory for amanda

#TODO: gnutar-lists dir for solaris??

add_service() {
    # Only needed on Solaris!
    entry="amanda       10080/tcp    # amanda backup services"
    # make sure amanda is in /etc/services
    if [ -z "`grep 'amanda' ${SYSCONFDIR}/services |grep '10080/tcp'`" ] ; then
        logger "Adding amanda entry to ${SYSCONFDIR}/services."
        echo "${entry}" >> ${SYSCONFDIR}/services
    fi

    # make sure kamanda is in /etc/services
    entry_2="amanda       10081/tcp    famdc    # amanda backup services (kerberos)"
    if [ -z "`grep 'kamanda' /etc/services |grep '10081/tcp'`" ] ; then
        logger "Adding kamanda entry to ${SYSCONFDIR}/services."
        echo "${entry_2}" >> ${SYSCONFDIR}/services
    fi
}

create_amandates() {
	logger "Creating ${AMANDATES}."
	if [ ! -f ${AMANDATES} ] ; then
		touch ${AMANDATES} || { logger "WARNING:  Could not create Amandates." ; return 1; }
	fi
}

check_amandates() {
	logger "Ensuring correct permissions for '${AMANDATES}'."
	log_output_of chown ${amanda_user}:${amanda_group} ${AMANDATES} || \
		{ logger "WARNING:  Could not chown ${AMANDATES}" ; return 1; }
	log_output_of chmod 0640 ${AMANDATES} || \
		{ logger "WARNING:  Could not fix perms on ${AMANDATES}" ; return 1; }
	if [ -x /sbin/restorecon ] ; then
		log_output_of /sbin/restorecon ${AMANDATES} || \
			{ logger "WARNING:  restorecon execution failed." ; return 1; }
	fi
}

create_gnupg() {
	# Install .gnupg directory
	if [ ! -d ${AMANDAHOMEDIR}/.gnupg ] ; then
		logger "Creating '${AMANDAHOMEDIR}/.gnupg'"
		log_output_of mkdir ${AMANDAHOMEDIR}/.gnupg || \
			{ logger "WARNING:  Could not create .gnupg dir" ; return 1; }
	fi
}

check_gnupg() {
	logger "Ensuring correct permissions for '${AMANDAHOMEDIR}/.gnupg'."
	log_output_of chown ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.gnupg || \
		{ logger "WARNING:  Could not chown .gnupg dir." ; return 1; }
	log_output_of chmod 700 ${AMANDAHOMEDIR}/.gnupg || \
		{ logger "WARNING:  Could not set permissions on .gnupg dir." ; return 1; }
}

create_amandahosts() {
	# Install .amandahosts to server
	if [ ! -f ${AMANDAHOMEDIR}/.amandahosts ] ; then
		logger "Creating ${AMANDAHOMEDIR}/.amandahosts"
        log_output_of touch ${AMANDAHOMEDIR}/.amandahosts || \
		{ logger "WARNING:  Could not create .amandahosts file." ; return 1; }
	fi
}

check_amandahosts_entry() {
	# Entries for client and server differ slightly 
	# $1 username (root, ${amanda_user})
	# subsequent parameters are a list of programs to check (amindexd 
	# amidxtaped, or amdump)
	logger "Checking '${AMANDAHOMEDIR}/.amandahosts' for '${@}' entries."
	# Generate our grep expression
	expr=""
	for prog in ${@} ; do
		expr=${expr}"[[:blank:]]\+${prog}"
	done
	for host in localhost localhost.localdomain ; do
		logger "Searching .amandahosts for ^${host}${expr}"
		if `grep "^${host}${expr}" ${AMANDAHOMEDIR}/.amandahosts >> /dev/null` ; then
			continue
		else
			add_amandahosts_entry ${host} ${@}
		fi
	done
}

add_amandahosts_entry() {
	# Add entries to amandahosts.
	# $@ is a fully formatted entry for amandahosts
	logger "Appending '${@}' to amandahosts"
	echo "${@}" >>${AMANDAHOMEDIR}/.amandahosts || \
		{ logger "WARNING:  Could not append to .amandahosts" ; return 1; }
}

check_amandahosts_perms() {
	logger "Ensuring correct permissions on .amandahosts"
	log_output_of chown ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.amandahosts || \
		{ logger "WARNING:  Could not chown .amandahosts." ; return 1; }
	log_output_of chmod 0600 ${AMANDAHOMEDIR}/.amandahosts || \
		{ logger "WARNING:  Could not fix permissions on .amandahosts" ; return 1; }
}

create_ssh_key() {
	# SSH RSA key generation for amdump and amrecover
	# $1 must be "server" or "client"
	KEYDIR="${AMANDAHOMEDIR}/.ssh"
	if [ $1 = "server" ] ; then
		KEYFILE="id_rsa_amdump"
	elif [ $1 = "client" ] ; then
		KEYFILE="id_rsa_amrecover"
	else
		logger "Bad parameter to create_ssh_key" ; return 1
	fi
	COMMENT="${amanda_user}@$1"
	if [ ! -d ${KEYDIR} ] ; then
		if [ -f ${KEYDIR} ] ; then
			logger "Directory '${KEYDIR}' exists as a file.  Renaming to '${KEYDIR}.save'."
			log_output_of mv ${KEYDIR} ${KEYDIR}.save || \
				{ logger "WARNING:  Could not backup old .ssh directory." ; return 1; }
		fi
		logger "Creating directory '${KEYDIR}'."
		log_output_of mkdir ${KEYDIR} || \
			{ logger "WARNING:  Could not create .ssh dir." ; return 1; }
	fi
	if [ ! -f ${KEYDIR}/${KEYFILE} ] ; then
		logger "Creating ssh RSA key in '${KEYDIR}/${KEYFILE}'"
		log_output_of ssh-keygen -q -C $COMMENT -t rsa -f ${KEYDIR}/${KEYFILE} -N '' || \
			{ logger "WARNING:  Error generating ssh key" ; return 1; }
	fi
	logger "Setting ownership and permissions for '${KEYDIR}' and '${KEYDIR}/${KEYFILE}*'"
	log_output_of chown ${amanda_user}:${amanda_group} ${KEYDIR} ${KEYDIR}/${KEYFILE}* || \
		{ logger "WARNING:  Could not chown one of ${KEYDIR} or ${KEYFILE}"; return 1; }
	log_output_of chmod 0750 ${KEYDIR} || \
		{ logger "WARNING:  Could not fix permissions on ${KEYDIR}"; return 1; }
	log_output_of chmod 0600 ${KEYDIR}/${KEYFILE}* || \
		{ logger "WARNING:  Could not fix permissions on ${KEYFILE}"; return 1; }
}

create_profile() {
	# environment variables (~${amanda_user}/.profile)
	logger "Checking for '${AMANDAHOMEDIR}/.profile'."
	if [ ! -f ${AMANDAHOMEDIR}/.profile ] ; then
		log_output_of touch ${AMANDAHOMEDIR}/.profile || \
			{ logger "WARNING:  Could not create .profile" ; return 1; }
	fi
}

check_profile(){
    logger "Checking for ${SBINDIR} in path statement."
    if [ -z "`grep PATH.*${SBINDIR} ${AMANDAHOMEDIR}/.profile`" ] ; then
        echo "PATH=\"\$PATH:${SBINDIR}\"" >>${AMANDAHOMEDIR}/.profile || \
            { logger "WARNING:  Could not append to .profile" ; return 1; }
        echo "export PATH" >>${AMANDAHOMEDIR}/.profile
    fi
    case $os in
      SunOS)
        sun_paths=/opt/csw/bin:/usr/ucb
        if [ -z "`grep PATH ${AMANDAHOMEDIR}/.profile | grep ${sun_paths}`" ] ; then
            echo "PATH=\"$PATH:${SBINDIR}:${sun_paths}\"" >>${AMANDAHOMEDIR}/.profile
        fi
      ;;
    esac
    logger "Setting ownership and permissions for '${AMANDAHOMEDIR}/.profile'"
    log_output_of chown ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.profile || \
        { logger "WARNING:  Could not chown .profile." ; return 1; }
    log_output_of chmod 0640 ${AMANDAHOMEDIR}/.profile || \
        { logger "WARNING:  Could not fix permissions on .profile" ; return 1; }
}

install_client_conf() {
    # Install client config
    if [ "$os" = "SunOS" ] ; then
        install="install -m 0600 -u ${amanda_user} -g ${amanda_group}"
    else
        install="install -m 0600 -o ${amanda_user} -g ${amanda_group}"
    fi
    logger "Checking '${SYSCONFDIR}/amanda/amanda-client.conf' file."
    if [ ! -f ${SYSCONFDIR}/amanda/amanda-client.conf ] ; then
        logger "Installing amanda-client.conf."
        log_output_of ${install} ${AMANDAHOMEDIR}/example/amanda-client.conf \
            ${SYSCONFDIR}/amanda/ || \
                { logger "WARNING:  Could not install amanda-client.conf" ; return 1; }
    else
        logger "Note: ${SYSCONFDIR}/amanda/amanda-client.conf exists. Please check ${AMANDAHOMEDIR}/example/amanda-client.conf for updates."
    fi
}

create_ampassphrase() {
	# install am_passphrase file to server
	logger "Checking '${AMANDAHOMEDIR}/.am_passphrase' file."
	if [ ! -f ${AMANDAHOMEDIR}/.am_passphrase ] ; then
		logger "Create '${AMANDAHOMEDIR}/.am_passphrase' file."
		log_output_of touch ${AMANDAHOMEDIR}/.am_passphrase || \
			{ logger "WARNING:  Could not create .am_passphrase." ; return 1; }
		phrase=`echo $RANDOM | md5sum | awk '{print $1}'` || \
			{ logger "WARNING:  Error creating pseudo random passphrase." ; return 1; }
		echo ${phrase} >>${AMANDAHOMEDIR}/.am_passphrase

		log_output_of chown ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.am_passphrase || \
			{ logger "WARNING:  Could not chown .am_passphrase" ; return 1; }
		log_output_of chmod 0600 ${AMANDAHOMEDIR}/.am_passphrase || \
			{ logger "WARNING:  Could not fix permissions on .am_passphrase" ; return 1; }
	fi

}

create_amtmp() {
	# Check for existence of and permissions on ${AMTMP}
	logger "Checking for '${AMTMP}' dir."
	if [ ! -d ${AMTMP} ]; then
		logger "Create '${AMTMP}' dir."
		log_output_of mkdir ${AMTMP} || \
			{ logger "WARNING:  Could not create ${AMTMP}." ; return 1; }
		log_output_of chown ${amanda_user}:${amanda_group} ${AMTMP} || \
			{ logger "WARNING:  Could not chown ${AMTMP}" ; return 1; }
	fi
}

# ------------- End Post Install Functions -----------------
