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
# encoder: either base64 or uuencode depending on the default for this platform

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

get_random_lines() {
    # Print $1 lines of random strings to stdout.

    [ "$1" ] && [ $1 -gt 0 ] || \
        { logger "Error: '$1' not valid number of lines" ; return 1 ; }
    lines=$1
    [ -f "${encoder}" ] || \
        { logger "Warning: Encoder '${encoder}' was not found.  Random passwords cannot be generated." ; return 1; }
    case ${encoder} in
        # "foo" is a required parameter that we throw away.
        *uuencode*) enc_cmd="${encoder} foo" ;;
        *base64*)   enc_cmd="${encoder}" ;;
    esac
    # Uuencode leaves a header (and footer) line, but base64 does not.
    # So we pad output with an extra line, and strip any trailing lines over
    # $lines
    pad_lines=`expr $lines + 1`
    # Increasing bs= is substantially faster than increasing count=.
    # The number of bytes needed to start line wrapping is implementation
    # specific.  base64. 60b > 1 base64 encoded line for all versions tested.
    block_size=`expr $pad_lines \* 60`
    # Head -c is not portable.
    dd bs=${block_size} count=1 if=/dev/urandom 2>/dev/null | \
            ${enc_cmd} | \
            head -$pad_lines | \
            tail -$lines || \
        { logger "Warning: Error generating random passphrase."; return 1; }
}

create_ampassphrase() {
    # install am_passphrase file to server
    logger "Checking '${AMANDAHOMEDIR}/.am_passphrase' file."
    if [ ! -f ${AMANDAHOMEDIR}/.am_passphrase ] ; then
        # Separate file creation from password creation to ease debugging.
        logger "Creating '${AMANDAHOMEDIR}/.am_passphrase' file."
        log_output_of touch ${AMANDAHOMEDIR}/.am_passphrase || \
            { logger "WARNING:  Could not create .am_passphrase." ; return 1; }
        phrase=`get_random_lines 1` || return 1 # Error already logged
        echo ${phrase} >>${AMANDAHOMEDIR}/.am_passphrase
    else
        logger "Info: ${AMANDAHOMEDIR}/.am_passphrase already exists."
    fi
    # Fix permissions for both new or existing installations.
    log_output_of chown ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.am_passphrase || \
        { logger "WARNING:  Could not chown .am_passphrase" ; return 1; }
    log_output_of chmod 0600 ${AMANDAHOMEDIR}/.am_passphrase || \
        { logger "WARNING:  Could not fix permissions on .am_passphrase" ; return 1; }
}

create_amkey() {
    [ -f ${AMANDAHOMEDIR}/.am_passphrase ] || \
        { logger "Error: ${AMANDAHOMEDIR}/.am_passphrase is missing, can't create amcrypt key."; return 1; }
    logger "Creating encryption key for amcrypt"
    if [ ! -f ${AMANDAHOMEDIR}/.gnupg/am_key.gpg ]; then
        # TODO: don't write this stuff to disk!
        get_random_lines 50 >${AMANDAHOMEDIR}/.gnupg/am_key || return 1

        GPG2=`which gpg2`
        if [ x"$GPG2" = x"" ]; then
           GPG=`which gpg`
           if [ x"$GPG" = x"" ]; then
                logger "Error: no gpg"
           else
                GPG_EXTRA=--no-use-agent
           fi
        else
           GPG_AGENT=`which gpg-agent`
           if [ x"$GPG_AGENT" = x"" ]; then
              echo "Error: no gpg-agent"
           else
              GPG="$GPG_AGENT --daemon --no-use-standard-socket -- $GPG2"
           fi
        fi

        exec 3<${AMANDAHOMEDIR}/.am_passphrase
        # setting homedir prevents some errors, but creates a permissions
        # warning. perms are fixed in check_gnupg.
        log_output_of $GPG --homedir ${AMANDAHOMEDIR}/.gnupg \
                --no-permission-warning \
                $GPG_EXTRA \
                --armor \
                --batch \
                --symmetric \
                --passphrase-fd 3 \
                --output ${AMANDAHOMEDIR}/.gnupg/am_key.gpg \
                ${AMANDAHOMEDIR}/.gnupg/am_key || \
            { logger "WARNING: Error encrypting keys." ;
              rm ${AMANDAHOMEDIR}/.gnupg/am_key;
              return 1; }
        # Be nice and clean up.
        exec 3<&-
    else
        logger "Info: Encryption key '${AMANDAHOMEDIR}/.gnupg/am_key.gpg' already exists."
    fi
    # Always try to delete unencrypted keys
    rm -f ${AMANDAHOMEDIR}/.gnupg/am_key
}

check_gnupg() {
    logger "Ensuring correct permissions for '${AMANDAHOMEDIR}/.gnupg'."
    log_output_of chown -R ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR}/.gnupg || \
        { logger "WARNING:  Could not chown .gnupg dir." ; return 1; }
    log_output_of chmod -R u=rwX,go= ${AMANDAHOMEDIR}/.gnupg || \
        { logger "WARNING:  Could not set permissions on .gnupg dir." ; return 1; }
    # If am_key.gpg and .am_passphrase already existed, we should check
    # if they match!
    if [ -f ${AMANDAHOMEDIR}/.gnupg/am_key.gpg ] && [ -f ${AMANDAHOMEDIR}/.am_passphrase ]; then

        GPG2=`which gpg2`
        if [ x"$GPG2" = x"" ]; then
           GPG=`which gpg`
           if [ x"$GPG" = x"" ]; then
                logger "Error: no gpg"
           else
                GPG_EXTRA=--no-use-agent
           fi
        else
           GPG_AGENT=`which gpg-agent`
           if [ x"$GPG_AGENT" = x"" ]; then
              echo "Error: no gpg-agent"
           else
              GPG="$GPG_AGENT --daemon --no-use-standard-socket -- $GPG2"
           fi
        fi

        exec 3<${AMANDAHOMEDIR}/.am_passphrase
        # Perms warning will persist because we are not running as ${amanda_user}
        log_output_of $GPG --homedir ${AMANDAHOMEDIR}/.gnupg \
                --no-permission-warning \
                $GPG_EXTRA \
                --batch \
                --decrypt \
                --passphrase-fd 3 \
                --output /dev/null \
                ${AMANDAHOMEDIR}/.gnupg/am_key.gpg || \
            { logger "WARNING: .am_passphrase does not decrypt .gnupg/am_key.gpg.";
                return 1;
            }
        # Be nice and clean up.
        exec 3<&-
    fi
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
