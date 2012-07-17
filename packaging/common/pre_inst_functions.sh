#!/bin/sh
# These functions are included by various different installers.

# We assume that the following variables are defined in the main script:
# amanda_user: the amanda account username
# amanda_group: the amanda account's group
# amandahomedir: a directory to use as amanda's home
# deb_uid: sepecific uid amanda_user should get on deb style systems.
# dist: used on linux for the distro.
# LOGFILE: a log file we append to
# os: Linux, Darwin, SunOS, `uname`...
# wanted_shell:  Linux/SunOS/OSX all keep them in different places
# SYSCONFDIR: location of system config files (ie, /etc)
# LOGDIR: logging directory for amanda

# These variables are defined, but null to allow checking names
checked_uid=
export checked_uid
# PreInstall Functions

create_user() {
    case $os in
        Linux|SunOS)
            # Create the amanda user with a specific uid on deb systems
            if [ "x${dist}" = "xDebian" ] || [ "x${dist}" = "xUbuntu" ] ; then
                uid_flag="-u ${deb_uid}"
            else
                uid_flag=
            fi
            logger "Checking for user: ${amanda_user}"
            # we want the output of id, but if the user doesn't exist,
            # sterr output just confuses things
            ID=`id ${amanda_user} 2> /dev/null | sed 's/uid=\([0-9]*\).*/\1/'`
            if [ "${ID}x" = "x" ] ; then
                logger "Adding ${amanda_user}."
                log_output_of useradd -c "Amanda" \
                            -g ${amanda_group} \
                            ${uid_flag} \
                            -d ${AMANDAHOMEDIR} \
                            -s ${wanted_shell} ${amanda_user}  || \
                { logger "WARNING:  Could not create user ${amanda_user}. Installation will fail." ; return 1 ; }
                logger "Created ${amanda_user} with blank password."

                if [ "x$os" = "xLinux" ] && [ "x${dist}" != "xSuSE" ]; then
                    # Lock the amanda account until admin sets password
                    log_output_of passwd -l ${amanda_user} && { \
                        logger "${info_create_user_success}"
                        logger "${info_unlock_account}"
                    } || { \
                        logger "${warning_user_passwd}"; }
                fi
                if [ "x$os" = "xSunOS" ]; then
                    r=`uname -r`
                    case $r in
                        5.8|5.9) log_output_of passwd -l ${amanda_user} ;;
                        5.10) # Special login-lock, while allowing execution.
                            log_output_of passwd -N ${amanda_user} && { \
                                logger "${info_create_user_success_sol}"
                                logger "${info_unlock_account}"
                            } || { \
                                logger "${warning_user_passwd}"; }
                        ;;
                    esac
                fi

            else
                # The user already existed
                logger "The Amanda user '${amanda_user}' exists on this system."
            fi
        ;;
        Darwin) : #TODO
        ;;
    esac
}

add_group() {
    # First, try to add the group, detect via return code if it
    # already exists. Then add ${amanda_user} to the group without
    # checking if account is already a member.  (check_user should
    # already have been called).
    #
    # Works on linux and solaris.  OSX has different tools.

    [ "x${1}" = "x" ] && { logger "Error: first argument was not a group to add." ; return 1 ; }
    group_to_add=${1}
    log_output_of groupadd ${group_to_add}
    rc=$?
    # return of 0 means group was added; 9 means group existed.
    if [ $rc -eq 0 ] || [ $rc -eq 9 ]; then
        logger "Adding ${amanda_user} to ${group_to_add}"
        # Generate a comma separated list of existing supplemental groups.
        # Linux prefaces output with '<username> : '.
        existing_sup_groups=`groups ${amanda_user}|sed 's/.*: //;s/ /,/g'`
        # usermod append is -A on Suse, all other linux use -a, and
        # -A means something else entirely on solaris. So we just use
        # -G, and append a list of the current groups from id.
        # So far, all linux distros have usermod
        log_output_of usermod -G ${existing_sup_groups},${group_to_add} ${amanda_user} || { \
            logger "Nonfatal ERROR: Failed to add ${group_to_add}."
            logger "${error_group_member}" ; return 1 ; }
    else
        logger "Error: groupadd failed in an unexpected way. return code='$rc'"
        return 1
    fi
}


# All check_user_* functions check various parameters of ${amanda_user}'s
# account. Return codes:
# 0 = success
# 1 = error
# 2 = usage or other error.  more info will be logged

check_user_group() {
    # checks the system group file for ${amanda_user}'s membership in
    # the group named $1.
    err=0
    [ "x" = "x$1" ] && { logger "check_user_group: no group given"; return 1; }
    logger "Verify ${amanda_user}'s primary group = $1 "
    # Check if the group exists, disregarding membership.
    group_entry=`grep "^${2}" ${SYSCONFDIR}/group 2> /dev/null`
    if [ ! "x" = "x${group_entry}" ]; then
        # Assume the user exists, and check the user's primary group.
        GROUP=`id ${amanda_user} 2> /dev/null |\
            cut -d " " -f 2 |\
            sed 's/.*gid=[0-9]*(\([^ ()]*\))/\1/'`
        if [ ! "x${GROUP}" = "x${1}" ] ; then
            logger "${amanda_user} not a member of ${1}"
            err=1
        fi
    else
        logger "User's primary group '${1}' does not exist"
        err=1
    fi
    return $err
}

check_user_supplemental_group() {
    # Checks for the group ${1}, and adds ${amanda_user} if missing.
    # Other groups are preserved.
    err=0
    [ "x" = "x$1" ] && { logger "check_user_supplemental_group: no supplemental group given"; return 1; }
    sup_group=${1}
    logger "Verify ${amanda_user} is a member of ${sup_group}."
    # First, check if the supplementary group exists.
    sup_group_entry=`grep "${sup_group}" ${SYSCONFDIR}/group 2>/dev/null`
    if [ ! "x" = "x${sup_group_entry}" ]; then
        SUP_MEM=`echo ${sup_group_entry} | cut -d: -f4`
        # Check if our user is a member.
        case ${SUP_MEM} in
            *${amanda_user}*) : ;;
            *)
            logger "${amanda_user} is not a member of supplemental group ${sup_group}."
            err=1
            ;;
        esac
    else
        logger "Supplemental group ${sup_group} does not exist"
        err=1
    fi
    return $err
}

check_user_shell() {
    # Confirms the passwd file's shell field for ${amanda_user} is $1
    [ "x" = "x$1" ] && { logger "check_user_shell: no shell given"; return 1; }
    wanted_shell=$1; export wanted_shell
    logger "Verify ${amanda_user}'s shell is ${wanted_shell}."
    real_shell=`grep "^${amanda_user}" ${SYSCONFDIR}/passwd | cut -d: -f7`
    export real_shell
    if [ ! "x${real_shell}" = "x${wanted_shell}" ] ; then
        logger "WARNING:  ${amanda_user} default shell= ${wanted_shell}"
        logger "WARNING: ${amanda_user} existing shell: ${real_shell}"
        logger "${warning_user_shell}"
        return 1
    fi
}

check_user_homedir() {
    # Confirm the passwd file's homedir field for ${amanda_user} is $1
    [ "x" = "x$1" ] && { logger "check_user_homedir: no homedir given"; return 1; }
    HOMEDIR=`grep "^${amanda_user}" ${SYSCONFDIR}/passwd | cut -d: -f6`
    if [ ! "x${HOMEDIR}" = "x${1}" ] ; then
        logger "${warning_user_homedir}"
        return 1
    fi
}

check_user_uid() {
    # Confirm that ${amanda_user}'s UID is $1.
    # Debian systems must use a specific UID
    [ "x" = "x$1" ] && { logger "check_user_uid: no uid given"; return 1; }
    ID=`id ${amanda_user} 2> /dev/null | sed 's/uid=\([0-9]*\).*/\1/'`
    if [ ! ${ID} -eq ${1} ] ; then
        checked_uid=${1}; export checked_uid
        logger "${warning_user_uid_debian}"
        return 1
    fi
}

check_homedir() {
	# Checks that the homedir has correct permissions and belongs to correct
	# user.  Uses $amanda_user and  $amanda_group.
	if [ -d ${AMANDAHOMEDIR} ] ; then
	    OWNER_GROUP=`ls -dl ${AMANDAHOMEDIR} | awk '{ print $3" "$4; }'`
	    [ "x$OWNER_GROUP" = "x${amanda_user} ${amanda_group}" ] || \
		logger "${warning_homedir_owner}"
	    return $?
	else
	    logger "homedir ${AMANDAHOMEDIR} does not exist."
	    return 1
	fi
}

create_homedir() {
	# Creates the homedir, if necessary, and fixes ownership.
	if [ ! -d ${AMANDAHOMEDIR} ] ; then
		logger "Creating ${AMANDAHOMEDIR}"
		log_output_of mkdir -p -m 0750 ${AMANDAHOMEDIR} ||
			{ logger "WARNING:  Could not create ${AMANDAHOMEDIR}" ; return 1 ; }
	fi
	log_output_of chown -R ${amanda_user}:${amanda_group} ${AMANDAHOMEDIR} ||
		{ logger "WARNING:  Could not chown ${AMANDAHOMEDIR}" ; return 1 ; }
}

create_logdir() {
    if [ -d ${LOGDIR} ] || [ -f ${LOGDIR} ] ; then
        logger "Found existing ${LOGDIR}"
        log_output_of mv ${LOGDIR} ${LOGDIR}.save || \
            { logger "WARNING:  Could not backup existing log directory: ${LOGDIR}" ; return 1 ; }
    fi
    logger "Creating ${LOGDIR}."
    log_output_of mkdir -p -m 0750 ${LOGDIR} || \
        { logger "WARNING:  Could not create ${LOGDIR}" ; return 1 ; }
    if [ -d ${LOGDIR}.save ] || [ -f ${LOGDIR}.save ] ; then
        # Move the saved log into the logdir.
        log_output_of mv ${LOGDIR}.save ${LOGDIR}
    fi
    log_output_of chown -R ${amanda_user}:${amanda_group} ${LOGDIR} || \
        { logger "WARNING:  Could not chown ${LOGDIR}" ; return 1 ; }
}

# Info, Warning, and Error strings used by the installer

info_create_user_success="NOTE: Set a password and unlock the ${amanda_user} account before
 running Amanda."

info_create_user_success_sol="NOTE: Account is set as non-login.  If interactive login
is required: a password must be set, and the account activated for login."

info_unlock_account=" The superuser can unlock the account by running:

 # passwd -u ${amanda_user}
"

info_existing_installs="Pre-existing Amanda installations must confirm:
  -${SYSCONFDIR}/amanda/* should have 'dumpuser' set to '${amanda_user}'.
  -${AMANDAHOMEDIR}/.amandahosts on client systems should allow connections by
   '${amanda_user}'."

warning_user_password="WARNING:  '${amanda_user}' no password. An error occured when locking the
 account.  SET A PASSWORD NOW:

 # passwd ${amanda_user}"

error_group_member="Nonfatal ERROR:  Amanda will not run until '${amanda_user}' is a member the
 preceeding group.  Install will continue..."

warning_user_shell="WARNING: The user '${amanda_user}' has a non-default shell. Other shells have not been tested."

warning_user_homedir="WARNING: The user '${amanda_user}' must have its home directory set to
'${AMANDAHOMEDIR}' Please correct before using Amanda."

warning_user_uid_debian="WARNING: Debian packages were built assuming that ${amanda_user}
uid = ${checked_uid}.  The uid of ${amanda_user} is different on this system.  Files
owned by ${checked_uid} must be chowned to ${amanda_user}."

warning_homedir_owner="WARNING: The ${amanda_user}'s home directory,'${AMANDAHOMEDIR}' ownership must be changed to '${amanda_user}:${amanda_group}'. "

# --------------- End included Functions -----------------
