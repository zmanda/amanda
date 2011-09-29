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
            if [ ${dist} = "Debian" -o ${dist} = "Ubuntu" ] ; then
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
                            -s /bin/bash ${amanda_user}  || \
                { logger "WARNING:  Could not create user ${amanda_user}. Installation will fail." ; return 1 ; }
                r=`uname -r`
                if [ "$os" = "Linux"] && [ ${dist} != "SuSE" ]; then
                    # Lock the amanda account until admin sets password
                    log_output_of passwd -l ${amanda_user} || { \
                        logger "${warning_user_passwd}"; }
                fi
                if [ "$os" = "SunOS" ]; then
                    case $r in
                        5.8|5.9) log_output_of passwd -l ${amanda_user};;
                        5.10) # Special login-lock, while allowing execution.
                            log_ouptut_of passwd -N ${amanda_user} || { \
                                logger "${warning_user_passwd}"; }
                        ;;
                    esac
                fi

                logger "${info_create_user_success}"
            else
                # The user already existed
                logger "${info_user_params}"
            fi
        ;;
        Darwin) : #TODO
        ;;
    esac
}

add_group() {
    # Try to add the group, detect via return code if it already exists.
    # This works on linux and solaris...
    log_output_of groupadd ${1}
    # return of 0 means group was added; 9 means group existed.
    if [ $? = "0" ] || [ $? = "9" ]; then
        logger "Adding ${amanda_user} to ${1}"
        case $os in
            Linux) um_flags="-a -G";;
            # Solaris does not have -a flag.
            SunOS) um_flags="-G `groups ${amanda_user}`";;
        esac
        # So far, all linux distros have usermod
        log_output_of usermod -a -G ${1} ${amanda_user} || \
            { logger "${error_group_member}" ; return 1 ; }
    else
        logger "Error: groupadd failed in an unexpected way."
        return 1
    fi
}

check_user() {
    # Check parameters of ${amanda_user}'s account.
    # $1= user field $2= value to check
    # group, shell, homedir, UID are valid for $1.
    # group:  checks the system group file for ${amanda_user}'s
    #	 membership in the group named $2.
    # shell:  confirms the passwd file's shell field for
    #	 ${amanda_user} is $2
    # homedir: confirm the passwd file's homedir field for
    #	 ${amanda_user} is $2
    # UID: confirm that ${amanda_user}'s UID is $2.
    #
    # Extra information about the failed check is written to the log.
    #
    # Return codes:
    # 0 = success
    # 1 = error
    # 2 = usage error
    err=0
    if [ ! $# -eq 2 ]; then
	echo "check_user(): Wrong number of parameters"
	return 2
    fi
    logger "Verify ${amanda_user}'s $1 = $2 "
    case $1 in
	"group")
	    # Check if the group exists, disregarding membership.
	    if `grep "^${2}" ${SYSCONFDIR}/group > /dev/null` ; then
		# Assume the user exists, and check the user's primary group.
		GROUP=`id ${amanda_user} 2> /dev/null | sed 's/.*gid=[0-9]*(\([^ ()]*\))/\1/'`
		if [ ! "x${GROUP}" = "x${2}" ] ; then
		    logger "${amanda_user} not a member of ${2}"
		    err=1
		fi
	    else
		logger "User group '${2}' does not exist"
		err=1
	    fi
	;;
        "group-sup*")
            # Check if a supplementary group exists.
	    SUP_MEM=`awk -F: "\$1 ~ ${2} { print \$4; }" 2> /dev/null`
            if [ -n "$SUP_MEM" ] ; then
                # Check if our user is a member.
                if echo "${SUP_MEM}"|grep "${amanda_user}" &> /dev/null ; then
                    :
                else
                    logger "${amanda_user} is not a member of supplemental group ${2}."
                    err=1
                fi
            else
                logger "Supplemental group ${2} does not exist"
                err=1
            fi
            ;;
	"shell")
	    SHELL=`grep "^${amanda_user}" ${SYSCONFDIR}/passwd | cut -d: -f7`
            wanted_shell=$2; export wanted_shell
	    if [ ! "x${SHELL}" = "x${2}" ] ; then
		logger "${warning_user_shell}"
		err=1
	    fi
	;;
	"homedir")
	    HOMEDIR=`grep "^${amanda_user}" ${SYSCONFDIR}/passwd | cut -d: -f6`
	    if [ ! "x${HOMEDIR}" = "x${2}" ] ; then
		logger "${warning_user_homedir}"
		err=1
	    fi
	;;
	"UID")
	    # Debian systems must use a specific UID
	    ID=`id ${amanda_user} 2> /dev/null | sed 's/uid=\([0-9]*\).*/\1/'`
	    if [ ! "${ID}" -eq "${2}" ] ; then
		checked_uid=${2}; export checked_uid
		logger "${warning_user_uid_debian}"
		err=1
	    fi
	;;
	*)
	    echo "check_user(): unknown user parameter."
	    err=2
	;;
    esac
	
    return $err
}

check_homedir() {
	# Checks that the homedir has correct permissions and belongs to correct
	# user.  Uses $amanda_user and  $amanda_group.
	if [ -d ${AMANDAHOMEDIR} ] ; then
	    OWNER_GROUP=`ls -dl ${AMANDAHOMEDIR} | awk '{ print $3" "$4; }'`
	    [ "$OWNER_GROUP" = "${amanda_user} ${amanda_group}" ] || \
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

info_create_user_success="The '${amanda_user}' user account has been successfully created. 
 Furthermore, the account has been automatically locked for you for security 
 purposes.  Once a password for the '${amanda_user}' account has been set, 
 the user can be unlocked by issuing the following command as root.: 

 # passwd -u ${amanda_user}

 If this is not a new installation of Amanda and you have pre-existing Amanda
 configurations in ${SYSCONFDIR}/amanda you should ensure that 'dumpuser'
 is set to '${amanda_user}' in those configurations.  Additionally, you
 should ensure that ${AMANDAHOMEDIR}/.amandahosts on your client systems
 is properly configured to allow connections for the user '${amanda_user}'." 

warning_user_password="!!! WARNING! WARNING! WARNING! WARNING! WARNING! !!!
!!!                                                             !!!
!!!  The '${amanda_user}' user account for this system has been   !!!
!!!  created, however the user has no password set. For         !!!
!!!  security purposes this account is normally locked after    !!!
!!!  creation.  Unfortunately, when locking this account an     !!!
!!!  error occurred.  To ensure the security of your system,    !!!
!!!  you should set a password for the user account             !!!
!!!  '${amanda_user}' immediately!  To set such a password,     !!!
!!!  please issue the following command:                        !!!
!!!                                                             !!!
!!!   # passwd ${amanda_user}                                   !!!
!!!                                                             !!!
!!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING!       !!!"

info_user_params="The Amanda backup software is configured to operate as the user 
 '${amanda_user}'.  This user exists on your system and has not been modified. 
 To ensure that Amanda functions properly, please see that the following 
 parameters are set for that user:
 SHELL:          /bin/bash
 HOME:           ${AMANDAHOMEDIR} 
 Default group:  ${amanda_group}"

error_group_member="!!!      Nonfatal ERROR.     Nonfatal ERROR.     !!!
!!! user '${amanda_user}' is not part of the '${amanda_group}' group,  !!!
!!! Amanda will not run until '${amanda_user}' is a member of '${amanda_group}'.  !!!
!!!    Nonfatal ERROR.    Nonfatal ERROR.    Nonfatal Error.    !!!"

warning_user_shell="WARNING: The user '${amanda_user}' has a non-default shell.
the default shell is ${wanted_shell}.  Other shells have not been tested."

warning_user_homedir="!!! WARNING! WARNING! WARNING! WARNING! WARNING! !!!
!!! The user '${amanda_user}' must have its home directory set to   !!!
!!! '${AMANDAHOMEDIR}' Please correct before using Amanda     !!!
!!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING!  !!!"

warning_user_uid_debian="!!! WARNING! WARNING! WARNING! WARNING! WARNING! !!!
!!! Debian packages were built assuming that ${amanda_user} !!!
!!! uid = ${checked_uid}.  The uid of ${amanda_user} is different     !!!
!!! different on this system.  Files owned by ${checked_uid} must    !!!
!!! be chowned to ${amanda_user}.                           !!!
!!! WARNING! WARNING! WARNING! WARNING! WARNING! WARNING!  !!!"

warning_homedir_owner="!!! Please ensure that the directory '${AMANDAHOMEDIR}' is owned by !!!
!!! the user '${amanda_user}' and group '${amanda_group}'. !!!"

# --------------- End included Functions -----------------
