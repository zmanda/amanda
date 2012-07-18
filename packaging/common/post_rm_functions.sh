#!/bin/sh

# ------------- Begin Post Removal Functions -----------------
# Functions used for post removal actions

rm_xinetd() {
    # Remove the xinetd configuration file $1.  check_xinetd should be
    # executed first.
    logger "Removing xinetd configuration $1"
    log_output_of rm ${SYSCONFDIR}/xinetd.d/$1
}

rm_inetd() {
    # Remove amanda entries from inetd.conf
    logger "Removing amanda's inetd.conf entries"
    log_output_of sed -i "/^amanda .* amandad/d"
}

remove_smf() {
    # Remove the amanda service from solaris style service framework.
    ret=0; export ret
    logger "Removing amanda's smf entry"
    log_output_of svcadm disable $1 || { ret=1; }
    log_output_of svccfg delete $1 || { ret=1; }
    return $ret
}

rm_user() {
    # Delete the user provided as first parameter ($1)
    logger "Deleting user: $1"
    log_output_of userdel $1
}

# ------------- End Post Removal Functions -----------------
