#!/usr/bin/env bash
setopt=$-
set +xv

# ------------- Begin Post Removal Functions -----------------
# Functions used for post removal actions

rm_xinetd() {
    # Remove the xinetd configuration file $1.  check_xinetd should be
    # executed first.
    logger "Removing xinetd configuration $1"
    log_output_of rm /etc/xinetd.d/$1
}

rm_inetd() {
    # Remove amanda entries from inetd.conf
    logger "Removing amanda's inetd.conf entries"
    log_output_of sed -i "/^amanda .* amandad/d"
}

rm_user() {
    # Delete the user provided as first parameter ($1)
    logger "Deleting user: $1"
    log_output_of userdel $1
}

rm_64b_amandad() {
    # Remove 64b amandad soft link
    logger "Removing 64b amandad soft link"
    log_output_of rm /usr/lib64/amanda/amandad
}

# ------------- End Post Removal Functions -----------------
set -${setopt/s}
