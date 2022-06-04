#!/bin/sh
#
# script "encrypt", use with server_encrypt "/whatever/encrypt":

AMANDA_HOME=~amanda
PASSPHRASE=$AMANDA_HOME/.am_passphrase    # required
RANDFILE=$AMANDA_HOME/.rnd
export RANDFILE

if [ "$1" = -d ]; then
    /usr/bin/openssl enc -pbkdf2 -d -aes-256-ctr -salt -pass fd:3 3< "${PASSPHRASE}"
else
    /usr/bin/openssl enc -pbkdf2 -e -aes-256-ctr -salt -pass fd:3 3< "${PASSPHRASE}"
fi
