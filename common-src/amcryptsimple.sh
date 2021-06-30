#!/bin/bash
#
# Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
# Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
# Contact information: Carbonite Inc., 756 N Pastoria Ave
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
#


#
# save the stdout and close it in case it is a failed pipe
#
exec 4>&1
exec >&2

AM_PASS=~@CLIENT_LOGIN@/.am_passphrase
export GNUPGHOME=~@CLIENT_LOGIN@/.gnupg

die() {
    echo "$@" >&2
    exit 100
}

gpg_home_agent_kill() {
    declare -a inodes

    # sick and wrong find matching process by socket path sockets
    inodes=($(sed -e '\, *'"$1/S.gpg-agent"',!d' -e 's, */.*$,,' -e 's,^.* ,,' /proc/net/unix))
    inodes=("${inodes[@]/#/-o -lname socket:\\[}")
    inodes=("${inodes[@]/%/]}")

    # search dirs of "proc" and "fd" and "<digits>" only...
    find 2>/dev/null /proc -maxdepth 3 \
       -name proc -o \
       -name fd -o \
       -name \*\[^0-9]\* -prune -o \
       -type l \( -false ${inodes[*]} \) -printf '%h\n' -quit |
     while read fdpath; do
        fdpath=${fdpath%/fd}
        kill ${fdpath#/proc/}
     done
}


gpg_options="--symmetric --cipher-algo AES256 --compress-level 0"
if [ x"$1" = x-d ]; then
   gpg_options="--decrypt --ignore-mdc-error"
   shift
fi

[ $# != 0 ] &&
   die "Usage: $0 [-d]"

[ -s "$AM_PASS" ] ||
   die "ERROR: secret key ~@CLIENT_LOGIN@/.am_passphrase not found";

[ -d $GNUPGHOME ] ||
   die "ERROR: GnuPG directory ~@CLIENT_LOGIN@/.gnupg directory not found"

gpgagent_cmd=$(command -v gpg-agent)
[ -x "$gpgagent_cmd" ] &&
   gpgagent_cmd="$gpgagent_cmd --quiet --daemon"

gpg_path=$(command -v gpg2 || command -v gpg)
[ -x $gpg_path ] ||
   die "ERROR: GnuPG executible (gpg or gpg2) is not found"

gpg_home_agent_kill $GNUPGHOME
( $gpgagent_cmd $gpg_path --batch --no-permission-warning --no-tty --quiet --passphrase-fd 3 $gpg_options 3<"$AM_PASS" >&4 )
r=$?
gpg_home_agent_kill $GNUPGHOME

[ $r -ge 128 ] &&
   die "ERROR: gpg died with signal=$(( r - 128 ))"

exit $r
