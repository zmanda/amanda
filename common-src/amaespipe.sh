#! @SHELL@
#
# Copyright (c) 2007-2012 Zmanda Inc.  All Rights Reserved.
# 
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 as published
# by the Free Software Foundation.
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
# Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
# Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
#

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

# add sbin and ucb dirs
PATH="$PATH:/usr/sbin:/sbin:/usr/ucb"
export PATH

# wrapper script to use aespipe
# based on bz2aespipe distributed by aespipe from 
# http://loop-aes.sourceforge.net/
# FILE FORMAT
# 10 bytes: constant string 'bz2aespipe'
# 10 bytes: itercountk digits
# 1 byte: '0' = AES128, '1' = AES192, '2' = AES256
# 1 byte: '0' = SHA256, '1' = SHA384, '2' = SHA512, '3' = RMD160
# 24 bytes: random seed string
# remaining bytes are aespipe encrypted

# These definitions are only used when encrypting.
# Decryption will autodetect these definitions from archive.
ENCRYPTION=AES256
HASHFUNC=SHA256
ITERCOUNTK=100
WAITSECONDS=1
AMANDA_HOME=~@CLIENT_LOGIN@
GPGKEY="$AMANDA_HOME/.gnupg/am_key.gpg"
FDNUMBER=3

if test x$1 = x-d ; then
    # decrypt
    n=`/bin/dd bs=10 count=1 2> /dev/null | tr -d -c 0-9a-zA-Z`
    if test x${n} != xbz2aespipe ; then
        echo "bz2aespipe: wrong magic - aborted" >/dev/tty
        exit 1
    fi
    itercountk=`/bin/dd bs=10 count=1 2> /dev/null | tr -d -c 0-9`
    if test x${itercountk} = x ; then itercountk=0; fi
    n=`/bin/dd bs=1 count=1 2> /dev/null | tr -d -c 0-9`
    encryption=AES128
    if test x${n} = x1 ; then encryption=AES192; fi
    if test x${n} = x2 ; then encryption=AES256; fi
    n=`/bin/dd bs=1 count=1 2> /dev/null | tr -d -c 0-9`
    hashfunc=SHA256
    if test x${n} = x1 ; then hashfunc=SHA384; fi
    if test x${n} = x2 ; then hashfunc=SHA512; fi
    if test x${n} = x3 ; then hashfunc=RMD160; fi
    seedstr=`/bin/dd bs=24 count=1 2> /dev/null | tr -d -c 0-9a-zA-Z+/`
    aespipe -K ${GPGKEY} -p ${FDNUMBER} -e ${encryption} -H ${hashfunc} \
	-S ${seedstr} -C ${itercountk} -d
else
    # encrypt
    echo -n bz2aespipe
    echo ${ITERCOUNTK} | awk '{printf "%10u", $1;}'
    n=`echo ${ENCRYPTION} | tr -d -c 0-9`
    aesstr=0
    if test x${n} = x192 ; then aesstr=1; fi
    if test x${n} = x256 ; then aesstr=2; fi
    n=`echo ${HASHFUNC} | tr -d -c 0-9`
    hashstr=0
    if test x${n} = x384 ; then hashstr=1; fi
    if test x${n} = x512 ; then hashstr=2; fi
    if test x${n} = x160 ; then hashstr=3; fi
    seedstr=`head -c 18 /dev/urandom | uuencode -m - | head -n 2 | tail -n 1`
    echo -n ${aesstr}${hashstr}${seedstr}
    aespipe -K ${GPGKEY} -p ${FDNUMBER} -e ${ENCRYPTION} -H ${HASHFUNC} \
	-S ${seedstr} -C ${ITERCOUNTK} -w ${WAITSECONDS}
fi
exit 0
