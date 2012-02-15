#!@SHELL@
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Amanda, The Advanced Maryland Automatic Network Disk Archiver
#
# Permission to use, copy, modify, distribute, and sell this software and its
# documentation for any purpose is hereby granted without fee, provided that
# the above copyright notice appear in all copies and that both that
# copyright notice and this permission notice appear in supporting
# documentation, and that the name of U.M. not be used in advertising or
# publicity pertaining to distribution of the software without specific,
# written prior permission.  U.M. makes no representations about the
# suitability of this software for any purpose.  It is provided "as is"
# without express or implied warranty.
#
# U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
# BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
# OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# Copyright (c) 2006  Ben Slusky <sluskyb@paranoiacs.org>


# amcrypt-ossl-asym.sh - asymmetric crypto helper using OpenSSL
# Usage: amcrypt-ossl-asym.sh [-d]
#

# Keys can be generated with the standard OpenSSL commands, e.g.:
#
# $ openssl genrsa -aes128 -out backup-privkey.pem 1024
# Generating RSA private key, 1024 bit long modulus
# [...]
# Enter pass phrase for backup-privkey.pem: <ENTER YOUR PASS PHRASE>
# Verifying - Enter pass phrase for backup-privkey.pem: <ENTER YOUR PASS PHRASE>
#
# $ openssl rsa -in backup-privkey.pem -pubout -out backup-pubkey.pem
# Enter pass phrase for backup-privkey.pem: <ENTER YOUR PASS PHRASE>
# Writing RSA key
#

prefix="@prefix@"
exec_prefix="@exec_prefix@"
sbindir="@sbindir@"
amlibexecdir="@amlibexecdir@"
. "${amlibexecdir}/amanda-sh-lib.sh"

# change these as needed
OPENSSL=			# whatever's in $PATH
CIPHER=aes-256-cbc		# see `openssl help` for more ciphers
AMANDA_HOME=~@CLIENT_LOGIN@
RANDFILE=$AMANDA_HOME/.rnd
export RANDFILE
PASSPHRASE=$AMANDA_HOME/.am_passphrase	# optional
PRIVKEY=$AMANDA_HOME/backup-privkey.pem
PUBKEY=$AMANDA_HOME/backup-pubkey.pem

# where might openssl be?
PATH=/bin:/usr/bin:/usr/local/bin:/usr/ssl/bin:/usr/local/ssl/bin:/opt/csw/bin
export PATH
MAGIC='AmAnDa+OpEnSsL'
ME=`basename "$0"`
WORKDIR="/tmp/.${ME}.$$"


# first things first
if [ -z "${OPENSSL:=`which openssl`}" ]; then
	echo `_ '%s: %s not found' "${ME}" "openssl"` >&2
	exit 1
elif [ ! -x "${OPENSSL}" ]; then
	echo `_ "%s: can't execute %s (%s)" "${ME}" "openssl" "${OPENSSL}"` >&2
	exit 1
fi

if [ -n "${PASSPHRASE}" ]; then
	# check the openssl version. if it's too old, we have to handle
	# the pass phrase differently.
	OSSL_VERSION=`eval \"${OPENSSL}\" version |cut -d\  -f2`
	case "${OSSL_VERSION}" in
	 ''|0.[0-8].*|0.9.[0-6]*|0.9.7|0.9.7[a-c]*)
		echo `_ '%s: %s is version %s' "${ME}" "${OPENSSL}" "${OSSL_VERSION}"` >&2
		echo `_ '%s: Using pass phrase kluge for OpenSSL version >=0.9.7d' "${ME}"` >&2
		PASS_FROM_STDIN=yes
		;;
	esac
fi

mkdir -m 700 "${WORKDIR}"
if [ $? -ne 0 ]; then
	echo `_ '%s: failed to create temp directory' "${ME}"` >&2
	exit 1
fi
# ignore SIGINT
trap "" 2
trap "rm -rf \"${WORKDIR}\"" 0 1 3 15

# we'll need to pad the datastream to a multiple of the cipher block size
# prior to encryption and decryption. 96 bytes (= 768 bits) should be good
# for any cipher.
pad() {
	perl -pe 'BEGIN { $bs = 96; $/ = \8192 } $nbytes = ($nbytes + length) % $bs; END { print "\0" x ($bs - $nbytes) }'
}

encrypt() {
	# generate a random printable cipher key (on one line)
	echo `"${OPENSSL}" rand -base64 80` >"${WORKDIR}/pass"

	# encrypt the cipher key using the RSA public key
	"${OPENSSL}" rsautl -encrypt -in "${WORKDIR}/pass" -out "${WORKDIR}/pass.ciphertext" -pubin -inkey "${PUBKEY}" -pkcs
	[ $? -eq 0 ] || return 1

	# print magic
	printf "%s" "${MAGIC}"

	# print the encrypted cipher key, preceded by size
	ls -l "${WORKDIR}/pass.ciphertext" | awk '{ printf("%-10d", $5) }'
	cat "${WORKDIR}/pass.ciphertext"

	# encrypt data using the cipher key and print
	pad | "${OPENSSL}" enc "-${CIPHER}" -nopad -e -pass "file:${WORKDIR}/pass" -nosalt
	[ $? -eq 0 ] || return 1
}

decrypt() {
	# read magic
	magicsize=`printf "%s" "${MAGIC}" | wc -c | sed 's/^ *//'`
	magic=`dd bs=$magicsize count=1 2>/dev/null`
	if [ "$magic" != "${MAGIC}" ]; then
		echo `_ '%s: bad magic' "${ME}"` >&2
		return 1
	fi

	# read size of encrypted cipher key
	n=`dd bs=10 count=1 2>/dev/null`
	[ $n -gt 0 ] 2>/dev/null
	if [ $? -ne 0 ]; then
		echo `_ '%s: bad header' "${ME}"` >&2
		return 1
	fi

	# read the encrypted cipher key
	dd "of=${WORKDIR}/pass.ciphertext" bs=$n count=1 2>/dev/null

	# decrypt the cipher key using the RSA private key
	if [ "${PASS_FROM_STDIN}" = yes ]; then
		"${OPENSSL}" rsautl -decrypt -in "${WORKDIR}/pass.ciphertext" -out "${WORKDIR}/pass" -inkey "${PRIVKEY}" -pkcs < "${PASSPHRASE}"
	else
		"${OPENSSL}" rsautl -decrypt -in "${WORKDIR}/pass.ciphertext" -out "${WORKDIR}/pass" -inkey "${PRIVKEY}" ${PASSARG} -pkcs 3< "${PASSPHRASE}"
	fi
	[ $? -eq 0 ] || return 1

	# use the cipher key to decrypt data
	pad | "${OPENSSL}" enc "-${CIPHER}" -nopad -d -pass "file:${WORKDIR}/pass" -nosalt

	# N.B.: in the likely event that we're piping to gzip, the above command
	# may return a spurious error if gzip closes the output stream early.
	return 0
}

if [ "$1" = -d ]; then
	if [ -z "${PRIVKEY}" ]; then
		echo `_ '%s: must specify private key for decryption' "${ME}"` >&2
		exit 1
	elif [ ! -r "${PRIVKEY}" ]; then
		echo `_ "%s: can't read private key from %s" "${ME}" "${PRIVKEY}"` >&2
		exit 1
	fi

	if [ -n "${PASSPHRASE}" -a -e "${PASSPHRASE}" -a -r "${PASSPHRASE}" ]; then
		PASSARG='-passin fd:3'
	else
		PASSPHRASE=/dev/null
	fi

	decrypt
	if [ $? -ne 0 ]; then
		echo `_ '%s: decryption failed' "${ME}"` >&2
		exit 1
	fi
else
	if [ -z "${PUBKEY}" ]; then
		echo `_ '%s: must specify public key for encryption' "${ME}"` >&2
		exit 1
	elif [ ! -r "${PUBKEY}" ]; then
		echo `_ "%s: can't read public key from %s" "${ME}" "${PUBKEY}"` >&2
		exit 1
	fi

	encrypt
	if [ $? -ne 0 ]; then
		echo `_ '%s: encryption failed' "${ME}"` >&2
		exit 1
	fi
fi
