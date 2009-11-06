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


# amcrypt-ossl.sh - crypto helper using OpenSSL
# Usage: amcrypt-ossl.sh [-d]
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
PASSPHRASE=$AMANDA_HOME/.am_passphrase	# required

# where might openssl be?
PATH=/bin:/usr/bin:/usr/local/bin:/usr/ssl/bin:/usr/local/ssl/bin:/opt/csw/bin
export PATH
ME=`basename "$0"`

if [ -z "${OPENSSL:=`which openssl`}" ]; then
	echo `_ '%s: openssl not found' "${ME}"` >&2
	exit 1
elif [ ! -x "${OPENSSL}" ]; then
	echo `_ "%s: can't execute %s (%s)" "${ME}" "openssl" "${OPENSSL}"` >&2
	exit 1
fi

# we'll need to pad the datastream to a multiple of the cipher block size prior
# to encryption. 96 bytes (= 768 bits) should be good for any cipher.
pad() {
	perl -pe 'BEGIN { $bs = 96; $/ = \8192 } $nbytes = ($nbytes + length) % $bs; END { print "\0" x ($bs - $nbytes) }'
}

if [ "$1" = -d ]; then
	# decrypt
	"${OPENSSL}" enc -d "-${CIPHER}" -nopad -salt -pass fd:3 3< "${PASSPHRASE}"
else
	# encrypt
	pad | "${OPENSSL}" enc -e "-${CIPHER}" -nopad -salt -pass fd:3 3< "${PASSPHRASE}"
fi

