#!@SHELL@
#
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

