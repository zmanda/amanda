##### http://autoconf-archive.cryp.to/ac_check_docbook_dtd.html
#
# SYNOPSIS
#
#   AC_CHECK_DOCBOOK_DTD([dtd-version])
#
# DESCRIPTION
#
#   Check for access to a docbook DTD of a particular revision.
#
#   This macro can be used for multiple versions within the same script.
#
#   Input:
#    $1 is the version of docbook to search for; default 'current'
#   Output:
#    $HAVE_DOCBOOK_DTD_VERS will be set to 'yes' or 'no' depending
#    on the results of the test, where VERS is $1, with '_' substituted
#    for '.'  $HAVE_DOCBOOK_DTD will also be set to the same value.
#
#   Example:
#    AC_CHECK_DOCBOOK_DTD(4.3)
#    if test "x$HAVE_DOCBOOK_DTD_4_3" = "xyes"; then
#      ..
#
# LAST MODIFICATION
#
#   2007-06-28
#
# AUTHOR
#
#   Dustin J. Mitchell <dustin@zmanda.com>
#
# COPYRIGHT
#
#   Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
#  
#   This program is free software; you can redistribute it and/or modify it
#   under the terms of the GNU General Public License version 2 as published
#   by the Free Software Foundation.
#  
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
#   or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
#   for more details.
#  
#   You should have received a copy of the GNU General Public License along
#   with this program; if not, write to the Free Software Foundation, Inc.,
#   59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
#
#   This special exception to the GPL applies to versions of the
#   Autoconf Macro released by the Autoconf Macro Archive. When you
#   make and distribute a modified version of the Autoconf Macro, you
#   may extend this special exception to the GPL to apply to your
#   modified version as well.
#
#   Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
#   Sunnyvale, CA 94086, USA, or: http://www.zmanda.com

AC_DEFUN([AC_CHECK_DOCBOOK_DTD],
[
    AC_REQUIRE([AC_PROG_XSLTPROC])

    dnl define a temporary variable for the version, so this macro can be
    dnl used with multiple versions
    define([_VERS], $1)
    ifelse(_VERS, [], [define([_VERS], [current])])

    dnl define variable names ending in _VERS which will actually have the
    dnl version number as a suffix
    define([ac_cv_docbook_dtd_VERS], patsubst([ac_cv_docbook_dtd_]_VERS, [\.], [_]))
    define([HAVE_DOCBOOK_DTD_VERS], patsubst([HAVE_DOCBOOK_DTD_]_VERS, [\.], [_]))

    AC_CACHE_CHECK([for Docbook DTD version ]_VERS, [ac_cv_docbook_dtd_VERS],
    [
	ac_cv_docbook_dtd_VERS=no
	if test -n "$XSLTPROC"; then
	    MY_XSLTPROC_FLAGS=`echo "" $XSLTPROC_FLAGS|sed -e s/--novalid//g`
	    cat <<EOF >conftest.xml
<?xml version="1.0" encoding='ISO-8859-1'?>
<!DOCTYPE book PUBLIC "-//OASIS//DTD DocBook XML V[]_VERS//EN" "http://www.oasis-open.org/docbook/xml/_VERS/docbookx.dtd">
<book id="empty">
</book>
EOF
	    echo "Trying '$XSLTPROC $MY_XSLTPROC_FLAGS conftest.xml'" >&AS_MESSAGE_LOG_FD
	    echo "conftest.xml:" >&AS_MESSAGE_LOG_FD
	    echo "====" >&AS_MESSAGE_LOG_FD
	    cat conftest.xml >&AS_MESSAGE_LOG_FD
	    echo "====" >&AS_MESSAGE_LOG_FD

	    $XSLTPROC $MY_XSLTPROC_FLAGS conftest.xml >conftest.out 2>&1
	    if test "$?" = 0 -o "$?" = 5; then
		# failing to load the DTD is just a warning, so check for it in the output.
		if grep 'warning: failed to load external entity' conftest.out >/dev/null 2>&1; then
		    : # no good..
		else
		    ac_cv_docbook_dtd_VERS=yes
		fi
	    fi
	    cat conftest.out >&AS_MESSAGE_LOG_FD

	    rm -f conftest.xml conftest.out
	fi
    ])

    HAVE_DOCBOOK_DTD_VERS="$ac_cv_docbook_dtd_VERS"
    HAVE_DOCBOOK_DTD="$HAVE_DOCBOOK_DTD_VERS"

    dnl clean up m4 namespace
    undefine([_VERS])
    undefine([ac_cv_docbook_dtd_VERS])
    undefine([HAVE_DOCBOOK_DTD_VERS])
])
