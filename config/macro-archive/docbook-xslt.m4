##### http://autoconf-archive.cryp.to/ac_check_docbook_xslt.html
#
# SYNOPSIS
#
#   AC_CHECK_DOCBOOK_XSLT([xslt-version])
#
# DESCRIPTION
#
#   Check for access to docbook stylesheets of a particular revision.
#
#   This macro can be used for multiple versions within the same script.
#
#   Input:
#    $1 is the version of docbook to search for; default 'current'
#   Output:
#    $HAVE_DOCBOOK_XSLT_VERS will be set to 'yes' or 'no' depending
#    on the results of the test, where VERS is $1, with '_' substituted
#    for '.'  $HAVE_DOCBOOK_XSLT will also be set to the same value.
#
#   Example:
#    AC_CHECK_DOCBOOK_XSLT(1.72.0)
#    if test "x$HAVE_DOCBOOK_XSLT_1_72_0" = "xyes"; then
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

AC_DEFUN([AC_CHECK_DOCBOOK_XSLT],
[
    AC_REQUIRE([AC_PROG_XSLTPROC])

    dnl define a temporary variable for the version, so this macro can be
    dnl used with multiple versions
    define([_VERS], $1)
    ifelse(_VERS, [], [define([_VERS], [current])])

    dnl define variable names ending in _VERS which will actually have the
    dnl version number as a suffix
    define([ac_cv_docbook_xslt_VERS], patsubst([ac_cv_docbook_xslt_]_VERS, [\.], [_]))
    define([HAVE_DOCBOOK_XSLT_VERS], patsubst([HAVE_DOCBOOK_XSLT_]_VERS, [\.], [_]))

    AC_CACHE_CHECK([for Docbook XSLT version ]_VERS, [ac_cv_docbook_xslt_VERS],
    [
	ac_cv_docbook_xslt_VERS=no
	if test -n "$XSLTPROC"; then
	    echo "Trying '$XSLTPROC $XSLTPROC_FLAGS http://docbook.sourceforge.net/release/xsl/_VERS/xhtml/docbook.xsl'" >&AS_MESSAGE_LOG_FD
	    $XSLTPROC $XSLTPROC_FLAGS http://docbook.sourceforge.net/release/xsl/_VERS/xhtml/docbook.xsl >&AS_MESSAGE_LOG_FD 2>&AS_MESSAGE_LOG_FD

	    if test "$?" = 0; then
		ac_cv_docbook_xslt_VERS=yes
	    fi
	fi
    ])

    HAVE_DOCBOOK_XSLT_VERS="$ac_cv_docbook_xslt_VERS"
    HAVE_DOCBOOK_XSLT="$HAVE_DOCBOOK_XSLT_VERS"

    dnl clean up m4 namespace
    undefine([_VERS])
    undefine([ac_cv_docbook_xslt_VERS])
    undefine([HAVE_DOCBOOK_XSLT_VERS])
])
