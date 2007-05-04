##### http://autoconf-archive.cryp.to/ac_check_docbook_xslt_min.html
#
# SYNOPSIS
#
#   AC_CHECK_DOCBOOK_XSLT_MIN(min-xslt-version)
#
# DESCRIPTION
#
#   Check that the 'current' version of docbook is at least version 
#   min-xslt-version.
#
#   If the test is successful, $DOCBOOK_XSLT_CURRENT_VERSION will be set to the
#   current docbook version; if not, it will be set to 'no'.
#
#   Example:
#    AC_CHECK_DOCBOOK_XSLT_MIN(1.72.0)
#    if test "x$DOCBOOK_XSLT_CURRENT_VERSION" = "xno"; then
#      ..
#
# LAST MODIFICATION
#
#   2007-04-20
#
# AUTHOR
#
#   Dustin J. Mitchell <dustin@zmanda.com>
#
# COPYRIGHT
#
#   Copyright (c) 2007 Zmanda Inc.  All Rights Reserved.
#  
#   This program is free software; you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation; either version 2 of the License, or (at your
#   option) any later version.
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
#   Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
#   Sunnyvale, CA 94085, USA, or: http://www.zmanda.com

AC_DEFUN([AC_CHECK_DOCBOOK_XSLT_MIN],
[
    AC_REQUIRE([AC_PROG_XSLTPROC])

    AC_CACHE_CHECK([for current Docbook XSLT version], [ac_cv_docbook_xslt_current_version],
    [
	ac_cv_docbook_xslt_current_version=no

	if test -n "$XSLTPROC"; then
	    cat >conftest.xsl <<EOF
		<xsl:stylesheet
		    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		    xmlns:fm="http://freshmeat.net/projects/freshmeat-submit/"
		    version="1.0">
		    <xsl:output method="text"/>
		    <xsl:template match="fm:project/fm:Version">
			<xsl:value-of select="." />
		    </xsl:template>
		    <!-- do nothing with any other text -->
		    <xsl:template match="text()"/>
		</xsl:stylesheet>
EOF
	    echo "Trying '$XSLTPROC $XSLTPROC_FLAGS http://docbook.sourceforge.net/release/xsl/current/VERSION' with input:" >&AS_MESSAGE_LOG_FD
	    echo "====" >&AS_MESSAGE_LOG_FD
	    cat conftest.xsl >&AS_MESSAGE_LOG_FD
	    echo "====" >&AS_MESSAGE_LOG_FD

	    ac_cv_docbook_xslt_current_version=`$XSLTPROC $XSLTPROC_FLAGS conftest.xsl http://docbook.sourceforge.net/release/xsl/current/VERSION 2>&AS_MESSAGE_LOG_FD`

	    if test "$?" != 0; then
		ac_cv_docbook_xslt_current_version='no'
	    fi

	    rm conftest.xsl
	fi
    ])

    DOCBOOK_XSLT_CURRENT_VERSION="$ac_cv_docbook_xslt_current_version"
    AC_MSG_CHECKING([whether Docbook XSLT version is $1 or newer])

    if test x"$DOCBOOK_XSLT_CURRENT_VERSION" = x"no"; then
	AC_MSG_RESULT([no])
    else
	# compare versions (current on left, minimum on right)

	# for each pattern, get the component number, or default to zero if not found
	changequote(,)
	MAJPAT='/^[0-9]*\./{               s/^\([0-9]*\)\..*/\1/;q};                 s/.*/0/; q'
	MINPAT='/^[0-9]*\.[0-9]*\./{       s/^[0-9]*\.\([0-9]*\)\..*/\1/;q};         s/.*/0/; q'
	REVPAT='/^[0-9]*\.[0-9]*\.[0-9]*/{ s/^[0-9]*\.[0-9]*\.\([0-9]*\).*/\1/;q};   s/.*/0/; q'
	changequote([,])

	# major version
	left=`echo "$DOCBOOK_XSLT_CURRENT_VERSION"|sed "$MAJPAT"`
	right=`echo "$1"|sed "$MAJPAT"`
	if test $left -lt $right; then
	    DOCBOOK_XSLT_CURRENT_VERSION=no
	else
	    # minor version
	    left=`echo "$DOCBOOK_XSLT_CURRENT_VERSION"|sed "$MINPAT"`
	    right=`echo "$1"|sed "$MINPAT"`
	    if test $left -lt $right; then
		DOCBOOK_XSLT_CURRENT_VERSION=no
	    else
		# revision
		left=`echo "$DOCBOOK_XSLT_CURRENT_VERSION"|sed "$REVPAT"`
		right=`echo "$1"|sed "$REVPAT"`
		if test $left -lt $right; then
		    DOCBOOK_XSLT_CURRENT_VERSION=no
		fi
	    fi
	fi
	MAJPAT=''
	MINPAT=''
	REVPAT=''
	left=''
	right=''

	if test x"$DOCBOOK_XSLT_CURRENT_VERSION" = x"no"; then
	    AC_MSG_RESULT([no])
	else
	    AC_MSG_RESULT([yes])
	fi
    fi
])
