##### http://autoconf-archive.cryp.to/ac_prog_xsltproc.html
#
# SYNOPSIS
#
#   AC_PROG_XSLTPROC([default-flags])
#
# DESCRIPTION
#
#   Finds an xsltproc executable.
#
#   Input:
#    default-flags is the default $XSLTPROC_FLAGS, which will be
#    overridden if the user specifies --with-xsltproc-flags.
#
#   Output:
#    $XSLTPROC contains the path to xsltproc, or is empty if none was
#    found or the user specified --without-xsltproc. $XSLTPROC_FLAGS 
#    contains the flags to use with xsltproc.
#
# LAST MODIFICATION
#
#   2007-04-17
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

AC_DEFUN([AC_PROG_XSLTPROC],
[
XSLTPROC_FLAGS="$1"
AC_SUBST(XSLTPROC_FLAGS)

# The (lack of) whitespace and overquoting here are all necessary for
# proper formatting.
AC_ARG_WITH(xsltproc,
AS_HELP_STRING([--with-xsltproc[[[[[=PATH]]]]]],
               [Use the xsltproc binary in in PATH.]),
    [ ac_with_xsltproc=$withval; ],
    [ ac_with_xsltproc=maybe; ])

AC_ARG_WITH(xsltproc-flags,
AS_HELP_STRING([  --with-xsltproc-flags=FLAGS],
               [Flags to pass to xsltproc (default $1)]),
    [ if test "x$withval" == "xno"; then
	XSLTPROC_FLAGS=''
    else
	if test "x$withval" != "xyes"; then
	    XSLTPROC_FLAGS="$withval"
	fi
    fi
	])

# search for xsltproc if it wasn't specified
if test "$ac_with_xsltproc" = "yes" -o "$ac_with_xsltproc" = "maybe"; then
    AC_PATH_PROGS(XSLTPROC,xsltproc,,$LOCSYSPATH)
else
    if test "$ac_with_xsltproc" != "no"; then
        if test -x "$ac_with_xsltproc"; then
            XSLTPROC="$ac_with_xsltproc";
        else
            AMANDA_MSG_WARN([Specified xsltproc of $ac_with_xsltproc isn't executable; searching for an alternative.])
            AC_PATH_PROGS(XSLTPROC,xsltproc,,$LOCSYSPATH)
        fi
    fi
fi
])
