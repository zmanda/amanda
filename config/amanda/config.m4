# SYNOPSIS
#
#   AMANDA_CONFIG_LCOAL
#
# OVERVIEW
#
#   Invoke ./config.local, if it exists
#
AC_DEFUN([AMANDA_CONFIG_LOCAL],
[
    if test -f config.local; then
	echo "running local script ./config.local"
	. ./config.local
    fi
])

# SYNOPSIS
#
#   AMANDA_GET_SVN_INFO
#
# OVERVIEW
#
#   If the build is in a Subversion working copy, and if an svn client
#   is available, then update common-src/svn-info.h to reflect the current
#   revision and branch.
#
#   If these things are not available, then the file is not updated, and
#   any previous contents are used.  If the file does not exist, it is
#   created.
#
AC_DEFUN([AMANDA_GET_SVN_INFO],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_REQUIRE([AMANDA_PROG_GREP])

    AC_PATH_PROG(SVN, svn,, $LOCSYSPATH)
    AC_MSG_CHECKING([Subversion revision information])
    if test -d $srcdir/.svn && test -n "$SVN" && (cd $srcdir > /dev/null ; $SVN info . ) > conftemp.svn; then
	rev=`$GREP Revision: conftemp.svn|cut -d: -f 2|cut -c2-`
	url=`$GREP URL: conftemp.svn|cut -d: -f 2-|cut -c2-`
	( echo '#define BUILT_REV "'$rev'"'
	  echo '#define BUILT_BRANCH "'`basename "$url"`'"'
	) > common-src/svn-info.h

	AC_MSG_RESULT([updated])
    else
	# Makefiles will be upset if the file doesn't exist, so double-check
	if test -f common-src/svn-info.h; then
	    : # all good
	    AC_MSG_RESULT([not changed])
	else
	    echo '/* no information available */' > common-src/svn-info.h
	    AC_MSG_RESULT([not available])
	fi
    fi

    rm -f conftemp.svn
])
