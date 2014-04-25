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
#   AMANDA_CONFIGURE_ARGS
#
# OVERVIEW
#
#   Set CONFIGURE_ARGS
#
AC_DEFUN([AMANDA_CONFIGURE_ARGS],
[
    CONFIGURE_ARGS=$ac_configure_args
    AC_DEFINE_UNQUOTED(CONFIGURE_ARGS, "$CONFIGURE_ARGS",
			[Define as the configure command line option. ])
    AC_SUBST(CONFIGURE_ARGS)
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
    if test -d $srcdir/.svn -a -n "$SVN" && (cd $srcdir > /dev/null ; $SVN info . ) > conftemp.svn; then
	SVN_REV=`$GREP Revision: conftemp.svn|cut -d: -f 2|cut -c2-`
	SVN_URL=`$GREP URL: conftemp.svn|cut -d: -f 2-|cut -c2-`
	SVN_PATH=`$GREP URL: conftemp.svn|cut -d "/" -f 7-`
	SVN_TYPE=`echo ${SVN_PATH} |cut -d "/" -f 1`
	SVN_BRANCH=`echo "${SVN_PATH}"| cut -d "/" -f 2`
	url=`$GREP URL: conftemp.svn|cut -d: -f 2-|cut -c2-`
	( echo '#define BUILT_REV "'$SVN_REV'"'
	  echo '#define BUILT_BRANCH "'$SVN_BRANCH'"'
	) > common-src/svn-info.h
	BUILT_REV="$SVN_REV"
	BUILT_BRANCH="$SVN_BRANCH"

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
	BUILT_REV=""
	BUILT_BRANCH=""
    fi

    AC_SUBST(BUILT_BRANCH)
    AC_SUBST(BUILT_REV)

#    AC_DEFINE_UNQUOTED([BUILT_BRANCH], [$BUILT_BRANCH], [Amanda built branch])
#    AC_DEFINE_UNQUOTED([BUILT_REV], [$BUILT_REV], [Amanda built svn rev])

    rm -f conftemp.svn
])

# SYNOPSIS
#
#   AMANDA_GET_GIT_INFO
#
# OVERVIEW
#
#   If the build is in a git working copy, and if an git client
#   is available, then set GIT_SHA1
#
AC_DEFUN([AMANDA_GET_GIT_INFO],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_REQUIRE([AMANDA_PROG_GREP])

    AC_PATH_PROG(GIT, git,, $LOCSYSPATH)
    AC_MSG_CHECKING([git revision information])
    if test -d $srcdir/.git -a -n "$GIT"; then
	GIT_SHA1=`(cd $srcdir > /dev/null ; $GIT rev-parse HEAD | cut -c -8 )`
	if test -n "$GIT_SHA1"; then
	    AC_MSG_RESULT([$GIT_SHA1])
	else
	    AC_MSG_RESULT(['git rev-parse HEAD' failed])
	fi
    else 
	AC_MSG_RESULT([not available])
    fi

    rm -f conftemp.git
])
