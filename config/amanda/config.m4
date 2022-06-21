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
#   AMANDA_GET_VERSION_INFO
#
# OVERVIEW
#
#   Attempt to use the present version-holding files first
#
AC_DEFUN([AMANDA_GET_VERSION_INFO],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_REQUIRE([AMANDA_PROG_GREP])

    AC_MSG_CHECKING([Earlier version information])
    BUILT_REV=""
    BUILT_BRANCH=""
    if test -s $srcdir/LONG_BRANCH -a -s $srcdir/FULL_VERSION; then
        read BUILT_REV <$srcdir/FULL_VERSION
        read BUILT_BRANCH <$srcdir/LONG_BRANCH

	AC_MSG_RESULT([updated])
        AC_SUBST(BUILT_BRANCH)
        AC_SUBST(BUILT_REV)

        AC_DEFINE_UNQUOTED([BUILT_BRANCH], "$BUILT_BRANCH", [Amanda built branch])
        AC_DEFINE_UNQUOTED([BUILT_REV], "$BUILT_REV", [Amanda built version name])
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
    if test -z "$BUILT_REV"; then
        test -d $srcdir/.svn -a -n "$SVN" &&
           read -d '' SVN_OUTPUT <<<$(command cd $srcdir; $SVN info .)

        if test -n "$SVN_OUTPUT"; then
            SVN_REV=$($GREP <<<"$SVN_OUTPUT" Revision:); SVN_REV=${SVN_REV#*: }
            SVN_URL=$($GREP <<<"$SVN_OUTPUT" URL:); SVN_URL=${SVN_URL#*: }
            # sourceforge/p/amanda/code/...
            SVN_PATH=${SVN_URL#*/code/}
            # HEAD/tree/....
            SVN_TYPE=${SVN_PATH%%/*}
            SVN_BRANCH=${SVN_PATH#*/}
            SVN_BRANCH=${SVN_BRANCH%%/*}
            BUILT_REV=$SVN_REV
            BUILT_BRANCH=$SVN_BRANCH

            AC_MSG_RESULT([updated])
        fi

        AC_SUBST(BUILT_BRANCH)
        AC_SUBST(BUILT_REV)

        AC_DEFINE_UNQUOTED([BUILT_BRANCH], "$BUILT_BRANCH", [Amanda built branch])
        AC_DEFINE_UNQUOTED([BUILT_REV], "$BUILT_REV", [Amanda built svn rev])
    fi
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

    if test -z "$BUILT_REV"; then
        AC_REQUIRE([AMANDA_INIT_PROGS])
        AC_REQUIRE([AMANDA_PROG_GREP])

        AC_PATH_PROG(GIT, git,, $LOCSYSPATH)
        AC_MSG_CHECKING([git revision information])
        if test -s $srcdir/.git -a -n "$GIT"; then
            BUILT_BRANCH=$($GIT --git-dir=$srcdir/.git rev-parse --symbolic-full-name '@{upstream}')
            BUILT_BRANCH=${BUILT_BRANCH#refs/remotes/}
            BUILT_REV=$($GIT --git-dir=$srcdir/.git describe --tags HEAD)
        fi

        AC_SUBST(BUILT_BRANCH)
        AC_SUBST(BUILT_REV)

        AC_DEFINE_UNQUOTED([BUILT_BRANCH], "$BUILT_BRANCH", [Amanda built git branch])
        AC_DEFINE_UNQUOTED([BUILT_REV], "$BUILT_REV", [Amanda built git commit])
    fi
])
