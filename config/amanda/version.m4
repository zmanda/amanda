# Amanda version handling macros

AC_DEFUN([AMANDA_VERSION],
[
    AMANDA_GET_SVN_INFO
    AMANDA_GET_GIT_INFO
    if test -n "$SVN_REV"; then
        VERSION=`cat VERSION`

        if test "${SVN_TYPE}" = "branches"; then
	    AC_MSG_NOTICE("building from svn branch ${SVN_BRANCH} revision ${SVN_REV}")
            # This makes it clear if a build is "unofficial"
            VERSION=${VERSION}-svn-${SVN_REV}
	else if test "${SVN_TYPE}" = "trunk"; then
	    AC_MSG_NOTICE("building from trunk revision ${SVN_REV}")
            # This makes it clear if a build is "unofficial"
            VERSION=${VERSION}-svn-${SVN_REV}
        else
	    AC_MSG_NOTICE("building from tag ${SVN_BRANCH} revision ${SVN_REV}")
            RC=`echo "${SVN_BRANCH}"| grep "rc"`
            if test -n "$RC"; then
                # Override version for zmanda tags.
		changequote(,)
                VERSION=`echo "${SVN_BRANCH}"| sed 's/[^0-9]*// ; s/[_.]//g'`
                VERSION=`echo ${VERSION}| sed 's/^\([0-9]\)\([0-9]\)\([0-9]\)/\1.\2.\3/'`
		changequote([,])
            fi
        fi
	fi

    else if test -n "$GIT_SHA1"; then
	AC_MSG_NOTICE("building from git revision ${GIT_SHA1}")
        # This makes it clear if a build is "unofficial"
        VERSION=`cat VERSION`"-git-"${GIT_SHA1}

    else
	AC_MSG_NOTICE("building from source")
        # This makes it clear if a build is "unofficial"
        VERSION=`cat VERSION`"-"`date "+%Y%m%d"`
    fi
    fi
    AC_MSG_NOTICE("version: $VERSION")
])
# SYNOPSIS
#
#   AMANDA_SNAPSHOT_STAMP
#
# DESCRIPTION
#
#   If srcdir contains a file named SNAPSHOT, with a line matching
#	Snapshot Date: [0-9]*
#   then set add the date to VERSION and set 
#   SNAPSHOT_STAMP=SNAPSHOT.
#
AC_DEFUN([AMANDA_SNAPSHOT_STAMP],
[
    if test -f "$srcdir/SNAPSHOT"; then
      cat < "$srcdir/SNAPSHOT"
    changequote(,)
      snapdate=`sed -n '/^Snapshot Date: \([0-9]*\)/ s//\1/p' < $srcdir/SNAPSHOT`
    changequote([,])
      test -z "$snapdate" || VERSION="$VERSION-$snapdate"
      SNAPSHOT_STAMP=SNAPSHOT
    else
      SNAPSHOT_STAMP=
    fi
    AC_SUBST(SNAPSHOT_STAMP)
])

# SYNOPSIS
#
#   AMANDA_SPLIT_VERSION
#
# DESCRIPTION
#
#   Set the version number of this release of Amanda from the VERSION
#   string, which is set in AM_INIT_AUTOMAKE.  Sets VERSION_MAJOR,
#   VERSION_MINOR, VERSION_PATCH, and VERSION_COMMENT to the 
#   corresponding components of VERSION.  These four variables are
#   also AC_DEFINE'd
#
AC_DEFUN([AMANDA_SPLIT_VERSION],
[
    changequote(,)
    VERSION_MAJOR=`expr "$VERSION" : '\([0-9]*\)'`
    VERSION_MINOR=`expr "$VERSION" : '[0-9]*\.\([0-9]*\)'`
    VERSION_PATCH=`expr "$VERSION" : '[0-9]*\.[0-9]*\.\([0-9]*\)'`
    if test -z "$VERSION_PATCH"; then
	VERSION_PATCH=0
        VERSION_COMMENT=\"`expr "$VERSION" : '[0-9]*\.[0-9]*\(.*\)'`\"
    else
        VERSION_COMMENT=\"`expr "$VERSION" : '[0-9]*\.[0-9]*\.[0-9]*\(.*\)'`\"
    fi
    changequote([,])

    AC_SUBST(VERSION_MAJOR)
    AC_SUBST(VERSION_MINOR)
    AC_SUBST(VERSION_PATCH)
    AC_SUBST(VERSION_COMMENT)

    AC_DEFINE_UNQUOTED([VERSION_MAJOR], [$VERSION_MAJOR], [major Amanda version number])
    AC_DEFINE_UNQUOTED([VERSION_MINOR], [$VERSION_MINOR], [minor Amanda version number])
    AC_DEFINE_UNQUOTED([VERSION_PATCH], [$VERSION_PATCH], [Amanda patch number])
    AC_DEFINE_UNQUOTED([VERSION_COMMENT], [$VERSION_COMMENT], [Amanda version information beyond patch])
])

# SYNOPSIS
#
#   AMANDA_WITH_SUFFIXES
#
# DESCRIPTION
#
#   Deprectated --with-suffixes option.
#
AC_DEFUN([AMANDA_WITH_SUFFIXES],
[
    AC_ARG_WITH(suffixes, [], [
	AMANDA_MSG_WARN([** --with-suffixes is deprecated])
    ])
])
