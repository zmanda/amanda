# Amanda version handling macros

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
    VERSION_COMMENT=\"`expr "$VERSION" : '[0-9]*\.[0-9]*\.[0-9]*\(.*\)'`\"
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
