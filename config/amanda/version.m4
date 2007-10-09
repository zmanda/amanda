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
#   corresponding components of VERSION, and VERSION_SUFFIX to a
#   version-specific filename suffix.
#
AC_DEFUN([AMANDA_SPLIT_VERSION],
[
    changequote(,)
    VERSION_MAJOR=`expr "$VERSION" : '\([0-9]*\)'`
    VERSION_MINOR=`expr "$VERSION" : '[0-9]*\.\([0-9]*\)'`
    VERSION_PATCH=`expr "$VERSION" : '[0-9]*\.[0-9]*\.\([0-9]*\)'`
    VERSION_COMMENT=\"`expr "$VERSION" : '[0-9]*\.[0-9]*\.[0-9]*\(.*\)'`\"
    changequote([,])

    VERSION_SUFFIX="$VERSION"
    AC_SUBST(VERSION_MAJOR)
    AC_SUBST(VERSION_MINOR)
    AC_SUBST(VERSION_PATCH)
    AC_SUBST(VERSION_COMMENT)
    AC_SUBST(VERSION_SUFFIX)
])

# SYNOPSIS
#
#   AMANDA_WITH_SUFFIXES
#
# DESCRIPTION
#
#   Implement the --with-suffixes option.  If it is given, then set
#   program_transform_name appropriately and AC_DEFINE USE_VERSION_SUFFIXES to 1.
#   USE_VERSION_SUFFIXES is substituted with the value 'no' or 'yes'.
#
AC_DEFUN([AMANDA_WITH_SUFFIXES],
[
    AC_ARG_WITH(suffixes,
	[  --with-suffixes        install binaries with version string appended to name],
	USE_VERSION_SUFFIXES=$withval,
	: ${USE_VERSION_SUFFIXES=no}
    )

    case "$USE_VERSION_SUFFIXES" in
    y | ye | yes) USE_VERSION_SUFFIXES=yes
	AC_DEFINE(USE_VERSION_SUFFIXES, 1,
	    [Define to have programs use version suffixes when calling other programs.])

	program_suffix="-$VERSION_SUFFIX"
	# This is from the output of configure.in.
	if test "x$program_transform_name" = xs,x,x,; then
	    program_transform_name=
	else
	    # Double any \ or $.  echo might interpret backslashes.
	    cat <<\EOF_SED > conftestsed
s,\\,\\\\,g; s,\$,$$,g
EOF_SED
	    program_transform_name="`echo $program_transform_name|sed -f conftestsed`"
	    rm -f conftestsed
	fi
	test "x$program_prefix" != xNONE &&
	    program_transform_name="s,^,${program_prefix},; $program_transform_name"
	# Use a double $ so make ignores it.
	test "x$program_suffix" != xNONE &&
	    program_transform_name="s,\$\$,${program_suffix},; $program_transform_name"

	# sed with no file args requires a program.
	test "x$program_transform_name" = "" && program_transform_name="xs,x,x,"
	# Remove empty command
	cat <<\EOF_SED > conftestsed
s,\;\;,\;,g; s,\; \$,,g; s,\;$,,g
EOF_SED
	program_transform_name="`echo $program_transform_name|sed -f conftestsed`"
	rm -f conftestsed
      ;;
    n | no) USE_VERSION_SUFFIXES=no
      ;;
    *) AC_MSG_ERROR([*** You must not supply an argument to --with-suffixes option.])
      ;;
    esac

    AC_SUBST(USE_VERSION_SUFFIXES)
])
