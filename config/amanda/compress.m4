# SYNOPSIS
#
#   AMANDA_CHECK_COMPRESSION
#
# OVERVIEW
#
#   Find a suitable compression program and substitute and define the following:
#
#    - COMPRESS_PATH
#    - COMPRESS_SUFFIX
#    - COMPRESS_FAST_OPT
#    - COMPRESS_BEST_OPT
#    - UNCOMPRESS_PATH
#    - UNCOMPRESS_OPT
#    - HAVE_GZIP
#
AC_DEFUN([AMANDA_CHECK_COMPRESSION],
[
    AC_REQUIRE([AMANDA_PROG_CAT])
    AC_REQUIRE([AMANDA_PROG_COMPRESS])
    AC_REQUIRE([AMANDA_PROG_GZIP])

    HAVE_GZIP=
    if test "$GZIP"; then
	AC_DEFINE(HAVE_GZIP,1,
	    [Define if Amanda is using the gzip program. ])
	HAVE_GZIP=1
	COMPRESS_PATH="$GZIP"
	COMPRESS_SUFFIX=".gz"
	COMPRESS_FAST_OPT="--fast"
	COMPRESS_BEST_OPT="--best"
	UNCOMPRESS_PATH="$GZIP"
	UNCOMPRESS_OPT="-dc"
    else
	if test "$COMPRESS"; then
	    COMPRESS_PATH="$COMPRESS"
	    COMPRESS_SUFFIX=".Z"
	    COMPRESS_FAST_OPT="-f"
	    COMPRESS_BEST_OPT="-f"
	    UNCOMPRESS_PATH="$COMPRESS"
	    UNCOMPRESS_OPT="-dc"
	else
	    # If we have to use cat, we don't define COMPRESS_FAST_OPT,
	    # COMPRESS_BEST_OPT, or UNCOMPRESS_OPT as "" since cat will look
	    # look for a file by the name of "".
	    # XXX is the above true? --dustin

	    AMANDA_MSG_WARN([Cannot find either gzip or compress.  Using cat.])
	    COMPRESS_PATH="$CAT"
	    COMPRESS_SUFFIX=""
	    COMPRESS_FAST_OPT=""
	    COMPRESS_BEST_OPT=""
	    UNCOMPRESS_PATH="$CAT"
	    UNCOMPRESS_OPT=""
	fi
    fi

    AC_DEFINE_UNQUOTED(COMPRESS_PATH,"$COMPRESS_PATH",
	[Define to the exact path to the gzip or the compress program. ])
    AC_DEFINE_UNQUOTED(COMPRESS_SUFFIX,"$COMPRESS_SUFFIX",
	[Define to the suffix for the COMPRESS_PATH compression program. ])
    AC_DEFINE_UNQUOTED(COMPRESS_FAST_OPT,"$COMPRESS_FAST_OPT",
	[Define as the command line option for fast compression. ])
    AC_DEFINE_UNQUOTED(COMPRESS_BEST_OPT,"$COMPRESS_BEST_OPT",
	[Define as the command line option for best compression. ])
    AC_DEFINE_UNQUOTED(UNCOMPRESS_PATH,"$UNCOMPRESS_PATH",
	[Define as the exact path to the gzip or compress command. ])
    AC_DEFINE_UNQUOTED(UNCOMPRESS_OPT,"$UNCOMPRESS_OPT",
	[Define as any optional arguments to get UNCOMPRESS_PATH to uncompress. ])

    AC_SUBST(COMPRESS_PATH)
    AC_SUBST(COMPRESS_SUFFIX)
    AC_SUBST(COMPRESS_FAST_OPT)
    AC_SUBST(COMPRESS_BEST_OPT)
    AC_SUBST(UNCOMPRESS_PATH)
    AC_SUBST(UNCOMPRESS_OPT)
    AC_SUBST(HAVE_GZIP)

    # Empty GZIP so that make dist works.
    GZIP=
])
