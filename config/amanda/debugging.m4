# SYNOPSIS
#
#   AMANDA_WITH_ASSERTIONS
#
# OVERVIEW
#
#   Handles the --with-assertions flag.  Defines and substitutes ASSERTIONS
#   if the flag is given.
#
AC_DEFUN([AMANDA_WITH_ASSERTIONS],
[
    ASSERTIONS=
    AC_ARG_WITH(assertions,
        AS_HELP_STRING([--with-assertions],
            [compile assertions into code]),
        [
            case "$withval" in
                n | no) : ;;
                y |  ye | yes)
		    ASSERTIONS=1
                    AC_DEFINE(ASSERTIONS,1,
                        [Define if you want assertion checking. ])
                  ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --with-assertions option.])
                  ;;
            esac
        ]
    )
    AC_SUBST(ASSERTIONS)
])

# SYNOPSIS
#
#   AMANDA_WITH_DEBUGGING
#
# OVERVIEW
#
#   Handles the --with[out]-debugging flag.  If debugging is not disabled, then define
#   DEBUG_CODE, and define and substitute AMANDA_DBGDIR to either the location the
#   user gave, or AMANDA_TMPDIR.
#
AC_DEFUN([AMANDA_WITH_DEBUGGING],
[
    AC_REQUIRE([AMANDA_WITH_TMPDIR])
    AC_ARG_WITH(debugging,
        AS_HELP_STRING([--with-debugging=DIR]
            [put debug logs in DIR (default same as --with-tmpdir)]), 
        [ debugging="$withval" ],
	[ debugging="yes" ]
    )

    case "$debugging" in
        n | no) AC_MSG_ERROR([Amanda no longer supports building with debugging disabled]);;
        y | ye | yes) AMANDA_DBGDIR="$AMANDA_TMPDIR";;
        *) AMANDA_DBGDIR="$debugging";;
    esac

    # evaluate any extra variables in the directory
    AC_DEFINE_DIR([AMANDA_DBGDIR], [AMANDA_DBGDIR],
	[Location of Amanda directories and files. ])
])

# SYNOPSIS
#
#   AMANDA_GLIBC_BACKTRACE
#
# OVERVIEW
#
#   Check for glibc's backtrace support, and define HAVE_GLIBC_BACKTRACE if it is present.
AC_DEFUN([AMANDA_GLIBC_BACKTRACE],
[
    AC_CHECK_HEADER([execinfo.h], [
	AC_CHECK_FUNC([backtrace_symbols_fd], [
	    AC_DEFINE(HAVE_GLIBC_BACKTRACE, 1,
		[Define this if glibc's backtrace functionality (execinfo.h) is present])
	])
    ])
])

# SYNOPSIS
#
#   AMANDA_WITH_DEBUG_DAYS
#
# OVERVIEW
#
#   Handles the --with-debug-days flag.  Defines and substitutes AMANDA_DEBUG_DAYS.
#
AC_DEFUN([AMANDA_WITH_DEBUG_DAYS],
[
    AC_ARG_WITH(debug_days,
        AS_HELP_STRING([--with-debug-days=NN],
            [number of days to keep debugging files (default: 4)]),
        [
            debug_days="$withval"
        ], [
            debug_days="yes"
        ]
    )
    case "$debug_days" in
        n | no) 
            AMANDA_DEBUG_DAYS=0 ;;
        y |  ye | yes) 
            AMANDA_DEBUG_DAYS=4 ;;
        [[0-9]] | [[0-9]][[0-9]] | [[0-9]][[0-9]][[0-9]]) 
            AMANDA_DEBUG_DAYS="$debug_days" ;;
        *) AC_MSG_ERROR([*** --with-debug-days value not numeric or out of range.])
          ;;
    esac
    AC_DEFINE_UNQUOTED(AMANDA_DEBUG_DAYS,$AMANDA_DEBUG_DAYS,
        [Number of days to keep debugging files. ])
    AC_SUBST(AMANDA_DEBUG_DAYS)
])

# SYNOPSIS
#
#   AMANDA_WITH_TESTING
#
# OVERVIEW
#
#   Handles the --with-testing flag.  Defines and substitutes SERVICE_SUFFIX, and
#   defines AMANDA_SERVICE_NAME and KAMANDA_SERVICE_NAME.
#
AC_DEFUN([AMANDA_WITH_TESTING],
[
    AC_ARG_WITH(testing,
        AS_HELP_STRING([--with-testing@<:@=SUFFIX@:>@],
            [use alternate service names with suffix (default 'test')]),
        [
            TESTING="$withval"
        ], [
            TESTING="no"
        ]
    )
    case "$TESTING" in
        n | no) SERVICE_SUFFIX="";;
        y |  ye | yes) SERVICE_SUFFIX="-test";;
        *) SERVICE_SUFFIX="-$TESTING";;
    esac

    AMANDA_SERVICE_NAME="amanda$SERVICE_SUFFIX"
    KAMANDA_SERVICE_NAME="kamanda$SERVICE_SUFFIX"

    AC_SUBST(SERVICE_SUFFIX)
    AC_DEFINE_UNQUOTED(SERVICE_SUFFIX, "$SERVICE_SUFFIX",
        [A suffix that will be appended to service names.
     * Useful for testing in parallel with a working version. ])
    AC_DEFINE_UNQUOTED(AMANDA_SERVICE_NAME,  "$AMANDA_SERVICE_NAME", 
        [The name for the Amanda service. ])
    AC_DEFINE_UNQUOTED(KAMANDA_SERVICE_NAME, "$KAMANDA_SERVICE_NAME", 
        [The name for the Kerberized Amanda service. ])
])

