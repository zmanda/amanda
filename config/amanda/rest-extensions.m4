# OVERVIEW
#

# SYNOPSIS
#
#   AMANDA_REST_EXTENSIONS
#
# OVERVIEW
#
#   Also handle --with-rest-extensions
#
AC_DEFUN([AMANDA_REST_EXTENSIONS],
[
    AC_REQUIRE([AMANDA_EXPAND_DIRS])

    REST_EXTENSIONS_DIR=$AMPERLLIB/Amanda/Rest/Amanda/bin/extensions
    AC_ARG_WITH(rest-extensions,
	AS_HELP_STRING([--with-rest-extensions=DIR_PATH],
		       [use DIR_PATH fir the rest-extensions]),
	[
	    # check withval
	    case "$withval" in
		/*) REST_EXTENSIONS_DIR="$withval";;
		*)  AC_MSG_ERROR([*** You must supply a full pathname to --with-rest-extensions]);;
	    esac
	    # done
	]
    )

    AC_DEFINE_UNQUOTED(REST_EXTENSIONS_DIR, "$REST_EXTENSIONS_DIR", [Location of the rest extensions directory])
    AC_ARG_VAR(REST_EXTENSIONS_DIR, [Location of the rest extensions directory])
    AC_SUBST(REST_EXTENSIONS_DIR)
])

