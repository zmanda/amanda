# OVERVIEW
#

# SYNOPSIS
#
#   AMANDA_EXTENSIONS_HOOK
#
# OVERVIEW
#
#   Also handle --with-rest-extensions
#
AC_DEFUN([AMANDA_EXTENSIONS_HOOK],
[
    AC_REQUIRE([AMANDA_EXPAND_DIRS])

    EXTENSIONS_HOOK_DIR=$AMPERLLIB/Amanda/Rest/Amanda/bin/extensions
    AC_ARG_WITH(rest-extensions,
	AS_HELP_STRING([--with-rest-extensions=DIR_PATH],
		       [use DIR_PATH fir the rest-extensions]),
	[
	    # check withval
	    case "$withval" in
		/*) EXTENSIONS_HOOK_DIR="$withval";;
		*)  AC_MSG_ERROR([*** You must supply a full pathname to --with-rest-extensions]);;
	    esac
	    # done
	]
    )

    AC_DEFINE_UNQUOTED(EXTENSIONS_HOOK_DIR, "$EXTENSIONS_HOOK_DIR", [Location of the rest extensions directory])
    AC_ARG_VAR(EXTENSIONS_HOOK_DIR, [Location of the rest extensions directory])
    AC_SUBST(EXTENSIONS_HOOK_DIR)
])

