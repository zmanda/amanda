# OVERVIEW
#

# SYNOPSIS
#
#   AMANDA_REST_HOOK
#
# OVERVIEW
#
#   Also handle --with-rest-hook
#
AC_DEFUN([AMANDA_REST_HOOK],
[
    AC_REQUIRE([AMANDA_EXPAND_DIRS])

    REST_HOOK_DIR=$AMPERLLIB/Amanda/Rest/Amanda/bin/hook
    AC_ARG_WITH(rest-hook,
	AS_HELP_STRING([--with-rest-hook=DIR_PATH],
		       [use DIR_PATH fir the rest-hook]),
	[
	    # check withval
	    case "$withval" in
		/*) REST_HOOK_DIR="$withval";;
		*)  AC_MSG_ERROR([*** You must supply a full pathname to --with-rest-hook]);;
	    esac
	    # done
	]
    )

    AC_DEFINE_UNQUOTED(REST_HOOK_DIR, "$REST_HOOK_DIR", [Location of the rest hook directory])
    AC_ARG_VAR(REST_HOOK_DIR, [Location of the rest hook directory])
    AC_SUBST(REST_HOOK_DIR)
])

