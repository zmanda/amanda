# SYNOPSIS
#
#   AMANDA_RSH_SECURITY
#
# OVERVIEW
#
#   Handle configuration for RSH security, implementing the --with-rsh-security
#   option and checking for the relevant programs and options.
#
AC_DEFUN([AMANDA_RSH_SECURITY],
[
    RSH_SECURITY=no
    AC_ARG_WITH(rsh-security,
        AS_HELP_STRING([--with-rsh-security], 
                [include RSH authentication]),
        [
            case "$withval" in
                n | no) : ;;
                y |  ye | yes) RSH_SECURITY=yes ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --with-rsh-security.])
              ;;
            esac
        ],
    )

    if test "x$RSH_SECURITY" = "xyes"; then
        AC_DEFINE(RSH_SECURITY,1,
                [Define if RSH transport should be enabled. ])
    fi
    AM_CONDITIONAL(WANT_RSH_SECURITY, test x"$RSH_SECURITY" = x"yes")
    AC_SUBST(RSH_SECURITY)
])

