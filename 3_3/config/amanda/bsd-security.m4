# SYNOPSIS
#
#   AMANDA_BSD_SECURITY
#
# OVERVIEW
#
#   Handle configuration for BSD security, implementing the 
#   --without-bsd-security option.  Also supplies the --without-amandahosts
#   option to use .rhosts instead of .amandahosts
#
#   Note that the defaults for *both* of these options are "yes", unlike
#   the remainder of the security implementations.
#
#   Defines and substitues BSD_SECURITY, and sets AM_CONDITIONAL
#   WANT_BSD_SECURITY, if the user has selected this mechanism.
#   Also defines and substitutes USE_AMANDAHOSTS unless the user has
#   specified --without-amandahosts.
#
AC_DEFUN([AMANDA_BSD_SECURITY],
[
    BSD_SECURITY="yes"
    AC_ARG_WITH(bsd-security,
        AS_HELP_STRING([--without-bsd-security],
                [do not include BSD authentication]),
        [
            case "$withval" in
                n | no) BSD_SECURITY=no ;;
                y |  ye | yes) BSD_SECURITY=yes ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --without-bsd-security.])
                    ;;
            esac
        ],
    )

    USE_AMANDAHOSTS=yes
    AC_ARG_WITH(amandahosts,
        AS_HELP_STRING([ --without-amandahosts],
            [use ".rhosts" instead of ".amandahosts"]),
        [
            case "$withval" in
                n | no ) USE_AMANDAHOSTS=no ;;
                y |  ye | yes) USE_AMANDAHOSTS=yes ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --without-amandahosts option.])
                  ;;
            esac
        ]
    )

    if test "x$BSD_SECURITY" = "xyes"; then
        AC_DEFINE(BSD_SECURITY,1,
            [Define to use BSD .rhosts/.amandahosts security. ])
        if test "x$USE_AMANDAHOSTS" = "xyes"; then
            AC_DEFINE(USE_AMANDAHOSTS,1,
                [Define if you want to use the ".amandahosts" for BSD security. ])
        fi
    fi

    AM_CONDITIONAL(WANT_BSD_SECURITY, test x"$BSD_SECURITY" = x"yes")
    AC_SUBST(BSD_SECURITY)
    AC_SUBST(USE_AMANDAHOSTS)
])
