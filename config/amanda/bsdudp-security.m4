# SYNOPSIS
#
#   AMANDA_BSDUDP_SECURITY
#
# OVERVIEW
#
#   Handle configuration for BSDUDP security, implementing the 
#   --with-bsdudp-security option.
#
#   Defines and substitutes BSDUDP_SECURITY, and sets AM_CONDITIONAL
#   WANT_BSDUDP_SECURITY, if the user has selected this mechanism.
#
AC_DEFUN([AMANDA_BSDUDP_SECURITY],
[
    BSDUDP_SECURITY="no"
    AC_ARG_WITH(bsdudp-security,
        AS_HELP_STRING([--with-bsdudp-security],
                [include BSDUDP authentication]),
        [
            case "$withval" in
                n | no) : ;;
                y |  ye | yes) BSDUDP_SECURITY=yes ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --with-bsdudp-security.])
              ;;
            esac
        ],
    )

    if test "x$BSDUDP_SECURITY" = "xyes"; then
        AC_DEFINE(BSDUDP_SECURITY,1,
            [Define if BSDUDP transport should be enabled.])
    fi

    AM_CONDITIONAL(WANT_BSDUDP_SECURITY, test x"$BSDUDP_SECURITY" = x"yes")
    AC_SUBST(BSDUDP_SECURITY)
])
