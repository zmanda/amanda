# SYNOPSIS
#
#   AMANDA_BSDTCP_SECURITY
#
# OVERVIEW
#
#   Handle configuration for BSDTCP security, implementing the 
#   --with-bsdtcp-security option.
#
#   Defines and substitutes BSDTCP_SECURITY, and sets AM_CONDITIONAL 
#   WANT_BSDTCP_SECURITY, if the user has selected this mechanism.
#
AC_DEFUN([AMANDA_BSDTCP_SECURITY],
[
    BSDTCP_SECURITY="yes"
    AC_ARG_WITH(bsdtcp-security,
        AS_HELP_STRING([--with-bsdtcp-security],
                [include BSDTCP authentication]),
        [
            case "$withval" in
                n | no) BSDTCP_SECURITY=no ;;
                y |  ye | yes) BSDTCP_SECURITY=yes ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --with-bsdtcp-security.])
              ;;
            esac
        ],
    )

    if test "x$BSDTCP_SECURITY" = "xyes"; then
        AC_DEFINE(BSDTCP_SECURITY,1,
            [Define if BSDTCP transport should be enabled.])
    fi

    AM_CONDITIONAL(WANT_BSDTCP_SECURITY, test x"$BSDTCP_SECURITY" = x"yes")
    AC_SUBST(BSDTCP_SECURITY)
])
