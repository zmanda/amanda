# SYNOPSIS
#
#   AMANDA_SSL_SECURITY
#
# OVERVIEW
#
#   Handle configuration for SSL security, implementing the --with-ssl-security
#   option and checking for the relevant programs and options.  Defines and substitutes
#
AC_DEFUN([AMANDA_SSL_SECURITY],
[
    AC_PATH_PROG(PKG_CONFIG, pkg-config, [], $LOCSYSPATH:/opt/csw/bin:/usr/local/bin:/opt/local/bin)

    SSL_SECURITY=yes
    AC_ARG_WITH(ssl-security,
        AS_HELP_STRING([--with-ssl-security],
                [include SSL authentication]),
        [
            case "$withval" in
                n | no) SSL_SECURITY=no ;;
                y |  ye | yes) SSL_SECURITY=yes ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --with-ssl-security.])
              ;;
            esac
        ],
    )

    if test "x$SSL_SECURITY" = "xyes"; then
	AC_CHECK_HEADERS(openssl/ssl.h openssl/err.h openssl/crypto.h,,[SSL_SECURITY=no],)
    fi

    if test "x$SSL_SECURITY" = "xyes"; then
	ssl_ld_flags=`$PKG_CONFIG openssl --libs-only-L 2>/dev/null`
	ssl_lib_flags=`$PKG_CONFIG openssl --libs-only-l --libs-other 2>/dev/null`
	ssl_cppflags=`$PKG_CONFIG openssl --cflags-only-I 2>/dev/null`
	ssl_cflags=`$PKG_CONFIG openssl --cflags-only-other 2>/dev/null`

	AMANDA_ADD_LIBS($ssl_ld_flags)
	AMANDA_ADD_LIBS($ssl_lib_flags)

	AMANDA_ADD_CPPFLAGS($ssl_cppflags)
	AMANDA_ADD_CFLAGS($ssl_cflags)

        # finally, make the various outputs for all of this
        AC_DEFINE(SSL_SECURITY,1,
                [Define if SSL transport should be enabled. ])
    fi
    AM_CONDITIONAL(WANT_SSL_SECURITY, test x"$SSL_SECURITY" = x"yes")

    AC_SUBST(SSL_SECURITY)
])
