#
# Checks to see if there's a sockaddr_storage structure
#
# usage:
#
#	AMANDA_SOCKADDR_STORAGE
#
# results:
#
#	HAVE_SOCKADDR_STORAGE (defined)
#
AC_DEFUN([AMANDA_SOCKADDR_STORAGE],
[
    AC_CACHE_CHECK([if sockaddr_storage struct exists],
	ac_cv_has_sockaddr_storage,
    [
	AC_TRY_COMPILE([
#	include <sys/types.h>
#	include <sys/socket.h>],
	[u_int i = sizeof (struct sockaddr_storage)],
	ac_cv_has_sockaddr_storage=yes,
	ac_cv_has_sockaddr_storage=no)
    ])

    if test $ac_cv_has_sockaddr_storage = yes ; then
	AC_DEFINE(HAVE_SOCKADDR_STORAGE,1,
	    [struct sockaddr_storage exists])
    fi
])

# SYNOPSIS
#
#   AMANDA_CHECK_IPV6
#
# DESCRIPTION
#
#   Determine if this system has basic IPv6 support.  This 
#   addresse general availability (defining WORKING_IPV6 if
#   there's some amount of compatibility there), as well as
#   searching for specific functionality by requiring the other
#   macros in this file.
#   
AC_DEFUN([AMANDA_CHECK_IPV6],
[
    AC_REQUIRE([AMANDA_SOCKADDR_STORAGE])

    WORKING_IPV6=no
    AC_ARG_WITH(ipv6,
	AS_HELP_STRING([--with-ipv6],
		       [enable IPv6 support (default if IPv6 is found)])
	AS_HELP_STRING([--without-ipv6],
		       [disable IPv6]),
	[
	    case "$withval" in
	    y | ye | yes) amanda_with_ipv6=yes;;
	    n | no) amanda_with_ipv6=no;;
	    *)
		AC_MSG_ERROR([*** You must not supply an argument to --with-ipv6 option.])
	      ;;
	    esac
	], [
	    amanda_with_ipv6=maybe
	]
    )

    if test x"$amanda_with_ipv6" = x"yes" ||
       test x"$amanda_with_ipv6" = x"maybe" ; then
	AC_CACHE_CHECK([for working IPv6],
		       amanda_cv_working_ipv6,
	[
	    case "$host" in
		*-pc-cygwin) amanda_cv_working_ipv6=no;;
		*)
		    AC_RUN_IFELSE([AC_LANG_SOURCE([[
#include <sys/types.h>
#ifdef HAVE_SYS_SOCKET_H
#include <sys/socket.h>
#endif
#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif
#ifdef HAVE_WINSOCK2_H
#include <winsock2.h>
#endif
#include <sys/socket.h>
#include <errno.h>

main()
{
   int aa;
   aa = socket(AF_INET6, SOCK_STREAM, 0);
   if (aa > 0) return 0;
   return aa;
}]])],
    [ amanda_cv_working_ipv6=yes ],
    [ amanda_cv_working_ipv6=no ],
    [ amanda_cv_working_ipv6=yes ]
		)
	    esac
	])

	if test "$amanda_cv_working_ipv6" = yes; then
	    WORKING_IPV6=yes
	    AC_DEFINE(WORKING_IPV6,1,
		[Target system has functional IPv6 support])
	else
	    # error out only if the user specifically requested support
	    if test x"$amanda_with_ipv6" = x"yes"; then
		AC_MSG_ERROR([IPv6 support was requested, but it is not working.])
	    fi
	fi
    fi
])

# SYNOPSIS
#
#   AMANDA_SHOW_IPV6_SUMMARY
#
# OVERVIEW
#
#   Show a summary of the settings from this file.
#
AC_DEFUN([AMANDA_SHOW_IPV6_SUMMARY],
[
    echo "Working IPv6:" $WORKING_IPV6
])
