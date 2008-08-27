# SYNOPSIS
#
#   AMANDA_CHECK_TYPE(type, replacement-type, header)
#
# OVERVIEW
# 
#   Like AC_CHECK_TYPE, where action-if-not-found DEFINEs $1 to $2.
#
#   'header' must be a single header name, or blank to use the default
#   headers.
#
AC_DEFUN([AMANDA_CHECK_TYPE], [
    AC_REQUIRE([AC_HEADER_STDC])
    AC_CHECK_TYPE($1, [], [
	AC_DEFINE($1, $2, [Type for $1, if it is not defined by the system])
    ], ifelse($3, [], [], [
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef STDC_HEADERS
#include <stdlib.h>
#include <stddef.h>
#endif
#include <$3>
	])
    )
])
#
# SYNOPSIS
#
#   AMANDA_TYPE_PID_T
#
# OVERVIEW
#
#   Check whether pid_t is a long, int, or short.  DEFINE PRINTF_PID_T to the
#   corresponding printf format.
#
AC_DEFUN([AMANDA_TYPE_PID_T], [
	AC_REQUIRE([AC_HEADER_STDC])
	AC_REQUIRE([AC_TYPE_PID_T])
	AC_CACHE_CHECK([for pid_t type], amanda_cv_pid_type,
	    [
		amanda_cv_pid_type=unknown
		if test "$ac_cv_type_pid_t" = no; then
		    amanda_cv_pid_type=int
		fi
		for TEST_amanda_cv_pid_type in long short int; do
		    if test $amanda_cv_pid_type = unknown; then
			AC_EGREP_CPP(typedef.*${TEST_amanda_cv_pid_type}.*pid_t,
			    [
#include <sys/types.h>
#if STDC_HEADERS
#include <stdlib.h>
#include <stddef.h>
#endif
			    ],
			amanda_cv_pid_type=$TEST_amanda_cv_pid_type)
		    fi
		    if test $amanda_cv_pid_type = unknown; then
			AC_EGREP_CPP(ZZZZ.*${TEST_amanda_cv_pid_type},
			    [
#include <sys/types.h>
#if STDC_HEADERS
#include <stdlib.h>
#include <stddef.h>
#endif
				ZZZZ pid_t
			],
			amanda_cv_pid_type=$TEST_amanda_cv_pid_type)
		    fi
		done
		if test $amanda_cv_pid_type = unknown; then
		    amanda_cv_pid_type=int
		fi
	    ]
	)
	case $amanda_cv_pid_type in
	    int)	AC_DEFINE_UNQUOTED(PRINTF_PID_T,"%d",[Define to printf formatting string to print a PID. ]) ;;
	    long)	AC_DEFINE_UNQUOTED(PRINTF_PID_T,"%ld") ;;
	    short)	AC_DEFINE_UNQUOTED(PRINTF_PID_T,"%d") ;;
	esac
    ]
)

# SYNOPSIS
#
#   CF_WAIT
#
# OVERVIEW
#
# Test for the presence of <sys/wait.h>, 'union wait', arg-type of 'wait()'.
# by T.E.Dickey" , Jim Spath <jspath@mail.bcpl.lib.md.us>
#
#   DEFINEs WAIT_USES_UNION if 'union wait' is found.  Note that many systems
#   support *both* 'union wait' and 'int' using a transparent union.
#
#   Original comments:
#
#     FIXME: These tests should have been in autoconf 1.11!
#
#     Note that we cannot simply grep for 'union wait' in the wait.h file,
#     because some Posix systems turn this on only when a BSD variable is
#     defined. Since I'm trying to do without special defines, I'll live
#     with the default behavior of the include-file.
#
#     I do _2_ compile checks, because we may have union-wait, but the
#     prototype for 'wait()' may want an int.
#
#     Don't use HAVE_UNION_WAIT, because the autoconf documentation implies
#     that if we've got union-wait, we'll automatically use it.
#
# Garrett Wollman adds:
#	The tests described above don't quite do the right thing,
#	since some systems have hacks which allow `union wait' to
#	still work even though `int' is preferred (and generates
#	fewer warnings).  Since all of these systems use prototypes,
#	we can use the prototype of wait(2) to disambiguate them.
#
# Alexandre Oliva adds:
#     A single compile check is enough.  If we don't have union wait,
#     it's obvious that the test will fail, and that we must use int.
#     If we do, the prototype (on STDC systems) and WIFEXITED will tell
#     whether we're supposed to be using union wait instead of int.
#
AC_DEFUN([CF_WAIT], [
    AC_REQUIRE([AC_TYPE_PID_T])
    AC_HAVE_HEADERS(sys/wait.h wait.h)
    AC_CACHE_CHECK([whether wait uses union wait], [cf_cv_arg_union_wait], [
        AC_TRY_COMPILE([
#include <sys/types.h>

#if HAVE_SYS_WAIT_H
# include <sys/wait.h>
#else
# if HAVE_WAIT_H
#  include <wait.h>
# endif
#endif

#ifdef __STDC__
pid_t wait(union wait *);
#endif
], [
  union wait x; int i;
  wait(&x); i = WIFEXITED(x)
], [cf_cv_arg_union_wait=yes], [cf_cv_arg_union_wait=no])])
    if test $cf_cv_arg_union_wait = yes; then
	    AC_DEFINE(WAIT_USES_UNION,1,
		[Defined if wait() puts the status in a union wait instead of int. ])
    fi
])

# SYNOPSIS
#
#   CF_WAIT_INT
#
# OVERVIEW
#
# Test for the presence of <sys/wait.h>, 'union wait', arg-type of 'wait()'.
# by T.E.Dickey" , Jim Spath <jspath@mail.bcpl.lib.md.us>
#
#   DEFINEs WAIT_USES_INT if an int result type is found.
#
AC_DEFUN([CF_WAIT_INT], [
    AC_REQUIRE([AC_TYPE_PID_T])
    AC_HAVE_HEADERS(sys/wait.h wait.h)
    AC_CACHE_CHECK([whether wait uses int], [cf_cv_arg_int], [
        AC_TRY_COMPILE([
#include <sys/types.h>

#if HAVE_SYS_WAIT_H
# include <sys/wait.h>
#else
# if HAVE_WAIT_H
#  include <wait.h>
# endif
#endif

#ifdef __STDC__
pid_t wait(int *);
#endif
], [
  int x; int i;
  wait(&x); i = WIFEXITED(x)
], [cf_cv_arg_int=yes], [cf_cv_arg_int=no])])
if test $cf_cv_arg_int = yes; then
        AC_DEFINE(WAIT_USES_INT,1,
	    [Defined if wait() puts the status in a int instead of a union wait. ])
fi
])

