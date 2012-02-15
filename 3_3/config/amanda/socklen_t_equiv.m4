# SYNOPSIS
#
#   AMANDA_SOCKLEN_T_EQUIV
#
# OVERVIEW
#
#   Find a type which will work like socklen_t should.  Unfortunately, 
#   HP/UX systems define socklen_t, but use int * as the result parameter
#   for socket functions returning a socket length.
#
#   This check defines a type socklen_t_equiv which is of the appropriate
#   size to be used with socket functions.
#
AC_DEFUN([AMANDA_SOCKLEN_T_EQUIV],
[
      ## lifted from config/gnulib/socklen.m4
      AC_REQUIRE([gl_HEADER_SYS_SOCKET])dnl
      AC_MSG_CHECKING([for socklen_t equivalent])
      AC_CACHE_VAL([gl_cv_socklen_t_equiv],
	[# Systems have either "struct sockaddr *" or
	 # "void *" as the second argument to getpeername
	 gl_cv_socklen_t_equiv=
	 for arg2 in "struct sockaddr" void; do
	   for t in socklen_t int size_t "unsigned int" "long int" "unsigned long int"; do
	     AC_TRY_COMPILE(
	       [#include <sys/types.h>
		#include <sys/socket.h>

		int getpeername (int, $arg2 *, $t *);],
	       [$t len;
		getpeername (0, 0, &len);],
	       [gl_cv_socklen_t_equiv="$t"])
	     test "$gl_cv_socklen_t_equiv" != "" && break
	   done
	   test "$gl_cv_socklen_t_equiv" != "" && break
	 done
      ])
      ## end lifting from config/gnulib/socklen.m4
      # fallback if the check fails
      if test "$gl_cv_socklen_t_equiv" = ""; then
	gl_cv_socklen_t_equiv=socklen_t
      fi
      AC_MSG_RESULT([$gl_cv_socklen_t_equiv])

      AC_DEFINE_UNQUOTED([socklen_t_equiv], [$gl_cv_socklen_t_equiv],
	[type to use for socket length parameters; use instead of socklen_t])
])

