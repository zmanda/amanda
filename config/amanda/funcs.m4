# SYNOPSIS
#
#   AMANDA_FUNC_SELECT_ARG_TYPE
# 
# OVERVIEW
#
#   Figure out the select() argument type.  DEFINEs SELECT_ARG_TYPE.
#
AC_DEFUN([AMANDA_FUNC_SELECT_ARG_TYPE],
    [
	AC_REQUIRE([AC_HEADER_TIME])
	AC_CHECK_HEADERS(
	    sys/time.h \
	    sys/types.h \
	    sys/select.h \
	    sys/socket.h \
	    unistd.h \
	)

	AC_CACHE_CHECK(
	    [for select() argument type],
	    amanda_cv_select_arg_type,
	    [
		rm -f conftest.c
		cat <<EOF >conftest.$ac_ext
#include "confdefs.h"
#ifdef TIME_WITH_SYS_TIME
#  include <sys/time.h>
#  include <time.h>
#else
#  ifdef HAVE_SYS_TIME_H
#    include <sys/time.h>
#  else
#    include <time.h>
#  endif
#endif
#ifdef HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#  include <sys/select.h>
#endif
#ifdef HAVE_SYS_SOCKET_H
#  include <sys/socket.h>
#endif
#ifdef HAVE_UNISTD_H
# include <unistd.h>
#endif

int main()
{
#ifdef FD_SET_POINTER
	(void)select(0, (fd_set *) 0, (fd_set *) 0, (fd_set *) 0, 0);
#else
	(void)select(0, (int *) 0, (int *) 0, (int *) 0, 0);
#endif
	return 0;
}
EOF

		# Figure out the select argument type by first trying to
		# compile with the fd_set argument.  If the compile fails,
		# then we know to use the int.  If it suceeds, then try to
		# use the int.  If the int fails, then use fd_set.  If
		# both suceeed, then do a line count on the number of
		# lines that the compiler spit out, assuming that the
		# compile outputing more lines had more errors.
		amanda_cv_select_arg_type=no
		select_compile="${CC-cc} -c $CFLAGS $CPPFLAGS"
		$select_compile -DFD_SET_POINTER conftest.$ac_ext 1>conftest.fd_set 2>&1
		if test $? -ne 0; then
		    amanda_cv_select_arg_type=int
		fi
		if test "$amanda_cv_select_arg_type" = no; then
		    $select_compile conftest.$ac_ext 1>conftest.int 2>&1
		    if test $? -ne 0; then
			amanda_cv_select_arg_type=fd_set
		    fi
		fi
		if test "$amanda_cv_select_arg_type" = no; then
		    wc_fdset=`wc -l <conftest.fd_set`
		    wc_int=`wc -l <conftest.int`
		    if test "$wc_fdset" -le "$wc_int"; then
			amanda_cv_select_arg_type=fd_set
		    else
			amanda_cv_select_arg_type=int
		    fi
		fi
		rm -f conftest*
	    ]
	)
	AC_DEFINE_UNQUOTED(SELECT_ARG_TYPE,$amanda_cv_select_arg_type,[Define to type of select arguments. ])
    ]
)

# SYNOPSIS
#
#   AMANDA_FUNC_SETSOCKOPT_SO_SNDTIMEO
#
# OVERVIEW
#
#   Check if setsockopt can use the SO_SNDTIMEO option.
#   This defines HAVE_SO_SNDTIMEO if setsockopt works with SO_SNDTIMEO.
#
AC_DEFUN([AMANDA_FUNC_SETSOCKOPT_SO_SNDTIMEO],
    [
	AC_REQUIRE([AC_HEADER_TIME])
	AC_CHECK_HEADERS(
	    time.h
	    sys/time.h
	)

	AC_CACHE_CHECK(
	    [for setsockopt SO_SNDTIMEO option],
	    amanda_cv_setsockopt_SO_SNDTIMEO,
	    [
		AC_TRY_RUN(
		    [
#include <sys/types.h>
#include <sys/socket.h>
#ifdef TIME_WITH_SYS_TIME
#  include <sys/time.h>
#  include <time.h>
#else
#  ifdef HAVE_SYS_TIME_H
#    include <sys/time.h>
#  else
#    include <time.h>
#  endif
#endif

main() {
#ifdef SO_SNDTIMEO
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    return (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO,
             (void *)&timeout, sizeof(timeout)));
#else
    return -1;
#endif
}
		    ],
		    amanda_cv_setsockopt_SO_SNDTIMEO=yes,
		    amanda_cv_setsockopt_SO_SNDTIMEO=no,
		    amanda_cv_setsockopt_SO_SNDTIMEO=no
		)
	    ]
	)
	if test "$amanda_cv_setsockopt_SO_SNDTIMEO" = yes; then
	    AC_DEFINE(HAVE_SO_SNDTIMEO,1,[Define if SO_SNDTIMEO is available. ])
	fi
    ]
)

# SYNOPSIS
#
#   AMANDA_FUNC_GETTIMEOFDAY_ARGS
#
# OVERVIEW
#
#   Check for the one or two argument version of gettimeofday.  DEFINEs
#   HAVE_TWO_ARG_GETTIMEOFDAY if the two argument version is present.
#
AC_DEFUN([AMANDA_FUNC_GETTIMEOFDAY_ARGS],
    [
	AC_REQUIRE([AC_HEADER_TIME])
	AC_CHECK_HEADERS(
	    time.h
	    sys/time.h
	)

	AC_CACHE_CHECK(
	    [for gettimeofday number of arguments],
	    amanda_cv_gettimeofday_args,
	    [
		AC_TRY_COMPILE(
		    [
#ifdef TIME_WITH_SYS_TIME
#  include <sys/time.h>
#  include <time.h>
#else
#  ifdef HAVE_SYS_TIME_H
#    include <sys/time.h>
#  else
#    include <time.h>
#  endif
#endif
		    ],
		    [
			struct timeval val;
			struct timezone zone;
			gettimeofday(&val, &zone);
		    ],
		    amanda_cv_gettimeofday_args=2,
		    amanda_cv_gettimeofday_args=1
		)
	    ]
	)
	if test "$amanda_cv_gettimeofday_args" = 2; then
	    AC_DEFINE(HAVE_TWO_ARG_GETTIMEOFDAY,1,[Define if gettimeofday takes two arguments. ])
	fi
    ]
)

# SYNOPSIS
#
#   ICE_CHECK_DECL (FUNCTION, HEADER-FILE...)
#
# OVERVIEW
#
#   If FUNCTION is available, define `HAVE_FUNCTION'.  If it is declared
#   in one of the headers named in the whitespace-separated list 
#   HEADER_FILE, define `HAVE_FUNCTION_DECL` (in all capitals).
#
AC_DEFUN([ICE_CHECK_DECL],
[
ice_have_$1=no
AC_CHECK_FUNCS($1, ice_have_$1=yes)
if test "${ice_have_$1}" = yes; then
AC_MSG_CHECKING(for $1 declaration in $2)
AC_CACHE_VAL(ice_cv_have_$1_decl,
[
ice_cv_have_$1_decl=no
changequote(,)dnl
ice_re_params='[a-zA-Z_][a-zA-Z0-9_]*'
ice_re_word='(^|[^a-zA-Z0-9_])'
changequote([,])dnl
for header in $2; do
# Check for ordinary declaration
AC_EGREP_HEADER([${ice_re_word}$1[ 	]*\(], $header, 
	ice_cv_have_$1_decl=yes)
if test "$ice_cv_have_$1_decl" = yes; then
	break
fi
# Check for "fixed" declaration like "getpid _PARAMS((int))"
AC_EGREP_HEADER([${ice_re_word}$1[ 	]*$ice_re_params\(\(], $header, 
	ice_cv_have_$1_decl=yes)
if test "$ice_cv_have_$1_decl" = yes; then
	break
fi
done
])
AC_MSG_RESULT($ice_cv_have_$1_decl)
if test "$ice_cv_have_$1_decl" = yes; then
AC_DEFINE_UNQUOTED([HAVE_]translit($1,[a-z],[A-Z])[_DECL],1,[Define if $1 is declared. ])
fi
fi
])dnl

# SYNOPSIS
#
#   AMANDA_FUNC_SETPGID
#
# OVERVIEW
#
#   Search for the function HAVE_SETPGID, and run an ICE_CHECK_DECL on it if so.
#
AC_DEFUN([AMANDA_FUNC_SETPGID],
[
    AC_CHECK_FUNC(setpgid, [
	AC_DEFINE(HAVE_SETPGID,1,[Define if setpgid() is available. ])
	ICE_CHECK_DECL(setpgid,sys/types.h unistd.h)
    ])
])
