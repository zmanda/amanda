# SYNOPSIS
#
#   AMANDA_FUNC_SHM_ARG_TYPE
#
# OVERVIEW
#
#   Determine the type of the second argument to shmdt/shmat, defining
#   that type (without the *) in SHM_ARG_TYPE.
#
AC_DEFUN([AMANDA_FUNC_SHM_ARG_TYPE], [
	AC_CHECK_HEADERS(
	    sys/types.h \
	    sys/ipc.h \
	    sys/shm.h \
	)

	AC_CACHE_CHECK(
	    [for shmdt() argument type],
	    amanda_cv_shmdt_arg_type,
	    [
		if test "$ac_cv_func_shmget" = yes; then
		    cat <<EOF >conftest.$ac_ext
#include "confdefs.h"
#ifdef HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#ifdef HAVE_SYS_IPC_H
# include <sys/ipc.h>
#endif
#ifdef HAVE_SYS_SHM_H
# include <sys/shm.h>
#endif

#ifdef __cplusplus
extern "C" void *shmat(int, void *, int);
#else
void *shmat();
#endif

int main()
{
    int i;
    return 0;
}
EOF
		    ${CC-cc} -c $CFLAGS $CPPFLAGS conftest.$ac_ext >/dev/null 2>/dev/null
		    if test $? = 0; then
			amanda_cv_shmdt_arg_type=void
		    else
			amanda_cv_shmdt_arg_type=char
		    fi
		    rm -f conftest*
		else
		    amanda_cv_shmdt_arg_type=nothing
		fi
	    ]
	)
	AC_DEFINE_UNQUOTED(SHM_ARG_TYPE,$amanda_cv_shmdt_arg_type,
	    [Define to type of shmget() function argument. ])
    ]
)

# SYNOPSIS
#
#   AMANDA_CHECK_SHMEM
#
# OVERVIEW
#
#   Check for shared memory support; checks for the --with-mmap option,
#   and then ensures that the proper compilation infrastructure is in place
#   for either mmap or shared memory support.
#
#   Defines several HAVE_*_DECL symbols via ICE_CHECK_DECL, as well as 
#   HAVE_SYSVSHM if shared memory support is discovered.
#
AC_DEFUN([AMANDA_CHECK_SHMEM],
[
    AC_REQUIRE([AC_HEADER_STDC])
    AC_ARG_WITH(mmap,
	AS_HELP_STRING([--with-mmap],
	    [force use of mmap instead of shared memory support]),
	[
	    case "$FORCE_MMAP" in
		y | ye | yes | n | no) : ;;
		*) AC_MSG_ERROR([*** You must not supply an argument to --with-mmap.]) ;;
	    esac
	    FORCE_MMAP=$withval
	],
	[ : ${FORCE_MMAP=no} ]
    )


    AC_CHECK_HEADERS(\
	    sys/shm.h \
	    sys/mman.h \
    )

    AC_FUNC_MMAP

    AC_CHECK_FUNCS(shmget,
	[
	    AMANDA_FUNC_SHM_ARG_TYPE
	    case "$FORCE_MMAP" in
	    n | no)
		AC_DEFINE(HAVE_SYSVSHM,1,
		    [Define if SysV shared-memory functions are available. ])
	      ;;
	    esac
	]
    )
    ICE_CHECK_DECL(shmat,sys/types.h sys/ipc.h sys/shm.h)
    ICE_CHECK_DECL(shmctl,sys/types.h sys/ipc.h sys/shm.h)
    ICE_CHECK_DECL(shmdt,sys/types.h sys/ipc.h sys/shm.h)
    ICE_CHECK_DECL(shmget,sys/types.h sys/ipc.h sys/shm.h)

    if test "x$ac_cv_func_mmap_fixed_mapped" != xyes; then
	case "$FORCE_MMAP" in
	n | no)
	    if test "x$ac_cv_func_shmget" != xyes; then
		AMANDA_MSG_WARN([Neither shmget() nor mmap() found. This system will not support the Amanda server.])
		NO_SERVER_MODE=true
	    fi
	  ;;
	y | ye | yes)
	    AMANDA_MSG_WARN([--with-mmap used on a system with no mmap() support.  This system will not support the Amanda server.])
	    NO_SERVER_MODE=true
	  ;;
	esac
    fi
])
