# SYNOPSIS
#
#   AMANDA_CHECK_READDIR
#
# OVERVIEW
#
#   Check for one of the readdir variants, as well as the dirent headers.
#   See common-src/util.c and amanda.h for the use of these symbols.
#
AC_DEFUN([AMANDA_CHECK_READDIR], [
    AC_HEADER_DIRENT

    # include the dirent headers as described in the autoconf documentation.
    AC_CHECK_DECLS([readdir, readdir_r, readdir64, readdir64_r],,,[
#if HAVE_DIRENT_H
# include <dirent.h>
# define NAMLEN(dirent) strlen((dirent)->d_name)
#else
# define dirent direct
# define NAMLEN(dirent) (dirent)->d_namlen
# if HAVE_SYS_NDIR_H
#  include <sys/ndir.h>
# endif
# if HAVE_SYS_DIR_H
#  include <sys/dir.h>
# endif
# if HAVE_NDIR_H
#  include <ndir.h>
# endif
#endif
    ])
])
