# SYNOPSIS
#
#   AMANDA_SETUP_GETFSENT
#
# OVERVIEW
#
#   Checks for support for client-src/getfsent.c
#
AC_DEFUN([AMANDA_SETUP_GETFSENT], [
    AC_CHECK_HEADERS(
	fstab.h \
	mntent.h \
	mnttab.h \
	sys/vfstab.h \
    )

    AC_CHECK_FUNCS(endmntent)
    AC_CHECK_FUNCS(setmntent)
])
