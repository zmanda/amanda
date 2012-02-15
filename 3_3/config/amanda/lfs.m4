# SYNOPSIS
#
#   AMANDA_SETUP_LFS
#
# OVERVIEW
#
#   Set up for large file suport on this system.  Besides adding compiler flags,
#   defines NEED_RESETOFS if the tape device's offset must be reset before it reaches
#   2GB (a Linux kernel bug in systems without LFS support).
#
AC_DEFUN([AMANDA_SETUP_LFS],
[
    AC_REQUIRE([AMANDA_PROG_GETCONF])
    AC_REQUIRE([AC_SYS_LARGEFILE])

    # we use 'getconf', if it exists, to get the relevant
    # compiler flags.
    GETCONF_LFS="LFS"
    case "$host" in
        *-hp-*) GETCONF_LFS="XBS5_ILP32_OFFBIG" ;;
        *-ibm-aix*) GETCONF_LFS="XBS5_ILP32_OFFBIG" ;;
    esac

    # Checks for compilers, typedefs, structures, and compiler characteristics.
    # Check for large file compilation environment.
    NEED_RESETOFS=yes
    AC_CACHE_CHECK([for large file compilation CFLAGS],
        amanda_cv_LFS_CFLAGS,
        [
        amanda_cv_LFS_CFLAGS=
        if test "$GETCONF"; then
            if $GETCONF ${GETCONF_LFS}_CFLAGS >/dev/null 2>&1; then
                amanda_cv_LFS_CFLAGS=`$GETCONF ${GETCONF_LFS}_CFLAGS 2>/dev/null`
                NEED_RESETOFS=no
            fi
        fi
        ]
    )
    AMANDA_ADD_CFLAGS([$amanda_cv_LFS_CFLAGS])

    AC_CACHE_CHECK(
        [for large file compilation LDFLAGS],
        amanda_cv_LFS_LDFLAGS,
        [
        amanda_cv_LFS_LDFLAGS=
        if test "$GETCONF"; then
            if $GETCONF ${GETCONF_LFS}_LDFLAGS >/dev/null 2>&1; then
                amanda_cv_LFS_LDFLAGS=`$GETCONF ${GETCONF_LFS}_LDFLAGS 2>/dev/null`
                NEED_RESETOFS=no
            fi
        fi
        ]
    )
    AMANDA_ADD_LDFLAGS([$amanda_cv_LFS_LDFLAGS])

    AC_CACHE_CHECK(
        [for large file compilation LIBS],
        amanda_cv_LFS_LIBS,
        [
        amanda_cv_LFS_LIBS=
        if test "$GETCONF"; then
            if $GETCONF ${GETCONF_LFS}_LIBS >/dev/null 2>&1; then
                amanda_cv_LFS_LIBS=`$GETCONF ${GETCONF_LFS}_LIBS 2>/dev/null`
                NEED_RESETOFS=no
            fi
        fi
        ]
    )
    AMANDA_ADD_LIBS([$amanda_cv_LFS_LIBS])

    if test x"$NEED_RESETOFS" = x"yes"; then
        AC_DEFINE(NEED_RESETOFS,1,
            [Define if we have to reset tape offsets when reaching 2GB. ])
    fi
])
