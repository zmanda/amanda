# SYNOPSIS
#
#   AMANDA_NDMP_DEVICE
#
# OVERVIEW
#
#   Set up for the 'ndmp' device.  WANT_NDMP_DEVICE is
#   defined and AM_CONDITIONAL'd if the ndmp device should be supported,
#   and a check is made that NDMP is being built
#
AC_DEFUN([AMANDA_NDMP_DEVICE], [
    AC_REQUIRE([AMANDA_CHECK_COMPONENTS])

    AC_ARG_ENABLE([ndmp-device],
        AS_HELP_STRING([--disable-ndmp-device],
                       [disable the NDMP device]),
        [ WANT_NDMP_DEVICE=$enableval ], [ WANT_NDMP_DEVICE=$WANT_NDMP ])

    if test x"$WANT_NDMP" != x"true" -a x"$WANT_NDMP_DEVICE" = x"true"; then
	AC_MSG_ERROR([NDMP support is required to build the ndmp device (--with-ndmp)])
    fi

    if test x"$WANT_NDMP_DEVICE" = x"true"; then
	AC_DEFINE(WANT_NDMP_DEVICE, 1, [Compile NDMP device])
    fi

    AM_CONDITIONAL([WANT_NDMP_DEVICE], [test x"$WANT_NDMP_DEVICE" = x"true"])
])
