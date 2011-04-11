# SYNOPSIS
#
#   AMANDA_AS_NEEDED
#
# OVERVIEW
#   Add --as-needed ld flag to LD_FLAGS except when using readline. On
#   some platforms, readline does not correctly advertise it's requirements,
#   causing ld errors.  In Amanda, readline is used by amrecover/amoldrecover.
#
AC_DEFUN([AMANDA_AS_NEEDED], [
    AC_ARG_ENABLE([as-needed],
	AS_HELP_STRING([--enable-as-needed],
		       [enable use od --as-needed linker flag]),
	[ WANT_AS_NEEDED=$enableval ], [ WANT_AS_NEEDED=no ])

    AM_CONDITIONAL([WANT_AS_NEEDED], [test x"$WANT_AS_NEEDED" = x"yes"])

    if test x"$WANT_AS_NEEDED" = x"yes"; then
	AS_NEEDED_FLAGS="-Wl,--as-needed"
    else
	AS_NEEDED_FLAGS=""
    fi
    AC_SUBST(AS_NEEDED_FLAGS)
])

