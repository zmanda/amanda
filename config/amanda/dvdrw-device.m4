# SYNOPSIS
#
#   AMANDA_DVDRW_DEVICE
#
# OVERVIEW
#
#   Perform the necessary checks for the DVDRW Device.  If the DVDRW device should be built,
#   WANT_DVDRW_DEVICE is DEFINEd and set up as an AM_CONDITIONAL.
#
AC_DEFUN([AMANDA_DVDRW_DEVICE], [
	AC_ARG_ENABLE([dvdrw-device],
	AS_HELP_STRING([--disable-dvdrw-device],
		[disable the DVD-RW device]),
	[ WANT_DVDRW_DEVICE=$enableval ], [ WANT_DVDRW_DEVICE=yes ])

	AC_MSG_CHECKING([whether to include the DVD-RW device])
	AC_MSG_RESULT($WANT_DVDRW_DEVICE)

	AM_CONDITIONAL([WANT_DVDRW_DEVICE], [test x"$WANT_DVDRW_DEVICE" = x"yes"])

	# Now handle any setup for DVDRW, if we want it.
	if test x"$WANT_DVDRW_DEVICE" = x"yes"; then
	AC_DEFINE(WANT_DVDRW_DEVICE, [], [Compile DVD-RW driver])
	fi
])
