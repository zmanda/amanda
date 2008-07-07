# SYNOPSIS
#
#   AMANDA_S3_DEVICE
#
# OVERVIEW
#
#   Perform the necessary checks for the S3 Device.  If the S3 device should be built,
#   WANT_S3_DEVICE is DEFINEd and set up as an AM_CONDITIONAL.
#
AC_DEFUN([AMANDA_S3_DEVICE], [
    AC_REQUIRE([AMANDA_CHECK_LIBCURL])
    AC_REQUIRE([AMANDA_CHECK_HMAC])

    AC_ARG_ENABLE([s3-device],
	AS_HELP_STRING([--disable-s3-device],
		       [disable the S3 device]),
	[ WANT_S3_DEVICE=$enableval ], [ WANT_S3_DEVICE=maybe ])

    AC_MSG_CHECKING([whether to include the Amazon S3 device])
    # if the user didn't specify 'no', then check for support
    if test x"$WANT_S3_DEVICE" != x"no"; then
	if test x"$HAVE_CURL" = x"yes" -a x"$HAVE_HMAC" = x"yes"; then
	    WANT_S3_DEVICE=yes
	else
	    # no support -- if the user explicitly enabled the device,
	    # then this is an error
	    if test x"$WANT_S3_DEVICE" = x"yes"; then
		AC_MSG_RESULT(no)
		AC_MSG_ERROR([Cannot build the Amazon S3 device: one or more prerequisites are missing.])
	    else
		WANT_S3_DEVICE=no
	    fi
	fi
    fi
    AC_MSG_RESULT($WANT_S3_DEVICE)

    AM_CONDITIONAL([WANT_S3_DEVICE], [test x"$WANT_S3_DEVICE" = x"yes"])

    # Now handle any setup for S3, if we want it.
    if test x"$WANT_S3_DEVICE" = x"yes"; then
	AC_DEFINE(WANT_S3_DEVICE, [], [Compile Amazon S3 driver])
    fi
])
