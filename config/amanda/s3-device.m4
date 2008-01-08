# SYNOPSIS
#
#   AMANDA_S3_DEVICE
#
# OVERVIEW
#
#   Perform the necessary checks for the S3 Device.  If the S3 device should be built,
#   WANT_S3_DEVICE is DEFINEd and set up as an AM_CONDITIONAL.
#
#   The subsidiary DevPay support, if enabled, defines and AM_CONDITIONALizes
#   WANT_DEVPAY.
#
AC_DEFUN([AMANDA_S3_DEVICE], [
    AC_REQUIRE([AMANDA_CHECK_LIBCURL])
    AC_REQUIRE([AMANDA_CHECK_HMAC])

    if test "$libcurl_feature_SSL" != "yes" ||
       test "$libcurl_protocol_HTTPS" != "yes"; then
        s3_ssl=no
    else
	s3_ssl=yes
    fi

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
	if test x"$s3_ssl" = x"no"; then
	  AMANDA_MSG_WARN([Encryption support is not available for S3; requests will be sent in plaintext.])
	fi
    fi
			  

    AC_ARG_ENABLE([devpay],
		  AS_HELP_STRING([--enable-devpay],
				 [Use devpay authentication for Amazon S3 driver]),
		  [WANT_DEVPAY=$enableval], [WANT_DEVPAY=no])

    AC_MSG_CHECKING([whether to include the Amazon S3 device's DevPay support])
    if test x"$WANT_DEVPAY" = x"yes"; then
	if test x"$WANT_S3_DEVICE" != x"yes"; then
	    AC_MSG_RESULT(no)
	    AC_MSG_ERROR([DevPay support requires the S3 device (--enable-s3-device)])
	fi

	if test "$s3_ssl" != "yes"; then
	    AC_MSG_RESULT(no)
	    AC_MSG_ERROR([Cannot use devpay without HTTPS/SSL support in libcurl.])
	fi

	AC_DEFINE([WANT_DEVPAY], [], [Compile Amazon DevPay support])
    fi
    AC_MSG_RESULT($WANT_DEVPAY)

    AM_CONDITIONAL([WANT_DEVPAY], [test "$WANT_DEVPAY" = "yes"])
])
