# SYNOPSIS
#
#   AMANDA_CHECK_DEVICES
#
# OVERVIEW
#
#
AC_DEFUN([AMANDA_CHECK_DEVICES], [
    AC_REQUIRE([AMANDA_S3_DEVICE])
    AC_REQUIRE([AMANDA_TAPE_DEVICE])
    AC_REQUIRE([AMANDA_DVDRW_DEVICE])
    AC_REQUIRE([AMANDA_NDMP_DEVICE])

    amanda_devices=' file null rait tape'
    missing_devices=''

    if test x"$WANT_DVDRW_DEVICE" = x"yes"; then
	amanda_devices="$amanda_devices dvdrw";
    else
	missing_devices="$missing_devices (no dvdrw)";
    fi
    if test x"$WANT_NDMP_DEVICE" = x"true"; then
	amanda_devices="$amanda_devices ndmp";
    else
	missing_devices="$missing_devices (no ndmp)";
    fi
    if test x"$WANT_S3_DEVICE" = x"yes"; then
	amanda_devices="$amanda_devices S3";
    else
	missing_devices="$missing_devices (no S3)";
    fi
    AMANDA_DEVICES=$amanda_devices
    AC_SUBST(AMANDA_DEVICES)
])


# SYNOPSIS
#
#   AMANDA_SHOW_DEVICES_SUMMARY
#
# OVERVIEW
#
#   Show a summary of the settings from this file.
#
AC_DEFUN([AMANDA_SHOW_DEVICES_SUMMARY],
[
    echo "Amanda Devices:$amanda_devices$missing_devices"
])
