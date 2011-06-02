# SYNOPSIS
#
#   AMANDA_CHECK_DEVICE_PREFIXES
#
# DESCRIPTION
#
#   Check for the prefixes used for particular devices.
#
#   Defines DEV_PREFIX to the appropriate prefix.
#
AC_DEFUN([AMANDA_CHECK_DEVICE_PREFIXES],
[
    # disk device prefixes
    AC_MSG_CHECKING(disk device prefixes)

    # Use df to find the mount point for the root filesystem.  Use
    # the positional parameters to find the particular line from df
    # that contains the root paritition.  We put it in a subshell so
    # that the original positional parameters are not messed with.
    dfline=`(
	df / | while read line; do
	    set -- $line
	    dnl @S|@ means $ to m4
	    while test @S|@# -gt 0; do
		if test "x@S|@1" = "x/"; then
		    echo $line
		    break 2
		fi
		shift
	    done
	done
    ) | sed 's/(//' | sed 's/)//' `

    # Search for the mount point by using expr to find the parameter
    # with dev in it.
    mount=`(
	set -- $dfline
	dnl @S|@ means $ to m4
	while test @S|@# -gt 0; do
	    if expr "@S|@1" : '.*dev' >/dev/null 2>&1; then
		echo @S|@1
		break
	    fi
	    shift
	done
    )`

    # get any system-specific configuration information
    case "$host" in
	*-hp-*)
	    CLIENT_SCRIPTS_OPT=amhpfixdevs
	    case $mount in
		/dev/vg*)
		    AMANDA_MSG_WARN([Run amhpfixdevs on HP-UX systems using /dev/vg??.])
		    ;;
	    esac
	    ;;
	*-sni-sysv4)
	    DEV_PREFIX=/dev/dsk/
	    CLIENT_SCRIPTS_OPT=amsinixfixdevs
	    if ! test -d /dev/dsk; then
		AMANDA_MSG_WARN([Run amsinixfixdevs on Sinix systems using VxFS.])
	    fi
	    ;;
	*-sco3.2v4*)
	    DEV_PREFIX=/dev/
	    ;;
	*)
	    CLIENT_SCRIPTS_OPT=
	    ;;
    esac

    if test "$DEV_PREFIX"; then
	AC_MSG_RESULT((predefined) $DEV_PREFIX)
    else
	if test -d /dev/dsk; then
	    DEV_PREFIX=/dev/dsk/
	elif test -d /dev; then
	    DEV_PREFIX=/dev/

	else
	    # just fake it..
	    DEV_PREFIX=/
	fi
	AC_MSG_RESULT($DEV_PREFIX)
    fi

    AC_DEFINE_UNQUOTED(DEV_PREFIX,"${DEV_PREFIX}",
	[Define as the prefix for disk devices, commonly /dev/ or /dev/dsk/ ])
    AC_SUBST(CLIENT_SCRIPTS_OPT)
])
