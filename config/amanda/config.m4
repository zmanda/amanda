# SYNOPSIS
#
#   AMANDA_CONFIG_LCOAL
#
# OVERVIEW
#
#   Invoke ./config.local, if it exists
#
AC_DEFUN([AMANDA_CONFIG_LOCAL],
[
    if test -f config.local; then
	echo "running local script ./config.local"
	. ./config.local
    fi
])
