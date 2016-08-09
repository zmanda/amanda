# SYNOPSIS
#
#   AMANDA_WITH_FAILURE_CODE
#
# OVERVIEW
#
#   define FAILURE_CODE if we want error injection the code.
#
AC_DEFUN([AMANDA_WITH_FAILURE_CODE],
[
    FAILURE_CODE=${FAILURE_CODE:-no}

    AC_ARG_WITH(failure-code,
        AS_HELP_STRING([--with-failure-code]
            [Allow error injection in the code (for testing)]),
        [   FAILURE_CODE=$withval ])

    if test x"$FAILURE_CODE" = x"yes"; then
        AC_DEFINE(FAILURE_CODE, 1,
	    [Define if we want error injection the code (for installchecks)])
	FAILURE_CODE=1
    else
	FAILURE_CODE=0
    fi
    AC_SUBST(FAILURE_CODE)
])
