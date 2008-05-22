#
#   Find ps argument for Amanda::Process
#
AC_DEFUN([AMANDA_PS_ARGUMENT],
[
    AC_MSG_CHECKING([ps argument to use])
    ps -eo pid,ppid,comm >/dev/null 2>/dev/null
    if test $? -eq 0; then
	PS_ARGUMENT="-eo pid,ppid,comm"
    else
	ps -aAco pid,ppid,command >/dev/null 2>/dev/null
	if test $? -eq 0; then
	    PS_ARGUMENT="-aAco pid,ppid,command"
	else
	    AC_MSG_ERROR([Can't find ps argument to use.])
	fi
    fi
    AC_MSG_RESULT(PS_ARGUMENT)
    AC_SUBST(PS_ARGUMENT)
])
