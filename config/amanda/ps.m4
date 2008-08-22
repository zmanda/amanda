#
#   Find ps argument for Amanda::Process
#
AC_DEFUN([AMANDA_PS_ARGUMENT],
[
    AC_MSG_CHECKING([ps argument to use])
    PS_ARGUMENT=
    for try in \
	"-eo pid,ppid,comm" \
	"-aAco pid,ppid,command" \
	"-axo pid,ppid,command"
    do
	ps $try >/dev/null 2>/dev/null
	if test $? -eq 0; then
	    PS_ARGUMENT="$try"
	    break
	fi
    done

    if test -z "$PS_ARGUMENT"; then
	case "$target" in
	    *-pc-cygwin)
	       PS_ARGUMENT=CYGWIN
	       ;;
	    *)
	       AC_MSG_ERROR([Can't find ps argument to use.])
	       ;;
	esac
    fi
    AC_MSG_RESULT($PS_ARGUMENT)
    AC_SUBST(PS_ARGUMENT)
])
