#
#   Find ps argument for Amanda::Process
#
AC_DEFUN([AMANDA_PS_ARGUMENT],
[
    AC_PATH_PROG(PS, ps)
    AC_MSG_CHECKING([ps argument to use])
    PS_ARGUMENT=

    # ps is *very* non-portable, and across many systems, the same option
    # (e.g., -e) can mean different things.  So this macro tries to
    # special-case most known systems, and makes an effort to detect unknown
    # systems
    case "$host" in
	*-*-solaris*)
	    PS_ARGUMENT="-eo pid,ppid,comm"
	    ;;

	*-*-linux-*)
	    PS_ARGUMENT="-eo pid,ppid,command"
	    ;;

	*-*-*bsd*)
	    PS_ARGUMENT="-axo pid,ppid,command"
	    ;;

	*-apple-darwin*)
	    PS_ARGUMENT="-aAco pid,ppid,command"
	    ;;

	*-pc-cygwin)
	    # Cygwin is special-cased in Amanda::Process
	    PS_ARGUMENT=CYGWIN
	    ;;

	*-*-hpux*)
	    # HPUX's 'PS' needs the env var UNIX95 to run in "xpg4" mode
	    PS="UNIX95=1 $PS"
	    PS_ARGUMENT="-eo pid,ppid,comm"
	    ;;

	*)
	    for try in \
		"-axo pid,ppid,command" \
		"-aAco pid,ppid,command" \
		"-eo pid,ppid,comm"
	    do
		ps $try >/dev/null 2>/dev/null
		if test $? -eq 0; then
		    PS_ARGUMENT="$try"
		    break
		fi
	    done
	    if test -z "$PS_ARGUMENT"; then
		AC_MSG_ERROR([Can't find ps argument to use.])
	    fi
	    ;;
    esac

    AC_MSG_RESULT($PS_ARGUMENT)
    AC_SUBST(PS_ARGUMENT)
])
