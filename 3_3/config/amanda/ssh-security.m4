# SYNOPSIS
#
#   AMANDA_SSH_SECURITY
#
# OVERVIEW
#
#   Handle configuration for SSH security, implementing the --with-ssh-security
#   option and checking for the relevant programs and options.  Defines and substitutes
#   SSH_SECURITY, searches for and defines SSH, and defines SSH_OPTIONS.
#
AC_DEFUN([AMANDA_SSH_SECURITY],
[
    SSH_SECURITY=yes
    AC_ARG_WITH(ssh-security,
        AS_HELP_STRING([--with-ssh-security], 
                [include SSH authentication]),
        [
            case "$withval" in
                n | no) SSH_SECURITY=no ;;
                y |  ye | yes) SSH_SECURITY=yes ;;
                *) AC_MSG_ERROR([*** You must not supply an argument to --with-ssh-security.])
              ;;
            esac
        ],
    )

    if test "x$SSH_SECURITY" = "xyes"; then
        # find the SSH binary
        AC_PATH_PROGS(SSH, ssh, , $LOCSYSPATH)

        # see what options we should use
        AC_ARG_WITH(ssh-options,
            AS_HELP_STRING([ --with-ssh-options=@<:@OPTIONS@:>@],
               [Use these ssh options for ssh security; the default should work]),
            [ SSH_OPTIONS="$withval" ],
            [ SSH_OPTIONS='' ]
        )

        case "$SSH_OPTIONS" in
            y | ye | yes | n | no)
                AC_MSG_ERROR([*** You must supply an argument to --with-ssh-options.]);;
            *) : ;;
        esac

        AC_MSG_CHECKING([SSH options])
        # if we didn't get SSH options from the user, figure them out for ourselves
        if test -z "$SSH_OPTIONS"; then
            case `$SSH -V 2>&1` in
                OpenSSH*) SSH_OPTIONS='-x -o BatchMode=yes -o PreferredAuthentications=publickey';;
                *) SSH_OPTIONS='-x -o BatchMode=yes' ;;
            esac
        fi

        # now convert that to a comma-separated list of C strings
        eval "set dummy ${SSH_OPTIONS}"; shift
        SSH_OPTIONS=''
	for i in "${@}"; do 
	    quoted="\"`echo "$i" | sed -e 's/\"/\\\"/'`\""
	    SSH_OPTIONS="${SSH_OPTIONS}${SSH_OPTIONS:+, }$quoted"; 
	done
        AC_MSG_RESULT($SSH_OPTIONS)

        # finally, make the various outputs for all of this
        AC_DEFINE(SSH_SECURITY,1,
                [Define if SSH transport should be enabled. ])
        AC_DEFINE_UNQUOTED(SSH, "$SSH", 
                [Path to the SSH binary])
        AC_DEFINE_UNQUOTED(SSH_OPTIONS, $SSH_OPTIONS, 
                [Arguments to ssh])
    fi
    AM_CONDITIONAL(WANT_SSH_SECURITY, test x"$SSH_SECURITY" = x"yes")

    AC_SUBST(SSH_SECURITY)
    # (note -- don't just substitute SSH_OPTIONS -- shell quoting will break)
])
