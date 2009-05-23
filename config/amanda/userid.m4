# SYNOPSIS
#
#   AMANDA_DISABLE_INSTALLPERMS
#
# OVERVIEW
#
#   Handle the --disable-installperms option, which disables all post-install
#   chown/chmod operations.  This is useful when packaging, as most packaging
#   systems build as non-root, and apply permissions in the post-install step of
#   the package itself.
#
AC_DEFUN([AMANDA_DISABLE_INSTALLPERMS],
[
    WANT_INSTALLPERMS=yes
    AC_ARG_ENABLE(installperms,
        AS_HELP_STRING([--disable-installperms],
                [do not modify ownership and permissions on installed files]),
        [ WANT_INSTALLPERMS="$enableval" ],
        [ WANT_INSTALLPERMS="yes" ]
    )
    AM_CONDITIONAL(WANT_INSTALLPERMS, test x"$WANT_INSTALLPERMS" = x"yes")
])

# SYNOPSIS
#
#   AMANDA_WITH_FORCE_UID
#
# OVERVIEW
#
#   Handle the --without-force-id option, which disables userid checks for
#   all Amanda applications.  Defines and substitutes CHECK_USERID *unless* 
#   this option is given.
#
AC_DEFUN([AMANDA_WITH_FORCE_UID],
[
    AC_ARG_WITH(force-uid,
        AS_HELP_STRING([--without-force-uid],
                [do not check userids when running programs]),
        CHECK_USERID="$withval",
        : ${CHECK_USERID=yes}
    )
    case "$CHECK_USERID" in
        y | ye | yes) 
	    CHECK_USERID=1
            AC_DEFINE(CHECK_USERID, 1,
                [Define to force to another user on client machines. ])
          ;;
        n | no) :
	    CHECK_USERID=
          ;;
        *)
            AC_MSG_ERROR([*** You must not supply an argument to --with-force-uid option.])
    esac
    AC_SUBST(CHECK_USERID)
])

# SYNOPSIS
#
#   AMANDA_WITH_USER
#
# OVERVIEW
#
#   Handle the --with-user option, which sets the userid Amanda expects to run
#   under.  Defines and substitutes CLIENT_LOGIN.
#
AC_DEFUN([AMANDA_WITH_USER],
[
    AC_ARG_WITH(user,
        AS_HELP_STRING([--with-user=USER],
                [force execution to USER on client systems (REQUIRED)]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-user option.])
                  ;;
                *) 
                    CLIENT_LOGIN="$withval"
                  ;;
            esac
        ], [
            AMANDA_MSG_WARN([[no user specified (--with-user) -- using 'amanda']])
	    CLIENT_LOGIN=amanda
        ]
    )

    AC_DEFINE_UNQUOTED(CLIENT_LOGIN,"$CLIENT_LOGIN",
        [Define as a the user to force to on client machines. ])
    AC_SUBST(CLIENT_LOGIN)
])

# SYNOPSIS
#
#   AMANDA_WITH_GROUP
#
# OVERVIEW
#
#   Handle the --with-group option, which sets the groupid Amanda expects to run
#   under.  Substitutes (but does not define) SETUID_GROUP.
#
AC_DEFUN([AMANDA_WITH_GROUP],
[
    AC_ARG_WITH(group,
        AS_HELP_STRING([--with-group=GROUP],
            [group allowed to execute setuid-root programs (REQUIRED)]),
        [
            case "$withval" in
                "" | y | ye | yes | n | no)
                    AC_MSG_ERROR([*** You must supply an argument to the --with-group option.])
                  ;;
                *) SETUID_GROUP="$withval"
                  ;;
            esac
        ], [
            AMANDA_MSG_WARN([[no group specified (--with-group) -- using 'backup']])
	    CLIENT_LOGIN=backup
        ]
    )
    AC_SUBST(SETUID_GROUP)
])

# SYNOPSIS
#
#   AMANDA_WITH_OWNER
#
# OVERVIEW
#
#   Handle the --with-owner option, which sets the userid the 'make install' process
#   will use.  Substitutes and defines BINARY_OWNER.
#
AC_DEFUN([AMANDA_WITH_OWNER],
[
    AC_REQUIRE([AMANDA_WITH_USER])
    AC_ARG_WITH(owner,
        AS_HELP_STRING([--with-owner=USER]
            [force ownership of installed files to USER (default same as --with-user)]),
        [
            case "$withval" in
            "" | y | ye | yes | n | no)
                AC_MSG_ERROR([*** You must supply an argument to the --with-owner option.])
              ;;
            *) BINARY_OWNER="$withval"
              ;;
            esac
        ], [
            BINARY_OWNER="$CLIENT_LOGIN"
        ]
    )
    AC_DEFINE_UNQUOTED(BINARY_OWNER,"$BINARY_OWNER",
        [Define as the user who owns installed binaries. ])
    AC_SUBST(BINARY_OWNER)
])

# SYNOPSIS
#
#   AMANDA_WITH_SINGLE_USERID
#
# OVERVIEW
#
#   Check if this system is one on which clients should be built setuid, 
#   Sets up AM_CONDITIONAL/define WANT_SETUID_CLIENT and defines 
#   SINGLE_USERID if either the system requires it or the user specified it.
#
AC_DEFUN([AMANDA_WITH_SINGLE_USERID],
[
    SINGLE_USERID=${SINGLE_USERID:-no}
    WANT_SETUID_CLIENT=${WANT_SETUID_CLIENT:-true}

    AC_ARG_WITH(single-userid,
        AS_HELP_STRING([--with-single-userid]
            [force amanda to run as a single userid (for testing)]),
        [   SINGLE_USERID=$withval ])

    case "$host" in
        *-pc-cygwin)
            WANT_SETUID_CLIENT=false
	    SINGLE_USERID=yes
            ;;
    esac

    if test x"$WANT_SETUID_CLIENT" = x"true"; then
        AC_DEFINE(WANT_SETUID_CLIENT,1,
            [Define if clients should be built setuid-root])
    fi
    AM_CONDITIONAL(WANT_SETUID_CLIENT, test x"$WANT_SETUID_CLIENT" = x"true")

    if test x"$SINGLE_USERID" = x"yes"; then
        AC_DEFINE(SINGLE_USERID, 1,
	    [Define if all of Amanda will run as a single userid (e.g., on Cygwin or for installchecks)])
    fi
])
