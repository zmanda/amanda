# SYNOPSIS
#
#   AMANDA_CHECK_COMPONENTS
#
# OVERVIEW
#
#   Defines --without-client, --without-server, --without-restore, and 
#   --without-amrecover, and checks the results.
#
#   Sets the AM_CONDITIONALs WANT_CLIENT, WANT_SERVER, WANT_RESTORE, 
#   and WANT_RECOVER.
#   
#   AM_CONDITIONAL WANT_TAPE is set if either server or restore support is
#   being built.
#
AC_DEFUN([AMANDA_CHECK_COMPONENTS], [
    AC_REQUIRE([AMANDA_WITHOUT_SERVER])
    AC_REQUIRE([AMANDA_WITHOUT_CLIENT])
    AC_REQUIRE([AMANDA_WITHOUT_RESTORE])
    AC_REQUIRE([AMANDA_WITHOUT_AMRECOVER])
    AC_REQUIRE([AMANDA_WITH_CLIENT_ONLY]) dnl deprecated
    AC_REQUIRE([AMANDA_WITH_SERVER_ONLY]) dnl deprecated
    AC_REQUIRE([AMANDA_WITHOUT_NDMP])
    AC_REQUIRE([AMANDA_WITHOUT_REST_SERVER])

    # detect invalid combinations of components
    if ! ${WANT_SERVER-true} && ${WANT_RESTORE-true}; then
        AC_MSG_ERROR([--without-server requires --without-restore])
    fi
    if ! ${WANT_CLIENT-true} && ${WANT_RECOVER-true}; then
        AC_MSG_ERROR([--without-client requires --without-amrecover])
    fi

    AM_CONDITIONAL(WANT_CLIENT, $WANT_CLIENT)
    AM_CONDITIONAL(WANT_RESTORE, $WANT_RESTORE)
    AM_CONDITIONAL(WANT_SERVER, $WANT_SERVER)
    AM_CONDITIONAL(WANT_RECOVER, $WANT_RECOVER)
    AM_CONDITIONAL(WANT_NDMP, $WANT_NDMP)
    AM_CONDITIONAL(WANT_REST_SERVER, $WANT_REST_SERVER)

    AM_CONDITIONAL(WANT_TAPE, $WANT_SERVER || $WANT_RESTORE)

    # AMANDA_COMPONENTS goes to Amanda::Constants; missing_components is just for the
    # summary, below
    AMANDA_COMPONENTS=''
    missing_components=''

    if $WANT_SERVER; then
	AMANDA_COMPONENTS="$AMANDA_COMPONENTS server";
    else
	missing_components="$missing_components (no server)";
    fi
    if $WANT_RESTORE; then
	AMANDA_COMPONENTS="$AMANDA_COMPONENTS restore";
    else
	missing_components="$missing_components (no restore)";
    fi
    if $WANT_CLIENT; then
	AMANDA_COMPONENTS="$AMANDA_COMPONENTS client";
    else
	missing_components="$missing_components (no client)";
    fi
    if $WANT_RECOVER; then
	AMANDA_COMPONENTS="$AMANDA_COMPONENTS amrecover";
    else
	missing_components="$missing_components (no amrecover)";
    fi
    if $WANT_NDMP; then
	AMANDA_COMPONENTS="$AMANDA_COMPONENTS ndmp";
    else
	missing_components="$missing_components (no ndmp)";
    fi
    if $WANT_REST_SERVER; then
	AMANDA_COMPONENTS="$AMANDA_COMPONENTS rest-server";
    else
	missing_components="$missing_components (no rest-server)";
    fi

    AC_SUBST(AMANDA_COMPONENTS)
])


# SYNOPSIS
#
#   AMANDA_WITHOUT_SERVER
#
# OVERVIEW
#
#   Add option --without-server, and set WANT_SERVER to true or false, 
#   accordingly.
#
AC_DEFUN([AMANDA_WITHOUT_SERVER], [
    WANT_SERVER=true
    AC_ARG_WITH(server,
	AS_HELP_STRING([--without-server], [do not build server stuff (set --without-restore)]), [
	    case "$withval" in
	    y | ye | yes) WANT_SERVER=true;;
	    n | no) WANT_SERVER=false;;
	    *) AC_MSG_ERROR([You must not supply an argument to the --without-server option.]) ;;
	    esac
    ])
])

# SYNOPSIS
#
#   AMANDA_WITHOUT_CLIENT
#
# OVERVIEW
#
#   Add option --without-client, and set WANT_CLIENT to true or false, 
#   accordingly.
#
AC_DEFUN([AMANDA_WITHOUT_CLIENT], [
    WANT_CLIENT=true
    AC_ARG_WITH(client,
	AS_HELP_STRING([--without-client], [do not build client stuff]), [
	    case "$withval" in
	    y | ye | yes) WANT_CLIENT=true;;
	    n | no) WANT_CLIENT=false;;
	    *) AC_MSG_ERROR([You must not supply an argument to the --without-client option.]) ;;
	    esac
    ])
])

# SYNOPSIS
#
#   AMANDA_WITHOUT_RESTORE
#
# OVERVIEW
#
#   Add option --without-restore, and set WANT_RESTORE to true or false, 
#   accordingly.
#
AC_DEFUN([AMANDA_WITHOUT_RESTORE], [
    AC_REQUIRE([AMANDA_WITHOUT_SERVER])
    WANT_RESTORE=${WANT_SERVER-true}
    AC_ARG_WITH(restore,
	AS_HELP_STRING([--without-restore], [do not build amrestore nor amidxtaped]), [
	    case "$withval" in
	    y | ye | yes) WANT_RESTORE=true;;
	    n | no) WANT_RESTORE=false;;
	    *) AC_MSG_ERROR([You must not supply an argument to --with-restore option.]) ;;
	    esac
    ])
])

# SYNOPSIS
#
#   AMANDA_WITHOUT_AMRECOVER
#
# OVERVIEW
#
#   Add option --without-amrecover, and set WANT_RECOVER (not WANT_AMRECOVER) to
#   true or false, accordingly.
#
AC_DEFUN([AMANDA_WITHOUT_AMRECOVER], [
    AC_REQUIRE([AMANDA_WITHOUT_CLIENT])
    WANT_RECOVER=${WANT_CLIENT-true}
    AC_ARG_WITH(amrecover,
	AS_HELP_STRING([--without-amrecover],
		       [do not build amrecover]), [
	    case "$withval" in
	    y | ye | yes) WANT_RECOVER=true;;
	    n | no) WANT_RECOVER=false;;
	    *) AC_MSG_ERROR([You must not supply an argument to --with-amrecover option.]) ;;
	    esac
	])
])

# SYNOPSIS
#
#   AMANDA_WITHOUT_NDMP
#
# OVERVIEW
#
#   Add option --without-ndmp, and set WANT_NDMP to
#   true or false, accordingly.
#
AC_DEFUN([AMANDA_WITHOUT_NDMP], [
    WANT_NDMP=${WANT_NDMP-true}
    AC_ARG_WITH(ndmp,
	AS_HELP_STRING([--without-ndmp],
		       [do not build ndmp]), [
	    case "$withval" in
	    y | ye | yes) WANT_NDMP=true;;
	    n | no) WANT_NDMP=false;;
	    *) AC_MSG_ERROR([You must not supply an argument to --with-ndmp option.]) ;;
	    esac
	])
])

# SYNOPSIS
#
#   AMANDA_WITHOUT_REST_SERVER
#
# OVERVIEW
#
#   Add option --without-rest-server, and set WANT_REST_SERVER to true or false, 
#   accordingly.
#
AC_DEFUN([AMANDA_WITHOUT_REST_SERVER], [
    WANT_REST_SERVER=true
    AC_ARG_WITH(rest-server,
	AS_HELP_STRING([--without-rest-server], [do not build the rest-server]), [
	    case "$withval" in
	    y | ye | yes) WANT_REST_SERVER=true
			  if test x"$WANT_SERVER" = x"false"; then
			      AC_MSG_ERROR([--with-rest-server is incompatible with --without-server])
			      WANT_REST_SERVER=false
			  fi
			  ;;
	    n | no) WANT_REST_SERVER=false;;
	    *) AC_MSG_ERROR([You must not supply an argument to the --without-rest-server option.]) ;;
	    esac
    ])
    if test x"$WANT_SERVER" = x"false"; then
	WANT_REST_SERVER=false
    fi
])

## deprecated --with-* options

AC_DEFUN([AMANDA_WITH_CLIENT_ONLY], [
    AC_ARG_WITH(client-only,
	AS_HELP_STRING([--with-client-only], [deprecated: use --without-server]),
	[   AC_MSG_ERROR([--with-client-only is deprecated, use --without-server instead.])
	])
],)

AC_DEFUN([AMANDA_WITH_SERVER_ONLY], [
    AC_ARG_WITH(server-only,
	AS_HELP_STRING([--with-server-only], [deprecated: use --without-client]),
	[   AC_MSG_ERROR([--with-server-only is deprecated, use --without-client instead.])
	],)
])

# SYNOPSIS
#
#   AMANDA_SHOW_COMPONENTS_SUMMARY
#
# OVERVIEW
#
#   Show a summary of the settings from this file.
#
AC_DEFUN([AMANDA_SHOW_COMPONENTS_SUMMARY],
[
    echo "Amanda Components:$AMANDA_COMPONENTS$missing_components"
])
