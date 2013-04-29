# OVERVIEW
#
#   This file contains macros that check if the JSON SERVER must be compiled

# SYNOPSIS
#
#   AMANDA_JSON_SERVER
#
# OVERVIEW
#
#   Check for --with-json-server
#   Check for Moose, Plack and Log::Any perl modules.
#
AC_DEFUN([AMANDA_JSON_SERVER], [

    AC_REQUIRE([AMANDA_PROG_PERL])
    WANT_JSON_SERVER=;
    AC_ARG_WITH(json-server,
	AC_HELP_STRING([--with-json-server],[If json server must be compiled]),
	[
	    case "$withval" in
		y|ye|yes) WANT_JSON_SERVER=true;;
		n|no) WANT_JSON_SERVER=false;;
		*) WANT_JSON_SERVER=false;;
	    esac
	])

    ALL_JSON_MODULE=true;
    JSON_MODULE=;
    # check Plack::Request module
    if ! `$PERL -e 'use Plack::Request'`; then
	JSON_MODULE=" Plack::Request";
	ALL_JSON_MODULE=false;
    fi
    # check Moose module
    if ! `$PERL -e 'use Moose'`; then
	JSON_MODULE="$JSON_MODULE Moose";
	ALL_JSON_MODULE=false;
    fi
    # check Log::Any module
    if ! `$PERL -e 'use Log::Any'`; then
	JSON_MODULE="$JSON_MODULE Log:Any";
	ALL_JSON_MODULE=false;
    fi

    if test x"$WANT_JSON_SERVER" = x""; then
	if test $ALL_JSON_MODULE = false; then
	    WANT_JSON_SERVER=false;
	    AMANDA_ADD_WARNING([missing perl modules for the json server: $JSON_MODULE])
	fi
    else
    if test $WANT_JSON_SERVER = true; then
	if test $ALL_JSON_MODULE = false; then
	    WANT_JSON_SERVER=false;
	    AC_MSG_ERROR([missing perl modules for the json server: $JSON_MODULE])
	fi
    fi
    fi
])

