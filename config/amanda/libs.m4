# OVERVIEW
#
#   This file contains macros that search for specific libraries that are
#   required or utilized by Amanda.

# SYNOPSIS
#
#   AMANDA_CHECK_LIBCURL
#
# OVERVIEW
#
#   Check for LIBCURL support.  Sets the shell variable HAVE_CURL to "yes" or
#   "no" depending on the result of the test.  If CURL is found, the necessary
#   compiler flags are added, and a few other type checks are performed.
#
#   Note that libcurl itself defines a number of useful symbols as well; see
#   the libcurl distribution for details.
#
AC_DEFUN([AMANDA_CHECK_LIBCURL], [
    LIBCURL_CHECK_CONFIG(yes, 7.10.0, HAVE_CURL=yes, HAVE_CURL=no)
    if test x"$HAVE_CURL" = x"yes"; then
	AMANDA_ADD_LIBS($LIBCURL)
	AMANDA_ADD_CPPFLAGS($LIBCURL_CPPFLAGS)

	AMANDA_CHECK_TYPE([curl_off_t], [off_t], [curl/curl.h])
	case "$host" in
	    *sun-solaris2*) # Solaris, all versions.
	    # we extract the -L flags and translate them to -R flags, as required
	    # by the runtime linker.
	    if test -n "$_libcurl_config"; then
		curlflags=`$_libcurl_config --libs 2>/dev/null`
		for flag in curlflags; do
		    case $flag in
			-L*) LDFLAGS="$LDFLAGS "`echo "x$flag" | sed -e 's/^x-L/-R/'`;;
		    esac
		done
	    fi
	    ;;
	esac
    fi

])

# SYNOPSIS
#
#   AMANDA_CHECK_HMAC
#
# OVERVIEW
#
#   Check for HMAC support in -lcrypto.  If found, the shell
#   variable HAVE_HMAC will be set to 'yes'.  The appropriate one of
#   HAVE_OPENSSL_HMAC_H, HAVE_CRYPTO_HMAC_H, and HAVE_HMAC_H are also
#   defined via AC_CHECK_HEADERS.
#
AC_DEFUN([AMANDA_CHECK_HMAC], [
    HAVE_HMAC=yes
    AC_CHECK_LIB([crypto], [HMAC_CTX_init], [], [HAVE_HMAC=no])

    found_hmac_h=no
    AC_CHECK_HEADERS([openssl/hmac.h crypto/hmac.h hmac.h],
		    [found_hmac_h=yes; break])
    if test x"$found_hmac_h" != x"yes"; then
	HAVE_HMAC=no
    fi
])

# SYNOPSIS
#
#   AMANDA_CHECK_NET_LIBS
#
# OVERIVEW
#
#   Check for the libraries we'll need to use sockets, etc.
#
AC_DEFUN([AMANDA_CHECK_NET_LIBS], [
    # Make sure we don't use -lnsl and -lsun on Irix systems.
    case "$host" in
	*sgi-irix*)
			    AC_CHECK_LIB(socket,main)
			    ;;
	*)
			    AC_CHECK_LIB(resolv,main)
			    AC_CHECK_LIB(nsl,main)
			    AC_CHECK_LIB(socket,main)
			    AC_CHECK_LIB(sun,main)
			    ;;
    esac
])

# SYNOPSIS
#
#   AMANDA_CHECK_GLIB
#
# OVERVIEW
#
#   Search for glib.  This is basically a wrapper for AM_PATH_GLIB_2_0, with
#   the addition of system-specific configuration to convince Amanda to compile
#   "out of the box" on more boxes.
#
AC_DEFUN([AMANDA_CHECK_GLIB], [
    AC_ARG_VAR(GLIB_CFLAGS, [CFLAGS to build with glib; disables use of pkg-config; must specify all GLIB_ vars])
    AC_ARG_VAR(GLIB_LIBS, [libraries to build with glib; disables use of pkg-config; must specify all GLIB_vars])
    AC_ARG_VAR(GLIB_GENMARSHAL, [genmarshal binary to use with glib; disables use of pkg-config; must specify all GLIB_ vars])
    AC_ARG_VAR(GOBJECT_QUERY, [gobject_query binary to use with glib; disables use of pkg-config; must specify all GLIB_ vars])
    AC_ARG_VAR(GLIB_MKENUMS, [mkenums binary to use with glib; disables use of pkg-config; must specify all GLIB_ vars])

    # if any of the precious variables are set, disable the pkg-config run.
    # Further, if any is specified, all must be specified.
    explicit_glib=no
    test x"$GLIB_CFLAGS" = x"" || explicit_glib=yes
    test x"$GLIB_LIBS" = x"" || explicit_glib=yes
    test x"$GLIB_GENMARSHAL" = x"" || explicit_glib=yes
    test x"$GOBJECT_QUERY" = x"" || explicit_glib=yes
    test x"$GLIB_MKENUMS" = x"" || explicit_glib=yes

    if test x"$explicit_glib" = x"no"; then
	# search for pkg-config, which the glib configuration uses, adding a few
	# system-specific search paths.
	AC_PATH_PROG(PKG_CONFIG, pkg-config, [], $LOCSYSPATH:/opt/csw/bin:/usr/local/bin:/opt/local/bin)

	case "$host" in
	    sparc-sun-solaris2.8) # Solaris 8
		# give the linker a runtime search path; pkg-config doesn't supply this.
		# Users could also specify this with LD_LIBRARY_PATH to both ./configure
		# and make.  Adding this support here makes straight './configure; make'
		# "just work" on Solaris 8
		if test -n "$PKG_CONFIG"; then
		    glib_R_flag=`$PKG_CONFIG glib-2.0 --libs-only-L 2>/dev/null | sed -e 's/-L/-R/g'`
		    LDFLAGS="$LDFLAGS $glib_R_flag"
		fi
		;;
	esac

	AM_PATH_GLIB_2_0(2.2.0,,[
	    AC_MSG_ERROR(glib not found or too old; See http://wiki.zmanda.com/index.php/Installation for help)
	], gmodule gobject gthread)
    else
        # Confirm that all GLIB_ variables are set
        if test ! x"$GLIB_CFLAGS" = x"" && \
           test ! x"$GLIB_LIBS" = x"" && \
           test ! x"$GLIB_GENMARSHAL" = x"" && \
           test ! x"$GOBJECT_QUERY" = x"" && \
           test ! x"$GLIB_MKENUMS" = x""; then
            :
        else
            AC_MSG_ERROR(Not all precious glib variables were set.)
        fi
    fi

    # GLIB_CPPFLAGS is not set by autoconf, yet GLIB_CFLAGS contains what GLIB_CPPFLAGS should contain.
    AMANDA_ADD_CPPFLAGS($GLIB_CFLAGS)
    AMANDA_ADD_LIBS($GLIB_LIBS)
])

# LIBCURL_CHECK_CONFIG is from the libcurl
# distribution and licensed under the BSD license:
# Copyright (c) 1996 - 2007, Daniel Stenberg, <daniel@haxx.se>.
#
# All rights reserved.
#
# Permission to use, copy, modify, and distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright
# notice and this permission notice appear in all copies.
#
# LIBCURL_CHECK_CONFIG ([DEFAULT-ACTION], [MINIMUM-VERSION],
#                       [ACTION-IF-YES], [ACTION-IF-NO])
# ----------------------------------------------------------
#      David Shaw <dshaw@jabberwocky.com>   May-09-2006
#
# Checks for libcurl.  DEFAULT-ACTION is the string yes or no to
# specify whether to default to --with-libcurl or --without-libcurl.
# If not supplied, DEFAULT-ACTION is yes.  MINIMUM-VERSION is the
# minimum version of libcurl to accept.  Pass the version as a regular
# version number like 7.10.1. If not supplied, any version is
# accepted.  ACTION-IF-YES is a list of shell commands to run if
# libcurl was successfully found and passed the various tests.
# ACTION-IF-NO is a list of shell commands that are run otherwise.
# Note that using --without-libcurl does run ACTION-IF-NO.
#
# This macro #defines HAVE_LIBCURL if a working libcurl setup is
# found, and sets @LIBCURL@ and @LIBCURL_CPPFLAGS@ to the necessary
# values.  Other useful defines are LIBCURL_FEATURE_xxx where xxx are
# the various features supported by libcurl, and LIBCURL_PROTOCOL_yyy
# where yyy are the various protocols supported by libcurl.  Both xxx
# and yyy are capitalized.  See the list of AH_TEMPLATEs at the top of
# the macro for the complete list of possible defines.  Shell
# variables $libcurl_feature_xxx and $libcurl_protocol_yyy are also
# defined to 'yes' for those features and protocols that were found.
# Note that xxx and yyy keep the same capitalization as in the
# curl-config list (e.g. it's "HTTP" and not "http").
#
# Users may override the detected values by doing something like:
# LIBCURL="-lcurl" LIBCURL_CPPFLAGS="-I/usr/myinclude" ./configure
#
# For the sake of sanity, this macro assumes that any libcurl that is
# found is after version 7.7.2, the first version that included the
# curl-config script.  Note that it is very important for people
# packaging binary versions of libcurl to include this script!
# Without curl-config, we can only guess what protocols are available,
# or use curl_version_info to figure it out at runtime.

AC_DEFUN([LIBCURL_CHECK_CONFIG],
[
  AH_TEMPLATE([LIBCURL_FEATURE_SSL],[Defined if libcurl supports SSL])
  AH_TEMPLATE([LIBCURL_FEATURE_KRB4],[Defined if libcurl supports KRB4])
  AH_TEMPLATE([LIBCURL_FEATURE_IPV6],[Defined if libcurl supports IPv6])
  AH_TEMPLATE([LIBCURL_FEATURE_LIBZ],[Defined if libcurl supports libz])
  AH_TEMPLATE([LIBCURL_FEATURE_ASYNCHDNS],[Defined if libcurl supports AsynchDNS])
  AH_TEMPLATE([LIBCURL_FEATURE_IDN],[Defined if libcurl supports IDN])
  AH_TEMPLATE([LIBCURL_FEATURE_SSPI],[Defined if libcurl supports SSPI])
  AH_TEMPLATE([LIBCURL_FEATURE_NTLM],[Defined if libcurl supports NTLM])

  AH_TEMPLATE([LIBCURL_PROTOCOL_HTTP],[Defined if libcurl supports HTTP])
  AH_TEMPLATE([LIBCURL_PROTOCOL_HTTPS],[Defined if libcurl supports HTTPS])
  AH_TEMPLATE([LIBCURL_PROTOCOL_FTP],[Defined if libcurl supports FTP])
  AH_TEMPLATE([LIBCURL_PROTOCOL_FTPS],[Defined if libcurl supports FTPS])
  AH_TEMPLATE([LIBCURL_PROTOCOL_FILE],[Defined if libcurl supports FILE])
  AH_TEMPLATE([LIBCURL_PROTOCOL_TELNET],[Defined if libcurl supports TELNET])
  AH_TEMPLATE([LIBCURL_PROTOCOL_LDAP],[Defined if libcurl supports LDAP])
  AH_TEMPLATE([LIBCURL_PROTOCOL_DICT],[Defined if libcurl supports DICT])
  AH_TEMPLATE([LIBCURL_PROTOCOL_TFTP],[Defined if libcurl supports TFTP])

  AC_ARG_WITH(libcurl,
     AC_HELP_STRING([--with-libcurl=PREFIX],
	[look for the curl library in PREFIX/lib and headers in PREFIX/include]),
     [_libcurl_with=$withval],[_libcurl_with=ifelse([$1],,[yes],[$1])])

  if test "$_libcurl_with" != "no" ; then

     AC_PROG_AWK

     _libcurl_version_parse="eval $AWK '{split(\$NF,A,\".\"); X=256*256*A[[1]]+256*A[[2]]+A[[3]]; print X;}'"

     _libcurl_try_link=yes

     if test -d "$_libcurl_with" ; then
        LIBCURL_CPPFLAGS="-I$withval/include"
        _libcurl_ldflags="-L$withval/lib"
        AC_PATH_PROG([_libcurl_config],[curl-config],[],["$withval/bin"])
     else
        AC_PATH_PROG([_libcurl_config],[curl-config],[],[$PATH])
     fi

     if test x$_libcurl_config != "x" ; then
        AC_CACHE_CHECK([for the version of libcurl],
           [libcurl_cv_lib_curl_version],
           [libcurl_cv_lib_curl_version=`$_libcurl_config --version | $AWK '{print $[]2}'`])

        _libcurl_version=`echo $libcurl_cv_lib_curl_version | $_libcurl_version_parse`
        _libcurl_wanted=`echo ifelse([$2],,[0],[$2]) | $_libcurl_version_parse`

        if test $_libcurl_wanted -gt 0 ; then
           AC_CACHE_CHECK([for libcurl >= version $2],
              [libcurl_cv_lib_version_ok],
              [
              if test $_libcurl_version -ge $_libcurl_wanted ; then
                 libcurl_cv_lib_version_ok=yes
              else
                 libcurl_cv_lib_version_ok=no
              fi
              ])
        fi

        if test $_libcurl_wanted -eq 0 || test x$libcurl_cv_lib_version_ok = xyes ; then
           if test x"$LIBCURL_CPPFLAGS" = "x" ; then
              LIBCURL_CPPFLAGS=`$_libcurl_config --cflags`
           fi
           if test x"$LIBCURL" = "x" ; then
              LIBCURL=`$_libcurl_config --libs`

              # This is so silly, but Apple actually has a bug in their
              # curl-config script.  Fixed in Tiger, but there are still
              # lots of Panther installs around.
              case "${host}" in
                 powerpc-apple-darwin7*)
                    LIBCURL=`echo $LIBCURL | sed -e 's|-arch i386||g'`
                 ;;
              esac
           fi

           # All curl-config scripts support --feature
           _libcurl_features=`$_libcurl_config --feature`

           # Is it modern enough to have --protocols? (7.12.4)
           if test $_libcurl_version -ge 461828 ; then
              _libcurl_protocols=`$_libcurl_config --protocols`
           fi
        else
           _libcurl_try_link=no
        fi

        unset _libcurl_wanted
     fi

     if test $_libcurl_try_link = yes ; then

        # we didn't find curl-config, so let's see if the user-supplied
        # link line (or failing that, "-lcurl") is enough.
        LIBCURL=${LIBCURL-"$_libcurl_ldflags -lcurl"}

        AC_CACHE_CHECK([whether libcurl is usable],
           [libcurl_cv_lib_curl_usable],
           [
           _libcurl_save_cppflags=$CPPFLAGS
           CPPFLAGS="$LIBCURL_CPPFLAGS $CPPFLAGS"
           _libcurl_save_libs=$LIBS
           LIBS="$LIBCURL $LIBS"

           AC_LINK_IFELSE([AC_LANG_PROGRAM([[#include <curl/curl.h>]],[[
/* Try and use a few common options to force a failure if we are
   missing symbols or can't link. */
int x;
curl_easy_setopt(NULL,CURLOPT_URL,NULL);
x=CURL_ERROR_SIZE;
x=CURLOPT_WRITEFUNCTION;
x=CURLOPT_FILE;
x=CURLOPT_ERRORBUFFER;
x=CURLOPT_STDERR;
x=CURLOPT_VERBOSE;
]])],[libcurl_cv_lib_curl_usable=yes],[libcurl_cv_lib_curl_usable=no])

           CPPFLAGS=$_libcurl_save_cppflags
           LIBS=$_libcurl_save_libs
           unset _libcurl_save_cppflags
           unset _libcurl_save_libs
           ])

        if test $libcurl_cv_lib_curl_usable = yes ; then

           # Does curl_free() exist in this version of libcurl?
           # If not, fake it with free()

           _libcurl_save_cppflags=$CPPFLAGS
           CPPFLAGS="$CPPFLAGS $LIBCURL_CPPFLAGS"
           _libcurl_save_libs=$LIBS
           LIBS="$LIBS $LIBCURL"

           AC_CHECK_FUNC(curl_free,,
              AC_DEFINE(curl_free,free,
                [Define curl_free() as free() if our version of curl lacks curl_free.]))

           CPPFLAGS=$_libcurl_save_cppflags
           LIBS=$_libcurl_save_libs
           unset _libcurl_save_cppflags
           unset _libcurl_save_libs

           AC_DEFINE(HAVE_LIBCURL,1,
             [Define to 1 if you have a functional curl library.])
           AC_SUBST(LIBCURL_CPPFLAGS)
           AC_SUBST(LIBCURL)

           for _libcurl_feature in $_libcurl_features ; do
              AC_DEFINE_UNQUOTED(AS_TR_CPP(libcurl_feature_$_libcurl_feature),[1])
              eval AS_TR_SH(libcurl_feature_$_libcurl_feature)=yes
           done

           if test "x$_libcurl_protocols" = "x" ; then

              # We don't have --protocols, so just assume that all
              # protocols are available
              _libcurl_protocols="HTTP FTP FILE TELNET LDAP DICT"

              if test x$libcurl_feature_SSL = xyes ; then
                 _libcurl_protocols="$_libcurl_protocols HTTPS"

                 # FTPS wasn't standards-compliant until version
                 # 7.11.0
                 if test $_libcurl_version -ge 461568; then
                    _libcurl_protocols="$_libcurl_protocols FTPS"
                 fi
              fi
           fi

           for _libcurl_protocol in $_libcurl_protocols ; do
              AC_DEFINE_UNQUOTED(AS_TR_CPP(libcurl_protocol_$_libcurl_protocol),[1])
              eval AS_TR_SH(libcurl_protocol_$_libcurl_protocol)=yes
           done
        else
           unset LIBCURL
           unset LIBCURL_CPPFLAGS
        fi
     fi

      LIBCURL_USE_NSS=no
      LIBCURL_USE_GNUTLS=no
      LIBCURL_USE_OPENSSL=yes
     _libcurl_configures=`$_libcurl_config --configure`
     for _libcurl_configure in $_libcurl_configures ; do
	if [[[ $_libcurl_configure = \'--with-nss* ]]]; then
	    LIBCURL_USE_NSS=yes
	fi
	if [[[ $_libcurl_configure = \'--without-nss* ]]]; then
	    LIBCURL_USE_NSS=no
	fi
	if [[[ $_libcurl_configure = \'--with-gnutls* ]]]; then
	    LIBCURL_USE_GNUTLS=yes
	fi
	if [[[ $_libcurl_configure = \'--without-gnutls* ]]]; then
	    LIBCURL_USE_GNUTLS=no
	fi
	if [[[ $_libcurl_configure = \'--with-ssl* ]]]; then
	    LIBCURL_USE_OPENSSL=yes
	fi
	if [[[ $_libcurl_configure = \'--without-ssl* ]]]; then
	    LIBCURL_USE_OPENSSL=no
	fi
     done

     if test "x$LIBCURL_USE_NSS" = "xyes"; then
       AC_DEFINE(LIBCURL_USE_NSS, 1, [Defined if libcurl use NSS])
     fi
     if test "x$LIBCURL_USE_GNUTLS" = "xyes"; then
       AC_DEFINE(LIBCURL_USE_GNUTLS, , [Defined if libcurl use GnuTLS])
     fi
     if test "x$LIBCURL_USE_OPENSSL" = "xyes"; then
       AC_DEFINE(LIBCURL_USE_OPENSSL, 1, [Defined if libcurl use OpenSSL])
     fi

     unset _libcurl_try_link
     unset _libcurl_version_parse
     unset _libcurl_config
     unset _libcurl_feature
     unset _libcurl_features
     unset _libcurl_protocol
     unset _libcurl_protocols
     unset _libcurl_version
     unset _libcurl_ldflags
     unset _libcurl_configure
     unset _libcurl_configures
  fi

  if test x$_libcurl_with = xno || test x$libcurl_cv_lib_curl_usable != xyes ; then
     # This is the IF-NO path
     ifelse([$4],,:,[$4])
  else
     # This is the IF-YES path
     ifelse([$3],,:,[$3])
  fi

  unset _libcurl_with
])dnl

