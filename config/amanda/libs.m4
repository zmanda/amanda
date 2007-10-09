# OVERVIEW
#
#   This file contains macros that search for specific libraries that are
#   required or utilized by Amanda.

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
    case "$target" in
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
    # search for pkg-config, which the glib configuration uses, adding a few
    # system-specific search paths.
    AC_PATH_PROG(PKG_CONFIG, pkg-config, [], $LOCSYSPATH:/opt/csw/bin:/usr/local/bin:/opt/local/bin)

    case "$target" in
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
    AMANDA_ADD_CFLAGS($GLIB_CFLAGS)
    AMANDA_ADD_CPPFLAGS($GLIB_CPPFLAGS)
    AMANDA_ADD_LIBS($GLIB_LIBS)
])
