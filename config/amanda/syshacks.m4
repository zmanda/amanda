# SYNOPSIS
#
#   AMANDA_SYSHACKS
#
# OVERVIEW
#
#   This macro encapsulates any system-specific hacks required to make Amanda
#   compile that don't fit neatly into any other macro.  It is implemented as a 
#   big 'case' statement based on the canonical target architecture.
#
#   It also serves as a list of the "supported" architectures, represented by 
#   case statements with empty bodies.  If no architecture matches, the user
#   is presented with a warning.
#
AC_DEFUN([AMANDA_SYSHACKS], [
    case "$host" in
	*-dec-osf*)
		    ;;
	*-dg-*)
		    ;;
	*-netbsd*)
		    ;;
	*-freebsd*)
		    ;;
	*-openbsd*)
		    ;;
	*-hp-*)
		    case "$CC" in
			*gcc*)
			    AMANDA_ADD_CPPFLAGS([-D__STDC_EXT__])
			    ;;
			*cc*)
			    AMANDA_ADD_CFLAGS([-Ae])
			    ;;
		    esac
		    # hpux needs LD_LIBRARY_PATH to load perl binary module
		    AC_DEFINE(NEED_LD_LIBRARY_PATH_ENV,1,[Define on hpux. ])
		    ;;
	*-ibm-aix*)
		    ;;
	m88k-motorola-sysv4)
		    ;;
	*-nextstep3)
		    ;;
	*-pc-bsdi*)
		    ;;
	*-pc-linux-*)
		    ;;
	*-redhat-linux-*)
		    ;;
	*-suse-linux-*)
		    ;;
	x86_64-*-linux-*)
		    ;;
	alpha*-*-linux-*)
		    ;;
	sparc*-*-linux-*)
		    ;;
	s390x-*-linux-*)
		    ;;
	powerpc-*-linux-*)
		    ;;
        *-sgi-irix3*)
		    # The old cc won't work!
		    if test "x$GCC" != "xyes"; then
			AC_MSG_ERROR([The old SGI IRIX compiler ($CC) will not compile Amanda; use CC=gcc])
		    fi
		    ;;
        *-sgi-irix4*)
		    ;;
        *-sgi-irix5*)
		    ;;
        *-sgi-irix6*)
		    ;;
        *-solaris2.11)  # none is fine
                    ;;
        *-solaris2.10)
		    AMANDA_ADD_CFLAGS([-D_XOPEN_SOURCE=600])
		    ;;
        *-solaris2*)
		    AMANDA_ADD_CFLAGS([-D_XOPEN_SOURCE=1 -D_XOPEN_SOURCE_EXTENDED=1])
		    ;;
        *-sun-sunos4.1*)
		    ;;
        *-ultrix*)
		    ;;
        *-sysv4.2uw2*)
		    ;;
        *-sco3.2v5*)
		    ;;
        i386-pc-isc4*)
		    ;;
        *-sni-sysv4)
		    ;;
        *-pc-cygwin)
		    AC_DEFINE(IGNORE_TAR_ERRORS,1,[Define on Cygwin. ])
		    # Cygwin needs PATH to find cygwin1.dll
		    AC_DEFINE(NEED_PATH_ENV,1,[Define on Cygwin. ])
		    AC_DEFINE(IGNORE_FSTAB,1,[Define on Cygwin. ])
		    AMANDA_ADD_LDFLAGS([-Wl,-enable-runtime-pseudo-reloc -no-undefined])
		    ;;
        *-apple-darwin7*) # MacOS X 10.3.* (Panther)
		    AC_DEFINE(BROKEN_SENDMSG, 1, [Define if sendmsg is broken])
		    ;;
        *-apple-darwin8*) # MacOS X 10.4.* (Tiger)
		    AC_DEFINE(BROKEN_SENDMSG, 1, [Define if sendmsg is broken])
		    ;;
        *-apple-darwin9*) # MacOS X 10.5.* (Leopard)
		    AC_DEFINE(BROKEN_SENDMSG, 1, [Define if sendmsg is broken])
		    ;;
        *-apple-darwin10*) # MacOS X 10.6.* (Snow Leopard)
		    AC_DEFINE(BROKEN_SENDMSG, 1, [Define if sendmsg is broken])
		    ;;
        *-apple-darwin*) # MacOS X *
		    AC_DEFINE(BROKEN_SENDMSG, 1, [Define if sendmsg is broken])
		    ;;
      *)
		AMANDA_ADD_WARNING(
[*****
This machine, target type $host, is not known to be fully supported
by this configure script.  If the installation of Amanda on this system
succeeds or needed any patches, please email amanda-hackers@amanda.org
with the patches or an indication of the sucess or failure of the
Amanda installation on your system.
*****])
		    ;;
    esac
])
