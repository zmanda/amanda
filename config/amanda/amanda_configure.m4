# SYNOPSIS
#
#   AMANDA_CONFIGURE
#
# OVERVIEW
#
#   Most of the configuration
#
AC_DEFUN([AMANDA_CONFIGURE],[

AM_INIT_AUTOMAKE([tar-ustar 1.10 subdir-objects])
AC_CONFIG_HEADERS([config/config.h])

dnl Minimum Autoconf version required.
AC_PREREQ(2.64)

#
# Take care of some early Amanda-specific setup
#
AMANDA_CONFIGURE_ARGS
AMANDA_INIT_SUMMARY
AMANDA_SNAPSHOT_STAMP
AMANDA_SPLIT_VERSION
AMANDA_CONFIG_LOCAL

#
# Set up compiler location, basic flags, and include locations
# and library locations before we start checking the system
# configuration in more detail...
#
AC_REQUIRE([AC_PROG_CC])
AC_REQUIRE([AC_OBJEXT])
AC_REQUIRE([AC_EXEEXT])
AC_REQUIRE([AMANDA_INIT_FLAGS])
AMANDA_ADD_CPPFLAGS([-D_GNU_SOURCE])
AMANDA_AS_NEEDED

dnl -------------------------------------------------------------------------

#
# Configuration flags (--with-xxx and --enable-xxx)
#

AMANDA_WITH_USER
AMANDA_WITH_GROUP
AMANDA_WITH_APPLICATIONDIR
AMANDA_WITH_CONFIGDIR
AMANDA_WITH_INDEXDIR
AMANDA_WITH_DBDIR
AMANDA_WITH_LOGDIR
AMANDA_WITH_SUFFIXES
AMANDA_WITH_GNUTAR_LISTDIR
AMANDA_WITH_TMPDIR
AMANDA_WITH_FORCE_UID
AMANDA_WITH_OWNER
AMANDA_WITH_FQDN
AMANDA_WITH_REUSEPORTS
AMANDA_WITH_PORTRANGES
AMANDA_WITH_BUFFERED_DUMP
AMANDA_WITH_MAXTAPEBLOCKSIZE
AMANDA_WITH_ASSERTIONS
AMANDA_WITH_DEBUGGING
AMANDA_WITH_DEBUG_DAYS
AMANDA_WITH_SINGLE_USERID
AMANDA_WITH_FAILURE_CODE
AMANDA_DISABLE_INSTALLPERMS
AMANDA_DISABLE_SYNTAX_CHECKS

dnl -------------------------------------------------------------------------

#
# Set up for Amanda components and modules
#
AC_REQUIRE([AMANDA_CHECK_COMPONENTS])
AMANDA_SETUP_AMPLOT
AMANDA_SETUP_DOCUMENTATION
AMANDA_SETUP_DEFAULTS
AMANDA_SETUP_LFS
AMANDA_SETUP_GETFSENT
AMANDA_SETUP_FILE_LOCKING
AMANDA_SETUP_SWIG
AMANDA_CHECK_COMPRESSION
AMANDA_CHECK_IPV6
AMANDA_CHECK_READDIR
AMANDA_CHECK_DEVICE_PREFIXES
AMANDA_SYSHACKS
AMANDA_EXPAND_DIRS
AMANDA_REST_EXTENSIONS

#
# Security (authentication) mechansims
#
AMANDA_BSD_SECURITY
AMANDA_BSDTCP_SECURITY
AMANDA_BSDUDP_SECURITY
AMANDA_RSH_SECURITY
AMANDA_SSL_SECURITY
AMANDA_SSH_SECURITY
AMANDA_KRB5_SECURITY

#
# Dumpers
#
AMANDA_PROG_REALPATH
AMANDA_PROG_READLINK
AMANDA_PROG_XFSDUMP_XFSRESTORE
AMANDA_PROG_VXDUMP_VXRESTORE
AMANDA_PROG_VDUMP_VRESTORE
AMANDA_PROG_DUMP_RESTORE
AMANDA_PROG_GNUTAR
AMANDA_PROG_STAR
AMANDA_PROG_BSDTAR
AMANDA_PROG_SUNTAR
AMANDA_PROG_SAMBA_CLIENT
AMANDA_CHECK_USE_RUNDUMP

#
# Look for other programs Amanda will use
# 
AMANDA_PROG_GREP
AC_PROG_EGREP
AMANDA_PROG_LINT
AMANDA_PROG_LEX
AMANDA_PROG_AR
AC_PROG_AWK
AC_PROG_YACC
AC_PROG_MKDIR_P
AMANDA_PROG_DD
AMANDA_PROG_BASH
AMANDA_PROG_SORT
AMANDA_PROG_MAILER
AMANDA_PROG_MT
AMANDA_PROG_MTX
AMANDA_PROG_MOUNT
AMANDA_PROG_UMOUNT
AMANDA_PROG_UNAME
AMANDA_PROG_LPR
AMANDA_PROG_PCAT
AMANDA_PROG_PERL
AMANDA_PROG_SWIG
AMANDA_PS_ARGUMENT
AMANDA_PROG_RPCGEN
AMANDA_PROG_NC

dnl -------------------------------------------------------------------------

#
# Compiler / system characteristics
#

#
# compiler
#
AC_PROG_GCC_TRADITIONAL
AC_C_CONST
AC_C_BIGENDIAN

# GCC_COMPILER is needed in the gnulib Makefile to silence errors
AM_CONDITIONAL([GCC_COMPILER], [test "x$GCC" = "xyes"])

#
# Warnings
#
AMANDA_ENABLE_GCC_WARNING([parentheses])
AMANDA_ENABLE_GCC_WARNING([declaration-after-statement])
AMANDA_ENABLE_GCC_WARNING([missing-prototypes])
AMANDA_ENABLE_GCC_WARNING([strict-prototypes])
AMANDA_ENABLE_GCC_WARNING([missing-declarations])
AMANDA_ENABLE_GCC_WARNING([format])
AMANDA_ENABLE_GCC_WARNING([format-security])
AMANDA_ENABLE_GCC_WARNING([sign-compare])
AMANDA_ENABLE_GCC_WARNING([float-equal])
AMANDA_ENABLE_GCC_WARNING([old-style-definition])
AMANDA_DISABLE_GCC_WARNING([strict-aliasing])
AMANDA_DISABLE_GCC_WARNING([unknown-pragmas])
AMANDA_CHECK_SSE42
AMANDA_WERROR_FLAGS
AMANDA_SWIG_ERROR

#
# Libtool
#
AM_PROG_LIBTOOL
AC_SUBST(LIBTOOL_DEPS)
AMANDA_STATIC_FLAGS

#
# headers
#
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_TIME
AC_CHECK_HEADERS(
	grp.h \
	libc.h \
	libgen.h \
	limits.h \
	math.h \
	netinet/in.h \
	regex.h \
	stdarg.h \
	stdlib.h \
	stdint.h \
	strings.h \
	rpc/rpc.h \
	sys/file.h \
	sys/ioctl.h \
	sys/ipc.h \
	sys/mntent.h \
	sys/param.h \
	sys/select.h \
	sys/stat.h \
	sys/shm.h \
	sys/time.h \
	sys/types.h \
	sys/uio.h \
	syslog.h \
	time.h \
	unistd.h \
)
AC_DEFINE([HAVE_AMANDA_H], 1, [Define to 1 if you have the "amanda.h" header file.])
AC_DEFINE([HAVE_UTIL_H], 1, [Define to 1 if you have the "util.h" header file.])

AC_DEFINE([USE_GETTEXT], 1, [Define to 1 if files will be processed with gettextize])

# okay if not found.. if not needed
PKG_CHECK_MODULES([TIRPC],[libtirpc],[ 
   AC_DEFINE([HAVE_RPC_RPC_H], 1) 
],[
   dnl nothing here
])

#
# Types
#
AC_CHECK_SIZEOF(int)
AC_CHECK_SIZEOF(long)
AC_CHECK_SIZEOF(long long)
AC_CHECK_SIZEOF(intmax_t)
AC_CHECK_SIZEOF(off_t)
AC_CHECK_SIZEOF(size_t)
AC_CHECK_SIZEOF(ssize_t)
AC_CHECK_SIZEOF(time_t)
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_TYPE_UID_T
AC_TYPE_SIGNAL
AC_STRUCT_TM
AMANDA_SOCKLEN_T_EQUIV
AMANDA_CHECK_TYPE(sa_family_t, unsigned short, sys/socket.h)
AMANDA_CHECK_TYPE(in_port_t, unsigned short, netinet/in.h)
CF_WAIT
CF_WAIT_INT

#
# Libraries
#
# cur_colr is on some HP's
AC_CHECK_LIB(cur_colr,main)
AC_CHECK_LIB(intl,main)
AMANDA_CHECK_NET_LIBS
AMANDA_CHECK_GLIB
AMANDA_CHECK_READLINE
AC_CHECK_LIB(m,modf)
AMANDA_CHECK_LIBDL
AMANDA_GLIBC_BACKTRACE
AC_SEARCH_LIBS([shm_open], [rt], [], [
  AC_MSG_ERROR([unable to find the shm_open() function])
])



#
# Declarations
#
# Checks for library functions and if the function is declared in
# an appropriate header file.  Functions which exist, but for which
# no declaration is available, are declared in common-src/amanda.h.
# It's not clear that any existing system implements but does not
# declare common functions such as these.
#
ICE_CHECK_DECL(accept,sys/types.h sys/socket.h)
AC_FUNC_ALLOCA
AC_CHECK_FUNCS(atexit)
ICE_CHECK_DECL(atof,stdlib.h)
ICE_CHECK_DECL(atol,stdlib.h)
ICE_CHECK_DECL(atoll,stdlib.h)
ICE_CHECK_DECL(strtol,stdlib.h)
ICE_CHECK_DECL(strtoll,stdlib.h)
AC_CHECK_FUNCS(basename)
ICE_CHECK_DECL(bind,sys/types.h sys/socket.h)
ICE_CHECK_DECL(bcopy,string.h strings.h stdlib.h)
ICE_CHECK_DECL(bzero,string.h strings.h stdlib.h)
AC_FUNC_CLOSEDIR_VOID
ICE_CHECK_DECL(closelog,syslog.h)
ICE_CHECK_DECL(connect,sys/types.h sys/socket.h)
ICE_CHECK_DECL(fclose,stdio.h)
ICE_CHECK_DECL(fflush,stdio.h)
ICE_CHECK_DECL(fprintf,stdio.h)
ICE_CHECK_DECL(fputc,stdio.h)
ICE_CHECK_DECL(fputs,stdio.h)
ICE_CHECK_DECL(fread,stdio.h stdlib.h)
ICE_CHECK_DECL(fseek,stdio.h)
ICE_CHECK_DECL(fwrite,stdio.h stdlib.h)
AC_CHECK_FUNCS(getgrgid_r)
AC_CHECK_FUNCS(getpwuid_r)
AC_CHECK_FUNCS(realpath)
ICE_CHECK_DECL(gethostname,unistd.h)
ICE_CHECK_DECL(getopt,stdlib.h unistd.h libc.h)
ICE_CHECK_DECL(getpeername,sys/types.h sys/socket.h)
AC_CHECK_FUNC(getpgrp)
AC_FUNC_GETPGRP
ICE_CHECK_DECL(getsockname,sys/types.h sys/socket.h)
ICE_CHECK_DECL(getsockopt,sys/types.h sys/socket.h)
ICE_CHECK_DECL(initgroups,grp.h sys/types.h unistd.h libc.h)
ICE_CHECK_DECL(ioctl,sys/ioctl.h unistd.h libc.h)
ICE_CHECK_DECL(isnormal,math.h)
ICE_CHECK_DECL(listen,sys/types.h sys/socket.h)
ICE_CHECK_DECL(lstat,sys/types.h sys/stat.h)
ICE_CHECK_DECL(malloc,stdlib.h)
ICE_CHECK_DECL(memmove,string.h strings.h)
ICE_CHECK_DECL(memset,string.h strings.h)
ICE_CHECK_DECL(mkstemp,stdlib.h)
ICE_CHECK_DECL(mktemp,stdlib.h)
ICE_CHECK_DECL(mktime,time.h sys/time.h)
AC_CHECK_FUNCS(on_exit)
ICE_CHECK_DECL(openlog,syslog.h)
ICE_CHECK_DECL(pclose,stdio.h)
ICE_CHECK_DECL(perror,stdio.h)
ICE_CHECK_DECL(printf,stdio.h)
AC_CHECK_FUNCS(putenv)
ICE_CHECK_DECL(puts,stdio.h)
ICE_CHECK_DECL(realloc,stdlib.h)
ICE_CHECK_DECL(recvfrom,sys/types.h sys/socket.h)
ICE_CHECK_DECL(remove,stdio.h)
ICE_CHECK_DECL(rename,stdio.h)
ICE_CHECK_DECL(rewind,stdio.h)
ICE_CHECK_DECL(ruserok,netdb.h sys/socket.h libc.h unistd.h)
ICE_CHECK_DECL(select,sys/types.h sys/socket.h sys/select.h time.h sys/time.h)
AMANDA_FUNC_SELECT_ARG_TYPE
ICE_CHECK_DECL(sendto,sys/types.h sys/socket.h)
ICE_CHECK_DECL(setegid,unistd.h)
ICE_CHECK_DECL(seteuid,unistd.h)
ICE_CHECK_DECL(setresgid,unistd.h)
ICE_CHECK_DECL(setresuid,unistd.h)
ICE_CHECK_DECL(snprintf,stdio.h)
ICE_CHECK_DECL(vsnprintf,stdio.h)
AMANDA_FUNC_SETPGID
AC_CHECK_FUNC(setpgrp,[AC_FUNC_SETPGRP])
ICE_CHECK_DECL(setpgrp,sys/types.h unistd.h libc.h)
ICE_CHECK_DECL(setsockopt,sys/types.h sys/socket.h)
AC_CHECK_FUNCS(sigaction sigemptyset sigvec)
ICE_CHECK_DECL(socket,sys/types.h sys/socket.h)
ICE_CHECK_DECL(socketpair,sys/types.h sys/socket.h)
ICE_CHECK_DECL(sscanf,stdio.h)
ICE_CHECK_DECL(strerror,string.h strings.h)
AC_FUNC_STRFTIME
ICE_CHECK_DECL(strftime,time.h sys/time.h)
ICE_CHECK_DECL(strncasecmp,string.h strings.h)
ICE_CHECK_DECL(syslog,syslog.h)
ICE_CHECK_DECL(system,stdlib.h)
ICE_CHECK_DECL(time,time.h sys/time.h)
ICE_CHECK_DECL(tolower,ctype.h)
ICE_CHECK_DECL(toupper,ctype.h)
ICE_CHECK_DECL(ungetc,stdio.h)
AC_CHECK_FUNCS(unsetenv)
ICE_CHECK_DECL(vfprintf,stdio.h stdlib.h)
ICE_CHECK_DECL(vprintf,stdio.h stdlib.h)
AC_CHECK_FUNC(wait4)
ICE_CHECK_DECL(writev, unistd.h sys/uio.h)
ICE_CHECK_DECL(strcasecmp,string.h strings.h)
ICE_CHECK_DECL(euidaccess,unistd.h)
ICE_CHECK_DECL(eaccess,unistd.h)
ICE_CHECK_DECL(clock_gettime,time.h)
AX_FUNC_WHICH_GETSERVBYNAME_R
AC_CHECK_FUNCS(sem_timedwait)

#
# Devices
#
# libcurl must be done at the end.
#
AMANDA_CHECK_DEVICES

])
