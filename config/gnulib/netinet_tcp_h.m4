# netinet_tcp_h.m4 serial 5
dnl Copyright (C) 2006-2020 Free Software Foundation, Inc.
dnl This file is free software; the Free Software Foundation
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([gl_HEADER_NETINET_TCP],
[
  AC_CACHE_CHECK([whether <netinet/tcp.h> is self-contained],
    [gl_cv_header_netinet_tcp_h_selfcontained],
    [
      AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[#include <netinet/tcp.h>]], [[]])],
        [gl_cv_header_netinet_tcp_h_selfcontained=yes],
        [gl_cv_header_netinet_tcp_h_selfcontained=no])
    ])
  if test $gl_cv_header_netinet_tcp_h_selfcontained = yes; then
    NETINET_TCP_H=''
  else
    NETINET_TCP_H='netinet/tcp.h'
    AC_CHECK_HEADERS([netinet/tcp.h])
    gl_CHECK_NEXT_HEADERS([netinet/tcp.h])
    if test $ac_cv_header_netinet_tcp_h = yes; then
      HAVE_NETINET_TCP_H=1
    else
      HAVE_NETINET_TCP_H=0
    fi
    AC_SUBST([HAVE_NETINET_TCP_H])
  fi
  AC_SUBST([NETINET_TCP_H])
  AM_CONDITIONAL([GL_GENERATE_NETINET_TCP_H], [test -n "$NETINET_TCP_H"])
])
