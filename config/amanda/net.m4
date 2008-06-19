# OVERVIEW
#
#   Networking-related macros

# SYNOPSIS
#
#   AMANDA_WITH_FQDN
#
# OVERVIEW
#
#   Check for --with-fqdn, and DEFINE USE_FQDN if given.
#
AC_DEFUN([AMANDA_WITH_FQDN], [
    AC_ARG_WITH(fqdn,
	AS_HELP_STRING([--with-fqdn],
		       [use FQDN's to backup multiple networks]),
	[ USE_FQDN=$withval ], [ USE_FQDN=no ])

    case "$USE_FQDN" in
    n | no) : ;;
    y |  ye | yes) 
	AC_DEFINE(USE_FQDN,1,
	    [Define for backups being done on a multiple networks and FQDNs are used. ])
      ;;
    *) AC_MSG_ERROR([You must not supply an argument to --with-fqdn option.])
      ;;
    esac
])

# SYNOPSIS
#
#   AMANDA_WITH_REUSEPORTS
#
# OVERVIEW
#
#   Check for --with-reuseports, and DEFINE USE_REUSEADDR if given.
#
AC_DEFUN([AMANDA_WITH_REUSEPORTS], [
    AC_ARG_WITH(reuseports,
	AS_HELP_STRING([--without-reuseaddr],
		       [Don't reuse network connections until full timeout period]),
	[ case "$withval" in
	    y | ye | yes) USE_REUSEADDR=no;;
	    n | no) USE_REUSEADDR=yes;;
	    *) AC_MSG_ERROR([You must not supply an argument to --without-reuseports]);;
	  esac
	],
	[ USE_REUSEADDR=yes; ])
    if test x"$USE_REUSEADDR" = x"yes"; then
	AC_DEFINE(USE_REUSEADDR,1,
		[Define to set SO_REUSEADDR on network connections.])
    fi
])

# SYNOPSIS
#
#   AMANDA_WITH_PORTRANGES
#
# OVERVIEW
#
#   Implement --with-low-tcpportrange, --with-tcpportrange, and --with-udpportrange.
#   Results are DEFINED and substituted in LOW_TCPPORTRANGE, TCPPORTRANGE, and 
#   UDPPORTRANGE, respectively.
#
AC_DEFUN([AMANDA_WITH_PORTRANGES], [
    AC_ARG_WITH(low-tcpportrange,
	AS_HELP_STRING([--with-low-tcpportrange=low/high],
	    [bind reserved TCP server sockets to ports within this range (default: unlimited)]),
	[ LOW_TCPPORTRANGE="$withval" ],
	[ LOW_TCPPORTRANGE=unlimited ])
     
    if test x"$LOW_TCPPORTRANGE" != x"unlimited"; then
	if test x`echo "$LOW_TCPPORTRANGE" | sed 's/[[0-9]][[0-9]]*,[[0-9]][[0-9]]*//'` != x""; then
	    AC_MSG_ERROR([--with-low-tcpportrange requires two comma-separated positive numbers])
	fi
	min_low_tcp_port=`echo "$LOW_TCPPORTRANGE" | sed 's/,.*//'`
	max_low_tcp_port=`echo "$LOW_TCPPORTRANGE" | sed 's/.*,//'`
	if test $min_low_tcp_port -gt $max_low_tcp_port; then
	    AC_MSG_ERROR([the second TCP port number must be greater than the first in --with-low-tcpportrange])
	fi
	if test $min_low_tcp_port -lt 512; then
	    AMANDA_MSG_WARN([the low TCP port range should be 512 or greater in --with-low-tcpportrange])
	fi
	if test $max_low_tcp_port -ge 1024; then
	    AMANDA_MSG_WARN([the low TCP port range should be less than 1024 in --with-low-tcpportrange])
	fi
	AC_DEFINE_UNQUOTED(LOW_TCPPORTRANGE,$LOW_TCPPORTRANGE,
   [A comma-separated list of two integers, determining the minimum and maximum
 * reserved TCP port numbers sockets should be bound to. (mainly for amrecover) ])
    fi

    AC_ARG_WITH(tcpportrange,
	AS_HELP_STRING([--with-tcpportrange=low/high],
	    [bind unreserved TCP server sockets to ports within this range (default: unlimited)]),
	[ TCPPORTRANGE="$withval" ],
	[ TCPPORTRANGE="unlimited" ])

    if test x"$TCPPORTRANGE" != x"unlimited"; then
	if test x`echo "$TCPPORTRANGE" | sed 's/[[0-9]][[0-9]]*,[[0-9]][[0-9]]*//'` != x""; then
	    AC_MSG_ERROR([--with-tcpportrange requires two comma-separated positive numbers])
	fi
	min_tcp_port=`echo "$TCPPORTRANGE" | sed 's/,.*//'`
	max_tcp_port=`echo "$TCPPORTRANGE" | sed 's/.*,//'`
	if test $min_tcp_port -gt $max_tcp_port; then
	    AC_MSG_ERROR([the second TCP port number must be greater than the first in --with-tcpportrange])
	fi
	if test $min_tcp_port -lt 1024; then
	    AMANDA_MSG_WARN([the TCP port range should be 1024 or greater in --with-tcpportrange])
	fi
	if test $max_tcp_port -ge 65536; then
	    AMANDA_MSG_WARN([the TCP port range should be less than 65536 in --with-tcpportrange])
	fi
	AC_DEFINE_UNQUOTED(TCPPORTRANGE,$TCPPORTRANGE,
  [A comma-separated list of two integers, determining the minimum and
 * maximum unreserved TCP port numbers sockets should be bound to. ])
    fi

    AC_ARG_WITH(udpportrange,
	AS_HELP_STRING([--with-udpportrange=low/high],
	    [bind reserved UDP server sockets to ports within this range (default: unlimited)]),
	[ UDPPORTRANGE="$withval" ],
	[ UDPPORTRANGE="unlimited" ])
    if test x"$UDPPORTRANGE" != x"unlimited"; then
	if test x`echo "$UDPPORTRANGE" | sed 's/[[0-9]][[0-9]]*,[[0-9]][[0-9]]*//'` != x""; then
	    AC_MSG_ERROR([--with-udpportrange requires two comma-separated positive numbers])
	fi
	min_udp_port=`echo "$UDPPORTRANGE" | sed 's/,.*//'`
	max_udp_port=`echo "$UDPPORTRANGE" | sed 's/.*,//'`
	if test $min_udp_port -gt $max_udp_port; then
	    AC_MSG_ERROR([the second UDP port number must be greater than the first in --with-udpportrange])
	fi
	if test $max_udp_port -ge 1024; then
	    AMANDA_MSG_WARN([the UDP port range should be less than 1025 in --with-udpportrange])
	fi
	if test $min_udp_port -le 0; then
	    AMANDA_MSG_WARN([the UDP port range should be greater than 0 in --with-udpportrange])
	fi
	AC_DEFINE_UNQUOTED(UDPPORTRANGE,$UDPPORTRANGE,
  [A comma-separated list of two integers, determining the minimum and
 * maximum reserved UDP port numbers sockets should be bound to. ])
    fi
    AC_SUBST(UDPPORTRANGE)
    AC_SUBST(TCPPORTRANGE)
    AC_SUBST(LOW_TCPPORTRANGE)
])

# SYNOPSIS
#
#   AMANDA_WITH_BUFFERED_DUMP
#
# OVERVIEW
#
#   Implement --with-buffered-dump, and DEFINEs DUMPER_SOCKET_BUFFERING if the option
#   is given.
#
AC_DEFUN([AMANDA_WITH_BUFFERED_DUMP], [
    AC_ARG_WITH(buffered-dump,
	AS_HELP_STRING([--with-buffered-dump],
	    [buffer the dumping sockets on the server for speed]),
	[ DUMPER_SOCKET_BUFFERING=$withval ],
	[ DUMPER_SOCKET_BUFFERING=no ])
    case "$DUMPER_SOCKET_BUFFERING" in
    n | no) ;;
    y | ye | yes)
	AC_DEFINE(DUMPER_SOCKET_BUFFERING,1,
	    [Define if dumper should buffer the sockets for faster throughput. ])
      ;;
    *) AC_MSG_ERROR([You must not supply an argument to --with-buffered-dump.]) ;;
    esac
])
