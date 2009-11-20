/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: stream.c,v 1.39 2006/08/24 01:57:15 paddy_s Exp $
 *
 * functions for managing stream sockets
 */
#include "amanda.h"
#include "dgram.h"
#include "stream.h"
#include "util.h"
#include "conffile.h"
#include "security-util.h"
#include "sockaddr-util.h"

/* local functions */
static void try_socksize(int sock, int which, size_t size);
static int stream_client_internal(const char *hostname, in_port_t port,
		size_t sendsize, size_t recvsize, in_port_t *localport,
		int nonblock, int priv);

int
stream_server(
    int family,
    in_port_t *portp,
    size_t sendsize,
    size_t recvsize,
    int    priv)
{
    int server_socket, retries;
    socklen_t_equiv len;
#if defined(SO_KEEPALIVE) || defined(USE_REUSEADDR)
    const int on = 1;
    int r;
#endif
    sockaddr_union server;
    int save_errno;
    int *portrange;
    socklen_t_equiv socklen;
    int socket_family;

    *portp = USHRT_MAX;				/* in case we error exit */
    if (family == -1) {
	socket_family = AF_NATIVE;
    } else {
	socket_family = family;
    }
    g_debug("stream_server opening socket with family %d (requested family was %d)", socket_family, family);
    server_socket = socket(socket_family, SOCK_STREAM, 0);

#ifdef WORKING_IPV6
    /* if that address family actually isn't supported, just try AF_INET */
    if (server_socket == -1 && errno == EAFNOSUPPORT) {
	g_debug("stream_server retrying socket with AF_INET");
	socket_family = AF_INET;
	server_socket = socket(AF_INET, SOCK_STREAM, 0);
    }
#endif
    if (server_socket == -1) {
	save_errno = errno;
	g_debug(_("stream_server: socket() failed: %s"),
		  strerror(save_errno));
	errno = save_errno;
	return -1;
    }
    if(server_socket < 0 || server_socket >= (int)FD_SETSIZE) {
	aclose(server_socket);
	errno = EMFILE;				/* out of range */
	save_errno = errno;
	g_debug(_("stream_server: socket out of range: %d"),
		  server_socket);
	errno = save_errno;
	return -1;
    }

    SU_INIT(&server, socket_family);
    SU_SET_INADDR_ANY(&server);

#ifdef USE_REUSEADDR
    r = setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR,
	(void *)&on, (socklen_t_equiv)sizeof(on));
    if (r < 0) {
	g_debug(_("stream_server: setsockopt(SO_REUSEADDR) failed: %s"),
		  strerror(errno));
    }
#endif

    try_socksize(server_socket, SO_SNDBUF, sendsize);
    try_socksize(server_socket, SO_RCVBUF, recvsize);

    /*
     * If a port range was specified, we try to get a port in that
     * range first.  Next, we try to get a reserved port.  If that
     * fails, we just go for any port.
     * 
     * In all cases, not to use port that's assigned to other services. 
     *
     * It is up to the caller to make sure we have the proper permissions
     * to get the desired port, and to make sure we return a port that
     * is within the range it requires.
     */
    for (retries = 0; ; retries++) {
	if (priv) {
	    portrange = getconf_intrange(CNF_RESERVED_TCP_PORT);
	} else {
	    portrange = getconf_intrange(CNF_UNRESERVED_TCP_PORT);
	}

	if (portrange[0] != 0 && portrange[1] != 0) {
	    if (bind_portrange(server_socket, &server, (in_port_t)portrange[0],
			       (in_port_t)portrange[1], "tcp") == 0)
		goto out;
	    g_debug(_("stream_server: Could not bind to port in range: %d - %d."),
		      portrange[0], portrange[1]);
	} else {
	    socklen = SS_LEN(&server);
	    if (bind(server_socket, (struct sockaddr *)&server, socklen) == 0)
		goto out;
	    g_debug(_("stream_server: Could not bind to any port: %s"),
		      strerror(errno));
	}

	if (retries >= BIND_CYCLE_RETRIES)
	    break;

	g_debug(_("stream_server: Retrying entire range after 10 second delay."));

	sleep(15);
    }

    save_errno = errno;
    g_debug(_("stream_server: bind(in6addr_any) failed: %s"),
		  strerror(save_errno));
    aclose(server_socket);
    errno = save_errno;
    return -1;

out:
    listen(server_socket, 1);

    /* find out what port was actually used */

    len = SIZEOF(server);
    if(getsockname(server_socket, (struct sockaddr *)&server, &len) == -1) {
	save_errno = errno;
	g_debug(_("stream_server: getsockname() failed: %s"),
		  strerror(save_errno));
	aclose(server_socket);
	errno = save_errno;
	return -1;
    }

#ifdef SO_KEEPALIVE
    r = setsockopt(server_socket, SOL_SOCKET, SO_KEEPALIVE,
	(void *)&on, (socklen_t_equiv)sizeof(on));
    if(r == -1) {
	save_errno = errno;
	g_debug(_("stream_server: setsockopt(SO_KEEPALIVE) failed: %s"),
		  strerror(save_errno));
        aclose(server_socket);
	errno = save_errno;
        return -1;
    }
#endif

    *portp = SU_GET_PORT(&server);
    g_debug(_("stream_server: waiting for connection: %s"),
	      str_sockaddr(&server));
    return server_socket;
}

static int
stream_client_internal(
    const char *hostname,
    in_port_t port,
    size_t sendsize,
    size_t recvsize,
    in_port_t *localport,
    int nonblock,
    int priv)
{
    sockaddr_union svaddr, claddr;
    int save_errno = 0;
    char *f;
    int client_socket = 0;
    int *portrange = NULL;
    int result;
    struct addrinfo *res, *res_addr;

    f = priv ? "stream_client_privileged" : "stream_client";

    result = resolve_hostname(hostname, SOCK_STREAM, &res, NULL);
    if(result != 0) {
	g_debug(_("resolve_hostname(%s): %s"), hostname, gai_strerror(result));
	errno = EHOSTUNREACH;
	return -1;
    }
    if(!res) {
	g_debug(_("resolve_hostname(%s): no results"), hostname);
	errno = EHOSTUNREACH;
	return -1;
    }

    for (res_addr = res; res_addr != NULL; res_addr = res_addr->ai_next) {
	/* copy the first (preferred) address we found */
	copy_sockaddr(&svaddr, (sockaddr_union *)res_addr->ai_addr);
	SU_SET_PORT(&svaddr, port);

	SU_INIT(&claddr, SU_GET_FAMILY(&svaddr));
	SU_SET_INADDR_ANY(&claddr);

	/*
	 * If a privileged port range was requested, we try to get a port in
	 * that range first and fail if it is not available.  Next, we try
	 * to get a port in the range built in when Amanda was configured.
	 * If that fails, we just go for any port.
	 *
	 * It is up to the caller to make sure we have the proper permissions
	 * to get the desired port, and to make sure we return a port that
	 * is within the range it requires.
	 */
	if (priv) {
	    portrange = getconf_intrange(CNF_RESERVED_TCP_PORT);
	} else {
	    portrange = getconf_intrange(CNF_UNRESERVED_TCP_PORT);
	}
	client_socket = connect_portrange(&claddr, (in_port_t)portrange[0],
					  (in_port_t)portrange[1],
					  "tcp", &svaddr, nonblock);
	save_errno = errno;
	if (client_socket > 0)
	    break;
    }

    freeaddrinfo(res);
					  
    if (client_socket > 0)
	goto out;

    g_debug(_("stream_client: Could not bind to port in range %d-%d."),
	      portrange[0], portrange[1]);

    errno = save_errno;
    return -1;

out:
    try_socksize(client_socket, SO_SNDBUF, sendsize);
    try_socksize(client_socket, SO_RCVBUF, recvsize);
    if (localport != NULL)
	*localport = SU_GET_PORT(&claddr);
    return client_socket;
}

int
stream_client_privileged(
    const char *hostname,
    in_port_t port,
    size_t sendsize,
    size_t recvsize,
    in_port_t *localport,
    int nonblock)
{
    return stream_client_internal(hostname,
				  port,
				  sendsize,
				  recvsize,
				  localport,
				  nonblock,
				  1);
}

int
stream_client(
    const char *hostname,
    in_port_t port,
    size_t sendsize,
    size_t recvsize,
    in_port_t *localport,
    int nonblock)
{
    return stream_client_internal(hostname,
				  port,
				  sendsize,
				  recvsize,
				  localport,
				  nonblock,
				  0);
}

/* don't care about these values */
static sockaddr_union addr;
static socklen_t_equiv addrlen;

int
stream_accept(
    int server_socket,
    int timeout,
    size_t sendsize,
    size_t recvsize)
{
    SELECT_ARG_TYPE readset;
    struct timeval tv;
    int nfound, connected_socket;
    int save_errno;
    int ntries = 0;
    in_port_t port;

    assert(server_socket >= 0);

    do {
	ntries++;
	memset(&tv, 0, SIZEOF(tv));
	tv.tv_sec = timeout;
	memset(&readset, 0, SIZEOF(readset));
	FD_ZERO(&readset);
	FD_SET(server_socket, &readset);
	nfound = select(server_socket+1, &readset, NULL, NULL, &tv);
	if(nfound <= 0 || !FD_ISSET(server_socket, &readset)) {
	    save_errno = errno;
	    if(nfound < 0) {
		g_debug(_("stream_accept: select() failed: %s"),
		      strerror(save_errno));
	    } else if(nfound == 0) {
		g_debug(plural(_("stream_accept: timeout after %d second"),
			        _("stream_accept: timeout after %d seconds"),
			       timeout),
			 timeout);
		errno = ETIMEDOUT;
		return -1;
	    } else if (!FD_ISSET(server_socket, &readset)) {
		int i;

		for(i = 0; i < server_socket + 1; i++) {
		    if(FD_ISSET(i, &readset)) {
			g_debug(_("stream_accept: got fd %d instead of %d"),
			      i,
			      server_socket);
		    }
		}
	        save_errno = EBADF;
	    }
	    if (ntries > 5) {
		errno = save_errno;
		return -1;
	    }
        }
    } while (nfound <= 0);

    while(1) {
	addrlen = (socklen_t_equiv)sizeof(sockaddr_union);
	connected_socket = accept(server_socket,
				  (struct sockaddr *)&addr,
				  &addrlen);
	if(connected_socket < 0) {
	    break;
	}
	g_debug(_("stream_accept: connection from %s"),
	          str_sockaddr(&addr));
	/*
	 * Make certain we got an inet connection and that it is not
	 * from port 20 (a favorite unauthorized entry tool).
	 */
	if (SU_GET_FAMILY(&addr) == AF_INET
#ifdef WORKING_IPV6
	    || SU_GET_FAMILY(&addr) == AF_INET6
#endif
	    ){
	    port = SU_GET_PORT(&addr);
	    if (port != (in_port_t)20) {
		try_socksize(connected_socket, SO_SNDBUF, sendsize);
		try_socksize(connected_socket, SO_RCVBUF, recvsize);
		return connected_socket;
	    } else {
		g_debug(_("remote port is %u: ignored"),
			  (unsigned int)port);
	    }
	} else {
#ifdef WORKING_IPV6
	    g_debug(_("family is %d instead of %d(AF_INET)"
		      " or %d(AF_INET6): ignored"),
		      SU_GET_FAMILY(&addr),
		      AF_INET, AF_INET6);
#else
	    g_debug(_("family is %d instead of %d(AF_INET)"
		      ": ignored"),
		      SU_GET_FAMILY(&addr),
		      AF_INET);
#endif
	}
	aclose(connected_socket);
    }

    save_errno = errno;
    g_debug(_("stream_accept: accept() failed: %s"),
	      strerror(save_errno));
    errno = save_errno;
    return -1;
}

static void
try_socksize(
    int sock,
    int which,
    size_t size)
{
    size_t origsize;
    int    isize;

    if (size == 0)
	return;

    origsize = size;
    isize = size;
    /* keep trying, get as big a buffer as possible */
    while((isize > 1024) &&
	  (setsockopt(sock, SOL_SOCKET,
		      which, (void *) &isize, (socklen_t_equiv)sizeof(isize)) < 0)) {
	isize -= 1024;
    }
    if(isize > 1024) {
	g_debug(_("try_socksize: %s buffer size is %d"),
		  (which == SO_SNDBUF) ? _("send") : _("receive"),
		  isize);
    } else {
	g_debug(_("try_socksize: could not allocate %s buffer of %zu"),
		  (which == SO_SNDBUF) ? _("send") : _("receive"),
		  origsize);
    }
}
