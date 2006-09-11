/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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
 * $Id: dgram.c,v 1.32 2006/07/05 19:54:20 martinea Exp $
 *
 * library routines to marshall/send, recv/unmarshall UDP packets
 */
#include "amanda.h"
#include "arglist.h"
#include "dgram.h"
#include "util.h"

void
dgram_socket(
    dgram_t *	dgram,
    int		socket)
{
    if(socket < 0 || socket >= FD_SETSIZE) {
	error("dgram_socket: socket %d out of range (0 .. %d)\n",
	      socket, FD_SETSIZE-1);
        /*NOTREACHED*/
    }
    dgram->socket = socket;
}


int
dgram_bind(
    dgram_t *	dgram,
    in_port_t *	portp)
{
    int s, retries;
    socklen_t len;
    struct sockaddr_in name;
    int save_errno;
#if defined(USE_REUSEADDR)
    const int on = 1;
    int r;
#endif

    *portp = (in_port_t)0;
    if((s = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
	save_errno = errno;
	dbprintf(("%s: dgram_bind: socket() failed: %s\n",
		  debug_prefix(NULL),
		  strerror(save_errno)));
	errno = save_errno;
	return -1;
    }
    if(s < 0 || s >= (int)FD_SETSIZE) {
	dbprintf(("%s: dgram_bind: socket out of range: %d\n",
		  debug_prefix(NULL),
		  s));
	aclose(s);
	errno = EMFILE;				/* out of range */
	return -1;
    }

    memset(&name, 0, SIZEOF(name));
    name.sin_family = (sa_family_t)AF_INET;
    name.sin_addr.s_addr = INADDR_ANY;

#ifdef USE_REUSEADDR
    r = setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
	(void *)&on, (socklen_t)sizeof(on));
    if (r < 0) {
	dbprintf(("%s: dgram_bind: setsockopt(SO_REUSEADDR) failed: %s\n",
		  debug_prefix(NULL),
		  strerror(errno)));
    }
#endif

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
#ifdef UDPPORTRANGE
	if (bind_portrange(s, &name, UDPPORTRANGE, "udp") == 0)
	    goto out;
	dbprintf(("%s: dgram_bind: Could to bind to port in range: %d - %d.\n",
		  debug_prefix(NULL), UDPPORTRANGE));
#endif

	if (bind_portrange(s, &name, (in_port_t)512,
		(in_port_t)(IPPORT_RESERVED - 1), "udp") == 0)
	    goto out;
	dbprintf(("%s: dgram_bind: Could to bind to port in range: 512 - %d.\n",
		  debug_prefix(NULL), IPPORT_RESERVED - 1));

	name.sin_port = INADDR_ANY;
	if (bind(s, (struct sockaddr *)&name, (socklen_t)sizeof(name)) == 0)
	    goto out;
	dbprintf(("%s: dgram_bind: Could to bind to any port: %s\n",
		  debug_prefix(NULL), strerror(errno)));

	if (retries >= BIND_CYCLE_RETRIES) {
	    dbprintf(("%s: dgram_bind: Giving up...\n", debug_prefix(NULL)));
	    break;
	}

	dbprintf(("%s: dgram_bind: Retrying entire range after 10 second delay.\n",
		  debug_prefix(NULL)));
	sleep(15);
    }

    save_errno = errno;
    dbprintf(("%s: dgram_bind: bind(INADDR_ANY) failed: %s\n",
		  debug_prefix(NULL),
		  strerror(save_errno)));
    aclose(s);
    errno = save_errno;
    return -1;

out:
    /* find out what name was actually used */

    len = (socklen_t)sizeof(name);
    if(getsockname(s, (struct sockaddr *)&name, &len) == -1) {
	save_errno = errno;
	dbprintf(("%s: dgram_bind: getsockname() failed: %s\n",
		  debug_prefix(NULL),
		  strerror(save_errno)));
	errno = save_errno;
	aclose(s);
	return -1;
    }
    *portp = (in_port_t)ntohs(name.sin_port);
    dgram->socket = s;

    dbprintf(("%s: dgram_bind: socket bound to %s.%d\n",
	      debug_prefix_time(NULL),
	      inet_ntoa(name.sin_addr),
	      *portp));
    return 0;
}


int
dgram_send_addr(
    struct sockaddr_in	addr,
    dgram_t *		dgram)
{
    int s, rc;
    int socket_opened;
    struct sockaddr_in addr_save;
    int save_errno;
    int max_wait;
    int wait_count;
#if defined(USE_REUSEADDR)
    const int on = 1;
    int r;
#endif

    dbprintf(("%s: dgram_send_addr(addr=%p, dgram=%p)\n",
			debug_prefix(NULL), &addr, dgram));
    dump_sockaddr(&addr);
    dbprintf(("%s: dgram_send_addr: %p->socket = %d\n",
			debug_prefix(NULL), dgram, dgram->socket));
    if(dgram->socket != -1) {
	s = dgram->socket;
	socket_opened = 0;
    } else {
	if((s = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
	    save_errno = errno;
	    dbprintf(("%s: dgram_send_addr: socket() failed: %s\n",
		      debug_prefix(NULL),
		      strerror(save_errno)));
	    errno = save_errno;
	    return -1;
	}
	socket_opened = 1;
#ifdef USE_REUSEADDR
	r = setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
		(void *)&on, (socklen_t)sizeof(on));
	if (r < 0) {
	    dbprintf(("%s: dgram_send_addr: setsockopt(SO_REUSEADDR) failed: %s\n",
			  debug_prefix(NULL),
			  strerror(errno)));
	}
#endif
    }

    memcpy(&addr_save, &addr, SIZEOF(addr));
    if(s < 0 || s >= FD_SETSIZE) {
	dbprintf(("%s: dgram_send_addr: socket out of range: %d\n",
		  debug_prefix(NULL),
		  s));
	errno = EMFILE;				/* out of range */
	rc = -1;
    } else {
	max_wait = 300 / 5;				/* five minutes */
	wait_count = 0;
	rc = 0;
	while(sendto(s,
		 dgram->data,
		 dgram->len,
		 0, 
		 (struct sockaddr *)&addr,
		 (int)sizeof(struct sockaddr_in)) == -1) {
#ifdef ECONNREFUSED
	    if(errno == ECONNREFUSED && wait_count++ < max_wait) {
		sleep(5);
		dbprintf(("%s: dgram_send_addr: sendto(%s.%hu): retry %d after ECONNREFUSED\n",
		      debug_prefix_time(NULL),
		      inet_ntoa(addr_save.sin_addr),
		      (in_port_t)ntohs(addr.sin_port),
		      wait_count));
		continue;
	    }
#endif
#ifdef EAGAIN
	    if(errno == EAGAIN && wait_count++ < max_wait) {
		sleep(5);
		dbprintf(("%s: dgram_send_addr: sendto(%s.%hu): retry %d after EAGAIN\n",
		      debug_prefix_time(NULL),
		      inet_ntoa(addr_save.sin_addr),
		      (in_port_t)ntohs(addr.sin_port),
		      wait_count));
		continue;
	    }
#endif
	    save_errno = errno;
	    dbprintf(("%s: dgram_send_addr: sendto(%s.%d) failed: %s \n",
		  debug_prefix_time(NULL),
		  inet_ntoa(addr_save.sin_addr),
		  (int) ntohs(addr.sin_port),
		  strerror(save_errno)));
	    errno = save_errno;
	    rc = -1;
	    break;
	}
    }

    if(socket_opened) {
	save_errno = errno;
	if(close(s) == -1) {
	    dbprintf(("%s: dgram_send_addr: close(%s.%d): failed: %s\n",
		      debug_prefix(NULL),
		      inet_ntoa(addr_save.sin_addr),
		      (int) ntohs(addr.sin_port),
		      strerror(errno)));
	    /*
	     * Calling function should not care that the close failed.
	     * It does care about the send results though.
	     */
	}
	errno = save_errno;
    }

    return rc;
}


int
dgram_send(
    char *	hostname,
    in_port_t	port,
    dgram_t *	dgram)
{
    struct sockaddr_in name;
    struct hostent *hp;
    int save_errno;

    if((hp = gethostbyname(hostname)) == 0) {
	save_errno = errno;
	dbprintf(("%s: dgram_send: gethostbyname(%s) failed\n",
		  debug_prefix_time(NULL),
		  hostname));
	errno = save_errno;
	return -1;
    }
    memcpy(&name.sin_addr, hp->h_addr, (size_t)hp->h_length);
    name.sin_family = (sa_family_t)AF_INET;
    name.sin_port = (in_port_t)htons(port);

    return dgram_send_addr(name, dgram);
}


ssize_t
dgram_recv(
    dgram_t *		dgram,
    int			timeout,
    struct sockaddr_in *fromaddr)
{
    SELECT_ARG_TYPE ready;
    struct timeval to;
    ssize_t size;
    int sock;
    socklen_t addrlen;
    ssize_t nfound;
    int save_errno;

    sock = dgram->socket;

    FD_ZERO(&ready);
    FD_SET(sock, &ready);
    to.tv_sec = timeout;
    to.tv_usec = 0;

    dbprintf(("%s: dgram_recv(dgram=%p, timeout=%u, fromaddr=%p)\n",
		debug_prefix_time(NULL), timeout, fromaddr));
    dump_sockaddr(fromaddr);
    
    nfound = (ssize_t)select(sock+1, &ready, NULL, NULL, &to);
    if(nfound <= 0 || !FD_ISSET(sock, &ready)) {
	save_errno = errno;
	if(nfound < 0) {
	    dbprintf(("%s: dgram_recv: select() failed: %s\n",
		      debug_prefix_time(NULL),
		      strerror(save_errno)));
	} else if(nfound == 0) {
	    dbprintf(("%s: dgram_recv: timeout after %d second%s\n",
		      debug_prefix_time(NULL),
		      timeout,
		      (timeout == 1) ? "" : "s"));
	    nfound = 0;
	} else if (!FD_ISSET(sock, &ready)) {
	    int i;

	    for(i = 0; i < sock + 1; i++) {
		if(FD_ISSET(i, &ready)) {
		    dbprintf(("%s: dgram_recv: got fd %d instead of %d\n",
			      debug_prefix_time(NULL),
			      i,
			      sock));
		}
	    }
	    save_errno = EBADF;
	    nfound = -1;
	}
	errno = save_errno;
	return nfound;
    }

    addrlen = (socklen_t)sizeof(struct sockaddr_in);
    size = recvfrom(sock, dgram->data, (size_t)MAX_DGRAM, 0,
		    (struct sockaddr *)fromaddr, &addrlen);
    if(size == -1) {
	save_errno = errno;
	dbprintf(("%s: dgram_recv: recvfrom() failed: %s\n",
		  debug_prefix(NULL),
		  strerror(save_errno)));
	errno = save_errno;
	return -1;
    }
    dgram->len = (size_t)size;
    dgram->data[size] = '\0';
    dgram->cur = dgram->data;
    return size;
}


void
dgram_zero(
    dgram_t *	dgram)
{
    dgram->cur = dgram->data;
    dgram->len = 0;
    *(dgram->cur) = '\0';
}

printf_arglist_function1(int dgram_cat, dgram_t *, dgram, const char *, fmt)
{
    ssize_t bufsize;
    va_list argp;
    int len;

    assert(dgram != NULL);
    assert(fmt != NULL);

    assert(dgram->len == (size_t)(dgram->cur - dgram->data));
    assert(dgram->len < SIZEOF(dgram->data));

    bufsize = (ssize_t)(sizeof(dgram->data) - dgram->len);
    if (bufsize <= 0)
	return -1;

    arglist_start(argp, fmt);
    len = vsnprintf(dgram->cur, (size_t)bufsize, fmt, argp);
    arglist_end(argp);
    if((ssize_t)len > bufsize) {
	dgram->len = sizeof(dgram->data);
	dgram->cur = dgram->data + dgram->len;
	return -1;
    }
    else {
	arglist_start(argp, fmt);
	dgram->len += vsnprintf(dgram->cur, (size_t)bufsize, fmt, argp);
	arglist_end(argp);
	dgram->cur = dgram->data + dgram->len;
    }
    return 0;
}

void
dgram_eatline(
    dgram_t *	dgram)
{
    char *p = dgram->cur;
    char *end = dgram->data + dgram->len;

    while(p < end && *p && *p != '\n')
	p++;
    if(*p == '\n')
	p++;
    dgram->cur = p;
}
