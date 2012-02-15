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
#include "conffile.h"
#include "sockaddr-util.h"

void
dgram_socket(
    dgram_t *	dgram,
    int		socket)
{
    if(socket < 0 || socket >= (int)FD_SETSIZE) {
	error(_("dgram_socket: socket %d out of range (0 .. %d)\n"),
	      socket, (int)FD_SETSIZE-1);
        /*NOTREACHED*/
    }
    dgram->socket = socket;
}


int
dgram_bind(
    dgram_t *	dgram,
    sa_family_t family,
    in_port_t *	portp)
{
    int s, retries;
    socklen_t_equiv len;
    sockaddr_union name;
    int save_errno;
    int *portrange;
    int sndbufsize = MAX_DGRAM;

    portrange = getconf_intrange(CNF_RESERVED_UDP_PORT);
    *portp = (in_port_t)0;
    g_debug("dgram_bind: setting up a socket with family %d", family);
    if((s = socket(family, SOCK_DGRAM, 0)) == -1) {
	save_errno = errno;
	dbprintf(_("dgram_bind: socket() failed: %s\n"),
		  strerror(save_errno));
	errno = save_errno;
	return -1;
    }
    if(s < 0 || s >= (int)FD_SETSIZE) {
	dbprintf(_("dgram_bind: socket out of range: %d\n"),
		  s);
	aclose(s);
	errno = EMFILE;				/* out of range */
	return -1;
    }

    /* try setting the buffer size (= maximum allowable UDP packet size) */
    if (setsockopt(s, SOL_SOCKET, SO_SNDBUF,
		   (void *) &sndbufsize, sizeof(sndbufsize)) < 0) {
       dbprintf("dgram_bind: could not set udp send buffer to %d: %s (ignored)\n",
		 sndbufsize, strerror(errno));
    }

    SU_INIT(&name, family);
    SU_SET_INADDR_ANY(&name);

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
	if (bind_portrange(s, &name, portrange[0], portrange[1], "udp") == 0)
	    goto out;
	dbprintf(_("dgram_bind: Could not bind to port in range: %d - %d.\n"),
		  portrange[0], portrange[1]);
	if (retries >= BIND_CYCLE_RETRIES) {
	    dbprintf(_("dgram_bind: Giving up...\n"));
	    break;
	}

	dbprintf(_("dgram_bind: Retrying entire range after 10 second delay.\n"));
	sleep(15);
    }

    save_errno = errno;
    dbprintf(_("dgram_bind: bind(in6addr_any) failed: %s\n"),
		  strerror(save_errno));
    aclose(s);
    errno = save_errno;
    return -1;

out:
    /* find out what name was actually used */

    len = (socklen_t_equiv)sizeof(name);
    if(getsockname(s, (struct sockaddr *)&name, &len) == -1) {
	save_errno = errno;
	dbprintf(_("dgram_bind: getsockname() failed: %s\n"), strerror(save_errno));
	errno = save_errno;
	aclose(s);
	return -1;
    }
    *portp = SU_GET_PORT(&name);
    dgram->socket = s;

    dbprintf(_("dgram_bind: socket %d bound to %s\n"),
	      dgram->socket, str_sockaddr(&name));
    return 0;
}


int
dgram_send_addr(
    sockaddr_union	*addr,
    dgram_t *		dgram)
{
    int s, rc;
    int socket_opened;
    int save_errno;
    int max_wait;
    int wait_count;
#if defined(USE_REUSEADDR)
    const int on = 1;
    int r;
#endif

    dbprintf(_("dgram_send_addr(addr=%p, dgram=%p)\n"),
	      addr, dgram);
    dump_sockaddr(addr);
    dbprintf(_("dgram_send_addr: %p->socket = %d\n"),
	      dgram, dgram->socket);
    if(dgram->socket != -1) {
	s = dgram->socket;
	socket_opened = 0;
    } else {
	int sndbufsize = MAX_DGRAM;

	g_debug("dgram_send_addr: setting up a socket with family %d", SU_GET_FAMILY(addr));
	if((s = socket(SU_GET_FAMILY(addr), SOCK_DGRAM, 0)) == -1) {
	    save_errno = errno;
	    dbprintf(_("dgram_send_addr: socket() failed: %s\n"),
		      strerror(save_errno));
	    errno = save_errno;
	    return -1;
	}
	socket_opened = 1;
#ifdef USE_REUSEADDR
	r = setsockopt(s, SOL_SOCKET, SO_REUSEADDR,
		(void *)&on, (socklen_t_equiv)sizeof(on));
	if (r < 0) {
	    dbprintf(_("dgram_send_addr: setsockopt(SO_REUSEADDR) failed: %s\n"),
		      strerror(errno));
	}
#endif

	/* try setting the buffer size (= maximum allowable UDP packet size) */
	if (setsockopt(s, SOL_SOCKET, SO_SNDBUF,
		       (void *) &sndbufsize, sizeof(sndbufsize)) < 0) {
	   dbprintf("dgram_send_addr: could not set udp send buffer to %d: %s (ignored)\n",
		     sndbufsize, strerror(errno));
	}
    }

    if(s < 0 || s >= (int)FD_SETSIZE) {
	dbprintf(_("dgram_send_addr: socket out of range: %d\n"), s);
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
		 (struct sockaddr *)addr,
		 SS_LEN(addr)) == -1) {
#ifdef ECONNREFUSED
	    if(errno == ECONNREFUSED && wait_count++ < max_wait) {
		sleep(5);
		dbprintf(_("dgram_send_addr: sendto(%s): retry %d after ECONNREFUSED\n"),
		      str_sockaddr(addr),
		      wait_count);
		continue;
	    }
#endif
#ifdef EAGAIN
	    if(errno == EAGAIN && wait_count++ < max_wait) {
		sleep(5);
		dbprintf(_("dgram_send_addr: sendto(%s): retry %d after EAGAIN\n"),
		      str_sockaddr(addr),
		      wait_count);
		continue;
	    }
#endif
	    save_errno = errno;
	    dbprintf(_("dgram_send_addr: sendto(%s) failed: %s \n"),
		  str_sockaddr(addr),
		  strerror(save_errno));
	    errno = save_errno;
	    rc = -1;
	    break;
	}
    }

    if(socket_opened) {
	save_errno = errno;
	if(close(s) == -1) {
	    dbprintf(_("dgram_send_addr: close(%s): failed: %s\n"),
		      str_sockaddr(addr),
		      strerror(errno));
	    /*
	     * Calling function should not care that the close failed.
	     * It does care about the send results though.
	     */
	}
	errno = save_errno;
    }

    return rc;
}



ssize_t
dgram_recv(
    dgram_t *		dgram,
    int			timeout,
    sockaddr_union *fromaddr)
{
    SELECT_ARG_TYPE ready;
    struct timeval to;
    ssize_t size;
    int sock;
    socklen_t_equiv addrlen;
    ssize_t nfound;
    int save_errno;

    sock = dgram->socket;

    FD_ZERO(&ready);
    FD_SET(sock, &ready);
    to.tv_sec = timeout;
    to.tv_usec = 0;

    dbprintf(_("dgram_recv(dgram=%p, timeout=%u, fromaddr=%p)\n"),
		dgram, timeout, fromaddr);
    
    nfound = (ssize_t)select(sock+1, &ready, NULL, NULL, &to);
    if(nfound <= 0 || !FD_ISSET(sock, &ready)) {
	save_errno = errno;
	if(nfound < 0) {
	    dbprintf(_("dgram_recv: select() failed: %s\n"), strerror(save_errno));
	} else if(nfound == 0) {
	    dbprintf(plural(_("dgram_recv: timeout after %d second\n"),
			    _("dgram_recv: timeout after %d seconds\n"),
			    timeout),
		     timeout);
	    nfound = 0;
	} else if (!FD_ISSET(sock, &ready)) {
	    int i;

	    for(i = 0; i < sock + 1; i++) {
		if(FD_ISSET(i, &ready)) {
		    dbprintf(_("dgram_recv: got fd %d instead of %d\n"), i, sock);
		}
	    }
	    save_errno = EBADF;
	    nfound = -1;
	}
	errno = save_errno;
	return nfound;
    }

    addrlen = (socklen_t_equiv)sizeof(sockaddr_union);
    size = recvfrom(sock, dgram->data, (size_t)MAX_DGRAM, 0,
		    (struct sockaddr *)fromaddr, &addrlen);
    if(size == -1) {
	save_errno = errno;
	dbprintf(_("dgram_recv: recvfrom() failed: %s\n"), strerror(save_errno));
	errno = save_errno;
	return -1;
    }
    dump_sockaddr(fromaddr);
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
    len = g_vsnprintf(dgram->cur, (size_t)bufsize, fmt, argp);
    arglist_end(argp);
    if(len < 0) {
	return -1;
    } else if((ssize_t)len > bufsize) {
	dgram->len = sizeof(dgram->data);
	dgram->cur = dgram->data + dgram->len;
	return -1;
    }
    else {
	dgram->len += len;
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
