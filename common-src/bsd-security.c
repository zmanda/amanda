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
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */
/*
 * $Id: bsd-security.c,v 1.75 2006/07/19 17:41:14 martinea Exp $
 *
 * "BSD" security module
 */

#include "amanda.h"
#include "util.h"
#include "clock.h"
#include "dgram.h"
#include "event.h"
#include "packet.h"
#include "security.h"
#include "security-util.h"
#include "sockaddr-util.h"
#include "stream.h"

#ifndef SO_RCVBUF
#undef DUMPER_SOCKET_BUFFERING
#endif

/*
 * Interface functions
 */
static void	bsd_connect(const char *, char *(*)(char *, void *), 
			void (*)(void *, security_handle_t *, security_status_t),
			void *, void *);
static void	bsd_accept(const struct security_driver *,
			char *(*)(char *, void *),
			int, int,
			void (*)(security_handle_t *, pkt_t *),
			void *);
static void	bsd_close(void *);
static void *	bsd_stream_server(void *);
static int	bsd_stream_accept(void *);
static void *	bsd_stream_client(void *, int);
static void	bsd_stream_close(void *);
static int	bsd_stream_auth(void *);
static int	bsd_stream_id(void *);
static void	bsd_stream_read(void *, void (*)(void *, void *, ssize_t), void *);
static ssize_t	bsd_stream_read_sync(void *, void **);
static void	bsd_stream_read_cancel(void *);

/*
 * This is our interface to the outside world
 */
const security_driver_t bsd_security_driver = {
    "BSD",
    bsd_connect,
    bsd_accept,
    bsd_close,
    udpbsd_sendpkt,
    udp_recvpkt,
    udp_recvpkt_cancel,
    bsd_stream_server,
    bsd_stream_accept,
    bsd_stream_client,
    bsd_stream_close,
    bsd_stream_auth,
    bsd_stream_id,
    tcp_stream_write,
    bsd_stream_read,
    bsd_stream_read_sync,
    bsd_stream_read_cancel,
    sec_close_connection_none,
    NULL,
    NULL
};

/*
 * This is data local to the datagram socket.  We have one datagram
 * per process, so it is global.
 */
static udp_handle_t netfd4;
static udp_handle_t netfd6;
static int not_init4 = 1;
static int not_init6 = 1;

/* generate new handles from here */
static int newhandle = 0;

/*
 * These are the internal helper functions
 */
static void	stream_read_callback(void *);
static void	stream_read_sync_callback(void *);

/*
 * Setup and return a handle outgoing to a client
 */

static void
bsd_connect(
    const char *	hostname,
    char *		(*conf_fn)(char *, void *),
    void		(*fn)(void *, security_handle_t *, security_status_t),
    void *		arg,
    void *		datap)
{
    struct sec_handle *bh;
    in_port_t port = 0;
    struct timeval sequence_time;
    int sequence;
    char *handle;
    int result;
    struct addrinfo *res, *res_addr;
    char *canonname;
    int result_bind;
    char *service;

    assert(hostname != NULL);

    (void)conf_fn;	/* Quiet unused parameter warning */
    (void)datap;        /* Quiet unused parameter warning */

    bh = alloc(SIZEOF(*bh));
    bh->proto_handle=NULL;
    security_handleinit(&bh->sech, &bsd_security_driver);

    result = resolve_hostname(hostname, SOCK_DGRAM, &res, &canonname);
    if(result != 0) {
	dbprintf(_("resolve_hostname(%s): %s\n"), hostname, gai_strerror(result));
	security_seterror(&bh->sech, _("resolve_hostname(%s): %s\n"), hostname,
			  gai_strerror(result));
	(*fn)(arg, &bh->sech, S_ERROR);
	return;
    }
    if (canonname == NULL) {
	dbprintf(_("resolve_hostname(%s) did not return a canonical name\n"), hostname);
	security_seterror(&bh->sech,
	        _("resolve_hostname(%s) did not return a canonical name\n"), hostname);
	(*fn)(arg, &bh->sech, S_ERROR);
	if (res) freeaddrinfo(res);
	return;
    }
    if (res == NULL) {
	dbprintf(_("resolve_hostname(%s): no results\n"), hostname);
	security_seterror(&bh->sech,
	        _("resolve_hostname(%s): no results\n"), hostname);
	(*fn)(arg, &bh->sech, S_ERROR);
	amfree(canonname);
	return;
    }

    for (res_addr = res; res_addr != NULL; res_addr = res_addr->ai_next) {
#ifdef WORKING_IPV6
	/* IPv6 socket already bound */
	if (res_addr->ai_addr->sa_family == AF_INET6 && not_init6 == 0) {
	    break;
	}
	/*
	 * Only init the IPv6 socket once
	 */
	if (res_addr->ai_addr->sa_family == AF_INET6 && not_init6 == 1) {
	    uid_t euid;
	    dgram_zero(&netfd6.dgram);

	    euid = geteuid();
	    set_root_privs(1);
	    result_bind = dgram_bind(&netfd6.dgram,
				     res_addr->ai_addr->sa_family, &port);
	    set_root_privs(0);
	    if (result_bind != 0) {
		continue;
	    }
	    netfd6.handle = NULL;
	    netfd6.pkt.body = NULL;
	    netfd6.recv_security_ok = &bsd_recv_security_ok;
	    netfd6.prefix_packet = &bsd_prefix_packet;
	    /*
	     * We must have a reserved port.  Bomb if we didn't get one.
	     */
	    if (port >= IPPORT_RESERVED) {
		security_seterror(&bh->sech,
		    _("unable to bind to a reserved port (got port %u)"),
		    (unsigned int)port);
		(*fn)(arg, &bh->sech, S_ERROR);
		freeaddrinfo(res);
		amfree(canonname);
		return;
	    }
	    not_init6 = 0;
	    bh->udp = &netfd6;
	    break;
	}
#endif

	/* IPv4 socket already bound */
	if (res_addr->ai_addr->sa_family == AF_INET && not_init4 == 0) {
	    break;
	}

	/*
	 * Only init the IPv4 socket once
	 */
	if (res_addr->ai_addr->sa_family == AF_INET && not_init4 == 1) {
	    uid_t euid;
	    dgram_zero(&netfd4.dgram);

	    euid = geteuid();
	    set_root_privs(1);
	    result_bind = dgram_bind(&netfd4.dgram,
				     res_addr->ai_addr->sa_family, &port);
	    set_root_privs(0);
	    if (result_bind != 0) {
		continue;
	    }
	    netfd4.handle = NULL;
	    netfd4.pkt.body = NULL;
	    netfd4.recv_security_ok = &bsd_recv_security_ok;
	    netfd4.prefix_packet = &bsd_prefix_packet;
	    /*
	     * We must have a reserved port.  Bomb if we didn't get one.
	     */
	    if (port >= IPPORT_RESERVED) {
		security_seterror(&bh->sech,
		    "unable to bind to a reserved port (got port %u)",
		    (unsigned int)port);
		(*fn)(arg, &bh->sech, S_ERROR);
		freeaddrinfo(res);
		amfree(canonname);
		return;
	    }
	    not_init4 = 0;
	    bh->udp = &netfd4;
	    break;
	}
    }

    if (res_addr == NULL) {
	dbprintf(_("Can't bind a socket to connect to %s\n"), hostname);
	security_seterror(&bh->sech,
	        _("Can't bind a socket to connect to %s\n"), hostname);
	(*fn)(arg, &bh->sech, S_ERROR);
       amfree(canonname);
       return;
    }

#ifdef WORKING_IPV6
    if (res_addr->ai_addr->sa_family == AF_INET6)
	bh->udp = &netfd6;
    else
#endif
	bh->udp = &netfd4;

    auth_debug(1, _("Resolved hostname=%s\n"), canonname);

    if (conf_fn) {
        service = conf_fn("client_port", datap);
        if (strlen(service) <= 1)
            service = "amanda";
    } else {
        service = "amanda";
    }
    port = find_port_for_service(service, "udp");
    if (port == 0) {
        security_seterror(&bh->sech, _("%s/udp unknown protocol"), service);
	(*fn)(arg, &bh->sech, S_ERROR);
        amfree(canonname);
	return;
    }

    amanda_gettimeofday(&sequence_time);
    sequence = (int)sequence_time.tv_sec ^ (int)sequence_time.tv_usec;
    handle=alloc(15);
    g_snprintf(handle, 14, "000-%08x",  (unsigned)newhandle++);
    if (udp_inithandle(bh->udp, bh, canonname,
	(sockaddr_union *)res_addr->ai_addr, port, handle, sequence) < 0) {
	(*fn)(arg, &bh->sech, S_ERROR);
	amfree(bh->hostname);
	amfree(bh);
    }
    else {
	(*fn)(arg, &bh->sech, S_OK);
    }
    amfree(handle);
    amfree(canonname);

    freeaddrinfo(res);
}

/*
 * Setup to accept new incoming connections
 */
static void
bsd_accept(
    const struct security_driver *	driver,
    char       *(*conf_fn)(char *, void *),
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *),
    void       *datap)
{

    assert(in >= 0 && out >= 0);
    assert(fn != NULL);

    (void)out;	/* Quiet unused parameter warning */
    (void)driver; /* Quiet unused parameter warning */
    (void)conf_fn;
    (void)datap;

    /*
     * We assume in and out point to the same socket, and just use
     * in.
     */
    dgram_socket(&netfd4.dgram, in);
    dgram_socket(&netfd6.dgram, in);

    /*
     * Assign the function and return.  When they call recvpkt later,
     * the recvpkt callback will call this function when it discovers
     * new incoming connections
     */
    netfd4.accept_fn = fn;
    netfd4.recv_security_ok = &bsd_recv_security_ok;
    netfd4.prefix_packet = &bsd_prefix_packet;
    netfd4.driver = &bsd_security_driver;

    udp_addref(&netfd4, &udp_netfd_read_callback);
}

/*
 * Frees a handle allocated by the above
 */
static void
bsd_close(
    void *	cookie)
{
    struct sec_handle *bh = cookie;

    if(bh->proto_handle == NULL) {
	return;
    }

    auth_debug(1, _("bsd: close handle '%s'\n"), bh->proto_handle);

    udp_recvpkt_cancel(bh);
    if(bh->next) {
	bh->next->prev = bh->prev;
    }
    else {
	if (!not_init6 && netfd6.bh_last == bh)
	    netfd6.bh_last = bh->prev;
	else
	    netfd4.bh_last = bh->prev;
    }
    if(bh->prev) {
	bh->prev->next = bh->next;
    }
    else {
	if (!not_init6 && netfd6.bh_first == bh)
	    netfd6.bh_first = bh->next;
	else
	    netfd4.bh_first = bh->next;
    }

    amfree(bh->proto_handle);
    amfree(bh->hostname);
    amfree(bh);
}

/*
 * Create the server end of a stream.  For bsd, this means setup a tcp
 * socket for receiving a connection.
 */
static void *
bsd_stream_server(
    void *	h)
{
    struct sec_stream *bs = NULL;
    struct sec_handle *bh = h;

    assert(bh != NULL);

    bs = alloc(SIZEOF(*bs));
    security_streaminit(&bs->secstr, &bsd_security_driver);
    bs->socket = stream_server(SU_GET_FAMILY(&bh->udp->peer), &bs->port,
			       (size_t)STREAM_BUFSIZE, (size_t)STREAM_BUFSIZE,
			       0);
    if (bs->socket < 0) {
	security_seterror(&bh->sech,
	    _("can't create server stream: %s"), strerror(errno));
	amfree(bs);
	return (NULL);
    }
    bs->fd = -1;
    bs->ev_read = NULL;
    return (bs);
}

/*
 * Accepts a new connection on unconnected streams.  Assumes it is ok to
 * block on accept()
 */
static int
bsd_stream_accept(
    void *	s)
{
    struct sec_stream *bs = s;

    assert(bs != NULL);
    assert(bs->socket != -1);
    assert(bs->fd < 0);

    bs->fd = stream_accept(bs->socket, 30, STREAM_BUFSIZE, STREAM_BUFSIZE);
    if (bs->fd < 0) {
	security_stream_seterror(&bs->secstr,
	    _("can't accept new stream connection: %s"), strerror(errno));
	return (-1);
    }
    return (0);
}

/*
 * Return a connected stream
 */
static void *
bsd_stream_client(
    void *	h,
    int		id)
{
    struct sec_stream *bs = NULL;
    struct sec_handle *bh = h;
#ifdef DUMPER_SOCKET_BUFFERING
    int rcvbuf = SIZEOF(bs->databuf) * 2;
#endif

    assert(bh != NULL);

    bs = alloc(SIZEOF(*bs));
    security_streaminit(&bs->secstr, &bsd_security_driver);
    bs->fd = stream_client(bh->hostname, (in_port_t)id,
	STREAM_BUFSIZE, STREAM_BUFSIZE, &bs->port, 0);
    if (bs->fd < 0) {
	security_seterror(&bh->sech,
	    _("can't connect stream to %s port %d: %s"), bh->hostname,
	    id, strerror(errno));
	amfree(bs);
	return (NULL);
    }
    bs->socket = -1;	/* we're a client */
    bs->ev_read = NULL;
#ifdef DUMPER_SOCKET_BUFFERING
    setsockopt(bs->fd, SOL_SOCKET, SO_RCVBUF, (void *)&rcvbuf, SIZEOF(rcvbuf));
#endif
    return (bs);
}

/*
 * Close and unallocate resources for a stream
 */
static void
bsd_stream_close(
    void *	s)
{
    struct sec_stream *bs = s;

    assert(bs != NULL);

    if (bs->fd != -1)
	aclose(bs->fd);
    if (bs->socket != -1)
	aclose(bs->socket);
    bsd_stream_read_cancel(bs);
    amfree(bs);
}

/*
 * Authenticate a stream.  bsd streams have no authentication
 */
static int
bsd_stream_auth(
    void *	s)
{
    (void)s;		/* Quiet unused parameter warning */

    return (0);	/* success */
}

/*
 * Returns the stream id for this stream.  This is just the local port.
 */
static int
bsd_stream_id(
    void *	s)
{
    struct sec_stream *bs = s;

    assert(bs != NULL);

    return ((int)bs->port);
}

/*
 * Submit a request to read some data.  Calls back with the given function
 * and arg when completed.
 */
static void
bsd_stream_read(
    void *	s,
    void	(*fn)(void *, void *, ssize_t),
    void *	arg)
{
    struct sec_stream *bs = s;

    /*
     * Only one read request can be active per stream.
     */
    if (bs->ev_read != NULL)
	event_release(bs->ev_read);

    bs->ev_read = event_register((event_id_t)bs->fd, EV_READFD, stream_read_callback, bs);
    bs->fn = fn;
    bs->arg = arg;
}

/*
 * Read a chunk of data to a stream.  Blocks until completion.
 */
static ssize_t
bsd_stream_read_sync(
    void *	s,
    void **	buf)
{
    struct sec_stream *bs = s;

    assert(bs != NULL);

    /*
     * Only one read request can be active per stream.
     */
    if(bs->ev_read != NULL) {
        return -1;
    }
    bs->ev_read = event_register((event_id_t)bs->fd, EV_READFD,
			stream_read_sync_callback, bs);
    event_wait(bs->ev_read);
    *buf = bs->databuf;
    return (bs->len);
}


/*
 * Callback for bsd_stream_read_sync
 */
static void
stream_read_sync_callback(
    void *	s)
{
    struct sec_stream *bs = s;
    ssize_t n;

    assert(bs != NULL);

    auth_debug(1, _("bsd: stream_read_callback_sync: fd %d\n"), bs->fd);

    /*
     * Remove the event first, in case they reschedule it in the callback.
     */
    bsd_stream_read_cancel(bs);
    do {
	n = read(bs->fd, bs->databuf, sizeof(bs->databuf));
    } while ((n < 0) && ((errno == EINTR) || (errno == EAGAIN)));
    if (n < 0)
        security_stream_seterror(&bs->secstr, "%s", strerror(errno));
    bs->len = n;
}

/*
 * Cancel a previous stream read request.  It's ok if we didn't
 * have a read scheduled.
 */
static void
bsd_stream_read_cancel(
    void *	s)
{
    struct sec_stream *bs = s;

    assert(bs != NULL);
    if (bs->ev_read != NULL) {
	event_release(bs->ev_read);
	bs->ev_read = NULL;
    }
}

/*
 * Callback for bsd_stream_read
 */
static void
stream_read_callback(
    void *	arg)
{
    struct sec_stream *bs = arg;
    ssize_t n;

    assert(bs != NULL);

    /*
     * Remove the event first, in case they reschedule it in the callback.
     */
    bsd_stream_read_cancel(bs);
    do {
	n = read(bs->fd, bs->databuf, SIZEOF(bs->databuf));
    } while ((n < 0) && ((errno == EINTR) || (errno == EAGAIN)));

    if (n < 0)
	security_stream_seterror(&bs->secstr, "%s", strerror(errno));

    (*bs->fn)(bs->arg, bs->databuf, n);
}
