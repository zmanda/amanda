/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland
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
 * $Id: security-util.c,v 1.25 2006/07/22 12:04:47 martinea Exp $
 *
 * sec-security.c - security and transport over sec or a sec-like command.
 *
 * XXX still need to check for initial keyword on connect so we can skip
 * over shell garbage and other stuff that sec might want to spew out.
 */

#include "amanda.h"
#include "util.h"
#include "event.h"
#include "packet.h"
#include "queue.h"
#include "security.h"
#include "security-util.h"
#include "stream.h"
#include "version.h"

/* #define	SEC_DEBUG */
#define	SHOW_SECURITY_DETAIL

#ifdef SEC_DEBUG
#  define	secprintf(x)	dbprintf(x)
#else
#  ifdef __lint
#    define	secprintf(x)	(void)(x)
#  else
#    define	secprintf(x)
#  endif
#endif

/*
 * Magic values for sec_conn->handle
 */
#define	H_TAKEN	-1		/* sec_conn->tok was already read */
#define	H_EOF	-2		/* this connection has been shut down */

/*
 * This is a queue of open connections
 */
struct connq_s connq = {
    TAILQ_HEAD_INITIALIZER(connq.tailq), 0
};
static int newhandle = 1;
static int newevent = 1;

/*
 * Local functions
 */
static void recvpkt_callback(void *, void *, ssize_t);
static void stream_read_callback(void *);
static void stream_read_sync_callback(void *);

static void sec_tcp_conn_read_cancel(struct tcp_conn *);
static void sec_tcp_conn_read_callback(void *);


/*
 * Authenticate a stream
 * Nothing needed for sec.  The connection is authenticated by secd
 * on startup.
 */
int
sec_stream_auth(
    void *	s)
{
    (void)s;	/* Quiet unused parameter warning */
    return (0);
}

/*
 * Returns the stream id for this stream.  This is just the local
 * port.
 */
int
sec_stream_id(
    void *	s)
{
    struct sec_stream *rs = s;

    assert(rs != NULL);

    return (rs->handle);
}

/*
 * Setup to handle new incoming connections
 */
void
sec_accept(
    const security_driver_t *driver,
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *))
{
    struct tcp_conn *rc;

    rc = sec_tcp_conn_get("unknown",0);
    rc->read = in;
    rc->write = out;
    rc->accept_fn = fn;
    rc->driver = driver;
    sec_tcp_conn_read(rc);
}

/*
 * frees a handle allocated by the above
 */
void
sec_close(
    void *	inst)
{
    struct sec_handle *rh = inst;

    assert(rh != NULL);

    secprintf(("%s: sec: closing handle to %s\n", debug_prefix_time(NULL),
	       rh->hostname));

    if (rh->rs != NULL) {
	/* This may be null if we get here on an error */
	stream_recvpkt_cancel(rh);
	security_stream_close(&rh->rs->secstr);
    }
    /* keep us from getting here again */
    rh->sech.driver = NULL;
    amfree(rh->hostname);
    amfree(rh);
}

/*
 * Called when a sec connection is finished connecting and is ready
 * to be authenticated.
 */
void
sec_connect_callback(
    void *	cookie)
{
    struct sec_handle *rh = cookie;

    event_release(rh->rs->ev_read);
    rh->rs->ev_read = NULL;
    event_release(rh->ev_timeout);
    rh->ev_timeout = NULL;

    (*rh->fn.connect)(rh->arg, &rh->sech, S_OK);
}

/*
 * Called if a connection times out before completion.
 */
void
sec_connect_timeout(
    void *	cookie)
{
    struct sec_handle *rh = cookie;

    event_release(rh->rs->ev_read);
    rh->rs->ev_read = NULL;
    event_release(rh->ev_timeout);
    rh->ev_timeout = NULL;

    (*rh->fn.connect)(rh->arg, &rh->sech, S_TIMEOUT);
}

void
sec_close_connection_none(
    void *h,
    char *hostname)
{
    h = h;
    hostname = hostname;

    return;
}



/*
 * Transmit a packet.
 */
ssize_t
stream_sendpkt(
    void *	cookie,
    pkt_t *	pkt)
{
    char *buf;
    struct sec_handle *rh = cookie;
    size_t len;
    char *s;

    assert(rh != NULL);
    assert(pkt != NULL);

    secprintf(("%s: sec: stream_sendpkt: enter\n", debug_prefix_time(NULL)));

    if (rh->rc->prefix_packet)
	s = rh->rc->prefix_packet(rh, pkt);
    else
	s = "";
    len = strlen(pkt->body) + strlen(s) + 2;
    buf = alloc(len);
    buf[0] = (char)pkt->type;
    strncpy(&buf[1], s, len - 1);
    strncpy(&buf[1 + strlen(s)], pkt->body, (len - strlen(s) - 1));
    if (strlen(s) > 0)
	amfree(s);

    secprintf((
	    "%s: sec: stream_sendpkt: %s (%d) pkt_t (len %d) contains:\n\n\"%s\"\n\n",
	    debug_prefix_time(NULL), pkt_type2str(pkt->type), pkt->type,
	    strlen(pkt->body), pkt->body));

    if (security_stream_write(&rh->rs->secstr, buf, len) < 0) {
	security_seterror(&rh->sech, security_stream_geterror(&rh->rs->secstr));
	return (-1);
    }
    amfree(buf);
    return (0);
}

/*
 * Set up to receive a packet asyncronously, and call back when
 * it has been read.
 */
void
stream_recvpkt(
    void *	cookie,
    void	(*fn)(void *, pkt_t *, security_status_t),
    void *	arg,
    int		timeout)
{
    struct sec_handle *rh = cookie;

    assert(rh != NULL);

    secprintf(("%s: sec: recvpkt registered for %s\n",
	       debug_prefix_time(NULL), rh->hostname));

    /*
     * Reset any pending timeout on this handle
     */
    if (rh->ev_timeout != NULL)
	event_release(rh->ev_timeout);

    /*
     * Negative timeouts mean no timeout
     */
    if (timeout < 0) {
	rh->ev_timeout = NULL;
    } else {
	rh->ev_timeout = event_register((event_id_t)timeout, EV_TIME,
		stream_recvpkt_timeout, rh);
    }
    rh->fn.recvpkt = fn;
    rh->arg = arg;
    security_stream_read(&rh->rs->secstr, recvpkt_callback, rh);
}

/*
 * This is called when a handle times out before receiving a packet.
 */
void
stream_recvpkt_timeout(
    void *	cookie)
{
    struct sec_handle *rh = cookie;

    assert(rh != NULL);

    secprintf(("%s: sec: recvpkt timeout for %s\n",
	       debug_prefix_time(NULL), rh->hostname));

    stream_recvpkt_cancel(rh);
    (*rh->fn.recvpkt)(rh->arg, NULL, S_TIMEOUT);
}

/*
 * Remove a async receive request from the queue
 */
void
stream_recvpkt_cancel(
    void *	cookie)
{
    struct sec_handle *rh = cookie;

    secprintf(("%s: sec: cancelling recvpkt for %s\n",
	       debug_prefix_time(NULL), rh->hostname));

    assert(rh != NULL);

    security_stream_read_cancel(&rh->rs->secstr);
    if (rh->ev_timeout != NULL) {
	event_release(rh->ev_timeout);
	rh->ev_timeout = NULL;
    }
}

/*
 * Write a chunk of data to a stream.  Blocks until completion.
 */
int
tcpm_stream_write(
    void *	s,
    const void *buf,
    size_t	size)
{
    struct sec_stream *rs = s;

    assert(rs != NULL);
    assert(rs->rc != NULL);

    secprintf(("%s: sec: stream_write: writing %d bytes to %s:%d %d\n",
	       debug_prefix_time(NULL), size, rs->rc->hostname, rs->handle,
	       rs->rc->write));

    if (tcpm_send_token(rs->rc->write, rs->handle, &rs->rc->errmsg,
			     buf, size)) {
	security_stream_seterror(&rs->secstr, rs->rc->errmsg);
	return (-1);
    }
    return (0);
}

/*
 * Submit a request to read some data.  Calls back with the given
 * function and arg when completed.
 */
void
tcpm_stream_read(
    void *	s,
    void	(*fn)(void *, void *, ssize_t),
    void *	arg)
{
    struct sec_stream *rs = s;

    assert(rs != NULL);

    /*
     * Only one read request can be active per stream.
     */
    if (rs->ev_read == NULL) {
	rs->ev_read = event_register((event_id_t)rs->rc, EV_WAIT,
	    stream_read_callback, rs);
	sec_tcp_conn_read(rs->rc);
    }
    rs->fn = fn;
    rs->arg = arg;
}

/*
 * Write a chunk of data to a stream.  Blocks until completion.
 */
ssize_t
tcpm_stream_read_sync(
    void *	s,
    void **	buf)
{
    struct sec_stream *rs = s;

    assert(rs != NULL);

    /*
     * Only one read request can be active per stream.
     */
    if (rs->ev_read != NULL) {
	return -1;
    }
    rs->ev_read = event_register((event_id_t)rs->rc, EV_WAIT,
        stream_read_sync_callback, rs);
    sec_tcp_conn_read(rs->rc);
    event_wait(rs->ev_read);
    *buf = rs->rc->pkt;
    return (rs->rc->pktlen);
}

/*
 * Cancel a previous stream read request.  It's ok if we didn't have a read
 * scheduled.
 */
void
tcpm_stream_read_cancel(
    void *	s)
{
    struct sec_stream *rs = s;

    assert(rs != NULL);

    if (rs->ev_read != NULL) {
	event_release(rs->ev_read);
	rs->ev_read = NULL;
	sec_tcp_conn_read_cancel(rs->rc);
    }
}

/*
 * Transmits a chunk of data over a rsh_handle, adding
 * the necessary headers to allow the remote end to decode it.
 */
ssize_t
tcpm_send_token(
    int		fd,
    int		handle,
    char **	errmsg,
    const void *buf,
    size_t	len)
{
    uint32_t		nethandle;
    uint32_t		netlength;
    struct iovec	iov[3];
    int			nb_iov = 3;

    assert(SIZEOF(netlength) == 4);

    /*
     * Format is:
     *   32 bit length (network byte order)
     *   32 bit handle (network byte order)
     *   data
     */
    netlength = htonl(len);
    iov[0].iov_base = (void *)&netlength;
    iov[0].iov_len = SIZEOF(netlength);

    nethandle = htonl((uint32_t)handle);
    iov[1].iov_base = (void *)&nethandle;
    iov[1].iov_len = SIZEOF(nethandle);

    if(len == 0) {
	nb_iov = 2;
    }
    else {
	iov[2].iov_base = (void *)buf;
	iov[2].iov_len = len;
        nb_iov = 3;
    }

    if (net_writev(fd, iov, nb_iov) < 0) {
	if (errmsg)
            *errmsg = newvstralloc(*errmsg, "write error to ",
				   ": ", strerror(errno), NULL);
        return (-1);
    }
    return (0);
}

/*
 *  return -1 on error
 *  return  0 on EOF:   *handle = H_EOF  && *size = 0    if socket closed
 *  return  0 on EOF:   *handle = handle && *size = 0    if stream closed
 *  return size     :   *handle = handle && *size = size for data read
 */

ssize_t
tcpm_recv_token(
    int		fd,
    int *	handle,
    char **	errmsg,
    char **	buf,
    ssize_t *	size,
    int		timeout)
{
    unsigned int netint[2];

    assert(SIZEOF(netint) == 8);

    switch (net_read(fd, &netint, SIZEOF(netint), timeout)) {
    case -1:
	if (errmsg)
	    *errmsg = newvstralloc(*errmsg, "recv error: ", strerror(errno),
				   NULL);
	secprintf(("%s: tcpm_recv_token: A return(-1)\n",
		   debug_prefix_time(NULL)));
	return (-1);
    case 0:
	*size = 0;
	*handle = H_EOF;
	*errmsg = newvstralloc(*errmsg, "SOCKET_EOF", NULL);
	secprintf(("%s: tcpm_recv_token: A return(0)\n",
		   debug_prefix_time(NULL)));
	return (0);
    default:
	break;
    }

    *size = (ssize_t)ntohl(netint[0]);
    if (*size > NETWORK_BLOCK_BYTES) {
	*errmsg = newvstralloc(*errmsg, "tcpm_recv_token: invalid size",
				   NULL);
	dbprintf(("%s: tcpm_recv_token: invalid size %d\n",
		   debug_prefix_time(NULL), *size));
	*size = -1;
	return -1;
    }
    amfree(*buf);
    *buf = alloc((size_t)*size);
    *handle = (int)ntohl(netint[1]);

    if(*size == 0) {
	secprintf(("%s: tcpm_recv_token: read EOF from %d\n",
		   debug_prefix_time(NULL), *handle));
	*errmsg = newvstralloc(*errmsg, "EOF",
				   NULL);
	return 0;
    }
    switch (net_read(fd, *buf, (size_t)*size, timeout)) {
    case -1:
	if (errmsg)
	    *errmsg = newvstralloc(*errmsg, "recv error: ", strerror(errno),
				   NULL);
	secprintf(("%s: tcpm_recv_token: B return(-1)\n",
		   debug_prefix_time(NULL)));
	return (-1);
    case 0:
	*size = 0;
	*errmsg = newvstralloc(*errmsg, "SOCKET_EOF", NULL);
	secprintf(("%s: tcpm_recv_token: B return(0)\n",
		   debug_prefix_time(NULL)));
	return (0);
    default:
	break;
    }

    secprintf(("%s: tcpm_recv_token: read %ld bytes from %d\n",
	       debug_prefix_time(NULL), *size, *handle));
    return((*size));
}

void
tcpm_close_connection(
    void *h,
    char *hostname)
{
    struct sec_handle *rh = h;

    hostname = hostname;

    if(rh->rc->toclose == 0) {
	rh->rc->toclose = 1;
	sec_tcp_conn_put(rh->rc);
    }
}



/*
 * Accept an incoming connection on a stream_server socket
 * Nothing needed for tcpma.
 */
int
tcpma_stream_accept(
    void *	s)
{
    (void)s;	/* Quiet unused parameter warning */

    return (0);
}

/*
 * Return a connected stream.  For sec, this means setup a stream
 * with the supplied handle.
 */
void *
tcpma_stream_client(
    void *	h,
    int		id)
{
    struct sec_handle *rh = h;
    struct sec_stream *rs;

    assert(rh != NULL);

    if (id <= 0) {
	security_seterror(&rh->sech,
	    "%hd: invalid security stream id", id);
	return (NULL);
    }

    rs = alloc(SIZEOF(*rs));
    security_streaminit(&rs->secstr, rh->sech.driver);
    rs->handle = id;
    rs->ev_read = NULL;
    rs->closed_by_me = 0;
    rs->closed_by_network = 0;
    if (rh->rc) {
	rs->rc = rh->rc;
	rh->rc->refcnt++;
    }
    else {
	rs->rc = sec_tcp_conn_get(rh->hostname, 0);
	rs->rc->driver = rh->sech.driver;
	rh->rc = rs->rc;
    }

    secprintf(("%s: sec: stream_client: connected to stream %hd\n",
	       debug_prefix_time(NULL), id));

    return (rs);
}

/*
 * Create the server end of a stream.  For sec, this means setup a stream
 * object and allocate a new handle for it.
 */
void *
tcpma_stream_server(
    void *	h)
{
    struct sec_handle *rh = h;
    struct sec_stream *rs;

    assert(rh != NULL);

    rs = alloc(SIZEOF(*rs));
    security_streaminit(&rs->secstr, rh->sech.driver);
    rs->closed_by_me = 0;
    rs->closed_by_network = 0;
    if (rh->rc) {
	rs->rc = rh->rc;
	rs->rc->refcnt++;
    }
    else {
	rs->rc = sec_tcp_conn_get(rh->hostname, 0);
	rs->rc->driver = rh->sech.driver;
	rh->rc = rs->rc;
    }
    /*
     * Stream should already be setup!
     */
    if (rs->rc->read < 0) {
	sec_tcp_conn_put(rs->rc);
	amfree(rs);
	security_seterror(&rh->sech, "lost connection to %s", rh->hostname);
	return (NULL);
    }
    assert(strcmp(rh->hostname, rs->rc->hostname) == 0);
    //amfree(rh->hostname);
    //rh->hostname = stralloc(rs->rc->hostname);
    /*
     * so as not to conflict with the amanda server's handle numbers,
     * we start at 500000 and work down
     */
    rs->handle = 500000 - newhandle++;
    rs->ev_read = NULL;
    secprintf(("%s: sec: stream_server: created stream %d\n",
	       debug_prefix_time(NULL), rs->handle));
    return (rs);
}

/*
 * Close and unallocate resources for a stream.
 */
void
tcpma_stream_close(
    void *	s)
{
    struct sec_stream *rs = s;
    char buf = 0;

    assert(rs != NULL);

    secprintf(("%s: sec: tcpma_stream_close: closing stream %d\n",
	       debug_prefix_time(NULL), rs->handle));

    if(rs->closed_by_network == 0 && rs->rc->write != -1)
	tcpm_stream_write(rs, &buf, 0);
    security_stream_read_cancel(&rs->secstr);
    if(rs->closed_by_network == 0)
	sec_tcp_conn_put(rs->rc);
    amfree(rs);
}

/*
 * Create the server end of a stream.  For bsdudp, this means setup a tcp
 * socket for receiving a connection.
 */
void *
tcp1_stream_server(
    void *	h)
{
    struct sec_stream *rs = NULL;
    struct sec_handle *rh = h;

    assert(rh != NULL);

    rs = alloc(SIZEOF(*rs));
    security_streaminit(&rs->secstr, rh->sech.driver);
    rs->closed_by_me = 0;
    rs->closed_by_network = 0;
    if (rh->rc) {
	rs->rc = rh->rc;
	rs->handle = 500000 - newhandle++;
	rs->rc->refcnt++;
	rs->socket = 0;		/* the socket is already opened */
    }
    else {
	rh->rc = sec_tcp_conn_get(rh->hostname, 1);
	rh->rc->driver = rh->sech.driver;
	rs->rc = rh->rc;
	rs->socket = stream_server(&rs->port, STREAM_BUFSIZE, 
		STREAM_BUFSIZE, 0);
	if (rs->socket < 0) {
	    security_seterror(&rh->sech,
			    "can't create server stream: %s", strerror(errno));
	    amfree(rs);
	    return (NULL);
	}
	rh->rc->read = rs->socket;
	rh->rc->write = rs->socket;
	rs->handle = (int)rs->port;
    }
    rs->fd = -1;
    rs->ev_read = NULL;
    return (rs);
}

/*
 * Accepts a new connection on unconnected streams.  Assumes it is ok to
 * block on accept()
 */
int
tcp1_stream_accept(
    void *	s)
{
    struct sec_stream *bs = s;

    assert(bs != NULL);
    assert(bs->socket != -1);
    assert(bs->fd < 0);

    if (bs->socket > 0) {
	bs->fd = stream_accept(bs->socket, 30, STREAM_BUFSIZE, STREAM_BUFSIZE);
	if (bs->fd < 0) {
	    security_stream_seterror(&bs->secstr,
				     "can't accept new stream connection: %s",
				     strerror(errno));
	    return (-1);
	}
	bs->rc->read = bs->fd;
	bs->rc->write = bs->fd;
    }
    return (0);
}

/*
 * Return a connected stream
 */
void *
tcp1_stream_client(
    void *	h,
    int		id)
{
    struct sec_stream *rs = NULL;
    struct sec_handle *rh = h;

    assert(rh != NULL);

    rs = alloc(SIZEOF(*rs));
    security_streaminit(&rs->secstr, rh->sech.driver);
    rs->handle = id;
    rs->ev_read = NULL;
    rs->closed_by_me = 0;
    rs->closed_by_network = 0;
    if (rh->rc) {
	rs->rc = rh->rc;
	rh->rc->refcnt++;
    }
    else {
	rh->rc = sec_tcp_conn_get(rh->hostname, 1);
	rs->rc = rh->rc;
	rh->rc->read = stream_client(rh->hostname, (in_port_t)id,
			STREAM_BUFSIZE, STREAM_BUFSIZE, &rs->port, 0);
	if (rh->rc->read < 0) {
	    security_seterror(&rh->sech,
			      "can't connect stream to %s port %d: %s",
			       rh->hostname, id, strerror(errno));
	    amfree(rs);
	    return (NULL);
        }
	rh->rc->write = rh->rc->read;
    }
    rs->socket = -1;	/* we're a client */
    rh->rs = rs;
    return (rs);
}

int
tcp_stream_write(
    void *	s,
    const void *buf,
    size_t	size)
{
    struct sec_stream *rs = s;

    assert(rs != NULL);

    if (fullwrite(rs->fd, buf, size) < 0) {
        security_stream_seterror(&rs->secstr,
            "write error on stream %d: %s", rs->port, strerror(errno));
        return (-1);
    }
    return (0);
}

char *
bsd_prefix_packet(
    void *	h,
    pkt_t *	pkt)
{
    struct sec_handle *rh = h;
    struct passwd *pwd;
    char *buf;

    if (pkt->type != P_REQ)
	return "";

    if ((pwd = getpwuid(getuid())) == NULL) {
	security_seterror(&rh->sech,
			  "can't get login name for my uid %ld",
			  (long)getuid());
	return(NULL);
    }
    buf = alloc(16+strlen(pwd->pw_name));
    strncpy(buf, "SECURITY USER ", (16 + strlen(pwd->pw_name)));
    strncpy(&buf[14], pwd->pw_name, (16 + strlen(pwd->pw_name) - 14));
    buf[14 + strlen(pwd->pw_name)] = '\n';
    buf[15 + strlen(pwd->pw_name)] = '\0';

    return (buf);
}


/*
 * Check the security of a received packet.  Returns negative on security
 * violation, or returns 0 if ok.  Removes the security info from the pkt_t.
 */
int
bsd_recv_security_ok(
    struct sec_handle *	rh,
    pkt_t *		pkt)
{
    char *tok, *security, *body, *result;
    char *service = NULL, *serviceX, *serviceY;
    char *security_line;
    size_t len;

    /*
     * Now, find the SECURITY line in the body, and parse it out
     * into an argv.
     */
    if (strncmp(pkt->body, "SECURITY ", SIZEOF("SECURITY ") - 1) == 0) {
	security = pkt->body;
	len = 0;
	while(*security != '\n' && len < pkt->size) {
	    security++;
	    len++;
	}
	if(*security == '\n') {
	    body = security+1;
	    *security = '\0';
	    security_line = stralloc(pkt->body);
	    security = pkt->body + strlen("SECURITY ");
	} else {
	    body = pkt->body;
	    security_line = NULL;
	    security = NULL;
	}
    } else {
	body = pkt->body;
	security_line = NULL;
	security = NULL;
    }

    /*
     * Now, find the SERVICE line in the body, and parse it out
     * into an argv.
     */
    if (strncmp(body, "SERVICE", SIZEOF("SERVICE") - 1) == 0) {
	serviceX = stralloc(body + strlen("SERVICE "));
	serviceY = strtok(serviceX, "\n");
	if (serviceY)
	    service  = stralloc(serviceY);
	amfree(serviceX);
    }

    /*
     * We need to do different things depending on which type of packet
     * this is.
     */
    switch (pkt->type) {
    case P_REQ:
	/*
	 * Request packets must come from a reserved port
	 */
	if (ntohs(rh->peer.sin_port) >= IPPORT_RESERVED) {
	    security_seterror(&rh->sech,
		"host %s: port %d not secure", rh->hostname,
		ntohs(rh->peer.sin_port));
	    amfree(service);
	    amfree(security_line);
	    return (-1);
	}

	if (!service) {
	    security_seterror(&rh->sech,
			      "packet as no SERVICE line");
	    amfree(security_line);
	    return (-1);
	}

	/*
	 * Request packets contain a remote username.  We need to check
	 * that we allow it in.
	 *
	 * They will look like:
	 *	SECURITY USER [username]
	 */

	/* there must be some security info */
	if (security == NULL) {
	    security_seterror(&rh->sech,
		"no bsd SECURITY for P_REQ");
	    amfree(service);
	    return (-1);
	}

	/* second word must be USER */
	if ((tok = strtok(security, " ")) == NULL) {
	    security_seterror(&rh->sech,
		"SECURITY line: %s", security_line);
	    amfree(service);
	    amfree(security_line);
	    return (-1);	/* default errmsg */
	}
	if (strcmp(tok, "USER") != 0) {
	    security_seterror(&rh->sech,
		"REQ SECURITY line parse error, expecting USER, got %s", tok);
	    amfree(service);
	    amfree(security_line);
	    return (-1);
	}

	/* the third word is the username */
	if ((tok = strtok(NULL, "")) == NULL) {
	    security_seterror(&rh->sech,
		"SECURITY line: %s", security_line);
	    amfree(security_line);
	    return (-1);	/* default errmsg */
	}
	if ((result = check_user(rh, tok, service)) != NULL) {
	    security_seterror(&rh->sech, "%s", result);
	    amfree(service);
	    amfree(result);
	    amfree(security_line);
	    return (-1);
	}

	/* we're good to go */
	break;
    default:
	break;
    }
    amfree(service);
    amfree(security_line);

    /*
     * If there is security info at the front of the packet, we need to
     * shift the rest of the data up and nuke it.
     */
    if (body != pkt->body)
	memmove(pkt->body, body, strlen(body) + 1);
    return (0);
}

/*
 * Transmit a packet.  Add security information first.
 */
ssize_t
udpbsd_sendpkt(
    void *	cookie,
    pkt_t *	pkt)
{
    struct sec_handle *rh = cookie;
    struct passwd *pwd;

    assert(rh != NULL);
    assert(pkt != NULL);

    secprintf(("%s: udpbsd_sendpkt: enter\n", get_pname()));
    /*
     * Initialize this datagram, and add the header
     */
    dgram_zero(&rh->udp->dgram);
    dgram_cat(&rh->udp->dgram, pkthdr2str(rh, pkt));

    /*
     * Add the security info.  This depends on which kind of packet we're
     * sending.
     */
    switch (pkt->type) {
    case P_REQ:
	/*
	 * Requests get sent with our username in the body
	 */
	if ((pwd = getpwuid(geteuid())) == NULL) {
	    security_seterror(&rh->sech,
		"can't get login name for my uid %ld", (long)getuid());
	    return (-1);
	}
	dgram_cat(&rh->udp->dgram, "SECURITY USER %s\n", pwd->pw_name);
	break;

    default:
	break;
    }

    /*
     * Add the body, and send it
     */
    dgram_cat(&rh->udp->dgram, pkt->body);

    secprintf((
	    "%s: sec: udpbsd_sendpkt: %s (%d) pkt_t (len %d) contains:\n\n\"%s\"\n\n",
	    debug_prefix_time(NULL), pkt_type2str(pkt->type), pkt->type,
	    strlen(pkt->body), pkt->body));

    if (dgram_send_addr(rh->peer, &rh->udp->dgram) != 0) {
	security_seterror(&rh->sech,
	    "send %s to %s failed: %s", pkt_type2str(pkt->type),
	    rh->hostname, strerror(errno));
	return (-1);
    }
    return (0);
}

void
udp_close(
    void *	cookie)
{
    struct sec_handle *rh = cookie;

    if (rh->proto_handle == NULL) {
	return;
    }

    secprintf(("%s: udp: close handle '%s'\n",
	       debug_prefix_time(NULL), rh->proto_handle));

    udp_recvpkt_cancel(rh);
    if (rh->next) {
	rh->next->prev = rh->prev;
    }
    else {
	rh->udp->bh_last = rh->prev;
    }
    if (rh->prev) {
	rh->prev->next = rh->next;
    }
    else {
	rh->udp->bh_first = rh->next;
    }

    amfree(rh->proto_handle);
    amfree(rh->hostname);
    amfree(rh);
}

/*
 * Set up to receive a packet asynchronously, and call back when it has
 * been read.
 */
void
udp_recvpkt(
    void *	cookie,
    void	(*fn)(void *, pkt_t *, security_status_t),
    void *	arg,
    int		timeout)
{
    struct sec_handle *rh = cookie;

    secprintf(("%s: udp_recvpkt(cookie=%p, fn=%p, arg=%p, timeout=%u)\n",
	debug_prefix(NULL), cookie, fn, arg, timeout));
    assert(rh != NULL);
    assert(fn != NULL);


    /*
     * Subsequent recvpkt calls override previous ones
     */
    if (rh->ev_read == NULL) {
	udp_addref(rh->udp, &udp_netfd_read_callback);
	rh->ev_read = event_register(rh->event_id, EV_WAIT,
	    udp_recvpkt_callback, rh);
    }
    if (rh->ev_timeout != NULL)
	event_release(rh->ev_timeout);
    if (timeout < 0)
	rh->ev_timeout = NULL;
    else
	rh->ev_timeout = event_register((event_id_t)timeout, EV_TIME,
					udp_recvpkt_timeout, rh);
    rh->fn.recvpkt = fn;
    rh->arg = arg;
}

/*
 * Remove a async receive request on this handle from the queue.
 * If it is the last one to be removed, then remove the event
 * handler for our network fd
 */
void
udp_recvpkt_cancel(
    void *	cookie)
{
    struct sec_handle *rh = cookie;

    assert(rh != NULL);

    if (rh->ev_read != NULL) {
	udp_delref(rh->udp);
	event_release(rh->ev_read);
	rh->ev_read = NULL;
    }

    if (rh->ev_timeout != NULL) {
	event_release(rh->ev_timeout);
	rh->ev_timeout = NULL;
    }
}

/*
 * This is called when a handle is woken up because data read off of the
 * net is for it.
 */
void
udp_recvpkt_callback(
    void *	cookie)
{
    struct sec_handle *rh = cookie;
    void (*fn)(void *, pkt_t *, security_status_t);
    void *arg;

    secprintf(("%s: udp: receive handle '%s' netfd '%s'\n",
	       debug_prefix_time(NULL), rh->proto_handle, rh->udp->handle));
    assert(rh != NULL);

    if (strcmp(rh->proto_handle, rh->udp->handle) != 0) assert(1);
    /* if it didn't come from the same host/port, forget it */
    if (memcmp(&rh->peer.sin_addr, &rh->udp->peer.sin_addr,
	SIZEOF(rh->udp->peer.sin_addr)) != 0 ||
	rh->peer.sin_port != rh->udp->peer.sin_port) {
	amfree(rh->udp->handle);
	//rh->udp->handle = NULL;
	return;
    }

    /*
     * We need to cancel the recvpkt request before calling the callback
     * because the callback may reschedule us.
     */
    fn = rh->fn.recvpkt;
    arg = rh->arg;
    udp_recvpkt_cancel(rh);

    /*
     * Check the security of the packet.  If it is bad, then pass NULL
     * to the packet handling function instead of a packet.
     */
    if (rh->udp->recv_security_ok &&
	rh->udp->recv_security_ok(rh, &rh->udp->pkt) < 0)
	(*fn)(arg, NULL, S_ERROR);
    else
	(*fn)(arg, &rh->udp->pkt, S_OK);
}

/*
 * This is called when a handle times out before receiving a packet.
 */
void
udp_recvpkt_timeout(
    void *	cookie)
{
    struct sec_handle *rh = cookie;
    void (*fn)(void *, pkt_t *, security_status_t);
    void *arg;

    assert(rh != NULL);

    assert(rh->ev_timeout != NULL);
    fn = rh->fn.recvpkt;
    arg = rh->arg;
    udp_recvpkt_cancel(rh);
    (*fn)(arg, NULL, S_TIMEOUT);
}

/*
 * Given a hostname and a port, setup a udp_handle
 */
int
udp_inithandle(
    udp_handle_t *	udp,
    struct sec_handle *	rh,
    struct hostent *	he,
    in_port_t		port,
    char *		handle,
    int			sequence)
{
    int i;

    /*
     * Save the hostname and port info
     */
    secprintf(("%s: udp_inithandle port %hd handle %s sequence %d\n",
	       debug_prefix_time(NULL), ntohs(port),
	       handle, sequence));
    assert(he != NULL);

    rh->hostname = stralloc(he->h_name);
    memcpy(&rh->peer.sin_addr, he->h_addr, SIZEOF(rh->peer.sin_addr));
    rh->peer.sin_port = port;
    rh->peer.sin_family = (sa_family_t)AF_INET;

    /*
     * Do a forward lookup of the hostname.  This is unnecessary if we
     * are initiating the connection, but is very serious if we are
     * receiving.  We want to make sure the hostname
     * resolves back to the remote ip for security reasons.
     */
    if ((he = gethostbyname(rh->hostname)) == NULL) {
    secprintf(("%s: udp: bb\n", debug_prefix_time(NULL)));
	security_seterror(&rh->sech,
	    "%s: could not resolve hostname", rh->hostname);
	return (-1);
    }

    /*
     * Make sure the hostname matches.  This should always work.
     */
    if (strncasecmp(rh->hostname, he->h_name, strlen(rh->hostname)) != 0) {
    secprintf(("%s: udp: cc\n", debug_prefix_time(NULL)));
	security_seterror(&rh->sech,
			  "%s: did not resolve to itself, it resolv to",
			  rh->hostname, he->h_name);
	return (-1);
    }

    /*
     * Now look for a matching ip address.
     */
    for (i = 0; he->h_addr_list[i] != NULL; i++) {
	if (memcmp(&rh->peer.sin_addr, he->h_addr_list[i],
	    SIZEOF(struct in_addr)) == 0) {
	    break;
	}
    }

    /*
     * If we didn't find it, try the aliases.  This is a workaround for
     * Solaris if DNS goes over NIS.
     */
    if (he->h_addr_list[i] == NULL) {
	const char *ipstr = inet_ntoa(rh->peer.sin_addr);
	for (i = 0; he->h_aliases[i] != NULL; i++) {
	    if (strcmp(he->h_aliases[i], ipstr) == 0)
		break;
	}
	/*
	 * No aliases either.  Failure.  Someone is fooling with us or
	 * DNS is messed up.
	 */
	if (he->h_aliases[i] == NULL) {
	    security_seterror(&rh->sech,
		"DNS check failed: no matching ip address for %s",
		rh->hostname);
	    return (-1);
	}
    }

    rh->prev = udp->bh_last;
    if (udp->bh_last) {
	rh->prev->next = rh;
    }
    if (!udp->bh_first) {
	udp->bh_first = rh;
    }
    rh->next = NULL;
    udp->bh_last = rh;

    rh->sequence = sequence;
    rh->event_id = (event_id_t)newevent++;
    amfree(rh->proto_handle);
    rh->proto_handle = stralloc(handle);
    rh->fn.connect = NULL;
    rh->arg = NULL;
    rh->ev_read = NULL;
    rh->ev_timeout = NULL;

    secprintf(("%s: udp: adding handle '%s'\n",
	       debug_prefix_time(NULL), rh->proto_handle));

    return(0);
}


/*
 * Callback for received packets.  This is the function bsd_recvpkt
 * registers with the event handler.  It is called when the event handler
 * realizes that data is waiting to be read on the network socket.
 */
void
udp_netfd_read_callback(
    void *	cookie)
{
    struct udp_handle *udp = cookie;
    struct sec_handle *rh;
    struct hostent *he;
    int a;

    secprintf(("%s: udp_netfd_read_callback(cookie=%p)\n",
		debug_prefix(NULL), cookie));
    assert(udp != NULL);
    
#ifndef TEST							/* { */
    /*
     * Receive the packet.
     */
    dgram_zero(&udp->dgram);
    if (dgram_recv(&udp->dgram, 0, &udp->peer) < 0)
	return;
#endif /* !TEST */						/* } */

    /*
     * Parse the packet.
     */
    if (str2pkthdr(udp) < 0)
	return;

    /*
     * If there are events waiting on this handle, we're done
     */
    rh = udp->bh_first;
    while(rh != NULL && (strcmp(rh->proto_handle, udp->handle) != 0 ||
			 rh->sequence != udp->sequence ||
			 rh->peer.sin_addr.s_addr != udp->peer.sin_addr.s_addr ||
			 rh->peer.sin_port != udp->peer.sin_port)) {
	rh = rh->next;
    }
    if (rh && event_wakeup(rh->event_id) > 0)
	return;

    /*
     * If we didn't find a handle, then check for a new incoming packet.
     * If no accept handler was setup, then just return.
     */
    if (udp->accept_fn == NULL)
	return;

    he = gethostbyaddr((void *)&udp->peer.sin_addr,
	(socklen_t)sizeof(udp->peer.sin_addr), AF_INET);
    if (he == NULL)
	return;
    rh = alloc(SIZEOF(*rh));
    rh->proto_handle=NULL;
    rh->udp = udp;
    rh->rc = NULL;
    security_handleinit(&rh->sech, udp->driver);
    a = udp_inithandle(udp, rh,
		   he,
		   udp->peer.sin_port,
		   udp->handle,
		   udp->sequence);
    if (a < 0) {
	secprintf(("%s: bsd: closeX handle '%s'\n",
		  debug_prefix_time(NULL), rh->proto_handle));

	amfree(rh);
	return;
    }
    /*
     * Check the security of the packet.  If it is bad, then pass NULL
     * to the accept function instead of a packet.
     */
    if (rh->udp->recv_security_ok(rh, &udp->pkt) < 0)
	(*udp->accept_fn)(&rh->sech, NULL);
    else
	(*udp->accept_fn)(&rh->sech, &udp->pkt);
}

/*
 * Locate an existing connection to the given host, or create a new,
 * unconnected entry if none exists.  The caller is expected to check
 * for the lack of a connection (rc->read == -1) and set one up.
 */
struct tcp_conn *
sec_tcp_conn_get(
    const char *hostname,
    int		want_new)
{
    struct tcp_conn *rc;

    secprintf(("%s: sec_tcp_conn_get: %s\n", debug_prefix_time(NULL), hostname));

    if (want_new == 0) {
    for (rc = connq_first(); rc != NULL; rc = connq_next(rc)) {
	if (strcasecmp(hostname, rc->hostname) == 0)
	    break;
    }

    if (rc != NULL) {
	rc->refcnt++;
	secprintf(("%s: sec_tcp_conn_get: exists, refcnt to %s is now %d\n",
		   debug_prefix_time(NULL),
		   rc->hostname, rc->refcnt));
	return (rc);
    }
    }

    secprintf(("%s: sec_tcp_conn_get: creating new handle\n",
	       debug_prefix_time(NULL)));
    /*
     * We can't be creating a new handle if we are the client
     */
    rc = alloc(SIZEOF(*rc));
    rc->read = rc->write = -1;
    rc->driver = NULL;
    rc->pid = -1;
    rc->ev_read = NULL;
    rc->toclose = 0;
    rc->donotclose = 0;
    strncpy(rc->hostname, hostname, SIZEOF(rc->hostname) - 1);
    rc->hostname[SIZEOF(rc->hostname) - 1] = '\0';
    rc->errmsg = NULL;
    rc->refcnt = 1;
    rc->handle = -1;
    rc->pkt = NULL;
    rc->accept_fn = NULL;
    rc->recv_security_ok = NULL;
    rc->prefix_packet = NULL;
    connq_append(rc);
    return (rc);
}

/*
 * Delete a reference to a connection, and close it if it is the last
 * reference.
 */
void
sec_tcp_conn_put(
    struct tcp_conn *	rc)
{
    amwait_t status;

    assert(rc->refcnt > 0);
    --rc->refcnt;
    secprintf(("%s: sec_tcp_conn_put: decrementing refcnt for %s to %d\n",
	       debug_prefix_time(NULL),
	rc->hostname, rc->refcnt));
    if (rc->refcnt > 0) {
	return;
    }
    secprintf(("%s: sec_tcp_conn_put: closing connection to %s\n",
	       debug_prefix_time(NULL), rc->hostname));
    if (rc->read != -1)
	aclose(rc->read);
    if (rc->write != -1)
	aclose(rc->write);
    if (rc->pid != -1) {
	waitpid(rc->pid, &status, WNOHANG);
    }
    if (rc->ev_read != NULL)
	event_release(rc->ev_read);
    if (rc->errmsg != NULL)
	amfree(rc->errmsg);
    connq_remove(rc);
    amfree(rc->pkt);
    if(!rc->donotclose)
	amfree(rc); /* someone might still use it           */
		    /* eg. in sec_tcp_conn_read_callback if */
		    /*     event_wakeup call us.            */
}

/*
 * Turn on read events for a conn.  Or, increase a ev_read_refcnt if we are
 * already receiving read events.
 */
void
sec_tcp_conn_read(
    struct tcp_conn *	rc)
{
    assert (rc != NULL);

    if (rc->ev_read != NULL) {
	rc->ev_read_refcnt++;
	secprintf((
	       "%s: sec: conn_read: incremented ev_read_refcnt to %d for %s\n",
	       debug_prefix_time(NULL), rc->ev_read_refcnt, rc->hostname));
	return;
    }
    secprintf(("%s: sec: conn_read registering event handler for %s\n",
	       debug_prefix_time(NULL), rc->hostname));
    rc->ev_read = event_register((event_id_t)rc->read, EV_READFD,
		sec_tcp_conn_read_callback, rc);
    rc->ev_read_refcnt = 1;
}

static void
sec_tcp_conn_read_cancel(
    struct tcp_conn *	rc)
{

    --rc->ev_read_refcnt;
    secprintf((
	"%s: sec: conn_read_cancel: decremented ev_read_refcnt to %d for %s\n",
	debug_prefix_time(NULL),
	rc->ev_read_refcnt, rc->hostname));
    if (rc->ev_read_refcnt > 0) {
	return;
    }
    secprintf(("%s: sec: conn_read_cancel: releasing event handler for %s\n",
	       debug_prefix_time(NULL), rc->hostname));
    event_release(rc->ev_read);
    rc->ev_read = NULL;
}

/*
 * This is called when a handle is woken up because data read off of the
 * net is for it.
 */
static void
recvpkt_callback(
    void *	cookie,
    void *	buf,
    ssize_t	bufsize)
{
    pkt_t pkt;
    struct sec_handle *rh = cookie;

    assert(rh != NULL);

    secprintf(("%s: sec: recvpkt_callback: %d\n",
	       debug_prefix_time(NULL), bufsize));
    /*
     * We need to cancel the recvpkt request before calling
     * the callback because the callback may reschedule us.
     */
    stream_recvpkt_cancel(rh);

    switch (bufsize) {
    case 0:
	security_seterror(&rh->sech,
	    "EOF on read from %s", rh->hostname);
	(*rh->fn.recvpkt)(rh->arg, NULL, S_ERROR);
	return;
    case -1:
	security_seterror(&rh->sech, security_stream_geterror(&rh->rs->secstr));
	(*rh->fn.recvpkt)(rh->arg, NULL, S_ERROR);
	return;
    default:
	break;
    }

    parse_pkt(&pkt, buf, (size_t)bufsize);
    secprintf((
	   "%s: sec: received %s packet (%d) from %s, contains:\n\n\"%s\"\n\n",
	   debug_prefix_time(NULL), pkt_type2str(pkt.type), pkt.type,
	   rh->hostname, pkt.body));
    if (rh->rc->recv_security_ok && (rh->rc->recv_security_ok)(rh, &pkt) < 0)
	(*rh->fn.recvpkt)(rh->arg, NULL, S_ERROR);
    else
	(*rh->fn.recvpkt)(rh->arg, &pkt, S_OK);
    amfree(pkt.body);
}

/*
 * Callback for tcpm_stream_read_sync
 */
static void
stream_read_sync_callback(
    void *	s)
{
    struct sec_stream *rs = s;
    assert(rs != NULL);

    secprintf(("%s: sec: stream_read_callback_sync: handle %d\n",
	       debug_prefix_time(NULL), rs->handle));

    /*
     * Make sure this was for us.  If it was, then blow away the handle
     * so it doesn't get claimed twice.  Otherwise, leave it alone.
     *
     * If the handle is EOF, pass that up to our callback.
     */
    if (rs->rc->handle == rs->handle) {
        secprintf(("%s: sec: stream_read_callback_sync: it was for us\n",
		   debug_prefix_time(NULL)));
        rs->rc->handle = H_TAKEN;
    } else if (rs->rc->handle != H_EOF) {
        secprintf(("%s: sec: stream_read_callback_sync: not for us\n",
		   debug_prefix_time(NULL)));
        return;
    }

    /*
     * Remove the event first, and then call the callback.
     * We remove it first because we don't want to get in their
     * way if they reschedule it.
     */
    tcpm_stream_read_cancel(rs);

    if (rs->rc->pktlen <= 0) {
	secprintf(("%s: sec: stream_read_sync_callback: %s\n",
		   debug_prefix_time(NULL), rs->rc->errmsg));
	security_stream_seterror(&rs->secstr, rs->rc->errmsg);
	if(rs->closed_by_me == 0 && rs->closed_by_network == 0)
	    sec_tcp_conn_put(rs->rc);
	rs->closed_by_network = 1;
	return;
    }
    secprintf((
	     "%s: sec: stream_read_callback_sync: read %ld bytes from %s:%d\n",
	     debug_prefix_time(NULL),
        rs->rc->pktlen, rs->rc->hostname, rs->handle));
}

/*
 * Callback for tcpm_stream_read
 */
static void
stream_read_callback(
    void *	arg)
{
    struct sec_stream *rs = arg;
    assert(rs != NULL);

    secprintf(("%s: sec: stream_read_callback: handle %d\n",
	       debug_prefix_time(NULL), rs->handle));

    /*
     * Make sure this was for us.  If it was, then blow away the handle
     * so it doesn't get claimed twice.  Otherwise, leave it alone.
     *
     * If the handle is EOF, pass that up to our callback.
     */
    if (rs->rc->handle == rs->handle) {
	secprintf(("%s: sec: stream_read_callback: it was for us\n",
		   debug_prefix_time(NULL)));
	rs->rc->handle = H_TAKEN;
    } else if (rs->rc->handle != H_EOF) {
	secprintf(("%s: sec: stream_read_callback: not for us\n",
		   debug_prefix_time(NULL)));
	return;
    }

    /*
     * Remove the event first, and then call the callback.
     * We remove it first because we don't want to get in their
     * way if they reschedule it.
     */
    tcpm_stream_read_cancel(rs);

    if (rs->rc->pktlen <= 0) {
	secprintf(("%s: sec: stream_read_callback: %s\n",
		   debug_prefix_time(NULL), rs->rc->errmsg));
	security_stream_seterror(&rs->secstr, rs->rc->errmsg);
	if(rs->closed_by_me == 0 && rs->closed_by_network == 0)
	    sec_tcp_conn_put(rs->rc);
	rs->closed_by_network = 1;
	(*rs->fn)(rs->arg, NULL, rs->rc->pktlen);
	return;
    }
    secprintf(("%s: sec: stream_read_callback: read %ld bytes from %s:%d\n",
	       debug_prefix_time(NULL),
	rs->rc->pktlen, rs->rc->hostname, rs->handle));
    (*rs->fn)(rs->arg, rs->rc->pkt, rs->rc->pktlen);
    secprintf(("%s: sec: after callback stream_read_callback\n",
	       debug_prefix_time(NULL)));
}

/*
 * The callback for the netfd for the event handler
 * Determines if this packet is for this security handle,
 * and does the real callback if so.
 */
static void
sec_tcp_conn_read_callback(
    void *	cookie)
{
    struct tcp_conn *	rc = cookie;
    struct sec_handle *	rh;
    pkt_t		pkt;
    ssize_t		rval;
    int			revent;

    assert(cookie != NULL);

    secprintf(("%s: sec: conn_read_callback\n", debug_prefix_time(NULL)));

    /* Read the data off the wire.  If we get errors, shut down. */
    rval = tcpm_recv_token(rc->read, &rc->handle, &rc->errmsg, &rc->pkt,
				&rc->pktlen, 60);
    secprintf(("%s: sec: conn_read_callback: tcpm_recv_token returned %d\n",
	       debug_prefix_time(NULL), rval));
    if (rval < 0 || rc->handle == H_EOF) {
	rc->pktlen = rval;
	rc->handle = H_EOF;
	revent = event_wakeup((event_id_t)rc);
	secprintf(("%s: sec: conn_read_callback: event_wakeup return %d\n",
		   debug_prefix_time(NULL), revent));
	/* delete our 'accept' reference */
	if (rc->accept_fn != NULL) {
	    if(rc->refcnt != 1) {
		dbprintf(("STRANGE, rc->refcnt should be 1"));
		rc->refcnt=1;
	    }
	    rc->accept_fn = NULL;
	    sec_tcp_conn_put(rc);
	}
	return;
    }

    if(rval == 0) {
	rc->pktlen = 0;
	revent = event_wakeup((event_id_t)rc);
	secprintf(("%s: 0 sec: conn_read_callback: event_wakeup return %d\n",
		   debug_prefix_time(NULL), revent));
	return;
    }

    /* If there are events waiting on this handle, we're done */
    rc->donotclose = 1;
    revent = event_wakeup((event_id_t)rc);
    secprintf(("%s: sec: conn_read_callback: event_wakeup return %d\n",
	       debug_prefix_time(NULL), rval));
    rc->donotclose = 0;
    if (rc->handle == H_TAKEN || rc->pktlen == 0) {
	if(rc->refcnt == 0) amfree(rc);
	return;
    }

    assert(rc->refcnt > 0);

    /* If there is no accept fn registered, then drop the packet */
    if (rc->accept_fn == NULL)
	return;

    rh = alloc(SIZEOF(*rh));
    security_handleinit(&rh->sech, rc->driver);
    rh->hostname = stralloc(rc->hostname);
    rh->ev_timeout = NULL;
    rh->rc = rc;
    rh->peer = rc->peer;
    rh->rs = tcpma_stream_client(rh, rc->handle);

    secprintf(("%s: sec: new connection\n", debug_prefix_time(NULL)));
    pkt.body = NULL;
    parse_pkt(&pkt, rc->pkt, (size_t)rc->pktlen);
    secprintf(("%s: sec: calling accept_fn\n", debug_prefix_time(NULL)));
    if (rh->rc->recv_security_ok && (rh->rc->recv_security_ok)(rh, &pkt) < 0)
	(*rc->accept_fn)(&rh->sech, NULL);
    else
	(*rc->accept_fn)(&rh->sech, &pkt);
    amfree(pkt.body);
}

void
parse_pkt(
    pkt_t *	pkt,
    const void *buf,
    size_t	bufsize)
{
    const unsigned char *bufp = buf;

    secprintf(("%s: sec: parse_pkt: parsing buffer of %d bytes\n",
	       debug_prefix_time(NULL), bufsize));

    pkt->type = (pktype_t)*bufp++;
    bufsize--;

    pkt->packet_size = bufsize+1;
    pkt->body = alloc(pkt->packet_size);
    if (bufsize == 0) {
	pkt->body[0] = '\0';
    } else {
	memcpy(pkt->body, bufp, bufsize);
	pkt->body[pkt->packet_size - 1] = '\0';
    }
    pkt->size = strlen(pkt->body);

    secprintf(("%s: sec: parse_pkt: %s (%d): \"%s\"\n",
	       debug_prefix_time(NULL), pkt_type2str(pkt->type),
	       pkt->type, pkt->body));
}

/*
 * Convert a packet header into a string
 */
const char *
pkthdr2str(
    const struct sec_handle *	rh,
    const pkt_t *		pkt)
{
    static char retbuf[256];

    assert(rh != NULL);
    assert(pkt != NULL);

    snprintf(retbuf, SIZEOF(retbuf), "Amanda %d.%d %s HANDLE %s SEQ %d\n",
	VERSION_MAJOR, VERSION_MINOR, pkt_type2str(pkt->type),
	rh->proto_handle, rh->sequence);

    secprintf(("%s: bsd: pkthdr2str handle '%s'\n",
	       debug_prefix_time(NULL), rh->proto_handle));

    /* check for truncation.  If only we had asprintf()... */
    assert(retbuf[strlen(retbuf) - 1] == '\n');

    return (retbuf);
}

/*
 * Parses out the header line in 'str' into the pkt and handle
 * Returns negative on parse error.
 */
int
str2pkthdr(
    udp_handle_t *	udp)
{
    char *str;
    const char *tok;
    pkt_t *pkt;

    pkt = &udp->pkt;

    assert(udp->dgram.cur != NULL);
    str = stralloc(udp->dgram.cur);

    /* "Amanda %d.%d <ACK,NAK,...> HANDLE %s SEQ %d\n" */

    /* Read in "Amanda" */
    if ((tok = strtok(str, " ")) == NULL || strcmp(tok, "Amanda") != 0)
	goto parse_error;

    /* nothing is done with the major/minor numbers currently */
    if ((tok = strtok(NULL, " ")) == NULL || strchr(tok, '.') == NULL)
	goto parse_error;

    /* Read in the packet type */
    if ((tok = strtok(NULL, " ")) == NULL)
	goto parse_error;
    amfree(pkt->body);
    pkt_init(pkt, pkt_str2type(tok), "");
    if (pkt->type == (pktype_t)-1)    
	goto parse_error;

    /* Read in "HANDLE" */
    if ((tok = strtok(NULL, " ")) == NULL || strcmp(tok, "HANDLE") != 0)
	goto parse_error;

    /* parse the handle */
    if ((tok = strtok(NULL, " ")) == NULL)
	goto parse_error;
    amfree(udp->handle);
    udp->handle = stralloc(tok);

    /* Read in "SEQ" */
    if ((tok = strtok(NULL, " ")) == NULL || strcmp(tok, "SEQ") != 0)   
	goto parse_error;

    /* parse the sequence number */   
    if ((tok = strtok(NULL, "\n")) == NULL)
	goto parse_error;
    udp->sequence = atoi(tok);

    /* Save the body, if any */       
    if ((tok = strtok(NULL, "")) != NULL)
	pkt_cat(pkt, "%s", tok);

    amfree(str);
    return (0);

parse_error:
#if 0 /* XXX we have no way of passing this back up */
    security_seterror(&rh->sech,
	"parse error in packet header : '%s'", origstr);
#endif
    amfree(str);
    return (-1);
}

char *
check_user(
    struct sec_handle *	rh,
    const char *	remoteuser,
    const char *	service)
{
    struct passwd *pwd;
    char *r;
    char *result = NULL;
    char *localuser = NULL;

    /* lookup our local user name */
    if ((pwd = getpwnam(CLIENT_LOGIN)) == NULL) {
	return vstralloc("getpwnam(", CLIENT_LOGIN, ") fails", NULL);
    }

    /*
     * Make a copy of the user name in case getpw* is called by
     * any of the lower level routines.
     */
    localuser = stralloc(pwd->pw_name);

#ifndef USE_AMANDAHOSTS
    r = check_user_ruserok(rh->hostname, pwd, remoteuser);
#else
    r = check_user_amandahosts(rh->hostname, rh->peer.sin_addr, pwd, remoteuser, service);
#endif
    if (r != NULL) {
	result = vstralloc("user ", remoteuser, " from ", rh->hostname,
			   " is not allowed to execute the service ",
			   service, ": ", r, NULL);
	amfree(r);
    }
    amfree(localuser);
    return result;
}

/*
 * See if a remote user is allowed in.  This version uses ruserok()
 * and friends.
 *
 * Returns 0 on success, or negative on error.
 */
char *
check_user_ruserok(
    const char *	host,
    struct passwd *	pwd,
    const char *	remoteuser)
{
    int saved_stderr;
    int fd[2];
    FILE *fError;
    amwait_t exitcode;
    pid_t ruserok_pid;
    pid_t pid;
    char *es;
    char *result;
    int ok;
    char number[NUM_STR_SIZE];
    uid_t myuid = getuid();

    /*
     * note that some versions of ruserok (eg SunOS 3.2) look in
     * "./.rhosts" rather than "~CLIENT_LOGIN/.rhosts", so we have to
     * chdir ourselves.  Sigh.
     *
     * And, believe it or not, some ruserok()'s try an initgroup just
     * for the hell of it.  Since we probably aren't root at this point
     * it'll fail, and initgroup "helpfully" will blatt "Setgroups: Not owner"
     * into our stderr output even though the initgroup failure is not a
     * problem and is expected.  Thanks a lot.  Not.
     */
    if (pipe(fd) != 0) {
	return stralloc2("pipe() fails: ", strerror(errno));
    }
    if ((ruserok_pid = fork()) < 0) {
	return stralloc2("fork() fails: ", strerror(errno));
    } else if (ruserok_pid == 0) {
	int ec;

	close(fd[0]);
	fError = fdopen(fd[1], "w");
	if (!fError) {
	    error("Can't fdopen: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	/* pamper braindead ruserok's */
	if (chdir(pwd->pw_dir) != 0) {
	    fprintf(fError, "chdir(%s) failed: %s",
		    pwd->pw_dir, strerror(errno));
	    fclose(fError);
	    exit(1);
	}

#if defined(SHOW_SECURITY_DETAIL)				/* { */
	{
	char *dir = stralloc(pwd->pw_dir);

	secprintf(("%s: bsd: calling ruserok(%s, %d, %s, %s)\n",
		   debug_prefix_time(NULL),
	           host, ((myuid == 0) ? 1 : 0), remoteuser, pwd->pw_name));
	if (myuid == 0) {
	    secprintf(("%s: bsd: because you are running as root, ",
		       debug_prefix(NULL)));
	    secprintf(("/etc/hosts.equiv will not be used\n"));
	} else {
	    show_stat_info("/etc/hosts.equiv", NULL);
	}
	show_stat_info(dir, "/.rhosts");
	amfree(dir);
	}
#endif								/* } */

	saved_stderr = dup(2);
	close(2);
	if (open("/dev/null", O_RDWR) == -1) {
            secprintf(("%s: Could not open /dev/null: %s\n",
	              debug_prefix(NULL), strerror(errno)));
	    ec = 1;
	} else {
	    ok = ruserok(host, myuid == 0, remoteuser, CLIENT_LOGIN);
	    if (ok < 0) {
	        ec = 1;
	    } else {
	        ec = 0;
	    }
	}
	(void)dup2(saved_stderr,2);
	close(saved_stderr);
	exit(ec);
    }
    close(fd[1]);
    fError = fdopen(fd[0], "r");
    if (!fError) {
	error("Can't fdopen: %s", strerror(errno));
	/*NOTREACHED*/
    }

    result = NULL;
    while ((es = agets(fError)) != NULL) {
	if (*es == 0) {
	    amfree(es);
	    continue;
	}
	if (result == NULL) {
	    result = stralloc("");
	} else {
	    strappend(result, ": ");
	}
	strappend(result, es);
	amfree(es);
    }
    close(fd[0]);

    pid = wait(&exitcode);
    while (pid != ruserok_pid) {
	if ((pid == (pid_t) -1) && (errno != EINTR)) {
	    amfree(result);
	    return stralloc2("ruserok wait failed: %s", strerror(errno));
	}
	pid = wait(&exitcode);
    }
    if (WIFSIGNALED(exitcode)) {
	amfree(result);
	snprintf(number, SIZEOF(number), "%d", WTERMSIG(exitcode));
	return stralloc2("ruserok child got signal ", number);
    }
    if (WEXITSTATUS(exitcode) == 0) {
	amfree(result);
    } else if (result == NULL) {
	result = stralloc("ruserok failed");
    }

    return result;
}

/*
 * Check to see if a user is allowed in.  This version uses .amandahosts
 * Returns -1 on failure, or 0 on success.
 */
char *
check_user_amandahosts(
    const char *	host,
    struct in_addr      addr,
    struct passwd *	pwd,
    const char *	remoteuser,
    const char *	service)
{
    char *line = NULL;
    char *filehost;
    const char *fileuser;
    char *ptmp = NULL;
    char *result = NULL;
    FILE *fp = NULL;
    int found;
    struct stat sbuf;
    char n1[NUM_STR_SIZE];
    char n2[NUM_STR_SIZE];
    int hostmatch;
    int usermatch;
    char *aservice = NULL;

    secprintf(("check_user_amandahosts(host=%s, pwd=%p, "
		"remoteuser=%s, service=%s)\n",
		host, pwd, remoteuser, service));

    ptmp = stralloc2(pwd->pw_dir, "/.amandahosts");
#if defined(SHOW_SECURITY_DETAIL)				/* { */
    show_stat_info(ptmp, "");;
#endif								/* } */
    if ((fp = fopen(ptmp, "r")) == NULL) {
	result = vstralloc("cannot open ", ptmp, ": ", strerror(errno), NULL);
	amfree(ptmp);
	return result;
    }

    /*
     * Make sure the file is owned by the Amanda user and does not
     * have any group/other access allowed.
     */
    if (fstat(fileno(fp), &sbuf) != 0) {
	result = vstralloc("cannot fstat ", ptmp, ": ", strerror(errno), NULL);
	goto common_exit;
    }
    if (sbuf.st_uid != pwd->pw_uid) {
	snprintf(n1, SIZEOF(n1), "%ld", (long)sbuf.st_uid);
	snprintf(n2, SIZEOF(n2), "%ld", (long)pwd->pw_uid);
	result = vstralloc(ptmp, ": ",
			   "owned by id ", n1,
			   ", should be ", n2,
			   NULL);
	goto common_exit;
    }
    if ((sbuf.st_mode & 077) != 0) {
	result = stralloc2(ptmp,
	  ": incorrect permissions; file must be accessible only by its owner");
	goto common_exit;
    }

    /*
     * Now, scan the file for the host/user/service.
     */
    found = 0;
    while ((line = agets(fp)) != NULL) {
	if (*line == 0) {
	    amfree(line);
	    continue;
	}

#if defined(SHOW_SECURITY_DETAIL)				/* { */
	secprintf(("%s: bsd: processing line: <%s>\n", debug_prefix(NULL), line));
#endif								/* } */
	/* get the host out of the file */
	if ((filehost = strtok(line, " \t")) == NULL) {
	    amfree(line);
	    continue;
	}

	/* get the username.  If no user specified, then use the local user */
	if ((fileuser = strtok(NULL, " \t")) == NULL) {
	    fileuser = pwd->pw_name;
	}

	hostmatch = (strcasecmp(filehost, host) == 0);
	/*  ok if addr=127.0.0.1 and
        *  either localhost or localhost.domain is in .amandahost */
       if ( !hostmatch ) {
         if (strcmp(inet_ntoa(addr), "127.0.0.1")== 0  &&
             (strcasecmp(filehost, "localhost")== 0 ||
	      strcasecmp(filehost, "localhost.localdomain")== 0))
           {
             hostmatch = 1;
           }
       }
	usermatch = (strcasecmp(fileuser, remoteuser) == 0);
#if defined(SHOW_SECURITY_DETAIL)				/* { */
	secprintf(("%s: bsd: comparing \"%s\" with\n", debug_prefix(NULL), filehost));
	secprintf(("%s: bsd:           \"%s\" (%s)\n", host,
		  debug_prefix(NULL), hostmatch ? "match" : "no match"));
	secprintf(("%s: bsd:       and \"%s\" with\n", fileuser, debug_prefix(NULL)));
	secprintf(("%s: bsd:           \"%s\" (%s)\n", remoteuser,
		  debug_prefix(NULL), usermatch ? "match" : "no match"));
#endif								/* } */
	/* compare */
	if (!hostmatch || !usermatch) {
	    amfree(line);
	    continue;
	}

        if (!service) {
	    /* success */
	    amfree(line);
	    found = 1;
	    break;
	}

	/* get the services.  If no service specified, then use
	 * noop/selfcheck/sendsize/sendbackup
         */
	aservice = strtok(NULL, " \t,");
	if (!aservice) {
	    if (strcmp(service,"noop") == 0 ||
	       strcmp(service,"selfcheck") == 0 ||
	       strcmp(service,"sendsize") == 0 ||
	       strcmp(service,"sendbackup") == 0) {
		/* success */
		found = 1;
		amfree(line);
		break;
	    }
	    else {
		amfree(line);
		break;
	    }
	}

	do {
	    if (strcmp(aservice,service) == 0) {
		found = 1;
		break;
	    }
	    if (strcmp(aservice, "amdump") == 0 && 
	       (strcmp(service, "noop") == 0 ||
		strcmp(service, "selfcheck") == 0 ||
		strcmp(service, "sendsize") == 0 ||
		strcmp(service, "sendbackup") == 0)) {
		found = 1;
		break;
	    }
	} while((aservice = strtok(NULL, " \t,")) != NULL);

	if (aservice && strcmp(aservice, service) == 0) {
	    /* success */
	    found = 1;
	    amfree(line);
	    break;
	}
	amfree(line);
    }
    if (! found) {
	if (strcmp(service, "amindexd") == 0 ||
	    strcmp(service, "amidxtaped") == 0) {
	    result = vstralloc("Please add \"amindexd amidxtaped\" to "
			       "the line in ", ptmp, NULL);
	} else if (strcmp(service, "amdump") == 0 ||
		   strcmp(service, "noop") == 0 ||
		   strcmp(service, "selfcheck") == 0 ||
		   strcmp(service, "sendsize") == 0 ||
		   strcmp(service, "sendbackup") == 0) {
	    result = vstralloc("Please add \"amdump\" to the line in ",
			       ptmp, NULL);
	} else {
	    result = vstralloc(ptmp, ": ",
			       "invalid service ", service, NULL);
	}
    }

common_exit:

    afclose(fp);
    amfree(ptmp);

    return result;
}

/* return 1 on success, 0 on failure */
int
check_security(
    struct sockaddr_in *addr,
    char *		str,
    unsigned long	cksum,
    char **		errstr)
{
    char *		remotehost = NULL, *remoteuser = NULL;
    char *		bad_bsd = NULL;
    struct hostent *	hp;
    struct passwd *	pwptr;
    uid_t		myuid;
    int			i;
    int			j;
    char *		s;
    char *		fp;
    int			ch;

    (void)cksum;	/* Quiet unused parameter warning */

    secprintf(("%s: check_security(addr=%p, str='%s', cksum=%ul, errstr=%p\n",
		debug_prefix(NULL), addr, str, cksum, errstr));
    dump_sockaddr(addr);

    *errstr = NULL;

    /* what host is making the request? */

    hp = gethostbyaddr((char *)&addr->sin_addr, SIZEOF(addr->sin_addr),
		       AF_INET);
    if (hp == NULL) {
	/* XXX include remote address in message */
	*errstr = vstralloc("[",
			    "addr ", inet_ntoa(addr->sin_addr), ": ",
			    "hostname lookup failed",
			    "]", NULL);
	return 0;
    }
    remotehost = stralloc(hp->h_name);

    /* Now let's get the hostent for that hostname */
    hp = gethostbyname( remotehost );
    if (hp == NULL) {
	/* XXX include remote hostname in message */
	*errstr = vstralloc("[",
			    "host ", remotehost, ": ",
			    "hostname lookup failed",
			    "]", NULL);
	amfree(remotehost);
	return 0;
    }

    /* Verify that the hostnames match -- they should theoretically */
    if (strncasecmp( remotehost, hp->h_name, strlen(remotehost)+1 ) != 0 ) {
	*errstr = vstralloc("[",
			    "hostnames do not match: ",
			    remotehost, " ", hp->h_name,
			    "]", NULL);
	amfree(remotehost);
	return 0;
    }

    /* Now let's verify that the ip which gave us this hostname
     * is really an ip for this hostname; or is someone trying to
     * break in? (THIS IS THE CRUCIAL STEP)
     */
    for (i = 0; hp->h_addr_list[i]; i++) {
	if (memcmp(hp->h_addr_list[i],
		   (char *) &addr->sin_addr, SIZEOF(addr->sin_addr)) == 0)
	    break;                     /* name is good, keep it */
    }

    /* If we did not find it, your DNS is messed up or someone is trying
     * to pull a fast one on you. :(
     */

   /*   Check even the aliases list. Work around for Solaris if dns goes over NIS */

    if (!hp->h_addr_list[i] ) {
        for (j = 0; hp->h_aliases[j] !=0 ; j++) {
	     if (strcmp(hp->h_aliases[j],inet_ntoa(addr->sin_addr)) == 0)
	         break;                          /* name is good, keep it */
        }
	if (!hp->h_aliases[j] ) {
	    *errstr = vstralloc("[",
			        "ip address ", inet_ntoa(addr->sin_addr),
			        " is not in the ip list for ", remotehost,
			        "]",
			        NULL);
	    amfree(remotehost);
	    return 0;
	}
    }

    /* next, make sure the remote port is a "reserved" one */

    if (ntohs(addr->sin_port) >= IPPORT_RESERVED) {
	char number[NUM_STR_SIZE];

	snprintf(number, SIZEOF(number), "%hd", (short)ntohs(addr->sin_port));
	*errstr = vstralloc("[",
			    "host ", remotehost, ": ",
			    "port ", number, " not secure",
			    "]", NULL);
	amfree(remotehost);
	return 0;
    }

    /* extract the remote user name from the message */

    s = str;
    ch = *s++;

    bad_bsd = vstralloc("[",
			"host ", remotehost, ": ",
			"bad bsd security line",
			"]", NULL);

#define sc "USER "
    if (strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	*errstr = bad_bsd;
	bad_bsd = NULL;
	amfree(remotehost);
	return 0;
    }
    s += SIZEOF(sc)-1;
    ch = s[-1];
#undef sc

    skip_whitespace(s, ch);
    if (ch == '\0') {
	*errstr = bad_bsd;
	bad_bsd = NULL;
	amfree(remotehost);
	return 0;
    }
    fp = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    remoteuser = stralloc(fp);
    s[-1] = (char)ch;
    amfree(bad_bsd);

    /* lookup our local user name */

    myuid = getuid();
    if ((pwptr = getpwuid(myuid)) == NULL)
        error("error [getpwuid(%d) fails]", myuid);

    secprintf(("%s: bsd security: remote host %s user %s local user %s\n",
	      debug_prefix(NULL), remotehost, remoteuser, pwptr->pw_name));

#ifndef USE_AMANDAHOSTS
    s = check_user_ruserok(remotehost, pwptr, remoteuser);
#else
    s = check_user_amandahosts(remotehost, addr->sin_addr, pwptr, remoteuser, NULL);
#endif

    if (s != NULL) {
	*errstr = vstralloc("[",
			    "access as ", pwptr->pw_name, " not allowed",
			    " from ", remoteuser, "@", remotehost,
			    ": ", s, "]", NULL);
    }
    amfree(s);
    amfree(remotehost);
    amfree(remoteuser);
    return *errstr == NULL;
}

/*
 * Writes out the entire iovec
 */
ssize_t
net_writev(
    int			fd,
    struct iovec *	iov,
    int			iovcnt)
{
    ssize_t delta, n, total;

    assert(iov != NULL);

    total = 0;
    while (iovcnt > 0) {
	/*
	 * Write the iovec
	 */
	n = writev(fd, iov, iovcnt);
	if (n < 0) {
	    if (errno != EINTR)
		return (-1);
	    secprintf(("%s: net_writev got EINTR\n",
		       debug_prefix(NULL)));
	}
	else if (n == 0) {
	    errno = EIO;
	    return (-1);
	} else {
	    total += n;
	    /*
	     * Iterate through each iov.  Figure out what we still need
	     * to write out.
	     */
	    for (; n > 0; iovcnt--, iov++) {
		/* 'delta' is the bytes written from this iovec */
		delta = ((size_t)n < iov->iov_len) ? n : (ssize_t)iov->iov_len;
		/* subtract from the total num bytes written */
		n -= delta;
		assert(n >= 0);
		/* subtract from this iovec */
		iov->iov_len -= delta;
		iov->iov_base = (char *)iov->iov_base + delta;
		/* if this iovec isn't empty, run the writev again */
		if (iov->iov_len > 0)
		    break;
	    }
	}
    }
    return (total);
}

/*
 * Like read(), but waits until the entire buffer has been filled.
 */
ssize_t
net_read(
    int		fd,
    void *	vbuf,
    size_t	origsize,
    int		timeout)
{
    char *buf = vbuf;	/* ptr arith */
    ssize_t nread;
    size_t size = origsize;

    secprintf(("%s: net_read: begin %d\n", debug_prefix_time(NULL), origsize));

    while (size > 0) {
	secprintf(("%s: net_read: while %d\n",
		   debug_prefix_time(NULL), size));
	nread = net_read_fillbuf(fd, timeout, buf, size);
	if (nread < 0) {
    	    secprintf(("%s: db: net_read: end return(-1)\n",
		       debug_prefix_time(NULL)));
	    return (-1);
	}
	if (nread == 0) {
    	    secprintf(("%s: net_read: end return(0)\n",
		       debug_prefix_time(NULL)));
	    return (0);
	}
	buf += nread;
	size -= nread;
    }
    secprintf(("%s: net_read: end %d\n",
	       debug_prefix_time(NULL), origsize));
    return ((ssize_t)origsize);
}

/*
 * net_read likes to do a lot of little reads.  Buffer it.
 */
ssize_t
net_read_fillbuf(
    int		fd,
    int		timeout,
    void *	buf,
    size_t	size)
{
    fd_set readfds;
    struct timeval tv;
    ssize_t nread;

    secprintf(("%s: net_read_fillbuf: begin\n", debug_prefix_time(NULL)));
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    tv.tv_sec = timeout;
    tv.tv_usec = 0;
    switch (select(fd + 1, &readfds, NULL, NULL, &tv)) {
    case 0:
	errno = ETIMEDOUT;
	/* FALLTHROUGH */
    case -1:
	secprintf(("%s: net_read_fillbuf: case -1\n",
		   debug_prefix_time(NULL)));
	return (-1);
    case 1:
	secprintf(("%s: net_read_fillbuf: case 1\n",
		   debug_prefix_time(NULL)));
	assert(FD_ISSET(fd, &readfds));
	break;
    default:
	secprintf(("%s: net_read_fillbuf: case default\n",
		   debug_prefix_time(NULL)));
	assert(0);
	break;
    }
    nread = read(fd, buf, size);
    if (nread < 0)
	return (-1);
    secprintf(("%s: net_read_fillbuf: end %d\n",
	       debug_prefix_time(NULL), nread));
    return (nread);
}


/*
 * Display stat() information about a file.
 */

void
show_stat_info(
    char *	a,
    char *	b)
{
    char *name = vstralloc(a, b, NULL);
    struct stat sbuf;
    struct passwd *pwptr;
    char *owner;
    struct group *grptr;
    char *group;

    if (stat(name, &sbuf) != 0) {
	secprintf(("%s: bsd: cannot stat %s: %s\n",
		   debug_prefix_time(NULL), name, strerror(errno)));
	amfree(name);
	return;
    }
    if ((pwptr = getpwuid(sbuf.st_uid)) == NULL) {
	owner = alloc(NUM_STR_SIZE + 1);
	snprintf(owner, NUM_STR_SIZE, "%ld", (long)sbuf.st_uid);
    } else {
	owner = stralloc(pwptr->pw_name);
    }
    if ((grptr = getgrgid(sbuf.st_gid)) == NULL) {
	group = alloc(NUM_STR_SIZE + 1);
	snprintf(owner, NUM_STR_SIZE, "%ld", (long)sbuf.st_gid);
    } else {
	group = stralloc(grptr->gr_name);
    }
    secprintf(("%s: bsd: processing file: %s\n", debug_prefix(NULL), name));
    secprintf(("%s: bsd:                  owner=%s group=%s mode=%03o\n",
	       debug_prefix(NULL), owner, group, (int) (sbuf.st_mode & 0777)));
    amfree(name);
    amfree(owner);
    amfree(group);
}
