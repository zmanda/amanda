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
#include "stream.h"
#include "version.h"

/*#define       BSD_DEBUG*/

#ifdef BSD_DEBUG
#define bsdprintf(x)    dbprintf(x)
#else
#define bsdprintf(x)
#endif

#ifndef SO_RCVBUF
#undef DUMPER_SOCKET_BUFFERING
#endif

#ifdef BSD_SECURITY						/* { */

/*
 * Change the following from #undef to #define to cause detailed logging
 * of the security steps, e.g. into /tmp/amanda/amandad*debug.
 */
#undef SHOW_SECURITY_DETAIL

#if defined(TEST)						/* { */
#define SHOW_SECURITY_DETAIL
#undef bsdprintf
#define bsdprintf(p)	printf p
#endif								/* } */

/*
 * Interface functions
 */
static void	bsd_connect(const char *, char *(*)(char *, void *), 
			void (*)(void *, security_handle_t *, security_status_t),
			void *, void *);
static void	bsd_accept(const struct security_driver *, int, int,
			void (*)(security_handle_t *, pkt_t *));
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
};

/*
 * This is data local to the datagram socket.  We have one datagram
 * per process, so it is global.
 */
static udp_handle_t netfd;
static int not_init = 1;

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
    struct servent *se;
    struct hostent *he;
    in_port_t port = 0;
    struct timeval sequence_time;
    amanda_timezone dontcare;
    int sequence;
    char *handle;

    assert(hostname != NULL);

    (void)conf_fn;	/* Quiet unused parameter warning */
    (void)datap;        /* Quiet unused parameter warning */

    bh = alloc(SIZEOF(*bh));
    bh->proto_handle=NULL;
    bh->udp = &netfd;
    security_handleinit(&bh->sech, &bsd_security_driver);

    /*
     * Only init the socket once
     */
    if (not_init == 1) {
	uid_t euid;
	dgram_zero(&netfd.dgram);

	euid = geteuid();
	seteuid((uid_t)0);
	dgram_bind(&netfd.dgram, &port);
	seteuid(euid);
	netfd.handle = NULL;
	netfd.pkt.body = NULL;
	netfd.recv_security_ok = &bsd_recv_security_ok;
	netfd.prefix_packet = &bsd_prefix_packet;
	/*
	 * We must have a reserved port.  Bomb if we didn't get one.
	 */
	if (port >= IPPORT_RESERVED) {
	    security_seterror(&bh->sech,
		"unable to bind to a reserved port (got port %u)",
		(unsigned int)port);
	    (*fn)(arg, &bh->sech, S_ERROR);
	    return;
	}
	not_init = 0;
    }

    if ((he = gethostbyname(hostname)) == NULL) {
	security_seterror(&bh->sech,
	    "%s: could not resolve hostname", hostname);
	(*fn)(arg, &bh->sech, S_ERROR);
	return;
    }
    bsdprintf(("Resolved hostname=%s\n", hostname));
    if ((se = getservbyname(AMANDA_SERVICE_NAME, "udp")) == NULL)
	port = (in_port_t)htons(AMANDA_SERVICE_DEFAULT);
    else
	port = (in_port_t)se->s_port;
    amanda_gettimeofday(&sequence_time, &dontcare);
    sequence = (int)sequence_time.tv_sec ^ (int)sequence_time.tv_usec;
    handle=alloc(15);
    snprintf(handle, 14, "000-%08x",  (unsigned)newhandle++);
    if (udp_inithandle(&netfd, bh, he, port, handle, sequence) < 0) {
	(*fn)(arg, &bh->sech, S_ERROR);
	amfree(bh->hostname);
	amfree(bh);
    }
    else
	(*fn)(arg, &bh->sech, S_OK);
    amfree(handle);
}

/*
 * Setup to accept new incoming connections
 */
static void
bsd_accept(
    const struct security_driver *	driver,
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *))
{

    assert(in >= 0 && out >= 0);
    assert(fn != NULL);

    (void)out;	/* Quiet unused parameter warning */
    (void)driver; /* Quiet unused parameter warning */

    /*
     * We assume in and out point to the same socket, and just use
     * in.
     */
    dgram_socket(&netfd.dgram, in);

    /*
     * Assign the function and return.  When they call recvpkt later,
     * the recvpkt callback will call this function when it discovers
     * new incoming connections
     */
    netfd.accept_fn = fn;
    netfd.recv_security_ok = &bsd_recv_security_ok;
    netfd.prefix_packet = &bsd_prefix_packet;
    netfd.driver = &bsd_security_driver;

    udp_addref(&netfd, &udp_netfd_read_callback);
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

    bsdprintf(("%s: bsd: close handle '%s'\n",
	       debug_prefix_time(NULL), bh->proto_handle));

    udp_recvpkt_cancel(bh);
    if(bh->next) {
	bh->next->prev = bh->prev;
    }
    else {
	netfd.bh_last = bh->prev;
    }
    if(bh->prev) {
	bh->prev->next = bh->next;
    }
    else {
	netfd.bh_first = bh->next;
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
#ifndef TEST							/* { */
    struct sec_stream *bs = NULL;
    struct sec_handle *bh = h;

    assert(bh != NULL);

    bs = alloc(SIZEOF(*bs));
    security_streaminit(&bs->secstr, &bsd_security_driver);
    bs->socket = stream_server(&bs->port, (size_t)STREAM_BUFSIZE, 
			(size_t)STREAM_BUFSIZE, 0);
    if (bs->socket < 0) {
	security_seterror(&bh->sech,
	    "can't create server stream: %s", strerror(errno));
	amfree(bs);
	return (NULL);
    }
    bs->fd = -1;
    bs->ev_read = NULL;
    return (bs);
#else
    return (NULL);
#endif /* !TEST */						/* } */
}

/*
 * Accepts a new connection on unconnected streams.  Assumes it is ok to
 * block on accept()
 */
static int
bsd_stream_accept(
    void *	s)
{
#ifndef TEST							/* { */
    struct sec_stream *bs = s;

    assert(bs != NULL);
    assert(bs->socket != -1);
    assert(bs->fd < 0);

    bs->fd = stream_accept(bs->socket, 30, STREAM_BUFSIZE, STREAM_BUFSIZE);
    if (bs->fd < 0) {
	security_stream_seterror(&bs->secstr,
	    "can't accept new stream connection: %s", strerror(errno));
	return (-1);
    }
#endif /* !TEST */						/* } */
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
#ifndef TEST							/* { */
    struct sec_handle *bh = h;
#ifdef DUMPER_SOCKET_BUFFERING
    size_t rcvbuf = SIZEOF(bs->databuf) * 2;
#endif

    assert(bh != NULL);

    bs = alloc(SIZEOF(*bs));
    security_streaminit(&bs->secstr, &bsd_security_driver);
    bs->fd = stream_client(bh->hostname, (in_port_t)id,
	STREAM_BUFSIZE, STREAM_BUFSIZE, &bs->port, 0);
    if (bs->fd < 0) {
	security_seterror(&bh->sech,
	    "can't connect stream to %s port %hd: %s", bh->hostname,
	    id, strerror(errno));
	amfree(bs);
	return (NULL);
    }
    bs->socket = -1;	/* we're a client */
    bs->ev_read = NULL;
#ifdef DUMPER_SOCKET_BUFFERING
    setsockopt(bs->fd, SOL_SOCKET, SO_RCVBUF, (void *)&rcvbuf, SIZEOF(rcvbuf));
#endif
#endif /* !TEST */						/* } */
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

    bsdprintf(("%s: bsd: stream_read_callback_sync: fd %d\n",
		 debug_prefix_time(NULL), bs->fd));

    /*
     * Remove the event first, in case they reschedule it in the callback.
     */
    bsd_stream_read_cancel(bs);
    do {
	n = read(bs->fd, bs->databuf, sizeof(bs->databuf));
    } while ((n < 0) && ((errno == EINTR) || (errno == EAGAIN)));
    if (n < 0)
        security_stream_seterror(&bs->secstr, strerror(errno));
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
	security_stream_seterror(&bs->secstr, strerror(errno));

    (*bs->fn)(bs->arg, bs->databuf, n);
}

#endif	/* BSD_SECURITY */					/* } */

#if defined(TEST)						/* { */

/*
 * The following dummy bind_portrange function is so we do not need to
 * drag in util.o just for the test program.
 */
int
bind_portrange(
    int			s,
    struct sockaddr_in *addrp,
    in_port_t		first_port,
    in_port_t		last_port,
    char *		proto)
{
    (void)s;		/* Quiet unused parameter warning */
    (void)addrp;	/* Quiet unused parameter warning */
    (void)first_port;	/* Quiet unused parameter warning */
    (void)last_port;	/* Quiet unused parameter warning */
    (void)proto;	/* Quiet unused parameter warning */

    return 0;
}

/*
 * Construct a datestamp (YYYYMMDD) from a time_t.
 */
char *
construct_datestamp(
    time_t *	t)
{
    struct tm *tm;
    char datestamp[3*NUM_STR_SIZE];
    time_t when;

    if(t == NULL) {
	when = time((time_t *)NULL);
    } else {
	when = *t;
    }
    tm = localtime(&when);
    snprintf(datestamp, SIZEOF(datestamp),
             "%04d%02d%02d", tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday);
    return stralloc(datestamp);
}

/*
 * Construct a timestamp (YYYYMMDDHHMMSS) from a time_t.
 */
char *
construct_timestamp(
    time_t *	t)
{
    struct tm *tm;
    char timestamp[6*NUM_STR_SIZE];
    time_t when;

    if(t == NULL) {
	when = time((time_t *)NULL);
    } else {
	when = *t;
    }
    tm = localtime(&when);
    snprintf(timestamp, SIZEOF(timestamp),
	     "%04d%02d%02d%02d%02d%02d",
	     tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
	     tm->tm_hour, tm->tm_min, tm->tm_sec);
    return stralloc(timestamp);
}

/*
 * The following are so we can include security.o but not all the rest
 * of the security modules.
 */
const security_driver_t krb4_security_driver = {};
const security_driver_t krb5_security_driver = {};
const security_driver_t rsh_security_driver = {};
const security_driver_t ssh_security_driver = {};
const security_driver_t bsdtcp_security_driver = {};
const security_driver_t bsdudp_security_driver = {};

/*
 * This function will be called to accept the connection and is used
 * to report success or failure.
 */
static void fake_accept_function(
    security_handle_t *	handle,
    pkt_t *		pkt)
{
    if (pkt == NULL) {
	fputs(handle->error, stdout);
	fputc('\n', stdout);
    } else {
	fputs("access is allowed\n", stdout);
    }
}

int
main (
    int		argc,
    char **	argv)
{
    char *remoteuser;
    char *remotehost;
    struct hostent *hp;
    struct sec_handle *bh;
    void *save_cur;
    struct passwd *pwent;

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    /*
     * The following is stolen from amandad to emulate what it would
     * do on startup.
     */
    if(client_uid == (uid_t) -1 && (pwent = getpwnam(CLIENT_LOGIN)) != NULL) {
	client_uid = pwent->pw_uid;
	client_gid = pwent->pw_gid;
	endpwent();
    }

#ifdef FORCE_USERID
    /* we'd rather not run as root */
    if (geteuid() == 0) {
	if(client_uid == (uid_t) -1) {
	    error("error [cannot find user %s in passwd file]\n", CLIENT_LOGIN);
	    /*NOTREACHED*/
	}
	initgroups(CLIENT_LOGIN, client_gid);
	setgid(client_gid);
	setegid(client_gid);
	seteuid(client_uid);
    }
#endif	/* FORCE_USERID */

    if (isatty(0)) {
	fputs("Remote user: ", stdout);
	fflush(stdout);
    }
    do {
	amfree(remoteuser);
	remoteuser = agets(stdin);
	if (remoteuser == NULL)
	    return 0;
    } while (remoteuser[0] == '\0');

    if (isatty(0)) {
	fputs("Remote host: ", stdout);
	fflush(stdout);
    }

    do {
	amfree(remotehost);
	remotehost = agets(stdin);
	if (remotehost == NULL)
	    return 0;
    } while (remotehost[0] == '\0');

    set_pname("security");
    dbopen(NULL);

    startclock();

    if ((hp = gethostbyname(remotehost)) == NULL) {
	fprintf(stderr, "cannot look up remote host %s\n", remotehost);
	return 1;
    }
    memcpy((char *)&netfd.peer.sin_addr,
	   (char *)hp->h_addr,
	   SIZEOF(hp->h_addr));
    /*
     * Fake that it is coming from a reserved port.
     */
    netfd.peer.sin_port = htons(IPPORT_RESERVED - 1);

    bh = alloc(SIZEOF(*bh));
    bh->proto_handle=NULL;
    bh->udp = &netfd;
    netfd.pkt.type = P_REQ;
    dgram_zero(&netfd.dgram);
    save_cur = netfd.dgram.cur;				/* cheating */
    dgram_cat(&netfd.dgram, "%s", pkthdr2str(bh, &netfd.pkt));
    dgram_cat(&netfd.dgram, "SECURITY USER %s\n", remoteuser);
    netfd.dgram.cur = save_cur;				/* cheating */

    netfd.accept_fn = fake_accept_function;
    netfd.recv_security_ok = &bsd_recv_security_ok;
    netfd.prefix_packet = &bsd_prefix_packet;
    udp_netfd_read_callback(&netfd);

    return 0;
}

#endif								/* } */
