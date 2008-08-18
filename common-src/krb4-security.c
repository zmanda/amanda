/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1993,1999 University of Maryland
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
 * $Id: krb4-security.c,v 1.18 2006/07/13 03:22:20 paddy_s Exp $
 *
 * krb4-security.c - helper functions for kerberos v4 security.
 */

#include "config.h"

#include <des.h>
#include <krb.h>

#include "amanda.h"
#include "dgram.h"
#include "event.h"
#include "packet.h"
#include "queue.h"
#include "security.h"
#include "security-util.h"
#include "protocol.h"
#include "stream.h"
#include "version.h"


/*
 * If you don't have atexit() or on_exit(), you could just consider
 * making atexit() empty and clean up your ticket files some other
 * way
 */
#ifndef HAVE_ATEXIT
#ifdef HAVE_ON_EXIT
#define	atexit(func)	on_exit(func, 0)
#else
#define	atexit(func)	(you must to resolve lack of atexit)
#endif	/* HAVE_ON_EXIT */
#endif	/* ! HAVE_ATEXIT */

/*
 * This is the tcp stream buffer size
 */
#ifndef STREAM_BUFSIZE
#define	STREAM_BUFSIZE	(32768*2)
#endif

int krb_set_lifetime(int);
int kuserok(AUTH_DAT *, char *);

/*
 * This is the private handle data
 */
struct krb4_handle {
    security_handle_t sech;	/* MUST be first */
    struct sockaddr_in6 peer;	/* host on other side */
    char hostname[MAX_HOSTNAME_LENGTH+1];	/* human form of above */
    char proto_handle[32];	/* protocol handle for this req */
    int sequence;		/* last sequence number we received */
    char inst[INST_SZ];		/* krb4 instance form of above */
    char realm[REALM_SZ];	/* krb4 realm of this host */
    unsigned long cksum;	/* cksum of the req packet we sent */
    des_cblock session_key;	/* session key */

    /*
     * The rest is used for the async recvpkt/recvpkt_cancel
     * interface.
     */
    void (*fn)(void *, pkt_t *, security_status_t);
					/* func to call when packet recvd */
    void *arg;				/* argument to pass function */
    event_handle_t *ev_timeout;		/* timeout handle for recv */
    TAILQ_ENTRY(krb4_handle) tq;	/* queue handle */
};

/*
 * This is the internal security_stream data for krb4.
 */
struct krb4_stream {
    security_stream_t secstr;		/* MUST be first */
    struct krb4_handle *krb4_handle;	/* pointer into above */
    int fd;				/* io file descriptor */
    in_port_t port;			/* local port this is bound to */
    int socket;				/* fd for server-side accepts */
    event_handle_t *ev_read;		/* read event handle */
    char databuf[STREAM_BUFSIZE];	/* read buffer */
    int len;				/* */
    void (*fn)(void *, void *, ssize_t);/* read event fn */
    void *arg;				/* arg for previous */
};

/*
 * Interface functions
 */
static void	krb4_connect(const char *, char *(*)(char *, void *),  
			void (*)(void *, security_handle_t *, security_status_t),
			void *, void *);
static void	krb4_accept(const struct security_driver *, int, int, void (*)(security_handle_t *, pkt_t *));
static void	krb4_close(void *);
static int	krb4_sendpkt(void *, pkt_t *);
static void	krb4_recvpkt(void *, void (*)(void *, pkt_t *, security_status_t),
			void *, int);
static void	krb4_recvpkt_cancel(void *);
static void *	krb4_stream_server(void *);
static int	krb4_stream_accept(void *);
static void *	krb4_stream_client(void *, int);
static void	krb4_stream_close(void *);
static int	krb4_stream_auth(void *);
static int	krb4_stream_id(void *);
static int	krb4_stream_write(void *, const void *, size_t);
static void	krb4_stream_read(void *, void (*)(void *, void *, int), void *);
static int	krb4_stream_read_sync(void *, void **);
static void	krb4_stream_read_cancel(void *);


/*
 * This is our interface to the outside world.
 */
const security_driver_t krb4_security_driver = {
    "KRB4",
    krb4_connect,
    krb4_accept,
    krb4_close,
    krb4_sendpkt,
    krb4_recvpkt,
    krb4_recvpkt_cancel,
    krb4_stream_server,
    krb4_stream_accept,
    krb4_stream_client,
    krb4_stream_close,
    krb4_stream_auth,
    krb4_stream_id,
    krb4_stream_write,
    krb4_stream_read,
    krb4_stream_read_sync,
    krb4_stream_read_cancel,
    sec_close_connection_none,
    NULL,
    NULL
};

/*
 * Cache the local hostname
 */
static char hostname[MAX_HOSTNAME_LENGTH+1];

/*
 * This is the dgram_t that we use to send and recv protocol packets
 * over the net.  There is only one per process, so it lives globally
 * here.
 */
static dgram_t netfd;

/*
 * This is a queue of outstanding async requests
 */
static struct {
    TAILQ_HEAD(, krb4_handle) tailq;
    int qlength;
} handleq = {
    TAILQ_HEAD_INITIALIZER(handleq.tailq), 0
};

/*
 * Macros to add or remove krb4_handles from the above queue
 */
#define	handleq_add(kh)	do {			\
    assert(handleq.qlength == 0 ? TAILQ_FIRST(&handleq.tailq) == NULL : 1); \
    TAILQ_INSERT_TAIL(&handleq.tailq, kh, tq);	\
    handleq.qlength++;				\
} while (0)

#define	handleq_remove(kh)	do {			\
    assert(handleq.qlength > 0);			\
    assert(TAILQ_FIRST(&handleq.tailq) != NULL);	\
    TAILQ_REMOVE(&handleq.tailq, kh, tq);		\
    handleq.qlength--;					\
    assert((handleq.qlength == 0) ^ (TAILQ_FIRST(&handleq.tailq) != NULL)); \
} while (0)

#define	handleq_first()		TAILQ_FIRST(&handleq.tailq)
#define	handleq_next(kh)	TAILQ_NEXT(kh, tq)


/*
 * This is the event manager's handle for our netfd
 */
static event_handle_t *ev_netfd;

/*
 * This is a function that should be called if a new security_handle_t is
 * created.  If NULL, no new handles are created.
 * It is passed the new handle and the received pkt
 */
static void (*accept_fn)(security_handle_t *, pkt_t *);


/*
 * This is a structure used in encoding the cksum in a mutual-auth
 * transaction.  The checksum is placed in here first before encryption
 * because encryption requires at least 8 bytes of data, and an unsigned
 * long on most machines (32 bit ones) is 4 bytes.
 */
union mutual {
    char pad[8];
    unsigned long cksum;
};

/*
 * Private functions
 */
static unsigned long krb4_cksum(const char *);
static void krb4_getinst(const char *, char *, size_t);
static void host2key(const char *, const char *, des_cblock *);
static void init(void);
static void inithandle(struct krb4_handle *, struct hostent *, int,
    const char *);
static void get_tgt(void);
static void killtickets(void);
static void recvpkt_callback(void *);
static void recvpkt_timeout(void *);
static int recv_security_ok(struct krb4_handle *, pkt_t *);
static void stream_read_callback(void *);
static void stream_read_sync_callback(void *);
static int knet_write(int, const void *, size_t);
static int knet_read(int, void *, size_t, int);

static int add_ticket(struct krb4_handle *, const pkt_t *, dgram_t *);
static void add_mutual_auth(struct krb4_handle *, dgram_t *);
static int check_ticket(struct krb4_handle *, const pkt_t *,
    const char *, unsigned long);
static int check_mutual_auth(struct krb4_handle *, const char *);

static const char *kpkthdr2str(const struct krb4_handle *, const pkt_t *);
static int str2kpkthdr(const char *, pkt_t *, char *, size_t, int *);

static const char *bin2astr(const unsigned char *, int);
static void astr2bin(const unsigned char *, unsigned char *, int *);

static void encrypt_data(void *, size_t, des_cblock *);
static void decrypt_data(void *, size_t, des_cblock *);

#define HOSTNAME_INSTANCE inst

static char *ticketfilename = NULL;

static void
killtickets(void)
{
    if (ticketfilename != NULL)
	unlink(ticketfilename);
    amfree(ticketfilename);
}

/*
 * Setup some things about krb4.  This should only be called once.
 */
static void
init(void)
{
    char tktfile[256];
    in_port_t port;
    static int beenhere = 0;

    if (beenhere)
	return;
    beenhere = 1;

    gethostname(hostname, SIZEOF(hostname) - 1);
    hostname[SIZEOF(hostname) - 1] = '\0';

    if (atexit(killtickets) < 0)
	error(_("could not setup krb4 exit handler: %s"), strerror(errno));

    /*
     * [XXX] It could be argued that if KRBTKFILE is set outside of amanda,
     * that it's value should be used instead of us setting one up.
     * This file also needs to be removed so that no extra tickets are
     * hanging around.
     */
    g_snprintf(tktfile, SIZEOF(tktfile), "/tmp/tkt%ld-%ld.amanda",
	(long)getuid(), (long)getpid());
    ticketfilename = stralloc(tktfile);
    unlink(ticketfilename);
    krb_set_tkt_string(ticketfilename);
#if defined(HAVE_PUTENV)
    {
	char *tkt_env = stralloc2("KRBTKFILE=", ticketfilename);
	putenv(tkt_env);
    }
#else
    setenv("KRBTKFILE", ticketfile, 1);
#endif

    dgram_zero(&netfd);
    dgram_bind(&netfd, &port);
}

/*
 * Get a ticket granting ticket and stuff it in the cache
 */
static void
get_tgt(void)
{
    char realm[REALM_SZ];
    int rc;

    strncpy(realm, krb_realmofhost(hostname), SIZEOF(realm) - 1);
    realm[SIZEOF(realm) - 1] = '\0';

    rc = krb_get_svc_in_tkt(SERVER_HOST_PRINCIPAL, SERVER_HOST_INSTANCE,
	realm, "krbtgt", realm, TICKET_LIFETIME, SERVER_HOST_KEY_FILE);

    if (rc != 0) {
	error(_("could not get krbtgt for %s.%s@%s from %s: %s"),
	    SERVER_HOST_PRINCIPAL, SERVER_HOST_INSTANCE, realm,
	    SERVER_HOST_KEY_FILE, krb_err_txt[rc]);
    }

    krb_set_lifetime(TICKET_LIFETIME);
}


/*
 * krb4 version of a security handle allocator.  Logically sets
 * up a network "connection".
 */
static void
krb4_connect(
    const char *hostname,
    char *	(*conf_fn)(char *, void *),
    void 	(*fn)(void *, security_handle_t *, security_status_t),
    void *	arg,
    void *	datap)
{
    struct krb4_handle *kh;
    char handle[32];
    struct servent *se;
    struct hostent *he;
    in_port_t port;

    (void)conf_fn;	/* Quiet unused parameter warning */
    (void)datap;	/* Quiet unused parameter warning */

    assert(hostname != NULL);

    /*
     * Make sure we're initted
     */
    init();

    kh = alloc(SIZEOF(*kh));
    security_handleinit(&kh->sech, &krb4_security_driver);

    if ((he = gethostbyname(hostname)) == NULL) {
	security_seterror(&kh->sech,
	    _("%s: could not resolve hostname"), hostname);
	(*fn)(arg, &kh->sech, S_ERROR);
	return;
    }
    if ((se = getservbyname(KAMANDA_SERVICE_NAME, "udp")) == NULL)
	port = (int)KAMANDA_SERVICE_DEFAULT;
    else
	port = ntohs(se->s_port);
    g_snprintf(handle, SIZEOF(handle), "%ld", (long)time(NULL));
    inithandle(kh, he, (int)port, handle);
    (*fn)(arg, &kh->sech, S_OK);
}

/*
 * Setup to handle new incoming connections
 */
static void
krb4_accept(
    const struct security_driver *driver,
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *))
{
    (void)driver;	/* Quiet unused parameter warning */
    (void)out;		/* Quiet unused parameter warning */

    /*
     * Make sure we're initted
     */
    init();

    /*
     * We assume that in and out both point to the same socket
     */
    dgram_socket(&netfd, in);

    /*
     * Assign the function and return.  When they call recvpkt later,
     * the recvpkt callback will call this function when it discovers
     * new incoming connections
     */
    accept_fn = fn;

    if (ev_netfd == NULL)
	ev_netfd = event_register((event_id_t)netfd.socket, EV_READFD,
			    recvpkt_callback, NULL);
}

/*
 * Given a hostname and a port, setup a krb4_handle
 */
static void
inithandle(
    struct krb4_handle *kh,
    struct hostent *	he,
    int			port,
    const char *	handle)
{

    /*
     * Get the instance and realm for this host
     * (krb_realmofhost always returns something)
     */
    krb4_getinst(he->h_name, kh->inst, SIZEOF(kh->inst));
    strncpy(kh->realm, krb_realmofhost(he->h_name), SIZEOF(kh->realm) - 1);
    kh->realm[SIZEOF(kh->realm) - 1] = '\0';

    /*
     * Save a copy of the hostname
     */
    strncpy(kh->hostname, he->h_name, SIZEOF(kh->hostname) - 1);
    kh->hostname[SIZEOF(kh->hostname) - 1] = '\0';

    /*
     * We have no checksum or session key at this point
     */
    kh->cksum = 0;
    memset(kh->session_key, 0, SIZEOF(kh->session_key));

    /*
     * Setup our peer info.  We don't do anything with the sequence yet,
     * so just leave it at 0.
     */
    kh->peer.sin6_family = (sa_family_t)AF_INET6;
    kh->peer.sin6_port = (in_port_t)port;
    kh->peer.sin6_addr = *(struct in6_addr *)he->h_addr;
    strncpy(kh->proto_handle, handle, SIZEOF(kh->proto_handle) - 1);
    kh->proto_handle[SIZEOF(kh->proto_handle) - 1] = '\0';
    kh->sequence = 0;
    kh->fn = NULL;
    kh->arg = NULL;
    kh->ev_timeout = NULL;
}

/*
 * frees a handle allocated by the above
 */
static void
krb4_close(
    void *	inst)
{

    krb4_recvpkt_cancel(inst);
    amfree(inst);
}

/*
 * Transmit a packet.  Add security information first.
 */
static ssize_t
krb4_sendpkt(
    void *	cookie,
    pkt_t *	pkt)
{
    struct krb4_handle *kh = cookie;

    assert(kh != NULL);
    assert(pkt != NULL);

    /*
     * Initialize this datagram
     */
    dgram_zero(&netfd);

    /*
     * Add the header to the packet
     */
    dgram_cat(&netfd, kpkthdr2str(kh, pkt));

    /*
     * Add the security info.  This depends on which kind of packet we're
     * sending.
     */
    switch (pkt->type) {
    case P_REQ:
	/*
	 * Requests get sent with a ticket embedded in the header.  The
	 * checksum is generated from the contents of the body.
	 */
	if (add_ticket(kh, pkt, &netfd) < 0)
	    return (-1);
	break;
    case P_REP:
	/*
	 * Replies get sent with a mutual authenticator added.  The
	 * mutual authenticator is the encrypted checksum from the
	 * ticket + 1
	 */
	add_mutual_auth(kh, &netfd);
	break;
    case P_ACK:
    case P_NAK:
    default:
	/*
	 * The other types have no security stuff added for krb4.
	 * Shamefull.
	 */
	break;
    }

    /*
     * Add the body, and send it
     */
    dgram_cat(&netfd, pkt->body);
    if (dgram_send_addr(&kh->peer, &netfd) != 0) {
	security_seterror(&kh->sech,
	    _("send %s to %s failed: %s"), pkt_type2str(pkt->type),
	    kh->hostname, strerror(errno));
	return (-1);
    }
    return (0);
}

/*
 * Set up to receive a packet asyncronously, and call back when
 * it has been read.
 */
static void
krb4_recvpkt(
    void *	cookie,
    void	(*fn)(void *, pkt_t *, security_status_t),
    void *	arg,
    int		timeout)
{
    struct krb4_handle *kh = cookie;

    assert(netfd.socket >= 0);
    assert(kh != NULL);

    /*
     * We register one event handler for our network fd which takes
     * care of all of our async requests.  When all async requests
     * have either been satisfied or cancelled, we unregister our
     * network event handler.
     */
    if (ev_netfd == NULL) {
	assert(handleq.qlength == 0);
	ev_netfd = event_register((event_id_t)netfd.socket, EV_READFD,
			    recvpkt_callback, NULL);
    }

    /*
     * Multiple recvpkt calls override previous ones
     * If kh->fn is NULL then it is not in the queue.
     */
    if (kh->fn == NULL)
	handleq_add(kh);
    if (kh->ev_timeout != NULL)
	event_release(kh->ev_timeout);
    if (timeout < 0)
	kh->ev_timeout = NULL;
    else
	kh->ev_timeout = event_register((event_id_t)timeout, EV_TIME,
				recvpkt_timeout, kh);
    kh->fn = fn;
    kh->arg = arg;
}

/*
 * Remove a async receive request from the queue
 * If it is the last one to be removed, then remove the event handler
 * for our network fd.
 */
static void
krb4_recvpkt_cancel(
    void *	cookie)
{
    struct krb4_handle *kh = cookie;

    assert(kh != NULL);

    if (kh->fn != NULL) {
	handleq_remove(kh);
	kh->fn = NULL;
	kh->arg = NULL;
    }
    if (kh->ev_timeout != NULL)
	event_release(kh->ev_timeout);
    kh->ev_timeout = NULL;

    if (handleq.qlength == 0 && accept_fn == NULL &&
	ev_netfd != NULL) {
	event_release(ev_netfd);
	ev_netfd = NULL;
    }
}

/*
 * Create the server end of a stream.  For krb4, this means setup a tcp
 * socket for receiving a connection.
 */
static void *
krb4_stream_server(
    void *	h)
{
    struct krb4_handle *kh = h;
    struct krb4_stream *ks;

    assert(kh != NULL);

    ks = alloc(SIZEOF(*ks));
    security_streaminit(&ks->secstr, &krb4_security_driver);
    ks->socket = stream_server(&ks->port, STREAM_BUFSIZE, STREAM_BUFSIZE, 1);
    if (ks->socket < 0) {
	security_seterror(&kh->sech,
	    _("can't create server stream: %s"), strerror(errno));
	amfree(ks);
	return (NULL);
    }
    ks->fd = -1;
    ks->krb4_handle = kh;
    ks->ev_read = NULL;
    return (ks);
}

/*
 * Accept an incoming connection on a stream_server socket
 */
static int
krb4_stream_accept(
    void *	s)
{
    struct krb4_stream *ks = s;
    struct krb4_handle *kh;

    assert(ks != NULL);
    kh = ks->krb4_handle;
    assert(kh != NULL);
    assert(ks->socket >= 0);
    assert(ks->fd == -1);

    ks->fd = stream_accept(ks->socket, 30, STREAM_BUFSIZE, STREAM_BUFSIZE);
    if (ks->fd < 0) {
	security_stream_seterror(&ks->secstr,
	    _("can't accept new stream connection: %s"), strerror(errno));
	return (-1);
    }
    return (0);
}

/*
 * Return a connected stream.
 */
static void *
krb4_stream_client(
    void *	h,
    int		id)
{
    struct krb4_handle *kh = h;
    struct krb4_stream *ks;

    assert(kh != NULL);

    ks = alloc(SIZEOF(*ks));
    security_streaminit(&ks->secstr, &krb4_security_driver);
    ks->fd = stream_client(kh->hostname, id, STREAM_BUFSIZE, STREAM_BUFSIZE,
	&ks->port, 0);
    if (ks->fd < 0) {
	security_seterror(&kh->sech,
	    _("can't connect stream to %s port %d: %s"), kh->hostname, id,
	    strerror(errno));
	amfree(ks);
	return (NULL);
    }

    ks->socket = -1;	/* we're a client */
    ks->krb4_handle = kh;
    ks->ev_read = NULL;
    return (ks);
}

/*
 * Close and unallocate resources for a stream.
 */
static void
krb4_stream_close(
    void *	s)
{
    struct krb4_stream *ks = s;

    assert(ks != NULL);

    if (ks->fd != -1)
	aclose(ks->fd);
    if (ks->socket != -1)
	aclose(ks->socket);
    krb4_stream_read_cancel(ks);
    amfree(ks);
}

/*
 * Authenticate a stream
 *
 * XXX this whole thing assumes the size of struct timeval is consistent,
 * which is may not be!  We need to extract the network byte order data
 * into byte arrays and send those.
 */
static int
krb4_stream_auth(
    void *	s)
{
    struct krb4_stream *ks = s;
    struct krb4_handle *kh;
    int fd;
    struct timeval local, enc;
    struct timezone tz;

    assert(ks != NULL);
    assert(ks->fd >= 0);

    fd = ks->fd;
    kh = ks->krb4_handle;

    /* make sure we're open */
    assert(fd >= 0);

    /* make sure our timeval is what we're expecting, see above */
    assert(SIZEOF(struct timeval) == 8);

    /*
     * Get the current time, put it in network byte order, encrypt it
     * and present it to the other side.
     */
    gettimeofday(&local, &tz);
    enc.tv_sec = (long)htonl((guint32)local.tv_sec);
    enc.tv_usec = (long)htonl((guint32)local.tv_usec);
    encrypt_data(&enc, SIZEOF(enc), &kh->session_key);
    if (knet_write(fd, &enc, SIZEOF(enc)) < 0) {
	security_stream_seterror(&ks->secstr,
	    _("krb4 stream handshake write error: %s"), strerror(errno));
	return (-1);
    }

    /*
     * Read back the other side's presentation.  Increment the seconds
     * and useconds by one.  Reencrypt, and present to the other side.
     * Timeout in 10 seconds.
     */
    if (knet_read(fd, &enc, SIZEOF(enc), 60) < 0) {
	security_stream_seterror(&ks->secstr,
	    _("krb4 stream handshake read error: %s"), strerror(errno));
	return (-1);
    }
    decrypt_data(&enc, SIZEOF(enc), &kh->session_key);
    /* XXX do timestamp checking here */
    enc.tv_sec = (long)htonl(ntohl((guint32)enc.tv_sec) + 1);
    enc.tv_usec =(long)htonl(ntohl((guint32)enc.tv_usec) + 1);
    encrypt_data(&enc, SIZEOF(enc), &kh->session_key);

    if (knet_write(fd, &enc, SIZEOF(enc)) < 0) {
	security_stream_seterror(&ks->secstr,
	    _("krb4 stream handshake write error: %s"), strerror(errno));
	return (-1);
    }

    /*
     * Read back the other side's processing of our data.
     * If they incremented it properly, then succeed.
     * Timeout in 10 seconds.
     */
    if (knet_read(fd, &enc, SIZEOF(enc), 60) < 0) {
	security_stream_seterror(&ks->secstr,
	    _("krb4 stream handshake read error: %s"), strerror(errno));
	return (-1);
    }
    decrypt_data(&enc, SIZEOF(enc), &kh->session_key);
    if ((ntohl((guint32)enc.tv_sec)  == (uint32_t)(local.tv_sec + 1)) &&
	(ntohl((guint32)enc.tv_usec) == (uint32_t)(local.tv_usec + 1)))
	    return (0);

    security_stream_seterror(&ks->secstr,
	_("krb4 handshake failed: sent %ld,%ld - recv %ld,%ld"),
	    (long)(local.tv_sec + 1), (long)(local.tv_usec + 1),
	    (long)ntohl((guint32)enc.tv_sec),
	    (long)ntohl((guint32)enc.tv_usec));
    return (-1);
}

/*
 * Returns the stream id for this stream.  This is just the local
 * port.
 */
static int
krb4_stream_id(
    void *	s)
{
    struct krb4_stream *ks = s;

    assert(ks != NULL);

    return (ks->port);
}

/*
 * Write a chunk of data to a stream.  Blocks until completion.
 */
static int
krb4_stream_write(
    void *	s,
    const void *buf,
    size_t	size)
{
    struct krb4_stream *ks = s;

    assert(ks != NULL);

    if (knet_write(ks->fd, buf, size) < 0) {
	security_stream_seterror(&ks->secstr,
	    _("write error on stream %d: %s"), ks->fd, strerror(errno));
	return (-1);
    }
    return (0);
}

/*
 * Submit a request to read some data.  Calls back with the given
 * function and arg when completed.
 */
static void
krb4_stream_read(
    void *	s,
    void	(*fn)(void *, void *, ssize_t),
    void *	arg)
{
    struct krb4_stream *ks = s;

    assert(ks != NULL);

    /*
     * Only one read request can be active per stream.
     */
    if (ks->ev_read != NULL)
	event_release(ks->ev_read);

    ks->ev_read = event_register((event_id_t)ks->fd, EV_READFD,
			stream_read_callback, ks);
    ks->fn = fn;
    ks->arg = arg;
}

/*
 * Write a chunk of data to a stream.  Blocks until completion.
 */
static ssize_t
krb4_stream_read_sync(
    void *	s,
    void **	buf)
{
    struct krb4_stream *ks = s;

    (void)buf;	/* Quiet unused variable warning */
    assert(ks != NULL);

    if (ks->ev_read != NULL)
	event_release(ks->ev_read);

    ks->ev_read = event_register((event_id_t)ks->fd, EV_READFD,
			stream_read_sync_callback, ks);
    event_wait(ks->ev_read);
    return((ssize_t)ks->len);
}

/*
 * Callback for krb4_stream_read_sync
 */
static void
stream_read_sync_callback(
    void *	arg)
{
    struct krb4_stream *ks = arg;
    ssize_t n;

    assert(ks != NULL);
    assert(ks->fd != -1);

    /*
     * Remove the event first, and then call the callback.
     * We remove it first because we don't want to get in their
     * way if they reschedule it.
     */
    krb4_stream_read_cancel(ks);
    n = read(ks->fd, ks->databuf, sizeof(ks->databuf));
    if (n < 0)
	security_stream_seterror(&ks->secstr,
	    strerror(errno));
    ks->len = (int)n;
}

/*
 * Cancel a previous stream read request.  It's ok if we didn't have a read
 * scheduled.
 */
static void
krb4_stream_read_cancel(
    void *	s)
{
    struct krb4_stream *ks = s;

    assert(ks != NULL);

    if (ks->ev_read != NULL) {
	event_release(ks->ev_read);
	ks->ev_read = NULL;
    }
}

/*
 * Callback for krb4_stream_read
 */
static void
stream_read_callback(
    void *	arg)
{
    struct krb4_stream *ks = arg;
    ssize_t n;

    assert(ks != NULL);
    assert(ks->fd != -1);

    /*
     * Remove the event first, and then call the callback.
     * We remove it first because we don't want to get in their
     * way if they reschedule it.
     */
    krb4_stream_read_cancel(ks);
    n = read(ks->fd, ks->databuf, SIZEOF(ks->databuf));
    if (n < 0)
	security_stream_seterror(&ks->secstr,
	    strerror(errno));
    (*ks->fn)(ks->arg, ks->databuf, n);
}

/*
 * The callback for recvpkt() for the event handler
 * Determines if this packet is for this security handle,
 * and does the real callback if so.
 */
static void
recvpkt_callback(
    void *	cookie)
{
    char handle[32];
    struct sockaddr_in6 peer;
    pkt_t pkt;
    int sequence;
    struct krb4_handle *kh;
    struct hostent *he;
    void (*fn)(void *, pkt_t *, security_status_t);
    void *arg;

    (void)cookie;		/* Quiet unused parameter warning */
    assert(cookie == NULL);

    /*
     * Find the handle that this packet is associated with.  We
     * need to peek at the packet to see what is in it, but we
     * want to save the actual reading for later.
     */
    dgram_zero(&netfd);
    if (dgram_recv(&netfd, 0, &peer) < 0)
	return;
    if (str2kpkthdr(netfd.cur, &pkt, handle, SIZEOF(handle), &sequence) < 0)
	return;

    for (kh = handleq_first(); kh != NULL; kh = handleq_next(kh)) {
	if (strcmp(kh->proto_handle, handle) == 0 &&
	    cmp_sockaddr(&kh->peer, &peer, 0) == 0) {
	    kh->sequence = sequence;

	    /*
	     * We need to cancel the recvpkt request before calling
	     * the callback because the callback may reschedule us.
	     */
	    fn = kh->fn;
	    arg = kh->arg;
	    krb4_recvpkt_cancel(kh);
	    if (recv_security_ok(kh, &pkt) < 0)
		(*fn)(arg, NULL, S_ERROR);
	    else
		(*fn)(arg, &pkt, S_OK);
	    return;
	}
    }
    /*
     * If we didn't find a handle, then check for a new incoming packet.
     * If no accept handler was setup, then just return.
     */
    if (accept_fn == NULL)
	return;

    he = gethostbyaddr((void *)&peer.sin6_addr, SIZEOF(peer.sin6_addr), AF_INET6);
    if (he == NULL)
	return;
    kh = alloc(SIZEOF(*kh));
    security_handleinit(&kh->sech, &krb4_security_driver);
    inithandle(kh, he, (int)peer.sin6_port, handle);

    /*
     * Check the security of the packet.  If it is bad, then pass NULL
     * to the accept function instead of a packet.
     */
    if (recv_security_ok(kh, &pkt) < 0)
	(*accept_fn)(&kh->sech, NULL);
    else
	(*accept_fn)(&kh->sech, &pkt);
}

/*
 * This is called when a handle times out before receiving a packet.
 */
static void
recvpkt_timeout(
    void *	cookie)
{
    struct krb4_handle *kh = cookie;
    void (*fn)(void *, pkt_t *, security_status_t);
    void *arg;

    assert(kh != NULL);

    assert(kh->ev_timeout != NULL);
    fn = kh->fn;
    arg = kh->arg;
    krb4_recvpkt_cancel(kh);
    (*fn)(arg, NULL, S_TIMEOUT);
}

/*
 * Add a ticket to the message
 */
static int
add_ticket(
    struct krb4_handle *kh,
    const pkt_t *	pkt,
    dgram_t *		msg)
{
    char inst[INST_SZ];
    KTEXT_ST ticket;
    char *security;
    int rc;

    kh->cksum = (long)krb4_cksum(pkt->body);
#if CLIENT_HOST_INSTANCE == HOSTNAME_INSTANCE
    /*
     * User requested that all instances be based on the target
     * hostname.
     */
    strncpy(inst, kh->inst, SIZEOF(inst) - 1);
#else
    /*
     * User requested a fixed instance.
     */
    strncpy(inst, CLIENT_HOST_INSTANCE, SIZEOF(inst) - 1);
#endif
    inst[SIZEOF(inst) - 1] = '\0';

    /*
     * Get a ticket with the user-defined service and instance,
     * and using the checksum of the body of the request packet.
     */
    rc = krb_mk_req(&ticket, CLIENT_HOST_PRINCIPAL, inst, kh->realm,
	kh->cksum);
    if (rc == NO_TKT_FIL) {
	/* It's been kdestroyed.  Get a new one and try again */
	get_tgt();
	rc = krb_mk_req(&ticket, CLIENT_HOST_PRINCIPAL, inst, kh->realm,
	    kh->cksum);
    }
    if (rc != 0) {
	security_seterror(&kh->sech,
	    _("krb_mk_req failed: %s (%d)"), error_message(rc), rc);
	return (-1);
    }
    /*
     * We now have the ticket.  Put it into the packet, and send
     * it.
     */
    security = vstralloc("SECURITY TICKET ",
	bin2astr(ticket.dat, ticket.length), "\n", NULL);
    dgram_cat(msg, security);
    amfree(security);

    return (0);
}

/*
 * Add the mutual authenticator.  This is the checksum from
 * the req, + 1
 */
static void
add_mutual_auth(
    struct krb4_handle *kh,
    dgram_t *		msg)
{
    union mutual mutual;
    char *security;

    assert(kh != NULL);
    assert(msg != NULL);
    assert(kh->cksum != 0);
    assert(kh->session_key[0] != '\0');

    memset(&mutual, 0, SIZEOF(mutual));
    mutual.cksum = (unsigned long)htonl((guint32)kh->cksum + 1);
    encrypt_data(&mutual, SIZEOF(mutual), &kh->session_key);

    security = vstralloc("SECURITY MUTUAL-AUTH ",
	bin2astr((unsigned char *)mutual.pad,
			(int)sizeof(mutual.pad)), "\n", NULL);
    dgram_cat(msg, security);
    amfree(security);
}

/*
 * Check the security of a received packet.  Returns negative on error
 * or security violation and otherwise returns 0 and fills in the
 * passed packet.
 */
static int
recv_security_ok(
    struct krb4_handle *kh,
    pkt_t *		pkt)
{
    char *tok, *security, *body;
    unsigned long cksum;

    assert(kh != NULL);
    assert(pkt != NULL);

    /*
     * Set this preemptively before we mangle the body.
     */
    security_seterror(&kh->sech,
	_("bad %s SECURITY line from %s: '%s'"), pkt_type2str(pkt->type),
	kh->hostname, pkt->body);


    /*
     * The first part of the body should be the security info.  Deal with it.
     * We only need to do this on a few packet types.
     *
     * First, parse the SECURITY line in the packet, if it exists.
     * Increment the cur pointer past it to the data section after
     * parsing is finished.
     */
    if (strncmp(pkt->body, "SECURITY", SIZEOF("SECURITY") - 1) == 0) {
	tok = strtok(pkt->body, " ");
	assert(strcmp(tok, "SECURITY") == 0);
	/* security info goes until the newline */
	security = strtok(NULL, "\n");
	body = strtok(NULL, "");
	/*
	 * If the body is f-ked, then try to recover.
	 */
	if (body == NULL) {
	    if (security != NULL)
		body = security + strlen(security) + 2;
	    else
		body = pkt->body;
	}
    } else {
	security = NULL;
	body = pkt->body;
    }

    /*
     * Get a checksum of the non-security parts of the body
     */
    cksum = krb4_cksum(body);

    /*
     * Now deal with the data we did (or didn't) parse above
     */
    switch (pkt->type) {
    case P_REQ:
	/*
	 * Request packets have a ticket after the security tag
	 * Get the ticket, make sure the checksum agrees with the
	 * checksum of the request we sent.
	 *
	 * Must look like: SECURITY TICKET [ticket str]
	 */

	/* there must be some security info */
	if (security == NULL)
	    return (-1);

	/* second word must be TICKET */
	if ((tok = strtok(security, " ")) == NULL)
	    return (-1);
	if (strcmp(tok, "TICKET") != 0) {
	    security_seterror(&kh->sech,
		_("REQ SECURITY line parse error, expecting TICKET, got %s"), tok);
	    return (-1);
	}

	/* the third word is the encoded ticket */
	if ((tok = strtok(NULL, "")) == NULL)
	    return (-1);
	if (check_ticket(kh, pkt, tok, cksum) < 0)
	    return (-1);

	/* we're good to go */
	break;

    case P_REP:
	/*
	 * Reply packets check the mutual authenticator for this host.
	 *
	 * Must look like: SECURITY MUTUAL-AUTH [mutual auth str]
	 */
	if (security == NULL)
	    return (-1);
	if ((tok = strtok(security, " ")) == NULL)
	    return (-1);
	if (strcmp(tok, "MUTUAL-AUTH") != 0)  {
	    security_seterror(&kh->sech,
		"REP SECURITY line parse error, expecting MUTUAL-AUTH, got %s",
		tok);
	    return (-1);
	}
	if ((tok = strtok(NULL, "")) == NULL)
	    return (-1);
	if (check_mutual_auth(kh, tok) < 0)
	    return (-1);

    case P_ACK:
    case P_NAK:
    default:
	/*
	 * These packets have no security.  They should, but such
	 * is life.  We can't change it without breaking compatibility.
	 *
	 * XXX Should we complain if argc > 0? (ie, some security info was
	 * sent?)
	 */
	break;

    }

    /*
     * If there is security info at the front of the packet, we need
     * to shift the rest of the data up and nuke it.
     */
    if (body != pkt->body)
	memmove(pkt->body, body, strlen(body) + 1);
    return (0);
}

/*
 * Check the ticket in a REQ packet for authenticity
 */
static int
check_ticket(
    struct krb4_handle *kh,
    const pkt_t *	pkt,
    const char *	ticket_str,
    unsigned long	cksum)
{
    char inst[INST_SZ];
    KTEXT_ST ticket;
    AUTH_DAT auth;
    struct passwd *pwd;
    char *user;
    int rc;

    (void)pkt;		/* Quiet unused parameter warning */

    assert(kh != NULL);
    assert(pkt != NULL);
    assert(ticket_str != NULL);

    ticket.length = (int)sizeof(ticket.dat);
    astr2bin((unsigned char *)ticket_str, ticket.dat, &ticket.length);
    assert(ticket.length > 0);

    /* get a copy of the instance into writable memory */
#if CLIENT_HOST_INSTANCE == HOSTNAME_INSTANCE
    strncpy(inst, krb_get_phost(hostname), SIZEOF(inst) - 1);
#else
    strncpy(inst, CLIENT_HOST_INSTANCE, SIZEOF(inst) - 1);
#endif
    inst[SIZEOF(inst) - 1] = '\0';

    /* get the checksum out of the ticket */
    rc = krb_rd_req(&ticket, CLIENT_HOST_PRINCIPAL, inst,
	kh->peer.sin6_addr.s_addr, &auth, CLIENT_HOST_KEY_FILE);
    if (rc != 0) {
	security_seterror(&kh->sech,
	    _("krb_rd_req failed for %s: %s (%d)"), kh->hostname,
	    error_message(rc), rc);
	return (-1);
    }

    /* verify and save the checksum and session key */
    if (auth.checksum != cksum) {
	security_seterror(&kh->sech,
	    _("krb4 checksum mismatch for %s (remote=%lu, local=%lu)"),
	    kh->hostname, (long)auth.checksum, cksum);
	return (-1);
    }
    kh->cksum = (unsigned long)cksum;
    memcpy(kh->session_key, auth.session, SIZEOF(kh->session_key));

    /*
     * If CHECK_USERID is set, then we need to specifically
     * check the userid we're forcing ourself to.  Otherwise,
     * just check the login we're currently setuid to.
     */
#ifdef CHECK_USERID
    if ((pwd = getpwnam(CLIENT_LOGIN)) == NULL)
	error(_("error [getpwnam(%s) fails]"), CLIENT_LOGIN);
#else
    if ((pwd = getpwuid(getuid())) == NULL)
	error(_("error  [getpwuid(%d) fails]"), getuid());
#endif

    /* save the username in case it's overwritten */
    user = stralloc(pwd->pw_name);

    /* check the klogin file */
    if (kuserok(&auth, user)) {
	security_seterror(&kh->sech,
	    _("access as %s not allowed from %s.%s@%s"), user, auth.pname,
	    auth.pinst, auth.prealm);
	amfree(user);
	return (-1);
    }
    amfree(user);

    /* it's good */
    return (0);
}

/*
 * Verify that the packet received is secure by verifying that it has
 * the same checksum as our request + 1.
 */
static int
check_mutual_auth(
    struct krb4_handle *kh,
    const char *	mutual_auth_str)
{
    union mutual mutual;
    int len;

    assert(kh != NULL);
    assert(mutual_auth_str != NULL);
    assert(kh->inst[0] != '\0');
    /* we had better have a checksum from a request we sent */
    assert(kh->cksum != 0);

    /* convert the encoded string into binary data */
    len = (int)sizeof(mutual);
    astr2bin((unsigned char *)mutual_auth_str, (unsigned char *)&mutual, &len);

    /* unencrypt the string using the key in the ticket file */
    host2key(kh->hostname, kh->inst, &kh->session_key);
    decrypt_data(&mutual, (size_t)len, &kh->session_key);
    mutual.cksum = (unsigned long)ntohl((guint32)mutual.cksum);

    /* the data must be the same as our request cksum + 1 */
    if (mutual.cksum != (kh->cksum + 1)) {
	security_seterror(&kh->sech,
	    _("krb4 checksum mismatch from %s (remote=%lu, local=%lu)"),
	    kh->hostname, mutual.cksum, kh->cksum + 1);
	return (-1);
    }

    return (0);
}

/*
 * Convert a pkt_t into a header string for our packet
 */
static const char *
kpkthdr2str(
    const struct krb4_handle *	kh,
    const pkt_t *		pkt)
{
    static char retbuf[256];

    assert(kh != NULL);
    assert(pkt != NULL);

    g_snprintf(retbuf, SIZEOF(retbuf), "Amanda %d.%d %s HANDLE %s SEQ %d\n",
	VERSION_MAJOR, VERSION_MINOR, pkt_type2str(pkt->type),
	kh->proto_handle, kh->sequence);

    /* check for truncation.  If only we had asprintf()... */
    assert(retbuf[strlen(retbuf) - 1] == '\n');

    return (retbuf);
}

/*
 * Parses out the header line in 'str' into the pkt and handle
 * Returns negative on parse error.
 */
static int
str2kpkthdr(
    const char *origstr,
    pkt_t *	pkt,
    char *	handle,
    size_t	handlesize,
    int *	sequence)
{
    char *str;
    const char *tok;

    assert(origstr != NULL);
    assert(pkt != NULL);

    str = stralloc(origstr);

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
    pkt_init(pkt, pkt_str2type(tok), "");
    if (pkt->type == (pktype_t)-1)
	goto parse_error;

    /* Read in "HANDLE" */
    if ((tok = strtok(NULL, " ")) == NULL || strcmp(tok, "HANDLE") != 0)
	goto parse_error;

    /* parse the handle */
    if ((tok = strtok(NULL, " ")) == NULL)
	goto parse_error;
    strncpy(handle, tok, handlesize - 1);
    handle[handlesize - 1] = '\0';

    /* Read in "SEQ" */
    if ((tok = strtok(NULL, " ")) == NULL || strcmp(tok, "SEQ") != 0)
	goto parse_error;

    /* parse the sequence number */
    if ((tok = strtok(NULL, "\n")) == NULL)
	goto parse_error;
    *sequence = atoi(tok);

    /* Save the body, if any */
    if ((tok = strtok(NULL, "")) != NULL)
	pkt_cat(pkt, tok);

    amfree(str);
    return (0);

parse_error:
#if 0 /* XXX we have no way of passing this back up */
    security_seterror(&kh->sech,
	_("parse error in packet header : '%s'"), origstr);
#endif
    amfree(str);
    return (-1);
}

static void
host2key(
    const char *hostname,
    const char *inst,
    des_cblock *key)
{
    char realm[256];
    CREDENTIALS cred;

    strncpy(realm, krb_realmofhost((char *)hostname), SIZEOF(realm) - 1);
    realm[SIZEOF(realm) - 1] = '\0';
#if CLIENT_HOST_INSTANCE != HOSTNAME_INSTANCE
    inst = CLIENT_HOST_INSTANCE
#endif
    krb_get_cred(CLIENT_HOST_PRINCIPAL, (char *)inst, realm, &cred);
    memcpy(key, cred.session, SIZEOF(des_cblock));
}


/*
 * Convert a chunk of data into a string.
 */
static const char *
bin2astr(
    const unsigned char *buf,
    int			len)
{
    static const char tohex[] = "0123456789ABCDEF";
    static char *str = NULL;
    char *q;
    const unsigned char *p;
    size_t slen;
    int	i;

    /*
     * calculate output string len
     * We quote everything, so each input byte == 3 output chars, plus
     * two more for quotes
     */
    slen = ((size_t)len * 3) + 2;

    /* allocate string and fill it in */
    if (str != NULL)
	amfree(str);
    str = alloc(slen + 1);
    p = buf;
    q = str;
    *q++ = '"';
    for (i = 0; i < len; i++) {
	*q++ = '$';
	*q++ = tohex[(*p >> 4) & 0xF];
	*q++ = tohex[*p & 0xF];
	p++;
    }
    *q++ = '"';
    *q = '\0';

    /* make sure we didn't overrun our allocated buffer */
    assert((size_t)(q - str) == slen);

    return (str);
}

/*
 * Convert an encoded string into a block of data bytes
 */
static void
astr2bin(
    const unsigned char *astr,
    unsigned char *	buf,
    int *		lenp)
{
    const unsigned char *p;
    unsigned char *q;
#define fromhex(h)	(isdigit((int)h) ? (h) - '0' : (h) - 'A' + 10)

    /*
     * Skip leading quote, if any
     */
    if (*astr == '"')
	astr++;

    /*
     * Run through the string.  Anything starting with a $ is a three
     * char representation of this byte.  Everything else is literal.
     */
    for (p = astr, q = buf; *p != '"' && *p != '\0'; ) {
	if (*p != '$') {
	    *q++ = *p++;
	} else {
	    *q++ = (fromhex(p[1]) << 4) + fromhex(p[2]);
	     p += 3;
	}
	if ((int)(q - buf) >= *lenp)
	    break;
    }
    *lenp = q - buf;
}

static unsigned long
krb4_cksum(
    const char *str)
{
    des_cblock seed;

    memset(seed, 0, SIZEOF(seed));
    /*
     * The first arg is an unsigned char * in some krb4 implementations,
     * and in others, it's a des_cblock *.  Just make it void here
     * to shut them all up.
     */
    return (quad_cksum((void *)str, NULL, (long)strlen(str), 1, &seed));
}

static void
krb4_getinst(
    const char *hname,
    char *	inst,
    size_t	size)
{

    /*
     * Copy the first part of the hostname up to the first '.' into
     * the buffer provided.  Leave room for a NULL.
     */
    while (size > 1 && *hname != '\0' && *hname != '.') {
	*inst++ = isupper((int)*hname) ? tolower((int)*hname) : *hname;
	hname++;
	size--;
    }
    *inst = '\0';
}

static void
encrypt_data(
    void *	data,
    size_t	length,
    des_cblock *key)
{
    des_key_schedule sched;

    /*
     * First arg is des_cblock * in some places, and just des_cblock in
     * others.  Since des_cblock is a char array, they are equivalent.
     * Just cast it to void * to keep both compilers quiet.  typedefing
     * arrays should be outlawed.
     */
    des_key_sched((void *)key, sched);
    des_pcbc_encrypt(data, data, (long)length, sched, key, DES_ENCRYPT);
}


static void
decrypt_data(
    void *	data,
    size_t	length,
    des_cblock *key)
{
    des_key_schedule sched;

    des_key_sched((void *)key, sched);
    des_pcbc_encrypt(data, data, (long)length, sched, key, DES_DECRYPT);
}

/*
 * like write(), but always writes out the entire buffer.
 */
static int
knet_write(
    int		fd,
    const void *vbuf,
    size_t	size)
{
    const char *buf = vbuf;	/* so we can do ptr arith */
    ssize_t n;

    while (size > 0) {
	n = write(fd, buf, size);
	if (n < 0)
	    return (-1);
	buf += n;
	size -= n;
    }
    return (0);
}

/*
 * Like read(), but waits until the entire buffer has been filled.
 */
static int
knet_read(
    int		fd,
    void *	vbuf,
    size_t	size,
    int		timeout)
{
    char *buf = vbuf;	/* ptr arith */
    ssize_t n;
    int neof = 0;
    SELECT_ARG_TYPE readfds;
    struct timeval tv;

    while (size > 0) {
	FD_ZERO(&readfds);
	FD_SET(fd, &readfds);
	tv.tv_sec = timeout;
	tv.tv_usec = 0;
	switch (select(fd + 1, &readfds, NULL, NULL, &tv)) {
	case 0:
	    errno = ETIMEDOUT;
	    /* FALLTHROUGH */
	case -1:
	    return (-1);
	case 1:
	    assert(FD_ISSET(fd, &readfds));
	    break;
	default:
	    assert(0);
	    break;
	}
	n = read(fd, buf, size);
	if (n < 0)
	    return (-1);
	/* we only tolerate so many eof's */
	if (n == 0 && ++neof > 1024) {
	    errno = EIO;
	    return (-1);
	}
	buf += n;
	size -= n;
    }
    return (0);
}

#if 0
/* -------------------------- */
/* debug routines */

static void
print_hex(
    const char *		str,
    const unsigned char *	buf,
    size_t			len)
{
    int i;

    dbprintf("%s:", str);
    for(i=0;i<len;i++) {
	if(i%25 == 0)
		dbprintf("\n");
	dbprintf(" %02X", buf[i]);
    }
    dbprintf("\n");
}

static void
print_ticket(
    const char *str,
    KTEXT	tktp)
{
    dbprintf(_("%s: length %d chk %lX\n"), str, tktp->length, tktp->mbz);
    print_hex(_("ticket data"), tktp->dat, tktp->length);
    fflush(stdout);
}

static void
print_auth(
    AUTH_DAT *authp)
{
    g_printf("\nAuth Data:\n");
    g_printf("  Principal \"%s\" Instance \"%s\" Realm \"%s\"\n",
	   authp->pname, authp->pinst, authp->prealm);
    g_printf("  cksum %d life %d keylen %ld\n", authp->checksum,
	   authp->life, SIZEOF(authp->session));
    print_hex("session key", authp->session, SIZEOF(authp->session));
    fflush(stdout);
}

static void
print_credentials(
    CREDENTIALS *credp)
{
    g_printf("\nCredentials:\n");
    g_printf("  service \"%s\" instance \"%s\" realm \"%s\" life %d kvno %d\n",
	   credp->service, credp->instance, credp->realm, credp->lifetime,
	   credp->kvno);
    print_hex("session key", credp->session, SIZEOF(credp->session));
    print_hex("ticket", credp->ticket_st.dat, credp->ticket_st.length);
    fflush(stdout);
}
#endif
