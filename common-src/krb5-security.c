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
 * $Id: krb5-security.c,v 1.22 2006/06/16 10:55:05 martinea Exp $
 *
 * krb5-security.c - kerberos V5 security module
 */

#include "config.h"
#ifdef KRB5_SECURITY
#include "amanda.h"
#include "util.h"
#include "arglist.h"
#include "event.h"
#include "packet.h"
#include "queue.h"
#include "security.h"
#include "security-util.h"
#include "stream.h"
#include "version.h"

#define	BROKEN_MEMORY_CCACHE

#ifdef BROKEN_MEMORY_CCACHE
/*
 * If you don't have atexit() or on_exit(), you could just consider
 * making atexit() empty and clean up your ticket files some other
 * way
 */
#ifndef HAVE_ATEXIT
#ifdef HAVE_ON_EXIT
#define atexit(func)    on_exit(func, 0)
#else
#define atexit(func)    (you must to resolve lack of atexit)
#endif  /* HAVE_ON_EXIT */
#endif  /* ! HAVE_ATEXIT */
#endif

#ifndef KRB5_HEIMDAL_INCLUDES
#include <gssapi/gssapi_generic.h>
#else
#include <gssapi/gssapi.h>
#endif
#include <krb5.h>

#ifndef KRB5_ENV_CCNAME
#define	KRB5_ENV_CCNAME	"KRB5CCNAME"
#endif

/*#define	KRB5_DEBUG*/

#ifdef KRB5_DEBUG
#define	k5printf(x)	dbprintf(x)
#else
#define	k5printf(x)
#endif

/*
 * consider undefining when kdestroy() is fixed.  The current version does
 * not work under krb5-1.2.4 in rh7.3, perhaps others.
 */
#define KDESTROY_VIA_UNLINK	1

/*
 * Define this if you want all network traffic encrypted.  This will
 * extract a serious performance hit.
 *
 * It would be nice if we could do this on a filesystem-by-filesystem basis.
 */
/*#define	AMANDA_KRB5_ENCRYPT*/

/*
 * Where the keytab lives, if defined.  Otherwise it expects something in the
 * config file.
 */
/* #define AMANDA_KEYTAB	"/.amanda-v5-keytab" */

/*
 * The name of the principal we authenticate with, if defined.  Otherwise
 * it expects something in the config file.
 */
/* #define	AMANDA_PRINCIPAL	"service/amanda" */

/*
 * The lifetime of our tickets in seconds.  This may or may not need to be
 * configurable.
 */
#define	AMANDA_TKT_LIFETIME	(12*60*60)

/*
 * The name of the service in /etc/services.  This probably shouldn't be
 * configurable.
 */
#define	AMANDA_KRB5_SERVICE_NAME	"k5amanda"

/*
 * The default port to use if above entry in /etc/services doesn't exist
 */
#define	AMANDA_KRB5_DEFAULT_PORT	10082

/*
 * The timeout in seconds for each step of the GSS negotiation phase
 */
#define	GSS_TIMEOUT			30

/*
 * The largest buffer we can send/receive.
 */
#define	AMANDA_MAX_TOK_SIZE		(MAX_TAPE_BLOCK_BYTES * 4)

/*
 * Magic values for krb5_conn->handle
 */
#define	H_EOF	-1		/* this connection has been shut down */

/*
 * This is the tcp stream buffer size
 */
#define	KRB5_STREAM_BUFSIZE	(MAX_TAPE_BLOCK_BYTES * 2)

/*
 * This is the max number of outgoing connections we can have at once.
 * planner/amcheck/etc will open a bunch of connections as it tries
 * to contact everything.  We need to limit this to avoid blowing
 * the max number of open file descriptors a process can have.
 */
#define	AMANDA_KRB5_MAXCONN	40

/*
 * This is a frame read off of the connection.  Each frame has an
 * associated handle and a gss_buffer which contains a len,value pair.
 */
struct krb5_frame {
    int handle;				/* proto handle */
    gss_buffer_desc tok;		/* token */
    TAILQ_ENTRY(krb5_frame) tq;		/* queue handle */
};

/*
 * This is a krb5 connection to a host.  We should only have
 * one connection per host.
 */
struct krb5_conn {
    int fd;					/* tcp connection */
    struct {					/* buffer read() calls */
	char buf[KRB5_STREAM_BUFSIZE];		/* buffer */
	size_t left;			/* unread data */
	ssize_t size;			/* size of last read */
    } readbuf;
    enum { unauthed, authed } state;
    event_handle_t *ev_read;			/* read (EV_READFD) handle */
    int ev_read_refcnt;				/* number of readers */
    char hostname[MAX_HOSTNAME_LENGTH+1];	/* human form of above */
    char *errmsg;				/* error passed up */
    gss_ctx_id_t gss_context;			/* GSSAPI context */
    int refcnt;					/* number of handles using */
    TAILQ_HEAD(, krb5_frame) frameq;		/* queue of read frames */
    TAILQ_ENTRY(krb5_conn) tq;			/* queue handle */
};


struct krb5_stream;

/*
 * This is the private handle data.
 */
struct krb5_handle {
    security_handle_t sech;		/* MUST be first */
    char *hostname;			/* ptr to kc->hostname */
    struct krb5_stream *ks;		/* virtual stream we xmit over */

    union {
	void (*recvpkt)(void *, pkt_t *, security_status_t);
					/* func to call when packet recvd */
	void (*connect)(void *, security_handle_t *, security_status_t);
					/* func to call when connected */
    } fn;
    void *arg;				/* argument to pass function */
    void *datap;			/* argument to pass function */
    event_handle_t *ev_wait;		/* wait handle for connects */
    char *(*conf_fn)(char *, void *); /* used to get config info */
    event_handle_t *ev_timeout;		/* timeout handle for recv */
};

/*
 * This is the internal security_stream data for krb5.
 */
struct krb5_stream {
    security_stream_t secstr;		/* MUST be first */
    struct krb5_conn *kc;		/* physical connection */
    int handle;				/* protocol handle */
    event_handle_t *ev_read;		/* read (EV_WAIT) event handle */
    void (*fn)(void *, void *, ssize_t);/* read event fn */
    void *arg;				/* arg for previous */
    char buf[KRB5_STREAM_BUFSIZE];
    ssize_t len;
};

/*
 * Interface functions
 */
static ssize_t	krb5_sendpkt(void *, pkt_t *);
static int	krb5_stream_accept(void *);
static int	krb5_stream_auth(void *);
static int	krb5_stream_id(void *);
static int	krb5_stream_write(void *, const void *, size_t);
static void *	krb5_stream_client(void *, int);
static void *	krb5_stream_server(void *);
static void	krb5_accept(const struct security_driver *, int, int,
			void (*)(security_handle_t *, pkt_t *));
static void	krb5_close(void *);
static void	krb5_connect(const char *, char *(*)(char *, void *),
			void (*)(void *, security_handle_t *, security_status_t),
			void *, void *);
static void	krb5_recvpkt(void *, void (*)(void *, pkt_t *, security_status_t),
			void *, int);
static void	krb5_recvpkt_cancel(void *);
static void	krb5_stream_close(void *);
static void	krb5_stream_read(void *, void (*)(void *, void *, ssize_t), void *);
static ssize_t	krb5_stream_read_sync(void *, void **);
static void	krb5_stream_read_cancel(void *);

/*
 * This is our interface to the outside world.
 */
const security_driver_t krb5_security_driver = {
    "krb5",
    krb5_connect,
    krb5_accept,
    krb5_close,
    krb5_sendpkt,
    krb5_recvpkt,
    krb5_recvpkt_cancel,
    krb5_stream_server,
    krb5_stream_accept,
    krb5_stream_client,
    krb5_stream_close,
    krb5_stream_auth,
    krb5_stream_id,
    krb5_stream_write,
    krb5_stream_read,
    krb5_stream_read_sync,
    krb5_stream_read_cancel,
    sec_close_connection_none,
};

/*
 * Cache the local hostname
 */
static char hostname[MAX_HOSTNAME_LENGTH+1];

/*
 * This is a queue of open connections
 */
static struct {
    TAILQ_HEAD(, krb5_conn) tailq;
    int qlength;
} krb5_connq = {
    TAILQ_HEAD_INITIALIZER(krb5_connq.tailq), 0
};
#define	krb5_connq_first()		TAILQ_FIRST(&krb5_connq.tailq)
#define	krb5_connq_next(kc)		TAILQ_NEXT(kc, tq)
#define	krb5_connq_append(kc)	do {					\
    TAILQ_INSERT_TAIL(&krb5_connq.tailq, kc, tq);				\
    krb5_connq.qlength++;							\
} while (0)
#define	krb5_connq_remove(kc)	do {					\
    assert(krb5_connq.qlength > 0);						\
    TAILQ_REMOVE(&krb5_connq.tailq, kc, tq);					\
    krb5_connq.qlength--;							\
} while (0)

static int newhandle = 1;

/*
 * This is a function that should be called if a new security_handle_t is
 * created.  If NULL, no new handles are created.
 * It is passed the new handle and the received pkt
 */
static void (*accept_fn)(security_handle_t *, pkt_t *);

/*
 * Local functions
 */
static void init(void);
#ifdef BROKEN_MEMORY_CCACHE
static void cleanup(void);
#endif
static const char *get_tgt(char *, char *);
static void	open_callback(void *);
static void	connect_callback(void *);
static void	connect_timeout(void *);
static int	send_token(struct krb5_conn *, int, const gss_buffer_desc *);
static ssize_t	recv_token(struct krb5_conn *, int *, gss_buffer_desc *, int);
static void	recvpkt_callback(void *, void *, ssize_t);
static void	recvpkt_timeout(void *);
static void	stream_read_callback(void *);
static void	stream_read_sync_callback2(void *, void *, ssize_t);
static int	gss_server(struct krb5_conn *);
static int	gss_client(struct krb5_handle *);
static const char *gss_error(OM_uint32, OM_uint32);

#ifdef AMANDA_KRB5_ENCRYPT
static int	kdecrypt(struct krb5_stream *, gss_buffer_desc *, gss_buffer_desc *);
static int	kencrypt(struct krb5_stream *, gss_buffer_desc *, gss_buffer_desc *);
#endif
static struct krb5_conn *conn_get(const char *);
static void	conn_put(struct krb5_conn *);
static void	conn_read(struct krb5_conn *);
static void	conn_read_cancel(struct krb5_conn *);
static void	conn_read_callback(void *);
static int	conn_run_frameq(struct krb5_conn *, struct krb5_stream *);
static char *	krb5_checkuser(char *, char *, char *);


/*
 * krb5 version of a security handle allocator.  Logically sets
 * up a network "connection".
 */
static void
krb5_connect(
    const char *hostname,
    char *	(*conf_fn)(char *, void *),
    void	(*fn)(void *, security_handle_t *, security_status_t),
    void *	arg,
    void *	datap)
{
    struct krb5_handle *kh;
    struct hostent *he;
    struct servent *se;
    int fd;
    int port;
    const char *err;
    char *keytab_name = NULL;
    char *principal_name = NULL;

    assert(hostname != NULL);

    k5printf(("krb5_connect: %s\n", hostname));

    /*
     * Make sure we're initted
     */
    init();

    kh = alloc(SIZEOF(*kh));
    security_handleinit(&kh->sech, &krb5_security_driver);
    kh->hostname = NULL;
    kh->ks = NULL;
    kh->ev_wait = NULL;
    kh->ev_timeout = NULL;

#ifdef AMANDA_KEYTAB
    keytab_name = AMANDA_KEYTAB;
#else
    if(conf_fn) {
    	keytab_name = conf_fn("krb5keytab", datap);
    }
#endif
#ifdef AMANDA_PRINCIPAL
    principal_name = AMANDA_PRINCIPAL;
#else
    if(conf_fn) {
    	principal_name = conf_fn("krb5principal", datap);
    }
#endif

    if ((err = get_tgt(keytab_name, principal_name)) != NULL) {
	security_seterror(&kh->sech, "%s: could not get TGT: %s",
	    hostname, err);
	(*fn)(arg, &kh->sech, S_ERROR);
	return;
    }

    if ((he = gethostbyname(hostname)) == NULL) {
	security_seterror(&kh->sech,
	    "%s: could not resolve hostname", hostname);
	(*fn)(arg, &kh->sech, S_ERROR);
	return;
    }
    kh->fn.connect = fn;
    kh->conf_fn = conf_fn;
    kh->arg = arg;
    kh->datap = datap;
    kh->hostname = stralloc(he->h_name);
    kh->ks = krb5_stream_client(kh, newhandle++);

    if (kh->ks == NULL)
	goto error;

    fd = kh->ks->kc->fd;

    if (fd < 0) {
	/*
	 * We need to open a new connection.  See if we have too
	 * many connections open.
	 */
	if (krb5_connq.qlength > AMANDA_KRB5_MAXCONN) {
	    k5printf(("krb5_connect: too many conections (%d), delaying %s\n",
		krb5_connq.qlength, kh->hostname));
	    krb5_stream_close(kh->ks);
	    kh->ev_wait = event_register((event_id_t)open_callback,
		EV_WAIT, open_callback, kh);
	    return;
	}

	if ((se = getservbyname(AMANDA_KRB5_SERVICE_NAME, "tcp")) == NULL)
	    port = htons(AMANDA_KRB5_DEFAULT_PORT);
	else
	    port = se->s_port;

	/*
	 * Get a non-blocking socket.
	 */
	fd = stream_client(kh->hostname, ntohs(port), KRB5_STREAM_BUFSIZE,
	    KRB5_STREAM_BUFSIZE, NULL, 1);
	if (fd < 0) {
	    security_seterror(&kh->sech,
		"can't connect to %s:%d: %s", hostname, ntohs(port),
		strerror(errno));
	    goto error;
	}
	kh->ks->kc->fd = fd;
    }
    /*
     * The socket will be opened async so hosts that are down won't
     * block everything.  We need to register a write event
     * so we will know when the socket comes alive.
     * We also register a timeout.
     */
    kh->ev_wait = event_register((event_id_t)fd, EV_WRITEFD,
		connect_callback, kh);
    kh->ev_timeout = event_register((event_id_t)GSS_TIMEOUT, EV_TIME,
		connect_timeout, kh);

    return;

error:
    (*fn)(arg, &kh->sech, S_ERROR);
}

/*
 * Called when there are not too many connections open such that
 * we can open more.
 */
static void
open_callback(
    void *	cookie)
{
    struct krb5_handle *kh = cookie;

    event_release(kh->ev_wait);

    k5printf(("krb5: open_callback: possible connections available, retry %s\n",
	kh->hostname));
    krb5_connect(kh->hostname, kh->conf_fn, kh->fn.connect, kh->arg,kh->datap);
    amfree(kh->hostname);
    amfree(kh);
}

/*
 * Called when a tcp connection is finished connecting and is ready
 * to be authenticated.
 */
static void
connect_callback(
    void *	cookie)
{
    struct krb5_handle *kh = cookie;

    event_release(kh->ev_wait);
    kh->ev_wait = NULL;
    event_release(kh->ev_timeout);
    kh->ev_timeout = NULL;

    if (kh->ks->kc->state == unauthed) {
	if (gss_client(kh) < 0) {
	    (*kh->fn.connect)(kh->arg, &kh->sech, S_ERROR);
	    return;
	}
	kh->ks->kc->state = authed;
    }
    assert(kh->ks->kc->gss_context != GSS_C_NO_CONTEXT);

    (*kh->fn.connect)(kh->arg, &kh->sech, S_OK);
}

/*
 * Called if a connection times out before completion.
 */
static void
connect_timeout(
    void *	cookie)
{
    struct krb5_handle *kh = cookie;

    event_release(kh->ev_wait);
    kh->ev_wait = NULL;
    event_release(kh->ev_timeout);
    kh->ev_timeout = NULL;

    (*kh->fn.connect)(kh->arg, &kh->sech, S_TIMEOUT);
}

/*
 * Setup to handle new incoming connections
 */
static void
krb5_accept(
    const struct security_driver *driver,
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *))
{
    struct sockaddr_in sin;
    socklen_t len;
    struct krb5_conn *kc;
    struct hostent *he;

    /*
     * Make sure we're initted
     */
    init();

    /* shut up compiler */
    driver=driver;
    out=out;

    len = SIZEOF(sin);
    if (getpeername(in, (struct sockaddr *)&sin, &len) < 0)
	return;
    he = gethostbyaddr((void *)&sin.sin_addr, SIZEOF(sin.sin_addr), AF_INET);
    if (he == NULL)
	return;

    kc = conn_get(he->h_name);
    kc->fd = in;
    if (gss_server(kc) < 0)
	error("gss_server failed: %s\n", kc->errmsg);
    kc->state = authed;
    accept_fn = fn;
    conn_read(kc);
}

/*
 * Locate an existing connection to the given host, or create a new,
 * unconnected entry if none exists.  The caller is expected to check
 * for the lack of a connection (kc->fd == -1) and set one up.
 */
static struct krb5_conn *
conn_get(
    const char *	hostname)
{
    struct krb5_conn *kc;

    k5printf(("krb5: conn_get: %s\n", hostname));

    for (kc = krb5_connq_first(); kc != NULL; kc = krb5_connq_next(kc)) {
	if (strcasecmp(hostname, kc->hostname) == 0)
	    break;
    }

    if (kc != NULL) {
	kc->refcnt++;
	k5printf(("krb5: conn_get: exists, refcnt to %s is now %d\n",
	    kc->hostname, kc->refcnt));
	return (kc);
    }

    k5printf(("krb5: conn_get: creating new handle\n"));
    /*
     * We can't be creating a new handle if we are the client
     */
    assert(accept_fn == NULL);
    kc = alloc(SIZEOF(*kc));
    kc->fd = -1;
    kc->readbuf.left = 0;
    kc->readbuf.size = 0;
    kc->state = unauthed;
    kc->ev_read = NULL;
    strncpy(kc->hostname, hostname, SIZEOF(kc->hostname) - 1);
    kc->hostname[SIZEOF(kc->hostname) - 1] = '\0';
    kc->errmsg = NULL;
    kc->gss_context = GSS_C_NO_CONTEXT;
    /*
     * [XXX] this is set to 2 in order to force the connection to stay
     * open and process more protocol requests.  (basically consistant
     * with bsd-security.c, and theoretically krb4-security.c.   This
     * needs to be addressed in a cleaner way.
     */
    kc->refcnt = 2;
    TAILQ_INIT(&kc->frameq);
    krb5_connq_append(kc);
    return (kc);
}

/*
 * Delete a reference to a connection, and close it if it is the last
 * reference.
 */
static void
conn_put(
    struct krb5_conn *	kc)
{
    OM_uint32 min_stat;
    struct krb5_frame *kf;

    assert(kc->refcnt > 0);
    if (--kc->refcnt > 0) {
	k5printf(("krb5: conn_put: decrementing refcnt for %s to %d\n",
	    kc->hostname, kc->refcnt));
	return;
    }
    k5printf(("krb5: conn_put: closing connection to %s\n", kc->hostname));
    if (kc->fd != -1)
	aclose(kc->fd);
    if (kc->ev_read != NULL)
	event_release(kc->ev_read);
    if (kc->errmsg != NULL)
	amfree(kc->errmsg);
    gss_delete_sec_context(&min_stat, &kc->gss_context, GSS_C_NO_BUFFER);
    while ((kf = TAILQ_FIRST(&kc->frameq)) != NULL) {
	TAILQ_REMOVE(&kc->frameq, kf, tq);
	if (kf->tok.value != NULL)
	    amfree(kf->tok.value);
	amfree(kf);
    }
    krb5_connq_remove(kc);
    amfree(kc);
    /* signal that a connection is available */
    event_wakeup((event_id_t)open_callback);
}

/*
 * Turn on read events for a conn.  Or, increase a refcnt if we are
 * already receiving read events.
 */
static void
conn_read(
    struct krb5_conn *kc)
{

    if (kc->ev_read != NULL) {
	kc->ev_read_refcnt++;
	k5printf(("krb5: conn_read: incremented refcnt to %d for %s\n",
	    kc->ev_read_refcnt, kc->hostname));
	return;
    }
    k5printf(("krb5: conn_read registering event handler for %s\n",
	kc->hostname));
    kc->ev_read = event_register((event_id_t)kc->fd, EV_READFD, conn_read_callback, kc);
    kc->ev_read_refcnt = 1;
}

static void
conn_read_cancel(
    struct krb5_conn *	kc)
{

    if (--kc->ev_read_refcnt > 0) {
	k5printf(("krb5: conn_read_cancel: decremented refcnt to %d for %s\n",
	    kc->ev_read_refcnt, kc->hostname));
	return;
    }
    k5printf(("krb5: conn_read_cancel: releasing event handler for %s\n",
	kc->hostname));
    event_release(kc->ev_read);
    kc->ev_read = NULL;
}

/*
 * frees a handle allocated by the above
 */
static void
krb5_close(
    void *	inst)
{
    struct krb5_handle *kh = inst;

    assert(kh != NULL);

    k5printf(("krb5: closing handle to %s\n", kh->hostname));

    if (kh->ks != NULL) {
	/* This may be null if we get here on an error */
	krb5_recvpkt_cancel(kh);
	security_stream_close(&kh->ks->secstr);
    }
    amfree(kh->hostname);
    amfree(kh);
}

/*
 * Transmit a packet.  Encrypt first.
 */
static ssize_t
krb5_sendpkt(
    void *	cookie,
    pkt_t *	pkt)
{
    struct krb5_handle *kh = cookie;
    gss_buffer_desc tok;
    int rval;
    unsigned char *buf;

    assert(kh != NULL);
    assert(pkt != NULL);

    k5printf(("krb5: sendpkt: enter\n"));

    if (pkt->body[0] == '\0') {
	tok.length = 1;
	tok.value = alloc(SIZEOF(pkt->type));
	memcpy(tok.value, &pkt->type, sizeof(unsigned char));
    } else {
	tok.length = strlen(pkt->body) + 2;
	tok.value = alloc(tok.length);
	buf = tok.value;
	*buf++ = (unsigned char)pkt->type;
	strncpy((char *)buf, pkt->body, tok.length - 2);
	buf[tok.length - 2] = '\0';
    }

    k5printf(("krb5: sendpkt: %s (%d) pkt_t (len %d) contains:\n\n\"%s\"\n\n",
	pkt_type2str(pkt->type), pkt->type, strlen(pkt->body), pkt->body));

    rval = krb5_stream_write(kh->ks, tok.value, tok.length);
    if (rval < 0)
	security_seterror(&kh->sech, security_stream_geterror(&kh->ks->secstr));
    /*@ignore@*/
    amfree(tok.value);
    /*@end@*/
    return (rval);
}

/*
 * Set up to receive a packet asyncronously, and call back when
 * it has been read.
 */
static void
krb5_recvpkt(
    void *	cookie,
    void	(*fn)(void *, pkt_t *, security_status_t),
    void *	arg,
    int		timeout)
{
    struct krb5_handle *kh = cookie;

    assert(kh != NULL);

    k5printf(("krb5: recvpkt registered for %s\n", kh->hostname));

    /*
     * Reset any pending timeout on this handle
     */
    if (kh->ev_timeout != NULL)
	event_release(kh->ev_timeout);

    /*
     * Negative timeouts mean no timeout
     */
    if (timeout < 0)
	kh->ev_timeout = NULL;
    else
	kh->ev_timeout = event_register((event_id_t)timeout, EV_TIME,
		recvpkt_timeout, kh);

    kh->fn.recvpkt = fn;
    kh->arg = arg;
    krb5_stream_read(kh->ks, recvpkt_callback, kh);
}

/*
 * Remove a async receive request from the queue
 */
static void
krb5_recvpkt_cancel(
    void *	cookie)
{
    struct krb5_handle *kh = cookie;

    k5printf(("krb5: cancelling recvpkt for %s\n", kh->hostname));

    assert(kh != NULL);

    krb5_stream_read_cancel(kh->ks);
    if (kh->ev_timeout != NULL) {
	event_release(kh->ev_timeout);
	kh->ev_timeout = NULL;
    }
}

/*
 * This is called when a handle is woken up because data read off of the
 * net is for it.
 */
static void
recvpkt_callback(
    void *cookie,
    void *buf,
    ssize_t bufsize)
{
    pkt_t pkt;
    struct krb5_handle *kh = cookie;

    assert(kh != NULL);

    /*
     * We need to cancel the recvpkt request before calling
     * the callback because the callback may reschedule us.
     */
    krb5_recvpkt_cancel(kh);

    switch (bufsize) {
    case 0:
	security_seterror(&kh->sech,
	    "EOF on read from %s", kh->hostname);
	(*kh->fn.recvpkt)(kh->arg, NULL, S_ERROR);
	return;
    case -1:
	security_seterror(&kh->sech, security_stream_geterror(&kh->ks->secstr));
	(*kh->fn.recvpkt)(kh->arg, NULL, S_ERROR);
	return;
    default:
	parse_pkt(&pkt, buf, (size_t)bufsize);
	k5printf(("krb5: received %s pkt (%d) from %s, contains:\n\n\"%s\"\n\n",
	    pkt_type2str(pkt.type), pkt.type, kh->hostname, pkt.body));
	(*kh->fn.recvpkt)(kh->arg, &pkt, S_OK);
	return;
    }
}

/*
 * This is called when a handle times out before receiving a packet.
 */
static void
recvpkt_timeout(
    void *	cookie)
{
    struct krb5_handle *kh = cookie;

    assert(kh != NULL);

    k5printf(("krb5: recvpkt timeout for %s\n", kh->hostname));

    krb5_recvpkt_cancel(kh);
    (*kh->fn.recvpkt)(kh->arg, NULL, S_TIMEOUT);
}

/*
 * Create the server end of a stream.  For krb5, this means setup a stream
 * object and allocate a new handle for it.
 */
static void *
krb5_stream_server(
    void *	h)
{
    struct krb5_handle *kh = h;
    struct krb5_stream *ks;

    assert(kh != NULL);

    ks = alloc(SIZEOF(*ks));
    security_streaminit(&ks->secstr, &krb5_security_driver);
    ks->kc = conn_get(kh->hostname);
    /*
     * Stream should already be setup!
     */
    if (ks->kc->fd < 0) {
	conn_put(ks->kc);
	amfree(ks);
	security_seterror(&kh->sech, "lost connection");
	return (NULL);
    }
    /*
     * so as not to conflict with the amanda server's handle numbers,
     * we start at 5000 and work down
     */
    ks->handle = (int)(5000 - newhandle++);
    ks->ev_read = NULL;
    k5printf(("krb5: stream_server: created stream %d\n", ks->handle));
    return (ks);
}

/*
 * Accept an incoming connection on a stream_server socket
 * Nothing needed for krb5.
 */
static int
krb5_stream_accept(
    void *	s)
{

    /* shut up compiler */
    s = s;

    return (0);
}

/*
 * Return a connected stream.  For krb5, this means setup a stream
 * with the supplied handle.
 */
static void *
krb5_stream_client(
    void *	h,
    int		id)
{
    struct krb5_handle *kh = h;
    struct krb5_stream *ks;

    assert(kh != NULL);

    if (id <= 0) {
	security_seterror(&kh->sech,
	    "%d: invalid security stream id", id);
	return (NULL);
    }

    ks = alloc(SIZEOF(*ks));
    security_streaminit(&ks->secstr, &krb5_security_driver);
    ks->handle = (int)id;
    ks->ev_read = NULL;
    ks->kc = conn_get(kh->hostname);

    k5printf(("krb5: stream_client: connected to stream %d\n", id));

    return (ks);
}

/*
 * Close and unallocate resources for a stream.
 */
static void
krb5_stream_close(
    void *	s)
{
    struct krb5_stream *ks = s;

    assert(ks != NULL);

    k5printf(("krb5: stream_close: closing stream %d\n", ks->handle));

    krb5_stream_read_cancel(ks);
    conn_put(ks->kc);
    amfree(ks);
}

/*
 * Authenticate a stream
 * Nothing needed for krb5.  The tcp connection is authenticated
 * on startup.
 */
static int
krb5_stream_auth(
    void *	s)
{
    /* shut up compiler */
    s = s;

    return (0);
}

/*
 * Returns the stream id for this stream.  This is just the local
 * port.
 */
static int
krb5_stream_id(
    void *	s)
{
    struct krb5_stream *ks = s;

    assert(ks != NULL);

    return (ks->handle);
}

/*
 * Write a chunk of data to a stream.  Blocks until completion.
 */
static int
krb5_stream_write(
    void *	s,
    const void *buf,
    size_t	size)
{
    struct krb5_stream *ks = s;
    gss_buffer_desc tok;
#ifdef AMANDA_KRB5_ENCRYPT
    gss_buffer_desc enctok;
    OM_uint32 min_stat;
#endif
    int rc;

    assert(ks != NULL);

    k5printf(("krb5: stream_write: writing %d bytes to %s:%d\n", size,
	ks->kc->hostname, ks->handle));

    tok.length = size;
    tok.value = (void *)buf;	/* safe to discard const */
#ifdef AMANDA_KRB5_ENCRYPT
    if (kencrypt(ks, &tok, &enctok) < 0)
	return (-1);
    rc = send_token(ks->kc, ks->handle, &enctok);
#else
    rc = send_token(ks->kc, ks->handle, &tok);
#endif
    if (rc < 0)
	security_stream_seterror(&ks->secstr, ks->kc->errmsg);
#ifdef AMANDA_KRB5_ENCRYPT
    gss_release_buffer(&min_stat, &enctok);
#endif
    return (rc);
}

/*
 * Submit a request to read some data.  Calls back with the given
 * function and arg when completed.
 */
static void
krb5_stream_read(
    void *	s,
    void	(*fn)(void *, void *, ssize_t),
    void *	arg)
{
    struct krb5_stream *ks = s;

    assert(ks != NULL);

    /*
     * Only one read request can be active per stream.
     */
    ks->fn = fn;
    ks->arg = arg;

    /*
     * First see if there's any queued frames for this stream.
     * If so, we're done.
     */
    if (conn_run_frameq(ks->kc, ks) > 0)
	return;

    if (ks->ev_read == NULL) {
	ks->ev_read = event_register((event_id_t)ks->kc, EV_WAIT,
	    stream_read_callback, ks);
	conn_read(ks->kc);
    }
}

/*
 * Submit a request to read some data.  Calls back with the given
 * function and arg when completed.
 */
static ssize_t
krb5_stream_read_sync(
    void *	s,
    void	**buf)
{
    struct krb5_stream *ks = s;

    assert(ks != NULL);

    /*
     * Only one read request can be active per stream.
     */
    ks->fn = stream_read_sync_callback2;
    ks->arg = ks;

    /*
     * First see if there's any queued frames for this stream.
     * If so, we're done.
     */
    if (conn_run_frameq(ks->kc, ks) > 0)
	return ks->len;

    if (ks->ev_read != NULL)
	event_release(ks->ev_read);

    ks->ev_read = event_register((event_id_t)ks->kc, EV_WAIT,
	stream_read_callback, ks);
    conn_read(ks->kc);
    event_wait(ks->ev_read);
    buf = (void **)&ks->buf;
    return ks->len;
}


/*
 * Callback for krb5_stream_read_sync
 */
static void
stream_read_sync_callback2(
    void *	arg,
    void *	buf,
    ssize_t	size)
{
    struct krb5_stream *ks = arg;

    assert(ks != NULL);

    k5printf(("krb5: stream_read_sync_callback2: handle %d\n", ks->handle));

    memcpy(ks->buf, buf, (size_t)size);
    ks->len = size;
}

/*
 * Cancel a previous stream read request.  It's ok if we didn't have a read
 * scheduled.
 */
static void
krb5_stream_read_cancel(
    void *	s)
{
    struct krb5_stream *ks = s;

    assert(ks != NULL);

    if (ks->ev_read != NULL) {
	event_release(ks->ev_read);
	ks->ev_read = NULL;
	conn_read_cancel(ks->kc);
    }
}

/*
 * Callback for krb5_stream_read
 */
static void
stream_read_callback(
    void *	arg)
{
    struct krb5_stream *ks = arg;

    assert(ks != NULL);

    k5printf(("krb5: stream_read_callback: handle %d\n", ks->handle));

    conn_run_frameq(ks->kc, ks);
}

/*
 * Run down a list of queued frames for a krb5_conn, and if we find one
 * that matches the passed handle, fire the read event.  Only
 * process one frame.
 *
 * Returns 1 if a frame was found and processed.
 */
static int
conn_run_frameq(
    /*@keep@*/	struct krb5_conn *	kc,
    /*@keep@*/	struct krb5_stream *	ks)
{
    struct krb5_frame *kf, *nextkf;
    gss_buffer_desc *enctok, *dectok;
#ifdef AMANDA_KRB5_ENCRYPT
    OM_uint32 min_stat;
    gss_buffer_desc tok;
#endif

    /*
     * Iterate through all of the frames in the queue.  If one
     * is for us, process it.  If we hit an EOF frame, shut down.
     * Stop after processing one frame, because we are only supposed
     * to return one read request.
     */
    for (kf = TAILQ_FIRST(&kc->frameq); kf != NULL; kf = nextkf) {
	nextkf = TAILQ_NEXT(kf, tq);

	if (kf->handle != ks->handle && kf->handle != H_EOF) {
	    k5printf(("krb5: conn_frameq_run: not for us (handle %d)\n",
		kf->handle));
	    continue;
	}
	/*
	 * We want all listeners to see the EOF, so never remove it.
	 * It will get cleaned up when the connection is closed
	 * in conn_put().
	 */
	if (kf->handle != H_EOF)
	    TAILQ_REMOVE(&kc->frameq, kf, tq);

	/*
	 * Remove the event first, and then call the callback.
	 * We remove it first because we don't want to get in their
	 * way if they reschedule it.
	 */
	krb5_stream_read_cancel(ks);

	enctok = &kf->tok;

	if (enctok->length == 0) {
	    assert(kf->handle == H_EOF);
	    k5printf(("krb5: stream_read_callback: EOF\n"));
	    (*ks->fn)(ks->arg, NULL, 0);
	    return (1);	/* stop after EOF */
	}

#ifdef AMANDA_KRB5_ENCRYPT
	dectok = &tok;
	if (kdecrypt(ks, enctok, &tok) < 0) {
	    k5printf(("krb5: stream_read_callback: kdecrypt error\n"));
	    (*ks->fn)(ks->arg, NULL, -1);
	} else
#else
	dectok = enctok;
#endif
	{
	    k5printf(("krb5: stream_read_callback: read %d bytes from %s:%d\n",
		dectok->length, ks->kc->hostname, ks->handle));
	    (*ks->fn)(ks->arg, dectok->value, (ssize_t)dectok->length);
#ifdef AMANDA_KRB5_ENCRYPT
	    gss_release_buffer(&min_stat, dectok);
#endif
	}
	amfree(enctok->value);
	amfree(kf);
	return (1);		/* stop after one frame */
    }
    return (0);
}

/*
 * The callback for the netfd for the event handler
 * Determines if this packet is for this security handle,
 * and does the real callback if so.
 */
static void
conn_read_callback(
    void *	cookie)
{
    struct krb5_conn *kc = cookie;
    struct krb5_handle *kh;
    struct krb5_frame *kf;
    pkt_t pkt;
    gss_buffer_desc *dectok;
    int rc;
#ifdef AMANDA_KRB5_ENCRYPT
    gss_buffer_desc tok;
    OM_uint32 min_stat;
#endif

    assert(cookie != NULL);

    k5printf(("krb5: conn_read_callback\n"));

    kf = alloc(SIZEOF(*kf));
    TAILQ_INSERT_TAIL(&kc->frameq, kf, tq);

    /* Read the data off the wire.  If we get errors, shut down. */
    rc = recv_token(kc, &kf->handle, &kf->tok, 5);
    k5printf(("krb5: conn_read_callback: recv_token returned %d handle = %d\n",
	rc, kf->handle));
    if (rc <= 0) {
	kf->tok.value = NULL;
	kf->tok.length = 0;
	kf->handle = H_EOF;
	rc = event_wakeup((event_id_t)kc);
	k5printf(("krb5: conn_read_callback: event_wakeup return %d\n", rc));
	return;
    }

    /* If there are events waiting on this handle, we're done */
    rc = event_wakeup((event_id_t)kc);
    k5printf(("krb5: conn_read_callback: event_wakeup return %d\n", rc));
    if (rc > 0)
	return;

    /*
     * If there is no accept fn registered, then just leave the
     * packet queued.  The caller may register a function later.
     */
    if (accept_fn == NULL) {
	k5printf(("krb5: no accept_fn so leaving packet queued.\n"));
	return;
    }

    kh = alloc(SIZEOF(*kh));
    security_handleinit(&kh->sech, &krb5_security_driver);
    kh->hostname = stralloc(kc->hostname);
    kh->ks = krb5_stream_client(kh, kf->handle);
    kh->ev_wait = NULL;
    kh->ev_timeout = NULL;

    TAILQ_REMOVE(&kc->frameq, kf, tq);
    k5printf(("krb5: new connection\n"));
#ifdef AMANDA_KRB5_ENCRYPT
    dectok = &tok;
    rc = kdecrypt(kh->ks, &kf->tok, dectok);
#else
    dectok = &kf->tok;
#endif

#ifdef AMANDA_KRB5_ENCRYPT
    if (rc < 0) {
	security_seterror(&kh->sech, security_geterror(&kh->ks->secstr));
	(*accept_fn)(&kh->sech, NULL);
    } else
#endif
    {
	parse_pkt(&pkt, dectok->value, dectok->length);
#ifdef AMANDA_KRB5_ENCRYPT
	gss_release_buffer(&min_stat, dectok);
#endif
	(*accept_fn)(&kh->sech, &pkt);
    }
    amfree(kf->tok.value);
    amfree(kf);

    /*
     * We can only accept one connection per process, since we're tcp
     * based and run out of inetd.  So, delete our accept reference once
     * we've gotten the first connection.
     */

    /*
     * [XXX] actually, the protocol has been changed to have multiple
     * requests in one session be possible.  By not resetting accept_fn,
     * this will caused them to be properly processed.  this needs to be
     * addressed in a much cleaner way.
     */
    if (accept_fn != NULL)
	conn_put(kc);
    /* accept_fn = NULL; */
}

/*
 * Negotiate a krb5 gss context from the client end.
 */
static int
gss_client(
    struct krb5_handle *kh)
{
    struct krb5_stream *ks = kh->ks;
    struct krb5_conn *kc = ks->kc;
    gss_buffer_desc send_tok, recv_tok;
    OM_uint32 maj_stat, min_stat;
    unsigned int ret_flags;
    int rc, rval = -1;
    gss_name_t gss_name;

    k5printf(("gss_client\n"));

    send_tok.value = vstralloc("host/", ks->kc->hostname, NULL);
    send_tok.length = strlen(send_tok.value) + 1;
    maj_stat = gss_import_name(&min_stat, &send_tok, GSS_C_NULL_OID,
	&gss_name);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	security_seterror(&kh->sech, "can't import name %s: %s",
	    (char *)send_tok.value, gss_error(maj_stat, min_stat));
	amfree(send_tok.value);
	return (-1);
    }
    amfree(send_tok.value);
    kc->gss_context = GSS_C_NO_CONTEXT;

    /*
     * Perform the context-establishement loop.
     *
     * Every generated token is stored in send_tok which is then
     * transmitted to the server; every received token is stored in
     * recv_tok (empty on the first pass) to be processed by
     * the next call to gss_init_sec_context.
     * 
     * GSS-API guarantees that send_tok's length will be non-zero
     * if and only if the server is expecting another token from us,
     * and that gss_init_sec_context returns GSS_S_CONTINUE_NEEDED if
     * and only if the server has another token to send us.
     */

    recv_tok.value = NULL;
    for (recv_tok.length = 0;;) {
	min_stat = 0;
	maj_stat = gss_init_sec_context(&min_stat,
	    GSS_C_NO_CREDENTIAL,
	    &kc->gss_context,
	    gss_name,
	    GSS_C_NULL_OID,
	    (OM_uint32)GSS_C_MUTUAL_FLAG|GSS_C_REPLAY_FLAG,
	    0, NULL,	/* no channel bindings */
	    (recv_tok.length == 0 ? GSS_C_NO_BUFFER : &recv_tok),
	    NULL,	/* ignore mech type */
	    &send_tok,
	    &ret_flags,
	    NULL);	/* ignore time_rec */

	if (recv_tok.length != 0) {
	    amfree(recv_tok.value);
	    recv_tok.length = 0;
	}

	if (maj_stat != (OM_uint32)GSS_S_COMPLETE && maj_stat != (OM_uint32)GSS_S_CONTINUE_NEEDED) {
	    security_seterror(&kh->sech,
		"error getting gss context: %s",
		gss_error(maj_stat, min_stat));
	    goto done;
	}

	/*
	 * Send back the response
	 */
	if (send_tok.length != 0 && send_token(kc, ks->handle, &send_tok) < 0) {
	    security_seterror(&kh->sech, kc->errmsg);
	    gss_release_buffer(&min_stat, &send_tok);
	    goto done;
	}
	gss_release_buffer(&min_stat, &send_tok);

	/*
	 * If we need to continue, then register for more packets
	 */
	if (maj_stat != (OM_uint32)GSS_S_CONTINUE_NEEDED)
	    break;

	if ((rc = recv_token(kc, NULL, &recv_tok, GSS_TIMEOUT)) <= 0) {
	    if (rc < 0)
		security_seterror(&kh->sech,
		    "recv error in gss loop: %s", kc->errmsg);
	    else
		security_seterror(&kh->sech, "EOF in gss loop");
	    goto done;
	}
    }

    rval = 0;
done:
    gss_release_name(&min_stat, &gss_name);
    return (rval);
}

/*
 * Negotiate a krb5 gss context from the server end.
 */
static int
gss_server(
    struct krb5_conn *	kc)
{
    OM_uint32 maj_stat, min_stat, ret_flags;
    gss_buffer_desc send_tok, recv_tok;
    gss_OID doid;
    gss_name_t gss_name;
    gss_cred_id_t gss_creds;
    char *p, *realm, *msg;
    uid_t euid;
    int rc, rval = -1;
    char errbuf[256];

    k5printf(("gss_server\n"));

    assert(kc != NULL);

    /*
     * We need to be root while in gss_acquire_cred() to read the host key
     * out of the default keytab.  We also need to be root in
     * gss_accept_context() thanks to the replay cache code.
     */
    euid = geteuid();
    if (getuid() != 0) {
	snprintf(errbuf, SIZEOF(errbuf),
	    "real uid is %ld, needs to be 0 to read krb5 host key",
	    (long)getuid());
	goto out;
    }
    if (seteuid(0) < 0) {
	snprintf(errbuf, SIZEOF(errbuf),
	    "can't seteuid to uid 0: %s", strerror(errno));
	goto out;
    }

    send_tok.value = vstralloc("host/", hostname, NULL);
    send_tok.length = strlen(send_tok.value) + 1;
    for (p = send_tok.value; *p != '\0'; p++) {
	if (isupper((int)*p))
	    *p = tolower(*p);
    }
    maj_stat = gss_import_name(&min_stat, &send_tok, GSS_C_NULL_OID,
	&gss_name);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	seteuid(euid);
	snprintf(errbuf, SIZEOF(errbuf),
	    "can't import name %s: %s", (char *)send_tok.value,
	    gss_error(maj_stat, min_stat));
	amfree(send_tok.value);
	goto out;
    }
    amfree(send_tok.value);

    maj_stat = gss_acquire_cred(&min_stat, gss_name, 0,
	GSS_C_NULL_OID_SET, GSS_C_ACCEPT, &gss_creds, NULL, NULL);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	snprintf(errbuf, SIZEOF(errbuf),
	    "can't acquire creds for host key host/%s: %s", hostname,
	    gss_error(maj_stat, min_stat));
	gss_release_name(&min_stat, &gss_name);
	seteuid(euid);
	goto out;
    }
    gss_release_name(&min_stat, &gss_name);

    for (recv_tok.length = 0;;) {
	if ((rc = recv_token(kc, NULL, &recv_tok, GSS_TIMEOUT)) <= 0) {
	    if (rc < 0) {
		snprintf(errbuf, SIZEOF(errbuf),
		    "recv error in gss loop: %s", kc->errmsg);
		amfree(kc->errmsg);
	    } else
		snprintf(errbuf, SIZEOF(errbuf), "EOF in gss loop");
	    goto out;
	}

	maj_stat = gss_accept_sec_context(&min_stat, &kc->gss_context,
	    gss_creds, &recv_tok, GSS_C_NO_CHANNEL_BINDINGS,
	    &gss_name, &doid, &send_tok, &ret_flags, NULL, NULL);

	if (maj_stat != (OM_uint32)GSS_S_COMPLETE &&
	    maj_stat != (OM_uint32)GSS_S_CONTINUE_NEEDED) {
	    snprintf(errbuf, SIZEOF(errbuf),
		"error accepting context: %s", gss_error(maj_stat, min_stat));
	    amfree(recv_tok.value);
	    goto out;
	}
	amfree(recv_tok.value);

	if (send_tok.length > 0 && send_token(kc, 0, &send_tok) < 0) {
	    strncpy(errbuf, kc->errmsg, SIZEOF(errbuf) - 1);
	    errbuf[SIZEOF(errbuf) - 1] = '\0';
	    amfree(kc->errmsg);
	    gss_release_buffer(&min_stat, &send_tok);
	    goto out;
	}
	gss_release_buffer(&min_stat, &send_tok);


	/*
	 * If we need to get more from the client, then register for
	 * more packets.
	 */
	if (maj_stat != (OM_uint32)GSS_S_CONTINUE_NEEDED)
	    break;
    }

    maj_stat = gss_display_name(&min_stat, gss_name, &send_tok, &doid);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	snprintf(errbuf, SIZEOF(errbuf),
	    "can't display gss name: %s", gss_error(maj_stat, min_stat));
	gss_release_name(&min_stat, &gss_name);
	goto out;
    }
    gss_release_name(&min_stat, &gss_name);

    /* get rid of the realm */
    if ((p = strchr(send_tok.value, '@')) == NULL) {
	snprintf(errbuf, SIZEOF(errbuf),
	    "malformed gss name: %s", (char *)send_tok.value);
	amfree(send_tok.value);
	goto out;
    }
    *p = '\0';
    realm = ++p;

    /* 
     * If the principal doesn't match, complain
     */
    if ((msg = krb5_checkuser(kc->hostname, send_tok.value, realm)) != NULL) {
	snprintf(errbuf, SIZEOF(errbuf),
	    "access not allowed from %s: %s", (char *)send_tok.value, msg);
	amfree(send_tok.value);
	goto out;
    }
    amfree(send_tok.value);

    rval = 0;
out:
    seteuid(euid);
    if (rval != 0)
	kc->errmsg = stralloc(errbuf);
    k5printf(("gss_server returning %d\n", rval));
    return (rval);
}

/*
 * Setup some things about krb5.  This should only be called once.
 */
static void
init(void)
{
    static int beenhere = 0;
    struct hostent *he;
    char *p;

    if (beenhere)
	return;
    beenhere = 1;

#ifndef BROKEN_MEMORY_CCACHE
    setenv(KRB5_ENV_CCNAME, "MEMORY:amanda_ccache", 1);
#else
    /*
     * MEMORY ccaches seem buggy and cause a lot of internal heap
     * corruption.  malloc has been known to core dump.  This behavior
     * has been witnessed in Cygnus' kerbnet 1.2, MIT's krb V 1.0.5 and
     * MIT's krb V -current as of 3/17/1999.
     *
     * We just use a lame ccache scheme with a uid suffix.
     */
    atexit(cleanup);
    {
	char ccache[64];
	snprintf(ccache, SIZEOF(ccache), "FILE:/tmp/amanda_ccache.%ld.%ld",
	    (long)geteuid(), (long)getpid());
	setenv(KRB5_ENV_CCNAME, ccache, 1);
    }
#endif

    gethostname(hostname, SIZEOF(hostname) - 1);
    hostname[SIZEOF(hostname) - 1] = '\0';
    /*
     * In case it isn't fully qualified, do a DNS lookup.
     */
    if ((he = gethostbyname(hostname)) != NULL)
	strncpy(hostname, he->h_name, SIZEOF(hostname) - 1);

    /*
     * Lowercase the results.  We assume all host/ principals will be
     * lowercased.
     */
    for (p = hostname; *p != '\0'; p++) {
	if (isupper((int)*p))
	    *p = tolower(*p);
    }
}

#ifdef BROKEN_MEMORY_CCACHE
static void
cleanup(void)
{
#ifdef KDESTROY_VIA_UNLINK
    char ccache[64];
    snprintf(ccache, SIZEOF(ccache), "/tmp/amanda_ccache.%ld.%ld",
        (long)geteuid(), (long)getpid());
    unlink(ccache);
#else
    kdestroy();
#endif
}
#endif

/*
 * Get a ticket granting ticket and stuff it in the cache
 */
static const char *
get_tgt(
    char *	keytab_name,
    char *	principal_name)
{
    krb5_context context;
    krb5_error_code ret;
    krb5_principal client = NULL, server = NULL;
    krb5_creds creds;
    krb5_keytab keytab = NULL;
    krb5_ccache ccache;
    krb5_timestamp now;
    krb5_data tgtname = { 0, KRB5_TGS_NAME_SIZE, KRB5_TGS_NAME };
    static char *error = NULL;

    if (error != NULL) {
	amfree(error);
	error = NULL;
    }

    if ((ret = krb5_init_context(&context)) != 0) {
	error = vstralloc("error initializing krb5 context: ",
	    error_message(ret), NULL);
	return (error);
    }

    /*krb5_init_ets(context);*/

    if(!keytab_name) {
        error = vstralloc("error  -- no krb5 keytab defined", NULL);
        return(error);
    }

    if(!principal_name) {
        error = vstralloc("error  -- no krb5 principal defined", NULL);
        return(error);
    }

    /*
     * Resolve keytab file into a keytab object
     */
    if ((ret = krb5_kt_resolve(context, keytab_name, &keytab)) != 0) {
	error = vstralloc("error resolving keytab ", keytab, ": ",
	    error_message(ret), NULL);
	return (error);
    }

    /*
     * Resolve the amanda service held in the keytab into a principal
     * object
     */
    ret = krb5_parse_name(context, principal_name, &client);
    if (ret != 0) {
	error = vstralloc("error parsing ", principal_name, ": ",
	    error_message(ret), NULL);
	return (error);
    }

    ret = krb5_build_principal_ext(context, &server,
	krb5_princ_realm(context, client)->length,
	krb5_princ_realm(context, client)->data,
	tgtname.length, tgtname.data,
	krb5_princ_realm(context, client)->length,
	krb5_princ_realm(context, client)->data,
	0);
    if (ret != 0) {
	error = vstralloc("error while building server name: ",
	    error_message(ret), NULL);
	return (error);
    }

    ret = krb5_timeofday(context, &now);
    if (ret != 0) {
	error = vstralloc("error getting time of day: ", error_message(ret),
	    NULL);
	return (error);
    }

    memset(&creds, 0, SIZEOF(creds));
    creds.times.starttime = 0;
    creds.times.endtime = now + AMANDA_TKT_LIFETIME;

    creds.client = client;
    creds.server = server;

    /*
     * Get a ticket for the service, using the keytab
     */
    ret = krb5_get_in_tkt_with_keytab(context, 0, NULL, NULL, NULL,
	keytab, 0, &creds, 0);

    if (ret != 0) {
	error = vstralloc("error getting ticket for ", principal_name,
	    ": ", error_message(ret), NULL);
	goto cleanup2;
    }

    if ((ret = krb5_cc_default(context, &ccache)) != 0) {
	error = vstralloc("error initializing ccache: ", error_message(ret),
	    NULL);
	goto cleanup;
    }
    if ((ret = krb5_cc_initialize(context, ccache, client)) != 0) {
	error = vstralloc("error initializing ccache: ", error_message(ret),
	    NULL);
	goto cleanup;
    }
    if ((ret = krb5_cc_store_cred(context, ccache, &creds)) != 0) {
	error = vstralloc("error storing creds in ccache: ",
	    error_message(ret), NULL);
	/* FALLTHROUGH */
    }
    krb5_cc_close(context, ccache);
cleanup:
    krb5_free_cred_contents(context, &creds);
cleanup2:
#if 0
    krb5_free_principal(context, client);
    krb5_free_principal(context, server);
#endif
    krb5_free_context(context);
    return (error);
}

/*
 * get rid of tickets
 */
static void
kdestroy(void)
{
    krb5_context context;
    krb5_ccache ccache;

    if ((krb5_init_context(&context)) != 0) {
	return;
    }
    if ((krb5_cc_default(context, &ccache)) != 0) {
	goto cleanup;
    }

    krb5_cc_destroy(context, ccache);
    krb5_cc_close(context, ccache);

cleanup:
     krb5_free_context(context);
     return;
}

/*
 * Formats an error from the gss api
 */
static const char *
gss_error(
    OM_uint32	major,
    OM_uint32	minor)
{
    static gss_buffer_desc msg;
    OM_uint32 min_stat, msg_ctx;

    if (msg.length > 0)
	gss_release_buffer(&min_stat, &msg);

    msg_ctx = 0;
    if (major == GSS_S_FAILURE)
	gss_display_status(&min_stat, minor, GSS_C_MECH_CODE, GSS_C_NULL_OID,
	    &msg_ctx, &msg);
    else
	gss_display_status(&min_stat, major, GSS_C_GSS_CODE, GSS_C_NULL_OID,
	    &msg_ctx, &msg);
    return ((const char *)msg.value);
}

/*
 * Transmits a gss_buffer_desc over a krb5_handle, adding
 * the necessary headers to allow the remote end to decode it.
 * Encryption must be done by the caller.
 */
static int
send_token(
    struct krb5_conn *		kc,
    int				handle,
    const gss_buffer_desc *	tok)
{
    OM_uint32 netlength, nethandle;
    struct iovec iov[3];

    k5printf(("krb5: send_token: writing %d bytes to %s\n", tok->length,
	kc->hostname));

    if (tok->length > AMANDA_MAX_TOK_SIZE) {
	kc->errmsg = newvstralloc(kc->errmsg, "krb5 write error to ",
	    kc->hostname, ": token too large", NULL);
	return (-1);
    }

    /*
     * Format is:
     *   32 bit length (network byte order)
     *   32 bit handle (network byte order)
     *   data
     */
    netlength = (OM_uint32)htonl(tok->length);
    iov[0].iov_base = (void *)&netlength;
    iov[0].iov_len = SIZEOF(netlength);

    nethandle = (OM_uint32)htonl((uint32_t)handle);
    iov[1].iov_base = (void *)&nethandle;
    iov[1].iov_len = SIZEOF(nethandle);

    iov[2].iov_base = (void *)tok->value;
    iov[2].iov_len = tok->length;

    if (net_writev(kc->fd, iov, 3) < 0) {
	kc->errmsg = newvstralloc(kc->errmsg, "krb5 write error to ",
	    kc->hostname, ": ", strerror(errno), NULL);
	return (-1);
    }
    return (0);
}

static ssize_t
recv_token(
    struct krb5_conn *	kc,
    int *		handle,
    gss_buffer_desc *	gtok,
    int			timeout)
{
    OM_uint32 netint[2];

    assert(kc->fd >= 0);
    assert(gtok != NULL);

    k5printf(("krb5: recv_token: reading from %s\n", kc->hostname));

    switch (net_read(kc->fd, &netint, SIZEOF(netint), timeout)) {
    case -1:
	kc->errmsg = newvstralloc(kc->errmsg, "recv error: ", strerror(errno),
	    NULL);
	k5printf(("krb5 recv_token error return: %s\n", kc->errmsg));
	return (-1);
    case 0:
	gtok->length = 0;
	return (0);
    default:
	break;
    }
    gtok->length = ntohl(netint[0]);

    if (gtok->length > AMANDA_MAX_TOK_SIZE) {
	kc->errmsg = newstralloc(kc->errmsg, "recv error: buffer too large");
	k5printf(("krb5 recv_token error return: %s\n", kc->errmsg));
	return (-1);
    }

    if (handle != NULL)
	*handle = ntohl(netint[1]);

    gtok->value = alloc(gtok->length);
    switch (net_read(kc->fd, gtok->value, gtok->length, timeout)) {
    case -1:
	kc->errmsg = newvstralloc(kc->errmsg, "recv error: ", strerror(errno),
	    NULL);
	k5printf(("krb5 recv_token error return: %s\n", kc->errmsg));
	amfree(gtok->value);
	return (-1);
    case 0:
	amfree(gtok->value);
	gtok->length = 0;
	break;
    default:
	break;
    }

    k5printf(("krb5: recv_token: read %d bytes from %s\n", gtok->length,
	kc->hostname));
    return ((ssize_t)gtok->length);
}

#ifdef AMANDA_KRB5_ENCRYPT
static int
kencrypt(
    struct krb5_stream *ks,
    gss_buffer_desc *	tok,
    gss_buffer_desc *	enctok)
{
    int conf_state;
    OM_uint32 maj_stat, min_stat;

    assert(ks->kc->gss_context != GSS_C_NO_CONTEXT);
    maj_stat = gss_seal(&min_stat, ks->kc->gss_context, 1,
	GSS_C_QOP_DEFAULT, tok, &conf_state, enctok);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE || conf_state == 0) {
	security_stream_seterror(&ks->secstr,
	    "krb5 encryption failed to %s: %s",
	    ks->kc->hostname, gss_error(maj_stat, min_stat));
	return (-1);
    }
    return (0);
}

static int
kdecrypt(
    struct krb5_stream *ks,
    gss_buffer_desc *	enctok,
    gss_buffer_desc *	tok)
{
    OM_uint32 maj_stat, min_stat;
    int conf_state, qop_state;

    k5printf(("krb5: kdecrypt: decrypting %d bytes\n", enctok->length));

    assert(ks->kc->gss_context != GSS_C_NO_CONTEXT);
    maj_stat = gss_unseal(&min_stat, ks->kc->gss_context, enctok, tok,
	&conf_state, &qop_state);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	security_stream_seterror(&ks->secstr, "krb5 decrypt error from %s: %s",
	    ks->kc->hostname, gss_error(maj_stat, min_stat));
	return (-1);
    }
    return (0);
}
#endif

/*
 * hackish, but you can #undef AMANDA_PRINCIPAL here, and you can both
 * hardcode a principal in your build and use the .k5amandahosts.  This is
 * available because sites that run pre-releases of amanda 2.5.0 before 
 * this feature was there do not behave this way...
 */

/*#undef AMANDA_PRINCIPAL*/

/*
 * check ~/.k5amandahosts to see if this principal is allowed in.  If it's
 * hardcoded, then we don't check the realm
 */
static char *
krb5_checkuser( char *	host,
    char *	name,
    char *	realm)
{
#ifdef AMANDA_PRINCIPAL
    if(strcmp(name, AMANDA_PRINCIPAL) == 0) {
	return(NULL);
    } else {
	return(vstralloc("does not match compiled in default"));
    }
#else
    struct passwd *pwd;
    char *ptmp;
    char *result = "generic error";	/* default is to not permit */
    FILE *fp = NULL;
    struct stat sbuf;
    uid_t localuid;
    char *line = NULL;
    char *filehost = NULL, *fileuser = NULL, *filerealm = NULL;
    char n1[NUM_STR_SIZE];
    char n2[NUM_STR_SIZE];

    assert( host != NULL);
    assert( name != NULL);

    if((pwd = getpwnam(CLIENT_LOGIN)) == NULL) {
	result = vstralloc("can not find user ", CLIENT_LOGIN, NULL);
    }
    localuid = pwd->pw_uid;

#ifdef USE_AMANDAHOSTS
    ptmp = stralloc2(pwd->pw_dir, "/.k5amandahosts");
#else
    ptmp = stralloc2(pwd->pw_dir, "/.k5login");
#endif

    if(!ptmp) {
	result = vstralloc("could not find home directory for ", CLIENT_LOGIN, NULL);
	goto common_exit;
   }

   /*
    * check to see if the ptmp file does nto exist.
    */
   if(access(ptmp, R_OK) == -1 && errno == ENOENT) {
	/*
	 * in this case we check to see if the principal matches
	 * the destination user mimicing the .k5login functionality.
	 */
	 if(strcmp(name, CLIENT_LOGIN) != 0) {
		result = vstralloc(name, " does not match ",
			CLIENT_LOGIN, NULL);
		return result;
	}
	result = NULL;
	goto common_exit;
    }

    k5printf(("opening ptmp: %s\n", (ptmp)?ptmp: "NULL!"));
    if((fp = fopen(ptmp, "r")) == NULL) {
	result = vstralloc("can not open ", ptmp, NULL);
	return result;
    }
    k5printf(("opened ptmp\n"));

    if (fstat(fileno(fp), &sbuf) != 0) {
	result = vstralloc("cannot fstat ", ptmp, ": ", strerror(errno), NULL);
	goto common_exit;
    }

    if (sbuf.st_uid != localuid) {
	snprintf(n1, SIZEOF(n1), "%ld", (long) sbuf.st_uid);
	snprintf(n2, SIZEOF(n2), "%ld", (long) localuid);
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

    while ((line = agets(fp)) != NULL) {
	if (line[0] == '\0') {
	    amfree(line);
	    continue;
	}

#if defined(SHOW_SECURITY_DETAIL)                               /* { */
	k5printf(("%s: processing line: <%s>\n", debug_prefix(NULL), line));
#endif                                                          /* } */
	/* if there's more than one column, then it's the host */
	if( (filehost = strtok(line, " \t")) == NULL) {
	    amfree(line);
	    continue;
	}

	/*
	 * if there's only one entry, then it's a username and we have
	 * no hostname.  (so the principal is allowed from anywhere.
	 */
	if((fileuser = strtok(NULL, " \t")) == NULL) {
	    fileuser = filehost;
	    filehost = NULL;
	}

	if(filehost && strcmp(filehost, host) != 0) {
	    amfree(line);
	    continue;
	} else {
		k5printf(("found a host match\n"));
	}

	if( (filerealm = strchr(fileuser, '@')) != NULL) {
	    *filerealm++ = '\0';
	}

	/*
	 * we have a match.  We're going to be a little bit insecure
	 * and indicate that the principal is correct but the realm is
	 * not if that's the case.  Technically we should say nothing
	 * and let the user figure it out, but it's helpful for debugging.
	 * You likely only get this far if you've turned on cross-realm auth
	 * anyway...
	 */
	k5printf(("comparing %s %s\n", fileuser, name));
	if(strcmp(fileuser, name) == 0) {
		k5printf(("found a match!\n"));
		if(realm && filerealm && (strcmp(realm, filerealm)!=0)) {
			amfree(line);
			continue;
		}
		result = NULL;
		amfree(line);
		goto common_exit;
	}
	amfree(line);
    }
    result = vstralloc("no match in ", ptmp, NULL);

common_exit:
    afclose(fp);
    return(result);
#endif /* AMANDA_PRINCIPAL */
}

#else

void krb5_security_dummy(void);

void
krb5_security_dummy(void)
{
}

#endif	/* KRB5_SECURITY */
