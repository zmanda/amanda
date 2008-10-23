#ifndef _SECURITY_UTIL_H
#define _SECURITY_UTIL_H

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
 * $Id: security-util.h,v 1.5 2006/07/01 00:10:38 paddy_s Exp $
 *
 */

#include "stream.h"
#include "dgram.h"
#include "queue.h"
#include "conffile.h"
#include "security.h"
#include "event.h"

#define auth_debug(i, ...) do {		\
	if ((i) <= debug_auth) {	\
	    dbprintf(__VA_ARGS__);	\
	}				\
} while (0)


#ifdef KRB5_SECURITY
#  define KRB5_DEPRECATED 1
#  ifndef KRB5_HEIMDAL_INCLUDES
#    include <gssapi/gssapi_generic.h>
#  else
#    include <gssapi/gssapi.h>
#  endif
#  include <krb5.h>
#endif

struct sec_handle;

/*
 * This is a sec connection to a host.  We should only have
 * one connection per host.
 */
struct tcp_conn {
    const struct security_driver *driver;	/* MUST be first */
    int			read, write;		/* pipes to sec */
    pid_t		pid;			/* pid of sec process */
    char *		pkt;			/* last pkt read */
    ssize_t		pktlen;			/* len of above */
    event_handle_t *	ev_read;		/* read (EV_READFD) handle */
    int			ev_read_refcnt;		/* number of readers */
    char		hostname[MAX_HOSTNAME_LENGTH+1];
						/* host we're talking to */
    char *		errmsg;			/* error passed up */
    int			refcnt;			/* number of handles using */
    int			handle;			/* last proto handle read */
    int			event_id;		/* event ID fired when token read */
    void		(*accept_fn)(security_handle_t *, pkt_t *);
    sockaddr_union	peer;
    TAILQ_ENTRY(tcp_conn) tq;			/* queue handle */
    int			(*recv_security_ok)(struct sec_handle *, pkt_t *);
    char *		(*prefix_packet)(void *, pkt_t *);
    int			toclose;
    int			donotclose;
    int			auth;
    char *              (*conf_fn)(char *, void *);
    void *              datap;
#ifdef KRB5_SECURITY
    gss_ctx_id_t	gss_context;
#endif
};


struct sec_stream;

/*
 * This is the private handle data.
 */
struct sec_handle {
    security_handle_t	sech;		/* MUST be first */
    char *		hostname;	/* ptr to rc->hostname */
    struct sec_stream *	rs;		/* virtual stream we xmit over */
    struct tcp_conn *	rc;		/* */
    union {
	void (*recvpkt)(void *, pkt_t *, security_status_t);
					/* func to call when packet recvd */
	void (*connect)(void *, security_handle_t *, security_status_t);
					/* func to call when connected */
    } fn;
    void *		arg;		/* argument to pass function */
    event_handle_t *	ev_timeout;	/* timeout handle for recv */
    sockaddr_union	peer;
    int			sequence;
    event_id_t		event_id;
    char *		proto_handle;
    event_handle_t *	ev_read;
    struct sec_handle *	prev;
    struct sec_handle *	next;
    struct udp_handle *	udp;
    void		(*accept_fn)(security_handle_t *, pkt_t *);
    int			(*recv_security_ok)(struct sec_handle *, pkt_t *);
};

/*
 * This is the internal security_stream data for sec.
 */
struct sec_stream {
    security_stream_t	secstr;		/* MUST be first */
    struct tcp_conn *	rc;		/* physical connection */
    int			handle;		/* protocol handle */
    event_handle_t *	ev_read;	/* read (EV_WAIT) event handle */
    void		(*fn)(void *, void *, ssize_t);	/* read event fn */
    void *		arg;		/* arg for previous */
    int			fd;
    char		databuf[NETWORK_BLOCK_BYTES];
    ssize_t		len;
    int			socket;
    in_port_t		port;
    int			closed_by_me;
    int			closed_by_network;
};

struct connq_s {
    TAILQ_HEAD(, tcp_conn) tailq;
    int qlength;
};
extern struct connq_s connq;

#define connq_first()           TAILQ_FIRST(&connq.tailq)
#define connq_next(rc)          TAILQ_NEXT(rc, tq)
#define connq_append(rc)        do {                                    \
    TAILQ_INSERT_TAIL(&connq.tailq, rc, tq);                            \
    connq.qlength++;                                                    \
} while (0)
#define connq_remove(rc)        do {                                    \
    assert(connq.qlength > 0);                                          \
    TAILQ_REMOVE(&connq.tailq, rc, tq);                                 \
    connq.qlength--;                                                    \
} while (0)

/*
 * This is data local to the datagram socket.  We have one datagram
 * per process per auth.
 */
typedef struct udp_handle {
    const struct security_driver *driver;	/* MUST be first */
    dgram_t dgram;		/* datagram to read/write from */
    sockaddr_union peer;	/* who sent it to us */
    pkt_t pkt;			/* parsed form of dgram */
    char *handle;		/* handle from recvd packet */
    int sequence;		/* seq no of packet */
    event_handle_t *ev_read;	/* read event handle from dgram */
    int refcnt;			/* number of handles blocked for reading */
    struct sec_handle *bh_first, *bh_last;
    void (*accept_fn)(security_handle_t *, pkt_t *);
    int (*recv_security_ok)(struct sec_handle *, pkt_t *);
    char *(*prefix_packet)(void *, pkt_t *);
} udp_handle_t;

/*
 * We register one event handler for our network fd which takes
 * care of all of our async requests.  When all async requests
 * have either been satisfied or cancelled, we unregister our
 * network event handler.
 */
#define	udp_addref(udp, netfd_read_callback) do {			\
    if ((udp)->refcnt++ == 0) {						\
	assert((udp)->ev_read == NULL);					\
	(udp)->ev_read = event_register((event_id_t)(udp)->dgram.socket,\
	    EV_READFD, netfd_read_callback, (udp));			\
    }									\
    assert((udp)->refcnt > 0);						\
} while (0)

/*
 * If this is the last request to be removed, then remove the
 * reader event from the netfd.
 */
#define	udp_delref(udp) do {						\
    assert((udp)->refcnt > 0);						\
    if (--(udp)->refcnt == 0) {						\
	assert((udp)->ev_read != NULL);					\
	event_release((udp)->ev_read);					\
	(udp)->ev_read = NULL;						\
    }									\
} while (0)


int	sec_stream_auth(void *);
int	sec_stream_id(void *);
void	sec_accept(const security_driver_t *,
		   char *(*)(char *, void *),
		   int, int,
		   void (*)(security_handle_t *, pkt_t *),
		   void *);
void	sec_close(void *);
void	sec_connect_callback(void *);
void	sec_connect_timeout(void *);
void	sec_close_connection_none(void *, char *);

ssize_t	stream_sendpkt(void *, pkt_t *);
void	stream_recvpkt(void *,
		        void (*)(void *, pkt_t *, security_status_t),
		        void *, int);
void	stream_recvpkt_timeout(void *);
void	stream_recvpkt_cancel(void *);

int	tcpm_stream_write(void *, const void *, size_t);
void	tcpm_stream_read(void *, void (*)(void *, void *, ssize_t), void *);
ssize_t	tcpm_stream_read_sync(void *, void **);
void	tcpm_stream_read_cancel(void *);
ssize_t	tcpm_send_token(struct tcp_conn *, int, int, char **, const void *, size_t);
ssize_t	tcpm_recv_token(struct tcp_conn *, int, int *, char **, char **, ssize_t *, int);
void	tcpm_close_connection(void *, char *);

int	tcpma_stream_accept(void *);
void *	tcpma_stream_client(void *, int);
void *	tcpma_stream_server(void *);
void	tcpma_stream_close(void *);

void *	tcp1_stream_server(void *);
int	tcp1_stream_accept(void *);
void *	tcp1_stream_client(void *, int);

int	tcp_stream_write(void *, const void *, size_t);

char *	bsd_prefix_packet(void *, pkt_t *);
int	bsd_recv_security_ok(struct sec_handle *, pkt_t *);

ssize_t	udpbsd_sendpkt(void *, pkt_t *);
void	udp_close(void *);
void	udp_recvpkt(void *, void (*)(void *, pkt_t *, security_status_t),
		     void *, int);
void	udp_recvpkt_cancel(void *);
void	udp_recvpkt_callback(void *);
void	udp_recvpkt_timeout(void *);
int	udp_inithandle(udp_handle_t *, struct sec_handle *, char *hostname,
		       sockaddr_union *, in_port_t, char *, int);
void	udp_netfd_read_callback(void *);

struct tcp_conn *sec_tcp_conn_get(const char *, int);
void	sec_tcp_conn_put(struct tcp_conn *);
void	sec_tcp_conn_read(struct tcp_conn *);
void	parse_pkt(pkt_t *, const void *, size_t);
const char *pkthdr2str(const struct sec_handle *, const pkt_t *);
int	str2pkthdr(udp_handle_t *);
char *	check_user(struct sec_handle *, const char *, const char *);

char *	check_user_ruserok    (const char *host,
				struct passwd *pwd,
				const char *user);
char *	check_user_amandahosts(const char *host,
			        sockaddr_union *addr,
				struct passwd *pwd,
				const char *user,
				const char *service);

ssize_t	net_read(int, void *, size_t, int);
ssize_t net_read_fillbuf(int, int, void *, size_t);
void	show_stat_info(char *a, char *b);
int     check_name_give_sockaddr(const char *hostname, struct sockaddr *addr,
				 char **errstr);

#endif /* _SECURITY_INFO_H */
