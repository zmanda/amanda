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
 * $Id: bsdtcp-security.c,v 1.7 2006/07/13 03:22:20 paddy_s Exp $
 *
 * bsdtcp-security.c - security and transport over bsdtcp or a bsdtcp-like command.
 *
 * XXX still need to check for initial keyword on connect so we can skip
 * over shell garbage and other stuff that bsdtcp might want to spew out.
 */

#include "amanda.h"
#include "util.h"
#include "event.h"
#include "packet.h"
#include "security.h"
#include "security-util.h"
#include "sockaddr-util.h"
#include "stream.h"

/*
 * Number of seconds bsdtcp has to start up
 */
#define	CONNECT_TIMEOUT	20

/*
 * Interface functions
 */
static void bsdtcp_accept(const struct security_driver *,
    char *(*)(char *, void *),
    int, int,
    void (*)(security_handle_t *, pkt_t *),
    void *);
static void bsdtcp_connect(const char *,
    char *(*)(char *, void *), 
    void (*)(void *, security_handle_t *, security_status_t), void *, void *);

/*
 * This is our interface to the outside world.
 */
const security_driver_t bsdtcp_security_driver = {
    "BSDTCP",
    bsdtcp_connect,
    bsdtcp_accept,
    sec_close,
    stream_sendpkt,
    stream_recvpkt,
    stream_recvpkt_cancel,
    tcpma_stream_server,
    tcpma_stream_accept,
    tcpma_stream_client,
    tcpma_stream_close,
    sec_stream_auth,
    sec_stream_id,
    tcpm_stream_write,
    tcpm_stream_read,
    tcpm_stream_read_sync,
    tcpm_stream_read_cancel,
    tcpm_close_connection,
    NULL,
    NULL
};

static int newhandle = 1;

/*
 * Local functions
 */
static int runbsdtcp(struct sec_handle *, in_port_t port);


/*
 * bsdtcp version of a security handle allocator.  Logically sets
 * up a network "connection".
 */
static void
bsdtcp_connect(
    const char *hostname,
    char *	(*conf_fn)(char *, void *),
    void	(*fn)(void *, security_handle_t *, security_status_t),
    void *	arg,
    void *	datap)
{
    struct sec_handle *rh;
    int result;
    char *canonname;
    char *service;
    in_port_t port;

    assert(fn != NULL);
    assert(hostname != NULL);
    (void)conf_fn;	/* Quiet unused parameter warning */
    (void)datap;	/* Quiet unused parameter warning */

    auth_debug(1, _("bsdtcp: bsdtcp_connect: %s\n"), hostname);

    rh = alloc(sizeof(*rh));
    security_handleinit(&rh->sech, &bsdtcp_security_driver);
    rh->hostname = NULL;
    rh->rs = NULL;
    rh->ev_timeout = NULL;
    rh->rc = NULL;

    result = resolve_hostname(hostname, 0, NULL, &canonname);
    if(result != 0) {
	dbprintf(_("resolve_hostname(%s): %s\n"), hostname, gai_strerror(result));
	security_seterror(&rh->sech, _("resolve_hostname(%s): %s\n"), hostname,
			  gai_strerror(result));
	(*fn)(arg, &rh->sech, S_ERROR);
	return;
    }
    if (canonname == NULL) {
	dbprintf(_("resolve_hostname(%s) did not return a canonical name\n"), hostname);
	security_seterror(&rh->sech,
	        _("resolve_hostname(%s) did not return a canonical name\n"), hostname);
	(*fn)(arg, &rh->sech, S_ERROR);
       return;
    }

    rh->hostname = canonname;	/* will be replaced */
    canonname = NULL; /* steal reference */
    rh->rs = tcpma_stream_client(rh, newhandle++);
    rh->rc->recv_security_ok = &bsd_recv_security_ok;
    rh->rc->prefix_packet = &bsd_prefix_packet;

    if (rh->rs == NULL)
	goto error;

    amfree(rh->hostname);
    rh->hostname = stralloc(rh->rs->rc->hostname);

    if (conf_fn) {
	service = conf_fn("client_port", datap);
	if (strlen(service) <= 1)
	    service = "amanda";
    } else {
	service = "amanda";
    }
    port = find_port_for_service(service, "tcp");
    if (port == 0) {
	security_seterror(&rh->sech, _("%s/tcp unknown protocol"), service);
	goto error;
    }

    /*
     * We need to open a new connection.
     *
     * XXX need to eventually limit number of outgoing connections here.
     */
    if(rh->rc->read == -1) {
	if (runbsdtcp(rh, port) < 0)
	    goto error;
	rh->rc->refcnt++;
    }

    /*
     * The socket will be opened async so hosts that are down won't
     * block everything.  We need to register a write event
     * so we will know when the socket comes alive.
     *
     * Overload rh->rs->ev_read to provide a write event handle.
     * We also register a timeout.
     */
    rh->fn.connect = fn;
    rh->arg = arg;
    rh->rs->ev_read = event_register((event_id_t)(rh->rs->rc->write),
	EV_WRITEFD, sec_connect_callback, rh);
    rh->ev_timeout = event_register(CONNECT_TIMEOUT, EV_TIME,
	sec_connect_timeout, rh);

    return;

error:
    (*fn)(arg, &rh->sech, S_ERROR);
}

/*
 * Setup to handle new incoming connections
 */
static void
bsdtcp_accept(
    const struct security_driver *driver,
    char *	(*conf_fn)(char *, void *),
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *),
    void       *datap)
{
    sockaddr_union sin;
    socklen_t_equiv len;
    struct tcp_conn *rc;
    char hostname[NI_MAXHOST];
    int result;
    char *errmsg = NULL;

    len = sizeof(sin);
    if (getpeername(in, (struct sockaddr *)&sin, &len) < 0) {
	dbprintf(_("getpeername returned: %s\n"), strerror(errno));
	return;
    }
    if ((result = getnameinfo((struct sockaddr *)&sin, len,
			      hostname, NI_MAXHOST, NULL, 0, 0) != 0)) {
	dbprintf(_("getnameinfo failed: %s\n"),
		  gai_strerror(result));
	return;
    }
    if (check_name_give_sockaddr(hostname,
				 (struct sockaddr *)&sin, &errmsg) < 0) {
	amfree(errmsg);
	return;
    }

    rc = sec_tcp_conn_get(hostname, 0);
    rc->recv_security_ok = &bsd_recv_security_ok;
    rc->prefix_packet = &bsd_prefix_packet;
    copy_sockaddr(&rc->peer, &sin);
    rc->read = in;
    rc->write = out;
    rc->accept_fn = fn;
    rc->driver = driver;
    rc->conf_fn = conf_fn;
    rc->datap = datap;
    sec_tcp_conn_read(rc);
}

/*
 * Forks a bsdtcp to the host listed in rc->hostname
 * Returns negative on error, with an errmsg in rc->errmsg.
 */
static int
runbsdtcp(
    struct sec_handle *	rh,
    in_port_t port)
{
    int			server_socket;
    in_port_t		my_port;
    struct tcp_conn *	rc = rh->rc;

    set_root_privs(1);

    server_socket = stream_client_privileged(rc->hostname,
				     port,
				     STREAM_BUFSIZE,
				     STREAM_BUFSIZE,
				     &my_port,
				     0);
    set_root_privs(0);

    if(server_socket < 0) {
	security_seterror(&rh->sech,
	    "%s", strerror(errno));
	
	return -1;
    }

    if(my_port >= IPPORT_RESERVED) {
	security_seterror(&rh->sech,
			  _("did not get a reserved port: %d"), my_port);
    }

    rc->read = rc->write = server_socket;
    return 0;
}
