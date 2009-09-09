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
 *
 * XXX still need to check for initial keyword on connect so we can skip
 * over shell garbage and other stuff that krb5 might want to spew out.
 */

#include "amanda.h"
#include "util.h"
#include "event.h"
#include "packet.h"
#include "security.h"
#include "security-util.h"
#include "stream.h"
#include "sockaddr-util.h"

#ifdef KRB5_HEIMDAL_INCLUDES
#include "com_err.h"
#endif

#define BROKEN_MEMORY_CCACHE

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
#define KRB5_ENV_CCNAME "KRB5CCNAME"
#endif

/*
 * consider undefining when kdestroy() is fixed.  The current version does
 * not work under krb5-1.2.4 in rh7.3, perhaps others.
 */
#define KDESTROY_VIA_UNLINK     1

/*
 * Where the keytab lives, if defined.  Otherwise it expects something in the
 * config file.
 */
/* #define AMANDA_KEYTAB        "/.amanda-v5-keytab" */

/*
 * The name of the principal we authenticate with, if defined.  Otherwise
 * it expects something in the config file.
 */
/* #define      AMANDA_PRINCIPAL        "service/amanda" */

/*
 * The lifetime of our tickets in seconds.  This may or may not need to be
 * configurable.
 */
#define AMANDA_TKT_LIFETIME     (12*60*60)


/*
 * The name of the service in /etc/services.  This probably shouldn't be
 * configurable.
 */
#define AMANDA_KRB5_SERVICE_NAME        "k5amanda"

/*
 * The default port to use if above entry in /etc/services doesn't exist
 */
#define AMANDA_KRB5_DEFAULT_PORT        10082

/*
 * The timeout in seconds for each step of the GSS negotiation phase
 */
#define GSS_TIMEOUT                     30

/*
 * This is the tcp stream buffer size
 */
#define KRB5_STREAM_BUFSIZE     (32768 * 2)

/*
 * This is the max number of outgoing connections we can have at once.
 * planner/amcheck/etc will open a bunch of connections as it tries
 * to contact everything.  We need to limit this to avoid blowing
 * the max number of open file descriptors a process can have.
 */
#define AMANDA_KRB5_MAXCONN     40


/*
 * Number of seconds krb5 has to start up
 */
#define	CONNECT_TIMEOUT	20

/*
 * Cache the local hostname
 */
static char myhostname[MAX_HOSTNAME_LENGTH+1];


/*
 * Interface functions
 */
static void krb5_accept(const struct security_driver *,
    char *(*)(char *, void *),
    int, int,
    void (*)(security_handle_t *, pkt_t *),
    void *);
static void krb5_connect(const char *,
    char *(*)(char *, void *), 
    void (*)(void *, security_handle_t *, security_status_t), void *, void *);

static void krb5_init(void);
#ifdef BROKEN_MEMORY_CCACHE
static void cleanup(void);
#endif
static const char *get_tgt(char *keytab_name, char *principal_name);
static int	   gss_server(struct tcp_conn *);
static int	   gss_client(struct sec_handle *);
static const char *gss_error(OM_uint32, OM_uint32);
static char       *krb5_checkuser(char *host, char *name, char *realm);

static int k5_encrypt(void *cookie, void *buf, ssize_t buflen,
		      void **encbuf, ssize_t *encbuflen);
static int k5_decrypt(void *cookie, void *buf, ssize_t buflen,
		      void **encbuf, ssize_t *encbuflen);

/*
 * This is our interface to the outside world.
 */
const security_driver_t krb5_security_driver = {
    "KRB5",
    krb5_connect,
    krb5_accept,
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
    k5_encrypt,
    k5_decrypt,
};

static int newhandle = 1;

/*
 * Local functions
 */
static int runkrb5(struct sec_handle *);

char *keytab_name;
char *principal_name;

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
    struct sec_handle *rh;
    int result;
    char *canonname;

    assert(fn != NULL);
    assert(hostname != NULL);

    auth_debug(1, "krb5: krb5_connect: %s\n", hostname);

    krb5_init();

    rh = alloc(sizeof(*rh));
    security_handleinit(&rh->sech, &krb5_security_driver);
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

    rh->hostname = canonname;        /* will be replaced */
    canonname = NULL; /* steal reference */
    rh->rs = tcpma_stream_client(rh, newhandle++);
    rh->rc->conf_fn = conf_fn;
    rh->rc->datap = datap;
    rh->rc->recv_security_ok = NULL;
    rh->rc->prefix_packet = NULL;

    if (rh->rs == NULL)
	goto error;

    amfree(rh->hostname);
    rh->hostname = stralloc(rh->rs->rc->hostname);

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

    /*
     * We need to open a new connection.
     *
     * XXX need to eventually limit number of outgoing connections here.
     */
    if(rh->rc->read == -1) {
	if (runkrb5(rh) < 0)
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

    amfree(canonname);
    return;

error:
    amfree(canonname);
    (*fn)(arg, &rh->sech, S_ERROR);
}

/*

 * Setup to handle new incoming connections
 */
static void
krb5_accept(
    const struct security_driver *driver,
    char       *(*conf_fn)(char *, void *),
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

    krb5_init();

    len = sizeof(sin);
    if (getpeername(in, (struct sockaddr *)&sin, &len) < 0) {
	dbprintf(_("getpeername returned: %s\n"),
		  strerror(errno));
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
	dbprintf(_("check_name_give_sockaddr(%s): %s\n"),
		  hostname, errmsg);
	amfree(errmsg);
	return;
    }


    rc = sec_tcp_conn_get(hostname, 0);
    rc->conf_fn = conf_fn;
    rc->datap = datap;
    rc->recv_security_ok = NULL;
    rc->prefix_packet = NULL;
    copy_sockaddr(&rc->peer, &sin);
    rc->read = in;
    rc->write = out;
    rc->driver = driver;
    if (gss_server(rc) < 0)
	error("gss_server failed: %s\n", rc->errmsg);
    rc->accept_fn = fn;
    sec_tcp_conn_read(rc);
}

/*
 * Forks a krb5 to the host listed in rc->hostname
 * Returns negative on error, with an errmsg in rc->errmsg.
 */
static int
runkrb5(
    struct sec_handle *	rh)
{
    struct servent *	sp;
    int			server_socket;
    in_port_t		my_port, port;
    struct tcp_conn *	rc = rh->rc;
    const char *err;

    if ((sp = getservbyname(AMANDA_KRB5_SERVICE_NAME, "tcp")) == NULL)
	port = htons(AMANDA_KRB5_DEFAULT_PORT);
    else
	port = sp->s_port;

    if ((err = get_tgt(keytab_name, principal_name)) != NULL) {
        security_seterror(&rh->sech, "%s: could not get TGT: %s",
            rc->hostname, err);
        return -1;
    }

    set_root_privs(1);
    server_socket = stream_client(rc->hostname,
				     (in_port_t)(ntohs(port)),
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

    rc->read = rc->write = server_socket;

    if (gss_client(rh) < 0) {
	return -1;
    }


    return 0;
}



/*

 * Negotiate a krb5 gss context from the client end.
 */
static int
gss_client(
    struct sec_handle *rh)
{
    struct sec_stream *rs = rh->rs;
    struct tcp_conn *rc = rs->rc;
    gss_buffer_desc send_tok, recv_tok, AA;
    gss_OID doid;
    OM_uint32 maj_stat, min_stat;
    unsigned int ret_flags;
    int rval = -1;
    int rvalue;
    gss_name_t gss_name;
    char *errmsg = NULL;

    auth_debug(1, "gss_client\n");

    send_tok.value = vstralloc("host/", rs->rc->hostname, NULL);
    send_tok.length = strlen(send_tok.value) + 1;
    maj_stat = gss_import_name(&min_stat, &send_tok, GSS_C_NULL_OID,
	&gss_name);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	security_seterror(&rh->sech, _("can't import name %s: %s"),
	    (char *)send_tok.value, gss_error(maj_stat, min_stat));
	amfree(send_tok.value);
	return (-1);
    }
    amfree(send_tok.value);
    rc->gss_context = GSS_C_NO_CONTEXT;
    maj_stat = gss_display_name(&min_stat, gss_name, &AA, &doid);
    dbprintf(_("gss_name %s\n"), (char *)AA.value);

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
	    &rc->gss_context,
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
	    security_seterror(&rh->sech,
		_("error getting gss context: %s %s"),
		gss_error(maj_stat, min_stat), (char *)send_tok.value);
	    goto done;
	}

	/*
	 * Send back the response
	 */
	if (send_tok.length != 0 && tcpm_send_token(rc, rc->write, rs->handle, &errmsg, send_tok.value, send_tok.length) < 0) {
	    security_seterror(&rh->sech, "%s", rc->errmsg);
	    gss_release_buffer(&min_stat, &send_tok);
	    goto done;
	}
	gss_release_buffer(&min_stat, &send_tok);

	/*
	 * If we need to continue, then register for more packets
	 */
	if (maj_stat != (OM_uint32)GSS_S_CONTINUE_NEEDED)
	    break;

        rvalue = tcpm_recv_token(rc, rc->read, &rc->handle, &rc->errmsg,
				 (void *)&recv_tok.value,
				 (ssize_t *)&recv_tok.length, 60);
	if (rvalue <= 0) {
	    if (rvalue < 0)
		security_seterror(&rh->sech,
		    _("recv error in gss loop: %s"), rc->errmsg);
	    else
		security_seterror(&rh->sech, _("EOF in gss loop"));
	    goto done;
	}
    }

    rval = 0;
    rc->auth = 1;
done:
    gss_release_name(&min_stat, &gss_name);
    return (rval);
}

/*
 * Negotiate a krb5 gss context from the server end.
 */
static int
gss_server(
    struct tcp_conn *rc)
{
    OM_uint32 maj_stat, min_stat, ret_flags;
    gss_buffer_desc send_tok, recv_tok, AA;
    gss_OID doid;
    gss_name_t gss_name;
    gss_cred_id_t gss_creds;
    char *p, *realm, *msg;
    int rval = -1;
    int rvalue;
    char errbuf[256];
    char *errmsg = NULL;

    auth_debug(1, "gss_server\n");

    assert(rc != NULL);

    /*
     * We need to be root while in gss_acquire_cred() to read the host key
     * out of the default keytab.  We also need to be root in
     * gss_accept_context() thanks to the replay cache code.
     */
    if (!set_root_privs(0)) {
	g_snprintf(errbuf, SIZEOF(errbuf),
	    _("can't take root privileges to read krb5 host key: %s"), strerror(errno));
	goto out;
    }

    rc->gss_context = GSS_C_NO_CONTEXT;
    send_tok.value = vstralloc("host/", myhostname, NULL);
    send_tok.length = strlen(send_tok.value) + 1;
    for (p = send_tok.value; *p != '\0'; p++) {
	if (isupper((int)*p))
	    *p = tolower(*p);
    }
    maj_stat = gss_import_name(&min_stat, &send_tok, GSS_C_NULL_OID,
	&gss_name);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	set_root_privs(0);
	g_snprintf(errbuf, SIZEOF(errbuf),
	    _("can't import name %s: %s"), (char *)send_tok.value,
	    gss_error(maj_stat, min_stat));
	amfree(send_tok.value);
	goto out;
    }
    amfree(send_tok.value);

    maj_stat = gss_display_name(&min_stat, gss_name, &AA, &doid);
    dbprintf(_("gss_name %s\n"), (char *)AA.value);
    maj_stat = gss_acquire_cred(&min_stat, gss_name, 0,
	GSS_C_NULL_OID_SET, GSS_C_ACCEPT, &gss_creds, NULL, NULL);
    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
	g_snprintf(errbuf, SIZEOF(errbuf),
	    _("can't acquire creds for host key host/%s: %s"), myhostname,
	    gss_error(maj_stat, min_stat));
	gss_release_name(&min_stat, &gss_name);
	set_root_privs(0);
	goto out;
    }
    gss_release_name(&min_stat, &gss_name);

    for (recv_tok.length = 0;;) {
	recv_tok.value = NULL;
        rvalue = tcpm_recv_token(rc, rc->read, &rc->handle, &rc->errmsg,
				 /* (void *) is to avoid type-punning warning */
				 (char **)(void *)&recv_tok.value,
				 (ssize_t *)&recv_tok.length, 60);
	if (rvalue <= 0) {
	    if (rvalue < 0) {
		g_snprintf(errbuf, SIZEOF(errbuf),
		    _("recv error in gss loop: %s"), rc->errmsg);
		amfree(rc->errmsg);
	    } else
		g_snprintf(errbuf, SIZEOF(errbuf), _("EOF in gss loop"));
	    goto out;
	}

	maj_stat = gss_accept_sec_context(&min_stat, &rc->gss_context,
	    gss_creds, &recv_tok, GSS_C_NO_CHANNEL_BINDINGS,
	    &gss_name, &doid, &send_tok, &ret_flags, NULL, NULL);

	if (maj_stat != (OM_uint32)GSS_S_COMPLETE &&
	    maj_stat != (OM_uint32)GSS_S_CONTINUE_NEEDED) {
	    g_snprintf(errbuf, SIZEOF(errbuf),
		_("error accepting context: %s"), gss_error(maj_stat, min_stat));
	    amfree(recv_tok.value);
	    goto out;
	}
	amfree(recv_tok.value);

	if (send_tok.length != 0 && tcpm_send_token(rc, rc->write, 0, &errmsg, send_tok.value, send_tok.length) < 0) {
	    strncpy(errbuf, rc->errmsg, SIZEOF(errbuf) - 1);
	    errbuf[SIZEOF(errbuf) - 1] = '\0';
	    amfree(rc->errmsg);
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
	g_snprintf(errbuf, SIZEOF(errbuf),
	    _("can't display gss name: %s"), gss_error(maj_stat, min_stat));
	gss_release_name(&min_stat, &gss_name);
	goto out;
    }
    gss_release_name(&min_stat, &gss_name);

    /* get rid of the realm */
    if ((p = strchr(send_tok.value, '@')) == NULL) {
	g_snprintf(errbuf, SIZEOF(errbuf),
	    _("malformed gss name: %s"), (char *)send_tok.value);
	amfree(send_tok.value);
	goto out;
    }
    *p = '\0';
    realm = ++p;

    /* 
     * If the principal doesn't match, complain
     */
    if ((msg = krb5_checkuser(rc->hostname, send_tok.value, realm)) != NULL) {
	g_snprintf(errbuf, SIZEOF(errbuf),
	    _("access not allowed from %s: %s"), (char *)send_tok.value, msg);
	amfree(send_tok.value);
	goto out;
    }
    amfree(send_tok.value);

    rval = 0;
out:
    set_root_privs(0);
    if (rval != 0) {
	rc->errmsg = stralloc(errbuf);
    } else {
	rc->auth = 1;
    }
    auth_debug(1, _("gss_server returning %d\n"), rval);
    return (rval);
}

/*
 * Setup some things about krb5.  This should only be called once.
 */
static void
krb5_init(void)
{
    static int beenhere = 0;
    char *p;
    char *myfqhostname=NULL;

    if (beenhere)
	return;
    beenhere = 1;

#ifndef BROKEN_MEMORY_CCACHE
    putenv(stralloc("KRB5_ENV_CCNAME=MEMORY:amanda_ccache"));
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
	char *ccache;
	ccache = malloc(128);
	g_snprintf(ccache, SIZEOF(ccache),
		 "KRB5_ENV_CCNAME=FILE:/tmp/amanda_ccache.%ld.%ld",
		 (long)geteuid(), (long)getpid());
	putenv(ccache);
    }
#endif

    gethostname(myhostname, SIZEOF(myhostname) - 1);
    myhostname[SIZEOF(myhostname) - 1] = '\0';

    /*
     * In case it isn't fully qualified, do a DNS lookup.  Ignore
     * any errors (this is best-effort).
     */
    if (resolve_hostname(myhostname, SOCK_STREAM, NULL, &myfqhostname) == 0
	&& myfqhostname != NULL) {
	strncpy(myhostname, myfqhostname, SIZEOF(myhostname)-1);
	myhostname[SIZEOF(myhostname)-1] = '\0';
	amfree(myfqhostname);
    }

    /*
     * Lowercase the results.  We assume all host/ principals will be
     * lowercased.
     */
    for (p = myhostname; *p != '\0'; p++) {
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
    g_snprintf(ccache, SIZEOF(ccache), "/tmp/amanda_ccache.%ld.%ld",
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
    krb5_keytab keytab;
    krb5_ccache ccache;
    krb5_timestamp now;
#ifdef KRB5_HEIMDAL_INCLUDES
    krb5_data tgtname = { KRB5_TGS_NAME_SIZE, KRB5_TGS_NAME };
#else
    krb5_data tgtname = { 0, KRB5_TGS_NAME_SIZE, KRB5_TGS_NAME };
#endif
    static char *error = NULL;

    if (error != NULL) {
	amfree(error);
	error = NULL;
    }
    if ((ret = krb5_init_context(&context)) != 0) {
	error = vstrallocf(_("error initializing krb5 context: %s"),
	    error_message(ret));
	return (error);
    }

    /*krb5_init_ets(context);*/

    if(!keytab_name) {
        error = vstrallocf(_("error  -- no krb5 keytab defined"));
        return(error);
    }

    if(!principal_name) {
        error = vstrallocf(_("error  -- no krb5 principal defined"));
        return(error);
    }

    /*
     * Resolve keytab file into a keytab object
     */
    if ((ret = krb5_kt_resolve(context, keytab_name, &keytab)) != 0) {
	error = vstrallocf(_("error resolving keytab %s: %s"), keytab_name, 
	    error_message(ret));
	return (error);
    }

    /*
     * Resolve the amanda service held in the keytab into a principal
     * object
     */
    ret = krb5_parse_name(context, principal_name, &client);
    if (ret != 0) {
	error = vstrallocf(_("error parsing %s: %s"), principal_name,
	    error_message(ret));
	return (error);
    }

#ifdef KRB5_HEIMDAL_INCLUDES
    ret = krb5_build_principal_ext(context, &server,
        krb5_realm_length(*krb5_princ_realm(context, client)),
        krb5_realm_data(*krb5_princ_realm(context, client)),
        tgtname.length, tgtname.data,
        krb5_realm_length(*krb5_princ_realm(context, client)),
        krb5_realm_data(*krb5_princ_realm(context, client)),
        0);
#else
    ret = krb5_build_principal_ext(context, &server,
	krb5_princ_realm(context, client)->length,
	krb5_princ_realm(context, client)->data,
	tgtname.length, tgtname.data,
	krb5_princ_realm(context, client)->length,
	krb5_princ_realm(context, client)->data,
	0);
#endif
    if (ret != 0) {
	error = vstrallocf(_("error while building server name: %s"),
	    error_message(ret));
	return (error);
    }

    ret = krb5_timeofday(context, &now);
    if (ret != 0) {
	error = vstrallocf(_("error getting time of day: %s"), error_message(ret));
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
	error = vstrallocf(_("error getting ticket for %s: %s"),
	    principal_name, error_message(ret));
	goto cleanup2;
    }

    if ((ret = krb5_cc_default(context, &ccache)) != 0) {
	error = vstrallocf(_("error initializing ccache: %s"), error_message(ret));
	goto cleanup;
    }
    if ((ret = krb5_cc_initialize(context, ccache, client)) != 0) {
	error = vstrallocf(_("error initializing ccache: %s"), error_message(ret));
	goto cleanup;
    }
    if ((ret = krb5_cc_store_cred(context, ccache, &creds)) != 0) {
	error = vstrallocf(_("error storing creds in ccache: %s"), 
	    error_message(ret));
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

#ifndef KDESTROY_VIA_UNLINK
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
#endif

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

static int
k5_encrypt(
    void *cookie,
    void *buf,
    ssize_t buflen,
    void **encbuf,
    ssize_t *encbuflen)
{
    struct tcp_conn *rc = cookie;
    gss_buffer_desc dectok;
    gss_buffer_desc enctok;
    OM_uint32 maj_stat, min_stat;
    int conf_state;

    if (rc->conf_fn && rc->conf_fn("kencrypt", rc->datap)) {
	auth_debug(1, _("krb5: k5_encrypt: enter %p\n"), rc);

	dectok.length = buflen;
	dectok.value  = buf;    

	if (rc->auth == 1) {
	    assert(rc->gss_context != GSS_C_NO_CONTEXT);
	    maj_stat = gss_seal(&min_stat, rc->gss_context, 1,
			        GSS_C_QOP_DEFAULT, &dectok, &conf_state,
				&enctok);
	    if (maj_stat != (OM_uint32)GSS_S_COMPLETE || conf_state == 0) {
		auth_debug(1, _("krb5 encrypt error to %s: %s\n"),
			   rc->hostname, gss_error(maj_stat, min_stat));
		return (-1);
	    }
	    auth_debug(1, _("krb5: k5_encrypt: give %zu bytes\n"),
		       enctok.length);
	    *encbuf = enctok.value;
	    *encbuflen = enctok.length;
	} else {
	    *encbuf = buf;
	    *encbuflen = buflen;
	}
	auth_debug(1, _("krb5: k5_encrypt: exit\n"));
    }
    return (0);
}


static int
k5_decrypt(
    void *cookie,
    void *buf,
    ssize_t buflen,
    void **decbuf,
    ssize_t *decbuflen)
{
    struct tcp_conn *rc = cookie;
    gss_buffer_desc enctok;
    gss_buffer_desc dectok;
    OM_uint32 maj_stat, min_stat;
    int conf_state, qop_state;

    if (rc->conf_fn && rc->conf_fn("kencrypt", rc->datap)) {
	auth_debug(1, _("krb5: k5_decrypt: enter\n"));
	if (rc->auth == 1) {
	    enctok.length = buflen;
	    enctok.value  = buf;    

	    auth_debug(1, _("krb5: k5_decrypt: decrypting %zu bytes\n"), enctok.length);

	    assert(rc->gss_context != GSS_C_NO_CONTEXT);
	    maj_stat = gss_unseal(&min_stat, rc->gss_context, &enctok, &dectok,
			      &conf_state, &qop_state);
	    if (maj_stat != (OM_uint32)GSS_S_COMPLETE) {
		auth_debug(1, _("krb5 decrypt error from %s: %s\n"),
			   rc->hostname, gss_error(maj_stat, min_stat));
		return (-1);
	    }
	    auth_debug(1, _("krb5: k5_decrypt: give %zu bytes\n"),
		       dectok.length);
	    *decbuf = dectok.value;
	    *decbuflen = dectok.length;
	} else {
	    *decbuf = buf;
	    *decbuflen = buflen;
	}
	auth_debug(1, _("krb5: k5_decrypt: exit\n"));
    } else {
	*decbuf = buf;
	*decbuflen = buflen;
    }
    return (0);
}

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
	return(vstrallocf(_("does not match compiled in default")));
    }
#else
    struct passwd *pwd;
    char *ptmp;
    char *result = _("generic error");	/* default is to not permit */
    FILE *fp = NULL;
    struct stat sbuf;
    uid_t localuid;
    char *line = NULL;
    char *filehost = NULL, *fileuser = NULL, *filerealm = NULL;

    assert( host != NULL);
    assert( name != NULL);

    if((pwd = getpwnam(CLIENT_LOGIN)) == NULL) {
	result = vstrallocf(_("can not find user %s"), CLIENT_LOGIN);
    }
    localuid = pwd->pw_uid;

#ifdef USE_AMANDAHOSTS
    ptmp = stralloc2(pwd->pw_dir, "/.k5amandahosts");
#else
    ptmp = stralloc2(pwd->pw_dir, "/.k5login");
#endif

    if(!ptmp) {
	result = vstrallocf(_("could not find home directory for %s"), CLIENT_LOGIN);
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
		result = vstrallocf(_("%s does not match %s"),
			name, CLIENT_LOGIN);
		return result;
	}
	result = NULL;
	goto common_exit;
    }

    auth_debug(1, _("opening ptmp: %s\n"), (ptmp)?ptmp: "NULL!");
    if((fp = fopen(ptmp, "r")) == NULL) {
	result = vstrallocf(_("can not open %s"), ptmp);
	return result;
    }
    auth_debug(1, _("opened ptmp\n"));

    if (fstat(fileno(fp), &sbuf) != 0) {
	result = vstrallocf(_("cannot fstat %s: %s"), ptmp, strerror(errno));
	goto common_exit;
    }

    if (sbuf.st_uid != localuid) {
	result = vstrallocf(_("%s is owned by %ld, should be %ld"),
		ptmp, (long)sbuf.st_uid, (long)localuid);
	goto common_exit;
    }
    if ((sbuf.st_mode & 077) != 0) {
	result = vstrallocf(
	    _("%s: incorrect permissions; file must be accessible only by its owner"), ptmp);
	goto common_exit;
    }

    while ((line = agets(fp)) != NULL) {
	if (line[0] == '\0') {
	    amfree(line);
	    continue;
	}

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
		auth_debug(1, _("found a host match\n"));
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
	auth_debug(1, _("comparing %s %s\n"), fileuser, name);
	if(strcmp(fileuser, name) == 0) {
		auth_debug(1, _("found a match!\n"));
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
    result = vstrallocf(_("no match in %s"), ptmp);

common_exit:
    afclose(fp);
    return(result);
#endif /* AMANDA_PRINCIPAL */
}
