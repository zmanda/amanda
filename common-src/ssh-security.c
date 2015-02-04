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
 * $Id: ssh-security.c,v 1.23 2006/08/21 20:17:10 martinea Exp $
 *
 * ssh-security.c - security and transport over ssh or a ssh-like command.
 *
 * XXX still need to check for initial keyword on connect so we can skip
 * over shell garbage and other stuff that ssh might want to spew out.
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
 * Number of seconds ssh has to start up
 */
#define	CONNECT_TIMEOUT	20

/*
 * Magic values for ssh_conn->handle
 */
#define	H_TAKEN	-1		/* ssh_conn->tok was already read */
#define	H_EOF	-2		/* this connection has been shut down */

/*
 * Interface functions
 */
static void	ssh_connect(const char *, char *(*)(char *, void *),
			void (*)(void *, security_handle_t *, security_status_t),
			void *, void *);
static void ssh_accept(const security_driver_t *driver, char *(*conf_fn)(char *, void *),
			int in, int out,
			void (*fn)(security_handle_t *, pkt_t *),
			void *datap);

/*
 * This is our interface to the outside world.
 */
const security_driver_t ssh_security_driver = {
    "SSH",
    ssh_connect,
    ssh_accept,
    sec_get_authenticated_peer_name_hostname,
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
static int runssh(struct tcp_conn *, const char *, const char *, const char *,
		  const char *);

/*
 * ssh version of a security handle allocator.  Logically sets
 * up a network "connection".
 */
static void
ssh_connect(
    const char *	hostname,
    char *		(*conf_fn)(char *, void *),
    void		(*fn)(void *, security_handle_t *, security_status_t),
    void *		arg,
    void *		datap)
{
    struct sec_handle *rh;
    char *amandad_path=NULL, *client_username=NULL, *ssh_keys=NULL;
    char *client_port = NULL;

    assert(fn != NULL);
    assert(hostname != NULL);

    auth_debug(1, "ssh_connect: %s\n", hostname);

    rh = g_new0(struct sec_handle, 1);
    security_handleinit(&rh->sech, &ssh_security_driver);
    rh->hostname = NULL;
    rh->rs = NULL;
    rh->ev_timeout = NULL;
    rh->rc = NULL;

    rh->hostname = g_strdup(hostname);
    rh->rs = tcpma_stream_client(rh, newhandle++);
    rh->rc->conf_fn = conf_fn;
    rh->rc->datap = datap;

    if (rh->rs == NULL)
	goto error;

    amfree(rh->hostname);
    rh->hostname = stralloc(rh->rs->rc->hostname);

    /*
     * We need to open a new connection.
     *
     * XXX need to eventually limit number of outgoing connections here.
     */
    if(conf_fn) {
	char *port_str;
	amandad_path    = conf_fn("amandad_path", datap);
	client_username = conf_fn("client_username", datap);
	ssh_keys        = conf_fn("ssh_keys", datap);
	port_str        = conf_fn("client_port", datap);
	if (port_str && strlen(port_str) >= 1) {
	    client_port = port_str;
	}
    }
    if(rh->rc->read == -1) {
	if (runssh(rh->rs->rc, amandad_path, client_username, ssh_keys,
		   client_port) < 0) {
	    security_seterror(&rh->sech, _("can't connect to %s: %s"),
			      hostname, rh->rs->rc->errmsg);
	    goto error;
	}
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
    rh->rs->ev_read = event_register((event_id_t)rh->rs->rc->write, EV_WRITEFD,
	sec_connect_callback, rh);
    rh->ev_timeout = event_register((event_id_t)CONNECT_TIMEOUT, EV_TIME,
	sec_connect_timeout, rh);

    return;

error:
    (*fn)(arg, &rh->sech, S_ERROR);
}

/* like sec_accept, but first it gets the remote system's hostname */
static void
ssh_accept(
    const security_driver_t *driver,
    char       *(*conf_fn)(char *, void *),
    int		in,
    int		out,
    void	(*fn)(security_handle_t *, pkt_t *),
    void       *datap)
{
    struct sec_handle *rh;
    struct tcp_conn *rc = sec_tcp_conn_get("", 0);
    char *ssh_connection, *p;
    char *errmsg = NULL;
    sockaddr_union addr;
    int result;

    /* "Accepting" an SSH connection means that amandad was invoked via sshd, so
     * we should have anSSH_CONNECTION env var.  If not, then this probably isn't
     * a real SSH connection and we should bail out. */
    ssh_connection = getenv("SSH_CONNECTION");
    if (!ssh_connection) {
	errmsg = g_strdup("$SSH_CONNECTION not set - was amandad started by sshd?");
	goto error;
    }

    /* make a local copy, to munge */
    ssh_connection = g_strdup(ssh_connection);

    /* strip off the first component - the ASCII IP address */
    if ((p = strchr(ssh_connection, ' ')) == NULL) {
	errmsg = g_strdup("$SSH_CONNECTION malformed");
	goto error;
    }
    *p = '\0';

    /* ---- everything from here on is just a warning, leaving hostname at "" */

    SU_INIT(&addr, AF_INET);

    /* turn the string address into a sockaddr */
    if ((result = str_to_sockaddr(ssh_connection, &addr)) != 1) {
	if (result == 0) {
	    g_warning("Could not parse peer address %s", ssh_connection);
	} else {
	    g_warning("Parsing peer address %s: %s", ssh_connection, gai_strerror(result));
	}
	goto done;
    }

    /* find the hostname */
    result = getnameinfo((struct sockaddr *)&addr, SS_LEN(&addr),
		 rc->hostname, sizeof(rc->hostname), NULL, 0, 0);
    if (result != 0) {
	g_warning("Could not get hostname for SSH client %s: %s", ssh_connection,
		gai_strerror(result));
	goto done;
    }

    /* and verify it */
    if (check_name_give_sockaddr(rc->hostname,
				 (struct sockaddr *)&addr, &errmsg) < 0) {
	rc->hostname[0] = '\0'; /* null out the bad hostname */
	g_warning("Checking SSH client DNS: %s", errmsg);
	amfree(errmsg);
	goto done;
    }

done:
    if (ssh_connection)
	g_free(ssh_connection);

    rc->read = in;
    rc->write = out;
    rc->accept_fn = fn;
    rc->driver = driver;
    rc->conf_fn = conf_fn;
    rc->datap = datap;
    sec_tcp_conn_read(rc);
    return;

error:
    if (ssh_connection)
	g_free(ssh_connection);

    /* make up a fake handle for the error */
    rh = g_new0(struct sec_handle, 1);
    security_handleinit(&rh->sech, driver);
    security_seterror((security_handle_t*)rh, "ssh_accept: %s", errmsg);
    amfree(errmsg);
    (*fn)(&rh->sech, NULL);
}

/*
 * Forks a ssh to the host listed in rc->hostname
 * Returns negative on error, with an errmsg in rc->errmsg.
 */
static int
runssh(
    struct tcp_conn *	rc,
    const char *	amandad_path,
    const char *	client_username,
    const char *	ssh_keys,
    const char *        client_port)
{
    int rpipe[2], wpipe[2];
    char *xamandad_path = (char *)amandad_path;
    char *xclient_username = (char *)client_username;
    char *xssh_keys = (char *)ssh_keys;
    char *xclient_port = (char *)client_port;
    GPtrArray *myargs;
    gchar *ssh_options[100] = {SSH_OPTIONS, NULL};
    gchar **ssh_option;
    gchar *cmd;

    memset(rpipe, -1, SIZEOF(rpipe));
    memset(wpipe, -1, SIZEOF(wpipe));
    if (pipe(rpipe) < 0 || pipe(wpipe) < 0) {
	rc->errmsg = newvstrallocf(rc->errmsg, _("pipe: %s"), strerror(errno));
	return (-1);
    }

    if(!xamandad_path || strlen(xamandad_path) <= 1) 
	xamandad_path = vstralloc(amlibexecdir, "/", "amandad", NULL);
    if(!xclient_username || strlen(xclient_username) <= 1)
	xclient_username = CLIENT_LOGIN;
    if(!xclient_port || strlen(xclient_port) <= 1)
	xclient_port = NULL;

    myargs = g_ptr_array_sized_new(20);
    g_ptr_array_add(myargs, SSH);
    for (ssh_option = ssh_options; *ssh_option != NULL; ssh_option++) {
	g_ptr_array_add(myargs, *ssh_option);
    }
    g_ptr_array_add(myargs, "-l");
    g_ptr_array_add(myargs, xclient_username);
    if (xclient_port) {
	g_ptr_array_add(myargs, "-p");
	g_ptr_array_add(myargs, xclient_port);
    }
    if (ssh_keys && strlen(ssh_keys) > 1) {
	g_ptr_array_add(myargs, "-i");
	g_ptr_array_add(myargs, xssh_keys);
    }
    g_ptr_array_add(myargs, rc->hostname);
    g_ptr_array_add(myargs, xamandad_path);
    g_ptr_array_add(myargs, "-auth=ssh");
    g_ptr_array_add(myargs, NULL);

    cmd = g_strjoinv(" ", (gchar **)myargs->pdata);
    g_debug("exec: %s", cmd);
    g_free(cmd);

    switch (rc->pid = fork()) {
    case -1:
	rc->errmsg = newvstrallocf(rc->errmsg, _("fork: %s"), strerror(errno));
	aclose(rpipe[0]);
	aclose(rpipe[1]);
	aclose(wpipe[0]);
	aclose(wpipe[1]);
	return (-1);
    case 0:
	dup2(wpipe[0], 0);
	dup2(rpipe[1], 1);
	break;
    default:
	rc->read = rpipe[0];
	aclose(rpipe[1]);
	rc->write = wpipe[1];
	aclose(wpipe[0]);
	return (0);
    }

    /* drop root privs for good */
    set_root_privs(-1);

    safe_fd(-1, 0);

    execvp(SSH, (gchar **)myargs->pdata);

    error("error: couldn't exec %s: %s", SSH, strerror(errno));

    /* should never go here, shut up compiler warning */
    return(-1);
}
