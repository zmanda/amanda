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
 * $Id: local-security.c 6512 2007-05-24 17:00:24Z ian $
 *
 * local-security.c - security and transport over local or a local-like command.
 *
 * XXX still need to check for initial keyword on connect so we can skip
 * over shell garbage and other stuff that local might want to spew out.
 */

#include "amanda.h"
#include "util.h"
#include "event.h"
#include "packet.h"
#include "security.h"
#include "security-util.h"
#include "stream.h"

/*
 * Number of seconds amandad has to start up
 */
#define CONNECT_TIMEOUT 20

/*
 * Interface functions
 */
static void local_connect(const char *, char *(*)(char *, void *),
			void (*)(void *, security_handle_t *, security_status_t),
			void *, void *);

/*
 * This is our interface to the outside world.
 */
const security_driver_t local_security_driver = {
    "LOCAL",
    local_connect,
    sec_accept,
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
static int runlocal(struct tcp_conn *, const char *, const char *);


/*
 * local version of a security handle allocator.  Logically sets
 * up a network "connection".
 */
static void
local_connect(
    const char *	hostname,
    char *		(*conf_fn)(char *, void *),
    void		(*fn)(void *, security_handle_t *, security_status_t),
    void *		arg,
    void *		datap)
{
    struct sec_handle *rh;
    char *amandad_path=NULL;
    char *client_username=NULL;
    char myhostname[MAX_HOSTNAME_LENGTH+1];

    assert(fn != NULL);
    assert(hostname != NULL);

    auth_debug(1, _("local: local_connect: %s\n"), hostname);

    rh = alloc(SIZEOF(*rh));
    security_handleinit(&rh->sech, &local_security_driver);
    rh->hostname = NULL;
    rh->rs = NULL;
    rh->ev_timeout = NULL;
    rh->rc = NULL;

    if (gethostname(myhostname, MAX_HOSTNAME_LENGTH) == -1) {
	security_seterror(&rh->sech, _("gethostname failed"));
	(*fn)(arg, &rh->sech, S_ERROR);
	return;
    }
    myhostname[SIZEOF(myhostname)-1] = '\0';

    if (strcmp(hostname, myhostname) != 0 &&
	match("^localhost(\\.localdomain)?$", hostname) == 0) {
	security_seterror(&rh->sech,
	    _("%s: is not local"), hostname);
	(*fn)(arg, &rh->sech, S_ERROR);
	return;
    }
    rh->hostname = stralloc(hostname);
    rh->rs = tcpma_stream_client(rh, newhandle++);

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
	amandad_path    = conf_fn("amandad_path", datap);
	client_username = conf_fn("client_username", datap);
    }
    if(rh->rc->read == -1) {
	if (runlocal(rh->rs->rc, amandad_path, client_username) < 0) {
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
    amfree(rh->hostname);
}

/*
 * Forks a local to the host listed in rc->hostname
 * Returns negative on error, with an errmsg in rc->errmsg.
 */
static int
runlocal(
    struct tcp_conn *	rc,
    const char *	amandad_path,
    const char *	client_username G_GNUC_UNUSED)
{
    int rpipe[2], wpipe[2];
    char *xamandad_path = (char *)amandad_path;

#ifndef SINGLE_USERID
    struct passwd *pwd = NULL;
    uid_t uid = 0;
    gid_t gid = 0;

    if (getuid() == 0) {
	if (client_username && strlen(client_username) > 1) {
	    pwd = getpwnam(client_username);
            if (!pwd) {
		dbprintf("User '%s' doesn't exist\n", client_username);
	    } else {
		uid = pwd->pw_uid;
		gid = pwd->pw_gid;
	    }
	}
	if (!pwd) {
	    uid = get_client_uid();
	    gid = get_client_gid();
	}
    }
#endif

    memset(rpipe, -1, SIZEOF(rpipe));
    memset(wpipe, -1, SIZEOF(wpipe));
    if (pipe(rpipe) < 0 || pipe(wpipe) < 0) {
	rc->errmsg = newvstrallocf(rc->errmsg, _("pipe: %s"), strerror(errno));
	return (-1);
    }

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

    safe_fd(-1, 0);

    if(!xamandad_path || strlen(xamandad_path) <= 1) 
	xamandad_path = vstralloc(amlibexecdir, "/", "amandad", NULL);

#ifndef SINGLE_USERID
    if (uid != 0)
	setreuid(uid, uid);
    if (gid != 0)
	setregid(gid, gid);
#endif

    execlp(xamandad_path, xamandad_path,
	   "-auth=local", "amdump", "amindexd", "amidxtaped", (char *)NULL);
    error(_("error: couldn't exec %s: %s"), xamandad_path, strerror(errno));

    /* should never go here, shut up compiler warning */
    return(-1);
}
