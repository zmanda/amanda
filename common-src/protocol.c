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
 * $Id: protocol.c,v 1.45 2006/05/25 17:07:31 martinea Exp $
 *
 * implements amanda protocol
 */
#include "amanda.h"
#include "conffile.h"
#include "event.h"
#include "packet.h"
#include "security.h"
#include "protocol.h"

#define proto_debug(i, ...) do {	\
       if ((i) <= debug_protocol) {	\
           dbprintf(__VA_ARGS__);	\
       }				\
} while (0)

/*
 * Valid actions that can be passed to the state machine
 */
typedef enum {
	PA_START,
	PA_TIMEOUT,
	PA_ERROR,
	PA_RCVDATA,
	PA_CONTPEND,
	PA_PENDING,
	PA_CONTINUE,
	PA_FINISH,
	PA_ABORT
} p_action_t;

/*
 * The current state type.  States are represented as function
 * vectors.
 */
struct proto;
typedef p_action_t (*pstate_t)(struct proto *, p_action_t, pkt_t *);

/*
 * This is a request structure that is wrapped around a packet while it
 * is being passed through amanda.  It holds the timeouts, state, and handles
 * for each request.
 */
typedef struct proto {
    pstate_t state;			/* current state of the request */
    char *hostname;			/* remote host */
    const security_driver_t *security_driver;	/* for connect retries */
    security_handle_t *security_handle;	/* network stream for this req */
    time_t timeout;			/* seconds for this timeout */
    time_t repwait;			/* seconds to wait for reply */
    time_t origtime;			/* orig start time of this request */
    time_t curtime;			/* time when this attempt started */
    int connecttries;			/* times we'll retry a connect */
    int resettries;			/* times we'll resend a REQ */
    int reqtries;			/* times we'll wait for an a ACK */
    pkt_t req;				/* the actual wire request */
    protocol_sendreq_callback continuation; /* call when req dies/finishes */
    void *datap;			/* opaque cookie passed to above */
    char *(*conf_fn)(char *, void *);	/* configuration function */
} proto_t;

#define	CONNECT_WAIT	5	/* secs between connect attempts */
#define ACK_WAIT	10	/* time (secs) to wait for ACK - keep short */
#define RESET_TRIES	2	/* num restarts (reboot/crash) */
#define CURTIME	(time(0) - proto_init_time) /* time relative to start */

/* if no reply in an hour, just forget it */
#define	DROP_DEAD_TIME(t)	(CURTIME - (t) > (60 * 60))

/* get the size of an array */
#define	ASIZE(arr)	(int)(sizeof(arr) / sizeof((arr)[0]))

/*
 * Initialization time
 */
static time_t proto_init_time;

/* local functions */

static const char *action2str(p_action_t);
static const char *pstate2str(pstate_t);

static void connect_callback(void *, security_handle_t *, security_status_t);
static void connect_wait_callback(void *);
static void recvpkt_callback(void *, pkt_t *, security_status_t);

static p_action_t s_sendreq(proto_t *, p_action_t, pkt_t *);
static p_action_t s_ackwait(proto_t *, p_action_t, pkt_t *);
static p_action_t s_repwait(proto_t *, p_action_t, pkt_t *);
static void state_machine(proto_t *, p_action_t, pkt_t *);

/*
 * -------------------
 * Interface functions
 */

/*
 * Initialize globals.
 */
void
protocol_init(void)
{

    proto_init_time = time(NULL);
}

/*
 * Generate a request packet, and submit it to the state machine
 * for transmission.
 */
void
protocol_sendreq(
    const char *		hostname,
    const security_driver_t *	security_driver,
    char *			(*conf_fn)(char *, void *),
    const char *		req,
    time_t			repwait,
    protocol_sendreq_callback	continuation,
    void *			datap)
{
    proto_t *p;

    p = alloc(SIZEOF(proto_t));
    p->state = s_sendreq;
    p->hostname = stralloc(hostname);
    p->security_driver = security_driver;
    /* p->security_handle set in connect_callback */
    p->repwait = repwait;
    p->origtime = CURTIME;
    /* p->curtime set in the sendreq state */
    p->connecttries = getconf_int(CNF_CONNECT_TRIES);
    p->resettries = RESET_TRIES;
    p->reqtries = getconf_int(CNF_REQ_TRIES);
    p->conf_fn = conf_fn;
    pkt_init(&p->req, P_REQ, "%s", req);

    /*
     * These are here for the caller
     * We call the continuation function after processing is complete.
     * We pass the datap on through untouched.  It is here so the caller
     * has a way to keep state with each request.
     */
    p->continuation = continuation;
    p->datap = datap;

    proto_debug(1, _("protocol: security_connect: host %s -> p %p\n"), 
		    hostname, p);

    security_connect(p->security_driver, p->hostname, conf_fn, connect_callback,
			 p, p->datap);
}

/*
 * This is a callback for security_connect.  After the security layer
 * has initiated a connection to the given host, this will be called
 * with a security_handle_t.
 *
 * On error, the security_status_t arg will reflect errors which can
 * be had via security_geterror on the handle.
 */
static void
connect_callback(
    void *		cookie,
    security_handle_t *	security_handle,
    security_status_t	status)
{
    proto_t *p = cookie;

    assert(p != NULL);
    p->security_handle = security_handle;

    proto_debug(1, _("protocol: connect_callback: p %p\n"), p);

    switch (status) {
    case S_OK:
	state_machine(p, PA_START, NULL);
	break;

    case S_TIMEOUT:
	security_seterror(p->security_handle, _("timeout during connect"));
	/* FALLTHROUGH */

    case S_ERROR:
	/*
	 * For timeouts or errors, retry a few times, waiting CONNECT_WAIT
	 * seconds between each attempt.  If they all fail, just return
	 * an error back to the caller.
	 */
	if (--p->connecttries == 0) {
	    state_machine(p, PA_ABORT, NULL);
	} else {
	    proto_debug(1, _("protocol: connect_callback: p %p: retrying %s\n"),
			    p, p->hostname);
	    security_close(p->security_handle);
	    /* XXX overload p->security handle to hold the event handle */
	    p->security_handle =
		(security_handle_t *)event_register(CONNECT_WAIT, EV_TIME,
		connect_wait_callback, p);
	}
	break;

    default:
	assert(0);
	break;
    }
}

/*
 * This gets called when a host has been put on a wait queue because
 * initial connection attempts failed.
 */
static void
connect_wait_callback(
    void *	cookie)
{
    proto_t *p = cookie;

    event_release((event_handle_t *)p->security_handle);
    security_connect(p->security_driver, p->hostname, p->conf_fn,
	connect_callback, p, p->datap);
}


/*
 * Does a one pass protocol sweep.  Handles any incoming packets that 
 * are waiting to be processed, and then deals with any pending
 * requests that have timed out.
 *
 * Callers should periodically call this after they have submitted
 * requests if they plan on doing a lot of work.
 */
void
protocol_check(void)
{

    /* arg == 1 means don't block */
    event_loop(1);
}


/*
 * Does an infinite pass protocol sweep.  This doesn't return until all
 * requests have been satisfied or have timed out.
 *
 * Callers should call this after they have finished submitting requests
 * and are just waiting for all of the answers to come back.
 */
void
protocol_run(void)
{

    /* arg == 0 means block forever until no more events are left */
    event_loop(0);
}


/*
 * ------------------
 * Internal functions
 */

/*
 * The guts of the protocol.  This handles the many paths a request can
 * make, including retrying the request and acknowledgements, and dealing
 * with timeouts and successfull replies.
 */
static void
state_machine(
    proto_t *	p,
    p_action_t	action,
    pkt_t *	pkt)
{
    pstate_t curstate;
    p_action_t retaction;

    proto_debug(1, _("protocol: state_machine: initial: p %p action %s pkt %p\n"),
		    p, action2str(action), (void *)NULL);

    assert(p != NULL);
    assert(action == PA_RCVDATA || pkt == NULL);
    assert(p->state != NULL);

    for (;;) {
	proto_debug(1, _("protocol: state_machine: p %p state %s action %s\n"),
			p, pstate2str(p->state), action2str(action));
	if (pkt != NULL) {
	    proto_debug(1, _("protocol: pkt: %s (t %d) orig REQ (t %d cur %d)\n"),
			    pkt_type2str(pkt->type), (int)CURTIME,
			    (int)p->origtime, (int)p->curtime);
	    proto_debug(1, _("protocol: pkt contents:\n-----\n%s-----\n"),
			    pkt->body);
	}

	/*
	 * p->state is a function pointer to the current state a request
	 * is in.
	 *
	 * We keep track of the last state we were in so we can make
	 * sure states which return PA_CONTINUE really have transitioned
	 * the request to a new state.
	 */
	curstate = p->state;

	if (action == PA_ABORT)
	    /*
	     * If the passed action indicates a terminal error, then we
	     * need to move to abort right away.
	     */
	    retaction = PA_ABORT;
	else
	    /*
	     * Else we run the state and perform the action it
	     * requests.
	     */
	    retaction = (*curstate)(p, action, pkt);

	proto_debug(1, _("protocol: state_machine: p %p state %s returned %s\n"),
			p, pstate2str(p->state), action2str(retaction));

	/*
	 * The state function is expected to return one of the following
	 * p_action_t's.
	 */
	switch (retaction) {

	/*
	 * Request is still waiting for more data off of the network.
	 * Setup to receive another pkt, and wait for the recv event
	 * to occur.
	 */
	case PA_CONTPEND:
	    (*p->continuation)(p->datap, pkt, p->security_handle);
	    /* FALLTHROUGH */

	case PA_PENDING:
	    proto_debug(1, _("protocol: state_machine: p %p state %s: timeout %d\n"),
			    p, pstate2str(p->state), (int)p->timeout);
	    /*
	     * Get the security layer to register a receive event for this
	     * security handle on our behalf.  Have it timeout in p->timeout
	     * seconds.
	     */
	    security_recvpkt(p->security_handle, recvpkt_callback, p,
		(int)p->timeout);

	    return;

	/*
	 * Request has moved to another state.  Loop and run it again.
	 */
	case PA_CONTINUE:
	    assert(p->state != curstate);
	    proto_debug(1, _("protocol: state_machine: p %p: moved from %s to %s\n"),
			    p, pstate2str(curstate),
			    pstate2str(p->state));
	    continue;

	/*
	 * Request has failed in some way locally.  The security_handle will
	 * contain an appropriate error message via security_geterror().  Set
	 * pkt to NULL to indicate failure to the callback, and then
	 * fall through to the common finish code.
	 *
	 * Note that remote failures finish via PA_FINISH, because they did
	 * complete successfully locally.
	 */
	case PA_ABORT:
	    pkt = NULL;
	    /* FALLTHROUGH */

	/*
	 * Request has completed successfully.
	 * Free up resources the request has used, call the continuation
	 * function specified by the caller and quit.
	 */
	case PA_FINISH:
	    (*p->continuation)(p->datap, pkt, p->security_handle);
	    security_close(p->security_handle);
	    amfree(p->hostname);
	    amfree(p->req.body);
	    amfree(p);
	    return;

	default:
	    assert(0);
	    break;	/* in case asserts are turned off */
	}
	/*NOTREACHED*/
    }
    /*NOTREACHED*/
}

/*
 * The request send state.  Here, the packet is actually transmitted
 * across the network.  After setting up timeouts, the request
 * moves to the acknowledgement wait state.  We return from the state
 * machine at this point, and let the request be received from the network.
 */
static p_action_t
s_sendreq(
    proto_t *	p,
    p_action_t	action,
    pkt_t *	pkt)
{

    assert(p != NULL);
    (void)action;	/* Quiet unused parameter warning */
    (void)pkt;		/* Quiet unused parameter warning */

    if (security_sendpkt(p->security_handle, &p->req) < 0) {
	/* XXX should retry */
	security_seterror(p->security_handle, _("error sending REQ: %s"),
	    security_geterror(p->security_handle));
	return (PA_ABORT);
    }

    /*
     * Remember when this request was first sent
     */
    p->curtime = CURTIME;

    /*
     * Move to the ackwait state
     */
    p->state = s_ackwait;
    p->timeout = ACK_WAIT;
    return (PA_PENDING);
}

/*
 * The acknowledge wait state.  We can enter here two ways:
 *
 *  - the caller has received a packet, located the request for
 *    that packet, and called us with an action of PA_RCVDATA.
 *    
 *  - the caller has determined that a request has timed out,
 *    and has called us with PA_TIMEOUT.
 *
 * Here we process the acknowledgment, which usually means that
 * the client has agreed to our request and is working on it.
 * It will later send a reply when finished.
 */
static p_action_t
s_ackwait(
    proto_t *	p,
    p_action_t	action,
    pkt_t *	pkt)
{

    assert(p != NULL);

    /*
     * The timeout case.  If our retry count has gone to zero
     * fail this request.  Otherwise, move to the send state
     * to retry the request.
     */
    if (action == PA_TIMEOUT) {
	assert(pkt == NULL);

	if (--p->reqtries == 0) {
	    security_seterror(p->security_handle, _("timeout waiting for ACK"));
	    return (PA_ABORT);
	}

	p->state = s_sendreq;
	return (PA_CONTINUE);
    }

    assert(action == PA_RCVDATA);
    assert(pkt != NULL);

    /*
     * The packet-received state.  Determine what kind of
     * packet we received, and act based on the reply type.
     */
    switch (pkt->type) {

    /*
     * Received an ACK.  Everything's good.  The client is
     * now working on the request.  We queue up again and
     * wait for the reply.
     */
    case P_ACK:
	p->state = s_repwait;
	p->timeout = p->repwait;
	return (PA_PENDING);

    /*
     * Received a NAK.  The request failed, so free up the
     * resources associated with it and return.
     *
     * This should NOT return PA_ABORT because it is not a local failure.
     */
    case P_NAK:
	return (PA_FINISH);

    /*
     * The client skipped the ACK, and replied right away.
     * Move to the reply state to handle it.
     */
    case P_REP:
    case P_PREP:
	p->state = s_repwait;
	return (PA_CONTINUE);

    /*
     * Unexpected packet.  Requeue this request and hope
     * we get what we want later.
     */
    default:
	return (PA_PENDING);
    }
}

/*
 * The reply wait state.  We enter here much like we do with s_ackwait.
 */
static p_action_t
s_repwait(
    proto_t *	p,
    p_action_t	action,
    pkt_t *	pkt)
{
    pkt_t ack;

    /*
     * Timeout waiting for a reply.
     */
    if (action == PA_TIMEOUT) {
	assert(pkt == NULL);

	/*
	 * If we've blown our timeout limit, free up this packet and
	 * return.
	 */
	if (p->resettries == 0 || DROP_DEAD_TIME(p->origtime)) {
	    security_seterror(p->security_handle, _("timeout waiting for REP"));
	    return (PA_ABORT);
	}

	/*
	 * We still have some tries left.  Resend the request.
	 */
	p->resettries--;
	p->state = s_sendreq;
	p->reqtries = getconf_int(CNF_REQ_TRIES);
	return (PA_CONTINUE);
    }

    assert(action == PA_RCVDATA);

    /* Finish if we get a NAK */
    if (pkt->type == P_NAK)
	return (PA_FINISH);

    /*
     * We've received some data.  If we didn't get a reply,
     * requeue the packet and retry.  Otherwise, acknowledge
     * the reply, cleanup this packet, and return.
     */
    if (pkt->type != P_REP && pkt->type != P_PREP)
	return (PA_PENDING);

    if(pkt->type == P_REP) {
	pkt_init_empty(&ack, P_ACK);
	if (security_sendpkt(p->security_handle, &ack) < 0) {
	    /* XXX should retry */
	    amfree(ack.body);
	    security_seterror(p->security_handle, _("error sending ACK: %s"),
		security_geterror(p->security_handle));
	    return (PA_ABORT);
	}
	amfree(ack.body);
	return (PA_FINISH);
    }
    else if(pkt->type == P_PREP) {
	p->timeout = p->repwait - CURTIME + p->curtime + 1;
	return (PA_CONTPEND);
    }

    /* should never go here, shut up compiler warning */
    return (PA_FINISH);
}

/*
 * event callback that receives a packet
 */
static void
recvpkt_callback(
    void *		cookie,
    pkt_t *		pkt,
    security_status_t	status)
{
    proto_t *p = cookie;

    assert(p != NULL);

    switch (status) {
    case S_OK:
	state_machine(p, PA_RCVDATA, pkt);
	break;
    case S_TIMEOUT:
	state_machine(p, PA_TIMEOUT, NULL);
	break;
    case S_ERROR:
	state_machine(p, PA_ABORT, NULL);
	break;
    default:
	assert(0);
	break;
    }
}

/*
 * --------------
 * Misc functions
 */

/*
 * Convert a pstate_t into a printable form.
 */
static const char *
pstate2str(
    pstate_t	pstate)
{
    static const struct {
	pstate_t type;
	const char name[12];
    } pstates[] = {
#define	X(s)	{ s, stringize(s) }
	X(s_sendreq),
	X(s_ackwait),
	X(s_repwait),
#undef X
    };
    int i;

    for (i = 0; i < ASIZE(pstates); i++)
	if (pstate == pstates[i].type)
	    return (pstates[i].name);
    return (_("BOGUS PSTATE"));
}

/*
 * Convert an p_action_t into a printable form
 */
static const char *
action2str(
    p_action_t	action)
{
    static const struct {
	p_action_t type;
	const char name[12];
    } actions[] = {
#define	X(s)	{ s, stringize(s) }
	X(PA_START),
	X(PA_TIMEOUT),
	X(PA_ERROR),
	X(PA_RCVDATA),
	X(PA_CONTPEND),
	X(PA_PENDING),
	X(PA_CONTINUE),
	X(PA_FINISH),
	X(PA_ABORT),
#undef X
    };
    int i;

    for (i = 0; i < ASIZE(actions); i++)
	if (action == actions[i].type)
	    return (actions[i].name);
    return (_("BOGUS ACTION"));
}
