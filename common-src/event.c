/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1999 University of Maryland at College Park
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
 * $Id: event.c,v 1.24 2006/06/16 10:55:05 martinea Exp $
 *
 * Event handler.  Serializes different kinds of events to allow for
 * a uniform interface, central state storage, and centralized
 * interdependency logic.
 */

/*#define	EVENT_DEBUG*/

#ifdef EVENT_DEBUG
#define eventprintf(x)    dbprintf(x)
#else
#define eventprintf(x)
#endif

#include "amanda.h"
#include "event.h"
#include "queue.h"

/*
 * The opaque handle passed back to the caller.  This is typedefed to
 * event_handle_t in our header file.
 */
struct event_handle {
    event_fn_t fn;		/* function to call when this fires */
    void *arg;			/* argument to pass to previous function */
    event_type_t type;		/* type of event */
    event_id_t data;		/* type data */
    time_t lastfired;		/* timestamp of last fired (EV_TIME only) */
    LIST_ENTRY(event_handle) le; /* queue handle */
};

/*
 * eventq is a queue of currently active events.
 * cache is a queue of unused handles.  We keep a few around to avoid
 * malloc overhead when doing a lot of register/releases.
 */
static struct {
    LIST_HEAD(, event_handle) listhead;
    int qlength;
} eventq = {
    LIST_HEAD_INITIALIZER(eventq.listhead), 0
}, cache = {
    LIST_HEAD_INITIALIZER(eventq.listhead), 0
};
#define	eventq_first(q)		LIST_FIRST(&q.listhead)
#define	eventq_next(eh)		LIST_NEXT(eh, le)
#define	eventq_add(q, eh)	LIST_INSERT_HEAD(&q.listhead, eh, le);
#define	eventq_remove(eh)	LIST_REMOVE(eh, le);

/*
 * How many items we can have in the handle cache before we start
 * freeing.
 */
#define	CACHEDEPTH	10

/*
 * A table of currently set signal handlers.
 */
static struct sigtabent {
    event_handle_t *handle;	/* handle for this signal */
    int score;			/* number of signals recvd since last checked */
    void (*oldhandler)(int);/* old handler (for unsetting) */
} sigtable[NSIG];

#ifdef EVENT_DEBUG
static const char *event_type2str(event_type_t);
#endif
#define	fire(eh)	(*(eh)->fn)((eh)->arg)
static void signal_handler(int);
static event_handle_t *gethandle(void);
static void puthandle(event_handle_t *);
static int event_loop_wait (event_handle_t *, const int);

/*
 * Add a new event.  See the comment in event.h for what the arguments
 * mean.
 */
event_handle_t *
event_register(
    event_id_t data,
    event_type_t type,
    event_fn_t fn,
    void *arg)
{
    event_handle_t *handle;

    if ((type == EV_READFD) || (type == EV_WRITEFD)) {
	/* make sure we aren't given a high fd that will overflow a fd_set */
	if (data >= FD_SETSIZE) {
	    error("event_register: Invalid file descriptor %d", data);
	    /*NOTREACHED*/
	}
#if !defined(__lint) /* Global checking knows that these are never called */
    } else if (type == EV_SIG) {
	/* make sure signals are within range */
	if (data >= NSIG) {
	    error("event_register: Invalid signal %d", data);
	    /*NOTREACHED*/
	}
	if (sigtable[data].handle != NULL) { 
	    error("event_register: signal %d already registered", data);
	    /*NOTREACHED*/
	}
    } else if (type >= EV_DEAD) {
	error("event_register: Invalid event type %d", type);
	/*NOTREACHED*/
#endif
    }

    handle = gethandle();
    handle->fn = fn;
    handle->arg = arg;
    handle->type = type;
    handle->data = data;
    handle->lastfired = -1;
    eventq_add(eventq, handle);
    eventq.qlength++;

    eventprintf(("%s: event: register: %p->data=%lu, type=%s\n",
		debug_prefix_time(NULL), handle, handle->data,
		event_type2str(handle->type)));
    return (handle);
}

/*
 * Mark an event to be released.  Because we may be traversing the queue
 * when this is called, we must wait until later to actually remove
 * the event.
 */
void
event_release(
    event_handle_t *handle)
{

    assert(handle != NULL);

    eventprintf(("%s: event: release (mark): %p data=%lu, type=%s\n",
		 debug_prefix_time(NULL), handle, handle->data,
		 event_type2str(handle->type)));
    assert(handle->type != EV_DEAD);

    /*
     * For signal events, we need to specially remove then from the
     * signal event table.
     */
    if (handle->type == EV_SIG) {
	struct sigtabent *se = &sigtable[handle->data];

	assert(se->handle == handle);
	signal((int)handle->data, se->oldhandler);
	se->handle = NULL;
	se->score = 0;
    }

    /*
     * Decrement the qlength now since this is no longer a real
     * event.
     */
    eventq.qlength--;

    /*
     * Mark it as dead and leave it for the loop to remove.
     */
    handle->type = EV_DEAD;
}

/*
 * Fire all EV_WAIT events waiting on the specified id.
 */
int
event_wakeup(
    event_id_t id)
{
    event_handle_t *eh;
    int nwaken = 0;

    eventprintf(("%s: event: wakeup: enter (%lu)\n",
		 debug_prefix_time(NULL), id));

    for (eh = eventq_first(eventq); eh != NULL; eh = eventq_next(eh)) {

	if (eh->type == EV_WAIT && eh->data == id) {
	    eventprintf(("%s: event: wakeup: %p id=%lu\n",
			 debug_prefix_time(NULL), eh, id));
	    fire(eh);
	    nwaken++;
	}
    }
    return (nwaken);
}


/*
 * The event loop.  We need to be specially careful here with adds and
 * deletes.  Since adds and deletes will often happen while this is running,
 * we need to make sure we don't end up referencing a dead event handle.
 */
void
event_loop(
    const int dontblock)
{
    event_loop_wait((event_handle_t *)NULL, dontblock);
}



int
event_wait(
    event_handle_t *eh)
{
    return event_loop_wait(eh, 0);
}

/*
 * The event loop.  We need to be specially careful here with adds and
 * deletes.  Since adds and deletes will often happen while this is running,
 * we need to make sure we don't end up referencing a dead event handle.
 */
static int
event_loop_wait(
    event_handle_t *wait_eh,
    const int       dontblock)
{
#ifdef ASSERTIONS
    static int entry = 0;
#endif
    fd_set readfds, writefds, errfds, werrfds;
    struct timeval timeout, *tvptr;
    int ntries, maxfd, rc;
    long interval;
    time_t curtime;
    event_handle_t *eh, *nexteh;
    struct sigtabent *se;
    int event_wait_fired = 0;
    int see_event;

    eventprintf(("%s: event: loop: enter: dontblock=%d, qlength=%d, eh=%p\n",
		 debug_prefix_time(NULL),
		 dontblock, eventq.qlength, wait_eh));

    /*
     * If we have no events, we have nothing to do
     */
    if (eventq.qlength == 0)
	return 0;

    /*
     * We must not be entered twice
     */
    assert(++entry == 1);

    ntries = 0;

    /*
     * Save a copy of the current time once, to reduce syscall load
     * slightly.
     */
    curtime = time(NULL);

    do {
#ifdef EVENT_DEBUG
	eventprintf(("%s: event: loop: dontblock=%d, qlength=%d eh=%p\n",
		     debug_prefix_time(NULL), dontblock, eventq.qlength,
		     wait_eh));
	for (eh = eventq_first(eventq); eh != NULL; eh = eventq_next(eh)) {
	    eventprintf(("%s: %p): %s data=%lu fn=%p arg=%p\n",
			 debug_prefix_time(NULL), eh,
			 event_type2str(eh->type), eh->data, eh->fn,
			 eh->arg));
	}
#endif
	/*
	 * Set ourselves up with no timeout initially.
	 */
	timeout.tv_sec = 0;
	timeout.tv_usec = 0;

	/*
	 * If we can block, initially set the tvptr to NULL.  If
	 * we come across timeout events in the loop below, they
	 * will set it to an appropriate buffer.  If we don't
	 * see any timeout events, then tvptr will remain NULL
	 * and the select will properly block indefinately.
	 *
	 * If we can't block, set it to point to the timeout buf above.
	 */
	if (dontblock)
	    tvptr = &timeout;
	else
	    tvptr = NULL;

	/*
	 * Rebuild the select bitmasks each time.
	 */
	FD_ZERO(&readfds);
	FD_ZERO(&writefds);
	FD_ZERO(&errfds);
	maxfd = 0;

	see_event = (wait_eh == (event_handle_t *)NULL);
	/*
	 * Run through each event handle and setup the events.
	 * We save our next pointer early in case we GC some dead
	 * events.
	 */
	for (eh = eventq_first(eventq); eh != NULL; eh = nexteh) {
	    nexteh = eventq_next(eh);

	    switch (eh->type) {

	    /*
	     * Read fds just get set into the select bitmask
	     */
	    case EV_READFD:
		FD_SET((int)eh->data, &readfds);
		FD_SET((int)eh->data, &errfds);
		maxfd = max(maxfd, (int)eh->data);
		see_event |= (eh == wait_eh);
		break;

	    /*
	     * Likewise with write fds
	     */
	    case EV_WRITEFD:
		FD_SET((int)eh->data, &writefds);
		FD_SET((int)eh->data, &errfds);
		maxfd = max(maxfd, (int)eh->data);
		see_event |= (eh == wait_eh);
		break;

	    /*
	     * Only set signals that aren't already set to avoid unnecessary
	     * syscall overhead.
	     */
	    case EV_SIG:
		se = &sigtable[eh->data];
		see_event |= (eh == wait_eh);

		if (se->handle == eh)
		    break;

		/* no previous handle */
		assert(se->handle == NULL);
		se->handle = eh;
		se->score = 0;
		/*@ignore@*/
		se->oldhandler = signal((int)eh->data, signal_handler);
		/*@end@*/
		break;

	    /*
	     * Compute the timeout for this select
	     */
	    case EV_TIME:
		/* if we're not supposed to block, then leave it at 0 */
		if (dontblock)
		    break;

		if (eh->lastfired == -1)
		    eh->lastfired = curtime;

		interval = (long)(eh->data - (curtime - eh->lastfired));
		if (interval < 0)
		    interval = 0;

		if (tvptr != NULL)
		    timeout.tv_sec = min(timeout.tv_sec, interval);
		else {
		    /* this is the first timeout */
		    tvptr = &timeout;
		    timeout.tv_sec = interval;
		}
		see_event |= (eh == wait_eh);
		break;

	    /*
	     * Wait events are processed immediately by event_wakeup()
	     */
	    case EV_WAIT:
		see_event |= (eh == wait_eh);
		break;

	    /*
	     * Prune dead events
	     */
	    case EV_DEAD:
		eventq_remove(eh);
		puthandle(eh);
		break;

	    default:
		assert(0);
		break;
	    }
	}

	if(!see_event) {
	    assert(--entry == 0);
	    return 0;
	}

	/*
	 * Let 'er rip
	 */
	eventprintf((
		    "%s: event: select: dontblock=%d, maxfd=%d, timeout=%ld\n",
		    debug_prefix_time(NULL), dontblock, maxfd,
		    tvptr != NULL ? timeout.tv_sec : -1));
	rc = select(maxfd + 1, &readfds, &writefds, &errfds, tvptr);
	eventprintf(("%s: event: select returns %d\n",
		     debug_prefix_time(NULL), rc));

	/*
	 * Select errors can mean many things.  Interrupted events should
	 * not be fatal, since they could be delivered signals which still
	 * need to have their events fired.
	 */
	if (rc < 0) {
	    if (errno != EINTR) {
		if (++ntries > 5) {
		    error("select failed: %s", strerror(errno));
		    /*NOTREACHED*/
		}
		continue;
	    }
	    /* proceed if errno == EINTR, we may have caught a signal */

	    /* contents cannot be trusted */
	    FD_ZERO(&readfds);
	    FD_ZERO(&writefds);
	    FD_ZERO(&errfds);
	}

	/*
	 * Grab the current time again for use in timed events.
	 */
	curtime = time(NULL);

	/*
	 * We need to copy the errfds into werrfds, so file descriptors
	 * that are being polled for both reading and writing have
	 * both of their poll events 'see' the error.
	 */
	memcpy(&werrfds, &errfds, SIZEOF(werrfds));

	/*
	 * Now run through the events and fire the ones that are ready.
	 * Don't handle file descriptor events if the select failed.
	 */
	for (eh = eventq_first(eventq); eh != NULL; eh = eventq_next(eh)) {

	    switch (eh->type) {

	    /*
	     * Read fds: just fire the event if set in the bitmask
	     */
	    case EV_READFD:
		if (FD_ISSET((int)eh->data, &readfds) ||
		    FD_ISSET((int)eh->data, &errfds)) {
		    FD_CLR((int)eh->data, &readfds);
		    FD_CLR((int)eh->data, &errfds);
		    fire(eh);
		    if(eh == wait_eh) event_wait_fired = 1;
		}
		break;

	    /*
	     * Write fds: same as Read fds
	     */
	    case EV_WRITEFD:
		if (FD_ISSET((int)eh->data, &writefds) ||
		    FD_ISSET((int)eh->data, &werrfds)) {
		    FD_CLR((int)eh->data, &writefds);
		    FD_CLR((int)eh->data, &werrfds);
		    fire(eh);
		    if(eh == wait_eh) event_wait_fired = 1;
		}
		break;

	    /*
	     * Signal events: check the score for fires, and run the
	     * event if we got one.
	     */
	    case EV_SIG:
		se = &sigtable[eh->data];
		if (se->score > 0) {
		    assert(se->handle == eh);
		    se->score = 0;
		    fire(eh);
		    if(eh == wait_eh) event_wait_fired = 1;
		}
		break;

	    /*
	     * Timed events: check the interval elapsed since last fired,
	     * and set it off if greater or equal to requested interval.
	     */
	    case EV_TIME:
		if (eh->lastfired == -1)
		    eh->lastfired = curtime;
		if ((curtime - eh->lastfired) >= (time_t)eh->data) {
		    eh->lastfired = curtime;
		    fire(eh);
		    if(eh == wait_eh) event_wait_fired = 1;
		}
		break;

	    /*
	     * Wait events are handled immediately by event_wakeup()
	     * Dead events are handled by the pre-select loop.
	     */
	    case EV_WAIT:
	    case EV_DEAD:
		break;

	    default:
		assert(0);
		break;
	    }
	}
    } while (!dontblock && eventq.qlength > 0 && event_wait_fired == 0);

    assert(--entry == 0);
    
    return (event_wait_fired == 1);
}

/*
 * Generic signal handler.  Used to count caught signals for the event
 * loop.
 */
static void
signal_handler(
    int	signo)
{

    assert((signo >= 0) && ((size_t)signo < (size_t)(sizeof(sigtable) / sizeof(sigtable[0]))));
    sigtable[signo].score++;
}

/*
 * Return a new handle.  Take from the handle cache if not empty.  Otherwise,
 * alloc a new one.
 */
static event_handle_t *
gethandle(void)
{
    event_handle_t *eh;

    if ((eh = eventq_first(cache)) != NULL) {
	assert(cache.qlength > 0);
	eventq_remove(eh);
	cache.qlength--;
	return (eh);
    }
    assert(cache.qlength == 0);
    return (alloc(SIZEOF(*eh)));
}

/*
 * Free a handle.  If there's space in the handle cache, put it there.
 * Otherwise, free it.
 */
static void
puthandle(
    event_handle_t *eh)
{

    if (cache.qlength > CACHEDEPTH) {
	amfree(eh);
	return;
    }
    eventq_add(cache, eh);
    cache.qlength++;
}

#ifdef EVENT_DEBUG
/*
 * Convert an event type into a string
 */
static const char *
event_type2str(
    event_type_t type)
{
    static const struct {
	event_type_t type;
	const char name[12];
    } event_types[] = {
#define	X(s)	{ s, stringize(s) }
	X(EV_READFD),
	X(EV_WRITEFD),
	X(EV_SIG),
	X(EV_TIME),
	X(EV_WAIT),
	X(EV_DEAD),
#undef X
    };
    size_t i;

    for (i = 0; i < (size_t)(sizeof(event_types) / sizeof(event_types[0])); i++)
	if (type == event_types[i].type)
	    return (event_types[i].name);
    return ("BOGUS EVENT TYPE");
}
#endif	/* EVENT_DEBUG */
