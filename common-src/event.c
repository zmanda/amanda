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
 *
 * This is a compatibility wrapper over Glib's GMainLoop.  New code should
 * use Glib's interface directly.
 *
 * Each event_handle is associated with a unique GSource, identified by it
 * event_source_id.
 */

#include "amanda.h"
#include "conffile.h"
#include "event.h"
#include "glib-util.h"

/* TODO: use mem chunks to allocate event_handles */
/* TODO: lock stuff for threading */

/* Write a debugging message if the config variable debug_event
 * is greater than or equal to i */
#define event_debug(i, ...) do {	\
       if ((i) <= debug_event) {	\
           dbprintf(__VA_ARGS__);	\
       }				\
} while (0)

/*
 * The opaque handle passed back to the caller.  This is typedefed to
 * event_handle_t in our header file.
 */
struct event_handle {
    event_fn_t fn;		/* function to call when this fires */
    void *arg;			/* argument to pass to previous function */

    event_type_t type;		/* type of event */
    event_id_t data;		/* type data */

    GSource *source;		/* Glib event source, if one exists */
    guint source_id;	        /* ID of the glib event source */

    gboolean has_fired;		/* for use by event_wait() */
    gboolean is_dead;		/* should this event be deleted? */
};

/* A list of all extant event_handle objects, used for searching for particular
 * events and for deleting dead events */
GSList *all_events;

/*
 * Utility functions
 */

static const char *event_type2str(event_type_t type);

/* "Fire" an event handle, by calling its callback function */
#define	fire(eh) do { \
	event_debug(1, "firing %p: %s/%jd\n", eh, event_type2str((eh)->type), (eh)->data); \
	(*(eh)->fn)((eh)->arg); \
	(eh)->has_fired = TRUE; \
} while(0)

/* Adapt a Glib callback to an event_handle_t callback; assumes that the
 * user_ptr for the Glib callback is a pointer to the event_handle_t.  */
static gboolean 
event_handle_callback(
    gpointer user_ptr)
{
    event_handle_t *hdl = (event_handle_t *)user_ptr;

    /* if the handle is dead, then don't fire the callback; this means that
     * we're in the process of freeing the event */
    if (!hdl->is_dead) {
	fire(hdl);
    }

    /* don't ever let GMainLoop destroy GSources */
    return TRUE;
}

/*
 * Public functions
 */

event_handle_t *
event_register(
    event_id_t data,
    event_type_t type,
    event_fn_t fn,
    void *arg)
{
    event_handle_t *handle;
    GIOCondition cond;

    /* sanity-checking */
    if ((type == EV_READFD) || (type == EV_WRITEFD)) {
	/* make sure we aren't given a high fd that will overflow a fd_set */
	if (data >= (int)FD_SETSIZE) {
	    error(_("event_register: Invalid file descriptor %jd"), data);
	    /*NOTREACHED*/
	}
    } else if (type == EV_TIME) {
	if (data <= 0) {
	    error(_("event_register: interval for EV_TIME must be greater than 0; got %jd"), data);
	}
    }

    handle = g_new0(event_handle_t, 1);
    handle->fn = fn;
    handle->arg = arg;
    handle->type = type;
    handle->data = data;
    handle->is_dead = FALSE;

    event_debug(1, _("event: register: %p->data=%jd, type=%s\n"),
		    handle, handle->data, event_type2str(handle->type));

    /* add to the list of events */
    all_events = g_slist_prepend(all_events, (gpointer)handle);

    /* and set up the GSource for this event */
    switch (type) {
	case EV_READFD:
	case EV_WRITEFD:
	    /* create a new source */
	    if (type == EV_READFD) {
		cond = G_IO_IN | G_IO_HUP | G_IO_ERR;
	    } else {
		cond = G_IO_OUT | G_IO_ERR;
	    }

	    handle->source = new_fdsource(data, cond);

	    /* attach it to the default GMainLoop */
	    g_source_attach(handle->source, NULL);
	    handle->source_id = g_source_get_id(handle->source);

	    /* And set its callbacks */
	    g_source_set_callback(handle->source, event_handle_callback,
				  (gpointer)handle, NULL);

	    /* drop our reference to it, so when it's detached, it will be
	     * destroyed. */
	    g_source_unref(handle->source);
	    break;

	case EV_TIME:
	    /* Glib provides a nice shortcut for timeouts.  The *1000 converts
	     * seconds to milliseconds. */
	    handle->source_id = g_timeout_add(data * 1000, event_handle_callback,
					      (gpointer)handle);

	    /* But it doesn't give us the source directly.. */
	    handle->source = g_main_context_find_source_by_id(NULL, handle->source_id);
	    /* EV_TIME must always be handled after EV_READ */
	    g_source_set_priority(handle->source, 10);
	    break;

	case EV_WAIT:
	    /* nothing to do -- these are handled independently of GMainLoop */
	    break;

	default:
	    error(_("Unknown event type %s"), event_type2str(type));
    }

    return handle;
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

    event_debug(1, _("event: release (mark): %p data=%jd, type=%s\n"),
		    handle, handle->data,
		    event_type2str(handle->type));
    assert(!handle->is_dead);

    /* Mark it as dead and leave it for the event_loop to remove */
    handle->is_dead = TRUE;
}

/*
 * Fire all EV_WAIT events waiting on the specified id.
 */
int
event_wakeup(
    event_id_t id)
{
    GSList *iter;
    GSList *tofire = NULL;
    int nwaken = 0;

    event_debug(1, _("event: wakeup: enter (%jd)\n"), id);

    /* search for any and all matching events, and record them.  This way
     * we have determined the whole list of events we'll be firing *before*
     * we fire any of them. */
    for (iter = all_events; iter != NULL; iter = g_slist_next(iter)) {
	event_handle_t *eh = (event_handle_t *)iter->data;
	if (eh->type == EV_WAIT && eh->data == id && !eh->is_dead) {
	    tofire = g_slist_append(tofire, (gpointer)eh);
	}
    }

    /* fire them */
    for (iter = tofire; iter != NULL; iter = g_slist_next(iter)) {
	event_handle_t *eh = (event_handle_t *)iter->data;
	if (eh->type == EV_WAIT && eh->data == id && !eh->is_dead) {
	    event_debug(1, _("A: event: wakeup triggering: %p id=%jd\n"), eh, id);
	    fire(eh);
	    nwaken++;
	}
    }

    /* and free the temporary list */
    g_slist_free(tofire);

    return (nwaken);
}


/*
 * The event loop.
 */

static void event_loop_wait (event_handle_t *, const int);

void
event_loop(
    int nonblock)
{
    event_loop_wait(NULL, nonblock);
}

void
event_wait(
    event_handle_t *eh)
{
    event_loop_wait(eh, 0);
}

/* Flush out any dead events in all_events.  Be careful that this
 * isn't called while someone is iterating over all_events.
 *
 * @param wait_eh: the event handle we're waiting on, which shouldn't
 *	    be flushed.
 */
static void
flush_dead_events(event_handle_t *wait_eh)
{
    GSList *iter, *next;

    for (iter = all_events; iter != NULL; iter = next) {
	event_handle_t *hdl = (event_handle_t *)iter->data;
	next = g_slist_next(iter);

	/* (handle the case when wait_eh is dead by simply not deleting
	 * it; the next run of event_loop will take care of it) */
	if (hdl->is_dead && hdl != wait_eh) {
	    all_events = g_slist_delete_link(all_events, iter);
	    if (hdl->source) g_source_destroy(hdl->source);

	    amfree(hdl);
	}
    }
}

/* Return TRUE if we have any events outstanding that can be dispatched
 * by GMainLoop.  Recall EV_WAIT events appear in all_events, but are
 * not dispatched by GMainLoop.  */
static gboolean
any_mainloop_events(void)
{
    GSList *iter;

    for (iter = all_events; iter != NULL; iter = g_slist_next(iter)) {
	event_handle_t *hdl = (event_handle_t *)iter->data;
	event_debug(2, _("list %p: %s/%jd\n"), hdl, event_type2str((hdl)->type), (hdl)->data);
	if (hdl->type != EV_WAIT)
	    return TRUE;
    }

    return FALSE;
}

static void
event_loop_wait(
    event_handle_t *wait_eh,
    int nonblock)
{
    event_debug(1, _("event: loop: enter: nonblockg=%d, eh=%p\n"), nonblock, wait_eh);

    /* If we're waiting for a specific event, then reset its has_fired flag */
    if (wait_eh) {
	wait_eh->has_fired = FALSE;
    }

    /* Keep looping until there are no events, or until wait_eh has fired */
    while (1) {
	/* clean up first, so we don't accidentally check a dead source */
	flush_dead_events(wait_eh);

	/* if there's nothing to wait for, then don't block, but run an
	 * iteration so that any other users of GMainLoop will get a chance
	 * to run. */
	if (!any_mainloop_events())
	    break;

	/* Do an interation */
	g_main_context_iteration(NULL, !nonblock);

	/* If the event we've been waiting for has fired or been released, as
	 * appropriate, we're done.  See the comments for event_wait in event.h
	 * for the skinny on this weird expression. */
	if (wait_eh && ((wait_eh->type == EV_WAIT && wait_eh->is_dead)
	             || (wait_eh->type != EV_WAIT && wait_eh->has_fired)))
	    break;

	/* Don't loop if we're not blocking */
	if (nonblock)
	    break;
    }

    /* extra cleanup, to keep all_events short, and to delete wait_eh if it
     * has been released. */
    flush_dead_events(NULL);

}

GMainLoop *
default_main_loop(void)
{
    static GMainLoop *loop = NULL;
    if (!loop)
	loop = g_main_loop_new(NULL, TRUE);
    return loop;
}

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
	X(EV_TIME),
	X(EV_WAIT),
#undef X
    };
    size_t i;

    for (i = 0; i < (size_t)(sizeof(event_types) / sizeof(event_types[0])); i++)
	if (type == event_types[i].type)
	    return (event_types[i].name);
    return (_("BOGUS EVENT TYPE"));
}

/*
 * FDSource -- a source for a file descriptor
 *
 * We could use Glib's GIOChannel for this, but it adds some buffering
 * and Unicode functionality that we really don't want.  The custom GSource
 * is simple enough anyway, and the Glib documentation describes it in prose.
 */

typedef struct FDSource {
    GSource source; /* must be the first element in the struct */
    GPollFD pollfd; /* Our file descriptor */
} FDSource;

static gboolean
fdsource_prepare(
    GSource *source G_GNUC_UNUSED,
    gint *timeout_)
{
    *timeout_ = -1; /* block forever, as far as we're concerned */
    return FALSE;
}

static gboolean
fdsource_check(
    GSource *source)
{
    FDSource *fds = (FDSource *)source;

    /* we need to be dispatched if any interesting events have been received by the FD */
    return fds->pollfd.events & fds->pollfd.revents;
}

static gboolean
fdsource_dispatch(
    GSource *source G_GNUC_UNUSED,
    GSourceFunc callback,
    gpointer user_data)
{
    if (callback)
	return callback(user_data);

    /* Don't automatically detach the event source if there's no callback. */
    return TRUE;
}

GSource *
new_fdsource(gint fd, GIOCondition events)
{
    static GSourceFuncs *fdsource_funcs = NULL;
    GSource *src;
    FDSource *fds;

    /* initialize these here to avoid a compiler warning */
    if (!fdsource_funcs) {
	fdsource_funcs = g_new0(GSourceFuncs, 1);
	fdsource_funcs->prepare = fdsource_prepare;
	fdsource_funcs->check = fdsource_check;
	fdsource_funcs->dispatch = fdsource_dispatch;
    }

    src = g_source_new(fdsource_funcs, sizeof(FDSource));
    fds = (FDSource *)src;

    fds->pollfd.fd = fd;
    fds->pollfd.events = events;
    g_source_add_poll(src, &fds->pollfd);

    return src;
}

/*
 * ChildWatchSource -- a source for a file descriptor
 *
 * Newer versions of glib provide equivalent functionality; consider
 * optionally using that, protected by a GLIB_CHECK_VERSION condition.
 */

/* Versions before glib-2.4.0 didn't include a child watch source, and versions
 * before 2.6.0 used unreliable signals.  On these versions, we implement
 * a "dumb" version of our own invention.  This is dumb in the sense that it
 * doesn't use SIGCHLD to detect a dead child, preferring to just poll at
 * exponentially increasing interals.  Writing a smarter implementation runs into
 * some tricky race conditions and extra machinery.  Since there are few, if any,
 * users of a glib version this old, such machinery wouldn't get much testing.
 *
 * FreeBSD users have also reported problems with the glib child watch source,
 * so we use the dumb version on FreeBSD, too.
 */

#if (defined(__FreeBSD__) || GLIB_MAJOR_VERSION < 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION < 6))
typedef struct ChildWatchSource {
    GSource source; /* must be the first element in the struct */

    pid_t pid;

    gint dead;
    gint status;

    gint timeout;
} ChildWatchSource;

/* this corresponds to rapid checks for about 10 seconds, after which the
 * waitpid() check occurs every 2 seconds. */
#define CWS_BASE_TIMEOUT 20
#define CWS_MULT_TIMEOUT 1.1
#define CWS_MAX_TIMEOUT 2000

static gboolean
child_watch_source_prepare(
    GSource *source,
    gint *timeout_)
{
    ChildWatchSource *cws = (ChildWatchSource *)source;

    *timeout_ = cws->timeout;

    cws->timeout *= CWS_MULT_TIMEOUT;
    if (cws->timeout > CWS_MAX_TIMEOUT) cws->timeout = CWS_MAX_TIMEOUT;

    return FALSE;
}

static gboolean
child_watch_source_check(
    GSource *source)
{
    ChildWatchSource *cws = (ChildWatchSource *)source;

    /* is it dead? */
    if (!cws->dead && waitpid(cws->pid, &cws->status, WNOHANG) > 0) {
	cws->dead = TRUE;
    }

    return cws->dead;
}

static gboolean
child_watch_source_dispatch(
    GSource *source G_GNUC_UNUSED,
    GSourceFunc callback,
    gpointer user_data)
{
    ChildWatchSource *cws = (ChildWatchSource *)source;

    /* this shouldn't happen, but just in case */
    if (cws->dead) {
	if (!callback) {
	    g_warning("child %jd died before callback was registered", (uintmax_t)cws->pid);
	    return FALSE;
	}

	((ChildWatchFunc)callback)(cws->pid, cws->status, user_data);

	/* Un-queue this source unconditionally -- the child can't die twice */
	return FALSE;
    }

    return TRUE;
}

GSource *
new_child_watch_source(pid_t pid)
{
    static GSourceFuncs *child_watch_source_funcs = NULL;
    GSource *src;
    ChildWatchSource *cws;

    /* initialize these here to avoid a compiler warning */
    if (!child_watch_source_funcs) {
	child_watch_source_funcs = g_new0(GSourceFuncs, 1);
	child_watch_source_funcs->prepare = child_watch_source_prepare;
	child_watch_source_funcs->check = child_watch_source_check;
	child_watch_source_funcs->dispatch = child_watch_source_dispatch;
    }

    src = g_source_new(child_watch_source_funcs, sizeof(ChildWatchSource));
    cws = (ChildWatchSource *)src;

    cws->pid = pid;
    cws->dead = FALSE;
    cws->timeout = CWS_BASE_TIMEOUT;

    return src;
}
#else
/* In more recent versions of glib, we just use the built-in glib source */
GSource *
new_child_watch_source(pid_t pid)
{
    return g_child_watch_source_new(pid);
}
#endif
