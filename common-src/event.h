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
 * $Id: event.h,v 1.9 2006/06/16 10:55:05 martinea Exp $
 */
#ifndef EVENT_H
#define EVENT_H

/*
 * These functions define a generic event interface.  One can register a
 * function vector and the type of events to act on, and the event handler
 * will dispatch as necessary.
 */

/*
 * An opaque handle returned by the registry functions that can be
 * used to unregister an event in the future.
 */
struct event_handle;
typedef struct event_handle event_handle_t;

/*
 * The 'id' of the event.  The meaning of this depends on the type of
 * event we are registering -- see event_register.  The name 'id' is
 * historical: it is quite possible to have many outstanding events with
 * the same ID (same timeout or same file descriptor).
 *
 * Event id's are supplied by the caller, and in some cases are cast from
 * pointers, so this value must be wide enough to hold a pointer without
 * truncation.
 */
typedef	intmax_t event_id_t;

/*
 * The types of events we can register.
 */
typedef enum {
    EV_READFD,			/* file descriptor is ready for reading */
    EV_WRITEFD,			/* file descriptor is ready for writing */
    EV_TIME,			/* n seconds have elapsed */
    EV_WAIT,			/* event_wakeup() was called with this id */
} event_type_t;

/*
 * The function signature for functions that get called when an event
 * fires.
 */
typedef void (*event_fn_t)(void *);

/*
 * Register an event handler.
 *
 * For readfd and writefd events, the first arg is the file descriptor.
 * There can be multiple callers firing on the same file descriptor.
 *
 * For signal events, the first arg is the signal number as defined in
 * <signal.h>.  There can only be one signal handler. (do we need more?)
 *
 * For time events, the first arg is the interval in seconds between
 * pulses.  There can be multiple time events, of course.  Don't
 * count on the time events being too accurate.  They depend on the
 * caller calling event_loop() often enough.
 */
event_handle_t *event_register(event_id_t, event_type_t, event_fn_t, void *);

/*
 * Release an event handler.
 */
void event_release(event_handle_t *);

/*
 * Wake up all EV_WAIT events waiting on a specific id.  This happens immediately,
 * not in the next iteration of the event loop.  If callbacks made during the wakeup
 * register a new event with the same ID, that new event will *not* be awakened.
 */
int event_wakeup(event_id_t);

/*
 * Call event_loop, returning when one of the following conditions is
 * true:
 *  evt is EV_WAIT, and it is released; or
 *  evt is EV_READFD, EV_WRITEFD, or EV_TIME, and it is fired.
 */
void event_wait(event_handle_t *evt);

/*
 * Process events.  If the argument is nonzero, then the loop does
 * not block.
 */
void event_loop(int nonblock);

/*
 * Get the default GMainLoop object.  Applications which use the Glib
 * main loop directly should use this object for calls to e.g.,
 * g_main_loop_run(loop).
 */
GMainLoop *default_main_loop(void);

/*
 * Utility GSources
 */

/* Create a GSource that will callback when the given file descriptor is in
 * any of the given conditions.  The callback is a simple GSourceFunc.
 *
 * @param fd: the file descriptr
 * @param events: the conditions (GIOCondition flags)
 * @return: GSource object
 */
GSource * new_fdsource(gint fd, GIOCondition events);

/* Create a GSource that will callback when the given child dies.  The callback
 * should match ChildWatchFunc.  Once the callback is made, it will not be called
 * again by this source.
 *
 * Note: This is provided by glib in later versions, but not in version 2.2.0.
 * This function and callback is modeled on g_child_watch_source_new.
 *
 * @param pid: the process ID @return: GSource object
 */
typedef void (*ChildWatchFunc)(pid_t pid, gint status, gpointer data); 
GSource * new_child_watch_source(pid_t pid);

#endif	/* EVENT_H */
