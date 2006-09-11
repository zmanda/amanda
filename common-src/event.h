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
 * The 'id' of the event.  The meaning of this is dependant on the type
 * of event we are registering.  This is hopefully wide enough that
 * callers can cast pointers to it and keep the value untruncated and
 * unique.
 */
typedef	unsigned long event_id_t;

/*
 * The types of events we can register.
 */
typedef enum {
    EV_READFD,			/* file descriptor is ready for reading */
    EV_WRITEFD,			/* file descriptor is ready for writing */
    EV_SIG,			/* signal has fired */
    EV_TIME,			/* n seconds have elapsed */
    EV_WAIT,			/* event_wakeup() was called with this id */
    EV_DEAD			/* internal use only */
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
 * Wake up all EV_WAIT events waiting on a specific id
 */
int event_wakeup(event_id_t);

/*
 * Block until the event is terminated.
 */
int event_wait(event_handle_t *);

/*
 * Process events.  If the argument is nonzero, then the loop does
 * not block.
 */
void event_loop(const int);

#endif	/* EVENT_H */
