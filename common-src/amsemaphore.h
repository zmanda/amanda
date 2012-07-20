/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 *
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* GLib does not provide semaphores, which are useful in queue.c.
   So, we implement it here. */

#include <glib.h>

#ifndef SEMAPHORE_H

typedef struct {
    int value;
    GMutex *mutex;
    GCond * decrement_cond;
    GCond * zero_cond;
} amsemaphore_t;

/* Create a new semaphore object with the given value.
 *
 * @param value: new value
 * @returns: newly allocated amsemaphore_t
 */
amsemaphore_t* amsemaphore_new_with_value(int value);

/* Shortcut to make a new semaphore with value 1.
 */
#define amsemaphore_new() amsemaphore_new_with_value(1)

/* Free a semaphore allocated by amsemaphore_with_new_value().  Be sure the
 * semaphore is no longer in use by any threads.
 *
 * @param sem: the semaphore to free
 */
void amsemaphore_free(amsemaphore_t *sem);

/* Increment the value of the semaphore by incr.  This corresponds to
 * Dijkstra's V(), or the typical semaphore's release().
 *
 * This function will not block, but may wake other threads waiting
 * on amsemaphore_decrement().
 *
 * @param sem: the semaphore
 * @param incr: added to the semaphore's value
 */
void amsemaphore_increment(amsemaphore_t *sem, unsigned int incr);

/* Shortcut to increment the semaphore by 1.
 */
#define amsemaphore_up(semaphore) amsemaphore_increment(semaphore,1)

/* Decrement the value of the semaphore by incr.  If this operation
 * would make the semaphore negative, block until the semaphore
 * value is large enough, then perform the decerement operation. Threads
 * waiting on amsemaphore_wait_empty() may be awakened if the value
 * reaches 0.
 *
 * @param sem: the semaphore
 * @param decr: subtracted from the semaphore's value
 */
void amsemaphore_decrement(amsemaphore_t *sem, unsigned int decr);

/* Shortcut to decrement the semaphore by 1.
 */
#define amsemaphore_down(semaphore) amsemaphore_decrement(semaphore, 1)

/* Increment or decrement (with a negative incr) the value without
 * blocking.  Threads waiting on amsemaphore_decrement() or
 * amsemaphore_wait_empty() will be awakened if necessary.
 *
 * @param sem: the semaphore
 * @param incr: added to the semaphore's value
 */
void amsemaphore_force_adjust(amsemaphore_t *sem, int incr);

/* Set the semaphore to a given value without blocking.  Threads
 * waiting on amsemaphore_decrement() or amsemaphore_wait_empty()
 * will be awakened if necessary.
 *
 * @param sem: the semaphore
 * @param value: the new value
 */
void amsemaphore_force_set(amsemaphore_t *sem, int value);

/* Block until the semaphore's value is zero.
 *
 * @param sem: the semaphore
 */
void amsemaphore_wait_empty(amsemaphore_t *sem);

#endif /* SEMAPHORE_H */
