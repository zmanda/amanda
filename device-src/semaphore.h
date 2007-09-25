/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as 
 * published by the Free Software Foundation.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 * 
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
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
} semaphore_t;

#define semaphore_new semaphore_new_with_value(1)
semaphore_t* semaphore_new_with_value(int);
void semaphore_free(semaphore_t*);
#define semaphore_up(semaphore) semphore_increment(semaphore,1)
void semaphore_increment(semaphore_t*, unsigned int);
#define semaphore_down(semaphore) semaphore_decrement(semaphore, 1)
/* May be zero; will wait until the semaphore is positive. */
void semaphore_decrement(semaphore_t*, unsigned int);
/* Never blocks; may increment or decrement. */
void semaphore_force_adjust(semaphore_t*, int);
/* Never blocks; sets semaphore to some arbitrary value. */
void semaphore_force_set(semaphore_t*, int);
/* Returns only once semaphore value is <= 0. */
void semaphore_wait_empty(semaphore_t *);

#endif /* SEMAPHORE_H */
