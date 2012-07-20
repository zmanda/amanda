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

#include "amanda.h"
#include "amsemaphore.h"

amsemaphore_t* amsemaphore_new_with_value(int value) {
    amsemaphore_t *rval;

    if (!g_thread_supported())
        return NULL;

    rval = malloc(sizeof(*rval));
    rval->value = value;
    rval->mutex = g_mutex_new();
    rval->decrement_cond = g_cond_new();
    rval->zero_cond = g_cond_new();
    
    if (rval->mutex == NULL || rval->decrement_cond == NULL ||
        rval->zero_cond == NULL) {
        amsemaphore_free(rval);
        return NULL;
    } else {
        return rval;
    }
}

void amsemaphore_free(amsemaphore_t* o) {
    g_mutex_free(o->mutex);
    g_cond_free(o->decrement_cond);
    g_cond_free(o->zero_cond);
    free(o);
}

/* This function checks if the semaphore would is zero or negative.
 * If so, the zero_cond is signalled. We assume that the mutex is
 * locked. */
static void check_empty(amsemaphore_t * o) {
    if (o->value <= 0) {
        g_cond_broadcast(o->zero_cond);
    }
}

void amsemaphore_increment(amsemaphore_t* o, unsigned int inc) {
    g_return_if_fail(o != NULL);
    g_return_if_fail(inc != 0);

    amsemaphore_force_adjust(o, inc);
}

void amsemaphore_decrement(amsemaphore_t* o, unsigned int dec) {
    int sdec;
    g_return_if_fail(o != NULL);
    sdec = (int) dec;
    g_return_if_fail(sdec >= 0);

    g_mutex_lock(o->mutex);
    while (o->value < sdec) {
        g_cond_wait(o->decrement_cond, o->mutex);
    }
    o->value -= sdec;
    check_empty(o);
    g_mutex_unlock(o->mutex);
}

void amsemaphore_force_adjust(amsemaphore_t* o, int inc) {
    g_return_if_fail(o != NULL);

    g_mutex_lock(o->mutex);
    o->value += inc;
    if (inc < 0)
	check_empty(o);
    else
	g_cond_broadcast(o->decrement_cond);
    g_mutex_unlock(o->mutex);

}

void amsemaphore_force_set(amsemaphore_t* o, int value) {
    int oldvalue;
    g_return_if_fail(o != NULL);
    
    g_mutex_lock(o->mutex);
    oldvalue = o->value;
    o->value = value;
    if (value < oldvalue)
	check_empty(o);
    else
	g_cond_broadcast(o->decrement_cond);
    g_mutex_unlock(o->mutex);
    
}

void amsemaphore_wait_empty(amsemaphore_t * o) {
    g_return_if_fail(o != NULL);
    
    g_mutex_lock(o->mutex);
    while (o->value > 0) {
        g_cond_wait(o->zero_cond, o->mutex);
    }
    g_mutex_unlock(o->mutex);
}
