/*
 * Copyright (c) 2008 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc, 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "simpleprng.h"

/* A *very* basic linear congruential generator; values are as cited in
 * http://en.wikipedia.org/wiki/Linear_congruential_generator for Numerical Recipes */

#define A 1664525
#define C 1013904223

void
simpleprng_seed(
    simpleprng_state_t *state,
    guint32 seed)
{
    g_assert(seed != 0);
    *state = seed;
}

guint32 simpleprng_rand(
    simpleprng_state_t *state)
{
    return (*state = (A * (*state)) + C);
}

void simpleprng_fill_buffer(
    simpleprng_state_t *state,
    gpointer buf,
    size_t len)
{
    guint8 *p = buf;
    while (len--) {
	*(p++) = simpleprng_rand_byte(state);
    }
}

gboolean simpleprng_verify_buffer(
    simpleprng_state_t *state,
    gpointer buf,
    size_t len)
{
    guint8 *p = buf;
    while (len--) {
	guint8 expected = simpleprng_rand_byte(state);
	guint8 got = *p;
	if (expected != got) {
	    g_fprintf(stderr,
		    "random value mismatch in buffer %p, offset %zd: got 0x%02x, expected 0x%02x\n", 
		    buf, (size_t)(p-(guint8*)buf), (int)got, (int)expected);
	    return FALSE;
	}
	p++;
    }

    return TRUE;
}
