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
 * Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include "amanda.h"
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
    state->val = seed;
    state->count = 0;
}

guint32
simpleprng_get_seed(
    simpleprng_state_t *state)
{
    return state->val;
}

guint32 simpleprng_rand(
    simpleprng_state_t *state)
{
    state->count++;
    return (state->val = (A * state->val) + C);
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

static char *
hexstr(guint8 *p, int len)
{
    char *result = NULL;
    int i;

    for (i = 0; i < len; i++) {
	if (result)
	    result = newvstrallocf(result, "%s %02x", result, (guint)(*(p++)));
	else
	    result = vstrallocf("[%02x", (guint)(*(p++)));
    }
    result = newvstrallocf(result, "%s]", result);

    return result;
}

gboolean simpleprng_verify_buffer(
    simpleprng_state_t *state,
    gpointer buf,
    size_t len)
{
    guint8 *p = buf;
    while (len--) {
	guint64 count = state->count;
	guint8 expected = simpleprng_rand_byte(state);
	guint8 got = *p;
	if (expected != got) {
	    int remaining = MIN(len, 16);
	    guint8 expbytes[16] = { expected };
	    char *gotstr = hexstr(p, remaining);
	    char *expstr;
	    int i;

	    for (i = 1; i < remaining; i++)
		expbytes[i] = simpleprng_rand_byte(state);
	    expstr = hexstr(expbytes, remaining);

	    g_fprintf(stderr,
		    "random value mismatch at offset %ju: got %s, expected %s\n",
		    (uintmax_t)count, gotstr, expstr);
	    g_free(gotstr);
	    return FALSE;
	}
	p++;
    }

    return TRUE;
}
