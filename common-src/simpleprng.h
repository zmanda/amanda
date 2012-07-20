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

#ifndef SIMPLEPRNG_H
#define	SIMPLEPRNG_H

#include "amanda.h"

/* A very simple, thread-safe PRNG.  This is intended for use in generating
 * reproducable bytestreams for testing purposes.  It is *not*
 * cryptographically secure! */

typedef struct {
    guint32 val;
    guint64 count;
} simpleprng_state_t;

/* Initialize and seed the PRNG
 *
 * @param state: pointer to PRNG state
 * @param seed: initial value
 */
void simpleprng_seed(
    simpleprng_state_t *state,
    guint32 seed);

/* Get a seed that will reproduce the PRNG's current state
 *
 * @param state: pointer to PRNG state
 * @returns: seed;
 */
guint32 simpleprng_get_seed(
    simpleprng_state_t *state);

/* Get a random guint32
 *
 * @param state: pointer to PRNG state
 * @returns: random integer
 */
guint32 simpleprng_rand(
    simpleprng_state_t *state);

/* Get a random byte
 *
 * @param state: pointer to PRNG state
 * @returns: random integer
 */
/* use the high-order bytes, as they're "more random" */
#define simpleprng_rand_byte(state) \
    ((guint8)(simpleprng_rand((state)) >> 24))

/* Fill the given buffer with a sequence of bytes
 *
 * @param state: pointer to PRNG state
 * @param buf: buffer to fill
 * @param len: number of bytes to write
 */
void simpleprng_fill_buffer(
    simpleprng_state_t *state,
    gpointer buf,
    size_t len);

/* Verify that a buffer matches the values from the PRNG.
 *
 * @param state: pointer to PRNG state
 * @param buf: buffer to verify
 * @param len: number of bytes to verify
 * @returns: true if all bytes match
 */
gboolean simpleprng_verify_buffer(
    simpleprng_state_t *state,
    gpointer buf,
    size_t len);

#endif	/* SIMPLEPRNG_H */
