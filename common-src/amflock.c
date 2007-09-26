/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * $Id: amflock.c 7161 2007-07-03 16:27:26Z dustin $
 *
 * file locking routines, put here to hide the system dependant stuff
 * from the rest of the code
 */
/*
**
** Notes:
** - These are "best effort" routines.
** - "configure" has four variables that are used to determine which type of
**   locking to use:
**     USE_POSIX_FCNTL - use fcntl().  The full job.
**     USE_FLOCK       - use flock().  Does just as well.
**     USE_LOCKF       - use lockf().  Only handles advisory, exclusive,
**                       blocking file locks as used by Amanda.
**     USE_LNLOCK      - Home brew exclusive, blocking file lock.
**     <none>          - No locking available.  User beware!
*/

/* FIXME: This code has several limitations to be fixed:
 * - It should be possible to select a locking mode (or detect the
 *   best mode for a particular filesystem) at runtime.
 * - There should be a locking mode that works with NFS filesystems.
 * - Semantics should be clear when different parts of a single 
 *   process (possibly in the same/different threads) both try to lock 
 *   the same file (but with different file descriptors).
 * - It should be possible to promote a read-only lock to an 
 *   exclusive lock.
 * - Arbitrary strings should be useable as resource names. */

#include "amanda.h"
/* Interface to the implementations in common-src/amflock-*.c */

#ifdef WANT_AMFLOCK_POSIX
extern amflock_impl_t amflock_posix_impl;
#endif
#ifdef WANT_AMFLOCK_FLOCK
extern amflock_impl_t amflock_flock_impl;
#endif
#ifdef WANT_AMFLOCK_LOCKF
extern amflock_impl_t amflock_lockf_impl;
#endif
#ifdef WANT_AMFLOCK_LNLOCK
extern amflock_impl_t amflock_lnlock_impl;
#endif

amflock_impl_t *amflock_impls[] = {
#ifdef WANT_AMFLOCK_POSIX
    &amflock_posix_impl,
#endif
#ifdef WANT_AMFLOCK_FLOCK
    &amflock_flock_impl,
#endif
#ifdef WANT_AMFLOCK_LOCKF
    &amflock_lockf_impl,
#endif
#ifdef WANT_AMFLOCK_LNLOCK
    &amflock_lnlock_impl,
#endif
    NULL
};

/* Interface functions */
/* FIXME: for now, these just use the first non-NULL implementation
 */

/* Get a file lock (for read/write files).
*/
int
amflock(
    int		fd,
    char *	resource)
{
    if (!amflock_impls[0]) return 0; /* no locking */
    return amflock_impls[0]->amflock_impl(fd, resource);
}

/*
 * Get a file lock (for read-only files).
 */
int
amroflock(
    int		fd,
    char *	resource)
{
    if (!amflock_impls[0]) return 0; /* no locking */
    return amflock_impls[0]->amroflock_impl(fd, resource);
}

/*
 * Release a file lock.
 */
int
amfunlock(
    int		fd,
    char *	resource)
{
    if (!amflock_impls[0]) return 0; /* no locking */
    return amflock_impls[0]->amfunlock_impl(fd, resource);
}
