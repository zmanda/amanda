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

/* moved from amflock.c by Dustin J. Mitchell <dustin@zmanda.com> */

#include "amanda.h"

/* XPG4-UNIX (eg, SGI IRIX, DEC DU) has F_ULOCK instead of F_UNLOCK */
#if defined(F_ULOCK) && !defined(F_UNLOCK)
#  define F_UNLOCK F_ULOCK
#endif

static int
lockf_lock(
    int	fd,
    G_GNUC_UNUSED char *resource)
{
    return lockf(fd, F_LOCK, (off_t)0);
}

static int
lockf_unlock(
    int	fd,
    G_GNUC_UNUSED char *resource)
{
    off_t pos;

    /* unlock from here on */
    if (lockf(fd, F_UNLOCK, (off_t)0) == -1) return -1;

    /* unlock from bof to here */
    pos = lseek(fd, (off_t)0, SEEK_CUR);
    if (pos == (off_t)-1) {
	if (errno == ESPIPE) 
	    pos = (off_t)0;
	else
	    return -1;
    }

    if (pos > (off_t)0) {
	if (lockf(fd, F_UNLOCK, -pos) == -1)
	    return -1;
    }

    return 0;
}

amflock_impl_t amflock_lockf_impl = {
    lockf_lock,
    lockf_lock, /* no read-only support */
    lockf_unlock,
    "lockf"
};
