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

#include "amanda.h"

/* from amflock.c */
extern amflock_impl_t *amflock_impls[];

int
main(void)
{
    amflock_impl_t **imp = amflock_impls;
    char *filename = "./amflocktest.file";
    char *resource = "rez";
    int fd;
    int lock_ro;

    while (*imp) {
	fprintf(stderr, _("Testing amflock-%s\n"), (*imp)->impl_name);
	alarm(5); /* time out after 5 seconds */

	for (lock_ro = 0; lock_ro < 2; lock_ro++) { /* false (0) or true (1) */
	    if (unlink(filename) == -1 && errno != ENOENT) {
		perror("unlink");
		return 1;
	    }

	    if ((fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600)) == -1) {
		perror("open");
		return 1;
	    }

	    if (lock_ro) {
		if ((*imp)->amroflock_impl(fd, resource) != 0) {
		    perror("amroflock");
		    return 1;
		}
	    } else {
		if ((*imp)->amflock_impl(fd, resource) != 0) {
		    perror("amflock");
		    return 1;
		}
	    }

	    if ((*imp)->amfunlock_impl(fd, resource) != 0) {
		perror("amfunlock");
		return 1;
	    }

	    close(fd); /* ignore error */
	    unlink(filename); /* ignore error */
	}

	imp++;
    }

    return 0;
}
