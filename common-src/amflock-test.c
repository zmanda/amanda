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
#include "testutils.h"

/* from amflock.c */
extern amflock_impl_t *amflock_impls[];

/* Test all amflock implementations available for basic 
 * functionality
 */
static int
test_impls(void)
{
    amflock_impl_t **imp = amflock_impls;
    char *filename = "./amflocktest.file";
    char *resource = "rez";
    int fd;
    int lock_ro;

    /* set lnlock's lock directory to the current directory */
    extern char *_lnlock_dir;
    _lnlock_dir = ".";

    while (*imp) {
	tu_dbg("Testing amflock-%s\n", (*imp)->impl_name);

	for (lock_ro = 0; lock_ro < 2; lock_ro++) { /* false (0) or true (1) */
	    if (unlink(filename) == -1 && errno != ENOENT) {
		perror("unlink");
		return 0;
	    }

	    if ((fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600)) == -1) {
		perror("open");
		return 0;
	    }

	    if (lock_ro) {
		if ((*imp)->amroflock_impl(fd, resource) != 0) {
		    perror("amroflock");
		    return 0;
		}
	    } else {
		if ((*imp)->amflock_impl(fd, resource) != 0) {
		    perror("amflock");
		    return 0;
		}
	    }

	    if ((*imp)->amfunlock_impl(fd, resource) != 0) {
		perror("amfunlock");
		return 0;
	    }

	    close(fd); /* ignore error */
	    unlink(filename); /* ignore error */
	}

	fprintf(stderr, "  PASS amflock-%s\n", (*imp)->impl_name);

	imp++;
    }

    return 1;
}

/* TODO: a more serious test of exclusion using multiple processes */

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_impls, 10),
	TU_END()
    };

    return testutils_run_tests(argc, argv, tests);
}
