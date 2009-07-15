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
#include "glib-util.h"

/* from amflock.c */
extern amflock_impl_t *amflock_impls[];

#define TEST_FILENAME "./amflocktest.file"

/* Test all amflock implementations available for basic 
 * functionality
 */
static int
test_old_impls(void)
{
    amflock_impl_t **imp = amflock_impls;
    char *resource = "rez";
    int fd;
    int lock_ro;

    /* set lnlock's lock directory to the current directory */
    extern char *_lnlock_dir;
    _lnlock_dir = ".";

    while (*imp) {
	tu_dbg("Testing amflock-%s\n", (*imp)->impl_name);

	for (lock_ro = 0; lock_ro < 2; lock_ro++) { /* false (0) or true (1) */
	    if (unlink(TEST_FILENAME) == -1 && errno != ENOENT) {
		perror("unlink");
		return 0;
	    }

	    if ((fd = open(TEST_FILENAME, O_RDWR | O_CREAT | O_EXCL, 0600)) == -1) {
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
	    unlink(TEST_FILENAME); /* ignore error */
	}

	fprintf(stderr, "  PASS amflock-%s\n", (*imp)->impl_name);

	imp++;
    }

    return 1;
}

/*
 * Test lock and write_and_unlock
 */
static gboolean
inc_counter(file_lock *lock)
{
    char old_val = 'a';
    char new_val;

    if (lock->len) {
	old_val = lock->data[0];
    }

    g_assert(old_val < 'z');

    new_val = old_val + 1;
    if (file_lock_write(lock, &new_val, 1) == -1) {
	g_fprintf(stderr, "file_lock_write: %s\n",
			strerror(errno));
	return FALSE;
    }

    return TRUE;
}

#define pipeget(fd, cp) \
    (read((fd), (cp), 1) == 1)

#define pipeput(fd, s) \
    g_assert(write((fd), s, 1) == 1);

static void
locking_slave(int in_fd, int out_fd)
{
    char cmd;
    int rv;
    file_lock *lock = file_lock_new(TEST_FILENAME);
    gboolean locked = 0;

    while (1) {
	if (!pipeget(in_fd, &cmd))
	    cmd = 'q';

	switch (cmd) {
	    case 'q': /* q = quit */
		tu_dbg("slave: quitting\n");
		file_lock_free(lock);
		lock = NULL;
		return;

	    case 'l': /* l = try locking; reply with 'y' or 'n' */
		g_assert(!locked);
		rv = file_lock_lock(lock);
		if (rv == -1) {
		    g_fprintf(stderr, "file_lock_lock: %s\n",
				    strerror(errno));
		    return;
		}
		tu_dbg("slave: lock attempt => %s\n", (rv == 1)? "n" : "y");
		pipeput(out_fd, (rv == 1)? "n" : "y");
		if (rv != 1)
		    locked = 1;
		break;

	    case 'i': /* i = increment counter, reply with new value */
		g_assert(locked);
		if (!inc_counter(lock))
		    return;
		tu_dbg("slave: inc'd to %c\n", lock->data[0]);
		pipeput(out_fd, lock->data);
		break;

	    case 'u': /* u = try unlocking; reply with 'k' */
		g_assert(locked);
		rv = file_lock_unlock(lock);
		if (rv != 0) {
		    g_fprintf(stderr, "file_lock_unlock: %s\n",
			    strerror(errno));
		    return;
		}
		tu_dbg("slave: unlocked\n");
		pipeput(out_fd, "k");
		locked = 0;
		break;

	    default:
		return;
	}
    }
}

static int
locking_master(int in_fd, int out_fd)
{
    file_lock *lock = file_lock_new(TEST_FILENAME);
    int rv;
    char slaveres;

    /* start by locking here and incrementing the value */
    rv = file_lock_lock(lock);
    if (rv == -1) {
	g_fprintf(stderr, "file_lock_lock: %s\n", strerror(errno));
	return 0;
    }
    g_assert(rv != 1); /* not already locked */
    tu_dbg("master: locked\n");

    if (!inc_counter(lock))
	return 0;

    g_assert(lock->data[0] == 'b');
    tu_dbg("master: inc'd to b\n");

    /* unlock and re-lock */
    rv = file_lock_unlock(lock);
    if (rv != 0) {
	g_fprintf(stderr, "file_lock_unlock: %s\n", strerror(errno));
	return 0;
    }
    tu_dbg("master: unlocked\n");

    rv = file_lock_lock(lock);
    if (rv == -1) {
	g_fprintf(stderr, "file_lock_lock: %s\n", strerror(errno));
	return 0;
    }
    g_assert(rv != 1); /* not already locked */
    tu_dbg("master: locked\n");

    /* inc it again */
    g_assert(lock->data[0] == 'b');
    inc_counter(lock);
    g_assert(lock->data[0] == 'c');
    tu_dbg("master: inc'd to c\n");

    /* the slave should fail to get a lock now */
    pipeput(out_fd, "l");
    g_assert(pipeget(in_fd, &slaveres));
    g_assert(slaveres == 'n');

    /* and, finally unlock */
    rv = file_lock_unlock(lock);
    if (rv != 0) {
	g_fprintf(stderr, "file_lock_unlock: %s\n", strerror(errno));
	return 0;
    }
    tu_dbg("master: unlocked\n");

    /* the slave should succeed now */
    pipeput(out_fd, "l");
    g_assert(pipeget(in_fd, &slaveres));
    g_assert(slaveres == 'y');

    pipeput(out_fd, "i");
    g_assert(pipeget(in_fd, &slaveres));
    g_assert(slaveres == 'd');

    /* master shouldn't be able to lock now */
    rv = file_lock_lock(lock);
    if (rv == -1) {
	g_fprintf(stderr, "file_lock_lock: %s\n", strerror(errno));
	return 0;
    }
    g_assert(rv == 1); /* already locked */
    tu_dbg("master: lock attempt failed (as expected)\n");

    pipeput(out_fd, "i");
    g_assert(pipeget(in_fd, &slaveres));
    g_assert(slaveres == 'e');

    /* get the slave to unlock */
    pipeput(out_fd, "u");
    g_assert(pipeget(in_fd, &slaveres));
    g_assert(slaveres == 'k');

    /* we should get a lock now */
    rv = file_lock_lock(lock);
    if (rv == -1) {
	g_fprintf(stderr, "file_lock_lock: %s\n", strerror(errno));
	return 0;
    }
    g_assert(rv != 1); /* not already locked */
    tu_dbg("master: lock attempt succeeded\n");

    g_assert(lock->data[0] == 'e');

    /* leave it unlocked, just to see what happens */

    return 1;
}

static gpointer
test_intra_proc_locking_thd(gpointer *fdptr)
{
    int *fds = (int *)fdptr;
    locking_slave(fds[0], fds[1]);
    return NULL;
}

static int
test_intra_proc_locking(void)
{
    GThread *thd;
    int outpipe[2], inpipe[2];
    int thd_fds[2];
    int rv;

    unlink(TEST_FILENAME);

    g_assert(pipe(outpipe) == 0);
    g_assert(pipe(inpipe) == 0);

    thd_fds[0] = outpipe[0];
    thd_fds[1] = inpipe[1];
    thd = g_thread_create((GThreadFunc)test_intra_proc_locking_thd, (gpointer)thd_fds, TRUE, NULL);

    rv = locking_master(inpipe[0], outpipe[1]);

    /* close the write end of the outgoing pipe, which should trigger an EOF on
     * the slave if it's still running */
    close(outpipe[1]);
    g_thread_join(thd);
    unlink(TEST_FILENAME);

    /* caller will kill the remaining files */

    return rv;
}

static int
test_inter_proc_locking(void)
{
    int outpipe[2], inpipe[2];
    int pid;
    int rv;

    unlink(TEST_FILENAME);

    g_assert(pipe(outpipe) == 0);
    g_assert(pipe(inpipe) == 0);

    if ((pid = fork()) == 0) {
	close(outpipe[1]);
	close(inpipe[0]);
	locking_slave(outpipe[0], inpipe[1]);
	exit(0);
    }

    close(outpipe[0]);
    close(inpipe[1]);

    rv = locking_master(inpipe[0], outpipe[1]);

    /* close the write end of the outgoing pipe, which should trigger an EOF on
     * the slave if it's still running */
    close(outpipe[1]);
    waitpid(pid, NULL, 0);
    unlink(TEST_FILENAME);

    /* caller will kill the remaining files */

    return rv;
}

int
main(int argc, char **argv)
{
    static TestUtilsTest tests[] = {
	TU_TEST(test_old_impls, 90),
	TU_TEST(test_inter_proc_locking, 60),
	TU_TEST(test_intra_proc_locking, 60),
	TU_END()
    };

    glib_init();
    return testutils_run_tests(argc, argv, tests);
}
