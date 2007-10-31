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

static int ln_lock(char *res, int op);
char *_lnlock_dir = AMANDA_TMPDIR; /* amflock-test changes this; it's a constant otherwise */

/* XXX - error checking in this section needs to be tightened up */

/* Delete a lock file.
*/
static int
delete_lock(
    char *fn)
{
	int rc;

	rc = unlink(fn);
	if (rc != 0 && errno == ENOENT) rc = 0;

	return rc;
}

/* Create a lock file.
*/
static int
create_lock(
    char *fn,
    long pid)
{
	int fd;
	FILE *f;
	int mask;

	(void)delete_lock(fn);			/* that's MY file! */

	mask = umask(0027);
	fd = open(fn, O_WRONLY | O_CREAT | O_EXCL, 0640);
	umask(mask);
	if (fd == -1) return -1;

	if((f = fdopen(fd, "w")) == NULL) {
	    aclose(fd);
	    return -1;
	}
	g_fprintf(f, "%ld\n", pid);
	if (fclose(f) == EOF)
	    return -1;
	return 0;
}

/* Read the pid out of a lock file.
**   -1=error, otherwise pid.
*/
static long
read_lock(
    char *	fn) /* name of lock file */
{
	int save_errno;
	FILE *f;
	long pid;

	if ((f = fopen(fn, "r")) == NULL) {
		return -1;
	}
	if (fscanf(f, "%ld", &pid) != 1) {
		save_errno = errno;
		afclose(f);
		errno = save_errno;
		return -1;
	}
	if (fclose(f) != 0) {
		return -1;
	}
	return pid;
}

/* Link a lock if we can.
**   0=done, 1=already locked, -1=error.
*/
static int
link_lock(
    char *	lk,	/* real lock file */
    char *	tlk)	/* temp lock file */
{
	int rc;
	int serrno;	/* saved errno */
	struct stat lkstat, tlkstat;

	/* an atomic check and set operation */
	rc = link(tlk, lk);
	if (rc == 0) return 0; /* XXX do we trust it? */

	/* link() says it failed - don't beleive it */
	serrno = errno;

	if (stat(lk, &lkstat) == 0 &&
	    stat(tlk, &tlkstat) == 0 &&
	    lkstat.st_ino == tlkstat.st_ino)
		return 0;	/* it did work! */

	errno = serrno;

	if (errno == EEXIST) rc = 1;

	return rc;
}

/* Steal a lock if we can.
**   0=done; 1=still in use; -1 = error.
*/
static int
steal_lock(
    char *	fn,	/* name of lock file to steal */
    long	mypid,	/* my process id */
    char *	sres)	/* name of steal-resource to lock */
{
	long pid;
	int rc;

	/* prevent a race with another stealer */
	rc = ln_lock(sres, 1);
	if (rc != 0) goto error;

	pid = read_lock(fn);
	if (pid == -1) {
		if (errno == ENOENT) goto done;
		goto error;
	}

	if (pid == mypid) goto steal; /* i'm the locker! */

	/* are they still there ? */
	rc = kill((pid_t)pid, 0);
	if (rc != 0) {
		if (errno == ESRCH) goto steal; /* locker has gone */
		goto error;
	}

	rc = ln_lock(sres, 0);
	if (rc != 0) goto error;

	return 1;

steal:
	rc = delete_lock(fn);
	if (rc != 0) goto error;

done:
	rc = ln_lock(sres, 0);
	if (rc != 0) goto error;

	return 0;

error:
	rc = ln_lock(sres, 0);

	return -1;
}

static int
ln_lock(
    char *	res, /* name of resource to lock */
    int		op)  /* true to lock; false to unlock */
{
	long mypid;
	char *lockfile = NULL;
	char *tlockfile = NULL;
	char *mres = NULL;
	int rc;
	char pid_str[NUM_STR_SIZE];

	mypid = (long)getpid();

	lockfile = vstralloc(_lnlock_dir, "/am", res, ".lock", NULL);

	if (!op) {
		/* unlock the resource */
		assert(read_lock(lockfile) == mypid);

		(void)delete_lock(lockfile);
		amfree(lockfile);
		return 0;
	}

	/* lock the resource */

	g_snprintf(pid_str, SIZEOF(pid_str), "%ld", mypid);
	tlockfile = vstralloc(_lnlock_dir, "/am", res, ".", pid_str, NULL);

	(void)create_lock(tlockfile, mypid);

	mres = stralloc2(res, ".");

	while(1) {
		rc = link_lock(lockfile, tlockfile);
		if (rc == -1) break;
		if (rc == 0) break;

		rc = steal_lock(lockfile, mypid, mres);
		if (rc == -1) break;
		if (rc == 0) continue;
		sleep(1);
	}

	(void) delete_lock(tlockfile);

	amfree(mres);
	amfree(tlockfile);
	amfree(lockfile);

	return rc;
}

static int
lnlock_lock(
    G_GNUC_UNUSED int fd,
    char *resource)
{
    return ln_lock(resource, 1);
}

static int
lnlock_unlock(
    G_GNUC_UNUSED int fd,
    char *resource)
{
    return ln_lock(resource, 0);
}

amflock_impl_t amflock_lnlock_impl = {
    lnlock_lock,
    lnlock_lock, /* no read-only support */
    lnlock_unlock,
    "lnlock"
};
