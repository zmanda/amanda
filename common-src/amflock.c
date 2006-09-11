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
 * $Id: amflock.c,v 1.28 2006/05/25 01:47:11 johnfranks Exp $
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
** - "configure" compiles this with -DCONFIGURE_TEST to try and determine
**   whether a particular type of locking works.
*/

#include "amanda.h"

#if defined(USE_POSIX_FCNTL)
     static struct flock lock; /* zero-initialized */
#endif

#if !defined(USE_POSIX_FCNTL) && defined(USE_FLOCK)
#  ifdef HAVE_SYS_FILE_H
#    include <sys/file.h>
#  endif

#  if !defined(HAVE_FLOCK_DECL) && !defined(CONFIGURE_TEST)
     extern int flock(int fd, int operation);
#  endif
#endif


#if !defined(USE_POSIX_FCNTL) && !defined(USE_FLOCK) && defined(USE_LOCKF)

/* XPG4-UNIX (eg, SGI IRIX, DEC DU) has F_ULOCK instead of F_UNLOCK */
#if defined(F_ULOCK) && !defined(F_UNLOCK)
#  define F_UNLOCK F_ULOCK
#endif

/* Lock a file using lockf().
** Notes:
** - returns errors for some non-files like pipes.
** - probably only works for files open for writing.
*/
int
use_lockf(
    int fd,	/* fd of file to operate on */
    int op)	/* true to lock; false to unlock */
{
	off_t pos;

	if (op) {
		/* lock from here on */
		if (lockf(fd, F_LOCK, (off_t)0) == -1) return -1;
	}
	else {
		/* unlock from here on */
		if (lockf(fd, F_UNLOCK, (off_t)0) == -1) return -1;

		/* unlock from bof to here */
		pos = lseek(fd, (off_t)0, SEEK_CUR);
		if (pos == (off_t)-1) {
			if (errno == ESPIPE) pos = (off_t)0;
			else return -1;
		}

		if (pos > (off_t)0 &&
		    lockf(fd, F_UNLOCK, -pos) == -1) return -1;
	}

	return 0;
}

#endif

#if !defined(USE_POSIX_FCNTL) && !defined(USE_FLOCK) && !defined(USE_LOCKF) && defined(USE_LNLOCK)
/* XXX - error checking in this section needs to be tightened up */

/* Delete a lock file.
*/
int
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
int
create_lock(
    char *fn,
    pid_t pid)
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
	fprintf(f, "%ld\n", pid);
	if (fclose(f) == EOF)
	    return -1;
	return 0;
}

/* Read the pid out of a lock file.
**   -1=error, otherwise pid.
*/
long
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
int
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
int
steal_lock(
    char *	fn,	/* name of lock file to steal */
    pid_t	mypid,	/* my process id */
    char *	sres)	/* name of steal-resource to lock */
{
	int fd;
	char buff[64];
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

inuse:
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

/* Locking using existance of a file.
*/
int
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

	lockfile = vstralloc(AMANDA_TMPDIR, "/am", res, ".lock", NULL);

	if (!op) {
		/* unlock the resource */
		assert(read_lock(lockfile) == mypid);

		(void)delete_lock(lockfile);
		amfree(lockfile);
		return 0;
	}

	/* lock the resource */

	snprintf(pid_str, SIZEOF(pid_str), "%ld", mypid);
	tlockfile = vstralloc(AMANDA_TMPDIR, "am", res, ".", pid_str, NULL);

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
#endif


/*
 * Get a file lock (for read-only files).
 */
int
amroflock(
    int		fd,
    char *	resource)
{
	int r;

#ifdef USE_POSIX_FCNTL
	(void)resource; /* Quiet unused paramater warning */
	lock.l_type = F_RDLCK;
	lock.l_whence = SEEK_SET;
	r = fcntl(fd, F_SETLKW, &lock);
#else
	(void)fd; /* Quiet unused paramater warning */
	r = amflock(fd, resource);
#endif

	return r;
}


/* Get a file lock (for read/write files).
*/
int
amflock(
    int		fd,
    char *	resource)
{
	int r;

#ifdef USE_POSIX_FCNTL
	(void)resource; /* Quiet unused paramater warning */
	lock.l_type = F_WRLCK;
	lock.l_whence = SEEK_SET;
	r = fcntl(fd, F_SETLKW, &lock);
#else
#ifdef USE_FLOCK
	(void)resource; /* Quiet unused paramater warning */
	r = flock(fd, LOCK_EX);
#else
#ifdef USE_LOCKF
	(void)resource; /* Quiet unused paramater warning */
	r = use_lockf(fd, 1);
#else
#ifdef USE_LNLOCK
	(void)fd; /* Quiet unused paramater warning */
	r = ln_lock(resource, 1);
#else
	(void)fd; /* Quiet unused paramater warning */
	(void)resource; /* Quiet unused paramater warning */
	r = 0;
#endif
#endif
#endif
#endif

	return r;
}


/* Release a file lock.
*/
int
amfunlock(
    int		fd,
    char *	resource)
{
	int r;

#ifdef USE_POSIX_FCNTL
	(void)resource; /* Quiet unused paramater warning */
	lock.l_type = F_UNLCK;
	lock.l_whence = SEEK_SET;
	r = fcntl(fd, F_SETLK, &lock);
#else
#ifdef USE_FLOCK
	(void)resource; /* Quiet unused paramater warning */
	r = flock(fd, LOCK_UN);
#else
#ifdef USE_LOCKF
	(void)fd; /* Quiet unused paramater warning */
	r = use_lockf(fd, 0);
#else
#ifdef USE_LNLOCK
	(void)fd; /* Quiet unused paramater warning */
	r = ln_lock(resource, 0);
#else
	(void)fd; /* Quiet unused paramater warning */
	(void)resource; /* Quiet unused paramater warning */
	r = 0;
#endif
#endif
#endif
#endif

	return r;
}


/* Test routine for use by configure.
** (I'm not sure why we use both return and exit!)
** XXX the testing here should be a lot more comprehensive.
**     - lock the file and then try and lock it from another process
**     - lock the file from another process and check that process
**       termination unlocks it.
**     The hard part is to find a system independent way to not block
**     for ever.
*/
#ifdef CONFIGURE_TEST
int
main(
    int argc,
    char **argv)
{
    int lockfd;
    char *filen = "/tmp/conftest.lock";
    char *resn = "test";
    int fd;

    (void)argc;		/* Quiet compiler warning */
    (void)argv;		/* Quiet compiler warning */

    unlink(filen);
    if ((lockfd = open(filen, O_RDONLY | O_CREAT | O_EXCL, 0600)) == -1) {
	perror (filen);
	exit(10);
    }

    if (amroflock(lockfd, resn) != 0) {
	perror ("amroflock");
	exit(1);
    }
    if (amfunlock(lockfd, resn) != 0) {
	perror ("amfunlock/2");
	exit(2);
    }

    /*
     * Do not use aclose() here.  During configure we do not have
     * areads_relbuf() available and it makes configure think all
     * the tests have failed.
     */
    close(lockfd);

    unlink(filen);
    if ((lockfd = open(filen, O_WRONLY | O_CREAT | O_EXCL, 0600)) == -1) {
	perror (filen);
	exit(20);
    }

    if (amflock(lockfd, resn) != 0) {
	perror ("amflock");
	exit(3);
    }
    if (amfunlock(lockfd, resn) != 0) {
	perror ("amfunlock/4");
	exit(4);
    }

    close(lockfd);

    exit(0);
}
#endif
