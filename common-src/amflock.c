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

#include "amanda.h"

/*
 * New Implementation
 */

#if (GLIB_MAJOR_VERSION > 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION >= 31))
# pragma GCC diagnostic push
# pragma GCC diagnostic ignored "-Wmissing-field-initializers"
  static GStaticMutex lock_lock = G_STATIC_MUTEX_INIT;
# pragma GCC diagnostic pop
#else
  static GStaticMutex lock_lock = G_STATIC_MUTEX_INIT;
#endif
static GHashTable *locally_locked_files = NULL;
static int lock_rw_rd(file_lock *lock, short l_type);

file_lock *
file_lock_new(
    const char *filename)
{
    file_lock *lock = g_new0(file_lock, 1);
    lock->filename = g_strdup(filename);
    lock->fd = -1;

    return lock;
}

void
file_lock_free(
    file_lock *lock)
{
    g_static_mutex_lock(&lock_lock);
    if (locally_locked_files) {
	g_hash_table_remove(locally_locked_files,
			    lock->filename);
    }

    if (lock->data)
	g_free(lock->data);
    if (lock->filename)
	g_free(lock->filename);

    if (lock->fd != -1)
	close(lock->fd);

    g_static_mutex_unlock(&lock_lock);
}

int
file_lock_lock(
    file_lock *lock)
{
    int rv = -2;
    int fd = -1;
    int saved_errno;
    struct flock lock_buf;
    struct stat stat_buf;

    g_assert(!lock->locked);

    /* protect from overlapping lock operations within a process */
    g_static_mutex_lock(&lock_lock);
    if (!locally_locked_files) {
	locally_locked_files = g_hash_table_new(g_str_hash, g_str_equal);
    }

    /* if this filename is in the hash table, then some other thread in this
     * process has locked it */
    if (g_hash_table_lookup(locally_locked_files, lock->filename)) {
	rv = 1;
	goto done;
    }

    /* The locks are advisory, so an error here never means the lock is already
     * taken. */
    lock->fd = fd = open(lock->filename, O_CREAT|O_RDWR, 0666);
    if (fd < 0) {
	rv = -1;
	goto done;
    }

    /* now try locking it */
    lock_buf.l_type = F_WRLCK;
    lock_buf.l_start = 0;
    lock_buf.l_whence = SEEK_SET;
    lock_buf.l_len = 0; /* to EOF */
    if (fcntl(fd, F_SETLK, &lock_buf) < 0) {
	if (errno == EACCES || errno == EAGAIN)
	    rv = 1;
	else
	    rv = -1;
	goto done;
    }

    /* and read the file in its entirety */
    if (fstat(fd, &stat_buf) < 0) {
	rv = -1;
	goto done;
    }

    if (!(stat_buf.st_mode & S_IFREG)) {
	rv = -1;
	errno = EINVAL;
	goto done;
    }

    if (stat_buf.st_size) {
	lock->data = g_malloc(stat_buf.st_size);
	lock->len = stat_buf.st_size;
	if (full_read(fd, lock->data, lock->len) < lock->len) {
	    rv = -1;
	    goto done;
	}
    }

    fd = -1; /* we'll keep the file now */
    lock->locked = TRUE;

    /* the lock is acquired; record this in the hash table */
    g_hash_table_insert(locally_locked_files, lock->filename, lock->filename);

    rv = 0;

done:
    saved_errno = errno;
    g_static_mutex_unlock(&lock_lock);
    if (fd >= 0) /* close and unlock if an error occurred */
	close(fd);
    errno = saved_errno;
    return rv;
}

static int
lock_rw_rd(
    file_lock *lock,
    short      l_type)
{
    int rv = -2;
    int fd = -1;
    int saved_errno;
    struct flock lock_buf;
    struct stat stat_buf;

    g_assert(!lock->locked);

    /* protect from overlapping lock operations within a process */
    g_static_mutex_lock(&lock_lock);

    /* The locks are advisory, so an error here never means the lock is already
     * taken. */
    lock->fd = fd = open(lock->filename, O_CREAT|O_RDWR, 0666);
    if (fd < 0) {
	rv = -1;
	goto done;
    }

    /* now try locking it */
    lock_buf.l_type = l_type;
    lock_buf.l_start = 0;
    lock_buf.l_whence = SEEK_SET;
    lock_buf.l_len = 0; /* to EOF */
    if (fcntl(fd, F_SETLK, &lock_buf) < 0) {
	if (errno == EACCES || errno == EAGAIN)
	    rv = 1;
	else
	    rv = -1;
	goto done;
    }

    /* and read the file in its entirety */
    if (fstat(fd, &stat_buf) < 0) {
	rv = -1;
	goto done;
    }

    if (!(stat_buf.st_mode & S_IFREG)) {
	rv = -1;
	errno = EINVAL;
	goto done;
    }

    fd = -1; /* we'll keep the file now */
    lock->locked = TRUE;

    rv = 0;

done:
    saved_errno = errno;
    g_static_mutex_unlock(&lock_lock);
    if (fd >= 0) /* close and unlock if an error occurred */
	close(fd);
    errno = saved_errno;
    return rv;
}

int
file_lock_lock_wr(
    file_lock *lock)
{
    return lock_rw_rd(lock, F_WRLCK);
}

int
file_lock_lock_rd(
    file_lock *lock)
{
    return lock_rw_rd(lock, F_RDLCK);
}

int
file_lock_locked(
    file_lock *lock)
{
    return lock->locked;
}

int
file_lock_write(
    file_lock *lock,
    const char *data,
    size_t len)
{
    int fd = lock->fd;

    g_assert(lock->locked);

    /* seek to position 0, rewrite, and truncate */
    if (lseek(fd, 0, SEEK_SET) < 0)
	return -1;

    /* from here on out, any errors have corrupted the datafile.. */
    if (full_write(fd, data, len) < len)
	return -1;

    if (lock->len > len) {
	if (ftruncate(fd, len) < 0)
	    return -1;
    }

    if (lock->data)
	g_free(lock->data);
    lock->data = g_strdup(data);
    lock->len = len;

    return 0;
}

int
file_lock_unlock(
    file_lock *lock)
{
    g_assert(lock->locked);

    g_static_mutex_lock(&lock_lock);

    /* relase the filesystem-level lock */
    close(lock->fd);

    /* and the hash table entry */
    if (locally_locked_files) {
	g_hash_table_remove(locally_locked_files, lock->filename);
    }

    g_static_mutex_unlock(&lock_lock);

    if (lock->data)
	g_free(lock->data);
    lock->data = NULL;
    lock->len = 0;
    lock->fd = -1;
    lock->locked = FALSE;

    return 0;
}

/*
 * Old Implementation
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
