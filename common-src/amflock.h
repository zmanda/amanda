/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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

/* Moved from amanda.h and amflock.c by Dustin J. Mitchell <dustin@zmanda.com> */

/*
 * New interface
 */

typedef struct file_lock_ {
    /* the entire contents of the locked file */
    char *data;
    size_t len;

    /* do not touch! */
    gboolean locked;
    int fd;
    char *filename;
} file_lock;

/* Create a new, unlocked file_lock object
 *
 * @param filename: filename of the file to lock
 */
file_lock *file_lock_new(const char *filename);

/* Free a file_lock object, unlocking it in the process
 * if necessary.
 *
 * @param lock: the file_lock object
 */
void file_lock_free(file_lock *lock);

/* Lock a file and read its contents.  This function will create the file if it
 * doesn't already exist, in which case lock->data will be NULL and lock->len
 * will be 0.
 *
 * If the file cannot be locked, this function will not block, but will return
 * 1.  Callers must poll for lock release, if it is desired.  This is for
 * compatibility with the GMainLoop, which does not allow arbitrary blocking.
 *
 * The lock represented by this function applies both within the current
 * process (excluding other threads) and across all Amanda processes on the
 * system, assuming that the filename is specified identically.  On
 * sufficiently modern systems, it will also function propertly across NFS
 * mounts.
 *
 * Becuse this function may use fcntl to implement the locking, it is critical
 * that the locked filename not be opened for any other reason while the lock
 * is held, as this may unintentionally release the lock.
 *
 * @param lock: the file_lock object @returns: -1 on error, 0 on success, 1 on
 * a busy lock (see above)
 */
int file_lock_lock(file_lock *lock);

/* Write the given data to the locked file, and reset the file_lock
 * data member to point to a copy of the new data.  This does not unlock
 * the file.
 *
 * @param lock: the file_lock object
 * @param data: data to write
 * @param len: size of data
 * @returns: -1 on error (with errno set), 0 on succes
 */
int file_lock_write(file_lock *lock, const char *data, size_t len);

/* Unlock a locked file, without first re-writing it to disk.
 *
 * @param lock: the file_lock object
 * @returns: -1 on error (with errno set), 0 on succes
 */
int file_lock_unlock(file_lock *lock);

/*
 * Old interface
 */

/*
 * Get a file lock (for read/write files).
 */
int amflock(int fd, char *resource);

/*
 * Get a file lock (for read-only files).
 */
int amroflock(int fd, char *resource);

/*
 * Release a file lock.
 */
int amfunlock(int fd, char *resource);

/* Implementation interface */
typedef int (*amflock_fn)(int, char *);
typedef struct amflock_impl_s {
    amflock_fn amflock_impl;
    amflock_fn amroflock_impl;
    amflock_fn amfunlock_impl;
    char *impl_name;
} amflock_impl_t;
