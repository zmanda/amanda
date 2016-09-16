/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1997-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc  All Rights Reserved.
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
 * Author: AMANDA core development group.
 */

#ifndef __AM_FILE_H__
#define __AM_FILE_H__

extern int    mkpdir(char *file, mode_t mode, uid_t uid, gid_t gid);
extern int    rmpdir(char *file, char *topdir);

/* Given a pathname, convert it to "canonical form" for this system.  Currently,
 * this means nothing on POSIX, but means substituting /cygdrive, etc. on Cygwin.
 *
 * @param pathname: the pathname to canonicalize
 * @param result_buf (output): the canonicalize pathname; this should be a buffer of
 * at least PATH_MAX bytes.
 */
void canonicalize_pathname(char *pathname, char *result_buf);

extern char  *sanitise_filename(char *inp);
char  *old_sanitise_filename(char *inp);
void    safe_fd(int fd_start, int fd_count);
void    safe_fd2(int fd_start, int fd_count, int fd1);
void    safe_fd3(int fd_start, int fd_count, int fd1, int fd2);
void	safe_cd(void);
void	save_core(void);

/* Get the uid of CLIENT_LOGIN, or -1 if it doesn't exist.  Note that, if
 * only running a server, CLIENT_LOGIN may legitimately not exist.
 *
 * @returns: userid, or -1 if invalid
 */
uid_t get_client_uid(void);

/* Get the gid of CLIENT_LOGIN, or -1 if it doesn't exist.  Note that, if
 * only running a server, CLIENT_LOGIN may legitimately not exist.
 *
 * @returns: groupid, or -1 if invalid
 */
gid_t get_client_gid(void);

extern /*@only@*/ /*@null@*/ char *debug_agets(const char *c, int l, FILE *file);
extern /*@only@*/ /*@null@*/ char *debug_pgets(const char *c, int l, FILE *file);
extern /*@only@*/ /*@null@*/ char *debug_areads(const char *c, int l, int fd);
#define agets(f)	      debug_agets(__FILE__,__LINE__,(f))
#define pgets(f)	      debug_pgets(__FILE__,__LINE__,(f))
#define areads(f)	      debug_areads(__FILE__,__LINE__,(f))

ssize_t	areads_dataready(int fd);
void	areads_relbuf(int fd);

/*
 * "Safe" close macros.  Close the object then set it to a value that
 * will cause an error if referenced.
 *
 * aclose(fd) -- close a file descriptor and set it to -1.
 * afclose(f) -- close a stdio file and set it to NULL.
 * apclose(p) -- close a stdio pipe file and set it to NULL.
 *
 * Note: be careful not to do the following:
 *
 *  for(fd = low; fd < high; fd++) {
 *      aclose(fd);
 *  }
 *
 * Since aclose() sets the argument to -1, this will loop forever.
 * Just copy fd to a temp variable and use that with aclose().
 *
 * Aclose() interacts with areads() to inform it to release any buffer
 * it has outstanding on the file descriptor.
 */

#define aclose(fd) do {							\
    if((fd) >= 0) {							\
	close(fd);							\
    }									\
    (fd) = -1;								\
} while(0)

#define aaclose(fd) do {						\
    if((fd) >= 0) {							\
	close(fd);							\
	areads_relbuf(fd);						\
    }									\
    (fd) = -1;								\
} while(0)

#define afclose(f) do {							\
    if((f) != NULL) {							\
	fclose(f);							\
    }									\
    (f) = NULL;								\
} while(0)

#define apclose(p) do {							\
    if((p) != NULL) {							\
	pclose(p);							\
    }									\
    (p) = NULL;								\
} while(0)


/* Calls system open(), but takes care of interrupted system calls and
 * clears the close-on-exec bit. In the failure case, errno is
 * retained from the final call to open(). */
extern int robust_open(const char * pathname, int flags, mode_t mode);

/* Same idea but for close. */
extern int robust_close(int fd);

/* Get the original working directory, at application startup
 *
 * @returns: pointer to statically allocated string
 */
char *get_original_cwd(void);

/**
 * Read up to "count" bytes from a file descriptor, optionally collecting the
 * read operation status (0 on success, not 0 otherwise).
 *
 * This function exists to overcome the confusing behavior of full_read():
 *
 * - unlike read(2), full_read() does not return -1 on failure;
 * - errno needs to be explicitly checked for each time the number of bytes
 *   actually read is less than the "count" argument.
 *
 * With this function, a full read becomes a one time process. Error collecting
 * is optional.
 *
 * @param fd: the file descriptor to read from
 * @param buf: the buffer to write data into
 * @param count: the number of bytes to write into the buffer
 * @param err (output): 0 if "count" bytes have been read; errno (as set by
 *                      full_read()) otherwise
 * @returns: the number of bytes read from the file descriptor
 */

gsize read_fully(int fd, void *buf, gsize count, int *err);

#endif /* __AM_FILE_H__ */
