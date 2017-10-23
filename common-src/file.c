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
/*
 * $Id: file.c,v 1.40 2006/07/19 17:41:15 martinea Exp $
 *
 * file and directory bashing routines
 */

#include "amanda.h"
#include "amutil.h"
#include "timestamp.h"
#include "file.h"

static struct areads_buffer *areads_getbuf(const char *s, int l, int fd);
static char *original_cwd = NULL;

/*
 * Make a directory hierarchy given an entry to be created (by the caller)
 * in the new target.  In other words, create all the directories down to
 * the last element, but not the last element.  So a (potential) file name
 * may be passed to mkpdir and all the parents of that file will be created.
 */
int
mkpdir(
    char *	file,	/* file to create parent directories for */
    mode_t	mode,	/* mode for new directories */
    uid_t	uid,	/* uid for new directories */
    gid_t	gid)	/* gid for new directories */
{
    char *dir;
    char *p;
    int rc;	/* return code */

    rc = 0;

    /* Remove last member of file, put the result in dir */
    dir = g_strdup(file); /* make a copy we can play with */
    p = strrchr(dir, '/');
    if (p)
	*p = '\0';

    rc = mkdir(dir, mode);
    if (rc != 0) {
	if (errno == ENOENT) { /* create parent directory */
	    rc = mkpdir(dir, mode, uid, gid);
	    if (rc != 0) {
		amfree(dir);
		return rc;
	    }
	    rc = mkdir(dir, mode);
	}
	if (rc != 0 && errno == EEXIST) {
	    amfree(dir);
	    return 0;
	}
    }

    /* mkdir succeded, set permission and ownership */
    if (rc == 0) {
	/* mkdir is affected by umask, so set the mode bits manually */
	rc = chmod(dir, mode);

	if (rc == 0 && geteuid() == 0) {
	    rc = chown(dir, uid, gid);
	}
    }

    amfree(dir);
    return rc;
}


/* Remove as much of a directory hierarchy as possible.
** Notes:
**  - assumes that rmdir() on a non-empty directory will fail!
**  - stops deleting before topdir, ie: topdir will not be removed
**  - if file is not under topdir this routine will not notice
*/
int
rmpdir(
    char *	file,	/* directory hierarchy to remove */
    char *	topdir)	/* where to stop removing */
{
    int rc;
    char *p, *dir;

    if(g_str_equal(file, topdir)) return 0; /* all done */

    rc = rmdir(file);
    if (rc != 0) switch(errno) {
#ifdef ENOTEMPTY
#if ENOTEMPTY != EEXIST			/* AIX makes these the same */
	case ENOTEMPTY:
#endif
#endif
	case EEXIST:	/* directory not empty */
	    return 0; /* cant do much more */
	case ENOENT:	/* it has already gone */
	    rc = 0; /* ignore */
	    break;
	case ENOTDIR:	/* it was a file */
	    rc = unlink(file);
	    break;
	}

    if(rc != 0) return -1; /* unexpected error */

    dir = g_strdup(file);

    p = strrchr(dir, '/');
    if (p == NULL || p == dir) {
        rc = 0;
    } else {
	*p = '\0';
	rc = rmpdir(dir, topdir);
    }

    amfree(dir);

    return rc;
}


/*
 *=====================================================================
 * Change directory to a "safe" location and set some base environment.
 *
 * void safe_cd (void)
 *
 * Set a default umask of 0077.
 *
 * Create the Amada debug directory (if defined) and the Amanda temp
 * directory.
 *
 * Try to chdir to the Amanda debug directory first, but it must be owned
 * by the Amanda user and not allow rwx to group or other.  Otherwise,
 * try the same thing to the Amanda temp directory.
 *
 * If that is all OK, call save_core().
 *
 * Otherwise, cd to "/" so if we take a signal we cannot drop core
 * unless the system administrator has made special arrangements (e.g.
 * pre-created a core file with the right ownership and permissions).
 *=====================================================================
 */

void
safe_cd(void)
{
    int			cd_ok = 0;
    struct stat		sbuf;
    char		*d;
    uid_t		client_uid = get_client_uid();
    gid_t		client_gid = get_client_gid();

    (void) umask(0077);

    /* stash away the current directory for later reference */
    if (original_cwd == NULL) {
	original_cwd = g_get_current_dir();
    }

    if (client_uid != (uid_t) -1) {
#if defined(AMANDA_DBGDIR)
	d = g_strconcat(AMANDA_DBGDIR, "/.", NULL);
	(void) mkpdir(d, (mode_t)0700, client_uid, client_gid);
	amfree(d);
#endif
	d = g_strconcat(AMANDA_TMPDIR, "/.", NULL);
	(void) mkpdir(d, (mode_t)0700, client_uid, client_gid);
	amfree(d);
    }

#if defined(AMANDA_DBGDIR)
    if (chdir(AMANDA_DBGDIR) != -1
	&& stat(".", &sbuf) != -1
	&& (sbuf.st_mode & 0777) == 0700	/* drwx------ */
	&& sbuf.st_uid == client_uid) {		/* owned by Amanda user */
	cd_ok = 1;				/* this is a good place to be */
    }
#endif
    if (! cd_ok
	&& chdir(AMANDA_TMPDIR) != -1
	&& stat(".", &sbuf) != -1
	&& (sbuf.st_mode & 0777) == 0700	/* drwx------ */
	&& sbuf.st_uid == client_uid) {		/* owned by Amanda user */
	cd_ok = 1;				/* this is a good place to be */
    }
    if(cd_ok) {
	save_core();				/* save any old core file */
    } else {
	if ((cd_ok = chdir("/")) == -1) {
	    (void)cd_ok;	/* Quiet compiler warning if DEBUG disabled */
	}
    }
}

/*
 *=====================================================================
 * Close all file descriptors except stdin, stdout and stderr.  Make
 * sure they are open.
 *
 * void safe_fd (fd_start, fd_count)
 *
 * entry:	fd_start - start of fd-s to leave alone (or -1)
 *		fd_count - count of fd-s to leave alone
 * exit:	none
 *
 * On exit, all three standard file descriptors will be open and pointing
 * someplace (either what we were handed or /dev/null) and all other
 * file descriptors (up to FD_SETSIZE) will be closed.
 *=====================================================================
 */

void
safe_fd(
    int		fd_start,
    int		fd_count)
{
    safe_fd5(fd_start, fd_count, 0, 0, 0, 0);
}

/*
 *=====================================================================
 * Close all file descriptors except stdin, stdout and stderr.  Make
 * sure they are open.
 *
 * void safe_fd2 (fd_start, fd_count, fd1)
 *
 * entry:	fd_start - start of fd-s to leave alone (or -1)
 *		fd_count - count of fd-s to leave alone
 *		fd1      - do not close
 * exit:	none
 *
 * On exit, all three standard file descriptors will be open and pointing
 * someplace (either what we were handed or /dev/null) and all other
 * file descriptors (up to FD_SETSIZE) will be closed.
 *=====================================================================
 */

void
safe_fd2(
    int		fd_start,
    int		fd_count,
    int		fd1)
{
    safe_fd5(fd_start, fd_count, fd1, 0, 0, 0);
}

/*
 *=====================================================================
 * Close all file descriptors except stdin, stdout and stderr.  Make
 * sure they are open.
 *
 * void safe_fd3 (fd_start, fd_count, fd1, fd2)
 *
 * entry:	fd_start - start of fd-s to leave alone (or -1)
 *		fd_count - count of fd-s to leave alone
 *		fd1      - do not close
 *		fd2      - do not close
 * exit:	none
 *
 * On exit, all three standard file descriptors will be open and pointing
 * someplace (either what we were handed or /dev/null) and all other
 * file descriptors (up to FD_SETSIZE) will be closed.
 *=====================================================================
 */

void
safe_fd3(
    int		fd_start,
    int		fd_count,
    int		fd1,
    int		fd2)
{
    safe_fd5(fd_start, fd_count, fd1, fd2, 0, 0);
}

/*
 *=====================================================================
 * Close all file descriptors except stdin, stdout and stderr.  Make
 * sure they are open.
 *
 * void safe_fd4 (fd_start, fd_count, fd1, fd2, fd3)
 *
 * entry:	fd_start - start of fd-s to leave alone (or -1)
 *		fd_count - count of fd-s to leave alone
 *		fd1      - do not close
 *		fd2      - do not close
 *		fd3      - do not close
 * exit:	none
 *
 * On exit, all three standard file descriptors will be open and pointing
 * someplace (either what we were handed or /dev/null) and all other
 * file descriptors (up to FD_SETSIZE) will be closed.
 *=====================================================================
 */

void
safe_fd4(
    int		fd_start,
    int		fd_count,
    int		fd1,
    int		fd2,
    int		fd3)
{
    safe_fd5(fd_start, fd_count, fd1, fd2, fd3, 0);
}

/*
 *=====================================================================
 * Close all file descriptors except stdin, stdout and stderr.  Make
 * sure they are open.
 *
 * void safe_fd5 (fd_start, fd_count, fd1, fd2, fd3, fd4)
 *
 * entry:	fd_start - start of fd-s to leave alone (or -1)
 *		fd_count - count of fd-s to leave alone
 *		fd1      - do not close
 *		fd2      - do not close
 *		fd3      - do not close
 *		fd4      - do not close
 * exit:	none
 *
 * On exit, all three standard file descriptors will be open and pointing
 * someplace (either what we were handed or /dev/null) and all other
 * file descriptors (up to FD_SETSIZE) will be closed.
 *=====================================================================
 */

void
safe_fd5(
    int		fd_start,
    int		fd_count,
    int		fd1,
    int		fd2,
    int		fd3,
    int		fd4)
{
    int			fd;

    for(fd = 0; fd < (int)FD_SETSIZE; fd++) {
	if (fd < 3) {
	    /*
	     * Open three file descriptors.  If one of the standard
	     * descriptors is not open it will be pointed to /dev/null...
	     *
	     * This avoids, for instance, someone running us with stderr
	     * closed so that when we open some other file, messages
	     * sent to stderr do not accidentally get written to the
	     * wrong file.
	     */
	    if (fcntl(fd, F_GETFD) == -1) {
		if (open("/dev/null", O_RDWR) == -1) {
		   g_fprintf(stderr, _("/dev/null is inaccessable: %s\n"),
		           strerror(errno));
		   exit(1);
		}
	    }
	} else {
	    /*
	     * Make sure nobody spoofs us with a lot of extra open files
	     * that would cause an open we do to get a very high file
	     * descriptor, which in turn might be used as an index into
	     * an array (e.g. an fd_set).
	     */
	    if ((fd < fd_start || fd >= fd_start + fd_count) &&
		(fd != fd1) &&
		(fd != fd2) &&
		(fd != fd3) &&
		(fd != fd4)) {
		close(fd);
	    }
	}
    }
}

/*
 *=====================================================================
 * Save an existing core file.
 *
 * void save_core (void)
 *
 * entry:	none
 * exit:	none
 *
 * Renames:
 *
 *	"core"          to "coreYYYYMMDD",
 *	"coreYYYYMMDD"  to "coreYYYYMMDDa",
 *	"coreYYYYMMDDa" to "coreYYYYMMDDb",
 *	...
 *
 * ... where YYYYMMDD is the modification time of the original file.
 * If it gets that far, an old "coreYYYYMMDDz" is thrown away.
 *=====================================================================
 */

void
save_core(void)
{
    struct stat sbuf;

    if(stat("core", &sbuf) != -1) {
        char *ts;
        char suffix[2];
        char *old, *new;

	ts = get_datestamp_from_time(sbuf.st_mtime);
        suffix[0] = 'z';
        suffix[1] = '\0';
        old = g_strjoin(NULL, "core", ts, suffix, NULL);
        new = NULL;
        while(ts[0] != '\0') {
            amfree(new);
            new = old;
            if(suffix[0] == 'a') {
                suffix[0] = '\0';
            } else if(suffix[0] == '\0') {
                ts[0] = '\0';
            } else {
                suffix[0]--;
            }
            old = g_strjoin(NULL, "core", ts, suffix, NULL);
            (void)rename(old, new);         /* it either works ... */
        }
	amfree(ts);
        amfree(old);
        amfree(new);
    }
}

/*
** Sanitise a file name.
** 
** Convert all '/', ':', and '\' characters to '_' so that we can use,
** for example, disk names as part of file names.
** Notes: 
**  - there is a many-to-one mapping between input and output
**  - Only / and '\0' are disallowed in filenames by POSIX, but Windows
**    disallows ':' and '\' as well.  Furthermore, we use ':' as a 
**    delimiter at other points in Amanda.
*/
char *
sanitise_filename(
    char *	inp)
{
    char *ret = g_strdup(inp);
    return g_strdelimit(ret, "/:\\", '_');
}

/* duplicate '_' */
char *
old_sanitise_filename(
    char *	inp)
{
    char *buf;
    size_t buf_size;
    char *s, *d;
    int ch;

    buf_size = 2*strlen(inp) + 1;		/* worst case */
    buf = g_malloc(buf_size);
    d = buf;
    s = inp;
    while((ch = *s++) != '\0') {
	if(ch == '_') {
	    *d++ = (char)ch;
	}
	if(ch == '/') {
	    ch = '_';	/* convert "bad" to "_" */
	}
	*d++ = (char)ch;
    }
    assert(d < buf + buf_size);
    *d = '\0';

    return buf;
}

void
canonicalize_pathname(char *pathname, char *result_buf)
{
#ifdef __CYGWIN__
    cygwin_conv_to_full_posix_path(pathname, result_buf);
#else
    strncpy(result_buf, pathname, PATH_MAX-1);
    result_buf[PATH_MAX-1] = '\0';
#endif
}

/*
 *=====================================================================
 * Get the next line of input from a stdio file.
 *
 * char *agets (FILE *stream)
 *
 * entry:	stream  -  stream to read
 * exit:	returns a pointer to an g_malloc'd string or NULL
 *		at EOF or error.  The functions ferror(stream) and
 *		feof(stream) should be checked by caller to determine
 *		stream status.
 *
 * Notes:	the newline at the end of a line, if read, is removed from
 *		the string. Quoted newlines are left intact.
 *		the caller is responsible for free'ing the string
 *
 *=====================================================================
 */

#define	AGETS_LINE_INCR	128

char *
debug_agets(
    const char *sourcefile,
    int		lineno,
    FILE *	stream)
{
    int	ch;
    char *line = g_malloc(AGETS_LINE_INCR);
    size_t line_size = 0;
    size_t loffset = 0;
    int	inquote = 0;
    int	escape = 0;

    (void)sourcefile;	/* Quiet unused parameter warning if not debugging */
    (void)lineno;	/* Quiet unused parameter warning if not debugging */

    while ((ch = fgetc(stream)) != EOF) {

	if (ch == '#' && !escape && !inquote) {
	    // consume to the end of line.
	    ch = fgetc(stream);
	    while (ch != EOF && ch != '\n') {
		ch = fgetc(stream);
	    }
	    break;
	}

	if (ch == '\n') {
	    if (!inquote) {
		if (escape) {
		    escape = 0;
		    loffset--;	/* Consume escape in buffer */
		    continue;
		}
		/* Reached end of line so exit without passing on LF */
		break;
	    }
	}

	if (ch == '\\') {
	    escape = !escape;
	} else {
	    if (ch == '"') {
		if (!escape)
		    inquote = !inquote;
	    }
	    escape = 0;
	}

	if ((loffset + 1) >= line_size) {
	    char *tmpline;

	    /*
	     * Reallocate input line.
	     * g_malloc() never return NULL pointer.
	     */
	    tmpline = g_malloc(line_size + AGETS_LINE_INCR);
	    memcpy(tmpline, line, line_size);
	    amfree(line);
	    line = tmpline;
	    line_size = line_size + AGETS_LINE_INCR;
	}
	line[loffset++] = (char)ch;
    }

    if ((ch == EOF) && (loffset == 0)) {
	amfree(line); /* amfree zeros line... */
    } else {
	line[loffset] = '\0';
    }

    /*
     * Return what we got even if there was not a newline.
     * Only report done (NULL) when no data was processed.
     */
    return line;
}


char *
debug_pgets(
    const char *sourcefile,
    int		lineno,
    FILE *	stream)
{
    char *line = g_malloc(AGETS_LINE_INCR);
    char *l;
    char *cline;
    char *untainted_line;
    char *ul;
    size_t line_size = AGETS_LINE_INCR;
    size_t loffset = 0;

    (void)sourcefile;	/* Quiet unused parameter warning if not debugging */
    (void)lineno;	/* Quiet unused parameter warning if not debugging */
    line[0] = '\0';

    cline = fgets(line, line_size, stream);
    if (!cline) {
	g_free(line);
	return cline;
    }
    loffset = strlen(line);
    while (cline && loffset == line_size-1 && line[loffset-1] != '\n') {
	char *tmpline;
	char *pline;

	line_size *= 2;
	tmpline = g_malloc(line_size);
	memcpy(tmpline, line, loffset+1);
	amfree(line);
	line = tmpline;

	pline = line + loffset;
	cline = fgets(pline, line_size-loffset, stream);
	loffset += strlen(pline);
    }

    if (line[loffset-1] == '\n')
	line[loffset-1] = '\0';

    untainted_line = ul = g_malloc(loffset+1);
    for (l = line; *l != '\0'; l++) {
	*ul++ = *l;
    }
    *ul = '\0';
    g_free(line);

    return untainted_line;
}


/*
 *=====================================================================
 * Find/create a buffer for a particular file descriptor for use with
 * areads().
 *
 * void areads_getbuf (const char *file, size_t line, int fd)
 *
 * entry:	file, line = caller source location
 *		fd = file descriptor to look up
 * exit:	returns a pointer to the buffer, possibly new
 *=====================================================================
 */

GMutex *file_mutex = NULL;
static struct areads_buffer {
    char *buffer;
    char *endptr;
    size_t bufsize;
} **areads_buffer = NULL;
static int areads_bufcount = 0;
static size_t areads_bufsize = BUFSIZ;		/* for the test program */

static struct areads_buffer *
areads_getbuf(
    const char *s G_GNUC_UNUSED,
    int		l G_GNUC_UNUSED,
    int		fd)
{
    struct areads_buffer *ptr;

    assert(fd >= 0);

    g_mutex_lock(file_mutex);
    if (fd >= areads_bufcount) {
	struct areads_buffer **new;
	int afd = 30;
	int i;

	if (afd < fd * 2)
	    afd = fd * 2;
	new = g_new0(struct areads_buffer *, (size_t)afd);
	if (areads_buffer) {
	    size_t size = areads_bufcount * sizeof(*areads_buffer);
	    memcpy(new, areads_buffer, size);
	}
	for (i = areads_bufcount; i < afd; i++){
	    new[i] = g_new0(struct areads_buffer, 1);
	}
	amfree(areads_buffer);
	areads_buffer = new;
	areads_bufcount = afd;
    }
    ptr = areads_buffer[fd];
    g_mutex_unlock(file_mutex);
    if (ptr->buffer == NULL) {
	ptr->bufsize = areads_bufsize;
	ptr->buffer = g_malloc(ptr->bufsize + 1);
	ptr->buffer[0] = '\0';
	ptr->endptr = ptr->buffer;
    }
    return ptr;
}

/*
 *=====================================================================
 * Return the amount of data still in an areads buffer.
 *
 * ssize_t areads_dataready (int fd)
 *
 * entry:	fd = file descriptor to release buffer for
 * exit:	returns number of bytes of data ready to process
 *=====================================================================
 */

ssize_t
areads_dataready(
    int	fd)
{
    ssize_t r = 0;
    SELECT_ARG_TYPE ready;
    struct timeval  to;
    int             nfound;

    if (fd < 0)
	return 0;

    g_mutex_lock(file_mutex);

    if (fd >= 0 && fd < areads_bufcount && areads_buffer[fd]->buffer != NULL) {
	r = (ssize_t) (areads_buffer[fd]->endptr - areads_buffer[fd]->buffer);
    }
    g_mutex_unlock(file_mutex);
    if (r) {
        return r;
    }

    FD_ZERO(&ready);
    FD_SET(fd, &ready);
    to.tv_sec = 0;
    to.tv_usec = 0;

    nfound = select(fd+1, &ready, NULL, NULL, &to);
    if (nfound > 0 && FD_ISSET(fd, &ready)) {
        return 1;
    } else {
	return 0;
    }

}

/*
 *=====================================================================
 * Release a buffer for a particular file descriptor used by areads().
 *
 * void areads_relbuf (int fd)
 *
 * entry:	fd = file descriptor to release buffer for
 * exit:	none
 *=====================================================================
 */

void
areads_relbuf(
    int fd)
{
    g_mutex_lock(file_mutex);
    if(fd >= 0 && fd < areads_bufcount) {
	amfree(areads_buffer[fd]->buffer);
	areads_buffer[fd]->endptr = NULL;
	areads_buffer[fd]->bufsize = 0;
    }
    g_mutex_unlock(file_mutex);
}

/*
 *=====================================================================
 * Get the next line of input from a file descriptor.
 *
 * char *areads (int fd)
 *
 * entry:	fd = file descriptor to read
 * exit:	returns a pointer to an g_malloc'd string or NULL at EOF
 *		or error (errno will be zero on EOF).
 *
 * Notes:	the newline, if read, is removed from the string
 *		the caller is responsible for free'ing the string
 *=====================================================================
 */

char *
debug_areads (
    const char *s,
    int		l,
    int		fd)
{
    char *nl;
    char *line;
    char *buffer;
    char *endptr;
    char *newbuf;
    size_t buflen;
    size_t size;
    ssize_t r;
    struct areads_buffer *ptr;

    if(fd < 0) {
	errno = EBADF;
	return NULL;
    }
    ptr = areads_getbuf(s, l, fd);

    buffer = ptr->buffer;
    endptr = ptr->endptr;
    buflen = ptr->bufsize - (size_t)(endptr - buffer);
    while((nl = strchr(buffer, '\n')) == NULL) {
	/*
	 * No newline yet, so get more data.
	 */
	if (buflen == 0) {
	    if ((size = ptr->bufsize) < 256 * areads_bufsize) {
		size *= 2;
	    } else {
		size += 256 * areads_bufsize;
	    }
	    newbuf = g_malloc(size + 1);
	    memcpy (newbuf, buffer, ptr->bufsize + 1);
	    amfree(ptr->buffer);
	    ptr->buffer = newbuf;
	    ptr->endptr = newbuf + ptr->bufsize;
	    ptr->bufsize = size;
	    buffer = ptr->buffer;
	    endptr = ptr->endptr;
	    buflen = ptr->bufsize - (size_t)(endptr - buffer);
	}
	if ((r = read(fd, endptr, buflen)) <= 0) {
	    if(r == 0) {
		if (buffer == endptr || *(endptr-1) == '\n') {
		    errno = 0;		/* flag EOF instead of error */
		    return NULL;
		}
		*endptr = '\n';
		r = 1;
	    } else {
		return NULL;
	    }
	} else {
	    endptr[r] = '\0';		/* we always leave room for this */
	    endptr += r;
	    buflen -= r;
	}
    }
    *nl++ = '\0';
    line = g_strdup(buffer);
    size = (size_t)(endptr - nl);	/* data still left in buffer */
    memmove(buffer, nl, size);
    ptr->endptr = buffer + size;
    ptr->endptr[0] = '\0';
    return line;
}

int robust_open(const char * pathname, int flags, mode_t mode) {
    int result = -1;
    int e_busy_count = 0;

    for (;;) {
        if (flags & O_CREAT) {
            result = open(pathname, flags, mode);
        } else {
            result = open(pathname, flags);
        }

        if (result < 0) {
#ifdef EBUSY
            /* EBUSY is a tricky one; sometimes it is synonymous with
               EINTR, but sometimes it means the device is open
               elsewhere (e.g., with a tape drive on Linux). We take
               the middle path and retry, but with limited
               patience. */
            if (errno == EBUSY && e_busy_count < 10) {
                e_busy_count ++;
                continue;
            } else
#endif
            if (0
                /* Always retry on EINTR; if the caller did
                   not specify non-blocking mode, then also retry on
                   EAGAIN or EWOULDBLOCK. */
#ifdef EINTR
                || errno == EINTR
#endif
                || ( 1
#ifdef O_NONBLOCK
                  && !(flags & O_NONBLOCK)
#endif
                  && ( 0
#ifdef EAGAIN
                       || errno == EAGAIN
#endif
#ifdef EWOULDBLOCK
                       || errno == EWOULDBLOCK
#endif
                       ) ) ) {
                /* Try again */
                continue;
            } else {
                /* Failure. */
                return result;
            }
        } else {
            break;
        }
    }

#ifdef F_SETFD
    if (result >= 0) {
        (void)fcntl(result, F_SETFD, 1); /* Throw away result. */
    }
#endif

    return result;
}

int robust_close(int fd) {
    for (;;) {
        int result;

        result = close(fd);
        if (result != 0 && (0
#ifdef EINTR
                            || errno == EINTR
#endif
#ifdef EBUSY
                            || errno == EBUSY
#endif
#ifdef EAGAIN
                            || errno == EAGAIN
#endif
#ifdef EWOULDBLOCK
                            || errno == EWOULDBLOCK
#endif
                            )) {
            continue;
        } else {
            return result;
        }
    }
}

uid_t
get_client_uid(void)
{
    static uid_t client_uid = (uid_t) -1;
    struct passwd      *pwent;

    if(client_uid == (uid_t) -1 && (pwent = getpwnam(CLIENT_LOGIN)) != NULL) {
	client_uid = pwent->pw_uid;
	endpwent();
    }

    return client_uid;
}

gid_t
get_client_gid(void)
{
    static gid_t client_gid = (gid_t) -1;
    struct passwd      *pwent;

    if(client_gid == (gid_t) -1 && (pwent = getpwnam(CLIENT_LOGIN)) != NULL) {
	client_gid = pwent->pw_gid;
	endpwent();
    }

    return client_gid;
}

char *
get_original_cwd(void)
{
    if (original_cwd == NULL) {
	original_cwd = g_get_current_dir();
    }

    return original_cwd;
}

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

gsize read_fully(int fd, void *buf, gsize count, int *err)
{
    gsize ret = full_read(fd, buf, count);

    if (err)
        *err = (ret == count) ? 0 : errno;

    return ret;
}

char *
untaint_fgets(
    char *s,
    int size,
    FILE *stream)
{
    char *untainted_line = malloc(size);
    char *line = fgets(untainted_line, size, stream);
    char *s1 = s;

    if (!line) {
	g_free(untainted_line);
	return NULL;
    }

    while (*line != '\0') {
	if ((unsigned int)*line <= 255)
	    *s1 = *line;
	else
	    *s1 = '\0';
	line++;
	s1++;
    }
    *s1 = '\0';
    g_free(untainted_line);
    return s;
}


#ifdef TEST

int
main(
    int		argc,
    char **	argv)
{
	int rc;
	int fd;
	char *name;
	char *top;
	char *file;
	char *line;

	glib_init();

	/*
	 * Configure program for internationalization:
	 *   1) Only set the message locale for now.
	 *   2) Set textdomain for all amanda related programs to "amanda"
	 *      We don't want to be forced to support dozens of message catalogs
	 */  
	setlocale(LC_MESSAGES, "C");
	textdomain("amanda"); 

	safe_fd(-1, 0);

	set_pname("file test");

	dbopen(NULL);

	/* Don't die when child closes pipe */
	signal(SIGPIPE, SIG_IGN);

	name = "/tmp/a/b/c/d/e";
	if (argc > 2 && argv[1][0] != '\0') {
		name = argv[1];
	}
	top = "/tmp";
	if (argc > 3 && argv[2][0] != '\0') {
		name = argv[2];
	}
	file = "/etc/hosts";
	if (argc > 4 && argv[3][0] != '\0') {
		name = argv[3];
	}

	g_fprintf(stderr, _("Create parent directories of %s ..."), name);
	rc = mkpdir(name, (mode_t)02777, (uid_t)-1, (gid_t)-1);
	if (rc == 0)
		g_fprintf(stderr, " done\n");
	else {
		perror(_("failed"));
		return rc;
	}

	g_fprintf(stderr, _("Delete %s back to %s ..."), name, top);
	rc = rmpdir(name, top);
	if (rc == 0)
		g_fprintf(stderr, _(" done\n"));
	else {
		perror(_("failed"));
		return rc;
	}

	g_fprintf(stderr, _("areads dump of %s ..."), file);
	if ((fd = open (file, 0)) < 0) {
		perror(file);
		return 1;
	}
	areads_bufsize = 1;			/* force buffer overflow */
	while ((line = areads(fd)) != NULL) {
		puts(line);
		amfree(line);
	}
	aclose(fd);
	g_fprintf(stderr, _(" done.\n"));

	g_fprintf(stderr, _("Finished.\n"));

	dbclose();
	return 0;
}

#endif

