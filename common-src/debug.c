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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: debug.c,v 1.40 2006/07/26 11:49:32 martinea Exp $
 *
 * Logging support
 */

#include "amanda.h"
#include "util.h"
#include "arglist.h"
#include "clock.h"
#include "timestamp.h"
#include "conffile.h"

#ifdef HAVE_GLIBC_BACKTRACE
#include <execinfo.h>
#endif

/* Minimum file descriptor on which to keep the debug file.  This is intended
 * to keep the descriptor "out of the way" of other processing.  It's not clear
 * that this is required any longer, but it doesn't hurt anything.
 */
#define	MIN_DB_FD			10

/* information on the current debug file */
static int db_fd = 2;			/* file descriptor (default stderr) */
static FILE *db_file = NULL;		/* stdio stream */
static char *db_name  = NULL;		/* unqualified filename */
static char *db_filename = NULL;	/* fully qualified pathname */

/* directory containing debug file, including trailing slash */
static char *dbgdir = NULL;

/* time debug log was opened (timestamp of the file) */
static time_t open_time;

/* storage for global variables */
int error_exit_status = 1;

/* static function prototypes */
static char *get_debug_name(time_t t, int n);
static void debug_setup_1(char *config, char *subdir);
static void debug_setup_2(char *s, int fd, char *annotation);
static char *msg_timestamp(void);

static void debug_logging_handler(const gchar *log_domain,
	GLogLevelFlags log_level,
	const gchar *message,
	gpointer user_data);
static void debug_setup_logging(void);

/* By default, do not suppress tracebacks */
static gboolean do_suppress_error_traceback = FALSE;

/* configured amanda_log_handlers */
static GSList *amanda_log_handlers = NULL;

/*
 * Generate a debug file name.  The name is based on the program name,
 * followed by a timestamp, an optional sequence number, and ".debug".
 *
 * @param t: timestamp
 * @param n: sequence number between 1 and 1000; if zero, no sequence number
 * is included.
 */
static char *
get_debug_name(
    time_t	t,
    int		n)
{
    char number[NUM_STR_SIZE];
    char *ts;
    char *result;

    if(n < 0 || n > 1000) {
	return NULL;
    }
    ts = get_timestamp_from_time(t);
    if(n == 0) {
	number[0] = '\0';
    } else {
	g_snprintf(number, SIZEOF(number), "%03d", n - 1);
    }
    result = vstralloc(get_pname(), ".", ts, number, ".debug", NULL);
    amfree(ts);
    return result;
}

/* Call this to suppress tracebacks on error() or g_critical().  This is used
 * when a critical error is indicated in perl, and the traceback will not be
 * useful. */
void
suppress_error_traceback(void)
{
    do_suppress_error_traceback = 1;
}

/* A GLogFunc to handle g_log calls.  This function assumes that user_data
 * is either NULL or a pointer to one of the debug_* configuration variables
 * in conffile.c, indicating whether logging for this log domain is enabled.
 *
 * @param log_domain: the log domain, or NULL for general logging
 * @param log_level: level, fatality, and recursion flags
 * @param message: the message to log
 * @param user_pointer: unused
 */
static void
debug_logging_handler(const gchar *log_domain G_GNUC_UNUSED,
	    GLogLevelFlags log_level,
	    const gchar *message,
	    gpointer user_data G_GNUC_UNUSED)
{
    GLogLevelFlags maxlevel;
    char *levprefix = NULL;
    pcontext_t context = get_pcontext();

    /* glib allows a message to have multiple levels, so calculate the "worst"
     * level */
    if (log_level & G_LOG_LEVEL_ERROR) {
	maxlevel = G_LOG_LEVEL_ERROR;
	levprefix = _("error (fatal): ");
    } else if (log_level & G_LOG_LEVEL_CRITICAL) {
	maxlevel = G_LOG_LEVEL_CRITICAL;
	levprefix = _("critical (fatal): ");
    } else if (log_level & G_LOG_LEVEL_WARNING) {
	maxlevel = G_LOG_LEVEL_WARNING;
	levprefix = _("warning: ");
    } else if (log_level & G_LOG_LEVEL_MESSAGE) {
	maxlevel = G_LOG_LEVEL_MESSAGE;
	levprefix = _("message: ");
    } else if (log_level & G_LOG_LEVEL_INFO) {
	maxlevel = G_LOG_LEVEL_INFO;
	levprefix = _("info: ");
    } else {
	maxlevel = G_LOG_LEVEL_DEBUG;
	levprefix = ""; /* no level displayed for debugging */
    }

    /* scriptutil context doesn't do any logging except for critical
     * and error levels */
    if (context != CONTEXT_SCRIPTUTIL) {
	/* convert the highest level to a string and dbprintf it */
	debug_printf("%s%s\n", levprefix, message);
    }

    if (amanda_log_handlers) {
	GSList *iter = amanda_log_handlers;
	while (iter) {
	    amanda_log_handler_t *hdlr = (amanda_log_handler_t *)iter->data;
	    hdlr(maxlevel, message);
	    iter = g_slist_next(iter);
	}
    } else {
	/* call the appropriate handlers, based on the context */
	amanda_log_stderr(maxlevel, message);
	if (context == CONTEXT_DAEMON)
	    amanda_log_syslog(maxlevel, message);
    }

    /* error and critical levels have special handling */
    if (log_level & (G_LOG_LEVEL_ERROR|G_LOG_LEVEL_CRITICAL)) {
#ifdef HAVE_GLIBC_BACKTRACE
	/* try logging a traceback to the debug log */
	if (!do_suppress_error_traceback && db_fd != -1) {
	    void *stack[32];
	    int naddrs;
	    naddrs = backtrace(stack, sizeof(stack)/sizeof(*stack));
	    backtrace_symbols_fd(stack, naddrs, db_fd);
	}
#endif

	/* we're done */
	if (log_level & G_LOG_LEVEL_CRITICAL)
	    exit(error_exit_status);
	else
	    abort();
	g_assert_not_reached();
    }
}

/* Install our handler into the glib log handling system.
 */
static void
debug_setup_logging(void)
{
    /* g_error and g_critical should be fatal, although the log handler
     * takes care of this anyway */
    g_log_set_always_fatal(G_LOG_LEVEL_ERROR |  G_LOG_LEVEL_CRITICAL);

    /* set up handler (g_log_set_default_handler is new in glib-2.6, and
     * hence not useable here) */
    g_log_set_handler(NULL, G_LOG_LEVEL_MASK | G_LOG_FLAG_FATAL | G_LOG_FLAG_RECURSION,
		      debug_logging_handler, NULL);
}

void
add_amanda_log_handler(amanda_log_handler_t *hdlr)
{
    amanda_log_handlers = g_slist_append(amanda_log_handlers, hdlr);
}

void
amanda_log_syslog(GLogLevelFlags log_level, const gchar *message)
{
    int priority = LOG_ERR;
    switch (log_level) {
	case G_LOG_LEVEL_ERROR:
	case G_LOG_LEVEL_CRITICAL:
	    priority = LOG_ERR;
	    break;

	case G_LOG_LEVEL_WARNING:
#ifdef LOG_WARNING
	    priority = LOG_WARNING;
#endif
	    break;

	default:
	    return;
    }

#ifdef LOG_DAEMON
    openlog(get_pname(), LOG_PID, LOG_DAEMON);
#else
    openlog(get_pname(), LOG_PID, 0);
#endif
    syslog(priority, "%s", message);
    closelog();

}

void
amanda_log_stderr(GLogLevelFlags log_level, const gchar *message)
{
    switch (log_level) {
	case G_LOG_LEVEL_ERROR:
	case G_LOG_LEVEL_CRITICAL:
	    g_fprintf(stderr, "%s: %s\n", get_pname(), message);
	    break;

	default:
	    return;
    }
}

void
amanda_log_null(GLogLevelFlags log_level G_GNUC_UNUSED, const gchar *message G_GNUC_UNUSED)
{
}

/* Set the global dbgdir according to 'config' and 'subdir', and clean
 * old debug files out of that directory
 *
 * The global open_time is set to the current time, and used to delete
 * old files.
 *
 * @param config: configuration or NULL
 * @param subdir: subdirectory (server, client, etc.) or NULL
 */
static void
debug_setup_1(char *config, char *subdir)
{
    char *pname;
    size_t pname_len;
    char *e = NULL;
    char *s = NULL;
    DIR *d;
    struct dirent *entry;
    int do_rename;
    char *test_name;
    size_t test_name_len;
    size_t d_name_len;
    struct stat sbuf;
    char *dbfilename = NULL;
    char *sane_config = NULL;
    int i;

    memset(&sbuf, 0, SIZEOF(sbuf));

    pname = get_pname();
    pname_len = strlen(pname);

    /*
     * Create the debug directory if it does not yet exist.
     */
    amfree(dbgdir);
    if (config)
	sane_config = sanitise_filename(config);
    if (sane_config && subdir)
	dbgdir = vstralloc(AMANDA_DBGDIR, "/", subdir, "/", sane_config,
			   "/", NULL);
    else if (sane_config)
	dbgdir = vstralloc(AMANDA_DBGDIR, "/", sane_config, "/", NULL);
    else if (subdir)
	dbgdir = vstralloc(AMANDA_DBGDIR, "/", subdir, "/", NULL);
    else
	dbgdir = stralloc2(AMANDA_DBGDIR, "/");
    if(mkpdir(dbgdir, 0700, get_client_uid(), get_client_gid()) == -1) {
	error(_("create debug directory \"%s\": %s"),
	      dbgdir, strerror(errno));
	/*NOTREACHED*/
    }
    amfree(sane_config);

    /*
     * Clean out old debug files.  We also rename files with old style
     * names (XXX.debug or XXX.$PID.debug) into the new name format.
     * We assume no system has 17 digit PID-s :-) and that there will
     * not be a conflict between an old and new name.
     */
    if((d = opendir(dbgdir)) == NULL) {
	error(_("open debug directory \"%s\": %s"),
	      dbgdir, strerror(errno));
	/*NOTREACHED*/
    }
    time(&open_time);
    test_name = get_debug_name(open_time - (AMANDA_DEBUG_DAYS * 24 * 60 * 60), 0);
    test_name_len = strlen(test_name);
    while((entry = readdir(d)) != NULL) {
	if(is_dot_or_dotdot(entry->d_name)) {
	    continue;
	}
	d_name_len = strlen(entry->d_name);
	if(strncmp(entry->d_name, pname, pname_len) != 0
	   || entry->d_name[pname_len] != '.'
	   || d_name_len < 6
	   || strcmp(entry->d_name + d_name_len - 6, ".debug") != 0) {
	    continue;				/* not one of our debug files */
	}
	e = newvstralloc(e, dbgdir, entry->d_name, NULL);
	if(d_name_len < test_name_len) {
	    /*
	     * Create a "pretend" name based on the last modification
	     * time.  This name will be used to decide if the real name
	     * should be removed.  If not, it will be used to rename the
	     * real name.
	     */
	    if(stat(e, &sbuf) != 0) {
		continue;			/* ignore errors */
	    }
	    amfree(dbfilename);
	    dbfilename = get_debug_name((time_t)sbuf.st_mtime, 0);
	    do_rename = 1;
	} else {
	    dbfilename = newstralloc(dbfilename, entry->d_name);
	    do_rename = 0;
	}
	if(strcmp(dbfilename, test_name) < 0) {
	    (void) unlink(e);			/* get rid of old file */
	    continue;
	}
	if(do_rename) {
	    i = 0;
	    while(dbfilename != NULL
		  && (s = newvstralloc(s, dbgdir, dbfilename, NULL)) != NULL
		  && rename(e, s) != 0 && errno != ENOENT) {
		amfree(dbfilename);
		dbfilename = get_debug_name((time_t)sbuf.st_mtime, ++i);
	    }
	    if(dbfilename == NULL) {
		error(_("cannot rename old debug file \"%s\""), entry->d_name);
		/*NOTREACHED*/
	    }
	}
    }
    amfree(dbfilename);
    amfree(e);
    amfree(s);
    amfree(test_name);
    closedir(d);
}

/* Given an already-opened debug file, set the file's ownership
 * appropriately, move its file descriptor above MIN_DB_FD, and
 * add an initial log entry to the file.
 *
 * This function records the file's identity in the globals
 * db_filename, db_fd, and db_file.  It does *not* set db_name.
 * db_file is not set if fd is -1
 *
 * This function uses the global 'open_time', which is set by
 * debug_setup_1.
 *
 * @param s: the filename of the debug file; string should be malloc'd,
 * and should *not* be freed by the caller.
 * @param fd: the descriptor connected to the debug file, or -1 if
 * no decriptor moving should take place.
 * @param annotation: an extra string to include in the initial
 * log entry.
 */
static void
debug_setup_2(
    char *	s,
    int		fd,
    char *	annotation)
{
    int i;
    int fd_close[MIN_DB_FD+1];

    amfree(db_filename);
    db_filename = s;
    s = NULL;

    /* If we're root, change the ownership of the debug files.  If we're not root,
     * this would either be redundant or an error. */
    if (geteuid() == 0) {
	if (chown(db_filename, get_client_uid(), get_client_gid()) < 0) {
	    dbprintf(_("chown(%s, %d, %d) failed: %s"),
		     db_filename, (int)get_client_uid(), (int)get_client_gid(), strerror(errno));
	}
    }
    amfree(dbgdir);
    /*
     * Move the file descriptor up high so it stays out of the way
     * of other processing, e.g. sendbackup.
     */
    if (fd >= 0) {
	i = 0;
	fd_close[i++] = fd;
	while((db_fd = dup(fd)) < MIN_DB_FD) {
	    fd_close[i++] = db_fd;
	}
	while(--i >= 0) {
	    close(fd_close[i]);
	}
	db_file = fdopen(db_fd, "a");
    }

    if (annotation) {
	/*
	 * Make the first debug log file entry.
	 */
	debug_printf(_("pid %ld ruid %ld euid %ld version %s: %s at %s"),
		     (long)getpid(),
		     (long)getuid(), (long)geteuid(),
		     VERSION,
		     annotation,
		     ctime(&open_time));
    }
}

/* Get current GMT time and return a message timestamp.
 * Used for g_printf calls to logs and such.  The return value
 * is to a static buffer, so it should be used immediately.
 *
 * @returns: timestamp
 */
static char *
msg_timestamp(void)
{
    static char timestamp[128];
    struct timeval tv;

    gettimeofday(&tv, NULL);
    g_snprintf(timestamp, SIZEOF(timestamp), "%lld.%06ld",
		(long long)tv.tv_sec, (long)tv.tv_usec);

    return timestamp;
}

/*
 * ---- public functions
 */

void
debug_init(void)
{
    debug_setup_logging();

    /* the scriptutil context does not create a debug log, since such
     * processes are invoked many times.
     */
    if (get_pcontext() != CONTEXT_SCRIPTUTIL) {
	debug_open(get_ptype());
    }
}

void
debug_open(char *subdir)
{
    int fd = -1;
    int i;
    char *s = NULL;
    mode_t mask;

    /* set up logging while we're here */
    debug_setup_logging();

    /* set 'dbgdir' and clean out old debug files */
    debug_setup_1(NULL, subdir);

    /*
     * Create the new file with a unique sequence number.
     */
    mask = (mode_t)umask((mode_t)0037); /* Allow the group read bit through */

    /* iteratate through sequence numbers until we find one that
     * is not already in use */
    for(i = 0; fd < 0; i++) {
	amfree(db_name);
	if ((db_name = get_debug_name(open_time, i)) == NULL) {
	    error(_("Cannot create debug file name in %d tries."), i);
	    /*NOTREACHED*/
	}

	if ((s = newvstralloc(s, dbgdir, db_name, NULL)) == NULL) {
	    error(_("Cannot allocate debug file name memory"));
	    /*NOTREACHED*/
	}

	if ((fd = open(s, O_WRONLY|O_CREAT|O_EXCL|O_APPEND, 0640)) < 0) {
	    if (errno != EEXIST) {
	        error(_("Cannot create debug file \"%s\": %s"),
			s, strerror(errno));
	        /*NOTREACHED*/
	    }
	    amfree(s);
	}
    }
    (void)umask(mask); /* Restore mask */

    /*
     * Finish setup.
     *
     * Note: we release control of the string 's' points to.
     */
    debug_setup_2(s, fd, "start");
}

void
debug_reopen(
    char *	dbfilename,
    char *	annotation)
{
    char *s = NULL;
    int fd;

    if (dbfilename == NULL) {
	return;
    }

    /* set 'dbgdir' and clean out old debug files */
    debug_setup_1(NULL, NULL);

    /*
     * Reopen the file.
     */
    if (*dbfilename == '/') {
	s = stralloc(dbfilename);
    } else {
	s = newvstralloc(s, dbgdir, dbfilename, NULL);
    }
    if ((fd = open(s, O_RDWR|O_APPEND)) < 0) {
	error(_("cannot reopen debug file %s"), dbfilename);
	/*NOTREACHED*/
    }

    /*
     * Finish setup.
     *
     * Note: we release control of the string 's' points to.
     */
    debug_setup_2(s, fd, annotation);
}

void
debug_rename(
    char *config,
    char *subdir)
{
    int fd = -1;
    int i;
    char *s = NULL;
    mode_t mask;

    if (!db_filename)
	return;

    /* set 'dbgdir' and clean out old debug files */
    debug_setup_1(config, subdir);

    s = newvstralloc(s, dbgdir, db_name, NULL);

    if (strcmp(db_filename, s) == 0) {
	amfree(s);
	return;
    }

    mask = (mode_t)umask((mode_t)0037);

#if defined(__CYGWIN__)
    /*
     * On cygwin, rename will not overwrite an existing file nor
     * will it rename a file that is open for writing...
     *
     * Rename file directly.  Expect failure if file already exists
     * or is open by another user.
     */

    i = 0;
    while (rename(db_filename, s) < 0) {
	if (errno != EEXIST) {
	    /*
	     * If the failure was not due to the target file name already
	     * existing then we have bigger issues at hand so we keep 
	     * the existing file.
	     */
	    dbprintf(_("Cannot rename \"%s\" to \"%s\": %s\n"),
		     db_filename, s, strerror(errno));
	    s = newvstralloc(s, db_filename, NULL);
	    i = -1;
	    break;
	}

	/*
	 * Files already exists:
	 * Continue searching for a unique file name that will work.
	 */
	amfree(db_name);
	if ((db_name = get_debug_name(open_time, i++)) == NULL) {
	    dbprintf(_("Cannot create unique debug file name"));
	    break;
	}
	s = newvstralloc(s, dbgdir, db_name, NULL);
    }
    if (i >= 0) {
	/*
	 * We need to close and reopen the original file handle to
	 * release control of the original debug file name.
	 */
	if ((fd = open(s, O_WRONLY|O_APPEND, 0640)) >= 0) {
	    /*
	     * We can safely close the the original log file
	     * since we now have a new working handle.
	     */
	    db_fd = 2;
	    fclose(db_file);
	    db_file = NULL;
	}
    }
#else
    /* check if a file with the same name already exists. */
    if ((fd = open(s, O_WRONLY|O_CREAT|O_EXCL|O_APPEND, 0640)) < 0) {
	for(i = 0; fd < 0; i++) {
	    amfree(db_name);
	    if ((db_name = get_debug_name(open_time, i)) == NULL) {
		dbprintf(_("Cannot create debug file"));
		break;
	    }

	    s = newvstralloc(s, dbgdir, db_name, NULL);
	    if ((fd = open(s, O_WRONLY|O_CREAT|O_EXCL|O_APPEND, 0640)) < 0) {
		if (errno != EEXIST) {
		    dbprintf(_("Cannot create debug file: %s"),
			      strerror(errno));
		    break;
		}
	    }
	}
    }

    if (fd >= 0) {
	close(fd);
	if (rename(db_filename, s) == -1) {
	    dbprintf(_("Cannot rename \"%s\" to \"%s\": %s\n"),
		     db_filename, s, strerror(errno));
	}
	fd = -1;
    }
#endif

    (void)umask(mask); /* Restore mask */
    /*
     * Finish setup.
     *
     * Note: we release control of the string 's' points to.
     */
    debug_setup_2(s, fd, "rename");
}

void
debug_close(void)
{
    time_t curtime;

    time(&curtime);
    debug_printf(_("pid %ld finish time %s"), (long)getpid(), ctime(&curtime));

    if(db_file && fclose(db_file) == EOF) {
	int save_errno = errno;

	db_file = NULL;				/* prevent recursion */
	g_fprintf(stderr, _("close debug file: %s"), strerror(save_errno));
	/*NOTREACHED*/
    }
    db_fd = 2;
    db_file = NULL;
    amfree(db_filename);
    amfree(db_name);
}

/*
 * Format and write a debug message to the process debug file.
 */
printf_arglist_function(void debug_printf, const char *, format)
{
    va_list argp;
    int save_errno;

    /*
     * It is common in the code to call dbprintf to write out
     * syserrno(errno) and then turn around and try to do something else
     * with errno (e.g. g_printf() or log()), so we make sure errno goes
     * back out with the same value it came in with.
     */

    save_errno = errno;

    /* handle the default (stderr) if debug_open hasn't been called yet */
    if(db_file == NULL && db_fd == 2) {
	db_file = stderr;
    }
    if(db_file != NULL) {
	char *prefix;
	char *text;

	if (db_file != stderr)
	    prefix = g_strdup_printf("%s: %s:", msg_timestamp(), get_pname());
	else 
	    prefix = g_strdup_printf("%s:", get_pname());
	arglist_start(argp, format);
	text = g_strdup_vprintf(format, argp);
	arglist_end(argp);
	fprintf(db_file, "%s %s", prefix, text);
	amfree(prefix);
	amfree(text);
	fflush(db_file);
    }
    errno = save_errno;
}

int
debug_fd(void)
{
    return db_fd;
}

FILE *
debug_fp(void)
{
    return db_file;
}

char *
debug_fn(void)
{
    return db_filename;
}

void
debug_dup_stderr_to_debug(void)
{
    if(db_fd != -1 && db_fd != STDERR_FILENO)
    {
       if(dup2(db_fd, STDERR_FILENO) != STDERR_FILENO)
       {
	   error(_("can't redirect stderr to the debug file: %d, %s"), db_fd, strerror(errno));
	   g_assert_not_reached();
       }
    }
}

