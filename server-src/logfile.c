/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * $Id: logfile.c,v 1.31 2006/06/01 14:54:39 martinea Exp $
 *
 * common log file writing routine
 */
#include "amanda.h"
#include "amutil.h"
#include "conffile.h"

#include "logfile.h"

char *logtype_str[] = {
    "BOGUS",
    "FATAL",		/* program died for some reason, used by error() */
    "ERROR", "WARNING",	"INFO", "SUMMARY",	 /* information messages */
    "START", "FINISH",				   /* start/end of a run */
    "DISK",							 /* disk */
    /* the end of a dump */
    "DONE", "PART", "PARTPARTIAL", "SUCCESS", "PARTIAL", "FAIL", "STRANGE",
    "CHUNK", "CHUNKSUCCESS",                            /* ... continued */
    "STATS",						   /* statistics */
    "CONT",				   /* continuation line; special */
    "RETRY",
    "MARKER",					  /* marker for reporter */
};

char *program_str[] = {
    "UNKNOWN", "planner", "driver", "amreport", "dumper", "chunker",
    "taper", "amflush", "amdump", "amidxtaped", "amfetchdump", "amcheckdump",
    "amvault", "amcleanup", "ambackupd", "amtrmidx", "amtrmlog",
};

int curlinenum;
logtype_t curlog;
program_t curprog;
char *curstr;

int multiline = -1;
static char *logfile;
static int logfd = -1;

 /*
  * Note that technically we could use two locks, a read lock
  * from 0-EOF and a write-lock from EOF-EOF, thus leaving the
  * beginning of the file open for read-only access.  Doing so
  * would open us up to some race conditions unless we're pretty
  * careful, and on top of that the functions here are so far
  * the only accesses to the logfile, so keep things simple.
  */

/* local functions */
static void open_log(void);
static void close_log(void);

void
amanda_log_trace_log(
    GLogLevelFlags log_level,
    const gchar *message)
{
    logtype_t logtype = L_ERROR;

    switch (log_level) {
	case G_LOG_LEVEL_ERROR:
	case G_LOG_LEVEL_CRITICAL:
	    logtype = L_FATAL;
	    break;

	default:
	    return;
    }
    log_add(logtype, "%s", message);
}

static void log_add_full_v(logtype_t typ, char *pname, char *format, va_list argp)
{
    char *leader = NULL;
    char *xlated_fmt = gettext(format);
    char linebuf[STR_SIZE];
    size_t n;
    static gboolean in_log_add = 0;

    /* avoid recursion */
    if (in_log_add)
	return;

    /* format error message */

    if((int)typ <= (int)L_BOGUS || (int)typ > (int)L_MARKER) typ = L_BOGUS;

    if(multiline > 0) {
	leader = g_strdup("  ");		/* continuation line */
    } else {
	leader = g_strjoin(NULL, logtype_str[(int)typ], " ", pname, " ", NULL);
    }

    /* use sizeof(linebuf)-2 to save space for a trailing newline */
    g_vsnprintf(linebuf, sizeof(linebuf)-2, xlated_fmt, argp);
						/* -1 to allow for '\n' */

    /* avoid recursive call from error() */

    in_log_add = 1;

    /* append message to the log file */

    if(multiline == -1) open_log();

    if (full_write(logfd, leader, strlen(leader)) < strlen(leader)) {
	error(_("log file write error: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    amfree(leader);

    /* add a newline if necessary */
    n = strlen(linebuf);
    if(n == 0 || linebuf[n-1] != '\n') linebuf[n++] = '\n';
    linebuf[n] = '\0';

    if (full_write(logfd, linebuf, n) < n) {
	error(_("log file write error: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    if(multiline != -1) multiline++;
    else close_log();

    in_log_add = 0;
}

void log_add(logtype_t typ, char *format, ...)
{
    va_list argp;

    arglist_start(argp, format);
    log_add_full_v(typ, get_pname(), format, argp);
    arglist_end(argp);
}

void log_add_full(logtype_t typ, char *pname, char *format, ...)
{
    va_list argp;

    arglist_start(argp, format);
    log_add_full_v(typ, pname, format, argp);
    arglist_end(argp);
}

void
log_start_multiline(void)
{
    assert(multiline == -1);

    multiline = 0;
    open_log();
}


void
log_end_multiline(void)
{
    assert(multiline != -1);
    multiline = -1;
    close_log();
}

char *
make_logname(
    char *process,
    char *datestamp)
{
    char *conf_logdir;
    char *fname = NULL;

    if (datestamp == NULL)
	datestamp = g_strdup("error-00000000");

    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    fname = g_strjoin(NULL, conf_logdir, "/log", NULL);
    while (1) {
	int fd;
        g_free(logfile);
        logfile = g_strconcat(fname, ".", datestamp, ".0", NULL);
	/* try to create it */
	fd = open(logfile, O_EXCL | O_CREAT | O_WRONLY, 0600);
	if (fd > -1) {
	    FILE *file;
	    file = fdopen(fd, "w");
	    if (file) {
		gchar *text = g_strdup_printf("INFO %s %s pid %ld\n",
				 get_pname(), process, (long)getpid());
		fprintf(file, "%s", text);
		fclose(file);
		file = fopen(logfile, "r");
		if (file) {
		    char line[1000];
		    if (fgets(line, 1000, file)) {
			if (g_str_equal(line, text)) {
			    /* the file is for us */
			    g_free(text);
			    fclose(file);
			    break;
			}
		    }
		    fclose(file);
		}
		g_free(text);
	    }
	}

	/* increase datestamp */
	datestamp[13]++;
	if (datestamp[13] == ':') {
	    datestamp[13] = '0';
	    datestamp[12]++;
	    if (datestamp[12] == '6') {
		datestamp[12] = '0';
		datestamp[11]++;
		if (datestamp[11] == ':') {
		    datestamp[11] = '0';
		    datestamp[10]++;
		    if (datestamp[10] == '6') {
			datestamp[10] = '0';
			datestamp[9]++;
			if (datestamp[9] == ':') {
			    datestamp[9] = '0';
			    datestamp[8]++;
			}
		    }
		}
	    }
	}
    }

    if (strcmp(process, "checkdump") != 0 &&
	strcmp(process, "fetchdump") != 0) {
	char *logf = g_strdup(rindex(logfile,'/')+1);
	unlink(fname);
	if (symlink(logf, fname) == -1) {
	    g_debug("Can't symlink '%s' to '%s': %s", fname, logf,
		    strerror(errno));
	}
	amfree(logf);
    }

    amfree(fname);
    amfree(conf_logdir);

    return (datestamp);
}

char *
get_logname(void)
{
    return g_strdup(logfile);
}

void
set_logname(
    char *filename)
{
    logfile = g_strdup(filename);
}

void
log_rename(
    char *	datestamp)
{
    char *conf_logdir;
    char *logfile;
    char *fname = NULL;
    char seq_str[NUM_STR_SIZE];
    unsigned int seq;
    struct stat statbuf;

    if(datestamp == NULL) datestamp = "error";

    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    logfile = g_strjoin(NULL, conf_logdir, "/log", NULL);

    if (lstat(logfile, &statbuf) == 0 && S_ISLNK(statbuf.st_mode)) {
	g_debug("Remove symbolic link %s", logfile);
	unlink(logfile);
	return;
    }

    for(seq = 0; 1; seq++) {	/* if you've got MAXINT files in your dir... */
	g_snprintf(seq_str, sizeof(seq_str), "%u", seq);
        g_free(fname);
        fname = g_strconcat(logfile, ".", datestamp, ".", seq_str, NULL);
	if(stat(fname, &statbuf) == -1 && errno == ENOENT) break;
    }

    if(rename(logfile, fname) == -1) {
	g_debug(_("could not rename \"%s\" to \"%s\": %s"),
	      logfile, fname, strerror(errno));
    }

    amfree(fname);
    amfree(logfile);
    amfree(conf_logdir);
}


static void
open_log(void)
{
    logfd = open(logfile, O_WRONLY|O_CREAT|O_APPEND, 0600);

    if(logfd == -1) {
	error(_("could not open log file %s: %s"), logfile, strerror(errno));
	/*NOTREACHED*/
    }

    if(amflock(logfd, "log") == -1) {
	error(_("could not lock log file %s: %s"), logfile, strerror(errno));
	/*NOTREACHED*/
    }
}


static void
close_log(void)
{
    if(amfunlock(logfd, "log") == -1) {
	error(_("could not unlock log file %s: %s"), logfile, strerror(errno));
	/*NOTREACHED*/
    }

    if(close(logfd) == -1) {
	error(_("close log file: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    logfd = -1;
}

/* WARNING: Function accesses globals curstr, curlog, and curprog
 * WARNING: Function has static member logline, returned via globals */
int
get_logline(
    FILE *	logf)
{
    static char *logline = NULL;
    static size_t line_size = 0;
    char *lline;
    size_t loffset = 0;
    char *logstr, *progstr;
    char *s;
    int ch;
    int n;

    if (!logline) {
	line_size = 256;
	logline = g_malloc(line_size);
    }

    logline[0] = '\0';
    while(1) {
	lline = fgets(logline + loffset, line_size - loffset, logf);
	if (lline == NULL) {
	    break; /* EOF */
	}
	if (strlen(logline) == line_size -1 &&
		   logline[strlen(logline)-1] != '\n') {
	    line_size *= 2;
	    logline = g_realloc(logline, line_size);
	    loffset = strlen(logline);
	} else if (strlen(logline) == 0 ||
		   (strlen(logline) == 1 && logline[0] == '\n')) {
	} else {
	    break; /* good line */
	}
	logline[loffset] = '\0';
    }
    if (logline[0] == '\0')
	return 0;

    /* remove \n */
    n = strlen(logline);
    if (logline[n-1] == '\n') logline[n-1] = '\0';

    curlinenum++;
    s = logline;
    ch = *s++;

    /* continuation lines are special */

    if(logline[0] == ' ' && logline[1] == ' ') {
	curlog = L_CONT;
	/* curprog stays the same */
	skip_whitespace(s, ch);
	curstr = s-1;
	return 1;
    }

    /* isolate logtype field */

    skip_whitespace(s, ch);
    logstr = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';

    /* isolate program name field */

    skip_whitespace(s, ch);
    progstr = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';

    if (strcmp(progstr,"checkdump") == 0) {
	progstr = "amcheckdump";
    } else if (strcmp(progstr,"fetchdump") == 0) {
	progstr = "amfetchdump";
    }
    /* rest of line is logtype dependent string */

    skip_whitespace(s, ch);
    curstr = s - 1;

    /* lookup strings */

    for(curlog = L_MARKER; curlog != L_BOGUS; curlog--)
	if(g_str_equal(logtype_str[curlog], logstr)) break;

    for(curprog = P_LAST; curprog != P_UNKNOWN; curprog--)
	if(g_str_equal(program_str[curprog], progstr)) break;

    return 1;
}

char *
get_logtype_str(
    logtype_t logtype)
{
    return logtype_str[logtype];
}

char *
get_program_str(
    program_t program)
{
    return program_str[program];
}

