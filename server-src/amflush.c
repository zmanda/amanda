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
 * $Id: amflush.c,v 1.95 2006/07/25 21:41:24 martinea Exp $
 *
 * write files from work directory onto tape
 */
#include "amanda.h"

#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "logfile.h"
#include "clock.h"
#include "version.h"
#include "holding.h"
#include "driverio.h"
#include "server_util.h"
#include "timestamp.h"

static char *conf_logdir;
FILE *driver_stream;
char *driver_program;
char *reporter_program;
char *logroll_program;
char *datestamp;
char *amflush_timestamp;
char *amflush_datestamp;
sl_t *datestamp_list;

/* local functions */
int main(int main_argc, char **main_argv);
void flush_holdingdisk(char *diskdir, char *datestamp);
void confirm(void);
void redirect_stderr(void);
void detach(void);
void run_dumps(void);
static int get_letter_from_user(void);

int
main(
    int		main_argc,
    char **	main_argv)
{
    int foreground;
    int batch;
    int redirect;
    char **datearg = NULL;
    int nb_datearg = 0;
    char *conffile;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_logfile;
    int conf_usetimestamps;
    disklist_t diskq;
    disk_t *dp;
    pid_t pid;
    pid_t driver_pid, reporter_pid;
    amwait_t exitcode;
    int opt;
    dumpfile_t file;
    sl_t *holding_list=NULL;
    sle_t *holding_file;
    int driver_pipe[2];
    char date_string[100];
    time_t today;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;
    char *errstr;
    struct tm *tm;
    char *tapedev;
    char *tpchanger;
    char *qdisk, *qhname;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amflush");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    erroutput_type = ERR_INTERACTIVE;
    foreground = 0;
    batch = 0;
    redirect = 1;

    /* process arguments */

    parse_conf(main_argc, main_argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    while((opt = getopt(my_argc, my_argv, "bfsD:")) != EOF) {
	switch(opt) {
	case 'b': batch = 1;
		  break;
	case 'f': foreground = 1;
		  break;
	case 's': redirect = 0;
		  break;
	case 'D': if (datearg == NULL)
		      datearg = alloc(21*SIZEOF(char *));
		  if(nb_datearg == 20) {
		      fprintf(stderr,_("maximum of 20 -D arguments.\n"));
		      exit(1);
		  }
		  datearg[nb_datearg++] = stralloc(optarg);
		  datearg[nb_datearg] = NULL;
		  break;
	}
    }
    if(!foreground && !redirect) {
	fprintf(stderr,_("Can't redirect to stdout/stderr if not in forground.\n"));
	exit(1);
    }

    my_argc -= optind, my_argv += optind;

    if(my_argc < 1) {
	error(_("Usage: amflush%s [-b] [-f] [-s] [-D date]* <confdir> [host [disk]* ]* [-o configoption]*"), versionsuffix());
	/*NOTREACHED*/
    }

    config_name = my_argv[0];
    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if(read_conffile(conffile)) {
	error(_("errors processing config file \"%s\""), conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    check_running_as(RUNNING_AS_DUMPUSER | RUNNING_WITHOUT_SETUID);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if (read_diskfile(conf_diskfile, &diskq) < 0) {
	error(_("could not read disklist file \"%s\""), conf_diskfile);
	/*NOTREACHED*/
    }
    errstr = match_disklist(&diskq, my_argc-1, my_argv+1);
    if (errstr) {
	printf(_("%s"),errstr);
	amfree(errstr);
    }
    amfree(conf_diskfile);

    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if(read_tapelist(conf_tapelist)) {
	error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_tapelist);

    conf_usetimestamps = getconf_boolean(CNF_USETIMESTAMPS);

    amflush_datestamp = get_datestamp_from_time(0);
    if(conf_usetimestamps == 0) {
	amflush_timestamp = stralloc(amflush_datestamp);
    }
    else {
	amflush_timestamp = get_timestamp_from_time(0);
    }

    conf_logdir = getconf_str(CNF_LOGDIR);
    if (*conf_logdir == '/') {
	conf_logdir = stralloc(conf_logdir);
    } else {
	conf_logdir = stralloc2(config_dir, conf_logdir);
    }
    conf_logfile = vstralloc(conf_logdir, "/log", NULL);
    if (access(conf_logfile, F_OK) == 0) {
	error(_("%s exists: amdump or amflush is already running, or you must run amcleanup"), conf_logfile);
	/*NOTREACHED*/
    }
    amfree(conf_logfile);

    driver_program = vstralloc(libexecdir, "/", "driver", versionsuffix(),
			       NULL);
    reporter_program = vstralloc(sbindir, "/", "amreport", versionsuffix(),
				 NULL);
    logroll_program = vstralloc(libexecdir, "/", "amlogroll", versionsuffix(),
				NULL);

    tapedev = getconf_str(CNF_TAPEDEV);
    tpchanger = getconf_str(CNF_TPCHANGER);
    if (tapedev == NULL && tpchanger == NULL) {
	error(_("No tapedev or tpchanger specified"));
    }

    /* if dates were specified (-D), then use match_datestamp
     * against the list of all datestamps to turn that list
     * into a set of existing datestamps (basically, evaluate the
     * expressions into actual datestamps) */
    if(datearg) {
	sle_t *datestamp, *next_datestamp;
	int i, ok;

	datestamp_list = holding_get_all_datestamps();
	for(datestamp = datestamp_list->first; datestamp != NULL;) {
	    next_datestamp = datestamp->next;
	    ok = 0;
	    for(i=0; i<nb_datearg && ok==0; i++) {
		ok = match_datestamp(datearg[i], datestamp->name);
	    }
	    if(ok == 0) { /* remove datestamp from list */
		remove_sl(datestamp_list, datestamp);
	    }
	    datestamp = next_datestamp;
	}
    }
    else {
	/* otherwise, in batch mode, use all datestamps */
	if(batch) {
	    datestamp_list = holding_get_all_datestamps();
	}
	/* and finally, allow the user to pick datestamps */
	else {
	    datestamp_list = pick_datestamp();
	}
    }

    if(is_empty_sl(datestamp_list)) {
	printf(_("Could not find any Amanda directories to flush.\n"));
	exit(1);
    }

    holding_list = holding_get_files_for_flush(datestamp_list);
    if(holding_list->first == NULL) {
	printf(_("Could not find any valid dump image, check directory.\n"));
	exit(1);
    }

    if(!batch) confirm();

    for(dp = diskq.head; dp != NULL; dp = dp->next) {
	if(dp->todo) {
	    char *qname;
	    qname = quote_string(dp->name);
	    log_add(L_DISK, "%s %s", dp->host->hostname, qname);
	    amfree(qname);
	}
    }

    if(!foreground) { /* write it before redirecting stdout */
	puts(_("Running in background, you can log off now."));
	puts(_("You'll get mail when amflush is finished."));
    }

    if(redirect) redirect_stderr();

    if(!foreground) detach();

    erroutput_type = (ERR_AMANDALOG|ERR_INTERACTIVE);
    set_logerror(logerror);
    today = time(NULL);
    tm = localtime(&today);
    if (tm)
	strftime(date_string, 100, "%a %b %e %H:%M:%S %Z %Y", tm);
    else
	error(_("BAD DATE")); /* should never happen */
    fprintf(stderr, _("amflush: start at %s\n"), date_string);
    fprintf(stderr, _("amflush: datestamp %s\n"), amflush_timestamp);
    if (1) {
        char * timestamp = get_proper_stamp_from_time(0);
        fprintf(stderr, _("amflush: starttime %s\n"), timestamp);
        amfree(timestamp);
    }
    log_add(L_START, _("date %s"), amflush_timestamp);

    /* START DRIVER */
    if(pipe(driver_pipe) == -1) {
	error(_("error [opening pipe to driver: %s]"), strerror(errno));
	/*NOTREACHED*/
    }
    if((driver_pid = fork()) == 0) {
	/*
	 * This is the child process.
	 */
	dup2(driver_pipe[0], 0);
	close(driver_pipe[1]);
	execle(driver_program,
	       "driver", config_name, "nodump", (char *)0,
	       safe_env());
	error(_("cannot exec %s: %s"), driver_program, strerror(errno));
	/*NOTREACHED*/
    } else if(driver_pid == -1) {
	error(_("cannot fork for %s: %s"), driver_program, strerror(errno));
	/*NOTREACHED*/
    }
    driver_stream = fdopen(driver_pipe[1], "w");
    if (!driver_stream) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    fprintf(driver_stream, "DATE %s\n", amflush_timestamp);
    for(holding_file=holding_list->first; holding_file != NULL;
				   holding_file = holding_file->next) {
	holding_file_get_dumpfile(holding_file->name, &file);

	if (holding_file_size(holding_file->name, 1) <= 0) {
	    log_add(L_INFO, "%s: removing file with no data.",
		    holding_file->name);
	    holding_file_unlink(holding_file->name);
	    continue;
	}

	dp = lookup_disk(file.name, file.disk);
	if (!dp) {
	    error("dp == NULL");
	    /*NOTREACHED*/
	}
	if (dp->todo == 0) continue;

	qdisk = quote_string(file.disk);
	qhname = quote_string(holding_file->name);
	fprintf(stderr,
		"FLUSH %s %s %s %d %s\n",
		file.name,
		qdisk,
		file.datestamp,
		file.dumplevel,
		qhname);
	fprintf(driver_stream,
		"FLUSH %s %s %s %d %s\n",
		file.name,
		qdisk,
		file.datestamp,
		file.dumplevel,
		qhname);
	amfree(qdisk);
	amfree(qhname);
    }
    fprintf(stderr, "ENDFLUSH\n"); fflush(stderr);
    fprintf(driver_stream, "ENDFLUSH\n"); fflush(driver_stream);
    fclose(driver_stream);

    /* WAIT DRIVER */
    while(1) {
	if((pid = wait(&exitcode)) == -1) {
	    if(errno == EINTR) {
		continue;
	    } else {
		error(_("wait for %s: %s"), driver_program, strerror(errno));
		/*NOTREACHED*/
	    }
	} else if (pid == driver_pid) {
	    break;
	}
    }

    free_sl(datestamp_list);
    datestamp_list = NULL;
    free_sl(holding_list);
    holding_list = NULL;

    if(redirect) { /* rename errfile */
	char *errfile, *errfilex, *nerrfilex, number[100];
	int tapecycle;
	int maxdays, days;
		
	struct stat stat_buf;

	errfile = vstralloc(conf_logdir, "/amflush", NULL);
	errfilex = NULL;
	nerrfilex = NULL;
	tapecycle = getconf_int(CNF_TAPECYCLE);
	maxdays = tapecycle + 2;
	days = 1;
	/* First, find out the last existing errfile,           */
	/* to avoid ``infinite'' loops if tapecycle is infinite */

	snprintf(number,100,"%d",days);
	errfilex = newvstralloc(errfilex, errfile, ".", number, NULL);
	while ( days < maxdays && stat(errfilex,&stat_buf)==0) {
	    days++;
	    snprintf(number,100,"%d",days);
	    errfilex = newvstralloc(errfilex, errfile, ".", number, NULL);
	}
	snprintf(number,100,"%d",days);
	errfilex = newvstralloc(errfilex, errfile, ".", number, NULL);
	nerrfilex = NULL;
	while (days > 1) {
	    amfree(nerrfilex);
	    nerrfilex = errfilex;
	    days--;
	    snprintf(number,100,"%d",days);
	    errfilex = vstralloc(errfile, ".", number, NULL);
	    if (rename(errfilex, nerrfilex) != 0) {
		error(_("cannot rename \"%s\" to \"%s\": %s"),
		      errfilex, nerrfilex, strerror(errno));
	        /*NOTREACHED*/
	    }
	}
	errfilex = newvstralloc(errfilex, errfile, ".1", NULL);
	if (rename(errfile,errfilex) != 0) {
	    error(_("cannot rename \"%s\" to \"%s\": %s"),
		  errfilex, nerrfilex, strerror(errno));
	    /*NOTREACHED*/
	}
	amfree(errfile);
	amfree(errfilex);
	amfree(nerrfilex);
    }

    /*
     * Have amreport generate report and send mail.  Note that we do
     * not bother checking the exit status.  If it does not work, it
     * can be rerun.
     */

    if((reporter_pid = fork()) == 0) {
	/*
	 * This is the child process.
	 */
	execle(reporter_program,
	       "amreport", config_name, (char *)0,
	       safe_env());
	error(_("cannot exec %s: %s"), reporter_program, strerror(errno));
	/*NOTREACHED*/
    } else if(reporter_pid == -1) {
	error(_("cannot fork for %s: %s"), reporter_program, strerror(errno));
	/*NOTREACHED*/
    }
    while(1) {
	if((pid = wait(&exitcode)) == -1) {
	    if(errno == EINTR) {
		continue;
	    } else {
		error(_("wait for %s: %s"), reporter_program, strerror(errno));
		/*NOTREACHED*/
	    }
	} else if (pid == reporter_pid) {
	    break;
	}
    }

    /*
     * Call amlogroll to rename the log file to its datestamped version.
     * Since we exec at this point, our exit code will be that of amlogroll.
     */
    execle(logroll_program,
	   "amlogroll", config_name, (char *)0,
	   safe_env());
    error(_("cannot exec %s: %s"), logroll_program, strerror(errno));
    /*NOTREACHED*/
    return 0;				/* keep the compiler happy */
}


static int
get_letter_from_user(void)
{
    int r, ch;

    fflush(stdout); fflush(stderr);
    while((ch = getchar()) != EOF && ch != '\n' && isspace(ch)) {
	(void)ch; /* Quite lint */
    }
    if(ch == '\n') {
	r = '\0';
    } else if (ch != EOF) {
	r = ch;
	if(islower(r)) r = toupper(r);
	while((ch = getchar()) != EOF && ch != '\n') { 
	    (void)ch; /* Quite lint */
	}
    } else {
	r = ch;
	clearerr(stdin);
    }
    return r;
}


/*
 * confirm before detaching and running
 */

void
confirm(void)
{
    tape_t *tp;
    char *tpchanger;
    sle_t *datestamp;
    int ch;
    char *extra;

    printf(_("\nToday is: %s\n"),amflush_datestamp);
    printf(_("Flushing dumps from"));
    extra = "";
    for(datestamp = datestamp_list->first; datestamp != NULL; datestamp = datestamp->next) {
	printf("%s %s", extra, datestamp->name);
	extra = ",";
    }
    tpchanger = getconf_str(CNF_TPCHANGER);
    if(*tpchanger != '\0') {
	printf(_(" using tape changer \"%s\".\n"), tpchanger);
    } else {
	printf(_(" to tape drive \"%s\".\n"), getconf_str(CNF_TAPEDEV));
    }

    printf(_("Expecting "));
    tp = lookup_last_reusable_tape(0);
    if(tp != NULL) printf(_("tape %s or "), tp->label);
    printf(_("a new tape."));
    tp = lookup_tapepos(1);
    if(tp != NULL) printf(_("  (The last dumps were to tape %s)"), tp->label);

    while (1) {
	printf(_("\nAre you sure you want to do this [yN]? "));
	if((ch = get_letter_from_user()) == 'Y') {
	    return;
	} else if (ch == 'N' || ch == '\0' || ch == EOF) {
	    if (ch == EOF) {
		putchar('\n');
	    }
	    break;
	}
    }

    printf(_("Ok, quitting.  Run amflush again when you are ready.\n"));
    exit(1);
}

void
redirect_stderr(void)
{
    int fderr;
    char *errfile;

    fflush(stdout); fflush(stderr);
    errfile = vstralloc(conf_logdir, "/amflush", NULL);
    if((fderr = open(errfile, O_WRONLY| O_CREAT | O_TRUNC, 0600)) == -1) {
	error(_("could not open %s: %s"), errfile, strerror(errno));
	/*NOTREACHED*/
    }
    dup2(fderr,1);
    dup2(fderr,2);
    aclose(fderr);
    amfree(errfile);
}

void
detach(void)
{
    int fd;

    fflush(stdout); fflush(stderr);
    if((fd = open("/dev/null", O_RDWR, 0666)) == -1) {
	error(_("could not open /dev/null: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    dup2(fd,0);
    aclose(fd);

    switch(fork()) {
    case -1:
    	error(_("could not fork: %s"), strerror(errno));
	/*NOTREACHED*/

    case 0:
	setsid();
	return;
    }

    exit(0);
}
