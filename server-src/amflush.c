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

/* local functions */
void flush_holdingdisk(char *diskdir, char *datestamp);
static GSList * pick_datestamp(void);
void confirm(GSList *datestamp_list);
void redirect_stderr(void);
void detach(void);
void run_dumps(void);
static int get_letter_from_user(void);

int
main(
    int		argc,
    char **	argv)
{
    int foreground;
    int batch;
    int redirect;
    char **datearg = NULL;
    int nb_datearg = 0;
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
    GSList *holding_list=NULL, *holding_file;
    int driver_pipe[2];
    char date_string[100];
    char date_string_standard[100];
    time_t today;
    char *errstr;
    struct tm *tm;
    char *tapedev;
    char *tpchanger;
    char *qdisk, *qhname;
    GSList *datestamp_list = NULL;
    config_overwrites_t *cfg_ovr;
    char **config_options;

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

    add_amanda_log_handler(amanda_log_stderr);
    foreground = 0;
    batch = 0;
    redirect = 1;

    /* process arguments */

    cfg_ovr = new_config_overwrites(argc/2);
    while((opt = getopt(argc, argv, "bfso:D:")) != EOF) {
	switch(opt) {
	case 'b': batch = 1;
		  break;
	case 'f': foreground = 1;
		  break;
	case 's': redirect = 0;
		  break;
	case 'o': add_config_overwrite_opt(cfg_ovr, optarg);
		  break;
	case 'D': if (datearg == NULL)
		      datearg = alloc(21*SIZEOF(char *));
		  if(nb_datearg == 20) {
		      g_fprintf(stderr,_("maximum of 20 -D arguments.\n"));
		      exit(1);
		  }
		  datearg[nb_datearg++] = stralloc(optarg);
		  datearg[nb_datearg] = NULL;
		  break;
	}
    }
    argc -= optind, argv += optind;

    if(!foreground && !redirect) {
	g_fprintf(stderr,_("Can't redirect to stdout/stderr if not in forground.\n"));
	exit(1);
    }

    if(argc < 1) {
	error(_("Usage: amflush%s [-b] [-f] [-s] [-D date]* [-o configoption]* <confdir> [host [disk]* ]*"), versionsuffix());
	/*NOTREACHED*/
    }

    config_init(CONFIG_INIT_EXPLICIT_NAME,
		argv[0]);
    apply_config_overwrites(cfg_ovr);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &diskq);
    amfree(conf_diskfile);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    errstr = match_disklist(&diskq, argc-1, argv+1);
    if (errstr) {
	g_printf(_("%s"),errstr);
	amfree(errstr);
    }

    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
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

    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    conf_logfile = vstralloc(conf_logdir, "/log", NULL);
    if (access(conf_logfile, F_OK) == 0) {
	run_amcleanup(get_config_name());
    }
    if (access(conf_logfile, F_OK) == 0) {
	char *process_name = get_master_process(conf_logfile);
	error(_("%s exists: %s is already running, or you must run amcleanup"), conf_logfile, process_name);
	/*NOTREACHED*/
    }
    amfree(conf_logfile);

    log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());
    driver_program = vstralloc(amlibexecdir, "/", "driver", versionsuffix(),
			       NULL);
    reporter_program = vstralloc(sbindir, "/", "amreport", versionsuffix(),
				 NULL);
    logroll_program = vstralloc(amlibexecdir, "/", "amlogroll", versionsuffix(),
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
	GSList *all_datestamps;
	GSList *datestamp;
	int i, ok;

	all_datestamps = holding_get_all_datestamps();
	for(datestamp = all_datestamps; datestamp != NULL; datestamp = datestamp->next) {
	    ok = 0;
	    for(i=0; i<nb_datearg && ok==0; i++) {
		ok = match_datestamp(datearg[i], (char *)datestamp->data);
	    }
	    if (ok)
		datestamp_list = g_slist_insert_sorted(datestamp_list,
		    stralloc((char *)datestamp->data),
		    g_compare_strings);
	}
	g_slist_free_full(all_datestamps);
    }
    else {
	/* otherwise, in batch mode, use all datestamps */
	if(batch) {
	    datestamp_list = holding_get_all_datestamps();
	}
	/* or allow the user to pick datestamps */
	else {
	    datestamp_list = pick_datestamp();
	}
    }

    if(!datestamp_list) {
	g_printf(_("Could not find any Amanda directories to flush.\n"));
	exit(1);
    }

    holding_list = holding_get_files_for_flush(datestamp_list);
    if (holding_list == NULL) {
	g_printf(_("Could not find any valid dump image, check directory.\n"));
	exit(1);
    }

    if(!batch) confirm(datestamp_list);

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

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_trace_log);
    today = time(NULL);
    tm = localtime(&today);
    if (tm) {
	strftime(date_string, 100, "%a %b %e %H:%M:%S %Z %Y", tm);
	strftime(date_string_standard, 100, "%Y-%m-%d %H:%M:%S %Z", tm);
    } else {
	error(_("BAD DATE")); /* should never happen */
    }
    g_fprintf(stderr, _("amflush: start at %s\n"), date_string);
    g_fprintf(stderr, _("amflush: datestamp %s\n"), amflush_timestamp);
    g_fprintf(stderr, _("amflush: starttime %s\n"), amflush_timestamp);
    g_fprintf(stderr, _("amflush: starttime-locale-independent %s\n"),
	      date_string_standard);
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
	config_options = get_config_options(3);
	config_options[0] = "driver";
	config_options[1] = get_config_name();
	config_options[2] = "nodump";
	safe_fd(-1, 0);
	execve(driver_program, config_options, safe_env());
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

    g_fprintf(driver_stream, "DATE %s\n", amflush_timestamp);
    for(holding_file=holding_list; holding_file != NULL;
				   holding_file = holding_file->next) {
	dumpfile_t file;
	holding_file_get_dumpfile((char *)holding_file->data, &file);

	if (holding_file_size((char *)holding_file->data, 1) <= 0) {
	    log_add(L_INFO, "%s: removing file with no data.",
		    (char *)holding_file->data);
	    holding_file_unlink((char *)holding_file->data);
	    dumpfile_free_data(&file);
	    continue;
	}

	dp = lookup_disk(file.name, file.disk);
	if (!dp) {
	    error("dp == NULL");
	    /*NOTREACHED*/
	}
	if (dp->todo == 0) continue;

	qdisk = quote_string(file.disk);
	qhname = quote_string((char *)holding_file->data);
	g_fprintf(stderr,
		"FLUSH %s %s %s %d %s\n",
		file.name,
		qdisk,
		file.datestamp,
		file.dumplevel,
		qhname);
	g_fprintf(driver_stream,
		"FLUSH %s %s %s %d %s\n",
		file.name,
		qdisk,
		file.datestamp,
		file.dumplevel,
		qhname);
	amfree(qdisk);
	amfree(qhname);
	dumpfile_free_data(&file);
    }
    g_fprintf(stderr, "ENDFLUSH\n"); fflush(stderr);
    g_fprintf(driver_stream, "ENDFLUSH\n"); fflush(driver_stream);
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

    g_slist_free_full(datestamp_list);
    datestamp_list = NULL;
    g_slist_free_full(holding_list);
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

	g_snprintf(number,100,"%d",days);
	errfilex = newvstralloc(errfilex, errfile, ".", number, NULL);
	while ( days < maxdays && stat(errfilex,&stat_buf)==0) {
	    days++;
	    g_snprintf(number,100,"%d",days);
	    errfilex = newvstralloc(errfilex, errfile, ".", number, NULL);
	}
	g_snprintf(number,100,"%d",days);
	errfilex = newvstralloc(errfilex, errfile, ".", number, NULL);
	nerrfilex = NULL;
	while (days > 1) {
	    amfree(nerrfilex);
	    nerrfilex = errfilex;
	    days--;
	    g_snprintf(number,100,"%d",days);
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
	config_options = get_config_options(2);
	config_options[0] = "amreport";
	config_options[1] = get_config_name();
	safe_fd(-1, 0);
	execve(reporter_program, config_options, safe_env());
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

    log_add(L_INFO, "pid-done %ld", (long)getpid());

    /*
     * Call amlogroll to rename the log file to its datestamped version.
     * Since we exec at this point, our exit code will be that of amlogroll.
     */
    config_options = get_config_options(2);
    config_options[0] = "amlogroll";
    config_options[1] = get_config_name();
    safe_fd(-1, 0);
    execve(logroll_program, config_options, safe_env());
    error(_("cannot exec %s: %s"), logroll_program, strerror(errno));
    /*NOTREACHED*/
    return 0;				/* keep the compiler happy */
}


static int
get_letter_from_user(void)
{
    int r, ch;

    fflush(stdout); fflush(stderr);
    while((ch = getchar()) != EOF && ch != '\n' && g_ascii_isspace(ch)) {
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

/* Allow the user to select a set of datestamps from those in
 * holding disks.  The result can be passed to 
 * holding_get_files_for_flush.  If less than two dates are
 * available, then no user interaction takes place.
 *
 * @returns: a new GSList listing the selected datestamps
 */
static GSList *
pick_datestamp(void)
{
    GSList *datestamp_list;
    GSList *r_datestamp_list = NULL;
    GSList *ds;
    char **datestamps = NULL;
    int i;
    char *answer = NULL;
    char *a = NULL;
    int ch = 0;
    char max_char = '\0', chupper = '\0';

    datestamp_list = holding_get_all_datestamps();

    if(g_slist_length(datestamp_list) < 2) {
	return datestamp_list;
    } else {
	datestamps = alloc(g_slist_length(datestamp_list) * SIZEOF(char *));
	for(ds = datestamp_list, i=0; ds != NULL; ds = ds->next,i++) {
	    datestamps[i] = (char *)ds->data; /* borrowing reference */
	}

	while(1) {
	    puts(_("\nMultiple Amanda runs in holding disks; please pick one by letter:"));
	    for(ds = datestamp_list, max_char = 'A';
		ds != NULL && max_char <= 'Z';
		ds = ds->next, max_char++) {
		g_printf("  %c. %s\n", max_char, (char *)ds->data);
	    }
	    max_char--;
	    g_printf(_("Select datestamps to flush [A..%c or <enter> for all]: "), max_char);
	    fflush(stdout); fflush(stderr);
	    amfree(answer);
	    if ((answer = agets(stdin)) == NULL) {
		clearerr(stdin);
		continue;
	    }

	    if (*answer == '\0' || strncasecmp(answer, "ALL", 3) == 0) {
		break;
	    }

	    a = answer;
	    while ((ch = *a++) != '\0') {
		if (!g_ascii_isspace(ch))
		    break;
	    }

	    /* rewrite the selected list into r_datestamp_list, then copy it over
	     * to datestamp_list */
	    do {
		if (g_ascii_isspace(ch) || ch == ',') {
		    continue;
		}
		chupper = (char)toupper(ch);
		if (chupper < 'A' || chupper > max_char) {
		    g_slist_free_full(r_datestamp_list);
		    r_datestamp_list = NULL;
		    break;
		}
		r_datestamp_list = g_slist_append(r_datestamp_list,
					   stralloc(datestamps[chupper - 'A']));
	    } while ((ch = *a++) != '\0');
	    if (r_datestamp_list && ch == '\0') {
		g_slist_free_full(datestamp_list);
		datestamp_list = r_datestamp_list;
		break;
	    }
	}
    }
    amfree(datestamps); /* references in this array are borrowed */
    amfree(answer);

    return datestamp_list;
}


/*
 * confirm before detaching and running
 */

void
confirm(GSList *datestamp_list)
{
    tape_t *tp;
    char *tpchanger;
    GSList *datestamp;
    int ch;
    char *extra;

    g_printf(_("\nToday is: %s\n"),amflush_datestamp);
    g_printf(_("Flushing dumps from"));
    extra = "";
    for(datestamp = datestamp_list; datestamp != NULL; datestamp = datestamp->next) {
	g_printf("%s %s", extra, (char *)datestamp->data);
	extra = ",";
    }
    tpchanger = getconf_str(CNF_TPCHANGER);
    if(*tpchanger != '\0') {
	g_printf(_(" using tape changer \"%s\".\n"), tpchanger);
    } else {
	g_printf(_(" to tape drive \"%s\".\n"), getconf_str(CNF_TAPEDEV));
    }

    g_printf(_("Expecting "));
    tp = lookup_last_reusable_tape(0);
    if(tp != NULL)
	g_printf(_("tape %s or "), tp->label);
    g_printf(_("a new tape."));
    tp = lookup_tapepos(1);
    if(tp != NULL)
	g_printf(_("  (The last dumps were to tape %s)"), tp->label);

    while (1) {
	g_printf(_("\nAre you sure you want to do this [yN]? "));
	if((ch = get_letter_from_user()) == 'Y') {
	    return;
	} else if (ch == 'N' || ch == '\0' || ch == EOF) {
	    if (ch == EOF) {
		putchar('\n');
	    }
	    break;
	}
    }

    g_printf(_("Ok, quitting.  Run amflush again when you are ready.\n"));
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
