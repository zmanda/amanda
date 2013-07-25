/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
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

#include "match.h"
#include "find.h"
#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "logfile.h"
#include "clock.h"
#include "holding.h"
#include "server_util.h"
#include "timestamp.h"
#include "getopt.h"
#include "cmdfile.h"

static struct option long_options[] = {
    {"version"         , 0, NULL,  1},
    {"exact-match"     , 0, NULL,  2},
    {NULL, 0, NULL, 0}
};

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

typedef struct cmdfile_data_s {
    char *ids;
    char *holding_file;
} cmdfile_data_t;

static void
cmdfile_flush(
    gpointer key G_GNUC_UNUSED,
    gpointer value,
    gpointer user_data)
{
    int id = GPOINTER_TO_INT(key);
    cmddata_t *cmddata = value;
    cmdfile_data_t *data = user_data;

    if (cmddata->operation == CMD_FLUSH &&
	g_str_equal(data->holding_file, cmddata->holding_file)) {
	if (data->ids) {
	    char *ids = g_strdup_printf("%s,%d;%s", data->ids, id, cmddata->storage_dest);
	    g_free(data->ids);
	    data->ids = ids;
	} else {
	    data->ids = g_strdup_printf("%d;%s", id, cmddata->storage_dest);
	}
    }
    cmddata->working_pid = getpid();
}

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
    char *conf_cmdfile;
    char *conf_logfile;
    int conf_usetimestamps;
    disklist_t diskq;
    GList  *dlist;
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
    config_overrides_t *cfg_ovr;
    char **config_options;
    find_result_t *holding_files;
    disklist_t holding_disklist = { NULL, NULL };
    gboolean exact_match = FALSE;
    cmddatas_t *cmddatas;
    GPtrArray *flush_ptr = g_ptr_array_sized_new(100);
    char     *line;
    gpointer *xline;

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

    cfg_ovr = new_config_overrides(argc/2);
    while((opt = getopt_long(argc, argv, "bfso:D:", long_options, NULL)) != EOF) {
	switch(opt) {
	case 1  : printf("amflush-%s\n", VERSION);
		  return(0);
		  break;
	case 2  : exact_match = TRUE;
		  break;
	case 'b': batch = 1;
		  break;
	case 'f': foreground = 1;
		  break;
	case 's': redirect = 0;
		  break;
	case 'o': add_config_override_opt(cfg_ovr, optarg);
		  break;
	case 'D': if (datearg == NULL)
		      datearg = g_malloc(21*sizeof(char *));
		  if(nb_datearg == 20) {
		      g_fprintf(stderr,_("maximum of 20 -D arguments.\n"));
		      exit(1);
		  }
		  datearg[nb_datearg++] = g_strdup(optarg);
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
	error(_("Usage: amflush [-b] [-f] [-s] [-D date]* [--exact-match] [-o configoption]* <confdir> [host [disk]* ]*"));
	/*NOTREACHED*/
    }

    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_EXPLICIT_NAME,
		argv[0]);

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

    holding_cleanup(NULL, stdout);

    /* load DLEs from the holding disk, in case there's anything to flush there */
    search_holding_disk(&holding_files, &holding_disklist);
    /* note that the dumps are added to the global disklist, so we need not
     * consult holding_files or holding_disklist after this.  The holding-only
     * dumps will be filtered properly by match_disklist, setting the dp->todo
     * flag appropriately. */

    errstr = match_disklist(&diskq, exact_match, argc-1, argv+1);
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
	amflush_timestamp = g_strdup(amflush_datestamp);
    }
    else {
	amflush_timestamp = get_timestamp_from_time(0);
    }

    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    amflush_timestamp = make_logname("amflush", amflush_timestamp);
    conf_logfile = get_logname();

    driver_program = g_strjoin(NULL, amlibexecdir, "/", "driver", NULL);
    reporter_program = g_strjoin(NULL, sbindir, "/", "amreport", NULL);

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
		    g_strdup((char *)datestamp->data),
		    g_compare_strings);
	}
	slist_free_full(all_datestamps, g_free);
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

    for (dlist = diskq.head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	if(dp->todo) {
	    /* is it holding_list */
	    for (holding_file=holding_list; holding_file != NULL;
					    holding_file = holding_file->next) {
		dumpfile_t file;
		holding_file_get_dumpfile((char *)holding_file->data, &file);
		if (g_str_equal(dp->host->hostname, file.name) &&
		    g_str_equal(dp->name, file.disk)) {
		    char *qname;
		    qname = quote_string(dp->name);
		    log_add(L_DISK, "%s %s", dp->host->hostname, qname);
		    amfree(qname);
		}
		dumpfile_free_data(&file);
	    }
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
	config_options = get_config_options(5);
	config_options[0] = "driver";
	config_options[1] = get_config_name();
	config_options[2] = "nodump";
	config_options[3] = "--log-filename";
	config_options[4] = conf_logfile;
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

    conf_cmdfile = config_dir_relative(getconf_str(CNF_CMDFILE));
    cmddatas = read_cmdfile(conf_cmdfile);
    g_free(conf_cmdfile);

    g_fprintf(driver_stream, "DATE %s\n", amflush_timestamp);
    for(holding_file=holding_list; holding_file != NULL;
				   holding_file = holding_file->next) {
	dumpfile_t file;
	cmdfile_data_t data;
	holding_file_get_dumpfile((char *)holding_file->data, &file);

	if (holding_file_size((char *)holding_file->data, 1) <= 0) {
	    g_debug("%s is empty - ignoring", (char *)holding_file->data);
	    log_add(L_INFO, "%s: removing file with no data.",
		    (char *)holding_file->data);
	    holding_file_unlink((char *)holding_file->data);
	    dumpfile_free_data(&file);
	    continue;
	}

	/* search_holding_disk should have already ensured that every
	 * holding dumpfile has an entry in the dynamic disklist */
	dp = lookup_disk(file.name, file.disk);
	assert(dp != NULL);

	/* but match_disklist may have indicated we should not flush it */
	if (dp->todo == 0) continue;

	/* find all cmdfile for that dump */
	data.ids = NULL;
	data.holding_file = (char *)holding_file->data;

	g_hash_table_foreach(cmddatas->cmdfile, &cmdfile_flush, &data);
	if (!data.ids) {
	    identlist_t  il;
	    cmddata_t   *cmddata = g_new0(cmddata_t, 1);

	    cmddata->operation = CMD_FLUSH;
	    cmddata->config = g_strdup(get_config_name());
	    cmddata->holding_file = g_strdup(data.holding_file);
	    cmddata->hostname = g_strdup(file.name);
	    cmddata->diskname = g_strdup(file.disk);
	    cmddata->dump_timestamp = g_strdup(file.datestamp);
	    /* add the first storage only */
	    il = getconf_identlist(CNF_STORAGE);
	    cmddata->storage_dest = g_strdup((char *)il->data);

	    cmddata->working_pid = getpid();
	    cmddata->status = CMD_TODO;
	    //add_cmd_in_cmdfile(cmddatas, cmddata);
	    cmddatas->max_id++;
	    cmddata->id = cmddatas->max_id;
	    g_hash_table_insert(cmddatas->cmdfile,
				GINT_TO_POINTER(cmddata->id), cmddata);
	    data.ids = g_strdup_printf("%d;%s", cmddata->id, (char *)il->data);
	}

	qdisk = quote_string(file.disk);
	qhname = quote_string((char *)holding_file->data);
	line = g_strdup_printf("FLUSH %s %s %s %s %d %s",
		data.ids,
		file.name,
		qdisk,
		file.datestamp,
		file.dumplevel,
		qhname);
	g_ptr_array_add(flush_ptr, line);
	g_debug("flushing '%s'", (char *)holding_file->data);

	g_free(data.ids);
	g_free(qdisk);
	g_free(qhname);
	dumpfile_free_data(&file);
    }
    // NULL terminate the array
    g_ptr_array_add(flush_ptr, NULL);
    write_cmdfile(cmddatas);
    for (xline = flush_ptr->pdata; *xline != NULL; xline++) {
	g_fprintf(stderr, "%s\n", (char *)*xline);
	g_fprintf(driver_stream, "%s\n", (char *)*xline);
    }
    g_ptr_array_free_full(flush_ptr);
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

    slist_free_full(datestamp_list, g_free);
    datestamp_list = NULL;
    slist_free_full(holding_list, g_free);
    holding_list = NULL;

    if(redirect) { /* rename errfile */
	char *errfile, *errfilex, *nerrfilex, number[100];
	int tapecycle;
	int maxdays, days;
		
	struct stat stat_buf;

	errfile = g_strjoin(NULL, conf_logdir, "/amflush", NULL);
	errfilex = NULL;
	nerrfilex = NULL;
	tapecycle = getconf_int(CNF_TAPECYCLE);
	maxdays = tapecycle + 2;
	days = 1;
	/* First, find out the last existing errfile,           */
	/* to avoid ``infinite'' loops if tapecycle is infinite */

	g_snprintf(number,100,"%d",days);
	g_free(errfilex);
	errfilex = g_strconcat(errfile, ".", number, NULL);
	while ( days < maxdays && stat(errfilex,&stat_buf)==0) {
	    days++;
	    g_snprintf(number,100,"%d",days);
	    g_free(errfilex);
	    errfilex = g_strconcat(errfile, ".", number, NULL);
	}
	g_snprintf(number,100,"%d",days);
	g_free(errfilex);
	errfilex = g_strconcat(errfile, ".", number, NULL);
	nerrfilex = NULL;
	while (days > 1) {
	    amfree(nerrfilex);
	    nerrfilex = errfilex;
	    days--;
	    g_snprintf(number,100,"%d",days);
	    errfilex = g_strjoin(NULL, errfile, ".", number, NULL);
	    if (rename(errfilex, nerrfilex) != 0) {
		error(_("cannot rename \"%s\" to \"%s\": %s"),
		      errfilex, nerrfilex, strerror(errno));
	        /*NOTREACHED*/
	    }
	}
	g_free(errfilex);
	errfilex = g_strconcat(errfile, ".1", NULL);
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
	config_options = get_config_options(5);
	config_options[0] = "amreport";
	config_options[1] = get_config_name();
        config_options[2] = "-l";
        config_options[3] = conf_logfile;
        config_options[4] = "--from-amdump";
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

    dbclose();

    return 0;				/* keep the compiler happy */
}


static int
get_letter_from_user(void)
{
    int r, ch;

    fflush(stdout); fflush(stderr);
    while((ch = getchar()) != EOF && ch != '\n' && ch >= 0 && ch <= 255 && g_ascii_isspace(ch)) {
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
	datestamps = g_malloc(g_slist_length(datestamp_list) * sizeof(char *));
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
		    slist_free_full(r_datestamp_list, g_free);
		    r_datestamp_list = NULL;
		    break;
		}
		r_datestamp_list = g_slist_append(r_datestamp_list,
					   g_strdup(datestamps[chupper - 'A']));
	    } while ((ch = *a++) != '\0');
	    if (r_datestamp_list && ch == '\0') {
		slist_free_full(datestamp_list, g_free);
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
    char *policy_n;
    char *storage_n;
    GSList *datestamp;
    int ch;
    char *extra;
    char *l_template;
    char *tapepool;
    int   tapecycle;
    int   retention_tapes;
    int   retention_days;
    int   retention_recover;
    int   retention_full;
    storage_t *st = NULL;
    policy_t *po = NULL;
    identlist_t il;

    g_printf(_("\nToday is: %s\n"),amflush_datestamp);
    for (il = getconf_identlist(CNF_STORAGE) ; il != NULL; il = il->next) {
	g_printf(_("Flushing dumps from"));
	extra = "";
	for(datestamp = datestamp_list; datestamp != NULL; datestamp = datestamp->next) {
	    g_printf("%s %s", extra, (char *)datestamp->data);
	    extra = ",";
	}
	storage_n = il->data;
	tpchanger = getconf_str(CNF_TPCHANGER);
	if (*storage_n != '\0') {
	    g_printf(_(" using storage \"%s\","), storage_n);
	    st = lookup_storage(storage_n);
	    tpchanger = storage_get_tpchanger(st);
	    g_printf(_(" tape changer \"%s\".\n"), tpchanger);
	    l_template = storage_get_labelstr(st)->template;
	    tapepool = storage_get_tapepool(st);
	    policy_n = storage_get_policy(st);
	    po = lookup_policy(policy_n);
	    if (po) {
		if (policy_seen(po, POLICY_RETENTION_TAPES))
		    retention_tapes = policy_get_retention_tapes(po);
		else
		    retention_tapes = getconf_int(CNF_TAPECYCLE);
		retention_days = policy_get_retention_days(po);
		retention_recover = policy_get_retention_recover(po);
		retention_full = policy_get_retention_full(po);
	    } else {
		retention_tapes = getconf_int(CNF_TAPECYCLE);
		retention_days = 0;
		retention_recover = 0;
		retention_full = 0;
	    }
	    tapecycle = getconf_int(CNF_TAPECYCLE);
	    tp = lookup_last_reusable_tape(l_template, tapepool, storage_n,
					   retention_tapes,
					   retention_days, retention_recover,
					   retention_full, 0);
	} else {
	    if (*tpchanger != '\0') {
		g_printf(_(" using tape changer \"%s\".\n"), tpchanger);
	    } else {
		tpchanger = getconf_str(CNF_TAPEDEV);
		g_printf(_(" to tape drive \"%s\".\n"), tpchanger);
	    }
	    l_template = getconf_labelstr(CNF_LABELSTR)->template;
	    tapecycle = getconf_int(CNF_TAPECYCLE);
	    tp = lookup_last_reusable_tape(l_template, "ERROR-POOL", "ERROR-STORAGE",
				       tapecycle, 0, 0, 0, 0);
	}

	if(tp != NULL)
	    g_printf(_("tape %s or "), tp->label);
	g_printf(_("a new tape."));
	tp = lookup_tapepos(1);
	if(tp != NULL)
	    g_printf(_("  (The last dumps were to tape %s)"), tp->label);
	g_printf("\n");
    }

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
    log_add(L_INFO, "pid-done %ld", (long)getpid());
    exit(1);
}

void
redirect_stderr(void)
{
    int fderr;
    char *errfile;

    fflush(stdout); fflush(stderr);
    errfile = g_strjoin(NULL, conf_logdir, "/amflush", NULL);
    if((fderr = open(errfile, O_WRONLY| O_APPEND | O_CREAT | O_TRUNC, 0600)) == -1) {
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
