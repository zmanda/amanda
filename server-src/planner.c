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
/*
 * $Id: planner.c 10421 2008-03-06 18:48:30Z martineau $
 *
 * backup schedule planner for the Amanda backup system.
 */
#include "amanda.h"
#include "arglist.h"
#include "find.h"
#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "infofile.h"
#include "logfile.h"
#include "clock.h"
#include "packet.h"
#include "security.h"
#include "protocol.h"
#include "version.h"
#include "amfeatures.h"
#include "server_util.h"
#include "holding.h"
#include "timestamp.h"
#include "amxml.h"

#define planner_debug(i,x) do {		\
	if ((i) <= debug_planner) {	\
	    dbprintf(x);		\
	}				\
} while (0)

#define MAX_LEVELS		    3	/* max# of estimates per filesys */

#define RUNS_REDZONE		    5	/* should be in conf file? */

#define PROMOTE_THRESHOLD	 0.05	/* if <5% unbalanced, don't promote */
#define DEFAULT_DUMPRATE	 1024.0	/* K/s */

/* configuration file stuff */

char *	conf_tapetype;
gint64 	conf_maxdumpsize;
int	conf_runtapes;
int	conf_dumpcycle;
int	conf_runspercycle;
int	conf_tapecycle;
time_t	conf_etimeout;
int	conf_reserve;
int	conf_autoflush;
int	conf_usetimestamps;

#define HOST_READY				((void *)0)	/* must be 0 */
#define HOST_ACTIVE				((void *)1)
#define HOST_DONE				((void *)2)

#define DISK_READY				0		/* must be 0 */
#define DISK_ACTIVE				1
#define DISK_PARTIALY_DONE			2
#define DISK_DONE				3

typedef struct one_est_s {
    int     level;
    gint64  nsize;	/* native size     */
    gint64  csize;	/* compressed size */
    char   *dumpdate;
    int     guessed;    /* If server guessed the estimate size */
} one_est_t;
static one_est_t default_one_est = {-1, -1, -1, "INVALID_DATE", 0};

typedef struct est_s {
    int state;
    int got_estimate;
    int dump_priority;
    one_est_t *dump_est;
    one_est_t *degr_est;
    one_est_t  estimate[MAX_LEVELS];
    int last_level;
    gint64 last_lev0size;
    int next_level0;
    int level_days;
    int promote;
    int post_dle;
    double fullrate, incrrate;
    double fullcomp, incrcomp;
    char *errstr;
    char *degr_mesg;
    info_t *info;
} est_t;

#define est(dp)	((est_t *)(dp)->up)

/* pestq = partial estimate */
disklist_t startq, waitq, pestq, estq, failq, schedq;
gint64 total_size;
double total_lev0, balanced_size, balance_threshold;
gint64 tape_length;
size_t tape_mark;

tapetype_t *tape;
size_t tt_blocksize;
size_t tt_blocksize_kb;
int runs_per_cycle = 0;
time_t today;
char *planner_timestamp = NULL;

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;

/* We keep a LIFO queue of before images for all modifications made
 * to schedq in our attempt to make the schedule fit on the tape.
 * Enough information is stored to reinstate a dump if it turns out
 * that it shouldn't have been touched after all.
 */
typedef struct bi_s {
    struct bi_s *next;
    struct bi_s *prev;
    int deleted;		/* 0=modified, 1=deleted */
    disk_t *dp;			/* The disk that was changed */
    int level;			/* The original level */
    gint64 nsize;		/* The original native size */
    gint64 csize;		/* The original compressed size */
    char *errstr;		/* A message describing why this disk is here */
} bi_t;

typedef struct bilist_s {
    bi_t *head, *tail;
} bilist_t;

bilist_t biq;			/* The BI queue itself */

/*
 * ========================================================================
 * MAIN PROGRAM
 *
 */

static void setup_estimate(disk_t *dp);
static void get_estimates(void);
static void analyze_estimate(disk_t *dp);
static void handle_failed(disk_t *dp);
static void delay_dumps(void);
static int promote_highest_priority_incremental(void);
static int promote_hills(void);
static void output_scheduleline(disk_t *dp);
static void server_estimate(disk_t *dp, int i, info_t *info, int level);
int main(int, char **);

int
main(
    int		argc,
    char **	argv)
{
    disklist_t origq;
    disk_t *dp;
    int moved_one;
    int diskarg_offset;
    gint64 initial_size;
    int i;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_infofile;
    times_t section_start;
    char *qname;
    int    nb_disk;
    char  *errstr = NULL;
    config_overrides_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;
    int    planner_setuid;
    int exit_status = EXIT_SUCCESS;
    gboolean no_taper = FALSE;
    gboolean from_client = FALSE;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("planner-%s\n", VERSION);
	return (0);
    }

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    /* drop root privileges */
    planner_setuid = set_root_privs(0);

    safe_fd(-1, 0);

    set_pname("planner");

    dbopen(DBG_SUBDIR_SERVER);

    cfg_ovr = extract_commandline_config_overrides(&argc, &argv);
    if (argc > 1) 
	cfg_opt = argv[1];

    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, cfg_opt);

    /* conf_diskfile is freed later, as it may be used in an error message */
    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &origq);
    disable_skip_disk(&origq);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    setvbuf(stderr, (char *)NULL, (int)_IOLBF, 0);

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_trace_log);

    if (!planner_setuid) {
	error(_("planner must be run setuid root"));
    }

    if (config_errors(NULL) >= CFGERR_ERRORS) {
	g_critical(_("errors processing config file"));
    }

    safe_cd();

    check_running_as(RUNNING_AS_ROOT | RUNNING_AS_UID_ONLY);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    startclock();
    section_start = curclock();

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());
    g_fprintf(stderr, _("%s: pid %ld executable %s version %s\n"),
	    get_pname(), (long) getpid(), argv[0], VERSION);
    for (i = 0; version_info[i] != NULL; i++)
	g_fprintf(stderr, _("%s: %s"), get_pname(), version_info[i]);

    diskarg_offset = 2;
    if (argc - diskarg_offset > 1 && strcmp(argv[diskarg_offset], "--starttime") == 0) {
	planner_timestamp = stralloc(argv[diskarg_offset+1]);
	diskarg_offset += 2;
    }
    if (argc - diskarg_offset > 0 && strcmp(argv[diskarg_offset], "--no-taper") == 0) {
	no_taper = TRUE;
	diskarg_offset += 1;
    }
    if (argc - diskarg_offset > 0 && strcmp(argv[diskarg_offset], "--from-client") == 0) {
	from_client = TRUE;
	diskarg_offset += 1;
    }


    run_server_global_scripts(EXECUTE_ON_PRE_ESTIMATE, get_config_name());

    /*
     * 1. Networking Setup
     *
     */

    protocol_init();

    /*
     * 2. Read in Configuration Information
     *
     * All the Amanda configuration files are loaded before we begin.
     */

    g_fprintf(stderr,_("READING CONF INFO...\n"));

    if(origq.head == NULL) {
	error(_("empty disklist \"%s\""), conf_diskfile);
	/*NOTREACHED*/
    }

    amfree(conf_diskfile);

    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
    if(read_tapelist(conf_tapelist)) {
	error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_tapelist);

    conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
    if(open_infofile(conf_infofile)) {
	error(_("could not open info db \"%s\""), conf_infofile);
	/*NOTREACHED*/
    }
    if (check_infofile(conf_infofile, &origq, &errstr) == -1) {
	log_add(L_WARNING, "problem copying infofile: %s", errstr);
	amfree(errstr);
    }
    amfree(conf_infofile);

    conf_tapetype = getconf_str(CNF_TAPETYPE);
    conf_maxdumpsize = getconf_int64(CNF_MAXDUMPSIZE);
    conf_runtapes = getconf_int(CNF_RUNTAPES);
    conf_dumpcycle = getconf_int(CNF_DUMPCYCLE);
    conf_runspercycle = getconf_int(CNF_RUNSPERCYCLE);
    conf_tapecycle = getconf_int(CNF_TAPECYCLE);
    conf_etimeout = (time_t)getconf_int(CNF_ETIMEOUT);
    conf_reserve  = getconf_int(CNF_RESERVE);
    conf_autoflush = getconf_no_yes_all(CNF_AUTOFLUSH);
    conf_usetimestamps = getconf_boolean(CNF_USETIMESTAMPS);

    today = time(0);
    if (planner_timestamp) {
	if (conf_usetimestamps == 0) {
	    planner_timestamp[8] = '\0';
	}
    } else if(conf_usetimestamps == 0) {
	planner_timestamp = get_datestamp_from_time(0);
    }
    else {
	planner_timestamp = get_timestamp_from_time(0);
    }
    log_add(L_START, _("date %s"), planner_timestamp);
    g_printf("DATE %s\n", planner_timestamp);
    fflush(stdout);
    g_fprintf(stderr, _("%s: timestamp %s\n"),
		    get_pname(), planner_timestamp);

    errstr = match_disklist(&origq, argc-diskarg_offset,
				    argv+diskarg_offset);
    if (errstr) {
	g_fprintf(stderr,"%s",errstr);
        exit_status = EXIT_FAILURE;
    }

    for (dp = origq.head; dp != NULL; dp = dp->next) {
	if (dp->todo) {
	    if (from_client) {
		if (!dp->dump_limit || !dp->dump_limit->same_host)
		    dp->todo = 0;
	    } else {
		if (dp->dump_limit && !dp->dump_limit->server)
		    dp->todo = 0;
	    }
	}
    }

    nb_disk = 0;
    for (dp = origq.head; dp != NULL; dp = dp->next) {
	if (dp->todo) {
	    qname = quote_string(dp->name);
	    log_add(L_DISK, "%s %s", dp->host->hostname, qname);
	    amfree(qname);
	    nb_disk++;
	}
    }

    if (nb_disk == 0) {
	if (errstr) {
	    error(_("no DLE to backup; %s"), errstr);
	} else {
	    error(_("no DLE to backup"));
	}
	/*NOTREACHED*/
    } else if (errstr) {
	log_add(L_WARNING, "WARNING: %s", errstr);
    }
    amfree(errstr);

    /* some initializations */

    if(conf_runspercycle == 0) {
	runs_per_cycle = conf_dumpcycle;
    } else if(conf_runspercycle == -1 ) {
	runs_per_cycle = guess_runs_from_tapelist();
    } else
	runs_per_cycle = conf_runspercycle;

    if (runs_per_cycle <= 0) {
	runs_per_cycle = 1;
    }

    /*
     * do some basic sanity checking
     */
     if(conf_tapecycle <= runs_per_cycle) {
	log_add(L_WARNING, _("tapecycle (%d) <= runspercycle (%d)"),
		conf_tapecycle, runs_per_cycle);
     }
    
    tape = lookup_tapetype(conf_tapetype);
    if(conf_maxdumpsize > (gint64)0) {
	tape_length = conf_maxdumpsize;
	g_fprintf(stderr, "planner: tape_length is set from maxdumpsize (%jd KB)\n",
			 (intmax_t)conf_maxdumpsize);
    }
    else {
	tape_length = tapetype_get_length(tape) * (gint64)conf_runtapes;
	g_fprintf(stderr, "planner: tape_length is set from tape length (%jd KB) * runtapes (%d) == %jd KB\n",
			 (intmax_t)tapetype_get_length(tape),
			 conf_runtapes,
			 (intmax_t)tape_length);
    }
    tape_mark = (size_t)tapetype_get_filemark(tape);
    tt_blocksize_kb = (size_t)tapetype_get_blocksize(tape);
    tt_blocksize = tt_blocksize_kb * 1024;

    g_fprintf(stderr, _("%s: time %s: startup took %s secs\n"),
		    get_pname(),
		    walltime_str(curclock()),
		    walltime_str(timessub(curclock(), section_start)));

    /*
     * 3. Send autoflush dumps left on the holding disks
     *
     * This should give us something to do while we generate the new
     * dump schedule.
     */

    g_fprintf(stderr,_("\nSENDING FLUSHES...\n"));

    if(conf_autoflush && !no_taper) {
	dumpfile_t  file;
	GSList *holding_list, *holding_file;
	char *qdisk, *qhname;

	/* get *all* flushable files in holding, without checking against
	 * the disklist (which may not contain some of the dumps) */
	holding_list = holding_get_files_for_flush(NULL);
	for(holding_file=holding_list; holding_file != NULL;
				       holding_file = holding_file->next) {
	    holding_file_get_dumpfile((char *)holding_file->data, &file);

	    if (holding_file_size((char *)holding_file->data, 1) <= 0) {
		log_add(L_INFO, "%s: removing file with no data.",
			(char *)holding_file->data);
		holding_file_unlink((char *)holding_file->data);
		dumpfile_free_data(&file);
		continue;
	    }

	    /* see if this matches the command-line arguments */
	    if (conf_autoflush == 1 &&
		!match_dumpfile(&file, argc-diskarg_offset,
				       argv+diskarg_offset)) {
		continue;
	    }

	    qdisk = quote_string(file.disk);
	    qhname = quote_string((char *)holding_file->data);
	    log_add(L_DISK, "%s %s", file.name, qdisk);
	    g_fprintf(stderr,
		    "FLUSH %s %s %s %d %s\n",
		    file.name,
		    qdisk,
		    file.datestamp,
		    file.dumplevel,
		    qhname);
	    g_fprintf(stdout,
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
	slist_free_full(holding_list, g_free);
	holding_list = NULL;
    }
    g_fprintf(stderr, _("ENDFLUSH\n"));
    g_fprintf(stdout, _("ENDFLUSH\n"));
    fflush(stdout);

    /*
     * 4. Calculate Preliminary Dump Levels
     *
     * Before we can get estimates from the remote slave hosts, we make a
     * first attempt at guessing what dump levels we will be dumping at
     * based on the curinfo database.
     */

    g_fprintf(stderr,_("\nSETTING UP FOR ESTIMATES...\n"));
    section_start = curclock();

    startq.head = startq.tail = NULL;
    while(!empty(origq)) {
	disk_t *dp = dequeue_disk(&origq);
	if(dp->todo == 1) {
	    setup_estimate(dp);
	}
    }

    g_fprintf(stderr, _("%s: time %s: setting up estimates took %s secs\n"),
		    get_pname(),
		    walltime_str(curclock()),
		    walltime_str(timessub(curclock(), section_start)));


    /*
     * 5. Get Dump Size Estimates from Remote Client Hosts
     *
     * Each host is queried (in parallel) for dump size information on all
     * of its disks, and the results gathered as they come in.
     */

    /* go out and get the dump estimates */

    g_fprintf(stderr,_("\nGETTING ESTIMATES...\n"));
    section_start = curclock();

    estq.head = estq.tail = NULL;
    pestq.head = pestq.tail = NULL;
    waitq.head = waitq.tail = NULL;
    failq.head = failq.tail = NULL;

    get_estimates();

    g_fprintf(stderr, _("%s: time %s: getting estimates took %s secs\n"),
		    get_pname(),
		    walltime_str(curclock()),
		    walltime_str(timessub(curclock(), section_start)));

    /*
     * At this point, all disks with estimates are in estq, and
     * all the disks on hosts that didn't respond to our inquiry
     * are in failq.
     */

    dump_queue("FAILED", failq, 15, stderr);
    dump_queue("DONE", estq, 15, stderr);

    if (!empty(failq)) {
        exit_status = EXIT_FAILURE;
    }

    /*
     * 6. Analyze Dump Estimates
     *
     * Each disk's estimates are looked at to determine what level it
     * should dump at, and to calculate the expected size and time taking
     * historical dump rates and compression ratios into account.  The
     * total expected size is accumulated as well.
     */

    g_fprintf(stderr,_("\nANALYZING ESTIMATES...\n"));
    section_start = curclock();

			/* an empty tape still has a label and an endmark */
    total_size = ((gint64)tt_blocksize_kb + (gint64)tape_mark) * (gint64)2;
    total_lev0 = 0.0;
    balanced_size = 0.0;

    schedq.head = schedq.tail = NULL;
    while(!empty(estq)) analyze_estimate(dequeue_disk(&estq));
    while(!empty(failq)) handle_failed(dequeue_disk(&failq));

    run_server_global_scripts(EXECUTE_ON_POST_ESTIMATE, get_config_name());

    /*
     * At this point, all the disks are on schedq sorted by priority.
     * The total estimated size of the backups is in total_size.
     */

    {
	disk_t *dp;

	g_fprintf(stderr, _("INITIAL SCHEDULE (size %lld):\n"),
		(long long)total_size);
	for(dp = schedq.head; dp != NULL; dp = dp->next) {
	    qname = quote_string(dp->name);
	    g_fprintf(stderr, _("  %s %s pri %d lev %d nsize %lld csize %lld\n"),
		    dp->host->hostname, qname, est(dp)->dump_priority,
		    est(dp)->dump_est->level,
		    (long long)est(dp)->dump_est->nsize,
                    (long long)est(dp)->dump_est->csize);
	    amfree(qname);
	}
    }


    /*
     * 7. Delay Dumps if Schedule Too Big
     *
     * If the generated schedule is too big to fit on the tape, we need to
     * delay some full dumps to make room.  Incrementals will be done
     * instead (except for new or forced disks).
     *
     * In extreme cases, delaying all the full dumps is not even enough.
     * If so, some low-priority incrementals will be skipped completely
     * until the dumps fit on the tape.
     */

    g_fprintf(stderr, _("\nDELAYING DUMPS IF NEEDED, total_size %lld, tape length %lld mark %zu\n"),
	    (long long)total_size,
	    (long long)tape_length,
	    tape_mark);

    initial_size = total_size;

    delay_dumps();

    /* XXX - why bother checking this? */
    if(empty(schedq) && total_size < initial_size) {
	error(_("cannot fit anything on tape, bailing out"));
	/*NOTREACHED*/
    }


    /*
     * 8. Promote Dumps if Schedule Too Small
     *
     * Amanda attempts to balance the full dumps over the length of the
     * dump cycle.  If this night's full dumps are too small relative to
     * the other nights, promote some high-priority full dumps that will be
     * due for the next run, to full dumps for tonight, taking care not to
     * overflow the tape size.
     *
     * This doesn't work too well for small sites.  For these we scan ahead
     * looking for nights that have an excessive number of dumps and promote
     * one of them.
     *
     * Amanda never delays full dumps just for the sake of balancing the
     * schedule, so it can take a full cycle to balance the schedule after
     * a big bump.
     */

    g_fprintf(stderr,
     _("\nPROMOTING DUMPS IF NEEDED, total_lev0 %1.0lf, balanced_size %1.0lf...\n"),
	    total_lev0, balanced_size);

    balance_threshold = balanced_size * PROMOTE_THRESHOLD;
    moved_one = 1;
    while((balanced_size - total_lev0) > balance_threshold && moved_one)
	moved_one = promote_highest_priority_incremental();

    moved_one = promote_hills();

    g_fprintf(stderr, _("%s: time %s: analysis took %s secs\n"),
		    get_pname(),
		    walltime_str(curclock()),
		    walltime_str(timessub(curclock(), section_start)));


    /*
     * 9. Output Schedule
     *
     * The schedule goes to stdout, presumably to driver.  A copy is written
     * on stderr for the debug file.
     */

    g_fprintf(stderr,_("\nGENERATING SCHEDULE:\n--------\n"));
    if (empty(schedq)) {
        exit_status = EXIT_FAILURE;
        g_fprintf(stderr, _("--> Generated empty schedule! <--\n"));
    } else {
        while(!empty(schedq)) output_scheduleline(dequeue_disk(&schedq));
    }
    g_fprintf(stderr, _("--------\n"));

    close_infofile();
    log_add(L_FINISH, _("date %s time %s"), planner_timestamp, walltime_str(curclock()));
    log_add(L_INFO, "pid-done %ld", (long)getpid());

    clear_tapelist();
    amfree(planner_timestamp);
    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;

    dbclose();

    return exit_status;
}



/*
 * ========================================================================
 * SETUP FOR ESTIMATES
 *
 */

static void askfor(est_t *, int, int, info_t *);
static int last_level(info_t *info);		  /* subroutines */
static one_est_t *est_for_level(disk_t *dp, int level);
static void est_csize(disk_t *dp, one_est_t *one_est);
static gint64 est_tape_size(disk_t *dp, int level);
static int next_level0(disk_t *dp, info_t *info);
static int runs_at(info_t *info, int lev);
static gint64 bump_thresh(int level, gint64 size_level_0, int bumppercent, gint64 bumpsize, double bumpmult);
static int when_overwrite(char *label);

static void askfor(
    est_t *ep,	/* esimate data block */
    int seq,	/* sequence number of request */
    int lev,	/* dump level being requested */
    info_t *info)	/* info block for disk */
{
    if(seq < 0 || seq >= MAX_LEVELS) {
	error(_("error [planner askfor: seq out of range 0..%d: %d]"),
	      MAX_LEVELS, seq);
	/*NOTREACHED*/
    }
    if(lev < -1 || lev >= DUMP_LEVELS) {
	error(_("error [planner askfor: lev out of range -1..%d: %d]"),
	      DUMP_LEVELS, lev);
	/*NOTREACHED*/
    }

    if (lev == -1) {
	ep->estimate[seq].level = -1;
	ep->estimate[seq].dumpdate = (char *)0;
	ep->estimate[seq].nsize = (gint64)-3;
	ep->estimate[seq].csize = (gint64)-3;
	ep->estimate[seq].guessed = 0;
	return;
    }

    ep->estimate[seq].level = lev;

    ep->estimate[seq].dumpdate = stralloc(get_dumpdate(info,lev));

    ep->estimate[seq].nsize = (gint64)-3;
    ep->estimate[seq].csize = (gint64)-3;
    ep->estimate[seq].guessed = 0;

    return;
}

static void
setup_estimate(
     disk_t *dp)
{
    est_t *ep;
    info_t *info;
    int i;
    char *qname;
    int overwrite_runs;

    assert(dp && dp->host);

    qname = quote_string(dp->name);
    g_fprintf(stderr, _("%s: time %s: setting up estimates for %s:%s\n"),
		    get_pname(), walltime_str(curclock()),
		    dp->host->hostname, qname);

    /* get current information about disk */

    info = g_new0(info_t, 1);
    if(get_info(dp->host->hostname, dp->name, info)) {
	/* no record for this disk, make a note of it */
	log_add(L_INFO, _("Adding new disk %s:%s."), dp->host->hostname, qname);
    }

    if (dp->data_path == DATA_PATH_DIRECTTCP) {
	if (dp->compress != COMP_NONE) {
	    log_add(L_FAIL, _("%s %s %s 0 [Can't compress directtcp data-path]"),
		    dp->host->hostname, qname, planner_timestamp);
	    g_fprintf(stderr,_("%s:%s lev 0 skipped can't compress directtcp data-path\n"),
		      dp->host->hostname, qname);
	    amfree(qname);
	    return;
	}
	if (dp->encrypt != ENCRYPT_NONE) {
	    log_add(L_FAIL, _("%s %s %s 0 [Can't encrypt directtcp data-path]"),
		    dp->host->hostname, qname, planner_timestamp);
	    g_fprintf(stderr,_("%s:%s lev 0 skipped can't encrypt directtcp data-path\n"),
		      dp->host->hostname, qname);
	    amfree(qname);
	    return;
	}
	if (dp->to_holdingdisk == HOLD_REQUIRED) {
	    log_add(L_FAIL, _("%s %s %s 0 [Holding disk can't be use for directtcp data-path]"),
		    dp->host->hostname, qname, planner_timestamp);
	    g_fprintf(stderr,_("%s:%s lev 0 skipped Holding disk can't be use for directtcp data-path\n"),
		      dp->host->hostname, qname);
	    amfree(qname);
	    return;
	} else if (dp->to_holdingdisk == HOLD_AUTO) {
	    g_fprintf(stderr,_("%s:%s Disabling holding disk\n"),
		      dp->host->hostname, qname);
	    dp->to_holdingdisk = HOLD_NEVER;
	}
    }

    /* setup working data struct for disk */

    ep = alloc(SIZEOF(est_t));
    dp->up = (void *) ep;
    ep->info = info;
    ep->state = DISK_READY;
    ep->dump_priority = dp->priority;
    ep->errstr = 0;
    ep->promote = 0;
    ep->post_dle = 0;
    ep->degr_mesg = NULL;
    ep->dump_est = &default_one_est;
    ep->degr_est = &default_one_est;

    /* calculated fields */

    if (ISSET(info->command, FORCE_FULL)) {
	/* force a level 0, kind of like a new disk */
	if(dp->strategy == DS_NOFULL) {
	    /*
	     * XXX - Not sure what it means to force a no-full disk.  The
	     * purpose of no-full is to just dump changes relative to a
	     * stable base, for example root partitions that vary only
	     * slightly from a site-wide prototype.  Only the variations
	     * are dumped.
	     *
	     * If we allow a level 0 onto the Amanda cycle, then we are
	     * hosed when that tape gets re-used next.  Disallow this for
	     * now.
	     */
	    log_add(L_WARNING,
		    _("Cannot force full dump of %s:%s with no-full option."),
		    dp->host->hostname, qname);

	    /* clear force command */
	    CLR(info->command, FORCE_FULL);
	    ep->last_level = last_level(info);
	    ep->next_level0 = next_level0(dp, info);
	} else if (dp->strategy == DS_INCRONLY) {
	    log_add(L_WARNING,
		    _("Cannot force full dump of %s:%s with incronly option."),
		    dp->host->hostname, qname);

	    /* clear force command */
	    CLR(info->command, FORCE_FULL);
	    ep->last_level = last_level(info);
	    ep->next_level0 = next_level0(dp, info);
	} else {
	    ep->degr_mesg = _("Skipping: force-full disk can't be dumped in degraded mode");
	    ep->last_level = -1;
	    ep->next_level0 = -conf_dumpcycle;
	    log_add(L_INFO, _("Forcing full dump of %s:%s as directed."),
		    dp->host->hostname, qname);
	}
    }
    else if(dp->strategy == DS_NOFULL) {
	/* force estimate of level 1 */
	ep->last_level = 1;
	ep->next_level0 = next_level0(dp, info);
    }
    else {
	ep->last_level = last_level(info);
	ep->next_level0 = next_level0(dp, info);
    }

    /* adjust priority levels */

    /* warn if dump will be overwritten */
    if (ep->last_level > -1 && strlen(info->inf[0].label) > 0) {
	overwrite_runs = when_overwrite(info->inf[0].label);
	if(overwrite_runs == 0) {
	    log_add(L_WARNING, _("Last full dump of %s:%s "
		    "on tape %s overwritten on this run."),
		    dp->host->hostname, qname, info->inf[0].label);
	} else if(overwrite_runs <= RUNS_REDZONE) {
	    log_add(L_WARNING,
		    plural(_("Last full dump of %s:%s on tape %s overwritten in %d run."),
			   _("Last full dump of %s:%s on tape %s overwritten in %d runs."), overwrite_runs),
		    dp->host->hostname, qname, info->inf[0].label,
		    overwrite_runs);
	}
    }

    /* warn if last level 1 will be overwritten */
    if (ep->last_level > 1 && strlen(info->inf[1].label) > 0) {
	overwrite_runs = when_overwrite(info->inf[1].label);
	if(overwrite_runs == 0) {
	    log_add(L_WARNING, _("Last level 1 dump of %s:%s "
		    "on tape %s overwritten on this run, resetting to level 1"),
		    dp->host->hostname, qname, info->inf[1].label);
	    ep->last_level = 0;
	} else if(overwrite_runs <= RUNS_REDZONE) {
	    log_add(L_WARNING,
		    plural(_("Last level 1 dump of %s:%s on tape %s overwritten in %d run."),
			   _("Last level 1 dump of %s:%s on tape %s overwritten in %d runs."), overwrite_runs),
		    dp->host->hostname, qname, info->inf[1].label,
		    overwrite_runs);
	}
    }

    if(ep->next_level0 < 0) {
	g_fprintf(stderr,plural(_("%s:%s overdue %d day for level 0\n"),
			      _("%s:%s overdue %d days for level 0\n"),
			      (-ep->next_level0)),
		dp->host->hostname, qname, (-ep->next_level0));
	ep->dump_priority -= ep->next_level0;
    }
    else if (ISSET(info->command, FORCE_FULL))
	ep->dump_priority += 1;
    /* else XXX bump up the priority of incrementals that failed last night */

    /* handle external level 0 dumps */

    if(dp->skip_full && dp->strategy != DS_NOINC) {
	if(ep->next_level0 <= 0) {
	    /* update the date field */
	    info->inf[0].date = today;
	    CLR(info->command, FORCE_FULL);
	    ep->next_level0 += conf_dumpcycle;
	    ep->last_level = 0;
	    if(put_info(dp->host->hostname, dp->name, info)) {
		error(_("could not put info record for %s:%s: %s"),
		      dp->host->hostname, qname, strerror(errno));
		/*NOTREACHED*/
	    }
	    log_add(L_INFO, _("Skipping full dump of %s:%s today."),
		    dp->host->hostname, qname);
	    g_fprintf(stderr,_("%s:%s lev 0 skipped due to skip-full flag\n"),
		    dp->host->hostname, qname);
	    /* don't enqueue the disk */
	    askfor(ep, 0, -1, info);
	    askfor(ep, 1, -1, info);
	    askfor(ep, 2, -1, info);
	    g_fprintf(stderr, _("%s: SKIPPED %s %s 0 [skip-full]\n"),
		    get_pname(), dp->host->hostname, qname);
	    log_add(L_SUCCESS, _("%s %s %s 0 [skipped: skip-full]"),
		    dp->host->hostname, qname, planner_timestamp);
	    amfree(qname);
	    return;
	}

	if(ep->last_level == -1) {
	    /* probably a new disk, but skip-full means no full! */
	    ep->last_level = 0;
	}

	if(ep->next_level0 == 1) {
	    log_add(L_WARNING, _("Skipping full dump of %s:%s tomorrow."),
		    dp->host->hostname, qname);
	}
    }

    if(dp->strategy == DS_INCRONLY && ep->last_level == -1 && !ISSET(info->command, FORCE_FULL)) {
	/* don't enqueue the disk */
	askfor(ep, 0, -1, info);
	askfor(ep, 1, -1, info);
	askfor(ep, 2, -1, info);
	log_add(L_FAIL, _("%s %s 19000101 1 [Skipping incronly because no full dump were done]"),
		dp->host->hostname, qname);
	g_fprintf(stderr,_("%s:%s lev 1 skipped due to strategy incronly and no full dump were done\n"),
		dp->host->hostname, qname);
	amfree(qname);
	return;
    }

    /* handle "skip-incr" type archives */

    if(dp->skip_incr && ep->next_level0 > 0) {
	g_fprintf(stderr,_("%s:%s lev 1 skipped due to skip-incr flag\n"),
		dp->host->hostname, qname);
	/* don't enqueue the disk */
	askfor(ep, 0, -1, info);
	askfor(ep, 1, -1, info);
	askfor(ep, 2, -1, info);

	g_fprintf(stderr, _("%s: SKIPPED %s %s 1 [skip-incr]\n"),
		get_pname(), dp->host->hostname, qname);

	log_add(L_SUCCESS, _("%s %s %s 1 [skipped: skip-incr]"),
		dp->host->hostname, qname, planner_timestamp);
	amfree(qname);
	return;
    }

    if( ep->last_level == -1 && ep->next_level0 > 0 && 
	dp->strategy != DS_NOFULL && dp->strategy != DS_INCRONLY &&
	conf_reserve == 100) {
	log_add(L_WARNING, _("%s:%s mismatch: no tapelist record, "
		"but curinfo next_level0: %d."),
		dp->host->hostname, qname, ep->next_level0);
	ep->next_level0 = 0;
    }

    //if(ep->last_level == 0) ep->level_days = 0;
    //else ep->level_days = runs_at(info, ep->last_level);
    ep->level_days = runs_at(info, ep->last_level);
    ep->last_lev0size = info->inf[0].csize;

    ep->fullrate = perf_average(info->full.rate, 0.0);
    ep->incrrate = perf_average(info->incr.rate, 0.0);

    ep->fullcomp = perf_average(info->full.comp, dp->comprate[0]);
    ep->incrcomp = perf_average(info->incr.comp, dp->comprate[1]);

    /* determine which estimates to get */

    i = 0;

    if (dp->strategy == DS_NOINC ||
	(!dp->skip_full &&
	 (!ISSET(info->command, FORCE_BUMP) ||
	  dp->skip_incr ||
	  ep->last_level == -1))) {
	if(ISSET(info->command, FORCE_BUMP) && ep->last_level == -1) {
	    log_add(L_INFO,
		  _("Remove force-bump command of %s:%s because it's a new disk."),
		    dp->host->hostname, qname);
	}
	switch (dp->strategy) {
	case DS_STANDARD: 
	case DS_NOINC:
	    askfor(ep, i++, 0, info);
	    if (ep->last_level == -1)
		ep->degr_mesg = _("Skipping: new disk can't be dumped in degraded mode");
	    else
		ep->degr_mesg = _("Skipping: strategy NOINC can't be dumped in degraded mode");
	    if(dp->skip_full) {
		log_add(L_INFO, _("Ignoring skip-full for %s:%s "
			"because the strategy is NOINC."),
			dp->host->hostname, qname);
	    }
	    if(ISSET(info->command, FORCE_BUMP)) {
		log_add(L_INFO,
		 _("Ignoring FORCE_BUMP for %s:%s because the strategy is NOINC."),
			dp->host->hostname, qname);
	    }
	    
	    break;

	case DS_NOFULL:
	    break;

	case DS_INCRONLY:
	    if (ISSET(info->command, FORCE_FULL))
		ep->last_level = 0;
	    break;
	}
    }

    if(!dp->skip_incr && !(dp->strategy == DS_NOINC)) {
	if(ep->last_level == -1) {		/* a new disk */
	    if (ep->degr_mesg == NULL)
		ep->degr_mesg = _("Skipping: new disk can't be dumped in degraded mode");
	    if(dp->strategy == DS_NOFULL || dp->strategy == DS_INCRONLY) {
		askfor(ep, i++, 1, info);
	    } else {
		assert(!dp->skip_full);		/* should be handled above */
	    }
	} else {				/* not new, pick normally */
	    int curr_level;

	    curr_level = ep->last_level;

	    if (ISSET(info->command, FORCE_NO_BUMP)) {
		if(curr_level > 0) { /* level 0 already asked for */
		    askfor(ep, i++, curr_level, info);
		}
		log_add(L_INFO,_("Preventing bump of %s:%s as directed."),
			dp->host->hostname, qname);
		ep->degr_mesg = _("Skipping: force-no-bump disk can't be dumped in degraded mode");
	    } else if (ISSET(info->command, FORCE_BUMP)
		       && curr_level + 1 < DUMP_LEVELS) {
		askfor(ep, i++, curr_level+1, info);
		log_add(L_INFO,_("Bumping of %s:%s at level %d as directed."),
			dp->host->hostname, qname, curr_level+1);
		ep->degr_mesg = _("Skipping: force-bump disk can't be dumped in degraded mode");
	    } else if (curr_level == 0) {
		askfor(ep, i++, 1, info);
	    } else {
		askfor(ep, i++, curr_level, info);
		/*
		 * If last time we dumped less than the threshold, then this
		 * time we will too, OR the extra size will be charged to both
		 * cur_level and cur_level + 1, so we will never bump.  Also,
		 * if we haven't been at this level 2 days, or the dump failed
		 * last night, we can't bump.
		 */
		if((info->inf[curr_level].size == (gint64)0 || /* no data, try it anyway */
		    (((info->inf[curr_level].size > bump_thresh(curr_level, info->inf[0].size,dp->bumppercent, dp->bumpsize, dp->bumpmult)))
		     && ep->level_days >= dp->bumpdays))
		   && curr_level + 1 < DUMP_LEVELS) {
		    askfor(ep, i++, curr_level+1, info);
		}
	    }
	}
    }

    while(i < MAX_LEVELS)	/* mark end of estimates */
	askfor(ep, i++, -1, info);

    /* debug output */

    g_fprintf(stderr, _("setup_estimate: %s:%s: command %u, options: %s    "
	    "last_level %d next_level0 %d level_days %d    getting estimates "
	    "%d (%lld) %d (%lld) %d (%lld)\n"),
	    dp->host->hostname, qname, info->command,
	    dp->strategy == DS_NOFULL ? "no-full" :
		 dp->strategy == DS_INCRONLY ? "incr-only" :
		 dp->skip_full ? "skip-full" :
		 dp->skip_incr ? "skip-incr" : "none",
	    ep->last_level, ep->next_level0, ep->level_days,
	    ep->estimate[0].level, (long long)ep->estimate[0].nsize,
	    ep->estimate[1].level, (long long)ep->estimate[1].nsize,
	    ep->estimate[2].level, (long long)ep->estimate[2].nsize);

    assert(ep->estimate[0].level != -1);
    enqueue_disk(&startq, dp);
    amfree(qname);
}

static int when_overwrite(
    char *label)
{
    tape_t *tp;
    int runtapes;

    runtapes = conf_runtapes;
    if(runtapes == 0) runtapes = 1;

    if((tp = lookup_tapelabel(label)) == NULL)
	return 1;	/* "shouldn't happen", but trigger warning message */
    else if(tp->reuse == 0)
	return 1024;
    else if(lookup_nb_tape() > conf_tapecycle)
	return (lookup_nb_tape() - tp->position) / runtapes;
    else
	return (conf_tapecycle - tp->position) / runtapes;
}

/* Return the estimated size for a particular dump */
static one_est_t *
est_for_level(
    disk_t *dp,
    int level)
{
    int i;

    if (level < 0 || level >= DUMP_LEVELS)
	return &default_one_est;

    for (i = 0; i < MAX_LEVELS; i++) {
        if (level == est(dp)->estimate[i].level) {
            if (est(dp)->estimate[i].csize <= -1) {
                est_csize(dp, &est(dp)->estimate[i]);
            }
	    return &est(dp)->estimate[i];
	}
    }
    return &default_one_est;
}

/* Return the estimated on-tape size of a particular dump */
static void
est_csize(
    disk_t    *dp,
    one_est_t *one_est)
{
    gint64 size = one_est->nsize;
    double ratio;

    if (dp->compress == COMP_NONE) {
        one_est->csize = one_est->nsize;
        return;
    }

    if (one_est->level == 0) ratio = est(dp)->fullcomp;
    else ratio = est(dp)->incrcomp;

    /*
     * make sure over-inflated compression ratios don't throw off the
     * estimates, this is mostly for when you have a small dump getting
     * compressed which takes up alot more disk/tape space relatively due
     * to the overhead of the compression.  This is specifically for
     * Digital Unix vdump.  This patch is courtesy of Rudolf Gabler
     * (RUG@USM.Uni-Muenchen.DE)
     */

    if (ratio > 1.1) ratio = 1.1;

    size = (gint64)((double)size * ratio);

    /*
     * Ratio can be very small in some error situations, so make sure
     * size goes back greater than zero.  It may not be right, but
     * indicates we did get an estimate.
     */
    if (size <= (gint64)0) {
	size = (gint64)1;
    }

    one_est->csize = size;
}

static gint64 est_tape_size(
    disk_t *dp,
    int level)
{
    one_est_t *dump_est;

    dump_est = est_for_level(dp, level);
    if (dump_est->level >= 0 && dump_est->csize <= -1)
	est_csize(dp, dump_est);
    return dump_est->csize;
}


/* what was the level of the last successful dump to tape? */
static int last_level(
    info_t *info)
{
    int min_pos, min_level, i;
    time_t lev0_date, last_date;
    tape_t *tp;

    if(info->last_level != -1)
	return info->last_level;

    /* to keep compatibility with old infofile */
    min_pos = 1000000000;
    min_level = -1;
    lev0_date = EPOCH;
    last_date = EPOCH;
    for(i = 0; i < 9; i++) {
	if(conf_reserve < 100) {
	    if(i == 0) lev0_date = info->inf[0].date;
	    else if(info->inf[i].date < lev0_date) continue;
	    if(info->inf[i].date > last_date) {
		last_date = info->inf[i].date;
		min_level = i;
	    }
	}
	else {
	    if((tp = lookup_tapelabel(info->inf[i].label)) == NULL) continue;
	    /* cull any entries from previous cycles */
	    if(i == 0) lev0_date = info->inf[0].date;
	    else if(info->inf[i].date < lev0_date) continue;

	    if(tp->position < min_pos) {
		min_pos = tp->position;
		min_level = i;
	    }
	}
    }
    info->last_level = i;
    return min_level;
}

/* when is next level 0 due? 0 = today, 1 = tomorrow, etc*/
static int
next_level0(
    disk_t *dp,
    info_t *info)
{
    if(dp->strategy == DS_NOFULL || dp->strategy == DS_INCRONLY)
	return 1;		/* fake it */
    else if (dp->strategy == DS_NOINC)
	return 0;
    else if(info->inf[0].date < (time_t)0)
	return -days_diff(EPOCH, today);	/* new disk */
    else
	return dp->dumpcycle - days_diff(info->inf[0].date, today);
}

/* how many runs at current level? */
static int runs_at(
    info_t *info,
    int lev)
{
    tape_t *cur_tape, *old_tape;
    int last, nb_runs;

    last = last_level(info);
    if(lev != last) return 0;
    if(info->consecutive_runs != -1)
	return info->consecutive_runs;
    if(lev == 0) return 1;

    /* to keep compatibility with old infofile */
    cur_tape = lookup_tapelabel(info->inf[lev].label);
    old_tape = lookup_tapelabel(info->inf[lev-1].label);
    if(cur_tape == NULL || old_tape == NULL) return 0;

    if(conf_runtapes == 0)
	nb_runs = (old_tape->position - cur_tape->position) / 1;
    else
	nb_runs = (old_tape->position - cur_tape->position) / conf_runtapes;
    info->consecutive_runs = nb_runs;

    return nb_runs;
}


static gint64 bump_thresh(
    int level,
    gint64 size_level_0,
    int bumppercent,
    gint64 bumpsize,
    double bumpmult)
{
    double bump;

    if ((bumppercent != 0) && (size_level_0 > (gint64)1024)) {
	bump = ((double)size_level_0 * (double)bumppercent) / 100.0;
    }
    else {
	bump = (double)bumpsize;
    }
    while(--level) bump = bump * bumpmult;

    return (gint64)bump;
}



/*
 * ========================================================================
 * GET REMOTE DUMP SIZE ESTIMATES
 *
 */

static void getsize(am_host_t *hostp);
static disk_t *lookup_hostdisk(am_host_t *hp, char *str);
static void handle_result(void *datap, pkt_t *pkt, security_handle_t *sech);


static void get_estimates(void)
{
    am_host_t *hostp;
    disk_t *dp, *dp1;
    int something_started;

    something_started = 1;
    while(something_started) {
	something_started = 0;
	for(dp = startq.head; dp != NULL; dp = dp->next) {
	    hostp = dp->host;
	    if(hostp->up == HOST_READY) {
		something_started = 1;
		run_server_host_scripts(EXECUTE_ON_PRE_HOST_ESTIMATE,
					get_config_name(), hostp);
		for(dp1 = hostp->disks; dp1 != NULL; dp1 = dp1->hostnext) {
		    if (dp1->todo)
			run_server_dle_scripts(EXECUTE_ON_PRE_DLE_ESTIMATE,
					   get_config_name(), dp1,
					   est(dp1)->estimate[0].level);
		}
		getsize(hostp);
		protocol_check();
		/*
		 * dp is no longer on startq, so dp->next is not valid
		 * and we have to start all over.
		 */
		break;
	    }
	}
    }
    protocol_run();

    while(!empty(waitq)) {
	disk_t *dp = dequeue_disk(&waitq);
	est(dp)->errstr = _("hmm, disk was stranded on waitq");
	enqueue_disk(&failq, dp);
    }

    while(!empty(pestq)) {
	disk_t *dp = dequeue_disk(&pestq);
	char *  qname = quote_string(dp->name);
	int i;

	for (i=0; i < MAX_LEVELS; i++) {
	    if (est(dp)->estimate[i].level != -1 &&
	        est(dp)->estimate[i].nsize < (gint64)0) {
	        if (est(dp)->estimate[i].nsize == (gint64)-3) {
		    log_add(L_WARNING,
			    _("disk %s:%s, estimate of level %d timed out."),
			    dp->host->hostname, qname, est(dp)->estimate[i].level);
	        }
	        est(dp)->estimate[i].level = -1;
            }
	}

	if ((est(dp)->estimate[0].level != -1 &&
	     est(dp)->estimate[0].nsize > (gint64)0) ||
	    (est(dp)->estimate[1].level != -1 &&
             est(dp)->estimate[1].nsize > (gint64)0) ||
	    (est(dp)->estimate[2].level != -1 &&
             est(dp)->estimate[2].nsize > (gint64)0)) {
	    enqueue_disk(&estq, dp);
	}
	else {
	   est(dp)->errstr = vstralloc("disk ", qname,
				       _(", all estimate timed out"), NULL);
	   enqueue_disk(&failq, dp);
	}
	amfree(qname);
    }
}

static void getsize(
    am_host_t *hostp)
{
    char	number[NUM_STR_SIZE], *req;
    disk_t *	dp;
    int		i;
    time_t	estimates, timeout;
    size_t	req_len;
    const	security_driver_t *secdrv;
    char *	calcsize;
    char *	qname, *b64disk = NULL;
    char *	qdevice, *b64device = NULL;
    estimate_t     estimate;
    estimatelist_t el;

    assert(hostp->disks != NULL);

    if(hostp->up != HOST_READY) {
	return;
    }

    /*
     * The first time through here we send a "noop" request.  This will
     * return the feature list from the client if it supports that.
     * If it does not, handle_result() will set the feature list to an
     * empty structure.  In either case, we do the disks on the second
     * (and subsequent) pass(es).
     */
    if(hostp->features != NULL) { /* sendsize service */
	int nb_client = 0;
	int nb_server = 0;

	int has_features = am_has_feature(hostp->features,
					  fe_req_options_features);
	int has_hostname = am_has_feature(hostp->features,
					  fe_req_options_hostname);
	int has_maxdumps = am_has_feature(hostp->features,
					  fe_req_options_maxdumps);
	int has_config   = am_has_feature(hostp->features,
					  fe_req_options_config);

	g_snprintf(number, SIZEOF(number), "%d", hostp->maxdumps);
	req = vstralloc("SERVICE ", "sendsize", "\n",
			"OPTIONS ",
			has_features ? "features=" : "",
			has_features ? our_feature_string : "",
			has_features ? ";" : "",
			has_maxdumps ? "maxdumps=" : "",
			has_maxdumps ? number : "",
			has_maxdumps ? ";" : "",
			has_hostname ? "hostname=" : "",
			has_hostname ? hostp->hostname : "",
			has_hostname ? ";" : "",
			has_config   ? "config=" : "",
			has_config   ? get_config_name() : "",
			has_config   ? ";" : "",
			"\n",
			NULL);
	req_len = strlen(req);
	req_len += 128;			/* room for SECURITY ... */
	estimates = 0;
	for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	    char *s = NULL;
	    char *es;
	    size_t s_len = 0;
	    GPtrArray *errarray;

	    if(dp->todo == 0) continue;

	    if(est(dp)->state != DISK_READY) continue;

	    est(dp)->got_estimate = 0;
	    if (est(dp)->estimate[0].level == -1) {
		est(dp)->state = DISK_DONE;
		continue;
	    }

	    qname = quote_string(dp->name);

	    errarray = validate_optionstr(dp);
	    if (errarray->len > 0) {
		guint i;
		for (i=0; i < errarray->len; i++) {
		    log_add(L_FAIL, _("%s %s %s 0 [%s]"),
			    dp->host->hostname, qname,
			    planner_timestamp,
			    (char *)g_ptr_array_index(errarray, i));
		}
		amfree(qname);
		est(dp)->state = DISK_DONE;
		continue;
	    }
		    
	    b64disk = amxml_format_tag("disk", dp->name);
	    qdevice = quote_string(dp->device);
	    estimate = (estimate_t)GPOINTER_TO_INT(dp->estimatelist->data);
	    if (dp->device)
		b64device = amxml_format_tag("diskdevice", dp->device);

	    estimate = ES_CLIENT;
	    for (el = dp->estimatelist; el != NULL; el = el->next) {
		estimate = (estimate_t)GPOINTER_TO_INT(el->data);
		if (estimate == ES_SERVER)
		    break;
	    }
	    if (estimate == ES_SERVER) {
		info_t info;
		nb_server++;
		get_info(dp->host->hostname, dp->name, &info);
		for(i = 0; i < MAX_LEVELS; i++) {
		    int lev = est(dp)->estimate[i].level;

		    if(lev == -1) break;
		    server_estimate(dp, i, &info, lev);
		}
		g_fprintf(stderr,_("%s time %s: got result for host %s disk %s:"),
			get_pname(), walltime_str(curclock()),
			dp->host->hostname, qname);
		g_fprintf(stderr,_(" %d -> %lldK, %d -> %lldK, %d -> %lldK\n"),
			  est(dp)->estimate[0].level,
			  (long long)est(dp)->estimate[0].nsize,
			  est(dp)->estimate[1].level,
                          (long long)est(dp)->estimate[1].nsize,
			  est(dp)->estimate[2].level,
                          (long long)est(dp)->estimate[2].nsize);
		if (!am_has_feature(hostp->features, fe_xml_estimate)) {
		    est(dp)->state = DISK_DONE;
		    remove_disk(&startq, dp);
		    enqueue_disk(&estq, dp);
		}
	    }

	    estimate = ES_SERVER;
	    for (el = dp->estimatelist; el != NULL; el = el->next) {
		estimate = (estimate_t)GPOINTER_TO_INT(el->data);
		if (estimate == ES_CLIENT || estimate == ES_CALCSIZE)
		    break;
	    }
	    if (estimate == ES_CLIENT ||
	        estimate == ES_CALCSIZE ||
		(am_has_feature(hostp->features, fe_req_xml) &&
		 am_has_feature(hostp->features, fe_xml_estimate))) {
		nb_client++;
		i = 0;

		if (am_has_feature(hostp->features, fe_req_xml)) {
		    char *levelstr = NULL;
		    char *spindlestr = NULL;
		    char level[NUM_STR_SIZE];
		    char spindle[NUM_STR_SIZE];
		    char *o;
		    char *l;
		    info_t info;

		    get_info(dp->host->hostname, dp->name, &info);
		    for(i = 0; i < MAX_LEVELS; i++) {
			char *server;
			int lev = est(dp)->estimate[i].level;
			if (lev == -1) break;
			g_snprintf(level, SIZEOF(level), "%d", lev);
			if (am_has_feature(hostp->features, fe_xml_level_server) &&
			    server_can_do_estimate(dp, &info, lev)) {
			    server = "<server>YES</server>";
			} else {
			    server = "";
			}
			vstrextend(&levelstr, "  <level>",
				   level, server,
				   "</level>\n", NULL);
		    }
		    g_snprintf(spindle, SIZEOF(spindle), "%d", dp->spindle);
		    spindlestr = vstralloc("  <spindle>",
					   spindle,
					   "</spindle>\n", NULL);
		    o = xml_optionstr(dp, 0);
		    
		    if (strcmp(dp->program,"DUMP") == 0 ||
			strcmp(dp->program,"GNUTAR") == 0) {
			l = vstralloc("<dle>\n",
				      "  <program>",
				      dp->program,
				      "</program>\n", NULL);
		    } else {
			l = vstralloc("<dle>\n",
				      "  <program>APPLICATION</program>\n",
				      NULL);
			if (dp->application) {
			    application_t *application;
			    char *xml_app;

			    application = lookup_application(dp->application);
			    g_assert(application != NULL);
			    xml_app = xml_application(dp, application,
						      hostp->features);
			    vstrextend(&l, xml_app, NULL);
			    amfree(xml_app);
			}
		    }

		    es = xml_estimate(dp->estimatelist, hostp->features);
		    vstrextend(&l, es, "\n", NULL);
		    amfree(es);
		    vstrextend(&l, "  ", b64disk, "\n", NULL);
		    if (dp->device)
			vstrextend(&l, "  ", b64device, "\n", NULL);
		    vstrextend(&l, levelstr, spindlestr, o, "</dle>\n", NULL);
		    strappend(s, l);
		    s_len += strlen(l);
		    amfree(l);
		    amfree(levelstr);
		    amfree(spindlestr);
		    amfree(o);
		} else if (strcmp(dp->program,"DUMP") != 0 &&
			   strcmp(dp->program,"GNUTAR") != 0) {
		    est(dp)->errstr = newvstrallocf(est(dp)->errstr,
					_("does not support application-api"));
		} else {
		    for(i = 0; i < MAX_LEVELS; i++) {
			char *l;
			char *exclude1 = "";
			char *exclude2 = "";
			char *excludefree = NULL;
			char *include1 = "";
			char *include2 = "";
			char *includefree = NULL;
			char spindle[NUM_STR_SIZE];
			char level[NUM_STR_SIZE];
			int lev = est(dp)->estimate[i].level;

			if(lev == -1) break;

			g_snprintf(level, SIZEOF(level), "%d", lev);
			g_snprintf(spindle, SIZEOF(spindle), "%d", dp->spindle);
			if (am_has_feature(hostp->features,
					   fe_sendsize_req_options)){
			    exclude1 = " OPTIONS |";
			    exclude2 = optionstr(dp);
			    if ( exclude2 == NULL ) {
				error(_("problem with option string, check the dumptype definition.\n"));
			    }
			    excludefree = exclude2;
			    includefree = NULL;
			} else {
			    if (dp->exclude_file &&
				dp->exclude_file->nb_element == 1) {
				exclude1 = " exclude-file=";
				exclude2 = quote_string(
						dp->exclude_file->first->name);
				excludefree = exclude2;
			    }
			    else if (dp->exclude_list &&
				     dp->exclude_list->nb_element == 1) {
				exclude1 = " exclude-list=";
				exclude2 = quote_string(
						dp->exclude_list->first->name);
				excludefree = exclude2;
			    }
			    if (dp->include_file &&
				dp->include_file->nb_element == 1) {
				include1 = " include-file=";
				include2 = quote_string(
						dp->include_file->first->name);
				includefree = include2;
			    }
			    else if (dp->include_list &&
				     dp->include_list->nb_element == 1) {
				include1 = " include-list=";
				include2 = quote_string(
						dp->include_list->first->name);
				includefree = include2;
			    }
			}

			if (estimate == ES_CALCSIZE &&
		   	    !am_has_feature(hostp->features,
					    fe_calcsize_estimate)) {
			    log_add(L_WARNING,
				    _("%s:%s does not support CALCSIZE for estimate, using CLIENT.\n"),
				    hostp->hostname, qname);
			    estimate = ES_CLIENT;
			}
			if(estimate == ES_CLIENT)
			    calcsize = "";
			else
			    calcsize = "CALCSIZE ";

			l = vstralloc(calcsize,
				      dp->program,
				      " ", qname,
				      " ", dp->device ? qdevice : "",
				      " ", level,
				      " ", est(dp)->estimate[i].dumpdate,
				      " ", spindle,
				      " ", exclude1, exclude2,
				      ((includefree != NULL) ? " " : ""),
				        include1, include2,
				      "\n",
				      NULL);
			strappend(s, l);
			s_len += strlen(l);
			amfree(l);
			amfree(includefree);
			amfree(excludefree);
		    }
		}
		if (s != NULL) {
		    estimates += i;
		    strappend(req, s);
		    req_len += s_len;
		    amfree(s);
		    if (est(dp)->state == DISK_DONE) {
		        remove_disk(&estq, dp);
		        est(dp)->state = DISK_PARTIALY_DONE;
			enqueue_disk(&pestq, dp);
		    } else {
		        remove_disk(&startq, dp);
		        est(dp)->state = DISK_ACTIVE;
		    }
		} else if (est(dp)->state != DISK_DONE) {
		    remove_disk(&startq, dp);
		    est(dp)->state = DISK_DONE;
		    if (est(dp)->errstr == NULL) {
			est(dp)->errstr = vstrallocf(
	                                        _("Can't request estimate"));
		    }
		    enqueue_disk(&failq, dp);
		}
	    }
	    amfree(b64disk);
	    amfree(b64device);
	    amfree(qname);
	    amfree(qdevice);
	}

	if(estimates == 0) {
	    amfree(req);
	    hostp->up = HOST_DONE;
	    return;
	}

	if (conf_etimeout < 0) {
	    timeout = - conf_etimeout;
	} else {
	    timeout = estimates * conf_etimeout;
	}
    } else { /* noop service */
	req = vstralloc("SERVICE ", "noop", "\n",
			"OPTIONS ",
			"features=", our_feature_string, ";",
			"\n",
			NULL);
	/*
	 * We use ctimeout for the "noop" request because it should be
	 * very fast and etimeout has other side effects.
	 */
	timeout = (time_t)getconf_int(CNF_CTIMEOUT);
    }

    dbprintf(_("send request:\n----\n%s\n----\n\n"), req);
    secdrv = security_getdriver(hostp->disks->auth);
    if (secdrv == NULL) {
	hostp->up = HOST_DONE;
	log_add(L_ERROR,
		_("Could not find security driver '%s' for host '%s'"),
		hostp->disks->auth, hostp->hostname);
	amfree(req);
	return;
    }
    hostp->up = HOST_ACTIVE;

    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	if(dp->todo == 0) {
	    continue;
	}
	if(est(dp)->state == DISK_ACTIVE) {
	    est(dp)->errstr = NULL;
	    enqueue_disk(&waitq, dp);
	}
    }

    protocol_sendreq(hostp->hostname, secdrv, amhost_get_security_conf, 
	req, timeout, handle_result, hostp);

    amfree(req);
}

static disk_t *lookup_hostdisk(
    /*@keep@*/ am_host_t *hp,
    char *str)
{
    disk_t *dp;

    for(dp = hp->disks; dp != NULL; dp = dp->hostnext)
	if(strcmp(str, dp->name) == 0) return dp;

    return NULL;
}


static void handle_result(
    void *datap,
    pkt_t *pkt,
    security_handle_t *sech)
{
    int level, i;
    gint64 size;
    disk_t *dp;
    am_host_t *hostp;
    char *msg, msg_undo;
    char *remoterr, *errbuf = NULL;
    char *s;
    char *t;
    char *fp;
    char *line;
    int ch;
    int tch;
    char *qname;
    char *disk;
    long long size_;

    hostp = (am_host_t *)datap;
    hostp->up = HOST_READY;

    if (pkt == NULL) {
	if (strcmp(security_geterror(sech), "timeout waiting for REP") == 0) {
	    errbuf = vstrallocf("Some estimate timeout on %s, using server estimate if possible", hostp->hostname);
	} else {
	    errbuf = vstrallocf(_("Request to %s failed: %s"),
			hostp->hostname, security_geterror(sech));
	}
	goto error_return;
    }
    if (pkt->type == P_NAK) {
	s = pkt->body;
	if(strncmp_const_skip(s, "ERROR ", s, ch) == 0) {
	    ch = *s++;
	} else {
	    goto NAK_parse_failed;
	}
	skip_whitespace(s, ch);
	if(ch == '\0') goto NAK_parse_failed;
	remoterr = s - 1;
	if((s = strchr(remoterr, '\n')) != NULL) {
	    if(s == remoterr) goto NAK_parse_failed;
		*s = '\0';
	}
	if (strcmp(remoterr, "unknown service: noop") != 0
		&& strcmp(remoterr, "noop: invalid service") != 0) {
	    errbuf = vstralloc(hostp->hostname, " NAK: ", remoterr, NULL);
	    if(s) *s = '\n';
	    goto error_return;
	}
    }

    dbprintf(_("got reply:\n----\n%s\n----\n\n"), pkt->body);
    s = pkt->body;
    ch = *s++;
    while(ch) {
	line = s - 1;

	if(strncmp_const(line, "OPTIONS ") == 0) {
	    t = strstr(line, "features=");
	    if(t != NULL && (g_ascii_isspace((int)t[-1]) || t[-1] == ';')) {
		char *u = strchr(t, ';');
		if (u)
		   *u = '\0';
		t += SIZEOF("features=")-1;
		am_release_feature_set(hostp->features);
		if((hostp->features = am_string_to_feature(t)) == NULL) {
		    errbuf = vstrallocf(hostp->hostname,
				       _(": bad features value: %s\n"), line);
		    goto error_return;
		}
		if (u)
		   *u = ';';
	    }
	    skip_quoted_line(s, ch);
	    continue;
	}

	t = line;
	if ((strncmp_const_skip(t, "ERROR ", t, tch) == 0) ||
	    (strncmp_const_skip(t, "WARNING ", t, tch) == 0)) { 
	    fp = t - 1;
	    skip_whitespace(t, tch);
	    if (tch == '\n') {
		t[-1] = '\0';
	    }

	    /*
	     * If the "error" is that the "noop" service is unknown, it
	     * just means the client is "old" (does not support the servie).
	     * We can ignore this.
	     */
	    if(hostp->features == NULL
	       && pkt->type == P_NAK
	       && (strcmp(t - 1, "unknown service: noop") == 0
		   || strcmp(t - 1, "noop: invalid service") == 0)) {
		skip_quoted_line(s, ch);
		continue;
	    }
	    t = strchr(t,'\n');
	    if (t) /* truncate after the first line */
		 *t = '\0';
	    errbuf = vstralloc(hostp->hostname,
				   (pkt->type == P_NAK) ? "NAK " : "",
				   ": ",
				   fp,
				   NULL);
	    goto error_return;
	}

	msg = t = line;
	tch = *(t++);
	skip_quoted_string(t, tch);
	t[-1] = '\0';
	disk = unquote_string(msg);

	skip_whitespace(t, tch);

	if (sscanf(t - 1, "%d", &level) != 1) {
	    goto bad_msg;
	}

	skip_integer(t, tch);
	skip_whitespace(t, tch);

	dp = lookup_hostdisk(hostp, disk);
	if(dp == NULL) {
	    log_add(L_ERROR, _("%s: invalid reply from sendsize: `%s'\n"),
		    hostp->hostname, line);
	    goto bad_msg;
	}

	size = (gint64)-1;
	if (strncmp_const(t-1,"SIZE ") == 0) {
	    if (sscanf(t - 1, "SIZE %lld", &size_) != 1) {
		goto bad_msg;
	    }
	    size = (gint64)size_;
	} else if ((strncmp_const(t-1,"ERROR ") == 0) ||
		   (strncmp_const(t-1,"WARNING ") == 0)) {
	    skip_non_whitespace(t, tch);
	    skip_whitespace(t, tch);
	    msg = t-1;
	    skip_quoted_string(t,tch);
	    msg_undo = t[-1];
	    t[-1] = '\0';
	    if (pkt->type == P_REP && !est(dp)->errstr) {
		est(dp)->errstr = unquote_string(msg);
	    }
	    t[-1] = msg_undo;
	} else {
	    goto bad_msg;
	}

	amfree(disk);

	for (i = 0; i < MAX_LEVELS; i++) {
	    if (est(dp)->estimate[i].level == level) {
		if (size == (gint64)-2) {
		    est(dp)->estimate[i].nsize = -1; /* remove estimate */
		    est(dp)->estimate[i].guessed = 0;
		} else if (size > (gint64)-1) {
		    /* take the size returned by the client */
		    est(dp)->estimate[i].nsize = size;
		    est(dp)->estimate[i].guessed = 0;
		}
		break;
	    }
	}
	if (i == MAX_LEVELS && level > 0) {
			/* client always report level 0 for some error */
	    goto bad_msg;		/* this est wasn't requested */
	}
	est(dp)->got_estimate++;

	s = t;
	ch = tch;
	skip_quoted_line(s, ch);
    }

    if(hostp->up == HOST_READY && hostp->features == NULL) {
	/*
	 * The client does not support the features list, so give it an
	 * empty one.
	 */
	dbprintf(_("no feature set from host %s\n"), hostp->hostname);
	hostp->features = am_set_default_feature_set();
    }

    security_close_connection(sech, hostp->hostname);

    /* XXX what about disks that only got some estimates...  do we care? */
    /* XXX amanda 2.1 treated that case as a bad msg */

    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	if(dp->todo == 0) continue;
	if(est(dp)->state != DISK_ACTIVE &&
	   est(dp)->state != DISK_PARTIALY_DONE) continue;

	if(est(dp)->state == DISK_ACTIVE) {
	    remove_disk(&waitq, dp);
	}
	else if(est(dp)->state == DISK_PARTIALY_DONE) {
	    remove_disk(&pestq, dp);
	}

	if(pkt->type == P_REP) {
	    est(dp)->state = DISK_DONE;
	}
	else if(pkt->type == P_PREP) {
	    est(dp)->state = DISK_PARTIALY_DONE;
	}

	if (est(dp)->estimate[0].level == -1) continue;   /* ignore this disk */


	qname = quote_string(dp->name);
	if(pkt->type == P_PREP) {
		g_fprintf(stderr,_("%s: time %s: got partial result for host %s disk %s:"),
			get_pname(), walltime_str(curclock()),
			dp->host->hostname, qname);
		g_fprintf(stderr,_(" %d -> %lldK, %d -> %lldK, %d -> %lldK\n"),
			  est(dp)->estimate[0].level,
                          (long long)est(dp)->estimate[0].nsize,
			  est(dp)->estimate[1].level,
                          (long long)est(dp)->estimate[1].nsize,
			  est(dp)->estimate[2].level,
                          (long long)est(dp)->estimate[2].nsize);
	    enqueue_disk(&pestq, dp);
	}
	else if(pkt->type == P_REP) {
		g_fprintf(stderr,_("%s: time %s: got result for host %s disk %s:"),
			get_pname(), walltime_str(curclock()),
			dp->host->hostname, qname);
		g_fprintf(stderr,_(" %d -> %lldK, %d -> %lldK, %d -> %lldK\n"),
			  est(dp)->estimate[0].level,
                          (long long)est(dp)->estimate[0].nsize,
			  est(dp)->estimate[1].level,
                          (long long)est(dp)->estimate[1].nsize,
			  est(dp)->estimate[2].level,
                          (long long)est(dp)->estimate[2].nsize);
		if ((est(dp)->estimate[0].level != -1 &&
                     est(dp)->estimate[0].nsize > (gint64)0) ||
		    (est(dp)->estimate[1].level != -1 &&
                     est(dp)->estimate[1].nsize > (gint64)0) ||
		    (est(dp)->estimate[2].level != -1 &&
                     est(dp)->estimate[2].nsize > (gint64)0)) {

                    for (i=MAX_LEVELS-1; i >=0; i--) {
		        if (est(dp)->estimate[i].level != -1 &&
                            est(dp)->estimate[i].nsize < (gint64)0) {
			    est(dp)->estimate[i].level = -1;
		        }
		    }
		    enqueue_disk(&estq, dp);
	    }
	    else {
		enqueue_disk(&failq, dp);
		if(est(dp)->got_estimate && !est(dp)->errstr) {
		    est(dp)->errstr = vstrallocf("disk %s, all estimate failed",
						 qname);
		}
		else {
		    g_fprintf(stderr,
			 _("error result for host %s disk %s: missing estimate\n"),
		   	 dp->host->hostname, qname);
		    if (est(dp)->errstr == NULL) {
			est(dp)->errstr = vstrallocf(_("missing result for %s in %s response"),
						    qname, dp->host->hostname);
		    }
		}
	    }
	    hostp->up = HOST_DONE;
	}
	if (est(dp)->post_dle == 0 &&
	    (pkt->type == P_REP ||
	     ((est(dp)->estimate[0].level == -1 ||
               est(dp)->estimate[0].nsize > (gint64)0) &&
	      (est(dp)->estimate[1].level == -1 ||
               est(dp)->estimate[1].nsize > (gint64)0) &&
	      (est(dp)->estimate[2].level == -1 ||
               est(dp)->estimate[2].nsize > (gint64)0)))) {
	    run_server_dle_scripts(EXECUTE_ON_POST_DLE_ESTIMATE,
			       get_config_name(), dp,
                               est(dp)->estimate[0].level);
	    est(dp)->post_dle = 1;
	}
	amfree(qname);
    }

    if(hostp->up == HOST_DONE) {
	if (pkt->type == P_REP) {
	    run_server_host_scripts(EXECUTE_ON_POST_HOST_ESTIMATE,
				    get_config_name(), hostp);
	}
    }

    getsize(hostp);
    /* try to clean up any defunct processes, since Amanda doesn't wait() for
       them explicitly */
    while(waitpid(-1, NULL, WNOHANG)> 0);
    return;

 NAK_parse_failed:

    errbuf = vstrallocf(_("%s NAK: [NAK parse failed]"), hostp->hostname);
    g_fprintf(stderr, _("got strange nak from %s:\n----\n%s----\n\n"),
	    hostp->hostname, pkt->body);
    goto error_return;

 bad_msg:
    g_fprintf(stderr,_("got a bad message, stopped at:\n"));
    /*@ignore@*/
    g_fprintf(stderr,_("----\n%s----\n\n"), line);
    errbuf = stralloc2(_("badly formatted response from "), hostp->hostname);
    /*@end@*/

 error_return:
    i = 0;
    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	if (dp->todo) {
	    if(est(dp)->state == DISK_ACTIVE) {
		qname = quote_string(dp->name);
		est(dp)->state = DISK_DONE;
		remove_disk(&waitq, dp);
		enqueue_disk(&failq, dp);
		i++;

		est(dp)->errstr = stralloc(errbuf);
		g_fprintf(stderr, _("error result for host %s disk %s: %s\n"),
			  dp->host->hostname, qname, errbuf);
		amfree(qname);
	    }
	}
    }
    if(i == 0) {
	/*
	 * If there were no disks involved, make sure the error gets
	 * reported.
	 */
	log_add(L_ERROR, "%s", errbuf);
    }
    hostp->up = HOST_DONE;
    amfree(errbuf);
    /* try to clean up any defunct processes, since Amanda doesn't wait() for
       them explicitly */
    while(waitpid(-1, NULL, WNOHANG)> 0);
}




/*
 * ========================================================================
 * ANALYSE ESTIMATES
 *
 */

static int schedule_order(disk_t *a, disk_t *b);	  /* subroutines */
static one_est_t *pick_inclevel(disk_t *dp);

static void analyze_estimate(
    disk_t *dp)
{
    est_t *ep;
    info_t info;
    int have_info = 0;
    char *qname = quote_string(dp->name);

    ep = est(dp);

    g_fprintf(stderr, _("pondering %s:%s... "),
	    dp->host->hostname, qname);
    g_fprintf(stderr, _("next_level0 %d last_level %d "),
	    ep->next_level0, ep->last_level);

    if(get_info(dp->host->hostname, dp->name, &info) == 0) {
	have_info = 1;
    }

    ep->degr_est = &default_one_est;

    if (ep->next_level0 <= 0 || (have_info && ep->last_level == 0
       && (ISSET(info.command, FORCE_NO_BUMP)))) {
	if (ep->next_level0 <= 0) {
	    g_fprintf(stderr,_("(due for level 0) "));
	}
	ep->dump_est = est_for_level(dp, 0);
	if (ep->dump_est->csize <= (gint64)0) {
	    g_fprintf(stderr,
		    _("(no estimate for level 0, picking an incr level)\n"));
	    ep->dump_est = pick_inclevel(dp);

	    if (ep->dump_est->nsize == (gint64)-1) {
		ep->dump_est = est_for_level(dp, ep->dump_est->level + 1);
	    }
	}
	else {
	    total_lev0 += (double) ep->dump_est->csize;
	    if(ep->last_level == -1 || dp->skip_incr) {
		g_fprintf(stderr,_("(%s disk, can't switch to degraded mode)\n"),
			dp->skip_incr? "skip-incr":_("new"));
		if (dp->skip_incr  && ep->degr_mesg == NULL) {
		    ep->degr_mesg = _("Skpping: skip-incr disk can't be dumped in degraded mode");
		}
		ep->degr_est = &default_one_est;
	    }
	    else {
		/* fill in degraded mode info */
		g_fprintf(stderr,_("(picking inclevel for degraded mode)"));
		ep->degr_est = pick_inclevel(dp);
		if (ep->degr_est->level >= 0 &&
		    ep->degr_est->csize == (gint64)-1) {
                    ep->degr_est = est_for_level(dp, ep->degr_est->level + 1);
		}
		if (ep->degr_est->csize == (gint64)-1) {
		    g_fprintf(stderr,_("(no inc estimate)"));
		    if (ep->degr_mesg == NULL)
			ep->degr_mesg = _("Skipping: an incremental estimate could not be performed, so disk cannot be dumped in degraded mode");
		    ep->degr_est = &default_one_est;
		}
		g_fprintf(stderr,"\n");
	    }
	}
    }
    else {
	g_fprintf(stderr,_("(not due for a full dump, picking an incr level)\n"));
	/* XXX - if this returns -1 may be we should force a total? */
	ep->dump_est = pick_inclevel(dp);

	if (ep->dump_est->csize == (gint64)-1) {
	    ep->dump_est = est_for_level(dp, ep->last_level);
	}
	if (ep->dump_est->csize == (gint64)-1) {
	    ep->dump_est = est_for_level(dp, ep->last_level + 1);
	}
	if (ep->dump_est->csize == (gint64)-1) {
	    ep->dump_est = est_for_level(dp, 0);
	}
	if (ep->degr_mesg == NULL) {
	    ep->degr_mesg = _("Skipping: a full is not planned, so can't dump in degraded mode");
	}
    }

    if (ep->dump_est->level < 0) {
	int   i;
	char *q = quote_string("no estimate");

	g_fprintf(stderr,_("  no valid estimate\n"));
	for(i=0; i<MAX_LEVELS; i++) {
	    if (est(dp)->estimate[i].level >= 0) {
		g_fprintf(stderr,("    level: %d  nsize: %lld csize: %lld\n"),
			  est(dp)->estimate[i].level,
			  (long long)est(dp)->estimate[i].nsize,
			  (long long)est(dp)->estimate[i].csize);
	    }
	}
	log_add(L_WARNING, _("%s %s %s 0 %s"), dp->host->hostname, qname,
		planner_timestamp, q);
	amfree(q);
    }

    g_fprintf(stderr,_("  curr level %d nsize %lld csize %lld "),
              ep->dump_est->level, (long long)ep->dump_est->nsize,
              (long long)ep->dump_est->csize);

    insert_disk(&schedq, dp, schedule_order);

    total_size += (gint64)tt_blocksize_kb + ep->dump_est->csize + tape_mark;

    /* update the balanced size */
    if(!(dp->skip_full || dp->strategy == DS_NOFULL || 
	 dp->strategy == DS_INCRONLY)) {
	gint64 lev0size;

	lev0size = est_tape_size(dp, 0);
	if(lev0size == (gint64)-1) lev0size = ep->last_lev0size;

	if (dp->strategy == DS_NOINC) {
	    balanced_size += (double)lev0size;
	} else if (dp->dumpcycle == 0) {
	    balanced_size += (double)(lev0size * conf_dumpcycle / (gint64)runs_per_cycle);
	} else if (dp->dumpcycle != conf_dumpcycle) {
	    balanced_size += (double)(lev0size * (conf_dumpcycle / dp->dumpcycle) / (gint64)runs_per_cycle);
	} else {
	    balanced_size += (double)(lev0size / (gint64)runs_per_cycle);
	}
    }

    g_fprintf(stderr,_("total size %lld total_lev0 %1.0lf balanced-lev0size %1.0lf\n"),
	    (long long)total_size, total_lev0, balanced_size);

    /* Log errstr even if the estimate succeeded */
    /* It can be an error from a script          */
    if (est(dp)->errstr) {
	char *qerrstr = quote_string(est(dp)->errstr);
	/* Log only a warning if a server estimate is available */
	if (est(dp)->estimate[0].nsize > 0 ||
	    est(dp)->estimate[1].nsize > 0 ||
	    est(dp)->estimate[2].nsize > 0) {
	    log_add(L_WARNING, _("%s %s %s 0 %s"), dp->host->hostname, qname, 
		    planner_timestamp, qerrstr);
	} else {
	    log_add(L_FAIL, _("%s %s %s 0 %s"), dp->host->hostname, qname, 
		    planner_timestamp, qerrstr);
	}
	amfree(qerrstr);
    }

    amfree(qname);
}

static void handle_failed(
    disk_t *dp)
{
    char *errstr, *errstr1, *qerrstr;
    char *qname = quote_string(dp->name);

    errstr = est(dp)->errstr? est(dp)->errstr : _("hmm, no error indicator!");
    errstr1 = vstralloc("[",errstr,"]", NULL);
    qerrstr = quote_string(errstr1);
    amfree(errstr1);

    g_fprintf(stderr, _("%s: FAILED %s %s %s 0 %s\n"),
	get_pname(), dp->host->hostname, qname, planner_timestamp, qerrstr);

    log_add(L_FAIL, _("%s %s %s 0 %s"), dp->host->hostname, qname, 
	    planner_timestamp, qerrstr);

    amfree(qerrstr);
    amfree(qname);
    /* XXX - memory leak with *dp */
}


/*
 * insert-sort by decreasing priority, then
 * by decreasing size within priority levels.
 */

static int schedule_order(
    disk_t *a,
    disk_t *b)
{
    int diff;
    gint64 ldiff;

    diff = est(b)->dump_priority - est(a)->dump_priority;
    if(diff != 0) return diff;

    ldiff = est(b)->dump_est->csize - est(a)->dump_est->csize;
    if(ldiff < (gint64)0) return -1; /* XXX - there has to be a better way to dothis */
    if(ldiff > (gint64)0) return 1;
    return 0;
}


static one_est_t *pick_inclevel(
    disk_t *dp)
{
    one_est_t *level0_est, *base_est, *bump_est;
    gint64 thresh;
    char *qname;

    level0_est = est_for_level(dp, 0);
    base_est = est_for_level(dp, est(dp)->last_level);

    /* if last night was level 0, do level 1 tonight, no ifs or buts */
    if (base_est->level == 0) {
	g_fprintf(stderr,_("   picklev: last night 0, so tonight level 1\n"));
	return est_for_level(dp, 1);
    }

    /* if no-full option set, always do level 1 */
    if(dp->strategy == DS_NOFULL) {
	g_fprintf(stderr,_("   picklev: no-full set, so always level 1\n"));
	return est_for_level(dp, 1);
    }

    /* if we didn't get an estimate, we can't do an inc */
    if (base_est->nsize == (gint64)-1) {
	bump_est = est_for_level(dp, est(dp)->last_level + 1);
	if (bump_est->nsize > (gint64)0) { /* FORCE_BUMP */
	    g_fprintf(stderr,_("   picklev: bumping to level %d\n"), bump_est->level);
	    return bump_est;
	}
	g_fprintf(stderr,_("   picklev: no estimate for level %d, so no incs\n"), base_est->level);
	return base_est;
    }

    thresh = bump_thresh(base_est->level, level0_est->nsize, dp->bumppercent,
                         dp->bumpsize, dp->bumpmult);

    g_fprintf(stderr,
	    _("   pick: size %lld level %d days %d (thresh %lldK, %d days)\n"),
	    (long long)base_est->nsize, base_est->level, est(dp)->level_days,
	    (long long)thresh, dp->bumpdays);

    if(base_est->level == (DUMP_LEVELS - 1)
       || est(dp)->level_days < dp->bumpdays
       || base_est->nsize <= thresh)
	    return base_est;

    bump_est = est_for_level(dp, base_est->level + 1);

    if (bump_est->nsize == (gint64)-1)
        return base_est;

    g_fprintf(stderr, _("   pick: next size %lld... "),
              (long long)bump_est->nsize);

    if (base_est->nsize - bump_est->nsize < thresh) {
	g_fprintf(stderr, _("not bumped\n"));
	return base_est;
    }

    qname = quote_string(dp->name);
    g_fprintf(stderr, _("BUMPED\n"));
    log_add(L_INFO, _("Incremental of %s:%s bumped to level %d."),
	    dp->host->hostname, qname, bump_est->level);
    amfree(qname);

    return bump_est;
}




/*
** ========================================================================
** ADJUST SCHEDULE
**
** We have two strategies here:
**
** 1. Delay dumps
**
** If we are trying to fit too much on the tape something has to go.  We
** try to delay totals until tomorrow by converting them into incrementals
** and, if that is not effective enough, dropping incrementals altogether.
** While we are searching for the guilty dump (the one that is really
** causing the schedule to be oversize) we have probably trampled on a lot of
** innocent dumps, so we maintain a "before image" list and use this to
** put back what we can.
**
** 2. Promote dumps.
**
** We try to keep the amount of tape used by total dumps the same each night.
** If there is some spare tape in this run we have a look to see if any of
** tonights incrementals could be promoted to totals and leave us with a
** more balanced cycle.
*/

static void delay_one_dump(disk_t *dp, int delete, ...);
static int promote_highest_priority_incremental(void);
static int promote_hills(void);

/* delay any dumps that will not fit */
static void delay_dumps(void)
{
    disk_t *	dp;
    disk_t *	ndp;
    disk_t *	preserve;
    disk_t *	delayed_dp;
    bi_t *	bi;
    bi_t  *	nbi;
    gint64	new_total;		/* New total_size */
    char	est_kb[20];		/* Text formatted dump size */
    int		nb_forced_level_0;
    info_t	info;
    int		delete;
    char *	message;
    gint64	full_size;
    time_t      timestamps;
    int         priority;

    biq.head = biq.tail = NULL;

    /*
    ** 1. Delay dumps that are way oversize.
    **
    ** Dumps larger that the size of the tapes we are using are just plain
    ** not going to fit no matter how many other dumps we drop.  Delay
    ** oversize totals until tomorrow (by which time my owner will have
    ** resolved the problem!) and drop incrementals altogether.  Naturally
    ** a large total might be delayed into a large incremental so these
    ** need to be checked for separately.
    */

    for(dp = schedq.head; dp != NULL; dp = ndp) {
	int avail_tapes = 1;
	if (dp->splitsize > (gint64)0 || dp->allow_split)
	    avail_tapes = conf_runtapes;

	ndp = dp->next; /* remove_disk zaps this */

	full_size = est_tape_size(dp, 0);
	if (full_size > tapetype_get_length(tape) * (gint64)avail_tapes) {
	    char *qname = quote_string(dp->name);
	    if (conf_runtapes > 1 && dp->splitsize == (gint64)0) {
		log_add(L_WARNING, _("disk %s:%s, full dump (%lldKB) will be larger than available tape space"
			", you could define a splitsize"),
			dp->host->hostname, qname,
			(long long)full_size);
	    } else {
		log_add(L_WARNING, _("disk %s:%s, full dump (%lldKB) will be larger than available tape space"),
			dp->host->hostname, qname,
			(long long)full_size);
	    }
	    amfree(qname);
	}

	if (est(dp)->dump_est->csize == (gint64)-1 ||
	    est(dp)->dump_est->csize <= tapetype_get_length(tape) * (gint64)avail_tapes) {
	    continue;
	}

	/* Format dumpsize for messages */
	g_snprintf(est_kb, 20, "%lld KB,",
                   (long long)est(dp)->dump_est->csize);

	if(est(dp)->dump_est->level == 0) {
	    if(dp->skip_incr) {
		delete = 1;
		message = _("but cannot incremental dump skip-incr disk");
	    }
	    else if(est(dp)->last_level < 0) {
		delete = 1;
		message = _("but cannot incremental dump new disk");
	    }
	    else if(est(dp)->degr_est->level < 0) {
		delete = 1;
		message = _("but no incremental estimate");
	    }
	    else if (est(dp)->degr_est->csize > tapetype_get_length(tape)) {
		delete = 1;
		message = _("incremental dump also larger than tape");
	    }
	    else {
		delete = 0;
		message = _("full dump delayed, doing incremental");
	    }
	}
	else {
	    delete = 1;
	    message = _("skipping incremental");
	}
	delay_one_dump(dp, delete, _("dump larger than available tape space,"),
		       est_kb, message, NULL);
    }

    /*
    ** 2. Delay total dumps.
    **
    ** Delay total dumps until tomorrow (or the day after!).  We start with
    ** the lowest priority (most dispensable) and work forwards.  We take
    ** care not to delay *all* the dumps since this could lead to a stale
    ** mate [for any one disk there are only three ways tomorrows dump will
    ** be smaller than todays: 1. we do a level 0 today so tomorows dump
    ** will be a level 1; 2. the disk gets more data so that it is bumped
    ** tomorrow (this can be a slow process); and, 3. the disk looses some
    ** data (when does that ever happen?)].
    */

    nb_forced_level_0 = 0;
    preserve = NULL;
    timestamps = 2147483647;
    priority = 0;
    for(dp = schedq.head; dp != NULL; dp = dp->next) {
	if (est(dp)->dump_est->level == 0) {
	    if (!preserve ||
		est(dp)->dump_priority > priority ||
		(est(dp)->dump_priority == priority &&
		 est(dp)->info->inf[0].date < timestamps)) {
		priority = est(dp)->dump_priority;
		timestamps = est(dp)->info->inf[0].date;
		preserve = dp;
	    }
	}
    }

    /* 2.a. Do not delay forced full */
    delayed_dp = NULL;
    do {
	delayed_dp = 0;
	timestamps = 0;
	for(dp = schedq.tail;
		dp != NULL && total_size > tape_length;
		dp = ndp) {
	    ndp = dp->prev;

	    if(est(dp)->dump_est->level != 0) continue;

	    get_info(dp->host->hostname, dp->name, &info);
	    if(ISSET(info.command, FORCE_FULL)) {
		nb_forced_level_0 += 1;
		preserve = dp;
		continue;
	    }

	    if (dp != preserve &&
		est(dp)->info->inf[0].date > timestamps) {
		delayed_dp = dp;
		timestamps = est(dp)->info->inf[0].date;
	    }
	}
	if (delayed_dp) {
	    /* Format dumpsize for messages */
	    g_snprintf(est_kb, 20, "%lld KB,",
                       (long long)est(delayed_dp)->dump_est->csize);

	    if(delayed_dp->skip_incr) {
		delete = 1;
		message = _("but cannot incremental dump skip-incr disk");
	    }
	    else if(est(delayed_dp)->last_level < 0) {
		delete = 1;
		message = _("but cannot incremental dump new disk");
	    }
	    else if(est(delayed_dp)->degr_est->level < 0) {
		delete = 1;
		message = _("but no incremental estimate");
	    }
	    else {
		delete = 0;
		message = _("full dump delayed, doing incremental");
	    }
	    delay_one_dump(delayed_dp, delete, _("dumps too big,"), est_kb,
			   message, NULL);
	}
    } while (delayed_dp);

    /* 2.b. Delay forced full if needed */
    if(nb_forced_level_0 > 0 && total_size > tape_length) {
	for(dp = schedq.tail;
		dp != NULL && total_size > tape_length;
		dp = ndp) {
	    ndp = dp->prev;

	    if(est(dp)->dump_est->level == 0 && dp != preserve) {

		/* Format dumpsize for messages */
		g_snprintf(est_kb, 20, "%lld KB,",
                           (long long)est(dp)->dump_est->csize);

		if(dp->skip_incr) {
		    delete = 1;
		    message = _("but cannot incremental dump skip-incr disk");
		}
		else if(est(dp)->last_level < 0) {
		    delete = 1;
		    message = _("but cannot incremental dump new disk");
		}
		else if(est(dp)->degr_est->level < 0) {
		    delete = 1;
		    message = _("but no incremental estimate");
		}
		else {
		    delete = 0;
		    message = _("full dump delayed");
		}
		delay_one_dump(dp, delete, _("dumps too big,"), est_kb,
			       message, NULL);
	    }
	}
    }

    /*
    ** 3. Delay incremental dumps.
    **
    ** Delay incremental dumps until tomorrow.  This is a last ditch attempt
    ** at making things fit.  Again, we start with the lowest priority (most
    ** dispensable) and work forwards.
    */

    for(dp = schedq.tail;
	    dp != NULL && total_size > tape_length;
	    dp = ndp) {
	ndp = dp->prev;

	if(est(dp)->dump_est->level != 0) {

	    /* Format dumpsize for messages */
	    g_snprintf(est_kb, 20, "%lld KB,",
                       (long long)est(dp)->dump_est->csize);

	    delay_one_dump(dp, 1,
			   _("dumps way too big,"),
			   est_kb,
			   _("must skip incremental dumps"),
			   NULL);
	}
    }

    /*
    ** 4. Reinstate delayed dumps.
    **
    ** We might not have needed to stomp on all of the dumps we have just
    ** delayed above.  Try to reinstate them all starting with the last one
    ** and working forwards.  It is unlikely that the last one will fit back
    ** in but why complicate the code?
    */

/*@i@*/ for(bi = biq.tail; bi != NULL; bi = nbi) {
	int avail_tapes = 1;
	nbi = bi->prev;
	dp = bi->dp;
	if(dp->splitsize > (gint64)0)
	    avail_tapes = conf_runtapes;

	if(bi->deleted) {
	    new_total = total_size + (gint64)tt_blocksize_kb +
			bi->csize + (gint64)tape_mark;
	} else {
	    new_total = total_size - est(dp)->dump_est->csize + bi->csize;
	}
	if((new_total <= tape_length) &&
	  (bi->csize < (tapetype_get_length(tape) * (gint64)avail_tapes))) {
	    /* reinstate it */
	    total_size = new_total;
	    if(bi->deleted) {
		if(bi->level == 0) {
		    total_lev0 += (double) bi->csize;
		}
		insert_disk(&schedq, dp, schedule_order);
	    }
	    else {
		est(dp)->dump_est = est_for_level(dp, bi->level);
	    }

	    /* Keep it clean */
	    if(bi->next == NULL)
		biq.tail = bi->prev;
	    else
		(bi->next)->prev = bi->prev;
	    if(bi->prev == NULL)
		biq.head = bi->next;
	    else
		(bi->prev)->next = bi->next;

	    amfree(bi->errstr);
	    amfree(bi);
	}
    }

    /*
    ** 5. Output messages about what we have done.
    **
    ** We can't output messages while we are delaying dumps because we might
    ** reinstate them later.  We remember all the messages and output them
    ** now.
    */

/*@i@*/ for(bi = biq.head; bi != NULL; bi = nbi) {
	nbi = bi->next;
	if(bi->deleted) {
	    g_fprintf(stderr, "%s: FAILED %s\n", get_pname(), bi->errstr);
	    log_add(L_FAIL, "%s", bi->errstr);
	}
	else {
	    dp = bi->dp;
	    g_fprintf(stderr, _("  delay: %s now at level %d\n"),
		bi->errstr, est(dp)->dump_est->level);
	    log_add(L_INFO, "%s", bi->errstr);
	}
	/*@ignore@*/
	amfree(bi->errstr);
	amfree(bi);
	/*@end@*/
    }

    g_fprintf(stderr, _("  delay: Total size now %lld.\n"),
    	     (long long)total_size);

    return;
}


/*
 * Remove a dump or modify it from full to incremental.
 * Keep track of it on the bi q in case we can add it back later.
 */
arglist_function1(
    static void delay_one_dump,
    disk_t *, dp,
    int, delete)
{
    bi_t *bi;
    va_list argp;
    char level_str[NUM_STR_SIZE];
    char *sep;
    char *next;
    char *qname = quote_string(dp->name);
    char *errstr, *qerrstr;

    arglist_start(argp, delete);

    total_size -= (gint64)tt_blocksize_kb + est(dp)->dump_est->csize + (gint64)tape_mark;
    if(est(dp)->dump_est->level == 0) {
	total_lev0 -= (double) est(dp)->dump_est->csize;
    }

    bi = alloc(SIZEOF(bi_t));
    bi->next = NULL;
    bi->prev = biq.tail;
    if(biq.tail == NULL)
	biq.head = bi;
    else
	biq.tail->next = bi;
    biq.tail = bi;

    bi->deleted = delete;
    bi->dp = dp;
    bi->level = est(dp)->dump_est->level;
    bi->nsize = est(dp)->dump_est->nsize;
    bi->csize = est(dp)->dump_est->csize;

    g_snprintf(level_str, SIZEOF(level_str), "%d", est(dp)->dump_est->level);
    bi->errstr = vstralloc(dp->host->hostname,
			   " ", qname,
			   " ", planner_timestamp ? planner_timestamp : "?",
			   " ", level_str,
			   NULL);
    errstr = NULL;
    sep = "[";
    while ((next = arglist_val(argp, char *)) != NULL) {
	vstrextend(&errstr, sep, next, NULL);
	sep = " ";
    }
    strappend(errstr, "]");
    qerrstr = quote_string(errstr);
    vstrextend(&bi->errstr, " ", qerrstr, NULL);
    amfree(errstr);
    amfree(qerrstr);
    arglist_end(argp);

    if (delete) {
	remove_disk(&schedq, dp);
    } else {
	est(dp)->dump_est = est(dp)->degr_est;
	total_size += (gint64)tt_blocksize_kb + est(dp)->dump_est->csize + (gint64)tape_mark;
    }
    amfree(qname);
    return;
}


static int promote_highest_priority_incremental(void)
{
    disk_t *dp, *dp1, *dp_promote;
    gint64 new_total, new_lev0;
    int check_days;
    int nb_today, nb_same_day, nb_today2;
    int nb_disk_today, nb_disk_same_day;
    char *qname;

    /*
     * return 1 if did so; must update total_size correctly; must not
     * cause total_size to exceed tape_length
     */

    dp_promote = NULL;
    for(dp = schedq.head; dp != NULL; dp = dp->next) {
	one_est_t *level0_est = est_for_level(dp, 0);
	est(dp)->promote = -1000;

	if (level0_est->nsize <= (gint64)0)
	    continue;

	if(est(dp)->next_level0 <= 0)
	    continue;

	if(est(dp)->next_level0 > dp->maxpromoteday)
	    continue;

	new_total = total_size - est(dp)->dump_est->csize + level0_est->csize;
	new_lev0 = (gint64)total_lev0 + level0_est->csize;

	nb_today = 0;
	nb_same_day = 0;
	nb_disk_today = 0;
	nb_disk_same_day = 0;
	for(dp1 = schedq.head; dp1 != NULL; dp1 = dp1->next) {
	    if(est(dp1)->dump_est->level == 0)
		nb_disk_today++;
	    else if(est(dp1)->next_level0 == est(dp)->next_level0)
		nb_disk_same_day++;
	    if(strcmp(dp->host->hostname, dp1->host->hostname) == 0) {
		if(est(dp1)->dump_est->level == 0)
		    nb_today++;
		else if(est(dp1)->next_level0 == est(dp)->next_level0)
		    nb_same_day++;
	    }
	}

	/* do not promote if overflow tape */
	if(new_total > tape_length)
	    continue;

	/* do not promote if overflow balanced size and something today */
	/* promote if nothing today */
	if((new_lev0 > (gint64)(balanced_size + balance_threshold)) &&
		(nb_disk_today > 0))
	    continue;

	/* do not promote if only one disk due that day and nothing today */
	if(nb_disk_same_day == 1 && nb_disk_today == 0)
	    continue;

	nb_today2 = nb_today*nb_today;
	if(nb_today == 0 && nb_same_day > 1)
	    nb_same_day++;

	if(nb_same_day >= nb_today2) {
	    est(dp)->promote = ((nb_same_day - nb_today2)*(nb_same_day - nb_today2)) + 
			       conf_dumpcycle - est(dp)->next_level0;
	}
	else {
	    est(dp)->promote = -nb_today2 +
			       conf_dumpcycle - est(dp)->next_level0;
	}

	qname = quote_string(dp->name);
	if(!dp_promote || est(dp_promote)->promote < est(dp)->promote) {
	    dp_promote = dp;
	    g_fprintf(stderr,"   try %s:%s %d %d %d = %d\n",
		    dp->host->hostname, qname, nb_same_day, nb_today, est(dp)->next_level0, est(dp)->promote);
	}
	else {
	    g_fprintf(stderr,"no try %s:%s %d %d %d = %d\n",
		    dp->host->hostname, qname, nb_same_day, nb_today, est(dp)->next_level0, est(dp)->promote);
	}
	amfree(qname);
    }

    if(dp_promote) {
	one_est_t *level0_est;
	dp = dp_promote;
	level0_est = est_for_level(dp, 0);

	qname = quote_string(dp->name);
	new_total = total_size - est(dp)->dump_est->csize + level0_est->csize;
	new_lev0 = (gint64)total_lev0 + level0_est->csize;

	total_size = new_total;
	total_lev0 = (double)new_lev0;
	check_days = est(dp)->next_level0;
	est(dp)->degr_est = est(dp)->dump_est;
	est(dp)->dump_est = level0_est;
	est(dp)->next_level0 = 0;

	g_fprintf(stderr,
	      _("   promote: moving %s:%s up, total_lev0 %1.0lf, total_size %lld\n"),
		dp->host->hostname, qname,
		total_lev0, (long long)total_size);

	log_add(L_INFO,
		plural(_("Full dump of %s:%s promoted from %d day ahead."),
		       _("Full dump of %s:%s promoted from %d days ahead."),
		      check_days),
		dp->host->hostname, qname, check_days);
	amfree(qname);
	return 1;
    }
    return 0;
}


static int promote_hills(void)
{
    disk_t *dp;
    struct balance_stats {
	int disks;
	gint64 size;
    } *sp = NULL;
    int days;
    int hill_days = 0;
    gint64 hill_size;
    gint64 new_total;
    int my_dumpcycle;
    char *qname;

    /* If we are already doing a level 0 don't bother */
    if(total_lev0 > 0)
	return 0;

    /* Do the guts of an "amadmin balance" */
    my_dumpcycle = conf_dumpcycle;
    if(my_dumpcycle > 10000) my_dumpcycle = 10000;

    sp = (struct balance_stats *)
	alloc(SIZEOF(struct balance_stats) * my_dumpcycle);

    for(days = 0; days < my_dumpcycle; days++) {
	sp[days].disks = 0;
	sp[days].size = (gint64)0;
    }

    for(dp = schedq.head; dp != NULL; dp = dp->next) {
	days = est(dp)->next_level0;
	if (days < 0) days = 0;
	if(days<my_dumpcycle && !dp->skip_full && dp->strategy != DS_NOFULL &&
	   dp->strategy != DS_INCRONLY) {
	    sp[days].disks++;
	    sp[days].size += est(dp)->last_lev0size;
	}
    }

    /* Search for a suitable big hill and cut it down */
    while(1) {
	/* Find the tallest hill */
	hill_size = (gint64)0;
	for(days = 0; days < my_dumpcycle; days++) {
	    if(sp[days].disks > 1 && sp[days].size > hill_size) {
		hill_size = sp[days].size;
		hill_days = days;
	    }
	}

	if(hill_size <= (gint64)0) break;	/* no suitable hills */

	/* Find all the dumps in that hill and try and remove one */
	for(dp = schedq.head; dp != NULL; dp = dp->next) {
	    one_est_t *level0_est;
	    if(est(dp)->next_level0 != hill_days ||
	       est(dp)->next_level0 > dp->maxpromoteday ||
	       dp->skip_full ||
	       dp->strategy == DS_NOFULL ||
	       dp->strategy == DS_INCRONLY)
		continue;
	    level0_est = est_for_level(dp, 0);
	    if (level0_est->nsize <= (gint64)0)
		continue;
	    new_total = total_size - est(dp)->dump_est->csize + level0_est->csize;
	    if(new_total > tape_length)
		continue;
	    /* We found a disk we can promote */
	    qname = quote_string(dp->name);
	    total_size = new_total;
	    total_lev0 += (double)level0_est->csize;
            est(dp)->degr_est = est(dp)->dump_est;
            est(dp)->dump_est = level0_est;
	    est(dp)->next_level0 = 0;

	    g_fprintf(stderr,
		    _("   promote: moving %s:%s up, total_lev0 %1.0lf, total_size %lld\n"),
		    dp->host->hostname, qname,
		    total_lev0, (long long)total_size);

	    log_add(L_INFO,
		    plural(_("Full dump of %s:%s specially promoted from %d day ahead."),
			   _("Full dump of %s:%s specially promoted from %d days ahead."),
			   hill_days),
		    dp->host->hostname, qname, hill_days);

	    amfree(qname);
	    amfree(sp);
	    return 1;
	}
	/* All the disks in that hill were unsuitable. */
	sp[hill_days].disks = 0;	/* Don't get tricked again */
    }

    amfree(sp);
    return 0;
}

/*
 * ========================================================================
 * OUTPUT SCHEDULE
 *
 * XXX - memory leak - we shouldn't just throw away *dp
 */
static void output_scheduleline(
    disk_t *dp)
{
    est_t *ep;
    time_t dump_time = 0, degr_time = 0;
    double dump_kps = 0, degr_kps = 0;
    char *schedline = NULL, *degr_str = NULL;
    char dump_priority_str[NUM_STR_SIZE];
    char dump_level_str[NUM_STR_SIZE];
    char dump_nsize_str[NUM_STR_SIZE];
    char dump_csize_str[NUM_STR_SIZE];
    char dump_time_str[NUM_STR_SIZE];
    char dump_kps_str[NUM_STR_SIZE];
    char degr_level_str[NUM_STR_SIZE];
    char degr_nsize_str[NUM_STR_SIZE];
    char degr_csize_str[NUM_STR_SIZE];
    char degr_time_str[NUM_STR_SIZE];
    char degr_kps_str[NUM_STR_SIZE];
    char *dump_date, *degr_date;
    char *features;
    char *qname = quote_string(dp->name);

    ep = est(dp);

    if(ep->dump_est->csize == (gint64)-1) {
	/* no estimate, fail the disk */
	g_fprintf(stderr,
		_("%s: FAILED %s %s %s %d \"[no estimate]\"\n"),
		get_pname(),
		dp->host->hostname, qname, planner_timestamp, ep->dump_est->level);
	log_add(L_FAIL, _("%s %s %s %d [no estimate]"),
		dp->host->hostname, qname, planner_timestamp, ep->dump_est->level);
	amfree(qname);
	return;
    }

    dump_date = ep->dump_est->dumpdate;
    degr_date = ep->degr_est->dumpdate;

#define fix_rate(rate) (rate < 1.0 ? DEFAULT_DUMPRATE : rate)

    if(ep->dump_est->level == 0) {
	dump_kps = fix_rate(ep->fullrate);
	dump_time = (time_t)((double)ep->dump_est->csize / dump_kps);

	if(ep->degr_est->csize != (gint64)-1) {
	    degr_kps = fix_rate(ep->incrrate);
	    degr_time = (time_t)((double)ep->degr_est->csize / degr_kps);
	}
    }
    else {
	dump_kps = fix_rate(ep->incrrate);
	dump_time = (time_t)((double)ep->dump_est->csize / dump_kps);
    }

    if(ep->dump_est->level == 0 && ep->degr_est->csize != (gint64)-1) {
	g_snprintf(degr_level_str, sizeof(degr_level_str),
		    "%d", ep->degr_est->level);
	g_snprintf(degr_nsize_str, sizeof(degr_nsize_str),
		    "%lld", (long long)ep->degr_est->nsize);
	g_snprintf(degr_csize_str, sizeof(degr_csize_str),
		    "%lld", (long long)ep->degr_est->csize);
	g_snprintf(degr_time_str, sizeof(degr_time_str),
		    "%lld", (long long)degr_time);
	g_snprintf(degr_kps_str, sizeof(degr_kps_str),
		    "%.0lf", degr_kps);
	degr_str = vstralloc(" ", degr_level_str,
			     " ", degr_date,
			     " ", degr_nsize_str,
			     " ", degr_csize_str,
			     " ", degr_time_str,
			     " ", degr_kps_str,
			     NULL);
    } else {
	char *degr_mesg;
	if (ep->degr_mesg) {
	    degr_mesg = quote_string(ep->degr_mesg);
	} else {
	    degr_mesg = quote_string(_("Skipping: cannot dump in degraded mode for unknown reason"));
	}
	degr_str = vstralloc(" ", degr_mesg, NULL);
	amfree(degr_mesg);
    }
    g_snprintf(dump_priority_str, SIZEOF(dump_priority_str),
		"%d", ep->dump_priority);
    g_snprintf(dump_level_str, SIZEOF(dump_level_str),
		"%d", ep->dump_est->level);
    g_snprintf(dump_nsize_str, sizeof(dump_nsize_str),
		"%lld", (long long)ep->dump_est->nsize);
    g_snprintf(dump_csize_str, sizeof(dump_csize_str),
		"%lld", (long long)ep->dump_est->csize);
    g_snprintf(dump_time_str, sizeof(dump_time_str),
		"%lld", (long long)dump_time);
    g_snprintf(dump_kps_str, sizeof(dump_kps_str),
		"%.0lf", dump_kps);
    features = am_feature_to_string(dp->host->features);
    schedline = vstralloc("DUMP ",dp->host->hostname,
			  " ", features,
			  " ", qname,
			  " ", planner_timestamp,
			  " ", dump_priority_str,
			  " ", dump_level_str,
			  " ", dump_date,
			  " ", dump_nsize_str,
			  " ", dump_csize_str,
			  " ", dump_time_str,
			  " ", dump_kps_str,
			  degr_str ? degr_str : "",
			  "\n", NULL);

    if (est(dp)->dump_est->guessed == 1) {
        log_add(L_WARNING, _("WARNING: no history available for %s:%s; guessing that size will be %lld KB\n"), dp->host->hostname, qname, (long long)est(dp)->dump_est->csize);
    }
    fputs(schedline, stdout);
    fputs(schedline, stderr);
    amfree(features);
    amfree(schedline);
    amfree(degr_str);
    amfree(qname);
}

static void
server_estimate(
    disk_t *dp,
    int     i,
    info_t *info,
    int     level)
{
    int    stats;
    gint64 size;

    size = internal_server_estimate(dp, info, level, &stats);

    est(dp)->dump_est = &est(dp)->estimate[i];
    est(dp)->estimate[i].nsize = size;
    if (stats == 0) {
	est(dp)->estimate[i].guessed = 1;
    }
}
