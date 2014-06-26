/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2014 Zmanda, Inc.  All Rights Reserved.
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
 * $Id: driver.c 6512 2007-05-24 17:00:24Z ian $
 *
 * controlling process for the Amanda backup system
 */

/*
 * XXX possibly modify tape queue to be cognizant of how much room is left on
 *     tape.  Probably not effective though, should do this in planner.
 */

#include "amanda.h"
#include "find.h"
#include "clock.h"
#include "conffile.h"
#include "diskfile.h"
#include "event.h"
#include "holding.h"
#include "infofile.h"
#include "logfile.h"
#include "fsusage.h"
#include "driverio.h"
#include "server_util.h"
#include "timestamp.h"
#include "amindex.h"
#include "cmdfile.h"

#define driver_debug(i, ...) do {	\
	if ((i) <= debug_driver) {	\
	    dbprintf(__VA_ARGS__);	\
	}				\
} while (0)

#define hold_debug(i, ...) do {		\
	if ((i) <= debug_holding) {	\
	    dbprintf(__VA_ARGS__);	\
	}				\
} while (0)

static disklist_t waitq;	// dle waiting estimate result
static disklist_t runq;		// dle waiting to be dumped to holding disk
static disklist_t directq;	// dle waiting to be dumped directly to tape
static disklist_t roomq;	// dle waiting for more space on holding disk
static int pending_aborts;
static gboolean all_degraded_mode = FALSE;
static off_t reserved_space;
static off_t total_disksize;
static char *dumper_program;
static char *chunker_program;
static int  inparallel;
static int nodump = 0;
static storage_t *storage;
static int conf_max_dle_by_volume;
static int conf_taperalgo;
static int conf_taper_parallel_write;
static int conf_runtapes;
static time_t sleep_time;
static int idle_reason;
static char *driver_timestamp;
static char *hd_driver_timestamp;
static am_host_t *flushhost = NULL;
static holdalloc_t *holdalloc;
static int num_holdalloc;
static event_handle_t *dumpers_ev_time = NULL;
static event_handle_t *flush_ev_read = NULL;
static event_handle_t *schedule_ev_read = NULL;
static int   schedule_done;			// 1 if we don't wait for a
						//   schedule from the planner
static int   force_flush;			// All dump are terminated, we
						// must now respect taper_flush
static int taper_nb_scan_volume = 0;
static int nb_sent_new_tape = 0;
static int taper_started = 0;
static int nb_storage;
static cmddatas_t *cmddatas = NULL;

static int wait_children(int count);
static void wait_for_children(void);
static void allocate_bandwidth(netif_t *ip, unsigned long kps);
static int assign_holdingdisk(assignedhd_t **holdp, disk_t *diskp);
static void adjust_diskspace(disk_t *diskp, cmd_t cmd);
static void delete_diskspace(disk_t *diskp);
static assignedhd_t **build_diskspace(char *destname);
static int client_constrained(disk_t *dp);
static void deallocate_bandwidth(netif_t *ip, unsigned long kps);
static void dump_schedule(disklist_t *qp, char *str);
static assignedhd_t **find_diskspace(off_t size, int *cur_idle,
					assignedhd_t *preferred);
static unsigned long free_kps(netif_t *ip);
static off_t free_space(void);
static void dumper_chunker_result(job_t *job);
static void dumper_taper_result(job_t *job);
static void file_taper_result(job_t *job);
static void handle_dumper_result(void *);
static void handle_chunker_result(void *);
static void handle_dumpers_time(void *);
static void handle_taper_result(void *);
static gboolean dump_match_selection(char *storage_n, disk_t *dp);

static void holdingdisk_state(char *time_str);
static wtaper_t *idle_taper(taper_t *taper);
static wtaper_t *wtaper_from_name(taper_t *taper, char *name);
static void interface_state(char *time_str);
static int queue_length(disklist_t *q);
static void read_flush(void *cookie);
static void read_schedule(void *cookie);
static void short_dump_state(void);
static void startaflush(void);
static void start_degraded_mode(disklist_t *queuep);
static void start_some_dumps(disklist_t *rq);
static void continue_port_dumps(void);
static void update_failed_dump(disk_t *);
static int no_taper_flushing(void);
static int active_dumper(void);
static void fix_index_header(disk_t *dp);
static int all_tapeq_empty(void);

typedef enum {
    TAPE_ACTION_NO_ACTION         = 0,
    TAPE_ACTION_SCAN              = (1 << 0),
    TAPE_ACTION_NEW_TAPE          = (1 << 1),
    TAPE_ACTION_NO_NEW_TAPE       = (1 << 2),
    TAPE_ACTION_START_A_FLUSH     = (1 << 3),
    TAPE_ACTION_START_A_FLUSH_FIT = (1 << 4),
    TAPE_ACTION_MOVE              = (1 << 5)
} TapeAction;

static TapeAction tape_action(wtaper_t *wtaper, char **why_no_new_tape);

static const char *idle_strings[] = {
#define NOT_IDLE		0
    T_("not-idle"),
#define IDLE_NO_DUMPERS		1
    T_("no-dumpers"),
#define IDLE_BUSY		2
    T_("busy"),
#define IDLE_START_WAIT		3
    T_("start-wait"),
#define IDLE_NO_HOLD		4
    T_("no-hold"),
#define IDLE_CLIENT_CONSTRAINED	5
    T_("client-constrained"),
#define IDLE_NO_BANDWIDTH	6
    T_("no-bandwidth"),
#define IDLE_NO_DISKSPACE	7
    T_("no-diskspace")
};

int
main(
    int		argc,
    char **	argv)
{
    disklist_t origq;
    disk_t *diskp;
    int dsk;
    dumper_t *dumper;
    taper_t  *taper;
    wtaper_t *wtaper;
    char *newdir = NULL;
    struct fs_usage fsusage;
    holdingdisk_t *hdp;
    identlist_t    il;
    unsigned long reserve = 100;
    char *conf_diskfile;
    char *taper_program;
    char *line;
    char hostname[1025];
    intmax_t kb_avail;
    config_overrides_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;
    holdalloc_t *ha, *ha_last;
    find_result_t *holding_files;
    disklist_t holding_disklist = { NULL, NULL };
    int no_taper = FALSE;
    int from_client = FALSE;
    char *storage_name;
    int sum_taper_parallel_write;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("driver-%s\n", VERSION);
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

    safe_fd(-1, 0);

    setvbuf(stdout, (char *)NULL, (int)_IOLBF, 0);
    setvbuf(stderr, (char *)NULL, (int)_IOLBF, 0);

    set_pname("driver");

    dbopen(DBG_SUBDIR_SERVER);

    atexit(wait_for_children);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_trace_log);

    startclock();

    cfg_ovr = extract_commandline_config_overrides(&argc, &argv);

    if (argc > 1)
	cfg_opt = argv[1];
    set_config_overrides(cfg_ovr);

    if(argc > 2) {
        if(g_str_equal(argv[2], "nodump")) {
            nodump = 1;
	    argv++;
	    argc--;
        }
    }

    log_filename = NULL;
    if (argc > 3) {
	if (g_str_equal(argv[2], "--log-filename")) {
	    log_filename = g_strdup(argv[3]);
	    set_logname(log_filename);
	    argv += 2;
	    argc -= 2;
	}
    }

    if (argc > 2) {
	if (g_str_equal(argv[2], "--no-taper")) {
	    no_taper = TRUE;
	    argv++;
	    argc--;
	}
    }

    if (argc > 2) {
	if (g_str_equal(argv[2], "--from-client")) {
	    from_client = TRUE;
	    from_client = from_client;
	    argv++;
	    argc--;
	}
    }

    config_init_with_global(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, cfg_opt);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &origq);
    disable_skip_disk(&origq);
    amfree(conf_diskfile);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());
    g_printf(_("%s: pid %ld executable %s version %s\n"),
	   get_pname(), (long) getpid(), argv[0], VERSION);

    safe_cd(); /* do this *after* config_init */

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    /* load DLEs from the holding disk, in case there's anything to flush there */
    search_holding_disk(&holding_files, &holding_disklist);
    /* note that the dumps are added to the global disklist, so we need not consult
     * holding_files or holding_disklist after this */

    amfree(driver_timestamp);
    /* read timestamp from stdin */
    while ((line = agets(stdin)) != NULL) {
	if (line[0] != '\0')
	    break;
	amfree(line);
    }
    if ( line == NULL ) {
      error(_("Did not get DATE line from planner"));
      /*NOTREACHED*/
    }
    driver_timestamp = g_malloc(15);
    strncpy(driver_timestamp, &line[5], 14);
    driver_timestamp[14] = '\0';
    amfree(line);
    log_add(L_START,_("date %s"), driver_timestamp);

    gethostname(hostname, sizeof(hostname));
    log_add(L_STATS,_("hostname %s"), hostname);

    /* check that we don't do many dump in a day and usetimestamps is off */
    if(strlen(driver_timestamp) == 8) {
	if (!nodump) {
	    char *conf_logdir = getconf_str(CNF_LOGDIR);
	    char *logfile    = g_strjoin(NULL, conf_logdir, "/log.",
					 driver_timestamp, ".0", NULL);
	    char *oldlogfile = g_strjoin(NULL, conf_logdir, "/oldlog/log.",
					 driver_timestamp, ".0", NULL);
	    if(access(logfile, F_OK) == 0 || access(oldlogfile, F_OK) == 0) {
		log_add(L_WARNING, _("WARNING: This is not the first amdump run today. Enable the usetimestamps option in the configuration file if you want to run amdump more than once per calendar day."));
	    }
	    amfree(oldlogfile);
	    amfree(logfile);
	}
	hd_driver_timestamp = get_timestamp_from_time(0);
    }
    else {
	hd_driver_timestamp = g_strdup(driver_timestamp);
    }

    taper_program = g_strjoin(NULL, amlibexecdir, "/", "taper", NULL);
    dumper_program = g_strjoin(NULL, amlibexecdir, "/", "dumper", NULL);
    chunker_program = g_strjoin(NULL, amlibexecdir, "/", "chunker", NULL);

    il = getconf_identlist(CNF_STORAGE);
    storage_name = il->data;
    storage = lookup_storage(storage_name);
    conf_taperalgo = storage_get_taperalgo(storage);
    conf_taper_parallel_write = storage_get_taper_parallel_write(storage);
    conf_runtapes = storage_get_runtapes(storage);
    conf_max_dle_by_volume = storage_get_max_dle_by_volume(storage);
    if (conf_taper_parallel_write > conf_runtapes) {
	conf_taper_parallel_write = conf_runtapes;
    }

    /* set up any configuration-dependent variables */

    inparallel	= getconf_int(CNF_INPARALLEL);

    reserve = (unsigned long)getconf_int(CNF_RESERVE);

    total_disksize = (off_t)0;
    ha_last = NULL;
    num_holdalloc = 0;
    for (il = getconf_identlist(CNF_HOLDINGDISK), dsk = 0;
	 il != NULL;
	 il = il->next, dsk++) {
	hdp = lookup_holdingdisk(il->data);
	ha = g_malloc(sizeof(holdalloc_t));
	num_holdalloc++;

	/* link the list in the same order as getconf_holdingdisks's results */
	ha->next = NULL;
	if (ha_last == NULL)
	    holdalloc = ha;
	else
	    ha_last->next = ha;
	ha_last = ha;

	ha->hdisk = hdp;
	ha->allocated_dumpers = 0;
	ha->allocated_space = (off_t)0;
	ha->disksize = holdingdisk_get_disksize(hdp);

	/* get disk size */
	if(get_fs_usage(holdingdisk_get_diskdir(hdp), NULL, &fsusage) == -1
	   || access(holdingdisk_get_diskdir(hdp), W_OK) == -1) {
	    log_add(L_WARNING, _("WARNING: ignoring holding disk %s: %s\n"),
		    holdingdisk_get_diskdir(hdp), strerror(errno));
	    ha->disksize = 0L;
	    continue;
	}

	/* do the division first to avoid potential integer overflow */
	if (fsusage.fsu_bavail_top_bit_set)
	    kb_avail = 0;
	else
	    kb_avail = fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;

	if(ha->disksize > (off_t)0) {
	    if(ha->disksize > kb_avail) {
		log_add(L_WARNING,
			_("WARNING: %s: %lld KB requested, "
			"but only %lld KB available."),
			holdingdisk_get_diskdir(hdp),
			(long long)ha->disksize,
			(long long)kb_avail);
			ha->disksize = kb_avail;
	    }
	}
	/* ha->disksize is negative; use all but that amount */
	else if(kb_avail < -ha->disksize) {
	    log_add(L_WARNING,
		    _("WARNING: %s: not %lld KB free."),
		    holdingdisk_get_diskdir(hdp),
		    (long long)-ha->disksize);
	    ha->disksize = (off_t)0;
	    continue;
	}
	else
	    ha->disksize += kb_avail;

	g_printf(_("driver: adding holding disk %d dir %s size %lld chunksize %lld\n"),
	       dsk, holdingdisk_get_diskdir(hdp),
	       (long long)ha->disksize,
	       (long long)(holdingdisk_get_chunksize(hdp)));

	g_free(newdir);
	newdir = g_strconcat(holdingdisk_get_diskdir(hdp), "/",
	    hd_driver_timestamp, NULL);

	if(!mkholdingdir(newdir)) {
	    ha->disksize = (off_t)0;
	}
	total_disksize += ha->disksize;
    }

    reserved_space = total_disksize * (off_t)(reserve / 100);

    g_printf(_("reserving %lld out of %lld for degraded-mode dumps\n"),
	   (long long)reserved_space, (long long)free_space());

    amfree(newdir);

    /* taper takes a while to get going, so start it up right away */

    nb_storage = 0;
    sum_taper_parallel_write = 0;
    for (il = getconf_identlist(CNF_STORAGE); il != NULL; il = il->next) {
	storage_t *storage = lookup_storage((char *)il->data);
	nb_storage++;
	sum_taper_parallel_write = storage_get_taper_parallel_write(storage);
    }
    init_driverio(inparallel, nb_storage, sum_taper_parallel_write);
    startup_tape_process(taper_program, no_taper);

    /* fire up the dumpers now while we are waiting */
    if(!nodump) startup_dump_processes(dumper_program, inparallel, driver_timestamp);

    /*
     * Read schedule from stdin.  Usually, this is a pipe from planner,
     * so the effect is that we wait here for the planner to
     * finish, but meanwhile the taper is rewinding the tape, reading
     * the label, checking it, writing a new label and all that jazz
     * in parallel with the planner.
     */

    runq.head = NULL;
    runq.tail = NULL;
    directq.head = NULL;
    directq.tail = NULL;
    waitq = origq;
    roomq.head = NULL;
    roomq.tail = NULL;

    if (no_taper || conf_runtapes <= 0) {
	taper_started = 1; /* we'll pretend the taper started and failed immediately */
	for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	    taper->degraded_mode = TRUE;
	}
    } else {
	for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	    wtaper = taper->wtapetable;
	    wtaper->state = TAPER_STATE_INIT;
	    taper->nb_wait_reply++;
	    taper->nb_scan_volume++;
	    taper->ev_read = event_register(taper->fd, EV_READFD,
					    handle_taper_result, taper);
            taper_cmd(taper, wtaper, START_TAPER, NULL, taper->wtapetable[0].name, 0, driver_timestamp);
	}
    }

    flush_ev_read = event_register((event_id_t)0, EV_READFD, read_flush, NULL);

    log_add(L_STATS, _("startup time %s"), walltime_str(curclock()));

    g_printf(_("driver: start time %s inparallel %d bandwidth %lu diskspace %lld "), walltime_str(curclock()), inparallel,
	   free_kps(NULL), (long long)free_space());
    g_printf(_(" dir %s datestamp %s driver: drain-ends tapeq %s big-dumpers %s\n"),
	   "OBSOLETE", driver_timestamp, taperalgo2str(conf_taperalgo),
	   getconf_str(CNF_DUMPORDER));
    fflush(stdout);

    schedule_done = nodump;
    force_flush = 0;

    short_dump_state();
    event_loop(0);

    force_flush = 1;

    /* mv runq to directq */
    while (!empty(runq)) {
	diskp = dequeue_disk(&runq);
	headqueue_disk(&directq, diskp);
    }

    run_server_global_scripts(EXECUTE_ON_POST_BACKUP, get_config_name());

    /* log error for any remaining dumps */
    while(!empty(directq)) {
	diskp = dequeue_disk(&directq);

	if (diskp->orig_holdingdisk == HOLD_REQUIRED) {
	    char *qname = quote_string(diskp->name);
	    log_add(L_FAIL, "%s %s %s %d [%s]",
		diskp->host->hostname, qname, sched(diskp)->datestamp,
		sched(diskp)->level,
		_("can't dump required holdingdisk"));
	    amfree(qname);
	} else {
	    gboolean dp_degraded_mode = FALSE;
	    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		if (dump_match_selection(taper->storage_name, diskp)) {
		    dp_degraded_mode |= taper->degraded_mode;
		}
	    }
	    if (!dp_degraded_mode) {
		char *qname = quote_string(diskp->name);
		log_add(L_FAIL, "%s %s %s %d [%s]",
			diskp->host->hostname, qname, sched(diskp)->datestamp,
			sched(diskp)->level,
			_("can't dump in non degraded mode"));
		amfree(qname);
	    } else {
		char *qname = quote_string(diskp->name);
		log_add(L_FAIL, "%s %s %s %d [%s]",
			diskp->host->hostname, qname, sched(diskp)->datestamp,
			sched(diskp)->level,
			num_holdalloc == 0 ?
			    _("can't do degraded dump without holding disk") :
			    diskp->orig_holdingdisk != HOLD_NEVER ?
				_("out of holding space in degraded mode") :
				_("can't dump 'holdingdisk never' dle in degraded mode"));
		amfree(qname);
	    }
	}
    }

    short_dump_state();				/* for amstatus */

    g_printf(_("driver: QUITTING time %s telling children to quit\n"),
           walltime_str(curclock()));
    fflush(stdout);

    if(!nodump) {
	for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	    if(dumper->fd >= 0)
		dumper_cmd(dumper, QUIT, NULL, NULL);
	}
    }

    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	if (taper->fd >= 0) {
	    taper_cmd(taper, NULL, QUIT, NULL, NULL, 0, NULL);
	}
    }

    /* wait for all to die */
    wait_children(600);

    /* cleanup */
    holding_cleanup(NULL, NULL);

    remove_working_in_cmdfile(cmddatas, getppid());

    amfree(newdir);

    check_unfree_serial();
    g_printf(_("driver: FINISHED time %s\n"), walltime_str(curclock()));
    fflush(stdout);
    log_add(L_FINISH,_("date %s time %s"), driver_timestamp, walltime_str(curclock()));
    log_add(L_INFO, "pid-done %ld", (long)getpid());
    amfree(driver_timestamp);

    amfree(dumper_program);
    amfree(taper_program);

    dbclose();

    return 0;
}

/* sleep up to count seconds, and wait for terminating child process */
/* if sleep is negative, this function will not timeout              */
/* exit once all child process are finished or the timout expired    */
/* return 0 if no more children to wait                              */
/* return 1 if some children are still alive                         */
static int
wait_children(int count)
{
    pid_t     pid;
    amwait_t  retstat;
    char     *who;
    char     *what;
    int       code=0;
    dumper_t *dumper;
    taper_t  *taper;
    int       wait_errno;

    do {
	do {
	    pid = waitpid((pid_t)-1, &retstat, WNOHANG);
	    wait_errno = errno;
	    if (pid > 0) {
		what = NULL;
		if (! WIFEXITED(retstat)) {
		    what = _("signal");
		    code = WTERMSIG(retstat);
		} else if (WEXITSTATUS(retstat) != 0) {
		    what = _("code");
		    code = WEXITSTATUS(retstat);
		}
		who = NULL;
		for (dumper = dmptable; dumper < dmptable + inparallel;
		     dumper++) {
		    if (pid == dumper->pid) {
			who = g_strdup(dumper->name);
			dumper->pid = -1;
			break;
		    }
		    if (dumper->job && dumper->job->chunker &&
			pid == dumper->job->chunker->pid) {
			who = g_strdup(dumper->job->chunker->name);
			dumper->job->chunker->pid = -1;
			break;
		    }
		}
		for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		    if (pid == taper->pid) {
			who = g_strdup(taper->name);
			taper->pid = -1;
			break;
		    }
		}
		if(what != NULL && who == NULL) {
		    who = g_strdup("unknown");
		}
		if(who && what) {
		    log_add(L_WARNING, _("%s pid %u exited with %s %d\n"), who,
			    (unsigned)pid, what, code);
		    g_printf(_("driver: %s pid %u exited with %s %d\n"), who,
			   (unsigned)pid, what, code);
		}
		amfree(who);
	    }
	} while (pid > 0 || wait_errno == EINTR);
	if (errno != ECHILD)
	    sleep(1);
	if (count > 0)
	    count--;
    } while ((errno != ECHILD) && (count != 0));
    return (errno != ECHILD);
}

static void
kill_children(int signal)
{
    dumper_t *dumper;
    taper_t  *taper;

    if(!nodump) {
        for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	    if (!dumper->down && dumper->pid > 1) {
		g_printf(_("driver: sending signal %d to %s pid %u\n"), signal,
		       dumper->name, (unsigned)dumper->pid);
		if (kill(dumper->pid, signal) == -1 && errno == ESRCH) {
		    if (dumper->job && dumper->job->chunker)
			dumper->job->chunker->pid = 0;
		}
		if (dumper->job && dumper->job->chunker &&
		    dumper->job->chunker->pid > 1) {
		    g_printf(_("driver: sending signal %d to %s pid %u\n"), signal,
			   dumper->job->chunker->name,
			   (unsigned)dumper->job->chunker->pid);
		    if (kill(dumper->job->chunker->pid, signal) == -1 &&
			errno == ESRCH)
			dumper->job->chunker->pid = 0;
		}
	    }
        }
    }

    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	if (taper->pid > 1) {
	    g_printf(_("driver: sending signal %d to %s pid %u\n"), signal,
		     taper->name, (unsigned)taper->pid);
	    if (kill(taper->pid, signal) == -1 && errno == ESRCH)
		taper->pid = 0;
	}
    }
}

static void
wait_for_children(void)
{
    dumper_t *dumper;
    taper_t  *taper;

    if(!nodump) {
	for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	    if (dumper->pid > 1 && dumper->fd >= 0) {
		dumper_cmd(dumper, QUIT, NULL, NULL);
		if (dumper->job && dumper->job->chunker &&
		    dumper->job->chunker->pid > 1 &&
		    dumper->job->chunker->fd >= 0)
		    chunker_cmd(dumper->job->chunker, QUIT, NULL, NULL);
	    }
	}
    }

    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	if (taper->pid > 1 && taper-> fd > 0) {
	    taper_cmd(taper, NULL, QUIT, NULL, NULL, 0, NULL);
	}
    }

    if(wait_children(60) == 0)
	return;

    kill_children(SIGHUP);
    if(wait_children(60) == 0)
	return;

    kill_children(SIGKILL);
    if(wait_children(-1) == 0)
	return;

}

static void startaflush_tape(wtaper_t *wtaper, gboolean *state_changed);

static void
startaflush(void)
{
    taper_t *taper;
    gboolean state_changed = FALSE;

    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	wtaper_t *wtaper;

	for (wtaper = taper->wtapetable;
	     wtaper <= taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_WAIT_FOR_TAPE) {
		startaflush_tape(wtaper, &state_changed);
	    }
	}
	for (wtaper = taper->wtapetable;
	     wtaper <= taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_TAPE_REQUESTED) {
		startaflush_tape(wtaper, &state_changed);
	    }
	}
	for (wtaper = taper->wtapetable;
	     wtaper <= taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_INIT) {
		startaflush_tape(wtaper, &state_changed);
	    }
	}
	for (wtaper = taper->wtapetable;
	     wtaper <= taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_IDLE) {
		startaflush_tape(wtaper, &state_changed);
	    }
	}
    }
    if (state_changed) {
	short_dump_state();
    }
}

static void
startaflush_tape(
    wtaper_t  *wtaper,
    gboolean  *state_changed)
{
    GList  *fit;
    disk_t *dp = NULL;
    disk_t *dfit = NULL;
    char *datestamp;
    off_t extra_tapes_size = 0;
    off_t taper_left;
    char *qname;
    TapeAction result_tape_action;
    char *why_no_new_tape = NULL;
    taper_t  *taper = wtaper->taper;
    wtaper_t *wtaper1;

    result_tape_action = tape_action(wtaper, &why_no_new_tape);

    if (result_tape_action & TAPE_ACTION_SCAN) {
	wtaper->state &= ~TAPER_STATE_TAPE_REQUESTED;
	wtaper->state |= TAPER_STATE_WAIT_FOR_TAPE;
	wtaper->taper->nb_scan_volume++;
	taper_cmd(taper, wtaper, START_SCAN, wtaper->job->disk, NULL, 0, NULL);
    } else if (result_tape_action & TAPE_ACTION_NEW_TAPE) {
	wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	wtaper->state |= TAPER_STATE_WAIT_NEW_TAPE;
	nb_sent_new_tape++;
	taper_cmd(taper, wtaper, NEW_TAPE, wtaper->job->disk, NULL, 0, NULL);
    } else if (result_tape_action & TAPE_ACTION_NO_NEW_TAPE) {
	wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	taper_cmd(taper, wtaper, NO_NEW_TAPE, wtaper->job->disk, why_no_new_tape, 0, NULL);
	wtaper->state |= TAPER_STATE_DONE;
	taper->degraded_mode = TRUE;
	start_degraded_mode(&runq);
	*state_changed = TRUE;
    } else if (result_tape_action & TAPE_ACTION_MOVE) {
	wtaper1 = idle_taper(taper);
	if (wtaper1) {
	    wtaper->state &= ~TAPER_STATE_TAPE_REQUESTED;
	    wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	    taper_cmd(taper, wtaper, TAKE_SCRIBE_FROM, wtaper->job->disk, wtaper1->name,
		      0 , NULL);
	    wtaper1->state = TAPER_STATE_DEFAULT;
	    wtaper->state |= TAPER_STATE_TAPE_STARTED;
	    wtaper->left = wtaper1->left;
	    wtaper->nb_dle++;
	    if (taper->last_started_wtaper == wtaper1) {
		taper->last_started_wtaper = wtaper;
	    }
	    *state_changed = TRUE;
	}
    }

    if (!taper->degraded_mode &&
        wtaper->state & TAPER_STATE_IDLE &&
	!empty(taper->tapeq) &&
	(result_tape_action & TAPE_ACTION_START_A_FLUSH ||
	 result_tape_action & TAPE_ACTION_START_A_FLUSH_FIT)) {

	int taperalgo = conf_taperalgo;
	if (result_tape_action & TAPE_ACTION_START_A_FLUSH_FIT) {
	    if (taperalgo == ALGO_FIRST)
		taperalgo = ALGO_FIRSTFIT;
	    else if (taperalgo == ALGO_LARGEST)
		taperalgo = ALGO_LARGESTFIT;
	    else if (taperalgo == ALGO_SMALLEST)
		taperalgo = ALGO_SMALLESTFIT;
	    else if (taperalgo == ALGO_LAST)
		taperalgo = ALGO_LASTFIT;
	}

	extra_tapes_size = taper->tape_length *
			   (off_t)(taper->runtapes - taper->current_tape);
	for (wtaper1 = taper->wtapetable;
	     wtaper1 < taper->wtapetable + taper->nb_worker;
	     wtaper1++) {
	    if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
		extra_tapes_size += wtaper1->left;
	    }
	    if (wtaper1->job) {
		extra_tapes_size -= (sched(wtaper1->job->disk)->act_size - wtaper1->written);
	    }
	}

	if (wtaper->state & TAPER_STATE_TAPE_STARTED) {
	    taper_left = wtaper->left;
	} else {
	    taper_left = taper->tape_length;
	}
	dp = NULL;
	datestamp = sched((disk_t *)(taper->tapeq.head->data))->datestamp;
	switch(taperalgo) {
	case ALGO_FIRST:
		dp = dequeue_disk(&taper->tapeq);
		break;
	case ALGO_FIRSTFIT:
		fit = taper->tapeq.head;
		while (fit != NULL) {
		    dfit = fit->data;
		    if (sched(dfit)->act_size <=
		             ((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
			     strcmp(sched(dfit)->datestamp, datestamp) <= 0) {
			dp = dfit;
			fit = NULL;
		    } else {
			fit = fit->next;
		    }
		}
		if (dp) remove_disk(&taper->tapeq, dp);
		break;
	case ALGO_LARGEST:
		fit = taper->tapeq.head;
		dp = fit->data;
		while (fit != NULL) {
		    dfit = fit->data;
		    if (sched(dfit)->act_size > sched(dp)->act_size &&
		        strcmp(sched(dfit)->datestamp, datestamp) <= 0) {
			dp = dfit;
		    }
		    fit = fit->next;
		}
		if (dp) remove_disk(&taper->tapeq, dp);
		break;
	case ALGO_LARGESTFIT:
		fit = taper->tapeq.head;
		while (fit != NULL) {
		    dfit = fit->data;
		    if (sched(dfit)->act_size <=
		        ((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
		        (!dp || sched(dfit)->act_size > sched(dp)->act_size) &&
		       strcmp(sched(dfit)->datestamp, datestamp) <= 0) {
			dp = dfit;
		    }
		    fit = fit->next;
		}
		if (dp) remove_disk(&taper->tapeq, dp);
		break;
	case ALGO_SMALLEST:
		fit = taper->tapeq.head;
		dp = fit->data;
		while (fit != NULL) {
		    dfit = fit->data;
		    if (sched(dfit)->act_size < sched(dp)->act_size &&
			strcmp(sched(dfit)->datestamp, datestamp) <= 0) {
			dp = dfit;
		    }
	            fit = fit->next;
		}
		if (dp) remove_disk(&taper->tapeq, dp);
		break;
	case ALGO_SMALLESTFIT:
		fit = taper->tapeq.head;
		while (fit != NULL) {
		    dfit = fit->data;
		    if (sched(dfit)->act_size <=
			((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
			(!dp || sched(dfit)->act_size < sched(dp)->act_size) &&
			strcmp(sched(dfit)->datestamp, datestamp) <= 0) {
			dp = dfit;
		    }
	            fit = fit->next;
		}
		if (dp) remove_disk(&taper->tapeq, dp);
		break;
	case ALGO_LAST:
		dp = taper->tapeq.tail->data;
		if (dp) remove_disk(&taper->tapeq, dp);
		break;
	case ALGO_LASTFIT:
		fit = taper->tapeq.tail;
		while (fit != NULL) {
		    dfit = fit->data;
		    if (sched(dfit)->act_size <=
			((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
			(!dp || sched(dfit)->act_size < sched(dp)->act_size) &&
			strcmp(sched(dfit)->datestamp, datestamp) <= 0) {
			dp = dfit;
		    }
	            fit = fit->prev;
		}
		if(dp) remove_disk(&taper->tapeq, dp);
		break;
	}
	if (!dp) {
	    if (!(result_tape_action & TAPE_ACTION_START_A_FLUSH_FIT)) {
		if(conf_taperalgo != ALGO_SMALLEST)  {
		    g_fprintf(stderr,
			_("driver: startaflush: Using SMALLEST because nothing fit\n"));
		}

		fit = taper->tapeq.head;
		dp = fit->data;
		while (fit != NULL) {
		    dfit = fit->data;
		    if (sched(dfit)->act_size < sched(dp)->act_size &&
			strcmp(sched(dfit)->datestamp, datestamp) <= 0) {
			dp = dfit;
		    }
	            fit = fit->next;
		}
		if(dp) remove_disk(&taper->tapeq, dp);
	    }
	}
	if (dp) {
	    job_t *job = alloc_job();

	    job->wtaper = wtaper;
	    job->disk = dp;
	    wtaper->job = job;

	    amfree(wtaper->input_error);
	    amfree(wtaper->tape_error);
	    wtaper->result = LAST_TOK;
	    wtaper->sendresult = 0;
	    amfree(wtaper->first_label);
	    wtaper->written = 0;
	    wtaper->state &= ~TAPER_STATE_IDLE;
	    wtaper->state |= TAPER_STATE_FILE_TO_TAPE;
	    qname = quote_string(dp->name);
	    if (taper->nb_wait_reply == 0) {
		taper->ev_read = event_register(taper->fd, EV_READFD,
					        handle_taper_result, taper);
	    }
	    taper->nb_wait_reply++;
	    wtaper->nb_dle++;
	    taper_cmd(taper, wtaper, FILE_WRITE, dp, sched(dp)->destname,
		      sched(dp)->level,
		      sched(dp)->datestamp);
	    g_fprintf(stderr,_("driver: startaflush: %s %s %s %lld %lld\n"),
		    taperalgo2str(taperalgo), dp->host->hostname, qname,
		    (long long)sched(job->disk)->act_size,
		    (long long)wtaper->left);
	    amfree(qname);
	    *state_changed = TRUE;
	}
    }
}

static int
client_constrained(
    disk_t *	dp)
{
    disk_t *dp2;

    /* first, check if host is too busy */

    if(dp->host->inprogress >= dp->host->maxdumps) {
	return 1;
    }

    /* next, check conflict with other dumps on same spindle */

    if(dp->spindle == -1) {	/* but spindle -1 never conflicts by def. */
	return 0;
    }

    for(dp2 = dp->host->disks; dp2 != NULL; dp2 = dp2->hostnext)
	if(dp2->inprogress && dp2->spindle == dp->spindle) {
	    return 1;
	}

    return 0;
}

static void
allow_dump_dle(
    disk_t         *diskp,
    wtaper_t       *wtaper,
    char            dumptype,
    disklist_t     *rq,
    const time_t    now,
    int             dumper_to_holding,
    int            *cur_idle,
    disk_t        **delayed_diskp,
    disk_t        **diskp_accept,
    assignedhd_t ***holdp_accept,
    off_t           extra_tapes_size)
{
    assignedhd_t **holdp=NULL;

    /* if the dump can go to that storage */
    if (wtaper) {
	if (wtaper->taper->degraded_mode) {
	    return;
	}
	if (!dump_match_selection(wtaper->taper->storage_name, diskp)) {
	    return;
	}
    }

    if (diskp->host->start_t > now) {
	*cur_idle = max(*cur_idle, IDLE_START_WAIT);
	if (*delayed_diskp == NULL || sleep_time > diskp->host->start_t) {
	    *delayed_diskp = diskp;
	    sleep_time = diskp->host->start_t;
	}
    } else if(diskp->start_t > now) {
	*cur_idle = max(*cur_idle, IDLE_START_WAIT);
	if (*delayed_diskp == NULL || sleep_time > diskp->start_t) {
	    *delayed_diskp = diskp;
	    sleep_time = diskp->start_t;
	}
    } else if (diskp->host->netif->curusage > 0 &&
	       sched(diskp)->est_kps > free_kps(diskp->host->netif)) {
	*cur_idle = max(*cur_idle, IDLE_NO_BANDWIDTH);
    } else if (!wtaper && sched(diskp)->no_space) {
	*cur_idle = max(*cur_idle, IDLE_NO_DISKSPACE);
    } else if (!wtaper && diskp->to_holdingdisk == HOLD_NEVER) {
	*cur_idle = max(*cur_idle, IDLE_NO_HOLD);
    } else if (extra_tapes_size && sched(diskp)->est_size > extra_tapes_size) {
	*cur_idle = max(*cur_idle, IDLE_NO_DISKSPACE);
	/* no tape space */
    } else if (!wtaper && (holdp =
	find_diskspace(sched(diskp)->est_size, cur_idle, NULL)) == NULL) {
	*cur_idle = max(*cur_idle, IDLE_NO_DISKSPACE);
	if (all_tapeq_empty() && dumper_to_holding == 0 && rq != &directq && no_taper_flushing()) {
	    remove_disk(rq, diskp);
	    if (diskp->to_holdingdisk == HOLD_REQUIRED) {
		char *qname = quote_string(diskp->name);
		log_add(L_FAIL, "%s %s %s %d [%s]",
			diskp->host->hostname, qname, sched(diskp)->datestamp,
			sched(diskp)->level,
			_("can't dump required holdingdisk when no holdingdisk space available "));
		amfree(qname);
	    } else {
		enqueue_disk(&directq, diskp);
		diskp->to_holdingdisk = HOLD_NEVER;
	    }
	    if (empty(*rq) && active_dumper() == 0) { force_flush = 1;}
	}
    } else if (client_constrained(diskp)) {
	free_assignedhd(holdp);
	*cur_idle = max(*cur_idle, IDLE_CLIENT_CONSTRAINED);
    } else {

	/* disk fits, dump it */
	int accept = !*diskp_accept;
	if(!accept) {
	    switch(dumptype) {
	      case 's': accept = (sched(diskp)->est_size < sched(*diskp_accept)->est_size);
			break;
	      case 'S': accept = (sched(diskp)->est_size > sched(*diskp_accept)->est_size);
			break;
	      case 't': accept = (sched(diskp)->est_time < sched(*diskp_accept)->est_time);
			break;
	      case 'T': accept = (sched(diskp)->est_time > sched(*diskp_accept)->est_time);
			break;
	      case 'b': accept = (sched(diskp)->est_kps < sched(*diskp_accept)->est_kps);
			break;
	      case 'B': accept = (sched(diskp)->est_kps > sched(*diskp_accept)->est_kps);
			break;
	      default:  log_add(L_WARNING, _("Unknown dumporder character \'%c\', using 's'.\n"),
				dumptype);
			accept = (sched(diskp)->est_size < sched(*diskp_accept)->est_size);
			break;
	    }
	}
	if(accept) {
	    if (!*diskp_accept || (wtaper && !wtaper->taper->degraded_mode) ||
		 diskp->priority >= (*diskp_accept)->priority) {
		if(*holdp_accept) free_assignedhd(*holdp_accept);
		*diskp_accept = diskp;
		*holdp_accept = holdp;
	    }
	    else {
		free_assignedhd(holdp);
	    }
	}
	else {
	    free_assignedhd(holdp);
	}
    }
}

static void
start_some_dumps(
    disklist_t *rq)
{
    const time_t now = time(NULL);
    int cur_idle;
    GList  *dlist, *dlist_next;
    disk_t *diskp, *delayed_diskp, *diskp_accept;
    disk_t *dp;
    assignedhd_t **holdp=NULL, **holdp_accept;
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    chunker_t *chunker;
    dumper_t *dumper;
    wtaper_t *wtaper;
    wtaper_t *wtaper_accept;
    taper_t  *taper;
    char dumptype;
    char *dumporder;
    int  dumper_to_holding = 0;
    gboolean state_changed = FALSE;

    /* don't start any actual dumps until the taper is started */
    if (!taper_started) return;

    idle_reason = IDLE_NO_DUMPERS;
    sleep_time = 0;

    if(dumpers_ev_time != NULL) {
	event_release(dumpers_ev_time);
	dumpers_ev_time = NULL;
    }

    for(dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if (dumper->busy && dumper->job &&
	    dumper->job->disk->to_holdingdisk != HOLD_NEVER) {
	    dumper_to_holding++;
	}
    }
    for (dumper = dmptable; dumper < dmptable+inparallel; dumper++) {
	gboolean directq_is_empty;

	if( dumper->busy || dumper->down) {
	    continue;
	}

	if (dumper->ev_read != NULL) {
	    event_release(dumper->ev_read);
	    dumper->ev_read = NULL;
	}

	/*
	 * A potential problem with starting from the bottom of the dump time
	 * distribution is that a slave host will have both one of the shortest
	 * and one of the longest disks, so starting its shortest disk first will
	 * tie up the host and eliminate its longest disk from consideration the
	 * first pass through.  This could cause a big delay in starting that long
	 * disk, which could drag out the whole night's dumps.
	 *
	 * While starting from the top of the dump time distribution solves the
	 * above problem, this turns out to be a bad idea, because the big dumps
	 * will almost certainly pack the holding disk completely, leaving no
	 * room for even one small dump to start.  This ends up shutting out the
	 * small-end dumpers completely (they stay idle).
	 *
	 * The introduction of multiple simultaneous dumps to one host alleviates
	 * the biggest&smallest dumps problem: both can be started at the
	 * beginning.
	 */

	diskp_accept = NULL;
	holdp_accept = NULL;
	delayed_diskp = NULL;

	cur_idle = NOT_IDLE;

	dumporder = getconf_str(CNF_DUMPORDER);
	if(strlen(dumporder) > (size_t)(dumper-dmptable)) {
	    dumptype = dumporder[dumper-dmptable];
	}
	else {
	    if(dumper-dmptable < 3)
		dumptype = 't';
	    else
		dumptype = 'T';
	}

	diskp = NULL;
	wtaper = NULL;
	directq_is_empty = empty(directq);
	if (!empty(directq)) {  /* to the first allowed storage only */
	    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		if (taper->degraded_mode)
		    continue;
		wtaper_accept = idle_taper(taper);
		if (wtaper_accept) {
		    TapeAction result_tape_action;
		    char *why_no_new_tape = NULL;

		    result_tape_action = tape_action(wtaper_accept,
						     &why_no_new_tape);
		    if (result_tape_action & TAPE_ACTION_START_A_FLUSH ||
			result_tape_action & TAPE_ACTION_START_A_FLUSH_FIT) {
			off_t extra_tapes_size = 0;
			wtaper_t *wtaper1;

			if (result_tape_action & TAPE_ACTION_START_A_FLUSH_FIT) {
			    extra_tapes_size = taper->tape_length *
				(off_t)(taper->runtapes - taper->current_tape);
			    for (wtaper1 = taper->wtapetable;
				 wtaper1 < taper->wtapetable + taper->nb_worker;
				 wtaper1++) {
				if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
				    extra_tapes_size += wtaper1->left;
				}
				if (wtaper1->job) {
				    dp = wtaper1->job->disk;
				    if (dp) {
					extra_tapes_size -=
							 (sched(dp)->est_size -
						         wtaper1->written);
				    }
				}
			    }
			}

			for (dlist = directq.head; dlist != NULL;
						   dlist = dlist_next) {
			    dlist_next = dlist->next;
			    diskp = dlist->data;
		            allow_dump_dle(diskp, wtaper_accept, dumptype, &directq, now,
					   dumper_to_holding, &cur_idle,
					   &delayed_diskp, &diskp_accept,
					   &holdp_accept, extra_tapes_size);
			}
			if (diskp_accept) {
			    diskp = diskp_accept;
			    wtaper = wtaper_accept;
			} else {
			    diskp = NULL;
			    wtaper = NULL;
			}
		    }
		}
	    }
	}

	if (diskp == NULL) {
	    for (dlist = rq->head; dlist != NULL;
				   dlist = dlist_next) {
		dlist_next = dlist->next;
		diskp = dlist->data;
		assert(diskp->host != NULL && sched(diskp) != NULL);

		allow_dump_dle(diskp, NULL, dumptype, rq, now,
			       dumper_to_holding, &cur_idle, &delayed_diskp,
			       &diskp_accept, &holdp_accept, 0);
	    }
	    diskp = diskp_accept;
	    holdp = holdp_accept;
	}

	/* Redo with same dumper if a diskp was moved to directq */
	if (diskp == NULL && directq_is_empty && !empty(directq)) {
	    dumper--;
	    continue;
	}

	idle_reason = max(idle_reason, cur_idle);
	if (diskp == NULL && idle_reason == IDLE_NO_DISKSPACE) {
	    /* continue flush waiting for new tape */
	    startaflush();
	}

	/*
	 * If we have no disk at this point, and there are disks that
	 * are delayed, then schedule a time event to call this dumper
	 * with the disk with the shortest delay.
	 */
	if (diskp == NULL && delayed_diskp != NULL) {
	    assert(sleep_time > now);
	    sleep_time -= now;
	    dumpers_ev_time = event_register((event_id_t)sleep_time, EV_TIME,
		handle_dumpers_time, &runq);
	    return;
	} else if (diskp != NULL && wtaper == NULL) {
	    job_t *job = alloc_job();

	    sched(diskp)->act_size = (off_t)0;
	    allocate_bandwidth(diskp->host->netif, sched(diskp)->est_kps);
	    sched(diskp)->activehd = assign_holdingdisk(holdp, diskp);
	    amfree(holdp);
	    g_free(sched(diskp)->destname);
	    sched(diskp)->destname = g_strdup(sched(diskp)->holdp[0]->destname);
	    diskp->host->inprogress++;	/* host is now busy */
	    diskp->inprogress = 1;
	    job->disk = diskp;
	    job->dumper = dumper;
	    dumper->job = job;
	    sched(diskp)->timestamp = now;
	    amfree(diskp->dataport_list);

	    dumper->busy = 1;		/* dumper is now busy */
	    remove_disk(rq, diskp);		/* take it off the run queue */

	    sched(diskp)->origsize = (off_t)-1;
	    sched(diskp)->dumpsize = (off_t)-1;
	    sched(diskp)->dumptime = (time_t)0;
	    chunker = &chktable[dumper - dmptable];
	    job->chunker = chunker;
	    chunker->job = job;
	    chunker->result = LAST_TOK;
	    chunker->sendresult = 0;
	    dumper->result = LAST_TOK;
	    startup_chunk_process(chunker,chunker_program);
	    chunker_cmd(chunker, START, NULL, driver_timestamp);
	    chunker_cmd(chunker, PORT_WRITE, diskp, sched(diskp)->datestamp);
	    cmd = getresult(chunker->fd, 1, &result_argc, &result_argv);
	    if(cmd != PORT) {
		assignedhd_t **h=NULL;
		int activehd;
		char *qname = quote_string(diskp->name);

		g_printf(_("driver: did not get PORT from %s for %s:%s\n"),
		       chunker->name, diskp->host->hostname, qname);
		amfree(qname);
		fflush(stdout);

		deallocate_bandwidth(diskp->host->netif, sched(diskp)->est_kps);
		h = sched(diskp)->holdp;
		activehd = sched(diskp)->activehd;
		h[activehd]->used = 0;
		h[activehd]->disk->allocated_dumpers--;
		adjust_diskspace(diskp, DONE);
		delete_diskspace(diskp);
		diskp->host->inprogress--;
		diskp->inprogress = 0;
		dumper->busy = 0;
		sched(diskp)->dump_attempted++;
		free_serial_job(job);
		free_job(job);
		if(sched(diskp)->dump_attempted < diskp->retry_dump)
		    enqueue_disk(rq, diskp);
	    }
	    else {
		dumper->ev_read = event_register((event_id_t)dumper->fd, EV_READFD,
						 handle_dumper_result, dumper);
		chunker->ev_read = event_register((event_id_t)chunker->fd, EV_READFD,
						   handle_chunker_result, chunker);
		dumper->output_port = atoi(result_argv[2]);
		amfree(diskp->dataport_list);
		diskp->dataport_list = g_strdup(result_argv[3]);

		if (diskp->host->pre_script == 0) {
		    run_server_host_scripts(EXECUTE_ON_PRE_HOST_BACKUP,
					    get_config_name(), diskp->host);
		    diskp->host->pre_script = 1;
		}
		run_server_dle_scripts(EXECUTE_ON_PRE_DLE_BACKUP,
				   get_config_name(), diskp,
				   sched(diskp)->level);
		dumper_cmd(dumper, PORT_DUMP, diskp, NULL);
	    }
	    diskp->host->start_t = now + 5;
	    if (empty(*rq) && active_dumper() == 0) { force_flush = 1;}

	    if (result_argv)
		g_strfreev(result_argv);
	    short_dump_state();
	} else if (diskp != NULL && wtaper != NULL) { /* dump to tape */
	    job_t *job = alloc_job();

	    sched(diskp)->act_size = (off_t)0;
	    allocate_bandwidth(diskp->host->netif, sched(diskp)->est_kps);
	    diskp->host->inprogress++;	/* host is now busy */
	    diskp->inprogress = 1;
	    job->disk = diskp;
	    job->dumper = dumper;
	    job->wtaper = wtaper;
	    wtaper->job = job;
	    dumper->job = job;

	    sched(diskp)->timestamp = now;
	    amfree(diskp->dataport_list);

	    dumper->busy = 1;		/* dumper is now busy */
	    remove_disk(&directq, diskp);  /* take it off the direct queue */

	    sched(diskp)->origsize = (off_t)-1;
	    sched(diskp)->dumpsize = (off_t)-1;
	    sched(diskp)->dumptime = (time_t)0;
	    dumper->result = LAST_TOK;
	    wtaper->result = LAST_TOK;
	    wtaper->input_error = NULL;
	    wtaper->tape_error = NULL;
	    wtaper->first_label = NULL;
	    wtaper->written = 0;
	    wtaper->state |= TAPER_STATE_DUMP_TO_TAPE;
	    wtaper->state &= ~TAPER_STATE_IDLE;
	    wtaper->nb_dle++;
	    taper = wtaper->taper;
	    if (taper->nb_wait_reply == 0) {
		taper->ev_read = event_register(taper->fd, EV_READFD,
					       handle_taper_result, taper);
	    }

	    taper->nb_wait_reply++;
	    taper_cmd(taper, wtaper, PORT_WRITE, diskp, NULL, sched(diskp)->level,
		      sched(diskp)->datestamp);
	    wtaper->ready = FALSE;
	    diskp->host->start_t = now + 5;

	    state_changed = TRUE;
	}
    }
    if (state_changed) {
	short_dump_state();
    }
}

/*
 * This gets called when a dumper is delayed for some reason.  It may
 * be because a disk has a delayed start, or amanda is constrained
 * by network or disk limits.
 */

static void
handle_dumpers_time(
    void *	cookie)
{
    disklist_t *runq = cookie;
    event_release(dumpers_ev_time);
    dumpers_ev_time = NULL;
    start_some_dumps(runq);
}

static void
dump_schedule(
    disklist_t *qp,
    char *	str)
{
    GList  *dlist;
    disk_t *dp;
    char *qname;

    g_printf(_("dump of driver schedule %s:\n--------\n"), str);

    for(dlist = qp->head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
        qname = quote_string(dp->name);
	g_printf("  %-20s %-25s lv %d t %5lu s %lld p %d\n",
	       dp->host->hostname, qname, sched(dp)->level,
	       sched(dp)->est_time,
	       (long long)sched(dp)->est_size, sched(dp)->priority);
        amfree(qname);
    }
    g_printf("--------\n");
}

static void
start_degraded_mode(
    /*@keep@*/ disklist_t *queuep)
{
    disk_t *dp;
    disklist_t newq;
    off_t est_full_size;
    char *qname;
    taper_t  *taper;
    static gboolean schedule_degraded = FALSE;

    if (!schedule_done || all_degraded_mode) {
	return;
    }

    if (!schedule_degraded) {
	gboolean one_degraded_mode = FALSE;
	schedule_degraded = TRUE;

	for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	    one_degraded_mode |= taper->degraded_mode;
	}
	if (!one_degraded_mode)
	    return;
    }

    newq.head = newq.tail = 0;

    dump_schedule(queuep, _("before start degraded mode"));

    est_full_size = (off_t)0;
    while(!empty(*queuep)) {
	dp = dequeue_disk(queuep);

	qname = quote_string(dp->name);
	if(sched(dp)->level != 0)
	    /* go ahead and do the disk as-is */
	    enqueue_disk(&newq, dp);
	else {
	    gboolean must_degrade_dp = FALSE;
	    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		if (taper->degraded_mode &&
		    dump_match_selection(taper->storage_name, dp)) {
		    must_degrade_dp = TRUE;
		}
	    }
	    if (!must_degrade_dp) {
		/* go ahead and do the disk as-is */
		enqueue_disk(&newq, dp);
	    } else if (reserved_space + est_full_size + sched(dp)->est_size
		<= total_disksize) {
		enqueue_disk(&newq, dp);
		est_full_size += sched(dp)->est_size;
	    }
	    else if(sched(dp)->degr_level != -1) {
		sched(dp)->level = sched(dp)->degr_level;
		sched(dp)->dumpdate = sched(dp)->degr_dumpdate;
		sched(dp)->est_nsize = sched(dp)->degr_nsize;
		sched(dp)->est_csize = sched(dp)->degr_csize;
		sched(dp)->est_time = sched(dp)->degr_time;
		sched(dp)->est_kps  = sched(dp)->degr_kps;
		enqueue_disk(&newq, dp);
	    }
	    else {
		log_add(L_FAIL, "%s %s %s %d [%s]",
		        dp->host->hostname, qname, sched(dp)->datestamp,
			sched(dp)->level, sched(dp)->degr_mesg);
	    }
	}
        amfree(qname);
    }

    /*@i@*/ *queuep = newq;
    all_degraded_mode = TRUE;
    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	all_degraded_mode &= taper->degraded_mode;
    }

    dump_schedule(queuep, _("after start degraded mode"));
}


static void
continue_port_dumps(void)
{
    GList  *dlist, *dlist_next;
    disk_t *dp;
    job_t  *job = NULL;
    assignedhd_t **h;
    int active_dumpers=0, busy_dumpers=0, i;
    dumper_t *dumper;

    /* First we try to grant diskspace to some dumps waiting for it. */
    for(dlist = roomq.head; dlist != NULL; dlist = dlist_next) {
	dlist_next = dlist->next;
	dp = dlist->data;
	/* find last holdingdisk used by this dump */
	for( i = 0, h = sched(dp)->holdp; h[i+1]; i++ ) {
	    (void)h; /* Quiet lint */
	}
	/* find more space */
	h = find_diskspace( sched(dp)->est_size - sched(dp)->act_size,
			    &active_dumpers, h[i] );
	if( h ) {
	    for(dumper = dmptable; dumper < dmptable + inparallel &&
				   dumper->job &&
				   dumper->job->disk != dp; dumper++) {
		(void)dp; /* Quiet lint */
	    }
	    assert( dumper < dmptable + inparallel );
	    assert( dumper->job );
	    sched(dp)->activehd = assign_holdingdisk( h, dp );
	    chunker_cmd( dumper->job->chunker, CONTINUE, dp, NULL );
	    amfree(h);
	    remove_disk( &roomq, dp );
	}
    }

    /* So for some disks there is less holding diskspace available than
     * was asked for. Possible reasons are
     * a) diskspace has been allocated for other dumps which are
     *    still running or already being written to tape
     * b) all other dumps have been suspended due to lack of diskspace
     * Case a) is not a problem. We just wait for the diskspace to
     * be freed by moving the current disk to a queue.
     * If case b) occurs, we have a deadlock situation. We select
     * a dump from the queue to be aborted and abort it. It will
     * be retried directly to tape.
     */
    for(dp=NULL, dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if( dumper->busy ) {
	    busy_dumpers++;
	    if( !find_disk(&roomq, dumper->job->disk) ) {
		if (dumper->job->chunker) {
		    active_dumpers++;
		}
	    } else if( !dp ||
		       sched(dp)->est_size > sched(dumper->job->disk)->est_size ) {
		dp = dumper->job->disk;
		job = dumper->job;
	    }
	}
    }
    if((dp != NULL) && (active_dumpers == 0) && (busy_dumpers > 0) &&
        ((no_taper_flushing() && all_tapeq_empty()) || all_degraded_mode) &&
	pending_aborts == 0 ) { /* case b */
	sched(dp)->no_space = 1;
	/* At this time, dp points to the dump with the smallest est_size.
	 * We abort that dump, hopefully not wasting too much time retrying it.
	 */
	remove_disk( &roomq, dp );
	chunker_cmd(job->chunker, ABORT, NULL, _("Not enough holding disk space"));
	dumper_cmd(job->dumper, ABORT, NULL, _("Not enough holding disk space"));
	pending_aborts++;
    }
}


static int
all_tapeq_empty(void)
{
    taper_t *taper;

    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	if (!empty(taper->tapeq))
	    return 0;
    }
    return 1;
}

static void
handle_taper_result(
	void *cookie G_GNUC_UNUSED)
{
    disk_t *dp = NULL;
    job_t *job = NULL;
    dumper_t *dumper;
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    char *qname, *q;
    char *s;
    taper_t  *taper;
    wtaper_t *wtaper = NULL;
    wtaper_t *wtaper1;
    int      i;
    off_t    partsize;

    assert(cookie != NULL);
    taper = cookie;

    do {

	short_dump_state();
	wtaper = NULL;

	cmd = getresult(taper->fd, 1, &result_argc, &result_argv);

	switch(cmd) {

	case TAPER_OK:
	    if(result_argc != 2) {
		error(_("error: [taper FAILED result_argc != 2: %d"), result_argc);
		/*NOTREACHED*/
	    }

	    wtaper = NULL;
	    taper_started = 1;
	    for (i=0; i < taper->nb_worker; i++) {
		if (g_str_equal(taper->wtapetable[i].name, result_argv[1])) {
		    wtaper= &taper->wtapetable[i];
		}
	    }
	    assert(wtaper != NULL);
	    wtaper->left = 0;
	    wtaper->nb_dle = 0;
	    wtaper->job = NULL;
	    wtaper->state &= ~TAPER_STATE_INIT;
	    wtaper->state |= TAPER_STATE_RESERVATION;
	    wtaper->state |= TAPER_STATE_IDLE;
	    amfree(wtaper->first_label);
	    taper->nb_wait_reply--;
	    taper->nb_scan_volume--;
	    taper->last_started_wtaper = wtaper;
	    if (taper->nb_wait_reply == 0) {
		event_release(taper->ev_read);
		taper->ev_read = NULL;
	    }
	    start_some_dumps(&runq);
	    startaflush();
	    break;

	case FAILED:	/* FAILED <worker> <handle> INPUT-* TAPE-* <input err mesg> <tape err mesg> */
	    if(result_argc != 7) {
		error(_("error: [taper FAILED result_argc != 7: %d"), result_argc);
		/*NOTREACHED*/
	    }

	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    qname = quote_string(dp->name);
	    g_printf(_("driver: finished-cmd time %s taper %s worker %s wrote %s:%s\n"),
		   walltime_str(curclock()), taper->name, wtaper->name, dp->host->hostname, qname);
	    fflush(stdout);

	    wtaper->result = cmd;
	    if (job->dumper && !dp->dataport_list) {
		job->dumper->result = FAILED;
	    }
	    if (g_str_equal(result_argv[3], "INPUT-ERROR")) {
		g_free(wtaper->input_error);
		wtaper->input_error = g_strdup(result_argv[5]);
		amfree(qname);
		break;
	    } else if (!g_str_equal(result_argv[3], "INPUT-GOOD")) {
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(_("Taper protocol error"));
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, wtaper->tape_error);
		amfree(qname);
		break;
	    }
	    if (g_str_equal(result_argv[4], "TAPE-ERROR") ||
		g_str_equal(result_argv[4], "TAPE-CONFIG")) {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(result_argv[6]);
		amfree(qname);
		break;
	    } else if (!g_str_equal(result_argv[4], "TAPE-GOOD")) {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(_("Taper protocol error"));
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, wtaper->tape_error);
		amfree(qname);
		break;
	    }

	    amfree(qname);

	    break;

	case READY:	/* READY <worker> <handle> */
	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    wtaper->ready = TRUE;
	    if (wtaper->job->dumper &&
		wtaper->job->dumper->result != LAST_TOK) {
		if( wtaper->job->dumper->result == DONE) {
		    taper_cmd(taper, wtaper, DONE, dp, NULL, 0, NULL);
		} else {
		    taper_cmd(taper, wtaper, FAILED, dp, NULL, 0, NULL);
		}
	    }
	    break;

	case PARTIAL:	/* PARTIAL <worker> <handle> INPUT-* TAPE-* server-crc <stat mess> <input err mesg> <tape err mesg>*/
	case DONE:	/* DONE <worker> <handle> INPUT-GOOD TAPE-GOOD server-crc <stat mess> <input err mesg> <tape err mesg> */
	    if(result_argc != 9) {
		error(_("error: [taper PARTIAL result_argc != 9: %d"), result_argc);
		/*NOTREACHED*/
	    }

	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    qname = quote_string(dp->name);
	    g_printf(_("driver: finished-cmd time %s taper %s worker %s wrote %s:%s\n"),
		   walltime_str(curclock()), taper->name, wtaper->name, dp->host->hostname, qname);
	    fflush(stdout);

	    if (g_str_equal(result_argv[3], "INPUT-ERROR")) {
		g_free(wtaper->input_error);
		wtaper->input_error = g_strdup(result_argv[7]);
		wtaper->result = FAILED;
		amfree(qname);
		break;
	    } else if (!g_str_equal(result_argv[3], "INPUT-GOOD")) {
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(_("Taper protocol error"));
		wtaper->result = FAILED;
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, wtaper->tape_error);
		amfree(qname);
		break;
	    }
	    if (g_str_equal(result_argv[4], "TAPE-ERROR") ||
		g_str_equal(result_argv[4], "TAPE-CONFIG")) {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(result_argv[8]);
		wtaper->result = FAILED;
		amfree(qname);
		break;
	    } else if (!g_str_equal(result_argv[4], "TAPE-GOOD")) {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(_("Taper protocol error"));
		wtaper->result = FAILED;
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, wtaper->tape_error);
		amfree(qname);
		break;
	    }

	    parse_crc(result_argv[5], &sched(dp)->server_crc);

	    s = strstr(result_argv[6], " kb ");
	    if (s) {
		s += 4;
		sched(dp)->dumpsize = OFF_T_ATOI(s);
	    } else {
		s = strstr(result_argv[6], " bytes ");
		if (s) {
		    s += 7;
		    sched(dp)->dumpsize = OFF_T_ATOI(s)/1024;
		}
	    }

	    wtaper->result = cmd;
	    amfree(qname);

	    break;

        case PARTDONE:  /* PARTDONE <worker> <handle> <label> <fileno> <kbytes> <stat> */
	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

            if (result_argc != 7) {
                error(_("error [taper PARTDONE result_argc != 7: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }
	    if (!wtaper->first_label) {
		amfree(wtaper->first_label);
		wtaper->first_label = g_strdup(result_argv[3]);
		wtaper->first_fileno = OFF_T_ATOI(result_argv[4]);
	    }
	    wtaper->written += OFF_T_ATOI(result_argv[5]);
	    if (wtaper->written > sched(dp)->act_size)
		sched(dp)->act_size = wtaper->written;

	    partsize = 0;
	    s = strstr(result_argv[6], " kb ");
	    if (s) {
		s += 4;
		partsize = OFF_T_ATOI(s);
	    } else {
		s = strstr(result_argv[6], " bytes ");
		if (s) {
		    s += 7;
		    partsize = OFF_T_ATOI(s)/1024;
		}
	    }
	    wtaper->left -= partsize;

            break;

        case REQUEST_NEW_TAPE:  /* REQUEST-NEW-TAPE <worker> <handle> */
            if (result_argc != 3) {
                error(_("error [taper REQUEST_NEW_TAPE result_argc != 3: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }

	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    if (wtaper->state & TAPER_STATE_DONE) {
		taper_cmd(taper, wtaper, NO_NEW_TAPE, job->disk, "taper found no tape", 0, NULL);
	    } else {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		wtaper->state |= TAPER_STATE_TAPE_REQUESTED;

		start_some_dumps(&runq);
		startaflush();
	    }
	    break;

	case NEW_TAPE: /* NEW-TAPE <worker> <handle> <label> */
            if (result_argc != 4) {
                error(_("error [taper NEW_TAPE result_argc != 4: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }

	    nb_sent_new_tape--;
	    taper->nb_scan_volume--;

	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

            /* Update our tape counter and reset taper->left */
	    taper->current_tape++;
	    wtaper->nb_dle = 1;
	    wtaper->left = taper->tape_length;
	    wtaper->state &= ~TAPER_STATE_WAIT_NEW_TAPE;
	    wtaper->state |= TAPER_STATE_TAPE_STARTED;
	    taper->last_started_wtaper = NULL;

	    /* start a new worker */
	    for (i = 0; i < taper->nb_worker ; i++) {
		wtaper1 = &taper->wtapetable[i];
		if (!taper->degraded_mode &&
		    wtaper1->state == TAPER_STATE_DEFAULT) {
		    wtaper1->state = TAPER_STATE_INIT;
		    if (taper->nb_wait_reply == 0) {
			taper->ev_read = event_register(taper->fd, EV_READFD,
						handle_taper_result, NULL);
		    }
		    taper->nb_wait_reply++;
		    taper->nb_scan_volume++;
		    taper_cmd(taper, wtaper1, START_TAPER, NULL, wtaper1->name, 0,
			      driver_timestamp);
		    break;
		}
	    }
	    break;

	case NO_NEW_TAPE:  /* NO-NEW-TAPE <worker> <handle> */
            if (result_argc != 3) {
                error(_("error [taper NO_NEW_TAPE result_argc != 3: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }
	    nb_sent_new_tape--;
	    taper_nb_scan_volume--;

	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    wtaper->state |= TAPER_STATE_DONE;
	    taper->last_started_wtaper = NULL;
	    taper->degraded_mode = TRUE;
	    start_degraded_mode(&runq);
	    break;

	case DUMPER_STATUS:  /* DUMPER-STATUS <worker> <handle> */
            if (result_argc != 3) {
                error(_("error [taper DUMPER_STATUS result_argc != 3: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }

	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    if (wtaper->job->dumper->result == LAST_TOK) {
		wtaper->sendresult = 1;
	    } else {
		if( wtaper->job->dumper->result == DONE) {
		    taper_cmd(taper, wtaper, DONE, dp, NULL, 0, NULL);
		} else {
		    taper_cmd(taper, wtaper, FAILED, dp, NULL, 0, NULL);
		}
	    }
	    break;

        case TAPE_ERROR: /* TAPE-ERROR <worker> <err mess> */
	    taper_started = 1;
	    if (g_str_equal(result_argv[1], "SETUP")) {
		taper->nb_wait_reply = 0;
		taper->nb_scan_volume = 0;
	    } else {
		wtaper = wtaper_from_name(taper, result_argv[1]);
		assert(wtaper);
		wtaper->state = TAPER_STATE_DONE;
		fflush(stdout);
		q = quote_string(result_argv[2]);
		log_add(L_WARNING, _("Taper error: %s"), q);
		amfree(q);
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(result_argv[2]);

		taper->nb_wait_reply--;
		taper->nb_scan_volume--;
	    }
	    if (taper->nb_wait_reply == 0) {
		event_release(taper->ev_read);
		taper->ev_read = NULL;
	    }
	    taper->degraded_mode = TRUE;
	    start_degraded_mode(&runq);
	    start_some_dumps(&runq);
	    break;

	case PORT: /* PORT <worker> <handle> <port> <dataport_list> */
	    job = serial2job(result_argv[2]);
	    dp = job->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    dumper = job->dumper;
	    dumper->output_port = atoi(result_argv[3]);
	    amfree(dp->dataport_list);
	    dp->dataport_list = g_strdup(result_argv[4]);

	    amfree(wtaper->input_error);
	    amfree(wtaper->tape_error);
	    amfree(wtaper->first_label);
	    wtaper->written = 0;
	    wtaper->state |= TAPER_STATE_DUMP_TO_TAPE;

	    if (dp->host->pre_script == 0) {
		run_server_host_scripts(EXECUTE_ON_PRE_HOST_BACKUP,
					get_config_name(), dp->host);
		dp->host->pre_script = 1;
	    }
	    run_server_dle_scripts(EXECUTE_ON_PRE_DLE_BACKUP,
			       get_config_name(), dp,
			       sched(dp)->level);
	    /* tell the dumper to dump to a port */
	    dumper_cmd(dumper, PORT_DUMP, dp, NULL);
	    dp->host->start_t = time(NULL) + 5;

	    wtaper->state |= TAPER_STATE_DUMP_TO_TAPE;

	    dumper->ev_read = event_register(dumper->fd, EV_READFD,
					     handle_dumper_result, dumper);
	    break;

        case BOGUS:
            log_add(L_WARNING, _("Taper protocol error"));
            /*
             * Since we received a taper error, we can't send anything more
             * to the taper.  Go into degraded mode to try to get everthing
             * onto disk.  Later, these dumps can be flushed to a new tape.
             * The tape queue is zapped so that it appears empty in future
             * checks. If there are dumps waiting for diskspace to be freed,
             * cancel one.
             */
	    taper_started = 1;
            if(!nodump) {
                log_add(L_WARNING,
                        _("going into degraded mode because of taper component error."));
	    }

	    for (wtaper = taper->wtapetable;
		 wtaper < taper->wtapetable + taper->nb_worker;
                 wtaper++) {
		if (wtaper && wtaper->job && wtaper->job->disk) {
		    g_free(wtaper->tape_error);
		    wtaper->tape_error = g_strdup("BOGUS");
		    wtaper->result = cmd;
		    if (wtaper->job->dumper) {
			if (wtaper->job->dumper->result != LAST_TOK) {
			    // Dumper already returned it's result
			    dumper_taper_result(wtaper->job);
			}
		    } else {
			file_taper_result(wtaper->job);
		    }
		}
	    }
	    wtaper = NULL;

            if(taper->ev_read != NULL) {
                event_release(taper->ev_read);
                taper->ev_read = NULL;
		taper->nb_wait_reply = 0;
            }
	    taper->degraded_mode = TRUE;
	    start_degraded_mode(&runq);
            taper->tapeq.head = taper->tapeq.tail = NULL;
            aclose(taper->fd);

            break;

	default:
            error(_("driver received unexpected token (%s) from taper"),
                  cmdstr[cmd]);
	    /*NOTREACHED*/
	}

	g_strfreev(result_argv);

	if (wtaper && job && job->disk && wtaper->result != LAST_TOK) {
	    if (wtaper->nb_dle >= taper->max_dle_by_volume) {
		taper_cmd(taper, wtaper, CLOSE_VOLUME, job->disk, NULL, 0, NULL);
	    }
	    if (job->dumper) {
		if (job->dumper->result != LAST_TOK) {
		    // Dumper already returned it's result
		    dumper_taper_result(job);
		}
	    } else {
		file_taper_result(job);
	    }
	}

    } while(areads_dataready(taper->fd));
    start_some_dumps(&runq);
    startaflush();
}


static void
file_taper_result(
    job_t *job)
{
    disk_t   *dp     = job->disk;
    wtaper_t *wtaper = job->wtaper;
    taper_t  *taper  = wtaper->taper;
    char    *qname = quote_string(dp->name);

    if (wtaper->result == DONE) {
	update_info_taper(dp, wtaper->first_label, wtaper->first_fileno,
			  sched(dp)->level);
    }

    sched(dp)->taper_attempted += 1;

    job->wtaper = NULL;

    if (wtaper->input_error) {
	g_printf("driver: taper failed %s %s: %s\n",
		   dp->host->hostname, qname, wtaper->input_error);
	if (g_str_equal(sched(dp)->datestamp, driver_timestamp)) {
	    if(sched(dp)->taper_attempted >= dp->retry_dump) {
		log_add(L_FAIL, _("%s %s %s %d [too many taper retries after holding disk error: %s]"),
		    dp->host->hostname, qname, sched(dp)->datestamp,
		    sched(dp)->level, wtaper->input_error);
		g_printf("driver: taper failed %s %s, too many taper retry after holding disk error\n",
		   dp->host->hostname, qname);
		if (sched(dp)->nb_flush == 0) {
		    amfree(sched(dp)->destname);
		    amfree(sched(dp)->dumpdate);
		    amfree(sched(dp)->degr_dumpdate);
		    amfree(sched(dp)->degr_mesg);
		    amfree(sched(dp)->datestamp);
		    g_hash_table_destroy(sched(dp)->to_storage);
		    amfree(dp->up);
		}
	    } else {
		log_add(L_INFO, _("%s %s %s %d [Will retry dump because of holding disk error: %s]"),
			dp->host->hostname, qname, sched(dp)->datestamp,
			sched(dp)->level, wtaper->input_error);
		g_printf("driver: taper will retry %s %s because of holding disk error\n",
			dp->host->hostname, qname);
		if (dp->to_holdingdisk != HOLD_REQUIRED) {
		    dp->to_holdingdisk = HOLD_NEVER;
		    sched(dp)->dump_attempted -= 1;
		    headqueue_disk(&directq, dp);
		} else {
		    if (sched(dp)->nb_flush == 0) {
			amfree(sched(dp)->destname);
			amfree(sched(dp)->dumpdate);
			amfree(sched(dp)->degr_dumpdate);
			amfree(sched(dp)->degr_mesg);
			amfree(sched(dp)->datestamp);
			g_hash_table_destroy(sched(dp)->to_storage);
			amfree(dp->up);
		    }
		}
	    }
	} else {
	    if (sched(dp)->nb_flush == 0) {
		amfree(sched(dp)->destname);
		amfree(sched(dp)->dumpdate);
		amfree(sched(dp)->degr_dumpdate);
		amfree(sched(dp)->degr_mesg);
		amfree(sched(dp)->datestamp);
		g_hash_table_destroy(sched(dp)->to_storage);
		amfree(dp->up);
	    }
	}
    } else if (wtaper->tape_error) {
	g_printf("driver: taper failed %s %s with tape error: %s\n",
		   dp->host->hostname, qname, wtaper->tape_error);
	if(sched(dp)->taper_attempted >= dp->retry_dump) {
	    log_add(L_FAIL, _("%s %s %s %d [too many taper retries]"),
		    dp->host->hostname, qname, sched(dp)->datestamp,
		    sched(dp)->level);
	    g_printf("driver: taper failed %s %s, too many taper retry\n",
		   dp->host->hostname, qname);
	    if (sched(dp)->nb_flush == 0) {
		amfree(sched(dp)->destname);
		amfree(sched(dp)->dumpdate);
		amfree(sched(dp)->degr_dumpdate);
		amfree(sched(dp)->degr_mesg);
		amfree(sched(dp)->datestamp);
		g_hash_table_destroy(sched(dp)->to_storage);
		amfree(dp->up);
	    }
	} else {
	    g_printf("driver: taper will retry %s %s\n",
		   dp->host->hostname, qname);
	    /* Re-insert into taper queue. */
	    headqueue_disk(&wtaper->taper->tapeq, dp);
	}
    } else if (wtaper->result != DONE) {
	g_printf("driver: taper failed %s %s without error\n",
		   dp->host->hostname, qname);
    } else {
	int        id;
	cmddata_t *cmddata;
	char      *holding_file;

	id = GPOINTER_TO_INT(g_hash_table_lookup(sched(dp)->to_storage,
						 taper->storage_name));
	cmddata = g_hash_table_lookup(cmddatas->cmdfile, GINT_TO_POINTER(id));
	holding_file = g_strdup(cmddata->holding_file);
	remove_cmd_in_cmdfile(cmddatas, id);
	sched(dp)->nb_flush--;
	if (sched(dp)->nb_flush == 0) {
	    if (!holding_in_cmdfile(cmddatas, holding_file)) {
		delete_diskspace(dp);
	    }
	    amfree(sched(dp)->destname);
	    amfree(sched(dp)->dumpdate);
	    amfree(sched(dp)->degr_dumpdate);
	    amfree(sched(dp)->degr_mesg);
	    amfree(sched(dp)->datestamp);
	    g_hash_table_destroy(sched(dp)->to_storage);
	    amfree(dp->up);
	}
	g_free(holding_file);
    }

    amfree(qname);

    free_serial_job(job);
    free_job(job);
    wtaper->job = NULL;

    wtaper->state &= ~TAPER_STATE_FILE_TO_TAPE;
    wtaper->state |= TAPER_STATE_IDLE;
    amfree(wtaper->input_error);
    amfree(wtaper->tape_error);
    taper->nb_wait_reply--;
    if (taper->nb_wait_reply == 0) {
	event_release(taper->ev_read);
	taper->ev_read = NULL;
    }

    /* continue with those dumps waiting for diskspace */
    continue_port_dumps();
    start_some_dumps(&runq);
    startaflush();
}

static void
dumper_taper_result(
    job_t *job)
{

    disk_t   *dp     = job->disk;
    wtaper_t *wtaper = job->wtaper;
    taper_t  *taper  = wtaper->taper;
    dumper_t *dumper = job->dumper;
    char     *qname;

    if(dumper->result == DONE && wtaper->result == DONE) {
	update_info_dumper(dp, sched(dp)->origsize,
			   sched(dp)->dumpsize, sched(dp)->dumptime);
	update_info_taper(dp, wtaper->first_label, wtaper->first_fileno,
			  sched(dp)->level);
	qname = quote_string(dp->name); /*quote to take care of spaces*/

	log_add(L_STATS, _("estimate %s %s %s %d [sec %ld nkb %lld ckb %lld kps %lu]"),
		dp->host->hostname, qname, sched(dp)->datestamp,
		sched(dp)->level,
		sched(dp)->est_time, (long long)sched(dp)->est_nsize,
		(long long)sched(dp)->est_csize,
		sched(dp)->est_kps);
	amfree(qname);

	fix_index_header(dp);
    } else {
	update_failed_dump(dp);
    }

    if (dumper->result != RETRY) {
	sched(dp)->dump_attempted += 1;
	sched(dp)->taper_attempted += 1;
    }

    if((dumper->result != DONE || wtaper->result != DONE) &&
	sched(dp)->dump_attempted < dp->retry_dump &&
	sched(dp)->taper_attempted < dp->retry_dump) {
	enqueue_disk(&directq, dp);
    }

    if(dumper->ev_read != NULL) {
	event_release(dumper->ev_read);
	dumper->ev_read = NULL;
    }
    taper->nb_wait_reply--;
    if (taper->nb_wait_reply == 0 && taper->ev_read != NULL) {
	event_release(taper->ev_read);
	taper->ev_read = NULL;
    }
    wtaper->state &= ~TAPER_STATE_DUMP_TO_TAPE;
    wtaper->state |= TAPER_STATE_IDLE;
    amfree(wtaper->input_error);
    amfree(wtaper->tape_error);
    dumper->busy = 0;
    dp->host->inprogress -= 1;
    dp->inprogress = 0;
    deallocate_bandwidth(dp->host->netif, sched(dp)->est_kps);
    free_serial_job(job);
    free_job(job);
    dumper->job = NULL;
    wtaper->job = NULL;
    start_some_dumps(&runq);
}


static wtaper_t *
idle_taper(taper_t *taper)
{
    wtaper_t *wtaper;

    /* Use an already started taper first */
    for (wtaper = taper->wtapetable;
	 wtaper < taper->wtapetable + taper->nb_worker;
	 wtaper++) {
	if ((wtaper->state & TAPER_STATE_IDLE) &&
	    (wtaper->state & TAPER_STATE_TAPE_STARTED) &&
	    !(wtaper->state & TAPER_STATE_DONE) &&
	    !(wtaper->state & TAPER_STATE_FILE_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_FILE_TO_TAPE))
	    return wtaper;
    }
    for (wtaper = taper->wtapetable;
	 wtaper < taper->wtapetable + taper->nb_worker;
	 wtaper++) {
	if ((wtaper->state & TAPER_STATE_IDLE) &&
	    (wtaper->state & TAPER_STATE_RESERVATION) &&
	    !(wtaper->state & TAPER_STATE_DONE) &&
	    !(wtaper->state & TAPER_STATE_FILE_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_FILE_TO_TAPE))
	    return wtaper;
    }
    return NULL;
}

static wtaper_t *
wtaper_from_name(
    taper_t *taper,
    char    *name)
{
    wtaper_t *wtaper;

    for (wtaper = taper->wtapetable;
	 wtaper < taper->wtapetable + taper->nb_worker;
	 wtaper++)
	if (g_str_equal(wtaper->name, name)) return wtaper;

    return NULL;
}

static void
dumper_chunker_result(
    job_t *job)
{
    dumper_t  *dumper;
    chunker_t *chunker;
    taper_t   *taper;
    disk_t    *dp;
    assignedhd_t **h=NULL;
    int activehd, i;
    off_t dummy;
    off_t size;
    int is_partial;

    dumper  = job->dumper;
    chunker = job->chunker;
    dp      = job->disk;

    h = sched(dp)->holdp;
    activehd = sched(dp)->activehd;

    if(dumper->result == DONE && chunker->result == DONE) {
	char *qname = quote_string(dp->name);/*quote to take care of spaces*/
	update_info_dumper(dp, sched(dp)->origsize,
			   sched(dp)->dumpsize, sched(dp)->dumptime);

	log_add(L_STATS, _("estimate %s %s %s %d [sec %ld nkb %lld ckb %lld kps %lu]"),
		dp->host->hostname, qname, sched(dp)->datestamp,
		sched(dp)->level,
		sched(dp)->est_time, (long long)sched(dp)->est_nsize,
                (long long)sched(dp)->est_csize,
		sched(dp)->est_kps);
	amfree(qname);
    } else {
	update_failed_dump(dp);
    }

    deallocate_bandwidth(dp->host->netif, sched(dp)->est_kps);

    is_partial = dumper->result != DONE || chunker->result != DONE;
    rename_tmp_holding(sched(dp)->destname, !is_partial);
    holding_set_from_driver(sched(dp)->destname, sched(dp)->origsize,
		sched(dp)->native_crc, sched(dp)->client_crc,
		sched(dp)->server_crc);
    fix_index_header(dp);

    dummy = (off_t)0;
    for( i = 0, h = sched(dp)->holdp; i < activehd; i++ ) {
	dummy += h[i]->used;
    }

    size = holding_file_size(sched(dp)->destname, 0);
    h[activehd]->used = size - dummy;
    h[activehd]->disk->allocated_dumpers--;
    adjust_diskspace(dp, DONE);

    if (dumper->result != RETRY) {
	sched(dp)->dump_attempted += 1;
    }

    if((dumper->result != DONE || chunker->result != DONE) &&
       sched(dp)->dump_attempted < dp->retry_dump) {
	delete_diskspace(dp);
	if (sched(dp)->no_space) {
	    enqueue_disk(&directq, dp);
	} else {
	    enqueue_disk(&runq, dp);
	}
    }
    else if(size > (off_t)DISK_BLOCK_KB) {
	sched(dp)->nb_flush = 0;
	for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	    cmddata_t *cmddata;

	    /* If the dle/level must go to the storage */
	    if (dump_match_selection(taper->storage_name, dp)) {
		char *qname = quote_string(dp->name);
		sched(dp)->nb_flush++;
		enqueue_disk(&taper->tapeq, dp);

		cmddata = g_new0(cmddata_t, 1);
		cmddata->operation = CMD_FLUSH;
		cmddata->config = g_strdup(get_config_name());
		cmddata->storage = NULL;
		cmddata->pool = NULL;
		cmddata->label = NULL;
		cmddata->holding_file = g_strdup(sched(dp)->destname);
		cmddata->hostname = g_strdup(dp->hostname);
		cmddata->diskname = g_strdup(dp->name);
		cmddata->dump_timestamp = g_strdup(driver_timestamp);
		cmddata->storage_dest = g_strdup(taper->storage_name);
		cmddata->working_pid = getppid();
		cmddata->status = CMD_TODO;
		add_cmd_in_cmdfile(cmddatas, cmddata);
		if (!sched(dp)->to_storage) {
		    sched(dp)->to_storage = g_hash_table_new(g_str_hash,
							     g_str_equal);
		}
		g_hash_table_insert(sched(dp)->to_storage, taper->storage_name,
				GINT_TO_POINTER(cmddata->id));
		g_printf("driver: to write host %s disk %s date %s on storage %s\n",
			 dp->host->hostname, qname, driver_timestamp, taper->storage_name);
		amfree(qname);
	    }
	}
    }
    else {
	delete_diskspace(dp);
    }

    dumper->busy = 0;
    dp->host->inprogress -= 1;
    dp->inprogress = 0;

    waitpid(chunker->pid, NULL, 0 );
    aclose(chunker->fd);
    chunker->fd = -1;
    chunker->down = 1;

    free_serial_job(job);
    free_job(job);
    dumper->job = NULL;
    chunker->job = NULL;

    dp = NULL;
    if (chunker->result == ABORT_FINISHED)
	pending_aborts--;
    continue_port_dumps();
    /*
     * Wakeup any dumpers that are sleeping because of network
     * or disk constraints.
     */
    start_some_dumps(&runq);
    startaflush();
}

static gboolean
dump_match_selection(
    char   *storage_n,
    disk_t *dp)
{
    storage_t *st = lookup_storage(storage_n);
    dump_selection_list_t dsl;

    dsl = storage_get_dump_selection(st);
    if (!dsl)
	return TRUE;

    for (; dsl != NULL ; dsl = dsl->next) {
	dump_selection_t *ds = dsl->data;
	gboolean ok = FALSE;

	if (ds->tag_type == TAG_ALL) {
	    ok = TRUE;
	} else if (ds->tag_type == TAG_NAME) {
	    identlist_t tags;
	    for (tags = dp->tags; tags != NULL ; tags = tags->next) {
		if (g_str_equal(ds->tag, tags->data)) {
		    ok = TRUE;
		    break;
		}
	    }
	} else if (ds->tag_type == TAG_OTHER) {
	    // WHAT DO TO HERE
	}

	if (ok) {
	    if (ds->level == LEVEL_ALL) {
		return TRUE;
	    } else if (ds->level == LEVEL_FULL && sched(dp)->level == 0) {
		return TRUE;
	    } else if (ds->level == LEVEL_INCR && sched(dp)->level > 0) {
		return TRUE;
	    }
	}
    }

    return FALSE;
}

static void
handle_dumper_result(
	void * cookie)
{
    /* uses global pending_aborts */
    dumper_t *dumper = cookie;
    job_t    *job;
    wtaper_t *wtaper;
    taper_t  *taper;
    GList  *dlist1;
    disk_t *dp, *sdp, *dp1;
    cmd_t cmd;
    int result_argc;
    char *qname;
    char **result_argv;

    assert(dumper != NULL);
    job = dumper->job;
    dp = job->disk;
    assert(dp != NULL);
    assert(sched(dp) != NULL);
    do {

	short_dump_state();

	cmd = getresult(dumper->fd, 1, &result_argc, &result_argv);

	if(cmd != BOGUS) {
	    /* result_argv[1] always contains the serial number */
	    job = serial2job(result_argv[1]);
	    assert(job == dumper->job);
	    sdp = job->disk;
	    if (sdp != dp) {
		error(_("Invalid serial number %s"), result_argv[1]);
                g_assert_not_reached();
	    }
	}

	qname = quote_string(dp->name);
	switch(cmd) {

	case DONE: /* DONE <handle> <origsize> <dumpsize> <dumptime> <native-crc> <client-crc> <errstr> */
	    if(result_argc != 8) {
		error(_("error [dumper DONE result_argc != 8: %d]"), result_argc);
		/*NOTREACHED*/
	    }

	    sched(dp)->origsize = OFF_T_ATOI(result_argv[2]);
	    sched(dp)->dumptime = TIME_T_ATOI(result_argv[4]);
	    parse_crc(result_argv[5], &sched(dp)->native_crc);
	    parse_crc(result_argv[6], &sched(dp)->client_crc);

	    g_printf(_("driver: finished-cmd time %s %s dumped %s:%s\n"),
		   walltime_str(curclock()), dumper->name,
		   dp->host->hostname, qname);
	    fflush(stdout);

	    dumper->result = cmd;

	    break;

	case TRYAGAIN: /* TRY-AGAIN <handle> <errstr> */
	    /*
	     * Requeue this disk, and fall through to the FAILED
	     * case for cleanup.
	     */
	    if(sched(dp)->dump_attempted >= dp->retry_dump-1) {
		char *qname = quote_string(dp->name);
		char *qerr = quote_string(result_argv[2]);
		log_add(L_FAIL, _("%s %s %s %d [too many dumper retry: %s]"),
		    dp->host->hostname, qname, sched(dp)->datestamp,
		    sched(dp)->level, qerr);
		g_printf(_("driver: dump failed %s %s %s, too many dumper retry: %s\n"),
		        result_argv[1], dp->host->hostname, qname, qerr);
		amfree(qname);
		amfree(qerr);
	    }
	    /* FALLTHROUGH */
	case FAILED: /* FAILED <handle> <errstr> */
	    /*free_serial(result_argv[1]);*/
	    dumper->result = cmd;
	    break;

	case ABORT_FINISHED: /* ABORT-FINISHED <handle> */
	    /*
	     * We sent an ABORT from the NO-ROOM case because this dump
	     * wasn't going to fit onto the holding disk.  We now need to
	     * clean up the remains of this image, and try to finish
	     * other dumps that are waiting on disk space.
	     */
	    assert(pending_aborts);
	    /*free_serial(result_argv[1]);*/
	    dumper->result = cmd;
	    break;

	case RETRY: /* RETRY <handle> <delay> <level> <errstr> */
	    {
		const time_t now = time(NULL);
		int delay = atoi(result_argv[2]);
		int level = atoi(result_argv[3]);

		dumper->result = cmd;
		if (delay >= 0) {
		    dp->start_t = now + delay;
		} else {
		    dp->start_t = now + 300;
		}
		if (level != -1) {
		    sched(dp)->level = level;
		}
		break;
	    }

	case BOGUS:
	    /* either EOF or garbage from dumper.  Turn it off */
	    log_add(L_WARNING, _("%s pid %ld is messed up, ignoring it.\n"),
		    dumper->name, (long)dumper->pid);
	    if (dumper->ev_read) {
		event_release(dumper->ev_read);
		dumper->ev_read = NULL;
	    }
	    aclose(dumper->fd);
	    dumper->busy = 0;
	    dumper->down = 1;	/* mark it down so it isn't used again */

            /* if it was dumping something, zap it and try again */
            if(sched(dp)->dump_attempted >= dp->retry_dump-1) {
		log_add(L_FAIL, _("%s %s %s %d [%s died]"),
			dp->host->hostname, qname, sched(dp)->datestamp,
			sched(dp)->level, dumper->name);
            } else {
		log_add(L_WARNING, _("%s died while dumping %s:%s lev %d."),
			dumper->name, dp->host->hostname, qname,
			sched(dp)->level);
            }
	    dumper->result = cmd;
	    break;

	default:
	    assert(0);
	}
        amfree(qname);
	g_strfreev(result_argv);

	if (cmd != BOGUS) {
	    int last_dump = 1;
	    dumper_t *dumper;

	    run_server_dle_scripts(EXECUTE_ON_POST_DLE_BACKUP,
			       get_config_name(), dp, sched(dp)->level);
	    /* check dump not yet started */
	    for(dlist1 = runq.head; dlist1 != NULL; dlist1 = dlist1->next) {
		dp1 = dlist1->data;
		if (dp1->host == dp->host)
		    last_dump = 0;
	    }
	    /* check direct to tape dump */
	    for(dlist1 = directq.head; dlist1 != NULL; dlist1 = dlist1->next) {
		dp1 = dlist1->data;
		if (dp1->host == dp->host)
		    last_dump = 0;
	    }
	    /* check dumping dle */
	    for (dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
		if (dumper->busy && dumper->job->disk != dp &&
		    dumper->job->disk->host == dp->host)
		 last_dump = 0;
	    }
	    if (last_dump && dp->host->post_script == 0) {
		if (dp->host->post_script == 0) {
		    run_server_host_scripts(EXECUTE_ON_POST_HOST_BACKUP,
					    get_config_name(), dp->host);
		    dp->host->post_script = 1;
		}
	    }
	}

	/* send the dumper result to the chunker */
	if (job->chunker) {
	    if (job->chunker->sendresult) {
		if (cmd == DONE) {
		    chunker_cmd(job->chunker, DONE, dp, NULL);
		} else {
		    chunker_cmd(job->chunker, FAILED, dp, NULL);
		}
		job->chunker->sendresult = 0;
	    }
	    if (dumper->result != LAST_TOK &&
		job->chunker->result != LAST_TOK)
		dumper_chunker_result(job);
	} else if (job->wtaper->ready) { /* send the dumper result to the taper */
	    wtaper = job->wtaper;
	    taper = wtaper->taper;
	    if (cmd == DONE) {
		taper_cmd(taper, wtaper, DONE, dp, NULL, 0, NULL);
	    } else {
		taper_cmd(taper, wtaper, FAILED, dp, NULL, 0, NULL);
	    }
	    wtaper->sendresult = 0;
	    if (job->dumper && wtaper->result != LAST_TOK) {
		dumper_taper_result(job);
	    }
	}
    } while(areads_dataready(dumper->fd));
}


static void
handle_chunker_result(
    void *	cookie)
{
    chunker_t *chunker = cookie;
    assignedhd_t **h=NULL;
    job_t    *job;
    disk_t *dp, *sdp;
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    int dummy;
    int activehd = -1;
    char *qname;

    assert(chunker != NULL);
    job = chunker->job;
    assert(job->dumper != NULL);
    dp = job->disk;
    assert(dp != NULL);
    assert(sched(dp) != NULL);
    assert(sched(dp)->destname != NULL);
    assert(dp != NULL && sched(dp) != NULL && sched(dp)->destname);

    if(sched(dp)->holdp) {
	h = sched(dp)->holdp;
	activehd = sched(dp)->activehd;
    }

    do {
	short_dump_state();

	cmd = getresult(chunker->fd, 1, &result_argc, &result_argv);

	if(cmd != BOGUS) {
	    /* result_argv[1] always contains the serial number */
	    job = serial2job(result_argv[1]);
	    assert(job == chunker->job);
	    sdp = job->disk;
	    if (sdp != dp) {
		error(_("Invalid serial number %s"), result_argv[1]);
                g_assert_not_reached();
	    }
	}

	switch(cmd) {

	case PARTIAL: /* PARTIAL <handle> <dumpsize> <server-crc> <errstr> */
	case DONE: /* DONE <handle> <dumpsize> <server-crc> <errstr> */
	    if(result_argc != 5) {
		error(_("error [chunker %s result_argc != 5: %d]"), cmdstr[cmd],
		      result_argc);
	        /*NOTREACHED*/
	    }
	    /*free_serial(result_argv[1]);*/

	    sched(dp)->dumpsize = (off_t)atof(result_argv[2]);
	    parse_crc(result_argv[3], &sched(dp)->server_crc);

	    qname = quote_string(dp->name);
	    g_printf(_("driver: finished-cmd time %s %s chunked %s:%s\n"),
		   walltime_str(curclock()), chunker->name,
		   dp->host->hostname, qname);
	    fflush(stdout);
            amfree(qname);

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    chunker_cmd(chunker, QUIT, NULL, NULL);
	    break;

	case TRYAGAIN: /* TRY-AGAIN <handle> <errstr> */
	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    chunker_cmd(chunker, QUIT, NULL, NULL);
	    break;

	case FAILED: /* FAILED <handle> <errstr> */
	    /*free_serial(result_argv[1]);*/

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    chunker_cmd(chunker, QUIT, NULL, NULL);
	    break;

	case DUMPER_STATUS: /* NO-ROOM <handle> */
	    if (job->dumper->result == LAST_TOK) {
		chunker->sendresult = 1;
	    } else {
		if (job->dumper->result == DONE) {
		    chunker_cmd(chunker, DONE, dp, NULL);
		} else {
		    chunker_cmd(chunker, FAILED, dp, NULL);
		}
	    }
	    break;

	case NO_ROOM: /* NO-ROOM <handle> <missing_size> */
	    if (!h || activehd < 0) { /* should never happen */
		error(_("!h || activehd < 0"));
		/*NOTREACHED*/
	    }
	    h[activehd]->used -= OFF_T_ATOI(result_argv[2]);
	    h[activehd]->reserved -= OFF_T_ATOI(result_argv[2]);
	    h[activehd]->disk->allocated_space -= OFF_T_ATOI(result_argv[2]);
	    h[activehd]->disk->disksize -= OFF_T_ATOI(result_argv[2]);
	    break;

	case RQ_MORE_DISK: /* RQ-MORE-DISK <handle> */
	    if (!h || activehd < 0) { /* should never happen */
		error(_("!h || activehd < 0"));
		/*NOTREACHED*/
	    }
	    h[activehd]->disk->allocated_dumpers--;
	    h[activehd]->used = h[activehd]->reserved;
	    if( h[++activehd] ) { /* There's still some allocated space left.
				   * Tell the dumper about it. */
		sched(dp)->activehd++;
		chunker_cmd( chunker, CONTINUE, dp, NULL );
	    } else { /* !h[++activehd] - must allocate more space */
		sched(dp)->act_size = sched(dp)->est_size; /* not quite true */
		sched(dp)->est_size = (sched(dp)->act_size/(off_t)20) * (off_t)21; /* +5% */
		sched(dp)->est_size = am_round(sched(dp)->est_size, (off_t)DISK_BLOCK_KB);
		if (sched(dp)->est_size < sched(dp)->act_size + 2*DISK_BLOCK_KB)
		    sched(dp)->est_size += 2 * DISK_BLOCK_KB;
		h = find_diskspace( sched(dp)->est_size - sched(dp)->act_size,
				    &dummy,
				    h[activehd-1] );
		if( !h ) {
		    /* No diskspace available. The reason for this will be
		     * determined in continue_port_dumps(). */
		    enqueue_disk( &roomq, dp );
		    continue_port_dumps();
		    /* continue flush waiting for new tape */
		    startaflush();
		} else {
		    /* OK, allocate space for disk and have chunker continue */
		    sched(dp)->activehd = assign_holdingdisk( h, dp );
		    chunker_cmd( chunker, CONTINUE, dp, NULL );
		    amfree(h);
		}
	    }
	    break;

	case ABORT_FINISHED: /* ABORT-FINISHED <handle> */
	    /*
	     * We sent an ABORT from the NO-ROOM case because this dump
	     * wasn't going to fit onto the holding disk.  We now need to
	     * clean up the remains of this image, and try to finish
	     * other dumps that are waiting on disk space.
	     */
	    /*assert(pending_aborts);*/

	    /*free_serial(result_argv[1]);*/

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    break;

	case BOGUS:
	    /* either EOF or garbage from chunker.  Turn it off */
	    log_add(L_WARNING, _("%s pid %ld is messed up, ignoring it.\n"),
		    chunker->name, (long)chunker->pid);

            /* if it was dumping something, zap it and try again */
            g_assert(h && activehd >= 0);
            qname = quote_string(dp->name);
            if(sched(dp)->dump_attempted >= dp->retry_dump-1) {
                log_add(L_FAIL, _("%s %s %s %d [%s died]"),
                        dp->host->hostname, qname, sched(dp)->datestamp,
                        sched(dp)->level, chunker->name);
            } else {
                log_add(L_WARNING, _("%s died while dumping %s:%s lev %d."),
                        chunker->name, dp->host->hostname, qname,
                        sched(dp)->level);
            }
            amfree(qname);

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    g_strfreev(result_argv);

	    if (chunker->result != LAST_TOK &&
		job->dumper->result != LAST_TOK) {
		dumper_chunker_result(job);
	    }

	    return;

	    break;

	default:
	    assert(0);
	}
	g_strfreev(result_argv);

	if(chunker->result != LAST_TOK && job->dumper->result != LAST_TOK)
	    dumper_chunker_result(job);

    } while(areads_dataready(chunker->fd));
}


static void
read_flush(
    void *	cookie)
{
    sched_t *sp;
    disk_t *dp;
    int line;
    char *hostname, *diskname, *datestamp;
    int level;
    char *destname;
    disk_t *dp1;
    char *inpline = NULL;
    char *command;
    char *s;
    int ch;
    char *qname = NULL;
    char *qdestname = NULL;
    char *conf_infofile;
    char *conf_cmdfile;
    char *ids;
    assignedhd_t **holdp;
    taper_t       *taper;
    cmddata_t     *cmddata;
    char        **ids_array;
    char        **one_id;

    (void)cookie;	/* Quiet unused parameter warning */

    event_release(flush_ev_read);
    flush_ev_read = NULL;

    conf_cmdfile = config_dir_relative(getconf_str(CNF_CMDFILE));
    cmddatas = read_cmdfile(conf_cmdfile);
    unlock_cmdfile(cmddatas);

    /* read schedule from stdin */
    conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
    if (open_infofile(conf_infofile)) {
	error(_("could not open info db \"%s\""), conf_infofile);
	/*NOTREACHED*/
    }
    amfree(conf_infofile);

    for(line = 0; (inpline = agets(stdin)) != NULL; free(inpline)) {
	dumpfile_t file;
	int id;

	line++;
	if (inpline[0] == '\0')
	    continue;

	s = inpline;
	ch = *s++;

	skip_whitespace(s, ch);                 /* find the command */
	if(ch == '\0') {
	    error(_("flush line %d: syntax error (no command)"), line);
	    /*NOTREACHED*/
	}
	command = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	if(g_str_equal(command, "ENDFLUSH")) {
	    break;
	}

	if(!g_str_equal(command, "FLUSH")) {
	    error(_("flush line %d: syntax error (%s != FLUSH)"), line, command);
	    /*NOTREACHED*/
	}

	skip_whitespace(s, ch);			/* find the level number */
	if(ch == '\0') {
	    error(_("flush line %d: syntax error (bad ids)"), line);
	    /*NOTREACHED*/
	}
	ids = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the hostname */
	if(ch == '\0') {
	    error(_("flush line %d: syntax error (no hostname)"), line);
	    /*NOTREACHED*/
	}
	hostname = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the diskname */
	if(ch == '\0') {
	    error(_("flush line %d: syntax error (no diskname)"), line);
	    /*NOTREACHED*/
	}
	qname = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	diskname = unquote_string(qname);

	skip_whitespace(s, ch);			/* find the datestamp */
	if(ch == '\0') {
	    error(_("flush line %d: syntax error (no datestamp)"), line);
	    /*NOTREACHED*/
	}
	datestamp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the level number */
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    error(_("flush line %d: syntax error (bad level)"), line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the filename */
	if(ch == '\0') {
	    error(_("flush line %d: syntax error (no filename)"), line);
	    /*NOTREACHED*/
	}
	qdestname = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';
	destname = unquote_string(qdestname);

	if (!holding_file_get_dumpfile(destname, &file)) {
	    continue;
	}

	if( file.type != F_DUMPFILE) {
	    if( file.type != F_CONT_DUMPFILE )
		log_add(L_INFO, _("%s: ignoring cruft file."), destname);
	    amfree(diskname);
	    amfree(destname);
	    dumpfile_free_data(&file);
	    continue;
	}

	if(!g_str_equal(hostname, file.name) ||
	   !g_str_equal(diskname, file.disk) ||
	   !g_str_equal(datestamp, file.datestamp)) {
	    log_add(L_INFO, _("disk %s:%s not consistent with file %s"),
		    hostname, diskname, destname);
	    amfree(diskname);
	    amfree(destname);
	    dumpfile_free_data(&file);
	    continue;
	}
	amfree(diskname);

	dp = lookup_disk(file.name, file.disk);

	if (dp == NULL) {
	    log_add(L_INFO, _("%s: disk %s:%s not in database, skipping it."),
		    destname, file.name, file.disk);
	    amfree(destname);
	    dumpfile_free_data(&file);
	    continue;
	}

	if (file.dumplevel < 0 || file.dumplevel > 399) {
	    log_add(L_INFO, _("%s: ignoring file with bogus dump level %d."),
		    destname, file.dumplevel);
	    amfree(destname);
	    dumpfile_free_data(&file);
	    continue;
	}

	if (holding_file_size(destname,1) <= 0) {
	    log_add(L_INFO, "%s: removing file with no data.", destname);
	    holding_file_unlink(destname);
	    amfree(destname);
	    dumpfile_free_data(&file);
	    continue;
	}

	dp1 = (disk_t *)g_malloc(sizeof(disk_t));
	*dp1 = *dp;

	/* add it to the flushhost list */
	if(!flushhost) {
	    flushhost = g_malloc(sizeof(am_host_t));
	    flushhost->next = NULL;
	    flushhost->hostname = g_strdup("FLUSHHOST");
	    flushhost->up = NULL;
	    flushhost->features = NULL;
	}
	dp1->hostnext = flushhost->disks;
	flushhost->disks = dp1;

	holdp = build_diskspace(destname);
	if (holdp == NULL) continue;

	sp = g_new0(sched_t, 1);
	sp->destname = destname;
	sp->level = file.dumplevel;
	sp->dumpdate = NULL;
	sp->degr_dumpdate = NULL;
	sp->degr_mesg = NULL;
	sp->datestamp = g_strdup(file.datestamp);
	sp->est_nsize = (off_t)0;
	sp->est_csize = (off_t)0;
	sp->est_time = 0;
	sp->est_kps = 10;
	sp->origsize = file.orig_size;
	sp->priority = 0;
	sp->degr_level = -1;
	sp->dump_attempted = 0;
	sp->taper_attempted = 0;
	sp->act_size = holding_file_size(destname, 0);
	sp->holdp = holdp;
	sp->timestamp = (time_t)0;

	dp1->up = (char *)sp;

	sched(dp1)->nb_flush = 0;
	/* for all ids */
	ids_array = g_strsplit(ids, ",", 0);
	for (one_id = ids_array; *one_id != NULL; one_id++) {
	    char *storage = strchr(*one_id, ':');
	    if (storage) {
		*storage = '\0';
		storage++;
	    }
	    id = atoi(*one_id);
	    cmddata = g_hash_table_lookup(cmddatas->cmdfile, GINT_TO_POINTER(id));
	    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		if (g_str_equal(taper->storage_name, cmddata->storage_dest)) {
		    if (!sched(dp1)->to_storage) {
			sched(dp1)->to_storage = g_hash_table_new(g_str_hash,
								  g_str_equal);
		    }
		    g_hash_table_insert(sched(dp1)->to_storage,
					taper->storage_name,
					GINT_TO_POINTER(cmddata->id));
		    sched(dp1)->nb_flush++;
		    enqueue_disk(&taper->tapeq, dp1);
		}
	    }
	}
	g_strfreev(ids_array);
	dumpfile_free_data(&file);
    }
    amfree(inpline);
    close_infofile();

    /* re-read de cmdfile */
    close_cmdfile(cmddatas);
    cmddatas = read_cmdfile(conf_cmdfile);
    unlock_cmdfile(cmddatas);
    g_free(conf_cmdfile);

    startaflush();
    if (!nodump) {
	schedule_ev_read = event_register((event_id_t)0, EV_READFD,
					  read_schedule, NULL);
    } else {
	force_flush = 1;
    }
}

static void
read_schedule(
    void *	cookie)
{
    sched_t *sp;
    disk_t *dp;
    int level, line, priority;
    char *dumpdate, *degr_dumpdate, *degr_mesg = NULL;
    int degr_level;
    time_t time, degr_time;
    time_t *time_p = &time;
    time_t *degr_time_p = &degr_time;
    off_t nsize, csize, degr_nsize, degr_csize;
    unsigned long kps, degr_kps;
    char *hostname, *features, *diskname, *datestamp, *inpline = NULL;
    char *command;
    char *s;
    int ch;
    off_t flush_size = (off_t)0;
    char *qname = NULL;
    long long time_;
    long long nsize_;
    long long csize_;
    long long degr_nsize_;
    long long degr_csize_;
    gchar **errors;

    (void)cookie;	/* Quiet unused parameter warning */

    event_release(schedule_ev_read);
    schedule_ev_read = NULL;

    /* read schedule from stdin */

    for(line = 0; (inpline = agets(stdin)) != NULL; free(inpline)) {
	if (inpline[0] == '\0')
	    continue;
	line++;

	s = inpline;
	ch = *s++;

	skip_whitespace(s, ch);			/* find the command */
	if(ch == '\0') {
	    error(_("schedule line %d: syntax error (no command)"), line);
	    /*NOTREACHED*/
	}
	command = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	if(!g_str_equal(command, "DUMP")) {
	    error(_("schedule line %d: syntax error (%s != DUMP)"), line, command);
	    /*NOTREACHED*/
	}

	skip_whitespace(s, ch);			/* find the host name */
	if(ch == '\0') {
	    error(_("schedule line %d: syntax error (no host name)"), line);
	    /*NOTREACHED*/
	}
	hostname = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the feature list */
	if(ch == '\0') {
	    error(_("schedule line %d: syntax error (no feature list)"), line);
	    /*NOTREACHED*/
	}
	features = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the disk name */
	if(ch == '\0') {
	    error(_("schedule line %d: syntax error (no disk name)"), line);
	    /*NOTREACHED*/
	}
	qname = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	diskname = unquote_string(qname);

	skip_whitespace(s, ch);			/* find the datestamp */
	if(ch == '\0') {
	    error(_("schedule line %d: syntax error (no datestamp)"), line);
	    /*NOTREACHED*/
	}
	datestamp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the priority number */
	if(ch == '\0' || sscanf(s - 1, "%d", &priority) != 1) {
	    error(_("schedule line %d: syntax error (bad priority)"), line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the level number */
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    error(_("schedule line %d: syntax error (bad level)"), line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the dump date */
	if(ch == '\0') {
	    error(_("schedule line %d: syntax error (bad dump date)"), line);
	    /*NOTREACHED*/
	}
	dumpdate = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the native size */
	nsize_ = (off_t)0;
	if(ch == '\0' || sscanf(s - 1, "%lld", &nsize_) != 1) {
	    error(_("schedule line %d: syntax error (bad nsize)"), line);
	    /*NOTREACHED*/
	}
	nsize = (off_t)nsize_;
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the compressed size */
	csize_ = (off_t)0;
	if(ch == '\0' || sscanf(s - 1, "%lld", &csize_) != 1) {
	    error(_("schedule line %d: syntax error (bad csize)"), line);
	    /*NOTREACHED*/
	}
	csize = (off_t)csize_;
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the time number */
	if(ch == '\0' || sscanf(s - 1, "%lld", &time_) != 1) {
	    error(_("schedule line %d: syntax error (bad estimated time)"), line);
	    /*NOTREACHED*/
	}
	*time_p = (time_t)time_;
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the kps number */
	if(ch == '\0' || sscanf(s - 1, "%lu", &kps) != 1) {
	    error(_("schedule line %d: syntax error (bad kps)"), line);
	    continue;
	}
	skip_integer(s, ch);

	degr_dumpdate = NULL;			/* flag if degr fields found */
	skip_whitespace(s, ch);			/* find the degr level number */
	degr_mesg = NULL;
	if (ch == '"') {
	    qname = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';			/* terminate degr mesg */
	    degr_mesg = unquote_string(qname);
	    degr_level = -1;
	    degr_nsize = (off_t)0;
	    degr_csize = (off_t)0;
	    degr_time = (time_t)0;
	    degr_kps = 0;
	} else if (ch != '\0') {
	    if(sscanf(s - 1, "%d", &degr_level) != 1) {
		error(_("schedule line %d: syntax error (bad degr level)"), line);
		/*NOTREACHED*/
	    }
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr dump date */
	    if(ch == '\0') {
		error(_("schedule line %d: syntax error (bad degr dump date)"), line);
		/*NOTREACHED*/
	    }
	    degr_dumpdate = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';

	    skip_whitespace(s, ch);		/* find the degr native size */
	    degr_nsize_ = (off_t)0;
	    if(ch == '\0'  || sscanf(s - 1, "%lld", &degr_nsize_) != 1) {
		error(_("schedule line %d: syntax error (bad degr nsize)"), line);
		/*NOTREACHED*/
	    }
	    degr_nsize = (off_t)degr_nsize_;
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr compressed size */
	    degr_csize_ = (off_t)0;
	    if(ch == '\0'  || sscanf(s - 1, "%lld", &degr_csize_) != 1) {
		error(_("schedule line %d: syntax error (bad degr csize)"), line);
		/*NOTREACHED*/
	    }
	    degr_csize = (off_t)degr_csize_;
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr time number */
	    if(ch == '\0' || sscanf(s - 1, "%lld", &time_) != 1) {
		error(_("schedule line %d: syntax error (bad degr estimated time)"), line);
		/*NOTREACHED*/
	    }
	    *degr_time_p = (time_t)time_;
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr kps number */
	    if(ch == '\0' || sscanf(s - 1, "%lu", &degr_kps) != 1) {
		error(_("schedule line %d: syntax error (bad degr kps)"), line);
		/*NOTREACHED*/
	    }
	    skip_integer(s, ch);
	} else {
	    error(_("schedule line %d: no degraded estimate or message"), line);
	}

	dp = lookup_disk(hostname, diskname);
	if(dp == NULL) {
	    log_add(L_WARNING,
		    _("schedule line %d: %s:'%s' not in disklist, ignored"),
		    line, hostname, qname);
	    amfree(diskname);
	    amfree(degr_mesg);
	    continue;
	}

	sp = g_new0(sched_t, 1);
	/*@ignore@*/
	sp->level = level;
	sp->dumpdate = g_strdup(dumpdate);
	sp->est_nsize = DISK_BLOCK_KB + nsize; /* include header */
	sp->est_csize = DISK_BLOCK_KB + csize; /* include header */
	/* round estimate to next multiple of DISK_BLOCK_KB */
	sp->est_csize = am_round(sp->est_csize, DISK_BLOCK_KB);
	sp->est_size = sp->est_csize;
	sp->est_time = time;
	sp->est_kps = kps;
	sp->priority = priority;
	sp->datestamp = g_strdup(datestamp);

	if(degr_dumpdate) {
	    sp->degr_level = degr_level;
	    sp->degr_dumpdate = g_strdup(degr_dumpdate);
	    sp->degr_nsize = DISK_BLOCK_KB + degr_nsize;
	    sp->degr_csize = DISK_BLOCK_KB + degr_csize;
	    /* round estimate to next multiple of DISK_BLOCK_KB */
	    sp->degr_csize = am_round(sp->degr_csize, DISK_BLOCK_KB);
	    sp->degr_time = degr_time;
	    sp->degr_kps = degr_kps;
	    sp->degr_mesg = NULL;
	    amfree(degr_mesg);
	} else {
	    sp->degr_level = -1;
	    sp->degr_dumpdate = NULL;
	    sp->degr_mesg = degr_mesg;
	    degr_mesg = NULL;
	}
	/*@end@*/

	sp->dump_attempted = 0;
	sp->taper_attempted = 0;
	sp->act_size = 0;
	sp->holdp = NULL;
	sp->activehd = -1;
	sp->timestamp = (time_t)0;
	sp->destname = NULL;
	sp->no_space = 0;

	dp->up = (char *) sp;
	if(dp->host->features == NULL) {
	    dp->host->features = am_string_to_feature(features);
	    if (!dp->host->features) {
		log_add(L_WARNING,
		    _("Invalid feature string from client '%s'"),
		    features);
		dp->host->features = am_set_default_feature_set();
	    }
	}
	remove_disk(&waitq, dp);

	errors = validate_optionstr(dp);

        if (errors) {
            gchar **ptr;
            for (ptr = errors; *ptr; ptr++)
                log_add(L_FAIL, "%s %s %s 0 [%s]", dp->host->hostname, qname,
                    sp->datestamp, *ptr);
            g_strfreev(errors);
	    amfree(qname);
	} else {

	    if (dp->data_path == DATA_PATH_DIRECTTCP &&
		dp->to_holdingdisk == HOLD_AUTO) {
		dp->to_holdingdisk = HOLD_NEVER;
	    }

	    if (dp->to_holdingdisk == HOLD_NEVER) {
		enqueue_disk(&directq, dp);
	    } else {
		enqueue_disk(&runq, dp);
	    }
	    flush_size += sp->act_size;
	}
	amfree(diskname);
    }
    g_printf(_("driver: flush size %lld\n"), (long long)flush_size);
    amfree(inpline);
    if(line == 0)
	log_add(L_WARNING, _("WARNING: got empty schedule from planner"));
    schedule_done = 1;
    start_degraded_mode(&runq);
    run_server_global_scripts(EXECUTE_ON_PRE_BACKUP, get_config_name());
    if (empty(runq)) force_flush = 1;
    start_some_dumps(&runq);
    startaflush();
}

static unsigned long
free_kps(
    netif_t *ip)
{
    unsigned long res = 0;

    if (ip == NULL) {
	netif_t *p;
	unsigned long maxusage=0;
	unsigned long curusage=0;
	for(p = disklist_netifs(); p != NULL; p = p->next) {
	    maxusage += interface_get_maxusage(p->config);
	    curusage += p->curusage;
	}
	if (maxusage >= curusage)
	    res = maxusage - curusage;
#ifndef __lint
    } else {
	if ((unsigned long)interface_get_maxusage(ip->config) >= ip->curusage)
	    res = interface_get_maxusage(ip->config) - ip->curusage;
#endif
    }

    return res;
}

static void
interface_state(
    char *time_str)
{
    netif_t *ip;

    g_printf(_("driver: interface-state time %s"), time_str);

    for(ip = disklist_netifs(); ip != NULL; ip = ip->next) {
	g_printf(_(" if %s: free %lu"), interface_name(ip->config), free_kps(ip));
    }
    g_printf("\n");
}

static void
allocate_bandwidth(
    netif_t *		ip,
    unsigned long	kps)
{
    ip->curusage += kps;
}

static void
deallocate_bandwidth(
    netif_t *		ip,
    unsigned long	kps)
{
    assert(kps <= ip->curusage);
    ip->curusage -= kps;
}

/* ------------ */
static off_t
free_space(void)
{
    holdalloc_t *ha;
    off_t total_free;
    off_t diff;

    total_free = (off_t)0;
    for(ha = holdalloc; ha != NULL; ha = ha->next) {
	diff = ha->disksize - ha->allocated_space;
	if(diff > (off_t)0)
	    total_free += diff;
    }
    return total_free;
}

/*
 * We return an array of pointers to assignedhd_t. The array contains at
 * most one entry per holding disk. The list of pointers is terminated by
 * a NULL pointer. Each entry contains a pointer to a holdingdisk and
 * how much diskspace to use on that disk. Later on, assign_holdingdisk
 * will allocate the given amount of space.
 * If there is not enough room on the holdingdisks, NULL is returned.
 */

static assignedhd_t **
find_diskspace(
    off_t		size,
    int *		cur_idle,
    assignedhd_t *	pref)
{
    assignedhd_t **result = NULL;
    holdalloc_t *ha, *minp;
    int i=0;
    int j, minj;
    char *used;
    off_t halloc, dalloc, hfree, dfree;

    (void)cur_idle;	/* Quiet unused parameter warning */

    if (holdalloc == NULL) {
	/* no holding disk in use */
	return NULL;
    }

    if (size < 2*DISK_BLOCK_KB)
	size = 2*DISK_BLOCK_KB;
    size = am_round(size, (off_t)DISK_BLOCK_KB);

    hold_debug(1, _("find_diskspace: want %lld K\n"),
		   (long long)size);

    used = g_malloc(sizeof(*used) * num_holdalloc);/*disks used during this run*/
    memset( used, 0, (size_t)num_holdalloc );
    result = g_malloc(sizeof(assignedhd_t *) * (num_holdalloc + 1));
    result[0] = NULL;

    while( i < num_holdalloc && size > (off_t)0 ) {
	/* find the holdingdisk with the fewest active dumpers and among
	 * those the one with the biggest free space
	 */
	minp = NULL; minj = -1;
	for(j = 0, ha = holdalloc; ha != NULL; ha = ha->next, j++ ) {
	    if( pref && pref->disk == ha && !used[j] &&
		ha->allocated_space <= ha->disksize - (off_t)DISK_BLOCK_KB) {
		minp = ha;
		minj = j;
		break;
	    }
	    else if( ha->allocated_space <= ha->disksize - (off_t)(2*DISK_BLOCK_KB) &&
		!used[j] &&
		(!minp ||
		 ha->allocated_dumpers < minp->allocated_dumpers ||
		 (ha->allocated_dumpers == minp->allocated_dumpers &&
		  ha->disksize-ha->allocated_space > minp->disksize-minp->allocated_space)) ) {
		minp = ha;
		minj = j;
	    }
	}

	pref = NULL;
	if( !minp ) { break; } /* all holding disks are full */
	used[minj] = 1;

	/* hfree = free space on the disk */
	hfree = minp->disksize - minp->allocated_space;

	/* dfree = free space for data, remove 1 header for each chunksize */
	dfree = hfree - (((hfree-(off_t)1)/holdingdisk_get_chunksize(minp->hdisk))+(off_t)1) * (off_t)DISK_BLOCK_KB;

	/* dalloc = space I can allocate for data */
	dalloc = ( dfree < size ) ? dfree : size;

	/* halloc = space to allocate, including 1 header for each chunksize */
	halloc = dalloc + (((dalloc-(off_t)1)/holdingdisk_get_chunksize(minp->hdisk))+(off_t)1) * (off_t)DISK_BLOCK_KB;

	hold_debug(1, _("find_diskspace: find diskspace: size %lld hf %lld df %lld da %lld ha %lld\n"),
		       (long long)size,
		       (long long)hfree,
		       (long long)dfree,
		       (long long)dalloc,
		       (long long)halloc);
	size -= dalloc;
	result[i] = g_malloc(sizeof(assignedhd_t));
	result[i]->disk = minp;
	result[i]->reserved = halloc;
	result[i]->used = (off_t)0;
	result[i]->destname = NULL;
	result[i+1] = NULL;
	i++;
    }
    amfree(used);

    if(size != (off_t)0) { /* not enough space available */
	g_printf(_("find diskspace: not enough diskspace. Left with %lld K\n"), (long long)size);
	fflush(stdout);
	free_assignedhd(result);
	result = NULL;
    }

    if (debug_holding > 1) {
	for( i = 0; result && result[i]; i++ ) {
	    hold_debug(1, _("find_diskspace: find diskspace: selected %s free %lld reserved %lld dumpers %d\n"),
			   holdingdisk_get_diskdir(result[i]->disk->hdisk),
			   (long long)(result[i]->disk->disksize -
			     result[i]->disk->allocated_space),
			   (long long)result[i]->reserved,
			   result[i]->disk->allocated_dumpers);
	}
    }

    return result;
}

static int
assign_holdingdisk(
    assignedhd_t **	holdp,
    disk_t *		diskp)
{
    int i, j, c, l=0;
    off_t size;
    char *sfn = sanitise_filename(diskp->name);
    char lvl[64];
    assignedhd_t **new_holdp;
    char *qname;

    g_snprintf( lvl, sizeof(lvl), "%d", sched(diskp)->level );

    size = am_round(sched(diskp)->est_size - sched(diskp)->act_size,
		    (off_t)DISK_BLOCK_KB);

    for( c = 0; holdp[c]; c++ )
	(void)c; /* count number of disks */

    /* allocate memory for sched(diskp)->holdp */
    for(j = 0; sched(diskp)->holdp && sched(diskp)->holdp[j]; j++)
	(void)j;	/* Quiet lint */
    new_holdp = (assignedhd_t **)g_malloc(sizeof(assignedhd_t*)*(j+c+1));
    if (sched(diskp)->holdp) {
	memcpy(new_holdp, sched(diskp)->holdp, j * sizeof(*new_holdp));
	amfree(sched(diskp)->holdp);
    }
    sched(diskp)->holdp = new_holdp;
    new_holdp = NULL;

    i = 0;
    if( j > 0 ) { /* This is a request for additional diskspace. See if we can
		   * merge assignedhd_t's */
	l=j;
	if( sched(diskp)->holdp[j-1]->disk == holdp[0]->disk ) { /* Yes! */
	    sched(diskp)->holdp[j-1]->reserved += holdp[0]->reserved;
	    holdp[0]->disk->allocated_space += holdp[0]->reserved;
	    size = (holdp[0]->reserved>size) ? (off_t)0 : size-holdp[0]->reserved;
	    qname = quote_string(diskp->name);
	    hold_debug(1, _("assign_holdingdisk: merging holding disk %s to disk %s:%s, add %lld for reserved %lld, left %lld\n"),
			   holdingdisk_get_diskdir(
					       sched(diskp)->holdp[j-1]->disk->hdisk),
			   diskp->host->hostname, qname,
			   (long long)holdp[0]->reserved,
			   (long long)sched(diskp)->holdp[j-1]->reserved,
			   (long long)size);
	    i++;
	    amfree(qname);
	    amfree(holdp[0]);
	    l=j-1;
	}
    }

    /* copy assignedhd_s to sched(diskp), adjust allocated_space */
    for( ; holdp[i]; i++ ) {
        g_free(holdp[i]->destname);
	holdp[i]->destname = g_strconcat(holdingdisk_get_diskdir(holdp[i]->disk->hdisk),
	    "/", hd_driver_timestamp, "/", diskp->host->hostname, ".", sfn, ".",
	    lvl, NULL);

	sched(diskp)->holdp[j++] = holdp[i];
	holdp[i]->disk->allocated_space += holdp[i]->reserved;
	size = (holdp[i]->reserved > size) ? (off_t)0 :
		  (size - holdp[i]->reserved);
	qname = quote_string(diskp->name);
	hold_debug(1,
		   _("assign_holdingdisk: %d assigning holding disk %s to disk %s:%s, reserved %lld, left %lld\n"),
		    i, holdingdisk_get_diskdir(holdp[i]->disk->hdisk),
		    diskp->host->hostname, qname,
		    (long long)holdp[i]->reserved,
		    (long long)size);
	amfree(qname);
	holdp[i] = NULL; /* so it doesn't get free()d... */
    }
    sched(diskp)->holdp[j] = NULL;
    amfree(sfn);

    return l;
}

static void
adjust_diskspace(
    disk_t *	diskp,
    cmd_t	cmd)
{
    assignedhd_t **holdp;
    off_t total = (off_t)0;
    off_t diff;
    int i;
    char *qname, *hqname, *qdest;

    (void)cmd;	/* Quiet unused parameter warning */

    qname = quote_string(diskp->name);
    qdest = quote_string(sched(diskp)->destname);
    hold_debug(1, _("adjust_diskspace: %s:%s %s\n"),
		   diskp->host->hostname, qname, qdest);

    holdp = sched(diskp)->holdp;

    assert(holdp != NULL);

    for( i = 0; holdp[i]; i++ ) { /* for each allocated disk */
	diff = holdp[i]->used - holdp[i]->reserved;
	total += holdp[i]->used;
	holdp[i]->disk->allocated_space += diff;
	hqname = quote_string(holdingdisk_name(holdp[i]->disk->hdisk));
	hold_debug(1, _("adjust_diskspace: hdisk %s done, reserved %lld used %lld diff %lld g_malloc %lld dumpers %d\n"),
		       holdingdisk_name(holdp[i]->disk->hdisk),
		       (long long)holdp[i]->reserved,
		       (long long)holdp[i]->used,
		       (long long)diff,
		       (long long)holdp[i]->disk->allocated_space,
		       holdp[i]->disk->allocated_dumpers );
	holdp[i]->reserved += diff;
	amfree(hqname);
    }

    sched(diskp)->act_size = total;

    hold_debug(1, _("adjust_diskspace: after: disk %s:%s used %lld\n"),
		   diskp->host->hostname, qname,
		   (long long)sched(diskp)->act_size);
    amfree(qdest);
    amfree(qname);
}

static void
delete_diskspace(
    disk_t *diskp)
{
    assignedhd_t **holdp;
    int i;

    holdp = sched(diskp)->holdp;

    assert(holdp != NULL);

    for( i = 0; holdp[i]; i++ ) { /* for each disk */
	/* find all files of this dump on that disk, and subtract their
	 * reserved sizes from the disk's allocated space
	 */
	holdp[i]->disk->allocated_space -= holdp[i]->used;
    }

    holding_file_unlink(holdp[0]->destname);	/* no need for the entire list,
						 * because holding_file_unlink
						 * will walk through all files
						 * using cont_filename */
    free_assignedhd(sched(diskp)->holdp);
    sched(diskp)->holdp = NULL;
    sched(diskp)->act_size = (off_t)0;
}

static assignedhd_t **
build_diskspace(
    char *	destname)
{
    int i, j;
    int fd;
    size_t buflen;
    char buffer[DISK_BLOCK_BYTES];
    dumpfile_t file;
    gboolean file_set = FALSE;
    assignedhd_t **result;
    holdalloc_t *ha;
    off_t *used;
    char dirname[1000], *ch;
    struct stat finfo;
    char *filename = destname;

    memset(buffer, 0, sizeof(buffer));
    used = g_malloc(sizeof(off_t) * num_holdalloc);
    for(i=0;i<num_holdalloc;i++)
	used[i] = (off_t)0;
    result = g_malloc(sizeof(assignedhd_t *) * (num_holdalloc + 1));
    result[0] = NULL;
    while(filename != NULL && filename[0] != '\0') {
	strncpy(dirname, filename, 999);
	dirname[999]='\0';
	ch = strrchr(dirname,'/');
	if (ch) {
	    *ch = '\0';
	    ch = strrchr(dirname,'/');
	    if (ch) {
		*ch = '\0';
	    }
	}

	if (!ch) {
	    g_fprintf(stderr,_("build_diskspace: bogus filename '%s'\n"), filename);
	    amfree(used);
	    amfree(result);
	    return NULL;
	}

	for(j = 0, ha = holdalloc; ha != NULL; ha = ha->next, j++ ) {
	    if(g_str_equal(dirname, holdingdisk_get_diskdir(ha->hdisk))) {
		break;
	    }
	}
	if (!ha || j >= num_holdalloc) {
	    fprintf(stderr,_("build_diskspace: holding disk file '%s' is not in a holding disk directory.\n"), filename);
	    amfree(used);
	    amfree(result);
	    return NULL;
	}
	if ((fd = open(filename,O_RDONLY)) == -1) {
	    g_fprintf(stderr,_("build_diskspace: open of %s failed: %s\n"),
		    filename, strerror(errno));
	    amfree(used);
	    amfree(result);
	    return NULL;
	}
	if (fstat(fd, &finfo) == -1) {
	    g_fprintf(stderr, _("build_diskspace: can't stat %s: %s\n"),
		      filename, strerror(errno));
	    amfree(used);
	    amfree(result);
	    close(fd);
	    return NULL;
	}
	used[j] += ((off_t)finfo.st_size+(off_t)1023)/(off_t)1024;
	if ((buflen = read_fully(fd, buffer, sizeof(buffer), NULL)) > 0) {
		if (file_set) dumpfile_free_data(&file);
		parse_file_header(buffer, &file, buflen);
		file_set = TRUE;
	}
	close(fd);
	filename = file.cont_filename;
    }
    if (file_set) dumpfile_free_data(&file);

    for(j = 0, i=0, ha = holdalloc; ha != NULL; ha = ha->next, j++ ) {
	if(used[j] != (off_t)0) {
	    result[i] = g_malloc(sizeof(assignedhd_t));
	    result[i]->disk = ha;
	    result[i]->reserved = used[j];
	    result[i]->used = used[j];
	    result[i]->destname = g_strdup(destname);
	    result[i+1] = NULL;
	    i++;
	}
    }

    amfree(used);
    return result;
}

static void
holdingdisk_state(
    char *	time_str)
{
    holdalloc_t *ha;
    int dsk;
    off_t diff;

    g_printf(_("driver: hdisk-state time %s"), time_str);

    for(ha = holdalloc, dsk = 0; ha != NULL; ha = ha->next, dsk++) {
	diff = ha->disksize - ha->allocated_space;
	g_printf(_(" hdisk %d: free %lld dumpers %d"), dsk,
	       (long long)diff, ha->allocated_dumpers);
    }
    g_printf("\n");
}

static void
update_failed_dump(
    disk_t *	dp)
{
    time_t save_timestamp = sched(dp)->timestamp;
    /* setting timestamp to 0 removes the current level from the
     * database, so that we ensure that it will not be bumped to the
     * next level on the next run.  If we didn't do this, dumpdates or
     * gnutar-lists might have been updated already, and a bumped
     * incremental might be created.  */
    sched(dp)->timestamp = 0;
    update_info_dumper(dp, (off_t)-1, (off_t)-1, (time_t)-1);
    sched(dp)->timestamp = save_timestamp;
}

/* ------------------- */

static int
queue_length(
    disklist_t	*q)
{
    return g_list_length(q->head);
}

static void
short_dump_state(void)
{
    int      i, nidle;
    char    *wall_time;
    taper_t *taper;

    wall_time = walltime_str(curclock());

    g_printf(_("driver: state time %s "), wall_time);
    g_printf(_("free kps: %lu space: %lld taper: "),
	   free_kps(NULL),
	   (long long)free_space());
    {
	taper_t *taper;
	int writing = 0;
	int down = TRUE;

	for (taper = tapetable; taper < tapetable + nb_storage ; taper++) {
	    wtaper_t *wtaper;
	    if (!taper->degraded_mode) {
		down = FALSE;
		for(wtaper = taper->wtapetable;
		    wtaper < taper->wtapetable + taper->nb_worker;
		    wtaper++) {
		    if (wtaper->state & TAPER_STATE_DUMP_TO_TAPE ||
			wtaper->state & TAPER_STATE_FILE_TO_TAPE)
			writing = 1;
		}
	    }
	}
	if (down)
	    g_printf(_("down"));
	else if (writing)
	    g_printf(_("writing"));
	else
	    g_printf(_("idle"));
    }
    nidle = 0;
    for(i = 0; i < inparallel; i++) if(!dmptable[i].busy) nidle++;
    g_printf(_(" idle-dumpers: %d"), nidle);
    g_printf(" qlen");
    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	g_printf(_(" tapeq %s: %d"), taper->name, queue_length(&taper->tapeq));
    }
    g_printf(_(" runq: %d"), queue_length(&runq));
    g_printf(_(" directq: %d"), queue_length(&directq));
    g_printf(_(" roomq: %d"), queue_length(&roomq));
    g_printf(_(" wakeup: %d"), (int)sleep_time);
    g_printf(_(" driver-idle: %s\n"), _(idle_strings[idle_reason]));
    interface_state(wall_time);
    holdingdisk_state(wall_time);
    fflush(stdout);
}

static TapeAction
tape_action(
    wtaper_t  *wtaper,
    char     **why_no_new_tape)
{
    TapeAction result = TAPE_ACTION_NO_ACTION;
    dumper_t *dumper;
    taper_t  *taper = wtaper->taper;
    wtaper_t *wtaper1;
    GList    *dlist;
    disk_t   *dp;
    off_t dumpers_size;
    off_t runq_size;
    off_t directq_size;
    off_t tapeq_size;
    off_t sched_size;
    off_t dump_to_disk_size;
    int   dump_to_disk_terminated;
    int   nb_wtaper_active = nb_sent_new_tape;
    int   nb_wtaper_flushing = 0;
    int   dle_free = 0;		/* number of dle that fit on started tape */
    int   new_dle = 0;		/* number of dle that doesn't fit on started tape */
    off_t new_data = 0;		/* size of dle that doesn't fit on started tape */
    off_t data_next_tape = 0;
    off_t data_free = 0;
    off_t data_lost = 0;
    off_t data_lost_next_tape = 0;
    gboolean taperflush_criteria;
    gboolean flush_criteria;

    driver_debug(2, "tape_action: ENTER %p\n", wtaper);
    dumpers_size = 0;
    for(dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if (dumper->busy && !dumper->job->chunker)
	    dumpers_size += sched(dumper->job->disk)->est_size;
    }
    driver_debug(2, _("dumpers_size: %lld\n"), (long long)dumpers_size);

    runq_size = 0;
    for(dlist = runq.head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	runq_size += sched(dp)->est_size;
    }
    driver_debug(2, _("runq_size: %lld\n"), (long long)runq_size);

    directq_size = 0;
    for(dlist = directq.head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	directq_size += sched(dp)->est_size;
    }
    driver_debug(2, _("directq_size: %lld\n"), (long long)directq_size);

    tapeq_size = directq_size;
    for(dlist = taper->tapeq.head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	tapeq_size += sched(dp)->act_size;
    }

    for (wtaper1 = taper->wtapetable;
	 wtaper1 < taper->wtapetable + taper->nb_worker;
	 wtaper1++) {
	if (wtaper1->state & TAPER_STATE_FILE_TO_TAPE ||
	    wtaper1->state & TAPER_STATE_DUMP_TO_TAPE) {
	    nb_wtaper_flushing++;
	}
    }

    /* Add what is currently written to tape and in the go. */
    new_data = 0;
    for (wtaper1 = taper->wtapetable;
	 wtaper1 < taper->wtapetable + taper->nb_worker;
	 wtaper1++) {
	if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
	    if (wtaper1->nb_dle < taper->max_dle_by_volume) {
		tapeq_size -= wtaper1->left;
	    }
	    dle_free += (taper->max_dle_by_volume - wtaper1->nb_dle);
	}
	if (wtaper1->job && wtaper1->job->disk) {
	    off_t data_to_go;
	    off_t t_size;
	    if (wtaper1->job->dumper) {
		t_size = sched(wtaper1->job->disk)->est_size;
	    } else {
		t_size = sched(wtaper1->job->disk)->act_size;
	    }
	    data_to_go =  t_size - wtaper1->written;
	    if (data_to_go > wtaper1->left) {
		data_next_tape += data_to_go - wtaper1->left;
		data_lost += wtaper1->written + wtaper1->left;
		if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
		    dle_free -= (taper->max_dle_by_volume - wtaper1->nb_dle) + 1;
		} else {
		    dle_free -= 2;
		    new_data += t_size;
		}
	    } else {
		if (!(wtaper1->state & TAPER_STATE_TAPE_STARTED)) {
		    dle_free--;
		    new_data += t_size;
		}
		data_free += wtaper1->left - data_to_go;
	    }
	    tapeq_size += data_to_go;
	}
    }

    new_dle = queue_length(&taper->tapeq) - dle_free;
    driver_debug(2, _("dle_free: %d\n"), dle_free);
    driver_debug(2, _("new_dle: %d\n"), new_dle);
    if (new_dle > 0) {
	if (taper->taperflush == 0 &&
	    taper->flush_threshold_dumped == 0 &&
	    taper->flush_threshold_scheduled == 0) {
	    /* shortcut, will trigger taperflush_criteria and/or flush_criteria */
	    new_data += 1;
	} else {
	    /* sum the size of the first new-dle in tapeq */
	    /* they should be the reverse taperalgo       */
	    for (dlist = taper->tapeq.head;
		 dlist != NULL && new_dle > 0;
		 dlist = dlist->next, new_dle--) {
		dp = dlist->data;
		new_data += sched(dp)->act_size;
	    }
	}
	if (tapeq_size < new_data) {
	    tapeq_size = new_data;
	}
    }
    driver_debug(2, _("new_data: %lld\n"), (long long)new_data);
    data_lost_next_tape = taper->tape_length + data_free - data_next_tape - runq_size - directq_size - tapeq_size;
    driver_debug(2, _("data_lost: %lld\n"), (long long)data_lost);
    driver_debug(2, _("data_free: %lld\n"), (long long)data_free);
    driver_debug(2, _("data_next_tape: %lld\n"), (long long)data_next_tape);
    driver_debug(2, _("data_lost_next_tape: %lld\n"), (long long)data_lost_next_tape);
;
    driver_debug(2, _("tapeq_size: %lld\n"), (long long)tapeq_size);

    sched_size = runq_size + directq_size + tapeq_size + dumpers_size;
    driver_debug(2, _("sched_size: %lld\n"), (long long)sched_size);

    dump_to_disk_size = dumpers_size + runq_size + directq_size;
    driver_debug(2, _("dump_to_disk_size: %lld\n"), (long long)dump_to_disk_size);

    dump_to_disk_terminated = schedule_done && dump_to_disk_size == 0;

    for (wtaper1 = taper->wtapetable;
	 wtaper1 < taper->wtapetable + taper->nb_worker;
	 wtaper1++) {
	if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
	    nb_wtaper_active++;
	}
    }

    taperflush_criteria = (taper->taperflush < tapeq_size &&
			   (force_flush == 1 || dump_to_disk_terminated));
    flush_criteria = (taper->flush_threshold_dumped < tapeq_size &&
		      taper->flush_threshold_scheduled < sched_size) ||
		     taperflush_criteria;

    driver_debug(2, "taperflush %lld\n", (long long)taper->taperflush);
    driver_debug(2, "flush_threshold_dumped %lld\n", (long long)taper->flush_threshold_dumped);
    driver_debug(2, "flush_threshold_scheduled %lld\n", (long long)taper->flush_threshold_scheduled);
    driver_debug(2, "force_flush %d\n", force_flush);
    driver_debug(2, "dump_to_disk_terminated %d\n", dump_to_disk_terminated);
    driver_debug(2, "queue_length(runq) %d\n", queue_length(&runq));
    driver_debug(2, "queue_length(directq) %d\n", queue_length(&directq));
    driver_debug(2, "queue_length(tapeq) %d\n", queue_length(&taper->tapeq));
    driver_debug(2, "taperflush_criteria %d\n", taperflush_criteria);
    driver_debug(2, "flush_criteria %d\n", flush_criteria);

    // Changing conditionals can produce a driver hang, take care.
    //
    // when to start writting to a new tape
    if (wtaper->state & TAPER_STATE_TAPE_REQUESTED) {
	driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED\n");
	if (taper->current_tape >= taper->runtapes &&
	    taper->nb_scan_volume == 0 &&
	    nb_wtaper_active == 0) {
	    *why_no_new_tape = g_strdup_printf(_("%d tapes filled; runtapes=%d "
		"does not allow additional tapes"), taper->current_tape,
		taper->runtapes);
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_NO_NEW_TAPE\n");
	    result |= TAPE_ACTION_NO_NEW_TAPE;
	} else if (taper->current_tape < taper->runtapes &&
		   taper->nb_scan_volume == 0 &&
		   (flush_criteria ||
		    (data_lost > data_lost_next_tape) ||
		    nb_wtaper_active == 0) &&
		   (taper->last_started_wtaper == NULL ||
		    taper->last_started_wtaper == wtaper)) {
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_SCAN\n");
	    result |= TAPE_ACTION_SCAN;
	} else {
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_MOVE\n");
	    result |= TAPE_ACTION_MOVE;
	}
    } else if ((wtaper->state & TAPER_STATE_WAIT_FOR_TAPE) &&
        ((wtaper->state & TAPER_STATE_DUMP_TO_TAPE) ||	// for dump to tape
	 !empty(directq) ||				// if a dle is waiting for a dump to tape
         !empty(roomq) ||				// holding disk constraint
         idle_reason == IDLE_NO_DISKSPACE ||		// holding disk constraint
	 flush_criteria ||				// flush criteria
	 data_lost > data_lost_next_tape
	)) {
	driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE return TAPE_ACTION_NEW_TAPE\n");
	result |= TAPE_ACTION_NEW_TAPE;
    // when to stop using new tape
    } else if ((wtaper->state & TAPER_STATE_WAIT_FOR_TAPE) &&
	       (taper->taperflush >= tapeq_size &&		// taperflush criteria
	       (force_flush == 1 ||			//  if force_flush
	        dump_to_disk_terminated))		//  or all dump to disk
	      ) {
	driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE B\n");
	if (nb_wtaper_active <= 0) {
	    if (taper->current_tape >= taper->runtapes) {
		*why_no_new_tape = g_strdup_printf(_("%d tapes filled; runtapes=%d "
		      "does not allow additional tapes"), taper->current_tape,
		      taper->runtapes);
		driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE return TAPE_ACTION_NO_NEW_TAPE\n");
		result |= TAPE_ACTION_NO_NEW_TAPE;
	    } else if (dumpers_size <= 0) {
		*why_no_new_tape = _("taperflush criteria not met");
		driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE return TAPE_ACTION_NO_NEW_TAPE\n");
		result |= TAPE_ACTION_NO_NEW_TAPE;
	    }
	}
    }

    // when to start a flush
    if (wtaper->state & TAPER_STATE_IDLE) {
	driver_debug(2, "tape_action: TAPER_STATE_IDLE\n");
	if (!taper->degraded_mode &&
	    (!empty(directq) ||
	     (!empty(taper->tapeq) &&
	      (wtaper->state & TAPER_STATE_TAPE_STARTED ||		// tape already started
               !empty(roomq) ||						// holding disk constraint
               idle_reason == IDLE_NO_DISKSPACE ||			// holding disk constraint
	       flush_criteria)))) {					// flush

	    if (nb_wtaper_flushing == 0) {
		driver_debug(2, "tape_action: TAPER_STATE_IDLE return TAPE_ACTION_START_A_FLUSH\n");
		result |= TAPE_ACTION_START_A_FLUSH;
	    } else {
		driver_debug(2, "tape_action: TAPER_STATE_IDLE return TAPE_ACTION_START_A_FLUSH_FIT\n");
		result |= TAPE_ACTION_START_A_FLUSH_FIT;
	    }
	} else {
	    driver_debug(2, "tape_action: TAPER_STATE_IDLE return TAPE_ACTION_NO_ACTION\n");
	}
    }
    return result;
}

static int
no_taper_flushing(void)
{
    taper_t  *taper;
    wtaper_t *wtaper;

    for (taper = tapetable; taper < tapetable + nb_storage; taper++) {
	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (wtaper->state & TAPER_STATE_FILE_TO_TAPE)
		return 0;
	}
    }
    return 1;
}

static int
active_dumper(void)
{
    int i, nidle=0;

    for(i = 0; i < inparallel; i++) if(!dmptable[i].busy) nidle++;
    return inparallel - nidle;
}

static void
fix_index_header(
    disk_t *dp)
{
    int         fd;
    size_t      buflen;
    char        buffer[DISK_BLOCK_BYTES];
    char       *read_buffer;
    dumpfile_t  file;
    char       *f = getheaderfname(dp->host->hostname, dp->name, driver_timestamp, sched(dp)->level);

    if((fd = robust_open(f, O_RDWR, 0)) == -1) {
        dbprintf(_("holding_set_origsize: open of %s failed: %s\n"),
                 f, strerror(errno));
	g_free(f);
        return;
    }

    buflen = read_fully(fd, buffer, sizeof(buffer), NULL);
    if (buflen <= 0) {
        dbprintf(_("fix_index_header: %s: empty file?\n"), f);
        close(fd);
	g_free(f);
        return;
    }
    parse_file_header(buffer, &file, (size_t)buflen);
    lseek(fd, (off_t)0, SEEK_SET);
    if (ftruncate(fd, 0) == -1) {
	g_debug("ftruncate of '%s' failed: %s", f, strerror(errno));
    }
    g_free(f);
    file.orig_size = sched(dp)->origsize;
    file.native_crc = sched(dp)->native_crc;
    file.client_crc = sched(dp)->client_crc;
    file.server_crc = sched(dp)->server_crc;
    read_buffer = build_header(&file, NULL, DISK_BLOCK_BYTES);
    full_write(fd, read_buffer, strlen(read_buffer));
    dumpfile_free_data(&file);
    amfree(read_buffer);
    close(fd);
}

#if 0
static void
dump_state(
    const char *str)
{
    int i;
    disk_t *dp;
    char *qname;

    g_printf("================\n");
    g_printf(_("driver state at time %s: %s\n"), walltime_str(curclock()), str);
    g_printf(_("free kps: %lu, space: %lld\n"),
	   free_kps(NULL),
	   (long long)free_space());
    if(degraded_mode) g_printf(_("taper: DOWN\n"));
    else if(taper->status == TAPER_IDLE) g_printf(_("taper: idle\n"));
    else g_printf(_("taper: writing %s:%s.%d est size %lld\n"),
		taper->disk->host->hostname, taper->disk->name,
		sched(taper->disk)->level,
		(long long)sched(taper->disk)->est_size);
    for(i = 0; i < inparallel; i++) {
	dp = dmptable[i].dp;
	if(!dmptable[i].busy)
	  g_printf(_("%s: idle\n"), dmptable[i].name);
	else
	  qname = quote_string(dp->name);
	  g_printf(_("%s: dumping %s:%s.%d est kps %d size %lld time %lu\n"),
		dmptable[i].name, dp->host->hostname, qname, sched(dp)->level,
		sched(dp)->est_kps, (long long)sched(dp)->est_size, sched(dp)->est_time);
          amfree(qname);
    }
    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	dump_queue("TAPE", taper->tapeq, 5, stdout);
    }
    dump_queue("ROOM", roomq, 5, stdout);
    dump_queue("RUN ", runq, 5, stdout);
    g_printf("================\n");
    fflush(stdout);
}
#endif
