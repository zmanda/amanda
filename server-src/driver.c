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
#include "tapefile.h"
#include "shm-ring.h"

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

#define HOST_DELAY 0

static disklist_t  waitq;	// dle waiting estimate result
static schedlist_t runq;	// dle waiting to be dumped to holding disk
static schedlist_t directq;	// dle waiting to be dumped directly to tape
static schedlist_t roomq;	// dle waiting for more space on holding disk
static int pending_aborts;
static gboolean all_degraded_mode = FALSE;
static off_t reserved_space;
static off_t total_disksize;
static char *dumper_program;
static char *chunker_program;
static int  inparallel;
static int nodump = 0;
static int novault = 0;
static storage_t *storage;
static int conf_max_dle_by_volume;
static int conf_taperalgo;
static int conf_taper_parallel_write;
static int conf_runtapes;
static char *conf_cmdfile;
static unsigned long conf_reserve = 100;
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
static int nb_sent_new_tape = 0;
static int taper_started = 0;
static int nb_storage;
static cmddatas_t *cmddatas = NULL;

static int wait_children(int count);
static void wait_for_children(void);
static void allocate_bandwidth(netif_t *ip, unsigned long kps);
static int assign_holdingdisk(assignedhd_t **holdp, sched_t *sp);
static void adjust_diskspace(sched_t *sp, cmd_t cmd);
static void delete_diskspace(sched_t *sp);
static assignedhd_t **build_diskspace(char *destname);
static int client_constrained(disk_t *dp);
static void deallocate_bandwidth(netif_t *ip, unsigned long kps);
static void dump_schedule(schedlist_t *qp, char *str);
static assignedhd_t **find_diskspace(off_t size, int *cur_idle,
					assignedhd_t *preferred);
static unsigned long network_free_kps(netif_t *ip);
static off_t holding_free_space(void);
static void dumper_chunker_result(job_t *job);
static void dumper_chunker_result_finish(job_t *job);
static void dumper_taper_result(job_t *job);
static void dumper_taper_result_finish(job_t *job);
static void vault_taper_result(job_t *job);
static void file_taper_result(job_t *job);
static void handle_dumper_result(void *);
static void handle_chunker_result(void *);
static void handle_dumpers_time(void *);
static void handle_taper_result(void *);
static gboolean dump_match_selection(char *storage_n, sched_t *sp);

static void holdingdisk_state(char *time_str);
static wtaper_t *idle_taper(taper_t *taper);
static wtaper_t *wtaper_from_name(taper_t *taper, char *name);
static void interface_state(char *time_str);
static int queue_length(schedlist_t *q);
static void read_flush(void *cookie);
static void read_schedule(void *cookie);
static void set_vaultqs(void);
static void short_dump_state(void);
static void start_a_flush_wtaper(wtaper_t    *wtaper,
                                 gboolean    *state_changed);
static void start_a_flush_taper(taper_t    *taper);
static void start_a_vault_wtaper(wtaper_t    *wtaper,
                                 gboolean    *state_changed);
static void start_a_vault_taper(taper_t    *taper);
static void start_a_vault(void);
static void start_a_flush(void);
static void start_degraded_mode(schedlist_t *queuep);
static void start_some_dumps(schedlist_t *rq);
static void start_vault_on_same_wtaper(wtaper_t *wtaper);
static void continue_port_dumps(void);
static void update_failed_dump(sched_t *sp);
static int no_taper_flushing(void);
static int active_dumper(void);
static void fix_index_header(sched_t *sp);
static int all_tapeq_empty(void);

typedef enum {
    TAPE_ACTION_NO_ACTION         = 0,
    TAPE_ACTION_SCAN              = (1 << 0),
    TAPE_ACTION_NEW_TAPE          = (1 << 1),
    TAPE_ACTION_NO_NEW_TAPE       = (1 << 2),
    TAPE_ACTION_START_A_FLUSH     = (1 << 3),
    TAPE_ACTION_START_A_FLUSH_FIT = (1 << 4),
    TAPE_ACTION_MOVE              = (1 << 5),
    TAPE_ACTION_START_TAPER       = (1 << 6)
} TapeAction;

static TapeAction tape_action(wtaper_t *wtaper,
			      char **why_no_new_tape,
			      gboolean action_flush);

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

static void enqueue_sched(schedlist_t *list, sched_t *sp);
static void headqueue_sched(schedlist_t *list, sched_t *sp);
static void insert_before_sched(schedlist_t *list, GList *list_before, sched_t *sp);
static int find_sched(schedlist_t *list, sched_t *sp);
static sched_t *dequeue_sched(schedlist_t *list);
static void remove_sched(schedlist_t *list, sched_t *sp);

int
main(
    int		argc,
    char **	argv)
{
    disklist_t origq;
    int dsk;
    dumper_t *dumper;
    taper_t  *taper;
    wtaper_t *wtaper;
    char *newdir = NULL;
    struct fs_usage fsusage;
    holdingdisk_t *hdp;
    identlist_t    il;
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
    char *storage_n;
    int sum_taper_parallel_write;
    char *argv0;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("driver-%s\n", VERSION);
	return (0);
    }

    glib_init();

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
    argv0 = argv[0];

    if(argc > 2) {
        if(g_str_equal(argv[2], "nodump")) {
            nodump = 1;
	    argv++;
	    argc--;
        }
    }

    if(argc > 2) {
        if(g_str_equal(argv[2], "--no-vault")) {
            novault = 1;
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
	    //driver do nothing with --from-client
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
	   get_pname(), (long) getpid(), argv0, VERSION);

    safe_cd(); /* do this *after* config_init */

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    /* load DLEs from the holding disk, in case there's anything to flush there */
    search_holding_disk(&holding_files, &holding_disklist, 1);
    /* note that the dumps are added to the global disklist, so we need not consult
     * holding_files or holding_disklist after this */

    cleanup_shm_ring();

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
	    char *conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
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
    storage_n = il->data;
    storage = lookup_storage(storage_n);
    conf_taperalgo = storage_get_taperalgo(storage);
    conf_taper_parallel_write = storage_get_taper_parallel_write(storage);
    conf_runtapes = storage_get_runtapes(storage);
    conf_max_dle_by_volume = storage_get_max_dle_by_volume(storage);
    if (conf_taper_parallel_write > conf_runtapes) {
	conf_taper_parallel_write = conf_runtapes;
    }

    /* set up any configuration-dependent variables */

    inparallel	= getconf_int(CNF_INPARALLEL);

    conf_reserve = (unsigned long)getconf_int(CNF_RESERVE);

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

    reserved_space = total_disksize * (off_t)(conf_reserve / 100);

    g_printf(_("reserving %lld out of %lld for degraded-mode dumps\n"),
	   (long long)reserved_space, (long long)holding_free_space());

    amfree(newdir);

    /* taper takes a while to get going, so start it up right away */

    nb_storage = 0;
    sum_taper_parallel_write = 0;
    for (il = getconf_identlist(CNF_STORAGE); il != NULL; il = il->next) {
	storage_t *storage = lookup_storage((char *)il->data);
	nb_storage++;
	sum_taper_parallel_write += storage_get_taper_parallel_write(storage);
    }
    for (il = getconf_identlist(CNF_VAULT_STORAGE); il != NULL; il = il->next) {
	storage_t *storage = lookup_storage((char *)il->data);
	nb_storage++;
	sum_taper_parallel_write += storage_get_taper_parallel_write(storage);
    }
    init_driverio(inparallel, nb_storage, sum_taper_parallel_write);
    nb_storage = startup_dump_tape_process(taper_program, no_taper);

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
	    if (!taper->down && taper->storage_name) {
		wtaper = taper->wtapetable;
		wtaper->state = TAPER_STATE_INIT;
		taper->nb_wait_reply++;
		taper->nb_scan_volume++;
		taper->ev_read = event_create(taper->fd, EV_READFD,
					      handle_taper_result, taper);
		event_activate(taper->ev_read);
		taper_cmd(taper, wtaper, START_TAPER, NULL, taper->wtapetable[0].name, 0, driver_timestamp);
	    }
	}
    }

    conf_cmdfile = config_dir_relative(getconf_str(CNF_CMDFILE));
    cmddatas = read_cmdfile(conf_cmdfile);
    unlock_cmdfile(cmddatas);

    flush_ev_read = event_create((event_id_t)0, EV_READFD, read_flush, NULL);
    event_activate(flush_ev_read);

    log_add(L_STATS, _("startup time %s"), walltime_str(curclock()));

    g_printf(_("driver: start time %s inparallel %d bandwidth %lu diskspace %lld "), walltime_str(curclock()), inparallel,
	   network_free_kps(NULL), (long long)holding_free_space());
    g_printf(_(" dir %s datestamp %s driver: drain-ends tapeq %s big-dumpers %s\n"),
	   "OBSOLETE", driver_timestamp, taperalgo2str(conf_taperalgo),
	   getconf_str(CNF_DUMPORDER));
    fflush(stdout);

    schedule_done = nodump;
    force_flush = 0;

    short_dump_state();
    event_loop(0);
    short_dump_state();

    force_flush = 1;

    /* mv runq to directq */
    short_dump_state();
    g_printf("Move runq to directq\n");
    while (!empty(runq)) {
	sched_t *sp = dequeue_sched(&runq);
	sp->action = ACTION_DUMP_TO_TAPE;
	headqueue_sched(&directq, sp);
    }
    short_dump_state();

    run_server_global_scripts(EXECUTE_ON_POST_BACKUP, get_config_name(),
			      driver_timestamp);

    /* log error for any remaining dumps */
    while(!empty(directq)) {
	sched_t *sp = dequeue_sched(&directq);
	disk_t *dp = sp->disk;
	char *qname = quote_string(dp->name);

	if (dp->orig_holdingdisk == HOLD_REQUIRED) {
	    log_add(L_FAIL, "%s %s %s %d [%s]",
		dp->host->hostname, qname, sp->datestamp,
		sp->level,
		_("can't dump required holdingdisk"));
	} else {
	    gboolean dp_degraded_mode = (nb_storage == 0);
	    gboolean reach_runtapes = FALSE;
	    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		if (dump_match_selection(taper->storage_name, sp)) {
		    dp_degraded_mode |= taper->degraded_mode;
		    reach_runtapes |= taper->current_tape >= taper->runtapes;
		}
	    }
	    if (!dp_degraded_mode && !reach_runtapes) {
		identlist_t tags;
		int count = 0;
		for (tags = dp->tags; tags != NULL ; tags = tags->next) {
		    identlist_t il;
		    for (il = getconf_identlist(CNF_STORAGE); il != NULL; il = il->next) {
			char *storage_n = il->data;
			storage_t *storage = lookup_storage(storage_n);
			if (storage) {
			    dump_selection_list_t dsl = storage_get_dump_selection(storage);
			    if (!dsl)
			    continue;
			    for (; dsl != NULL ; dsl = dsl->next) {
				dump_selection_t *ds = dsl->data;
				if (ds->tag_type == TAG_ALL) {
				    count++;
				} else if (ds->tag_type == TAG_NAME) {
				    if (g_str_equal(ds->tag, tags->data)) {
					if (ds->level == LEVEL_ALL ||
					    (ds->level == LEVEL_FULL && sp->level == 0) ||
					    (ds->level == LEVEL_INCR && sp->level > 0)) {
					    count++;
					}
				    }
				}
			    }
			}
		    }
		}
		if (dp->tags && count == 0) {
		    log_add(L_FAIL, "%s %s %s %d [%s]",
			dp->host->hostname, qname, sp->datestamp,
			sp->level,
			_("The tags matches none of the active storage"));
		} else {
		    log_add(L_FAIL, "%s %s %s %d [%s]",
			dp->host->hostname, qname, sp->datestamp,
			sp->level,
			_("can't dump in non degraded mode"));
		}
	    } else {
		log_add(L_FAIL, "%s %s %s %d [%s]",
			dp->host->hostname, qname, sp->datestamp,
			sp->level,
			num_holdalloc == 0 ?
			    _("can't do degraded dump without holding disk") :
			    dp->orig_holdingdisk != HOLD_NEVER ?
				_("out of holding space in degraded mode") :
				reach_runtapes ?
				    _("can't dump 'holdingdisk never' dle after runtapes tapes are used") :
				    _("can't dump 'holdingdisk never' dle in degraded mode"));
	    }
	}
	amfree(qname);
    }

    short_dump_state();				/* for amstatus */

    /* close device for storage */
    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	if (!taper->degraded_mode && !taper->vault_storage && taper->storage_name) {
	    for (wtaper = taper->wtapetable;
		 wtaper < taper->wtapetable + taper->nb_worker;
		 wtaper++) {
		if (wtaper->state & TAPER_STATE_RESERVATION) {
		    if (taper->nb_wait_reply == 0) {
			taper->ev_read = event_create(taper->fd,
						EV_READFD,
						handle_taper_result, taper);
			event_activate(taper->ev_read);
		    }
		    taper->nb_wait_reply++;
		    wtaper->state |= TAPER_STATE_WAIT_CLOSED_VOLUME;
		    wtaper->state &= ~TAPER_STATE_IDLE;
		    taper_cmd(taper, wtaper, CLOSE_VOLUME, NULL, NULL, 0, NULL);
		    wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		}
	    }
	}
    }

    short_dump_state();
    /* wait for the device to be closed */
    event_loop(0);
    short_dump_state();

    if (!novault) {
	/* close device for storage */
	for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	    if (taper->fd >= 0 && !taper->vault_storage) {
		taper_cmd(taper, NULL, QUIT, NULL, NULL, 0, NULL);
	    }
	}

	nb_storage = startup_vault_tape_process(taper_program, no_taper);

	for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	    if (!taper->down && taper->storage_name) {
		wtaper = taper->wtapetable;
		wtaper->state = TAPER_STATE_INIT;
		taper->nb_wait_reply++;
		taper->nb_scan_volume++;
		taper->ev_read = event_create(taper->fd, EV_READFD,
						handle_taper_result, taper);
		event_activate(taper->ev_read);
		taper_cmd(taper, wtaper, START_TAPER, NULL, taper->wtapetable[0].name, 0, driver_timestamp);
	    }
	}

	set_vaultqs();
	short_dump_state();

	start_a_vault();
	event_loop(0);
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

    for (taper = tapetable; taper < tapetable+nb_storage; taper++) {
	if (taper->fd >= 0) {
	    taper_cmd(taper, NULL, QUIT, NULL, NULL, 0, NULL);
	}
    }

    /* wait for all to die */
    wait_children(600);

    /* cleanup */
    holding_cleanup(NULL, NULL);

    cmddatas = remove_working_in_cmdfile(cmddatas, getppid());

    amfree(newdir);

    check_unfree_serial();
    g_printf(_("driver: FINISHED time %s\n"), walltime_str(curclock()));
    fflush(stdout);
    log_add(L_FINISH,_("date %s time %s"), driver_timestamp, walltime_str(curclock()));
    log_add(L_INFO, "pid-done %ld", (long)getpid());
    amfree(driver_timestamp);

    amfree(dumper_program);
    amfree(taper_program);

    cleanup_shm_ring();

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
		g_debug("reap: %d", (int)pid);
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
		for (taper = tapetable; taper < tapetable+nb_storage; taper++) {
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
	if (errno != ECHILD) {
	    gulong delay = G_USEC_PER_SEC/100; /* 1 msec */
	    g_usleep(delay);
	}
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

    for (taper = tapetable; taper < tapetable+nb_storage; taper++) {
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

    for (taper = tapetable; taper < tapetable+nb_storage; taper++) {
	if (taper->pid > 1 && taper->fd > 0) {
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

static void
start_a_flush(void)
{
    taper_t *taper;

    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
        if (taper->storage_name && taper->flush_storage) {
	    start_a_flush_taper(taper);
	}
    }
}

static void
start_a_flush_taper(
    taper_t *taper)
{
    gboolean state_changed = FALSE;
    wtaper_t *wtaper;

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_TAPE_STARTED) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_RESERVATION) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_TAPE_STARTED) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_RESERVATION) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_WAIT_FOR_TAPE) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_TAPE_REQUESTED) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}
	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_INIT) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}
	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_IDLE) {
		start_a_flush_wtaper(wtaper, &state_changed);
	    }
	}

    if (state_changed) {
	short_dump_state();
    }
}

static void
start_a_flush_wtaper(
    wtaper_t    *wtaper,
    gboolean    *state_changed)
{
    GList  *fit;
    sched_t *sp = NULL;
    sched_t *sfit = NULL;
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

    result_tape_action = tape_action(wtaper, &why_no_new_tape, TRUE);

    if (result_tape_action & TAPE_ACTION_SCAN) {
	wtaper->state &= ~TAPER_STATE_TAPE_REQUESTED;
	wtaper->state |= TAPER_STATE_WAIT_FOR_TAPE;
	wtaper->taper->nb_scan_volume++;
	taper_cmd(taper, wtaper, START_SCAN, wtaper->job->sched, NULL, 0, NULL);
    } else if (result_tape_action & TAPE_ACTION_NEW_TAPE) {
	wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	wtaper->state |= TAPER_STATE_WAIT_NEW_TAPE;
	nb_sent_new_tape++;
	taper_cmd(taper, wtaper, NEW_TAPE, wtaper->job->sched, NULL, 0, NULL);
    } else if (result_tape_action & TAPE_ACTION_NO_NEW_TAPE) {
	wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	taper_cmd(taper, wtaper, NO_NEW_TAPE, wtaper->job->sched, why_no_new_tape, 0, NULL);
	wtaper->state |= TAPER_STATE_DONE;
	taper->degraded_mode = TRUE;
	start_degraded_mode(&runq);
	*state_changed = TRUE;
    } else if (result_tape_action & TAPE_ACTION_MOVE) {
	/* move from wtaper to wtaper1 */
	wtaper1 = idle_taper(taper);
	if (wtaper1) {
	    wtaper->state &= ~TAPER_STATE_TAPE_REQUESTED;
	    wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	    taper_cmd(taper, wtaper, TAKE_SCRIBE_FROM, wtaper->job->sched, wtaper1->name,
		      0 , NULL);
	    wtaper->state |= (wtaper1->state & TAPER_STATE_TAPE_STARTED);
	    wtaper->left = wtaper1->left;
	    wtaper->nb_dle = wtaper1->nb_dle+1;
	    wtaper1->state = TAPER_STATE_DEFAULT;
	    if (taper->last_started_wtaper == wtaper) {
		taper->last_started_wtaper = NULL;
	    } else if (taper->last_started_wtaper == wtaper1) {
		taper->last_started_wtaper = wtaper;
	    }
	    if (taper->sent_first_write == wtaper) {
		taper->sent_first_write = NULL;
	    } else if (taper->sent_first_write == wtaper1) {
		taper->sent_first_write = wtaper;
	    }
	    *state_changed = TRUE;
	}
    }

    if (!taper->degraded_mode &&
        wtaper->state & TAPER_STATE_IDLE &&
	!empty(wtaper->taper->tapeq) &&
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
		extra_tapes_size -= (wtaper1->job->sched->act_size - wtaper1->written);
	    }
	}

	if (wtaper->state & TAPER_STATE_TAPE_STARTED) {
	    taper_left = wtaper->left;
	} else {
	    taper_left = taper->tape_length;
	}
	sp = NULL;
	datestamp = ((sched_t *)(wtaper->taper->tapeq.head->data))->datestamp;
	switch(taperalgo) {
	case ALGO_FIRST:
		sp = dequeue_sched(&wtaper->taper->tapeq);
		break;
	case ALGO_FIRSTFIT:
		fit = wtaper->taper->tapeq.head;
		while (fit != NULL) {
		    sfit = fit->data;
		    dfit = sfit->disk;
		    if (sfit->act_size <=
		             ((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
			     strcmp(sfit->datestamp, datestamp) <= 0) {
			sp = sfit;
			fit = NULL;
		    } else {
			fit = fit->next;
		    }
		}
		if (sp) remove_sched(&wtaper->taper->tapeq, sp);
		break;
	case ALGO_LARGEST:
		fit = wtaper->taper->tapeq.head;
		sp = fit->data;
		while (fit != NULL) {
		    sfit = fit->data;
		    if (sfit->act_size > sp->act_size &&
		        strcmp(sfit->datestamp, datestamp) <= 0) {
			sp = sfit;
		    }
		    fit = fit->next;
		}
		if (sp) remove_sched(&wtaper->taper->tapeq, sp);
		break;
	case ALGO_LARGESTFIT:
		fit = wtaper->taper->tapeq.head;
		while (fit != NULL) {
		    sfit = fit->data;
		    dfit = sfit->disk;
		    if (sfit->act_size <=
		        ((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
		        (!sp || sfit->act_size > sp->act_size) &&
		       strcmp(sfit->datestamp, datestamp) <= 0) {
			sp = sfit;
		    }
		    fit = fit->next;
		}
		if (sp) remove_sched(&wtaper->taper->tapeq, sp);
		break;
	case ALGO_SMALLEST:
		fit = wtaper->taper->tapeq.head;
		sp = fit->data;
		while (fit != NULL) {
		    sfit = fit->data;
		    if (sfit->act_size < sp->act_size &&
			strcmp(sfit->datestamp, datestamp) <= 0) {
			sp = sfit;
		    }
	            fit = fit->next;
		}
		if (sp) remove_sched(&wtaper->taper->tapeq, sp);
		break;
	case ALGO_SMALLESTFIT:
		fit = wtaper->taper->tapeq.head;
		while (fit != NULL) {
		    sfit = fit->data;
		    dfit = sfit->disk;
		    if (sfit->act_size <=
			((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
			(!sp || sfit->act_size < sp->act_size) &&
			strcmp(sfit->datestamp, datestamp) <= 0) {
			sp = sfit;
		    }
	            fit = fit->next;
		}
		if (sp) remove_sched(&wtaper->taper->tapeq, sp);
		break;
	case ALGO_LAST:
		sp = wtaper->taper->tapeq.tail->data;
		if (sp) remove_sched(&wtaper->taper->tapeq, sp);
		break;
	case ALGO_LASTFIT:
		fit = wtaper->taper->tapeq.tail;
		while (fit != NULL) {
		    sfit = fit->data;
		    dfit = sfit->disk;
		    if (sfit->act_size <=
			((dfit->tape_splitsize || dfit->allow_split) ? extra_tapes_size : taper_left) &&
			(!sp || sfit->act_size < sp->act_size) &&
			strcmp(sfit->datestamp, datestamp) <= 0) {
			sp = sfit;
		    }
	            fit = fit->prev;
		}
		if (sp) remove_sched(&wtaper->taper->tapeq, sp);
		break;
	}
	if (!sp) {
	    if (!(result_tape_action & TAPE_ACTION_START_A_FLUSH_FIT)) {
		if (conf_taperalgo != ALGO_SMALLEST)  {
		    g_fprintf(stderr,
			_("driver: startaflush: Using SMALLEST because nothing fit\n"));
		}

		fit = wtaper->taper->tapeq.head;
		sp = fit->data;
		while (fit != NULL) {
		    sfit = fit->data;
		    if (sfit->act_size < sp->act_size &&
			strcmp(sfit->datestamp, datestamp) <= 0) {
			sp = sfit;
		    }
	            fit = fit->next;
		}
		if(sp) remove_sched(&wtaper->taper->tapeq, sp);
	    }
	}
	if (sp) {
	    job_t *job = alloc_job();

	    job->wtaper = wtaper;
	    job->sched = sp;
	    dp = sp->disk;
	    wtaper->job = job;

	    amfree(wtaper->input_error);
	    amfree(wtaper->tape_error);
	    wtaper->result = LAST_TOK;
	    wtaper->sendresult = FALSE;
	    amfree(wtaper->first_label);
	    amfree(wtaper->dst_labels_str);
	    if (wtaper->dst_labels) {
		slist_free_full(wtaper->dst_labels, g_free);
		wtaper->dst_labels = NULL;
	    }
	    wtaper->written = 0;
	    wtaper->state &= ~TAPER_STATE_IDLE;
	    wtaper->state |= TAPER_STATE_FILE_TO_TAPE;
	    qname = quote_string(dp->name);
	    if (taper->nb_wait_reply == 0) {
		taper->ev_read = event_create(taper->fd, EV_READFD,
					      handle_taper_result, taper);
		event_activate(taper->ev_read);
	    }
	    taper->nb_wait_reply++;
	    wtaper->nb_dle++;
	    if (!(wtaper->state & TAPER_STATE_TAPE_STARTED)) {
		assert(taper->sent_first_write == NULL);
		taper->sent_first_write = wtaper;
		wtaper->nb_dle = 1;
		wtaper->left = taper->tape_length;
	    }
	    taper_cmd(taper, wtaper, FILE_WRITE, sp, sp->destname,
		      sp->level,
		      sp->datestamp);
	    amfree(qname);
	    *state_changed = TRUE;
	}
    }
}

static void
start_a_vault(void)
{
    taper_t *taper;

    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
        if (taper->storage_name && taper->vault_storage) {
	    start_a_vault_taper(taper);
	}
    }
}

static void
start_a_vault_taper(
    taper_t *taper)
{
    gboolean state_changed = FALSE;
    wtaper_t *wtaper;

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_WAIT_FOR_TAPE) {
		start_a_vault_wtaper(wtaper, &state_changed);
	    }
	}

	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_TAPE_REQUESTED) {
		start_a_vault_wtaper(wtaper, &state_changed);
	    }
	}
	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_INIT) {
		start_a_vault_wtaper(wtaper, &state_changed);
	    }
	}
	for (wtaper = taper->wtapetable;
	     wtaper < taper->wtapetable + taper->nb_worker;
	     wtaper++) {
	    if (!(wtaper->state & TAPER_STATE_DONE) &&
		wtaper->state & TAPER_STATE_IDLE) {
		start_a_vault_wtaper(wtaper, &state_changed);
	    }
	}

    if (state_changed) {
	short_dump_state();
    }
}

static void
start_a_vault_wtaper(
    wtaper_t    *wtaper,
    gboolean    *state_changed)
{
    sched_t *sp = NULL;
    disk_t *dp = NULL;
    char *qname;
    TapeAction result_tape_action;
    char *why_no_new_tape = NULL;
    taper_t  *taper = wtaper->taper;
    wtaper_t *wtaper1;

    result_tape_action = tape_action(wtaper, &why_no_new_tape, FALSE);

    if (result_tape_action & TAPE_ACTION_SCAN) {
	wtaper->state &= ~TAPER_STATE_TAPE_REQUESTED;
	wtaper->state |= TAPER_STATE_WAIT_FOR_TAPE;
	wtaper->taper->nb_scan_volume++;
	taper_cmd(taper, wtaper, START_SCAN, wtaper->job->sched, NULL, 0, NULL);
    } else if (result_tape_action & TAPE_ACTION_NEW_TAPE) {
	wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	wtaper->state |= TAPER_STATE_WAIT_NEW_TAPE;
	nb_sent_new_tape++;
	taper_cmd(taper, wtaper, NEW_TAPE, wtaper->job->sched, NULL, 0, NULL);
    } else if (result_tape_action & TAPE_ACTION_NO_NEW_TAPE) {
	wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	taper_cmd(taper, wtaper, NO_NEW_TAPE, wtaper->job->sched, why_no_new_tape, 0, NULL);
	wtaper->state |= TAPER_STATE_DONE;
	taper->degraded_mode = TRUE;
	start_degraded_mode(&runq);
	*state_changed = TRUE;
    } else if (result_tape_action & TAPE_ACTION_MOVE) {
	wtaper1 = idle_taper(taper);
	if (wtaper1) {
	    wtaper->state &= ~TAPER_STATE_TAPE_REQUESTED;
	    wtaper->state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	    taper_cmd(taper, wtaper, TAKE_SCRIBE_FROM, wtaper->job->sched, wtaper1->name,
		      0 , NULL);
	    wtaper1->state = TAPER_STATE_DEFAULT;
	    wtaper->state |= TAPER_STATE_TAPE_STARTED;
	    wtaper->left = wtaper1->left;
	    wtaper->nb_dle = wtaper1->nb_dle+1;
	    if (taper->last_started_wtaper == wtaper) {
		taper->last_started_wtaper = NULL;
	    } else if (taper->last_started_wtaper == wtaper1) {
		taper->last_started_wtaper = wtaper;
	    }
	    if (taper->sent_first_write == wtaper) {
		taper->sent_first_write = NULL;
	    } else if (taper->sent_first_write == wtaper1) {
		taper->sent_first_write = wtaper;
	    }
	    *state_changed = TRUE;
	}
    }

    if (!taper->degraded_mode &&
        wtaper->state & TAPER_STATE_IDLE &&
	(!empty(wtaper->vaultqs.vaultq) ||
	 wtaper->taper->vaultqss) &&
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

	if (!empty(wtaper->vaultqs.vaultq)) {
	    sp = dequeue_sched(&wtaper->vaultqs.vaultq);
	} else if (wtaper->taper->vaultqss) {
	    vaultqs_t *vaultqs;
	    /* JLM must find a vaultqs where each vaultqs->src_labels are closed */
	    vaultqs = (vaultqs_t *)wtaper->taper->vaultqss->data;
	    amfree(wtaper->vaultqs.src_labels_str);
	    slist_free_full(wtaper->vaultqs.src_labels, g_free);
	    wtaper->vaultqs.src_labels = NULL;
	    wtaper->vaultqs = *vaultqs;
	    wtaper->taper->vaultqss = g_slist_remove_link(wtaper->taper->vaultqss, wtaper->taper->vaultqss);
	    sp = dequeue_sched(&wtaper->vaultqs.vaultq);
	}

	if (sp) {
	    job_t *job = alloc_job();

	    job->wtaper = wtaper;
	    job->sched = sp;
	    dp = sp->disk;
	    wtaper->job = job;

	    amfree(wtaper->input_error);
	    amfree(wtaper->tape_error);
	    wtaper->result = LAST_TOK;
	    wtaper->sendresult = FALSE;
	    amfree(wtaper->first_label);
	    amfree(wtaper->dst_labels_str);
	    slist_free_full(wtaper->dst_labels, g_free);
	    wtaper->dst_labels = NULL;
	    wtaper->written = 0;
	    wtaper->state &= ~TAPER_STATE_IDLE;
	    wtaper->state |= TAPER_STATE_VAULT_TO_TAPE;
	    qname = quote_string(dp->name);
	    if (taper->nb_wait_reply == 0) {
		taper->ev_read = event_create(taper->fd, EV_READFD,
					      handle_taper_result, taper);
		event_activate(taper->ev_read);
	    }
	    taper->nb_wait_reply++;
	    wtaper->nb_dle++;
	    taper_cmd(taper, wtaper, VAULT_WRITE, sp, sp->destname,
		      sp->level,
		      sp->datestamp);
	    g_fprintf(stderr,
		      _("driver: start_a_vault: %s %s %s %lld %lld\n"),
		      taperalgo2str(taperalgo), dp->host->hostname, qname,
		      (long long)sp->act_size,
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

    if (dp->host->inprogress >= dp->host->maxdumps) {
	return 1;
    }

    /* next, check conflict with other dumps on same spindle */

    if (dp->spindle == -1) {	/* but spindle -1 never conflicts by def. */
	return 0;
    }

    for (dp2 = dp->host->disks; dp2 != NULL; dp2 = dp2->hostnext)
	if (dp2->inprogress && dp2->spindle == dp->spindle) {
	    return 1;
	}

    return 0;
}

static void
allow_dump_dle(
    sched_t        *sp,
    wtaper_t       *wtaper,
    char            dumptype,
    schedlist_t    *rq,
    const time_t    now,
    int             dumper_to_holding,
    int            *cur_idle,
    sched_t       **delayed_sp,
    sched_t       **sp_accept,
    assignedhd_t ***holdp_accept,
    off_t           extra_tapes_size)
{
    assignedhd_t **holdp=NULL;
    disk_t        *diskp = sp->disk;

    /* if the dump can go to that storage */
    if (wtaper) {
	if (wtaper->taper->degraded_mode) {
	    return;
	}
	if (!dump_match_selection(wtaper->taper->storage_name, sp)) {
	    return;
	}
    }

    if (diskp->host->start_t > now) {
	*cur_idle = max(*cur_idle, IDLE_START_WAIT);
	if (*delayed_sp == NULL || sleep_time > diskp->host->start_t) {
	    *delayed_sp = sp;
	    sleep_time = diskp->host->start_t;
	}
    } else if(diskp->start_t > now) {
	*cur_idle = max(*cur_idle, IDLE_START_WAIT);
	if (*delayed_sp == NULL || sleep_time > diskp->start_t) {
	    *delayed_sp = sp;
	    sleep_time = diskp->start_t;
	}
    } else if (diskp->host->netif->curusage > 0 &&
	       sp->est_kps > network_free_kps(diskp->host->netif)) {
	*cur_idle = max(*cur_idle, IDLE_NO_BANDWIDTH);
    } else if (!wtaper && sp->no_space) {
	*cur_idle = max(*cur_idle, IDLE_NO_DISKSPACE);
    } else if (!wtaper && diskp->to_holdingdisk == HOLD_NEVER) {
	*cur_idle = max(*cur_idle, IDLE_NO_HOLD);
    } else if (extra_tapes_size && sp->est_size > extra_tapes_size) {
	*cur_idle = max(*cur_idle, IDLE_NO_DISKSPACE);
	/* no tape space */
    } else if (!wtaper && (holdp =
	find_diskspace(sp->est_size, cur_idle, NULL)) == NULL) {
	*cur_idle = max(*cur_idle, IDLE_NO_DISKSPACE);
	if (all_tapeq_empty() && dumper_to_holding == 0 && rq != &directq && no_taper_flushing()) {
	    char *qname = quote_string(diskp->name);
	    remove_sched(rq, sp);
	    if (diskp->to_holdingdisk == HOLD_REQUIRED) {
		log_add(L_FAIL, "%s %s %s %d [%s]",
			diskp->host->hostname, qname, sp->datestamp,
			sp->level,
			_("can't dump required holdingdisk when no holdingdisk space available "));
	    } else {
		char *wall_time = walltime_str(curclock());
		sp->action = ACTION_DUMP_TO_TAPE;
		g_printf("driver: requeue dump_to_tape time %s %s %s %s\n", wall_time, diskp->host->hostname, qname, sp->datestamp);
		enqueue_sched(&directq, sp);
		diskp->to_holdingdisk = HOLD_NEVER;
	    }
	    amfree(qname);
	    if (empty(*rq) && active_dumper() == 0) { force_flush = 1;}
	}
    } else if (client_constrained(diskp)) {
	free_assignedhd(holdp);
	*cur_idle = max(*cur_idle, IDLE_CLIENT_CONSTRAINED);
    } else {

	/* disk fits, dump it */
	int accept = !*sp_accept;
	if(!accept) {
	    switch(dumptype) {
	      case 's': accept = (sp->est_size < (*sp_accept)->est_size);
			break;
	      case 'S': accept = (sp->est_size > (*sp_accept)->est_size);
			break;
	      case 't': accept = (sp->est_time < (*sp_accept)->est_time);
			break;
	      case 'T': accept = (sp->est_time > (*sp_accept)->est_time);
			break;
	      case 'b': accept = (sp->est_kps < (*sp_accept)->est_kps);
			break;
	      case 'B': accept = (sp->est_kps > (*sp_accept)->est_kps);
			break;
	      default:  log_add(L_WARNING, _("Unknown dumporder character \'%c\', using 's'.\n"),
				dumptype);
			accept = (sp->est_size < (*sp_accept)->est_size);
			break;
	    }
	}
	if(accept) {
	    if (!*sp_accept || (wtaper && !wtaper->taper->degraded_mode) ||
		 diskp->priority >= (*sp_accept)->disk->priority) {
		if(*holdp_accept) free_assignedhd(*holdp_accept);
		*sp_accept = sp;
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
    schedlist_t *rq)
{
    const time_t now = time(NULL);
    int cur_idle;
    GList  *slist, *slist_next;
    sched_t *sp, *delayed_sp, *sp_accept;
    assignedhd_t **holdp=NULL, **holdp_accept;
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
    if (!taper_started && conf_reserve > 0) return;

    idle_reason = IDLE_NO_DUMPERS;
    sleep_time = 0;

    if(dumpers_ev_time != NULL) {
	event_release(dumpers_ev_time);
	dumpers_ev_time = NULL;
    }

    for(dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if (dumper->busy && dumper->job &&
	    dumper->job->sched->disk->to_holdingdisk != HOLD_NEVER) {
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

	sp_accept = NULL;
	holdp_accept = NULL;
	delayed_sp = NULL;

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

	sp = NULL;
	//diskp = NULL;
	wtaper = NULL;
	directq_is_empty = empty(directq);
	if (!empty(directq)) {  /* to the first allowed storage only */
	    for (taper = tapetable; taper < tapetable+nb_storage && !sp_accept; taper++) {
		if (!taper->storage_name || !taper->flush_storage ||
		    taper->degraded_mode || taper->down) {
		    continue;
		}
		sp_accept = NULL;
		wtaper_accept = idle_taper(taper);
		if (wtaper_accept) {
		    TapeAction result_tape_action;
		    char *why_no_new_tape = NULL;

		    result_tape_action = tape_action(wtaper_accept,
						     &why_no_new_tape, TRUE);
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
				    sp = wtaper1->job->sched;
				    if (sp) {
					extra_tapes_size -=
							 (sp->est_size -
						         wtaper1->written);
				    }
				}
			    }
			}

			for (slist = directq.head; slist != NULL;
						   slist = slist_next) {
			    slist_next = slist->next;
			    sp = get_sched(slist);
		            allow_dump_dle(sp, wtaper_accept, dumptype, &directq, now,
					   dumper_to_holding, &cur_idle,
					   &delayed_sp, &sp_accept,
					   &holdp_accept, extra_tapes_size);
			}
			if (sp_accept) {
			    sp = sp_accept;
			    wtaper = wtaper_accept;
			} else {
			    sp = NULL;
			    wtaper = NULL;
			}
		    }
		}
	    }
	}
	if (sp == NULL) {
	    for (slist = rq->head; slist != NULL;
				   slist = slist_next) {
		slist_next = slist->next;
		sp = get_sched(slist);
		assert(sp != NULL && sp->disk != NULL && sp->disk->host);

		allow_dump_dle(sp, NULL, dumptype, rq, now,
			       dumper_to_holding, &cur_idle, &delayed_sp,
			       &sp_accept, &holdp_accept, 0);
	    }
	    sp = sp_accept;
	    holdp = holdp_accept;
	}

	/* Redo with same dumper if a diskp was moved to directq */
	if (sp == NULL && directq_is_empty && !empty(directq)) {
	    dumper--;
	    continue;
	}

	idle_reason = max(idle_reason, cur_idle);
	if (sp == NULL && idle_reason == IDLE_NO_DISKSPACE) {
	    /* continue flush waiting for new tape */
	    start_a_flush();
	    start_a_vault();
	}

	/*
	 * If we have no disk at this point, and there are disks that
	 * are delayed, then schedule a time event to call this dumper
	 * with the disk with the shortest delay.
	 */
	if (sp == NULL && delayed_sp != NULL) {
	    assert(sleep_time > now);
	    sleep_time -= now;
	    dumpers_ev_time = event_create((event_id_t)sleep_time, EV_TIME,
		handle_dumpers_time, &runq);
	    event_activate(dumpers_ev_time);
	    return;
	} else if (sp != NULL && wtaper == NULL) {
	    job_t *job = alloc_job();

	    sp->act_size = (off_t)0;
	    allocate_bandwidth(sp->disk->host->netif, sp->est_kps);
	    sp->activehd = assign_holdingdisk(holdp, sp);
	    amfree(holdp);
	    g_free(sp->destname);
	    sp->destname = g_strdup(sp->holdp[0]->destname);
	    sp->disk->host->inprogress++;	/* host is now busy */
	    sp->disk->inprogress = 1;
	    job->sched = sp;
	    job->dumper = dumper;
	    dumper->job = job;
	    sp->timestamp = now;
	    g_free(sp->try_again_message);
	    amfree(sp->disk->dataport_list);

	    dumper->busy = 1;		/* dumper is now busy */
	    remove_sched(rq, sp);		/* take it off the run queue */

	    sp->origsize = (off_t)-1;
	    sp->dumpsize = (off_t)-1;
	    sp->dumptime = (time_t)0;
	    chunker = &chktable[dumper - dmptable];
	    job->chunker = chunker;
	    chunker->job = job;
	    chunker->result = LAST_TOK;
	    chunker->sendresult = FALSE;
	    dumper->result = LAST_TOK;
	    dumper->sent_command = FALSE;
	    dumper->sent_result = 0;
	    dumper->dump_finish = 0;
	    startup_chunk_process(chunker,chunker_program);
	    chunker_cmd(chunker, START, NULL, driver_timestamp);
	    if (sp->disk->compress == COMP_SERVER_FAST ||
		sp->disk->compress == COMP_SERVER_BEST ||
		sp->disk->compress == COMP_SERVER_CUST ||
		sp->disk->encrypt == ENCRYPT_SERV_CUST) {
		chunker_cmd(chunker, PORT_WRITE, sp, sp->datestamp);
		job->do_port_write = TRUE;
	    } else {
		chunker_cmd(chunker, SHM_WRITE, sp, sp->datestamp);
		job->do_port_write = FALSE;
	    }
	    chunker->ev_read = event_create((event_id_t)chunker->fd,
					    EV_READFD,
					    handle_chunker_result, chunker);
	    event_activate(chunker->ev_read);
	    sp->disk->host->start_t = now + HOST_DELAY;
	    if (empty(*rq) && active_dumper() == 0) { force_flush = 1;}

	    short_dump_state();
	} else if (sp != NULL && wtaper != NULL) { /* dump to tape */
	    job_t *job = alloc_job();

	    sp->act_size = (off_t)0;
	    allocate_bandwidth(sp->disk->host->netif, sp->est_kps);
	    sp->disk->host->inprogress++;	/* host is now busy */
	    sp->disk->inprogress = 1;
	    job->sched = sp;
	    job->dumper = dumper;
	    job->wtaper = wtaper;
	    wtaper->job = job;
	    dumper->job = job;

	    sp->timestamp = now;
	    g_free(sp->try_again_message);
	    amfree(sp->disk->dataport_list);

	    dumper->busy = 1;		/* dumper is now busy */
	    remove_sched(&directq, sp);  /* take it off the direct queue */

	    sp->origsize = (off_t)-1;
	    sp->dumpsize = (off_t)-1;
	    sp->dumptime = (time_t)0;
	    dumper->result = LAST_TOK;
	    dumper->sent_command = FALSE;
	    dumper->sent_result = 0;
	    dumper->dump_finish = 0;
	    wtaper->result = LAST_TOK;
	    amfree(wtaper->input_error);
	    amfree(wtaper->tape_error);
	    amfree(wtaper->first_label);
	    amfree(wtaper->dst_labels_str);
	    slist_free_full(wtaper->dst_labels, g_free);
	    wtaper->dst_labels = NULL;
	    wtaper->written = 0;
	    wtaper->state |= TAPER_STATE_DUMP_TO_TAPE;
	    wtaper->state &= ~TAPER_STATE_IDLE;
	    wtaper->nb_dle++;
	    taper = wtaper->taper;
	    if (!(wtaper->state & TAPER_STATE_TAPE_STARTED)) {
		assert(taper->sent_first_write == NULL);
		taper->sent_first_write = wtaper;
		wtaper->nb_dle = 1;
		wtaper->left = taper->tape_length;
	    }
	    if (taper->nb_wait_reply == 0) {
		taper->ev_read = event_create(taper->fd, EV_READFD,
					      handle_taper_result, taper);
	    event_activate(taper->ev_read);
	    }

	    taper->nb_wait_reply++;
	    if (sp->disk->data_path == DATA_PATH_DIRECTTCP ||
		sp->disk->compress == COMP_SERVER_FAST ||
		sp->disk->compress == COMP_SERVER_BEST ||
		sp->disk->compress == COMP_SERVER_CUST ||
		sp->disk->encrypt == ENCRYPT_SERV_CUST) {
		taper_cmd(taper, wtaper, PORT_WRITE, sp, NULL, sp->level,
			  sp->datestamp);
		job->do_port_write = TRUE;
	    } else {
		taper_cmd(taper, wtaper, SHM_WRITE, sp, NULL, sp->level,
			  sp->datestamp);
		job->do_port_write = FALSE;
	    }

	    wtaper->ready = FALSE;
	    sp->disk->host->start_t = now + HOST_DELAY;

	    state_changed = TRUE;
	}
    }
    if (state_changed) {
	short_dump_state();
    }
}


/* try to find another vault from same source volume with smallest fileno */
static void
start_vault_on_same_wtaper(
    wtaper_t *wtaper)
{

    sched_t *sp = dequeue_sched(&wtaper->vaultqs.vaultq);
    if (sp) {
	job_t    *job   = alloc_job();
	disk_t   *dp    = sp->disk;
	taper_t  *taper = wtaper->taper;
	char     *qname;

	job->wtaper = wtaper;
	job->sched = sp;
	wtaper->job = job;

	amfree(wtaper->input_error);
	amfree(wtaper->tape_error);
	wtaper->result = LAST_TOK;
	wtaper->sendresult = FALSE;
	amfree(wtaper->first_label);
	amfree(wtaper->dst_labels_str);
	slist_free_full(wtaper->dst_labels, g_free);
	wtaper->dst_labels = NULL;
	wtaper->written = 0;
	wtaper->state &= ~TAPER_STATE_IDLE;
	wtaper->state |= TAPER_STATE_VAULT_TO_TAPE;
	qname = quote_string(dp->name);
	if (taper->nb_wait_reply == 0) {
	    taper->ev_read = event_create(taper->fd, EV_READFD,
					  handle_taper_result, taper);
	    event_activate(taper->ev_read);
	}
	taper->nb_wait_reply++;
	wtaper->nb_dle++;
	taper_cmd(taper, wtaper, VAULT_WRITE, sp, sp->destname,
		  sp->level, sp->datestamp);
	g_fprintf(stderr,
		 _("driver: start_a_vault: %s %s %s %lld %lld\n"),
		 "samewtape", dp->host->hostname, qname,
		 (long long)sp->act_size,
		 (long long)wtaper->left);
	amfree(qname);
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
    schedlist_t *runq = cookie;
    event_release(dumpers_ev_time);
    dumpers_ev_time = NULL;
    start_some_dumps(runq);
}

static void
dump_schedule(
    schedlist_t *qp,
    char *	str)
{
    GList   *slist;
    sched_t *sp;
    disk_t  *dp;
    char    *qname;

    g_printf(_("dump of driver schedule %s:\n--------\n"), str);

    for(slist = qp->head; slist != NULL; slist = slist->next) {
	sp = slist->data;
	dp = sp->disk;
        qname = quote_string(dp->name);
	g_printf("  %-20s %-25s lv %d t %5lu s %lld p %d\n",
	       dp->host->hostname, qname, sp->level,
	       sp->est_time,
	       (long long)sp->est_size, sp->priority);
        amfree(qname);
    }
    g_printf("--------\n");
}

static void
start_degraded_mode(
    /*@keep@*/ schedlist_t *queuep)
{
    schedlist_t newq;
    off_t est_full_size;
    char *qname;
    taper_t  *taper;
    static gboolean schedule_degraded = FALSE;

    if (!schedule_done || all_degraded_mode) {
	return;
    }

    if (!schedule_degraded) {
	gboolean one_degraded_mode = (nb_storage == 0);
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
	sched_t *sp = dequeue_sched(queuep);
	disk_t  *dp = sp->disk;

	qname = quote_string(dp->name);
	if (sp->level != 0) {
	    /* go ahead and do the disk as-is */
	    enqueue_sched(&newq, sp);
	} else {
	    gboolean must_degrade_dp = (nb_storage == 0);
	    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		if (taper->degraded_mode &&
		    dump_match_selection(taper->storage_name, sp)) {
		    must_degrade_dp = TRUE;
		}
	    }
	    if (!must_degrade_dp) {
		/* go ahead and do the disk as-is */
		enqueue_sched(&newq, sp);
	    } else if (reserved_space + est_full_size + sp->est_size
		<= total_disksize) {
		enqueue_sched(&newq, sp);
		est_full_size += sp->est_size;
	    }
	    else if(sp->degr_level != -1) {
		sp->level = sp->degr_level;
		sp->dumpdate = g_strdup(sp->degr_dumpdate);
		sp->est_nsize = sp->degr_nsize;
		sp->est_csize = sp->degr_csize;
		sp->est_time = sp->degr_time;
		sp->est_kps  = sp->degr_kps;
		enqueue_sched(&newq, sp);
	    }
	    else {
		log_add(L_FAIL, "%s %s %s %d [%s]",
		        dp->host->hostname, qname, sp->datestamp,
			sp->level, sp->degr_mesg);
	    }
	}
        amfree(qname);
    }

    /*@i@*/ *queuep = newq;
    all_degraded_mode = (nb_storage == 0);
    for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
	all_degraded_mode &= taper->degraded_mode;
    }

    dump_schedule(queuep, _("after start degraded mode"));
}


static void
continue_port_dumps(void)
{
    GList   *slist, *slist_next;
    sched_t *sp;
    job_t   *job = NULL;
    assignedhd_t **h;
    int active_dumpers=0, busy_dumpers=0, i;
    dumper_t *dumper;

    /* First we try to grant diskspace to some dumps waiting for it. */
    for (slist = roomq.head; slist != NULL; slist = slist_next) {
	slist_next = slist->next;
	sp = get_sched(slist);
	/* find last holdingdisk used by this dump */
	for (i = 0, h = sp->holdp; h[i+1]; i++) {
	    (void)h; /* Quiet lint */
	}
	/* find more space */
	h = find_diskspace(sp->est_size - sp->act_size,
			   &active_dumpers, h[i]);
	if( h ) {
	    for (dumper = dmptable; dumper < dmptable + inparallel &&
				    (!dumper->job || dumper->job->sched != sp);
				    dumper++) {
		(void)sp; /* Quiet lint */
	    }
	    assert(dumper < dmptable + inparallel);
	    assert(dumper->job);
	    sp->activehd = assign_holdingdisk(h, sp);
	    chunker_cmd(dumper->job->chunker, CONTINUE, sp, NULL);
	    amfree(h);
	    remove_sched(&roomq, sp);
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
    for (sp = NULL, dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if (dumper->busy) {
	    busy_dumpers++;
	    if (!find_sched(&roomq, dumper->job->sched)) {
		if (dumper->job->chunker) {
		    active_dumpers++;
		}
	    } else if (!sp ||
		       sp->est_size > dumper->job->sched->est_size) {
		sp = dumper->job->sched;
		job = dumper->job;
	    }
	}
    }

    if ((sp != NULL) && (active_dumpers == 0) && (busy_dumpers > 0) &&
        ((no_taper_flushing() && all_tapeq_empty()) || all_degraded_mode) &&
	pending_aborts == 0 ) { /* case b */
	sp->no_space = 1;
	/* At this time, dp points to the dump with the smallest est_size.
	 * We abort that dump, hopefully not wasting too much time retrying it.
	 */
	remove_sched( &roomq, sp );
	chunker_cmd(job->chunker, ABORT, NULL, _("Not enough holding disk space"));
	dumper_cmd(job->dumper, ABORT, NULL, _("Not enough holding disk space"));
	job->dumper->sent_result = 1;
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
	void *cookie)
{
    sched_t *sp = NULL;
    disk_t  *dp = NULL;
    job_t   *job = NULL;
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
	sp = NULL;
	dp = NULL;
	job = NULL;
	dumper =NULL;
	wtaper = NULL;
	wtaper1 = NULL;

	cmd = getresult(taper->fd, 1, &result_argc, &result_argv);

	switch(cmd) {

	case TAPER_OK:
	    if(result_argc != 3) {
		error(_("error: [taper FAILED result_argc != 3: %d"), result_argc);
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
	    if (g_str_equal(result_argv[2], "ALLOW-TAKE-SCRIBE-FROM")) {
		wtaper->allow_take_scribe_from = TRUE;
	    } else {
		wtaper->allow_take_scribe_from = FALSE;
	    }
	    amfree(wtaper->input_error);
	    amfree(wtaper->tape_error);
	    amfree(wtaper->first_label);
	    amfree(wtaper->dst_labels_str);
	    slist_free_full(wtaper->dst_labels, g_free);
	    wtaper->dst_labels = NULL;
	    taper->nb_wait_reply--;
	    taper->nb_scan_volume--;
	    taper->last_started_wtaper = wtaper;
	    if (taper->nb_wait_reply == 0) {
		event_release(taper->ev_read);
		taper->ev_read = NULL;
	    }
	    start_some_dumps(&runq);
	    start_a_flush();
	    start_a_vault();
	    break;

	case FAILED:	/* FAILED <worker> <handle> INPUT-* TAPE-* <input err mesg> <tape err mesg> */
	    if(result_argc != 7) {
		error(_("error: [taper FAILED result_argc != 7: %d"), result_argc);
		/*NOTREACHED*/
	    }

	    job = serial2job(result_argv[2]);
	    sp = job->sched;
	    dp = sp->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    qname = quote_string(dp->name);
	    g_printf(_("driver: finished-cmd time %s taper %s worker %s wrote %s:%s\n"),
		   walltime_str(curclock()), taper->name, wtaper->name, dp->host->hostname, qname);
	    fflush(stdout);

	    if (taper->sent_first_write == wtaper) {
		taper->sent_first_write = NULL;
	    }

	    wtaper->nb_dle--;
	    wtaper->result = cmd;
	    if (job->dumper && !dp->dataport_list && !dp->shm_name) {
		job->dumper->result = FAILED;
	    }
	    if (g_str_equal(result_argv[3], "INPUT-ERROR")) {
		g_free(wtaper->input_error);
		wtaper->input_error = g_strdup(result_argv[5]);
	    } else if (!g_str_equal(result_argv[3], "INPUT-GOOD")) {
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(_("Taper protocol error"));
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sp->datestamp,
		        sp->level, wtaper->tape_error);
	    } else if (g_str_equal(result_argv[4], "TAPE-ERROR") ||
		g_str_equal(result_argv[4], "TAPE-CONFIG")) {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(result_argv[6]);
	    } else if (!g_str_equal(result_argv[4], "TAPE-GOOD")) {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		g_free(wtaper->tape_error);
		wtaper->tape_error = g_strdup(_("Taper protocol error"));
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sp->datestamp,
		        sp->level, wtaper->tape_error);
	    }

	    if (taper->last_started_wtaper == wtaper) {
		taper->last_started_wtaper = NULL;
	    }
	    if (taper->sent_first_write == wtaper) {
		taper->sent_first_write = NULL;
	    }
	    amfree(qname);

	    break;

	case READY:	/* READY <worker> <handle> */
	    job = serial2job(result_argv[2]);
	    sp = job->sched;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    wtaper->ready = TRUE;
	    if (wtaper->job->dumper &&
		wtaper->job->dumper->result != LAST_TOK &&
		wtaper->sendresult) {
		if( wtaper->job->dumper->result == DONE) {
		    taper_cmd(taper, wtaper, DONE, sp, NULL, 0, NULL);
		} else {
		    taper_cmd(taper, wtaper, FAILED, sp, NULL, 0, NULL);
		}
		wtaper->sendresult = FALSE;
	    }
	    break;

	case PARTIAL:	/* PARTIAL <worker> <handle> INPUT-* TAPE-* server-crc <stat mess> <input err mesg> <tape err mesg>*/
	case DONE:	/* DONE <worker> <handle> INPUT-GOOD TAPE-GOOD server-crc <stat mess> <input err mesg> <tape err mesg> */
	    if(result_argc != 9) {
		error(_("error: [taper PARTIAL result_argc != 9: %d"), result_argc);
		/*NOTREACHED*/
	    }

	    job = serial2job(result_argv[2]);
	    sp = job->sched;
	    dp = sp->disk;
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
		        dp->host->hostname, qname, sp->datestamp,
		        sp->level, wtaper->tape_error);
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
		        dp->host->hostname, qname, sp->datestamp,
		        sp->level, wtaper->tape_error);
		amfree(qname);
		break;
	    }

	    parse_crc(result_argv[5], &sp->server_crc);

	    s = strstr(result_argv[6], " kb ");
	    if (s) {
		s += 4;
		sp->dumpsize = OFF_T_ATOI(s);
	    } else {
		s = strstr(result_argv[6], " bytes ");
		if (s) {
		    s += 7;
		    sp->dumpsize = OFF_T_ATOI(s)/1024;
		}
	    }

	    wtaper->result = cmd;
	    amfree(qname);

	    break;

        case PARTDONE:  /* PARTDONE <worker> <handle> <label> <fileno> <kbytes> <stat> */
	  {
	    char *label = result_argv[3];
	    job = serial2job(result_argv[2]);
	    sp = job->sched;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

            if (result_argc != 7) {
                error(_("error [taper PARTDONE result_argc != 7: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }

	    if (!wtaper->first_label) {
		amfree(wtaper->first_label);
		wtaper->first_label = g_strdup(label);
		wtaper->first_fileno = OFF_T_ATOI(result_argv[4]);
	    }

	    /* Add the label to dst_labels */
	    if (!wtaper->dst_labels || strcmp((char *)wtaper->dst_labels->data, label) != 0) {
		char *s;
		if (!wtaper->dst_labels_str) {
		    wtaper->dst_labels_str = g_strdup(" ;");
		}
		s = g_strconcat(wtaper->dst_labels_str, label, " ;", NULL);
		g_free(wtaper->dst_labels_str);
		wtaper->dst_labels_str = s;
		wtaper->dst_labels = g_slist_append(wtaper->dst_labels, g_strdup(label));
	    }

	    wtaper->written += OFF_T_ATOI(result_argv[5]);
	    if (wtaper->written > sp->act_size)
		sp->act_size = wtaper->written;

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
	  }
            break;

        case REQUEST_NEW_TAPE:  /* REQUEST-NEW-TAPE <worker> <handle> */
            if (result_argc != 3) {
                error(_("error [taper REQUEST_NEW_TAPE result_argc != 3: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }

	    job = serial2job(result_argv[2]);
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    wtaper->left = 0;
	    if (wtaper->state & TAPER_STATE_DONE) {
		taper_cmd(taper, wtaper, NO_NEW_TAPE, job->sched, "taper found no tape", 0, NULL);
	    } else {
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
		wtaper->state |= TAPER_STATE_TAPE_REQUESTED;

		start_some_dumps(&runq);
		start_a_flush();
		start_a_vault();
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
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

            /* Update our tape counter and reset taper->left */
	    taper->current_tape++;
	    wtaper->nb_dle = 1;
	    wtaper->left = taper->tape_length;
	    wtaper->state &= ~TAPER_STATE_WAIT_NEW_TAPE;
	    wtaper->state |= TAPER_STATE_TAPE_STARTED;
	    if (taper->last_started_wtaper == wtaper) {
		taper->last_started_wtaper = NULL;
	    }
	    if (taper->sent_first_write == wtaper) {
		taper->sent_first_write = NULL;
	    }

	    /* start a new worker */
	    for (i = 0; i < taper->nb_worker ; i++) {
		wtaper1 = &taper->wtapetable[i];
		if (!taper->down &&
		    !taper->degraded_mode &&
		    wtaper1->state == TAPER_STATE_DEFAULT &&
		    tape_action(wtaper1, NULL, FALSE) == TAPE_ACTION_START_TAPER) {
		    wtaper1->state = TAPER_STATE_INIT;
		    if (taper->nb_wait_reply == 0) {
			taper->ev_read = event_create(taper->fd, EV_READFD,
						handle_taper_result, NULL);
			event_activate(taper->ev_read);
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
	    taper->nb_scan_volume--;

	    job = serial2job(result_argv[2]);
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    wtaper->state |= TAPER_STATE_DONE;
	    if (taper->last_started_wtaper == wtaper) {
		taper->last_started_wtaper = NULL;
	    }
	    if (taper->sent_first_write == wtaper) {
		taper->sent_first_write = NULL;
	    }
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
	    sp = job->sched;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    if (wtaper->job->dumper->result == LAST_TOK) {
		wtaper->sendresult = TRUE;
	    } else {
		if( wtaper->job->dumper->result == DONE) {
		    taper_cmd(taper, wtaper, DONE, sp, NULL, 0, NULL);
		} else {
		    taper_cmd(taper, wtaper, FAILED, sp, NULL, 0, NULL);
		}
	    }
	    break;

        case TAPE_ERROR: /* TAPE-ERROR <worker> <err mess> */
	    taper_started = 1;
	    taper->down =1;
	    if (g_str_equal(result_argv[1], "SETUP")) {
		taper->nb_wait_reply = 0;
		taper->nb_scan_volume = 0;
		taper->fd = -1;
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
	case SHM_NAME: /* SHM-NAME <worker> <handle> <port> <shm_name> */
	    job = serial2job(result_argv[2]);
	    sp = job->sched;
	    dp = sp->disk;
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    assert(wtaper == job->wtaper);

	    dumper = job->dumper;
	    dumper->output_port = atoi(result_argv[3]);
	    amfree(dp->dataport_list);
	    amfree(dp->shm_name);
	    if (cmd == PORT) {
		dp->dataport_list = g_strdup(result_argv[4]);
	    } else {
		dp->shm_name = g_strdup(result_argv[4]);
	    }

	    amfree(wtaper->input_error);
	    amfree(wtaper->tape_error);
	    amfree(wtaper->first_label);
	    amfree(wtaper->dst_labels_str);
	    slist_free_full(wtaper->dst_labels, g_free);
	    wtaper->dst_labels = NULL;
	    wtaper->written = 0;
	    wtaper->state |= TAPER_STATE_DUMP_TO_TAPE;

	    if (dp->host->pre_script == 0) {
		run_server_host_scripts(EXECUTE_ON_PRE_HOST_BACKUP,
					get_config_name(), driver_timestamp,
					dp->host);
		dp->host->pre_script = 1;
	    }
	    run_server_dle_scripts(EXECUTE_ON_PRE_DLE_BACKUP,
			       get_config_name(), driver_timestamp, dp,
			       sp->level, BOGUS);
	    /* tell the dumper to dump to a port */
	    if (cmd == PORT) {
		dumper_cmd(dumper, PORT_DUMP, sp, NULL);
	    } else {
		dumper_cmd(dumper, SHM_DUMP, sp, NULL);
	    }
	    dumper->sent_command = TRUE;
	    dp->host->start_t = time(NULL) + HOST_DELAY;

	    wtaper->state |= TAPER_STATE_DUMP_TO_TAPE;

	    dumper->ev_read = event_create(dumper->fd, EV_READFD,
					   handle_dumper_result, dumper);
	    event_activate(dumper->ev_read);
	    break;

        case CLOSED_VOLUME:
	    g_debug("got CLOSED_VOLUME message");
	    wtaper = wtaper_from_name(taper, result_argv[1]);

	    if (wtaper->state & TAPER_STATE_WAIT_CLOSED_VOLUME) {
		wtaper->state &= ~TAPER_STATE_WAIT_CLOSED_VOLUME;
		taper->nb_wait_reply--;
	    }

	    wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
	    wtaper->state &= ~TAPER_STATE_RESERVATION;
	    if (!(wtaper->state & TAPER_STATE_FILE_TO_TAPE) &&
		!(wtaper->state & TAPER_STATE_DUMP_TO_TAPE) &&
		!(wtaper->state & TAPER_STATE_VAULT_TO_TAPE)) {
		wtaper->state |= TAPER_STATE_IDLE;
		if (wtaper->job) {
		    free_serial_job(wtaper->job);
		    free_job(wtaper->job);
		    wtaper->job = NULL;
		    start_vault_on_same_wtaper(wtaper);
		}
	    }

	    if (taper->nb_wait_reply == 0) {
		event_release(taper->ev_read);
		taper->ev_read = NULL;
	    }

	    continue_port_dumps();
	    start_some_dumps(&runq);
	    start_a_flush();
	    start_a_vault();
	    break;

        case OPENED_SOURCE_VOLUME: /* worker_name label */
	    g_debug("got OPENED_SOURCE_VOLUME message");
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    amfree(wtaper->current_source_label);
	    wtaper->current_source_label = g_strdup(result_argv[2]);
	    break;

        case CLOSED_SOURCE_VOLUME: /* worker_name */
	    g_debug("got CLOSED_SOURCE_VOLUME message");
	    wtaper = wtaper_from_name(taper, result_argv[1]);
	    amfree(wtaper->current_source_label);

	    if (wtaper->state & TAPER_STATE_WAIT_CLOSED_SOURCE_VOLUME) {
		taper->nb_wait_reply--;
		wtaper->state &= ~TAPER_STATE_WAIT_CLOSED_SOURCE_VOLUME;
	    }

	    if (!(wtaper->state & TAPER_STATE_FILE_TO_TAPE) &&
		!(wtaper->state & TAPER_STATE_DUMP_TO_TAPE) &&
		!(wtaper->state & TAPER_STATE_VAULT_TO_TAPE)) {
		wtaper->state |= TAPER_STATE_IDLE;
		if (wtaper->job) {
		    free_serial_job(wtaper->job);
		    free_job(wtaper->job);
		    wtaper->job = NULL;
		    start_vault_on_same_wtaper(wtaper);
		}
	    }
	    if (taper->nb_wait_reply == 0) {
		event_release(taper->ev_read);
		taper->ev_read = NULL;
	    }
	    continue_port_dumps();
	    start_some_dumps(&runq);
	    start_a_flush();
	    start_a_vault();
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

	    taper->down = TRUE;
	    for (wtaper = taper->wtapetable;
		 wtaper < taper->wtapetable + taper->nb_worker;
                 wtaper++) {
		if (wtaper && wtaper->job && wtaper->job->sched) {
		    g_free(wtaper->tape_error);
		    wtaper->tape_error = g_strdup("BOGUS");
		    wtaper->result = cmd;
		    if (wtaper->job->dumper) {
			if (wtaper->job->dumper->result != LAST_TOK) {
			    // Dumper already returned it's result
			    dumper_taper_result(wtaper->job);
			} else {
			    dumper_cmd(wtaper->job->dumper, ABORT, wtaper->job->sched, "Taper Failed");
			    wtaper->job->dumper->sent_result = 1;
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
            aaclose(taper->fd);

            break;

	default:
            error(_("driver received unexpected token (%s) from taper"),
                  cmdstr[cmd]);
	    /*NOTREACHED*/
	}

	g_strfreev(result_argv);

	if (wtaper && job && job->sched && wtaper->result != LAST_TOK) {
	    if (wtaper->nb_dle >= taper->max_dle_by_volume) {
		taper->nb_wait_reply++;
		wtaper->state |= TAPER_STATE_WAIT_CLOSED_VOLUME;
		taper_cmd(taper, wtaper, CLOSE_VOLUME, job->sched, NULL, 0, NULL);
		wtaper->state &= ~TAPER_STATE_TAPE_STARTED;
	    }
	    if (job->sched->action == ACTION_DUMP_TO_TAPE) {
		assert(job->dumper != NULL);
		if (job->dumper->result != LAST_TOK) {
		    // Dumper already returned it's result
		    dumper_taper_result(job);
		}
		if (job->dumper->result != LAST_TOK &&
		    job->dumper->dump_finish) {
		    // Dumper already returned it's result
		    dumper_taper_result_finish(job);
		}
	    } else if (job->sched->action == ACTION_FLUSH) {
		file_taper_result(job);
	    } else if (job->sched->action == ACTION_VAULT) {
		vault_taper_result(job);
	    } else {
		g_critical("Invalid job->sched->action %s %s %d", job->sched->disk->host->hostname, job->sched->disk->name, job->sched->action);
	    }
	}

    } while(areads_dataready(taper->fd));
    start_some_dumps(&runq);
    start_a_flush();
    start_a_vault();
}

static void
vault_taper_result(
    job_t *job)
{
    sched_t  *sp     = job->sched;
    disk_t   *dp     = sp->disk;
    wtaper_t *wtaper = job->wtaper;
    taper_t  *taper  = wtaper->taper;
    char     *qname  = quote_string(dp->name);

    sp->taper_attempted += 1;

    job->wtaper = NULL;

    if (wtaper->input_error) {
        g_printf("driver: taper failed %s %s: %s\n",
                   dp->host->hostname, qname, wtaper->input_error);
        if (g_str_equal(sp->datestamp, driver_timestamp)) {
            if (sp->taper_attempted >= dp->retry_dump) {
                //log_add(L_FAIL, _("%s %s %s %d [recovery error for vaulting: %s]"),
		//	dp->host->hostname, qname, sp->datestamp,
		//	sp->level, wtaper->input_error);
                g_printf("driver: taper failed %s %s, recovery error for vaulting\n",
			 dp->host->hostname, qname);
		free_sched(sp);
		sp = NULL;
            } else {
                //log_add(L_INFO, _("%s %s %s %d [Will retry because of recovery error in vaulting: %s]"),
                //        dp->host->hostname, qname, sp->datestamp,
                //        sp->level, wtaper->input_error);
                g_printf("driver: taper will retry %s %s because of recovery error in vaulting\n",
                        dp->host->hostname, qname);
		headqueue_sched(&wtaper->vaultqs.vaultq, sp);
            }
        } else {
	    free_sched(sp);
	    sp = NULL;
        }
    } else if (wtaper->tape_error) {
        g_printf("driver: vaulting failed %s %s with tape error: %s\n",
                   dp->host->hostname, qname, wtaper->tape_error);
        if (sp->taper_attempted >= dp->retry_dump) {
            g_printf("driver: vaulting failed %s %s, too many taper retry\n",
                   dp->host->hostname, qname);
	    free_sched(sp);
	    sp = NULL;
        } else {
            g_printf("driver: vaulting will retry %s %s\n",
                   dp->host->hostname, qname);
            /* Re-insert into vault queue. */
	    sp->action = ACTION_VAULT;
            headqueue_sched(&wtaper->vaultqs.vaultq, sp);
        }
    } else if (wtaper->result != DONE) {
        g_printf("driver: vault failed %s %s without error\n",
                 dp->host->hostname, qname);
	free_sched(sp);
	sp = NULL;
    } else {
        time_t     now = time(NULL);

        cmddatas = remove_cmd_in_cmdfile(cmddatas, sp->command_id);

        /* this code is duplicated in file_taper_result and dumper_taper_result */
        /* Add COPY */
        if (taper->storage_name) {
            storage_t *storage = lookup_storage(taper->storage_name);
            if (storage) {
                vault_list_t vl = storage_get_vault_list(storage);
                for (; vl != NULL; vl = vl->next) {
                    vault_el_t *v = vl->data;
                    if (dump_match_selection(v->storage, sp)) {
                        cmddata_t *cmddata = g_new0(cmddata_t, 1);
                        cmddata->operation = CMD_COPY;
                        cmddata->config = g_strdup(get_config_name());
                        cmddata->src_storage = g_strdup(taper->storage_name);
                        cmddata->src_pool = g_strdup(storage_get_tapepool(storage));
                        cmddata->src_label = g_strdup(wtaper->first_label);
                        cmddata->src_fileno = wtaper->first_fileno;
			cmddata->src_labels_str = g_strdup(wtaper->dst_labels_str);
			cmddata->src_labels = wtaper->dst_labels;
			wtaper->dst_labels = NULL;
                        cmddata->holding_file = NULL;
                        cmddata->hostname = g_strdup(dp->host->hostname);
                        cmddata->diskname = g_strdup(dp->name);
                        cmddata->dump_timestamp = g_strdup(sp->datestamp);
                        cmddata->level = sp->level;
                        cmddata->dst_storage = g_strdup(v->storage);
                        cmddata->working_pid = getppid();
                        cmddata->status = CMD_TODO;
                        cmddata->start_time = now + v->days * 60*60*24;
                        cmddatas = add_cmd_in_cmdfile(cmddatas, cmddata);
			// JLM Should call cmdfile_vault
                    }
                }
            }
        }
	free_sched(sp);
	sp = NULL;
    }

    amfree(qname);

    wtaper->state &= ~TAPER_STATE_VAULT_TO_TAPE;

    amfree(wtaper->input_error);
    amfree(wtaper->tape_error);

    if (!(wtaper->state & (TAPER_STATE_WAIT_CLOSED_VOLUME|TAPER_STATE_WAIT_CLOSED_SOURCE_VOLUME))) {
	wtaper->state |= TAPER_STATE_IDLE;
	free_serial_job(job);
	free_job(job);
	wtaper->job = NULL;
	start_vault_on_same_wtaper(wtaper);
    }

    taper->nb_wait_reply--;
    if (taper->nb_wait_reply == 0) {
        event_release(taper->ev_read);
        taper->ev_read = NULL;
    }

    /* continue with those dumps waiting for diskspace */
    continue_port_dumps();
    start_some_dumps(&runq);
    start_a_flush();
    start_a_vault();
}


static void
file_taper_result(
    job_t *job)
{
    sched_t  *sp     = job->sched;
    wtaper_t *wtaper = job->wtaper;
    taper_t  *taper  = wtaper->taper;
    disk_t   *dp;
    char    *qname = quote_string(sp->disk->name);

    if (wtaper->result == DONE) {
	update_info_taper(sp, wtaper->first_label, wtaper->first_fileno,
			  sp->level);
    }

    sp->taper_attempted += 1;
    dp = sp->disk;

    job->wtaper = NULL;

    if (wtaper->input_error) {
	g_printf("driver: taper failed %s %s: %s\n",
		   dp->host->hostname, qname, wtaper->input_error);
	if (g_str_equal(sp->datestamp, driver_timestamp)) {
	    if (sp->taper_attempted >= dp->retry_dump) {
		g_printf("driver: taper failed %s %s, too many taper retry after holding disk error\n",
		   dp->host->hostname, qname);
		free_sched(sp);
		sp = NULL;
	    } else {
		log_add(L_INFO, _("%s %s %s %d [Will retry dump because of holding disk error: %s]"),
			dp->host->hostname, qname, sp->datestamp,
			sp->level, wtaper->input_error);
		g_printf("driver: taper will retry %s %s because of holding disk error\n",
			dp->host->hostname, qname);
		if (dp->to_holdingdisk != HOLD_REQUIRED) {
		    dp->to_holdingdisk = HOLD_NEVER;
		    sp->dump_attempted -= 1;
		    sp->action = ACTION_DUMP_TO_TAPE;
		    headqueue_sched(&directq, sp);
		} else {
		    free_sched(sp);
		    sp = NULL;
		}
	    }
	} else {
	    free_sched(sp);
	    sp = NULL;
	}
    } else if (wtaper->tape_error) {
	g_printf("driver: taper failed %s %s with tape error: %s\n",
		   dp->host->hostname, qname, wtaper->tape_error);
	if (sp->taper_attempted >= dp->retry_dump) {
	    g_printf("driver: taper failed %s %s, too many taper retry\n",
		   dp->host->hostname, qname);
	    free_sched(sp);
	    sp = NULL;
	} else {
	    char *wall_time = walltime_str(curclock());
	    g_printf("driver: taper will retry %s %s\n",
		   dp->host->hostname, qname);
	    /* Re-insert into taper queue. */
	    sp->action = ACTION_FLUSH;
	    g_printf("driver: requeue write time %s %s %s %s %s\n", wall_time, sp->disk->host->hostname, qname, sp->datestamp, wtaper->taper->storage_name);
	    headqueue_sched(&wtaper->taper->tapeq, sp);
	}
    } else if (wtaper->result != DONE) {
	g_printf("driver: taper failed %s %s without error\n",
		   dp->host->hostname, qname);
    } else {
	cmddata_t *cmddata;
        time_t     now = time(NULL);
	char      *holding_file;

	cmddata = g_hash_table_lookup(cmddatas->cmdfile, GINT_TO_POINTER(sp->command_id));
	if (cmddata) {
	    holding_file = g_strdup(cmddata->holding_file);
	    cmddatas = remove_cmd_in_cmdfile(cmddatas, sp->command_id);

	    /* this code is duplicated in dumper_taper_result  and vault_taper_result */
	    /* Add COPY */
	    if (taper->storage_name) {
		storage_t *storage = lookup_storage(taper->storage_name);
		if (storage) {
		    vault_list_t vl = storage_get_vault_list(storage);
		    for (; vl != NULL; vl = vl->next) {
			vault_el_t *v = vl->data;
			if (dump_match_selection(v->storage, sp)) {
			    cmddata_t *cmddata = g_new0(cmddata_t, 1);
			    cmddata->operation = CMD_COPY;
			    cmddata->config = g_strdup(get_config_name());
			    cmddata->src_storage = g_strdup(taper->storage_name);
			    cmddata->src_pool = g_strdup(storage_get_tapepool(storage));
			    cmddata->src_label = g_strdup(wtaper->first_label);
			    cmddata->src_fileno = wtaper->first_fileno;
			    cmddata->src_labels_str = g_strdup(wtaper->dst_labels_str);
			    cmddata->src_labels = wtaper->dst_labels;
			    wtaper->dst_labels = NULL;
			    cmddata->holding_file = NULL;
			    cmddata->hostname = g_strdup(dp->host->hostname);
			    cmddata->diskname = g_strdup(dp->name);
			    cmddata->dump_timestamp = g_strdup(sp->datestamp);
			    cmddata->level = sp->level;
			    cmddata->dst_storage = g_strdup(v->storage);
			    cmddata->working_pid = getppid();
			    cmddata->status = CMD_TODO;
			    cmddata->start_time = now + v->days * 60*60*24;
			    cmddatas = add_cmd_in_cmdfile(cmddatas, cmddata);
			    // JLM Should call cmdfile_vault
			}
		    }
		}
	    }

	    if (!holding_in_cmdfile(cmddatas, holding_file)) {
		delete_diskspace(sp);
	    }
	    free_sched(sp);
	    sp = NULL;
	    g_free(holding_file);
	};
    }

    amfree(qname);

    wtaper->state &= ~TAPER_STATE_FILE_TO_TAPE;
    if (!(wtaper->state & (TAPER_STATE_WAIT_CLOSED_VOLUME|TAPER_STATE_WAIT_CLOSED_SOURCE_VOLUME))) {
	wtaper->state |= TAPER_STATE_IDLE;
    }
    free_serial_job(job);
    free_job(job);
    wtaper->job = NULL;
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
    start_a_flush();
    start_a_vault();
}

static void
dumper_taper_result(
    job_t *job)
{

    sched_t  *sp     = job->sched;
    wtaper_t *wtaper = job->wtaper;
    dumper_t *dumper = job->dumper;
    disk_t   *dp     = sp->disk;
    char     *qname;

    if (!dumper->sent_result) {
	if (dumper->result == DONE && wtaper->result == DONE) {
	    dumper_cmd(dumper, SUCCESS, sp, NULL);
	    dumper->sent_result = 1;
	    update_info_dumper(sp, sp->origsize,
			       sp->dumpsize, sp->dumptime);
	    update_info_taper(sp, wtaper->first_label, wtaper->first_fileno,
			      sp->level);
	    qname = quote_string(dp->name); /*quote to take care of spaces*/

	    log_add(L_STATS, _("estimate %s %s %s %d [sec %ld nkb %lld ckb %lld kps %lu]"),
		    dp->host->hostname, qname, sp->datestamp,
		    sp->level,
		    sp->est_time, (long long)sp->est_nsize,
		    (long long)sp->est_csize,
		    sp->est_kps);
	    amfree(qname);

	    fix_index_header(sp);
	} else {
	    if (dumper->result == DONE) {
		dumper_cmd(dumper, FAILED, sp, NULL);
		dumper->sent_result = 1;
	    }
	    update_failed_dump(sp);
	}
    }

    if (dumper->result != RETRY) {
	sp->dump_attempted += 1;
	sp->taper_attempted += 1;
    }
}

static void
dumper_taper_result_finish(
    job_t *job)
{
    sched_t  *sp     = job->sched;
    wtaper_t *wtaper = job->wtaper;
    taper_t  *taper  = wtaper->taper;
    dumper_t *dumper = job->dumper;
    disk_t   *dp     = sp->disk;
    char     *qname;

    if ((dumper->result != DONE || wtaper->result != DONE) &&
	sp->dump_attempted < dp->retry_dump &&
	sp->taper_attempted < dp->retry_dump) {
	char *wall_time = walltime_str(curclock());
	sp->action = ACTION_DUMP_TO_TAPE;
	enqueue_sched(&directq, sp);
	qname = quote_string(dp->name); /*quote to take care of spaces*/
	g_printf("driver: requeue dump_to_tape time %s %s %s %s %s\n", wall_time, sp->disk->host->hostname, qname, sp->datestamp, wtaper->taper->storage_name);
	amfree(qname);
    }

    if (dumper->result == DONE && wtaper->result == DONE) {
	time_t now = time(NULL);
	/* this code is duplicated in file_taper_result and vault_taper_result*/
	/* Add COPY */
	if (taper->storage_name) {
	    storage_t *storage = lookup_storage(taper->storage_name);
	    if (storage) {
		vault_list_t vl = storage_get_vault_list(storage);
		for (; vl != NULL; vl = vl->next) {
		    vault_el_t *v = vl->data;
		    if (dump_match_selection(v->storage, sp)) {
			cmddata_t *cmddata = g_new0(cmddata_t, 1);
			cmddata->operation = CMD_COPY;
			cmddata->config = g_strdup(get_config_name());
			cmddata->src_storage = g_strdup(taper->storage_name);
			cmddata->src_pool = g_strdup(storage_get_tapepool(storage));
			cmddata->src_label = g_strdup(wtaper->first_label);
                        cmddata->src_fileno = wtaper->first_fileno;
			cmddata->src_labels_str = g_strdup(wtaper->dst_labels_str);
			cmddata->src_labels = wtaper->dst_labels;
			wtaper->dst_labels = NULL;
			cmddata->holding_file = NULL;
			cmddata->hostname = g_strdup(dp->host->hostname);
			cmddata->diskname = g_strdup(dp->name);
			cmddata->dump_timestamp = g_strdup(sp->datestamp);
			cmddata->level = sp->level;
			cmddata->dst_storage = g_strdup(v->storage);
			cmddata->working_pid = getppid();
			cmddata->status = CMD_TODO;
			cmddata->start_time = now + v->days * 60*60*24;
			cmddatas = add_cmd_in_cmdfile(cmddatas, cmddata);
			// JLM Should call cmdfile_vault
		    }
		}
	    }
	}
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
    if (!(wtaper->state & (TAPER_STATE_WAIT_CLOSED_VOLUME|TAPER_STATE_WAIT_CLOSED_SOURCE_VOLUME))) {
	wtaper->state |= TAPER_STATE_IDLE;
    }
    amfree(wtaper->input_error);
    amfree(wtaper->tape_error);
    dumper->busy = 0;
    dp->host->inprogress -= 1;
    dp->inprogress = 0;
    deallocate_bandwidth(dp->host->netif, sp->est_kps);
    free_serial_job(job);
    free_job(job);
    dumper->job = NULL;
    wtaper->job = NULL;

    start_some_dumps(&runq);
    start_a_flush();
    start_a_vault();
}


static wtaper_t *
idle_taper(taper_t *taper)
{
    wtaper_t *wtaper;

    /* Use an already started wtaper first */
    for (wtaper = taper->wtapetable;
	 wtaper < taper->wtapetable + taper->nb_worker;
	 wtaper++) {
	if ((wtaper->state & TAPER_STATE_IDLE) &&
	    (wtaper->state & TAPER_STATE_TAPE_STARTED) &&
	    !(wtaper->state & TAPER_STATE_DONE) &&
	    !(wtaper->state & TAPER_STATE_FILE_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_DUMP_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_VAULT_TO_TAPE))
	    return wtaper;
    }

    /* Then use one with a reservation */
    for (wtaper = taper->wtapetable;
	 wtaper < taper->wtapetable + taper->nb_worker;
	 wtaper++) {
	if ((wtaper->state & TAPER_STATE_IDLE) &&
	    (wtaper->state & TAPER_STATE_RESERVATION) &&
	    !(wtaper->state & TAPER_STATE_DONE) &&
	    !(wtaper->state & TAPER_STATE_FILE_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_DUMP_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_VAULT_TO_TAPE))
	    return wtaper;
    }

    /* Then use any idle wtaper */
    for (wtaper = taper->wtapetable;
	 wtaper < taper->wtapetable + taper->nb_worker;
	 wtaper++) {
	if ((wtaper->state & TAPER_STATE_IDLE) &&
	    !(wtaper->state & TAPER_STATE_DONE) &&
	    !(wtaper->state & TAPER_STATE_FILE_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_DUMP_TO_TAPE) &&
	    !(wtaper->state & TAPER_STATE_VAULT_TO_TAPE))
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
    sched_t   *sp;
    disk_t    *dp;

    dumper  = job->dumper;
    chunker = job->chunker;
    sp      = job->sched;
    dp      = sp->disk;

    if (dumper->sent_result == 1)
	return;

    if (!dumper->sent_result) {
	if (dumper->result == DONE && chunker->result == DONE) {
	    char *qname = quote_string(dp->name);/*quote to take care of spaces*/
	    dumper_cmd(dumper, SUCCESS, sp, NULL);
	    dumper->sent_result = 1;
	    update_info_dumper(sp, sp->origsize,
			       sp->dumpsize, sp->dumptime);

	    log_add(L_STATS, _("estimate %s %s %s %d [sec %ld nkb %lld ckb %lld kps %lu]"),
		    dp->host->hostname, qname, sp->datestamp,
		    sp->level,
		    sp->est_time, (long long)sp->est_nsize,
                    (long long)sp->est_csize,
		    sp->est_kps);
	    amfree(qname);
	} else {
	    if (dumper->result == DONE) {
		dumper_cmd(dumper, FAILED, sp, NULL);
		dumper->sent_result = 1;
	    }
	    update_failed_dump(sp);
	}
    }

}

static void
dumper_chunker_result_finish(
    job_t *job)
{
    dumper_t  *dumper;
    chunker_t *chunker;
    taper_t   *taper;
    sched_t   *sp;
    disk_t    *dp;
    assignedhd_t **h=NULL;
    int activehd, i;
    off_t dummy;
    off_t size;
    int is_partial;

    dumper  = job->dumper;
    chunker = job->chunker;
    sp      = job->sched;
    dp      = sp->disk;

    h = sp->holdp;
    activehd = sp->activehd;

    deallocate_bandwidth(dp->host->netif, sp->est_kps);

    is_partial = dumper->result != DONE || chunker->result != DONE;
    rename_tmp_holding(sp->destname, !is_partial);
    holding_set_from_driver(sp->destname, sp->origsize,
		sp->native_crc, sp->client_crc,
		sp->server_crc);
    fix_index_header(sp);

    dummy = (off_t)0;
    for (i = 0, h = sp->holdp; i < activehd; i++) {
	dummy += h[i]->used;
    }

    size = holding_file_size(sp->destname, 0);
    h[activehd]->used = size - dummy;
    h[activehd]->disk->allocated_dumpers--;
    adjust_diskspace(sp, DONE);

    if (dumper->result != RETRY) {
	sp->dump_attempted += 1;
    }

    if ((dumper->result != DONE || chunker->result != DONE) &&
        sp->dump_attempted < dp->retry_dump) {
	char *wall_time = walltime_str(curclock());
	char *qname = quote_string(sp->disk->name);
	delete_diskspace(sp);
	if (sp->no_space) {
	    sp->action = ACTION_DUMP_TO_TAPE;
	    g_printf("driver: requeue dump_to_tape time %s %s %s %s\n", wall_time, sp->disk->host->hostname, qname, sp->datestamp);
	    enqueue_sched(&directq, sp);
	} else {
	    sp->action = ACTION_DUMP_TO_HOLDING;
	    g_printf("driver: requeue dump time %s %s %s %s\n", wall_time, sp->disk->host->hostname, qname, sp->datestamp);
	    enqueue_sched(&runq, sp);
	}
	g_free(qname);
    } else if (size > (off_t)DISK_BLOCK_KB) {
	identlist_t il;

	for (il = getconf_identlist(CNF_ACTIVE_STORAGE); il != NULL; il = il->next) {
	    char *storage_name = (char *)il->data;
	    if (dump_match_selection(storage_name, sp)) {
		char *qname = quote_string(dp->name);
		cmddata_t *cmddata = g_new0(cmddata_t, 1);

		cmddata->operation = CMD_FLUSH;
		cmddata->config = g_strdup(get_config_name());
		cmddata->src_storage = NULL;
		cmddata->src_pool = NULL;
		cmddata->src_label = NULL;
		cmddata->src_fileno = 0;
		cmddata->src_labels = NULL;
		cmddata->holding_file = g_strdup(sp->destname);
		cmddata->hostname = g_strdup(dp->hostname);
		cmddata->diskname = g_strdup(dp->name);
		cmddata->dump_timestamp = g_strdup(driver_timestamp);
		cmddata->dst_storage = g_strdup(storage_name);
		cmddata->working_pid = getppid();
		cmddata->status = CMD_TODO;
		cmddatas = add_cmd_in_cmdfile(cmddatas, cmddata);

		for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		    if (g_str_equal(storage_name, taper->storage_name)) {
			sched_t *sp1 = g_new0(sched_t, 1);
			*sp1 = *sp;
			sp1->action = ACTION_FLUSH;
	                sp1->destname = g_strdup(sp->destname);
	                sp1->dumpdate = g_strdup(sp->dumpdate);
	                sp1->degr_dumpdate = g_strdup(sp->degr_dumpdate);
	                sp1->degr_mesg = g_strdup(sp->degr_mesg);
	                sp1->datestamp = g_strdup(sp->datestamp);
			enqueue_sched(&taper->tapeq, sp1);

			sp1->command_id = cmddata->id;
			g_printf("driver: to write host %s disk %s date %s on storage %s\n",
				 dp->host->hostname, qname, driver_timestamp, taper->storage_name);
		    }
		}
		amfree(qname);
	    }
	}
	free_sched(sp);
    } else {
	free_sched(sp);
    }

    dumper->busy = 0;
    dp->host->inprogress -= 1;
    dp->inprogress = 0;

    waitpid(chunker->pid, NULL, 0 );
    aaclose(chunker->fd);
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
    start_a_flush();
    start_a_vault();
}

static gboolean
dump_match_selection(
    char    *storage_n,
    sched_t *sp)
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
	    if (!sp->disk->tags) {
		ok = TRUE;
	    } else {
		for (tags = sp->disk->tags; tags != NULL ; tags = tags->next) {
		    if (g_str_equal(ds->tag, tags->data)) {
			ok = TRUE;
			break;
		    }
		}
	    }
	} else if (ds->tag_type == TAG_OTHER) {
	    // WHAT DO TO HERE
	}

	if (ok) {
	    if (ds->level == LEVEL_ALL) {
		return TRUE;
	    } else if (ds->level == LEVEL_FULL && sp->level == 0) {
		return TRUE;
	    } else if (ds->level == LEVEL_INCR && sp->level > 0) {
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
    sched_t  *sp, *sp1;
    GList  *slist1;
    disk_t *dp, *dp1;
    cmd_t cmd;
    int result_argc;
    char *qname;
    char **result_argv;
    cmd_t result = FAILED;

    assert(dumper != NULL);
    job = dumper->job;
    sp = job->sched;
    assert(sp != NULL);
    dp = sp->disk;
    assert(dp != NULL);

    do {

	short_dump_state();

	cmd = getresult(dumper->fd, 1, &result_argc, &result_argv);

	if (cmd != BOGUS) {
	    /* result_argv[1] always contains the serial number */
	    job = serial2job(result_argv[1]);
	    assert(job == dumper->job);
	    if (sp != job->sched) {
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

	    sp->origsize = OFF_T_ATOI(result_argv[2]);
	    sp->dumptime = TIME_T_ATOI(result_argv[4]);
	    parse_crc(result_argv[5], &sp->native_crc);
	    parse_crc(result_argv[6], &sp->client_crc);

	    g_printf(_("driver: finished-cmd time %s %s dumped %s:%s\n"),
		   walltime_str(curclock()), dumper->name,
		   dp->host->hostname, qname);
	    fflush(stdout);

	    dumper->result = cmd;
	    result = SUCCESS;

	    break;

	case TRYAGAIN: /* TRY-AGAIN <handle> <errstr> */
	    /*
	     * Requeue this disk, and fall through to the FAILED
	     * case for cleanup.
	     */
	    g_free(sp->try_again_message);
	    sp->try_again_message = g_strdup(result_argv[2]);
	    if (sp->dump_attempted >= dp->retry_dump) {
		char *qname = quote_string(dp->name);
		char *qerr = quote_string(result_argv[2]);
		log_add(L_FAIL, _("%s %s %s %d [too many dumper retry: %s]"),
		    dp->host->hostname, qname, sp->datestamp,
		    sp->level, qerr);
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
		    sp->level = level;
		}
		break;
	    }

	case DUMP_FINISH: /* DUMP_FINISH <handle> */
	    dumper->dump_finish = 1;
	    break;
	case BOGUS:
	    /* either EOF or garbage from dumper.  Turn it off */
	    log_add(L_WARNING, _("%s pid %ld is messed up, ignoring it.\n"),
		    dumper->name, (long)dumper->pid);
	    if (dumper->ev_read) {
		event_release(dumper->ev_read);
		dumper->ev_read = NULL;
	    }
	    aaclose(dumper->fd);
	    dumper->busy = 0;
	    dumper->down = 1;	/* mark it down so it isn't used again */

            /* if it was dumping something, zap it and try again */
            if (sp->dump_attempted >= dp->retry_dump-1) {
		log_add(L_FAIL, _("%s %s %s %d [%s died]"),
			dp->host->hostname, qname, sp->datestamp,
			sp->level, dumper->name);
            } else {
		log_add(L_WARNING, _("%s died while dumping %s:%s lev %d."),
			dumper->name, dp->host->hostname, qname,
			sp->level);
            }
	    dumper->result = cmd;
	    break;

	default:
	    assert(0);
	}
        amfree(qname);
	g_strfreev(result_argv);

	if (cmd == DUMP_FINISH) {
	} else if (cmd != BOGUS) {
	    int last_dump = 1;
	    dumper_t *dumper;

	    run_server_dle_scripts(EXECUTE_ON_POST_DLE_BACKUP,
			       get_config_name(), driver_timestamp,
			       dp, sp->level, result);
	    /* check dump not yet started */
	    for (slist1 = runq.head; slist1 != NULL; slist1 = slist1->next) {
		sp1 = get_sched(slist1);
		dp1 = sp1->disk;
		if (dp1->host == dp->host)
		    last_dump = 0;
	    }
	    /* check direct to tape dump */
	    for(slist1 = directq.head; slist1 != NULL; slist1 = slist1->next) {
		sp1 = get_sched(slist1);
		dp1 = sp1->disk;
		if (dp1->host == dp->host)
		    last_dump = 0;
	    }
	    /* check dumping dle */
	    for (dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
		if (dumper->busy && dumper->job->sched != sp &&
		    dumper->job->sched->disk->host == dp->host)
		 last_dump = 0;
	    }
	    if (last_dump && dp->host->post_script == 0) {
		if (dp->host->post_script == 0) {
		    run_server_host_scripts(EXECUTE_ON_POST_HOST_BACKUP,
					    get_config_name(), driver_timestamp,
					    dp->host);
		    dp->host->post_script = 1;
		}
	    }
	}

	    /* send the dumper result to the chunker */
	    if (job->chunker) {
		if (cmd == TRYAGAIN) {
		    char *abort_message = g_strdup_printf("dumper TRYAGAIN: %s",
						sp->try_again_message);
		    chunker_cmd(job->chunker, ABORT, sp, abort_message);
		    g_free(abort_message);
		    pending_aborts++;
		} else if (job->chunker->sendresult) {
		    if (cmd == DONE) {
			chunker_cmd(job->chunker, DONE, sp, NULL);
		    } else {
			chunker_cmd(job->chunker, FAILED, sp, NULL);
		    }
		    job->chunker->sendresult = FALSE;
		}
		if (dumper->result != LAST_TOK &&
		    job->chunker->result != LAST_TOK)
		    dumper_chunker_result(job);
	    } else if (job->wtaper) {
		if (job->wtaper->ready) { /* send the dumper result to the taper */
		    wtaper = job->wtaper;
		    taper = wtaper->taper;
		    if (cmd == TRYAGAIN) {
			char *abort_message = g_strdup_printf("dumper TRYAGAIN: %s",
							sp->try_again_message);
			taper_cmd(taper, wtaper, ABORT, sp, NULL, 0, abort_message);
			g_free(abort_message);
		    } else if (cmd == DONE && wtaper->sendresult) {
			taper_cmd(taper, wtaper, DONE, sp, NULL, 0, NULL);
			wtaper->sendresult = FALSE;
		    } else if (wtaper->sendresult) {
			taper_cmd(taper, wtaper, FAILED, sp, NULL, 0, NULL);
			wtaper->sendresult = FALSE;
		    }
		}
		if (job->dumper && job->wtaper->result != LAST_TOK) {
		    dumper_taper_result(job);
		}
	    }

	if (dumper->dump_finish) {
	    if (job->chunker) {
		if (job->chunker->result != LAST_TOK) {
		    dumper_chunker_result_finish(job);
		}
	    } else if (job->wtaper) {
		if (job->wtaper->result != LAST_TOK) {
		    dumper_taper_result_finish(job);
		}
	    }
	}
    } while(areads_dataready(dumper->fd));
}


static void
handle_chunker_result(
    void *	cookie)
{
    chunker_t *chunker = cookie;
    dumper_t *dumper;
    assignedhd_t **h=NULL;
    job_t    *job;
    sched_t  *sp;
    disk_t *dp;
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    int dummy;
    int activehd;
    char *qname;

    assert(chunker != NULL);

    do {
	short_dump_state();

	job = NULL;
	sp = NULL;
	dp = NULL;
	h = NULL;
	activehd = -1;

	cmd = getresult(chunker->fd, 1, &result_argc, &result_argv);

	if (cmd != BOGUS) {
	    /* result_argv[1] always contains the serial number */
	    job = serial2job(result_argv[1]);
	    assert(job == chunker->job);
	    sp = job->sched;
	    assert(sp != NULL);
	    dp = sp->disk;
	    assert(dp != NULL);
	    assert(sp->destname != NULL);
	    assert(dp != NULL && sp != NULL && sp->destname);
	    if (sp->holdp) {
		h = sp->holdp;
		activehd = sp->activehd;
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

	    sp->dumpsize = (off_t)atof(result_argv[2]);
	    parse_crc(result_argv[3], &sp->server_crc);

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

	case PORT: /* PORT <handle> <port> <dataport_list> */
	case SHM_NAME: /* SHM-NAME <handle> <port> <shm_name> */
	    if (result_argc != 4) {
                error(_("error [chunker %s result_argc != 4: %d]"), cmdstr[cmd],
                      result_argc);
                /*NOTREACHED*/
            }
	    dumper = job->dumper;
	    amfree(sp->disk->dataport_list);
	    amfree(sp->disk->shm_name);

	    dumper->output_port = atoi(result_argv[2]);
	    if (job->do_port_write) {
		sp->disk->dataport_list = g_strdup(result_argv[3]);
	    } else {
		sp->disk->shm_name = g_strdup(result_argv[3]);
	    }

	    if (sp->disk->host->pre_script == 0) {
		run_server_host_scripts(EXECUTE_ON_PRE_HOST_BACKUP,
					get_config_name(), driver_timestamp,
					sp->disk->host);
		sp->disk->host->pre_script = 1;
	    }
	    run_server_dle_scripts(EXECUTE_ON_PRE_DLE_BACKUP,
				   get_config_name(), driver_timestamp,
				   sp->disk, sp->level, BOGUS);
	    if (job->do_port_write) {
		dumper_cmd(dumper, PORT_DUMP, sp, NULL);
	    } else {
		dumper_cmd(dumper, SHM_DUMP, sp, NULL);
	    }
	    dumper->sent_command = TRUE;
	    dumper->ev_read = event_create(
				(event_id_t)dumper->fd,
				EV_READFD,
				handle_dumper_result, dumper);
	    event_activate(dumper->ev_read);
	    break;

	case DUMPER_STATUS: /* DUMP-STATUS <handle> */
	    if (job->dumper->result == LAST_TOK) {
		chunker->sendresult = TRUE;
	    } else {
		if (job->dumper->result == DONE) {
		    chunker_cmd(chunker, DONE, sp, NULL);
		} else {
		    chunker_cmd(chunker, FAILED, sp, NULL);
		}
	    }
	    break;

	case NO_ROOM: /* NO-ROOM <handle> <missing_size> <message> */
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
		sp->activehd++;
		chunker_cmd(chunker, CONTINUE, sp, NULL);
	    } else { /* !h[++activehd] - must allocate more space */
		sp->act_size = sp->est_size; /* not quite true */
		sp->est_size = (sp->act_size/(off_t)20) * (off_t)21; /* +5% */
		sp->est_size = am_round(sp->est_size, (off_t)DISK_BLOCK_KB);
		if (sp->est_size < sp->act_size + 2*DISK_BLOCK_KB)
		    sp->est_size += 2 * DISK_BLOCK_KB;
		h = find_diskspace(sp->est_size - sp->act_size,
				   &dummy,
				   h[activehd-1]);
		if( !h ) {
		    /* No diskspace available. The reason for this will be
		     * determined in continue_port_dumps(). */
		    enqueue_sched(&roomq, sp);
		    continue_port_dumps();
		    /* continue flush waiting for new tape */
		    start_a_flush();
		    start_a_vault();
		} else {
		    /* OK, allocate space for disk and have chunker continue */
		    sp->activehd = assign_holdingdisk( h, sp );
		    chunker_cmd(chunker, CONTINUE, sp, NULL);
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
	    chunker_cmd(chunker, QUIT, NULL, NULL);

	    break;

	case BOGUS:
	    /* either EOF or garbage from chunker.  Turn it off */
	    log_add(L_WARNING, _("%s pid %ld is messed up, ignoring it.\n"),
		    chunker->name, (long)chunker->pid);

            /* if it was dumping something, zap it and try again */
	    sp = chunker->job->sched;
	    assert(sp != NULL);
	    dp = sp->disk;
	    assert(dp != NULL);
	    assert(sp->destname != NULL);
	    assert(dp != NULL && sp != NULL && sp->destname);
	    job = chunker->job;
	    if (sp->holdp) {
		h = sp->holdp;
		activehd = sp->activehd;
	    }
            g_assert(h && activehd >= 0);
            qname = quote_string(dp->name);
            if (sp->dump_attempted >= dp->retry_dump-1) {
                log_add(L_FAIL, _("%s %s %s %d [%s died]"),
                        dp->host->hostname, qname, sp->datestamp,
                        sp->level, chunker->name);
            } else {
                log_add(L_WARNING, _("%s died while dumping %s:%s lev %d."),
                        chunker->name, dp->host->hostname, qname,
                        sp->level);
            }
            amfree(qname);

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    g_strfreev(result_argv);

	    if (chunker->result != LAST_TOK &&
		job->dumper && (job->dumper->result != LAST_TOK ||
				!job->dumper->sent_command)) {
		dumper_chunker_result(job);
	    }
	    if (chunker->result != LAST_TOK &&
		job->dumper && job->dumper->result != LAST_TOK &&
		job->dumper->dump_finish) {
		dumper_chunker_result_finish(job);
	    }

	    return;

	    break;

	default:
	    assert(0);
	}
	g_strfreev(result_argv);

	if (cmd != BOGUS) {
	    if (chunker->result != LAST_TOK && (job->dumper->result != LAST_TOK ||
						!job->dumper->sent_command)) {
		dumper_chunker_result(job);
	    }
	    if (chunker->result != LAST_TOK && ((job->dumper->result != LAST_TOK &&
						 job->dumper->dump_finish) ||
						!job->dumper->sent_command)) {
		dumper_chunker_result_finish(job);
	    }
	}

    } while(areads_dataready(chunker->fd));
}


static void
read_flush(
    void *	cookie)
{
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
    char *ids;
    assignedhd_t **holdp;
    taper_t       *taper;
    cmddata_t     *cmddata;
    char        **ids_array;
    char        **one_id;
    gboolean      free_holdp;

    (void)cookie;	/* Quiet unused parameter warning */

    event_release(flush_ev_read);
    flush_ev_read = NULL;

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
	    amfree(destname);
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
	    flushhost->status = 0;
	    flushhost->features = NULL;
	}
	dp1->hostnext = flushhost->disks;
	flushhost->disks = dp1;

	holdp = build_diskspace(destname);
	if (holdp == NULL) continue;
	free_holdp = TRUE;
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
	    if (cmddata) {
		for (taper = tapetable; taper < tapetable+nb_storage ; taper++) {
		    if (g_str_equal(taper->storage_name, cmddata->dst_storage)) {
			sched_t *sp;
			sp = g_new0(sched_t, 1);
			sp->command_id = cmddata->id;
			sp->action = ACTION_FLUSH;
			sp->destname = g_strdup(destname);
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
			free_holdp = FALSE;
			sp->timestamp = (time_t)0;
			sp->disk = dp;

			enqueue_sched(&taper->tapeq, sp);
		    }
		}
	    } else {
		// What to do with the holding file?
	    }
	}
	if (free_holdp) {
	    free_assignedhd(holdp);
	}
	g_strfreev(ids_array);
	dumpfile_free_data(&file);
	amfree(destname);
    }
    amfree(inpline);
    close_infofile();

    /* re-read de cmdfile */
    close_cmdfile(cmddatas);
    cmddatas = read_cmdfile(conf_cmdfile);
    unlock_cmdfile(cmddatas);

    start_a_flush();
    if (!nodump) {
	schedule_ev_read = event_create((event_id_t)0, EV_READFD,
					  read_schedule, NULL);
	event_activate(schedule_ev_read);
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

	if (degr_dumpdate) {
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

	sp->disk = dp;
	if (dp->host->features == NULL) {
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
		sp->action = ACTION_DUMP_TO_TAPE;
		enqueue_sched(&directq, sp);
	    } else {
		sp->action = ACTION_DUMP_TO_HOLDING;
		enqueue_sched(&runq, sp);
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
    run_server_global_scripts(EXECUTE_ON_PRE_BACKUP, get_config_name(),
			      driver_timestamp);
    if (empty(runq)) force_flush = 1;
    start_some_dumps(&runq);
    start_a_flush();
    start_a_vault();
}

static void
cmdfile_vault(
    gpointer key G_GNUC_UNUSED G_GNUC_UNUSED,
    gpointer value,
    gpointer user_data G_GNUC_UNUSED)
{
    cmddata_t    *cmddata = value;
    const time_t  now = time(NULL);
    taper_t      *taper = NULL;
    int           i;
    disk_t       *dp;
    //disk_t       *dp1;
    sched_t      *sp;
    GSList       *sl;
    GList        *slist;
    GSList       *vaultqss;
    vaultqs_t    *vaultqs;
    vaultqs_t    *found_vaultqs;
    GSList       *labels;
    int           nb_cmddata_src_labels = 0;
    int           nb_vaultqs_src_labels = 0;
    gboolean      inserted;

    //cmdfile_data_t *data = user_data;

    if (cmddata->operation != CMD_COPY)
	return;

    if (cmddata->start_time > now)
	return;

    // find taper_t for the storage
    for (i=0; i < nb_storage ; i++) {
	if (tapetable[i].storage_name &&
	    g_str_equal(tapetable[i].storage_name, cmddata->dst_storage)) {
	    taper = &tapetable[i];
	    break;
	}
    }
    if (!taper)
	return;

    // create a disk_t and sched_t
    dp = lookup_disk(cmddata->hostname, cmddata->diskname);
    if (!dp) {
	log_add(L_INFO, _("driver: disk %s:%s not in database, skipping it."),
			  cmddata->hostname, cmddata->diskname);
	return;
    }

    sp = g_new0(sched_t, 1);
    sp->level = cmddata->level;
    sp->dumpdate = NULL;
    sp->degr_dumpdate = NULL;
    sp->degr_mesg = NULL;
    sp->datestamp = g_strdup(cmddata->dump_timestamp);
    sp->est_nsize = (off_t)0;
    sp->est_csize = (off_t)0;
    sp->est_time = 0;
    sp->est_kps = 10;
    sp->origsize = 0;
    sp->priority = 0;
    sp->degr_level = -1;
    sp->dump_attempted = 0;
    sp->taper_attempted = 0;
    if (cmddata->size > 1000)
        sp->act_size = cmddata->size;
    else
	sp->act_size =  1000;
    sp->holdp = NULL;
    sp->timestamp = (time_t)0;
    sp->src_storage = g_strdup(cmddata->src_storage);
    sp->src_pool = g_strdup(cmddata->src_pool);
    sp->src_label = g_strdup(cmddata->src_label);
    sp->src_fileno = cmddata->src_fileno;
    sp->command_id = cmddata->id;

    sp->disk = dp;

    cmddata->working_pid = getpid();

    sp->action = ACTION_VAULT;

    found_vaultqs = NULL;
    for (sl = cmddata->src_labels; sl != NULL; sl = sl->next) {
	char *s = g_strconcat(" ;", (char *)sl->data, " ;", NULL);
	nb_cmddata_src_labels++;
	for (vaultqss = taper->vaultqss; vaultqss != NULL; vaultqss = vaultqss->next) {
	     vaultqs = (vaultqs_t *)vaultqss->data;
	    if (strstr(vaultqs->src_labels_str, s) != NULL) {
		found_vaultqs = vaultqs;
		break;
	    }
	}
    }
    vaultqs = found_vaultqs;
    inserted = FALSE;
    if (!vaultqs) {
	g_debug("New VAULTQS");
	vaultqs = g_new0(vaultqs_t, 1);
	vaultqs->src_labels_str = g_strdup(cmddata->src_labels_str);
	taper->vaultqss = g_slist_append(taper->vaultqss, vaultqs);
	for (sl = cmddata->src_labels; sl != NULL; sl = sl->next) {
	    vaultqs->src_labels = g_slist_append(vaultqs->src_labels,
						 g_strdup((char *)sl->data));
	}
    }
    else {
	g_debug("Same VAULTQS");
	for (labels = vaultqs->src_labels; labels != NULL; labels = labels->next) {
	    nb_vaultqs_src_labels++;
	}

	/* must sort: in src_label sequence */
	/*            in src_fileno         */
	if (nb_cmddata_src_labels == 1 &&
            nb_vaultqs_src_labels == 1) {
	    for (slist = vaultqs->vaultq.head; slist != NULL; slist = slist->next) {
		sched_t *sp1 = get_sched(slist);
		if (sp1->src_fileno > sp->src_fileno) {
		    insert_before_sched(&vaultqs->vaultq, slist, sp);
		    inserted = TRUE;
		    break;
		}
	    }
	} else if (nb_cmddata_src_labels == 1 &&
		   nb_vaultqs_src_labels > 1) {
	    gboolean label_seen = FALSE;
	    for (slist = vaultqs->vaultq.head; slist != NULL; slist = slist->next) {
		sched_t *sp1 = get_sched(slist);
		if (strcmp(sp1->src_label, cmddata->src_label) == 0)
		    label_seen = TRUE;
		if (((strcmp(sp1->src_label, cmddata->src_label) == 0) &&
		     sp1->src_fileno > sp->src_fileno) ||
		    (label_seen &&
		     (strcmp(sp1->src_label, cmddata->src_label) != 0))) {
		    insert_before_sched(&vaultqs->vaultq, slist, sp);
		    inserted = TRUE;
		    break;
		}
	    }
	} else if (nb_cmddata_src_labels > 1) {
	    GSList **vaultqsa, **vaultqsa1, **vaultqsa2;
	    vaultqsa = vaultqsa1 = g_new0(GSList *, nb_cmddata_src_labels+1);
	    for (sl = cmddata->src_labels; sl != NULL; sl = sl->next) {
		char *s = g_strconcat(" ;", (char *)sl->data, " ;", NULL);
		for (vaultqss = taper->vaultqss; vaultqss != NULL; vaultqss = vaultqss->next) {
		     vaultqs = (vaultqs_t *)vaultqss->data;
		    if (strstr(vaultqs->src_labels_str, s) != NULL) {
			gboolean found = FALSE;
			for (vaultqsa2 = vaultqsa; vaultqsa2 < vaultqsa1; vaultqsa2++) {
			    if (vaultqs == (*vaultqsa2)->data) {
				found = TRUE;
			    }
			}
			if (!found) {
			    *vaultqsa1++ = vaultqss;
			}
		    }
		}
	    }

	    /* must remove them from taper->vaultqss, except first */
	    for (vaultqsa2 = vaultqsa+1; *vaultqsa2 != NULL; vaultqsa2++) {
		taper->vaultqss = g_slist_remove_link(taper->vaultqss, *vaultqsa2);
	    }

	    vaultqs = (vaultqs_t *)((*vaultqsa)->data);
	    if (strcmp(cmddata->src_label, (char *)vaultqs->src_labels->data) == 0) {
		    /* add last */
		    enqueue_sched(&vaultqs->vaultq, sp);
		    inserted = TRUE;
	    } else {
		    /* add first */
		    headqueue_sched(&vaultqs->vaultq, sp);
		    inserted = TRUE;
	    }

	    /* must merge all vaultq in vaultqsa to the first one */
	    for (vaultqsa2 = vaultqsa+1; *vaultqsa2 != NULL; vaultqsa2++) {
		*vaultqsa = g_slist_concat(*vaultqsa, *vaultqsa2);
	    }

	    g_free(vaultqsa);

	}
    }

    if (!inserted) {
	enqueue_sched(&vaultqs->vaultq, sp);
    }
}

static void
set_vaultqs(void)
{

    close_cmdfile(cmddatas);
    cmddatas = read_cmdfile(conf_cmdfile);

    g_hash_table_foreach(cmddatas->cmdfile, &cmdfile_vault, NULL);

    write_cmdfile(cmddatas);
}

static unsigned long
network_free_kps(
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
	g_printf(_(" if %s: free %lu"), interface_name(ip->config), network_free_kps(ip));
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
holding_free_space(void)
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
    sched_t *		sp)
{
    int i, j, c, l=0;
    off_t size;
    char lvl[64];
    assignedhd_t **new_holdp;
    char *qname;
    disk_t *dp = sp->disk;
    char *sfn = sanitise_filename(dp->name);

    g_snprintf( lvl, sizeof(lvl), "%d", sp->level );

    size = am_round(sp->est_size - sp->act_size,
		    (off_t)DISK_BLOCK_KB);

    for( c = 0; holdp[c]; c++ )
	(void)c; /* count number of disks */

    /* allocate memory for sp->holdp */
    for(j = 0; sp->holdp && sp->holdp[j]; j++)
	(void)j;	/* Quiet lint */
    new_holdp = (assignedhd_t **)g_malloc(sizeof(assignedhd_t*)*(j+c+1));
    if (sp->holdp) {
	memcpy(new_holdp, sp->holdp, j * sizeof(*new_holdp));
	amfree(sp->holdp);
    }
    sp->holdp = new_holdp;
    new_holdp = NULL;

    i = 0;
    if( j > 0 ) { /* This is a request for additional diskspace. See if we can
		   * merge assignedhd_t's */
	l=j;
	if (sp->holdp[j-1]->disk == holdp[0]->disk ) { /* Yes! */
	    sp->holdp[j-1]->reserved += holdp[0]->reserved;
	    holdp[0]->disk->allocated_space += holdp[0]->reserved;
	    size = (holdp[0]->reserved>size) ? (off_t)0 : size-holdp[0]->reserved;
	    qname = quote_string(dp->name);
	    hold_debug(1, _("assign_holdingdisk: merging holding disk %s to disk %s:%s, add %lld for reserved %lld, left %lld\n"),
			   holdingdisk_get_diskdir(
					       sp->holdp[j-1]->disk->hdisk),
			   dp->host->hostname, qname,
			   (long long)holdp[0]->reserved,
			   (long long)sp->holdp[j-1]->reserved,
			   (long long)size);
	    i++;
	    amfree(qname);
	    amfree(holdp[0]);
	    l=j-1;
	}
    }

    /* copy assignedhd_s to sp, adjust allocated_space */
    for( ; holdp[i]; i++ ) {
        g_free(holdp[i]->destname);
	holdp[i]->destname = g_strconcat(holdingdisk_get_diskdir(holdp[i]->disk->hdisk),
	    "/", hd_driver_timestamp, "/", dp->host->hostname, ".", sfn, ".",
	    lvl, NULL);

	sp->holdp[j++] = holdp[i];
	holdp[i]->disk->allocated_space += holdp[i]->reserved;
	size = (holdp[i]->reserved > size) ? (off_t)0 :
		  (size - holdp[i]->reserved);
	qname = quote_string(dp->name);
	hold_debug(1,
		   _("assign_holdingdisk: %d assigning holding disk %s to disk %s:%s, reserved %lld, left %lld\n"),
		    i, holdingdisk_get_diskdir(holdp[i]->disk->hdisk),
		    dp->host->hostname, qname,
		    (long long)holdp[i]->reserved,
		    (long long)size);
	amfree(qname);
	holdp[i] = NULL; /* so it doesn't get free()d... */
    }
    sp->holdp[j] = NULL;
    amfree(sfn);

    return l;
}

static void
adjust_diskspace(
    sched_t *	sp,
    cmd_t	cmd)
{
    assignedhd_t **holdp;
    off_t total = (off_t)0;
    off_t diff;
    int i;
    char *qname, *hqname, *qdest;
    disk_t *dp = sp->disk;

    (void)cmd;	/* Quiet unused parameter warning */

    qname = quote_string(dp->name);
    qdest = quote_string(sp->destname);
    hold_debug(1, _("adjust_diskspace: %s:%s %s\n"),
		   dp->host->hostname, qname, qdest);

    holdp = sp->holdp;

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

    sp->act_size = total;

    hold_debug(1, _("adjust_diskspace: after: disk %s:%s used %lld\n"),
		   dp->host->hostname, qname,
		   (long long)sp->act_size);
    amfree(qdest);
    amfree(qname);
}

static void
delete_diskspace(
    sched_t *sp)
{
    assignedhd_t **holdp;
    int i;

    holdp = sp->holdp;

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
    free_assignedhd(sp->holdp);
    sp->holdp = NULL;
    sp->act_size = (off_t)0;
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
    sched_t *sp)
{
    time_t save_timestamp = sp->timestamp;
    /* setting timestamp to 0 removes the current level from the
     * database, so that we ensure that it will not be bumped to the
     * next level on the next run.  If we didn't do this, dumpdates or
     * gnutar-lists might have been updated already, and a bumped
     * incremental might be created.  */
    sp->timestamp = 0;
    update_info_dumper(sp, (off_t)-1, (off_t)-1, (time_t)-1);
    sp->timestamp = save_timestamp;
}

/* ------------------- */

static int
queue_length(
    schedlist_t	*q)
{
    if (!q) return 0;
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
	   network_free_kps(NULL),
	   (long long)holding_free_space());
    {
	taper_t *taper;
	int writing = 0;
	int down = TRUE;

	for (taper = tapetable; taper < tapetable + nb_storage ; taper++) {
	    wtaper_t *wtaper;
	    if (taper->storage_name && !taper->degraded_mode) {
		down = FALSE;
		for(wtaper = taper->wtapetable;
		    wtaper < taper->wtapetable + taper->nb_worker;
		    wtaper++) {
		    if (wtaper->state & TAPER_STATE_DUMP_TO_TAPE ||
			wtaper->state & TAPER_STATE_FILE_TO_TAPE ||
			wtaper->state & TAPER_STATE_VAULT_TO_TAPE)
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
	if (taper->storage_name) {
	    wtaper_t *wtaper;
	    GSList *vsl;
	    int nb_vault = 0;
	    for (vsl = taper->vaultqss; vsl != NULL; vsl = vsl->next) {
		nb_vault += queue_length((schedlist_t *)&((vaultqs_t *)vsl->data)->vaultq);
	    }
	    for (wtaper = taper->wtapetable;
		 wtaper < taper->wtapetable + taper->nb_worker;
		 wtaper++) {
		nb_vault += queue_length(&wtaper->vaultqs.vaultq);
	    }
	    g_printf(_(" tapeq %s: %d:%d"), taper->name, queue_length(&taper->tapeq), nb_vault);
	}
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
    char     **why_no_new_tape,
    gboolean   action_flush)
{
    TapeAction result = TAPE_ACTION_NO_ACTION;
    dumper_t *dumper;
    taper_t  *taper = wtaper->taper;
    wtaper_t *wtaper1;
    GList    *slist;
    sched_t  *sp;
    off_t dumpers_taper_size;		/* dumper size to taper */
    off_t dumpers_chunker_size;		/* dumper size to holding disk */
    off_t runq_size;
    off_t directq_size;
    off_t tapeq_size;
    off_t sched_size;
    off_t dump_to_disk_size;
    int   dump_to_disk_terminated;
    int   nb_wtaper_active = nb_sent_new_tape;
    int   nb_wtaper_flushing = 0;
    int   nb_wtaper_waiting = 0;
    int   nb_wtaper_init = 0;
    int   dle_free = 0;		/* number of dle that fit on started tape */
    int   new_dle = 0;		/* number of dle that doesn't fit on started tape */
    off_t new_data = 0;		/* size of dle that doesn't fit on started tape */
    off_t data_free = 0;	/* space available on started tape */
    off_t data_lost = 0;	/* space lost if do not use  a new tape */
    gboolean taperflush_criteria;
    gboolean flush_criteria;
    GSList *vsl;

    driver_debug(2, "tape_action: ENTER %s\n", wtaper->name);
    dumpers_taper_size = 0;
    dumpers_chunker_size = 0;
    for (dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if (dumper->busy && dumper->job->wtaper)
	    dumpers_taper_size += dumper->job->sched->est_size;
	if (dumper->busy && dumper->job->chunker)
	    dumpers_chunker_size += dumper->job->sched->est_size;
    }
    driver_debug(2, _("dumpers_taper_size: %lld\n"), (long long)dumpers_taper_size);
    driver_debug(2, _("dumpers_chunker_size: %lld\n"), (long long)dumpers_chunker_size);

    runq_size = 0;
    for (slist = runq.head; slist != NULL; slist = slist->next) {
	sp = get_sched(slist);
	runq_size += sp->est_size;
    }
    driver_debug(2, _("runq_size: %lld\n"), (long long)runq_size);

    directq_size = 0;
    for (slist = directq.head; slist != NULL; slist = slist->next) {
	sp = get_sched(slist);
	directq_size += sp->est_size;
    }
    driver_debug(2, _("directq_size: %lld\n"), (long long)directq_size);

    tapeq_size = directq_size;
    for (slist = taper->tapeq.head; slist != NULL; slist = slist->next) {
	sp = get_sched(slist);
	tapeq_size += sp->act_size;
    }
    for (vsl = taper->vaultqss; vsl != NULL; vsl = vsl->next) {
	for (slist = ((schedlist_t *)&((vaultqs_t *)vsl->data)->vaultq)->head; slist != NULL; slist = slist->next) {
	    sp = get_sched(slist);
	    tapeq_size += sp->act_size;
	}
    }
    for (wtaper1 = taper->wtapetable;
	 wtaper1 < taper->wtapetable + taper->nb_worker;
	 wtaper1++) {
	for (slist = wtaper1->vaultqs.vaultq.head; slist != NULL; slist = slist->next) {
	    sp = get_sched(slist);
	    tapeq_size += sp->act_size;
	}
    }

    for (wtaper1 = taper->wtapetable;
	 wtaper1 < taper->wtapetable + taper->nb_worker;
	 wtaper1++) {
	if (wtaper1->state & TAPER_STATE_FILE_TO_TAPE ||
	    wtaper1->state & TAPER_STATE_DUMP_TO_TAPE ||
	    wtaper1->state & TAPER_STATE_VAULT_TO_TAPE) {
	    nb_wtaper_flushing++;
	}
	if (wtaper1->state & TAPER_STATE_TAPE_STARTED &&
	    wtaper1->state & TAPER_STATE_IDLE) {
	    nb_wtaper_waiting++;
	}
	if (wtaper1->state & TAPER_STATE_RESERVATION &&
	    wtaper1->state & TAPER_STATE_IDLE &&
	    wtaper->nb_dle == 0) {
	    nb_wtaper_init++;
	}
    }

    /* Add what is currently written to tape and in the go. */
    new_data = 0;
    data_free = 0;
    data_lost = 0;
    for (wtaper1 = taper->wtapetable;
	 wtaper1 < taper->wtapetable + taper->nb_worker;
	 wtaper1++) {
	if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
	    dle_free += (taper->max_dle_by_volume - wtaper1->nb_dle);
	}
	    if (wtaper1->job && wtaper1->job->sched) {
		off_t data_to_go;
		off_t t_size;
		if (wtaper1->job->dumper) {
		    t_size = wtaper1->job->sched->est_size;
		} else {
		    t_size = wtaper1->job->sched->act_size;
		}
		data_to_go =  t_size - wtaper1->written;
		if (data_to_go > wtaper1->left) {
		    if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
			data_free -= data_to_go - wtaper1->left;
			data_lost += wtaper1->written + wtaper1->left;
		    } else {
			data_free -= data_to_go;
			data_lost += wtaper1->written;
		    }
		} else {
		    if (!(wtaper1->state & TAPER_STATE_TAPE_STARTED)) {
			data_free -= data_to_go;
		    } else {
			data_free += wtaper1->left - data_to_go;
		    }
		}
	    } else if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
		data_free += wtaper1->left;
	    }
    }

    if (data_free < 0) {
	new_data = -data_free;
	data_free = 0;
    }

    new_dle = queue_length(&taper->tapeq) + queue_length(&directq) - dle_free;
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
	    for (slist = taper->tapeq.head;
		 slist != NULL && new_dle > 0;
		 slist = slist->next, new_dle--) {
		sp = get_sched(slist);
		new_data += sp->act_size;
	    }
	}
    }
    driver_debug(2, _("new_data: %lld\n"), (long long)new_data);
    driver_debug(2, _("data_free: %lld\n"), (long long)data_free);
    driver_debug(2, _("data_lost: %lld\n"), (long long)data_lost);
;
    tapeq_size -= data_free;
    tapeq_size += new_data;
    driver_debug(2, _("tapeq_size: %lld\n"), (long long)tapeq_size);

    sched_size = runq_size + tapeq_size + dumpers_chunker_size + dumpers_taper_size;
    driver_debug(2, _("sched_size: %lld\n"), (long long)sched_size);

    dump_to_disk_size = dumpers_chunker_size + runq_size;
    driver_debug(2, _("dump_to_disk_size: %lld\n"), (long long)dump_to_disk_size);

    dump_to_disk_terminated = schedule_done && dump_to_disk_size == 0;

    for (wtaper1 = taper->wtapetable;
	 wtaper1 < taper->wtapetable + taper->nb_worker;
	 wtaper1++) {
	if (wtaper1->state & TAPER_STATE_TAPE_STARTED) {
	    nb_wtaper_active++;
	}
    }

    taperflush_criteria = (((taper->taperflush < tapeq_size) && dump_to_disk_terminated &&
			    (new_data > 0 || force_flush == 1 || dump_to_disk_terminated)) ||
			   ((data_lost > ( taper->tape_length - tapeq_size)) &&
			    (dump_to_disk_terminated)));
    flush_criteria = (taper->flush_threshold_dumped < tapeq_size &&
		      taper->flush_threshold_scheduled < sched_size &&
		      (new_data > 0 || force_flush == 1 || dump_to_disk_terminated)) ||
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
    driver_debug(2, "current_tape %d\n", taper->current_tape);
    driver_debug(2, "runtapes %d\n", taper->runtapes);
    driver_debug(2, "nb_scan_volume %d\n", taper->nb_scan_volume);
    driver_debug(2, "nb_wtaper_active %d\n", nb_wtaper_active);
    driver_debug(2, "last_started_wtaper %p %p %s\n", taper->last_started_wtaper, wtaper, wtaper->name);
    driver_debug(2, "taper->sent_first_write %p %p %s\n", taper->sent_first_write, wtaper, wtaper->name);
    driver_debug(2, "wtaper->state %d\n", wtaper->state);

driver_debug(2, "%d  R%d W%d D%d I%d\n", wtaper->state, TAPER_STATE_TAPE_REQUESTED, TAPER_STATE_WAIT_FOR_TAPE, TAPER_STATE_DUMP_TO_TAPE, TAPER_STATE_IDLE);
    // Changing conditionals can produce a driver hang, take care.
    //
    // when to start writing to a new tape
    if (wtaper->state & TAPER_STATE_TAPE_REQUESTED) {
	driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED\n");
	if (taper->current_tape >= taper->runtapes &&
	    taper->nb_scan_volume == 0 &&
	    nb_wtaper_active == 0 && taper->sent_first_write == NULL) {
	    *why_no_new_tape = g_strdup_printf(_("%d tapes filled; runtapes=%d "
		"does not allow additional tapes"), taper->current_tape,
		taper->runtapes);
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_NO_NEW_TAPE\n");
	    result |= TAPE_ACTION_NO_NEW_TAPE;
	} else if (nb_wtaper_init > 0 && wtaper->allow_take_scribe_from) {
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_MOVE (nb_wtaper_init > 0)\n");
	    result |= TAPE_ACTION_MOVE;
	} else if (taper->last_started_wtaper && taper->last_started_wtaper != wtaper && taper->last_started_wtaper != taper->sent_first_write && wtaper->allow_take_scribe_from) {
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_MOVE (last_started_wtaper)\n");
	    result |= TAPE_ACTION_MOVE;
	} else if (taper->current_tape < taper->runtapes &&
		   taper->nb_scan_volume == 0 &&
		    (
			//taper->sent_first_write == wtaper ||
		    flush_criteria || new_dle > 0 ||
		    !wtaper->allow_take_scribe_from ||
		    nb_wtaper_active == 0)) {
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_SCAN\n");
	    result |= TAPE_ACTION_SCAN;
	} else if (nb_wtaper_waiting > 0 && wtaper->allow_take_scribe_from) {
	    driver_debug(2, "tape_action: TAPER_STATE_TAPE_REQUESTED return TAPE_ACTION_MOVE (nb_wtaper_waiting)\n");
	    result |= TAPE_ACTION_MOVE;
	}
    } else if (wtaper->state & TAPER_STATE_WAIT_FOR_TAPE) {
	if ((wtaper->state & TAPER_STATE_DUMP_TO_TAPE) ||	// for dump to tape
	    !empty(directq) ||					// if a dle is waiting for a dump to tape
            !empty(roomq) ||					// holding disk constraint
            idle_reason == IDLE_NO_DISKSPACE ||			// holding disk constraint
	    //taper->sent_first_write == wtaper ||
	    flush_criteria || new_dle > 0			// flush criteria
	   ) {
	    driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE return TAPE_ACTION_NEW_TAPE\n");
	    result |= TAPE_ACTION_NEW_TAPE;
	// when to stop using new tape
	} else if ((taper->taperflush >= tapeq_size &&		// taperflush criteria
	           (force_flush == 1 ||				//  if force_flush
	            dump_to_disk_terminated))			//  or all dump to disk
	          ) {
	    driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE B\n");
	    if (nb_wtaper_active <= 0) {
		if (taper->current_tape >= taper->runtapes) {
		    *why_no_new_tape = g_strdup_printf(_("%d tapes filled; runtapes=%d "
		          "does not allow additional tapes"), taper->current_tape,
		          taper->runtapes);
		    driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE return TAPE_ACTION_NO_NEW_TAPE\n");
		    result |= TAPE_ACTION_NO_NEW_TAPE;
		} else if (dumpers_chunker_size <= 0) {
		    *why_no_new_tape = _("taperflush criteria not met");
		    driver_debug(2, "tape_action: TAPER_STATE_WAIT_FOR_TAPE return TAPE_ACTION_NO_NEW_TAPE\n");
		    result |= TAPE_ACTION_NO_NEW_TAPE;
		}
	    }
	}
    }

    // when to start a flush
    if (wtaper->state & TAPER_STATE_IDLE) {
	driver_debug(2, "tape_action: TAPER_STATE_IDLE\n");
	if (!taper->degraded_mode &&
	    ((wtaper->state & TAPER_STATE_TAPE_STARTED) ||
	     ((!empty(directq) ||
	       (((action_flush && !empty(taper->tapeq)) ||
		 (!action_flush && taper->vaultqss)) &&
	        (!empty(roomq) ||			// holding disk constraint
	         idle_reason == IDLE_NO_DISKSPACE ||	// holding disk constraint
	         flush_criteria))) &&			// flush
	      (taper->current_tape < taper->runtapes &&
	       taper->nb_scan_volume == 0 &&
	       taper->sent_first_write == NULL &&
	       (flush_criteria || new_dle > 0 ||
	        nb_wtaper_active == 0))))) {

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
    } else if (wtaper->state == TAPER_STATE_DEFAULT) {
	driver_debug(2, "tape_action: TAPER_STATE_DEFAULT\n");
	if (!taper->degraded_mode &&
	    (new_dle > 0 || new_data > 0)) {
	    driver_debug(2, "tape_action: TAPER_STATE_DEFAULT return TAPE_ACTION_START_TAPER\n");
                result |= TAPE_ACTION_START_TAPER;
	}
    } else {
	driver_debug(2, "tape_action: NO ACTION %d\n", wtaper->state);
    }
    return result;
}

static int
no_taper_flushing(void)
{
    taper_t  *taper;
    wtaper_t *wtaper;

    for (taper = tapetable; taper < tapetable + nb_storage; taper++) {
	if (taper->storage_name) {
	    for (wtaper = taper->wtapetable;
		wtaper < taper->wtapetable + taper->nb_worker;
		wtaper++) {
		if (wtaper->state & TAPER_STATE_FILE_TO_TAPE)
		    return 0;
	    }
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
    sched_t *sp)
{
    int         fd;
    size_t      buflen;
    char        buffer[DISK_BLOCK_BYTES];
    char       *read_buffer;
    dumpfile_t  file;
    disk_t     *dp = sp->disk;
    char       *f = getheaderfname(dp->host->hostname, dp->name, driver_timestamp, sp->level);

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
    file.orig_size = sp->origsize;
    file.native_crc = sp->native_crc;
    file.client_crc = sp->client_crc;
    file.server_crc = sp->server_crc;
    read_buffer = build_header(&file, NULL, DISK_BLOCK_BYTES);
    full_write(fd, read_buffer, strlen(read_buffer));
    dumpfile_free_data(&file);
    amfree(read_buffer);
    close(fd);
}

/*
 *  * put disk on end of queue
 *   */

static void
enqueue_sched(
    schedlist_t *list,
    sched_t *    sp)
{
    list->head = g_am_list_insert_after(list->head, list->tail, sp);
    if (list->tail) {
	list->tail = list->tail->next;
    } else {
	list->tail = list->head;
    }
}


/*
 *  * put disk on head of queue
 *   */

static void
headqueue_sched(
    schedlist_t *list,
    sched_t *    sp)
{
    list->head = g_list_prepend(list->head, sp);
    if (!list->tail) {
	list->tail = list->head;
    }
}

static void
insert_before_sched(
    schedlist_t *list,
    GList       *list_before,
    sched_t     *sp)
{
    list->head = g_list_insert_before(list->head, list_before, sp);
}

/*
 *  * check if disk is present in list. Return true if so, false otherwise.
 *   */

static int
find_sched(
    schedlist_t *list,
    sched_t     *sp)
{
    GList *glist = list->head;

    while (glist && glist->data != sp) {
	glist = glist->next;
    }
    return (glist && glist->data == sp);
}

/*
 *  * remove disk from front of queue
 *   */

static sched_t *
dequeue_sched(
    schedlist_t *list)
{
    sched_t *sp;

    if (list->head == NULL) return NULL;

    sp = list->head->data;
    list->head = g_list_delete_link(list->head, list->head);

    if (list->head == NULL) list->tail = NULL;

    return sp;
}

static void
remove_sched(
    schedlist_t *list,
    sched_t *    sp)
{
    GList *ltail;

    if (list->tail && list->tail->data == sp) {
	ltail = list->tail;
	list->tail = list->tail->prev;
	list->head = g_list_delete_link(list->head, ltail);
    } else {
	list->head = g_list_remove(list->head, sp);
    }
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
	   network_free_kps(NULL),
	   (long long)holding_free_space());
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
