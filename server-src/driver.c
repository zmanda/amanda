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
 * $Id: driver.c 6512 2007-05-24 17:00:24Z ian $
 *
 * controlling process for the Amanda backup system
 */

/*
 * XXX possibly modify tape queue to be cognizant of how much room is left on
 *     tape.  Probably not effective though, should do this in planner.
 */

#include "amanda.h"
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
static disklist_t tapeq;	// dle on holding disk waiting to be written
				//   to tape
static disklist_t roomq;	// dle waiting for more space on holding disk
static int pending_aborts;
static disk_t *taper_disk;
static int degraded_mode;
static off_t reserved_space;
static off_t total_disksize;
static char *dumper_program;
static char *chunker_program;
static int  inparallel;
static int nodump = 0;
static off_t tape_length = (off_t)0;
static off_t tape_left = (off_t)0;
static int current_tape = 0;
static int conf_taperalgo;
static int conf_runtapes;
static time_t sleep_time;
static int idle_reason;
static char *driver_timestamp;
static char *hd_driver_timestamp;
static am_host_t *flushhost = NULL;
static int need_degraded=0;
static holdalloc_t *holdalloc;
static int num_holdalloc;
static event_handle_t *dumpers_ev_time = NULL;
static event_handle_t *schedule_ev_read = NULL;
static int   conf_flush_threshold_dumped;
static int   conf_flush_threshold_scheduled;
static int   conf_taperflush;
static off_t flush_threshold_dumped;
static off_t flush_threshold_scheduled;
static off_t taperflush;
static int   schedule_done;			// 1 if we don't wait for a
						//   schedule from the planner
static int   force_flush;			// All dump are terminated, we
						// must now respect taper_flush

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
static void dump_to_tape(disk_t *dp);
static assignedhd_t **find_diskspace(off_t size, int *cur_idle,
					assignedhd_t *preferred);
static unsigned long free_kps(netif_t *ip);
static off_t free_space(void);
static void dumper_chunker_result(disk_t *dp);
static void dumper_taper_result(disk_t *dp);
static void file_taper_result(disk_t *dp);
static void handle_dumper_result(void *);
static void handle_chunker_result(void *);
static void handle_dumpers_time(void *);
static void handle_taper_result(void *);

static void holdingdisk_state(char *time_str);
static dumper_t *idle_dumper(void);
static void interface_state(char *time_str);
static int queue_length(disklist_t q);
static disklist_t read_flush(void);
static void read_schedule(void *cookie);
static void short_dump_state(void);
static void startaflush(void);
static void start_degraded_mode(disklist_t *queuep);
static void start_some_dumps(disklist_t *rq);
static void continue_port_dumps(void);
static void update_failed_dump(disk_t *);

typedef enum {
    TAPE_ACTION_NO_ACTION     = 0,
    TAPE_ACTION_NEW_TAPE      = (1 << 0),
    TAPE_ACTION_NO_NEW_TAPE   = (1 << 1),
    TAPE_ACTION_START_A_FLUSH = (1 << 2)
} TapeAction;

static TapeAction tape_action(char **why_no_new_tape);

static const char *idle_strings[] = {
#define NOT_IDLE		0
    T_("not-idle"),
#define IDLE_NO_DUMPERS		1
    T_("no-dumpers"),
#define IDLE_START_WAIT		2
    T_("start-wait"),
#define IDLE_NO_HOLD		3
    T_("no-hold"),
#define IDLE_CLIENT_CONSTRAINED	4
    T_("client-constrained"),
#define IDLE_NO_BANDWIDTH	5
    T_("no-bandwidth"),
#define IDLE_NO_DISKSPACE	6
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
    char *newdir = NULL;
    struct fs_usage fsusage;
    holdingdisk_t *hdp;
    identlist_t    il;
    unsigned long reserve = 100;
    char *conf_diskfile;
    cmd_t cmd;
    int result_argc;
    char **result_argv = NULL;
    char *taper_program;
    char *conf_tapetype;
    tapetype_t *tape;
    char *line;
    char hostname[1025];
    intmax_t kb_avail;
    config_overrides_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;
    holdalloc_t *ha, *ha_last;

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
    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, cfg_opt);
    apply_config_overrides(cfg_ovr);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &origq);
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

    if(argc > 2) {
        if(strncmp(argv[2], "nodump", 6) == 0) {
            nodump = 1;
        }
    }

    safe_cd(); /* do this *after* config_init */

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

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
    driver_timestamp = alloc(15);
    strncpy(driver_timestamp, &line[5], 14);
    driver_timestamp[14] = '\0';
    amfree(line);
    log_add(L_START,_("date %s"), driver_timestamp);

    gethostname(hostname, SIZEOF(hostname));
    log_add(L_STATS,_("hostname %s"), hostname);

    /* check that we don't do many dump in a day and usetimestamps is off */
    if(strlen(driver_timestamp) == 8) {
	if (!nodump) {
	    char *conf_logdir = getconf_str(CNF_LOGDIR);
	    char *logfile    = vstralloc(conf_logdir, "/log.",
					 driver_timestamp, ".0", NULL);
	    char *oldlogfile = vstralloc(conf_logdir, "/oldlog/log.",
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
	hd_driver_timestamp = stralloc(driver_timestamp);
    }

    taper_program = vstralloc(amlibexecdir, "/", "taper", NULL);
    dumper_program = vstralloc(amlibexecdir, "/", "dumper", NULL);
    chunker_program = vstralloc(amlibexecdir, "/", "chunker", NULL);

    conf_taperalgo = getconf_taperalgo(CNF_TAPERALGO);
    conf_tapetype = getconf_str(CNF_TAPETYPE);
    conf_runtapes = getconf_int(CNF_RUNTAPES);
    tape = lookup_tapetype(conf_tapetype);
    tape_length = tapetype_get_length(tape);
    g_printf("driver: tape size %lld\n", (long long)tape_length);
    conf_flush_threshold_dumped = getconf_int(CNF_FLUSH_THRESHOLD_DUMPED);
    conf_flush_threshold_scheduled = getconf_int(CNF_FLUSH_THRESHOLD_SCHEDULED);
    conf_taperflush = getconf_int(CNF_TAPERFLUSH);

    flush_threshold_dumped = (conf_flush_threshold_dumped * tape_length) / 100;
    flush_threshold_scheduled = (conf_flush_threshold_scheduled * tape_length) / 100;
    taperflush = (conf_taperflush *tape_length) / 100;

    driver_debug(1, _("flush_threshold_dumped: %lld\n"), (long long)flush_threshold_dumped);
    driver_debug(1, _("flush_threshold_scheduled: %lld\n"), (long long)flush_threshold_scheduled);
    driver_debug(1, _("taperflush: %lld\n"), (long long)taperflush);

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
	ha = alloc(SIZEOF(holdalloc_t));
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

	newdir = newvstralloc(newdir,
			      holdingdisk_get_diskdir(hdp), "/", hd_driver_timestamp,
			      NULL);
	if(!mkholdingdir(newdir)) {
	    ha->disksize = (off_t)0;
	}
	total_disksize += ha->disksize;
    }

    reserved_space = total_disksize * (off_t)(reserve / 100);

    g_printf(_("reserving %lld out of %lld for degraded-mode dumps\n"),
	   (long long)reserved_space, (long long)free_space());

    amfree(newdir);

    if(inparallel > MAX_DUMPERS) inparallel = MAX_DUMPERS;

    /* taper takes a while to get going, so start it up right away */

    init_driverio();
    if(conf_runtapes > 0) {
        startup_tape_process(taper_program);
        taper_cmd(START_TAPER, driver_timestamp, NULL, 0, NULL);
    }

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
    taper_state = TAPER_STATE_DEFAULT;
    tapeq = read_flush();

    roomq.head = roomq.tail = NULL;

    log_add(L_STATS, _("startup time %s"), walltime_str(curclock()));

    g_printf(_("driver: start time %s inparallel %d bandwidth %lu diskspace %lld "), walltime_str(curclock()), inparallel,
	   free_kps(NULL), (long long)free_space());
    g_printf(_(" dir %s datestamp %s driver: drain-ends tapeq %s big-dumpers %s\n"),
	   "OBSOLETE", driver_timestamp, taperalgo2str(conf_taperalgo),
	   getconf_str(CNF_DUMPORDER));
    fflush(stdout);

    /* ok, planner is done, now lets see if the tape is ready */

    if (conf_runtapes > 0) {
	cmd = getresult(taper, 1, &result_argc, &result_argv);
	if (cmd != TAPER_OK) {
	    /* no tape, go into degraded mode: dump to holding disk */
	    need_degraded = 1;
	}
    } else {
	need_degraded = 1;
    }

    tape_left = tape_length;
    taper_busy = 0;
    amfree(taper_input_error);
    amfree(taper_tape_error);
    taper_disk = NULL;
    taper_ev_read = NULL;

    schedule_done = nodump;
    force_flush = 0;

    if(!need_degraded) startaflush();

    if(!nodump)
	schedule_ev_read = event_register((event_id_t)0, EV_READFD, read_schedule, NULL);

    short_dump_state();
    event_loop(0);

    force_flush = 1;

    /* mv runq to directq */
    while (!empty(runq)) {
	diskp = dequeue_disk(&runq);
	headqueue_disk(&directq, diskp);
    }

    /* handle any remaining dumps by dumping directly to tape, if possible */
    while(!empty(directq) && taper > 0) {
	time_t  sleep_time  = 100000000;
	disk_t *sleep_diskp = NULL;
	time_t  now         = time(0);

	/* Find one we can do immediately or the sonner */
	for (diskp = directq.head; diskp != NULL; diskp = diskp->next) {
	    if (diskp->to_holdingdisk == HOLD_REQUIRED ||
		degraded_mode) {
		sleep_time = 0;
		sleep_diskp = diskp;
	    } else if (diskp->host->start_t - now < sleep_time &&
		       diskp->start_t -now < sleep_time) {
		if (diskp->host->start_t > diskp->start_t)
		    sleep_time = diskp->host->start_t - now;
		else
		    sleep_time = diskp->start_t - now;
		sleep_diskp = diskp;
	    }
	}
	diskp = sleep_diskp;
	if (sleep_time > 0)
	    sleep(sleep_time);
	remove_disk(&directq, diskp);

	if (diskp->to_holdingdisk == HOLD_REQUIRED) {
	    char *qname = quote_string(diskp->name);
	    log_add(L_FAIL, "%s %s %s %d [%s]",
		diskp->host->hostname, qname, sched(diskp)->datestamp,
		sched(diskp)->level,
		_("can't dump required holdingdisk"));
	    amfree(qname);
	}
	else if (!degraded_mode) {
	    taper_state |= TAPER_STATE_DUMP_TO_TAPE;
	    dump_to_tape(diskp);
	    event_loop(0);
	    taper_state &= ~TAPER_STATE_DUMP_TO_TAPE;
	}
	else {
	    char *qname = quote_string(diskp->name);
	    log_add(L_FAIL, "%s %s %s %d [%s]",
		diskp->host->hostname, qname, sched(diskp)->datestamp,
		sched(diskp)->level,
		num_holdalloc == 0 ?
		    _("can't do degraded dump without holding disk") :
		diskp->to_holdingdisk != HOLD_NEVER ?
		    _("out of holding space in degraded mode") :
	 	    _("can't dump 'holdingdisk never' dle in degraded mode"));
	    amfree(qname);
	}
    }

    /* fill up the tape or start new one for taperflush */
    startaflush();
    event_loop(0);

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

    if(taper >= 0) {
	taper_cmd(QUIT, NULL, NULL, 0, NULL);
    }

    /* wait for all to die */
    wait_children(600);

    /* cleanup */
    holding_cleanup(NULL, NULL);

    amfree(newdir);

    check_unfree_serial();
    g_printf(_("driver: FINISHED time %s\n"), walltime_str(curclock()));
    fflush(stdout);
    log_add(L_FINISH,_("date %s time %s"), driver_timestamp, walltime_str(curclock()));
    log_add(L_INFO, "pid-done %ld", (long)getpid());
    amfree(driver_timestamp);

    amfree(dumper_program);
    amfree(taper_program);
    if (result_argv)
	g_strfreev(result_argv);

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
			who = stralloc(dumper->name);
			dumper->pid = -1;
			break;
		    }
		    if (dumper->chunker && pid == dumper->chunker->pid) {
			who = stralloc(dumper->chunker->name);
			dumper->chunker->pid = -1;
			break;
		    }
		}
		if (who == NULL && pid == taper_pid) {
		    who = stralloc("taper");
		    taper_pid = -1;
		}
		if(what != NULL && who == NULL) {
		    who = stralloc("unknown");
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

    if(!nodump) {
        for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	    if (!dumper->down && dumper->pid > 1) {
		g_printf(_("driver: sending signal %d to %s pid %u\n"), signal,
		       dumper->name, (unsigned)dumper->pid);
		if (kill(dumper->pid, signal) == -1 && errno == ESRCH) {
		    if (dumper->chunker)
			dumper->chunker->pid = 0;
		}
		if (dumper->chunker && dumper->chunker->pid > 1) {
		    g_printf(_("driver: sending signal %d to %s pid %u\n"), signal,
			   dumper->chunker->name,
			   (unsigned)dumper->chunker->pid);
		    if (kill(dumper->chunker->pid, signal) == -1 &&
			errno == ESRCH)
			dumper->chunker->pid = 0;
		}
	    }
        }
    }

    if(taper_pid > 1) {
	g_printf(_("driver: sending signal %d to %s pid %u\n"), signal,
	       "taper", (unsigned)taper_pid);
	if (kill(taper_pid, signal) == -1 && errno == ESRCH)
	    taper_pid = 0;
    }
}

static void
wait_for_children(void)
{
    dumper_t *dumper;

    if(!nodump) {
	for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	    if (dumper->pid > 1 && dumper->fd >= 0) {
		dumper_cmd(dumper, QUIT, NULL, NULL);
		if (dumper->chunker && dumper->chunker->pid > 1 &&
		    dumper->chunker->fd >= 0)
		    chunker_cmd(dumper->chunker, QUIT, NULL, NULL);
	    }
	}
    }

    if(taper_pid > 1 && taper > 0) {
	taper_cmd(QUIT, NULL, NULL, 0, NULL);
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
startaflush(void)
{
    disk_t *dp = NULL;
    disk_t *fit = NULL;
    char *datestamp;
    int extra_tapes = 0;
    char *qname;
    TapeAction result_tape_action;
    char *why_no_new_tape;

    result_tape_action = tape_action(&why_no_new_tape);

    if (result_tape_action & TAPE_ACTION_NEW_TAPE) {
	taper_state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	taper_cmd(NEW_TAPE, NULL, NULL, 0, NULL);
    } else if (result_tape_action & TAPE_ACTION_NO_NEW_TAPE) {
	taper_state &= ~TAPER_STATE_WAIT_FOR_TAPE;
	taper_cmd(NO_NEW_TAPE, why_no_new_tape, NULL, 0, NULL);
	start_degraded_mode(&runq);
    }

    if (!degraded_mode && !taper_busy && !empty(tapeq) &&
	(result_tape_action & TAPE_ACTION_START_A_FLUSH)) {
	
	datestamp = sched(tapeq.head)->datestamp;
	switch(conf_taperalgo) {
	case ALGO_FIRST:
		dp = dequeue_disk(&tapeq);
		break;
	case ALGO_FIRSTFIT:
		fit = tapeq.head;
		while (fit != NULL) {
		    extra_tapes = (fit->tape_splitsize > (off_t)0) ? 
					conf_runtapes - current_tape : 0;
		    if(sched(fit)->act_size <= (tape_left +
		             tape_length * (off_t)extra_tapes) &&
			     strcmp(sched(fit)->datestamp, datestamp) <= 0) {
			dp = fit;
			fit = NULL;
		    }
		    else {
			fit = fit->next;
		    }
		}
		if(dp) remove_disk(&tapeq, dp);
		break;
	case ALGO_LARGEST:
		fit = dp = tapeq.head;
		while (fit != NULL) {
		    if(sched(fit)->act_size > sched(dp)->act_size &&
		       strcmp(sched(fit)->datestamp, datestamp) <= 0) {
			dp = fit;
		    }
		    fit = fit->next;
		}
		if(dp) remove_disk(&tapeq, dp);
		break;
	case ALGO_LARGESTFIT:
		fit = tapeq.head;
		while (fit != NULL) {
		    extra_tapes = (fit->tape_splitsize > (off_t)0) ? 
					conf_runtapes - current_tape : 0;
		    if(sched(fit)->act_size <=
		       (tape_left + tape_length * (off_t)extra_tapes) &&
		       (!dp || sched(fit)->act_size > sched(dp)->act_size) &&
		       strcmp(sched(fit)->datestamp, datestamp) <= 0) {
			dp = fit;
		    }
		    fit = fit->next;
		}
		if(dp) remove_disk(&tapeq, dp);
		break;
	case ALGO_SMALLEST:
		break;
	case ALGO_LAST:
		dp = tapeq.tail;
		remove_disk(&tapeq, dp);
		break;
	}
	if(!dp) { /* ALGO_SMALLEST, or default if nothing fit. */
	    if(conf_taperalgo != ALGO_SMALLEST)  {
		g_fprintf(stderr,
		   _("driver: startaflush: Using SMALLEST because nothing fit\n"));
	    }
	    fit = dp = tapeq.head;
	    while (fit != NULL) {
		if(sched(fit)->act_size < sched(dp)->act_size &&
		   strcmp(sched(fit)->datestamp, datestamp) <= 0) {
		    dp = fit;
		}
	        fit = fit->next;
	    }
	    if(dp) remove_disk(&tapeq, dp);
	}
	if(taper_ev_read == NULL) {
	    taper_ev_read = event_register((event_id_t)taper, EV_READFD,
					   handle_taper_result, NULL);
	}
	if (dp) {
	    taper_disk = dp;
	    taper_busy = 1;
	    amfree(taper_input_error);
	    amfree(taper_tape_error);
	    taper_result = LAST_TOK;
	    taper_sendresult = 0;
	    taper_first_label = NULL;
	    taper_written = 0;
	    taper_state &= ~TAPER_STATE_DUMP_TO_TAPE;
	    taper_dumper = NULL;
	    qname = quote_string(dp->name);
	    taper_cmd(FILE_WRITE, dp, sched(dp)->destname, sched(dp)->level,
		      sched(dp)->datestamp);
	    g_fprintf(stderr,_("driver: startaflush: %s %s %s %lld %lld\n"),
		    taperalgo2str(conf_taperalgo), dp->host->hostname, qname,
		    (long long)sched(taper_disk)->act_size,
		    (long long)tape_left);
	    if(sched(dp)->act_size <= tape_left)
		tape_left -= sched(dp)->act_size;
	    else
		tape_left = (off_t)0;
	    amfree(qname);
	} else {
	    error(_("FATAL: Taper marked busy and no work found."));
	    /*NOTREACHED*/
	}
	short_dump_state();
    } else if(!taper_busy && taper_ev_read != NULL) {
	event_release(taper_ev_read);
	taper_ev_read = NULL;
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
start_some_dumps(
    disklist_t *	rq)
{
    int cur_idle;
    disk_t *diskp, *delayed_diskp, *diskp_accept;
    disk_t *dp;
    assignedhd_t **holdp=NULL, **holdp_accept;
    const time_t now = time(NULL);
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    chunker_t *chunker;
    dumper_t *dumper;
    char dumptype;
    char *dumporder;
    int  busy_dumpers = 0;

    idle_reason = IDLE_NO_DUMPERS;
    sleep_time = 0;

    if(dumpers_ev_time != NULL) {
	event_release(dumpers_ev_time);
	dumpers_ev_time = NULL;
    }

    for(dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if( dumper->busy ) {
	    busy_dumpers++;
	}
    }

    for (dumper = dmptable; dumper < dmptable+inparallel; dumper++) {

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

	for(diskp = rq->head; diskp != NULL; diskp = diskp->next) {
	    assert(diskp->host != NULL && sched(diskp) != NULL);

	    if (diskp->host->start_t > now) {
		cur_idle = max(cur_idle, IDLE_START_WAIT);
		if (delayed_diskp == NULL || sleep_time > diskp->host->start_t) {
		    delayed_diskp = diskp;
		    sleep_time = diskp->host->start_t;
		}
	    } else if(diskp->start_t > now) {
		cur_idle = max(cur_idle, IDLE_START_WAIT);
		if (delayed_diskp == NULL || sleep_time > diskp->start_t) {
		    delayed_diskp = diskp;
		    sleep_time = diskp->start_t;
		}
	    } else if (diskp->host->netif->curusage > 0 &&
		       sched(diskp)->est_kps > free_kps(diskp->host->netif)) {
		cur_idle = max(cur_idle, IDLE_NO_BANDWIDTH);
	    } else if(sched(diskp)->no_space) {
		cur_idle = max(cur_idle, IDLE_NO_DISKSPACE);
	    } else if (diskp->to_holdingdisk == HOLD_NEVER) {
		cur_idle = max(cur_idle, IDLE_NO_HOLD);
	    } else if ((holdp =
		find_diskspace(sched(diskp)->est_size, &cur_idle, NULL)) == NULL) {
		cur_idle = max(cur_idle, IDLE_NO_DISKSPACE);
		if (empty(tapeq) && busy_dumpers == 0) {
		    remove_disk(rq, diskp);
		    enqueue_disk(&directq, diskp);
		}
	    } else if (client_constrained(diskp)) {
		free_assignedhd(holdp);
		cur_idle = max(cur_idle, IDLE_CLIENT_CONSTRAINED);
	    } else {

		/* disk fits, dump it */
		int accept = !diskp_accept;
		if(!accept) {
		    switch(dumptype) {
		      case 's': accept = (sched(diskp)->est_size < sched(diskp_accept)->est_size);
				break;
		      case 'S': accept = (sched(diskp)->est_size > sched(diskp_accept)->est_size);
				break;
		      case 't': accept = (sched(diskp)->est_time < sched(diskp_accept)->est_time);
				break;
		      case 'T': accept = (sched(diskp)->est_time > sched(diskp_accept)->est_time);
				break;
		      case 'b': accept = (sched(diskp)->est_kps < sched(diskp_accept)->est_kps);
				break;
		      case 'B': accept = (sched(diskp)->est_kps > sched(diskp_accept)->est_kps);
				break;
		      default:  log_add(L_WARNING, _("Unknown dumporder character \'%c\', using 's'.\n"),
					dumptype);
				accept = (sched(diskp)->est_size < sched(diskp_accept)->est_size);
				break;
		    }
		}
		if(accept) {
		    if( !diskp_accept || !degraded_mode || diskp->priority >= diskp_accept->priority) {
			if(holdp_accept) free_assignedhd(holdp_accept);
			diskp_accept = diskp;
			holdp_accept = holdp;
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

	diskp = diskp_accept;
	holdp = holdp_accept;

	idle_reason = max(idle_reason, cur_idle);

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
	} else if (diskp != NULL) {
	    sched(diskp)->act_size = (off_t)0;
	    allocate_bandwidth(diskp->host->netif, sched(diskp)->est_kps);
	    sched(diskp)->activehd = assign_holdingdisk(holdp, diskp);
	    amfree(holdp);
	    sched(diskp)->destname = newstralloc(sched(diskp)->destname,
						 sched(diskp)->holdp[0]->destname);
	    diskp->host->inprogress++;	/* host is now busy */
	    diskp->inprogress = 1;
	    sched(diskp)->dumper = dumper;
	    sched(diskp)->timestamp = now;

	    dumper->busy = 1;		/* dumper is now busy */
	    dumper->dp = diskp;		/* link disk to dumper */
	    remove_disk(rq, diskp);		/* take it off the run queue */

	    sched(diskp)->origsize = (off_t)-1;
	    sched(diskp)->dumpsize = (off_t)-1;
	    sched(diskp)->dumptime = (time_t)0;
	    sched(diskp)->tapetime = (time_t)0;
	    chunker = dumper->chunker;
	    chunker->result = LAST_TOK;
	    dumper->result = LAST_TOK;
	    startup_chunk_process(chunker,chunker_program);
	    chunker_cmd(chunker, START, NULL, driver_timestamp);
	    chunker->dumper = dumper;
	    chunker_cmd(chunker, PORT_WRITE, diskp, NULL);
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
		sched(diskp)->dumper = NULL;
		dumper->busy = 0;
		dumper->dp = NULL;
		sched(diskp)->dump_attempted++;
		free_serial_dp(diskp);
		if(sched(diskp)->dump_attempted < 2)
		    enqueue_disk(rq, diskp);
	    }
	    else {
		dumper->ev_read = event_register((event_id_t)dumper->fd, EV_READFD,
						 handle_dumper_result, dumper);
		chunker->ev_read = event_register((event_id_t)chunker->fd, EV_READFD,
						   handle_chunker_result, chunker);
		dumper->output_port = atoi(result_argv[1]);

		if (diskp->host->pre_script == 0) {
		    for (dp=diskp->host->disks; dp != NULL; dp = dp->hostnext) {
			run_server_scripts(EXECUTE_ON_PRE_HOST_BACKUP,
					   get_config_name(), dp, -1);
		    }
		    diskp->host->pre_script = 1;
		}
		run_server_scripts(EXECUTE_ON_PRE_DLE_BACKUP,
				   get_config_name(), diskp,
				   sched(diskp)->level);
		dumper_cmd(dumper, PORT_DUMP, diskp, NULL);
	    }
	    diskp->host->start_t = now + 15;

	    if (result_argv)
		g_strfreev(result_argv);
	    short_dump_state();
	}
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
    disk_t *dp;
    char *qname;

    g_printf(_("dump of driver schedule %s:\n--------\n"), str);

    for(dp = qp->head; dp != NULL; dp = dp->next) {
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
	    if (reserved_space + est_full_size + sched(dp)->est_size
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
    degraded_mode = 1;

    dump_schedule(queuep, _("after start degraded mode"));
}


static void
continue_port_dumps(void)
{
    disk_t *dp, *ndp;
    assignedhd_t **h;
    int active_dumpers=0, busy_dumpers=0, i;
    dumper_t *dumper;

    /* First we try to grant diskspace to some dumps waiting for it. */
    for( dp = roomq.head; dp; dp = ndp ) {
	ndp = dp->next;
	/* find last holdingdisk used by this dump */
	for( i = 0, h = sched(dp)->holdp; h[i+1]; i++ ) {
	    (void)h; /* Quiet lint */
	}
	/* find more space */
	h = find_diskspace( sched(dp)->est_size - sched(dp)->act_size,
			    &active_dumpers, h[i] );
	if( h ) {
	    for(dumper = dmptable; dumper < dmptable + inparallel &&
				   dumper->dp != dp; dumper++) {
		(void)dp; /* Quiet lint */
	    }
	    assert( dumper < dmptable + inparallel );
	    sched(dp)->activehd = assign_holdingdisk( h, dp );
	    chunker_cmd( dumper->chunker, CONTINUE, dp, NULL );
	    amfree(h);
	    remove_disk( &roomq, dp );
	}
    }

    /* So for some disks there is less holding diskspace available than
     * was asked for. Possible reasons are
     * a) diskspace has been allocated for other dumps which are
     *    still running or already being written to tape
     * b) all other dumps have been suspended due to lack of diskspace
     * c) this dump doesn't fit on all the holding disks
     * Case a) is not a problem. We just wait for the diskspace to
     * be freed by moving the current disk to a queue.
     * If case b) occurs, we have a deadlock situation. We select
     * a dump from the queue to be aborted and abort it. It will
     * be retried later dumping to disk.
     * If case c) is detected, the dump is aborted. Next time
     * it will be dumped directly to tape. Actually, case c is a special
     * manifestation of case b) where only one dumper is busy.
     */
    for(dp=NULL, dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if( dumper->busy ) {
	    busy_dumpers++;
	    if( !find_disk(&roomq, dumper->dp) ) {
		active_dumpers++;
	    } else if( !dp || 
		       sched(dp)->est_size > sched(dumper->dp)->est_size ) {
		dp = dumper->dp;
	    }
	}
    }
    if((dp != NULL) && (active_dumpers == 0) && (busy_dumpers > 0) && 
        ((!taper_busy && empty(tapeq)) || degraded_mode) &&
	pending_aborts == 0 ) { /* not case a */
	if( busy_dumpers == 1 ) { /* case c */
	    sched(dp)->no_space = 1;
	}
	/* case b */
	/* At this time, dp points to the dump with the smallest est_size.
	 * We abort that dump, hopefully not wasting too much time retrying it.
	 */
	remove_disk( &roomq, dp );
	chunker_cmd(sched(dp)->dumper->chunker, ABORT, NULL, _("Not enough holding disk space"));
	dumper_cmd( sched(dp)->dumper, ABORT, NULL, _("Not enough holding disk space"));
	pending_aborts++;
    }
}


static void
handle_taper_result(
	void *cookie G_GNUC_UNUSED)
{
    disk_t *dp;
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    char *qname, *q;
    char *s;

    assert(cookie == NULL);
    amfree(taper_input_error);
    amfree(taper_tape_error);
    
    do {
        
	short_dump_state();
        
	cmd = getresult(taper, 1, &result_argc, &result_argv);
        
	switch(cmd) {
            
	case FAILED:	/* FAILED <handle> INPUT-* TAPE-* <input err mesg> <tape err mesg> */
	    if(result_argc != 6) {
		error(_("error: [taper FAILED result_argc != 6: %d"), result_argc);
		/*NOTREACHED*/
	    }
            
	    dp = serial2disk(result_argv[1]);
	    assert(dp == taper_disk);
	    if (!taper_dumper)
		free_serial(result_argv[1]);
            
	    qname = quote_string(dp->name);
	    g_printf(_("driver: finished-cmd time %s taper wrote %s:%s\n"),
		   walltime_str(curclock()), dp->host->hostname, qname);
	    fflush(stdout);

	    if (strcmp(result_argv[2], "INPUT-ERROR") == 0) {
		taper_input_error = newstralloc(taper_input_error, result_argv[4]);
	    } else if (strcmp(result_argv[2], "INPUT-GOOD") != 0) {
		taper_tape_error = newstralloc(taper_tape_error,
					       _("Taper protocol error"));
		taper_result = FAILED;
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, taper_tape_error);
		amfree(qname);
		break;
	    }
	    if (strcmp(result_argv[3], "TAPE-ERROR") == 0) {
		taper_tape_error = newstralloc(taper_tape_error, result_argv[5]);
	    } else if (strcmp(result_argv[3], "TAPE-GOOD") != 0) {
		taper_tape_error = newstralloc(taper_tape_error,
					       _("Taper protocol error"));
		taper_result = FAILED;
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, taper_tape_error);
		amfree(qname);
		break;
	    }

	    amfree(qname);
	    taper_result = cmd;

	    break;
            
	case PARTIAL:	/* PARTIAL <handle> INPUT-* TAPE-* <stat mess> <input err mesg> <tape err mesg>*/
	case DONE:	/* DONE <handle> INPUT-GOOD TAPE-GOOD <stat mess> <input err mesg> <tape err mesg> */
	    if(result_argc != 7) {
		error(_("error: [taper PARTIAL result_argc != 7: %d"), result_argc);
		/*NOTREACHED*/
	    }
            
	    dp = serial2disk(result_argv[1]);
	    assert(dp == taper_disk);
            if (!taper_dumper)
                free_serial(result_argv[1]);

	    qname = quote_string(dp->name);
	    g_printf(_("driver: finished-cmd time %s taper wrote %s:%s\n"),
		   walltime_str(curclock()), dp->host->hostname, qname);
	    fflush(stdout);

	    if (strcmp(result_argv[2], "INPUT-ERROR") == 0) {
		taper_input_error = newstralloc(taper_input_error, result_argv[5]);
	    } else if (strcmp(result_argv[2], "INPUT-GOOD") != 0) {
		taper_tape_error = newstralloc(taper_tape_error,
					       _("Taper protocol error"));
		taper_result = FAILED;
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, taper_tape_error);
		amfree(qname);
		break;
	    }
	    if (strcmp(result_argv[3], "TAPE-ERROR") == 0) {
		taper_tape_error = newstralloc(taper_tape_error, result_argv[6]);
	    } else if (strcmp(result_argv[3], "TAPE-GOOD") != 0) {
		taper_tape_error = newstralloc(taper_tape_error,
					       _("Taper protocol error"));
		taper_result = FAILED;
		log_add(L_FAIL, _("%s %s %s %d [%s]"),
		        dp->host->hostname, qname, sched(dp)->datestamp,
		        sched(dp)->level, taper_tape_error);
		amfree(qname);
		break;
	    }

	    s = strstr(result_argv[4], " kb ");
	    if (s) {
		s += 4;
		sched(dp)->dumpsize = atol(s);
	    }

	    taper_result = cmd;
	    amfree(qname);

	    break;
            
        case PARTDONE:  /* PARTDONE <handle> <label> <fileno> <kbytes> <stat> */
	    dp = serial2disk(result_argv[1]);
	    assert(dp == taper_disk);
            if (result_argc != 6) {
                error(_("error [taper PARTDONE result_argc != 6: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }
	    if (!taper_first_label) {
		taper_first_label = stralloc(result_argv[2]);
		taper_first_fileno = OFF_T_ATOI(result_argv[3]);
	    }
	    taper_written = OFF_T_ATOI(result_argv[4]);
	    if (taper_written > sched(taper_disk)->act_size)
		sched(taper_disk)->act_size = taper_written;
            
            break;

        case REQUEST_NEW_TAPE:  /* REQUEST-NEW-TAPE <handle> */
            if (result_argc != 2) {
                error(_("error [taper REQUEST_NEW_TAPE result_argc != 2: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }
	    taper_state &= ~TAPER_STATE_TAPE_STARTED;

	    if (current_tape >= conf_runtapes) {
		char *usermsg = g_strdup_printf(_("%d tapes filled; runtapes=%d "
		    "does not allow additional tapes"), current_tape, conf_runtapes);
		taper_cmd(NO_NEW_TAPE, usermsg, NULL, 0, NULL);
		g_free(usermsg);
		log_add(L_WARNING,
			_("Out of tapes; going into degraded mode."));
		start_degraded_mode(&runq);
	    } else {
		TapeAction result_tape_action;
		char *why_no_new_tape;

		taper_state |= TAPER_STATE_WAIT_FOR_TAPE;
		result_tape_action = tape_action(&why_no_new_tape);
		if (result_tape_action & TAPE_ACTION_NEW_TAPE) {
		    taper_cmd(NEW_TAPE, NULL, NULL, 0, NULL);
		    taper_state &= ~TAPER_STATE_WAIT_FOR_TAPE;
		} else if (result_tape_action & TAPE_ACTION_NO_NEW_TAPE) {
		    taper_cmd(NO_NEW_TAPE, why_no_new_tape, NULL, 0, NULL);
		    taper_state &= ~TAPER_STATE_WAIT_FOR_TAPE;
		    start_degraded_mode(&runq);
		}
	    }
	    break;

	case NEW_TAPE: /* NEW-TAPE <handle> <label> */
            if (result_argc != 3) {
                error(_("error [taper NEW_TAPE result_argc != 3: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }

            /* Update our tape counter and reset tape_left */
	    current_tape++;
	    tape_left = tape_length;
	    taper_state |= TAPER_STATE_TAPE_STARTED;
	    break;

	case NO_NEW_TAPE:  /* NO-NEW-TAPE <handle> */
            if (result_argc != 2) {
                error(_("error [taper NO_NEW_TAPE result_argc != 2: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }
	    start_degraded_mode(&runq);
	    break;

	case DUMPER_STATUS:  /* DUMPER-STATUS <handle> */
            if (result_argc != 2) {
                error(_("error [taper NO_NEW_TAPE result_argc != 2: %d]"),
                      result_argc);
		/*NOTREACHED*/
            }
	    if (taper_dumper->result == LAST_TOK) {
		taper_sendresult = 1;
	    } else {
		if( taper_dumper->result == DONE) {
		    taper_cmd(DONE, NULL, NULL, 0, NULL);
		} else {
		    taper_cmd(FAILED, NULL, NULL, 0, NULL);
		}
	    }
	    break;

        case TAPE_ERROR: /* TAPE-ERROR <handle> <err mess> */
            dp = serial2disk(result_argv[1]);
	    if (!taper_dumper)
		free_serial(result_argv[1]);
	    qname = quote_string(dp->name);
            g_printf(_("driver: finished-cmd time %s taper wrote %s:%s\n"),
                   walltime_str(curclock()), dp->host->hostname, qname);
	    amfree(qname);
            fflush(stdout);
	    q = quote_string(result_argv[2]);
            log_add(L_WARNING, _("Taper error: %s"), q);
	    amfree(q);
	    taper_tape_error = newstralloc(taper_tape_error, result_argv[2]);
            /*FALLTHROUGH*/

        case BOGUS:
            if (cmd == BOGUS) {
        	log_add(L_WARNING, _("Taper protocol error"));
		taper_tape_error = newstralloc(taper_tape_error, "BOGUS");
            }
            /*
             * Since we received a taper error, we can't send anything more
             * to the taper.  Go into degraded mode to try to get everthing
             * onto disk.  Later, these dumps can be flushed to a new tape.
             * The tape queue is zapped so that it appears empty in future
             * checks. If there are dumps waiting for diskspace to be freed,
             * cancel one.
             */
            if(!nodump) {
                log_add(L_WARNING,
                        _("going into degraded mode because of taper component error."));
	    }
	    start_degraded_mode(&runq);
            tapeq.head = tapeq.tail = NULL;
            taper_busy = 0;
            if(taper_ev_read != NULL) {
                event_release(taper_ev_read);
                taper_ev_read = NULL;
            }
            if(cmd != TAPE_ERROR) aclose(taper);
	    taper_result = cmd;

            break;

	default:
            error(_("driver received unexpected token (%s) from taper"),
                  cmdstr[cmd]);
	    /*NOTREACHED*/
	}

	g_strfreev(result_argv);

	if (taper_result != LAST_TOK) {
	    if(taper_dumper) {
		if (taper_dumper->result != LAST_TOK) {
		    // Dumper already returned it's result
		    dumper_taper_result(taper_disk);
		}
	    } else {
		file_taper_result(taper_disk);
	    }
	}
        
    } while(areads_dataready(taper));
}


static void
file_taper_result(
    disk_t *dp)
{
    char *qname = quote_string(dp->name);

    if (taper_result == DONE) {
	update_info_taper(dp, taper_first_label, taper_first_fileno,
			  sched(dp)->level);
    }

    sched(dp)->taper_attempted += 1;

    if (taper_input_error) {
	g_printf("driver: taper failed %s %s: %s\n",
		   dp->host->hostname, qname, taper_input_error);
	if (strcmp(sched(dp)->datestamp, driver_timestamp) == 0) {
	    if(sched(dp)->taper_attempted >= 2) {
		log_add(L_FAIL, _("%s %s %s %d [too many taper retries after holding disk error: %s]"),
		    dp->host->hostname, qname, sched(dp)->datestamp,
		    sched(dp)->level, taper_input_error);
		g_printf("driver: taper failed %s %s, too many taper retry after holding disk error\n",
		   dp->host->hostname, qname);
		amfree(sched(dp)->destname);
		amfree(sched(dp)->dumpdate);
		amfree(sched(dp)->degr_dumpdate);
		amfree(sched(dp)->degr_mesg);
		amfree(sched(dp)->datestamp);
		amfree(dp->up);
	    } else {
		log_add(L_INFO, _("%s %s %s %d [Will retry dump because of holding disk error: %s]"),
			dp->host->hostname, qname, sched(dp)->datestamp,
			sched(dp)->level, taper_input_error);
		g_printf("driver: taper will retry %s %s because of holding disk error\n",
			dp->host->hostname, qname);
		if (dp->to_holdingdisk != HOLD_REQUIRED) {
		    dp->to_holdingdisk = HOLD_NEVER;
		    sched(dp)->dump_attempted -= 1;
		    headqueue_disk(&directq, dp);
		} else {
		    amfree(sched(dp)->destname);
		    amfree(sched(dp)->dumpdate);
		    amfree(sched(dp)->degr_dumpdate);
		    amfree(sched(dp)->degr_mesg);
		    amfree(sched(dp)->datestamp);
		    amfree(dp->up);
		}
	    }
	} else {
	    amfree(sched(dp)->destname);
	    amfree(sched(dp)->dumpdate);
	    amfree(sched(dp)->degr_dumpdate);
	    amfree(sched(dp)->degr_mesg);
	    amfree(sched(dp)->datestamp);
	    amfree(dp->up);
	}
    } else if (taper_tape_error) {
	g_printf("driver: taper failed %s %s with tape error: %s\n",
		   dp->host->hostname, qname, taper_tape_error);
	if(sched(dp)->taper_attempted >= 2) {
	    log_add(L_FAIL, _("%s %s %s %d [too many taper retries]"),
		    dp->host->hostname, qname, sched(dp)->datestamp,
		    sched(dp)->level);
	    g_printf("driver: taper failed %s %s, too many taper retry\n",
		   dp->host->hostname, qname);
	    amfree(sched(dp)->destname);
	    amfree(sched(dp)->dumpdate);
	    amfree(sched(dp)->degr_dumpdate);
	    amfree(sched(dp)->degr_mesg);
	    amfree(sched(dp)->datestamp);
	    amfree(dp->up);
	} else {
	    g_printf("driver: taper will retry %s %s\n",
		   dp->host->hostname, qname);
	    /* Re-insert into taper queue. */
	    headqueue_disk(&tapeq, dp);
	}
    } else if (taper_result != DONE) {
	g_printf("driver: taper failed %s %s without error\n",
		   dp->host->hostname, qname);
    } else {
	delete_diskspace(dp);
	amfree(sched(dp)->destname);
	amfree(sched(dp)->dumpdate);
	amfree(sched(dp)->degr_dumpdate);
	amfree(sched(dp)->degr_mesg);
	amfree(sched(dp)->datestamp);
	amfree(dp->up);
    }

    amfree(qname);

    taper_busy = 0;
    amfree(taper_input_error);
    amfree(taper_tape_error);
    taper_disk = NULL;
            
    /* continue with those dumps waiting for diskspace */
    continue_port_dumps();
    start_some_dumps(&runq);
    startaflush();
}

static void
dumper_taper_result(
    disk_t *dp)
{
    dumper_t *dumper;
    int is_partial;
    char *qname;

    dumper = sched(dp)->dumper;

    free_serial_dp(dp);
    if(dumper->result == DONE && taper_result == DONE) {
	update_info_dumper(dp, sched(dp)->origsize,
			   sched(dp)->dumpsize, sched(dp)->dumptime);
	update_info_taper(dp, taper_first_label, taper_first_fileno,
			  sched(dp)->level);
	qname = quote_string(dp->name); /*quote to take care of spaces*/

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

    is_partial = dumper->result != DONE || taper_result != DONE;

    sched(dp)->dump_attempted += 1;
    sched(dp)->taper_attempted += 1;

    if((dumper->result != DONE || taper_result != DONE) &&
	sched(dp)->dump_attempted <= 1 &&
	sched(dp)->taper_attempted <= 1) {
	enqueue_disk(&directq, dp);
    }

    if(dumper->ev_read != NULL) {
	event_release(dumper->ev_read);
	dumper->ev_read = NULL;
    }
    if(taper_ev_read != NULL) {
	event_release(taper_ev_read);
	taper_ev_read = NULL;
    }
    taper_busy = 0;
    amfree(taper_input_error);
    amfree(taper_tape_error);
    dumper->busy = 0;
    dp->host->inprogress -= 1;
    dp->inprogress = 0;
    deallocate_bandwidth(dp->host->netif, sched(dp)->est_kps);
}


static dumper_t *
idle_dumper(void)
{
    dumper_t *dumper;

    for(dumper = dmptable; dumper < dmptable+inparallel; dumper++)
	if(!dumper->busy && !dumper->down) return dumper;

    return NULL;
}

static void
dumper_chunker_result(
    disk_t *	dp)
{
    dumper_t *dumper;
    chunker_t *chunker;
    assignedhd_t **h=NULL;
    int activehd, i;
    off_t dummy;
    off_t size;
    int is_partial;
    char *qname;

    dumper = sched(dp)->dumper;
    chunker = dumper->chunker;

    free_serial_dp(dp);

    h = sched(dp)->holdp;
    activehd = sched(dp)->activehd;

    if(dumper->result == DONE && chunker->result == DONE) {
	update_info_dumper(dp, sched(dp)->origsize,
			   sched(dp)->dumpsize, sched(dp)->dumptime);
	qname = quote_string(dp->name);/*quote to take care of spaces*/

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

    dummy = (off_t)0;
    for( i = 0, h = sched(dp)->holdp; i < activehd; i++ ) {
	dummy += h[i]->used;
    }

    size = holding_file_size(sched(dp)->destname, 0);
    h[activehd]->used = size - dummy;
    h[activehd]->disk->allocated_dumpers--;
    adjust_diskspace(dp, DONE);

    sched(dp)->dump_attempted += 1;

    if((dumper->result != DONE || chunker->result != DONE) &&
       sched(dp)->dump_attempted <= 1) {
	delete_diskspace(dp);
	if (sched(dp)->no_space) {
	    enqueue_disk(&directq, dp);
	} else {
	    enqueue_disk(&runq, dp);
	}
    }
    else if(size > (off_t)DISK_BLOCK_KB) {
	enqueue_disk(&tapeq, dp);
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


static void
handle_dumper_result(
	void * cookie)
{
    /* uses global pending_aborts */
    dumper_t *dumper = cookie;
    disk_t *dp, *sdp, *dp1;
    cmd_t cmd;
    int result_argc;
    char *qname;
    char **result_argv;

    assert(dumper != NULL);
    dp = dumper->dp;
    assert(dp != NULL);
    assert(sched(dp) != NULL);
    do {

	short_dump_state();

	cmd = getresult(dumper->fd, 1, &result_argc, &result_argv);

	if(cmd != BOGUS) {
	    /* result_argv[1] always contains the serial number */
	    sdp = serial2disk(result_argv[1]);
	    if (sdp != dp) {
		error(_("Invalid serial number %s"), result_argv[1]);
                g_assert_not_reached();
	    }
	}

	qname = quote_string(dp->name);
	switch(cmd) {

	case DONE: /* DONE <handle> <origsize> <dumpsize> <dumptime> <errstr> */
	    if(result_argc != 6) {
		error(_("error [dumper DONE result_argc != 6: %d]"), result_argc);
		/*NOTREACHED*/
	    }

	    sched(dp)->origsize = OFF_T_ATOI(result_argv[2]);
	    sched(dp)->dumptime = TIME_T_ATOI(result_argv[4]);

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
	    if(sched(dp)->dump_attempted) {
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
            if(sched(dp)->dump_attempted) {
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
	    run_server_scripts(EXECUTE_ON_POST_DLE_BACKUP,
			       get_config_name(), dp, sched(dp)->level);
	    for (dp1=runq.head; dp1 != NULL; dp1 = dp1->next) {
		if (dp1 != dp) last_dump = 0;
	    }
	    if (last_dump && dp->host->post_script == 0) {
		if (dp->host->post_script == 0) {
		    for (dp1=dp->host->disks; dp1 != NULL; dp1 = dp1->hostnext) {
			run_server_scripts(EXECUTE_ON_POST_HOST_BACKUP,
					   get_config_name(), dp1, -1);
		    }
		    dp->host->post_script = 1;
		}
	    }
	}

	/* send the dumper result to the chunker */
	if (dumper->chunker) {
	    if (dumper->chunker->down == 0 && dumper->chunker->fd != -1 &&
		dumper->chunker->result == LAST_TOK) {
		if (cmd == DONE) {
		    chunker_cmd(dumper->chunker, DONE, dp, NULL);
		}
		else {
		    chunker_cmd(dumper->chunker, FAILED, dp, NULL);
		}
	    }
	    if( dumper->result != LAST_TOK &&
	 	dumper->chunker->result != LAST_TOK)
		dumper_chunker_result(dp);
	} else { /* send the dumper result to the taper */
	    if (taper_sendresult) {
		if (cmd == DONE) {
		    taper_cmd(DONE, driver_timestamp, NULL, 0, NULL);
		} else {
		    taper_cmd(FAILED, driver_timestamp, NULL, 0, NULL);
		}
		taper_sendresult = 0;
	    }
	}
	if (taper_dumper && taper_result != LAST_TOK) {
	    dumper_taper_result(dp);
	}
    } while(areads_dataready(dumper->fd));
}


static void
handle_chunker_result(
    void *	cookie)
{
    chunker_t *chunker = cookie;
    assignedhd_t **h=NULL;
    dumper_t *dumper;
    disk_t *dp, *sdp;
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    int dummy;
    int activehd = -1;
    char *qname;

    assert(chunker != NULL);
    dumper = chunker->dumper;
    assert(dumper != NULL);
    dp = dumper->dp;
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
	    sdp = serial2disk(result_argv[1]);
	    if (sdp != dp) {
		error(_("Invalid serial number %s"), result_argv[1]);
                g_assert_not_reached();
	    }
	}

	switch(cmd) {

	case PARTIAL: /* PARTIAL <handle> <dumpsize> <errstr> */
	case DONE: /* DONE <handle> <dumpsize> <errstr> */
	    if(result_argc != 4) {
		error(_("error [chunker %s result_argc != 4: %d]"), cmdstr[cmd],
		      result_argc);
	        /*NOTREACHED*/
	    }
	    /*free_serial(result_argv[1]);*/

	    sched(dp)->dumpsize = (off_t)atof(result_argv[2]);

	    qname = quote_string(dp->name);
	    g_printf(_("driver: finished-cmd time %s %s chunked %s:%s\n"),
		   walltime_str(curclock()), chunker->name,
		   dp->host->hostname, qname);
	    fflush(stdout);
            amfree(qname);

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    break;

	case TRYAGAIN: /* TRY-AGAIN <handle> <errstr> */
	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    break;
	case FAILED: /* FAILED <handle> <errstr> */
	    /*free_serial(result_argv[1]);*/

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

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
            if(sched(dp)->dump_attempted) {
                log_add(L_FAIL, _("%s %s %s %d [%s died]"),
                        dp->host->hostname, qname, sched(dp)->datestamp,
                        sched(dp)->level, chunker->name);
            } else {
                log_add(L_WARNING, _("%s died while dumping %s:%s lev %d."),
                        chunker->name, dp->host->hostname, qname,
                        sched(dp)->level);
            }
            amfree(qname);
            dp = NULL;

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    break;

	default:
	    assert(0);
	}
	g_strfreev(result_argv);

	if(chunker->result != LAST_TOK && chunker->dumper->result != LAST_TOK)
	    dumper_chunker_result(dp);

    } while(areads_dataready(chunker->fd));
}


static disklist_t
read_flush(void)
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
    disklist_t tq;
    char *qname = NULL;
    char *qdestname = NULL;

    tq.head = tq.tail = NULL;

    for(line = 0; (inpline = agets(stdin)) != NULL; free(inpline)) {
	dumpfile_t file;

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

	if(strcmp(command,"ENDFLUSH") == 0) {
	    break;
	}

	if(strcmp(command,"FLUSH") != 0) {
	    error(_("flush line %d: syntax error (%s != FLUSH)"), line, command);
	    /*NOTREACHED*/
	}

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

	holding_file_get_dumpfile(destname, &file);
	if( file.type != F_DUMPFILE) {
	    if( file.type != F_CONT_DUMPFILE )
		log_add(L_INFO, _("%s: ignoring cruft file."), destname);
	    amfree(diskname);
	    amfree(destname);
	    dumpfile_free_data(&file);
	    continue;
	}

	if(strcmp(hostname, file.name) != 0 ||
	   strcmp(diskname, file.disk) != 0 ||
	   strcmp(datestamp, file.datestamp) != 0) {
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

	if(file.dumplevel < 0 || file.dumplevel > 9) {
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

	dp1 = (disk_t *)alloc(SIZEOF(disk_t));
	*dp1 = *dp;
	dp1->next = dp1->prev = NULL;

	/* add it to the flushhost list */
	if(!flushhost) {
	    flushhost = alloc(SIZEOF(am_host_t));
	    flushhost->next = NULL;
	    flushhost->hostname = stralloc("FLUSHHOST");
	    flushhost->up = NULL;
	    flushhost->features = NULL;
	}
	dp1->hostnext = flushhost->disks;
	flushhost->disks = dp1;

	sp = (sched_t *) alloc(SIZEOF(sched_t));
	sp->destname = destname;
	sp->level = file.dumplevel;
	sp->dumpdate = NULL;
	sp->degr_dumpdate = NULL;
	sp->degr_mesg = NULL;
	sp->datestamp = stralloc(file.datestamp);
	sp->est_nsize = (off_t)0;
	sp->est_csize = (off_t)0;
	sp->est_time = 0;
	sp->est_kps = 10;
	sp->priority = 0;
	sp->degr_level = -1;
	sp->dump_attempted = 0;
	sp->taper_attempted = 0;
	sp->act_size = holding_file_size(destname, 0);
	sp->holdp = build_diskspace(destname);
	if(sp->holdp == NULL) continue;
	sp->dumper = NULL;
	sp->timestamp = (time_t)0;

	dp1->up = (char *)sp;

	enqueue_disk(&tq, dp1);
	dumpfile_free_data(&file);
    }
    amfree(inpline);

    /*@i@*/ return tq;
}

static void
read_schedule(
    void *	cookie)
{
    sched_t *sp;
    disk_t *dp;
    int level, line, priority;
    char *dumpdate, *degr_dumpdate, *degr_mesg;
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

    (void)cookie;	/* Quiet unused parameter warning */

    event_release(schedule_ev_read);

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

	if(strcmp(command,"DUMP") != 0) {
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
	    continue;
	}

	sp = (sched_t *) alloc(SIZEOF(sched_t));
	/*@ignore@*/
	sp->level = level;
	sp->dumpdate = stralloc(dumpdate);
	sp->est_nsize = DISK_BLOCK_KB + nsize; /* include header */
	sp->est_csize = DISK_BLOCK_KB + csize; /* include header */
	/* round estimate to next multiple of DISK_BLOCK_KB */
	sp->est_csize = am_round(sp->est_csize, DISK_BLOCK_KB);
	sp->est_size = sp->est_csize;
	sp->est_time = time;
	sp->est_kps = kps;
	sp->priority = priority;
	sp->datestamp = stralloc(datestamp);

	if(degr_dumpdate) {
	    sp->degr_level = degr_level;
	    sp->degr_dumpdate = stralloc(degr_dumpdate);
	    sp->degr_nsize = DISK_BLOCK_KB + degr_nsize;
	    sp->degr_csize = DISK_BLOCK_KB + degr_csize;
	    /* round estimate to next multiple of DISK_BLOCK_KB */
	    sp->degr_csize = am_round(sp->degr_csize, DISK_BLOCK_KB);
	    sp->degr_time = degr_time;
	    sp->degr_kps = degr_kps;
	    sp->degr_mesg = NULL;
	} else {
	    sp->degr_level = -1;
	    sp->degr_dumpdate = NULL;
	    sp->degr_mesg = degr_mesg;
	}
	/*@end@*/

	sp->dump_attempted = 0;
	sp->taper_attempted = 0;
	sp->act_size = 0;
	sp->holdp = NULL;
	sp->activehd = -1;
	sp->dumper = NULL;
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

	if (dp->data_path == DATA_PATH_DIRECTTCP &&
	    dp->to_holdingdisk == HOLD_AUTO) {
	    /* planner already logged a warning. */
	    dp->to_holdingdisk = HOLD_NEVER;
	}

	if (dp->to_holdingdisk == HOLD_NEVER) {
	    enqueue_disk(&directq, dp);
	} else {
	    enqueue_disk(&runq, dp);
	}
	flush_size += sp->act_size;
	amfree(diskname);
    }
    g_printf(_("driver: flush size %lld\n"), (long long)flush_size);
    amfree(inpline);
    if(line == 0)
	log_add(L_WARNING, _("WARNING: got empty schedule from planner"));
    if(need_degraded==1) start_degraded_mode(&runq);
    schedule_done = 1;
    start_some_dumps(&runq);
    startaflush();
}

static unsigned long
free_kps(
    netif_t *ip)
{
    unsigned long res;

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
	else
	    res = 0;
#ifndef __lint
    } else {
	if ((unsigned long)interface_get_maxusage(ip->config) >= ip->curusage)
	    res = interface_get_maxusage(ip->config) - ip->curusage;
	else
	    res = 0;
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

    if (size < 2*DISK_BLOCK_KB)
	size = 2*DISK_BLOCK_KB;
    size = am_round(size, (off_t)DISK_BLOCK_KB);

    hold_debug(1, _("find_diskspace: want %lld K\n"),
		   (long long)size);

    used = alloc(SIZEOF(*used) * num_holdalloc);/*disks used during this run*/
    memset( used, 0, (size_t)num_holdalloc );
    result = alloc(SIZEOF(assignedhd_t *) * (num_holdalloc + 1));
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
	result[i] = alloc(SIZEOF(assignedhd_t));
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

    g_snprintf( lvl, SIZEOF(lvl), "%d", sched(diskp)->level );

    size = am_round(sched(diskp)->est_size - sched(diskp)->act_size,
		    (off_t)DISK_BLOCK_KB);

    for( c = 0; holdp[c]; c++ )
	(void)c; /* count number of disks */

    /* allocate memory for sched(diskp)->holdp */
    for(j = 0; sched(diskp)->holdp && sched(diskp)->holdp[j]; j++)
	(void)j;	/* Quiet lint */
    new_holdp = (assignedhd_t **)alloc(SIZEOF(assignedhd_t*)*(j+c+1));
    if (sched(diskp)->holdp) {
	memcpy(new_holdp, sched(diskp)->holdp, j * SIZEOF(*new_holdp));
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
	holdp[i]->destname = newvstralloc( holdp[i]->destname,
					   holdingdisk_get_diskdir(holdp[i]->disk->hdisk), "/",
					   hd_driver_timestamp, "/",
					   diskp->host->hostname, ".",
					   sfn, ".",
					   lvl, NULL );
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
	hold_debug(1, _("adjust_diskspace: hdisk %s done, reserved %lld used %lld diff %lld alloc %lld dumpers %d\n"),
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
    assignedhd_t **result;
    holdalloc_t *ha;
    off_t *used;
    char dirname[1000], *ch;
    struct stat finfo;
    char *filename = destname;

    memset(buffer, 0, sizeof(buffer));
    used = alloc(SIZEOF(off_t) * num_holdalloc);
    for(i=0;i<num_holdalloc;i++)
	used[i] = (off_t)0;
    result = alloc(SIZEOF(assignedhd_t *) * (num_holdalloc + 1));
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
	    if(strcmp(dirname, holdingdisk_get_diskdir(ha->hdisk))==0) {
		break;
	    }
	}

	if(stat(filename, &finfo) == -1) {
	    g_fprintf(stderr, _("stat %s: %s\n"), filename, strerror(errno));
	    finfo.st_size = (off_t)0;
	}
	used[j] += ((off_t)finfo.st_size+(off_t)1023)/(off_t)1024;
	if((fd = open(filename,O_RDONLY)) == -1) {
	    g_fprintf(stderr,_("build_diskspace: open of %s failed: %s\n"),
		    filename, strerror(errno));
	    amfree(used);
	    amfree(result);
	    return NULL;
	}
	if ((buflen = full_read(fd, buffer, SIZEOF(buffer))) > 0) {;
		parse_file_header(buffer, &file, buflen);
	}
	close(fd);
	filename = file.cont_filename;
    }

    for(j = 0, i=0, ha = holdalloc; ha != NULL; ha = ha->next, j++ ) {
	if(used[j] != (off_t)0) {
	    result[i] = alloc(SIZEOF(assignedhd_t));
	    result[i]->disk = ha;
	    result[i]->reserved = used[j];
	    result[i]->used = used[j];
	    result[i]->destname = stralloc(destname);
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
static void
dump_to_tape(
    disk_t *	dp)
{
    dumper_t *dumper;
    cmd_t cmd;
    int result_argc;
    char **result_argv;
    char *qname;
    disk_t *dp1;

    qname = quote_string(dp->name);
    g_printf(_("driver: dumping %s:%s directly to tape\n"),
	   dp->host->hostname, qname);
    fflush(stdout);

    /* pick a dumper and fail if there are no idle dumpers */

    dumper = idle_dumper();
    if (!dumper) {
	g_printf(_("driver: no idle dumpers for %s:%s.\n"), 
		dp->host->hostname, qname);
	fflush(stdout);
	log_add(L_WARNING, _("no idle dumpers for %s:%s.\n"),
	        dp->host->hostname, qname);
        amfree(qname);
	return;	/* fatal problem */
    }

    /* tell the taper to read from a port number of its choice */

    taper_cmd(PORT_WRITE, dp, NULL, sched(dp)->level, sched(dp)->datestamp);
    cmd = getresult(taper, 1, &result_argc, &result_argv);
    if (dp->data_path == DATA_PATH_AMANDA && cmd != PORT) {
	g_printf(_("driver: did not get PORT from taper for %s:%s\n"),
		dp->host->hostname, qname);
	fflush(stdout);
	log_add(L_WARNING, _("driver: did not get PORT from taper for %s:%s.\n"),
	        dp->host->hostname, qname);
        amfree(qname);
	return;	/* fatal problem */
    }
    if (dp->data_path == DATA_PATH_DIRECTTCP && cmd != DIRECTTCP_PORT) {
	g_printf(_("driver: did not get DIRECTTCP_PORT from taper for %s:%s\n"),
		dp->host->hostname, qname);
	fflush(stdout);
	log_add(L_WARNING, _("driver: did not get DIRECTTCP_PORT from taper for %s:%s.\n"),
	        dp->host->hostname, qname);
        amfree(qname);
	return;	/* fatal problem */
    }
    amfree(qname);

    /* copy port number */
    dumper->output_port = atoi(result_argv[1]);
    if (dp->data_path == DATA_PATH_DIRECTTCP) {
	dp->directtcp_list = g_slist_append(dp->directtcp_list,
					    stralloc(result_argv[2]));
    } else {
	dp->directtcp_list = NULL;
    }

    dumper->dp = dp;
    dumper->chunker = NULL;
    dumper->result = LAST_TOK;
    taper_result = LAST_TOK;
    sched(dp)->dumper = dumper;

    if (dp->host->pre_script == 0) {
	for (dp1=dp->host->disks; dp1 != NULL; dp1 = dp1->hostnext) {
	    run_server_scripts(EXECUTE_ON_PRE_HOST_BACKUP,
			       get_config_name(), dp1, -1);
	}
	dp->host->pre_script = 1;
    }
    run_server_scripts(EXECUTE_ON_PRE_DLE_BACKUP, get_config_name(), dp,
		       sched(dp)->level);

    /* tell the dumper to dump to a port */
    dumper_cmd(dumper, PORT_DUMP, dp, NULL);
    dp->host->start_t = time(NULL) + 15;
    g_slist_free_full(dp->directtcp_list);
    dp->directtcp_list = NULL;

    /* update statistics & print state */

    taper_busy = dumper->busy = 1;
    taper_input_error = NULL;
    taper_tape_error = NULL;
    taper_dumper = dumper;
    taper_disk = dp;
    taper_first_label = NULL;
    taper_written = 0;
    taper_state |= TAPER_STATE_DUMP_TO_TAPE;
    sched(dp)->act_size = sched(dp)->est_size;
    dp->host->inprogress += 1;
    dp->inprogress = 1;
    sched(dp)->timestamp = time((time_t *)0);
    allocate_bandwidth(dp->host->netif, sched(dp)->est_kps);
    idle_reason = NOT_IDLE;

    short_dump_state();

    dumper->ev_read = event_register(dumper->fd, EV_READFD,
				     handle_dumper_result, dumper);
    taper_ev_read = event_register(taper, EV_READFD,
				   handle_taper_result, NULL);

    g_strfreev(result_argv);
}

static int
queue_length(
    disklist_t	q)
{
    disk_t *p;
    int len;

    for(len = 0, p = q.head; p != NULL; len++, p = p->next)
	(void)len;	/* Quiet lint */
    return len;
}

static void
short_dump_state(void)
{
    int i, nidle;
    char *wall_time;

    wall_time = walltime_str(curclock());

    g_printf(_("driver: state time %s "), wall_time);
    g_printf(_("free kps: %lu space: %lld taper: "),
	   free_kps(NULL),
	   (long long)free_space());
    if(degraded_mode) g_printf(_("DOWN"));
    else if(!taper_busy) g_printf(_("idle"));
    else g_printf(_("writing"));
    nidle = 0;
    for(i = 0; i < inparallel; i++) if(!dmptable[i].busy) nidle++;
    g_printf(_(" idle-dumpers: %d"), nidle);
    g_printf(_(" qlen tapeq: %d"), queue_length(tapeq));
    g_printf(_(" runq: %d"), queue_length(runq));
    g_printf(_(" roomq: %d"), queue_length(roomq));
    g_printf(_(" wakeup: %d"), (int)sleep_time);
    g_printf(_(" driver-idle: %s\n"), _(idle_strings[idle_reason]));
    interface_state(wall_time);
    holdingdisk_state(wall_time);
    fflush(stdout);
}

static TapeAction tape_action(char **why_no_new_tape)
{
    TapeAction result = TAPE_ACTION_NO_ACTION;
    dumper_t *dumper;
    disk_t   *dp;
    off_t dumpers_size;
    off_t runq_size;
    off_t directq_size;
    off_t tapeq_size;
    off_t sched_size;
    off_t dump_to_disk_size;
    int   dump_to_disk_terminated;

    dumpers_size = 0;
    for(dumper = dmptable; dumper < (dmptable+inparallel); dumper++) {
	if (dumper->busy)
	    dumpers_size += sched(dumper->dp)->est_size;
    }
    driver_debug(1, _("dumpers_size: %lld\n"), (long long)dumpers_size);

    runq_size = 0;
    for(dp = runq.head; dp != NULL; dp = dp->next) {
	runq_size += sched(dp)->est_size;
    }
    driver_debug(1, _("runq_size: %lld\n"), (long long)runq_size);

    directq_size = 0;
    for(dp = directq.head; dp != NULL; dp = dp->next) {
	directq_size += sched(dp)->est_size;
    }
    driver_debug(1, _("directq_size: %lld\n"), (long long)directq_size);

    tapeq_size = 0;
    for(dp = tapeq.head; dp != NULL; dp = dp->next) {
	tapeq_size += sched(dp)->act_size;
    }
    if (taper_disk) {
	tapeq_size += sched(taper_disk)->act_size - taper_written;
    }
    driver_debug(1, _("tapeq_size: %lld\n"), (long long)tapeq_size);

    sched_size = runq_size + tapeq_size + dumpers_size;
    driver_debug(1, _("sched_size: %lld\n"), (long long)sched_size);

    dump_to_disk_size = dumpers_size + runq_size;
    driver_debug(1, _("dump_to_disk_size: %lld\n"), (long long)dump_to_disk_size);

    dump_to_disk_terminated = schedule_done && dump_to_disk_size == 0;

    // Changing conditionals can produce a driver hang, take care.
    // 
    // when to start writting to a new tape
    if ((taper_state & TAPER_STATE_WAIT_FOR_TAPE) &&
        ((taper_state & TAPER_STATE_DUMP_TO_TAPE) ||	// for dump to tape
	 !empty(directq) ||				// if a dle is waiting for a dump to tape
         !empty(roomq) ||				// holding disk constraint
         idle_reason == IDLE_NO_DISKSPACE ||		// holding disk constraint
         (flush_threshold_dumped < tapeq_size &&	// flush-threshold-dumped &&
	  flush_threshold_scheduled < sched_size) ||    //  flush-threshold-scheduled
	 (taperflush < tapeq_size &&			// taperflush
	  (force_flush == 1 ||				//  if force_flush
	   dump_to_disk_terminated))			//  or all dump to disk terminated
	)) {
	result |= TAPE_ACTION_NEW_TAPE;
    // when to stop using new tape
    } else if ((taper_state & TAPER_STATE_WAIT_FOR_TAPE) &&
	       (taperflush >= tapeq_size &&		// taperflush criteria not meet
	        (force_flush == 1 ||			//  if force_flush
		 dump_to_disk_terminated))		//  or all dump to disk terminated
	      ) {
	result |= TAPE_ACTION_NO_NEW_TAPE;
	if (flush_threshold_dumped >= tapeq_size) {
	    *why_no_new_tape = _("flush-threshold-dumped criteria not met");
	} else if (flush_threshold_scheduled >= sched_size) {
	    *why_no_new_tape = _("flush-threshold-scheduled criteria not met");
	} else {
	    *why_no_new_tape = _("taperflush criteria not met");
	}
    }

    // when to start a flush
    // We don't start a flush if taper_tape_started == 1 && dump_to_disk_terminated && force_flush == 0,
    // it is a criteria need to exit the first event_loop without flushing everything to tape,
    // they will be flush in another event_loop.
    if (!degraded_mode && !taper_busy && !empty(tapeq) &&
	(!((taper_state & TAPER_STATE_TAPE_STARTED) &&
	    dump_to_disk_terminated && force_flush == 0) ||	// if tape already started and dump to disk not terminated
         ((taper_state & TAPER_STATE_TAPE_STARTED) &&
	  force_flush == 1) ||					// if tape already started and force_flush
         !empty(roomq) ||					// holding disk constraint
         idle_reason == IDLE_NO_DISKSPACE ||			// holding disk constraint
         (flush_threshold_dumped < tapeq_size &&		// flush-threshold-dumped &&
 	 flush_threshold_scheduled < sched_size) ||		//  flush-threshold-scheduled
         (force_flush == 1 && taperflush < tapeq_size))) {	// taperflush if force_flush
	result |= TAPE_ACTION_START_A_FLUSH;
    }
    return result;
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
    else if(!taper_busy) g_printf(_("taper: idle\n"));
    else g_printf(_("taper: writing %s:%s.%d est size %lld\n"),
		taper_disk->host->hostname, taper_disk->name,
		sched(taper_disk)->level,
		(long long)sched(taper_disk)->est_size);
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
    dump_queue("TAPE", tapeq, 5, stdout);
    dump_queue("ROOM", roomq, 5, stdout);
    dump_queue("RUN ", runq, 5, stdout);
    g_printf("================\n");
    fflush(stdout);
}
#endif
