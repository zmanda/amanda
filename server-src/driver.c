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
 * $Id: driver.c,v 1.198 2006/08/24 01:57:16 paddy_s Exp $
 *
 * controlling process for the Amanda backup system
 */

/*
 * XXX possibly modify tape queue to be cognizant of how much room is left on
 *     tape.  Probably not effective though, should do this in planner.
 */

#define HOLD_DEBUG

#include "amanda.h"
#include "clock.h"
#include "conffile.h"
#include "diskfile.h"
#include "event.h"
#include "holding.h"
#include "infofile.h"
#include "logfile.h"
#include "statfs.h"
#include "version.h"
#include "driverio.h"
#include "server_util.h"

static disklist_t waitq, runq, tapeq, roomq;
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
static int current_tape = 1;
static int conf_taperalgo;
static int conf_runtapes;
static time_t sleep_time;
static int idle_reason;
static char *driver_timestamp;
static char *hd_driver_timestamp;
static am_host_t *flushhost = NULL;
static int need_degraded=0;

static event_handle_t *dumpers_ev_time = NULL;
static event_handle_t *schedule_ev_read = NULL;

static int wait_children(int count);
static void wait_for_children(void);
static void allocate_bandwidth(interface_t *ip, unsigned long kps);
static int assign_holdingdisk(assignedhd_t **holdp, disk_t *diskp);
static void adjust_diskspace(disk_t *diskp, cmd_t cmd);
static void delete_diskspace(disk_t *diskp);
static assignedhd_t **build_diskspace(char *destname);
static int client_constrained(disk_t *dp);
static void deallocate_bandwidth(interface_t *ip, unsigned long kps);
static void dump_schedule(disklist_t *qp, char *str);
static int dump_to_tape(disk_t *dp);
static assignedhd_t **find_diskspace(off_t size, int *cur_idle,
					assignedhd_t *preferred);
static unsigned long free_kps(interface_t *ip);
static off_t free_space(void);
static void dumper_result(disk_t *dp);
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
static void update_failed_dump_to_tape(disk_t *);
#if 0
static void dump_state(const char *str);
#endif
int main(int main_argc, char **main_argv);

static const char *idle_strings[] = {
#define NOT_IDLE		0
    "not-idle",
#define IDLE_NO_DUMPERS		1
    "no-dumpers",
#define IDLE_START_WAIT		2
    "start-wait",
#define IDLE_NO_HOLD		3
    "no-hold",
#define IDLE_CLIENT_CONSTRAINED	4
    "client-constrained",
#define IDLE_NO_DISKSPACE	5
    "no-diskspace",
#define IDLE_TOO_LARGE		6
    "file-too-large",
#define IDLE_NO_BANDWIDTH	7
    "no-bandwidth",
#define IDLE_TAPER_WAIT		8
    "taper-wait",
};

int
main(
    int		main_argc,
    char **	main_argv)
{
    disklist_t origq;
    disk_t *diskp;
    int dsk;
    dumper_t *dumper;
    char *newdir = NULL;
    generic_fs_stats_t fs;
    holdingdisk_t *hdp;
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
    unsigned long reserve = 100;
    char *conffile;
    char *conf_diskfile;
    cmd_t cmd;
    int result_argc;
    char *result_argv[MAX_ARGS+1];
    char *taper_program;
    char *conf_tapetype;
    tapetype_t *tape;
    char *line;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;

    safe_fd(-1, 0);

    setvbuf(stdout, (char *)NULL, (int)_IOLBF, 0);
    setvbuf(stderr, (char *)NULL, (int)_IOLBF, 0);

    set_pname("driver");

    dbopen(DBG_SUBDIR_SERVER);

    atexit(wait_for_children);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    erroutput_type = (ERR_AMANDALOG|ERR_INTERACTIVE);
    set_logerror(logerror);

    startclock();

    parse_server_conf(main_argc, main_argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    printf("%s: pid %ld executable %s version %s\n",
	   get_pname(), (long) getpid(), my_argv[0], version());

    if (my_argc > 1) {
	config_name = stralloc(my_argv[1]);
	config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
	if(my_argc > 2) {
	    if(strncmp(my_argv[2], "nodump", 6) == 0) {
		nodump = 1;
	    }
	}

    } else {

	char my_cwd[STR_SIZE];

	if (getcwd(my_cwd, SIZEOF(my_cwd)) == NULL) {
	    error("cannot determine current working directory");
	    /*NOTREACHED*/
	}
	config_dir = stralloc2(my_cwd, "/");
	if ((config_name = strrchr(my_cwd, '/')) != NULL) {
	    config_name = stralloc(config_name + 1);
	}
    }

    safe_cd();

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if(read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    amfree(driver_timestamp);
    /* read timestamp from stdin */
    while ((line = agets(stdin)) != NULL) {
	if (line[0] != '\0')
	    break;
	amfree(line);
    }
    if ( line == NULL ) {
      error("Did not get DATE line from planner");
      /*NOTREACHED*/
    }
    driver_timestamp = alloc(15);
    strncpy(driver_timestamp, &line[5], 14);
    driver_timestamp[14] = '\0';
    amfree(line);
    log_add(L_START,"date %s", driver_timestamp);

    /* check that we don't do many dump in a day and usetimestamps is off */
    if(strlen(driver_timestamp) == 8) {
	char *conf_logdir = getconf_str(CNF_LOGDIR);
	char *logfile    = vstralloc(conf_logdir, "/log.",
				     driver_timestamp, ".0", NULL);
	char *oldlogfile = vstralloc(conf_logdir, "/oldlog/log.",
				     driver_timestamp, ".0", NULL);
	if(access(logfile, F_OK) == 0 || access(oldlogfile, F_OK) == 0) {
	    log_add(L_WARNING, "WARNING: This is not the first amdump run today. Enable the usetimestamps option in the configuration file if you want to run amdump more than once per calendar day.");
	}
	amfree(oldlogfile);
	amfree(logfile);
	hd_driver_timestamp = construct_timestamp(NULL);
    }
    else {
	hd_driver_timestamp = stralloc(driver_timestamp);
    }

    taper_program = vstralloc(libexecdir, "/", "taper", versionsuffix(), NULL);
    dumper_program = vstralloc(libexecdir, "/", "dumper", versionsuffix(),
			       NULL);
    chunker_program = vstralloc(libexecdir, "/", "chunker", versionsuffix(),
			       NULL);

    conf_taperalgo = getconf_taperalgo(CNF_TAPERALGO);
    conf_tapetype = getconf_str(CNF_TAPETYPE);
    conf_runtapes = getconf_int(CNF_RUNTAPES);
    tape = lookup_tapetype(conf_tapetype);
    tape_length = tapetype_get_length(tape);
    printf("driver: tape size " OFF_T_FMT "\n", (OFF_T_FMT_TYPE)tape_length);

    /* start initializing: read in databases */

    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if (read_diskfile(conf_diskfile, &origq) < 0) {
	error("could not load disklist \"%s\"", conf_diskfile);
	/*NOTREACHED*/
    }
    amfree(conf_diskfile);

    /* set up any configuration-dependent variables */

    inparallel	= getconf_int(CNF_INPARALLEL);

    reserve = (unsigned long)getconf_int(CNF_RESERVE);

    total_disksize = (off_t)0;
    for(hdp = getconf_holdingdisks(), dsk = 0; hdp != NULL; hdp = hdp->next, dsk++) {
	hdp->up = (void *)alloc(SIZEOF(holdalloc_t));
	holdalloc(hdp)->allocated_dumpers = 0;
	holdalloc(hdp)->allocated_space = (off_t)0;

	if(get_fs_stats(holdingdisk_get_diskdir(hdp), &fs) == -1
	   || access(holdingdisk_get_diskdir(hdp), W_OK) == -1) {
	    log_add(L_WARNING, "WARNING: ignoring holding disk %s: %s\n",
		    holdingdisk_get_diskdir(hdp), strerror(errno));
	    hdp->disksize = 0L;
	    continue;
	}

	if(fs.avail != (off_t)-1) {
	    if(hdp->disksize > (off_t)0) {
		if(hdp->disksize > fs.avail) {
		    log_add(L_WARNING,
			    "WARNING: %s: " OFF_T_FMT " KB requested, "
			    "but only " OFF_T_FMT " KB available.",
			    holdingdisk_get_diskdir(hdp),
			    (OFF_T_FMT_TYPE)hdp->disksize,
			    (OFF_T_FMT_TYPE)fs.avail);
			    hdp->disksize = fs.avail;
		}
	    }
	    else if((fs.avail + hdp->disksize) < (off_t)0) {
		log_add(L_WARNING,
			"WARNING: %s: not " OFF_T_FMT " KB free.",
			holdingdisk_get_diskdir(hdp), -hdp->disksize);
		hdp->disksize = (off_t)0;
		continue;
	    }
	    else
		hdp->disksize += fs.avail;
	}

	printf("driver: adding holding disk %d dir %s size "
		OFF_T_FMT " chunksize " OFF_T_FMT "\n",
	       dsk, holdingdisk_get_diskdir(hdp),
	       (OFF_T_FMT_TYPE)hdp->disksize,
	       (OFF_T_FMT_TYPE)(holdingdisk_get_chunksize(hdp)));

	newdir = newvstralloc(newdir,
			      holdingdisk_get_diskdir(hdp), "/", hd_driver_timestamp,
			      NULL);
	if(!mkholdingdir(newdir)) {
	    hdp->disksize = (off_t)0;
	}
	total_disksize += hdp->disksize;
    }

    reserved_space = total_disksize * (off_t)(reserve / 100);

    printf("reserving " OFF_T_FMT " out of " OFF_T_FMT
    	   " for degraded-mode dumps\n",
	   (OFF_T_FMT_TYPE)reserved_space, (OFF_T_FMT_TYPE)free_space());

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
    waitq = origq;
    tapeq = read_flush();

    roomq.head = roomq.tail = NULL;

    log_add(L_STATS, "startup time %s", walltime_str(curclock()));

    printf("driver: start time %s inparallel %d bandwidth %lu diskspace "
    	   OFF_T_FMT " ", walltime_str(curclock()), inparallel,
	   free_kps((interface_t *)0), (OFF_T_FMT_TYPE)free_space());
    printf(" dir %s datestamp %s driver: drain-ends tapeq %s big-dumpers %s\n",
	   "OBSOLETE", driver_timestamp, taperalgo2str(conf_taperalgo),
	   getconf_str(CNF_DUMPORDER));
    fflush(stdout);

    /* ok, planner is done, now lets see if the tape is ready */

    if(conf_runtapes > 0) {
	cmd = getresult(taper, 1, &result_argc, result_argv, MAX_ARGS+1);

	if(cmd != TAPER_OK) {
	    /* no tape, go into degraded mode: dump to holding disk */
	    need_degraded=1;
	}
    }
    else {
	need_degraded=1;
    }

    tape_left = tape_length;
    taper_busy = 0;
    taper_disk = NULL;
    taper_ev_read = NULL;
    if(!need_degraded) startaflush();

    if(!nodump)
	schedule_ev_read = event_register((event_id_t)0, EV_READFD, read_schedule, NULL);

    short_dump_state();
    event_loop(0);

    /* handle any remaining dumps by dumping directly to tape, if possible */

    while(!empty(runq) && taper > 0) {
	diskp = dequeue_disk(&runq);
	if (diskp->to_holdingdisk == HOLD_REQUIRED) {
	    log_add(L_FAIL, "%s %s %s %d [%s]",
		diskp->host->hostname, diskp->name, sched(diskp)->datestamp,
		sched(diskp)->level,
		"can't dump required holdingdisk");
	}
	else if (!degraded_mode) {
	    int rc = dump_to_tape(diskp);
	    if(rc == 1)
		log_add(L_INFO,
			"%s %s %d [dump to tape failed, will try again]",
		        diskp->host->hostname,
			diskp->name,
			sched(diskp)->level);
	    else if(rc == 2)
		log_add(L_FAIL, "%s %s %s %d [dump to tape failed]",
		        diskp->host->hostname,
			diskp->name,
			sched(diskp)->datestamp,
			sched(diskp)->level);
	}
	else
	    log_add(L_FAIL, "%s %s %s %d [%s]",
		diskp->host->hostname, diskp->name, sched(diskp)->datestamp,
		sched(diskp)->level,
		diskp->to_holdingdisk == HOLD_AUTO ?
		    "no more holding disk space" :
		    "can't dump no-hold disk in degraded mode");
    }

    short_dump_state();				/* for amstatus */

    printf("driver: QUITTING time %s telling children to quit\n",
           walltime_str(curclock()));
    fflush(stdout);

    if(!nodump) {
	for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	    if(dumper->fd >= 0)
		dumper_cmd(dumper, QUIT, NULL);
	}
    }

    if(taper >= 0) {
	taper_cmd(QUIT, NULL, NULL, 0, NULL);
    }

    /* wait for all to die */
    wait_children(600);

    for(hdp = getconf_holdingdisks(); hdp != NULL; hdp = hdp->next) {
	cleanup_holdingdisk(holdingdisk_get_diskdir(hdp), 0);
	amfree(hdp->up);
    }
    amfree(newdir);

    check_unfree_serial();
    printf("driver: FINISHED time %s\n", walltime_str(curclock()));
    fflush(stdout);
    log_add(L_FINISH,"date %s time %s", driver_timestamp, walltime_str(curclock()));
    amfree(driver_timestamp);

    free_new_argv(new_argc, new_argv);
    amfree(dumper_program);
    amfree(taper_program);
    amfree(config_dir);
    amfree(config_name);

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

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
		    what = "signal";
		    code = WTERMSIG(retstat);
		} else if (WEXITSTATUS(retstat) != 0) {
		    what = "code";
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
		    if (pid == dumper->chunker->pid) {
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
		    log_add(L_WARNING, "%s pid %u exited with %s %d\n", who, 
			    (unsigned)pid, what, code);
		    printf("driver: %s pid %u exited with %s %d\n", who,
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
		printf("driver: sending signal %d to %s pid %u\n", signal,
		       dumper->name, (unsigned)dumper->pid);
		if (kill(dumper->pid, signal) == -1 && errno == ESRCH) {
		    if (dumper->chunker)
			dumper->chunker->pid = 0;
		}
		if (dumper->chunker && dumper->chunker->pid > 1) {
		    printf("driver: sending signal %d to %s pid %u\n", signal,
			   dumper->chunker->name,
			   (unsigned)dumper->chunker->pid);
		    if (kill(dumper->chunker->pid, signal) == -1 &&
			errno == ESRCH)
			dumper->chunker->pid = 0;
		}
	    }
        }
    }

    if(taper_pid > 1)
	printf("driver: sending signal %d to %s pid %u\n", signal,
	       "taper", (unsigned)taper_pid);
	if (kill(taper_pid, signal) == -1 && errno == ESRCH)
	    taper_pid = 0;
}

static void
wait_for_children(void)
{
    dumper_t *dumper;

    if(!nodump) {
	for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	    if (dumper->pid > 1 && dumper->fd >= 0) {
		dumper_cmd(dumper, QUIT, NULL);
		if (dumper->chunker && dumper->chunker->pid > 1 &&
		    dumper->chunker->fd >= 0)
		    chunker_cmd(dumper->chunker, QUIT, NULL);
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

    if(!degraded_mode && !taper_busy && !empty(tapeq)) {
	
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
		fprintf(stderr,
		   "driver: startaflush: Using SMALLEST because nothing fit\n");
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
	    qname = quote_string(dp->name);
	    taper_cmd(FILE_WRITE, dp, sched(dp)->destname, sched(dp)->level,
		      sched(dp)->datestamp);
	    fprintf(stderr,"driver: startaflush: %s %s %s "
		    OFF_T_FMT " " OFF_T_FMT "\n",
		    taperalgo2str(conf_taperalgo), dp->host->hostname, qname,
		    (OFF_T_FMT_TYPE)sched(taper_disk)->act_size,
		    (OFF_T_FMT_TYPE)tape_left);
	    if(sched(dp)->act_size <= tape_left)
		tape_left -= sched(dp)->act_size;
	    else
		tape_left = (off_t)0;
	    amfree(qname);
	} else {
	    error("FATAL: Taper marked busy and no work found.");
	    /*NOTREACHED*/
	}
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
    assignedhd_t **holdp=NULL, **holdp_accept;
    const time_t now = time(NULL);
    cmd_t cmd;
    int result_argc;
    char *result_argv[MAX_ARGS+1];
    chunker_t *chunker;
    dumper_t *dumper;
    char dumptype;
    char *dumporder;

    idle_reason = IDLE_NO_DUMPERS;
    sleep_time = 0;

    if(dumpers_ev_time != NULL) {
	event_release(dumpers_ev_time);
	dumpers_ev_time = NULL;
    }

    for (dumper = dmptable; dumper < dmptable+inparallel; dumper++) {

	if( dumper->busy ) {
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
		      default:  log_add(L_WARNING, "Unknown dumporder character \'%c\', using 's'.\n",
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
	    chunker_cmd(chunker, START, (void *)driver_timestamp);
	    chunker->dumper = dumper;
	    chunker_cmd(chunker, PORT_WRITE, diskp);
	    cmd = getresult(chunker->fd, 1, &result_argc, result_argv, MAX_ARGS+1);
	    if(cmd != PORT) {
		assignedhd_t **h=NULL;
		int activehd;

		printf("driver: did not get PORT from %s for %s:%s\n",
		       chunker->name, diskp->host->hostname, diskp->name);
		fflush(stdout);

		deallocate_bandwidth(diskp->host->netif, sched(diskp)->est_kps);
		h = sched(diskp)->holdp;
		activehd = sched(diskp)->activehd;
		h[activehd]->used = 0;
		holdalloc(h[activehd]->disk)->allocated_dumpers--;
		adjust_diskspace(diskp, DONE);
		delete_diskspace(diskp);
		diskp->host->inprogress--;
		diskp->inprogress = 0;
		sched(diskp)->dumper = NULL;
		dumper->busy = 0;
		dumper->dp = NULL;
		sched(diskp)->attempted++;
		free_serial_dp(diskp);
		if(sched(diskp)->attempted < 2)
		    enqueue_disk(rq, diskp);
	    }
	    else {
		dumper->ev_read = event_register((event_id_t)dumper->fd, EV_READFD,
						 handle_dumper_result, dumper);
		chunker->ev_read = event_register((event_id_t)chunker->fd, EV_READFD,
						   handle_chunker_result, chunker);
		dumper->output_port = atoi(result_argv[2]);

		dumper_cmd(dumper, PORT_DUMP, diskp);
	    }
	    diskp->host->start_t = now + 15;
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

    printf("dump of driver schedule %s:\n--------\n", str);

    for(dp = qp->head; dp != NULL; dp = dp->next) {
        qname = quote_string(dp->name);
	printf("  %-20s %-25s lv %d t %5lu s " OFF_T_FMT " p %d\n",
	       dp->host->hostname, qname, sched(dp)->level,
	       sched(dp)->est_time,
	       (OFF_T_FMT_TYPE)sched(dp)->est_size, sched(dp)->priority);
        amfree(qname);
    }
    printf("--------\n");
}

static void
start_degraded_mode(
    /*@keep@*/ disklist_t *queuep)
{
    disk_t *dp;
    disklist_t newq;
    off_t est_full_size;
    char *qname;

    if (taper_ev_read != NULL) {
	event_release(taper_ev_read);
	taper_ev_read = NULL;
    }

    newq.head = newq.tail = 0;

    dump_schedule(queuep, "before start degraded mode");

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
		log_add(L_FAIL,"%s %s %s %d [can't switch to incremental dump]",
		        dp->host->hostname, qname, sched(dp)->datestamp,
			sched(dp)->level);
	    }
	}
        amfree(qname);
    }

    /*@i@*/ *queuep = newq;
    degraded_mode = 1;

    dump_schedule(queuep, "after start degraded mode");
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
	    chunker_cmd( dumper->chunker, CONTINUE, dp );
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
	chunker_cmd( sched(dp)->dumper->chunker, ABORT, NULL);
	dumper_cmd( sched(dp)->dumper, ABORT, NULL );
	pending_aborts++;
    }
}


static void
handle_taper_result(
    void *	cookie)
{
    disk_t *dp;
    off_t filenum;
    cmd_t cmd;
    int result_argc;
    char *result_argv[MAX_ARGS+1];
    int avail_tapes = 0;
    
    (void)cookie;	/* Quiet unused parameter warning */

    assert(cookie == NULL);
    
    do {
        
	short_dump_state();
        
	cmd = getresult(taper, 1, &result_argc, result_argv, MAX_ARGS+1);
        
	switch(cmd) {
            
	case PARTIAL:
	case DONE:	/* DONE <handle> <label> <tape file> <err mess> */
	    if(result_argc != 5) {
		error("error: [taper DONE result_argc != 5: %d", result_argc);
		/*NOTREACHED*/
	    }
            
	    dp = serial2disk(result_argv[2]);
	    free_serial(result_argv[2]);
            
	    filenum = OFF_T_ATOI(result_argv[4]);
	    if(cmd == DONE) {
		update_info_taper(dp, result_argv[3], filenum,
                                  sched(dp)->level);
	    }
            
	    delete_diskspace(dp);
            
	    printf("driver: finished-cmd time %s taper wrote %s:%s\n",
		   walltime_str(curclock()), dp->host->hostname, dp->name);
	    fflush(stdout);
            
	    amfree(sched(dp)->destname);
	    amfree(sched(dp)->dumpdate);
	    amfree(sched(dp)->degr_dumpdate);
	    amfree(sched(dp)->datestamp);
	    amfree(dp->up);
            
	    taper_busy = 0;
	    taper_disk = NULL;
	    startaflush();
            
	    /* continue with those dumps waiting for diskspace */
	    continue_port_dumps();
	    break;
            
	case TRYAGAIN:  /* TRY-AGAIN <handle> <err mess> */
	    if (result_argc < 2) {
		error("error [taper TRYAGAIN result_argc < 2: %d]",
		      result_argc);
	        /*NOTREACHED*/
	    }
	    dp = serial2disk(result_argv[2]);
	    free_serial(result_argv[2]);
	    printf("driver: taper-tryagain time %s disk %s:%s\n",
		   walltime_str(curclock()), dp->host->hostname, dp->name);
	    fflush(stdout);
            
	    /* See how many tapes we have left, but we alwyays
	       retry once (why?) */
	    current_tape++;
	    if(dp->tape_splitsize > (off_t)0)
		avail_tapes = conf_runtapes - current_tape;
	    else
		avail_tapes = 0;
            
	    if(sched(dp)->attempted > avail_tapes) {
		log_add(L_FAIL, "%s %s %s %d [too many taper retries]",
                        dp->host->hostname, dp->name, sched(dp)->datestamp,
                        sched(dp)->level);
		printf("driver: taper failed %s %s %s, too many taper retry\n",
                       result_argv[2], dp->host->hostname, dp->name);
	    }
	    else {
		/* Re-insert into taper queue. */
		sched(dp)->attempted++;
		headqueue_disk(&tapeq, dp);
	    }
            
	    tape_left = tape_length;
            
	    /* run next thing from queue */
            
	    taper_busy = 0;
	    taper_disk = NULL;
	    startaflush();
	    continue_port_dumps();
	    break;
            
        case SPLIT_CONTINUE:  /* SPLIT_CONTINUE <handle> <new_label> */
            if (result_argc != 3) {
                error("error [taper SPLIT_CONTINUE result_argc != 3: %d]",
                      result_argc);
		/*NOTREACHED*/
            }
            
            break;
        case SPLIT_NEEDNEXT:  /* SPLIT-NEEDNEXT <handle> <kb written> */
            if (result_argc != 3) {
                error("error [taper SPLIT_NEEDNEXT result_argc != 3: %d]",
                      result_argc);
		/*NOTREACHED*/
            }
            
            /* Update our tape counter and reset tape_left */
            current_tape++;
            tape_left = tape_length;
            
            /* Reduce the size of the dump by amount written and reduce
               tape_left by the amount left over */
            dp = serial2disk(result_argv[2]);
            sched(dp)->act_size -= OFF_T_ATOI(result_argv[3]);
            if (sched(dp)->act_size < tape_left)
                tape_left -= sched(dp)->act_size;
            else
                tape_length = 0;
            
            break;
            
        case TAPE_ERROR: /* TAPE-ERROR <handle> <err mess> */
            dp = serial2disk(result_argv[2]);
            free_serial(result_argv[2]);
            printf("driver: finished-cmd time %s taper wrote %s:%s\n",
                   walltime_str(curclock()), dp->host->hostname, dp->name);
            fflush(stdout);
            log_add(L_WARNING, "Taper  error: %s", result_argv[3]);
            /*FALLTHROUGH*/

        case BOGUS:
            if (cmd == BOGUS) {
        	log_add(L_WARNING, "Taper protocol error");
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
                        "going into degraded mode because of taper component error.");
                start_degraded_mode(&runq);
            }
            tapeq.head = tapeq.tail = NULL;
            taper_busy = 0;
            taper_disk = NULL;
            if(taper_ev_read != NULL) {
                event_release(taper_ev_read);
                taper_ev_read = NULL;
            }
            if(cmd != TAPE_ERROR) aclose(taper);
            continue_port_dumps();
            break;

	default:
            error("driver received unexpected token (%s) from taper",
                  cmdstr[cmd]);
	    /*NOTREACHED*/
	}
	/*
	 * Wakeup any dumpers that are sleeping because of network
	 * or disk constraints.
	 */
	start_some_dumps(&runq);
        
    } while(areads_dataready(taper));
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
dumper_result(
    disk_t *	dp)
{
    dumper_t *dumper;
    chunker_t *chunker;
    assignedhd_t **h=NULL;
    int activehd, i;
    off_t dummy;
    off_t size;
    int is_partial;

    dumper = sched(dp)->dumper;
    chunker = dumper->chunker;

    free_serial_dp(dp);

    h = sched(dp)->holdp;
    activehd = sched(dp)->activehd;

    if(dumper->result == DONE && chunker->result == DONE) {
	update_info_dumper(dp, sched(dp)->origsize,
			   sched(dp)->dumpsize, sched(dp)->dumptime);
	log_add(L_STATS, "estimate %s %s %s %d [sec %ld nkb " OFF_T_FMT
		" ckb " OFF_T_FMT " kps %d]",
		dp->host->hostname, dp->name, sched(dp)->datestamp,
		sched(dp)->level,
		sched(dp)->est_time, (OFF_T_FMT_TYPE)sched(dp)->est_nsize, 
                (OFF_T_FMT_TYPE)sched(dp)->est_csize,
		sched(dp)->est_kps);
    }

    deallocate_bandwidth(dp->host->netif, sched(dp)->est_kps);

    is_partial = dumper->result != DONE || chunker->result != DONE;
    rename_tmp_holding(sched(dp)->destname, !is_partial);

    dummy = (off_t)0;
    for( i = 0, h = sched(dp)->holdp; i < activehd; i++ ) {
	dummy += h[i]->used;
    }

    size = size_holding_files(sched(dp)->destname, 0);
    h[activehd]->used = size - dummy;
    holdalloc(h[activehd]->disk)->allocated_dumpers--;
    adjust_diskspace(dp, DONE);

    sched(dp)->attempted += 1;

    if((dumper->result != DONE || chunker->result != DONE) &&
       sched(dp)->attempted <= 1) {
	delete_diskspace(dp);
	enqueue_disk(&runq, dp);
    }
    else if(size > (off_t)DISK_BLOCK_KB) {
	sched(dp)->attempted = 0;
	enqueue_disk(&tapeq, dp);
	startaflush();
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
}


static void
handle_dumper_result(
    void *	cookie)
{
    /*static int pending_aborts = 0;*/
    dumper_t *dumper = cookie;
    disk_t *dp, *sdp;
    cmd_t cmd;
    int result_argc;
    char *qname;
    char *result_argv[MAX_ARGS+1];

    assert(dumper != NULL);
    dp = dumper->dp;
    assert(dp != NULL && sched(dp) != NULL);

    do {

	short_dump_state();

	cmd = getresult(dumper->fd, 1, &result_argc, result_argv, MAX_ARGS+1);

	if(cmd != BOGUS) {
	    /* result_argv[2] always contains the serial number */
	    sdp = serial2disk(result_argv[2]);
	    if (sdp != dp) {
		error("%s: Invalid serial number", get_pname(), result_argv[2]);
		/*NOTREACHED*/
	    }
	}

	qname = quote_string(dp->name);
	switch(cmd) {

	case DONE: /* DONE <handle> <origsize> <dumpsize> <dumptime> <errstr> */
	    if(result_argc != 6) {
		error("error [dumper DONE result_argc != 6: %d]", result_argc);
		/*NOTREACHED*/
	    }

	    /*free_serial(result_argv[2]);*/

	    sched(dp)->origsize = OFF_T_ATOI(result_argv[3]);
	    sched(dp)->dumptime = TIME_T_ATOI(result_argv[5]);

	    printf("driver: finished-cmd time %s %s dumped %s:%s\n",
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
	    if(sched(dp)->attempted) {
		log_add(L_FAIL, "%s %s %s %d [too many dumper retry: %s]",
	    	    dp->host->hostname, dp->name, sched(dp)->datestamp,
	    	    sched(dp)->level, result_argv[3]);
		printf("driver: dump failed %s %s %s, too many dumper retry: %s\n",
		        result_argv[2], dp->host->hostname, dp->name,
		        result_argv[3]);
	    }
	    /* FALLTHROUGH */
	case FAILED: /* FAILED <handle> <errstr> */
	    /*free_serial(result_argv[2]);*/
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
	    /*free_serial(result_argv[2]);*/
	    dumper->result = cmd;
	    break;

	case BOGUS:
	    /* either EOF or garbage from dumper.  Turn it off */
	    log_add(L_WARNING, "%s pid %ld is messed up, ignoring it.\n",
		    dumper->name, (long)dumper->pid);
	    if (dumper->ev_read) {
		event_release(dumper->ev_read);
		dumper->ev_read = NULL;
	    }
	    aclose(dumper->fd);
	    dumper->busy = 0;
	    dumper->down = 1;	/* mark it down so it isn't used again */
	    if(dp) {
		/* if it was dumping something, zap it and try again */
		if(sched(dp)->attempted) {
	    	log_add(L_FAIL, "%s %s %s %d [%s died]",
	    		dp->host->hostname, qname, sched(dp)->datestamp,
	    		sched(dp)->level, dumper->name);
		}
		else {
	    	log_add(L_WARNING, "%s died while dumping %s:%s lev %d.",
	    		dumper->name, dp->host->hostname, qname,
	    		sched(dp)->level);
		}
	    }
	    dumper->result = cmd;
	    break;

	default:
	    assert(0);
	}
        amfree(qname);

	/* send the dumper result to the chunker */
	if(dumper->chunker->down == 0 && dumper->chunker->fd != -1 &&
	   dumper->chunker->result == LAST_TOK) {
	    if(cmd == DONE) {
		chunker_cmd(dumper->chunker, DONE, dp);
	    }
	    else {
		chunker_cmd(dumper->chunker, FAILED, dp);
	    }
	}

	if(dumper->result != LAST_TOK && dumper->chunker->result != LAST_TOK)
	    dumper_result(dp);

    } while(areads_dataready(dumper->fd));
}


static void
handle_chunker_result(
    void *	cookie)
{
    /*static int pending_aborts = 0;*/
    chunker_t *chunker = cookie;
    assignedhd_t **h=NULL;
    dumper_t *dumper;
    disk_t *dp, *sdp;
    cmd_t cmd;
    int result_argc;
    char *result_argv[MAX_ARGS+1];
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

    if(dp && sched(dp) && sched(dp)->holdp) {
	h = sched(dp)->holdp;
	activehd = sched(dp)->activehd;
    }

    do {

	short_dump_state();

	cmd = getresult(chunker->fd, 1, &result_argc, result_argv, MAX_ARGS+1);

	if(cmd != BOGUS) {
	    /* result_argv[2] always contains the serial number */
	    sdp = serial2disk(result_argv[2]);
	    if (sdp != dp) {
		error("%s: Invalid serial number", get_pname(), result_argv[2]);
		/*NOTREACHED*/
	    }
	}

	switch(cmd) {

	case PARTIAL: /* PARTIAL <handle> <dumpsize> <errstr> */
	case DONE: /* DONE <handle> <dumpsize> <errstr> */
	    if(result_argc != 4) {
		error("error [chunker %s result_argc != 4: %d]", cmdstr[cmd],
		      result_argc);
	        /*NOTREACHED*/
	    }
	    /*free_serial(result_argv[2]);*/

	    sched(dp)->dumpsize = (off_t)atof(result_argv[3]);

	    qname = quote_string(dp->name);
	    printf("driver: finished-cmd time %s %s chunked %s:%s\n",
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
	    /*free_serial(result_argv[2]);*/

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    break;

	case NO_ROOM: /* NO-ROOM <handle> <missing_size> */
	    if (!h || activehd < 0) { /* should never happen */
		error("!h || activehd < 0");
		/*NOTREACHED*/
	    }
	    h[activehd]->used -= OFF_T_ATOI(result_argv[3]);
	    h[activehd]->reserved -= OFF_T_ATOI(result_argv[3]);
	    holdalloc(h[activehd]->disk)->allocated_space -= OFF_T_ATOI(result_argv[3]);
	    h[activehd]->disk->disksize -= OFF_T_ATOI(result_argv[3]);
	    break;

	case RQ_MORE_DISK: /* RQ-MORE-DISK <handle> */
	    if (!h || activehd < 0) { /* should never happen */
		error("!h || activehd < 0");
		/*NOTREACHED*/
	    }
	    holdalloc(h[activehd]->disk)->allocated_dumpers--;
	    h[activehd]->used = h[activehd]->reserved;
	    if( h[++activehd] ) { /* There's still some allocated space left.
				   * Tell the dumper about it. */
		sched(dp)->activehd++;
		chunker_cmd( chunker, CONTINUE, dp );
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
		    chunker_cmd( chunker, CONTINUE, dp );
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

	    /*free_serial(result_argv[2]);*/

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    break;

	case BOGUS:
	    /* either EOF or garbage from chunker.  Turn it off */
	    log_add(L_WARNING, "%s pid %ld is messed up, ignoring it.\n",
		    chunker->name, (long)chunker->pid);

	    if(dp) {
		/* if it was dumping something, zap it and try again */
		if (!h || activehd < 0) { /* should never happen */
		    error("!h || activehd < 0");
		    /*NOTREACHED*/
		}
		qname = quote_string(dp->name);
		if(sched(dp)->attempted) {
		    log_add(L_FAIL, "%s %s %s %d [%s died]",
	    		    dp->host->hostname, qname, sched(dp)->datestamp,
	    		    sched(dp)->level, chunker->name);
		}
		else {
	    	    log_add(L_WARNING, "%s died while dumping %s:%s lev %d.",
	    		    chunker->name, dp->host->hostname, qname,
	    		    sched(dp)->level);
		}
        	amfree(qname);
		dp = NULL;
	    }

	    event_release(chunker->ev_read);

	    chunker->result = cmd;

	    break;

	default:
	    assert(0);
	}

	if(chunker->result != LAST_TOK && chunker->dumper->result != LAST_TOK)
	    dumper_result(dp);

    } while(areads_dataready(chunker->fd));
}


static disklist_t
read_flush(void)
{
    sched_t *sp;
    disk_t *dp;
    int line;
    dumpfile_t file;
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

    tq.head = tq.tail = NULL;

    for(line = 0; (inpline = agets(stdin)) != NULL; free(inpline)) {
	line++;
	if (inpline[0] == '\0')
	    continue;

	s = inpline;
	ch = *s++;

	skip_whitespace(s, ch);                 /* find the command */
	if(ch == '\0') {
	    error("flush line %d: syntax error (no command)", line);
	    /*NOTREACHED*/
	}
	command = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	if(strcmp(command,"ENDFLUSH") == 0) {
	    break;
	}

	if(strcmp(command,"FLUSH") != 0) {
	    error("flush line %d: syntax error (%s != FLUSH)", line, command);
	    /*NOTREACHED*/
	}

	skip_whitespace(s, ch);			/* find the hostname */
	if(ch == '\0') {
	    error("flush line %d: syntax error (no hostname)", line);
	    /*NOTREACHED*/
	}
	hostname = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the diskname */
	if(ch == '\0') {
	    error("flush line %d: syntax error (no diskname)", line);
	    /*NOTREACHED*/
	}
	qname = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	diskname = unquote_string(qname);

	skip_whitespace(s, ch);			/* find the datestamp */
	if(ch == '\0') {
	    error("flush line %d: syntax error (no datestamp)", line);
	    /*NOTREACHED*/
	}
	datestamp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the level number */
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    error("flush line %d: syntax error (bad level)", line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the filename */
	if(ch == '\0') {
	    error("flush line %d: syntax error (no filename)", line);
	    /*NOTREACHED*/
	}
	destname = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	get_dumpfile(destname, &file);
	if( file.type != F_DUMPFILE) {
	    if( file.type != F_CONT_DUMPFILE )
		log_add(L_INFO, "%s: ignoring cruft file.", destname);
	    amfree(diskname);
	    continue;
	}

	if(strcmp(hostname, file.name) != 0 ||
	   strcmp(diskname, file.disk) != 0 ||
	   strcmp(datestamp, file.datestamp) != 0) {
	    log_add(L_INFO, "disk %s:%s not consistent with file %s",
		    hostname, diskname, destname);
	    amfree(diskname);
	    continue;
	}
	amfree(diskname);

	dp = lookup_disk(file.name, file.disk);

	if (dp == NULL) {
	    log_add(L_INFO, "%s: disk %s:%s not in database, skipping it.",
		    destname, file.name, file.disk);
	    continue;
	}

	if(file.dumplevel < 0 || file.dumplevel > 9) {
	    log_add(L_INFO, "%s: ignoring file with bogus dump level %d.",
		    destname, file.dumplevel);
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
	sp->destname = stralloc(destname);
	sp->level = file.dumplevel;
	sp->dumpdate = NULL;
	sp->degr_dumpdate = NULL;
	sp->datestamp = stralloc(file.datestamp);
	sp->est_nsize = (off_t)0;
	sp->est_csize = (off_t)0;
	sp->est_time = 0;
	sp->est_kps = 10;
	sp->priority = 0;
	sp->degr_level = -1;
	sp->attempted = 0;
	sp->act_size = size_holding_files(destname, 0);
	sp->holdp = build_diskspace(destname);
	if(sp->holdp == NULL) continue;
	sp->dumper = NULL;
	sp->timestamp = (time_t)0;

	dp1->up = (char *)sp;

	enqueue_disk(&tq, dp1);
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
    char *dumpdate, *degr_dumpdate;
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
	    error("schedule line %d: syntax error (no command)", line);
	    /*NOTREACHED*/
	}
	command = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	if(strcmp(command,"DUMP") != 0) {
	    error("schedule line %d: syntax error (%s != DUMP)", line, command);
	    /*NOTREACHED*/
	}

	skip_whitespace(s, ch);			/* find the host name */
	if(ch == '\0') {
	    error("schedule line %d: syntax error (no host name)", line);
	    /*NOTREACHED*/
	}
	hostname = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the feature list */
	if(ch == '\0') {
	    error("schedule line %d: syntax error (no feature list)", line);
	    /*NOTREACHED*/
	}
	features = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the disk name */
	if(ch == '\0') {
	    error("schedule line %d: syntax error (no disk name)", line);
	    /*NOTREACHED*/
	}
	qname = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	diskname = unquote_string(qname);

	skip_whitespace(s, ch);			/* find the datestamp */
	if(ch == '\0') {
	    error("schedule line %d: syntax error (no datestamp)", line);
	    /*NOTREACHED*/
	}
	datestamp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the priority number */
	if(ch == '\0' || sscanf(s - 1, "%d", &priority) != 1) {
	    error("schedule line %d: syntax error (bad priority)", line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the level number */
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    error("schedule line %d: syntax error (bad level)", line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the dump date */
	if(ch == '\0') {
	    error("schedule line %d: syntax error (bad dump date)", line);
	    /*NOTREACHED*/
	}
	dumpdate = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	skip_whitespace(s, ch);			/* find the native size */
	if(ch == '\0' || sscanf(s - 1, OFF_T_FMT, 
				(OFF_T_FMT_TYPE *)&nsize) != 1) {
	    error("schedule line %d: syntax error (bad nsize)", line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the compressed size */
	if(ch == '\0' || sscanf(s - 1, OFF_T_FMT, 
				(OFF_T_FMT_TYPE *)&csize) != 1) {
	    error("schedule line %d: syntax error (bad csize)", line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the time number */
	if(ch == '\0' || sscanf(s - 1, TIME_T_FMT,
				(TIME_T_FMT_TYPE *)time_p) != 1) {
	    error("schedule line %d: syntax error (bad estimated time)", line);
	    /*NOTREACHED*/
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the kps number */
	if(ch == '\0' || sscanf(s - 1, "%lu", &kps) != 1) {
	    error("schedule line %d: syntax error (bad kps)", line);
	    continue;
	}
	skip_integer(s, ch);

	degr_dumpdate = NULL;			/* flag if degr fields found */
	skip_whitespace(s, ch);			/* find the degr level number */
	if(ch != '\0') {
	    if(sscanf(s - 1, "%d", &degr_level) != 1) {
		error("schedule line %d: syntax error (bad degr level)", line);
		/*NOTREACHED*/
	    }
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr dump date */
	    if(ch == '\0') {
		error("schedule line %d: syntax error (bad degr dump date)", line);
		/*NOTREACHED*/
	    }
	    degr_dumpdate = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';

	    skip_whitespace(s, ch);		/* find the degr native size */
	    if(ch == '\0'  || sscanf(s - 1, OFF_T_FMT, 
			(OFF_T_FMT_TYPE *)&degr_nsize) != 1) {
		error("schedule line %d: syntax error (bad degr nsize)", line);
		/*NOTREACHED*/
	    }
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr compressed size */
	    if(ch == '\0'  || sscanf(s - 1, OFF_T_FMT, 
			(OFF_T_FMT_TYPE *)&degr_csize) != 1) {
		error("schedule line %d: syntax error (bad degr csize)", line);
		/*NOTREACHED*/
	    }
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr time number */
	    if(ch == '\0' || sscanf(s - 1, TIME_T_FMT,
				(TIME_T_FMT_TYPE *)degr_time_p) != 1) {
		error("schedule line %d: syntax error (bad degr estimated time)", line);
		/*NOTREACHED*/
	    }
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the degr kps number */
	    if(ch == '\0' || sscanf(s - 1, "%lu", &degr_kps) != 1) {
		error("schedule line %d: syntax error (bad degr kps)", line);
		/*NOTREACHED*/
	    }
	    skip_integer(s, ch);
	}

	dp = lookup_disk(hostname, diskname);
	if(dp == NULL) {
	    log_add(L_WARNING,
		    "schedule line %d: %s:'%s' not in disklist, ignored",
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
	} else {
	    sp->degr_level = -1;
	    sp->degr_dumpdate = NULL;
	}
	/*@end@*/

	sp->attempted = 0;
	sp->act_size = (off_t)0;
	sp->holdp = NULL;
	sp->activehd = -1;
	sp->dumper = NULL;
	sp->timestamp = (time_t)0;
	sp->destname = NULL;
	sp->no_space = 0;

	dp->up = (char *) sp;
	if(dp->host->features == NULL) {
	    dp->host->features = am_string_to_feature(features);
	}
	remove_disk(&waitq, dp);
	enqueue_disk(&runq, dp);
	flush_size += sp->act_size;
	amfree(diskname);
    }
    printf("driver: flush size " OFF_T_FMT "\n", (OFF_T_FMT_TYPE)flush_size);
    amfree(inpline);
    if(line == 0)
	log_add(L_WARNING, "WARNING: got empty schedule from planner");
    if(need_degraded==1) start_degraded_mode(&runq);
    start_some_dumps(&runq);
}

static unsigned long
free_kps(
    interface_t *ip)
{
    unsigned long res;

    if (ip == (interface_t *)0) {
	interface_t *p;
	unsigned long maxusage=0;
	unsigned long curusage=0;
	for(p = lookup_interface(NULL); p != NULL; p = p->next) {
	    maxusage += interface_get_maxusage(p);
	    curusage += p->curusage;
	}
	res = maxusage - curusage;
#ifndef __lint
    } else {
	res = interface_get_maxusage(ip) - ip->curusage;
#endif
    }

    return res;
}

static void
interface_state(
    char *time_str)
{
    interface_t *ip;

    printf("driver: interface-state time %s", time_str);

    for(ip = lookup_interface(NULL); ip != NULL; ip = ip->next) {
	printf(" if %s: free %lu", ip->name, free_kps(ip));
    }
    printf("\n");
}

static void
allocate_bandwidth(
    interface_t *	ip,
    unsigned long	kps)
{
    ip->curusage += kps;
}

static void
deallocate_bandwidth(
    interface_t *	ip,
    unsigned long	kps)
{
    assert(kps <= ip->curusage);
    ip->curusage -= kps;
}

/* ------------ */
static off_t
free_space(void)
{
    holdingdisk_t *hdp;
    off_t total_free;
    off_t diff;

    total_free = (off_t)0;
    for(hdp = getconf_holdingdisks(); hdp != NULL; hdp = hdp->next) {
	diff = hdp->disksize - holdalloc(hdp)->allocated_space;
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
    holdingdisk_t *minp, *hdp;
    int i=0, num_holdingdisks=0; /* are we allowed to use the global thing? */
    int j, minj;
    char *used;
    off_t halloc, dalloc, hfree, dfree;

    (void)cur_idle;	/* Quiet unused parameter warning */

    if (size < 2*DISK_BLOCK_KB)
	size = 2*DISK_BLOCK_KB;
    size = am_round(size, (off_t)DISK_BLOCK_KB);

#ifdef HOLD_DEBUG
    printf("%s: want " OFF_T_FMT " K\n", debug_prefix_time(": find_diskspace"),
	   (OFF_T_FMT_TYPE)size);
    fflush(stdout);
#endif

    for(hdp = getconf_holdingdisks(); hdp != NULL; hdp = hdp->next) {
	num_holdingdisks++;
    }

    used = alloc(SIZEOF(*used) * num_holdingdisks);/*disks used during this run*/
    memset( used, 0, (size_t)num_holdingdisks );
    result = alloc(SIZEOF(assignedhd_t *) * (num_holdingdisks + 1));
    result[0] = NULL;

    while( i < num_holdingdisks && size > (off_t)0 ) {
	/* find the holdingdisk with the fewest active dumpers and among
	 * those the one with the biggest free space
	 */
	minp = NULL; minj = -1;
	for(j = 0, hdp = getconf_holdingdisks(); hdp != NULL; hdp = hdp->next, j++ ) {
	    if( pref && pref->disk == hdp && !used[j] &&
		holdalloc(hdp)->allocated_space <= hdp->disksize - (off_t)DISK_BLOCK_KB) {
		minp = hdp;
		minj = j;
		break;
	    }
	    else if( holdalloc(hdp)->allocated_space <= hdp->disksize - (off_t)(2*DISK_BLOCK_KB) &&
		!used[j] &&
		(!minp ||
		 holdalloc(hdp)->allocated_dumpers < holdalloc(minp)->allocated_dumpers ||
		 (holdalloc(hdp)->allocated_dumpers == holdalloc(minp)->allocated_dumpers &&
		  hdp->disksize-holdalloc(hdp)->allocated_space > minp->disksize-holdalloc(minp)->allocated_space)) ) {
		minp = hdp;
		minj = j;
	    }
	}

	pref = NULL;
	if( !minp ) { break; } /* all holding disks are full */
	used[minj] = 1;

	/* hfree = free space on the disk */
	hfree = minp->disksize - holdalloc(minp)->allocated_space;

	/* dfree = free space for data, remove 1 header for each chunksize */
	dfree = hfree - (((hfree-(off_t)1)/holdingdisk_get_chunksize(minp))+(off_t)1) * (off_t)DISK_BLOCK_KB;

	/* dalloc = space I can allocate for data */
	dalloc = ( dfree < size ) ? dfree : size;

	/* halloc = space to allocate, including 1 header for each chunksize */
	halloc = dalloc + (((dalloc-(off_t)1)/holdingdisk_get_chunksize(minp))+(off_t)1) * (off_t)DISK_BLOCK_KB;

#ifdef HOLD_DEBUG
	printf("%s: find diskspace: size " OFF_T_FMT " hf " OFF_T_FMT
	       " df " OFF_T_FMT " da " OFF_T_FMT " ha " OFF_T_FMT "\n",
	       debug_prefix_time(": find_diskspace"),
	       (OFF_T_FMT_TYPE)size,
	       (OFF_T_FMT_TYPE)hfree,
	       (OFF_T_FMT_TYPE)dfree,
	       (OFF_T_FMT_TYPE)dalloc,
	       (OFF_T_FMT_TYPE)halloc);
	fflush(stdout);
#endif
	size -= dalloc;
	result[i] = alloc(SIZEOF(assignedhd_t));
	result[i]->disk = minp;
	result[i]->reserved = halloc;
	result[i]->used = (off_t)0;
	result[i]->destname = NULL;
	result[i+1] = NULL;
	i++;
    } /* while i < num_holdingdisks && size > 0 */
    amfree(used);

    if(size != (off_t)0) { /* not enough space available */
	printf("find diskspace: not enough diskspace. Left with "
	       OFF_T_FMT " K\n", (OFF_T_FMT_TYPE)size);
	fflush(stdout);
	free_assignedhd(result);
	result = NULL;
    }

#ifdef HOLD_DEBUG
    for( i = 0; result && result[i]; i++ ) {
	printf("%s: find diskspace: selected %s free " OFF_T_FMT " reserved " OFF_T_FMT " dumpers %d\n",
		debug_prefix_time(": find_diskspace"),
		holdingdisk_get_diskdir(result[i]->disk),
		(OFF_T_FMT_TYPE)(result[i]->disk->disksize -
		  holdalloc(result[i]->disk)->allocated_space),
		(OFF_T_FMT_TYPE)result[i]->reserved,
		holdalloc(result[i]->disk)->allocated_dumpers);
    }
    fflush(stdout);
#endif

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

    snprintf( lvl, SIZEOF(lvl), "%d", sched(diskp)->level );

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
	    holdalloc(holdp[0]->disk)->allocated_space += holdp[0]->reserved;
	    size = (holdp[0]->reserved>size) ? (off_t)0 : size-holdp[0]->reserved;
	    qname = quote_string(diskp->name);
#ifdef HOLD_DEBUG
	    printf("%s: merging holding disk %s to disk %s:%s, add " OFF_T_FMT " for reserved " OFF_T_FMT ", left " OFF_T_FMT "\n",
		   debug_prefix_time(": assign_holdingdisk"),
		   holdingdisk_get_diskdir(sched(diskp)->holdp[j-1]->disk),
		   diskp->host->hostname, qname,
		   (OFF_T_FMT_TYPE)holdp[0]->reserved,
		   (OFF_T_FMT_TYPE)sched(diskp)->holdp[j-1]->reserved,
		   (OFF_T_FMT_TYPE)size);
	    fflush(stdout);
#endif
	    i++;
	    amfree(qname);
	    amfree(holdp[0]);
	    l=j-1;
	}
    }

    /* copy assignedhd_s to sched(diskp), adjust allocated_space */
    for( ; holdp[i]; i++ ) {
	holdp[i]->destname = newvstralloc( holdp[i]->destname,
					   holdingdisk_get_diskdir(holdp[i]->disk), "/",
					   hd_driver_timestamp, "/",
					   diskp->host->hostname, ".",
					   sfn, ".",
					   lvl, NULL );
	sched(diskp)->holdp[j++] = holdp[i];
	holdalloc(holdp[i]->disk)->allocated_space += holdp[i]->reserved;
	size = (holdp[i]->reserved > size) ? (off_t)0 :
		  (size - holdp[i]->reserved);
	qname = quote_string(diskp->name);
#ifdef HOLD_DEBUG
	printf("%s: %d assigning holding disk %s to disk %s:%s, reserved " OFF_T_FMT ", left " OFF_T_FMT "\n",
		debug_prefix_time(": assign_holdingdisk"),
		i, holdingdisk_get_diskdir(holdp[i]->disk), diskp->host->hostname, qname,
		(OFF_T_FMT_TYPE)holdp[i]->reserved,
		(OFF_T_FMT_TYPE)size);
	fflush(stdout);
#endif
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
#ifdef HOLD_DEBUG
    printf("%s: %s:%s %s\n",
	   debug_prefix_time(": adjust_diskspace"),
	   diskp->host->hostname, qname, qdest);
    fflush(stdout);
#endif

    holdp = sched(diskp)->holdp;

    assert(holdp != NULL);

    for( i = 0; holdp[i]; i++ ) { /* for each allocated disk */
	diff = holdp[i]->used - holdp[i]->reserved;
	total += holdp[i]->used;
	holdalloc(holdp[i]->disk)->allocated_space += diff;
	hqname = quote_string(holdp[i]->disk->name);
#ifdef HOLD_DEBUG
	printf("%s: hdisk %s done, reserved " OFF_T_FMT " used " OFF_T_FMT " diff " OFF_T_FMT " alloc " OFF_T_FMT " dumpers %d\n",
		debug_prefix_time(": adjust_diskspace"),
		holdp[i]->disk->name,
		(OFF_T_FMT_TYPE)holdp[i]->reserved,
		(OFF_T_FMT_TYPE)holdp[i]->used,
		(OFF_T_FMT_TYPE)diff,
		(OFF_T_FMT_TYPE)holdalloc(holdp[i]->disk)->allocated_space,
		holdalloc(holdp[i]->disk)->allocated_dumpers );
	fflush(stdout);
#endif
	holdp[i]->reserved += diff;
	amfree(hqname);
    }

    sched(diskp)->act_size = total;

#ifdef HOLD_DEBUG
    printf("%s: after: disk %s:%s used " OFF_T_FMT "\n",
	   debug_prefix_time(": adjust_diskspace"),
	   diskp->host->hostname, qname,
	   (OFF_T_FMT_TYPE)sched(diskp)->act_size);
    fflush(stdout);
#endif
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
	holdalloc(holdp[i]->disk)->allocated_space -= holdp[i]->used;
    }

    unlink_holding_files(holdp[0]->destname);	/* no need for the entire list,
						 * because unlink_holding_files
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
    ssize_t buflen;
    char buffer[DISK_BLOCK_BYTES];
    dumpfile_t file;
    assignedhd_t **result;
    holdingdisk_t *hdp;
    off_t *used;
    int num_holdingdisks=0;
    char dirname[1000], *ch;
    struct stat finfo;
    char *filename = destname;

    memset(buffer, 0, sizeof(buffer));
    for(hdp = getconf_holdingdisks(); hdp != NULL; hdp = hdp->next) {
        num_holdingdisks++;
    }
    used = alloc(SIZEOF(off_t) * num_holdingdisks);
    for(i=0;i<num_holdingdisks;i++)
	used[i] = (off_t)0;
    result = alloc(SIZEOF(assignedhd_t *) * (num_holdingdisks + 1));
    result[0] = NULL;
    while(filename != NULL && filename[0] != '\0') {
	strncpy(dirname, filename, 999);
	dirname[999]='\0';
	ch = strrchr(dirname,'/');
        *ch = '\0';
	ch = strrchr(dirname,'/');
        *ch = '\0';

	for(j = 0, hdp = getconf_holdingdisks(); hdp != NULL;
						 hdp = hdp->next, j++ ) {
	    if(strcmp(dirname, holdingdisk_get_diskdir(hdp))==0) {
		break;
	    }
	}

	if(stat(filename, &finfo) == -1) {
	    fprintf(stderr, "stat %s: %s\n", filename, strerror(errno));
	    finfo.st_size = (off_t)0;
	}
	used[j] += ((off_t)finfo.st_size+(off_t)1023)/(off_t)1024;
	if((fd = open(filename,O_RDONLY)) == -1) {
	    fprintf(stderr,"build_diskspace: open of %s failed: %s\n",
		    filename, strerror(errno));
	    return NULL;
	}
	if ((buflen = fullread(fd, buffer, SIZEOF(buffer))) > 0) {;
		parse_file_header(buffer, &file, (size_t)buflen);
	}
	close(fd);
	filename = file.cont_filename;
    }

    for(j = 0, i=0, hdp = getconf_holdingdisks(); hdp != NULL;
						  hdp = hdp->next, j++ ) {
	if(used[j] != (off_t)0) {
	    result[i] = alloc(SIZEOF(assignedhd_t));
	    result[i]->disk = hdp;
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
    holdingdisk_t *hdp;
    int dsk;
    off_t diff;

    printf("driver: hdisk-state time %s", time_str);

    for(hdp = getconf_holdingdisks(), dsk = 0; hdp != NULL; hdp = hdp->next, dsk++) {
	diff = hdp->disksize - holdalloc(hdp)->allocated_space;
	printf(" hdisk %d: free " OFF_T_FMT " dumpers %d", dsk,
	       (OFF_T_FMT_TYPE)diff, holdalloc(hdp)->allocated_dumpers);
    }
    printf("\n");
}

static void
update_failed_dump_to_tape(
    disk_t *	dp)
{
/* JLM
 * should simply set no_bump
 */

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
dump_to_tape(
    disk_t *	dp)
{
    dumper_t *dumper;
    int failed = 0;
    off_t filenum;
    off_t origsize = (off_t)0;
    off_t dumpsize = (off_t)0;
    time_t dumptime = (time_t)0;
    double tapetime = 0.0;
    cmd_t cmd;
    int result_argc, rc;
    char *result_argv[MAX_ARGS+1];
    int dumper_tryagain = 0;
    char *qname;

    qname = quote_string(dp->name);
    printf("driver: dumping %s:%s directly to tape\n",
	   dp->host->hostname, qname);
    fflush(stdout);

    /* pick a dumper and fail if there are no idle dumpers */

    dumper = idle_dumper();
    if (!dumper) {
	printf("driver: no idle dumpers for %s:%s.\n", 
		dp->host->hostname, qname);
	fflush(stdout);
	log_add(L_WARNING, "no idle dumpers for %s:%s.\n",
	        dp->host->hostname, qname);
        amfree(qname);
	return 2;	/* fatal problem */
    }

    /* tell the taper to read from a port number of its choice */

    taper_cmd(PORT_WRITE, dp, NULL, sched(dp)->level, sched(dp)->datestamp);
    cmd = getresult(taper, 1, &result_argc, result_argv, MAX_ARGS+1);
    if(cmd != PORT) {
	printf("driver: did not get PORT from taper for %s:%s\n",
		dp->host->hostname, qname);
	fflush(stdout);
        amfree(qname);
	return 2;	/* fatal problem */
    }
    /* copy port number */
    dumper->output_port = atoi(result_argv[2]);

    /* tell the dumper to dump to a port */

    dumper_cmd(dumper, PORT_DUMP, dp);
    dp->host->start_t = time(NULL) + 15;

    /* update statistics & print state */

    taper_busy = dumper->busy = 1;
    dp->host->inprogress += 1;
    dp->inprogress = 1;
    sched(dp)->timestamp = time((time_t *)0);
    allocate_bandwidth(dp->host->netif, sched(dp)->est_kps);
    idle_reason = NOT_IDLE;

    short_dump_state();

    /* wait for result from dumper */

    cmd = getresult(dumper->fd, 1, &result_argc, result_argv, MAX_ARGS+1);

    switch(cmd) {
    case BOGUS:
	/* either eof or garbage from dumper */
	log_add(L_WARNING, "%s pid %ld is messed up, ignoring it.\n",
	        dumper->name, (long)dumper->pid);
	dumper->down = 1;	/* mark it down so it isn't used again */
	failed = 1;	/* dump failed, must still finish up with taper */
	break;

    case DONE: /* DONE <handle> <origsize> <dumpsize> <dumptime> <errstr> */
	/* everything went fine */
	origsize = (off_t)atof(result_argv[3]);
	/*dumpsize = (off_t)atof(result_argv[4]);*/
	dumptime = (time_t)atof(result_argv[5]);
	break;

    case NO_ROOM: /* NO-ROOM <handle> */
	dumper_cmd(dumper, ABORT, dp);
	cmd = getresult(dumper->fd, 1, &result_argc, result_argv, MAX_ARGS+1);
	assert(cmd == ABORT_FINISHED);

    case TRYAGAIN: /* TRY-AGAIN <handle> <errstr> */
    default:
	/* dump failed, but we must still finish up with taper */
	/* problem with dump, possibly nonfatal, retry one time */
	sched(dp)->attempted++;
	failed = sched(dp)->attempted;
	dumper_tryagain = 1;
	break;
	
    case FAILED: /* FAILED <handle> <errstr> */
	/* dump failed, but we must still finish up with taper */
	failed = 2;     /* fatal problem with dump */
	break;
    }

    /*
     * Note that at this point, even if the dump above failed, it may
     * not be a fatal failure if taper below says we can try again.
     * E.g. a dumper failure above may actually be the result of a
     * tape overflow, which in turn causes dump to see "broken pipe",
     * "no space on device", etc., since taper closed the port first.
     */

    continue_port_dump:

    cmd = getresult(taper, 1, &result_argc, result_argv, MAX_ARGS+1);

    switch(cmd) {
    case PARTIAL:
    case DONE: /* DONE <handle> <label> <tape file> <err mess> */
	if(result_argc != 5) {
	    error("error [dump to tape DONE result_argc != 5: %d]", result_argc);
	    /*NOTREACHED*/
	}

	if(failed == 1) goto tryagain;	/* dump didn't work */
	else if(failed == 2) goto failed_dumper;

	free_serial(result_argv[2]);

	if (*result_argv[5] == '"') {
	    /* String was quoted */
	    rc = sscanf(result_argv[5],"\"[sec %lf kb " OFF_T_FMT " ",
			&tapetime, (OFF_T_FMT_TYPE *)&dumpsize);
	} else {
	    /* String was not quoted */
	    rc = sscanf(result_argv[5],"[sec %lf kb " OFF_T_FMT " ",
			&tapetime, (OFF_T_FMT_TYPE *)&dumpsize);
	}
	if (rc < 2) {
	    error("error [malformed result: %d items matched in '%s']",
		  rc, result_argv[5]);
	    /*NOTREACHED*/
	}

	if(cmd == DONE) {
	    /* every thing went fine */
	    update_info_dumper(dp, origsize, dumpsize, dumptime);
	    filenum = OFF_T_ATOI(result_argv[4]);
	    update_info_taper(dp, result_argv[3], filenum, sched(dp)->level);
	    /* note that update_info_dumper() must be run before
	       update_info_taper(), since update_info_dumper overwrites
	       tape information.  */
	}

	break;

    case TRYAGAIN: /* TRY-AGAIN <handle> <err mess> */
	tape_left = tape_length;
	current_tape++;
	if(dumper_tryagain == 0) {
	    sched(dp)->attempted++;
	    if(sched(dp)->attempted > failed)
		failed = sched(dp)->attempted;
	}
    tryagain:
	if(failed <= 1)
	    headqueue_disk(&runq, dp);
    failed_dumper:
	update_failed_dump_to_tape(dp);
	free_serial(result_argv[2]);
	break;

    case SPLIT_CONTINUE:  /* SPLIT_CONTINUE <handle> <new_label> */
        if (result_argc != 3) {
            error("error [taper SPLIT_CONTINUE result_argc != 3: %d]", result_argc);
	    /*NOTREACHED*/
        }
        fprintf(stderr, "driver: Got SPLIT_CONTINUE %s %s\n",
		result_argv[2], result_argv[3]);
        goto continue_port_dump;

    case SPLIT_NEEDNEXT:
        fprintf(stderr, "driver: Got SPLIT_NEEDNEXT %s %s\n", result_argv[2], result_argv[3]);

        goto continue_port_dump;

    case TAPE_ERROR: /* TAPE-ERROR <handle> <err mess> */
    case BOGUS:
    default:
	update_failed_dump_to_tape(dp);
	free_serial(result_argv[2]);
	failed = 2;	/* fatal problem */
	start_degraded_mode(&runq);
	break;
    }

    /* reset statistics & return */

    taper_busy = dumper->busy = 0;
    dp->host->inprogress -= 1;
    dp->inprogress = 0;
    deallocate_bandwidth(dp->host->netif, sched(dp)->est_kps);
    amfree(qname);

    return failed;
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

    printf("driver: state time %s ", wall_time);
    printf("free kps: %lu space: " OFF_T_FMT " taper: ",
	   free_kps((interface_t *)0),
	   (OFF_T_FMT_TYPE)free_space());
    if(degraded_mode) printf("DOWN");
    else if(!taper_busy) printf("idle");
    else printf("writing");
    nidle = 0;
    for(i = 0; i < inparallel; i++) if(!dmptable[i].busy) nidle++;
    printf(" idle-dumpers: %d", nidle);
    printf(" qlen tapeq: %d", queue_length(tapeq));
    printf(" runq: %d", queue_length(runq));
    printf(" roomq: %d", queue_length(roomq));
    printf(" wakeup: %d", (int)sleep_time);
    printf(" driver-idle: %s\n", idle_strings[idle_reason]);
    interface_state(wall_time);
    holdingdisk_state(wall_time);
    fflush(stdout);
}

#if 0
static void
dump_state(
    const char *str)
{
    int i;
    disk_t *dp;
    char *qname;

    printf("================\n");
    printf("driver state at time %s: %s\n", walltime_str(curclock()), str);
    printf("free kps: %lu, space: " OFF_T_FMT "\n",
    	   free_kps((interface_t *)0),
    	   (OFF_T_FMT_TYPE)free_space());
    if(degraded_mode) printf("taper: DOWN\n");
    else if(!taper_busy) printf("taper: idle\n");
    else printf("taper: writing %s:%s.%d est size " OFF_T_FMT "\n",
		taper_disk->host->hostname, taper_disk->name,
		sched(taper_disk)->level,
		sched(taper_disk)->est_size);
    for(i = 0; i < inparallel; i++) {
	dp = dmptable[i].dp;
	if(!dmptable[i].busy)
	  printf("%s: idle\n", dmptable[i].name);
	else
	  qname = quote_string(dp->name);
	  printf("%s: dumping %s:%s.%d est kps %d size " OFF_T_FMT " time %lu\n",
		dmptable[i].name, dp->host->hostname, qname, sched(dp)->level,
		sched(dp)->est_kps, sched(dp)->est_size, sched(dp)->est_time);
          amfree(qname);
    }
    dump_queue("TAPE", tapeq, 5, stdout);
    dump_queue("ROOM", roomq, 5, stdout);
    dump_queue("RUN ", runq, 5, stdout);
    printf("================\n");
    fflush(stdout);
}
#endif
