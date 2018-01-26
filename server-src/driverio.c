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
 * $Id: driverio.c,v 1.92 2006/08/24 01:57:16 paddy_s Exp $
 *
 * I/O-related functions for driver program
 */
#include "amanda.h"
#include "amutil.h"
#include "clock.h"
#include "server_util.h"
#include "conffile.h"
#include "diskfile.h"
#include "infofile.h"
#include "logfile.h"
#include "timestamp.h"

#define GLOBAL		/* the global variables defined here */
#include "driverio.h"

static const char *childstr(int);
static long generation = 1;

typedef struct serial_s {
    long   gen;
    job_t *job;
} serial_t;

static int max_serial;
static serial_t *stable;
static int max_jobs;
static job_t *jobs;

static taper_t * start_one_tape_process(char *taper_program,
					char *storage_name,
					gboolean  no_taper,
					int nb_taper);

void
init_driverio(
    int inparallel,
    int nb_storage,
    int sum_taper_parallel_write)
{
    dumper_t *dumper;

    tapetable = g_new0(taper_t, nb_storage+1);
    dmptable = g_new0(dumper_t, inparallel+1);
    chktable = g_new0(chunker_t, inparallel+1);
    for(dumper = dmptable; dumper < dmptable + inparallel; dumper++) {
	dumper->fd = -1;
    }

    max_serial = inparallel + sum_taper_parallel_write;
    stable = g_new0(serial_t, max_serial);
    max_jobs = inparallel + sum_taper_parallel_write;
    jobs   = g_new0(job_t, max_jobs);
}


static const char *
childstr(
    int fd)
{
    static char buf[NUM_STR_SIZE + 32];
    dumper_t  *dumper;
    chunker_t *chunker;
    taper_t   *taper;

    for (taper = tapetable; taper->fd != 0; taper++) {
	if (taper->fd == fd)
	    return (taper->name);
    }

    for (dumper = dmptable; dumper->fd != 0; dumper++) {
	if (dumper->fd == fd)
	    return (dumper->name);
    }
    for (chunker = chktable; chunker->fd != 0; chunker++) {
	if (chunker->fd == fd)
	    return (chunker->name);
    }
    g_snprintf(buf, sizeof(buf), _("unknown child (fd %d)"), fd);
    return (buf);
}


static int nb_taper = 0;
int
startup_dump_tape_process(
    char      *taper_program,
    gboolean   no_taper)
{
    identlist_t  il;
    taper_t     *taper;

    for (il = getconf_identlist(CNF_STORAGE); il != NULL; il = il->next) {
	taper = start_one_tape_process(taper_program, (char *)il->data, no_taper, nb_taper);
	if (taper) {
	    taper->flush_storage = TRUE;
	    nb_taper++;
	}
    }
    return nb_taper;
}

int
startup_vault_tape_process(
    char      *taper_program,
    gboolean   no_taper)
{
    identlist_t  il;
    taper_t     *taper;

    for (il = getconf_identlist(CNF_VAULT_STORAGE); il != NULL; il = il->next) {
	char *storage_name = (char *)il->data;
	gboolean found = FALSE;
	for (taper = tapetable; taper < tapetable+nb_taper ; taper++) {
	    if (strcmp(taper->storage_name, storage_name) == 0) {
		taper->vault_storage = TRUE;
		found = TRUE;
		break;
	    }
	}
	if (!found) {
	    taper = start_one_tape_process(taper_program, storage_name, no_taper, nb_taper);
	    if (taper) {
		taper->vault_storage = TRUE;
		nb_taper++;
	    }
	}
    }
    return nb_taper;
}

static taper_t *
start_one_tape_process(
    char      *taper_program,
    char      *storage_n,
    gboolean   no_taper,
    int        nb_taper)
{
    int       fd[2];
    int       nb_wtaper;
    int       nb_worker;
    int       i;
    char    **config_options;
    char    **env;
    taper_t  *taper;
    wtaper_t *wtaper;

    storage_t *storage = lookup_storage(storage_n);
    char *tapetype;
    tapetype_t *tape;
    int   conf_flush_threshold_dumped;
    int   conf_flush_threshold_scheduled;
    int   conf_taperflush;

    taper = &tapetable[nb_taper];
    taper->pid = -1;
    for (i=0; i<nb_taper; i++) {
	if (tapetable[i].storage_name &&
	    g_str_equal(storage_n, tapetable[i].storage_name)) {
	    return NULL;
	}
    }

	taper->name = g_strdup_printf("taper%d", nb_taper);
	taper->storage_name = g_strdup(storage_n);
	taper->ev_read = NULL;
	taper->nb_wait_reply = 0;
	nb_worker = storage_get_taper_parallel_write(storage);
	taper->runtapes = storage_get_runtapes(storage);
	if (nb_worker > taper->runtapes)
	    nb_worker = taper->runtapes;
	taper->nb_worker = nb_worker;
	tapetype = storage_get_tapetype(storage);
	tape = lookup_tapetype(tapetype);
	taper->tape_length = tapetype_get_length(tape);
	taper->current_tape = 0;
	conf_flush_threshold_dumped = storage_get_flush_threshold_dumped(storage);
	conf_flush_threshold_scheduled = storage_get_flush_threshold_scheduled(storage);
	conf_taperflush = storage_get_taperflush(storage);
	taper->flush_threshold_dumped = (conf_flush_threshold_dumped * taper->tape_length) /100;
	taper->flush_threshold_scheduled = (conf_flush_threshold_scheduled * taper->tape_length) /100;
	taper->taperflush = (conf_taperflush * taper->tape_length) /100;
	g_debug("storage %s: tape_length %lld", storage_name(storage), (long long)taper->tape_length);
	g_debug("storage %s: flush_threshold_dumped %lld", storage_name(storage), (long long)taper->flush_threshold_dumped);
	g_debug("storage %s: flush_threshold_scheduled  %lld", storage_name(storage), (long long)taper->flush_threshold_scheduled );
	g_debug("storage %s: taperflush %lld", storage_name(storage), (long long)taper->taperflush);
	taper->max_dle_by_volume = storage_get_max_dle_by_volume(storage);
	taper->tapeq.head = NULL;
	taper->tapeq.tail = NULL;
	taper->vaultqss = NULL;
	taper->degraded_mode = no_taper;
	taper->down = FALSE;

	taper->wtapetable = g_new0(wtaper_t, tapetable[nb_taper].nb_worker + 1);
	if (!taper->wtapetable) {
	    error(_("could not g_malloc tapetable"));
	    /*NOTREACHED*/
	}

	for (wtaper = taper->wtapetable, nb_wtaper = 0; nb_wtaper < nb_worker; wtaper++, nb_wtaper++) {
	    wtaper->name = g_strdup_printf("worker%d-%d", nb_taper, nb_wtaper);
	    wtaper->sendresult = 0;
	    wtaper->input_error = NULL;
	    wtaper->tape_error = NULL;
	    wtaper->result = LAST_TOK;
	    wtaper->job = NULL;
	    wtaper->first_label = NULL;
	    wtaper->first_fileno = 0;
	    wtaper->state = TAPER_STATE_DEFAULT;
	    wtaper->left = 0;
	    wtaper->written = 0;
	    wtaper->vaultqs.src_labels_str = NULL;
	    wtaper->vaultqs.src_labels = NULL;
	    wtaper->vaultqs.vaultq.head = NULL;
	    wtaper->vaultqs.vaultq.tail = NULL;
	    wtaper->taper = taper;

	    /* jump right to degraded mode if there's no taper */
	    if (no_taper) {
		wtaper->tape_error = g_strdup("no taper started (--no-taper)");
		wtaper->result = BOGUS;
	    }
	}

	/* don't start the taper if we're not supposed to */
	taper->fd = -1;
	if (no_taper)
	    return NULL;

	if (socketpair(AF_UNIX, SOCK_STREAM, 0, fd) == -1) {
	    error(_("taper pipe: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	if (fd[0] < 0 || fd[0] >= (int)FD_SETSIZE) {
	    error(_("taper socketpair 0: descriptor %d out of range (0 .. %d)\n"),
	          fd[0], (int)FD_SETSIZE-1);
            /*NOTREACHED*/
	}
	if (fd[1] < 0 || fd[1] >= (int)FD_SETSIZE) {
	    error(_("taper socketpair 1: descriptor %d out of range (0 .. %d)\n"),
	          fd[1], (int)FD_SETSIZE-1);
            /*NOTREACHED*/
	}

	switch(taper->pid = fork()) {
	case -1:
	    error(_("fork taper: %s"), strerror(errno));
	    /*NOTREACHED*/

	case 0:	/* child process */
	    aclose(fd[0]);
	    if (dup2(fd[1], 0) == -1 || dup2(fd[1], 1) == -1)
	        error(_("taper dup2: %s"), strerror(errno));
	    config_options = get_config_options(6);
	    config_options[0] = "taper";
	    config_options[1] = get_config_name();
	    config_options[2] = "--storage";
	    config_options[3] = storage_name(storage);
	    config_options[4] = "--log-filename";
	    config_options[5] = log_filename;
	    safe_fd(-1, 0);
	    env = safe_env();
	    execve(taper_program, config_options, env);
	    free_env(env);
	    error("exec %s: %s", taper_program, strerror(errno));
	    /*NOTREACHED*/

	default: /* parent process */
	    aclose(fd[1]);
	    taper->fd = fd[0];
	}
	g_fprintf(stderr, "driver: taper %s storage %s tape_size %lld\n", taper->name, taper->storage_name, (long long)taper->tape_length);

    return taper;
}

void
startup_dump_process(
    dumper_t *dumper,
    char *dumper_program)
{
    int    fd[2];
    char **config_options;
    char **env;

    if(socketpair(AF_UNIX, SOCK_STREAM, 0, fd) == -1) {
	error(_("%s pipe: %s"), dumper->name, strerror(errno));
	/*NOTREACHED*/
    }

    switch(dumper->pid = fork()) {
    case -1:
	error(_("fork %s: %s"), dumper->name, strerror(errno));
	/*NOTREACHED*/

    case 0:		/* child process */
	aclose(fd[0]);
	if(dup2(fd[1], 0) == -1 || dup2(fd[1], 1) == -1)
	    error(_("%s dup2: %s"), dumper->name, strerror(errno));
	config_options = get_config_options(4);
	config_options[0] = dumper->name ? dumper->name : "dumper",
	config_options[1] = get_config_name();
	config_options[2] = "--log-filename";
	config_options[3] = log_filename;
	safe_fd(-1, 0);
	env = safe_env();
	execve(dumper_program, config_options, env);
	free_env(env);
	error(_("exec %s (%s): %s"), dumper_program,
	      dumper->name, strerror(errno));
        /*NOTREACHED*/

    default:	/* parent process */
	aclose(fd[1]);
	dumper->fd = fd[0];
	dumper->ev_read = NULL;
	dumper->busy = dumper->down = 0;
	g_fprintf(stderr,_("driver: started %s pid %u\n"),
		dumper->name, (unsigned)dumper->pid);
	fflush(stderr);
    }
}

void
startup_dump_processes(
    char *dumper_program,
    int inparallel,
    char *timestamp)
{
    int i;
    dumper_t *dumper;
    char number[NUM_STR_SIZE];

    for(dumper = dmptable, i = 0; i < inparallel; dumper++, i++) {
	g_snprintf(number, sizeof(number), "%d", i);
	dumper->name = g_strconcat("dumper", number, NULL);
	dumper->job = NULL;
	chktable[i].name = g_strconcat("chunker", number, NULL);
	chktable[i].job = NULL;
	chktable[i].fd = -1;

	startup_dump_process(dumper, dumper_program);
	dumper_cmd(dumper, START, NULL, (void *)timestamp);
    }
}

void
startup_chunk_process(
    chunker_t *chunker,
    char *chunker_program)
{
    int    fd[2];
    char **config_options;
    char **env;

    if(socketpair(AF_UNIX, SOCK_STREAM, 0, fd) == -1) {
	error(_("%s pipe: %s"), chunker->name, strerror(errno));
	/*NOTREACHED*/
    }

    switch(chunker->pid = fork()) {
    case -1:
	error(_("fork %s: %s"), chunker->name, strerror(errno));
	/*NOTREACHED*/

    case 0:		/* child process */
	aclose(fd[0]);
	if(dup2(fd[1], 0) == -1 || dup2(fd[1], 1) == -1) {
	    error(_("%s dup2: %s"), chunker->name, strerror(errno));
	    /*NOTREACHED*/
	}
	config_options = get_config_options(4);
	config_options[0] = chunker->name ? chunker->name : "chunker",
	config_options[1] = get_config_name();
	config_options[2] = "--log-filename";
	config_options[3] = log_filename;
	safe_fd(-1, 0);
	env = safe_env();
	execve(chunker_program, config_options, env);
	free_env(env);
	error(_("exec %s (%s): %s"), chunker_program,
	      chunker->name, strerror(errno));
        /*NOTREACHED*/

    default:	/* parent process */
	aclose(fd[1]);
	chunker->down = 0;
	chunker->fd = fd[0];
	chunker->ev_read = NULL;
	g_fprintf(stderr,_("driver: started %s pid %u\n"),
		chunker->name, (unsigned)chunker->pid);
	fflush(stderr);
    }
}

cmd_t
getresult(
    int fd,
    int show,
    int *result_argc,
    char ***result_argv)
{
    cmd_t t;
    char *line;

    if ((line = areads(fd)) == NULL) {
	if(errno) {
	    g_fprintf(stderr, _("reading result from %s: %s"), childstr(fd), strerror(errno));
	}
	*result_argv = NULL;
	*result_argc = 0;				/* EOF */
    } else {
	*result_argv = split_quoted_strings(line);
	*result_argc = g_strv_length(*result_argv);
    }

    if (show) {
	gchar *msg;
	msg = g_strdup_printf("driver: result time %s from %s: %s", walltime_str(curclock()), childstr(fd), line?line:"(eof)");
	g_printf("%s\n", msg);
	fflush(stdout);
	g_debug("%s", msg);
	g_free(msg);
    }
    amfree(line);

    if (*result_argc < 1) return BOGUS;

    for (t = (cmd_t)(BOGUS+1); t < LAST_TOK; t++)
	if (g_str_equal((*result_argv)[0], cmdstr[t])) return t;

    return BOGUS;
}


static char *
taper_splitting_args(
    char *storage_name,
    disk_t *dp)
{
    GString *args = NULL;
    char *q = NULL;
    dumptype_t *dt = dp->config;
    storage_t  *st;
    tapetype_t *tt;

    st = lookup_storage(storage_name);
    tt = lookup_tapetype(storage_get_tapetype(st));
    g_assert(tt != NULL);

    args = g_string_new("");

    /* old dumptype-based parameters, using empty strings when not seen */
    if (dt) { /* 'dt' may be NULL for flushes */
	if (dumptype_seen(dt, DUMPTYPE_TAPE_SPLITSIZE)) {
	    g_string_append_printf(args, "%ju ",
			(uintmax_t)dumptype_get_tape_splitsize(dt)*1024);
	} else {
	    g_string_append(args, "\"\" ");
	}

	q = quote_string(dumptype_seen(dt, DUMPTYPE_SPLIT_DISKBUFFER)?
		dumptype_get_split_diskbuffer(dt) : "");
	g_string_append_printf(args, "%s ", q);
	g_free(q);

	if (dumptype_seen(dt, DUMPTYPE_FALLBACK_SPLITSIZE)) {
	    g_string_append_printf(args, "%ju ",
			(uintmax_t)dumptype_get_fallback_splitsize(dt)*1024);
	} else {
	    g_string_append(args, "\"\" ");
	}

	if (dumptype_seen(dt, DUMPTYPE_ALLOW_SPLIT)) {
	    g_string_append_printf(args, "%d ",
			(int)dumptype_get_allow_split(dt));
	} else {
	    g_string_append(args, "\"\" ");
	}
    } else {
	g_string_append(args, "\"\" \"\" \"\" \"\" ");
    }

    /* new tapetype-based parameters */
    if (tapetype_seen(tt, TAPETYPE_PART_SIZE)) {
	g_string_append_printf(args, "%ju ",
		    (uintmax_t)tapetype_get_part_size(tt)*1024);
    } else {
	g_string_append(args, "\"\" ");
    }

    q = "";
    if (tapetype_seen(tt, TAPETYPE_PART_CACHE_TYPE)) {
	switch (tapetype_get_part_cache_type(tt)) {
	    default:
	    case PART_CACHE_TYPE_NONE:
		q = "none";
		break;

	    case PART_CACHE_TYPE_MEMORY:
		q = "memory";
		break;

	    case PART_CACHE_TYPE_DISK:
		q = "disk";
		break;
	}
    }
    q = quote_string(q);
    g_string_append_printf(args, "%s ", q);
    g_free(q);

    q = quote_string(tapetype_seen(tt, TAPETYPE_PART_CACHE_DIR)?
	    tapetype_get_part_cache_dir(tt) : "");
    g_string_append_printf(args, "%s ", q);
    g_free(q);

    if (tapetype_seen(tt, TAPETYPE_PART_CACHE_MAX_SIZE)) {
	g_string_append_printf(args, "%ju ",
		    (uintmax_t)tapetype_get_part_cache_max_size(tt)*1024);
    } else {
	g_string_append(args, "\"\" ");
    }


    return g_string_free(args, FALSE);
}

int
taper_cmd(
    taper_t  *taper,
    wtaper_t *wtaper,
    cmd_t cmd,
    void *ptr,
    char *destname,
    int level,
    char *datestamp)
{
    char *cmdline = NULL;
    char number[NUM_STR_SIZE];
    char orig_kb[NUM_STR_SIZE];
    char n_crc[NUM_STR_SIZE+11];
    char c_crc[NUM_STR_SIZE+11];
    char s_crc[NUM_STR_SIZE+11];
    char *data_path;
    sched_t *sp;
    disk_t *dp;
    char *qname;
    char *qdest;
    char *q;
    char *splitargs;
    uintmax_t origsize;

    switch(cmd) {
    case START_TAPER:
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", taper->name,
			    " ", wtaper->name,
			    " ", taper->storage_name,
			    " ", datestamp,
			    "\n", NULL);
	break;
    case CLOSE_VOLUME:
	dp = (disk_t *) ptr;
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    "\n", NULL);
	break;
    case CLOSE_SOURCE_VOLUME:
	dp = (disk_t *) ptr;
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    "\n", NULL);
	break;
    case FILE_WRITE:
	sp = (sched_t *)ptr;
	dp = sp->disk;
        qname = quote_string(dp->name);
	qdest = quote_string(destname);
	g_snprintf(number, sizeof(number), "%d", level);
	if (sp->origsize >= 0)
	    origsize = sp->origsize;
	else
	    origsize = 0;
	g_snprintf(orig_kb, sizeof(orig_kb), "%ju", origsize);
	splitargs = taper_splitting_args(taper->storage_name, dp);
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    " ", qdest,
			    " ", dp->host->hostname,
			    " ", qname,
			    " ", number,
			    " ", datestamp,
			    " ", splitargs,
			         orig_kb,
			    "\n", NULL);
	amfree(splitargs);
	amfree(qdest);
	amfree(qname);
	break;

    case PORT_WRITE:
    case SHM_WRITE:
	sp = (sched_t *)ptr;
	dp = sp->disk;
        qname = quote_string(dp->name);
	g_snprintf(number, sizeof(number), "%d", level);
	data_path = data_path_to_string(dp->data_path);

	/*
          If we haven't been given a place to buffer split dumps to disk,
          make the argument something besides and empty string so's taper
          won't get confused
	*/
	splitargs = taper_splitting_args(taper->storage_name, dp);
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    " ", dp->host->hostname,
			    " ", qname,
			    " ", number,
			    " ", datestamp,
			    " ", splitargs,
			         data_path,
			    "\n", NULL);
	amfree(splitargs);
	amfree(qname);
	break;

    case VAULT_WRITE:
	sp = (sched_t *) ptr;
	dp = sp->disk;
        qname = quote_string(dp->name);
	g_snprintf(number, sizeof(number), "%d", level);
	if (sp->origsize >= 0)
	    origsize = sp->origsize;
	else
	    origsize = 0;
	g_snprintf(orig_kb, sizeof(orig_kb), "%ju", origsize);
	splitargs = taper_splitting_args(taper->storage_name, dp);
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    " ", sp->src_storage,
			    " ", sp->src_pool,
			    " ", sp->src_label,
			    " ", dp->host->hostname,
			    " ", qname,
			    " ", number,
			    " ", datestamp,
			    " ", splitargs,
				 orig_kb,
			    "\n", NULL);
	amfree(splitargs);
	amfree(qname);
	break;

    case DONE: /* handle */
	sp = (sched_t *)ptr;
	dp = sp->disk;
	if (sp->origsize >= 0)
	    origsize = sp->origsize;
	else
	    origsize = 0;
	g_snprintf(number, sizeof(number), "%ju", origsize);
	g_snprintf(n_crc, sizeof(n_crc), "%08x:%lld", sp->native_crc.crc,
		   (long long)sp->native_crc.size);
	g_snprintf(c_crc, sizeof(c_crc), "%08x:%lld", sp->client_crc.crc,
		   (long long)sp->client_crc.size);
	if (dp->compress == COMP_SERVER_FAST ||
	    dp->compress == COMP_SERVER_BEST ||
	    dp->compress == COMP_SERVER_CUST ||
	    dp->encrypt  == ENCRYPT_SERV_CUST) {
	    /* The server-crc do not match the client-crc */
	    g_snprintf(s_crc, sizeof(s_crc), "00000000:0");
	} else {
	    /* The server-crc should match the client-crc */
	    g_snprintf(s_crc, sizeof(s_crc), "%08x:%lld",
		       sp->client_crc.crc,
		       (long long)sp->client_crc.size);
	}
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    " ", number,
			    " ", n_crc,
			    " ", c_crc,
			    " ", s_crc,
			    "\n", NULL);
	break;
    case FAILED: /* handle */
	sp = (sched_t *)ptr;
	dp = sp->disk;
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    "\n", NULL);
	break;
    case NO_NEW_TAPE:
	sp = (sched_t *)ptr;
	dp = sp->disk;
	q = quote_string(destname);	/* reason why no new tape */
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    " ", q,
			    "\n", NULL);
	amfree(q);
	break;
    case NEW_TAPE:
	sp = (sched_t *)ptr;
	dp = sp->disk;
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    "\n", NULL);
	break;
    case START_SCAN:
	sp = (sched_t *)ptr;
	dp = sp->disk;
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    "\n", NULL);
	break;
    case TAKE_SCRIBE_FROM:
	sp = (sched_t *)ptr;
	dp = sp->disk;
	cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", wtaper->name,
			    " ", job2serial(wtaper->job),
			    " ", destname,  /* name of worker */
			    "\n", NULL);
	break;
    case QUIT:
	cmdline = g_strconcat(cmdstr[cmd], "\n", NULL);
	break;
    default:
	error(_("Don't know how to send %s command to taper"), cmdstr[cmd]);
	/*NOTREACHED*/
    }

    /*
     * Note: cmdline already has a '\n'.
     */
    g_printf(_("driver: send-cmd time %s to %s: %s"),
	   walltime_str(curclock()), taper->name, cmdline);
    fflush(stdout);
    if ((full_write(taper->fd, cmdline, strlen(cmdline))) < strlen(cmdline)) {
	g_printf(_("writing taper command '%s' failed: %s\n"),
		cmdline, strerror(errno));
	fflush(stdout);
	amfree(cmdline);
	return 0;
    }
    cmdline[strlen(cmdline)-1] = '\0';
    g_debug("driver: send-cmd time %s to %s: %s", walltime_str(curclock()), taper->name, cmdline);
    if (cmd == QUIT) {
	aclose(taper->fd);
	amfree(taper->name);
	amfree(taper->storage_name);
    }
    amfree(cmdline);
    return 1;
}

int
dumper_cmd(
    dumper_t *dumper,
    cmd_t cmd,
    sched_t *sp,
    char   *mesg)
{
    char *cmdline = NULL;
    char *qmesg;
    disk_t *dp;

    switch(cmd) {
    case START:
        cmdline = g_strdup_printf("%s %s\n", cmdstr[cmd], mesg);
	break;
    case PORT_DUMP:
        if (!sp)
            error("PORT-DUMP without sched pointer\n");
	// fall through
    case SHM_DUMP: {
        application_t *application = NULL;
        GPtrArray *array;
        GString *strbuf;
        gchar **args;
        am_feature_t *features;
        char *device, *plugin;
        char *tmp;

        if (!sp)
            error("SHM-DUMP without sched pointer\n");

	dp = sp->disk;
        array = g_ptr_array_new();
        features = dp->host->features;

        device = (dp->device) ? dp->device : "NODEVICE";

        if (dp->application != NULL) {
            application = lookup_application(dp->application);
            g_assert(application != NULL);
        }

        g_ptr_array_add(array, g_strdup(cmdstr[cmd]));
        g_ptr_array_add(array, g_strdup(job2serial(dumper->job)));
        g_ptr_array_add(array, g_strdup_printf("%d", dumper->output_port));
        g_ptr_array_add(array, g_strdup(interface_get_src_ip(dp->host->netif->config)));
        g_ptr_array_add(array, g_strdup_printf("%d", dp->host->maxdumps));
        g_ptr_array_add(array, g_strdup(dp->host->hostname));
        g_ptr_array_add(array, am_feature_to_string(features));
        g_ptr_array_add(array, quote_string(dp->name));
        g_ptr_array_add(array, quote_string(device));
        g_ptr_array_add(array, g_strdup_printf("%d", sp->level));
        g_ptr_array_add(array, g_strdup(sp->dumpdate));


        /*
         * Build the last argument
         */
        strbuf = g_string_new("|");

        if (am_has_feature(features, fe_req_xml)) {
            char *qtmp;

            tmp = xml_optionstr(dp, 1);
            qtmp = quote_string(tmp);
            g_free(tmp);

            g_string_append(strbuf, qtmp);
            g_free(qtmp);

	    tmp = xml_dumptype_properties(dp);
	    qtmp = quote_string(tmp);
	    g_free(tmp);
	    g_string_append(strbuf, qtmp);
	    g_free(qtmp);

            if (application) {
                tmp = xml_application(dp, application, features);
                qtmp = quote_string(tmp);
                g_free(tmp);

                g_string_append(strbuf, qtmp);
                g_free(qtmp);
            }
        } else {
            tmp = optionstr(dp);
            g_string_append(strbuf, tmp);
            g_free(tmp);
        }

        g_string_append_c(strbuf, '\n');

        g_assert(dp->program != NULL);

        if (g_str_equal(dp->program, "APPLICATION")) {
            g_assert(application != NULL);
            plugin = application_get_plugin(application);
        } else {
            plugin = dp->program;
        }

        g_ptr_array_add(array, quote_string(plugin));
        g_ptr_array_add(array, quote_string(dp->amandad_path));
        g_ptr_array_add(array, quote_string(dp->client_username));
        g_ptr_array_add(array, quote_string(dp->ssl_fingerprint_file));
        g_ptr_array_add(array, quote_string(dp->ssl_cert_file));
        g_ptr_array_add(array, quote_string(dp->ssl_key_file));
        g_ptr_array_add(array, quote_string(dp->ssl_ca_cert_file));
        g_ptr_array_add(array, quote_string(dp->ssl_cipher_list));
        g_ptr_array_add(array, g_strdup_printf("%d", dp->ssl_check_certificate_host));
        g_ptr_array_add(array, quote_string(dp->client_port));
        g_ptr_array_add(array, quote_string(dp->ssh_keys));
        g_ptr_array_add(array, g_strdup(dp->auth));
        g_ptr_array_add(array, g_strdup(data_path_to_string(dp->data_path)));
	if (cmd == PORT_DUMP) {
            g_ptr_array_add(array, g_strdup(dp->dataport_list));
	} else {
            g_ptr_array_add(array, g_strdup(dp->shm_name));
	}
        g_ptr_array_add(array, g_strdup_printf("%d", dp->max_warnings));
        g_ptr_array_add(array, g_string_free(strbuf, FALSE));
        g_ptr_array_add(array, NULL);

        args = (gchar **)g_ptr_array_free(array, FALSE);
        cmdline = g_strjoinv(" ", args);
        g_strfreev(args);

	break;
    }
    case ABORT:
	qmesg = quote_string(mesg);
        cmdline = g_strdup_printf("%s %s %s\n", cmdstr[cmd], job2serial(dumper->job), qmesg);
	amfree(qmesg);
	break;
    case QUIT:
	qmesg = quote_string(mesg);
        cmdline = g_strdup_printf("%s %s\n", cmdstr[cmd], qmesg);
	amfree(qmesg);
	break;
    default:
	error("Don't know how to send %s command to dumper", cmdstr[cmd]);
	/*NOTREACHED*/
    }

    /*
     * Note: cmdline already has a '\n'.
     */
    if (dumper->down) {
	g_printf(_("driver: send-cmd time %s ignored to down dumper %s: %s"),
	       walltime_str(curclock()), dumper->name, cmdline);
    } else {
	g_printf(_("driver: send-cmd time %s to %s: %s"),
	       walltime_str(curclock()), dumper->name, cmdline);
	fflush(stdout);
	if (full_write(dumper->fd, cmdline, strlen(cmdline)) < strlen(cmdline)) {
	    g_printf(_("writing %s command: %s\n"), dumper->name, strerror(errno));
	    fflush(stdout);
	    g_free(cmdline);
	    return 0;
	}
	cmdline[strlen(cmdline)-1] = '\0';
	g_debug("driver: send-cmd time %s to %s: %s", walltime_str(curclock()), dumper->name, cmdline);
	if (cmd == QUIT) aclose(dumper->fd);
    }
    g_free(cmdline);
    return 1;
}

int
chunker_cmd(
    chunker_t *chunker,
    cmd_t cmd,
    sched_t *sp,
    char   *mesg)
{
    char *cmdline = NULL;
    char number[NUM_STR_SIZE];
    char chunksize[NUM_STR_SIZE];
    char use[NUM_STR_SIZE];
    char c_crc[NUM_STR_SIZE+11];
    char *o;
    int activehd=0;
    assignedhd_t **h=NULL;
    char *features;
    char *qname;
    char *qdest;
    disk_t *dp;

    switch(cmd) {
    case START:
	cmdline = g_strjoin(NULL, cmdstr[cmd], " ", mesg, "\n", NULL);
	break;
    case PORT_WRITE:
    case SHM_WRITE:
	dp = sp->disk;
	if(sp->holdp) {
	    h = sp->holdp;
	    activehd = sp->activehd;
	}

	if (dp && h) {
	    qname = quote_string(dp->name);
	    qdest = quote_string(sp->destname);
	    h[activehd]->disk->allocated_dumpers++;
	    g_snprintf(number, sizeof(number), "%d", sp->level);
	    g_snprintf(chunksize, sizeof(chunksize), "%lld",
		    (long long)holdingdisk_get_chunksize(h[0]->disk->hdisk));
	    g_snprintf(use, sizeof(use), "%lld",
		    (long long)h[0]->reserved);
	    features = am_feature_to_string(dp->host->features);
	    o = optionstr(dp);
	    cmdline = g_strjoin(NULL, cmdstr[cmd],
			    " ", job2serial(chunker->job),
			    " ", qdest,
			    " ", dp->host->hostname,
			    " ", features,
			    " ", qname,
			    " ", number,
			    " ", mesg,  /* datestamp */
			    " ", chunksize,
			    " ", dp->program,
			    " ", use,
			    " |", o,
			    "\n", NULL);
	    amfree(features);
	    amfree(o);
	    amfree(qdest);
	    amfree(qname);
	} else {
		error(_("%s command without disk and holding disk.\n"),
		      cmdstr[cmd]);
		/*NOTREACHED*/
	}
	break;
    case CONTINUE:
	dp = sp->disk;
	if(sp->holdp) {
	    h = sp->holdp;
	    activehd = sp->activehd;
	}

	if(dp && h) {
	    qname = quote_string(dp->name);
	    qdest = quote_string(h[activehd]->destname);
	    h[activehd]->disk->allocated_dumpers++;
	    g_snprintf(chunksize, sizeof(chunksize), "%lld",
		     (long long)holdingdisk_get_chunksize(h[activehd]->disk->hdisk));
	    g_snprintf(use, sizeof(use), "%lld",
		     (long long)(h[activehd]->reserved - h[activehd]->used));
	    cmdline = g_strjoin(NULL, cmdstr[cmd],
				" ", job2serial(chunker->job),
				" ", qdest,
				" ", chunksize,
				" ", use,
				"\n", NULL );
	    amfree(qdest);
	    amfree(qname);
	} else {
	    cmdline = g_strconcat(cmdstr[cmd], "\n", NULL);
	}
	break;
    case QUIT:
	cmdline = g_strjoin(NULL, cmdstr[cmd], "\n", NULL);
	break;
    case ABORT:
	{
	    char *q = quote_string(mesg);
	    cmdline = g_strjoin(NULL, cmdstr[cmd], " ", q, "\n", NULL);
            cmdline = g_strdup_printf("%s %s %s\n", cmdstr[cmd], job2serial(chunker->job), q);
	    amfree(q);
	}
	break;
    case DONE:
	dp = sp->disk;
	if (dp) {
	    if (sp->client_crc.crc == 0 ||
		dp->compress == COMP_SERVER_FAST ||
		dp->compress == COMP_SERVER_BEST ||
		dp->compress == COMP_SERVER_CUST ||
		dp->encrypt  == ENCRYPT_SERV_CUST) {
		g_snprintf(c_crc, sizeof(c_crc), "00000000:0");
	    } else {
		g_snprintf(c_crc, sizeof(c_crc), "%08x:%lld",
			   sp->client_crc.crc,
			   (long long)sp->client_crc.size);
	    }
	    cmdline = g_strjoin(NULL, cmdstr[cmd],
				" ", job2serial(chunker->job),
				" ", c_crc,
				"\n",  NULL);
	} else {
	    cmdline = g_strjoin(NULL, cmdstr[cmd], "\n", NULL);
	}
	break;
    case FAILED:
	dp = sp->disk;
	if( dp ) {
	    cmdline = g_strjoin(NULL, cmdstr[cmd],
				" ", job2serial(chunker->job),
				"\n",  NULL);
	} else {
	    cmdline = g_strjoin(NULL, cmdstr[cmd], "\n", NULL);
	}
	break;
    default:
	error(_("Don't know how to send %s command to chunker"), cmdstr[cmd]);
	/*NOTREACHED*/
    }

    /*
     * Note: cmdline already has a '\n'.
     */
    g_printf(_("driver: send-cmd time %s to %s: %s"),
	   walltime_str(curclock()), chunker->name, cmdline);
    fflush(stdout);
    if (full_write(chunker->fd, cmdline, strlen(cmdline)) < strlen(cmdline)) {
	g_printf(_("writing %s command: %s\n"), chunker->name, strerror(errno));
	fflush(stdout);
	amfree(cmdline);
	return 0;
    }
    cmdline[strlen(cmdline)-1] = '\0';
    g_debug("driver: send-cmd time %s to %s: %s", walltime_str(curclock()), chunker->name, cmdline);
    if (cmd == QUIT) aclose(chunker->fd);
    amfree(cmdline);
    return 1;
}

job_t *
alloc_job(void)
{
    int i;

    for (i=0; i<max_jobs; i++) {
	if (jobs[i].in_use == 0) {
	    jobs[i].in_use = 1;
	    return &jobs[i];
	}
    }
    error("All job in use");
}

void
free_job(
    job_t *job)
{
    job->in_use  = 0;
    job->sched   = NULL;
    job->dumper  = NULL;
    job->chunker = NULL;
    job->wtaper  = NULL;
}

void
free_sched(
    sched_t *sp)
{
    g_free(sp->dumpdate);
    g_free(sp->degr_dumpdate);
    g_free(sp->destname);
    g_free(sp->datestamp);
    g_free(sp->degr_mesg);
    g_free(sp->src_storage);
    g_free(sp->src_pool);
    g_free(sp->src_label);
    g_free(sp);
}

job_t *
serial2job(
    char *str)
{
    int rc, s;
    long gen;

    rc = sscanf(str, "%d-%ld", &s, &gen);
    if(rc != 2) {
	error(_("error [serial2job \"%s\" parse error]"), str);
	/*NOTREACHED*/
    } else if (s < 0 || s >= max_serial) {
	error(_("error [serial out of range 0..%d: %d]"), max_serial, s);
	/*NOTREACHED*/
    }
    if(gen != stable[s].gen)
	g_printf("driver: serial2job error time %s serial gen mismatch %s %d %ld %ld\n",
	       walltime_str(curclock()), str, s, gen, stable[s].gen);
    return stable[s].job;
}

void
free_serial(
    char *str)
{
    int rc, s;
    long gen;

    rc = sscanf(str, _("%d-%ld"), &s, &gen);
    if(!(rc == 2 && s >= 0 && s < max_serial)) {
	/* nuke self to get core dump for Brett */
	g_fprintf(stderr, _("driver: free_serial: str \"%s\" rc %d s %d\n"),
		str, rc, s);
	fflush(stderr);
	abort();
    }

    if(gen != stable[s].gen)
	g_printf(_("driver: free_serial error time %s serial gen mismatch %s\n"),
	       walltime_str(curclock()),str);
    stable[s].gen = 0;
    stable[s].job = NULL;
}


void
free_serial_job(
    job_t *job)
{
    int s;

    for(s = 0; s < max_serial; s++) {
	if(stable[s].job == job) {
	    //g_printf("free serial %02d-%05ld for disk\n", s, stable[s].gen);
	    stable[s].gen = 0;
	    stable[s].job = NULL;
	    return;
	}
    }

    // Should try to print DB name found by dumper/chunker or wtaper.
    g_printf(_("driver: error time %s serial not found for job %p\n"),
	   walltime_str(curclock()), job);
}


void
check_unfree_serial(void)
{
    int s;

    /* find used serial number */
    for(s = 0; s < max_serial; s++) {
	if(stable[s].gen != 0 || stable[s].job != NULL) {
	    g_printf(_("driver: error time %s bug: serial in use: %02d-%05ld\n"),
		   walltime_str(curclock()), s, stable[s].gen);
	}
    }
}

char *job2serial(
    job_t *job)
{
    int s;
    static char str[NUM_STR_SIZE];

    for(s = 0; s < max_serial; s++) {
	if(stable[s].job == job) {
	    g_snprintf(str, sizeof(str), "%02d-%05ld", s, stable[s].gen);
	    return str;
	}
    }

    /* find unused serial number */
    for(s = 0; s < max_serial; s++)
	if(stable[s].gen == 0 && stable[s].job == NULL)
	    break;
    if(s >= max_serial) {
	g_printf(_("driver: error time %s bug: out of serial numbers\n"),
	       walltime_str(curclock()));
	s = 0;
    }

    stable[s].gen = generation++;
    stable[s].job = job;

    g_snprintf(str, sizeof(str), "%02d-%05ld", s, stable[s].gen);
    return str;
}

void
update_info_dumper(
     sched_t *sp,
     off_t origsize,
     off_t dumpsize,
     time_t dumptime)
{
    int level, i;
    info_t info;
    stats_t *infp;
    perf_t *perfp;
    char *conf_infofile;
    disk_t *dp = sp->disk;

    level = sp->level;

    if (origsize == 0 || dumpsize == 0) {
	g_debug("not updating because origsize or dumpsize is 0");
	return;
    }

    conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
    if (open_infofile(conf_infofile)) {
	error(_("could not open info db \"%s\""), conf_infofile);
	/*NOTREACHED*/
    }
    amfree(conf_infofile);

    get_info(dp->host->hostname, dp->name, &info);

    /* Clean up information about this and higher-level dumps.  This
       assumes that update_info_dumper() is always run before
       update_info_taper(). */
    for (i = level; i < DUMP_LEVELS; ++i) {
      infp = &info.inf[i];
      infp->size = (off_t)-1;
      infp->csize = (off_t)-1;
      infp->secs = (time_t)-1;
      infp->date = (time_t)-1;
      infp->label[0] = '\0';
      infp->filenum = 0;
    }

    /* now store information about this dump */
    infp = &info.inf[level];
    infp->size = origsize;
    infp->csize = dumpsize;
    infp->secs = dumptime;
    if (sp->timestamp == 0) {
	infp->date = 0;
    } else {
	infp->date = get_time_from_timestamp(sp->datestamp);
    }

    if(level == 0) perfp = &info.full;
    else perfp = &info.incr;

    /* Update the stats, but only if the new values are meaningful */
    if(dp->compress != COMP_NONE && origsize > (off_t)0) {
	newperf(perfp->comp, (double)dumpsize/(double)origsize);
    }
    if(dumptime > (time_t)0) {
	if((off_t)dumptime >= dumpsize)
	    newperf(perfp->rate, 1);
	else
	    newperf(perfp->rate, (double)dumpsize/(double)dumptime);
    }

    if(origsize >= (off_t)0 && getconf_int(CNF_RESERVE)<100) {
	info.command = NO_COMMAND;
    }

    if (origsize >= (off_t)0 && level == info.last_level) {
	info.consecutive_runs++;
    } else if (origsize >= (off_t)0) {
	info.last_level = level;
	info.consecutive_runs = 1;
    }

    if(origsize >= (off_t)0 && dumpsize >= (off_t)0) {
	for(i=NB_HISTORY-1;i>0;i--) {
	    info.history[i] = info.history[i-1];
	}

	info.history[0].level = level;
	info.history[0].size  = origsize;
	info.history[0].csize = dumpsize;
	if (sp->timestamp == 0) {
	    info.history[0].date = 0;
	} else {
	    info.history[0].date = get_time_from_timestamp(sp->datestamp);
	}
	info.history[0].secs  = dumptime;
    }

    if (put_info(dp->host->hostname, dp->name, &info)) {
	int save_errno = errno;
	g_fprintf(stderr, _("infofile update failed (%s,'%s'): %s\n"),
		  dp->host->hostname, dp->name, strerror(save_errno));
	log_add(L_ERROR, _("infofile update failed (%s,'%s'): %s\n"),
		dp->host->hostname, dp->name, strerror(save_errno));
	error(_("infofile update failed (%s,'%s'): %s\n"),
	      dp->host->hostname, dp->name, strerror(save_errno));
	/*NOTREACHED*/
    }

    close_infofile();
}

void
update_info_taper(
    sched_t *sp,
    char *label,
    off_t filenum,
    int level)
{
    info_t info;
    stats_t *infp;
    int rc;
    disk_t *dp = sp->disk;

    if (!label) {
	log_add(L_ERROR, "update_info_taper without label");
	return;
    }

    rc = open_infofile(getconf_str(CNF_INFOFILE));
    if(rc) {
	error(_("could not open infofile %s: %s (%d)"), getconf_str(CNF_INFOFILE),
	      strerror(errno), rc);
	/*NOTREACHED*/
    }

    get_info(dp->host->hostname, dp->name, &info);

    infp = &info.inf[level];
    /* XXX - should we record these two if no-record? */
    strncpy(infp->label, label, sizeof(infp->label)-1);
    infp->label[sizeof(infp->label)-1] = '\0';
    infp->filenum = filenum;

    info.command = NO_COMMAND;

    if (put_info(dp->host->hostname, dp->name, &info)) {
	int save_errno = errno;
	g_fprintf(stderr, _("infofile update failed (%s,'%s'): %s\n"),
		  dp->host->hostname, dp->name, strerror(save_errno));
	log_add(L_ERROR, _("infofile update failed (%s,'%s'): %s\n"),
		dp->host->hostname, dp->name, strerror(save_errno));
	error(_("infofile update failed (%s,'%s'): %s\n"),
	      dp->host->hostname, dp->name, strerror(save_errno));
	/*NOTREACHED*/
    }
    close_infofile();
}

/* Free an array of pointers to assignedhd_t after freeing the
 * assignedhd_t themselves. The array must be NULL-terminated.
 */
void free_assignedhd(
    assignedhd_t **ahd)
{
    int i;

    if( !ahd ) { return; }

    for( i = 0; ahd[i]; i++ ) {
	amfree(ahd[i]->destname);
	amfree(ahd[i]);
    }
    amfree(ahd);
}

