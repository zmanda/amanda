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
 * $Id: sendsize.c 10421 2008-03-06 18:48:30Z martineau $
 *
 * send estimated backup sizes using dump
 */

#include "amanda.h"
#include "match.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "amandates.h"
#include "clock.h"
#include "util.h"
#include "getfsent.h"
#include "client_util.h"
#include "conffile.h"

#ifdef SAMBA_CLIENT
#include "findpass.h"
#endif

#define sendsize_debug(i, ...) do {	\
	if ((i) <= debug_sebdsize) {	\
	    dbprintf(__VA_ARGS__);	\
	}				\
} while (0)

#ifdef HAVE_SETPGID
#  define SETPGRP	setpgid(getpid(), getpid())
#  define SETPGRP_FAILED() do {						\
    dbprintf(_("setpgid(%ld,%ld) failed: %s\n"),				\
	      (long)getpid(), (long)getpid(), strerror(errno));	\
} while(0)

#else /* () line 0 */
#if defined(SETPGRP_VOID)
#  define SETPGRP	setpgrp()
#  define SETPGRP_FAILED() do {						\
    dbprintf(_("setpgrp() failed: %s\n"), strerror(errno));		\
} while(0)

#else
#  define SETPGRP	setpgrp(0, getpid())
#  define SETPGRP_FAILED() do {						\
    dbprintf(_("setpgrp(0,%ld) failed: %s\n"),				\
	      (long)getpid(), strerror(errno));			\
} while(0)

#endif
#endif

typedef struct level_estimates_s {
    time_t dumpsince;
    int estsize;
    int needestimate;
    int server;		/* server can do estimate */
} level_estimate_t;

typedef struct disk_estimates_s {
    struct disk_estimates_s *next;
    char *qamname;
    char *qamdevice;
    char *dirname;
    char *qdirname;
    pid_t child;
    int done;
    dle_t *dle;
    level_estimate_t est[DUMP_LEVELS];
} disk_estimates_t;

disk_estimates_t *est_list;

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static g_option_t *g_options = NULL;
static gboolean amandates_started = FALSE;

/* local functions */
int main(int argc, char **argv);
void dle_add_diskest(dle_t *dle);
void calc_estimates(disk_estimates_t *est);
void free_estimates(disk_estimates_t *est);
void dump_calc_estimates(disk_estimates_t *);
void star_calc_estimates(disk_estimates_t *);
void smbtar_calc_estimates(disk_estimates_t *);
void gnutar_calc_estimates(disk_estimates_t *);
void application_api_calc_estimate(disk_estimates_t *);
void generic_calc_estimates(disk_estimates_t *);

int
main(
    int		argc,
    char **	argv)
{
    int level;
    char *dumpdate;
    disk_estimates_t *est;
    disk_estimates_t *est1;
    disk_estimates_t *est_prev;
    disk_estimates_t *est_next;
    char *line = NULL;
    char *s, *fp;
    int ch;
    char *err_extra = NULL;
    int done;
    int need_wait;
    int dumpsrunning;
    char *qdisk = NULL;
    char *qlist = NULL;
    char *qamdevice = NULL;
    dle_t *dle;
    GSList *errlist;
    am_level_t *alevel;

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("sendsize-%s\n", VERSION);
	return (0);
    }

    /* initialize */

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);
    openbsd_fd_inform();
    safe_cd();

    set_pname("sendsize");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_syslog);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();
    dbprintf(_("version %s\n"), VERSION);

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    config_init(CONFIG_INIT_CLIENT, NULL);
    /* (check for config errors comes later) */

    check_running_as(RUNNING_AS_CLIENT_LOGIN);

    /* handle all service requests */

    for(; (line = agets(stdin)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	if(strncmp_const(line, "OPTIONS ") == 0) {
	    g_options = parse_g_options(line+8, 1);
	    if(!g_options->hostname) {
		g_options->hostname = alloc(MAX_HOSTNAME_LENGTH+1);
		gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
	    }

	    g_printf("OPTIONS ");
	    if(am_has_feature(g_options->features, fe_rep_options_features)) {
		g_printf("features=%s;", our_feature_string);
	    }
	    if(am_has_feature(g_options->features, fe_rep_options_maxdumps)) {
		g_printf("maxdumps=%d;", g_options->maxdumps);
	    }
	    if(am_has_feature(g_options->features, fe_rep_options_hostname)) {
		g_printf("hostname=%s;", g_options->hostname);
	    }
	    g_printf("\n");
	    fflush(stdout);

	    if (g_options->config) {
		/* overlay this configuration on the existing (nameless) configuration */
		config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
			    g_options->config);

		dbrename(get_config_name(), DBG_SUBDIR_CLIENT);
	    }

	    /* check for any config errors now */
	    if (config_errors(&errlist) >= CFGERR_ERRORS) {
		char *errstr = config_errors_to_error_string(errlist);
		g_printf("%s\n", errstr);
		dbclose();
		return 1;
	    }

	    if (am_has_feature(g_options->features, fe_req_xml)) {
		break;
	    }
	    continue;
	}

	dle = alloc_dle();
	s = line;
	ch = *s++;

	skip_whitespace(s, ch);			/* find the program name */
	if(ch == '\0') {
	    err_extra = stralloc(_("no program name"));
	    goto err;				/* no program name */
	}
	dle->program = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	dle->program_is_application_api=0;
	if(strncmp_const(dle->program, "CALCSIZE") == 0) {
	    skip_whitespace(s, ch);		/* find the program name */
	    if(ch == '\0') {
		err_extra = stralloc(_("no program name"));
		goto err;
	    }
	    dle->estimatelist = g_slist_append(dle->estimatelist,
					       GINT_TO_POINTER(ES_CALCSIZE));
	    dle->program = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	    if (strcmp(dle->program,"APPLICATION") == 0) {
		dle->program_is_application_api=1;
		skip_whitespace(s, ch);		/* find dumper name */
		if (ch == '\0') {
		    goto err;			/* no program */
		}
		dle->program = s - 1;
		skip_non_whitespace(s, ch);
		s[-1] = '\0';
	    }
	}
	else {
	    dle->estimatelist = g_slist_append(dle->estimatelist,
					       GINT_TO_POINTER(ES_CLIENT));
	    if (strcmp(dle->program,"APPLICATION") == 0) {
		dle->program_is_application_api=1;
		skip_whitespace(s, ch);		/* find dumper name */
		if (ch == '\0') {
		    goto err;			/* no program */
		}
		dle->program = s - 1;
		skip_non_whitespace(s, ch);
		s[-1] = '\0';
	    }
	}
	dle->program = stralloc(dle->program);

	skip_whitespace(s, ch);			/* find the disk name */
	if(ch == '\0') {
	    err_extra = stralloc(_("no disk name"));
	    goto err;				/* no disk name */
	}

	if (qdisk != NULL)
	    amfree(qdisk);

	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	qdisk = stralloc(fp);
	dle->disk = unquote_string(qdisk);

	skip_whitespace(s, ch);			/* find the device or level */
	if (ch == '\0') {
	    err_extra = stralloc(_("bad level"));
	    goto err;
	}
	if(!isdigit((int)s[-1])) {
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    qamdevice = stralloc(fp);
	    dle->device = unquote_string(qamdevice);
	    skip_whitespace(s, ch);		/* find level number */
	}
	else {
	    dle->device = stralloc(dle->disk);
	    qamdevice = stralloc(qdisk);
	}

						/* find the level number */
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    err_extra = stralloc(_("bad level"));
	    goto err;				/* bad level */
	}
	if (level < 0 || level >= DUMP_LEVELS) {
	    err_extra = stralloc(_("bad level"));
	    goto err;
	}
	skip_integer(s, ch);
	alevel = g_new0(am_level_t, 1);
	alevel->level = level;
	dle->levellist = g_slist_append(dle->levellist, alevel);

	skip_whitespace(s, ch);			/* find the dump date */
	if(ch == '\0') {
	    err_extra = stralloc(_("no dumpdate"));
	    goto err;				/* no dumpdate */
	}
	dumpdate = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	(void)dumpdate;				/* XXX: Set but not used */

	dle->spindle = 0;			/* default spindle */

	skip_whitespace(s, ch);			/* find the spindle */
	if(ch != '\0') {
	    if(sscanf(s - 1, "%d", &dle->spindle) != 1) {
		err_extra = stralloc(_("bad spindle"));
		goto err;			/* bad spindle */
	    }
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the parameters */
	    if(ch != '\0') {
		if(strncmp_const(s-1, "OPTIONS |;") == 0) {
		    parse_options(s + 8,
				  dle,
				  g_options->features,
				  0);
		}
		else {
		    while (ch != '\0') {
			if(strncmp_const(s-1, "exclude-file=") == 0) {
			    qlist = unquote_string(s+12);
			    dle->exclude_file =
				append_sl(dle->exclude_file, qlist);
			    amfree(qlist);
			} else if(strncmp_const(s-1, "exclude-list=") == 0) {
			    qlist = unquote_string(s+12);
			    dle->exclude_list =
				append_sl(dle->exclude_list, qlist);
			    amfree(qlist);
			} else if(strncmp_const(s-1, "include-file=") == 0) {
			    qlist = unquote_string(s+12);
			    dle->include_file =
				append_sl(dle->include_file, qlist);
			    amfree(qlist);
			} else if(strncmp_const(s-1, "include-list=") == 0) {
			    qlist = unquote_string(s+12);
			    dle->include_list =
				append_sl(dle->include_list, qlist);
			    amfree(qlist);
			} else {
			    err_extra = vstrallocf(_("Invalid parameter (%s)"), s-1);
			    goto err;		/* should have gotten to end */
			}
			skip_quoted_string(s, ch);
			skip_whitespace(s, ch);	/* find the inclusion list */
			amfree(qlist);
		    }
		}
	    }
	}

	/*@ignore@*/
	dle_add_diskest(dle);
	/*@end@*/
    }
    if (g_options == NULL) {
	g_printf(_("ERROR [Missing OPTIONS line in sendsize input]\n"));
	error(_("Missing OPTIONS line in sendsize input\n"));
	/*NOTREACHED*/
    }
    amfree(line);

    if (am_has_feature(g_options->features, fe_req_xml)) {
	char    *errmsg = NULL;
	dle_t   *dles, *dle;

	dles = amxml_parse_node_FILE(stdin, &errmsg);
	if (errmsg) {
	    err_extra = errmsg;
	    goto err;
	}
	for (dle = dles; dle != NULL; dle = dle->next) {
	    dle_add_diskest(dle);
	}
    }

    if (amandates_started) {
	finish_amandates();
	free_amandates();
	amandates_started = FALSE;
    }

    est_prev = NULL;
    for(est = est_list; est != NULL; est = est_next) {
	int good = merge_dles_properties(est->dle, 0);
	est_next = est->next;
	if (!good) {
	    if (est == est_list) {
		est_list = est_next;
	    } else {
		est_prev->next = est_next;
	    }
	} else {
	    est_prev = est;
	}
    }
    for(est = est_list; est != NULL; est = est->next) {
	run_client_scripts(EXECUTE_ON_PRE_HOST_ESTIMATE, g_options, est->dle,
			   stdout);
    }

    dumpsrunning = 0;
    need_wait = 0;
    done = 0;
    while(! done) {
	done = 1;
	/*
	 * See if we need to wait for a child before we can do anything
	 * else in this pass.
	 */
	if(need_wait) {
	    pid_t child_pid;
	    amwait_t child_status;

	    need_wait = 0;
	    dbprintf(_("waiting for any estimate child: %d running\n"),
		      dumpsrunning);
	    child_pid = wait(&child_status);
	    if(child_pid == -1) {
		error(_("wait failed: %s"), strerror(errno));
		/*NOTREACHED*/
	    }

	    if (!WIFEXITED(child_status) || WEXITSTATUS(child_status) != 0) {
		char *child_name = vstrallocf(_("child %ld"), (long)child_pid);
		char *child_status_str = str_exit_status(child_name, child_status);
		dbprintf("%s\n", child_status_str);
		amfree(child_status_str);
		amfree(child_name);
	    }

	    /*
	     * Find the child and mark it done.
	     */
	    for(est = est_list; est != NULL; est = est->next) {
		if(est->child == child_pid) {
		    break;
		}
	    }
	    if(est == NULL) {
		dbprintf(_("unexpected child %ld\n"), (long)child_pid);
	    } else {
		est->done = 1;
		est->child = 0;
		dumpsrunning--;
		run_client_scripts(EXECUTE_ON_POST_DLE_ESTIMATE, g_options,
				   est->dle, stdout);
	    }
	}
	/*
	 * If we are already running the maximum number of children
	 * go back and wait until one of them finishes.
	 */
	if(dumpsrunning >= g_options->maxdumps) {
	    done = 0;
	    need_wait = 1;
	    continue;				/* have to wait first */
	}
	/*
	 * Find a new child to start.
	 */
	for(est = est_list; est != NULL; est = est->next) {
	    if(est->done == 0) {
		done = 0;			/* more to do */
	    }
	    if(est->child != 0 || est->done) {
		continue;			/* child is running or done */
	    }
	    /*
	     * Make sure there is no spindle conflict.
	     */
	    if(est->dle->spindle != -1) {
		for(est1 = est_list; est1 != NULL; est1 = est1->next) {
		    if(est1->child == 0 || est == est1 || est1->done) {
			/*
			 * Ignore anything not yet started, ourself,
			 * and anything completed.
			 */
			continue;
		    }
		    if(est1->dle->spindle == est->dle->spindle) {
			break;			/* oops -- they match */
		    }
		}
		if(est1 != NULL) {
		    continue;			/* spindle conflict */
		}
	    }
	    break;				/* start this estimate */
	}
	if(est == NULL) {
	    if(dumpsrunning > 0) {
		need_wait = 1;			/* nothing to do but wait */
	    }
	} else {
	    done = 0;
	    run_client_scripts(EXECUTE_ON_PRE_DLE_ESTIMATE, g_options,
			       est->dle, stdout);

	    if((est->child = fork()) == 0) {
		calc_estimates(est);		/* child does the estimate */
		exit(0);
	    } else if(est->child == -1) {
		error(_("calc_estimates fork failed: %s"), strerror(errno));
		/*NOTREACHED*/
	    }
	    dumpsrunning++;			/* parent */
	}
    }

    for(est = est_list; est != NULL; est = est->next) {
	run_client_scripts(EXECUTE_ON_POST_HOST_ESTIMATE, g_options, est->dle,
			   stdout);
    }

    est_prev = NULL;
    for(est = est_list; est != NULL; est = est->next) {
	free_estimates(est);
	amfree(est_prev);
	est_prev = est;
    }
    amfree(est_prev);
    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;
    free_g_options(g_options);

    dbclose();
    return 0;
 err:
    if (err_extra) {
	g_printf(_("ERROR FORMAT ERROR IN REQUEST PACKET '%s'\n"), err_extra);
	dbprintf(_("REQ packet is bogus: %s\n"), err_extra);
	amfree(err_extra);
    } else {
	g_printf(_("ERROR FORMAT ERROR IN REQUEST PACKET\n"));
	dbprintf(_("REQ packet is bogus\n"));
    }

    free_g_options(g_options);

    dbclose();
    return 1;
}


void
dle_add_diskest(
    dle_t    *dle)
{
    disk_estimates_t *newp, *curp;
    amandates_t *amdp;
    int dumplev, estlev;
    time_t dumpdate;
    levellist_t levellist;
    char *amandates_file;
    gboolean need_amandates = FALSE;
    estimatelist_t el;

    if (dle->levellist == NULL) {
	g_printf(_("ERROR Missing level in request\n"));
	return;
    }

    /* should we use amandates for this? */
    for (el = dle->estimatelist; el != NULL; el=el->next) {
	estimate_t estimate = (estimate_t)GPOINTER_TO_INT(el->data);
	if (estimate == ES_CALCSIZE)
	    need_amandates = TRUE;
    }

    if (strcmp(dle->program, "GNUTAR") == 0) {
	/* GNUTAR only needs amandates if gnutar_list_dir is NULL */
	char *gnutar_list_dir = getconf_str(CNF_GNUTAR_LIST_DIR);
	if (!gnutar_list_dir || !*gnutar_list_dir)
	    need_amandates = TRUE;
    }

    /* start amandates here, before adding this DLE to est_list, in case
     * we encounter an error. */
    if (need_amandates) {
	if (!amandates_started) {
	    amandates_file = getconf_str(CNF_AMANDATES);
	    if(!start_amandates(amandates_file, 0)) {
		char *errstr = strerror(errno);
		char *qamname = quote_string(dle->disk);
		char *errmsg = vstrallocf(_("could not open %s: %s"), amandates_file, errstr);
		char *qerrmsg = quote_string(errmsg);
		g_printf(_("%s %d ERROR %s\n"), qamname, 0, qerrmsg);
		amfree(qamname);
		amfree(errmsg);
		amfree(qerrmsg);
		return;
	    }
	    amandates_started = TRUE;
	}
    }

    levellist = dle->levellist;
    while (levellist != NULL) {
	am_level_t *alevel = (am_level_t *)levellist->data;
	if (alevel->level < 0)
	    alevel->level = 0;
	if (alevel->level >= DUMP_LEVELS)
	    alevel->level = DUMP_LEVELS - 1;
	levellist = g_slist_next(levellist);
    }

    for(curp = est_list; curp != NULL; curp = curp->next) {
	if(strcmp(curp->dle->disk, dle->disk) == 0) {
	    /* already have disk info, just note the level request */
	    levellist = dle->levellist;
	    while (levellist != NULL) {
		am_level_t *alevel = (am_level_t *)levellist->data;
		int      level  = alevel->level;
		curp->est[level].needestimate = 1;
		curp->est[level].server = alevel->server;
		levellist = g_slist_next(levellist);
	    }

	    return;
	}
    }

    newp = (disk_estimates_t *) alloc(SIZEOF(disk_estimates_t));
    memset(newp, 0, SIZEOF(*newp));
    newp->next = est_list;
    est_list = newp;
    newp->qamname = quote_string(dle->disk);
    if (dle->device) {
	newp->qamdevice = quote_string(dle->device);
	newp->dirname = amname_to_dirname(dle->device);
	newp->qdirname = quote_string(newp->dirname);
    } else {
	newp->qamdevice = stralloc("");
	newp->dirname = stralloc("");
	newp->qdirname = stralloc("");
    }
    levellist = dle->levellist;
    while (levellist != NULL) {
	am_level_t *alevel = (am_level_t *)levellist->data;
	newp->est[alevel->level].needestimate = 1;
	newp->est[alevel->level].server = alevel->server;
	levellist = g_slist_next(levellist);
    }
    newp->dle = dle;

    /* fill in dump-since dates */
    if (need_amandates) {
	amdp = amandates_lookup(newp->dle->disk);

	newp->est[0].dumpsince = EPOCH;
	for(dumplev = 0; dumplev < DUMP_LEVELS; dumplev++) {
	    dumpdate = amdp->dates[dumplev];
	    for(estlev = dumplev+1; estlev < DUMP_LEVELS; estlev++) {
		if(dumpdate > newp->est[estlev].dumpsince)
		    newp->est[estlev].dumpsince = dumpdate;
	    }
	}
    } else {
	/* just zero everything out */
	for(dumplev = 0; dumplev < DUMP_LEVELS; dumplev++) {
	    newp->est[dumplev].dumpsince = 0;
	}
    }
}


void
free_estimates(
    disk_estimates_t *	est)
{
    amfree(est->qamname);
    amfree(est->qamdevice);
    amfree(est->dirname);
    amfree(est->qdirname);
    if (est->dle) {
	free_dle(est->dle);
    }
}

/*
 * ------------------------------------------------------------------------
 *
 */

void
calc_estimates(
    disk_estimates_t *	est)
{
    dbprintf(_("calculating for amname %s, dirname %s, spindle %d %s\n"),
	     est->qamname, est->qdirname, est->dle->spindle, est->dle->program);

    if(est->dle->program_is_application_api ==  1)
	application_api_calc_estimate(est);
    else {
	estimatelist_t el;
	estimate_t     estimate;
	int            level;
	estimate_t     estimate_method = ES_ES;
	estimate_t     client_method = ES_ES;

	/* find estimate method to use */
	for (el = est->dle->estimatelist; el != NULL; el = el->next) {
	    estimate = (estimate_t)GPOINTER_TO_INT(el->data);
	    if (estimate == ES_SERVER) {
		if (estimate_method == ES_ES)
		    estimate_method = ES_SERVER;
	    }
	    if (estimate == ES_CLIENT || 
		(estimate == ES_CALCSIZE &&
		 (est->dle->device[0] != '/' || est->dle->device[1] != '/'))) {
		if (client_method == ES_ES)
		    client_method = estimate;
		if (estimate_method == ES_ES)
		    estimate_method = estimate;
	    }
	}

	/* do server estimate */
	if (estimate_method == ES_SERVER) {
	    for (level = 0; level < DUMP_LEVELS; level++) {
		if (est->est[level].needestimate) {
		    if (est->est[level].server || client_method == ES_ES) {
			g_printf(_("%s %d SIZE -1\n"), est->qamname, level);
			est->est[level].needestimate = 0;
		    }
		}
	    }
	}

	if (client_method == ES_ES && estimate_method != ES_SERVER) {
	    g_printf(_("%s %d SIZE -2\n"), est->qamname, 0);
	    dbprintf(_("Can't use CALCSIZE for samba estimate: %s %s\n"),
		     est->qamname, est->qdirname);
	} else if (client_method == ES_CALCSIZE) {
	    generic_calc_estimates(est);
	} else if (client_method == ES_CLIENT) {
#ifndef USE_GENERIC_CALCSIZE
	    if (strcmp(est->dle->program, "DUMP") == 0)
		dump_calc_estimates(est);
	    else
#endif
#ifdef SAMBA_CLIENT
	    if (strcmp(est->dle->program, "GNUTAR") == 0 &&
		est->dle->device[0] == '/' && est->dle->device[1] == '/')
		smbtar_calc_estimates(est);
	    else
#endif
#ifdef GNUTAR
	    if (strcmp(est->dle->program, "GNUTAR") == 0)
		gnutar_calc_estimates(est);
	    else
#endif
		dbprintf(_("Invalid program: %s %s %s\n"),
			 est->qamname, est->qdirname, est->dle->program);
	}
    }

    dbprintf(_("done with amname %s dirname %s spindle %d\n"),
	      est->qamname, est->qdirname, est->dle->spindle);
}

/*
 * ------------------------------------------------------------------------
 *
 */

/* local functions */
off_t getsize_dump(dle_t *dle, int level, char **errmsg);
off_t getsize_smbtar(dle_t *dle, int level, char **errmsg);
off_t getsize_gnutar(dle_t *dle, int level, time_t dumpsince, char **errmsg);
off_t getsize_application_api(disk_estimates_t *est, int nb_level,
			      int *levels, backup_support_option_t *bsu);
off_t handle_dumpline(char *str);
double first_num(char *str);

void
application_api_calc_estimate(
    disk_estimates_t *	est)
{
    int    level;
    int    i;
    int    levels[DUMP_LEVELS];
    int    nb_level = 0;
    backup_support_option_t *bsu;
    GPtrArray               *errarray;
    estimatelist_t el;
    estimate_t     estimate;
    estimate_t     estimate_method = ES_ES;
    estimate_t     client_method = ES_ES;
    int            has_calcsize = 0;
    int            has_client = 0;

    bsu = backup_support_option(est->dle->program, g_options, est->dle->disk,
				est->dle->device, &errarray);
    if (!bsu) {
	guint  i;
	for (i=0; i < errarray->len; i++) {
	    char  *line;
	    char  *errmsg;
	    char  *qerrmsg;
	    line = g_ptr_array_index(errarray, i);
	    if(am_has_feature(g_options->features,
			      fe_rep_sendsize_quoted_error)) {
		errmsg = g_strdup_printf(_("Application '%s': %s"),
				     est->dle->program, line);
		qerrmsg = quote_string(errmsg);
		for (level = 0; level < DUMP_LEVELS; level++) {
		    if (est->est[level].needestimate) {
			g_printf(_("%s %d ERROR %s\n"),
				 est->dle->disk, level, qerrmsg);
			dbprintf(_("%s %d ERROR %s\n"),
				 est->qamname, level, qerrmsg);
		    }
		}
		amfree(errmsg);
		amfree(qerrmsg);
	    }
	}
	if (i == 0) { /* nothing in errarray */
	    char  *errmsg;
	    char  *qerrmsg;
	    errmsg = g_strdup_printf(
		_("Application '%s': cannon execute support command"),
		est->dle->program);
	    qerrmsg = quote_string(errmsg);
	    for (level = 0; level < DUMP_LEVELS; level++) {
		if (est->est[level].needestimate) {
		    g_printf(_("%s %d ERROR %s\n"),
			     est->dle->disk, level, qerrmsg);
		    dbprintf(_("%s %d ERROR %s\n"),
			     est->qamname, level, qerrmsg);
		}
	    }
	    amfree(errmsg);
	    amfree(qerrmsg);
	}
	for (level = 0; level < DUMP_LEVELS; level++) {
	    est->est[level].needestimate = 0;
	}
	g_ptr_array_free(errarray, TRUE);
    }

    if (est->dle->data_path == DATA_PATH_AMANDA &&
	(bsu->data_path_set & DATA_PATH_AMANDA)==0) {
	g_printf("%s %d ERROR application %s doesn't support amanda data-path\n", est->qamname, 0, est->dle->program);
	amfree(bsu);
	return;
    }
    if (est->dle->data_path == DATA_PATH_DIRECTTCP &&
	(bsu->data_path_set & DATA_PATH_DIRECTTCP)==0) {
	g_printf("%s %d ERROR application %s doesn't support directtcp data-path\n", est->qamname, 0, est->dle->program);
	amfree(bsu);
	return;
    }

    /* find estimate method to use */
    for (el = est->dle->estimatelist; el != NULL; el = el->next) {
	estimate = (estimate_t)GPOINTER_TO_INT(el->data);
	if (estimate == ES_CLIENT)
	    has_client = 1;
	if (estimate == ES_CALCSIZE)
	    has_calcsize = 1;
	if (estimate == ES_SERVER) {
	    if (estimate_method == ES_ES)
		estimate_method = ES_SERVER;
	}
	if ((estimate == ES_CLIENT && bsu->client_estimate) || 
	    (estimate == ES_CALCSIZE && bsu->calcsize)) {
	    if (client_method == ES_ES)
		client_method = estimate;
	    if (estimate_method == ES_ES)
		estimate_method = estimate;
	}
    }

    for(level = 0; level < DUMP_LEVELS; level++) {
	if (est->est[level].needestimate) {
	    if (level > bsu->max_level) {
		/* planner will not even consider this level */
		g_printf("%s %d SIZE %lld\n", est->qamname, level,
			 (long long)-2);
		est->est[level].needestimate = 0;
		dbprintf(_("Application '%s' can't estimate level %d\n"),
                         est->dle->program, level);
	    } else if (estimate_method == ES_ES) {
		g_printf("%s %d SIZE %lld\n", est->qamname, level,
			 (long long)-2);
		est->est[level].needestimate = 0;
		if (am_has_feature(g_options->features,
				   fe_rep_sendsize_quoted_error)) {
		    char *errmsg, *qerrmsg;
		    if (has_client && !bsu->client_estimate &&
			has_calcsize && !bsu->calcsize) {
			errmsg = vstrallocf(_("Application '%s' can't do CLIENT or CALCSIZE estimate"),
					    est->dle->program);
		    } else if (has_client && !bsu->client_estimate) {
			errmsg = vstrallocf(_("Application '%s' can't do CLIENT estimate"),
					    est->dle->program);
		    } else if (has_calcsize && !bsu->calcsize) {
			errmsg = vstrallocf(_("Application '%s' can't do CALCSIZE estimate"),
					    est->dle->program);
		    } else {
			errmsg = vstrallocf(_("Application '%s' can't do estimate"),
					    est->dle->program);
		    }
		    qerrmsg = quote_string(errmsg);
		    dbprintf(_("errmsg is %s\n"), errmsg);
		    g_printf("%s %d ERROR %s\n",
			     est->qamname, 0, qerrmsg);
		    amfree(errmsg);
		    amfree(qerrmsg);
		}
	    } else if (estimate_method == ES_SERVER &&
		       (est->est[level].server || client_method == ES_ES)) {
		/* planner will consider this level, */
		/* but use a server-side estimate    */
		g_printf("%s %d SIZE -1\n", est->qamname, level);
		est->est[level].needestimate = 0;
	    } else if (client_method == ES_CLIENT) {
		levels[nb_level++] = level;
	    } else if (client_method == ES_CALCSIZE) {
		levels[nb_level++] = level;
	    }
	}
    }

    if (nb_level == 0) {
	amfree(bsu);
	return;
    }

    if (bsu->multi_estimate) {
	for (i=0;i<nb_level;i++) {
	    dbprintf(_("getting size via application API for %s %s level %d\n"),
		     est->qamname, est->qamdevice, levels[i]);
	}
	getsize_application_api(est, nb_level, levels, bsu);

    } else {
	for(level = 0; level < DUMP_LEVELS; level++) {
	    if (est->est[level].needestimate) {
		dbprintf(
		    _("getting size via application API for %s %s level %d\n"),
		    est->qamname, est->qamdevice, level);
		levels[0] = level;
		getsize_application_api(est, 1, levels, bsu);
	    }
	}
    }

    amfree(bsu);
}


void
generic_calc_estimates(
    disk_estimates_t *	est)
{
    int pipefd = -1, nullfd = -1;
    char *cmd;
    char *cmdline;
    char *command;
    GPtrArray *argv_ptr = g_ptr_array_new();
    char number[NUM_STR_SIZE];
    unsigned int i;
    int level;
    pid_t calcpid;
    int nb_exclude = 0;
    int nb_include = 0;
    char *file_exclude = NULL;
    char *file_include = NULL;
    times_t start_time;
    FILE *dumpout = NULL;
    char *line = NULL;
    char *match_expr;
    amwait_t wait_status;
    char *errmsg = NULL, *qerrmsg;
    char tmppath[PATH_MAX];
    int len;

    cmd = vstralloc(amlibexecdir, "/", "calcsize", NULL);

    g_ptr_array_add(argv_ptr, stralloc("calcsize"));
    if (g_options->config)
	g_ptr_array_add(argv_ptr, stralloc(g_options->config));
    else
	g_ptr_array_add(argv_ptr, stralloc("NOCONFIG"));

    g_ptr_array_add(argv_ptr, stralloc(est->dle->program));
    canonicalize_pathname(est->dle->disk, tmppath);
    g_ptr_array_add(argv_ptr, stralloc(tmppath));
    canonicalize_pathname(est->dirname, tmppath);
    g_ptr_array_add(argv_ptr, stralloc(tmppath));

    if (est->dle->exclude_file)
	nb_exclude += est->dle->exclude_file->nb_element;
    if (est->dle->exclude_list)
	nb_exclude += est->dle->exclude_list->nb_element;
    if (est->dle->include_file)
	nb_include += est->dle->include_file->nb_element;
    if (est->dle->include_list)
	nb_include += est->dle->include_list->nb_element;

    if (nb_exclude > 0)
	file_exclude = build_exclude(est->dle, 0);
    if (nb_include > 0)
	file_include = build_include(est->dle, 0);

    if(file_exclude) {
	g_ptr_array_add(argv_ptr, stralloc("-X"));
	g_ptr_array_add(argv_ptr, stralloc(file_exclude));
    }

    if(file_include) {
	g_ptr_array_add(argv_ptr, stralloc("-I"));
	g_ptr_array_add(argv_ptr, stralloc(file_include));
    }
    start_time = curclock();

    command = (char *)g_ptr_array_index(argv_ptr, 0);
    cmdline = stralloc(command);
    for(i = 1; i < argv_ptr->len-1; i++) {
	cmdline = vstrextend(&cmdline, " ",
			     (char *)g_ptr_array_index(argv_ptr, i), NULL);
    }
    dbprintf(_("running: \"%s\"\n"), cmdline);
    amfree(cmdline);

    for(level = 0; level < DUMP_LEVELS; level++) {
	if(est->est[level].needestimate) {
	    g_snprintf(number, SIZEOF(number), "%d", level);
	    g_ptr_array_add(argv_ptr, stralloc(number));
	    dbprintf(" %s", number);
	    g_snprintf(number, SIZEOF(number),
			"%ld", (long)est->est[level].dumpsince);
	    g_ptr_array_add(argv_ptr, stralloc(number));
	    dbprintf(" %s", number);
	}
    }
    g_ptr_array_add(argv_ptr, NULL);
    dbprintf("\n");

    fflush(stderr); fflush(stdout);

    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	errmsg = vstrallocf(_("Cannot access /dev/null : %s"),
			    strerror(errno));
	dbprintf("%s\n", errmsg);
	goto common_exit;
    }

    calcpid = pipespawnv(cmd, STDERR_PIPE, 0,
			 &nullfd, &nullfd, &pipefd, (char **)argv_ptr->pdata);
    amfree(cmd);

    dumpout = fdopen(pipefd,"r");
    if (!dumpout) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }
    match_expr = vstralloc(" %d SIZE %lld", NULL);
    len = strlen(est->qamname);
    for(; (line = agets(dumpout)) != NULL; free(line)) {
	long long size_ = (long long)0;
	if (line[0] == '\0' || (int)strlen(line) <= len)
	    continue;
	/* Don't use sscanf for est->qamname because it can have a '%'. */
	if (strncmp(line, est->qamname, len) == 0 &&
	    sscanf(line+len, match_expr, &level, &size_) == 2) {
	    g_printf("%s\n", line); /* write to amandad */
	    dbprintf(_("estimate size for %s level %d: %lld KB\n"),
		      est->qamname,
		      level,
		      size_);
	}
    }
    amfree(match_expr);

    dbprintf(_("waiting for %s %s child (pid=%d)\n"),
	     command, est->qamdevice, (int)calcpid);
    waitpid(calcpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
			    "calcsize", WTERMSIG(wait_status),
			    dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    errmsg = vstrallocf(_("%s exited with status %d: see %s"),
			        "calcsize", WEXITSTATUS(wait_status),
				dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = vstrallocf(_("%s got bad exit: see %s"),
			     "calcsize", dbfn());
    }
    dbprintf(_("after %s %s wait: child pid=%d status=%d\n"),
	     command, est->qamdevice,
	      (int)calcpid, WEXITSTATUS(wait_status));

    dbprintf(_(".....\n"));
    dbprintf(_("estimate time for %s: %s\n"),
	      est->qamname,
	      walltime_str(timessub(curclock(), start_time)));

common_exit:
    if (errmsg && errmsg[0] != '\0') {
	if(am_has_feature(g_options->features, fe_rep_sendsize_quoted_error)) {
	    qerrmsg = quote_string(errmsg);
	    dbprintf(_("errmsg is %s\n"), errmsg);
	    g_printf("%s %d ERROR %s\n",
		    est->qamname, 0, qerrmsg);
	    amfree(qerrmsg);
	}
    }
    amfree(errmsg);
    g_ptr_array_free_full(argv_ptr);
    amfree(cmd);
}


void
dump_calc_estimates(
    disk_estimates_t *	est)
{
    int level;
    off_t size;
    char *errmsg=NULL, *qerrmsg;

    for(level = 0; level < DUMP_LEVELS; level++) {
	if(est->est[level].needestimate) {
	    dbprintf(_("getting size via dump for %s level %d\n"),
		      est->qamname, level);
	    size = getsize_dump(est->dle, level, &errmsg);

	    amflock(1, "size");

	    g_printf(_("%s %d SIZE %lld\n"),
		   est->qamname, level, (long long)size);
	    if (errmsg && errmsg[0] != '\0') {
		if(am_has_feature(g_options->features,
				  fe_rep_sendsize_quoted_error)) {
		    qerrmsg = quote_string(errmsg);
		    dbprintf(_("errmsg is %s\n"), errmsg);
		    g_printf("%s %d ERROR %s\n",
			   est->qamname, level, qerrmsg);
		    amfree(qerrmsg);
		}
	    }
	    amfree(errmsg);
	    fflush(stdout);

	    amfunlock(1, "size");
	}
    }
}

#ifdef SAMBA_CLIENT
void
smbtar_calc_estimates(
    disk_estimates_t *	est)
{
    int level;
    off_t size;
    char  *errmsg = NULL, *qerrmsg;

    for(level = 0; level < DUMP_LEVELS; level++) {
	if(est->est[level].needestimate) {
	    dbprintf(_("getting size via smbclient for %s level %d\n"),
		      est->qamname, level);
	    size = getsize_smbtar(est->dle, level, &errmsg);

	    amflock(1, "size");

	    g_printf(_("%s %d SIZE %lld\n"),
		   est->qamname, level, (long long)size);
	    if (errmsg && errmsg[0] != '\0') {
		if(am_has_feature(g_options->features,
				  fe_rep_sendsize_quoted_error)) {
		    qerrmsg = quote_string(errmsg);
		    dbprintf(_("errmsg is %s\n"), errmsg);
		    g_printf("%s %d ERROR %s\n",
			   est->qamname, level, qerrmsg);
		    amfree(qerrmsg);
		}
	    }
	    amfree(errmsg);
	    fflush(stdout);

	    amfunlock(1, "size");
	}
    }
}
#endif

#ifdef GNUTAR
void
gnutar_calc_estimates(
    disk_estimates_t *	est)
{
    int level;
    off_t size;
    char *errmsg = NULL, *qerrmsg;

    for(level = 0; level < DUMP_LEVELS; level++) {
	if (est->est[level].needestimate) {
	    dbprintf(_("getting size via gnutar for %s level %d\n"),
		      est->qamname, level);
	    size = getsize_gnutar(est->dle, level,
				  est->est[level].dumpsince,
				  &errmsg);

	    amflock(1, "size");

	    g_printf(_("%s %d SIZE %lld\n"),
		   est->qamname, level, (long long)size);
	    if (errmsg && errmsg[0] != '\0') {
		if(am_has_feature(g_options->features,
				  fe_rep_sendsize_quoted_error)) {
		    qerrmsg = quote_string(errmsg);
		    dbprintf(_("errmsg is %s\n"), errmsg);
		    g_printf(_("%s %d ERROR %s\n"),
			   est->qamname, level, qerrmsg);
		    amfree(qerrmsg);
		}
	    }
	    amfree(errmsg);
	    fflush(stdout);

	    amfunlock(1, "size");
	}
    }
}
#endif

typedef struct regex_scale_s {
    char *regex;
    int scale;
} regex_scale_t;

/*@ignore@*/
regex_scale_t re_size[] = {
#ifdef DUMP
    {"  DUMP: estimated -*[0-9][0-9]* tape blocks", 1024},
    {"  DUMP: [Ee]stimated [0-9][0-9]* blocks", 512},
    {"  DUMP: [Ee]stimated [0-9][0-9]* bytes", 1},		/* Ultrix 4.4 */
    {" UFSDUMP: estimated [0-9][0-9]* blocks", 512},		/* NEC EWS-UX */
    {"dump: Estimate: [0-9][0-9]* tape blocks", 1024},		    /* OSF/1 */
    {"backup: There are an estimated [0-9][0-9]* tape blocks.",1024}, /* AIX */
    {"backup: estimated [0-9][0-9]* 1k blocks", 1024},		      /* AIX */
    {"backup: estimated [0-9][0-9]* tape blocks", 1024},	      /* AIX */
    {"backup: [0-9][0-9]* tape blocks on [0-9][0-9]* tape(s)",1024},  /* AIX */
    {"backup: [0-9][0-9]* 1k blocks on [0-9][0-9]* volume(s)",1024},  /* AIX */
    {"dump: Estimate: [0-9][0-9]* blocks being output to pipe",1024},
							      /* DU 4.0 dump  */
    {"dump: Dumping [0-9][0-9]* bytes, ", 1},		      /* DU 4.0 vdump */
    {"DUMP: estimated [0-9][0-9]* KB output", 1024},		      /* HPUX */
    {"DUMP: estimated [0-9][0-9]* KB\\.", 1024},		    /* NetApp */
    {"  UFSDUMP: estimated [0-9][0-9]* blocks", 512},		     /* Sinix */

#ifdef HAVE_DUMP_ESTIMATE
    {"[0-9][0-9]* blocks, [0-9][0-9]*.[0-9][0-9]* volumes", 1024},
							   /* DU 3.2g dump -E */
    {"^[0-9][0-9]* blocks$", 1024},			   /* DU 4.0 dump  -E */
    {"^[0-9][0-9]*$", 1},				/* Solaris ufsdump -S */
#endif
#endif

#ifdef VDUMP
    {"vdump: Dumping [0-9][0-9]* bytes, ", 1},		       /* OSF/1 vdump */
#endif
    
#ifdef VXDUMP
    {"vxdump: estimated [0-9][0-9]* blocks", 512},	     /* HPUX's vxdump */
    {"  VXDUMP: estimated [0-9][0-9]* blocks", 512},		     /* Sinix */
#endif

#ifdef XFSDUMP
    {"xfsdump: estimated dump size: [0-9][0-9]* bytes", 1},  /* Irix 6.2 xfs */
#endif

#ifdef GNUTAR
    {"Total bytes written: [0-9][0-9]*", 1},		    /* Gnutar client */
#endif

#ifdef SAMBA_CLIENT
#if SAMBA_VERSION >= 2
#define SAMBA_DEBUG_LEVEL "0"
    {"Total number of bytes: [0-9][0-9]*", 1},			 /* Samba du */
#else
#define SAMBA_DEBUG_LEVEL "3"
    {"Total bytes listed: [0-9][0-9]*", 1},			/* Samba dir */
#endif
#endif

    { NULL, 0 }
};
/*@end@*/

off_t
getsize_dump(
    dle_t      *dle,
    int		level,
    char      **errmsg)
{
    int pipefd[2], nullfd, stdoutfd, killctl[2];
    pid_t dumppid;
    off_t size;
    FILE *dumpout;
    char *dumpkeys = NULL;
    char *device = NULL;
    char *fstype = NULL;
    char *cmd = NULL;
    char *name = NULL;
    char *line = NULL;
    char *rundump_cmd = NULL;
    char level_str[NUM_STR_SIZE];
    int s;
    times_t start_time;
    char *qdisk = quote_string(dle->disk);
    char *qdevice;
    char *config;
    amwait_t wait_status;
#if defined(DUMP) || defined(VDUMP) || defined(VXDUMP) || defined(XFSDUMP)
    int is_rundump = 1;
#endif

    if (level > 9)
	return -2; /* planner will not even consider this level */

    g_snprintf(level_str, SIZEOF(level_str), "%d", level);

    device = amname_to_devname(dle->device);
    qdevice = quote_string(device);
    fstype = amname_to_fstype(dle->device);

    dbprintf(_("calculating for device %s with %s\n"),
	      qdevice, fstype);

    cmd = vstralloc(amlibexecdir, "/rundump", NULL);
    rundump_cmd = stralloc(cmd);
    if (g_options->config)
        config = g_options->config;
    else
        config = "NOCONFIG";
    if ((stdoutfd = nullfd = open("/dev/null", O_RDWR)) == -1) {
	*errmsg = vstrallocf(_("getsize_dump could not open /dev/null: %s"),
			     strerror(errno));
	dbprintf("%s\n", *errmsg);
	amfree(cmd);
	amfree(rundump_cmd);
	amfree(fstype);
	amfree(device);
	amfree(qdevice);
	amfree(qdisk);
	return(-1);
    }
    pipefd[0] = pipefd[1] = killctl[0] = killctl[1] = -1;
    if (pipe(pipefd) < 0) {
	*errmsg = vstrallocf(_("getsize_dump could create data pipes: %s"),
			     strerror(errno));
	dbprintf("%s\n", *errmsg);
	amfree(cmd);
	amfree(rundump_cmd);
	amfree(fstype);
	amfree(device);
	amfree(qdevice);
	amfree(qdisk);
	return(-1);
    }

#ifdef XFSDUMP						/* { */
#ifdef DUMP						/* { */
    if (strcmp(fstype, "xfs") == 0)
#else							/* } { */
    if (1)
#endif							/* } */
    {
	name = stralloc(" (xfsdump)");
	dbprintf(_("running \"%s%s -F -J -l %s - %s\"\n"),
		  cmd, name, level_str, qdevice);
    }
    else
#endif							/* } */
#ifdef VXDUMP						/* { */
#ifdef DUMP						/* { */
    if (strcmp(fstype, "vxfs") == 0)
#else							/* } { */
    if (1)
#endif							/* } */
    {
#ifdef USE_RUNDUMP
	name = stralloc(" (vxdump)");
#else
	name = stralloc("");
	cmd = newstralloc(cmd, VXDUMP);
	config = skip_argument;
	is_rundump = 0;
#endif
	dumpkeys = vstralloc(level_str, "s", "f", NULL);
	dbprintf(_("running \"%s%s %s 1048576 - %s\"\n"),
		  cmd, name, dumpkeys, qdevice);
    }
    else
#endif							/* } */
#ifdef VDUMP						/* { */
#ifdef DUMP						/* { */
    if (strcmp(fstype, "advfs") == 0)
#else							/* } { */
    if (1)
#endif							/* } */
    {
	name = stralloc(" (vdump)");
	dumpkeys = vstralloc(level_str, "b", "f", NULL);
	dbprintf(_("running \"%s%s %s 60 - %s\"\n"),
		  cmd, name, dumpkeys, qdevice);
    }
    else
#endif							/* } */
#ifdef DUMP						/* { */
    if (1) {
# ifdef USE_RUNDUMP					/* { */
#  ifdef AIX_BACKUP					/* { */
	name = stralloc(" (backup)");
#  else							/* } { */
	name = vstralloc(" (", DUMP, ")", NULL);
#  endif						/* } */
# else							/* } { */
	name = stralloc("");
	cmd = newstralloc(cmd, DUMP);
        config = skip_argument;
	is_rundump = 0;
# endif							/* } */

# ifdef AIX_BACKUP					/* { */
	dumpkeys = vstralloc("-", level_str, "f", NULL);
	dbprintf(_("running \"%s%s %s - %s\"\n"),
		  cmd, name, dumpkeys, qdevice);
# else							/* } { */
#  ifdef HAVE_DUMP_ESTIMATE
#    define PARAM_DUMP_ESTIMATE HAVE_DUMP_ESTIMATE
#  else
#    define PARAM_DUMP_ESTIMATE ""
#  endif
#  ifdef HAVE_HONOR_NODUMP
#    define PARAM_HONOR_NODUMP "h"
#  else
#    define PARAM_HONOR_NODUMP ""
#  endif
	dumpkeys = vstralloc(level_str,
			     PARAM_DUMP_ESTIMATE,
			     PARAM_HONOR_NODUMP,
			     "s", "f", NULL);

#  ifdef HAVE_DUMP_ESTIMATE
	stdoutfd = pipefd[1];
#  endif

#  ifdef HAVE_HONOR_NODUMP				/* { */
	dbprintf(_("running \"%s%s %s 0 1048576 - %s\"\n"),
		  cmd, name, dumpkeys, qdevice);
#  else							/* } { */
	dbprintf(_("running \"%s%s %s 1048576 - %s\"\n"),
		  cmd, name, dumpkeys, qdevice);
#  endif						/* } */
# endif							/* } */
    }
    else
#endif							/* } */
    {
	error(_("no dump program available"));
	/*NOTREACHED*/
    }

    if (pipe(killctl) < 0) {
	dbprintf(_("Could not create pipe: %s\n"), strerror(errno));
	/* Message will be printed later... */
	killctl[0] = killctl[1] = -1;
    }

    start_time = curclock();
    switch(dumppid = fork()) {
    case -1:
	*errmsg = vstrallocf(_("cannot fork for killpgrp: %s"),
			     strerror(errno));
	dbprintf("%s\n", *errmsg);
	amfree(dumpkeys);
	amfree(cmd);
	amfree(rundump_cmd);
	amfree(device);
	amfree(qdevice);
	amfree(qdisk);
	amfree(name);
	amfree(fstype);
	return -1;
    default:
	break; 
    case 0:	/* child process */
	if(SETPGRP == -1)
	    SETPGRP_FAILED();
	else if (killctl[0] == -1 || killctl[1] == -1)
	    dbprintf(_("Trying without killpgrp\n"));
	else {
	    switch(fork()) {
	    case -1:
		dbprintf(_("fork failed, trying without killpgrp\n"));
		break;

	    default:
	    {
		char *config;
		char *killpgrp_cmd = vstralloc(amlibexecdir, "/killpgrp", NULL);
		dbprintf(_("running %s\n"), killpgrp_cmd);
		dup2(killctl[0], 0);
		dup2(nullfd, 1);
		dup2(nullfd, 2);
		close(pipefd[0]);
		close(pipefd[1]);
		close(killctl[1]);
		close(nullfd);
		if (g_options->config)
		    config = g_options->config;
		else
		    config = "NOCONFIG";
		safe_fd(-1, 0);
		execle(killpgrp_cmd, killpgrp_cmd, config, (char *)0,
		       safe_env());
		dbprintf(_("cannot execute %s: %s\n"),
			  killpgrp_cmd, strerror(errno));
		exit(-1);
	    }

	    case 0:  /* child process */
		break;
	    }
	}

	dup2(nullfd, 0);
	dup2(stdoutfd, 1);
	dup2(pipefd[1], 2);
	aclose(pipefd[0]);
	if (killctl[0] != -1)
	    aclose(killctl[0]);
	if (killctl[1] != -1)
	    aclose(killctl[1]);
	safe_fd(-1, 0);

#ifdef XFSDUMP
#ifdef DUMP
	if (strcmp(fstype, "xfs") == 0)
#else
	if (1)
#endif
	    if (is_rundump)
		execle(cmd, "rundump", config, "xfsdump", "-F", "-J", "-l",
		       level_str, "-", device, (char *)0, safe_env());
	    else
		execle(cmd, "xfsdump", "-F", "-J", "-l",
		       level_str, "-", device, (char *)0, safe_env());
	else
#endif
#ifdef VXDUMP
#ifdef DUMP
	if (strcmp(fstype, "vxfs") == 0)
#else
	if (1)
#endif
	    if (is_rundump)
		execle(cmd, "rundump", config, "vxdump", dumpkeys, "1048576",
		       "-", device, (char *)0, safe_env());
	    else
		execle(cmd, "vxdump", dumpkeys, "1048576", "-",
		       device, (char *)0, safe_env());
	else
#endif
#ifdef VDUMP
#ifdef DUMP
	if (strcmp(fstype, "advfs") == 0)
#else
	if (1)
#endif
	    if (is_rundump)
		execle(cmd, "rundump", config, "vdump", dumpkeys, "60", "-",
		       device, (char *)0, safe_env());
	    else
		execle(cmd, "vdump", dumpkeys, "60", "-",
		       device, (char *)0, safe_env());
	else
#endif
#ifdef DUMP
# ifdef AIX_BACKUP
	    if (is_rundump)
		execle(cmd, "rundump", config, "backup", dumpkeys, "-",
		       device, (char *)0, safe_env());
	    else
		execle(cmd, "backup", dumpkeys, "-",
		       device, (char *)0, safe_env());
# else
	    if (is_rundump) {
		execle(cmd, "rundump", config, "dump", dumpkeys, 
#ifdef HAVE_HONOR_NODUMP
		       "0",
#endif
		       "1048576", "-", device, (char *)0, safe_env());
	    } else {
		execle(cmd, "dump", dumpkeys, 
#ifdef HAVE_HONOR_NODUMP
		       "0",
#endif
		       "1048576", "-", device, (char *)0, safe_env());
	    }
# endif
#endif
	{
	    error(_("exec %s failed or no dump program available: %s"),
		  cmd, strerror(errno));
	    /*NOTREACHED*/
	}
    }

    amfree(dumpkeys);
    amfree(rundump_cmd);

    aclose(pipefd[1]);
    if (killctl[0] != -1)
	aclose(killctl[0]);
    dumpout = fdopen(pipefd[0],"r");
    if (!dumpout) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	dbprintf("%s\n", line);
	size = handle_dumpline(line);
	if(size > (off_t)-1) {
	    amfree(line);
	    while ((line = agets(dumpout)) != NULL) {
	        if (line[0] != '\0')
		    break;
		amfree(line);
	    }
	    if (line != NULL) {
		dbprintf("%s\n", line);
	    }
	    break;
	}
    }
    amfree(line);

    dbprintf(".....\n");
    dbprintf(_("estimate time for %s level %d: %s\n"),
	      qdisk,
	      level,
	      walltime_str(timessub(curclock(), start_time)));
    if(size == (off_t)-1) {
	*errmsg = vstrallocf(_("no size line match in %s%s output"),
			     cmd, name);
	dbprintf(_("%s for %s\n"),
		  *errmsg, qdisk);

	dbprintf(".....\n");
	dbprintf(_("Run %s%s manually to check for errors\n"),
		    cmd, name);
    } else if(size == (off_t)0 && level == 0) {
	dbprintf(_("possible %s%s problem -- is \"%s\" really empty?\n"),
		  cmd, name, dle->disk);
	dbprintf(".....\n");
    } else {
	    dbprintf(_("estimate size for %s level %d: %lld KB\n"),
	      qdisk,
	      level,
	      (long long)size);
    }

    if (killctl[1] != -1) {
	dbprintf(_("asking killpgrp to terminate\n"));
	aclose(killctl[1]);
	for(s = 5; s > 0; --s) {
	    sleep(1);
	    if (waitpid(dumppid, NULL, WNOHANG) != -1)
		goto terminated;
	}
    }
    
    /*
     * First, try to kill the dump process nicely.  If it ignores us
     * for several seconds, hit it harder.
     */
    dbprintf(_("sending SIGTERM to process group %ld\n"), (long)dumppid);
    if (kill(-dumppid, SIGTERM) == -1) {
	dbprintf(_("kill failed: %s\n"), strerror(errno));
    }
    /* Now check whether it dies */
    for(s = 5; s > 0; --s) {
	sleep(1);
	if (waitpid(dumppid, NULL, WNOHANG) != -1)
	    goto terminated;
    }

    dbprintf(_("sending SIGKILL to process group %ld\n"), (long)dumppid);
    if (kill(-dumppid, SIGKILL) == -1) {
	dbprintf(_("kill failed: %s\n"), strerror(errno));
    }
    for(s = 5; s > 0; --s) {
	sleep(1);
	if (waitpid(dumppid, NULL, WNOHANG) != -1)
	    goto terminated;
    }

    dbprintf(_("waiting for %s%s \"%s\" child\n"), cmd, name, qdisk);
    waitpid(dumppid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	*errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
			     cmd, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    *errmsg = vstrallocf(_("%s exited with status %d: see %s"),
			         cmd, WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	*errmsg = vstrallocf(_("%s got bad exit: see %s"),
			     cmd, dbfn());
    }
    dbprintf(_("after %s%s %s wait\n"), cmd, name, qdisk);

 terminated:

    aclose(nullfd);
    afclose(dumpout);

    amfree(device);
    amfree(qdevice);
    amfree(qdisk);
    amfree(fstype);

    amfree(cmd);
    amfree(name);

    return size;
}

#ifdef SAMBA_CLIENT
off_t
getsize_smbtar(
    dle_t      *dle,
    int		level,
    char      **errmsg)
{
    int pipefd = -1, nullfd = -1, passwdfd = -1;
    pid_t dumppid;
    off_t size;
    FILE *dumpout;
    char *tarkeys, *sharename, *user_and_password = NULL, *domain = NULL;
    char *share = NULL, *subdir = NULL;
    size_t lpass;
    char *pwtext;
    size_t pwtext_len;
    char *line;
    char *pw_fd_env;
    times_t start_time;
    char *error_pn = NULL;
    char *qdisk = quote_string(dle->disk);
    amwait_t wait_status;

    error_pn = stralloc2(get_pname(), "-smbclient");

    if (level > 1)
	return -2; /* planner will not even consider this level */

    parsesharename(dle->device, &share, &subdir);
    if (!share) {
	amfree(share);
	amfree(subdir);
	set_pname(error_pn);
	amfree(error_pn);
	error(_("cannot parse disk entry %s for share/subdir"), qdisk);
	/*NOTREACHED*/
    }
    if ((subdir) && (SAMBA_VERSION < 2)) {
	amfree(share);
	amfree(subdir);
	set_pname(error_pn);
	amfree(error_pn);
	error(_("subdirectory specified for share %s but samba not v2 or better"), qdisk);
	/*NOTREACHED*/
    }
    if ((user_and_password = findpass(share, &domain)) == NULL) {

	if(domain) {
	    memset(domain, '\0', strlen(domain));
	    amfree(domain);
	}
	set_pname(error_pn);
	amfree(error_pn);
	error(_("cannot find password for %s"), dle->disk);
	/*NOTREACHED*/
    }
    lpass = strlen(user_and_password);
    if ((pwtext = strchr(user_and_password, '%')) == NULL) {
	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
	if(domain) {
	    memset(domain, '\0', strlen(domain));
	    amfree(domain);
	}
	set_pname(error_pn);
	amfree(error_pn);
	error(_("password field not \'user%%pass\' for %s"), dle->disk);
	/*NOTREACHED*/
    }
    *pwtext++ = '\0';
    pwtext_len = strlen(pwtext);
    if ((sharename = makesharename(share, 0)) == NULL) {
	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
	if(domain) {
	    memset(domain, '\0', strlen(domain));
	    amfree(domain);
	}
	set_pname(error_pn);
	amfree(error_pn);
	error(_("cannot make share name of %s"), share);
	/*NOTREACHED*/
    }
    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
	if(domain) {
	    memset(domain, '\0', strlen(domain));
	    amfree(domain);
	}
	set_pname(error_pn);
	amfree(error_pn);
	amfree(sharename);
	error(_("could not open /dev/null: %s\n"),
	      strerror(errno));
	/*NOTREACHED*/
    }

#if SAMBA_VERSION >= 2
    if (level == 0)
	tarkeys = "archive 0;recurse;du";
    else
	tarkeys = "archive 1;recurse;du";
#else
    if (level == 0)
	tarkeys = "archive 0;recurse;dir";
    else
	tarkeys = "archive 1;recurse;dir";
#endif

    start_time = curclock();

    if (pwtext_len > 0) {
	pw_fd_env = "PASSWD_FD";
    } else {
	pw_fd_env = "dummy_PASSWD_FD";
    }
    dumppid = pipespawn(SAMBA_CLIENT, STDERR_PIPE|PASSWD_PIPE, 0,
	      &nullfd, &nullfd, &pipefd, 
	      pw_fd_env, &passwdfd,
	      "smbclient",
	      sharename,
	      "-d", SAMBA_DEBUG_LEVEL,
	      *user_and_password ? "-U" : skip_argument,
	      *user_and_password ? user_and_password : skip_argument,
	      "-E",
	      domain ? "-W" : skip_argument,
	      domain ? domain : skip_argument,
#if SAMBA_VERSION >= 2
	      subdir ? "-D" : skip_argument,
	      subdir ? subdir : skip_argument,
#endif
	      "-c", tarkeys,
	      NULL);
    if(domain) {
	memset(domain, '\0', strlen(domain));
	amfree(domain);
    }
    aclose(nullfd);
    if(pwtext_len > 0 && full_write(passwdfd, pwtext, pwtext_len) < pwtext_len) {
	int save_errno = errno;

	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
	aclose(passwdfd);
	set_pname(error_pn);
	amfree(error_pn);
	error(_("password write failed: %s"), strerror(save_errno));
	/*NOTREACHED*/
    }
    memset(user_and_password, '\0', (size_t)lpass);
    amfree(user_and_password);
    aclose(passwdfd);
    amfree(sharename);
    amfree(share);
    amfree(subdir);
    amfree(error_pn);
    dumpout = fdopen(pipefd,"r");
    if (!dumpout) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	dbprintf("%s\n", line);
	size = handle_dumpline(line);
	if(size > -1) {
	    amfree(line);
	    while ((line = agets(dumpout)) != NULL) {
	        if (line[0] != '\0')
		    break;
		amfree(line);
	    }
	    if(line != NULL) {
		dbprintf("%s\n", line);
	    }
	    break;
	}
    }
    amfree(line);

    dbprintf(".....\n");
    dbprintf(_("estimate time for %s level %d: %s\n"),
	      qdisk,
	      level,
	      walltime_str(timessub(curclock(), start_time)));
    if(size == (off_t)-1) {
	*errmsg = vstrallocf(_("no size line match in %s output"),
			     SAMBA_CLIENT);
	dbprintf(_("%s for %s\n"),
		  *errmsg, qdisk);
	dbprintf(".....\n");
    } else if(size == (off_t)0 && level == 0) {
	dbprintf(_("possible %s problem -- is \"%s\" really empty?\n"),
		  SAMBA_CLIENT, dle->disk);
	dbprintf(".....\n");
    }
    dbprintf(_("estimate size for %s level %d: %lld KB\n"),
	      qdisk,
	      level,
	      (long long)size);

    kill(-dumppid, SIGTERM);

    dbprintf(_("waiting for %s \"%s\" child\n"), SAMBA_CLIENT, qdisk);
    waitpid(dumppid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	*errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
			     SAMBA_CLIENT, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    *errmsg = vstrallocf(_("%s exited with status %d: see %s"),
				 SAMBA_CLIENT, WEXITSTATUS(wait_status),
				 dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	*errmsg = vstrallocf(_("%s got bad exit: see %s"),
			     SAMBA_CLIENT, dbfn());
    }
    dbprintf(_("after %s %s wait\n"), SAMBA_CLIENT, qdisk);

    afclose(dumpout);
    pipefd = -1;

    amfree(error_pn);
    amfree(qdisk);

    return size;
}
#endif

#ifdef GNUTAR
off_t
getsize_gnutar(
    dle_t      *dle,
    int		level,
    time_t	dumpsince,
    char      **errmsg)
{
    int pipefd = -1, nullfd = -1;
    pid_t dumppid;
    off_t size = (off_t)-1;
    FILE *dumpout = NULL;
    char *incrname = NULL;
    char *basename = NULL;
    char *dirname = NULL;
    char *inputname = NULL;
    FILE *in = NULL;
    FILE *out = NULL;
    char *line = NULL;
    char *cmd = NULL;
    char *command = NULL;
    char dumptimestr[80];
    struct tm *gmtm;
    int nb_exclude = 0;
    int nb_include = 0;
    GPtrArray *argv_ptr = g_ptr_array_new();
    char *file_exclude = NULL;
    char *file_include = NULL;
    times_t start_time;
    int infd, outfd;
    ssize_t nb;
    char buf[32768];
    char *qdisk = quote_string(dle->disk);
    char *gnutar_list_dir;
    amwait_t wait_status;
    char tmppath[PATH_MAX];

    if (level > 9)
	return -2; /* planner will not even consider this level */

    if(dle->exclude_file) nb_exclude += dle->exclude_file->nb_element;
    if(dle->exclude_list) nb_exclude += dle->exclude_list->nb_element;
    if(dle->include_file) nb_include += dle->include_file->nb_element;
    if(dle->include_list) nb_include += dle->include_list->nb_element;

    if(nb_exclude > 0) file_exclude = build_exclude(dle, 0);
    if(nb_include > 0) file_include = build_include(dle, 0);

    gnutar_list_dir = getconf_str(CNF_GNUTAR_LIST_DIR);
    if (strlen(gnutar_list_dir) == 0)
	gnutar_list_dir = NULL;
    if (gnutar_list_dir) {
	char number[NUM_STR_SIZE];
	int baselevel;
	char *sdisk = sanitise_filename(dle->disk);

	basename = vstralloc(gnutar_list_dir,
			     "/",
			     g_options->hostname,
			     sdisk,
			     NULL);
	amfree(sdisk);

	g_snprintf(number, SIZEOF(number), "%d", level);
	incrname = vstralloc(basename, "_", number, ".new", NULL);
	unlink(incrname);

	/*
	 * Open the listed incremental file from the previous level.  Search
	 * backward until one is found.  If none are found (which will also
	 * be true for a level 0), arrange to read from /dev/null.
	 */
	baselevel = level;
	infd = -1;
	while (infd == -1) {
	    if (--baselevel >= 0) {
		g_snprintf(number, SIZEOF(number), "%d", baselevel);
		inputname = newvstralloc(inputname,
					 basename, "_", number, NULL);
	    } else {
		inputname = newstralloc(inputname, "/dev/null");
	    }
	    if ((infd = open(inputname, O_RDONLY)) == -1) {

		*errmsg = vstrallocf(_("gnutar: error opening %s: %s"),
				     inputname, strerror(errno));
		dbprintf("%s\n", *errmsg);
		if (baselevel < 0) {
		    goto common_exit;
		}
		amfree(*errmsg);
	    }
	}

	/*
	 * Copy the previous listed incremental file to the new one.
	 */
	if ((outfd = open(incrname, O_WRONLY|O_CREAT, 0600)) == -1) {
	    *errmsg = vstrallocf(_("opening %s: %s"),
			         incrname, strerror(errno));
	    dbprintf("%s\n", *errmsg);
	    goto common_exit;
	}

	while ((nb = read(infd, &buf, SIZEOF(buf))) > 0) {
	    if (full_write(outfd, &buf, (size_t)nb) < (size_t)nb) {
		*errmsg = vstrallocf(_("writing to %s: %s"),
				     incrname, strerror(errno));
		dbprintf("%s\n", *errmsg);
		goto common_exit;
	    }
	}

	if (nb < 0) {
	    *errmsg = vstrallocf(_("reading from %s: %s"),
			         inputname, strerror(errno));
	    dbprintf("%s\n", *errmsg);
	    goto common_exit;
	}

	if (close(infd) != 0) {
	    *errmsg = vstrallocf(_("closing %s: %s"),
			         inputname, strerror(errno));
	    dbprintf("%s\n", *errmsg);
	    goto common_exit;
	}
	if (close(outfd) != 0) {
	    *errmsg = vstrallocf(_("closing %s: %s"),
			         incrname, strerror(errno));
	    dbprintf("%s\n", *errmsg);
	    goto common_exit;
	}

	amfree(inputname);
	amfree(basename);
    }

    gmtm = gmtime(&dumpsince);
    g_snprintf(dumptimestr, SIZEOF(dumptimestr),
		"%04d-%02d-%02d %2d:%02d:%02d GMT",
		gmtm->tm_year + 1900, gmtm->tm_mon+1, gmtm->tm_mday,
		gmtm->tm_hour, gmtm->tm_min, gmtm->tm_sec);

    dirname = amname_to_dirname(dle->device);

    cmd = vstralloc(amlibexecdir, "/", "runtar", NULL);
    g_ptr_array_add(argv_ptr, stralloc("runtar"));
    if (g_options->config)
	g_ptr_array_add(argv_ptr, stralloc(g_options->config));
    else
	g_ptr_array_add(argv_ptr, stralloc("NOCONFIG"));

#ifdef GNUTAR
    g_ptr_array_add(argv_ptr, stralloc(GNUTAR));
#else
    g_ptr_array_add(argv_ptr, stralloc("tar"));
#endif
    g_ptr_array_add(argv_ptr, stralloc("--create"));
    g_ptr_array_add(argv_ptr, stralloc("--file"));
    g_ptr_array_add(argv_ptr, stralloc("/dev/null"));
    /* use --numeric-owner for estimates, to reduce the number of user/group
     * lookups required */
    g_ptr_array_add(argv_ptr, stralloc("--numeric-owner"));
    g_ptr_array_add(argv_ptr, stralloc("--directory"));
    canonicalize_pathname(dirname, tmppath);
    g_ptr_array_add(argv_ptr, stralloc(tmppath));
    g_ptr_array_add(argv_ptr, stralloc("--one-file-system"));
    if (gnutar_list_dir) {
	g_ptr_array_add(argv_ptr, stralloc("--listed-incremental"));
	g_ptr_array_add(argv_ptr, stralloc(incrname));
    } else {
	g_ptr_array_add(argv_ptr, stralloc("--incremental"));
	g_ptr_array_add(argv_ptr, stralloc("--newer"));
	g_ptr_array_add(argv_ptr, stralloc(dumptimestr));
    }
#ifdef ENABLE_GNUTAR_ATIME_PRESERVE
    /* --atime-preserve causes gnutar to call
     * utime() after reading files in order to
     * adjust their atime.  However, utime()
     * updates the file's ctime, so incremental
     * dumps will think the file has changed. */
    g_ptr_array_add(argv_ptr, stralloc("--atime-preserve"));
#endif
    g_ptr_array_add(argv_ptr, stralloc("--sparse"));
    g_ptr_array_add(argv_ptr, stralloc("--ignore-failed-read"));
    g_ptr_array_add(argv_ptr, stralloc("--totals"));

    if(file_exclude) {
	g_ptr_array_add(argv_ptr, stralloc("--exclude-from"));
	g_ptr_array_add(argv_ptr, stralloc(file_exclude));
    }

    if(file_include) {
	g_ptr_array_add(argv_ptr, stralloc("--files-from"));
	g_ptr_array_add(argv_ptr, stralloc(file_include));
    }
    else {
	g_ptr_array_add(argv_ptr, stralloc("."));
    }
    g_ptr_array_add(argv_ptr, NULL);

    start_time = curclock();

    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	*errmsg = vstrallocf(_("Cannot access /dev/null : %s"),
			     strerror(errno));
	dbprintf("%s\n", *errmsg);
	goto common_exit;
    }

    command = (char *)g_ptr_array_index(argv_ptr, 0);
    dumppid = pipespawnv(cmd, STDERR_PIPE, 0,
			 &nullfd, &nullfd, &pipefd, (char **)argv_ptr->pdata);

    dumpout = fdopen(pipefd,"r");
    if (!dumpout) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	dbprintf("%s\n", line);
	size = handle_dumpline(line);
	if(size > (off_t)-1) {
	    amfree(line);
	    while ((line = agets(dumpout)) != NULL) {
		if (line[0] != '\0') {
		    break;
		}
		amfree(line);
	    }
	    if (line != NULL) {
		dbprintf("%s\n", line);
		break;
	    }
	    break;
	}
    }
    amfree(line);

    dbprintf(".....\n");
    dbprintf(_("estimate time for %s level %d: %s\n"),
	      qdisk,
	      level,
	      walltime_str(timessub(curclock(), start_time)));
    if(size == (off_t)-1) {
	*errmsg = vstrallocf(_("no size line match in %s output"),
			     command);
	dbprintf(_("%s for %s\n"), *errmsg, qdisk);
	dbprintf(".....\n");
    } else if(size == (off_t)0 && level == 0) {
	dbprintf(_("possible %s problem -- is \"%s\" really empty?\n"),
		  command, dle->disk);
	dbprintf(".....\n");
    }
    dbprintf(_("estimate size for %s level %d: %lld KB\n"),
	      qdisk,
	      level,
	      (long long)size);

    kill(-dumppid, SIGTERM);

    dbprintf(_("waiting for %s \"%s\" child\n"),
	     command, qdisk);
    waitpid(dumppid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	*errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
			     cmd, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    *errmsg = vstrallocf(_("%s exited with status %d: see %s"),
			         cmd, WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	*errmsg = vstrallocf(_("%s got bad exit: see %s"),
			     cmd, dbfn());
    }
    dbprintf(_("after %s %s wait\n"), command, qdisk);

common_exit:

    if (incrname) {
	unlink(incrname);
    }
    amfree(incrname);
    amfree(basename);
    amfree(dirname);
    amfree(inputname);
    g_ptr_array_free_full(argv_ptr);
    amfree(qdisk);
    amfree(cmd);
    amfree(file_exclude);
    amfree(file_include);

    aclose(nullfd);
    afclose(dumpout);
    afclose(in);
    afclose(out);

    return size;
}
#endif

off_t
getsize_application_api(
    disk_estimates_t         *est,
    int		              nb_level,
    int		             *levels,
    backup_support_option_t  *bsu)
{
    dle_t *dle = est->dle;
    int pipeinfd[2], pipeoutfd[2], pipeerrfd[2];
    pid_t dumppid;
    off_t size = (off_t)-1;
    FILE *dumpout;
    FILE *dumperr;
    char *line = NULL;
    char *cmd = NULL;
    char *cmdline;
    guint i;
    int   j;
    GPtrArray *argv_ptr = g_ptr_array_new();
    char *newoptstr = NULL;
    off_t size1, size2;
    times_t start_time;
    char *qdisk = quote_string(dle->disk);
    char *qamdevice = quote_string(dle->device);
    amwait_t wait_status;
    char levelstr[NUM_STR_SIZE];
    GSList   *scriptlist;
    script_t *script;
    char     *errmsg = NULL;
    estimate_t     estimate;
    estimatelist_t el;

    cmd = vstralloc(APPLICATION_DIR, "/", dle->program, NULL);

    g_ptr_array_add(argv_ptr, stralloc(dle->program));
    g_ptr_array_add(argv_ptr, stralloc("estimate"));
    if (bsu->message_line == 1) {
	g_ptr_array_add(argv_ptr, stralloc("--message"));
	g_ptr_array_add(argv_ptr, stralloc("line"));
    }
    if (g_options->config && bsu->config == 1) {
	g_ptr_array_add(argv_ptr, stralloc("--config"));
	g_ptr_array_add(argv_ptr, stralloc(g_options->config));
    }
    if (g_options->hostname && bsu->host == 1) {
	g_ptr_array_add(argv_ptr, stralloc("--host"));
	g_ptr_array_add(argv_ptr, stralloc(g_options->hostname));
    }
    g_ptr_array_add(argv_ptr, stralloc("--device"));
    g_ptr_array_add(argv_ptr, stralloc(dle->device));
    if (dle->disk && bsu->disk == 1) {
	g_ptr_array_add(argv_ptr, stralloc("--disk"));
	g_ptr_array_add(argv_ptr, stralloc(dle->disk));
    }
    for (j=0; j < nb_level; j++) {
	g_ptr_array_add(argv_ptr, stralloc("--level"));
	g_snprintf(levelstr,SIZEOF(levelstr),"%d", levels[j]);
	g_ptr_array_add(argv_ptr, stralloc(levelstr));
    }
    /* find the first in ES_CLIENT and ES_CALCSIZE */
    estimate = ES_CLIENT;
    for (el = dle->estimatelist; el != NULL; el = el->next) {
	estimate = (estimate_t)GPOINTER_TO_INT(el->data);
	if ((estimate == ES_CLIENT && bsu->client_estimate) ||
	    (estimate == ES_CALCSIZE && bsu->calcsize))
	    break;
	estimate = ES_CLIENT;
    }
    if (estimate == ES_CALCSIZE && bsu->calcsize) {
	g_ptr_array_add(argv_ptr, stralloc("--calcsize"));
    }

    application_property_add_to_argv(argv_ptr, dle, bsu, g_options->features);

    for (scriptlist = dle->scriptlist; scriptlist != NULL;
	 scriptlist = scriptlist->next) {
	script = (script_t *)scriptlist->data;
	if (script->result && script->result->proplist) {
	    property_add_to_argv(argv_ptr, script->result->proplist);
	}
    }

    g_ptr_array_add(argv_ptr, NULL);

    cmdline = stralloc(cmd);
    for(i = 1; i < argv_ptr->len-1; i++)
	cmdline = vstrextend(&cmdline, " ",
			     (char *)g_ptr_array_index(argv_ptr, i), NULL);
    dbprintf("running: \"%s\"\n", cmdline);
    amfree(cmdline);

    if (pipe(pipeerrfd) < 0) {
	errmsg = vstrallocf(_("getsize_application_api could not create data pipes: %s"),
			    strerror(errno));
	goto common_exit;
    }

    if (pipe(pipeinfd) < 0) {
	errmsg = vstrallocf(_("getsize_application_api could not create data pipes: %s"),
			    strerror(errno));
	goto common_exit;
    }

    if (pipe(pipeoutfd) < 0) {
	errmsg = vstrallocf(_("getsize_application_api could not create data pipes: %s"),
			    strerror(errno));
	goto common_exit;
    }

    start_time = curclock();

    switch(dumppid = fork()) {
    case -1:
      size = (off_t)-1;
      goto common_exit;
    default:
      break; /* parent */
    case 0:
      dup2(pipeinfd[0], 0);
      dup2(pipeoutfd[1], 1);
      dup2(pipeerrfd[1], 2);
      aclose(pipeinfd[1]);
      aclose(pipeoutfd[0]);
      aclose(pipeerrfd[0]);
      safe_fd(-1, 0);

      execve(cmd, (char **)argv_ptr->pdata, safe_env());
      error(_("exec %s failed: %s"), cmd, strerror(errno));
      /*NOTREACHED*/
    }
    amfree(newoptstr);

    aclose(pipeinfd[0]);
    aclose(pipeoutfd[1]);
    aclose(pipeerrfd[1]);
    aclose(pipeinfd[1]);

    dumpout = fdopen(pipeoutfd[0],"r");
    if (!dumpout) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	long long size1_ = (long long)0;
	long long size2_ = (long long)0;
	int  level = 0;
	if (line[0] == '\0')
	    continue;
	dbprintf("%s\n", line);
	if (strncmp(line,"ERROR ", 6) == 0) {
	    char *errmsg, *qerrmsg;

	    errmsg = stralloc(line+6);
	    qerrmsg = quote_string(errmsg);
	    dbprintf(_("errmsg is %s\n"), errmsg);
	    g_printf(_("%s %d ERROR %s\n"), est->qamname, levels[0], qerrmsg);
	    amfree(qerrmsg);
	    continue;
	}
	i = sscanf(line, "%d %lld %lld", &level, &size1_, &size2_);
	if (i != 3) {
	    i = sscanf(line, "%lld %lld", &size1_, &size2_);
	    level = levels[0];
	    if (i != 2) {
		char *errmsg, *qerrmsg;

		errmsg = vstrallocf(_("bad line %s"), line);
		qerrmsg = quote_string(errmsg);
		dbprintf(_("errmsg is %s\n"), errmsg);
		g_printf(_("%s %d ERROR %s\n"), est->qamname, levels[0], qerrmsg);
		amfree(qerrmsg);
		continue;
	    }
	}
	size1 = (off_t)size1_;
	size2 = (off_t)size2_;
	if (size1 <= 0 || size2 <=0)
	    size = -1;
	else if (size1 * size2 > 0)
	    size = size1 * size2;
	dbprintf(_("estimate size for %s level %d: %lld KB\n"),
		 qamdevice,
		 level,
		 (long long)size);
	g_printf("%s %d SIZE %lld\n", est->qamname, level, (long long)size);
    }
    amfree(line);

    dumperr = fdopen(pipeerrfd[0],"r");
    if (!dumperr) {
	error(_("Can't fdopen: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    while ((line = agets(dumperr)) != NULL) {
	    if (strlen(line) > 0) {
	    char *err =  g_strdup_printf(_("Application '%s': %s"),
					 dle->program, line);
	    char *qerr = quote_string(err);
	    for (j=0; j < nb_level; j++) {
		fprintf(stdout, "%s %d ERROR %s\n",
			est->qamname, levels[j], qerr);
	    }
	    dbprintf("ERROR %s", qerr);
	    amfree(err);
	    amfree(qerr);
	}
	amfree(line);
    }

    dbprintf(".....\n");
    if (nb_level == 1) {
	dbprintf(_("estimate time for %s level %d: %s\n"), qamdevice,
		 levels[0], walltime_str(timessub(curclock(), start_time)));
    } else {
	dbprintf(_("estimate time for %s all level: %s\n"), qamdevice,
		 walltime_str(timessub(curclock(), start_time)));
    }

    kill(-dumppid, SIGTERM);

    dbprintf(_("waiting for %s \"%s\" child\n"), cmd, qdisk);
    waitpid(dumppid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
			    cmd, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    errmsg = vstrallocf(_("%s exited with status %d: see %s"), cmd,
				WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = vstrallocf(_("%s got bad exit: see %s"),
			    cmd, dbfn());
    }
    dbprintf(_("after %s %s wait\n"), cmd, qdisk);

    afclose(dumpout);
    afclose(dumperr);

common_exit:

    amfree(cmd);
    g_ptr_array_free_full(argv_ptr);
    amfree(newoptstr);
    amfree(qdisk);
    amfree(qamdevice);
    if (errmsg) {
	char *qerrmsg = quote_string(errmsg);
	dbprintf(_("errmsg is %s\n"), errmsg);
	for (j=0; j < nb_level; j++) {
	    g_printf(_("%s %d ERROR %s\n"), est->qamname, levels[j], qerrmsg);
	}
	amfree(errmsg);
	amfree(qerrmsg);
    }
    return size;
}


/*
 * Returns the value of the first integer in a string.
 */

double
first_num(
    char *	str)
{
    char *start;
    int ch;
    double d;

    ch = *str++;
    while(ch && !isdigit(ch)) ch = *str++;
    start = str-1;
    while(isdigit(ch) || (ch == '.')) ch = *str++;
    str[-1] = '\0';
    d = atof(start);
    str[-1] = (char)ch;
    return d;
}


/*
 * Checks the dump output line against the error and size regex tables.
 */

off_t
handle_dumpline(
    char *	str)
{
    regex_scale_t *rp;
    double size;

    /* check for size match */
    /*@ignore@*/
    for(rp = re_size; rp->regex != NULL; rp++) {
	if(match(rp->regex, str)) {
	    size = ((first_num(str)*rp->scale+1023.0)/1024.0);
	    if(size < 0.0)
		size = 1.0;		/* found on NeXT -- sigh */
	    return (off_t)size;
	}
    }
    /*@end@*/
    return (off_t)-1;
}
