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
 * $Id: sendsize.c,v 1.167 2006/07/25 18:27:56 martinea Exp $
 *
 * send estimated backup sizes using dump
 */

#include "amanda.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "amandates.h"
#include "clock.h"
#include "util.h"
#include "getfsent.h"
#include "version.h"
#include "client_util.h"
#include "clientconf.h"
#include "amandad.h"

#ifdef SAMBA_CLIENT
#include "findpass.h"
#endif

#ifdef HAVE_SETPGID
#  define SETPGRP	setpgid(getpid(), getpid())
#  define SETPGRP_FAILED() do {						\
    dbprintf(("setpgid(%ld,%ld) failed: %s\n",				\
	      (long)getpid(), (long)getpid(), strerror(errno)));	\
} while(0)

#else /* () line 0 */
#if defined(SETPGRP_VOID)
#  define SETPGRP	setpgrp()
#  define SETPGRP_FAILED() do {						\
    dbprintf(("setpgrp() failed: %s\n", strerror(errno)));		\
} while(0)

#else
#  define SETPGRP	setpgrp(0, getpid())
#  define SETPGRP_FAILED() do {						\
    dbprintf(("setpgrp(0,%ld) failed: %s\n",				\
	      (long)getpid(), strerror(errno)));			\
} while(0)

#endif
#endif

typedef struct level_estimates_s {
    time_t dumpsince;
    int estsize;
    int needestimate;
} level_estimate_t;

typedef struct disk_estimates_s {
    struct disk_estimates_s *next;
    char *amname;
    char *qamname;
    char *amdevice;
    char *qamdevice;
    char *dirname;
    char *qdirname;
    char *program;
    char *calcprog;
    int program_is_wrapper;
    int spindle;
    pid_t child;
    int done;
    option_t *options;
    level_estimate_t est[DUMP_LEVELS];
} disk_estimates_t;

disk_estimates_t *est_list;

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static g_option_t *g_options = NULL;

/* local functions */
int main(int argc, char **argv);
void add_diskest(char *disk, char *amdevice, int level, int spindle, 
		    int program_is_wrapper, char *prog, char *calcprog,
		    option_t *options);
void calc_estimates(disk_estimates_t *est);
void free_estimates(disk_estimates_t *est);
void dump_calc_estimates(disk_estimates_t *);
void star_calc_estimates(disk_estimates_t *);
void smbtar_calc_estimates(disk_estimates_t *);
void gnutar_calc_estimates(disk_estimates_t *);
void wrapper_calc_estimates(disk_estimates_t *);
void generic_calc_estimates(disk_estimates_t *);


int
main(
    int		argc,
    char **	argv)
{
    int level, spindle;
    char *prog, *calcprog, *dumpdate;
    option_t *options = NULL;
    int program_is_wrapper;
    disk_estimates_t *est;
    disk_estimates_t *est1;
    disk_estimates_t *est_prev;
    char *line = NULL;
    char *s, *fp;
    int ch;
    char *err_extra = NULL;
    int done;
    int need_wait;
    int dumpsrunning;
    char *disk = NULL;
    char *qdisk = NULL;
    char *qlist = NULL;
    char *amdevice = NULL;
    char *qamdevice = NULL;
    char *conffile;
#if defined(USE_DBMALLOC)
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
#endif

    (void)argc;	/* Quiet unused parameter warning */
    (void)argv;	/* Quiet unused parameter warning */

    /* initialize */

    safe_fd(-1, 0);
    safe_cd();

    set_pname("sendsize");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

#if defined(USE_DBMALLOC)
    malloc_size_1 = malloc_inuse(&malloc_hist_1);
#endif

    erroutput_type = (ERR_INTERACTIVE|ERR_SYSLOG);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();
    dbprintf(("%s: version %s\n", get_pname(), version()));

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    set_debug_prefix_pid(getpid());

    conffile = vstralloc(CONFIG_DIR, "/", "amanda-client.conf", NULL);
    if (read_clientconf(conffile) > 0) {
	error("error reading conffile: %s", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    /* handle all service requests */

    if(!start_amandates(0))
        error("error [opening %s: %s]", AMANDATES_FILE, strerror(errno));

    for(; (line = agets(stdin)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
#define sc "OPTIONS "
	if(strncmp(line, sc, SIZEOF(sc)-1) == 0) {
#undef sc
	    g_options = parse_g_options(line+8, 1);
	    if(!g_options->hostname) {
		g_options->hostname = alloc(MAX_HOSTNAME_LENGTH+1);
		gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
	    }

	    printf("OPTIONS ");
	    if(am_has_feature(g_options->features, fe_rep_options_features)) {
		printf("features=%s;", our_feature_string);
	    }
	    if(am_has_feature(g_options->features, fe_rep_options_maxdumps)) {
		printf("maxdumps=%d;", g_options->maxdumps);
	    }
	    if(am_has_feature(g_options->features, fe_rep_options_hostname)) {
		printf("hostname=%s;", g_options->hostname);
	    }
	    printf("\n");
	    fflush(stdout);

	    if (g_options->config) {
		conffile = vstralloc(CONFIG_DIR, "/", g_options->config, "/",
				     "amanda-client.conf", NULL);
		if (read_clientconf(conffile) > 0) {
		    error("error reading conffile: %s", conffile);
		    /*NOTREACHED*/
		}
		amfree(conffile);

		dbrename(g_options->config, DBG_SUBDIR_CLIENT);
	    }

	    continue;
	}

	s = line;
	ch = *s++;

	skip_whitespace(s, ch);			/* find the program name */
	if(ch == '\0') {
	    err_extra = stralloc("no program name");
	    goto err;				/* no program name */
	}
	prog = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';

	program_is_wrapper=0;
	if(strcmp(prog,"DUMPER")==0) {
	    program_is_wrapper=1;
	    skip_whitespace(s, ch);		/* find dumper name */
	    if (ch == '\0') {
		goto err;			/* no program */
	    }
	    prog = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	}

	if(strncmp(prog, "CALCSIZE", 8) == 0) {
	    skip_whitespace(s, ch);		/* find the program name */
	    if(ch == '\0') {
		err_extra = stralloc("no program name");
		goto err;
	    }
	    calcprog = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	}
	else {
	    calcprog = NULL;
	}

	skip_whitespace(s, ch);			/* find the disk name */
	if(ch == '\0') {
	    err_extra = stralloc("no disk name");
	    goto err;				/* no disk name */
	}

	if (qdisk != NULL)
	    amfree(qdisk);
	if (disk != NULL)
	    amfree(disk);

	fp = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	qdisk = stralloc(fp);
	disk = unquote_string(qdisk);

	skip_whitespace(s, ch);			/* find the device or level */
	if (ch == '\0') {
	    err_extra = stralloc("bad level");
	    goto err;
	}
	if(!isdigit((int)s[-1])) {
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';
	    qamdevice = stralloc(fp);
	    amdevice = unquote_string(qamdevice);
	    skip_whitespace(s, ch);		/* find level number */
	}
	else {
	    amdevice = stralloc(disk);
	    qamdevice = stralloc(qdisk);
	}

						/* find the level number */
	if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    err_extra = stralloc("bad level");
	    goto err;				/* bad level */
	}
	if (level < 0 || level >= DUMP_LEVELS) {
	    err_extra = stralloc("bad level");
	    goto err;
	}
	skip_integer(s, ch);

	skip_whitespace(s, ch);			/* find the dump date */
	if(ch == '\0') {
	    err_extra = stralloc("no dumpdate");
	    goto err;				/* no dumpdate */
	}
	dumpdate = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	(void)dumpdate;				/* XXX: Set but not used */

	spindle = 0;				/* default spindle */

	skip_whitespace(s, ch);			/* find the spindle */
	if(ch != '\0') {
	    if(sscanf(s - 1, "%d", &spindle) != 1) {
		err_extra = stralloc("bad spindle");
		goto err;			/* bad spindle */
	    }
	    skip_integer(s, ch);

	    skip_whitespace(s, ch);		/* find the parameters */
	    if(ch != '\0') {
		if(strncmp(s-1, "OPTIONS |;",10) == 0) {
		    options = parse_options(s + 8,
					    disk,
					    amdevice,
					    g_options->features,
					    0);
		}
		else {
		    options = alloc(SIZEOF(option_t));
		    init_options(options);
		    while (ch != '\0') {
			if(strncmp(s-1, "exclude-file=", 13) == 0) {
			    qlist = unquote_string(s+12);
			    options->exclude_file =
				append_sl(options->exclude_file, qlist);
			    amfree(qlist);
			} else if(strncmp(s-1, "exclude-list=", 13) == 0) {
			    options->exclude_list =
				append_sl(options->exclude_list, qlist);
			    qlist = unquote_string(s+12);
			    amfree(qlist);
			} else if(strncmp(s-1, "include-file=", 13) == 0) {
			    options->include_file =
				append_sl(options->include_file, qlist);
			    qlist = unquote_string(s+12);
			    amfree(qlist);
			} else if(strncmp(s-1, "include-list=", 13) == 0) {
			    options->include_list =
				append_sl(options->include_list, qlist);
			    qlist = unquote_string(s+12);
			    amfree(qlist);
			} else {
			    err_extra = vstralloc("Invalid parameter (",
				s-1, ")", NULL);
			    goto err;		/* should have gotten to end */
			}
			skip_quoted_string(s, ch);
			skip_whitespace(s, ch);	/* find the inclusion list */
			amfree(qlist);
		    }
		}
	    }
	    else {
		options = alloc(SIZEOF(option_t));
		init_options(options);
	    }
	}
	else {
	    options = alloc(SIZEOF(option_t));
	    init_options(options);
	}

	/*@ignore@*/
	add_diskest(disk, amdevice, level, spindle, program_is_wrapper, prog, calcprog, options);
	/*@end@*/
	amfree(disk);
	amfree(qdisk);
	amfree(amdevice);
	amfree(qamdevice);
    }
    if (g_options == NULL) {
	error("Missing OPTIONS line in sendsize input\n");
	/*NOTREACHED*/
    }
    amfree(line);

    finish_amandates();
    free_amandates();

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
	    int exit_code;

	    need_wait = 0;
	    dbprintf(("%s: waiting for any estimate child: %d running\n",
		      debug_prefix_time(NULL), dumpsrunning));
	    child_pid = wait(&child_status);
	    if(child_pid == -1) {
		error("wait failed: %s", strerror(errno));
		/*NOTREACHED*/
	    }
	    if(WIFSIGNALED(child_status)) {
		dbprintf(("%s: child %ld terminated with signal %d\n",
			  debug_prefix_time(NULL),
			  (long) child_pid, WTERMSIG(child_status)));
	    } else {
		exit_code = WEXITSTATUS(child_status);
		if(exit_code == 0) {
		    dbprintf(("%s: child %ld terminated normally\n",
			      debug_prefix_time(NULL), (long) child_pid));
		} else {
		    dbprintf(("%s: child %ld terminated with code %d\n",
			      debug_prefix_time(NULL),
			      (long) child_pid, exit_code));
		}
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
		dbprintf(("%s: unexpected child %ld\n",
			  debug_prefix_time(NULL), (long)child_pid));
	    } else {
		est->done = 1;
		est->child = 0;
		dumpsrunning--;
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
	    if(est->spindle != -1) {
		for(est1 = est_list; est1 != NULL; est1 = est1->next) {
		    if(est1->child == 0 || est == est1 || est1->done) {
			/*
			 * Ignore anything not yet started, ourself,
			 * and anything completed.
			 */
			continue;
		    }
		    if(est1->spindle == est->spindle) {
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
	    if((est->child = fork()) == 0) {
		set_debug_prefix_pid(getpid());
		calc_estimates(est);		/* child does the estimate */
		exit(0);
	    } else if(est->child == -1) {
		error("calc_estimates fork failed: %s", strerror(errno));
		/*NOTREACHED*/
	    }
	    dumpsrunning++;			/* parent */
	}
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
    am_release_feature_set(g_options->features);
    g_options->features = NULL;
    amfree(g_options->hostname);
    amfree(g_options->str);
    amfree(g_options);

#if defined(USE_DBMALLOC)
    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(dbfd(), malloc_hist_1, malloc_hist_2);
    }
#endif

    dbclose();
    return 0;
 err:
    printf("FORMAT ERROR IN REQUEST PACKET\n");
    dbprintf(("%s: REQ packet is bogus%s%s\n",
	      debug_prefix_time(NULL),
	      err_extra ? ": " : "",
	      err_extra ? err_extra : ""));
    amfree(err_extra);
    dbclose();
    return 1;
}


void
add_diskest(
    char *	disk,
    char *	amdevice,
    int		level,
    int		spindle,
    int		program_is_wrapper,
    char *	prog,
    char *	calcprog,
    option_t *	options)
{
    disk_estimates_t *newp, *curp;
    amandates_t *amdp;
    int dumplev, estlev;
    time_t dumpdate;

    if (level < 0)
	level = 0;
    if (level >= DUMP_LEVELS)
	level = DUMP_LEVELS - 1;

    for(curp = est_list; curp != NULL; curp = curp->next) {
	if(strcmp(curp->amname, disk) == 0) {
	    /* already have disk info, just note the level request */
	    curp->est[level].needestimate = 1;
	    if(options) {
		free_sl(options->exclude_file);
		free_sl(options->exclude_list);
		free_sl(options->include_file);
		free_sl(options->include_list);
		amfree(options->auth);
		amfree(options->str);
		amfree(options);
	    }
	    return;
	}
    }

    newp = (disk_estimates_t *) alloc(SIZEOF(disk_estimates_t));
    memset(newp, 0, SIZEOF(*newp));
    newp->next = est_list;
    est_list = newp;
    newp->amname = stralloc(disk);
    newp->qamname = quote_string(disk);
    newp->amdevice = stralloc(amdevice);
    newp->qamdevice = quote_string(amdevice);
    newp->dirname = amname_to_dirname(newp->amdevice);
    newp->qdirname = quote_string(newp->dirname);
    newp->program = stralloc(prog);
    if(calcprog != NULL)
	newp->calcprog = stralloc(calcprog);
    else
	newp->calcprog = NULL;
    newp->program_is_wrapper = program_is_wrapper;
    newp->spindle = spindle;
    newp->est[level].needestimate = 1;
    newp->options = options;

    /* fill in dump-since dates */

    amdp = amandates_lookup(newp->amname);

    newp->est[0].dumpsince = EPOCH;
    for(dumplev = 0; dumplev < DUMP_LEVELS; dumplev++) {
	dumpdate = amdp->dates[dumplev];
	for(estlev = dumplev+1; estlev < DUMP_LEVELS; estlev++) {
	    if(dumpdate > newp->est[estlev].dumpsince)
		newp->est[estlev].dumpsince = dumpdate;
	}
    }
}


void
free_estimates(
    disk_estimates_t *	est)
{
    amfree(est->amname);
    amfree(est->qamname);
    amfree(est->amdevice);
    amfree(est->qamdevice);
    amfree(est->dirname);
    amfree(est->qdirname);
    amfree(est->program);
    if(est->options) {
	free_sl(est->options->exclude_file);
	free_sl(est->options->exclude_list);
	free_sl(est->options->include_file);
	free_sl(est->options->include_list);
	amfree(est->options->str);
	amfree(est->options->auth);
	amfree(est->options);
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
    dbprintf(("%s: calculating for amname %s, dirname %s, spindle %d\n",
	      debug_prefix_time(NULL),
	      est->qamname, est->qdirname, est->spindle));
	
    if(est->program_is_wrapper ==  1)
	wrapper_calc_estimates(est);
    else
#ifndef USE_GENERIC_CALCSIZE
    if(strcmp(est->program, "DUMP") == 0)
	dump_calc_estimates(est);
    else
#endif
#ifdef SAMBA_CLIENT
      if (strcmp(est->program, "GNUTAR") == 0 &&
	  est->amdevice[0] == '/' && est->amdevice[1] == '/')
	smbtar_calc_estimates(est);
      else
#endif
#ifdef GNUTAR
	if (strcmp(est->program, "GNUTAR") == 0)
	  gnutar_calc_estimates(est);
	else
#endif
#ifdef SAMBA_CLIENT
	  if (est->amdevice[0] == '/' && est->amdevice[1] == '/')
	    dbprintf(("%s: Can't use CALCSIZE for samba estimate: %s %s\n",
		      debug_prefix_time(NULL),
		      est->qamname, est->qdirname));
	  else
#endif
	    generic_calc_estimates(est);

    dbprintf(("%s: done with amname %s dirname %s spindle %d\n",
	      debug_prefix_time(NULL),
	      est->qamname, est->qdirname, est->spindle));
}

/*
 * ------------------------------------------------------------------------
 *
 */

/* local functions */
off_t getsize_dump(char *disk, char *amdevice, int level, option_t *options);
off_t getsize_star(char *disk, char *amdevice, int level,
		       option_t *options, time_t dumpsince);
off_t getsize_smbtar(char *disk, char *amdevice, int level, option_t *options);
off_t getsize_gnutar(char *disk, char *amdevice, int level,
		       option_t *options, time_t dumpsince);
off_t getsize_wrapper(char *program, char *disk, char *amdevice, int level,
			option_t *options, time_t dumpsince);
off_t handle_dumpline(char *str);
double first_num(char *str);

void
wrapper_calc_estimates(
    disk_estimates_t *	est)
{
  int level;
  off_t size;

  for(level = 0; level < DUMP_LEVELS; level++) {
      if (est->est[level].needestimate) {
	  dbprintf(("%s: getting size via wrapper for %s level %d\n",
		    debug_prefix_time(NULL), est->qamname, level));
	  size = getsize_wrapper(est->program, est->amname, est->amdevice,
			level, est->options, est->est[level].dumpsince);

	  amflock(1, "size");

	  if (fseek(stdout, 0L, SEEK_END) < 0) {
	      dbprintf(("wrapper_calc_estimates: warning - seek failed: %s\n",
			strerror(errno)));
	  }

	  printf("%s %d SIZE " OFF_T_FMT "\n", est->qamname, level,
		 (OFF_T_FMT_TYPE)size);
	  fflush(stdout);

	  amfunlock(1, "size");
      }
  }
}


void
generic_calc_estimates(
    disk_estimates_t *	est)
{
    int pipefd = -1, nullfd = -1;
    char *cmd;
    char *my_argv[DUMP_LEVELS*2+22];
    char number[NUM_STR_SIZE];
    int i, level, my_argc, status;
    pid_t calcpid;
    int nb_exclude = 0;
    int nb_include = 0;
    char *file_exclude = NULL;
    char *file_include = NULL;
    times_t start_time;
    FILE *dumpout = NULL;
    off_t size = (off_t)1;
    char *line = NULL;
    char *match_expr;

    cmd = vstralloc(libexecdir, "/", "calcsize", versionsuffix(), NULL);

    my_argc = 0;

    my_argv[my_argc++] = stralloc("calcsize");
    if (g_options->config)
	my_argv[my_argc++] = stralloc(g_options->config);
    else
	my_argv[my_argc++] = stralloc("NOCONFIG");

    my_argv[my_argc++] = stralloc(est->calcprog);

    my_argv[my_argc++] = stralloc(est->amname);
    my_argv[my_argc++] = stralloc(est->dirname);


    if(est->options->exclude_file)
	nb_exclude += est->options->exclude_file->nb_element;
    if(est->options->exclude_list)
	nb_exclude += est->options->exclude_list->nb_element;
    if(est->options->include_file)
	nb_include += est->options->include_file->nb_element;
    if(est->options->include_list)
	nb_include += est->options->include_list->nb_element;

    if(nb_exclude > 0)
	file_exclude = build_exclude(est->amname,
		est->amdevice, est->options, 0);
    if(nb_include > 0)
	file_include = build_include(est->amname,
		est->amdevice, est->options, 0);

    if(file_exclude) {
	my_argv[my_argc++] = stralloc("-X");
	my_argv[my_argc++] = file_exclude;
    }

    if(file_include) {
	my_argv[my_argc++] = stralloc("-I");
	my_argv[my_argc++] = file_include;
    }
    start_time = curclock();

    dbprintf(("%s: running cmd: %s", debug_prefix_time(NULL), my_argv[0]));
    for(i=0; i<my_argc; ++i)
	dbprintf((" %s", my_argv[i]));

    for(level = 0; level < DUMP_LEVELS; level++) {
	if(est->est[level].needestimate) {
	    snprintf(number, SIZEOF(number), "%d", level);
	    my_argv[my_argc++] = stralloc(number); 
	    dbprintf((" %s", number));
	    snprintf(number, SIZEOF(number),
			"%ld", (long)est->est[level].dumpsince);
	    my_argv[my_argc++] = stralloc(number); 
	    dbprintf((" %s", number));
	}
    }
    my_argv[my_argc] = NULL;
    dbprintf(("\n"));

    fflush(stderr); fflush(stdout);

    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	dbprintf(("Cannot access /dev/null : %s\n", strerror(errno)));
	goto common_exit;
    }

    calcpid = pipespawnv(cmd, STDERR_PIPE, &nullfd, &nullfd, &pipefd, my_argv);
    amfree(cmd);

    dumpout = fdopen(pipefd,"r");
    if (!dumpout) {
	error("Can't fdopen: %s", strerror(errno));
	/*NOTREACHED*/
    }
    match_expr = vstralloc(est->qamname," %d SIZE " OFF_T_FMT, NULL);
    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	if(sscanf(line, match_expr, &level, &size) == 2) {
	    printf("%s\n", line); /* write to amandad */
	    dbprintf(("%s: estimate size for %s level %d: " OFF_T_FMT " KB\n",
		      debug_prefix(NULL),
		      est->qamname,
		      level,
		      size));
	}
    }
    amfree(match_expr);

    dbprintf(("%s: waiting for %s %s child (pid=%d)\n",
	      debug_prefix_time(NULL), my_argv[0], est->qamdevice, calcpid));
    wait(&status);
    dbprintf(("%s: after %s %s wait: child pid=%d status=%d\n",
	      debug_prefix_time(NULL), my_argv[0], est->qamdevice,
	      calcpid, WEXITSTATUS(status)));

    dbprintf(("%s: .....\n", debug_prefix_time(NULL)));
    dbprintf(("%s: estimate time for %s: %s\n",
	      debug_prefix(NULL),
	      est->qamname,
	      walltime_str(timessub(curclock(), start_time))));

common_exit:
    for(i = 0; i < my_argc; i++) {
	amfree(my_argv[i]);
    }
    amfree(cmd);
}


void
dump_calc_estimates(
    disk_estimates_t *	est)
{
    int level;
    off_t size;

    for(level = 0; level < DUMP_LEVELS; level++) {
	if(est->est[level].needestimate) {
	    dbprintf(("%s: getting size via dump for %s level %d\n",
		      debug_prefix_time(NULL), est->qamname, level));
	    size = getsize_dump(est->amname, est->amdevice,
			level, est->options);

	    amflock(1, "size");

	    if (fseek(stdout, 0L, SEEK_END) < 0) {
		dbprintf(("dump_calc_estimates: warning - seek failed: %s\n",
				strerror(errno)));
	    }

	    printf("%s %d SIZE " OFF_T_FMT "\n",
		   est->qamname, level, (OFF_T_FMT_TYPE)size);
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

    for(level = 0; level < DUMP_LEVELS; level++) {
	if(est->est[level].needestimate) {
	    dbprintf(("%s: getting size via smbclient for %s level %d\n",
		      debug_prefix_time(NULL), est->qamname, level));
	    size = getsize_smbtar(est->amname, est->amdevice, level, est->options);

	    amflock(1, "size");

	    if (fseek(stdout, 0L, SEEK_END) < 0) {
		dbprintf(("smbtar_calc_estimates: warning - seek failed: %s\n",
				strerror(errno)));
	    }

	    printf("%s %d SIZE " OFF_T_FMT "\n",
		   est->qamname, level, (OFF_T_FMT_TYPE)size);
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

  for(level = 0; level < DUMP_LEVELS; level++) {
      if (est->est[level].needestimate) {
	  dbprintf(("%s: getting size via gnutar for %s level %d\n",
		    debug_prefix_time(NULL), est->qamname, level));
	  size = getsize_gnutar(est->amname, est->amdevice, level,
				est->options, est->est[level].dumpsince);

	  amflock(1, "size");

	  if (fseek(stdout, 0L, SEEK_END) < 0) {
	      dbprintf(("gnutar_calc_estimates: warning - seek failed: %s\n",
				strerror(errno)));
	  }

	  printf("%s %d SIZE " OFF_T_FMT "\n",
		 est->qamname, level, (OFF_T_FMT_TYPE)size);
	  fflush(stdout);

	  amfunlock(1, "size");
      }
  }
}
#endif

typedef struct regex_s {
    char *regex;
    int scale;
} regex_t;

/*@ignore@*/
regex_t re_size[] = {
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
    char	*disk,
    char	*amdevice,
    int		level,
    option_t *	options)
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
    char *qdisk = quote_string(disk);
    char *qdevice;
    char *config;
    int is_rundump = 1;

    (void)options;	/* Quiet unused parameter warning */

    (void)getsize_smbtar;	/* Quiet unused parameter warning */

    snprintf(level_str, SIZEOF(level_str), "%d", level);

    device = amname_to_devname(amdevice);
    qdevice = quote_string(device);
    fstype = amname_to_fstype(amdevice);

    dbprintf(("%s: calculating for device %s with %s\n",
	      debug_prefix_time(NULL), qdevice, fstype));

    cmd = vstralloc(libexecdir, "/rundump", versionsuffix(), NULL);
    rundump_cmd = stralloc(cmd);
    if (g_options->config)
        config = g_options->config;
    else
        config = "NOCONFIG";
    if ((stdoutfd = nullfd = open("/dev/null", O_RDWR)) == -1) {
	dbprintf(("getsize_dump could not open /dev/null: %s\n",
		  strerror(errno)));
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
	dbprintf(("getsize_dump could create data pipes: %s\n",
		  strerror(errno)));
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
	dbprintf(("%s: running \"%s%s -F -J -l %s - %s\"\n",
		  debug_prefix_time(NULL), cmd, name, level_str, qdevice));
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
	dbprintf(("%s: running \"%s%s %s 1048576 - %s\"\n",
		  debug_prefix_time(NULL), cmd, name, dumpkeys, qdevice));
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
	amfree(device);
	amfree(qdevice);
	device = amname_to_dirname(amdevice);
	qdevice = quote_string(device);
	dumpkeys = vstralloc(level_str, "b", "f", NULL);
	dbprintf(("%s: running \"%s%s %s 60 - %s\"\n",
		  debug_prefix_time(NULL), cmd, name, dumpkeys, qdevice));
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
	dbprintf(("%s: running \"%s%s %s - %s\"\n",
		  debug_prefix_time(NULL), cmd, name, dumpkeys, qdevice));
# else							/* } { */
	dumpkeys = vstralloc(level_str,
#  ifdef HAVE_DUMP_ESTIMATE				/* { */
			     HAVE_DUMP_ESTIMATE,
#  endif						/* } */
#  ifdef HAVE_HONOR_NODUMP				/* { */
			     "h",
#  endif						/* } */
			     "s", "f", NULL);

#  ifdef HAVE_DUMP_ESTIMATE
	stdoutfd = pipefd[1];
#  endif

#  ifdef HAVE_HONOR_NODUMP				/* { */
	dbprintf(("%s: running \"%s%s %s 0 1048576 - %s\"\n",
		  debug_prefix_time(NULL), cmd, name, dumpkeys, qdevice));
#  else							/* } { */
	dbprintf(("%s: running \"%s%s %s 1048576 - %s\"\n",
		  debug_prefix_time(NULL), cmd, name, dumpkeys, qdevice));
#  endif						/* } */
# endif							/* } */
    }
    else
#endif							/* } */
    {
	error("no dump program available");
	/*NOTREACHED*/
    }

    if (pipe(killctl) < 0) {
	dbprintf(("%s: Could not create pipe: %s\n",
		debug_prefix(NULL), strerror(errno)));
	/* Message will be printed later... */
	killctl[0] = killctl[1] = -1;
    }

    start_time = curclock();
    switch(dumppid = fork()) {
    case -1:
	dbprintf(("%s: cannot fork for killpgrp: %s\n",
		  debug_prefix(NULL), strerror(errno)));
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
	    dbprintf(("%s: Trying without killpgrp\n", debug_prefix(NULL)));
	else {
	    switch(fork()) {
	    case -1:
		dbprintf(("%s: fork failed, trying without killpgrp\n",
			  debug_prefix(NULL)));
		break;

	    default:
	    {
		char *config;
		char *killpgrp_cmd = vstralloc(libexecdir, "/killpgrp",
					       versionsuffix(), NULL);
		dbprintf(("%s: running %s\n",
			  debug_prefix_time(NULL), killpgrp_cmd));
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
		execle(killpgrp_cmd, killpgrp_cmd, config, (char *)0,
		       safe_env());
		dbprintf(("%s: cannot execute %s: %s\n",
			  debug_prefix(NULL), killpgrp_cmd, strerror(errno)));
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
# endif
	    }
#endif
	{
	    error("exec %s failed or no dump program available: %s",
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
	error("Can't fdopen: %s", strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
	size = handle_dumpline(line);
	if(size > (off_t)-1) {
	    amfree(line);
	    while ((line = agets(dumpout)) != NULL) {
	        if (line[0] != '\0')
		    break;
		amfree(line);
	    }
	    if (line != NULL) {
		dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
	    }
	    break;
	}
    }
    amfree(line);

    dbprintf(("%s: .....\n", debug_prefix_time(NULL)));
    dbprintf(("%s: estimate time for %s level %d: %s\n",
	      debug_prefix(NULL),
	      qdisk,
	      level,
	      walltime_str(timessub(curclock(), start_time))));
    if(size == (off_t)-1) {
	dbprintf(("%s: no size line match in %s%s output for \"%s\"\n",
		  debug_prefix(NULL), cmd, name, disk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
	dbprintf(("%s: Run %s%s manually to check for errors\n",
		    debug_prefix(NULL), cmd, name));
    } else if(size == (off_t)0 && level == 0) {
	dbprintf(("%s: possible %s%s problem -- is \"%s\" really empty?\n",
		  debug_prefix(NULL), cmd, name, disk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
    } else {
	    dbprintf(("%s: estimate size for %s level %d: %ld KB\n",
	      debug_prefix(NULL),
	      qdisk,
	      level,
	      size));
    }

    if (killctl[1] != -1) {
	dbprintf(("%s: asking killpgrp to terminate\n",
		  debug_prefix_time(NULL)));
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
    dbprintf(("%s: sending SIGTERM to process group %ld\n",
	      debug_prefix_time(NULL), (long)dumppid));
    if (kill(-dumppid, SIGTERM) == -1) {
	dbprintf(("%s: kill failed: %s\n",
		  debug_prefix(NULL), strerror(errno)));
    }
    /* Now check whether it dies */
    for(s = 5; s > 0; --s) {
	sleep(1);
	if (waitpid(dumppid, NULL, WNOHANG) != -1)
	    goto terminated;
    }

    dbprintf(("%s: sending SIGKILL to process group %ld\n",
	      debug_prefix_time(NULL), (long)dumppid));
    if (kill(-dumppid, SIGKILL) == -1) {
	dbprintf(("%s: kill failed: %s\n",
		  debug_prefix(NULL), strerror(errno)));
    }
    for(s = 5; s > 0; --s) {
	sleep(1);
	if (waitpid(dumppid, NULL, WNOHANG) != -1)
	    goto terminated;
    }

    dbprintf(("%s: waiting for %s%s \"%s\" child\n",
	      debug_prefix_time(NULL), cmd, name, qdisk));
    wait(NULL);
    dbprintf(("%s: after %s%s %s wait\n",
	      debug_prefix_time(NULL), cmd, name, qdisk));

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
    char	*disk,
    char	*amdevice,
    int		level,
    option_t *	options)
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
    char *qdisk = quote_string(disk);

    (void)options;	/* Quiet unused parameter warning */

    error_pn = stralloc2(get_pname(), "-smbclient");

    parsesharename(amdevice, &share, &subdir);
    if (!share) {
	amfree(share);
	amfree(subdir);
	set_pname(error_pn);
	amfree(error_pn);
	error("cannot parse disk entry %s for share/subdir", qdisk);
	/*NOTREACHED*/
    }
    if ((subdir) && (SAMBA_VERSION < 2)) {
	amfree(share);
	amfree(subdir);
	set_pname(error_pn);
	amfree(error_pn);
	error("subdirectory specified for share %s but samba not v2 or better", qdisk);
	/*NOTREACHED*/
    }
    if ((user_and_password = findpass(share, &domain)) == NULL) {

	if(domain) {
	    memset(domain, '\0', strlen(domain));
	    amfree(domain);
	}
	set_pname(error_pn);
	amfree(error_pn);
	error("cannot find password for %s", disk);
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
	error("password field not \'user%%pass\' for %s", disk);
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
	error("cannot make share name of %s", share);
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
	error("could not open /dev/null: %s\n",
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
    dumppid = pipespawn(SAMBA_CLIENT, STDERR_PIPE|PASSWD_PIPE,
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
    if(pwtext_len > 0 && fullwrite(passwdfd, pwtext, (size_t)pwtext_len) < 0) {
	int save_errno = errno;

	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
	aclose(passwdfd);
	set_pname(error_pn);
	amfree(error_pn);
	error("password write failed: %s", strerror(save_errno));
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
	error("Can't fdopen: %s", strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
	size = handle_dumpline(line);
	if(size > -1) {
	    amfree(line);
	    while ((line = agets(dumpout)) != NULL) {
	        if (line[0] != '\0')
		    break;
		amfree(line);
	    }
	    if(line != NULL) {
		dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
	    }
	    break;
	}
    }
    amfree(line);

    dbprintf(("%s: .....\n", debug_prefix_time(NULL)));
    dbprintf(("%s: estimate time for %s level %d: %s\n",
	      debug_prefix(NULL),
	      qdisk,
	      level,
	      walltime_str(timessub(curclock(), start_time))));
    if(size == (off_t)-1) {
	dbprintf(("%s: no size line match in %s output for \"%s\"\n",
		  debug_prefix(NULL), SAMBA_CLIENT, disk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
    } else if(size == (off_t)0 && level == 0) {
	dbprintf(("%s: possible %s problem -- is \"%s\" really empty?\n",
		  debug_prefix(NULL), SAMBA_CLIENT, disk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
    }
    dbprintf(("%s: estimate size for %s level %d: %ld KB\n",
	      debug_prefix(NULL),
	      qdisk,
	      level,
	      size));

    kill(-dumppid, SIGTERM);

    dbprintf(("%s: waiting for %s \"%s\" child\n",
	      debug_prefix_time(NULL), SAMBA_CLIENT, qdisk));
    wait(NULL);
    dbprintf(("%s: after %s %s wait\n",
	      debug_prefix_time(NULL), SAMBA_CLIENT, qdisk));

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
    char	*disk,
    char	*amdevice,
    int		level,
    option_t *	options,
    time_t	dumpsince)
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
    char dumptimestr[80];
    struct tm *gmtm;
    int nb_exclude = 0;
    int nb_include = 0;
    char **my_argv;
    int i;
    char *file_exclude = NULL;
    char *file_include = NULL;
    times_t start_time;
    int infd, outfd;
    ssize_t nb;
    char buf[32768];
    char *qdisk = quote_string(disk);

    if(options->exclude_file) nb_exclude += options->exclude_file->nb_element;
    if(options->exclude_list) nb_exclude += options->exclude_list->nb_element;
    if(options->include_file) nb_include += options->include_file->nb_element;
    if(options->include_list) nb_include += options->include_list->nb_element;

    if(nb_exclude > 0) file_exclude = build_exclude(disk, amdevice, options, 0);
    if(nb_include > 0) file_include = build_include(disk, amdevice, options, 0);

    my_argv = alloc(SIZEOF(char *) * 22);
    i = 0;

#ifdef GNUTAR_LISTED_INCREMENTAL_DIR
    {
	char number[NUM_STR_SIZE];
	char *s;
	int ch;
	int baselevel;

	basename = vstralloc(GNUTAR_LISTED_INCREMENTAL_DIR,
			     "/",
			     g_options->hostname,
			     disk,
			     NULL);
	/*
	 * The loop starts at the first character of the host name,
	 * not the '/'.
	 */
	s = basename + SIZEOF(GNUTAR_LISTED_INCREMENTAL_DIR);
	while((ch = *s++) != '\0') {
	    if(ch == '/' || isspace(ch)) s[-1] = '_';
	}

	snprintf(number, SIZEOF(number), "%d", level);
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
		snprintf(number, SIZEOF(number), "%d", baselevel);
		inputname = newvstralloc(inputname,
					 basename, "_", number, NULL);
	    } else {
		inputname = newstralloc(inputname, "/dev/null");
	    }
	    if ((infd = open(inputname, O_RDONLY)) == -1) {
		int save_errno = errno;

		dbprintf(("%s: gnutar: error opening %s: %s\n",
			  debug_prefix(NULL), inputname, strerror(save_errno)));
		if (baselevel < 0) {
		    goto common_exit;
		}
	    }
	}

	/*
	 * Copy the previous listed incremental file to the new one.
	 */
	if ((outfd = open(incrname, O_WRONLY|O_CREAT, 0600)) == -1) {
	    dbprintf(("%s: opening %s: %s\n",
		      debug_prefix(NULL), incrname, strerror(errno)));
	    goto common_exit;
	}

	while ((nb = read(infd, &buf, SIZEOF(buf))) > 0) {
	    if (fullwrite(outfd, &buf, (size_t)nb) < nb) {
		dbprintf(("%s: writing to %s: %s\n",
			   debug_prefix(NULL), incrname, strerror(errno)));
		goto common_exit;
	    }
	}

	if (nb < 0) {
	    dbprintf(("%s: reading from %s: %s\n",
		      debug_prefix(NULL), inputname, strerror(errno)));
	    goto common_exit;
	}

	if (close(infd) != 0) {
	    dbprintf(("%s: closing %s: %s\n",
		      debug_prefix(NULL), inputname, strerror(errno)));
	    goto common_exit;
	}
	if (close(outfd) != 0) {
	    dbprintf(("%s: closing %s: %s\n",
		      debug_prefix(NULL), incrname, strerror(errno)));
	    goto common_exit;
	}

	amfree(inputname);
	amfree(basename);
    }
#endif

    gmtm = gmtime(&dumpsince);
    snprintf(dumptimestr, SIZEOF(dumptimestr),
		"%04d-%02d-%02d %2d:%02d:%02d GMT",
		gmtm->tm_year + 1900, gmtm->tm_mon+1, gmtm->tm_mday,
		gmtm->tm_hour, gmtm->tm_min, gmtm->tm_sec);

    dirname = amname_to_dirname(amdevice);


    cmd = vstralloc(libexecdir, "/", "runtar", versionsuffix(), NULL);

    my_argv[i++] = "runtar";
    if (g_options->config)
	my_argv[i++] = g_options->config;
    else
	my_argv[i++] = "NOCONFIG";

#ifdef GNUTAR
    my_argv[i++] = GNUTAR;
#else
    my_argv[i++] = "tar";
#endif
    my_argv[i++] = "--create";
    my_argv[i++] = "--file";
    my_argv[i++] = "/dev/null";
    my_argv[i++] = "--directory";
    my_argv[i++] = dirname;
    my_argv[i++] = "--one-file-system";
#ifdef GNUTAR_LISTED_INCREMENTAL_DIR
    my_argv[i++] = "--listed-incremental";
    my_argv[i++] = incrname;
#else
    my_argv[i++] = "--incremental";
    my_argv[i++] = "--newer";
    my_argv[i++] = dumptimestr;
#endif
#ifdef ENABLE_GNUTAR_ATIME_PRESERVE
    /* --atime-preserve causes gnutar to call
     * utime() after reading files in order to
     * adjust their atime.  However, utime()
     * updates the file's ctime, so incremental
     * dumps will think the file has changed. */
    my_argv[i++] = "--atime-preserve";
#endif
    my_argv[i++] = "--sparse";
    my_argv[i++] = "--ignore-failed-read";
    my_argv[i++] = "--totals";

    if(file_exclude) {
	my_argv[i++] = "--exclude-from";
	my_argv[i++] = file_exclude;
    }

    if(file_include) {
	my_argv[i++] = "--files-from";
	my_argv[i++] = file_include;
    }
    else {
	my_argv[i++] = ".";
    }
    my_argv[i++] = NULL;

    start_time = curclock();

    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	dbprintf(("Cannot access /dev/null : %s\n", strerror(errno)));
	goto common_exit;
    }

    dumppid = pipespawnv(cmd, STDERR_PIPE, &nullfd, &nullfd, &pipefd, my_argv);

    dumpout = fdopen(pipefd,"r");
    if (!dumpout) {
	error("Can't fdopen: %s", strerror(errno));
	/*NOTREACHED*/
    }
    if (!dumpout) {
	error("Can't fdopen: %s", strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
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
		dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
		break;
	    }
	    break;
	}
    }
    amfree(line);

    dbprintf(("%s: .....\n", debug_prefix_time(NULL)));
    dbprintf(("%s: estimate time for %s level %d: %s\n",
	      debug_prefix(NULL),
	      qdisk,
	      level,
	      walltime_str(timessub(curclock(), start_time))));
    if(size == (off_t)-1) {
	dbprintf(("%s: no size line match in %s output for \"%s\"\n",
		  debug_prefix(NULL), my_argv[0], disk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
    } else if(size == (off_t)0 && level == 0) {
	dbprintf(("%s: possible %s problem -- is \"%s\" really empty?\n",
		  debug_prefix(NULL), my_argv[0], disk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
    }
    dbprintf(("%s: estimate size for %s level %d: %ld KB\n",
	      debug_prefix(NULL),
	      qdisk,
	      level,
	      size));

    kill(-dumppid, SIGTERM);

    dbprintf(("%s: waiting for %s \"%s\" child\n",
	      debug_prefix_time(NULL), my_argv[0], qdisk));
    wait(NULL);
    dbprintf(("%s: after %s %s wait\n",
	      debug_prefix_time(NULL), my_argv[0], qdisk));

common_exit:

    if (incrname) {
	unlink(incrname);
    }
    amfree(incrname);
    amfree(basename);
    amfree(dirname);
    amfree(inputname);
    amfree(my_argv);
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
getsize_wrapper(
    char	*program,
    char	*disk,
    char	*amdevice,
    int		level,
    option_t *	options,
    time_t	dumpsince)
{
    int pipefd[2], nullfd;
    pid_t dumppid;
    off_t size = (off_t)-1;
    FILE *dumpout;
    char *line = NULL;
    char *cmd = NULL;
    char dumptimestr[80];
    struct tm *gmtm;
    int  i, j;
    char *argvchild[10];
    char *newoptstr = NULL;
    off_t size1, size2;
    times_t start_time;
    char *qdisk = quote_string(disk);
    char *qamdevice = quote_string(amdevice);

    gmtm = gmtime(&dumpsince);
    snprintf(dumptimestr, SIZEOF(dumptimestr),
		"%04d-%02d-%02d %2d:%02d:%02d GMT",
		gmtm->tm_year + 1900, gmtm->tm_mon+1, gmtm->tm_mday,
		gmtm->tm_hour, gmtm->tm_min, gmtm->tm_sec);

    cmd = vstralloc(DUMPER_DIR, "/", program, NULL);

    i=0;
    argvchild[i++] = program;
    argvchild[i++] = "estimate";
    if(level == 0)
	argvchild[i++] = "full";
    else {
	char levelstr[NUM_STR_SIZE];
	snprintf(levelstr,SIZEOF(levelstr),"%d",level);
	argvchild[i++] = "level";
	argvchild[i++] = levelstr;
    }
    argvchild[i++] = amdevice;
    newoptstr = vstralloc(options->str,"estimate-direct;", NULL);
    argvchild[i++] = newoptstr;
    argvchild[i] = NULL;

    dbprintf(("%s: running %s", debug_prefix_time(NULL), cmd));
    for(j = 1; j < i; j++) {
	dbprintf((" %s", argvchild[j]));
    }
    dbprintf(("\n"));

    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	dbprintf(("Cannot access /dev/null : %s\n", strerror(errno)));
	goto common_exit;
    }

    if (pipe(pipefd) < 0) {
	dbprintf(("getsize_wrapper could create data pipes: %s\n",
		  strerror(errno)));
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
      dup2(nullfd, 0);
      dup2(nullfd, 2);
      dup2(pipefd[1], 1);
      aclose(pipefd[0]);

      execve(cmd, argvchild, safe_env());
      error("exec %s failed: %s", cmd, strerror(errno));
      /*NOTREACHED*/
    }
    amfree(newoptstr);

    aclose(pipefd[1]);
    dumpout = fdopen(pipefd[0],"r");
    if (!dumpout) {
	error("Can't fdopen: %s", strerror(errno));
	/*NOTREACHED*/
    }

    for(size = (off_t)-1; (line = agets(dumpout)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
	i = sscanf(line, OFF_T_FMT " " OFF_T_FMT,
		(OFF_T_FMT_TYPE *)&size1, 
		(OFF_T_FMT_TYPE *)&size2);
	if(i == 2) {
	    size = size1 * size2;
	}
	if(size > -1) {
	    amfree(line);
	    while ((line = agets(dumpout)) != NULL) {
	        if (line[0] != '\0')
		    break;
		amfree(line);
	    }
	    if(line != NULL) {
		dbprintf(("%s: %s\n", debug_prefix_time(NULL), line));
	    }
	    break;
	}
    }
    amfree(line);

    dbprintf(("%s: .....\n", debug_prefix_time(NULL)));
    dbprintf(("%s: estimate time for %s level %d: %s\n",
	      debug_prefix(NULL),
	      qamdevice,
	      level,
	      walltime_str(timessub(curclock(), start_time))));
    if(size == (off_t)-1) {
	dbprintf(("%s: no size line match in %s output for \"%s\"\n",
		  debug_prefix(NULL), cmd, qdisk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
    } else if(size == (off_t)0 && level == 0) {
	dbprintf(("%s: possible %s problem -- is \"%s\" really empty?\n",
		  debug_prefix(NULL), cmd, qdisk));
	dbprintf(("%s: .....\n", debug_prefix(NULL)));
    }
    dbprintf(("%s: estimate size for %s level %d: " OFF_T_FMT " KB\n",
	      debug_prefix(NULL),
	      qamdevice,
	      level,
	      size));

    kill(-dumppid, SIGTERM);

    dbprintf(("%s: waiting for %s \"%s\" child\n",
	      debug_prefix_time(NULL), cmd, qdisk));
    wait(NULL);
    dbprintf(("%s: after %s %s wait\n",
	      debug_prefix_time(NULL), cmd, qdisk));

    aclose(nullfd);
    afclose(dumpout);

common_exit:

    amfree(cmd);
    amfree(newoptstr);
    amfree(qdisk);
    amfree(qamdevice);
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
    regex_t *rp;
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
