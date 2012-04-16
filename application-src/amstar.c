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
 * $Id: amstar.c 8888 2007-10-02 13:40:42Z martineau $
 *
 * send estimated backup sizes using dump
 */

/* PROPERTY:
 *
 * STAR-PATH (default STAR)
 * STAR-TARDUMP
 * STAR-DLE-TARDUMP
 * ONE-FILE-SYSTEM
 * SPARSE
 * NORMAL
 * IGNORE
 * STRANGE
 * INCLUDE-LIST		(for restore only)
 * EXCLUDE-FILE
 * EXCLUDE-LIST
 * DIRECTORY
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
#include "getopt.h"
#include "sendbackup.h"

int debug_application = 1;
#define application_debug(i, ...) do {	\
	if ((i) <= debug_application) {	\
	    dbprintf(__VA_ARGS__);	\
	}				\
} while (0)

static amregex_t init_re_table[] = {
  /* tar prints the size in bytes */
  AM_SIZE_RE("star: [0-9][0-9]* blocks", 10240, 1),
  AM_NORMAL_RE("^could not open conf file"),
  AM_NORMAL_RE("^Type of this level "),
  AM_NORMAL_RE("^Date of this level "),
  AM_NORMAL_RE("^Date of last level "),
  AM_NORMAL_RE("^Dump record  level "),
  AM_NORMAL_RE("^Throughput"),
  AM_NORMAL_RE("^.*is sparse$"),

#ifdef IGNORE_TAR_ERRORS
  AM_NORMAL_RE("^.*shrunk*$"),
  AM_NORMAL_RE("^.*changed size.*$"),
  AM_NORMAL_RE("^.*Cannot listxattr for.*$"),
  AM_NORMAL_RE("^.Cannot: stat .*$"),
  AM_NORMAL_RE("^.Missing links .*$"),
  AM_NORMAL_RE("^.Cannot get xattr.*$"),
  AM_NORMAL_RE("^.Cannot.*acl.*$"),
#endif

  AM_NORMAL_RE("^star: dumped [0-9][0-9]* (tar )?files"),
  AM_NORMAL_RE("^.*The following problems occurred during .* processing.*$"),
  AM_NORMAL_RE("^.*Processed all possible files, despite earlier errors.*$"),
  AM_NORMAL_RE("^.*not written due to problems during backup.*$"),

  AM_STRANGE_RE("^Perform a level 0 dump first.*$"),

  /* catch-all: DMP_STRANGE is returned for all other lines */
  AM_STRANGE_RE(NULL)
};
static amregex_t *re_table;

/* local functions */
int main(int argc, char **argv);

typedef struct application_argument_s {
    char      *config;
    char      *host;
    int        message;
    int        collection;
    int        calcsize;
    GSList    *level;
    GSList    *command_options;
    dle_t      dle;
    int        argc;
    char     **argv;
} application_argument_t;

enum { CMD_ESTIMATE, CMD_BACKUP };

static void amstar_support(application_argument_t *argument);
static void amstar_selfcheck(application_argument_t *argument);
static void amstar_estimate(application_argument_t *argument);
static void amstar_backup(application_argument_t *argument);
static void amstar_restore(application_argument_t *argument);
static void amstar_validate(application_argument_t *argument);
static GPtrArray *amstar_build_argv(application_argument_t *argument,
				int level,
				int command);
static int check_device(application_argument_t *argument);

static char *star_path;
static char *star_tardumps;
static int   star_dle_tardumps;
static int   star_onefilesystem;
static int   star_sparse;
static int   star_acl;
static char *star_directory;
static GSList *normal_message = NULL;
static GSList *ignore_message = NULL;
static GSList *strange_message = NULL;

static struct option long_options[] = {
    {"config"          , 1, NULL,  1},
    {"host"            , 1, NULL,  2},
    {"disk"            , 1, NULL,  3},
    {"device"          , 1, NULL,  4},
    {"level"           , 1, NULL,  5},
    {"index"           , 1, NULL,  6},
    {"message"         , 1, NULL,  7},
    {"collection"      , 0, NULL,  8},
    {"record"          , 0, NULL,  9},
    {"star-path"       , 1, NULL, 10},
    {"star-tardump"    , 1, NULL, 11},
    {"star-dle-tardump", 1, NULL, 12},
    {"one-file-system" , 1, NULL, 13},
    {"sparse"          , 1, NULL, 14},
    {"calcsize"        , 0, NULL, 15},
    {"normal"          , 1, NULL, 16},
    {"ignore"          , 1, NULL, 17},
    {"strange"         , 1, NULL, 18},
    {"include-list"    , 1, NULL, 19},
    {"exclude-list"    , 1, NULL, 20},
    {"directory"       , 1, NULL, 21},
    {"command-options" , 1, NULL, 22},
    {"exclude-file"    , 1, NULL, 23},
    {"acl"             , 1, NULL, 24},
    { NULL, 0, NULL, 0}
};


int
main(
    int		argc,
    char **	argv)
{
    int c;
    char *command;
    application_argument_t argument;

#ifdef STAR
    star_path = STAR;
#else
    star_path = NULL;
#endif
    star_tardumps = "/etc/tardumps";
    star_dle_tardumps = 0;
    star_onefilesystem = 1;
    star_sparse = 1;
    star_acl = 1;
    star_directory = NULL;

    /* initialize */

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda");

    if (argc < 2) {
	printf("ERROR no command given to amstar\n");
	error(_("No command given to amstar"));
    }

    /* drop root privileges */
    if (!set_root_privs(0)) {
	if (strcmp(argv[1], "selfcheck") == 0) {
	    printf("ERROR amstar must be run setuid root\n");
	}
	error(_("amstar must be run setuid root"));
    }

    safe_fd(3, 2);

    set_pname("amstar");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

#if defined(USE_DBMALLOC)
    malloc_size_1 = malloc_inuse(&malloc_hist_1);
#endif

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_syslog);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();
    dbprintf(_("version %s\n"), VERSION);

    config_init(CONFIG_INIT_CLIENT, NULL);

    //check_running_as(RUNNING_AS_DUMPUSER_PREFERRED);
    //root for amrecover
    //RUNNING_AS_CLIENT_LOGIN from selfcheck, sendsize, sendbackup

    /* parse argument */
    command = argv[1];

    argument.config     = NULL;
    argument.host       = NULL;
    argument.message    = 0;
    argument.collection = 0;
    argument.calcsize   = 0;
    argument.level      = NULL;
    argument.command_options = NULL;
    init_dle(&argument.dle);
    argument.dle.record = 0;

    opterr = 0;
    while (1) {
	int option_index = 0;
    	c = getopt_long (argc, argv, "", long_options, &option_index);
	if (c == -1)
	    break;

	switch (c) {
	case 1: argument.config = stralloc(optarg);
		break;
	case 2: argument.host = stralloc(optarg);
		break;
	case 3: argument.dle.disk = stralloc(optarg);
		break;
	case 4: argument.dle.device = stralloc(optarg);
		break;
	case 5: argument.level = g_slist_append(argument.level,
						GINT_TO_POINTER(atoi(optarg)));
		break;
	case 6: argument.dle.create_index = 1;
		break;
	case 7: argument.message = 1;
		break;
	case 8: argument.collection = 1;
		break;
	case 9: argument.dle.record = 1;
		break;
	case 10: star_path = stralloc(optarg);
		 break;
	case 11: star_tardumps = stralloc(optarg);
		 break;
	case 12: if (optarg && strcasecmp(optarg, "NO") == 0)
		     star_dle_tardumps = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     star_dle_tardumps = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad STAR-DLE-TARDUMP property value (%s)]\n"), get_pname(), optarg);
		 break;
	case 13: if (optarg && strcasecmp(optarg, "YES") != 0) {
		     /* This option is required to be YES */
		     /* star_onefilesystem = 0; */
		 }
		 break;
	case 14: if (optarg && strcasecmp(optarg, "NO") == 0)
		     star_sparse = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     star_sparse = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad SPARSE property value (%s)]\n"), get_pname(), optarg);
		 break;
	case 15: argument.calcsize = 1;
		 break;
        case 16: if (optarg)
                     normal_message =
                         g_slist_append(normal_message, optarg);
                 break;
        case 17: if (optarg)
                     ignore_message =
                         g_slist_append(ignore_message, optarg);
                 break;
        case 18: if (optarg)
                     strange_message =
                         g_slist_append(strange_message, optarg);
                 break;
	case 19: if (optarg)
		     argument.dle.include_list =
			 append_sl(argument.dle.include_list, optarg);
		 break;
	case 20: if (optarg)
		     argument.dle.exclude_list =
			 append_sl(argument.dle.exclude_list, optarg);
		 break;
	case 21: if (optarg)
		     star_directory = stralloc(optarg);
		 break;
	case 22: argument.command_options =
			g_slist_append(argument.command_options,
				       stralloc(optarg));
		 break;
	case 23: if (optarg)
		     argument.dle.exclude_file =
			 append_sl(argument.dle.exclude_file, optarg);
		 break;
	case 24: if (optarg && strcasecmp(optarg, "NO") == 0)
		     star_acl = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     star_acl = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad ACL property value (%s)]\n"), get_pname(), optarg);
		 break;
	case ':':
	case '?':
		break;
	}
    }

    if (!argument.dle.disk && argument.dle.device)
	argument.dle.disk = stralloc(argument.dle.device);
    if (!argument.dle.device && argument.dle.disk)
	argument.dle.device = stralloc(argument.dle.disk);

    argument.argc = argc - optind;
    argument.argv = argv + optind;

    if (argument.config) {
	/* overlay this configuration on the existing (nameless) configuration */
	config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
		    argument.config);
	dbrename(get_config_name(), DBG_SUBDIR_CLIENT);

    }

    if (config_errors(NULL) >= CFGERR_ERRORS) {
	g_critical(_("errors processing config file"));
    }

    re_table = build_re_table(init_re_table, normal_message, ignore_message,
			      strange_message);

    if (strcmp(command, "support") == 0) {
	amstar_support(&argument);
    } else if (strcmp(command, "selfcheck") == 0) {
	amstar_selfcheck(&argument);
    } else if (strcmp(command, "estimate") == 0) {
	amstar_estimate(&argument);
    } else if (strcmp(command, "backup") == 0) {
	amstar_backup(&argument);
    } else if (strcmp(command, "restore") == 0) {
	amstar_restore(&argument);
    } else if (strcmp(command, "validate") == 0) {
	amstar_validate(&argument);
    } else {
	fprintf(stderr, "Unknown command `%s'.\n", command);
	exit (1);
    }
    return 0;
}

static void
amstar_support(
    application_argument_t *argument)
{
    (void)argument;
    fprintf(stdout, "CONFIG YES\n");
    fprintf(stdout, "HOST YES\n");
    fprintf(stdout, "DISK YES\n");
    fprintf(stdout, "MAX-LEVEL 9\n");
    fprintf(stdout, "INDEX-LINE YES\n");
    fprintf(stdout, "INDEX-XML NO\n");
    fprintf(stdout, "MESSAGE-LINE YES\n");
    fprintf(stdout, "MESSAGE-XML NO\n");
    fprintf(stdout, "RECORD YES\n");
    fprintf(stdout, "INCLUDE-FILE NO\n");
    fprintf(stdout, "INCLUDE-LIST YES\n");
    fprintf(stdout, "EXCLUDE-FILE YES\n");
    fprintf(stdout, "EXCLUDE-LIST YES\n");
    fprintf(stdout, "COLLECTION NO\n");
    fprintf(stdout, "MULTI-ESTIMATE YES\n");
    fprintf(stdout, "CALCSIZE YES\n");
    fprintf(stdout, "CLIENT-ESTIMATE YES\n");
}

static void
amstar_selfcheck(
    application_argument_t *argument)
{
    if (argument->dle.disk) {
	char *qdisk = quote_string(argument->dle.disk);
	fprintf(stdout, "OK disk %s\n", qdisk);
	amfree(qdisk);
    }

    fprintf(stdout, "OK amstar version %s\n", VERSION);
    fprintf(stdout, "OK amstar\n");

    if (argument->dle.device) {
	char *qdevice = quote_string(argument->dle.device);
	fprintf(stdout, "OK %s\n", qdevice);
	amfree(qdevice);
    }
    if (star_directory) {
	char *qdirectory = quote_string(star_directory);
	fprintf(stdout, "OK %s\n", qdirectory);
	amfree(qdirectory);
    }

    if (argument->dle.include_list &&
	argument->dle.include_list->nb_element >= 0) {
	fprintf(stdout, "ERROR include-list not supported for backup\n");
    }

    if (!star_path) {
	fprintf(stdout, "ERROR STAR-PATH not defined\n");
    } else {
	if (check_file(star_path, X_OK)) {
	    char *star_version;
	    GPtrArray *argv_ptr = g_ptr_array_new();

	    g_ptr_array_add(argv_ptr, star_path);
	    g_ptr_array_add(argv_ptr, "--version");
	    g_ptr_array_add(argv_ptr, NULL);

	    star_version = get_first_line(argv_ptr);

	    if (star_version) {
		char *sv, *sv1;
		for (sv = star_version; *sv && !g_ascii_isdigit(*sv); sv++);
		for (sv1 = sv; *sv1 && *sv1 != ' '; sv1++);
		*sv1 = '\0';
		printf("OK amstar star-version %s\n", sv);
	    } else {
		printf(_("ERROR [Can't get %s version]\n"), star_path);
	    }
	    g_ptr_array_free(argv_ptr, TRUE);
	    amfree(star_version);

	}
    }

    if (argument->calcsize) {
	char *calcsize = vstralloc(amlibexecdir, "/", "calcsize", NULL);
	check_file(calcsize, X_OK);
	check_suid(calcsize);
	amfree(calcsize);
    }

    {
	char *amandates_file;
	amandates_file = getconf_str(CNF_AMANDATES);
	check_file(amandates_file, R_OK|W_OK);
    }

    set_root_privs(1);
    if (argument->dle.device) {
	check_dir(argument->dle.device, R_OK);
    }
    set_root_privs(0);
}

static void
amstar_estimate(
    application_argument_t *argument)
{
    GPtrArray  *argv_ptr;
    char       *cmd = NULL;
    int         nullfd;
    int         pipefd;
    FILE       *dumpout = NULL;
    off_t       size = -1;
    char        line[32768];
    char       *errmsg = NULL;
    char       *qerrmsg;
    char       *qdisk;
    amwait_t    wait_status;
    int         starpid;
    amregex_t  *rp;
    times_t     start_time;
    int         level = 0;
    GSList     *levels = NULL;

    if (!argument->level) {
	fprintf(stderr, "ERROR No level argument\n");
	error(_("No level argument"));
    }
    if (!argument->dle.disk) {
	fprintf(stderr, "ERROR No disk argument\n");
	error(_("No disk argument"));
    }
    if (!argument->dle.device) {
	fprintf(stderr, "ERROR No device argument\n");
	error(_("No device argument"));
    }

    if (argument->dle.include_list &&
	argument->dle.include_list->nb_element >= 0) {
	fprintf(stderr, "ERROR include-list not supported for backup\n");
    }

    if (check_device(argument) == 0) {
	return;
    }

    qdisk = quote_string(argument->dle.disk);
    if (argument->calcsize) {
	char *dirname;

	if (star_directory) {
	    dirname = star_directory;
	} else {
	    dirname = argument->dle.device;
	}
	run_calcsize(argument->config, "STAR", argument->dle.disk, dirname,
		     argument->level, NULL, NULL);
	return;
    }

    if (!star_path) {
	errmsg = vstrallocf(_("STAR-PATH not defined"));
	goto common_error;
    }
    cmd = stralloc(star_path);

    start_time = curclock();

    for (levels = argument->level; levels != NULL; levels = levels->next) {
	level = GPOINTER_TO_INT(levels->data);
	argv_ptr = amstar_build_argv(argument, level, CMD_ESTIMATE);

	if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	    errmsg = vstrallocf(_("Cannot access /dev/null : %s"),
				strerror(errno));
	    goto common_error;
	}

	starpid = pipespawnv(cmd, STDERR_PIPE, 1,
			     &nullfd, &nullfd, &pipefd,
			     (char **)argv_ptr->pdata);

	dumpout = fdopen(pipefd,"r");
	if (!dumpout) {
	    errmsg = vstrallocf(_("Can't fdopen: %s"), strerror(errno));
	    goto common_error;
	}

	size = (off_t)-1;
	while (size < 0 && (fgets(line, sizeof(line), dumpout)) != NULL) {
	    if (line[strlen(line)-1] == '\n') /* remove trailling \n */
		line[strlen(line)-1] = '\0';
	    if (line[0] == '\0')
		continue;
	    dbprintf("%s\n", line);
	    /* check for size match */
	    /*@ignore@*/
	    for(rp = re_table; rp->regex != NULL; rp++) {
		if(match(rp->regex, line)) {
		    if (rp->typ == DMP_SIZE) {
			size = ((the_num(line, rp->field)*rp->scale+1023.0)/1024.0);
			if(size < 0.0)
			    size = 1.0;             /* found on NeXT -- sigh */
		    }
		    break;
		}
	    }
	    /*@end@*/
	}

	while ((fgets(line, sizeof(line), dumpout)) != NULL) {
	    dbprintf("%s", line);
	}

	dbprintf(".....\n");
	dbprintf(_("estimate time for %s level %d: %s\n"),
		 qdisk,
		 level,
		 walltime_str(timessub(curclock(), start_time)));
	if(size == (off_t)-1) {
	    errmsg = vstrallocf(_("no size line match in %s output"),
				cmd);
	    dbprintf(_("%s for %s\n"), errmsg, qdisk);
	    dbprintf(".....\n");
	} else if(size == (off_t)0 && argument->level == 0) {
	    dbprintf(_("possible %s problem -- is \"%s\" really empty?\n"),
		     cmd, argument->dle.disk);
	    dbprintf(".....\n");
	}
	dbprintf(_("estimate size for %s level %d: %lld KB\n"),
		 qdisk,
		 level,
		 (long long)size);

	kill(-starpid, SIGTERM);

	dbprintf(_("waiting for %s \"%s\" child\n"), cmd, qdisk);
	waitpid(starpid, &wait_status, 0);
	if (WIFSIGNALED(wait_status)) {
	    errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
				cmd, WTERMSIG(wait_status), dbfn());
	} else if (WIFEXITED(wait_status)) {
	    if (WEXITSTATUS(wait_status) != 0) {
		errmsg = vstrallocf(_("%s exited with status %d: see %s"),
				    cmd, WEXITSTATUS(wait_status), dbfn());
	    } else {
		/* Normal exit */
	    }
	} else {
	    errmsg = vstrallocf(_("%s got bad exit: see %s"), cmd, dbfn());
	}
	dbprintf(_("after %s %s wait\n"), cmd, qdisk);

	g_ptr_array_free_full(argv_ptr);

	aclose(nullfd);
	afclose(dumpout);

	fprintf(stdout, "%d %lld 1\n", level, (long long)size);
    }
    amfree(qdisk);
    amfree(cmd);
    return;

common_error:
    dbprintf("%s\n", errmsg);
    qerrmsg = quote_string(errmsg);
    amfree(qdisk);
    dbprintf("%s", errmsg);
    fprintf(stdout, "ERROR %s\n", qerrmsg);
    amfree(errmsg);
    amfree(qerrmsg);
    amfree(cmd);
}

static void
amstar_backup(
    application_argument_t *argument)
{
    int        dumpin;
    char      *cmd = NULL;
    char      *qdisk;
    char       line[32768];
    amregex_t *rp;
    off_t      dump_size = -1;
    char      *type;
    char       startchr;
    GPtrArray *argv_ptr;
    int        starpid;
    int        dataf = 1;
    int        mesgf = 3;
    int        indexf = 4;
    int        outf;
    FILE      *mesgstream;
    FILE      *indexstream = NULL;
    FILE      *outstream;
    int        level;
    regex_t    regex_root;
    regex_t    regex_dir;
    regex_t    regex_file;
    regex_t    regex_special;
    regex_t    regex_symbolic;
    regex_t    regex_hard;

    mesgstream = fdopen(mesgf, "w");
    if (!mesgstream) {
	error(_("error mesgstream(%d): %s\n"), mesgf, strerror(errno));
    }

    if (!argument->level) {
	fprintf(mesgstream, "? No level argument\n");
	error(_("No level argument"));
    }
    if (!argument->dle.disk) {
	fprintf(mesgstream, "? No disk argument\n");
	error(_("No disk argument"));
    }
    if (!argument->dle.device) {
	fprintf(mesgstream, "? No device argument\n");
	error(_("No device argument"));
    }

    if (argument->dle.include_list &&
	argument->dle.include_list->nb_element >= 0) {
	fprintf(mesgstream, "? include-list not supported for backup\n");
    }

    level = GPOINTER_TO_INT(argument->level->data);

    qdisk = quote_string(argument->dle.disk);

    argv_ptr = amstar_build_argv(argument, level, CMD_BACKUP);

    cmd = stralloc(star_path);

    starpid = pipespawnv(cmd, STDIN_PIPE|STDERR_PIPE, 1,
			 &dumpin, &dataf, &outf, (char **)argv_ptr->pdata);

    g_ptr_array_free_full(argv_ptr);
    /* close the write ends of the pipes */
    aclose(dumpin);
    aclose(dataf);
    if (argument->dle.create_index) {
	indexstream = fdopen(indexf, "w");
	if (!indexstream) {
	    error(_("error indexstream(%d): %s\n"), indexf, strerror(errno));
	}
    }
    outstream = fdopen(outf, "r");
    if (!outstream) {
	error(_("error outstream(%d): %s\n"), outf, strerror(errno));
    }

    regcomp(&regex_root, "^a \\.\\/ directory$", REG_EXTENDED|REG_NEWLINE);
    regcomp(&regex_dir, "^a (.*) directory$", REG_EXTENDED|REG_NEWLINE);
    regcomp(&regex_file, "^a (.*) (.*) bytes", REG_EXTENDED|REG_NEWLINE);
    regcomp(&regex_special, "^a (.*) special", REG_EXTENDED|REG_NEWLINE);
    regcomp(&regex_symbolic, "^a (.*) symbolic", REG_EXTENDED|REG_NEWLINE);
    regcomp(&regex_hard, "^a (.*) link to", REG_EXTENDED|REG_NEWLINE);

    while ((fgets(line, sizeof(line), outstream)) != NULL) {
	regmatch_t regmatch[3];

	if (line[strlen(line)-1] == '\n') /* remove trailling \n */
	    line[strlen(line)-1] = '\0';

	if (regexec(&regex_root, line, 1, regmatch, 0) == 0) {
	    if (argument->dle.create_index)
		fprintf(indexstream, "%s\n", "/");
	    continue;
	}

	if (regexec(&regex_dir, line, 3, regmatch, 0) == 0) {
	    if (argument->dle.create_index && regmatch[1].rm_so == 2) {
		line[regmatch[1].rm_eo]='\0';
		fprintf(indexstream, "/%s\n", &line[regmatch[1].rm_so]);
	    }
	    continue;
	}

	if (regexec(&regex_file, line, 3, regmatch, 0) == 0 ||
	    regexec(&regex_special, line, 3, regmatch, 0) == 0 ||
	    regexec(&regex_symbolic, line, 3, regmatch, 0) == 0 ||
	    regexec(&regex_hard, line, 3, regmatch, 0) == 0) {
	    if (argument->dle.create_index && regmatch[1].rm_so == 2) {
		line[regmatch[1].rm_eo]='\0';
		fprintf(indexstream, "/%s\n", &line[regmatch[1].rm_so]);
	    }
	    continue;
	}

	for (rp = re_table; rp->regex != NULL; rp++) {
	    if (match(rp->regex, line)) {
		break;
	    }
	}
	if (rp->typ == DMP_SIZE) {
	    dump_size = (off_t)((the_num(line, rp->field)* rp->scale+1023.0)/1024.0);
	}
	switch (rp->typ) {
	    case DMP_IGNORE:
		continue;
	    case DMP_NORMAL:
		type = "normal";
		startchr = '|';
		break;
	    case DMP_STRANGE:
		type = "strange";
		startchr = '?';
		break;
	    case DMP_SIZE:
		type = "size";
		startchr = '|';
		break;
	    case DMP_ERROR:
		type = "error";
		startchr = '?';
		break;
	    default:
		type = "unknown";
		startchr = '!';
		break;
	}
	dbprintf("%3d: %7s(%c): %s\n", rp->srcline, type, startchr, line);
	fprintf(mesgstream,"%c %s\n", startchr, line);
    }

    regfree(&regex_root);
    regfree(&regex_dir);
    regfree(&regex_file);
    regfree(&regex_special);
    regfree(&regex_symbolic);
    regfree(&regex_hard);

    dbprintf(_("gnutar: %s: pid %ld\n"), cmd, (long)starpid);

    dbprintf("sendbackup: size %lld\n", (long long)dump_size);
    fprintf(mesgstream, "sendbackup: size %lld\n", (long long)dump_size);
    dbprintf("sendbackup: end\n");
    fprintf(mesgstream, "sendbackup: end\n");

    fclose(mesgstream);
    if (argument->dle.create_index)
	fclose(indexstream);

    amfree(qdisk);
    amfree(cmd);
}

static void
amstar_restore(
    application_argument_t *argument)
{
    char       *cmd;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    char      **env;
    int         j;
    char       *e;

    if (!star_path) {
	error(_("STAR-PATH not defined"));
    }

    cmd = stralloc(star_path);

    g_ptr_array_add(argv_ptr, stralloc(star_path));
    if (star_directory) {
	struct stat stat_buf;
	if(stat(star_directory, &stat_buf) != 0) {
	    fprintf(stderr,"can not stat directory %s: %s\n", star_directory, strerror(errno));
	    exit(1);
	}
	if (!S_ISDIR(stat_buf.st_mode)) {
	    fprintf(stderr,"%s is not a directory\n", star_directory);
	    exit(1);
	}
	if (access(star_directory, W_OK) != 0 ) {
	    fprintf(stderr, "Can't write to %s: %s\n", star_directory, strerror(errno));
	    exit(1);
	}

	g_ptr_array_add(argv_ptr, stralloc("-C"));
	g_ptr_array_add(argv_ptr, stralloc(star_directory));
    }
    g_ptr_array_add(argv_ptr, stralloc("-x"));
    g_ptr_array_add(argv_ptr, stralloc("-v"));
    g_ptr_array_add(argv_ptr, stralloc("-xattr"));
    g_ptr_array_add(argv_ptr, stralloc("-acl"));
    g_ptr_array_add(argv_ptr, stralloc("errctl=WARN|SAMEFILE|SETTIME|DIFF|SETACL|SETXATTR|SETMODE|BADACL *"));
    g_ptr_array_add(argv_ptr, stralloc("-no-fifo"));
    g_ptr_array_add(argv_ptr, stralloc("-f"));
    g_ptr_array_add(argv_ptr, stralloc("-"));

    if (argument->dle.exclude_list &&
	argument->dle.exclude_list->nb_element == 1) {
	g_ptr_array_add(argv_ptr, stralloc("-exclude-from"));
	g_ptr_array_add(argv_ptr,
			stralloc(argument->dle.exclude_list->first->name));
    }

    if (argument->dle.include_list &&
	argument->dle.include_list->nb_element == 1) {
	FILE *include_list = fopen(argument->dle.include_list->first->name, "r");
	char  line[2*PATH_MAX+2];
	while (fgets(line, 2*PATH_MAX, include_list)) {
	    line[strlen(line)-1] = '\0'; /* remove '\n' */
	    if (strncmp(line, "./", 2) == 0)
		g_ptr_array_add(argv_ptr, stralloc(line+2)); /* remove ./ */
	    else if (strcmp(line, ".") != 0)
		g_ptr_array_add(argv_ptr, stralloc(line));
	}
	fclose(include_list);
    }
    for (j=1; j< argument->argc; j++) {
	if (strncmp(argument->argv[j], "./", 2) == 0)
	    g_ptr_array_add(argv_ptr, stralloc(argument->argv[j]+2));/*remove ./ */
	else if (strcmp(argument->argv[j], ".") != 0)
	    g_ptr_array_add(argv_ptr, stralloc(argument->argv[j]));
    }
    g_ptr_array_add(argv_ptr, NULL);

    debug_executing(argv_ptr);
    env = safe_env();
    become_root();
    execve(cmd, (char **)argv_ptr->pdata, env);
    e = strerror(errno);
    error(_("error [exec %s: %s]"), cmd, e);

}

static void
amstar_validate(
    application_argument_t *argument G_GNUC_UNUSED)
{
    char       *cmd;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    char      **env;
    char       *e;
    char        buf[32768];

    if (!star_path) {
	dbprintf("STAR-PATH not set; Piping to /dev/null\n");
	fprintf(stderr,"STAR-PATH not set; Piping to /dev/null\n");
	goto pipe_to_null;
    }

    cmd = stralloc(star_path);

    g_ptr_array_add(argv_ptr, stralloc(star_path));
    g_ptr_array_add(argv_ptr, stralloc("-t"));
    g_ptr_array_add(argv_ptr, stralloc("-f"));
    g_ptr_array_add(argv_ptr, stralloc("-"));
    g_ptr_array_add(argv_ptr, NULL);

    debug_executing(argv_ptr);
    env = safe_env();
    execve(cmd, (char **)argv_ptr->pdata, env);
    e = strerror(errno);
    dbprintf("failed to execute %s: %s; Piping to /dev/null\n", cmd, e);
    fprintf(stderr,"failed to execute %s: %s; Piping to /dev/null\n", cmd, e);
pipe_to_null:
    while (read(0, buf, 32768) > 0) {
    }

}

static GPtrArray *amstar_build_argv(
    application_argument_t *argument,
    int   level,
    int   command)
{
    char      *dirname;
    char      *fsname;
    char       levelstr[NUM_STR_SIZE+7];
    GPtrArray *argv_ptr = g_ptr_array_new();
    char      *s;
    char      *tardumpfile;
    GSList    *copt;

    if (star_directory) {
	dirname = star_directory;
    } else {
	dirname = argument->dle.device;
    }
    fsname = vstralloc("fs-name=", dirname, NULL);
    for (s = fsname; *s != '\0'; s++) {
	if (iscntrl((int)*s))
	    *s = '-';
    }
    snprintf(levelstr, SIZEOF(levelstr), "-level=%d", level);

    if (star_dle_tardumps) {
	char *sdisk = sanitise_filename(argument->dle.disk);
	tardumpfile = vstralloc(star_tardumps, sdisk, NULL);
	amfree(sdisk);
    } else {
	tardumpfile = stralloc(star_tardumps);
    }

    g_ptr_array_add(argv_ptr, stralloc(star_path));

    g_ptr_array_add(argv_ptr, stralloc("-c"));
    g_ptr_array_add(argv_ptr, stralloc("-f"));
    if (command == CMD_ESTIMATE) {
	g_ptr_array_add(argv_ptr, stralloc("/dev/null"));
    } else {
	g_ptr_array_add(argv_ptr, stralloc("-"));
    }
    g_ptr_array_add(argv_ptr, stralloc("-C"));

#if defined(__CYGWIN__)
    {
	char tmppath[PATH_MAX];

	cygwin_conv_to_full_posix_path(dirname, tmppath);
	g_ptr_array_add(argv_ptr, stralloc(tmppath));
    }
#else
    g_ptr_array_add(argv_ptr, stralloc(dirname));
#endif
    g_ptr_array_add(argv_ptr, stralloc(fsname));
    if (star_onefilesystem)
	g_ptr_array_add(argv_ptr, stralloc("-xdev"));
    g_ptr_array_add(argv_ptr, stralloc("-link-dirs"));
    g_ptr_array_add(argv_ptr, stralloc(levelstr));
    g_ptr_array_add(argv_ptr, stralloc2("tardumps=", tardumpfile));
    if (command == CMD_BACKUP)
	g_ptr_array_add(argv_ptr, stralloc("-wtardumps"));

    g_ptr_array_add(argv_ptr, stralloc("-xattr"));
    if (star_acl)
	g_ptr_array_add(argv_ptr, stralloc("-acl"));
    g_ptr_array_add(argv_ptr, stralloc("H=exustar"));
    g_ptr_array_add(argv_ptr, stralloc("errctl=WARN|SAMEFILE|DIFF|GROW|SHRINK|SPECIALFILE|GETXATTR|BADACL *"));
    if (star_sparse)
	g_ptr_array_add(argv_ptr, stralloc("-sparse"));
    g_ptr_array_add(argv_ptr, stralloc("-dodesc"));

    if (command == CMD_BACKUP && argument->dle.create_index)
	g_ptr_array_add(argv_ptr, stralloc("-v"));

    if ((argument->dle.exclude_file &&
	 argument->dle.exclude_file->nb_element >= 1) ||
	(argument->dle.exclude_list &&
	 argument->dle.exclude_list->nb_element >= 1)) {
	g_ptr_array_add(argv_ptr, stralloc("-match-tree"));
	g_ptr_array_add(argv_ptr, stralloc("-not"));
    }
    if (argument->dle.exclude_file &&
	argument->dle.exclude_file->nb_element >= 1) {
	sle_t *excl;
	for (excl = argument->dle.exclude_file->first; excl != NULL;
	     excl = excl->next) {
	    char *ex;
	    if (strcmp(excl->name, "./") == 0) {
		ex = g_strdup_printf("pat=%s", excl->name+2);
	    } else {
		ex = g_strdup_printf("pat=%s", excl->name);
	    }
	    g_ptr_array_add(argv_ptr, ex);
	}
    }
    if (argument->dle.exclude_list &&
	argument->dle.exclude_list->nb_element >= 1) {
	sle_t *excl;
	for (excl = argument->dle.exclude_list->first; excl != NULL;
	     excl = excl->next) {
	    char *exclname = fixup_relative(excl->name, argument->dle.device);
	    FILE *exclude;
	    char *aexc;
	    if ((exclude = fopen(exclname, "r")) != NULL) {
		while ((aexc = agets(exclude)) != NULL) {
		    if (aexc[0] != '\0') {
			char *ex;
			if (strcmp(aexc, "./") == 0) {
			    ex = g_strdup_printf("pat=%s", aexc+2);
			} else {
			    ex = g_strdup_printf("pat=%s", aexc);
			}
			g_ptr_array_add(argv_ptr, ex);
		    }
		    amfree(aexc);
		}
		fclose(exclude);
	    }
	    amfree(exclname);
	}
    }

    /* It is best to place command_options at the and of command line.
     * For example '-find' option requires that it is the last option used.
     * See: http://cdrecord.berlios.de/private/man/star/star.1.html
     */
    for (copt = argument->command_options; copt != NULL; copt = copt->next) {
	g_ptr_array_add(argv_ptr, stralloc((char *)copt->data));
    }

    g_ptr_array_add(argv_ptr, stralloc("."));

    g_ptr_array_add(argv_ptr, NULL);

    amfree(tardumpfile);
    amfree(fsname);

    return(argv_ptr);
}

static int
check_device(
    application_argument_t *argument)
{
    char *qdevice;
    struct stat stat_buf;

    qdevice = quote_string(argument->dle.device);
    set_root_privs(1);
    if(!stat(argument->dle.device, &stat_buf)) { 
	if (!S_ISDIR(stat_buf.st_mode)) {
	    set_root_privs(0);
	    g_fprintf(stderr, _("ERROR %s is not a directory\n"), qdevice);
	    amfree(qdevice);
	    return 0;
	}
    } else {
	set_root_privs(0);
	g_fprintf(stderr, _("ERROR can not stat %s: %s\n"), qdevice,
                  strerror(errno));
	amfree(qdevice);
	return 0;
    }
    if (access(argument->dle.device, R_OK|X_OK) == -1) {
	set_root_privs(0);
	g_fprintf(stderr, _("ERROR can not access %s: %s\n"),
		  argument->dle.device, strerror(errno));
	amfree(qdevice);
	return 0;
    }
    set_root_privs(0);
    amfree(qdevice);
    return 1;
}

