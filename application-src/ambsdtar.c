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

/* PROPERTY:
 *
 * BSDTAR-PATH     (default /usr/bin/tar))
 * STATE-DIR       (default $AMSTATDIR/bsdtar)
 * DIRECTORY       (no default, if set, the backup will be from that directory
 *		    instead of from the --device)
 * ONE-FILE-SYSTEM (default YES)
 * INCLUDE-FILE
 * INCLUDE-LIST
 * INCLUDE-OPTIONAL
 * EXCLUDE-FILE
 * EXCLUDE-LIST
 * EXCLUDE-OPTIONAL
 * NORMAL
 * IGNORE
 * STRANGE
 * EXIT-HANDLING   (1=GOOD 2=BAD)
 * TAR-BLOCKSIZE   (default does not add --block-size option,
 *                  using tar's default)
 * VERBOSE
 */

#include "amanda.h"
#include "match.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "clock.h"
#include "amutil.h"
#include "getfsent.h"
#include "client_util.h"
#include "conffile.h"
#include "getopt.h"

int debug_application = 1;
#define application_debug(i, ...) do {	\
	if ((i) <= debug_application) {	\
	    g_debug(__VA_ARGS__);	\
	}				\
} while (0)

static amregex_t init_re_table[] = {
  /* tar prints the size in bytes */
  AM_SIZE_RE("^ *Total bytes written: [0-9][0-9]*", 1, 1),
  AM_NORMAL_RE("^could not open conf file"),
  AM_NORMAL_RE("^Elapsed time:"),
  AM_NORMAL_RE("^Throughput"),
  AM_IGNORE_RE(": Directory is new$"),
  AM_IGNORE_RE(": Directory has been renamed"),

  /* GNU tar 1.13.17 will print this warning when (not) backing up a
     Unix named socket.  */
  AM_NORMAL_RE(": socket ignored$"),

  /* GNUTAR produces a few error messages when files are modified or
     removed while it is running.  They may cause data to be lost, but
     then they may not.  We shouldn't consider them NORMAL until
     further investigation.  */
  AM_NORMAL_RE(": File .* shrunk by [0-9][0-9]* bytes, padding with zeros"),
  AM_NORMAL_RE(": Cannot add file .*: No such file or directory$"),
  AM_NORMAL_RE(": Error exit delayed from previous errors"),

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
    char      *tar_blocksize;
    GSList    *level;
    GSList    *command_options;
    dle_t      dle;
    int        argc;
    char     **argv;
    int        verbose;
} application_argument_t;

enum { CMD_ESTIMATE, CMD_BACKUP };

static void ambsdtar_support(application_argument_t *argument);
static void ambsdtar_selfcheck(application_argument_t *argument);
static void ambsdtar_estimate(application_argument_t *argument);
static void ambsdtar_backup(application_argument_t *argument);
static void ambsdtar_restore(application_argument_t *argument);
static void ambsdtar_validate(application_argument_t *argument);
static void ambsdtar_index(application_argument_t *argument);
static void ambsdtar_build_exinclude(dle_t *dle, int verbose,
				     int *nb_exclude, char **file_exclude,
				     int *nb_include, char **file_include);
static char *ambsdtar_get_timestamps(application_argument_t *argument,
				     int level,
				     FILE *mesgstream, int command);
static void ambsdtar_set_timestamps(application_argument_t *argument,
				    int                     level,
				    char                   *timestamps,
				    FILE                   *mesgstream,
				    int                     command);
static GPtrArray *ambsdtar_build_argv(application_argument_t *argument,
				      char *timestamps,
				      char **file_exclude, char **file_include,
				      int command);
static char *bsdtar_path;
static char *state_dir;
static char *bsdtar_directory;
static int bsdtar_onefilesystem;
static GSList *normal_message = NULL;
static GSList *ignore_message = NULL;
static GSList *strange_message = NULL;
static char   *exit_handling;
static int    exit_value[256];

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
    {"bsdtar-path"     , 1, NULL, 10},
    {"state-dir"       , 1, NULL, 11},
    {"one-file-system" , 1, NULL, 12},
    {"include-file"    , 1, NULL, 16},
    {"include-list"    , 1, NULL, 17},
    {"include-optional", 1, NULL, 18},
    {"exclude-file"    , 1, NULL, 19},
    {"exclude-list"    , 1, NULL, 20},
    {"exclude-optional", 1, NULL, 21},
    {"directory"       , 1, NULL, 22},
    {"normal"          , 1, NULL, 23},
    {"ignore"          , 1, NULL, 24},
    {"strange"         , 1, NULL, 25},
    {"exit-handling"   , 1, NULL, 26},
    {"calcsize"        , 0, NULL, 27},
    {"tar-blocksize"   , 1, NULL, 28},
    {"no-unquote"      , 1, NULL, 29},
    {"command-options" , 1, NULL, 33},
    {"verbose"          , 1, NULL, 36},
    {NULL, 0, NULL, 0}
};

static char *
escape_tar_glob(
    char *str,
    int  *in_argv)
{
    char *result = malloc(4*strlen(str)+1);
    char *r = result;
    char *s;

    *in_argv = 0;
    for (s = str; *s != '\0'; s++) {
	if (*s == '\\') {
	    char c = *(s+1);
	    if (c == '\\') {
		*r++ = '\\';
		*r++ = '\\';
		*r++ = '\\';
		s++;
	    } else if (c == '?') {
		*r++ = 127;
		s++;
		continue;
	    } else if (c == 'a') {
		*r++ = 7;
		s++;
		continue;
	    } else if (c == 'b') {
		*r++ = 8;
		s++;
		continue;
	    } else if (c == 'f') {
		*r++ = 12;
		s++;
		continue;
	    } else if (c == 'n') {
		*r++ = 10;
		s++;
		*in_argv = 1;
		continue;
	    } else if (c == 'r') {
		*r++ = 13;
		s++;
		*in_argv = 1;
		continue;
	    } else if (c == 't') {
		*r++ = 9;
		s++;
		continue;
	    } else if (c == 'v') {
		*r++ = 11;
		s++;
		continue;
	    } else if (c >= '0' && c <= '9') {
		char d = c-'0';
		s++;
		c = *(s+1);
		if (c >= '0' && c <= '9') {
		    d = (d*8)+(c-'0');
		    s++;
		    c = *(s+1);
		    if (c >= '0' && c <= '9') {
			d = (d*8)+(c-'0');
			s++;
		    }
		}
		*r++ = d;
		continue;
	    } else {
		*r++ = '\\';
	    }
	} else if (*s == '?') {
	    *r++ = '\\';
	    *r++ = '\\';
	} else if (*s == '*' || *s == '[') {
	    *r++ = '\\';
	}
	*r++ = *s;
    }
    *r = '\0';

    return result;
}


int
main(
    int		argc,
    char **	argv)
{
    int c;
    char *command;
    application_argument_t argument;
    int i;

#ifdef BSDTAR
    bsdtar_path = g_strdup(BSDTAR);
#else
    bsdtar_path = NULL;
#endif
    state_dir = NULL;
    bsdtar_directory = NULL;
    bsdtar_onefilesystem = 1;
    exit_handling = NULL;

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
        printf("ERROR no command given to ambsdtar\n");
        error(_("No command given to ambsdtar"));
    }

    /* drop root privileges */
    if (!set_root_privs(0)) {
	if (g_str_equal(argv[1], "selfcheck")) {
	    printf("ERROR ambsdtar must be run setuid root\n");
	}
	error(_("ambsdtar must be run setuid root"));
    }

    safe_fd(3, 2);

    set_pname("ambsdtar");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

#if defined(USE_DBMALLOC)
    malloc_size_1 = malloc_inuse(&malloc_hist_1);
#endif

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_syslog);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();
    g_debug(_("version %s"), VERSION);

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
    argument.calcsize = 0;
    argument.tar_blocksize = NULL;
    argument.level      = NULL;
    argument.command_options = NULL;
    argument.verbose = 0;
    init_dle(&argument.dle);
    argument.dle.record = 0;

    while (1) {
	int option_index = 0;
	c = getopt_long (argc, argv, "", long_options, &option_index);
	if (c == -1) {
	    break;
	}
	switch (c) {
	case 1: amfree(argument.config);
		argument.config = g_strdup(optarg);
		break;
	case 2: amfree(argument.host);
		argument.host = g_strdup(optarg);
		break;
	case 3: amfree(argument.dle.disk);
		argument.dle.disk = g_strdup(optarg);
		break;
	case 4: amfree(argument.dle.device);
		argument.dle.device = g_strdup(optarg);
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
	case 10: amfree(bsdtar_path);
		 bsdtar_path = g_strdup(optarg);
		 break;
	case 11: amfree(state_dir);
		 state_dir = g_strdup(optarg);
		 break;
	case 12: if (strcasecmp(optarg, "NO") == 0)
		     bsdtar_onefilesystem = 0;
		 else if (strcasecmp(optarg, "YES") == 0)
		     bsdtar_onefilesystem = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad ONE-FILE-SYSTEM property value (%s)]\n"), get_pname(), optarg);
		 break;
	case 16: argument.dle.include_file =
			 append_sl(argument.dle.include_file, optarg);
		 break;
	case 17: argument.dle.include_list =
			 append_sl(argument.dle.include_list, optarg);
		 break;
	case 18: argument.dle.include_optional = 1;
		 break;
	case 19: argument.dle.exclude_file =
			 append_sl(argument.dle.exclude_file, optarg);
		 break;
	case 20: argument.dle.exclude_list =
			 append_sl(argument.dle.exclude_list, optarg);
		 break;
	case 21: argument.dle.exclude_optional = 1;
		 break;
	case 22: amfree(bsdtar_directory);
		 bsdtar_directory = g_strdup(optarg);
		 break;
	case 23: normal_message =
			 g_slist_append(normal_message, optarg);
		 break;
	case 24: ignore_message =
			 g_slist_append(ignore_message, optarg);
		 break;
	case 25: strange_message =
			 g_slist_append(strange_message, optarg);
		 break;
	case 26: amfree(exit_handling);
		 exit_handling = g_strdup(optarg);
		 break;
	case 27: argument.calcsize = 1;
		 break;
	case 28: amfree(argument.tar_blocksize);
		 argument.tar_blocksize = g_strdup(optarg);
		 break;
	case 33: argument.command_options =
			g_slist_append(argument.command_options,
				       g_strdup(optarg));
		 break;
	case 36: if (strcasecmp(optarg, "YES") == 0)
		     argument.verbose = 1;
		 break;
	case ':':
	case '?':
		break;
	}
    }

    if (!argument.dle.disk && argument.dle.device)
	argument.dle.disk = g_strdup(argument.dle.device);
    if (!argument.dle.device && argument.dle.disk)
	argument.dle.device = g_strdup(argument.dle.disk);

    argument.argc = argc - optind;
    argument.argv = argv + optind;

    if (argument.config) {
	config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
		    argument.config);
	dbrename(get_config_name(), DBG_SUBDIR_CLIENT);
    }

    if (config_errors(NULL) >= CFGERR_ERRORS) {
	g_critical(_("errors processing config file"));
    }

    if (state_dir && strlen(state_dir) == 0)
	amfree(state_dir);
    if (!state_dir) {
	state_dir = g_strdup_printf("%s/%s", amdatadir, "bsdtar");
    }

    re_table = build_re_table(init_re_table, normal_message, ignore_message,
			      strange_message);

    for(i=0;i<256;i++)
	exit_value[i] = 1; /* BAD  */
    exit_value[0] = 0;     /* GOOD */
    exit_value[1] = 0;     /* GOOD */
    if (exit_handling) {
	char *s = exit_handling;
	while (s) {
	    char *r = strchr(s, '=');
	    if (r) {
		int j = atoi(s);
		if (j >= 0 && j < 256) {
		    r++;
		    if (strncasecmp(r, "GOOD", 4) == 0) {
			exit_value[j] = 0;
		    }
		}
	    }
	    s = strchr(s+1, ' ');
	}
    }

    if (bsdtar_path) {
	g_debug("BSDTAR-PATH %s", bsdtar_path);
    } else {
	g_debug("BSDTAR-PATH is not set");
    }
    if (state_dir) {
	    g_debug("STATE-DIR %s", state_dir);
    } else {
	g_debug("STATE-DIR is not set");
    }
    if (bsdtar_directory) {
	g_debug("DIRECTORY %s", bsdtar_directory);
    }
    g_debug("ONE-FILE-SYSTEM %s", bsdtar_onefilesystem? "yes":"no");
    {
	amregex_t *rp;
	for (rp = re_table; rp->regex != NULL; rp++) {
	    switch (rp->typ) {
		case DMP_NORMAL : g_debug("NORMAL %s", rp->regex); break;
		case DMP_IGNORE : g_debug("IGNORE %s", rp->regex); break;
		case DMP_STRANGE: g_debug("STRANGE %s", rp->regex); break;
		case DMP_SIZE   : g_debug("SIZE %s", rp->regex); break;
		case DMP_ERROR  : g_debug("ERROR %s", rp->regex); break;
	    }
	}
    }

    if (g_str_equal(command, "support")) {
	ambsdtar_support(&argument);
    } else if (g_str_equal(command, "selfcheck")) {
	ambsdtar_selfcheck(&argument);
    } else if (g_str_equal(command, "estimate")) {
	ambsdtar_estimate(&argument);
    } else if (g_str_equal(command, "backup")) {
	ambsdtar_backup(&argument);
    } else if (g_str_equal(command, "restore")) {
	ambsdtar_restore(&argument);
    } else if (g_str_equal(command, "validate")) {
	ambsdtar_validate(&argument);
    } else if (g_str_equal(command, "index")) {
	ambsdtar_index(&argument);
    } else {
	g_debug("Unknown command `%s'.", command);
	fprintf(stderr, "Unknown command `%s'.\n", command);
	exit (1);
    }

    g_free(argument.config);
    g_free(argument.host);
    g_free(argument.dle.disk);
    g_free(argument.dle.device);
    g_free(argument.tar_blocksize);
    g_slist_free(argument.level);

    dbclose();

    return 0;
}

static void
ambsdtar_support(
    application_argument_t *argument)
{
    (void)argument;
    fprintf(stdout, "CONFIG YES\n");
    fprintf(stdout, "HOST YES\n");
    fprintf(stdout, "DISK YES\n");
    fprintf(stdout, "MAX-LEVEL 399\n");
    fprintf(stdout, "INDEX-LINE YES\n");
    fprintf(stdout, "INDEX-XML NO\n");
    fprintf(stdout, "MESSAGE-LINE YES\n");
    fprintf(stdout, "MESSAGE-XML NO\n");
    fprintf(stdout, "RECORD YES\n");
    fprintf(stdout, "INCLUDE-FILE YES\n");
    fprintf(stdout, "INCLUDE-LIST YES\n");
    fprintf(stdout, "INCLUDE-OPTIONAL YES\n");
    fprintf(stdout, "EXCLUDE-FILE YES\n");
    fprintf(stdout, "EXCLUDE-LIST YES\n");
    fprintf(stdout, "EXCLUDE-OPTIONAL YES\n");
    fprintf(stdout, "COLLECTION NO\n");
    fprintf(stdout, "MULTI-ESTIMATE YES\n");
    fprintf(stdout, "CALCSIZE YES\n");
    fprintf(stdout, "CLIENT-ESTIMATE YES\n");
}

static void
ambsdtar_selfcheck(
    application_argument_t *argument)
{
    if (argument->dle.disk) {
	char *qdisk = quote_string(argument->dle.disk);
	fprintf(stdout, "OK disk %s\n", qdisk);
	amfree(qdisk);
    }

    printf("OK ambsdtar version %s\n", VERSION);
    ambsdtar_build_exinclude(&argument->dle, 1, NULL, NULL, NULL, NULL);

    printf("OK ambsdtar\n");
    if (bsdtar_path) {
	if (check_file(bsdtar_path, X_OK)) {
	    char *bsdtar_version;
	    GPtrArray *argv_ptr = g_ptr_array_new();

	    g_ptr_array_add(argv_ptr, bsdtar_path);
	    g_ptr_array_add(argv_ptr, "--version");
	    g_ptr_array_add(argv_ptr, NULL);

	    bsdtar_version = get_first_line(argv_ptr);
	    if (bsdtar_version) {
		char *tv, *bv;
		for (tv = bsdtar_version; *tv && !g_ascii_isdigit(*tv); tv++);
		for (bv = tv; *bv && *bv != ' '; bv++);
		if (*bv) *bv = '\0';
		printf("OK ambsdtar bsdtar-version %s\n", tv);
	    } else {
		printf(_("ERROR [Can't get %s version]\n"), bsdtar_path);
	    }

	    g_ptr_array_free(argv_ptr, TRUE);
	    amfree(bsdtar_version);
	}
    } else {
	printf(_("ERROR [BSDTAR program not available]\n"));
    }

    set_root_privs(1);
    if (state_dir && strlen(state_dir) == 0)
	state_dir = NULL;
    if (state_dir) {
	check_dir(state_dir, R_OK|W_OK);
    } else {
	printf(_("ERROR [No STATE-DIR]\n"));
    }

    if (bsdtar_directory) {
	check_dir(bsdtar_directory, R_OK);
    } else if (argument->dle.device) {
	check_dir(argument->dle.device, R_OK);
    }

    if (argument->calcsize) {
	char *calcsize = g_strjoin(NULL, amlibexecdir, "/", "calcsize", NULL);
	check_file(calcsize, X_OK);
	check_suid(calcsize);
	amfree(calcsize);
    }
    set_root_privs(0);
}

static void
ambsdtar_estimate(
    application_argument_t *argument)
{
    char      *incrname = NULL;
    GPtrArray *argv_ptr;
    char      *cmd = NULL;
    int        nullfd = -1;
    int        pipefd = -1;
    FILE      *dumpout = NULL;
    off_t      size = -1;
    char       line[32768];
    char      *errmsg = NULL;
    char      *qerrmsg = NULL;
    char      *qdisk = NULL;
    amwait_t   wait_status;
    int        tarpid;
    amregex_t *rp;
    times_t    start_time;
    int        level;
    GSList    *levels;
    char      *file_exclude = NULL;
    char      *file_include = NULL;
    GString   *strbuf;

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

    if (argument->calcsize) {
	char *dirname;
	int   nb_exclude;
	int   nb_include;
	if (bsdtar_directory) {
	    dirname = bsdtar_directory;
	} else {
	    dirname = argument->dle.device;
	}
	ambsdtar_build_exinclude(&argument->dle, 1,
				 &nb_exclude, &file_exclude,
				 &nb_include, &file_include);

	run_calcsize(argument->config, "BSDTAR", argument->dle.disk, dirname,
		     argument->level, file_exclude, file_include);

	if (argument->verbose == 0) {
	    if (file_exclude)
		unlink(file_exclude);
	    if (file_include)
		unlink(file_include);
	}
	amfree(file_exclude);
	amfree(file_include);
	amfree(qdisk);
	return;
    }

    if (!bsdtar_path) {
	errmsg = g_strdup(_("BSDTAR-PATH not defined"));
	goto common_error;
    }

    if (!state_dir) {
	errmsg = g_strdup(_("STATE-DIR not defined"));
	goto common_error;
    }

    qdisk = quote_string(argument->dle.disk);
    for (levels = argument->level; levels != NULL; levels = levels->next) {
	char *timestamps;
	level = GPOINTER_TO_INT(levels->data);
	timestamps = ambsdtar_get_timestamps(argument, level, stdout, CMD_ESTIMATE);
	cmd = g_strdup(bsdtar_path);
	argv_ptr = ambsdtar_build_argv(argument, timestamps, &file_exclude,
				       &file_include, CMD_ESTIMATE);
	amfree(timestamps);

	start_time = curclock();

	if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	    errmsg = g_strdup_printf(_("Cannot access /dev/null : %s"),
				strerror(errno));
	    goto common_exit;
	}

	tarpid = pipespawnv(cmd, STDERR_PIPE, 1,
			    &nullfd, &nullfd, &pipefd,
			    (char **)argv_ptr->pdata);

	dumpout = fdopen(pipefd,"r");
	if (!dumpout) {
	    error(_("Can't fdopen: %s"), strerror(errno));
	    /*NOTREACHED*/
	}

	size = (off_t)-1;
	while (size < 0 && (fgets(line, sizeof(line), dumpout) != NULL)) {
	    if (strlen(line) > 0 && line[strlen(line)-1] == '\n') {
		/* remove trailling \n */
		line[strlen(line)-1] = '\0';
	    }
	    if (line[0] == '\0')
		continue;
	    g_debug("%s", line);
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

	while (fgets(line, sizeof(line), dumpout) != NULL) {
	    g_debug("%s", line);
	}

	g_debug(".....");
	g_debug(_("estimate time for %s level %d: %s"),
		 qdisk,
		 level,
		 walltime_str(timessub(curclock(), start_time)));
	if(size == (off_t)-1) {
	    errmsg = g_strdup_printf(_("no size line match in %s output"), cmd);
	    g_debug(_("%s for %s"), errmsg, qdisk);
	    g_debug(".....");
	} else if(size == (off_t)0 && argument->level == 0) {
	    g_debug(_("possible %s problem -- is \"%s\" really empty?"),
		     cmd, argument->dle.disk);
	    g_debug(".....");
	}
	g_debug(_("estimate size for %s level %d: %lld KB"),
		 qdisk,
		 level,
		 (long long)size);

	kill(-tarpid, SIGTERM);

	g_debug(_("waiting for %s \"%s\" child"), cmd, qdisk);
	waitpid(tarpid, &wait_status, 0);
	if (WIFSIGNALED(wait_status)) {
	    strbuf = g_string_new(errmsg);
	    g_string_append_printf(strbuf, "%s terminated with signal %d: see %s",
                cmd, WTERMSIG(wait_status), dbfn());
	    g_free(errmsg);
            errmsg = g_string_free(strbuf, FALSE);
	} else if (WIFEXITED(wait_status)) {
	    if (exit_value[WEXITSTATUS(wait_status)] == 1) {
                strbuf = g_string_new(errmsg);
                g_string_append_printf(strbuf, "%s exited with status %d: see %s",
                    cmd, WEXITSTATUS(wait_status), dbfn());
		g_free(errmsg);
                errmsg = g_string_free(strbuf, FALSE);
	    } else {
		/* Normal exit */
	    }
	} else {
	    errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
				cmd, dbfn());
	}
	g_debug(_("after %s %s wait"), cmd, qdisk);

common_exit:
	if (errmsg) {
	    g_debug("%s", errmsg);
	    fprintf(stdout, "ERROR %s\n", errmsg);
	    amfree(errmsg);
	}

	if (argument->verbose == 0) {
	    if (file_exclude)
		unlink(file_exclude);
	    if (file_include)
		unlink(file_include);
        }

	g_ptr_array_free_full(argv_ptr);
	amfree(cmd);
	amfree(incrname);

	aclose(nullfd);
	afclose(dumpout);

	fprintf(stdout, "%d %lld 1\n", level, (long long)size);
    }
    amfree(qdisk);
    amfree(file_exclude);
    amfree(file_include);
    amfree(errmsg);
    amfree(qerrmsg);
    amfree(incrname);
    return;

common_error:
    qerrmsg = quote_string(errmsg);
    amfree(qdisk);
    g_debug("%s", errmsg);
    fprintf(stdout, "ERROR %s\n", qerrmsg);
    amfree(file_exclude);
    amfree(file_include);
    amfree(errmsg);
    amfree(qerrmsg);
    amfree(incrname);
    return;
}

static void
ambsdtar_backup(
    application_argument_t *argument)
{
    int         dumpin;
    char      *cmd = NULL;
    char      *qdisk;
    char      *timestamps;
    char       line[32768];
    amregex_t *rp;
    off_t      dump_size = -1;
    char      *type;
    char       startchr;
    int        dataf = 1;
    int        mesgf = 3;
    int        indexf = 4;
    int        outf;
    FILE      *mesgstream;
    FILE      *indexstream = NULL;
    FILE      *outstream;
    char      *errmsg = NULL;
    amwait_t   wait_status;
    GPtrArray *argv_ptr;
    int        tarpid;
    time_t     tt;
    char      *file_exclude;
    char      *file_include;
    char       new_timestamps[64];

    mesgstream = fdopen(mesgf, "w");
    if (!mesgstream) {
	error(_("error mesgstream(%d): %s\n"), mesgf, strerror(errno));
    }

    if (!bsdtar_path) {
	error(_("BSDTAR-PATH not defined"));
    }
    if (!state_dir) {
	error(_("STATE-DIR not defined"));
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

    qdisk = quote_string(argument->dle.disk);

    tt = time(NULL);
    ctime_r(&tt, new_timestamps);
    new_timestamps[strlen(new_timestamps)-1] = '\0';
    timestamps = ambsdtar_get_timestamps(argument,
				   GPOINTER_TO_INT(argument->level->data),
				   mesgstream, CMD_BACKUP);
    cmd = g_strdup(bsdtar_path);
    argv_ptr = ambsdtar_build_argv(argument, timestamps, &file_exclude,
				   &file_include, CMD_BACKUP);

    tarpid = pipespawnv(cmd, STDIN_PIPE|STDERR_PIPE, 1,
			&dumpin, &dataf, &outf, (char **)argv_ptr->pdata);
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

    while (fgets(line, sizeof(line), outstream) != NULL) {
	if (strlen(line) > 0 && line[strlen(line)-1] == '\n') {
	    /* remove trailling \n */
	    line[strlen(line)-1] = '\0';
	}
	if (strlen(line) == 3 && *line == 'a' && *(line+1) == ' ' &&
	    *(line+2) == '.') { /* 'a .' */
	    if (argument->dle.create_index) {
		fprintf(indexstream, "/\n"); /* add '/' */
	    }
	} else if (strlen(line) >= 4 && *line == 'a' && *(line+1) == ' ' &&
	    *(line+2) == '.' && *(line+3) == '/') { /* filename */
	    if (argument->dle.create_index) {
		fprintf(indexstream, "%s\n", &line[3]); /* remove 'a .' */
	    }
	} else { /* message */
	    for(rp = re_table; rp->regex != NULL; rp++) {
		if(match(rp->regex, line)) {
		    break;
		}
	    }
	    if(rp->typ == DMP_SIZE) {
		dump_size = (off_t)((the_num(line, rp->field)* rp->scale+1023.0)/1024.0);
	    }
	    switch(rp->typ) {
	    case DMP_NORMAL:
		type = "normal";
		startchr = '|';
		break;
	    case DMP_IGNORE:
		continue;
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
	    g_debug("%3d: %7s(%c): %s", rp->srcline, type, startchr, line);
	    fprintf(mesgstream,"%c %s\n", startchr, line);
        }
    }
    fclose(outstream);

    waitpid(tarpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = g_strdup_printf(_("%s terminated with signal %d: see %s"),
			    cmd, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (exit_value[WEXITSTATUS(wait_status)] == 1) {
	    errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
				cmd, WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	    ambsdtar_set_timestamps(argument,
				    GPOINTER_TO_INT(argument->level->data),
				    new_timestamps,
				    mesgstream, CMD_BACKUP);
	}
    } else {
	errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
			    cmd, dbfn());
    }
    g_debug(_("after %s %s wait"), cmd, qdisk);
    g_debug(_("ambsdtar: %s: pid %ld"), cmd, (long)tarpid);
    if (errmsg) {
	g_debug("%s", errmsg);
	g_fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
    }


    g_debug("sendbackup: size %lld", (long long)dump_size);
    fprintf(mesgstream, "sendbackup: size %lld\n", (long long)dump_size);

    if (argument->dle.create_index)
	fclose(indexstream);

    fclose(mesgstream);

    if (argument->verbose == 0) {
	if (file_exclude)
	    unlink(file_exclude);
	if (file_include)
	    unlink(file_include);
    }

    amfree(file_exclude);
    amfree(file_include);
    amfree(timestamps);
    amfree(qdisk);
    amfree(cmd);
    amfree(errmsg);
    g_ptr_array_free_full(argv_ptr);
}

static void
ambsdtar_restore(
    application_argument_t *argument)
{
    char       *cmd;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    char      **env;
    int         j;
    char       *e;
    char       *include_filename = NULL;
    char       *exclude_filename = NULL;
    int         tarpid;

    if (!bsdtar_path) {
	error(_("BSDTAR-PATH not defined"));
    }

    cmd = g_strdup(bsdtar_path);
    g_ptr_array_add(argv_ptr, g_strdup(bsdtar_path));
    g_ptr_array_add(argv_ptr, g_strdup("--numeric-owner"));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    if (argument->tar_blocksize) {
	g_ptr_array_add(argv_ptr, g_strdup("--block-size"));
	g_ptr_array_add(argv_ptr, g_strdup(argument->tar_blocksize));
    }
    g_ptr_array_add(argv_ptr, g_strdup("-xvf"));
    g_ptr_array_add(argv_ptr, g_strdup("-"));
    if (bsdtar_directory) {
	struct stat stat_buf;
	if(stat(bsdtar_directory, &stat_buf) != 0) {
	    fprintf(stderr,"can not stat directory %s: %s\n", bsdtar_directory, strerror(errno));
	    exit(1);
	}
	if (!S_ISDIR(stat_buf.st_mode)) {
	    fprintf(stderr,"%s is not a directory\n", bsdtar_directory);
	    exit(1);
	}
	if (access(bsdtar_directory, W_OK) != 0) {
	    fprintf(stderr, "Can't write to %s: %s\n", bsdtar_directory, strerror(errno));
	    exit(1);
	}
	g_ptr_array_add(argv_ptr, g_strdup("--directory"));
	g_ptr_array_add(argv_ptr, g_strdup(bsdtar_directory));
    }

    if (argument->dle.exclude_list &&
	argument->dle.exclude_list->nb_element == 1) {
	FILE      *exclude;
	char      *sdisk;
	int        in_argv;
	int        entry_in_exclude = 0;
	char       line[2*PATH_MAX];
	FILE      *exclude_list;

	if (argument->dle.disk) {
	    sdisk = sanitise_filename(argument->dle.disk);
	} else {
	    sdisk = g_strdup_printf("no_dle-%d", (int)getpid());
	}
	exclude_filename= g_strjoin(NULL, AMANDA_TMPDIR, "/", "exclude-", sdisk,  NULL);
	exclude_list = fopen(argument->dle.exclude_list->first->name, "r");
	if (!exclude_list) {
	    fprintf(stderr, "Cannot open exclude file '%s': %s\n",
		    argument->dle.exclude_list->first->name, strerror(errno));
	    error("Cannot open exclude file '%s': %s\n",
		  argument->dle.exclude_list->first->name, strerror(errno));
	    /*NOTREACHED*/
	}
	exclude = fopen(exclude_filename, "w");
	if (!exclude) {
	    fprintf(stderr, "Cannot open exclude file '%s': %s\n",
		    exclude_filename, strerror(errno));
	    fclose(exclude_list);
	    error("Cannot open exclude file '%s': %s\n",
		  exclude_filename, strerror(errno));
	    /*NOTREACHED*/
	}
	while (fgets(line, 2*PATH_MAX, exclude_list)) {
	    char *escaped;
	    line[strlen(line)-1] = '\0'; /* remove '\n' */
	    escaped = escape_tar_glob(line, &in_argv);
	    if (in_argv) {
		g_ptr_array_add(argv_ptr, "--exclude");
		g_ptr_array_add(argv_ptr, escaped);
	    } else {
		fprintf(exclude,"%s\n", escaped);
		entry_in_exclude++;
		amfree(escaped);
	    }
	}
	fclose(exclude_list);
	fclose(exclude);
	g_ptr_array_add(argv_ptr, g_strdup("--exclude-from"));
	g_ptr_array_add(argv_ptr, exclude_filename);
    }

    {
	GPtrArray *argv_include = g_ptr_array_new();
	FILE      *include;
	char      *sdisk;
	int        in_argv;
	guint      i;
	int        entry_in_include = 0;

	if (argument->dle.disk) {
	    sdisk = sanitise_filename(argument->dle.disk);
	} else {
	    sdisk = g_strdup_printf("no_dle-%d", (int)getpid());
	}
	include_filename = g_strjoin(NULL, AMANDA_TMPDIR, "/", "include-", sdisk,  NULL);
	include = fopen(include_filename, "w");
	if (!include) {
	    fprintf(stderr, "Cannot open include file '%s': %s\n",
		    include_filename, strerror(errno));
	    error("Cannot open include file '%s': %s\n",
		  include_filename, strerror(errno));
	    /*NOTREACHED*/
	}
	if (argument->dle.include_list &&
	    argument->dle.include_list->nb_element == 1) {
	    char line[2*PATH_MAX];
	    FILE *include_list = fopen(argument->dle.include_list->first->name, "r");
	    if (!include_list) {
		fclose(include);
		fprintf(stderr, "Cannot open include file '%s': %s\n",
			argument->dle.include_list->first->name,
			strerror(errno));
		error("Cannot open include file '%s': %s\n",
		      argument->dle.include_list->first->name,
		      strerror(errno));
		/*NOTREACHED*/
	    }
	    while (fgets(line, 2*PATH_MAX, include_list)) {
		char *escaped;
		line[strlen(line)-1] = '\0'; /* remove '\n' */
		if (!g_str_equal(line, ".")) {
		    escaped = escape_tar_glob(line, &in_argv);
		    if (in_argv) {
			g_ptr_array_add(argv_include, escaped);
		    } else {
			fprintf(include,"%s\n", escaped);
			entry_in_include++;
			amfree(escaped);
		    }
		}
	    }
	    fclose(include_list);
	}

	for (j=1; j< argument->argc; j++) {
	    if (!g_str_equal(argument->argv[j], ".")) {
		char *escaped = escape_tar_glob(argument->argv[j], &in_argv);
		if (in_argv) {
		    g_ptr_array_add(argv_include, escaped);
		} else {
		    fprintf(include,"%s\n", escaped);
		    entry_in_include++;
		    amfree(escaped);
		}
	    }
	}
	fclose(include);

	if (entry_in_include) {
	    g_ptr_array_add(argv_ptr, g_strdup("--files-from"));
	    g_ptr_array_add(argv_ptr, include_filename);
	}

	for (i = 0; i < argv_include->len; i++) {
	    g_ptr_array_add(argv_ptr, (char *)g_ptr_array_index(argv_include,i));
	}
	amfree(sdisk);
    }
    g_ptr_array_add(argv_ptr, NULL);

    debug_executing(argv_ptr);

    tarpid = fork();
    switch (tarpid) {
    case -1: error(_("%s: fork returned: %s"), get_pname(), strerror(errno));
    case 0:
	env = safe_env();
	become_root();
	execve(cmd, (char **)argv_ptr->pdata, env);
	execv(cmd, (char **)argv_ptr->pdata);
	e = strerror(errno);
	error(_("error [exec %s: %s]"), cmd, e);
	break;
    default: break;
    }

    waitpid(tarpid, NULL, 0);
    if (argument->verbose == 0) {
	if (exclude_filename)
	    unlink(exclude_filename);
	unlink(include_filename);
    }
    amfree(cmd);
    amfree(include_filename);
    amfree(exclude_filename);
}

static void
ambsdtar_validate(
    application_argument_t *argument G_GNUC_UNUSED)
{
    char       *cmd = NULL;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    char      **env;
    char       *e;
    char        buf[32768];

    if (!bsdtar_path) {
	g_debug("BSDTAR-PATH not set; Piping to /dev/null");
	fprintf(stderr,"BSDTAR-PATH not set; Piping to /dev/null\n");
	goto pipe_to_null;
    }

    cmd = g_strdup(bsdtar_path);
    g_ptr_array_add(argv_ptr, g_strdup(bsdtar_path));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    g_ptr_array_add(argv_ptr, g_strdup("-tf"));
    g_ptr_array_add(argv_ptr, g_strdup("-"));
    g_ptr_array_add(argv_ptr, NULL);

    debug_executing(argv_ptr);
    env = safe_env();
    execve(cmd, (char **)argv_ptr->pdata, env);
    e = strerror(errno);
    g_debug("failed to execute %s: %s; Piping to /dev/null", cmd, e);
    fprintf(stderr,"failed to execute %s: %s; Piping to /dev/null\n", cmd, e);
    free_env(env);
pipe_to_null:
    while (read(0, buf, 32768) > 0) {
    }
    amfree(cmd);
}

static void
ambsdtar_index(
    application_argument_t *argument G_GNUC_UNUSED)
{
    char       *cmd = NULL;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    int         datain = 0;
    int         indexf;
    int         errf = 2;
    pid_t       tarpid;
    FILE       *indexstream;
    char        line[32768];
    amwait_t    wait_status;
    char       *errmsg = NULL;

    if (!bsdtar_path) {
	g_debug("BSDTAR-PATH not set");
	fprintf(stderr,"BSDTAR-PATH not set");
	while (read(0, line, 32768) > 0) {
	}
	exit(1);
    }

    cmd = g_strdup(bsdtar_path);
    g_ptr_array_add(argv_ptr, g_strdup(bsdtar_path));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    g_ptr_array_add(argv_ptr, g_strdup("-tf"));
    g_ptr_array_add(argv_ptr, g_strdup("-"));
    g_ptr_array_add(argv_ptr, NULL);

    tarpid = pipespawnv(cmd, STDOUT_PIPE, 0,
			&datain, &indexf, &errf, (char **)argv_ptr->pdata);
    aclose(datain);

    indexstream = fdopen(indexf, "r");
    if (!indexstream) {
	error(_("error indexstream(%d): %s\n"), indexf, strerror(errno));
    }

    fprintf(stdout, "/\n");
    while (fgets(line, sizeof(line), indexstream) != NULL) {
	if (strlen(line) > 0 && line[strlen(line)-1] == '\n') {
	    /* remove trailling \n */
	    line[strlen(line)-1] = '\0';
	}
	if (strlen(line) > 1 && line[strlen(line)-1] == '/') {
	    /* remove trailling / */
	    line[strlen(line)-1] = '\0';
	}
	if (*line == '.' && *(line+1) == '/') { /* filename */
	    fprintf(stdout, "%s\n", &line[1]); /* remove . */
	}
    }
    fclose(indexstream);
    waitpid(tarpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = g_strdup_printf(_("%s terminated with signal %d: see %s"),
				 cmd, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (exit_value[WEXITSTATUS(wait_status)] == 1) {
	    errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
				     cmd, WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
				 cmd, dbfn());
    }
    g_debug(_("ambsdtar: %s: pid %ld"), cmd, (long)tarpid);
    if (errmsg) {
	g_debug("%s", errmsg);
	fprintf(stderr, "error [%s]\n", errmsg);
    }

    amfree(cmd);
}

static void
ambsdtar_build_exinclude(
    dle_t  *dle,
    int     verbose,
    int    *nb_exclude,
    char  **file_exclude,
    int    *nb_include,
    char  **file_include)
{
    int n_exclude = 0;
    int n_include = 0;
    char *exclude = NULL;
    char *include = NULL;

    if (dle->exclude_file) n_exclude += dle->exclude_file->nb_element;
    if (dle->exclude_list) n_exclude += dle->exclude_list->nb_element;
    if (dle->include_file) n_include += dle->include_file->nb_element;
    if (dle->include_list) n_include += dle->include_list->nb_element;

    if (n_exclude > 0) exclude = build_exclude(dle, verbose);
    if (n_include > 0) include = build_include(dle, verbose);

    if (nb_exclude)
	*nb_exclude = n_exclude;
    if (file_exclude)
	*file_exclude = exclude;
    else
	amfree(exclude);

    if (nb_include)
	*nb_include = n_include;
    if (file_include)
	*file_include = include;
    else
	amfree(include);
}

static char *
ambsdtar_get_timestamps(
    application_argument_t *argument,
    int                     level,
    FILE                   *mesgstream,
    int                     command)
{
    char *basename = NULL;
    char *filename = NULL;
    int   infd;
    char *inputname = NULL;
    char *errmsg = NULL;

    if (state_dir) {
	char number[NUM_STR_SIZE];
	int baselevel;
	char *sdisk = sanitise_filename(argument->dle.disk);
	FILE *tt;
	char line[1024];

	basename = g_strjoin(NULL, state_dir,
			     "/",
			     argument->host,
			     sdisk,
			     NULL);
	amfree(sdisk);

	snprintf(number, sizeof(number), "%d", level);
	filename = g_strjoin(NULL, basename, "_", number, NULL);
	unlink(filename);

	/*
	 * Open the timestamps file from the previous level.  Search
	 * backward until one is found.  If none are found (which will also
	 * be true for a level 0), arrange to read from /dev/null.
	 */
	baselevel = level;
	infd = -1;
	inputname = g_strdup("AA");
	while (infd == -1 && inputname != NULL) {
	    amfree(inputname);
	    if (--baselevel >= 0) {
		snprintf(number, sizeof(number), "%d", baselevel);
		inputname = g_strconcat(basename, "_", number, NULL);
	    } else {
		inputname = NULL;
		amfree(basename);
		amfree(filename);
		g_debug("Using NULL timestamps");
		return NULL;
	    }
	    if ((infd = open(inputname, O_RDONLY)) == -1) {

		errmsg = g_strdup_printf(_("ambsdtar: error opening %s: %s"),
					 inputname, strerror(errno));
		g_debug("%s", errmsg);
		if (baselevel < 0) {
		    if (command == CMD_ESTIMATE) {
			fprintf(mesgstream, "ERROR %s\n", errmsg);
		    } else {
			fprintf(mesgstream, "? %s\n", errmsg);
		    }
		    exit(1);
		}
		amfree(errmsg);
	    }
	}

	tt = fdopen(infd, "r");
	if (!fgets(line, 1024, tt)) {
	    errmsg = g_strdup_printf(_("ambsdtar: error reading '%s': %s"),
				     inputname, strerror(errno));
	    if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	    } else {
		fprintf(mesgstream, "? %s\n", errmsg);
	    }
	    exit(1);
	}
	fclose(tt);
	tt = NULL;
	infd = -1;
	g_debug("Read timestamps '%s' to '%s'", line, filename);
	amfree(basename);
	amfree(filename);
	amfree(inputname);
	return g_strdup(line);
    } else {
	errmsg =  _("STATE-DIR is not defined");
	g_debug("%s", errmsg);
	if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	} else {
		fprintf(mesgstream, "? %s\n", errmsg);
	}
	exit(1);
    }
    return NULL;
}

static void
ambsdtar_set_timestamps(
    application_argument_t *argument,
    int                     level,
    char                   *timestamps,
    FILE                   *mesgstream,
    int                     command)
{
    char *basename = NULL;
    char *filename = NULL;
    char *errmsg = NULL;

    if (state_dir) {
	char number[NUM_STR_SIZE];
	char *sdisk = sanitise_filename(argument->dle.disk);
	FILE *tt;

	basename = g_strjoin(NULL, state_dir,
			     "/",
			     argument->host,
			     sdisk,
			     NULL);
	amfree(sdisk);

	snprintf(number, sizeof(number), "%d", level);
	filename = g_strjoin(NULL, basename, "_", number, NULL);

	tt = fopen(filename, "w");
	fprintf(tt, "%s", timestamps);
	fclose(tt);
	g_debug("Wrote timestamps '%s' to '%s'", timestamps, filename);
	amfree(basename);
	amfree(filename);
    } else {
	errmsg =  _("STATE-DIR is not defined");
	g_debug("%s", errmsg);
	if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	} else {
		fprintf(mesgstream, "? %s\n", errmsg);
	}
	exit(1);
    }
}

static GPtrArray *
ambsdtar_build_argv(
    application_argument_t *argument,
    char  *timestamps,
    char **file_exclude,
    char **file_include,
    int    command)
{
    int        nb_exclude = 0;
    int        nb_include = 0;
    char      *dirname;
    char       tmppath[PATH_MAX];
    GPtrArray *argv_ptr = g_ptr_array_new();
    GSList    *copt;

    ambsdtar_build_exinclude(&argument->dle, 1,
			     &nb_exclude, file_exclude,
			     &nb_include, file_include);

    if (bsdtar_directory) {
	dirname = bsdtar_directory;
    } else {
	dirname = argument->dle.device;
    }

    g_ptr_array_add(argv_ptr, g_strdup(bsdtar_path));

    g_ptr_array_add(argv_ptr, g_strdup("--create"));
    if (command == CMD_BACKUP && argument->dle.create_index)
        g_ptr_array_add(argv_ptr, g_strdup("--verbose"));
    g_ptr_array_add(argv_ptr, g_strdup("--file"));
    if (command == CMD_ESTIMATE) {
        g_ptr_array_add(argv_ptr, g_strdup("/dev/null"));
    } else {
        g_ptr_array_add(argv_ptr, g_strdup("-"));
    }
    g_ptr_array_add(argv_ptr, g_strdup("--directory"));
    canonicalize_pathname(dirname, tmppath);
    g_ptr_array_add(argv_ptr, g_strdup(tmppath));
    if (timestamps) {
	g_ptr_array_add(argv_ptr, g_strdup("--newer"));
	g_ptr_array_add(argv_ptr, g_strdup(timestamps));
    }
    if (bsdtar_onefilesystem)
	g_ptr_array_add(argv_ptr, g_strdup("--one-file-system"));
    if (argument->tar_blocksize) {
	g_ptr_array_add(argv_ptr, g_strdup("--block-size"));
	g_ptr_array_add(argv_ptr, g_strdup(argument->tar_blocksize));
    }
    g_ptr_array_add(argv_ptr, g_strdup("--totals"));

    for (copt = argument->command_options; copt != NULL; copt = copt->next) {
	g_ptr_array_add(argv_ptr, g_strdup((char *)copt->data));
    }

    if (*file_exclude) {
	g_ptr_array_add(argv_ptr, g_strdup("--exclude-from"));
	g_ptr_array_add(argv_ptr, g_strdup(*file_exclude));
    }

    if (*file_include) {
	g_ptr_array_add(argv_ptr, g_strdup("--files-from"));
	g_ptr_array_add(argv_ptr, g_strdup(*file_include));
    } else {
	g_ptr_array_add(argv_ptr, g_strdup("."));
    }
    g_ptr_array_add(argv_ptr, NULL);

    return(argv_ptr);
}

