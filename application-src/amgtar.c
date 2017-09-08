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
 * $Id: amgtar.c 8888 2007-10-02 13:40:42Z martineau $
 *
 * send estimated backup sizes using dump
 */

/* PROPERTY:
 *
 * GNUTAR-PATH     (default GNUTAR)
 * GNUTAR-LISTDIR  (default CNF_GNUTAR_LIST_DIR)
 * TARGET (DIRECTORY)  (no default, if set, the backup will be from that
 *			directory instead of from the --device)
 * ONE-FILE-SYSTEM (default YES)
 * SPARSE          (default YES)
 * ATIME-PRESERVE  (default YES)
 * CHECK-DEVICE    (default YES)
 * NO-UNQUOTE      (default NO)
 * ACLS            (default NO)
 * SELINUX         (default NO)
 * XATTRS          (default NO)
 * INCLUDE-FILE
 * INCLUDE-LIST
 * INCLUDE-LIST-GLOB
 * INCLUDE-OPTIONAL
 * EXCLUDE-FILE
 * EXCLUDE-LIST
 * EXCLUDE-LIST-GLOB
 * EXCLUDE-OPTIONAL
 * NORMAL
 * IGNORE
 * STRANGE
 * EXIT-HANDLING   (1=GOOD 2=BAD)
 * TAR-BLOCKSIZE   (default does not add --blocking-factor option,
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
#include "security-file.h"

int debug_application = 1;
#define application_debug(i, ...) do {	\
	if ((i) <= debug_application) {	\
	    dbprintf(__VA_ARGS__);	\
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

  /* GNU tar 1.27 */
  AM_NORMAL_RE(": directory is on a different filesystem; not dumped"),

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

  AM_ERROR_RE("amgtar: error"),

  /* catch-all: DMP_STRANGE is returned for all other lines */
  AM_STRANGE_RE(NULL)
};
static amregex_t *re_table;

/* local functions */
int main(int argc, char **argv);

typedef struct application_argument_s {
    char         *config;
    char         *host;
    int           message;
    int           collection;
    int           calcsize;
    char         *tar_blocksize;
    GSList       *level;
    GSList       *command_options;
    char         *include_list_glob;
    char         *exclude_list_glob;
    dle_t         dle;
    int           argc;
    char        **argv;
    int           verbose;
    int           ignore_zeros;
    am_feature_t *amfeatures;
    char         *recover_dump_state_file;
    gboolean      dar;
    int 	  state_stream;
} application_argument_t;

enum { CMD_ESTIMATE, CMD_BACKUP };

static void amgtar_support(application_argument_t *argument);
static void amgtar_selfcheck(application_argument_t *argument);
static void amgtar_discover(application_argument_t *argument);
static void amgtar_estimate(application_argument_t *argument);
static void amgtar_backup(application_argument_t *argument);
static void amgtar_restore(application_argument_t *argument);
static void amgtar_validate(application_argument_t *argument);
static void amgtar_index(application_argument_t *argument);
static void amgtar_build_exinclude(dle_t *dle,
				   int *nb_exclude, char **file_exclude,
				   int *nb_include, char **file_includei,
				   char *dirname, messagelist_t *mlist);
static char *amgtar_get_incrname(application_argument_t *argument, int level,
				 FILE *mesgstream, int command);
static void check_no_check_device(void);
static GPtrArray *amgtar_build_argv(char *gnutar_realpath,
				application_argument_t *argument,
				char *incrname, char **file_exclude,
				char **file_include, int command,
				messagelist_t *mlist);
static char *command = NULL;
static char *gnutar_path;
static char *gnutar_listdir;
static char *gnutar_target;
static int gnutar_onefilesystem;
static int gnutar_atimepreserve;
static int gnutar_acls;
static int gnutar_selinux;
static int gnutar_xattrs;
static int gnutar_checkdevice;
static int gnutar_no_unquote;
static int gnutar_sparse;
static int gnutar_sparse_set = 0;
static GSList *normal_message = NULL;
static GSList *ignore_message = NULL;
static GSList *strange_message = NULL;
static char   *exit_handling;
static int     exit_value[256];
static FILE   *mesgstream = NULL;
static int     amgtar_exit_value = 0;

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
    {"gnutar-path"     , 1, NULL, 10},
    {"gnutar-listdir"  , 1, NULL, 11},
    {"one-file-system" , 1, NULL, 12},
    {"sparse"          , 1, NULL, 13},
    {"atime-preserve"  , 1, NULL, 14},
    {"check-device"    , 1, NULL, 15},
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
    {"acls"            , 1, NULL, 30},
    {"selinux"         , 1, NULL, 31},
    {"xattrs"          , 1, NULL, 32},
    {"command-options" , 1, NULL, 33},
    {"include-list-glob", 1, NULL, 34},
    {"exclude-list-glob", 1, NULL, 35},
    {"verbose"          , 1, NULL, 36},
    {"ignore-zeros"     , 1, NULL, 37},
    {"amfeatures"       , 1, NULL, 38},
    {"recover-dump-state-file", 1, NULL, 39},
    {"dar"                    , 1, NULL, 40},
    {"state-stream"           , 1, NULL, 41},
    {"target"           , 1, NULL, 42},
    {NULL, 0, NULL, 0}
};

static message_t *
amgtar_print_message(
    message_t *message)
{
    if (strcasecmp(command, "selfcheck") == 0 ||
	strcasecmp(command, "discover") == 0) {
	return print_message(message);
    }
    if (message_get_severity(message) <= MSG_INFO) {
	if (g_str_equal(command, "estimate")) {
	    fprintf(stdout, "OK %s\n", get_message(message));
	} else if (g_str_equal(command, "backup")) {
	    fprintf(mesgstream, "| %s\n", get_message(message));
	} else {
	    fprintf(stdout, "%s\n", get_message(message));
	}
    } else {
	amgtar_exit_value = 1;
	if (g_str_equal(command, "estimate")) {
	    fprintf(stdout, "ERROR %s\n", get_message(message));
	} else if (g_str_equal(command, "backup")) {
	    fprintf(mesgstream, "sendbackup: error [%s]\n", get_message(message));
	} else {
	    fprintf(stdout, "%s\n", get_message(message));
	}
    }
    return message;
}

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
    application_argument_t argument;
    int i;
    char *gnutar_onefilesystem_value = NULL;
    char *gnutar_sparse_value = NULL;
    char *gnutar_atimepreserve_value = NULL;
    char *gnutar_checkdevice_value = NULL;
    char *gnutar_no_unquote_value = NULL;
    char *gnutar_dar_value = NULL;

#ifdef GNUTAR
    gnutar_path = g_strdup(GNUTAR);
#else
    gnutar_path = NULL;
#endif
    gnutar_listdir = NULL;
    gnutar_target = NULL;
    gnutar_onefilesystem = 1;
    gnutar_atimepreserve = 1;
    gnutar_acls = 0;
    gnutar_selinux = 0;
    gnutar_xattrs = 0;
    gnutar_checkdevice = 1;
    gnutar_sparse = 1;
    gnutar_no_unquote = 0;
    exit_handling = NULL;

    /* initialize */

    glib_init();

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda");

    if (argc < 2) {
        printf("ERROR no command given to amgtar\n");
        error(_("No command given to amgtar"));
    }

    /* drop root privileges */
    if (!set_root_privs(0)) {
	if (g_str_equal(argv[1], "selfcheck")) {
	    printf("ERROR amgtar must be run setuid root\n");
	}
	error(_("amgtar must be run setuid root"));
    }

    set_pname("amgtar");
    set_pcomponent("application");
    set_pmodule("amgtar");

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

    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);

    //check_running_as(RUNNING_AS_DUMPUSER_PREFERRED);
    //root for amrecover
    //RUNNING_AS_CLIENT_LOGIN from selfcheck, sendsize, sendbackup

    /* parse argument */
    command = argv[1];

    if (strcasecmp(command,"selfcheck") == 0) {
	fprintf(stdout, "MESSAGE JSON\n");
    }
    argument.config     = NULL;
    argument.host       = NULL;
    argument.message    = 0;
    argument.collection = 0;
    argument.calcsize   = 0;
    argument.tar_blocksize = NULL;
    argument.level      = NULL;
    argument.command_options = NULL;
    argument.include_list_glob = NULL;
    argument.exclude_list_glob = NULL;
    argument.verbose = 0;
    argument.ignore_zeros = 1;
    argument.amfeatures = NULL;
    argument.recover_dump_state_file = NULL;
    argument.dar        = FALSE;
    argument.state_stream = -1;
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
	case 10: amfree(gnutar_path);
		 gnutar_path = g_strdup(optarg);
		 break;
	case 11: amfree(gnutar_listdir);
		 gnutar_listdir = g_strdup(optarg);
		 break;
	case 12: amfree(gnutar_onefilesystem_value);
		 gnutar_onefilesystem_value = g_strdup(optarg);
		 break;
	case 13: amfree(gnutar_sparse_value);
		 gnutar_sparse_value = g_strdup(optarg);
		 break;
	case 14: amfree(gnutar_atimepreserve_value);
		 gnutar_atimepreserve_value = g_strdup(optarg);
		 break;
	case 15: amfree(gnutar_checkdevice_value);
		 gnutar_checkdevice_value = g_strdup(optarg);
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
	case 22: amfree(gnutar_target);
		 gnutar_target = g_strdup(optarg);
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
	case 29: amfree(gnutar_no_unquote_value);
		 gnutar_no_unquote_value = g_strdup(optarg);
		 break;
        case 30: if (strcasecmp(optarg, "YES") == 0)
                   gnutar_acls = 1;
                 break;
        case 31: if (strcasecmp(optarg, "YES") == 0)
                   gnutar_selinux = 1;
                 break;
        case 32: if (strcasecmp(optarg, "YES") == 0)
                   gnutar_xattrs = 1;
                 break;
	case 33: argument.command_options =
			g_slist_append(argument.command_options,
				       g_strdup(optarg));
		 break;
	case 34: amfree(argument.include_list_glob);
		 argument.include_list_glob = g_strdup(optarg);
		 break;
	case 35: amfree(argument.exclude_list_glob);
		 argument.exclude_list_glob = g_strdup(optarg);
		 break;
	case 36: if (strcasecmp(optarg, "YES") == 0)
		     argument.verbose = 1;
		 break;
	case 37: if (strcasecmp(optarg, "YES") != 0)
		     argument.ignore_zeros = 0;
		 break;
	case 38: amfree(argument.amfeatures);
		 argument.amfeatures = am_string_to_feature(optarg);
		 break;
	case 39: amfree(argument.recover_dump_state_file);
		 argument.recover_dump_state_file = g_strdup(optarg);
		 break;
	case 40: amfree(gnutar_dar_value);
		 gnutar_dar_value = g_strdup(optarg);
		 break;
	case 41: argument.state_stream = atoi(optarg);
		 break;
	case 42: amfree(gnutar_target);
		 gnutar_target = g_strdup(optarg);
		 break;
	case ':':
	case '?':
		break;
	}
    }

    if ((g_str_equal(command, "backup") ||
	 g_str_equal(command, "restore")) && argument.state_stream >= 0) {
	g_debug("state_stream: %d", argument.state_stream);
	safe_fd3(3, 2, dbfd(), argument.state_stream);
    } else {
	safe_fd2(3, 2, dbfd());
    }

    if (g_str_equal(command, "backup")) {
	mesgstream = fdopen(3, "w");
	if (!mesgstream) {
	    error(_("error mesgstream(%d): %s\n"), 3, strerror(errno));
	}
    }

    if (!argument.dle.disk && argument.dle.device)
	argument.dle.disk = g_strdup(argument.dle.device);
    if (!argument.dle.device && argument.dle.disk)
	argument.dle.device = g_strdup(argument.dle.disk);
    if (!argument.dle.disk && !argument.dle.device) {
	argument.dle.disk = g_strdup("no disk");
	argument.dle.device = g_strdup("no device");
    }
    if (!argument.host)
	argument.host = g_strdup("no host");

    if (gnutar_onefilesystem_value) {
	if (strcasecmp(gnutar_onefilesystem_value, "NO") == 0) {
	    gnutar_onefilesystem = 0;
	} else if (strcasecmp(gnutar_onefilesystem_value, "YES") == 0) {
	    gnutar_onefilesystem = 1;
	} else {
	    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700007, MSG_ERROR, 4,
			"value", gnutar_onefilesystem_value,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

    if (gnutar_sparse_value) {
	if (strcasecmp(gnutar_sparse_value, "NO") == 0) {
	    gnutar_sparse = 0;
	} else if (strcasecmp(gnutar_sparse_value, "YES") == 0) {
	    gnutar_sparse = 1;
	} else {
	    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700008, MSG_ERROR, 4,
			"value", gnutar_sparse_value,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

    if (gnutar_atimepreserve_value) {
	if (strcasecmp(gnutar_atimepreserve_value, "NO") == 0) {
	    gnutar_atimepreserve = 0;
	} else if (strcasecmp(gnutar_atimepreserve_value, "YES") == 0) {
	    gnutar_atimepreserve = 1;
	} else {
	    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700009, MSG_ERROR, 4,
			"value", gnutar_atimepreserve_value,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

    if (gnutar_checkdevice_value) {
	if (strcasecmp(gnutar_checkdevice_value, "NO") == 0) {
	    gnutar_checkdevice = 0;
	} else if (strcasecmp(gnutar_checkdevice_value, "YES") == 0) {
	    gnutar_checkdevice = 1;
	} else {
	    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700010, MSG_ERROR, 4,
			"value", gnutar_checkdevice_value,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

    if (gnutar_no_unquote_value) {
	if (strcasecmp(gnutar_no_unquote_value, "NO") == 0) {
	    gnutar_no_unquote = 0;
	} else if (strcasecmp(gnutar_no_unquote_value, "YES") == 0) {
	    gnutar_no_unquote = 1;
	} else {
	    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700011, MSG_ERROR, 4,
			"value", gnutar_no_unquote_value,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

    if (gnutar_dar_value) {
	if (strcasecmp(gnutar_dar_value, "NO") == 0) {
	    argument.dar = FALSE;
	} else if (strcasecmp(gnutar_dar_value, "YES") == 0) {
	    argument.dar = TRUE;
	} else {
	    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700015, MSG_ERROR, 4,
			"property_value", optarg,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

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

    if (!gnutar_listdir) {
	gnutar_listdir = g_strdup(getconf_str(CNF_GNUTAR_LIST_DIR));
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

    if (strlen(gnutar_listdir) == 0)
	amfree(gnutar_listdir);

    if (gnutar_path) {
	dbprintf("GNUTAR-PATH %s\n", gnutar_path);
    } else {
	dbprintf("GNUTAR-PATH is not set\n");
    }
    if (gnutar_listdir) {
	    dbprintf("GNUTAR-LISTDIR %s\n", gnutar_listdir);
    } else {
	dbprintf("GNUTAR-LISTDIR is not set\n");
    }
    if (gnutar_target) {
	dbprintf("TARGET %s\n", gnutar_target);
    }
    dbprintf("ONE-FILE-SYSTEM %s\n", gnutar_onefilesystem? "yes":"no");
    dbprintf("SPARSE %s\n", gnutar_sparse? "yes":"no");
    dbprintf("NO-UNQUOTE %s\n", gnutar_no_unquote? "yes":"no");
    dbprintf("ATIME-PRESERVE %s\n", gnutar_atimepreserve? "yes":"no");
    dbprintf("ACLS %s\n", gnutar_acls? "yes":"no");
    dbprintf("SELINUX %s\n", gnutar_selinux? "yes":"no");
    dbprintf("XATTRS %s\n", gnutar_xattrs? "yes":"no");
    dbprintf("CHECK-DEVICE %s\n", gnutar_checkdevice? "yes":"no");
    {
	amregex_t *rp;
	for (rp = re_table; rp->regex != NULL; rp++) {
	    switch (rp->typ) {
		case DMP_NORMAL : dbprintf("NORMAL %s\n", rp->regex); break;
		case DMP_IGNORE : dbprintf("IGNORE %s\n", rp->regex); break;
		case DMP_STRANGE: dbprintf("STRANGE %s\n", rp->regex); break;
		case DMP_SIZE   : dbprintf("SIZE %s\n", rp->regex); break;
		case DMP_ERROR  : dbprintf("ERROR %s\n", rp->regex); break;
	    }
	}
    }

    if (g_str_equal(command, "support")) {
	amgtar_support(&argument);
    } else if (g_str_equal(command, "selfcheck")) {
	amgtar_selfcheck(&argument);
    } else if (g_str_equal(command, "estimate")) {
	amgtar_estimate(&argument);
    } else if (g_str_equal(command, "backup")) {
	amgtar_backup(&argument);
    } else if (g_str_equal(command, "restore")) {
	amgtar_restore(&argument);
    } else if (g_str_equal(command, "validate")) {
	amgtar_validate(&argument);
    } else if (g_str_equal(command, "index")) {
	amgtar_index(&argument);
    } else if (g_str_equal(command, "discover")) {
       amgtar_discover(&argument);
    } else {
	dbprintf("Unknown command `%s'.\n", command);
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

    return amgtar_exit_value;
}

static char *validate_command_options(
    application_argument_t *argument)
{
    GSList    *copt;

    for (copt = argument->command_options; copt != NULL; copt = copt->next) {
	char *opt = (char *)copt->data;

	if (g_str_has_prefix(opt, "--rsh-command") ||
	    g_str_has_prefix(opt,"--to-command") ||
	    g_str_has_prefix(opt,"--info-script") ||
	    g_str_has_prefix(opt,"--new-volume-script") ||
	    g_str_has_prefix(opt,"--rmt-command") ||
	    g_str_has_prefix(opt,"--use-compress-program")) {
	    return opt;
	}
    }

    return NULL;
}

static void
amgtar_support(
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
    fprintf(stdout, "MESSAGE-SELFCHECK-JSON YES\n");
    fprintf(stdout, "MESSAGE-XML NO\n");
    fprintf(stdout, "RECORD YES\n");
    fprintf(stdout, "INCLUDE-FILE YES\n");
    fprintf(stdout, "INCLUDE-LIST YES\n");
    fprintf(stdout, "INCLUDE-LIST-GLOB YES\n");
    fprintf(stdout, "INCLUDE-OPTIONAL YES\n");
    fprintf(stdout, "EXCLUDE-FILE YES\n");
    fprintf(stdout, "EXCLUDE-LIST YES\n");
    fprintf(stdout, "EXCLUDE-LIST-GLOB YES\n");
    fprintf(stdout, "EXCLUDE-OPTIONAL YES\n");
    fprintf(stdout, "COLLECTION NO\n");
    fprintf(stdout, "MULTI-ESTIMATE YES\n");
    fprintf(stdout, "CALCSIZE YES\n");
    fprintf(stdout, "CLIENT-ESTIMATE YES\n");
    fprintf(stdout, "DISCOVER YES\n");
    fprintf(stdout, "AMFEATURES YES\n");
    fprintf(stdout, "RECOVER-DUMP-STATE-FILE YES\n");
    fprintf(stdout, "DAR YES\n");
    fprintf(stdout, "STATE-STREAM YES\n");
}

static void
amgtar_discover(
    application_argument_t *argument )
{
    DIR *dir;
    struct dirent *dp;
    char *full_path;
    size_t path_len;
    size_t path_alloc;
    struct stat stat_buf;
    char *file_type;
    gboolean reported_entries = FALSE;

    if (argument->dle.disk) {
	if ((dir = opendir(argument->dle.disk)) == NULL) {
            delete_message(amgtar_print_message(build_message(
				AMANDA_FILE, __LINE__, 3700012, MSG_ERROR, 2,
				"diskname", argument->dle.disk,
				"errno", errno)));
	    return;
	}

	path_len = strlen(argument->dle.disk);
	path_alloc = path_len + 258;
	full_path = g_malloc(path_alloc);
	strcpy(full_path, argument->dle.disk);
	if (full_path[path_len-1] != '/')  {
	    strcat(full_path, "/");
            path_len++;
	}

        while ((dp = readdir (dir)) != NULL) {
            if (strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
                continue;

            while (path_len + strlen(dp->d_name) + 1 > path_alloc) {
                path_alloc *= 2;
                full_path = g_realloc(full_path, path_alloc);
            }
            strcpy(full_path + path_len, dp->d_name);

            if (lstat(full_path, &stat_buf) != 0) {
                delete_message(amgtar_print_message(build_message(
				AMANDA_FILE, __LINE__, 3700013, MSG_ERROR, 2,
				"diskname", argument->dle.disk,
				"errno", errno)));
                continue;
            }
            file_type = "unknown";
            if (S_ISDIR(stat_buf.st_mode)) {
                file_type = "dir";
            } else if (S_ISCHR(stat_buf.st_mode)) {
                file_type = "char";
            } else if (S_ISBLK(stat_buf.st_mode)) {
                file_type = "block";
            } else if (S_ISREG(stat_buf.st_mode)) {
                file_type = "regular";
            } else if (S_ISSOCK(stat_buf.st_mode)) {
                file_type = "socket";
            } else if (S_ISLNK(stat_buf.st_mode)) {
                file_type = "link";
            } else if (S_ISFIFO(stat_buf.st_mode)) {
                file_type = "fifo";
            }
            delete_message(amgtar_print_message(build_message(
                               AMANDA_FILE, __LINE__, 3100005, MSG_INFO, 4,
                               "file_type", file_type,
                               "full_path", full_path,
                               "diskname", argument->dle.disk,
			       "dle_name", dp->d_name)));
	    reported_entries = TRUE;
         }
         closedir(dir);
	 g_free(full_path);
    }

    if (!reported_entries) {
        delete_message(amgtar_print_message(build_message(
                               AMANDA_FILE, __LINE__, 3100006, MSG_ERROR, 1,
                               "diskname", argument->dle.disk)));
    }
}

static void
amgtar_selfcheck(
    application_argument_t *argument)
{
    char *option;
    messagelist_t mlist = NULL;
    messagelist_t mesglist = NULL;
    char *dirname;

    if (argument->dle.disk) {
	delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700000, MSG_INFO, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }

    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700001, MSG_INFO, 4,
			"version", VERSION,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));

    if (gnutar_target) {
	dirname = gnutar_target;
    } else {
	dirname = argument->dle.device;
    }

    amgtar_build_exinclude(&argument->dle, NULL, NULL, NULL, NULL,
                           dirname, &mlist);
    for (mesglist = mlist; mesglist != NULL; mesglist = mesglist->next){
	message_t *message = mesglist->data;
	if (message_get_severity(message) > MSG_INFO)
	    amgtar_print_message(message);
	delete_message(message);
    }
    g_slist_free(mlist);

    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700004, MSG_INFO, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));

    if ((option = validate_command_options(argument))) {
	delete_message(amgtar_print_message(build_message(
		AMANDA_FILE, __LINE__, 3700014, MSG_ERROR, 4,
		"disk", argument->dle.disk,
		"device", argument->dle.device,
		"hostname", argument->host,
		"command-options", option)));
    }

    if (gnutar_path) {
	char *gnutar_realpath = NULL;
	message_t *message;
	if ((message = check_exec_for_suid_message("GNUTAR_PATH", gnutar_path, &gnutar_realpath))) {
	    delete_message(amgtar_print_message(message));
	} else {
	    message = amgtar_print_message(check_file_message(gnutar_path, X_OK));
	    if (message && message_get_severity(message) <= MSG_INFO) {
		char *gtar_version;
		GPtrArray *argv_ptr = g_ptr_array_new();

		g_ptr_array_add(argv_ptr, gnutar_path);
		g_ptr_array_add(argv_ptr, "--version");
		g_ptr_array_add(argv_ptr, NULL);

		gtar_version = get_first_line(argv_ptr);
		if (gtar_version) {
		    char *gv;
		    for (gv = gtar_version; *gv && !g_ascii_isdigit(*gv); gv++);
		    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700002, MSG_INFO, 4,
			"gtar-version", gv,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
		} else {
		    delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700003, MSG_ERROR, 4,
			"gtar-path", gnutar_path,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
		}

		g_ptr_array_free(argv_ptr, TRUE);
		amfree(gtar_version);
	    }
	    if (message)
		delete_message(message);
	}
	if (gnutar_realpath) g_free(gnutar_realpath);
    } else {
	delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700005, MSG_ERROR, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }
    if (gnutar_listdir && strlen(gnutar_listdir) == 0)
	gnutar_listdir = NULL;
    if (gnutar_listdir) {
	delete_message(amgtar_print_message(check_dir_message(gnutar_listdir, R_OK|W_OK)));
    } else {
	delete_message(amgtar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3700006, MSG_ERROR, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }

    set_root_privs(1);
    if (gnutar_target) {
	delete_message(amgtar_print_message(check_dir_message(gnutar_target, R_OK)));
    } else if (argument->dle.device) {
	delete_message(amgtar_print_message(check_dir_message(argument->dle.device, R_OK)));
    }
    if (argument->calcsize) {
	char *calcsize = g_strjoin(NULL, amlibexecdir, "/", "calcsize", NULL);
	delete_message(amgtar_print_message(check_file_message(calcsize, X_OK)));
	delete_message(amgtar_print_message(check_suid_message(calcsize)));
	amfree(calcsize);
    }
    set_root_privs(0);
}

static void
amgtar_estimate(
    application_argument_t *argument)
{
    char      *incrname = NULL;
    GPtrArray *argv_ptr;
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
    char      *option;
    char      *gnutar_realpath = NULL;
    message_t *message;

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

    if ((option = validate_command_options(argument))) {
	fprintf(stderr, "ERROR Invalid '%s' COMMAND-OPTIONS\n", option);
	error("Invalid '%s' COMMAND-OPTIONS", option);
    }

    if (argument->calcsize) {
	char *dirname;
	int   nb_exclude;
	int   nb_include;
	messagelist_t mlist = NULL;
	messagelist_t mesglist = NULL;

	if (gnutar_target) {
	    dirname = gnutar_target;
	} else {
	    dirname = argument->dle.device;
	}

	amgtar_build_exinclude(&argument->dle,
			       &nb_exclude, &file_exclude,
			       &nb_include, &file_include, dirname, &mlist);
	for (mesglist = mlist; mesglist != NULL; mesglist = mesglist->next){
	    message_t *message = mesglist->data;
	    if (message_get_severity(message) > MSG_INFO)
		fprintf(stdout, "ERROR %s\n", get_message(message));
	    delete_message(message);
	}
	g_slist_free(mlist);
	mlist = NULL;

	run_calcsize(argument->config, "GNUTAR", argument->dle.disk, dirname,
		     argument->level, file_exclude, file_include);

	if (argument->verbose == 0) {
	    if (file_exclude)
		unlink(file_exclude);
	    if (file_include)
		unlink(file_include);
        }
	amfree(file_exclude);
	amfree(file_include);
	return;
    }

    if (!gnutar_path) {
	errmsg = g_strdup(_("GNUTAR-PATH not defined"));
	goto common_error;
    }

    if (!gnutar_listdir) {
	errmsg = g_strdup(_("GNUTAR-LISTDIR not defined"));
	goto common_error;
    }

    if ((message = check_exec_for_suid_message("GNUTAR_PATH", gnutar_path, &gnutar_realpath))) {
	errmsg = g_strdup(get_message(message));
	delete_message(message);
	goto common_error;
    }

    qdisk = quote_string(argument->dle.disk);
    for (levels = argument->level; levels != NULL; levels = levels->next) {
	messagelist_t mlist = NULL;
	messagelist_t mesglist = NULL;
	level = GPOINTER_TO_INT(levels->data);
	incrname = amgtar_get_incrname(argument, level, stdout, CMD_ESTIMATE);
	argv_ptr = amgtar_build_argv(gnutar_realpath,
				     argument, incrname, &file_exclude,
				     &file_include, CMD_ESTIMATE, &mlist);
	for (mesglist = mlist; mesglist != NULL; mesglist = mesglist->next){
	    message_t *message = mesglist->data;
	    if (message_get_severity(message) > MSG_INFO)
		fprintf(stdout, "ERROR %s\n", get_message(message));
	    delete_message(message);
	}
	g_slist_free(mlist);
	mlist = NULL;

	start_time = curclock();

	if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	    errmsg = g_strdup_printf(_("Cannot access /dev/null : %s"),
				strerror(errno));
	    goto common_exit;
	}

	tarpid = pipespawnv(gnutar_realpath, STDERR_PIPE, 1,
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

	while (fgets(line, sizeof(line), dumpout) != NULL) {
	    dbprintf("%s", line);
	}

	dbprintf(".....\n");
	dbprintf(_("estimate time for %s level %d: %s\n"),
		 qdisk,
		 level,
		 walltime_str(timessub(curclock(), start_time)));
	if(size == (off_t)-1) {
	    errmsg = g_strdup_printf(_("no size line match in %s output"), gnutar_realpath);
	    dbprintf(_("%s for %s\n"), errmsg, qdisk);
	    dbprintf(".....\n");
	} else if(size == (off_t)0 && argument->level == 0) {
	    dbprintf(_("possible %s problem -- is \"%s\" really empty?\n"),
		     gnutar_realpath, argument->dle.disk);
	    dbprintf(".....\n");
	}
	dbprintf(_("estimate size for %s level %d: %lld KB\n"),
		 qdisk,
		 level,
		 (long long)size);

	(void)kill(-tarpid, SIGTERM);

	dbprintf(_("waiting for %s \"%s\" child\n"), gnutar_realpath, qdisk);
	waitpid(tarpid, &wait_status, 0);
	if (WIFSIGNALED(wait_status)) {
	    strbuf = g_string_new(errmsg);
	    g_string_append_printf(strbuf, "%s terminated with signal %d: see %s",
                gnutar_realpath, WTERMSIG(wait_status), dbfn());
	    g_free(errmsg);
            errmsg = g_string_free(strbuf, FALSE);
	} else if (WIFEXITED(wait_status)) {
	    if (exit_value[WEXITSTATUS(wait_status)] == 1) {
                strbuf = g_string_new(errmsg);
                g_string_append_printf(strbuf, "%s exited with status %d: see %s",
                    gnutar_realpath, WEXITSTATUS(wait_status), dbfn());
		g_free(errmsg);
                errmsg = g_string_free(strbuf, FALSE);
	    } else {
		/* Normal exit */
	    }
	} else {
	    errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
				gnutar_realpath, dbfn());
	}
	dbprintf(_("after %s %s wait\n"), gnutar_realpath, qdisk);

common_exit:
	if (errmsg) {
	    dbprintf("%s", errmsg);
	    fprintf(stdout, "ERROR %s\n", errmsg);
	    amfree(errmsg);
	}

	unlink(incrname);

	if (argument->verbose == 0) {
	    if (file_exclude)
		unlink(file_exclude);
	    if (file_include)
		unlink(file_include);
        }

	g_ptr_array_free_full(argv_ptr);
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
    amfree(gnutar_realpath);
    return;

common_error:
    qerrmsg = quote_string(errmsg);
    amfree(qdisk);
    dbprintf("%s", errmsg);
    fprintf(stdout, "ERROR %s\n", qerrmsg);
    amfree(file_exclude);
    amfree(file_include);
    amfree(errmsg);
    amfree(qerrmsg);
    amfree(incrname);
    amfree(gnutar_realpath);
    return;
}

static void
amgtar_backup(
    application_argument_t *argument)
{
    int         dumpin;
    char      *qdisk;
    char      *incrname;
    char       line[32768];
    amregex_t *rp;
    off_t      dump_size = -1;
    char      *type;
    char       startchr;
    int        dataf = 1;
    int        indexf = 4;
    int        outf;
    FILE      *indexstream = NULL;
    FILE      *outstream;
    char      *errmsg = NULL;
    amwait_t   wait_status;
    GPtrArray *argv_ptr;
    int        tarpid;
    char      *file_exclude;
    char      *file_include;
    char      *option;
    char      *gnutar_realpath = NULL;
    messagelist_t mlist = NULL;
    messagelist_t mesglist = NULL;
    message_t *message;

    if (!gnutar_path) {
        fprintf(mesgstream, "sendbackup:: error [GNUTAR-PATH not defined]\n");
	exit(1);
    }
    if (!gnutar_listdir) {
        fprintf(mesgstream, "sendbackup:: error [GNUTAR-LISTDIR not defined]\n");
	exit(1);
    }

    if (!argument->level) {
        fprintf(mesgstream, "sendbackup:: error [No level argument]\n");
	exit(1);
    }
    if (!argument->dle.disk) {
        fprintf(mesgstream, "sendbackup:: error [No disk argument]\n");
	exit(1);
    }
    if (!argument->dle.device) {
        fprintf(mesgstream, "sendbackup:: error [No device argument]\n");
	exit(1);
    }

    if ((message = check_exec_for_suid_message("GNUTAR_PATH", gnutar_path, &gnutar_realpath))) {
        fprintf(mesgstream, "sendbackup: error [%s]\n", get_message(message));
	delete_message(message);
	exit(1);
    }

    if ((option = validate_command_options(argument))) {
	fprintf(mesgstream, "? Invalid '%s' COMMAND-OPTIONS\n", option);
	error("Invalid '%s' COMMAND-OPTIONS", option);
    }

    qdisk = quote_string(argument->dle.disk);

    incrname = amgtar_get_incrname(argument,
				   GPOINTER_TO_INT(argument->level->data),
				   mesgstream, CMD_BACKUP);
    argv_ptr = amgtar_build_argv(gnutar_realpath,
				 argument, incrname, &file_exclude,
				 &file_include, CMD_BACKUP, &mlist);
    for (mesglist = mlist; mesglist != NULL; mesglist = mesglist->next){
	message_t *message = mesglist->data;
	if (message_get_severity(message) <= MSG_INFO) {
	    fprintf(mesgstream, "| %s\n", get_message(message));
	} else {
	    fprintf(mesgstream, "? %s\n", get_message(message));
	}
	delete_message(message);
    }
    g_slist_free(mlist);

    tarpid = pipespawnv(gnutar_realpath, STDIN_PIPE|STDERR_PIPE, 1,
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
	if (strncmp(line, "block ", 6) == 0) { /* filename */
	    off_t block_no = g_ascii_strtoull(line+6, NULL, 0);
	    char *filename = strchr(line, ':');
	    if (filename) {
		filename += 2;
		if (*filename == '.' && *(filename+1) == '/') {
		    if (argument->dle.create_index) {
			fprintf(indexstream, "%s\n", &filename[1]); /* remove . */
		    }
		    if (argument->state_stream != -1) {
			char *s = g_strdup_printf("%lld %s\n",
					 (long long)block_no, &filename[1]);
			guint a = full_write(argument->state_stream, s, strlen(s));
			if (a < strlen(s)) {
			    g_debug("Failed to write to the state stream: %s",
				    strerror(errno));
			}
			g_free(s);
		    } else if (argument->amfeatures &&
			       am_has_feature(argument->amfeatures,
					      fe_sendbackup_state)) {
			fprintf(mesgstream, "sendbackup: state %lld %s\n",
			        (long long)block_no, &filename[1]);
		    }
		}
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
	    dbprintf("%3d: %7s(%c): %s\n", rp->srcline, type, startchr, line);
	    fprintf(mesgstream,"%c %s\n", startchr, line);
        }
    }
    fclose(outstream);

    waitpid(tarpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = g_strdup_printf(_("%s terminated with signal %d: see %s"),
			    gnutar_realpath, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (exit_value[WEXITSTATUS(wait_status)] == 1) {
	    errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
				gnutar_realpath, WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
			    gnutar_realpath, dbfn());
    }
    dbprintf(_("after %s %s wait\n"), gnutar_realpath, qdisk);
    dbprintf(_("amgtar: %s: pid %ld\n"), gnutar_realpath, (long)tarpid);
    if (errmsg) {
	dbprintf("%s", errmsg);
	g_fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
    }

    if (!errmsg && strlen(incrname) > 4) {
	if (argument->dle.record) {
	    char *nodotnew;
	    nodotnew = g_strdup(incrname);
	    nodotnew[strlen(nodotnew)-4] = '\0';
	    if (rename(incrname, nodotnew)) {
		dbprintf(_("%s: warning [renaming %s to %s: %s]\n"),
			 get_pname(), incrname, nodotnew, strerror(errno));
		g_fprintf(mesgstream, _("? warning [renaming %s to %s: %s]\n"),
			  incrname, nodotnew, strerror(errno));
	    }
	    amfree(nodotnew);
	} else {
	    if (unlink(incrname) == -1) {
		dbprintf(_("%s: warning [unlink %s: %s]\n"),
			 get_pname(), incrname, strerror(errno));
		g_fprintf(mesgstream, _("? warning [unlink %s: %s]\n"),
			  incrname, strerror(errno));
	    }
	}
    }

    dbprintf("sendbackup: size %lld\n", (long long)dump_size);
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
    amfree(incrname);
    amfree(qdisk);
    amfree(errmsg);
    amfree(gnutar_realpath);
    g_ptr_array_free_full(argv_ptr);
}

static void
amgtar_restore(
    application_argument_t *argument)
{
    GPtrArray  *argv_ptr = g_ptr_array_new();
    GPtrArray  *include_array = g_ptr_array_new();
    char      **env;
    int         j;
    char       *e;
    char       *include_filename = NULL;
    char       *exclude_filename = NULL;
    int         tarpid;
    amwait_t    wait_status;
    int         exit_status = 0;
    char       *errmsg = NULL;
    FILE       *dar_file;
    char       *gnutar_realpath = NULL;

    if (!gnutar_path) {
	error(_("GNUTAR-PATH not defined"));
    }

    if (!check_exec_for_suid("GNUTAR_PATH", gnutar_path, NULL, &gnutar_realpath)) {
	error("'%s' binary is not secure", gnutar_path);
    }

    if (!security_allow_to_restore()) {
	error("The user is not allowed to restore files");
    }

    g_ptr_array_add(argv_ptr, g_strdup(gnutar_realpath));
    g_ptr_array_add(argv_ptr, g_strdup("--numeric-owner"));
    if (gnutar_no_unquote)
	g_ptr_array_add(argv_ptr, g_strdup("--no-unquote"));
    if (gnutar_acls)
	g_ptr_array_add(argv_ptr, g_strdup("--acls"));
    if (gnutar_selinux)
	g_ptr_array_add(argv_ptr, g_strdup("--selinux"));
    if (gnutar_xattrs)
	g_ptr_array_add(argv_ptr, g_strdup("--xattrs"));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    if (argument->ignore_zeros) {
	g_ptr_array_add(argv_ptr, g_strdup("--ignore-zeros"));
    }
    if (argument->tar_blocksize) {
	g_ptr_array_add(argv_ptr, g_strdup("--blocking-factor"));
	g_ptr_array_add(argv_ptr, g_strdup(argument->tar_blocksize));
    }
    g_ptr_array_add(argv_ptr, g_strdup("-xpGvf"));
    g_ptr_array_add(argv_ptr, g_strdup("-"));
    if (gnutar_target) {
	struct stat stat_buf;
	if(stat(gnutar_target, &stat_buf) != 0) {
	    fprintf(stderr, "can not stat directory %s: %s\n",
		    gnutar_target, strerror(errno));
	    exit(1);
	}
	if (!S_ISDIR(stat_buf.st_mode)) {
	    fprintf(stderr,"%s is not a directory\n", gnutar_target);
	    exit(1);
	}
	if (access(gnutar_target, W_OK) != 0) {
	    fprintf(stderr, "Can't write to %s: %s\n",
		    gnutar_target, strerror(errno));
	    exit(1);
	}
	g_ptr_array_add(argv_ptr, g_strdup("--directory"));
	g_ptr_array_add(argv_ptr, g_strdup(gnutar_target));
    }

    g_ptr_array_add(argv_ptr, g_strdup("--wildcards"));
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
	exclude_filename= g_strjoin(NULL, AMANDA_TMPDIR, "/", "exclude-",
				    sdisk,  NULL);
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

    if (argument->exclude_list_glob) {
	g_ptr_array_add(argv_ptr, g_strdup("--exclude-from"));
	g_ptr_array_add(argv_ptr, g_strdup(argument->exclude_list_glob));
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
		escaped = escape_tar_glob(line, &in_argv);
		g_ptr_array_add(include_array, g_strdup(line));
		if (in_argv) {
		    g_ptr_array_add(argv_include, escaped);
		} else {
		    fprintf(include,"%s\n", escaped);
		    entry_in_include++;
		    amfree(escaped);
		}
	    }
	    fclose(include_list);
	}

	if (argument->dle.include_file) {
	    sle_t *slif;

	    for (slif = argument->dle.include_file->first; slif != NULL; slif = slif->next) {
		char *escaped = escape_tar_glob(slif->name, &in_argv);
		g_ptr_array_add(include_array, slif->name);
		if (in_argv) {
		    g_ptr_array_add(argv_include, escaped);
		} else {
		    fprintf(include,"%s\n", escaped);
		    entry_in_include++;
		    amfree(escaped);
		}
	    }
	}

	for (j=1; j< argument->argc; j++) {
	    char *escaped = escape_tar_glob(argument->argv[j], &in_argv);
	    g_ptr_array_add(include_array, argument->argv[j]);
	    if (in_argv) {
		g_ptr_array_add(argv_include, escaped);
	    } else {
		fprintf(include,"%s\n", escaped);
		entry_in_include++;
		amfree(escaped);
	    }
	}
	fclose(include);

	if (entry_in_include) {
	    g_ptr_array_add(argv_ptr, g_strdup("--files-from"));
	    g_ptr_array_add(argv_ptr, include_filename);
	}

	if (argument->include_list_glob) {
	    g_ptr_array_add(argv_ptr, g_strdup("--files-from"));
	    g_ptr_array_add(argv_ptr, g_strdup(argument->include_list_glob));
	}

	for (i = 0; i < argv_include->len; i++) {
	    g_ptr_array_add(argv_ptr, (char *)g_ptr_array_index(argv_include,i));
	}
	amfree(sdisk);
    }
    g_ptr_array_add(argv_ptr, NULL);

    if (argument->dar) {
	int dar_fd = argument->state_stream;
	if (dar_fd == -1) dar_fd = 3;
	dar_file = fdopen(dar_fd, "w");
	if (!dar_file) {
	    int save_errno = errno;
	    fprintf(stderr, "Can't fdopen the DAR file (fd %d): %s\n",
		    dar_fd, strerror(save_errno));
	    g_debug("Can't fdopen the DAR file (fd %d): %s",
		    dar_fd, strerror(save_errno));
	   exit(1);
	}
	if (argument->recover_dump_state_file &&
		   include_array->len > 0) {
	    char  line[32768];
	    FILE *recover_state_file = fopen(argument->recover_dump_state_file,
					     "r");
	    if (!recover_state_file) {
		fprintf(dar_file,"DAR 0:-1\n");
		g_debug("full dar: 0:-1");
	    } else {
		int   previous_block = -1;

		while (fgets(line, 32768, recover_state_file) != NULL) {
		    off_t     block_no = g_ascii_strtoull(line, NULL, 0);
		    gboolean  match    = FALSE;
		    char     *filename = strchr(line, ' ');
		    char     *ii;
		    guint     i;

		    if (!filename)
			continue;

		    filename++;
		    if (filename[strlen(filename)-1] == '\n')
			filename[strlen(filename)-1] = '\0';

		    g_debug("recover_dump_state_file: %lld %s",
					 (long long)block_no, filename);
		    for (i = 0; i < include_array->len; i++) {
			size_t strlenii;

			ii = g_ptr_array_index(include_array, i);
			ii++; // remove leading '.'
			//g_debug("check %s : %s :", filename, ii);
			strlenii = strlen(ii);
			if (g_str_equal(filename, ii) == 1 ||
			    (strlenii < strlen(filename) &&
			     strncmp(filename, ii, strlenii) == 0 &&
			     filename[strlenii] == '/')) {
			    //g_debug("match %s", ii);
			    match = TRUE;
			    break;
			}
		    }

		    if (match) {
			if (previous_block < 0)
			    previous_block = block_no;
		    } else if (previous_block >= 0) {
			g_debug("restore block %lld (%lld) to %lld (%lld)",
				(long long)previous_block * 512,
				(long long)previous_block,
				(long long)block_no * 512 - 1,
				(long long)block_no);
			fprintf(dar_file, "DAR %lld:%lld\n",
				(long long)previous_block * 512,
				(long long)block_no * 512- 1);
			previous_block = -1;
		    }
		}
		fclose(recover_state_file);
		if (previous_block >= 0) {
		    g_debug("restore block %lld (%lld) to END",
			    (long long)previous_block * 512,
			    (long long)previous_block);
		    fprintf(dar_file, "DAR %lld:-1\n",
			    (long long)previous_block * 512);
		}
	    }
	} else {
	    fprintf(dar_file,"DAR 0:-1\n");
	    g_debug("full dar: 0:-1");
	}
	fflush(dar_file);
	fclose(dar_file);
    }

    debug_executing(argv_ptr);

    tarpid = fork();
    switch (tarpid) {
    case -1: error(_("%s: fork returned: %s"), get_pname(), strerror(errno));
    case 0:
	env = safe_env();
	become_root();
	execve(gnutar_realpath, (char **)argv_ptr->pdata, env);
	free_env(env);
	e = strerror(errno);
	error(_("error [exec %s: %s]"), gnutar_realpath, e);
	break;
    default: break;
    }

    waitpid(tarpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = g_strdup_printf(_("%s terminated with signal %d: see %s"),
                                 gnutar_realpath, WTERMSIG(wait_status), dbfn());
	exit_status = 1;
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) > 0) {
	    errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
				     gnutar_realpath, WEXITSTATUS(wait_status), dbfn());
	    exit_status = 1;
	} else {
	    /* Normal exit */
	    exit_status = 0;
	}
    } else {
	errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
				 gnutar_realpath, dbfn());
	exit_status = 1;
    }
    if (errmsg) {
	dbprintf("%s", errmsg);
	fprintf(stderr, "ERROR %s\n", errmsg);
	amfree(errmsg);
    }

    if (argument->verbose == 0) {
	if (exclude_filename)
	    unlink(exclude_filename);
	unlink(include_filename);
    }
    amfree(include_filename);
    amfree(exclude_filename);
    amfree(gnutar_realpath);
    exit(exit_status);
}

static void
amgtar_validate(
    application_argument_t *argument G_GNUC_UNUSED)
{
    char       *cmd = NULL;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    char      **env;
    char       *e;
    char        buf[32768];

    if (!gnutar_path) {
	dbprintf("GNUTAR-PATH not set; Piping to /dev/null\n");
	fprintf(stderr,"GNUTAR-PATH not set; Piping to /dev/null\n");
	goto pipe_to_null;
    }

    cmd = g_strdup(gnutar_path);
    g_ptr_array_add(argv_ptr, g_strdup(gnutar_path));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    g_ptr_array_add(argv_ptr, g_strdup("--ignore-zeros"));
    g_ptr_array_add(argv_ptr, g_strdup("-tf"));
    g_ptr_array_add(argv_ptr, g_strdup("-"));
    g_ptr_array_add(argv_ptr, NULL);

    debug_executing(argv_ptr);
    env = safe_env();
    execve(cmd, (char **)argv_ptr->pdata, env);
    e = strerror(errno);
    dbprintf("failed to execute %s: %s; Piping to /dev/null\n", cmd, e);
    fprintf(stderr,"failed to execute %s: %s; Piping to /dev/null\n", cmd, e);
    free_env(env);
pipe_to_null:
    while (read(0, buf, 32768) > 0) {
    }
    amfree(cmd);
}

static void
amgtar_index(
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

    if (!gnutar_path) {
	dbprintf("GNUTAR-PATH not set\n");
	fprintf(stderr,"GNUTAR-PATH not set\n");
	while (read(0, line, 32768) > 0) {
	}
	exit(1);
    }

    cmd = g_strdup(gnutar_path);
    g_ptr_array_add(argv_ptr, g_strdup(gnutar_path));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    g_ptr_array_add(argv_ptr, g_strdup("--ignore-zeros"));
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

    while (fgets(line, sizeof(line), indexstream) != NULL) {
	if (strlen(line) > 0 && line[strlen(line)-1] == '\n') {
	    /* remove trailling \n */
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
    dbprintf(_("amgtar: %s: pid %ld\n"), cmd, (long)tarpid);
    if (errmsg) {
	dbprintf("%s", errmsg);
	fprintf(stderr, "error [%s]\n", errmsg);
    }

    amfree(cmd);
}

static void
amgtar_build_exinclude(
    dle_t  *dle,
    int    *nb_exclude,
    char  **file_exclude,
    int    *nb_include,
    char  **file_include,
    char   *dirname,
    messagelist_t *mlist)
{
    int n_exclude = 0;
    int n_include = 0;
    char *exclude = NULL;
    char *include = NULL;

    if (dle->exclude_file) n_exclude += dle->exclude_file->nb_element;
    if (dle->exclude_list) n_exclude += dle->exclude_list->nb_element;
    if (dle->include_file) n_include += dle->include_file->nb_element;
    if (dle->include_list) n_include += dle->include_list->nb_element;

    if (n_exclude > 0) exclude = build_exclude(dle, mlist);
    if (n_include > 0) include = build_include(dle, dirname, mlist);

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
amgtar_get_incrname(
    application_argument_t *argument,
    int                     level,
    FILE                   *mesgstream,
    int                     command)
{
    char *basename = NULL;
    char *incrname = NULL;
    int   infd, outfd;
    ssize_t   nb;
    char *inputname = NULL;
    char *errmsg = NULL;
    char *buf;

    if (gnutar_listdir) {
	char number[NUM_STR_SIZE];
	int baselevel;
	char *sdisk = sanitise_filename(argument->dle.disk);

	basename = g_strjoin(NULL, gnutar_listdir,
			     "/",
			     argument->host,
			     sdisk,
			     NULL);
	amfree(sdisk);

	snprintf(number, sizeof(number), "%d", level);
	incrname = g_strjoin(NULL, basename, "_", number, ".new", NULL);
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
		snprintf(number, sizeof(number), "%d", baselevel);
		g_free(inputname);
		inputname = g_strconcat(basename, "_", number, NULL);
	    } else {
		g_free(inputname);
		inputname = g_strdup("/dev/null");
	    }
	    if ((infd = open(inputname, O_RDONLY)) == -1) {

		errmsg = g_strdup_printf(_("amgtar: error opening %s: %s"),
				     inputname, strerror(errno));
		dbprintf("%s\n", errmsg);
		if (baselevel < 0) {
		    if (command == CMD_ESTIMATE) {
			fprintf(mesgstream, "ERROR %s\n", errmsg);
		    } else {
			fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
		    }
		    exit(1);
		}
		amfree(errmsg);
	    }
	}

	/*
	 * Copy the previous listed incremental file to the new one.
	 */
	if ((outfd = open(incrname, O_WRONLY|O_CREAT, 0600)) == -1) {
	    errmsg = g_strdup_printf(_("error opening %s: %s"),
			         incrname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	    } else {
		fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
	    }
	    exit(1);
	}

	amfree(inputname);
	amfree(basename);

	while ((nb = read(infd, &buf, sizeof(buf))) > 0) {
	    if (full_write(outfd, &buf, (size_t)nb) < (size_t)nb) {
		errmsg = g_strdup_printf(_("writing to %s: %s"),
				     incrname, strerror(errno));
		dbprintf("%s\n", errmsg);
		aclose(infd);
		aclose(outfd);
		amfree(incrname);
		dbprintf("%s\n", errmsg);
		if (command == CMD_ESTIMATE) {
		    fprintf(mesgstream, "ERROR %s\n", errmsg);
		} else {
		    fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
		}
		amfree(errmsg);
		exit(1);
	    }
	}

	if (nb < 0) {
	    errmsg = g_strdup_printf(_("reading from %s: %s"),
			         inputname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    aclose(infd);
	    aclose(outfd);
	    amfree(incrname);
	    dbprintf("%s\n", errmsg);
	    if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	    } else {
		fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
	    }
	    amfree(errmsg);
	    exit(1);
	}

	if (close(infd) != 0) {
	    errmsg = g_strdup_printf(_("closing %s: %s"),
			         inputname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    aclose(outfd);
	    amfree(incrname);
	    amfree(errmsg);
	    dbprintf("%s\n", errmsg);
	    if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	    } else {
		fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
	    }
	    exit(1);
	}
	if (close(outfd) != 0) {
	    errmsg = g_strdup_printf(_("closing %s: %s"),
			         incrname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    amfree(incrname);
	    amfree(errmsg);
	    dbprintf("%s\n", errmsg);
	    if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	    } else {
		fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
	    }
	    exit(1);
	}

    } else {
	errmsg =  _("GNUTAR-LISTDIR is not defined");
	dbprintf("%s\n", errmsg);
	if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	} else {
		fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
	}
	exit(1);
    }
    return incrname;
}

static void
check_no_check_device(void)
{
    if (gnutar_checkdevice == 0) {
	GPtrArray *argv_ptr = g_ptr_array_new();
	int dumpin;
	int dataf;
	int outf;
	int size;
	char buf[32768];

	g_ptr_array_add(argv_ptr, gnutar_path);
	g_ptr_array_add(argv_ptr, "-x");
	g_ptr_array_add(argv_ptr, "--no-check-device");
	g_ptr_array_add(argv_ptr, "-f");
	g_ptr_array_add(argv_ptr, "-");
	g_ptr_array_add(argv_ptr, NULL);

	pipespawnv(gnutar_path, STDIN_PIPE|STDOUT_PIPE|STDERR_PIPE, 0,
			     &dumpin, &dataf, &outf, (char **)argv_ptr->pdata);
	aclose(dumpin);
	aclose(dataf);
	size = read(outf, buf, 32767);
	if (size > 0) {
	    buf[size] = '\0';
	    if (strstr(buf, "--no-check-device")) {
		g_debug("disabling --no-check-device since '%s' doesn't support it", gnutar_path);
		gnutar_checkdevice = 1;
	    }
	}
	aclose(outf);
	g_ptr_array_free(argv_ptr, TRUE);
    }
}

GPtrArray *amgtar_build_argv(
    char *gnutar_realpath,
    application_argument_t *argument,
    char  *incrname,
    char **file_exclude,
    char **file_include,
    int    command,
    messagelist_t *mlist)
{
    int    nb_exclude;
    int    nb_include;
    char  *dirname;
    char   tmppath[PATH_MAX];
    GPtrArray *argv_ptr = g_ptr_array_new();
    GSList    *copt;

    check_no_check_device();

    if (gnutar_target) {
	dirname = gnutar_target;
    } else {
	dirname = argument->dle.device;
    }

    amgtar_build_exinclude(&argument->dle,
			   &nb_exclude, file_exclude,
			   &nb_include, file_include, dirname, mlist);

    g_ptr_array_add(argv_ptr, g_strdup(gnutar_realpath));

    g_ptr_array_add(argv_ptr, g_strdup("--create"));
    if (command == CMD_BACKUP && argument->dle.create_index) {
        g_ptr_array_add(argv_ptr, g_strdup("--verbose"));
        g_ptr_array_add(argv_ptr, g_strdup("--block-number"));
    }
    g_ptr_array_add(argv_ptr, g_strdup("--file"));
    if (command == CMD_ESTIMATE) {
        g_ptr_array_add(argv_ptr, g_strdup("/dev/null"));
    } else {
        g_ptr_array_add(argv_ptr, g_strdup("-"));
    }
    if (gnutar_no_unquote)
	g_ptr_array_add(argv_ptr, g_strdup("--no-unquote"));
    g_ptr_array_add(argv_ptr, g_strdup("--directory"));
    canonicalize_pathname(dirname, tmppath);
    g_ptr_array_add(argv_ptr, g_strdup(tmppath));
    if (gnutar_onefilesystem)
	g_ptr_array_add(argv_ptr, g_strdup("--one-file-system"));
    if (gnutar_atimepreserve)
	g_ptr_array_add(argv_ptr, g_strdup("--atime-preserve=system"));
    if (!gnutar_checkdevice)
	g_ptr_array_add(argv_ptr, g_strdup("--no-check-device"));
    if (gnutar_acls)
	g_ptr_array_add(argv_ptr, g_strdup("--acls"));
    if (gnutar_selinux)
	g_ptr_array_add(argv_ptr, g_strdup("--selinux"));
    if (gnutar_xattrs)
	g_ptr_array_add(argv_ptr, g_strdup("--xattrs"));
    g_ptr_array_add(argv_ptr, g_strdup("--listed-incremental"));
    g_ptr_array_add(argv_ptr, g_strdup(incrname));
    if (gnutar_sparse) {
	if (!gnutar_sparse_set) {
	    char  *gtar_version;
	    char  *minor_version;
	    char  *sminor_version;
	    char  *gv;
	    int    major;
	    int    minor;
	    GPtrArray *version_ptr = g_ptr_array_new();

	    g_ptr_array_add(version_ptr, gnutar_path);
	    g_ptr_array_add(version_ptr, "--version");
	    g_ptr_array_add(version_ptr, NULL);
	    gtar_version = get_first_line(version_ptr);
	    if (gtar_version) {
		for (gv = gtar_version; *gv && !g_ascii_isdigit(*gv); gv++);
		minor_version = index(gtar_version, '.');
		if (minor_version) {
		    *minor_version++ = '\0';
		    sminor_version = index(minor_version, '.');
		    if (sminor_version) {
			*sminor_version = '\0';
		    }
		    major = atoi(gv);
		    minor = atoi(minor_version);
		    if (major < 1 ||
			(major == 1 && minor < 28)) {
			gnutar_sparse = 0;
		    }
		}
	    }
	    g_ptr_array_free(version_ptr, TRUE);
	    amfree(gtar_version);
	}
	if (gnutar_sparse) {
	    g_ptr_array_add(argv_ptr, g_strdup("--sparse"));
	}
    }
    if (argument->tar_blocksize) {
	g_ptr_array_add(argv_ptr, g_strdup("--blocking-factor"));
	g_ptr_array_add(argv_ptr, g_strdup(argument->tar_blocksize));
    }
    g_ptr_array_add(argv_ptr, g_strdup("--ignore-failed-read"));
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
    }
    else {
	g_ptr_array_add(argv_ptr, g_strdup("."));
    }
    g_ptr_array_add(argv_ptr, NULL);

    return(argv_ptr);
}

