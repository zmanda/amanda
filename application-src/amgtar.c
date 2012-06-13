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
 * $Id: amgtar.c 8888 2007-10-02 13:40:42Z martineau $
 *
 * send estimated backup sizes using dump
 */

/* PROPERTY:
 *
 * GNUTAR-PATH     (default GNUTAR)
 * GNUTAR-LISTDIR  (default CNF_GNUTAR_LIST_DIR)
 * DIRECTORY       (no default, if set, the backup will be from that directory
 *		    instead of from the --device)
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
#include "util.h"
#include "getfsent.h"
#include "client_util.h"
#include "conffile.h"
#include "getopt.h"

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
    char      *include_list_glob;
    char      *exclude_list_glob;
    dle_t      dle;
    int        argc;
    char     **argv;
    int        verbose;
    int        ignore_zeros;
} application_argument_t;

enum { CMD_ESTIMATE, CMD_BACKUP };

static void amgtar_support(application_argument_t *argument);
static void amgtar_selfcheck(application_argument_t *argument);
static void amgtar_estimate(application_argument_t *argument);
static void amgtar_backup(application_argument_t *argument);
static void amgtar_restore(application_argument_t *argument);
static void amgtar_validate(application_argument_t *argument);
static void amgtar_build_exinclude(dle_t *dle, int verbose,
				   int *nb_exclude, char **file_exclude,
				   int *nb_include, char **file_include);
static char *amgtar_get_incrname(application_argument_t *argument, int level,
				 FILE *mesgstream, int command);
static void check_no_check_device(void);
static GPtrArray *amgtar_build_argv(application_argument_t *argument,
				char *incrname, char **file_exclude,
				char **file_include, int command);
static char *gnutar_path;
static char *gnutar_listdir;
static char *gnutar_directory;
static int gnutar_onefilesystem;
static int gnutar_atimepreserve;
static int gnutar_acls;
static int gnutar_selinux;
static int gnutar_xattrs;
static int gnutar_checkdevice;
static int gnutar_no_unquote;
static int gnutar_sparse;
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

#ifdef GNUTAR
    gnutar_path = GNUTAR;
#else
    gnutar_path = NULL;
#endif
    gnutar_listdir = NULL;
    gnutar_directory = NULL;
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
	if (strcmp(argv[1], "selfcheck") == 0) {
	    printf("ERROR amgtar must be run setuid root\n");
	}
	error(_("amgtar must be run setuid root"));
    }

    safe_fd(3, 2);

    set_pname("amgtar");

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

    gnutar_listdir = stralloc(getconf_str(CNF_GNUTAR_LIST_DIR));
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
    init_dle(&argument.dle);
    argument.dle.record = 0;

    while (1) {
	int option_index = 0;
    	c = getopt_long (argc, argv, "", long_options, &option_index);
	if (c == -1) {
	    break;
	}
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
	case 10: gnutar_path = stralloc(optarg);
		 break;
	case 11: gnutar_listdir = stralloc(optarg);
		 break;
	case 12: if (optarg && strcasecmp(optarg, "NO") == 0)
		     gnutar_onefilesystem = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     gnutar_onefilesystem = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad ONE-FILE-SYSTEM property value (%s)]\n"), get_pname(), optarg);
		 break;
	case 13: if (optarg && strcasecmp(optarg, "NO") == 0)
		     gnutar_sparse = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     gnutar_sparse = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad SPARSE property value (%s)]\n"), get_pname(), optarg);
		 break;
	case 14: if (optarg && strcasecmp(optarg, "NO") == 0)
		     gnutar_atimepreserve = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     gnutar_atimepreserve = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad ATIME-PRESERVE property value (%s)]\n"), get_pname(), optarg);
		 break;
	case 15: if (optarg && strcasecmp(optarg, "NO") == 0)
		     gnutar_checkdevice = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     gnutar_checkdevice = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad CHECK-DEVICE property value (%s)]\n"), get_pname(), optarg);
		 break;
	case 16: if (optarg)
		     argument.dle.include_file =
			 append_sl(argument.dle.include_file, optarg);
		 break;
	case 17: if (optarg)
		     argument.dle.include_list =
			 append_sl(argument.dle.include_list, optarg);
		 break;
	case 18: argument.dle.include_optional = 1;
		 break;
	case 19: if (optarg)
		     argument.dle.exclude_file =
			 append_sl(argument.dle.exclude_file, optarg);
		 break;
	case 20: if (optarg)
		     argument.dle.exclude_list =
			 append_sl(argument.dle.exclude_list, optarg);
		 break;
	case 21: argument.dle.exclude_optional = 1;
		 break;
	case 22: gnutar_directory = stralloc(optarg);
		 break;
	case 23: if (optarg)
		     normal_message = 
			 g_slist_append(normal_message, optarg);
		 break;
	case 24: if (optarg)
		     ignore_message = 
			 g_slist_append(ignore_message, optarg);
		 break;
	case 25: if (optarg)
		     strange_message = 
			 g_slist_append(strange_message, optarg);
		 break;
	case 26: if (optarg)
		     exit_handling = stralloc(optarg);
		 break;
	case 27: argument.calcsize = 1;
		 break;
	case 28: argument.tar_blocksize = stralloc(optarg);
		 break;
	case 29: if (optarg && strcasecmp(optarg, "NO") == 0)
		     gnutar_no_unquote = 0;
		 else if (optarg && strcasecmp(optarg, "YES") == 0)
		     gnutar_no_unquote = 1;
		 else if (strcasecmp(command, "selfcheck") == 0)
		     printf(_("ERROR [%s: bad No_UNQUOTE property value (%s)]\n"), get_pname(), optarg);
		 break;
        case 30: if (optarg && strcasecmp(optarg, "YES") == 0)
                   gnutar_acls = 1;
                 break;
        case 31: if (optarg && strcasecmp(optarg, "YES") == 0)
                   gnutar_selinux = 1;
                 break;
        case 32: if (optarg && strcasecmp(optarg, "YES") == 0)
                   gnutar_xattrs = 1;
                 break;
	case 33: argument.command_options =
			g_slist_append(argument.command_options,
				       stralloc(optarg));
		 break;
	case 34: if (optarg)
		     argument.include_list_glob = stralloc(optarg);
		 break;
	case 35: if (optarg)
		     argument.exclude_list_glob = stralloc(optarg);
		 break;
	case 36: if (optarg  && strcasecmp(optarg, "YES") == 0)
		     argument.verbose = 1;
		 break;
	case 37: if (strcasecmp(optarg, "YES") != 0)
		     argument.ignore_zeros = 0;
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
	config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
		    argument.config);
	dbrename(get_config_name(), DBG_SUBDIR_CLIENT);
    }

    if (config_errors(NULL) >= CFGERR_ERRORS) {
	g_critical(_("errors processing config file"));
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
	gnutar_listdir = NULL;

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
    if (gnutar_directory) {
	dbprintf("DIRECTORY %s\n", gnutar_directory);
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

    if (strcmp(command, "support") == 0) {
	amgtar_support(&argument);
    } else if (strcmp(command, "selfcheck") == 0) {
	amgtar_selfcheck(&argument);
    } else if (strcmp(command, "estimate") == 0) {
	amgtar_estimate(&argument);
    } else if (strcmp(command, "backup") == 0) {
	amgtar_backup(&argument);
    } else if (strcmp(command, "restore") == 0) {
	amgtar_restore(&argument);
    } else if (strcmp(command, "validate") == 0) {
	amgtar_validate(&argument);
    } else {
	dbprintf("Unknown command `%s'.\n", command);
	fprintf(stderr, "Unknown command `%s'.\n", command);
	exit (1);
    }
    return 0;
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
}

static void
amgtar_selfcheck(
    application_argument_t *argument)
{
    if (argument->dle.disk) {
	char *qdisk = quote_string(argument->dle.disk);
	fprintf(stdout, "OK disk %s\n", qdisk);
	amfree(qdisk);
    }

    printf("OK amgtar version %s\n", VERSION);
    amgtar_build_exinclude(&argument->dle, 1, NULL, NULL, NULL, NULL);

    printf("OK amgtar\n");
    if (gnutar_path) {
	if (check_file(gnutar_path, X_OK)) {
	    char *gtar_version;
	    GPtrArray *argv_ptr = g_ptr_array_new();

	    g_ptr_array_add(argv_ptr, gnutar_path);
	    g_ptr_array_add(argv_ptr, "--version");
	    g_ptr_array_add(argv_ptr, NULL);

	    gtar_version = get_first_line(argv_ptr);
	    if (gtar_version) {
		char *gv;
		for (gv = gtar_version; *gv && !g_ascii_isdigit(*gv); gv++);
		printf("OK amgtar gtar-version %s\n", gv);
	    } else {
		printf(_("ERROR [Can't get %s version]\n"), gnutar_path);
	    }

	    g_ptr_array_free(argv_ptr, TRUE);
	    amfree(gtar_version);
	}
    } else {
	printf(_("ERROR [GNUTAR program not available]\n"));
    }

    set_root_privs(1);
    if (gnutar_listdir && strlen(gnutar_listdir) == 0)
	gnutar_listdir = NULL;
    if (gnutar_listdir) {
	check_dir(gnutar_listdir, R_OK|W_OK);
    } else {
	printf(_("ERROR [No GNUTAR-LISTDIR]\n"));
    }

    if (gnutar_directory) {
	check_dir(gnutar_directory, R_OK);
    } else if (argument->dle.device) {
	check_dir(argument->dle.device, R_OK);
    }
    if (argument->calcsize) {
	char *calcsize = vstralloc(amlibexecdir, "/", "calcsize", NULL);
	check_file(calcsize, X_OK);
	check_suid(calcsize);
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
    char      *cmd = NULL;
    int        nullfd = -1;
    int        pipefd = -1;
    FILE      *dumpout = NULL;
    off_t      size = -1;
    char       line[32768];
    char      *errmsg = NULL;
    char      *qerrmsg = NULL;
    char      *qdisk;
    amwait_t   wait_status;
    int        tarpid;
    amregex_t *rp;
    times_t    start_time;
    int        level;
    GSList    *levels;
    char      *file_exclude;
    char      *file_include;

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

    qdisk = quote_string(argument->dle.disk);

    if (argument->calcsize) {
	char *dirname;
	int   nb_exclude;
	int   nb_include;

	if (gnutar_directory) {
	    dirname = gnutar_directory;
	} else {
	    dirname = argument->dle.device;
	}
	amgtar_build_exinclude(&argument->dle, 1,
			       &nb_exclude, &file_exclude,
			       &nb_include, &file_include);

	run_calcsize(argument->config, "GNUTAR", argument->dle.disk, dirname,
		     argument->level, file_exclude, file_include);

	if (argument->verbose == 0) {
	    if (file_exclude)
		unlink(file_exclude);
	    if (file_include)
		unlink(file_include);
        }
	return;
    }

    if (!gnutar_path) {
	errmsg = vstrallocf(_("GNUTAR-PATH not defined"));
	goto common_error;
    }

    if (!gnutar_listdir) {
	errmsg = vstrallocf(_("GNUTAR-LISTDIR not defined"));
	goto common_error;
    }

    for (levels = argument->level; levels != NULL; levels = levels->next) {
	level = GPOINTER_TO_INT(levels->data);
	incrname = amgtar_get_incrname(argument, level, stdout, CMD_ESTIMATE);
	cmd = stralloc(gnutar_path);
	argv_ptr = amgtar_build_argv(argument, incrname, &file_exclude,
				     &file_include, CMD_ESTIMATE);

	start_time = curclock();

	if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	    errmsg = vstrallocf(_("Cannot access /dev/null : %s"),
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

	while (fgets(line, sizeof(line), dumpout) != NULL) {
	    dbprintf("%s", line);
	}

	dbprintf(".....\n");
	dbprintf(_("estimate time for %s level %d: %s\n"),
		 qdisk,
		 level,
		 walltime_str(timessub(curclock(), start_time)));
	if(size == (off_t)-1) {
	    errmsg = vstrallocf(_("no size line match in %s output"), cmd);
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

	kill(-tarpid, SIGTERM);

	dbprintf(_("waiting for %s \"%s\" child\n"), cmd, qdisk);
	waitpid(tarpid, &wait_status, 0);
	if (WIFSIGNALED(wait_status)) {
	    errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
				cmd, WTERMSIG(wait_status), dbfn());
	} else if (WIFEXITED(wait_status)) {
	    if (exit_value[WEXITSTATUS(wait_status)] == 1) {
		errmsg = vstrallocf(_("%s exited with status %d: see %s"),
				    cmd, WEXITSTATUS(wait_status), dbfn());
	    } else {
		/* Normal exit */
	    }
	} else {
	    errmsg = vstrallocf(_("%s got bad exit: see %s"),
				cmd, dbfn());
	}
	dbprintf(_("after %s %s wait\n"), cmd, qdisk);

common_exit:
	if (errmsg) {
	    dbprintf("%s", errmsg);
	    fprintf(stdout, "ERROR %s\n", errmsg);
	}

	if (incrname) {
	    unlink(incrname);
	}

	if (argument->verbose == 0) {
	    if (file_exclude)
		unlink(file_exclude);
	    if (file_include)
		unlink(file_include);
        }

	g_ptr_array_free_full(argv_ptr);
	amfree(cmd);

	aclose(nullfd);
	afclose(dumpout);

	fprintf(stdout, "%d %lld 1\n", level, (long long)size);
    }
    amfree(qdisk);
    return;

common_error:
    qerrmsg = quote_string(errmsg);
    amfree(qdisk);
    dbprintf("%s", errmsg);
    fprintf(stdout, "ERROR %s\n", qerrmsg);
    amfree(errmsg);
    amfree(qerrmsg);
    return;
}

static void
amgtar_backup(
    application_argument_t *argument)
{
    int         dumpin;
    char      *cmd = NULL;
    char      *qdisk;
    char      *incrname;
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
    char      *file_exclude;
    char      *file_include;

    mesgstream = fdopen(mesgf, "w");
    if (!mesgstream) {
	error(_("error mesgstream(%d): %s\n"), mesgf, strerror(errno));
    }

    if (!gnutar_path) {
	error(_("GNUTAR-PATH not defined"));
    }
    if (!gnutar_listdir) {
	error(_("GNUTAR-LISTDIR not defined"));
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

    incrname = amgtar_get_incrname(argument,
				   GPOINTER_TO_INT(argument->level->data),
				   mesgstream, CMD_BACKUP);
    cmd = stralloc(gnutar_path);
    argv_ptr = amgtar_build_argv(argument, incrname, &file_exclude,
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
	if (line[strlen(line)-1] == '\n') /* remove trailling \n */
	    line[strlen(line)-1] = '\0';
	if (*line == '.' && *(line+1) == '/') { /* filename */
	    if (argument->dle.create_index) {
		fprintf(indexstream, "%s\n", &line[1]); /* remove . */
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

    waitpid(tarpid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = vstrallocf(_("%s terminated with signal %d: see %s"),
			    cmd, WTERMSIG(wait_status), dbfn());
    } else if (WIFEXITED(wait_status)) {
	if (exit_value[WEXITSTATUS(wait_status)] == 1) {
	    errmsg = vstrallocf(_("%s exited with status %d: see %s"),
				cmd, WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = vstrallocf(_("%s got bad exit: see %s"),
			    cmd, dbfn());
    }
    dbprintf(_("after %s %s wait\n"), cmd, qdisk);
    dbprintf(_("amgtar: %s: pid %ld\n"), cmd, (long)tarpid);
    if (errmsg) {
	dbprintf("%s", errmsg);
	g_fprintf(mesgstream, "sendbackup: error [%s]\n", errmsg);
    }

    if (!errmsg && incrname && strlen(incrname) > 4) {
	if (argument->dle.record) {
	    char *nodotnew;
	    nodotnew = stralloc(incrname);
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
    dbprintf("sendbackup: end\n");
    fprintf(mesgstream, "sendbackup: end\n");

    if (argument->dle.create_index)
	fclose(indexstream);

    fclose(mesgstream);

    if (argument->verbose == 0) {
	if (file_exclude)
	    unlink(file_exclude);
	if (file_include)
	    unlink(file_include);
    }

    amfree(incrname);
    amfree(qdisk);
    amfree(cmd);
    g_ptr_array_free_full(argv_ptr);
}

static void
amgtar_restore(
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

    if (!gnutar_path) {
	error(_("GNUTAR-PATH not defined"));
    }

    cmd = stralloc(gnutar_path);
    g_ptr_array_add(argv_ptr, stralloc(gnutar_path));
    g_ptr_array_add(argv_ptr, stralloc("--numeric-owner"));
    if (gnutar_no_unquote)
	g_ptr_array_add(argv_ptr, stralloc("--no-unquote"));
    if (gnutar_acls)
	g_ptr_array_add(argv_ptr, stralloc("--acls"));
    if (gnutar_selinux)
	g_ptr_array_add(argv_ptr, stralloc("--selinux"));
    if (gnutar_xattrs)
	g_ptr_array_add(argv_ptr, stralloc("--xattrs"));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    if (argument->ignore_zeros) {
	g_ptr_array_add(argv_ptr, stralloc("--ignore-zeros"));
    }
    if (argument->tar_blocksize) {
	g_ptr_array_add(argv_ptr, stralloc("--blocking-factor"));
	g_ptr_array_add(argv_ptr, stralloc(argument->tar_blocksize));
    }
    g_ptr_array_add(argv_ptr, stralloc("-xpGvf"));
    g_ptr_array_add(argv_ptr, stralloc("-"));
    if (gnutar_directory) {
	struct stat stat_buf;
	if(stat(gnutar_directory, &stat_buf) != 0) {
	    fprintf(stderr,"can not stat directory %s: %s\n", gnutar_directory, strerror(errno));
	    exit(1);
	}
	if (!S_ISDIR(stat_buf.st_mode)) {
	    fprintf(stderr,"%s is not a directory\n", gnutar_directory);
	    exit(1);
	}
	if (access(gnutar_directory, W_OK) != 0) {
	    fprintf(stderr, "Can't write to %s: %s\n", gnutar_directory, strerror(errno));
	    exit(1);
	}
	g_ptr_array_add(argv_ptr, stralloc("--directory"));
	g_ptr_array_add(argv_ptr, stralloc(gnutar_directory));
    }

    g_ptr_array_add(argv_ptr, stralloc("--wildcards"));
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
	    sdisk = g_strdup_printf("no_dle-%d", getpid());
	}
	exclude_filename= vstralloc(AMANDA_TMPDIR, "/", "exclude-", sdisk,  NULL);
	exclude_list = fopen(argument->dle.exclude_list->first->name, "r");

	exclude = fopen(exclude_filename, "w");
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
	fclose(exclude);
	g_ptr_array_add(argv_ptr, stralloc("--exclude-from"));
	g_ptr_array_add(argv_ptr, exclude_filename);
    }

    if (argument->exclude_list_glob) {
	g_ptr_array_add(argv_ptr, stralloc("--exclude-from"));
	g_ptr_array_add(argv_ptr, stralloc(argument->exclude_list_glob));
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
	    sdisk = g_strdup_printf("no_dle-%d", getpid());
	}
	include_filename = vstralloc(AMANDA_TMPDIR, "/", "include-", sdisk,  NULL);
	include = fopen(include_filename, "w");
	if (argument->dle.include_list &&
	    argument->dle.include_list->nb_element == 1) {
	    char line[2*PATH_MAX];
	    FILE *include_list = fopen(argument->dle.include_list->first->name, "r");
	    while (fgets(line, 2*PATH_MAX, include_list)) {
		char *escaped;
		line[strlen(line)-1] = '\0'; /* remove '\n' */
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

	for (j=1; j< argument->argc; j++) {
	    char *escaped = escape_tar_glob(argument->argv[j], &in_argv);
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
	    g_ptr_array_add(argv_ptr, stralloc("--files-from"));
	    g_ptr_array_add(argv_ptr, include_filename);
	}

	if (argument->include_list_glob) {
	    g_ptr_array_add(argv_ptr, stralloc("--files-from"));
	    g_ptr_array_add(argv_ptr, stralloc(argument->include_list_glob));
	}

	for (i = 0; i < argv_include->len; i++) {
	    g_ptr_array_add(argv_ptr, (char *)g_ptr_array_index(argv_include,i));
	}
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
	e = strerror(errno);
	error(_("error [exec %s: %s]"), cmd, e);
	break;
    default: break;
    }

    waitpid(tarpid, NULL, 0);
    if (argument->verbose == 0) {
	if (exclude_filename)
	    unlink(exclude_filename);
	if (include_filename)
	    unlink(include_filename);
    }
}

static void
amgtar_validate(
    application_argument_t *argument G_GNUC_UNUSED)
{
    char       *cmd;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    char      **env;
    char       *e;
    char        buf[32768];

    if (!gnutar_path) {
	dbprintf("GNUTAR-PATH not set; Piping to /dev/null\n");
	fprintf(stderr,"GNUTAR-PATH not set; Piping to /dev/null\n");
	goto pipe_to_null;
    }

    cmd = stralloc(gnutar_path);
    g_ptr_array_add(argv_ptr, stralloc(gnutar_path));
    /* ignore trailing zero blocks on input (this was the default until tar-1.21) */
    g_ptr_array_add(argv_ptr, stralloc("--ignore-zeros"));
    g_ptr_array_add(argv_ptr, stralloc("-tf"));
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

static void
amgtar_build_exinclude(
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

	basename = vstralloc(gnutar_listdir,
			     "/",
			     argument->host,
			     sdisk,
			     NULL);
	amfree(sdisk);

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

		errmsg = vstrallocf(_("amgtar: error opening %s: %s"),
				     inputname, strerror(errno));
		dbprintf("%s\n", errmsg);
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

	/*
	 * Copy the previous listed incremental file to the new one.
	 */
	if ((outfd = open(incrname, O_WRONLY|O_CREAT, 0600)) == -1) {
	    errmsg = vstrallocf(_("error opening %s: %s"),
			         incrname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    if (command == CMD_ESTIMATE) {
		fprintf(mesgstream, "ERROR %s\n", errmsg);
	    } else {
		fprintf(mesgstream, "? %s\n", errmsg);
	    }
	    exit(1);
	}

	while ((nb = read(infd, &buf, SIZEOF(buf))) > 0) {
	    if (full_write(outfd, &buf, (size_t)nb) < (size_t)nb) {
		errmsg = vstrallocf(_("writing to %s: %s"),
				     incrname, strerror(errno));
		dbprintf("%s\n", errmsg);
		return NULL;
	    }
	}

	if (nb < 0) {
	    errmsg = vstrallocf(_("reading from %s: %s"),
			         inputname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    return NULL;
	}

	if (close(infd) != 0) {
	    errmsg = vstrallocf(_("closing %s: %s"),
			         inputname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    return NULL;
	}
	if (close(outfd) != 0) {
	    errmsg = vstrallocf(_("closing %s: %s"),
			         incrname, strerror(errno));
	    dbprintf("%s\n", errmsg);
	    return NULL;
	}

	amfree(inputname);
	amfree(basename);
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
    application_argument_t *argument,
    char  *incrname,
    char **file_exclude,
    char **file_include,
    int    command)
{
    int    nb_exclude;
    int    nb_include;
    char  *dirname;
    char   tmppath[PATH_MAX];
    GPtrArray *argv_ptr = g_ptr_array_new();
    GSList    *copt;

    check_no_check_device();
    amgtar_build_exinclude(&argument->dle, 1,
			   &nb_exclude, file_exclude,
			   &nb_include, file_include);

    if (gnutar_directory) {
	dirname = gnutar_directory;
    } else {
	dirname = argument->dle.device;
    }

    g_ptr_array_add(argv_ptr, stralloc(gnutar_path));

    g_ptr_array_add(argv_ptr, stralloc("--create"));
    if (command == CMD_BACKUP && argument->dle.create_index)
        g_ptr_array_add(argv_ptr, stralloc("--verbose"));
    g_ptr_array_add(argv_ptr, stralloc("--file"));
    if (command == CMD_ESTIMATE) {
        g_ptr_array_add(argv_ptr, stralloc("/dev/null"));
    } else {
        g_ptr_array_add(argv_ptr, stralloc("-"));
    }
    if (gnutar_no_unquote)
	g_ptr_array_add(argv_ptr, stralloc("--no-unquote"));
    g_ptr_array_add(argv_ptr, stralloc("--directory"));
    canonicalize_pathname(dirname, tmppath);
    g_ptr_array_add(argv_ptr, stralloc(tmppath));
    if (gnutar_onefilesystem)
	g_ptr_array_add(argv_ptr, stralloc("--one-file-system"));
    if (gnutar_atimepreserve)
	g_ptr_array_add(argv_ptr, stralloc("--atime-preserve=system"));
    if (!gnutar_checkdevice)
	g_ptr_array_add(argv_ptr, stralloc("--no-check-device"));
    if (gnutar_acls)
	g_ptr_array_add(argv_ptr, stralloc("--acls"));
    if (gnutar_selinux)
	g_ptr_array_add(argv_ptr, stralloc("--selinux"));
    if (gnutar_xattrs)
	g_ptr_array_add(argv_ptr, stralloc("--xattrs"));
    g_ptr_array_add(argv_ptr, stralloc("--listed-incremental"));
    g_ptr_array_add(argv_ptr, stralloc(incrname));
    if (gnutar_sparse)
	g_ptr_array_add(argv_ptr, stralloc("--sparse"));
    if (argument->tar_blocksize) {
	g_ptr_array_add(argv_ptr, stralloc("--blocking-factor"));
	g_ptr_array_add(argv_ptr, stralloc(argument->tar_blocksize));
    }
    g_ptr_array_add(argv_ptr, stralloc("--ignore-failed-read"));
    g_ptr_array_add(argv_ptr, stralloc("--totals"));

    for (copt = argument->command_options; copt != NULL; copt = copt->next) {
	g_ptr_array_add(argv_ptr, stralloc((char *)copt->data));
    }

    if (*file_exclude) {
	g_ptr_array_add(argv_ptr, stralloc("--exclude-from"));
	g_ptr_array_add(argv_ptr, stralloc(*file_exclude));
    }

    if (*file_include) {
	g_ptr_array_add(argv_ptr, stralloc("--files-from"));
	g_ptr_array_add(argv_ptr, stralloc(*file_include));
    }
    else {
	g_ptr_array_add(argv_ptr, stralloc("."));
    }
    g_ptr_array_add(argv_ptr, NULL);

    return(argv_ptr);
}

