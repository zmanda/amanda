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
 * TARGET
 */

#include "amanda.h"
#include "match.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "amandates.h"
#include "clock.h"
#include "amutil.h"
#include "getfsent.h"
#include "client_util.h"
#include "conffile.h"
#include "getopt.h"
#include "sendbackup.h"
#include "security-file.h"
#include "ammessage.h"
#include "event.h"

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

  AM_ERROR_RE("amstar: error"),

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
static void amstar_index(application_argument_t *argument);
static GPtrArray *amstar_build_argv(char *star_realpath,
				application_argument_t *argument,
				int level,
				int command,
				FILE *mesgstream);
static int check_device(application_argument_t *argument);

static char *command;
static char *star_path;
static char *star_tardumps;
static int   star_dle_tardumps;
static int   star_onefilesystem;
static int   star_sparse;
static int   star_acl;
static char *star_target;
static GSList *normal_message = NULL;
static GSList *ignore_message = NULL;
static GSList *strange_message = NULL;
static FILE   *mesgstream = NULL;
static int     amstar_exit_value = 0;


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
    {"include-file"    , 1, NULL, 25},
    {"target"          , 1, NULL, 26},
    { NULL, 0, NULL, 0}
};

typedef struct filter_s {
    int    fd;
    char  *name;
    char  *buffer;
    gint64 first;		/* first byte used */
    gint64 size;		/* number of byte use in the buffer */
    gint64 allocated_size;	/* allocated size of the buffer     */
    event_handle_t *event;
    int    out;
} filter_t;

static void read_fd(int fd, char *name, event_fn_t fn);
static void read_text(void *cookie);

static void
read_fd(
    int   fd,
    char *name,
    event_fn_t fn)
{
    filter_t *filter = g_new0(filter_t, 1);
    filter->fd = fd;
    filter->name = g_strdup(name);
    filter->event = event_create((event_id_t)filter->fd, EV_READFD,
				 fn, filter);
    event_activate(filter->event);
}

static void
read_text(
    void *cookie)
{
    filter_t  *filter = cookie;
    char      *line;
    char      *p;
    ssize_t    nread;
    int        len;

    if (filter->buffer == NULL) {
	/* allocate initial buffer */
	filter->buffer = g_malloc(32768);
	filter->first = 0;
	filter->size = 0;
	filter->allocated_size = 32768;
    } else if (filter->first > 0) {
	if (filter->allocated_size - filter->size - filter->first < 1024) {
	    memmove(filter->buffer, filter->buffer + filter->first,
		    filter->size);
	    filter->first = 0;
	}
    } else if (filter->allocated_size - filter->size < 1024) {
	/* double the size of the buffer */
	filter->allocated_size *= 2;
	filter->buffer = g_realloc(filter->buffer, filter->allocated_size);
    }

    nread = read(filter->fd, filter->buffer + filter->first + filter->size,
		 filter->allocated_size - filter->first - filter->size - 2);

    if (nread <= 0) {
	event_release(filter->event);
	aclose(filter->fd);
	if (filter->size > 0 && filter->buffer[filter->first + filter->size - 1] != '\n') {
	    /* Add a '\n' at end of buffer */
	    filter->buffer[filter->first + filter->size] = '\n';
	    filter->size++;
	}
    } else {
	filter->size += nread;
    }

    /* process all complete lines */
    line = filter->buffer + filter->first;
    line[filter->size] = '\0';
    while (line < filter->buffer + filter->first + filter->size &&
	   (p = strchr(line, '\n')) != NULL) {
	*p = '\0';
	if (g_str_equal(filter->name, "restore stdout")) {
	    g_fprintf(stdout, "%s\n", line);
	} else if (g_str_equal(filter->name, "restore stderr")) {
	    if (!match("^star: [0-9]* blocks?", line)) {
		g_fprintf(stderr, "%s\n", line);
	    }
	} else if (g_str_equal(filter->name, "validate stdout")) {
	    g_fprintf(stdout, "%s\n", line);
	} else if (g_str_equal(filter->name, "validate stderr")) {
	    if (!match("^star: [0-9]* blocks?", line)) {
		g_fprintf(stderr, "%s\n", line);
	    }
	} else {
	    g_debug("unknown: %s", line);
	}
	len = p - line + 1;
	filter->first += len;
	filter->size -= len;
	line = p + 1;
    }

    if (nread <= 0) {
	g_free(filter->name);
	g_free(filter->buffer);
	g_free(filter);
    }
}

static message_t *
amstar_print_message(
    message_t *message)
{
    if (strcasecmp(command, "selfcheck") == 0) {
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
	amstar_exit_value = 1;
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



int
main(
    int		argc,
    char **	argv)
{
    int c;
    application_argument_t argument;
    char *star_sparse_value = NULL;
    char *star_acl_value = NULL;

#ifdef STAR
    star_path = g_strdup(STAR);
#else
    star_path = NULL;
#endif
    star_tardumps = g_strdup("/etc/tardumps");
    star_dle_tardumps = 0;
    star_onefilesystem = 1;
    star_sparse = 1;
    star_acl = 1;
    star_target = NULL;

    glib_init();

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
	if (g_str_equal(argv[1], "selfcheck")) {
	    printf("ERROR amstar must be run setuid root\n");
	}
	error(_("amstar must be run setuid root"));
    }

    safe_fd(3, 2);

    set_pname("amstar");
    set_pcomponent("application");
    set_pmodule("amstar");

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
	case 1: if (optarg) {
		    amfree(argument.config);
		    argument.config = g_strdup(optarg);
		}
		break;
	case 2: if (optarg) {
		    amfree(argument.host);
		    argument.host = g_strdup(optarg);
		}
		break;
	case 3: if (optarg) {
		    amfree(argument.dle.disk);
		    argument.dle.disk = g_strdup(optarg);
		}
		break;
	case 4: if (optarg) {
		    amfree(argument.dle.device);
		    argument.dle.device = g_strdup(optarg);
		}
		break;
	case 5: if (optarg) {
		    argument.level = g_slist_append(argument.level,
						GINT_TO_POINTER(atoi(optarg)));
		}
		break;
	case 6: argument.dle.create_index = 1;
		break;
	case 7: argument.message = 1;
		break;
	case 8: argument.collection = 1;
		break;
	case 9: argument.dle.record = 1;
		break;
	case 10: if (optarg) {
		     amfree(star_path);
		     star_path = g_strdup(optarg);
		 }
		 break;
	case 11: if (optarg) {
		     amfree(star_tardumps);
		     star_tardumps = g_strdup(optarg);
		 }
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
	case 14: amfree(star_sparse_value);
		 star_sparse_value = g_strdup(optarg);
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
	case 21: if (optarg) {
		     amfree(star_target);
		     star_target = g_strdup(optarg);
		 }
		 break;
	case 22: if (optarg)
		     argument.command_options =
			g_slist_append(argument.command_options,
				       g_strdup(optarg));
		 break;
	case 23: if (optarg)
		     argument.dle.exclude_file =
			 append_sl(argument.dle.exclude_file, optarg);
		 break;
	case 24: amfree(star_acl_value);
		 star_acl_value = g_strdup(optarg);
		 break;
	case 25: if (optarg)
		     argument.dle.include_file =
			 append_sl(argument.dle.include_file, optarg);
		 break;
	case 26: if (optarg) {
		     amfree(star_target);
		     star_target = g_strdup(optarg);
		 }
		 break;
	case ':':
	case '?':
		break;
	}
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

    if (star_sparse_value) {
	if (strcasecmp(star_sparse_value, "NO") == 0) {
	    star_sparse = 0;
	} else if (strcasecmp(star_sparse_value, "YES") == 0) {
	    star_sparse = 1;
	} else {
	    delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701008, MSG_ERROR, 4,
			"value", star_sparse_value,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

    if (star_acl_value) {
	if (strcasecmp(star_acl_value, "NO") == 0){
	    star_acl = 0;
	} else if (strcasecmp(star_acl_value, "YES") == 0){
	    star_acl = 1;
	} else {
	    delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701009, MSG_ERROR, 4,
			"value", star_acl_value,
			"disk", argument.dle.disk,
			"device", argument.dle.device,
			"hostname", argument.host)));
	}
    }

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

    if (g_str_equal(command, "support")) {
	amstar_support(&argument);
    } else if (g_str_equal(command, "selfcheck")) {
	amstar_selfcheck(&argument);
    } else if (g_str_equal(command, "estimate")) {
	amstar_estimate(&argument);
    } else if (g_str_equal(command, "backup")) {
	amstar_backup(&argument);
    } else if (g_str_equal(command, "restore")) {
	amstar_restore(&argument);
    } else if (g_str_equal(command, "validate")) {
	amstar_validate(&argument);
    } else if (g_str_equal(command, "index")) {
	amstar_index(&argument);
    } else {
	fprintf(stderr, "Unknown command `%s'.\n", command);
	exit (1);
    }

    dbclose();

    return amstar_exit_value;
}

static char *validate_command_options(
    application_argument_t *argument)
{
    GSList    *copt;

    for (copt = argument->command_options; copt != NULL; copt = copt->next) {
	char *opt = (char *)copt->data;

	if (g_str_has_prefix(opt, "new-volume-script") ||
		g_str_has_prefix(opt, "-new-volume-script") ||
		g_str_has_prefix(opt, "--new-volume-script")) {
	   return opt;
	}
    }

    return NULL;
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
    fprintf(stdout, "MESSAGE-SELFCHECK-JSON YES\n");
    fprintf(stdout, "MESSAGE-XML NO\n");
    fprintf(stdout, "RECORD YES\n");
    fprintf(stdout, "INCLUDE-FILE YES\n");
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
    char *option;

    if (argument->dle.disk) {
	delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701000, MSG_INFO, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }

    delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701001, MSG_INFO, 4,
			"version", VERSION,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));

    delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701004, MSG_INFO, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));

    if (argument->dle.device) {
	delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701019, MSG_INFO, 4,
			"directory", star_target,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }
    if (star_target) {
	delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701018, MSG_INFO, 4,
			"directory", star_target,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }

    if (((argument->dle.include_list &&
	  argument->dle.include_list->nb_element >= 0) ||
         (argument->dle.include_file &&
	  argument->dle.include_file->nb_element >= 0)) &&
	((argument->dle.exclude_list &&
	  argument->dle.exclude_list->nb_element >= 0) ||
         (argument->dle.exclude_file &&
	  argument->dle.exclude_file->nb_element >= 0))) {
	delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701017, MSG_ERROR, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }

    if ((option = validate_command_options(argument))) {
	delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701014, MSG_ERROR, 4,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host,
			"command-options", option)));
    }

    if (star_path) {
	char *star_realpath = NULL;
	message_t *message;
	if ((message = check_exec_for_suid_message("STAR_PATH", star_path, &star_realpath))) {
	    delete_message(amstar_print_message(message));
	} else {
	    message = amstar_print_message(check_file_message(star_path, X_OK));
	    if (message && message_get_severity(message) <= MSG_INFO) {
		char *star_version;
                char line[1025];
		GPtrArray *argv_ptr = g_ptr_array_new();

		g_ptr_array_add(argv_ptr, star_realpath);
		g_ptr_array_add(argv_ptr, "--version");
		g_ptr_array_add(argv_ptr, NULL);

		star_version = get_first_line(line,sizeof(line),argv_ptr);

		if (star_version) {
		    char *sv, *sv1;
		    for (sv = star_version; *sv && !g_ascii_isdigit(*sv); sv++);
		    for (sv1 = sv; *sv1 && *sv1 != ' '; sv1++);
		    *sv1 = '\0';
		    delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701002, MSG_INFO, 4,
			"star-version", sv,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
		} else {
		    printf(_("ERROR Can't get %s version\n"), star_path);
		    delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701003, MSG_INFO, 4,
			"star-path", star_path,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
		}
	    }
	    if (message)
		delete_message(message);
	}
	if (star_realpath)
	    g_free(star_realpath);
    } else {
	delete_message(amstar_print_message(build_message(
			AMANDA_FILE, __LINE__, 3701005, MSG_ERROR, 3,
			"disk", argument->dle.disk,
			"device", argument->dle.device,
			"hostname", argument->host)));
    }

    delete_message(amstar_print_message(check_file_message(star_tardumps, W_OK)));

    if (argument->calcsize) {
	char *calcsize = g_strjoin(NULL, amlibexecdir, "/", "calcsize", NULL);
	delete_message(amstar_print_message(check_file_message(calcsize, X_OK)));
	delete_message(amstar_print_message(check_suid_message(calcsize)));
	amfree(calcsize);
    }

    {
	char *amandates_file;
	amandates_file = getconf_str(CNF_AMANDATES);
	delete_message(amstar_print_message(check_file_message(amandates_file, R_OK|W_OK)));
    }

    set_root_privs(1);
    if (argument->dle.device) {
	delete_message(amstar_print_message(check_dir_message(argument->dle.device, R_OK)));
    }
    set_root_privs(0);
}

static void
amstar_estimate(
    application_argument_t *argument)
{
    GPtrArray  *argv_ptr;
    int         nullfd;
    int         pipefd;
    FILE       *dumpout = NULL;
    off_t       size = -1;
    char        line[32768];
    char       *errmsg = NULL;
    char       *qerrmsg;
    char       *qdisk = NULL;
    amwait_t    wait_status;
    int         starpid;
    amregex_t  *rp;
    times_t     start_time;
    int         level = 0;
    GSList     *levels = NULL;
    char       *option;
    char       *star_realpath = NULL;
    message_t  *message;

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

    if ((option = validate_command_options(argument))) {
	fprintf(stdout, "ERROR Invalid '%s' COMMAND-OPTIONS\n", option);
	error("Invalid '%s' COMMAND-OPTIONS", option);
    }

    if (argument->calcsize) {
	char *dirname;

	if (star_target) {
	    dirname = star_target;
	} else {
	    dirname = argument->dle.device;
	}
	run_calcsize(argument->config, "STAR", argument->dle.disk, dirname,
		     argument->level, NULL, NULL);
	return;
    }

    qdisk = quote_string(argument->dle.disk);
    if (!star_path) {
	errmsg = g_strdup(_("STAR-PATH not defined"));
	goto common_error;
    }
    if ((message = check_exec_for_suid_message("STAR_PATH", star_path, &star_realpath))) {
	errmsg = g_strdup(get_message(message));
	delete_message(message);
	goto common_error;
    }

    if ((message = check_file_message(star_tardumps, W_OK))) {
	if (message_get_severity(message) > MSG_INFO) {
	    errmsg = g_strdup(get_message(message));
	    delete_message(message);
	    goto common_error;
	}
	delete_message(message);
    }

    start_time = curclock();

    qdisk = quote_string(argument->dle.disk);
    for (levels = argument->level; levels != NULL; levels = levels->next) {
	level = GPOINTER_TO_INT(levels->data);
	argv_ptr = amstar_build_argv(star_realpath, argument, level, CMD_ESTIMATE, NULL);

	if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	    errmsg = g_strdup_printf(_("Cannot access /dev/null : %s"),
				strerror(errno));
	    goto common_error;
	}

	starpid = pipespawnv(star_realpath, STDERR_PIPE, 1,
			     &nullfd, &nullfd, &pipefd,
			     (char **)argv_ptr->pdata);

	dumpout = fdopen(pipefd,"r");
	if (!dumpout) {
	    errmsg = g_strdup_printf(_("Can't fdopen: %s"), strerror(errno));
	    aclose(nullfd);
	    goto common_error;
	}

	size = (off_t)-1;
	while (size < 0 && (fgets(line, sizeof(line), dumpout)) != NULL) {
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

	while ((fgets(line, sizeof(line), dumpout)) != NULL) {
	    dbprintf("%s", line);
	}

	dbprintf(".....\n");
	dbprintf(_("estimate time for %s level %d: %s\n"),
		 qdisk,
		 level,
		 walltime_str(timessub(curclock(), start_time)));
	if(size == (off_t)-1) {
	    errmsg = g_strdup_printf(_("no size line match in %s output"),
				star_realpath);
	    dbprintf(_("%s for %s\n"), errmsg, qdisk);
	    dbprintf(".....\n");
	    qerrmsg = quote_string(errmsg);
	    fprintf(stdout, "ERROR %s\n", qerrmsg);
	    amfree(errmsg);
	    amfree(qerrmsg);
	} else if(size == (off_t)0 && argument->level == 0) {
	    dbprintf(_("possible %s problem -- is \"%s\" really empty?\n"),
		     star_realpath, argument->dle.disk);
	    dbprintf(".....\n");
	}
	dbprintf(_("estimate size for %s level %d: %lld KB\n"),
		 qdisk,
		 level,
		 (long long)size);

	(void)kill(-starpid, SIGTERM);

	dbprintf(_("waiting for %s \"%s\" child\n"), star_realpath, qdisk);
	waitpid(starpid, &wait_status, 0);
	if (WIFSIGNALED(wait_status)) {
	    amfree(errmsg);
	    errmsg = g_strdup_printf(_("%s terminated with signal %d: see %s"),
				star_realpath, WTERMSIG(wait_status), dbfn());
	    dbprintf(_("%s for %s\n"), errmsg, qdisk);
	    dbprintf(".....\n");
	    qerrmsg = quote_string(errmsg);
	    fprintf(stdout, "ERROR %s\n", qerrmsg);
	    amfree(errmsg);
	    amfree(qerrmsg);
	} else if (WIFEXITED(wait_status)) {
	    if (WEXITSTATUS(wait_status) != 0) {
		amfree(errmsg);
		errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
				    star_realpath, WEXITSTATUS(wait_status), dbfn());
		dbprintf(_("%s for %s\n"), errmsg, qdisk);
		dbprintf(".....\n");
		qerrmsg = quote_string(errmsg);
		fprintf(stdout, "ERROR %s\n", qerrmsg);
		amfree(errmsg);
		amfree(qerrmsg);
	    } else {
		/* Normal exit */
	    }
	} else {
	    amfree(errmsg);
	    errmsg = g_strdup_printf(_("%s got bad exit: see %s"), star_realpath, dbfn());
	    dbprintf(_("%s for %s\n"), errmsg, qdisk);
	    dbprintf(".....\n");
	    qerrmsg = quote_string(errmsg);
	    fprintf(stdout, "ERROR %s\n", qerrmsg);
	    amfree(errmsg);
	    amfree(qerrmsg);
	}
	dbprintf(_("after %s %s wait\n"), star_realpath, qdisk);

	g_ptr_array_free_full(argv_ptr);

	aclose(nullfd);
	afclose(dumpout);

	fprintf(stdout, "%d %lld 1\n", level, (long long)size);
    }
    amfree(qdisk);
    amfree(star_realpath);
    return;

common_error:
    dbprintf("%s\n", errmsg);
    qerrmsg = quote_string(errmsg);
    amfree(qdisk);
    dbprintf("%s", errmsg);
    fprintf(stdout, "ERROR %s\n", qerrmsg);
    amfree(errmsg);
    amfree(qerrmsg);
    amfree(star_realpath);
}

static void
amstar_backup(
    application_argument_t *argument)
{
    int        dumpin;
    char      *qdisk;
    char       line[32768];
    amregex_t *rp;
    off_t      dump_size = -1;
    char      *type;
    char       startchr;
    GPtrArray *argv_ptr;
    int        starpid;
    int        dataf = 1;
    int        indexf = 4;
    int        outf;
    FILE      *indexstream = NULL;
    FILE      *outstream;
    int        level;
    regex_t    regex_root;
    regex_t    regex_dir;
    regex_t    regex_file;
    regex_t    regex_special;
    regex_t    regex_symbolic;
    regex_t    regex_hard;
    char      *option;
    char      *star_realpath = NULL;
    message_t *message;

    if (!argument->level) {
	fprintf(mesgstream, "sendbackup: error [No level argument]\n");
	error(_("No level argument"));
    }
    if (!argument->dle.disk) {
	fprintf(mesgstream, "sendbackup: error [No disk argument]\n");
	error(_("No disk argument"));
    }
    if (!argument->dle.device) {
	fprintf(mesgstream, "sendbackup: error [No device argument]\n");
	error(_("No device argument"));
    }

    if ((message = check_exec_for_suid_message("STAR_PATH", star_path, &star_realpath))) {
	fprintf(mesgstream, "sendbackup: error [%s]", get_message(message));
	exit(1);
    }

    if ((message = check_file_message(star_tardumps, W_OK))) {
	if (message_get_severity(message) > MSG_INFO) {
	    fprintf(mesgstream, "sendbackup: error [%s]", get_message(message));
	    delete_message(message);
	    exit(1);
	}
	delete_message(message);
    }

    if ((option = validate_command_options(argument))) {
	fprintf(mesgstream, "sendbackup: error [Invalid '%s' COMMAND-OPTIONS]\n", option);
	exit(1);
    }

    if (argument->dle.include_list &&
	argument->dle.include_list->nb_element >= 0) {
	fprintf(mesgstream, "sendbackup: error [include-list not supported for backup]\n");
    }

    level = GPOINTER_TO_INT(argument->level->data);

    qdisk = quote_string(argument->dle.disk);

    argv_ptr = amstar_build_argv(star_realpath, argument, level, CMD_BACKUP, mesgstream);

    starpid = pipespawnv(star_realpath, STDIN_PIPE|STDERR_PIPE, 1,
			 &dumpin, &dataf, &outf, (char **)argv_ptr->pdata);

    g_ptr_array_free_full(argv_ptr);
    /* close the write ends of the pipes */
    aclose(dumpin);
    aclose(dataf);
    if (argument->dle.create_index) {
	indexstream = fdopen(indexf, "w");
	if (!indexstream) {
	    fprintf(mesgstream, "sendbackup: error [error indexstream(%d): %s]\n", indexf, strerror(errno));
	    error(_("error indexstream(%d): %s\n"), indexf, strerror(errno));
	}
    }
    outstream = fdopen(outf, "r");
    if (!outstream) {
	fprintf(mesgstream, "sendbackup: error [error outstream(%d): %s]\n", outf, strerror(errno));
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

	if (strlen(line) > 0 && line[strlen(line)-1] == '\n') {
	    /* remove trailling \n */
	    line[strlen(line)-1] = '\0';
	}

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
    fclose(outstream);

    regfree(&regex_root);
    regfree(&regex_dir);
    regfree(&regex_file);
    regfree(&regex_special);
    regfree(&regex_symbolic);
    regfree(&regex_hard);

    dbprintf(_("gnutar: %s: pid %ld\n"), star_realpath, (long)starpid);

    dbprintf("sendbackup: size %lld\n", (long long)dump_size);
    fprintf(mesgstream, "sendbackup: size %lld\n", (long long)dump_size);

    fclose(mesgstream);
    if (argument->dle.create_index)
	fclose(indexstream);

    amfree(qdisk);
    amfree(star_realpath);
}

static void
amstar_restore(
    application_argument_t *argument)
{
    GPtrArray  *argv_ptr = g_ptr_array_new();
    int         j;
    char       *star_realpath = NULL;
    message_t  *message;
    int         datain = 0;
    int         outf;
    int         errf;
    int         star_pid;
    amwait_t    wait_status;
    char       *errmsg = NULL;

    if (!star_path) {
	error(_("STAR-PATH not defined"));
    }

    if ((message = check_exec_for_suid_message("STAR_PATH", star_path, &star_realpath))) {
	fprintf(stderr, "%s\n", get_message(message));
	delete_message(message);
	exit(1);
    }

    if (!security_allow_to_restore()) {
	error("The user is not allowed to restore files");
    }

    g_ptr_array_add(argv_ptr, g_strdup(star_realpath));
    if (star_target) {
	struct stat stat_buf;
	if(stat(star_target, &stat_buf) != 0) {
	    fprintf(stderr,"can not stat directory %s: %s\n", star_target, strerror(errno));
	    exit(1);
	}
	if (!S_ISDIR(stat_buf.st_mode)) {
	    fprintf(stderr,"%s is not a directory\n", star_target);
	    exit(1);
	}
	if (access(star_target, W_OK) != 0 ) {
	    fprintf(stderr, "Can't write to %s: %s\n", star_target, strerror(errno));
	    exit(1);
	}

	g_ptr_array_add(argv_ptr, g_strdup("-C"));
	g_ptr_array_add(argv_ptr, g_strdup(star_target));
    }
    g_ptr_array_add(argv_ptr, g_strdup("-x"));
    g_ptr_array_add(argv_ptr, g_strdup("-v"));
    g_ptr_array_add(argv_ptr, g_strdup("-xattr"));
    g_ptr_array_add(argv_ptr, g_strdup("-acl"));
    g_ptr_array_add(argv_ptr, g_strdup("errctl=WARN|SAMEFILE|SETTIME|DIFF|SETACL|SETXATTR|SETMODE|BADACL *"));
    g_ptr_array_add(argv_ptr, g_strdup("-no-fifo"));
    g_ptr_array_add(argv_ptr, g_strdup("-f"));
    g_ptr_array_add(argv_ptr, g_strdup("-"));

    if (argument->dle.exclude_list &&
	argument->dle.exclude_list->nb_element == 1) {
	g_ptr_array_add(argv_ptr, g_strdup("-exclude-from"));
	g_ptr_array_add(argv_ptr,
			g_strdup(argument->dle.exclude_list->first->name));
    }

    if (argument->dle.include_list &&
	argument->dle.include_list->nb_element == 1) {
	FILE *include_list = fopen(argument->dle.include_list->first->name, "r");
	if (include_list) {
	    char  line[2*PATH_MAX+2];
	    while (fgets(line, 2*PATH_MAX, include_list)) {
		line[strlen(line)-1] = '\0'; /* remove '\n' */
		if (g_str_has_prefix(line, "./"))
		    g_ptr_array_add(argv_ptr, g_strdup(line+2)); /* remove ./ */
		else if (!g_str_equal(line, "."))
		    g_ptr_array_add(argv_ptr, g_strdup(line));
	    }
	    fclose(include_list);
	}
    }
    for (j=1; j< argument->argc; j++) {
	if (g_str_has_prefix(argument->argv[j], "./"))
	    g_ptr_array_add(argv_ptr, g_strdup(argument->argv[j]+2));/*remove ./ */
	else if (!g_str_equal(argument->argv[j], "."))
	    g_ptr_array_add(argv_ptr, g_strdup(argument->argv[j]));
    }
    g_ptr_array_add(argv_ptr, NULL);

    debug_executing(argv_ptr);
    star_pid = pipespawnv(star_realpath, STDOUT_PIPE|STDERR_PIPE, 1,
			&datain, &outf, &errf, (char **)argv_ptr->pdata);
    aclose(datain);

    read_fd(outf, "restore stdout", &read_text);
    read_fd(errf, "restore stderr", &read_text);

    event_loop(0);
    waitpid(star_pid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = g_strdup_printf(_("%s terminated with signal %d: see %s"),
			star_path, WTERMSIG(wait_status), dbfn());
	amstar_exit_value = 1;
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
			star_path, WEXITSTATUS(wait_status), dbfn());
	    amstar_exit_value = 1;
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
			star_path, dbfn());
	amstar_exit_value = 1;
    }

    if (errmsg) {
	g_debug("%s", errmsg);
	fprintf(stderr,"%s\n", errmsg);
    }
    g_ptr_array_free_full(argv_ptr);
    return;
}

static void
amstar_validate(
    application_argument_t *argument G_GNUC_UNUSED)
{
    char       *cmd;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    char        buf[32768];
    int         datain = 0;
    int         outf;
    int         errf;
    int         star_pid;
    amwait_t    wait_status;
    char       *errmsg = NULL;

    set_root_privs(-1);

    if (!star_path) {
	dbprintf("STAR-PATH not set; Piping to /dev/null\n");
	fprintf(stderr,"STAR-PATH not set; Piping to /dev/null\n");
	goto pipe_to_null;
    }

    cmd = g_strdup(star_path);

    g_ptr_array_add(argv_ptr, g_strdup(star_path));
    g_ptr_array_add(argv_ptr, g_strdup("-t"));
    g_ptr_array_add(argv_ptr, g_strdup("-f"));
    g_ptr_array_add(argv_ptr, g_strdup("-"));
    g_ptr_array_add(argv_ptr, NULL);

    debug_executing(argv_ptr);
    star_pid = pipespawnv(cmd, STDOUT_PIPE|STDERR_PIPE, 0,
			&datain, &outf, &errf, (char **)argv_ptr->pdata);
    aclose(datain);

    read_fd(outf, "validate stdout", &read_text);
    read_fd(errf, "validate stderr", &read_text);

    event_loop(0);
    waitpid(star_pid, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	errmsg = g_strdup_printf(_("%s terminated with signal %d: see %s"),
			star_path, WTERMSIG(wait_status), dbfn());
	amstar_exit_value = 1;
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
			star_path, WEXITSTATUS(wait_status), dbfn());
	    amstar_exit_value = 1;
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
			star_path, dbfn());
	amstar_exit_value = 1;
    }

    if (errmsg) {
	g_debug("%s", errmsg);
	fprintf(stderr,"%s\n", errmsg);
    }
    g_ptr_array_free_full(argv_ptr);
    return;

pipe_to_null:
    while (read(0, buf, 32768) > 0) {
    }
    g_ptr_array_free_full(argv_ptr);
}

static void
amstar_index(
    application_argument_t *argument G_GNUC_UNUSED)
{
    char       *cmd;
    GPtrArray  *argv_ptr = g_ptr_array_new();
    int         datain = 0;
    int         indexf;
    int         errf = 2;
    pid_t       tarpid;
    FILE       *indexstream;
    char        line[32768];
    amwait_t    wait_status;
    char       *errmsg = NULL;

    if (!star_path) {
	dbprintf("STAR-PATH not set; Piping to /dev/null\n");
	fprintf(stderr,"STAR-PATH not set; Piping to /dev/null\n");
	while (read(0, line, 32768) > 0) {
	}
	exit(1);
    }

    cmd = g_strdup(star_path);

    g_ptr_array_add(argv_ptr, g_strdup(star_path));
    g_ptr_array_add(argv_ptr, g_strdup("-t"));
    g_ptr_array_add(argv_ptr, g_strdup("-f"));
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
	if (WEXITSTATUS(wait_status) > 0) {
	    errmsg = g_strdup_printf(_("%s exited with status %d: see %s"),
				     cmd, WEXITSTATUS(wait_status), dbfn());
	} else {
	    /* Normal exit */
	}
    } else {
	errmsg = g_strdup_printf(_("%s got bad exit: see %s"),
				 cmd, dbfn());
    }
    dbprintf(_("amstar: %s: pid %ld\n"), cmd, (long)tarpid);
    if (errmsg) {
	dbprintf("%s", errmsg);
	fprintf(stderr, "error [%s]\n", errmsg);
    }

    amfree(cmd);
}

static GPtrArray *amstar_build_argv(
    char *star_realpath,
    application_argument_t *argument,
    int   level,
    int   command,
    FILE *mesgstream)
{
    char      *dirname;
    char      *fsname;
    char       levelstr[NUM_STR_SIZE+7];
    GPtrArray *argv_ptr = g_ptr_array_new();
    char      *s;
    char      *tardumpfile;
    GSList    *copt;

    if (star_target) {
	dirname = star_target;
    } else {
	dirname = argument->dle.device;
    }
    fsname = g_strjoin(NULL, "fs-name=", dirname, NULL);
    for (s = fsname; *s != '\0'; s++) {
	if (iscntrl((int)*s))
	    *s = '-';
    }
    snprintf(levelstr, sizeof(levelstr), "-level=%d", level);

    if (star_dle_tardumps) {
	char *sdisk = sanitise_filename(argument->dle.disk);
	tardumpfile = g_strjoin(NULL, star_tardumps, sdisk, NULL);
	amfree(sdisk);
    } else {
	tardumpfile = g_strdup(star_tardumps);
    }

    g_ptr_array_add(argv_ptr, g_strdup(star_realpath));

    g_ptr_array_add(argv_ptr, g_strdup("-c"));
    g_ptr_array_add(argv_ptr, g_strdup("-f"));
    if (command == CMD_ESTIMATE) {
	g_ptr_array_add(argv_ptr, g_strdup("/dev/null"));
    } else {
	g_ptr_array_add(argv_ptr, g_strdup("-"));
    }
    g_ptr_array_add(argv_ptr, g_strdup("-C"));

#if defined(__CYGWIN__)
    {
	char tmppath[PATH_MAX];

	cygwin_conv_to_full_posix_path(dirname, tmppath);
	g_ptr_array_add(argv_ptr, g_strdup(tmppath));
    }
#else
    g_ptr_array_add(argv_ptr, g_strdup(dirname));
#endif
    g_ptr_array_add(argv_ptr, g_strdup(fsname));
    if (star_onefilesystem)
	g_ptr_array_add(argv_ptr, g_strdup("-xdev"));
    g_ptr_array_add(argv_ptr, g_strdup("-link-dirs"));
    g_ptr_array_add(argv_ptr, g_strdup(levelstr));
    g_ptr_array_add(argv_ptr, g_strconcat("tardumps=", tardumpfile, NULL));
    if (command == CMD_BACKUP)
	g_ptr_array_add(argv_ptr, g_strdup("-wtardumps"));

    g_ptr_array_add(argv_ptr, g_strdup("-xattr"));
    if (star_acl)
	g_ptr_array_add(argv_ptr, g_strdup("-acl"));
    g_ptr_array_add(argv_ptr, g_strdup("H=exustar"));
    g_ptr_array_add(argv_ptr, g_strdup("errctl=WARN|SAMEFILE|DIFF|GROW|SHRINK|SPECIALFILE|GETXATTR|BADACL *"));
    if (star_sparse)
	g_ptr_array_add(argv_ptr, g_strdup("-sparse"));
    g_ptr_array_add(argv_ptr, g_strdup("-dodesc"));

    if (command == CMD_BACKUP && argument->dle.create_index)
	g_ptr_array_add(argv_ptr, g_strdup("-v"));

    if (((argument->dle.include_file &&
	  argument->dle.include_file->nb_element >= 1) ||
	 (argument->dle.include_list &&
	  argument->dle.include_list->nb_element >= 1)) &&
	((argument->dle.exclude_file &&
	  argument->dle.exclude_file->nb_element >= 1) ||
	 (argument->dle.exclude_list &&
	  argument->dle.exclude_list->nb_element >= 1))) {

	if (mesgstream && command == CMD_BACKUP) {
	    fprintf(mesgstream, "? include and exclude specified, disabling exclude\n");
	}
	free_sl(argument->dle.exclude_file);
	argument->dle.exclude_file = NULL;
	free_sl(argument->dle.exclude_list);
	argument->dle.exclude_list = NULL;
    }

    if ((argument->dle.exclude_file &&
	 argument->dle.exclude_file->nb_element >= 1) ||
	(argument->dle.exclude_list &&
	 argument->dle.exclude_list->nb_element >= 1)) {
	g_ptr_array_add(argv_ptr, g_strdup("-match-tree"));
	g_ptr_array_add(argv_ptr, g_strdup("-not"));
    }
    if (argument->dle.exclude_file &&
	argument->dle.exclude_file->nb_element >= 1) {
	sle_t *excl;
	for (excl = argument->dle.exclude_file->first; excl != NULL;
	     excl = excl->next) {
	    char *ex;
	    if (strncmp(excl->name, "./", 2) == 0) {
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
		while ((aexc = pgets(exclude)) != NULL) {
		    if (aexc[0] != '\0') {
			char *ex;
			if (strncmp(aexc, "./", 2) == 0) {
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

    if (argument->dle.include_file &&
	argument->dle.include_file->nb_element >= 1) {
	sle_t *excl;
	for (excl = argument->dle.include_file->first; excl != NULL;
	     excl = excl->next) {
	    char *ex;
	    if (strncmp(excl->name, "./", 2) == 0) {
		ex = g_strdup_printf("pat=%s", excl->name+2);
	    } else {
		ex = g_strdup_printf("pat=%s", excl->name);
	    }
	    g_ptr_array_add(argv_ptr, ex);
	}
    }
    if (argument->dle.include_list &&
	argument->dle.include_list->nb_element >= 1) {
	sle_t *excl;
	for (excl = argument->dle.include_list->first; excl != NULL;
	     excl = excl->next) {
	    char *exclname = fixup_relative(excl->name, argument->dle.device);
	    FILE *include;
	    char *aexc;
	    if ((include = fopen(exclname, "r")) != NULL) {
		while ((aexc = pgets(include)) != NULL) {
		    if (aexc[0] != '\0') {
			char *ex;
			if (strncmp(aexc, "./", 2) == 0) {
			    ex = g_strdup_printf("pat=%s", aexc+2);
			} else {
			    ex = g_strdup_printf("pat=%s", aexc);
			}
			g_ptr_array_add(argv_ptr, ex);
		    }
		    amfree(aexc);
		}
		fclose(include);
	    }
	    amfree(exclname);
	}
    }

    /* It is best to place command_options at the and of command line.
     * For example '-find' option requires that it is the last option used.
     * See: http://cdrecord.berlios.de/private/man/star/star.1.html
     */
    for (copt = argument->command_options; copt != NULL; copt = copt->next) {
	g_ptr_array_add(argv_ptr, g_strdup((char *)copt->data));
    }

    g_ptr_array_add(argv_ptr, g_strdup("."));

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
    if (!stat(argument->dle.device, &stat_buf)) {
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

