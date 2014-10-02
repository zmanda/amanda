/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
 * Copyright (c) 2007-2014 Zmanda, Inc.  All Rights Reserved.
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

#include "amanda.h"
#include "amandad.h"
#include "match.h"
#include "clock.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "getfsent.h"
#include "conffile.h"
#include "amandates.h"
#include "stream.h"
#include "amxml.h"
#include "client_util.h"
#include "ammessage.h"

#define senddiscover_debug(i, ...) do {	\
	if ((i) <= debug_sendbackup) {	\
	    g_debug(__VA_LIST__);	\
	}				\
} while (0)

#define TIMEOUT 30

pid_t application_api_pid = (pid_t)-1;

g_option_t *g_options = NULL;

dle_t *gdle = NULL;

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static char *amandad_auth = NULL;

/* local functions */
int main(int argc, char **argv);
char *childstr(pid_t pid);
int check_status(pid_t pid, amwait_t w);

int check_result(void);

int
main(
    int		argc,
    char **	argv)
{
    dle_t *dle = NULL;
    char *line = NULL;
    GSList *errlist;

    if (argc > 1 && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("senddiscover-%s\n", VERSION);
	return (0);
    }

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

    safe_fd(DATA_FD_OFFSET, DATA_FD_COUNT*2);
    openbsd_fd_inform();

    safe_cd();

    set_pname("senddiscover");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    /* Don't die when interrupt received */
    signal(SIGINT, SIG_IGN);

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_syslog);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();
    g_debug("Version %s", VERSION);

    if(argc > 2 && g_str_equal(argv[1], "amandad")) {
	amandad_auth = g_strdup(argv[2]);
    }

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);
    /* (check for config errors comes later) */

    check_running_as(RUNNING_AS_CLIENT_LOGIN);


    for(; (line = agets(stdin)) != NULL; free(line)) {
	if (line[0] == '\0')
	    continue;
	if(strncmp_const(line, "OPTIONS ") == 0) {
	    g_options = parse_g_options(line+8, 1);
	    if(!g_options->hostname) {
		g_options->hostname = g_malloc(MAX_HOSTNAME_LENGTH+1);
		gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
	    }

	    if (g_options->config) {
		/* overlay this configuration on the existing (nameless) configuration */
		config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
			    g_options->config);

		dbrename(get_config_name(), DBG_SUBDIR_CLIENT);
	    }

	    /* check for any config errors now */
	    if (config_errors(&errlist) >= CFGERR_ERRORS) {
		g_printf("OPTIONS \n");
		g_printf("[\n");
		config_print_errors_as_message();
		goto err;
	    }

	    if (am_has_feature(g_options->features, fe_req_xml)) {
		break;
	    }
	    continue;
	}

    }
    amfree(line);
    if (g_options == NULL) {
	g_printf("OPTIONS \n");
	g_printf("[\n");
	delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900004, 16, 0)));
	goto err;
    }
    g_printf("OPTIONS ");
    if(am_has_feature(g_options->features, fe_rep_options_features)) {
	g_printf("features=%s;", our_feature_string);
    }
    if(am_has_feature(g_options->features, fe_rep_options_hostname)) {
	g_printf("hostname=%s;", g_options->hostname);
    }
    if (!am_has_feature(g_options->features, fe_rep_options_features) &&
	!am_has_feature(g_options->features, fe_rep_options_hostname)) {
	g_printf(";");
    }
    g_printf("\n");
    g_printf("[\n");
    fflush(stdout);

    if (am_has_feature(g_options->features, fe_req_xml)) {
	char *errmsg = NULL;

	dle = amxml_parse_node_FILE(stdin, &errmsg);
	if (errmsg) {
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900013, 16, 1,
			"errmsg", errmsg)));
	    goto err;
	}
	if (!dle) {
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900014, 16, 0)));
	    goto err;
	} else if (dle->next) {
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900015, 16, 0)));
	    goto err;
	}

    } else {
	delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900007, 16, 0)));
	goto err;
    }
    gdle = dle;

    if (dle->program   == NULL) {
	delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900008, 16, 0)));
	goto err;
    }

    g_debug("  Parsed request as: program '%s'", dle->program);

    if (dle->program_is_application_api == 0) {
	delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900003, 16, 0)));
	goto err;
    }

    if (dle->auth && amandad_auth) {
	if (strcasecmp(dle->auth, amandad_auth) != 0) {
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900009, 16, 2,
			"amandad_auth", amandad_auth,
			"dle_auth", dle->auth)));
	    goto err;
	}
    }

    if (merge_dles_properties(dle, 0) == 0) {
	delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900003, 16, 0)));
	goto err;
    }
    if (dle->program_is_application_api == 1) {
	guint j;
	char *cmd=NULL;
	GPtrArray *argv_ptr;
	backup_support_option_t *bsu;
	int        result;
	GPtrArray *errarray;
	int        errfd[2];
	FILE      *dumperr;

	bsu = backup_support_option(dle->program, &errarray);
	if (!bsu) {
	    char  *errmsg;
	    guint  i;
	    for (i=0; i < errarray->len; i++) {
		errmsg = g_ptr_array_index(errarray, i);
		delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900000, 16, 2,
			"application", dle->program,
			"errmsg", errmsg)));
	    }
	    if (i == 0) { /* no errarray */
		delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900000, 16, 1,
			"application", dle->program)));
	    }
	    g_ptr_array_free_full(errarray);
	    goto err;
	}

	if (!bsu->discover) {
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900002, 16, 1,
			"application", dle->program)));
	    goto err;
	}
	if (pipe(errfd) < 0) {
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900005, 16, 1,
			"application", dle->program)));
	    goto err;
	}

	switch(application_api_pid=fork()) {
	case 0: /* child */

	    argv_ptr = g_ptr_array_new();
	    cmd = g_strjoin(NULL, APPLICATION_DIR, "/", dle->program, NULL);
	    g_ptr_array_add(argv_ptr, g_strdup(dle->program));
	    g_ptr_array_add(argv_ptr, g_strdup("discover"));
	    if (bsu->message_line == 1) {
		g_ptr_array_add(argv_ptr, g_strdup("--message"));
		g_ptr_array_add(argv_ptr, g_strdup("json"));
	    }
	    if (g_options->config && bsu->config == 1) {
		g_ptr_array_add(argv_ptr, g_strdup("--config"));
		g_ptr_array_add(argv_ptr, g_strdup(g_options->config));
	    }
	    g_ptr_array_add(argv_ptr, g_strdup("--device"));
	    g_ptr_array_add(argv_ptr, g_strdup(dle->device));
	    if (g_options->hostname && bsu->host == 1) {
		g_ptr_array_add(argv_ptr, g_strdup("--host"));
		g_ptr_array_add(argv_ptr, g_strdup(g_options->hostname));
	    }
	    application_property_add_to_argv(argv_ptr, dle, bsu,
					     g_options->features);

	    g_ptr_array_add(argv_ptr, NULL);
	    g_debug("%s: running \"%s", get_pname(), cmd);
	    for (j = 1; j < argv_ptr->len - 1; j++)
		g_debug(" %s", (char *)g_ptr_array_index(argv_ptr,j));
	    g_debug("\"");
	    if (dup2(errfd[1], 2) == -1) {
		int save_errno = errno;
	        delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900006, 16, 1,
			"errno", save_errno)));
		goto err;
	    }
	    if (dup2(1, 3) == -1) {
		int save_errno = errno;
	        delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900006, 16, 1,
			"errno", save_errno)));
		goto err;
	    }
	    safe_fd(3, 1);
	    execve(cmd, (char **)argv_ptr->pdata, safe_env());
	    goto err;
	    break;

	default:
	    break;
	case -1: {
	    int save_errno = errno;
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900010, 16, 2,
			"application", dle->program,
			"errno", save_errno)));
	    goto err;
	  }
	}

	close(errfd[1]);
	dumperr = fdopen(errfd[0],"r");
	if (!dumperr) {
	    int save_errno = errno;
	    delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900011, 16, 1,
			"errno", save_errno)));
	    goto err;
	}

	result = 0;
	while ((line = agets(dumperr)) != NULL) {
	    if (strlen(line) > 0) {
		delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900012, 16, 2,
			"application", dle->program,
			"errmsg", line)));
		result = 1;
	    }
	    amfree(line);
	}

	result |= check_result();

	amfree(bsu);
    }

    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;
    free_g_options(g_options);

err:
    g_printf("]\n");
    dbclose();
    return 0;
}


/*
 * Returns a string for a child process.  Checks the saved dump and
 * compress pids to see which it is.
 */

char *
childstr(
    pid_t pid)
{
    if (pid == application_api_pid) {
	if (!gdle) {
	    g_debug("gdle == NULL");
	    return "gdle == NULL";
	}
	return gdle->program;
    }
    return "unknown";
}


/*
 * Determine if the child return status really indicates an error.
 * If so, add the error message to the error string; more than one
 * child can have an error.
 */

int
check_status(
    pid_t	pid,
    amwait_t	w)
{
    char *str;
    int ret, sig, rc;

    str = childstr(pid);

    if (WIFSIGNALED(w)) {
	ret = 0;
	rc = sig = WTERMSIG(w);
    } else {
	sig = 0;
	rc = ret = WEXITSTATUS(w);
    }

    if (pid == application_api_pid)
	application_api_pid = -1;

    if (rc == 0) {
	return 0;				/* normal exit */
    }

    if (ret == 0) {
	delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900016, 16, 3,
			"application", str,
			"pid", g_strdup_printf("%d", (int)pid),
			"signal", g_strdup_printf("%d", (int)sig))));
    } else {
	delete_message(fprint_message(stdout, build_message(
			__FILE__, __LINE__, 2900017, 16, 3,
			"application", str,
			"pid", g_strdup_printf("%d", (int)pid),
			"return_code", g_strdup_printf("%d", (int)ret))));
    }

    return 1;
}


int
check_result(void)
{
    int goterror;
    pid_t wpid;
    amwait_t retstat;

    goterror = 0;


    while ((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	if (check_status(wpid, retstat))
	    goterror = 1;
    }

    if (application_api_pid != -1) {
	sleep(5);
	while ((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	    if (check_status(wpid, retstat))
		goterror = 1;
	}
    }
    if (application_api_pid != -1) {
	g_debug("Sending SIGHUP to application process %d",
		  (int)application_api_pid);
	if (application_api_pid != -1) {
	    if (kill(application_api_pid, SIGHUP) == -1) {
		g_debug("Can't send SIGHUP to %d: %s",
			  (int)application_api_pid,
			  strerror(errno));
	    }
	}
	sleep(5);
	while ((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	    if (check_status(wpid, retstat))
		goterror = 1;
	}
    }
    if (application_api_pid != -1) {
	g_debug("Sending SIGKILL to application process %d",
		  (int)application_api_pid);
	if (application_api_pid != -1) {
	    if (kill(application_api_pid, SIGKILL) == -1) {
		g_debug("Can't send SIGKILL to %d: %s",
			  (int)application_api_pid,
			  strerror(errno));
	    }
	}
	sleep(5);
	while ((wpid = waitpid((pid_t)-1, &retstat, WNOHANG)) > 0) {
	    if (check_status(wpid, retstat))
		goterror = 1;
	}
    }

    return goterror;
}

