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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/* 
 * $Id: selfcheck.c 10421 2008-03-06 18:48:30Z martineau $
 *
 * do self-check and send back any error messages
 */

#include "amanda.h"
#include "fsusage.h"
#include "getfsent.h"
#include "amandates.h"
#include "clock.h"
#include "amutil.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "client_util.h"
#include "conffile.h"
#include "amandad.h"
#include "ammessage.h"
#include "amxml.h"
#include "base64.h"
#include "security-file.h"

#ifdef SAMBA_CLIENT
#include "findpass.h"
#endif

int need_samba=0;
int need_rundump=0;
int need_dump=0;
int need_restore=0;
int need_vdump=0;
int need_vrestore=0;
int need_xfsdump=0;
int need_xfsrestore=0;
int need_vxdump=0;
int need_vxrestore=0;
int need_runtar=0;
int need_gnutar=0;
int need_compress_path=0;
int need_calcsize=0;
int need_global_check=0;
int program_is_application_api=0;

static char *amandad_auth = NULL;
static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static g_option_t *g_options = NULL;

/* local functions */
int main(int argc, char **argv);

static void check_options(dle_t *dle);
static void check_disk(dle_t *dle);
static void check_overall(void);
static int check_file_exist(char *filename);
static void check_space(char *dir, off_t kbytes);
static void print_platform(void);

static message_t *
selfcheck_fprint_message(
    FILE *stream,
    message_t *message)
{
    if (message == NULL)
	return NULL;

    if (am_has_feature(g_options->features, fe_selfcheck_message)) {
	fprint_message(stream, message);
    } else {
	if (message_get_code(message) == 3600004) {
	    fprintf(stream, "%s\n", get_quoted_message(message));
	} else if (message_get_severity(message) == MSG_INFO) {
	    fprintf(stream, "OK %s\n", get_quoted_message(message));
	} else if (message_get_severity(message) == MSG_ERROR) {
	    fprintf(stream, "ERROR [%s]\n", get_quoted_message(message));
	} else {
	    fprintf(stream, "%s\n", get_message(message));
	}
    }
    return message;
}

static message_t *
selfcheck_print_message(
    message_t *message)
{
    if (message == NULL)
	return NULL;

    return selfcheck_fprint_message(stdout, message);
}

int
main(
    int		argc,
    char **	argv)
{
    char *line = NULL;
    char *qdisk = NULL;
    char *qamdevice = NULL;
    char *optstr = NULL;
    char *err_extra = NULL;
    char *s, *fp;
    int ch;
    dle_t *dle = NULL;
    int level;
    GSList *errlist;
    am_level_t *alevel;

    if (argc > 1 && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("selfcheck-%s\n", VERSION);
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

    set_pname("selfcheck");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    add_amanda_log_handler(amanda_log_stderr);
    add_amanda_log_handler(amanda_log_syslog);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();

    if(argc > 2 && g_str_equal(argv[1], "amandad")) {
	amandad_auth = g_strdup(argv[2]);
    }

    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);
    /* (check for config errors comes later) */

    check_running_as(RUNNING_AS_CLIENT_LOGIN);

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    /* handle all service requests */

    /*@ignore@*/
    for(; (line = agets(stdin)) != NULL; free(line)) {
    /*@end@*/
	if (line[0] == '\0')
	    continue;

	if(strncmp_const(line, "OPTIONS ") == 0) {
	    if (g_options) {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600003, MSG_ERROR, 0)));
		exit(1);
		/*NOTREACHED*/
	    }
	    g_options = parse_g_options(line+8, 1);
	    if(!g_options->hostname) {
		g_options->hostname = g_malloc(MAX_HOSTNAME_LENGTH+1);
		gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
	    }

	    if (am_has_feature(g_options->features, fe_selfcheck_message))
		printf("MESSAGE JSON\n");
	    delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600000, MSG_INFO, 2,
		"version", VERSION,
		"hostname", g_options->hostname)));
	    print_platform();

	    delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600004, MSG_INFO, 2,
		"features", our_feature_string,
		"hostname", g_options->hostname)));

	    if (g_options->config) {
		/* overlay this configuration on the existing (nameless) configuration */
		config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
			    g_options->config);

		dbrename(get_config_name(), DBG_SUBDIR_CLIENT);
	    }

	    /* check for any config errors now */
	    if (config_errors(&errlist) >= CFGERR_ERRORS) {
		char *errstr = config_errors_to_error_string(errlist);
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600005, MSG_ERROR, 2,
			"errstr", errstr,
			"hostname", g_options->hostname)));
		amfree(errstr);
		amfree(line);
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

	skip_whitespace(s, ch);			/* find program name */
	if (ch == '\0') {
	    goto err;				/* no program */
	}
	dle->program = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';				/* terminate the program name */

	dle->program_is_application_api = 0;
	if(g_str_equal(dle->program, "APPLICATION")) {
	    dle->program_is_application_api = 1;
	    skip_whitespace(s, ch);		/* find dumper name */
	    if (ch == '\0') {
		goto err;			/* no program */
	    }
	    dle->program = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';			/* terminate the program name */
	}

	if(strncmp_const(dle->program, "CALCSIZE") == 0) {
	    skip_whitespace(s, ch);		/* find program name */
	    if (ch == '\0') {
		goto err;			/* no program */
	    }
	    dle->program = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	    dle->estimatelist = g_slist_append(dle->estimatelist,
					       GINT_TO_POINTER(ES_CALCSIZE));
	}
	else {
	    dle->estimatelist = g_slist_append(dle->estimatelist,
					       GINT_TO_POINTER(ES_CLIENT));
	}

	skip_whitespace(s, ch);			/* find disk name */
	if (ch == '\0') {
	    goto err;				/* no disk */
	}
	qdisk = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	dle->disk = unquote_string(qdisk);

	skip_whitespace(s, ch);                 /* find the device or level */
	if (ch == '\0') {
	    goto err;				/* no device or level */
	}
	if(!isdigit((int)s[-1])) {
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	     s[-1] = '\0';			/* terminate the device */
	    qamdevice = g_strdup(fp);
	    dle->device = unquote_string(qamdevice);
	    skip_whitespace(s, ch);		/* find level number */
	}
	else {
	    dle->device = g_strdup(dle->disk);
	    qamdevice = g_strdup(qdisk);
	}
	amfree(qamdevice);

						/* find level number */
	if (ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    goto err;				/* bad level */
	}
	alevel = g_new0(am_level_t, 1);
	alevel->level = level;
	dle->levellist = g_slist_append(dle->levellist, alevel);
	skip_integer(s, ch);

	skip_whitespace(s, ch);
	if (ch && strncmp_const_skip(s - 1, "OPTIONS ", s, ch) == 0) {
	    skip_whitespace(s, ch);		/* find the option string */
	    if(ch == '\0') {
		goto err;			/* bad options string */
	    }
	    optstr = s - 1;
	    skip_quoted_string(s, ch);
	    s[-1] = '\0';			/* terminate the options */
	    parse_options(optstr, dle, g_options->features, 1);
	    /*@ignore@*/

	    check_options(dle);
	    check_disk(dle);

	    /*@end@*/
	} else if (ch == '\0') {
	    /* check all since no option */
	    need_samba=1;
	    need_rundump=1;
	    need_dump=1;
	    need_restore=1;
	    need_vdump=1;
	    need_vrestore=1;
	    need_xfsdump=1;
	    need_xfsrestore=1;
	    need_vxdump=1;
	    need_vxrestore=1;
	    need_runtar=1;
	    need_gnutar=1;
	    need_compress_path=1;
	    need_calcsize=1;
	    need_global_check=1;
	    /*@ignore@*/
	    check_disk(dle);
	    /*@end@*/
	} else {
	    goto err;				/* bad syntax */
	}
	free_dle(dle);
	dle = NULL;
    }
    if (g_options == NULL) {
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600006, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
	exit(1);
	/*NOTREACHED*/
    }

    if (am_has_feature(g_options->features, fe_req_xml)) {
	char  *errmsg = NULL;
	dle_t *dles, *dle, *dle_next;

	dles = amxml_parse_node_FILE(stdin, &errmsg);
	if (errmsg) {
	    err_extra = errmsg;
	    goto err;
	}
	if (merge_dles_properties(dles, 1) == 0) {
	    goto checkoverall;
	}
	for (dle = dles; dle != NULL; dle = dle->next) {
	    run_client_scripts(EXECUTE_ON_PRE_HOST_AMCHECK, g_options, dle,
			       stdout, &selfcheck_fprint_message);
	}
	for (dle = dles; dle != NULL; dle = dle->next) {
	    check_options(dle);
	    run_client_scripts(EXECUTE_ON_PRE_DLE_AMCHECK, g_options, dle,
			       stdout, &selfcheck_fprint_message);
	    check_disk(dle);
	    run_client_scripts(EXECUTE_ON_POST_DLE_AMCHECK, g_options, dle,
			       stdout, &selfcheck_fprint_message);
	}
	for (dle = dles; dle != NULL; dle = dle->next) {
	    run_client_scripts(EXECUTE_ON_POST_HOST_AMCHECK, g_options, dle,
			       stdout, &selfcheck_fprint_message);
	}
	for (dle = dles; dle != NULL; dle = dle_next) {
	    dle_next = dle->next;
	    free_dle(dle);
	}
    }

checkoverall:
    delete_message(selfcheck_print_message(check_security_file_permission_message()));

    check_overall();

    amfree(line);
    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;
    free_g_options(g_options);

    dbclose();
    return 0;

 err:
    if (err_extra) {
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600007, MSG_ERROR, 2,
			"err_extra", err_extra,
			"hostname", g_options->hostname)));
    } else {
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600008, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
    }
    amfree(err_extra);
    amfree(line);
    if (dle)
	free_dle(dle);
    dbclose();
    return 1;
}


static void
check_options(
    dle_t *dle)
{
    if (GPOINTER_TO_INT(dle->estimatelist->data) == ES_CALCSIZE) {
	need_calcsize=1;
    }

    if (g_str_equal(dle->program, "GNUTAR")) {
	need_gnutar=1;
        if (dle->device[0] == '/' && dle->device[1] == '/') {
	    if(dle->exclude_file && dle->exclude_file->nb_element > 1) {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600009, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    }
	    if (dle->exclude_list && dle->exclude_list->nb_element > 0 &&
	        dle->exclude_optional==0) {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600010, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    }
	    if (dle->include_file && dle->include_file->nb_element > 0) {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600011, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    }
	    if (dle->include_list && dle->include_list->nb_element > 0 &&
	        dle->include_optional==0) {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600012, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    }
	    need_samba=1;
	} else {
	    int nb_exclude = 0;
	    int nb_include = 0;
	    char *file_exclude = NULL;
	    char *file_include = NULL;

	    if (dle->exclude_file) nb_exclude += dle->exclude_file->nb_element;
	    if (dle->exclude_list) nb_exclude += dle->exclude_list->nb_element;
	    if (dle->include_file) nb_include += dle->include_file->nb_element;
	    if (dle->include_list) nb_include += dle->include_list->nb_element;

	    if (nb_exclude > 0) file_exclude = build_exclude(dle, 1);
	    if (nb_include > 0) file_include = build_include(dle, 1);

	    amfree(file_exclude);
	    amfree(file_include);

	    need_runtar=1;
	}
    }

    if (g_str_equal(dle->program, "DUMP")) {
	if (dle->exclude_file && dle->exclude_file->nb_element > 0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600013, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	}
	if (dle->exclude_list && dle->exclude_list->nb_element > 0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600014, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	}
	if (dle->include_file && dle->include_file->nb_element > 0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600015, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	}
	if (dle->include_list && dle->include_list->nb_element > 0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600016, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	}
#ifdef USE_RUNDUMP
	need_rundump=1;
#endif
#ifndef AIX_BACKUP
#ifdef VDUMP
#ifdef DUMP
	if (g_str_equal(amname_to_fstype(dle->device), "advfs"))
#else
	if (1)
#endif
	{
	    need_vdump=1;
	    need_rundump=1;
	    if (dle->create_index)
		need_vrestore=1;
	}
	else
#endif /* VDUMP */
#ifdef XFSDUMP
#ifdef DUMP
	if (g_str_equal(amname_to_fstype(dle->device), "xfs"))
#else
	if (1)
#endif
	{
	    need_xfsdump=1;
	    need_rundump=1;
	    if (dle->create_index)
		need_xfsrestore=1;
	}
	else
#endif /* XFSDUMP */
#ifdef VXDUMP
#ifdef DUMP
	if (g_str_equal(amname_to_fstype(dle->device), "vxfs"))
#else
	if (1)
#endif
	{
	    need_vxdump=1;
	    if (dle->create_index)
		need_vxrestore=1;
	}
	else
#endif /* VXDUMP */
	{
	    need_dump=1;
	    if (dle->create_index)
		need_restore=1;
	}
#else
	/* AIX backup program */
	need_dump=1;
	if (dle->create_index)
	    need_restore=1;
#endif
    }
    if ((dle->compress == COMP_BEST) || (dle->compress == COMP_FAST)
		|| (dle->compress == COMP_CUST)) {
	need_compress_path=1;
    }
    if (dle->auth && amandad_auth) {
	if (strcasecmp(dle->auth, amandad_auth) != 0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600017, MSG_ERROR, 5,
			"auth", amandad_auth,
			"auth-requested", dle->auth,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    if (g_str_equal(dle->auth, "ssh"))  {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600018, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    }
	    else {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600019, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    }
	}
    }
}

static void selfcheck_print_array_message(gpointer data, gpointer user_data);
static void
selfcheck_print_array_message(
    gpointer data,
    gpointer user_data G_GNUC_UNUSED)
{
    message_t *message = data;

    delete_message(selfcheck_print_message(message));
}

static void
check_disk(
    dle_t *dle)
{
    char *device = NULL;
#ifdef SAMBA_CLIENT
    char *user_and_password = NULL;
    char *domain = NULL;
    char *share = NULL, *subdir = NULL;
    size_t lpass = 0;
    char *extra_info = NULL;
#endif
    int amode = R_OK;
    int access_result;
    char *access_type;
    char *qdisk = NULL;
    char *qamdevice = NULL;
    char *qdevice = NULL;
    int nb_error = 0;
    char **env;

    if (dle->disk) {
	need_global_check=1;
	qdisk = quote_string(dle->disk);
	qamdevice = quote_string(dle->device);
	device = g_strdup("nodevice");
	dbprintf(_("checking disk %s\n"), qdisk);
	if (GPOINTER_TO_INT(dle->estimatelist->data) == ES_CALCSIZE) {
	    if (dle->device[0] == '/' && dle->device[1] == '/') {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600020, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		nb_error++;
		goto common_exit;
	    }
	}

	if (g_str_equal(dle->program, "GNUTAR")) {
            if(dle->device[0] == '/' && dle->device[1] == '/') {
#ifdef SAMBA_CLIENT
		int nullfd, checkerr;
		int passwdfd;
		char *pwtext;
		size_t pwtext_len;
		pid_t checkpid;
		amwait_t retstat;
		int rc;
		char *line;
		char *sep;
		FILE *ferr;
		char *pw_fd_env;
		int errdos;
		char *err = NULL;

		parsesharename(dle->device, &share, &subdir);
		if (!share) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600021, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		}
		if ((subdir) && (SAMBA_VERSION < 2)) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600022, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		}
		if ((user_and_password = findpass(share, &domain)) == NULL) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600023, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		}
		lpass = strlen(user_and_password);
		if ((pwtext = strchr(user_and_password, '%')) == NULL) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600024, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		}
		*pwtext++ = '\0';
		pwtext_len = (size_t)strlen(pwtext);
		amfree(device);
		if ((device = makesharename(share, 0)) == NULL) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600025, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		}

		if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600026, MSG_ERROR, 4,
			"errno", errno,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		}

		if (pwtext_len > 0) {
		    pw_fd_env = "PASSWD_FD";
		} else {
		    pw_fd_env = "dummy_PASSWD_FD";
		}
		checkpid = pipespawn(SAMBA_CLIENT, STDERR_PIPE|PASSWD_PIPE, 0,
				     &nullfd, &nullfd, &checkerr,
				     pw_fd_env, &passwdfd,
				     "smbclient",
				     device,
				     *user_and_password ? "-U" : skip_argument,
				     *user_and_password ? user_and_password
							: skip_argument,
				     "-E",
				     domain ? "-W" : skip_argument,
				     domain ? domain : skip_argument,
#if SAMBA_VERSION >= 2
				     subdir ? "-D" : skip_argument,
				     subdir ? subdir : skip_argument,
#endif
				     "-c", "quit",
				     NULL);
		amfree(domain);
		aclose(nullfd);
		/*@ignore@*/
		if ((pwtext_len > 0) &&
		    full_write(passwdfd, pwtext, pwtext_len) < pwtext_len) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600027, MSG_ERROR, 4,
			"errno", errno,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    aclose(passwdfd);
		    goto common_exit;
		}
		/*@end@*/
		memset(user_and_password, '\0', (size_t)lpass);
		amfree(user_and_password);
		aclose(passwdfd);
		ferr = fdopen(checkerr, "r");
		if (!ferr) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600028, MSG_ERROR, 4,
			"errno", errno,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    exit(1);
		    /*NOTREACHED*/
		}
		sep = "";
		errdos = 0;
		for(sep = ""; (line = agets(ferr)) != NULL; free(line)) {
		    if (line[0] == '\0')
			continue;
		    strappend(extra_info, sep);
		    strappend(extra_info, line);
		    sep = ": ";
		    if(strstr(line, "ERRDOS") != NULL) {
			errdos = 1;
		    }
		}
		afclose(ferr);
		checkerr = -1;
		rc = 0;
		sep = "";
		waitpid(checkpid, &retstat, 0);
		if (!WIFEXITED(retstat) || WEXITSTATUS(retstat) != 0) {
		    char *exitstr = str_exit_status("smbclient", retstat);
		    strappend(err, sep);
		    strappend(err, exitstr);
		    sep = "\n";
		    amfree(exitstr);

		    rc = 1;
		}
		if (errdos != 0 || rc != 0) {
		    char *errmsg;
		    if (extra_info) {
			errmsg = g_strdup_printf("%s %s", extra_info, err);
		    } else {
			errmsg = g_strdup(err);
		    }
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600087, MSG_ERROR, 4,
			"errmsg", errmsg,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));

		    g_free(errmsg);
		    g_free(err);
		}
#else
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600030, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		nb_error++;
#endif
		goto common_exit;
	    }
	    amode = F_OK;
	    amfree(device);
	    device = amname_to_dirname(dle->device);
	} else if (g_str_equal(dle->program, "DUMP")) {
	    if(dle->device[0] == '/' && dle->device[1] == '/') {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600031, MSG_ERROR, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		nb_error++;
		goto common_exit;
	    }
#ifdef VDUMP								/* { */
#ifdef DUMP								/* { */
            if (g_str_equal(amname_to_fstype(dle->device), "advfs"))
#else									/* }{*/
	    if (1)
#endif									/* } */
	    {
		amfree(device);
		device = amname_to_dirname(dle->device);
		amode = F_OK;
	    } else
#endif									/* } */
	    {
		amfree(device);
		device = amname_to_devname(dle->device);
#ifdef USE_RUNDUMP
		amode = F_OK;
#else
		amode = R_OK;
#endif
	    }
	}
    }
    if (dle->program_is_application_api) {
	pid_t                    application_api_pid;
	backup_support_option_t *bsu;
	int                      app_out[2];
	int                      app_err[2];
	GPtrArray               *errarray;

	bsu = backup_support_option(dle->program, &errarray);

	if (!bsu) {
	    char  *line;
	    guint  i;
	    for (i=0; i < errarray->len; i++) {
		line = g_ptr_array_index(errarray, i);
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600032, MSG_ERROR, 5,
			"application", dle->program,
			"errstr", line,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		nb_error++;
	    }
	    g_ptr_array_free_full(errarray);
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600033, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	    goto common_exit;
	}

	if (dle->data_path == DATA_PATH_AMANDA &&
	    (bsu->data_path_set & DATA_PATH_AMANDA)==0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600034, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (dle->data_path == DATA_PATH_DIRECTTCP &&
	    (bsu->data_path_set & DATA_PATH_DIRECTTCP)==0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600034, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (GPOINTER_TO_INT(dle->estimatelist->data) == ES_CALCSIZE &&
			    !bsu->calcsize) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600036, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (dle->include_file && dle->include_file->nb_element > 0 &&
	    !bsu->include_file) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600037, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (dle->include_list && dle->include_list->nb_element > 0 &&
	    !bsu->include_list) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600038, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (dle->include_optional && !bsu->include_optional) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600039, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (dle->exclude_file && dle->exclude_file->nb_element > 0 &&
	    !bsu->exclude_file) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600040, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (dle->exclude_list && dle->exclude_list->nb_element > 0 &&
	    !bsu->exclude_list) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600041, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	if (dle->exclude_optional && !bsu->exclude_optional) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600042, MSG_ERROR, 4,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	}
	fflush(stdout);fflush(stderr);

	if (pipe(app_out) < 0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600043, MSG_ERROR, 5,
			"errno", errno,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	    goto common_exit;
	}

	if (pipe(app_err) < 0) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600043, MSG_ERROR, 5,
			"errno", errno,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	    goto common_exit;
	}

	switch (application_api_pid = fork()) {
	case -1:
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600044, MSG_ERROR, 5,
			"errno", errno,
			"application", dle->program,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	    nb_error++;
	    goto common_exit;

	case 0: /* child */
	    {
		GPtrArray *argv_ptr = g_ptr_array_new();
                GPtrArray *argv_quoted = g_ptr_array_new();
                gchar **args, **quoted_strings, **ptr;
		char *cmd = g_strjoin(NULL, APPLICATION_DIR, "/", dle->program, NULL);
		GSList   *scriptlist;
		script_t *script;
		estimatelist_t el;
		char *cmdline;

		aclose(app_err[0]);
		dup2(app_err[1], 2);
		aclose(app_out[0]);
		dup2(app_out[1], 1);

		g_ptr_array_add(argv_ptr, g_strdup(dle->program));
		g_ptr_array_add(argv_ptr, g_strdup("selfcheck"));
		if (bsu->message_selfcheck_json == 1) {
		    g_ptr_array_add(argv_ptr, g_strdup("--message"));
		    g_ptr_array_add(argv_ptr, g_strdup("json"));
		} else if (bsu->message_line == 1) {
		    g_ptr_array_add(argv_ptr, g_strdup("--message"));
		    g_ptr_array_add(argv_ptr, g_strdup("line"));
		}
		if (g_options->config != NULL && bsu->config == 1) {
		    g_ptr_array_add(argv_ptr, g_strdup("--config"));
		    g_ptr_array_add(argv_ptr, g_strdup(g_options->config));
		}
		if (g_options->hostname != NULL && bsu->host == 1) {
		    g_ptr_array_add(argv_ptr, g_strdup("--host"));
		    g_ptr_array_add(argv_ptr, g_strdup(g_options->hostname));
		}
		if (dle->disk != NULL && bsu->disk == 1) {
		    g_ptr_array_add(argv_ptr, g_strdup("--disk"));
		    g_ptr_array_add(argv_ptr, g_strdup(dle->disk));
		}
		if (dle->device) {
		    g_ptr_array_add(argv_ptr, g_strdup("--device"));
		    g_ptr_array_add(argv_ptr, g_strdup(dle->device));
		}
		if (dle->create_index && bsu->index_line == 1) {
		    g_ptr_array_add(argv_ptr, g_strdup("--index"));
		    g_ptr_array_add(argv_ptr, g_strdup("line"));
		}
		if (dle->record && bsu->record == 1) {
		    g_ptr_array_add(argv_ptr, g_strdup("--record"));
		}

		for (el = dle->estimatelist; el != NULL; el=el->next) {
		    estimate_t estimate = (estimate_t)GPOINTER_TO_INT(el->data);
		    if (estimate == ES_CALCSIZE && bsu->calcsize == 1) {
			g_ptr_array_add(argv_ptr, g_strdup("--calcsize"));
		    }
		}
		application_property_add_to_argv(argv_ptr, dle, bsu,
						 g_options->features);

		for (scriptlist = dle->scriptlist; scriptlist != NULL;
		     scriptlist = scriptlist->next) {
		    script = (script_t *)scriptlist->data;
		    if (script->result && script->result->proplist) {
			property_add_to_argv(argv_ptr,
					     script->result->proplist);
		    }
		}

		g_ptr_array_add(argv_ptr, NULL);
                args = (gchar **)g_ptr_array_free(argv_ptr, FALSE);

                /*
                 * Build the command line to display
                 */
                g_ptr_array_add(argv_quoted, g_strdup(cmd));

                for (ptr = args; *ptr; ptr++)
                    g_ptr_array_add(argv_quoted, quote_string(*ptr));

                g_ptr_array_add(argv_quoted, NULL);

                quoted_strings = (gchar **)g_ptr_array_free(argv_quoted, FALSE);

                cmdline = g_strjoinv(" ", quoted_strings);
                g_strfreev(quoted_strings);

		dbprintf(_("Spawning \"%s\" in pipeline\n"), cmdline);
		amfree(cmdline);

		safe_fd(-1, 0);
		env = safe_env();
		execve(cmd, args, env);
		free_env(env);
		g_printf(_("ERROR [Can't execute %s: %s]\n"), cmd, strerror(errno));
		/* if the application support message
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600045, MSG_ERROR, 5,
			"errno", errno,
			"cmd", cmd.
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		*/
		exit(127);
	    }
	default: /* parent */
	    {
		int   status;
		FILE *app_stdout;
		FILE *app_stderr;
		char *line;
		GString *json_message = NULL;

		aclose(app_out[1]);
		aclose(app_err[1]);
		app_stdout = fdopen(app_out[0], "r");
		app_stderr = fdopen(app_err[0], "r");
		if (!app_stdout) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600055, MSG_ERROR, 4,
			"errno", errno,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    exit(1);
		    /*NOTREACHED*/
		}
		if (!app_stderr) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600046, MSG_ERROR, 4,
			"errno", errno,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
		    nb_error++;
		    exit(1);
		    /*NOTREACHED*/
		}
		while((line = agets(app_stdout)) != NULL) {
		    if (strlen(line) > 0) {
			if (strncmp(line, "MESSAGE JSON", 12) == 0) {
			    g_free(line);
			    json_message = g_string_sized_new(1024);
			    while((line = agets(app_stdout)) != NULL) {
				g_string_append(json_message, line);
				g_free(line);
			    }
			    break;
			} else if (strncmp(line, "OK ", 3) == 0) {
			    delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600056, MSG_INFO, 5,
				"application", dle->program,
				"ok_line", line+3,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
			} else if (strncmp(line, "ERROR ", 6) == 0) {
			    delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600057, MSG_ERROR, 5,
				"application", dle->program,
				"error_line", line+6,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
			} else {
			    delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600058, MSG_ERROR, 5,
				"application", dle->program,
				"errstr", line,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
			}
		    }
		    amfree(line);
		}
		while((line = agets(app_stderr)) != NULL) {
		    if (strlen(line) > 0) {
			delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600047, MSG_ERROR, 5,
				"application", dle->program,
				"errstr", line,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
			nb_error++;
		    }
		    amfree(line);
		}
		fclose(app_stdout);
		fclose(app_stderr);
		if (json_message) {
		    /* parse json_message into a message_t array */
		    GPtrArray *message_array = parse_json_message(json_message->str);
		    g_debug("json_message: %s", json_message->str);

		    /* print and delete the messages */
		    g_ptr_array_foreach(message_array, selfcheck_print_array_message, NULL);

		    g_ptr_array_free(message_array, TRUE);
		    g_string_free(json_message, TRUE);
		}

		if (waitpid(application_api_pid, &status, 0) < 0) {
		    delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600048, MSG_ERROR, 4,
				"errno", errno,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		} else if (!WIFEXITED(status)) {
		    delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600049, MSG_ERROR, 5,
				"signal", g_strdup_printf("%d", WTERMSIG(status)),
				"application", dle->program,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		} else if (WEXITSTATUS(status) != 0) {
		    delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600050, MSG_ERROR, 5,
				"exit_status", g_strdup_printf("%d", WEXITSTATUS(status)),
				"application", dle->program,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
		    nb_error++;
		    goto common_exit;
		}
	    }
	}
	amfree(bsu);
	fflush(stdout);fflush(stderr);
	amfree(device);
	amfree(qamdevice);
	amfree(qdisk);
	return;
    }

    if (device) {
	qdevice = quote_string(device);
	dbprintf(_("device %s\n"), qdevice);

	/* skip accessability test if this is an AFS entry */
	if(strncmp_const(device, "afs:") != 0) {
#ifdef CHECK_FOR_ACCESS_WITH_OPEN
	    access_result = open(device, O_RDONLY);
	    access_type = "open";
#else
	    access_result = access(device, amode);
	    access_type = "access";
#endif
	    if(access_result == -1) {
		delete_message(selfcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 3600051, MSG_ERROR, 5,
				"errno", errno,
				"type", access_type,
				"hostname", g_options->hostname,
				"device", dle->device,
				"disk", dle->disk )));
		nb_error++;
	    }
#ifdef CHECK_FOR_ACCESS_WITH_OPEN
	    aclose(access_result);
#endif
	}
    }

common_exit:

    if (!qdevice)
	qdevice = quote_string(device);

#ifdef SAMBA_CLIENT
    amfree(share);
    amfree(subdir);
    if(user_and_password) {
	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
    }
    amfree(domain);
#endif

    if (nb_error == 0) {
	if (dle->disk) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600052, MSG_INFO, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	}
	if (dle->device) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600053, MSG_INFO, 4,
			"amdevice", dle->device,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	}
	if (device) {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600054, MSG_INFO, 3,
			"hostname", g_options->hostname,
			"device", dle->device,
			"disk", dle->disk )));
	}
    }
#ifdef SAMBA_CLIENT
    if(extra_info) {
	dbprintf(_("extra info: %s\n"), extra_info);
	amfree(extra_info);
    }
#endif
    amfree(qdisk);
    amfree(qdevice);
    amfree(qamdevice);
    amfree(device);

    /* XXX perhaps do something with level: read dumpdates and sanity check */
}

static void
check_overall(void)
{
    char *cmd;
    struct stat buf;
    int testfd;
    char *gnutar_list_dir;
    int   need_amandates = 0;

    if( need_runtar )
    {
#ifdef GNUTAR
	char *my_pname = g_strdup(get_pname());
	char *gnutar_realpath = NULL;
#endif
	cmd = g_strjoin(NULL, amlibexecdir, "/", "runtar", NULL);
	delete_message(selfcheck_print_message(check_file_message(cmd,X_OK)));
	delete_message(selfcheck_print_message(check_suid_message(cmd)));
	amfree(cmd);

#ifdef GNUTAR
	set_pname("runtar");
	delete_message(selfcheck_print_message(check_exec_for_suid_message("GNUTAR_PATH", GNUTAR, &gnutar_realpath)));
	set_pname(my_pname);
	amfree(gnutar_realpath);
#endif
    }

    if( need_rundump )
    {
	cmd = g_strjoin(NULL, amlibexecdir, "/", "rundump", NULL);
	delete_message(selfcheck_print_message(check_file_message(cmd,X_OK)));
	delete_message(selfcheck_print_message(check_suid_message(cmd)));
	amfree(cmd);
    }

    if( need_dump ) {
#ifdef DUMP
	delete_message(selfcheck_print_message(check_file_message(DUMP, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600072, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if( need_restore ) {
#ifdef RESTORE
	delete_message(selfcheck_print_message(check_file_message(RESTORE, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600073, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if ( need_vdump ) {
#ifdef VDUMP
	delete_message(selfcheck_print_message(check_file_message(VDUMP, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600074, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if ( need_vrestore ) {
#ifdef VRESTORE
	delete_message(selfcheck_print_message(check_file_message(VRESTORE, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600075, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if( need_xfsdump ) {
#ifdef XFSDUMP
	delete_message(selfcheck_print_message(check_file_message(XFSDUMP, F_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600076, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if( need_xfsrestore ) {
#ifdef XFSRESTORE
	delete_message(selfcheck_print_message(check_file_message(XFSRESTORE, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600077, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if( need_vxdump ) {
#ifdef VXDUMP
	delete_message(selfcheck_print_message(check_file_message(VXDUMP, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600078, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if( need_vxrestore ) {
#ifdef VXRESTORE
	delete_message(selfcheck_print_message(check_file_message(VXRESTORE, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600079, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
    }

    if( need_gnutar ) {
#ifdef GNUTAR
	delete_message(selfcheck_print_message(check_file_message(GNUTAR, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600080, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
	gnutar_list_dir = getconf_str(CNF_GNUTAR_LIST_DIR);
	if (strlen(gnutar_list_dir) == 0)
	    gnutar_list_dir = NULL;
	if (gnutar_list_dir) {
	    /* make sure our listed-incremental dir is ready */
	    delete_message(selfcheck_print_message(check_dir_message(gnutar_list_dir, R_OK|W_OK)));
	} else {
	    /* no listed-incremental dir, so check that amandates is ready */
	    need_amandates = 1;
	}
    }

    if( need_calcsize ) {
	char *cmd;

	cmd = g_strjoin(NULL, amlibexecdir, "/", "calcsize", NULL);

	delete_message(selfcheck_print_message(check_file_message(cmd, X_OK)));

	amfree(cmd);

	/* calcsize uses amandates */
	need_amandates = 1;
    }

    if (need_amandates) {
	char *amandates_file;
	amandates_file = getconf_str(CNF_AMANDATES);
	delete_message(selfcheck_print_message(check_file_message(amandates_file, R_OK|W_OK)));
    }

    if( need_samba ) {
#ifdef SAMBA_CLIENT
	delete_message(selfcheck_print_message(check_file_message(SAMBA_CLIENT, X_OK)));
#else
	delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600081, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
#endif
	testfd = open("/etc/amandapass", R_OK);
	if (testfd >= 0) {
	    if(fstat(testfd, &buf) == 0) {
		if ((buf.st_mode & 0x7) != 0) {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600082, MSG_ERROR, 1,
			"hostname", g_options->hostname)));
		} else {
		    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600083, MSG_INFO, 1,
			"hostname", g_options->hostname)));
		}
	    } else {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600084, MSG_ERROR, 2,
			"errno", errno,
			"hostname", g_options->hostname)));
	    }
	    aclose(testfd);
	} else {
	    delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600085, MSG_ERROR, 2,
			"errno", errno,
			"hostname", g_options->hostname)));
	}
    }

    if (need_compress_path )
	delete_message(selfcheck_print_message(check_file_message(COMPRESS_PATH, X_OK)));

    if (need_dump || need_xfsdump ) {
	if (check_file_exist("/etc/dumpdates")) {
	    delete_message(selfcheck_print_message(check_file_message("/etc/dumpdates",
#ifdef USE_RUNDUMP
		       F_OK
#else
		       R_OK|W_OK
#endif
		      )));
	} else {
#ifndef USE_RUNDUMP
	    if (access("/etc", R_OK|W_OK) == -1) {
		delete_message(selfcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 3600086, MSG_ERROR, 2,
			"errno", errno,
			"hostname", g_options->hostname)));
	    }
#endif
	}
    }

    if (need_vdump) {
	if (check_file_exist("/etc/vdumpdates")) {
            delete_message(selfcheck_print_message(check_file_message("/etc/vdumpdates", F_OK)));
	}
    }

    if (need_global_check) {
    delete_message(selfcheck_print_message(check_access_message("/dev/null", R_OK|W_OK)));
    check_space(AMANDA_TMPDIR, (off_t)64);	/* for amandad i/o */

#ifdef AMANDA_DBGDIR
    check_space(AMANDA_DBGDIR, (off_t)64);	/* for amandad i/o */
#endif

    check_space("/etc", (off_t)64);		/* for /etc/dumpdates writing */
    }
}

static void
check_space(
    char *	dir,
    off_t	kbytes)
{
    struct fs_usage fsusage;
    intmax_t kb_avail;

    if (get_fs_usage(dir, NULL, &fsusage) == -1) {
        delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600068, MSG_ERROR, 3,
		"errno", errno,
		"dirname", dir,
		"hostname", g_options->hostname)));
	return;
    }

    /* do the division first to avoid potential integer overflow */
    kb_avail = fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;

    if (fsusage.fsu_bavail_top_bit_set || fsusage.fsu_bavail == 0) {
        delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600069, MSG_ERROR, 3,
		"required", g_strdup_printf("%jd", (intmax_t)kbytes),
		"dirname", dir,
		"hostname", g_options->hostname)));
    } else if (kb_avail < kbytes) {
        delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600070, MSG_ERROR, 4,
		"required", g_strdup_printf("%jd", (intmax_t)kbytes),
		"avail", g_strdup_printf("%jd", kb_avail),
		"dirname", dir,
		"hostname", g_options->hostname)));
    } else {
        delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600071, MSG_INFO, 3,
		"avail", g_strdup_printf("%jd", kb_avail),
		"dirname", dir,
		"hostname", g_options->hostname)));
    }
}

static int
check_file_exist(
    char *filename)
{
    struct stat stat_buf;

    if (stat(filename, &stat_buf) != 0) {
	if(errno == ENOENT) {
	    return 0;
	}
    }
    return 1;
}

static void
print_platform(void)
{
    char *uname = NULL;
    char *distro = NULL;
    char *platform = NULL;
    char *productName = NULL;
    char *productVersion = NULL;
    char  line[1025];
    GPtrArray *argv_ptr;
    FILE *release;
    struct stat stat_buf;

    if (!stat("/usr/bin/lsb_release", &stat_buf)) {
	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/lsb_release");
	g_ptr_array_add(argv_ptr, "--id");
	g_ptr_array_add(argv_ptr, "-s");
	g_ptr_array_add(argv_ptr, NULL);
	distro = get_first_line(argv_ptr);
	if (distro && distro[0] == '"') {
	    char *p= g_strdup(distro+1);
	    p[strlen(p)-1] = '\0';
	    g_free(distro);
	    distro = p;
	}
	g_ptr_array_free(argv_ptr, TRUE);

	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/lsb_release");
	g_ptr_array_add(argv_ptr, "--description");
	g_ptr_array_add(argv_ptr, "-s");
	g_ptr_array_add(argv_ptr, NULL);
	platform = get_first_line(argv_ptr);
	if (platform && platform[0] == '"') {
	    char *p= g_strdup(platform+1);
	    p[strlen(p)-1] = '\0';
	    g_free(platform);
	    platform = p;
	}
	g_ptr_array_free(argv_ptr, TRUE);
	goto print_platform_out;
    }
    release = fopen("/etc/redhat-release", "r");
    if (release) {
	char *result;
	distro = g_strdup("RPM");
	result = fgets(line, 1024, release);
	if (result) {
	    platform = g_strdup(line);
	}
	fclose(release);
	goto print_platform_out;
    }

    release = fopen("/etc/lsb-release", "r");
    if (release) {
	distro = g_strdup("Ubuntu");
	while (fgets(line, 1024, release)) {
	    if (strstr(line, "DISTRIB_ID")) {
		char *p = strchr(line, '=');
		if (p) {
		    g_free(distro);
		    distro = g_strdup(p+1);
		}
	    }
	    if (strstr(line, "DESCRIPTION")) {
		char *p = strchr(line, '=');
		if (p) {
		    g_free(platform);
		    platform = g_strdup(p+1);
		}
	    }
	}
	fclose(release);
	goto print_platform_out;
    }

    release = fopen("/etc/debian_version", "r");
    if (release) {
	char *result;
	distro = g_strdup("Debian");
	result = fgets(line, 1024, release);
	if (result) {
	    platform = g_strdup(line);
	}
	fclose(release);
	goto print_platform_out;
    }

    argv_ptr = g_ptr_array_new();
    g_ptr_array_add(argv_ptr, UNAME_PATH);
    g_ptr_array_add(argv_ptr, "-s");
    g_ptr_array_add(argv_ptr, NULL);
    uname = get_first_line(argv_ptr);
    g_ptr_array_free(argv_ptr, TRUE);
    if (uname) {
	if (strncmp(uname, "SunOS", 5) == 0) {
	    FILE *release = fopen("/etc/release", "r");
	    distro = g_strdup("Solaris");
	    g_free(uname);
	    if (release) {
		char *result;
		result = fgets(line, 1024, release);
		if (result) {
		   platform = g_strdup(line);
		}
		fclose(release);
		goto print_platform_out;
	    }
	} else if (strlen(uname) >= 3 &&
		   g_strcasecmp(uname+strlen(uname)-3, "bsd") == 0) {
	    distro = uname;
	    argv_ptr = g_ptr_array_new();
	    g_ptr_array_add(argv_ptr, UNAME_PATH);
	    g_ptr_array_add(argv_ptr, "-r");
	    g_ptr_array_add(argv_ptr, NULL);
	    platform = get_first_line(argv_ptr);
	    g_ptr_array_free(argv_ptr, TRUE);
	} else {
	    g_free(uname);
	}
    }
    if (!stat("/usr/bin/sw_vers", &stat_buf)) {
	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/sw_vers");
	g_ptr_array_add(argv_ptr, "-productName");
	g_ptr_array_add(argv_ptr, NULL);
	productName = get_first_line(argv_ptr);
	g_ptr_array_free(argv_ptr, TRUE);
	argv_ptr = g_ptr_array_new();
	g_ptr_array_add(argv_ptr, "/usr/bin/sw_vers");
	g_ptr_array_add(argv_ptr, "-productVersion");
	g_ptr_array_add(argv_ptr, NULL);
	productVersion = get_first_line(argv_ptr);
	g_ptr_array_free(argv_ptr, TRUE);
	if (productName && productVersion &&
	    !g_str_equal(productName, "unknown") &&
	    !g_str_equal( productVersion, "unknown")) {
	    distro = g_strdup("mac");
	    platform = g_strdup_printf("%s %s", productVersion, productVersion);
	    goto print_platform_out;
	}
    }

print_platform_out:
    if (!distro) {
	distro = g_strdup("Unknown");
    }
    if (!platform) {
	platform = g_strdup("Unknown");
    }
    if (platform[strlen(platform) -1] == '\n') {
	platform[strlen(platform) -1] = '\0';
    }
    delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600001, MSG_INFO, 2,
		"distro", distro,
		"hostname", g_options->hostname)));
    delete_message(selfcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 3600002, MSG_INFO, 2,
		"platform", platform,
		"hostname", g_options->hostname)));

    amfree(distro);
    amfree(platform);
    amfree(productName);
    amfree(productVersion);
}
