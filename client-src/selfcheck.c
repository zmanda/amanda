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
#include "util.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "client_util.h"
#include "conffile.h"
#include "amandad.h"
#include "amxml.h"
#include "base64.h"

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
    dle_t *dle;
    int level;
    GSList *errlist;
    am_level_t *alevel;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
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
    dbprintf(_("version %s\n"), VERSION);
    g_printf("OK version %s\n", VERSION);
    print_platform();

    if(argc > 2 && strcmp(argv[1], "amandad") == 0) {
	amandad_auth = stralloc(argv[2]);
    }

    config_init(CONFIG_INIT_CLIENT, NULL);
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

	skip_whitespace(s, ch);			/* find program name */
	if (ch == '\0') {
	    goto err;				/* no program */
	}
	dle->program = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';				/* terminate the program name */

	dle->program_is_application_api = 0;
	if(strcmp(dle->program,"APPLICATION")==0) {
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
	    qamdevice = stralloc(fp);
	    dle->device = unquote_string(qamdevice);
	    skip_whitespace(s, ch);		/* find level number */
	}
	else {
	    dle->device = stralloc(dle->disk);
	    qamdevice = stralloc(qdisk);
	}

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
	amfree(qamdevice);
    }
    if (g_options == NULL) {
	g_printf(_("ERROR [Missing OPTIONS line in selfcheck input]\n"));
	error(_("Missing OPTIONS line in selfcheck input\n"));
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
			       stdout);
	}
	for (dle = dles; dle != NULL; dle = dle->next) {
	    check_options(dle);
	    run_client_scripts(EXECUTE_ON_PRE_DLE_AMCHECK, g_options, dle,
			       stdout);
	    check_disk(dle);
	    run_client_scripts(EXECUTE_ON_POST_DLE_AMCHECK, g_options, dle,
			       stdout);
	}
	for (dle = dles; dle != NULL; dle = dle->next) {
	    run_client_scripts(EXECUTE_ON_POST_HOST_AMCHECK, g_options, dle,
			       stdout);
	}
	for (dle = dles; dle != NULL; dle = dle_next) {
	    dle_next = dle->next;
	    free_dle(dle);
	}
    }

checkoverall:
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
	g_printf(_("ERROR [FORMAT ERROR IN REQUEST PACKET %s]\n"), err_extra);
	dbprintf(_("REQ packet is bogus: %s\n"), err_extra);
    } else {
	g_printf(_("ERROR [FORMAT ERROR IN REQUEST PACKET]\n"));
	dbprintf(_("REQ packet is bogus\n"));
    }
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

    if (strcmp(dle->program,"GNUTAR") == 0) {
	need_gnutar=1;
        if(dle->device && dle->device[0] == '/' && dle->device[1] == '/') {
	    if(dle->exclude_file && dle->exclude_file->nb_element > 1) {
		g_printf(_("ERROR [samba support only one exclude file]\n"));
	    }
	    if (dle->exclude_list && dle->exclude_list->nb_element > 0 &&
	        dle->exclude_optional==0) {
		g_printf(_("ERROR [samba does not support exclude list]\n"));
	    }
	    if (dle->include_file && dle->include_file->nb_element > 0) {
		g_printf(_("ERROR [samba does not support include file]\n"));
	    }
	    if (dle->include_list && dle->include_list->nb_element > 0 &&
	        dle->include_optional==0) {
		g_printf(_("ERROR [samba does not support include list]\n"));
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

    if (strcmp(dle->program,"DUMP") == 0) {
	if (dle->exclude_file && dle->exclude_file->nb_element > 0) {
	    g_printf(_("ERROR [DUMP does not support exclude file]\n"));
	}
	if (dle->exclude_list && dle->exclude_list->nb_element > 0) {
	    g_printf(_("ERROR [DUMP does not support exclude list]\n"));
	}
	if (dle->include_file && dle->include_file->nb_element > 0) {
	    g_printf(_("ERROR [DUMP does not support include file]\n"));
	}
	if (dle->include_list && dle->include_list->nb_element > 0) {
	    g_printf(_("ERROR [DUMP does not support include list]\n"));
	}
#ifdef USE_RUNDUMP
	need_rundump=1;
#endif
#ifndef AIX_BACKUP
#ifdef VDUMP
#ifdef DUMP
	if (dle->device && strcmp(amname_to_fstype(dle->device), "advfs") == 0)
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
	if (dle->device && strcmp(amname_to_fstype(dle->device), "xfs") == 0)
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
	if (dle->device && strcmp(amname_to_fstype(dle->device), "vxfs") == 0)
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
	    g_fprintf(stdout,_("ERROR [client configured for auth=%s while server requested '%s']\n"),
		    amandad_auth, dle->auth);
	    if (strcmp(dle->auth, "ssh") == 0)  {	
		g_fprintf(stderr, _("ERROR [The auth in ~/.ssh/authorized_keys "
				  "should be \"--auth=ssh\", or use another auth "
				  " for the DLE]\n"));
	    }
	    else {
		g_fprintf(stderr, _("ERROR [The auth in the inetd/xinetd configuration "
				  " must be the same as the DLE]\n"));
	    }		
	}
    }
}

static void
check_disk(
    dle_t *dle)
{
    char *device = NULL;
    char *err = NULL;
    char *user_and_password = NULL;
    char *domain = NULL;
    char *share = NULL, *subdir = NULL;
    size_t lpass = 0;
    int amode = R_OK;
    int access_result;
    char *access_type;
    char *extra_info = NULL;
    char *qdisk = NULL;
    char *qamdevice = NULL;
    char *qdevice = NULL;

    if (dle->disk) {
	need_global_check=1;
	qdisk = quote_string(dle->disk);
	qamdevice = quote_string(dle->device);
	device = stralloc("nodevice");
	dbprintf(_("checking disk %s\n"), qdisk);
	if (GPOINTER_TO_INT(dle->estimatelist->data) == ES_CALCSIZE) {
	    if (dle->device[0] == '/' && dle->device[1] == '/') {
		err = vstrallocf(
		    _("Can't use CALCSIZE for samba estimate, use CLIENT: %s"),
		    dle->device);
		goto common_exit;
	    }
	}

	if (strcmp(dle->program, "GNUTAR")==0) {
            if(dle->device[0] == '/' && dle->device[1] == '/') {
		#ifdef SAMBA_CLIENT
		int nullfd, checkerr;
		int passwdfd;
		char *pwtext;
		size_t pwtext_len;
		pid_t checkpid;
		amwait_t retstat;
		pid_t wpid;
		int rc;
		char *line;
		char *sep;
		FILE *ferr;
		char *pw_fd_env;
		int errdos;

		parsesharename(dle->device, &share, &subdir);
		if (!share) {
		    err = vstrallocf(
			      _("cannot parse for share/subdir disk entry %s"),
			      dle->device);
		    goto common_exit;
		}
		if ((subdir) && (SAMBA_VERSION < 2)) {
		    err = vstrallocf(_("subdirectory specified for share '%s' but, samba is not v2 or better"),
				     dle->device);
		    goto common_exit;
		}
		if ((user_and_password = findpass(share, &domain)) == NULL) {
		    err = vstrallocf(_("cannot find password for %s"),
				     dle->device);
		    goto common_exit;
		}
		lpass = strlen(user_and_password);
		if ((pwtext = strchr(user_and_password, '%')) == NULL) {
		    err = vstrallocf(
				_("password field not \'user%%pass\' for %s"),
				dle->device);
		    goto common_exit;
		}
		*pwtext++ = '\0';
		pwtext_len = (size_t)strlen(pwtext);
		amfree(device);
		if ((device = makesharename(share, 0)) == NULL) {
		    err = vstrallocf(_("cannot make share name of %s"), share);
		    goto common_exit;
		}

		if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	            err = vstrallocf(_("Cannot access /dev/null : %s"),
				     strerror(errno));
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
		checkpid = checkpid;
		amfree(domain);
		aclose(nullfd);
		/*@ignore@*/
		if ((pwtext_len > 0) &&
		    full_write(passwdfd, pwtext, pwtext_len) < pwtext_len) {
		    err = vstrallocf(_("password write failed: %s: %s"),
				     dle->device, strerror(errno));
		    aclose(passwdfd);
		    goto common_exit;
		}
		/*@end@*/
		memset(user_and_password, '\0', (size_t)lpass);
		amfree(user_and_password);
		aclose(passwdfd);
		ferr = fdopen(checkerr, "r");
		if (!ferr) {
		    g_printf(_("ERROR [Can't fdopen: %s]\n"), strerror(errno));
		    error(_("Can't fdopen: %s"), strerror(errno));
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
		while ((wpid = wait(&retstat)) != -1) {
		    if (!WIFEXITED(retstat) || WEXITSTATUS(retstat) != 0) {
			char *exitstr = str_exit_status("smbclient", retstat);
			strappend(err, sep);
			strappend(err, exitstr);
			sep = "\n";
			amfree(exitstr);

			rc = 1;
		    }
		}
		if (errdos != 0 || rc != 0) {
		    if (extra_info) {
			err = newvstrallocf(err,
					    _("samba access error: %s: %s %s"),
					    dle->device, extra_info, err);
			amfree(extra_info);
		    } else {
			err = newvstrallocf(err,
					    _("samba access error: %s: %s"),
					   dle->device, err);
		    }
		}
#else
		err = vstrallocf(
			      _("This client is not configured for samba: %s"),
			      qdisk);
#endif
		goto common_exit;
	    }
	    amode = F_OK;
	    amfree(device);
	    device = amname_to_dirname(dle->device);
	} else if (strcmp(dle->program, "DUMP") == 0) {
	    if(dle->device[0] == '/' && dle->device[1] == '/') {
		err = vstrallocf(
		  _("The DUMP program cannot handle samba shares, use GNUTAR: %s"),
		  qdisk);
		goto common_exit;
	    }
#ifdef VDUMP								/* { */
#ifdef DUMP								/* { */
            if (strcmp(amname_to_fstype(dle->device), "advfs") == 0)
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
	int                      app_err[2];
	GPtrArray               *errarray;

	bsu = backup_support_option(dle->program, g_options, dle->disk,
				    dle->device, &errarray);

	if (!bsu) {
	    char  *line;
	    guint  i;
	    for (i=0; i < errarray->len; i++) {
		line = g_ptr_array_index(errarray, i);
		fprintf(stdout, _("ERROR Application '%s': %s\n"),
			dle->program, line);
		amfree(line);
	    }
	    err = vstrallocf(_("Application '%s': can't run support command"),
			     dle->program);
	    goto common_exit;
	}

	if (dle->data_path == DATA_PATH_AMANDA &&
	    (bsu->data_path_set & DATA_PATH_AMANDA)==0) {
	    g_printf("ERROR application %s doesn't support amanda data-path\n",
		     dle->program);
	}
	if (dle->data_path == DATA_PATH_DIRECTTCP &&
	    (bsu->data_path_set & DATA_PATH_DIRECTTCP)==0) {
	    g_printf("ERROR application %s doesn't support directtcp data-path\n",
		     dle->program);
	}
	if (GPOINTER_TO_INT(dle->estimatelist->data) == ES_CALCSIZE &&
			    !bsu->calcsize) {
	    g_printf("ERROR application %s doesn't support calcsize estimate\n",
		     dle->program);
	}
	if (dle->include_file && dle->include_file->nb_element > 0 &&
	    !bsu->include_file) {
	    g_printf("ERROR application %s doesn't support include-file\n",
		   dle->program);
	}
	if (dle->include_list && dle->include_list->nb_element > 0 &&
	    !bsu->include_list) {
	    g_printf("ERROR application %s doesn't support include-list\n",
		   dle->program);
	}
	if (dle->include_optional && !bsu->include_optional) {
	    g_printf("ERROR application %s doesn't support optional include\n",
		   dle->program);
	}
	if (dle->exclude_file && dle->exclude_file->nb_element > 0 &&
	    !bsu->exclude_file) {
	    g_printf("ERROR application %s doesn't support exclude-file\n",
		   dle->program);
	}
	if (dle->exclude_list && dle->exclude_list->nb_element > 0 &&
	    !bsu->exclude_list) {
	    g_printf("ERROR application %s doesn't support exclude-list\n",
		   dle->program);
	}
	if (dle->exclude_optional && !bsu->exclude_optional) {
	    g_printf("ERROR application %s doesn't support optional exclude\n",
		   dle->program);
	}
	fflush(stdout);fflush(stderr);

	if (pipe(app_err) < 0) {
	    err = vstrallocf(_("Application '%s': can't create pipe"),
			     dle->program);
	    goto common_exit;
	}

	switch (application_api_pid = fork()) {
	case -1:
	    err = vstrallocf(_("fork failed: %s"), strerror(errno));
	    goto common_exit;

	case 0: /* child */
	    {
		GPtrArray *argv_ptr = g_ptr_array_new();
		guint i;
		char *cmd = vstralloc(APPLICATION_DIR, "/", dle->program, NULL);
		GSList   *scriptlist;
		script_t *script;
		estimatelist_t el;
		char *cmdline;

		aclose(app_err[0]);
		dup2(app_err[1], 2);

		g_ptr_array_add(argv_ptr, stralloc(dle->program));
		g_ptr_array_add(argv_ptr, stralloc("selfcheck"));
		if (bsu->message_line == 1) {
		    g_ptr_array_add(argv_ptr, stralloc("--message"));
		    g_ptr_array_add(argv_ptr, stralloc("line"));
		}
		if (g_options->config != NULL && bsu->config == 1) {
		    g_ptr_array_add(argv_ptr, stralloc("--config"));
		    g_ptr_array_add(argv_ptr, stralloc(g_options->config));
		}
		if (g_options->hostname != NULL && bsu->host == 1) {
		    g_ptr_array_add(argv_ptr, stralloc("--host"));
		    g_ptr_array_add(argv_ptr, stralloc(g_options->hostname));
		}
		if (dle->disk != NULL && bsu->disk == 1) {
		    g_ptr_array_add(argv_ptr, stralloc("--disk"));
		    g_ptr_array_add(argv_ptr, stralloc(dle->disk));
		}
		if (dle->device) {
		    g_ptr_array_add(argv_ptr, stralloc("--device"));
		    g_ptr_array_add(argv_ptr, stralloc(dle->device));
		}
		if (dle->create_index && bsu->index_line == 1) {
		    g_ptr_array_add(argv_ptr, stralloc("--index"));
		    g_ptr_array_add(argv_ptr, stralloc("line"));
		}
		if (dle->record && bsu->record == 1) {
		    g_ptr_array_add(argv_ptr, stralloc("--record"));
		}
		
		for (el = dle->estimatelist; el != NULL; el=el->next) {
		    estimate_t estimate = (estimate_t)GPOINTER_TO_INT(el->data);
		    if (estimate == ES_CALCSIZE && bsu->calcsize == 1) {
			g_ptr_array_add(argv_ptr, stralloc("--calcsize"));
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

		cmdline = stralloc(cmd);
		for (i = 0; i < argv_ptr->len-1; i++) {
		    char *quoted = quote_string(
					(char *)g_ptr_array_index(argv_ptr,i));
		    cmdline = vstrextend(&cmdline, " ", quoted, NULL);
		    amfree(quoted);
		}
		dbprintf(_("Spawning \"%s\" in pipeline\n"), cmdline);
		amfree(cmdline);

		safe_fd(-1, 0);
		execve(cmd, (char **)argv_ptr->pdata, safe_env());
		g_printf(_("ERROR [Can't execute %s: %s]\n"), cmd, strerror(errno));
		exit(127);
	    }
	default: /* parent */
	    {
		int   status;
		FILE *app_stderr;
		char *line;

		aclose(app_err[1]);
		app_stderr = fdopen(app_err[0], "r");
		while((line = agets(app_stderr)) != NULL) {
		    if (strlen(line) > 0) {
			fprintf(stdout, "ERROR Application '%s': %s\n",
				dle->program, line);
			dbprintf("ERROR %s\n", line);
		    }
		    amfree(line);
		}
		fclose(app_stderr);
		if (waitpid(application_api_pid, &status, 0) < 0) {
		    err = vstrallocf(_("waitpid failed: %s"),
					 strerror(errno));
		    goto common_exit;
		} else if (!WIFEXITED(status)) {
		    err = vstrallocf(_("Application '%s': exited with signal %d"),
				     dle->program, WTERMSIG(status));
		    goto common_exit;
		} else if (WEXITSTATUS(status) != 0) {
		    err = vstrallocf(_("Application '%s': exited with status %d"),
				     dle->program, WEXITSTATUS(status));
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
		err = vstrallocf(_("Could not %s %s (%s): %s"),
				 access_type, qdevice, qdisk, strerror(errno));
	    }
#ifdef CHECK_FOR_ACCESS_WITH_OPEN
	    aclose(access_result);
#endif
	}
    }

common_exit:

    if (!qdevice)
	qdevice = quote_string(device);

    amfree(share);
    amfree(subdir);
    if(user_and_password) {
	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
    }
    amfree(domain);

    if(err) {
	g_printf(_("ERROR %s\n"), err);
	dbprintf(_("%s\n"), err);
	amfree(err);
    } else {
	if (dle->disk) {
	    g_printf("OK %s\n", qdisk);
	    dbprintf(_("disk %s OK\n"), qdisk);
	}
	if (dle->device) {
	    g_printf("OK %s\n", qamdevice);
	    dbprintf(_("amdevice %s OK\n"), qamdevice);
	}
	if (device) {
	    g_printf("OK %s\n", qdevice);
	    dbprintf(_("device %s OK\n"), qdevice);
	}
    }
    if(extra_info) {
	dbprintf(_("extra info: %s\n"), extra_info);
	amfree(extra_info);
    }
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
	cmd = vstralloc(amlibexecdir, "/", "runtar", NULL);
	check_file(cmd,X_OK);
	check_suid(cmd);
	amfree(cmd);
    }

    if( need_rundump )
    {
	cmd = vstralloc(amlibexecdir, "/", "rundump", NULL);
	check_file(cmd,X_OK);
	check_suid(cmd);
	amfree(cmd);
    }

    if( need_dump ) {
#ifdef DUMP
	check_file(DUMP, X_OK);
#else
	g_printf(_("ERROR [DUMP program not available]\n"));
#endif
    }

    if( need_restore ) {
#ifdef RESTORE
	check_file(RESTORE, X_OK);
#else
	g_printf(_("ERROR [RESTORE program not available]\n"));
#endif
    }

    if ( need_vdump ) {
#ifdef VDUMP
	check_file(VDUMP, X_OK);
#else
	g_printf(_("ERROR [VDUMP program not available]\n"));
#endif
    }

    if ( need_vrestore ) {
#ifdef VRESTORE
	check_file(VRESTORE, X_OK);
#else
	g_printf(_("ERROR [VRESTORE program not available]\n"));
#endif
    }

    if( need_xfsdump ) {
#ifdef XFSDUMP
	check_file(XFSDUMP, F_OK);
#else
	g_printf(_("ERROR [XFSDUMP program not available]\n"));
#endif
    }

    if( need_xfsrestore ) {
#ifdef XFSRESTORE
	check_file(XFSRESTORE, X_OK);
#else
	g_printf(_("ERROR [XFSRESTORE program not available]\n"));
#endif
    }

    if( need_vxdump ) {
#ifdef VXDUMP
	check_file(VXDUMP, X_OK);
#else
	g_printf(_("ERROR [VXDUMP program not available]\n"));
#endif
    }

    if( need_vxrestore ) {
#ifdef VXRESTORE
	check_file(VXRESTORE, X_OK);
#else
	g_printf(_("ERROR [VXRESTORE program not available]\n"));
#endif
    }

    if( need_gnutar ) {
#ifdef GNUTAR
	check_file(GNUTAR, X_OK);
#else
	g_printf(_("ERROR [GNUTAR program not available]\n"));
#endif
	gnutar_list_dir = getconf_str(CNF_GNUTAR_LIST_DIR);
	if (strlen(gnutar_list_dir) == 0)
	    gnutar_list_dir = NULL;
	if (gnutar_list_dir) {
	    /* make sure our listed-incremental dir is ready */
	    check_dir(gnutar_list_dir, R_OK|W_OK);
	} else {
	    /* no listed-incremental dir, so check that amandates is ready */
	    need_amandates = 1;
	}
    }

    if( need_calcsize ) {
	char *cmd;

	cmd = vstralloc(amlibexecdir, "/", "calcsize", NULL);

	check_file(cmd, X_OK);

	amfree(cmd);

	/* calcsize uses amandates */
	need_amandates = 1;
    }

    if (need_amandates) {
	char *amandates_file;
	amandates_file = getconf_str(CNF_AMANDATES);
	check_file(amandates_file, R_OK|W_OK);
    }

    if( need_samba ) {
#ifdef SAMBA_CLIENT
	check_file(SAMBA_CLIENT, X_OK);
#else
	g_printf(_("ERROR [SMBCLIENT program not available]\n"));
#endif
	testfd = open("/etc/amandapass", R_OK);
	if (testfd >= 0) {
	    if(fstat(testfd, &buf) == 0) {
		if ((buf.st_mode & 0x7) != 0) {
		    g_printf(_("ERROR [/etc/amandapass is world readable!]\n"));
		} else {
		    g_printf(_("OK [/etc/amandapass is readable, but not by all]\n"));
		}
	    } else {
		g_printf(_("OK [unable to stat /etc/amandapass: %s]\n"),
		       strerror(errno));
	    }
	    aclose(testfd);
	} else {
	    g_printf(_("ERROR [unable to open /etc/amandapass: %s]\n"),
		   strerror(errno));
	}
    }

    if (need_compress_path )
	check_file(COMPRESS_PATH, X_OK);

    if (need_dump || need_xfsdump ) {
	if (check_file_exist("/etc/dumpdates")) {
	    check_file("/etc/dumpdates",
#ifdef USE_RUNDUMP
		       F_OK
#else
		       R_OK|W_OK
#endif
		      );
	} else {
#ifndef USE_RUNDUMP
	    if (access("/etc", R_OK|W_OK) == -1) {
		g_printf(_("ERROR [dump will not be able to create the /etc/dumpdates file: %s]\n"), strerror(errno));
	    }
#endif
	}
    }

    if (need_vdump) {
	if (check_file_exist("/etc/vdumpdates")) {
            check_file("/etc/vdumpdates", F_OK);
	}
    }

    if (need_global_check) {
    check_access("/dev/null", R_OK|W_OK);
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
    char *quoted = quote_string(dir);
    intmax_t kb_avail;

    if(get_fs_usage(dir, NULL, &fsusage) == -1) {
	g_printf(_("ERROR [cannot get filesystem usage for %s: %s]\n"), quoted, strerror(errno));
	amfree(quoted);
	return;
    }

    /* do the division first to avoid potential integer overflow */
    kb_avail = fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;

    if (fsusage.fsu_bavail_top_bit_set || fsusage.fsu_bavail == 0) {
	g_printf(_("ERROR [dir %s needs %lldKB, has nothing available.]\n"), quoted,
		(long long)kbytes);
    } else if (kb_avail < kbytes) {
	g_printf(_("ERROR [dir %s needs %lldKB, only has %lldKB available.]\n"), quoted,
		(long long)kbytes,
		(long long)kb_avail);
    } else {
	g_printf(_("OK %s has more than %lldKB available.\n"),
		quoted, (long long)kbytes);
    }
    amfree(quoted);
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
    struct stat stat_buf;
    char *uname;
    char *distro = NULL;
    char *platform = NULL;
    char  line[1025];
    GPtrArray *argv_ptr;

    if (stat("/etc/lsb-release", &stat_buf) == 0) {
	FILE *release = fopen("/etc/lsb-release", "r");
	distro = "Ubuntu";
	if (release) {
	    char *result;
	    while ((result = fgets(line, 1024, release))) {
		if (strstr(line, "DESCRIPTION")) {
		    platform = strchr(line, '=');
		    if (platform) platform++;
		}
	    }
	    fclose(release);
	}
    } else if (stat("/etc/redhat-release", &stat_buf) == 0) {
	FILE *release = fopen("/etc/redhat-release", "r");
	distro = "RPM";
	if (release) {
	    char *result;
	    result = fgets(line, 1024, release);
	    if (result) {
		platform = line;
	    }
	    fclose(release);
	}
    } else if (stat("/etc/debian_version", &stat_buf) == 0) {
	FILE *release = fopen("/etc/debian_version", "r");
	distro = "Debian";
	if (release) {
	    char *result;
	    result = fgets(line, 1024, release);
	    if (result) {
		platform = line;
	    }
	    fclose(release);
	}
    } else {
	argv_ptr = g_ptr_array_new();

	g_ptr_array_add(argv_ptr, UNAME_PATH);
	g_ptr_array_add(argv_ptr, "-s");
	g_ptr_array_add(argv_ptr, NULL);
	uname = get_first_line(argv_ptr);
	if (uname) {
	    if (strncmp(uname, "SunOS", 5) == 0) {
		FILE *release = fopen("/etc/release", "r");
		distro = "Solaris";
		if (release) {
		    char *result;
		    result = fgets(line, 1024, release);
		    if (result) {
			platform = line;
		    }
		}
		fclose(release);
	    }
	    amfree(uname);
	}
	g_ptr_array_free(argv_ptr, TRUE);
    }

    if (!distro) {
	distro = "Unknown";
    }
    if (!platform) {
	platform = "Unknown";
    }
    if (platform[strlen(platform) -1] == '\n') {
	platform[strlen(platform) -1] = '\0';
    }
    g_fprintf(stdout, "OK distro %s\n", distro);
    g_fprintf(stdout, "OK platform %s\n", platform);
}
