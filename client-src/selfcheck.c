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
 * $Id: selfcheck.c,v 1.95 2006/08/29 11:21:00 martinea Exp $
 *
 * do self-check and send back any error messages
 */

#include "amanda.h"
#include "statfs.h"
#include "version.h"
#include "getfsent.h"
#include "amandates.h"
#include "clock.h"
#include "util.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "client_util.h"
#include "conffile.h"
#include "amandad.h"

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
int program_is_backup_api=0;

static char *amandad_auth = NULL;
static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static g_option_t *g_options = NULL;

/* local functions */
int main(int argc, char **argv);

static void check_options(char *program, char *calcprog, char *disk, char *amdevice, option_t *options);
static void check_disk(char *program, char *calcprog, char *disk, char *amdevice, int level, option_t *options);
static void check_overall(void);
static void check_access(char *filename, int mode);
static int check_file_exist(char *filename);
static void check_file(char *filename, int mode);
static void check_dir(char *dirname, int mode);
static void check_suid(char *filename);
static void check_space(char *dir, off_t kbytes);

int
main(
    int		argc,
    char **	argv)
{
    int level;
    char *line = NULL;
    char *program = NULL;
    char *calcprog = NULL;
    char *disk = NULL;
    char *qdisk = NULL;
    char *amdevice = NULL;
    char *qamdevice = NULL;
    char *optstr = NULL;
    char *err_extra = NULL;
    char *s, *fp;
    char *conffile;
    option_t *options;
    int ch;
#if defined(USE_DBMALLOC)
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
#endif

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
    safe_cd();

    set_pname("selfcheck");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

#if defined(USE_DBMALLOC)
    malloc_size_1 = malloc_inuse(&malloc_hist_1);
#endif

    erroutput_type = (ERR_INTERACTIVE|ERR_SYSLOG);
    dbopen(DBG_SUBDIR_CLIENT);
    startclock();
    dbprintf(_("version %s\n"), version());

    if(argc > 2 && strcmp(argv[1], "amandad") == 0) {
	amandad_auth = stralloc(argv[2]);
    }

    conffile = vstralloc(CONFIG_DIR, "/", "amanda-client.conf", NULL);
    if (read_clientconf(conffile) > 0) {
	printf(_("ERROR [reading conffile: %s]\n"), conffile);
	error(_("error reading conffile: %s"), conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

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

	    printf("OPTIONS ");
	    if(am_has_feature(g_options->features, fe_rep_options_features)) {
		printf("features=%s;", our_feature_string);
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
		    printf(_("ERROR [reading conffile: %s]\n"), conffile);
		    error(_("error reading conffile: %s"), conffile);
		    /*NOTREACHED*/
		}
		amfree(conffile);

		dbrename(g_options->config, DBG_SUBDIR_CLIENT);
	    }

	    continue;
	}

	s = line;
	ch = *s++;

	skip_whitespace(s, ch);			/* find program name */
	if (ch == '\0') {
	    goto err;				/* no program */
	}
	program = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';				/* terminate the program name */

	program_is_backup_api = 0;
	if(strcmp(program,"BACKUP")==0) {
	    program_is_backup_api = 1;
	    skip_whitespace(s, ch);		/* find dumper name */
	    if (ch == '\0') {
		goto err;			/* no program */
	    }
	    program = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';			/* terminate the program name */
	}

	if(strncmp_const(program, "CALCSIZE") == 0) {
	    skip_whitespace(s, ch);		/* find program name */
	    if (ch == '\0') {
		goto err;			/* no program */
	    }
	    calcprog = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	}
	else {
	    calcprog = NULL;
	}

	skip_whitespace(s, ch);			/* find disk name */
	if (ch == '\0') {
	    goto err;				/* no disk */
	}
	qdisk = s - 1;
	skip_quoted_string(s, ch);
	s[-1] = '\0';				/* terminate the disk name */
	disk = unquote_string(qdisk);

	skip_whitespace(s, ch);                 /* find the device or level */
	if (ch == '\0') {
	    goto err;				/* no device or level */
	}
	if(!isdigit((int)s[-1])) {
	    fp = s - 1;
	    skip_quoted_string(s, ch);
	     s[-1] = '\0';			/* terminate the device */
	    qamdevice = stralloc(fp);
	    amdevice = unquote_string(qamdevice);
	    skip_whitespace(s, ch);		/* find level number */
	}
	else {
	    amdevice = stralloc(disk);
	}

						/* find level number */
	if (ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
	    goto err;				/* bad level */
	}
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
	    options = parse_options(optstr, disk, amdevice, g_options->features, 1);
	    /*@ignore@*/
	    check_options(program, calcprog, disk, amdevice, options);
	    check_disk(program, calcprog, disk, amdevice, level, options);
	    /*@end@*/
	    free_sl(options->exclude_file);
	    free_sl(options->exclude_list);
	    free_sl(options->include_file);
	    free_sl(options->include_list);
	    amfree(options->auth);
	    amfree(options->str);
	    amfree(options);
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
	    /*@ignore@*/
	    check_disk(program, calcprog, disk, amdevice, level, NULL);
	    /*@end@*/
	} else {
	    goto err;				/* bad syntax */
	}
	amfree(disk);
	amfree(amdevice);
    }

    check_overall();

    amfree(line);
    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;
    am_release_feature_set(g_options->features);
    g_options->features = NULL;
    amfree(g_options->str);
    amfree(g_options->hostname);
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
    printf(_("ERROR [BOGUS REQUEST PACKET]\n"));
    dbprintf(_("REQ packet is bogus%s%s\n"),
	      err_extra ? ": " : "",
	      err_extra ? err_extra : "");
    dbclose();
    return 1;
}


static void
check_options(
    char *	program,
    char *	calcprog,
    char *	disk,
    char *	amdevice,
    option_t *	options)
{
    char *myprogram = program;

    if(strcmp(myprogram,"CALCSIZE") == 0) {
	int nb_exclude = 0;
	int nb_include = 0;
	char *file_exclude = NULL;
	char *file_include = NULL;

	if(options->exclude_file) nb_exclude += options->exclude_file->nb_element;
	if(options->exclude_list) nb_exclude += options->exclude_list->nb_element;
	if(options->include_file) nb_include += options->include_file->nb_element;
	if(options->include_list) nb_include += options->include_list->nb_element;

	if(nb_exclude > 0) file_exclude = build_exclude(disk, amdevice, options, 1);
	if(nb_include > 0) file_include = build_include(disk, amdevice, options, 1);

	amfree(file_exclude);
	amfree(file_include);

	need_calcsize=1;
	if (calcprog == NULL) {
	    printf(_("ERROR [no program name for calcsize]\n"));
	} else {
	    myprogram = calcprog;
	}
    }

    if(strcmp(myprogram,"GNUTAR") == 0) {
	need_gnutar=1;
        if(amdevice[0] == '/' && amdevice[1] == '/') {
	    if(options->exclude_file && options->exclude_file->nb_element > 1) {
		printf(_("ERROR [samba support only one exclude file]\n"));
	    }
	    if(options->exclude_list && options->exclude_list->nb_element > 0 &&
	       options->exclude_optional==0) {
		printf(_("ERROR [samba does not support exclude list]\n"));
	    }
	    if(options->include_file && options->include_file->nb_element > 0) {
		printf(_("ERROR [samba does not support include file]\n"));
	    }
	    if(options->include_list && options->include_list->nb_element > 0 &&
	       options->include_optional==0) {
		printf(_("ERROR [samba does not support include list]\n"));
	    }
	    need_samba=1;
	}
	else {
	    int nb_exclude = 0;
	    int nb_include = 0;
	    char *file_exclude = NULL;
	    char *file_include = NULL;

	    if(options->exclude_file) nb_exclude += options->exclude_file->nb_element;
	    if(options->exclude_list) nb_exclude += options->exclude_list->nb_element;
	    if(options->include_file) nb_include += options->include_file->nb_element;
	    if(options->include_list) nb_include += options->include_list->nb_element;

	    if(nb_exclude > 0) file_exclude = build_exclude(disk, amdevice, options, 1);
	    if(nb_include > 0) file_include = build_include(disk, amdevice, options, 1);

	    amfree(file_exclude);
	    amfree(file_include);

	    need_runtar=1;
	}
    }

    if(strcmp(myprogram,"DUMP") == 0) {
	if(options->exclude_file && options->exclude_file->nb_element > 0) {
	    printf(_("ERROR [DUMP does not support exclude file]\n"));
	}
	if(options->exclude_list && options->exclude_list->nb_element > 0) {
	    printf(_("ERROR [DUMP does not support exclude list]\n"));
	}
	if(options->include_file && options->include_file->nb_element > 0) {
	    printf(_("ERROR [DUMP does not support include file]\n"));
	}
	if(options->include_list && options->include_list->nb_element > 0) {
	    printf(_("ERROR [DUMP does not support include list]\n"));
	}
#ifdef USE_RUNDUMP
	need_rundump=1;
#endif
#ifndef AIX_BACKUP
#ifdef VDUMP
#ifdef DUMP
	if (strcmp(amname_to_fstype(amdevice), "advfs") == 0)
#else
	if (1)
#endif
	{
	    need_vdump=1;
	    need_rundump=1;
	    if (options->createindex)
		need_vrestore=1;
	}
	else
#endif /* VDUMP */
#ifdef XFSDUMP
#ifdef DUMP
	if (strcmp(amname_to_fstype(amdevice), "xfs") == 0)
#else
	if (1)
#endif
	{
	    need_xfsdump=1;
	    need_rundump=1;
	    if (options->createindex)
		need_xfsrestore=1;
	}
	else
#endif /* XFSDUMP */
#ifdef VXDUMP
#ifdef DUMP
	if (strcmp(amname_to_fstype(amdevice), "vxfs") == 0)
#else
	if (1)
#endif
	{
	    need_vxdump=1;
	    if (options->createindex)
		need_vxrestore=1;
	}
	else
#endif /* VXDUMP */
	{
	    need_dump=1;
	    if (options->createindex)
		need_restore=1;
	}
#else
	/* AIX backup program */
	need_dump=1;
	if (options->createindex)
	    need_restore=1;
#endif
    }
    if ((options->compress == COMP_BEST) || (options->compress == COMP_FAST) 
		|| (options->compress == COMP_CUST)) {
	need_compress_path=1;
    }
    if(options->auth && amandad_auth) {
	if(strcasecmp(options->auth, amandad_auth) != 0) {
	    fprintf(stdout,_("ERROR [client configured for auth=%s while server requested '%s']\n"),
		    amandad_auth, options->auth);
	    if(strcmp(options->auth, "ssh") == 0)  {	
		fprintf(stderr, _("ERROR [The auth in ~/.ssh/authorized_keys "
				  "should be \"--auth=ssh\", or use another auth "
				  " for the DLE]\n"));
	    }
	    else {
		fprintf(stderr, _("ERROR [The auth in the inetd/xinetd configuration "
				  " must be the same as the DLE]\n"));
	    }		
	}
    }
}

static void
check_disk(
    char *	program,
    char *	calcprog,
    char *	disk,
    char *	amdevice,
    int		level,
    option_t    *options)
{
    char *device = stralloc("nodevice");
    char *err = NULL;
    char *user_and_password = NULL;
    char *domain = NULL;
    char *share = NULL, *subdir = NULL;
    size_t lpass = 0;
    int amode;
    int access_result;
    char *access_type;
    char *extra_info = NULL;
    char *myprogram = program;
    char *qdisk = quote_string(disk);
    char *qamdevice = quote_string(amdevice);
    char *qdevice = NULL;
    FILE *toolin;

    (void)level;	/* Quiet unused parameter warning */

    dbprintf(_("checking disk %s\n"), qdisk);

    if(strcmp(myprogram,"CALCSIZE") == 0) {
	if(amdevice[0] == '/' && amdevice[1] == '/') {
	    err = vstrallocf(_("Can't use CALCSIZE for samba estimate, use CLIENT: %s"),
			    amdevice);
	    goto common_exit;
	}
	if (calcprog == NULL) {
	    err = _("no program for calcsize");
	    goto common_exit;
	}
	myprogram = calcprog;
    }

    if (strcmp(myprogram, "GNUTAR")==0) {
        if(amdevice[0] == '/' && amdevice[1] == '/') {
#ifdef SAMBA_CLIENT
	    int nullfd, checkerr;
	    int passwdfd;
	    char *pwtext;
	    size_t pwtext_len;
	    pid_t checkpid;
	    amwait_t retstat;
	    pid_t wpid;
	    int ret, sig, rc;
	    char *line;
	    char *sep;
	    FILE *ferr;
	    char *pw_fd_env;
	    int errdos;

	    parsesharename(amdevice, &share, &subdir);
	    if (!share) {
		err = vstrallocf(_("cannot parse for share/subdir disk entry %s"), amdevice);
		goto common_exit;
	    }
	    if ((subdir) && (SAMBA_VERSION < 2)) {
		err = vstrallocf(_("subdirectory specified for share '%s' but, samba is not v2 or better"),
				amdevice);
		goto common_exit;
	    }
	    if ((user_and_password = findpass(share, &domain)) == NULL) {
		err = vstrallocf(_("cannot find password for %s"), amdevice);
		goto common_exit;
	    }
	    lpass = strlen(user_and_password);
	    if ((pwtext = strchr(user_and_password, '%')) == NULL) {
		err = vstrallocf(_("password field not \'user%%pass\' for %s"), amdevice);
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
	        err = vstrallocf(_("Cannot access /dev/null : %s"), strerror(errno));
		goto common_exit;
	    }

	    if (pwtext_len > 0) {
		pw_fd_env = "PASSWD_FD";
	    } else {
		pw_fd_env = "dummy_PASSWD_FD";
	    }
	    checkpid = pipespawn(SAMBA_CLIENT, STDERR_PIPE|PASSWD_PIPE,
				 &nullfd, &nullfd, &checkerr,
				 pw_fd_env, &passwdfd,
				 "smbclient",
				 device,
				 *user_and_password ? "-U" : skip_argument,
				 *user_and_password ? user_and_password : skip_argument,
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
	    if ((pwtext_len > 0)
	      && fullwrite(passwdfd, pwtext, (size_t)pwtext_len) < 0) {
		err = vstrallocf(_("password write failed: %s: %s"),
				amdevice, strerror(errno));
		aclose(passwdfd);
		goto common_exit;
	    }
	    /*@end@*/
	    memset(user_and_password, '\0', (size_t)lpass);
	    amfree(user_and_password);
	    aclose(passwdfd);
	    ferr = fdopen(checkerr, "r");
	    if (!ferr) {
		printf(_("ERROR [Can't fdopen: %s]\n"), strerror(errno));
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
	    while ((wpid = wait(&retstat)) != -1) {
		if (WIFSIGNALED(retstat)) {
		    ret = 0;
		    rc = sig = WTERMSIG(retstat);
		} else {
		    sig = 0;
		    rc = ret = WEXITSTATUS(retstat);
		}
		if (rc != 0) {
		    if (ret == 0) {
			err = newvstrallocf(err, _("%s%s got signal %d"),
				sep, err, sig);
			ret = sig;
		    } else {
			err = newvstrallocf(err, _("%s%s returned %d"),
				sep, err, ret);
		    }
		}
	    }
	    if (errdos != 0 || rc != 0) {
		if (extra_info) {
		    err = newvstrallocf(err,
				   _("samba access error: %s: %s %s"),
				   amdevice, extra_info, err);
		    amfree(extra_info);
		} else {
		    err = newvstrallocf(err, _("samba access error: %s: %s"),
				   amdevice, err);
		}
	    }
#else
	    err = vstrallocf(_("This client is not configured for samba: %s"),
			qdisk);
#endif
	    goto common_exit;
	}
	amode = F_OK;
	amfree(device);
	device = amname_to_dirname(amdevice);
    } else if (strcmp(myprogram, "DUMP") == 0) {
	if(amdevice[0] == '/' && amdevice[1] == '/') {
	    err = vstrallocf(
		  _("The DUMP program cannot handle samba shares, use GNUTAR: %s"),
		  qdisk);
	    goto common_exit;
	}
#ifdef VDUMP								/* { */
#ifdef DUMP								/* { */
        if (strcmp(amname_to_fstype(amdevice), "advfs") == 0)
#else									/* }{ */
	if (1)
#endif									/* } */
	{
	    amfree(device);
	    device = amname_to_dirname(amdevice);
	    amode = F_OK;
	} else
#endif									/* } */
	{
	    amfree(device);
	    device = amname_to_devname(amdevice);
#ifdef USE_RUNDUMP
	    amode = F_OK;
#else
	    amode = R_OK;
#endif
	}
    }
    else { /* program_is_backup_api==1 */
	pid_t  backup_api_pid;
	int    property_pipe[2];
	backup_support_option_t *bsu;

	bsu = backup_support_option(program, g_options, disk, amdevice);

	if (pipe(property_pipe) < 0) {
	    err = vstrallocf(_("pipe failed: %s"), strerror(errno));
	    goto common_exit;
	}
	fflush(stdout);fflush(stderr);
	
	switch (backup_api_pid = fork()) {
	case -1:
	    err = vstrallocf(_("fork failed: %s"), strerror(errno));
	    goto common_exit;

	case 0: /* child */
	    {
		char *argvchild[14];
		char *cmd = vstralloc(DUMPER_DIR, "/", program, NULL);
		int j=0;
		argvchild[j++] = program;
		argvchild[j++] = "selfcheck";
		if (bsu->message_line == 1) {
		    argvchild[j++] = "--message";
		    argvchild[j++] = "line";
		}
		if (g_options->config != NULL && bsu->config == 1) {
		    argvchild[j++] = "--config";
		    argvchild[j++] = g_options->config;
		}
		if (g_options->hostname != NULL && bsu->host == 1) {
		    argvchild[j++] = "--host";
		    argvchild[j++] = g_options->hostname;
		}
		if (disk != NULL && bsu->disk == 1) {
		    argvchild[j++] = "--disk";
		    argvchild[j++] = disk;
		}
		argvchild[j++] = "--device";
		argvchild[j++] = amdevice;
		if(options && options->createindex && bsu->index_line == 1) {
		    argvchild[j++] = "--index";
		    argvchild[j++] = "line";
		}
		if (!options->no_record && bsu->record == 1) {
		    argvchild[j++] = "--record";
		}
		argvchild[j++] = NULL;
		dup2(property_pipe[0], 0);
		aclose(property_pipe[1]);
		execve(cmd,argvchild,safe_env());
		printf(_("ERROR [Can't execute %s: %s]\n"), cmd, strerror(errno));
		exit(127);
	    }
	default: /* parent */
	    {
		int status;
		aclose(property_pipe[0]);
		toolin = fdopen(property_pipe[1],"w");
		if (!toolin) {
		    err = vstrallocf(_("Can't fdopen: %s"), strerror(errno));
		    goto common_exit;
		}
		output_tool_property(toolin, options);
		fflush(toolin);
		fclose(toolin);
		if (waitpid(backup_api_pid, &status, 0) < 0) {
		    if (!WIFEXITED(status)) {
			err = vstrallocf(_("Tool exited with signal %d"),
					 WTERMSIG(status));
		    } else if (WEXITSTATUS(status) != 0) {
			err = vstrallocf(_("Tool exited with status %d"),
					 WEXITSTATUS(status));
		    } else {
			err = vstrallocf(_("waitpid returned negative value"));
		    }
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
	    err = vstrallocf(_("Could not access %s (%s): %s"),
			qdevice, qdisk, strerror(errno));
	}
#ifdef CHECK_FOR_ACCESS_WITH_OPEN
	aclose(access_result);
#endif
    }

common_exit:

    amfree(share);
    amfree(subdir);
    if(user_and_password) {
	memset(user_and_password, '\0', (size_t)lpass);
	amfree(user_and_password);
    }
    amfree(domain);

    if(err) {
	printf(_("ERROR [%s]\n"), err);
	dbprintf(_("%s\n"), err);
	amfree(err);
    } else {
	printf("OK %s\n", qdisk);
	dbprintf(_("disk %s OK\n"), qdisk);
	printf("OK %s\n", qamdevice);
	dbprintf(_("amdevice %s OK\n"), qamdevice);
	printf("OK %s\n", qdevice);
	dbprintf(_("device %s OK\n"), qdevice);
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
	cmd = vstralloc(libexecdir, "/", "runtar", versionsuffix(), NULL);
	check_file(cmd,X_OK);
	check_suid(cmd);
	amfree(cmd);
    }

    if( need_rundump )
    {
	cmd = vstralloc(libexecdir, "/", "rundump", versionsuffix(), NULL);
	check_file(cmd,X_OK);
	check_suid(cmd);
	amfree(cmd);
    }

    if( need_dump ) {
#ifdef DUMP
	check_file(DUMP, X_OK);
#else
	printf(_("ERROR [DUMP program not available]\n"));
#endif
    }

    if( need_restore ) {
#ifdef RESTORE
	check_file(RESTORE, X_OK);
#else
	printf(_("ERROR [RESTORE program not available]\n"));
#endif
    }

    if ( need_vdump ) {
#ifdef VDUMP
	check_file(VDUMP, X_OK);
#else
	printf(_("ERROR [VDUMP program not available]\n"));
#endif
    }

    if ( need_vrestore ) {
#ifdef VRESTORE
	check_file(VRESTORE, X_OK);
#else
	printf(_("ERROR [VRESTORE program not available]\n"));
#endif
    }

    if( need_xfsdump ) {
#ifdef XFSDUMP
	check_file(XFSDUMP, F_OK);
#else
	printf(_("ERROR [XFSDUMP program not available]\n"));
#endif
    }

    if( need_xfsrestore ) {
#ifdef XFSRESTORE
	check_file(XFSRESTORE, X_OK);
#else
	printf(_("ERROR [XFSRESTORE program not available]\n"));
#endif
    }

    if( need_vxdump ) {
#ifdef VXDUMP
	check_file(VXDUMP, X_OK);
#else
	printf(_("ERROR [VXDUMP program not available]\n"));
#endif
    }

    if( need_vxrestore ) {
#ifdef VXRESTORE
	check_file(VXRESTORE, X_OK);
#else
	printf(_("ERROR [VXRESTORE program not available]\n"));
#endif
    }

    if( need_gnutar ) {
#ifdef GNUTAR
	check_file(GNUTAR, X_OK);
#else
	printf(_("ERROR [GNUTAR program not available]\n"));
#endif
	need_amandates = 1;
	gnutar_list_dir = getconf_str(CNF_GNUTAR_LIST_DIR);
	if (strlen(gnutar_list_dir) == 0)
	    gnutar_list_dir = NULL;
	if (gnutar_list_dir) 
	    check_dir(gnutar_list_dir, R_OK|W_OK);
    }

    if (need_amandates) {
	char *amandates_file;
	amandates_file = getconf_str(CNF_AMANDATES);
	check_file(amandates_file, R_OK|W_OK);
    }
    if( need_calcsize ) {
	char *cmd;

	cmd = vstralloc(libexecdir, "/", "calcsize", versionsuffix(), NULL);

	check_file(cmd, X_OK);

	amfree(cmd);
    }

    if( need_samba ) {
#ifdef SAMBA_CLIENT
	check_file(SAMBA_CLIENT, X_OK);
#else
	printf(_("ERROR [SMBCLIENT program not available]\n"));
#endif
	testfd = open("/etc/amandapass", R_OK);
	if (testfd >= 0) {
	    if(fstat(testfd, &buf) == 0) {
		if ((buf.st_mode & 0x7) != 0) {
		    printf(_("ERROR [/etc/amandapass is world readable!]\n"));
		} else {
		    printf(_("OK [/etc/amandapass is readable, but not by all]\n"));
		}
	    } else {
		printf(_("OK [unable to stat /etc/amandapass: %s]\n"),
		       strerror(errno));
	    }
	    aclose(testfd);
	} else {
	    printf(_("ERROR [unable to open /etc/amandapass: %s]\n"),
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
		printf(_("ERROR [dump will not be able to create the /etc/dumpdates file: %s]\n"), strerror(errno));
	    }
#endif
	}
    }

    if (need_vdump) {
	if (check_file_exist("/etc/vdumpdates")) {
            check_file("/etc/vdumpdates", F_OK);
	}
    }

    check_access("/dev/null", R_OK|W_OK);
    check_space(AMANDA_TMPDIR, (off_t)64);	/* for amandad i/o */

#ifdef AMANDA_DBGDIR
    check_space(AMANDA_DBGDIR, (off_t)64);	/* for amandad i/o */
#endif

    check_space("/etc", (off_t)64);		/* for /etc/dumpdates writing */
}

static void
check_space(
    char *	dir,
    off_t	kbytes)
{
    generic_fs_stats_t statp;
    char *quoted = quote_string(dir);

    if(get_fs_stats(dir, &statp) == -1) {
	printf(_("ERROR [cannot statfs %s: %s]\n"), quoted, strerror(errno));
    } else if(statp.avail < kbytes) {
	printf(_("ERROR [dir %s needs " OFF_T_FMT "KB, only has "
		OFF_T_FMT "KB available.]\n"), quoted,
		(OFF_T_FMT_TYPE)kbytes,
		(OFF_T_FMT_TYPE)statp.avail);
    } else {
	printf(_("OK %s has more than " OFF_T_FMT " KB available.\n"),
		quoted, (OFF_T_FMT_TYPE)kbytes);
    }
    amfree(quoted);
}

static void
check_access(
    char *	filename,
    int		mode)
{
    char *noun, *adjective;
    char *quoted = quote_string(filename);

    if(mode == F_OK)
        noun = "find", adjective = "exists";
    else if((mode & X_OK) == X_OK)
	noun = "execute", adjective = "executable";
    else if((mode & (W_OK|R_OK)) == (W_OK|R_OK))
	noun = "read/write", adjective = "read/writable";
    else 
	noun = "access", adjective = "accessible";

    if(access(filename, mode) == -1)
	printf(_("ERROR [can not %s %s: %s]\n"), noun, quoted, strerror(errno));
    else
	printf(_("OK %s %s\n"), quoted, adjective);
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
check_file(
    char *	filename,
    int		mode)
{
    struct stat stat_buf;
    char *quoted;

    if(!stat(filename, &stat_buf)) {
	if(!S_ISREG(stat_buf.st_mode)) {
	    quoted = quote_string(filename);
	    printf(_("ERROR [%s is not a file]\n"), quoted);
	    amfree(quoted);
	}
    }
    check_access(filename, mode);
}

static void
check_dir(
    char *	dirname,
    int		mode)
{
    struct stat stat_buf;
    char *quoted;
    char *dir;

    if(!stat(dirname, &stat_buf)) {
	if(!S_ISDIR(stat_buf.st_mode)) {
	    quoted = quote_string(dirname);
	    printf(_("ERROR [%s is not a directory]\n"), quoted);
	    amfree(quoted);
	}
    }
    dir = stralloc2(dirname, "/.");
    check_access(dir, mode);
    amfree(dir);
}

static void
check_suid(
    char *	filename)
{
/* The following is only valid for real Unixs */
#ifndef IGNORE_UID_CHECK
    struct stat stat_buf;
    char *quoted = quote_string(filename);

    if(!stat(filename, &stat_buf)) {
	if(stat_buf.st_uid != 0 ) {
	    printf(_("ERROR [%s is not owned by root]\n"), quoted);
	}
	if((stat_buf.st_mode & S_ISUID) != S_ISUID) {
	    printf(_("ERROR [%s is not SUID root]\n"), quoted);
	}
    }
    else {
	printf(_("ERROR [can not stat %s]\n"), quoted);
    }
    amfree(quoted);
#else
    (void)filename;	/* Quiet unused parameter warning */
#endif
}
