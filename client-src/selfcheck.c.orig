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
 * $Id: selfcheck.c,v 1.92 2006/07/25 18:35:21 martinea Exp $
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
#include "clientconf.h"
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
int program_is_wrapper=0;

static char *amandad_auth = NULL;
static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static g_option_t *g_options = NULL;

/* local functions */
int main(int argc, char **argv);

static void check_options(char *program, char *calcprog, char *disk, char *amdevice, option_t *options);
static void check_disk(char *program, char *calcprog, char *disk, char *amdevice, int level, char *optstr);
static void check_overall(void);
static void check_access(char *filename, int mode);
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
    dbprintf(("%s: version %s\n", get_pname(), version()));

    if(argc > 2 && strcmp(argv[1], "amandad") == 0) {
	amandad_auth = stralloc(argv[2]);
    }

    conffile = vstralloc(CONFIG_DIR, "/", "amanda-client.conf", NULL);
    if (read_clientconf(conffile) > 0) {
	error("error reading conffile: %s", conffile);
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

	skip_whitespace(s, ch);			/* find program name */
	if (ch == '\0') {
	    goto err;				/* no program */
	}
	program = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';				/* terminate the program name */

	program_is_wrapper = 0;
	if(strcmp(program,"DUMPER")==0) {
	    program_is_wrapper = 1;
	    skip_whitespace(s, ch);		/* find dumper name */
	    if (ch == '\0') {
		goto err;			/* no program */
	    }
	    program = s - 1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';			/* terminate the program name */
	}

	if(strncmp(program, "CALCSIZE", 8) == 0) {
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
#define sc "OPTIONS "
	if (ch && strncmp (s - 1, sc, SIZEOF(sc)-1) == 0) {
	    s += SIZEOF(sc)-1;
	    ch = s[-1];
#undef sc
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
	    check_disk(program, calcprog, disk, amdevice, level, &optstr[2]);
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
	    check_disk(program, calcprog, disk, amdevice, level, "");
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
	extern int dbfd;

	malloc_list(dbfd(), malloc_hist_1, malloc_hist_2);
    }
#endif

    dbclose();
    return 0;

 err:
    printf("ERROR [BOGUS REQUEST PACKET]\n");
    dbprintf(("%s: REQ packet is bogus%s%s\n",
	      debug_prefix_time(NULL),
	      err_extra ? ": " : "",
	      err_extra ? err_extra : ""));
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
	    printf("ERROR [no program name for calcsize]\n");
	} else {
	    myprogram = calcprog;
	}
    }

    if(strcmp(myprogram,"GNUTAR") == 0) {
	need_gnutar=1;
        if(amdevice[0] == '/' && amdevice[1] == '/') {
	    if(options->exclude_file && options->exclude_file->nb_element > 1) {
		printf("ERROR [samba support only one exclude file]\n");
	    }
	    if(options->exclude_list && options->exclude_list->nb_element > 0 &&
	       options->exclude_optional==0) {
		printf("ERROR [samba does not support exclude list]\n");
	    }
	    if(options->include_file && options->include_file->nb_element > 0) {
		printf("ERROR [samba does not support include file]\n");
	    }
	    if(options->include_list && options->include_list->nb_element > 0 &&
	       options->include_optional==0) {
		printf("ERROR [samba does not support include list]\n");
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
	    printf("ERROR [DUMP does not support exclude file]\n");
	}
	if(options->exclude_list && options->exclude_list->nb_element > 0) {
	    printf("ERROR [DUMP does not support exclude list]\n");
	}
	if(options->include_file && options->include_file->nb_element > 0) {
	    printf("ERROR [DUMP does not support include file]\n");
	}
	if(options->include_list && options->include_list->nb_element > 0) {
	    printf("ERROR [DUMP does not support include list]\n");
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
    if ((options->compress == COMPR_BEST) || (options->compress == COMPR_FAST) 
		|| (options->compress == COMPR_CUST)) {
	need_compress_path=1;
    }
    if(options->auth && amandad_auth) {
	if(strcasecmp(options->auth, amandad_auth) != 0) {
	    fprintf(stdout,"ERROR [client configured for auth=%s while server requested '%s']\n",
		    amandad_auth, options->auth);
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
    char *	optstr)
{
    char *device = "nodevice";
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

    (void)level;	/* Quiet unused parameter warning */

    dbprintf(("%s: checking disk %s\n", debug_prefix_time(NULL), qdisk));

    if(strcmp(myprogram,"CALCSIZE") == 0) {
	if(amdevice[0] == '/' && amdevice[1] == '/') {
	    err = vstralloc("Can't use CALCSIZE for samba estimate,",
			    " use CLIENT: ",
			    amdevice,
			    NULL);
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
	    char number[NUM_STR_SIZE];
	    pid_t wpid;
	    int ret, sig, rc;
	    char *line;
	    char *sep;
	    FILE *ferr;
	    char *pw_fd_env;
	    int errdos;

	    parsesharename(amdevice, &share, &subdir);
	    if (!share) {
		err = stralloc2("cannot parse for share/subdir disk entry ", amdevice);
		goto common_exit;
	    }
	    if ((subdir) && (SAMBA_VERSION < 2)) {
		err = vstralloc("subdirectory specified for share '",
				amdevice,
				"' but samba not v2 or better",
				NULL);
		goto common_exit;
	    }
	    if ((user_and_password = findpass(share, &domain)) == NULL) {
		err = stralloc2("cannot find password for ", amdevice);
		goto common_exit;
	    }
	    lpass = strlen(user_and_password);
	    if ((pwtext = strchr(user_and_password, '%')) == NULL) {
		err = stralloc2("password field not \'user%pass\' for ", amdevice);
		goto common_exit;
	    }
	    *pwtext++ = '\0';
	    pwtext_len = (size_t)strlen(pwtext);
	    if ((device = makesharename(share, 0)) == NULL) {
		err = stralloc2("cannot make share name of ", share);
		goto common_exit;
	    }

	    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	        err = stralloc2("Cannot access /dev/null : ", strerror(errno));
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
		err = vstralloc("password write failed: ",
				amdevice,
				": ",
				strerror(errno),
				NULL);
		aclose(passwdfd);
		goto common_exit;
	    }
	    /*@end@*/
	    memset(user_and_password, '\0', (size_t)lpass);
	    amfree(user_and_password);
	    aclose(passwdfd);
	    ferr = fdopen(checkerr, "r");
	    if (!ferr) {
		error("Can't fdopen: %s", strerror(errno));
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
		    strappend(err, sep);
		    if (ret == 0) {
			strappend(err, "got signal ");
			ret = sig;
		    } else {
			strappend(err, "returned ");
		    }
		    snprintf(number, (size_t)sizeof(number), "%d", ret);
		    strappend(err, number);
		}
	    }
	    if (errdos != 0 || rc != 0) {
		err = newvstralloc(err,
				   "samba access error: ",
				   amdevice,
				   ": ",
				   extra_info ? extra_info : "",
				   err,
				   NULL);
		amfree(extra_info);
	    }
#else
	    err = stralloc2("This client is not configured for samba: ", qdisk);
#endif
	    goto common_exit;
	}
	amode = F_OK;
	device = amname_to_dirname(amdevice);
    } else if (strcmp(program, "DUMP") == 0) {
	if(amdevice[0] == '/' && amdevice[1] == '/') {
	    err = vstralloc("The DUMP program cannot handle samba shares,",
			    " use GNUTAR: ",
			    qdisk,
			    NULL);
	    goto common_exit;
	}
#ifdef VDUMP								/* { */
#ifdef DUMP								/* { */
        if (strcmp(amname_to_fstype(amdevice), "advfs") == 0)
#else									/* }{ */
	if (1)
#endif									/* } */
	{
	    device = amname_to_dirname(amdevice);
	    amode = F_OK;
	} else
#endif									/* } */
	{
	    device = amname_to_devname(amdevice);
#ifdef USE_RUNDUMP
	    amode = F_OK;
#else
	    amode = R_OK;
#endif
	}
    }
    else { /* program_is_wrapper==1 */
	pid_t pid_wrapper;
	fflush(stdout);fflush(stdin);
	switch (pid_wrapper = fork()) {
	case -1:
	    error("fork: %s", strerror(errno));
	    /*NOTREACHED*/

	case 0: /* child */
	    {
		char *argvchild[6];
		char *cmd = vstralloc(DUMPER_DIR, "/", program, NULL);
		argvchild[0] = program;
		argvchild[1] = "selfcheck";
		argvchild[2] = disk;
		argvchild[3] = amdevice;
		argvchild[4] = optstr;
		argvchild[5] = NULL;
		execve(cmd,argvchild,safe_env());
		exit(127);
	    }
	default: /* parent */
	    {
		int status;
		waitpid(pid_wrapper, &status, 0);
	    }
	}
	fflush(stdout);fflush(stdin);
	amfree(qamdevice);
	amfree(qdisk);
	return;
    }

    qdevice = quote_string(device);
    dbprintf(("%s: device %s\n", debug_prefix_time(NULL), qdevice));

    /* skip accessability test if this is an AFS entry */
    if(strncmp(device, "afs:", 4) != 0) {
#ifdef CHECK_FOR_ACCESS_WITH_OPEN
	access_result = open(device, O_RDONLY);
	access_type = "open";
#else
	access_result = access(device, amode);
	access_type = "access";
#endif
	if(access_result == -1) {
	    err = vstralloc("could not ", access_type, " ", qdevice,
			" (", qdisk, "): ", strerror(errno), NULL);
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
	printf("ERROR [%s]\n", err);
	dbprintf(("%s: %s\n", debug_prefix_time(NULL), err));
	amfree(err);
    } else {
	printf("OK %s\n", qdisk);
	dbprintf(("%s: disk %s OK\n", debug_prefix_time(NULL), qdisk));
	printf("OK %s\n", qamdevice);
	dbprintf(("%s: amdevice %s OK\n",
		  debug_prefix_time(NULL), qamdevice));
	printf("OK %s\n", qdevice);
	dbprintf(("%s: device %s OK\n", debug_prefix_time(NULL), qdevice));
    }
    if(extra_info) {
	dbprintf(("%s: extra info: %s\n", debug_prefix_time(NULL), extra_info));
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
    int   need_amandates;

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
	printf("ERROR [DUMP program not available]\n");
#endif
    }

    if( need_restore ) {
#ifdef RESTORE
	check_file(RESTORE, X_OK);
#else
	printf("ERROR [RESTORE program not available]\n");
#endif
    }

    if ( need_vdump ) {
#ifdef VDUMP
	check_file(VDUMP, X_OK);
#else
	printf("ERROR [VDUMP program not available]\n");
#endif
    }

    if ( need_vrestore ) {
#ifdef VRESTORE
	check_file(VRESTORE, X_OK);
#else
	printf("ERROR [VRESTORE program not available]\n");
#endif
    }

    if( need_xfsdump ) {
#ifdef XFSDUMP
	check_file(XFSDUMP, F_OK);
#else
	printf("ERROR [XFSDUMP program not available]\n");
#endif
    }

    if( need_xfsrestore ) {
#ifdef XFSRESTORE
	check_file(XFSRESTORE, X_OK);
#else
	printf("ERROR [XFSRESTORE program not available]\n");
#endif
    }

    if( need_vxdump ) {
#ifdef VXDUMP
	check_file(VXDUMP, X_OK);
#else
	printf("ERROR [VXDUMP program not available]\n");
#endif
    }

    if( need_vxrestore ) {
#ifdef VXRESTORE
	check_file(VXRESTORE, X_OK);
#else
	printf("ERROR [VXRESTORE program not available]\n");
#endif
    }

    if( need_gnutar ) {
#ifdef GNUTAR
	check_file(GNUTAR, X_OK);
#else
	printf("ERROR [GNUTAR program not available]\n");
#endif
	need_amandates = 1;
	gnutar_list_dir = client_getconf_str(CLN_GNUTAR_LIST_DIR);
	if (strlen(gnutar_list_dir) == 0)
	    gnutar_list_dir = NULL;
	if (gnutar_list_dir) 
	    check_dir(gnutar_list_dir, R_OK|W_OK);
    }

    if (need_amandates) {
	char *amandates_file;
	amandates_file = client_getconf_str(CLN_AMANDATES);
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
	printf("ERROR [SMBCLIENT program not available]\n");
#endif
	testfd = open("/etc/amandapass", R_OK);
	if (testfd >= 0) {
	    if(fstat(testfd, &buf) == 0) {
		if ((buf.st_mode & 0x7) != 0) {
		    printf("ERROR [/etc/amandapass is world readable!]\n");
		} else {
		    printf("OK [/etc/amandapass is readable, but not by all]\n");
		}
	    } else {
		printf("OK [unable to stat /etc/amandapass: %s]\n",
		       strerror(errno));
	    }
	    aclose(testfd);
	} else {
	    printf("ERROR [unable to open /etc/amandapass: %s]\n",
		   strerror(errno));
	}
    }

    if( need_compress_path )
	check_file(COMPRESS_PATH, X_OK);

    if( need_dump || need_xfsdump )
	check_file("/etc/dumpdates",
#ifdef USE_RUNDUMP
		   F_OK
#else
		   R_OK|W_OK
#endif
		   );

    if (need_vdump)
        check_file("/etc/vdumpdates", F_OK);

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
	printf("ERROR [cannot statfs %s: %s]\n", quoted, strerror(errno));
    } else if(statp.avail < kbytes) {
	printf("ERROR [dir %s needs " OFF_T_FMT "KB, only has "
		OFF_T_FMT "KB available.]\n", quoted,
		(OFF_T_FMT_TYPE)kbytes,
		(OFF_T_FMT_TYPE)statp.avail);
    } else {
	printf("OK %s has more than " OFF_T_FMT " KB available.\n",
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
	printf("ERROR [can not %s %s: %s]\n", noun, quoted, strerror(errno));
    else
	printf("OK %s %s\n", quoted, adjective);
    amfree(quoted);
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
	    printf("ERROR [%s is not a file]\n", quoted);
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
	    printf("ERROR [%s is not a directory]\n", quoted);
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
	    printf("ERROR [%s is not owned by root]\n", quoted);
	}
	if((stat_buf.st_mode & S_ISUID) != S_ISUID) {
	    printf("ERROR [%s is not SUID root]\n", quoted);
	}
    }
    else {
	printf("ERROR [can not stat %s]\n", quoted);
    }
    amfree(quoted);
#else
    (void)filename;	/* Quiet unused parameter warning */
#endif
}
