/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * $Id: amcheck.c,v 1.149 2006/08/24 01:57:16 paddy_s Exp $
 *
 * checks for common problems in server and clients
 */
#include "amanda.h"
#include "util.h"
#include "conffile.h"
#include "statfs.h"
#include "diskfile.h"
#include "tapefile.h"
#include "tapeio.h"
#include "changer.h"
#include "packet.h"
#include "security.h"
#include "protocol.h"
#include "clock.h"
#include "version.h"
#include "amindex.h"
#include "token.h"
#include "taperscan.h"
#include "server_util.h"
#include "pipespawn.h"
#include "amfeatures.h"

#define BUFFER_SIZE	32768

static time_t conf_ctimeout;
static int overwrite;

static disklist_t origq;

static uid_t uid_dumpuser;

/* local functions */

void usage(void);
pid_t start_client_checks(int fd);
pid_t start_server_check(int fd, int do_localchk, int do_tapechk);
int main(int argc, char **argv);
int test_server_pgm(FILE *outf, char *dir, char *pgm, int suid, uid_t dumpuid);

void
usage(void)
{
    error("Usage: amcheck%s [-am] [-w] [-sclt] [-M <address>] <conf> [host [disk]* ]* [-o configoption]*", versionsuffix());
    /*NOTREACHED*/
}

static unsigned long malloc_hist_1, malloc_size_1;
static unsigned long malloc_hist_2, malloc_size_2;

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static char *displayunit;
static long int unitdivisor;

int
main(
    int		argc,
    char **	argv)
{
    char buffer[BUFFER_SIZE];
    char *version_string;
    char *mainfname = NULL;
    char pid_str[NUM_STR_SIZE];
    int do_clientchk, client_probs;
    int do_localchk, do_tapechk, server_probs;
    pid_t clientchk_pid, serverchk_pid;
    int opt, tempfd, mainfd;
    ssize_t size;
    amwait_t retstat;
    pid_t pid;
    extern int optind;
    char *mailto = NULL;
    extern char *optarg;
    int mailout;
    int alwaysmail;
    char *tempfname = NULL;
    char *conffile;
    char *conf_diskfile;
    char *dumpuser;
    struct passwd *pw;
    uid_t uid_me;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;
    char *errstr;

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amcheck");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    memset(buffer, 0, sizeof(buffer));
    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    snprintf(pid_str, SIZEOF(pid_str), "%ld", (long)getpid());

    erroutput_type = ERR_INTERACTIVE;

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    if(geteuid() == 0) {
	seteuid(getuid());
    }
    uid_me = getuid();

    alwaysmail = mailout = overwrite = 0;
    do_localchk = do_tapechk = do_clientchk = 0;
    server_probs = client_probs = 0;
    tempfd = mainfd = -1;

    parse_server_conf(argc, argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    /* process arguments */

    while((opt = getopt(my_argc, my_argv, "M:mawsclt")) != EOF) {
	switch(opt) {
	case 'M':	mailto=stralloc(optarg);
			if(!validate_mailto(mailto)){
			   printf("Invalid characters in mail address\n");
			   exit(1);
			}
			/*FALLTHROUGH*/
	case 'm':	
#ifdef MAILER
			mailout = 1;
#else
			printf("You can't use -%c because configure didn't find a mailer.\n",
				opt);
			exit(1);
#endif
			break;
	case 'a':	
#ifdef MAILER
			mailout = 1;
			alwaysmail = 1;
#else
			printf("You can't use -%c because configure didn't find a mailer.\n",
				opt);
			exit(1);
#endif
			break;
	case 's':	do_localchk = do_clientchk = do_tapechk = 1;
			break;
	case 'c':	do_clientchk = 1;
			break;
	case 'l':	do_localchk = 1;
			break;
	case 'w':	overwrite = 1;
			break;
	case 't':	do_tapechk = 1;
			break;
	case '?':
	default:
	    usage();
	}
    }
    my_argc -= optind, my_argv += optind;
    if(my_argc < 1) usage();


    if ((do_localchk | do_clientchk | do_tapechk) == 0) {
	/* Check everything if individual checks were not asked for */
	do_localchk = do_clientchk = do_tapechk = 1;
    }

    if(overwrite)
	do_tapechk = 1;

    config_name = stralloc(*my_argv);

    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if(read_conffile(conffile)) {
	error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    amfree(conffile);
    if(mailout && !mailto && 
       (getconf_seen(CNF_MAILTO)==0 || strlen(getconf_str(CNF_MAILTO)) == 0)) {
	printf("\nNo mail address configured in  amanda.conf\n");
        if(alwaysmail)        
 		printf("When using -a option please specify -Maddress also\n\n"); 
	else
 		printf("Use -Maddress instead of -m\n\n"); 
	exit(1);
    }
    if(mailout && !mailto)
    { 
       if(getconf_seen(CNF_MAILTO) && 
          strlen(getconf_str(CNF_MAILTO)) > 0) {
          if(!validate_mailto(getconf_str(CNF_MAILTO))){
		printf("\nMail address in amanda.conf has invalid characters"); 
		printf("\nNo email will be sent\n"); 
                mailout = 0;
          }
       }
       else {
	  printf("\nNo mail address configured in  amanda.conf\n");
          if(alwaysmail)        
 		printf("When using -a option please specify -Maddress also\n\n"); 
	  else
 		printf("Use -Maddress instead of -m\n\n"); 
	  exit(1);
      }
    }

    conf_ctimeout = getconf_time(CNF_CTIMEOUT);

    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if(read_diskfile(conf_diskfile, &origq) < 0) {
	error("could not load disklist %s", conf_diskfile);
	/*NOTREACHED*/
    }
    errstr = match_disklist(&origq, my_argc-1, my_argv+1);
    if (errstr) {
	printf("%s",errstr);
	amfree(errstr);
    }
    amfree(conf_diskfile);

    /*
     * Make sure we are running as the dump user.
     */
    dumpuser = getconf_str(CNF_DUMPUSER);
    if ((pw = getpwnam(dumpuser)) == NULL) {
	error("cannot look up dump user \"%s\"", dumpuser);
	/*NOTREACHED*/
    }
    uid_dumpuser = pw->pw_uid;
    if ((pw = getpwuid(uid_me)) == NULL) {
	error("cannot look up my own uid (%ld)", (long)uid_me);
	/*NOTREACHED*/
    }
    if (uid_me != uid_dumpuser) {
	error("running as user \"%s\" instead of \"%s\"",
	      pw->pw_name, dumpuser);
        /*NOTREACHED*/
    }

    displayunit = getconf_str(CNF_DISPLAYUNIT);
    unitdivisor = getconf_unit_divisor();

    /*
     * If both server and client side checks are being done, the server
     * check output goes to the main output, while the client check output
     * goes to a temporary file and is copied to the main output when done.
     *
     * If the output is to be mailed, the main output is also a disk file,
     * otherwise it is stdout.
     */
    if(do_clientchk && (do_localchk || do_tapechk)) {
	/* we need the temp file */
	tempfname = vstralloc(AMANDA_TMPDIR, "/amcheck.temp.", pid_str, NULL);
	if((tempfd = open(tempfname, O_RDWR|O_CREAT|O_TRUNC, 0600)) == -1) {
	    error("could not open %s: %s", tempfname, strerror(errno));
	    /*NOTREACHED*/
	}
	unlink(tempfname);			/* so it goes away on close */
	amfree(tempfname);
    }

    if(mailout) {
	/* the main fd is a file too */
	mainfname = vstralloc(AMANDA_TMPDIR, "/amcheck.main.", pid_str, NULL);
	if((mainfd = open(mainfname, O_RDWR|O_CREAT|O_TRUNC, 0600)) == -1) {
	    error("could not open %s: %s", mainfname, strerror(errno));
	    /*NOTREACHED*/
	}
	unlink(mainfname);			/* so it goes away on close */
	amfree(mainfname);
    }
    else
	/* just use stdout */
	mainfd = 1;

    /* start server side checks */

    if(do_localchk || do_tapechk)
	serverchk_pid = start_server_check(mainfd, do_localchk, do_tapechk);
    else
	serverchk_pid = 0;

    /* start client side checks */

    if(do_clientchk) {
	clientchk_pid = start_client_checks((do_localchk || do_tapechk) ? tempfd : mainfd);
    } else {
	clientchk_pid = 0;
    }

    /* wait for child processes and note any problems */

    while(1) {
	if((pid = wait(&retstat)) == -1) {
	    if(errno == EINTR) continue;
	    else break;
	} else if(pid == clientchk_pid) {
	    client_probs = WIFSIGNALED(retstat) || WEXITSTATUS(retstat);
	    clientchk_pid = 0;
	} else if(pid == serverchk_pid) {
	    server_probs = WIFSIGNALED(retstat) || WEXITSTATUS(retstat);
	    serverchk_pid = 0;
	} else {
	    char number[NUM_STR_SIZE];
	    char *wait_msg = NULL;

	    snprintf(number, SIZEOF(number), "%ld", (long)pid);
	    wait_msg = vstralloc("parent: reaped bogus pid ", number, "\n",
				 NULL);
	    if (fullwrite(mainfd, wait_msg, strlen(wait_msg)) < 0) {
		error("write main file: %s", strerror(errno));
		/*NOTREACHED*/
	    }
	    amfree(wait_msg);
	}
    }

    /* copy temp output to main output and write tagline */

    if(do_clientchk && (do_localchk || do_tapechk)) {
	if(lseek(tempfd, (off_t)0, 0) == (off_t)-1) {
	    error("seek temp file: %s", strerror(errno));
	    /*NOTREACHED*/
	}

	while((size = fullread(tempfd, buffer, SIZEOF(buffer))) > 0) {
	    if (fullwrite(mainfd, buffer, (size_t)size) < 0) {
		error("write main file: %s", strerror(errno));
		/*NOTREACHED*/
	    }
	}
	if(size < 0) {
	    error("read temp file: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	aclose(tempfd);
    }

    version_string = vstralloc("\n",
			       "(brought to you by Amanda ", version(), ")\n",
			       NULL);
    if (fullwrite(mainfd, version_string, strlen(version_string)) < 0) {
	error("write main file: %s", strerror(errno));
	/*NOTREACHED*/
    }
    amfree(version_string);
    amfree(config_dir);
    amfree(config_name);
    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

    /* send mail if requested, but only if there were problems */
#ifdef MAILER

#define	MAILTO_LIMIT	10

    if((server_probs || client_probs || alwaysmail) && mailout) {
	int mailfd;
	int nullfd;
	int errfd;
	FILE *ferr;
	char *subject;
	char **a;
	amwait_t retstat;
	ssize_t r;
	ssize_t w;
	char *err = NULL;
	char *extra_info = NULL;
	char *line = NULL;
	int ret;
	int rc;
	int sig;
	char number[NUM_STR_SIZE];

	fflush(stdout);
	if(lseek(mainfd, (off_t)0, SEEK_SET) == (off_t)-1) {
	    error("lseek main file: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	if(alwaysmail && !(server_probs || client_probs)) {
	    subject = stralloc2(getconf_str(CNF_ORG),
			    " AMCHECK REPORT: NO PROBLEMS FOUND");
	} else {
	    subject = stralloc2(getconf_str(CNF_ORG),
			    " AMANDA PROBLEM: FIX BEFORE RUN, IF POSSIBLE");
	}
	/*
	 * Variable arg lists are hard to deal with when we do not know
	 * ourself how many args are involved.  Split the address list
	 * and hope there are not more than 9 entries.
	 *
	 * Remember that split() returns the original input string in
	 * argv[0], so we have to skip over that.
	 */
	a = (char **) alloc((MAILTO_LIMIT + 1) * SIZEOF(char *));
	memset(a, 0, (MAILTO_LIMIT + 1) * SIZEOF(char *));
	if(mailto) {
	    a[1] = mailto;
	    a[2] = NULL;
	} else {
	    r = (ssize_t)split(getconf_str(CNF_MAILTO), a, MAILTO_LIMIT, " ");
	    a[r + 1] = NULL;
	}
	if((nullfd = open("/dev/null", O_RDWR)) < 0) {
	    error("nullfd: /dev/null: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	pipespawn(MAILER, STDIN_PIPE | STDERR_PIPE,
			    &mailfd, &nullfd, &errfd,
			    MAILER,
			    "-s", subject,
			          a[1], a[2], a[3], a[4],
			    a[5], a[6], a[7], a[8], a[9],
			    NULL);
	amfree(subject);
	/*
	 * There is the potential for a deadlock here since we are writing
	 * to the process and then reading stderr, but in the normal case,
	 * nothing should be coming back to us, and hopefully in error
	 * cases, the pipe will break and we will exit out of the loop.
	 */
	signal(SIGPIPE, SIG_IGN);
	while((r = fullread(mainfd, buffer, SIZEOF(buffer))) > 0) {
	    if((w = fullwrite(mailfd, buffer, (size_t)r)) != (ssize_t)r) {
		if(w < 0 && errno == EPIPE) {
		    strappend(extra_info, "EPIPE writing to mail process\n");
		    break;
		} else if(w < 0) {
		    error("mailfd write: %s", strerror(errno));
		    /*NOTREACHED*/
		} else {
		    error("mailfd write: wrote %d instead of %d", w, r);
		    /*NOTREACHED*/
		}
	    }
	}
	aclose(mailfd);
	ferr = fdopen(errfd, "r");
	if (!ferr) {
	    error("Can't fdopen: %s", strerror(errno));
	    /*NOTREACHED*/
	}
	for(; (line = agets(ferr)) != NULL; free(line)) {
	    if (line[0] == '\0')
		continue;
	    strappend(extra_info, line);
	    strappend(extra_info, "\n");
	}
	afclose(ferr);
	errfd = -1;
	rc = 0;
	while (wait(&retstat) != -1) {
	    if (WIFSIGNALED(retstat)) {
		    ret = 0;
		    rc = sig = WTERMSIG(retstat);
	    } else {
		    sig = 0;
		    rc = ret = WEXITSTATUS(retstat);
	    }
	    if (rc != 0) {
		    if (ret == 0) {
			strappend(err, "got signal ");
			ret = sig;
		    } else {
			strappend(err, "returned ");
		    }
		    snprintf(number, SIZEOF(number), "%d", ret);
		    strappend(err, number);
	    }
	}
	if (rc != 0) {
	    if(extra_info) {
		fputs(extra_info, stderr);
		amfree(extra_info);
	    }
	    error("error running mailer %s: %s", MAILER, err);
	    /*NOTREACHED*/
	}
    }
#endif
    free_new_argv(new_argc, new_argv);
    free_server_config();

    dbclose();
    return (server_probs || client_probs);
}

/* --------------------------------------------------- */

int nslots, backwards, found, got_match, tapedays;
char *datestamp;
char *first_match_label = NULL, *first_match = NULL, *found_device = NULL;
char *label;
char *searchlabel, *labelstr;
tape_t *tp;
FILE *errf = NULL;

int
test_server_pgm(
    FILE *	outf,
    char *	dir,
    char *	pgm,
    int		suid,
    uid_t	dumpuid)
{
    struct stat statbuf;
    int pgmbad = 0;
    char *quoted;

    pgm = vstralloc(dir, "/", pgm, versionsuffix(), NULL);
    quoted = quote_string(pgm);
    if(stat(pgm, &statbuf) == -1) {
	fprintf(outf, "ERROR: program %s: does not exist\n",
		quoted);
	pgmbad = 1;
    } else if (!S_ISREG(statbuf.st_mode)) {
	fprintf(outf, "ERROR: program %s: not a file\n",
		quoted);
	pgmbad = 1;
    } else if (access(pgm, X_OK) == -1) {
	fprintf(outf, "ERROR: program %s: not executable\n",
		quoted);
	pgmbad = 1;
    } else if (suid \
	       && dumpuid != 0
	       && (statbuf.st_uid != 0 || (statbuf.st_mode & 04000) == 0)) {
	fprintf(outf, "ERROR: program %s: not setuid-root\n",
		quoted);
	pgmbad = 1;
    }
    amfree(quoted);
    amfree(pgm);
    return pgmbad;
}

pid_t
start_server_check(
    int		fd,
    int		do_localchk,
    int		do_tapechk)
{
    char *tapename;
    generic_fs_stats_t fs;
    FILE *outf = NULL;
    holdingdisk_t *hdp;
    pid_t pid;
    int confbad = 0, tapebad = 0, disklow = 0, logbad = 0;
    int userbad = 0, infobad = 0, indexbad = 0, pgmbad = 0;
    int testtape = do_tapechk;
    tapetype_t *tp = NULL;
    char *quoted;

    switch(pid = fork()) {
    case -1:
    	error("could not fork server check: %s", strerror(errno));
	/*NOTREACHED*/

    case 0:
    	break;

    default:
	return pid;
    }

    dup2(fd, 1);
    dup2(fd, 2);

    set_pname("amcheck-server");

    startclock();

    if((outf = fdopen(fd, "w")) == NULL) {
	error("fdopen %d: %s", fd, strerror(errno));
	/*NOTREACHED*/
    }
    errf = outf;

    fprintf(outf, "Amanda Tape Server Host Check\n");
    fprintf(outf, "-----------------------------\n");

    if (do_localchk || testtape) {
        tp = lookup_tapetype(getconf_str(CNF_TAPETYPE));
    }

    /*
     * Check various server side config file settings.
     */
    if(do_localchk) {
	char *ColumnSpec;
	char *errstr = NULL;
	char *lbl_templ;

	ColumnSpec = getconf_str(CNF_COLUMNSPEC);
	if(SetColumDataFromString(ColumnData, ColumnSpec, &errstr) < 0) {
	    fprintf(outf, "ERROR: %s\n", errstr);
	    amfree(errstr);
	    confbad = 1;
	}
	lbl_templ = tapetype_get_lbl_templ(tp);
	if(strcmp(lbl_templ, "") != 0) {
	    if(strchr(lbl_templ, '/') == NULL) {
		lbl_templ = stralloc2(config_dir, lbl_templ);
	    } else {
		lbl_templ = stralloc(lbl_templ);
	    }
	    if(access(lbl_templ, R_OK) == -1) {
		fprintf(outf,
			"ERROR: cannot access lbl_templ file %s: %s\n",
			lbl_templ,
			strerror(errno));
		confbad = 1;
	    }
#if !defined(LPRCMD)
	    fprintf(outf, "ERROR: lbl_templ set but no LPRCMD defined, you should reconfigure amanda\n       and make sure it find a lpr or lp command.\n");
	    confbad = 1;
#endif
	}

	/* check that localhost is resolvable */
	if ((gethostbyname("localhost")) == NULL) {
	    fprintf(outf, "ERROR: Cannot resolve `localhost'.\n");
	}
    }

    /*
     * Look up the programs used on the server side.
     */
    if(do_localchk) {
	/* 
	 * entreprise version will do planner/dumper suid check
	 */
	if(access(libexecdir, X_OK) == -1) {
	    quoted = quote_string(libexecdir);
	    fprintf(outf, "ERROR: program dir %s: not accessible\n",
		    quoted);
	    pgmbad = 1;
	    amfree(quoted);
	} else {
	    if(test_server_pgm(outf, libexecdir, "planner", 1, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, libexecdir, "dumper", 1, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, libexecdir, "driver", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, libexecdir, "taper", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, libexecdir, "amtrmidx", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, libexecdir, "amlogroll", 0, uid_dumpuser))
		pgmbad = 1;
	}
	if(access(sbindir, X_OK) == -1) {
	    quoted = quote_string(sbindir);
	    fprintf(outf, "ERROR: program dir %s: not accessible\n",
		    sbindir);
	    pgmbad = 1;
	    amfree(quoted);
	} else {
	    if(test_server_pgm(outf, sbindir, "amgetconf", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, sbindir, "amcheck", 1, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, sbindir, "amdump", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, sbindir, "amreport", 0, uid_dumpuser))
		pgmbad = 1;
	}
	if(access(COMPRESS_PATH, X_OK) == -1) {
	    quoted = quote_string(COMPRESS_PATH);
	    fprintf(outf, "WARNING: %s is not executable, server-compression and indexing will not work\n",
	            quoted);
	    amfree(quoted);
	}
    }

    /*
     * Check that the directory for the tapelist file is writable, as well
     * as the tapelist file itself (if it already exists).  Also, check for
     * a "hold" file (just because it is convenient to do it here) and warn
     * if tapedev is set to the null device.
     */

    if(do_localchk || do_tapechk) {
	char *conf_tapelist;
	char *tapefile;
	char *tape_dir;
	char *lastslash;
	char *holdfile;
	struct stat statbuf;
	
	conf_tapelist=getconf_str(CNF_TAPELIST);
	if (*conf_tapelist == '/') {
	    tapefile = stralloc(conf_tapelist);
	} else {
	    tapefile = stralloc2(config_dir, conf_tapelist);
	}
	/*
	 * XXX There Really Ought to be some error-checking here... dhw
	 */
	tape_dir = stralloc(tapefile);
	if ((lastslash = strrchr((const char *)tape_dir, '/')) != NULL) {
	    *lastslash = '\0';
	/*
	 * else whine Really Loudly about a path with no slashes??!?
	 */
	}
	if(access(tape_dir, W_OK) == -1) {
	    quoted = quote_string(tape_dir);
	    fprintf(outf, "ERROR: tapelist dir %s: not writable.\n", quoted);
	    tapebad = 1;
	    amfree(quoted);
	}
	else if(stat(tapefile, &statbuf) == -1) {
	    quoted = quote_string(tape_dir);
	    fprintf(outf, "ERROR: tapefile %s (%s), "
		    "you must create an empty file.\n",
		    quoted, strerror(errno));
	    tapebad = 1;
	    amfree(quoted);
	}
	else if(!S_ISREG(statbuf.st_mode)) {
	    quoted = quote_string(tapefile);
	    fprintf(outf, "ERROR: tapefile %s: should be a regular file.\n",
		    quoted);
	    tapebad = 1;
	    amfree(quoted);
	}
	else if(access(tapefile, F_OK) != 0) {
	    quoted = quote_string(tapefile);
	    fprintf(outf, "ERROR: can't access tape list %s\n", quoted);
	    tapebad = 1;
	    amfree(quoted);
	} else if(access(tapefile, F_OK) == 0 && access(tapefile, W_OK) != 0) {
	    quoted = quote_string(tapefile);
	    fprintf(outf, "ERROR: tape list %s: not writable\n", quoted);
	    tapebad = 1;
	    amfree(quoted);
	} else if(read_tapelist(tapefile)) {
	    quoted = quote_string(tapefile);
	    fprintf(outf, "ERROR: tape list %s: parse error\n", quoted);
	    tapebad = 1;
	    amfree(quoted);
	}
	holdfile = vstralloc(config_dir, "/", "hold", NULL);
	if(access(holdfile, F_OK) != -1) {
	    quoted = quote_string(holdfile);
	    fprintf(outf, "WARNING: hold file %s exists\n", holdfile);
	    amfree(quoted);
	}
	amfree(tapefile);
	amfree(tape_dir);
	amfree(holdfile);
	tapename = getconf_str(CNF_TAPEDEV);
	if (strncmp(tapename, "null:", 5) == 0) {
	    fprintf(outf,
		    "WARNING: tapedev is %s, dumps will be thrown away\n",
		    tapename);
	    testtape = 0;
	    do_tapechk = 0;
	}
    }

    /* check available disk space */

    if(do_localchk) {
	for(hdp = holdingdisks; hdp != NULL; hdp = hdp->next) {
    	    quoted = quote_string(holdingdisk_get_diskdir(hdp));
	    if(get_fs_stats(holdingdisk_get_diskdir(hdp), &fs) == -1) {
		fprintf(outf, "ERROR: holding dir %s (%s), "
			"you must create a directory.\n",
			quoted, strerror(errno));
		disklow = 1;
	    }
	    else if(access(holdingdisk_get_diskdir(hdp), W_OK) == -1) {
		fprintf(outf, "ERROR: holding disk %s: not writable: %s.\n",
			quoted, strerror(errno));
		disklow = 1;
	    }
	    else if(access(holdingdisk_get_diskdir(hdp), X_OK) == -1) {
		fprintf(outf, "ERROR: holding disk %s: not searcheable: %s.\n",
			quoted, strerror(errno));
		disklow = 1;
	    }
	    else if(fs.avail == (off_t)-1) {
		fprintf(outf,
			"WARNING: holding disk %s: "
			"available space unknown (" OFF_T_FMT" KB requested)\n",
			quoted, (OFF_T_FMT_TYPE)holdingdisk_get_disksize(hdp));
		disklow = 1;
	    }
	    else if(holdingdisk_get_disksize(hdp) > (off_t)0) {
		if(fs.avail < holdingdisk_get_disksize(hdp)) {
		    fprintf(outf,
			    "WARNING: holding disk %s: "
			    "only " OFF_T_FMT " %sB free ("
			    OFF_T_FMT " %sB requested)\n", quoted,
			    (OFF_T_FMT_TYPE)(fs.avail / (off_t)unitdivisor),
			    displayunit,
			    (OFF_T_FMT_TYPE)(holdingdisk_get_disksize(hdp)/(off_t)unitdivisor),
			    displayunit);
		    disklow = 1;
		}
		else {
		    fprintf(outf,
			    "Holding disk %s: " OFF_T_FMT
			    " %sB disk space available, that's plenty\n",
			    quoted, (OFF_T_FMT_TYPE)(fs.avail/(off_t)unitdivisor),
			    displayunit);
		}
	    }
	    else {
		assert(holdingdisk_get_disksize(hdp) < (off_t)0);
		if((fs.avail + holdingdisk_get_disksize(hdp)) <= (off_t)0) {
		    fprintf(outf,
			    "WARNING: holding disk %s: "
			    "only " OFF_T_FMT " %sB free, using nothing\n",
			    quoted, (OFF_T_FMT_TYPE)(fs.avail/(off_t)unitdivisor),
			    displayunit);
		    disklow = 1;
		}
		else {
		    fprintf(outf,
			    "Holding disk %s: "
			    OFF_T_FMT " %sB disk space available, using "
			    OFF_T_FMT " %sB\n",
			    quoted,
			    (OFF_T_FMT_TYPE)(fs.avail/(off_t)unitdivisor),
			    displayunit,
			    (OFF_T_FMT_TYPE)(fs.avail + holdingdisk_get_disksize(hdp) / (off_t)unitdivisor),
			    displayunit);
		}
	    }
	    amfree(quoted);
	}
    }

    /* check that the log file is writable if it already exists */

    if(do_localchk) {
	char *conf_logdir;
	char *logfile;
	char *olddir;
	struct stat stat_old;
	struct stat statbuf;

	conf_logdir = getconf_str(CNF_LOGDIR);
	if (*conf_logdir == '/') {
	    conf_logdir = stralloc(conf_logdir);
	} else {
	    conf_logdir = stralloc2(config_dir, conf_logdir);
	}
	logfile = vstralloc(conf_logdir, "/log", NULL);

	quoted = quote_string(conf_logdir);
	if(stat(conf_logdir, &statbuf) == -1) {
	    fprintf(outf, "ERROR: logdir %s (%s), you must create directory.\n",
		    quoted, strerror(errno));
	    disklow = 1;
	}
	else if(access(conf_logdir, W_OK) == -1) {
	    fprintf(outf, "ERROR: log dir %s: not writable\n", quoted);
	    logbad = 1;
	}
	amfree(quoted);

	if(access(logfile, F_OK) == 0) {
	    testtape = 0;
	    logbad = 1;
	    if(access(logfile, W_OK) != 0) {
		quoted = quote_string(logfile);
		fprintf(outf, "ERROR: log file %s: not writable\n", quoted);
		amfree(quoted);
	    }
	}

	olddir = vstralloc(conf_logdir, "/oldlog", NULL);
	quoted = quote_string(olddir);
	if (stat(olddir,&stat_old) == 0) { /* oldlog exist */
	    if(!(S_ISDIR(stat_old.st_mode))) {
		fprintf(outf, "ERROR: oldlog directory %s is not a directory\n",
			quoted);
	    }
	    if(access(olddir, W_OK) == -1) {
		fprintf(outf, "ERROR: oldlog dir %s: not writable\n", quoted);
	    }
	}
	else if(lstat(olddir,&stat_old) == 0) {
	    fprintf(outf, "ERROR: oldlog directory %s is not a directory\n",
		    quoted);
	}
	amfree(quoted);

	if (testtape) {
	    logfile = newvstralloc(logfile, conf_logdir, "/amdump", NULL);
	    if (access(logfile, F_OK) == 0) {
		testtape = 0;
		logbad = 1;
	    }
	}

	amfree(olddir);
	amfree(logfile);
	amfree(conf_logdir);
    }

    if (testtape) {
	/* check that the tape is a valid amanda tape */
        int tape_status;

	tapedays = getconf_int(CNF_TAPECYCLE);
	labelstr = getconf_str(CNF_LABELSTR);
	tapename = getconf_str(CNF_TAPEDEV);

	if (!getconf_seen(CNF_TPCHANGER) && getconf_int(CNF_RUNTAPES) != 1) {
	    fprintf(outf,
		    "WARNING: if a tape changer is not available, runtapes must be set to 1\n");
	}

        tape_status = taper_scan(NULL, &label, &datestamp, &tapename,
				 FILE_taperscan_output_callback, outf);
        if (tape_status < 0) {
	    tape_t *exptape = lookup_last_reusable_tape(0);
	    fprintf(outf, "       (expecting ");
	    if(exptape != NULL) fprintf(outf, "tape %s or ", exptape->label);
	    fprintf(outf, "a new tape)\n");
            tapebad = 1;
	} else {
            if (overwrite) {
                char *wrlabel_status;
                wrlabel_status = tape_wrlabel(tapename, "X", label,
				(unsigned)(tapetype_get_blocksize(tp) * 1024));
                if (wrlabel_status != NULL) {
                    if (tape_status == 3) {
                        fprintf(outf,
                                "ERROR: Could not label brand new tape: %s\n",
                                wrlabel_status);
                    } else {
                        fprintf(outf,
                                "ERROR: tape %s label ok, but is not writable (%s)\n",
                                label, wrlabel_status);
                    }
                    tapebad = 1;
                } else {
                    if (tape_status != 3) {
                        fprintf(outf, "Tape %s is writable; rewrote label.\n", label);
                    } else {
                        fprintf(outf, "Wrote label %s to brand new tape.\n", label);
                    }
                }
            } else {
                fprintf(outf, "NOTE: skipping tape-writable test\n");
                if (tape_status == 3) {
                    fprintf(outf,
                            "Found a brand new tape, will label it %s.\n", 
                            label);
                } else {
                    fprintf(outf, "Tape %s label ok\n", label);
                }                    
            }
        }
	amfree(tapename);
    } else if (do_tapechk) {
	fprintf(outf, "WARNING: skipping tape test because amdump or amflush seem to be running\n");
	fprintf(outf, "WARNING: if they are not, you must run amcleanup\n");
    } else {
	fprintf(outf, "NOTE: skipping tape checks\n");
    }

    /*
     * See if the information file and index directory for each client
     * and disk is OK.  Since we may be seeing clients and/or disks for
     * the first time, these are just warnings, not errors.
     */
    if(do_localchk) {
	char *conf_infofile;
	char *conf_indexdir;
	char *hostinfodir = NULL;
	char *hostindexdir = NULL;
	char *diskdir = NULL;
	char *infofile = NULL;
	struct stat statbuf;
	disk_t *dp;
	am_host_t *hostp;
	int indexdir_checked = 0;
	int hostindexdir_checked = 0;
	char *host;
	char *disk;
	int conf_tapecycle, conf_runspercycle;

	conf_tapecycle = getconf_int(CNF_TAPECYCLE);
	conf_runspercycle = getconf_int(CNF_RUNSPERCYCLE);

	if(conf_tapecycle <= conf_runspercycle) {
		fprintf(outf, "WARNING: tapecycle (%d) <= runspercycle (%d).\n",
			conf_tapecycle, conf_runspercycle);
	}

	conf_infofile = getconf_str(CNF_INFOFILE);
	if (*conf_infofile == '/') {
	    conf_infofile = stralloc(conf_infofile);
	} else {
	    conf_infofile = stralloc2(config_dir, conf_infofile);
	}

	conf_indexdir = getconf_str(CNF_INDEXDIR);
	if (*conf_indexdir == '/') {
	    conf_indexdir = stralloc(conf_indexdir);
	} else {
	    conf_indexdir = stralloc2(config_dir, conf_indexdir);
	}

#if TEXTDB
	quoted = quote_string(conf_infofile);
	if(stat(conf_infofile, &statbuf) == -1) {
	    if (errno == ENOENT) {
		fprintf(outf, "NOTE: conf info dir %s does not exist\n",
			quoted);
		fprintf(outf, "NOTE: it will be created on the next run.\n");
	    } else {
		fprintf(outf, "ERROR: conf info dir %s (%s)\n",
			quoted, strerror(errno));
	    }	
	    amfree(conf_infofile);
	} else if (!S_ISDIR(statbuf.st_mode)) {
	    fprintf(outf, "ERROR: info dir %s: not a directory\n", quoted);
	    amfree(conf_infofile);
	    infobad = 1;
	} else if (access(conf_infofile, W_OK) == -1) {
	    fprintf(outf, "ERROR: info dir %s: not writable\n", quoted);
	    amfree(conf_infofile);
	    infobad = 1;
	} else {
	    strappend(conf_infofile, "/");
	}
	amfree(quoted);
#endif

	while(!empty(origq)) {
	    hostp = origq.head->host;
	    host = sanitise_filename(hostp->hostname);
#if TEXTDB
	    if(conf_infofile) {
		hostinfodir = newstralloc2(hostinfodir, conf_infofile, host);
		quoted = quote_string(hostinfodir);
		if(stat(hostinfodir, &statbuf) == -1) {
		    if (errno == ENOENT) {
			fprintf(outf, "NOTE: host info dir %s does not exist\n",
				quoted);
			fprintf(outf,
				"NOTE: it will be created on the next run.\n");
		    } else {
			fprintf(outf, "ERROR: host info dir %s (%s)\n",
				quoted, strerror(errno));
		    }	
		    amfree(hostinfodir);
		} else if (!S_ISDIR(statbuf.st_mode)) {
		    fprintf(outf, "ERROR: info dir %s: not a directory\n",
			    quoted);
		    amfree(hostinfodir);
		    infobad = 1;
		} else if (access(hostinfodir, W_OK) == -1) {
		    fprintf(outf, "ERROR: info dir %s: not writable\n", quoted);
		    amfree(hostinfodir);
		    infobad = 1;
		} else {
		    strappend(hostinfodir, "/");
		}
		amfree(quoted);
	    }
#endif
	    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
		disk = sanitise_filename(dp->name);
#if TEXTDB
		if(hostinfodir) {
		    char *quotedif;

		    diskdir = newstralloc2(diskdir, hostinfodir, disk);
		    infofile = vstralloc(diskdir, "/", "info", NULL);
		    quoted = quote_string(diskdir);
		    quotedif = quote_string(infofile);
		    if(stat(diskdir, &statbuf) == -1) {
			if (errno == ENOENT) {
			    fprintf(outf, "NOTE: info dir %s does not exist\n",
				quoted);
			    fprintf(outf,
				"NOTE: it will be created on the next run.\n");
			} else {
			    fprintf(outf, "ERROR: info dir %s (%s)\n",
				    quoted, strerror(errno));
			}	
		    } else if (!S_ISDIR(statbuf.st_mode)) {
			fprintf(outf, "ERROR: info dir %s: not a directory\n",
				quoted);
			infobad = 1;
		    } else if (access(diskdir, W_OK) == -1) {
			fprintf(outf, "ERROR: info dir %s: not writable\n",
				quoted);
			infobad = 1;
		    } else if(stat(infofile, &statbuf) == -1) {
			if (errno == ENOENT) {
			    fprintf(outf, "NOTE: info file %s does not exist\n",
				    quotedif);
			    fprintf(outf, "NOTE: it will be created on the next run.\n");
			} else {
			    fprintf(outf, "ERROR: info dir %s (%s)\n",
				quoted, strerror(errno));
			}	
		    } else if (!S_ISREG(statbuf.st_mode)) {
			fprintf(outf, "ERROR: info file %s: not a file\n",
				quotedif);
			infobad = 1;
		    } else if (access(infofile, R_OK) == -1) {
			fprintf(outf, "ERROR: info file %s: not readable\n",
				quotedif);
			infobad = 1;
		    }
		    amfree(quotedif);
		    amfree(quoted);
		    amfree(infofile);
		}
#endif
		if(dp->index) {
		    if(! indexdir_checked) {
			quoted = quote_string(conf_indexdir);
			if(stat(conf_indexdir, &statbuf) == -1) {
			    if (errno == ENOENT) {
				fprintf(outf, "NOTE: index dir %s does not exist\n",
				    quoted);
				fprintf(outf, "NOTE: it will be created on the next run.\n");
			    } else {
				fprintf(outf, "ERROR: index dir %s (%s)\n",
					quoted, strerror(errno));
			    }	
			    amfree(conf_indexdir);
			} else if (!S_ISDIR(statbuf.st_mode)) {
			    fprintf(outf, "ERROR: index dir %s: not a directory\n",
				    quoted);
			    amfree(conf_indexdir);
			    indexbad = 1;
			} else if (access(conf_indexdir, W_OK) == -1) {
			    fprintf(outf, "ERROR: index dir %s: not writable\n",
				    quoted);
			    amfree(conf_indexdir);
			    indexbad = 1;
			} else {
			    strappend(conf_indexdir, "/");
			}
			indexdir_checked = 1;
			amfree(quoted);
		    }
		    if(conf_indexdir) {
			if(! hostindexdir_checked) {
			    hostindexdir = stralloc2(conf_indexdir, host);
			    quoted = quote_string(hostindexdir);
			    if(stat(hostindexdir, &statbuf) == -1) {
				if (errno == ENOENT) {
				    fprintf(outf, "NOTE: index dir %s does not exist\n",
				        quoted);
				    fprintf(outf, "NOTE: it will be created on the next run.\n");
			        } else {
				    fprintf(outf, "ERROR: index dir %s (%s)\n",
					    quoted, strerror(errno));
				}
			        amfree(hostindexdir);
			    } else if (!S_ISDIR(statbuf.st_mode)) {
			        fprintf(outf, "ERROR: index dir %s: not a directory\n",
				        quoted);
			        amfree(hostindexdir);
			        indexbad = 1;
			    } else if (access(hostindexdir, W_OK) == -1) {
			        fprintf(outf, "ERROR: index dir %s: not writable\n",
				        quoted);
			        amfree(hostindexdir);
			        indexbad = 1;
			    } else {
				strappend(hostindexdir, "/");
			    }
			    hostindexdir_checked = 1;
			    amfree(quoted);
			}
			if(hostindexdir) {
			    diskdir = newstralloc2(diskdir, hostindexdir, disk);
			    quoted = quote_string(diskdir);
			    if(stat(diskdir, &statbuf) == -1) {
				if (errno == ENOENT) {
				    fprintf(outf, "NOTE: index dir %s does not exist\n",
					quoted);
				    fprintf(outf, "NOTE: it will be created on the next run.\n");
				} else {
				    fprintf(outf, "ERROR: index dir %s (%s)\n",
					quoted, strerror(errno));
				}	
			    } else if (!S_ISDIR(statbuf.st_mode)) {
				fprintf(outf, "ERROR: index dir %s: not a directory\n",
					quoted);
				indexbad = 1;
			    } else if (access(diskdir, W_OK) == -1) {
				fprintf(outf, "ERROR: index dir %s: is not writable\n",
					quoted);
				indexbad = 1;
			    }
			    amfree(quoted);
			}
		    }
		}

		if ( dp->encrypt == ENCRYPT_SERV_CUST ) {
		  if ( dp->srv_encrypt[0] == '\0' ) {
		    fprintf(outf, "ERROR: server encryption program not specified\n");
		    pgmbad = 1;
		  }
		  else if(access(dp->srv_encrypt, X_OK) == -1) {
		    fprintf(outf, "ERROR: %s is not executable, server encryption will not work\n",
			    dp->srv_encrypt );
		    pgmbad = 1;
		  }
		}
		if ( dp->compress == COMP_SERV_CUST ) {
		  if ( dp->srvcompprog[0] == '\0' ) {
		    fprintf(outf, "ERROR: server custom compression program not specified\n");
		    pgmbad = 1;
		  }
		  else if(access(dp->srvcompprog, X_OK) == -1) {
		    quoted = quote_string(dp->srvcompprog);

		    fprintf(outf, "ERROR: %s is not executable, server custom compression will not work\n",
			    quoted);
		    amfree(quoted);
		    pgmbad = 1;
		  }
		}

		amfree(disk);
		remove_disk(&origq, dp);
	    }
	    amfree(host);
	    amfree(hostindexdir);
	    hostindexdir_checked = 0;
	}
	amfree(diskdir);
	amfree(hostinfodir);
	amfree(conf_infofile);
	amfree(conf_indexdir);
    }

    amfree(datestamp);
    amfree(label);
    amfree(config_dir);
    amfree(config_name);

    fprintf(outf, "Server check took %s seconds\n", walltime_str(curclock()));

    fflush(outf);

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fd, malloc_hist_1, malloc_hist_2);
    }

    exit(userbad \
	 || confbad \
	 || tapebad \
	 || disklow \
	 || logbad \
	 || infobad \
	 || indexbad \
	 || pgmbad);
    /*NOTREACHED*/
    return 0;
}

/* --------------------------------------------------- */

int remote_errors;
FILE *outf;

static void handle_result(void *, pkt_t *, security_handle_t *);
void start_host(am_host_t *hostp);

#define HOST_READY				((void *)0)	/* must be 0 */
#define HOST_ACTIVE				((void *)1)
#define HOST_DONE				((void *)2)

#define DISK_READY				((void *)0)	/* must be 0 */
#define DISK_ACTIVE				((void *)1)
#define DISK_DONE				((void *)2)

void
start_host(
    am_host_t *hostp)
{
    disk_t *dp;
    char *req = NULL;
    size_t req_len = 0;
    int disk_count;
    const security_driver_t *secdrv;
    char number[NUM_STR_SIZE];

    if(hostp->up != HOST_READY) {
	return;
    }

    if (strncmp (hostp->hostname,"localhost",9) == 0) {
	fprintf(outf,
                    "WARNING: Usage of fully qualified hostname recommended for Client %s.\n",
                    hostp->hostname);
    }

    /*
     * The first time through here we send a "noop" request.  This will
     * return the feature list from the client if it supports that.
     * If it does not, handle_result() will set the feature list to an
     * empty structure.  In either case, we do the disks on the second
     * (and subsequent) pass(es).
     */
    disk_count = 0;
    if(hostp->features != NULL) { /* selfcheck service */
	int has_features = am_has_feature(hostp->features,
					  fe_req_options_features);
	int has_hostname = am_has_feature(hostp->features,
					  fe_req_options_hostname);
	int has_maxdumps = am_has_feature(hostp->features,
					  fe_req_options_maxdumps);
	int has_config   = am_has_feature(hostp->features,
					  fe_req_options_config);

	if(!am_has_feature(hostp->features, fe_selfcheck_req) &&
	   !am_has_feature(hostp->features, fe_selfcheck_req_device)) {
	    fprintf(outf,
		    "ERROR: Client %s does not support selfcheck REQ packet.\n",
		    hostp->hostname);
	}
	if(!am_has_feature(hostp->features, fe_selfcheck_rep)) {
	    fprintf(outf,
		    "ERROR: Client %s does not support selfcheck REP packet.\n",
		    hostp->hostname);
	}
	if(!am_has_feature(hostp->features, fe_sendsize_req_options) &&
	   !am_has_feature(hostp->features, fe_sendsize_req_no_options) &&
	   !am_has_feature(hostp->features, fe_sendsize_req_device)) {
	    fprintf(outf,
		    "ERROR: Client %s does not support sendsize REQ packet.\n",
		    hostp->hostname);
	}
	if(!am_has_feature(hostp->features, fe_sendsize_rep)) {
	    fprintf(outf,
		    "ERROR: Client %s does not support sendsize REP packet.\n",
		    hostp->hostname);
	}
	if(!am_has_feature(hostp->features, fe_sendbackup_req) &&
	   !am_has_feature(hostp->features, fe_sendbackup_req_device)) {
	    fprintf(outf,
		   "ERROR: Client %s does not support sendbackup REQ packet.\n",
		   hostp->hostname);
	}
	if(!am_has_feature(hostp->features, fe_sendbackup_rep)) {
	    fprintf(outf,
		   "ERROR: Client %s does not support sendbackup REP packet.\n",
		   hostp->hostname);
	}

	snprintf(number, SIZEOF(number), "%d", hostp->maxdumps);
	req = vstralloc("SERVICE ", "selfcheck", "\n",
			"OPTIONS ",
			has_features ? "features=" : "",
			has_features ? our_feature_string : "",
			has_features ? ";" : "",
			has_maxdumps ? "maxdumps=" : "",
			has_maxdumps ? number : "",
			has_maxdumps ? ";" : "",
			has_hostname ? "hostname=" : "",
			has_hostname ? hostp->hostname : "",
			has_hostname ? ";" : "",
			has_config   ? "config=" : "",
			has_config   ? config_name : "",
			has_config   ? ";" : "",
			"\n",
			NULL);

	req_len = strlen(req);
	req_len += 128;                         /* room for SECURITY ... */
	req_len += 256;                         /* room for non-disk answers */
	for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	    char *l;
	    size_t l_len;
	    char *o;
	    char *calcsize;
	    char *qname;
	    char *qdevice;

	    if(dp->up != DISK_READY || dp->todo != 1) {
		continue;
	    }
	    o = optionstr(dp, hostp->features, outf);
	    if (o == NULL) {
	        remote_errors++;
		continue;
	    }
	    qname = quote_string(dp->name); 
	    qdevice = quote_string(dp->device); 
	    if ((dp->name && qname[0] == '"') || 
		(dp->device && qdevice[0] == '"')) {
		if(!am_has_feature(hostp->features, fe_interface_quoted_text)) {
		    fprintf(outf,
			    "WARNING: %s:%s:%s host does not support quoted text\n",
			    hostp->hostname, qname, qdevice);
		}
	    }

	    if(dp->device) {
		if(!am_has_feature(hostp->features, fe_selfcheck_req_device)) {
		    fprintf(outf,
		     "ERROR: %s:%s (%s): selfcheck does not support device.\n",
		     hostp->hostname, qname, dp->device);
		}
		if(!am_has_feature(hostp->features, fe_sendsize_req_device)) {
		    fprintf(outf,
		     "ERROR: %s:%s (%s): sendsize does not support device.\n",
		     hostp->hostname, qname, dp->device);
		}
		if(!am_has_feature(hostp->features, fe_sendbackup_req_device)) {
		    fprintf(outf,
		     "ERROR: %s:%s (%s): sendbackup does not support device.\n",
		     hostp->hostname, qname, dp->device);
		}
	    }
	    if(strncmp(dp->program,"DUMP",4) == 0 || 
	       strncmp(dp->program,"GNUTAR",6) == 0) {
		if(strcmp(dp->program, "DUMP") == 0 &&
		   !am_has_feature(hostp->features, fe_program_dump)) {
		    fprintf(outf, "ERROR: %s:%s does not support DUMP.\n",
			    hostp->hostname, qname);
		}
		if(strcmp(dp->program, "GNUTAR") == 0 &&
		   !am_has_feature(hostp->features, fe_program_gnutar)) {
		    fprintf(outf, "ERROR: %s:%s does not support GNUTAR.\n",
			    hostp->hostname, qname);
		}
		if(dp->estimate == ES_CALCSIZE &&
		   !am_has_feature(hostp->features, fe_calcsize_estimate)) {
		    fprintf(outf, "ERROR: %s:%s does not support CALCSIZE for estimate, using CLIENT.\n",
			    hostp->hostname, qname);
		    dp->estimate = ES_CLIENT;
		}
		if(dp->estimate == ES_CALCSIZE &&
		   am_has_feature(hostp->features, fe_selfcheck_calcsize))
		    calcsize = "CALCSIZE ";
		else
		    calcsize = "";

		if(dp->compress == COMP_CUST &&
		   !am_has_feature(hostp->features, fe_options_compress_cust)) {
		  fprintf(outf,
			  "ERROR: Client %s does not support custom compression.\n",
			  hostp->hostname);
		}
		if(dp->encrypt == ENCRYPT_CUST ) {
		  if ( !am_has_feature(hostp->features, fe_options_encrypt_cust)) {
		    fprintf(outf,
			    "ERROR: Client %s does not support data encryption.\n",
			    hostp->hostname);
		    remote_errors++;
		  } else if ( dp->compress == COMP_SERV_FAST || 
			      dp->compress == COMP_SERV_BEST ||
			      dp->compress == COMP_SERV_CUST ) {
		    fprintf(outf,
			    "ERROR: %s: Client encryption with server compression is not supported. See amanda.conf(5) for detail.\n", hostp->hostname);
		    remote_errors++;
		  } 
		}
		if(dp->device) {
		    l = vstralloc(calcsize,
				  dp->program, " ",
				  qname, " ",
				  dp->device,
				  " 0 OPTIONS |",
				  o,
				  "\n",
				  NULL);
		}
		else {
		    l = vstralloc(calcsize,
				  dp->program, " ",
				  qname,
				  " 0 OPTIONS |",
				  o,
				  "\n",
				  NULL);
		}
	    } else {
		if(!am_has_feature(hostp->features, fe_program_dumper_api)) {
		    fprintf(outf, "ERROR: %s:%s does not support DUMPER-API.\n",
			    hostp->hostname, qname);
		}
		l = vstralloc("DUMPER ",
			      dp->program, 
			      " ",
			      qname,
			      " ",
			      dp->device,
			      " 0 OPTIONS |",
			      o,
			      "\n",
			      NULL);
	    }
	    amfree(qname);
	    amfree(qdevice);
	    l_len = strlen(l);
	    amfree(o);

	    strappend(req, l);
	    req_len += l_len;
	    amfree(l);
	    dp->up = DISK_ACTIVE;
	    disk_count++;
	}
    }
    else { /* noop service */
	req = vstralloc("SERVICE ", "noop", "\n",
			"OPTIONS ",
			"features=", our_feature_string, ";",
			"\n",
			NULL);
	for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	    if(dp->up != DISK_READY || dp->todo != 1) {
		continue;
	    }
	    disk_count++;
	}
    }

    if(disk_count == 0) {
	amfree(req);
	hostp->up = HOST_DONE;
	return;
    }

    secdrv = security_getdriver(hostp->disks->security_driver);
    if (secdrv == NULL) {
	error("could not find security driver '%s' for host '%s'",
	      hostp->disks->security_driver, hostp->hostname);
        /*NOTREACHED*/
    }
    protocol_sendreq(hostp->hostname, secdrv, amhost_get_security_conf, 
		     req, conf_ctimeout, handle_result, hostp);

    amfree(req);

    hostp->up = HOST_ACTIVE;
}

pid_t
start_client_checks(
    int		fd)
{
    am_host_t *hostp;
    disk_t *dp;
    int hostcount;
    pid_t pid;
    int userbad = 0;

    switch(pid = fork()) {
    case -1:
    	error("could not fork client check: %s", strerror(errno));
	/*NOTREACHED*/

    case 0:
    	break;

    default:
	return pid;
    }

    dup2(fd, 1);
    dup2(fd, 2);

    set_pname("amcheck-clients");

    startclock();

    if((outf = fdopen(fd, "w")) == NULL) {
	error("fdopen %d: %s", fd, strerror(errno));
	/*NOTREACHED*/
    }
    errf = outf;

    fprintf(outf, "\nAmanda Backup Client Hosts Check\n");
    fprintf(outf,   "--------------------------------\n");

    protocol_init();

    hostcount = remote_errors = 0;

    for(dp = origq.head; dp != NULL; dp = dp->next) {
	hostp = dp->host;
	if(hostp->up == HOST_READY && dp->todo == 1) {
	    start_host(hostp);
	    hostcount++;
	    protocol_check();
	}
    }

    protocol_run();

    fprintf(outf,
     "Client check: %d host%s checked in %s seconds, %d problem%s found\n",
	    hostcount, (hostcount == 1) ? "" : "s",
	    walltime_str(curclock()),
	    remote_errors, (remote_errors == 1) ? "" : "s");
    fflush(outf);

    amfree(config_dir);
    amfree(config_name);

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fd, malloc_hist_1, malloc_hist_2);
    }

    exit(userbad || remote_errors > 0);
    /*NOTREACHED*/
    return 0;
}

static void
handle_result(
    void *		datap,
    pkt_t *		pkt,
    security_handle_t *	sech)
{
    am_host_t *hostp;
    disk_t *dp;
    char *line;
    char *s;
    char *t;
    int ch;
    int tch;

    hostp = (am_host_t *)datap;
    hostp->up = HOST_READY;

    if (pkt == NULL) {
	fprintf(outf,
	    "WARNING: %s: selfcheck request failed: %s\n", hostp->hostname,
	    security_geterror(sech));
	remote_errors++;
	hostp->up = HOST_DONE;
	return;
    }

#if 0
    fprintf(errf, "got response from %s:\n----\n%s----\n\n",
	    hostp->hostname, pkt->body);
#endif

    s = pkt->body;
    ch = *s++;
    while(ch) {
	line = s - 1;
	skip_quoted_line(s, ch);
	if (s[-2] == '\n') {
	    s[-2] = '\0';
	}

#define sc "OPTIONS "
	if(strncmp(line, sc, SIZEOF(sc)-1) == 0) {
#undef sc

#define sc "features="
	    t = strstr(line, sc);
	    if(t != NULL && (isspace((int)t[-1]) || t[-1] == ';')) {
		t += SIZEOF(sc)-1;
#undef sc
		am_release_feature_set(hostp->features);
		if((hostp->features = am_string_to_feature(t)) == NULL) {
		    fprintf(outf, "ERROR: %s: bad features value: %s\n",
			    hostp->hostname, line);
		}
	    }

	    continue;
	}

#define sc "OK "
	if(strncmp(line, sc, SIZEOF(sc)-1) == 0) {
	    continue;
#undef sc
	}

#define sc "ERROR "
	if(strncmp(line, sc, SIZEOF(sc)-1) == 0) {
	    t = line + SIZEOF(sc) - 1;
	    tch = t[-1];
#undef sc

	    skip_whitespace(t, tch);
	    /*
	     * If the "error" is that the "noop" service is unknown, it
	     * just means the client is "old" (does not support the service).
	     * We can ignore this.
	     */
	    if(!((hostp->features == NULL) && (pkt->type == P_NAK)
	       && ((strcmp(t - 1, "unknown service: noop") == 0)
		   || (strcmp(t - 1, "noop: invalid service") == 0)))) {
		fprintf(outf, "ERROR: %s%s: %s\n",
			(pkt->type == P_NAK) ? "NAK " : "",
			hostp->hostname,
			t - 1);
		remote_errors++;
		hostp->up = HOST_DONE;
	    }
	    continue;
	}

	fprintf(outf, "ERROR: %s: unknown response: %s\n",
		hostp->hostname, line);
	remote_errors++;
	hostp->up = HOST_DONE;
    }
    if(hostp->up == HOST_READY && hostp->features == NULL) {
	/*
	 * The client does not support the features list, so give it an
	 * empty one.
	 */
	dbprintf(("%s: no feature set from host %s\n",
		  debug_prefix_time(NULL), hostp->hostname));
	hostp->features = am_set_default_feature_set();
    }
    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	if(dp->up == DISK_ACTIVE) {
	    dp->up = DISK_DONE;
	}
    }
    start_host(hostp);
    if(hostp->up == HOST_DONE)
	security_close_connection(sech, hostp->hostname);
}
