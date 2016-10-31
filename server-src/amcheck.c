/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * $Id: amcheck.c,v 1.149 2006/08/24 01:57:16 paddy_s Exp $
 *
 * checks for common problems in server and clients
 */
#include "amanda.h"
#include "amutil.h"
#include "conffile.h"
#include "fsusage.h"
#include "diskfile.h"
#include "tapefile.h"
#include "packet.h"
#include "security.h"
#include "protocol.h"
#include "clock.h"
#include "amindex.h"
#include "server_util.h"
#include "pipespawn.h"
#include "amfeatures.h"
#include "device.h"
#include "property.h"
#include "timestamp.h"
#include "amxml.h"
#include "ammessage.h"
#include "physmem.h"
#include <getopt.h>

#define BUFFER_SIZE	32768

static time_t conf_ctimeout;
static int overwrite;

static disklist_t origq;

static uid_t uid_dumpuser;
static gboolean who_check_host_setting = TRUE;//  TRUE =>  local check auth
	                                      // FALSE => client check auth

/* local functions */

void amcheck_exit(int status);
void usage(void);
pid_t start_client_checks(FILE *fd);
pid_t start_server_check(FILE *fd, int do_localchk, int do_tapechk);
int main(int argc, char **argv);
int check_tapefile(FILE *outf, char *tapefile);
int test_server_pgm(FILE *outf, char *dir, char *pgm, int suid, uid_t dumpuid);
static int check_host_setting(FILE *outf);

static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static char *displayunit;
static long int unitdivisor;
static gboolean dev_amanda_data_path = TRUE;
static gboolean dev_directtcp_data_path = TRUE;

static int client_verbose = FALSE;
static gboolean exact_match = FALSE;
static gboolean opt_message = FALSE;
static struct option long_options[] = {
    {"client-verbose", 0, NULL,  1},
    {"version"       , 0, NULL,  2},
    {"exact-match"   , 0, NULL,  3},
    {"message"       , 0, NULL,  4},
    {NULL, 0, NULL, 0}
};

static message_t *
amcheck_fprint_message(
    FILE      *file,
    message_t *message);

static message_t *
amcheck_print_message(
    message_t *message)
{
    return amcheck_fprint_message(stdout, message);
}

static message_t *
amcheck_fprint_message(
    FILE      *file,
    message_t *message)
{
    char *hint;

    if (opt_message) {
	fprint_message(file, message);
    } else {
	char *prefix;
	int severity = message_get_severity(message);
	char *dle_hostname = message_get_argument(message, "dle_hostname");
	if (dle_hostname && *dle_hostname) {
	    if (severity == MSG_SUCCESS) {
		prefix = g_strdup_printf("HOST %s OK: ", dle_hostname);
	    } else if (severity == MSG_INFO) {
		prefix = g_strdup_printf("HOST %s OK: ", dle_hostname);
	    } else if (severity == MSG_MESSAGE) {
		prefix = g_strdup_printf("HOST %s OK: ", dle_hostname);
	    } else if (severity == MSG_WARNING) {
		prefix = g_strdup_printf("HOST %s WARNING: ", dle_hostname);
	    } else if (severity == MSG_ERROR) {
		prefix = g_strdup_printf("HOST %s ERROR: ", dle_hostname);
	    } else if (severity == MSG_CRITICAL) {
		prefix = g_strdup_printf("HOST %s CRITICAL: ", dle_hostname);
	    } else {
		prefix = g_strdup_printf("HOST %s BAD: ", dle_hostname);
	    }
	} else {
	    if (severity == MSG_SUCCESS) {
		prefix = g_strdup("NOTE: ");
	    } else if (severity == MSG_INFO) {
		prefix = g_strdup("NOTE: ");
	    } else if (severity == MSG_MESSAGE) {
		prefix = g_strdup("");
	    } else if (severity == MSG_WARNING) {
		prefix = g_strdup("WARNING: ");
	    } else if (severity == MSG_ERROR) {
		prefix = g_strdup("ERROR: ");
	    } else if (severity == MSG_CRITICAL) {
		prefix = g_strdup("CRITICAL: ");
	    } else {
		prefix = g_strdup("BAD: ");
	    }
	}
	g_fprintf(file, "%s%s\n", prefix, get_message(message));
	if ((hint = message_get_hint(message)) != NULL) {
	    int len = strlen(prefix);
	    g_fprintf(file, "%*c%s\n", len, ' ', hint);
	}
	g_free(prefix);
    }
    return message;
}

void
amcheck_exit(
    int status)
{
    exit(status);
}

void
usage(void)
{
    delete_message(amcheck_print_message(build_message(
		AMANDA_FILE, __LINE__, 2800000, MSG_MESSAGE, 0)));

    amcheck_exit(1);
    /*NOTREACHED*/
}

int
main(
    int		argc,
    char **	argv)
{
    char buffer[BUFFER_SIZE];
    char *mainfname = NULL;
    char pid_str[NUM_STR_SIZE];
    int do_clientchk, client_probs;
    int do_localchk, do_tapechk, server_probs;
    pid_t clientchk_pid, serverchk_pid;
    FILE *tempfd, *mainfd;
    amwait_t retstat;
    pid_t pid;
    extern int optind;
    char *mailto = NULL;
    extern char *optarg;
    int mailout;
    int alwaysmail;
    char *tempfname = NULL;
    char *conf_diskfile;
    char *dumpuser;
    struct passwd *pw;
    uid_t uid_me;
    GPtrArray *err_array;
    guint i;
    config_overrides_t *cfg_ovr;
    char *mailer;

    glib_init();

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

    set_pname("amcheck");
    /* drop root privileges */
    if (!set_root_privs(0)) {
	error("amcheck must be run setuid root");
    }

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    memset(buffer, 0, sizeof(buffer));

    g_snprintf(pid_str, sizeof(pid_str), "%ld", (long)getpid());

    add_amanda_log_handler(amanda_log_stderr);

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    uid_me = geteuid();

    alwaysmail = mailout = overwrite = 0;
    do_localchk = do_tapechk = do_clientchk = 0;
    server_probs = client_probs = 0;
    tempfd = mainfd = NULL;

    /* process arguments */

    cfg_ovr = new_config_overrides(argc/2);
    while (1) {
	int option_index = 0;
	int c;
	c = getopt_long (argc, argv, "M:mawsclto:", long_options, &option_index);
	if (c == -1) {
	    break;
	}

	switch(c) {
	case 1:		client_verbose = TRUE;
			break;
	case 2:		delete_message(amcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 2800001, MSG_MESSAGE, 1,
				"version", VERSION)));
			return(0);
			break;
	case 3:		exact_match = TRUE;
			break;
	case 4:		opt_message = TRUE;
			break;
	case 'M':	if (mailto) {
			    delete_message(amcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 2800002, MSG_ERROR, 0)));
			    amcheck_exit(1);
			}
			mailto=g_strdup(optarg);
			if(!validate_mailto(mailto)){
			    delete_message(amcheck_print_message(build_message(
				AMANDA_FILE, __LINE__, 2800003, MSG_ERROR, 0)));
			   amcheck_exit(1);
			}
			/*FALLTHROUGH*/
	case 'm':	mailout = 1;
			break;
	case 'a':	mailout = 1;
			alwaysmail = 1;
			break;
	case 's':	do_localchk = do_tapechk = 1;
			break;
	case 'c':	do_clientchk = 1;
			break;
	case 'l':	do_localchk = 1;
			break;
	case 'w':	overwrite = 1;
			break;
	case 'o':	add_config_override_opt(cfg_ovr, optarg);
			break;
	case 't':	do_tapechk = 1;
			break;
	case '?':
	default:
	    usage();
	}
    }

    argc -= optind, argv += optind;
    if(argc < 1) usage();


    if ((do_localchk | do_clientchk | do_tapechk) == 0) {
	/* Check everything if individual checks were not asked for */
	do_localchk = do_clientchk = do_tapechk = 1;
    }

    if(overwrite)
	do_tapechk = 1;

    set_config_overrides(cfg_ovr);
    config_init_with_global(CONFIG_INIT_EXPLICIT_NAME, argv[0]);
    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &origq);
    disable_skip_disk(&origq);
    amfree(conf_diskfile);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	if (opt_message) {
	    config_print_errors_as_message();
	} else {
	    config_print_errors();
	}
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800228, MSG_ERROR, 0)));
	    exit(1);
	    //g_critical("errors processing config file");
	}
    }

    mailer = getconf_str(CNF_MAILER);
    if ((!mailer || *mailer == '\0') && mailout == 1) {
	if (alwaysmail == 1) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800004, MSG_ERROR, 0)));
	} else {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800005, MSG_ERROR, 0)));
	}
	amcheck_exit(1);
    }
    if(mailout && !mailto &&
       (getconf_seen(CNF_MAILTO)==0 || strlen(getconf_str(CNF_MAILTO)) == 0)) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800006, MSG_ERROR, 0)));
        if (alwaysmail) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800008, MSG_ERROR, 0)));
	} else {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800009, MSG_ERROR, 0)));
	}
	amcheck_exit(1);
    }
    if(mailout && !mailto)
    {
       if(getconf_seen(CNF_MAILTO) &&
          strlen(getconf_str(CNF_MAILTO)) > 0) {
          if(!validate_mailto(getconf_str(CNF_MAILTO))){
		delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800010, MSG_ERROR, 1,
			"mailto", getconf_str(CNF_MAILTO))));
		delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800011, MSG_INFO, 0)));
                mailout = 0;
          }
       }
       else {
	  delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800012, MSG_ERROR, 0)));
          if (alwaysmail) {
		delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800013, MSG_ERROR, 0)));
	  } else {
		delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800014, MSG_ERROR, 0)));
	  }
	  amcheck_exit(1);
      }
    }

    conf_ctimeout = (time_t)getconf_int(CNF_CTIMEOUT);

    err_array = match_disklist(&origq, exact_match, argc-1, argv+1);
    if (err_array->len > 0) {
	for (i = 0; i < err_array->len; i++) {
	    char *errstr = g_ptr_array_index(err_array, i);
	    g_debug("%s", errstr);
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800015, MSG_MESSAGE, 1,
			"errstr", errstr)));
	}
    }
    g_ptr_array_free(err_array, TRUE);

    /*
     * Make sure we are running as the dump user.  Don't use
     * check_running_as(..) here, because we want to produce more
     * verbose error messages.
     */
    dumpuser = getconf_str(CNF_DUMPUSER);
    if ((pw = getpwnam(dumpuser)) == NULL) {
	delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800215, MSG_ERROR, 1,
			"dumpuser"	, dumpuser)));
	exit(1);
	/*NOTREACHED*/
    }
    uid_dumpuser = pw->pw_uid;
    if ((pw = getpwuid(uid_me)) == NULL) {
	// leak memory
	delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800216, MSG_ERROR, 1,
			"uid"	, g_strdup_printf("%ld", (long)uid_me))));
	exit(1);
	/*NOTREACHED*/
    }
#ifdef CHECK_USERID
    if (uid_me != uid_dumpuser) {
	delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800217, MSG_ERROR, 2,
			"running_user"	, pw->pw_name,
			"expected_user" , dumpuser)));
	exit(1);
        /*NOTREACHED*/
    }
#endif

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
	tempfname = g_strjoin(NULL, AMANDA_TMPDIR, "/amcheck.temp.", pid_str, NULL);
	if((tempfd = fopen(tempfname, "w+")) == NULL) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800218, MSG_ERROR, 2,
			"filename", tempfname,
			"errno"   , errno)));
	    exit(1);
	    /*NOTREACHED*/
	}
    }

    if(mailout) {
	/* the main fd is a file too */
	mainfname = g_strjoin(NULL, AMANDA_TMPDIR, "/amcheck.main.", pid_str, NULL);
	if((mainfd = fopen(mainfname, "w+")) == NULL) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800219, MSG_ERROR, 2,
			"filename", tempfname,
			"errno"   , errno)));
	    exit(1);
	    /*NOTREACHED*/
	}
	unlink(mainfname);			/* so it goes away on close */
	amfree(mainfname);
    }
    else
	/* just use stdout */
	mainfd = stdout;

    who_check_host_setting = do_localchk;

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
	    delete_message(amcheck_fprint_message(mainfd, build_message(
					AMANDA_FILE, __LINE__, 2800021, MSG_ERROR, 1,
					"pid", g_strdup_printf("%ld", (long)pid))));
	}
    }

    /* copy temp output to main output and write tagline */

    if (tempfd) {
	char line[1025];
	FILE *tempfdr;
	tempfdr = fopen(tempfname, "r");
	if (!tempfdr) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800228, MSG_ERROR, 2,
			"filename", tempfname,
			"errno"   , errno)));
	} else {
	    if(fseek(tempfdr, (off_t)0, 0) == (off_t)-1) {
		delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800220, MSG_ERROR, 2,
			"errno"   , errno)));
		exit(1);
		/*NOTREACHED*/
	    }

	    if (opt_message) fprintf(mainfd,",\n");
	    while (fgets(line, 1024, tempfdr)) {
		fprintf(mainfd, "%s", line);
	    }
	    fclose(tempfdr);
	}
	fclose(tempfd);
	unlink(tempfname);			/* so it goes away on close */
	amfree(tempfname);
    }

    if (opt_message) fprintf(mainfd, "\n");
    delete_message(amcheck_fprint_message(mainfd, build_message(
			AMANDA_FILE, __LINE__, 2800016, MSG_MESSAGE, 1,
			"version", VERSION)));

    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;

    /* send mail if requested, but only if there were problems */

    if((server_probs || client_probs || alwaysmail) && mailout) {
	int mailfd;
	int nullfd;
	int errfd;
	FILE *ferr;
	char *subject;
	char **a, **b;
	GPtrArray *pipeargs;
	amwait_t retstat;
	size_t r;
	size_t w;
	char *err = NULL;
	char *extra_info = NULL;
	char *line = NULL;
	int rc;
	int valid_mailto = 0;

	fflush(stdout);
	if (fseek(mainfd, (off_t)0, SEEK_SET) == (off_t)-1) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800221, MSG_ERROR, 2,
			"errno"   , errno)));
	    exit(1);
	    /*NOTREACHED*/
	}
	if(alwaysmail && !(server_probs || client_probs)) {
	    subject = g_strdup_printf("%s AMCHECK REPORT: NO PROBLEMS FOUND",
			getconf_str(CNF_ORG));
	} else {
	    subject = g_strdup_printf(
			"%s AMANDA PROBLEM: FIX BEFORE RUN, IF POSSIBLE",
			getconf_str(CNF_ORG));
	}
	if(mailto) {
	    a = (char **) g_new0(char *, 2);
	    a[0] = g_strdup(mailto);
	    a[1] = NULL;
	} else {
	    /* (note that validate_mailto doesn't allow any quotes, so this
	     * is really just splitting regular old strings) */
	    a = split_quoted_strings(getconf_str(CNF_MAILTO));
	    if (!a) {
		 delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800017, MSG_ERROR, 1,
			"mailto", getconf_str(CNF_MAILTO))));
		amcheck_exit(1);
	    }
	}
	if((nullfd = open("/dev/null", O_RDWR)) < 0) {
		delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800227, MSG_ERROR, 2,
			"errno"   , errno)));
	    exit(1);
	    /*NOTREACHED*/
	}

	/* assemble the command line for the mailer */
	pipeargs = g_ptr_array_sized_new(4);
	g_ptr_array_add(pipeargs, mailer);
	g_ptr_array_add(pipeargs, "-s");
	g_ptr_array_add(pipeargs, subject);
	for (b = a; *b; b++) {
	    if (strlen(*b) > 0) {
	        g_ptr_array_add(pipeargs, *b);
		valid_mailto++;
	    }
	}
	g_ptr_array_add(pipeargs, NULL);
	if (!valid_mailto) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800017, MSG_ERROR, 1,
			"mailto", getconf_str(CNF_MAILTO))));
	    amcheck_exit(1);
	}

	pipespawnv(mailer, STDIN_PIPE | STDERR_PIPE, 0,
		   &mailfd, &nullfd, &errfd,
		   (char **)pipeargs->pdata);

	g_ptr_array_free(pipeargs, FALSE);
	amfree(subject);
	amfree(mailto);
	g_strfreev(a);

	/*
	 * There is the potential for a deadlock here since we are writing
	 * to the process and then reading stderr, but in the normal case,
	 * nothing should be coming back to us, and hopefully in error
	 * cases, the pipe will break and we will exit out of the loop.
	 */
	signal(SIGPIPE, SIG_IGN);
	while((r = read_fully(fileno(mainfd), buffer, sizeof(buffer), NULL)) > 0) {
	    if((w = full_write(mailfd, buffer, r)) != r) {
		if(errno == EPIPE) {
		    strappend(extra_info, "EPIPE writing to mail process\n");
		    break;
		} else if(errno != 0) {
		    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800222, MSG_ERROR, 2,
			"errno"   , errno)));
		    exit(1);
		    /*NOTREACHED*/
		} else {
		    // the 2 g_strdup_printf leak memory.
		    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800223, MSG_ERROR, 2,
			"write_size"	, g_strdup_printf("%zd", w),
			"expected_size"	, g_strdup_printf("%zd", r))));
		    exit(1);
		    /*NOTREACHED*/
		}
	    }
	}
	aclose(mailfd);
	ferr = fdopen(errfd, "r");
	if (!ferr) {
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800224, MSG_ERROR, 1,
			"errno"   , errno)));
	    exit(1);
	    /*NOTREACHED*/
	}
	for(; (line = pgets(ferr)) != NULL; free(line)) {
	    if (line[0] == '\0')
		continue;
	    strappend(extra_info, line);
	    strappend(extra_info, "\n");
	}
	afclose(ferr);
	errfd = -1;
	rc = 0;
	while (wait(&retstat) != -1) {
	    if (!WIFEXITED(retstat) || WEXITSTATUS(retstat) != 0) {
		char *mailer_error = str_exit_status("mailer", retstat);
		strappend(err, mailer_error);
		amfree(mailer_error);

		rc = 1;
	    }
	}
	if (rc != 0) {
	    if(extra_info) {
		fputs(extra_info, stderr);
		amfree(extra_info);
	    }
	    delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800225, MSG_ERROR, 2,
			"mailer"   , mailer,
			"errmsg"   , err?err:"(unknown)")));
	    exit(1);
	    /*NOTREACHED*/
	} else {
	    amfree(extra_info);
	}
    }

    printf("\n");
    g_debug("server_probs: %d", server_probs);
    g_debug("client_probs: %d", client_probs);
    dbclose();
    return (server_probs || client_probs);
}

/* --------------------------------------------------- */

static char *datestamp;

int check_tapefile(
    FILE *outf,
    char *tapefile)
{
    struct stat statbuf;
    int tapebad = 0;

    if (stat(tapefile, &statbuf) == 0) {
	if (!S_ISREG(statbuf.st_mode)) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800018, MSG_ERROR, 1,
			"tapelist" , tapefile)));
	    tapebad = 1;
	} else if (access(tapefile, F_OK) != 0) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800019, MSG_ERROR, 2,
			"errno"	   , errno,
			"tapelist" , tapefile)));
	    tapebad = 1;
	} else if (access(tapefile, W_OK) != 0) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800020, MSG_ERROR, 2,
			"errno"	   , errno,
			"tapelist" , tapefile)));
	    tapebad = 1;
	}
    }
    return tapebad;
}

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

    pgm = g_strjoin(NULL, dir, "/", pgm, NULL);
    if(stat(pgm, &statbuf) == -1) {
	delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800022, MSG_ERROR, 1,
					"program", pgm)));
	pgmbad = 1;
    } else if (!S_ISREG(statbuf.st_mode)) {
	delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800023, MSG_ERROR, 1,
					"program", pgm)));
	pgmbad = 1;
    } else if (access(pgm, X_OK) == -1) {
	delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800024, MSG_ERROR, 1,
					"program", pgm)));
	pgmbad = 1;
#ifndef SINGLE_USERID
    } else if (suid \
	       && dumpuid != 0
	       && (statbuf.st_uid != 0 || (statbuf.st_mode & 04000) == 0)) {
	delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800025, MSG_ERROR, 1,
					"program", pgm)));
	pgmbad = 1;
#else
    /* Quiet unused parameter warnings */
    (void)suid;
    (void)dumpuid;
#endif /* SINGLE_USERID */
    }
    amfree(pgm);
    return pgmbad;
}

/* check that the tape is a valid amanda tape
   Returns TRUE if all tests passed; FALSE otherwise. */
static gboolean test_tape_status(FILE * outf) {
    int dev_outfd = -1;
    FILE *dev_outf = NULL;
    int outfd;
    int nullfd = -1;
    pid_t devpid;
    char *amcheck_device = NULL;
    char *line;
    gchar **args;
    amwait_t wait_status;
    gboolean success = TRUE;
    identlist_t il;
    int i;

    if ((nullfd = open("/dev/null", O_RDWR)) == -1) {
	return FALSE;
    }

    fflush(outf);
    outfd = fileno(outf);

    amcheck_device = g_strjoin(NULL, amlibexecdir, "/", "amcheck-device", NULL);
    i = 3;
    if (opt_message) i++;
    if (overwrite) i++;
    args = get_config_options(i);
    args[0] = amcheck_device; /* steal the reference */
    args[1] = g_strdup(get_config_name());
    args[2] = NULL;
    i = 3;
    if (opt_message)
	args[i++] = g_strdup("--message");
    if (overwrite)
	args[i++] = g_strdup("-w");

    for (il = getconf_identlist(CNF_STORAGE); il != NULL; il = il->next) {

	args[2] = (char *)il->data;
	/* run libexecdir/amcheck-device.pl, capturing STDERR and STDOUT to outf */
	devpid = pipespawnv(amcheck_device, STDOUT_PIPE, 0,
	    &nullfd, &dev_outfd, &outfd,
	    (char **)args);

	dev_outf = fdopen(dev_outfd, "r");
	if (dev_outf == NULL) {
	    g_debug("Can't fdopen amcheck-device stdout: %s", strerror(errno));
	    aclose(dev_outfd);
	} else {
	    while ((line = agets(dev_outf)) != NULL) {
		if (strncmp(line, "DATA-PATH", 9) == 0) {
		    char *c = line;
		    dev_amanda_data_path = FALSE;
		    dev_directtcp_data_path = FALSE;
		    while ((c = strchr(c, ' ')) != NULL) {
			c++;
			if (strncmp(c, "AMANDA", 6) == 0) {
			    dev_amanda_data_path = TRUE;
			} else if (strncmp(c, "DIRECTTCP", 9) == 0) {
			    dev_directtcp_data_path = TRUE;
			}
		    }
		} else {
		    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 123, MSG_MESSAGE, 1,
					"errstr", line)));
		}
		g_free(line);
	    }
	    fclose(dev_outf);
	}

	/* and immediately wait for it to die */
	waitpid(devpid, &wait_status, 0);

	if (WIFSIGNALED(wait_status)) {
	    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800026, MSG_ERROR, 1,
					"signal", g_strdup_printf("%d", WTERMSIG(wait_status)))));
	    success = FALSE;
	} else if (WIFEXITED(wait_status)) {
	    if (WEXITSTATUS(wait_status) != 0)
		success = FALSE;
	} else {
	    success = FALSE;
	}
	args[2] = NULL;
    }

    g_strfreev(args);
    close(nullfd);

    return success;
}

pid_t
start_server_check(
    FILE	*outf,
    int		do_localchk,
    int		do_tapechk)
{
    struct fs_usage fsusage;
    pid_t pid G_GNUC_UNUSED;
    int confbad = 0, tapebad = 0, disklow = 0, logbad = 0;
    int userbad = 0, infobad = 0, indexbad = 0, pgmbad = 0;
    int testtape = do_tapechk;
    tapetype_t *tp = NULL;
    storage_t  *storage = NULL;
    int res;
    intmax_t kb_avail, kb_needed;
    off_t tape_size;
    gboolean printed_small_part_size_warning = FALSE;

    switch(pid = fork()) {
    case -1:
	delete_message(amcheck_print_message(build_message(
			AMANDA_FILE, __LINE__, 2800226, MSG_ERROR, 1,
			"errno"	, errno)));
	exit(1);
        g_assert_not_reached();

    case 0:
	break;

    default:
	return pid;
    }

    set_pname("amcheck-server");

    startclock();

    /* server does not need root privileges, and the access() calls below use the real userid,
     * so totally drop privileges at this point (making the userid equal to the dumpuser) */
    set_root_privs(-1);

    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800027, MSG_MESSAGE, 0)));
    if (!opt_message) {
	fprintf(outf, "-----------------------------\n");
    }

    if (who_check_host_setting) {
	check_host_setting(outf);
    }

    if (do_localchk || testtape) {
	identlist_t il;
	for (il = getconf_identlist(CNF_STORAGE); il != NULL; il = il->next) {
	    char *storage_n = il->data;
	    policy_s *policy;
	    char *policy_n;
	    char *lbl_templ;

	    storage = lookup_storage(storage_n);
	    if (storage)
		tp = lookup_tapetype(storage_get_tapetype(storage));

	    lbl_templ = tapetype_get_lbl_templ(tp);
	    if (!g_str_equal(lbl_templ, "")) {
		lbl_templ = config_dir_relative(lbl_templ);
		if (access(lbl_templ, R_OK) == -1) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800029, MSG_ERROR, 3,
			"errno",	errno,
			"storage",	storage_n,
			"filename",	lbl_templ)));
		    confbad = 1;
		}
		amfree(lbl_templ);
#if !defined(HAVE_LPR_CMD)
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800030, MSG_ERROR, 1,
			"storage",	storage_n)));
		confbad = 1;
#endif
	    }

	    if (storage_get_flush_threshold_scheduled(storage)
		< storage_get_flush_threshold_dumped(storage)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800031, MSG_WARNING, 3,
			"storage",	storage_n,
			"flush_threshold_dumped", g_strdup_printf("%d",storage_get_flush_threshold_dumped(storage)),
			"flush_threshold_scheduled", g_strdup_printf("%d",storage_get_flush_threshold_scheduled(storage)))));
	    }

	    if (storage_get_flush_threshold_scheduled(storage)
		< storage_get_taperflush(storage)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800032, MSG_WARNING, 3,
			"storage",	storage_n,
			"taperflush", g_strdup_printf("%d",storage_get_taperflush(storage)),
			"flush_threshold_scheduled", g_strdup_printf("%d",storage_get_flush_threshold_scheduled(storage)))));
	    }

	    if (storage_get_taperflush(storage) &&
	        !storage_get_autoflush(storage)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800033, MSG_WARNING, 3,
			"storage",	storage_n,
			"taperflush", g_strdup_printf("%d",storage_get_taperflush(storage)))));
	    }

	    if (!storage_seen(storage, STORAGE_TAPETYPE)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800034, MSG_ERROR, 1,
			"storage",	storage_n)));
		confbad = 1;
	    }

	    policy_n = storage_get_policy(storage);
	    policy = lookup_policy(policy_n);
	    if (policy_get_retention_tapes(policy) <= storage_get_runtapes(storage)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800035, MSG_ERROR, 2,
			"storage",	storage_n,
			"policy",	policy_n)));
	    }

	    {
		uintmax_t kb_avail = physmem_total() / 1024;
		uintmax_t kb_needed = storage_get_device_output_buffer_size(storage) / 1024;
		if (kb_avail < kb_needed) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800036, MSG_ERROR, 2,
			"kb_avail",	g_strdup_printf("%ju", kb_avail),
			"kb_needed",	g_strdup_printf("%ju", kb_needed))));
		}
	    }
	}

	/* Double-check that 'localhost' resolves properly */
	if ((res = resolve_hostname("localhost", 0, NULL, NULL) != 0)) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800037, MSG_ERROR, 1,
			"gai_strerror",	gai_strerror(res))));
	    confbad = 1;
	}

    }

    /*
     * Look up the programs used on the server side.
     */
    if(do_localchk) {
	/*
	 * entreprise version will do planner/dumper suid check
	 */
	if(access(amlibexecdir, X_OK) == -1) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800038, MSG_ERROR, 2,
			"dir",	amlibexecdir,
			"errno", errno)));
	    pgmbad = 1;
	} else {
	    if(test_server_pgm(outf, amlibexecdir, "planner", 1, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, amlibexecdir, "dumper", 1, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, amlibexecdir, "driver", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, amlibexecdir, "taper", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, amlibexecdir, "amtrmidx", 0, uid_dumpuser))
		pgmbad = 1;
	    if(test_server_pgm(outf, amlibexecdir, "amlogroll", 0, uid_dumpuser))
		pgmbad = 1;
	}
	if(access(sbindir, X_OK) == -1) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800040, MSG_ERROR, 2,
			"dir",	sbindir,
			"errno", errno)));
	    pgmbad = 1;
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
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800042, MSG_WARNING, 2,
			"program", COMPRESS_PATH,
			"errno",   errno)));
	}
    }

    /*
     * Check that the directory for the tapelist file is writable, as well
     * as the tapelist file itself (if it already exists).  Also, check for
     * a "hold" file (just because it is convenient to do it here) and warn
     * if tapedev is set to the null device.
     */

    if(do_localchk || do_tapechk) {
	char *tapefile;
	char *newtapefile;
	char *tape_dir;
	char *lastslash;
	char *holdfile;
        char * tapename;
	struct stat statbuf;
	guint64 part_size, part_cache_max_size, tape_size;
	part_cache_type_t part_cache_type;
	char *part_cache_dir;

	tapefile = config_dir_relative(getconf_str(CNF_TAPELIST));
	/*
	 * XXX There Really Ought to be some error-checking here... dhw
	 */
	tape_dir = g_strdup(tapefile);
	if ((lastslash = strrchr((const char *)tape_dir, '/')) != NULL) {
	    *lastslash = '\0';
	/*
	 * else whine Really Loudly about a path with no slashes??!?
	 */
	}
	if(access(tape_dir, W_OK) == -1) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800044, MSG_ERROR, 2,
			"tape_dir", tape_dir,
			"errno",   errno)));
	    tapebad = 1;
	}
	else if(stat(tapefile, &statbuf) == -1) {
	    if (errno != ENOENT) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800045, MSG_ERROR, 2,
			"tapefile", tapefile,
			"errno",   errno)));
		tapebad = 1;
	    } else {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800046, MSG_INFO, 0)));
	    }
	} else {
	    tapebad |= check_tapefile(outf, tapefile);
	    if (tapebad == 0 && read_tapelist(tapefile)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800047, MSG_INFO, 1,
			"tapefile", tapefile)));
		tapebad = 1;
	    }
	    newtapefile = g_strconcat(tapefile, ".new", NULL);
	    tapebad |= check_tapefile(outf, newtapefile);
	    amfree(newtapefile);
	    newtapefile = g_strconcat(tapefile, ".amlabel", NULL);
	    tapebad |= check_tapefile(outf, newtapefile);
	    amfree(newtapefile);
	    newtapefile = g_strconcat(tapefile, ".amlabel.new", NULL);
	    tapebad |= check_tapefile(outf, newtapefile);
	    amfree(newtapefile);
	    newtapefile = g_strconcat(tapefile, ".yesterday", NULL);
	    tapebad |= check_tapefile(outf, newtapefile);
	    amfree(newtapefile);
	    newtapefile = g_strconcat(tapefile, ".yesterday.new", NULL);
	    tapebad |= check_tapefile(outf, newtapefile);
	    amfree(newtapefile);
	}
	holdfile = config_dir_relative("hold");
	if(access(holdfile, F_OK) != -1) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800048, MSG_WARNING, 1,
			"holdfile", holdfile)));
	}
	amfree(tapefile);
	amfree(tape_dir);
	amfree(holdfile);
	tapename = getconf_str(CNF_TAPEDEV);
	if (tapename == NULL) {
	    if (getconf_str(CNF_TPCHANGER) == NULL &&
		getconf_identlist(CNF_STORAGE) == NULL) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800051, MSG_WARNING, 0)));
		testtape = 0;
		do_tapechk = 0;
	    }
	}

	/* check tapetype-based splitting parameters */
	part_size = tapetype_get_part_size(tp);
	part_cache_type = tapetype_get_part_cache_type(tp);
	part_cache_dir = tapetype_get_part_cache_dir(tp);
	part_cache_max_size = tapetype_get_part_cache_max_size(tp);

	if (!tapetype_seen(tp, TAPETYPE_PART_SIZE)) {
	    if (tapetype_seen(tp, TAPETYPE_PART_CACHE_TYPE)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800052, MSG_ERROR, 0)));
		tapebad = 1;
	    }
	    if (tapetype_seen(tp, TAPETYPE_PART_CACHE_DIR)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800053, MSG_ERROR, 0)));
		tapebad = 1;
	    }
	    if (tapetype_seen(tp, TAPETYPE_PART_CACHE_MAX_SIZE)) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800054, MSG_ERROR, 0)));
		tapebad = 1;
	    }
	} else {
	    switch (part_cache_type) {
	    case PART_CACHE_TYPE_DISK:
		if (!tapetype_seen(tp, TAPETYPE_PART_CACHE_DIR)
			    || !part_cache_dir || !*part_cache_dir) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800055, MSG_ERROR, 0)));
		    tapebad = 1;
		} else {
		    if(get_fs_usage(part_cache_dir, NULL, &fsusage) == -1) {
			delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800056, MSG_ERROR, 2,
				"errno", errno,
				"part-cache-dir", part_cache_dir)));
			tapebad = 1;
		    } else {
			kb_avail = fsusage.fsu_bavail_top_bit_set?
			    0 : fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;
			kb_needed = part_size;
			if (tapetype_seen(tp, TAPETYPE_PART_CACHE_MAX_SIZE)) {
			    kb_needed = part_cache_max_size;
			}
			if (kb_avail < kb_needed) {
			    delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800057, MSG_ERROR, 2,
				"kb_avail", kb_avail,
				"kb_needed", kb_needed)));
			    tapebad = 1;
			}
		    }
		}
		break;

	    case PART_CACHE_TYPE_MEMORY:
		kb_avail = physmem_total() / 1024;
		kb_needed = part_size;
		if (tapetype_seen(tp, TAPETYPE_PART_CACHE_MAX_SIZE)) {
		    kb_needed = part_cache_max_size;
		}
		if (kb_avail < kb_needed) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800058, MSG_ERROR, 2,
			"kb_avail", kb_avail,
			"kb_needed", kb_needed)));
		    tapebad = 1;
		}

		/* FALL THROUGH */

	    case PART_CACHE_TYPE_NONE:
		if (tapetype_seen(tp, TAPETYPE_PART_CACHE_DIR)) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800059, MSG_ERROR, 0)));
		    tapebad = 1;
		}
		break;
	    }
	}

	if (tapetype_seen(tp, TAPETYPE_PART_SIZE) && part_size == 0
		&& part_cache_type != PART_CACHE_TYPE_NONE) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800060, MSG_ERROR, 0)));
	    tapebad = 1;
	}

	if (tapetype_seen(tp, TAPETYPE_PART_CACHE_MAX_SIZE)) {
	    if (part_cache_type == PART_CACHE_TYPE_NONE) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800061, MSG_ERROR, 0)));
		tapebad = 1;
	    }

	    if (part_cache_max_size > part_size) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800062, MSG_WARNING, 0)));
	    }
	}

	tape_size = tapetype_get_length(tp);
	if (part_size && part_size * 1000 < tape_size) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800063, MSG_WARNING, 1,
			"part_size", g_strdup_printf("%ju", (uintmax_t)part_size))));
	    if (!printed_small_part_size_warning) {
		printed_small_part_size_warning = TRUE;
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800064, MSG_WARNING, 0)));
	    }
	} else if (part_cache_max_size && part_cache_max_size * 1000 < tape_size) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800065, MSG_WARNING, 1,
			"part_size_max_size", part_cache_max_size)));
	    if (!printed_small_part_size_warning) {
		printed_small_part_size_warning = TRUE;
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800064, MSG_WARNING, 0)));
	    }
	}
    }

    /* check available disk space */

    if(do_localchk) {
	identlist_t    il;
	holdingdisk_t *hdp;

	for (il = getconf_identlist(CNF_HOLDINGDISK);
		il != NULL;
		il = il->next) {
	    hdp = lookup_holdingdisk(il->data);
	    if(get_fs_usage(holdingdisk_get_diskdir(hdp), NULL, &fsusage) == -1) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800066, MSG_ERROR, 2,
			"errno", errno,
			"holding_dir", holdingdisk_get_diskdir(hdp))));
		disklow = 1;
		continue;
	    }

	    /* do the division first to avoid potential integer overflow */
	    if (fsusage.fsu_bavail_top_bit_set)
		kb_avail = 0;
	    else
		kb_avail = fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;

	    if(access(holdingdisk_get_diskdir(hdp), W_OK) == -1) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800067, MSG_ERROR, 2,
			"errno", errno,
			"holding_dir", holdingdisk_get_diskdir(hdp))));
		disklow = 1;
	    }
	    else if(access(holdingdisk_get_diskdir(hdp), X_OK) == -1) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800069, MSG_ERROR, 2,
			"errno", errno,
			"holding_dir", holdingdisk_get_diskdir(hdp))));
		disklow = 1;
	    }
	    else if(holdingdisk_get_disksize(hdp) > (off_t)0) {
		if(kb_avail == 0) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800071, MSG_WARNING, 2,
			"holding_dir", holdingdisk_get_diskdir(hdp),
			"size", holdingdisk_get_disksize(hdp))));
		    disklow = 1;
		}
		else if(kb_avail < holdingdisk_get_disksize(hdp)) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800072, MSG_WARNING, 3,
			"holding_dir", holdingdisk_get_diskdir(hdp),
			"avail", g_strdup_printf("%jd", kb_avail),
			"requested", g_strdup_printf("%jd", holdingdisk_get_disksize(hdp)))));
		    disklow = 1;
		}
		else {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800073, MSG_INFO, 3,
			"holding_dir", holdingdisk_get_diskdir(hdp),
			"avail", g_strdup_printf("%jd", kb_avail),
			"requested", g_strdup_printf("%jd", holdingdisk_get_disksize(hdp)))));
		}
	    }
	    else {
		if(kb_avail < -holdingdisk_get_disksize(hdp)) {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800074, MSG_WARNING, 2,
			"holding_dir", holdingdisk_get_diskdir(hdp),
			"avail", g_strdup_printf("%jd", kb_avail))));
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800075, MSG_WARNING, 0)));
		    disklow = 1;
		}
		else {
		    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800076, MSG_INFO, 3,
			"holding_dir", holdingdisk_get_diskdir(hdp),
			"avail", g_strdup_printf("%jd", kb_avail),
			"using", g_strdup_printf("%jd", kb_avail + holdingdisk_get_disksize(hdp)))));
		}
	    }
	}
    }

    /* check that the log file is writable if it already exists */

    if(do_localchk) {
	char *conf_logdir;
	char *olddir;
	struct stat stat_old;
	struct stat statbuf;

	conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));

	if(stat(conf_logdir, &statbuf) == -1) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800077, MSG_INFO, 2,
			"errno", errno,
			"logdir", conf_logdir)));
	    disklow = 1;
	}
	else if(access(conf_logdir, W_OK) == -1) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800078, MSG_ERROR, 2,
			"errno", errno,
			"logdir", conf_logdir)));
	    logbad = 1;
	}

	olddir = g_strjoin(NULL, conf_logdir, "/oldlog", NULL);
	if (logbad == 0 && stat(olddir,&stat_old) == 0) { /* oldlog exist */
	    if(!(S_ISDIR(stat_old.st_mode))) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800079, MSG_ERROR, 2,
			"oldlogdir", olddir)));
		logbad = 1;
	    }
	    if(logbad == 0 && access(olddir, W_OK) == -1) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800081, MSG_ERROR, 2,
			"errno", errno,
			"oldlogdir", olddir)));
		logbad = 1;
	    }
	}
	else if(logbad == 0 && lstat(olddir,&stat_old) == 0) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800083, MSG_ERROR, 2,
			"errno", errno,
			"oldlogdir", olddir)));
	    logbad = 1;
	}

	amfree(olddir);
	amfree(conf_logdir);
    }

    if (testtape) {
        tapebad = !test_tape_status(outf);
    } else if (do_tapechk) {
	delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800085, MSG_WARNING, 0)));
	dev_amanda_data_path = TRUE;
	dev_directtcp_data_path = TRUE;
    } else if (logbad == 2) {
	delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800087, MSG_MESSAGE, 0)));

	/* we skipped the tape checks, but this is just a NOTE and
	 * should not result in a nonzero exit status, so reset logbad to 0 */
	logbad = 0;
	dev_amanda_data_path = TRUE;
	dev_directtcp_data_path = TRUE;
    } else {
	delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800089, MSG_INFO, 0)));
	dev_amanda_data_path = TRUE;
	dev_directtcp_data_path = TRUE;
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
	int conf_tapecycle;
	int conf_runspercycle;
	int conf_runtapes;
	identlist_t pp_scriptlist;

	conf_tapecycle = getconf_int(CNF_TAPECYCLE);
	conf_runspercycle = getconf_int(CNF_RUNSPERCYCLE);
	conf_runtapes = getconf_int(CNF_RUNTAPES);

	if (conf_tapecycle <= conf_runspercycle) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800090, MSG_INFO, 2,
			"tapecycle", g_strdup_printf("%d", conf_tapecycle),
			"runspercycle", g_strdup_printf("%d", conf_runspercycle))));
	}

	if (conf_tapecycle <= conf_runtapes) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800091, MSG_INFO, 2,
			"tapecycle", g_strdup_printf("%d", conf_tapecycle),
			"runtapes", g_strdup_printf("%d", conf_runtapes))));
	}

	conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
	conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));

	if(stat(conf_infofile, &statbuf) == -1) {
	    if (errno == ENOENT) {
	        delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800092, MSG_INFO, 1,
			"infodir", conf_infofile)));
	    } else {
	        delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800094, MSG_ERROR, 2,
			"errno", errno,
			"infodir", conf_infofile)));
		infobad = 1;
	    }
	    amfree(conf_infofile);
	} else if (!S_ISDIR(statbuf.st_mode)) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800095, MSG_ERROR, 1,
			"infodir", conf_infofile)));
	    amfree(conf_infofile);
	    infobad = 1;
	} else if (access(conf_infofile, W_OK) == -1) {
	    delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800097, MSG_ERROR, 1,
			"infodir", conf_infofile)));
	    amfree(conf_infofile);
	    infobad = 1;
	} else {
	    char *errmsg = NULL;
	    if (check_infofile(conf_infofile, &origq, &errmsg) == -1) {
		delete_message(amcheck_fprint_message(outf, build_message(
			AMANDA_FILE, __LINE__, 2800099, MSG_ERROR, 1,
			"errmsg", errmsg)));
		infobad = 1;
		amfree(errmsg);
	    }
	    strappend(conf_infofile, "/");
	}

	while(!empty(origq)) {
	    hostp = ((disk_t *)origq.head->data)->host;
	    host = sanitise_filename(hostp->hostname);
	    if(conf_infofile) {
		g_free(hostinfodir);
		hostinfodir = g_strconcat(conf_infofile, host, NULL);
		if(stat(hostinfodir, &statbuf) == -1) {
		    if (errno == ENOENT) {
			delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800100, MSG_INFO, 1,
				"hostinfodir", hostinfodir)));
		    } else {
			delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800102, MSG_ERROR, 2,
				"errno", errno,
				"hostinfodir", hostinfodir)));
			infobad = 1;
		    }
		    amfree(hostinfodir);
		} else if (!S_ISDIR(statbuf.st_mode)) {
		    delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800103, MSG_ERROR, 1,
				"hostinfodir", hostinfodir)));
		    amfree(hostinfodir);
		    infobad = 1;
		} else if (access(hostinfodir, W_OK) == -1) {
		    delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800105, MSG_ERROR, 1,
				"hostinfodir", hostinfodir)));
		    amfree(hostinfodir);
		    infobad = 1;
		} else {
		    strappend(hostinfodir, "/");
		}
	    }
	    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
		disk = sanitise_filename(dp->name);
		if(hostinfodir) {

		    g_free(diskdir);
		    diskdir = g_strconcat(hostinfodir, disk, NULL);
		    infofile = g_strjoin(NULL, diskdir, "/", "info", NULL);
		    if(stat(diskdir, &statbuf) == -1) {
			if (errno == ENOENT) {
			    delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800107, MSG_INFO, 1,
				"diskdir", diskdir)));
			} else {
			    delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800109, MSG_ERROR, 2,
				"errno", errno,
				"diskdir", diskdir)));
			    infobad = 1;
			}
		    } else if (!S_ISDIR(statbuf.st_mode)) {
			delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800110, MSG_ERROR, 1,
				"diskdir", diskdir)));
			infobad = 1;
		    } else if (access(diskdir, W_OK) == -1) {
			delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800112, MSG_ERROR, 1,
				"diskdir", diskdir)));
			infobad = 1;
		    } else if(stat(infofile, &statbuf) == -1) {
			if (errno == ENOENT) {
			    delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800114, MSG_INFO, 1,
				"infofile", infofile)));
			} else {
			    delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800116, MSG_INFO, 2,
				"errno", errno,
				"diskdir", diskdir)));
			    infobad = 1;
			}
		    } else if (!S_ISREG(statbuf.st_mode)) {
			delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800117, MSG_ERROR, 1,
				"infofile", infofile)));
			infobad = 1;
		    } else if (access(infofile, R_OK) == -1) {
			delete_message(amcheck_fprint_message(outf, build_message(
				AMANDA_FILE, __LINE__, 2800119, MSG_ERROR, 1,
				"infofile", infofile)));
			infobad = 1;
		    }
		    amfree(infofile);
		}
		if(dp->index) {
		    if(! indexdir_checked) {
			if(stat(conf_indexdir, &statbuf) == -1) {
			    if (errno == ENOENT) {
				delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800120, MSG_INFO, 1,
					"indexdir", conf_indexdir)));
			    } else {
				delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800122, MSG_ERROR, 2,
					"errno", errno,
					"indexdir", conf_indexdir)));
				indexbad = 1;
			    }
			    amfree(conf_indexdir);
			} else if (!S_ISDIR(statbuf.st_mode)) {
			    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800123, MSG_ERROR, 1,
					"indexdir", conf_indexdir)));
			    amfree(conf_indexdir);
			    indexbad = 1;
			} else if (access(conf_indexdir, W_OK) == -1) {
			    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800125, MSG_ERROR, 1,
					"indexdir", conf_indexdir)));
			    amfree(conf_indexdir);
			    indexbad = 1;
			} else {
			    strappend(conf_indexdir, "/");
			}
			indexdir_checked = 1;
		    }
		    if(conf_indexdir) {
			if(! hostindexdir_checked) {
			    hostindexdir = g_strconcat(conf_indexdir, host, NULL);
			    if(stat(hostindexdir, &statbuf) == -1) {
				if (errno == ENOENT) {
				    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800126, MSG_INFO, 1,
					"hostindexdir", hostindexdir)));
			        } else {
				    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800128, MSG_ERROR, 2,
					"errno", errno,
					"hostindexdir", hostindexdir)));
				    indexbad = 1;
				}
			        amfree(hostindexdir);
			    } else if (!S_ISDIR(statbuf.st_mode)) {
				delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800129, MSG_ERROR, 1,
					"hostindexdir", hostindexdir)));
			        amfree(hostindexdir);
			        indexbad = 1;
			    } else if (access(hostindexdir, W_OK) == -1) {
				delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800131, MSG_ERROR, 1,
					"hostindexdir", hostindexdir)));
			        amfree(hostindexdir);
			        indexbad = 1;
			    } else {
				strappend(hostindexdir, "/");
			    }
			    hostindexdir_checked = 1;
			}
			if(hostindexdir) {
			    g_free(diskdir);
			    diskdir = g_strconcat(hostindexdir, disk, NULL);
			    if(stat(diskdir, &statbuf) == -1) {
				if (errno == ENOENT) {
				    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800132, MSG_INFO, 1,
					"diskindexdir", diskdir)));
				} else {
				    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800132, MSG_ERROR, 2,
					"errno", errno,
					"diskindexdir", diskdir)));
				    indexbad = 1;
				}
			    } else if (!S_ISDIR(statbuf.st_mode)) {
				delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800135, MSG_ERROR, 2,
					"errno", errno,
					"diskindexdir", diskdir)));
				indexbad = 1;
			    } else if (access(diskdir, W_OK) == -1) {
				delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800137, MSG_ERROR, 1,
					"diskindexdir", diskdir)));
				indexbad = 1;
			    }
			}
		    }
		}

		if ( dp->encrypt == ENCRYPT_SERV_CUST ) {
		  if ( dp->srv_encrypt[0] == '\0' ) {
		    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800138, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		    pgmbad = 1;
		  }
		  else if(access(dp->srv_encrypt, X_OK) == -1) {
		    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800140, MSG_ERROR, 1,
					"program", dp->srv_encrypt)));
		    pgmbad = 1;
		  }
		}
		if ( dp->compress == COMP_SERVER_CUST ) {
		  if ( dp->srvcompprog[0] == '\0' ) {
		    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800142, MSG_ERROR, 0)));
		    pgmbad = 1;
		  }
		  else if(access(dp->srvcompprog, X_OK) == -1) {

		    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800144, MSG_ERROR, 1,
					"program", dp->srvcompprog)));
		    pgmbad = 1;
		  }
		}

		/* check deprecated splitting parameters */
		if (dumptype_seen(dp->config, DUMPTYPE_TAPE_SPLITSIZE)
		    || dumptype_seen(dp->config, DUMPTYPE_SPLIT_DISKBUFFER)
		    || dumptype_seen(dp->config, DUMPTYPE_FALLBACK_SPLITSIZE)) {
		    tape_size = tapetype_get_length(tp);
		    if (dp->tape_splitsize > tape_size) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800145, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			pgmbad = 1;
		    }
		    if (dp->tape_splitsize && dp->fallback_splitsize * 1024 > physmem_total()) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800147, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			pgmbad = 1;
		    }
		    if (dp->tape_splitsize && dp->fallback_splitsize > tape_size) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800148, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			pgmbad = 1;
		    }

		    /* also check for part sizes that are too small */
		    if (dp->tape_splitsize && dp->tape_splitsize * 1000 < tape_size) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800149, MSG_WARNING, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"tape_splitsize", g_strdup_printf("%jd", (intmax_t)dp->tape_splitsize))));
			if (!printed_small_part_size_warning) {
			    printed_small_part_size_warning = TRUE;
			    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800064, MSG_WARNING, 0)));
			}
		    }

		    /* fallback splitsize will be used if split_diskbuffer is empty or NULL */
		    if (dp->tape_splitsize != 0 && dp->fallback_splitsize != 0 &&
			    (dp->split_diskbuffer == NULL ||
			     dp->split_diskbuffer[0] == '\0') &&
			    dp->fallback_splitsize * 1000 < tape_size) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800151, MSG_WARNING, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"fallback_splitsize", g_strdup_printf("%jd", (intmax_t)dp->fallback_splitsize))));
			if (!printed_small_part_size_warning) {
			    printed_small_part_size_warning = TRUE;
			    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800064, MSG_WARNING, 0)));
			}
		    }
		}

		if (dp->data_path == DATA_PATH_DIRECTTCP) {
		    if (dp->compress != COMP_NONE) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800153, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			pgmbad = 1;
		    }
		    if (dp->encrypt != ENCRYPT_NONE) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800154, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			pgmbad = 1;
		    }
		    if (dp->to_holdingdisk == HOLD_REQUIRED) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800155, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			pgmbad = 1;
		    }
		}
		if (dp->data_path == DATA_PATH_DIRECTTCP && !dev_directtcp_data_path) {
		    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800156, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		    pgmbad = 1;
		}
		if (dp->data_path == DATA_PATH_AMANDA && !dev_amanda_data_path) {
		    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800157, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		    pgmbad = 1;
		}

		for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
		     pp_scriptlist = pp_scriptlist->next) {
		    pp_script_t *pp_script = lookup_pp_script((char *)pp_scriptlist->data);
		    g_assert(pp_script != NULL);
		    if (pp_script_get_execute_where(pp_script) == EXECUTE_WHERE_CLIENT &&
			pp_script_get_execute_on(pp_script) & EXECUTE_ON_PRE_HOST_BACKUP) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800158, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		    } else if (pp_script_get_execute_where(pp_script) == EXECUTE_WHERE_CLIENT &&
			pp_script_get_execute_on(pp_script) & EXECUTE_ON_POST_HOST_BACKUP) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800159, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		    }
		}

		// check TAG match one of the storage
		{
		    identlist_t tags;
		    int count = 0;
		    int match_all = 0;
		    int match_full = 0;
		    int match_incr = 0;
		    for (tags = dp->tags; tags != NULL ; tags = tags->next) {
			gboolean found = FALSE;
			count++;
			for (storage = get_first_storage(); storage != NULL;
			     storage = get_next_storage(storage)) {
			    dump_selection_list_t dsl = storage_get_dump_selection(storage);
			    if (!dsl)
				continue;
			    for (; dsl != NULL ; dsl = dsl->next) {
				dump_selection_t *ds = dsl->data;
				if (ds->tag_type == TAG_NAME) {
				    if (g_str_equal(ds->tag, tags->data)) {
					found = TRUE;
					if (ds->level == LEVEL_ALL)
					    match_all++;
					else if (ds->level == LEVEL_FULL)
					    match_full++;
					else if (ds->level == LEVEL_INCR)
					    match_incr++;
				    }
				}
			    }
			}
			if (!found) {
			    delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800233, MSG_WARNING, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"tag", tags->data)));
			}
		    }
		    if (dp->to_holdingdisk == HOLD_NEVER &&
			(match_all+match_full > 1 ||
			 match_all+match_incr > 1)) {
			delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800234, MSG_WARNING, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
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

     delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__, 2800160, MSG_MESSAGE, 1,
					"seconds", walltime_str(curclock()))));

    fflush(outf);
    g_debug("userbad: %d", userbad);
    g_debug("confbad: %d", confbad);
    g_debug("tapebad: %d", tapebad);
    g_debug("disklow: %d", disklow);
    g_debug("logbad: %d", logbad);
    g_debug("infobad: %d", infobad);
    g_debug("indexbad: %d", indexbad);
    g_debug("pgmbad: %d", pgmbad);

    amcheck_exit(userbad \
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
FILE *client_outf;

static void handle_result(void *, pkt_t *, security_handle_t *);
void start_host(am_host_t *hostp);

#define HOST_READY				(0)	/* must be 0 */
#define HOST_ACTIVE				(1)
#define HOST_DONE				(2)

#define DISK_READY				(0)	/* must be 0 */
#define DISK_ACTIVE				(1)
#define DISK_DONE				(2)

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
    estimate_t estimate;
    GString *strbuf;

    if(hostp->status != HOST_READY) {
	return;
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
	    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800161, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
	}
	if(!am_has_feature(hostp->features, fe_selfcheck_rep)) {
	    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800163, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
	}
	if(!am_has_feature(hostp->features, fe_sendsize_req_options) &&
	   !am_has_feature(hostp->features, fe_sendsize_req_no_options) &&
	   !am_has_feature(hostp->features, fe_sendsize_req_device)) {
	    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800165, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
	}
	if(!am_has_feature(hostp->features, fe_sendsize_rep)) {
	    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800167, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
	}
	if(!am_has_feature(hostp->features, fe_sendbackup_req) &&
	   !am_has_feature(hostp->features, fe_sendbackup_req_device)) {
	    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800169, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
	}
	if(!am_has_feature(hostp->features, fe_sendbackup_rep)) {
	    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800171, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
	}

	g_snprintf(number, sizeof(number), "%d", hostp->maxdumps);
	req = g_strjoin(NULL, "SERVICE ", "selfcheck", "\n",
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
			has_config   ? get_config_name() : "",
			has_config   ? ";" : "",
			"\n",
			NULL);

	req_len = strlen(req);
	req_len += 128;                         /* room for SECURITY ... */
	req_len += 256;                         /* room for non-disk answers */
	for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	    char *l;
	    char *es;
	    size_t l_len;
	    char *o = NULL;
	    char *calcsize;
	    char *qname, *b64disk;
	    char *qdevice, *b64device = NULL;
	    gchar **errors;

	    if(dp->status != DISK_READY || dp->todo != 1) {
		continue;
	    }
	    qname = quote_string(dp->name);

	    errors = validate_optionstr(dp);

            if (errors) {
                gchar **ptr;
                for (ptr = errors; *ptr; ptr++)
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800173, MSG_ERROR, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"errstr"  , *ptr)));
                g_strfreev(errors);
		amfree(qname);
		remote_errors++;
		continue;
	    } else  if (am_has_feature(hostp->features, fe_req_xml)) {
		o = xml_optionstr(dp, 0);
	    } else {
		o = optionstr(dp);
	    }

	    b64disk = amxml_format_tag("disk", dp->name);
	    qdevice = quote_string(dp->device);
	    if (dp->device)
		b64device = amxml_format_tag("diskdevice", dp->device);
	    if ((qname[0] == '"') ||
		(dp->device && qdevice[0] == '"')) {
		if(!am_has_feature(hostp->features, fe_interface_quoted_text)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800174, MSG_ERROR, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"device"  , dp->device)));
		}
	    }

	    if(dp->device) {
		if(!am_has_feature(hostp->features, fe_selfcheck_req_device)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800176, MSG_ERROR, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"device"  , dp->device)));
		}
		if(!am_has_feature(hostp->features, fe_sendsize_req_device)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800178, MSG_ERROR, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"device"  , dp->device)));
		}
		if(!am_has_feature(hostp->features, fe_sendbackup_req_device)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800180, MSG_ERROR, 3,
					"hostname", hostp->hostname,
					"diskname", dp->name,
					"device"  , dp->device)));
		}

		if (dp->data_path != DATA_PATH_AMANDA &&
		    !am_has_feature(hostp->features, fe_xml_data_path)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800182, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"data-path", data_path_to_string(dp->data_path))));
		} else if (dp->data_path == DATA_PATH_DIRECTTCP &&
		    !am_has_feature(hostp->features, fe_xml_directtcp_list)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800183, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
		}
	    }
	    if (dp->program &&
	        (g_str_equal(dp->program, "DUMP") ||
	         g_str_equal(dp->program, "GNUTAR"))) {
		if(g_str_equal(dp->program, "DUMP") &&
		   !am_has_feature(hostp->features, fe_program_dump)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800184, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		}
		if(g_str_equal(dp->program, "GNUTAR") &&
		   !am_has_feature(hostp->features, fe_program_gnutar)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800186, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		}
		estimate = (estimate_t)GPOINTER_TO_INT(dp->estimatelist->data);
		if(estimate == ES_CALCSIZE &&
		   !am_has_feature(hostp->features, fe_calcsize_estimate)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800188, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		    estimate = ES_CLIENT;
		}
		if(estimate == ES_CALCSIZE &&
		   am_has_feature(hostp->features, fe_selfcheck_calcsize))
		    calcsize = "CALCSIZE ";
		else
		    calcsize = "";

		if(dp->compress == COMP_CUST &&
		   !am_has_feature(hostp->features, fe_options_compress_cust)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800190, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
		}
		if(dp->encrypt == ENCRYPT_CUST ) {
		  if ( !am_has_feature(hostp->features, fe_options_encrypt_cust)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800193, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
		    remote_errors++;
		  } else if ( dp->compress == COMP_SERVER_FAST ||
			      dp->compress == COMP_SERVER_BEST ||
			      dp->compress == COMP_SERVER_CUST ) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800195, MSG_ERROR, 1,
					"hostname", hostp->hostname)));
		    remote_errors++;
		  }
		}
		if (am_has_feature(hostp->features, fe_req_xml)) {
                    strbuf = g_string_new("<dle>\n");
                    es = xml_estimate(dp->estimatelist, hostp->features);
                    g_string_append_printf(strbuf,
                        "  <program>%s</program>\n"
                        "%s\n"
                        "  %s\n",
                        dp->program, es, b64disk
                    );
                    g_free(es);
                    if (dp->device)
                        g_string_append_printf(strbuf, "  %s\n", b64device);
                    g_string_append_printf(strbuf, "%s</dle>\n", o);
                    l = g_string_free(strbuf, FALSE);
		} else {
		    if (dp->device) {
			l = g_strjoin(NULL, calcsize,
				      dp->program, " ",
				      qname, " ",
				      qdevice,
				      " 0 OPTIONS |",
				      o,
				      "\n",
				      NULL);
		    } else {
			l = g_strjoin(NULL, calcsize,
				      dp->program, " ",
				      qname,
				      " 0 OPTIONS |",
				      o,
				      "\n",
				      NULL);
		    }
		}
	    } else {
		if (!am_has_feature(hostp->features, fe_program_application_api) ||
		    !am_has_feature(hostp->features, fe_req_xml)) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800196, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
		    remote_errors++;
		    l = g_strdup("");
		} else {
                    strbuf = g_string_new("<dle>\n  <program>APPLICATION</program>\n");
		    if (dp->application) {
			application_t *application = lookup_application(dp->application);

			if (!application) {
			    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800198, MSG_ERROR, 1,
					"application", dp->application)));
			} else {
			    char *xml_app = xml_application(dp, application, hostp->features);
			    char *client_name = application_get_client_name(application);
			    if (client_name && strlen(client_name) > 0 &&
				!am_has_feature(hostp->features, fe_application_client_name)) {
				delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800199, MSG_WARNING, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			    }
                            g_string_append(strbuf, xml_app);
			    g_free(xml_app);
			}
		    }

		    if (dp->pp_scriptlist) {
			if (!am_has_feature(hostp->features, fe_pp_script)) {
			   delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800200, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
			} else {
			    identlist_t pp_scriptlist;
			    for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
				pp_scriptlist = pp_scriptlist->next) {
				pp_script_t *pp_script = lookup_pp_script((char *)pp_scriptlist->data);
				char *client_name = pp_script_get_client_name(pp_script);;
				if (client_name && strlen(client_name) > 0 &&
				    !am_has_feature(hostp->features, fe_script_client_name)) {
				   delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800201, MSG_WARNING, 2,
					"hostname", hostp->hostname,
					"diskname", dp->name)));
				}
			    }
			}
		    }
		    es = xml_estimate(dp->estimatelist, hostp->features);
                    g_string_append_printf(strbuf, "%s\n  %s\n", es, b64disk);
		    g_free(es);

		    if (dp->device)
			g_string_append_printf(strbuf, "  %s\n", b64device);
                    g_string_append_printf(strbuf, "%s</dle>\n", o);
                    l = g_string_free(strbuf, FALSE);
		}
	    }
	    amfree(qname);
	    amfree(qdevice);
	    amfree(b64disk);
	    amfree(b64device);
	    l_len = strlen(l);
	    amfree(o);

	    strappend(req, l);
	    req_len += l_len;
	    amfree(l);
	    dp->status = DISK_ACTIVE;
	    disk_count++;
	}
    }
    else { /* noop service */
	req = g_strjoin(NULL, "SERVICE ", "noop", "\n",
			"OPTIONS ",
			"features=", our_feature_string, ";",
			"\n",
			NULL);
	for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	    if(dp->status != DISK_READY || dp->todo != 1) {
		continue;
	    }
	    disk_count++;
	}
    }

    if(disk_count == 0) {
	amfree(req);
	hostp->status = HOST_DONE;
	return;
    }

    secdrv = security_getdriver(hostp->disks->auth);
    if (secdrv == NULL) {
	delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800213, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"auth", hostp->disks->auth)));
    } else {
	protocol_sendreq(hostp->hostname, secdrv, amhost_get_security_conf,
			 req, conf_ctimeout, handle_result, hostp);
    }

    amfree(req);

    hostp->status = HOST_ACTIVE;
}

pid_t
start_client_checks(
    FILE *outf)
{
    am_host_t *hostp;
    GList     *dlist;
    disk_t *dp, *dp1;
    int hostcount;
    pid_t pid;
    int userbad = 0;

    switch(pid = fork()) {
    case -1:
	error("INTERNAL ERROR:could not fork client check: %s", strerror(errno));
	/*NOTREACHED*/

    case 0:
	break;

    default:
	return pid;
    }

    set_pname("amcheck-clients");

    client_outf = outf;
    startclock();

//    fprint_message(outf, 2800214, "\n");
    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800202, MSG_MESSAGE, 0)));
    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800203, MSG_MESSAGE, 0)));

    hostcount = remote_errors = 0;

    if (!who_check_host_setting) {
	remote_errors = check_host_setting(client_outf);
    }

    run_server_global_scripts(EXECUTE_ON_PRE_AMCHECK, get_config_name());
    protocol_init();

    for(dlist = origq.head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	hostp = dp->host;
	if(hostp->status == HOST_READY && dp->todo == 1) {
	    run_server_host_scripts(EXECUTE_ON_PRE_HOST_AMCHECK,
				    get_config_name(), hostp);
	    for(dp1 = hostp->disks; dp1 != NULL; dp1 = dp1->hostnext) {
		run_server_dle_scripts(EXECUTE_ON_PRE_DLE_AMCHECK,
				   get_config_name(), dp1, -1);
	    }
	    start_host(hostp);
	    hostcount++;
	    protocol_check();
	}
    }

    protocol_run();
    run_server_global_scripts(EXECUTE_ON_POST_AMCHECK, get_config_name());

    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800204, MSG_MESSAGE, 3,
					"hostcount", g_strdup_printf("%d", hostcount),
					"remote_errors", g_strdup_printf("%d", remote_errors),
					"seconds", walltime_str(curclock()))));
    fflush(outf);

    g_debug("userbad: %d", userbad);
    g_debug("remote_errors: %d", remote_errors);
    amcheck_exit(userbad || remote_errors > 0);
    /*NOTREACHED*/
    return 0;
}

static void amcheck_print_array_message(gpointer data, gpointer user_data);
static void
amcheck_print_array_message(
    gpointer data,
    gpointer user_data)
{
    FILE *stream = (FILE *)user_data;
    message_t *message = data;
    int severity = message_get_severity(message);

    if ((severity == MSG_SUCCESS ||
	 severity == MSG_INFO) &&
	client_verbose) {
	amcheck_fprint_message(stream, message);
    } else if (severity > MSG_INFO) {
	remote_errors++;
	amcheck_fprint_message(stream, message);
    }

    delete_message(message);
}

static void add_hostname_message(gpointer data, gpointer user_data);
static void
add_hostname_message(
    gpointer data,
    gpointer user_data)
{
    char *dle_hostname = (char *)user_data;
    message_t *message = data;

    message_add_argument(message, "dle_hostname", dle_hostname);
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
    gboolean printed_hostname = FALSE;
    char *message_buffer = NULL;

    hostp = (am_host_t *)datap;
    hostp->status = HOST_READY;

    if (pkt == NULL) {
	delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800206, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"errstr", security_geterror(sech))));
	remote_errors++;
	hostp->status = HOST_DONE;
	return;
    }

    s = pkt->body;
    ch = *s++;
    while(ch) {
	line = s - 1;

	skip_quoted_line(s, ch);
	if (s[-2] == '\n') {
	    s[-2] = '\0';
	}

	if(strncmp_const(line, "OPTIONS ") == 0) {

	    t = strstr(line, "features=");
	    if(t != NULL && (g_ascii_isspace((int)t[-1]) || t[-1] == ';')) {
		char *u = strchr(t, ';');
		if (u)
		   *u = '\0';
		t += sizeof("features=")-1;
		am_release_feature_set(hostp->features);
		if((hostp->features = am_string_to_feature(t)) == NULL) {
		    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800207, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"features", t)));
		    remote_errors++;
		    hostp->status = HOST_DONE;
		}
		if (u)
		   *u = ';';
	    }

	    continue;
	}

	if (client_verbose && !printed_hostname) {
	    delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800209, MSG_INFO, 1,
					"dle_hostname", hostp->hostname)));
	    printed_hostname = TRUE;
	}

	if (strncmp_const(line, "MESSAGE JSON") == 0) {
	    // line+13 is the complete buffer
	    message_buffer = g_strdup(line+13);
	    break;
	}

	if(strncmp_const(line, "OK ") == 0) {
	    if (client_verbose) {
		delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800210, MSG_INFO, 2,
					"dle_hostname", hostp->hostname,
					"ok_line", line+3)));
	    }
	    continue;
	}

	t = line;
	if(strncmp_const_skip(line, "ERROR ", t, tch) == 0) {
	    skip_whitespace(t, tch);
	    /*
	     * If the "error" is that the "noop" service is unknown, it
	     * just means the client is "old" (does not support the service).
	     * We can ignore this.
	     */
	    if(!((hostp->features == NULL) && (pkt->type == P_NAK)
	       && ((g_str_equal(t - 1, "unknown service: noop"))
		   || (g_str_equal(t - 1, "noop: invalid service"))))) {
		delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800211, MSG_ERROR, 3,
					"hostname", hostp->hostname,
					"type", (pkt->type == P_NAK) ? "NAK " : "",
					"errstr", t - 1)));
		remote_errors++;
		hostp->status = HOST_DONE;
	    }
	    continue;
	}

	delete_message(amcheck_fprint_message(client_outf, build_message(
					AMANDA_FILE, __LINE__, 2800212, MSG_ERROR, 2,
					"hostname", hostp->hostname,
					"errstr", line)));
	remote_errors++;
	hostp->status = HOST_DONE;
    }

    if (message_buffer) {
	/* parse message_buffer into a message_t array */
	GPtrArray *message_array = parse_json_message(message_buffer);

	/* add hostname to all messages */
	g_ptr_array_foreach(message_array, add_hostname_message, hostp->hostname);

	/* print and delete the messages */
	g_ptr_array_foreach(message_array, amcheck_print_array_message, client_outf);

	g_free(message_buffer);
	g_ptr_array_free(message_array, TRUE);
    }

    if(hostp->status == HOST_READY && hostp->features == NULL) {
	/*
	 * The client does not support the features list, so give it an
	 * empty one.
	 */
	g_debug("no feature set from host %s", hostp->hostname);
	hostp->features = am_set_default_feature_set();
    }
    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	if(dp->status == DISK_ACTIVE) {
	    dp->status = DISK_DONE;
	}
    }
    start_host(hostp);
    if(hostp->status == HOST_DONE) {
	security_close_connection(sech, hostp->hostname);
	for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	    run_server_dle_scripts(EXECUTE_ON_POST_DLE_AMCHECK,
			       get_config_name(), dp, -1);
	}
	run_server_host_scripts(EXECUTE_ON_POST_HOST_AMCHECK,
				get_config_name(), hostp);
    }
    /* try to clean up any defunct processes, since Amanda doesn't wait() for
       them explicitly */
    while(waitpid(-1, NULL, WNOHANG)> 0);
}

static int
check_host_setting(
    FILE *outf)
{
    am_host_t *p;
    disk_t *dp;
    int count = 0;

    for (p = get_hostlist(); p != NULL; p = p->next) {
	for(dp = p->disks; dp != NULL; dp = dp->hostnext) {
	    if (strcmp(dp->auth, p->disks->auth) != 0) {
		delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__,
					2800231, MSG_ERROR, 3,
					"hostname", p->hostname,
					"auth1", p->disks->auth,
					"auth2", dp->auth)));
		count++;
		break;
	    }
	}
	for(dp = p->disks; dp != NULL; dp = dp->hostnext) {
	    if (dp->maxdumps != p->disks->maxdumps) {
		delete_message(amcheck_fprint_message(outf, build_message(
					AMANDA_FILE, __LINE__,
					2800232, MSG_ERROR, 1,
					"hostname", p->hostname)));
		count++;
		break;
	    }
	}
    }
    return count;
}
