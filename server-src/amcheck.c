/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
#include "physmem.h"
#include <getopt.h>

#define BUFFER_SIZE	32768

static time_t conf_ctimeout;
static int overwrite;

static disklist_t origq;

static uid_t uid_dumpuser;

/* local functions */

void amcheck_exit(int status);
void usage(void);
pid_t start_client_checks(FILE *fd);
pid_t start_server_check(FILE *fd, int do_localchk, int do_tapechk);
int main(int argc, char **argv);
int check_tapefile(FILE *outf, char *tapefile);
int test_server_pgm(FILE *outf, char *dir, char *pgm, int suid, uid_t dumpuid);

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

char *encode_json(char *str);

static char encoded_json[4096];
char *
encode_json(
    char *str)
{
    int i = 0;
    char *s = str;
    char *e = encoded_json;
    while(*s != '\0') {
	if (i++ >= 4094) {
	    error("encode_json: str is too long: %s", str);
	}
	if (*s == '\\' || *s == '"')
	    *e++ = '\\';
	*e++ = *s++;
    }
    *e = '\0';
    return encoded_json;
}

#define print_message(code, msg) if (opt_message) g_printf( \
"  {\n" \
"    \"source_filename\" : \"" __FILE__ "\",\n" \
"    \"source_line\" : \"%d\",\n" \
"    \"severity\" : \"16\",\n" \
"    \"code\" : \"%d\",\n" \
"    \"message\" : \"%s\"\n" \
"  },\n", __LINE__, code, encode_json(msg)); else \
g_printf("%s\n", msg);

#define fprint_message(file, code, msg) if (opt_message) g_fprintf(file, \
"  {\n" \
"    \"source_filename\" : \"" __FILE__ "\",\n" \
"    \"source_line\" : \"%d\",\n" \
"    \"severity\" : \"16\",\n" \
"    \"code\" : \"%d\",\n" \
"    \"message\" : \"%s\"\n" \
"  },\n", __LINE__, code, encode_json(msg)); else \
g_fprintf(file, "%s\n", msg);

#define printf_message(code, msg, ...) { \
  char *msg1 = g_strdup_printf(msg, __VA_ARGS__); \
  if (opt_message) g_printf( \
"  {\n" \
"    \"source_filename\" : \"" __FILE__ "\",\n" \
"    \"source_line\" : \"%d\",\n" \
"    \"severity\" : \"16\",\n" \
"    \"code\" : \"%d\",\n" \
"    \"message\" : \"%s\"\n" \
"  },\n", __LINE__, code, encode_json(msg1)); else \
g_printf("%s\n", msg1); \
g_free(msg1); \
}
#define fprintf_message(file, code, msg, ...) { \
  char *msg1 = g_strdup_printf(msg, __VA_ARGS__); \
  if (opt_message) g_fprintf(file, \
"  {\n" \
"    \"source_filename\" : \"" __FILE__ "\",\n" \
"    \"source_line\" : \"%d\",\n" \
"    \"severity\" : \"16\",\n" \
"    \"code\" : \"%d\",\n" \
"    \"message\" : \"%s\"\n" \
"  },\n", __LINE__, code, encode_json(msg1)); else \
g_fprintf(file, "%s\n", msg1); \
g_free(msg1); \
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
    print_message(2800000, "Usage: amcheck [--version] [-am] [-w] [-sclt] [-M <address>] [--client-verbose] [--exact_match] [-o configoption]* <conf> [host [disk]* ]*");

    amcheck_exit(1);
    /*NOTREACHED*/
}

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
	case 2:		print_message(2800001, g_strdup_printf("amcheck-%s", VERSION));
			return(0);
			break;
	case 3:		exact_match = TRUE;
			break;
	case 4:		opt_message = TRUE;
			break;
	case 'M':	if (mailto) {
			    print_message(2800002, "Multiple -M options");
			    amcheck_exit(1);
			}
			mailto=g_strdup(optarg);
			if(!validate_mailto(mailto)){
			   print_message(2800003, "Invalid characters in mail address");
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
	    print_message(2800228, "errors processing config file");
	    exit(1);
	    //g_critical("errors processing config file");
	}
    }

    mailer = getconf_str(CNF_MAILER);
    if ((!mailer || *mailer == '\0') && mailout == 1) {
	if (alwaysmail == 1) {
	    print_message(2800004, "You can't use -a because a mailer is not defined");
	} else {
	    print_message(2800005, "You can't use -m because a mailer is not defined");
	}
	amcheck_exit(1);
    }
    if(mailout && !mailto &&
       (getconf_seen(CNF_MAILTO)==0 || strlen(getconf_str(CNF_MAILTO)) == 0)) {
	print_message(2800006, "WARNING:No mail address configured in  amanda.conf.");
	print_message(2800007, "To receive dump results by email configure the "
		 "\"mailto\" parameter in amanda.conf");
        if (alwaysmail) {
	    print_message(2800008, "When using -a option please specify -Maddress also");
	} else {
	    print_message(2800009, "Use -Maddress instead of -m");
	}
	amcheck_exit(1);
    }
    if(mailout && !mailto)
    {
       if(getconf_seen(CNF_MAILTO) &&
          strlen(getconf_str(CNF_MAILTO)) > 0) {
          if(!validate_mailto(getconf_str(CNF_MAILTO))){
		print_message(2800010, "Mail address in amanda.conf has invalid characters");
		print_message(2800011, "No email will be sent");
                mailout = 0;
          }
       }
       else {
	  print_message(2800012, "No mail address configured in  amanda.conf");
          if (alwaysmail) {
		print_message(2800013, "When using -a option please specify -Maddress also");
	  } else {
		print_message(2800014, "Use -Maddress instead of -m");
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
	    print_message(2800015, errstr);
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
	printf_message(2800215, "amanda.conf has dump user configured to \"%s\", but that user does not exist.", dumpuser);
	exit(1);
	/*NOTREACHED*/
    }
    uid_dumpuser = pw->pw_uid;
    if (getpwuid(uid_me) == NULL) {
	printf_message(2800216, "cannot get username for running user, uid %ld is not in your user database.",
	    (long)uid_me);
	exit(1);
	/*NOTREACHED*/
    }
#ifdef CHECK_USERID
    if (uid_me != uid_dumpuser) {
	printf_message(2800217, "running as user \"%s\" instead of \"%s\".\n"
		"Change user to \"%s\" or change dump user to \"%s\" in amanda.conf",
	      pw->pw_name, dumpuser, dumpuser, pw->pw_name);
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
	    printf_message(2800218, "could not open temporary amcheck output file %s: %s. Check permissions", tempfname, strerror(errno));
	    exit(1);
	    /*NOTREACHED*/
	}
    }

    if(mailout) {
	/* the main fd is a file too */
	mainfname = g_strjoin(NULL, AMANDA_TMPDIR, "/amcheck.main.", pid_str, NULL);
	if((mainfd = fopen(mainfname, "w+")) == NULL) {
	    printf_message(2800219, "could not open amcheck server output file %s: %s. Check permissions", mainfname, strerror(errno));
	    exit(1);
	    /*NOTREACHED*/
	}
	unlink(mainfname);			/* so it goes away on close */
	amfree(mainfname);
    }
    else
	/* just use stdout */
	mainfd = stdout;

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
	    fprintf_message(mainfd, 2800021, "parent: reaped bogus pid %ld", (long)pid);
	}
    }

    /* copy temp output to main output and write tagline */

    if (tempfd) {
	char line[1025];
	FILE *tempfdr;
	tempfdr = fopen(tempfname, "r");
	if(fseek(tempfdr, (off_t)0, 0) == (off_t)-1) {
	    printf_message(2800220, "seek temp file: %s", strerror(errno));
	    /*NOTREACHED*/
	}

	while (fgets(line, 1024, tempfdr)) {
	    fprintf(mainfd, "%s", line);
	}
	fclose(tempfdr);
	fclose(tempfd);
	unlink(tempfname);			/* so it goes away on close */
	amfree(tempfname);
    }

    version_string = g_strdup_printf("(brought to you by Amanda %s)", VERSION);
    print_message(2800016, version_string);
    amfree(version_string);

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
	    printf_message(2800221, "fseek main file: %s", strerror(errno));
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
	    a[1] = g_strdup(mailto);
	    a[2] = NULL;
	} else {
	    /* (note that validate_mailto doesn't allow any quotes, so this
	     * is really just splitting regular old strings) */
	    a = split_quoted_strings(getconf_str(CNF_MAILTO));
	    if (!a) {
		print_message(2800016, g_strdup_printf("Invalid mailto address '%s'", getconf_str(CNF_MAILTO)));
		amcheck_exit(1);
	    }
	}
	if((nullfd = open("/dev/null", O_RDWR)) < 0) {
	    printf_message(2800227, "nullfd: /dev/null: %s", strerror(errno));
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
	    print_message(2800017, g_strdup_printf("Invalid mailto address '%s'", getconf_str(CNF_MAILTO)));
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
		    printf_message(2800222, "mailfd write: %s", strerror(errno));
		    exit(1);
		    /*NOTREACHED*/
		} else {
		    printf_message(2800223, "mailfd write: wrote %zd instead of %zd", w, r);
		    exit(1);
		    /*NOTREACHED*/
		}
	    }
	}
	aclose(mailfd);
	ferr = fdopen(errfd, "r");
	if (!ferr) {
	    printf_message(2800224, "Can't fdopen: %s", strerror(errno));
	    exit(1);
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
	    printf_message(2800225, "error running mailer %s: %s", mailer, err?err:"(unknown)");
	    exit(1);
	    /*NOTREACHED*/
	} else {
	    amfree(extra_info);
	}
    }

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
    char *quoted;
    int tapebad = 0;
    char *s;

    if (stat(tapefile, &statbuf) == 0) {
	if (!S_ISREG(statbuf.st_mode)) {
	    quoted = quote_string(tapefile);
	    s = g_strdup_printf("ERROR: tapelist %s: should be a regular file.", quoted);
	    fprint_message(outf, 2800018, s);
	    tapebad = 1;
	    amfree(s);
	    amfree(quoted);
	} else if (access(tapefile, F_OK) != 0) {
	    quoted = quote_string(tapefile);
	    s = g_strdup_printf("ERROR: can't access tapelist %s", quoted);
	    fprint_message(outf, 2800019, s);
	    tapebad = 1;
	    amfree(s);
	    amfree(quoted);
	} else if (access(tapefile, W_OK) != 0) {
	    quoted = quote_string(tapefile);
	    s = g_strdup_printf("ERROR: tapelist %s: not writable", quoted);
	    fprint_message(outf, 2800020, s);
	    tapebad = 1;
	    amfree(s);
	    amfree(quoted);
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
    char *quoted;

    pgm = g_strjoin(NULL, dir, "/", pgm, NULL);
    quoted = quote_string(pgm);
    if(stat(pgm, &statbuf) == -1) {
	fprintf_message(outf, 2800022, "ERROR: program %s: does not exist",
		quoted);
	pgmbad = 1;
    } else if (!S_ISREG(statbuf.st_mode)) {
	fprintf_message(outf, 2800023, "ERROR: program %s: not a file",
		quoted);
	pgmbad = 1;
    } else if (access(pgm, X_OK) == -1) {
	fprintf_message(outf, 2800024, "ERROR: program %s: not executable",
		quoted);
	pgmbad = 1;
#ifndef SINGLE_USERID
    } else if (suid \
	       && dumpuid != 0
	       && (statbuf.st_uid != 0 || (statbuf.st_mode & 04000) == 0)) {
	fprintf_message(outf, 2800025, "ERROR: program %s: not setuid-root",
		quoted);
	pgmbad = 1;
#else
    /* Quiet unused parameter warnings */
    (void)suid;
    (void)dumpuid;
#endif /* SINGLE_USERID */
    }
    amfree(quoted);
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
		    fprintf_message(outf, 123, "%s", line);
		}
		g_free(line);
	    }
	    fclose(dev_outf);
	}

	/* and immediately wait for it to die */
	waitpid(devpid, &wait_status, 0);

	if (WIFSIGNALED(wait_status)) {
	    fprintf_message(outf, 2800026, "amcheck-device terminated with signal %d",
		      WTERMSIG(wait_status));
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
    char *quoted;
    int res;
    intmax_t kb_avail, kb_needed;
    off_t tape_size;
    gboolean printed_small_part_size_warning = FALSE;
    char *small_part_size_warning =
	" This may create > 1000 parts, severely degrading backup/restore performance."
	" See http://wiki.zmanda.com/index.php/Splitsize_too_small for more information.";

    switch(pid = fork()) {
    case -1:
	printf_message(2800226, "could not spawn a process for checking the server: %s", strerror(errno));
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

    fprint_message(outf, 2800027, "Amanda Tape Server Host Check");
    fprint_message(outf, 2800028, "-----------------------------");

    if (do_localchk || testtape) {
	identlist_t il;
	for (il = getconf_identlist(CNF_STORAGE); il != NULL; il = il->next) {
	    char *storage_n = il->data;
	    policy_t *policy;
	    char *policy_n;
	    char *lbl_templ;

	    storage = lookup_storage(storage_n);
	    if (storage)
		tp = lookup_tapetype(storage_get_tapetype(storage));

	    lbl_templ = tapetype_get_lbl_templ(tp);
	    if (!g_str_equal(lbl_templ, "")) {
		lbl_templ = config_dir_relative(lbl_templ);
		if (access(lbl_templ, R_OK) == -1) {
		    fprintf_message(outf, 2800029,
			"ERROR: storage %s: cannot read label template (lbl-templ) file %s: %s. Check permissions",
			storage_n,
			lbl_templ,
			strerror(errno));
		    confbad = 1;
		}
		amfree(lbl_templ);
#if !defined(HAVE_LPR_CMD)
		fprintf_message(outf, 2800030, "ERROR: storage %s: lbl-templ set but no LPR command defined. You should reconfigure amanda and make sure it finds a lpr or lp command.", storage_n);
		confbad = 1;
#endif
	    }

	    if (storage_get_flush_threshold_scheduled(storage)
		< storage_get_flush_threshold_dumped(storage)) {
		fprintf_message(outf, 2800031, "WARNING: storage %s: flush-threshold-dumped (%d) must be less than or equal to flush-threshold-scheduled (%d).",
			  storage_n,
			  storage_get_flush_threshold_dumped(storage),
			  storage_get_flush_threshold_scheduled(storage));
	    }

	    if (storage_get_flush_threshold_scheduled(storage)
		< storage_get_taperflush(storage)) {
		fprintf_message(outf, 2800032, "WARNING: storage %s: taperflush (%d) must be less than or equal to flush-threshold-scheduled (%d).",
			  storage_n,
			  storage_get_taperflush(storage),
			  storage_get_flush_threshold_scheduled(storage));
	    }

	    if (storage_get_taperflush(storage) &&
	        !storage_get_autoflush(storage)) {
	        fprintf_message(outf, 2800033, "WARNING: storage %s: autoflush must be set to 'yes' or 'all' if taperflush (%d) is greater that 0.",
			  storage_n,
			  storage_get_taperflush(storage));
	    }

	    if (!storage_seen(storage, STORAGE_TAPETYPE)) {
		fprintf_message(outf, 2800034,
			  "ERROR: storage %s: no tapetype specified; you must give a value for "
			  "the 'tapetype' parameter or the storage",
			  storage_n);
		confbad = 1;
	    }

	    policy_n = storage_get_policy(storage);
	    policy = lookup_policy(policy_n);
	    if (policy_get_retention_tapes(policy) <= storage_get_runtapes(storage)) {
		fprintf_message(outf, 2800035,
			  "ERROR: storage %s: runtapes is larger or equal to policy '%s' retention-tapes",
			  storage_n, policy_n);
	    }

	    {
		uintmax_t kb_avail = physmem_total() / 1024;
		uintmax_t kb_needed = storage_get_device_output_buffer_size(storage) / 1024;
		if (kb_avail < kb_needed) {
		    fprintf_message(outf, 2800036,
			"ERROR: system has %ju %sB memory, but device-output-buffer-size needs %ju %sB",
			kb_avail/(uintmax_t)unitdivisor, displayunit,
			kb_needed/(uintmax_t)unitdivisor, displayunit);
		}
	    }
	}

	/* Double-check that 'localhost' resolves properly */
	if ((res = resolve_hostname("localhost", 0, NULL, NULL) != 0)) {
	    fprintf_message(outf, 2800037, "ERROR: Cannot resolve `localhost': %s", gai_strerror(res));
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
	    quoted = quote_string(amlibexecdir);
	    fprintf_message(outf, 2800038, "ERROR: Directory %s containing Amanda tools is not accessible.",
		    quoted);
	    fprint_message(outf, 2800039, "Check permissions");
	    pgmbad = 1;
	    amfree(quoted);
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
	    quoted = quote_string(sbindir);
	    fprintf_message(outf, 2800040, "ERROR: Directory %s containing Amanda tools is not accessible",
		    sbindir);
	    fprint_message(outf, 2800041, "Check permissions");
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
	    fprintf_message(outf, 2800042, "WARNING: %s is not executable, server-compression "
			    "and indexing will not work.",quoted);
	    fprint_message(outf, 2800043, "Check permissions");
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
	    quoted = quote_string(tape_dir);
	    fprintf_message(outf, 2800044, "ERROR: tapelist dir %s: not writable.\nCheck permissions",
		    quoted);
	    tapebad = 1;
	    amfree(quoted);
	}
	else if(stat(tapefile, &statbuf) == -1) {
	    if (errno != ENOENT) {
		quoted = quote_string(tape_dir);
		fprintf_message(outf, 2800045, "ERROR: tapelist %s (%s), "
			"you must create an empty file.",
			quoted, strerror(errno));
		tapebad = 1;
		amfree(quoted);
	    } else {
		fprint_message(outf, 2800046, "NOTE: tapelist will be created on the next run.");
	    }
	} else {
	    tapebad |= check_tapefile(outf, tapefile);
	    if (tapebad == 0 && read_tapelist(tapefile)) {
		quoted = quote_string(tapefile);
		fprintf_message(outf, 2800047, "ERROR: tapelist %s: parse error", quoted);
		tapebad = 1;
		amfree(quoted);
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
	    quoted = quote_string(holdfile);
	    fprintf_message(outf, 2800048, "WARNING: hold file %s exists.", holdfile);
	    fprint_message(outf, 2800049, "Amdump will sleep as long as this file exists.");
	    fprint_message(outf, 2800050, "You might want to delete the existing hold file");
	    amfree(quoted);
	}
	amfree(tapefile);
	amfree(tape_dir);
	amfree(holdfile);
	tapename = getconf_str(CNF_TAPEDEV);
	if (tapename == NULL) {
	    if (getconf_str(CNF_TPCHANGER) == NULL &&
		getconf_identlist(CNF_STORAGE) == NULL) {
		fprint_message(outf, 2800051, "WARNING:Parameter \"tapedev\", \"tpchanger\" or storage not specified in amanda.conf.");
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
		fprint_message(outf, 2800052, "ERROR: part-cache-type specified, but no part-size");
		tapebad = 1;
	    }
	    if (tapetype_seen(tp, TAPETYPE_PART_CACHE_DIR)) {
		fprint_message(outf, 2800053, "ERROR: part-cache-dir specified, but no part-size");
		tapebad = 1;
	    }
	    if (tapetype_seen(tp, TAPETYPE_PART_CACHE_MAX_SIZE)) {
		fprint_message(outf, 2800054, "ERROR: part-cache-max-size specified, but no part-size");
		tapebad = 1;
	    }
	} else {
	    switch (part_cache_type) {
	    case PART_CACHE_TYPE_DISK:
		if (!tapetype_seen(tp, TAPETYPE_PART_CACHE_DIR)
			    || !part_cache_dir || !*part_cache_dir) {
		    fprint_message(outf, 2800055,
			"ERROR: part-cache-type is DISK, but no part-cache-dir specified");
		    tapebad = 1;
		} else {
		    if(get_fs_usage(part_cache_dir, NULL, &fsusage) == -1) {
			fprintf_message(outf, 2800056, "ERROR: part-cache-dir '%s': %s",
				part_cache_dir, strerror(errno));
			tapebad = 1;
		    } else {
			kb_avail = fsusage.fsu_bavail_top_bit_set?
			    0 : fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;
			kb_needed = part_size;
			if (tapetype_seen(tp, TAPETYPE_PART_CACHE_MAX_SIZE)) {
			    kb_needed = part_cache_max_size;
			}
			if (kb_avail < kb_needed) {
			    fprintf_message(outf, 2800057,
				"ERROR: part-cache-dir has %ju %sB available, but needs %ju %sB",
				kb_avail/(uintmax_t)unitdivisor, displayunit,
				kb_needed/(uintmax_t)unitdivisor, displayunit);
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
		    fprintf_message(outf, 2800058,
			"ERROR: system has %ju %sB memory, but part cache needs %ju %sB",
			kb_avail/(uintmax_t)unitdivisor, displayunit,
			kb_needed/(uintmax_t)unitdivisor, displayunit);
		    tapebad = 1;
		}

		/* FALL THROUGH */

	    case PART_CACHE_TYPE_NONE:
		if (tapetype_seen(tp, TAPETYPE_PART_CACHE_DIR)) {
		    fprint_message(outf, 2800059,
			"ERROR: part-cache-dir specified, but part-cache-type is not DISK");
		    tapebad = 1;
		}
		break;
	    }
	}

	if (tapetype_seen(tp, TAPETYPE_PART_SIZE) && part_size == 0
		&& part_cache_type != PART_CACHE_TYPE_NONE) {
	    fprint_message(outf, 2800060,
		    "ERROR: part_size is zero, but part-cache-type is not 'none'");
	    tapebad = 1;
	}

	if (tapetype_seen(tp, TAPETYPE_PART_CACHE_MAX_SIZE)) {
	    if (part_cache_type == PART_CACHE_TYPE_NONE) {
		fprint_message(outf, 2800061,
		    "ERROR: part-cache-max-size is specified but no part cache is in use");
		tapebad = 1;
	    }

	    if (part_cache_max_size > part_size) {
		fprint_message(outf, 2800062,
		    "WARNING: part-cache-max-size is greater than part-size");
	    }
	}

	tape_size = tapetype_get_length(tp);
	if (part_size && part_size * 1000 < tape_size) {
	    fprintf_message(outf, 2800063,
		      "WARNING: part-size of %ju %sB < 0.1%% of tape length.",
		      (uintmax_t)part_size/(uintmax_t)unitdivisor, displayunit);
	    if (!printed_small_part_size_warning) {
		printed_small_part_size_warning = TRUE;
		fprintf_message(outf, 2800064, "%s", small_part_size_warning);
	    }
	} else if (part_cache_max_size && part_cache_max_size * 1000 < tape_size) {
	    fprintf_message(outf, 2800065,
		      "WARNING: part-cache-max-size of %ju %sB < 0.1%% of tape length.",
		      (uintmax_t)part_cache_max_size/(uintmax_t)unitdivisor, displayunit);
	    if (!printed_small_part_size_warning) {
		printed_small_part_size_warning = TRUE;
		fprintf_message(outf, 2800064, "%s", small_part_size_warning);
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
	    quoted = quote_string(holdingdisk_get_diskdir(hdp));
	    if(get_fs_usage(holdingdisk_get_diskdir(hdp), NULL, &fsusage) == -1) {
		fprintf_message(outf, 2800066, "ERROR: holding dir %s (%s), "
			"you must create a directory.",
			quoted, strerror(errno));
		disklow = 1;
		amfree(quoted);
		continue;
	    }

	    /* do the division first to avoid potential integer overflow */
	    if (fsusage.fsu_bavail_top_bit_set)
		kb_avail = 0;
	    else
		kb_avail = fsusage.fsu_bavail / 1024 * fsusage.fsu_blocksize;

	    if(access(holdingdisk_get_diskdir(hdp), W_OK) == -1) {
		fprintf_message(outf, 2800067, "ERROR: holding disk %s: not writable: %s.",
			quoted, strerror(errno));
		fprint_message(outf, 2800068, "Check permissions");
		disklow = 1;
	    }
	    else if(access(holdingdisk_get_diskdir(hdp), X_OK) == -1) {
		fprintf_message(outf, 2800069, "ERROR: holding disk %s: not searcheable: %s.",
			quoted, strerror(errno));
		fprintf_message(outf, 2800070, "Check permissions of ancestors of %s", quoted);
		disklow = 1;
	    }
	    else if(holdingdisk_get_disksize(hdp) > (off_t)0) {
		if(kb_avail == 0) {
		    fprintf_message(outf, 2800071,
			    "WARNING: holding disk %s: "
			    "no space available (%lld %sB requested)", quoted,
			    (long long)(holdingdisk_get_disksize(hdp)/(off_t)unitdivisor),
			    displayunit);
		    disklow = 1;
		}
		else if(kb_avail < holdingdisk_get_disksize(hdp)) {
		    fprintf_message(outf, 2800072,
			    "WARNING: holding disk %s: "
			    "only %lld %sB available (%lld %sB requested)", quoted,
			    (long long)(kb_avail / (off_t)unitdivisor),
			    displayunit,
			    (long long)(holdingdisk_get_disksize(hdp)/(off_t)unitdivisor),
			    displayunit);
		    disklow = 1;
		}
		else {
		    fprintf_message(outf, 2800073,
			    "Holding disk %s: %lld %sB disk space available,"
			    " using %lld %sB as requested",
			    quoted,
			    (long long)(kb_avail / (off_t)unitdivisor),
			    displayunit,
			    (long long)(holdingdisk_get_disksize(hdp)/(off_t)unitdivisor),
			    displayunit);
		}
	    }
	    else {
		if(kb_avail < -holdingdisk_get_disksize(hdp)) {
		    fprintf_message(outf, 2800074,
			    "WARNING: holding disk %s: "
			    "only %lld %sB free, using nothing",
			    quoted, (long long)(kb_avail / (off_t)unitdivisor),
			    displayunit);
		    fprint_message(outf, 2800075, "WARNING: Not enough free space specified in amanda.conf");
		    disklow = 1;
		}
		else {
		    fprintf_message(outf, 2800076,
			    "Holding disk %s: %lld %sB disk space available, using %lld %sB",
			    quoted,
			    (long long)(kb_avail/(off_t)unitdivisor),
			    displayunit,
			    (long long)((kb_avail + holdingdisk_get_disksize(hdp)) / (off_t)unitdivisor),
			    displayunit);
		}
	    }
	    amfree(quoted);
	}
    }

    /* check that the log file is writable if it already exists */

    if(do_localchk) {
	char *conf_logdir;
	char *olddir;
	struct stat stat_old;
	struct stat statbuf;

	conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));

	quoted = quote_string(conf_logdir);
	if(stat(conf_logdir, &statbuf) == -1) {
	    fprintf_message(outf, 2800077, "ERROR: logdir %s (%s), you must create directory.",
		    quoted, strerror(errno));
	    disklow = 1;
	}
	else if(access(conf_logdir, W_OK) == -1) {
	    fprintf_message(outf, 2800078, "ERROR: log dir %s: not writable", quoted);
	    logbad = 1;
	}
	amfree(quoted);

	olddir = g_strjoin(NULL, conf_logdir, "/oldlog", NULL);
	quoted = quote_string(olddir);
	if (logbad == 0 && stat(olddir,&stat_old) == 0) { /* oldlog exist */
	    if(!(S_ISDIR(stat_old.st_mode))) {
		fprintf_message(outf, 2800079, "ERROR: oldlog directory %s is not a directory",
			quoted);
		fprint_message(outf, 2800080, "Remove the entry and create a new directory");
		logbad = 1;
	    }
	    if(logbad == 0 && access(olddir, W_OK) == -1) {
		fprintf_message(outf, 2800081, "ERROR: oldlog dir %s: not writable", quoted);
		fprint_message(outf, 2800082, "Check permissions");
		logbad = 1;
	    }
	}
	else if(logbad == 0 && lstat(olddir,&stat_old) == 0) {
	    fprintf_message(outf, 2800083, "ERROR: oldlog directory %s is not a directory",
		    quoted);
	    fprint_message(outf, 2800084, "Remove the entry and create a new directory");
	    logbad = 1;
	}
	amfree(quoted);

	amfree(olddir);
	amfree(conf_logdir);
    }

    if (testtape) {
        tapebad = !test_tape_status(outf);
    } else if (do_tapechk) {
	fprint_message(outf, 2800085, "WARNING: skipping tape test because amdump or amflush seem to be running");
	fprint_message(outf, 2800086, "WARNING: if they are not, you must run amcleanup");
	dev_amanda_data_path = TRUE;
	dev_directtcp_data_path = TRUE;
    } else if (logbad == 2) {
	fprint_message(outf, 2800087, "NOTE: amdump or amflush seem to be running");
	fprint_message(outf, 2800088, "NOTE: if they are not, you must run amcleanup");

	/* we skipped the tape checks, but this is just a NOTE and
	 * should not result in a nonzero exit status, so reset logbad to 0 */
	logbad = 0;
	dev_amanda_data_path = TRUE;
	dev_directtcp_data_path = TRUE;
    } else {
	fprint_message(outf, 2800089, "NOTE: skipping tape checks");
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
		fprintf_message(outf, 2800090, "WARNING: tapecycle (%d) <= runspercycle (%d).",
			conf_tapecycle, conf_runspercycle);
	}

	if (conf_tapecycle <= conf_runtapes) {
		fprintf_message(outf, 2800091, "WARNING: tapecycle (%d) <= runtapes (%d).",
			conf_tapecycle, conf_runtapes);
	}

	conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
	conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));

	quoted = quote_string(conf_infofile);
	if(stat(conf_infofile, &statbuf) == -1) {
	    if (errno == ENOENT) {
		fprintf_message(outf, 2800092, "NOTE: conf info dir %s does not exist",
			quoted);
		fprint_message(outf, 2800093, "NOTE: it will be created on the next run.");
	    } else {
		fprintf_message(outf, 2800094, "ERROR: conf info dir %s (%s)",
			quoted, strerror(errno));
		infobad = 1;
	    }
	    amfree(conf_infofile);
	} else if (!S_ISDIR(statbuf.st_mode)) {
	    fprintf_message(outf, 2800095, "ERROR: info dir %s: not a directory", quoted);
	    fprint_message(outf, 2800096, "Remove the entry and create a new directory");
	    amfree(conf_infofile);
	    infobad = 1;
	} else if (access(conf_infofile, W_OK) == -1) {
	    fprintf_message(outf, 2800097, "ERROR: info dir %s: not writable", quoted);
	    fprint_message(outf, 2800098, "Check permissions");
	    amfree(conf_infofile);
	    infobad = 1;
	} else {
	    char *errmsg = NULL;
	    if (check_infofile(conf_infofile, &origq, &errmsg) == -1) {
		fprintf_message(outf, 2800099, "ERROR: Can't copy infofile: %s", errmsg);
		infobad = 1;
		amfree(errmsg);
	    }
	    strappend(conf_infofile, "/");
	}
	amfree(quoted);

	while(!empty(origq)) {
	    hostp = ((disk_t *)origq.head->data)->host;
	    host = sanitise_filename(hostp->hostname);
	    if(conf_infofile) {
		g_free(hostinfodir);
		hostinfodir = g_strconcat(conf_infofile, host, NULL);
		quoted = quote_string(hostinfodir);
		if(stat(hostinfodir, &statbuf) == -1) {
		    if (errno == ENOENT) {
			fprintf_message(outf, 2800100, "NOTE: host info dir %s does not exist",
				quoted);
			fprint_message(outf, 2800101,
				"NOTE: it will be created on the next run.");
		    } else {
			fprintf_message(outf, 2800102, "ERROR: host info dir %s (%s)",
				quoted, strerror(errno));
			infobad = 1;
		    }
		    amfree(hostinfodir);
		} else if (!S_ISDIR(statbuf.st_mode)) {
		    fprintf_message(outf, 2800103, "ERROR: info dir %s: not a directory",
			    quoted);
		    fprint_message(outf, 2800104, "Remove the entry and create a new directory");
		    amfree(hostinfodir);
		    infobad = 1;
		} else if (access(hostinfodir, W_OK) == -1) {
		    fprintf_message(outf, 2800105, "ERROR: info dir %s: not writable", quoted);
		    fprint_message(outf, 2800106, "Check permissions");
		    amfree(hostinfodir);
		    infobad = 1;
		} else {
		    strappend(hostinfodir, "/");
		}
		amfree(quoted);
	    }
	    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
		disk = sanitise_filename(dp->name);
		if(hostinfodir) {
		    char *quotedif;

		    g_free(diskdir);
		    diskdir = g_strconcat(hostinfodir, disk, NULL);
		    infofile = g_strjoin(NULL, diskdir, "/", "info", NULL);
		    quoted = quote_string(diskdir);
		    quotedif = quote_string(infofile);
		    if(stat(diskdir, &statbuf) == -1) {
			if (errno == ENOENT) {
			    fprintf_message(outf, 2800107, "NOTE: info dir %s does not exist",
				quoted);
			    fprint_message(outf, 2800108,
				"NOTE: it will be created on the next run.");
			} else {
			    fprintf_message(outf, 2800109, "ERROR: info dir %s (%s)",
				    quoted, strerror(errno));
			    infobad = 1;
			}
		    } else if (!S_ISDIR(statbuf.st_mode)) {
			fprintf_message(outf, 2800110, "ERROR: info dir %s: not a directory",
				quoted);
			fprint_message(outf, 2800111, "Remove the entry and create a new directory");
			infobad = 1;
		    } else if (access(diskdir, W_OK) == -1) {
			fprintf_message(outf, 2800112, "ERROR: info dir %s: not writable",
				quoted);
			fprint_message(outf, 2800113, "Check permissions");
			infobad = 1;
		    } else if(stat(infofile, &statbuf) == -1) {
			if (errno == ENOENT) {
			    fprintf_message(outf, 2800114, "NOTE: info file %s does not exist",
				    quotedif);
			    fprint_message(outf, 2800115, "NOTE: it will be created on the next run.");
			} else {
			    fprintf_message(outf, 2800116, "ERROR: info dir %s (%s)",
				    quoted, strerror(errno));
			    infobad = 1;
			}
		    } else if (!S_ISREG(statbuf.st_mode)) {
			fprintf_message(outf, 2800117, "ERROR: info file %s: not a file",
				quotedif);
			fprint_message(outf, 2800118, "Remove the entry and create a new file");
			infobad = 1;
		    } else if (access(infofile, R_OK) == -1) {
			fprintf_message(outf, 2800119, "ERROR: info file %s: not readable",
				quotedif);
			infobad = 1;
		    }
		    amfree(quotedif);
		    amfree(quoted);
		    amfree(infofile);
		}
		if(dp->index) {
		    if(! indexdir_checked) {
			quoted = quote_string(conf_indexdir);
			if(stat(conf_indexdir, &statbuf) == -1) {
			    if (errno == ENOENT) {
				fprintf_message(outf, 2800120, "NOTE: index dir %s does not exist",
				        quoted);
				fprint_message(outf, 2800121, "NOTE: it will be created on the next run.");
			    } else {
				fprintf_message(outf, 2800122, "ERROR: index dir %s (%s)",
					quoted, strerror(errno));
				indexbad = 1;
			    }
			    amfree(conf_indexdir);
			} else if (!S_ISDIR(statbuf.st_mode)) {
			    fprintf_message(outf, 2800123, "ERROR: index dir %s: not a directory",
				    quoted);
			    fprint_message(outf, 2800124, "Remove the entry and create a new directory");
			    amfree(conf_indexdir);
			    indexbad = 1;
			} else if (access(conf_indexdir, W_OK) == -1) {
			    fprintf_message(outf, 2800125, "ERROR: index dir %s: not writable",
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
			    hostindexdir = g_strconcat(conf_indexdir, host, NULL);
			    quoted = quote_string(hostindexdir);
			    if(stat(hostindexdir, &statbuf) == -1) {
				if (errno == ENOENT) {
				    fprintf_message(outf, 2800126, "NOTE: index dir %s does not exist",
				            quoted);
				    fprint_message(outf, 2800127, "NOTE: it will be created on the next run.");
			        } else {
				    fprintf_message(outf, 2800128, "ERROR: index dir %s (%s)",
					    quoted, strerror(errno));
				    indexbad = 1;
				}
			        amfree(hostindexdir);
			    } else if (!S_ISDIR(statbuf.st_mode)) {
			        fprintf_message(outf, 2800129, "ERROR: index dir %s: not a directory",
				        quoted);
				fprint_message(outf, 2800130, "Remove the entry and create a new directory");
			        amfree(hostindexdir);
			        indexbad = 1;
			    } else if (access(hostindexdir, W_OK) == -1) {
			        fprintf_message(outf, 2800131, "ERROR: index dir %s: not writable",
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
			    g_free(diskdir);
			    diskdir = g_strconcat(hostindexdir, disk, NULL);
			    quoted = quote_string(diskdir);
			    if(stat(diskdir, &statbuf) == -1) {
				if (errno == ENOENT) {
				    fprintf_message(outf, 2800132, "NOTE: index dir %s does not exist",
					    quoted);
				    fprint_message(outf, 2800133, "NOTE: it will be created on the next run.");
				} else {
				    fprintf_message(outf, 2800134, "ERROR: index dir %s (%s)",
					quoted, strerror(errno));
				    indexbad = 1;
				}
			    } else if (!S_ISDIR(statbuf.st_mode)) {
				fprintf_message(outf, 2800135, "ERROR: index dir %s: not a directory",
					quoted);
				fprint_message(outf, 2800136, "Remove the entry and create a new directory");
				indexbad = 1;
			    } else if (access(diskdir, W_OK) == -1) {
				fprintf_message(outf, 2800137, "ERROR: index dir %s: is not writable",
					quoted);
				indexbad = 1;
			    }
			    amfree(quoted);
			}
		    }
		}

		if ( dp->encrypt == ENCRYPT_SERV_CUST ) {
		  if ( dp->srv_encrypt[0] == '\0' ) {
		    fprint_message(outf, 2800138, "ERROR: server encryption program not specified");
		    fprint_message(outf, 2800139, "Specify \"server-custom-encrypt\" in the dumptype");
		    pgmbad = 1;
		  }
		  else if(access(dp->srv_encrypt, X_OK) == -1) {
		    fprintf_message(outf, 2800140, "ERROR: %s is not executable, server encryption will not work",
			    dp->srv_encrypt );
		   fprint_message(outf, 2800141, "Check file type");
		    pgmbad = 1;
		  }
		}
		if ( dp->compress == COMP_SERVER_CUST ) {
		  if ( dp->srvcompprog[0] == '\0' ) {
		    fprint_message(outf, 2800142, "ERROR: server custom compression program "
				    "not specified");
		    fprint_message(outf, 2800143, "Specify \"server-custom-compress\" in "
				    "the dumptype");
		    pgmbad = 1;
		  }
		  else if(access(dp->srvcompprog, X_OK) == -1) {
		    quoted = quote_string(dp->srvcompprog);

		    fprintf_message(outf, 2800144, "ERROR: %s is not executable, server custom "
				    "compression will not work",
			    quoted);
		    amfree(quoted);
		   fprint_message(outf, 2800145, "Check file type");
		    pgmbad = 1;
		  }
		}

		/* check deprecated splitting parameters */
		if (dumptype_seen(dp->config, DUMPTYPE_TAPE_SPLITSIZE)
		    || dumptype_seen(dp->config, DUMPTYPE_SPLIT_DISKBUFFER)
		    || dumptype_seen(dp->config, DUMPTYPE_FALLBACK_SPLITSIZE)) {
		    tape_size = tapetype_get_length(tp);
		    if (dp->tape_splitsize > tape_size) {
			fprintf_message(outf, 2800146,
				  "ERROR: %s %s: tape-splitsize > tape size",
				  hostp->hostname, dp->name);
			pgmbad = 1;
		    }
		    if (dp->tape_splitsize && dp->fallback_splitsize * 1024 > physmem_total()) {
			fprintf_message(outf, 2800147,
				  "ERROR: %s %s: fallback-splitsize > total available memory",
				  hostp->hostname, dp->name);
			pgmbad = 1;
		    }
		    if (dp->tape_splitsize && dp->fallback_splitsize > tape_size) {
			fprintf_message(outf, 2800148,
				  "ERROR: %s %s: fallback-splitsize > tape size",
				  hostp->hostname, dp->name);
			pgmbad = 1;
		    }

		    /* also check for part sizes that are too small */
		    if (dp->tape_splitsize && dp->tape_splitsize * 1000 < tape_size) {
			fprintf_message(outf, 2800149,
				  "WARNING: %s %s: tape-splitsize of %ju %sB < 0.1%% of tape length.",
				  hostp->hostname, dp->name,
				  (uintmax_t)dp->tape_splitsize/(uintmax_t)unitdivisor,
				  displayunit);
			if (!printed_small_part_size_warning) {
			    printed_small_part_size_warning = TRUE;
			    fprintf_message(outf, 2800150, "%s", small_part_size_warning);
			}
		    }

		    /* fallback splitsize will be used if split_diskbuffer is empty or NULL */
		    if (dp->tape_splitsize != 0 && dp->fallback_splitsize != 0 &&
			    (dp->split_diskbuffer == NULL ||
			     dp->split_diskbuffer[0] == '\0') &&
			    dp->fallback_splitsize * 1000 < tape_size) {
			fprintf_message(outf, 2800151,
			      "WARNING: %s %s: fallback-splitsize of %ju %sB < 0.1%% of tape length.",
			      hostp->hostname, dp->name,
			      (uintmax_t)dp->fallback_splitsize/(uintmax_t)unitdivisor,
			      displayunit);
			if (!printed_small_part_size_warning) {
			    printed_small_part_size_warning = TRUE;
			    fprintf_message(outf, 2800152, "%s", small_part_size_warning);
			}
		    }
		}

		if (dp->data_path == DATA_PATH_DIRECTTCP) {
		    if (dp->compress != COMP_NONE) {
			fprintf_message(outf, 2800153,
				  "ERROR: %s %s: Can't compress directtcp data-path",
				  hostp->hostname, dp->name);
			pgmbad = 1;
		    }
		    if (dp->encrypt != ENCRYPT_NONE) {
			fprintf_message(outf, 2800154,
				  "ERROR: %s %s: Can't encrypt directtcp data-path",
				  hostp->hostname, dp->name);
			pgmbad = 1;
		    }
		    if (dp->to_holdingdisk == HOLD_REQUIRED) {
			fprintf_message(outf, 2800155,
				  "ERROR: %s %s: Holding disk can't be use for directtcp data-path",
				  hostp->hostname, dp->name);
			pgmbad = 1;
		    }
		}
		if (dp->data_path == DATA_PATH_DIRECTTCP && !dev_directtcp_data_path) {
		    fprintf_message(outf, 2800156,
			      "ERROR: %s %s: data-path is DIRECTTCP but device do not support it",
			      hostp->hostname, dp->name);
		    pgmbad = 1;
		}
		if (dp->data_path == DATA_PATH_AMANDA && !dev_amanda_data_path) {
		    fprintf_message(outf, 2800157,
			      "ERROR: %s %s: data-path is AMANDA but device do not support it",
			      hostp->hostname, dp->name);
		    pgmbad = 1;
		}

		for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
		     pp_scriptlist = pp_scriptlist->next) {
		    pp_script_t *pp_script = lookup_pp_script((char *)pp_scriptlist->data);
		    g_assert(pp_script != NULL);
		    if (pp_script_get_execute_where(pp_script) == ES_CLIENT &&
			pp_script_get_execute_on(pp_script) & EXECUTE_ON_PRE_HOST_BACKUP) {
			fprintf_message(outf, 2800158,
				  "ERROR: %s %s: Can't run pre-host-backup script on client",
				  hostp->hostname, dp->name);
		    } else if (pp_script_get_execute_where(pp_script) == ES_CLIENT &&
			pp_script_get_execute_on(pp_script) & EXECUTE_ON_POST_HOST_BACKUP) {
			fprintf_message(outf, 2800159,
				  "ERROR: %s %s: Can't run post-host-backup script on client",
				  hostp->hostname, dp->name);
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

    fprintf_message(outf, 2800160, "Server check took %s seconds", walltime_str(curclock()));

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
    estimate_t estimate;
    GString *strbuf;

    if(hostp->up != HOST_READY) {
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
	    fprintf_message(client_outf, 2800161,
		    "ERROR: Client %s does not support selfcheck REQ packet.",
		    hostp->hostname);
	    fprint_message(client_outf, 2800162, "Client might be of a very old version");
	}
	if(!am_has_feature(hostp->features, fe_selfcheck_rep)) {
	    fprintf_message(client_outf, 2800163,
		    "ERROR: Client %s does not support selfcheck REP packet.",
		    hostp->hostname);
	    fprint_message(client_outf, 2800164, "Client might be of a very old version");
	}
	if(!am_has_feature(hostp->features, fe_sendsize_req_options) &&
	   !am_has_feature(hostp->features, fe_sendsize_req_no_options) &&
	   !am_has_feature(hostp->features, fe_sendsize_req_device)) {
	    fprintf_message(client_outf, 2800165,
		    "ERROR: Client %s does not support sendsize REQ packet.",
		    hostp->hostname);
	    fprint_message(client_outf, 2800166, "Client might be of a very old version");
	}
	if(!am_has_feature(hostp->features, fe_sendsize_rep)) {
	    fprintf_message(client_outf, 2800167,
		    "ERROR: Client %s does not support sendsize REP packet.",
		    hostp->hostname);
	    fprint_message(client_outf, 2800168, "Client might be of a very old version");
	}
	if(!am_has_feature(hostp->features, fe_sendbackup_req) &&
	   !am_has_feature(hostp->features, fe_sendbackup_req_device)) {
	    fprintf_message(client_outf, 2800169,
		   "ERROR: Client %s does not support sendbackup REQ packet.",
		   hostp->hostname);
	    fprint_message(client_outf, 2800170, "Client might be of a very old version");
	}
	if(!am_has_feature(hostp->features, fe_sendbackup_rep)) {
	    fprintf_message(client_outf, 2800171,
		   "ERROR: Client %s does not support sendbackup REP packet.",
		   hostp->hostname);
	    fprint_message(client_outf, 2800172, "Client might be of a very old version");
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

	    if(dp->up != DISK_READY || dp->todo != 1) {
		continue;
	    }
	    qname = quote_string(dp->name);

	    errors = validate_optionstr(dp);

            if (errors) {
                gchar **ptr;
                for (ptr = errors; *ptr; ptr++)
                    fprintf_message(client_outf, 2800173, "ERROR: %s:%s %s", hostp->hostname, qname,
                        *ptr);
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
		    fprintf_message(client_outf, 2800174,
			    "WARNING: %s:%s:%s host does not support quoted text",
			    hostp->hostname, qname, qdevice);
		    fprint_message(client_outf, 2800175, "You must upgrade amanda on the client to "
				    "specify a quoted text/device in the disklist, "
				    "or don't use quoted text for the device.");
		}
	    }

	    if(dp->device) {
		if(!am_has_feature(hostp->features, fe_selfcheck_req_device)) {
		    fprintf_message(client_outf, 2800176,
		     "ERROR: %s:%s (%s): selfcheck does not support device.",
		     hostp->hostname, qname, dp->device);
		    fprint_message(client_outf, 2800177, "You must upgrade amanda on the client to "
				    "specify a diskdevice in the disklist "
				    "or don't specify a diskdevice in the disklist.");
		}
		if(!am_has_feature(hostp->features, fe_sendsize_req_device)) {
		    fprintf_message(client_outf, 2800178,
		     "ERROR: %s:%s (%s): sendsize does not support device.",
		     hostp->hostname, qname, dp->device);
		    fprint_message(client_outf, 2800179, "You must upgrade amanda on the client to "
				    "specify a diskdevice in the disklist"
				    " or don't specify a diskdevice in the disklist.");
		}
		if(!am_has_feature(hostp->features, fe_sendbackup_req_device)) {
		    fprintf_message(client_outf, 2800180,
		     "ERROR: %s:%s (%s): sendbackup does not support device.",
		     hostp->hostname, qname, dp->device);
		    fprint_message(client_outf, 2800181, "You must upgrade amanda on the client to "
				    "specify a diskdevice in the disklist"
				    " or don't specify a diskdevice in the disklist.");
		}

		if (dp->data_path != DATA_PATH_AMANDA &&
		    !am_has_feature(hostp->features, fe_xml_data_path)) {
		    fprintf_message(client_outf, 2800182,
			      "ERROR: Client %s does not support %s data-path",
			      hostp->hostname,  data_path_to_string(dp->data_path));
		} else if (dp->data_path == DATA_PATH_DIRECTTCP &&
		    !am_has_feature(hostp->features, fe_xml_directtcp_list)) {
		    fprintf_message(client_outf, 2800183,
			      "ERROR: Client %s does not support directtcp data-path",
			      hostp->hostname);
		}
	    }
	    if (dp->program &&
	        (g_str_equal(dp->program, "DUMP") ||
	         g_str_equal(dp->program, "GNUTAR"))) {
		if(g_str_equal(dp->program, "DUMP") &&
		   !am_has_feature(hostp->features, fe_program_dump)) {
		    fprintf_message(client_outf, 2800184, "ERROR: %s:%s does not support DUMP.",
			    hostp->hostname, qname);
	    fprint_message(client_outf, 2800185, "You must upgrade amanda on the client to use DUMP "
				    "or you can use another program.");
		}
		if(g_str_equal(dp->program, "GNUTAR") &&
		   !am_has_feature(hostp->features, fe_program_gnutar)) {
		    fprintf_message(client_outf, 2800186, "ERROR: %s:%s does not support GNUTAR.",
			    hostp->hostname, qname);
		    fprint_message(client_outf, 2800187, "You must upgrade amanda on the client to use GNUTAR "
				    "or you can use another program.");
		}
		estimate = (estimate_t)GPOINTER_TO_INT(dp->estimatelist->data);
		if(estimate == ES_CALCSIZE &&
		   !am_has_feature(hostp->features, fe_calcsize_estimate)) {
		    fprintf_message(client_outf, 2800188, "ERROR: %s:%s does not support CALCSIZE for "
				    "estimate, using CLIENT.",
			    hostp->hostname, qname);
		    fprint_message(client_outf, 2800189, "You must upgrade amanda on the client to use "
				    "CALCSIZE for estimate or don't use CALCSIZE for estimate.");
		    estimate = ES_CLIENT;
		}
		if(estimate == ES_CALCSIZE &&
		   am_has_feature(hostp->features, fe_selfcheck_calcsize))
		    calcsize = "CALCSIZE ";
		else
		    calcsize = "";

		if(dp->compress == COMP_CUST &&
		   !am_has_feature(hostp->features, fe_options_compress_cust)) {
		  fprintf_message(client_outf, 2800190,
			  "ERROR: Client %s does not support custom compression.",
			  hostp->hostname);
		    fprint_message(client_outf, 2800191, "You must upgrade amanda on the client to "
				    "use custom compression");
		    fprint_message(client_outf, 2800192, "Otherwise you can use the default client "
				    "compression program.");
		}
		if(dp->encrypt == ENCRYPT_CUST ) {
		  if ( !am_has_feature(hostp->features, fe_options_encrypt_cust)) {
		    fprintf_message(client_outf, 2800193,
			    "ERROR: Client %s does not support data encryption.",
			    hostp->hostname);
		    fprint_message(client_outf, 2800194, "You must upgrade amanda on the client to use encryption program.");
		    remote_errors++;
		  } else if ( dp->compress == COMP_SERVER_FAST ||
			      dp->compress == COMP_SERVER_BEST ||
			      dp->compress == COMP_SERVER_CUST ) {
		    fprintf_message(client_outf, 2800195,
			    "ERROR: %s: Client encryption with server compression "
			      "is not supported. See amanda.conf(5) for detail.",
			    hostp->hostname);
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
		    fprintf_message(client_outf, 2800196, "ERROR: %s:%s does not support APPLICATION-API.",
			    hostp->hostname, qname);
		    fprint_message(client_outf, 2800197, "Dumptype configuration is not GNUTAR or DUMP."
				    " It is case sensitive");
		    remote_errors++;
		    l = g_strdup("");
		} else {
                    strbuf = g_string_new("<dle>\n  <program>APPLICATION</program>\n");
		    if (dp->application) {
			application_t *application = lookup_application(dp->application);

			if (!application) {
			    fprintf_message(client_outf, 2800198,
			      "ERROR: application '%s' not found.", dp->application);
			} else {
			    char *xml_app = xml_application(dp, application, hostp->features);
			    char *client_name = application_get_client_name(application);
			    if (client_name && strlen(client_name) > 0 &&
				!am_has_feature(hostp->features, fe_application_client_name)) {
				fprintf_message(client_outf, 2800199,
			      "WARNING: %s:%s does not support client-name in application.",
			      hostp->hostname, qname);
			    }
                            g_string_append(strbuf, xml_app);
			    g_free(xml_app);
			}
		    }

		    if (dp->pp_scriptlist) {
			if (!am_has_feature(hostp->features, fe_pp_script)) {
			    fprintf_message(client_outf, 2800200,
			      "ERROR: %s:%s does not support SCRIPT-API.",
			      hostp->hostname, qname);
			} else {
			    identlist_t pp_scriptlist;
			    for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
				pp_scriptlist = pp_scriptlist->next) {
				pp_script_t *pp_script = lookup_pp_script((char *)pp_scriptlist->data);
				char *client_name = pp_script_get_client_name(pp_script);;
				if (client_name && strlen(client_name) > 0 &&
				    !am_has_feature(hostp->features, fe_script_client_name)) {
				    fprintf_message(client_outf, 2800201,
					"WARNING: %s:%s does not support client-name in script.",
					hostp->hostname, dp->name);
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
	    dp->up = DISK_ACTIVE;
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

    secdrv = security_getdriver(hostp->disks->auth);
    if (secdrv == NULL) {
	fprintf_message(stderr, 2800213, "Could not find security driver \"%s\" for host \"%s\". auth for this dle is invalid",
	      hostp->disks->auth, hostp->hostname);
    } else {
	protocol_sendreq(hostp->hostname, secdrv, amhost_get_security_conf,
			 req, conf_ctimeout, handle_result, hostp);
    }

    amfree(req);

    hostp->up = HOST_ACTIVE;
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
    fprint_message(outf, 2800202, "Amanda Backup Client Hosts Check");
    fprint_message(outf, 2800203,   "--------------------------------");

    run_server_global_scripts(EXECUTE_ON_PRE_AMCHECK, get_config_name());
    protocol_init();

    hostcount = remote_errors = 0;

    for(dlist = origq.head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	hostp = dp->host;
	if(hostp->up == HOST_READY && dp->todo == 1) {
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

    {
	char *a = plural("Client check: %d host checked in %s seconds.",
			 "Client check: %d hosts checked in %s seconds.",
			 hostcount);
	char *b = plural("  %d problem found.",
			 "  %d problems found.",
			 remote_errors);
	char *c = g_strdup_printf("%s%s", a, b);
	fprintf_message(outf, 2800204, c,
	    hostcount, walltime_str(curclock()), remote_errors);
	g_free(c);
    }
    fflush(outf);

    amcheck_exit(userbad || remote_errors > 0);
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
    gboolean printed_hostname = FALSE;

    hostp = (am_host_t *)datap;
    hostp->up = HOST_READY;

    if (pkt == NULL) {
	fprintf_message(client_outf, 2800206,
	    "WARNING: %s: selfcheck request failed: %s", hostp->hostname,
	    security_geterror(sech));
	remote_errors++;
	hostp->up = HOST_DONE;
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
		    fprintf_message(client_outf, 2800207, "ERROR: %s: bad features value: '%s'",
			    hostp->hostname, t);
		    fprint_message(client_outf, 2800208, "The amfeature in the reply packet is invalid");
		    remote_errors++;
		    hostp->up = HOST_DONE;
		}
		if (u)
		   *u = ';';
	    }

	    continue;
	}

	if (client_verbose && !printed_hostname) {
	    fprintf_message(client_outf, 2800209, "HOST %s", hostp->hostname);
	    printed_hostname = TRUE;
	}

	if(strncmp_const(line, "OK ") == 0) {
	    if (client_verbose) {
		fprintf_message(client_outf, 2800210, "%s", line);
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
		fprintf_message(client_outf, 2800211, "ERROR: %s%s: %s",
			(pkt->type == P_NAK) ? "NAK " : "",
			hostp->hostname,
			t - 1);
		remote_errors++;
		hostp->up = HOST_DONE;
	    }
	    continue;
	}

	fprintf_message(client_outf, 2800212, "ERROR: %s: unknown response: %s",
		hostp->hostname, line);
	remote_errors++;
	hostp->up = HOST_DONE;
    }
    if(hostp->up == HOST_READY && hostp->features == NULL) {
	/*
	 * The client does not support the features list, so give it an
	 * empty one.
	 */
	g_debug("no feature set from host %s", hostp->hostname);
	hostp->features = am_set_default_feature_set();
    }
    for(dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	if(dp->up == DISK_ACTIVE) {
	    dp->up = DISK_DONE;
	}
    }
    start_host(hostp);
    if(hostp->up == HOST_DONE) {
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
