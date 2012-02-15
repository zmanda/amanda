/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
 * $Id: amrecover.c,v 1.7 2006/07/25 18:27:57 martinea Exp $
 *
 * an interactive program for recovering backed-up files
 */

#include "amanda.h"
#include "stream.h"
#include "amfeatures.h"
#include "amrecover.h"
#include "getfsent.h"
#include "dgram.h"
#include "util.h"
#include "conffile.h"

extern int process_line(char *line);
int guess_disk(char *cwd, size_t cwd_len, char **dn_guess, char **mpt_guess);
int get_line(void);
int grab_reply(int show);
void sigint_handler(int signum);
int main(int argc, char **argv);

#define USAGE _("Usage: amoldrecover [[-C] <config>] [-s <index-server>] [-t <tape-server>] [-d <tape-device>]\n")

char *config = NULL;
char *server_name = NULL;
int server_socket;
char *server_line = NULL;
char *dump_datestamp = NULL;		/* date we are restoring */
char *dump_hostname;			/* which machine we are restoring */
char *disk_name = NULL;			/* disk we are restoring */
char *mount_point = NULL;		/* where disk was mounted */
char *disk_path = NULL;			/* path relative to mount point */
char dump_date[STR_SIZE];		/* date on which we are restoring */
int quit_prog;				/* set when time to exit parser */
char *tape_server_name = NULL;
int tape_server_socket;
char *tape_device_name = NULL;
am_feature_t *our_features = NULL;
am_feature_t *indexsrv_features = NULL;
am_feature_t *tapesrv_features = NULL;

/* gets a "line" from server and put in server_line */
/* server_line is terminated with \0, \r\n is striped */
/* returns -1 if error */

int
get_line(void)
{
    char *line = NULL;
    char *part = NULL;
    size_t len;

    while(1) {
	if((part = areads(server_socket)) == NULL) {
	    int save_errno = errno;

	    if(server_line) {
		fputs(server_line, stderr);	/* show the last line read */
		fputc('\n', stderr);
	    }
	    if(save_errno != 0) {
		g_fprintf(stderr, _("%s: Error reading line from server: %s\n"),
				get_pname(),
				strerror(save_errno));
	    } else {
		g_fprintf(stderr, _("%s: Unexpected end of file, check amindexd*debug on server %s\n"),
			get_pname(),
			server_name);
	    }
	    errno = save_errno;
	    break;	/* exit while loop */
	}
	if(line) {
	    strappend(line, part);
	    amfree(part);
	} else {
	    line = part;
	    part = NULL;
	}
	if((len = strlen(line)) > 0 && line[len-1] == '\r') {
	    line[len-1] = '\0';
	    server_line = newstralloc(server_line, line);
	    amfree(line);
	    return 0;
	}
	/*
	 * Hmmm.  We got a "line" from areads(), which means it saw
	 * a '\n' (or EOF, etc), but there was not a '\r' before it.
	 * Put a '\n' back in the buffer and loop for more.
	 */
	strappend(line, "\n");
    }
    amfree(line);
    amfree(server_line);
    return -1;
}


/* get reply from server and print to screen */
/* handle multi-line reply */
/* return -1 if error */
/* return code returned by server always occupies first 3 bytes of global
   variable server_line */
int
grab_reply(
    int		show)
{
    do {
	if (get_line() == -1) {
	    return -1;
	}
	if(show) puts(server_line);
    } while (server_line[3] == '-');
    if(show) fflush(stdout);

    return 0;
}


/* get 1 line of reply */
/* returns -1 if error, 0 if last (or only) line, 1 if more to follow */
int
get_reply_line(void)
{
    if (get_line() == -1)
	return -1;
    return server_line[3] == '-';
}


/* returns pointer to returned line */
char *
reply_line(void)
{
    return server_line;
}



/* returns 0 if server returned an error code (ie code starting with 5)
   and non-zero otherwise */
int
server_happy(void)
{
    return server_line[0] != '5';
}


int
send_command(
    char *	cmd)
{
    /*
     * NOTE: this routine is called from sigint_handler, so we must be
     * **very** careful about what we do since there is no way to know
     * our state at the time the interrupt happened.  For instance,
     * do not use any stdio or malloc routines here.
     */
    struct iovec msg[2];
    ssize_t bytes;

    memset(msg, 0, sizeof(msg));
    msg[0].iov_base = cmd;
    msg[0].iov_len = strlen(msg[0].iov_base);
    msg[1].iov_base = "\r\n";
    msg[1].iov_len = strlen(msg[1].iov_base);
    bytes = (ssize_t)(msg[0].iov_len + msg[1].iov_len);

    if (writev(server_socket, msg, 2) < bytes) {
	return -1;
    }
    return (0);
}


/* send a command to the server, get reply and print to screen */
int
converse(
    char *	cmd)
{
    if (send_command(cmd) == -1) return -1;
    if (grab_reply(1) == -1) return -1;
    return 0;
}


/* same as converse() but reply not echoed to stdout */
int
exchange(
    char *	cmd)
{
    if (send_command(cmd) == -1) return -1;
    if (grab_reply(0) == -1) return -1;
    return 0;
}


/* basic interrupt handler for when user presses ^C */
/* Bale out, letting server know before doing so */
void
sigint_handler(
    int	signum)
{
    /*
     * NOTE: we must be **very** careful about what we do here since there
     * is no way to know our state at the time the interrupt happened.
     * For instance, do not use any stdio routines here or in any called
     * routines.  Also, use _exit() instead of exit() to make sure stdio
     * buffer flushing is not attempted.
     */
    (void)signum;	/* Quiet unused parameter warning */

    if (extract_restore_child_pid != -1)
	(void)kill(extract_restore_child_pid, SIGKILL);
    extract_restore_child_pid = -1;

    (void)send_command("QUIT");
    _exit(1);
}


void
clean_pathname(
    char *	s)
{
    size_t length;
    length = strlen(s);

    /* remove "/" at end of path */
    if(length>1 && s[length-1]=='/')
	s[length-1]='\0';

    /* change "/." to "/" */
    if(strcmp(s,"/.")==0)
	s[1]='\0';

    /* remove "/." at end of path */
    if(strcmp(&(s[length-2]),"/.")==0)
	s[length-2]='\0';
}


/* try and guess the disk the user is currently on.
   Return -1 if error, 0 if disk not local, 1 if disk local,
   2 if disk local but can't guess name */
/* do this by looking for the longest mount point which matches the
   current directory */
int
guess_disk (
    char *	cwd,
    size_t	cwd_len,
    char **	dn_guess,
    char **	mpt_guess)
{
    size_t longest_match = 0;
    size_t current_length;
    size_t cwd_length;
    int local_disk = 0;
    generic_fsent_t fsent;
    char *fsname = NULL;
    char *disk_try = NULL;

    *dn_guess = NULL;
    *mpt_guess = NULL;

    if (getcwd(cwd, cwd_len) == NULL) {
	return -1;
	/*NOTREACHED*/
    }
    cwd_length = strlen(cwd);
    dbprintf(_("guess_disk: %zu: \"%s\"\n"), cwd_length, cwd);

    if (open_fstab() == 0) {
	return -1;
	/*NOTREACHED*/
    }

    while (get_fstab_nextentry(&fsent))
    {
	current_length = fsent.mntdir ? strlen(fsent.mntdir) : (size_t)0;
	dbprintf(_("guess_disk: %zu: %zu: \"%s\": \"%s\"\n"),
		  longest_match,
		  current_length,
		  fsent.mntdir ? fsent.mntdir : _("(mntdir null)"),
		  fsent.fsname ? fsent.fsname : _("(fsname null)"));
	if ((current_length > longest_match)
	    && (current_length <= cwd_length)
	    && (strncmp(fsent.mntdir, cwd, current_length) == 0))
	{
	    longest_match = current_length;
	    *mpt_guess = newstralloc(*mpt_guess, fsent.mntdir);
	    if(strncmp(fsent.fsname,DEV_PREFIX,(strlen(DEV_PREFIX))))
	    {
	        fsname = newstralloc(fsname, fsent.fsname);
            }
	    else
	    {
	        fsname = newstralloc(fsname,fsent.fsname+strlen(DEV_PREFIX));
	    }
	    local_disk = is_local_fstype(&fsent);
	    dbprintf(_("guess_disk: local_disk = %d, fsname = \"%s\"\n"),
		      local_disk,
		      fsname);
	}
    }
    close_fstab();

    if (longest_match == 0) {
	amfree(*mpt_guess);
	amfree(fsname);
	return -1;			/* ? at least / should match */
    }

    if (!local_disk) {
	amfree(*mpt_guess);
	amfree(fsname);
	return 0;
    }

    /* have mount point now */
    /* disk name may be specified by mount point (logical name) or
       device name, have to determine */
    g_printf(_("Trying disk %s ...\n"), *mpt_guess);
    disk_try = stralloc2("DISK ", *mpt_guess);		/* try logical name */
    if (exchange(disk_try) == -1)
	exit(1);
    amfree(disk_try);
    if (server_happy())
    {
	*dn_guess = stralloc(*mpt_guess);		/* logical is okay */
	amfree(fsname);
	return 1;
    }
    g_printf(_("Trying disk %s ...\n"), fsname);
    disk_try = stralloc2("DISK ", fsname);		/* try device name */
    if (exchange(disk_try) == -1)
	exit(1);
    amfree(disk_try);
    if (server_happy())
    {
	*dn_guess = stralloc(fsname);			/* dev name is okay */
	amfree(fsname);
	return 1;
    }

    /* neither is okay */
    amfree(*mpt_guess);
    amfree(fsname);
    return 2;
}


void
quit(void)
{
    quit_prog = 1;
    (void)converse("QUIT");
}

char *localhost = NULL;

#ifdef DEFAULT_TAPE_SERVER
# define DEFAULT_TAPE_SERVER_FAILOVER (DEFAULT_TAPE_SERVER)
#else
# define DEFAULT_TAPE_SERVER_FAILOVER (NULL)
#endif

int
main(
    int		argc,
    char **	argv)
{
    in_port_t my_port;
    struct servent *sp;
    int i;
    time_t timer;
    char *lineread = NULL;
    struct sigaction act, oact;
    extern char *optarg;
    extern int optind;
    char cwd[STR_SIZE], *dn_guess = NULL, *mpt_guess = NULL;
    char *service_name;
    char *line = NULL;
    struct tm *tm;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("amoldrecover");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);

    localhost = alloc(MAX_HOSTNAME_LENGTH+1);
    if (gethostname(localhost, MAX_HOSTNAME_LENGTH) != 0) {
	error(_("cannot determine local host name\n"));
	/*NOTREACHED*/
    }
    localhost[MAX_HOSTNAME_LENGTH] = '\0';

    config = newstralloc(config, DEFAULT_CONFIG);

    dbrename(config, DBG_SUBDIR_CLIENT);

    check_running_as(RUNNING_AS_ROOT);

    amfree(server_name);
    server_name = getenv("AMANDA_SERVER");
    if(!server_name) server_name = DEFAULT_SERVER;
    server_name = stralloc(server_name);

    amfree(tape_server_name);
    tape_server_name = getenv("AMANDA_TAPESERVER");
    if(!tape_server_name) tape_server_name = DEFAULT_TAPE_SERVER;
    tape_server_name = stralloc(tape_server_name);

    config_init(CONFIG_INIT_CLIENT, NULL);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    if (argc > 1 && argv[1][0] != '-')
    {
	/*
	 * If the first argument is not an option flag, then we assume
	 * it is a configuration name to match the syntax of the other
	 * Amanda utilities.
	 */
	char **new_argv;

	new_argv = (char **) alloc((size_t)((argc + 1 + 1) * sizeof(*new_argv)));
	new_argv[0] = argv[0];
	new_argv[1] = "-C";
	for (i = 1; i < argc; i++)
	{
	    new_argv[i + 1] = argv[i];
	}
	new_argv[i + 1] = NULL;
	argc++;
	argv = new_argv;
    }
    while ((i = getopt(argc, argv, "C:s:t:d:U")) != EOF)
    {
	switch (i)
	{
	    case 'C':
		config = newstralloc(config, optarg);
		break;

	    case 's':
		server_name = newstralloc(server_name, optarg);
		break;

	    case 't':
		tape_server_name = newstralloc(tape_server_name, optarg);
		break;

	    case 'd':
		tape_device_name = newstralloc(tape_device_name, optarg);
		break;

	    case 'U':
	    case '?':
		(void)g_printf(USAGE);
		return 0;
	}
    }
    if (optind != argc)
    {
	(void)g_fprintf(stderr, USAGE);
	exit(1);
    }

    amfree(disk_name);
    amfree(mount_point);
    amfree(disk_path);
    dump_date[0] = '\0';

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    /* set up signal handler */
    act.sa_handler = sigint_handler;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    if (sigaction(SIGINT, &act, &oact) != 0) {
	error(_("error setting signal handler: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    service_name = stralloc2("amandaidx", SERVICE_SUFFIX);

    g_printf(_("AMRECOVER Version %s. Contacting server on %s ...\n"),
	   VERSION, server_name);  
    if ((sp = getservbyname(service_name, "tcp")) == NULL) {
	error(_("%s/tcp unknown protocol"), service_name);
	/*NOTREACHED*/
    }
    amfree(service_name);
    server_socket = stream_client_privileged(server_name,
					     (in_port_t)ntohs((in_port_t)sp->s_port),
					     0,
					     0,
					     &my_port,
					     0);
    if (server_socket < 0) {
	error(_("cannot connect to %s: %s"), server_name, strerror(errno));
	/*NOTREACHED*/
    }
    if (my_port >= IPPORT_RESERVED) {
        aclose(server_socket);
	error(_("did not get a reserved port: %d"), my_port);
	/*NOTREACHED*/
    }

    /* get server's banner */
    if (grab_reply(1) == -1) {
        aclose(server_socket);
	exit(1);
    }
    if (!server_happy())
    {
	dbclose();
	aclose(server_socket);
	exit(1);
    }

    /* do the security thing */
    line = get_security();
    if (converse(line) == -1) {
        aclose(server_socket);
	exit(1);
    }
    if (!server_happy()) {
        aclose(server_socket);
	exit(1);
    }
    memset(line, '\0', strlen(line));
    amfree(line);

    /* try to get the features from the server */
    {
	char *our_feature_string = NULL;
	char *their_feature_string = NULL;

	our_features = am_init_feature_set();
	our_feature_string = am_feature_to_string(our_features);
	line = stralloc2("FEATURES ", our_feature_string);
	if(exchange(line) == 0) {
	    their_feature_string = stralloc(server_line+13);
	    indexsrv_features = am_string_to_feature(their_feature_string);
	}
	else {
	    indexsrv_features = am_set_default_feature_set();
        }
	amfree(our_feature_string);
	amfree(their_feature_string);
	amfree(line);
    }

    /* set the date of extraction to be today */
    (void)time(&timer);
    tm = localtime(&timer);
    if (tm)
	strftime(dump_date, sizeof(dump_date), "%Y-%m-%d", tm);
    else
	error(_("BAD DATE"));

    g_printf(_("Setting restore date to today (%s)\n"), dump_date);
    line = stralloc2("DATE ", dump_date);
    if (converse(line) == -1) {
        aclose(server_socket);
	exit(1);
    }
    amfree(line);

    line = stralloc2("SCNF ", config);
    if (converse(line) == -1) {
        aclose(server_socket);
	exit(1);
    }
    amfree(line);

    if (server_happy())
    {
	/* set host we are restoring to this host by default */
	amfree(dump_hostname);
	set_host(localhost);
	if (dump_hostname)
	{
            /* get a starting disk and directory based on where
	       we currently are */
	    switch (guess_disk(cwd, sizeof(cwd), &dn_guess, &mpt_guess))
	    {
		case 1:
		    /* okay, got a guess. Set disk accordingly */
		    g_printf(_("$CWD '%s' is on disk '%s' mounted at '%s'.\n"),
			   cwd, dn_guess, mpt_guess);
		    set_disk(dn_guess, mpt_guess);
		    set_directory(cwd);
		    if (server_happy() && strcmp(cwd, mpt_guess) != 0)
		        g_printf(_("WARNING: not on root of selected filesystem, check man-page!\n"));
		    amfree(dn_guess);
		    amfree(mpt_guess);
		    break;

		case 0:
		    g_printf(_("$CWD '%s' is on a network mounted disk\n"),
			   cwd);
		    g_printf(_("so you must 'sethost' to the server\n"));
		    /* fake an unhappy server */
		    server_line[0] = '5';
		    break;

		case 2:
		case -1:
		default:
		    g_printf(_("Use the setdisk command to choose dump disk to recover\n"));
		    /* fake an unhappy server */
		    server_line[0] = '5';
		    break;
	    }
	}
    }

    quit_prog = 0;
    do
    {
	if ((lineread = readline("amrecover> ")) == NULL) {
	    clearerr(stdin);
	    putchar('\n');
	    break;
	}
	if (lineread[0] != '\0') 
	{
	    add_history(lineread);
	    process_line(lineread);	/* act on line's content */
	}
	amfree(lineread);
    } while (!quit_prog);

    dbclose();

    aclose(server_socket);
    return 0;
}

char *
get_security(void)
{
    struct passwd *pwptr;

    if((pwptr = getpwuid(getuid())) == NULL) {
	error(_("can't get login name for my uid %ld"), (long)getuid());
	/*NOTREACHED*/
    }
    return stralloc2("SECURITY USER ", pwptr->pw_name);
}
