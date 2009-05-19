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
 * $Id: amrecover.c,v 1.73 2006/07/25 18:27:57 martinea Exp $
 *
 * an interactive program for recovering backed-up files
 */

#include "amanda.h"
#include "version.h"
#include "stream.h"
#include "amfeatures.h"
#include "amrecover.h"
#include "getfsent.h"
#include "dgram.h"
#include "util.h"
#include "conffile.h"
#include "protocol.h"
#include "event.h"
#include "security.h"

#define amrecover_debug(i, ...) do {	\
	if ((i) <= debug_amrecover) {	\
	    dbprintf(__VA_ARGS__);	\
	}				\
} while (0)

extern int process_line(char *line);
int get_line(void);
int grab_reply(int show);
void sigint_handler(int signum);
int main(int argc, char **argv);

#define USAGE _("Usage: amrecover [[-C] <config>] [-s <index-server>] [-t <tape-server>] [-d <tape-device>] [-o <clientconfigoption>]*\n")

char *server_name = NULL;
int server_socket;
char *server_line = NULL;
char *dump_datestamp = NULL;		/* date we are restoring */
char *dump_hostname;			/* which machine we are restoring */
char *disk_name = NULL;			/* disk we are restoring */
dle_t *dump_dle = NULL;
char *mount_point = NULL;		/* where disk was mounted */
char *disk_path = NULL;			/* path relative to mount point */
char dump_date[STR_SIZE];		/* date on which we are restoring */
int quit_prog;				/* set when time to exit parser */
char *tape_server_name = NULL;
int tape_server_socket;
char *tape_device_name = NULL;
am_feature_t *our_features = NULL;
char *our_features_string = NULL;
am_feature_t *indexsrv_features = NULL;
am_feature_t *tapesrv_features = NULL;
static char *errstr = NULL;
char *authopt;
int amindexd_alive = 0;

static struct {
    const char *name;
    security_stream_t *fd;
} streams[] = {
#define MESGFD  0
    { "MESG", NULL },
};
#define NSTREAMS        (int)(sizeof(streams) / sizeof(streams[0]))

static void amindexd_response(void *, pkt_t *, security_handle_t *);
void stop_amindexd(void);

static char* mesg_buffer = NULL;
/* gets a "line" from server and put in server_line */
/* server_line is terminated with \0, \r\n is striped */
/* returns -1 if error */

int
get_line(void)
{
    ssize_t size;
    char *newbuf, *s;
    void *buf;

    if (!mesg_buffer)
	mesg_buffer = stralloc("");
 
    while (!strstr(mesg_buffer,"\r\n")) {
	buf = NULL;
	size = security_stream_read_sync(streams[MESGFD].fd, &buf);
	if(size < 0) {
	    amrecover_debug(1, "amrecover: get_line size < 0 (%zd)\n", size);
	    return -1;
	}
	else if(size == 0) {
	    amrecover_debug(1, "amrecover: get_line size == 0 (%zd)\n", size);
	    return -1;
	}
	else if (buf == NULL) {
	    amrecover_debug(1, "amrecover: get_line buf == NULL\n");
	    return -1;
	}
        amrecover_debug(1, "amrecover: get_line size = %zd\n", size);
	newbuf = alloc(strlen(mesg_buffer)+size+1);
	strncpy(newbuf, mesg_buffer, (size_t)(strlen(mesg_buffer) + size));
	memcpy(newbuf+strlen(mesg_buffer), buf, (size_t)size);
	newbuf[strlen(mesg_buffer)+size] = '\0';
	amfree(mesg_buffer);
	mesg_buffer = newbuf;
    }

    s = strstr(mesg_buffer,"\r\n");
    *s = '\0';
    newbuf = stralloc(s+2);
    server_line = newstralloc(server_line, mesg_buffer);
    amfree(mesg_buffer);
    mesg_buffer = newbuf;
    amrecover_debug(1, "get: %s\n", mesg_buffer);
    return 0;
}


/* get reply from server and print to screen */
/* handle multi-line reply */
/* return -1 if error */
/* return code returned by server always occupies first 3 bytes of global
   variable server_line */
/* show == 0: Print the reply if it is an error */
/* show == 1: Always print the reply            */
int
grab_reply(
    int show)
{
    do {
	if (get_line() == -1) {
	    return -1;
	}
	if (show || server_line[0] == '5') {
	    puts(server_line);
	}
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
    char *buffer;

    buffer = alloc(strlen(cmd)+3);
    strncpy(buffer, cmd, strlen(cmd));
    buffer[strlen(cmd)] = '\r';
    buffer[strlen(cmd)+1] = '\n';
    buffer[strlen(cmd)+2] = '\0';

    if(security_stream_write(streams[MESGFD].fd, buffer, strlen(buffer)) < 0) {
	return -1;
    }
    amfree(buffer);
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

    if(amindexd_alive) 
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


void
quit(void)
{
    quit_prog = 1;
    (void)converse("QUIT");
    stop_amindexd();
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
    int i;
    time_t timer;
    char *lineread = NULL;
    struct sigaction act, oact;
    extern char *optarg;
    extern int optind;
    char *line = NULL;
    const security_driver_t *secdrv;
    char *req = NULL;
    int response_error;
    struct tm *tm;
    config_overwrites_t *cfg_ovr;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("amrecover");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);

    localhost = alloc(MAX_HOSTNAME_LENGTH+1);
    if (gethostname(localhost, MAX_HOSTNAME_LENGTH) != 0) {
	error(_("cannot determine local host name\n"));
	/*NOTREACHED*/
    }
    localhost[MAX_HOSTNAME_LENGTH] = '\0';

    /* load the base client configuration */
    config_init(CONFIG_INIT_CLIENT, NULL);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    /* treat amrecover-specific command line options as the equivalent
     * -o command-line options to set configuration values */
    cfg_ovr = new_config_overwrites(argc/2);

    /* If the first argument is not an option flag, then we assume
     * it is a configuration name to match the syntax of the other
     * Amanda utilities. */
    if (argc > 1 && argv[1][0] != '-') {
	add_config_overwrite(cfg_ovr, "conf", argv[1]);

	/* remove that option from the command line */
	argv[1] = argv[0];
	argv++; argc--;
    }

    /* now parse regular command-line '-' options */
    while ((i = getopt(argc, argv, "o:C:s:t:d:U")) != EOF) {
	switch (i) {
	    case 'C':
		add_config_overwrite(cfg_ovr, "conf", optarg);
		break;

	    case 's':
		add_config_overwrite(cfg_ovr, "index_server", optarg);
		break;

	    case 't':
		add_config_overwrite(cfg_ovr, "tape_server", optarg);
		break;

	    case 'd':
		add_config_overwrite(cfg_ovr, "tapedev", optarg);
		break;

	    case 'o':
		add_config_overwrite_opt(cfg_ovr, optarg);
		break;

	    case 'U':
	    case '?':
		(void)g_printf(USAGE);
		return 0;
	}
    }
    if (optind != argc) {
	(void)g_fprintf(stderr, USAGE);
	exit(1);
    }

    /* and now try to load the configuration named in that file */
    apply_config_overwrites(cfg_ovr);
    config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
		getconf_str(CNF_CONF));
    reapply_config_overwrites();

    check_running_as(RUNNING_AS_ROOT);

    dbrename(get_config_name(), DBG_SUBDIR_CLIENT);

    our_features = am_init_feature_set();
    our_features_string = am_feature_to_string(our_features);

    server_name = NULL;
    if (getconf_seen(CNF_INDEX_SERVER) == -2) { /* command line argument */
	server_name = getconf_str(CNF_INDEX_SERVER);
    }
    if (!server_name) {
	server_name = getenv("AMANDA_SERVER");
	if (server_name) {
	    g_printf(_("Using index server from environment AMANDA_SERVER (%s)\n"), server_name);
	}
    }
    if (!server_name) {
	server_name = getconf_str(CNF_INDEX_SERVER);
    }
    if (!server_name) {
	error(_("No index server set"));
	/*NOTREACHED*/
    }
    server_name = stralloc(server_name);

    tape_server_name = NULL;
    if (getconf_seen(CNF_TAPE_SERVER) == -2) { /* command line argument */
	tape_server_name = getconf_str(CNF_TAPE_SERVER);
    }
    if (!tape_server_name) {
	tape_server_name = getenv("AMANDA_TAPE_SERVER");
	if (!tape_server_name) {
	    tape_server_name = getenv("AMANDA_TAPESERVER");
	    if (tape_server_name) {
		g_printf(_("Using tape server from environment AMANDA_TAPESERVER (%s)\n"), tape_server_name);
	    }
	} else {
	    g_printf(_("Using tape server from environment AMANDA_TAPE_SERVER (%s)\n"), tape_server_name);
	}
    }
    if (!tape_server_name) {
	tape_server_name = getconf_str(CNF_TAPE_SERVER);
    }
    if (!tape_server_name) {
	error(_("No tape server set"));
	/*NOTREACHED*/
    }
    tape_server_name = stralloc(tape_server_name);

    amfree(tape_device_name);
    tape_device_name = getconf_str(CNF_TAPEDEV);
    if (!tape_device_name ||
	strlen(tape_device_name) == 0 ||
	!getconf_seen(CNF_TAPEDEV)) {
	tape_device_name = NULL;
    } else {
	tape_device_name = stralloc(tape_device_name);
    }

    authopt = stralloc(getconf_str(CNF_AUTH));


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

    protocol_init();

    /* We assume that amindexd support fe_amindexd_options_features */
    /*                             and fe_amindexd_options_auth     */
    /* We should send a noop to really know                         */
    req = vstrallocf("SERVICE amindexd\n"
		    "OPTIONS features=%s;auth=%s;\n",
		    our_features_string, authopt);

    secdrv = security_getdriver(authopt);
    if (secdrv == NULL) {
	error(_("no '%s' security driver available for host '%s'"),
	    authopt, server_name);
	/*NOTREACHED*/
    }

    protocol_sendreq(server_name, secdrv, generic_client_get_security_conf,
		     req, STARTUP_TIMEOUT, amindexd_response, &response_error);

    amfree(req);
    protocol_run();

    g_printf(_("AMRECOVER Version %s. Contacting server on %s ...\n"),
	   version(), server_name);

    if(response_error != 0) {
	g_fprintf(stderr,"%s\n",errstr);
	exit(1);
    }

    /* get server's banner */
    if (grab_reply(1) == -1) {
        aclose(server_socket);
	exit(1);
    }
    if (!server_happy()) {
	dbclose();
	aclose(server_socket);
	exit(1);
    }

    /* try to get the features from the server */
    {
	char *their_feature_string = NULL;

	indexsrv_features = NULL;

	line = vstrallocf("FEATURES %s", our_features_string);
	if(exchange(line) == 0) {
	    their_feature_string = stralloc(server_line+13);
	    indexsrv_features = am_string_to_feature(their_feature_string);
	    if (!indexsrv_features)
		g_printf(_("Bad feature string from server: %s"), their_feature_string);
	}
	if (!indexsrv_features)
	    indexsrv_features = am_set_default_feature_set();

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
    line = vstrallocf("DATE %s", dump_date);
    if (converse(line) == -1) {
        aclose(server_socket);
	exit(1);
    }
    amfree(line);

    line = vstrallocf("SCNF %s", get_config_name());
    if (converse(line) == -1) {
        aclose(server_socket);
	exit(1);
    }
    amfree(line);

    if (server_happy()) {
	/* set host we are restoring to this host by default */
	amfree(dump_hostname);
	set_host(localhost);
	if (dump_hostname)
	    g_printf(_("Use the setdisk command to choose dump disk to recover\n"));
	else
	    g_printf(_("Use the sethost command to choose a host to recover\n"));

    }

    quit_prog = 0;
    do {
	if ((lineread = readline("amrecover> ")) == NULL) {
	    clearerr(stdin);
	    putchar('\n');
	    break;
	}
	if (lineread[0] != '\0') 
	{
	    add_history(lineread);
	    dbprintf(_("user command: '%s'\n"), lineread);
	    process_line(lineread);	/* act on line's content */
	}
	amfree(lineread);
    } while (!quit_prog);

    dbclose();

    aclose(server_socket);
    return 0;
}

static void
amindexd_response(
    void *datap,
    pkt_t *pkt,
    security_handle_t *sech)
{
    int ports[NSTREAMS], *response_error = datap, i;
    char *p;
    char *tok;
    char *extra = NULL;

    assert(response_error != NULL);
    assert(sech != NULL);

    if (pkt == NULL) {
	errstr = newvstrallocf(errstr, _("[request failed: %s]"),
			     security_geterror(sech));
	*response_error = 1;
	return;
    }

    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	dbprintf(_("got nak response:\n----\n%s\n----\n\n"), pkt->body);
#endif

	tok = strtok(pkt->body, " ");
	if (tok == NULL || strcmp(tok, "ERROR") != 0)
	    goto bad_nak;

	tok = strtok(NULL, "\n");
	if (tok != NULL) {
	    errstr = newvstrallocf(errstr, "NAK: %s", tok);
	    *response_error = 1;
	} else {
bad_nak:
	    errstr = newvstrallocf(errstr, _("request NAK"));
	    *response_error = 2;
	}
	return;
    }

    if (pkt->type != P_REP) {
	errstr = newvstrallocf(errstr, _("received strange packet type %s: %s"),
			      pkt_type2str(pkt->type), pkt->body);
	*response_error = 1;
	return;
    }

#if defined(PACKET_DEBUG)
    g_fprintf(stderr, _("got response:\n----\n%s\n----\n\n"), pkt->body);
#endif

    for(i = 0; i < NSTREAMS; i++) {
        ports[i] = -1;
        streams[i].fd = NULL;
    }

    p = pkt->body;
    while((tok = strtok(p, " \n")) != NULL) {
	p = NULL;

	/*
	 * Error response packets have "ERROR" followed by the error message
	 * followed by a newline.
	 */
	if (strcmp(tok, "ERROR") == 0) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL) {
	        errstr = newvstrallocf(errstr, _("[bogus error packet]"));
	    } else {
		errstr = newvstrallocf(errstr, "%s", tok);
	    }
	    *response_error = 2;
	    return;
	}


        /*
         * Regular packets have CONNECT followed by three streams
         */
        if (strcmp(tok, "CONNECT") == 0) {

	    /*
	     * Parse the three stream specifiers out of the packet.
	     */
	    for (i = 0; i < NSTREAMS; i++) {
		tok = strtok(NULL, " ");
		if (tok == NULL || strcmp(tok, streams[i].name) != 0) {
		    extra = vstrallocf(
			   _("CONNECT token is \"%s\": expected \"%s\""),
			   tok ? tok : _("(null)"), streams[i].name);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = vstrallocf(
			   _("CONNECT %s token is \"%s\" expected a port number"),
			   streams[i].name, tok ? tok : _("(null)"));
		    goto parse_error;
		}
	    }
	    continue;
	}

	/*
	 * OPTIONS [options string] '\n'
	 */
	if (strcmp(tok, "OPTIONS") == 0) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL) {
		extra = vstrallocf(_("OPTIONS token is missing"));
		goto parse_error;
	    }
#if 0
	    tok_end = tok + strlen(tok);
	    while((p = strchr(tok, ';')) != NULL) {
		*p++ = '\0';
		if(strncmp_const(tok, "features=") == 0) {
		    tok += SIZEOF("features=") - 1;
		    am_release_feature_set(their_features);
		    if((their_features = am_string_to_feature(tok)) == NULL) {
			errstr = newvstrallocf(errstr,
				      _("OPTIONS: bad features value: %s"),
				      tok);
			goto parse_error;
		    }
		}
		tok = p;
	    }
#endif
	    continue;
	}
#if 0
	extra = vstrallocf(_("next token is \"%s\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\""), tok ? tok : _("(null)"));
	goto parse_error;
#endif
    }

    /*
     * Connect the streams to their remote ports
     */
    for (i = 0; i < NSTREAMS; i++) {
/*@i@*/	if (ports[i] == -1)
	    continue;
	streams[i].fd = security_stream_client(sech, ports[i]);
	if (streams[i].fd == NULL) {
	    errstr = newvstrallocf(errstr,
			_("[could not connect %s stream: %s]"),
			streams[i].name, security_geterror(sech));
	    goto connect_error;
	}
    }
    /*
     * Authenticate the streams
     */
    for (i = 0; i < NSTREAMS; i++) {
	if (streams[i].fd == NULL)
	    continue;
	if (security_stream_auth(streams[i].fd) < 0) {
	    errstr = newvstrallocf(errstr,
		_("[could not authenticate %s stream: %s]"),
		streams[i].name, security_stream_geterror(streams[i].fd));
	    goto connect_error;
	}
    }

    /*
     * The MESGFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (streams[MESGFD].fd == NULL) {
        errstr = newvstrallocf(errstr, _("[couldn't open MESG streams]"));
        goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    amindexd_alive = 1;
    return;

parse_error:
    errstr = newvstrallocf(errstr,
			  _("[parse of reply message failed: %s]"),
			  extra ? extra : _("(no additional information)"));
    amfree(extra);
    *response_error = 2;
    return;

connect_error:
    stop_amindexd();
    *response_error = 1;
}

/*
 * This is called when everything needs to shut down so event_loop()
 * will exit.
 */
void
stop_amindexd(void)
{
    int i;

    amindexd_alive = 0;
    for (i = 0; i < NSTREAMS; i++) {
        if (streams[i].fd != NULL) {
            security_stream_close(streams[i].fd);
            streams[i].fd = NULL;
        }
    }
}
