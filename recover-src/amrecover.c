/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
 * $Id: amrecover.c,v 1.73 2006/07/25 18:27:57 martinea Exp $
 *
 * an interactive program for recovering backed-up files
 */

#include "amanda.h"
#include "stream.h"
#include "amfeatures.h"
#include "amrecover.h"
#include "getfsent.h"
#include "dgram.h"
#include "amutil.h"
#include "conffile.h"
#include "protocol.h"
#include "event.h"
#include "security.h"
#include "conffile.h"
#include "getopt.h"

#define amrecover_debug(i, ...) do {	\
	if ((i) <= debug_amrecover) {	\
	    dbprintf(__VA_ARGS__);	\
	}				\
} while (0)

static struct option long_options[] = {
    {"version"         , 0, NULL,  1},
    {NULL, 0, NULL, 0}
};

extern int process_line(char *line);
int get_line(void);
int grab_reply(int show);
void sigint_handler(int signum);
int main(int argc, char **argv);

#define USAGE _("Usage: amrecover [--version] [[-C] <config>] [-s <index-server>] [-t <tape-server>] [-d <tape-device>] [-o <clientconfigoption>]*\n")

char *server_name = NULL;
int server_socket;
char *server_line = NULL;
char *dump_datestamp = NULL;		/* date we are restoring */
char *dump_hostname;			/* which machine we are restoring */
char *disk_name = NULL;			/* disk we are restoring */
dle_t *dump_dle = NULL;
char *mount_point = NULL;		/* where disk was mounted */
char *disk_path = NULL;			/* path relative to mount point */
char *disk_tpath = NULL;		/* translated path relative to mount point */
char dump_date[STR_SIZE];		/* date on which we are restoring */
int quit_prog;				/* set when time to exit parser */
char *tape_server_name = NULL;
int tape_server_socket;
char *tape_device_name = NULL;
am_feature_t *our_features = NULL;
char *our_features_string = NULL;
am_feature_t *indexsrv_features = NULL;
am_feature_t *tapesrv_features = NULL;
proplist_t proplist = NULL;
static char *errstr = NULL;
char *authopt;
int amindexd_alive = 0;
security_handle_t *gsech;

static struct {
    const char *name;
    security_stream_t *fd;
} streams[] = {
#define MESGFD  0
    { "MESG", NULL },
};
#define NSTREAMS G_N_ELEMENTS(streams)

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
	mesg_buffer = g_strdup("");
 
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
	newbuf = g_malloc(strlen(mesg_buffer)+size+1);
	strncpy(newbuf, mesg_buffer, (size_t)(strlen(mesg_buffer) + size));
	memcpy(newbuf+strlen(mesg_buffer), buf, (size_t)size);
	newbuf[strlen(mesg_buffer)+size] = '\0';
	amfree(mesg_buffer);
	mesg_buffer = newbuf;
	amfree(buf);
    }

    s = strstr(mesg_buffer,"\r\n");
    *s = '\0';
    newbuf = g_strdup(s+2);
    g_free(server_line);
    server_line = g_strdup(mesg_buffer);
    amfree(mesg_buffer);
    mesg_buffer = newbuf;
    amrecover_debug(1, "server_line: %s\n", server_line);
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
server_warning(void)
{
    return server_line[0] == '5' &&
	   server_line[1] == '0' &&
	   server_line[2] == '2';
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

    buffer = g_malloc(strlen(cmd)+3);
    strncpy(buffer, cmd, strlen(cmd));
    buffer[strlen(cmd)] = '\r';
    buffer[strlen(cmd)+1] = '\n';
    buffer[strlen(cmd)+2] = '\0';

    g_debug("sending: %s", buffer);
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


char *
clean_pathname(
    char *	s)
{
    size_t length;
    length = strlen(s);

    /* remove "/" at end of path */
    if(length>1 && s[length-1]=='/')
	s[length-1]='\0';

    /* change "/." to "/" */
    if(g_str_equal(s, "/."))
	s[1]='\0';

    /* remove "/." at end of path */
    if(g_str_equal(&(s[length - 2]), "/."))
	s[length-2]='\0';

    return s;
}


void
quit(void)
{
    quit_prog = 1;
    (void)converse("QUIT");
    stop_amindexd();
}

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
    config_overrides_t *cfg_ovr;
    char *starting_hostname = NULL;

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

    set_pname("amrecover");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);
    make_crc_table();

    /* treat amrecover-specific command line options as the equivalent
     * -o command-line options to set configuration values */
    cfg_ovr = new_config_overrides(argc/2);

    /* If the first argument is not an option flag, then we assume
     * it is a configuration name to match the syntax of the other
     * Amanda utilities. */
    if (argc > 1 && argv[1][0] != '-') {
	add_config_override(cfg_ovr, "conf", argv[1]);

	/* remove that option from the command line */
	argv[1] = argv[0];
	argv++; argc--;
    }

    /* now parse regular command-line '-' options */
    while ((i = getopt_long(argc, argv, "o:C:s:t:d:Uh:", long_options, NULL)) != EOF) {
	switch (i) {
	    case 1:
		printf("amrecover-%s\n", VERSION);
		return(0);
		break;
	    case 'C':
		add_config_override(cfg_ovr, "conf", optarg);
		break;

	    case 's':
		add_config_override(cfg_ovr, "index_server", optarg);
		break;

	    case 't':
		add_config_override(cfg_ovr, "tape_server", optarg);
		break;

	    case 'd':
		add_config_override(cfg_ovr, "tapedev", optarg);
		break;

	    case 'o':
		add_config_override_opt(cfg_ovr, optarg);
		break;

	    case 'h':
		starting_hostname = g_strdup(optarg);
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

    /* load the base client configuration */
    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    /* and now try to load the configuration named in that file */
    config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY,
		getconf_str(CNF_CONF));

    check_running_as(RUNNING_AS_ROOT);

    dbrename(get_config_name(), DBG_SUBDIR_CLIENT);

    our_features = am_init_feature_set();
    our_features_string = am_feature_to_string(our_features);

    if (!starting_hostname && getconf_seen(CNF_HOSTNAME)) {
	starting_hostname = g_strdup(getconf_str(CNF_HOSTNAME));
    }
    if (!starting_hostname) {
	starting_hostname = g_malloc(MAX_HOSTNAME_LENGTH+1);
	if (gethostname(starting_hostname, MAX_HOSTNAME_LENGTH) != 0) {
	    error(_("cannot determine local host name\n"));
	    /*NOTREACHED*/
	}
	starting_hostname[MAX_HOSTNAME_LENGTH] = '\0';
    }

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
    server_name = g_strdup(server_name);

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
    tape_server_name = g_strdup(tape_server_name);

    amfree(tape_device_name);
    tape_device_name = getconf_str(CNF_TAPEDEV);
    if (!tape_device_name ||
	strlen(tape_device_name) == 0 ||
	!getconf_seen(CNF_TAPEDEV)) {
	tape_device_name = NULL;
    } else {
	tape_device_name = g_strdup(tape_device_name);
    }

    authopt = g_strdup(getconf_str(CNF_AUTH));


    amfree(disk_name);
    amfree(mount_point);
    amfree(disk_path);
    amfree(disk_tpath);
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

    proplist = g_hash_table_new_full(g_str_hash, g_str_equal, &g_free, &free_property_t);

    protocol_init();

    /* We assume that amindexd support fe_amindexd_options_features */
    /*                             and fe_amindexd_options_auth     */
    /* We should send a noop to really know                         */
    req = g_strdup_printf("SERVICE amindexd\n"
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
	   VERSION, server_name);

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

	line = g_strdup_printf("FEATURES %s", our_features_string);
	if(exchange(line) == 0) {
	    their_feature_string = g_strdup(server_line+13);
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
    line = g_strdup_printf("DATE %s", dump_date);
    if (converse(line) == -1) {
        aclose(server_socket);
	exit(1);
    }
    amfree(line);

    line = g_strdup_printf("SCNF %s", get_config_name());
    if (converse(line) == -1) {
        aclose(server_socket);
	exit(1);
    }
    amfree(line);

    if (server_happy()) {
	/* set host we are restoring to this host by default */
	amfree(dump_hostname);
	set_host(starting_hostname);
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
    int ports[NSTREAMS], *response_error = datap;
    guint i;
    char *p;
    char *tok;
    char *extra = NULL;

    assert(response_error != NULL);
    assert(sech != NULL);

    gsech = sech;

    if (pkt == NULL) {
	g_free(errstr);
	errstr = g_strdup_printf(_("[request failed: %s]"),
                                 security_geterror(sech));
	*response_error = 1;
	return;
    }

    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	dbprintf(_("got nak response:\n----\n%s\n----\n\n"), pkt->body);
#endif

	tok = strtok(pkt->body, " ");
	if (tok == NULL || !g_str_equal(tok, "ERROR"))
	    goto bad_nak;

	tok = strtok(NULL, "\n");
	if (tok != NULL) {
	    g_free(errstr);
	    errstr = g_strdup_printf("NAK: %s", tok);
	    *response_error = 1;
	} else {
bad_nak:
	    g_free(errstr);
	    errstr = g_strdup("request NAK");
	    *response_error = 2;
	}
	return;
    }

    if (pkt->type != P_REP) {
	g_free(errstr);
	errstr = g_strdup_printf(_("received strange packet type %s: %s"),
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
	if (g_str_equal(tok, "ERROR")) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL) {
	        g_free(errstr);
	        errstr = g_strdup("[bogus error packet]");
	    } else {
		g_free(errstr);
		errstr = g_strdup_printf("%s", tok);
	    }
	    *response_error = 2;
	    return;
	}


        /*
         * Regular packets have CONNECT followed by three streams
         */
        if (g_str_equal(tok, "CONNECT")) {

	    /*
	     * Parse the three stream specifiers out of the packet.
	     */
	    for (i = 0; i < NSTREAMS; i++) {
		tok = strtok(NULL, " ");
		if (tok == NULL || !g_str_equal(tok, streams[i].name)) {
		    extra = g_strdup_printf(
			   _("CONNECT token is \"%s\": expected \"%s\""),
			   tok ? tok : _("(null)"), streams[i].name);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = g_strdup_printf(
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
	if (g_str_equal(tok, "OPTIONS")) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL) {
		extra = g_strdup(_("OPTIONS token is missing"));
		goto parse_error;
	    }
#if 0
	    tok_end = tok + strlen(tok);
	    while((p = strchr(tok, ';')) != NULL) {
		*p++ = '\0';
		if(strncmp_const(tok, "features=") == 0) {
		    tok += sizeof("features=") - 1;
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
	extra = g_strdup_printf(_("next token is \"%s\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\""), tok ? tok : _("(null)"));
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
            g_free(errstr);
            errstr = g_strdup_printf(_("[could not connect %s stream: %s]"),
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
            g_free(errstr);
            errstr = g_strdup_printf(_("[could not authenticate %s stream: %s]"),
                                     streams[i].name, security_stream_geterror(streams[i].fd));
	    goto connect_error;
	}
    }

    /*
     * The MESGFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (streams[MESGFD].fd == NULL) {
        g_free(errstr);
        errstr = g_strdup("[couldn't open MESG streams]");
        goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    amindexd_alive = 1;
    return;

parse_error:
			  g_free(errstr);
			  errstr = g_strdup_printf(_("[parse of reply message failed: %s]"),
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
    guint i;

    amindexd_alive = 0;
    for (i = 0; i < NSTREAMS; i++) {
        if (streams[i].fd != NULL) {
            security_stream_close(streams[i].fd);
            streams[i].fd = NULL;
        }
    }
}


char *
translate_octal(
    char *line)
{
    char *s = line, *s1, *s2;
    char *p = line;
    int i;

    if (!translate_mode)
	return strdup(line);

    while(*s != '\0') {
	if ((s == line || *(s-1) != '\\') && *s == '\\') {
	    s++;
	    s1 = s+1;
	    s2 = s+2;
	    if (g_ascii_isdigit(*s) && *s1 != '\0' &&
		g_ascii_isdigit(*s1) &&
		g_ascii_isdigit(*s2)) {
		i = ((*s)-'0')*64 + ((*s1)-'0')*8 + ((*s2)-'0');
		*p++ = i;
		s += 3;
	    } else {
		*p++ = *s++;
	    }

	} else {
	    *p++ = *s++;
	}
    }
    *p = '\0';

    return line;
}

