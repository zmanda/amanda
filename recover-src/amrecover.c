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
#include "clientconf.h"
#include "protocol.h"
#include "event.h"
#include "security.h"

extern int process_line(char *line);
int get_line(void);
int grab_reply(int show);
void sigint_handler(int signum);
int main(int argc, char **argv);

#define USAGE "Usage: amrecover [[-C] <config>] [-s <index-server>] [-t <tape-server>] [-d <tape-device>] [-o <clientconfigoption>]*\n"

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
char *amindexd_client_get_security_conf(char *, void *);

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
	size = security_stream_read_sync(streams[MESGFD].fd, &buf);
	if(size < 0) {
	    return -1;
	}
	else if(size == 0) {
	    return -1;
	}
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
    return 0;
}


/* get reply from server and print to screen */
/* handle multi-line reply */
/* return -1 if error */
/* return code returned by server always occupies first 3 bytes of global
   variable server_line */
int
grab_reply(
    int show)
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
    char *conffile;
    const security_driver_t *secdrv;
    char *req = NULL;
    int response_error;
    int new_argc;
    char **new_argv;
    struct tm *tm;

    safe_fd(-1, 0);

    set_pname("amrecover");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_CLIENT);

#ifndef IGNORE_UID_CHECK
    if (geteuid() != 0) {
	erroutput_type |= ERR_SYSLOG;
	error("amrecover must be run by root");
	/*NOTREACHED*/
    }
#endif

    localhost = alloc(MAX_HOSTNAME_LENGTH+1);
    if (gethostname(localhost, MAX_HOSTNAME_LENGTH) != 0) {
	error("cannot determine local host name\n");
	/*NOTREACHED*/
    }
    localhost[MAX_HOSTNAME_LENGTH] = '\0';

    parse_client_conf(argc, argv, &new_argc, &new_argv);

    if (new_argc > 1 && new_argv[1][0] != '-') {
	/*
	 * If the first argument is not an option flag, then we assume
	 * it is a configuration name to match the syntax of the other
	 * Amanda utilities.
	 */
	char **new_argv1;

	new_argv1 = (char **) alloc((size_t)((new_argc + 1 + 1) * sizeof(*new_argv1)));
	new_argv1[0] = new_argv[0];
	new_argv1[1] = "-C";
	for (i = 1; i < new_argc; i++) {
	    new_argv1[i + 1] = new_argv[i];
	}
	new_argv1[i + 1] = NULL;
	new_argc++;
	amfree(new_argv);
	new_argv = new_argv1;
    }
    while ((i = getopt(new_argc, new_argv, "C:s:t:d:U")) != EOF) {
	switch (i) {
	    case 'C':
		add_client_conf(CLN_CONF, optarg);
		//config = newstralloc(config, optarg);
		break;

	    case 's':
		add_client_conf(CLN_INDEX_SERVER, optarg);
		//server_name = newstralloc(server_name, optarg);
		break;

	    case 't':
		add_client_conf(CLN_TAPE_SERVER, optarg);
		//tape_server_name = newstralloc(tape_server_name, optarg);
		break;

	    case 'd':
		add_client_conf(CLN_TAPEDEV, optarg);
		//tape_device_name = newstralloc(tape_device_name, optarg);
		break;

	    case 'U':
	    case '?':
		(void)printf(USAGE);
		return 0;
	}
    }
    if (optind != new_argc) {
	(void)fprintf(stderr, USAGE);
	exit(1);
    }

    our_features = am_init_feature_set();
    our_features_string = am_feature_to_string(our_features);

    conffile = vstralloc(CONFIG_DIR, "/", "amanda-client.conf", NULL);
    if (read_clientconf(conffile) > 0) {
	error("error reading conffile: %s", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    config = stralloc(client_getconf_str(CLN_CONF));

    conffile = vstralloc(CONFIG_DIR, "/", config, "/", "amanda-client.conf",
			 NULL);
    if (read_clientconf(conffile) > 0) {
	error("error reading conffile: %s", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    dbrename(config, DBG_SUBDIR_CLIENT);

    report_bad_client_arg();

    amfree(server_name);
    server_name = getenv("AMANDA_SERVER");
    if(!server_name) server_name = client_getconf_str(CLN_INDEX_SERVER);
    server_name = stralloc(server_name);

    amfree(tape_server_name);
    tape_server_name = getenv("AMANDA_TAPESERVER");
    if(!tape_server_name) tape_server_name = client_getconf_str(CLN_TAPE_SERVER);
    tape_server_name = stralloc(tape_server_name);

    amfree(tape_device_name);
    tape_device_name = client_getconf_str(CLN_TAPEDEV);
    if (tape_device_name)
	tape_device_name = stralloc(tape_device_name);

    authopt = stralloc(client_getconf_str(CLN_AUTH));


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
	error("error setting signal handler: %s", strerror(errno));
	/*NOTREACHED*/
    }

    protocol_init();

    /* We assume that amindexd support fe_amindexd_options_features */
    /*                             and fe_amindexd_options_auth     */
    /* We should send a noop to really know                         */
    req = vstralloc("SERVICE amindexd\n",
		    "OPTIONS ", "features=", our_features_string, ";",
				"auth=", authopt, ";",
		    "\n", NULL);

    secdrv = security_getdriver(authopt);
    if (secdrv == NULL) {
	error("no '%s' security driver available for host '%s'",
	    authopt, server_name);
	/*NOTREACHED*/
    }

    protocol_sendreq(server_name, secdrv, amindexd_client_get_security_conf,
		     req, STARTUP_TIMEOUT, amindexd_response, &response_error);

    amfree(req);
    protocol_run();

    printf("AMRECOVER Version %s. Contacting server on %s ...\n",
	   version(), server_name);

    if(response_error != 0) {
	fprintf(stderr,"%s\n",errstr);
	exit(1);
    }

#if 0
    /*
     * We may need root privilege again later for a reserved port to
     * the tape server, so we will drop down now but might have to
     * come back later.
     */
    setegid(getgid());
    seteuid(getuid());
#endif

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

	line = stralloc2("FEATURES ", our_features_string);
	if(exchange(line) == 0) {
	    their_feature_string = stralloc(server_line+13);
	    indexsrv_features = am_string_to_feature(their_feature_string);
	}
	else {
	    indexsrv_features = am_set_default_feature_set();
        }
	amfree(their_feature_string);
	amfree(line);
    }

    /* set the date of extraction to be today */
    (void)time(&timer);
    tm = localtime(&timer);
    if (tm) 
	strftime(dump_date, sizeof(dump_date), "%Y-%m-%d", tm);
    else
	error("BAD DATE");

    printf("Setting restore date to today (%s)\n", dump_date);
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

    if (server_happy()) {
	/* set host we are restoring to this host by default */
	amfree(dump_hostname);
	set_host(localhost);
	if (dump_hostname)
	    printf("Use the setdisk command to choose dump disk to recover\n");
	else
	    printf("Use the sethost command to choose a host to recover\n");

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
	errstr = newvstralloc(errstr, "[request failed: ",
			     security_geterror(sech), "]", NULL);
	*response_error = 1;
	return;
    }

    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	fprintf(stderr, "got nak response:\n----\n%s\n----\n\n", pkt->body);
#endif

	tok = strtok(pkt->body, " ");
	if (tok == NULL || strcmp(tok, "ERROR") != 0)
	    goto bad_nak;

	tok = strtok(NULL, "\n");
	if (tok != NULL) {
	    errstr = newvstralloc(errstr, "NAK: ", tok, NULL);
	    *response_error = 1;
	} else {
bad_nak:
	    errstr = newstralloc(errstr, "request NAK");
	    *response_error = 2;
	}
	return;
    }

    if (pkt->type != P_REP) {
	errstr = newvstralloc(errstr, "received strange packet type ",
			      pkt_type2str(pkt->type), ": ", pkt->body, NULL);
	*response_error = 1;
	return;
    }

#if defined(PACKET_DEBUG)
    fprintf(stderr, "got response:\n----\n%s\n----\n\n", pkt->body);
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
	    if (tok == NULL)
		tok = "[bogus error packet]";
	    errstr = newstralloc(errstr, tok);
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
		    extra = vstralloc("CONNECT token is \"",
				      tok ? tok : "(null)",
				      "\": expected \"",
				      streams[i].name,
				      "\"",
				      NULL);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = vstralloc("CONNECT ",
				      streams[i].name,
				      " token is \"",
				      tok ? tok : "(null)",
				      "\": expected a port number",
				      NULL);
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
		extra = stralloc("OPTIONS token is missing");
		goto parse_error;
	    }
/*
	    tok_end = tok + strlen(tok);
	    while((p = strchr(tok, ';')) != NULL) {
		*p++ = '\0';
#define sc "features="
		if(strncmp(tok, sc, sizeof(sc)-1) == 0) {
		    tok += sizeof(sc) - 1;
#undef sc
		    am_release_feature_set(their_features);
		    if((their_features = am_string_to_feature(tok)) == NULL) {
			errstr = newvstralloc(errstr,
					      "OPTIONS: bad features value: ",
					      tok,
					      NULL);
			goto parse_error;
		    }
		}
		tok = p;
	    }
*/
	    continue;
	}
/*
	extra = vstralloc("next token is \"",
			  tok ? tok : "(null)",
			  "\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\"",
			  NULL);
	goto parse_error;
*/
    }

    /*
     * Connect the streams to their remote ports
     */
    for (i = 0; i < NSTREAMS; i++) {
/*@i@*/	if (ports[i] == -1)
	    continue;
	streams[i].fd = security_stream_client(sech, ports[i]);
	if (streams[i].fd == NULL) {
	    errstr = newvstralloc(errstr,
			"[could not connect ", streams[i].name, " stream: ",
			security_geterror(sech), "]", NULL);
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
	    errstr = newvstralloc(errstr,
		"[could not authenticate ", streams[i].name, " stream: ",
		security_stream_geterror(streams[i].fd), "]", NULL);
	    goto connect_error;
	}
    }

    /*
     * The MESGFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (streams[MESGFD].fd == NULL) {
        errstr = newstralloc(errstr, "[couldn't open MESG streams]");
        goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    amindexd_alive = 1;
    return;

parse_error:
    errstr = newvstralloc(errstr,
			  "[parse of reply message failed: ",
			  extra ? extra : "(no additional information)",
			  "]",
			  NULL);
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

char *
amindexd_client_get_security_conf(
    char *	string,
    void *	arg)
{
    (void)arg;	/* Quiet unused parameter warning */

    if(!string || !*string)
	return(NULL);

    if(strcmp(string, "auth")==0) {
	return(client_getconf_str(CLN_AUTH));
    }
    else if(strcmp(string, "ssh_keys")==0) {
	return(client_getconf_str(CLN_SSH_KEYS));
    }
    return(NULL);
}
