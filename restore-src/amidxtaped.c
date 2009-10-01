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
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */
/* $Id: amidxtaped.c,v 1.73 2006/07/25 19:06:46 martinea Exp $
 *
 * This daemon extracts a dump image off a tape for amrecover and
 * returns it over the network. It basically, reads a number of
 * arguments from stdin (it is invoked via inet), one per line,
 * preceeded by the number of them, and forms them into an argv
 * structure, then execs amrestore
 */

#include "amanda.h"
#include "clock.h"
#include "restore.h"
#include "cmdline.h"

#include "changer.h"
#include "conffile.h"
#include "logfile.h"
#include "amfeatures.h"
#include "stream.h"
#include "amandad.h"
#include "server_util.h"

#define amidxtaped_debug(i,x) do {	\
	if ((i) <= debug_amidxtaped) {	\
	    dbprintf(x);		\
	}				\
} while (0)

#define TIMEOUT 30

static char *pgm = "amidxtaped";	/* in case argv[0] is not set */

extern char *rst_conf_logfile;

static int get_lock = 0;
static int from_amandad;

static am_feature_t *our_features = NULL;
static am_feature_t *their_features = NULL;
static g_option_t *g_options = NULL;
static int ctlfdin, ctlfdout, datafdout;
static char *amandad_auth = NULL;
static FILE *cmdin, *cmdout;

static char *get_client_line(FILE *in);
static void check_security_buffer(char *);
static char *get_client_line_fd(int);

/* exit routine */
static pid_t parent_pid = -1;
static void cleanup(void);

int main(int argc, char **argv);

/* get a line from client - line terminated by \r\n */
static char *
get_client_line(FILE *in)
{
    static char *line = NULL;
    char *part = NULL;
    size_t len;

    amfree(line);
    while(1) {
	if((part = agets(in)) == NULL) {
	    if(errno != 0) {
		dbprintf(_("read error: %s\n"), strerror(errno));
	    } else {
		dbprintf(_("EOF reached\n"));
	    }
	    if(line) {
		dbprintf(_("s: unprocessed input:\n"));
		dbprintf("-----\n");
		dbprintf("%s\n", line);
		dbprintf("-----\n");
	    }
	    amfree(line);
	    amfree(part);
	    dbclose();
	    exit(1);
	    /*NOTREACHED*/
	}
	if(line) {
	    strappend(line, part);
	    amfree(part);
	} else {
	    line = part;
	    part = NULL;
	}
	if((len = strlen(line)) > 0 && line[len-1] == '\r') {
	    line[len-1] = '\0';		/* zap the '\r' */
	    break;
	}
	/*
	 * Hmmm.  We got a "line" from agets(), which means it saw
	 * a '\n' (or EOF, etc), but there was not a '\r' before it.
	 * Put a '\n' back in the buffer and loop for more.
	 */
	strappend(line, "\n");
    }
    dbprintf("> %s\n", line);
    return line;
}

/* get a line from client - line terminated by \r\n */
static char *
get_client_line_fd(
    int		fd)
{
    static char *line = NULL;
    static size_t line_size = 0;
    char *s = line;
    size_t len = 0;
    char c;
    ssize_t nb;

    if(line == NULL) { /* first time only, allocate initial buffer */
	s = line = alloc(128);
	line_size = 128;
    }
    while(1) {
	nb = read(fd, &c, 1);
	if (nb <= 0) {
	    /* EOF or error */
	    if ((nb <= 0) && ((errno == EINTR) || (errno == EAGAIN))) {
		/* Keep looping if failure is temporary */
		continue;
	    }
	    dbprintf(_("%s: Control pipe read error - %s\n"),
		      pgm, strerror(errno));
	    break;
	}

	if(len >= line_size-1) { /* increase buffer size */
	    line_size *= 2;
	    line = realloc(line, line_size);
	    if (line == NULL) {
		error(_("Memory reallocation failure"));
		/*NOTREACHED*/
	    }
	    s = &line[len];
	}
	*s = c;
	if(c == '\n') {
	    if(len > 0 && *(s-1) == '\r') { /* remove '\r' */
		s--;
		len--;
	    }
	    *s = '\0';
	    return line;
	}
	s++;
	len++;
    }
    line[len] = '\0';
    return line;
}


void
check_security_buffer(
    char *	buffer)
{
    socklen_t_equiv i;
    struct sockaddr_in addr;
    char *s, *fp, ch;
    char *errstr = NULL;

    dbprintf(_("check_security_buffer(buffer='%s')\n"), buffer);

    i = SIZEOF(addr);
    if (getpeername(0, (struct sockaddr *)&addr, &i) == -1) {
	error(_("getpeername: %s"), strerror(errno));
	/*NOTREACHED*/
    }
    if ((addr.sin_family != (sa_family_t)AF_INET)
		|| (ntohs(addr.sin_port) == 20)) {
	error(_("connection rejected from %s family %d port %d"),
             inet_ntoa(addr.sin_addr), addr.sin_family, htons(addr.sin_port));
	/*NOTREACHED*/
    }

    /* do the security thing */
    s = buffer;
    ch = *s++;

    skip_whitespace(s, ch);
    if (ch == '\0') {
	error(_("cannot parse SECURITY line"));
	/*NOTREACHED*/
    }
    fp = s-1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    if (strcmp(fp, "SECURITY") != 0) {
	error(_("cannot parse SECURITY line"));
	/*NOTREACHED*/
    }
    skip_whitespace(s, ch);
    if (!check_security((sockaddr_union *)&addr, s-1, 0, &errstr)) {
	error(_("security check failed: %s"), errstr);
	/*NOTREACHED*/
    }
}

int
main(
    int		argc,
    char **	argv)
{
    char *buf = NULL;
    int data_sock = -1;
    in_port_t data_port = (in_port_t)-1;
    socklen_t_equiv socklen;
    struct sockaddr_in addr;
    GSList *dumpspecs;
    tapelist_t *tapes = NULL;
    char *their_feature_string = NULL;
    rst_flags_t *rst_flags;
    int use_changer = 0;
    int re_end;
    char *re_config = NULL;
    char *conf_tapetype;
    tapetype_t *tape;
    char *line;
    char *tapedev;
    dumpspec_t *ds;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(DATA_FD_OFFSET, 4);
    openbsd_fd_inform();
    safe_cd();

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    rst_flags = new_rst_flags();
    rst_flags->mask_splits = 1; /* for older clients */
    rst_flags->amidxtaped = 1;
    our_features = am_init_feature_set();
    their_features = am_set_default_feature_set();

    /*
     * When called via inetd, it is not uncommon to forget to put the
     * argv[0] value on the config line.  On some systems (e.g. Solaris)
     * this causes argv and/or argv[0] to be NULL, so we have to be
     * careful getting our name.
     */
    if (argc >= 1 && argv != NULL && argv[0] != NULL) {
	if((pgm = strrchr(argv[0], '/')) != NULL) {
	    pgm++;
	} else {
	    pgm = argv[0];
	}
    }

    set_pname(pgm);

    if(argv && argv[1] && strcmp(argv[1], "amandad") == 0) {
	from_amandad = 1;
	if(argv[2])
	    amandad_auth = argv[2];
    }
    else {
	from_amandad = 0;
	safe_fd(-1, 0);
    }

    /* initialize */
    /* close stderr first so that debug file becomes it - amrestore
       chats to stderr, which we don't want going to client */
    /* if no debug file, ship to bit bucket */
    (void)close(STDERR_FILENO);
    dbopen(DBG_SUBDIR_SERVER);
    startclock();
    dbprintf(_("%s: version %s\n"), pgm, VERSION);
    debug_dup_stderr_to_debug();

    if (! (argc >= 1 && argv != NULL && argv[0] != NULL)) {
	dbprintf(_("WARNING: argv[0] not defined: check inetd.conf\n"));
    }

    if(from_amandad == 0) {
	socklen = SIZEOF(addr);
	if (getpeername(0, (struct sockaddr *)&addr, &socklen) == -1) {
	    error(_("getpeername: %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	if ((addr.sin_family != (sa_family_t)AF_INET)
		|| (ntohs(addr.sin_port) == 20)) {
	    error(_("connection rejected from %s family %d port %d"),
		  inet_ntoa(addr.sin_addr), addr.sin_family,
		  htons(addr.sin_port));
	    /*NOTREACHED*/
	}

	/* do the security thing */
	amfree(buf);
	fflush(stdout);
	cmdout = stdout;
	cmdin  = stdin;
	buf = stralloc(get_client_line(cmdin));
	check_security_buffer(buf);
    }
    else {
	ctlfdout  = DATA_FD_OFFSET + 0;
	ctlfdin   = DATA_FD_OFFSET + 1;
	datafdout = DATA_FD_OFFSET + 2;
	close(DATA_FD_OFFSET +3);

	/* read the REQ packet */
	for(; (line = agets(stdin)) != NULL; free(line)) {
	    if(strncmp_const(line, "OPTIONS ") == 0) {
                if (g_options)
                    error(_("ERROR recover program sent multiple OPTIONS"));
		g_options = parse_g_options(line+8, 1);
		if(!g_options->hostname) {
		    g_options->hostname = alloc(MAX_HOSTNAME_LENGTH+1);
		    gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		    g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
		}
	    }
	}
	amfree(line);

	if(amandad_auth && g_options->auth) {
	    if(strcasecmp(amandad_auth, g_options->auth) != 0) {
		g_printf(_("ERROR recover program ask for auth=%s while amidxtaped is configured for '%s'\n"),
		       g_options->auth, amandad_auth);
		error(_("ERROR recover program ask for auth=%s while amidxtaped is configured for '%s'"),
		      g_options->auth, amandad_auth);
		/*NOTREACHED*/
	    }
	}
	/* send the REP packet */
	g_printf("CONNECT CTL %d DATA %d\n", DATA_FD_OFFSET, DATA_FD_OFFSET+1);
	g_printf("\n");
	fflush(stdout);
	fclose(stdin);
	fclose(stdout);
	cmdout = fdopen(ctlfdout, "a");
	if (!cmdout) {
	    error(_("amidxtaped: Can't fdopen(ctlfdout): %s"), strerror(errno));
	    /*NOTREACHED*/
	}
	cmdin = fdopen(ctlfdin, "r");
	if (!cmdin) {
	    error(_("amidxtaped: Can't fdopen(ctlfdin): %s"), strerror(errno));
	    /*NOTREACHED*/
	}
    }

    ds = dumpspec_new(NULL, NULL, NULL, NULL);
    for (re_end = 0; re_end == 0; ) {
	char *s, ch;
	amfree(buf);
	buf = stralloc(get_client_line(cmdin));
	s = buf;
	if(strncmp_const_skip(buf, "LABEL=", s, ch) == 0) {
	    tapes = unmarshal_tapelist_str(s);
	}
	else if(strncmp_const_skip(buf, "FSF=", s, ch) == 0) {
	    rst_flags->fsf = OFF_T_ATOI(s);
	}
	else if(strncmp_const_skip(buf, "HEADER", s, ch) == 0) {
	    rst_flags->headers = 1;
	    rst_flags->header_to_fd = -1;
	}
	else if(strncmp_const_skip(buf, "FEATURES=", s, ch) == 0) {
	    char *our_feature_string = NULL;
	    their_feature_string = stralloc(s);
	    am_release_feature_set(their_features);
	    their_features = am_string_to_feature(their_feature_string);
	    amfree(their_feature_string);
	    our_feature_string = am_feature_to_string(our_features);
	    if(from_amandad == 1) 
		g_fprintf(cmdout,"FEATURES=%s\r\n", our_feature_string);
	    else
		g_fprintf(cmdout,"%s", our_feature_string);
	    fflush(cmdout);
	    amfree(our_feature_string);
	}
	else if(strncmp_const_skip(buf, "DEVICE=", s, ch) == 0) {
	    rst_flags->alt_tapedev= stralloc(s);
	}
	else if(strncmp_const_skip(buf, "HOST=", s, ch) == 0) {
            if (ds->host) {
                dbprintf(_("WARNING: HOST appeared twice in client request.\n"));
                amfree(ds->host);
            }
            ds->host = stralloc(s);
	}
	else if(strncmp_const_skip(buf, "DISK=", s, ch) == 0) {
            if (ds->disk) {
                dbprintf(_("WARNING: DISK appeared twice in client request.\n"));
                amfree(ds->disk);
            }
	    ds->disk = stralloc(s);
	}
	else if(strncmp_const_skip(buf, "DATESTAMP=", s, ch) == 0) {
            if (ds->datestamp) {
                dbprintf(_("WARNING: DATESTAMP appeared twice in client request.\n"));
                amfree(ds->datestamp);
            }
	    ds->datestamp = stralloc(s);
	}
	else if(strncmp_const(buf, "END") == 0) {
	    re_end = 1;
	}
	else if(strncmp_const_skip(buf, "CONFIG=", s, ch) == 0) {
	    re_config = stralloc(s);
	    if(strlen(re_config) == 0)
		amfree(re_config);
	}
	else if(buf[0] != '\0' && buf[0] >= '0' && buf[0] <= '9') {
	    re_end = 1;
	}
    }
    amfree(buf);

    if(re_config) {
	config_init(CONFIG_INIT_EXPLICIT_NAME, re_config);
	dbrename(re_config, DBG_SUBDIR_SERVER);
    } else {
	config_init(0, NULL);
    }

    if (config_errors(NULL) >= CFGERR_ERRORS) {
	g_critical(_("errors processing config file"));
    }

    check_running_as(RUNNING_AS_DUMPUSER_PREFERRED);

    if(tapes &&
       (!rst_flags->alt_tapedev  ||
        (re_config && ( strcmp(rst_flags->alt_tapedev,
                               getconf_str(CNF_AMRECOVER_CHANGER)) == 0 ||
                        strcmp(rst_flags->alt_tapedev,
                               getconf_str(CNF_TPCHANGER)) == 0 ) ) ) ) {
	/* We need certain options, if restoring from more than one tape */
        if(tapes->next && !am_has_feature(their_features, fe_recover_splits)) {
            error(_("Client must support split dumps to restore requested data."));
            /*NOTREACHED*/
        }
	dbprintf(_("Restoring from changer, checking labels\n"));
	rst_flags->check_labels = 1;
	use_changer = 1;
    }

    /* build the dumpspec list from our single dumpspec */
    dumpspecs = g_slist_append(NULL, (gpointer)ds);
    ds = NULL;

    if(!tapes && rst_flags->alt_tapedev){
	dbprintf(_("Looks like we're restoring from a holding file...\n"));
        tapes = unmarshal_tapelist_str(rst_flags->alt_tapedev);
	tapes->isafile = 1;
	amfree(rst_flags->alt_tapedev);
	rst_flags->alt_tapedev = NULL;
        use_changer = FALSE;
    } 

    tapedev = getconf_str(CNF_TAPEDEV);
    /* If we'll be stepping on the tape server's devices, lock them. */
    if(re_config &&
       (use_changer || (rst_flags->alt_tapedev && tapedev &&
                        strcmp(rst_flags->alt_tapedev, tapedev) == 0) ) ) {
	dbprintf(_("Locking devices\n"));
	parent_pid = getpid();
	atexit(cleanup);
	get_lock = lock_logfile();
    }
    if (get_lock)
	log_add(L_INFO, "%s pid %ld", get_pname(), (long)getpid());

    /* Init the tape changer */
    if(tapes && use_changer && changer_init() == 0) {
	dbprintf(_("No changer available\n"));
    }

    /* Read the default block size from the tape type */
    if(re_config && (conf_tapetype = getconf_str(CNF_TAPETYPE)) != NULL) {
	tape = lookup_tapetype(conf_tapetype);
	rst_flags->blocksize = tapetype_get_blocksize(tape) * 1024;
    }

    if(rst_flags->fsf && re_config &&
       getconf_boolean(CNF_AMRECOVER_DO_FSF) == 0) {
	rst_flags->fsf = (off_t)0;
    }

    if (!use_changer && re_config &&
	getconf_boolean(CNF_AMRECOVER_CHECK_LABEL) == 0) {
	rst_flags->check_labels = 0;
    }

    /* establish a distinct data connection for dumpfile data */
    if(am_has_feature(their_features, fe_recover_splits)) {
	if(from_amandad == 1) {
	    rst_flags->pipe_to_fd = datafdout;
	}
	else {
	    int data_fd;
	    char *buf;

	    dbprintf(_("Client understands split dumpfiles\n"));

	    if((data_sock = stream_server(AF_INET, &data_port, STREAM_BUFSIZE, 
		 STREAM_BUFSIZE, 0)) < 0){
		error(_("could not create data socket: %s"), strerror(errno));
		/*NOTREACHED*/
	    }
	    dbprintf(_("Local port %d set aside for data\n"), data_port);

	    /* tell client where to connect */
	    g_printf(_("CONNECT %hu\n"), (unsigned short)data_port);
	    fflush(stdout);

	    if((data_fd = stream_accept(data_sock, TIMEOUT, STREAM_BUFSIZE, 
                 STREAM_BUFSIZE)) < 0){
		error(_("stream_accept failed for client data connection: %s\n"),
		      strerror(errno));
		/*NOTREACHED*/
	    }

	    buf = get_client_line_fd(data_fd);

	    check_security_buffer(buf);
	    rst_flags->pipe_to_fd = data_fd;
	}
    }
    else {
	rst_flags->pipe_to_fd = fileno(stdout);
        cmdout = stderr;
    }
    dbprintf(_("Sending output to file descriptor %d\n"), rst_flags->pipe_to_fd);


    tapedev = getconf_str(CNF_TAPEDEV);
    if(get_lock == 0 &&
       re_config && 
       (use_changer || (rst_flags->alt_tapedev && tapedev &&
                        strcmp(rst_flags->alt_tapedev, tapedev) == 0) ) ) {
	char *process_name = get_master_process(rst_conf_logfile);
	send_message(cmdout, rst_flags, their_features,
		     _("%s exists: %s is already running, "
		     "or you must run amcleanup"), 
		     rst_conf_logfile, process_name);
	error(_("%s exists: %s is already running, "
	      "or you must run amcleanup"),
	      rst_conf_logfile, process_name);
    }

    /* make sure our restore flags aren't crazy */
    if (check_rst_flags(rst_flags) == -1) {
	if (rst_flags->pipe_to_fd != -1)
	    aclose(rst_flags->pipe_to_fd);
	send_message(cmdout, rst_flags, their_features,
		     _("restore flags are crazy"));
	exit(1);
    }

    /* actual restoration */
    search_tapes(cmdout, cmdin, use_changer, tapes, dumpspecs, rst_flags,
		 their_features);
    dbprintf(_("Restoration finished\n"));

    /* cleanup */
    if(rst_flags->pipe_to_fd != -1) aclose(rst_flags->pipe_to_fd);
    free_tapelist(tapes);

    am_release_feature_set(their_features);

    amfree(rst_flags->alt_tapedev);
    amfree(rst_flags);
    dumpspec_list_free(dumpspecs);
    amfree(re_config);
    dbclose();
    return 0;
}

static void
cleanup(void)
{
    if (parent_pid == getpid()) {
	if (get_lock) {
	    log_add(L_INFO, "pid-done %ld\n", (long)getpid());
	    unlink(rst_conf_logfile);
	}
    }
}
