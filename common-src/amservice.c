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
 * $Id: amservice.c 11167 2008-05-06 11:53:54Z martineau $
 *
 * Take the REQ packet in stdin and output the REP packet in stdout
 */
#include "amanda.h"
#include "amutil.h"
#include "conffile.h"
#include "packet.h"
#include "protocol.h"
#include "amfeatures.h"
#include "event.h"
#include "getopt.h"

static struct option long_options[] = {
    {"version"         , 0, NULL,  1},
    {"features"        , 1, NULL,  2},
    {"stream"          , 1, NULL,  3},
    {"config"          , 1, NULL,  4},
    {NULL, 0, NULL, 0}
};

static int copy_stream = 0;
static time_t conf_ctimeout;
static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static int remote_errors = 0;
static event_handle_t *event_in;
static security_stream_t *fd;
static gboolean event_paused = FALSE;

/* local functions */

void usage(void);
void client_protocol(char *hostname, char *auth, char *service, char *config,
		     FILE *input_file);
static void connect_streams(security_handle_t *sech);
static void client_first_stream(security_handle_t *sech, int port_num);
int main(int argc, char **argv);
static void read_stream_in(void *cookie);
static void read_stream_server(void *cookie, void *buf, ssize_t size);

static void read_in(void *cookie);
static void read_server(void *cookie, void *buf, ssize_t size);

void
usage(void)
{
    fprintf(stderr, _("Usage: amservice [--version] [-o configoption]* [-f input_file [-s]]\n"
                      "                 [--config CONFIG] [--features FEATURES-STRING]\n"
                      "                 [--stream NAME,IN,OUT]* host auth service\n"));
    exit(1);
    /*NOTREACHED*/
}

typedef struct lstream_t {
    char             *name;
    int               fd_in;
    int               fd_out;
    event_handle_t   *event;
    gboolean          event_paused;
    struct rstream_t *rstream;
} lstream_t;

typedef struct rstream_t {
    char              *name;
    int                port;
    security_stream_t *fd;
    lstream_t         *lstream;
} rstream_t;

static int nb_lstream = 0;
static lstream_t lstreams[DATA_FD_COUNT];
static int nb_rstream = 0;
static rstream_t rstreams[DATA_FD_COUNT];

int
main(
    int		argc,
    char **	argv)
{
    config_overrides_t *cfg_ovr;
    char *hostname;
    char *auth;
    char *service;
    char *config = NULL;
    int opt;
    extern int optind;
    extern char *optarg;
    FILE *input_file;
    int use_connect = 0;
    int got_input_file = 0;
    int i;
    unsigned char gfd[32768];

    glib_init();

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda");

//    safe_fd(-1, 0);
    safe_cd();

    set_pname("amservice");

    if (geteuid() != getuid()) {
	error(_("amservice must not be setuid root"));
    }

    /* drop root privileges */
    set_root_privs(-1);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    add_amanda_log_handler(amanda_log_stderr);

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);
    /* process arguments */

    for (i=0; i<argc; i++) {
	g_debug("argv[%d] = %s", i, argv[i]);
    }

    for (i = 0;i < 32768; i++) {
	gfd[i] = 0;
    }

    cfg_ovr = new_config_overrides(argc/2);
    input_file = stdin;
    while((opt = getopt_long(argc, argv, "o:f:s", long_options, NULL)) != EOF) {
	switch(opt) {
	case 1:		printf("amservice-%s\n", VERSION);
			return(0);
			break;
	case 2:		g_free(our_feature_string);
			g_free(our_features);
			our_feature_string = g_strdup(optarg);
			our_features = am_string_to_feature(our_feature_string);
			break;
	case 3: {	gchar *copy_optarg = g_strdup(optarg);
			gchar *coma = strchr(copy_optarg, ',');
			gchar *stream_in;
			if (nb_lstream == DATA_FD_COUNT) {
			    g_critical("Too many --stream, maximum is %d",
				       DATA_FD_COUNT);
			    exit(1);
			} else if (coma) {
			    *coma++ = '\0';
			    stream_in = coma;
			    coma = strchr(coma, ',');
			    if (coma) {
				*coma++ = '\0';
				lstreams[nb_lstream].name = g_strdup(copy_optarg);
				lstreams[nb_lstream].fd_in = atoi(stream_in);
				lstreams[nb_lstream].fd_out = atoi(coma);
				gfd[lstreams[nb_lstream].fd_in] = 1;
				gfd[lstreams[nb_lstream].fd_out] = 1;
				nb_lstream++;
			    }
			}
			if (!coma) {
			    g_critical("Invalid --stream option (%s)", optarg);
			    exit(1);
			}
			g_free(copy_optarg);
			break;
		  }
	case 4:		g_free(config);
			config = g_strdup(optarg);
			break;
	case 'o':	add_config_override_opt(cfg_ovr, optarg);
			break;
	case 'f':	if (got_input_file == 1) {
			    g_critical("Invalid two -f argument");
			    exit(1);
			}
			got_input_file = 1;
			if (*optarg == '/') {
			    input_file = fopen(optarg, "r");
			} else {
			    char *name = g_strjoin(NULL, get_original_cwd(), "/",
						   optarg, NULL);
			    input_file = fopen(name, "r");
			    amfree(name);
			}
			if (!input_file) {
			    g_critical("Cannot open input file '%s': %s",
				optarg, strerror(errno));
			    exit(1);
			}
			break;
	case 's':	use_connect = 1;
			break;
	}
    }

    if (use_connect && !got_input_file) {
	g_critical("The -s option require -f");
	exit(1);
    }

    /* close all unused fd */
    for (i = 3;i < 32768; i++) {
	if (gfd[i] == 0 && i != dbfd() &&
	    (!got_input_file ||  i != fileno(input_file))) {
	    close(i);
	}
    }
    argc -= optind, argv += optind;
    if(argc < 3) usage();

    /* set a default config */
    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);
    if (config) {
	config_init(CONFIG_INIT_CLIENT | CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_OVERLAY, config);
    }
    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    conf_ctimeout = (time_t)getconf_int(CNF_CTIMEOUT);

    hostname = argv[0];
    auth = argv[1];
    if (g_str_equal(auth,"NULL")) {
	auth = getconf_str(CNF_AUTH);
    }
    service = argv[2];

    /* start client side checks */

    copy_stream = use_connect && got_input_file;
    client_protocol(hostname, auth, service, config, input_file);

    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;
    if (got_input_file)
	fclose(input_file);

    dbclose();
    return(remote_errors != 0);
}

/* --------------------------------------------------- */

static void handle_result(void *, pkt_t *, security_handle_t *);
void start_host(char *hostname, char *auth, char *req);

void
start_host(
    char        *hostname,
    char        *auth,
    char        *req)
{
    const security_driver_t *secdrv;
    secdrv = security_getdriver(auth);
    if (secdrv == NULL) {
	fprintf(stderr, _("Could not find security driver \"%s\".\n"), auth);
    } else {
	protocol_sendreq(hostname, secdrv, generic_client_get_security_conf,
			 req, conf_ctimeout, handle_result, NULL);
    }

}

void
client_protocol(
    char        *hostname,
    char        *auth,
    char        *service,
    char        *config,
    FILE        *input_file)
{
    GString *strbuf = g_string_new(NULL);
    char *req, *req1;

    g_string_append_printf(strbuf, "SERVICE %s\nOPTIONS ", service);
    g_string_append_printf(strbuf, "features=%s;", our_feature_string);
    g_string_append_printf(strbuf, "hostname=%s;", hostname);
    if (config) {
	g_string_append_printf(strbuf, "config=%s;", config);
    }
    g_string_append_printf(strbuf, "\n");

    req1 = g_malloc(1024);
    while(fgets(req1, 1024, input_file) != NULL)
	g_string_append(strbuf, req1);

    g_free(req1);
    protocol_init();

    req = g_string_free(strbuf, FALSE);

    start_host(hostname, auth, req);

    protocol_run();

    fflush(stdout);

    amfree(our_feature_string);

    return;
}

static void
handle_result(
    void              *datap G_GNUC_UNUSED,
    pkt_t             *pkt,
    security_handle_t *sech)
{
    char *line;
    char *s;
    int ch;

    security_close_connection(sech, "AA");

    if (pkt == NULL) {
	g_fprintf(stdout,
		  _("Request failed: %s\n"), security_geterror(sech));
	remote_errors++;
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

	if (copy_stream) {
	    g_debug("REP: %s", line);
	} else {
	    fprintf(stdout, "%s\n", line);
	}
	if (g_str_has_prefix(line, "CONNECT ")) {
	    gchar *copy_line = g_strdup(line);
	    gchar *name;
	    gchar *port;
	    copy_line = strchr(copy_line, ' ');
	    if (copy_line) {
		copy_line++;
		name = copy_line;
		while (copy_line && (copy_line = strchr(copy_line, ' '))) {
		    *copy_line++ = '\0';
		    port = copy_line;
		    copy_line = strchr(copy_line, ' ');
		    if (copy_line) {
			*copy_line++ = '\0';
		    }
		    rstreams[nb_rstream].name = g_strdup(name);
		    rstreams[nb_rstream].port = atoi(port);
		    nb_rstream++;
		    name = copy_line;
		}
	    } else {
		remote_errors++;
	    }
	} else if (g_str_has_prefix(line, "ERROR ")) {
	    if (copy_stream) {
		fprintf(stdout, "%s\n", line);
	    }
	    remote_errors++;
	}
    }

    if (remote_errors)
	return;

    if (copy_stream) {
	client_first_stream(sech, rstreams[0].port);
    } else if (nb_rstream > 0) {
	fprintf(stdout, "\n");
	fclose(stdout);
	close(1);
	connect_streams(sech);
    } else {
	fprintf(stdout, "\n");
	fclose(stdout);
    }

}

static void
connect_streams(
    security_handle_t *sech)
{
    int r, l;

    for (r = 0; r < nb_rstream; r++) {
	for (l = 0; l < nb_lstream; l++) {
	    if (g_str_equal(rstreams[r].name, lstreams[l].name)) {
		security_stream_t *fd;

		rstreams[r].lstream = &lstreams[l];
		lstreams[l].rstream = &rstreams[r];
		fd = security_stream_client(sech, rstreams[r].port);
		if (!fd) {
		    g_critical("Could not connect to stream: security_stream_client failed\n");
		    exit(1);
		}
		if (security_stream_auth(fd) < 0) {
		    g_critical("could not authenticate stream: %s\n", security_stream_geterror(fd));
		    exit(1);
		}
		rstreams[r].fd = fd;
		/* read from server */
		lstreams[l].event = event_create((event_id_t)lstreams[l].fd_in, EV_READFD, read_stream_in, &lstreams[l]);
		event_activate(lstreams[l].event);

		/* read from connected stream */
		security_stream_read(fd, read_stream_server, &rstreams[r]);
	    }
	}
    }
    for (r = 0; r < nb_rstream; r++) {
	if (!rstreams[r].lstream) {
	    g_critical("Remote stream '%s' is not redirected", rstreams[r].name);
	    exit(1);
	}
    }
    for (l = 0; l < nb_lstream; l++) {
	if (!lstreams[l].rstream) {
	    g_critical("Stream '%s' is not connected", lstreams[l].name);
	    exit(1);
	}
    }
}

#define STACK_SIZE 1048576

static void read_stream_in_callback(void *, ssize_t, void *, ssize_t);
static void
read_stream_in_callback(
    void    *cookie G_GNUC_UNUSED,
    ssize_t  stack_size,
    void    *buf,
    ssize_t  size G_GNUC_UNUSED)
{
    int      l;

    g_free(buf);

    /* re-enable events */
    if (stack_size < STACK_SIZE && event_paused) {
	for (l = 0; l < nb_lstream; l++) {
	    if (lstreams[l].event_paused) {
		lstreams[l].event_paused = FALSE;
		lstreams[l].event = event_create((event_id_t)lstreams[l].fd_in, EV_READFD, read_stream_in, &lstreams[l]);
		event_activate(lstreams[l].event);
	    }
	}
	event_paused = FALSE;
    }
}

static void
read_stream_in(
    void *cookie)
{
    lstream_t *lstream = cookie;
    ssize_t  nread;
    char    *buf = g_malloc(65536);
    int      stack_size;
    int      l;

    nread = read(lstream->fd_in, buf, 65536);
    if (nread <= 0) {
	if (lstream->event) {
	    event_release(lstream->event);
	    lstream->event = NULL;
	}
	g_free(buf);
	security_stream_close_async(lstream->rstream->fd, read_stream_in_callback, NULL);
	return;
    }

    stack_size = security_stream_write_async(lstream->rstream->fd, buf, nread, read_stream_in_callback, NULL);

    /* pause events */
    if (stack_size > STACK_SIZE) {
	for (l = 0; l < nb_lstream; l++) {
	    if (lstreams[l].event) {
		event_release(lstreams[l].event);
		lstreams[l].event_paused = TRUE;
	    } else {
		lstreams[l].event_paused = FALSE;
	    }
	}
	event_paused = TRUE;
    }
}

static void
read_stream_server(
    void *      cookie,
    void *      buf,
    ssize_t     size)
{
    rstream_t *rstream = cookie;
    ssize_t result;

    switch (size) {
    case -1:
    case  0:
	     security_stream_read_cancel(rstream->fd);
	     close(rstream->lstream->fd_out);
	     rstream->lstream->fd_out = -1;
	     break;
    default:
	result = full_write(rstream->lstream->fd_out, buf, size);
	if (result != size) {
	    g_debug("failed to write to fd %d: %s", rstream->lstream->fd_out, strerror(errno));
	}
	break;
    }
}

static void
client_first_stream(
    security_handle_t *sech,
    int port_num)
{

    if (port_num == 0) {
	g_critical("The service did not ask to open stream, do not use '-s' with that service");
    }

    fd = security_stream_client(sech, port_num);
    if (!fd) {
	g_critical("Could not connect to stream: %s\n", security_geterror(sech));
    }
    if (security_stream_auth(fd) < 0) {
	g_critical("could not authenticate stream: %s\n", security_stream_geterror(fd));
    }

    printf("Connected\n");
    /* read from stdin */
    event_in = event_create((event_id_t)0, EV_READFD, read_in, NULL);
    event_activate(event_in);

    /* read from connected stream */
    security_stream_read(fd, read_server, NULL);
}


static void
read_in(
    void *cookie G_GNUC_UNUSED)
{
    ssize_t nread;
    char    buf[1025];

    nread = read(0, buf, 1024);
    if (nread <= 0) {
	event_release(event_in);
	security_stream_close(fd);
	return;
    }

    buf[nread] = '\0';
    security_stream_write(fd, buf, nread);
}

static void
read_server(
    void *      cookie G_GNUC_UNUSED,
    void *      buf,
    ssize_t     size)
{
    ssize_t result;

    switch (size) {
    case -1:
    case  0: security_stream_close(fd);
	     event_release(event_in);
	     break;
    default:
	result = full_write(1, buf, size);
	if (result != size) {
	    g_debug("failed to write to stdout: %s", strerror(errno));
	}
	break;
    }
}

