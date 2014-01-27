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
    {NULL, 0, NULL, 0}
};

static int copy_stream = 0;
static time_t conf_ctimeout;
static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static int remote_errors = 0;
static event_handle_t *event_in;
static security_stream_t *fd;

/* local functions */

void usage(void);
void client_protocol(char *hostname, char *auth, char *service,
		     FILE *input_file);
void client_first_stream(security_handle_t *sech, int port_num);
int main(int argc, char **argv);
static void read_in(void *cookie);
void aaa(void);
static void read_server(void *cookie, void *buf, ssize_t size);

void
usage(void)
{
    error(_("Usage: amservice [--version] [-o configoption]* [-f input_file [-s]] host auth service"));
    /*NOTREACHED*/
}

int
main(
    int		argc,
    char **	argv)
{
    config_overrides_t *cfg_ovr;
    char *hostname;
    char *auth;
    char *service;
    int opt;
    extern int optind;
    extern char *optarg;
    FILE *input_file;
    int use_connect = 0;
    int got_input_file = 0;

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

    set_pname("amservice");
    /* drop root privileges */
    if (!set_root_privs(0)) {
	error(_("amservice must be run setuid root"));
    }

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);

    add_amanda_log_handler(amanda_log_stderr);

    our_features = am_init_feature_set();
    our_feature_string = am_feature_to_string(our_features);

    /* process arguments */

    cfg_ovr = new_config_overrides(argc/2);
    input_file = stdin;
    while((opt = getopt_long(argc, argv, "o:f:s", long_options, NULL)) != EOF) {
	switch(opt) {
	case 1:		printf("amservice-%s\n", VERSION);
			return(0);
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
			    g_critical("Cannot open output file '%s': %s",
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

    argc -= optind, argv += optind;
    if(argc < 3) usage();

    /* set a default config */
    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_CLIENT|CONFIG_INIT_GLOBAL, NULL);
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
    service = argv[2];

    /* start client side checks */

    copy_stream = use_connect && got_input_file;
    client_protocol(hostname, auth, service, input_file);

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
    FILE        *input_file)
{
    GString *strbuf = g_string_new(NULL);
    char *req, *req1;

    g_string_append_printf(strbuf, "SERVICE %s\nOPTIONS features=%s\n",
        service, our_feature_string);

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
    int port_num = 0;
    int has_error = 0;

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
	    g_debug("REP: %s\n", line);
	} else {
	    fprintf(stdout, "%s\n", line);
	}
	if (g_str_has_prefix(line, "CONNECT ")) {
	    char *port = strchr(line, ' ');
	    if (port) {
		port = strchr(port+1, ' ');
		if (port) {
		    port_num = atoi(port+1);
		}
	    }
	} else if (g_str_has_prefix(line, "ERROR ")) {
	    if (copy_stream) {
		fprintf(stdout, "%s\n", line);
	    }
	    has_error++;
	}
    }

    if (has_error)
	return;

    if (copy_stream) {
	client_first_stream(sech, port_num);
    } else {
	fprintf(stdout, "\n");
    }

}

void
client_first_stream(
    security_handle_t *sech,
    int port_num)
{

    if (port_num == 0) {
	g_critical("The service did not ask to open stream, do not use '-s' with that service");
    }

    fd = security_stream_client(sech, port_num);
    if (!fd) {
	g_critical("Could not connect to stream: %s\n", security_stream_geterror(fd));
    }
    if (security_stream_auth(fd) < 0) {
	g_critical("could not authenticate stream: %s\n", security_stream_geterror(fd));
    }

    printf("Connected\n");
    /* read from stdin */
    event_in = event_register((event_id_t)0, EV_READFD, read_in, NULL);

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
    switch (size) {
    case -1:
    case  0: security_stream_close(fd);
	     event_release(event_in);
	     break;
    default:
	full_write(1, buf, size);
	if (errno > 0) {
	    g_debug("failed to write to stdout: %s", strerror(errno));
	}
	break;
    }
}
