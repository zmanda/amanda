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
 * $Id: amservice.c 11167 2008-05-06 11:53:54Z martineau $
 *
 * Take the REQ packet in stdin and output the REP packet in stdout
 */
#include "amanda.h"
#include "util.h"
#include "conffile.h"
#include "packet.h"
#include "protocol.h"
#include "version.h"
#include "server_util.h"
#include "amfeatures.h"

static time_t conf_ctimeout;
static am_feature_t *our_features = NULL;
static char *our_feature_string = NULL;
static int remote_errors = 0;

/* local functions */

void usage(void);
void client_protocol(char *hostname, char *auth, char *service, FILE *input_file);
int main(int argc, char **argv);

void
usage(void)
{
    error(_("Usage: amservice%s [-o configoption]* [-f input_file] host auth service"),
	  versionsuffix());
    /*NOTREACHED*/
}

int
main(
    int		argc,
    char **	argv)
{
    config_overwrites_t *cfg_ovr;
    char *hostname;
    char *auth;
    char *service;
    int opt;
    extern int optind;
    extern char *optarg;
    FILE *input_file;

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

    cfg_ovr = new_config_overwrites(argc/2);
    input_file = stdin;
    while((opt = getopt(argc, argv, "o:f:")) != EOF) {
	switch(opt) {
	case 'o':	add_config_overwrite_opt(cfg_ovr, optarg);
			break;
	case 'f':	if (*optarg == '/') {
			    input_file = fopen(optarg, "r");
			} else {
			    char *name = vstralloc(get_original_cwd(), "/",
						   optarg, NULL);
			    input_file = fopen(name, "r");
			    amfree(name);
			}
			if (!input_file)
			    g_critical("Cannot open output file '%s': %s",
				optarg, strerror(errno));
			break;
	}
    }

    argc -= optind, argv += optind;
    if(argc < 3) usage();

    /* set a default config */
    config_init(CONFIG_INIT_CLIENT, NULL);
    apply_config_overwrites(cfg_ovr);
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

    client_protocol(hostname, auth, service, input_file);

    amfree(our_feature_string);
    am_release_feature_set(our_features);
    our_features = NULL;

    dbclose();
    return(remote_errors != 0);
}

/* --------------------------------------------------- */

static void handle_result(void *, pkt_t *, security_handle_t *);
void start_host(char *hostname, char *auth, char *req);

void
start_host(
    char *hostname,
    char *auth,
    char *req)
{
    const security_driver_t *secdrv;
    secdrv = security_getdriver(auth);
    if (secdrv == NULL) {
	fprintf(stderr, _("Could not find security driver \"%s\".\n"), auth);
    } else {
	protocol_sendreq(hostname, secdrv, amhost_get_security_conf, 
			 req, conf_ctimeout, handle_result, NULL);
    }

}

void
client_protocol(
    char *hostname,
    char *auth,
    char *service,
    FILE *input_file)
{
    char *req, *req1;

    req = g_strdup_printf("SERVICE %s\nOPTIONS features=%s\n",
			  service, our_feature_string);
    req1 = malloc(1024);
    while(fgets(req1, 1024, input_file) != NULL) {
	vstrextend(&req, req1, NULL);
    }
    protocol_init();

    start_host(hostname, auth, req);

    protocol_run();

    fflush(stdout);

    amfree(our_feature_string);

    return;
}

static void
handle_result(
    G_GNUC_UNUSED void *datap,
    pkt_t *		pkt,
    security_handle_t *	sech)
{
    char *line;
    char *s;
    int ch;

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

	fprintf(stdout, "%s\n", line);
    }
    fprintf(stdout, "\n");
}
