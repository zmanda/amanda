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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: amlogroll.c,v 1.14 2006/07/25 18:27:57 martinea Exp $
 *
 * rename a live log file to the datestamped name.
 */

#include "amanda.h"
#include "conffile.h"
#include "logfile.h"
#include "version.h"

char *datestamp;

void handle_start(void);
int main(int argc, char **argv);

int
main(
    int		argc,
    char **	argv)
{
    char *logfname;
    char *conf_logdir;
    FILE *logfile;
    config_overwrites_t *cfg_ovr = NULL;
    char *cfg_opt = NULL;

    /*
     * Configure program for internationalization:
     *   1) Only set the message locale for now.
     *   2) Set textdomain for all amanda related programs to "amanda"
     *      We don't want to be forced to support dozens of message catalogs.
     */  
    setlocale(LC_MESSAGES, "C");
    textdomain("amanda"); 

    safe_fd(-1, 0);

    set_pname("amlogroll");

    dbopen(DBG_SUBDIR_SERVER);

    add_amanda_log_handler(amanda_log_stderr);

    /* Process options */
    cfg_ovr = extract_commandline_config_overwrites(&argc, &argv);

    if (argc >= 2) {
	cfg_opt = argv[1];
    }

    /* read configuration files */

    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, cfg_opt);
    apply_config_overwrites(cfg_ovr);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    safe_cd(); /* must happen after config_init */

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    logfname = vstralloc(conf_logdir, "/", "log", NULL);
    amfree(conf_logdir);

    if((logfile = fopen(logfname, "r")) == NULL) {
	error(_("could not open log %s: %s"), logfname, strerror(errno));
	/*NOTREACHED*/
    }
    amfree(logfname);

    add_amanda_log_handler(amanda_log_trace_log);

    while(get_logline(logfile)) {
	if(curlog == L_START) {
	    handle_start();
	    if(datestamp != NULL) {
		break;
	    }
	}
    }
    afclose(logfile);
 
    log_rename(datestamp);

    amfree(datestamp);

    dbclose();

    return 0;
}

void handle_start(void)
{
    static int started = 0;
    char *s, *fp;
    int ch;

    if(!started) {
	s = curstr;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0' || strncmp_const_skip(s - 1, "date", s, ch) != 0) {
	    return;				/* ignore bogus line */
	}

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    return;
	}
	fp = s - 1;
	skip_non_whitespace(s, ch);
	s[-1] = '\0';
	datestamp = newstralloc(datestamp, fp);
	s[-1] = (char)ch;

	started = 1;
    }
}
