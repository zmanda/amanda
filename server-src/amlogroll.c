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

int main(int argc, char **argv)
{
    char *conffile;
    char *logfname;
    char *conf_logdir;
    FILE *logfile;
    unsigned long malloc_hist_1, malloc_size_1;
    unsigned long malloc_hist_2, malloc_size_2;
    char my_cwd[STR_SIZE];
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;

    safe_fd(-1, 0);

    set_pname("amlogroll");

    dbopen(DBG_SUBDIR_SERVER);

    malloc_size_1 = malloc_inuse(&malloc_hist_1);

    /* Process options */
    
    erroutput_type = ERR_INTERACTIVE;

    if (getcwd(my_cwd, SIZEOF(my_cwd)) == NULL) {
	error("cannot determine current working directory");
	/*NOTREACHED*/
    }

    parse_server_conf(argc, argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    if (my_argc < 2) {
	config_dir = stralloc2(my_cwd, "/");
	if ((config_name = strrchr(my_cwd, '/')) != NULL) {
	    config_name = stralloc(config_name + 1);
	}
    } else {
	config_name = stralloc(my_argv[1]);
	config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    }

    safe_cd();

    /* read configuration files */

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if(read_conffile(conffile)) {
        error("errors processing config file \"%s\"", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    conf_logdir = getconf_str(CNF_LOGDIR);
    if (*conf_logdir == '/') {
        conf_logdir = stralloc(conf_logdir);
    } else {
        conf_logdir = stralloc2(config_dir, conf_logdir);
    }
    logfname = vstralloc(conf_logdir, "/", "log", NULL);
    amfree(conf_logdir);

    if((logfile = fopen(logfname, "r")) == NULL) {
	error("could not open log %s: %s", logfname, strerror(errno));
	/*NOTREACHED*/
    }
    amfree(logfname);

    erroutput_type |= ERR_AMANDALOG;
    set_logerror(logerror);

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
    amfree(config_dir);
    amfree(config_name);
    free_new_argv(new_argc, new_argv);
    free_server_config();

    malloc_size_2 = malloc_inuse(&malloc_hist_2);

    if(malloc_size_1 != malloc_size_2) {
	malloc_list(fileno(stderr), malloc_hist_1, malloc_hist_2);
    }

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
#define sc "date"
	if(ch == '\0' || strncmp(s - 1, sc, SIZEOF(sc)-1) != 0) {
	    return;				/* ignore bogus line */
	}
	s += SIZEOF(sc) - 1;
	ch = s[-1];
#undef sc
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
