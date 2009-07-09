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
/*
 * $Id: amcleanupdisk.c 7238 2007-07-06 20:03:37Z dustin $
 */
#include "amanda.h"

#include "conffile.h"
#include "diskfile.h"
#include "clock.h"
#include "holding.h"
#include "infofile.h"
#include "server_util.h"

/* Utility funcitons */

/* Call open_infofile() with the infofile from the configuration
 */
static void
init_infofile(void)
{
    char *conf_infofile = config_dir_relative(getconf_str(CNF_INFOFILE));
    if (open_infofile(conf_infofile) < 0) {
	error(_("could not open info db \"%s\""), conf_infofile);
	/*NOTREACHED*/
    }
    amfree(conf_infofile);
}

/* A callback for holding_cleanup to mark corrupt DLEs with force_no_bump
 * for their next run.
 *
 * @param hostname: hostname of DLE
 * @param disk: disk of DLE
 */
static void
corrupt_dle(
    char *hostname,
    char *disk)
{
    info_t info;

    dbprintf(_("Corrupted dump of DLE %s:%s found; setting force-no-bump.\n"),
	hostname, disk);

    get_info(hostname, disk, &info);
    info.command &= ~FORCE_BUMP;
    info.command |= FORCE_NO_BUMP;
    if(put_info(hostname, disk, &info)) {
	dbprintf(_("could not put info record for %s:%s: %s"),
	      hostname, disk, strerror(errno));
    }
}

int
main(
    int		argc,
    char **	argv)
{
    FILE *verbose_output = NULL;
    char *cfg_opt = NULL;
    char *conf_diskfile;
    disklist_t diskq;

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

    set_pname("amcleanupdisk");

    dbopen(DBG_SUBDIR_SERVER);

    if(argc < 2) {
	error(_("Usage: amcleanupdisk <config>"));
	/*NOTREACHED*/
    }

    /* parse options */
    if (argc >= 2 && strcmp(argv[1], "-v") == 0) {
	verbose_output = stderr;
	cfg_opt = argv[2];
    } else {
	cfg_opt = argv[1];
    }

    config_init(CONFIG_INIT_EXPLICIT_NAME,
		cfg_opt);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &diskq);
    /* diskq also ends up in a global, used by holding_cleanup */
    amfree(conf_diskfile);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    init_infofile();

    /* actually perform the cleanup */
    holding_cleanup(corrupt_dle, verbose_output);

    close_infofile();
    return 0;
}

