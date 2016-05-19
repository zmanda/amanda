/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * $Id: amtrmlog.c,v 1.17 2006/07/25 18:27:57 martinea Exp $
 *
 * trims number of index files to only those still in system.  Well
 * actually, it keeps a few extra, plus goes back to the last level 0
 * dump.
 */

#include "amanda.h"
#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "find.h"

int amtrmidx_debug = 0;

int main(int argc, char **argv);

int
main(
    int		argc,
    char **	argv)
{
    disklist_t diskl;
    int no_keep;			/* files per system to keep */
    GHashTable *hash_output_find_log;
    DIR *dir;
    struct dirent *adir;
    int useful;
    char *olddir;
    time_t today, date_keep;
    char *logname = NULL;
    struct stat stat_log;
    struct stat stat_old;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_logdir;
    int dumpcycle;
    config_overrides_t *cfg_ovr = NULL;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("amtrmlog-%s\n", VERSION);
	return (0);
    }

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

    set_pname("amtrmlog");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    cfg_ovr = extract_commandline_config_overrides(&argc, &argv);

    if (argc > 1 && g_str_equal(argv[1], "-t")) {
	amtrmidx_debug = 1;
	argc--;
	argv++;
    }

    if (argc < 2) {
	g_fprintf(stderr, _("Usage: %s [-t] <config> [-o configoption]*\n"), argv[0]);
	return 1;
    }

    dbopen(DBG_SUBDIR_SERVER);
    dbprintf(_("%s: version %s\n"), argv[0], VERSION);

    set_config_overrides(cfg_ovr);
    config_init_with_global(CONFIG_INIT_EXPLICIT_NAME, argv[1]);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &diskl);
    amfree(conf_diskfile);

    if (config_errors(NULL) >= CFGERR_WARNINGS) {
	config_print_errors();
	if (config_errors(NULL) >= CFGERR_ERRORS) {
	    g_critical(_("errors processing config file"));
	}
    }

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
    if (read_tapelist(conf_tapelist)) {
	error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_tapelist);

    today = time((time_t *)NULL);
    dumpcycle = getconf_int(CNF_DUMPCYCLE);
    if (dumpcycle > 30)
	dumpcycle = 30;
    date_keep = today - (dumpcycle * 86400);

    hash_output_find_log = hash_find_log();

    /* determine how many log to keep */
    no_keep = getconf_int(CNF_TAPECYCLE) * 2;
    if (no_keep > 30)
	no_keep = 30;

    dbprintf(plural(_("Keeping %d log file\n"),
		    _("Keeping %d log files\n"), no_keep),
	     no_keep);

    conf_logdir = config_dir_relative(getconf_str(CNF_LOGDIR));
    olddir = g_strjoin(NULL, conf_logdir, "/oldlog", NULL);
    if (mkpdir(olddir, 0700, (uid_t)-1, (gid_t)-1) != 0) {
	error(_("could not create parents of %s: %s"), olddir, strerror(errno));
	/*NOTREACHED*/
    }
    if (mkdir(olddir, 0700) != 0 && errno != EEXIST) {
	error(_("could not create %s: %s"), olddir, strerror(errno));
	/*NOTREACHED*/
    }

    if (stat(olddir,&stat_old) == -1) {
	error(_("can't stat oldlog directory \"%s\": %s"), olddir, strerror(errno));
	/*NOTREACHED*/
    }

    if (!S_ISDIR(stat_old.st_mode)) {
	error(_("Oldlog directory \"%s\" is not a directory"), olddir);
	/*NOTREACHED*/
    }

    if ((dir = opendir(conf_logdir)) == NULL) {
	error(_("could not open log directory \"%s\": %s"), conf_logdir,strerror(errno));
	/*NOTREACHED*/
    }
    while ((adir=readdir(dir)) != NULL) {
	if (g_str_has_prefix(adir->d_name, "log.")) {
	    char *name = g_strdup(adir->d_name);
	    char *dot;
	    useful=0;
	    dot = strchr(name, '.');
	    if (dot) {
		dot++;
		dot = strchr(dot, '.');
		if (dot)
		    *dot = '\0';
		if (g_hash_table_lookup(hash_output_find_log, name)) {
		    useful = 1;
		}
	    }
	    g_free(name);
	    logname = g_strconcat(conf_logdir, "/", adir->d_name, NULL);
	    if (!useful && stat(logname,&stat_log) == 0) {
		if ((time_t)stat_log.st_mtime > date_keep) {
		    useful = 1;
		}
	    }
	    if (useful == 0) {
		char *d;
		char *datestamp;
		char *amdumpfile;
		char *newfile;
		char *oldfile;

		g_free(newfile);
		oldfile = g_strconcat(conf_logdir, "/", adir->d_name, NULL);
		newfile = g_strconcat(olddir, "/", adir->d_name, NULL);
		if (rename(oldfile, newfile) != 0) {
		    error(_("could not rename \"%s\" to \"%s\": %s"),
			  logname, newfile, strerror(errno));
		    /*NOTREACHED*/
		}
		amfree(newfile);
		amfree(oldfile);

		datestamp = g_strdup(adir->d_name);
		d = strrchr(datestamp+4, '.');
		if (*d) *d = '\0';
		amdumpfile = g_strconcat(conf_logdir, "/amdump.", datestamp+4, NULL);
		if (unlink(amdumpfile) != 0 && errno != ENOENT) {
		    g_debug("Failed to unlink '%s': %s", amdumpfile, strerror(errno));
		}
		g_free(datestamp);
		g_free(amdumpfile);
	    }
	}
    }
    closedir(dir);
    free_dump_hash(hash_output_find_log);
    amfree(logname);
    amfree(olddir);
    amfree(conf_logdir);
    clear_tapelist();
    free_disklist(&diskl);
    unload_disklist();

    dbclose();

    return 0;
}
