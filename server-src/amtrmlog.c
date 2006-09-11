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
 * $Id: amtrmlog.c,v 1.17 2006/07/25 18:27:57 martinea Exp $
 *
 * trims number of index files to only those still in system.  Well
 * actually, it keeps a few extra, plus goes back to the last level 0
 * dump.
 */

#include "amanda.h"
#include "arglist.h"
#ifdef HAVE_NETINET_IN_SYSTM_H
#include <netinet/in_systm.h>
#endif
#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "find.h"
#include "version.h"

int amtrmidx_debug = 0;

int main(int argc, char **argv);

int
main(
    int		argc,
    char **	argv)
{
    disklist_t diskl;
    int no_keep;			/* files per system to keep */
    char **output_find_log;
    DIR *dir;
    struct dirent *adir;
    char **name;
    int useful;
    char *olddir;
    char *oldfile = NULL, *newfile = NULL;
    time_t today, date_keep;
    char *logname = NULL;
    struct stat stat_log;
    struct stat stat_old;
    char *conffile;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_logdir;
    int dumpcycle;
    int    new_argc,   my_argc;
    char **new_argv, **my_argv;

    safe_fd(-1, 0);
    safe_cd();

    set_pname("amtrmlog");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    parse_server_conf(argc, argv, &new_argc, &new_argv);
    my_argc = new_argc;
    my_argv = new_argv;

    if (my_argc > 1 && strcmp(my_argv[1], "-t") == 0) {
	amtrmidx_debug = 1;
	my_argc--;
	my_argv++;
    }

    if (my_argc < 2) {
	fprintf(stderr, "Usage: %s [-t] <config> [-o configoption]*\n", my_argv[0]);
	return 1;
    }

    dbopen(DBG_SUBDIR_SERVER);
    dbprintf(("%s: version %s\n", my_argv[0], version()));

    config_name = my_argv[1];

    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if (read_conffile(conffile)) {
	error("errors processing amanda config file \"%s\"", conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    report_bad_conf_arg();

    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if (read_diskfile(conf_diskfile, &diskl) < 0) {
	error("could not load disklist \"%s\"", conf_diskfile);
	/*NOTREACHED*/
    }
    amfree(conf_diskfile);

    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if (read_tapelist(conf_tapelist)) {
	error("could not load tapelist \"%s\"", conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_tapelist);

    today = time((time_t *)NULL);
    dumpcycle = getconf_int(CNF_DUMPCYCLE);
    if(dumpcycle > 5000)
	dumpcycle = 5000;
    date_keep = today - (dumpcycle * 86400);

    output_find_log = find_log();

    /* determine how many log to keep */
    no_keep = getconf_int(CNF_TAPECYCLE) * 2;
    dbprintf(("Keeping %d log file%s\n", no_keep, (no_keep == 1) ? "" : "s"));

    conf_logdir = getconf_str(CNF_LOGDIR);
    if (*conf_logdir == '/') {
	conf_logdir = stralloc(conf_logdir);
    } else {
	conf_logdir = stralloc2(config_dir, conf_logdir);
    }
    olddir = vstralloc(conf_logdir, "/oldlog", NULL);
    if (mkpdir(olddir, 02700, (uid_t)-1, (gid_t)-1) != 0) {
	error("could not create parents of %s: %s", olddir, strerror(errno));
	/*NOTREACHED*/
    }
    if (mkdir(olddir, 02700) != 0 && errno != EEXIST) {
	error("could not create %s: %s", olddir, strerror(errno));
	/*NOTREACHED*/
    }

    if (stat(olddir,&stat_old) == -1) {
	error("can't stat oldlog directory \"%s\": %s", olddir, strerror(errno));
	/*NOTREACHED*/
    }

    if (!S_ISDIR(stat_old.st_mode)) {
	error("Oldlog directory \"%s\" is not a directory", olddir);
	/*NOTREACHED*/
    }

    if ((dir = opendir(conf_logdir)) == NULL) {
	error("could not open log directory \"%s\": %s", conf_logdir,strerror(errno));
	/*NOTREACHED*/
    }
    while ((adir=readdir(dir)) != NULL) {
	if(strncmp(adir->d_name,"log.",4)==0) {
	    useful=0;
	    for (name=output_find_log;*name !=NULL; name++) {
		if((strlen(adir->d_name) >= 13 &&
		    strlen(*name) >= 13 &&
		    adir->d_name[12] == '.' && (*name)[12] == '.' &&
		    strncmp(adir->d_name,*name,12)==0) ||
		   strncmp(adir->d_name,*name,18)==0) {
		    useful=1;
		    break;
		}
	    }
	    logname=newvstralloc(logname,
				 conf_logdir, "/" ,adir->d_name, NULL);
	    if(stat(logname,&stat_log)==0) {
		if((time_t)stat_log.st_mtime > date_keep) {
		    useful = 1;
		}
	    }
	    if(useful == 0) {
		oldfile = newvstralloc(oldfile,
				       conf_logdir, "/", adir->d_name, NULL);
		newfile = newvstralloc(newfile,
				       olddir, "/", adir->d_name, NULL);
		if (rename(oldfile,newfile) != 0) {
		    error("could not rename \"%s\" to \"%s\": %s",
			  oldfile, newfile, strerror(errno));
		    /*NOTREACHED*/
	    	}
	    }
	}
    }
    closedir(dir);
    for (name = output_find_log; *name != NULL; name++) {
	amfree(*name);
    }
    amfree(output_find_log);
    amfree(logname);
    amfree(oldfile);
    amfree(newfile);
    amfree(olddir);
    amfree(config_dir);
    amfree(conf_logdir);
    clear_tapelist();
    free_disklist(&diskl);
    free_new_argv(new_argc, new_argv);
    free_server_config();

    dbclose();

    return 0;
}
