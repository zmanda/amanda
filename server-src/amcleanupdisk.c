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
 * $Id: amcleanupdisk.c,v 1.22 2006/07/25 18:27:57 martinea Exp $
 */
#include "amanda.h"

#include "conffile.h"
#include "diskfile.h"
#include "clock.h"
#include "version.h"
#include "holding.h"
#include "infofile.h"
#include "server_util.h"

sl_t *holding_list;
char *datestamp;

/* local functions */
int main(int argc, char **argv);
void check_holdingdisk(char *diskdir, char *datestamp);
void check_disks(void);

int
main(
    int		main_argc,
    char **	main_argv)
{
    struct passwd *pw;
    char *dumpuser;
    disklist_t diskq;
    char *conffile;
    char *conf_diskfile;
    char *conf_infofile;

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

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    if(main_argc < 2) {
	error(_("Usage: amcleanupdisk%s <config>"), versionsuffix());
	/*NOTREACHED*/
    }

    config_name = main_argv[1];

    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);

    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if(read_conffile(conffile)) {
	error(_("errors processing config file \"%s\""), conffile);
	/*NOTREACHED*/
    }
    amfree(conffile);

    check_running_as(RUNNING_AS_DUMPUSER);

    dbrename(config_name, DBG_SUBDIR_SERVER);

    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if (read_diskfile(conf_diskfile, &diskq) < 0) {
	error(_("could not load disklist %s"), conf_diskfile);
	/*NOTREACHED*/
    }
    amfree(conf_diskfile);

    conf_infofile = getconf_str(CNF_INFOFILE);
    if (*conf_infofile == '/') {
	conf_infofile = stralloc(conf_infofile);
    } else {
	conf_infofile = stralloc2(config_dir, conf_infofile);
    }
    if (open_infofile(conf_infofile) < 0) {
	error(_("could not open info db \"%s\""), conf_infofile);
	/*NOTREACHED*/
    }
    amfree(conf_infofile);

    datestamp = construct_datestamp(NULL);

    holding_list = holding_get_all_datestamps();

    check_disks();

    close_infofile();

    free_sl(holding_list);
    holding_list = NULL;
    amfree(config_dir);
    return 0;
}


void
check_holdingdisk(
    char *	diskdir,
    char *	datestamp)
{
    DIR *workdir;
    struct dirent *entry;
    char *dirname = NULL;
    char *tmpname = NULL;
    char *destname = NULL;
    disk_t *dp;
    info_t info;
    size_t dl, l;
    dumpfile_t file;
    int stat;

    dirname = vstralloc(diskdir, "/", datestamp, NULL);
    dl = strlen(dirname);

    if((workdir = opendir(dirname)) == NULL) {
	amfree(dirname);
	return;
    }

    while((entry = readdir(workdir)) != NULL) {
	if(is_dot_or_dotdot(entry->d_name)) {
	    continue;
	}

	if((l = strlen(entry->d_name)) < 7 ) {
	    continue;
	}

	if(strncmp(&entry->d_name[l-4],".tmp",4) != 0) {
	    continue;
	}

	tmpname = newvstralloc(tmpname,
			       dirname, "/", entry->d_name,
			       NULL);

	destname = newstralloc(destname, tmpname);
	destname[dl + 1 + l - 4] = '\0';

	stat = holding_file_get_dumpfile(tmpname, &file);
	amfree(tmpname);

	if (!stat)
	    continue;

	if(file.type != F_DUMPFILE)
	    continue;

	dp = lookup_disk(file.name, file.disk);

	if (dp == NULL) {
	    continue;
	}

	if(file.dumplevel < 0 || file.dumplevel > 9) {
	    continue;
	}

	if(rename_tmp_holding(destname, 0)) {
	    get_info(dp->host->hostname, dp->name, &info);
	    info.command &= ~FORCE_BUMP;
	    info.command |= FORCE_NO_BUMP;
	    if(put_info(dp->host->hostname, dp->name, &info)) {
		error(_("could not put info record for %s:%s: %s"),
		      dp->host->hostname, dp->name, strerror(errno));
	        /*NOTREACHED*/
	    }
	} else {
	    fprintf(stderr,_("rename_tmp_holding(%s) failed\n"), destname);
	}
    }
    closedir(workdir);

    /* try to zap the potentially empty working dir */
    /* ignore any errors -- it either works or it doesn't */
    (void) rmdir(dirname);

    amfree(destname);
    amfree(dirname);
}


void
check_disks(void)
{
    holdingdisk_t *hdisk;
    sle_t *dir;

    /* if there are no holding files, we're done */
    if (!holding_list) return;

    for(dir = holding_list->first; dir !=NULL; dir = dir->next) {
	for(hdisk = getconf_holdingdisks(); hdisk != NULL; hdisk = hdisk->next)
	    check_holdingdisk(holdingdisk_get_diskdir(hdisk), dir->name);
    }
}

