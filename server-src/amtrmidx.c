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
 * $Id: amtrmidx.c,v 1.42 2006/07/25 18:27:57 martinea Exp $
 *
 * trims number of index files to only those still in system.  Well
 * actually, it keeps a few extra, plus goes back to the last level 0
 * dump.
 */

#include "amanda.h"
#include "arglist.h"
#include "conffile.h"
#include "diskfile.h"
#include "tapefile.h"
#include "find.h"
#include "version.h"
#include "util.h"

static int sort_by_name_reversed(const void *a, const void *b);

int main(int argc, char **argv);

static int sort_by_name_reversed(
    const void *a,
    const void *b)
{
    char **ap = (char **) a;
    char **bp = (char **) b;

    return -1 * strcmp(*ap, *bp);
}


int
main(
    int		argc,
    char **	argv)
{
    disk_t *diskp;
    disklist_t diskl;
    size_t i;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_indexdir;
    find_result_t *output_find;
    time_t tmp_time;
    int amtrmidx_debug = 0;
    config_overwrites_t *cfg_ovr = NULL;

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

    set_pname("amtrmidx");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);
    dbprintf(_("%s: version %s\n"), argv[0], version());

    cfg_ovr = extract_commandline_config_overwrites(&argc, &argv);

    if (argc > 1 && strcmp(argv[1], "-t") == 0) {
	amtrmidx_debug = 1;
	argc--;
	argv++;
    }

    if (argc < 2) {
	g_fprintf(stderr, _("Usage: %s [-t] <config> [-o configoption]*\n"), argv[0]);
	return 1;
    }

    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, argv[1]);
    apply_config_overwrites(cfg_ovr);

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
    if(read_tapelist(conf_tapelist)) {
	error(_("could not load tapelist \"%s\""), conf_tapelist);
	/*NOTREACHED*/
    }
    amfree(conf_tapelist);

    output_find = find_dump(&diskl);

    conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));

    /* now go through the list of disks and find which have indexes */
    time(&tmp_time);
    tmp_time -= 7*24*60*60;			/* back one week */
    for (diskp = diskl.head; diskp != NULL; diskp = diskp->next)
    {
	if (diskp->index)
	{
	    char *indexdir, *qindexdir;
	    DIR *d;
	    struct dirent *f;
	    char **names;
	    size_t name_length;
	    size_t name_count;
	    char *host;
	    char *disk, *qdisk;
	    size_t len_date;


	    /* get listing of indices, newest first */
	    host = sanitise_filename(diskp->host->hostname);
	    disk = sanitise_filename(diskp->name);
	    qdisk = quote_string(diskp->name);
	    indexdir = vstralloc(conf_indexdir, "/",
				 host, "/",
				 disk, "/",
				 NULL);
	    qindexdir = quote_string(indexdir);

	    dbprintf("%s %s -> %s\n", diskp->host->hostname,
			qdisk, qindexdir);
	    amfree(host);
	    amfree(qdisk);
	    amfree(disk);
	    if ((d = opendir(indexdir)) == NULL) {
		dbprintf(_("could not open index directory %s\n"), qindexdir);
		amfree(indexdir);
	        amfree(qindexdir);
		continue;
	    }
	    name_length = 100;
	    names = (char **)alloc(name_length * SIZEOF(char *));
	    name_count = 0;
	    while ((f = readdir(d)) != NULL) {
		size_t l;

		if(is_dot_or_dotdot(f->d_name)) {
		    continue;
		}
		for(i = 0; i < SIZEOF("YYYYMMDDHHMMSS")-1; i++) {
		    if(! isdigit((int)(f->d_name[i]))) {
			break;
		    }
		}
		len_date = i;
		/* len_date=8  for YYYYMMDD       */
		/* len_date=14 for YYYYMMDDHHMMSS */
		if((len_date != 8 && len_date != 14)
		    || f->d_name[len_date] != '_'
		    || ! isdigit((int)(f->d_name[len_date+1]))) {
		    continue;			/* not an index file */
		}
		/*
		 * Clear out old index temp files.
		 */
		l = strlen(f->d_name) - (SIZEOF(".tmp")-1);
		if ((l > (len_date + 1))
			&& (strcmp(f->d_name + l, ".tmp")==0)) {
		    struct stat sbuf;
		    char *path, *qpath;

		    path = stralloc2(indexdir, f->d_name);
		    qpath = quote_string(path);
		    if(lstat(path, &sbuf) != -1
			&& ((sbuf.st_mode & S_IFMT) == S_IFREG)
			&& ((time_t)sbuf.st_mtime < tmp_time)) {
			dbprintf("rm %s\n", qpath);
		        if(amtrmidx_debug == 0 && unlink(path) == -1) {
			    dbprintf(_("Error removing %s: %s\n"),
				      qpath, strerror(errno));
		        }
		    }
		    amfree(qpath);
		    amfree(path);
		    continue;
		}
		if(name_count >= name_length) {
		    char **new_names;

		    new_names = alloc((name_length * 2) * SIZEOF(char *));
		    memcpy(new_names, names, name_length * SIZEOF(char *));
		    amfree(names);
		    names = new_names;
		    name_length *= 2;
		}
		names[name_count++] = stralloc(f->d_name);
	    }
	    closedir(d);
	    qsort(names, name_count, SIZEOF(char *), sort_by_name_reversed);

	    /*
	     * Search for the first full dump past the minimum number
	     * of index files to keep.
	     */
	    for(i = 0; i < name_count; i++) {
		char *datestamp;
		int level;
		size_t len_date;

		for(len_date = 0; len_date < SIZEOF("YYYYMMDDHHMMSS")-1; len_date++) {
                    if(! isdigit((int)(names[i][len_date]))) {
                        break;
                    }
                }

		datestamp = stralloc(names[i]);
		datestamp[len_date] = '\0';
		if (sscanf(&names[i][len_date+1], "%d", &level) != 1)
		    level = 0;
		if(!dump_exist(output_find, diskp->host->hostname,
				diskp->name, datestamp, level)) {
		    char *path, *qpath;
		    path = stralloc2(indexdir, names[i]);
		    qpath = quote_string(path);
		    dbprintf("rm %s\n", qpath);
		    if(amtrmidx_debug == 0 && unlink(path) == -1) {
			dbprintf(_("Error removing %s: %s\n"),
				  qpath, strerror(errno));
		    }
		    amfree(qpath);
		    amfree(path);
		}
		amfree(datestamp);
		amfree(names[i]);
	    }
	    amfree(names);
	    amfree(indexdir);
	    amfree(qindexdir);
	}
    }

    amfree(conf_indexdir);
    free_find_result(&output_find);
    clear_tapelist();
    free_disklist(&diskl);

    dbclose();

    return 0;
}
