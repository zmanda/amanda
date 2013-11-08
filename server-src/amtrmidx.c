/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * $Id: amtrmidx.c,v 1.42 2006/07/25 18:27:57 martinea Exp $
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
#include "amutil.h"
#include "amindex.h"
#include "pipespawn.h"

static int sort_by_name_reversed(const void *a, const void *b);
static gboolean file_exists(char *filename);
static gboolean run_compress(char *source_filename, char *dest_filename);
static gboolean run_uncompress(char *source_filename, char *dest_filename);
static gboolean run_sort(char *source_filename, char *dest_filename);


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
    GList  *dlist;
    GList  *dlist1;
    disk_t *diskp;
    disklist_t diskl;
    size_t i;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_indexdir;
    find_result_t *output_find;
    time_t tmp_time;
    int amtrmidx_debug = 0;
    config_overrides_t *cfg_ovr = NULL;
    gboolean   compress_index;
    gboolean   sort_index;
    char      *lock_file;
    file_lock *lock_index;

    if (argc > 1 && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("amtrmidx-%s\n", VERSION);
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

    set_pname("amtrmidx");

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

    dbopen(DBG_SUBDIR_SERVER);
    dbprintf(_("%s: version %s\n"), argv[0], VERSION);

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

    set_config_overrides(cfg_ovr);
    config_init(CONFIG_INIT_EXPLICIT_NAME | CONFIG_INIT_USE_CWD, argv[1]);

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

    compress_index = getconf_boolean(CNF_COMPRESS_INDEX);
    sort_index = getconf_boolean(CNF_SORT_INDEX);

    output_find = find_dump(&diskl);

    conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));

    /* take a lock file to prevent concurent trim */
    lock_file = g_strdup_printf("%s/%s", conf_indexdir, "lock");
    lock_index = file_lock_new(lock_file);
    if (file_lock_lock_wr(lock_index) != 0)
	goto lock_failed;

    /* now go through the list of disks and find which have indexes */
    time(&tmp_time);
    tmp_time -= 7*24*60*60;			/* back one week */
    for (dlist = diskl.head; dlist != NULL; dlist = dlist->next)
    {
	diskp = dlist->data;
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
	    disk_t *dp;
	    GSList *matching_dp = NULL;

	    /* get listing of indices, newest first */
	    host = sanitise_filename(diskp->host->hostname);
	    disk = sanitise_filename(diskp->name);
	    qdisk = quote_string(diskp->name);
	    indexdir = g_strjoin(NULL, conf_indexdir, "/",
				 host, "/",
				 disk, "/",
				 NULL);
	    qindexdir = quote_string(indexdir);

	    /* find all dles that use the same indexdir */
	    for (dlist1 = diskl.head; dlist1 != NULL; dlist1 = dlist1->next)
	    {
		char *dp_host, *dp_disk;

		dp = dlist1->data;
		dp_host = sanitise_filename(dp->host->hostname);
		dp_disk = sanitise_filename(dp->name);
		if (g_str_equal(host, dp_host) &&
		    g_str_equal(disk, dp_disk)) {
		    matching_dp = g_slist_append(matching_dp, dp);
		}
		amfree(dp_host);
		amfree(dp_disk);
	    }

	    dbprintf("%s %s -> %s\n", diskp->host->hostname,
			qdisk, qindexdir);
	    amfree(qdisk);
	    if ((d = opendir(indexdir)) == NULL) {
		dbprintf(_("could not open index directory %s\n"), qindexdir);
		amfree(host);
		amfree(disk);
		amfree(indexdir);
	        amfree(qindexdir);
		g_slist_free(matching_dp);
		continue;
	    }
	    name_length = 100;
	    names = (char **)g_malloc(name_length * sizeof(char *));
	    name_count = 0;
	    while ((f = readdir(d)) != NULL) {
		size_t l;

		if(is_dot_or_dotdot(f->d_name)) {
		    continue;
		}
		for(i = 0; i < sizeof("YYYYMMDDHHMMSS")-1; i++) {
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
		l = strlen(f->d_name) - (sizeof(".tmp")-1);
		if ((l > (len_date + 1))
			&& (g_str_equal(f->d_name + l, ".tmp"))) {
		    struct stat sbuf;
		    char *path, *qpath;

		    path = g_strconcat(indexdir, f->d_name, NULL);
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

		    new_names = g_malloc((name_length * 2) * sizeof(char *));
		    memcpy(new_names, names, name_length * sizeof(char *));
		    amfree(names);
		    names = new_names;
		    name_length *= 2;
		}
		names[name_count++] = g_strdup(f->d_name);
	    }
	    closedir(d);
	    qsort(names, name_count, sizeof(char *), sort_by_name_reversed);

	    /*
	     * Search for the first full dump past the minimum number
	     * of index files to keep.
	     */
	    for(i = 0; i < name_count; i++) {
		char *datestamp;
		int level;
		size_t len_date;
		int matching = 0;
		GSList *mdp;

		for(len_date = 0; len_date < sizeof("YYYYMMDDHHMMSS")-1; len_date++) {
                    if(! isdigit((int)(names[i][len_date]))) {
                        break;
                    }
                }

		datestamp = g_strdup(names[i]);
		datestamp[len_date] = '\0';
		if (sscanf(&names[i][len_date+1], "%d", &level) != 1)
		    level = 0;
		for (mdp = matching_dp; mdp != NULL; mdp = mdp->next) {
		    dp = mdp->data;
		    if (dump_exist(output_find, dp->host->hostname,
				   dp->name, datestamp, level)) {
			matching = 1;
		    }
		}
		if (!matching) {
		    char *path, *qpath;
		    path = g_strconcat(indexdir, names[i], NULL);
		    qpath = quote_string(path);
		    dbprintf("rm %s\n", qpath);
		    if(amtrmidx_debug == 0 && unlink(path) == -1) {
			dbprintf(_("Error removing %s: %s\n"),
				  qpath, strerror(errno));
		    }
		    amfree(qpath);
		    amfree(path);
		}

		/* Did it require un/compression and/or sorting */
		{
		char *orig_name = getindexfname(host, disk, datestamp, level);
		char *sorted_name = getindex_sorted_fname(host, disk, datestamp, level);
		char *sorted_gz_name = getindex_sorted_gz_fname(host, disk, datestamp, level);
		char *unsorted_name = getindex_unsorted_fname(host, disk, datestamp, level);
		char *unsorted_gz_name = getindex_unsorted_gz_fname(host, disk, datestamp, level);
		char *path = g_strconcat(indexdir, names[i], NULL);

		//struct stat sorted_stat;
		//struct stat sorted_gz_stat;
		//struct stat unsorted_stat;
		//struct stat unsorted_gz_stat;

		gboolean orig_exist = FALSE;
		gboolean sorted_exist = FALSE;
		gboolean sorted_gz_exist = FALSE;
		gboolean unsorted_exist = FALSE;
		gboolean unsorted_gz_exist = FALSE;

		orig_exist = file_exists(orig_name);
		sorted_exist = file_exists(sorted_name);
		sorted_gz_exist = file_exists(sorted_gz_name);
		unsorted_exist = file_exists(unsorted_name);
		unsorted_gz_exist = file_exists(unsorted_gz_name);

		if (sort_index && compress_index) {
		    if (!sorted_gz_exist) {
			if (sorted_exist) {
			    // COMPRESS
			    run_compress(sorted_name, sorted_gz_name);
			    unlink(sorted_name);
			} else if (unsorted_exist) {
			    // SORT AND COMPRESS
			    run_sort(unsorted_name, sorted_name);
			    run_compress(sorted_name, sorted_gz_name);
			    unlink(unsorted_name);
			    unlink(sorted_name);
			} else if (unsorted_gz_exist) {
			    // UNCOMPRESS SORT AND COMPRESS
			    run_uncompress(unsorted_gz_name, unsorted_name);
			    run_sort(unsorted_name, sorted_name);
			    run_compress(sorted_name, sorted_gz_name);
			    unlink(unsorted_gz_name);
			    unlink(unsorted_name);
			    unlink(sorted_name);
			} else if (orig_exist) {
			    // UNCOMPRESS SORT AND COMPRESS
			    run_uncompress(orig_name, unsorted_name);
			    run_sort(unsorted_name, sorted_name);
			    run_compress(sorted_name, sorted_gz_name);
			    unlink(orig_name);
			    unlink(unsorted_name);
			    unlink(sorted_name);
			}
		    }
		    if (strcmp(path, sorted_gz_name) != 0) {
			//unlink(path);
		    }
		} else if (sort_index && !compress_index) {
		    if (!sorted_exist) {
			if (sorted_gz_exist) {
			    // UNCOMPRESS
			    run_uncompress(sorted_gz_name, sorted_name);
			    unlink(sorted_gz_name);
			} else if (unsorted_exist) {
			    // SORT
			    run_sort(unsorted_name, sorted_name);
			    unlink(unsorted_name);
			} else if (unsorted_gz_exist) {
			    // UNCOMPRESS AND SORT
			    run_uncompress(unsorted_gz_name, unsorted_name);
			    run_sort(unsorted_name, sorted_name);
			    unlink(unsorted_name);
			    unlink(unsorted_gz_name);
			} else if (orig_exist) {
			    // UNCOMPRESS AND SORT
			    run_uncompress(orig_name, unsorted_name);
			    run_sort(unsorted_name, sorted_name);
			    unlink(unsorted_name);
			    unlink(orig_name);
			}
		    }
		    if (strcmp(path, sorted_name) != 0) {
			//unlink(path);
		    }
		} else if (!sort_index && compress_index) {
		    if (!sorted_gz_exist && !unsorted_gz_exist) {
			if (sorted_exist) {
			    // COMPRESS sorted
			    run_compress(sorted_name, sorted_gz_name);
			    unlink(sorted_name);
			} else if (unsorted_exist) {
			    // COMPRESS unsorted
			    run_compress(unsorted_name, unsorted_gz_name);
			    unlink(unsorted_name);
			} else if (orig_exist) {
			    // RENAME orig
			    rename(orig_name, unsorted_gz_name);
			}
		    }
		    if (strcmp(path, sorted_gz_name) != 0 &&
			strcmp(path, unsorted_gz_name) != 0) {
			//unlink(path);
		    }
		} else if (!sort_index && !compress_index) {
		    if (!sorted_exist && !unsorted_exist) {
			if (sorted_gz_exist) {
			    // UNCOMPRESS sorted
			    run_uncompress(sorted_gz_name, sorted_name);
			    unlink(sorted_gz_name);
			} else if (unsorted_gz_exist) {
			    // UNCOMPRESS unsorted
			    run_uncompress(unsorted_gz_name, unsorted_name);
			    unlink(unsorted_gz_name);
			} else if (orig_exist) {
			    // UNCOMPRESS orig
			    run_uncompress(orig_name, unsorted_name);
			    unlink(orig_name);
			}
		    }
		    if (strcmp(path, sorted_gz_name) != 0) {
			//unlink(path);
		    }
		}
		}

		amfree(datestamp);
		amfree(names[i]);
	    }
	    g_slist_free(matching_dp);
	    amfree(names);
	    amfree(host);
	    amfree(disk);
	    amfree(indexdir);
	    amfree(qindexdir);
	}
    }

    file_lock_unlock(lock_index);
lock_failed:
    file_lock_free(lock_index);
    amfree(conf_indexdir);
    amfree(lock_file);
    free_find_result(&output_find);
    clear_tapelist();
    free_disklist(&diskl);
    unload_disklist();

    dbclose();

    return 0;
}

static gboolean
file_exists(
    char *filename)
{
    struct stat stat_buf;

    if (stat(filename, &stat_buf) != 0) {
	if (errno == ENOENT) {
	    return FALSE;
	}
    }
    return TRUE;
}

static gboolean
run_compress(
    char *source_filename,
    char *dest_filename)
{
    int in_fd = open(source_filename, O_RDONLY);
    int out_fd = open(dest_filename, O_WRONLY|O_CREAT, S_IRUSR);
    int compress_errfd = 0;
    gboolean rval = TRUE;
    amwait_t  wait_status;
    pid_t pid_gzip;
    char *line;

    pid_gzip = pipespawn(COMPRESS_PATH, STDERR_PIPE, 0,
			 &in_fd, &out_fd, &compress_errfd,
			 COMPRESS_PATH, COMPRESS_BEST_OPT, NULL);
    close(in_fd);
    close(out_fd);
    while ((line = areads(compress_errfd)) != NULL) {
	g_debug("compress stderr: %s", line);
	rval = FALSE;
	free(line);
    }
    close(compress_errfd);
    waitpid(pid_gzip, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	g_debug("compress terminated with signal %d", WTERMSIG(wait_status));
	rval = FALSE;
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    g_debug("compress exited with status %d", WEXITSTATUS(wait_status));
	    rval = FALSE;
	}
    } else {
	g_debug("compress got bad exit");
	rval = FALSE;
    }

    return rval;
}

static gboolean
run_uncompress(
    char *source_filename,
    char *dest_filename)
{
    int in_fd = open(source_filename, O_RDONLY);
    int out_fd = open(dest_filename, O_WRONLY|O_CREAT, S_IRUSR);
    int uncompress_errfd = 0;
    gboolean rval = TRUE;
    amwait_t  wait_status;
    pid_t pid_gzip;
    char *line;

    pid_gzip = pipespawn(UNCOMPRESS_PATH, STDERR_PIPE, 0,
			 &in_fd, &out_fd, &uncompress_errfd,
			 UNCOMPRESS_PATH, UNCOMPRESS_OPT, NULL);
    close(in_fd);
    close(out_fd);
    while ((line = areads(uncompress_errfd)) != NULL) {
	g_debug("uncompress stderr: %s", line);
	rval = FALSE;
	free(line);
    }
    close(uncompress_errfd);
    waitpid(pid_gzip, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	g_debug("uncompress terminated with signal %d", WTERMSIG(wait_status));
	rval = FALSE;
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    g_debug("uncompress exited with status %d", WEXITSTATUS(wait_status));
	    rval = FALSE;
	}
    } else {
	g_debug("uncompress got bad exit");
	rval = FALSE;
    }

    return rval;
}

static gboolean
run_sort(
    char *source_filename,
    char *dest_filename)
{
    int in_fd;
    int out_fd;
    int sort_errfd = 0;
    gboolean rval = TRUE;
    amwait_t  wait_status;
    pid_t pid_sort;
    char *line;
    gchar *tmpdir = getconf_str(CNF_TMPDIR);

    pid_sort = pipespawn(SORT_PATH, STDIN_PIPE|STDOUT_PIPE|STDERR_PIPE, 0,
			 &in_fd, &out_fd, &sort_errfd,
			 SORT_PATH, "--output", dest_filename,
				    "-T", tmpdir, source_filename, NULL);
    close(in_fd);
    while ((line = areads(out_fd)) != NULL) {
	g_debug("sort stdout: %s", line);
	rval = FALSE;
	free(line);
    }
    close(out_fd);
    while ((line = areads(sort_errfd)) != NULL) {
	g_debug("sort stderr: %s", line);
	rval = FALSE;
	free(line);
    }
    close(sort_errfd);
    waitpid(pid_sort, &wait_status, 0);
    if (WIFSIGNALED(wait_status)) {
	g_debug("sort terminated with signal %d", WTERMSIG(wait_status));
	rval = FALSE;
    } else if (WIFEXITED(wait_status)) {
	if (WEXITSTATUS(wait_status) != 0) {
	    g_debug("sort exited with status %d", WEXITSTATUS(wait_status));
	    rval = FALSE;
	}
    } else {
	g_debug("sort got bad exit");
	rval = FALSE;
    }

    return rval;
}
