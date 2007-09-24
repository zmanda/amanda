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
 * $Id: holding.c,v 1.56 2006/06/09 23:07:26 martinea Exp $
 *
 * Functions to access holding disk
 */

#include "amanda.h"
#include "util.h"
#include "holding.h"
#include "fileheader.h"
#include "logfile.h"

/*
 * utilities */

/* Is fname a directory?
 *
 * @param fname: filename (fully qualified)
 * @returns: boolean
 */
static int is_dir(char *fname);

/* Is fname an empty file?
 *
 * @param fname: filename (fully qualified)
 * @returns: boolean
 */
static int is_emptyfile(char *fname);

/* sanity check that datestamp is of the form YYYYMMDD or 
 * YYYYMMDDhhmmss
 *
 * @param fname: a filename (without directory)
 * @returns: boolean
 */
static int is_datestr(char *fname);

/* Get a list of holding directories, optionally limited to a single
 * holding disk.  Can return a list either of full pathnames or of
 * bare directory names (datestamps).
 *
 * @param hdisk: holding disk to enumerate, or NULL for all
 * @param fullpaths: if true, return full pathnames
 * @returns: newly allocated GSList of matching directories
 */
static GSList * holding_get_directories(
	    char *hdisk,
	    int fullpaths);

/*
 * Static functions */

static int
is_dir(
    char *fname)
{
    struct stat statbuf;

    if(stat(fname, &statbuf) == -1) return 0;

    return (statbuf.st_mode & S_IFDIR) == S_IFDIR;
}

static int
is_emptyfile(
    char *fname)
{
    struct stat statbuf;

    if(stat(fname, &statbuf) == -1) return 0;

    return ((statbuf.st_mode & S_IFDIR) != S_IFDIR) &&
		(statbuf.st_size == (off_t)0);
}

static int
is_datestr(
    char *fname)
{
    char *cp;
    int ch, num, date, year, month, hour, minute, second;
    char ymd[9], hms[7];

    /* must be 8 digits */
    for(cp = fname; (ch = *cp) != '\0'; cp++) {
	if(!isdigit(ch)) {
	    break;
	}
    }
    if(ch != '\0' || (cp-fname != 8 && cp-fname != 14)) {
	return 0;
    }

    /* sanity check year, month, and day */

    strncpy(ymd, fname, 8);
    ymd[8] = '\0';
    num = atoi(ymd);
    year = num / 10000;
    month = (num / 100) % 100;
    date = num % 100;
    if(year<1990 || year>2100 || month<1 || month>12 || date<1 || date>31)
	return 0;

    if(cp-fname == 8)
	return 1;

    /* sanity check hour, minute, and second */
    strncpy(hms, fname+8, 6);
    hms[6] = '\0';
    num = atoi(hms);
    hour = num / 10000;
    minute = (num / 100) % 100;
    second = num % 100;
    if(hour> 23 || minute>59 || second>59)
	return 0;

    /* yes, we passed all the checks */

    return 1;
}

static GSList *
holding_get_directories_per_disk(
    char *hdisk,
    int fullpaths,
    GSList *rv)
{
    DIR *dir;
    struct dirent *workdir;
    char *hdir = NULL;

    if ((dir = opendir(hdisk)) == NULL) {
        if (errno != ENOENT)
           dbprintf(_("Warning: could not open holding disk %s: %s\n"),
                  hdisk, strerror(errno));
        return rv;
    }

    while ((workdir = readdir(dir)) != NULL) {
        if (is_dot_or_dotdot(workdir->d_name))
            continue;

        hdir = newvstralloc(hdir,
                     hdisk, "/", workdir->d_name,
                     NULL);

        /* filter out various undesirables */
        if (!is_dir(hdir)) {
	    dbprintf(_("skipping cruft file '%s', perhaps you should delete it."), hdir);
        } else if (!is_datestr(workdir->d_name)) {
            /* EXT2/3 leave these in the root of each volume */
            if (strcmp(workdir->d_name, "lost+found")==0)
                dbprintf(_("skipping system directory '%s'"), hdir);
	    dbprintf(_("skipping cruft directory '%s', perhaps you should delete it."), hdir);
        } else {
            /* found a holding directory -- keep it */
            if (fullpaths)
                rv = g_slist_insert_sorted(rv, stralloc(hdir), g_compare_strings);
            else
                rv = g_slist_insert_sorted(rv, stralloc(workdir->d_name), g_compare_strings);
        }
    }

    if (hdir)
	amfree(hdir);

    return rv;
}


/* Get a list of holding directories, optionally limited to a single
 * holding disk.  Can return a list either of full pathnames or of
 * bare directory names (datestamps).
 *
 * @param hdisk: holding disk to enumerate, or NULL for all
 * @param fullpaths: if true, return full pathnames
 * @returns: newly allocated GSList of matching directories
 */
static GSList *
holding_get_directories(
    char *hdisk,
    int fullpaths)
{
    holdingdisk_t *hdisk_conf;
    GSList *rv = NULL;

    /* call _per_disk for the hdisk we were given, or for all
     * hdisks if we were given NULL */
    if (hdisk) {
        rv = holding_get_directories_per_disk(hdisk, fullpaths, rv);
    } else {
        for (hdisk_conf = getconf_holdingdisks(); 
                    hdisk_conf != NULL;
                    hdisk_conf = hdisk_conf->next) {
            hdisk = holdingdisk_get_diskdir(hdisk_conf);
            rv = holding_get_directories_per_disk(hdisk, fullpaths, rv);
        }
    }

    return rv;
}

/*
 * Holding files
 */
static GSList *
holding_get_files_per_dir(
    char *hdir,
    int fullpaths,
    GSList *rv)
{
    DIR *dir;
    struct dirent *workdir;
    char *hfile = NULL;
    dumpfile_t dumpf;
    int dumpf_ok;

    if ((dir = opendir(hdir)) == NULL) {
        if (errno != ENOENT)
           dbprintf(_("Warning: could not open holding dir %s: %s\n"),
                  hdir, strerror(errno));
        return rv;
    }

    while ((workdir = readdir(dir)) != NULL) {
        if (is_dot_or_dotdot(workdir->d_name))
            continue;

        hfile = newvstralloc(hfile,
                     hdir, "/", workdir->d_name,
                     NULL);

        /* filter out various undesirables */
        if (is_emptyfile(hfile))
            continue;

        if (is_dir(hfile)) {
	    dbprintf(_("%s: ignoring directory\n"), hfile);
            continue;
        }

        if (!(dumpf_ok=holding_file_get_dumpfile(hfile, &dumpf)) ||
            dumpf.type != F_DUMPFILE) {
            if (dumpf_ok && dumpf.type == F_CONT_DUMPFILE)
                continue; /* silently skip expected file */
	    dbprintf(_("%s: not a dumpfile\n"), hfile);
            continue;
        }

	if (dumpf.dumplevel < 0 || dumpf.dumplevel > 9) {
	    dbprintf(_("%s: ignoring file with bogus dump level %d.\n"),
                       hfile, dumpf.dumplevel);
	    continue;
	}

        /* found a holding file -- keep it */
        if (fullpaths)
            rv = g_slist_insert_sorted(rv, stralloc(hfile), g_compare_strings);
        else
            rv = g_slist_insert_sorted(rv, stralloc(workdir->d_name), g_compare_strings);
    }

    if (hfile) amfree(hfile);

    return rv;
}

GSList *
holding_get_files(
    char *hdir,
    int fullpaths)
{
    GSList *hdirs, *e;
    GSList *rv = NULL;

    /* call _per_dir for the hdir we were given, or for all
     * hdir if we were given NULL */
    if (hdir) {
        rv = holding_get_files_per_dir(hdir, fullpaths, rv);
    } else {
        hdirs = holding_get_directories(NULL, 1);
        for (e = hdirs; e != NULL; e = e->next) {
	    rv = holding_get_files_per_dir((char *)e->data, fullpaths, rv);
        }
	g_slist_free_full(hdirs);
    }

    return rv;
}

GSList *
holding_get_files_for_flush(
    GSList *dateargs)
{
    GSList *file_list, *file_elt;
    GSList *date;
    int date_matches;
    disk_t *dp;
    dumpfile_t file;
    GSList *result_list = NULL;

    /* loop over *all* files, checking each one's datestamp against the expressions
     * in dateargs */
    file_list = holding_get_files(NULL, 1);
    for (file_elt = file_list; file_elt != NULL; file_elt = file_elt->next) {
        /* get info on that file */
	if (!holding_file_get_dumpfile((char *)file_elt->data, &file))
	    continue;

        if (file.type != F_DUMPFILE)
            continue;

	if (dateargs) {
	    date_matches = 0;
	    /* loop over date args, until we find a match */
	    for (date = dateargs; date !=NULL; date = date->next) {
		if (strcmp((char *)date->data, file.datestamp) == 0) {
		    date_matches = 1;
		    break;
		}
	    }
	} else {
	    /* if no date list was provided, then all dates match */
	    date_matches = 1;
	}
        if (!date_matches)
            continue;

        /* check that the hostname and disk are in the disklist */
        dp = lookup_disk(file.name, file.disk);
        if (dp == NULL) {
	    dbprintf(_("%s: disk %s:%s not in database, skipping it."),
                        (char *)file_elt->data, file.name, file.disk);
            continue;
        }

        /* passed all tests -- we'll flush this file */
        result_list = g_slist_insert_sorted(result_list, 
	    stralloc(file_elt->data), 
	    g_compare_strings);
    }

    if (file_list) g_slist_free_full(file_list);

    return result_list;
}

GSList *
holding_get_file_chunks(char *hfile)
{
    dumpfile_t file;
    char *filename;
    GSList *rv = NULL;

    /* Loop through all cont_filenames (subsequent chunks) */
    filename = stralloc(hfile);
    while (filename != NULL && filename[0] != '\0') {
        /* get the header to look for cont_filename */
        if (!holding_file_get_dumpfile(filename, &file)) {
	    dbprintf(_("holding_get_file_chunks: open of %s failed.\n"), filename);
            amfree(filename);
            return rv;
        }

        /* add the file to the results (steals the reference in 'filename') */
        rv = g_slist_insert_sorted(rv, filename, g_compare_strings);

        /* and go on to the next chunk */
	filename = stralloc(file.cont_filename);
    }
    amfree(filename);

    return rv;
}

GSList *
holding_get_all_datestamps(void)
{
    GSList *all_files, *file;
    GSList *datestamps = NULL;

    /* enumerate all files */
    all_files = holding_get_files(NULL, 1);
    for (file = all_files; file != NULL; file = file->next) {
	dumpfile_t dfile;
	if (!holding_file_get_dumpfile((char *)file->data, &dfile))
	    continue;
	datestamps = g_slist_insert_sorted(datestamps, 
	    stralloc(dfile.datestamp), 
	    g_compare_strings);
    }

    g_slist_free_full(all_files);

    return datestamps;
}

off_t
holding_file_size(
    char *hfile,
    int strip_headers)
{
    dumpfile_t file;
    char *filename;
    off_t size = (off_t)0;
    struct stat finfo;

    /* (note: we don't use holding_get_file_chunks here because that would
     * entail opening each file twice) */

    /* Loop through all cont_filenames (subsequent chunks) */
    filename = stralloc(hfile);
    while (filename != NULL && filename[0] != '\0') {
        /* stat the file for its size */
        if (stat(filename, &finfo) == -1) {
	    dbprintf(_("stat %s: %s\n"), filename, strerror(errno));
            return (off_t)-1;
        }
        size += (finfo.st_size+(off_t)1023)/(off_t)1024;
        if (strip_headers)
            size -= (off_t)(DISK_BLOCK_BYTES / 1024);

        /* get the header to look for cont_filename */
        if (!holding_file_get_dumpfile(filename, &file)) {
	    dbprintf(_("holding_file_size: open of %s failed.\n"), filename);
            amfree(filename);
            return (off_t)-1;
        }

        /* on to the next chunk */
        filename = newstralloc(filename, file.cont_filename);
    }
    amfree(filename);
    return size;
}


int
holding_file_unlink(
    char *hfile)
{
    GSList *chunklist;
    GSList *chunk;

    chunklist = holding_get_file_chunks(hfile);
    if (!chunklist)
        return 0;

    for (chunk = chunklist; chunk != NULL; chunk = chunk->next) {
        if (unlink((char *)chunk->data)<0) {
	    dbprintf(_("holding_file_unlink: could not unlink %s: %s\n"),
                    (char *)chunk->data, strerror(errno));
            return 0;
        }
    }
    return 1;
}

int
holding_file_get_dumpfile(
    char *	fname,
    dumpfile_t *file)
{
    char buffer[DISK_BLOCK_BYTES];
    int fd;

    memset(buffer, 0, sizeof(buffer));

    fh_init(file);
    file->type = F_UNKNOWN;
    if((fd = robust_open(fname, O_RDONLY, 0)) == -1)
        return 0;

    if(fullread(fd, buffer, SIZEOF(buffer)) != (ssize_t)sizeof(buffer)) {
        aclose(fd);
        return 0;
    }
    aclose(fd);

    parse_file_header(buffer, file, SIZEOF(buffer));
    return 1;
}

/*
 * Application support
 */

int
rename_tmp_holding(
    char *	holding_file,
    int		complete)
{
    int fd;
    ssize_t buflen;
    char buffer[DISK_BLOCK_BYTES];
    dumpfile_t file;
    char *filename;
    char *filename_tmp = NULL;

    memset(buffer, 0, sizeof(buffer));
    filename = stralloc(holding_file);
    while(filename != NULL && filename[0] != '\0') {
	filename_tmp = newvstralloc(filename_tmp, filename, ".tmp", NULL);
	if((fd = robust_open(filename_tmp,O_RDONLY, 0)) == -1) {
	    dbprintf(_("rename_tmp_holding: open of %s failed: %s\n"),filename_tmp,strerror(errno));
	    amfree(filename);
	    amfree(filename_tmp);
	    return 0;
	}
	buflen = fullread(fd, buffer, SIZEOF(buffer));
	close(fd);

	if(rename(filename_tmp, filename) != 0) {
	    dbprintf(_("rename_tmp_holding: could not rename \"%s\" to \"%s\": %s"),
		    filename_tmp, filename, strerror(errno));
	}

	if (buflen <= 0) {
	    dbprintf(_("rename_tmp_holding: %s: empty file?\n"), filename);
	    amfree(filename);
	    amfree(filename_tmp);
	    return 0;
	}
	parse_file_header(buffer, &file, (size_t)buflen);
	if(complete == 0 ) {
            char * header;
	    if((fd = robust_open(filename, O_RDWR, 0)) == -1) {
		dbprintf(_("rename_tmp_holdingX: open of %s failed: %s\n"),
			filename, strerror(errno));
		amfree(filename);
		amfree(filename_tmp);
		return 0;

	    }
	    file.is_partial = 1;
            header = build_header(&file, DISK_BLOCK_BYTES);
	    fullwrite(fd, header, DISK_BLOCK_BYTES);
	    close(fd);
	}
	filename = newstralloc(filename, file.cont_filename);
    }
    amfree(filename);
    amfree(filename_tmp);
    return 1;
}

void
cleanup_holdingdisk(
    char *	diskdir)
{
    DIR *topdir;
    struct dirent *workdir;

    if((topdir = opendir(diskdir)) == NULL) {
	if(errno != ENOENT)
	    dbprintf(_("Warning: could not open holding dir %s: %s\n"),
		   diskdir, strerror(errno));
	return;
   }

    /* find all directories of the right format  */

    if ((chdir(diskdir)) == -1) {
	log_add(L_INFO, _("%s: could not chdir: %s"),
		    diskdir, strerror(errno));
    }
    while((workdir = readdir(topdir)) != NULL) {
	if(strcmp(workdir->d_name, ".") == 0
	   || strcmp(workdir->d_name, "..") == 0
	   || strcmp(workdir->d_name, "lost+found") == 0)
	    continue;

	if(!is_dir(workdir->d_name)) {
	    dbprintf(_("skipping cruft file '%s/%s', perhaps you should delete it."), 
		    diskdir, workdir->d_name);
	}
	else if(!is_datestr(workdir->d_name)) {
	    if(strcmp(workdir->d_name, "lost+found")!=0)
	        dbprintf(_("skipping cruft directory '%s/%s', perhaps you should delete it."),
		    diskdir, workdir->d_name);
	}
	else if(rmdir(workdir->d_name) == 0) {
	    dbprintf(_("deleted empty Amanda directory."));
	}
     }
     closedir(topdir);
}


int
mkholdingdir(
    char *	diskdir)
{
    struct stat stat_hdp;
    int success = 1;

    if (mkpdir(diskdir, 0770, (uid_t)-1, (gid_t)-1) != 0 && errno != EEXIST) {
	log_add(L_WARNING, _("WARNING: could not create parents of %s: %s"),
		diskdir, strerror(errno));
	success = 0;
    }
    else if (mkdir(diskdir, 0770) != 0 && errno != EEXIST) {
	log_add(L_WARNING, _("WARNING: could not create %s: %s"),
		diskdir, strerror(errno));
	success = 0;
    }
    else if (stat(diskdir, &stat_hdp) == -1) {
	log_add(L_WARNING, _("WARNING: could not stat %s: %s"),
		diskdir, strerror(errno));
	success = 0;
    }
    else {
	if (!S_ISDIR((stat_hdp.st_mode))) {
	    log_add(L_WARNING, _("WARNING: %s is not a directory"),
		    diskdir);
	    success = 0;
	}
	else if (access(diskdir,W_OK) != 0) {
	    log_add(L_WARNING, _("WARNING: directory %s is not writable"),
		    diskdir);
	    success = 0;
	}
    }
    return success;
}
