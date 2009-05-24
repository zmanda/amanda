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

/*
 * Recursion functions
 *
 * These implement a general-purpose walk down the holding-* hierarchy.
 */

/* Perform a custom action for this holding element (disk, dir, file, chunk).
 *
 * If the element is not cruft, the next step into the tree will only take place 
 * if this function returns a nonzero value.
 *
 * The walk is depth-first, with the callback for an element invoked
 * before entering that element.  Callbacks may depend on this behavior.
 *
 * @param datap: generic user-data pointer
 * @param base: the parent of the element being examined, or NULL for 
 * holding disks
 * @param element: the name of the element being examined
 * @param fqpath: fully qualified path to 'element'
 * @param is_cruft: nonzero if this element doesn't belong here
 * @returns: nonzero if the walk should descend into this element.
 */
typedef int (*holding_walk_fn)(
    gpointer datap,
    char *base,
    char *element,
    char *fqpath,
    int is_cruft);

typedef enum {
    STOP_AT_DISK,
    STOP_AT_DIR,
    STOP_AT_FILE,
    STOP_AT_CHUNK
} stop_at_t;

/* Recurse over all holding chunks in a holding file.
 *
 * Call per_chunk_fn for each chunk of the given file
 *
 * datap is passed, unchanged, to all holding_walk_fns.
 *
 * @param hfile: holding file to examine (fully qualified path)
 * @param datap: generic user-data pointer
 * @param per_chunk_fn: function to call for each holding chunk
 */
static void holding_walk_file(
    char *hfile,
    gpointer datap,
    holding_walk_fn per_chunk_fn)
{
    dumpfile_t file;
    char *filename = NULL;

    /* Loop through all cont_filenames (subsequent chunks) */
    filename = stralloc(hfile);
    while (filename != NULL && filename[0] != '\0') {
	int is_cruft = 0;

        /* get the header to look for cont_filename */
        if (!holding_file_get_dumpfile(filename, &file)) {
	    is_cruft = 1;
        }

	if (per_chunk_fn) 
	    per_chunk_fn(datap, 
			hfile, 
			filename, 
			filename, 
			is_cruft);
	amfree(filename);

        /* and go on to the next chunk if this wasn't cruft */
	if (!is_cruft)
	    filename = stralloc(file.cont_filename);
	dumpfile_free_data(&file);
    }

    amfree(filename);
}

/* Recurse over all holding files in a holding directory.
 *
 * Call per_file_fn for each file, and so on, stopping at the level given by 
 * stop_at.
 *
 * datap is passed, unchanged, to all holding_walk_fns.
 *
 * @param hdir: holding directory to examine (fully qualified path)
 * @param datap: generic user-data pointer
 * @param stop_at: do not proceed beyond this level of the hierarchy
 * @param per_file_fn: function to call for each holding file
 * @param per_chunk_fn: function to call for each holding chunk
 */
static void holding_walk_dir(
    char *hdir,
    gpointer datap,
    stop_at_t stop_at,
    holding_walk_fn per_file_fn,
    holding_walk_fn per_chunk_fn)
{
    DIR *dir;
    struct dirent *workdir;
    char *hfile = NULL;
    dumpfile_t dumpf;
    int dumpf_ok;
    int proceed = 1;

    if ((dir = opendir(hdir)) == NULL) {
        if (errno != ENOENT)
           dbprintf(_("Warning: could not open holding dir %s: %s\n"),
                  hdir, strerror(errno));
        return;
    }

    while ((workdir = readdir(dir)) != NULL) {
	int is_cruft = 0;

        if (is_dot_or_dotdot(workdir->d_name))
            continue; /* expected cruft */

        hfile = newvstralloc(hfile,
                     hdir, "/", workdir->d_name,
                     NULL);

        /* filter out various undesirables */
        if (is_emptyfile(hfile))
            is_cruft = 1;

        if (is_dir(hfile)) {
            is_cruft= 1;
        }

        if (!(dumpf_ok=holding_file_get_dumpfile(hfile, &dumpf)) ||
            dumpf.type != F_DUMPFILE) {
            if (dumpf_ok && dumpf.type == F_CONT_DUMPFILE)
                continue; /* silently skip expected file */

            is_cruft = 1;
        }

	if (dumpf.dumplevel < 0 || dumpf.dumplevel > 9) {
	    is_cruft = 1;
	}

	if (per_file_fn) 
	    proceed = per_file_fn(datap, 
			hdir, 
			workdir->d_name, 
			hfile, 
			is_cruft);
	if (!is_cruft && proceed && stop_at != STOP_AT_FILE)
	    holding_walk_file(hfile,
		    datap,
		    per_chunk_fn);
	dumpfile_free_data(&dumpf);
    }

    closedir(dir);
    amfree(hfile);
}

/* Recurse over all holding directories in a holding disk.
 *
 * Call per_dir_fn for each dir, and so on, stopping at the level given by 
 * stop_at.
 *
 * datap is passed, unchanged, to all holding_walk_fns.
 *
 * @param hdisk: holding disk to examine (fully qualified path)
 * @param datap: generic user-data pointer
 * @param stop_at: do not proceed beyond this level of the hierarchy
 * @param per_dir_fn: function to call for each holding dir
 * @param per_file_fn: function to call for each holding file
 * @param per_chunk_fn: function to call for each holding chunk
 */
static void 
holding_walk_disk(
    char *hdisk,
    gpointer datap,
    stop_at_t stop_at,
    holding_walk_fn per_dir_fn,
    holding_walk_fn per_file_fn,
    holding_walk_fn per_chunk_fn)
{
    DIR *dir;
    struct dirent *workdir;
    char *hdir = NULL;
    int proceed = 1;

    if ((dir = opendir(hdisk)) == NULL) {
        if (errno != ENOENT)
           dbprintf(_("Warning: could not open holding disk %s: %s\n"),
                  hdisk, strerror(errno));
        return;
    }

    while ((workdir = readdir(dir)) != NULL) {
	int is_cruft = 0;

        if (is_dot_or_dotdot(workdir->d_name))
            continue; /* expected cruft */

        hdir = newvstralloc(hdir,
                     hdisk, "/", workdir->d_name,
                     NULL);

        /* detect cruft */
        if (!is_dir(hdir)) {
	    is_cruft = 1;
        } else if (!is_datestr(workdir->d_name)) {
            /* EXT2/3 leave these in the root of each volume */
            if (strcmp(workdir->d_name, "lost+found") == 0)
		continue; /* expected cruft */
	    else
		is_cruft = 1; /* unexpected */
        }

	if (per_dir_fn) 
	    proceed = per_dir_fn(datap, 
			hdisk, 
			workdir->d_name, 
			hdir, 
			is_cruft);
	if (!is_cruft && proceed && stop_at != STOP_AT_DIR)
	    holding_walk_dir(hdir,
		    datap,
		    stop_at,
		    per_file_fn,
		    per_chunk_fn);
    }

    closedir(dir);
    amfree(hdir);
}

/* Recurse over all holding disks.
 *
 * Call per_disk_fn for each disk, per_dir_fn for each dir, and so on, stopping
 * at the level given by stop_at.
 *
 * datap is passed, unchanged, to all holding_walk_fns.
 *
 * @param datap: generic user-data pointer
 * @param stop_at: do not proceed beyond this level of the hierarchy
 * @param per_disk_fn: function to call for each holding disk
 * @param per_dir_fn: function to call for each holding dir
 * @param per_file_fn: function to call for each holding file
 * @param per_chunk_fn: function to call for each holding chunk
 */
static void 
holding_walk(
    gpointer datap,
    stop_at_t stop_at,
    holding_walk_fn per_disk_fn,
    holding_walk_fn per_dir_fn,
    holding_walk_fn per_file_fn,
    holding_walk_fn per_chunk_fn)
{
    identlist_t    il;
    holdingdisk_t *hdisk_conf;
    char *hdisk;
    int proceed = 1;

    for (il = getconf_identlist(CNF_HOLDINGDISK);
		il != NULL;
		il = il->next) {
	int is_cruft = 0;
	hdisk_conf = lookup_holdingdisk(il->data);

	hdisk = holdingdisk_get_diskdir(hdisk_conf);
	if (!is_dir(hdisk))
	    is_cruft = 1;

	if (per_disk_fn) 
	    proceed = per_disk_fn(datap, 
			NULL, 
			hdisk, 
			hdisk, 
			0);
	if (proceed && stop_at != STOP_AT_DISK)
	    holding_walk_disk(hdisk,
		    datap,
		    stop_at,
		    per_dir_fn,
		    per_file_fn,
		    per_chunk_fn);
    }
}

/*
 * holding_get_* functions
 */
typedef struct {
    GSList *result;
    int fullpaths;
} holding_get_datap_t;

/* Functor for holding_get_*; adds 'element' or 'fqpath' to
 * the result.
 */
static int
holding_get_walk_fn(
    gpointer datap,
    G_GNUC_UNUSED char *base,
    char *element,
    char *fqpath,
    int is_cruft)
{
    holding_get_datap_t *data = (holding_get_datap_t *)datap;

    /* ignore cruft */
    if (is_cruft) return 0;

    if (data->fullpaths)
	data->result = g_slist_insert_sorted(data->result,
		stralloc(fqpath), 
		g_compare_strings);
    else
	data->result = g_slist_insert_sorted(data->result, 
		stralloc(element), 
		g_compare_strings);

    /* don't proceed any deeper */
    return 0;
}

GSList *
holding_get_disks(void)
{
    holding_get_datap_t data;
    data.result = NULL;
    data.fullpaths = 1; /* ignored anyway */

    holding_walk((gpointer)&data,
	STOP_AT_DISK,
	holding_get_walk_fn, NULL, NULL, NULL);

    return data.result;
}

GSList *
holding_get_files(
    char *hdir,
    int fullpaths)
{
    holding_get_datap_t data;
    data.result = NULL;
    data.fullpaths = fullpaths;

    if (hdir) {
        holding_walk_dir(hdir, (gpointer)&data,
	    STOP_AT_FILE,
	    holding_get_walk_fn, NULL);
    } else {
        holding_walk((gpointer)&data,
	    STOP_AT_FILE,
	    NULL, NULL, holding_get_walk_fn, NULL);
    }

    return data.result;
}

GSList *
holding_get_file_chunks(char *hfile)
{
    holding_get_datap_t data;
    data.result = NULL;
    data.fullpaths = 1;

    holding_walk_file(hfile, (gpointer)&data,
	holding_get_walk_fn);

    return data.result;
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

        if (file.type != F_DUMPFILE) {
	    dumpfile_free_data(&file);
            continue;
	}

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
        if (!date_matches) {
	    dumpfile_free_data(&file);
            continue;
	}

        /* check that the hostname and disk are in the disklist */
        dp = lookup_disk(file.name, file.disk);
        if (dp == NULL) {
	    dbprintf(_("%s: disk %s:%s not in database, skipping it."),
                        (char *)file_elt->data, file.name, file.disk);
	    dumpfile_free_data(&file);
            continue;
        }

        /* passed all tests -- we'll flush this file */
        result_list = g_slist_insert_sorted(result_list, 
	    stralloc(file_elt->data), 
	    g_compare_strings);
	dumpfile_free_data(&file);
    }

    if (file_list) g_slist_free_full(file_list);

    return result_list;
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
	if (!g_slist_find_custom(datestamps, dfile.datestamp,
				 g_compare_strings)) {
	    datestamps = g_slist_insert_sorted(datestamps, 
					       stralloc(dfile.datestamp), 
					       g_compare_strings);
	}
	dumpfile_free_data(&dfile);
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
            size = -1;
	    break;
        }
        size += (finfo.st_size+(off_t)1023)/(off_t)1024;
        if (strip_headers)
            size -= (off_t)(DISK_BLOCK_BYTES / 1024);

        /* get the header to look for cont_filename */
        if (!holding_file_get_dumpfile(filename, &file)) {
	    dbprintf(_("holding_file_size: open of %s failed.\n"), filename);
            size = -1;
	    break;
        }

        /* on to the next chunk */
        filename = newstralloc(filename, file.cont_filename);
	dumpfile_free_data(&file);
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

    if(full_read(fd, buffer, SIZEOF(buffer)) != sizeof(buffer)) {
        aclose(fd);
        return 0;
    }
    aclose(fd);

    parse_file_header(buffer, file, SIZEOF(buffer));
    return 1;
}

/*
 * Cleanup
 */

typedef struct {
    corrupt_dle_fn corrupt_dle;
    FILE *verbose_output;
} holding_cleanup_datap_t;

static int
holding_cleanup_disk(
    gpointer datap,
    G_GNUC_UNUSED char *base,
    G_GNUC_UNUSED char *element,
    char *fqpath,
    int is_cruft)
{
    holding_cleanup_datap_t *data = (holding_cleanup_datap_t *)datap;

    if (data->verbose_output) {
	if (is_cruft)
	    g_fprintf(data->verbose_output, 
		_("Invalid holding disk '%s'\n"), fqpath);
	else
	    g_fprintf(data->verbose_output, 
		_("Cleaning up holding disk '%s'\n"), fqpath);
    }

    return 1;
}

static int
holding_cleanup_dir(
    gpointer datap,
    G_GNUC_UNUSED char *base,
    char *element,
    char *fqpath,
    int is_cruft)
{
    holding_cleanup_datap_t *data = (holding_cleanup_datap_t *)datap;

    if (is_cruft) {
	if (data->verbose_output)
	    g_fprintf(data->verbose_output, 
		_("Invalid holding directory '%s'\n"), fqpath);
	return 0;
    }

    /* try removing it */
    if (rmdir(fqpath) == 0) {
	/* success, so don't try to walk into it */
	if (data->verbose_output)
	    g_fprintf(data->verbose_output,
		_(" ..removed empty directory '%s'\n"), element);
	return 0;
    }

    if (data->verbose_output)
	g_fprintf(data->verbose_output, 
	    _(" ..cleaning up holding directory '%s'\n"), element);

    return 1;
}

static int
holding_cleanup_file(
    gpointer datap,
    G_GNUC_UNUSED char *base,
    char *element,
    char *fqpath,
    int is_cruft)
{
    holding_cleanup_datap_t *data = (holding_cleanup_datap_t *)datap;
    int stat;
    int l;
    dumpfile_t file;
    disk_t *dp;

    if (is_cruft) {
	if (data->verbose_output)
	    g_fprintf(data->verbose_output, 
		_("Invalid holding file '%s'\n"), element);
	return 0;
    }


    stat = holding_file_get_dumpfile(fqpath, &file);

    if (!stat) {
	if (data->verbose_output)
	    g_fprintf(data->verbose_output, 
		_("Could not read read header from '%s'\n"), element);
	dumpfile_free_data(&file);
	return 0;
    }

    if (file.type != F_DUMPFILE && file.type != F_CONT_DUMPFILE) {
	if (data->verbose_output)
	    g_fprintf(data->verbose_output, 
		_("File '%s' is not a dump file\n"), element);
	dumpfile_free_data(&file);
	return 0;
    }

    if(file.dumplevel < 0 || file.dumplevel > 9) {
	if (data->verbose_output)
	    g_fprintf(data->verbose_output, 
		_("File '%s' has invalid level %d\n"), element, file.dumplevel);
	dumpfile_free_data(&file);
	return 0;
    }

    dp = lookup_disk(file.name, file.disk);

    if (dp == NULL) {
	if (data->verbose_output)
	    g_fprintf(data->verbose_output, 
		_("File '%s' is for '%s:%s', which is not in the disklist\n"), 
		    element, file.name, file.disk);
	dumpfile_free_data(&file);
	return 0;
    }

    if ((l = strlen(element)) >= 7 && strncmp(&fqpath[l-4],".tmp",4) == 0) {
	char *destname;

	/* generate a name without '.tmp' */
	destname = stralloc(fqpath);
	destname[strlen(destname) - 4] = '\0';

	/* OK, it passes muster -- rename it to salvage some data,
	 * and mark the DLE as corrupted */
	if (data->verbose_output)
	    g_fprintf(data->verbose_output, 
		_("Processing partial holding file '%s'\n"), element);

	if(rename_tmp_holding(destname, 0)) {
	    if (data->corrupt_dle)
		data->corrupt_dle(dp->host->hostname, dp->name);
	} else {
	    dbprintf(_("rename_tmp_holding(%s) failed\n"), destname);
	    if (data->verbose_output)
		g_fprintf(data->verbose_output, 
		    _("Rename of '%s' to '%s' failed.\n"), element, destname);
	}

	amfree(destname);
    }

    dumpfile_free_data(&file);
    return 1;
}

void
holding_cleanup(
    corrupt_dle_fn corrupt_dle,
    FILE *verbose_output)
{
    holding_cleanup_datap_t data;
    data.corrupt_dle = corrupt_dle;
    data.verbose_output = verbose_output;

    holding_walk((gpointer)&data,
	STOP_AT_FILE,
	holding_cleanup_disk,
	holding_cleanup_dir,
	holding_cleanup_file,
	NULL);
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
    size_t buflen;
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
	buflen = full_read(fd, buffer, SIZEOF(buffer));
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
		dumpfile_free_data(&file);
		amfree(filename);
		amfree(filename_tmp);
		return 0;

	    }
	    file.is_partial = 1;
	    if (debug_holding > 1)
		dump_dumpfile_t(&file);
            header = build_header(&file, NULL, DISK_BLOCK_BYTES);
	    if (!header) /* this shouldn't happen */
		error(_("header does not fit in %zd bytes"), (size_t)DISK_BLOCK_BYTES);
	    if (full_write(fd, header, DISK_BLOCK_BYTES) != DISK_BLOCK_BYTES) {
		dbprintf(_("rename_tmp_holding: writing new header failed: %s"),
			strerror(errno));
		dumpfile_free_data(&file);
		amfree(filename);
		amfree(filename_tmp);
		close(fd);
		return 0;
	    }
	    close(fd);
	}
	filename = newstralloc(filename, file.cont_filename);
	dumpfile_free_data(&file);
    }
    amfree(filename);
    amfree(filename_tmp);
    return 1;
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
