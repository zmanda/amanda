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

/* sanity check that datestamp is of the form YYYYMMDD or 
 * YYYYMMDDhhmmss
 *
 * @param fname: a filename (without directory)
 * @returns: boolean
 */
static int is_datestr(char *fname);

/*
 * Static variables */
static int verbose = 0;

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
 * Verbosity
 */
int
holding_set_verbosity(int v)
{
    int old = verbose;
    verbose = v;
    return old;
}

/*
 * Holding directories
 */

static void
holding_get_directories_per_disk(
    char *hdisk,
    sl_t *date_list,
    int fullpaths,
    sl_t *rv)
{
    DIR *dir;
    struct dirent *workdir;
    char *hdir = NULL;
    sle_t *dl;
    int date_found;

    if ((dir = opendir(hdisk)) == NULL) {
        if (verbose && errno != ENOENT)
           printf(_("Warning: could not open holding disk %s: %s\n"),
                  hdisk, strerror(errno));
        return;
    }

    if (verbose)
        printf(_("Scanning %s...\n"), hdisk);

    while ((workdir = readdir(dir)) != NULL) {
        if (is_dot_or_dotdot(workdir->d_name))
            continue;

        if(verbose) 
            printf("  %s: ", workdir->d_name);
            
        hdir = newvstralloc(hdir,
                     hdisk, "/", workdir->d_name,
                     NULL);

        /* filter out various undesirables */
        if (!is_dir(hdir)) {
            if (verbose)
                puts(_("skipping cruft file, perhaps you should delete it."));
        } else if (!is_datestr(workdir->d_name)) {
            /* EXT2/3 leave these in the root of each volume */
            if (strcmp(workdir->d_name, "lost+found")==0)
                puts(_("skipping system directory"));
            if (verbose)
                puts(_("skipping cruft directory, perhaps you should delete it."));
        } else {
            /* found a holding directory -- keep it */
	    if (date_list) {
		date_found = 0;
		for (dl= date_list->first; dl != NULL; dl = dl->next) {
		    if (strcmp(dl->name, workdir->d_name)) {
			date_found = 1;
			break;
		    }
		}
	    } else {
		date_found = 1;
	    }
	    if (date_found == 1) {
		if (fullpaths)
                    rv = insert_sort_sl(rv, hdir);
		else
                    rv = insert_sort_sl(rv, workdir->d_name);
		if (verbose) {
                    puts(_("found Amanda directory."));
		}
	    }
        }
    }

    if (hdir) amfree(hdir);
}


sl_t *
holding_get_directories(
    char *hdisk,
    sl_t *date_list,
    int fullpaths)
{
    holdingdisk_t *hdisk_conf;
    sl_t *rv;

    rv = new_sl();
    if (!rv) {
        return NULL;
    }

    /* call _per_disk for the hdisk we were given, or for all
     * hdisks if we were given NULL */
    if (hdisk) {
        holding_get_directories_per_disk(hdisk, date_list, fullpaths, rv);
    } else {
        for (hdisk_conf = getconf_holdingdisks(); 
                    hdisk_conf != NULL;
                    hdisk_conf = hdisk_conf->next) {
            hdisk = holdingdisk_get_diskdir(hdisk_conf);
            holding_get_directories_per_disk(hdisk, date_list, fullpaths, rv);
        }
    }

    return rv;
}

/*
 * Holding files
 */
static void
holding_get_files_per_dir(
    char *hdir,
    int fullpaths,
    sl_t *rv)
{
    DIR *dir;
    struct dirent *workdir;
    char *hfile = NULL;
    dumpfile_t dumpf;
    int dumpf_ok;

    if ((dir = opendir(hdir)) == NULL) {
        if (verbose && errno != ENOENT)
           printf(_("Warning: could not open holding dir %s: %s\n"),
                  hdir, strerror(errno));
        return;
    }

    if (verbose)
        printf(_("Scanning %s...\n"), hdir);

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
            if (verbose)
                printf(_("%s: ignoring directory\n"), hfile);
            continue;
        }

        if (!(dumpf_ok=holding_file_get_dumpfile(hfile, &dumpf)) ||
            dumpf.type != F_DUMPFILE) {
            if (dumpf_ok && dumpf.type == F_CONT_DUMPFILE)
                continue; /* silently skip expected file */
            if (verbose)
                printf(_("%s: not a dumpfile\n"), hfile);
            continue;
        }

	if (dumpf.dumplevel < 0 || dumpf.dumplevel > 9) {
            if (verbose)
                printf(_("%s: ignoring file with bogus dump level %d.\n"),
                       hfile, dumpf.dumplevel);
	    continue;
	}

        /* found a holding file -- keep it */
        if (fullpaths)
            rv = insert_sort_sl(rv, hfile);
        else
            rv = insert_sort_sl(rv, workdir->d_name);
    }

    if (hfile) amfree(hfile);
}

sl_t *
holding_get_files(
    char *hdir,
    sl_t *date_list,
    int fullpaths)
{
    sl_t *hdirs;
    sle_t *e;
    sl_t *rv;

    rv = new_sl();
    if (!rv) {
        return NULL;
    }

    /* call _per_dir for the hdir we were given, or for all
     * hdir if we were given NULL */
    if (hdir) {
        holding_get_files_per_dir(hdir, fullpaths, rv);
    } else {
        hdirs = holding_get_directories(NULL, date_list, 1);
        for (e = hdirs->first; e != NULL; e = e->next) {
            holding_get_files_per_dir(e->name, fullpaths, rv);
        }
    }

    return rv;
}

sl_t *
holding_get_files_for_flush(
    sl_t *dateargs,
    int interactive)
{
    sl_t *date_list;
    sl_t *file_list;
    sl_t *result_list;
    sle_t *datearg;
    sle_t *date, *next_date;
    sle_t *file_elt;
    int date_matches;
    disk_t *dp;
    char *host;
    char *disk;
    char *datestamp;
    filetype_t filetype;

    /* make date_list the intersection of available holding directories and
     * the dateargs parameter.  */
    if (dateargs) {
        int ok;

        date_list = pick_all_datestamp(verbose);
        for (date = date_list->first; date != NULL;) {
            next_date = date->next;
            ok = 0;
            for(datearg=dateargs->first; datearg != NULL && ok==0;
                datearg = datearg->next) {
                ok = match_datestamp(datearg->name, date->name);
            }
            if(ok == 0) { /* remove dir */
                remove_sl(date_list, date);
            }
            date = next_date;
        }
    }
    else {
        /* no date args were provided, so use everything */
        if (interactive)
            date_list = pick_datestamp(verbose);
        else
            date_list = pick_all_datestamp(verbose);
    }

    result_list = new_sl();
    if (!result_list) {
        return NULL;
    }

    /* loop over *all* files, checking each one */
    file_list = holding_get_files(NULL, date_list, 1);
    for (file_elt = file_list->first; file_elt != NULL; file_elt = file_elt->next) {
        /* get info on that file */
        filetype = holding_file_read_header(file_elt->name, &host, &disk, NULL, &datestamp);
        if (filetype != F_DUMPFILE)
            continue;

        /* check that the hostname and disk are in the disklist */
        dp = lookup_disk(host, disk);
        if (dp == NULL) {
            if (verbose)
	        printf(_("%s: disk %s:%s not in database, skipping it."),
                        file_elt->name, host, disk);
            continue;
        }

        /* passed all tests -- we'll flush this file */
        result_list = insert_sort_sl(result_list, file_elt->name);
    }

    if (date_list) free_sl(date_list);
    if (file_list) free_sl(file_list);

    return result_list;
}

sl_t *
holding_get_file_chunks(char *hfile)
{
    dumpfile_t file;
    char *filename;
    sl_t *rv = new_sl();

    if (!rv) {
        return NULL;
    }

    /* Loop through all cont_filenames (subsequent chunks) */
    filename = stralloc(hfile);
    while (filename != NULL && filename[0] != '\0') {
        /* get the header to look for cont_filename */
        if (!holding_file_get_dumpfile(filename, &file)) {
            if (verbose)
                printf(_("holding_get_file_chunks: open of %s failed.\n"), filename);
            amfree(filename);
            return rv;
        }

        /* add the file to the results */
        insert_sort_sl(rv, filename);

        /* and go on to the next chunk */
        filename = newstralloc(filename, file.cont_filename);
    }
    amfree(filename);
    return rv;
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
            if (verbose)
                printf(_("stat %s: %s\n"), filename, strerror(errno));
            return (off_t)-1;
        }
        size += (finfo.st_size+(off_t)1023)/(off_t)1024;
        if (strip_headers)
            size -= (off_t)(DISK_BLOCK_BYTES / 1024);

        /* get the header to look for cont_filename */
        if (!holding_file_get_dumpfile(filename, &file)) {
            if (verbose)
                printf(_("holding_file_size: open of %s failed.\n"), filename);
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
    sl_t *chunklist;
    sle_t *chunk;

    chunklist = holding_get_file_chunks(hfile);
    if (!chunklist)
        return 0;

    for (chunk = chunklist->first; chunk != NULL; chunk = chunk->next) {
        if (unlink(chunk->name)<0) {
            if (verbose)
                printf(_("holding_file_unlink: could not unlink %s: %s\n"),
                    chunk->name, strerror(errno));
            return 0;
        }
    }

    return 1;
}

filetype_t
holding_file_read_header( 
    char *	fname,
    char **	hostname,
    char **	diskname,
    int *	level,
    char ** datestamp)
{
    dumpfile_t file;

    if (hostname) *hostname = NULL;
    if (diskname) *diskname = NULL;
    if (datestamp) *datestamp = NULL;

    if (!holding_file_get_dumpfile(fname, &file)) {
        return F_UNKNOWN;
    }

    if(file.type != F_DUMPFILE && file.type != F_CONT_DUMPFILE) {
        return file.type;
    }

    if (hostname) *hostname = stralloc(file.name);
    if (diskname) *diskname = stralloc(file.disk);
    if (level) *level = file.dumplevel;
    if (datestamp) *datestamp = stralloc(file.datestamp);

    return file.type;
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
    if((fd = open(fname, O_RDONLY)) == -1)
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
 * Interactive functions 
 */

sl_t *
pick_all_datestamp(
    int	v)
{
    int old_verbose = holding_set_verbosity(v);
    sl_t *rv;

    /* get all holding directories, without full paths -- this
     * will be datestamps only */
    rv = holding_get_directories(NULL, NULL, 0);

    holding_set_verbosity(old_verbose);
    return rv;
}

sl_t *
pick_datestamp(
    int		verbose)
{
    sl_t *holding_list;
    sl_t *r_holding_list = NULL;
    sle_t *dir;
    char **directories = NULL;
    int i;
    char *answer = NULL;
    char *a = NULL;
    int ch = 0;
    char max_char = '\0', chupper = '\0';

    holding_list = pick_all_datestamp(verbose);

    if(holding_list->nb_element == 0) {
	return holding_list;
    }
    else if(holding_list->nb_element == 1 || !verbose) {
	return holding_list;
    }
    else {
	directories = alloc((holding_list->nb_element) * SIZEOF(char *));
	for(dir = holding_list->first, i=0; dir != NULL; dir = dir->next,i++) {
	    directories[i] = dir->name;
	}

	while(1) {
	    puts(_("\nMultiple Amanda directories, please pick one by letter:"));
	    for(dir = holding_list->first, max_char = 'A';
		dir != NULL && max_char <= 'Z';
		dir = dir->next, max_char++) {
		printf("  %c. %s\n", max_char, dir->name);
	    }
	    max_char--;
	    printf(_("Select directories to flush [A..%c]: [ALL] "), max_char);
	    fflush(stdout); fflush(stderr);
	    amfree(answer);
	    if ((answer = agets(stdin)) == NULL) {
		clearerr(stdin);
		continue;
	    }

	    if (*answer == '\0' || strncasecmp(answer, "ALL", 3) == 0) {
		break;
	    }

	    a = answer;
	    while ((ch = *a++) != '\0') {
		if (!isspace(ch))
		    break;
	    }

	    do {
		if (isspace(ch) || ch == ',') {
		    continue;
		}
		chupper = (char)toupper(ch);
		if (chupper < 'A' || chupper > max_char) {
		    free_sl(r_holding_list);
		    r_holding_list = NULL;
		    break;
		}
		r_holding_list = append_sl(r_holding_list,
					   directories[chupper - 'A']);
	    } while ((ch = *a++) != '\0');
	    if (r_holding_list && ch == '\0') {
		free_sl(holding_list);
		holding_list = r_holding_list;
		break;
	    }
	}
    }
    amfree(directories);
    amfree(answer);
    return holding_list;
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
	if((fd = open(filename_tmp,O_RDONLY)) == -1) {
	    fprintf(stderr,_("rename_tmp_holding: open of %s failed: %s\n"),filename_tmp,strerror(errno));
	    amfree(filename);
	    amfree(filename_tmp);
	    return 0;
	}
	buflen = fullread(fd, buffer, SIZEOF(buffer));
	close(fd);

	if(rename(filename_tmp, filename) != 0) {
	    fprintf(stderr,
		    _("rename_tmp_holding: could not rename \"%s\" to \"%s\": %s"),
		    filename_tmp, filename, strerror(errno));
	}

	if (buflen <= 0) {
	    fprintf(stderr,_("rename_tmp_holding: %s: empty file?\n"), filename);
	    amfree(filename);
	    amfree(filename_tmp);
	    return 0;
	}
	parse_file_header(buffer, &file, (size_t)buflen);
	if(complete == 0 ) {
	    if((fd = open(filename, O_RDWR)) == -1) {
		fprintf(stderr, _("rename_tmp_holdingX: open of %s failed: %s\n"),
			filename, strerror(errno));
		amfree(filename);
		amfree(filename_tmp);
		return 0;

	    }
	    file.is_partial = 1;
	    build_header(buffer, &file, SIZEOF(buffer));
	    fullwrite(fd, buffer, SIZEOF(buffer));
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
    char *	diskdir,
    int		verbose)
{
    DIR *topdir;
    struct dirent *workdir;

    if((topdir = opendir(diskdir)) == NULL) {
	if(verbose && errno != ENOENT)
	    printf(_("Warning: could not open holding dir %s: %s\n"),
		   diskdir, strerror(errno));
	return;
   }

    /* find all directories of the right format  */

    if(verbose)
	printf(_("Scanning %s...\n"), diskdir);
    if ((chdir(diskdir)) == -1) {
	log_add(L_INFO, _("%s: could not chdir: %s"),
		    diskdir, strerror(errno));
    }
    while((workdir = readdir(topdir)) != NULL) {
	if(strcmp(workdir->d_name, ".") == 0
	   || strcmp(workdir->d_name, "..") == 0
	   || strcmp(workdir->d_name, "lost+found") == 0)
	    continue;

	if(verbose)
	    printf("  %s: ", workdir->d_name);
	if(!is_dir(workdir->d_name)) {
	    if(verbose)
	        puts(_("skipping cruft file, perhaps you should delete it."));
	}
	else if(!is_datestr(workdir->d_name)) {
	    if(verbose && (strcmp(workdir->d_name, "lost+found")!=0) )
	        puts(_("skipping cruft directory, perhaps you should delete it."));
	}
	else if(rmdir(workdir->d_name) == 0) {
	    if(verbose)
	        puts(_("deleted empty Amanda directory."));
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
