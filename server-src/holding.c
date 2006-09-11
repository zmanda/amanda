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

static sl_t *scan_holdingdisk(sl_t *holding_list, char *diskdir, int verbose);
static sl_t *scan_holdingdir(sl_t *holding_list, holdingdisk_t *holdp, char *datestamp);

int
is_dir(
    char *fname)
{
    struct stat statbuf;

    if(stat(fname, &statbuf) == -1) return 0;

    return (statbuf.st_mode & S_IFDIR) == S_IFDIR;
}

int
is_emptyfile(
    char *fname)
{
    struct stat statbuf;

    if(stat(fname, &statbuf) == -1) return 0;

    return ((statbuf.st_mode & S_IFDIR) != S_IFDIR) &&
		(statbuf.st_size == (off_t)0);
}


/*
 * sanity check on datestamp of the form YYYYMMDD or YYYYMMDDhhmmss
 */

int
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


int
non_empty(
    char *	fname)
{
    DIR *dir;
    struct dirent *entry;
    int gotentry;

    if((dir = opendir(fname)) == NULL)
	return 0;

    gotentry = 0;
    while(!gotentry && (entry = readdir(dir)) != NULL) {
	gotentry = !is_dot_or_dotdot(entry->d_name);
    }

    closedir(dir);
    return gotentry;
}


static sl_t *
scan_holdingdisk(
    sl_t *	holding_list,
    char *	diskdir,
    int		verbose)
{
    DIR *topdir;
    struct dirent *workdir;
    char *entryname = NULL;

    if((topdir = opendir(diskdir)) == NULL) {
	if(verbose && errno != ENOENT)
	   printf("Warning: could not open holding dir %s: %s\n",
		  diskdir, strerror(errno));
	return holding_list;
    }

    /* find all directories of the right format  */

    if(verbose)
	printf("Scanning %s...\n", diskdir);
    while((workdir = readdir(topdir)) != NULL) {
	if(is_dot_or_dotdot(workdir->d_name)) {
	    continue;
	}
	entryname = newvstralloc(entryname,
				 diskdir, "/", workdir->d_name, NULL);
	if(verbose) {
	    printf("  %s: ", workdir->d_name);
	}
	if(!is_dir(entryname)) {
	    if(verbose) {
	        puts("skipping cruft file, perhaps you should delete it.");
	    }
	} else if(!is_datestr(workdir->d_name)) {
	    if(verbose && (strcmp(workdir->d_name, "lost+found")!=0) ) {
	        puts("skipping cruft directory, perhaps you should delete it.");
	    }
	} else {
	    holding_list = insert_sort_sl(holding_list, workdir->d_name);
	    if(verbose) {
		puts("found Amanda directory.");
	    }
	}
    }
    closedir(topdir);
    amfree(entryname);
    return holding_list;
}


static sl_t *
scan_holdingdir(
    sl_t *		holding_list,
    holdingdisk_t *	holdp,
    char *		datestamp)
{
    DIR *workdir;
    struct dirent *entry;
    char *dirname = NULL;
    char *destname = NULL;
    disk_t *dp;
    dumpfile_t file;

    dirname = vstralloc(holdingdisk_get_diskdir(holdp), "/", datestamp, NULL);
    if((workdir = opendir(dirname)) == NULL) {
	if(errno != ENOENT)
	    log_add(L_INFO, "%s: could not open dir: %s",
		    dirname, strerror(errno));
	amfree(dirname);
	return holding_list;
    }
    if ((chdir(dirname)) == -1) {
	log_add(L_INFO, "%s: could not chdir: %s",
		    dirname, strerror(errno));
	amfree(dirname);
	return holding_list;
    }

    while((entry = readdir(workdir)) != NULL) {
	if(strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
	    continue;

	if(is_emptyfile(entry->d_name))
	    continue;

	destname = newvstralloc(destname,
				dirname, "/", entry->d_name,
				NULL);
	get_dumpfile(destname, &file);
	if( file.type != F_DUMPFILE) {
	    if( file.type != F_CONT_DUMPFILE )
		log_add(L_INFO, "%s: ignoring cruft file.", entry->d_name);
	    continue;
	}

	dp = lookup_disk(file.name, file.disk);

	if (dp == NULL) {
	    log_add(L_INFO, "%s: disk %s:%s not in database, skipping it.",
		    entry->d_name, file.name, file.disk);
	    continue;
	}

	if(file.dumplevel < 0 || file.dumplevel > 9) {
	    log_add(L_INFO, "%s: ignoring file with bogus dump level %d.",
		    entry->d_name, file.dumplevel);
	    continue;
	}

	holding_list = append_sl(holding_list, destname);
    }
    closedir(workdir);
    amfree(dirname);
    amfree(destname);
    return holding_list;
}



sl_t *
get_flush(
    sl_t *dateargs,
    char *datestamp,  /* don't do this date */
    int amflush,
    int verbose)
{
    sl_t *holding_list;
    sl_t *date_list;
    sle_t *datearg;
    sle_t *date, *next_date;
    holdingdisk_t *hdisk;
    char current_dir[PATH_MAX];

    holding_list = new_sl();

    if (getcwd(current_dir, SIZEOF(current_dir)-1) == NULL) {
	log_add(L_INFO, "get_flush: could get current working directory: %s",
		    strerror(errno));
	return holding_list;
    }

    if(dateargs) {
	int ok;

	date_list = pick_all_datestamp(verbose);
	for(date = date_list->first; date != NULL;) {
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
    else if (amflush) {
	date_list = pick_datestamp(verbose);
    }
    else {
	date_list = pick_all_datestamp(verbose);
    }

    for(date = date_list->first; date !=NULL; date = date->next) {
	if(!datestamp || strcmp(datestamp,date->name) != 0) {
	    for(hdisk = getconf_holdingdisks(); hdisk != NULL;
						hdisk = hdisk->next) {
		holding_list = scan_holdingdir(holding_list, hdisk, date->name);
	    }
	}
    }

    free_sl(date_list);
    date_list = NULL;
    if (chdir(current_dir) == -1) {
	log_add(L_INFO, "%s: could not chdir: %s",
		    current_dir, strerror(errno));
    }
    return(holding_list);
}


sl_t *
pick_all_datestamp(
    int		verbose)
{
    sl_t *holding_list = NULL;
    holdingdisk_t *hdisk;

    holding_list = new_sl();
    for(hdisk = getconf_holdingdisks(); hdisk != NULL; hdisk = hdisk->next)
	holding_list = scan_holdingdisk(holding_list, holdingdisk_get_diskdir(hdisk), verbose);

    return holding_list;
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
	    puts("\nMultiple Amanda directories, please pick one by letter:");
	    for(dir = holding_list->first, max_char = 'A';
		dir != NULL && max_char <= 'Z';
		dir = dir->next, max_char++) {
		printf("  %c. %s\n", max_char, dir->name);
	    }
	    max_char--;
	    printf("Select directories to flush [A..%c]: [ALL] ", max_char);
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


filetype_t
get_amanda_names( char *	fname,
    char **	hostname,
    char **	diskname,
    int *	level)
{
    dumpfile_t file;
    char buffer[DISK_BLOCK_BYTES];
    int fd;
    *hostname = *diskname = NULL;

    memset(buffer, 0, sizeof(buffer));
    if((fd = open(fname, O_RDONLY)) == -1)
	return F_UNKNOWN;

    if(fullread(fd, buffer, SIZEOF(buffer)) != (ssize_t)sizeof(buffer)) {
	aclose(fd);
	return F_UNKNOWN;
    }
    aclose(fd);

    parse_file_header(buffer, &file, SIZEOF(buffer));
    if(file.type != F_DUMPFILE && file.type != F_CONT_DUMPFILE) {
	return file.type;
    }
    *hostname = stralloc(file.name);
    *diskname = stralloc(file.disk);
    *level = file.dumplevel;

    return file.type;
}


void
get_dumpfile(
    char *	fname,
    dumpfile_t *file)
{
    char buffer[DISK_BLOCK_BYTES];
    int fd;

    memset(buffer, 0, sizeof(buffer));

    fh_init(file);
    file->type = F_UNKNOWN;
    if((fd = open(fname, O_RDONLY)) == -1)
	return;

    if(fullread(fd, buffer, SIZEOF(buffer)) != (ssize_t)sizeof(buffer)) {
	aclose(fd);
	return;
    }
    aclose(fd);

    parse_file_header(buffer, file, SIZEOF(buffer));
    return;
}


off_t
size_holding_files(
    char *	holding_file,
    int		strip_headers)
{
    int fd;
    ssize_t buflen;
    char buffer[DISK_BLOCK_BYTES];
    dumpfile_t file;
    char *filename;
    off_t size = (off_t)0;
    struct stat finfo;

    memset(buffer, 0, sizeof(buffer));
    filename = stralloc(holding_file);
    while(filename != NULL && filename[0] != '\0') {
	if((fd = open(filename,O_RDONLY)) == -1) {
	    fprintf(stderr,"size_holding_files: open of %s failed: %s\n",filename,strerror(errno));
	    amfree(filename);
	    return (off_t)-1;
	}
	if ((buflen = fullread(fd, buffer, SIZEOF(buffer))) > 0) {
		parse_file_header(buffer, &file, (size_t)buflen);
	}
	close(fd);
	if(stat(filename, &finfo) == -1) {
	    printf("stat %s: %s\n", filename, strerror(errno));
	    finfo.st_size = (off_t)0;
	}
	size += (finfo.st_size+(off_t)1023)/(off_t)1024;
	if(strip_headers)
	    size -= (off_t)(DISK_BLOCK_BYTES / 1024);
	if(buflen > 0) {
	    filename = newstralloc(filename, file.cont_filename);
	}
	else {
	    amfree(filename);
	}
    }
    amfree(filename);
    return size;
}


int
unlink_holding_files(
    char *	holding_file)
{
    int fd;
    ssize_t buflen;
    char buffer[DISK_BLOCK_BYTES];
    dumpfile_t file;
    char *filename;

    memset(buffer, 0, sizeof(buffer));
    filename = stralloc(holding_file);
    while(filename != NULL && filename[0] != '\0') {
	if((fd = open(filename,O_RDONLY)) == -1) {
	    fprintf(stderr,"unlink_holding_files: open of %s failed: %s\n",filename,strerror(errno));
	    amfree(filename);
	    return 0;
	}
	if ((buflen = fullread(fd, buffer, SIZEOF(buffer))) > 0) {
	    parse_file_header(buffer, &file, (size_t)buflen);
	}
	close(fd);
	unlink(filename);
	if(buflen > 0) {
	    filename = newstralloc(filename, file.cont_filename);
	}
	else {
	    amfree(filename);
	}
    }
    amfree(filename);
    return 1;
}


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
	    fprintf(stderr,"rename_tmp_holding: open of %s failed: %s\n",filename_tmp,strerror(errno));
	    amfree(filename);
	    amfree(filename_tmp);
	    return 0;
	}
	buflen = fullread(fd, buffer, SIZEOF(buffer));
	close(fd);

	if(rename(filename_tmp, filename) != 0) {
	    fprintf(stderr,
		    "rename_tmp_holding: could not rename \"%s\" to \"%s\": %s",
		    filename_tmp, filename, strerror(errno));
	}

	if (buflen <= 0) {
	    fprintf(stderr,"rename_tmp_holding: %s: empty file?\n", filename);
	    amfree(filename);
	    amfree(filename_tmp);
	    return 0;
	}
	parse_file_header(buffer, &file, (size_t)buflen);
	if(complete == 0 ) {
	    if((fd = open(filename, O_RDWR)) == -1) {
		fprintf(stderr, "rename_tmp_holdingX: open of %s failed: %s\n",
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
	    printf("Warning: could not open holding dir %s: %s\n",
		   diskdir, strerror(errno));
	return;
   }

    /* find all directories of the right format  */

    if(verbose)
	printf("Scanning %s...\n", diskdir);
    if ((chdir(diskdir)) == -1) {
	log_add(L_INFO, "%s: could not chdir: %s",
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
	        puts("skipping cruft file, perhaps you should delete it.");
	}
	else if(!is_datestr(workdir->d_name)) {
	    if(verbose && (strcmp(workdir->d_name, "lost+found")!=0) )
	        puts("skipping cruft directory, perhaps you should delete it.");
	}
	else if(rmdir(workdir->d_name) == 0) {
	    if(verbose)
	        puts("deleted empty Amanda directory.");
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
	log_add(L_WARNING, "WARNING: could not create parents of %s: %s",
		diskdir, strerror(errno));
	success = 0;
    }
    else if (mkdir(diskdir, 0770) != 0 && errno != EEXIST) {
	log_add(L_WARNING, "WARNING: could not create %s: %s",
		diskdir, strerror(errno));
	success = 0;
    }
    else if (stat(diskdir, &stat_hdp) == -1) {
	log_add(L_WARNING, "WARNING: could not stat %s: %s",
		diskdir, strerror(errno));
	success = 0;
    }
    else {
	if (!S_ISDIR((stat_hdp.st_mode))) {
	    log_add(L_WARNING, "WARNING: %s is not a directory",
		    diskdir);
	    success = 0;
	}
	else if (access(diskdir,W_OK) != 0) {
	    log_add(L_WARNING, "WARNING: directory %s is not writable",
		    diskdir);
	    success = 0;
	}
    }
    return success;
}
