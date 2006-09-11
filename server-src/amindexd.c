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
 * $Id: amindexd.c,v 1.106 2006/07/25 18:27:57 martinea Exp $
 *
 * This is the server daemon part of the index client/server system.
 * It is assumed that this is launched from inetd instead of being
 * started as a daemon because it is not often used
 */

/*
** Notes:
** - this server will do very little until it knows what Amanda config it
**   is to use.  Setting the config has the side effect of changing to the
**   index directory.
** - XXX - I'm pretty sure the config directory name should have '/'s stripped
**   from it.  It is given to us by an unknown person via the network.
*/

#include "amanda.h"
#include "conffile.h"
#include "diskfile.h"
#include "arglist.h"
#include "clock.h"
#include "version.h"
#include "amindex.h"
#include "disk_history.h"
#include "list_dir.h"
#include "logfile.h"
#include "token.h"
#include "find.h"
#include "tapefile.h"
#include "util.h"
#include "amandad.h"

#include <grp.h>

typedef struct REMOVE_ITEM
{
    char *filename;
    struct REMOVE_ITEM *next;
} REMOVE_ITEM;

/* state */
static int from_amandad;
static char local_hostname[MAX_HOSTNAME_LENGTH+1];	/* me! */
static char *remote_hostname = NULL;			/* the client */
static char *dump_hostname = NULL;		/* machine we are restoring */
static char *disk_name;				/* disk we are restoring */
char *qdisk_name = NULL;			/* disk we are restoring */
static char *target_date = NULL;
static disklist_t disk_list;			/* all disks in cur config */
static find_result_t *output_find = NULL;
static g_option_t *g_options = NULL;
static int cmdfdin, cmdfdout;

static int amindexd_debug = 0;

static REMOVE_ITEM *uncompress_remove = NULL;
					/* uncompressed files to remove */

static am_feature_t *our_features = NULL;
static am_feature_t *their_features = NULL;

static REMOVE_ITEM *remove_files(REMOVE_ITEM *);
static char *uncompress_file(char *, char **);
static int process_ls_dump(char *, DUMP_ITEM *, int, char **);

static size_t reply_buffer_size = 1;
static char *reply_buffer = NULL;
static char *amandad_auth = NULL;

static void reply(int, char *, ...)
    __attribute__ ((format (printf, 2, 3)));
static void lreply(int, char *, ...)
    __attribute__ ((format (printf, 2, 3)));
static void fast_lreply(int, char *, ...)
    __attribute__ ((format (printf, 2, 3)));
static int is_dump_host_valid(char *);
static int is_disk_valid(char *);
static int is_config_valid(char *);
static int build_disk_table(void);
static int disk_history_list(void);
static int is_dir_valid_opaque(char *);
static int opaque_ls(char *, int);
static void opaque_ls_one (DIR_ITEM *dir_item, am_feature_e marshall_feature,
			     int recursive);
static int tapedev_is(void);
static int are_dumps_compressed(void);
static char *amindexd_nicedate (char *datestamp);
static int cmp_date (const char *date1, const char *date2);
int main(int, char **);

static REMOVE_ITEM *
remove_files(
    REMOVE_ITEM *remove)
{
    REMOVE_ITEM *prev;

    while(remove) {
	dbprintf(("%s: removing index file: %s\n",
		  debug_prefix_time(NULL), remove->filename));
	unlink(remove->filename);
	amfree(remove->filename);
	prev = remove;
	remove = remove->next;
	amfree(prev);
    }
    return remove;
}

static char *
uncompress_file(
    char *	filename_gz,
    char **	emsg)
{
    char *cmd = NULL;
    char *filename = NULL;
    struct stat stat_filename;
    int result;
    size_t len;

    filename = stralloc(filename_gz);
    len = strlen(filename);
    if(len > 3 && strcmp(&(filename[len-3]),".gz")==0) {
	filename[len-3]='\0';
    } else if(len > 2 && strcmp(&(filename[len-2]),".Z")==0) {
	filename[len-2]='\0';
    }

    /* uncompress the file */
    result=stat(filename,&stat_filename);
    if(result==-1 && errno==ENOENT) {		/* file does not exist */
	struct stat statbuf;
	REMOVE_ITEM *remove_file;

	/*
	 * Check that compressed file exists manually.
	 */
	if (stat(filename_gz, &statbuf) < 0) {
	    *emsg = newvstralloc(*emsg, "Compressed file '",
				filename_gz,
				"' is inaccessable: ",
				strerror(errno),
				NULL);
	    dbprintf(("%s\n",*emsg));
	    amfree(filename);
	    return NULL;
 	}

	cmd = vstralloc(UNCOMPRESS_PATH,
#ifdef UNCOMPRESS_OPT
			" ", UNCOMPRESS_OPT,
#endif
			" \'", filename_gz, "\'",
			" 2>/dev/null",
			" | (LC_ALL=C; export LC_ALL ; sort) ",
			" > ", "\'", filename, "\'",
			NULL);
	dbprintf(("%s: uncompress command: %s\n",
		  debug_prefix_time(NULL), cmd));
	if (system(cmd) != 0) {
	    *emsg = newvstralloc(*emsg, "\"", cmd, "\" failed", NULL);
	    unlink(filename);
	    errno = -1;
	    amfree(filename);
	    amfree(cmd);
	    return NULL;
	}

	/* add at beginning */
	remove_file = (REMOVE_ITEM *)alloc(SIZEOF(REMOVE_ITEM));
	remove_file->filename = stralloc(filename);
	remove_file->next = uncompress_remove;
	uncompress_remove = remove_file;
    } else if(!S_ISREG((stat_filename.st_mode))) {
	    amfree(*emsg);
	    *emsg = vstralloc("\"", filename, "\" is not a regular file", NULL);
	    errno = -1;
	    amfree(filename);
	    amfree(cmd);
	    return NULL;
    }
    amfree(cmd);
    return filename;
}

/* find all matching entries in a dump listing */
/* return -1 if error */
static int
process_ls_dump(
    char *	dir,
    DUMP_ITEM *	dump_item,
    int		recursive,
    char **	emsg)
{
    char *line = NULL;
    char *old_line = NULL;
    char *filename = NULL;
    char *filename_gz;
    char *dir_slash = NULL;
    FILE *fp;
    char *s;
    int ch;
    size_t len_dir_slash;

    if (strcmp(dir, "/") == 0) {
	dir_slash = stralloc(dir);
    } else {
	dir_slash = stralloc2(dir, "/");
    }

    filename_gz = getindexfname(dump_hostname, disk_name, dump_item->date,
			        dump_item->level);
    if((filename = uncompress_file(filename_gz, emsg)) == NULL) {
	amfree(filename_gz);
	amfree(dir_slash);
	return -1;
    }
    amfree(filename_gz);

    if((fp = fopen(filename,"r"))==0) {
	amfree(*emsg);
	*emsg = stralloc(strerror(errno));
	amfree(dir_slash);
	return -1;
    }

    len_dir_slash=strlen(dir_slash);

    while ((line = agets(fp)) != NULL) {
	if (line[0] != '\0') {
	    if(strncmp(dir_slash, line, len_dir_slash) == 0) {
		if(!recursive) {
		    s = line + len_dir_slash;
		    ch = *s++;
		    while(ch && ch != '/')
			ch = *s++;/* find end of the file name */
		    if(ch == '/') {
			ch = *s++;
		    }
		    s[-1] = '\0';
		}
		if(old_line == NULL || strcmp(line, old_line) != 0) {
		    add_dir_list_item(dump_item, line);
		    amfree(old_line);
		    old_line = line;
		    line = NULL;
		}
	    }
	}
	/*@i@*/ amfree(line);
    }
    afclose(fp);
    /*@i@*/ amfree(old_line);
    amfree(filename);
    amfree(dir_slash);
    return 0;
}

/* send a 1 line reply to the client */
printf_arglist_function1(static void reply, int, n, char *, fmt)
{
    va_list args;
    int len;

    if(!reply_buffer)
	reply_buffer = alloc(reply_buffer_size);

    while(1) {
	arglist_start(args, fmt);
	len = vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = alloc(reply_buffer_size);
    }

    if (printf("%03d %s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(("%s: ! error %d (%s) in printf\n",
		  debug_prefix_time(NULL), errno, strerror(errno)));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }
    if (fflush(stdout) != 0)
    {
	dbprintf(("%s: ! error %d (%s) in fflush\n",
		  debug_prefix_time(NULL), errno, strerror(errno)));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }
    dbprintf(("%s: < %03d %s\n", debug_prefix_time(NULL), n, reply_buffer));
}

/* send one line of a multi-line response */
printf_arglist_function1(static void lreply, int, n, char *, fmt)
{
    va_list args;
    int len;

    if(!reply_buffer)
	reply_buffer = alloc(reply_buffer_size);

    while(1) {
	arglist_start(args, fmt);
	len = vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = alloc(reply_buffer_size);
    }

    if (printf("%03d-%s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(("%s: ! error %d (%s) in printf\n",
		  debug_prefix_time(NULL), errno, strerror(errno)));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }
    if (fflush(stdout) != 0)
    {
	dbprintf(("%s: ! error %d (%s) in fflush\n",
		  debug_prefix_time(NULL), errno, strerror(errno)));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }

    dbprintf(("%s: < %03d-%s\n", debug_prefix_time(NULL), n, reply_buffer));

}

/* send one line of a multi-line response */
printf_arglist_function1(static void fast_lreply, int, n, char *, fmt)
{
    va_list args;
    int len;

    if(!reply_buffer)
	reply_buffer = alloc(reply_buffer_size);

    while(1) {
	arglist_start(args, fmt);
	len = vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = alloc(reply_buffer_size);
    }

    if (printf("%03d-%s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(("%s: ! error %d (%s) in printf\n",
		  debug_prefix_time(NULL), errno, strerror(errno)));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }

    dbprintf(("%s: < %03d-%s\n", debug_prefix_time(NULL), n, reply_buffer));
}

/* see if hostname is valid */
/* valid is defined to be that there is an index directory for it */
/* also do a security check on the requested dump hostname */
/* to restrict access to index records if required */
/* return -1 if not okay */
static int
is_dump_host_valid(
    char *	host)
{
    struct stat dir_stat;
    char *fn;
    am_host_t *ihost;

    if (config_name == NULL) {
	reply(501, "Must set config before setting host.");
	return -1;
    }

#if 0
    /* only let a client restore itself for now unless it is the server */
    if (strcasecmp(remote_hostname, local_hostname) == 0)
	return 0;
    if (strcasecmp(remote_hostname, host) != 0)
    {
	reply(501,
	      "You don't have the necessary permissions to set dump host to %s.",
	      buf1);
	return -1;
    }
#endif

    /* check that the config actually handles that host */
    ihost = lookup_host(host);
    if(ihost == NULL) {
	reply(501, "Host %s is not in your disklist.", host);
	return -1;
    }

    /* assume an index dir already */
    fn = getindexfname(host, NULL, NULL, 0);
    if (stat (fn, &dir_stat) != 0 || !S_ISDIR(dir_stat.st_mode)) {
	reply(501, "No index records for host: %s. Have you enabled indexing?", host);
	amfree(fn);
	return -1;
    }

    amfree(fn);
    return 0;
}


static int
is_disk_valid(
    char *disk)
{
    char *fn;
    struct stat dir_stat;
    disk_t *idisk;
    char *qdisk;

    if (config_name == NULL) {
	reply(501, "Must set config,host before setting disk.");
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(501, "Must set host before setting disk.");
	return -1;
    }

    /* check that the config actually handles that disk */
    idisk = lookup_disk(dump_hostname, disk);
    if(idisk == NULL) {
	qdisk = quote_string(disk);
	reply(501, "Disk %s:%s is not in your disklist.", dump_hostname, qdisk);
	amfree(qdisk);
	return -1;
    }

    /* assume an index dir already */
    fn = getindexfname(dump_hostname, disk, NULL, 0);
    if (stat (fn, &dir_stat) != 0 || !S_ISDIR(dir_stat.st_mode)) {
	qdisk = quote_string(disk);
	reply(501, "No index records for disk: %s. Invalid?", qdisk);
	amfree(fn);
	amfree(qdisk);
	return -1;
    }

    amfree(fn);
    return 0;
}


static int
is_config_valid(
    char *	config)
{
    char *conffile;
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_indexdir;
    struct stat dir_stat;

    /* check that the config actually exists */
    if (config == NULL) {
	reply(501, "Must set config first.");
	return -1;
    }

    /* read conffile */
    conffile = stralloc2(config_dir, CONFFILE_NAME);
    if (read_conffile(conffile)) {
	reply(501, "Could not read config file %s!", conffile);
	amfree(conffile);
	return -1;
    }
    amfree(conffile);

    conf_diskfile = getconf_str(CNF_DISKFILE);
    if (*conf_diskfile == '/') {
	conf_diskfile = stralloc(conf_diskfile);
    } else {
	conf_diskfile = stralloc2(config_dir, conf_diskfile);
    }
    if (read_diskfile(conf_diskfile, &disk_list) < 0) {
	reply(501, "Could not read disk file %s!", conf_diskfile);
	amfree(conf_diskfile);
	return -1;
    }
    amfree(conf_diskfile);

    conf_tapelist = getconf_str(CNF_TAPELIST);
    if (*conf_tapelist == '/') {
	conf_tapelist = stralloc(conf_tapelist);
    } else {
	conf_tapelist = stralloc2(config_dir, conf_tapelist);
    }
    if(read_tapelist(conf_tapelist)) {
	reply(501, "Could not read tapelist file %s!", conf_tapelist);
	amfree(conf_tapelist);
	return -1;
    }
    amfree(conf_tapelist);

    dbrename(config, DBG_SUBDIR_SERVER);

    output_find = find_dump(1, &disk_list);
    sort_find_result("DLKHpB", &output_find);

    conf_indexdir = getconf_str(CNF_INDEXDIR);
    if(*conf_indexdir == '/') {
	conf_indexdir = stralloc(conf_indexdir);
    } else {
	conf_indexdir = stralloc2(config_dir, conf_indexdir);
    }
    if (stat (conf_indexdir, &dir_stat) != 0 || !S_ISDIR(dir_stat.st_mode)) {
	reply(501, "Index directory %s does not exist", conf_indexdir);
	amfree(conf_indexdir);
	return -1;
    }
    amfree(conf_indexdir);

    return 0;
}


static int
build_disk_table(void)
{
    char *date;
    char *last_timestamp;
    off_t last_filenum;
    int last_level;
    int last_partnum;
    find_result_t *find_output;

    if (config_name == NULL) {
	reply(590, "Must set config,host,disk before building disk table");
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(590, "Must set host,disk before building disk table");
	return -1;
    }
    else if (disk_name == NULL) {
	reply(590, "Must set disk before building disk table");
	return -1;
    }

    clear_list();
    last_timestamp = NULL;
    last_filenum = (off_t)-1;
    last_level = -1;
    last_partnum = -1;
    for(find_output = output_find;
	find_output != NULL; 
	find_output = find_output->next) {
	if(strcasecmp(dump_hostname, find_output->hostname) == 0 &&
	   strcmp(disk_name    , find_output->diskname) == 0 &&
	   strcmp("OK"         , find_output->status)   == 0) {
	    int partnum = -1;
	    if(strcmp("--", find_output->partnum)){
		partnum = atoi(find_output->partnum);
	    }
	    /*
	     * The sort order puts holding disk entries first.  We want to
	     * use them if at all possible, so ignore any other entries
	     * for the same datestamp after we see a holding disk entry
	     * (as indicated by a filenum of zero).
	     */
	    if(last_timestamp &&
	       strcmp(find_output->timestamp, last_timestamp) == 0 &&
	       find_output->level == last_level && 
	       partnum == last_partnum && last_filenum == 0) {
		continue;
	    }
	    last_timestamp = find_output->timestamp;
	    last_filenum = find_output->filenum;
	    last_level = find_output->level;
	    last_partnum = partnum;
	    date = amindexd_nicedate(find_output->timestamp);
	    add_dump(date, find_output->level, find_output->label, 
		     find_output->filenum, partnum);
	    dbprintf(("%s: - %s %d %s " OFF_T_FMT " %d\n",
		     debug_prefix_time(NULL), date, find_output->level, 
		     find_output->label,
		     (OFF_T_FMT_TYPE)find_output->filenum,
		     partnum));
	}
    }
    return 0;
}


static int
disk_history_list(void)
{
    DUMP_ITEM *item;
    char date[20];

    if (config_name == NULL) {
	reply(502, "Must set config,host,disk before listing history");
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(502, "Must set host,disk before listing history");
	return -1;
    }
    else if (disk_name == NULL) {
	reply(502, "Must set disk before listing history");
	return -1;
    }

    lreply(200, " Dump history for config \"%s\" host \"%s\" disk %s",
	  config_name, dump_hostname, qdisk_name);

    for (item=first_dump(); item!=NULL; item=next_dump(item)){
        char *tapelist_str = marshal_tapelist(item->tapes, 1);

	strncpy(date, item->date, 20);
	date[19] = '\0';
	if(!am_has_feature(their_features,fe_amrecover_timestamp))
	    date[10] = '\0';

	if(am_has_feature(their_features, fe_amindexd_marshall_in_DHST)){
	    lreply(201, " %s %d %s", date, item->level, tapelist_str);
	}
	else{
	    lreply(201, " %s %d %s " OFF_T_FMT, date, item->level,
		tapelist_str, (OFF_T_FMT_TYPE)item->file);
	}
	amfree(tapelist_str);
    }

    reply(200, "Dump history for config \"%s\" host \"%s\" disk %s",
	  config_name, dump_hostname, qdisk_name);

    return 0;
}


/*
 * is the directory dir backed up - dir assumed complete relative to
 * disk mount point
 */
/* opaque version of command */
static int
is_dir_valid_opaque(
    char *dir)
{
    DUMP_ITEM *item;
    char *line = NULL;
    FILE *fp;
    int last_level;
    char *ldir = NULL;
    char *filename_gz = NULL;
    char *filename = NULL;
    size_t ldir_len;
    static char *emsg = NULL;

    if (config_name == NULL || dump_hostname == NULL || disk_name == NULL) {
	reply(502, "Must set config,host,disk before asking about directories");
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(502, "Must set host,disk before asking about directories");
	return -1;
    }
    else if (disk_name == NULL) {
	reply(502, "Must set disk before asking about directories");
	return -1;
    }
    else if (target_date == NULL) {
	reply(502, "Must set date before asking about directories");
	return -1;
    }

    /* scan through till we find first dump on or before date */
    for (item=first_dump(); item!=NULL; item=next_dump(item))
	if (cmp_date(item->date, target_date) <= 0)
	    break;

    if (item == NULL)
    {
	/* no dump for given date */
	reply(500, "No dumps available on or before date \"%s\"", target_date);
	return -1;
    }

    if(strcmp(dir, "/") == 0) {
	ldir = stralloc(dir);
    } else {
	ldir = stralloc2(dir, "/");
    }
    ldir_len = strlen(ldir);

    /* go back till we hit a level 0 dump */
    do
    {
	amfree(filename);
	filename_gz = getindexfname(dump_hostname, disk_name,
				    item->date, item->level);
	if((filename = uncompress_file(filename_gz, &emsg)) == NULL) {
	    reply(599, "System error %s", emsg);
	    amfree(filename_gz);
	    amfree(emsg);
	    amfree(ldir);
	    return -1;
	}
	amfree(filename_gz);
	dbprintf(("%s: f %s\n", debug_prefix_time(NULL), filename));
	if ((fp = fopen(filename, "r")) == NULL) {
	    reply(599, "System error %s", strerror(errno));
	    amfree(filename);
	    amfree(ldir);
	    return -1;
	}
	for(; (line = agets(fp)) != NULL; free(line)) {
	    if (line[0] == '\0')
		continue;
	    if (strncmp(line, ldir, ldir_len) != 0) {
		continue;			/* not found yet */
	    }
	    amfree(filename);
	    amfree(ldir);
	    amfree(line);
	    afclose(fp);
	    return 0;
	}
	afclose(fp);

	last_level = item->level;
	do
	{
	    item=next_dump(item);
	} while ((item != NULL) && (item->level >= last_level));
    } while (item != NULL);

    amfree(filename);
    amfree(ldir);
    reply(500, "\"%s\" is an invalid directory", dir);
    return -1;
}

static int
opaque_ls(
    char *	dir,
    int		recursive)
{
    DUMP_ITEM *dump_item;
    DIR_ITEM *dir_item;
    int level, last_level;
    static char *emsg = NULL;
    am_feature_e marshall_feature;

    if (recursive) {
        marshall_feature = fe_amindexd_marshall_in_ORLD;
    } else {
        marshall_feature = fe_amindexd_marshall_in_OLSD;
    }

    clear_dir_list();

    if (config_name == NULL) {
	reply(502, "Must set config,host,disk before listing a directory");
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(502, "Must set host,disk before listing a directory");
	return -1;
    }
    else if (disk_name == NULL) {
	reply(502, "Must set disk before listing a directory");
	return -1;
    }
    else if (target_date == NULL) {
	reply(502, "Must set date before listing a directory");
	return -1;
    }

    /* scan through till we find first dump on or before date */
    for (dump_item=first_dump(); dump_item!=NULL; dump_item=next_dump(dump_item))
	if (cmp_date(dump_item->date, target_date) <= 0)
	    break;

    if (dump_item == NULL)
    {
	/* no dump for given date */
	reply(500, "No dumps available on or before date \"%s\"", target_date);
	return -1;
    }

    /* get data from that dump */
    if (process_ls_dump(dir, dump_item, recursive, &emsg) == -1) {
	reply(599, "System error %s", emsg);
	amfree(emsg);
	return -1;
    }

    /* go back processing higher level dumps till we hit a level 0 dump */
    last_level = dump_item->level;
    while ((last_level != 0) && ((dump_item=next_dump(dump_item)) != NULL))
    {
	if (dump_item->level < last_level)
	{
	    last_level = dump_item->level;
	    if (process_ls_dump(dir, dump_item, recursive, &emsg) == -1) {
		reply(599, "System error %s", emsg);
		amfree(emsg);
		return -1;
	    }
	}
    }

    /* return the information to the caller */
    lreply(200, " Opaque list of %s", dir);
    for(level=0; level<=9; level++) {
	for (dir_item = get_dir_list(); dir_item != NULL; 
	     dir_item = dir_item->next) {

	    if(dir_item->dump->level == level) {
		if (!am_has_feature(their_features, marshall_feature) &&
	            (num_entries(dir_item->dump->tapes) > 1 ||
	            dir_item->dump->tapes->numfiles > 1)) {
	            fast_lreply(501, " ERROR: Split dumps not supported"
				" with old version of amrecover.");
		    break;
		}
		else {
		    opaque_ls_one(dir_item, marshall_feature, recursive);
		}
	    }
	}
    }
    reply(200, " Opaque list of %s", dir);

    clear_dir_list();
    return 0;
}

void opaque_ls_one(
    DIR_ITEM *	 dir_item,
    am_feature_e marshall_feature,
    int		 recursive)
{
   char date[20];
   char *tapelist_str;
    char *qpath;

    if (am_has_feature(their_features, marshall_feature)) {
	tapelist_str = marshal_tapelist(dir_item->dump->tapes, 1);
    } else {
	tapelist_str = dir_item->dump->tapes->label;
    }

    strncpy(date, dir_item->dump->date, 20);
    date[19] = '\0';
    if(!am_has_feature(their_features,fe_amrecover_timestamp))
	date[10] = '\0';

    qpath = quote_string(dir_item->path);
    if((!recursive && am_has_feature(their_features,
				     fe_amindexd_fileno_in_OLSD)) ||
       (recursive && am_has_feature(their_features,
				    fe_amindexd_fileno_in_ORLD))) {
	fast_lreply(201, " %s %d %s " OFF_T_FMT " %s",
		    date,
		    dir_item->dump->level,
		    tapelist_str,
		    (OFF_T_FMT_TYPE)dir_item->dump->file,
		    qpath);
    }
    else {

	fast_lreply(201, " %s %d %s %s",
		    date, dir_item->dump->level,
		    tapelist_str, qpath);
    }
    amfree(qpath);
    if(am_has_feature(their_features, marshall_feature)) {
	amfree(tapelist_str);
    }
}

/*
 * returns the value of changer or tapedev from the amanda.conf file if set,
 * otherwise reports an error
 */

static int
tapedev_is(void)
{
    char *result;

    /* check state okay to do this */
    if (config_name == NULL) {
	reply(501, "Must set config before asking about tapedev.");
	return -1;
    }

    /* use amrecover_changer if possible */
    if ((result = getconf_str(CNF_AMRECOVER_CHANGER)) != NULL  &&
        *result != '\0') {
	dbprintf(("%s: tapedev_is amrecover_changer: %s\n",
                  debug_prefix_time(NULL), result));
	reply(200, result);
	return 0;
    }

    /* use changer if possible */
    if ((result = getconf_str(CNF_TPCHANGER)) != NULL  &&  *result != '\0') {
	dbprintf(("%s: tapedev_is tpchanger: %s\n",
                  debug_prefix_time(NULL), result));
	reply(200, result);
	return 0;
    }

    /* get tapedev value */
    if ((result = getconf_str(CNF_TAPEDEV)) != NULL  &&  *result != '\0') {
	dbprintf(("%s: tapedev_is tapedev: %s\n",
                  debug_prefix_time(NULL), result));
	reply(200, result);
	return 0;
    }

    dbprintf(("%s: No tapedev or changer in config site.\n",
              debug_prefix_time(NULL)));
    reply(501, "Tapedev or changer not set in config file.");
    return -1;
}


/* returns YES if dumps for disk are compressed, NO if not */
static int
are_dumps_compressed(void)
{
    disk_t *diskp;

    /* check state okay to do this */
    if (config_name == NULL) {
	reply(501, "Must set config,host,disk name before asking about dumps.");
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(501, "Must set host,disk name before asking about dumps.");
	return -1;
    }
    else if (disk_name == NULL) {
	reply(501, "Must set disk name before asking about dumps.");
	return -1;
    }

    /* now go through the list of disks and find which have indexes */
    for (diskp = disk_list.head; diskp != NULL; diskp = diskp->next) {
	if ((strcasecmp(diskp->host->hostname, dump_hostname) == 0)
		&& (strcmp(diskp->name, disk_name) == 0)) {
	    break;
	}
    }

    if (diskp == NULL) {
	reply(501, "Couldn't find host/disk in disk file.");
	return -1;
    }

    /* send data to caller */
    if (diskp->compress == COMP_NONE)
	reply(200, "NO");
    else
	reply(200, "YES");

    return 0;
}

int
main(
    int		argc,
    char **	argv)
{
    char *line = NULL, *part = NULL;
    char *s, *fp;
    int ch;
    char *cmd_undo, cmd_undo_ch;
    socklen_t socklen;
    struct sockaddr_in his_addr;
    struct hostent *his_name;
    char *arg = NULL;
    char *cmd;
    size_t len;
    int user_validated = 0;
    char *errstr = NULL;
    char *pgm = "amindexd";		/* in case argv[0] is not set */

    safe_fd(DATA_FD_OFFSET, 2);
    safe_cd();

    /*
     * When called via inetd, it is not uncommon to forget to put the
     * argv[0] value on the config line.  On some systems (e.g. Solaris)
     * this causes argv and/or argv[0] to be NULL, so we have to be
     * careful getting our name.
     */
    if (argc >= 1 && argv != NULL && argv[0] != NULL) {
	if((pgm = strrchr(argv[0], '/')) != NULL) {
	    pgm++;
	} else {
	    pgm = argv[0];
	}
    }

    set_pname(pgm);

    /* Don't die when child closes pipe */
    signal(SIGPIPE, SIG_IGN);

#ifdef FORCE_USERID

    /* we'd rather not run as root */

    if(geteuid() == 0) {
	if(client_uid == (uid_t) -1) {
	    error("error [cannot find user %s in passwd file]\n", CLIENT_LOGIN);
	    /*NOTREACHED*/
	}

	/*@ignore@*/
	initgroups(CLIENT_LOGIN, client_gid);
	/*@end@*/
	setgid(client_gid);
	setuid(client_uid);
    }

#endif	/* FORCE_USERID */

    dbopen(DBG_SUBDIR_SERVER);
    dbprintf(("%s: version %s\n", get_pname(), version()));

    if(argv == NULL) {
	error("argv == NULL\n");
    }

    if (! (argc >= 1 && argv[0] != NULL)) {
	dbprintf(("%s: WARNING: argv[0] not defined: check inetd.conf\n",
		  debug_prefix_time(NULL)));
    }

    {
	int db_fd = dbfd();
	if(db_fd != -1) {
	    dup2(db_fd, 2);
	}
    }

    /* initialize */

    argc--;
    argv++;

    if(argc > 0 && strcmp(*argv, "-t") == 0) {
	amindexd_debug = 1;
	argc--;
	argv++;
    }

    if(argc > 0 && strcmp(*argv, "amandad") == 0) {
	from_amandad = 1;
	argc--;
	argv++;
	if(argc > 0) {
	    amandad_auth = *argv;
	    argc--;
	    argv++;
	}
    }
    else {
	from_amandad = 0;
	safe_fd(-1, 0);
    }

    if (argc > 0) {
	config_name = stralloc(*argv);
	config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
	argc--;
	argv++;
    }

    if(gethostname(local_hostname, SIZEOF(local_hostname)-1) == -1) {
	error("gethostname: %s", strerror(errno));
	/*NOTREACHED*/
    }
    local_hostname[SIZEOF(local_hostname)-1] = '\0';

    /* now trim domain off name */
    s = local_hostname;
    ch = *s++;
    while(ch && ch != '.') ch = *s++;
    s[-1] = '\0';


    if(from_amandad == 0) {
	if(amindexd_debug) {
	    /*
	     * Fake the remote address as the local address enough to get
	     * through the security check.
	     */
	    his_name = gethostbyname(local_hostname);
	    if(his_name == NULL) {
		error("gethostbyname(%s) failed\n", local_hostname);
                /*NOTREACHED*/
	    }
	    assert((sa_family_t)his_name->h_addrtype == (sa_family_t)AF_INET);
	    his_addr.sin_family = (sa_family_t)his_name->h_addrtype;
	    his_addr.sin_port = (in_port_t)htons(0);
	    memcpy((void *)&his_addr.sin_addr.s_addr,
		   (void *)his_name->h_addr_list[0], 
                   (size_t)his_name->h_length);
	} else {
	    /* who are we talking to? */
	    socklen = sizeof (his_addr);
	    if (getpeername(0, (struct sockaddr *)&his_addr, &socklen) == -1)
		error("getpeername: %s", strerror(errno));
	}
	if ((his_addr.sin_family != (sa_family_t)AF_INET)
		|| (ntohs(his_addr.sin_port) == 20)) {
	    error("connection rejected from %s family %d port %d",
		  inet_ntoa(his_addr.sin_addr), his_addr.sin_family,
		  htons(his_addr.sin_port));
	    /*NOTREACHED*/
	}
	if ((his_name = gethostbyaddr((char *)&(his_addr.sin_addr),
				      sizeof(his_addr.sin_addr),
				      AF_INET)) == NULL) {
	    error("gethostbyaddr(%s): hostname lookup failed",
		  inet_ntoa(his_addr.sin_addr));
	    /*NOTREACHED*/
	}
	fp = s = stralloc(his_name->h_name);
	ch = *s++;
	while(ch && ch != '.') ch = *s++;
	s[-1] = '\0';
	remote_hostname = newstralloc(remote_hostname, fp);
	s[-1] = (char)ch;
	amfree(fp);
    }
    else {
	cmdfdout  = DATA_FD_OFFSET + 0;
	cmdfdin   = DATA_FD_OFFSET + 1;

	/* read the REQ packet */
	for(; (line = agets(stdin)) != NULL; free(line)) {
#define sc "OPTIONS "
	    if(strncmp(line, sc, sizeof(sc)-1) == 0) {
#undef sc
		g_options = parse_g_options(line+8, 1);
		if(!g_options->hostname) {
		    g_options->hostname = alloc(MAX_HOSTNAME_LENGTH+1);
		    gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		    g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
		}
	    }
	}
	amfree(line);

	if(amandad_auth && g_options->auth) {
	    if(strcasecmp(amandad_auth, g_options->auth) != 0) {
		printf("ERROR recover program ask for auth=%s while amindexd is configured for '%s'\n",
		       g_options->auth, amandad_auth);
		error("amindexd: ERROR recover program ask for auth=%s while amindexd is configured for '%s'",
		      g_options->auth, amandad_auth);
		/*NOTREACHED*/
	    }
	}
	/* send the REP packet */
	printf("CONNECT MESG %d\n", DATA_FD_OFFSET);
	printf("\n");
	fflush(stdin);
	fflush(stdout);
	if ((dup2(cmdfdout, fileno(stdout)) < 0)
		 || (dup2(cmdfdin, fileno(stdin)) < 0)) {
	    error("amandad: Failed to setup stdin or stdout");
	    /*NOTREACHED*/
	}
    }

    /* clear these so we can detect when the have not been set by the client */
    amfree(dump_hostname);
    amfree(qdisk_name);
    amfree(disk_name);
    amfree(target_date);

    our_features = am_init_feature_set();
    their_features = am_set_default_feature_set();

    if (config_name != NULL && is_config_valid(config_name) != -1) {
	return 1;
    }

    reply(220, "%s AMANDA index server (%s) ready.", local_hostname,
	  version());

    user_validated = from_amandad;

    /* a real simple parser since there are only a few commands */
    while (1)
    {
	/* get a line from the client */
	while(1) {
	    if((part = agets(stdin)) == NULL) {
		if(errno != 0) {
		    dbprintf(("%s: ? read error: %s\n",
			      debug_prefix_time(NULL), strerror(errno)));
		} else {
		    dbprintf(("%s: ? unexpected EOF\n",
			      debug_prefix_time(NULL)));
		}
		if(line) {
		    dbprintf(("%s: ? unprocessed input:\n",
			      debug_prefix_time(NULL)));
		    dbprintf(("-----\n"));
		    dbprintf(("? %s\n", line));
		    dbprintf(("-----\n"));
		}
		amfree(line);
		amfree(part);
		uncompress_remove = remove_files(uncompress_remove);
		dbclose();
		return 1;		/* they hung up? */
	    }
	    strappend(line, part);	/* Macro: line can be null */
	    amfree(part);

	    if(amindexd_debug) {
		break;			/* we have a whole line */
	    }
	    if((len = strlen(line)) > 0 && line[len-1] == '\r') {
		line[len-1] = '\0';	/* zap the '\r' */
		break;
	    }
	    /*
	     * Hmmm.  We got a "line" from agets(), which means it saw
	     * a '\n' (or EOF, etc), but there was not a '\r' before it.
	     * Put a '\n' back in the buffer and loop for more.
	     */
	    strappend(line, "\n");
	}

	dbprintf(("%s: > %s\n", debug_prefix_time(NULL), line));

	if (arg != NULL)
	    amfree(arg);
	s = line;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    reply(500, "Command not recognised/incorrect: %s", line);
	    amfree(line);
	    continue;
	}
	cmd = s - 1;

	skip_non_whitespace(s, ch);
	cmd_undo = s-1;				/* for error message */
	cmd_undo_ch = *cmd_undo;
	*cmd_undo = '\0';
	if (ch) {
	    skip_whitespace(s, ch);		/* find the argument */
	    if (ch) {
		arg = s-1;
		skip_quoted_string(s, ch);
		arg = unquote_string(arg);
	    }
	}

	amfree(errstr);
	if (!user_validated && strcmp(cmd, "SECURITY") == 0 && arg) {
	    user_validated = check_security(&his_addr, arg, 0, &errstr);
	    if(user_validated) {
		reply(200, "Access OK");
		amfree(line);
		continue;
	    }
	}
	if (!user_validated) {  /* don't tell client the reason, just log it to debug log */
	    reply(500, "Access not allowed");
	    if (errstr) {   
		dbprintf(("%s: %s\n", debug_prefix_time(NULL), errstr));
	    }
	    break;
	}

	if (strcmp(cmd, "QUIT") == 0) {
	    amfree(line);
	    break;
	} else if (strcmp(cmd, "HOST") == 0 && arg) {
	    /* set host we are restoring */
	    s[-1] = '\0';
	    if (is_dump_host_valid(arg) != -1)
	    {
		dump_hostname = newstralloc(dump_hostname, arg);
		reply(200, "Dump host set to %s.", dump_hostname);
		amfree(qdisk_name);		/* invalidate any value */
		amfree(disk_name);		/* invalidate any value */
	    }
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "LISTHOST") == 0) {
	    disk_t *disk, 
                   *diskdup;
	    int nbhost = 0,
                found = 0;
	    s[-1] = '\0';
	    if (config_name == NULL) {
		reply(501, "Must set config before listhost");
	    }
	    else {
		lreply(200, " List hosts for config %s", config_name);
		for (disk = disk_list.head; disk!=NULL; disk = disk->next) {
                    found = 0;
		    for (diskdup = disk_list.head; diskdup!=disk; diskdup = diskdup->next) {
		        if(strcmp(diskdup->host->hostname, disk->host->hostname) == 0) {
                          found = 1;
                          break;
		        }
                    }
                    if(!found){
	                fast_lreply(201, " %s", disk->host->hostname);
                        nbhost++;
                    }
		}
		if(nbhost > 0) {
		    reply(200, " List hosts for config %s", config_name);
		}
		else {
		    reply(200, "No hosts for config %s", config_name);
		}
	    }
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "DISK") == 0 && arg) {
	    s[-1] = '\0';
	    if (is_disk_valid(arg) != -1) {
		disk_name = newstralloc(disk_name, arg);
		qdisk_name = quote_string(disk_name);
		if (build_disk_table() != -1) {
		    reply(200, "Disk set to %s.", qdisk_name);
		}
	    }
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "LISTDISK") == 0) {
	    char *qname;
	    disk_t *disk;
	    int nbdisk = 0;
	    s[-1] = '\0';
	    if (config_name == NULL) {
		reply(501, "Must set config, host before listdisk");
	    }
	    else if (dump_hostname == NULL) {
		reply(501, "Must set host before listdisk");
	    }
	    else if(arg) {
		lreply(200, " List of disk for device %s on host %s", arg,
		       dump_hostname);
		for (disk = disk_list.head; disk!=NULL; disk = disk->next) {

		    if (strcmp(disk->host->hostname, dump_hostname) == 0 &&
		      ((disk->device && strcmp(disk->device, arg) == 0) ||
		      (!disk->device && strcmp(disk->name, arg) == 0))) {
			qname = quote_string(disk->name);
			fast_lreply(201, " %s", qname);
			amfree(qname);
			nbdisk++;
		    }
		}
		if(nbdisk > 0) {
		    reply(200, "List of disk for device %s on host %s", arg,
			  dump_hostname);
		}
		else {
		    reply(200, "No disk for device %s on host %s", arg,
			  dump_hostname);
		}
	    }
	    else {
		lreply(200, " List of disk for host %s", dump_hostname);
		for (disk = disk_list.head; disk!=NULL; disk = disk->next) {
		    if(strcmp(disk->host->hostname, dump_hostname) == 0) {
			qname = quote_string(disk->name);
			fast_lreply(201, " %s", qname);
			amfree(qname);
			nbdisk++;
		    }
		}
		if(nbdisk > 0) {
		    reply(200, "List of disk for host %s", dump_hostname);
		}
		else {
		    reply(200, "No disk for host %s", dump_hostname);
		}
	    }
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "SCNF") == 0 && arg) {
	    s[-1] = '\0';
	    amfree(config_name);
	    amfree(config_dir);
	    config_name = newstralloc(config_name, arg);
	    config_dir = vstralloc(CONFIG_DIR, "/", config_name, "/", NULL);
	    if (is_config_valid(arg) != -1) {
		amfree(dump_hostname);		/* invalidate any value */
		amfree(qdisk_name);		/* invalidate any value */
		amfree(disk_name);		/* invalidate any value */
		reply(200, "Config set to %s.", config_name);
	    } else {
		amfree(config_name);
		amfree(config_dir);
	    }
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "FEATURES") == 0 && arg) {
	    char *our_feature_string = NULL;
	    char *their_feature_string = NULL;
	    s[-1] = '\0';
	    am_release_feature_set(our_features);
	    am_release_feature_set(their_features);
	    our_features = am_init_feature_set();
	    our_feature_string = am_feature_to_string(our_features);
	    their_feature_string = newstralloc(their_feature_string, arg);
	    their_features = am_string_to_feature(their_feature_string);
	    reply(200, "FEATURES %s", our_feature_string);
	    amfree(our_feature_string);
	    amfree(their_feature_string);
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "DATE") == 0 && arg) {
	    s[-1] = '\0';
	    target_date = newstralloc(target_date, arg);
	    reply(200, "Working date set to %s.", target_date);
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "DHST") == 0) {
	    (void)disk_history_list();
	} else if (strcmp(cmd, "OISD") == 0 && arg) {
	    if (is_dir_valid_opaque(arg) != -1) {
		reply(200, "\"%s\" is a valid directory", arg);
	    }
	} else if (strcmp(cmd, "OLSD") == 0 && arg) {
	    (void)opaque_ls(arg,0);
	} else if (strcmp(cmd, "ORLD") == 0 && arg) {
	    (void)opaque_ls(arg, 1);
	} else if (strcmp(cmd, "TAPE") == 0) {
	    (void)tapedev_is();
	} else if (strcmp(cmd, "DCMP") == 0) {
	    (void)are_dumps_compressed();
	} else {
	    *cmd_undo = cmd_undo_ch;	/* restore the command line */
	    reply(500, "Command not recognised/incorrect: %s", cmd);
	}
	amfree(line);
    }
    amfree(arg);
    
    uncompress_remove = remove_files(uncompress_remove);
    free_find_result(&output_find);
    reply(200, "Good bye.");
    dbclose();
    return 0;
}

static char *
amindexd_nicedate(
    char *	datestamp)
{
    static char nice[20];
    int year, month, day;
    int hours, minutes, seconds;
    char date[9], atime[7];
    int  numdate, numtime;

    strncpy(date, datestamp, 8);
    date[8] = '\0';
    numdate = atoi(date);
    year  = numdate / 10000;
    month = (numdate / 100) % 100;
    day   = numdate % 100;

    if(strlen(datestamp) <= 8) {
	snprintf(nice, SIZEOF(nice), "%4d-%02d-%02d",
		year, month, day);
    }
    else {
	strncpy(atime, &(datestamp[8]), 6);
	atime[6] = '\0';
	numtime = atoi(atime);
	hours = numtime / 10000;
	minutes = (numtime / 100) % 100;
	seconds = numtime % 100;

	snprintf(nice, SIZEOF(nice), "%4d-%02d-%02d-%02d-%02d-%02d",
		year, month, day, hours, minutes, seconds);
    }

    return nice;
}

static int
cmp_date(
    const char *	date1,
    const char *	date2)
{
    return strncmp(date1, date2, strlen(date2));
}
