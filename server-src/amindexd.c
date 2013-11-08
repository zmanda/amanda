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
#include "clock.h"
#include "match.h"
#include "amindex.h"
#include "disk_history.h"
#include "list_dir.h"
#include "logfile.h"
#include "find.h"
#include "tapefile.h"
#include "amutil.h"
#include "amandad.h"
#include "pipespawn.h"
#include "sockaddr-util.h"
#include "amxml.h"

#include <grp.h>

#define DBG(i, ...) do {		\
	if ((i) <= debug_amindexd) {	\
	    g_debug(__VA_ARGS__);	\
	}				\
} while (0)

typedef struct REMOVE_ITEM
{
    char *filename;
    struct REMOVE_ITEM *next;
} REMOVE_ITEM;

/* state */
static int from_amandad;
static char local_hostname[MAX_HOSTNAME_LENGTH+1];	/* me! */
static char *dump_hostname = NULL;		/* machine we are restoring */
static char *disk_name;				/* disk we are restoring */
static char *qdisk_name = NULL;			/* disk we are restoring */
static char *target_date = NULL;
static char **storage_list = NULL;
static disklist_t disk_list;			/* all disks in cur config */
static find_result_t *output_find = NULL;
static g_option_t *g_options = NULL;
static file_lock *lock_index = NULL;

static int amindexd_debug = 0;

static REMOVE_ITEM *uncompress_remove = NULL;
					/* uncompressed files to remove */
static REMOVE_ITEM *compress_sorted_files = NULL;
					/* compress new sorted files */

static am_feature_t *our_features = NULL;
static am_feature_t *their_features = NULL;

static int get_pid_status(int pid, char *program, GPtrArray **emsg);
static REMOVE_ITEM *remove_files(REMOVE_ITEM *);
static REMOVE_ITEM *compress_files(REMOVE_ITEM *);
static char *uncompress_file(char *, char *, char *, int,
			     char *, GPtrArray **,
			     gboolean need_uncompress, gboolean need_sort);
static int process_ls_dump(char *, DUMP_ITEM *, int, GPtrArray **);

static size_t reply_buffer_size = 1;
static char *reply_buffer = NULL;
static char *amandad_auth = NULL;
static FILE *cmdin;
static FILE *cmdout;

static void reply_ptr_array(int, GPtrArray *);
static void reply(int, char *, ...) G_GNUC_PRINTF(2, 3);
static void lreply(int, char *, ...) G_GNUC_PRINTF(2, 3);
static void fast_lreply(int, char *, ...) G_GNUC_PRINTF(2, 3);
static am_host_t *is_dump_host_valid(char *);
static int is_disk_valid(char *);
static int check_and_load_config(char *);
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
static char *get_index_name(char *dump_hostname, char *hostname,
			    char *diskname, char *timestamps, int level,
			    GPtrArray **emsg);
static int get_index_dir(char *dump_hostname, char *hostname, char *diskname);

int main(int, char **);


static int
get_pid_status(
    int         pid,
    char       *program,
    GPtrArray **emsg)
{
    int       status;
    amwait_t  wait_status;
    char     *msg;
    int       result = 1;

    status = waitpid(pid, &wait_status, 0);
    if (status < 0) {
	msg = g_strdup_printf(
		_("%s (%d) returned negative value: %s"),
		program, pid, strerror(errno));
	dbprintf("%s\n", msg);
	g_ptr_array_add(*emsg, msg);
	result = 0;
    } else {
	if (!WIFEXITED(wait_status)) {
	    msg = g_strdup_printf(
			_("%s exited with signal %d"),
			program, WTERMSIG(wait_status));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    result = 0;
	} else if (WEXITSTATUS(wait_status) != 0) {
	    msg = g_strdup_printf(
			_("%s exited with status %d"),
			program, WEXITSTATUS(wait_status));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    result = 0;
	}
    }
    return result;
}

static REMOVE_ITEM *
remove_files(
    REMOVE_ITEM *remove)
{
    REMOVE_ITEM *prev;

    if (file_lock_locked(lock_index)) {
	while(remove) {
	    dbprintf(_("removing index file: %s\n"), remove->filename);
	    unlink(remove->filename);
	    amfree(remove->filename);
	    prev = remove;
	    remove = remove->next;
	    amfree(prev);
	}
    }
    return remove;
}

static REMOVE_ITEM *
compress_files(
    REMOVE_ITEM *compress)
{
    REMOVE_ITEM *prev;
    pid_t        pid;

    if (file_lock_locked(lock_index)) {
	while(compress) {
	    dbprintf(_("compressing index file: %s\n"), compress->filename);

	    switch (pid = fork()) {
	    case -1:
		error("error: couldn't fork: %s", strerror(errno));
	    case 0:
		execlp(COMPRESS_PATH, COMPRESS_PATH, COMPRESS_BEST_OPT, compress->filename, NULL);
		error("error: couldn't exec %s: %s", COMPRESS_PATH, strerror(errno));
		/*NOTREACHED*/
	    default:
		break;
	    }
	    waitpid(pid, NULL, 0);
	    amfree(compress->filename);
	    prev = compress;
	    compress = compress->next;
	    amfree(prev);
	}
    }
    return compress;
}

/* find all matching entries in a dump listing */
/* return -1 if error */
static int
process_ls_dump(
    char *	dir,
    DUMP_ITEM *	dump_item,
    int		recursive,
    GPtrArray **emsg)
{
    char line[STR_SIZE], old_line[STR_SIZE];
    char *filename = NULL;
    char *dir_slash = NULL;
    FILE *fp;
    char *s;
    int ch;
    size_t len_dir_slash;

    old_line[0] = '\0';
    if (g_str_equal(dir, "/")) {
	dir_slash = g_strdup(dir);
    } else {
	dir_slash = g_strconcat(dir, "/", NULL);
    }

    filename = get_index_name(dump_hostname, dump_item->hostname, disk_name,
			      dump_item->date, dump_item->level, emsg);
    if (filename == NULL) {
	amfree(filename);
	amfree(dir_slash);
	return -1;
    }

    if((fp = fopen(filename,"r"))==0) {
	g_ptr_array_add(*emsg, g_strdup_printf("%s", strerror(errno)));
	amfree(dir_slash);
        amfree(filename);
	return -1;
    }

    len_dir_slash=strlen(dir_slash);

    while (fgets(line, STR_SIZE, fp) != NULL) {
	if (line[0] != '\0') {
	    if(strlen(line) > 0 && line[strlen(line)-1] == '\n')
		line[strlen(line)-1] = '\0';
	    if (g_str_has_prefix(line, dir_slash )) {
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
		if(!g_str_equal(line, old_line)) {
		    add_dir_list_item(dump_item, line);
		    strcpy(old_line, line);
		}
	    }
	}
    }
    afclose(fp);
    amfree(filename);
    amfree(dir_slash);
    return 0;
}

static void
reply_ptr_array(
    int n,
    GPtrArray *emsg)
{
    gpointer *p;

    if (emsg->len == 0)
	return;

    p = emsg->pdata;
    while (p != emsg->pdata + emsg->len -1) {
	fast_lreply(n, "%s", (char *)*p);
	p++;
    }
    reply(n, "%s", (char *)*p);
}

/* send a 1 line reply to the client */
static void reply(int n, char *fmt, ...)
{
    va_list args;
    int len;

    if(!reply_buffer)
	reply_buffer = g_malloc(reply_buffer_size);

    while(1) {
	arglist_start(args, fmt);
	len = g_vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size-1)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = g_malloc(reply_buffer_size);
    }

    if (g_fprintf(cmdout,"%03d %s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(_("! error %d (%s) in printf\n"), errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	compress_sorted_files = compress_files(compress_sorted_files);
	exit(1);
    }
    if (fflush(cmdout) != 0)
    {
	dbprintf(_("! error %d (%s) in fflush\n"), errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	compress_sorted_files = compress_files(compress_sorted_files);
	exit(1);
    }
    dbprintf(_("< %03d %s\n"), n, reply_buffer);
}

/* send one line of a multi-line response */
static void lreply(int n, char *fmt, ...)
{
    va_list args;
    int len;

    if(!reply_buffer)
	reply_buffer = g_malloc(reply_buffer_size);

    while(1) {
	arglist_start(args, fmt);
	len = g_vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size-1)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = g_malloc(reply_buffer_size);
    }

    if (g_fprintf(cmdout,"%03d-%s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(_("! error %d (%s) in printf\n"),
		  errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	compress_sorted_files = compress_files(compress_sorted_files);
	exit(1);
    }
    if (fflush(cmdout) != 0)
    {
	dbprintf(_("! error %d (%s) in fflush\n"),
		  errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	compress_sorted_files = compress_files(compress_sorted_files);
	exit(1);
    }

    dbprintf("< %03d-%s\n", n, reply_buffer);

}

/* send one line of a multi-line response */
static void fast_lreply(int n, char *fmt, ...)
{
    va_list args;
    int len;

    if(!reply_buffer)
	reply_buffer = g_malloc(reply_buffer_size);

    while(1) {
	arglist_start(args, fmt);
	len = g_vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size-1)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = g_malloc(reply_buffer_size);
    }

    if (g_fprintf(cmdout,"%03d-%s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(_("! error %d (%s) in printf\n"),
		  errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	compress_sorted_files = compress_files(compress_sorted_files);
	exit(1);
    }

    DBG(2, "< %03d-%s", n, reply_buffer);
}

/* see if hostname is valid */
/* valid is defined to be that there is an index directory for it */
/* also do a security check on the requested dump hostname */
/* to restrict access to index records if required */
/* return -1 if not okay */
static am_host_t *
is_dump_host_valid(
    char *	host)
{
    am_host_t   *ihost;
    disk_t      *diskp;

    if (get_config_name() == NULL) {
	reply(501, _("Must set config before setting host."));
	return NULL;
    }

    /* check that the config actually handles that host */
    ihost = lookup_host(host);
    if(ihost == NULL) {
	reply(501, _("Host %s is not in your disklist."), host);
	return NULL;
    }

    /* check if an index dir exist */
    if(get_index_dir(host, ihost->hostname, NULL)) {
	return ihost;
    }

    /* check if an index dir exist for at least one DLE */
    for(diskp = ihost->disks; diskp != NULL; diskp = diskp->hostnext) {
	if (get_index_dir(diskp->hostname, NULL, NULL)) {
	    return ihost;
	}
    }

    reply(501, _("No index records for host: %s. Have you enabled indexing?"),
	  host);
    return NULL;
}


static gboolean
is_disk_allowed(
    disk_t *disk)
{
    dumptype_t *dt = disk->config;
    host_limit_t *rl = NULL;
    char *peer;
    char *dle_hostname;
    GSList *iter;

    /* get the config: either for the DLE or the global config */
    if (dt) {
	if (dumptype_seen(dt, DUMPTYPE_RECOVERY_LIMIT)) {
	    g_debug("using recovery limit from DLE");
	    rl = dumptype_get_recovery_limit(dt);
	}
    }
    if (!rl) {
	if (getconf_seen(CNF_RECOVERY_LIMIT)) {
	    g_debug("using global recovery limit");
	    rl = getconf_recovery_limit(CNF_RECOVERY_LIMIT);
	}
    }
    if (!rl) {
	g_debug("no recovery limit found; allowing access");
	return TRUE;
    }

    peer = getenv("AMANDA_AUTHENTICATED_PEER");
    if (!peer || !*peer) {
	g_warning("DLE has a recovery-limit, but no authenticated peer name is "
		  "available; rejecting");
	return FALSE;
    }

    /* check same-host */
    dle_hostname = disk->host? disk->host->hostname : NULL;
    if (rl->same_host && dle_hostname) {
	if (0 == g_ascii_strcasecmp(peer, dle_hostname)) {
	    g_debug("peer matched same-host ('%s')", dle_hostname);
	    return TRUE;
	}
    }

    /* check server */
    if (rl->server) {
	char myhostname[MAX_HOSTNAME_LENGTH+1];
	if (gethostname(myhostname, MAX_HOSTNAME_LENGTH) == 0) {
	    myhostname[MAX_HOSTNAME_LENGTH] = '\0';
	    g_debug("server hostname: %s", myhostname);
	    if (0 == g_ascii_strcasecmp(peer, myhostname)) {
		g_debug("peer matched server ('%s')", myhostname);
		return TRUE;
	    }
	}
    }

    /* check the match list */
    for (iter = rl->match_pats; iter; iter = iter->next) {
	char *pat = iter->data;
	if (match_host(pat, peer))
	    return TRUE;
    }

    g_warning("peer '%s' does not match any of the recovery-limit restrictions; rejecting",
	    peer);

    return FALSE;
}

static int
is_disk_valid(
    char *disk)
{
    disk_t *idisk;
    char *qdisk;

    if (get_config_name() == NULL) {
	reply(501, _("Must set config,host before setting disk."));
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(501, _("Must set host before setting disk."));
	return -1;
    }

    /* check that the config actually handles that disk, and that recovery-limit allows it */
    idisk = lookup_disk(dump_hostname, disk);
    if(idisk == NULL || !is_disk_allowed(idisk)) {
	qdisk = quote_string(disk);
	reply(501, _("Disk %s:%s is not in the server's disklist."), dump_hostname, qdisk);
	amfree(qdisk);
	return -1;
    }

    /* assume an index dir already */
    if (get_index_dir(dump_hostname, idisk->hostname, disk) == 0) {
	qdisk = quote_string(disk);
	reply(501, _("No index records for disk: %s. Invalid?"), qdisk);
	amfree(qdisk);
	return -1;
    }

    return 0;
}


static int
check_and_load_config(
    char *	config)
{
    char *conf_diskfile;
    char *conf_tapelist;
    char *conf_indexdir;
    char *lock_file;
    struct stat dir_stat;

    /* check that the config actually exists */
    if (config == NULL) {
	reply(501, _("Must set config first."));
	return -1;
    }

    /* (re-)initialize configuration with the new config name */
    config_init(CONFIG_INIT_EXPLICIT_NAME, config);
    if (config_errors(NULL) >= CFGERR_ERRORS) {
	reply(501, _("Could not read config file for %s!"), config);
	return -1;
    }

    check_running_as(RUNNING_AS_DUMPUSER_PREFERRED);

    conf_diskfile = config_dir_relative(getconf_str(CNF_DISKFILE));
    read_diskfile(conf_diskfile, &disk_list);
    amfree(conf_diskfile);
    if (config_errors(NULL) >= CFGERR_ERRORS) {
	reply(501, _("Could not read disk file %s!"), conf_diskfile);
	return -1;
    }

    conf_tapelist = config_dir_relative(getconf_str(CNF_TAPELIST));
    if(read_tapelist(conf_tapelist)) {
	reply(501, _("Could not read tapelist file %s!"), conf_tapelist);
	amfree(conf_tapelist);
	return -1;
    }
    amfree(conf_tapelist);

    dbrename(get_config_name(), DBG_SUBDIR_SERVER);

    output_find = find_dump(&disk_list);
    /* the 'w' here sorts by write timestamp, so that the first instance of
     * any particular datestamp/host/disk/level/part that we see is the one
     * written earlier */
    sort_find_result("DLKHspwB", &output_find);

    conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));
    if (stat (conf_indexdir, &dir_stat) != 0 || !S_ISDIR(dir_stat.st_mode)) {
	reply(501, _("Index directory %s does not exist"), conf_indexdir);
	amfree(conf_indexdir);
	return -1;
    }

    /* remove previous lock */
    if (lock_index) {
	file_lock_unlock(lock_index);
	file_lock_free(lock_index);
	lock_index = NULL;
    }

    /* take a lock file to prevent concurent trim */
    lock_file = g_strdup_printf("%s/%s", conf_indexdir, "lock");
    lock_index = file_lock_new(lock_file);
    file_lock_lock_rd(lock_index);

    amfree(lock_file);
    amfree(conf_indexdir);

    return 0;
}


static int
build_disk_table(void)
{
    char *date;
    char *last_timestamp;
    char *last_storage;
    off_t last_filenum;
    int last_level;
    int last_partnum;
    find_result_t *find_output;

    if (get_config_name() == NULL) {
	reply(590, _("Must set config,host,disk before building disk table"));
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(590, _("Must set host,disk before building disk table"));
	return -1;
    }
    else if (disk_name == NULL) {
	reply(590, _("Must set disk before building disk table"));
	return -1;
    }

    clear_list();
    last_timestamp = NULL;
    last_storage = NULL;
    last_filenum = (off_t)-1;
    last_level = -1;
    last_partnum = -1;
    for(find_output = output_find;
	find_output != NULL; 
	find_output = find_output->next) {
	if(strcasecmp(dump_hostname, find_output->hostname) == 0 &&
	   g_str_equal(disk_name, find_output->diskname) &&
	   g_str_equal("OK", find_output->status) &&
	   g_str_equal("OK", find_output->dump_status)) {
	    /*
	     * The sort order puts holding disk entries first.  We want to
	     * use them if at all possible, so ignore any other entries
	     * for the same datestamp after we see a holding disk entry
	     * (as indicated by a filenum of zero).
	     */
	    if(last_timestamp &&
	       g_str_equal(find_output->timestamp, last_timestamp) &&
	       find_output->level == last_level && 
	       last_filenum == 0) {
		continue;
	    }
	    /* ignore duplicate partnum */
	    if (last_timestamp &&
	        g_str_equal(find_output->timestamp, last_timestamp) &&
	        find_output->level == last_level &&
	        find_output->partnum == last_partnum &&
	        (!am_has_feature(their_features, fe_amindexd_STORAGE) ||
		 g_str_equal(find_output->storage, last_storage))) {
		continue;
	    }
	    if (storage_list) {
		char **storage_l;
		gboolean found = FALSE;
		for (storage_l = storage_list; *storage_l != NULL; storage_l++) {
		     if (g_str_equal(find_output->storage, *storage_l))
			found = TRUE;
		}
		if (!found)
		    continue;
	    }
	    last_timestamp = find_output->timestamp;
	    last_storage = find_output->storage;
	    last_filenum = find_output->filenum;
	    last_level = find_output->level;
	    last_partnum = find_output->partnum;
	    date = amindexd_nicedate(find_output->timestamp);
	    add_dump(find_output->hostname, date, find_output->level,
		     find_output->storage, find_output->label, find_output->filenum,
		     find_output->partnum, find_output->totalparts);
	    dbprintf("- %s %d %s %lld %d %d\n",
		     date, find_output->level, 
		     find_output->label,
		     (long long)find_output->filenum,
		     find_output->partnum, find_output->totalparts);
	}
    }

    clean_dump();

    return 0;
}


static int
disk_history_list(void)
{
    DUMP_ITEM *item;
    char date[20];

    if (get_config_name() == NULL) {
	reply(502, _("Must set config,host,disk before listing history"));
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(502, _("Must set host,disk before listing history"));
	return -1;
    }
    else if (disk_name == NULL) {
	reply(502, _("Must set disk before listing history"));
	return -1;
    }

    lreply(200, _(" Dump history for config \"%s\" host \"%s\" disk %s"),
	  get_config_name(), dump_hostname, qdisk_name);

    for (item=first_dump(); item!=NULL; item=next_dump(item)){
        char *tapelist_str = marshal_tapelist(item->tapes, 1,
		am_has_feature(their_features, fe_amrecover_storage_in_marshall));

	strncpy(date, item->date, 20);
	date[19] = '\0';
	if(!am_has_feature(their_features,fe_amrecover_timestamp))
	    date[10] = '\0';

	if(am_has_feature(their_features, fe_amindexd_marshall_in_DHST)){
	    lreply(201, " %s %d %s", date, item->level, tapelist_str);
	}
	else{
	    lreply(201, " %s %d %s %lld", date, item->level,
		tapelist_str, (long long)item->file);
	}
	amfree(tapelist_str);
    }

    reply(200, _("Dump history for config \"%s\" host \"%s\" disk %s"),
	  get_config_name(), dump_hostname, qdisk_name);

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
    char line[STR_SIZE];
    FILE *fp;
    int last_level;
    char *ldir = NULL;
    char *filename = NULL;
    size_t ldir_len;
    GPtrArray *emsg = NULL;

    if (get_config_name() == NULL || dump_hostname == NULL || disk_name == NULL) {
	reply(502, _("Must set config,host,disk before asking about directories"));
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(502, _("Must set host,disk before asking about directories"));
	return -1;
    }
    else if (disk_name == NULL) {
	reply(502, _("Must set disk before asking about directories"));
	return -1;
    }
    else if (target_date == NULL) {
	reply(502, _("Must set date before asking about directories"));
	return -1;
    }
    /* scan through till we find first dump on or before date */
    for (item=first_dump(); item!=NULL; item=next_dump(item))
	if (cmp_date(item->date, target_date) <= 0)
	    break;

    if (item == NULL)
    {
	/* no dump for given date */
	reply(500, _("No dumps available on or before date \"%s\""), target_date);
	return -1;
    }

    if(g_str_equal(dir, "/")) {
	ldir = g_strdup(dir);
    } else {
	ldir = g_strconcat(dir, "/", NULL);
    }
    ldir_len = strlen(ldir);

    /* go back till we hit a level 0 dump */
    do
    {
	amfree(filename);
	emsg = g_ptr_array_new();
	filename = get_index_name(dump_hostname, item->hostname, disk_name,
				  item->date, item->level, &emsg);
	if (filename == NULL) {
	    reply_ptr_array(599, emsg);
	    amfree(ldir);
	    return -1;
	}
	g_ptr_array_free_full(emsg);
	dbprintf("f %s\n", filename);
	if ((fp = fopen(filename, "r")) == NULL) {
	    reply(599, _("System error: %s"), strerror(errno));
	    amfree(filename);
	    amfree(ldir);
	    return -1;
	}
	while (fgets(line, STR_SIZE, fp) != NULL) {
	    if (line[0] == '\0')
		continue;
	    if(strlen(line) > 0 && line[strlen(line)-1] == '\n')
		line[strlen(line)-1] = '\0';
	    if (strncmp(line, ldir, ldir_len) != 0) {
		continue;			/* not found yet */
	    }
	    amfree(filename);
	    amfree(ldir);
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
    reply(500, _("\"%s\" is an invalid directory"), dir);
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
    GPtrArray *emsg = NULL;
    am_feature_e marshall_feature;

    if (recursive) {
        marshall_feature = fe_amindexd_marshall_in_ORLD;
    } else {
        marshall_feature = fe_amindexd_marshall_in_OLSD;
    }

    clear_dir_list();

    if (get_config_name() == NULL) {
	reply(502, _("Must set config,host,disk before listing a directory"));
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(502, _("Must set host,disk before listing a directory"));
	return -1;
    }
    else if (disk_name == NULL) {
	reply(502, _("Must set disk before listing a directory"));
	return -1;
    }
    else if (target_date == NULL) {
	reply(502, _("Must set date before listing a directory"));
	return -1;
    }

    /* scan through till we find first dump on or before date */
    for (dump_item=first_dump(); dump_item!=NULL; dump_item=next_dump(dump_item))
	if (cmp_date(dump_item->date, target_date) <= 0)
	    break;

    if (dump_item == NULL)
    {
	/* no dump for given date */
	reply(500, _("No dumps available on or before date \"%s\""), target_date);
	return -1;
    }

    /* get data from that dump */
    emsg = g_ptr_array_new();
    if (process_ls_dump(dir, dump_item, recursive, &emsg) == -1) {
	reply_ptr_array(599, emsg);
	g_ptr_array_free_full(emsg);
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
		reply_ptr_array(599, emsg);
		g_ptr_array_free_full(emsg);
		return -1;
	    }
	}
    }
    g_ptr_array_free_full(emsg);

    /* return the information to the caller */
    lreply(200, _(" Opaque list of %s"), dir);
    for(level=0; level < DUMP_LEVELS; level++) {
	for (dir_item = get_dir_list(); dir_item != NULL; 
	     dir_item = dir_item->next) {

	    if(dir_item->dump->level == level) {
		if (!am_has_feature(their_features, marshall_feature) &&
	            (num_entries(dir_item->dump->tapes) > 1 ||
	            dir_item->dump->tapes->numfiles > 1)) {
	            fast_lreply(501, _(" ERROR: Split dumps not supported"
				" with old version of amrecover."));
		    break;
		}
		else {
		    opaque_ls_one(dir_item, marshall_feature, recursive);
		}
	    }
	}
    }
    reply(200, _(" Opaque list of %s"), dir);

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
    char *qtapelist_str;
    char *qpath;

    if (am_has_feature(their_features, marshall_feature)) {
	tapelist_str = marshal_tapelist(dir_item->dump->tapes, 1,
		 am_has_feature(their_features, fe_amrecover_storage_in_marshall));
    } else {
	tapelist_str = dir_item->dump->tapes->label;
    }

    if (am_has_feature(their_features, fe_amindexd_quote_label)) {
	qtapelist_str = quote_string(tapelist_str);
    } else {
	qtapelist_str = g_strdup(tapelist_str);
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
	fast_lreply(201, " %s %d %s %lld %s",
		    date,
		    dir_item->dump->level,
		    qtapelist_str,
		    (long long)dir_item->dump->file,
		    qpath);
    }
    else {

	fast_lreply(201, " %s %d %s %s",
		    date, dir_item->dump->level,
		    qtapelist_str, qpath);
    }
    amfree(qpath);
    if(am_has_feature(their_features, marshall_feature)) {
	amfree(tapelist_str);
    }
    amfree(qtapelist_str);
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
    if (get_config_name() == NULL) {
	reply(501, _("Must set config before asking about tapedev."));
	return -1;
    }

    /* use amrecover_changer if possible */
    if ((result = getconf_str(CNF_AMRECOVER_CHANGER)) != NULL  &&
        *result != '\0') {
	dbprintf(_("tapedev_is amrecover_changer: %s\n"), result);
	reply(200, "%s", result);
	return 0;
    }

    /* use changer if possible */
    if ((result = getconf_str(CNF_TPCHANGER)) != NULL  &&  *result != '\0') {
	dbprintf(_("tapedev_is tpchanger: %s\n"), result);
	reply(200, "%s", result);
	return 0;
    }

    /* get tapedev value */
    if ((result = getconf_str(CNF_TAPEDEV)) != NULL  &&  *result != '\0') {
	dbprintf(_("tapedev_is tapedev: %s\n"), result);
	reply(200, "%s", result);
	return 0;
    }

    dbprintf(_("No tapedev or tpchanger in config site.\n"));

    reply(501, _("Tapedev or tpchanger not set in config file."));
    return -1;
}


/* returns YES if dumps for disk are compressed, NO if not */
static int
are_dumps_compressed(void)
{
    GList  *dlist;
    disk_t *diskp = NULL;

    /* check state okay to do this */
    if (get_config_name() == NULL) {
	reply(501, _("Must set config,host,disk name before asking about dumps."));
	return -1;
    }
    else if (dump_hostname == NULL) {
	reply(501, _("Must set host,disk name before asking about dumps."));
	return -1;
    }
    else if (disk_name == NULL) {
	reply(501, _("Must set disk name before asking about dumps."));
	return -1;
    }

    /* now go through the list of disks and find which have indexes */
    for (dlist = disk_list.head; dlist != NULL; dlist = dlist->next) {
	diskp = dlist->data;
	if ((strcasecmp(diskp->host->hostname, dump_hostname) == 0)
		&& (g_str_equal(diskp->name, disk_name))) {
	    break;
	}
    }

    if (diskp == NULL) {
	reply(501, _("Couldn't find host/disk in disk file."));
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
    char *s;
    int ch;
    char *cmd_undo, cmd_undo_ch;
    socklen_t_equiv socklen;
    sockaddr_union his_addr;
    char *arg = NULL;
    char *cmd;
    size_t len;
    int user_validated = 0;
    char *errstr = NULL;
    char *pgm = "amindexd";		/* in case argv[0] is not set */
    char his_hostname[MAX_HOSTNAME_LENGTH];
    char *cfg_opt = NULL;

    if (argc > 1 && argv && argv[1] && g_str_equal(argv[1], "--version")) {
	printf("amindexd-%s\n", VERSION);
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

    safe_fd(DATA_FD_OFFSET, 2);
    openbsd_fd_inform();
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

    dbopen(DBG_SUBDIR_SERVER);
    dbprintf(_("version %s\n"), VERSION);

    if(argv == NULL) {
	error("argv == NULL\n");
    }

    if (! (argc >= 1 && argv[0] != NULL)) {
	dbprintf(_("WARNING: argv[0] not defined: check inetd.conf\n"));
    }

    debug_dup_stderr_to_debug();

    /* initialize */

    argc--;
    argv++;

    if(argc > 0 && g_str_equal(*argv, "-t")) {
	amindexd_debug = 1;
	argc--;
	argv++;
    }

    if(argc > 0 && g_str_equal(*argv, "amandad")) {
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
	safe_fd(dbfd(), 1);
    }

    if (argc > 0) {
	cfg_opt = *argv;
	argc--;
	argv++;
    }

    if(gethostname(local_hostname, sizeof(local_hostname)-1) == -1) {
	error(_("gethostname: %s"), strerror(errno));
	/*NOTREACHED*/
    }
    local_hostname[sizeof(local_hostname)-1] = '\0';

    /* now trim domain off name */
    s = local_hostname;
    ch = *s++;
    while(ch && ch != '.') ch = *s++;
    s[-1] = '\0';


    if(from_amandad == 0) {
	if(!amindexd_debug) {
	    /* who are we talking to? */
	    socklen = sizeof (his_addr);
	    if (getpeername(0, (struct sockaddr *)&his_addr, &socklen) == -1)
		error(_("getpeername: %s"), strerror(errno));

	    /* Try a reverse (IP->hostname) resolution, and fail if it does
	     * not work -- this is a basic security check */
	    if (getnameinfo((struct sockaddr *)&his_addr, SS_LEN(&his_addr),
			    his_hostname, sizeof(his_hostname),
			    NULL, 0,
			    0)) {
		error(_("getnameinfo(%s): hostname lookup failed"),
		      str_sockaddr(&his_addr));
		/*NOTREACHED*/
	    }
	}

	/* Set up the input and output FILEs */
	cmdout = stdout;
	cmdin = stdin;
    }
    else {
	/* read the REQ packet */
	for(; (line = agets(stdin)) != NULL; free(line)) {
	    if(strncmp_const(line, "OPTIONS ") == 0) {
                if (g_options != NULL) {
		    dbprintf(_("REQ packet specified multiple OPTIONS.\n"));
                    free_g_options(g_options);
                }
		g_options = parse_g_options(line+8, 1);
		if(!g_options->hostname) {
		    g_options->hostname = g_malloc(MAX_HOSTNAME_LENGTH+1);
		    gethostname(g_options->hostname, MAX_HOSTNAME_LENGTH);
		    g_options->hostname[MAX_HOSTNAME_LENGTH] = '\0';
		}
	    }
	}
	amfree(line);

	if(amandad_auth && g_options->auth) {
	    if(strcasecmp(amandad_auth, g_options->auth) != 0) {
		g_printf(_("ERROR recover program ask for auth=%s while amindexd is configured for '%s'\n"),
		       g_options->auth, amandad_auth);
		error(_("amindexd: ERROR recover program ask for auth=%s while amindexd is configured for '%s'"),
		      g_options->auth, amandad_auth);
		/*NOTREACHED*/
	    }
	}
	/* send the REP packet */
	g_printf("CONNECT MESG %d\n", DATA_FD_OFFSET);
	g_printf("\n");
	fflush(stdout);
	fclose(stdin);
	fclose(stdout);

	cmdout = fdopen(DATA_FD_OFFSET + 0, "a");
	if (!cmdout) {
	    error(_("amindexd: Can't fdopen(%d): %s"), DATA_FD_OFFSET + 0, strerror(errno));
	    /*NOTREACHED*/
	}

	cmdin = fdopen(DATA_FD_OFFSET + 1, "r");
	if (!cmdin) {
	    error(_("amindexd: Can't fdopen(%d): %s"), DATA_FD_OFFSET + 1, strerror(errno));
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

    if (cfg_opt != NULL && check_and_load_config(cfg_opt) != -1) { /* load the config */
	return 1;
    }

    reply(220, _("%s AMANDA index server (%s) ready."), local_hostname,
	  VERSION);

    user_validated = from_amandad;

    /* a real simple parser since there are only a few commands */
    while (1)
    {
	/* get a line from the client */
	while(1) {
	    if((part = agets(cmdin)) == NULL) {
		if(errno != 0) {
		    dbprintf(_("? read error: %s\n"), strerror(errno));
		} else {
		    dbprintf(_("? unexpected EOF\n"));
		}
		if(line) {
		    dbprintf(_("? unprocessed input:\n"));
		    dbprintf("-----\n");
		    dbprintf("? %s\n", line);
		    dbprintf("-----\n");
		}
		amfree(line);
		amfree(part);
		amfree(arg);
		uncompress_remove = remove_files(uncompress_remove);
		compress_sorted_files = compress_files(compress_sorted_files);
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

	dbprintf("> %s\n", line);

	if (arg != NULL)
	    amfree(arg);
	s = line;
	ch = *s++;

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    reply(500, _("Command not recognised/incorrect: %s"), line);
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
	if (!user_validated && g_str_equal(cmd, "SECURITY") && arg) {
	    user_validated = amindexd_debug ||
				check_security(
					(sockaddr_union *)&his_addr,
					arg, 0, &errstr);
	    if(user_validated) {
		reply(200, _("Access OK"));
		amfree(line);
		continue;
	    }
	}
	if (!user_validated) {  /* don't tell client the reason, just log it to debug log */
	    reply(500, _("Access not allowed"));
	    if (errstr) {
		dbprintf("%s\n", errstr);
		amfree(errstr);
	    }
	    break;
	}

	if (g_str_equal(cmd, "QUIT")) {
	    amfree(line);
	    break;
	} else if (g_str_equal(cmd, "HOST") && arg) {
	    am_host_t *lhost;
	    /* set host we are restoring */
	    s[-1] = '\0';
	    if ((lhost = is_dump_host_valid(arg)) != NULL)
	    {
		g_free(dump_hostname);
		dump_hostname = g_strdup(lhost->hostname);
		reply(200, _("Dump host set to %s."), dump_hostname);
		amfree(qdisk_name);		/* invalidate any value */
		amfree(disk_name);		/* invalidate any value */
	    }
	    s[-1] = (char)ch;
	} else if (g_str_equal(cmd, "LISTHOST")) {
	    GList  *dlist, *dlistup;
	    disk_t *disk,
                   *diskdup;
	    int nbhost = 0,
                found = 0;
	    s[-1] = '\0';
	    if (get_config_name() == NULL) {
		reply(501, _("Must set config before listhost"));
	    }
	    else {
		lreply(200, _(" List hosts for config %s"), get_config_name());
		for (dlist = disk_list.head; dlist != NULL; dlist = dlist->next) {
		    disk = dlist->data;
                    found = 0;
		    for (dlistup = disk_list.head; dlistup != NULL; dlistup = dlistup->next) {
			diskdup = dlistup->data;
		        if(g_str_equal(diskdup->host->hostname,
                                       disk->host->hostname)) {
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
		    reply(200, _(" List hosts for config %s"), get_config_name());
		}
		else {
		    reply(200, _("No hosts for config %s"), get_config_name());
		}
	    }
	    s[-1] = (char)ch;
	} else if (g_str_equal(cmd, "DISK") && arg) {
	    s[-1] = '\0';
	    if (is_disk_valid(arg) != -1) {
		g_free(disk_name);
		disk_name = g_strdup(arg);
		amfree(qdisk_name);
		qdisk_name = quote_string(disk_name);
		if (build_disk_table() != -1) {
		    reply(200, _("Disk set to %s."), qdisk_name);
		}
	    }
	    s[-1] = (char)ch;
	} else if (g_str_equal(cmd, "DLE")) {

	    if (!dump_hostname || !disk_name) {
		reply(200, "NODLE");
	    } else {
                disk_t *dp = lookup_disk(dump_hostname, disk_name);
                if (dp->line == 0) {
                    reply(200, "NODLE");
                } else {
                    char *b64disk;
                    gchar **errors;

                    b64disk = amxml_format_tag("disk", dp->name);
                    dp->host->features = their_features;

                    errors = validate_optionstr(dp);

                    if (errors) {
                        gchar **ptr;
                        for (ptr = errors; *ptr; ptr++)
                            g_debug("ERROR: %s:%s %s", dump_hostname, disk_name,
                                *ptr);
                        g_strfreev(errors);
                        reply(200, "NODLE");
                    } else {
                        GString *strbuf = g_string_new("<dle>\n");
                        char *l, *ql;
                        char *optionstr;

                        g_string_append_printf(strbuf,
                            "  <program>%s</program>\n", dp->program);

                        if (dp->application) {
                            application_t *application;
                            char *xml_app;

                            application = lookup_application(dp->application);
                            g_assert(application != NULL);
                            xml_app = xml_application(dp, application,
                                                      their_features);
                            g_string_append(strbuf, xml_app);
                            g_free(xml_app);
                        }

                        g_string_append_printf(strbuf, "  %s\n", b64disk);

                        if (dp->device) {
                            char *b64device = amxml_format_tag("diskdevice",
                                                               dp->device);
                            g_string_append_printf(strbuf, "  %s\n", b64device);
                            g_free(b64device);
                        }

                        optionstr = xml_optionstr(dp, 0);
                        g_string_append_printf(strbuf, "%s</dle>\n",
                            optionstr);
                        g_free(optionstr);

                        l = g_string_free(strbuf, FALSE);
                        ql = quote_string(l);
                        g_free(l);

                        reply(200, "%s", ql);
                        g_free(ql);
                    }
                    g_free(b64disk);
                }
	    }
	} else if (g_str_equal(cmd, "LISTDISK")) {
	    char *qname;
	    GList  *dlist;
	    disk_t *disk;
	    int nbdisk = 0;
	    s[-1] = '\0';
	    if (get_config_name() == NULL) {
		reply(501, _("Must set config, host before listdisk"));
	    }
	    else if (dump_hostname == NULL) {
		reply(501, _("Must set host before listdisk"));
	    }
	    else if(arg) {
		lreply(200, _(" List of disk for device %s on host %s"), arg,
		       dump_hostname);
		for (dlist = disk_list.head; dlist != NULL; dlist = dlist->next) {
		    disk = dlist->data;

		    if (strcasecmp(disk->host->hostname, dump_hostname) == 0 &&
		      ((disk->device && g_str_equal(disk->device, arg)) ||
		      (!disk->device && g_str_equal(disk->name, arg)))) {
			qname = quote_string(disk->name);
			fast_lreply(201, " %s", qname);
			amfree(qname);
			nbdisk++;
		    }
		}
		if(nbdisk > 0) {
		    reply(200, _("List of disk for device %s on host %s"), arg,
			  dump_hostname);
		}
		else {
		    reply(200, _("No disk for device %s on host %s"), arg,
			  dump_hostname);
		}
	    }
	    else {
		lreply(200, _(" List of disk for host %s"), dump_hostname);
		for (dlist = disk_list.head; dlist != NULL; dlist = dlist->next) {
		    disk = dlist->data;
		    if(strcasecmp(disk->host->hostname, dump_hostname) == 0) {
			qname = quote_string(disk->name);
			fast_lreply(201, " %s", qname);
			amfree(qname);
			nbdisk++;
		    }
		}
		if(nbdisk > 0) {
		    reply(200, _("List of disk for host %s"), dump_hostname);
		}
		else {
		    reply(200, _("No disk for host %s"), dump_hostname);
		}
	    }
	    s[-1] = (char)ch;
	} else if (g_str_equal(cmd, "SCNF") && arg) {
	    s[-1] = '\0';
	    if (check_and_load_config(arg) != -1) {    /* try to load the new config */
		amfree(dump_hostname);		/* invalidate any value */
		amfree(qdisk_name);		/* invalidate any value */
		amfree(disk_name);		/* invalidate any value */
		reply(200, _("Config set to %s."), get_config_name());
	    } /* check_and_load_config replies with any failure messages */
	    s[-1] = (char)ch;
	} else if (g_str_equal(cmd, "FEATURES") && arg) {
	    char *our_feature_string = NULL;
	    char *their_feature_string = NULL;
	    s[-1] = '\0';
	    am_release_feature_set(our_features);
	    am_release_feature_set(their_features);
	    our_features = am_init_feature_set();
	    our_feature_string = am_feature_to_string(our_features);
	    g_free(their_feature_string);
	    their_feature_string = g_strdup(arg);
	    their_features = am_string_to_feature(their_feature_string);
	    if (!their_features) {
		g_warning("Invalid client feature set '%s'", their_feature_string);
		their_features = am_set_default_feature_set();
	    }
	    reply(200, "FEATURES %s", our_feature_string);
	    amfree(our_feature_string);
	    amfree(their_feature_string);
	    s[-1] = (char)ch;
	} else if (g_str_equal(cmd, "DATE") && arg) {
	    s[-1] = '\0';
	    g_free(target_date);
	    target_date = g_strdup(arg);
	    reply(200, _("Working date set to %s."), target_date);
	    s[-1] = (char)ch;
	} else if (g_str_equal(cmd, "DHST")) {
	    (void)disk_history_list();
	} else if (g_str_equal(cmd, "OISD") && arg) {
	    if (is_dir_valid_opaque(arg) != -1) {
		reply(200, _("\"%s\" is a valid directory"), arg);
	    }
	} else if (g_str_equal(cmd, "OLSD") && arg) {
	    (void)opaque_ls(arg,0);
	} else if (g_str_equal(cmd, "ORLD") && arg) {
	    (void)opaque_ls(arg, 1);
	} else if (g_str_equal(cmd, "TAPE")) {
	    (void)tapedev_is();
	} else if (g_str_equal(cmd, "DCMP")) {
	    (void)are_dumps_compressed();
	} else if (g_str_equal(cmd, "STORAGE")) {
	    s[-1] = '\0';
	    g_strfreev(storage_list);
	    if (arg && *arg != '\0') {
		char **storage_l;
		char **storage_n;
		char *invalid_storage = NULL;

		storage_list = split_quoted_strings(arg);
		storage_n = storage_list;
		for (storage_l = storage_list; *storage_l != NULL; storage_l++) {
		    if (g_str_equal(*storage_l, "HOLDING") ||
			lookup_storage(*storage_l)) {
			*storage_n = *storage_l;
			storage_n++;
		    } else {
			char *qstorage = quote_string(*storage_l);
			if (invalid_storage) {
			    char *new_invalid_storage = g_strconcat(invalid_storage, " ", qstorage, NULL);
			    g_free(invalid_storage);
			    invalid_storage = new_invalid_storage;
			    g_free(qstorage);
			} else {
			    invalid_storage = qstorage;
			}
			g_free(*storage_l);
			*storage_l = NULL;
		    }
		}
		if (invalid_storage) {
		    reply(599, _("invalid storage: %s"), invalid_storage);
		    g_free(invalid_storage);
		    invalid_storage = NULL;
		} else {
		    reply(200, _("storage set to %s"), arg);
		}
	    } else {
		storage_list = NULL;
		reply(200, _("storage unset"));
	    }
	    sort_find_result_with_storage("DLKHspwB", storage_list, &output_find);
	    build_disk_table();
	    s[-1] = (char)ch;
	} else {
	    *cmd_undo = cmd_undo_ch;	/* restore the command line */
	    reply(500, _("Command not recognised/incorrect: %s"), cmd);
	}
	amfree(line);
    }
    amfree(arg);
    amfree(line);

    uncompress_remove = remove_files(uncompress_remove);
    compress_sorted_files = compress_files(compress_sorted_files);

    /* remove previous lock */
    if (lock_index) {
	file_lock_unlock(lock_index);
	file_lock_free(lock_index);
	lock_index = NULL;
    }

    free_find_result(&output_find);
    reply(200, _("Good bye."));
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
	g_snprintf(nice, sizeof(nice), "%4d-%02d-%02d",
		year, month, day);
    }
    else {
	strncpy(atime, &(datestamp[8]), 6);
	atime[6] = '\0';
	numtime = atoi(atime);
	hours = numtime / 10000;
	minutes = (numtime / 100) % 100;
	seconds = numtime % 100;

	g_snprintf(nice, sizeof(nice), "%4d-%02d-%02d-%02d-%02d-%02d",
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

static int
get_index_dir(
    char *dump_hostname,
    char *hostname,
    char *diskname)
{
    struct stat  dir_stat;
    char        *fn;
    char        *s;
    char        *lower_hostname;

    lower_hostname = g_strdup(dump_hostname);
    for(s=lower_hostname; *s != '\0'; s++)
	*s = tolower(*s);

    fn = getindexfname(dump_hostname, diskname, NULL, 0);
    if (stat(fn, &dir_stat) == 0 && S_ISDIR(dir_stat.st_mode)) {
	amfree(lower_hostname);
	amfree(fn);
	return 1;
    }
    amfree(fn);
    if (hostname != NULL) {
	fn = getindexfname(hostname, diskname, NULL, 0);
	if (stat(fn, &dir_stat) == 0 && S_ISDIR(dir_stat.st_mode)) {
	    amfree(lower_hostname);
	    amfree(fn);
	    return 1;
	}
    }
    amfree(fn);
    fn = getindexfname(lower_hostname, diskname, NULL, 0);
    if (stat(fn, &dir_stat) == 0 && S_ISDIR(dir_stat.st_mode)) {
	amfree(lower_hostname);
	amfree(fn);
	return 1;
    }
    amfree(fn);
    if(diskname != NULL) {
	fn = getoldindexfname(dump_hostname, diskname, NULL, 0);
	if (stat(fn, &dir_stat) == 0 && S_ISDIR(dir_stat.st_mode)) {
	    amfree(lower_hostname);
	    amfree(fn);
	    return 1;
	}
	amfree(fn);
	if (hostname != NULL) {
	    fn = getoldindexfname(hostname, diskname, NULL, 0);
	    if (stat(fn, &dir_stat) == 0 && S_ISDIR(dir_stat.st_mode)) {
		amfree(lower_hostname);
		amfree(fn);
		return 1;
	    }
	}
	amfree(fn);
	fn = getoldindexfname(lower_hostname, diskname, NULL, 0);
	if (stat(fn, &dir_stat) == 0 && S_ISDIR(dir_stat.st_mode)) {
	    amfree(lower_hostname);
	    amfree(fn);
	    return 1;
	}
	amfree(fn);
    }
    amfree(lower_hostname);
    return -1;
}

static char *
get_index_name(
    char       *dump_hostname,
    char       *hostname,
    char       *diskname,
    char       *timestamps,
    int         level,
    GPtrArray **emsg)
{
    struct stat  dir_stat;
    char        *fn;
    char        *s;
    char        *lower_hostname;
    gboolean     need_uncompress = FALSE;
    gboolean     need_sort = FALSE;

    lower_hostname = g_strdup(dump_hostname);
    for(s=lower_hostname; *s != '\0'; s++)
	*s = tolower(*s);

    fn = getindex_sorted_fname(dump_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	need_uncompress = FALSE;
	need_sort = FALSE;
	goto process_dump;
    }
    amfree(fn);

    fn = getindex_sorted_gz_fname(dump_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	need_uncompress = TRUE;
	need_sort = FALSE;
	goto process_dump;
    }
    amfree(fn);

    fn = getindex_unsorted_fname(dump_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	need_uncompress = FALSE;
	need_sort = TRUE;
	goto process_dump;
    }
    amfree(fn);

    fn = getindex_unsorted_gz_fname(dump_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	need_uncompress = TRUE;
	need_sort = TRUE;
	goto process_dump;
    }
    amfree(fn);

    fn = getindexfname(dump_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	need_uncompress = TRUE;
	need_sort = TRUE;
	goto process_dump;
    }
    amfree(fn);
    if(hostname != NULL) {
	fn = getindexfname(hostname, diskname, timestamps, level);
	if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	    need_uncompress = TRUE;
	    need_sort = TRUE;
	    goto process_dump;
	}
	amfree(fn);
    }
    amfree(fn);
    fn = getindexfname(lower_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	need_uncompress = TRUE;
	need_sort = TRUE;
	goto process_dump;
    }
    amfree(fn);
    if(diskname != NULL) {
	fn = getoldindexfname(dump_hostname, diskname, timestamps, level);
	if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	    need_uncompress = TRUE;
	    need_sort = TRUE;
	    goto process_dump;
	}
	amfree(fn);
	if(hostname != NULL) {
	    fn = getoldindexfname(hostname, diskname, timestamps, level);
	    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
		need_uncompress = TRUE;
		need_sort = TRUE;
		goto process_dump;
	    }
	}
	amfree(fn);
	fn = getoldindexfname(lower_hostname, diskname, timestamps, level);
	if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	    need_uncompress = TRUE;
	    need_sort = TRUE;
	    goto process_dump;
	}
	amfree(fn);
    }
    amfree(lower_hostname);
    g_ptr_array_add(*emsg, g_strdup_printf(_("index file not found for host '%s' disk '%s' level '%d' date '%s'"), dump_hostname, diskname, level, timestamps));
    return NULL;

process_dump:
    amfree(lower_hostname);
    return uncompress_file(hostname, diskname, timestamps, level,
			   fn, emsg, need_uncompress, need_sort);
}

static char *
uncompress_file(
    char       *hostname,
    char       *diskname,
    char       *timestamps,
    int         level,
    char       *filename,
    GPtrArray **emsg,
    gboolean    need_uncompress,
    gboolean    need_sort)
{
    char *cmd = NULL;
    char *new_filename = NULL;
    struct stat stat_filename;
    int result;
    int pipe_from_gzip;
    int pipe_to_sort;
    int indexfd;
    int nullfd;
    int uncompress_errfd;
    int sort_errfd;
    char line[STR_SIZE];
    FILE *pipe_stream;
    pid_t pid_gzip;
    pid_t pid_sort;
    pid_t pid_index;
    int        status;
    char      *msg;
    gpointer  *p;
    gpointer  *p_last;
    GPtrArray *uncompress_err;
    GPtrArray *sort_err;
    FILE      *uncompress_err_stream;
    FILE      *sort_err_stream;

    new_filename = getindex_unsorted_fname(hostname, diskname, timestamps, level);

    /* uncompress the file */
    result = stat(filename, &stat_filename);
    if (result == -1) {
	msg = g_strdup_printf(_("Source index file '%s' is inaccessible: %s"),
			     filename, strerror(errno));
	dbprintf("%s\n", msg);
	g_ptr_array_add(*emsg, msg);
	amfree(filename);
	return NULL;
    } else if(!S_ISREG((stat_filename.st_mode))) {
	    msg = g_strdup_printf(_("\"%s\" is not a regular file"), filename);
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    errno = -1;
	    amfree(filename);
	    amfree(cmd);
	    return NULL;
    }

    if (g_str_equal(filename, new_filename)) {
	return new_filename;
    }
    if (!need_uncompress || need_sort) {
	return new_filename;
    }

#ifdef UNCOMPRESS_OPT
#  define PARAM_UNCOMPRESS_OPT UNCOMPRESS_OPT
#else
#  define PARAM_UNCOMPRESS_OPT skip_argument
#endif

    indexfd = open(new_filename, O_WRONLY|O_CREAT, 0600);
    if (indexfd == -1) {
	msg = g_strdup_printf(_("Can't open '%s' for writting: %s"),
			      filename, strerror(errno));
	dbprintf("%s\n", msg);
	g_ptr_array_add(*emsg, msg);
	amfree(filename);
	return NULL;
    }

    if (need_uncompress) {
	int  pipedef;
	int *out_fd;

	/* just use our stderr directly for the pipe's stderr; in
	 * main() we send stderr to the debug file, or /dev/null
	 * if debugging is disabled */

	/* start the uncompress process */
	putenv(g_strdup("LC_ALL=C"));
	nullfd = open("/dev/null", O_RDONLY);

	if (need_sort) {
	    pipedef = STDOUT_PIPE|STDERR_PIPE;
	    out_fd = &pipe_from_gzip;
	} else {
	    pipedef = STDERR_PIPE;
	    out_fd = &indexfd;
	}
	pid_gzip = pipespawn(UNCOMPRESS_PATH, pipedef, 0,
			     &nullfd, out_fd, &uncompress_errfd,
			     UNCOMPRESS_PATH, PARAM_UNCOMPRESS_OPT,
			     filename, NULL);
	aclose(nullfd);

	if (need_sort) {
	    pipe_stream = fdopen(pipe_from_gzip, "r");
	    if (pipe_stream == NULL) {
		msg = g_strdup_printf(_("Can't fdopen pipe from gzip: %s"),
				      strerror(errno));
		dbprintf("%s\n", msg);
		g_ptr_array_add(*emsg, msg);
		amfree(filename);
		aclose(indexfd);
		return NULL;
	    }
	} else {
	    aclose(indexfd);
	}
    } else {
	pipe_stream = fopen(filename, "r");
	if (pipe_stream == NULL) {
	    msg = g_strdup_printf(_("Can't fopen index file (%s): %s"),
			     filename, strerror(errno));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    amfree(filename);
	    aclose(indexfd);
	    return NULL;
	}
    }

    if (need_sort) {
	/* start the sort process */
	putenv(g_strdup("LC_ALL=C"));

	if (getconf_seen(CNF_TMPDIR)) {
	    gchar *tmpdir = getconf_str(CNF_TMPDIR);
	    pid_sort = pipespawn(SORT_PATH, STDIN_PIPE|STDERR_PIPE, 0,
				 &pipe_to_sort, &indexfd, &sort_errfd,
				 SORT_PATH, "-T", tmpdir, NULL);
	} else {
	    pid_sort = pipespawn(SORT_PATH, STDIN_PIPE|STDERR_PIPE, 0,
				 &pipe_to_sort, &indexfd, &sort_errfd,
				 SORT_PATH, NULL);
	}
	aclose(indexfd);

	/* start a subprocess */
	/* send all ouput from uncompress process to sort process */
	pid_index = fork();
	switch (pid_index) {
	case -1:
	    msg = g_strdup_printf(
			_("fork error: %s"),
			strerror(errno));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    unlink(filename);
	    amfree(filename);
	default: break;
	case 0:
	    while (fgets(line, STR_SIZE, pipe_stream) != NULL) {
		if (line[0] != '\0') {
		    if (strchr(line,'/')) {
			full_write(pipe_to_sort,line,strlen(line));
		    }
		}
	    }
	    exit(0);
	}

	fclose(pipe_stream);
	aclose(pipe_to_sort);

	uncompress_err_stream = fdopen(uncompress_errfd, "r");
	uncompress_err = g_ptr_array_new();
	if (!uncompress_err_stream) {
	    g_ptr_array_add(uncompress_err,
		    g_strdup_printf("Can't fdopen uncompress_err_stream: %s\n",
				    strerror(errno)));
	} else {
	    while (fgets(line, sizeof(line), uncompress_err_stream) != NULL) {
		if (strlen(line) > 0 && line[strlen(line)-1] == '\n')
		    line[strlen(line)-1] = '\0';
		g_ptr_array_add(uncompress_err, g_strdup_printf("  %s", line));
		dbprintf("Uncompress: %s\n", line);
	    }
	    fclose(uncompress_err_stream);
	}

	sort_err_stream = fdopen(sort_errfd, "r");
	sort_err = g_ptr_array_new();
	if (!sort_err_stream) {
	    g_ptr_array_add(sort_err,
		    g_strdup_printf("Can't fdopen sort_err_stream: %s\n",
				    strerror(errno)));
	} else {
	    while (fgets(line, sizeof(line), sort_err_stream) != NULL) {
		if (strlen(line) > 0 && line[strlen(line)-1] == '\n')
		    line[strlen(line)-1] = '\0';
		g_ptr_array_add(sort_err, g_strdup_printf("  %s", line));
		dbprintf("Sort: %s\n", line);
	    }
	    fclose(sort_err_stream);
	}

	status = get_pid_status(pid_gzip, UNCOMPRESS_PATH, emsg);
	if (status == 0 && filename) {
	    unlink(filename);
	    amfree(filename);
	}
	if (uncompress_err->len > 0) {
	    p_last = uncompress_err->pdata + uncompress_err->len;
	    for (p = uncompress_err->pdata; p < p_last ;p++) {
		g_ptr_array_add(*emsg, (char *)*p);
	    }
	}
	g_ptr_array_free(uncompress_err, TRUE);

	status = get_pid_status(pid_index, "index", emsg);
	if (status == 0 && filename) {
	    unlink(filename);
	    amfree(filename);
	}

	status = get_pid_status(pid_sort, SORT_PATH, emsg);
	if (status == 0 && filename) {
	    unlink(filename);
	    amfree(filename);
	}
	if (sort_err->len > 0) {
	    p_last = sort_err->pdata + sort_err->len;
	    for (p = sort_err->pdata; p < p_last ;p++) {
		g_ptr_array_add(*emsg, (char *)*p);
	    }
	}
	g_ptr_array_free(sort_err, TRUE);
    }

    if (need_sort && new_filename && getconf_boolean(CNF_COMPRESS_INDEX)) {
	/* add at beginning */
	REMOVE_ITEM *compress_file;
	compress_file = (REMOVE_ITEM *)g_malloc(sizeof(REMOVE_ITEM));
	compress_file->filename = g_strdup(new_filename);
	compress_file->next = uncompress_remove;
	compress_sorted_files = compress_file;
    } else if (need_uncompress && new_filename && !getconf_boolean(CNF_COMPRESS_INDEX)) {
	/* add at beginning */
	REMOVE_ITEM *remove_file;
	remove_file = (REMOVE_ITEM *)g_malloc(sizeof(REMOVE_ITEM));
	remove_file->filename = g_strdup(new_filename);
	remove_file->next = uncompress_remove;
	uncompress_remove = remove_file;
    }

    amfree(cmd);
    return new_filename;
}

