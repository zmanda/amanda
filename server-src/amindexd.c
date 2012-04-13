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
#include "match.h"
#include "amindex.h"
#include "disk_history.h"
#include "list_dir.h"
#include "logfile.h"
#include "find.h"
#include "tapefile.h"
#include "util.h"
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
char *qdisk_name = NULL;			/* disk we are restoring */
static char *target_date = NULL;
static disklist_t disk_list;			/* all disks in cur config */
static find_result_t *output_find = NULL;
static g_option_t *g_options = NULL;

static int amindexd_debug = 0;

static REMOVE_ITEM *uncompress_remove = NULL;
					/* uncompressed files to remove */

static am_feature_t *our_features = NULL;
static am_feature_t *their_features = NULL;

static int get_pid_status(int pid, char *program, GPtrArray **emsg);
static REMOVE_ITEM *remove_files(REMOVE_ITEM *);
static char *uncompress_file(char *, GPtrArray **);
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
			    char *diskname, char *timestamps, int level);
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
	msg = vstrallocf(
		_("%s (%d) returned negative value: %s"),
		program, pid, strerror(errno));
	dbprintf("%s\n", msg);
	g_ptr_array_add(*emsg, msg);
	result = 0;
    } else {
	if (!WIFEXITED(wait_status)) {
	    msg = vstrallocf(
			_("%s exited with signal %d"),
			program, WTERMSIG(wait_status));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    result = 0;
	} else if (WEXITSTATUS(wait_status) != 0) {
	    msg = vstrallocf(
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

    while(remove) {
	dbprintf(_("removing index file: %s\n"), remove->filename);
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
    char       *filename_gz,
    GPtrArray **emsg)
{
    char *cmd = NULL;
    char *filename = NULL;
    struct stat stat_filename;
    int result;
    size_t len;
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
	    msg = vstrallocf(_("Compressed file '%s' is inaccessable: %s"),
			     filename_gz, strerror(errno));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    amfree(filename);
	    return NULL;
 	}

#ifdef UNCOMPRESS_OPT
#  define PARAM_UNCOMPRESS_OPT UNCOMPRESS_OPT
#else
#  define PARAM_UNCOMPRESS_OPT skip_argument
#endif

	nullfd = open("/dev/null", O_RDONLY);

	indexfd = open(filename,O_WRONLY|O_CREAT, 0600);
	if (indexfd == -1) {
	    msg = vstrallocf(_("Can't open '%s' for writting: %s"),
			      filename, strerror(errno));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    amfree(filename);
	    aclose(nullfd);
	    return NULL;
	}

	/* just use our stderr directly for the pipe's stderr; in 
	 * main() we send stderr to the debug file, or /dev/null
	 * if debugging is disabled */

	/* start the uncompress process */
	putenv(stralloc("LC_ALL=C"));
	pid_gzip = pipespawn(UNCOMPRESS_PATH, STDOUT_PIPE|STDERR_PIPE, 0,
			     &nullfd, &pipe_from_gzip, &uncompress_errfd,
			     UNCOMPRESS_PATH, PARAM_UNCOMPRESS_OPT,
			     filename_gz, NULL);
	aclose(nullfd);

	pipe_stream = fdopen(pipe_from_gzip,"r");
	if(pipe_stream == NULL) {
	    msg = vstrallocf(_("Can't fdopen pipe from gzip: %s"),
			     strerror(errno));
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
	    amfree(filename);
	    aclose(indexfd);
	    return NULL;
	}

	/* start the sort process */
	putenv(stralloc("LC_ALL=C"));
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
	    msg = vstrallocf(
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
	while (fgets(line, sizeof(line), uncompress_err_stream) != NULL) {
	    if (line[strlen(line)-1] == '\n')
		line[strlen(line)-1] = '\0';
	    g_ptr_array_add(uncompress_err, vstrallocf("  %s", line));
	    dbprintf("Uncompress: %s\n", line);
	}
	fclose(uncompress_err_stream);

	sort_err_stream = fdopen(sort_errfd, "r");
	sort_err = g_ptr_array_new();
	while (fgets(line, sizeof(line), sort_err_stream) != NULL) {
	    if (line[strlen(line)-1] == '\n')
		line[strlen(line)-1] = '\0';
	    g_ptr_array_add(sort_err, vstrallocf("  %s", line));
	    dbprintf("Sort: %s\n", line);
	}
	fclose(sort_err_stream);

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

	/* add at beginning */
	if (filename) {
	    remove_file = (REMOVE_ITEM *)alloc(SIZEOF(REMOVE_ITEM));
	    remove_file->filename = stralloc(filename);
	    remove_file->next = uncompress_remove;
	    uncompress_remove = remove_file;
	}

    } else if(!S_ISREG((stat_filename.st_mode))) {
	    msg = vstrallocf(_("\"%s\" is not a regular file"), filename);
	    dbprintf("%s\n", msg);
	    g_ptr_array_add(*emsg, msg);
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
    GPtrArray **emsg)
{
    char line[STR_SIZE], old_line[STR_SIZE];
    char *filename = NULL;
    char *filename_gz;
    char *dir_slash = NULL;
    FILE *fp;
    char *s;
    int ch;
    size_t len_dir_slash;

    old_line[0] = '\0';
    if (strcmp(dir, "/") == 0) {
	dir_slash = stralloc(dir);
    } else {
	dir_slash = stralloc2(dir, "/");
    }

    filename_gz = get_index_name(dump_hostname, dump_item->hostname, disk_name,
				 dump_item->date, dump_item->level);
    if (filename_gz == NULL) {
	g_ptr_array_add(*emsg, stralloc(_("index file not found")));
	amfree(filename_gz);
	return -1;
    }
    filename = uncompress_file(filename_gz, emsg);
    if(filename == NULL) {
	amfree(filename_gz);
	amfree(dir_slash);
	return -1;
    }
    amfree(filename_gz);

    if((fp = fopen(filename,"r"))==0) {
	g_ptr_array_add(*emsg, vstrallocf("%s", strerror(errno)));
	amfree(dir_slash);
        amfree(filename);
	return -1;
    }

    len_dir_slash=strlen(dir_slash);

    while (fgets(line, STR_SIZE, fp) != NULL) {
	if (line[0] != '\0') {
	    if(line[strlen(line)-1] == '\n')
		line[strlen(line)-1] = '\0';
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
		if(strcmp(line, old_line) != 0) {
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
printf_arglist_function1(static void reply, int, n, char *, fmt)
{
    va_list args;
    int len;

    if(!reply_buffer)
	reply_buffer = alloc(reply_buffer_size);

    while(1) {
	arglist_start(args, fmt);
	len = g_vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size-1)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = alloc(reply_buffer_size);
    }

    if (g_fprintf(cmdout,"%03d %s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(_("! error %d (%s) in printf\n"), errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }
    if (fflush(cmdout) != 0)
    {
	dbprintf(_("! error %d (%s) in fflush\n"), errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }
    dbprintf(_("< %03d %s\n"), n, reply_buffer);
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
	len = g_vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size-1)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = alloc(reply_buffer_size);
    }

    if (g_fprintf(cmdout,"%03d-%s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(_("! error %d (%s) in printf\n"),
		  errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }
    if (fflush(cmdout) != 0)
    {
	dbprintf(_("! error %d (%s) in fflush\n"),
		  errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
	exit(1);
    }

    dbprintf("< %03d-%s\n", n, reply_buffer);

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
	len = g_vsnprintf(reply_buffer, reply_buffer_size, fmt, args);
	arglist_end(args);

	if (len > -1 && (size_t)len < reply_buffer_size-1)
	    break;

	reply_buffer_size *= 2;
	amfree(reply_buffer);
	reply_buffer = alloc(reply_buffer_size);
    }

    if (g_fprintf(cmdout,"%03d-%s\r\n", n, reply_buffer) < 0)
    {
	dbprintf(_("! error %d (%s) in printf\n"),
		  errno, strerror(errno));
	uncompress_remove = remove_files(uncompress_remove);
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
    sort_find_result("DLKHpwB", &output_find);

    conf_indexdir = config_dir_relative(getconf_str(CNF_INDEXDIR));
    if (stat (conf_indexdir, &dir_stat) != 0 || !S_ISDIR(dir_stat.st_mode)) {
	reply(501, _("Index directory %s does not exist"), conf_indexdir);
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
    last_filenum = (off_t)-1;
    last_level = -1;
    last_partnum = -1;
    for(find_output = output_find;
	find_output != NULL; 
	find_output = find_output->next) {
	if(strcasecmp(dump_hostname, find_output->hostname) == 0 &&
	   strcmp(disk_name    , find_output->diskname)     == 0 &&
	   strcmp("OK"         , find_output->status)       == 0 &&
	   strcmp("OK"         , find_output->dump_status)  == 0) {
	    /*
	     * The sort order puts holding disk entries first.  We want to
	     * use them if at all possible, so ignore any other entries
	     * for the same datestamp after we see a holding disk entry
	     * (as indicated by a filenum of zero).
	     */
	    if(last_timestamp &&
	       strcmp(find_output->timestamp, last_timestamp) == 0 &&
	       find_output->level == last_level && 
	       last_filenum == 0) {
		continue;
	    }
	    /* ignore duplicate partnum */
	    if(last_timestamp &&
	       strcmp(find_output->timestamp, last_timestamp) == 0 &&
	       find_output->level == last_level && 
	       find_output->partnum == last_partnum) {
		continue;
	    }
	    last_timestamp = find_output->timestamp;
	    last_filenum = find_output->filenum;
	    last_level = find_output->level;
	    last_partnum = find_output->partnum;
	    date = amindexd_nicedate(find_output->timestamp);
	    add_dump(find_output->hostname, date, find_output->level,
		     find_output->label, find_output->filenum,
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
        char *tapelist_str = marshal_tapelist(item->tapes, 1);

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
    char *filename_gz = NULL;
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
	filename_gz = get_index_name(dump_hostname, item->hostname, disk_name,
				     item->date, item->level);
	if (filename_gz == NULL) {
	    reply(599, "index not found");
	    amfree(ldir);
	    return -1;
	}
	emsg = g_ptr_array_new();
	if((filename = uncompress_file(filename_gz, &emsg)) == NULL) {
	    reply_ptr_array(599, emsg);
	    amfree(filename_gz);
	    g_ptr_array_free_full(emsg);
	    amfree(ldir);
	    return -1;
	}
	g_ptr_array_free_full(emsg);
	amfree(filename_gz);
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
	    if(line[strlen(line)-1] == '\n')
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
	tapelist_str = marshal_tapelist(dir_item->dump->tapes, 1);
    } else {
	tapelist_str = dir_item->dump->tapes->label;
    }

    if (am_has_feature(their_features, fe_amindexd_quote_label)) {
	qtapelist_str = quote_string(tapelist_str);
    } else {
	qtapelist_str = stralloc(tapelist_str);
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
    disk_t *diskp;

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
    for (diskp = disk_list.head; diskp != NULL; diskp = diskp->next) {
	if ((strcasecmp(diskp->host->hostname, dump_hostname) == 0)
		&& (strcmp(diskp->name, disk_name) == 0)) {
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
	safe_fd(dbfd(), 1);
    }

    if (argc > 0) {
	cfg_opt = *argv;
	argc--;
	argv++;
    }

    if(gethostname(local_hostname, SIZEOF(local_hostname)-1) == -1) {
	error(_("gethostname: %s"), strerror(errno));
	/*NOTREACHED*/
    }
    local_hostname[SIZEOF(local_hostname)-1] = '\0';

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
		    g_options->hostname = alloc(MAX_HOSTNAME_LENGTH+1);
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
	fflush(stdin);
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
	if (!user_validated && strcmp(cmd, "SECURITY") == 0 && arg) {
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
	    }
	    break;
	}

	if (strcmp(cmd, "QUIT") == 0) {
	    amfree(line);
	    break;
	} else if (strcmp(cmd, "HOST") == 0 && arg) {
	    am_host_t *lhost;
	    /* set host we are restoring */
	    s[-1] = '\0';
	    if ((lhost = is_dump_host_valid(arg)) != NULL)
	    {
		dump_hostname = newstralloc(dump_hostname, lhost->hostname);
		reply(200, _("Dump host set to %s."), dump_hostname);
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
	    if (get_config_name() == NULL) {
		reply(501, _("Must set config before listhost"));
	    }
	    else {
		lreply(200, _(" List hosts for config %s"), get_config_name());
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
		    reply(200, _(" List hosts for config %s"), get_config_name());
		}
		else {
		    reply(200, _("No hosts for config %s"), get_config_name());
		}
	    }
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "DISK") == 0 && arg) {
	    s[-1] = '\0';
	    if (is_disk_valid(arg) != -1) {
		disk_name = newstralloc(disk_name, arg);
		qdisk_name = quote_string(disk_name);
		if (build_disk_table() != -1) {
		    reply(200, _("Disk set to %s."), qdisk_name);
		}
	    }
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "DLE") == 0) {
	    disk_t *dp;
	    char *optionstr;
	    char *b64disk;
	    char *l, *ql;

	    dp = lookup_disk(dump_hostname, disk_name);
	    if (dp->line == 0) {
		reply(200, "NODLE");
	    } else {
		GPtrArray *errarray;
		guint      i;

		b64disk = amxml_format_tag("disk", dp->name);
		dp->host->features = their_features;
		errarray = validate_optionstr(dp);
		if (errarray->len > 0) {
		    for (i=0; i < errarray->len; i++) {
			g_debug(_("ERROR: %s:%s %s"),
				dump_hostname, disk_name,
				(char *)g_ptr_array_index(errarray, i));
		    }
		    g_ptr_array_free(errarray, TRUE);
		    reply(200, "NODLE");
		} else {
		    optionstr = xml_optionstr(dp, 0);
		    l = vstralloc("<dle>\n",
			      "  <program>", dp->program, "</program>\n", NULL);
		    if (dp->application) {
			application_t *application;
			char *xml_app;

			application = lookup_application(dp->application);
			g_assert(application != NULL);
			xml_app = xml_application(dp, application,
						  their_features);
			vstrextend(&l, xml_app, NULL);
			amfree(xml_app);
		    }
		    vstrextend(&l, "  ", b64disk, "\n", NULL);
		    if (dp->device) {
			char *b64device = amxml_format_tag("diskdevice",
							   dp->device);
			vstrextend(&l, "  ", b64device, "\n", NULL);
			amfree(b64device);
		    }
		    vstrextend(&l, optionstr, "</dle>\n", NULL);
		    ql = quote_string(l);
		    reply(200, "%s", ql);
		    amfree(optionstr);
		    amfree(l);
		    amfree(ql);
		    amfree(b64disk);
		}
	    }
	} else if (strcmp(cmd, "LISTDISK") == 0) {
	    char *qname;
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
		for (disk = disk_list.head; disk!=NULL; disk = disk->next) {

		    if (strcasecmp(disk->host->hostname, dump_hostname) == 0 &&
		      ((disk->device && strcmp(disk->device, arg) == 0) ||
		      (!disk->device && strcmp(disk->name, arg) == 0))) {
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
		for (disk = disk_list.head; disk!=NULL; disk = disk->next) {
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
	} else if (strcmp(cmd, "SCNF") == 0 && arg) {
	    s[-1] = '\0';
	    if (check_and_load_config(arg) != -1) {    /* try to load the new config */
		amfree(dump_hostname);		/* invalidate any value */
		amfree(qdisk_name);		/* invalidate any value */
		amfree(disk_name);		/* invalidate any value */
		reply(200, _("Config set to %s."), get_config_name());
	    } /* check_and_load_config replies with any failure messages */
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
	    if (!their_features) {
		g_warning("Invalid client feature set '%s'", their_feature_string);
		their_features = am_set_default_feature_set();
	    }
	    reply(200, "FEATURES %s", our_feature_string);
	    amfree(our_feature_string);
	    amfree(their_feature_string);
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "DATE") == 0 && arg) {
	    s[-1] = '\0';
	    target_date = newstralloc(target_date, arg);
	    reply(200, _("Working date set to %s."), target_date);
	    s[-1] = (char)ch;
	} else if (strcmp(cmd, "DHST") == 0) {
	    (void)disk_history_list();
	} else if (strcmp(cmd, "OISD") == 0 && arg) {
	    if (is_dir_valid_opaque(arg) != -1) {
		reply(200, _("\"%s\" is a valid directory"), arg);
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
	    reply(500, _("Command not recognised/incorrect: %s"), cmd);
	}
	amfree(line);
    }
    amfree(arg);
    
    uncompress_remove = remove_files(uncompress_remove);
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
	g_snprintf(nice, SIZEOF(nice), "%4d-%02d-%02d",
		year, month, day);
    }
    else {
	strncpy(atime, &(datestamp[8]), 6);
	atime[6] = '\0';
	numtime = atoi(atime);
	hours = numtime / 10000;
	minutes = (numtime / 100) % 100;
	seconds = numtime % 100;

	g_snprintf(nice, SIZEOF(nice), "%4d-%02d-%02d-%02d-%02d-%02d",
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

    lower_hostname = stralloc(dump_hostname);
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
    char *dump_hostname,
    char *hostname,
    char *diskname,
    char *timestamps,
    int   level)
{
    struct stat  dir_stat;
    char        *fn;
    char        *s;
    char        *lower_hostname;

    lower_hostname = stralloc(dump_hostname);
    for(s=lower_hostname; *s != '\0'; s++)
	*s = tolower(*s);

    fn = getindexfname(dump_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	amfree(lower_hostname);
	return fn;
    }
    amfree(fn);
    if(hostname != NULL) {
	fn = getindexfname(hostname, diskname, timestamps, level);
	if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	    amfree(lower_hostname);
	    return fn;
	}
    }
    amfree(fn);
    fn = getindexfname(lower_hostname, diskname, timestamps, level);
    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	amfree(lower_hostname);
	return fn;
    }
    amfree(fn);
    if(diskname != NULL) {
	fn = getoldindexfname(dump_hostname, diskname, timestamps, level);
	if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	    amfree(lower_hostname);
	    return fn;
	}
	amfree(fn);
	if(hostname != NULL) {
	    fn = getoldindexfname(hostname, diskname, timestamps, level);
	    if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
		amfree(lower_hostname);
		return fn;
	    }
	}
	amfree(fn);
	fn = getoldindexfname(lower_hostname, diskname, timestamps, level);
	if (stat(fn, &dir_stat) == 0 && S_ISREG(dir_stat.st_mode)) {
	    amfree(lower_hostname);
	    return fn;
	}
    }
    amfree(lower_hostname);
    return NULL;
}
