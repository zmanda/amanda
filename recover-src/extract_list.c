/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
 * $Id$
 *
 * implements the "extract" command in amrecover
 */

#include "amanda.h"
#include "match.h"
#include "amrecover.h"
#include "fileheader.h"
#include "dgram.h"
#include "stream.h"
#include "tapelist.h"
#ifdef SAMBA_CLIENT
#include "findpass.h"
#endif
#include "util.h"
#include "conffile.h"
#include "protocol.h"
#include "event.h"
#include "client_util.h"
#include "security.h"
#include "pipespawn.h"

typedef struct EXTRACT_LIST_ITEM {
    char *path;
    char *tpath;
    struct EXTRACT_LIST_ITEM *next;
}
EXTRACT_LIST_ITEM;

typedef struct EXTRACT_LIST {
    char *date;			/* date tape created */
    int  level;			/* level of dump */
    char *tape;			/* tape label */
    off_t fileno;		/* fileno on tape */
    EXTRACT_LIST_ITEM *files;	/* files to get off tape */

    struct EXTRACT_LIST *next;
}
EXTRACT_LIST;

typedef struct ctl_data_s {
  int                      header_done;
  int                      child_pipe[2];
  int                      pid;
  EXTRACT_LIST            *elist;
  dumpfile_t               file;
  data_path_t              data_path;
  char                    *addrs;
  backup_support_option_t *bsu;
  gint64                   bytes_read;
} ctl_data_t;

#define SKIP_TAPE 2
#define RETRY_TAPE 3

static struct {
    const char *name;
    security_stream_t *fd;
} amidxtaped_streams[] = {
#define CTLFD  0
    { "CTL", NULL },
#define DATAFD  1
    { "DATA", NULL },
};
#define NSTREAMS  (int)(sizeof(amidxtaped_streams) / sizeof(amidxtaped_streams[0]))


static void amidxtaped_response(void *, pkt_t *, security_handle_t *);
static void stop_amidxtaped(void);
static char *dump_device_name = NULL;
static char *errstr;
static char *amidxtaped_line = NULL;
extern char *localhost;
static char header_buf[32768];
static int  header_size = 0;


/* global pid storage for interrupt handler */
pid_t extract_restore_child_pid = -1;

static EXTRACT_LIST *extract_list = NULL;
static const security_driver_t *amidxtaped_secdrv;

unsigned short samba_extract_method = SAMBA_TAR;

#define READ_TIMEOUT	240*60

EXTRACT_LIST *first_tape_list(void);
EXTRACT_LIST *next_tape_list(EXTRACT_LIST *list);
static int is_empty_dir(char *fname);
int is_extract_list_nonempty(void);
int length_of_tape_list(EXTRACT_LIST *tape_list);
void add_file(char *path, char *regex);
void add_glob(char *glob);
void add_regex(char *regex);
void clear_extract_list(void);
void clean_tape_list(EXTRACT_LIST *tape_list);
void clean_extract_list(void);
void check_file_overwrite(char *filename);
void delete_file(char *path, char *regex);
void delete_glob(char *glob);
void delete_regex(char *regex);
void delete_tape_list(EXTRACT_LIST *tape_list);
void display_extract_list(char *file);
void extract_files(void);
void read_file_header(char *buffer,
			dumpfile_t *file,
			size_t buflen,
			int tapedev);
static int add_extract_item(DIR_ITEM *ditem);
static int delete_extract_item(DIR_ITEM *ditem);
static int extract_files_setup(char *label, off_t fsf);
static int okay_to_continue(int allow_tape,
			int allow_skip,
			int allow_retry);
static ssize_t read_buffer(int datafd,
			char *buffer,
			size_t buflen,
			long timeout_s);
static void clear_tape_list(EXTRACT_LIST *tape_list);
static void extract_files_child(ctl_data_t *ctl_data);
static void send_to_tape_server(security_stream_t *stream, char *cmd);
int writer_intermediary(EXTRACT_LIST *elist);
int get_amidxtaped_line(void);
static void read_amidxtaped_data(void *, void *, ssize_t);
static char *merge_path(char *path1, char *path2);
static gboolean ask_file_overwrite(ctl_data_t *ctl_data);
static void start_processing_data(ctl_data_t *ctl_data);

/*
 * Function:  ssize_t read_buffer(datafd, buffer, buflen, timeout_s)
 *
 * Description:
 *	read data from input file desciptor waiting up to timeout_s
 *	seconds before returning data.
 *
 * Inputs:
 *	datafd    - File descriptor to read from.
 *	buffer    - Buffer to read into.
 *	buflen    - Maximum number of bytes to read into buffer.
 *	timeout_s - Seconds to wait before returning what was already read.
 *
 * Returns:
 *      >0	  - Number of data bytes in buffer.
 *       0	  - EOF
 *      -1        - errno == ETIMEDOUT if no data available in specified time.
 *                  errno == ENFILE if datafd is invalid.
 *                  otherwise errno is set by select or read..
 */

static ssize_t
read_buffer(
    int		datafd,
    char *	buffer,
    size_t	buflen,
    long	timeout_s)
{
    ssize_t size = 0;
    SELECT_ARG_TYPE readset;
    struct timeval timeout;
    char *dataptr;
    ssize_t spaceleft;
    int nfound;

    if(datafd < 0 || datafd >= (int)FD_SETSIZE) {
	errno = EMFILE;					/* out of range */
	return -1;
    }

    dataptr = buffer;
    spaceleft = (ssize_t)buflen;

    do {
        FD_ZERO(&readset);
        FD_SET(datafd, &readset);
        timeout.tv_sec = timeout_s;
        timeout.tv_usec = 0;
        nfound = select(datafd+1, &readset, NULL, NULL, &timeout);
        if(nfound < 0 ) {
            /* Select returned an error. */
	    g_fprintf(stderr,_("select error: %s\n"), strerror(errno));
            size = -1;
	    break;
        }

	if (nfound == 0) {
            /* Select timed out. */
            if (timeout_s != 0)  {
                /* Not polling: a real read timeout */
                g_fprintf(stderr,_("timeout waiting for restore\n"));
                g_fprintf(stderr,_("increase READ_TIMEOUT in recover-src/extract_list.c if your tape is slow\n"));
            }
            errno = ETIMEDOUT;
            size = -1;
	    break;
        }

	if(!FD_ISSET(datafd, &readset))
	    continue;

        /* Select says data is available, so read it.  */
        size = read(datafd, dataptr, (size_t)spaceleft);
        if (size < 0) {
	    if ((errno == EINTR) || (errno == EAGAIN)) {
		continue;
	    }
	    if (errno != EPIPE) {
	        g_fprintf(stderr, _("read_buffer: read error - %s"),
		    strerror(errno));
	        break;
	    }
	    size = 0;
	}
        spaceleft -= size;
        dataptr += size;
    } while ((size > 0) && (spaceleft > 0));

    return ((((ssize_t)buflen-spaceleft) > 0) ? ((ssize_t)buflen-spaceleft) : size);
}


EXTRACT_LIST *
first_tape_list(void)
{
    return extract_list;
}

EXTRACT_LIST *
next_tape_list(
    /*@keep@*/EXTRACT_LIST *list)
{
    if (list == NULL)
	return NULL;
    return list->next;
}

static void
clear_tape_list(
    EXTRACT_LIST *	tape_list)
{
    EXTRACT_LIST_ITEM *this, *next;


    this = tape_list->files;
    while (this != NULL)
    {
	next = this->next;
        amfree(this->path);
        amfree(this->tpath);
        amfree(this);
	this = next;
    }
    tape_list->files = NULL;
}


/* remove a tape list from the extract list, clearing the tape list
   beforehand if necessary */
void
delete_tape_list(
    EXTRACT_LIST *tape_list)
{
    EXTRACT_LIST *this, *prev;

    if (tape_list == NULL)
        return;

    /* is it first on the list? */
    if (tape_list == extract_list)
    {
	extract_list = tape_list->next;
	clear_tape_list(tape_list);
        amfree(tape_list->date);
        amfree(tape_list->tape);
	amfree(tape_list);
	return;
    }

    /* so not first on list - find it and delete */
    prev = extract_list;
    this = extract_list->next;
    while (this != NULL)
    {
	if (this == tape_list)
	{
	    prev->next = tape_list->next;
	    clear_tape_list(tape_list);
            amfree(tape_list->date);
            amfree(tape_list->tape);
	    amfree(tape_list);
	    return;
	}
	prev = this;
	this = this->next;
    }
    /*NOTREACHED*/
}


/* return the number of files on a tape's list */
int
length_of_tape_list(
    EXTRACT_LIST *tape_list)
{
    EXTRACT_LIST_ITEM *fn;
    int n;

    n = 0;
    for (fn = tape_list->files; fn != NULL; fn = fn->next)
	n++;

    return n;
}


void
clear_extract_list(void)
{
    while (extract_list != NULL)
	delete_tape_list(extract_list);
}


void
clean_tape_list(
    EXTRACT_LIST *tape_list)
{
    EXTRACT_LIST_ITEM *fn1, *pfn1, *ofn1;
    EXTRACT_LIST_ITEM *fn2, *pfn2, *ofn2;
    int remove_fn1;
    int remove_fn2;

    pfn1 = NULL;
    fn1 = tape_list->files;
    while (fn1 != NULL) {
	remove_fn1 = 0;

	pfn2 = fn1;
	fn2 = fn1->next;
	while (fn2 != NULL && remove_fn1 == 0) {
	    remove_fn2 = 0;
	    if(strcmp(fn1->path, fn2->path) == 0) {
		remove_fn2 = 1;
	    } else if (strncmp(fn1->path, fn2->path, strlen(fn1->path)) == 0 &&
		       ((strlen(fn2->path) > strlen(fn1->path) &&
			 fn2->path[strlen(fn1->path)] == '/') ||
		       (fn1->path[strlen(fn1->path)-1] == '/'))) {
		remove_fn2 = 1;
	    } else if (strncmp(fn2->path, fn1->path, strlen(fn2->path)) == 0 &&
		       ((strlen(fn1->path) > strlen(fn2->path) &&
			 fn1->path[strlen(fn2->path)] == '/')  ||
		       (fn2->path[strlen(fn2->path)-1] == '/'))) {
		remove_fn1 = 1;
		break;
	    }

	    if (remove_fn2) {
		dbprintf(_("removing path %s, it is included in %s\n"),
			  fn2->path, fn1->path);
		ofn2 = fn2;
		fn2 = fn2->next;
		amfree(ofn2->path);
		amfree(ofn2->tpath);
		amfree(ofn2);
		pfn2->next = fn2;
	    } else if (remove_fn1 == 0) {
		pfn2 = fn2;
		fn2 = fn2->next;
	    }
	}

	if(remove_fn1 != 0) {
	    /* fn2->path is always valid */
	    /*@i@*/ dbprintf(_("removing path %s, it is included in %s\n"),
	    /*@i@*/	      fn1->tpath, fn2->tpath);
	    ofn1 = fn1;
	    fn1 = fn1->next;
	    amfree(ofn1->path);
	    amfree(ofn1->tpath);
	    if(pfn1 == NULL) {
		amfree(tape_list->files);
		tape_list->files = fn1;
	    } else {
		amfree(pfn1->next);
		pfn1->next = fn1;
	    }
	} else {
	    pfn1 = fn1;
	    fn1 = fn1->next;
	}
    }
}


static char *
file_of_path(
    char *path,
    char **dir)
{
    char *npath = g_path_get_basename(path);
    *dir = g_path_get_dirname(path);
    if (strcmp(*dir, ".") == 0) {
	amfree(*dir);
    }
    return npath;
}

void
clean_extract_list(void)
{
    EXTRACT_LIST *this;

    for (this = extract_list; this != NULL; this = this->next)
	clean_tape_list(this);
}


int add_to_unlink_list(char *path);
int do_unlink_list(void);
void free_unlink_list(void);

typedef struct s_unlink_list {
    char *path;
    struct s_unlink_list *next;
} t_unlink_list;
t_unlink_list *unlink_list = NULL;

int
add_to_unlink_list(
    char *path)
{
    t_unlink_list *ul;

    if (!unlink_list) {
	unlink_list = alloc(SIZEOF(*unlink_list));
	unlink_list->path = stralloc(path);
	unlink_list->next = NULL;
    } else {
	for (ul = unlink_list; ul != NULL; ul = ul->next) {
	    if (strcmp(ul->path, path) == 0)
		return 0;
	}
	ul = alloc(SIZEOF(*ul));
	ul->path = stralloc(path);
	ul->next = unlink_list;
	unlink_list = ul;
    }
    return 1;
}

int
do_unlink_list(void)
{
    t_unlink_list *ul;
    int ret = 1;

    for (ul = unlink_list; ul != NULL; ul = ul->next) {
	if (unlink(ul->path) < 0) {
	    g_fprintf(stderr,_("Can't unlink %s: %s\n"), ul->path, strerror(errno));
	    ret = 0;
	}
    }
    return ret;
}


void
free_unlink_list(void)
{
    t_unlink_list *ul, *ul1;

    for (ul = unlink_list; ul != NULL; ul = ul1) {
	amfree(ul->path);
	ul1 = ul->next;
	amfree(ul);
    }

    unlink_list = NULL;
}



void
check_file_overwrite(
    char *dir)
{
    EXTRACT_LIST      *this;
    EXTRACT_LIST_ITEM *fn;
    struct stat        stat_buf;
    char              *filename;
    char              *path, *s;

    for (this = extract_list; this != NULL; this = this->next) {
	for (fn = this->files; fn != NULL ; fn = fn->next) {

	    /* Check path component of fn->path */

	    path = stralloc2(dir, fn->path);
	    if (path[strlen(path)-1] == '/') {
		path[strlen(path)-1] = '\0';
	    }

	    s = path + strlen(dir) + 1;
	    while((s = strchr(s, '/'))) {
		*s = '\0';
		if (lstat(path, &stat_buf) == 0) {
		    if(!S_ISDIR(stat_buf.st_mode)) {
			if (add_to_unlink_list(path)) {
			    g_printf(_("WARNING: %s is not a directory, "
				   "it will be deleted.\n"),
				   path);
			}
		    }
		}
		else if (errno != ENOENT) {
		    g_printf(_("Can't stat %s: %s\n"), path, strerror(errno));
		}
		*s = '/';
		s++;
	    }
	    amfree(path);

	    /* Check fn->path */

	    filename = stralloc2(dir, fn->path);
	    if (filename[strlen(filename)-1] == '/') {
		filename[strlen(filename)-1] = '\0';
	    }

	    if (lstat(filename, &stat_buf) == 0) {
		if(S_ISDIR(stat_buf.st_mode)) {
		    if(!is_empty_dir(filename)) {
			g_printf(_("WARNING: All existing files in %s "
			       "will be deleted.\n"), filename);
		    }
		} else if(S_ISREG(stat_buf.st_mode)) {
		    g_printf(_("WARNING: Existing file %s will be overwritten\n"),
			   filename);
		} else {
		    if (add_to_unlink_list(filename)) {
			g_printf(_("WARNING: Existing entry %s will be deleted\n"),
			       filename);
		    }
		}
	    } else if (errno != ENOENT) {
		g_printf(_("Can't stat %s: %s\n"), filename, strerror(errno));
	    }
	    amfree(filename);
	}
    }
}


/* returns -1 if error */
/* returns  0 on succes */
/* returns  1 if already added */
static int
add_extract_item(
    DIR_ITEM *ditem)
{
    EXTRACT_LIST *this, *this1;
    EXTRACT_LIST_ITEM *that, *curr;
    char *ditem_path;

    ditem_path = stralloc(ditem->path);
    clean_pathname(ditem_path);

    for (this = extract_list; this != NULL; this = this->next)
    {
	/* see if this is the list for the tape */	
	if (this->level == ditem->level && strcmp(this->tape, ditem->tape) == 0)
	{
	    /* yes, so add to list */
	    curr=this->files;
	    while(curr!=NULL)
	    {
		if (strcmp(curr->path,ditem_path) == 0) {
		    g_free(ditem_path);
		    return 1;
		}
		curr=curr->next;
	    }
	    that = (EXTRACT_LIST_ITEM *)alloc(sizeof(EXTRACT_LIST_ITEM));
            that->path = ditem_path;
	    that->tpath = clean_pathname(g_strdup(ditem->tpath));
	    that->next = this->files;
	    this->files = that;		/* add at front since easiest */
	    return 0;
	}
    }

    /* so this is the first time we have seen this tape */
    this = (EXTRACT_LIST *)alloc(sizeof(EXTRACT_LIST));
    this->tape = stralloc(ditem->tape);
    this->level = ditem->level;
    this->fileno = ditem->fileno;
    this->date = stralloc(ditem->date);
    that = (EXTRACT_LIST_ITEM *)alloc(sizeof(EXTRACT_LIST_ITEM));
    that->path = ditem_path;
    that->tpath = clean_pathname(g_strdup(ditem->tpath));
    that->next = NULL;
    this->files = that;

    /* add this in date increasing order          */
    /* because restore must be done in this order */
    /* add at begining */
    if(extract_list==NULL || strcmp(this->date,extract_list->date) < 0)
    {
	this->next = extract_list;
	extract_list = this;
	return 0;
    }
    for (this1 = extract_list; this1->next != NULL; this1 = this1->next)
    {
	/* add in the middle */
	if(strcmp(this->date,this1->next->date) < 0)
	{
	    this->next = this1->next;
	    this1->next = this;
	    return 0;
	}
    }
    /* add at end */
    this->next = NULL;
    this1->next = this;
    return 0;
}


/* returns -1 if error */
/* returns  0 on deletion */
/* returns  1 if not there */
static int
delete_extract_item(
    DIR_ITEM *ditem)
{
    EXTRACT_LIST *this;
    EXTRACT_LIST_ITEM *that, *prev;
    char *ditem_path = NULL;

    ditem_path = stralloc(ditem->path);
    clean_pathname(ditem_path);

    for (this = extract_list; this != NULL; this = this->next)
    {
	/* see if this is the list for the tape */	
	if (this->level == ditem->level && strcmp(this->tape, ditem->tape) == 0)
	{
	    /* yes, so find file on list */
	    that = this->files;
	    if (strcmp(that->path, ditem_path) == 0)
	    {
		/* first on list */
		this->files = that->next;
                amfree(that->path);
                amfree(that->tpath);
		amfree(that);
		/* if list empty delete it */
		if (this->files == NULL)
		    delete_tape_list(this);
		amfree(ditem_path);
		return 0;
	    }
	    prev = that;
	    that = that->next;
	    while (that != NULL)
	    {
		if (strcmp(that->path, ditem_path) == 0)
		{
		    prev->next = that->next;
                    amfree(that->path);
                    amfree(that->tpath);
		    amfree(that);
		    amfree(ditem_path);
		    return 0;
		}
		prev = that;
		that = that->next;
	    }
	    amfree(ditem_path);
	    return 1;
	}
    }

    amfree(ditem_path);
    return 1;
}

static char *
merge_path(
    char *path1,
    char *path2)
{
    char *result;
    int len = strlen(path1);
    if (path1[len-1] == '/' && path2[0] == '/') {
	result = stralloc2(path1, path2+1);
    } else if (path1[len-1] != '/' && path2[0] != '/') {
	result = vstralloc(path1, "/", path2, NULL);
    } else {
	result = stralloc2(path1, path2);
    }
    return result;
}

void
add_glob(
    char *	glob)
{
    char *regex;
    char *regex_path;
    char *s;
    char *uqglob;
    char *dir;
    char *sdir = NULL;
    int   result = 1;

    if (disk_path == NULL) {
	g_printf(_("Must select directory before adding files\n"));
	return;
    }

    uqglob = unquote_string(glob);
    glob = file_of_path(uqglob, &dir);
    if (dir) {
	sdir = merge_path(mount_point, disk_path);
	result = cd_glob(dir, 0);
	amfree(dir);
    }
    if (result) {
	regex = glob_to_regex(glob);
	dbprintf(_("add_glob (%s) -> %s\n"), uqglob, regex);
	if ((s = validate_regexp(regex)) != NULL) {
	    g_printf(_("%s is not a valid shell wildcard pattern: "), glob);
	    puts(s);
	} else {
            /*
             * glob_to_regex() anchors the beginning of the pattern with ^,
             * but we will be tacking it onto the end of the current directory
             * in add_file, so strip that off.  Also, it anchors the end with
             * $, but we need to match an optional trailing /, so tack that on
             * the end.
             */
            regex_path = stralloc(regex + 1);
            regex_path[strlen(regex_path) - 1] = '\0';
            strappend(regex_path, "[/]*$");
            add_file(uqglob, regex_path);
            amfree(regex_path);
	}
	if (sdir) {
	    set_directory(sdir, 0);
	}
	amfree(regex);
    }
    amfree(sdir);
    amfree(uqglob);
    amfree(glob);
}

void
add_regex(
    char *	regex)
{
    char *s;
    char *dir;
    char *sdir = NULL;
    char *uqregex;
    char *newregex;
    int   result = 1;

    if (disk_path == NULL) {
	g_printf(_("Must select directory before adding files\n"));
	return;
    }

    uqregex = unquote_string(regex);
    newregex = file_of_path(uqregex, &dir);
    if (dir) {
	sdir = merge_path(mount_point, disk_path);
	result = cd_regex(dir, 0);
	amfree(dir);
    }

    if (result) { 
	if ((s = validate_regexp(newregex)) != NULL) {
	    g_printf(_("\"%s\" is not a valid regular expression: "), newregex);
	    puts(s);
	} else {
            add_file(uqregex, newregex);
	}
	if (sdir) {
	    set_directory(sdir, 0);
	}
    }
    amfree(sdir);
    amfree(uqregex);
    amfree(newregex);
}

void
add_file(
    char *	path,
    char *	regex)
{
    DIR_ITEM *ditem, lditem;
    char *tpath_on_disk = NULL;
    char *cmd = NULL;
    char *err = NULL;
    int i;
    ssize_t j;
    char *dir_undo, dir_undo_ch = '\0';
    char *ditem_path = NULL;
    char *qditem_path = NULL;
    char *l = NULL;
    int  added;
    char *s, *fp, *quoted;
    int ch;
    int found_one;
    int dir_entries;

    if (disk_path == NULL) {
	g_printf(_("Must select directory before adding files\n"));
	return;
    }
    memset(&lditem, 0, sizeof(lditem)); /* Prevent use of bogus data... */

    dbprintf(_("add_file: Looking for \"%s\"\n"), regex);

    if(strcmp(regex, "/[/]*$") == 0) {	/* "/" behave like "." */
	regex = "\\.[/]*$";
    }
    else if(strcmp(regex, "[^/]*[/]*$") == 0) {		/* "*" */
	regex = "([^/.]|\\.[^/]+|[^/.][^/]*)[/]*$";
    } else {
	/* remove "/" at end of path */
	j = (ssize_t)(strlen(regex) - 1);
	while(j >= 0 && regex[j] == '/')
	    regex[j--] = '\0';
    }

    /* convert path (assumed in cwd) to one on disk */
    if (strcmp(disk_path, "/") == 0) {
        if (*regex == '/') {
	    /* No mods needed if already starts with '/' */
	    tpath_on_disk = g_strdup(regex);
	} else {
	    /* Prepend '/' */
	    tpath_on_disk = g_strconcat("/", regex, NULL);
	}
    } else {
	char *clean_disk_tpath = clean_regex(disk_tpath, 0);
	tpath_on_disk = g_strjoin(NULL, clean_disk_tpath, "/", regex, NULL);
	amfree(clean_disk_tpath);
    }

    dbprintf(_("add_file: Converted path=\"%s\" to tpath_on_disk=\"%s\"\n"),
	      regex, tpath_on_disk);

    found_one = 0;
    dir_entries = 0;
    for (ditem=get_dir_list(); ditem!=NULL; ditem=get_next_dir_item(ditem))
    {
	dir_entries++;
	quoted = quote_string(ditem->tpath);
	dbprintf(_("add_file: Pondering ditem->path=%s\n"), quoted);
	amfree(quoted);
	if (match(tpath_on_disk, ditem->tpath))
	{
	    found_one = 1;
	    j = (ssize_t)strlen(ditem->tpath);
	    if((j > 0 && ditem->tpath[j-1] == '/')
	       || (j > 1 && ditem->tpath[j-2] == '/' && ditem->tpath[j-1] == '.'))
	    {	/* It is a directory */
		ditem_path = newstralloc(ditem_path, ditem->path);
		clean_pathname(ditem_path);

		qditem_path = quote_string(ditem_path);
		cmd = newstralloc2(cmd, "ORLD ", qditem_path);
		amfree(qditem_path);
		if(send_command(cmd) == -1) {
		    amfree(cmd);
		    amfree(ditem_path);
		    amfree(tpath_on_disk);
		    exit(1);
		}
		amfree(cmd);
		cmd = NULL;
		/* skip preamble */
		if ((i = get_reply_line()) == -1) {
		    amfree(ditem_path);
		    amfree(tpath_on_disk);
		    exit(1);
		}
		if(i==0) {		/* assume something wrong */
		    amfree(ditem_path);
		    amfree(tpath_on_disk);
		    l = reply_line();
		    g_printf("%s\n", l);
		    return;
		}
		dir_undo = NULL;
		added=0;
		g_free(lditem.path);
		g_free(lditem.tpath);
                lditem.path = g_strdup(ditem->path);
                lditem.tpath = g_strdup(ditem->tpath);
		/* skip the last line -- duplicate of the preamble */

		while ((i = get_reply_line()) != 0) {
		    if (i == -1) {
			amfree(ditem_path);
		        amfree(tpath_on_disk);
			exit(1);
		    }
		    if(err) {
			if(cmd == NULL) {
			    if(dir_undo) *dir_undo = dir_undo_ch;
			    dir_undo = NULL;
			    cmd = stralloc(l);	/* save for error report */
			}
			continue;	/* throw the rest of the lines away */
		    }
		    l=reply_line();
		    if (!server_happy()) {
			puts(l);
			continue;
		    }

		    s = l;
		    if(strncmp_const_skip(l, "201-", s, ch) != 0) {
			err = _("bad reply: not 201-");
			continue;
		    }
		    ch = *s++;

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = _("bad reply: missing date field");
			continue;
		    }
                    fp = s-1;
                    skip_non_whitespace(s, ch);
                    s[-1] = '\0';
                    lditem.date = newstralloc(lditem.date, fp);
                    s[-1] = (char)ch;

		    skip_whitespace(s, ch);
		    if(ch == '\0' || sscanf(s - 1, "%d", &lditem.level) != 1) {
			err = _("bad reply: cannot parse level field");
			continue;
		    }
		    skip_integer(s, ch);

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = _("bad reply: missing tape field");
			continue;
		    }
                    fp = s-1;
                    skip_quoted_string(s, ch);
                    s[-1] = '\0';
		    amfree(lditem.tape);
		    lditem.tape = unquote_string(fp);
                    s[-1] = (char)ch;

		    if(am_has_feature(indexsrv_features, fe_amindexd_fileno_in_ORLD)) {
			long long fileno_ = (long long)0;
			skip_whitespace(s, ch);
			if(ch == '\0' ||
			   sscanf(s - 1, "%lld", &fileno_) != 1) {
			    err = _("bad reply: cannot parse fileno field");
			    continue;
			}
			lditem.fileno = (off_t)fileno_;
			skip_integer(s, ch);
		    }

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = _("bad reply: missing directory field");
			continue;
		    }
		    skip_quoted_string(s, ch);
		    dir_undo = s - 1;
		    dir_undo_ch = *dir_undo;
		    *dir_undo = '\0';

		    switch(add_extract_item(&lditem)) {
		    case -1:
			g_printf(_("System error\n"));
			dbprintf(_("add_file: (Failed) System error\n"));
			break;

		    case  0:
			quoted = quote_string(lditem.tpath);
			g_printf(_("Added dir %s at date %s\n"),
			       quoted, lditem.date);
			dbprintf(_("add_file: (Successful) Added dir %s at date %s\n"),
				  quoted, lditem.date);
			amfree(quoted);
			added=1;
			break;

		    case  1:
			break;
		    }
		}
		if(!server_happy()) {
		    puts(reply_line());
		} else if(err) {
		    if (*err)
			puts(err);
		    if (cmd)
			puts(cmd);
		} else if(added == 0) {
		    quoted = quote_string(ditem_path);
		    g_printf(_("dir %s already added\n"), quoted);
		    dbprintf(_("add_file: dir %s already added\n"), quoted);
		    amfree(quoted);
		}
	    }
	    else /* It is a file */
	    {
		switch(add_extract_item(ditem)) {
		case -1:
		    g_printf(_("System error\n"));
		    dbprintf(_("add_file: (Failed) System error\n"));
		    break;

		case  0:
		    quoted = quote_string(ditem->tpath);
		    g_printf(_("Added file %s\n"), quoted);
		    dbprintf(_("add_file: (Successful) Added %s\n"), quoted);
		    amfree(quoted);
		    break;

		case  1:
		    quoted = quote_string(ditem->tpath);
		    g_printf(_("File %s already added\n"), quoted);
		    dbprintf(_("add_file: file %s already added\n"), quoted);
		    amfree(quoted);
		}
	    }
	}
    }

    amfree(cmd);
    amfree(ditem_path);
    amfree(tpath_on_disk);

    amfree(lditem.path);
    amfree(lditem.tpath);
    amfree(lditem.date);
    amfree(lditem.tape);

    if(! found_one) {
	quoted = quote_string(path);
	g_printf(_("File %s doesn't exist in directory\n"), quoted);
	dbprintf(_("add_file: (Failed) File %s doesn't exist in directory\n"),
	          quoted);
	amfree(quoted);
    }
}


void
delete_glob(
    char *	glob)
{
    char *regex;
    char *regex_path;
    char *s;
    char *uqglob;
    char *newglob;
    char *dir;
    char *sdir = NULL;
    int   result = 1;

    if (disk_path == NULL) {
	g_printf(_("Must select directory before adding files\n"));
	return;
    }

    uqglob = unquote_string(glob);
    newglob = file_of_path(uqglob, &dir);
    if (dir) {
	sdir = merge_path(mount_point, disk_path);
	result = cd_glob(dir, 0);
	amfree(dir);
    }
    if (result) {
	regex = glob_to_regex(newglob);
	dbprintf(_("delete_glob (%s) -> %s\n"), newglob, regex);
	if ((s = validate_regexp(regex)) != NULL) {
	    g_printf(_("\"%s\" is not a valid shell wildcard pattern: "),
		     newglob);
	    puts(s);
} else {
            /*
             * glob_to_regex() anchors the beginning of the pattern with ^,
             * but we will be tacking it onto the end of the current directory
             * in add_file, so strip that off.  Also, it anchors the end with
             * $, but we need to match an optional trailing /, so tack that on
             * the end.
             */
            regex_path = stralloc(regex + 1);
            regex_path[strlen(regex_path) - 1] = '\0';
            strappend(regex_path, "[/]*$");
            delete_file(uqglob, regex_path);
            amfree(regex_path);
	}
	if (sdir) {
	    set_directory(sdir, 0);
	}
	amfree(regex);
    }
    amfree(sdir);
    amfree(uqglob);
    amfree(newglob);
}

void
delete_regex(
    char *	regex)
{
    char *s;
    char *dir;
    char *sdir = NULL;
    char *uqregex;
    char *newregex;
    int   result = 1;

    if (disk_path == NULL) {
	g_printf(_("Must select directory before adding files\n"));
	return;
    }

    uqregex = unquote_string(regex);
    newregex = file_of_path(uqregex, &dir);
    if (dir) {
	sdir = merge_path(mount_point, disk_path);
	result = cd_regex(dir, 0);
	amfree(dir);
    }

    if (result == 1) {
	if ((s = validate_regexp(newregex)) != NULL) {
	    g_printf(_("\"%s\" is not a valid regular expression: "), newregex);
	    puts(s);
	} else {
	    delete_file(newregex, regex);
	}
	if (sdir) {
	    set_directory(sdir, 0);
	}
    }
    amfree(sdir);
    amfree(uqregex);
    amfree(newregex);
}

void
delete_file(
    char *	tpath,
    char *	regex)
{
    DIR_ITEM *ditem, lditem;
    char *tpath_on_disk = NULL;
    char *cmd = NULL;
    char *err = NULL;
    int i;
    ssize_t j;
    char *date;
    char *tape, *tape_undo, tape_undo_ch = '\0';
    char *dir_undo, dir_undo_ch = '\0';
    int  level = 0;
    char *ditem_path = NULL;
    char *ditem_tpath = NULL;
    char *qditem_path;
    char *l = NULL;
    int  deleted;
    char *s;
    int ch;
    int found_one;
    char *quoted;

    if (disk_path == NULL) {
	g_printf(_("Must select directory before deleting files\n"));
	return;
    }
    memset(&lditem, 0, sizeof(lditem)); /* Prevent use of bogus data... */

    dbprintf(_("delete_file: Looking for \"%s\"\n"), tpath);

    if (strcmp(regex, "[^/]*[/]*$") == 0) {
	/* Looking for * find everything but single . */
	regex = "([^/.]|\\.[^/]+|[^/.][^/]*)[/]*$";
    } else {
	/* remove "/" at end of path */
	j = (ssize_t)(strlen(regex) - 1);
	while(j >= 0 && regex[j] == '/') regex[j--] = '\0';
    }

    /* convert path (assumed in cwd) to one on disk */
    if (strcmp(disk_path, "/") == 0) {
        if (*regex == '/') {
	    if (strcmp(regex, "/[/]*$") == 0) {
		/* We want "/" to match the directory itself: "/." */
		tpath_on_disk = stralloc("/\\.[/]*$");
	    } else {
		/* No mods needed if already starts with '/' */
		tpath_on_disk = stralloc(regex);
	    }
	} else {
	    /* Prepend '/' */
	    tpath_on_disk = g_strconcat("/", regex, NULL);
	}
    } else {
	char *clean_disk_tpath = clean_regex(disk_tpath, 0);
	tpath_on_disk = g_strjoin(NULL, clean_disk_tpath, "/", regex, NULL);
	amfree(clean_disk_tpath);
    }

    dbprintf(_("delete_file: Converted path=\"%s\" to tpath_on_disk=\"%s\"\n"),
	      regex, tpath_on_disk);
    found_one = 0;
    for (ditem=get_dir_list(); ditem!=NULL; ditem=get_next_dir_item(ditem))
    {
	quoted = quote_string(ditem->tpath);
	dbprintf(_("delete_file: Pondering ditem->path=%s\n"), quoted);
	amfree(quoted);
	if (match(tpath_on_disk, ditem->tpath))
	{
	    found_one = 1;
	    j = (ssize_t)strlen(ditem->tpath);
	    if((j > 0 && ditem->tpath[j-1] == '/')
	       || (j > 1 && ditem->tpath[j-2] == '/' && ditem->tpath[j-1] == '.'))
	    {	/* It is a directory */
		ditem_path = newstralloc(ditem_path, ditem->path);
		ditem_tpath = newstralloc(ditem_tpath, ditem->tpath);
		clean_pathname(ditem_path);
		clean_pathname(ditem_tpath);

		qditem_path = quote_string(ditem_path);
		cmd = newstralloc2(cmd, "ORLD ", qditem_path);
		amfree(qditem_path);
		if(send_command(cmd) == -1) {
		    amfree(cmd);
		    amfree(ditem_path);
		    amfree(tpath_on_disk);
		    exit(1);
		}
		amfree(cmd);
		/* skip preamble */
		if ((i = get_reply_line()) == -1) {
		    amfree(ditem_path);
		    amfree(tpath_on_disk);
		    exit(1);
		}
		if(i==0)		/* assume something wrong */
		{
		    amfree(ditem_path);
		    amfree(tpath_on_disk);
		    l = reply_line();
		    g_printf("%s\n", l);
		    return;
		}
		deleted=0;
                lditem.path = newstralloc(lditem.path, ditem->path);
                lditem.tpath = newstralloc(lditem.tpath, ditem->tpath);
		amfree(cmd);
		tape_undo = dir_undo = NULL;
		/* skip the last line -- duplicate of the preamble */
		while ((i = get_reply_line()) != 0)
		{
		    if (i == -1) {
			amfree(ditem_path);
			amfree(tpath_on_disk);
			exit(1);
		    }
		    if(err) {
			if(cmd == NULL) {
			    if(tape_undo) *tape_undo = tape_undo_ch;
			    if(dir_undo) *dir_undo = dir_undo_ch;
			    tape_undo = dir_undo = NULL;
			    cmd = stralloc(l);	/* save for the error report */
			}
			continue;	/* throw the rest of the lines away */
		    }
		    l=reply_line();
		    if (!server_happy()) {
			puts(l);
			continue;
		    }

		    s = l;
		    if(strncmp_const_skip(l, "201-", s, ch) != 0) {
			err = _("bad reply: not 201-");
			continue;
		    }
		    ch = *s++;

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = _("bad reply: missing date field");
			continue;
		    }
		    date = s - 1;
		    skip_non_whitespace(s, ch);
		    *(s - 1) = '\0';

		    skip_whitespace(s, ch);
		    if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
			err = _("bad reply: cannot parse level field");
			continue;
		    }
		    skip_integer(s, ch);

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = _("bad reply: missing tape field");
			continue;
		    }
		    tape = s - 1;
		    skip_non_whitespace(s, ch);
		    tape_undo = s - 1;
		    tape_undo_ch = *tape_undo;
		    *tape_undo = '\0';

		    if(am_has_feature(indexsrv_features, fe_amindexd_fileno_in_ORLD)) {
			long long fileno_ = (long long)0;
			skip_whitespace(s, ch);
			if(ch == '\0' ||
			   sscanf(s - 1, "%lld", &fileno_) != 1) {
			    err = _("bad reply: cannot parse fileno field");
			    continue;
			}
			skip_integer(s, ch);
		    }

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = _("bad reply: missing directory field");
			continue;
		    }
		    skip_non_whitespace(s, ch);
		    dir_undo = s - 1;
		    dir_undo_ch = *dir_undo;
		    *dir_undo = '\0';

                    lditem.date = newstralloc(lditem.date, date);
		    lditem.level=level;
		    g_free(lditem.tape);
		    lditem.tape = unquote_string(tape);
		    switch(delete_extract_item(&lditem)) {
		    case -1:
			g_printf(_("System error\n"));
			dbprintf(_("delete_file: (Failed) System error\n"));
			break;
		    case  0:
			g_printf(_("Deleted dir %s at date %s\n"), ditem_tpath, date);
			dbprintf(_("delete_file: (Successful) Deleted dir %s at date %s\n"),
				  ditem_tpath, date);
			deleted=1;
			break;
		    case  1:
			break;
		    }
		}
		if(!server_happy()) {
		    puts(reply_line());
		} else if(err) {
		    if (*err)
			puts(err);
		    if (cmd)
			puts(cmd);
		} else if(deleted == 0) {
		    g_printf(_("Warning - dir '%s' not on tape list\n"),
			   ditem_tpath);
		    dbprintf(_("delete_file: dir '%s' not on tape list\n"),
			      ditem_tpath);
		}
	    }
	    else
	    {
		switch(delete_extract_item(ditem)) {
		case -1:
		    g_printf(_("System error\n"));
		    dbprintf(_("delete_file: (Failed) System error\n"));
		    break;
		case  0:
		    g_printf(_("Deleted %s\n"), ditem->tpath);
		    dbprintf(_("delete_file: (Successful) Deleted %s\n"),
			      ditem->tpath);
		    break;
		case  1:
		    g_printf(_("Warning - file '%s' not on tape list\n"),
			   ditem->tpath);
		    dbprintf(_("delete_file: file '%s' not on tape list\n"),
			      ditem->tpath);
		    break;
		}
	    }
	}
    }
    amfree(cmd);
    amfree(ditem_path);
    amfree(ditem_tpath);
    amfree(tpath_on_disk);

    if(! found_one) {
	g_printf(_("File %s doesn't exist in directory\n"), tpath);
	dbprintf(_("delete_file: (Failed) File %s doesn't exist in directory\n"),
	          tpath);
    }
}


/* print extract list into file. If NULL ptr passed print to screen */
void
display_extract_list(
    char *	file)
{
    EXTRACT_LIST *this;
    EXTRACT_LIST_ITEM *that;
    FILE *fp;
    char *pager;
    char *pager_command;
    char *uqfile;

    if (file == NULL)
    {
	if ((pager = getenv("PAGER")) == NULL)
	{
	    pager = "more";
	}
	/*
	 * Set up the pager command so if the pager is terminated, we do
	 * not get a SIGPIPE back.
	 */
	pager_command = stralloc2(pager, " ; /bin/cat > /dev/null");
	if ((fp = popen(pager_command, "w")) == NULL)
	{
	    g_printf(_("Warning - can't pipe through %s\n"), pager);
	    fp = stdout;
	}
	amfree(pager_command);
    }
    else
    {
	uqfile = unquote_string(file);
	if ((fp = fopen(uqfile, "w")) == NULL)
	{
	    g_printf(_("Can't open file %s to print extract list into\n"), file);
	    amfree(uqfile);
	    return;
	}
	amfree(uqfile);
    }

    for (this = extract_list; this != NULL; this = this->next)
    {
	g_fprintf(fp, _("TAPE %s LEVEL %d DATE %s\n"),
		this->tape, this->level, this->date);
	for (that = this->files; that != NULL; that = that->next)
	    g_fprintf(fp, "\t%s\n", that->tpath);
    }

    if (file == NULL) {
	apclose(fp);
    } else {
	g_printf(_("Extract list written to file %s\n"), file);
	afclose(fp);
    }
}


static int
is_empty_dir(
    char *fname)
{
    DIR *dir;
    struct dirent *entry;
    int gotentry;

    if((dir = opendir(fname)) == NULL)
        return 1;

    gotentry = 0;
    while(!gotentry && (entry = readdir(dir)) != NULL) {
        gotentry = !is_dot_or_dotdot(entry->d_name);
    }

    closedir(dir);
    return !gotentry;

}

/* returns 0 if extract list empty and 1 if it isn't */
int
is_extract_list_nonempty(void)
{
    return (extract_list != NULL);
}


/* prints continue prompt and waits for response,
   returns 0 if don't, non-0 if do */
static int
okay_to_continue(
    int	allow_tape,
    int	allow_skip,
    int	allow_retry)
{
    int ch;
    int ret = -1;
    char *line = NULL;
    char *s;
    char *prompt;
    int get_device;

    get_device = 0;
    while (ret < 0) {
	if (get_device) {
	    prompt = _("New device name [?]: ");
	} else if (allow_tape && allow_skip) {
	    prompt = _("Continue [?/Y/n/s/d]? ");
	} else if (allow_tape && !allow_skip) {
	    prompt = _("Continue [?/Y/n/d]? ");
	} else if (allow_retry) {
	    prompt = _("Continue [?/Y/n/r]? ");
	} else {
	    prompt = _("Continue [?/Y/n]? ");
	}
	fputs(prompt, stdout);
	fflush(stdout); fflush(stderr);
	amfree(line);
	if ((line = agets(stdin)) == NULL) {
	    putchar('\n');
	    clearerr(stdin);
	    if (get_device) {
		get_device = 0;
		continue;
	    }
	    ret = 0;
	    break;
	}
	dbprintf("User prompt: '%s'; response: '%s'\n", prompt, line);

	s = line;
	while ((ch = *s++) != '\0' && g_ascii_isspace(ch)) {
	    (void)ch;	/* Quiet empty loop compiler warning */
	}
	if (ch == '?') {
	    if (get_device) {
		g_printf(_("Enter a new device name or \"default\"\n"));
	    } else {
		g_printf(_("Enter \"y\"es to continue, \"n\"o to stop"));
		if(allow_skip) {
		    g_printf(_(", \"s\"kip this tape"));
		}
		if(allow_retry) {
		    g_printf(_(" or \"r\"etry this tape"));
		}
		if (allow_tape) {
		    g_printf(_(" or \"d\" to change to a new device"));
		}
		putchar('\n');
	    }
	} else if (get_device) {
	    char *tmp = stralloc(tape_server_name);

	    if (strncmp_const(s - 1, "default") == 0) {
		set_device(tmp, NULL); /* default device, existing host */
	    } else if (s[-1] != '\0') {
		set_device(tmp, s - 1); /* specified device, existing host */
	    } else {
		g_printf(_("No change.\n"));
	    }

	    amfree(tmp);

	    get_device = 0;
	} else if (ch == '\0' || ch == 'Y' || ch == 'y') {
	    ret = 1;
	} else if (allow_tape && (ch == 'D' || ch == 'd' || ch == 'T' || ch == 't')) {
	    get_device = 1; /* ('T' and 't' are for backward-compatibility) */
	} else if (ch == 'N' || ch == 'n') {
	    ret = 0;
	} else if (allow_retry && (ch == 'R' || ch == 'r')) {
	    ret = RETRY_TAPE;
	} else if (allow_skip && (ch == 'S' || ch == 's')) {
	    ret = SKIP_TAPE;
	}
    }
    /*@ignore@*/
    amfree(line);
    /*@end@*/
    return ret;
}

static void
send_to_tape_server(
    security_stream_t *	stream,
    char *		cmd)
{
    char *msg = stralloc2(cmd, "\r\n");

    g_debug("send_to_tape_server: %s\n", cmd);
    if (security_stream_write(stream, msg, strlen(msg)) < 0)
    {
	error(_("Error writing to tape server"));
	exit(101);
	/*NOTREACHED*/
    }
    amfree(msg);
}


/* start up connection to tape server and set commands to initiate
   transfer of dump image.
   Return tape server socket on success, -1 on error. */
static int
extract_files_setup(
    char *	label,
    off_t	fsf)
{
    char *disk_regex = NULL;
    char *host_regex = NULL;
    char *clean_datestamp, *ch, *ch1;
    char *tt = NULL;
    char *req;
    int response_error;

    amidxtaped_secdrv = security_getdriver(authopt);
    if (amidxtaped_secdrv == NULL) {
	error(_("no '%s' security driver available for host '%s'"),
	      authopt, tape_server_name);
    }

    /* We assume that amidxtaped support fe_amidxtaped_options_features */
    /*                               and fe_amidxtaped_options_auth     */
    /* We should send a noop to really know                             */
    req = vstralloc("SERVICE amidxtaped\n",
		    "OPTIONS ", "features=", our_features_string, ";",
				"auth=", authopt, ";",
		    "\n", NULL);
    protocol_sendreq(tape_server_name, amidxtaped_secdrv,
		     generic_client_get_security_conf, req, STARTUP_TIMEOUT,
		     amidxtaped_response, &response_error);
    amfree(req);
    protocol_run();
    if(response_error != 0) {
	return -1;
    }

    disk_regex = make_exact_disk_expression(disk_name);
    host_regex = make_exact_host_expression(dump_hostname);

    clean_datestamp = stralloc(dump_datestamp);
    for(ch=ch1=clean_datestamp;*ch1 != '\0';ch1++) {
	if(*ch1 != '-') {
	    *ch = *ch1;
	    ch++;
	}
    }
    *ch = '\0';
    /* push our feature list off to the tape server */
    /* XXX assumes that index server and tape server are equivalent, ew */

    if(am_has_feature(indexsrv_features, fe_amidxtaped_exchange_features)){
	tt = newstralloc2(tt, "FEATURES=", our_features_string);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	get_amidxtaped_line();
	if (!amidxtaped_line) {
	    g_fprintf(stderr, _("amrecover - amidxtaped closed the connection\n"));
	    stop_amidxtaped();
	    amfree(disk_regex);
	    amfree(host_regex);
	    amfree(clean_datestamp);
	    return -1;
	} else if(strncmp_const(amidxtaped_line,"FEATURES=") == 0) {
	    tapesrv_features = am_string_to_feature(amidxtaped_line+9);
	} else {
	    g_fprintf(stderr, _("amrecover - expecting FEATURES line from amidxtaped\n"));
	    stop_amidxtaped();
	    amfree(disk_regex);
	    amfree(host_regex);
	    amfree(clean_datestamp);
	    return -1;
	}
    } else {
	*tapesrv_features = *indexsrv_features;
    }


    if(am_has_feature(indexsrv_features, fe_amidxtaped_header) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_device) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_host) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_disk) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_datestamp)) {

	if(am_has_feature(indexsrv_features, fe_amidxtaped_config)) {
	    tt = newstralloc2(tt, "CONFIG=", get_config_name());
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_label) &&
	   label && label[0] != '/') {
	    tt = newstralloc2(tt,"LABEL=",label);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_fsf)) {
	    char v_fsf[100];
	    g_snprintf(v_fsf, 99, "%lld", (long long)fsf);
	    tt = newstralloc2(tt, "FSF=",v_fsf);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "HEADER");
	tt = newstralloc2(tt, "DEVICE=", dump_device_name);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	tt = newstralloc2(tt, "HOST=", host_regex);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	tt = newstralloc2(tt, "DISK=", disk_regex);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	tt = newstralloc2(tt, "DATESTAMP=", clean_datestamp);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "END");
	amfree(tt);
    }
    else if(am_has_feature(indexsrv_features, fe_amidxtaped_nargs)) {
	/* send to the tape server what tape file we want */
	/* 6 args:
	 *   "-h"
	 *   "-p"
	 *   "tape device"
	 *   "hostname"
	 *   "diskname"
	 *   "datestamp"
	 */
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "6");
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "-h");
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "-p");
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, dump_device_name);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, host_regex);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, disk_regex);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, clean_datestamp);

	dbprintf(_("Started amidxtaped with arguments \"6 -h -p %s %s %s %s\"\n"),
		  dump_device_name, host_regex, disk_regex, clean_datestamp);
    }

    amfree(disk_regex);
    amfree(host_regex);
    amfree(clean_datestamp);

    return 0;
}


/*
 * Reads the first block of a tape file.
 */

void
read_file_header(
    char *	buffer,
    dumpfile_t *file,
    size_t	buflen,
    int		tapedev)
{
    ssize_t bytes_read;
    bytes_read = read_buffer(tapedev, buffer, buflen, READ_TIMEOUT);
    if(bytes_read < 0) {
	error(_("error reading header (%s), check amidxtaped.*.debug on server"),
	      strerror(errno));
	/*NOTREACHED*/
    }

    if((size_t)bytes_read < buflen) {
	g_fprintf(stderr, plural(_("%s: short block %d byte\n"),
			       _("%s: short block %d bytes\n"), bytes_read),
		get_pname(), (int)bytes_read);
	print_header(stdout, file);
	error(_("Can't read file header"));
	/*NOTREACHED*/
    }

    /* bytes_read == buflen */
    parse_file_header(buffer, file, (size_t)bytes_read);
}

enum dumptypes {
	IS_UNKNOWN,
	IS_DUMP,
	IS_GNUTAR,
	IS_TAR,
	IS_SAMBA,
	IS_SAMBA_TAR,
	IS_APPLICATION_API
};

static void
extract_files_child(
    ctl_data_t         *ctl_data)
{
    int save_errno;
    int   i;
    guint j;
    GPtrArray *argv_ptr = g_ptr_array_new();
    int files_off_tape;
    EXTRACT_LIST_ITEM *fn;
    enum dumptypes dumptype = IS_UNKNOWN;
    size_t len_program;
    char *cmd = NULL;
    guint passwd_field = 999999999;
#ifdef SAMBA_CLIENT
    char *domain = NULL, *smbpass = NULL;
#endif

    /* code executed by child to do extraction */
    /* never returns */

    /* make in_fd be our stdin */
    if (dup2(ctl_data->child_pipe[0], STDIN_FILENO) == -1)
    {
	error(_("dup2 failed in extract_files_child: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    if(ctl_data->file.type != F_DUMPFILE) {
	dump_dumpfile_t(&ctl_data->file);
	error(_("bad header"));
	/*NOTREACHED*/
    }

    if (ctl_data->file.program != NULL) {
	if (strcmp(ctl_data->file.program, "APPLICATION") == 0)
	    dumptype = IS_APPLICATION_API;
#ifdef GNUTAR
	if (strcmp(ctl_data->file.program, GNUTAR) == 0)
	    dumptype = IS_GNUTAR;
#endif

	if (dumptype == IS_UNKNOWN) {
	    len_program = strlen(ctl_data->file.program);
	    if(len_program >= 3 &&
	       strcmp(&ctl_data->file.program[len_program-3],"tar") == 0)
		dumptype = IS_TAR;
	}

#ifdef SAMBA_CLIENT
	if (dumptype == IS_UNKNOWN && strcmp(ctl_data->file.program, SAMBA_CLIENT) ==0) {
	    if (samba_extract_method == SAMBA_TAR)
	      dumptype = IS_SAMBA_TAR;
	    else
	      dumptype = IS_SAMBA;
	}
#endif
    }

    /* form the arguments to restore */
    files_off_tape = length_of_tape_list(ctl_data->elist);
    switch(dumptype) {
    case IS_SAMBA:
#ifdef SAMBA_CLIENT
	g_ptr_array_add(argv_ptr, stralloc("smbclient"));
	smbpass = findpass(ctl_data->file.disk, &domain);
	if (smbpass) {
	    g_ptr_array_add(argv_ptr, stralloc(ctl_data->file.disk));
	    g_ptr_array_add(argv_ptr, stralloc("-U"));
	    passwd_field = argv_ptr->len;
	    g_ptr_array_add(argv_ptr, stralloc(smbpass));
	    if (domain) {
		g_ptr_array_add(argv_ptr, stralloc("-W"));
		g_ptr_array_add(argv_ptr, stralloc(domain));
	    }
	}
	g_ptr_array_add(argv_ptr, stralloc("-d0"));
	g_ptr_array_add(argv_ptr, stralloc("-Tx"));
	g_ptr_array_add(argv_ptr, stralloc("-"));	/* data on stdin */
	break;
#endif
    case IS_TAR:
    case IS_GNUTAR:
	g_ptr_array_add(argv_ptr, stralloc("tar"));
	/* ignore trailing zero blocks on input (this was the default until tar-1.21) */
	g_ptr_array_add(argv_ptr, stralloc("--ignore-zeros"));
	g_ptr_array_add(argv_ptr, stralloc("--numeric-owner"));
	g_ptr_array_add(argv_ptr, stralloc("-xpGvf"));
	g_ptr_array_add(argv_ptr, stralloc("-"));	/* data on stdin */
	break;
    case IS_SAMBA_TAR:
	g_ptr_array_add(argv_ptr, stralloc("tar"));
	g_ptr_array_add(argv_ptr, stralloc("-xpvf"));
	g_ptr_array_add(argv_ptr, stralloc("-"));	/* data on stdin */
	break;
    case IS_UNKNOWN:
    case IS_DUMP:
	g_ptr_array_add(argv_ptr, stralloc("restore"));
#ifdef AIX_BACKUP
	g_ptr_array_add(argv_ptr, stralloc("-xB"));
#else
#if defined(XFSDUMP)
	if (strcmp(ctl_data->file.program, XFSDUMP) == 0) {
	    g_ptr_array_add(argv_ptr, stralloc("-v"));
	    g_ptr_array_add(argv_ptr, stralloc("silent"));
	} else
#endif
#if defined(VDUMP)
	if (strcmp(ctl_data->file.program, VDUMP) == 0) {
	    g_ptr_array_add(argv_ptr, stralloc("xf"));
	    g_ptr_array_add(argv_ptr, stralloc("-"));	/* data on stdin */
	} else
#endif
	{
	g_ptr_array_add(argv_ptr, stralloc("xbf"));
	g_ptr_array_add(argv_ptr, stralloc("2")); /* read in units of 1K */
	g_ptr_array_add(argv_ptr, stralloc("-"));	/* data on stdin */
	}
#endif
	break;
    case IS_APPLICATION_API:
	g_ptr_array_add(argv_ptr, stralloc(ctl_data->file.application));
	g_ptr_array_add(argv_ptr, stralloc("restore"));
	g_ptr_array_add(argv_ptr, stralloc("--config"));
	g_ptr_array_add(argv_ptr, stralloc(get_config_name()));
	g_ptr_array_add(argv_ptr, stralloc("--disk"));
	g_ptr_array_add(argv_ptr, stralloc(ctl_data->file.disk));
	if (dump_dle && dump_dle->device) {
	    g_ptr_array_add(argv_ptr, stralloc("--device"));
	    g_ptr_array_add(argv_ptr, stralloc(dump_dle->device));
	}
	if (ctl_data->data_path == DATA_PATH_DIRECTTCP) {
	    g_ptr_array_add(argv_ptr, stralloc("--data-path"));
	    g_ptr_array_add(argv_ptr, stralloc("DIRECTTCP"));
	    g_ptr_array_add(argv_ptr, stralloc("--direct-tcp"));
	    g_ptr_array_add(argv_ptr, stralloc(ctl_data->addrs));
	}
	if (ctl_data->bsu && ctl_data->bsu->smb_recover_mode &&
	    samba_extract_method == SAMBA_SMBCLIENT){
	    g_ptr_array_add(argv_ptr, stralloc("--recover-mode"));
	    g_ptr_array_add(argv_ptr, stralloc("smb"));
	}
	g_ptr_array_add(argv_ptr, stralloc("--level"));
	g_ptr_array_add(argv_ptr, g_strdup_printf("%d", ctl_data->elist->level));
	if (dump_dle) {
	    GSList   *scriptlist;
	    script_t *script;

	    merge_properties(dump_dle, NULL, dump_dle->application_property,
			     proplist, 0);
	    application_property_add_to_argv(argv_ptr, dump_dle, NULL,
					     tapesrv_features);
	    for (scriptlist = dump_dle->scriptlist; scriptlist != NULL;
		 scriptlist = scriptlist->next) {
		script = (script_t *)scriptlist->data;
		if (script->result && script->result->proplist) {
		    property_add_to_argv(argv_ptr, script->result->proplist);
		}
	    }

	} else if (proplist) {
	    property_add_to_argv(argv_ptr, proplist);
	}
	break;
    }

    for (i = 0, fn = ctl_data->elist->files; i < files_off_tape;
					     i++, fn = fn->next)
    {
	switch (dumptype) {
	case IS_APPLICATION_API:
    	case IS_TAR:
    	case IS_GNUTAR:
    	case IS_SAMBA_TAR:
    	case IS_SAMBA:
	    if (strcmp(fn->path, "/") == 0)
		g_ptr_array_add(argv_ptr, stralloc("."));
	    else
		g_ptr_array_add(argv_ptr, stralloc2(".", fn->path));
	    break;
	case IS_UNKNOWN:
	case IS_DUMP:
#if defined(XFSDUMP)
	    if (strcmp(ctl_data->file.program, XFSDUMP) == 0) {
		/*
		 * xfsrestore needs a -s option before each file to be
		 * restored, and also wants them to be relative paths.
		 */
		g_ptr_array_add(argv_ptr, stralloc("-s"));
		g_ptr_array_add(argv_ptr, stralloc(fn->path + 1));
	    } else
#endif
	    {
	    g_ptr_array_add(argv_ptr, stralloc(fn->path));
	    }
	    break;
  	}
    }
#if defined(XFSDUMP)
    if (strcmp(ctl_data->file.program, XFSDUMP) == 0) {
	g_ptr_array_add(argv_ptr, stralloc("-"));
	g_ptr_array_add(argv_ptr, stralloc("."));
    }
#endif
    g_ptr_array_add(argv_ptr, NULL);

    switch (dumptype) {
    case IS_SAMBA:
#ifdef SAMBA_CLIENT
    	cmd = stralloc(SAMBA_CLIENT);
    	break;
#else
	/* fall through to ... */
#endif
    case IS_TAR:
    case IS_GNUTAR:
    case IS_SAMBA_TAR:
#ifndef GNUTAR
	g_fprintf(stderr, _("warning: GNUTAR program not available.\n"));
	cmd = stralloc("tar");
#else
  	cmd = stralloc(GNUTAR);
#endif
    	break;
    case IS_UNKNOWN:
    case IS_DUMP:
	cmd = NULL;
#if defined(DUMP)
	if (strcmp(ctl_data->file.program, DUMP) == 0) {
    	    cmd = stralloc(RESTORE);
	}
#endif
#if defined(VDUMP)
	if (strcmp(ctl_data->file.program, VDUMP) == 0) {
    	    cmd = stralloc(VRESTORE);
	}
#endif
#if defined(VXDUMP)
	if (strcmp(ctl_data->file.program, VXDUMP) == 0) {
    	    cmd = stralloc(VXRESTORE);
	}
#endif
#if defined(XFSDUMP)
	if (strcmp(ctl_data->file.program, XFSDUMP) == 0) {
    	    cmd = stralloc(XFSRESTORE);
	}
#endif
	if (cmd == NULL) {
	    g_fprintf(stderr, _("warning: restore program for %s not available.\n"),
		    ctl_data->file.program);
	    cmd = stralloc("restore");
	}
	break;
    case IS_APPLICATION_API:
	cmd = vstralloc(APPLICATION_DIR, "/", ctl_data->file.application, NULL);
	break;
    }
    if (cmd) {
        dbprintf(_("Exec'ing %s with arguments:\n"), cmd);
	for (j = 0; j < argv_ptr->len - 1; j++) {
	    if (j == passwd_field)
		dbprintf("\tXXXXX\n");
	    else
		dbprintf(_("\t%s\n"), (char *)g_ptr_array_index(argv_ptr, j));
	}
	safe_fd(-1, 0);
        (void)execv(cmd, (char **)argv_ptr->pdata);
	/* only get here if exec failed */
	save_errno = errno;
	g_ptr_array_free_full(argv_ptr);
	errno = save_errno;
        perror(_("amrecover couldn't exec"));
        g_fprintf(stderr, _(" problem executing %s\n"), cmd);
	amfree(cmd);
    }
    exit(1);
    /*NOT REACHED */
}

/*
 * Interpose something between the process writing out the dump (writing it to
 * some extraction program, really) and the socket from which we're reading, so
 * that we can do things like prompt for human interaction for multiple tapes.
 */
int
writer_intermediary(
    EXTRACT_LIST *	elist)
{
    ctl_data_t ctl_data;
    amwait_t   extractor_status;

    ctl_data.header_done   = 0;
    ctl_data.child_pipe[0] = -1;
    ctl_data.child_pipe[1] = -1;
    ctl_data.pid           = -1;
    ctl_data.elist         = elist;
    fh_init(&ctl_data.file);
    ctl_data.data_path     = DATA_PATH_AMANDA;
    ctl_data.addrs         = NULL;
    ctl_data.bsu           = NULL;
    ctl_data.bytes_read    = 0;

    header_size = 0;
    security_stream_read(amidxtaped_streams[DATAFD].fd,
			 read_amidxtaped_data, &ctl_data);

    while(get_amidxtaped_line() >= 0) {
	char desired_tape[MAX_TAPE_LABEL_BUF];
	g_debug("get amidxtaped line: %s", amidxtaped_line);

	/* if prompted for a tape, relay said prompt to the user */
	if(sscanf(amidxtaped_line, "FEEDME %132s\n", desired_tape) == 1) {
	    int done;
	    g_printf(_("Load tape %s now\n"), desired_tape);
	    dbprintf(_("Requesting tape %s from user\n"), desired_tape);
	    done = okay_to_continue(am_has_feature(indexsrv_features,
						   fe_amrecover_feedme_tape),
				    0, 0);
	    if (done == 1) {
		if (am_has_feature(indexsrv_features,
				   fe_amrecover_feedme_tape)) {
		    char *reply = stralloc2("TAPE ", tape_device_name);
		    send_to_tape_server(amidxtaped_streams[CTLFD].fd, reply);
		    amfree(reply);
		} else {
		    send_to_tape_server(amidxtaped_streams[CTLFD].fd, "OK");
		}
	    } else {
		send_to_tape_server(amidxtaped_streams[CTLFD].fd, "ERROR");
		break;
	    }
	} else if (strncmp_const(amidxtaped_line, "USE-DATAPATH ") == 0) {
	    if (strncmp_const(amidxtaped_line+13, "AMANDA") == 0) {
		ctl_data.data_path = DATA_PATH_AMANDA;
		g_debug("Using AMANDA data-path");
	    } else if (strncmp_const(amidxtaped_line+13, "DIRECT-TCP") == 0) {
		ctl_data.data_path = DATA_PATH_DIRECTTCP;
		ctl_data.addrs = stralloc(amidxtaped_line+24);
		g_debug("Using DIRECT-TCP data-path with %s", ctl_data.addrs);
	    }
	    start_processing_data(&ctl_data);
	} else if(strncmp_const(amidxtaped_line, "MESSAGE ") == 0) {
	    g_printf("%s\n",&amidxtaped_line[8]);
	} else {
	    g_fprintf(stderr, _("Strange message from tape server: %s"),
		    amidxtaped_line);
	    break;
	}
    }

    /* CTL might be close before DATA */
    event_loop(0);
    dumpfile_free_data(&ctl_data.file);
    amfree(ctl_data.addrs);
    amfree(ctl_data.bsu);
    if (ctl_data.child_pipe[1] != -1)
	aclose(ctl_data.child_pipe[1]);

    if (ctl_data.header_done == 0) {
	g_printf(_("Got no header and data from server, check in amidxtaped.*.debug and amandad.*.debug files on server\n"));
    }

    if (ctl_data.pid != -1) {
	waitpid(ctl_data.pid, &extractor_status, 0);
	if(WEXITSTATUS(extractor_status) != 0){
	    int ret = WEXITSTATUS(extractor_status);
            if(ret == 255) ret = -1;
	    g_printf(_("Extractor child exited with status %d\n"), ret);
	    return -1;
	}
    }
    g_debug("bytes read: %jd", (intmax_t)ctl_data.bytes_read);
    return(0);
}

/* exec restore to do the actual restoration */

/* does the actual extraction of files */
/*
 * The original design had the dump image being returned exactly as it
 * appears on the tape, and this routine getting from the index server
 * whether or not it is compressed, on the assumption that the tape
 * server may not know how to uncompress it. But
 * - Amrestore can't do that. It returns either compressed or uncompressed
 * (always). Amrestore assumes it can uncompress files. It is thus a good
 * idea to run the tape server on a machine with gzip.
 * - The information about compression in the disklist is really only
 * for future dumps. It is possible to change compression on a drive
 * so the information in the disklist may not necessarily relate to
 * the dump image on the tape.
 *   Consequently the design was changed to assuming that amrestore can
 * uncompress any dump image and have it return an uncompressed file
 * always.
 */
void
extract_files(void)
{
    EXTRACT_LIST *elist;
    char *l;
    int first;
    int otc;
    tapelist_t *tlist = NULL, *a_tlist;
    g_option_t g_options;
    levellist_t all_level = NULL;
    int last_level;

    if (!is_extract_list_nonempty())
    {
	g_printf(_("Extract list empty - No files to extract!\n"));
	return;
    }

    clean_extract_list();

    /* get tape device name from index server if none specified */
    if (tape_server_name == NULL) {
	tape_server_name = newstralloc(tape_server_name, server_name);
    }
    if (tape_device_name == NULL) {
	if (send_command("TAPE") == -1)
	    exit(1);
	if (get_reply_line() == -1)
	    exit(1);
	l = reply_line();
	if (!server_happy())
	{
	    g_printf("%s\n", l);
	    exit(1);
	}
	/* skip reply number */
	tape_device_name = newstralloc(tape_device_name, l+4);
    }

    if (strcmp(tape_device_name, "/dev/null") == 0)
    {
	g_printf(_("amrecover: warning: using %s as the tape device will not work\n"),
	       tape_device_name);
    }

    first=1;
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist)) {
	if(elist->tape[0]!='/') {
	    if(first) {
		g_printf(_("\nExtracting files using tape drive %s on host %s.\n"),
			tape_device_name, tape_server_name);
		g_printf(_("The following tapes are needed:"));
		first=0;
	    }
	    else
		g_printf("                               ");
	    tlist = unmarshal_tapelist_str(elist->tape);
	    for(a_tlist = tlist ; a_tlist != NULL; a_tlist = a_tlist->next)
		g_printf(" %s", a_tlist->label);
	    g_printf("\n");
	    free_tapelist(tlist);
	}
    }
    first=1;
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist)) {
	if(elist->tape[0]=='/') {
	    if(first) {
		g_printf(_("\nExtracting files from holding disk on host %s.\n"),
			tape_server_name);
		g_printf(_("The following files are needed:"));
		first=0;
	    }
	    else
		g_printf("                               ");
	    tlist = unmarshal_tapelist_str(elist->tape);
	    for(a_tlist = tlist; a_tlist != NULL; a_tlist = a_tlist->next)
		g_printf(" %s", a_tlist->label);
	    g_printf("\n");
	    free_tapelist(tlist);
	}
    }
    g_printf("\n");

    g_options.config = get_config_name();
    g_options.hostname = dump_hostname;
    for (elist = first_tape_list(); elist != NULL;
	 elist = next_tape_list(elist)) {
	am_level_t *level = g_new0(am_level_t, 1);
	level->level = elist->level;
	all_level = g_slist_append(all_level, level);
    }
    if (dump_dle) {
	slist_free_full(dump_dle->levellist, g_free);
	dump_dle->levellist = all_level;
	run_client_scripts(EXECUTE_ON_PRE_RECOVER, &g_options, dump_dle,
			   stderr);
	dump_dle->levellist = NULL;
    }
    last_level = -1;
    while ((elist = first_tape_list()) != NULL)
    {
	if(elist->tape[0]=='/') {
	    dump_device_name = newstralloc(dump_device_name, elist->tape);
	    g_printf(_("Extracting from file "));
	    tlist = unmarshal_tapelist_str(dump_device_name);
	    for(a_tlist = tlist; a_tlist != NULL; a_tlist = a_tlist->next)
		g_printf(" %s", a_tlist->label);
	    g_printf("\n");
	    free_tapelist(tlist);
	}
	else {
	    g_printf(_("Extracting files using tape drive %s on host %s.\n"),
		   tape_device_name, tape_server_name);
	    tlist = unmarshal_tapelist_str(elist->tape);
	    g_printf(_("Load tape %s now\n"), tlist->label);
	    dbprintf(_("Requesting tape %s from user\n"), tlist->label);
	    free_tapelist(tlist);
	    otc = okay_to_continue(1,1,0);
	    if (otc == 0)
	        return;
	    else if (otc == SKIP_TAPE) {
		delete_tape_list(elist); /* skip this tape */
		continue;
	    }
	    dump_device_name = newstralloc(dump_device_name, tape_device_name);
	}
	dump_datestamp = newstralloc(dump_datestamp, elist->date);

	if (last_level != -1 && dump_dle) {
	    am_level_t *level;

	    level = g_new0(am_level_t, 1);
	    level->level = last_level;
	    dump_dle->levellist = g_slist_append(dump_dle->levellist, level);

	    level = g_new0(am_level_t, 1);
	    level->level = elist->level;
	    dump_dle->levellist = g_slist_append(dump_dle->levellist, level);
	    run_client_scripts(EXECUTE_ON_INTER_LEVEL_RECOVER, &g_options,
			       dump_dle, stderr);
	    slist_free_full(dump_dle->levellist, g_free);
	    dump_dle->levellist = NULL;
	}

	/* connect to the tape handler daemon on the tape drive server */
	if ((extract_files_setup(elist->tape, elist->fileno)) == -1)
	{
	    g_fprintf(stderr, _("amrecover - can't talk to tape server: %s\n"),
		    errstr);
	    return;
	}
	if (dump_dle) {
	    am_level_t *level;

	    level = g_new0(am_level_t, 1);
	    level->level = elist->level;
	    dump_dle->levellist = g_slist_append(dump_dle->levellist, level);
	    run_client_scripts(EXECUTE_ON_PRE_LEVEL_RECOVER, &g_options,
			       dump_dle, stderr);
	}
	last_level = elist->level;

	/* if the server have fe_amrecover_feedme_tape, it has asked for
	 * the tape itself, even if the restore didn't succeed, we should
	 * remove it.
	 */
	if(writer_intermediary(elist) == 0 ||
	   am_has_feature(indexsrv_features, fe_amrecover_feedme_tape))
	    delete_tape_list(elist);	/* tape done so delete from list */

	am_release_feature_set(tapesrv_features);
	stop_amidxtaped();

	if (dump_dle) {
	    run_client_scripts(EXECUTE_ON_POST_LEVEL_RECOVER, &g_options,
			       dump_dle, stderr);
	    slist_free_full(dump_dle->levellist, g_free);
	    dump_dle->levellist = NULL;
	}
    }
    if (dump_dle) {
	dump_dle->levellist = all_level;
	run_client_scripts(EXECUTE_ON_POST_RECOVER, &g_options, dump_dle,
			   stderr);
	slist_free_full(dump_dle->levellist, g_free);
	all_level = NULL;
	dump_dle->levellist = NULL;
    }
}

static void
amidxtaped_response(
    void *		datap,
    pkt_t *		pkt,
    security_handle_t *	sech)
{
    int ports[NSTREAMS], *response_error = datap, i;
    char *p;
    char *tok;
    char *extra = NULL;

    assert(response_error != NULL);
    assert(sech != NULL);
    memset(ports, -1, SIZEOF(ports));

    if (pkt == NULL) {
	errstr = newvstrallocf(errstr, _("[request failed: %s]"), security_geterror(sech));
	*response_error = 1;
	return;
    }
    security_close_connection(sech, dump_hostname);

    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	g_fprintf(stderr, _("got nak response:\n----\n%s\n----\n\n"), pkt->body);
#endif

	tok = strtok(pkt->body, " ");
	if (tok == NULL || strcmp(tok, "ERROR") != 0)
	    goto bad_nak;

	tok = strtok(NULL, "\n");
	if (tok != NULL) {
	    errstr = newvstralloc(errstr, "NAK: ", tok, NULL);
	    *response_error = 1;
	} else {
bad_nak:
	    errstr = newstralloc(errstr, "request NAK");
	    *response_error = 2;
	}
	return;
    }

    if (pkt->type != P_REP) {
	errstr = newvstrallocf(errstr, _("received strange packet type %s: %s"),
			      pkt_type2str(pkt->type), pkt->body);
	*response_error = 1;
	return;
    }

#if defined(PACKET_DEBUG)
    g_fprintf(stderr, _("got response:\n----\n%s\n----\n\n"), pkt->body);
#endif

    for(i = 0; i < NSTREAMS; i++) {
        ports[i] = -1;
        amidxtaped_streams[i].fd = NULL;
    }

    p = pkt->body;
    while((tok = strtok(p, " \n")) != NULL) {
	p = NULL;

	/*
	 * Error response packets have "ERROR" followed by the error message
	 * followed by a newline.
	 */
	if (strcmp(tok, "ERROR") == 0) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL)
		tok = _("[bogus error packet]");
	    errstr = newstralloc(errstr, tok);
	    *response_error = 2;
	    return;
	}


        /*
         * Regular packets have CONNECT followed by three streams
         */
        if (strcmp(tok, "CONNECT") == 0) {

	    /*
	     * Parse the three stream specifiers out of the packet.
	     */
	    for (i = 0; i < NSTREAMS; i++) {
		tok = strtok(NULL, " ");
		if (tok == NULL || strcmp(tok, amidxtaped_streams[i].name) != 0) {
		    extra = vstrallocf(_("CONNECT token is \"%s\": expected \"%s\""),
				      tok ? tok : "(null)",
				      amidxtaped_streams[i].name);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = vstrallocf(_("CONNECT %s token is \"%s\": expected a port number"),
				      amidxtaped_streams[i].name,
				      tok ? tok : "(null)");
		    goto parse_error;
		}
	    }
	    continue;
	}

	/*
	 * OPTIONS [options string] '\n'
	 */
	if (strcmp(tok, "OPTIONS") == 0) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL) {
		extra = stralloc(_("OPTIONS token is missing"));
		goto parse_error;
	    }
/*
	    while((p = strchr(tok, ';')) != NULL) {
		*p++ = '\0';
		if(strncmp_const(tok, "features=") == 0) {
		    tok += sizeof("features=") - 1;
		    am_release_feature_set(their_features);
		    if((their_features = am_string_to_feature(tok)) == NULL) {
			errstr = newvstralloc(errstr,
					      _("OPTIONS: bad features value: "),
					      tok,
					      NULL);
			goto parse_error;
		    }
		}
		tok = p;
	    }
*/
	    continue;
	}
/*
	extra = vstrallocf("next token is \"%s\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\""),
			  tok ? tok : _("(null)"));
	goto parse_error;
*/
    }

    /*
     * Connect the streams to their remote ports
     */
    for (i = 0; i < NSTREAMS; i++) {
	if (ports[i] == -1)
	    continue;
	amidxtaped_streams[i].fd = security_stream_client(sech, ports[i]);
	dbprintf(_("amidxtaped_streams[%d].fd = %p\n"),i, amidxtaped_streams[i].fd);
	if (amidxtaped_streams[i].fd == NULL) {
	    errstr = newvstrallocf(errstr,\
			_("[could not connect %s stream: %s]"),
			amidxtaped_streams[i].name,
			security_geterror(sech));
	    goto connect_error;
	}
    }
    /*
     * Authenticate the streams
     */
    for (i = 0; i < NSTREAMS; i++) {
	if (amidxtaped_streams[i].fd == NULL)
	    continue;
	if (security_stream_auth(amidxtaped_streams[i].fd) < 0) {
	    errstr = newvstrallocf(errstr,
		_("[could not authenticate %s stream: %s]"),
		amidxtaped_streams[i].name,
		security_stream_geterror(amidxtaped_streams[i].fd));
	    goto connect_error;
	}
    }

    /*
     * The CTLFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (amidxtaped_streams[CTLFD].fd == NULL) {
        errstr = newvstrallocf(errstr, _("[couldn't open CTL streams]"));
        goto connect_error;
    }
    if (amidxtaped_streams[DATAFD].fd == NULL) {
        errstr = newvstrallocf(errstr, _("[couldn't open DATA streams]"));
        goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    return;

parse_error:
    if (extra) {
	errstr = newvstrallocf(errstr,
			  _("[parse of reply message failed: %s]"), extra);
    } else {
	errstr = newvstrallocf(errstr,
			  _("[parse of reply message failed: (no additional information)"));
    }
    amfree(extra);
    *response_error = 2;
    return;

connect_error:
    stop_amidxtaped();
    *response_error = 1;
}

/*
 * This is called when everything needs to shut down so event_loop()
 * will exit.
 */
static void
stop_amidxtaped(void)
{
    int i;

    for (i = 0; i < NSTREAMS; i++) {
        if (amidxtaped_streams[i].fd != NULL) {
            security_stream_close(amidxtaped_streams[i].fd);
            amidxtaped_streams[i].fd = NULL;
        }
    }
}

static char* ctl_buffer = NULL;
/* gets a "line" from server and put in server_line */
/* server_line is terminated with \0, \r\n is striped */
/* returns -1 if error */

int
get_amidxtaped_line(void)
{
    ssize_t size;
    char *newbuf, *s;
    void *buf;

    amfree(amidxtaped_line);
    if (!ctl_buffer)
	ctl_buffer = stralloc("");

    while (!strstr(ctl_buffer,"\r\n")) {
	if (amidxtaped_streams[CTLFD].fd == NULL)
	    return -1;

        size = security_stream_read_sync(amidxtaped_streams[CTLFD].fd, &buf);
        if(size < 0) {
            return -1;
        }
        else if(size == 0) {
            return -1;
        }
        newbuf = alloc(strlen(ctl_buffer)+size+1);
        strncpy(newbuf, ctl_buffer, (size_t)(strlen(ctl_buffer) + size + 1));
        memcpy(newbuf+strlen(ctl_buffer), buf, (size_t)size);
        newbuf[strlen(ctl_buffer)+size] = '\0';
        amfree(ctl_buffer);
        ctl_buffer = newbuf;
	amfree(buf);
    }

    s = strstr(ctl_buffer,"\r\n");
    *s = '\0';
    newbuf = stralloc(s+2);
    amidxtaped_line = stralloc(ctl_buffer);
    amfree(ctl_buffer);
    ctl_buffer = newbuf;
    return 0;
}


static void
read_amidxtaped_data(
    void *	cookie,
    void *	buf,
    ssize_t	size)
{
    ctl_data_t *ctl_data = (ctl_data_t *)cookie;
    assert(cookie != NULL);

    if (size < 0) {
	errstr = newstralloc2(errstr, _("amidxtaped read: "),
		 security_stream_geterror(amidxtaped_streams[DATAFD].fd));
	return;
    }

    /*
     * EOF.  Stop and return.
     */
    if (size == 0) {
	security_stream_close(amidxtaped_streams[DATAFD].fd);
	amidxtaped_streams[DATAFD].fd = NULL;
	/*
	 * If the mesg fd has also shut down, then we're done.
	 */
	return;
    }

    assert(buf != NULL);

    if (ctl_data->header_done == 0) {
	GPtrArray  *errarray;
	g_option_t  g_options;
	data_path_t data_path_set = DATA_PATH_AMANDA;
	int to_move;

	to_move = MIN(32768-header_size, size);
	memcpy(header_buf+header_size, buf, to_move);
	header_size += to_move;

	g_debug("read header %zd => %d", size, header_size);
	if (header_size < 32768) {
            security_stream_read(amidxtaped_streams[DATAFD].fd,
				 read_amidxtaped_data, cookie);
	    return;
	} else if (header_size > 32768) {
	    error("header_size is %d\n", header_size);
	}
	assert (to_move == size);
	/* parse the file header */
	fh_init(&ctl_data->file);
	parse_file_header(header_buf, &ctl_data->file, (size_t)header_size);

	/* call backup_support_option */
	g_options.config = get_config_name();
	g_options.hostname = dump_hostname;
	if (strcmp(ctl_data->file.program, "APPLICATION") == 0) {
	    if (dump_dle) {
	        ctl_data->bsu = backup_support_option(ctl_data->file.application,
						      &g_options,
						      ctl_data->file.disk,
						      dump_dle->device,
						      &errarray);
	    } else {
	        ctl_data->bsu = backup_support_option(ctl_data->file.application,
						      &g_options,
						      ctl_data->file.disk, NULL,
						      &errarray);
	    }
	    if (!ctl_data->bsu) {
		guint  i;
		for (i=0; i < errarray->len; i++) {
		    char *line;
		    line = g_ptr_array_index(errarray, i);
		    g_fprintf(stderr, "%s\n", line);
		}
		g_ptr_array_free_full(errarray);
		exit(1);
	    }
	    data_path_set = ctl_data->bsu->data_path_set;
	}
	/* handle backup_support_option failure */

	ctl_data->header_done = 1;
	if (!ask_file_overwrite(ctl_data)) {
	    if (am_has_feature(tapesrv_features, fe_amidxtaped_abort)) {
		send_to_tape_server(amidxtaped_streams[CTLFD].fd, "ABORT");
	    }
	    stop_amidxtaped();
	    return;
	}

	if (am_has_feature(tapesrv_features, fe_amidxtaped_datapath)) {
 	    char       *msg;
	    /* send DATA-PATH request */
	    msg = stralloc("AVAIL-DATAPATH");
	    if (data_path_set & DATA_PATH_AMANDA)
		vstrextend(&msg, " AMANDA", NULL);
	    if (data_path_set & DATA_PATH_DIRECTTCP)
		vstrextend(&msg, " DIRECT-TCP", NULL);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, msg);
	    amfree(msg);
	} else {
	    start_processing_data(ctl_data);
	}
    } else {
	ctl_data->bytes_read += size;
	/* Only the data is sent to the child */
	/*
	 * We ignore errors while writing to the index file.
	 */
	(void)full_write(ctl_data->child_pipe[1], buf, (size_t)size);
        security_stream_read(amidxtaped_streams[DATAFD].fd,
			     read_amidxtaped_data, cookie);
    }
}

static gboolean
ask_file_overwrite(
    ctl_data_t *ctl_data)
{
    char *restore_dir = NULL;

    if (ctl_data->file.dumplevel == 0) {
	property_t *property = g_hash_table_lookup(proplist, "directory");
	if (property && property->values && property->values->data) {
	    /* take first property value */
	    restore_dir = strdup(property->values->data);
	}
	if (samba_extract_method == SAMBA_SMBCLIENT ||
	    (ctl_data->bsu &&
	     ctl_data->bsu->recover_path == RECOVER_PATH_REMOTE)) {
	    if (!restore_dir) {
		restore_dir = g_strdup(ctl_data->file.disk);
	    }
	    g_printf(_("Restoring files into target host %s\n"), restore_dir);
	} else {
	    if (!restore_dir) {
		restore_dir = g_get_current_dir();
	    }
	    g_printf(_("Restoring files into directory %s\n"), restore_dir);
	}

	/* Collect files to delete befause of a bug in gnutar */
	if (strcmp(ctl_data->file.program, "GNUTAR") == 0 ||
	    (strcmp(ctl_data->file.program, "APPLICATION") == 0 &&
	     strcmp(ctl_data->file.application, "amgtar") == 0)) {
	    check_file_overwrite(restore_dir);
	} else {
	    g_printf(_("All existing files in %s can be deleted\n"),
		     restore_dir);
	}

	if (!okay_to_continue(0,0,0)) {
	    free_unlink_list();
	    amfree(restore_dir);
	    return FALSE;
	}
	g_printf("\n");

	/* delete the files for gnutar */
	if (unlink_list) {
	    if (!do_unlink_list()) {
		g_fprintf(stderr, _("Can't recover because I can't cleanup the restore directory (%s)\n"),
			  restore_dir);
		free_unlink_list();
		amfree(restore_dir);
		return FALSE;
	    }
	    free_unlink_list();
	}
	amfree(restore_dir);
    }
    return TRUE;
}

static void
start_processing_data(
    ctl_data_t *ctl_data)
{
    if (pipe(ctl_data->child_pipe) == -1) {
	error(_("extract_list - error setting up pipe to extractor: %s\n"),
	      strerror(errno));
	/*NOTREACHED*/
    }

    /* decrypt */
    if (ctl_data->file.encrypted) {
	char *argv[3];
	int  crypt_out;
	int  errfd = fileno(stderr);

	g_debug("image is encrypted %s %s", ctl_data->file.clnt_encrypt, ctl_data->file.clnt_decrypt_opt);
	argv[0] = ctl_data->file.clnt_encrypt;
	argv[1] = ctl_data->file.clnt_decrypt_opt;
	argv[2] = NULL;
	pipespawnv(ctl_data->file.clnt_encrypt, STDOUT_PIPE, 0, &ctl_data->child_pipe[0], &crypt_out, &errfd, argv);
	ctl_data->child_pipe[0] = crypt_out;
    }

    /* decompress */
    if (ctl_data->file.compressed) {
	char *argv[3];
	int  comp_out;
	int  errfd = fileno(stderr);
	char *comp_prog;
	char *comp_arg;

	g_debug("image is compressed %s", ctl_data->file.clntcompprog);
	if (strlen(ctl_data->file.clntcompprog) > 0) {
	    comp_prog = ctl_data->file.clntcompprog;
	    comp_arg = "-d";
	} else {
	    comp_prog = UNCOMPRESS_PATH;
	    comp_arg = UNCOMPRESS_OPT;
	}
	argv[0] = comp_prog;
	argv[1] = comp_arg;
	argv[2] = NULL;
	pipespawnv(comp_prog, STDOUT_PIPE, 0, &ctl_data->child_pipe[0], &comp_out, &errfd, argv);
	ctl_data->child_pipe[0] = comp_out;
    }

    /* okay, ready to extract. fork a child to do the actual work */
    if ((ctl_data->pid = fork()) == 0) {
	/* this is the child process */
	/* never gets out of this clause */
	aclose(ctl_data->child_pipe[1]);
	extract_files_child(ctl_data);
	/*NOTREACHED*/
    }
	
    if (ctl_data->pid == -1) {
	errstr = newstralloc(errstr, _("writer_intermediary - error forking child"));
	g_printf(_("writer_intermediary - error forking child"));
	return;
    }
    aclose(ctl_data->child_pipe[0]);
    security_stream_read(amidxtaped_streams[DATAFD].fd, read_amidxtaped_data,
			 ctl_data);
    if (am_has_feature(tapesrv_features, fe_amidxtaped_datapath)) {
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "DATAPATH-OK");
    }
}
