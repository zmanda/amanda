/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998, 2000 University of Maryland at College Park
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
#include "amutil.h"
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

typedef struct cdata_s {
    int             fd;
    FILE           *output;
    char           *name;
    char           *buffer;
    gint64          first;           /* first byte used */
    gint64          size;            /* number of byte use in the buffer */
    gint64          allocated_size ; /* allocated size of the buffer     */
    event_handle_t *event;
} cdata_t;

typedef struct ctl_data_s {
  int                      header_done;
  int                      child_in[2];
  int                      child_out[2];
  int                      child_err[2];
  int                      crc_pipe[2];
  int                      pid;
  EXTRACT_LIST            *elist;
  dumpfile_t               file;
  data_path_t              data_path;
  char                    *addrs;
  backup_support_option_t *bsu;
  gint64                   bytes_read;
  cdata_t		   decrypt_cdata;
  cdata_t		   decompress_cdata;
  cdata_t		   child_out_cdata;
  cdata_t		   child_err_cdata;
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

typedef struct send_crc_s {
    int      in;
    int      out;
    crc_t    crc;
    GThread *thread;
} send_crc_t;

#define NSTREAMS G_N_ELEMENTS(amidxtaped_streams)

static void amidxtaped_response(void *, pkt_t *, security_handle_t *);
static void stop_amidxtaped(void);
static char *dump_device_name = NULL;
static char *errstr;
static char *amidxtaped_line = NULL;
extern char *localhost;
static char header_buf[32768];
static int  header_size = 0;
static int  stderr_isatty;
static time_t last_time = 0;
static gboolean  last_is_size;
static crc_t crc_in;
static send_crc_t native_crc;
static crc_t network_crc;


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
static void handle_child_out(void *);
static void read_amidxtaped_data(void *, void *, ssize_t);
static char *merge_path(char *path1, char *path2);
static gboolean ask_file_overwrite(ctl_data_t *ctl_data);
static void start_processing_data(ctl_data_t *ctl_data);
static gpointer handle_crc_thread(gpointer data);

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
	    if(g_str_equal(fn1->path, fn2->path)) {
		remove_fn2 = 1;
	    } else if (g_str_has_prefix(fn2->path, fn1->path) &&
		       ((strlen(fn2->path) > strlen(fn1->path) &&
			 fn2->path[strlen(fn1->path)] == '/') ||
		       (fn1->path[strlen(fn1->path)-1] == '/'))) {
		remove_fn2 = 1;
	    } else if (g_str_has_prefix(fn1->path, fn2->path) &&
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
    if (g_str_equal(*dir, ".")) {
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
	unlink_list = g_malloc(sizeof(*unlink_list));
	unlink_list->path = g_strdup(path);
	unlink_list->next = NULL;
    } else {
	for (ul = unlink_list; ul != NULL; ul = ul->next) {
	    if (g_str_equal(ul->path, path))
		return 0;
	}
	ul = g_malloc(sizeof(*ul));
	ul->path = g_strdup(path);
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

	    path = g_strconcat(dir, fn->path, NULL);
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

	    filename = g_strconcat(dir, fn->path, NULL);
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

    ditem_path = g_strdup(ditem->path);
    clean_pathname(ditem_path);

    for (this = extract_list; this != NULL; this = this->next)
    {
	/* see if this is the list for the tape */	
	if (this->level == ditem->level && g_str_equal(this->tape,
                                                       ditem->tape))
	{
	    /* yes, so add to list */
	    curr=this->files;
	    while(curr!=NULL)
	    {
		if (g_str_equal(curr->path, ditem_path)) {
		    g_free(ditem_path);
		    return 1;
		}
		curr=curr->next;
	    }
	    that = (EXTRACT_LIST_ITEM *)g_malloc(sizeof(EXTRACT_LIST_ITEM));
            that->path = ditem_path;
            that->tpath = clean_pathname(g_strdup(ditem->tpath));
	    that->next = this->files;
	    this->files = that;		/* add at front since easiest */
	    return 0;
	}
    }

    /* so this is the first time we have seen this tape */
    this = (EXTRACT_LIST *)g_malloc(sizeof(EXTRACT_LIST));
    this->tape = g_strdup(ditem->tape);
    this->level = ditem->level;
    this->fileno = ditem->fileno;
    this->date = g_strdup(ditem->date);
    that = (EXTRACT_LIST_ITEM *)g_malloc(sizeof(EXTRACT_LIST_ITEM));
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

    ditem_path = g_strdup(ditem->path);
    clean_pathname(ditem_path);

    for (this = extract_list; this != NULL; this = this->next)
    {
	/* see if this is the list for the tape */	
	if (this->level == ditem->level && g_str_equal(this->tape,
                                                       ditem->tape))
	{
	    /* yes, so find file on list */
	    that = this->files;
	    if (g_str_equal(that->path, ditem_path))
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
		if (g_str_equal(that->path, ditem_path))
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
	result = g_strconcat(path1, path2 + 1, NULL);
    } else if (path1[len-1] != '/' && path2[0] != '/') {
	result = g_strjoin(NULL, path1, "/", path2, NULL);
    } else {
	result = g_strconcat(path1, path2, NULL);
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
            regex_path = g_strdup(regex + 1);
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

    if(g_str_equal(regex, "/[/]*$")) {	/* "/" behave like "." */
	regex = "\\.[/]*$";
    }
    else if(g_str_equal(regex, "[^/]*[/]*$")) {		/* "*" */
	regex = "([^/.]|\\.[^/]+|[^/.][^/]*)[/]*$";
    } else {
	/* remove "/" at end of path */
	j = (ssize_t)(strlen(regex) - 1);
	while(j >= 0 && regex[j] == '/')
	    regex[j--] = '\0';
    }

    /* convert path (assumed in cwd) to one on disk */
    if (g_str_equal(disk_path, "/")) {
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
		g_free(ditem_path);
		ditem_path = g_strdup(ditem->path);
		clean_pathname(ditem_path);

		qditem_path = quote_string(ditem_path);
		g_free(cmd);
		cmd = g_strconcat("ORLD ", qditem_path, NULL);
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
		    amfree(lditem.path);
		    amfree(lditem.date);
		    amfree(lditem.tape);
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
			    cmd = g_strdup(l);	/* save for error report */
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
                    g_free(lditem.date);
                    lditem.date = g_strdup(fp);
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
            regex_path = g_strdup(regex + 1);
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

    if (g_str_equal(regex, "[^/]*[/]*$")) {
	/* Looking for * find everything but single . */
	regex = "([^/.]|\\.[^/]+|[^/.][^/]*)[/]*$";
    } else {
	/* remove "/" at end of path */
	j = (ssize_t)(strlen(regex) - 1);
	while(j >= 0 && regex[j] == '/') regex[j--] = '\0';
    }

    /* convert path (assumed in cwd) to one on disk */
    if (g_str_equal(disk_path, "/")) {
        if (*regex == '/') {
	    if (g_str_equal(regex, "/[/]*$")) {
		/* We want "/" to match the directory itself: "/." */
		tpath_on_disk = g_strdup("/\\.[/]*$");
	    } else {
		/* No mods needed if already starts with '/' */
		tpath_on_disk = g_strdup(regex);
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
		g_free(ditem_path);
		g_free(ditem_tpath);
		ditem_path = g_strdup(ditem->path);
		ditem_tpath = g_strdup(ditem->tpath);
		clean_pathname(ditem_path);
		clean_pathname(ditem_tpath);

		qditem_path = quote_string(ditem_path);
		g_free(cmd);
		cmd = g_strconcat("ORLD ", qditem_path, NULL);
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
		    amfree(ditem_tpath);
		    amfree(tpath_on_disk);
		    exit(1);
		}
		if(i==0)		/* assume something wrong */
		{
		    amfree(ditem_path);
		    amfree(ditem_tpath);
		    amfree(tpath_on_disk);
		    amfree(lditem.path);
		    l = reply_line();
		    g_printf("%s\n", l);
		    return;
		}
		deleted=0;
                g_free(lditem.path);
                g_free(lditem.tpath);
                lditem.path = g_strdup(ditem->path);
                lditem.tpath = g_strdup(ditem->tpath);
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
			    cmd = g_strdup(l);	/* save for the error report */
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

                    g_free(lditem.date);
                    lditem.date = g_strdup(date);
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
    amfree(lditem.path);

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
	pager_command = g_strconcat(pager, " ; /bin/cat > /dev/null", NULL);
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
	    char *tmp = g_strdup(tape_server_name);

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
    char *msg = g_strconcat(cmd, "\r\n", NULL);

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
    req = g_strjoin(NULL, "SERVICE amidxtaped\n",
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

    clean_datestamp = g_strdup(dump_datestamp);
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
	g_free(tt);
	tt = g_strconcat("FEATURES=", our_features_string, NULL);
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
	    g_free(tt);
	    tt = g_strconcat("CONFIG=", get_config_name(), NULL);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_label) && label &&
	   label[0] != '/' && strncmp(label, "HOLDING:/", 9) != 0) {
	    g_free(tt);
	    tt = g_strconcat("LABEL=", label, NULL);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_fsf)) {
	    char v_fsf[100];
	    g_snprintf(v_fsf, 99, "%lld", (long long)fsf);
	    g_free(tt);
	    tt = g_strconcat("FSF=", v_fsf, NULL);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "HEADER");
	g_free(tt);
	tt = g_strconcat("DEVICE=", dump_device_name, NULL);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	g_free(tt);
	tt = g_strconcat("HOST=", host_regex, NULL);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	g_free(tt);
	tt = g_strconcat("DISK=", disk_regex, NULL);
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	g_free(tt);
	tt = g_strconcat("DATESTAMP=", clean_datestamp, NULL);
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
    if (dup2(ctl_data->child_in[0], STDIN_FILENO) == -1)
    {
	error(_("dup2 failed in extract_files_child: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    /* make out_fd be our stdout */
    if (dup2(ctl_data->child_out[1], STDOUT_FILENO) == -1)
    {
	error(_("dup2 failed in extract_files_child: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    /* make err_fd be our stdout */
    if (dup2(ctl_data->child_err[1], STDERR_FILENO) == -1)
    {
	error(_("dup2 failed in extract_files_child: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    if(ctl_data->file.type != F_DUMPFILE) {
	dump_dumpfile_t(&ctl_data->file);
	error(_("bad header"));
	/*NOTREACHED*/
    }

    if (ctl_data->file.program[0] != '\0') {
	if (g_str_equal(ctl_data->file.program, "APPLICATION"))
	    dumptype = IS_APPLICATION_API;
#ifdef GNUTAR
	if (g_str_equal(ctl_data->file.program, GNUTAR))
	    dumptype = IS_GNUTAR;
#endif

	if (dumptype == IS_UNKNOWN) {
	    len_program = strlen(ctl_data->file.program);
	    if(len_program >= 3 &&
	       g_str_equal(&ctl_data->file.program[len_program - 3], "tar"))
		dumptype = IS_TAR;
	}

#ifdef SAMBA_CLIENT
	if (dumptype == IS_UNKNOWN && g_str_equal(ctl_data->file.program,
                                                  SAMBA_CLIENT)) {
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
	g_ptr_array_add(argv_ptr, g_strdup("smbclient"));
	smbpass = findpass(ctl_data->file.disk, &domain);
	if (smbpass) {
	    g_ptr_array_add(argv_ptr, g_strdup(ctl_data->file.disk));
	    g_ptr_array_add(argv_ptr, g_strdup("-U"));
	    passwd_field = argv_ptr->len;
	    g_ptr_array_add(argv_ptr, g_strdup(smbpass));
	    if (domain) {
		g_ptr_array_add(argv_ptr, g_strdup("-W"));
		g_ptr_array_add(argv_ptr, g_strdup(domain));
	    }
	}
	g_ptr_array_add(argv_ptr, g_strdup("-d0"));
	g_ptr_array_add(argv_ptr, g_strdup("-Tx"));
	g_ptr_array_add(argv_ptr, g_strdup("-"));	/* data on stdin */
	break;
#endif
    case IS_TAR:
    case IS_GNUTAR:
	g_ptr_array_add(argv_ptr, g_strdup("tar"));
	/* ignore trailing zero blocks on input (this was the default until tar-1.21) */
	g_ptr_array_add(argv_ptr, g_strdup("--ignore-zeros"));
	g_ptr_array_add(argv_ptr, g_strdup("--numeric-owner"));
	g_ptr_array_add(argv_ptr, g_strdup("-xpGvf"));
	g_ptr_array_add(argv_ptr, g_strdup("-"));	/* data on stdin */
	break;
    case IS_SAMBA_TAR:
	g_ptr_array_add(argv_ptr, g_strdup("tar"));
	g_ptr_array_add(argv_ptr, g_strdup("-xpvf"));
	g_ptr_array_add(argv_ptr, g_strdup("-"));	/* data on stdin */
	break;
    case IS_UNKNOWN:
    case IS_DUMP:
	g_ptr_array_add(argv_ptr, g_strdup("restore"));
#ifdef AIX_BACKUP
	g_ptr_array_add(argv_ptr, g_strdup("-xB"));
#else
#if defined(XFSDUMP)
	if (g_str_equal(ctl_data->file.program, XFSDUMP)) {
	    g_ptr_array_add(argv_ptr, g_strdup("-v"));
	    g_ptr_array_add(argv_ptr, g_strdup("silent"));
	} else
#endif
#if defined(VDUMP)
	if (g_str_equal(ctl_data->file.program, VDUMP)) {
	    g_ptr_array_add(argv_ptr, g_strdup("xf"));
	    g_ptr_array_add(argv_ptr, g_strdup("-"));	/* data on stdin */
	} else
#endif
	{
	g_ptr_array_add(argv_ptr, g_strdup("xbf"));
	g_ptr_array_add(argv_ptr, g_strdup("2")); /* read in units of 1K */
	g_ptr_array_add(argv_ptr, g_strdup("-"));	/* data on stdin */
	}
#endif
	break;
    case IS_APPLICATION_API:
	g_ptr_array_add(argv_ptr, g_strdup(ctl_data->file.application));
	g_ptr_array_add(argv_ptr, g_strdup("restore"));
	g_ptr_array_add(argv_ptr, g_strdup("--config"));
	g_ptr_array_add(argv_ptr, g_strdup(get_config_name()));
	g_ptr_array_add(argv_ptr, g_strdup("--disk"));
	g_ptr_array_add(argv_ptr, g_strdup(ctl_data->file.disk));
	if (dump_dle && dump_dle->device) {
	    g_ptr_array_add(argv_ptr, g_strdup("--device"));
	    g_ptr_array_add(argv_ptr, g_strdup(dump_dle->device));
	}
	if (ctl_data->data_path == DATA_PATH_DIRECTTCP) {
	    g_ptr_array_add(argv_ptr, g_strdup("--data-path"));
	    g_ptr_array_add(argv_ptr, g_strdup("DIRECTTCP"));
	    g_ptr_array_add(argv_ptr, g_strdup("--direct-tcp"));
	    g_ptr_array_add(argv_ptr, g_strdup(ctl_data->addrs));
	}
	if (ctl_data->bsu && ctl_data->bsu->smb_recover_mode &&
	    samba_extract_method == SAMBA_SMBCLIENT){
	    g_ptr_array_add(argv_ptr, g_strdup("--recover-mode"));
	    g_ptr_array_add(argv_ptr, g_strdup("smb"));
	}
	g_ptr_array_add(argv_ptr, g_strdup("--level"));
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
	    if (g_str_equal(fn->path, "/"))
		g_ptr_array_add(argv_ptr, g_strdup("."));
	    else
		g_ptr_array_add(argv_ptr, g_strconcat(".", fn->path, NULL));
	    break;
	case IS_UNKNOWN:
	case IS_DUMP:
#if defined(XFSDUMP)
	    if (g_str_equal(ctl_data->file.program, XFSDUMP)) {
		/*
		 * xfsrestore needs a -s option before each file to be
		 * restored, and also wants them to be relative paths.
		 */
		g_ptr_array_add(argv_ptr, g_strdup("-s"));
		g_ptr_array_add(argv_ptr, g_strdup(fn->path + 1));
	    } else
#endif
	    {
	    g_ptr_array_add(argv_ptr, g_strdup(fn->path));
	    }
	    break;
  	}
    }
#if defined(XFSDUMP)
    if (g_str_equal(ctl_data->file.program, XFSDUMP)) {
	g_ptr_array_add(argv_ptr, g_strdup("-"));
	g_ptr_array_add(argv_ptr, g_strdup("."));
    }
#endif
    g_ptr_array_add(argv_ptr, NULL);

    switch (dumptype) {
    case IS_SAMBA:
#ifdef SAMBA_CLIENT
    	cmd = g_strdup(SAMBA_CLIENT);
    	break;
#else
	/* fall through to ... */
#endif
    case IS_TAR:
    case IS_GNUTAR:
    case IS_SAMBA_TAR:
#ifndef GNUTAR
	g_fprintf(stderr, _("warning: GNUTAR program not available.\n"));
	cmd = g_strdup("tar");
#else
  	cmd = g_strdup(GNUTAR);
#endif
    	break;
    case IS_UNKNOWN:
    case IS_DUMP:
	cmd = NULL;
#if defined(DUMP)
	if (g_str_equal(ctl_data->file.program, DUMP)) {
    	    cmd = g_strdup(RESTORE);
	}
#endif
#if defined(VDUMP)
	if (g_str_equal(ctl_data->file.program, VDUMP)) {
    	    cmd = g_strdup(VRESTORE);
	}
#endif
#if defined(VXDUMP)
	if (g_str_equal(ctl_data->file.program, VXDUMP)) {
    	    cmd = g_strdup(VXRESTORE);
	}
#endif
#if defined(XFSDUMP)
	if (g_str_equal(ctl_data->file.program, XFSDUMP)) {
    	    cmd = g_strdup(XFSRESTORE);
	}
#endif
	if (cmd == NULL) {
	    g_fprintf(stderr, _("warning: restore program for %s not available.\n"),
		    ctl_data->file.program);
	    cmd = g_strdup("restore");
	}
	break;
    case IS_APPLICATION_API:
	cmd = g_strjoin(NULL, APPLICATION_DIR, "/", ctl_data->file.application, NULL);
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
    ctl_data.child_in[0]  = -1;
    ctl_data.child_in[1]  = -1;
    ctl_data.child_out[0] = -1;
    ctl_data.child_out[1] = -1;
    ctl_data.child_err[0] = -1;
    ctl_data.child_err[1] = -1;
    ctl_data.pid           = -1;
    ctl_data.elist         = elist;
    fh_init(&ctl_data.file);
    ctl_data.data_path     = DATA_PATH_AMANDA;
    ctl_data.addrs         = NULL;
    ctl_data.bsu           = NULL;
    ctl_data.bytes_read    = 0;

    header_size = 0;
    crc32_init(&crc_in);
    crc32_init(&native_crc.crc);
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
		    char *reply = g_strconcat("TAPE ", tape_device_name, NULL);
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
		ctl_data.addrs = g_strdup(amidxtaped_line+24);
		g_debug("Using DIRECT-TCP data-path with %s", ctl_data.addrs);
	    }
	    start_processing_data(&ctl_data);
	} else if(strncmp_const(amidxtaped_line, "MESSAGE ") == 0) {
	    g_printf("%s\n",&amidxtaped_line[8]);
	} else if(strncmp_const(amidxtaped_line, "DATA-STATUS ") == 0) {
	    //g_printf("status: %s\n",&amidxtaped_line[12]);
	} else if(strncmp_const(amidxtaped_line, "DATA-CRC ") == 0) {
	    parse_crc(&amidxtaped_line[9], &network_crc);
	} else {
	    g_fprintf(stderr, _("Strange message from tape server: %s\n"),
		    amidxtaped_line);
	    break;
	}
    }

    /* CTL might be close before DATA */
    event_loop(0);

    if (native_crc.thread) {
	g_thread_join(native_crc.thread);
    }

    if (!ctl_data.file.encrypted && !ctl_data.file.compressed) {
	native_crc.crc.crc = crc_in.crc;
	native_crc.crc.size = crc_in.size;
    }
    g_debug("native_crc: %08x:%lld", ctl_data.file.native_crc.crc, (long long)ctl_data.file.native_crc.size);
    g_debug("client_crc: %08x:%lld", ctl_data.file.client_crc.crc, (long long)ctl_data.file.client_crc.size);
    g_debug("server_crc: %08x:%lld", ctl_data.file.server_crc.crc, (long long)ctl_data.file.server_crc.size);
    g_debug("crc_in    : %08x:%lld", crc32_finish(&crc_in), (long long)crc_in.size);
    g_debug("crc_native: %08x:%lld", crc32_finish(&native_crc.crc), (long long)native_crc.crc.size);

    if (network_crc.crc > 0 && network_crc.crc != crc32_finish(&crc_in)) {
	g_fprintf(stderr,
		"Network-crc (%08x:%lld) and data-in-crc (%08x:%lld) differ\n",
		network_crc.crc, (long long)network_crc.size,
		crc32_finish(&crc_in), (long long)crc_in.size);
    }

    if (ctl_data.file.native_crc.crc > 0 &&
	ctl_data.file.native_crc.crc != crc32_finish(&native_crc.crc)) {
	g_fprintf(stderr,
		"dump-native-crc (%08x:%lld) and native-crc (%08x:%lld) differ\n",
		ctl_data.file.native_crc.crc,
		(long long)ctl_data.file.native_crc.size,
		crc32_finish(&native_crc.crc), (long long)native_crc.crc.size);
    }
    dumpfile_free_data(&ctl_data.file);
    amfree(ctl_data.addrs);
    amfree(ctl_data.bsu);
    if (ctl_data.child_in[1] != -1)
	aclose(ctl_data.child_in[1]);
    if (ctl_data.child_out[0] != -1)
	aclose(ctl_data.child_out[0]);
    if (ctl_data.child_err[0] != -1)
	aclose(ctl_data.child_err[0]);

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
    char *etapelist;

    if (!is_extract_list_nonempty())
    {
	g_printf(_("Extract list empty - No files to extract!\n"));
	return;
    }

    clean_extract_list();

    /* get tape device name from index server if none specified */
    if (tape_server_name == NULL) {
	g_free(tape_server_name);
	tape_server_name = g_strdup(server_name);
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
	g_free(tape_device_name);
	tape_device_name = g_strdup(l + 4);
    }

    if (g_str_equal(tape_device_name, "/dev/null"))
    {
	g_printf(_("amrecover: warning: using %s as the tape device will not work\n"),
	       tape_device_name);
    }

    first=1;
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist)) {
	if (elist->tape[0] != '/' && strncmp(elist->tape, "HOLDING:/",9) != 0) {
	    if(first) {
		g_printf(_("\nExtracting files using tape drive %s on host %s.\n"),
			tape_device_name, tape_server_name);
		g_printf(_("The following tapes are needed:"));
		first=0;
	    }
	    else
		g_printf("                               ");
	    tlist = unmarshal_tapelist_str(elist->tape,
			am_has_feature(indexsrv_features,
				       fe_amrecover_storage_in_marshall));
	    for(a_tlist = tlist ; a_tlist != NULL; a_tlist = a_tlist->next)
		g_printf(" %s", a_tlist->label);
	    g_printf("\n");
	    free_tapelist(tlist);
	}
    }
    first=1;
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist)) {
	if (elist->tape[0] == '/' || strncmp(elist->tape, "HOLDING:/",9) == 0) {
	    if(first) {
		g_printf(_("\nExtracting files from holding disk on host %s.\n"),
			tape_server_name);
		g_printf(_("The following files are needed:"));
		first=0;
	    }
	    else
		g_printf("                               ");
	    tlist = unmarshal_tapelist_str(elist->tape,
			am_has_feature(indexsrv_features,
				       fe_amrecover_storage_in_marshall));
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
	if (elist->tape[0] == '/' || strncmp(elist->tape, "HOLDING:/",9) == 0) {
	    g_free(dump_device_name);
	    if (elist->tape[0] == '/') {
		dump_device_name = g_strdup(elist->tape);
	    } else {
		dump_device_name = g_strdup(elist->tape+8);
	    }
	    g_printf(_("Extracting from file "));
	    tlist = unmarshal_tapelist_str(dump_device_name,
			am_has_feature(indexsrv_features,
				       fe_amrecover_storage_in_marshall));
	    for(a_tlist = tlist; a_tlist != NULL; a_tlist = a_tlist->next)
		g_printf(" %s", a_tlist->label);
	    g_printf("\n");
	    free_tapelist(tlist);
	}
	else {
	    g_printf(_("Extracting files using tape drive %s on host %s.\n"),
		   tape_device_name, tape_server_name);
	    tlist = unmarshal_tapelist_str(elist->tape,
			am_has_feature(indexsrv_features,
				       fe_amrecover_storage_in_marshall));
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
	    g_free(dump_device_name);
	    dump_device_name = g_strdup(tape_device_name);
	}
	g_free(dump_datestamp);
	dump_datestamp = g_strdup(elist->date);

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

	if (am_has_feature(indexsrv_features, fe_amrecover_storage_in_marshall) &&
	    !am_has_feature(indexsrv_features, fe_amidxtaped_storage_in_marshall)) {
	    tlist = unmarshal_tapelist_str(elist->tape, 1);
	    etapelist = marshal_tapelist(tlist, 1, 7);
	    free_tapelist(tlist);
	} else if (!am_has_feature(indexsrv_features, fe_amidxtaped_storage_in_marshall) &&
	            am_has_feature(indexsrv_features, fe_amidxtaped_storage_in_marshall)) {
	    tlist = unmarshal_tapelist_str(elist->tape, 0);
	    for(a_tlist = tlist; a_tlist != NULL; a_tlist = a_tlist->next)
		a_tlist->storage = g_strdup(get_config_name());
	    etapelist = marshal_tapelist(tlist, 1, 0);
	    free_tapelist(tlist);
	} else {
	    etapelist = g_strdup(elist->tape);
	}
	/* connect to the tape handler daemon on the tape drive server */
	if ((extract_files_setup(etapelist, elist->fileno)) == -1)
	{
	    g_fprintf(stderr, _("amrecover - can't talk to tape server: %s\n"),
		    errstr);
	    g_free(etapelist);
	    return;
	}
	g_free(etapelist);
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
    int ports[NSTREAMS], *response_error = datap;
    guint i;
    char *p;
    char *tok;
    char *extra = NULL;

    assert(response_error != NULL);
    assert(sech != NULL);
    memset(ports, -1, sizeof(ports));

    if (pkt == NULL) {
	g_free(errstr);
	errstr = g_strdup_printf(_("[request failed: %s]"),
                                 security_geterror(sech));
	*response_error = 1;
	return;
    }
    security_close_connection(sech, dump_hostname);

    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	g_fprintf(stderr, _("got nak response:\n----\n%s\n----\n\n"), pkt->body);
#endif

	tok = strtok(pkt->body, " ");
	if (tok == NULL || !g_str_equal(tok, "ERROR"))
	    goto bad_nak;

	tok = strtok(NULL, "\n");
	if (tok != NULL) {
	    g_free(errstr);
	    errstr = g_strconcat("NAK: ", tok, NULL);
	    *response_error = 1;
	} else {
bad_nak:
	    g_free(errstr);
	    errstr = g_strdup("request NAK");
	    *response_error = 2;
	}
	return;
    }

    if (pkt->type != P_REP) {
	g_free(errstr);
	errstr = g_strdup_printf(_("received strange packet type %s: %s"),
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
	if (g_str_equal(tok, "ERROR")) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL)
		tok = _("[bogus error packet]");
	    g_free(errstr);
	    errstr = g_strdup(tok);
	    *response_error = 2;
	    return;
	}


        /*
         * Regular packets have CONNECT followed by three streams
         */
        if (g_str_equal(tok, "CONNECT")) {

	    /*
	     * Parse the three stream specifiers out of the packet.
	     */
	    for (i = 0; i < NSTREAMS; i++) {
		tok = strtok(NULL, " ");
		if (tok == NULL || !g_str_equal(tok,
                                                amidxtaped_streams[i].name)) {
		    extra = g_strdup_printf(_("CONNECT token is \"%s\": expected \"%s\""),
				      tok ? tok : "(null)",
				      amidxtaped_streams[i].name);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = g_strdup_printf(_("CONNECT %s token is \"%s\": expected a port number"),
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
	if (g_str_equal(tok, "OPTIONS")) {
	    tok = strtok(NULL, "\n");
	    if (tok == NULL) {
		extra = g_strdup(_("OPTIONS token is missing"));
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
	extra = g_strdup_printf("next token is \"%s\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\""),
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
            g_free(errstr);
            errstr = g_strdup_printf(_("[could not connect %s stream: %s]"),
                                     amidxtaped_streams[i].name, security_geterror(sech));
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
            g_free(errstr);
            errstr = g_strdup_printf(_("[could not authenticate %s stream: %s]"),
                                     amidxtaped_streams[i].name, security_stream_geterror(amidxtaped_streams[i].fd));
	    goto connect_error;
	}
    }

    /*
     * The CTLFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (amidxtaped_streams[CTLFD].fd == NULL) {
        g_free(errstr);
        errstr = g_strdup("[couldn't open CTL streams]");
        goto connect_error;
    }
    if (amidxtaped_streams[DATAFD].fd == NULL) {
        g_free(errstr);
        errstr = g_strdup("[couldn't open DATA streams]");
        goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    return;

parse_error:
    if (extra) {
	g_free(errstr);
	errstr = g_strdup_printf(_("[parse of reply message failed: %s]"),
                                 extra);
    } else {
	g_free(errstr);
	errstr = g_strdup("[parse of reply message failed: (no additional information)");
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
    guint i;

    for (i = 0; i < NSTREAMS; i++) {
        if (amidxtaped_streams[i].fd != NULL) {
            security_stream_close(amidxtaped_streams[i].fd);
            amidxtaped_streams[i].fd = NULL;
        }
    }

    if (stderr_isatty) {
	fprintf(stderr, "\n");
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
	ctl_buffer = g_strdup("");

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
        newbuf = g_malloc(strlen(ctl_buffer)+size+1);
        strncpy(newbuf, ctl_buffer, (size_t)(strlen(ctl_buffer) + size + 1));
        memcpy(newbuf+strlen(ctl_buffer), buf, (size_t)size);
        newbuf[strlen(ctl_buffer)+size] = '\0';
        amfree(ctl_buffer);
        ctl_buffer = newbuf;
	amfree(buf);
    }

    s = strstr(ctl_buffer,"\r\n");
    *s = '\0';
    newbuf = g_strdup(s+2);
    amidxtaped_line = g_strdup(ctl_buffer);
    amfree(ctl_buffer);
    ctl_buffer = newbuf;
    return 0;
}

static void
handle_child_out(
    void *cookie)
{
    cdata_t  *cdata = cookie;
    ssize_t   nread;
    char     *b, *p;
    gint64    len;

    if (cdata->buffer == NULL) {
	/* allocate initial buffer */
	cdata->buffer = g_malloc(2048);
	cdata->first = 0;
	cdata->size = 0;
	cdata->allocated_size = 2048;
    } else if (cdata->first > 0) {
	if (cdata->allocated_size - cdata->size - cdata->first < 1024) {
	    memmove(cdata->buffer, cdata->buffer + cdata->first,
	                            cdata->size);
	    cdata->first = 0;
	}
    } else if (cdata->allocated_size - cdata->size < 1024) {
	/* double the size of the buffer */
	cdata->allocated_size *= 2;
	cdata->buffer = g_realloc(cdata->buffer, cdata->allocated_size);
    }

    nread = read(cdata->fd, cdata->buffer + cdata->first + cdata->size,
		 cdata->allocated_size - cdata->first - cdata->size - 2);

    if (nread <= 0) {
	event_release(cdata->event);
	aclose(cdata->fd);
	if (cdata->size > 0 && cdata->buffer[cdata->first + cdata->size - 1] != '\n') {
	    /* Add a '\n' at end of buffer */
	    cdata->buffer[cdata->first + cdata->size] = '\n';
	    cdata->size++;
	}
    } else {
	cdata->size += nread;
    }

    /* process all complete lines */
    b = cdata->buffer + cdata->first;
    b[cdata->size] = '\0';
    while (b < cdata->buffer + cdata->first + cdata->size &&
           (p = strchr(b, '\n')) != NULL) {
        *p = '\0';
	if (last_is_size)
	    g_fprintf(cdata->output, "\n");
	if (cdata->name) {
            g_fprintf(cdata->output, "%s: %s\n", cdata->name, b);
	} else {
            g_fprintf(cdata->output, "%s\n", b);
	}
	last_is_size = FALSE;
        len = p - b + 1;
        cdata->first += len;
        cdata->size -= len;
        b = p + 1;
    }

    if (nread <= 0) {
        g_free(cdata->name);
        g_free(cdata->buffer);
    }


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
	g_free(errstr);
	errstr = g_strconcat(_("amidxtaped read: "),
                             security_stream_geterror(amidxtaped_streams[DATAFD].fd),
                             NULL);
	return;
    }

    /*
     * EOF.  Stop and return.
     */
    if (size == 0) {
	security_stream_close(amidxtaped_streams[DATAFD].fd);
	amidxtaped_streams[DATAFD].fd = NULL;
	aclose(ctl_data->child_in[1]);
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
	    /* wait to read more data */
	    return;
	} else if (header_size > 32768) {
	    error("header_size is %d\n", header_size);
	}
	assert (to_move == size);
	security_stream_read_cancel(amidxtaped_streams[DATAFD].fd);
	/* parse the file header */
	fh_init(&ctl_data->file);
	parse_file_header(header_buf, &ctl_data->file, (size_t)header_size);

	/* call backup_support_option */
	g_options.config = get_config_name();
	g_options.hostname = dump_hostname;
	if (g_str_equal(ctl_data->file.program, "APPLICATION")) {
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
	    /* send DATA-PATH request */
            char *msg = g_strdup_printf("AVAIL-DATAPATH%s%s",
                (data_path_set & DATA_PATH_AMANDA) ? " AMANDA" : "",
                (data_path_set & DATA_PATH_DIRECTTCP) ? " DIRECT-TCP" : "");
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, msg);
	    g_free(msg);
	} else {
	    start_processing_data(ctl_data);
	}
    } else {
	/* print every second */
	time_t current_time = time(NULL);
	ctl_data->bytes_read += size;
	if (current_time > last_time) {
	    last_time = current_time;

	    if (stderr_isatty) {
		if (last_is_size) {
		    fprintf(stderr, "\r%lld kb ",
			    (long long)ctl_data->bytes_read/1024);
		} else {
		    fprintf(stderr, "%lld kb ",
			    (long long)ctl_data->bytes_read/1024);
		    last_is_size = TRUE;
		}
	    } else {
		fprintf(stderr, "%lld kb\n",
			(long long)ctl_data->bytes_read/1024);
	    }
	}

	/* Only the data is sent to the child */
	/*
	 * We ignore errors while writing to the index file.
	 */
	(void)full_write(ctl_data->child_in[1], buf, (size_t)size);
	crc32_add((uint8_t *)buf, size, &crc_in);
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
	if (g_str_equal(ctl_data->file.program, "GNUTAR") ||
	    (g_str_equal(ctl_data->file.program, "APPLICATION") &&
	     g_str_equal(ctl_data->file.application, "amgtar"))) {
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
    if (pipe(ctl_data->child_in) == -1) {
	error(_("extract_list - error setting up pipe to extractor: %s\n"),
	      strerror(errno));
	/*NOTREACHED*/
    }

    if (pipe(ctl_data->child_out) == -1) {
	error(_("extract_list - error setting up pipe to extractor: %s\n"),
	      strerror(errno));
	/*NOTREACHED*/
    }

    if (pipe(ctl_data->child_err) == -1) {
	error(_("extract_list - error setting up pipe to extractor: %s\n"),
	      strerror(errno));
	/*NOTREACHED*/
    }

    /* decrypt */
    ctl_data->decrypt_cdata.fd = -1;
    if (ctl_data->file.encrypted) {
	char *argv[3];
	int  crypt_out;
	int pipe_decrypt_err[2];

	if (pipe(pipe_decrypt_err) == -1) {
	    error(_("extract_list - error setting up pipe to extractor: %s\n"),
	          strerror(errno));
	    /*NOTREACHED*/
	}

	g_debug("image is encrypted %s %s", ctl_data->file.clnt_encrypt, ctl_data->file.clnt_decrypt_opt);
	argv[0] = ctl_data->file.clnt_encrypt;
	argv[1] = ctl_data->file.clnt_decrypt_opt;
	argv[2] = NULL;
	pipespawnv(ctl_data->file.clnt_encrypt, STDOUT_PIPE, 0,
		   &ctl_data->child_in[0], &crypt_out, &pipe_decrypt_err[1],
		   argv);
	ctl_data->child_in[0] = crypt_out;
	aclose(pipe_decrypt_err[1]);
	ctl_data->decrypt_cdata.fd = pipe_decrypt_err[0];
	ctl_data->decrypt_cdata.output = stderr;
	ctl_data->decrypt_cdata.name = g_strdup(ctl_data->file.clnt_encrypt);
	ctl_data->decrypt_cdata.buffer = NULL;
	ctl_data->decrypt_cdata.event = NULL;
    }

    /* decompress */
    ctl_data->decompress_cdata.fd = -1;
    if (ctl_data->file.compressed) {
	char *argv[3];
	int  comp_out;
	char *comp_prog;
	char *comp_arg;
	int pipe_decompress_err[2];

	if (pipe(pipe_decompress_err) == -1) {
	    error(_("extract_list - error setting up pipe to extractor: %s\n"),
	          strerror(errno));
	    /*NOTREACHED*/
	}

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
	pipespawnv(comp_prog, STDOUT_PIPE, 0, &ctl_data->child_in[0],
		   &comp_out, &pipe_decompress_err[1], argv);
	ctl_data->child_in[0] = comp_out;
	aclose(pipe_decompress_err[1]);
	ctl_data->decrypt_cdata.fd = pipe_decompress_err[0];
	ctl_data->decrypt_cdata.output = stderr;
	ctl_data->decrypt_cdata.name = g_strdup(comp_prog);
	ctl_data->decrypt_cdata.buffer = NULL;
	ctl_data->decrypt_cdata.event = NULL;
    }

    /* compute native-crc */
    if (ctl_data->file.encrypted || ctl_data->file.compressed) {
	if (pipe(ctl_data->crc_pipe) == -1) {
	    error(_("extract_list - error setting up ctc pipe: %s\n"),
		  strerror(errno));
	    /*NOTREACHED*/
	}

	native_crc.in  = ctl_data->child_in[0];
        native_crc.out = ctl_data->crc_pipe[1];
	crc32_init(&native_crc.crc);
	ctl_data->child_in[0] = ctl_data->crc_pipe[0];
	native_crc.thread = g_thread_create(handle_crc_thread,
                                 (gpointer)&native_crc, TRUE, NULL);
    } else {
	native_crc.thread = NULL;
    }

    /* okay, ready to extract. fork a child to do the actual work */
    if ((ctl_data->pid = fork()) == 0) {
	/* this is the child process */
	/* never gets out of this clause */
	aclose(ctl_data->child_in[1]);
	aclose(ctl_data->child_out[0]);
	aclose(ctl_data->child_err[0]);
	extract_files_child(ctl_data);
	/*NOTREACHED*/
    }

    stderr_isatty = isatty(fileno(stderr));

    if (ctl_data->pid == -1) {
	g_free(errstr);
	errstr = g_strdup(_("writer_intermediary - error forking child"));
	g_printf(_("writer_intermediary - error forking child"));
	return;
    }
    last_is_size = FALSE;
    aclose(ctl_data->child_in[0]);
    aclose(ctl_data->child_out[1]);
    aclose(ctl_data->child_err[1]);
    security_stream_read(amidxtaped_streams[DATAFD].fd, read_amidxtaped_data,
			 ctl_data);

    ctl_data->child_out_cdata.fd = ctl_data->child_out[0];
    ctl_data->child_out_cdata.output = stdout;
    ctl_data->child_out_cdata.name = NULL;
    ctl_data->child_out_cdata.buffer = NULL;
    ctl_data->child_out_cdata.event = event_register(
				(event_id_t)ctl_data->child_out[0],
				EV_READFD, handle_child_out,
				&ctl_data->child_out_cdata);

    ctl_data->child_err_cdata.fd = ctl_data->child_err[0];
    ctl_data->child_err_cdata.output = stderr;
    ctl_data->child_err_cdata.name = NULL;
    ctl_data->child_err_cdata.buffer = NULL;
    ctl_data->child_err_cdata.event = event_register(
				(event_id_t)ctl_data->child_err[0],
				EV_READFD, handle_child_out,
				&ctl_data->child_err_cdata);

    if (ctl_data->decrypt_cdata.fd != -1) {
	ctl_data->decrypt_cdata.event = event_register(
				(event_id_t)ctl_data->decrypt_cdata.fd,
				EV_READFD, handle_child_out,
				&ctl_data->decrypt_cdata);
    }

    if (ctl_data->decompress_cdata.fd != -1) {
	ctl_data->decompress_cdata.event = event_register(
				(event_id_t)ctl_data->decompress_cdata.fd,
				EV_READFD, handle_child_out,
				&ctl_data->decompress_cdata);
    }

    if (am_has_feature(tapesrv_features, fe_amidxtaped_datapath)) {
	send_to_tape_server(amidxtaped_streams[CTLFD].fd, "DATAPATH-OK");
    }
}

static gpointer
handle_crc_thread(
    gpointer data)
{
    send_crc_t *crc = (send_crc_t *)data;
    uint8_t  buf[32768];
    size_t   size;

    while ((size = full_read(crc->in, buf, 32768)) > 0) {
        if (full_write(crc->out, buf, size) == size) {
            crc32_add(buf, size, &crc->crc);
        }
    }
    close(crc->in);
    close(crc->out);

    return NULL;
}

