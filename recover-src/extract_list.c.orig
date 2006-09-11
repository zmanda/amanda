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
 * $Id: extract_list.c,v 1.115 2006/07/11 14:53:24 martinea Exp $
 *
 * implements the "extract" command in amrecover
 */

#include "amanda.h"
#include "version.h"
#include "amrecover.h"
#include "fileheader.h"
#include "dgram.h"
#include "stream.h"
#include "tapelist.h"
#ifdef SAMBA_CLIENT
#include "findpass.h"
#endif
#include "util.h"
#include "clientconf.h"
#include "protocol.h"
#include "event.h"
#include "security.h"

typedef struct EXTRACT_LIST_ITEM {
    char *path;
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
char *amidxtaped_client_get_security_conf(char *, void *);
static char *dump_device_name = NULL;
static char *errstr;
static char *amidxtaped_line = NULL;

extern char *localhost;

/* global pid storage for interrupt handler */
pid_t extract_restore_child_pid = -1;

static EXTRACT_LIST *extract_list = NULL;
static const security_driver_t *amidxtaped_secdrv;

#ifdef SAMBA_CLIENT
unsigned short samba_extract_method = SAMBA_TAR;
#endif /* SAMBA_CLIENT */

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
static void extract_files_child(int in_fd, EXTRACT_LIST *elist);
static void send_to_tape_server(security_stream_t *stream, char *cmd);
int writer_intermediary(EXTRACT_LIST *elist);
int get_amidxtaped_line(void);
static void read_amidxtaped_data(void *, void *, ssize_t);

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
    fd_set readset;
    struct timeval timeout;
    char *dataptr;
    ssize_t spaceleft;
    int nfound;

    if(datafd < 0 || datafd >= FD_SETSIZE) {
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
	    fprintf(stderr,"select error: %s\n", strerror(errno));
            size = -1;
	    break;
        }

	if (nfound == 0) {
            /* Select timed out. */
            if (timeout_s != 0)  {
                /* Not polling: a real read timeout */
                fprintf(stderr,"timeout waiting for restore\n");
                fprintf(stderr,"increase READ_TIMEOUT in recover-src/extract_list.c if your tape is slow\n");
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
	        fprintf(stderr, "read_buffer: read error - %s",
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
		dbprintf(("removing path %s, it is included in %s\n",
			  fn2->path, fn1->path));
		ofn2 = fn2;
		fn2 = fn2->next;
		amfree(ofn2->path);
		amfree(ofn2);
		pfn2->next = fn2;
	    } else if (remove_fn1 == 0) {
		pfn2 = fn2;
		fn2 = fn2->next;
	    }
	}

	if(remove_fn1 != 0) {
	    /* fn2->path is always valid */
	    /*@i@*/ dbprintf(("removing path %s, it is included in %s\n",
	    /*@i@*/	      fn1->path, fn2->path));
	    ofn1 = fn1;
	    fn1 = fn1->next;
	    amfree(ofn1->path);
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
	    fprintf(stderr,"Can't unlink %s: %s\n", ul->path, strerror(errno));
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
			    printf("WARNING: %s is not a directory, "
				   "it will be deleted.\n",
				   path);
			}
		    }
		}
		else if (errno != ENOENT) {
		    printf("Can't stat %s: %s\n", path, strerror(errno));
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
			printf("WARNING: All existing files in %s "
			       "will be deleted.\n", filename);
		    }
		} else if(S_ISREG(stat_buf.st_mode)) {
		    printf("WARNING: Existing file %s will be overwritten\n",
			   filename);
		} else {
		    if (add_to_unlink_list(filename)) {
			printf("WARNING: Existing entry %s will be deleted\n",
			       filename);
		    }
		}
	    } else if (errno != ENOENT) {
		printf("Can't stat %s: %s\n", filename, strerror(errno));
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
    char *ditem_path = NULL;

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
		    amfree(ditem_path);
		    return 1;
		}
		curr=curr->next;
	    }
	    that = (EXTRACT_LIST_ITEM *)alloc(sizeof(EXTRACT_LIST_ITEM));
            that->path = stralloc(ditem_path);
	    that->next = this->files;
	    this->files = that;		/* add at front since easiest */
	    amfree(ditem_path);
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
    that->path = stralloc(ditem_path);
    that->next = NULL;
    this->files = that;

    /* add this in date increasing order          */
    /* because restore must be done in this order */
    /* add at begining */
    if(extract_list==NULL || strcmp(this->date,extract_list->date) < 0) 
    {
	this->next = extract_list;
	extract_list = this;
	amfree(ditem_path);
	return 0;
    }
    for (this1 = extract_list; this1->next != NULL; this1 = this1->next)
    {
	/* add in the middle */
	if(strcmp(this->date,this1->next->date) < 0)
	{
	    this->next = this1->next;
	    this1->next = this;
	    amfree(ditem_path);
	    return 0;
	}
    }
    /* add at end */
    this->next = NULL;
    this1->next = this;
    amfree(ditem_path);
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


void
add_glob(
    char *	glob)
{
    char *regex;
    char *regex_path;
    char *s;
    char *uqglob = unquote_string(glob);
 
    regex = glob_to_regex(uqglob);
    dbprintf(("add_glob (%s) -> %s\n", uqglob, regex));
    if ((s = validate_regexp(regex)) != NULL) {
	printf("%s is not a valid shell wildcard pattern: ", glob);
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
    amfree(regex);
    amfree(uqglob);
}

void
add_regex(
    char *	regex)
{
    char *s;
    char *uqregex = unquote_string(regex);
 
    if ((s = validate_regexp(uqregex)) != NULL) {
	printf("\"%s\" is not a valid regular expression: ", regex);
	puts(s);
    } else {
        add_file(uqregex, regex);
    }
    amfree(uqregex);
}

void
add_file(
    char *	path,
    char *	regex)
{
    DIR_ITEM *ditem, lditem;
    char *path_on_disk = NULL;
    char *path_on_disk_slash = NULL;
    char *cmd = NULL;
    char *err = NULL;
    int i;
    ssize_t j;
    char *dir, *dir_undo, dir_undo_ch = '\0';
    char *ditem_path = NULL;
    char *l = NULL;
    int  added;
    char *s, *fp, *quoted;
    int ch;
    int found_one;
    int dir_entries;

    if (disk_path == NULL) {
	printf("Must select directory before adding files\n");
	return;
    }
    memset(&lditem, 0, sizeof(lditem)); /* Prevent use of bogus data... */

    dbprintf(("add_file: Looking for \"%s\"\n", regex));

    if(strcmp(regex, "/[/]*$") == 0) {	/* "/" behave like "." */
	regex = "\\.[/]*$";
    }
    else if(strcmp(regex, "[^/]*[/]*$") == 0) {		/* "*" */
	//regex = 
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
	    path_on_disk = stralloc(regex);
	} else {
	    /* Prepend '/' */
	    path_on_disk = stralloc2("/", regex);
	}
    } else {
	char *clean_disk_path = clean_regex(disk_path);
	path_on_disk = vstralloc(clean_disk_path, "/", regex, NULL);
	amfree(clean_disk_path);
    }

    path_on_disk_slash = stralloc2(path_on_disk, "/");

    dbprintf(("add_file: Converted path=\"%s\" to path_on_disk=\"%s\"\n",
	      regex, path_on_disk));

    found_one = 0;
    dir_entries = 0;
    for (ditem=get_dir_list(); ditem!=NULL; ditem=get_next_dir_item(ditem))
    {
	dir_entries++;
	quoted = quote_string(ditem->path);
	dbprintf(("add_file: Pondering ditem->path=%s\n", quoted));
	amfree(quoted);
	if (match(path_on_disk, ditem->path)
	    || match(path_on_disk_slash, ditem->path))
	{
	    found_one = 1;
	    j = (ssize_t)strlen(ditem->path);
	    if((j > 0 && ditem->path[j-1] == '/')
	       || (j > 1 && ditem->path[j-2] == '/' && ditem->path[j-1] == '.'))
	    {	/* It is a directory */
		ditem_path = newstralloc(ditem_path, ditem->path);
		clean_pathname(ditem_path);

		cmd = stralloc2("ORLD ", ditem_path);
		if(send_command(cmd) == -1) {
		    amfree(cmd);
		    amfree(ditem_path);
		    amfree(path_on_disk);
		    amfree(path_on_disk_slash);
		    exit(1);
		}
		amfree(cmd);
		cmd = NULL;
		/* skip preamble */
		if ((i = get_reply_line()) == -1) {
		    amfree(ditem_path);
		    amfree(path_on_disk);
		    amfree(path_on_disk_slash);
		    exit(1);
		}
		if(i==0) {		/* assume something wrong */
		    amfree(ditem_path);
		    amfree(path_on_disk);
		    amfree(path_on_disk_slash);
		    l = reply_line();
		    printf("%s\n", l);
		    return;
		}
		dir_undo = NULL;
		added=0;
                lditem.path = newstralloc(lditem.path, ditem->path);
		/* skip the last line -- duplicate of the preamble */

		while ((i = get_reply_line()) != 0) {
		    if (i == -1) {
			amfree(ditem_path);
		        amfree(path_on_disk);
		        amfree(path_on_disk_slash);
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
#define sc "201-"
		    if(strncmp(l, sc, sizeof(sc)-1) != 0) {
			err = "bad reply: not 201-";
			continue;
		    }

		    s = l + sizeof(sc)-1;
		    ch = *s++;
#undef sc
		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = "bad reply: missing date field";
			continue;
		    }
                    fp = s-1;
                    skip_non_whitespace(s, ch);
                    s[-1] = '\0';
                    lditem.date = newstralloc(lditem.date, fp);
                    s[-1] = (char)ch;

		    skip_whitespace(s, ch);
		    if(ch == '\0' || sscanf(s - 1, "%d", &lditem.level) != 1) {
			err = "bad reply: cannot parse level field";
			continue;
		    }
		    skip_integer(s, ch);

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = "bad reply: missing tape field";
			continue;
		    }
                    fp = s-1;
                    skip_non_whitespace(s, ch);
                    s[-1] = '\0';
                    lditem.tape = newstralloc(lditem.tape, fp);
                    s[-1] = (char)ch;

		    if(am_has_feature(indexsrv_features, fe_amindexd_fileno_in_ORLD)) {
			skip_whitespace(s, ch);
			if(ch == '\0' || sscanf(s - 1, OFF_T_FMT,
				(OFF_T_FMT_TYPE *)&lditem.fileno) != 1) {
			    err = "bad reply: cannot parse fileno field";
			    continue;
			}
			skip_integer(s, ch);
		    }

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = "bad reply: missing directory field";
			continue;
		    }
		    dir = s - 1;
		    skip_quoted_string(s, ch);
		    dir_undo = s - 1;
		    dir_undo_ch = *dir_undo;
		    *dir_undo = '\0';

		    switch(add_extract_item(&lditem)) {
		    case -1:
			printf("System error\n");
			dbprintf(("add_file: (Failed) System error\n"));
			break;

		    case  0:
			quoted = quote_string(lditem.path);
			printf("Added dir %s at date %s\n",
			       quoted, lditem.date);
			dbprintf(("add_file: (Successful) Added dir %s at date %s\n",
				  quoted, lditem.date));
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
		    printf("dir %s already added\n", quoted);
		    dbprintf(("add_file: dir %s already added\n", quoted));
		    amfree(quoted);
		}
	    }
	    else /* It is a file */
	    {
		switch(add_extract_item(ditem)) {
		case -1:
		    printf("System error\n");
		    dbprintf(("add_file: (Failed) System error\n"));
		    break;

		case  0:
		    quoted = quote_string(ditem->path);
		    printf("Added file %s\n", quoted);
		    dbprintf(("add_file: (Successful) Added %s\n", quoted));
		    amfree(quoted);
		    break;

		case  1:
		    quoted = quote_string(ditem->path);
		    printf("File %s already added\n", quoted);
		    dbprintf(("add_file: file %s already added\n", quoted));
		    amfree(quoted);
		}
	    }
	}
    }

    amfree(cmd);
    amfree(ditem_path);
    amfree(path_on_disk);
    amfree(path_on_disk_slash);

    amfree(lditem.path);
    amfree(lditem.date);
    amfree(lditem.tape);

    if(! found_one) {
	quoted = quote_string(path);
	printf("File %s doesn't exist in directory\n", quoted);
	dbprintf(("add_file: (Failed) File %s doesn't exist in directory\n",
	          quoted));
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
    char *uqglob = unquote_string(glob);
 
    regex = glob_to_regex(uqglob);
    dbprintf(("delete_glob (%s) -> %s\n", uqglob, regex));
    if ((s = validate_regexp(regex)) != NULL) {
	printf("\"%s\" is not a valid shell wildcard pattern: ", glob);
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
    amfree(regex);
    amfree(uqglob);
}

void
delete_regex(
    char *	regex)
{
    char *s;
    char *uqregex = unquote_string(regex);

    if ((s = validate_regexp(regex)) != NULL) {
	printf("\"%s\" is not a valid regular expression: ", regex);
	puts(s);
    } else {
	delete_file(uqregex, uqregex);
    }
    amfree(uqregex);
}

void
delete_file(
    char *	path,
    char *	regex)
{
    DIR_ITEM *ditem, lditem;
    char *path_on_disk = NULL;
    char *path_on_disk_slash = NULL;
    char *cmd = NULL;
    char *err = NULL;
    int i;
    ssize_t j;
    char *date;
    char *tape, *tape_undo, tape_undo_ch = '\0';
    char *dir_undo, dir_undo_ch = '\0';
    int  level = 0;
    off_t fileno;
    char *ditem_path = NULL;
    char *l = NULL;
    int  deleted;
    char *s;
    int ch;
    int found_one;
    char *quoted;

    if (disk_path == NULL) {
	printf("Must select directory before deleting files\n");
	return;
    }
    memset(&lditem, 0, sizeof(lditem)); /* Prevent use of bogus data... */

    dbprintf(("delete_file: Looking for \"%s\"\n", path));

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
		path_on_disk = stralloc("/\\.[/]*$");
	    } else {
		/* No mods needed if already starts with '/' */
		path_on_disk = stralloc(regex);
	    }
	} else {
	    /* Prepend '/' */
	    path_on_disk = stralloc2("/", regex);
	}
    } else {
	char *clean_disk_path = clean_regex(disk_path);
	path_on_disk = vstralloc(clean_disk_path, "/", regex, NULL);
	amfree(clean_disk_path);
    }

    path_on_disk_slash = stralloc2(path_on_disk, "/");

    dbprintf(("delete_file: Converted path=\"%s\" to path_on_disk=\"%s\"\n",
	      regex, path_on_disk));
    found_one = 0;
    for (ditem=get_dir_list(); ditem!=NULL; ditem=get_next_dir_item(ditem))
    {
	quoted = quote_string(ditem->path);
	dbprintf(("delete_file: Pondering ditem->path=%s\n", quoted));
	amfree(quoted);
	if (match(path_on_disk, ditem->path)
	    || match(path_on_disk_slash, ditem->path))
	{
	    found_one = 1;
	    j = (ssize_t)strlen(ditem->path);
	    if((j > 0 && ditem->path[j-1] == '/')
	       || (j > 1 && ditem->path[j-2] == '/' && ditem->path[j-1] == '.'))
	    {	/* It is a directory */
		ditem_path = newstralloc(ditem_path, ditem->path);
		clean_pathname(ditem_path);

		cmd = stralloc2("ORLD ", ditem_path);
		if(send_command(cmd) == -1) {
		    amfree(cmd);
		    amfree(ditem_path);
		    amfree(path_on_disk);
		    amfree(path_on_disk_slash);
		    exit(1);
		}
		amfree(cmd);
		/* skip preamble */
		if ((i = get_reply_line()) == -1) {
		    amfree(ditem_path);
		    amfree(path_on_disk);
		    amfree(path_on_disk_slash);
		    exit(1);
		}
		if(i==0)		/* assume something wrong */
		{
		    amfree(ditem_path);
		    amfree(path_on_disk);
		    amfree(path_on_disk_slash);
		    l = reply_line();
		    printf("%s\n", l);
		    return;
		}
		deleted=0;
                lditem.path = newstralloc(lditem.path, ditem->path);
		amfree(cmd);
		tape_undo = dir_undo = NULL;
		/* skip the last line -- duplicate of the preamble */
		while ((i = get_reply_line()) != 0)
		{
		    if (i == -1) {
			amfree(ditem_path);
			amfree(path_on_disk);
			amfree(path_on_disk_slash);
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
#define sc "201-"
		    if(strncmp(l, sc, sizeof(sc)-1) != 0) {
			err = "bad reply: not 201-";
			continue;
		    }
		    s = l + sizeof(sc)-1;
		    ch = *s++;
#undef sc
		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = "bad reply: missing date field";
			continue;
		    }
		    date = s - 1;
		    skip_non_whitespace(s, ch);
		    *(s - 1) = '\0';

		    skip_whitespace(s, ch);
		    if(ch == '\0' || sscanf(s - 1, "%d", &level) != 1) {
			err = "bad reply: cannot parse level field";
			continue;
		    }
		    skip_integer(s, ch);

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = "bad reply: missing tape field";
			continue;
		    }
		    tape = s - 1;
		    skip_non_whitespace(s, ch);
		    tape_undo = s - 1;
		    tape_undo_ch = *tape_undo;
		    *tape_undo = '\0';

		    if(am_has_feature(indexsrv_features, fe_amindexd_fileno_in_ORLD)) {
			skip_whitespace(s, ch);
			if(ch == '\0' || sscanf(s - 1, OFF_T_FMT,
				(OFF_T_FMT_TYPE *)&fileno) != 1) {
			    err = "bad reply: cannot parse fileno field";
			    continue;
			}
			skip_integer(s, ch);
		    }

		    skip_whitespace(s, ch);
		    if(ch == '\0') {
			err = "bad reply: missing directory field";
			continue;
		    }
		    skip_non_whitespace(s, ch);
		    dir_undo = s - 1;
		    dir_undo_ch = *dir_undo;
		    *dir_undo = '\0';

                    lditem.date = newstralloc(lditem.date, date);
		    lditem.level=level;
                    lditem.tape = newstralloc(lditem.tape, tape);
		    switch(delete_extract_item(&lditem)) {
		    case -1:
			printf("System error\n");
			dbprintf(("delete_file: (Failed) System error\n"));
			break;
		    case  0:
			printf("Deleted dir %s at date %s\n", ditem_path, date);
			dbprintf(("delete_file: (Successful) Deleted dir %s at date %s\n",
				  ditem_path, date));
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
		    printf("Warning - dir '%s' not on tape list\n",
			   ditem_path);
		    dbprintf(("delete_file: dir '%s' not on tape list\n",
			      ditem_path));
		}
	    }
	    else
	    {
		switch(delete_extract_item(ditem)) {
		case -1:
		    printf("System error\n");
		    dbprintf(("delete_file: (Failed) System error\n"));
		    break;
		case  0:
		    printf("Deleted %s\n", ditem->path);
		    dbprintf(("delete_file: (Successful) Deleted %s\n",
			      ditem->path));
		    break;
		case  1:
		    printf("Warning - file '%s' not on tape list\n",
			   ditem->path);
		    dbprintf(("delete_file: file '%s' not on tape list\n",
			      ditem->path));
		    break;
		}
	    }
	}
    }
    amfree(cmd);
    amfree(ditem_path);
    amfree(path_on_disk);
    amfree(path_on_disk_slash);

    if(! found_one) {
	printf("File %s doesn't exist in directory\n", path);
	dbprintf(("delete_file: (Failed) File %s doesn't exist in directory\n",
	          path));
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
	    printf("Warning - can't pipe through %s\n", pager);
	    fp = stdout;
	}
	amfree(pager_command);
    }
    else
    {
	uqfile = unquote_string(file);
	if ((fp = fopen(uqfile, "w")) == NULL)
	{
	    printf("Can't open file %s to print extract list into\n", file);
	    amfree(uqfile);
	    return;
	}
	amfree(uqfile);
    }

    for (this = extract_list; this != NULL; this = this->next)
    {
	fprintf(fp, "TAPE %s LEVEL %d DATE %s\n",
		this->tape, this->level, this->date);
	for (that = this->files; that != NULL; that = that->next)
	    fprintf(fp, "\t%s\n", that->path);
    }

    if (file == NULL) {
	apclose(fp);
    } else {
	printf("Extract list written to file %s\n", file);
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
    int get_tape;

    get_tape = 0;
    while (ret < 0) {
	if (get_tape) {
	    prompt = "New tape device [?]: ";
	} else if (allow_tape && allow_skip) {
	    prompt = "Continue [?/Y/n/s/t]? ";
	} else if (allow_tape && !allow_skip) {
	    prompt = "Continue [?/Y/n/t]? ";
	} else if (allow_retry) {
	    prompt = "Continue [?/Y/n/r]? ";
	} else {
	    prompt = "Continue [?/Y/n]? ";
	}
	fputs(prompt, stdout);
	fflush(stdout); fflush(stderr);
	amfree(line);
	if ((line = agets(stdin)) == NULL) {
	    putchar('\n');
	    clearerr(stdin);
	    if (get_tape) {
		get_tape = 0;
		continue;
	    }
	    ret = 0;
	    break;
	}
	s = line;
	while ((ch = *s++) != '\0' && isspace(ch)) {
	    (void)ch;	/* Quiet empty loop compiler warning */
	}
	if (ch == '?') {
	    if (get_tape) {
		printf("Enter a new device ([host:]device) or \"default\"\n");
	    } else {
		printf("Enter \"y\"es to continue, \"n\"o to stop");
		if(allow_skip) {
		    printf(", \"s\"kip this tape");
		}
		if(allow_retry) {
		    printf(" or \"r\"etry this tape");
		}
		if (allow_tape) {
		    printf(" or \"t\"ape to change tape drives");
		}
		putchar('\n');
	    }
	} else if (get_tape) {
	    set_tape(s - 1);
	    get_tape = 0;
	} else if (ch == '\0' || ch == 'Y' || ch == 'y') {
	    ret = 1;
	} else if (allow_tape && (ch == 'T' || ch == 't')) {
	    get_tape = 1;
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

    if (security_stream_write(stream, msg, strlen(msg)) < 0)
    {
	error("Error writing to tape server");
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
	error("no '%s' security driver available for host '%s'",
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
		     amidxtaped_client_get_security_conf, req, STARTUP_TIMEOUT,
		     amidxtaped_response, &response_error);
    amfree(req);
    protocol_run();
    if(response_error != 0) {
	return -1;
    }

    disk_regex = alloc(strlen(disk_name) * 2 + 3);

    ch = disk_name;
    ch1 = disk_regex;

    /* we want to force amrestore to only match disk_name exactly */
    *(ch1++) = '^';

    /* We need to escape some characters first... NT compatibilty crap */
    for (; *ch != 0; ch++, ch1++) {
	switch (*ch) {     /* done this way in case there are more */
	case '$':
	    *(ch1++) = '\\';
	    /* no break; we do want to fall through... */
	default:
	    *ch1 = *ch;
	}
    }

    /* we want to force amrestore to only match disk_name exactly */
    *(ch1++) = '$';

    *ch1 = '\0';

    host_regex = alloc(strlen(dump_hostname) * 2 + 3);

    ch = dump_hostname;
    ch1 = host_regex;

    /* we want to force amrestore to only match dump_hostname exactly */
    *(ch1++) = '^';

    /* We need to escape some characters first... NT compatibilty crap */
    for (; *ch != 0; ch++, ch1++) {
	switch (*ch) {     /* done this way in case there are more */
	case '$':
	    *(ch1++) = '\\';
	    /* no break; we do want to fall through... */
	default:
	    *ch1 = *ch;
	}
    }

    /* we want to force amrestore to only match dump_hostname exactly */
    *(ch1++) = '$';

    *ch1 = '\0';

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
	if(strncmp(amidxtaped_line,"FEATURES=",9) == 0) {
	    tapesrv_features = am_string_to_feature(amidxtaped_line+9);
	} else {
	    fprintf(stderr, "amrecover - expecting FEATURES line from amidxtaped\n");
	    stop_amidxtaped();
	    return -1;
	}
	am_release_feature_set(tapesrv_features);
    }


    if(am_has_feature(indexsrv_features, fe_amidxtaped_header) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_device) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_host) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_disk) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_datestamp)) {

	if(am_has_feature(indexsrv_features, fe_amidxtaped_config)) {
	    tt = newstralloc2(tt, "CONFIG=", config);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_label) &&
	   label && label[0] != '/') {
	    tt = newstralloc2(tt,"LABEL=",label);
	    send_to_tape_server(amidxtaped_streams[CTLFD].fd, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_fsf)) {
	    char v_fsf[100];
	    snprintf(v_fsf, 99, OFF_T_FMT, (OFF_T_FMT_TYPE)fsf);
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

	dbprintf(("Started amidxtaped with arguments \"6 -h -p %s %s %s %s\"\n",
		  dump_device_name, host_regex, disk_regex, clean_datestamp));
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
	error("error reading header (%s), check amidxtaped.*.debug on server",
	      strerror(errno));
	/*NOTREACHED*/
    }

    if((size_t)bytes_read < buflen) {
	fprintf(stderr, "%s: short block %d byte%s\n",
		get_pname(), (int)bytes_read, (bytes_read == 1) ? "" : "s");
	print_header(stdout, file);
	error("Can't read file header");
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
	IS_SAMBA_TAR
};

static void
extract_files_child(
    int			in_fd,
    EXTRACT_LIST *	elist)
{
    int save_errno;
    int extra_params = 0;
    int i,j=0;
    char **restore_args = NULL;
    int files_off_tape;
    EXTRACT_LIST_ITEM *fn;
    enum dumptypes dumptype = IS_UNKNOWN;
    char buffer[DISK_BLOCK_BYTES];
    dumpfile_t file;
    size_t len_program;
    char *cmd = NULL;
    int passwd_field = -1;
#ifdef SAMBA_CLIENT
    char *domain = NULL, *smbpass = NULL;
#endif

    /* code executed by child to do extraction */
    /* never returns */

    /* make in_fd be our stdin */
    if (dup2(in_fd, STDIN_FILENO) == -1)
    {
	error("dup2 failed in extract_files_child: %s", strerror(errno));
	/*NOTREACHED*/
    }

    /* read the file header */
    fh_init(&file);
    read_file_header(buffer, &file, sizeof(buffer), STDIN_FILENO);

    if(file.type != F_DUMPFILE) {
	print_header(stdout, &file);
	error("bad header");
	/*NOTREACHED*/
    }

    if (file.program != NULL) {
#ifdef GNUTAR
	if (strcmp(file.program, GNUTAR) == 0)
	    dumptype = IS_GNUTAR;
#endif

	if (dumptype == IS_UNKNOWN) {
	    len_program = strlen(file.program);
	    if(len_program >= 3 &&
	       strcmp(&file.program[len_program-3],"tar") == 0)
		dumptype = IS_TAR;
	}

#ifdef SAMBA_CLIENT
	if (dumptype == IS_UNKNOWN && strcmp(file.program, SAMBA_CLIENT) ==0) {
	    if (samba_extract_method == SAMBA_TAR)
	      dumptype = IS_SAMBA_TAR;
	    else
	      dumptype = IS_SAMBA;
	}
#endif
    }

    /* form the arguments to restore */
    files_off_tape = length_of_tape_list(elist);
    switch (dumptype) {
    case IS_SAMBA:
#ifdef SAMBA_CLIENT
    	extra_params = 10;
    	break;
#endif
    case IS_TAR:
    case IS_GNUTAR:
        extra_params = 4;
        break;
    case IS_SAMBA_TAR:
        extra_params = 3;
        break;
    case IS_UNKNOWN:
    case IS_DUMP:
#ifdef AIX_BACKUP
        extra_params = 2;
#else
#if defined(XFSDUMP)
	if (strcmp(file.program, XFSDUMP) == 0) {
            extra_params = 4 + files_off_tape;
	} else
#endif
	{
        extra_params = 4;
	}
#endif
    	break;
    }

    restore_args = (char **)alloc((size_t)((extra_params + files_off_tape + 1)
				  * sizeof(char *)));
    switch(dumptype) {
    case IS_SAMBA:
#ifdef SAMBA_CLIENT
    	restore_args[j++] = stralloc("smbclient");
    	smbpass = findpass(file.disk, &domain);
    	if (smbpass) {
            restore_args[j++] = stralloc(file.disk);
	    passwd_field=j;
    	    restore_args[j++] = stralloc("-U");
    	    restore_args[j++] = smbpass;
    	    if (domain) {
            	restore_args[j++] = stralloc("-W");
    	    	restore_args[j++] = stralloc(domain);
   	    } else
		extra_params -= 2;
    	} else
	    extra_params -= 6;
    	restore_args[j++] = stralloc("-d0");
    	restore_args[j++] = stralloc("-Tx");
	restore_args[j++] = stralloc("-");	/* data on stdin */
    	break;
#endif
    case IS_TAR:
    case IS_GNUTAR:
    	restore_args[j++] = stralloc("tar");
	restore_args[j++] = stralloc("--numeric-owner");
	restore_args[j++] = stralloc("-xpGvf");
	restore_args[j++] = stralloc("-");	/* data on stdin */
	break;
    case IS_SAMBA_TAR:
    	restore_args[j++] = stralloc("tar");
	restore_args[j++] = stralloc("-xpvf");
	restore_args[j++] = stralloc("-");	/* data on stdin */
	break;
    case IS_UNKNOWN:
    case IS_DUMP:
        restore_args[j++] = stralloc("restore");
#ifdef AIX_BACKUP
        restore_args[j++] = stralloc("-xB");
#else
#if defined(XFSDUMP)
	if (strcmp(file.program, XFSDUMP) == 0) {
            restore_args[j++] = stralloc("-v");
            restore_args[j++] = stralloc("silent");
	} else
#endif
#if defined(VDUMP)
	if (strcmp(file.program, VDUMP) == 0) {
            restore_args[j++] = stralloc("xf");
            restore_args[j++] = stralloc("-");	/* data on stdin */
	} else
#endif
	{
        restore_args[j++] = stralloc("xbf");
        restore_args[j++] = stralloc("2");	/* read in units of 1K */
        restore_args[j++] = stralloc("-");	/* data on stdin */
	}
#endif
    }
  
    for (i = 0, fn = elist->files; i < files_off_tape; i++, fn = fn->next)
    {
	switch (dumptype) {
    	case IS_TAR:
    	case IS_GNUTAR:
    	case IS_SAMBA_TAR:
    	case IS_SAMBA:
	    if (strcmp(fn->path, "/") == 0)
	        restore_args[j++] = stralloc(".");
	    else
	        restore_args[j++] = stralloc2(".", fn->path);
	    break;
	case IS_UNKNOWN:
	case IS_DUMP:
#if defined(XFSDUMP)
	    if (strcmp(file.program, XFSDUMP) == 0) {
		/*
		 * xfsrestore needs a -s option before each file to be
		 * restored, and also wants them to be relative paths.
		 */
		restore_args[j++] = stralloc("-s");
		restore_args[j++] = stralloc(fn->path + 1);
	    } else
#endif
	    {
	    restore_args[j++] = stralloc(fn->path);
	    }
  	}
    }
#if defined(XFSDUMP)
    if (strcmp(file.program, XFSDUMP) == 0) {
	restore_args[j++] = stralloc("-");
	restore_args[j++] = stralloc(".");
    }
#endif
    restore_args[j] = NULL;

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
	fprintf(stderr, "warning: GNUTAR program not available.\n");
	cmd = stralloc("tar");
#else
  	cmd = stralloc(GNUTAR);
#endif
    	break;
    case IS_UNKNOWN:
    case IS_DUMP:
	cmd = NULL;
#if defined(DUMP)
	if (strcmp(file.program, DUMP) == 0) {
    	    cmd = stralloc(RESTORE);
	}
#endif
#if defined(VDUMP)
	if (strcmp(file.program, VDUMP) == 0) {
    	    cmd = stralloc(VRESTORE);
	}
#endif
#if defined(VXDUMP)
	if (strcmp(file.program, VXDUMP) == 0) {
    	    cmd = stralloc(VXRESTORE);
	}
#endif
#if defined(XFSDUMP)
	if (strcmp(file.program, XFSDUMP) == 0) {
    	    cmd = stralloc(XFSRESTORE);
	}
#endif
	if (cmd == NULL) {
	    fprintf(stderr, "warning: restore program for %s not available.\n",
		    file.program);
	    cmd = stralloc("restore");
	}
    }
    if (cmd) {
        dbprintf(("Exec'ing %s with arguments:\n", cmd));
	for (i = 0; i < j; i++) {
	    if( i == passwd_field)
		dbprintf(("\tXXXXX\n"));
	    else
  	        dbprintf(("\t%s\n", restore_args[i]));
	}
        (void)execv(cmd, restore_args);
	/* only get here if exec failed */
	save_errno = errno;
	for (i = 0; i < j; i++) {
  	    amfree(restore_args[i]);
  	}
  	amfree(restore_args);
	errno = save_errno;
        perror("amrecover couldn't exec");
        fprintf(stderr, " problem executing %s\n", cmd);
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
    int child_pipe[2];
    pid_t pid;
    amwait_t extractor_status;

    if(pipe(child_pipe) == -1) {
	error("extract_list - error setting up pipe to extractor: %s\n",
	      strerror(errno));
	/*NOTREACHED*/
    }

    /* okay, ready to extract. fork a child to do the actual work */
    if ((pid = fork()) == 0) {
	/* this is the child process */
	/* never gets out of this clause */
	aclose(child_pipe[1]);
        extract_files_child(child_pipe[0], elist);
	/*NOTREACHED*/
    }

    /* This is the parent */
    if (pid == -1) {
	printf("writer_intermediary - error forking child");
	return -1;
    }

    aclose(child_pipe[0]);

    security_stream_read(amidxtaped_streams[DATAFD].fd,
			 read_amidxtaped_data, &(child_pipe[1]));

    while(get_amidxtaped_line() >= 0) {
	char desired_tape[MAX_TAPE_LABEL_BUF];
                
	/* if prompted for a tape, relay said prompt to the user */
	if(sscanf(amidxtaped_line, "FEEDME %132s\n", desired_tape) == 1) {
	    int done;
	    printf("Load tape %s now\n", desired_tape);
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
	} else if(strncmp(amidxtaped_line, "MESSAGE ", 8) == 0) {
	    printf("%s\n",&amidxtaped_line[8]);
	} else {
	    fprintf(stderr, "Strange message from tape server: %s",
		    amidxtaped_line);
	    break;
	}
    }

    /* CTL might be close before DATA */
    event_loop(0);
    aclose(child_pipe[1]);

    waitpid(pid, &extractor_status, 0);
    if(WEXITSTATUS(extractor_status) != 0){
	int ret = WEXITSTATUS(extractor_status);
        if(ret == 255) ret = -1;
	printf("Extractor child exited with status %d\n", ret);
	return -1;
    }
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
    char cwd[STR_SIZE];
    char *l;
    int first;
    int otc;
    tapelist_t *tlist = NULL, *a_tlist;

    if (!is_extract_list_nonempty())
    {
	printf("Extract list empty - No files to extract!\n");
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
	    printf("%s\n", l);
	    exit(1);
	}
	/* skip reply number */
	tape_device_name = newstralloc(tape_device_name, l+4);
    }

    if (strcmp(tape_device_name, "/dev/null") == 0)
    {
	printf("amrecover: warning: using %s as the tape device will not work\n",
	       tape_device_name);
    }

    first=1;
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist)) {
	if(elist->tape[0]!='/') {
	    if(first) {
		printf("\nExtracting files using tape drive %s on host %s.\n",
			tape_device_name, tape_server_name);
		printf("The following tapes are needed:");
		first=0;
	    }
	    else
		printf("                               ");
	    tlist = unmarshal_tapelist_str(elist->tape); 
	    for(a_tlist = tlist ; a_tlist != NULL; a_tlist = a_tlist->next)
		printf(" %s", a_tlist->label);
	    printf("\n");
	    free_tapelist(tlist);
	}
    }
    first=1;
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist)) {
	if(elist->tape[0]=='/') {
	    if(first) {
		printf("\nExtracting files from holding disk on host %s.\n",
			tape_server_name);
		printf("The following files are needed:");
		first=0;
	    }
	    else
		printf("                               ");
	    tlist = unmarshal_tapelist_str(elist->tape); 
	    for(a_tlist = tlist; a_tlist != NULL; a_tlist = a_tlist->next)
		printf(" %s", a_tlist->label);
	    printf("\n");
	    free_tapelist(tlist);
	}
    }
    printf("\n");

    if (getcwd(cwd, sizeof(cwd)) == NULL) {
	perror("extract_list: Current working directory unavailable");
	exit(1);
    }

    printf("Restoring files into directory %s\n", cwd);
    check_file_overwrite(cwd);

#ifdef SAMBA_CLIENT
    if (samba_extract_method == SAMBA_SMBCLIENT)
      printf("(unless it is a Samba backup, that will go through to the SMB server)\n");
#endif
    if (!okay_to_continue(0,0,0))
	return;
    printf("\n");

    if (!do_unlink_list()) {
	fprintf(stderr, "Can't recover because I can't cleanup the cwd (%s)\n",
		cwd);
	return;
    }
    free_unlink_list();

    while ((elist = first_tape_list()) != NULL)
    {
	if(elist->tape[0]=='/') {
	    dump_device_name = newstralloc(dump_device_name, elist->tape);
	    printf("Extracting from file ");
	    tlist = unmarshal_tapelist_str(dump_device_name); 
	    for(a_tlist = tlist; a_tlist != NULL; a_tlist = a_tlist->next)
		printf(" %s", a_tlist->label);
	    printf("\n");
	    free_tapelist(tlist);
	}
	else {
	    printf("Extracting files using tape drive %s on host %s.\n",
		   tape_device_name, tape_server_name);
	    tlist = unmarshal_tapelist_str(elist->tape); 
	    printf("Load tape %s now\n", tlist->label);
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

	/* connect to the tape handler daemon on the tape drive server */
	if ((extract_files_setup(elist->tape, elist->fileno)) == -1)
	{
	    fprintf(stderr, "amrecover - can't talk to tape server\n");
	    return;
	}

	/* if the server have fe_amrecover_feedme_tape, it has asked for
	 * the tape itself, even if the restore didn't succeed, we should
	 * remove it.
	 */
	if(writer_intermediary(elist) == 0 ||
	   am_has_feature(indexsrv_features, fe_amrecover_feedme_tape))
	    delete_tape_list(elist);	/* tape done so delete from list */

	stop_amidxtaped();
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

    security_close_connection(sech, dump_hostname);
    if (pkt == NULL) {
	errstr = newvstralloc(errstr, "[request failed: ",
			     security_geterror(sech), "]", NULL);
	*response_error = 1;
	return;
    }

    if (pkt->type == P_NAK) {
#if defined(PACKET_DEBUG)
	fprintf(stderr, "got nak response:\n----\n%s\n----\n\n", pkt->body);
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
	errstr = newvstralloc(errstr, "received strange packet type ",
			      pkt_type2str(pkt->type), ": ", pkt->body, NULL);
	*response_error = 1;
	return;
    }

#if defined(PACKET_DEBUG)
    fprintf(stderr, "got response:\n----\n%s\n----\n\n", pkt->body);
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
		tok = "[bogus error packet]";
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
		    extra = vstralloc("CONNECT token is \"",
				      tok ? tok : "(null)",
				      "\": expected \"",
				      amidxtaped_streams[i].name,
				      "\"",
				      NULL);
		    goto parse_error;
		}
		tok = strtok(NULL, " \n");
		if (tok == NULL || sscanf(tok, "%d", &ports[i]) != 1) {
		    extra = vstralloc("CONNECT ",
				      amidxtaped_streams[i].name,
				      " token is \"",
				      tok ? tok : "(null)",
				      "\": expected a port number",
				      NULL);
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
		extra = stralloc("OPTIONS token is missing");
		goto parse_error;
	    }
/*
	    while((p = strchr(tok, ';')) != NULL) {
		*p++ = '\0';
#define sc "features="
		if(strncmp(tok, sc, sizeof(sc)-1) == 0) {
		    tok += sizeof(sc) - 1;
#undef sc
		    am_release_feature_set(their_features);
		    if((their_features = am_string_to_feature(tok)) == NULL) {
			errstr = newvstralloc(errstr,
					      "OPTIONS: bad features value: ",
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
	extra = vstralloc("next token is \"",
			  tok ? tok : "(null)",
			  "\": expected \"CONNECT\", \"ERROR\" or \"OPTIONS\"",
			  NULL);
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
	dbprintf(("amidxtaped_streams[%d].fd = %p\n",i, amidxtaped_streams[i].fd));
	if (amidxtaped_streams[i].fd == NULL) {
	    errstr = newvstralloc(errstr,
			"[could not connect ", amidxtaped_streams[i].name, " stream: ",
			security_geterror(sech), "]", NULL);
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
	    errstr = newvstralloc(errstr,
		"[could not authenticate ", amidxtaped_streams[i].name, " stream: ",
		security_stream_geterror(amidxtaped_streams[i].fd), "]", NULL);
	    goto connect_error;
	}
    }

    /*
     * The CTLFD and DATAFD streams are mandatory.  If we didn't get
     * them, complain.
     */
    if (amidxtaped_streams[CTLFD].fd == NULL) {
        errstr = newstralloc(errstr, "[couldn't open CTL streams]");
        goto connect_error;
    }
    if (amidxtaped_streams[DATAFD].fd == NULL) {
        errstr = newstralloc(errstr, "[couldn't open DATA streams]");
        goto connect_error;
    }

    /* everything worked */
    *response_error = 0;
    return;

parse_error:
    errstr = newvstralloc(errstr,
			  "[parse of reply message failed: ",
			  extra ? extra : "(no additional information)",
			  "]",
			  NULL);
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
    int fd;

    assert(cookie != NULL);

    fd = *(int *)cookie;
    if (size < 0) {
	errstr = newstralloc2(errstr, "amidxtaped read: ",
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

    /*
     * We ignore errors while writing to the index file.
     */
    (void)fullwrite(fd, buf, (size_t)size);
    security_stream_read(amidxtaped_streams[DATAFD].fd, read_amidxtaped_data, cookie);
}

char *
amidxtaped_client_get_security_conf(
    char *	string,
    void *	arg)
{
    (void)arg;	/* Quiet unused parameter warning */

    if(!string || !*string)
	return(NULL);

    if(strcmp(string, "auth")==0) {
	return(client_getconf_str(CLN_AUTH));
    }
    if(strcmp(string, "ssh_keys")==0) {
	return(client_getconf_str(CLN_SSH_KEYS));
    }
    return(NULL);
}
