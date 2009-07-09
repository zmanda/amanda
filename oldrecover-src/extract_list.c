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
#include "conffile.h"
#include "amrecover.h"
#include "fileheader.h"
#include "dgram.h"
#include "stream.h"
#include "tapelist.h"
#ifdef SAMBA_CLIENT
#include "findpass.h"
#endif
#include "util.h"

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

char *dump_device_name = NULL;

extern char *localhost;

/* global pid storage for interrupt handler */
pid_t extract_restore_child_pid = -1;

static EXTRACT_LIST *extract_list = NULL;
static int tape_control_sock = -1;
static int tape_data_sock = -1;

#ifdef SAMBA_CLIENT
unsigned short samba_extract_method = SAMBA_TAR;
#endif /* SAMBA_CLIENT */

#define READ_TIMEOUT	240*60

EXTRACT_LIST *first_tape_list(void);
EXTRACT_LIST *next_tape_list(EXTRACT_LIST *list);
int is_extract_list_nonempty(void);
int length_of_tape_list(EXTRACT_LIST *tape_list);
void add_file(char *path, char *regex);
void add_glob(char *glob);
void add_regex(char *regex);
void clear_extract_list(void);
void clean_tape_list(EXTRACT_LIST *tape_list);
void clean_extract_list(void);
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
void writer_intermediary(int ctl_fd, int data_fd, EXTRACT_LIST *elist);
void writer_intermediary(int ctl_fd, int data_fd, EXTRACT_LIST *elist);

static int add_extract_item(DIR_ITEM *ditem);
static int delete_extract_item(DIR_ITEM *ditem);
static int extract_files_setup(char *label, off_t fsf);
static int okay_to_continue(int allow_tape,
			int allow_skip,
			int allow_retry);
static int okay_to_continue(int, int,  int);
static ssize_t read_buffer(int datafd,
			char *buffer,
			size_t buflen,
			long timeout_s);
static void clear_tape_list(EXTRACT_LIST *tape_list);
static void extract_files_child(int in_fd, EXTRACT_LIST *elist);
static void send_to_tape_server(int tss, char *cmd);


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
    /*@keep@*/ EXTRACT_LIST *list)
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
    EXTRACT_LIST *	tape_list)
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
    EXTRACT_LIST *	tape_list)
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
	    /*@i@*/	      fn1->path, fn2->path);
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


/* returns -1 if error */
/* returns  0 on succes */
/* returns  1 if already added */
static int
add_extract_item(
    DIR_ITEM *	ditem)
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
		if (strcmp(curr->path, ditem_path) == 0) {
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
    DIR_ITEM *	ditem)
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
	g_printf(_("%s is not a valid regular expression: "), regex);
	puts(s);
    } else {
        add_file(uqregex, regex);
    }
    amfree(uqregex);
}

void add_file(
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

    dbprintf(_("add_file: Converted path=\"%s\" to path_on_disk=\"%s\"\n"),
	      regex, path_on_disk);

    found_one = 0;
    dir_entries = 0;
    for (ditem=get_dir_list(); ditem!=NULL; ditem=get_next_dir_item(ditem))
    {
	dir_entries++;
	quoted = quote_string(ditem->path);
	dbprintf(_("add_file: Pondering ditem->path=%s\n"), quoted);
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

		cmd = newstralloc2(cmd, "ORLD ", ditem_path);
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
		    g_printf(_("%s\n"), l);
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
                    skip_non_whitespace(s, ch);
                    s[-1] = '\0';
                    lditem.tape = newstralloc(lditem.tape, fp);
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
		    dir = s - 1;
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
			quoted = quote_string(lditem.path);
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
		    quoted = quote_string(ditem->path);
		    g_printf(_("Added file %s\n"), quoted);
		    dbprintf(_("add_file: (Successful) Added %s\n"), quoted);
		    amfree(quoted);
		    break;

		case  1:
		    quoted = quote_string(ditem->path);
		    g_printf(_("File %s already added\n"), quoted);
		    dbprintf(_("add_file: file %s already added\n"), quoted);
		    amfree(quoted);
		    break;
		}
	    }
	}
    }
    if (cmd != NULL)
	amfree(cmd);
    amfree(ditem_path);
    amfree(path_on_disk);
    amfree(path_on_disk_slash);

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
    char *uqglob = unquote_string(glob);

    regex = glob_to_regex(uqglob);
    dbprintf(_("delete_glob (%s) -> %s\n"), uqglob, regex);
    if ((s = validate_regexp(regex)) != NULL) {
	g_printf(_("\"%s\" is not a valid shell wildcard pattern: "), glob);
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
	g_printf(_("\"%s\" is not a valid regular expression: "), regex);
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
	g_printf(_("Must select directory before deleting files\n"));
	return;
    }
    memset(&lditem, 0, sizeof(lditem)); /* Prevent use of bogus data... */

    dbprintf(_("delete_file: Looking for \"%s\"\n"), path);

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

    dbprintf(_("delete_file: Converted path=\"%s\" to path_on_disk=\"%s\"\n"),
	      regex, path_on_disk);
    found_one = 0;
    for (ditem=get_dir_list(); ditem!=NULL; ditem=get_next_dir_item(ditem))
    {
	quoted = quote_string(ditem->path);
	dbprintf(_("delete_file: Pondering ditem->path=%s\n"), quoted);
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

		cmd = newstralloc2(cmd, "ORLD ", ditem_path);
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
		    g_printf("%s\n", l);
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
			fileno = (off_t)fileno_;
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
                    lditem.tape = newstralloc(lditem.tape, tape);
		    switch(delete_extract_item(&lditem)) {
		    case -1:
			g_printf(_("System error\n"));
			dbprintf(_("delete_file: (Failed) System error\n"));
			break;
		    case  0:
			g_printf(_("Deleted dir %s at date %s\n"), ditem_path, date);
			dbprintf(_("delete_file: (Successful) Deleted dir %s at date %s\n"),
				  ditem_path, date);
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
			   ditem_path);
		    dbprintf(_("delete_file: dir '%s' not on tape list\n"),
			      ditem_path);
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
		    g_printf(_("Deleted %s\n"), ditem->path);
		    dbprintf(_("delete_file: (Successful) Deleted %s\n"),
			      ditem->path);
		    break;
		case  1:
		    g_printf(_("Warning - file '%s' not on tape list\n"),
			   ditem->path);
		    dbprintf(_("delete_file: file '%s' not on tape list\n"),
			      ditem->path);
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
	g_printf(_("File %s doesn't exist in directory\n"), path);
	dbprintf(_("delete_file: (Failed) File %s doesn't exist in directory\n"),
	          path);
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
	    g_fprintf(fp, "\t%s\n", that->path);
    }

    if (file == NULL) {
	apclose(fp);
    } else {
	g_printf(_("Extract list written to file %s\n"), file);
	afclose(fp);
    }
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
    int		allow_tape,
    int		allow_skip,
    int		allow_retry)
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
	    prompt = _("New tape device [?]: ");
	} else if (allow_tape && allow_skip) {
	    prompt = _("Continue [?/Y/n/s/t]? ");
	} else if (allow_tape && !allow_skip) {
	    prompt = _("Continue [?/Y/n/t]? ");
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
	    if (get_tape) {
		get_tape = 0;
		continue;
	    }
	    ret = 0;
	    break;
	}
	s = line;
	while ((ch = *s++) != '\0' && g_ascii_isspace(ch)) {
	    (void)ch;  /* Quiet empty loop body warning */
	}
	if (ch == '?') {
	    if (get_tape) {
		g_printf(_("Enter a new device ([host:]device) or \"default\"\n"));
	    } else {
		g_printf(_("Enter \"y\"es to continue, \"n\"o to stop"));
		if(allow_skip) {
		    g_printf(_(", \"s\"kip this tape"));
		}
		if(allow_retry) {
		    g_printf(_(" or \"r\"etry this tape"));
		}
		if (allow_tape) {
		    g_printf(_(" or \"t\"ape to change tape drives"));
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
    int		tss,
    char *	cmd)
{
    char *msg = stralloc2(cmd, "\r\n");

    if (full_write(tss, msg, strlen(msg)) < strlen(msg))
    {
	error(_("Error writing to tape server: %s"), strerror(errno));
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
    struct servent *sp;
    in_port_t my_port, my_data_port;
    char *disk_regex = NULL;
    char *host_regex = NULL;
    char *service_name = NULL;
    char *line = NULL;
    char *clean_datestamp, *ch, *ch1;
    char *our_feature_string = NULL;
    char *tt = NULL;

    service_name = stralloc2("amidxtape", SERVICE_SUFFIX);

    /* get tape server details */
    if ((sp = getservbyname(service_name, "tcp")) == NULL)
    {
	g_printf(_("%s/tcp unknown protocol - config error?\n"), service_name);
	amfree(service_name);
	return -1;
    }
    amfree(service_name);
    seteuid(0);					/* it either works ... */
    setegid(0);
    tape_control_sock = stream_client_privileged(tape_server_name,
						  (in_port_t)ntohs((in_port_t)sp->s_port),
						  0,
						  STREAM_BUFSIZE,
						  &my_port,
						  0);
    if (tape_control_sock < 0)
    {
	g_printf(_("cannot connect to %s: %s\n"), tape_server_name, strerror(errno));
	return -1;
    }
    if (my_port >= IPPORT_RESERVED) {
	aclose(tape_control_sock);
	g_printf(_("did not get a reserved port: %u\n"), (unsigned)my_port);
	return -1;
    }
 
    setegid(getgid());
    seteuid(getuid());				/* put it back */

    /* do the security thing */
    line = get_security();
    send_to_tape_server(tape_control_sock, line);
    memset(line, '\0', strlen(line));
    amfree(line);

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
	char buffer[32768] = "\0";

	our_feature_string = am_feature_to_string(our_features);
	tt = newstralloc2(tt, "FEATURES=", our_feature_string);
	send_to_tape_server(tape_control_sock, tt);
	if (read(tape_control_sock, buffer, sizeof(buffer)) <= 0) {
	    error(_("Could not read features from control socket\n"));
	    /*NOTREACHED*/
	}
	tapesrv_features = am_string_to_feature(buffer);
	amfree(our_feature_string);
    }


    if(am_has_feature(indexsrv_features, fe_amidxtaped_header) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_device) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_host) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_disk) &&
       am_has_feature(indexsrv_features, fe_amidxtaped_datestamp)) {

	if(am_has_feature(indexsrv_features, fe_amidxtaped_config)) {
	    tt = newstralloc2(tt, "CONFIG=", config);
	    send_to_tape_server(tape_control_sock, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_label) &&
	   label && label[0] != '/') {
	    tt = newstralloc2(tt,"LABEL=",label);
	    send_to_tape_server(tape_control_sock, tt);
	}
	if(am_has_feature(indexsrv_features, fe_amidxtaped_fsf)) {
	    char v_fsf[100];
	    g_snprintf(v_fsf, 99, "%lld", (long long)fsf);
	    tt = newstralloc2(tt, "FSF=",v_fsf);
	    send_to_tape_server(tape_control_sock, tt);
	}
	send_to_tape_server(tape_control_sock, "HEADER");
	tt = newstralloc2(tt, "DEVICE=", dump_device_name);
	send_to_tape_server(tape_control_sock, tt);
	tt = newstralloc2(tt, "HOST=", host_regex);
	send_to_tape_server(tape_control_sock, tt);
	tt = newstralloc2(tt, "DISK=", disk_regex);
	send_to_tape_server(tape_control_sock, tt);
	tt = newstralloc2(tt, "DATESTAMP=", clean_datestamp);
	send_to_tape_server(tape_control_sock, tt);
	send_to_tape_server(tape_control_sock, "END");
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
	send_to_tape_server(tape_control_sock, "6");
	send_to_tape_server(tape_control_sock, "-h");
	send_to_tape_server(tape_control_sock, "-p");
	send_to_tape_server(tape_control_sock, dump_device_name);
	send_to_tape_server(tape_control_sock, host_regex);
	send_to_tape_server(tape_control_sock, disk_regex);
	send_to_tape_server(tape_control_sock, clean_datestamp);

	dbprintf(_("Started amidxtaped with arguments \"6 -h -p %s %s %s %s\"\n"),
		  dump_device_name, host_regex, disk_regex, clean_datestamp);
    }

    /*
     * split-restoring amidxtaped versions will expect to set up a data
     * connection for dumpfile data, distinct from the socket we're already
     * using for control data
     */

    if(am_has_feature(tapesrv_features, fe_recover_splits)){
	char buffer[32768];
	in_port_t data_port = (in_port_t)-1;
        ssize_t nread;

        nread = read(tape_control_sock, buffer, sizeof(buffer));

	if (nread <= 0) {
	    error(_("Could not read from control socket: %s\n"),
                  strerror(errno));
	    /*NOTREACHED*/
        }

	buffer[nread] = '\0';
        if (sscanf(buffer, "CONNECT %hu\n",
		(unsigned short *)&data_port) != 1) {
	    error(_("Recieved invalid port number message from control socket: %s\n"),
                  buffer);
	    /*NOTREACHED*/
        }	

	tape_data_sock = stream_client_privileged(server_name,
						  data_port,
						  0,
						  STREAM_BUFSIZE,
						  &my_data_port,
						  0);
	if(tape_data_sock == -1){
	    error(_("Unable to make data connection to server: %s\n"),
		      strerror(errno));
	    /*NOTREACHED*/
	}

	amfree(our_feature_string);

	line = get_security();

	send_to_tape_server(tape_data_sock, line);
	memset(line, '\0', strlen(line));
	amfree(line);
    }

    amfree(disk_regex);
    amfree(host_regex);
    amfree(clean_datestamp);

    return tape_control_sock;
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
	IS_SAMBA_TAR
};

static void
extract_files_child(
    int			in_fd,
    EXTRACT_LIST *	elist)
{
    int save_errno;
    int   i;
    guint j;
    GPtrArray *argv_ptr = g_ptr_array_new();
    int files_off_tape;
    EXTRACT_LIST_ITEM *fn;
    enum dumptypes dumptype = IS_UNKNOWN;
    char buffer[DISK_BLOCK_BYTES];
    dumpfile_t file;
    size_t len_program;
    char *cmd = NULL;
    guint passwd_field = 999999999;
#ifdef SAMBA_CLIENT
    char *domain = NULL, *smbpass = NULL;
#endif

    /* code executed by child to do extraction */
    /* never returns */

    /* make in_fd be our stdin */
    if (dup2(in_fd, STDIN_FILENO) == -1)
    {
	error(_("dup2 failed in extract_files_child: %s"), strerror(errno));
	/*NOTREACHED*/
    }

    /* read the file header */
    fh_init(&file);
    read_file_header(buffer, &file, sizeof(buffer), STDIN_FILENO);

    if(file.type != F_DUMPFILE) {
	print_header(stdout, &file);
	error(_("bad header"));
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
    switch(dumptype) {
    case IS_SAMBA:
#ifdef SAMBA_CLIENT
	g_ptr_array_add(argv_ptr, stralloc("smbclient"));
	smbpass = findpass(file.disk, &domain);
	if (smbpass) {
	    g_ptr_array_add(argv_ptr, stralloc(file.disk));
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
	if (strcmp(file.program, XFSDUMP) == 0) {
	    g_ptr_array_add(argv_ptr, stralloc("-v"));
	    g_ptr_array_add(argv_ptr, stralloc("silent"));
	} else
#endif
#if defined(VDUMP)
	if (strcmp(file.program, VDUMP) == 0) {
	    g_ptr_array_add(argv_ptr, stralloc("xf"));
	    g_ptr_array_add(argv_ptr, stralloc("-"));	/* data on stdin */
	} else
#endif
	{
	g_ptr_array_add(argv_ptr, stralloc("xbf"));
	g_ptr_array_add(argv_ptr, stralloc("2")); /* read in units of 1K */
	g_ptr_array_add(argv_ptr, stralloc("-")); /* data on stdin */
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
		g_ptr_array_add(argv_ptr, stralloc("."));
	    else
		g_ptr_array_add(argv_ptr, stralloc2(".", fn->path));
	    break;
	case IS_UNKNOWN:
	case IS_DUMP:
#if defined(XFSDUMP)
	    if (strcmp(file.program, XFSDUMP) == 0) {
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
	}
    }
#if defined(XFSDUMP)
    if (strcmp(file.program, XFSDUMP) == 0) {
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
	    g_fprintf(stderr, _("warning: restore program for %s not available.\n"),
		    file.program);
	    cmd = stralloc("restore");
	}
    }
    if (cmd) {
        dbprintf(_("Exec'ing %s with arguments:\n"), cmd);
	for (j = 0; j < argv_ptr->len - 1; j++) {
	    if( j == passwd_field)
		dbprintf("\tXXXXX\n");
	    else
		dbprintf("\t%s\n", (char *)g_ptr_array_index(argv_ptr, j));
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
void
writer_intermediary(
    int			ctl_fd,
    int			data_fd,
    EXTRACT_LIST *	elist)
{
    int child_pipe[2];
    pid_t pid;
    char buffer[DISK_BLOCK_BYTES];
    size_t bytes_read;
    amwait_t extractor_status;
    int max_fd, nfound;
    SELECT_ARG_TYPE readset, selectset;
    struct timeval timeout;

    /*
     * If there's no distinct data channel (such as if we're talking to an
     * older server), don't bother doing anything complicated.  Just run the
     * extraction.
     */
    if(data_fd == -1){
	extract_files_child(ctl_fd, elist);
	/*NOTREACHED*/
    }

    if(pipe(child_pipe) == -1) {
	error(_("extract_list - error setting up pipe to extractor: %s\n"),
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
	error(_("writer_intermediary - error forking child"));
	/*NOTREACHED*/
    }

    aclose(child_pipe[0]);

    if(data_fd > ctl_fd) max_fd = data_fd+1;
                    else max_fd = ctl_fd+1;
    FD_ZERO(&readset);
    FD_SET(data_fd, &readset);
    FD_SET(ctl_fd, &readset);

    do {
	timeout.tv_sec = READ_TIMEOUT;
	timeout.tv_usec = 0;
	FD_COPY(&readset, &selectset);

	nfound = select(max_fd, &selectset, NULL, NULL,
			&timeout);
	if(nfound < 0) {
	    g_fprintf(stderr,_("select error: %s\n"), strerror(errno));
	    break;
	}

	if (nfound == 0) { /* timeout */
	    g_fprintf(stderr, _("timeout waiting %d seconds for restore\n"),
		    READ_TIMEOUT);
	    g_fprintf(stderr, _("increase READ_TIMEOUT in recover-src/extract_list.c if your tape is slow\n"));
	    break;
	}

	if(FD_ISSET(ctl_fd, &selectset)) {
	    bytes_read = read(ctl_fd, buffer, sizeof(buffer)-1);
	    switch(bytes_read) {
            case -1:
                if ((errno != EINTR) && (errno != EAGAIN)) {
                    if (errno != EPIPE) {
                        g_fprintf(stderr,_("writer ctl fd read error: %s"),
                                strerror(errno));
                    }
                    FD_CLR(ctl_fd, &readset);
                }
                break;

            case  0:
                FD_CLR(ctl_fd, &readset);
                break;

            default: {
                char desired_tape[MAX_TAPE_LABEL_BUF];

                buffer[bytes_read] = '\0';
                /* if prompted for a tape, relay said prompt to the user */
                if(sscanf(buffer, "FEEDME %132s\n", desired_tape) == 1) {
                    int done = 0;
                    while (!done) {
                        char *input = NULL;
                        g_printf(_("Please insert tape %s. Continue? [Y|n]: "),
                               desired_tape);
                        fflush(stdout);

                        input = agets(stdin); /* strips \n */
                        if (strcasecmp("", input) == 0||
                            strcasecmp("y", input) == 0||
                            strcasecmp("yes", input) == 0) {
                            send_to_tape_server(tape_control_sock, "OK");
                            done = 1;
                        } else if (strcasecmp("n", input) == 0||
                                   strcasecmp("no", input) == 0) {
                            send_to_tape_server(tape_control_sock, "ERROR");
                            /* Abort!
                               We are the middle process, so just die. */
                            exit(EXIT_FAILURE);
                        }
                        amfree(input);
                    }
                } else {
                    g_fprintf(stderr, _("Strange message from tape server: %s"), buffer);
		    break;
                }
	      }
            }
        }

        /* now read some dump data */
        if(FD_ISSET(data_fd, &selectset)) {
            bytes_read = read(data_fd, buffer, sizeof(buffer)-1);
            switch(bytes_read) {
            case -1:
                if ((errno != EINTR) && (errno != EAGAIN)) {
                    if (errno != EPIPE) {
                        g_fprintf(stderr,_("writer data fd read error: %s"),
                                strerror(errno));
                    }
                    FD_CLR(data_fd, &readset);
                }
                break;

            case  0:
                FD_CLR(data_fd, &readset);
                break;

            default:
                /*
                 * spit what we got from the server to the child
                 *  process handling actual dumpfile extraction
                 */
                if(full_write(child_pipe[1], buffer, bytes_read) < bytes_read) {
                    if(errno == EPIPE) {
                        error(_("pipe data reader has quit: %s\n"),
                              strerror(errno));
                        /* NOTREACHED */
                    }
                    error(_("Write error to extract child: %s\n"),
                          strerror(errno));
                    /* NOTREACHED */
                }
                break;
	    }
	}
    } while(FD_ISSET(ctl_fd, &readset) || FD_ISSET(data_fd, &readset));

    aclose(child_pipe[1]);

    waitpid(pid, &extractor_status, 0);
    if(WEXITSTATUS(extractor_status) != 0){
	int ret = WEXITSTATUS(extractor_status);
        if(ret == 255) ret = -1;
	error(_("Extractor child exited with status %d\n"), ret);
	/*NOTREACHED*/
    }

    exit(0);
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
    pid_t pid;
    amwait_t child_stat;
    char buf[STR_SIZE];
    char *l;
    int first;
    int otc;
    tapelist_t *tlist = NULL;

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
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist))
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
	    for( ; tlist != NULL; tlist = tlist->next)
		g_printf(" %s", tlist->label);
	    g_printf("\n");
	    amfree(tlist);
	}
    first=1;
    for (elist = first_tape_list(); elist != NULL; elist = next_tape_list(elist))
    {
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
	    for( ; tlist != NULL; tlist = tlist->next)
		g_printf(" %s", tlist->label);
	    g_printf("\n");
	    amfree(tlist);
	}
    }
    g_printf("\n");

    if (getcwd(buf, sizeof(buf)) == NULL) {
	perror(_("extract_list: Cannot determine current working directory"));
	exit(1);
    }

    g_printf(_("Restoring files into directory %s\n"), buf);
#ifdef SAMBA_CLIENT
    if (samba_extract_method == SAMBA_SMBCLIENT)
      g_printf(_("(unless it is a Samba backup, that will go through to the SMB server)\n"));
#endif
    if (!okay_to_continue(0,0,0))
	return;
    g_printf("\n");

    while ((elist = first_tape_list()) != NULL)
    {
	if(elist->tape[0]=='/') {
	    dump_device_name = newstralloc(dump_device_name, elist->tape);
	    g_printf(_("Extracting from file "));
	    tlist = unmarshal_tapelist_str(dump_device_name);
	    for( ; tlist != NULL; tlist = tlist->next)
		g_printf(" %s", tlist->label);
	    g_printf("\n");
	    amfree(tlist);
	}
	else {
	    g_printf(_("Extracting files using tape drive %s on host %s.\n"),
		   tape_device_name, tape_server_name);
	    tlist = unmarshal_tapelist_str(elist->tape);
	    g_printf(_("Load tape %s now\n"), tlist->label);
	    amfree(tlist);
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
	if ((tape_control_sock = extract_files_setup(elist->tape, elist->fileno)) == -1)
	{
	    g_fprintf(stderr, _("amrecover - can't talk to tape server\n"));
	    return;
	}

	/* okay, ready to extract. fork a child to do the actual work */
	if ((pid = fork()) == 0)
	{
	    /* this is the child process */
	    /* never gets out of this clause */
	    writer_intermediary(tape_control_sock, tape_data_sock, elist);
	    /*NOT REACHED*/
	}
	/* this is the parent */
	if (pid == -1)
	{
	    perror(_("extract_list - error forking child"));
	    aclose(tape_control_sock);
	    exit(1);
	}

	/* store the child pid globally so that it can be killed on intr */
	extract_restore_child_pid = pid;

	/* wait for the child process to finish */
	if ((pid = waitpid(-1, &child_stat, 0)) == (pid_t)-1)
	{
	    perror(_("extract_list - error waiting for child"));
	    exit(1);
	}

	if(tape_data_sock != -1) {
	    aclose(tape_data_sock);
	}

	if (pid == extract_restore_child_pid)
	{
	    extract_restore_child_pid = -1;
	}
	else
	{
	    g_fprintf(stderr, _("extract list - unknown child terminated?\n"));
	    exit(1);
	}
	if ((WIFEXITED(child_stat) != 0) && (WEXITSTATUS(child_stat) != 0))
	{
	    g_fprintf(stderr,
		    _("extract_list - child returned non-zero status: %d\n"),
		    WEXITSTATUS(child_stat));
	    otc = okay_to_continue(0,0,1);
	    if(otc == 0)
		return;

	    if(otc == 1) {
		delete_tape_list(elist); /* tape failed so delete from list */
	    }
	}
	else {
	    delete_tape_list(elist);	/* tape done so delete from list */
	}
    }
}
