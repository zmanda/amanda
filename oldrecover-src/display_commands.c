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
 * $Id: display_commands.c,v 1.3 2006/07/05 19:42:17 martinea Exp $
 *
 * implements the directory-display related commands in amrecover
 */

#include "amanda.h"
#include "amrecover.h"
#include "util.h"

DIR_ITEM *get_dir_list(void);
DIR_ITEM *get_next_dir_item(DIR_ITEM *this);

void clear_dir_list(void);
void free_dir_item(DIR_ITEM *item);
static int add_dir_list_item(char *date,
				int level,
				char *tape,
				off_t fileno,
				char *path);
void list_disk_history(void);
void suck_dir_list_from_server(void);
void list_directory(void);

static DIR_ITEM *dir_list = NULL;

DIR_ITEM *
get_dir_list(void)
{
    return dir_list;
}

DIR_ITEM *
get_next_dir_item(
    /*@keep@*/ DIR_ITEM *	this)
{
    return this->next;
}


void
clear_dir_list(void)
{
    free_dir_item(dir_list); /* Frees all items from dir_list to end of list */
    dir_list = NULL;
}

void
free_dir_item(
    DIR_ITEM *	item)
{
    DIR_ITEM *next;

    while (item != NULL) {
	next = item->next;
        amfree(item->date);
        amfree(item->tape);
        amfree(item->path);
        amfree(item);
	item = next;
    }
}

/* add item to list if path not already on list */
static int
add_dir_list_item(
    char *	date,
    int		level,
    char *	tape,
    off_t	fileno,
    char *	path)
{
    DIR_ITEM *next;

    dbprintf(_("add_dir_list_item: Adding \"%s\" \"%d\" \"%s\" \"%lld\" \"%s\"\n"),
	      date, level, tape, (long long)fileno, path);

    next = (DIR_ITEM *)alloc(sizeof(DIR_ITEM));
    memset(next, 0, sizeof(DIR_ITEM));

    next->date = stralloc(date);
    next->level = level;
    next->tape = stralloc(tape);
    next->fileno = fileno;
    next->path = stralloc(path);

    next->next = dir_list;
    dir_list = next;

    return 0;
}


void
list_disk_history(void)
{
    if (converse("DHST") == -1)
	exit(1);
}


void
suck_dir_list_from_server(void)
{
    char *cmd = NULL;
    char *err = NULL;
    int i;
    char *l = NULL;
    char *date;
    int level = 0;
    off_t fileno = (off_t)-1;
    char *tape, *tape_undo, tape_undo_ch = '\0';
    char *dir, *qdir;
    char *disk_path_slash = NULL;
    char *disk_path_slash_dot = NULL;
    char *s;
    int ch;

    if (disk_path == NULL) {
	g_printf(_("Directory must be set before getting listing\n"));
	return;
    } else if(strcmp(disk_path, "/") == 0) {
	disk_path_slash = stralloc(disk_path);
    } else {
	disk_path_slash = stralloc2(disk_path, "/");
    }

    clear_dir_list();

    cmd = stralloc2("OLSD ", disk_path);
    if (send_command(cmd) == -1) {
	amfree(cmd);
	amfree(disk_path_slash);
	exit(1);
    }
    amfree(cmd);
    /* skip preamble */
    if ((i = get_reply_line()) == -1) {
	amfree(disk_path_slash);
	exit(1);
    }
    if (i == 0)				/* assume something wrong! */
    {
	amfree(disk_path_slash);
	l = reply_line();
	g_printf("%s\n", l);
	return;
    }
    disk_path_slash_dot = stralloc2(disk_path_slash, ".");
    amfree(cmd);
    amfree(err);
    tape_undo = NULL;
    /* skip the last line -- duplicate of the preamble */
    while ((i = get_reply_line()) != 0)
    {
	if (i == -1) {
	    amfree(disk_path_slash_dot);
	    amfree(disk_path_slash);
	    exit(1);
	}
	if(err) {
	    if(cmd == NULL) {
		if(tape_undo) *tape_undo = tape_undo_ch;
		tape_undo = NULL;
		cmd = stralloc(l);	/* save for the error report */
	    }
	    continue;			/* throw the rest of the lines away */
	}
	l = reply_line();
	if (!server_happy())
	{
	    g_printf("%s\n", l);
	    continue;
	}
	s = l;
	if (strncmp_const_skip(s, "201-", s, ch) != 0) {
	    err = "bad reply: not 201-";
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

	if(am_has_feature(indexsrv_features, fe_amindexd_fileno_in_OLSD)) {
	    long long fileno_ = (long long)0;
	    skip_whitespace(s, ch);
	    if(ch == '\0' || sscanf(s - 1, "%lld", &fileno_) != 1) {
		err = _("bad reply: cannot parse fileno field");
		continue;
	    }
	    fileno = (off_t)fileno_;
	    skip_integer(s, ch);
	}
	else {
	    fileno = (off_t)-1;
	}

	skip_whitespace(s, ch);
	if(ch == '\0') {
	    err = _("bad reply: missing directory field");
	    continue;
	}
	qdir = s - 1;
	dir = unquote_string(qdir);

	/* add a '.' if it a the entry for the current directory */
	if((strcmp(disk_path,dir)==0) || (strcmp(disk_path_slash,dir)==0)) {
	    amfree(dir);
	    dir = stralloc(disk_path_slash_dot);
	}
	add_dir_list_item(date, level, tape, fileno, dir);
	amfree(dir);
    }
    amfree(disk_path_slash_dot);
    amfree(disk_path_slash);
    if(!server_happy()) {
	puts(reply_line());
    } else if(err) {
	if(*err) {
	    puts(err);
	}
	if (cmd)
	   puts(cmd);
	clear_dir_list();
    }
    amfree(cmd);
}


void
list_directory(void)
{
    size_t i;
    DIR_ITEM *item;
    FILE *fp;
    char *pager;
    char *pager_command;
    char *quoted;

    if (disk_path == NULL) {
	g_printf(_("Must select a disk before listing files; use the setdisk command.\n"));
	return;
    }

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
    i = strlen(disk_path);
    if (i != 1)
	i++;				/* so disk_path != "/" */
    for (item = get_dir_list(); item != NULL; item=get_next_dir_item(item)) {
	quoted = quote_string(item->path + i);
	g_fprintf(fp, "%s %s\n", item->date, quoted);
	amfree(quoted);
    }
    apclose(fp);
}
