/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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
 * $Id: display_commands.c,v 1.22 2006/07/05 19:42:17 martinea Exp $
 *
 * implements the directory-display related commands in amrecover
 */

#include "amanda.h"
#include "amrecover.h"
#include "amutil.h"
#include "match.h"

gboolean translate_mode = TRUE;

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
char *convert_name(char *name);

static DIR_ITEM *dir_list = NULL;

DIR_ITEM *
get_dir_list(void)
{
    return dir_list;
}

DIR_ITEM *
get_next_dir_item(
    DIR_ITEM *	this)
{
    /*@ignore@*/
    return this->next;
    /*@end@*/
}


void list_file_history(char* name)
{
    size_t i;
    DIR_ITEM *item;
    char *quoted;
    char *l;
    char *s;
    char *date;
    int ch;
    int ind = 0;
    int size = 1;
    char *next_date;
    char *cmd;
    char **list_date;
    list_date = g_malloc0(sizeof(char *) * size);

    if (disk_path == NULL) {
        g_printf(_("Must select a disk before listing file history; use the setdisk command.\n"));
        return;
    }

    send_command("DHST");
    get_reply_line();

    while (get_reply_line() != 0)
    {
        s = reply_line();
        l = s;
        if (strncmp_const_skip(l, "201-", s, ch) != 0) {
            g_printf("bad reply: not 201-");
            continue;
        }
        ch = *s++;
        skip_whitespace(s, ch);
        if(ch == '\0') {
            g_printf("bad reply: missing date field");
            continue;
        }
        date = s - 1;
        skip_non_whitespace(s, ch);
        *(s - 1) = '\0';
        list_date[ind] = g_strdup(date);
        ind++;
        if (ind == size) {
            size *= 2;
            list_date = g_realloc(list_date, sizeof(char *) * size);
            for (int j = size/2; j < size; j++) {
                list_date[j] = NULL;
            }
        }
    }
    g_printf("History of file: %s\n", name);
    i = strlen(disk_tpath);
    if (i != 1)
        i++;
    ind = 0;
    next_date = g_strdup(list_date[ind]);
    while ((ind < size) && (list_date[ind] != NULL)) {
        if (!(strcmp(list_date[ind], next_date) > 0))
        {

            clear_dir_list();
            cmd = g_strconcat("DATE ", list_date[ind], NULL);
            if (exchange(cmd) == -1)
                exit(1);
            if (server_happy())
            {
                suck_dir_list_from_server();
            } else {
                continue;
            }
            for (item = get_dir_list(); item != NULL; item = get_next_dir_item(item)) {
                quoted = quote_string(item->tpath + i);
                if (strcmp(quoted, name) == 0) {
                    if (strcmp(item->date, list_date[ind]) == 0) {
                        g_printf("%s %s\n", item->date, quoted);
                        amfree(quoted);
                        break;
                    } else {
                        amfree(next_date);
                        next_date = g_strdup(item->date);
                    }
                }
                amfree(quoted);
            }
        }
        amfree(list_date[ind]);
        ind++;
    }
    amfree(list_date);
    cmd = g_strconcat("DATE ", listing_date, NULL);
    if (exchange(cmd) == -1)
        exit(1);
    if (server_happy())
    {
        suck_dir_list_from_server();
    }
    amfree(cmd);
}


void
list_all_file(char *dir, char *name, int re)
{
    DIR_ITEM *item;
    char *quote;
    int size = 5;
    int nb_folder = 0;
    char **dir_list;
    char *next_dir;
    int i;
    int len;
    int dir_len;
    int printed = 0;

    if (disk_path == NULL) {
        g_printf(_("Must select a disk before listing files; use the setdisk command.\n"));
        return;
    }

    dir_list = g_malloc0(sizeof(char *) * size);

    if (set_directory(dir, 0) == 1) {
        dir_len = strlen(disk_tpath);
        if (dir_len == 1)
            dir_len = 0;
        for (item = get_dir_list(); item != NULL; item = get_next_dir_item(item)) {
            quote = quote_string(item->tpath + 1);
            i = 0;
            if (quote[strlen(quote) - 1] == '\"') {
                quote++;
                quote[strlen(quote) - 1] = '\0';
                i = 1;
            }
            len = strlen(quote);
            if (quote[len - 1] == '/') {
                if (dir[strlen(dir) - 1] == '/') {
                    next_dir = g_strconcat(dir, &quote[dir_len], NULL);
                } else {
                    next_dir = g_strconcat(dir, "/", &quote[dir_len], NULL);
                }
                dir_list[nb_folder] = next_dir;
                nb_folder++;
                if (nb_folder == size) {
                    size *= 2;
                    dir_list = g_realloc(dir_list, sizeof(char *) * size);
                    for (int j = size/2; j < size; j++) {
                        dir_list[j] = NULL;
                    }
                }
            } else {
                if ((re && (match(name, &quote[dir_len]) == 1)) || (!re && (strcmp(name, &quote[dir_len]) == 0))) {
                    if (!printed) {
                        printed = 1;
                        g_printf("\nFolder: %s\n", dir);
                    }
                    g_printf("%s: %s\n", item->date, quote);
                }
            }
            quote -= i;
            amfree(quote);
        }
        i = 0;
        while ((i < size) && (dir_list[i] != NULL)) {
            list_all_file(dir_list[i], name, re);
            amfree(dir_list[i]);
            i++;
        }
        set_directory(dir, 0);
    }
    amfree(dir_list);
}


void
find_file(char *name, char *dir, int re)
{
    char *error = NULL;
    char *regex = NULL;

    if (disk_path == NULL) {
        g_printf(_("Must select a disk before search for a file; use the setdisk command.\n"));
        return;
    }

    if ((strlen(dir) != 1) && (dir[0] == '\"') && (dir[strlen(dir) - 1] == '\"')) {
        dir = &dir[1];
        dir[strlen(dir) - 1] = '\0';
    }
    if (!g_str_has_prefix(dir, mount_point)) {
        if (dir[0] != '/') {
            if (disk_tpath[strlen(disk_tpath) - 1] == '/') {
                dir = g_strconcat(mount_point, disk_tpath, dir, NULL);
            } else {
                dir = g_strconcat(mount_point, disk_tpath, "/", dir, NULL);
            }
        } else {
            dir = g_strconcat(mount_point, dir, NULL);
        }
    }

    name = convert_name(name);
    if (re == 1) {
        error = validate_regexp(name);
        if (error == NULL) {
            regex = g_strconcat("^", name, "$", NULL);

            g_printf("Search regex : %s in %s\n", regex, dir);

            list_all_file(dir, regex, re);
        } else {
            g_printf("Regex error: %s\n", error);
        }
    } else {
        g_printf("Search %s in %s\n", name, dir);

        list_all_file(dir, name, re);
    }
    amfree(name);
}


char *
convert_name(char *name)
{
    char *new_name;
    char *tmp;
    int i;

    if ((name[0] == '\"') && (name[strlen(name) - 1] == '\"')) {
        new_name = g_strdup(name + 1);
        new_name[strlen(new_name) - 1] = '\0';
    } else {
        new_name = g_strdup(name);
    }
    i = 0;
    while (new_name[i] != '\0') {
        if (new_name[i] == '/') {
            new_name[i] = '\0';
            tmp = g_strconcat(new_name, "\342\201\204", &new_name[i + 1], NULL);
            amfree(new_name);
            new_name = tmp;
            i += 2;
        }
        i++;
    }
    return new_name;
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
        amfree(item->tpath);
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

    next = (DIR_ITEM *)g_malloc(sizeof(DIR_ITEM));
    memset(next, 0, sizeof(DIR_ITEM));

    next->date = g_strdup(date);
    next->level = level;
    next->tape = g_strdup(tape);
    next->fileno = fileno;
    next->path = g_strdup(path);
    next->tpath = translate_octal(g_strdup(path));
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
    char *qdisk_path;

    if (disk_path == NULL) {
	g_printf(_("Directory must be set before getting listing\n"));
	return;
    } else if(g_str_equal(disk_path, "/")) {
	disk_path_slash = g_strdup(disk_path);
    } else {
	disk_path_slash = g_strconcat(disk_path, "/", NULL);
    }

    clear_dir_list();

    qdisk_path = quote_string(disk_path);
    cmd = g_strconcat("OLSD ", qdisk_path, NULL);
    amfree(qdisk_path);
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
    disk_path_slash_dot = g_strconcat(disk_path_slash, ".", NULL);
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
		cmd = g_strdup(l);	/* save for the error report */
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
	if (strncmp_const_skip(l, "201-", s, ch) != 0) {
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
	skip_quoted_string(s, ch);
	tape_undo = s - 1;
	tape_undo_ch = *tape_undo;
	*tape_undo = '\0';
	tape = unquote_string(tape);

	if(am_has_feature(indexsrv_features, fe_amindexd_fileno_in_OLSD)) {
	    long long fileno_ = (long long)0;
	    skip_whitespace(s, ch);
	    if(ch == '\0' || sscanf(s - 1, "%lld", &fileno_) != 1) {
		err = _("bad reply: cannot parse fileno field");
		amfree(tape);
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
	    amfree(tape);
	    continue;
	}
	qdir = s - 1;
	dir = unquote_string(qdir);

	/* add a '.' if it a the entry for the current directory */
	if((g_str_equal(disk_path, dir)) || (g_str_equal(disk_path_slash, dir))) {
	    amfree(dir);
	    dir = g_strdup(disk_path_slash_dot);
	}
	add_dir_list_item(date, level, tape, fileno, dir);
	amfree(tape);
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
    pager_command = g_strconcat(pager, " ; /bin/cat > /dev/null", NULL);
    if ((fp = popen(pager_command, "w")) == NULL)
    {
	g_printf(_("Warning - can't pipe through %s\n"), pager);
	fp = stdout;
    }
    amfree(pager_command);
    i = strlen(disk_tpath);
    if (i != 1)
	i++;				/* so disk_tpath != "/" */
    for (item = get_dir_list(); item != NULL; item=get_next_dir_item(item)) {
	quoted = quote_string(item->tpath + i);
	g_fprintf(fp, "%s %s\n", item->date, quoted);
	amfree(quoted);
    }
    apclose(fp);
}
