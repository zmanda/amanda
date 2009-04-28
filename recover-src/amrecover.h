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
 * $Id: amrecover.h,v 1.19 2006/05/25 01:47:14 johnfranks Exp $
 *
 * data structures and declarations for amrecover
 */

#include "amanda.h"
#include "amfeatures.h"
#include "amxml.h"

#define STARTUP_TIMEOUT 60

typedef struct DIR_ITEM
{
    char *date;
    int  level;
    char *tape;
    char *path;
    off_t  fileno;

    struct DIR_ITEM *next;
}
DIR_ITEM;

extern char *server_name;
extern char *config;
extern char *dump_datestamp;		/* date we are restoring */
extern char *dump_hostname;		/* which machine we are restoring */
extern char *disk_name;			/* disk we are restoring */
extern dle_t *dump_dle;
extern char *mount_point;		/* where disk was mounted */
extern char *disk_path;			/* path relative to mount point */
extern char dump_date[STR_SIZE];	/* date on which we are restoring */
extern int quit_prog;			/* set when time to exit parser */
extern char *tape_server_name;
extern char *tape_device_name;
extern char *authopt;
extern am_feature_t *our_features;
extern char *our_features_string;
extern am_feature_t *indexsrv_features;
extern am_feature_t *tapesrv_features;
extern pid_t extract_restore_child_pid;

extern void free_dir_item(DIR_ITEM *item);

extern int converse(char *cmd);
extern int exchange(char *cmd);
extern int server_happy(void);
extern int send_command(char *cmd);
extern int get_reply_line(void);
extern char *reply_line(void);

extern void quit(void);

extern void help_list(void);		/* list commands */

extern void set_disk(char *dsk, char *mtpt);
extern void list_disk(char *amdevice);
extern void set_host(const char *host);
extern void list_host(void);
extern int set_date(char *date);
extern int set_directory(char *dir, int verbose);
extern void local_cd(char *dir);
extern int cd_glob(char *dir, int verbose);
extern int cd_regex(char *dir, int verbose);
extern int cd_dir(char *dir, char *default_dir, int verbose);
extern void set_tape(char *tape);
extern void set_device(char *host, char *device);
extern void show_directory(void);
extern void set_mode(int mode);
extern void show_mode(void);

extern void list_disk_history(void);
extern void list_directory(void);
extern DIR_ITEM *get_dir_list(void);
extern DIR_ITEM *get_next_dir_item(DIR_ITEM *this);
extern void suck_dir_list_from_server(void);
extern void clear_dir_list(void);
extern void clean_pathname(char *s);
extern void display_extract_list(char *file);
extern void clear_extract_list(void);
extern int is_extract_list_nonempty(void);
extern void add_glob(char *glob);
extern void add_regex(char *regex);
extern void add_file(char *path, char *regex);
extern void delete_glob(char *glob);
extern void delete_regex(char *regex);
extern void delete_file(char *path, char *regex);

extern void extract_files(void);

#define SAMBA_SMBCLIENT 0
#define SAMBA_TAR       1

extern char *get_security(void);
extern void stop_amindexd(void);
