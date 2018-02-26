/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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
 * $Id: server_util.h,v 1.11 2006/05/25 01:47:20 johnfranks Exp $
 *
 */
#ifndef SERVER_UTIL_H
#define	SERVER_UTIL_H

#include "amutil.h"
#include "diskfile.h"
#include "infofile.h"
#include "cmdfile.h"

#define MAX_ARGS 32

/*
 * Some lints are confused by: typedef enum (...) xxx_t;
 * So here we use an equivalent of type int and an unnamed enum for constants.
 */
typedef int cmd_t;
enum {
    BOGUS, QUIT, QUITTING, DONE, PARTIAL,
    START, FILE_DUMP, PORT_DUMP, CONTINUE, ABORT,	/* dumper cmds */
    SUCCESS, FAILED, TRYAGAIN, NO_ROOM, RQ_MORE_DISK,	/* dumper results */
    ABORT_FINISHED, BAD_COMMAND,			/* dumper results */
    START_TAPER, FILE_WRITE, NEW_TAPE, NO_NEW_TAPE,     /* taper... */
    SHM_WRITE, SHM_DUMP, SHM_NAME,
    PARTDONE, PORT_WRITE, VAULT_WRITE, DUMPER_STATUS,   /* ... cmds */
    PORT, TAPE_ERROR, TAPER_OK,				/* taper results */
    REQUEST_NEW_TAPE, DIRECTTCP_PORT, TAKE_SCRIBE_FROM,
    START_SCAN, CLOSE_VOLUME, CLOSED_VOLUME,
    OPENED_SOURCE_VOLUME,
    CLOSE_SOURCE_VOLUME, CLOSED_SOURCE_VOLUME,
    RETRY, READY, DUMP_FINISH, LAST_TOK
};
extern const char *cmdstr[];

struct cmdargs {
    cmd_t cmd;
    int argc;
    char **argv;
};

typedef struct part_result_t {
    char *timestamp;
    char *hostname;
    char *diskname;
    int   level;
    char *storage;
    char *pool;
    char *label;
    char *dump_status;
    char *copy_status;
    char *part_status;
    int   filenum;
    int   nb_parts;
    int   partnum;
    int   nb_files;
    int   nb_directory;
} part_result_t;

struct cmdargs *getcmd(void);
struct cmdargs *get_pending_cmd(void);
void free_cmdargs(struct cmdargs *cmdargs);
void putresult(cmd_t result, const char *, ...) G_GNUC_PRINTF(2, 3);

struct taper_s;
struct wtaper_s;
int taper_cmd(struct taper_s *taper, struct wtaper_s *wtaper, cmd_t cmd,
	      void *ptr, char *destname, int level, char *datestamp);

struct sched_s;
struct disk_s;
struct chunker_s;
int chunker_cmd(struct chunker_s *chunker, cmd_t cmd, struct sched_s *sp,
		char *mesg);

struct dumper_s;
int dumper_cmd(struct dumper_s *dumper, cmd_t cmd, struct sched_s *sp,
	       char *mesg);

char *amhost_get_security_conf(char *string, void *arg);
int check_infofile(char *infodir, disklist_t *dl, char **errmsg);

void run_server_script(pp_script_t  *pp_script,
		       execute_on_t  execute_on,
		       char         *config,
		       char         *timestamp,
		       disk_t       *dp,
		       int           level,
		       cmd_t         result);
void run_server_dle_scripts(execute_on_t  execute_on,
			    char         *config,
			    char         *timestamp,
			    disk_t       *dp,
		            int           level,
			    cmd_t         result);
void run_server_host_scripts(execute_on_t  execute_on,
			     char         *config,
			     char         *timestamp,
			     am_host_t    *hostp);
void run_server_global_scripts(execute_on_t  execute_on,
			       char         *config,
			       char         *timestamp);

void run_amcleanup(char *config_name);
char *get_master_process(char *logfile);

gint64 internal_server_estimate(disk_t *dp, info_t *info,
                                int level, int *stats, tapetype_t *tapetype);
int server_can_do_estimate(disk_t *dp, info_t *info, int level,
			   tapetype_t *tapetype);

char *run_amcatalog(char *command, int n_args, ...);
void quit_amcatalog(void);
void amcatalog_remove_working_cmd(int pid);
void amcatalog_remove_cmd(int id);
int amcatalog_add_cmd(cmddata_t *cmddata);
cmddata_t * amcatalog_get_cmd_from_id(int id);
int         amcatalog_get_nb_image_cmd_for_storage(char *hostname, char *diskname, char *dump_timestamp, int level, char *dst_storage);
GPtrArray * amcatalog_get_flush_cmd(void);
GPtrArray * amcatalog_get_copy_cmd(void);
gboolean    amcatalog_holding_have_cmd(char *holding);
GHashTable *amcatalog_get_log_names(void);
GHashTable *amcatalog_get_dump_list(void);
GHashTable *amcatalog_get_parts(char *hostname, char *diskname);
gboolean cat_dump_hash_exist(GHashTable *dump_hash, char *hostname,
			     char *diskname, char *timestamp, int level);

#endif	/* SERVER_UTIL_H */
