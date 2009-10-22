/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1999 University of Maryland at College Park
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

#include "util.h"
#include "diskfile.h"

#define MAX_ARGS 32

/*
 * Some lints are confused by: typedef enum (...) xxx_t;
 * So here we use an equivalent of type int and an unnamed enum for constants.
 */
typedef int cmd_t;
enum {
    BOGUS, QUIT, QUITTING, DONE, PARTIAL,
    START, FILE_DUMP, PORT_DUMP, CONTINUE, ABORT,	/* dumper cmds */
    FAILED, TRYAGAIN, NO_ROOM, RQ_MORE_DISK,		/* dumper results */
    ABORT_FINISHED, BAD_COMMAND,			/* dumper results */
    START_TAPER, FILE_WRITE, NEW_TAPE, NO_NEW_TAPE,     /* taper... */
    PARTDONE, PORT_WRITE, DUMPER_STATUS,                /* ... cmds */
    PORT, TAPE_ERROR, TAPER_OK,				/* taper results */
    REQUEST_NEW_TAPE, DIRECTTCP_PORT,
    LAST_TOK
};
extern const char *cmdstr[];

struct cmdargs {
    cmd_t cmd;
    int argc;
    char **argv;
};

struct cmdargs *getcmd(void);
struct cmdargs *get_pending_cmd(void);
void free_cmdargs(struct cmdargs *cmdargs);
void putresult(cmd_t result, const char *, ...) G_GNUC_PRINTF(2, 3);
int taper_cmd(cmd_t cmd, void *ptr, char *destname, int level, char *datestamp);

struct disk_s;
struct chunker_s;
int chunker_cmd(struct chunker_s *chunker, cmd_t cmd, struct disk_s *dp,
		char *mesg);

struct dumper_s;
int dumper_cmd(struct dumper_s *dumper, cmd_t cmd, struct disk_s *dp,
	       char *mesg);

char *amhost_get_security_conf(char *string, void *arg);
int check_infofile(char *infodir, disklist_t *dl, char **errmsg);

void run_server_script(pp_script_t  *pp_script,
		       execute_on_t  execute_on,
		       char         *config,
		       disk_t       *dp,
		       int           level);
void run_server_scripts(execute_on_t  execute_on,
			char         *config,
			disk_t       *dp,
		        int           level);

void run_amcleanup(char *config_name);
char *get_master_process(char *logfile);

#endif	/* SERVER_UTIL_H */
