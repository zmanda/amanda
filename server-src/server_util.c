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
 * $Id: server_util.c,v 1.17 2006/05/25 01:47:20 johnfranks Exp $
 *
 */

#include "amanda.h"
#include "server_util.h"
#include "arglist.h"
#include "token.h"
#include "logfile.h"
#include "util.h"
#include "conffile.h"
#include "diskfile.h"

const char *cmdstr[] = {
    "BOGUS", "QUIT", "QUITTING", "DONE", "PARTIAL", 
    "START", "FILE-DUMP", "PORT-DUMP", "CONTINUE", "ABORT",/* dumper cmds */
    "FAILED", "TRY-AGAIN", "NO-ROOM", "RQ-MORE-DISK",	/* dumper results */
    "ABORT-FINISHED", "BAD-COMMAND",			/* dumper results */
    "START-TAPER", "FILE-WRITE", "NEW-TAPE", "NO-NEW-TAPE",
     
    "PARTDONE", "PORT-WRITE", "DUMPER-STATUS",		    /* taper cmds */
    "PORT", "TAPE-ERROR", "TAPER-OK",			 /* taper results */
    "REQUEST-NEW-TAPE",
    "LAST_TOK",
    NULL
};


cmd_t
getcmd(
    struct cmdargs *	cmdargs)
{
    char *line;
    cmd_t cmd_i;

    assert(cmdargs != NULL);

    if (isatty(0)) {
	g_printf("%s> ", get_pname());
	fflush(stdout);
        line = readline(NULL);
    } else {
        line = agets(stdin);
    }
    if (line == NULL) {
	line = stralloc("QUIT");
    }

    cmdargs->argc = split(line, cmdargs->argv,
	(int)(sizeof(cmdargs->argv) / sizeof(cmdargs->argv[0])), " ");
    dbprintf(_("getcmd: %s\n"), line);
    amfree(line);

#if DEBUG
    {
	int i;
	g_fprintf(stderr,_("argc = %d\n"), cmdargs->argc);
	for (i = 0; i < cmdargs->argc+1; i++)
	    g_fprintf(stderr,_("argv[%d] = \"%s\"\n"), i, cmdargs->argv[i]);
    }
#endif

    if (cmdargs->argc < 1)
	return (BOGUS);

    for(cmd_i=BOGUS; cmdstr[cmd_i] != NULL; cmd_i++)
	if(strcmp(cmdargs->argv[1], cmdstr[cmd_i]) == 0)
	    return (cmd_i);
    return (BOGUS);
}


printf_arglist_function1(void putresult, cmd_t, result, const char *, format)
{
    va_list argp;

    arglist_start(argp, format);
    dbprintf(_("putresult: %d %s\n"), result, cmdstr[result]);
    g_printf("%s ", cmdstr[result]);
    g_vprintf(format, argp);
    fflush(stdout);
    arglist_end(argp);
}

char *
amhost_get_security_conf(
    char *	string,
    void *	arg)
{
    if(!string || !*string)
	return(NULL);

    if(strcmp(string, "krb5principal")==0)
	return(getconf_str(CNF_KRB5PRINCIPAL));
    else if(strcmp(string, "krb5keytab")==0)
	return(getconf_str(CNF_KRB5KEYTAB));

    if(!arg || !((am_host_t *)arg)->disks) return(NULL);

    if(strcmp(string, "amandad_path")==0)
	return ((am_host_t *)arg)->disks->amandad_path;
    else if(strcmp(string, "client_username")==0)
	return ((am_host_t *)arg)->disks->client_username;
    else if(strcmp(string, "ssh_keys")==0)
	return ((am_host_t *)arg)->disks->ssh_keys;

    return(NULL);
}

int check_infofile(
    char        *infodir,
    disklist_t  *dl,
    char       **errmsg)
{
    disk_t      *dp, *diskp;
    char        *hostinfodir, *old_hostinfodir, *Xhostinfodir;
    char        *diskdir,     *old_diskdir,     *Xdiskdir;
    char        *infofile,    *old_infofile,    *Xinfofile;
    struct stat  statbuf;
    int other_dle_match;

    if (stat(infodir, &statbuf) != 0) {
	return 0;
    }

    for (dp = dl->head; dp != NULL; dp = dp->next) {
	hostinfodir = sanitise_filename(dp->host->hostname);
	diskdir     = sanitise_filename(dp->name);
	infofile = vstralloc(infodir, "/", hostinfodir, "/", diskdir,
			     "/info", NULL);
	if (stat(infofile, &statbuf) == -1 && errno == ENOENT) {
	    old_hostinfodir = old_sanitise_filename(dp->host->hostname);
	    old_diskdir     = old_sanitise_filename(dp->name);
	    old_infofile    = vstralloc(infodir, old_hostinfodir, "/",
					old_diskdir, "/info", NULL);
	    if (stat(old_infofile, &statbuf) == 0) {
		other_dle_match = 0;
		diskp = dl->head;
		while (diskp != NULL) {
		    Xhostinfodir = sanitise_filename(diskp->host->hostname);
		    Xdiskdir     = sanitise_filename(diskp->name);
		    Xinfofile = vstralloc(infodir, "/", Xhostinfodir, "/",
					  Xdiskdir, "/info", NULL);
		    if (strcmp(old_infofile, Xinfofile) == 0) {
			other_dle_match = 1;
			diskp = NULL;
		    }
		    else {
			diskp = diskp->next;
		    }
		}
		if (other_dle_match == 0) {
		    if(mkpdir(infofile, (mode_t)02755, (uid_t)-1,
			      (gid_t)-1) == -1) 
			*errmsg = vstralloc("Can't create directory for ",
					    infofile, NULL);
			return -1;
		    if(copy_file(infofile, old_infofile, errmsg) == -1) 
			return -1;
		}
	    }
	}
    }
    return 0;
}
