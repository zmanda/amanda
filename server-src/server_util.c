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
#include "logfile.h"
#include "util.h"
#include "conffile.h"
#include "diskfile.h"
#include "pipespawn.h"
#include "conffile.h"
#include "sys/wait.h"

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


struct cmdargs *
getcmd(void)
{
    char *line;
    cmd_t cmd_i;
    struct cmdargs *cmdargs = g_new0(struct cmdargs, 1);

    if (isatty(0)) {
	g_printf("%s> ", get_pname());
	fflush(stdout);
        line = agets(stdin);
    } else {
        line = agets(stdin);
    }
    if (line == NULL) {
	line = stralloc("QUIT");
    }

    dbprintf(_("getcmd: %s\n"), line);

    cmdargs->argv = split_quoted_strings(line);
    cmdargs->argc = g_strv_length(cmdargs->argv);
    cmdargs->cmd = BOGUS;

    amfree(line);

    if (cmdargs->argc < 1) {
	return cmdargs;
    }

    for(cmd_i=BOGUS; cmdstr[cmd_i] != NULL; cmd_i++)
	if(strcmp(cmdargs->argv[0], cmdstr[cmd_i]) == 0) {
	    cmdargs->cmd = cmd_i;
	    return cmdargs;
	}
    return cmdargs;
}

struct cmdargs *
get_pending_cmd(void)
{
    SELECT_ARG_TYPE ready;
    struct timeval  to;
    int             nfound;

    FD_ZERO(&ready);
    FD_SET(0, &ready);
    to.tv_sec = 0;
    to.tv_usec = 0;

    nfound = select(1, &ready, NULL, NULL, &to);
    if (nfound && FD_ISSET(0, &ready)) {
        return getcmd();
    } else {
	return NULL;
    }
}

void
free_cmdargs(
    struct cmdargs *cmdargs)
{
    if (!cmdargs)
	return;
    if (cmdargs->argv)
	g_strfreev(cmdargs->argv);
    g_free(cmdargs);
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
    else if(strcmp(string, "client_port")==0)
	return ((am_host_t *)arg)->disks->client_port;
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
		    if(mkpdir(infofile, (mode_t)0755, (uid_t)-1,
			      (gid_t)-1) == -1) {
			*errmsg = vstralloc("Can't create directory for ",
					    infofile, NULL);
			return -1;
		    }
		    if(copy_file(infofile, old_infofile, errmsg) == -1) 
			return -1;
		}
	    }
	}
	amfree(diskdir);
	amfree(hostinfodir);
	amfree(infofile);
    }
    return 0;
}

void
run_server_script(
    pp_script_t  *pp_script,
    execute_on_t  execute_on,
    char         *config,
    disk_t	 *dp,
    int           level)
{
    pid_t      scriptpid;
    int        scriptin, scriptout, scripterr;
    char      *cmd;
    char      *command = NULL;
    GPtrArray *argv_ptr = g_ptr_array_new();
    FILE      *streamout;
    char      *line;
    char      *plugin;
    char       level_number[NUM_STR_SIZE];

    if ((pp_script_get_execute_on(pp_script) & execute_on) == 0)
	return;
    if (pp_script_get_execute_where(pp_script) != ES_SERVER)
	return;

    plugin = pp_script_get_plugin(pp_script);
    cmd = vstralloc(APPLICATION_DIR, "/", plugin, NULL);
    g_ptr_array_add(argv_ptr, stralloc(plugin));

    switch (execute_on) {
    case EXECUTE_ON_PRE_DLE_AMCHECK:
	command = "PRE-DLE-AMCHECK";
	break;
    case EXECUTE_ON_PRE_HOST_AMCHECK:
	command = "PRE-HOST-AMCHECK";
	break;
    case EXECUTE_ON_POST_DLE_AMCHECK:
	command = "POST-DLE-AMCHECK";
	break;
    case EXECUTE_ON_POST_HOST_AMCHECK:
	command = "POST-HOST-AMCHECK";
	break;
    case EXECUTE_ON_PRE_DLE_ESTIMATE:
	command = "PRE-DLE-ESTIMATE";
	break;
    case EXECUTE_ON_PRE_HOST_ESTIMATE:
	command = "PRE-HOST-ESTIMATE";
	break;
    case EXECUTE_ON_POST_DLE_ESTIMATE:
	command = "POST-DLE-ESTIMATE";
	break;
    case EXECUTE_ON_POST_HOST_ESTIMATE:
	command = "POST-HOST-ESTIMATE";
	break;
    case EXECUTE_ON_PRE_DLE_BACKUP:
	command = "PRE-DLE-BACKUP";
	break;
    case EXECUTE_ON_PRE_HOST_BACKUP:
	command = "PRE-HOST-BACKUP";
	break;
    case EXECUTE_ON_POST_DLE_BACKUP:
	command = "POST-DLE-BACKUP";
	break;
    case EXECUTE_ON_POST_HOST_BACKUP:
	command = "POST-HOST-BACKUP";
	break;
    case EXECUTE_ON_PRE_RECOVER:
    case EXECUTE_ON_POST_RECOVER:
    case EXECUTE_ON_PRE_LEVEL_RECOVER:
    case EXECUTE_ON_POST_LEVEL_RECOVER:
    case EXECUTE_ON_INTER_LEVEL_RECOVER:
	{
	     // ERROR these script can't be executed on server.
	     return;
	}
    }

    g_ptr_array_add(argv_ptr, stralloc(command));
    g_ptr_array_add(argv_ptr, stralloc("--execute-where"));
    g_ptr_array_add(argv_ptr, stralloc("server"));

    if (config) {
	g_ptr_array_add(argv_ptr, stralloc("--config"));
	g_ptr_array_add(argv_ptr, stralloc(config));
    }
    if (dp->host->hostname) {
	g_ptr_array_add(argv_ptr, stralloc("--host"));
	g_ptr_array_add(argv_ptr, stralloc(dp->host->hostname));
    }
    if (dp->name) {
	g_ptr_array_add(argv_ptr, stralloc("--disk"));
	g_ptr_array_add(argv_ptr, stralloc(dp->name));
    }
    if (dp->device) {
	g_ptr_array_add(argv_ptr, stralloc("--device"));
	g_ptr_array_add(argv_ptr, stralloc(dp->device));
    }
    if (level >= 0) {
	g_snprintf(level_number, SIZEOF(level_number), "%d", level);
	g_ptr_array_add(argv_ptr, stralloc("--level"));
	g_ptr_array_add(argv_ptr, stralloc(level_number));
    }

    property_add_to_argv(argv_ptr, pp_script_get_property(pp_script));
    g_ptr_array_add(argv_ptr, NULL);

    scripterr = fileno(stderr);
    scriptpid = pipespawnv(cmd, STDIN_PIPE|STDOUT_PIPE, 0, &scriptin,
			   &scriptout, &scripterr,
			   (char **)argv_ptr->pdata);
    close(scriptin);

    streamout = fdopen(scriptout, "r");
    if (streamout) {
	while((line = agets(streamout)) != NULL) {
	    dbprintf("script: %s\n", line);
	}
    }
    fclose(streamout);
    waitpid(scriptpid, NULL, 0);
    g_ptr_array_free_full(argv_ptr);
}


void
run_server_scripts(
    execute_on_t  execute_on,
    char         *config,
    disk_t	 *dp,
    int           level)
{
    identlist_t pp_scriptlist;

    for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
	 pp_scriptlist = pp_scriptlist->next) {
	pp_script_t *pp_script = lookup_pp_script((char *)pp_scriptlist->data);
	g_assert(pp_script != NULL);
	run_server_script(pp_script, execute_on, config, dp, level);
    }
}

void
run_amcleanup(
    char *config_name)
{
    pid_t amcleanup_pid;
    char *amcleanup_program;
    char *amcleanup_options[4];

    switch(amcleanup_pid = fork()) {
	case -1:
	    return;
	    break;
	case  0: /* child process */
	    amcleanup_program = vstralloc(sbindir, "/", "amcleanup", NULL);
	    amcleanup_options[0] = amcleanup_program;
	    amcleanup_options[1] = "-p";
	    amcleanup_options[2] = config_name;
	    amcleanup_options[3] = NULL;
	    execve(amcleanup_program, amcleanup_options, safe_env());
	    error("exec %s: %s", amcleanup_program, strerror(errno));
	    /*NOTREACHED*/
	default:
	    break;
    }
    waitpid(amcleanup_pid, NULL, 0);
}

char *
get_master_process(
    char *logfile)
{
    FILE *log;
    char line[1024];
    char *s, ch;
    char *process_name;

    log = fopen(logfile, "r");
    if (!log)
	return stralloc("UNKNOWN");

    while(fgets(line, 1024, log)) {
	if (strncmp_const(line, "INFO ") == 0) {
	    s = line+5;
	    ch = *s++;
	    process_name = s-1;
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	    skip_whitespace(s, ch);
	    skip_non_whitespace(s, ch);
	    s[-1] = '\0';
	    skip_whitespace(s, ch);
	    if (strncmp_const(s-1, "pid ") == 0) {
		process_name = stralloc(process_name);
		fclose(log);
		return process_name;
	    }
	}
    }
    fclose(log);
    return stralloc("UNKNOWN");
}
