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
 * $Id: server_util.c,v 1.17 2006/05/25 01:47:20 johnfranks Exp $
 *
 */

#include "amanda.h"
#include "server_util.h"
#include "logfile.h"
#include "amutil.h"
#include "conffile.h"
#include "diskfile.h"
#include "pipespawn.h"
#include "conffile.h"
#include "infofile.h"
#include "backup_support_option.h"
#include "sys/wait.h"

const char *cmdstr[] = {
    "BOGUS", "QUIT", "QUITTING", "DONE", "PARTIAL",
    "START", "FILE-DUMP", "PORT-DUMP", "CONTINUE", "ABORT",/* dumper cmds */
    "FAILED", "TRY-AGAIN", "NO-ROOM", "RQ-MORE-DISK",	/* dumper results */
    "ABORT-FINISHED", "BAD-COMMAND",			/* dumper results */
    "START-TAPER", "FILE-WRITE", "NEW-TAPE", "NO-NEW-TAPE",
    "SHM-WRITE", "SHM-DUMP", "SHM-NAME",
    "PARTDONE", "PORT-WRITE", "VAULT-WRITE", "DUMPER-STATUS", /* taper cmds */
    "PORT", "TAPE-ERROR", "TAPER-OK",			 /* taper results */
    "REQUEST-NEW-TAPE", "DIRECTTCP-PORT", "TAKE-SCRIBE-FROM",
    "START-SCAN", "CLOSE-VOLUME", "CLOSED-VOLUME",
    "OPENED-SOURCE-VOLUME",
    "CLOSE-SOURCE-VOLUME", "CLOSED-SOURCE-VOLUME",
    "RETRY", "READY", "LAST_TOK",
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
        line = areads(0);
    }
    if (line == NULL) {
	line = g_strdup("QUIT");
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
	if(g_str_equal(cmdargs->argv[0], cmdstr[cmd_i])) {
	    cmdargs->cmd = cmd_i;
	    return cmdargs;
	}
    return cmdargs;
}

struct cmdargs *
get_pending_cmd(void)
{
    if (!areads_dataready(0))
	return NULL;
    return getcmd();
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

void putresult(cmd_t result, const char *format, ...)
{
    va_list argp;
    char *msg;

    arglist_start(argp, format);
    msg = g_strdup_vprintf(format, argp);
    arglist_end(argp);
    g_debug("putresult: %d %s %s", result, cmdstr[result], msg);
    g_printf("%s %s", cmdstr[result], msg);
    fflush(stdout);
    g_free(msg);
}

char *
amhost_get_security_conf(
    char *string,
    void *arg G_GNUC_UNUSED)
{
    char *result = NULL;
    disk_t *dp;

    if(!string || !*string)
	return(NULL);

    if (g_str_equal(string, "krb5principal"))
	result = getconf_str(CNF_KRB5PRINCIPAL);
    else if (g_str_equal(string, "krb5keytab"))
	result = getconf_str(CNF_KRB5KEYTAB);
    if (result) {
	if (strlen(result) == 0)
	    result = NULL;
	return result;
    }

    if (!arg || !((am_host_t *)arg)->disks) return(NULL);

    for (dp = ((am_host_t *)arg)->disks; dp != NULL; dp = dp->hostnext) {
	if (dp->todo)
	    break;
    }
    if (!dp) return(NULL);

    if (g_str_equal(string, "amandad_path"))
	result =  dp->amandad_path;
    else if (g_str_equal(string, "client_username"))
	result =  dp->client_username;
    else if (g_str_equal(string, "client_port"))
	result =  dp->client_port;
    else if (g_str_equal(string, "src_ip")) {
	char *result = interface_get_src_ip(((am_host_t *)arg)->netif->config);
	if (g_str_equal(result, "NULL"))
	    result = NULL;
    } else if(g_str_equal(string, "ssh_keys"))
	result =  dp->ssh_keys;
    else if (g_str_equal(string, "ssl_fingerprint_file"))
	result =  dp->ssl_fingerprint_file;
    else if (g_str_equal(string, "ssl_cert_file"))
	result =  dp->ssl_cert_file;
    else if (g_str_equal(string, "ssl_key_file"))
	result =  dp->ssl_key_file;
    else if (g_str_equal(string, "ssl_ca_cert_file"))
	result =  dp->ssl_ca_cert_file;
    else if (g_str_equal(string, "ssl_cipher_list"))
	result =  dp->ssl_cipher_list;
    else if (g_str_equal(string, "ssl_check_certificate_host")) {
	if (dp->ssl_check_certificate_host)
	    result = "1";
	else
	    result = "0";
    } else if (g_str_equal(string, "ssl_check_host")) {
	if (dp->ssl_check_host)
	    result = "1";
	else
	    result = "0";
    } else if (g_str_equal(string, "ssl_check_fingerprint")) {
	if (dp->ssl_check_fingerprint)
	    result = "1";
	else
	    result = "0";
    }

    if (result && strlen(result) == 0)
	result = NULL;

    return(result);
}

int check_infofile(
    char        *infodir,
    disklist_t  *dl,
    char       **errmsg)
{
    GList       *dlist, *dlist1;
    disk_t      *dp, *diskp;
    char        *hostinfodir, *old_hostinfodir;
    char        *diskdir,     *old_diskdir;
    char        *infofile,    *old_infofile;
    struct stat  statbuf;
    int other_dle_match;

    if (stat(infodir, &statbuf) != 0) {
	return 0;
    }

//    for (dp = dl->head; dp != NULL; dp = dp->next) {
    for (dlist = dl->head; dlist != NULL; dlist = dlist->next) {
	dp = dlist->data;
	hostinfodir = sanitise_filename(dp->host->hostname);
	diskdir     = sanitise_filename(dp->name);
	infofile = g_strjoin(NULL, infodir, "/", hostinfodir, "/", diskdir,
			     "/info", NULL);
	if (stat(infofile, &statbuf) == -1 && errno == ENOENT) {
	    old_hostinfodir = old_sanitise_filename(dp->host->hostname);
	    old_diskdir     = old_sanitise_filename(dp->name);
	    old_infofile    = g_strjoin(NULL, infodir, old_hostinfodir, "/",
					old_diskdir, "/info", NULL);
	    if (stat(old_infofile, &statbuf) == 0) {
		other_dle_match = 0;
		dlist1 = dl->head;
		while (dlist1 != NULL) {
		    char *Xhostinfodir;
		    char *Xdiskdir;
		    char *Xinfofile;

		    diskp = dlist1->data;
		    Xhostinfodir = sanitise_filename(diskp->host->hostname);
		    Xdiskdir     = sanitise_filename(diskp->name);
		    Xinfofile = g_strjoin(NULL, infodir, "/", Xhostinfodir, "/",
					  Xdiskdir, "/info", NULL);
		    if (g_str_equal(old_infofile, Xinfofile)) {
			other_dle_match = 1;
			dlist1 = NULL;
		    }
		    else {
			dlist1 = dlist1->next;
		    }
		    amfree(Xhostinfodir);
		    amfree(Xdiskdir);
		    amfree(Xinfofile);
		}
		if (other_dle_match == 0) {
		    if(mkpdir(infofile, (mode_t)0755, (uid_t)-1,
			      (gid_t)-1) == -1) {
			*errmsg = g_strjoin(NULL, "Can't create directory for ",
					    infofile, NULL);
			amfree(hostinfodir);
			amfree(diskdir);
			amfree(infofile);
			amfree(old_hostinfodir);
			amfree(old_diskdir);
			amfree(old_infofile);
			return -1;
		    }
		    if(copy_file(infofile, old_infofile, errmsg) == -1) {
			amfree(hostinfodir);
			amfree(diskdir);
			amfree(infofile);
			amfree(old_hostinfodir);
			amfree(old_diskdir);
			amfree(old_infofile);
			return -1;
		    }
		}
	    }
	    amfree(old_hostinfodir);
	    amfree(old_diskdir);
	    amfree(old_infofile);
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
    char         *timestamp,
    disk_t	 *dp,
    int           level)
{
    pid_t      scriptpid;
    int        scriptin, scriptout, scripterr;
    char      *cmd;
    char      *command = NULL;
    GPtrArray *argv_ptr;
    GPtrArray *errarray = NULL;
    FILE      *streamout;
    char      *line;
    char      *plugin;
    char       level_number[NUM_STR_SIZE];
    struct stat cmd_stat;
    int         result;
    backup_support_option_t *bsu;

    if ((pp_script_get_execute_on(pp_script) & execute_on) == 0)
	return;
    if (pp_script_get_execute_where(pp_script) != EXECUTE_WHERE_SERVER)
	return;

    plugin = pp_script_get_plugin(pp_script);

    cmd = g_strjoin(NULL, APPLICATION_DIR, "/", plugin, NULL);
    result = stat(cmd, &cmd_stat);
    if (result == -1) {
	dbprintf("Can't stat script '%s': %s\n", cmd, strerror(errno));
	amfree(cmd);
	cmd = g_strjoin(NULL, get_config_dir(), "/application/", plugin, NULL);
	result = stat(cmd, &cmd_stat);
	if (result == -1) {
	    dbprintf("Can't stat script '%s': %s\n", cmd, strerror(errno));
	    amfree(cmd);
	    cmd = g_strjoin(NULL, CONFIG_DIR, "/application/", plugin, NULL);
	    result = stat(cmd, &cmd_stat);
	    if (result == -1) {
		dbprintf("Can't stat script '%s': %s\n", cmd, strerror(errno));
		amfree(cmd);
		cmd = g_strjoin(NULL, APPLICATION_DIR, "/", plugin, NULL);
	    }
	}
    }

    switch (execute_on) {
    case EXECUTE_ON_PRE_AMCHECK:
	command = "PRE-AMCHECK";
	break;
    case EXECUTE_ON_PRE_DLE_AMCHECK:
	command = "PRE-DLE-AMCHECK";
	break;
    case EXECUTE_ON_PRE_HOST_AMCHECK:
	command = "PRE-HOST-AMCHECK";
	break;
    case EXECUTE_ON_POST_AMCHECK:
	command = "POST-AMCHECK";
	break;
    case EXECUTE_ON_POST_DLE_AMCHECK:
	command = "POST-DLE-AMCHECK";
	break;
    case EXECUTE_ON_POST_HOST_AMCHECK:
	command = "POST-HOST-AMCHECK";
	break;
    case EXECUTE_ON_PRE_ESTIMATE:
	command = "PRE-ESTIMATE";
	break;
    case EXECUTE_ON_PRE_DLE_ESTIMATE:
	command = "PRE-DLE-ESTIMATE";
	break;
    case EXECUTE_ON_PRE_HOST_ESTIMATE:
	command = "PRE-HOST-ESTIMATE";
	break;
    case EXECUTE_ON_POST_ESTIMATE:
	command = "POST-ESTIMATE";
	break;
    case EXECUTE_ON_POST_DLE_ESTIMATE:
	command = "POST-DLE-ESTIMATE";
	break;
    case EXECUTE_ON_POST_HOST_ESTIMATE:
	command = "POST-HOST-ESTIMATE";
	break;
    case EXECUTE_ON_PRE_BACKUP:
	command = "PRE-BACKUP";
	break;
    case EXECUTE_ON_PRE_DLE_BACKUP:
	command = "PRE-DLE-BACKUP";
	break;
    case EXECUTE_ON_PRE_HOST_BACKUP:
	command = "PRE-HOST-BACKUP";
	break;
    case EXECUTE_ON_POST_BACKUP:
	command = "POST-BACKUP";
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
	     amfree(cmd);
	     return;
	}
    default:
	amfree(cmd);
	return;
    }

    bsu = backup_support_option(plugin, &errarray);
    if (!bsu) {
        guint  i;
        for (i=0; i < errarray->len; i++) {
            char *line = g_ptr_array_index(errarray, i);
	    g_debug("Script: '%s': %s", plugin, line);
        }
        if (i == 0) { /* nothing in errarray */
	    g_debug("Script: '%s': cannot execute support command", plugin);
        }
        g_ptr_array_free_full(errarray);
    }

    argv_ptr = g_ptr_array_new();
    g_ptr_array_add(argv_ptr, g_strdup(plugin));
    g_ptr_array_add(argv_ptr, g_strdup(command));
    if (!bsu || bsu->execute_where) {
	g_ptr_array_add(argv_ptr, g_strdup("--execute-where"));
	g_ptr_array_add(argv_ptr, g_strdup("server"));
    }

    if (config && (!bsu || bsu->config)) {
	g_ptr_array_add(argv_ptr, g_strdup("--config"));
	g_ptr_array_add(argv_ptr, g_strdup(config));
    }
    if (timestamp && (!bsu || bsu->timestamp)) {
	g_ptr_array_add(argv_ptr, g_strdup("--timestamp"));
	g_ptr_array_add(argv_ptr, g_strdup(timestamp));
    }
    if (dp->host->hostname && (!bsu || bsu->host)) {
	g_ptr_array_add(argv_ptr, g_strdup("--host"));
	g_ptr_array_add(argv_ptr, g_strdup(dp->host->hostname));
    }
    if (dp->name && (!bsu || bsu->disk)) {
	g_ptr_array_add(argv_ptr, g_strdup("--disk"));
	g_ptr_array_add(argv_ptr, g_strdup(dp->name));
    }
    if (dp->device) {
	g_ptr_array_add(argv_ptr, g_strdup("--device"));
	g_ptr_array_add(argv_ptr, g_strdup(dp->device));
    }
    if (level >= 0) {
	g_snprintf(level_number, sizeof(level_number), "%d", level);
	g_ptr_array_add(argv_ptr, g_strdup("--level"));
	g_ptr_array_add(argv_ptr, g_strdup(level_number));
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
	    amfree(line);
	}
	fclose(streamout);
    }
    waitpid(scriptpid, NULL, 0);
    g_ptr_array_free_full(argv_ptr);
    amfree(cmd);
    g_free(bsu);
}


void
run_server_dle_scripts(
    execute_on_t  execute_on,
    char         *config,
    char         *timestamp,
    disk_t	 *dp,
    int           level)
{
    identlist_t pp_scriptlist;

    for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
	 pp_scriptlist = pp_scriptlist->next) {
	pp_script_t *pp_script = lookup_pp_script((char *)pp_scriptlist->data);
	g_assert(pp_script != NULL);
	run_server_script(pp_script, execute_on, config, timestamp, dp, level);
    }
}

void
run_server_host_scripts(
    execute_on_t  execute_on,
    char         *config,
    char         *timestamp,
    am_host_t	 *hostp)
{
    identlist_t pp_scriptlist;
    disk_t *dp;

    GHashTable* executed = g_hash_table_new_full(g_str_hash, g_str_equal,
						 NULL, NULL);
    for (dp = hostp->disks; dp != NULL; dp = dp->hostnext) {
	if (dp->todo) {
	    for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
		 pp_scriptlist = pp_scriptlist->next) {
		int todo = 1;
		pp_script_t *pp_script = lookup_pp_script((char *)pp_scriptlist->data);
		g_assert(pp_script != NULL);
		if (pp_script_get_single_execution(pp_script)) {
		    todo = g_hash_table_lookup(executed,
					       pp_script_get_plugin(pp_script))
			   == NULL;
		}
		if (todo) {
		    run_server_script(pp_script, execute_on, config, timestamp,
				      dp, -1);
		    if (pp_script_get_single_execution(pp_script)) {
			g_hash_table_insert(executed,
					    pp_script_get_plugin(pp_script),
					    GINT_TO_POINTER(1));
		    }
		}
	    }
	}
    }

    g_hash_table_destroy(executed);
}

void
run_server_global_scripts(
    execute_on_t  execute_on,
    char         *config,
    char         *timestamp)
{
    identlist_t  pp_scriptlist;
    disk_t      *dp;
    am_host_t   *host;

    GHashTable* executed = g_hash_table_new_full(g_str_hash, g_str_equal,
						 NULL, NULL);
    for (host = get_hostlist(); host != NULL; host = host->next) {
	for (dp = host->disks; dp != NULL; dp = dp->hostnext) {
	    if (dp->todo) {
		for (pp_scriptlist = dp->pp_scriptlist; pp_scriptlist != NULL;
		     pp_scriptlist = pp_scriptlist->next) {
		    int todo = 1;
		    pp_script_t *pp_script =
				 lookup_pp_script((char *)pp_scriptlist->data);
		    g_assert(pp_script != NULL);
		    if (pp_script_get_single_execution(pp_script)) {
			todo = g_hash_table_lookup(executed,
				pp_script_get_plugin(pp_script)) == NULL;
		    }
		    if (todo) {
			run_server_script(pp_script, execute_on, config,
					  timestamp, dp, -1);
			if (pp_script_get_single_execution(pp_script)) {
			    g_hash_table_insert(executed,
					pp_script_get_plugin(pp_script),
					GINT_TO_POINTER(1));
			}
		    }
		}
	    }
	}
    }
    g_hash_table_destroy(executed);
}

void
run_amcleanup(
    char *config_name)
{
    pid_t  amcleanup_pid;
    char  *amcleanup_program;
    char  *amcleanup_options[4];
    char **env;

    switch(amcleanup_pid = fork()) {
	case -1:
	    return;
	    break;
	case  0: /* child process */
	    amcleanup_program = g_strjoin(NULL, sbindir, "/", "amcleanup", NULL);
	    amcleanup_options[0] = amcleanup_program;
	    amcleanup_options[1] = "-p";
	    amcleanup_options[2] = config_name;
	    amcleanup_options[3] = NULL;
	    env = safe_env();
	    execve(amcleanup_program, amcleanup_options, env);
	    free_env(env);
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
	return g_strdup("UNKNOWN");

    while (untaint_fgets(line, 1024, log)) {
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
		process_name = g_strdup(process_name);
		fclose(log);
		return process_name;
	    }
	}
    }
    fclose(log);
    return g_strdup("UNKNOWN");
}


gint64
internal_server_estimate(
    disk_t *dp,
    info_t *info,
    int     level,
    int    *stats,
    tapetype_t *tapetype)
{
    int    j;
    gint64 size = 0;

    *stats = 0;

    if (level == 0) { /* use latest level 0, should do extrapolation */
	gint64 est_size = (gint64)0;
	int nb_est = 0;

	for (j=NB_HISTORY-2; j>=0; j--) {
	    if (info->history[j].level == 0) {
		if (info->history[j].size <= (gint64)0) continue;
		est_size = info->history[j].size;
		nb_est++;
	    }
	}
	if (nb_est > 0) {
	    size = est_size;
	    *stats = 1;
	} else if (info->inf[level].size > (gint64)1000) { /* stats */
	    size = info->inf[level].size;
	    *stats = 1;
	} else {
	    size = (gint64)1000000;
	    if (size > tapetype_get_length(tapetype)/2)
		size = tapetype_get_length(tapetype)/2;
	    *stats = 0;
	}
    } else if (level == info->last_level) {
	/* means of all X day at the same level */
	#define NB_DAY 30
	int nb_day = 0;
	gint64 est_size_day[NB_DAY];
	int nb_est_day[NB_DAY];

	for (j=0; j<NB_DAY; j++) {
	    est_size_day[j] = (gint64)0;
	    nb_est_day[j] = 0;
	}

	for (j=NB_HISTORY-2; j>=0; j--) {
	    if (info->history[j].level <= 0) continue;
	    if (info->history[j].size <= (gint64)0) continue;
	    if (info->history[j].level == info->history[j+1].level) {
		if (nb_day <NB_DAY-1) nb_day++;
		est_size_day[nb_day] += info->history[j].size;
		nb_est_day[nb_day]++;
	    } else {
		nb_day=0;
	    }
	}
	nb_day = info->consecutive_runs + 1;
	if (nb_day > NB_DAY-1) nb_day = NB_DAY-1;

	while (nb_day > 0 && nb_est_day[nb_day] == 0) nb_day--;

	if (nb_est_day[nb_day] > 0) {
	    size = est_size_day[nb_day] / (gint64)nb_est_day[nb_day];
	    *stats = 1;
	}
	else if (info->inf[level].size > (gint64)1000) { /* stats */
	    size = info->inf[level].size;
	    *stats = 1;
	}
	else {
	    int level0_stat;
	    gint64 level0_size;

            level0_size = internal_server_estimate(dp, info, 0, &level0_stat,
						   tapetype);
	    size = (gint64)10000;
	    if (size > tapetype_get_length(tapetype)/2)
		size = tapetype_get_length(tapetype)/2;
	    if (level0_size > 0 && dp->strategy != DS_NOFULL) {
		if (size > level0_size/2)
		    size = level0_size/2;
	    }
	    *stats = 0;
	}
    }
    else if (level == info->last_level + 1) {
	/* means of all first day at a new level */
	gint64 est_size = (gint64)0;
	int nb_est = 0;

	for (j=NB_HISTORY-2; j>=0; j--) {
	    if (info->history[j].level <= 0) continue;
	    if (info->history[j].size <= (gint64)0) continue;
	    if (info->history[j].level == info->history[j+1].level + 1 ) {
		est_size += info->history[j].size;
		nb_est++;
	    }
	}
	if (nb_est > 0) {
	    size = est_size / (gint64)nb_est;
	    *stats = 1;
	} else if (info->inf[level].size > (gint64)1000) { /* stats */
	    size = info->inf[level].size;
	    *stats = 1;
	} else {
	    int level0_stat;
	    gint64 level0_size;

            level0_size = internal_server_estimate(dp, info, 0, &level0_stat,
						   tapetype);
	    size = (gint64)100000;
	    if (size > tapetype_get_length(tapetype)/2)
		size = tapetype_get_length(tapetype)/2;
	    if (level0_size > 0 && dp->strategy != DS_NOFULL) {
		if (size > level0_size/2)
		    size = level0_size/2;
	    }
	    *stats = 0;
	}
    } else {
	size = (gint64)100000;
	if (size > tapetype_get_length(tapetype)/2)
	    size = tapetype_get_length(tapetype)/2;
    }

    return size;
}

int
server_can_do_estimate(
    disk_t *dp,
    info_t *info,
    int     level,
    tapetype_t *tapetype)
{
    int     stats;

    internal_server_estimate(dp, info, level, &stats, tapetype);
    return stats;
}

