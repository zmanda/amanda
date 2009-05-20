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
 * $Id: changer.c,v 1.36 2006/08/24 01:57:16 paddy_s Exp $
 *
 * interface routines for tape changers
 */
#include "amanda.h"
#include "util.h"
#include "conffile.h"
#include "version.h"

#include "changer.h"

/*
 * If we don't have the new-style wait access functions, use our own,
 * compatible with old-style BSD systems at least.  Note that we don't
 * care about the case w_stopval == WSTOPPED since we don't ask to see
 * stopped processes, so should never get them from wait.
 */
#ifndef WEXITSTATUS
#   define WEXITSTATUS(r)       (((union wait *) &(r))->w_retcode)
#   define WTERMSIG(r)          (((union wait *) &(r))->w_termsig)

#   undef  WIFSIGNALED
#   define WIFSIGNALED(r)       (((union wait *) &(r))->w_termsig != 0)
#endif


int changer_debug = 0;
char *changer_resultstr = NULL;

static pid_t tpchanger_pid = -1;
static int tpchanger_stdout = -1;
static int tpchanger_stdin = -1;

/* local functions */
static int changer_command(char *cmd, char *arg);
static int report_bad_resultstr(char *cmd);
static int run_changer_command(char *cmd, char *arg, char **slotstr, char **rest);

int
changer_init(void)
{
    return strcmp(getconf_str(CNF_TPCHANGER), "") != 0;
}


static int
report_bad_resultstr(char *cmd)
{
    char *s;

    s = vstrallocf(_("<error> badly formed result from changer command %s: \"%s\""),
		  cmd, changer_resultstr);
    amfree(changer_resultstr);
    changer_resultstr = s;
    return 2;
}

static int
run_changer_command(
    char *	cmd,
    char *	arg,
    char **	slotstr,
    char **	rest)
{
    int exitcode;
    char *result_copy;
    char *slot;
    char *s;
    int ch;

    if (slotstr) {
        *slotstr = NULL;
    }
    if (rest) {
        *rest = NULL;
    }
    exitcode = changer_command(cmd, arg);
    s = changer_resultstr;
    ch = *s++;

    skip_whitespace(s, ch);
    if(ch == '\0') return report_bad_resultstr(cmd);
    slot = s - 1;
    skip_non_whitespace(s, ch);
    s[-1] = '\0';
    if (slotstr) {
        *slotstr = newstralloc(*slotstr, slot);
    }
    s[-1] = (char)ch;

    skip_whitespace(s, ch);
    if (rest) {
        *rest = s - 1;
    }

    if(exitcode) {
	if(ch == '\0') return report_bad_resultstr(cmd);
	result_copy = stralloc(s - 1);
	amfree(changer_resultstr);
	changer_resultstr = result_copy;
	return exitcode;
    }
    return 0;
}

int
changer_reset(
    char **	slotstr)
{
    char *rest;

    return run_changer_command("-reset", (char *) NULL, slotstr, &rest);
}

int
changer_clean(
    char **	slotstr)
{
    char *rest;

    return run_changer_command("-clean", (char *) NULL, slotstr, &rest);
}

int
changer_eject(
    char **	slotstr)
{
    char *rest;

    return run_changer_command("-eject", (char *) NULL, slotstr, &rest);
}

int
changer_loadslot(
    char *inslotstr,
    char **outslotstr,
    char **devicename)
{
    char *rest;
    int rc;

    rc = run_changer_command("-slot", inslotstr, outslotstr, &rest);

    if(rc) return rc;
    if(*rest == '\0') return report_bad_resultstr("-slot");

    *devicename = newstralloc(*devicename, rest);
    return 0;
}


/*
 * This function is somewhat equal to changer_info with one additional
 * parameter, to get information, if the changer is able to search for
 * tapelabels himself. E.g. Barcodereader
 * The changer_script answers with an additional parameter, if it is able
 * to search. This one should be 1, if it is able to search, and 0 if it
 * knows about the extension. If the additional answer is omitted, the
 * changer is not able to search for a tape. 
 */

int
changer_query(
    int *	nslotsp,
    char **	curslotstr,
    int *	backwardsp,
    int *	searchable)
{
    char *rest;
    int rc;

    rc = run_changer_command("-info", (char *) NULL, curslotstr, &rest);
    if(rc) return rc;

    dbprintf(_("changer_query: changer return was %s\n"),rest);
    if (sscanf(rest, "%d %d %d", nslotsp, backwardsp, searchable) != 3) {
      if (sscanf(rest, "%d %d", nslotsp, backwardsp) != 2) {
        return report_bad_resultstr("-info");
      } else {
        *searchable = 0;
      }
    }
    dbprintf(_("changer_query: searchable = %d\n"),*searchable);
    return 0;
}

int
changer_info(
    int *	nslotsp,
    char **	curslotstr,
    int *	backwardsp)
{
    char *rest;
    int rc;

    rc = run_changer_command("-info", (char *) NULL, curslotstr, &rest);
    if(rc) return rc;

    if (sscanf(rest, "%d %d", nslotsp, backwardsp) != 2) {
	return report_bad_resultstr("-info");
    }
    return 0;
}


/* ---------------------------- */

/*
 * This function first uses searchlabel and changer_search, if
 * the library is able to find a tape itself. If it is not, or if 
 * the tape could not be found, then the normal scan is done.
 *
 * See interface documentation in changer.h.
 */

void
changer_find(
     void *	user_data,
     int	(*user_init)(void *, int, int, int, int),
     int	(*user_slot)(void *, int, char *, char *),
     char *	searchlabel)
{
    char *slotstr, *device = NULL, *curslotstr = NULL;
    int nslots, checked, backwards, rc, done, searchable;

    rc = changer_query(&nslots, &curslotstr, &backwards, &searchable);

    if (rc != 0) {
        /* Problem with the changer script. Bail. */
        g_fprintf(stderr, _("Changer problem: %s\n"), changer_resultstr);
        amfree(curslotstr);
        return;
    }

    done = user_init(user_data, rc, nslots, backwards, searchable);
    amfree(curslotstr);
   
    if (searchlabel != NULL)
    {
      dbprintf(_("changer_find: looking for %s changer is searchable = %d\n"),
		searchlabel, searchable);
    } else {
      dbprintf(_("changer_find: looking for NULL changer is searchable = %d\n"),
		searchable);
    }

    if ((searchlabel!=NULL) && searchable && !done){
      rc=changer_search(searchlabel, &curslotstr, &device);
      if(rc == 0)
        done = user_slot(user_data, rc, curslotstr, device);
    }
 
    slotstr = "current";
    checked = 0;

    while(!done && checked < nslots) {
	rc = changer_loadslot(slotstr, &curslotstr, &device);
	if(rc > 0)
	    done = user_slot(user_data, rc, curslotstr, device);
	else if(!done)
	    done = user_slot(user_data, 0,  curslotstr, device);
	amfree(curslotstr);
	amfree(device);

	checked += 1;
	slotstr = "next";
    }
}

/* ---------------------------- */

void
changer_current(
    void *	user_data,
    int		(*user_init)(void *, int, int, int, int),
    int		(*user_slot)(void *, int, char *, char *))
{
    char *device = NULL, *curslotstr = NULL;
    int nslots, backwards, rc, done, searchable;

    rc = changer_query(&nslots, &curslotstr, &backwards, &searchable);
    done = user_init(user_data, rc, nslots, backwards, searchable);
    amfree(curslotstr);

    rc = changer_loadslot("current", &curslotstr, &device);
    if(rc > 0) {
	done = user_slot(user_data, rc, curslotstr, device);
    } else if(!done) {
	done = user_slot(user_data, 0,  curslotstr, device);
    }
    amfree(curslotstr);
    amfree(device);
}

/* ---------------------------- */

static int
start_chg_glue(void)
{
    int stdin_pipe[2] = { -1, -1 };
    int stdout_pipe[2] = { -1, -1 };
    char *chg_glue = NULL;
    char **config_options = NULL;
    char *cmdline = NULL;

    /* is it already running? */
    if (tpchanger_pid != -1)
	return 1;

    if (pipe(stdin_pipe) == -1 || pipe(stdout_pipe) == -1) {
	changer_resultstr = vstrallocf(
			_("<error> could not make pipe: %s"), strerror(errno));
	goto error;
    }

    config_options = get_config_options(2);
    config_options[0] = g_strdup("chg-glue");
    config_options[1] = g_strdup(get_config_name());
    chg_glue = g_strdup_printf("%s/chg-glue", amlibexecdir);

    cmdline = g_strjoinv(" ", config_options);
    g_debug("invoking %s: %s", chg_glue, cmdline);
    amfree(cmdline);

    switch(tpchanger_pid = fork()) {
    case -1:
	changer_resultstr = vstrallocf(
			_("<error> could not fork: %s"), strerror(errno));
	goto error;

    case 0:
	debug_dup_stderr_to_debug();
	if(dup2(stdin_pipe[0], 0) == -1) {
	    changer_resultstr = vstrallocf(
			_("<error> could not dup2: %s\n"), strerror(errno));
	    goto child_err;
	}

	if(dup2(stdout_pipe[1], 1) == -1) {
	    changer_resultstr = vstrallocf(
			_("<error> could not dup2: %s\n"), strerror(errno));
	    goto child_err;
	}

	safe_fd(-1, 0);
	execv(chg_glue, config_options);
	changer_resultstr = vstrallocf(
			_("<error> could not exec \"%s\": %s\n"), chg_glue, strerror(errno));

child_err:
	(void)full_write(stdout_pipe[1], changer_resultstr, strlen(changer_resultstr));
	exit(1);

    default:
	aclose(stdin_pipe[0]);
	aclose(stdout_pipe[1]);

	g_strfreev(config_options);
	amfree(chg_glue);
	amfree(cmdline);

	tpchanger_stdout = stdout_pipe[0];
	tpchanger_stdin = stdin_pipe[1];

	return 1;
    }

error:
    amfree(chg_glue);
    amfree(cmdline);
    aclose(stdin_pipe[0]);
    aclose(stdin_pipe[1]);
    aclose(stdout_pipe[0]);
    aclose(stdout_pipe[1]);

    return 0;
}

static int
changer_command(
     char *cmd,
     char *arg)
{
    int exitcode = 0;
    char *cmdstr = NULL;

    amfree(changer_resultstr);

    if (!start_chg_glue()) {
	exitcode = 2;
	goto failed;
    }

    cmdstr = vstralloc(cmd,
		       arg ? " " : "",
		       arg ? arg : "",
		       "\n",
		       NULL);

    g_debug("changer: >> %s %s", cmd, arg? arg : "");

    /* write the command to chg_glue */
    if (full_write(tpchanger_stdin, cmdstr, strlen(cmdstr)) != strlen(cmdstr)) {
        changer_resultstr = g_strdup("<error> chg-glue exited unexpectedly");
	exitcode = 2;
	goto failed;
    }

    /* read the first line of the response */
    changer_resultstr = areads(tpchanger_stdout);
    if (!changer_resultstr || !*changer_resultstr) {
        changer_resultstr = g_strdup("<error> unexpected EOF");
        exitcode = 2;
        goto failed;
    }
    g_debug("changer: << %s", changer_resultstr);

    if (strncmp_const(changer_resultstr, "EXITSTATUS ") != 0) {
	report_bad_resultstr(cmd);
	exitcode = 2;
	goto failed;
    }
    exitcode = atoi(changer_resultstr + strlen("EXITSTATUS "));

    /* and the second */
    changer_resultstr = areads(tpchanger_stdout);
    if (!changer_resultstr) {
        changer_resultstr = g_strdup("<error> unexpected EOF");
        exitcode = 2;
        goto failed;
    }
    g_debug("changer: << %s", changer_resultstr);

failed:
    if (exitcode != 0) {
	g_debug("changer: ERROR %s", changer_resultstr);
    }

    amfree(cmdstr);

    return exitcode;
}


/*
 * This function commands the changerscript to look for a tape named
 * searchlabel. If is found, the changerscript answers with the device,
 * in which the tape can be accessed.
 */

int
changer_search(
    char *	searchlabel,
    char **	outslotstr,
    char **	devicename)
{
    char *rest;
    int rc;

    dbprintf("changer_search: %s\n",searchlabel);
    rc = run_changer_command("-search", searchlabel, outslotstr, &rest);
    if(rc) return rc;

    if(*rest == '\0') return report_bad_resultstr("-search");

    *devicename = newstralloc(*devicename, rest);
    return 0;
}


/*
 * Because barcodelabel are short, and may not be the same as the 
 * amandalabels, the changerscript should be informed, which tapelabel
 * is associated with a tape. This function should be called after 
 * giving a label for a tape. (Maybe also, when the label and the associated
 * slot is known. e.g. during library scan.
 */

int
changer_label(
    char *	slotsp, 
    char *	labelstr)
{
    int rc;
    char *rest=NULL;
    char *slotstr;
    char *curslotstr = NULL;
    int nslots, backwards, searchable;

    dbprintf(_("changer_label: %s for slot %s\n"),labelstr,slotsp);
    rc = changer_query(&nslots, &curslotstr, &backwards,&searchable);
    amfree(curslotstr);

    if ((rc == 0) && (searchable == 1)){
	dbprintf(_("changer_label: calling changer -label %s\n"),labelstr);
	rc = run_changer_command("-label", labelstr, &slotstr, &rest);
	amfree(slotstr);
    }

    if(rc) return rc;

    return 0;
}
