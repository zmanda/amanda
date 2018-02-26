/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2017 Carbonite, Inc.  All Rights Reserved.
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

#include "amanda.h"
#include "conffile.h"
#include "pipespawn.h"
#include "backup_support_option.h"

backup_support_option_t *
backup_support_option(
    char       *program,
    GPtrArray **errarray)
{
    pid_t   supportpid;
    int     supportin, supportout, supporterr;
    char   *cmd;
    GPtrArray *argv_ptr = g_ptr_array_new();
    FILE   *streamout;
    FILE   *streamerr;
    char   *line;
    int     status;
    char   *err = NULL;
    backup_support_option_t *bsu;

    if (errarray)
	*errarray = NULL;
    cmd = g_strjoin(NULL, APPLICATION_DIR, "/", program, NULL);
    g_ptr_array_add(argv_ptr, g_strdup(program));
    g_ptr_array_add(argv_ptr, g_strdup("support"));
    g_ptr_array_add(argv_ptr, NULL);

    supporterr = fileno(stderr);
    supportpid = pipespawnv(cmd, STDIN_PIPE|STDOUT_PIPE|STDERR_PIPE, 0,
			    &supportin, &supportout, &supporterr,
			    (char **)argv_ptr->pdata);

    aclose(supportin);

    bsu = g_new0(backup_support_option_t, 1);
    bsu->config = 1;
    bsu->host = 1;
    bsu->disk = 1;
    streamout = fdopen(supportout, "r");
    if (!streamout) {
	error(_("Error opening pipe to child: %s"), strerror(errno));
	/* NOTREACHED */
    }
    while((line = pgets(streamout)) != NULL) {
	dbprintf(_("support line: %s\n"), line);
	if (g_str_has_prefix(line, "CONFIG ")) {
	    if (g_str_equal(line + 7, "YES"))
		bsu->config = 1;
	} else if (g_str_has_prefix(line, "HOST ")) {
	    if (g_str_equal(line + 5, "YES"))
	    bsu->host = 1;
	} else if (g_str_has_prefix(line, "DISK ")) {
	    if (g_str_equal(line + 5, "YES"))
		bsu->disk = 1;
	} else if (g_str_has_prefix(line, "INDEX-LINE ")) {
	    if (g_str_equal(line + 11, "YES"))
		bsu->index_line = 1;
	} else if (g_str_has_prefix(line, "INDEX-XML ")) {
	    if (g_str_equal(line + 10, "YES"))
		bsu->index_xml = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-LINE ")) {
	    if (g_str_equal(line + 13, "YES"))
		bsu->message_line = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-SELFCHECK-JSON ")) {
	    if (g_str_equal(line + 23, "YES"))
		bsu->message_selfcheck_json = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-ESTIMATE-JSON ")) {
	    if (g_str_equal(line + 22, "YES"))
		bsu->message_estimate_json = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-BACKUP-JSON ")) {
	    if (g_str_equal(line + 20, "YES"))
		bsu->message_backup_json = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-RESTORE-JSON ")) {
	    if (g_str_equal(line + 21, "YES"))
		bsu->message_restore_json = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-VALIDATE-JSON ")) {
	    if (g_str_equal(line + 22, "YES"))
		bsu->message_validate_json = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-INDEX-JSON ")) {
	    if (g_str_equal(line + 19, "YES"))
		bsu->message_index_json = 1;
	} else if (g_str_has_prefix(line, "MESSAGE-XML ")) {
	    if (g_str_equal(line + 12, "YES"))
		bsu->message_xml = 1;
	} else if (g_str_has_prefix(line, "RECORD ")) {
	    if (g_str_equal(line + 7, "YES"))
		bsu->record = 1;
	} else if (g_str_has_prefix(line, "INCLUDE-FILE ")) {
	    if (g_str_equal(line + 13, "YES"))
		bsu->include_file = 1;
	} else if (g_str_has_prefix(line, "INCLUDE-LIST ")) {
	    if (g_str_equal(line + 13, "YES"))
		bsu->include_list = 1;
	} else if (g_str_has_prefix(line, "INCLUDE-LIST-GLOB ")) {
	    if (g_str_equal(line + 17, "YES"))
		bsu->include_list_glob = 1;
	} else if (g_str_has_prefix(line, "INCLUDE-OPTIONAL ")) {
	    if (g_str_equal(line + 17, "YES"))
		bsu->include_optional = 1;
	} else if (g_str_has_prefix(line, "EXCLUDE-FILE ")) {
	    if (g_str_equal(line + 13, "YES"))
		bsu->exclude_file = 1;
	} else if (g_str_has_prefix(line, "EXCLUDE-LIST ")) {
	    if (g_str_equal(line + 13, "YES"))
		bsu->exclude_list = 1;
	} else if (g_str_has_prefix(line, "EXCLUDE-LIST-GLOB ")) {
	    if (g_str_equal(line + 17, "YES"))
		bsu->exclude_list_glob = 1;
	} else if (g_str_has_prefix(line, "EXCLUDE-OPTIONAL ")) {
	    if (g_str_equal(line + 17, "YES"))
		bsu->exclude_optional = 1;
	} else if (g_str_has_prefix(line, "COLLECTION ")) {
	    if (g_str_equal(line + 11, "YES"))
		bsu->collection = 1;
	} else if (g_str_has_prefix(line, "CALCSIZE ")) {
	    if (g_str_equal(line + 9, "YES"))
		bsu->calcsize = 1;
	} else if (g_str_has_prefix(line, "CLIENT-ESTIMATE ")) {
	    if (g_str_equal(line + 16, "YES"))
		bsu->client_estimate = 1;
	} else if (g_str_has_prefix(line, "MULTI-ESTIMATE ")) {
	    if (g_str_equal(line + 15, "YES"))
		bsu->multi_estimate = 1;
	} else if (g_str_has_prefix(line, "MAX-LEVEL ")) {
	    bsu->max_level  = atoi(line+10);
	} else if (g_str_has_prefix(line, "RECOVER-MODE ")) {
	    if (strcasecmp(line+13, "SMB") == 0)
		bsu->smb_recover_mode = 1;
	} else if (g_str_has_prefix(line, "DATA-PATH ")) {
	    if (strcasecmp(line+10, "AMANDA") == 0)
		bsu->data_path_set |= DATA_PATH_AMANDA;
	    else if (strcasecmp(line+10, "DIRECTTCP") == 0)
		bsu->data_path_set |= DATA_PATH_DIRECTTCP;
	} else if (g_str_has_prefix(line, "RECOVER-PATH ")) {
	    if (strcasecmp(line+13, "CWD") == 0)
		bsu->recover_path = RECOVER_PATH_CWD;
	    else if (strcasecmp(line+13, "REMOTE") == 0)
		bsu->recover_path = RECOVER_PATH_REMOTE;
	} else if (g_str_has_prefix(line, "AMFEATURES ")) {
	    if (g_str_equal(line + 11, "YES"))
		bsu->features = 1;
	} else if (g_str_has_prefix(line, "RECOVER-DUMP-STATE-FILE ")) {
	    if (g_str_equal(line + 24, "YES"))
		bsu->recover_dump_state_file = 1;
	} else if (g_str_has_prefix(line, "DISCOVER ")) {
	    if (g_str_equal(line + 9, "YES"))
		bsu->discover = 1;
	} else if (g_str_has_prefix(line, "DAR ")) {
	    if (g_str_equal(line + 4, "YES"))
		bsu->dar = 1;
	} else if (g_str_has_prefix(line, "STATE-STREAM ")) {
	    if (g_str_equal(line + 13, "YES"))
		bsu->state_stream = 1;
	} else if (g_str_has_prefix(line, "TIMESTAMP ")) {
	    if (g_str_equal(line + 10, "YES"))
		bsu->timestamp = 1;
	} else if (g_str_has_prefix(line, "EXECUTE-WHERE ")) {
	    if (g_str_equal(line + 14, "YES"))
		bsu->execute_where = 1;
	} else if (g_str_has_prefix(line, "CMD-STREAM ")) {
	    if (g_str_equal(line + 11, "YES"))
		bsu->cmd_stream = 1;
	} else if (g_str_has_prefix(line, "WANT-SERVER-BACKUP-RESULT ")) {
	    if (g_str_equal(line + 26, "YES"))
		bsu->want_server_backup_result = 1;
	} else {
	    dbprintf(_("Invalid support line: %s\n"), line);
	}
	amfree(line);
    }
    fclose(streamout);

    if (bsu->data_path_set == 0)
	bsu->data_path_set = DATA_PATH_AMANDA;

    streamerr = fdopen(supporterr, "r");
    if (!streamerr) {
	error(_("Error opening pipe to child: %s"), strerror(errno));
	/* NOTREACHED */
    }
    while((line = pgets(streamerr)) != NULL) {
	if (strlen(line) > 0) {
	    if (errarray) {
		if (!*errarray)
		    *errarray = g_ptr_array_new();
		g_ptr_array_add(*errarray, g_strdup(line));
	    }
	    dbprintf("Application '%s': %s\n", program, line);
	}
	amfree(bsu);
	amfree(line);
    }
    fclose(streamerr);

    if (waitpid(supportpid, &status, 0) < 0) {
	err = g_strdup_printf(_("waitpid failed: %s"), strerror(errno));
    } else if (!WIFEXITED(status)) {
	err = g_strdup_printf(_("exited with signal %d"), WTERMSIG(status));
    } else if (WEXITSTATUS(status) != 0) {
	err = g_strdup_printf(_("exited with status %d"), WEXITSTATUS(status));
    }

    if (err) {
	if (errarray) {
	    if (!*errarray)
		*errarray = g_ptr_array_new();
	    g_ptr_array_add(*errarray, err);
	}
	dbprintf("Application '%s': %s\n", program, err);
	amfree(bsu);
    }
    g_ptr_array_free_full(argv_ptr);
    amfree(cmd);
    return bsu;
}

