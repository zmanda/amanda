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

#ifndef BACKUP_SUPPORT_OPTION_H
#define BACKUP_SUPPORT_OPTION_H

typedef enum {
    RECOVER_PATH_CWD    = 0,
    RECOVER_PATH_REMOTE = 1,
} recover_path_t;

typedef struct backup_support_option_s {
    int config;
    int host;
    int disk;
    int max_level;
    int index_line;
    int index_xml;
    int message_line;
    int message_selfcheck_json;
    int message_estimate_json;
    int message_backup_json;
    int message_restore_json;
    int message_validate_json;
    int message_index_json;
    int message_xml;
    int record;
    int include_file;
    int include_list;
    int include_list_glob;
    int include_optional;
    int exclude_file;
    int exclude_list;
    int exclude_list_glob;
    int exclude_optional;
    int collection;
    int calcsize;
    int client_estimate;
    int multi_estimate;
    int smb_recover_mode;
    int features;
    gboolean dar;
    int state_stream;
    int timestamp;
    data_path_t data_path_set;  /* bitfield of all allowed data-path */
    recover_path_t recover_path;
    int recover_dump_state_file;
    int discover;
    int execute_where;
    int cmd_stream;
    int want_server_backup_result;
} backup_support_option_t;

backup_support_option_t *backup_support_option(char       *program,
					       GPtrArray **errarray);

#endif
