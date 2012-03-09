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
 * $Id: client_util.h,v 1.14 2006/05/25 01:47:11 johnfranks Exp $
 *
 */

#ifndef CLIENT_UTIL_H
#define CLIENT_UTIL_H

#include "amanda.h"
#include "conffile.h"
#include "amfeatures.h"
#include "am_sl.h"
#include "util.h"		/* for bstrncmp() */
#include "amandad.h"		/* for g_option_t */
#include "amxml.h"		/* for dle_t	  */

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
    data_path_t data_path_set;  /* bitfield of all allowed data-path */
    recover_path_t recover_path;
} backup_support_option_t;

typedef struct client_script_result_s {
    int exit_code;
    proplist_t proplist;
    GPtrArray *output;
    GPtrArray *err;
} client_script_result_t;

typedef enum {
    DMP_NORMAL, DMP_IGNORE, DMP_STRANGE, DMP_SIZE, DMP_ERROR
} dmpline_t;

typedef struct regex_s {
    char *regex;
    int srcline;
    int scale;			/* only used for size lines */
    int field;
    dmpline_t typ;
} amregex_t;

#define AM_NORMAL_RE(re)	{(re), __LINE__, 0, 0, DMP_NORMAL}
#define AM_IGNORE_RE(re)	{(re), __LINE__, 0, 0, DMP_IGNORE}
#define AM_STRANGE_RE(re)	{(re), __LINE__, 0, 0, DMP_STRANGE}
#define AM_SIZE_RE(re,s,f)	{(re), __LINE__, (s), (f), DMP_SIZE}
#define AM_ERROR_RE(re)		{(re), __LINE__, 0, 0, DMP_ERROR}

char *build_exclude(dle_t *dle, int verbose);
char *build_include(dle_t *dle, int verbose);
void parse_options(char *str,
		   dle_t *dle,
		   am_feature_t *features,
		   int verbose);

/* Add all properties of an application for a dle to an ARGV.
 * include/exclude options are converted to properties.
 *
 * @param argv_ptr: the ARGV where to store properties.
 * @param dle: the dle.
 * @returns: Number of argument added to ARGV.
 */
void application_property_add_to_argv(GPtrArray *argv_ptr,
				      dle_t *dle,
				      backup_support_option_t *bsu,
				      am_feature_t *amfeatures);

/* Merge properties from amanda-client.conf files to dles (application and scripts)
 *
 * @param dle: the dle list.
 * @returns: Return 1 on success
 *           Return 0 on failure
 */
int merge_dles_properties(dle_t *dles, int verbose);

char *fixup_relative(char *name, char *device);
backup_support_option_t *backup_support_option(char *program,
					       g_option_t *g_options,
					       char *disk,
					       char *amdevice,
					       GPtrArray **errarray);

void run_client_script(script_t     *script,
		       execute_on_t  execute_on,
		       g_option_t   *g_options,
		       dle_t        *dle);

void run_client_scripts(execute_on_t  execute_on,
			g_option_t   *g_options,
			dle_t        *dle,
			FILE         *streamout);

void run_calcsize(char *config, char *program, char *disk,
                  char *dirname, GSList *levels,
                  char *file_exclude, char *file_include);

gboolean check_access(char *filename, int mode);
gboolean check_file(char *filename, int mode);
gboolean check_dir(char *dirname, int mode);
gboolean check_suid(char *filename);
double the_num(char * str, int pos);

/* Convert a GSList returned from config_errors into an "ERROR "
 * line suitable for inclusion in a NAK or REP packet.  Because we only
 * get one ERROR line per packet, this includes only the first 
 * error, with an indication that there are more to follow.
 *
 * @param errlist: the list of errors from config_errors
 * @returns: newly allocated string containing the error messages
 */
char *config_errors_to_error_string(GSList *errlist);

amregex_t *build_re_table(amregex_t *orig_re_table,
                          GSList *normal_message,
                          GSList *ignore_message,
                          GSList *strange_message);
void add_type_table(dmpline_t typ,
                    amregex_t **re_table, amregex_t *orig_re_table,
                    GSList *normal_message, GSList *ignore_message,
                    GSList *strange_message);
void add_list_table(dmpline_t typ, amregex_t **re_table,
                    GSList *message);

/* Merge properties from conf_proplist to dle_proplist
   If verbose is 1, then dle->disk and name are used in output.
 * @returns: Return 1 on success
 *           Return 0 on failure
 */
int
merge_properties(dle_t *dle, char *name, proplist_t dle_proplist,
		 proplist_t conf_proplist, int verbose);

#endif

