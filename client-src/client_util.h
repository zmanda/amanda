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
#include "sl.h"
#include "util.h"		/* for bstrncmp() */
#include "amandad.h"		/* for g_option_t */

typedef struct option_s {
    char *str;
    int compress;
    int encrypt;
    char *srvcompprog;
    char *clntcompprog;
    char *srv_encrypt;
    char *clnt_encrypt;
    char *srv_decrypt_opt;
    char *clnt_decrypt_opt;
    int no_record;
    int createindex;
    char *auth;
    sl_t *exclude_file;
    sl_t *exclude_list;
    sl_t *include_file;
    sl_t *include_list;
    int exclude_optional;
    int include_optional;
} option_t;

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
    int include_optional;
    int exclude_file;
    int exclude_list;
    int exclude_optional;
    int collection;
} backup_support_option_t;

char *build_exclude(char *disk, char *device, option_t *options, int verbose);
char *build_include(char *disk, char *device, option_t *options, int verbose);
void init_options(option_t *options);
option_t *parse_options(char *str,
			   char *disk,
			   char *device,
			   am_feature_t *features,
			   int verbose);
void output_tool_property(FILE *tool, option_t *options);
char *fixup_relative(char *name, char *device);
backup_support_option_t *backup_support_option(char *program,
					       g_option_t *g_options,
					       char *disk,
					       char *amdevice);

#endif
