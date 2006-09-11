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
#include "amfeatures.h"
#include "sl.h"
#include "util.h"		/* for bstrncmp() */

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

#define NO_COMPR   0
#define COMPR_FAST 1
#define COMPR_BEST 2
#define COMPR_SERVER_FAST 3
#define COMPR_SERVER_BEST 4
#define COMPR_SERVER_CUST 5	/* server-side custom compression */
#define COMPR_CUST 6            /* client-side custom compression */

#define ENCRYPT_NONE         0	/* no encryption  */
#define ENCRYPT_CUST         1	/* client-side custom encryption */
#define ENCRYPT_SERV_CUST    2	/* server-side custom encryption */

char *build_exclude(char *disk, char *device, option_t *options, int verbose);
char *build_include(char *disk, char *device, option_t *options, int verbose);
void init_options(option_t *options);
option_t *parse_options(char *str,
			   char *disk,
			   char *device,
			   am_feature_t *features,
			   int verbose);

char *fixup_relative(char *name, char *device);

#endif
