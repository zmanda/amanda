/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
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
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */
/*
 * $Id: clientconf.h,v 1.8 2006/07/25 18:35:21 martinea Exp $
 *
 * interface for client config file reading code
 */
#ifndef CLIENTCONF_H
#define CLIENTCONF_H

#include "sl.h"

#define CLIENTCONFFILE_NAME "client.conf"

typedef enum conf_e {
    CLN_CONF,
    CLN_INDEX_SERVER,
    CLN_TAPE_SERVER,
    CLN_TAPEDEV,
    CLN_AUTH,
    CLN_SSH_KEYS,
    CLN_AMANDAD_PATH,
    CLN_CLIENT_USERNAME,
    CLN_GNUTAR_LIST_DIR,
    CLN_AMANDATES,
    CLN_CLN
} cconfparm_t;

extern char *config_name;
extern char *config_dir;

void parse_client_conf(int, char **, int *, char ***);
int  add_client_conf(cconfparm_t parm, char *value);
void report_bad_client_arg(void);
int read_clientconf(char *filename);
int client_getconf_seen(cconfparm_t parameter);
int client_getconf_int(cconfparm_t parameter);
off_t client_getconf_am64(cconfparm_t parameter);
double client_getconf_real(cconfparm_t parameter);
char *client_getconf_str(cconfparm_t parameter);
char *client_getconf_byname(char *confname);

/* this is in securityconf.h */
char *generic_client_get_security_conf(char *, void *);
#endif /* ! CONFFILE_H */
