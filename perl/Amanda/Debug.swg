/*
 * Copyright (c) Zmanda, Inc.  All Rights Reserved.
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 *
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

%module "Amanda::Debug"
%include "amglue/exports.swg"
%include "amglue/constants.swg"

%{
#include <glib.h>
#include "debug.h"
%}

amglue_export_tag(logging,
    g_error g_critical g_warning g_message g_info g_debug
);

amglue_export_ok(
    dbopen dbreopen dbrename dbclose
    set_pname
);

/* TODO: typemap these so that they come out as g_foo("%s", $1); */

/* These are really macros, but SWIG will get along just fine */
void g_error(char *);
void g_critical(char *);
void g_warning(char *);
void g_message(char *);
void g_info(char *);
void g_debug(char *);

amglue_add_flag_tag_fns(erroutput_type_t);
amglue_add_constant_short(ERR_INTERACTIVE, INTERACTIVE, erroutput_type_t);
amglue_add_constant_short(ERR_SYSLOG, SYSLOG, erroutput_type_t);
amglue_add_constant_short(ERR_AMANDALOG, AMANDALOG, erroutput_type_t);

erroutput_type_t erroutput_type;

int error_exit_status;

void	dbopen(char *subdir); /* TODO alloc */
void	dbreopen(char *file, char *notation); /* TODO alloc */
void	dbrename(char *config, char *subdir); /* TODO alloc */
void	dbclose(void);
int	dbfd(void);
char *	dbfn(void); /* TODO alloc? */
void debug_dup_stderr_to_debug(void);

void set_pname(char *pname); /* TODO alloc? */
char *get_pname(void);

