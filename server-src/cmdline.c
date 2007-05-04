/*
 * Copyright (c) 2005 Zmanda Inc.  All Rights Reserved.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 * 
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 * 
 * Contact information: Zmanda Inc, 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */
/*
 * $Id$
 *
 * Utility routines for handling command lines.
 */

#include <ctype.h>
#include "amanda.h"
#include "cmdline.h"
#include "holding.h"

dumpspec_t *
dumpspec_new(
    char *host, 
    char *disk, 
    char *datestamp)
{
    dumpspec_t *rv;

    rv = calloc(1, sizeof(*rv));
    if (!rv) return NULL;
    if (host) rv->host = stralloc(host);
    if (disk) rv->disk = stralloc(disk);
    if (datestamp) rv->datestamp = stralloc(datestamp);

    return rv;
}

void
dumpspec_free(
    dumpspec_t *dumpspec)
{
    if (!dumpspec) return;
    if (dumpspec->host) free(dumpspec->host);
    if (dumpspec->disk) free(dumpspec->disk);
    if (dumpspec->datestamp) free(dumpspec->datestamp);
    free(dumpspec);
}

void
dumpspec_free_list(
    dumpspec_list_t *dumpspec_list)
{
    dumpspec_t *dumpspec = (dumpspec_t *)dumpspec_list;
    dumpspec_t *next;

    while (dumpspec) {
        next = dumpspec->next;
        dumpspec_free(dumpspec);
        dumpspec = next;
    }
}

dumpspec_list_t *
cmdline_parse_dumpspecs(
    int argc,
    char **argv)
{
    dumpspec_t *dumpspec = NULL, *t;
    char *errstr;
    char *name;
    int optind = 0;
    enum { ARG_GET_HOST, ARG_GET_DISK, ARG_GET_DATE } arg_state = ARG_GET_HOST;

    while (optind < argc) {
        name = argv[optind++];
        switch (arg_state) {
            case ARG_GET_HOST:
                if (name[0] != '\0'
                    && (errstr=validate_regexp(name)) != NULL) {
                    fprintf(stderr, _("%s: bad hostname regex \"%s\": %s\n"),
		                    get_pname(), name, errstr);
                    goto error;
                }
                t = dumpspec_new(name, NULL, NULL);
                t->next = (dumpspec_t *)dumpspec;
                dumpspec = t;
                arg_state = ARG_GET_DISK;
                break;

            case ARG_GET_DISK:
                if (name[0] != '\0'
                    && (errstr=validate_regexp(name)) != NULL) {
                    fprintf(stderr, _("%s: bad diskname regex \"%s\": %s\n"),
		                    get_pname(), name, errstr);
                    goto error;
                }
                dumpspec->disk = stralloc(name);
                arg_state = ARG_GET_DATE;
                break;

            case ARG_GET_DATE:
                if (name[0] != '\0'
                    && (errstr=validate_regexp(name)) != NULL) {
                    fprintf(stderr, _("%s: bad datestamp regex \"%s\": %s\n"),
		                    get_pname(), name, errstr);
                    goto error;
                }
                dumpspec->datestamp = stralloc(name);
                arg_state = ARG_GET_HOST;
                break;
        }
    }

    if (dumpspec == NULL) 
        dumpspec = dumpspec_new("", "", "");
    return (dumpspec_list_t *)dumpspec;

error:
    dumpspec_free_list((dumpspec_list_t *)dumpspec);
    return NULL;
}

char *
cmdline_format_dumpspec(
    dumpspec_t *dumpspec)
{
    if (!dumpspec) return NULL;
    return cmdline_format_dumpspec_components(
        dumpspec->host,
        dumpspec->disk,
        dumpspec->datestamp);
}

/* Quote str for shell interpretation, being conservative.
 * Any non-alphanumeric charcacters other than '.' and '/'
 * trigger surrounding single quotes, and single quotes and
 * backslashes within those single quotes are escaped.
 */
static char *
quote_dumpspec_string(char *str)
{
    char *rv;
    char *p, *q;
    int len = 0;
    int need_single_quotes = 0;

    for (p = str; *p; p++) {
        if (!isalnum(*p) && *p != '.' && *p != '/') need_single_quotes=1;
        if (*p == '\'' || *p == '\\') len++; /* extra byte for '\' */
        len++;
    }
    if (need_single_quotes) len += 2;

    q = rv = malloc(len+1);
    if (need_single_quotes) *(q++) = '\'';
    for (p = str; *p; p++) {
        if (*p == '\'' || *p == '\\') *(q++) = '\\';
        *(q++) = *p;
    }
    if (need_single_quotes) *(q++) = '\'';
    *(q++) = '\0';

    return rv;
}

char *
cmdline_format_dumpspec_components(
    char *host,
    char *disk,
    char *datestamp)
{
    char *rv = NULL;

    host = host? quote_dumpspec_string(host):NULL;
    disk = disk? quote_dumpspec_string(disk):NULL;
    datestamp = datestamp? quote_dumpspec_string(datestamp):NULL;

    if (host) {
        rv = host;
        if (disk) {
            rv = newvstralloc(rv, rv, " ", disk, NULL);
            amfree(disk);
            if (datestamp) {
                rv = newvstralloc(rv, rv, " ", datestamp, NULL);
                amfree(datestamp);
            }
        }
    }
    if (disk) amfree(disk);
    if (datestamp) amfree(datestamp);

    return rv;
}

sl_t *
cmdline_match_holding(
    dumpspec_list_t *dumpspec_list)
{
    dumpspec_t *de;
    sl_t *holding_files;
    sle_t *he;
    sl_t *matching_files = new_sl();
    dumpfile_t file;

    holding_set_verbosity(0);
    holding_files = holding_get_files(NULL, 1);

    for (he = holding_files->first; he != NULL; he = he->next) {
	if (!holding_file_get_dumpfile(he->name, &file)) continue;
        if (file.type != F_DUMPFILE) continue;
        for (de = (dumpspec_t *)dumpspec_list; de != NULL; de = de->next) {
            if (de->host && !match_host(de->host, file.name)) continue;
            if (de->disk && !match_disk(de->disk, file.disk)) continue;
            if (de->datestamp && !match_datestamp(de->datestamp, file.datestamp)) continue;
            matching_files = insert_sort_sl(matching_files, he->name);
            break;
        }
    }

    return matching_files;
}
