/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
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
 * Contact information: Carbonite Inc., 756 N Pastoria Ave
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */
/*
 * $Id$
 *
 * Utility routines for handling command lines.
 */

#include "amanda.h"
#include <ctype.h>
#include "match.h"
#include "cmdline.h"
#include "holding.h"

dumpspec_t *
dumpspec_new(
    char *host, 
    char *disk, 
    char *datestamp,
    char *level,
    char *write_timestamp)
{
    dumpspec_t *rv;

    rv = g_new0(dumpspec_t, 1);
    if (host) rv->host = g_strdup(host);
    if (disk) rv->disk = g_strdup(disk);
    if (datestamp) rv->datestamp = g_strdup(datestamp);
    if (level) rv->level = g_strdup(level);
    if (write_timestamp) rv->write_timestamp = g_strdup(write_timestamp);

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
    if (dumpspec->level) free(dumpspec->level);
    if (dumpspec->write_timestamp) free(dumpspec->write_timestamp);
    free(dumpspec);
}

void
dumpspec_list_free(
    GSList *dumpspec_list)
{
    /* first free all of the individual dumpspecs */
    g_slist_foreach_nodata(dumpspec_list, dumpspec_free);

    /* then free the list itself */
    g_slist_free(dumpspec_list);
}

GSList *
cmdline_parse_dumpspecs(
    int argc,
    char **argv,
    int flags)
{
    dumpspec_t *dumpspec = NULL;
    GSList *list = NULL;
    char *errstr;
    char *name;
    int optind = 0;
    enum { ARG_GET_HOST, ARG_GET_DISK, ARG_GET_DATESTAMP, ARG_GET_LEVEL } arg_state = ARG_GET_HOST;

    while (optind < argc) {
	char *new_name = NULL;
        name = argv[optind];
	if (flags & CMDLINE_EXACT_MATCH  && *name != '=') {
	    new_name = g_strconcat("=", name, NULL);
	    name = new_name;
	}
        switch (arg_state) {
            case ARG_GET_HOST:
                arg_state = ARG_GET_DISK;
                dumpspec = dumpspec_new(name, NULL, NULL, NULL, NULL);
		list = g_slist_append(list, (gpointer)dumpspec);
                break;

            case ARG_GET_DISK:
                arg_state = ARG_GET_DATESTAMP;
                dumpspec->disk = g_strdup(name);
                break;

            case ARG_GET_DATESTAMP:
                arg_state = ARG_GET_LEVEL;
		if (!(flags & CMDLINE_PARSE_DATESTAMP)) continue;
                dumpspec->datestamp = g_strdup(name);
                break;

            case ARG_GET_LEVEL:
                arg_state = ARG_GET_HOST;
		if (!(flags & CMDLINE_PARSE_LEVEL)) continue;
                if (name[0] != '\0'
		    && !(flags & CMDLINE_EXACT_MATCH)
                    && (errstr=validate_regexp(name)) != NULL) {
                    error(_("bad level regex \"%s\": %s\n"), name, errstr);
                }
                dumpspec->level = g_strdup(name);
                break;
        }
	amfree(new_name);

	optind++;
    }

    /* if nothing was processed and the caller has requested it, 
     * then add an "empty" element */
    if (list == NULL && (flags & CMDLINE_EMPTY_TO_WILDCARD)) {
        dumpspec = dumpspec_new("", "", 
		(flags & CMDLINE_PARSE_DATESTAMP)?"":NULL,
		(flags & CMDLINE_PARSE_LEVEL)?"":NULL, "");
	list = g_slist_append(list, (gpointer)dumpspec);
    }

    return list;
}

char *
cmdline_format_dumpspec(
    dumpspec_t *dumpspec)
{
    if (!dumpspec) return NULL;
    return cmdline_format_dumpspec_components(
        dumpspec->host,
        dumpspec->disk,
        dumpspec->datestamp,
	dumpspec->level);
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

    if (!str[0])
	return g_strdup("''"); /* special-case the empty string */

    for (p = str; *p; p++) {
        if (!isalnum((int)*p) && *p != '.' && *p != '/') need_single_quotes=1;
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
    char *datestamp,
    char *level)
{
    GPtrArray *array = g_ptr_array_new();
    gchar **strings;
    char *ret = NULL;

    if (!host)
        goto out;

    g_ptr_array_add(array, quote_dumpspec_string(host));

    if (!disk)
        goto out;

    g_ptr_array_add(array, quote_dumpspec_string(disk));

    if (!datestamp)
        goto out;

    g_ptr_array_add(array, quote_dumpspec_string(datestamp));

    if (level)
        g_ptr_array_add(array, quote_dumpspec_string(level));

out:
    g_ptr_array_add(array, NULL);
    strings = (gchar **) g_ptr_array_free(array, FALSE);

    /*
     * Note that if the only element of the GPtrArray is a NULL pointer, the
     * result will be an array with only a NULL pointer, in which case
     * g_strjoinv() will return an empty string. But we want to return NULL to
     * the caller in that case.
     */

    ret = (*strings) ? g_strjoinv(" ", strings) : NULL;
    g_strfreev(strings);

    return ret;
}

GSList *
cmdline_match_holding(
    GSList *dumpspec_list)
{
    dumpspec_t *de;
    GSList *li, *hi;
    GSList *holding_files;
    GSList *matching_files = NULL;
    dumpfile_t file;

    holding_files = holding_get_files(NULL, 1);

    for (hi = holding_files; hi != NULL; hi = hi->next) {
	/* TODO add level */
	if (!holding_file_get_dumpfile((char *)hi->data, &file)) continue;
        if (file.type != F_DUMPFILE) {
	    dumpfile_free_data(&file);
	    continue;
	}
        for (li = dumpspec_list; li != NULL; li = li->next) {
	    de = (dumpspec_t *)(li->data);
            if (de->host && de->host[0] && !match_host(de->host, file.name)) continue;
            if (de->disk && de->disk[0] && !match_disk(de->disk, file.disk)) continue;
            if (de->datestamp && de->datestamp[0] && !match_datestamp(de->datestamp, file.datestamp)) continue;
            matching_files = g_slist_append(matching_files, g_strdup((char *)hi->data));
            break;
        }
	dumpfile_free_data(&file);
    }

    slist_free_full(holding_files, g_free);

    return matching_files;
}
