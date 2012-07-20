/*
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * 
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
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
 * Contact information: Zmanda Inc, 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 *
 * Author: Dustin J. Mitchell <dustin@zmanda.com>
 */
/*
 * $Id$
 *
 * Utility routines for handling command lines.
 */

#ifndef CMDLINE_H
#define CMDLINE_H

#include <glib.h>
#include "glib-util.h"

/* A dumpspec can specify a particular dump (combining host, disk, and 
 * datestamp), or can be less specific by leaving out some components.
 * Missing components are NULL, except in the special case of an 
 * "wildcard" dumpspec, as detailed below.
 *
 * All strings in this struct are independently malloc()ed.
 */
typedef struct dumpspec_s {
    char *host;
    char *disk;
    char *datestamp;
    char *level;
    char *write_timestamp;
} dumpspec_t;

/*
 * Dumpspec list management
 */

/* Create a new dumpspec with the given components
 *
 * @param host: host name
 * @param disk: disk name
 * @param datestamp: datestamp
 * @param level: level (as a string, allowing regexes)
 * @param write_timestamp: timestamp written to tape.
 * @returns: dumpspec, or NULL on error
 */
dumpspec_t *
dumpspec_new(
    char *host, 
    char *disk, 
    char *datestamp,
    char *level,
    char *write_timestamp);

/* Free memory associated with a single dumpspec.  (Does not chase 
 * next pointers)
 *
 * @param dumpspec: the dumpspec to free
 */
void
dumpspec_free(
    dumpspec_t *dumpspec);

/* Free memory associated with a list of dumpspecs.  CAUTION: do not
 * use glib's g_slist_free directly on a dumpspec list, as it will not
 * free the elements themselves.
 *
 * @param dumpspec_list: the GSList of dumpspecs to free
 */
void
dumpspec_list_free(
    GSList *dumpspec_list);

/*
 * Parsing
 */

/* Parse a command line matching the following syntax, and return
 * the results as a linked list.  
 *
 *  [ host [ disk [ datestamp [ host [ disk [ datestamp .. ] ] ] ] ] ]
 *
 * If no results are specified, the function either returns NULL (an 
 * empty list) or, if CMDLINE_EMPTY_TO_WILDCARD is given, a list 
 * containing a single dumpspec with all fields set to "".
 *
 * Calls error() with any fatal errors, e.g., invalid regexes.
 *
 * @param argc: count of command line arguments
 * @param argv: command line arguments
 * @param flags: bitmask of the CMDLINE_* flags
 * @returns: dumpspec list
 */
GSList *
cmdline_parse_dumpspecs(
    int argc,
    char **argv,
    int flags);
/* flags values (bitmask): */
    /* parse datestamps after disks */
#    define CMDLINE_PARSE_DATESTAMP (1<<0)
    /* parse levels after datestamps or disks */
#    define CMDLINE_PARSE_LEVEL (1<<1)
    /* an empty argv should result in a wildcard dumpspec */
#    define CMDLINE_EMPTY_TO_WILDCARD (1<<2)

/*
 * Formatting
 */

/* Format a dumpspec into a string, with shell-compatible quoting.
 *
 * Caller is responsible for freeing the string.
 *
 * @param dumpspec: the dumpspec to format
 * @returns: newly allocated string, or NULL on error
 */
char *
cmdline_format_dumpspec(
    dumpspec_t *dumpspec);

/* Like cmdline_format_dumpspec, but with components supplied 
 * individually.  Caller is responsible for freeing the 
 * string.
 *
 * @param host: host name
 * @param disk: disk name
 * @param datestamp: datestamp
 * @returns: newly allocated string, or NULL on error
 */
char *
cmdline_format_dumpspec_components(
    char *host,
    char *disk,
    char *datestamp,
    char *level);

/*
 * Searching
 */

/* Find all holding files matching the dumpspec list.  If
 * the dumpspec list contains a dumpspec with all blank
 * entries, all holding files are returned.
 *
 * Free the resulting list with slist_free_full()
 *
 * @param dumpspec_list: a list of dumpspecs
 * @returns: a list of holding disk filenames.
 */
GSList *
cmdline_match_holding(
    GSList *dumpspec_list);

#endif /* CMDLINE_H */

