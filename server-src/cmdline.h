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

#ifndef CMDLINE_H
#define CMDLINE_H

#include "sl.h"

/* TODO: use glib's linked lists instead; dumpspec_list_t provides basic
 * type-checking to allow that to be implemented with a simple search and
 * replace. */

/* A dumpspec can specify a particular dump (combining host, disk, and 
 * datestamp), or can be less specific by leaving out some components.
 * In some cases (such as selecting DLEs), the datestamp is not relevant.
 * Functions for these cases leave the datestamp NULL.
 */
typedef struct dumpspec_s {
    char *host;
    char *disk;
    char *datestamp;

    struct dumpspec_s * next;
} dumpspec_t;

/* temporary */
typedef dumpspec_t dumpspec_list_t;
#define dumpspec_list_first(dsl) ((dumpspec_t *)(dsl))

/*
 * Dumpspec list management
 */

/* Create a new dumpspec with the given components
 *
 * @param host: host name
 * @param disk: disk name
 * @param datestamp: datestamp
 * @returns: dumpspec, or NULL on error
 */
dumpspec_t *
dumpspec_new(
    char *host, 
    char *disk, 
    char *datestamp);

/* Free memory associated with a single dumpspec.  (Does not chase 
 * next pointers)
 *
 * @param dumpspec: the dumpspec to free
 */
void
dumpspec_free(
    dumpspec_t *dumpspec);

/* Free memory associated with a list of dumpspecs.
 *
 * @param dumpspec_list: the dumpspec list to free
 */
void
dumpspec_free_list(
    dumpspec_list_t *dumpspec_list);

/*
 * Parsing
 */

/* Parse a command line matching the following syntax, and return
 * the results as a linked list.  
 *
 *  [ host [ disk [ datestamp [ host [ disk [ datestamp .. ] ] ] ] ] ]
 *
 * If no results are specified, a dumpspec with all entries set to ""
 * is returned; the caller may treat this as a wildcard or an error, as
 * appropriate.
 *
 * Prints a message to stderr and returns NULL if an error occurs.
 *
 * @param argc: count of command line arguments
 * @param argv: command line arguments
 * @returns: dumpspec list, or NULL on error
 */
dumpspec_list_t *
cmdline_parse_dumpspecs(
    int argc,
    char **argv);

/* TODO: new name for match_disklist */
int
cmdline_parse_disk_list_entries(
    int argc,
    char **argv);

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
    char *datestamp);

/*
 * Searching
 */

/* TODO: use glib here too */

/* Find all holding files matching the dumpspec list.  
 *
 * @param dumpspec_list: a list of dumpspecs
 * @returns: a list of holding disk filenames.
 */
sl_t *
cmdline_match_holding(
    dumpspec_list_t *dumpspec_list);

#endif /* CMDLINE_H */

