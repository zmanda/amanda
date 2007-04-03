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
 * $Id: holding.h,v 1.23 2006/05/25 01:47:20 johnfranks Exp $
 *
 * Terminology:
 *
 * Holding disk: a top-level directory given in amanda.conf
 * Holding directory: a subdirectory of a holding disk, named by datestamp
 * Holding file: one or more os-level files in a holding directory, together
 *  representing a single dump
 * Holding file chunks: the individual os-level files (continuations) of
 *  a holding file.
 *
 * Example:
 *
 * /data/holding                                     <-- holding disk
 * /data/holding/200703061234                        <-- holding dir
 * /data/holding/200703061234/videoserver._video_a   <-- holding file, 
                                                         holding file chunk
 * /data/holding/200703061234/videoserver._video_a.1 <-- holding file chunk
 *
 */

#ifndef HOLDING_H
#define HOLDING_H

#include "amanda.h"
#include "diskfile.h"
#include "fileheader.h"
#include "sl.h"

/* utility (used by find.c) */
int is_emptyfile(char *fname);

/*
 * Verbosity
 */

/* Set verbose flag for holding-disk functions
 *
 * @param verbose: if true, log verbosely to stdout
 * @returns: old verbosity
 */
int
holding_set_verbosity(int verbose);

/*
 * Holding disks
 *
 * Use getconf_holdingdisks() to access the list of holding disks.
 */

/*
 * Holding directories
 */

/* Get a list of holding directories, optionally limited to a single
 * holding disk.  Can return a list either of full pathnames or of
 * bare directory names (datestamps).
 *
 * @param hdisk: holding disk to enumerate, or NULL for all
 * @param fullpaths: if true, return full pathnames
 * @returns: newly allocated sl_t of matching directories
 */
sl_t *
holding_get_directories(char *hdisk,
                        int fullpaths);

/*
 * Holding files
 */

/* Get a list of holding files, optionally limited to a single holding
 * directory.  Can return a list either of full pathnames or of
 * bare file names.
 *
 * @param hdir: holding directory to enumerate, or NULL for all
 * @param fullpaths: if true, return full pathnames
 * @returns: newly allocated sl_t of matching files
 */
sl_t *
holding_get_files(char *hdir,
                  int fullpaths);

/* Get a list of holding files that should be flushed, optionally
 * matching only certain datestamps.  This function filters out
 * files for host/disks that are no longer in the disklist.
 *
 * @param dateargs: sl_t of datestamps to dump, or NULL for all
 * @param interactive: if true, be interactive
 * @returns: a newly allocated sl_t listing all matching holding 
 * files
 */
sl_t *
holding_get_files_for_flush(sl_t *dateargs, 
                            int interactive);

/* Get the total size of a holding file, including all holding 
 * file chunks, in kilobytes.
 *
 * @param holding_file: full pathname of holding file
 * @param strip_headers: if true, don't count the headers in the
 * total size
 * @returns: total size of the holding file, or -1 in an error
 */
off_t 
holding_file_size(char *holding_file, 
                  int strip_headers);

/* Unlink a holding file, including all holding file chunks.
 *
 * @param holding_file: full pathname of holding file
 * @returns: 1 on success, else 0
 */
int 
holding_file_unlink(char *holding_file);

/* Given a pathname of a holding file, extract the hostname, diskname,
 * level, and filetype from the header.
 *
 * Caller is responsible for freeing memory for hostname and diskname.
 * None of the result parameters can be NULL.
 *
 * @param fname: full pathname of holding file
 * @param hostname: (result) hostname
 * @param diskname: (result) diskname
 * @param level: (result) level
 * @param datestamp: (result) datestamp of the dump
 * @returns: filetype (see common-src/fileheader.h)
 */
filetype_t 
holding_file_read_header(char *fname,
                         char **hostname,
                         char **diskname,
                         int *level,
                         char **datestamp);

/* Given a pathname of a holding file, read the file header.
 * the result parameter may be altered even if an error is
 * returned.
 *
 * @param fname: full pathname of holding file
 * @param file: (result) dumpfile_t structure
 * @returns: 1 on success, else 0
 */
int 
holding_file_get_dumpfile(char *fname, 
                          dumpfile_t *file);

/*
 * Holding file chunks
 */

/* Get a list of holding files chunks in the given holding 
 * file.  Always returns full paths.
 *
 * @param hfile: holding file to enumerate
 * @returns: newly allocated sl_t of matching holding file chunks
 */
sl_t *
holding_get_file_chunks(char *hfile);

/*
 * application-specific support
 */

/* Allow the user to select a set of datestamps from those in
 * holding disks.
 *
 * @param verbose: verbose logging to stdout
 * @returns: a new sl_t listing all matching datestamps
 */
sl_t *
pick_datestamp(int verbose);

/* Similar to pick_datestamp, but always select all available
 * datestamps.  Non-interactive, but outputs progress to stdout.
 *
 * @param verbose: verbose logging to stdout
 * @returns: a new sl_t listing all matching datestamps
 */
sl_t *
pick_all_datestamp(int verbose);

/* Rename holding files from the temporary names used during
 * creation.
 *
 * @param holding_file: full pathname of holding file,
 * without '.tmp'
 * @param complete: if 0, set 'is_partial' to 1 in each file
 * @returns: 1 on success, else 0
 */
int 
rename_tmp_holding(char *holding_file, 
                   int complete);

/* Remove any empty datestamp directories.
 *
 * @param diskdir: holding directory to clean
 * @param verbose: verbose logging to stdout
 */
void 
cleanup_holdingdisk(char *diskdir, 
                    int verbose);

/* Set up a holding directory and do basic permission
 * checks on it
 *
 * @param diskdir: holding directory to set up
 * @returns: 1 on success, else 0
 */
int 
mkholdingdir(char *diskdir);

#endif /* HOLDING_H */
