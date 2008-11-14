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
 * Holding directory: a subdirectory of a holding disk, usually named by 
 *  datestamp.  These are not accessible through this API.
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

/* Get a list of holding disks.  This is equivalent to 
 * getconf_holdingdisks() with holdingdisk_get_diskdir().
 *
 * @returns: newly allocated GSList of matching disks
 */
GSList *
holding_get_disks(void);

/* Get a list of holding files, optionally limited to a single holding
 * directory.  Can return a list either of full pathnames or of
 * bare file names.
 *
 * @param hdir: holding directory to enumerate, or NULL for all
 * @param fullpaths: if true, return full pathnames
 * @returns: newly allocated GSList of matching files
 */
GSList *
holding_get_files(char *hdir,
                  int fullpaths);

/* Get a list of holding files chunks in the given holding
 * file.  Always returns full paths.
 *
 * @param hfile: holding file to enumerate
 * @returns: newly allocated GSList of matching holding file chunks
 */
GSList *
holding_get_file_chunks(char *hfile);

/* Get a list of holding files that should be flushed, optionally
 * matching only certain datestamps.  This function filters out
 * files for host/disks that are no longer in the disklist.
 *
 * @param dateargs: GSList of datestamps expressions to dump, or NULL 
 * for all
 * @returns: a newly allocated GSList listing all matching holding
 * files
 */
GSList *
holding_get_files_for_flush(GSList *dateargs);

/* Get a list of all datestamps for which dumps are in the holding
 * disk.  This scans all dumps and takes the union of their
 * datestamps (some/all of which may actually be timestamps, 
 * depending on the setting of config option usetimestamps)
 *
 * @returns: a newly allocated GSList listing all datestamps
 */
GSList *
holding_get_all_datestamps(void);

/* Get the total size of a holding file, including all holding
 * file chunks, in kilobytes.
 *
 * @param holding_file: full pathname of holding file
 * @param strip_headers: if true, don't count the headers in the
 * total size
 * @returns: total size in kbytes of the holding file, or -1 in an error
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
 * Maintenance
 */

/* Clean up all holding disks, restoring from a possible crash or
 * other errors.  This function is intentionally opaque, as the
 * details of holding disk are hidden from other applications.
 *
 * All error and warning messages go to the debug log.
 *
 * @param corrupt_dle: function that is called for any DLEs for
 * which corrupt dumps are found.
 * @param verbose_output: if non-NULL, send progress messages to
 * this file.
 */
typedef void (*corrupt_dle_fn)(char *hostname, char *disk);
void
holding_cleanup(corrupt_dle_fn corrupt_dle,
    FILE *verbose_output);

/*
 * application-specific support
 */

/* Rename holding files from the temporary names used during
 * creation.
 *
 * This is currently called by driver.c, but will disappear when
 * holding is fully converted to the device API
 *
 * @param holding_file: full pathname of holding file,
 * without '.tmp'
 * @param complete: if 0, set 'is_partial' to 1 in each file
 * @returns: 1 on success, else 0
 */
int
rename_tmp_holding(char *holding_file,
                   int complete);

/* Set up a holding directory and do basic permission
 * checks on it
 *
 * @param diskdir: holding directory to set up
 * @returns: 1 on success, else 0
 */
int
mkholdingdir(char *diskdir);

#endif /* HOLDING_H */
