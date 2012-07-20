/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 465 S. Mathilda Ave., Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

#include <glib.h>

/* A note regarding error handling in this module.  Amar returns errors via the
 * Glib GError mechanism.  Most functions return a boolean, where TRUE
 * indicates success, and FALSE indicates an error which is indicated in the
 * 'error' parameter.
 *
 * Fatal programming errors are handled with assertions and error exits; any
 * fatal format or system errors are handled via GError.  Some format errors
 * (e.g., missing EOAs at the end of a file) are handled without any
 * acknowledgement.
 *
 * The domain for amar errors is that returned from amar_error_quark, and error
 * codes are system error codes (e.g., EINVAL, ENOSPC). */

GQuark amar_error_quark(void);

/* opaque types for archives, files, and attributes */

typedef struct amar_s amar_t;
typedef struct amar_file_s amar_file_t;
typedef struct amar_attr_s amar_attr_t;

/* Application attribute IDs should start at AMAR_ATTR_APP_START */

enum {
    /* internal-use only attributes */
    AMAR_ATTR_FILENAME = 0,
    AMAR_ATTR_EOF = 1,

    /* anything above this value can be used by the application */
    AMAR_ATTR_APP_START = 16,
    AMAR_ATTR_GENERIC_DATA = AMAR_ATTR_APP_START,
};

/* Create an object to read/write an amanda archive on the file descriptor fd.
 * @param fd: file descriptor of the file, it must already be opened
 * @mode: O_RDONLY for reading, O_WRONLY for writing
 * @returns: NULL on error
 */
amar_t *amar_new(int fd, mode_t mode, GError **error);

/* Finish writing to this fd.  All buffers are flushed, but the file descriptor
 * is not closed -- the user must close it. */
gboolean amar_close(amar_t *archive, GError **error);

/* create a new 'file' object on the archive.  The filename is treated as a
 * binary blob, but if filename_len is zero, then its length will be calculated
 * with strlen().  A zero-length filename_buf is not allowed.
 *
 * Note that a header record will only be written if header_offset is non-NULL,
 * as this represents a location to which a reader could seek.
 *
 * @param archive: the archive containing this file
 * @param filename_buf: filename to include in the file
 * @param filename_len: length of the filename_buf, or 0 to calculate
 * @param header_offset (output): offset of the header record preceding
 *	this file; pass NULL to ignore.
 * @returns: NULL on error, otherwise a file object
 */
amar_file_t *amar_new_file(
	    amar_t *archive,
	    char *filename_buf,
	    gsize filename_len,
	    off_t *header_offset,
	    GError **error);

/* Flush all buffer the 'file' object and write a record with ID=2 */
gboolean amar_file_close(
	    amar_file_t *file,
	    GError **error);

/* create a new 'attribute' object with attrid attached to the file
 *
 * @returns: NULL on error, otherwise an attribute object
 */
amar_attr_t *amar_new_attr(
	    amar_file_t *file,
	    uint16_t attrid,
	    GError **error);

/* flush all buffers and mark the end of the attribute */
gboolean amar_attr_close(
	    amar_attr_t *attribute,
	    GError **error);

/* Add 'size' byte of data from 'data' to the attribute.  If this is the
 * last data in this attribute, set eoa to TRUE.  This will save space by
 * writing and end-of-attribute indication in this record, instead of adding
 * an empty EOA record.
 */
gboolean amar_attr_add_data_buffer(
	    amar_attr_t *attribute,
	    gpointer data,
	    gsize size,
	    gboolean eoa,
	    GError **error);

/* This function reads from the file descriptor 'fd' until EOF and adds
 * the resulting data to the attribute.  The end of the attribute is
 * flagged appropriately if EOA is true.
 *
 * @param attribute: the attribute for the data
 * @param fd: the file descriptor from which to read
 * @param eoa: add an EOA bit to the end?
 * @returns: number of bytes read from fd, or -1 on error
 */
off_t amar_attr_add_data_fd(
	    amar_attr_t *attribute,
	    int fd,
	    gboolean eoa,
	    GError **error);

/* When reading files, the handling of each attribute can be configured
 * separately.  Some attributes may always be short enough to fit in memory,
 * and in this case the archive interface will take care of assembling any
 * fragments for you.  Some attributes should be ignored, while others
 * will call a function for each fragment.
 *
 * There are a a number of xx_data options available here, that deserve some
 * disambiguation.
 *  - user_data is global to the entire read operation (it is a parameter to
 *    amar_read)
 *  - file_data is specific to the current file; it is set by the start_file
 *    callback and freed by the finish_file callback.
 *  - attrid_data is specific this the current attribute ID, across all files;
 *    it comes from the amar_attr_handling_t struct.
 *  - attr_data is specific to the current instance of the particular
 *    attribute.  It points to a NULL pointer on the first call to the fragment
 *    callback, and can be set at that time.  It should be freed when the EOA
 *    argument is TRUE.
 *
 * @param user_data: the pointer passed to amar_read
 * @param filenum: the file number for this record
 * @param file_data: the file_data pointer returned from the start_file callback
 * @param attrid: the attribute id for this record
 * @param attrid_data: the data from the handling array
 * @param attr_data (in/out): data for this attribute; this will be the same
 *	  pointer for every callback for a particular instance of an attribute.
 *	  Any resources should be freed when eoa is true.
 * @param data: the data for this fragment
 * @param size: the size of data
 * @param eoa: TRUE iff this is the last fragment for this attribute
 * @param truncated: TRUE if this attribute is likely to be incomplete (e.g.,
 *	  in an error situation)
 * @returns: FALSE if the amar_read call should be aborted
 */
typedef gboolean (*amar_fragment_callback_t)(
	gpointer user_data,
	uint16_t filenum,
	gpointer file_data,
	uint16_t attrid,
	gpointer attrid_data,
	gpointer *attr_data,
	gpointer data,
	gsize size,
	gboolean eoa,
	gboolean truncated);

/* amar_read takes an array of this struct, terminated by an entry
 * with attrid 0.  This final entry is used as the "catchall" for attributes
 * not matching any other array entries. */
typedef struct amar_attr_handling_s {
    uint16_t attrid;

    /* if nonzero, this is the minimum size fragment that will be passed to the
     * callback.  Use SIZE_MAX for no limit, although this may result in
     * excessive memory use while parsing a malicious or corrupt archive. */
    gsize min_size;

    /* if non-NULL, this function will be called for each fragment
     * with this attribute ID */
    amar_fragment_callback_t callback;

    /* this value is passed as the attr_data parameter to the callback */
    gpointer attrid_data;
} amar_attr_handling_t;

/* This function is called for each new file, and can decide whether to ignore
 * the file, or set up file-specific data.
 *
 * @param user_data: the pointer passed to amar_read
 * @param filenum: the file number for this record
 * @param filename_buf: the filename of this file
 * @param filename_len: the length of the filename
 * @param ignore (output): if set to TRUE, ignore all attributes for this file.
 * @param file_data (output): space to store file-specific data
 * @returns: FALSE if the amar_read call should be aborted
 */
typedef gboolean (*amar_file_start_callback_t)(
	gpointer user_data,
	uint16_t filenum,
	gpointer filename_buf,
	gsize filename_len,
	gboolean *ignore,
	gpointer *file_data);

/* This function is called for each new file, and can decide whether to ignore
 * the file, or set up file-specific data.
 *
 * @param user_data: the pointer passed to amar_read
 * @param filenum: the file number for this record
 * @param file_data (output): space to store file-specific data
 * @param truncated: TRUE if this file is likely to be incomplete (e.g.,
 *	  in an error situation, or at an early EOF)
 * @returns: FALSE if the amar_read call should be aborted
 */
typedef gboolean (*amar_file_finish_callback_t)(
	gpointer user_data,
	uint16_t filenum,
	gpointer *file_data,
	gboolean truncated);

/* This function actually performs the read operation, calling all of the
 * above callbacks.  If any of the callbacks return FALSE, this function
 * returns FALSE but does not set its error parameter.
 *
 * @param user_data: passed to all callbacks
 * @param handling_array: array giving handling information
 * @param file_start_cb: callback for file starts
 * @param file_finish_cb: callback for file finishs
 * @param error (output): the error result
 * @returns: FALSE on error or an early exit, otherwise TRUE
 */
gboolean amar_read(
	amar_t *archive,
	gpointer user_data,
	amar_attr_handling_t *handling_array,
	amar_file_start_callback_t file_start_cb,
	amar_file_finish_callback_t file_finish_cb,
	GError **error);
