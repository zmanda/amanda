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

#include "amanda.h"
#include "util.h"
#include "amar.h"

/* Each block in an archive is made up of one or more records, where each
 * record is either a header record or a data record.  The two are
 * distinguished by the header magic string; the string 'AM' is
 * explicitly excluded as an allowed filenum to prevent ambiguity. */

#define HEADER_MAGIC "AMANDA ARCHIVE FORMAT"
#define MAGIC_FILENUM 0x414d
#define HEADER_VERSION 1
#define EOA_BIT 0x80000000

typedef struct header_s {
    /* magic is HEADER_MAGIC + ' ' + decimal version, NUL padded */
    char     magic[28];
} header_t;
#define HEADER_SIZE (SIZEOF(header_t))

typedef struct record_s {
    uint16_t filenum;
    uint16_t attrid;
    uint32_t size;
} record_t;
#define RECORD_SIZE (SIZEOF(record_t))
#define MAX_RECORD_DATA_SIZE (4*1024*1024)

#define MKRECORD(ptr, f, a, s, eoa) do { \
    record_t r; \
    uint32_t size = s; \
    if (eoa) size |= EOA_BIT; \
    r.filenum = htons(f); \
    r.attrid = htons(a); \
    r.size = htonl(size); \
    memcpy(ptr, &r, sizeof(record_t)); \
} while(0)

/* N.B. - f, a, s, and eoa must be simple lvalues */
#define GETRECORD(ptr, f, a, s, eoa) do { \
    record_t r; \
    memcpy(&r, ptr, sizeof(record_t)); \
    s = ntohl(r.size); \
    if (s & EOA_BIT) { \
	eoa = TRUE; \
	s &= ~EOA_BIT; \
    } else { \
	eoa = FALSE; \
    } \
    f = ntohs(r.filenum); \
    a = ntohs(r.attrid); \
} while(0)

/* performance knob: how much data will we buffer before just
 * writing straight out of the user's buffers? */
#define WRITE_BUFFER_SIZE (512*1024)

struct amar_s {
    int       fd;		/* file descriptor			*/
    mode_t    mode;		/* mode O_RDONLY or O_WRONLY		*/
    uint16_t  maxfilenum;	/* Next file number to allocate		*/
    header_t  hdr;		/* pre-constructed header		*/
    off_t     position;		/* current position in the archive	*/
    GHashTable *files;		/* List of all amar_file_t	*/
    gboolean  seekable;		/* does lseek() work on this fd? */

    /* internal buffer; on writing, this is WRITE_BUFFER_SIZE bytes, and
     * always has at least RECORD_SIZE bytes free. */
    gpointer buf;
    size_t buf_len;
    size_t buf_size;
};

struct amar_file_s {
    amar_t *archive;		/* archive for this file	*/
    gint             filenum;		/* filenum of this file; gint is required by hash table */
    GHashTable       *attributes;	/* all attributes for this file */
};

struct amar_attr_s {
    amar_file_t *file;	/* file for this attribute	*/
    gint attrid;	/* id of this attribute		*/
    gboolean wrote_eoa;	/* If the attribute is finished	*/
};

/*
 * Internal functions
 */

GQuark
amar_error_quark(void)
{
    static GQuark q;
    if (!q)
	q = g_quark_from_static_string("amar_error");
    return q;
}

static gboolean
flush_buffer(
	amar_t *archive,
	GError **error)
{
    if (archive->buf_len) {
	if (full_write(archive->fd, archive->buf, archive->buf_len) != archive->buf_len) {
	    g_set_error(error, amar_error_quark(), errno,
			"Error writing to amanda archive: %s", strerror(errno));
	    return FALSE;
	}
	archive->buf_len = 0;
    }

    return TRUE;
}

static gboolean
write_header(
	amar_t *archive,
	GError **error)
{
    /* if it won't fit in the buffer, take the easy way out and flush it */
    if (archive->buf_len + HEADER_SIZE >= WRITE_BUFFER_SIZE - RECORD_SIZE) {
	if (!flush_buffer(archive, error))
	    return FALSE;
    }

    memcpy(archive->buf + archive->buf_len, &archive->hdr, HEADER_SIZE);
    archive->buf_len += HEADER_SIZE;
    archive->position += HEADER_SIZE;

    return TRUE;
}

static gboolean
write_record(
	amar_t *archive,
	uint16_t filenum,
	uint16_t attrid,
	gboolean eoa,
	gpointer data,
	gsize data_size,
	GError **error)
{
    /* the buffer always has room for a new record header */
    MKRECORD(archive->buf + archive->buf_len, filenum, attrid, data_size, eoa);
    archive->buf_len += RECORD_SIZE;

    /* is it worth copying this record into the buffer? */
    if (archive->buf_len + RECORD_SIZE + data_size < WRITE_BUFFER_SIZE - RECORD_SIZE) {
	/* yes, it is */
	if (data_size)
	    memcpy(archive->buf + archive->buf_len, data, data_size);
	archive->buf_len += data_size;
    } else {
	/* no, it's not */
	struct iovec iov[2];

	/* flush the buffer and write the new data, all in one syscall */
	iov[0].iov_base = archive->buf;
	iov[0].iov_len = archive->buf_len;
	iov[1].iov_base = data;
	iov[1].iov_len = data_size;
	if (full_writev(archive->fd, iov, 2) < 0) {
	    g_set_error(error, amar_error_quark(), errno,
			"Error writing to amanda archive: %s", strerror(errno));
	    return FALSE;
	}
	archive->buf_len = 0;
    }

    archive->position += data_size + RECORD_SIZE;
    return TRUE;
}

/*
 * Public functions
 */

amar_t *
amar_new(
    int       fd,
    mode_t mode,
    GError **error)
{
    amar_t *archive = malloc(SIZEOF(amar_t));

    /* make some sanity checks first */
    g_assert(fd >= 0);
    g_assert(mode == O_RDONLY || mode == O_WRONLY);

    archive->fd = fd;
    archive->mode = mode;
    archive->maxfilenum = 0;
    archive->position = 0;
    archive->seekable = TRUE; /* assume seekable until lseek() fails */
    archive->files = g_hash_table_new(g_int_hash, g_int_equal);
    archive->buf = NULL;

    if (mode == O_WRONLY) {
	archive->buf = g_malloc(WRITE_BUFFER_SIZE);
	archive->buf_size = WRITE_BUFFER_SIZE;
    }
    archive->buf_len = 0;

    if (mode == O_WRONLY) {
	/* preformat a header with our version number */
	bzero(archive->hdr.magic, HEADER_SIZE);
	snprintf(archive->hdr.magic, HEADER_SIZE,
	    HEADER_MAGIC " %d", HEADER_VERSION);

	/* and write it out to start the file */
	if (!write_header(archive, error)) {
	    amar_close(archive, NULL); /* flushing buffer won't fail */
	    return NULL;
	}
    }

    return archive;
}

gboolean
amar_close(
    amar_t *archive,
    GError **error)
{
    gboolean success = TRUE;

    /* verify all files are done */
    g_assert(g_hash_table_size(archive->files) == 0);

    if (!flush_buffer(archive, error))
	success = FALSE;

    g_hash_table_destroy(archive->files);
    if (archive->buf) g_free(archive->buf);
    amfree(archive);

    return success;
}

/*
 * Writing
 */

amar_file_t *
amar_new_file(
    amar_t *archive,
    char *filename_buf,
    gsize filename_len,
    off_t *header_offset,
    GError **error)
{
    amar_file_t *file = NULL;

    g_assert(archive->mode == O_WRONLY);
    g_assert(filename_buf != NULL);

    /* set filename_len if it wasn't specified */
    if (!filename_len)
	filename_len = strlen(filename_buf);
    g_assert(filename_len != 0);

    if (filename_len > MAX_RECORD_DATA_SIZE) {
	g_set_error(error, amar_error_quark(), ENOSPC,
		    "filename is too long for an amanda archive");
	return NULL;
    }

    /* pick a new, unused filenum */

    if (g_hash_table_size(archive->files) == 65535) {
	g_set_error(error, amar_error_quark(), ENOSPC,
		    "No more file numbers available");
	return NULL;
    }

    do {
	gint filenum;

	archive->maxfilenum++;

	/* MAGIC_FILENUM can't be used because it matches the header record text */
	if (archive->maxfilenum == MAGIC_FILENUM) {
	    continue;
	}

	/* see if this fileid is already in use */
	filenum = archive->maxfilenum;
	if (g_hash_table_lookup(archive->files, &filenum))
	    continue;

    } while (0);

    file = g_new0(amar_file_t, 1);
    file->archive = archive;
    file->filenum = archive->maxfilenum;
    file->attributes = g_hash_table_new_full(g_int_hash, g_int_equal, NULL, g_free);
    g_hash_table_insert(archive->files, &file->filenum, file);

    /* record the current position and write a header there, if desired */
    if (header_offset) {
	*header_offset = archive->position;
	if (!write_header(archive, error))
	    goto error_exit;
    }

    /* add a filename record */
    if (!write_record(archive, file->filenum, AMAR_ATTR_FILENAME,
		      1, filename_buf, filename_len, error))
	goto error_exit;

    return file;

error_exit:
    if (file) {
	g_hash_table_remove(archive->files, &file->filenum);
	g_hash_table_destroy(file->attributes);
	g_free(file);
    }
    return NULL;
}

static void
foreach_attr_close(
	gpointer key G_GNUC_UNUSED,
	gpointer value,
	gpointer user_data)
{
    amar_attr_t *attr = value;
    GError **error = user_data;

    /* return immediately if we've already seen an error */
    if (*error)
	return;

    if (!attr->wrote_eoa) {
	amar_attr_close(attr, error);
    }
}

gboolean
amar_file_close(
    amar_file_t *file,
    GError **error)
{
    gboolean success = TRUE;
    amar_t *archive = file->archive;

    /* close all attributes that haven't already written EOA */
    g_hash_table_foreach(file->attributes, foreach_attr_close, error);
    if (*error)
	success = FALSE;

    /* write an EOF record */
    if (success) {
	if (!write_record(archive, file->filenum, AMAR_ATTR_EOF, 1,
			  NULL, 0, error))
	    success = FALSE;
    }

    /* remove from archive->file list */
    g_hash_table_remove(archive->files, &file->filenum);

    /* clean up */
    g_hash_table_destroy(file->attributes);
    amfree(file);

    return success;
}

amar_attr_t *
amar_new_attr(
    amar_file_t *file,
    uint16_t attrid,
    GError **error G_GNUC_UNUSED)
{
    amar_attr_t *attribute;
    gint attrid_gint = attrid;

    /* make sure this attrid isn't already present */
    g_assert(attrid >= AMAR_ATTR_APP_START);
    g_assert(g_hash_table_lookup(file->attributes, &attrid_gint) == NULL);

    attribute = malloc(SIZEOF(amar_attr_t));
    attribute->file = file;
    attribute->attrid = attrid;
    attribute->wrote_eoa = FALSE;
    g_hash_table_replace(file->attributes, &attribute->attrid, attribute);

    /* (note this function cannot currently return an error) */

    return attribute;
}

gboolean
amar_attr_close(
    amar_attr_t *attribute,
    GError **error)
{
    amar_file_t   *file    = attribute->file;
    amar_t        *archive = file->archive;
    gboolean rv = TRUE;

    /* write an empty record with EOA_BIT set if we haven't ended
     * this attribute already */
    if (!attribute->wrote_eoa) {
	if (!write_record(archive, file->filenum, attribute->attrid,
			  1, NULL, 0, error))
	    rv = FALSE;
	attribute->wrote_eoa = TRUE;
    }

    return rv;
}

gboolean
amar_attr_add_data_buffer(
    amar_attr_t *attribute,
    gpointer data, gsize size,
    gboolean eoa,
    GError **error)
{
    amar_file_t *file = attribute->file;
    amar_t *archive = file->archive;

    g_assert(!attribute->wrote_eoa);

    /* write records until we've consumed all of the buffer */
    while (size) {
	gsize rec_data_size;
	gboolean rec_eoa = FALSE;

	if (size > MAX_RECORD_DATA_SIZE) {
	    rec_data_size = MAX_RECORD_DATA_SIZE;
	} else {
	    rec_data_size = size;
	    if (eoa)
		rec_eoa = TRUE;
	}

	if (!write_record(archive, file->filenum, attribute->attrid,
			  rec_eoa, data, rec_data_size, error))
	    return FALSE;

	data += rec_data_size;
	size -= rec_data_size;
    }

    if (eoa) {
	attribute->wrote_eoa = TRUE;
    }

    return TRUE;
}

off_t
amar_attr_add_data_fd(
    amar_attr_t *attribute,
    int fd,
    gboolean eoa,
    GError **error)
{
    amar_file_t   *file    = attribute->file;
    amar_t        *archive = file->archive;
    gssize size;
    off_t filesize = 0;
    gpointer buf = g_malloc(MAX_RECORD_DATA_SIZE);

    g_assert(!attribute->wrote_eoa);

    /* read and write until reaching EOF */
    while ((size = full_read(fd, buf, MAX_RECORD_DATA_SIZE)) >= 0) {
	if (!write_record(archive, file->filenum, attribute->attrid,
			    eoa && (size < MAX_RECORD_DATA_SIZE), buf, size, error))
	    goto error_exit;

	filesize += size;

	if (size < MAX_RECORD_DATA_SIZE)
	    break;
    }

    if (size < 0) {
	g_set_error(error, amar_error_quark(), errno,
		    "Error reading from fd %d: %s", fd, strerror(errno));
	goto error_exit;
    }
    g_free(buf);

    attribute->wrote_eoa = eoa;

    return filesize;

error_exit:
    g_free(buf);
    return -1;
}

/*
 * Reading
 */

/* Note that this implementation assumes that an archive will have a "small"
 * number of open files at any time, and a limited number of attributes for
 * each file. */

typedef struct attr_state_s {
    uint16_t attrid;
    amar_attr_handling_t *handling;
    gpointer buf;
    gsize buf_len;
    gsize buf_size;
    gpointer attr_data;
    gboolean wrote_eoa;
} attr_state_t;

typedef struct file_state_s {
    uint16_t filenum;
    gpointer file_data; /* user's data */
    gboolean ignore;

    GSList *attr_states;
} file_state_t;

typedef struct handling_params_s {
    /* parameters from the user */
    gpointer user_data;
    amar_attr_handling_t *handling_array;
    amar_file_start_callback_t file_start_cb;
    amar_file_finish_callback_t file_finish_cb;

    /* tracking for open files and attributes */
    GSList *file_states;

    /* read buffer */
    gpointer buf;
    gsize buf_size; /* allocated size */
    gsize buf_len; /* number of active bytes .. */
    gsize buf_offset; /* ..starting at buf + buf_offset */
    gboolean got_eof;
    gboolean just_lseeked; /* did we just call lseek? */
} handling_params_t;

/* buffer-handling macros and functions */

/* Ensure that the archive buffer contains at least ATLEAST bytes.  Returns
 * FALSE if that many bytes are not available due to EOF or another error. */
static gboolean
buf_atleast_(
    amar_t *archive,
    handling_params_t *hp,
    gsize atleast)
{
    gsize to_read;
    gsize bytes_read;

    /* easy case of hp->buf_len >= atleast is taken care of by the macro, below */

    if (hp->got_eof)
	return FALSE;

    /* If we just don't have space for this much data yet, then we'll have to reallocate
     * the buffer */
    if (hp->buf_size < atleast) {
	if (hp->buf_offset == 0) {
	    hp->buf = g_realloc(hp->buf, atleast);
	} else {
	    gpointer newbuf = g_malloc(atleast);
	    if (hp->buf) {
		memcpy(newbuf, hp->buf+hp->buf_offset, hp->buf_len);
		g_free(hp->buf);
	    }
	    hp->buf = newbuf;
	    hp->buf_offset = 0;
	}
	hp->buf_size = atleast;
    }

    /* if we have space in this buffer to satisfy the request, but not without moving
     * the existing data around, then move the data around */
    else if (hp->buf_size - hp->buf_offset < atleast) {
	memmove(hp->buf, hp->buf+hp->buf_offset, hp->buf_len);
	hp->buf_offset = 0;
    }

    /* as an optimization, if we just called lseek, then only read the requested
     * bytes in case we're going to lseek again. */
    if (hp->just_lseeked)
	to_read = atleast - hp->buf_len;
    else
	to_read = hp->buf_size - hp->buf_offset - hp->buf_len;

    bytes_read = full_read(archive->fd,
			   hp->buf+hp->buf_offset+hp->buf_len,
			   to_read);
    if (bytes_read < to_read)
	hp->got_eof = TRUE;
    hp->just_lseeked = FALSE;

    hp->buf_len += bytes_read;

    return hp->buf_len >= atleast;
}

#define buf_atleast(archive, hp, atleast) \
    (((hp)->buf_len >= (atleast))? TRUE : buf_atleast_((archive), (hp), (atleast)))

/* Skip the buffer ahead by SKIPBYTES bytes.  This will discard data from the
 * buffer, and may call lseek() if some of the skipped bytes have not yet been
 * read.  Returns FALSE if the requisite bytes cannot be skipped due to EOF or
 * another error. */
static gboolean
buf_skip_(
    amar_t *archive,
    handling_params_t *hp,
    gsize skipbytes)
{
    /* easy case of buf_len > skipbytes is taken care of by the macro, below,
     * so we know we're clearing out the entire buffer here */

    skipbytes -= hp->buf_len;
    hp->buf_len = 0;

    hp->buf_offset = 0;

retry:
    if (archive->seekable) {
	if (lseek(archive->fd, skipbytes, SEEK_CUR) < 0) {
	    /* did we fail because archive->fd is a pipe or something? */
	    if (errno == ESPIPE) {
		archive->seekable = FALSE;
		goto retry;
	    }
	    hp->got_eof = TRUE;
	    return FALSE;
	}
    } else {
	while (skipbytes) {
	    gsize toread = MIN(skipbytes, hp->buf_size);
	    gsize bytes_read = full_read(archive->fd, hp->buf, toread);

	    if (bytes_read < toread) {
		hp->got_eof = TRUE;
		return FALSE;
	    }

	    skipbytes -= bytes_read;
	}
    }

    return TRUE;
}

#define buf_skip(archive, hp, skipbytes) \
    (((skipbytes) <= (hp)->buf_len) ? \
	((hp)->buf_len -= (skipbytes), \
	 (hp)->buf_offset += (skipbytes), \
	 TRUE) \
      : buf_skip_((archive), (hp), (skipbytes)))

/* Get a pointer to the current position in the buffer */
#define buf_ptr(hp) ((hp)->buf + (hp)->buf_offset)

/* Get the amount of data currently available in the buffer */
#define buf_avail(hp) ((hp)->buf_len)

static gboolean
finish_attr(
    handling_params_t *hp,
    file_state_t *fs,
    attr_state_t *as,
    gboolean truncated)
{
    gboolean success = TRUE;
    if (!as->wrote_eoa && as->handling && as->handling->callback) {
	success = as->handling->callback(hp->user_data, fs->filenum,
			fs->file_data, as->attrid, as->handling->attrid_data,
			&as->attr_data, as->buf, as->buf_len, TRUE, truncated);
    }
    amfree(as->buf);
    amfree(as);

    return success;
}

static gboolean
finish_file(
    handling_params_t *hp,
    file_state_t *fs,
    gboolean truncated)
{
    GSList *iter;
    gboolean success = TRUE;

    /* free up any attributes not yet ended */
    for (iter = fs->attr_states; iter; iter = iter->next) {
	attr_state_t *as = (attr_state_t *)iter->data;
	success = success && finish_attr(hp, fs, as, TRUE);
    }
    g_slist_free(fs->attr_states);
    fs->attr_states = NULL;

    if (hp->file_finish_cb && !fs->ignore)
	success = success && hp->file_finish_cb(hp->user_data, fs->filenum, &fs->file_data, truncated);

    amfree(fs);
    return success;
}

/* buffer the data and/or call the callback for this attribute */
static gboolean
handle_hunk(
    handling_params_t *hp,
    file_state_t *fs,
    attr_state_t *as,
    amar_attr_handling_t *hdl,
    gpointer buf,
    gsize len,
    gboolean eoa)
{
    gboolean success = TRUE;

    /* capture any conditions where we don't have to copy into the buffer */
    if (hdl->min_size == 0 || (as->buf_len == 0 && len >= hdl->min_size)) {
	success = success && hdl->callback(hp->user_data, fs->filenum,
		fs->file_data, as->attrid, hdl->attrid_data, &as->attr_data,
		buf, len, eoa, FALSE);
	as->wrote_eoa = eoa;
    } else {
	/* ok, copy into the buffer */
	if (as->buf_len + len > as->buf_size) {
	    gpointer newbuf = g_malloc(as->buf_len + len);
	    if (as->buf) {
		memcpy(newbuf, as->buf, as->buf_len);
		g_free(as->buf);
	    }
	    as->buf = newbuf;
	    as->buf_size = as->buf_len + len;
	}
	memcpy(as->buf + as->buf_len, buf, len);
	as->buf_len += len;

	/* and call the callback if we have enough data or if this is the last attr */
	if (as->buf_len >= hdl->min_size || eoa) {
	    success = success && hdl->callback(hp->user_data, fs->filenum,
		    fs->file_data, as->attrid, hdl->attrid_data, &as->attr_data,
		    as->buf, as->buf_len, eoa, FALSE);
	    as->buf_len = 0;
	    as->wrote_eoa = eoa;
	}
    }

    return success;
}

gboolean
amar_read(
	amar_t *archive,
	gpointer user_data,
	amar_attr_handling_t *handling_array,
	amar_file_start_callback_t file_start_cb,
	amar_file_finish_callback_t file_finish_cb,
	GError **error)
{
    file_state_t *fs = NULL;
    attr_state_t *as = NULL;
    GSList *iter;
    handling_params_t hp;
    uint16_t filenum;
    uint16_t attrid;
    uint32_t datasize;
    gboolean eoa;
    amar_attr_handling_t *hdl;
    gboolean success = TRUE;

    g_assert(archive->mode == O_RDONLY);

    hp.user_data = user_data;
    hp.handling_array = handling_array;
    hp.file_start_cb = file_start_cb;
    hp.file_finish_cb = file_finish_cb;
    hp.file_states = NULL;
    hp.buf_len = 0;
    hp.buf_offset = 0;
    hp.buf_size = 1024; /* use a 1K buffer to start */
    hp.buf = g_malloc(hp.buf_size);
    hp.got_eof = FALSE;
    hp.just_lseeked = FALSE;

    /* check that we are starting at a header record, but don't advance
     * the buffer past it */
    if (buf_atleast(archive, &hp, RECORD_SIZE)) {
	GETRECORD(buf_ptr(&hp), filenum, attrid, datasize, eoa);
	if (filenum != MAGIC_FILENUM) {
	    g_set_error(error, amar_error_quark(), EINVAL,
			"Archive read does not begin at a header record");
	    return FALSE;
	}
    }

    while (1) {
	if (!buf_atleast(archive, &hp, RECORD_SIZE))
	    break;

	GETRECORD(buf_ptr(&hp), filenum, attrid, datasize, eoa);

	/* handle headers specially */
	if (G_UNLIKELY(filenum == MAGIC_FILENUM)) {
	    int vers;

	    /* bail if an EOF occurred in the middle of the header */
	    if (!buf_atleast(archive, &hp, HEADER_SIZE))
		break;

	    if (sscanf(buf_ptr(&hp), HEADER_MAGIC " %d", &vers) != 1) {
		g_set_error(error, amar_error_quark(), EINVAL,
			    "Invalid archive header");
		return FALSE;
	    }

	    if (vers > HEADER_VERSION) {
		g_set_error(error, amar_error_quark(), EINVAL,
			    "Archive version %d is not supported", vers);
		return FALSE;
	    }

	    buf_skip(archive, &hp, HEADER_SIZE);

	    continue;
	}

	buf_skip(archive, &hp, RECORD_SIZE);

	if (datasize > MAX_RECORD_DATA_SIZE) {
	    g_set_error(error, amar_error_quark(), EINVAL,
			"Invalid record: data size must be less than %d",
			MAX_RECORD_DATA_SIZE);
	    return FALSE;
	}

	/* find the file_state_t, if it exists */
	if (!fs || fs->filenum != filenum) {
	    fs = NULL;
	    for (iter = hp.file_states; iter; iter = iter->next) {
		if (((file_state_t *)iter->data)->filenum == filenum) {
		    fs = (file_state_t *)iter->data;
		    break;
		}
	    }
	}

	/* get the "special" attributes out of the way */
	if (G_UNLIKELY(attrid < AMAR_ATTR_APP_START)) {
	    if (attrid == AMAR_ATTR_EOF) {
		if (datasize != 0) {
		    g_set_error(error, amar_error_quark(), EINVAL,
				"Archive contains an EOF record with nonzero size");
		    return FALSE;
		}
		if (fs) {
		    success = finish_file(&hp, fs, FALSE);
		    hp.file_states = g_slist_remove(hp.file_states, fs);
		    as = NULL;
		    fs = NULL;
		    if (!success)
			break;
		}
		continue;
	    } else if (attrid == AMAR_ATTR_FILENAME) {
		/* for filenames, we need the whole filename in the buffer */
		if (!buf_atleast(archive, &hp, datasize))
		    break;

		if (fs) {
		    /* TODO: warn - previous file did not end correctly */
		    success = finish_file(&hp, fs, TRUE);
		    hp.file_states = g_slist_remove(hp.file_states, fs);
		    as = NULL;
		    fs = NULL;
		    if (!success)
			break;
		}

		if (!datasize) {
		    unsigned int i, nul_padding = 1;
		    char *bb;
		    /* try to detect NULL padding bytes */
		    if (!buf_atleast(archive, &hp, 512 - RECORD_SIZE)) {
			/* close to end of file */
			break;
		    }
		    bb = buf_ptr(&hp);
		    /* check all byte == 0 */
		    for (i=0; i<512 - RECORD_SIZE; i++) {
			if (*bb++ != 0)
			    nul_padding = 0;
		    }
		    if (nul_padding) {
			break;
		    }
		    g_set_error(error, amar_error_quark(), EINVAL,
				"Archive file %d has an empty filename",
				(int)filenum);
		    return FALSE;
		}

		if (!eoa) {
		    g_set_error(error, amar_error_quark(), EINVAL,
				"Filename record for fileid %d does "
				"not have its EOA bit set", (int)filenum);
		    return FALSE;
		}

		fs = g_new0(file_state_t, 1);
		fs->filenum = filenum;
		hp.file_states = g_slist_prepend(hp.file_states, fs);

		if (hp.file_start_cb) {
		    success = hp.file_start_cb(hp.user_data, filenum,
			    buf_ptr(&hp), datasize,
			    &fs->ignore, &fs->file_data);
		    if (!success)
			break;
		}

		buf_skip(archive, &hp, datasize);

		continue;
	    } else {
		g_set_error(error, amar_error_quark(), EINVAL,
			    "Unknown attribute id %d in archive file %d",
			    (int)attrid, (int)filenum);
		return FALSE;
	    }
	}

	/* if this is an unrecognized file or a known file that's being
	 * ignored, then skip it. */
	if (!fs || fs->ignore) {
	    buf_skip(archive, &hp, datasize);
	    continue;
	}

	/* ok, this is an application attribute.  Look up its as, if it exists. */
	if (!as || as->attrid != attrid) {
	    as = NULL;
	    for (iter = fs->attr_states; iter; iter = iter->next) {
		if (((attr_state_t *)(iter->data))->attrid == attrid) {
		    as = (attr_state_t *)(iter->data);
		    break;
		}
	    }
	}

	/* and get the proper handling for that attribute */
	if (as) {
	    hdl = as->handling;
	} else {
	    hdl = hp.handling_array;
	    for (hdl = hp.handling_array; hdl->attrid != 0; hdl++) {
		if (hdl->attrid == attrid)
		    break;
	    }
	}

	/* As a shortcut, if this is a one-record attribute, handle it without
	 * creating a new attribute_state_t. */
	if (eoa && !as) {
	    gpointer tmp = NULL;
	    if (hdl->callback) {
		/* a simple single-part callback */
		if (buf_avail(&hp) >= datasize) {
		    success = hdl->callback(hp.user_data, filenum, fs->file_data, attrid,
			    hdl->attrid_data, &tmp, buf_ptr(&hp), datasize, eoa, FALSE);
		    if (!success)
			break;
		    buf_skip(archive, &hp, datasize);
		    continue;
		}

		/* we only have part of the data, but if it's big enough to exceed
		 * the attribute's min_size, then just call the callback for each
		 * part of the data */
		else if (buf_avail(&hp) >= hdl->min_size) {
		    gsize firstpart = buf_avail(&hp);
		    gsize lastpart = datasize - firstpart;

		    success = hdl->callback(hp.user_data, filenum, fs->file_data, attrid,
			    hdl->attrid_data, &tmp, buf_ptr(&hp), firstpart, FALSE, FALSE);
		    if (!success)
			break;
		    buf_skip(archive, &hp, firstpart);

		    if (!buf_atleast(archive, &hp, lastpart))
			break;

		    success = hdl->callback(hp.user_data, filenum, fs->file_data, attrid,
			    hdl->attrid_data, &tmp, buf_ptr(&hp), lastpart, eoa, FALSE);
		    if (!success)
			break;
		    buf_skip(archive, &hp, lastpart);
		    continue;
		}
	    } else {
		/* no callback -> just skip it */
		buf_skip(archive, &hp, datasize);
		continue;
	    }
	}

	/* ok, set up a new attribute state */
	if (!as) {
	    as = g_new0(attr_state_t, 1);
	    as->attrid = attrid;
	    as->handling = hdl;
	    fs->attr_states = g_slist_prepend(fs->attr_states, as);
	}

	if (hdl->callback) {
	    /* handle the data as one or two hunks, depending on whether it's
	     * all in the buffer right now */
	    if (buf_avail(&hp) >= datasize) {
		success = handle_hunk(&hp, fs, as, hdl, buf_ptr(&hp), datasize, eoa);
		if (!success)
		    break;
		buf_skip(archive, &hp, datasize);
	    } else {
		gsize hunksize = buf_avail(&hp);
		success = handle_hunk(&hp, fs, as, hdl, buf_ptr(&hp), hunksize, FALSE);
		if (!success)
		    break;
		buf_skip(archive, &hp, hunksize);

		hunksize = datasize - hunksize;
		if (!buf_atleast(archive, &hp, hunksize))
		    break;

		handle_hunk(&hp, fs, as, hdl, buf_ptr(&hp), hunksize, eoa);
		buf_skip(archive, &hp, hunksize);
	    }
	} else {
	    buf_skip(archive, &hp, datasize);
	}

	/* finish the attribute if this is its last record */
	if (eoa) {
	    success = finish_attr(&hp, fs, as, FALSE);
	    fs->attr_states = g_slist_remove(fs->attr_states, as);
	    if (!success)
		break;
	    as = NULL;
	}
    }

    /* close any open files, assuming that they have been truncated */

    for (iter = hp.file_states; iter; iter = iter->next) {
	file_state_t *fs = (file_state_t *)iter->data;
	finish_file(&hp, fs, TRUE);
    }
    g_slist_free(hp.file_states);
    g_free(hp.buf);

    return success;
}
