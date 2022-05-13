/*
 * Copyright (c) 2008-2012 Zmanda, Inc.  All Rights Reserved.
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
 * Sunnyvale, CA 94085, or: http://www.zmanda.com
 */

#ifndef __S3_UTIL_H__
#define __S3_UTIL_H__

#ifdef HAVE_REGEX_H
#  ifdef HAVE_SYS_TYPES_H
#  include <sys/types.h>
#  endif
#include <regex.h>
#endif
#include <glib.h>

/*
 * Constants
 */

/* number of raw bytes in MD5 hash */
#define S3_MD5_HASH_BYTE_LEN 16
/* length of an MD5 hash encoded as base64 (not including terminating NULL) */
#define S3_MD5_HASH_B64_LEN 25
/* length of an MD5 hash encoded as hexadecimal (not including terminating NULL) */
#define S3_MD5_HASH_HEX_LEN 32

// NOTE: minmum size part allowed for AWS/S3 multipart
#define BUFFER_SIZE_MAX_DEFAULT (5*1024*1024)
/*
 * Types
 */

#ifndef HAVE_REGEX_H
typedef GRegex* regex_t;

typedef enum
{
    REG_NOERROR = 0,      /* Success.  */
    REG_NOMATCH          /* Didn't find a match (for regexec).  */
} reg_errcode_t;
#endif

// NOTE: kept identical to s3.h
typedef guint64 objbytes_t;
typedef guint64 xferbytes_t;
typedef gint64 signed_xferbytes_t;
typedef guint32 membytes_t;
typedef gint32 signed_membytes_t;

/*
 * Functions
 */

#ifndef USE_GETTEXT
/* we don't use gettextize, so hack around this ... */
#undef _
#define _(str) (str)
#endif

/*
 * Wrapper around regexec to handle programmer errors.
 * Only returns if the regexec returns 0 (match) or REG_NOMATCH.
 * See regexec(3) documentation for the rest.
 */
int
s3_regexec_wrap(regex_t *regex,
           const char *str,
           size_t nmatch,
           regmatch_t pmatch[],
           int eflags);

#ifndef HAVE_AMANDA_H
char*
find_regex_substring(const char* base_string,
           const regmatch_t match);
#endif

/*
 * Encode bytes using hexadecimal
 *
 * @param to_enc: The data to encode.
 * @returns:  A new, null-terminated string or NULL if to_enc is NULL.
 */
gchar*
s3_hex_encode(const GByteArray *to_enc);

/*
 * Compute the MD5 hash of a blob of data.
 *
 * @param to_hash: The data to compute the hash for.
 * @returns:  A new GByteArray containing the MD5 hash of data or
 * NULL if to_hash is NULL.
 */
GByteArray*
s3_compute_md5_hash(const GByteArray *to_hash, const GByteArray *to_hash_ext);

GByteArray*
s3_compute_sha256_hash(const GByteArray *to_hash, const GByteArray *to_hash_ext);

char *
s3_compute_sha256_hash_ptr(const unsigned char *to_hash, gsize len);

unsigned char *
EncodeHMACSHA256(
    unsigned char* key,
    size_t keylen,
    const char* data,
    size_t datalen);

unsigned char *
s3_tohex(unsigned char *s, size_t len_s);

/* These functions are for if you want to use curl on your own. You get more
 * control, but it's a lot of work that way:
 */
/* circle buffer
 * buffer: pointer to the buffer memory 
 * buffer_end: indice of the last+1 (latest) filled byte in the buffer
 * buffer_pos: indice of the first (earliest) filled byte in the buffer
 * max_buffer_size: desired or real allocated size of the buffer
 * mutex: !NULL
 * cond: !NULL
 *
 * buffer_end == buffer_pos: buffer is empty
 * The data in use are
 *     buffer_end > buffer_pos: from buffer_pos to buffer_end
 *     buffer_end < buffer_pos: from buffer_pos to max_buffer_size
 *                          and from 0 to buffer_end
 */
typedef struct {
    char *buffer;
    	     membytes_t max_buffer_size; // mostly fixed
    volatile membytes_t buffer_end;      // moves upon writes [use mutex only]
    volatile membytes_t buffer_pos;      // moves upon reads [use mutex and atomic reads]
    volatile membytes_t bytes_filled;

    guint8           opt_verbose:1; // copied from S3Device.verbose
    guint8           opt_padded:1;  // reads-after-eod get memset() of zeroes
    volatile guint8  cancel;  // (safer than resetting mutex/cond)
    volatile guint8  noreset; // cannot rewind/reset to start...
    volatile guint8  waiting; // a thread is blocked until other operation

    volatile objbytes_t rdbytes_to_eod; // counts down for reads only
    GMutex   *mutex;
    GCond    *cond;
    GMutex   _mutex_store;
    GCond    _cond_store;
    void     *hash_ctxt;
} CurlBuffer;

membytes_t
s3_buffer_ctor(CurlBuffer *s3buf, xferbytes_t allocate, gboolean verbose);

membytes_t
s3_buffer_init(CurlBuffer *s3buf, xferbytes_t allocate, gboolean verbose);

membytes_t
s3_buffer_load(CurlBuffer *s3buf, gpointer buffer, membytes_t size, gboolean verbose);

membytes_t
s3_buffer_load_string(CurlBuffer *s3buf, char *buffer, gboolean verbose);

void
s3_buffer_destroy(CurlBuffer *s3buf);


static inline void
s3_buffer_lock(const CurlBuffer *s3buf) { if(s3buf->mutex) g_mutex_lock(s3buf->mutex); } 

static inline gboolean
s3_buffer_trylock(const CurlBuffer *s3buf) { return (s3buf->mutex ? g_mutex_trylock(s3buf->mutex) : 1 ); }

static inline void
s3_buffer_lock_assert(const CurlBuffer *s3buf) { g_assert( !s3buf->mutex || g_mutex_trylock(s3buf->mutex) ); } 

static inline void
s3_buffer_prelocked_assert(const CurlBuffer *s3buf) { g_assert( !s3buf->mutex || !g_mutex_trylock(s3buf->mutex) ); } 

static inline void
s3_buffer_unlock(const CurlBuffer *s3buf) { if (s3buf->mutex) g_mutex_unlock(s3buf->mutex); } 

char *
s3_buffer_data_func(const CurlBuffer *s3buf);

typedef xferbytes_t (*device_size_func_t)(void *);
typedef GByteArray* (*device_hash_func_t)(void *);
typedef void        (*device_reset_func_t)(void *);

xferbytes_t
s3_buffer_size_func(const CurlBuffer *s3buf);

// the linear-size from the start of the buffer (must be >0 if any data present)
xferbytes_t
s3_buffer_lsize_func(const CurlBuffer *s3buf);

xferbytes_t
s3_buffer_read_size_func(const CurlBuffer *s3buf);

gboolean
s3_buffer_eod_func(const CurlBuffer *s3buf);


#define S3_BUFFER_READ_FUNCS s3_buffer_read_func, (device_reset_func_t) s3_buffer_read_reset_func, (device_size_func_t) s3_buffer_read_size_func, (device_hash_func_t) s3_buffer_md5_func

#define S3_BUFFER_WRITE_FUNCS s3_buffer_write_func, (device_reset_func_t) s3_buffer_write_reset_func

/* a CURLOPT_READFUNCTION to read data from a buffer. */
xferbytes_t
s3_buffer_read_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void *stream);

GByteArray*
s3_buffer_md5_func(CurlBuffer *s3buf);

GByteArray*
s3_buffer_sha256_func(CurlBuffer *s3buf);

void
s3_buffer_reset_eod_func(CurlBuffer *stream, objbytes_t rdbytes); // reset only if buffer was full/single-use

void
s3_buffer_read_reset_func(CurlBuffer *s3buf); // reset only if buffer was full/single-use

void
s3_buffer_write_reset_func(CurlBuffer *s3buf); // reset to empty

#define S3_EMPTY_READ_FUNCS s3_empty_read_func, NULL, (device_size_func_t) s3_empty_size_func, s3_empty_md5_func

/* a CURLOPT_WRITEFUNCTION to write data to a buffer. */
xferbytes_t
s3_buffer_write_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void *stream);

membytes_t
s3_buffer_write_padding(CurlBuffer *s3buf);

int
s3_buffer_fill_wait(membytes_t fill_size, CurlBuffer *stream);

/* a CURLOPT_READFUNCTION that writes nothing. */
xferbytes_t
s3_empty_read_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void * stream);

xferbytes_t
s3_empty_size_func(void *stream);

GByteArray*
s3_empty_md5_func(void *stream);

#define S3_COUNTER_WRITE_FUNCS s3_counter_write_func, s3_counter_reset_func

/* a CURLOPT_WRITEFUNCTION to write data that just counts data.
 * s3_write_data should be NULL or a pointer to an gint64.
 */
xferbytes_t
s3_counter_write_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void *stream);

void
s3_counter_reset_func(void *stream);

#ifdef _WIN32
/* a CURLOPT_READFUNCTION to read data from a file. */
xferbytes_t
s3_file_read_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void * stream);

xferbytes_t
s3_file_size_func(void *stream);

GByteArray*
s3_file_md5_func(void *stream);

xferbytes_t
s3_file_reset_func(void *stream);

/* a CURLOPT_WRITEFUNCTION to write data to a file. */
xferbytes_t
s3_file_write_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void *stream);
#endif

#endif
