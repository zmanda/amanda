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

/*
 * Types
 */

#ifndef HAVE_REGEX_H
typedef GRegex* regex_t;

typedef gint regoff_t;
typedef struct
{
    regoff_t rm_so;  /* Byte offset from string's start to substring's start.  */
    regoff_t rm_eo;  /* Byte offset from string's start to substring's end.  */
} regmatch_t;

typedef enum
{
    REG_NOERROR = 0,      /* Success.  */
    REG_NOMATCH          /* Didn't find a match (for regexec).  */
} reg_errcode_t;
#endif

/*
 * Functions
 */

#ifndef USE_GETTEXT
/* we don't use gettextize, so hack around this ... */
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
 * Encode bytes using Base-64
 *
 * @note: GLib 2.12+ has a function for this (g_base64_encode)
 *     but we support much older versions. gnulib does as well, but its
 *     hard to use correctly (see its notes).
 *
 * @param to_enc: The data to encode.
 * @returns:  A new, null-terminated string or NULL if to_enc is NULL.
 */
gchar*
s3_base64_encode(const GByteArray *to_enc);

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
s3_compute_md5_hash(const GByteArray *to_hash);

#endif
