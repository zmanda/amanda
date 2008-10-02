/*
 * Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as 
 * published by the Free Software Foundation.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 * 
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include <sys/types.h>
#include <regex.h>
#include <glib.h>
#include <openssl/md5.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/bn.h>
#include "amanda.h"
#include "s3-util.h"

int
s3_regexec_wrap(regex_t *regex,
           const char *str,
           size_t nmatch,
           regmatch_t pmatch[],
           int eflags)
{
    char *message;
    int size;
    int reg_result;

    reg_result = regexec(regex, str, nmatch, pmatch, eflags);
    if (reg_result != 0 && reg_result != REG_NOMATCH) {
        size = regerror(reg_result, regex, NULL, 0);
        message = g_malloc(size);
        regerror(reg_result, regex, message, size);

        /* this is programmer error (bad regexp), so just log
         * and abort().  There's no good way to signal a
         * permanaent error from interpret_response. */
        g_critical(_("Regex error: %s"), message);
    }

    return reg_result;
}

gchar*
s3_base64_encode(const GByteArray *to_enc) {
    BIO *bio_b64 = NULL, *bio_buff = NULL;
    long bio_b64_len;
    char *bio_b64_data = NULL, *ret = NULL;
    if (!to_enc) return NULL;
    
    /* Initialize base64 encoding filter */
    bio_b64 = BIO_new(BIO_f_base64());
    g_assert(bio_b64);
    BIO_set_flags(bio_b64, BIO_FLAGS_BASE64_NO_NL);

    /* Initialize memory buffer for the base64 encoding */
    bio_buff = BIO_new(BIO_s_mem());
    g_assert(bio_buff);
    bio_buff = BIO_push(bio_b64, bio_buff);

    /* Write the MD5 hash into the buffer to encode it in base64 */
    BIO_write(bio_buff, to_enc->data, to_enc->len);
    /* BIO_flush is a macro and GCC 4.1.2 complains without this cast*/
    (void) BIO_flush(bio_buff);

    /* Pull out the base64 encoding of the MD5 hash */
    bio_b64_len = BIO_get_mem_data(bio_buff, &bio_b64_data);
    g_assert(bio_b64_data);
    ret = g_strndup(bio_b64_data, bio_b64_len);

    /* If bio_b64 is freed separately, freeing bio_buff will 
     * invalidly free memory and potentially segfault.
     */
    BIO_free_all(bio_buff);
    return ret;
}

gchar*
s3_hex_encode(const GByteArray *to_enc)  {
    guint i;
    gchar *ret = NULL, table[] = "0123456789abcdef";
    if (!to_enc) return NULL;

    ret = g_new(gchar, to_enc->len*2 + 1);
    for (i = 0; i < to_enc->len; i++) {
        /* most significant 4 bits */
        ret[i*2] = table[to_enc->data[i] >> 4];
        /* least significant 4 bits */
        ret[i*2 + 1] = table[to_enc->data[i] & 0xf];
    }
    ret[to_enc->len*2] = '\0';

    return ret;
}

GByteArray*
s3_compute_md5_hash(const GByteArray *to_hash) {
    MD5_CTX md5_ctx;
    GByteArray *ret;
    if (!to_hash) return NULL;
    
    ret = g_byte_array_sized_new(S3_MD5_HASH_BYTE_LEN);
    g_byte_array_set_size(ret, S3_MD5_HASH_BYTE_LEN);
    
    MD5_Init(&md5_ctx);
    MD5_Update(&md5_ctx, to_hash->data, to_hash->len);
    MD5_Final(ret->data, &md5_ctx);
    
    return ret;
}
