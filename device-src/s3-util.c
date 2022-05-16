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


#ifdef HAVE_CONFIG_H
/* use a relative path here to avoid conflicting with Perl's config.h. */
#include "../config/config.h"
#endif

#include "s3-util.h"
#include "amanda.h"

#ifdef HAVE_REGEX_H
#include <regex.h>
#endif

#include <curl/curl.h>
#include <glib.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/md5.h>
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/bn.h>
#include <sys/types.h>

#ifdef HAVE_REGEX_H
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
#else

int
s3_regexec_wrap(regex_t *regex,
           const char *str,
           size_t nmatch,
           regmatch_t pmatch[],
           int eflags)
{
    GMatchInfo *match_info;
    int ret = REG_NOERROR;
    guint i;

    g_assert(regex && *regex);
    g_regex_match(*regex, str, eflags, &match_info);
    if (g_match_info_matches(match_info)) {
        g_assert(g_match_info_get_match_count(match_info) <= (glong) nmatch);
        for (i = 0; i < nmatch; i++) {
            pmatch[i].rm_eo = pmatch[i].rm_so = -1;
            g_match_info_fetch_pos(match_info, i, &pmatch[i].rm_so, &pmatch[i].rm_eo);
        }
    } else {
        ret = REG_NOMATCH;
    }
    g_match_info_free(match_info);
    return ret;
}
#endif

gchar*
s3_hex_encode(const GByteArray *to_enc)  
{
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

static GByteArray*
compute_hash(GChecksumType type, const GByteArray *to_hash, const GByteArray *to_hash_ext) 
{
    GChecksum *sum = NULL;
    gsize sumlen = g_checksum_type_get_length(type);
    GByteArray *ret;

    if (!to_hash) return NULL;

    sum = g_checksum_new(type);
    ret = g_byte_array_sized_new(sumlen); // allocate
    g_byte_array_set_size(ret, sumlen); // lengthen

    g_checksum_update(sum, to_hash->data, to_hash->len);
    if ( to_hash_ext && to_hash_ext->len )
        g_checksum_update(sum, to_hash_ext->data, to_hash_ext->len);

    g_checksum_get_digest(sum, ret->data, &sumlen);
    g_checksum_free(sum);

    return ret;
}

static char *
compute_hash_ptr(GChecksumType type, const guchar *to_hash, int len)
{
    GChecksum *sum = NULL;
    char *ret = NULL;

    if (!to_hash || !len) {
        to_hash = (const guchar*)"";
        len = 0;
    }

    sum = g_checksum_new(type);
    g_checksum_update(sum, to_hash, len);
    ret = g_strdup(g_checksum_get_string(sum));
    g_checksum_free(sum);

    return ret;
}


GByteArray*
s3_compute_md5_hash(const GByteArray *to_hash, const GByteArray *to_hash_ext) 
{
    return compute_hash(G_CHECKSUM_MD5, to_hash, to_hash_ext);
}

GByteArray*
s3_compute_sha256_hash(const GByteArray *to_hash, const GByteArray *to_hash_ext) 
{
    return compute_hash(G_CHECKSUM_SHA256, to_hash, to_hash_ext);
}

char *
s3_compute_sha256_hash_ptr(const guchar *to_hash, size_t len)
{
    return compute_hash_ptr(G_CHECKSUM_SHA256, to_hash, len);
}

unsigned char *
EncodeHMACSHA256(
    unsigned char* key,
    size_t keylen,
    const char* data,
    size_t datalen)
{
    unsigned char *hmachash = malloc(32);
    const unsigned char *datatohash = (unsigned char *)data;
    unsigned char tk[SHA256_DIGEST_LENGTH];

    // Initialise HMACh
#if (defined OPENSSL_VERSION_NUMBER && OPENSSL_VERSION_NUMBER < 0x10100000L) \
    || defined LIBRESSL_VERSION_NUMBER

    HMAC_CTX HMAC;
#else
    HMAC_CTX *HMAC;
#endif
    unsigned int hmaclength = 32;
    memset(hmachash, 0, hmaclength);

    if (keylen > 64 ) {
	SHA256(key, keylen, tk);
	key    = tk;
	keylen = SHA256_DIGEST_LENGTH;
    }

    // Digest the key and message using SHA256
#if (defined OPENSSL_VERSION_NUMBER && OPENSSL_VERSION_NUMBER < 0x10100000L) \
    || defined LIBRESSL_VERSION_NUMBER
    HMAC_CTX_init(&HMAC);
    HMAC_Init_ex(&HMAC, key, keylen, EVP_sha256(),NULL);
    HMAC_Update(&HMAC, datatohash, datalen);
    HMAC_Final(&HMAC, hmachash, &hmaclength);
    HMAC_CTX_cleanup(&HMAC);
#else
    HMAC = HMAC_CTX_new();
    HMAC_CTX_reset(HMAC);
    HMAC_Init_ex(HMAC, key, keylen, EVP_sha256(),NULL);
    HMAC_Update(HMAC, datatohash, datalen);
    HMAC_Final(HMAC, hmachash, &hmaclength);
    HMAC_CTX_free(HMAC);
#endif

    return hmachash;
}

unsigned char *
s3_tohex(
    unsigned char *s,
    size_t len_s)
{
    unsigned char *r = malloc(len_s*2+1);
    unsigned char *t = r;
    size_t   i;
    gchar table[] = "0123456789abcdef";

    for (i = 0; i < len_s; i++) {
	/* most significant 4 bits */
	*t++ = table[s[i] >> 4];
	/* least significant 4 bits */
	*t++ = table[s[i] & 0xf];
    }
    *t = '\0';
    return r;
}

membytes_t
s3_buffer_init(CurlBuffer *s3buf, xferbytes_t allocate, gboolean verbose)
{
    // size of zero implies requesting same-size-as-before
    // size of 1 implies "dealloc any buffer"
    xferbytes_t bufsize = ( allocate ? : s3buf->max_buffer_size ); // limited if allocate was too huge

    // decrement to 0 if exactly 1
    if (bufsize == 1) --bufsize;

    if (bufsize >= (G_MAXINT32/2) ) // if accidentally > 1GB or any negative 32-bit int?
        bufsize = BUFFER_SIZE_MAX_DEFAULT; // safe minimum allocated size in its place

    // must lock.. even if not needed
    g_mutex_lock(&s3buf->_mutex_store); // wait for a chance
   
    // ALT1: clear whatever we have...
    if (!bufsize) {
	g_free((char *)s3buf->buffer);
	if (verbose && s3buf->buffer)
	    g_debug("[%p] %s: freed buffer=%p orig=[%#x]", s3buf, __FUNCTION__, s3buf->buffer, s3buf->max_buffer_size);
	else if (verbose)
	    g_debug("[%p] %s: recheck-none buffer=%p", s3buf, __FUNCTION__, s3buf->buffer);
	s3buf->max_buffer_size = 0;
	s3buf->buffer = NULL;
    }
    // ALT2: content with whatever size we have
    else if (s3buf->max_buffer_size > bufsize)
    {
        g_assert(s3buf->buffer);
	if (verbose)
	    g_debug("[%p] %s: retain buffer=%p desired/stored=[%#lx/%#x]", s3buf, __FUNCTION__, s3buf->buffer, 
	    	bufsize, s3buf->max_buffer_size);
	// s3buf->buffer[0] = '\0';
	// s3buf->buffer[s3buf->max_buffer_size-1] = '\0';
    } 
    // ALT3: must newly allocate memory for the new size
    else
    {
        g_free((char *)s3buf->buffer); // NULL or not...
	if (verbose)
	    g_debug("[%p] %s: new-alloc buffer=%p alloc=[%#lx] orig=[%#x]", s3buf, __FUNCTION__,
	    	s3buf->buffer, bufsize, s3buf->max_buffer_size);

	s3buf->buffer = g_try_malloc(bufsize);

	if (!s3buf->buffer) {
	    g_error("[%p] %s: FAILURE new-alloc buffer=%p alloc=[%#lx]", s3buf, __FUNCTION__,
	    	s3buf->buffer, bufsize);
	    return 0;
	}
	// s3buf->buffer[0] = '\0';
	// s3buf->buffer[bufsize] = '\0';
	s3buf->max_buffer_size = bufsize;
    }

    // setup transfer
    s3buf->buffer_pos = 0; // nothing in buffer yet
    s3buf->buffer_end = 0; // nothing in buffer yet

    if (s3buf->rdbytes_to_eod)
        g_warning("[%p] %s: reset eod to zero [%#lx]", s3buf, __FUNCTION__, s3buf->rdbytes_to_eod);

    s3buf->rdbytes_to_eod = 0;
    s3buf->noreset = FALSE;
 
    s3buf->bytes_filled = 0; // nothing yet

    s3buf->mutex = &s3buf->_mutex_store;
    s3buf->cond = &s3buf->_cond_store;

    s3buf->opt_verbose = verbose;
    s3buf->opt_padded = FALSE; // must be set explicitly
    s3buf->cancel = FALSE;

    // must unlock
    g_mutex_unlock(&s3buf->_mutex_store); // cleaned up

    return s3buf->max_buffer_size;
}

// from raw memory, if needed...
membytes_t
s3_buffer_ctor(CurlBuffer *s3buf, xferbytes_t allocate, gboolean verbose)
{
    memset(s3buf,'\0',sizeof(*s3buf));

    g_mutex_init(&s3buf->_mutex_store);
    g_cond_init(&s3buf->_cond_store);
    return s3_buffer_init(s3buf,allocate,verbose);
}

// reset the position of the read-eod
void
s3_buffer_reset_eod_func(CurlBuffer *stream, objbytes_t rdbytes)
{
    CurlBuffer *const s3buf = stream;
    s3buf->rdbytes_to_eod = rdbytes; 
}

void
s3_buffer_destroy(CurlBuffer *s3buf)
{
    GMutex *mutex = s3buf->mutex;
    GCond *cond = s3buf->cond;
    char *buffer = s3buf->buffer;

    if (s3buf->opt_verbose)
        g_debug("[%p] %s: buffer=%p filled=[%#x]", s3buf, __FUNCTION__, s3buf->buffer, s3buf->bytes_filled);

    // acquire last atomic-grab of mutex/cond pair [if present]...
    if ( mutex && cond ) {
        g_mutex_lock(mutex);
    } else {
        mutex = NULL; // dont touch mutex
        cond = NULL; // dont touch cond
    }

    // notify waiters to cancel!
    if (mutex && s3buf->waiting) {
        g_atomic_pointer_compare_and_exchange(&s3buf->mutex, mutex, NULL);
        g_atomic_pointer_compare_and_exchange(&s3buf->cond, cond, NULL);
        g_cond_broadcast(cond); // allow waiting threads to detect cleanup
    }

    buffer = s3buf->buffer;
    // atomic grab of buffer pointer just in case
    if (buffer) {
        // if buffer is non-NULL and grabbed out
        if (g_atomic_pointer_compare_and_exchange(&s3buf->buffer, buffer, NULL))
            g_free((char *)buffer);
	else {
            g_warning("[%p] %s: could not free buffer buffer=%p filled=[%#x]", s3buf, __FUNCTION__, s3buf->buffer, s3buf->bytes_filled);
	}
    }

    if ( mutex && cond ) {
        // g_mutex_trylock(mutex); // just in case..
        g_mutex_unlock(mutex);
        g_cond_clear(cond);
        g_mutex_clear(mutex);
    }

    memset(s3buf, '\0', sizeof(*s3buf)); // wipe all the rest clean [verbose=0/cancel=0]
}

membytes_t
s3_buffer_load(CurlBuffer *s3buf, gpointer buffer, membytes_t size, gboolean verbose)
{
    // set up a alloc-emptied buffer
    s3_buffer_init(s3buf,0,verbose); // no setup of mutex and cond

    s3buf->cond = NULL; // none desired
    s3buf->mutex = NULL; // none desierd

    s3buf->buffer = buffer; // access a buffer directly
    s3buf->buffer_end = 0;
    s3buf->max_buffer_size = size;
    s3buf->noreset = FALSE;  // can always reset
    s3buf->rdbytes_to_eod = size;  // total amount left is same
    s3buf->bytes_filled = size;  // total amount left is same

    if (s3buf->opt_verbose)
       g_debug("[%p] s3_buffer_load (write): buffer=%p [%#x]", s3buf, s3buf->buffer, size);

    return ( buffer ? size : 0 );
}

// write pad-0 bytes until filled
membytes_t
s3_buffer_write_padding(CurlBuffer *s3buf)
{
    guint endpos;
    membytes_t towrite,len;

    if (!s3buf || !s3buf->buffer) return 0;

    {
        g_mutex_lock(s3buf->mutex);

        // read buffer_end cleanly...
        endpos = s3buf->buffer_end;

        // get total bytes to go...
        towrite = s3buf->max_buffer_size - s3buf->bytes_filled; // max amount to write
        if (s3buf->rdbytes_to_eod)
            towrite = min(towrite, s3buf->rdbytes_to_eod); // decrease to eod point [if present]

        // start of first contiguous buffer uses [negative-wrap --> ignore buffer_pos]
        len = min(s3buf->max_buffer_size - endpos, s3buf->buffer_pos - endpos);                      // max upper-linear-space segment...
        len = min(len,towrite);               // reduce first segment if at eod

        // write zeroed bytes into buffer
        memset(s3buf->buffer + endpos,'\0',len);  // write first segment
        memset(s3buf->buffer,'\0',towrite - len); // write second segment (if any left)

        s3buf->buffer_end += towrite;
        s3buf->buffer_end %= s3buf->max_buffer_size; // circle to zero if needed
        s3buf->bytes_filled += towrite; // advance to the fullest amount

        g_mutex_unlock(s3buf->mutex);
    }

    return towrite;
}

membytes_t
s3_buffer_load_string(CurlBuffer *s3buf, char *buffer, gboolean verbose)
{
    membytes_t r = s3_buffer_load(s3buf, buffer, strlen(buffer), verbose); // presumed length is str+1 for nul

    if (s3buf->opt_verbose)
       g_debug("[%p] s3_buffer_load (string): \"%s\"", s3buf, buffer);
    return r;
}

static xferbytes_t
s3_buffer_data_copy(gboolean copy_bytes_out, void *ptr, xferbytes_t size, xferbytes_t nmemb, void * stream)
{
    const char *const copytype = ( copy_bytes_out ? "read" : "write" );
    CurlBuffer *const s3buf = stream;
    xferbytes_t bytes_desired = size * nmemb; // in or out... (includes multiple waits)
    xferbytes_t tocopy = bytes_desired;

    volatile membytes_t *const cursor_ind = ( copy_bytes_out ? &s3buf->buffer_pos : &s3buf->buffer_end );
    volatile membytes_t *const empty_ind = ( copy_bytes_out ? &s3buf->buffer_end : &s3buf->buffer_pos );
    GCond *cond = s3buf->cond;
    GMutex *mutex = s3buf->mutex;

    if ( ! bytes_desired )
        return 0;

    tocopy = bytes_desired;

    //////////////////////////////////////////////////////////// locked
    if (mutex)
        g_mutex_lock(mutex);

    // noop: end "early" if EOD and empty....
    if ( copy_bytes_out && ! s3buf->bytes_filled && ! s3buf->rdbytes_to_eod ) 
    {
        if (s3buf->opt_verbose)
            g_debug("[%p] s3_buffer_%s_func: starting EOD-empty [%#x:%#x) (past eod +%#lx)",
                    s3buf, copytype, *cursor_ind, *empty_ind, tocopy);
        if (mutex)
            g_mutex_unlock(mutex);

        // cannot send meaningful abort flag
        if (bytes_desired >= CURL_READFUNC_ABORT) return 0; 
        return CURL_READFUNC_ABORT; // flag the read correctly...
    }

    // handle single-or-double threaded modes
    {
        // NOTE: only init buffer contents if no buffer allocated
        const membytes_t initsize = ( s3buf->max_buffer_size ? : size*nmemb );
        const membytes_t buffmax = ( !s3buf->buffer ? s3_buffer_init(s3buf, initsize, s3buf->opt_verbose) : initsize );
        // NOTE: its possible buffer_end or buffer_pos may equal buffmax... so avoid confusion
        membytes_t buffend = *empty_ind % buffmax; // first non-usable byte

        // estimate if flow-control is necessary now...
        membytes_t avail = 1;               // non-zero start
        membytes_t alt_avail_low = buffmax; // keep track of lowest
        membytes_t last_avail;
        membytes_t nextbytes = G_MAXINT32;
        membytes_t last_cursor = *cursor_ind;

        int retries = 10;  // useless loops before failure

        for( ; tocopy ; last_cursor = *cursor_ind )
        {
            if ((mutex && mutex != s3buf->mutex) || (cond && cond != s3buf->cond)) {
                g_debug("[%p] s3_buffer_%s_func WAKEUP/ABORT DETECTION (mutex, tocopy +%#lx bytes)",
                  s3buf, copytype, tocopy);
                mutex = NULL;
                cond = NULL;
            }

            // when needed.. cease all copying
            if (s3buf->cancel) {
		g_debug("[%p] s3_buffer_%s_func ABORT-CANCEL of %#lx/%#lx bytes eod=%d-%#lx)",
		  s3buf, copytype, tocopy, bytes_desired,
		  s3_buffer_eod_func(s3buf), s3_buffer_read_size_func(s3buf));
                break;
	    }
            if (!s3buf->buffer) {
		g_debug("[%p] s3_buffer_%s_func ABORT-NO-BUFFER of %#lx/%#lx bytes eod=%d-%#lx)",
		  s3buf, copytype, tocopy, bytes_desired,
		  s3_buffer_eod_func(s3buf), s3_buffer_read_size_func(s3buf));
                break;
	    }

            last_avail = avail;
            avail = ( copy_bytes_out
                        ? s3buf->bytes_filled
                        : buffmax - s3buf->bytes_filled );

            // detect if bytes_filled is equivalent [under mod] to avail window
            g_assert ( avail % buffmax == ( buffmax + buffend - *cursor_ind ) % buffmax );
            g_assert ( buffend == *empty_ind % buffmax ); // write over const


            // detect if other thread may be waiting!!
            if ( mutex && alt_avail_low ) {
                alt_avail_low = ( copy_bytes_out
                            ? min(alt_avail_low, buffmax - s3buf->bytes_filled)
                            : min(alt_avail_low, s3buf->bytes_filled) );
            }

            // CANNOT CONTINUE: first-and-second (after waiting.. and after loop-around) failed exit
            // NOTE: use "noreset" as a way to detect non-initial wait: full-buffer wraparound
            if ( ! avail && ! last_avail && s3buf->noreset ) {
                g_debug("[%p] s3_buffer_%s_func RETRY-FAIL left=%d/10 (filled=%#x, missing %#lx bytes)",
                  s3buf, copytype, retries, s3buf->bytes_filled, tocopy);

                // no escape if no mutex is available...
                if ( !mutex || !--retries )
                    break; // cannot proceed in deadlock (50 seconds idle causes bailout)
            }

            // end "early" if avail-matches-filled and EOD was reached
            if ( ! avail && ! s3buf->bytes_filled && ! s3buf->rdbytes_to_eod ) {
                if (s3buf->opt_verbose)
                    g_debug("[%p] s3_buffer_%s_func: avail/filled == 0 [%#x:%#x) (uncopied %#lx)",
                               s3buf, copytype, *cursor_ind, *empty_ind, tocopy);
                break;
            }

            // handle simple no-wait case ...
            if ( ( !mutex || ! cond) && ! avail ) {
                g_debug("[%p] s3_buffer_%s_func SINGLE-OP FAILURE (filled=%#x, uncopied %#lx bytes)",
                  s3buf, copytype, s3buf->bytes_filled, tocopy);
                break; // no way to proceed without a mutex
            }

            // flow-control delay here...
            if ( ! avail )
            {
                // in case need to free them...

                // wake alt thread if it is blocked
                if ( ! alt_avail_low && s3buf->waiting ) {
                    if (s3buf->opt_verbose)
                        g_debug("[%p] s3_buffer_%s_func WAKE-AND-WAIT (#%d) filled=%#x tocopy +%#lx bytes)",
                              s3buf, copytype, retries, s3buf->bytes_filled, tocopy);
                    g_cond_signal(s3buf->cond);
                }
                else {
                    if (s3buf->opt_verbose)
                        g_debug("[%p] s3_buffer_%s_func WAIT #%d (filled=%#x, tocopy +%#lx bytes)",
                              s3buf, copytype, retries, s3buf->bytes_filled, tocopy);
                }

                s3buf->waiting = 2 + copy_bytes_out; // mark a reader/writer as waiting
                // try to recover if accidentally woken up                                              ////// COND_WAIT
                g_assert( ! g_mutex_trylock(mutex) );
                g_cond_wait_until(cond, mutex, g_get_monotonic_time () + 5 * G_TIME_SPAN_SECOND);

                /// reset one var if value changed...
                // *(__typeof__(&nextbytes)) &buffend = *empty_ind % buffmax; // write over const
                buffend = *empty_ind % buffmax; // write over [ideally] const

                continue; // re-try a new value for avail
            } ///////////////////////////////////////////////////////////// loop back if !avail

            // avail > 0 so reset the timeout counter
            retries = 10;

            // index-distance from first-byte-ready index to first-byte-notready index
            //    in the circle-buffer [readable or fillable]

            // find next contiguous bytes copy
            nextbytes = *cursor_ind + avail;     // new cursor pos
            nextbytes = min(nextbytes, buffmax); // limit at end
            nextbytes -= *cursor_ind;            // num of bytes to copy
            nextbytes = min(nextbytes, tocopy);  // limit to req

	    // (1) wrote bytes before this pass
	    // (2) writing bytes for this pass
            // (3) starting pass from index-zero cursor?
            //   ---> we've wrapped from end of buffer back to start
	    if (tocopy < bytes_desired && !*cursor_ind && nextbytes)
	    	s3buf->noreset = TRUE; // cannot reset back to start

            if ( s3buf->opt_verbose && avail && ! last_avail ) {
                g_debug("[%p] s3_buffer_%s_func AVAIL %s: nnext=%#x ([%#x->%#x]-%#x) tocopy +%#lx",
                   s3buf, copytype, 
                   ( nextbytes == tocopy ? "FINAL" : ( s3buf->noreset ? "AWAKEN" : "BEGIN" )), 
                   nextbytes, last_cursor, *cursor_ind, *empty_ind, tocopy);
            }

            // nextbytes *may* be zero

            if ( copy_bytes_out ) {
                memcpy((char *)ptr, &s3buf->buffer[*cursor_ind], nextbytes);
                s3buf->bytes_filled -= nextbytes; // bytes filled changed
                s3buf->rdbytes_to_eod -= min(s3buf->rdbytes_to_eod,nextbytes); // total bytes to read [external too]
            } else {
                memcpy(&s3buf->buffer[*cursor_ind], (char*)ptr, nextbytes);
                // ---------- don't adjust rdbytes_to_eod ... as it must be fixed *before* all reads only
                s3buf->bytes_filled += nextbytes; // bytes filled changed
            }

            // shift read point
            *cursor_ind += nextbytes;
            *cursor_ind %= buffmax; // roll around to bottom again if needed

            // shift next write point
            ptr = (char*) ptr + nextbytes;
            tocopy -= nextbytes; // cannot underflow

            // tocopy is done ... [started w/buffmax or less]
            if (tocopy && s3buf->opt_verbose) {
                char *which = ( tocopy + nextbytes == bytes_desired ? "" : "repeated " );
                g_debug("[%p] s3_buffer_%s_func (reloop): nlast=%#x ([%#x->%#x]-%#x) tocopy +%#lx %sloop",
                            s3buf, copytype, nextbytes, last_cursor, *cursor_ind, *empty_ind, tocopy, which);
            }
        } // for(;tocopy;)

        // clear waiting tag only if we set it
        if (s3buf->waiting == 2 + copy_bytes_out)  // detect same reader/writer as waiting
            s3buf->waiting = FALSE;

        // show info if progresed and other op is waiting
        // if (tocopy < bytes_desired && s3buf->opt_verbose)
        if (tocopy < bytes_desired && s3buf->waiting && s3buf->opt_verbose)
        {
            guint fillmax = ( copy_bytes_out ? 0 : min(s3buf->max_buffer_size, s3buf->rdbytes_to_eod) );
            guint lo_fill, hi_fill;
	    guint old_cursor = ( *cursor_ind + buffmax - nextbytes ) % buffmax;

            lo_fill = min( max(fillmax/4, 0x2000), fillmax/2);
            hi_fill = fillmax - lo_fill;

            // if (tocopy || (copy_bytes_out))
            if (tocopy || (copy_bytes_out && fillmax && s3buf->bytes_filled <= lo_fill))
               g_debug("[%p] s3_buffer_%s_func: n=%#x [%#x->%#x] (eod=%d-%#lx) read-%s-emptied=%c%#lx]",
                       s3buf, copytype, nextbytes, old_cursor, *cursor_ind,
                       s3_buffer_eod_func(s3buf), s3_buffer_read_size_func(s3buf), 
		       ( s3buf->waiting ? "wake":"pre" ), 
                       (s3buf->bytes_filled <= lo_fill ? '+' : '-'),
		       labs((signed_xferbytes_t)s3buf->bytes_filled - (signed_xferbytes_t)lo_fill) );

            // if (tocopy || (!copy_bytes_out))
            if (tocopy || (!copy_bytes_out && s3buf->bytes_filled > hi_fill))
               g_debug("[%p] s3_buffer_%s_func: n=%#x [%#x->%#x] (full=%#lx) write-%s-filled=%c%#lx",
                       s3buf, copytype, nextbytes, old_cursor, *cursor_ind,
                       s3_buffer_size_func(s3buf), 
                       ( s3buf->waiting ? "wake":"pre" ),
                       (s3buf->bytes_filled >= hi_fill ? '+' : '-'),
		       labs((signed_xferbytes_t)s3buf->bytes_filled - (signed_xferbytes_t)hi_fill) );
        }
    } // var-context

    // if no bytes filled at all [and no one waiting] ... reset to base position
    if ( s3buf->noreset && ! s3buf->waiting && ! s3buf->bytes_filled && *cursor_ind == *empty_ind) {
        *cursor_ind = 0;          // reset empty buffer back to beginning
        // *(__typeof__(cursor_ind)) empty_ind = 0;
        *empty_ind = 0; // write over [ideally] const
       g_debug("[%p] s3_buffer_%s_func: empty-reset", s3buf, copytype);
    }

    // wake alternate thread if someone is there...
    if (tocopy < bytes_desired && mutex && cond && s3buf->waiting)
    {
        guint fillmax = ( copy_bytes_out ? 0 : min(s3buf->max_buffer_size, s3buf->rdbytes_to_eod) );
        guint lo_fill,hi_fill;

        lo_fill = min( max(fillmax/4, 0x2000), fillmax/2);
        hi_fill = fillmax - lo_fill;

        if (tocopy)                   // wake if stopped mid-operation
            g_cond_signal(cond);

        // wake writer if now read-emptied enough .. and more left
        if (!tocopy && copy_bytes_out && fillmax && s3buf->bytes_filled <= lo_fill)
            g_cond_signal(cond);

        // wake reader if now write-filled enough
        if (!tocopy && !copy_bytes_out && s3buf->bytes_filled > hi_fill)
            g_cond_signal(cond);
    }

    if (mutex) {
        g_assert(!g_mutex_trylock(mutex) ); // must be locked
        g_mutex_unlock(mutex); // now unlocked
    }

    // zero bytes transferred was just deadlock-prevention!
    if ( bytes_desired == tocopy ) {
        g_debug("[%p] s3_buffer_%s_func ABORT-DETECT STOP of %#lx bytes eod=%d-%#lx)",
          s3buf, copytype, bytes_desired,
          s3_buffer_eod_func(s3buf), s3_buffer_read_size_func(s3buf));

        if ( ! copy_bytes_out ) return 0; // flag the write correctly

        // cannot send meaningful read-abort flag
        if (bytes_desired >= CURL_READFUNC_ABORT) return 0; 
        return CURL_READFUNC_ABORT; // flag the read correctly;
    }

    return bytes_desired - tocopy; // note if less than desired
}


int
s3_buffer_fill_wait(membytes_t fill_level, CurlBuffer *stream)
{
    CurlBuffer *const s3buf = stream;
    int retries = 10;
    membytes_t filled = 0;
    GMutex *mutex = s3buf->mutex;
    GCond *cond = s3buf->cond;

    if (!mutex)
        return ( s3_buffer_size_func(s3buf) >= fill_level ? 0 : 1 );

    // mutex is non-zero

    {
        g_mutex_lock(mutex);

        while( --retries && !s3buf->cancel && cond == s3buf->cond ) 
        {
            if (filled < s3_buffer_size_func(s3buf)) {
                filled = s3_buffer_size_func(s3buf);
                retries = 10;
            }
            if (fill_level && filled >= fill_level)
                goto done; // success
            if (!fill_level && filled >= s3_buffer_read_size_func(s3buf))
                goto done; // success

            s3buf->waiting = 2 + TRUE; // mark a reader as waiting

            // must be locked already!
            g_assert( ! g_mutex_trylock(mutex) );
            g_cond_wait_until(cond, mutex, g_get_monotonic_time () + 5 * G_TIME_SPAN_SECOND);
        }

        if (s3buf->waiting == 2 + TRUE)
            s3buf->waiting = FALSE; // cleared if reader is still set

        // failed

        if (mutex == s3buf->mutex)
            g_mutex_unlock(mutex);
        return -1;

  done:
        if (s3buf->waiting == 2 + TRUE)
            s3buf->waiting = FALSE; // cleared if reader is still set
        g_mutex_unlock(mutex);
        return 0;
    }
}

/* a CURLOPT_READFUNCTION to read data from a buffer. */
xferbytes_t
s3_buffer_read_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void * stream)
{
    CurlBuffer *s3buf = stream;
    /* can return any size over 0 */
    /* can return CURL_READFUNC_ABORT */
    /* can return CURL_READFUNC_PAUSE (wont stop progress checks) */
    /* return of 0 will signal EOD */

    if (s3buf->opt_padded && !s3buf->rdbytes_to_eod && !s3buf->bytes_filled) {
        memset(ptr,'\0',size*nmemb); // instant padding..!
        return size*nmemb;
    }

    return s3_buffer_data_copy(TRUE, ptr, size, nmemb, stream);
}

xferbytes_t
s3_buffer_write_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void * stream)
{
    CurlBuffer *s3buf = stream;
    /* must return == size*nmemb bytes for success */
    /* can return CURL_WRITEFUNC_PAUSE */
    /* can abort transfer if [0:size*nmemb) bytes are returned */
    if (! s3buf->buffer)
        s3_buffer_init(s3buf,size,s3buf->opt_verbose);

    return s3_buffer_data_copy(FALSE, ptr, size, nmemb, stream);
}

// give amount of data stored in buffer (after old write or before new read)
gboolean
s3_buffer_eod_func(const CurlBuffer *stream)
{
    const CurlBuffer *s3buf = stream;

    if (! s3buf) return FALSE;
    if (! s3buf->buffer) return FALSE;      // nothing in buffer to deliver

    if (! s3buf->rdbytes_to_eod) return TRUE; // single use buffer was read out earlier
    return s3buf->rdbytes_to_eod == s3buf->bytes_filled; // amount left == filled
}

// give amount of data stored in buffer (after old write or before new read)
char *
s3_buffer_data_func(const CurlBuffer *stream)
{
    const CurlBuffer *s3buf = stream;

    if (!s3buf || !s3buf->buffer) return NULL;
    return s3buf->buffer + g_atomic_int_get(&s3buf->buffer_pos); // start of first contiguous buffer
}

// give amount of data stored in buffer (after old write or before new read)
xferbytes_t
s3_buffer_size_func(const CurlBuffer *stream)
{
    const CurlBuffer *s3buf = stream;
    if (!s3buf) return 0;
    return s3buf->bytes_filled; // amount left/filled right after buffer_pos index
}

// give amount of data stored in buffer (after old write or before new read)
xferbytes_t
s3_buffer_lsize_func(const CurlBuffer *stream)
{
    const CurlBuffer *s3buf = stream;
    xferbytes_t bufflen = 0;

    if (!s3buf) return 0;

    {
	xferbytes_t buffpos = g_atomic_int_get(&s3buf->buffer_pos);
	xferbytes_t filled = g_atomic_int_get(&s3buf->bytes_filled);
	bufflen = buffpos + filled;
	bufflen = min(bufflen, s3buf->max_buffer_size);
	bufflen -= buffpos;
    }

    return bufflen; // amount left/filled right after buffer_pos index
}


// give amount of data stored in buffer (after old write or before new read)
objbytes_t
s3_buffer_read_size_func(const CurlBuffer *stream)
{
    const CurlBuffer *s3buf = stream;
    objbytes_t rdbytes;

    if (!s3buf) return 0;

    rdbytes = (size_t) g_atomic_pointer_get(&s3buf->rdbytes_to_eod);
    if (rdbytes) return rdbytes; // amount left to read
    return s3_buffer_size_func(stream); // amount left in buffer
}

static GByteArray*
s3_buffer_hash_func(CurlBuffer *s3buf, GByteArray *(*hash_func)(const GByteArray*,const GByteArray*))
{
    GByteArray *r = NULL;
    gboolean eod;
    guint body_lead_len;

    if (!s3buf) return NULL;

    // reasonably thread-safe with atomics
    eod = s3_buffer_eod_func(s3buf);
    body_lead_len = s3_buffer_lsize_func(s3buf);
        
    // wait if buffer is non-EOD and empty
    if (!eod && !body_lead_len) {
        s3_buffer_fill_wait(1, s3buf); // wait for any signal

        // reasonably thread-safe with atomics
        eod = s3_buffer_eod_func(s3buf);
        body_lead_len = s3_buffer_lsize_func(s3buf);
    }

    {
        // first segment
        // second segment (or not)
        guint body_back_len = s3buf->bytes_filled - body_lead_len;
        GByteArray req_body = { (guint8 *)s3_buffer_data_func(s3buf), body_lead_len };
        GByteArray req_body_ext = {(guint8 *)s3buf->buffer, body_back_len };

        if ( !eod || !body_lead_len ) {
            g_debug("[%p] %s: INCOMPLETE BUFFER %#x = [%#x:%#x)[%#x:%#x) eod=%d-%#lx",
                       s3buf, __FUNCTION__, s3buf->bytes_filled,
                       s3buf->buffer_pos, s3buf->buffer_pos + body_lead_len,
                       s3buf->buffer_end - body_back_len, s3buf->buffer_end,
                       eod, s3buf->rdbytes_to_eod);

            goto cleanup; // cannot perform md5 if not single-buffer content
        }

        if (s3buf->opt_verbose)
            g_debug("[%p] %s: buffer-relative %#x = [%#x:%#x)[%#x:%#x) eod=%d-%#lx",
                       s3buf, __FUNCTION__, s3buf->bytes_filled,
                       s3buf->buffer_pos, s3buf->buffer_pos + body_lead_len,
                       s3buf->buffer_end - body_back_len, s3buf->buffer_end,
                       eod, s3buf->rdbytes_to_eod);

        // measure hash in correct byte order
        r = (*hash_func)(&req_body, &req_body_ext);
    }

cleanup:
    return r;
}

GByteArray*
s3_buffer_sha256_func(CurlBuffer *stream)
{
    return s3_buffer_hash_func((CurlBuffer *)stream, &s3_compute_sha256_hash);
}

GByteArray*
s3_buffer_md5_func(CurlBuffer *stream)
{
    return s3_buffer_hash_func((CurlBuffer *)stream, &s3_compute_md5_hash);
}

void
s3_buffer_read_reset_func(CurlBuffer *stream)
{
    CurlBuffer *s3buf = stream;

    if (!s3buf || !s3buf->buffer) return;

    s3_buffer_lock(s3buf);

    if (s3buf->noreset) {
	g_warning("[%p] %s: failing to read-reset (orig) %#lx > %#x.", s3buf, __FUNCTION__, 
	    s3buf->rdbytes_to_eod, s3buf->max_buffer_size);
	goto failed;
    }

    do
    {
        membytes_t buffmax = s3buf->max_buffer_size;
        membytes_t buffend = s3buf->buffer_end % buffmax;
        membytes_t linearbuff = (s3buf->buffer_pos + s3buf->bytes_filled) % buffmax;

        // buffer not filled linearly?
        if ( linearbuff != buffend ) {
            g_warning("[%p] %s: failing to read-reset %#x != %#x.", s3buf, __FUNCTION__, 
               linearbuff, buffend);
	    goto failed;
        }

	// re-add whole buffer [dangerous!]
	if (!buffend && !s3buf->bytes_filled && !s3buf->rdbytes_to_eod) {
	    s3buf->rdbytes_to_eod += s3buf->max_buffer_size;
	    s3buf->bytes_filled += s3buf->max_buffer_size;
	    break; // successfully reset
	}

        // just reverse some advanced reads
	if (s3buf->buffer_pos) {
	    s3buf->rdbytes_to_eod += s3buf->buffer_pos;
	    s3buf->bytes_filled += s3buf->buffer_pos;
	    s3buf->buffer_pos -= s3buf->buffer_pos;
	    break; // successfully reset
	}

	if (s3buf->bytes_filled == s3buf->rdbytes_to_eod) {
	    break; // can be reset in future .. but doesn't need it
	}

	// buffer_pos == 0 
	// bytes_filled 

	goto failed; // could not reset any other cases
    } while(FALSE);

    g_info("[%p] %s: performed upload-buffer reset [%#x-%#x/%#x)", s3buf, __FUNCTION__, 
       s3buf->buffer_pos, s3buf->buffer_pos + s3buf->bytes_filled, s3buf->max_buffer_size);

    // wake writers (any??) up to notice new state
    if (s3buf->cond)
	g_cond_signal(s3buf->cond);

    s3_buffer_unlock(s3buf);
    return;

failed:
    s3buf->noreset = TRUE; // do not process from this position
    s3buf->cancel = TRUE; // do not process from this position
    s3_buffer_unlock(s3buf);
}

void
s3_buffer_write_reset_func(CurlBuffer *stream)
{
    CurlBuffer *s3buf = stream;
    objbytes_t content_toread;

    if (!s3buf) return;

    // retain no data.. but hold at least the same buffer and expected eod position
    content_toread = s3_buffer_read_size_func(s3buf);
    s3_buffer_reset_eod_func(s3buf, 0);
    // eod == 0 for now
    // NOTE: CALLS LOCK AND UNLOCK
    s3_buffer_init(s3buf, s3buf->max_buffer_size, s3buf->opt_verbose); 
    // eod == old value 
    s3_buffer_reset_eod_func(s3buf, content_toread); // set back again (if it matters)

    g_info("[%p] %s: performed download-buffer reset [%#x-%#x/%#x)", s3buf, __FUNCTION__, 
       s3buf->buffer_pos, s3buf->buffer_pos + s3buf->bytes_filled, s3buf->max_buffer_size);

    // wake readers (any??) up to notice new state
    if (s3buf->cond)
	g_cond_signal(s3buf->cond);
}

/* a CURLOPT_READFUNCTION that writes nothing. */
xferbytes_t
s3_empty_read_func(G_GNUC_UNUSED void *ptr, G_GNUC_UNUSED xferbytes_t size, G_GNUC_UNUSED xferbytes_t nmemb, G_GNUC_UNUSED void * stream)
{
    return 0;
}

xferbytes_t
s3_empty_size_func(G_GNUC_UNUSED void *stream)
{
    return 0;
}

GByteArray*
s3_empty_md5_func(G_GNUC_UNUSED void *stream)
{
    static const GByteArray empty = {(guint8 *) "", 0};

    return s3_compute_md5_hash(&empty,NULL);
}

/* a CURLOPT_WRITEFUNCTION to write data that just counts data.
 * s3_write_data should be NULL or a pointer to an gint64.
 */
xferbytes_t
s3_counter_write_func(G_GNUC_UNUSED void *ptr, xferbytes_t size, xferbytes_t nmemb, void *stream)
{
    gint64 *count = (gint64*) stream, inc = nmemb*size;

    if (count) *count += inc;
    return inc;
}

void
s3_counter_reset_func(void *stream)
{
    gint64 *count = (gint64*) stream;

    if (count) *count = 0;
}

#ifdef _WIN32
/* a CURLOPT_READFUNCTION to read data from a file. */
xferbytes_t
s3_file_read_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void * stream)
{
    HANDLE *hFile = (HANDLE *) stream;
    DWORD bytes_read;

    ReadFile(hFile, ptr, (DWORD) size*nmemb, &bytes_read, NULL);
    return bytes_read;
}

xferbytes_t
s3_file_size_func(void *stream)
{
    HANDLE *hFile = (HANDLE *) stream;
    DWORD size = GetFileSize(hFile, NULL);

    if (INVALID_FILE_SIZE == size) {
        return -1;
    } else {
        return size;
    }
}

GByteArray*
s3_file_md5_func(void *stream)
{
#define S3_MD5_BUF_SIZE (10*1024)
    HANDLE *hFile = (HANDLE *) stream;
    guint8 buf[S3_MD5_BUF_SIZE];
    DWORD bytes_read;
    MD5_CTX md5_ctx;
    GByteArray *ret = NULL;

    g_assert(INVALID_SET_FILE_POINTER != SetFilePointer(hFile, 0, NULL, FILE_BEGIN));

    ret = g_byte_array_sized_new(S3_MD5_HASH_BYTE_LEN);
    g_byte_array_set_size(ret, S3_MD5_HASH_BYTE_LEN);
    MD5_Init(&md5_ctx);

    while (ReadFile(hFile, buf, S3_MD5_BUF_SIZE, &bytes_read, NULL)) {
        MD5_Update(&md5_ctx, buf, bytes_read);
    }
    MD5_Final(ret->data, &md5_ctx);

    g_assert(INVALID_SET_FILE_POINTER != SetFilePointer(hFile, 0, NULL, FILE_BEGIN));
    return ret;
#undef S3_MD5_BUF_SIZE
}

GByteArray*
s3_file_reset_func(void *stream)
{
    g_assert(INVALID_SET_FILE_POINTER != SetFilePointer(hFile, 0, NULL, FILE_BEGIN));
}

/* a CURLOPT_WRITEFUNCTION to write data to a file. */
xferbytes_t
s3_file_write_func(void *ptr, xferbytes_t size, xferbytes_t nmemb, void *stream)
{
    HANDLE *hFile = (HANDLE *) stream;
    DWORD bytes_written;

    WriteFile(hFile, ptr, (DWORD) size*nmemb, &bytes_written, NULL);
    return bytes_written;
}
#endif
