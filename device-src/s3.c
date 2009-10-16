/*
 * Copyright (c) 2008,2009 Zmanda, Inc.  All Rights Reserved.
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

/* TODO
 * - collect speed statistics
 * - debugging mode
 */

#ifdef HAVE_CONFIG_H
/* use a relative path here to avoid conflicting with Perl's config.h. */
#include "../config/config.h"
#endif
#include <string.h>
#include "s3.h"
#include "s3-util.h"
#ifdef HAVE_REGEX_H
#include <regex.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_DIRENT_H
#include <dirent.h>
#endif
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_UTIL_H
#include "util.h"
#endif
#ifdef HAVE_AMANDA_H
#include "amanda.h"
#endif

#include <curl/curl.h>

/* Constant renamed after version 7.10.7 */
#ifndef CURLINFO_RESPONSE_CODE
#define CURLINFO_RESPONSE_CODE CURLINFO_HTTP_CODE
#endif

/* We don't need OpenSSL's kerberos support, and it's broken in
 * RHEL 3 anyway. */
#define OPENSSL_NO_KRB5

#ifdef HAVE_OPENSSL_HMAC_H
# include <openssl/hmac.h>
#else
# ifdef HAVE_CRYPTO_HMAC_H
#  include <crypto/hmac.h>
# else
#  ifdef HAVE_HMAC_H
#   include <hmac.h>
#  endif
# endif
#endif

#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/md5.h>

/* Maximum key length as specified in the S3 documentation
 * (*excluding* null terminator) */
#define S3_MAX_KEY_LENGTH 1024

#define AMAZON_SECURITY_HEADER "x-amz-security-token"
#define AMAZON_BUCKET_CONF_TEMPLATE "\
  <CreateBucketConfiguration>\n\
    <LocationConstraint>%s</LocationConstraint>\n\
  </CreateBucketConfiguration>"

#define AMAZON_WILDCARD_LOCATION "*"

/* parameters for exponential backoff in the face of retriable errors */

/* start at 0.01s */
#define EXPONENTIAL_BACKOFF_START_USEC G_USEC_PER_SEC/100
/* double at each retry */
#define EXPONENTIAL_BACKOFF_BASE 2
/* retry 14 times (for a total of about 3 minutes spent waiting) */
#define EXPONENTIAL_BACKOFF_MAX_RETRIES 14

/* general "reasonable size" parameters */
#define MAX_ERROR_RESPONSE_LEN (100*1024)

/* Results which should always be retried */
#define RESULT_HANDLING_ALWAYS_RETRY \
        { 400,  S3_ERROR_RequestTimeout,     0,                          S3_RESULT_RETRY }, \
        { 409,  S3_ERROR_OperationAborted,   0,                          S3_RESULT_RETRY }, \
        { 412,  S3_ERROR_PreconditionFailed, 0,                          S3_RESULT_RETRY }, \
        { 500,  S3_ERROR_InternalError,      0,                          S3_RESULT_RETRY }, \
        { 501,  S3_ERROR_NotImplemented,     0,                          S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_COULDNT_CONNECT,      S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_COULDNT_RESOLVE_HOST, S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_PARTIAL_FILE,         S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_OPERATION_TIMEOUTED,  S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_SEND_ERROR,           S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_RECV_ERROR,           S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_GOT_NOTHING,          S3_RESULT_RETRY }

/*
 * Data structures and associated functions
 */

struct S3Handle {
    /* (all strings in this struct are freed by s3_free()) */

    char *access_key;
    char *secret_key;
    char *user_token;

    char *bucket_location;

    char *ca_info;

    CURL *curl;

    gboolean verbose;
    gboolean use_ssl;

    /* information from the last request */
    char *last_message;
    guint last_response_code;
    s3_error_code_t last_s3_error_code;
    CURLcode last_curl_code;
    guint last_num_retries;
    void *last_response_body;
    guint last_response_body_size;
};

typedef struct {
    CurlBuffer resp_buf;
    s3_write_func write_func;
    s3_reset_func reset_func;
    gpointer write_data;

    gboolean headers_done;
    gboolean int_write_done;
    char *etag;
} S3InternalData;

/* Callback function to examine headers one-at-a-time
 *
 * @note this is the same as CURLOPT_HEADERFUNCTION
 *
 * @param data: The pointer to read data from
 * @param size: The size of each "element" of the data buffer in bytes
 * @param nmemb: The number of elements in the data buffer.
 * So, the buffer's size is size*nmemb bytes.
 * @param stream: the header_data (an opaque pointer)
 *
 * @return The number of bytes written to the buffer or
 * CURL_WRITEFUNC_PAUSE to pause.
 * If it's the number of bytes written, it should match the buffer size
 */
typedef size_t (*s3_header_func)(void *data, size_t size, size_t nmemb, void *stream);


/*
 * S3 errors */

/* (see preprocessor magic in s3.h) */

static char * s3_error_code_names[] = {
#define S3_ERROR(NAME) #NAME
    S3_ERROR_LIST
#undef S3_ERROR
};

/* Convert an s3 error name to an error code.  This function
 * matches strings case-insensitively, and is appropriate for use
 * on data from the network.
 *
 * @param s3_error_code: the error name
 * @returns: the error code (see constants in s3.h)
 */
static s3_error_code_t
s3_error_code_from_name(char *s3_error_name);

/* Convert an s3 error code to a string
 *
 * @param s3_error_code: the error code to convert
 * @returns: statically allocated string
 */
static const char *
s3_error_name_from_code(s3_error_code_t s3_error_code);


/*
 * result handling */

/* result handling is specified by a static array of result_handling structs,
 * which match based on response_code (from HTTP) and S3 error code.  The result
 * given for the first match is used.  0 acts as a wildcard for both response_code
 * and s3_error_code.  The list is terminated with a struct containing 0 for both
 * response_code and s3_error_code; the result for that struct is the default
 * result.
 *
 * See RESULT_HANDLING_ALWAYS_RETRY for an example.
 */
typedef enum {
    S3_RESULT_RETRY = -1,
    S3_RESULT_FAIL = 0,
    S3_RESULT_OK = 1
} s3_result_t;

typedef struct result_handling {
    guint response_code;
    s3_error_code_t s3_error_code;
    CURLcode curl_code;
    s3_result_t result;
} result_handling_t;

/* Lookup a result in C{result_handling}.
 *
 * @param result_handling: array of handling specifications
 * @param response_code: response code from operation
 * @param s3_error_code: s3 error code from operation, if any
 * @param curl_code: the CURL error, if any
 * @returns: the matching result
 */
static s3_result_t
lookup_result(const result_handling_t *result_handling,
              guint response_code,
              s3_error_code_t s3_error_code,
              CURLcode curl_code);

/*
 * Precompiled regular expressions */
static regex_t etag_regex, error_name_regex, message_regex, subdomain_regex,
    location_con_regex;

/*
 * Utility functions
 */

/* Check if a string is non-empty
 *
 * @param str: string to check
 * @returns: true iff str is non-NULL and not "\0"
 */
static gboolean is_non_empty_string(const char *str);

/* Construct the URL for an Amazon S3 REST request.
 *
 * A new string is allocated and returned; it is the responsiblity of the caller.
 *
 * @param hdl: the S3Handle object
 * @param verb: capitalized verb for this request ('PUT', 'GET', etc.)
 * @param bucket: the bucket being accessed, or NULL for none
 * @param key: the key being accessed, or NULL for none
 * @param subresource: the sub-resource being accessed (e.g. "acl"), or NULL for none
 * @param use_subdomain: if TRUE, a subdomain of s3.amazonaws.com will be used
 */
static char *
build_url(const char *bucket,
      const char *key,
      const char *subresource,
      const char *query,
      gboolean use_subdomain,
      gboolean use_ssl);

/* Create proper authorization headers for an Amazon S3 REST
 * request to C{headers}.
 *
 * @note: C{X-Amz} headers (in C{headers}) must
 *  - be in lower-case
 *  - be in alphabetical order
 *  - have no spaces around the colon
 * (don't yell at me -- see the Amazon Developer Guide)
 *
 * @param hdl: the S3Handle object
 * @param verb: capitalized verb for this request ('PUT', 'GET', etc.)
 * @param bucket: the bucket being accessed, or NULL for none
 * @param key: the key being accessed, or NULL for none
 * @param subresource: the sub-resource being accessed (e.g. "acl"), or NULL for none
 * @param md5_hash: the MD5 hash of the request body, or NULL for none
 * @param use_subdomain: if TRUE, a subdomain of s3.amazonaws.com will be used
 */
static struct curl_slist *
authenticate_request(S3Handle *hdl,
                     const char *verb,
                     const char *bucket,
                     const char *key,
                     const char *subresource,
                     const char *md5_hash,
                     gboolean use_subdomain);



/* Interpret the response to an S3 operation, assuming CURL completed its request
 * successfully.  This function fills in the relevant C{hdl->last*} members.
 *
 * @param hdl: The S3Handle object
 * @param body: the response body
 * @param body_len: the length of the response body
 * @param etag: The response's ETag header
 * @param content_md5: The hex-encoded MD5 hash of the request body,
 *     which will be checked against the response's ETag header.
 *     If NULL, the header is not checked.
 *     If non-NULL, then the body should have the response headers at its beginnning.
 * @returns: TRUE if the response should be retried (e.g., network error)
 */
static gboolean
interpret_response(S3Handle *hdl,
                   CURLcode curl_code,
                   char *curl_error_buffer,
                   gchar *body,
                   guint body_len,
                   const char *etag,
                   const char *content_md5);

/* Perform an S3 operation.  This function handles all of the details
 * of retryig requests and so on.
 *
 * The concepts of bucket and keys are defined by the Amazon S3 API.
 * See: "Components of Amazon S3" - API Version 2006-03-01 pg. 8
 *
 * Individual sub-resources are defined in several places. In the REST API,
 * they they are represented by a "flag" in the "query string".
 * See: "Constructing the CanonicalizedResource Element" - API Version 2006-03-01 pg. 60
 *
 * @param hdl: the S3Handle object
 * @param verb: the HTTP request method
 * @param bucket: the bucket to access, or NULL for none
 * @param key: the key to access, or NULL for none
 * @param subresource: the "sub-resource" to request (e.g. "acl") or NULL for none
 * @param query: the query string to send (not including th initial '?'),
 * or NULL for none
 * @param read_func: the callback for reading data
 *   Will use s3_empty_read_func if NULL is passed in.
 * @param read_reset_func: the callback for to reset reading data
 * @param size_func: the callback to get the number of bytes to upload
 * @param md5_func: the callback to get the MD5 hash of the data to upload
 * @param read_data: pointer to pass to the above functions
 * @param write_func: the callback for writing data.
 *   Will use s3_counter_write_func if NULL is passed in.
 * @param write_reset_func: the callback for to reset writing data
 * @param write_data: pointer to pass to C{write_func}
 * @param progress_func: the callback for progress information
 * @param progress_data: pointer to pass to C{progress_func}
 * @param result_handling: instructions for handling the results; see above.
 * @returns: the result specified by result_handling; details of the response
 * are then available in C{hdl->last*}
 */
static s3_result_t
perform_request(S3Handle *hdl,
                const char *verb,
                const char *bucket,
                const char *key,
                const char *subresource,
                const char *query,
                s3_read_func read_func,
                s3_reset_func read_reset_func,
                s3_size_func size_func,
                s3_md5_func md5_func,
                gpointer read_data,
                s3_write_func write_func,
                s3_reset_func write_reset_func,
                gpointer write_data,
                s3_progress_func progress_func,
                gpointer progress_data,
                const result_handling_t *result_handling);

/*
 * a CURLOPT_WRITEFUNCTION to save part of the response in memory and
 * call an external function if one was provided.
 */
static size_t
s3_internal_write_func(void *ptr, size_t size, size_t nmemb, void * stream);

/*
 * a function to reset to our internal buffer
 */
static void
s3_internal_reset_func(void * stream);

/*
 * a CURLOPT_HEADERFUNCTION to save the ETag header only.
 */
static size_t
s3_internal_header_func(void *ptr, size_t size, size_t nmemb, void * stream);

static gboolean
compile_regexes(void);

/*
 * Static function implementations
 */
static s3_error_code_t
s3_error_code_from_name(char *s3_error_name)
{
    int i;

    if (!s3_error_name) return S3_ERROR_Unknown;

    /* do a brute-force search through the list, since it's not sorted */
    for (i = 0; i < S3_ERROR_END; i++) {
        if (g_strcasecmp(s3_error_name, s3_error_code_names[i]) == 0)
            return i;
    }

    return S3_ERROR_Unknown;
}

static const char *
s3_error_name_from_code(s3_error_code_t s3_error_code)
{
    if (s3_error_code >= S3_ERROR_END)
        s3_error_code = S3_ERROR_Unknown;

    return s3_error_code_names[s3_error_code];
}

gboolean
s3_curl_supports_ssl(void)
{
    static int supported = -1;
    if (supported == -1) {
#if defined(CURL_VERSION_SSL)
    curl_version_info_data *info = curl_version_info(CURLVERSION_NOW);
    if (info->features & CURL_VERSION_SSL)
        supported = 1;
    else
        supported = 0;
#else
    supported = 0;
#endif
    }

    return supported;
}

static s3_result_t
lookup_result(const result_handling_t *result_handling,
              guint response_code,
              s3_error_code_t s3_error_code,
              CURLcode curl_code)
{
    while (result_handling->response_code
        || result_handling->s3_error_code
        || result_handling->curl_code) {
        if ((result_handling->response_code && result_handling->response_code != response_code)
         || (result_handling->s3_error_code && result_handling->s3_error_code != s3_error_code)
         || (result_handling->curl_code && result_handling->curl_code != curl_code)) {
            result_handling++;
            continue;
        }

        return result_handling->result;
    }

    /* return the result for the terminator, as the default */
    return result_handling->result;
}

static gboolean
is_non_empty_string(const char *str)
{
    return str && str[0] != '\0';
}

static char *
build_url(const char *bucket,
      const char *key,
      const char *subresource,
      const char *query,
      gboolean use_subdomain,
      gboolean use_ssl)
{
    GString *url = NULL;
    char *esc_bucket = NULL, *esc_key = NULL;

    /* scheme */
    url = g_string_new("http");
    if (use_ssl)
        g_string_append(url, "s");

    g_string_append(url, "://");

    /* domain */
    if (use_subdomain && bucket)
        g_string_append_printf(url, "%s.s3.amazonaws.com/", bucket);
    else
        g_string_append(url, "s3.amazonaws.com/");

    /* path */
    if (!use_subdomain && bucket) {
        esc_bucket = curl_escape(bucket, 0);
    if (!esc_bucket) goto cleanup;
        g_string_append_printf(url, "%s", esc_bucket);
        if (key)
            g_string_append(url, "/");
    }

    if (key) {
        esc_key = curl_escape(key, 0);
    if (!esc_key) goto cleanup;
        g_string_append_printf(url, "%s", esc_key);
    }

    /* query string */
    if (subresource || query)
        g_string_append(url, "?");

    if (subresource)
        g_string_append(url, subresource);

    if (subresource && query)
        g_string_append(url, "&");

    if (query)
        g_string_append(url, query);

cleanup:
    if (esc_bucket) curl_free(esc_bucket);
    if (esc_key) curl_free(esc_key);

    return g_string_free(url, FALSE);
}

static struct curl_slist *
authenticate_request(S3Handle *hdl,
                     const char *verb,
                     const char *bucket,
                     const char *key,
                     const char *subresource,
                     const char *md5_hash,
                     gboolean use_subdomain)
{
    time_t t;
    struct tm tmp;
    char *date = NULL;
    char *buf = NULL;
    HMAC_CTX ctx;
    GByteArray *md = NULL;
    char *auth_base64 = NULL;
    struct curl_slist *headers = NULL;
    char *esc_bucket = NULL, *esc_key = NULL;
    GString *auth_string = NULL;

    /* From RFC 2616 */
    static const char *wkday[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    static const char *month[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};



    /* Build the string to sign, per the S3 spec.
     * See: "Authenticating REST Requests" - API Version 2006-03-01 pg 58
     */

    /* verb */
    auth_string = g_string_new(verb);
    g_string_append(auth_string, "\n");

    /* Content-MD5 header */
    if (md5_hash)
        g_string_append(auth_string, md5_hash);
    g_string_append(auth_string, "\n");

    /* Content-Type is empty*/
    g_string_append(auth_string, "\n");


    /* calculate the date */
    t = time(NULL);
#ifdef _WIN32
    if (!gmtime_s(&tmp, &t)) g_debug("localtime error");
#else
    if (!gmtime_r(&t, &tmp)) perror("localtime");
#endif
    date = g_strdup_printf("%s, %02d %s %04d %02d:%02d:%02d GMT",
        wkday[tmp.tm_wday], tmp.tm_mday, month[tmp.tm_mon], 1900+tmp.tm_year,
        tmp.tm_hour, tmp.tm_min, tmp.tm_sec);

    g_string_append(auth_string, date);
    g_string_append(auth_string, "\n");

    if (is_non_empty_string(hdl->user_token)) {
        g_string_append(auth_string, AMAZON_SECURITY_HEADER);
        g_string_append(auth_string, ":");
        g_string_append(auth_string, hdl->user_token);
        g_string_append(auth_string, ",");
        g_string_append(auth_string, STS_PRODUCT_TOKEN);
        g_string_append(auth_string, "\n");
    }

    /* CanonicalizedResource */
    g_string_append(auth_string, "/");
    if (bucket) {
        if (use_subdomain)
            g_string_append(auth_string, bucket);
        else {
            esc_bucket = curl_escape(bucket, 0);
            if (!esc_bucket) goto cleanup;
            g_string_append(auth_string, esc_bucket);
        }
    }

    if (bucket && (use_subdomain || key))
        g_string_append(auth_string, "/");

    if (key) {
            esc_key = curl_escape(key, 0);
            if (!esc_key) goto cleanup;
            g_string_append(auth_string, esc_key);
    }

    if (subresource) {
        g_string_append(auth_string, "?");
        g_string_append(auth_string, subresource);
    }

    /* run HMAC-SHA1 on the canonicalized string */
    md = g_byte_array_sized_new(EVP_MAX_MD_SIZE+1);
    HMAC_CTX_init(&ctx);
    HMAC_Init_ex(&ctx, hdl->secret_key, (int) strlen(hdl->secret_key), EVP_sha1(), NULL);
    HMAC_Update(&ctx, (unsigned char*) auth_string->str, auth_string->len);
    HMAC_Final(&ctx, md->data, &md->len);
    HMAC_CTX_cleanup(&ctx);
    auth_base64 = s3_base64_encode(md);

    /* append the new headers */
    if (is_non_empty_string(hdl->user_token)) {
        /* Devpay headers are included in hash. */
        buf = g_strdup_printf(AMAZON_SECURITY_HEADER ": %s", hdl->user_token);
        headers = curl_slist_append(headers, buf);
        g_free(buf);

        buf = g_strdup_printf(AMAZON_SECURITY_HEADER ": %s", STS_PRODUCT_TOKEN);
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }

    buf = g_strdup_printf("Authorization: AWS %s:%s",
                          hdl->access_key, auth_base64);
    headers = curl_slist_append(headers, buf);
    g_free(buf);

    if (md5_hash && '\0' != md5_hash[0]) {
        buf = g_strdup_printf("Content-MD5: %s", md5_hash);
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }

    buf = g_strdup_printf("Date: %s", date);
    headers = curl_slist_append(headers, buf);
    g_free(buf);
cleanup:
    g_free(date);
    g_free(esc_bucket);
    g_free(esc_key);
    g_byte_array_free(md, TRUE);
    g_free(auth_base64);
    g_string_free(auth_string, TRUE);

    return headers;
}

static gboolean
interpret_response(S3Handle *hdl,
                   CURLcode curl_code,
                   char *curl_error_buffer,
                   gchar *body,
                   guint body_len,
                   const char *etag,
                   const char *content_md5)
{
    long response_code = 0;
    regmatch_t pmatch[2];
    char *error_name = NULL, *message = NULL;
    char *body_copy = NULL;
    gboolean ret = TRUE;

    if (!hdl) return FALSE;

    if (hdl->last_message) g_free(hdl->last_message);
    hdl->last_message = NULL;

    /* bail out from a CURL error */
    if (curl_code != CURLE_OK) {
        hdl->last_curl_code = curl_code;
        hdl->last_message = g_strdup_printf("CURL error: %s", curl_error_buffer);
        return FALSE;
    }

    /* CURL seems to think things were OK, so get its response code */
    curl_easy_getinfo(hdl->curl, CURLINFO_RESPONSE_CODE, &response_code);
    hdl->last_response_code = response_code;

    /* check ETag, if present */
    if (etag && content_md5 && 200 == response_code) {
        if (etag && g_strcasecmp(etag, content_md5))
            hdl->last_message = g_strdup("S3 Error: Possible data corruption (ETag returned by Amazon did not match the MD5 hash of the data sent)");
        else
            ret = FALSE;
        return ret;
    }

    if (200 <= response_code && response_code < 400) {
        /* 2xx and 3xx codes won't have a response body we care about */
        hdl->last_s3_error_code = S3_ERROR_None;
        return FALSE;
    }

    /* Now look at the body to try to get the actual Amazon error message. Rather
     * than parse out the XML, just use some regexes. */

    /* impose a reasonable limit on body size */
    if (body_len > MAX_ERROR_RESPONSE_LEN) {
        hdl->last_message = g_strdup("S3 Error: Unknown (response body too large to parse)");
        return FALSE;
    } else if (!body || body_len == 0) {
        hdl->last_message = g_strdup("S3 Error: Unknown (empty response body)");
        return TRUE; /* perhaps a network error; retry the request */
    }

    /* use strndup to get a zero-terminated string */
    body_copy = g_strndup(body, body_len);
    if (!body_copy) goto cleanup;

    if (!s3_regexec_wrap(&error_name_regex, body_copy, 2, pmatch, 0))
        error_name = find_regex_substring(body_copy, pmatch[1]);

    if (!s3_regexec_wrap(&message_regex, body_copy, 2, pmatch, 0))
        message = find_regex_substring(body_copy, pmatch[1]);

    if (error_name) {
        hdl->last_s3_error_code = s3_error_code_from_name(error_name);
    }

    if (message) {
        hdl->last_message = message;
        message = NULL; /* steal the reference to the string */
    }

cleanup:
    g_free(body_copy);
    g_free(message);
    g_free(error_name);

    return FALSE;
}

/* a CURLOPT_READFUNCTION to read data from a buffer. */
size_t
s3_buffer_read_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    CurlBuffer *data = stream;
    guint bytes_desired = (guint) size * nmemb;

    /* check the number of bytes remaining, just to be safe */
    if (bytes_desired > data->buffer_len - data->buffer_pos)
        bytes_desired = data->buffer_len - data->buffer_pos;

    memcpy((char *)ptr, data->buffer + data->buffer_pos, bytes_desired);
    data->buffer_pos += bytes_desired;

    return bytes_desired;
}

size_t
s3_buffer_size_func(void *stream)
{
    CurlBuffer *data = stream;
    return data->buffer_len;
}

GByteArray*
s3_buffer_md5_func(void *stream)
{
    CurlBuffer *data = stream;
    GByteArray req_body_gba = {(guint8 *)data->buffer, data->buffer_len};

    return s3_compute_md5_hash(&req_body_gba);
}

void
s3_buffer_reset_func(void *stream)
{
    CurlBuffer *data = stream;
    data->buffer_pos = 0;
}

/* a CURLOPT_WRITEFUNCTION to write data to a buffer. */
size_t
s3_buffer_write_func(void *ptr, size_t size, size_t nmemb, void *stream)
{
    CurlBuffer * data = stream;
    guint new_bytes = (guint) size * nmemb;
    guint bytes_needed = data->buffer_pos + new_bytes;

    /* error out if the new size is greater than the maximum allowed */
    if (data->max_buffer_size && bytes_needed > data->max_buffer_size)
        return 0;

    /* reallocate if necessary. We use exponential sizing to make this
     * happen less often. */
    if (bytes_needed > data->buffer_len) {
        guint new_size = MAX(bytes_needed, data->buffer_len * 2);
        if (data->max_buffer_size) {
            new_size = MIN(new_size, data->max_buffer_size);
        }
        data->buffer = g_realloc(data->buffer, new_size);
        data->buffer_len = new_size;
    }
    if (!data->buffer)
        return 0; /* returning zero signals an error to libcurl */

    /* actually copy the data to the buffer */
    memcpy(data->buffer + data->buffer_pos, ptr, new_bytes);
    data->buffer_pos += new_bytes;

    /* signal success to curl */
    return new_bytes;
}

/* a CURLOPT_READFUNCTION that writes nothing. */
size_t
s3_empty_read_func(G_GNUC_UNUSED void *ptr, G_GNUC_UNUSED size_t size, G_GNUC_UNUSED size_t nmemb, G_GNUC_UNUSED void * stream)
{
    return 0;
}

size_t
s3_empty_size_func(G_GNUC_UNUSED void *stream)
{
    return 0;
}

GByteArray*
s3_empty_md5_func(G_GNUC_UNUSED void *stream)
{
    static const GByteArray empty = {(guint8 *) "", 0};

    return s3_compute_md5_hash(&empty);
}

/* a CURLOPT_WRITEFUNCTION to write data that just counts data.
 * s3_write_data should be NULL or a pointer to an gint64.
 */
size_t
s3_counter_write_func(G_GNUC_UNUSED void *ptr, size_t size, size_t nmemb, void *stream)
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
size_t
s3_file_read_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    HANDLE *hFile = (HANDLE *) stream;
    DWORD bytes_read;

    ReadFile(hFile, ptr, (DWORD) size*nmemb, &bytes_read, NULL);
    return bytes_read;
}

size_t
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
size_t
s3_file_write_func(void *ptr, size_t size, size_t nmemb, void *stream)
{
    HANDLE *hFile = (HANDLE *) stream;
    DWORD bytes_written;

    WriteFile(hFile, ptr, (DWORD) size*nmemb, &bytes_written, NULL);
    return bytes_written;
}
#endif

static int
curl_debug_message(CURL *curl G_GNUC_UNUSED,
           curl_infotype type,
           char *s,
           size_t len,
           void *unused G_GNUC_UNUSED)
{
    char *lineprefix;
    char *message;
    char **lines, **line;

    switch (type) {
    case CURLINFO_TEXT:
        lineprefix="";
        break;

    case CURLINFO_HEADER_IN:
        lineprefix="Hdr In: ";
        break;

    case CURLINFO_HEADER_OUT:
        lineprefix="Hdr Out: ";
        break;

    default:
        /* ignore data in/out -- nobody wants to see that in the
         * debug logs! */
        return 0;
    }

    /* split the input into lines */
    message = g_strndup(s, (gsize) len);
    lines = g_strsplit(message, "\n", -1);
    g_free(message);

    for (line = lines; *line; line++) {
    if (**line == '\0') continue; /* skip blank lines */
    g_debug("%s%s", lineprefix, *line);
    }
    g_strfreev(lines);

    return 0;
}

static s3_result_t
perform_request(S3Handle *hdl,
                const char *verb,
                const char *bucket,
                const char *key,
                const char *subresource,
                const char *query,
                s3_read_func read_func,
                s3_reset_func read_reset_func,
                s3_size_func size_func,
                s3_md5_func md5_func,
                gpointer read_data,
                s3_write_func write_func,
                s3_reset_func write_reset_func,
                gpointer write_data,
                s3_progress_func progress_func,
                gpointer progress_data,
                const result_handling_t *result_handling)
{
    gboolean use_subdomain;
    char *url = NULL;
    s3_result_t result = S3_RESULT_FAIL; /* assume the worst.. */
    CURLcode curl_code = CURLE_OK;
    char curl_error_buffer[CURL_ERROR_SIZE] = "";
    struct curl_slist *headers = NULL;
    S3InternalData int_writedata = {{NULL, 0, 0, MAX_ERROR_RESPONSE_LEN}, NULL, NULL, NULL, FALSE, FALSE, NULL};
    gboolean should_retry;
    guint retries = 0;
    gulong backoff = EXPONENTIAL_BACKOFF_START_USEC;
    /* corresponds to PUT, HEAD, GET, and POST */
    int curlopt_upload = 0, curlopt_nobody = 0, curlopt_httpget = 0, curlopt_post = 0;
    /* do we want to examine the headers */
    const char *curlopt_customrequest = NULL;
    /* for MD5 calculation */
    GByteArray *md5_hash = NULL;
    gchar *md5_hash_hex = NULL, *md5_hash_b64 = NULL;
    size_t request_body_size = 0;

    g_assert(hdl != NULL && hdl->curl != NULL);

    s3_reset(hdl);

    use_subdomain = is_non_empty_string(hdl->bucket_location);
    url = build_url(bucket, key, subresource, query, use_subdomain, hdl->use_ssl);
    if (!url) goto cleanup;

    /* libcurl may behave strangely if these are not set correctly */
    if (!strncmp(verb, "PUT", 4)) {
        curlopt_upload = 1;
    } else if (!strncmp(verb, "GET", 4)) {
        curlopt_httpget = 1;
    } else if (!strncmp(verb, "POST", 5)) {
        curlopt_post = 1;
    } else if (!strncmp(verb, "HEAD", 5)) {
        curlopt_nobody = 1;
    } else {
        curlopt_customrequest = verb;
    }

    if (size_func) {
        request_body_size = size_func(read_data);
    }
    if (md5_func) {

        md5_hash = md5_func(read_data);
        if (md5_hash) {
            md5_hash_b64 = s3_base64_encode(md5_hash);
            md5_hash_hex = s3_hex_encode(md5_hash);
            g_byte_array_free(md5_hash, TRUE);
        }
    }
    if (!read_func) {
        /* Curl will use fread() otherwise */
        read_func = s3_empty_read_func;
    }

    if (write_func) {
        int_writedata.write_func = write_func;
        int_writedata.reset_func = write_reset_func;
        int_writedata.write_data = write_data;
    } else {
        /* Curl will use fwrite() otherwise */
        int_writedata.write_func = s3_counter_write_func;
        int_writedata.reset_func = s3_counter_reset_func;
        int_writedata.write_data = NULL;
    }

    while (1) {
        /* reset things */
        if (headers) {
            curl_slist_free_all(headers);
        }
        curl_error_buffer[0] = '\0';
        if (read_reset_func) {
            read_reset_func(read_data);
        }
        /* calls write_reset_func */
        s3_internal_reset_func(&int_writedata);

        /* set up the request */
        headers = authenticate_request(hdl, verb, bucket, key, subresource,
            md5_hash_b64, is_non_empty_string(hdl->bucket_location));

        if (hdl->use_ssl && hdl->ca_info) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_CAINFO, hdl->ca_info)))
                goto curl_error;
        }

        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_VERBOSE, hdl->verbose)))
            goto curl_error;
        if (hdl->verbose) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_DEBUGFUNCTION,
                              curl_debug_message)))
                goto curl_error;
        }
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_ERRORBUFFER,
                                          curl_error_buffer)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_NOPROGRESS, 1)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_FOLLOWLOCATION, 1)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_URL, url)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_HTTPHEADER,
                                          headers)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_WRITEFUNCTION, s3_internal_write_func)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_WRITEDATA, &int_writedata)))
            goto curl_error;
        /* Note: we always have to set this apparently, for consistent "end of header" detection */
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_HEADERFUNCTION, s3_internal_header_func)))
            goto curl_error;
        /* Note: if set, CURLOPT_HEADERDATA seems to also be used for CURLOPT_WRITEDATA ? */
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_HEADERDATA, &int_writedata)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_PROGRESSFUNCTION, progress_func)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_PROGRESSDATA, progress_data)))
            goto curl_error;

#ifdef CURLOPT_INFILESIZE_LARGE
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)request_body_size)))
            goto curl_error;
#else
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_INFILESIZE, (long)request_body_size)))
            goto curl_error;
#endif

        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_HTTPGET, curlopt_httpget)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_UPLOAD, curlopt_upload)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_POST, curlopt_post)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_NOBODY, curlopt_nobody)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_CUSTOMREQUEST,
                                          curlopt_customrequest)))
            goto curl_error;


        if (curlopt_upload) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READFUNCTION, read_func)))
                goto curl_error;
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READDATA, read_data)))
                goto curl_error;
        } else {
            /* Clear request_body options. */
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READFUNCTION,
                                              NULL)))
                goto curl_error;
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READDATA,
                                              NULL)))
                goto curl_error;
        }

        /* Perform the request */
        curl_code = curl_easy_perform(hdl->curl);


        /* interpret the response into hdl->last* */
    curl_error: /* (label for short-circuiting the curl_easy_perform call) */
        should_retry = interpret_response(hdl, curl_code, curl_error_buffer,
            int_writedata.resp_buf.buffer, int_writedata.resp_buf.buffer_pos, int_writedata.etag, md5_hash_hex);

        /* and, unless we know we need to retry, see what we're to do now */
        if (!should_retry) {
            result = lookup_result(result_handling, hdl->last_response_code,
                                   hdl->last_s3_error_code, hdl->last_curl_code);

            /* break out of the while(1) unless we're retrying */
            if (result != S3_RESULT_RETRY)
                break;
        }

        if (retries >= EXPONENTIAL_BACKOFF_MAX_RETRIES) {
            /* we're out of retries, so annotate hdl->last_message appropriately and bail
             * out. */
            char *m = g_strdup_printf("Too many retries; last message was '%s'", hdl->last_message);
            if (hdl->last_message) g_free(hdl->last_message);
            hdl->last_message = m;
            result = S3_RESULT_FAIL;
            break;
        }

        g_usleep(backoff);
        retries++;
        backoff *= EXPONENTIAL_BACKOFF_BASE;
    }

    if (result != S3_RESULT_OK) {
        g_debug(_("%s %s failed with %d/%s"), verb, url,
                hdl->last_response_code,
                s3_error_name_from_code(hdl->last_s3_error_code));
    }

cleanup:
    g_free(url);
    if (headers) curl_slist_free_all(headers);
    g_free(md5_hash_b64);
    g_free(md5_hash_hex);

    /* we don't deallocate the response body -- we keep it for later */
    hdl->last_response_body = int_writedata.resp_buf.buffer;
    hdl->last_response_body_size = int_writedata.resp_buf.buffer_pos;
    hdl->last_num_retries = retries;

    return result;
}


static size_t
s3_internal_write_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    S3InternalData *data = (S3InternalData *) stream;
    size_t bytes_saved;

    if (!data->headers_done)
        return size*nmemb;

    /* call write on internal buffer (if not full) */
    if (data->int_write_done) {
        bytes_saved = 0;
    } else {
        bytes_saved = s3_buffer_write_func(ptr, size, nmemb, &data->resp_buf);
        if (!bytes_saved) {
            data->int_write_done = TRUE;
        }
    }
    /* call write on user buffer */
    if (data->write_func) {
        return data->write_func(ptr, size, nmemb, data->write_data);
    } else {
        return bytes_saved;
    }
}

static void
s3_internal_reset_func(void * stream)
{
    S3InternalData *data = (S3InternalData *) stream;

    s3_buffer_reset_func(&data->resp_buf);
    data->headers_done = FALSE;
    data->int_write_done = FALSE;
    data->etag = NULL;
    if (data->reset_func) {
        data->reset_func(data->write_data);
    }
}

static size_t
s3_internal_header_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    static const char *final_header = "\r\n";
    char *header;
    regmatch_t pmatch[2];
    S3InternalData *data = (S3InternalData *) stream;

    header = g_strndup((gchar *) ptr, (gsize) size*nmemb);
    if (!s3_regexec_wrap(&etag_regex, header, 2, pmatch, 0))
            data->etag = find_regex_substring(header, pmatch[1]);
    if (!strcmp(final_header, header))
        data->headers_done = TRUE;

    return size*nmemb;
}

static gboolean
compile_regexes(void)
{
#ifdef HAVE_REGEX_H

  /* using POSIX regular expressions */
  struct {const char * str; int flags; regex_t *regex;} regexes[] = {
        {"<Code>[[:space:]]*([^<]*)[[:space:]]*</Code>", REG_EXTENDED | REG_ICASE, &error_name_regex},
        {"^ETag:[[:space:]]*\"([^\"]+)\"[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &etag_regex},
        {"<Message>[[:space:]]*([^<]*)[[:space:]]*</Message>", REG_EXTENDED | REG_ICASE, &message_regex},
        {"^[a-z0-9](-*[a-z0-9]){2,62}$", REG_EXTENDED | REG_NOSUB, &subdomain_regex},
        {"(/>)|(>([^<]*)</LocationConstraint>)", REG_EXTENDED | REG_ICASE, &location_con_regex},
        {NULL, 0, NULL}
    };
    char regmessage[1024];
    int size, i;
    int reg_result;

    for (i = 0; regexes[i].str; i++) {
        reg_result = regcomp(regexes[i].regex, regexes[i].str, regexes[i].flags);
        if (reg_result != 0) {
            size = regerror(reg_result, regexes[i].regex, regmessage, sizeof(regmessage));
            g_error(_("Regex error: %s"), regmessage);
            return FALSE;
        }
    }
#else /* ! HAVE_REGEX_H */
  /* using PCRE via GLib */
  struct {const char * str; int flags; regex_t *regex;} regexes[] = {
        {"<Code>\\s*([^<]*)\\s*</Code>",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &error_name_regex},
        {"^ETag:\\s*\"([^\"]+)\"\\s*$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &etag_regex},
        {"<Message>\\s*([^<]*)\\s*</Message>",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &message_regex},
        {"^[a-z0-9]((-*[a-z0-9])|(\\.[a-z0-9])){2,62}$",
         G_REGEX_OPTIMIZE | G_REGEX_NO_AUTO_CAPTURE,
         &subdomain_regex},
        {"(/>)|(>([^<]*)</LocationConstraint>)",
         G_REGEX_CASELESS,
         &location_con_regex},
        {NULL, 0, NULL}
  };
  int i;
  GError *err = NULL;

  for (i = 0; regexes[i].str; i++) {
      *(regexes[i].regex) = g_regex_new(regexes[i].str, regexes[i].flags, 0, &err);
      if (err) {
          g_error(_("Regex error: %s"), err->message);
          g_error_free(err);
          return FALSE;
      }
  }
#endif
    return TRUE;
}

/*
 * Public function implementations
 */

gboolean s3_init(void)
{
    static GStaticMutex mutex = G_STATIC_MUTEX_INIT;
    static gboolean init = FALSE, ret;

    /* n.b. curl_global_init is called in common-src/glib-util.c:glib_init() */

    g_static_mutex_lock (&mutex);
    if (!init) {
        ret = compile_regexes();
        init = TRUE;
    }
    g_static_mutex_unlock(&mutex);
    return ret;
}

gboolean
s3_curl_location_compat(void)
{
    curl_version_info_data *info;

    info = curl_version_info(CURLVERSION_NOW);
    return info->version_num > 0x070a02;
}

gboolean
s3_bucket_location_compat(const char *bucket)
{
    return !s3_regexec_wrap(&subdomain_regex, bucket, 0, NULL, 0);
}

S3Handle *
s3_open(const char *access_key,
        const char *secret_key,
        const char *user_token,
        const char *bucket_location,
        const char *ca_info
        ) {
    S3Handle *hdl;

    hdl = g_new0(S3Handle, 1);
    if (!hdl) goto error;

    hdl->verbose = FALSE;
    hdl->use_ssl = s3_curl_supports_ssl();

    g_assert(access_key);
    hdl->access_key = g_strdup(access_key);
    g_assert(secret_key);
    hdl->secret_key = g_strdup(secret_key);
    /* NULL is okay */
    hdl->user_token = g_strdup(user_token);

    /* NULL is okay */
    hdl->bucket_location = g_strdup(bucket_location);

    /* NULL is okay */
    hdl->ca_info = g_strdup(ca_info);

    hdl->curl = curl_easy_init();
    if (!hdl->curl) goto error;

    return hdl;

error:
    s3_free(hdl);
    return NULL;
}

void
s3_free(S3Handle *hdl)
{
    s3_reset(hdl);

    if (hdl) {
        g_free(hdl->access_key);
        g_free(hdl->secret_key);
        if (hdl->user_token) g_free(hdl->user_token);
        if (hdl->bucket_location) g_free(hdl->bucket_location);
        if (hdl->curl) curl_easy_cleanup(hdl->curl);

        g_free(hdl);
    }
}

void
s3_reset(S3Handle *hdl)
{
    if (hdl) {
        /* We don't call curl_easy_reset here, because doing that in curl
         * < 7.16 blanks the default CA certificate path, and there's no way
         * to get it back. */
        if (hdl->last_message) {
            g_free(hdl->last_message);
            hdl->last_message = NULL;
        }

        hdl->last_response_code = 0;
        hdl->last_curl_code = 0;
        hdl->last_s3_error_code = 0;
        hdl->last_num_retries = 0;

        if (hdl->last_response_body) {
            g_free(hdl->last_response_body);
            hdl->last_response_body = NULL;
        }

        hdl->last_response_body_size = 0;
    }
}

void
s3_error(S3Handle *hdl,
         const char **message,
         guint *response_code,
         s3_error_code_t *s3_error_code,
         const char **s3_error_name,
         CURLcode *curl_code,
         guint *num_retries)
{
    if (hdl) {
        if (message) *message = hdl->last_message;
        if (response_code) *response_code = hdl->last_response_code;
        if (s3_error_code) *s3_error_code = hdl->last_s3_error_code;
        if (s3_error_name) *s3_error_name = s3_error_name_from_code(hdl->last_s3_error_code);
        if (curl_code) *curl_code = hdl->last_curl_code;
        if (num_retries) *num_retries = hdl->last_num_retries;
    } else {
        /* no hdl? return something coherent, anyway */
        if (message) *message = "NULL S3Handle";
        if (response_code) *response_code = 0;
        if (s3_error_code) *s3_error_code = 0;
        if (s3_error_name) *s3_error_name = NULL;
        if (curl_code) *curl_code = 0;
        if (num_retries) *num_retries = 0;
    }
}

void
s3_verbose(S3Handle *hdl, gboolean verbose)
{
    hdl->verbose = verbose;
}

gboolean
s3_use_ssl(S3Handle *hdl, gboolean use_ssl)
{
    gboolean ret = TRUE;
    if (use_ssl & !s3_curl_supports_ssl()) {
        ret = FALSE;
    } else {
        hdl->use_ssl = use_ssl;
    }
    return ret;
}

char *
s3_strerror(S3Handle *hdl)
{
    const char *message;
    guint response_code;
    const char *s3_error_name;
    CURLcode curl_code;
    guint num_retries;

    char s3_info[256] = "";
    char response_info[16] = "";
    char curl_info[32] = "";
    char retries_info[32] = "";

    s3_error(hdl, &message, &response_code, NULL, &s3_error_name, &curl_code, &num_retries);

    if (!message)
        message = "Unknown S3 error";
    if (s3_error_name)
        g_snprintf(s3_info, sizeof(s3_info), " (%s)", s3_error_name);
    if (response_code)
        g_snprintf(response_info, sizeof(response_info), " (HTTP %d)", response_code);
    if (curl_code)
        g_snprintf(curl_info, sizeof(curl_info), " (CURLcode %d)", curl_code);
    if (num_retries)
        g_snprintf(retries_info, sizeof(retries_info), " (after %d retries)", num_retries);

    return g_strdup_printf("%s%s%s%s%s", message, s3_info, curl_info, response_info, retries_info);
}

/* Perform an upload. When this function returns, KEY and
 * BUFFER remain the responsibility of the caller.
 *
 * @param self: the s3 device
 * @param bucket: the bucket to which the upload should be made
 * @param key: the key to which the upload should be made
 * @param buffer: the data to be uploaded
 * @param buffer_len: the length of the data to upload
 * @returns: false if an error ocurred
 */
gboolean
s3_upload(S3Handle *hdl,
          const char *bucket,
          const char *key,
          s3_read_func read_func,
          s3_reset_func reset_func,
          s3_size_func size_func,
          s3_md5_func md5_func,
          gpointer read_data,
          s3_progress_func progress_func,
          gpointer progress_data)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    g_assert(hdl != NULL);

    result = perform_request(hdl, "PUT", bucket, key, NULL, NULL,
                 read_func, reset_func, size_func, md5_func, read_data,
                 NULL, NULL, NULL, progress_func, progress_data,
                 result_handling);

    return result == S3_RESULT_OK;
}


/* Private structure for our "thunk", which tracks where the user is in the list
 * of keys. */
struct list_keys_thunk {
    GSList *filename_list; /* all pending filenames */

    gboolean in_contents; /* look for "key" entities in here */
    gboolean in_common_prefixes; /* look for "prefix" entities in here */

    gboolean is_truncated;
    gchar *next_marker;

    gboolean want_text;

    gchar *text;
    gsize text_len;
};

/* Functions for a SAX parser to parse the XML from Amazon */

static void
list_start_element(GMarkupParseContext *context G_GNUC_UNUSED,
                   const gchar *element_name,
                   const gchar **attribute_names G_GNUC_UNUSED,
                   const gchar **attribute_values G_GNUC_UNUSED,
                   gpointer user_data,
                   GError **error G_GNUC_UNUSED)
{
    struct list_keys_thunk *thunk = (struct list_keys_thunk *)user_data;

    thunk->want_text = 0;
    if (g_strcasecmp(element_name, "contents") == 0) {
        thunk->in_contents = 1;
    } else if (g_strcasecmp(element_name, "commonprefixes") == 0) {
        thunk->in_common_prefixes = 1;
    } else if (g_strcasecmp(element_name, "prefix") == 0 && thunk->in_common_prefixes) {
        thunk->want_text = 1;
    } else if (g_strcasecmp(element_name, "key") == 0 && thunk->in_contents) {
        thunk->want_text = 1;
    } else if (g_strcasecmp(element_name, "istruncated")) {
        thunk->want_text = 1;
    } else if (g_strcasecmp(element_name, "nextmarker")) {
        thunk->want_text = 1;
    }
}

static void
list_end_element(GMarkupParseContext *context G_GNUC_UNUSED,
                 const gchar *element_name,
                 gpointer user_data,
                 GError **error G_GNUC_UNUSED)
{
    struct list_keys_thunk *thunk = (struct list_keys_thunk *)user_data;

    if (g_strcasecmp(element_name, "contents") == 0) {
        thunk->in_contents = 0;
    } else if (g_strcasecmp(element_name, "commonprefixes") == 0) {
        thunk->in_common_prefixes = 0;
    } else if (g_strcasecmp(element_name, "key") == 0 && thunk->in_contents) {
        thunk->filename_list = g_slist_prepend(thunk->filename_list, thunk->text);
        thunk->text = NULL;
    } else if (g_strcasecmp(element_name, "prefix") == 0 && thunk->in_common_prefixes) {
        thunk->filename_list = g_slist_prepend(thunk->filename_list, thunk->text);
        thunk->text = NULL;
    } else if (g_strcasecmp(element_name, "istruncated") == 0) {
        if (thunk->text && g_strncasecmp(thunk->text, "false", 5) != 0)
            thunk->is_truncated = TRUE;
    } else if (g_strcasecmp(element_name, "nextmarker") == 0) {
        if (thunk->next_marker) g_free(thunk->next_marker);
        thunk->next_marker = thunk->text;
        thunk->text = NULL;
    }
}

static void
list_text(GMarkupParseContext *context G_GNUC_UNUSED,
          const gchar *text,
          gsize text_len,
          gpointer user_data,
          GError **error G_GNUC_UNUSED)
{
    struct list_keys_thunk *thunk = (struct list_keys_thunk *)user_data;

    if (thunk->want_text) {
        if (thunk->text) g_free(thunk->text);
        thunk->text = g_strndup(text, text_len);
    }
}

/* Perform a fetch from S3; several fetches may be involved in a
 * single listing operation */
static s3_result_t
list_fetch(S3Handle *hdl,
           const char *bucket,
           const char *prefix,
           const char *delimiter,
           const char *marker,
           const char *max_keys,
           CurlBuffer *buf)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200, 0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };
   const char* pos_parts[][2] = {
        {"prefix", prefix},
        {"delimiter", delimiter},
        {"marker", marker},
        {"max-keys", max_keys},
        {NULL, NULL}
        };
    char *esc_value;
    GString *query;
    guint i;
    gboolean have_prev_part = FALSE;

    /* loop over possible parts to build query string */
    query = g_string_new("");
    for (i = 0; pos_parts[i][0]; i++) {
      if (pos_parts[i][1]) {
          if (have_prev_part)
              g_string_append(query, "&");
          else
              have_prev_part = TRUE;
          esc_value = curl_escape(pos_parts[i][1], 0);
          g_string_append_printf(query, "%s=%s", pos_parts[i][0], esc_value);
          curl_free(esc_value);
      }
    }

    /* and perform the request on that URI */
    result = perform_request(hdl, "GET", bucket, NULL, NULL, query->str,
                             NULL, NULL, NULL, NULL, NULL,
                             S3_BUFFER_WRITE_FUNCS, buf, NULL, NULL,
                             result_handling);

    if (query) g_string_free(query, TRUE);

    return result;
}

gboolean
s3_list_keys(S3Handle *hdl,
              const char *bucket,
              const char *prefix,
              const char *delimiter,
              GSList **list)
{
    /*
     * max len of XML variables:
     * bucket: 255 bytes (p12 API Version 2006-03-01)
     * key: 1024 bytes (p15 API Version 2006-03-01)
     * size per key: 5GB bytes (p6 API Version 2006-03-01)
     * size of size 10 bytes (i.e. 10 decimal digits)
     * etag: 44 (observed+assumed)
     * owner ID: 64 (observed+assumed)
     * owner DisplayName: 255 (assumed)
     * StorageClass: const (p18 API Version 2006-03-01)
     */
    static const guint MAX_RESPONSE_LEN = 1000*2000;
    static const char *MAX_KEYS = "1000";
    struct list_keys_thunk thunk;
    GMarkupParseContext *ctxt = NULL;
    static GMarkupParser parser = { list_start_element, list_end_element, list_text, NULL, NULL };
    GError *err = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    CurlBuffer buf = {NULL, 0, 0, MAX_RESPONSE_LEN};

    g_assert(list);
    *list = NULL;
    thunk.filename_list = NULL;
    thunk.text = NULL;
    thunk.next_marker = NULL;

    /* Loop until S3 has given us the entire picture */
    do {
        s3_buffer_reset_func(&buf);
        /* get some data from S3 */
        result = list_fetch(hdl, bucket, prefix, delimiter, thunk.next_marker, MAX_KEYS, &buf);
        if (result != S3_RESULT_OK) goto cleanup;

        /* run the parser over it */
        thunk.in_contents = FALSE;
        thunk.in_common_prefixes = FALSE;
        thunk.is_truncated = FALSE;
        thunk.want_text = FALSE;

        ctxt = g_markup_parse_context_new(&parser, 0, (gpointer)&thunk, NULL);

        if (!g_markup_parse_context_parse(ctxt, buf.buffer, buf.buffer_pos, &err)) {
            if (hdl->last_message) g_free(hdl->last_message);
            hdl->last_message = g_strdup(err->message);
            result = S3_RESULT_FAIL;
            goto cleanup;
        }

        if (!g_markup_parse_context_end_parse(ctxt, &err)) {
            if (hdl->last_message) g_free(hdl->last_message);
            hdl->last_message = g_strdup(err->message);
            result = S3_RESULT_FAIL;
            goto cleanup;
        }

        g_markup_parse_context_free(ctxt);
        ctxt = NULL;
    } while (thunk.next_marker);

cleanup:
    if (err) g_error_free(err);
    if (thunk.text) g_free(thunk.text);
    if (thunk.next_marker) g_free(thunk.next_marker);
    if (ctxt) g_markup_parse_context_free(ctxt);
    if (buf.buffer) g_free(buf.buffer);

    if (result != S3_RESULT_OK) {
        g_slist_free(thunk.filename_list);
        return FALSE;
    } else {
        *list = thunk.filename_list;
        return TRUE;
    }
}

gboolean
s3_read(S3Handle *hdl,
        const char *bucket,
        const char *key,
        s3_write_func write_func,
        s3_reset_func reset_func,
        gpointer write_data,
        s3_progress_func progress_func,
        gpointer progress_data)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200, 0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };

    g_assert(hdl != NULL);
    g_assert(write_func != NULL);

    result = perform_request(hdl, "GET", bucket, key, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, write_func, reset_func, write_data,
        progress_func, progress_data, result_handling);

    return result == S3_RESULT_OK;
}

gboolean
s3_delete(S3Handle *hdl,
          const char *bucket,
          const char *key)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 204,  0,                     0, S3_RESULT_OK },
        { 404,  S3_ERROR_NoSuchBucket, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0,                     0, /* default: */ S3_RESULT_FAIL  }
        };

    g_assert(hdl != NULL);

    result = perform_request(hdl, "DELETE", bucket, key, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 result_handling);

    return result == S3_RESULT_OK;
}

gboolean
s3_make_bucket(S3Handle *hdl,
               const char *bucket)
{
    char *body = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,                    0, S3_RESULT_OK },
        { 404, S3_ERROR_NoSuchBucket, 0, S3_RESULT_RETRY },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        };
    regmatch_t pmatch[4];
    char *loc_end_open, *loc_content;
    CurlBuffer buf = {NULL, 0, 0, 0}, *ptr = NULL;
    s3_read_func read_func = NULL;
    s3_reset_func reset_func = NULL;
    s3_md5_func md5_func = NULL;
    s3_size_func size_func = NULL;

    g_assert(hdl != NULL);

    if (is_non_empty_string(hdl->bucket_location) &&
        0 != strcmp(AMAZON_WILDCARD_LOCATION, hdl->bucket_location)) {
        if (s3_bucket_location_compat(bucket)) {
            ptr = &buf;
            buf.buffer = g_strdup_printf(AMAZON_BUCKET_CONF_TEMPLATE, hdl->bucket_location);
            buf.buffer_len = (guint) strlen(buf.buffer);
            buf.buffer_pos = 0;
            buf.max_buffer_size = buf.buffer_len;
            read_func = s3_buffer_read_func;
            reset_func = s3_buffer_reset_func;
            size_func = s3_buffer_size_func;
            md5_func = s3_buffer_md5_func;
        } else {
            hdl->last_message = g_strdup_printf(_(
                "Location constraint given for Amazon S3 bucket, "
                "but the bucket name (%s) is not usable as a subdomain."), bucket);
            return FALSE;
        }
    }

    result = perform_request(hdl, "PUT", bucket, NULL, NULL, NULL,
                 read_func, reset_func, size_func, md5_func, ptr,
                 NULL, NULL, NULL, NULL, NULL, result_handling);

   if (result == S3_RESULT_OK ||
       (is_non_empty_string(hdl->bucket_location) && result != S3_RESULT_OK
         && hdl->last_s3_error_code == S3_ERROR_BucketAlreadyOwnedByYou)) {
        /* verify the that the location constraint on the existing bucket matches
         * the one that's configured.
         */
        result = perform_request(hdl, "GET", bucket, NULL, "location", NULL,
                                 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                 NULL, NULL, result_handling);

        /* note that we can check only one of the three AND conditions above 
         * and infer that the others are true
         */
        if (result == S3_RESULT_OK && is_non_empty_string(hdl->bucket_location)) {
            /* return to the default state of failure */
            result = S3_RESULT_FAIL;

            if (body) g_free(body);
            /* use strndup to get a null-terminated string */
            body = g_strndup(hdl->last_response_body, hdl->last_response_body_size);
            if (!body) {
                hdl->last_message = g_strdup(_("No body received for location request"));
                goto cleanup;
            } else if ('\0' == body[0]) {
                hdl->last_message = g_strdup(_("Empty body received for location request"));
                goto cleanup;
            }

            if (!s3_regexec_wrap(&location_con_regex, body, 4, pmatch, 0)) {
                loc_end_open = find_regex_substring(body, pmatch[1]);
                loc_content = find_regex_substring(body, pmatch[3]);

                /* The case of an empty string is special because XML allows
                 * "self-closing" tags
                 */
                if (0 == strcmp(AMAZON_WILDCARD_LOCATION, hdl->bucket_location) &&
                    '/' != loc_end_open[0])
                    hdl->last_message = g_strdup(_("A wildcard location constraint is "
                        "configured, but the bucket has a non-empty location constraint"));
                else if (strcmp(AMAZON_WILDCARD_LOCATION, hdl->bucket_location)?
                    strncmp(loc_content, hdl->bucket_location, strlen(hdl->bucket_location)) :
                    ('\0' != loc_content[0]))
                    hdl->last_message = g_strdup(_("The location constraint configured "
                        "does not match the constraint currently on the bucket"));
                else
                    result = S3_RESULT_OK;
            } else {
                hdl->last_message = g_strdup(_("Unexpected location response from Amazon S3"));
            }
        }
   }

cleanup:
    if (body) g_free(body);

    return result == S3_RESULT_OK;

}

gboolean
s3_delete_bucket(S3Handle *hdl,
                 const char *bucket)
{
    return s3_delete(hdl, bucket, NULL);
}
