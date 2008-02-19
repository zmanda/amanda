/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* TODO
 * - Compute and send Content-MD5 header
 * - check SSL certificate
 * - collect speed statistics
 * - debugging mode
 */

#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <regex.h>
#include <time.h>
#include "util.h"
#include "amanda.h"
#include "s3.h"
#include "base64.h"
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

/*
 * Constants / definitions
 */

/* Maximum key length as specified in the S3 documentation
 * (*excluding* null terminator) */
#define S3_MAX_KEY_LENGTH 1024

#if defined(LIBCURL_FEATURE_SSL) && defined(LIBCURL_PROTOCOL_HTTPS)
# define S3_URL "https://s3.amazonaws.com"
#else
# define S3_URL "http://s3.amazonaws.com"
#endif

#define AMAZON_SECURITY_HEADER "x-amz-security-token"

/* parameters for exponential backoff in the face of retriable errors */

/* start at 0.01s */
#define EXPONENTIAL_BACKOFF_START_USEC 10000
/* double at each retry */
#define EXPONENTIAL_BACKOFF_BASE 2
/* retry 15 times (for a total of about 5 minutes spent waiting) */
#define EXPONENTIAL_BACKOFF_MAX_RETRIES 5

/* general "reasonable size" parameters */
#define MAX_ERROR_RESPONSE_LEN (100*1024)

/* Results which should always be retried */
#define RESULT_HANDLING_ALWAYS_RETRY \
        { 400,  S3_ERROR_RequestTimeout,     0,                         S3_RESULT_RETRY }, \
        { 409,  S3_ERROR_OperationAborted,   0,                         S3_RESULT_RETRY }, \
        { 412,  S3_ERROR_PreconditionFailed, 0,                         S3_RESULT_RETRY }, \
        { 500,  S3_ERROR_InternalError,      0,                         S3_RESULT_RETRY }, \
        { 501,  S3_ERROR_NotImplemented,     0,                         S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_COULDNT_CONNECT,     S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_PARTIAL_FILE,        S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_OPERATION_TIMEOUTED, S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_SEND_ERROR,          S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_RECV_ERROR,          S3_RESULT_RETRY }

/*
 * Data structures and associated functions
 */

struct S3Handle {
    /* (all strings in this struct are freed by s3_free()) */

    char *access_key;
    char *secret_key;
#ifdef WANT_DEVPAY
    char *user_token;
#endif

    CURL *curl;

    gboolean verbose;

    /* information from the last request */
    char *last_message;
    guint last_response_code;
    s3_error_code_t last_s3_error_code;
    CURLcode last_curl_code;
    guint last_num_retries;
    void *last_response_body;
    guint last_response_body_size;
};

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

static const char *error_name_regex_string = "<Code>[:space:]*([^<]*)[:space:]*</Code>";
static const char *message_regex_string = "<Message>[:space:]*([^<]*)[:space:]*</Message>";
static regex_t error_name_regex, message_regex;

/*
 * Utility functions
 */

/* Build a resource URI as /[bucket[/key]], with proper URL
 * escaping.
 *
 * The caller is responsible for freeing the resulting string.
 *
 * @param bucket: the bucket, or NULL if none is involved
 * @param key: the key within the bucket, or NULL if none is involved
 * @returns: completed URI
 */
static char *
build_resource(const char *bucket,
               const char *key);

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
 * @param resource: the resource being accessed
 */
static struct curl_slist *
authenticate_request(S3Handle *hdl,
                     const char *verb,
                     const char *resource);

/* Interpret the response to an S3 operation, assuming CURL completed its request
 * successfully.  This function fills in the relevant C{hdl->last*} members.
 *
 * @param hdl: The S3Handle object
 * @param body: the response body
 * @param body_len: the length of the response body
 * @returns: TRUE if the response should be retried (e.g., network error)
 */
static gboolean
interpret_response(S3Handle *hdl,
                   CURLcode curl_code,
                   char *curl_error_buffer,
                   void *body,
                   guint body_len);

/* Perform an S3 operation.  This function handles all of the details
 * of retryig requests and so on.
 * 
 * @param hdl: the S3Handle object
 * @param resource: the UTF-8 encoded resource to access
                    (without query parameters)
 * @param uri: the urlencoded URI to access at Amazon (may be identical to resource)
 * @param verb: the HTTP request method
 * @param request_body: the request body, or NULL if none should be sent
 * @param request_body_size: the length of the request body
 * @param max_response_size: the maximum number of bytes to accept in the
 * response, or 0 for no limit.
 * @param preallocate_response_size: for more efficient operation, preallocate
 * a buffer of this size for the response body.  Addition space will be allocated
 * if the response exceeds this size.
 * @param result_handling: instructions for handling the results; see above.
 * @returns: the result specified by result_handling; details of the response
 * are then available in C{hdl->last*}
 */
static s3_result_t
perform_request(S3Handle *hdl,
                const char *resource,
                const char *uri,
                const char *verb,
                const void *request_body,
                guint request_body_size,
                guint max_response_size,
                guint preallocate_response_size,
                const result_handling_t *result_handling);

/*
 * Static function implementations
 */

/* {{{ s3_error_code_from_name */
static s3_error_code_t
s3_error_code_from_name(char *s3_error_name)
{
    int i;

    if (!s3_error_name) return S3_ERROR_Unknown;

    /* do a brute-force search through the list, since it's not sorted */
    for (i = 0; i < S3_ERROR_END; i++) {
        if (strcasecmp(s3_error_name, s3_error_code_names[i]) == 0)
            return i;
    }

    return S3_ERROR_Unknown;
}
/* }}} */

/* {{{ s3_error_name_from_code */
static const char *
s3_error_name_from_code(s3_error_code_t s3_error_code)
{
    if (s3_error_code >= S3_ERROR_END)
        s3_error_code = S3_ERROR_Unknown;

    if (s3_error_code == 0)
        return NULL;

    return s3_error_code_names[s3_error_code];
}
/* }}} */

/* {{{ lookup_result */
static s3_result_t
lookup_result(const result_handling_t *result_handling,
              guint response_code,
              s3_error_code_t s3_error_code,
              CURLcode curl_code)
{
    g_return_val_if_fail(result_handling != NULL, S3_RESULT_FAIL);

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
/* }}} */

/* {{{ build_resource */
static char *
build_resource(const char *bucket,
               const char *key)
{
    char *esc_bucket = NULL, *esc_key = NULL;
    char *resource = NULL;

    if (bucket)
        if (!(esc_bucket = curl_escape(bucket, 0)))
            goto cleanup;

    if (key)
        if (!(esc_key = curl_escape(key, 0)))
            goto cleanup;

    if (esc_bucket) {
        if (esc_key) {
            resource = g_strdup_printf("/%s/%s", esc_bucket, esc_key);
        } else {
            resource = g_strdup_printf("/%s", esc_bucket);
        }
    } else {
        resource = g_strdup("/");
    }
cleanup:
    if (esc_bucket) curl_free(esc_bucket);
    if (esc_key) curl_free(esc_key);

    return resource;
}
/* }}} */

/* {{{ authenticate_request */
static struct curl_slist *
authenticate_request(S3Handle *hdl,
                     const char *verb,
                     const char *resource) 
{
    time_t t;
    struct tm tmp;
    char date[100];
    char * buf;
    HMAC_CTX ctx;
    char md_value[EVP_MAX_MD_SIZE+1];
    char auth_base64[40];
    unsigned int md_len;
    struct curl_slist *headers = NULL;
    char * auth_string;

    /* calculate the date */
    t = time(NULL);
    if (!localtime_r(&t, &tmp)) perror("localtime");
    if (!strftime(date, sizeof(date), "%a, %d %b %Y %H:%M:%S %Z", &tmp)) 
        perror("strftime");

    /* run HMAC-SHA1 on the canonicalized string */
    HMAC_CTX_init(&ctx);
    HMAC_Init_ex(&ctx, hdl->secret_key, strlen(hdl->secret_key), EVP_sha1(), NULL);
    auth_string = g_strconcat(verb, "\n\n\n", date, "\n",
#ifdef WANT_DEVPAY
                              AMAZON_SECURITY_HEADER, ":",
                              hdl->user_token, ",",
                              STS_PRODUCT_TOKEN, "\n",
#endif
                              resource, NULL);
    HMAC_Update(&ctx, (unsigned char*) auth_string, strlen(auth_string));
    g_free(auth_string);
    md_len = EVP_MAX_MD_SIZE;
    HMAC_Final(&ctx, (unsigned char*)md_value, &md_len);
    HMAC_CTX_cleanup(&ctx);
    base64_encode(md_value, md_len, auth_base64, sizeof(auth_base64));

    /* append the new headers */
#ifdef WANT_DEVPAY
    /* Devpay headers are included in hash. */
    buf = g_strdup_printf(AMAZON_SECURITY_HEADER ": %s", hdl->user_token);
    headers = curl_slist_append(headers, buf);
    amfree(buf);

    buf = g_strdup_printf(AMAZON_SECURITY_HEADER ": %s", STS_PRODUCT_TOKEN);
    headers = curl_slist_append(headers, buf);
    amfree(buf);
#endif

    buf = g_strdup_printf("Authorization: AWS %s:%s",
                          hdl->access_key, auth_base64);
    headers = curl_slist_append(headers, buf);
    amfree(buf);
    
    buf = g_strdup_printf("Date: %s", date);
    headers = curl_slist_append(headers, buf);
    amfree(buf);

    return headers;
}
/* }}} */

/* {{{ interpret_response */
static void
regex_error(regex_t *regex, int reg_result)
{
    char *message;
    int size;

    size = regerror(reg_result, regex, NULL, 0);
    message = g_malloc(size);
    if (!message) abort(); /* we're really out of luck */
    regerror(reg_result, regex, message, size);

    /* this is programmer error (bad regexp), so just log
     * and abort().  There's no good way to signal a
     * permanaent error from interpret_response. */
    g_error(_("Regex error: %s"), message);
    g_assert_not_reached();
}

static gboolean
interpret_response(S3Handle *hdl,
                   CURLcode curl_code,
                   char *curl_error_buffer,
                   void *body,
                   guint body_len)
{
    long response_code = 0;
    regmatch_t pmatch[2];
    int reg_result;
    char *error_name = NULL, *message = NULL;
    char *body_copy = NULL;

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

    /* 2xx and 3xx codes won't have a response body*/
    if (200 <= response_code && response_code < 400) {
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

    reg_result = regexec(&error_name_regex, body_copy, 2, pmatch, 0);
    if (reg_result != 0) {
        if (reg_result == REG_NOMATCH) {
            error_name = NULL;
        } else {
            regex_error(&error_name_regex, reg_result);
            g_assert_not_reached();
        }
    } else {
        error_name = find_regex_substring(body_copy, pmatch[1]);
    }

    reg_result = regexec(&message_regex, body_copy, 2, pmatch, 0);
    if (reg_result != 0) {
        if (reg_result == REG_NOMATCH) {
            message = NULL;
        } else {
            regex_error(&message_regex, reg_result);
            g_assert_not_reached();
        }
    } else {
        message = find_regex_substring(body_copy, pmatch[1]);
    }

    if (error_name) {
        hdl->last_s3_error_code = s3_error_code_from_name(error_name);
    }

    if (message) {
        hdl->last_message = message;
        message = NULL; /* steal the reference to the string */
    }

cleanup:
    if (body_copy) g_free(body_copy);
    if (message) g_free(message);
    if (error_name) g_free(error_name);

    return FALSE;
}
/* }}} */

/* {{{ perform_request */
size_t buffer_readfunction(void *ptr, size_t size,
                           size_t nmemb, void * stream) {
    CurlBuffer *data = stream;
    guint bytes_desired = size * nmemb;

    /* check the number of bytes remaining, just to be safe */
    if (bytes_desired > data->buffer_len - data->buffer_pos)
        bytes_desired = data->buffer_len - data->buffer_pos;

    memcpy((char *)ptr, data->buffer + data->buffer_pos, bytes_desired);
    data->buffer_pos += bytes_desired;

    return bytes_desired;
}

size_t
buffer_writefunction(void *ptr, size_t size, size_t nmemb, void *stream)
{
    CurlBuffer * data = stream;
    guint new_bytes = size * nmemb;
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
    g_return_val_if_fail(data->buffer, 0); /* returning zero signals an error to libcurl */

    /* actually copy the data to the buffer */
    memcpy(data->buffer + data->buffer_pos, ptr, new_bytes);
    data->buffer_pos += new_bytes;

    /* signal success to curl */
    return new_bytes;
}

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
    message = g_strndup(s, len);
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
                const char *resource,
                const char *uri,
                const char *verb,
                const void *request_body,
                guint request_body_size,
                guint max_response_size,
                guint preallocate_response_size,
                const result_handling_t *result_handling)
{
    char *url = NULL;
    s3_result_t result = S3_RESULT_FAIL; /* assume the worst.. */
    CURLcode curl_code = CURLE_OK;
    char curl_error_buffer[CURL_ERROR_SIZE] = "";
    struct curl_slist *headers = NULL;
    CurlBuffer readdata = { (void*)request_body, request_body_size, 0, 0 };
    CurlBuffer writedata = { NULL, 0, 0, max_response_size };
    gboolean should_retry;
    guint retries = 0;
    gulong backoff = EXPONENTIAL_BACKOFF_START_USEC;

    g_return_val_if_fail(hdl != NULL && hdl->curl != NULL, S3_RESULT_FAIL);

    s3_reset(hdl);

    url = g_strconcat(S3_URL, uri, NULL);
    if (!url) goto cleanup;

    if (preallocate_response_size) {
        writedata.buffer = g_malloc(preallocate_response_size);
        if (!writedata.buffer) goto cleanup;
        writedata.buffer_len = preallocate_response_size;
    }

    while (1) {
        /* reset things */
        if (headers) {
            curl_slist_free_all(headers);
        }
        readdata.buffer_pos = 0;
        writedata.buffer_pos = 0;
	curl_error_buffer[0] = '\0';

        /* set up the request */
        headers = authenticate_request(hdl, verb, resource);

        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_VERBOSE, hdl->verbose)))
            goto curl_error;
	if (hdl->verbose)
	    if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_DEBUGFUNCTION, 
					      curl_debug_message)))
		goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_ERRORBUFFER,
                                          curl_error_buffer)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_NOPROGRESS, 1)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_URL, url)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_HTTPHEADER,
                                          headers)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_CUSTOMREQUEST,
                                          verb)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_WRITEFUNCTION, buffer_writefunction))) 
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_WRITEDATA, &writedata))) 
            goto curl_error;
        if (max_response_size) {
#ifdef CURLOPT_MAXFILESIZE_LARGE
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_MAXFILESIZE_LARGE, (curl_off_t)max_response_size))) 
                goto curl_error;
#else
# ifdef CURLOPT_MAXFILESIZE
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_MAXFILESIZE, (long)max_response_size))) 
                goto curl_error;
# else
	    /* no MAXFILESIZE option -- that's OK */
# endif
#endif
	}

        if (request_body) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_UPLOAD, 1))) 
                goto curl_error;
#ifdef CURLOPT_INFILESIZE_LARGE
	    if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)request_body_size))) 
                goto curl_error;
#else
	    if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_INFILESIZE, (long)request_body_size))) 
                goto curl_error;
#endif
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READFUNCTION, buffer_readfunction))) 
                goto curl_error;
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READDATA, &readdata))) 
                goto curl_error;
        } else {
            /* Clear request_body options. */
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_UPLOAD, 0))) 
                goto curl_error;
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
                            writedata.buffer, writedata.buffer_pos);
        
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
    if (url) g_free(url);
    if (headers) curl_slist_free_all(headers);
    
    /* we don't deallocate the response body -- we keep it for later */
    hdl->last_response_body = writedata.buffer;
    hdl->last_response_body_size = writedata.buffer_pos;
    hdl->last_num_retries = retries;

    return result;
}
/* }}} */

/*
 * Public function implementations
 */

/* {{{ s3_init */
gboolean
s3_init(void)
{
    char regmessage[1024];
    int size;
    int reg_result;

    reg_result = regcomp(&error_name_regex, error_name_regex_string, REG_EXTENDED | REG_ICASE);
    if (reg_result != 0) {
        size = regerror(reg_result, &error_name_regex, regmessage, sizeof(regmessage));
        g_error(_("Regex error: %s"), regmessage);
        return FALSE;
    }

    reg_result = regcomp(&message_regex, message_regex_string, REG_EXTENDED | REG_ICASE);
    if (reg_result != 0) {
        size = regerror(reg_result, &message_regex, regmessage, sizeof(regmessage));
        g_error(_("Regex error: %s"), regmessage);
        return FALSE;
    }

    return TRUE;
}
/* }}} */

/* {{{ s3_open */
S3Handle *
s3_open(const char *access_key,
        const char *secret_key
#ifdef WANT_DEVPAY
        ,
        const char *user_token
#endif
        ) {
    S3Handle *hdl;

    hdl = g_new0(S3Handle, 1);
    if (!hdl) goto error;

    hdl->verbose = FALSE;

    hdl->access_key = g_strdup(access_key);
    if (!hdl->access_key) goto error;

    hdl->secret_key = g_strdup(secret_key);
    if (!hdl->secret_key) goto error;

#ifdef WANT_DEVPAY
    hdl->user_token = g_strdup(user_token);
    if (!hdl->user_token) goto error;
#endif

    hdl->curl = curl_easy_init();
    if (!hdl->curl) goto error;

    return hdl;

error:
    s3_free(hdl);
    return NULL;
}
/* }}} */

/* {{{ s3_free */
void
s3_free(S3Handle *hdl)
{
    s3_reset(hdl);

    if (hdl) {
        if (hdl->access_key) g_free(hdl->access_key);
        if (hdl->secret_key) g_free(hdl->secret_key);
#ifdef WANT_DEVPAY
        if (hdl->user_token) g_free(hdl->user_token);
#endif
        if (hdl->curl) curl_easy_cleanup(hdl->curl);

        g_free(hdl);
    }
}
/* }}} */

/* {{{ s3_reset */
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
/* }}} */

/* {{{ s3_error */
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
/* }}} */

/* {{{ s3_verbose */
void
s3_verbose(S3Handle *hdl, gboolean verbose)
{
    hdl->verbose = verbose;
}
/* }}} */

/* {{{ s3_sterror */
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
        message = "Unkonwn S3 error";
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
/* }}} */

/* {{{ s3_upload */
/* Perform an upload. When this function returns, KEY and
 * BUFFER remain the responsibility of the caller.
 *
 * @param self: the s3 device
 * @param key: the key to which the upload should be made
 * @param buffer: the data to be uploaded
 * @param buffer_len: the length of the data to upload
 * @returns: false if an error ocurred
 */
gboolean
s3_upload(S3Handle *hdl,
          const char *bucket,
          const char *key, 
          gpointer buffer,
          guint buffer_len)
{
    char *resource = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,          0,                   S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,    0,                /* default: */ S3_RESULT_FAIL }
        };

    g_return_val_if_fail(hdl != NULL, FALSE);

    resource = build_resource(bucket, key);
    if (resource) {
        result = perform_request(hdl, resource, resource, "PUT",
                                 buffer, buffer_len, MAX_ERROR_RESPONSE_LEN, 0,
                                 result_handling);
        g_free(resource);
    }

    return result == S3_RESULT_OK;
}
/* }}} */

/* {{{ s3_list_keys */

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
    if (strcasecmp(element_name, "contents") == 0) {
        thunk->in_contents = 1;
    } else if (strcasecmp(element_name, "commonprefixes") == 0) {
        thunk->in_common_prefixes = 1;
    } else if (strcasecmp(element_name, "prefix") == 0 && thunk->in_common_prefixes) {
        thunk->want_text = 1;
    } else if (strcasecmp(element_name, "key") == 0 && thunk->in_contents) {
        thunk->want_text = 1;
    } else if (strcasecmp(element_name, "istruncated")) {
        thunk->want_text = 1;
    } else if (strcasecmp(element_name, "nextmarker")) {
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

    if (strcasecmp(element_name, "contents") == 0) {
        thunk->in_contents = 0;
    } else if (strcasecmp(element_name, "commonprefixes") == 0) {
        thunk->in_common_prefixes = 0;
    } else if (strcasecmp(element_name, "key") == 0 && thunk->in_contents) {
        thunk->filename_list = g_slist_prepend(thunk->filename_list, thunk->text);
        thunk->text = NULL;
    } else if (strcasecmp(element_name, "prefix") == 0 && thunk->in_common_prefixes) {
        thunk->filename_list = g_slist_prepend(thunk->filename_list, thunk->text);
        thunk->text = NULL;
    } else if (strcasecmp(element_name, "istruncated") == 0) {
        if (thunk->text && strncasecmp(thunk->text, "false", 5) != 0)
            thunk->is_truncated = TRUE;
    } else if (strcasecmp(element_name, "nextmarker") == 0) {
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

/* Helper function for list_fetch */
static gboolean
list_build_url_component(char **rv,
                         const char *delim,
                         const char *key,
                         const char *value)
{
    char *esc_value = NULL;
    char *new_rv = NULL;

    esc_value = curl_escape(value, 0);
    if (!esc_value) goto cleanup;

    new_rv = g_strconcat(*rv, delim, key, "=", esc_value, NULL);
    if (!new_rv) goto cleanup;

    g_free(*rv);
    *rv = new_rv;
    curl_free(esc_value);

    return TRUE;

cleanup:
    if (new_rv) g_free(new_rv);
    if (esc_value) curl_free(esc_value);

    return FALSE;
}

/* Perform a fetch from S3; several fetches may be involved in a
 * single listing operation */
static s3_result_t
list_fetch(S3Handle *hdl,
           const char *resource,
           const char *prefix, 
           const char *delimiter, 
           const char *marker,
           const char *max_keys)
{
    char *urldelim = "?";
    char *uri = g_strdup(resource);
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,          0,                   S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,    0,                /* default: */ S3_RESULT_FAIL  }
        };

    /* build the URI */
    if (prefix) {
        if (!list_build_url_component(&uri, urldelim, "prefix", prefix)) goto cleanup;
        urldelim = "&";
    }
    if (delimiter) {
        if (!list_build_url_component(&uri, urldelim, "delimiter", delimiter)) goto cleanup;
        urldelim = "&";
    }
    if (marker) {
        if (!list_build_url_component(&uri, urldelim, "marker", marker)) goto cleanup;
        urldelim = "&";
    }
    if (max_keys) {
        if (!list_build_url_component(&uri, urldelim, "max-keys", max_keys)) goto cleanup;
        urldelim = "&";
    }

    /* and perform the request on that URI */
    result = perform_request(hdl, resource, uri, "GET", NULL,
                             0, MAX_ERROR_RESPONSE_LEN, 0, result_handling);

cleanup:
    if (uri) g_free(uri);
    return result;
}

gboolean
s3_list_keys(S3Handle *hdl,
              const char *bucket,
              const char *prefix,
              const char *delimiter,
              GSList **list)
{
    char *resource = NULL;
    struct list_keys_thunk thunk;
    GMarkupParseContext *ctxt = NULL;
    static GMarkupParser parser = { list_start_element, list_end_element, list_text, NULL, NULL };
    GError *err = NULL;
    s3_result_t result = S3_RESULT_FAIL;

    g_assert(list);
    *list = NULL;
    thunk.filename_list = NULL;
    thunk.text = NULL;
    thunk.next_marker = NULL;

    resource = build_resource(bucket, NULL);
    if (!resource) goto cleanup;

    /* Loop until S3 has given us the entire picture */
    do {
        /* get some data from S3 */
        result = list_fetch(hdl, resource, prefix, delimiter, thunk.next_marker, NULL);
        if (result != S3_RESULT_OK) goto cleanup;

        /* run the parser over it */
        thunk.in_contents = FALSE;
        thunk.in_common_prefixes = FALSE;
        thunk.is_truncated = FALSE;
        thunk.want_text = FALSE;

        ctxt = g_markup_parse_context_new(&parser, 0, (gpointer)&thunk, NULL);

        if (!g_markup_parse_context_parse(ctxt, hdl->last_response_body, 
                                          hdl->last_response_body_size, &err)) {
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
    if (resource) g_free(resource);
    if (ctxt) g_markup_parse_context_free(ctxt);

    if (result != S3_RESULT_OK) {
        g_slist_free(thunk.filename_list);
        return FALSE;
    } else {
        *list = thunk.filename_list;
        return TRUE;
    }
}
/* }}} */

/* {{{ s3_read */
gboolean
s3_read(S3Handle *hdl,
        const char *bucket,
        const char *key,
        gpointer *buf_ptr,
        guint *buf_size,
        guint max_size)
{
    char *resource = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,          0,                   S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,    0,                /* default: */ S3_RESULT_FAIL  }
        };

    g_return_val_if_fail(hdl != NULL, FALSE);
    g_assert(buf_ptr != NULL);
    g_assert(buf_size != NULL);

    *buf_ptr = NULL;
    *buf_size = 0;

    resource = build_resource(bucket, key);
    if (resource) {
        result = perform_request(hdl, resource, resource,
                                 "GET", NULL, 0, max_size, 0, result_handling);
        g_free(resource);

        /* copy the pointer to the result parameters and remove
         * our reference to it */
        if (result == S3_RESULT_OK) {
            *buf_ptr = hdl->last_response_body;
            *buf_size = hdl->last_response_body_size;
            
            hdl->last_response_body = NULL;
            hdl->last_response_body_size = 0;
        }
    }        

    return result == S3_RESULT_OK;
}
/* }}} */

/* {{{ s3_delete */
gboolean
s3_delete(S3Handle *hdl,
          const char *bucket,
          const char *key)
{
    char *resource = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 204,  0,          0,                   S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,    0,                /* default: */ S3_RESULT_FAIL  }
        };

    g_return_val_if_fail(hdl != NULL, FALSE);

    resource = build_resource(bucket, key);
    if (resource) {
        result = perform_request(hdl, resource, resource, "DELETE", NULL, 0,
                                 MAX_ERROR_RESPONSE_LEN, 0, result_handling);
        g_free(resource);
    }

    return result == S3_RESULT_OK;
}
/* }}} */

/* {{{ s3_make_bucket */
gboolean
s3_make_bucket(S3Handle *hdl,
               const char *bucket)
{
    char *resource = NULL;
    s3_result_t result = result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,          0,                   S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,    0,                /* default: */ S3_RESULT_FAIL  }
        };

    g_return_val_if_fail(hdl != NULL, FALSE);

    resource = build_resource(bucket, NULL);
    if (resource) {
        result = perform_request(hdl, resource, resource, "PUT", NULL, 0, 
                                 MAX_ERROR_RESPONSE_LEN, 0, result_handling);
        g_free(resource);
    }

    return result == S3_RESULT_OK;
}
/* }}} */
