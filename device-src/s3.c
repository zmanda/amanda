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
 * Contact information: BETSOL - 10901 W 120th Ave #235,
 * Broomfield, CO 80021 or: http://www.zmanda.com
 */

/* TODO
 * - collect speed statistics
 * - debugging mode
 */
#ifdef HAVE_CONFIG_H
#include "../config/config.h"
/* use a relative path here to avoid conflicting with Perl's config.h. */
#endif

#include "s3.h"
#include "s3-device.h"
#include "s3-util.h"

#ifdef HAVE_UTIL_H
#include "amutil.h"
#endif
#ifdef HAVE_AMANDA_H
#include "amanda.h"
#include "sockaddr-util.h"
#endif
#include "amjson.h"

#include <string.h>

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
#include <curl/curl.h>

#ifdef HAVE_STDINT_H
#include <stdint.h>
#endif

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
#include <glib.h>

char *S3_name[] = {
   "UNKNOWN",
   "S3",
   "OpenStack",
   "OpenStack",
   "OpenStack",
   "Google Storage",
   "Castor",
   "S3",
};

char *S3_bucket_name[] = {
   "UNKNOWN",
   "S3 bucket",
   "OpenStack bucket",
   "OpenStack bucket",
   "OpenStack bucket",
   "Google Storage bucket",
   "Castor bucket",
   "S3 bucket",
};

/* Maximum key length as specified in the S3 documentation
 * (*excluding* null terminator) */
#define S3_MAX_KEY_LENGTH 1024

#define AMAZON_COPY_SOURCE_HEADER "x-amz-copy-source"

#define AMAZON_SECURITY_HEADER "x-amz-security-token"

#define AMAZON_STORAGE_CLASS_HEADER "x-amz-storage-class"

#define AMAZON_SERVER_SIDE_ENCRYPTION_HEADER "x-amz-server-side-encryption"

#define AMAZON_WILDCARD_LOCATION "*"

/* parameters for exponential backoff in the face of retriable errors */

/* start at 0.01s */
#define BACKOFF_DELAY_START_USEC ((guint64)G_USEC_PER_SEC)/100
/* retry 14 times (~164 seconds for final timeout, ~328 total wait) */
#define BACKOFF_MAX_RETRIES 14
#define MAX_BACKOFF_DELAY_USEC  (BACKOFF_DELAY_START_USEC << BACKOFF_MAX_RETRIES)


/* general "reasonable size" parameters */
#define MAX_ERROR_RESPONSE_LEN (100*1024)

// CURLE_SSL_CACERT_BADFILE is defined in 7.16.0
#if LIBCURL_VERSION_NUM >= 0x071000
#define AMAMDA_CURLE_SSL_CACERT_BADFILE CURLE_SSL_CACERT_BADFILE
#else
# define AMAMDA_CURLE_SSL_CACERT_BADFILE CURLE_GOT_NOTHING
#endif
/* Results which should always be retried */
#define RESULT_HANDLING_ALWAYS_RETRY \
        { 400,  S3_ERROR_RequestTimeout,     0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 403,  S3_ERROR_RequestTimeTooSkewed,0,                         S3_RESULT_RETRY_BACKOFF }, \
        { 409,  S3_ERROR_OperationAborted,   0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 412,  S3_ERROR_PreconditionFailed, 0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 429,  0,                           0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 500,  S3_ERROR_None,               0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 500,  S3_ERROR_InternalError,      0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 501,  S3_ERROR_NotImplemented,     0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 503,  S3_ERROR_ServiceUnavailable, 0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 503,  S3_ERROR_SlowDown,           0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 503,  0,                           0,                          S3_RESULT_RETRY_BACKOFF }, \
        { 401,  S3_ERROR_AuthenticationRequired, 0,                      S3_RESULT_RETRY_AUTH }, \
        { 0,    0,                           CURLE_COULDNT_CONNECT,      S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_COULDNT_RESOLVE_HOST, S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_PARTIAL_FILE,         S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_OPERATION_TIMEOUTED,  S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_SSL_CONNECT_ERROR,    S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_SEND_ERROR,           S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_RECV_ERROR,           S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_ABORTED_BY_CALLBACK,  S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_GOT_NOTHING,          S3_RESULT_RETRY }

//
// no amount of retries will help this
//        { 0,    0,                           AMAMDA_CURLE_SSL_CACERT_BADFILE,   S3_RESULT_RETRY }

/*
 * Data structures and associated functions
 */

struct S3Handle {
    /* (all strings in this struct are freed by s3_free()) */

    char *access_key;
    char *secret_key;
    char *session_token;
    char *user_token;
    char *swift_account_id;
    char *swift_access_key;
    char *username;
    char *password;
    char *tenant_id;
    char *tenant_name;
    char *project_name;
    char *domain_name;
    char *client_id;
    char *client_secret;
    char *refresh_token;
    char *access_token;
    time_t expires; 		// set in record_response
    gboolean getting_oauth2_access_token;
    gboolean getting_swift_2_token;
    gboolean getting_swift_3_token;

    /* attributes for new objects */
    char *bucket_location;
    char *storage_class;
    char *server_side_encryption;
    char *proxy;
    char *host;
    char *host_without_port;
    char *service_path;
    gboolean use_subdomain;
    gboolean use_google_subresources;
    S3_api s3_api;
    char *ca_info;
    char *x_auth_token;		// NOT cleared on s3_reset
    char *x_storage_url;	// NOT cleared on s3_reset

    char *x_amz_expiration;	// cleared on s3_reset
    char *x_amz_restore;	// cleared on s3_reset

    CURL *curl;
    GMutex curl_mutex;

    gboolean verbose;
    gboolean use_ssl;
    gboolean server_side_encryption_header;

    objbytes_t max_send_speed;
    objbytes_t max_recv_speed;

    /* information from the last request */
    char *last_message;			// cleared on s3_reset
    guint last_response_code;		// cleared on s3_reset
    s3_error_code_t last_s3_error_code;	// cleared on s3_reset
    CURLcode last_curl_code;		// cleared on s3_reset
    guint last_num_retries;		// cleared on s3_reset
    void *last_response_body;		// cleared on s3_reset
    guint last_response_body_size;	// cleared on s3_reset
    char *last_uploadId;                // cleared on s3_reset
    char *last_etag;			// cleared on s3_reset

    /* offset with s3 */
    time_t time_offset_with_s3;
    char *content_type;
    objbytes_t object_bytes;

    gboolean reuse_connection;
    char *transfer_encoding;				// cleared on s3_reset
    gint64     timeout;

    /* CAStor */
    char *reps;
    char *reps_bucket;
};

typedef enum S3InternalContentType {
    CONTENT_UNKN_ENCODED=-4,
    CONTENT_UNKN_CHUNKED=-3,
    CONTENT_UNKN_CONTENT=-2,
    CONTENT_UNKN_XMLBODY=-1,
    CONTENT_UNKN=-1,
    CONTENT_BINARY=0,
    CONTENT_TEXT=1,
    CONTENT_JSON=2,
    CONTENT_XML=3
} eS3InternalContentType;

typedef struct {
    CurlBuffer dup_buffer;
    s3_write_func write_func;
    s3_reset_func reset_func;
    gpointer write_data;

    /* Points to current handle: Added to get hold of s3 offset */
    struct S3Handle *hdl;
    eS3InternalContentType bodytype;
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

#if LIBCURL_VERSION_NUM >= 0x071101 && defined(TCP_CONGESTION_ALG)
static curl_socket_t 
s3_thread_linux_opensocket_func(void *clientp,
				curlsocktype purpose,
				struct curl_sockaddr *address);
#endif


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
typedef struct result_handling {
    guint           response_code;
    s3_error_code_t s3_error_code;
    CURLcode        curl_code;
    s3_result_t     result;
} result_handling_t;

/*
 * get the access token for OAUTH2
 */
static s3_result_t
oauth2_get_access_token(S3Handle *hdl);

static s3_result_t
s3_refresh_token(S3Handle *hdl);

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
static regex_t etag_regex, error_name_regex, message_regex, 
    date_sync_regex, 
    access_token_regex,
    content_type_regex, content_range_regex, 
    transfer_encoding_regex,
    details_regex, code_regex, 
    uploadId_regex,
    json_message_regex, html_error_name_regex, html_message_regex,
    bucket_regex, bucket_regex_google, bucket_reject_regex,
    location_con_regex, 
    x_auth_token_regex,
    x_subject_token_regex,
    x_storage_url_regex, 
    expires_in_regex,
    x_amz_expiration_regex, x_amz_restore_regex;


/*
 * Utility functions
 */

/* Check if a string is non-empty
 *
 * @param str: string to check
 * @returns: true iff str is non-NULL and not "\0"
 */
static gboolean is_non_empty_string(const char *str);
char *am_strrmspace(char *str);

/* Construct the URL for an Amazon S3 REST request.
 *
 * A new string is allocated and returned; it is the responsiblity of the caller.
 *
 * @param hdl: the S3Handle object
 * @param service_path: A path to add in the URL, or NULL for none.
 * @param bucket: the bucket being accessed, or NULL for none
 * @param key: the key being accessed, or NULL for none
 * @param subresource: the sub-resource being accessed (e.g. "acl"), or NULL for none
 * @param query: the query being accessed (e.g. "acl"), or NULL for none
 *
 * !use_subdomain: http://host/service_path/bucket/key
 * use_subdomain : http://bucket.host/service_path/key
 *
 */
static char *
build_url(
      S3Handle *hdl,
      const char *bucket,
      const char *key,
      const char *subresource,
      const char **query);

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
 * @param query: the query being used
 * @param md5_hash: the MD5 hash of the request body, or NULL for none
 */
static struct curl_slist *
authenticate_request(S3Handle *hdl,
                     const char *verb,
                     const char *bucket,
                     const char *key,
                     const char *subresource,
                     const char **query,
                     const char *md5_hash,
                     const char *data_SHA256Hash,
                     const char *content_type,
                     const char *project_id,
                     const char *copy_source);


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
 * @returns: S3_RESULT_RETRY or S3_RESULT_RETRY_BACKOFF if the response should be retried (e.g., network error)
 */
static s3_result_t
record_response(S3Handle *hdl,
                   CURLcode curl_code,
                   char *curl_error_buffer,
                   eS3InternalContentType bodytype,
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
 * @param query: NULL terminated array of query string, the must be in alphabetid order
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
                const char **query,
                const char *content_type,
                const char *project_id,
		struct curl_slist *user_headers,
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
                const result_handling_t *result_handling,
		gboolean chunked);

#if LIBCURL_VERSION_NUM >= 0x071101 && defined(TCP_CONGESTION_ALG)
static curl_socket_t 
s3_thread_linux_opensocket_func(void *data G_GNUC_UNUSED,
				curlsocktype purpose G_GNUC_UNUSED,
				struct curl_sockaddr *sa)
{
  curl_socket_t sockfd = socket(sa->family, sa->socktype, sa->protocol);
  if (sockfd < 0) return sockfd;
 
  // attempt to optimize for long-internet connections (if available and permitted)
  (void) setsockopt(sockfd, IPPROTO_TCP, TCP_CONGESTION, TCP_CONGESTION_ALG, sizeof(TCP_CONGESTION_ALG));
  return sockfd;
}
#endif

static void
http_response_reset(S3Handle *hdl);

/*
 * a CURLOPT_WRITEFUNCTION to save part of the response in memory and
 * call an external function if one was provided.
 */
static size_t
s3_internal_write_func(void *ptr, size_t size, size_t nmemb, void * stream);

/*
 * a function to clear/start our internal buffer
 */
static void
s3_internal_write_init(void * stream, S3Handle *hdl,
                 s3_write_func write_func, s3_reset_func reset_func,
                 gpointer write_hdnl);
/*
 * a function to reset to our internal buffer
 */
static void
s3_internal_write_reset_func(void * stream);

/*
 * a CURLOPT_HEADERFUNCTION to save the ETag header only.
 */
static size_t
s3_internal_header_func(void *ptr, size_t size, size_t nmemb, void * stream);

static void s3_new_curl(S3Handle *hdl);

static s3_result_t get_openstack_swift_api_v1_setting(S3Handle *hdl);
static s3_result_t get_openstack_swift_api_v2_setting(S3Handle *hdl);
static s3_result_t get_openstack_swift_api_v3_setting(S3Handle *hdl);

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
        if (g_ascii_strcasecmp(s3_error_name, s3_error_code_names[i]) == 0)
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

static gboolean
s3_curl_throttling_compat(void)
{
/* CURLOPT_MAX_SEND_SPEED_LARGE added in 7.15.5 */
#if LIBCURL_VERSION_NUM >= 0x070f05
    curl_version_info_data *info;

    /* check the runtime version too */
    info = curl_version_info(CURLVERSION_NOW);
    return info->version_num >= 0x070f05;
#else
    return FALSE;
#endif
}

static int
subresource_sort_cmp(const void *a, const void *b)
{
    const char *const *astr = a;
    const char *const *bstr = b;
    return g_ascii_strcasecmp(*astr,*bstr); // deref and compare strings
}

static s3_result_t
lookup_result(const result_handling_t *arr,
              guint response_code,
              s3_error_code_t s3_error_code,
              CURLcode curl_code)
{
    for(;; arr++) {
        if (arr->response_code && arr->response_code != response_code)
            continue;
        if (arr->s3_error_code && arr->s3_error_code != s3_error_code)
            continue;
        if (arr->curl_code && arr->curl_code != curl_code)
            continue;

        // match found... (or default)
        break;
    }

    return arr->result;

}

static time_t
rfc3339_date(
    const char *date)
{
    gint year, month, day, hour, minute, seconds;
    const char *atz;

    if (strlen(date) < 19)
        return 0x40000000; // Sat Jan 10 13:37:04 GMT 2004

    year = atoi(date);
    month = atoi(date+5);
    day = atoi(date+8);
    hour = atoi(date+11);
    minute = atoi(date+14);
    seconds = atoi(date+17);
    atz = date+19;
    if (*atz == '.') {   /* skip decimal seconds */
	atz++;
	while (*atz >= '0' && *atz <= '9') {
	    atz++;
	}
    }

#if GLIB_CHECK_VERSION(2,26,0)
    if (!glib_check_version(2,26,0)) {
	GTimeZone *tz;
	GDateTime *dt;
	time_t a;

	tz = g_time_zone_new(atz);
	dt = g_date_time_new(tz, year, month, day, hour, minute, seconds);
	a = g_date_time_to_unix(dt);
	g_time_zone_unref(tz);
	g_date_time_unref(dt);
	return a;
    } else
#endif
    {
	struct tm tm;
	time_t t;

	tm.tm_year = year - 1900;
	tm.tm_mon = month - 1;
	tm.tm_mday = day;
	tm.tm_hour = hour;
	tm.tm_min = minute;
	tm.tm_sec = seconds;
	tm.tm_wday = 0;
	tm.tm_yday = 0;
	tm.tm_isdst = -1;
	t = time(NULL);

	if (*atz == '-' || *atz == '+') {  /* numeric timezone */
	    time_t lt, gt;
	    time_t a;
	    struct tm ltt, gtt;
	    gint Hour = atoi(atz);
	    gint Min  = atoi(atz+4);

	    if (Hour < 0)
		Min = -Min;
	    tm.tm_hour -= Hour;
	    tm.tm_min -= Min;
	    tm.tm_isdst = 0;
	    localtime_r(&t, &ltt);
	    lt = mktime(&ltt);
	    gmtime_r(&t, &gtt);
	    gt = mktime(&gtt);
	    tm.tm_sec += lt - gt;
	    a = mktime(&tm);
	    return a;
	} else if (*atz == 'Z' && *(atz+1) == '\0') { /* Z timezone */
	    time_t lt, gt;
	    time_t a;
	    struct tm ltt, gtt;

	    tm.tm_isdst = 0;
	    localtime_r(&t, &ltt);
	    lt = mktime(&ltt);
	    gmtime_r(&t, &gtt);
	    gt = mktime(&gtt);
	    tm.tm_sec += lt - gt;
	    a = mktime(&tm);
	    return a;
	} else { /* named timezone */
	    int pid;
	    int fd[2];
	    char buf[101];
	    time_t a;
	    ssize_t size;

	    if (pipe(fd) == -1)
		return 0x40000000;
	    pid = fork();
	    switch (pid) {
		case -1:
		    close(fd[0]);
		    close(fd[1]);
		    return 0x40000000;
		case 0:
		    close(fd[0]);
		    setenv("TZ", atz, 1);
		    tzset();
		    a = mktime(&tm);
		    g_snprintf(buf, 100, "%d", (int)a);
		    full_write(fd[1], buf, strlen(buf));
		    close(fd[1]);
		    exit(0);
		default:
		    close(fd[1]);
		    size = full_read(fd[0], buf, 100);
		    if (size < 0) size = 0;
		    close(fd[0]);
		    buf[size] = '\0';
		    waitpid(pid, NULL, 0);
		    break;
	    }
	    return atoi(buf);
	}
    }
}


static gboolean
is_non_empty_string(const char *str)
{
    return str && str[0] != '\0';
}

static char *
build_url(
      S3Handle   *hdl,
      const char *bucket,
      const char *key,
      const char *subresource,
      const char **query)
{
    GString *url = NULL;
    char *esc_bucket = NULL, *esc_key = NULL;

    if (hdl->s3_api >= S3_API_SWIFT_1 && hdl->s3_api <= S3_API_OAUTH2 && hdl->x_storage_url) {
	url = g_string_new(hdl->x_storage_url);
	g_string_append_c(url, '/');
    } else {
	/* scheme */
	url = g_string_new("http");
	if (hdl->use_ssl)
            g_string_append_c(url, 's');

	g_string_append(url, "://");

	/* domain */
	if (hdl->use_subdomain && bucket)
            g_string_append_printf(url, "%s.%s", bucket, hdl->host);
	else
            g_string_append_printf(url, "%s", hdl->host);

	if (hdl->service_path) {
            g_string_append_printf(url, "%s/", hdl->service_path);
	} else {
	    g_string_append_c(url, '/');
	}
    }

    /* path */
    if (!hdl->use_subdomain && bucket) {
	esc_bucket = g_uri_escape_string(bucket, NULL, 0);
        if (!esc_bucket) goto cleanup;
        g_string_append_printf(url, "%s", esc_bucket);
        if (key)
            g_string_append_c(url, '/');
	curl_free(esc_bucket);
    }

    if (key) {
	esc_key = g_uri_escape_string(key, NULL, 0);
        if (!esc_key) goto cleanup;
        g_string_append_printf(url, "%s", esc_key);
	curl_free(esc_key);
    }

    if (url->str[strlen(url->str)-1] == '/') {
	g_string_truncate(url, strlen(url->str)-1);
    }

    /* query string */
    if (subresource || query || (hdl->s3_api == S3_API_CASTOR && hdl->tenant_name))
        g_string_append_c(url, '?');

    if (subresource)
        g_string_append(url, subresource);

    if (query && query[0]) 
    {
	const char **q = query;

	if (subresource)
	    g_string_append_c(url, '&');
	g_string_append(url, *q++);

	for (; *q ; q++) {
	    g_string_append_c(url, '&');
            g_string_append(url, *q);
	}
    }

    /* add CAStor tenant domain override query arg */
    if (hdl->s3_api == S3_API_CASTOR && hdl->tenant_name) {
        if (subresource || query)
            g_string_append_c(url, '&');
        g_string_append_printf(url, "domain=%s", hdl->tenant_name);
    }

cleanup:

    return g_string_free(url, FALSE);
}

static struct curl_slist *
authenticate_request(S3Handle *hdl,
                     const char *verb,
                     const char *bucket,
                     const char *key,
                     const char *subresource,
                     const char **query,
                     const char *md5_hash,
                     const char *data_SHA256Hash,
                     const char *content_type,
                     const char *project_id,
                     const char *copy_source)
{
    // parameters that must be moved to the subresource string for AWS v2, but not for Google
    static const char *const amz_subrsrc_params[] = {
	"acl", "lifecycle", "location", "logging",
	"notification", "partNumber", "policy",
	"requestPayment", "uploadId", "uploads",
	"versionId", "versioning", "versions", "website",
	NULL
    };

    time_t t;
    struct tm tmp;
    char *date = NULL;
    char *szS3Date = NULL;
    char *zulu_date = NULL;
    char *buf = NULL;
    GByteArray *md = NULL;
    char *auth_base64 = NULL;
    struct curl_slist *headers = NULL;
    char *esc_bucket = NULL, *esc_key = NULL;
    GString *auth_string = NULL;
    char *reps = NULL;
    const char *subresource_uploadId = NULL;
    const char *query_uploadId = NULL;
    const char **q = NULL;

    /* From RFC 2616 */
    static const char *wkday[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
    static const char *month[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    /* calculate the date */
    t = time(NULL);

    /* sync clock with amazon s3 */
    t = t + hdl->time_offset_with_s3;

#ifdef _WIN32
    if (!gmtime_s(&tmp, &t)) g_debug("localtime error");
#else
    if (!gmtime_r(&t, &tmp)) perror("localtime");
#endif

    //
    // detect these two troublemakers
    //
    if (subresource) {
    	if (g_str_has_prefix(subresource, "uploadId=") || g_strstr_len(subresource, -1, "&uploadId="))
	    subresource_uploadId = subresource;
    }

    for( q = query ; q && *q && ! g_str_has_prefix(*q,"uploadId=") ; ++q) 
	{ }
    if (q && *q)
	query_uploadId = *q;

    date = g_strdup_printf("%s, %02d %s %04d %02d:%02d:%02d GMT",
        wkday[tmp.tm_wday], tmp.tm_mday, month[tmp.tm_mon], 1900+tmp.tm_year,
        tmp.tm_hour, tmp.tm_min, tmp.tm_sec);

    szS3Date = g_strdup_printf("%04d%02d%02d",1900+tmp.tm_year,tmp.tm_mon+1,tmp.tm_mday);

    zulu_date = g_strdup_printf("%04d%02d%02dT%02d%02d%02dZ",
				1900+tmp.tm_year, tmp.tm_mon+1, tmp.tm_mday,
				tmp.tm_hour, tmp.tm_min, tmp.tm_sec);
    switch ( hdl->s3_api ) {

        case S3_API_SWIFT_1:
            if (!bucket) {
                buf = g_strdup_printf("X-Auth-User: %s", hdl->swift_account_id);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
                buf = g_strdup_printf("X-Auth-Key: %s", hdl->swift_access_key);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            } else {
                buf = g_strdup_printf("X-Auth-Token: %s", hdl->x_auth_token);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }
            break;

        case S3_API_SWIFT_2:
            if (bucket) {
                buf = g_strdup_printf("X-Auth-Token: %s", hdl->x_auth_token);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
                buf = g_strdup_printf("Accept: %s", "application/xml");
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            } else {
                buf = g_strdup_printf("Accept: %s", "application/json");
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }
            break;

        case S3_API_SWIFT_3:
            if (bucket) {
                buf = g_strdup_printf("X-Auth-Token: %s", hdl->x_auth_token);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
                buf = g_strdup_printf("Accept: %s", "application/xml");
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            } else {
                buf = g_strdup_printf("Accept: %s", "application/json");
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }
            break;

        case S3_API_OAUTH2:
            if (bucket) {
                buf = g_strdup_printf("Authorization: Bearer %s", hdl->access_token);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

	    buf = g_strdup_printf("x-goog-api-version: 2");
	    headers = curl_slist_append(headers, buf);
	    g_free(buf);

	    if (project_id) {
		buf = g_strdup_printf("x-goog-project-id: %s", project_id);
		headers = curl_slist_append(headers, buf);
		g_free(buf);
	    }
            break;

        case S3_API_CASTOR:
            if (g_str_equal(verb, "PUT") || g_str_equal(verb, "POST")) {
                if (key) {
                    buf = g_strdup("CAStor-Application: Amanda");
                    headers = curl_slist_append(headers, buf);
                    g_free(buf);
                    reps = g_strdup(hdl->reps); /* object replication level */
                } else {
                    reps = g_strdup(hdl->reps_bucket); /* bucket replication level */
                }

                /* set object replicas in lifepoint */
                buf = g_strdup_printf("lifepoint: [] reps=%s", reps);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
                g_free(reps);
            }
            break;

        case S3_API_AWS4:
        {
            /* http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html */
            /* http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html */

            GString *strSignedHeaders = g_string_new("");
            GString *string_to_sign;
            char *canonical_hash = NULL;
            unsigned char *strSecretKey = NULL;
            unsigned char *signkey1 = NULL;
            unsigned char *signkey2 = NULL;
            unsigned char *signkey3 = NULL;
            unsigned char *signingKey = NULL;
            unsigned char *signature = NULL;
            unsigned char *signatureHex = NULL;

            /* verb */
            auth_string = g_string_new(verb);
            g_string_append_c(auth_string, '\n');

            /* CanonicalizedResource */
            g_string_append_c(auth_string, '/');

            if (!hdl->use_subdomain)
                g_string_append(auth_string, bucket);

            if (key && key[0] == '/')
                ++key;
                
            if (key && !g_str_has_suffix(auth_string->str,"/")) 
                g_string_append_c(auth_string, '/');

            if (key) {
                char *esc_key = g_uri_escape_string(key, "/", FALSE); // non utf8 escaping
                g_string_append(auth_string, esc_key);
                g_free(esc_key);
            }
            g_string_append_c(auth_string, '\n');

	    // construct a sorted-params of query-list/subresource together
	    if (subresource || query)
	    {
	        int qlen = ( query ? g_strv_length((char**)query) : 0 );
		char **joinV = alloca(sizeof(char*)* ( qlen + 2 ));
		char **j = NULL;
		char *subtmp = ( subresource ? g_strdup_printf("%s=",subresource) : NULL );

		memcpy(joinV, query, sizeof(char*)*qlen);

                j = &joinV[qlen];
		if (subtmp) *j++ = subtmp;
		*j = NULL;

	        // sort all into new-correct order
		qsort(joinV, g_strv_length(joinV), sizeof(*joinV), subresource_sort_cmp);

		g_string_append(auth_string, joinV[0]);
		for ( j = joinV+1 ; *j ; j++ ) {
		    g_string_append_c(auth_string, '&');
		    g_string_append(auth_string, *j);
		}
		g_free(subtmp);
	    }
            g_string_append_c(auth_string, '\n');

            // if (.... && g_str_equal(subresource, "lifecycle") )
            if (subresource && is_non_empty_string(md5_hash) ) {
                g_string_append(auth_string, "content-md5:");
                g_string_append(auth_string, md5_hash);
                g_string_append_c(auth_string, '\n');

                buf = g_strdup_printf("Content-MD5: %s", md5_hash);
                headers = curl_slist_append(headers, buf);

                g_string_append(strSignedHeaders, "content-md5;");
                g_free(buf);
            }

            /* Header must be in alphebetic order */
	    g_string_append(auth_string, "host:");
            if (hdl->use_subdomain) {
                g_string_append_printf(auth_string, "%s.%s", bucket, hdl->host);
            } else {
                g_string_append(auth_string, hdl->host);
            }
            g_string_append_c(auth_string, '\n');

            g_string_append(strSignedHeaders, "host");

            // XXX: it may be impossible to go without the hash!
            if (data_SHA256Hash) {
                g_string_append(auth_string, "x-amz-content-sha256:");
                g_string_append(auth_string, data_SHA256Hash);
                g_string_append_c(auth_string, '\n');
                g_string_append(strSignedHeaders, ";x-amz-content-sha256");
            }

            if (copy_source) {
                g_string_append(auth_string, AMAZON_COPY_SOURCE_HEADER":");
                g_string_append(auth_string, copy_source);
                g_string_append_c(auth_string, '\n');

                g_string_append(strSignedHeaders, ";"AMAZON_COPY_SOURCE_HEADER);
            }

            g_string_append(auth_string, "x-amz-date:");
            g_string_append(auth_string, zulu_date);
            g_string_append_c(auth_string, '\n');
            g_string_append(strSignedHeaders, ";x-amz-date");

            if (hdl->server_side_encryption_header &&
                is_non_empty_string(hdl->server_side_encryption)) {
                g_string_append(auth_string, AMAZON_SERVER_SIDE_ENCRYPTION_HEADER);
                g_string_append_c(auth_string, ':');
                g_string_append(auth_string, hdl->server_side_encryption);
                g_string_append_c(auth_string, '\n');
                g_string_append(strSignedHeaders, ";"AMAZON_SERVER_SIDE_ENCRYPTION_HEADER);

                buf = g_strdup_printf(AMAZON_SERVER_SIDE_ENCRYPTION_HEADER ": %s",
                                      hdl->server_side_encryption);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

            if (!subresource_uploadId && !query_uploadId && is_non_empty_string(hdl->storage_class)) 
	    {
                g_string_append(auth_string, AMAZON_STORAGE_CLASS_HEADER);
                g_string_append_c(auth_string, ':');
                g_string_append(auth_string, hdl->storage_class);
                g_string_append_c(auth_string, '\n');
                g_string_append(strSignedHeaders, ";"AMAZON_STORAGE_CLASS_HEADER);

                buf = g_strdup_printf(AMAZON_STORAGE_CLASS_HEADER ": %s",
                                      hdl->storage_class);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

            /* no more header */
            g_string_append_c(auth_string, '\n');

            g_string_append(auth_string, strSignedHeaders->str);
            g_string_append_c(auth_string, '\n');

            if (data_SHA256Hash)
                g_string_append(auth_string, data_SHA256Hash);

            canonical_hash = s3_compute_sha256_hash_ptr((unsigned char *)auth_string->str, auth_string->len);

            if (!hdl->bucket_location) {
                hdl->bucket_location = g_strdup("us-east-1");
            }

            string_to_sign = g_string_new("AWS4-HMAC-SHA256\n");
            g_string_append(string_to_sign, zulu_date);
            g_string_append_c(string_to_sign, '\n');
            g_string_append(string_to_sign, szS3Date);
            g_string_append_c(string_to_sign, '/');
            g_string_append(string_to_sign, hdl->bucket_location);
            g_string_append(string_to_sign, "/s3/aws4_request");
            g_string_append_c(string_to_sign, '\n');
            g_string_append(string_to_sign, canonical_hash);

            //Calculate the AWS Signature Version 4
            strSecretKey = (unsigned char *)g_strdup_printf("AWS4%s", hdl->secret_key);

            signkey1 = EncodeHMACSHA256(strSecretKey, strlen((char *)strSecretKey),
                                        szS3Date, strlen(szS3Date));

            signkey2 = EncodeHMACSHA256(signkey1, 32, hdl->bucket_location, strlen(hdl->bucket_location));

            signkey3 = EncodeHMACSHA256(signkey2, 32, "s3", 2);

            signingKey = EncodeHMACSHA256(signkey3, 32, "aws4_request", 12);

            signature = EncodeHMACSHA256(signingKey, 32, string_to_sign->str, (int)string_to_sign->len);
            signatureHex = s3_tohex(signature, 32);

            if (data_SHA256Hash) {
                buf = g_strdup_printf("x-amz-content-sha256: %s", data_SHA256Hash);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

            buf = g_strdup_printf("x-amz-date: %s", zulu_date);
            headers = curl_slist_append(headers, buf);
            g_free(buf);

            buf = g_strdup_printf("Authorization: AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request,SignedHeaders=%s,Signature=%s", hdl->access_key, szS3Date, hdl->bucket_location, strSignedHeaders->str, signatureHex);
            headers = curl_slist_append(headers, buf);
            g_free(buf);

            if (hdl->verbose) {
                g_debug("bucket: %s", bucket);
                g_debug("auth_string->str: %s", auth_string->str);
                g_debug("string_to_sign: %s", string_to_sign->str);
                g_debug("strSignedHeaders: %s", strSignedHeaders->str);
                g_debug("canonical_hash: %s", canonical_hash);
                g_debug("strSecretKey: %s", strSecretKey);
                g_debug("signatureHex: %s", signatureHex);
            }

            g_free(canonical_hash);
            g_free(strSecretKey);
            g_free(signkey1);
            g_free(signkey2);
            g_free(signkey3);
            g_free(signingKey);
            g_free(signature);
            g_free(signatureHex);
            md5_hash = NULL;
            break;
         }

        default: // case S3_API_S3:
        {
            /* Build the string to sign, per the S3 spec.
             * See: "Authenticating REST Requests" - API Version 2006-03-01 pg 58
             * http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
             */

            /* verb */
            auth_string = g_string_new(verb);
            g_string_append_c(auth_string, '\n');

            /* Content-MD5 header */
            if (md5_hash)
                g_string_append(auth_string, md5_hash);
            g_string_append_c(auth_string, '\n');

            if (content_type) {
                g_string_append(auth_string, content_type);
            }
            g_string_append_c(auth_string, '\n');

            /* Date */
            g_string_append(auth_string, date);
            g_string_append_c(auth_string, '\n');

            if (copy_source) {
                g_string_append(auth_string, AMAZON_COPY_SOURCE_HEADER":");
                g_string_append(auth_string, copy_source);
                g_string_append_c(auth_string, '\n');
            }

            /* CanonicalizedAmzHeaders, sorted lexicographically */
            if (is_non_empty_string(hdl->user_token)) {
                g_string_append(auth_string, AMAZON_SECURITY_HEADER);
                g_string_append_c(auth_string, ':');
                g_string_append(auth_string, hdl->user_token);
                g_string_append_c(auth_string, ',');
                g_string_append(auth_string, STS_PRODUCT_TOKEN);
                g_string_append_c(auth_string, '\n');
            }

            /* CanonicalizedAmzHeaders, sorted lexicographically */
            if (is_non_empty_string(hdl->session_token)) {
                g_string_append(auth_string, AMAZON_SECURITY_HEADER);
                g_string_append_c(auth_string, ':');
                g_string_append(auth_string, hdl->session_token);
                g_string_append_c(auth_string, '\n');
            }

            if (hdl->server_side_encryption_header &&
                is_non_empty_string(hdl->server_side_encryption)) {
                g_string_append(auth_string, AMAZON_SERVER_SIDE_ENCRYPTION_HEADER);
                g_string_append_c(auth_string, ':');
                g_string_append(auth_string, hdl->server_side_encryption);
                g_string_append_c(auth_string, '\n');
            }

            if (!subresource_uploadId && !query_uploadId && is_non_empty_string(hdl->storage_class)) {
                g_string_append(auth_string, AMAZON_STORAGE_CLASS_HEADER);
                g_string_append_c(auth_string, ':');
                g_string_append(auth_string, hdl->storage_class);
                g_string_append_c(auth_string, '\n');
            }

            /* CanonicalizedResource */
            if (hdl->service_path) {
                g_string_append(auth_string, hdl->service_path);
            }
            g_string_append_c(auth_string, '/');
            if (bucket && hdl->use_subdomain)
                g_string_append(auth_string, bucket); // NOTE: without escape

            if (bucket && !hdl->use_subdomain)
            {
                esc_bucket = g_uri_escape_string(bucket, NULL, 0);
                if (!esc_bucket) goto cleanup;
                g_string_append(auth_string, esc_bucket);
            }

            if (bucket && hdl->use_subdomain)
                g_string_append_c(auth_string, '/');
            else if (bucket && key)
                g_string_append_c(auth_string, '/');

            if (key) {
                esc_key = g_uri_escape_string(key, NULL, 0);
                if (!esc_key) goto cleanup;
                g_string_append(auth_string, esc_key);
            }

	    if (subresource) {
                 g_string_append_c(auth_string, '?');
                 g_string_append(auth_string, subresource);
            }

	    //
	    // must determine if some params should be copied over
	    // NOTE: Google does not agree at all with Amazon on subresources!!
	    //
	    if (query && !subresource && !hdl->use_google_subresources)
	    {
		int count = 0;
	    	const char **q = query;

		for ( ; *q ; q++ )
		{
		    int nmlen = strchrnul(*q,'=') - *q; // get param name only [or whole string if no '='
		    char *qname = strncpy(alloca(nmlen+1), *q, nmlen);
                    char *r;

                    qname[nmlen] = '\0'; // terminate correctly
		    r = bsearch(&qname, amz_subrsrc_params, 
		    		g_strv_length((char**)amz_subrsrc_params), sizeof(char*), 
				subresource_sort_cmp);
		    if (!r) continue;

		    // param name is found.. so append it
		    g_string_append_c(auth_string, ( !count++ ? '?' : '&'));
		    g_string_append(auth_string, *q);
		}
	    }

            /* run HMAC-SHA1 on the canonicalized string */
            md = g_byte_array_sized_new(EVP_MAX_MD_SIZE+1);
            {
#if (defined OPENSSL_VERSION_NUMBER && OPENSSL_VERSION_NUMBER < 0x10100000L) \
        || defined LIBRESSL_VERSION_NUMBER
                HMAC_CTX ctx_orig;
                HMAC_CTX *ctx = &ctx_orig;
                HMAC_CTX_init(ctx);
#else
                HMAC_CTX *ctx = HMAC_CTX_new();
                HMAC_CTX_reset(ctx);
#endif

                HMAC_Init_ex(ctx, hdl->secret_key, (int) strlen(hdl->secret_key),
                             EVP_sha1(), NULL);
                HMAC_Update(ctx, (unsigned char*) auth_string->str, auth_string->len);
                HMAC_Final(ctx, md->data, &md->len);

#if (defined OPENSSL_VERSION_NUMBER && OPENSSL_VERSION_NUMBER < 0x10100000L) \
            || defined LIBRESSL_VERSION_NUMBER
                HMAC_CTX_cleanup(ctx);
#else
                HMAC_CTX_free(ctx);
#endif
            }
            auth_base64 = g_base64_encode(md->data, md->len);

            if (hdl->verbose) {
                g_debug("bucket: %s", bucket);
                g_debug("auth_string->str: \"%s\"", auth_string->str);
                g_debug("auth_base64: %s", auth_base64);
            }

            /* append the new headers */
            if (is_non_empty_string(hdl->user_token)) {
                /* Devpay headers are included in hash. */
                buf = g_strdup_printf(AMAZON_SECURITY_HEADER ": %s",
                                      hdl->user_token);
                headers = curl_slist_append(headers, buf);
                g_free(buf);

                buf = g_strdup_printf(AMAZON_SECURITY_HEADER ": %s",
                                      STS_PRODUCT_TOKEN);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

            if (is_non_empty_string(hdl->session_token)) {
                /* Devpay headers are included in hash. */
                buf = g_strdup_printf(AMAZON_SECURITY_HEADER ": %s",
                                      hdl->session_token);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

            if (hdl->server_side_encryption_header &&
                is_non_empty_string(hdl->server_side_encryption)) {
                buf = g_strdup_printf(AMAZON_SERVER_SIDE_ENCRYPTION_HEADER ": %s",
                                      hdl->server_side_encryption);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

            if (!subresource_uploadId && !query_uploadId && is_non_empty_string(hdl->storage_class)) {
                buf = g_strdup_printf(AMAZON_STORAGE_CLASS_HEADER ": %s",
                                      hdl->storage_class);
                headers = curl_slist_append(headers, buf);
                g_free(buf);
            }

            /* Remove Content-Type header */
            buf = g_strdup("Content-Type:");
            headers = curl_slist_append(headers, buf);
            g_free(buf);

            buf = g_strdup_printf("Authorization: AWS %s:%s",
                                  hdl->access_key, auth_base64);
            headers = curl_slist_append(headers, buf);
            g_free(buf);
            break;
        }
    }

    if (md5_hash && '\0' != md5_hash[0]) {
        buf = g_strdup_printf("Content-MD5: %s", md5_hash);
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }

    if (content_type) {
        buf = g_strdup_printf("Content-Type: %s", content_type);
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }

    buf = g_strdup_printf("Date: %s", date);
    headers = curl_slist_append(headers, buf);
    g_free(buf);

cleanup:
    g_free(date);
    g_free(szS3Date);
    g_free(esc_bucket);
    g_free(esc_key);
    if (md) g_byte_array_free(md, TRUE);
    g_free(auth_base64);
    if (auth_string) g_string_free(auth_string, TRUE);

    return headers;
}

/* Functions for a SAX parser to parse the XML failure from Amazon */

/* Private structure for our "thunk", which tracks where the user is in the list
 *  * of keys. */
struct failure_thunk {
    gboolean want_text;

    int in_title:1;
    int in_body:1;
    int in_code:1;
    int in_message:1;
    int in_details:1;
    int in_access:1;
    int in_token:1;
    int in_serviceCatalog:1;
    int in_service:1;
    int in_endpoint:1;
    int in_uploadId:1;
    int in_etag:1;
    gint in_others;

    gchar *text;
    gsize text_len;

    gchar *message;
    gchar *details;
    gchar *error_name;
    gchar *token_id;
    gchar *service_type;
    gchar *service_public_url;
    time_t expires;
    gchar *uploadId;
    gchar *etag;

    gchar *bucket_location;
};

static void
failure_start_element(GMarkupParseContext *context G_GNUC_UNUSED,
                   const gchar *element_name,
                   const gchar **attribute_names,
                   const gchar **attribute_values,
                   gpointer user_data,
                   GError **error G_GNUC_UNUSED)
{
    struct failure_thunk *thunk = (struct failure_thunk *)user_data;
    const gchar **att_name, **att_value;

    if (g_ascii_strcasecmp(element_name, "title") == 0) {
        thunk->in_title = 1;
	thunk->in_others = 0;
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "body") == 0) {
        thunk->in_body = 1;
	thunk->in_others = 0;
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "code") == 0) {
        thunk->in_code = 1;
	thunk->in_others = 0;
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "message") == 0) {
        thunk->in_message = 1;
	thunk->in_others = 0;
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "details") == 0) {
        thunk->in_details = 1;
	thunk->in_others = 0;
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "access") == 0) {
        thunk->in_access = 1;
	thunk->in_others = 0;
    } else if (g_ascii_strcasecmp(element_name, "token") == 0) {
        thunk->in_token = 1;
	thunk->in_others = 0;
	for (att_name=attribute_names, att_value=attribute_values;
	     *att_name != NULL;
	     att_name++, att_value++) {
	    if (g_str_equal(*att_name, "id")) {
		thunk->token_id = g_strdup(*att_value);
	    }
	    if (g_str_equal(*att_name, "expires") && strlen(*att_value) >= 19) {
		thunk->expires = rfc3339_date(*att_value) - 600;
	    }
	}
    } else if (g_ascii_strcasecmp(element_name, "serviceCatalog") == 0) {
        thunk->in_serviceCatalog = 1;
	thunk->in_others = 0;
    } else if (g_ascii_strcasecmp(element_name, "service") == 0) {
        thunk->in_service = 1;
	thunk->in_others = 0;
	for (att_name=attribute_names, att_value=attribute_values;
	     *att_name != NULL;
	     att_name++, att_value++) {
	    if (g_str_equal(*att_name, "type")) {
		thunk->service_type = g_strdup(*att_value);
	    }
	}
    } else if (g_ascii_strcasecmp(element_name, "endpoint") == 0) {
        thunk->in_endpoint = 1;
	thunk->in_others = 0;
	if (thunk->service_type &&
	    g_str_equal(thunk->service_type, "object-store")) {
	    char *service_public_url = NULL;
	    char *region = NULL;
	    for (att_name=attribute_names, att_value=attribute_values;
		 *att_name != NULL;
		 att_name++, att_value++) {
		if (g_str_equal(*att_name, "publicURL")) {
		    service_public_url = g_strdup(*att_value);
		}
		if (g_str_equal(*att_name, "region")) {
		    region = g_strdup(*att_value);
		}
	    }
	    if (region && service_public_url) {
		if (!thunk->bucket_location ||
		    strcmp(thunk->bucket_location, region) == 0) {
		    thunk->service_public_url = service_public_url;
		} else {
		    g_free(service_public_url);
		}
	    } else {
		thunk->service_public_url = service_public_url;
	    }
	    g_free(region);
	}
    } else if (g_ascii_strcasecmp(element_name, "error") == 0) {
	for (att_name=attribute_names, att_value=attribute_values;
	     *att_name != NULL;
	     att_name++, att_value++) {
	    if (g_str_equal(*att_name, "message")) {
		thunk->message = g_strdup(*att_value);
	    }else if (g_str_equal(*att_name, "title")) {
		thunk->error_name = g_strdup(*att_value);
//		hdl->last_s3_error_code = s3_error_code_from_name(thunk->error_name);
	    }
	}
    } else if (g_ascii_strcasecmp(element_name, "uploadid") == 0) {
        thunk->in_uploadId = 1;
	thunk->in_others = 0;
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "etag") == 0) {
        thunk->in_etag = 1;
	thunk->in_others = 0;
        thunk->want_text = 1;
    } else {
	thunk->in_others++;
    }
}

static void
failure_end_element(GMarkupParseContext *context G_GNUC_UNUSED,
                 const gchar *element_name,
                 gpointer user_data,
                 GError **error G_GNUC_UNUSED)
{
    struct failure_thunk *thunk = (struct failure_thunk *)user_data;

    if (g_ascii_strcasecmp(element_name, "title") == 0) {
	char *p = strchr(thunk->text, ' ');
	if (p) {
	    p++;
	    if (*p) {
		thunk->error_name = g_strdup(p);
	    }
	}
	g_free(thunk->text);
	thunk->text = NULL;
        thunk->in_title = 0;
    } else if (g_ascii_strcasecmp(element_name, "body") == 0) {
	g_free(thunk->message);
	thunk->message = thunk->text;
	g_strstrip(thunk->message);
	thunk->text = NULL;
        thunk->in_body = 0;
    } else if (g_ascii_strcasecmp(element_name, "code") == 0) {
	g_free(thunk->error_name);
	thunk->error_name = thunk->text;
	thunk->text = NULL;
        thunk->in_code = 0;
    } else if (g_ascii_strcasecmp(element_name, "message") == 0) {
	g_free(thunk->message);
	thunk->message = thunk->text;
	thunk->text = NULL;
        thunk->in_message = 0;
    } else if (g_ascii_strcasecmp(element_name, "details") == 0) {
	g_free(thunk->details);
	thunk->details = thunk->text;
	thunk->text = NULL;
        thunk->in_details = 0;
    } else if (g_ascii_strcasecmp(element_name, "access") == 0) {
	g_free(thunk->message);
	thunk->message = thunk->text;
	thunk->text = NULL;
        thunk->in_access = 0;
    } else if (g_ascii_strcasecmp(element_name, "token") == 0) {
	g_free(thunk->message);
	thunk->message = thunk->text;
	thunk->text = NULL;
        thunk->in_token = 0;
    } else if (g_ascii_strcasecmp(element_name, "serviceCatalog") == 0) {
	g_free(thunk->message);
	thunk->message = thunk->text;
	thunk->text = NULL;
        thunk->in_serviceCatalog = 0;
    } else if (g_ascii_strcasecmp(element_name, "service") == 0) {
	g_free(thunk->message);
	thunk->message = thunk->text;
	thunk->text = NULL;
	g_free(thunk->service_type);
	thunk->service_type = NULL;
        thunk->in_service = 0;
    } else if (g_ascii_strcasecmp(element_name, "endpoint") == 0) {
	g_free(thunk->message);
	thunk->message = thunk->text;
	thunk->text = NULL;
        thunk->in_endpoint = 0;
    } else if (g_ascii_strcasecmp(element_name, "uploadid") == 0) {
	g_free(thunk->uploadId);
	thunk->uploadId = thunk->text;
	thunk->text = NULL;
        thunk->in_uploadId = 0;
    } else if (g_ascii_strcasecmp(element_name, "etag") == 0) {
	g_free(thunk->etag);
	thunk->etag = thunk->text;
	thunk->text = NULL;
        thunk->in_etag = 0;
    } else {
	thunk->in_others--;
	g_free(thunk->text);
	thunk->text = NULL;
    }
}

static void
failure_text(GMarkupParseContext *context G_GNUC_UNUSED,
          const gchar *text,
          gsize text_len,
          gpointer user_data,
          GError **error G_GNUC_UNUSED)
{
    struct failure_thunk *thunk = (struct failure_thunk *)user_data;

    if (thunk->want_text && thunk->in_others == 0) {
        char *new_text;

        new_text = g_strndup(text, text_len);
	if (thunk->text) {
	    strappend(thunk->text, new_text);
	    g_free(new_text);
	} else {
	    thunk->text = new_text;
	}
    }
}

static void
parse_swift_v2_serviceCatalog(
    gpointer data,
    gpointer user_data);
static void
parse_swift_v3_catalog(
    gpointer data,
    gpointer user_data);

static s3_result_t
record_response(S3Handle *hdl,
                   CURLcode curl_code,
                   char *curl_error_buffer,
                   eS3InternalContentType bodytype,
                   const char *content_md5)
{
    struct failure_thunk thunk = { 0 };
    char *body_copy = NULL;
    guint body_len = 0;
    CURLcode last_curl_code = CURLE_OK;

    if (!hdl)
        return S3_RESULT_FAIL;

    body_len = hdl->last_response_body_size;
    last_curl_code = hdl->last_curl_code; // save if needed
    hdl->last_curl_code = curl_code; // reset each time...

    /* bail out from a CURL error */
    if (curl_code != CURLE_OK) {
        if (curl_code == last_curl_code)
            return S3_RESULT_RETRY_BACKOFF; /* perhaps a network error; consider a slower retry of request */
        hdl->last_message = g_strdup_printf("CURL error: %s", curl_error_buffer);
    }

    // go no further?
    if (hdl->last_response_code == 0)
        return S3_RESULT_RETRY_BACKOFF; // must be a curl error

    /* check ETag, if present and not CAStor */
    if (hdl->last_etag && content_md5) {
    	if (strlen(hdl->last_etag) == strlen(content_md5) && g_ascii_strcasecmp(hdl->last_etag, content_md5) != 0) {
	    hdl->last_message = g_strdup("S3 Error: Possible data corruption (ETag returned by Amazon did not match the MD5 hash of the data sent)");
	    return S3_RESULT_RETRY;
	}
    	if (strlen(hdl->last_etag) != strlen(content_md5)) 
	    g_warning("%s: etag format mismatch \"%s\" != \"%s\"",__FUNCTION__, hdl->last_etag, content_md5);
    }

    /* Now look at the body to try to get the actual Amazon error message. */

    if (!body_len && hdl->last_response_code >= 200 && hdl->last_response_code < 400 ) {
        /* 2xx and 3xx codes without body are good result */
        return S3_RESULT_OK;
    }

    if (!body_len || !hdl->last_response_body) {
        hdl->last_message = g_strdup("S3 Error: Unknown (empty response body)");
        return S3_RESULT_RETRY_BACKOFF; /* perhaps a network error; consider a retry of request */
    }

    /* impose a reasonable limit on body size */
    if (body_len > MAX_ERROR_RESPONSE_LEN) {
        hdl->last_message = g_strdup("S3 Error: Unknown (response body too large to parse)");
        return S3_RESULT_FAIL;
    }

    //////////////////////////////////// must clean up from here....

    thunk.bucket_location = hdl->bucket_location;
    body_copy = g_strndup(hdl->last_response_body, body_len);

    if (hdl->verbose) {
	g_debug("[%u] api #%d and data content #%d", hdl->last_response_code, hdl->s3_api, bodytype);
	if (bodytype > CONTENT_UNKN)
	    g_debug("[%u] data (if usable): -----\n%s\n-------", hdl->last_response_code, body_copy);
    }

#define MIX_API_AND_CONTENT(api,content)  ( (api)<<16 | ((int)content) )

    g_free(hdl->last_message);
    hdl->last_message = NULL;

    switch ( MIX_API_AND_CONTENT(hdl->s3_api,bodytype) )
    {
        case MIX_API_AND_CONTENT(S3_API_SWIFT_1,CONTENT_TEXT):
        case MIX_API_AND_CONTENT(S3_API_SWIFT_2,CONTENT_TEXT):
        {
            char *b = body_copy;
            char *p = strchr(b, '\n');
            char *p1;
            if (p) { /* first line: error code */
                *p = '\0';
                p++;
                p1 = strchr(b, ' ');
                if (p1) {
                    p1++;
                    if (*p1) {
                        thunk.error_name = g_strdup(p1);
                    }
                }
                b = p;
            }
            p = strchr(b, '\n');
            if (p) { /* second line: error message */
                *p = '\0';
                p++;
                thunk.message = g_strdup(p);
                g_strstrip(thunk.message);
                b = p;
            }
            break;
        }

        case MIX_API_AND_CONTENT(S3_API_SWIFT_3,CONTENT_TEXT):
        {
            regmatch_t pmatch[2];

            if (!s3_regexec_wrap(&html_error_name_regex, body_copy, 2, pmatch, 0)) {
                thunk.error_name = find_regex_substring(body_copy, pmatch[1]);
                am_strrmspace(thunk.error_name);
            }
            if (!s3_regexec_wrap(&html_message_regex, body_copy, 2, pmatch, 0)) {
                thunk.message = find_regex_substring(body_copy, pmatch[1]);
            }
            break;
        }

        case MIX_API_AND_CONTENT(S3_API_SWIFT_1,CONTENT_JSON):
	swift_json_fallback:
	    {
		char *code = NULL;
		char *details = NULL;
		regmatch_t pmatch[2];

		if (!s3_regexec_wrap(&code_regex, body_copy, 1, pmatch, 0)) {
		    code = find_regex_substring(body_copy, pmatch[1]);
		}
		if (!s3_regexec_wrap(&details_regex, body_copy, 2, pmatch, 0)) {
		    details = find_regex_substring(body_copy, pmatch[1]);
		}
		if (!details) {
		    if (!s3_regexec_wrap(&json_message_regex, body_copy, 2, pmatch, 0)) {
			details = find_regex_substring(body_copy, pmatch[1]);
		    }
		}
		if (code && details) {
		    hdl->last_message = g_strdup_printf("%s (%s)", details, code);
		} else if (code) {
		    hdl->last_message = g_strdup_printf("(%s)", code);
		} else if (details) {
		    hdl->last_message = g_strdup_printf("%s", details);
		} else {
		    hdl->last_message = NULL;
		}
		g_free(code);
		g_free(details);
		break;
	    }

        case MIX_API_AND_CONTENT(S3_API_SWIFT_2,CONTENT_JSON):
	    do {
		amjson_t *json;
		amjson_t *json_access, *json_token, *json_catalog, *json_token_id, *json_token_expires;

		if (!hdl->getting_swift_2_token) break;

		json = parse_json(body_copy);

		if (get_json_type(json) != JSON_HASH) break;
		json_access = get_json_hash_from_key(json, "access");

		if (!json_access) break;
		if (get_json_type(json_access) != JSON_HASH) break;

		json_token = get_json_hash_from_key(json_access, "token");
		json_catalog = get_json_hash_from_key(json_access, "serviceCatalog");

		if (!json_token) break;
		if (!json_catalog) break;
		if (get_json_type(json_token) != JSON_HASH) break;
		if (get_json_type(json_catalog) != JSON_ARRAY) break;

		json_token_id = get_json_hash_from_key(json_token, "id");
		json_token_expires = get_json_hash_from_key(json_token, "expires");

		if (!json_token_id) break;
		if (!json_token_expires) break;
		if (get_json_type(json_token_id) != JSON_STRING) break;
		if (get_json_type(json_token_expires) != JSON_STRING) break;

		/// access.token.id
		thunk.token_id = g_strdup(get_json_string(json_token_id));
		if (!thunk.token_id) break;

		/// access.token.expires
		thunk.expires = rfc3339_date(get_json_string(json_token_expires));
		if (!thunk.expires) break;

		/// access.serviceCatalog.*
		foreach_json_array(json_catalog, parse_swift_v2_serviceCatalog, &thunk);
		if (!thunk.service_public_url) break;

		hdl->expires = thunk.expires;

		if (!hdl->x_auth_token) {
		    hdl->x_auth_token = thunk.token_id;
		    thunk.token_id = NULL;
		    if (hdl->verbose)
			g_debug("x_auth_token: %s", hdl->x_auth_token);
		}

		if (!hdl->x_storage_url) {
		    hdl->x_storage_url = thunk.service_public_url;
		    thunk.service_public_url = NULL;
		    if (hdl->verbose)
			g_debug("x_storage_url: %s", hdl->x_storage_url);
		}

		if (!hdl->x_storage_url && !thunk.message && !thunk.error_name) {
		    thunk.message = g_strdup_printf("Did not find the publicURL [region='%s']", thunk.bucket_location);
		    thunk.error_name =g_strdup("RegionNotFound");
		}
		goto cleanup_thunk; // sucessfully continue

	    } while(FALSE);
	    goto swift_json_fallback;

        case MIX_API_AND_CONTENT(S3_API_SWIFT_3,CONTENT_JSON):
	    do {
		amjson_t *json;
		amjson_t *json_token, *json_catalog, *json_expires_at;

		if (!hdl->getting_swift_3_token) break;

		json = parse_json(body_copy);

		if (get_json_type(json) != JSON_HASH) break;

		json_token = get_json_hash_from_key(json, "token");

		if (!json_token) break;
		if (get_json_type(json_token) != JSON_HASH) break;

		json_catalog = get_json_hash_from_key(json_token, "catalog");
		json_expires_at = get_json_hash_from_key(json_token, "expires_at");

		if (!json_catalog) break;
		if (!json_expires_at) break;
		if (get_json_type(json_catalog) != JSON_ARRAY) break;
		if (get_json_type(json_expires_at) != JSON_STRING) break;

		// token.catalog.*
		foreach_json_array(json_catalog, parse_swift_v3_catalog, &thunk);
		// token.expires_at
		thunk.expires = rfc3339_date(get_json_string(json_expires_at));

		if (!thunk.expires && thunk.service_public_url) break;
		if (!thunk.service_public_url) break;

		hdl->expires = ( thunk.expires ? : hdl->expires );

		if (!hdl->x_storage_url) {
		    hdl->x_storage_url = thunk.service_public_url;
		    thunk.service_public_url = NULL;
		    if (hdl->verbose)
			g_debug("x_storage_url: %s", hdl->x_storage_url);
		} 

		if (!hdl->x_storage_url && !thunk.message && !thunk.error_name) {
		    thunk.message = g_strdup_printf("Did not find the publicURL [region='%s']", thunk.bucket_location);
		    thunk.error_name =g_strdup("RegionNotFound");
		}
		goto cleanup_thunk; // sucessfully continue

	    } while(FALSE);
	    goto swift_json_fallback;

    case MIX_API_AND_CONTENT(S3_API_CASTOR,CONTENT_JSON):
        hdl->last_message = body_copy;
	body_copy = NULL;
	break;

    default:
        // skip if not possibly XML
        if (bodytype == CONTENT_XML)
	{
	    static GMarkupParser parser = { failure_start_element, failure_end_element, failure_text, NULL, NULL };
	    GMarkupParseContext *ctxt = NULL;
	    GError *err = NULL;

	    /* run the parser over it */
	    ctxt = g_markup_parse_context_new(&parser, 0, (gpointer)&thunk, NULL);

	    (void) ( g_markup_parse_context_parse(ctxt, body_copy, body_len, &err) && 
		    g_markup_parse_context_end_parse(ctxt, &err) );

	    if (err) hdl->last_message = g_strdup(err->message);

	    g_markup_parse_context_free(ctxt);
	} 

	if (bodytype <= CONTENT_BINARY && body_len < 50)
	    g_debug("[%u] data: %s", hdl->last_response_code, body_copy);
        break;
    } // switch

cleanup_thunk:
    if (thunk.error_name)
        hdl->last_s3_error_code = s3_error_code_from_name(thunk.error_name);

    if (thunk.message && thunk.details)
	hdl->last_message = g_strdup_printf("%s: %s", thunk.message, thunk.details);

    if (thunk.message && !thunk.details) {
	hdl->last_message = thunk.message;
	thunk.message = NULL; /* steal the reference to the string */
    }
    if (!thunk.message && thunk.details) {
	hdl->last_message = thunk.details;
	thunk.details = NULL; /* steal the reference to the string */
    }
    if (thunk.uploadId) {
	g_free(hdl->last_uploadId);
        hdl->last_uploadId = thunk.uploadId;
        thunk.uploadId = NULL; /* steal the reference to the string */
    }
    if (thunk.etag) {
	g_free(hdl->last_etag);
        if (thunk.etag[0] == '"') {
            if (strrchr(thunk.etag+1,'"')) *strchr(thunk.etag+1,'"') = '\0';
            hdl->last_etag = g_strdup(thunk.etag+1);
            g_free(thunk.etag);
        } else {
            hdl->last_etag = thunk.etag;
        }
        thunk.etag = NULL; /* steal the reference to the string */
    }
    g_free(thunk.text);
    g_free(thunk.message);
    g_free(thunk.details);
    g_free(thunk.error_name);
    g_free(thunk.token_id);
    g_free(thunk.service_type);
    g_free(thunk.service_public_url);
    g_free(thunk.uploadId);
    g_free(thunk.etag);
    g_free(body_copy);

    return S3_RESULT_OK;
}

int
s3_curl_debug_message(CURL *curl G_GNUC_UNUSED,
           curl_infotype type,
           char *s,
           size_t len,
           void *unused G_GNUC_UNUSED)
{
    char *lineprefix;
    char *message;
    char **lines, **line;
    size_t i;

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

        case CURLINFO_DATA_IN:
            if (len > 3000) return 0;
            for (i=0;i<len;i++) {
                if ( s[i] < 0 || (g_ascii_iscntrl(s[i]) && ! g_ascii_isspace(s[i])) ) {
                    return 0;
                }
            }
            lineprefix="Data In: ";
            break;

        case CURLINFO_DATA_OUT:
            if (len > 3000) return 0;
            for (i=0;i<len;i++) {
                if ( s[i] < 0 || (g_ascii_iscntrl(s[i]) && ! g_ascii_isspace(s[i])) ) {
                    return 0;
                }
            }
            lineprefix="Data Out: ";
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
	g_debug("%s [%ld] %s", lineprefix, line - lines, *line);
    }
    g_strfreev(lines);

    return 0;
}


static s3_result_t
s3_refresh_token(S3Handle *hdl)
{
    s3_result_t result = S3_RESULT_FAIL; /* assume the worst.. */
    gboolean in_token_fetch = FALSE;
    char *access_token = NULL;
    const char *token_fxn_name = NULL;
    s3_result_t (*token_fetch_fxn)(S3Handle *hdl) = NULL;

    switch (hdl->s3_api) {
        case S3_API_OAUTH2:
            in_token_fetch = hdl->getting_oauth2_access_token; // prevent recursion?
            access_token = hdl->access_token;
            token_fetch_fxn = &oauth2_get_access_token;
            token_fxn_name = "oauth2_get_access_token";
            break;
        case S3_API_SWIFT_2:
            in_token_fetch = hdl->getting_swift_2_token; // prevent recursion?
            access_token = hdl->x_auth_token;
            token_fetch_fxn = &get_openstack_swift_api_v2_setting;
            token_fxn_name = "get_openstack_swift_api_v2_setting";
            break;
        case S3_API_SWIFT_3:
            in_token_fetch = hdl->getting_swift_3_token; // prevent recursion?
            access_token = hdl->x_auth_token;
            token_fetch_fxn = &get_openstack_swift_api_v3_setting;
            token_fxn_name = "get_openstack_swift_api_v3_setting";
            break;
        default:
	    break;
            // S3_API_UNKNOWN
            // S3_API_S3
            // S3_API_SWIFT_1
            // S3_API_CASTOR
            // S3_API_AWS4
    }

    if (!token_fetch_fxn)
	return S3_RESULT_OK;
    if (in_token_fetch)
	return S3_RESULT_OK;
    if (access_token && time(NULL) <= hdl->expires)
	return S3_RESULT_OK;

    // NOTE: likely will recurse!
    result = (*token_fetch_fxn)(hdl); // success is true
    if ( result != S3_RESULT_OK) {
	g_debug("%s returned %d", token_fxn_name, result);
    }
    return result; // cut off now...
}


static s3_result_t
perform_request(S3Handle *hdl,
                const char *verb,
                const char *bucket,
                const char *key,
                const char *subresource,
                const char **query,
                const char *content_type,
                const char *project_id,    // for auth
		struct curl_slist *user_headers,
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
                const result_handling_t *result_handling,
		gboolean chunked)
{
    char *url = NULL;
    s3_result_t result = S3_RESULT_FAIL; /* assume the worst.. */
    s3_result_t tok_result; /* assume the worst.. */
    CURLcode curl_code = CURLE_OK;
    char curl_error_buffer[CURL_ERROR_SIZE] = "";
    struct curl_slist *headers = NULL;
    const char *amzCopySource = NULL;

    // start max_buffer_size as 1 == fail all writes
    S3InternalData int_writedata; // must zero with memset()
    CurlBuffer *s3buf_dup = &int_writedata.dup_buffer;
    /* corresponds to PUT, HEAD, GET, and POST */
    int curlopt_methodopt = 0;
    int curlopt_setsizeopt = 0;
    /* do we want to examine the headers */
    const char *curlopt_customrequest = NULL;
    /* for MD5 calculation */
    gchar *md5_hash_hex = NULL, *md5_hash_b64 = NULL;
    xferbytes_t request_body_size = 0;
    char *sha256_hash_hex = NULL;
    gulong backoff;
    gint retries = 0;
    gint retry_num = 0;
    gint retry_after_close = 0;

    g_assert(hdl != NULL && hdl->curl != NULL);
    // cleared to zero.. disallow use at first...
    memset(&int_writedata, '\0', sizeof(int_writedata));
    s3_buffer_ctor(s3buf_dup,1,hdl->verbose);

    // clear all last_* response strings too
    s3_reset(hdl);

    tok_result = s3_refresh_token(hdl);
    if ( tok_result != S3_RESULT_OK)
	return tok_result; // cut off now...
    
    //////////////////////////////////////////////////////////////////////////////////////////
    // hold exclusive access to this curl context until completed
    //////////////////////////////////////////////////////////////////////////////////////////
    g_assert( g_mutex_trylock(&hdl->curl_mutex) );

    // reorder query in ascii-char order
    if (query)
	qsort(query, g_strv_length((char**)query), sizeof(*query), subresource_sort_cmp);

    url = build_url(hdl, bucket, key, subresource, query);
    if (!url) goto cleanup;

    /* libcurl may behave strangely if these are not set correctly */
    if (g_str_has_prefix(verb, "PUT")) {
        curlopt_methodopt = CURLOPT_UPLOAD;
        curlopt_setsizeopt = CURLOPT_INFILESIZE;   // dangerous for large sizes!
#if LIBCURL_VERSION_NUM > 0x070b00
        curlopt_setsizeopt = CURLOPT_INFILESIZE_LARGE;
#endif
    } else if (g_str_has_prefix(verb, "GET")) {
        curlopt_methodopt = CURLOPT_HTTPGET;
    } else if (g_str_has_prefix(verb, "POST")) {
        curlopt_methodopt = CURLOPT_POST;
        curlopt_setsizeopt = CURLOPT_POSTFIELDSIZE; // dangerous for large sizes!
#if LIBCURL_VERSION_NUM > 0x070b00
        curlopt_setsizeopt = CURLOPT_POSTFIELDSIZE_LARGE;
#endif
    } else if (g_str_has_prefix(verb, "HEAD")) {
        curlopt_methodopt = CURLOPT_NOBODY;
    } else {
        curlopt_customrequest = verb;
    }

    if (!read_func) {
        /* Curl will use fread() otherwise */
        read_func = s3_empty_read_func;
    }

    if (hdl->s3_api == S3_API_AWS4 && !read_data) 
        sha256_hash_hex = s3_compute_sha256_hash_ptr((unsigned char *)"", 0);
    
    // should not have zero-bytes in read-buffer
    g_assert( !read_data || s3_buffer_read_size_func(read_data) );

    // init write-thru functions and block use of s3buf_dup (at first)
    s3_internal_write_init(&int_writedata, hdl,
           write_func, write_reset_func, write_data);

    // need this to extract one case for authentication...
    {
	struct curl_slist *header = user_headers;
        char *prefix = AMAZON_COPY_SOURCE_HEADER ": ";
        int prefix_len = strlen(prefix);

        for( ; header && strncasecmp(header->data,prefix,prefix_len) != 0 
               ; header = header->next )
           { }

        if (header)
            amzCopySource = header->data + prefix_len; // use trailing chars as content
    }

    for( (backoff = BACKOFF_DELAY_START_USEC),
         (retries = BACKOFF_MAX_RETRIES)
           ; backoff <= MAX_BACKOFF_DELAY_USEC && retries
              ; ++retry_num, --retries )
    {
	struct curl_slist *header;

        /* reset things */
        curl_slist_free_all(headers);
        headers = NULL;
        curl_error_buffer[0] = '\0';

	// reset upload data back to start of read [if possible]
        if (retry_num && read_data && read_reset_func) {
            read_reset_func(read_data); 

	    if (read_reset_func == (device_reset_func_t) s3_buffer_read_reset_func && ((CurlBuffer*)read_data)->cancel) {
		g_warning("[%p] read_data: retry RE-BUFFERING FAILURE: r=%d", __FUNCTION__, result);
		break;
	    }
	}

        // for download data.. and reset all last_* strings
        if (retry_num)
	    s3_internal_write_reset_func(&int_writedata);

        // upload data is ready with unknown size?
        if (size_func && read_data && request_body_size != size_func(read_data)) 
        {
            GByteArray *sha256_hash = NULL;
            GByteArray *md5_hash = NULL;

	    g_free(sha256_hash_hex);
	    g_free(md5_hash_b64);
	    g_free(md5_hash_hex);

	    sha256_hash_hex = NULL;
	    md5_hash_b64 = NULL;
	    md5_hash_hex = NULL;

            request_body_size = size_func(read_data);

            if (hdl->verbose)
                g_debug("[%p] %s: towrite buffer size=%lu",read_data, __FUNCTION__, request_body_size);

            if (hdl->s3_api == S3_API_AWS4)
                sha256_hash = s3_buffer_sha256_func(read_data);
            if (md5_func)
                md5_hash = md5_func(read_data);

            if (sha256_hash) {
                // sha256_hash_b64 = g_base64_encode(sha256_hash->data, sha256_hash->len);
                sha256_hash_hex = s3_hex_encode(sha256_hash);
                g_byte_array_free(sha256_hash, TRUE);
            }
            if (md5_hash) {
                md5_hash_b64 = g_base64_encode(md5_hash->data, md5_hash->len);
                md5_hash_hex = s3_hex_encode(md5_hash);
                g_byte_array_free(md5_hash, TRUE);
            }
        }

        /* set up the request */
        headers = authenticate_request(hdl, verb, bucket, key, subresource, query,
		    md5_hash_b64, sha256_hash_hex,
		    content_type, project_id, amzCopySource);

	/* add user header to headers */
	for (header = user_headers; header != NULL; header = header->next) {
	    headers = curl_slist_append(headers, header->data);
	}

	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4 )))
	    goto curl_error;

	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_NOSIGNAL, TRUE)))
	    goto curl_error;

        if (hdl->ca_info) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_CAINFO, hdl->ca_info)))
                goto curl_error;
        }

        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_VERBOSE, hdl->verbose)))
            goto curl_error;

        if (hdl->verbose) {
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_DEBUGFUNCTION,
                          s3_curl_debug_message)))
                goto curl_error;
        }
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_ERRORBUFFER,
                                          curl_error_buffer)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_NOPROGRESS, 1)))
            goto curl_error;
        // XXX: maybe better to disable to allow re-authorization
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_FOLLOWLOCATION, 1)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_UNRESTRICTED_AUTH, 1)))
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
#if LIBCURL_VERSION_NUM >= 0x071101 && defined(TCP_CONGESTION_ALG)
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_OPENSOCKETFUNCTION, s3_thread_linux_opensocket_func)))
            goto curl_error;
#endif
        /* Note: old libcurl needed this for "end of header" detection */
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_HEADERFUNCTION, s3_internal_header_func)))
            goto curl_error;
        /* Note: if set, CURLOPT_HEADERDATA seems to also be used for CURLOPT_WRITEDATA ? */
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_HEADERDATA, &int_writedata)))
            goto curl_error;
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_PROGRESSFUNCTION, progress_func)))
            goto curl_error;
	if (progress_func) {
	    if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_NOPROGRESS,0)))
		goto curl_error;
	}
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_PROGRESSDATA, progress_data)))
            goto curl_error;

        // set expected body size.. [may be zero]
        if (!chunked && curlopt_setsizeopt) {
            if ((curl_code=curl_easy_setopt(hdl->curl, curlopt_setsizeopt, (curl_off_t)request_body_size)))
                goto curl_error;
        } else if (chunked && curlopt_setsizeopt) {
            if ((curl_code=curl_easy_setopt(hdl->curl, curlopt_setsizeopt, (curl_off_t)0)))
                goto curl_error;
        }

        // set upload body buffer.. [may be zero]
        if (curlopt_setsizeopt)
        {
            xferbytes_t buffsize;

#if LIBCURL_VERSION_NUM >= 0x073e00
            buffsize = max(hdl->max_send_speed*5,CURL_MAX_READ_SIZE);
            if (request_body_size)
                buffsize = min(request_body_size,buffsize);
            buffsize = (buffsize |(0x1000-1)) + 1;

            if ((curl_code=curl_easy_setopt(hdl->curl, CURLOPT_UPLOAD_BUFFERSIZE, buffsize)))
                goto curl_error;
#endif
        }

#if LIBCURL_VERSION_NUM >= 0x073e00
        if (! curlopt_setsizeopt && write_data && size_func)
        {
	    // some overwrite of data found! need oversized buffer.
            objbytes_t buffsize = 3*s3_buffer_read_size_func(write_data)/2;

            // works even if buffsize starts as 0
            buffsize = max(buffsize,hdl->max_recv_speed*5);
            buffsize = max(buffsize,20*1024); // apparently the maximum..
            buffsize = min(buffsize,CURL_MAX_READ_SIZE);
            buffsize = (buffsize |(0x1000-1)) + 1; // buffsize goes up on 4k boundary

            // 512K
            if ((curl_code=curl_easy_setopt(hdl->curl, CURLOPT_BUFFERSIZE, buffsize)))
                goto curl_error;
        }
#endif

/* CURLOPT_MAX_{RECV,SEND}_SPEED_LARGE added in 7.15.5 */
#if LIBCURL_VERSION_NUM >= 0x070f05
	if (s3_curl_throttling_compat()) 
        {
	    if (hdl->max_send_speed)
		if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_MAX_SEND_SPEED_LARGE, (curl_off_t)hdl->max_send_speed)))
		    goto curl_error;

	    if (hdl->max_recv_speed)
		if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)hdl->max_recv_speed)))
		    goto curl_error;
        }
#endif

        // perform only one call [*mandatory*] to set httpreq
        if (curlopt_methodopt) {
            if ((curl_code = curl_easy_setopt(hdl->curl, curlopt_methodopt, 1)))
                goto curl_error;
        } else if (curlopt_customrequest) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_CUSTOMREQUEST, curlopt_customrequest)))
                goto curl_error;
        } else {
	    g_debug("perform_request: no method was selected");
            goto curl_error;
        }

        if (curlopt_setsizeopt) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READFUNCTION, read_func)))
                goto curl_error;
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READDATA, read_data)))
                goto curl_error;
        } else {
            /* Clear request_body options. */
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READFUNCTION, NULL)))
                goto curl_error;
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_READDATA, NULL)))
                goto curl_error;
        }

	if (hdl->proxy) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_PROXY,
                                              hdl->proxy)))
                goto curl_error;
	}

	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_FRESH_CONNECT,
		(long)(! hdl->reuse_connection || retry_after_close ? 1 : 0)))) {
	    goto curl_error;
	}
	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_FORBID_REUSE,
		(long)(! hdl->reuse_connection? 1 : 0)))) {
	    goto curl_error;
	}
	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_TIMEOUT, (long)hdl->timeout))) {
	    goto curl_error;
	}

        if (hdl->verbose) {
            g_debug("[%p] %s: try=#%d before %s of %s",
                           (curlopt_setsizeopt ? read_data : write_data),__FUNCTION__, retry_num, 
                           verb,url);
        }

        /* Perform the request */
        curl_code = curl_easy_perform(hdl->curl);

        /* interpret the response into hdl->last* */

    curl_error: /* (label for short-circuiting the curl_easy_perform call) */
        // assign current response code into hdl
        {
            CurlBuffer *s3buf = (CurlBuffer*) (curlopt_setsizeopt ? read_data : write_data);

            s3_result_t rsp_result = record_response(hdl, curl_code, curl_error_buffer,
                                                    int_writedata.bodytype, md5_hash_hex);

            if (hdl->last_response_code == 503) { // HTTP: Service Unavailable --> reset curl entirely
                if (hdl->verbose)
                    g_debug("[%p] %s: [503] try=#%d after %s of %s",
                             (curlopt_setsizeopt ? read_data : write_data),__FUNCTION__, retry_num, 
                             verb,url);

                s3_new_curl(hdl);
            }

            /* if we know we need to retry without a delay [delay implicit in event] */
            if (rsp_result == S3_RESULT_RETRY) {
                if (hdl->verbose)
                    g_debug("[%p] %s: RESP-RETRY try=#%d after %s of %s",
                          (curlopt_setsizeopt ? read_data : write_data),__FUNCTION__, retry_num, 
                          verb,url);
                continue; // no backoff yet
            }

            result = lookup_result(result_handling, hdl->last_response_code,
                                   hdl->last_s3_error_code, hdl->last_curl_code);

            if (result == S3_RESULT_RETRY_AUTH && hdl->s3_api == S3_API_OAUTH2 )
            {
                result = oauth2_get_access_token(hdl); // perform a recursive try
                if ( result != S3_RESULT_FAIL )
                    result = S3_RESULT_RETRY; // use the result now..
            }

	    // blocked by failures to upload
            if (result == S3_RESULT_RETRY && 
                   curl_code == CURLE_ABORTED_BY_CALLBACK && 
                   curlopt_setsizeopt && 
                   request_body_size && 
                   read_reset_func) 
            {
                if (hdl->verbose)
                    g_debug("[%p] %s: READ-ABORT-RETRY try=#%d (%#x/%#lx) [%#x:%#x] after %s of %s", 
                          read_data, __FUNCTION__, retry_num, 
                          s3buf->bytes_filled, s3buf->rdbytes_to_eod, s3buf->buffer_pos, s3buf->buffer_end,  
                          verb,url);
                continue; // retransmit with final limit
            }

            if (result == S3_RESULT_RETRY) {
                if (hdl->verbose)
                    g_debug("[%p] %s: LOOKUP-RETRY try=#%d after %s of %s",
                          (curlopt_setsizeopt ? read_data : write_data),__FUNCTION__, retry_num, 
                          verb,url);
                continue; // no backoff yet
            }

            /* break out of the while(1) unless we're retrying */
            if (result != S3_RESULT_RETRY_BACKOFF && rsp_result != S3_RESULT_RETRY_BACKOFF )
                break;

            ////////////////////////////////////////// perform backoff delay!
            if (hdl->verbose)
                g_debug("[%p] %s: BACKOFF-RETRY try=#%d after %s of %s",
                         (curlopt_setsizeopt ? read_data : write_data),__FUNCTION__, retry_num, 
                         verb,url);
            // an official retry requires a delay
            g_usleep(backoff);
            backoff <<= 1; // twice as big next time

            if (hdl->last_s3_error_code == S3_ERROR_RequestTimeout
                   && backoff > MAX_BACKOFF_DELAY_USEC && retry_after_close < 3)
            {
                backoff = BACKOFF_DELAY_START_USEC; // start over
                retry_after_close++;
                g_debug("Retry on a new connection");
            }

            // increment retry_num, decrement retries
        }
    } // for(backoff)

    if (write_data && ! int_writedata.write_data) {
        // response was an error that was not passed on ... so notify any readers
        s3_buffer_reset_eod_func(write_data, 0);  // assert simple zero bytes to read (assumes reader is waiting)
        s3_buffer_read_reset_func(write_data);  // lock/reset and then notify any readers
    }

    if (hdl->verbose)
        g_debug("[%p] %s: requested try=#%d after %s of %s",(curlopt_setsizeopt ? read_data : write_data),__FUNCTION__, retry_num, verb,url);

    if (backoff > MAX_BACKOFF_DELAY_USEC || retries <= 0 )
    {
        char *t = hdl->last_message;
        /* we're out of retries, so annotate hdl->last_message appropriately and bail
         * out. */
        hdl->last_message = g_strdup_printf("Too many retries; last message was '%s'", ( t ? : "<null>" ) );
        g_free(t);
        result = S3_RESULT_FAIL;
    }

    if (result != S3_RESULT_OK) {
        g_debug(_("%s [r=%d/%d] %s failed with %d/%s backoff=%lu retries=%d"), 
                verb, curl_code, result, url, 
                 hdl->last_response_code, s3_error_name_from_code(hdl->last_s3_error_code),
                backoff,
                retries);
    }

cleanup:
    g_free(url);
    curl_slist_free_all(headers);
    headers = NULL;
    g_free(md5_hash_b64);
    g_free(md5_hash_hex);
    g_free(sha256_hash_hex);

    hdl->last_num_retries = retry_num;
    if (!hdl->last_response_body) {
	hdl->last_response_body = s3_buffer_data_func(s3buf_dup); // must free it from there
	hdl->last_response_body_size = s3_buffer_lsize_func(s3buf_dup);
    }

    if (hdl->verbose)
        g_debug("[%p] %s: last_response_body: %p", (curlopt_setsizeopt ? read_data : write_data), 
		__FUNCTION__, hdl->last_response_body);

    if (s3buf_dup->buffer == hdl->last_response_body) {
	s3buf_dup->buffer = NULL;
	s3buf_dup->max_buffer_size = 0;
    }
    s3_buffer_destroy(s3buf_dup);

    g_mutex_unlock(&hdl->curl_mutex);
    return result;
}

static void
s3_internal_write_init(void * stream, S3Handle *hdl,
                 s3_write_func write_func, s3_reset_func write_reset_func,
                 gpointer write_data)
{
    S3InternalData *data = (S3InternalData *) stream;
    CurlBuffer *s3buf_dup = &data->dup_buffer;

    data->write_func = write_func;
    data->reset_func = write_reset_func;
    data->write_data = write_data;
    data->hdl = hdl;

    if (!data->write_func) {
        /* Curl will use fwrite() otherwise */
        data->write_func = s3_counter_write_func;
        data->reset_func = s3_counter_reset_func;
        data->write_data = NULL;
    }

    http_response_reset(hdl);

    s3_buffer_init(s3buf_dup, 0, hdl->verbose); // start with a one-write-only buffer (!!)
}

static void
s3_internal_write_reset_func(void * stream)
{
    S3InternalData *data = (S3InternalData *) stream;
    CurlBuffer *s3buf_dup = &data->dup_buffer;

    // remove alias immediately before freeing things
    if (s3buf_dup->buffer == data->hdl->last_response_body)
	data->hdl->last_response_body = NULL;

    // remove all last_* settings
    http_response_reset(data->hdl); // erase previous state...

    s3_buffer_write_reset_func(s3buf_dup);
    if (data->reset_func)
        data->reset_func(data->write_data);
}


/* a CURLOPT_WRITEFUNCTION to write data to a buffer. */
static size_t
s3_internal_write_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    /* must return == size*nmemb bytes for success */
    /* can return CURL_WRITEFUNC_PAUSE */
    /* can abort transfer if [0:size*nmemb) bytes are returned */

    S3InternalData *data = (S3InternalData *) stream;
    S3Handle *hdl = data->hdl;
    CurlBuffer *s3buf_dup = &data->dup_buffer;
    size_t dupavail = 0;

    size = size*nmemb; // just in case
    nmemb = 1; // just in case

    // process all headers together 
    // share the response-data buffer as response_body
    if (!hdl->last_response_body) 
    {
	curl_off_t content_len = MAX_ERROR_RESPONSE_LEN;

	// NOTE: only as an upper limit.. as transfer-encoding may contract [or expand?] actual bytes?
	(void) curl_easy_getinfo(hdl->curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &content_len);

        if (content_len < 0)
            content_len = MAX_ERROR_RESPONSE_LEN; // be gracious in case its needed

	if (hdl->transfer_encoding && hdl->verbose)
	   g_debug("%s: [%p] [%p] transfer encoding may overestimate length=%#lx",__FUNCTION__, &data->dup_buffer, data->write_data, content_len);

	// may set response_code.  will set data->bodytype
        s3_internal_header_func(NULL, 0, 0, data);

	// block any copying for a confirmed CONTENT_BINARY... else init buffer for other cases
	if (data->bodytype == CONTENT_BINARY)
	    s3_buffer_load_string(&data->dup_buffer, g_strdup("[binary/octet-string]"), hdl->verbose);
	else
	    s3_buffer_init(&data->dup_buffer, min(MAX_ERROR_RESPONSE_LEN,content_len), hdl->verbose);

	hdl->last_response_body = s3_buffer_data_func(s3buf_dup);
	hdl->last_response_body_size = s3_buffer_lsize_func(s3buf_dup);

	// cut off external buffer if its an error
	if (hdl->last_response_code < 300 && data->write_data) {
	    // normal data or response is coming... [may need parsing]
	    s3_buffer_reset_eod_func(data->write_data, content_len);
	}

	if (hdl->last_response_code >= 300 && data->write_data)
	    data->write_data = NULL; // no writes into external buffer
    }

    dupavail = s3buf_dup->max_buffer_size - hdl->last_response_body_size;

    if (dupavail) {
        // attempt to write if possible ... but write zero after that!
        (void) s3_buffer_write_func(ptr, min(size,dupavail), nmemb, s3buf_dup);
	hdl->last_response_body_size = s3_buffer_lsize_func(s3buf_dup);
    }

    // first written bytes?
    if (dupavail == s3buf_dup->max_buffer_size) { 
	// happens for UNKN types and for text/html as well...
	if (data->bodytype != CONTENT_BINARY  && data->bodytype != CONTENT_XML 
		&& g_strstr_len(hdl->last_response_body, min(30,hdl->last_response_body_size), "<?xml")) {
	    data->bodytype = CONTENT_XML; // treat response as an XML body for parsing
    	}

	// block copying of non-XML unknown types
	{
	    const char *desc = NULL;

	    switch (data->bodytype) {
		case CONTENT_UNKN:
		    desc = "[untyped/unencoded]";
		    break;
		case CONTENT_UNKN_CHUNKED:
		    desc = "[untyped/chunked]";
		    break;
		case CONTENT_UNKN_ENCODED: 
		    desc = "[untyped/encoded]";
		    break;
		case CONTENT_UNKN_CONTENT:
		    desc = "[unkntype/unencoded]";
		    break;
		default:
		    // accept for parsing
		    break;
	    }

	    // reset dup_buffer to full-buffer with a descriptive string
	    if (desc) {
	    	s3_buffer_destroy(s3buf_dup); // ignore parts alreayd received
	    	s3_buffer_load_string(s3buf_dup,g_strdup(desc),hdl->verbose);
		hdl->last_response_body = s3_buffer_data_func(s3buf_dup);
		hdl->last_response_body_size = s3_buffer_lsize_func(s3buf_dup);
	    }
	}
    }

    // the dup buffer may not read anything more...
    if (!data->write_data)
        return size; // use internal as only buffer

    // pass through to normal write...
    return data->write_func(ptr, size, nmemb, data->write_data);
}

static size_t
s3_internal_header_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    time_t remote_time_in_sec,local_time;
    char *header;
    regmatch_t pmatch[4];
    S3InternalData *data = (S3InternalData *) stream;
    S3Handle *hdl;
    long response_code;

    if (!data) 
        return 0;

    hdl = data->hdl;

    // non-permanent response code
    if (hdl->last_response_code < 200 && 
            curl_easy_getinfo(hdl->curl, CURLINFO_RESPONSE_CODE, &response_code) == CURLE_OK ) {
	hdl->last_response_code = response_code;
    }

    // after all headers are complete.. but response-body is empty
    if (!ptr && !hdl->last_response_body)
    {
	// basic guesses
        if (hdl->s3_api == S3_API_CASTOR) // always the same in legacy code
            data->bodytype = CONTENT_JSON;
	else if (hdl->content_type) { // content-types
	    if (g_str_has_prefix(hdl->content_type, "text/"))
		data->bodytype = CONTENT_TEXT;
	    else if (g_str_has_suffix(hdl->content_type, "/json"))
		data->bodytype = CONTENT_JSON;
	    else if (g_str_has_suffix(hdl->content_type, "/xml"))
		data->bodytype = CONTENT_XML;
	    else if (g_str_has_suffix(hdl->content_type,"/octet-stream"))
		data->bodytype = CONTENT_BINARY;
	    else
		data->bodytype = CONTENT_UNKN_CONTENT; // == -2 [check for xml]
	}
	else { // no-content-type provided
	    if (!hdl->transfer_encoding)
		data->bodytype = CONTENT_UNKN; // == 0  [check for xml]
	    else if (g_str_equal(hdl->transfer_encoding, "chunked"))
		data->bodytype = CONTENT_UNKN_CHUNKED; // == -3  [check for xml]
	    else
		data->bodytype = CONTENT_UNKN_ENCODED; // == -4  [check for xml]
	}
	return 0;
    } // if !ptr for call

    ///// ptr is non-NULL

    header = g_strndup((gchar *) ptr, (gsize) size*nmemb);

    if (header[strlen(header)-1] == '\n')
	header[strlen(header)-1] = '\0';
    if (header[strlen(header)-1] == '\r')
	header[strlen(header)-1] = '\0';

    //
    // can detect the 100 Continue first of all... but read_block_func probably the same
    //
    //if (hdl->verbose) 
    //    g_debug("%s: [%p] recvd %s",__FUNCTION__,data->write_data, header);

    if (!s3_regexec_wrap(&etag_regex, header, 2, pmatch, 0)) {
        g_free(hdl->last_etag); // in case
        hdl->last_etag = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_auth_token_regex, header, 2, pmatch, 0)) {
	g_free(hdl->x_auth_token);
	hdl->x_auth_token = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_subject_token_regex, header, 2, pmatch, 0)) {
	g_free(hdl->x_auth_token);
	hdl->x_auth_token = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_storage_url_regex, header, 2, pmatch, 0)) {
	g_free(hdl->x_storage_url);
	hdl->x_storage_url = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&content_type_regex, header, 2, pmatch, 0)) {
	g_free(hdl->content_type);
	hdl->content_type = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&content_range_regex, header, 4, pmatch, 0)) 
    {
        char *whole_len = find_regex_substring(header, pmatch[3]);
	// atoll(find_regex_substring(header, pmatch[1])); --- as sent in Range
	// atoll(find_regex_substring(header, pmatch[2])); --- as sent in Range
        if (whole_len)
            hdl->object_bytes = atoll(whole_len);
        g_free(whole_len);
    }

    if (!s3_regexec_wrap(&transfer_encoding_regex, header, 2, pmatch, 0)) {
	g_free(hdl->transfer_encoding);
	hdl->transfer_encoding = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_amz_expiration_regex, header, 2, pmatch, 0)) {
	g_free(hdl->x_amz_expiration);
	hdl->x_amz_expiration = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_amz_restore_regex, header, 2, pmatch, 0)) {
	g_free(hdl->x_amz_restore);
	hdl->x_amz_restore = find_regex_substring(header, pmatch[1]);
    }

    /* If date header is found */
    if (!s3_regexec_wrap(&date_sync_regex, header, 2, pmatch, 0))
    {
        char *date = find_regex_substring(header, pmatch[1]);

        /* Remote time is always in GMT: RFC 2616 */
        /* both curl_getdate and time operate in UTC, so no timezone math is necessary */
        if ( (remote_time_in_sec = curl_getdate(date, NULL)) < 0 )
        {
            g_debug("Error: Conversion of remote time to seconds failed.");
            hdl->time_offset_with_s3 = 0;
        }
        else
        {
            local_time = time(NULL);
            /* Offset time */
            hdl->time_offset_with_s3 = remote_time_in_sec - local_time;

	    if (hdl->verbose)
		g_debug("Time Offset (remote - local) :%ld",(long)hdl->time_offset_with_s3);
        }

        g_free(date);
    }

    g_free(header);
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
        {"^X-Auth-Token:[[:space:]]*([^ ]+)[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &x_auth_token_regex},
        {"^X-Subject-Token:[[:space:]]*([^ ]+)[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &x_subject_token_regex},
        {"^X-Storage-Url:[[:space:]]*([^ ]+)[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &x_storage_url_regex},
        {"^Content-Type:[[:space:]]*([^ ;]+).*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &content_type_regex},
        {"^Content-Range:[[:space:]]+bytes[[:space:]]+([0-9]+)-([0-9]+)/([0-9]+)$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &content_range_regex},
        {"^Transfer-Encoding:[[:space:]]*([^ ;]+).*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &transfer_encoding_regex},
        {"<Message>[[:space:]]*([^<]*)[[:space:]]*</Message>", REG_EXTENDED | REG_ICASE, &message_regex},

        {"^[a-z0-9]([-.a-z0-9]){1,61}[a-z0-9]$", REG_EXTENDED | REG_NOSUB, &bucket_regex},
	// add a case where _ is allowed but . is not
        {"^[a-z0-9]([-_a-z0-9]){1,61}[a-z0-9]$", REG_EXTENDED | REG_NOSUB, &bucket_regex_google},
	// sequential .. or -- or .- or -. are illegal for wasabi and for bucket+DNS
        {"[.\\-][.\\-]|^[0-9.]*$|-s3alias$", REG_EXTENDED | REG_NOSUB, &bucket_reject_regex},

        {"(/>)|(>([^<]*)</LocationConstraint>)", REG_EXTENDED | REG_ICASE, &location_con_regex},
        {"^Date:(.*)$",REG_EXTENDED | REG_ICASE | REG_NEWLINE, &date_sync_regex},
        {"\"access_token\" : \"([^\"]*)\",", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &access_token_regex},
	{"\"expires_in\" : (.*)", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &expires_in_regex},
        {"\"details\": \"([^\"]*)\",", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &details_regex},
        {"\"code\": (.*),", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &code_regex},
        {"^x-amz-expiration:[[:space:]]*([^ ]+)[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &x_amz_expiration_regex},
        {"^x-amz-restore:[[:space:]]*([^ ]+)[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &x_amz_restore_regex},
        {"\"message\": \"([^\"]*)\",", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &json_message_regex},
	{"<UploadId>[[:space:]]*([^<]*)[[:space:]]*</UploadId>", REG_EXTENDED | REG_ICASE, &uploadId_regex},
	{"<h1>[[:space:]]*([^<]*)[[:space:]]*</h1>", REG_EXTENDED | REG_ICASE, &html_error_name_regex},
	{"<p>[[:space:]]*([^<]*)[[:space:]]*</p>", REG_EXTENDED | REG_ICASE, &html_message_regex},
        {NULL, 0, NULL}
    };
    char regmessage[1024];
    int i;
    int reg_result;

    for (i = 0; regexes[i].str; i++) {
        reg_result = regcomp(regexes[i].regex, regexes[i].str, regexes[i].flags);
        if (reg_result != 0) {
            regerror(reg_result, regexes[i].regex, regmessage, sizeof(regmessage));
            g_error(_("Regex error: %s"), regmessage);
            return FALSE;
        }
    }
#else /* ! HAVE_REGEX_H */
  /* using PCRE via GLib */
  struct {const char * str; int flags; regex_t *regex;} regexes[] = {
        {"<Code>\\s*([^<]*)\\s*</Code>", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &error_name_regex},
        {"^ETag:\\s*\"([^\"]+)\"\\s*$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &etag_regex},
        {"^X-Auth-Token:\\s*([^ ]+)\\s*$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &x_auth_token_regex},
        {"^X-Subject-Token:\\s*([^ ]+)\\s*$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &x_subject_token_regex},
        {"^X-Storage-Url:\\s*([^ ]+)\\s*$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &x_storage_url_regex},
        {"^Content-Type:\\s*([^ ]+)\\s*$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &content_type_regex},
        {"^Transfer-Encoding:\\s*([^ ]+)\\s*$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &transfer_encoding_regex},
        {"<Message>\\s*([^<]*)\\s*</Message>", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &message_regex},

        {"^[a-z]([-.a-z0-9]){1,61}[a-z0-9]$", G_REGEX_OPTIMIZE | G_REGEX_CASELESSRE, &bucket_regex}, // includes wasabi
	// add a case where _ is allowed but . is not
        {"^[a-z0-9]([-_a-z0-9]){1,61}[a-z0-9]$", REG_EXTENDED | REG_NOSUB, &bucket_regex_google},
	// sequential .. or -- or .- or -. are illegal for wasabi and for bucket+DNS
        {"[.\\-][.\\-]|^[0-9.]*$|-s3alias$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &bucket_regex_reject},

        {"(/>)|(>([^<]*)</LocationConstraint>)", G_REGEX_CASELESS, &location_con_regex},
        {"^Date:(.*)$", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &date_sync_regex},
        {"\"access_token\" : \"([^\"]*)\"", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &access_token_regex},
        {"\"expires_n\" : (.*)", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &expires_in_regex},
        {"\"details\" : \"([^\"]*)\"", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &details_regex},
        {"\"code\": *\"([^\"]*)\"", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &code_regex},
        {"\"message\" : \"([^\"]*)\"", G_REGEX_OPTIMIZE | G_REGEX_CASELESS, &json_message_regex},
        {"(/>)|(>([^<]*)</UploadId>)", G_REGEX_CASELESS, &uploadId_regex},
        {"(/>)|(<h1>([^<]*)</h1>)", G_REGEX_CASELESS, &html_error_name_regex},
        {"(/>)|(<p>([^<]*)</p>)", G_REGEX_CASELESS, &html_message_regex},
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

#if (GLIB_MAJOR_VERSION > 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION >= 31))
# pragma GCC diagnostic push
# pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#endif
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
#if (GLIB_MAJOR_VERSION > 2 || (GLIB_MAJOR_VERSION == 2 && GLIB_MINOR_VERSION >= 31))
# pragma GCC diagnostic pop
#endif

gboolean
s3_curl_location_compat(void)
{
    curl_version_info_data *info;

    info = curl_version_info(CURLVERSION_NOW);
    return info->version_num > 0x070a02;
}

gboolean
s3_bucket_name_compat(const char *bucket)
{
    return !s3_regexec_wrap(&bucket_regex, bucket, 0, NULL, 0) && 
            s3_regexec_wrap(&bucket_reject_regex, bucket, 0, NULL, 0);
}


S3Handle *
s3_open(const char *access_key,
        const char *secret_key,
        const char *session_token,
        const char *swift_account_id,
        const char *swift_access_key,
        const char *host,
        const char *service_path,
        const gboolean use_subdomain,
        const char *user_token,
        const char *bucket_location,
        const char *storage_class,
        const char *ca_info,
        const char *server_side_encryption,
        const char *proxy,
        const S3_api s3_api,
        const char *username,
        const char *password,
        const char *tenant_id,
        const char *tenant_name,
        const char *project_name,
        const char *domain_name,
	const char *client_id,
	const char *client_secret,
	const char *refresh_token,
	const gboolean reuse_connection,
	const gboolean read_from_glacier G_GNUC_UNUSED,
	const long timeout,
        const char *reps,
        const char *reps_bucket)
{
    S3Handle *hdl;

    hdl = g_new0(S3Handle, 1);
    if (!hdl) goto error;

    hdl->verbose = FALSE;
    hdl->use_ssl = s3_curl_supports_ssl();
    hdl->reuse_connection = reuse_connection;
    hdl->timeout = timeout;

    switch (s3_api) {
      case S3_API_S3:
	g_assert(access_key);
	hdl->access_key = g_strdup(access_key);
	g_assert(secret_key);
	hdl->secret_key = g_strdup(secret_key);
	/* NULL is okay */
	hdl->session_token = g_strdup(session_token);
        break;

      case S3_API_AWS4:
	g_assert(access_key);
	hdl->access_key = g_strdup(access_key);
	g_assert(secret_key);
	hdl->secret_key = g_strdup(secret_key);
	/* NULL is okay */
	hdl->session_token = g_strdup(session_token);
        break;

      case S3_API_SWIFT_1:
	g_assert(swift_account_id);
	hdl->swift_account_id = g_strdup(swift_account_id);
	g_assert(swift_access_key);
	hdl->swift_access_key = g_strdup(swift_access_key);
        break;

      case S3_API_SWIFT_2:
	g_assert((username && password) || (access_key && secret_key));
	hdl->username = g_strdup(username);
	hdl->password = g_strdup(password);
	hdl->access_key = g_strdup(access_key);
	hdl->secret_key = g_strdup(secret_key);
	g_assert(tenant_id || tenant_name);
	hdl->tenant_id = g_strdup(tenant_id);
	hdl->tenant_name = g_strdup(tenant_name);
        break;

      case S3_API_SWIFT_3:
	g_assert((username && password) || (access_key && secret_key));
	hdl->username = g_strdup(username);
	hdl->password = g_strdup(password);
	hdl->access_key = g_strdup(access_key);
	hdl->secret_key = g_strdup(secret_key);
	hdl->tenant_id = g_strdup(tenant_id);
	hdl->tenant_name = g_strdup(tenant_name);
	if (project_name) {
	    hdl->project_name = g_strdup(project_name);
	} else {
	    hdl->project_name = g_strdup(username);
	}
	if (domain_name) {
	    hdl->domain_name = g_strdup(domain_name);
	} else {
	    hdl->domain_name = g_strdup("Default");
	}
        break;

      case S3_API_OAUTH2:
	hdl->client_id = g_strdup(client_id);
	hdl->client_secret = g_strdup(client_secret);
	hdl->refresh_token = g_strdup(refresh_token);
        break;

      case S3_API_CASTOR:
	hdl->username = g_strdup(username);
	hdl->password = g_strdup(password);
	hdl->tenant_name = g_strdup(tenant_name);
        hdl->reps = g_strdup(reps);
        hdl->reps_bucket = g_strdup(reps_bucket);
        break;

      default:
        break;
    }

    /* NULL is okay */
    hdl->user_token = g_strdup(user_token);

    /* NULL is okay */
    hdl->bucket_location = g_strdup(bucket_location);

    /* NULL is ok */
    hdl->storage_class = g_strdup(storage_class);

    /* NULL is ok */
    hdl->server_side_encryption = g_strdup(server_side_encryption);

    /* NULL is ok */
    hdl->proxy = g_strdup(proxy);

    /* NULL is okay */
    hdl->ca_info = g_strdup(ca_info);

    if (!is_non_empty_string(host))
	host = AMZ_TOP_DOMAIN;
    hdl->host = g_ascii_strdown(host, -1);

    {
        char *hwp = strrchr(hdl->host, ':');
        if (hwp) {
            *hwp = '\0';
            hdl->host_without_port = g_strdup(hdl->host);
            *hwp = ':';
        } else {
            hdl->host_without_port = g_strdup(hdl->host);
        }
    }

    hdl->use_google_subresources = g_str_has_suffix(hdl->host_without_port,GCP_TOP_DOMAIN);
    hdl->use_subdomain = use_subdomain ||
			 (g_str_equal(hdl->host_without_port, AMZ_TOP_DOMAIN) &&
			  is_non_empty_string(hdl->bucket_location));
    hdl->s3_api = s3_api;

    if (service_path) {
	if (strlen(service_path) == 0 ||
	    (strlen(service_path) == 1 && service_path[0] == '/')) {
	    hdl->service_path = NULL;
	} else if (service_path[0] != '/') {
	    hdl->service_path = g_strdup_printf("/%s", service_path);
	} else {
	    hdl->service_path = g_strdup(service_path);
	}
	if (hdl->service_path) {
	    /* remove trailling / */
	    size_t len = strlen(hdl->service_path) - 1;
	    if (hdl->service_path[len] == '/')
		hdl->service_path[len] = '\0';
	}
    } else {
	hdl->service_path = NULL;
    }

    s3_new_curl(hdl);
    if (!hdl->curl) goto error;
    return hdl;

error:
    s3_free(hdl);
    return NULL;
}

static void
s3_new_curl(
    S3Handle *hdl)
{
    if (hdl->curl)
	curl_easy_cleanup(hdl->curl);

    // first time init ONLY
    if (!hdl->curl) {
	g_mutex_init(&hdl->curl_mutex);
    }

    hdl->curl = curl_easy_init();
    if (!hdl->curl) {
        g_error("Failed to init new curl");
	abort();
    	return;
    }

    /* Set HTTP handling options for CAStor */
    if (hdl->s3_api == S3_API_CASTOR) {
#if LIBCURL_VERSION_NUM >= 0x071301
	curl_version_info_data *info;
	/* check the runtime version too */
	info = curl_version_info(CURLVERSION_NOW);
	if (info->version_num >= 0x071301) {
            curl_easy_setopt(hdl->curl, CURLOPT_FOLLOWLOCATION, 1);
            curl_easy_setopt(hdl->curl, CURLOPT_UNRESTRICTED_AUTH, 1);
            curl_easy_setopt(hdl->curl, CURLOPT_MAXREDIRS, 5);
            curl_easy_setopt(hdl->curl, CURLOPT_POSTREDIR, CURL_REDIR_POST_ALL);
            curl_easy_setopt(hdl->curl, CURLOPT_HTTP_VERSION,
					CURL_HTTP_VERSION_2TLS);
            if (hdl->username)
		 curl_easy_setopt(hdl->curl, CURLOPT_USERNAME, hdl->username);
            if (hdl->password)
		 curl_easy_setopt(hdl->curl, CURLOPT_PASSWORD, hdl->password);
            curl_easy_setopt(hdl->curl, CURLOPT_HTTPAUTH,
			     (CURLAUTH_BASIC | CURLAUTH_DIGEST));
	}
#endif
    }
}

gboolean
s3_open2(
    S3Handle *hdl)
{
    s3_result_t ret = S3_RESULT_OK;

    /* get the X-Storage-Url and X-Auth-Token */
    if (hdl->s3_api == S3_API_SWIFT_1) {
	ret = get_openstack_swift_api_v1_setting(hdl);
    } else if (hdl->s3_api == S3_API_SWIFT_2) {
	ret = get_openstack_swift_api_v2_setting(hdl);
    } else if (hdl->s3_api == S3_API_SWIFT_3) {
	ret = get_openstack_swift_api_v3_setting(hdl);
    }

    return ret == S3_RESULT_OK;
}

void
s3_free(S3Handle *hdl)
{
    if (!hdl) return;

    s3_reset(hdl);

    g_free(hdl->access_key);
    g_free(hdl->secret_key);
    g_free(hdl->swift_account_id);
    g_free(hdl->swift_access_key);
    g_free(hdl->ca_info);
    g_free(hdl->proxy);
    g_free(hdl->username);
    g_free(hdl->password);
    g_free(hdl->tenant_id);
    g_free(hdl->tenant_name);
    g_free(hdl->project_name);
    g_free(hdl->domain_name);
    g_free(hdl->client_id);
    g_free(hdl->client_secret);
    g_free(hdl->refresh_token);
    g_free(hdl->session_token);
    g_free(hdl->access_token);
    g_free(hdl->user_token);
    g_free(hdl->bucket_location);
    g_free(hdl->server_side_encryption);
    g_free(hdl->host);
    g_free(hdl->host_without_port);
    g_free(hdl->storage_class);
    g_free(hdl->service_path);

    if (hdl->curl) curl_easy_cleanup(hdl->curl);

    g_free(hdl);
}

static void
http_response_reset(S3Handle *hdl)
{
    if (!hdl) return;

    g_free(hdl->last_message);
    g_free(hdl->last_response_body); // response buffer from previous perform_request()

    g_free(hdl->content_type);
    g_free(hdl->last_etag);
    g_free(hdl->last_uploadId);
    g_free(hdl->transfer_encoding); 
    g_free(hdl->x_amz_expiration);
    g_free(hdl->x_amz_restore);
//    g_free(hdl->x_auth_token);      // needed until end of session
//    g_free(hdl->x_storage_url);     // needed until end of session

    // hdl->last_curl_code = CURLE_OK; // use record_request() to reset it
    hdl->last_response_code = 0;
    hdl->last_s3_error_code = S3_ERROR_None;
    hdl->last_num_retries = 0;
    hdl->last_response_body_size = 0;

    hdl->last_response_body = NULL;
    hdl->last_message = NULL;
    hdl->last_etag = NULL;
    hdl->last_uploadId = NULL;
    hdl->content_type = NULL;
    hdl->object_bytes = 0;
    hdl->transfer_encoding = NULL;
    hdl->x_amz_expiration = NULL;
    hdl->x_amz_restore = NULL;
    // hdl->x_auth_token = NULL;
    // hdl->x_storage_url = NULL;
}

void
s3_reset(S3Handle *hdl)
{
    if (!hdl) return;

    /* We don't call curl_easy_reset here, because doing that in curl
     * < 7.16 blanks the default CA certificate path, and there's no way
     * to get it back. */
#if LIBCURL_VERSION_NUM >= 0x071000
    if ( hdl->curl ) curl_easy_reset(hdl->curl);
#endif
    http_response_reset(hdl); // erase the rest of 
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
s3_set_max_send_speed(S3Handle *hdl, objbytes_t max_send_speed)
{
    if (!s3_curl_throttling_compat())
	return FALSE;

    hdl->max_send_speed = max_send_speed;

    return TRUE;
}

gboolean
s3_set_max_recv_speed(S3Handle *hdl, objbytes_t max_recv_speed)
{
    if (!s3_curl_throttling_compat())
	return FALSE;

    hdl->max_recv_speed = max_recv_speed;

    return TRUE;
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
 * @param buffer_end: the length of the data to upload
 * @returns: false if an error ocurred
 */
gboolean
s3_upload(S3Handle *hdl,
          const char *bucket,
          const char *key,
          const gboolean chunked,
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
        { 201,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };
    char *verb = "PUT";
    char *content_type = NULL;
    struct curl_slist *headers = NULL;

    g_assert(hdl != NULL);

    if (hdl->s3_api == S3_API_CASTOR) {
        verb = "POST";
	content_type = "application/x-amanda-backup-data";
    }

    if (chunked) {
	headers = curl_slist_append(headers, "Transfer-Encoding: chunked");
	size_func = NULL;
	md5_func = NULL;
    }

    hdl->server_side_encryption_header = TRUE;
    result = perform_request(hdl, verb, bucket, key, NULL,
		 NULL, content_type, NULL, headers,
                 read_func, reset_func, size_func, md5_func, read_data,
                 NULL, NULL, NULL, progress_func, progress_data,
                 result_handling, chunked);
    hdl->server_side_encryption_header = FALSE;

    curl_slist_free_all(headers);
    return result == S3_RESULT_OK;
}

/* Perform an upload. When this function returns, KEY and
 * BUFFER remain the responsibility of the caller.
 *
 * @param self: the s3 device
 * @param bucket: the bucket to which the upload should be made
 * @param key: the key to which the upload should be made
 * @param buffer: the data to be uploaded
 * @param buffer_end: the length of the data to upload
 * @returns: false if an error ocurred
 */
gboolean
s3_part_upload(S3Handle *hdl,
          const char *bucket,
          const char *key,
	  const char *uploadId,
	  int         partNumber,
	  char       **etag,
          s3_read_func read_func,
          s3_reset_func reset_func,
          s3_size_func size_func,
          s3_md5_func md5_func,
          gpointer read_data,
          s3_progress_func progress_func,
          gpointer progress_data)
{
    char *query[3] = { NULL };
    char **q = &query[0];
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    g_assert(hdl != NULL);

    // perform work later to reorder
    *q++ = g_strdup_printf("partNumber=%d", partNumber);
    *q++ = g_strdup_printf("uploadId=%s", uploadId);
    *q++ = NULL;

    result = perform_request(hdl, "PUT", bucket, key, NULL,
                 (const char **)query, NULL, NULL, NULL,
                 read_func, reset_func, size_func, md5_func, read_data,
                 NULL, NULL, NULL, progress_func, progress_data,
                 result_handling, FALSE);

    for(q = query; *q ; ++q) 
    	g_free(*q);
    if (etag) {
	*etag = hdl->last_etag;
	hdl->last_etag = NULL;
    }

    return result == S3_RESULT_OK;
}

/* Perform an upload. When this function returns, KEY and
 * BUFFER remain the responsibility of the caller.
 *
 * @param self: the s3 device
 * @param bucket: the bucket to which the upload should be made
 * @param key: the key to which the upload should be made
 * @param buffer: the data to be uploaded
 * @param buffer_end: the length of the data to upload
 * @returns: false if an error ocurred
 */
s3_result_t
s3_copypart_upload(S3Handle *hdl,
          const char *bucket,
          const char *key,
	  const char *uploadId,
	  int         partNumber,
	  char       **etag,
          const char *sourceKey)
{
    char *query[3] = { NULL };
    char **q = &query[0];
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };
    struct curl_slist *headers = NULL;
    char *buf;

    g_assert(hdl != NULL);

    // perform work later to reorder
    *q++ = g_strdup_printf("partNumber=%d", partNumber);
    *q++ = g_strdup_printf("uploadId=%s", uploadId);
    *q++ = NULL;

    buf = g_strdup_printf(AMAZON_COPY_SOURCE_HEADER ": %s/%s", bucket, sourceKey);
    headers = curl_slist_append(headers, buf);
    g_free(buf);

    result = perform_request(hdl, "PUT", bucket, key, NULL,
                 (const char **)query, NULL, NULL, headers,
                 NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);

    for(q = query; *q ; ++q) 
    	g_free(*q);
    if (etag) {
	*etag = hdl->last_etag;
	hdl->last_etag = NULL;
    }

    curl_slist_free_all(headers);
    return result;
}


char *
s3_initiate_multi_part_upload(
    S3Handle *hdl,
    const char *bucket,
    const char *key)
{
    s3_result_t result = S3_RESULT_FAIL;
    static const result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    hdl->server_side_encryption_header = TRUE;

    result = perform_request(hdl, "POST", bucket, key, "uploads",
                 NULL, "binary/octet-stream", NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);
    hdl->server_side_encryption_header = FALSE;

    if (result != S3_RESULT_OK)
	return NULL;

    return hdl->last_uploadId;
}

s3_result_t
s3_complete_multi_part_upload(
    S3Handle *hdl,
    const char *bucket,
    const char *key,
    const char *uploadId,
    s3_read_func read_func,
    s3_reset_func reset_func,
    s3_size_func size_func,
    s3_md5_func md5_func,
    gpointer read_data)
{
    char *query[2] = { NULL };
    char **q = &query[0];
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 				0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    *q++ = g_strdup_printf("uploadId=%s", uploadId);
    *q++ = NULL;

    result = perform_request(hdl, "POST", bucket, key, NULL,
		 (const char **)query, "application/xml", NULL, NULL,
		 read_func, reset_func, size_func, md5_func, read_data,
		 NULL, NULL, NULL, NULL, NULL,
		 result_handling, FALSE);

    g_free(query[0]);

    return result;
}

void
free_s3_object(
    gpointer data)
{
    s3_object *object = data;
    if (!data) return;
    g_free(object->key);
    g_free(object->mp_uploadId);
    g_free(object->prefix);
    g_free(object);
}

gboolean
s3_abort_multi_part_upload(
    S3Handle *hdl,
    const char *bucket,
    const char *key,
    const char *uploadId)
{
    char *query[2] = { NULL };
    char **q = &query[0];
    s3_result_t result = S3_RESULT_FAIL;

    static result_handling_t result_handling[] = {
        { 200,  0, 				0, S3_RESULT_OK },
        { 204,  0, 				0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    *q++ = g_strdup_printf("uploadId=%s", uploadId);
    *q++ = NULL;

    result = perform_request(hdl, "DELETE", bucket, key, NULL,
		 (const char **)query, "application/xml", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL,
		 result_handling, FALSE);

    g_free(query[0]);

    return (result == S3_RESULT_OK);


}

/* Private structure for our "thunk", which tracks where the user is in the list
 * of keys. */
struct list_keys_thunk {
    GSList *object_list; /* all pending filenames */
    s3_object *object;

    gboolean in_contents; /* look for "key" entities in here */
    gboolean in_common_prefixes; /* look for "prefix" entities in here */

    gboolean is_truncated;
    gchar *next_marker;
    objbytes_t size;

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
    if (g_ascii_strcasecmp(element_name, "contents") == 0 ||
	g_ascii_strcasecmp(element_name, "subdir") == 0 ||
	g_ascii_strcasecmp(element_name, "object") == 0 ||
	g_ascii_strcasecmp(element_name, "upload") == 0) {
        thunk->in_contents = 1;
	thunk->object = g_new0(s3_object, 1);
    } else if (g_ascii_strcasecmp(element_name, "commonprefixes") == 0) {
        thunk->in_common_prefixes = 1;
	thunk->object = g_new0(s3_object, 1);
    } else if (g_ascii_strcasecmp(element_name, "prefix") == 0 && thunk->in_common_prefixes) {
        thunk->want_text = 1;
    } else if ((g_ascii_strcasecmp(element_name, "key") == 0 ||
		g_ascii_strcasecmp(element_name, "name") == 0) &&
	       thunk->in_contents) {
        thunk->want_text = 1;
    } else if ((g_ascii_strcasecmp(element_name, "size") == 0 ||
		g_ascii_strcasecmp(element_name, "bytes") == 0) &&
	       thunk->in_contents) {
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "uploadid") == 0 && thunk->in_contents) {
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "istruncated")) {
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "nextmarker")) {
        thunk->want_text = 1;
    } else if (g_ascii_strcasecmp(element_name, "storageclass")) {
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

    if (g_ascii_strcasecmp(element_name, "contents") == 0 ||
	g_ascii_strcasecmp(element_name, "subdir") == 0 ||
	g_ascii_strcasecmp(element_name, "object") == 0 ||
	g_ascii_strcasecmp(element_name, "upload") == 0) {
        thunk->in_contents = 0;
	thunk->object_list = g_slist_prepend(thunk->object_list, thunk->object);
	thunk->object = NULL;
    } else if (g_ascii_strcasecmp(element_name, "commonprefixes") == 0) {
	thunk->object_list = g_slist_prepend(thunk->object_list, thunk->object);
	thunk->object = NULL;
        thunk->in_common_prefixes = 0;
    } else if ((g_ascii_strcasecmp(element_name, "key") == 0 ||
		g_ascii_strcasecmp(element_name, "name") == 0) &&
	       thunk->in_contents) {
	thunk->object->key = thunk->text;
	if (thunk->is_truncated) {
	    g_free(thunk->next_marker);
	    thunk->next_marker = g_strdup(thunk->text);
	}
        thunk->text = NULL;
    } else if ((g_ascii_strcasecmp(element_name, "size") == 0 ||
		g_ascii_strcasecmp(element_name, "bytes") == 0) &&
	       thunk->in_contents) {
	thunk->object->size = g_ascii_strtoull (thunk->text, NULL, 10);
        thunk->size += thunk->object->size;
	g_free(thunk->text);
        thunk->text = NULL;
    } else if (g_ascii_strcasecmp(element_name, "uploadid") == 0 && thunk->in_contents) {
	thunk->object->mp_uploadId = thunk->text;
        thunk->text = NULL;
    } else if (g_ascii_strcasecmp(element_name, "prefix") == 0 && thunk->in_common_prefixes) {
	thunk->object->prefix = thunk->text;
        thunk->text = NULL;
    } else if (g_ascii_strcasecmp(element_name, "istruncated") == 0) {
        if (thunk->text && g_ascii_strncasecmp(thunk->text, "false", 5) != 0)
            thunk->is_truncated = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "nextmarker") == 0) {
        g_free(thunk->next_marker);
        thunk->next_marker = thunk->text;
        thunk->text = NULL;
    } else if (g_ascii_strcasecmp(element_name, "storageclass") == 0) {
	if (g_str_equal(thunk->text, "STANDARD")) {
	    thunk->object->storage_class = S3_SC_STANDARD;
	} else if (g_str_equal(thunk->text, "STANDARD_IA")) {
	    thunk->object->storage_class = S3_SC_STANDARD_IA;
	} else if (g_str_equal(thunk->text, "REDUCED_REDUNDANCY")) {
	    thunk->object->storage_class = S3_SC_REDUCED_REDUNDANCY;
	} else if (g_str_equal(thunk->text, "GLACIER")) {
	    thunk->object->storage_class = S3_SC_GLACIER;
	} else if (g_str_equal(thunk->text, "DEEP_ARCHIVE")) {
            thunk->object->storage_class = S3_SC_DEEP_ARCHIVE;
        }
	g_free(thunk->text);
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
        g_free(thunk->text);
        thunk->text = g_strndup(text, text_len);
    }
}

/* Perform a fetch from S3; several fetches may be involved in a
 * single listing operation */
static s3_result_t
list_fetch(S3Handle *hdl,
           const char *bucket,
           const char *subresource,
           const char *prefix,
           const char *delimiter,
           const char *marker,
           const char *max_keys,
           CurlBuffer *buf)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200, 0, 0, S3_RESULT_OK },
        { 204, 0, 0, S3_RESULT_OK },
        { 404, S3_ERROR_NoSuchBucket, 0, S3_RESULT_OK }, // zero elements returned is okay
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };
    // six entries...
    char buff[1024]; // longest buffer for a header
    char *query[6];
    char **q = query;

    // NOTE: sorted into alphabetic order within perform_request()
    if ( delimiter ) {
        snprintf(buff,sizeof(buff),"%s~%s",      "delimiter",delimiter);
        *q++ = g_uri_escape_string(buff,NULL,FALSE);
    }
    if ( marker ) {
        snprintf(buff,sizeof(buff),"%s~%s",      "marker",marker);
        *q++ = g_uri_escape_string(buff,NULL,FALSE);
    }
    if ( prefix ) {
        snprintf(buff,sizeof(buff),"%s~%s",      "prefix",prefix);
        *q++ = g_uri_escape_string(buff,NULL,FALSE);
    }

    if ( max_keys ) {
        switch (hdl->s3_api) {
          case S3_API_SWIFT_1:
          case S3_API_SWIFT_2:
          case S3_API_SWIFT_3:
             snprintf(buff,sizeof(buff),"%s~%s",  "limit",max_keys);
             *q++ = g_uri_escape_string(buff,NULL,FALSE);
             *q++ = g_uri_escape_string("format~xml",NULL,FALSE);
             break;

          case S3_API_CASTOR:
             snprintf(buff,sizeof(buff),"%s~%s",  "size",max_keys);
             *q++ = g_uri_escape_string(buff,NULL,FALSE);
             *q++ = g_uri_escape_string("format~xml",NULL,FALSE);
             break;

          default:
             snprintf(buff,sizeof(buff),"%s~%s",  "max-keys",max_keys);
             *q++ = g_uri_escape_string(buff,NULL,FALSE);
             break;
        }
    }

    *q++ = NULL;

    // convert to no-escape char again
    for (q = query; *q != NULL; q++)
        *strchr(*q,'~') = '=';

    /* and perform the request on that URI */
    result = perform_request(hdl, "GET", bucket, NULL, subresource,
			     (const char **)query, NULL, NULL, NULL,
                             NULL, NULL, NULL, NULL, NULL,
                             S3_BUFFER_WRITE_FUNCS, buf, NULL, NULL,
                             result_handling, FALSE);

    for (q = query; *q != NULL; q++)
	curl_free(*q);

    return result;
}


gboolean
s3_list_keys(S3Handle *hdl,
              const char *bucket,
              const char *subresource,
              const char *prefix,
              const char *delimiter,
              size_t limit,
              GSList **list,
              objbytes_t *total_size)
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
     * StorageClass: STANDARD | REDUCED_REDUNDANCY | GLACIER
     */
    static const guint MAX_RESPONSE_LEN = 1000*2000;
    struct list_keys_thunk thunk;
    GMarkupParseContext *ctxt = NULL;
    static GMarkupParser parser = { list_start_element, list_end_element, list_text, NULL, NULL };
    GError *err = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    CurlBuffer buf_tmp_store = { 0 };
    CurlBuffer *s3buf_tmp = &buf_tmp_store;

    limit = ( limit ? : G_MAXINT );

    g_assert(!s3buf_tmp->buffer);

    s3_buffer_init(s3buf_tmp, MAX_RESPONSE_LEN, hdl->verbose);

    g_assert(list);
    *list = NULL;
    thunk.object_list = NULL;
    thunk.object = NULL;
    thunk.text = NULL;
    thunk.next_marker = NULL;
    thunk.size = 0;

    /* Loop until S3 has given us the entire picture */
    do {
        char *buff;
        xferbytes_t bufflen;
        char maxkeys[6];

        if (!limit)
            break; // ran out of interest

        snprintf(maxkeys,sizeof(maxkeys),"%lu",min(limit,1000));

        limit -= min(limit,1000);

        s3_buffer_reset_eod_func(s3buf_tmp,0);
        s3_buffer_write_reset_func(s3buf_tmp); // empty the buffer again
        /* get some data from S3 */
        result = list_fetch(hdl, bucket, subresource, prefix, delimiter, thunk.next_marker, maxkeys, s3buf_tmp);

        if (result != S3_RESULT_OK)
            goto cleanup;
	if (!s3_buffer_lsize_func(s3buf_tmp))
            goto cleanup; /* no body */

        /* run the parser over it */
        thunk.in_contents = FALSE;
        thunk.in_common_prefixes = FALSE;
        thunk.is_truncated = FALSE;
        g_free(thunk.next_marker);
	thunk.next_marker = NULL;
        thunk.want_text = FALSE;

        ctxt = g_markup_parse_context_new(&parser, 0, (gpointer)&thunk, NULL);

        buff = s3_buffer_data_func(s3buf_tmp);
        bufflen = s3_buffer_lsize_func(s3buf_tmp);

        if (!g_markup_parse_context_parse(ctxt, buff, bufflen, &err)) {
            g_free(hdl->last_message);
            hdl->last_message = g_strdup(err->message);
            result = S3_RESULT_FAIL;
            goto cleanup;
        }

        if (!g_markup_parse_context_end_parse(ctxt, &err)) {
            g_free(hdl->last_message);
            hdl->last_message = g_strdup(err->message);
            result = S3_RESULT_FAIL;
            goto cleanup;
        }

        g_markup_parse_context_free(ctxt);
        ctxt = NULL;
    } while (thunk.next_marker);

cleanup:
    if (err) g_error_free(err);
    g_free(thunk.text);
    g_free(thunk.next_marker);
    if (ctxt) g_markup_parse_context_free(ctxt);
    s3_buffer_destroy(s3buf_tmp);

    if (result != S3_RESULT_OK) {
        slist_free_full(thunk.object_list, free_s3_object);
        return FALSE;
    }

    *list = thunk.object_list;
    if(total_size) {
        *total_size = thunk.size;
    }
    return TRUE;
}

gboolean
s3_init_restore(S3Handle *hdl,
    const char *bucket,
    const char *key)
{
    CurlBuffer buf_tmp_store = { 0 };
    CurlBuffer *buf_tmp = &buf_tmp_store;
    char *str;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200, 0, 0, S3_RESULT_OK },
        { 202, 0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };

    str = g_strdup("<RestoreRequest xmlns=\"http://" AMZ_TOP_DOMAIN "/doc/2006-3-01\"> <Days>4</Days> </RestoreRequest>");
    s3_buffer_load_string(buf_tmp, str, hdl->verbose);

    result = perform_request(hdl, "POST", bucket, key, "restore", NULL,
	"application/xml", NULL, NULL,
	S3_BUFFER_READ_FUNCS, buf_tmp,
	NULL, NULL, NULL,
	NULL, NULL, result_handling, FALSE);

    s3_buffer_destroy(buf_tmp);
    return (result == S3_RESULT_OK);
}



/*Getting blob properties*/
s3_head_t *
s3_head(
    S3Handle *hdl,
    const char *bucket,
    const char *key)
{
    s3_result_t result = S3_RESULT_FAIL;
    s3_head_t *head;
    static result_handling_t result_handling[] = {
        { 200, 0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };

    amfree(hdl->x_amz_expiration);
    amfree(hdl->x_amz_restore);
    result = perform_request(hdl, "HEAD", bucket, key, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL, NULL,
                            result_handling, FALSE);

    if (result != S3_RESULT_OK) {
	return NULL;
    }
    head = g_new0(s3_head_t, 1);
    head->key = g_strdup(key);
    head->x_amz_expiration = g_strdup(hdl->x_amz_expiration);
    head->x_amz_restore = g_strdup(hdl->x_amz_restore);
    return head;
}

void
free_s3_head(
    s3_head_t *head)
{
    if (!head) return;
    g_free(head->key);
    g_free(head->x_amz_expiration);
    g_free(head->x_amz_restore);
    g_free(head);
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
        { 200, 0,                            0, S3_RESULT_OK },
        { 403, S3_ERROR_InvalidObjectState,  0, S3_RESULT_RETRY_BACKOFF },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0,                            0, /* default: */ S3_RESULT_FAIL  }
        };

    g_assert(hdl != NULL);
    g_assert(write_func != NULL);

    result = perform_request(hdl, "GET", bucket, key, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL,
        write_func, reset_func, write_data, progress_func, progress_data,
        result_handling, FALSE);

    return result == S3_RESULT_OK;
}

gboolean
s3_read_range(S3Handle *hdl,
        const char *bucket,
        const char *key,
	const objbytes_t range_begin,
	const objbytes_t range_end,
        s3_write_func write_func,
        s3_reset_func reset_func,
        gpointer write_data,
        s3_progress_func progress_func,
        gpointer progress_data,
        objbytes_t *object_bytes)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200, 0,                            0, S3_RESULT_OK },
        { 206, 0,                            0, S3_RESULT_OK },
        { 403, S3_ERROR_InvalidObjectState,  0, S3_RESULT_RETRY_BACKOFF },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0,                            0, /* default: */ S3_RESULT_FAIL  }
        };
    struct curl_slist *headers = NULL;
    char *buf;

    g_assert(hdl != NULL);
    g_assert(write_func != NULL);

    buf = g_strdup_printf("Range: bytes=%llu-%llu",
			  (long long unsigned) range_begin,
			  (long long unsigned) range_end);
    headers = curl_slist_append(headers, buf);
    g_free(buf);

    result = perform_request(hdl, "GET", bucket, key, NULL,
        NULL, NULL, NULL, headers,
        NULL, NULL, NULL, NULL, NULL,
        write_func, reset_func, write_data, progress_func, progress_data,
        result_handling, FALSE);

    if (object_bytes && hdl->object_bytes)
        *object_bytes = hdl->object_bytes;

    curl_slist_free_all(headers);
    return result == S3_RESULT_OK;
}

s3_result_t
s3_delete(S3Handle *hdl,
          const char *bucket,
          const char *key)
{
    s3_result_t result = S3_RESULT_FAIL;

    static result_handling_t result_handling[] = { // delete function responses
        { 200,  0,                     0, S3_RESULT_OK },
        // { 202,  0,                     0, S3_RESULT_OK },
        { 204,  0,                     0, S3_RESULT_OK },
        { 404,  0,                     0, S3_RESULT_OK },
        { 0,    S3_ERROR_NoSuchBucket, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 409,  0,                     0, S3_RESULT_OK },
        { 0,    0,                     0, /* default: */ S3_RESULT_FAIL  }
        }; // delete function responses

    g_assert(hdl != NULL);

    result = perform_request(hdl, "DELETE", bucket, key, NULL,
                 NULL, "application/xml", NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL,  // delete needs no write
		 NULL, NULL, NULL, NULL, NULL,  // delete needs no read
                 result_handling, FALSE);

    return result;
}

gboolean
s3_make_bucket(S3Handle *hdl,
               const char *bucket,
	       const char *project_id)
{
    char *body = NULL;
    char *verb = "PUT";
    char *content_type = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = { // make bucket responses
        { 200,  0,                    0, S3_RESULT_OK },
        { 201,  0,                    0, S3_RESULT_OK },
        { 202,  0,                    0, S3_RESULT_OK },
        { 204,  0,                    0, S3_RESULT_OK },
        { 409, S3_ERROR_BucketAlreadyOwnedByYou, 0, S3_RESULT_OK }, // no read to remake it
        { 404, S3_ERROR_NoSuchBucket, 0, S3_RESULT_RETRY },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        }; // make bucket responses
    regmatch_t pmatch[4];
    GString *CreateBucketConfiguration;
    gboolean add_create = FALSE;
    char *loc_end_open = NULL;
    char *loc_content = NULL;
    gboolean location;
    gboolean wild_location;

    g_assert(hdl != NULL);

    location = is_non_empty_string(hdl->bucket_location);
    wild_location = location && g_str_equal(AMAZON_WILDCARD_LOCATION, hdl->bucket_location);

    /*
    Bucket names must be between 3 and 63 characters long.
    Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
    Bucket names must begin and end with a letter or number.
    Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
    Bucket names must not start with the prefix xn--.
    Bucket names must not end with the suffix -s3alias. This suffix
        is reserved for access point alias names. For more information,
        see Using a bucket-style alias for your access point.
    Bucket names must be unique within a partition. A partition is
        a grouping of Regions. AWS currently has three partitions: aws
        (Standard Regions), aws-cn (China Regions), and aws-us-gov (AWS
        GovCloud [US] Regions).
    Buckets used with Amazon S3 Transfer Acceleration can't have dots
        (.) in their names. For more information about Transfer Acceleration,
        see Configuring fast, secure file transfers using Amazon S3 Transfer
        Acceleration.
    */

    if (!s3_bucket_name_compat(bucket)) {
        hdl->last_message = g_strdup_printf(_(
            "Bucket name constraints for %s, but the bucket name (%s) is not usable."), 
               S3_bucket_name[hdl->s3_api], bucket);
        return FALSE;
    }

    CreateBucketConfiguration = g_string_new("<CreateBucketConfiguration");
    if (g_str_equal(hdl->host, "gss.iijgio.com")) {
	g_string_append(CreateBucketConfiguration,
			" xmlns=\"http://acs.iijgio.com/doc/2006-03-01/\"");
    }
    g_string_append_c(CreateBucketConfiguration, '>');
    if (location && !g_str_equal(hdl->bucket_location, "us-east-1") && !wild_location) {
        g_string_append_printf(CreateBucketConfiguration,
                        "<LocationConstraint>%s</LocationConstraint>",
                        hdl->bucket_location);
        add_create = TRUE;
    }

    if (hdl->s3_api == S3_API_OAUTH2 && hdl->storage_class) {
	g_string_append_printf(CreateBucketConfiguration,
			       "<StorageClass>%s</StorageClass>",
			       hdl->storage_class);
	add_create = TRUE;
    }

    g_string_append(CreateBucketConfiguration, "</CreateBucketConfiguration>");

    if (hdl->s3_api == S3_API_CASTOR) {
        verb = "POST";
        content_type = "application/castorcontext";
    }

    if (add_create)
    {
        CurlBuffer buf_tmp_store = { 0 }; // to load once only
        CurlBuffer *buf_tmp = &buf_tmp_store;

        s3_buffer_load_string(buf_tmp, g_string_free(CreateBucketConfiguration, FALSE), hdl->verbose);
        result = perform_request(hdl, verb, bucket, NULL, NULL,
                     NULL, content_type, project_id, NULL,
                     S3_BUFFER_READ_FUNCS, buf_tmp,
                     NULL, NULL, NULL, NULL, NULL,
                     result_handling, FALSE);
	s3_buffer_destroy(buf_tmp);
    } else {
	g_string_free(CreateBucketConfiguration, TRUE);
        result = perform_request(hdl, verb, bucket, NULL, NULL,
                     NULL, content_type, project_id, NULL,
                     NULL, NULL, NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL, NULL,
                     result_handling, FALSE);
    }

   if (result != S3_RESULT_OK && hdl->last_s3_error_code != S3_ERROR_BucketAlreadyOwnedByYou)
      goto cleanup;

    /* verify the that the location constraint on the existing bucket matches
     * the one that's configured.
     */
    if (location) {
        result = perform_request(hdl, "GET", bucket, NULL, "location",
                                 NULL, NULL, NULL, NULL,
                                 NULL, NULL, NULL, NULL, NULL,
                                 NULL, NULL, NULL, NULL, NULL,
                                 result_handling, FALSE);
    } else {
        result = perform_request(hdl, "GET", bucket, NULL, NULL,
                                 NULL, NULL, NULL, NULL,
                                 NULL, NULL, NULL, NULL, NULL,
                                 NULL, NULL, NULL, NULL, NULL,
                                 result_handling, FALSE);
    }

    // return an error?
    if (result != S3_RESULT_OK )
        goto cleanup;                                                //// RETURN S3_RESULT_FAIL (or similar)

    // non-empty bucket_location string?
    if (location)
    {
        // bucket_location string needs to be handled

        /* return to the default state of failure */
        result = S3_RESULT_FAIL;

        /* use strndup to get a null-terminated string */
        body = g_strndup(hdl->last_response_body, hdl->last_response_body_size);
        if (!body) {
            hdl->last_message = g_strdup(_("No body received for location request"));
            goto cleanup;                                                //// RETURN S3_RESULT_FAIL
        }

        if (!*body) {
            hdl->last_message = g_strdup(_("Empty body received for location request"));
            goto cleanup;                                                //// RETURN S3_RESULT_FAIL
        }

        // try to match for location info

        if (s3_regexec_wrap(&location_con_regex, body, 4, pmatch, 0)) {
            hdl->last_message = g_strdup_printf(_("Unexpected location response from %s"), S3_name[hdl->s3_api]);
            goto cleanup;                                                //// RETURN S3_RESULT_FAIL
        }

        loc_end_open = find_regex_substring(body, pmatch[1]);

        /* The case of an empty string is special because XML allows "self-closing" tags */
        if (wild_location && !g_str_has_prefix(loc_end_open, "/")) {
            hdl->last_message = g_strdup(_("A wildcard location constraint is "
                "configured, but the bucket has a non-empty location constraint"));
            goto cleanup;                                                //// RETURN S3_RESULT_FAIL
        }

        loc_content = find_regex_substring(body, pmatch[3]);

        if (wild_location && !g_str_equal(loc_content,"")) {
            hdl->last_message = g_strdup_printf("The location constraint configured (%s) "
                "does not match the constraint currently on the bucket (%s)", hdl->bucket_location, loc_content);
            goto cleanup;                                                //// RETURN S3_RESULT_FAIL
        }

        // NOTE: added nul-string as an okay "unspecified" location
        if (!wild_location && !g_str_equal(loc_content,"") && !g_str_has_prefix(loc_content, hdl->bucket_location)) {
            hdl->last_message = g_strdup_printf("The location constraint configured (%s) "
                "does not match the constraint currently on the bucket (%s)", hdl->bucket_location, loc_content);
            goto cleanup;                                                //// RETURN S3_RESULT_FAIL
        }

        result = S3_RESULT_OK;
    }

cleanup:
    g_free(body);
    g_free(loc_end_open);
    g_free(loc_content);

    return result == S3_RESULT_OK;
}

static s3_result_t
oauth2_get_access_token(S3Handle *hdl)
{
    GString *query;
    CurlBuffer buf_tmp_store = { 0 };
    CurlBuffer *buf_tmp = &buf_tmp_store;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = { // get access token responses
        { 200,  0,                    0, S3_RESULT_OK },
        { 204,  0,                    0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        }; // get access token responses
    char *body;
    regmatch_t pmatch[2];

    g_assert(hdl != NULL);

    query = g_string_new(NULL);
    g_string_append(query, "client_id=");
    g_string_append(query, hdl->client_id);
    g_string_append(query, "&client_secret=");
    g_string_append(query, hdl->client_secret);
    g_string_append(query, "&refresh_token=");
    g_string_append(query, hdl->refresh_token);
    g_string_append(query, "&grant_type=refresh_token");

    s3_buffer_load_string(buf_tmp, g_strdup(query->str), hdl->verbose);

    hdl->x_storage_url = "https://accounts.google.com/o/oauth2/token";
    hdl->getting_oauth2_access_token = TRUE;

    result = perform_request(hdl, "POST", NULL, NULL, NULL,
                             NULL, "application/x-www-form-urlencoded", NULL, NULL,
                             S3_BUFFER_READ_FUNCS, buf_tmp,
                             NULL, NULL, NULL, NULL, NULL,
			     result_handling, FALSE);
    hdl->x_storage_url = NULL;
    hdl->getting_oauth2_access_token = 0;

    s3_buffer_destroy(buf_tmp);

    /* use strndup to get a null-terminated string */
    body = g_strndup(hdl->last_response_body, hdl->last_response_body_size);
    if (!body) {
        hdl->last_message = g_strdup(_("No body received for location request"));
        goto cleanup;
    }

    if (!*body) {
        hdl->last_message = g_strdup(_("Empty body received for location request"));
        goto cleanup;
    }

    if (!s3_regexec_wrap(&access_token_regex, body, 2, pmatch, 0)) {
        hdl->access_token = find_regex_substring(body, pmatch[1]);
        hdl->x_auth_token = g_strdup(hdl->access_token);
    }

    if (!s3_regexec_wrap(&expires_in_regex, body, 2, pmatch, 0)) {
        char *expires_in = find_regex_substring(body, pmatch[1]);
	hdl->expires = time(NULL) + atoi(expires_in) - 600;
	g_free(expires_in);
    }

cleanup:
    g_free(body);
    return result;
}

gboolean
s3_is_bucket_exists(S3Handle *hdl,
		    const char *bucket,
		    const char *prefix,
		    const char *project_id)
{
    s3_result_t result = S3_RESULT_FAIL;
    char *query[3] = { NULL };
    char **q = query;
    static result_handling_t result_handling[] = {
        { 200,  0,                    0, S3_RESULT_OK },
        { 204,  0,                    0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        };
    char *q_prefix = NULL;

    switch( hdl->s3_api) {
    	case S3_API_SWIFT_1:
    	case S3_API_SWIFT_2:
    	case S3_API_SWIFT_3: 
	    *q++ = g_strdup("limit=1");
	    break;

    	case S3_API_CASTOR: 
	    *q++ = g_strdup("format=xml");
	    *q++ = g_strdup("size=0");
	    break;

	default:
	    *q++ = g_strdup("max-keys=1");
	    if (!prefix) break; // continue only if needed
	    q_prefix = g_uri_escape_string(prefix, NULL, 0);
	    *q++ = g_strdup_printf("prefix=%s", q_prefix);
	    g_free(q_prefix);
	    break;
    }
    *q++ = NULL;

    result = perform_request(hdl, "GET", bucket, NULL, NULL,
			     (const char **)query, NULL, project_id, NULL,
                             NULL, NULL, NULL, NULL, NULL,
                             NULL, NULL, NULL, NULL, NULL,
                             result_handling, FALSE);

    for (q = query; *q ; q++)
	g_free(*q);
    return result == S3_RESULT_OK;
}

gboolean
s3_delete_bucket(S3Handle *hdl,
                 const char *bucket)
{
    return s3_delete(hdl, bucket, NULL);
}

/* Private structure for our "thunk", which tracks where the user is in the list
 * of keys. */

typedef struct lifecycle_thunk {
    GSList *lifecycle; /* all rules */
    lifecycle_rule *rule;
    lifecycle_action *action;

    gboolean in_LifecycleConfiguration;
    gboolean in_Rule;
    gboolean in_ID;
    gboolean in_Filter;
    gboolean in_Prefix;
    gboolean in_Status;
    gboolean in_Transition;
    gboolean in_Expiration;
    gboolean in_Days;
    gboolean in_Date;
    gboolean in_StorageClass;

    gboolean want_text;

    gchar *text;
    gsize text_len;

    gchar *error;
} lifecycle_thunk;

static void
lifecycle_start_element(GMarkupParseContext *context G_GNUC_UNUSED,
                        const gchar *element_name,
                        const gchar **attribute_names G_GNUC_UNUSED,
                        const gchar **attribute_values G_GNUC_UNUSED,
                        gpointer user_data,
                        GError **error G_GNUC_UNUSED);

static void
lifecycle_end_element(GMarkupParseContext *context G_GNUC_UNUSED,
                      const gchar *element_name,
                      gpointer user_data,
                      GError **error G_GNUC_UNUSED);

static void
lifecycle_text(GMarkupParseContext *context G_GNUC_UNUSED,
               const gchar *text,
               gsize text_len,
               gpointer user_data,
               GError **error G_GNUC_UNUSED);

void
free_lifecycle_rule(
    gpointer data)
{
    lifecycle_rule *rule = (lifecycle_rule *)data;

    g_free(rule->id);
    g_free(rule->filter);
    g_free(rule->prefix);
    g_free(rule->status);
    if (rule->transition) {
	g_free(rule->transition->date);
	g_free(rule->transition->storage_class);
	g_free(rule->transition);
    }
    if (rule->expiration) {
	g_free(rule->expiration->date);
	g_free(rule->expiration->storage_class);
	g_free(rule->expiration);
    }
    g_free(rule);
}

void
free_lifecycle(
    GSList *lifecycle)
{
    slist_free_full(lifecycle, free_lifecycle_rule);
}

gboolean
s3_get_lifecycle(S3Handle *hdl,
    const char *bucket,
    GSList **lifecycle)
{
    s3_result_t result = S3_RESULT_FAIL;
    CurlBuffer buf_tmp_store = { 0 }; // to read max of 100000
    CurlBuffer *buf_tmp = &buf_tmp_store; // to read max of 100000
    struct lifecycle_thunk thunk;
    GMarkupParseContext *ctxt = NULL;
    static GMarkupParser parser = { lifecycle_start_element, lifecycle_end_element, lifecycle_text, NULL, NULL };
    GError *err = NULL;
    static result_handling_t result_handling[] = {
        { 200,  0,                    0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        };

    thunk.lifecycle = NULL;
    thunk.rule = NULL;
    thunk.action = NULL;

    thunk.in_LifecycleConfiguration = FALSE;
    thunk.in_Rule = FALSE;
    thunk.in_ID = FALSE;
    thunk.in_Filter = FALSE;
    thunk.in_Prefix = FALSE;
    thunk.in_Status = FALSE;
    thunk.in_Transition = FALSE;
    thunk.in_Expiration = FALSE;
    thunk.in_Days = FALSE;
    thunk.in_Date = FALSE;
    thunk.in_StorageClass = FALSE;
    thunk.want_text = FALSE;
    thunk.text = NULL;
    thunk.text_len = 0;
    thunk.error = NULL;

    s3_buffer_init(buf_tmp, 100000, hdl->verbose); // XXX: buffer size?
    result = perform_request(hdl, "GET", bucket, NULL, "lifecycle", NULL,
			     NULL, NULL, NULL,
                             NULL, NULL, NULL, NULL, NULL,
			     S3_BUFFER_WRITE_FUNCS, buf_tmp,
                             NULL, NULL, result_handling, FALSE);

    if (result == S3_RESULT_FAIL &&
	hdl->last_response_code == 404 &&
	hdl->last_s3_error_code == S3_ERROR_NoSuchLifecycleConfiguration) {
	result = S3_RESULT_OK;
	return TRUE;
    }
    if (result != S3_RESULT_OK) goto cleanup;
    if (!s3_buffer_lsize_func(buf_tmp)) goto cleanup;

    /* run the parser over it */

    ctxt = g_markup_parse_context_new(&parser, 0, (gpointer)&thunk, NULL);

    if (!g_markup_parse_context_parse(ctxt, s3_buffer_data_func(buf_tmp), s3_buffer_lsize_func(buf_tmp), &err)) {
	g_free(hdl->last_message);
	hdl->last_message = g_strdup(err->message);
	result = S3_RESULT_FAIL;
	goto cleanup;
    }

    if (!g_markup_parse_context_end_parse(ctxt, &err)) {
	g_free(hdl->last_message);
	hdl->last_message = g_strdup(err->message);
	result = S3_RESULT_FAIL;
	goto cleanup;
    }

    g_markup_parse_context_free(ctxt);
    ctxt = NULL;

    if (thunk.error) {
	g_free(hdl->last_message);
	hdl->last_message = thunk.error;
	thunk.error = NULL;
	result = S3_RESULT_FAIL;
	goto cleanup;
    }

cleanup:
    if (err) g_error_free(err);
    g_free(thunk.text);
    if (ctxt) g_markup_parse_context_free(ctxt);
    s3_buffer_destroy(buf_tmp);

    if (result == S3_RESULT_OK) {
	*lifecycle = thunk.lifecycle;
    } else {
	free_lifecycle(thunk.lifecycle);
    }
    return result == S3_RESULT_OK;
}

gboolean
s3_put_lifecycle(S3Handle *hdl,
                const char *bucket,
                GSList *lifecycle)
{
    s3_result_t result = S3_RESULT_FAIL;
    CurlBuffer buf_tmp_store = { 0 }; // to load once only
    CurlBuffer *buf_tmp = &buf_tmp_store; // to load once only
    GString *body = g_string_new("<LifecycleConfiguration>");
    GSList *life;
    lifecycle_rule *rule;
    static result_handling_t result_handling[] = {
        { 200,  0,                    0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        };

    for (life = lifecycle; life != NULL; life = life->next) {
	rule = (lifecycle_rule *)life->data;
	g_string_append_printf(body,
		"<Rule><ID>%s</ID><Filter><Prefix>%s</Prefix></Filter><Status>%s</Status>",
		rule->id, rule->prefix, rule->status);
	if (rule->transition) {
	    g_string_append(body, "<Transition>");
	    if (rule->transition->date) {
		g_string_append_printf(body, "<Date>%s</Date>",
				       rule->transition->date);
	    } else {
		g_string_append_printf(body, "<Days>%u</Days>",
				       rule->transition->days);
	    }
	    g_string_append_printf(body,
			"<StorageClass>%s</StorageClass></Transition>",
			rule->transition->storage_class);
	}
	if (rule->expiration) {
	    g_string_append(body, "<Expiration>");
	    if (rule->expiration->date) {
		g_string_append_printf(body, "<Date>%s</Date>",
				       rule->expiration->date);
	    } else {
		g_string_append_printf(body, "<Days>%u</Days>",
				       rule->expiration->days);
	    }
	    g_string_append(body, "</Expiration>");
	}
	g_string_append_printf(body, "</Rule>");
    }
    g_string_append(body, "</LifecycleConfiguration>");

    s3_buffer_load_string(buf_tmp, g_string_free(body, FALSE), hdl->verbose);

    s3_verbose(hdl, 1);
    result = perform_request(hdl, "PUT", bucket, NULL, "lifecycle", 
                             NULL, "application/xml", NULL, NULL,
                             S3_BUFFER_READ_FUNCS, buf_tmp,
			     NULL, NULL, NULL, NULL, NULL, 
                             result_handling, FALSE);
    s3_buffer_destroy(buf_tmp);

    return result == S3_RESULT_OK;
}

char *
am_strrmspace(
    char *str)
{

    char *s, *t;

    for(s=str,t=str; *s != '\0'; s++) {
	if (*s != ' ') {
	    *t++ = *s;
	}
    }
    *t = '\0';
    return t;
}

/* Functions for a SAX parser to parse the XML from Amazon */

static void
lifecycle_start_element(GMarkupParseContext *context G_GNUC_UNUSED,
                        const gchar *element_name,
                        const gchar **attribute_names G_GNUC_UNUSED,
                        const gchar **attribute_values G_GNUC_UNUSED,
                        gpointer user_data,
                        GError **error G_GNUC_UNUSED)
{
    struct lifecycle_thunk *thunk = (lifecycle_thunk *)user_data;

    thunk->want_text = FALSE;
    if (g_ascii_strcasecmp(element_name, "lifecycleconfiguration") == 0) {
	thunk->in_LifecycleConfiguration = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "rule") == 0) {
	thunk->in_Rule = TRUE;
	thunk->rule = g_new0(lifecycle_rule, 1);
    } else if (g_ascii_strcasecmp(element_name, "id") == 0) {
	thunk->in_ID = TRUE;
        thunk->want_text = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "filter") == 0) {
        thunk->in_Filter = TRUE;
        thunk->want_text = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "prefix") == 0) {
	thunk->in_Prefix = TRUE;
        thunk->want_text = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "status") == 0) {
	thunk->in_Status = TRUE;
        thunk->want_text = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "transition") == 0) {
	thunk->in_Transition = TRUE;
	thunk->action = g_new0(lifecycle_action, 1);
    } else if (g_ascii_strcasecmp(element_name, "expiration") == 0) {
	thunk->in_Expiration = TRUE;
	thunk->action = g_new0(lifecycle_action, 1);
    } else if (g_ascii_strcasecmp(element_name, "days") == 0) {
	thunk->in_Days = TRUE;
        thunk->want_text = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "date") == 0) {
	thunk->in_Date = TRUE;
        thunk->want_text = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "storageclass") == 0) {
	thunk->in_StorageClass = TRUE;
        thunk->want_text = TRUE;
    } else {
	g_free(thunk->error);
	thunk->error = g_strdup("Unknown element name in lifecycle get");
    }
}

static void
lifecycle_end_element(GMarkupParseContext *context G_GNUC_UNUSED,
                      const gchar *element_name,
                      gpointer user_data,
                      GError **error G_GNUC_UNUSED)
{
    lifecycle_thunk *thunk = (lifecycle_thunk *)user_data;

    if (g_ascii_strcasecmp(element_name, "lifecycleconfiguration") == 0) {
	thunk->in_LifecycleConfiguration = FALSE;
    } else if (g_ascii_strcasecmp(element_name, "rule") == 0) {
	thunk->in_Rule = FALSE;
	thunk->lifecycle = g_slist_prepend(thunk->lifecycle, thunk->rule);
	thunk->rule = NULL;
    } else if (g_ascii_strcasecmp(element_name, "id") == 0) {
	thunk->in_ID = FALSE;
	thunk->rule->id = thunk->text;
	thunk->text = NULL;
        thunk->want_text = FALSE;
    } else if (g_ascii_strcasecmp(element_name, "filter") == 0) {
        thunk->in_Filter = FALSE;
        thunk->rule->filter = thunk->text;
        thunk->text = NULL;
        thunk->want_text = FALSE;
    } else if (g_ascii_strcasecmp(element_name, "prefix") == 0) {
	thunk->in_Prefix = FALSE;
	thunk->rule->prefix = thunk->text;
	thunk->text = NULL;
        thunk->want_text = FALSE;
    } else if (g_ascii_strcasecmp(element_name, "status") == 0) {
	thunk->in_Status = FALSE;
	thunk->rule->status = thunk->text;
	thunk->text = NULL;
        thunk->want_text = FALSE;
    } else if (g_ascii_strcasecmp(element_name, "transition") == 0) {
	thunk->in_Transition = FALSE;
	thunk->rule->transition = thunk->action;
	thunk->action = NULL;
    } else if (g_ascii_strcasecmp(element_name, "expiration") == 0) {
	thunk->in_Expiration = FALSE;
	thunk->rule->expiration = thunk->action;
	thunk->action = NULL;
    } else if (g_ascii_strcasecmp(element_name, "days") == 0) {
	thunk->in_Days = FALSE;
	thunk->action->days = atoi(thunk->text);
	g_free(thunk->text);
	thunk->text = NULL;
        thunk->want_text = FALSE;
    } else if (g_ascii_strcasecmp(element_name, "date") == 0) {
	thunk->in_Date = FALSE;
	thunk->action->date = thunk->text;
	thunk->text = NULL;
        thunk->want_text = FALSE;
    } else if (g_ascii_strcasecmp(element_name, "storageclass") == 0) {
	thunk->in_StorageClass = FALSE;
	thunk->action->storage_class = thunk->text;
	thunk->text = NULL;
        thunk->want_text = FALSE;
    }
}

static void
lifecycle_text(GMarkupParseContext *context G_GNUC_UNUSED,
               const gchar *text,
               gsize text_len,
               gpointer user_data,
               GError **error G_GNUC_UNUSED)
{
    lifecycle_thunk *thunk = (lifecycle_thunk *)user_data;

    if (thunk->want_text) {
        g_free(thunk->text);
        thunk->text = g_strndup(text, text_len);
    }
}

static s3_result_t
get_openstack_swift_api_v1_setting(
	S3Handle *hdl)
{
    static result_handling_t result_handling[] = {
	{ 200,  S3_ERROR_RegionNotFound, 0, S3_RESULT_FAIL },
	{ 200,  0,                       0, S3_RESULT_OK },
	RESULT_HANDLING_ALWAYS_RETRY,
	{ 0, 0,                          0, /* default: */ S3_RESULT_FAIL  }
	};

    s3_verbose(hdl, 1);
    return perform_request(hdl, "GET", NULL, NULL, NULL, NULL, NULL, NULL,
			     NULL,
                             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);
}

static gboolean
get_openstack_swift_api_v2_setting(
	S3Handle *hdl)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
	{ 200,  0,                    0, S3_RESULT_OK },
	RESULT_HANDLING_ALWAYS_RETRY,
	{ 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
	};

    CurlBuffer buf_tmp_store = { 0 }; // to load once only
    CurlBuffer *buf_tmp = &buf_tmp_store; // to load once only
    GString *body = g_string_new("");

    g_string_append_printf(body, "{ \"auth\": {\n");
    if (hdl->tenant_id) {
	g_string_append_printf(body, "\"tenantId\":\"%s\"", hdl->tenant_id);
    } else if (hdl->tenant_name) {
	g_string_append_printf(body, "\"tenantName\":\"%s\"", hdl->tenant_name);
    }
    if (hdl->username && hdl->password) {
	g_string_append_printf(body, ",\"passwordCredentials\": { \"username\":\"%s\", \"password\":\"%s\" }", hdl->username, hdl->password);
    } else {
	g_string_append_printf(body, ",\"apiAccessKeyCredentialsi\":{ \"accessKey\":\"%s\", \"secretKey\":\"%s\" }", hdl->access_key, hdl->secret_key);
    }
    g_string_append(body, "}}");

    s3_buffer_load_string(buf_tmp, g_string_free(body, FALSE), hdl->verbose);

    s3_verbose(hdl, 1);
    hdl->getting_swift_2_token = TRUE; // naturally atomic
    g_free(hdl->x_auth_token);
    hdl->x_auth_token = NULL;
    g_free(hdl->x_storage_url);
    hdl->x_storage_url = NULL;
    result = perform_request(hdl, "POST", NULL, NULL, NULL, NULL,
			     "application/json", NULL, NULL,
			     S3_BUFFER_READ_FUNCS, buf_tmp,
			     NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);
    hdl->getting_swift_2_token = FALSE; // naturally atomic

    return result;
}

static s3_result_t
get_openstack_swift_api_v3_setting(
	S3Handle *hdl)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
	{ 200,  0,                    0, S3_RESULT_OK },
	{ 201,  0,                    0, S3_RESULT_OK },
	RESULT_HANDLING_ALWAYS_RETRY,
	{ 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
	};

    CurlBuffer buf_tmp_store = { 0 }; // to load once only
    CurlBuffer *buf_tmp = &buf_tmp_store; // to load once only
    GString *body = g_string_new("");

    g_string_append_printf(body, "{ \"auth\": {\n");
    g_string_append_printf(body, "    \"scope\": {\n");
    g_string_append_printf(body, "      \"project\": {\n");
    g_string_append_printf(body, "        \"domain\": {\n");
    g_string_append_printf(body, "          \"name\": \"%s\" },\n", hdl->domain_name);
    g_string_append_printf(body, "        \"name\": \"%s\" }},\n", hdl->project_name);
    g_string_append_printf(body, "    \"identity\": {\n");
    g_string_append_printf(body, "      \"methods\": [ \"password\" ],\n");
    g_string_append_printf(body, "      \"password\": {\n");
    g_string_append_printf(body, "        \"user\": {\n");
    g_string_append_printf(body, "          \"name\": \"%s\",\n", hdl->username);
    g_string_append_printf(body, "          \"domain\": {\n");
    g_string_append_printf(body, "            \"name\": \"%s\" },\n", hdl->domain_name);
    g_string_append_printf(body, "          \"password\": \"%s\" }}}}}\n", hdl->password);

    s3_buffer_load_string(buf_tmp, g_string_free(body, FALSE), hdl->verbose);
    s3_verbose(hdl, 1);
    hdl->getting_swift_3_token = TRUE;
    g_free(hdl->x_auth_token);
    hdl->x_auth_token = NULL;
    g_free(hdl->x_storage_url);
    hdl->x_storage_url = NULL;
    result = perform_request(hdl, "POST", NULL, NULL, NULL, NULL,
			     "application/json", NULL, NULL,
			     S3_BUFFER_READ_FUNCS, buf_tmp,
			     NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);
    hdl->getting_swift_3_token = FALSE;

    s3_buffer_destroy(buf_tmp);
    return result;
}
static void
parse_swift_v2_endpoints(
    gpointer data,
    gpointer user_data)
{
    amjson_t *json = data;
    struct failure_thunk *pthunk = user_data;

    assert(json);
    if (get_json_type(json) == JSON_HASH) {
	amjson_t *endpoint_region = get_json_hash_from_key(json, "region");
	amjson_t *endpoint_publicURL = get_json_hash_from_key(json, "publicURL");
	char *region = NULL;
	char *service_public_url = NULL;
        if (endpoint_region && get_json_type(endpoint_region) == JSON_STRING) {
	    region = get_json_string(endpoint_region);
	}
        if (endpoint_publicURL && get_json_type(endpoint_publicURL) == JSON_STRING) {
	    service_public_url = get_json_string(endpoint_publicURL);
	}
	if (region && service_public_url) {
	    if (!pthunk->bucket_location ||
		strcmp(pthunk->bucket_location, region) == 0) {
		pthunk->service_public_url = g_strdup(service_public_url);
	    }
	} else {
	    pthunk->service_public_url = g_strdup(service_public_url);
	}
    }
}

static void
parse_swift_v2_serviceCatalog(
    gpointer data,
    gpointer user_data)
{
    amjson_t *json = data;

    assert(json);
    if (get_json_type(json) == JSON_HASH) {
	amjson_t *catalog_type = get_json_hash_from_key(json, "type");
	if (get_json_type(catalog_type) == JSON_STRING && g_str_equal(get_json_string(catalog_type), "object-store")) {
	    amjson_t *catalog_endpoints = get_json_hash_from_key(json, "endpoints");
	    if (get_json_type(catalog_endpoints) == JSON_ARRAY) {
		foreach_json_array(catalog_endpoints, parse_swift_v2_endpoints, user_data);
	    }
	}
    }
}

static void
parse_swift_v3_endpoints(
    gpointer data,
    gpointer user_data)
{
    amjson_t *json = data;
    struct failure_thunk *pthunk = user_data;

    assert(json);
    if (get_json_type(json) == JSON_HASH) {
	amjson_t *endpoint_region = get_json_hash_from_key(json, "region_id");
	amjson_t *endpoint_interface = get_json_hash_from_key(json, "interface");
	amjson_t *endpoint_url = get_json_hash_from_key(json, "url");
	char *region = NULL;
	char *service_public_url = NULL;
        if (endpoint_region && get_json_type(endpoint_region) == JSON_STRING) {
	    region = get_json_string(endpoint_region);
	}
        if (endpoint_interface && get_json_type(endpoint_interface) == JSON_STRING) {
	    char *interface = get_json_string(endpoint_interface);
	    if (g_str_equal(interface, "public")) {
		if (endpoint_url && get_json_type(endpoint_url) == JSON_STRING) {
		    service_public_url = get_json_string(endpoint_url);
		}
	    }
	}
	if (region && service_public_url) {
	    if (!pthunk->bucket_location ||
		strcmp(pthunk->bucket_location, region) == 0) {
		pthunk->service_public_url = g_strdup(service_public_url);
	    }
	} else if (!pthunk->service_public_url && service_public_url) {
	    pthunk->service_public_url = g_strdup(service_public_url);
	}
    }
}

static void
parse_swift_v3_catalog(
    gpointer data,
    gpointer user_data)
{
    amjson_t *json = data;

    assert(json);
    if (get_json_type(json) == JSON_HASH) {
	amjson_t *catalog_type = get_json_hash_from_key(json, "type");
	if (get_json_type(catalog_type) == JSON_STRING && g_str_equal(get_json_string(catalog_type), "object-store")) {
	    amjson_t *catalog_endpoints = get_json_hash_from_key(json, "endpoints");
	    if (get_json_type(catalog_endpoints) == JSON_ARRAY) {
		foreach_json_array(catalog_endpoints, parse_swift_v3_endpoints, user_data);
	    }
	}
    }
}

s3_result_t
s3_multi_delete(S3Handle *hdl,
		const char *bucket,
		GSList *objects)
{
    GString *query;
    s3_result_t result = S3_RESULT_FAIL;
    const char *mime_type = "text/plain";
    const char *method = "POST";
    const char *subrsrc = "bulk-delete";
    g_assert(hdl != NULL);

    query = g_string_new(NULL);

    switch ( hdl->s3_api ) {
      case S3_API_SWIFT_1:
      case S3_API_SWIFT_2:
            method = "DELETE";
            // fall through...
      case S3_API_SWIFT_3:
            break;

      default:
            subrsrc = "delete";
            break;
    }

    if (hdl->s3_api == S3_API_SWIFT_1 ||
	hdl->s3_api == S3_API_SWIFT_2 ||
	hdl->s3_api == S3_API_SWIFT_3)
    {
	char *container = g_uri_escape_string(bucket, NULL, FALSE);
        bucket = "";

	for( ; objects != NULL ; objects = objects->next) {
	    s3_object *object = objects->data;
	    char *name = g_uri_escape_string(object->key, NULL, FALSE);
	    g_string_append_printf(query, "%s/%s\n", container, name);
            g_free(name);
	}
        g_free(container);

    } else {
        mime_type = "application/xml";
	g_string_append(query, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	g_string_append(query, "<Delete>\n");
	if (!hdl->verbose) {
	    g_string_append(query, "  <Quiet>true</Quiet>\n");
	}

	for( ; objects != NULL ; objects = objects->next) {
	    s3_object *object = objects->data;
	    g_string_append(query, "  <Object>\n");
	    g_string_append(query, "    <Key>");
	    g_string_append(query, object->key);
	    g_string_append(query, "</Key>\n");
	    g_string_append(query, "  </Object>\n");
	}
	g_string_append(query, "</Delete>\n");
    }

    {
        CurlBuffer buf_tmp_store = { 0 };
        CurlBuffer *buf_tmp = &buf_tmp_store;
        static result_handling_t result_handling[] = {
            { 200,  0,                     0, S3_RESULT_OK },
            { 204,  0,                     0, S3_RESULT_OK },
            { 400,  0,                     0, S3_RESULT_NOTIMPL },
            { 403,  0,                     0, S3_RESULT_NOTIMPL },
            { 404,  S3_ERROR_NoSuchBucket, 0, S3_RESULT_OK },
            RESULT_HANDLING_ALWAYS_RETRY,
            { 0,    0,                     0, /* default: */ S3_RESULT_FAIL  }
            };

        s3_buffer_load_string(buf_tmp, g_strdup(query->str), hdl->verbose);

        result = perform_request(hdl, method, bucket, NULL, subrsrc,
                            NULL, mime_type, NULL, NULL,
                            S3_BUFFER_READ_FUNCS, buf_tmp,
                            NULL, NULL, NULL, NULL, NULL,
                            result_handling, FALSE);
        s3_buffer_destroy(buf_tmp);
    }

    g_string_free(query, TRUE);

    if (result == S3_RESULT_NOTIMPL)
	s3_new_curl(hdl);

    return result;
}
