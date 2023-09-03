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
#include "amutil.h"
#endif
#ifdef HAVE_AMANDA_H
#include "amanda.h"
#endif
#include "amjson.h"
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

#define AMAZON_SECURITY_HEADER "x-amz-security-token"

#define AMAZON_STORAGE_CLASS_HEADER "x-amz-storage-class"

#define AMAZON_SERVER_SIDE_ENCRYPTION_HEADER "x-amz-server-side-encryption"

#define AMAZON_WILDCARD_LOCATION "*"
#define AMZ_QUOTA_CLOUD_URL ".s3.amazonaws.com"

/* parameters for exponential backoff in the face of retriable errors */

/* start at 0.01s */
#define EXPONENTIAL_BACKOFF_START_USEC G_USEC_PER_SEC/100
/* double at each retry */
#define EXPONENTIAL_BACKOFF_BASE 2
/* retry 14 times (for a total of about 3 minutes spent waiting) */
#define EXPONENTIAL_BACKOFF_MAX_RETRIES 14

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
        { 400,  S3_ERROR_RequestTimeout,     0,                          S3_RESULT_RETRY }, \
        { 403,  S3_ERROR_RequestTimeTooSkewed,0,                          S3_RESULT_RETRY }, \
        { 409,  S3_ERROR_OperationAborted,   0,                          S3_RESULT_RETRY }, \
        { 412,  S3_ERROR_PreconditionFailed, 0,                          S3_RESULT_RETRY }, \
        { 429,  0,                           0,                          S3_RESULT_RETRY }, \
        { 500,  S3_ERROR_None,               0,                          S3_RESULT_RETRY }, \
        { 500,  S3_ERROR_InternalError,      0,                          S3_RESULT_RETRY }, \
        { 501,  S3_ERROR_NotImplemented,     0,                          S3_RESULT_RETRY }, \
        { 503,  S3_ERROR_ServiceUnavailable, 0,                          S3_RESULT_RETRY }, \
        { 503,  S3_ERROR_SlowDown,           0,                          S3_RESULT_RETRY }, \
        { 503,  0,                           0,                          S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_COULDNT_CONNECT,      S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_COULDNT_RESOLVE_HOST, S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_PARTIAL_FILE,         S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_OPERATION_TIMEOUTED,  S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_SSL_CONNECT_ERROR,    S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_SEND_ERROR,           S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_RECV_ERROR,           S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_ABORTED_BY_CALLBACK,  S3_RESULT_RETRY }, \
        { 0,    0,                           CURLE_GOT_NOTHING,          S3_RESULT_RETRY }, \
        { 0,    0,                           AMAMDA_CURLE_SSL_CACERT_BADFILE,   S3_RESULT_RETRY }

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
    time_t expires;
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
    S3_api s3_api;
    char *ca_info;
    char *x_auth_token;
    char *x_storage_url;
    char *x_amz_expiration;
    char *x_amz_restore;

    CURL *curl;

    gboolean verbose;
    gboolean use_ssl;
    gboolean server_side_encryption_header;

    guint64 max_send_speed;
    guint64 max_recv_speed;

    /* information from the last request */
    char *last_message;
    guint last_response_code;
    s3_error_code_t last_s3_error_code;
    CURLcode last_curl_code;
    guint last_num_retries;
    void *last_response_body;
    guint last_response_body_size;
    char *uploadId;
    char *etag;

    /* offset with s3 */
    time_t time_offset_with_s3;
    char *content_type;

    gboolean reuse_connection;
    gboolean read_from_glacier;
    char *transfer_encoding;
    long     timeout;

    /* CAStor */
    char *reps;
    char *reps_bucket;

    gboolean http_v1_1;
};

typedef struct {
    CurlBuffer resp_buf;
    s3_write_func write_func;
    s3_reset_func reset_func;
    gpointer write_data;

    gboolean headers_done;
    gboolean int_write_done;
    char *etag;
    /* Points to current handle: Added to get hold of s3 offset */
    struct S3Handle *hdl;
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


static void s3_new_curl(S3Handle *hdl);

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
    S3_RESULT_OK = 1,
    S3_RESULT_NOTIMPL = 2
} s3_result_t;

typedef struct result_handling {
    guint response_code;
    s3_error_code_t s3_error_code;
    CURLcode curl_code;
    s3_result_t result;
} result_handling_t;

/*
 * get the access token for OAUTH2
 */
static gboolean oauth2_get_access_token(S3Handle *hdl);

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
    location_con_regex, date_sync_regex, x_auth_token_regex,
    x_subject_token_regex,
    x_storage_url_regex, access_token_regex, expires_in_regex,
    content_type_regex, details_regex, code_regex, uploadId_regex,
    json_message_regex, html_error_name_regex, html_message_regex,
    transfer_encoding_regex, x_amz_expiration_regex, x_amz_restore_regex;


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
                     const size_t content_length,
                     const char *project_id);



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

static gboolean get_openstack_swift_api_v1_setting(S3Handle *hdl);
static gboolean get_openstack_swift_api_v2_setting(S3Handle *hdl);
static gboolean get_openstack_swift_api_v3_setting(S3Handle *hdl);

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

static time_t
rfc3339_date(
    const char *date)
{
    gint year, month, day, hour, minute, seconds;
    const char *atz;

    if (strlen(date) < 19)
	return 1073741824;

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
		return 1073741824;
	    pid = fork();
	    switch (pid) {
		case -1:
		    close(fd[0]);
		    close(fd[1]);
		    return 1073741824;
		    break;
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

    if ((hdl->s3_api == S3_API_SWIFT_1 || hdl->s3_api == S3_API_SWIFT_2 ||
	 hdl->s3_api == S3_API_SWIFT_3 || hdl->s3_api == S3_API_OAUTH2) &&
	 hdl->x_storage_url) {
	url = g_string_new(hdl->x_storage_url);
	g_string_append(url, "/");
    } else {
	/* scheme */
	url = g_string_new("http");
	if (hdl->use_ssl)
            g_string_append(url, "s");

	g_string_append(url, "://");

	/* domain */
	if (hdl->use_subdomain && bucket)
            g_string_append_printf(url, "%s.%s", bucket, hdl->host);
	else
            g_string_append_printf(url, "%s", hdl->host);

	if (hdl->service_path) {
            g_string_append_printf(url, "%s/", hdl->service_path);
	} else {
	    g_string_append(url, "/");
	}
    }

    /* path */
    if (!hdl->use_subdomain && bucket) {
	/* curl_easy_escape addeded in 7.15.4 */
	#if LIBCURL_VERSION_NUM >= 0x070f04
	    curl_version_info_data *info;
	    /* check the runtime version too */
	    info = curl_version_info(CURLVERSION_NOW);
	    if (info->version_num >= 0x070f04)
		esc_bucket = curl_easy_escape(hdl->curl, bucket, 0);
	    else
		esc_bucket = curl_escape(bucket, 0);
	#else
	    esc_bucket = curl_escape(bucket, 0);
	#endif
        if (!esc_bucket) goto cleanup;
        g_string_append_printf(url, "%s", esc_bucket);
        if (key)
            g_string_append(url, "/");
	curl_free(esc_bucket);
    }

    if (key) {
	/* curl_easy_escape addeded in 7.15.4 */
	#if LIBCURL_VERSION_NUM >= 0x070f04
	    curl_version_info_data *info;
	    /* check the runtime version too */
	    info = curl_version_info(CURLVERSION_NOW);
	    if (info->version_num >= 0x070f04)
		esc_key = curl_easy_escape(hdl->curl, key, 0);
	    else
		esc_key = curl_escape(key, 0);
	#else
	    esc_key = curl_escape(key, 0);
	#endif
        if (!esc_key) goto cleanup;
        g_string_append_printf(url, "%s", esc_key);
	curl_free(esc_key);
    }

    if (url->str[strlen(url->str)-1] == '/') {
	g_string_truncate(url, strlen(url->str)-1);
    }

    /* query string */
    if (subresource || query || (hdl->s3_api == S3_API_CASTOR && hdl->tenant_name))
        g_string_append(url, "?");

    if (subresource)
        g_string_append(url, subresource);

    if (subresource && query)
        g_string_append(url, "&");

    if (query) {
	const char **q;
	gboolean first = TRUE;

	for (q = query; *q != NULL; q++) {
	    if (!first) {
		g_string_append_c(url, '&');
	    }
            g_string_append(url, *q);
	    first = FALSE;
	}
    }

    /* add CAStor tenant domain override query arg */
    if (hdl->s3_api == S3_API_CASTOR && hdl->tenant_name) {
        if (subresource || query) {
            g_string_append(url, "&");
        }
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
                     const size_t content_length,
                     const char *project_id)
{
    time_t t;
    struct tm tmp;
    char *date = NULL;
    char *szS3Date = NULL;
    char *zulu_date = NULL;
    char *buf = NULL;
#if (defined OPENSSL_VERSION_NUMBER && OPENSSL_VERSION_NUMBER < 0x10100000L) \
    || defined LIBRESSL_VERSION_NUMBER
    HMAC_CTX ctx;
#else
    HMAC_CTX *ctx;
#endif
    GByteArray *md = NULL;
    char *auth_base64 = NULL;
    struct curl_slist *headers = NULL;
    char *esc_bucket = NULL, *esc_key = NULL;
    GString *auth_string = NULL;
    char *reps = NULL;

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


    date = g_strdup_printf("%s, %02d %s %04d %02d:%02d:%02d GMT",
        wkday[tmp.tm_wday], tmp.tm_mday, month[tmp.tm_mon], 1900+tmp.tm_year,
        tmp.tm_hour, tmp.tm_min, tmp.tm_sec);

    szS3Date = g_strdup_printf("%04d%02d%02d",1900+tmp.tm_year,tmp.tm_mon+1,tmp.tm_mday);

    zulu_date = g_strdup_printf("%04d%02d%02dT%02d%02d%02dZ",
				1900+tmp.tm_year, tmp.tm_mon+1, tmp.tm_mday,
				tmp.tm_hour, tmp.tm_min, tmp.tm_sec);
    if (hdl->s3_api == S3_API_SWIFT_1) {
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
    } else if (hdl->s3_api == S3_API_SWIFT_2) {
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
    } else if (hdl->s3_api == S3_API_SWIFT_3) {
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
    } else if (hdl->s3_api == S3_API_OAUTH2) {
	if (bucket) {
            buf = g_strdup_printf("Authorization: Bearer %s", hdl->access_token);
            headers = curl_slist_append(headers, buf);
            g_free(buf);
	}
    } else if (hdl->s3_api == S3_API_CASTOR) {
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
    } else if (hdl->s3_api == S3_API_AWS4) {
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
	g_string_append(auth_string, "\n");

	/* CanonicalizedResource */
	g_string_append(auth_string, "/");

	if (hdl->use_subdomain) {
	    if (key) {
		char *esc_key = s3_uri_encode(key, 0);
		g_string_append(auth_string, esc_key);
		g_free(esc_key);
	    }
	} else {
	    g_string_append(auth_string, bucket);
	    if (key) {
		char *esc_key = s3_uri_encode(key, 0);
		g_string_append(auth_string, "/");
		g_string_append(auth_string, esc_key);
		g_free(esc_key);
	    }
	}
	g_string_append(auth_string, "\n");

	if (query) {
	    gboolean sub_done = !subresource;
	    gboolean first = TRUE;
	    const char **q;
	    for (q = query; *q != NULL; q++) {
		if (!first) {
		    g_string_append_c(auth_string, '&');
		}
		if (!sub_done && strcmp(subresource, *q) < 0) {
		    g_string_append(auth_string, subresource);
		    g_string_append_c(auth_string, '=');
		    g_string_append_c(auth_string, '&');
		    sub_done = TRUE;
		}
		g_string_append(auth_string, *q);
		first = FALSE;
	    }
	    if (!sub_done) {
		g_string_append_c(auth_string, '&');
		g_string_append(auth_string, subresource);
		g_string_append_c(auth_string, '=');
	    }
	} else if (subresource) {
	    g_string_append(auth_string, subresource);
	    g_string_append(auth_string, "=");
	}
	g_string_append(auth_string, "\n");

        if (subresource && strcmp(subresource, "lifecycle") == 0 && md5_hash && '\0' != md5_hash[0]) {
            g_string_append(auth_string, "content-md5:");
            g_string_append(auth_string, md5_hash);
            g_string_append(auth_string, "\n");

            buf = g_strdup_printf("Content-MD5: %s", md5_hash);
            headers = curl_slist_append(headers, buf);

            g_string_append(strSignedHeaders, "content-md5;");
            g_free(buf);
        }

    /* Header must be in alphebetic order */
	if (hdl->use_subdomain) {
	    g_string_append(auth_string, "host:");
	    g_string_append(auth_string, bucket);
	    g_string_append(auth_string, ".");
	    g_string_append(auth_string, hdl->host_without_port);
	    g_string_append(auth_string, "\n");
	} else {
	    g_string_append(auth_string, "host:");
	    g_string_append(auth_string, hdl->host_without_port);
	    g_string_append(auth_string, "\n");
	}
	g_string_append(strSignedHeaders, "host");

	g_string_append(auth_string, "x-amz-content-sha256:");
	g_string_append(auth_string, data_SHA256Hash);
	g_string_append(auth_string, "\n");
	g_string_append(strSignedHeaders, ";x-amz-content-sha256");

	g_string_append(auth_string, "x-amz-date:");
	g_string_append(auth_string, zulu_date);
	g_string_append(auth_string, "\n");
	g_string_append(strSignedHeaders, ";x-amz-date");

	if (hdl->server_side_encryption_header &&
	    is_non_empty_string(hdl->server_side_encryption)) {
	    g_string_append(auth_string, AMAZON_SERVER_SIDE_ENCRYPTION_HEADER);
	    g_string_append(auth_string, ":");
	    g_string_append(auth_string, hdl->server_side_encryption);
	    g_string_append(auth_string, "\n");
	    g_string_append(strSignedHeaders, ";"AMAZON_SERVER_SIDE_ENCRYPTION_HEADER);

	    buf = g_strdup_printf(AMAZON_SERVER_SIDE_ENCRYPTION_HEADER ": %s",
				  hdl->server_side_encryption);
	    headers = curl_slist_append(headers, buf);
	    g_free(buf);
	}

	if ((!subresource || !g_strstr_len(subresource, -1, "uploadId")) &&
	    (!query || !query[0] || !g_strstr_len(query[0], -1, "uploadId")) &&
	    (!query || !query[1] || !g_strstr_len(query[1], -1, "uploadId")) &&
	    is_non_empty_string(hdl->storage_class)) {
	    g_string_append(auth_string, AMAZON_STORAGE_CLASS_HEADER);
	    g_string_append(auth_string, ":");
	    g_string_append(auth_string, hdl->storage_class);
	    g_string_append(auth_string, "\n");
	    g_string_append(strSignedHeaders, ";"AMAZON_STORAGE_CLASS_HEADER);

	    buf = g_strdup_printf(AMAZON_STORAGE_CLASS_HEADER ": %s",
				  hdl->storage_class);
	    headers = curl_slist_append(headers, buf);
	    g_free(buf);
	}

	/* no more header */
	g_string_append(auth_string, "\n");

	g_string_append(auth_string, strSignedHeaders->str);
	g_string_append(auth_string, "\n");
	g_string_append(auth_string, data_SHA256Hash);

	canonical_hash = s3_compute_sha256_hash((unsigned char *)auth_string->str, auth_string->len);

	if (!hdl->bucket_location) {
	    hdl->bucket_location = g_strdup("us-east-1");
	}
	string_to_sign = g_string_new("AWS4-HMAC-SHA256\n");
	g_string_append(string_to_sign, zulu_date);
	g_string_append(string_to_sign, "\n");
	g_string_append(string_to_sign, szS3Date);
	g_string_append(string_to_sign, "/");
	g_string_append(string_to_sign, hdl->bucket_location);
	g_string_append(string_to_sign, "/s3/aws4_request");
	g_string_append(string_to_sign, "\n");
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

        buf = g_strdup_printf("x-amz-content-sha256: %s", data_SHA256Hash);
        headers = curl_slist_append(headers, buf);
        g_free(buf);

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

    } else { /* hdl->s3_api == S3_API_S3 */
	/* Build the string to sign, per the S3 spec.
	 * See: "Authenticating REST Requests" - API Version 2006-03-01 pg 58
	 * http://docs.aws.amazon.com/AmazonS3/latest/dev/RESTAuthentication.html
	 */

	/* verb */
	auth_string = g_string_new(verb);
	g_string_append(auth_string, "\n");

	/* Content-MD5 header */
	if (md5_hash)
            g_string_append(auth_string, md5_hash);
	g_string_append(auth_string, "\n");

	if (content_type) {
	    g_string_append(auth_string, content_type);
	}
	g_string_append(auth_string, "\n");

	/* Date */
	g_string_append(auth_string, date);
	g_string_append(auth_string, "\n");

	/* CanonicalizedAmzHeaders, sorted lexicographically */
	if (is_non_empty_string(hdl->user_token)) {
	    g_string_append(auth_string, AMAZON_SECURITY_HEADER);
	    g_string_append(auth_string, ":");
	    g_string_append(auth_string, hdl->user_token);
	    g_string_append(auth_string, ",");
	    g_string_append(auth_string, STS_PRODUCT_TOKEN);
	    g_string_append(auth_string, "\n");
	}

	/* CanonicalizedAmzHeaders, sorted lexicographically */
	if (is_non_empty_string(hdl->session_token)) {
	    g_string_append(auth_string, AMAZON_SECURITY_HEADER);
	    g_string_append(auth_string, ":");
	    g_string_append(auth_string, hdl->session_token);
	    g_string_append(auth_string, "\n");
	}

	if (hdl->server_side_encryption_header &&
	    is_non_empty_string(hdl->server_side_encryption)) {
	    g_string_append(auth_string, AMAZON_SERVER_SIDE_ENCRYPTION_HEADER);
	    g_string_append(auth_string, ":");
	    g_string_append(auth_string, hdl->server_side_encryption);
	    g_string_append(auth_string, "\n");
	}

	if ((!subresource || !g_strstr_len(subresource, -1, "uploadId")) &&
	    hdl->s3_api == S3_API_S3 &&
	    is_non_empty_string(hdl->storage_class)) {
	    g_string_append(auth_string, AMAZON_STORAGE_CLASS_HEADER);
	    g_string_append(auth_string, ":");
	    g_string_append(auth_string, hdl->storage_class);
	    g_string_append(auth_string, "\n");
	}

	/* CanonicalizedResource */
	if (hdl->service_path) {
	    g_string_append(auth_string, hdl->service_path);
	}
	g_string_append(auth_string, "/");
	if (bucket) {
	    if (hdl->use_subdomain)
		g_string_append(auth_string, bucket);
	    else {
		esc_bucket = curl_escape(bucket, 0);
		if (!esc_bucket) goto cleanup;
		g_string_append(auth_string, esc_bucket);
	    }
	}

	if (bucket && (hdl->use_subdomain || key))
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
#if (defined OPENSSL_VERSION_NUMBER && OPENSSL_VERSION_NUMBER < 0x10100000L) \
    || defined LIBRESSL_VERSION_NUMBER
	HMAC_CTX_init(&ctx);
	HMAC_Init_ex(&ctx, hdl->secret_key, (int) strlen(hdl->secret_key),
		     EVP_sha1(), NULL);
	HMAC_Update(&ctx, (unsigned char*) auth_string->str, auth_string->len);
	HMAC_Final(&ctx, md->data, &md->len);
	HMAC_CTX_cleanup(&ctx);
#else
	ctx = HMAC_CTX_new();
	HMAC_CTX_reset(ctx);
	HMAC_Init_ex(ctx, hdl->secret_key, (int) strlen(hdl->secret_key),
		     EVP_sha1(), NULL);
	HMAC_Update(ctx, (unsigned char*) auth_string->str, auth_string->len);
	HMAC_Final(ctx, md->data, &md->len);
	HMAC_CTX_free(ctx);
#endif
	auth_base64 = s3_base64_encode(md);
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

	if ((!subresource || !g_strstr_len(subresource, -1, "uploadId")) &&
	    hdl->s3_api != S3_API_OAUTH2 &&
	    is_non_empty_string(hdl->storage_class)) {
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
    }

    if (md5_hash && '\0' != md5_hash[0]) {
        buf = g_strdup_printf("Content-MD5: %s", md5_hash);
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }
    if (content_length > 0) {
        buf = g_strdup_printf("Content-Length: %zu", content_length);
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }

    if (content_type) {
        buf = g_strdup_printf("Content-Type: %s", content_type);
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }

    if (hdl->s3_api == S3_API_OAUTH2) {
        buf = g_strdup_printf("x-goog-api-version: 2");
        headers = curl_slist_append(headers, buf);
        g_free(buf);
    }

    if (project_id && hdl->s3_api == S3_API_OAUTH2) {
        buf = g_strdup_printf("x-goog-project-id: %s", project_id);
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

    gboolean in_title;
    gboolean in_body;
    gboolean in_code;
    gboolean in_message;
    gboolean in_details;
    gboolean in_access;
    gboolean in_token;
    gboolean in_serviceCatalog;
    gboolean in_service;
    gboolean in_endpoint;
    gboolean in_uploadId;
    gint     in_others;

    gchar *text;
    gsize text_len;

    gchar *message;
    gchar *details;
    gchar *error_name;
    gchar *token_id;
    gchar *service_type;
    gchar *service_public_url;
    gint64 expires;
    gchar *uploadId;

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
    gboolean ret = TRUE;
    struct failure_thunk thunk;
    GMarkupParseContext *ctxt = NULL;
    static GMarkupParser parser = { failure_start_element, failure_end_element, failure_text, NULL, NULL };
    GError *err = NULL;

    if (!hdl) return FALSE;

    if (hdl->last_message) g_free(hdl->last_message);
    hdl->last_message = NULL;
    hdl->last_response_code = 0;
    hdl->last_s3_error_code = 0;
    hdl->last_curl_code = 0;

    curl_easy_getinfo(hdl->curl, CURLINFO_RESPONSE_CODE, &response_code);
    hdl->last_response_code = response_code;

    /* bail out from a CURL error */
    if (curl_code != CURLE_OK) {
        hdl->last_curl_code = curl_code;
        hdl->last_message = g_strdup_printf("CURL error: %s", curl_error_buffer);
	if (response_code == 0)
            return FALSE;
    }

    /* check ETag, if present and not CAStor */
    if (etag && content_md5 && 200 == response_code &&
        hdl->s3_api != S3_API_CASTOR) {
        if (etag && g_ascii_strcasecmp(etag, content_md5))
            hdl->last_message = g_strdup("S3 Error: Possible data corruption (ETag returned by Amazon did not match the MD5 hash of the data sent)");
        else
            ret = FALSE;
        return ret;
    }

    /* Now look at the body to try to get the actual Amazon error message. */

    /* impose a reasonable limit on body size */
    if (body_len > MAX_ERROR_RESPONSE_LEN) {
        hdl->last_message = g_strdup("S3 Error: Unknown (response body too large to parse)");
        return FALSE;
    } else if (!body || body_len == 0) {
	if (response_code < 100 || response_code >= 400) {
	    hdl->last_message =
			g_strdup("S3 Error: Unknown (empty response body)");
            return TRUE; /* perhaps a network error; retry the request */
	} else {
	    /* 2xx and 3xx codes without body are good result */
	    hdl->last_s3_error_code = S3_ERROR_None;
	    return FALSE;
	}
    }

    if (hdl->verbose) {
	char *body_copy = g_strndup(body, body_len);
	g_debug("data in %d: %s", (int)body_len, body_copy);
	amfree(body_copy);
    }

    thunk.in_title = FALSE;
    thunk.in_body = FALSE;
    thunk.in_code = FALSE;
    thunk.in_message = FALSE;
    thunk.in_details = FALSE;
    thunk.in_access = FALSE;
    thunk.in_token = FALSE;
    thunk.in_serviceCatalog = FALSE;
    thunk.in_service = FALSE;
    thunk.in_endpoint = FALSE;
    thunk.in_uploadId = FALSE;
    thunk.in_others = 0;
    thunk.text = NULL;
    thunk.want_text = FALSE;
    thunk.text_len = 0;
    thunk.message = NULL;
    thunk.details = NULL;
    thunk.error_name = NULL;
    thunk.token_id = NULL;
    thunk.service_type = NULL;
    thunk.service_public_url = NULL;
    thunk.expires = 0;
    thunk.uploadId = NULL;
    thunk.bucket_location = hdl->bucket_location;

    if ((hdl->s3_api == S3_API_SWIFT_1 ||
         hdl->s3_api == S3_API_SWIFT_2) &&
	hdl->content_type &&
	(g_str_equal(hdl->content_type, "text/html") ||
	 g_str_equal(hdl->content_type, "text/plain"))) {

	char *body_copy = g_strndup(body, body_len);
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
	goto parsing_done;
    } else if ((hdl->s3_api == S3_API_SWIFT_3) &&
	hdl->content_type &&
	(g_str_equal(hdl->content_type, "text/html") ||
	 g_str_equal(hdl->content_type, "text/plain"))) {

	char *body_copy = g_strndup(body, body_len);
	regmatch_t pmatch[2];
	if (!s3_regexec_wrap(&html_error_name_regex, body_copy, 2, pmatch, 0)) {
            thunk.error_name = find_regex_substring(body_copy, pmatch[1]);
	    am_strrmspace(thunk.error_name);
        }
	if (!s3_regexec_wrap(&html_message_regex, body_copy, 2, pmatch, 0)) {
            thunk.message = find_regex_substring(body_copy, pmatch[1]);
        }
	goto parsing_done;
    } else if ((hdl->s3_api == S3_API_SWIFT_1 ||
		hdl->s3_api == S3_API_SWIFT_2 ||
		hdl->s3_api == S3_API_SWIFT_3) &&
	       hdl->content_type &&
	       g_str_equal(hdl->content_type, "application/json")) {
	char *body_copy = g_strndup(body, body_len);
	char *code = NULL;
	char *details = NULL;
	regmatch_t pmatch[2];

	if (hdl->getting_swift_2_token) {
	    amjson_t *json = parse_json(body_copy);
	    if (get_json_type(json) == JSON_HASH) {
		amjson_t *json_access = get_json_hash_from_key(json, "access");
		if (json_access && get_json_type(json_access) == JSON_HASH) {
		    amjson_t *json_token = get_json_hash_from_key(json_access, "token");
		    amjson_t *json_catalog = get_json_hash_from_key(json_access, "serviceCatalog");
		    if (json_token && get_json_type(json_token) == JSON_HASH) {
			amjson_t *json_token_id = get_json_hash_from_key(json_token, "id");
			amjson_t *json_token_expires = get_json_hash_from_key(json_token, "expires");
			if (json_token_id && get_json_type(json_token_id) == JSON_STRING) {
			    thunk.token_id = g_strdup(get_json_string(json_token_id));
			}
			if (json_token_expires && get_json_type(json_token_expires) == JSON_STRING) {
			    thunk.expires = rfc3339_date(get_json_string(json_token_expires));
			}
		    }
		    if (json_catalog && get_json_type(json_catalog) == JSON_ARRAY) {
			foreach_json_array(json_catalog, parse_swift_v2_serviceCatalog, &thunk);
		    }
		    if (thunk.token_id && thunk.expires && thunk.service_public_url) {
			g_free(body_copy);
			goto parse_done;
		    }
		}
	    }
	} else if (hdl->getting_swift_3_token) {
	    amjson_t *json = parse_json(body_copy);
	    // g_debug("json: %s", json_to_string(json));
	    if (get_json_type(json) == JSON_HASH) {
		amjson_t *json_token = get_json_hash_from_key(json, "token");
		if (json_token && get_json_type(json_token) == JSON_HASH) {
		    amjson_t *json_catalog = get_json_hash_from_key(json_token, "catalog");
		    amjson_t *json_expires_at = get_json_hash_from_key(json_token, "expires_at");
		    if (json_catalog && get_json_type(json_catalog) == JSON_ARRAY) {
			foreach_json_array(json_catalog, parse_swift_v3_catalog, &thunk);
		    }
		    if (json_expires_at && get_json_type(json_expires_at) == JSON_STRING) {
			thunk.expires = rfc3339_date(get_json_string(json_expires_at));
		    }
		    if (thunk.expires && thunk.service_public_url) {
			g_free(body_copy);
			goto parse_done;
		    }
		}
	    }
	}

	if (!s3_regexec_wrap(&code_regex, body_copy, 2, pmatch, 0)) {
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
	g_free(body_copy);
	return FALSE;
    } else if (hdl->s3_api == S3_API_CASTOR) {
	/* The error mesage is the body */
        hdl->last_message = g_strndup(body, body_len);
        return FALSE;
    } else if ((hdl->content_type &&
		!g_str_equal(hdl->content_type, "application/xml")) ||
	       (!hdl->content_type && hdl->transfer_encoding &&
		!g_str_equal(hdl->transfer_encoding, "chunked") &&
		!g_strstr_len(body, body_len, "xml version"))) {
	return FALSE;
    }

    /* run the parser over it */
    ctxt = g_markup_parse_context_new(&parser, 0, (gpointer)&thunk, NULL);
    if (!g_markup_parse_context_parse(ctxt, body, body_len, &err)) {
	    if (hdl->last_message) g_free(hdl->last_message);
	    hdl->last_message = g_strdup(err->message);
	    goto cleanup;
    }

    if (!g_markup_parse_context_end_parse(ctxt, &err)) {
	    if (hdl->last_message) g_free(hdl->last_message);
	    hdl->last_message = g_strdup(err->message);
	    goto cleanup;
    }

    g_markup_parse_context_free(ctxt);
    ctxt = NULL;

parse_done:
    if (hdl->s3_api == S3_API_SWIFT_2) {
	if (!hdl->x_auth_token && thunk.token_id) {
	    hdl->x_auth_token = thunk.token_id;
	    g_debug("x_auth_token: %s", hdl->x_auth_token);
	    thunk.token_id = NULL;
	}
	if (!hdl->x_storage_url && thunk.service_public_url) {
	    hdl->x_storage_url = thunk.service_public_url;
	    g_debug("x_storage_url: %s", hdl->x_storage_url);
	    thunk.service_public_url = NULL;
	} else if (!thunk.message && !thunk.error_name) {
	    if (!hdl->x_storage_url && !thunk.service_public_url) {
		if (thunk.bucket_location) {
		    thunk.message = g_strdup_printf("Did not find the publicURL for the '%s' region", thunk.bucket_location);
		} else {
		    thunk.message = g_strdup_printf("Did not find the publicURL");
		}
		thunk.error_name =g_strdup("RegionNotFound");
	    }
	}
    } else if (hdl->s3_api == S3_API_SWIFT_3) {
	if (!hdl->x_storage_url && thunk.service_public_url) {
	    hdl->x_storage_url = thunk.service_public_url;
	    g_debug("x_storage_url: %s", hdl->x_storage_url);
	    thunk.service_public_url = NULL;
	} else if (!thunk.message && !thunk.error_name) {
	    if (!hdl->x_storage_url && !thunk.service_public_url) {
		if (thunk.bucket_location) {
		    thunk.message = g_strdup_printf("Did not find the public url for the '%s' region", thunk.bucket_location);
		} else {
		    thunk.message = g_strdup_printf("Did not find the public url");
		}
		thunk.error_name =g_strdup("RegionNotFound");
	    }
	}
    }

    if (thunk.expires > 0) {
	hdl->expires = thunk.expires;
    }
parsing_done:
    if (thunk.error_name) {
        hdl->last_s3_error_code = s3_error_code_from_name(thunk.error_name);
	g_free(thunk.error_name);
	thunk.error_name = NULL;
    }

    if (thunk.message) {
	g_free(hdl->last_message);
	if (thunk.details) {
	    hdl->last_message = g_strdup_printf("%s: %s", thunk.message,
							  thunk.details);
	    amfree(thunk.message);
	    amfree(thunk.details);
	} else {
            hdl->last_message = thunk.message;
            thunk.message = NULL; /* steal the reference to the string */
	}
    } else if (thunk.details) {
	hdl->last_message = thunk.details;
	thunk.details = NULL; /* steal the reference to the string */
    }

    if (thunk.uploadId) {
	g_free(hdl->uploadId);
        hdl->uploadId = thunk.uploadId;
        thunk.uploadId = NULL; /* steal the reference to the string */
    }

cleanup:
    g_free(thunk.text);
    g_free(thunk.message);
    g_free(thunk.details);
    g_free(thunk.error_name);
    g_free(thunk.token_id);
    g_free(thunk.service_type);
    g_free(thunk.service_public_url);
    g_free(thunk.uploadId);
    return FALSE;
}

/* a CURLOPT_READFUNCTION to read data from a buffer. */
size_t
s3_buffer_read_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    CurlBuffer *data = stream;
    guint bytes_desired = (guint) size * nmemb;
    guint avail;

    if (data->mutex) { /* chunked transfer */
	g_mutex_lock(data->mutex);

	while (1) {
	    if (data->buffer_len == data->buffer_pos) {
		avail = 0;
	    } else if (data->buffer_len > data->buffer_pos) {
		avail = data->buffer_len - data->buffer_pos;
	    } else {
		avail = data->max_buffer_size - data->buffer_pos + data->buffer_len;
	    }
	    if (avail > bytes_desired || data->end_of_buffer)
		break;

	    g_cond_wait(data->cond, data->mutex);
	}

	if (bytes_desired > avail)
	    bytes_desired = avail;
	if (bytes_desired > 0) {
	    if (data->buffer_len > data->buffer_pos) {
		memcpy((char *)ptr, data->buffer + data->buffer_pos, bytes_desired);
		data->buffer_pos += bytes_desired;
	    } else {
		guint count_end = data->max_buffer_size - data->buffer_pos;
		guint count_begin;

		if (count_end > bytes_desired)
		    count_end = bytes_desired;
		memcpy((char *)ptr, data->buffer + data->buffer_pos, count_end);
		data->buffer_pos += count_end;
		count_begin = bytes_desired - count_end;
		if (count_begin > 0) {
		    memcpy((char *)ptr + count_end, data->buffer, count_begin);
		    data->buffer_pos = count_begin;
		}
	    }
	}
	/* signal to add more data to the buffer */
	g_cond_broadcast(data->cond);
	g_mutex_unlock(data->mutex);

    } else {
	/* check the number of bytes remaining, just to be safe */
	if (bytes_desired > data->buffer_len - data->buffer_pos)
            bytes_desired = data->buffer_len - data->buffer_pos;

	memcpy((char *)ptr, data->buffer + data->buffer_pos, bytes_desired);
	data->buffer_pos += bytes_desired;
    }

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


    if (data->mutex) { /* chunked transfer */
	g_mutex_lock(data->mutex);

	/* error out if 2 block can't fit in the buffer */
	if (new_bytes*2 > data->max_buffer_size) {
	    g_mutex_unlock(data->mutex);
	    return 0;
	}

	// Check for enough space in the buffer
	while (1) {
	    guint avail;
	    if (data->buffer_len == data->buffer_pos) {
		avail =  data->max_buffer_size;
	    } else if (data->buffer_len > data->buffer_pos) {
		avail = data->buffer_pos + data->max_buffer_size - data->buffer_len;
	    } else {
		avail = data->buffer_pos - data->buffer_len;
	    }
	    if (avail > new_bytes) {
		break;
	    }
	    g_cond_wait(data->cond, data->mutex);
	}

	// Copy the new data to the buffer
	if (data->buffer_len > data->buffer_pos) {
	    guint count_end = data->max_buffer_size - data->buffer_len;
	    guint count_begin;
	    if (count_end > new_bytes)
		count_end = new_bytes;
	    memcpy(data->buffer + data->buffer_len, ptr, count_end);
	    data->buffer_len += count_end;
	    count_begin = new_bytes - count_end;
	    if (count_begin > 0) {
		memcpy(data->buffer, ptr + count_end, count_begin);
		data->buffer_len = count_begin;
	    }
	} else {
	    memcpy(data->buffer + data->buffer_len, ptr, new_bytes);
	    data->buffer_len += new_bytes;
	}

	g_cond_broadcast(data->cond);
	g_mutex_unlock(data->mutex);
	return new_bytes;
    }

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
	    if (!g_ascii_isprint(s[i])) {
		return 0;
	    }
	}
        lineprefix="Data In: ";
        break;

    case CURLINFO_DATA_OUT:
	if (len > 3000) return 0;
	for (i=0;i<len;i++) {
	    if (!g_ascii_isprint(s[i])) {
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
		gboolean chunked)
{
    char *url = NULL;
    s3_result_t result = S3_RESULT_FAIL; /* assume the worst.. */
    CURLcode curl_code = CURLE_OK;
    char curl_error_buffer[CURL_ERROR_SIZE] = "";
    struct curl_slist *headers = NULL;
    /* Set S3Internal Data */
    S3InternalData int_writedata = {{NULL, 0, 0, MAX_ERROR_RESPONSE_LEN, TRUE, NULL, NULL}, NULL, NULL, NULL, FALSE, FALSE, NULL, hdl};
    gboolean should_retry;
    gint retries = 0;
    gint retry_after_close = 0;
    gulong backoff = EXPONENTIAL_BACKOFF_START_USEC;
    /* corresponds to PUT, HEAD, GET, and POST */
    int curlopt_upload = 0, curlopt_nobody = 0, curlopt_httpget = 0, curlopt_post = 0;
    /* do we want to examine the headers */
    const char *curlopt_customrequest = NULL;
    /* for MD5 calculation */
    GByteArray *md5_hash = NULL;
    gchar *md5_hash_hex = NULL, *md5_hash_b64 = NULL;
    size_t request_body_size = 0;
    char *data_SHA256Hash = NULL;

    g_assert(hdl != NULL && hdl->curl != NULL);

    if (hdl->s3_api == S3_API_OAUTH2 && !hdl->getting_oauth2_access_token &&
	(!hdl->access_token || hdl->expires < time(NULL))) {
	result = oauth2_get_access_token(hdl);
	if (!result) {
	    g_debug("oauth2_get_access_token returned %d", result);
	    return result;
	}
    } else if (hdl->s3_api == S3_API_SWIFT_2 && !hdl->getting_swift_2_token &&
	       (!hdl->x_auth_token || hdl->expires < time(NULL))) {
	result = get_openstack_swift_api_v2_setting(hdl);
	if (!result) {
	    g_debug("get_openstack_swift_api_v2_setting returned %d", result);
	    return result;
	}
    } else if (hdl->s3_api == S3_API_SWIFT_3 && !hdl->getting_swift_3_token &&
	       (!hdl->x_auth_token || hdl->expires < time(NULL))) {
	result = get_openstack_swift_api_v3_setting(hdl);
	if (!result) {
	    g_debug("get_openstack_swift_api_v3_setting returned %d", result);
	    return result;
	}
    }

    s3_reset(hdl);

    url = build_url(hdl, bucket, key, subresource, query);
    if (!url) goto cleanup;

    /* libcurl may behave strangely if these are not set correctly */
    if (g_str_has_prefix(verb, "PUT")) {
        curlopt_upload = 1;
    } else if (g_str_has_prefix(verb, "GET")) {
        curlopt_httpget = 1;
    } else if (g_str_has_prefix(verb, "POST")) {
        curlopt_post = 1;
    } else if (g_str_has_prefix(verb, "HEAD")) {
        curlopt_nobody = 1;
    } else {
        curlopt_customrequest = verb;
    }

    if (size_func) {
        request_body_size = size_func(read_data);
    }

    if (hdl->s3_api == S3_API_AWS4) {
        if (read_data) {
            data_SHA256Hash = s3_compute_sha256_hash_ba(read_data);
        } else {
            data_SHA256Hash = s3_compute_sha256_hash((unsigned char *)"", 0);
        }
        if (subresource && strcmp(subresource, "lifecycle") == 0 && md5_func) {
            md5_hash = md5_func(read_data);
            if (md5_hash) {
                md5_hash_b64 = s3_base64_encode(md5_hash);
                md5_hash_hex = s3_hex_encode(md5_hash);
                g_byte_array_free(md5_hash, TRUE);
            }
        }
    } else if (md5_func) {
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
	struct curl_slist *header;

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
        headers = authenticate_request(hdl, verb, bucket, key, subresource, query,
            md5_hash_b64, data_SHA256Hash, content_type, request_body_size, project_id);

	/* add user header to headers */
	for (header = user_headers; header != NULL; header = header->next) {
	    headers = curl_slist_append(headers, header->data);
	}

	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4 ))) {
	    goto curl_error;

	}
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
	if (progress_func) {
	    if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_NOPROGRESS,0)))
		goto curl_error;
	}
        if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_PROGRESSDATA, progress_data)))
            goto curl_error;

	if (!chunked) {
	    /* CURLOPT_INFILESIZE_LARGE added in 7.11.0 */
#if LIBCURL_VERSION_NUM >= 0x070b00
            if ((curl_code = curl_easy_setopt(hdl->curl,
					      CURLOPT_INFILESIZE_LARGE,
					      (curl_off_t)request_body_size)))
		goto curl_error;
#else
            if ((curl_code = curl_easy_setopt(hdl->curl,
					      CURLOPT_INFILESIZE,
					      (long)request_body_size)))
		goto curl_error;
#endif

	    /* CURLOPT_POSTFIELDSIZE_LARGE added in 7.11.1 */
#if LIBCURL_VERSION_NUM >= 0x070b01
            if ((curl_code = curl_easy_setopt(hdl->curl,
					      CURLOPT_POSTFIELDSIZE_LARGE,
					      (curl_off_t)request_body_size)))
		goto curl_error;
#else
            if ((curl_code = curl_easy_setopt(hdl->curl,
					      CURLOPT_POSTFIELDSIZE,
					      (long)request_body_size)))
		goto curl_error;
#endif
	}

/* CURLOPT_MAX_{RECV,SEND}_SPEED_LARGE added in 7.15.5 */
#if LIBCURL_VERSION_NUM >= 0x070f05
	if (s3_curl_throttling_compat()) {
	    if (hdl->max_send_speed)
		if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_MAX_SEND_SPEED_LARGE, (curl_off_t)hdl->max_send_speed)))
		    goto curl_error;

	    if (hdl->max_recv_speed)
		if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_MAX_RECV_SPEED_LARGE, (curl_off_t)hdl->max_recv_speed)))
		    goto curl_error;
	}
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


        if (curlopt_upload || curlopt_post) {
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
	if (hdl->proxy) {
            if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_PROXY,
                                              hdl->proxy)))
                goto curl_error;
	}

	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_FRESH_CONNECT,
		(long)(hdl->reuse_connection && retry_after_close == 0 ? 0 : 1)))) {
	    goto curl_error;
	}
	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_FORBID_REUSE,
		(long)(hdl->reuse_connection? 0 : 1)))) {
	    goto curl_error;
	}
	if ((curl_code = curl_easy_setopt(hdl->curl, CURLOPT_TIMEOUT,
		(long)hdl->timeout))) {
	    goto curl_error;
	}

        /* Perform the request */
        curl_code = curl_easy_perform(hdl->curl);


        /* interpret the response into hdl->last* */
    curl_error: /* (label for short-circuiting the curl_easy_perform call) */
        should_retry = interpret_response(hdl, curl_code, curl_error_buffer,
            int_writedata.resp_buf.buffer, int_writedata.resp_buf.buffer_pos, int_writedata.etag, md5_hash_hex);

	if (hdl->last_response_code == 503) {
	    s3_new_curl(hdl);
	}

	if (hdl->s3_api == S3_API_OAUTH2 &&
	    hdl->last_response_code == 401 &&
	    hdl->last_s3_error_code == S3_ERROR_AuthenticationRequired) {
	    should_retry = oauth2_get_access_token(hdl);
	}
        /* and, unless we know we need to retry, see what we're to do now */
        if (!should_retry) {
            result = lookup_result(result_handling, hdl->last_response_code,
                                   hdl->last_s3_error_code, hdl->last_curl_code);

            /* break out of the while(1) unless we're retrying */
            if (result != S3_RESULT_RETRY)
                break;
        }

        if (retries >= EXPONENTIAL_BACKOFF_MAX_RETRIES &&
	    retry_after_close < 3 &&
	    hdl->last_s3_error_code == S3_ERROR_RequestTimeout) {
	    retries = -1;
	    retry_after_close++;
	    g_debug("Retry on a new connection");
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
    g_free(data_SHA256Hash);

    /* we don't deallocate the response body -- we keep it for later */
    g_free(hdl->etag);
    hdl->etag = int_writedata.etag;
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
    amfree(data->etag);
    if (data->reset_func) {
        data->reset_func(data->write_data);
    }
}

static size_t
s3_internal_header_func(void *ptr, size_t size, size_t nmemb, void * stream)
{
    static const char *final_header = "\r\n";
    time_t remote_time_in_sec,local_time;
    char *header;
    regmatch_t pmatch[2];
    S3InternalData *data = (S3InternalData *) stream;

    header = g_strndup((gchar *) ptr, (gsize) size*nmemb);

    if (header[strlen(header)-1] == '\n')
	header[strlen(header)-1] = '\0';
    if (header[strlen(header)-1] == '\r')
	header[strlen(header)-1] = '\0';
    if (!s3_regexec_wrap(&etag_regex, header, 2, pmatch, 0)) {
	g_free(data->etag);
        data->etag = find_regex_substring(header, pmatch[1]);
    }
    if (!s3_regexec_wrap(&x_auth_token_regex, header, 2, pmatch, 0)) {
	g_free(data->hdl->x_auth_token);
	data->hdl->x_auth_token = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_subject_token_regex, header, 2, pmatch, 0)) {
	g_free(data->hdl->x_auth_token);
	data->hdl->x_auth_token = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_storage_url_regex, header, 2, pmatch, 0)) {
	g_free(data->hdl->x_storage_url);
	data->hdl->x_storage_url = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&content_type_regex, header, 2, pmatch, 0)) {
	g_free(data->hdl->content_type);
	data->hdl->content_type = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&transfer_encoding_regex, header, 2, pmatch, 0)) {
	g_free(data->hdl->transfer_encoding);
	data->hdl->transfer_encoding = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_amz_expiration_regex, header, 2, pmatch, 0)) {
	g_free(data->hdl->x_amz_expiration);
	data->hdl->x_amz_expiration = find_regex_substring(header, pmatch[1]);
    }

    if (!s3_regexec_wrap(&x_amz_restore_regex, header, 2, pmatch, 0)) {
	g_free(data->hdl->x_amz_restore);
	data->hdl->x_amz_restore = find_regex_substring(header, pmatch[1]);
    }

    if (strlen(header) == 0)
	data->headers_done = TRUE;
    if (g_str_equal(final_header, header))
        data->headers_done = TRUE;
    if (g_str_equal("\n", header))
        data->headers_done = TRUE;

    /* If date header is found */
    if (!s3_regexec_wrap(&date_sync_regex, header, 2, pmatch, 0)){
        char *date = find_regex_substring(header, pmatch[1]);

        /* Remote time is always in GMT: RFC 2616 */
        /* both curl_getdate and time operate in UTC, so no timezone math is necessary */
        if ( (remote_time_in_sec = curl_getdate(date, NULL)) < 0 ){
            g_debug("Error: Conversion of remote time to seconds failed.");
            data->hdl->time_offset_with_s3 = 0;
        }else{
            local_time = time(NULL);
            /* Offset time */
            data->hdl->time_offset_with_s3 = remote_time_in_sec - local_time;

	    if (data->hdl->verbose)
		g_debug("Time Offset (remote - local) :%ld",(long)data->hdl->time_offset_with_s3);
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
        {"^Transfer-Encoding:[[:space:]]*([^ ;]+).*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &transfer_encoding_regex},
        {"<Message>[[:space:]]*([^<]*)[[:space:]]*</Message>", REG_EXTENDED | REG_ICASE, &message_regex},
        {"^[a-z0-9](-*[a-z0-9]){2,62}$", REG_EXTENDED | REG_NOSUB, &subdomain_regex},
        {"(/>)|(>([^<]*)</LocationConstraint>)", REG_EXTENDED | REG_ICASE, &location_con_regex},
        {"^Date:(.*)$",REG_EXTENDED | REG_ICASE | REG_NEWLINE, &date_sync_regex},
        {"\"access_token\"[[:space:]]*:[[:space:]]*\"([^\"]*)\",", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &access_token_regex},
	{"\"expires_in\"[[:space:]]*:[[:space:]]*(.*),", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &expires_in_regex},
        {"\"details\": \"([^\"]*)\",", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &details_regex},
        {"\"code\": (.*),", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &code_regex},
        {"\"message\": \"([^\"]*)\",", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &json_message_regex},
	{"<UploadId>[[:space:]]*([^<]*)[[:space:]]*</UploadId>", REG_EXTENDED | REG_ICASE, &uploadId_regex},
	{"<h1>[[:space:]]*([^<]*)[[:space:]]*</h1>", REG_EXTENDED | REG_ICASE, &html_error_name_regex},
	{"<p>[[:space:]]*([^<]*)[[:space:]]*</p>", REG_EXTENDED | REG_ICASE, &html_message_regex},
        {"^x-amz-expiration:[[:space:]]*([^ ]+)[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &x_amz_expiration_regex},
        {"^x-amz-restore:[[:space:]]*([^ ]+)[[:space:]]*$", REG_EXTENDED | REG_ICASE | REG_NEWLINE, &x_amz_restore_regex},
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
        {"<Code>\\s*([^<]*)\\s*</Code>",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &error_name_regex},
        {"^ETag:\\s*\"([^\"]+)\"\\s*$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &etag_regex},
        {"^X-Auth-Token:\\s*([^ ]+)\\s*$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &x_auth_token_regex},
        {"^X-Subject-Token:\\s*([^ ]+)\\s*$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &x_subject_token_regex},
        {"^X-Storage-Url:\\s*([^ ]+)\\s*$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &x_storage_url_regex},
        {"^Content-Type:\\s*([^ ]+)\\s*$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &content_type_regex},
        {"^Transfer-Encoding:\\s*([^ ]+)\\s*$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &transfer_encoding_regex},
        {"<Message>\\s*([^<]*)\\s*</Message>",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &message_regex},
        {"^[a-z0-9]((-*[a-z0-9])|(\\.[a-z0-9])){2,62}$",
         G_REGEX_OPTIMIZE | G_REGEX_NO_AUTO_CAPTURE,
         &subdomain_regex},
        {"(/>)|(>([^<]*)</LocationConstraint>)",
         G_REGEX_CASELESS,
         &location_con_regex},
        {"^Date:(.*)$",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &date_sync_regex},
        {"\"access_token\"\\s*:\\s*\"([^\"]*)\"",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &access_token_regex},
        {"\"expires_in\"\\s*:\\s*(.*),",
         G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
         &expires_in_regex},
        {"\"details\" : \"([^\"]*)\"",
	 G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
	 &details_regex},
        {"\"code\" : (.*)",
	 G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
	 &code_regex},
        {"\"message\" : \"([^\"]*)\"",
	 G_REGEX_OPTIMIZE | G_REGEX_CASELESS,
	 &json_message_regex},
        {"(/>)|(>([^<]*)</UploadId>)",
         G_REGEX_CASELESS,
         &uploadId_regex},
        {"(/>)|(<h1>([^<]*)</h1>)",
         G_REGEX_CASELESS,
         &html_error_name_regex},
        {"(/>)|(<p>([^<]*)</p>)",
         G_REGEX_CASELESS,
         &html_message_regex},
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
s3_bucket_location_compat(const char *bucket)
{
    return !s3_regexec_wrap(&subdomain_regex, bucket, 0, NULL, 0);
}

static gboolean
get_openstack_swift_api_v1_setting(
	S3Handle *hdl)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
	{ 200,  S3_ERROR_RegionNotFound, 0, S3_RESULT_FAIL },
	{ 200,  0,                       0, S3_RESULT_OK },
	RESULT_HANDLING_ALWAYS_RETRY,
	{ 0, 0,                          0, /* default: */ S3_RESULT_FAIL  }
	};

    s3_verbose(hdl, 1);
    result = perform_request(hdl, "GET", NULL, NULL, NULL, NULL, NULL, NULL,
			     NULL,
                             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);

    return result == S3_RESULT_OK;
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

    CurlBuffer buf = {NULL, 0, 0, 0, TRUE, NULL, NULL};
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

    buf.buffer = g_string_free(body, FALSE);
    buf.buffer_len = strlen(buf.buffer);
    s3_verbose(hdl, 1);
    hdl->getting_swift_2_token = TRUE;
    g_free(hdl->x_auth_token);
    hdl->x_auth_token = NULL;
    g_free(hdl->x_storage_url);
    hdl->x_storage_url = NULL;
    result = perform_request(hdl, "POST", NULL, NULL, NULL, NULL,
			     "application/json", NULL, NULL,
			     S3_BUFFER_READ_FUNCS, &buf,
			     NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);
    hdl->getting_swift_2_token = FALSE;

    return result == S3_RESULT_OK;
}

static gboolean
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

    CurlBuffer buf = {NULL, 0, 0, 0, TRUE, NULL, NULL};
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

    buf.buffer = g_string_free(body, FALSE);
    // g_debug("buffer: %s", buf.buffer);
    buf.buffer_len = strlen(buf.buffer);
    s3_verbose(hdl, 1);
    hdl->getting_swift_3_token = TRUE;
    g_free(hdl->x_auth_token);
    hdl->x_auth_token = NULL;
    g_free(hdl->x_storage_url);
    hdl->x_storage_url = NULL;
    result = perform_request(hdl, "POST", NULL, NULL, NULL, NULL,
			     "application/json", NULL, NULL,
			     S3_BUFFER_READ_FUNCS, &buf,
			     NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);
    hdl->getting_swift_3_token = FALSE;

    return result == S3_RESULT_OK;
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
	const gboolean read_from_glacier,
	const long timeout,
        const char *reps,
        const char *reps_bucket,
        const gboolean http_v1_1)
{
    S3Handle *hdl;
    char *hwp;

    hdl = g_new0(S3Handle, 1);
    if (!hdl) goto error;

    hdl->verbose = TRUE;
    hdl->use_ssl = s3_curl_supports_ssl();
    hdl->reuse_connection = reuse_connection;
    hdl->read_from_glacier = read_from_glacier;
    hdl->timeout = timeout;

    if (s3_api == S3_API_S3) {
	g_assert(access_key);
	hdl->access_key = g_strdup(access_key);
	g_assert(secret_key);
	hdl->secret_key = g_strdup(secret_key);
	/* NULL is okay */
	hdl->session_token = g_strdup(session_token);
    } else if (s3_api == S3_API_AWS4) {
	g_assert(access_key);
	hdl->access_key = g_strdup(access_key);
	g_assert(secret_key);
	hdl->secret_key = g_strdup(secret_key);
	/* NULL is okay */
	hdl->session_token = g_strdup(session_token);
    } else if (s3_api == S3_API_SWIFT_1) {
	g_assert(swift_account_id);
	hdl->swift_account_id = g_strdup(swift_account_id);
	g_assert(swift_access_key);
	hdl->swift_access_key = g_strdup(swift_access_key);
    } else if (s3_api == S3_API_SWIFT_2) {
	g_assert((username && password) || (access_key && secret_key));
	hdl->username = g_strdup(username);
	hdl->password = g_strdup(password);
	hdl->access_key = g_strdup(access_key);
	hdl->secret_key = g_strdup(secret_key);
	g_assert(tenant_id || tenant_name);
	hdl->tenant_id = g_strdup(tenant_id);
	hdl->tenant_name = g_strdup(tenant_name);
    } else if (s3_api == S3_API_SWIFT_3) {
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
    } else if (s3_api == S3_API_OAUTH2) {
	hdl->client_id = g_strdup(client_id);
	hdl->client_secret = g_strdup(client_secret);
	hdl->refresh_token = g_strdup(refresh_token);
    } else if (s3_api == S3_API_CASTOR) {
	hdl->username = g_strdup(username);
	hdl->password = g_strdup(password);
	hdl->tenant_name = g_strdup(tenant_name);
        hdl->reps = g_strdup(reps);
        hdl->reps_bucket = g_strdup(reps_bucket);
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
	host = "s3.amazonaws.com";
    hdl->host = g_ascii_strdown(host, -1);
    hwp = strchr(hdl->host, ':');
    if (hwp) {
	*hwp = '\0';
	hdl->host_without_port = g_strdup(hdl->host);
	*hwp = ':';
    } else {
	hdl->host_without_port = g_strdup(hdl->host);
    }

    hdl->use_subdomain = use_subdomain ||
			 (g_str_equal(hdl->host, "s3.amazonaws.com") &&
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

    hdl->http_v1_1 = http_v1_1;

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
    if (hdl->curl) {
	curl_easy_cleanup(hdl->curl);
    }

    hdl->curl = curl_easy_init();
    if (!hdl->curl) return;

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
					CURL_HTTP_VERSION_1_1);
            if (hdl->username)
		 curl_easy_setopt(hdl->curl, CURLOPT_USERNAME, hdl->username);
            if (hdl->password)
		 curl_easy_setopt(hdl->curl, CURLOPT_PASSWORD, hdl->password);
            curl_easy_setopt(hdl->curl, CURLOPT_HTTPAUTH,
			     (CURLAUTH_BASIC | CURLAUTH_DIGEST));
	}
#endif
    }

    /* if user wants to set HTTP version to 1.1 */
    if (hdl->http_v1_1)
        curl_easy_setopt(hdl->curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
}

gboolean
s3_open2(
    S3Handle *hdl)
{
    gboolean ret = TRUE;

    /* get the X-Storage-Url and X-Auth-Token */
    if (hdl->s3_api == S3_API_SWIFT_1) {
	ret = get_openstack_swift_api_v1_setting(hdl);
    } else if (hdl->s3_api == S3_API_SWIFT_2) {
	ret = get_openstack_swift_api_v2_setting(hdl);
    } else if (hdl->s3_api == S3_API_SWIFT_3) {
	ret = get_openstack_swift_api_v3_setting(hdl);
    }

    return ret;
}

void
s3_free(S3Handle *hdl)
{
    s3_reset(hdl);

    if (hdl) {
        g_free(hdl->access_key);
        g_free(hdl->secret_key);
        g_free(hdl->session_token);
        g_free(hdl->swift_account_id);
        g_free(hdl->swift_access_key);
        g_free(hdl->content_type);
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
        g_free(hdl->access_token);
        g_free(hdl->etag);
        g_free(hdl->transfer_encoding);
        if (hdl->user_token) g_free(hdl->user_token);
        if (hdl->bucket_location) g_free(hdl->bucket_location);
        if (hdl->storage_class) g_free(hdl->storage_class);
        if (hdl->server_side_encryption) g_free(hdl->server_side_encryption);
        if (hdl->host) g_free(hdl->host);
        if (hdl->service_path) g_free(hdl->service_path);
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
        if (hdl->content_type) {
            g_free(hdl->content_type);
            hdl->content_type = NULL;
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
s3_set_max_send_speed(S3Handle *hdl, guint64 max_send_speed)
{
    if (!s3_curl_throttling_compat())
	return FALSE;

    hdl->max_send_speed = max_send_speed;

    return TRUE;
}

gboolean
s3_set_max_recv_speed(S3Handle *hdl, guint64 max_recv_speed)
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
 * @param buffer_len: the length of the data to upload
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

    return result == S3_RESULT_OK;
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
    char *subresource = NULL;
    char **query = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    g_assert(hdl != NULL);

    if (uploadId) {
	if (hdl->s3_api == S3_API_AWS4) {
	    query = g_new0(char *, 3);
	    query[0] = g_strdup_printf("partNumber=%d", partNumber);
	    query[1] = g_strdup_printf("uploadId=%s", uploadId);
	    query[2] = NULL;
	} else {
	    subresource = g_strdup_printf("partNumber=%d&uploadId=%s",
				partNumber, uploadId);
	}
    }

    result = perform_request(hdl, "PUT", bucket, key, subresource,
		 (const char **)query, NULL, NULL,
		 NULL,
                 read_func, reset_func, size_func, md5_func, read_data,
                 NULL, NULL, NULL, progress_func, progress_data,
                 result_handling, FALSE);

    g_free(subresource);
    if (query) {
	g_free(query[0]);
	g_free(query[1]);
	g_free(query);
    }
    if (etag) {
	*etag = hdl->etag;
	hdl->etag = NULL;
    }

    return result == S3_RESULT_OK;
}


char *
s3_initiate_multi_part_upload(
    S3Handle *hdl,
    const char *bucket,
    const char *key)
{
    char *subresource;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    subresource = g_strdup_printf("uploads");
    hdl->server_side_encryption_header = TRUE;
    result = perform_request(hdl, "POST", bucket, key, subresource, NULL,
		 NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);
    hdl->server_side_encryption_header = FALSE;

    g_free(subresource);

    if (result == S3_RESULT_OK) {
	return hdl->uploadId;
    } else {
	return NULL;
    }
}

gboolean
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
    char *subresource = NULL;
    char **query = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    if (hdl->s3_api == S3_API_AWS4) {
	query = g_new0(char *, 2);
	query[0] = g_strdup_printf("uploadId=%s", uploadId);
	query[1] = NULL;
    } else {
	subresource = g_strdup_printf("uploadId=%s", uploadId);
    }
    result = perform_request(hdl, "POST", bucket, key, subresource,
		 (const char **)query,
		 "application/xml", NULL, NULL,
                 read_func, reset_func, size_func, md5_func, read_data,
                 NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);

    g_free(subresource);

    return (result == S3_RESULT_OK);
}

void
free_s3_object(
    gpointer data)
{
    s3_object *object = data;
    g_free(object->key);
    g_free(object->uploadId);
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
    char *subresource = NULL;
    char **query = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0, 0, S3_RESULT_OK },
        { 204,  0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0, 0, /* default: */ S3_RESULT_FAIL }
        };

    if (hdl->s3_api == S3_API_AWS4) {
	query = g_new0(char *, 2);
	query[0] = g_strdup_printf("uploadId=%s", uploadId);
	query[1] = NULL;
    } else {
	subresource = g_strdup_printf("uploadId=%s", uploadId);
    }

    result = perform_request(hdl, "DELETE", bucket, key, subresource,
		 (const char **)query,
		 "application/xml", NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);

    if (hdl->s3_api == S3_API_AWS4) {
	g_free(query[0]);
	g_free(query);
    } else {
	g_free(subresource);
    }

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
    guint64 size;

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
	    if (thunk->next_marker) g_free(thunk->next_marker);
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
	thunk->object->uploadId = thunk->text;
        thunk->text = NULL;
    } else if (g_ascii_strcasecmp(element_name, "prefix") == 0 && thunk->in_common_prefixes) {
	thunk->object->prefix = thunk->text;
        thunk->text = NULL;
    } else if (g_ascii_strcasecmp(element_name, "istruncated") == 0) {
        if (thunk->text && g_ascii_strncasecmp(thunk->text, "false", 5) != 0)
            thunk->is_truncated = TRUE;
    } else if (g_ascii_strcasecmp(element_name, "nextmarker") == 0) {
        if (thunk->next_marker) g_free(thunk->next_marker);
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
        if (thunk->text) g_free(thunk->text);
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
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };
   const char* pos_parts[][2] = {
        {"delimiter", delimiter},
        {"marker", marker},
        {"max-keys", max_keys},
        {"prefix", prefix},
        {NULL, NULL}
        };
    char *esc_value;
    char **query = g_new0(char *, 6);
    char **q = query;
    guint i;

    /* loop over possible parts to build query string */
    for (i = 0; pos_parts[i][0]; i++) {
	if (pos_parts[i][1]) {
	    const char *keyword;
            esc_value = curl_escape(pos_parts[i][1], 0);
	    keyword = pos_parts[i][0];
	    if ((hdl->s3_api == S3_API_SWIFT_1 ||
		 hdl->s3_api == S3_API_SWIFT_2 ||
		 hdl->s3_api == S3_API_SWIFT_3) &&
		strcmp(keyword, "max-keys") == 0) {
		keyword = "limit";
	    } else if ((hdl->s3_api == S3_API_CASTOR) &&
                strcmp(keyword, "max-keys") == 0) {
                keyword = "size";
            }
	    *q++ = g_strdup_printf("%s=%s", keyword, esc_value);
            curl_free(esc_value);
	}
    }
    if (hdl->s3_api == S3_API_SWIFT_1 ||
        hdl->s3_api == S3_API_SWIFT_2 ||
        hdl->s3_api == S3_API_SWIFT_3 ||
        hdl->s3_api == S3_API_CASTOR) {
	*q++ = g_strdup("format=xml");
    }

    /* and perform the request on that URI */
    result = perform_request(hdl, "GET", bucket, NULL, subresource,
			     (const char **)query, NULL,
                             NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                             S3_BUFFER_WRITE_FUNCS, buf, NULL, NULL,
                             result_handling, FALSE);

    for (q = query; *q != NULL; q++) {
	g_free(*q);
    }

    return result;
}


gboolean
s3_list_keys(S3Handle *hdl,
              const char *bucket,
              const char *subresource,
              const char *prefix,
              const char *delimiter,
              GSList **list,
              guint64 *total_size)
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
    static const char *MAX_KEYS = "1000";
    struct list_keys_thunk thunk;
    GMarkupParseContext *ctxt = NULL;
    static GMarkupParser parser = { list_start_element, list_end_element, list_text, NULL, NULL };
    GError *err = NULL;
    s3_result_t result = S3_RESULT_FAIL;
    CurlBuffer buf = {NULL, 0, 0, MAX_RESPONSE_LEN, TRUE, NULL, NULL};

    g_assert(list);
    *list = NULL;
    thunk.object_list = NULL;
    thunk.object = NULL;
    thunk.text = NULL;
    thunk.next_marker = NULL;
    thunk.size = 0;

    /* Loop until S3 has given us the entire picture */
    do {
        s3_buffer_reset_func(&buf);
        /* get some data from S3 */
        result = list_fetch(hdl, bucket, subresource, prefix, delimiter, thunk.next_marker, MAX_KEYS, &buf);
        if (result != S3_RESULT_OK) goto cleanup;
	if (buf.buffer_pos == 0) goto cleanup; /* no body */

        /* run the parser over it */
        thunk.in_contents = FALSE;
        thunk.in_common_prefixes = FALSE;
        thunk.is_truncated = FALSE;
        if (thunk.next_marker) g_free(thunk.next_marker);
	thunk.next_marker = NULL;
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
        slist_free_full(thunk.object_list, free_s3_object);
        return FALSE;
    } else {
        *list = thunk.object_list;
        if(total_size) {
            *total_size = thunk.size;
        }
        return TRUE;
    }
}

gboolean
s3_init_restore(
    S3Handle *hdl,
    const char *bucket,
    const char *key)
{
    CurlBuffer data;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200, 0, 0, S3_RESULT_OK },
        { 202, 0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };

    data.buffer = "<RestoreRequest xmlns=\"http://s3.amazonaws.com/doc/2006-3-01\"> <Days>4</Days> </RestoreRequest>";
    data.buffer_len = strlen(data.buffer);
    data.buffer_pos = 0;
    data.max_buffer_size = data.buffer_len;
    data.end_of_buffer = TRUE;
    data.mutex = NULL;
    data.cond = NULL;

    result = perform_request(hdl, "POST", bucket, key, "restore", NULL,
	"application/xml", NULL, NULL,
	s3_buffer_read_func, s3_buffer_reset_func,
	s3_buffer_size_func, s3_buffer_md5_func,
	&data,
	NULL, NULL, NULL,
	NULL, NULL, result_handling, FALSE);

    return (result == S3_RESULT_OK);
}

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
    result = perform_request(hdl, "HEAD", bucket, key, NULL, NULL, NULL, NULL,
	NULL,
	NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, result_handling, FALSE);

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
        { 200, 0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };
    int timeout = 300; /* 5 minutes */

    g_assert(hdl != NULL);
    g_assert(write_func != NULL);

    while (1) {
	result = perform_request(hdl, "GET", bucket, key, NULL, NULL, NULL,
	    NULL, NULL,
            NULL, NULL, NULL, NULL, NULL, write_func, reset_func, write_data,
            progress_func, progress_data, result_handling, FALSE);
	if (!hdl->read_from_glacier ||
	    result != S3_RESULT_FAIL ||
	    hdl->last_response_code != 403 ||
	    hdl->last_s3_error_code != S3_ERROR_InvalidObjectState) {
	    break;
	}
	/* retry if a restore from glacier is ongoing */
	sleep(timeout);
    }

    return result == S3_RESULT_OK;
}

gboolean
s3_read_range(S3Handle *hdl,
        const char *bucket,
        const char *key,
	const guint64 range_begin,
	const guint64 range_end,
        s3_write_func write_func,
        s3_reset_func reset_func,
        gpointer write_data,
        s3_progress_func progress_func,
        gpointer progress_data)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200, 0, 0, S3_RESULT_OK },
        { 206, 0, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,   0, 0, /* default: */ S3_RESULT_FAIL  }
        };
    struct curl_slist *headers = NULL;
    char *buf;
    int timeout = 300; /* 5 minutes */

    g_assert(hdl != NULL);
    g_assert(write_func != NULL);

    buf = g_strdup_printf("Range: bytes=%llu-%llu",
			  (long long unsigned)range_begin,
			  (long long unsigned) range_end);
    headers = curl_slist_append(headers, buf);
    g_free(buf);

    while(1) {
	result = perform_request(hdl, "GET", bucket, key, NULL, NULL, NULL,
	    NULL, headers,
            NULL, NULL, NULL, NULL, NULL, write_func, reset_func, write_data,
            progress_func, progress_data, result_handling, FALSE);

	if (!hdl->read_from_glacier ||
	    result != S3_RESULT_FAIL ||
	    hdl->last_response_code != 403 ||
	    hdl->last_s3_error_code != S3_ERROR_InvalidObjectState) {
	    break;
	}
	/* retry if a restore from glacier is ongoing */
	sleep(timeout);
    }

    curl_slist_free_all(headers);
    return result == S3_RESULT_OK;
}

gboolean
s3_delete(S3Handle *hdl,
          const char *bucket,
          const char *key)
{
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,                     0, S3_RESULT_OK },
        { 204,  0,                     0, S3_RESULT_OK },
        { 404,  0,                     0, S3_RESULT_OK },
        { 404,  S3_ERROR_NoSuchBucket, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 409,  0,                     0, S3_RESULT_OK },
        { 0,    0,                     0, /* default: */ S3_RESULT_FAIL  }
        };

    g_assert(hdl != NULL);

    result = perform_request(hdl, "DELETE", bucket, key, NULL, NULL,
		 "application/xml", NULL, NULL,
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);

    return result == S3_RESULT_OK;
}

int
s3_multi_delete(S3Handle *hdl,
		const char *bucket,
		GSList *objects)
{
    GString *query;
    CurlBuffer data;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,                     0, S3_RESULT_OK },
        { 204,  0,                     0, S3_RESULT_OK },
        { 400,  0,                     0, S3_RESULT_NOTIMPL },
        { 403,  0,                     0, S3_RESULT_NOTIMPL },
        { 404,  S3_ERROR_NoSuchBucket, 0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0,    0,                     0, /* default: */ S3_RESULT_FAIL  }
        };

    g_assert(hdl != NULL);

    query = g_string_new(NULL);
    if (hdl->s3_api == S3_API_SWIFT_1 ||
	hdl->s3_api == S3_API_SWIFT_2 ||
	hdl->s3_api == S3_API_SWIFT_3) {
	char *container = s3_uri_encode(bucket, TRUE);
	char *cmd = "DELETE";
	while (objects != NULL) {
	    s3_object *object = objects->data;
	    char *name = s3_uri_encode(object->key, TRUE);
	    g_string_append_printf(query, "%s/%s\n", container, name);
	    objects = objects->next;
	}

	data.buffer_len = query->len;
	data.buffer = query->str;
	data.buffer_pos = 0;
	data.max_buffer_size = data.buffer_len;
	data.end_of_buffer = TRUE;
	data.mutex = NULL;
	data.cond = NULL;

	if (hdl->s3_api == S3_API_SWIFT_3)
	    cmd = "POST";
	result = perform_request(hdl, cmd, "", NULL, "bulk-delete", NULL,
		 "text/plain", NULL, NULL,
		 s3_buffer_read_func, s3_buffer_reset_func,
		 s3_buffer_size_func, s3_buffer_md5_func,
		 &data, NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);

    } else {
	g_string_append(query, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
	g_string_append(query, "<Delete>\n");
	if (!hdl->verbose) {
	    g_string_append(query, "  <Quiet>true</Quiet>\n");
	}
	while (objects != NULL) {
	    s3_object *object = objects->data;
	    g_string_append(query, "  <Object>\n");
	    g_string_append(query, "    <Key>");
	    g_string_append(query, object->key);
	    g_string_append(query, "</Key>\n");
	    g_string_append(query, "  </Object>\n");
	    objects = objects->next;
	}
	g_string_append(query, "</Delete>\n");

	data.buffer_len = query->len;
	data.buffer = query->str;
	data.buffer_pos = 0;
	data.max_buffer_size = data.buffer_len;
	data.end_of_buffer = TRUE;
	data.mutex = NULL;
	data.cond = NULL;

	result = perform_request(hdl, "POST", bucket, NULL, "delete", NULL,
		 "application/xml", NULL, NULL,
		 s3_buffer_read_func, s3_buffer_reset_func,
		 s3_buffer_size_func, s3_buffer_md5_func,
		 &data, NULL, NULL, NULL, NULL, NULL,
                 result_handling, FALSE);
    }

    g_string_free(query, TRUE);
    if (result == S3_RESULT_OK) {
	return 1;
    } else if (result == S3_RESULT_NOTIMPL) {
	s3_new_curl(hdl);
	return 2;
    } else {
	return 0;
    }
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
    static result_handling_t result_handling[] = {
        { 200,  0,                    0, S3_RESULT_OK },
        { 201,  0,                    0, S3_RESULT_OK },
        { 202,  0,                    0, S3_RESULT_OK },
        { 204,  0,                    0, S3_RESULT_OK },
        { 404, S3_ERROR_NoSuchBucket, 0, S3_RESULT_RETRY },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        };
    regmatch_t pmatch[4];
    CurlBuffer buf = {NULL, 0, 0, 0, TRUE, NULL, NULL}, *ptr = NULL;
    s3_read_func read_func = NULL;
    s3_reset_func reset_func = NULL;
    s3_md5_func md5_func = NULL;
    s3_size_func size_func = NULL;
    GString *CreateBucketConfiguration;
    gboolean add_create = FALSE;

    g_assert(hdl != NULL);

    CreateBucketConfiguration = g_string_new("<CreateBucketConfiguration");
    if (g_str_equal(hdl->host, "gss.iijgio.com")) {
	g_string_append(CreateBucketConfiguration,
			" xmlns=\"http://acs.iijgio.com/doc/2006-03-01/\"");
    }
    g_string_append(CreateBucketConfiguration, ">");
    if (is_non_empty_string(hdl->bucket_location) &&
	strcmp(hdl->bucket_location, "us-east-1") != 0 &&
        !g_str_equal(AMAZON_WILDCARD_LOCATION, hdl->bucket_location)) {
        if (s3_bucket_location_compat(bucket)) {
	    g_string_append_printf(CreateBucketConfiguration,
			    "<LocationConstraint>%s</LocationConstraint>",
			    hdl->bucket_location);
	    add_create = TRUE;
        } else {
            hdl->last_message = g_strdup_printf(_(
                "Location constraint given for %s, "
                "but the bucket name (%s) is not usable as a subdomain."), S3_bucket_name[hdl->s3_api], bucket);
	    g_string_free(CreateBucketConfiguration, TRUE);
            return FALSE;
        }
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

    if (add_create) {
	ptr = &buf;
	buf.buffer = g_string_free(CreateBucketConfiguration, FALSE);
	buf.buffer_len = (guint) strlen(buf.buffer);
	buf.buffer_pos = 0;
	buf.max_buffer_size = buf.buffer_len;
	read_func = s3_buffer_read_func;
	reset_func = s3_buffer_reset_func;
	size_func = s3_buffer_size_func;
	md5_func = s3_buffer_md5_func;
    } else {
	g_string_free(CreateBucketConfiguration, TRUE);
    }
    result = perform_request(hdl, verb, bucket, NULL, NULL,
		 NULL, content_type, project_id, NULL,
                 read_func, reset_func, size_func, md5_func, ptr,
                 NULL, NULL, NULL, NULL, NULL, result_handling, FALSE);
   if (ptr)
	g_free(ptr->buffer);

   if (result == S3_RESULT_OK ||
       (result != S3_RESULT_OK &&
        hdl->last_s3_error_code == S3_ERROR_BucketAlreadyOwnedByYou)) {
        /* verify the that the location constraint on the existing bucket matches
         * the one that's configured.
         */
	if (is_non_empty_string(hdl->bucket_location)) {
            result = perform_request(hdl, "GET", bucket, NULL, "location", NULL, NULL, NULL, NULL,
                                     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                     NULL, NULL, result_handling, FALSE);
	} else {
            result = perform_request(hdl, "GET", bucket, NULL, NULL, NULL, NULL, NULL, NULL,
                                     NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                                     NULL, NULL, result_handling, FALSE);
	}

        if (result == S3_RESULT_OK && is_non_empty_string(hdl->bucket_location)) {
            /* return to the default state of failure */
            result = S3_RESULT_FAIL;

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
                char *loc_end_open = find_regex_substring(body, pmatch[1]);
                char *loc_content = find_regex_substring(body, pmatch[3]);

                /* The case of an empty string is special because XML allows
                 * "self-closing" tags
                 */
                if (g_str_equal(AMAZON_WILDCARD_LOCATION,
                                hdl->bucket_location) &&
                    '/' != loc_end_open[0])
                    hdl->last_message = g_strdup(_("A wildcard location constraint is "
                        "configured, but the bucket has a non-empty location constraint"));
                else if (!g_str_equal(AMAZON_WILDCARD_LOCATION,
                                      hdl->bucket_location)?
                    strncmp(loc_content, hdl->bucket_location, strlen(hdl->bucket_location)) :
                    ('\0' != loc_content[0]))
                    hdl->last_message = g_strdup_printf("The location constraint configured (%s) "
                        "does not match the constraint currently on the bucket (%s)", hdl->bucket_location, loc_content);
                else
                    result = S3_RESULT_OK;
		g_free(loc_end_open);
		g_free(loc_content);
            } else {
                hdl->last_message = g_strdup_printf(_("Unexpected location response from %s"), S3_name[hdl->s3_api]);
            }
        }
   }

cleanup:
    if (body) g_free(body);

    return result == S3_RESULT_OK;

}

static s3_result_t
oauth2_get_access_token(
    S3Handle *hdl)
{
    GString *query;
    CurlBuffer data;
    s3_result_t result = S3_RESULT_FAIL;
    static result_handling_t result_handling[] = {
        { 200,  0,                    0, S3_RESULT_OK },
        { 204,  0,                    0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        };
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

    data.buffer_len = query->len;
    data.buffer = query->str;
    data.buffer_pos = 0;
    data.max_buffer_size = data.buffer_len;
    data.end_of_buffer = TRUE;
    data.mutex = NULL;
    data.cond = NULL;

    hdl->x_storage_url = "https://accounts.google.com/o/oauth2/token";
    hdl->getting_oauth2_access_token = 1;
    result = perform_request(hdl, "POST", NULL, NULL, NULL, NULL,
			     "application/x-www-form-urlencoded", NULL, NULL,
			     s3_buffer_read_func, s3_buffer_reset_func,
			     s3_buffer_size_func, s3_buffer_md5_func,
                             &data, NULL, NULL, NULL, NULL, NULL,
			     result_handling, FALSE);
    hdl->x_storage_url = NULL;
    hdl->getting_oauth2_access_token = 0;

    /* use strndup to get a null-terminated string */
    body = g_strndup(hdl->last_response_body, hdl->last_response_body_size);
    if (!body) {
        hdl->last_message = g_strdup(_("No body received for location request"));
        goto cleanup;
    } else if ('\0' == body[0]) {
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
    return result == S3_RESULT_OK;
}

gboolean
s3_is_bucket_exists(S3Handle *hdl,
		    const char *bucket,
		    const char *prefix,
		    const char *project_id)
{
    s3_result_t result = S3_RESULT_FAIL;
    char **query = g_new0(char *, 3);
    char **q = query;
    static result_handling_t result_handling[] = {
        { 200,  0,                    0, S3_RESULT_OK },
        { 204,  0,                    0, S3_RESULT_OK },
        RESULT_HANDLING_ALWAYS_RETRY,
        { 0, 0,                       0, /* default: */ S3_RESULT_FAIL  }
        };

    if (hdl->s3_api == S3_API_SWIFT_1 ||
	hdl->s3_api == S3_API_SWIFT_2 ||
	hdl->s3_api == S3_API_SWIFT_3) {
	*q++ = g_strdup("limit=1");
    } else if (hdl->s3_api == S3_API_CASTOR) {
        *q++ = g_strdup("format=xml");
	*q++ = g_strdup("size=0");
    } else if (prefix) {
	char *q_prefix = curl_escape(prefix, 0);
        *q++ = g_strdup("max-keys=1");
        *q++ = g_strdup_printf("prefix=%s", q_prefix);
	g_free(q_prefix);
    } else {
        *q++ = g_strdup("max-keys=1");
    }

    result = perform_request(hdl, "GET", bucket, NULL, NULL,
			     (const char **)query,
			     NULL, project_id, NULL,
                             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);

    for (q = query; *q != NULL; q++) {
	g_free(*q);
    }

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
        if (thunk->text) g_free(thunk->text);
        thunk->text = g_strndup(text, text_len);
    }
}


gboolean
s3_get_lifecycle(
    S3Handle *hdl,
    const char *bucket,
    GSList **lifecycle)
{
    s3_result_t result = S3_RESULT_FAIL;
    CurlBuffer buf = {NULL, 0, 0, 100000, TRUE, NULL, NULL};
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

    result = perform_request(hdl, "GET", bucket, NULL, "lifecycle", NULL,
			     NULL, NULL, NULL,
                             NULL, NULL, NULL, NULL, NULL,
			     S3_BUFFER_WRITE_FUNCS, &buf,
                             NULL, NULL, result_handling, FALSE);

    if (result == S3_RESULT_FAIL &&
	hdl->last_response_code == 404 &&
	hdl->last_s3_error_code == S3_ERROR_NoSuchLifecycleConfiguration) {
	result = S3_RESULT_OK;
	return TRUE;
    }
    if (result != S3_RESULT_OK) goto cleanup;
    if (buf.buffer_pos == 0) goto cleanup;

    /* run the parser over it */

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

    if (thunk.error) {
	if (hdl->last_message) g_free(hdl->last_message);
	hdl->last_message = thunk.error;
	thunk.error = NULL;
	result = S3_RESULT_FAIL;
	goto cleanup;
    }

cleanup:
    if (err) g_error_free(err);
    if (thunk.text) g_free(thunk.text);
    if (ctxt) g_markup_parse_context_free(ctxt);
    if (buf.buffer) g_free(buf.buffer);

    if (result == S3_RESULT_OK) {
	*lifecycle = thunk.lifecycle;
    } else {
	free_lifecycle(thunk.lifecycle);
    }
    return result == S3_RESULT_OK;
}

gboolean
s3_put_lifecycle(
    S3Handle *hdl,
    const char *bucket,
    GSList *lifecycle)
{
    s3_result_t result = S3_RESULT_FAIL;
    CurlBuffer buf = {NULL, 0, 0, 0, TRUE, NULL, NULL};
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

    buf.buffer = g_string_free(body, FALSE);
    buf.buffer_len = strlen(buf.buffer);

    s3_verbose(hdl, 1);
    result = perform_request(hdl, "PUT", bucket, NULL, "lifecycle", NULL,
			     "application/xml", NULL, NULL,
                             S3_BUFFER_READ_FUNCS, &buf,
			     NULL, NULL, NULL,
                             NULL, NULL, result_handling, FALSE);

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
