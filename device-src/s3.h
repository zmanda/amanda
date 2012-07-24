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

#ifndef __S3_H__
#define __S3_H__
#include <glib.h>
#include <curl/curl.h>

/*
 * Data types
 */

typedef enum {
   S3_API_S3,
   S3_API_SWIFT_1,
   S3_API_SWIFT_2,
   S3_API_OAUTH2
} S3_api;

/* An opaque handle.  S3Handles should only be accessed from a single
 * thread at any given time, although it is fine to use different handles
 * in different threads simultaneously. */
typedef struct S3Handle S3Handle;

/* Callback function to read data to upload
 * 
 * @note this is the same as CURLOPT_READFUNCTION
 *
 * @param data: The pointer to write data to
 * @param size: The size of each "element" of the data buffer in bytes
 * @param nmemb: The number of elements in the data buffer.
 * So, the buffer's size is size*nmemb bytes.
 * @param stream: The read_data (an opaque pointer)
 *
 * @return The number of bytes written to the buffer,
 * CURL_READFUNC_PAUSE to pause, or CURL_READFUNC_ABORT to abort.
 * Return 0 only if there's no more data to be uploaded.
 */
typedef size_t (*s3_read_func)(void *data, size_t size, size_t nmemb, void *stream);

/* This function is called to get size of the upload data
 *
 * @param data: The write_data (opaque pointer)
 *
 * @return The number of bytes of data, negative for error
 */
typedef size_t (*s3_size_func)(void *data);

/* This function is called to get MD5 hash of the upload data
 *
 * @param data: The write_data (opaque pointer)
 *
 * @return The MD5 hash, NULL on error
 */
typedef GByteArray* (*s3_md5_func)(void *data);

/* This function is called to reset an upload or download data stream
 * to the beginning
 *
 * @param data: The read_data or write_data (opaque pointer)
 *
 * @return The number of bytes of data, negative for error
 */
typedef void (*s3_reset_func)(void *data);

/* Callback function to write data that's been downloaded
 * 
 * @note this is the same as CURLOPT_WRITEFUNCTION
 *
 * @param data: The pointer to read data from
 * @param size: The size of each "element" of the data buffer in bytes
 * @param nmemb: The number of elements in the data buffer.
 * So, the buffer's size is size*nmemb bytes.
 * @param stream: the write_data (an opaque pointer)
 *
 * @return The number of bytes written to the buffer or
 * CURL_WRITEFUNC_PAUSE to pause.
 * If it's the number of bytes written, it should match the buffer size
 */
typedef size_t (*s3_write_func)(void *data, size_t size, size_t nmemb, void *stream);

/**
 * Callback function to track progress
 *
 * @note this is the same as CURLOPT_PROGRESSFUNCTION
 *
 * @param data: The progress_data
 * @param dltotal: The total number of bytes to downloaded
 * @param dlnow: The current number of bytes downloaded
 * @param ultotal: The total number of bytes to downloaded
 * @param ulnow: The current number of bytes downloaded
 *
 * @return 0 to continue, non-zero to abort.
 */
typedef curl_progress_callback s3_progress_func;

/*
 * Constants
 */

/* These are assumed to be already URL-escaped. */
# define STS_BASE_URL "https://ls.amazonaws.com/"
# define STS_PRODUCT_TOKEN "{ProductToken}AAAGQXBwVGtu4geoGybuwuk8VEEPzJ9ZANpu0yzbf9g4Gs5Iarzff9B7qaDBEEaWcAzWpcN7zmdMO765jOtEFc4DWTRNkpPSzUnTdkHbdYUamath73OreaZtB86jy/JF0gsHZfhxeKc/3aLr8HNT//DsX3r272zYHLDPWWUbFguOwqNjllnt6BshYREx59l8RrWABLSa37dyJeN+faGvz3uQxiDakZRn3LfInOE6d9+fTFl50LPoP08LCqI/SJfpouzWix7D/cep3Jq8yYNyM1rgAOTF7/wh7r8OuPDLJ/xZUDLfykePIAM="

/* This preprocessor magic will enumerate constants named S3_ERROR_XxxYyy for
 * each of the errors in parentheses.
 *
 * see http://docs.amazonwebservices.com/AmazonS3/latest/API/ErrorResponses.html
 * for Amazon's breakdown of error responses.
 */
#define S3_ERROR_LIST \
    S3_ERROR(None), \
    S3_ERROR(AccountProblem), \
    S3_ERROR(AllAccessDisabled), \
    S3_ERROR(AmbiguousGrantByEmailAddress), \
    S3_ERROR(OperationAborted), \
    S3_ERROR(BadDigest), \
    S3_ERROR(BucketAlreadyExists), \
    S3_ERROR(BucketAlreadyOwnedByYou), \
    S3_ERROR(BucketNotEmpty), \
    S3_ERROR(CredentialsNotSupported), \
    S3_ERROR(EntityTooLarge), \
    S3_ERROR(IncompleteBody), \
    S3_ERROR(InternalError), \
    S3_ERROR(InvalidAccessKeyId), \
    S3_ERROR(InvalidArgument), \
    S3_ERROR(InvalidBucketName), \
    S3_ERROR(InvalidDigest), \
    S3_ERROR(InvalidRange), \
    S3_ERROR(InvalidSecurity), \
    S3_ERROR(InvalidSOAPRequest), \
    S3_ERROR(InvalidStorageClass), \
    S3_ERROR(InvalidTargetBucketForLogging), \
    S3_ERROR(KeyTooLong), \
    S3_ERROR(InvalidURI), \
    S3_ERROR(MalformedACLError), \
    S3_ERROR(MaxMessageLengthExceeded), \
    S3_ERROR(MetadataTooLarge), \
    S3_ERROR(MethodNotAllowed), \
    S3_ERROR(MissingAttachment), \
    S3_ERROR(MissingContentLength), \
    S3_ERROR(MissingSecurityElement), \
    S3_ERROR(MissingSecurityHeader), \
    S3_ERROR(NoLoggingStatusForKey), \
    S3_ERROR(NoSuchBucket), \
    S3_ERROR(NoSuchEntity), \
    S3_ERROR(NoSuchKey), \
    S3_ERROR(NotImplemented), \
    S3_ERROR(NotSignedUp), \
    S3_ERROR(PreconditionFailed), \
    S3_ERROR(RequestTimeout), \
    S3_ERROR(RequestTimeTooSkewed), \
    S3_ERROR(RequestTorrentOfBucketError), \
    S3_ERROR(SignatureDoesNotMatch), \
    S3_ERROR(TooManyBuckets), \
    S3_ERROR(UnexpectedContent), \
    S3_ERROR(UnresolvableGrantByEmailAddress), \
    S3_ERROR(Unknown), \
    S3_ERROR(Unauthorized), \
    S3_ERROR(Created), \
    S3_ERROR(Accepted), \
    S3_ERROR(Forbidden), \
    S3_ERROR(Conflict), \
    S3_ERROR(AuthenticationRequired), \
    S3_ERROR(END)

typedef enum {
#define S3_ERROR(NAME) S3_ERROR_ ## NAME
    S3_ERROR_LIST
#undef S3_ERROR
} s3_error_code_t;

/*
 * Functions
 */

/* Does this install of curl support SSL?
 *
 * @returns: boolean
 */
gboolean
s3_curl_supports_ssl(void);

/* Checks if the version of libcurl being used supports and checks
 * wildcard certificates correctly (used for the subdomains required
 * by location constraints).
 *
 * @returns: true if the version of libcurl is new enough
 */
gboolean
s3_curl_location_compat(void);

/* Checks if a bucket name is compatible with setting a location
 * constraint.
 *
 * @note This doesn't guarantee that bucket name is entirely valid,
 * just that using it as one (or more) subdomain(s) of s3.amazonaws.com
 * won't fail; that would prevent the reporting of useful messages from
 * the service.
 *
 * @param bucket: the bucket name
 * @returns: true if the bucket name is compatible
 */
gboolean
s3_bucket_location_compat(const char *bucket);

/* Initialize S3 operation
 *
 * If an error occurs in this function, diagnostic information is
 * printed to stderr.
 *
 * @returns: false if an error occurred
 */
gboolean
s3_init(void);

/* Set up an S3Handle.
 *
 * The concept of a bucket is defined by the Amazon S3 API.
 * See: "Components of Amazon S3" - API Version 2006-03-01 pg. 8
 *
 * @param access_key: the secret key for Amazon Web Services
 * @param secret_key: the secret key for Amazon Web Services
 * @param user_token: the user token for Amazon DevPay
 * @param bucket_location: the location constraint for buckets
 * @param storage_class: the storage class for new objects
 * @param ca_info: the path to pass to libcurl as the certificate authority.
 *                 see curl_easy_setopt() CURLOPT_CAINFO for more
 * @returns: the new S3Handle
 */
S3Handle *
s3_open(const char * access_key, const char *secret_key,
	const char *swift_account_id, const char *swift_access_key,
	const char *host,
        const char *service_path, gboolean use_subdomain,
        const char * user_token,
        const char * bucket_location, const char * storage_class,
	const char * ca_info, const char * server_side_encryption,
	const char *proxy,
	const S3_api s3_api,
	const char *username,
	const char *password,
	const char *tenant_id,
	const char *tenant_name,
	const char *client_id,
	const char *client_secret,
	const char *refresh_token,
	const gboolean reuse_connection);

/* latest step of setting up the S3Handle.
 *
 * Must be done after all properties are set.
 *
 * @param hdl: the S3Handle to set up.
 * @returns: false if an error occured
 */
gboolean
s3_open2(S3Handle *hdl);

/* Deallocate an S3Handle
 *
 * @param hdl: the S3Handle object
 */
void
s3_free(S3Handle *hdl);

/* Reset the information about the last request, including
 * freeing any allocated memory.  The S3Handle itself is not
 * freed and may be used again.  This function is called
 * automatically as needed, and should be called to free memory
 * when the handle will not be used for some time.
 *
 * @param hdl: the S3Handle object
 */
void
s3_reset(S3Handle *hdl);

/* Get the error information for the last operation
 *
 * All results are returned via result parameters.  If any parameter is
 * NULL, that result will not be returned.  Caller is not responsible for
 * freeing any returned strings, although the results are only valid until
 * the next call to an S3 function with this handle.
 *
 * @param hdl: the S3Handle object
 * @param message: (result) the error message, or NULL if none exists
 * @param response_code: (result) the HTTP response code (or 0 if none exists)
 * @param s3_error_code: (result) the S3 error code (see constants, above)
 * @param s3_error_name: (result) the S3 error name (e.g., "RequestTimeout"),
 * or NULL if none exists
 * @param curl_code: (result) the curl error code (or 0 if none exists)
 * @param num_retries: (result) number of retries
 */
void
s3_error(S3Handle *hdl,
         const char **message,
         guint *response_code,
         s3_error_code_t *s3_error_code,
         const char **s3_error_name,
         CURLcode *curl_code,
         guint *num_retries);

/* Control verbose output of HTTP transactions, etc.
 *
 * @param hdl: the S3Handle object
 * @param verbose: if true, send HTTP transactions, etc. to debug output
 */
void
s3_verbose(S3Handle *hdl,
       gboolean verbose);

/* Control the use of SSL with HTTP transactions.
 *
 * @param hdl: the S3Handle object
 * @param use_ssl: if true, use SSL (if curl supports it)
 * @returns: true if the setting is valid
 */
gboolean
s3_use_ssl(S3Handle *hdl, gboolean use_ssl);

/* Control the throttling of S3 uploads.  Only supported with curl >= 7.15.5.
 *
 * @param hdl: the S3Handle object
 * @param max_send_speed: max speed (bytes/sec) at which to send
 * @returns: true if the setting is valid
 */
gboolean
s3_set_max_send_speed(S3Handle *hdl, guint64 max_send_speed);

/* Control the throttling of S3 downloads.  Only supported with curl >= 7.15.5.
 *
 * @param hdl: the S3Handle object
 * @param max_recv_speed: max speed (bytes/sec) at which to receive
 * @returns: true if the setting is valid
 */
gboolean
s3_set_max_recv_speed(S3Handle *hdl, guint64 max_recv_speed);

/* Get the error information from the last operation on this handle,
 * formatted as a string.
 *
 * Caller is responsible for freeing the resulting string.
 *
 * @param hdl: the S3Handle object
 * @returns: string, or NULL if no error occurred
 */
char *
s3_strerror(S3Handle *hdl);

/* Perform an upload.
 *
 * When this function returns, KEY and BUFFER remain the
 * responsibility of the caller.
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to which the upload should be made
 * @param key: the key to which the upload should be made
 * @param read_func: the callback for reading data
 * @param reset_func: the callback for to reset reading data
 * @param size_func: the callback to get the number of bytes to upload
 * @param md5_func: the callback to get the MD5 hash of the data to upload
 * @param read_data: pointer to pass to the above functions
 * @param progress_func: the callback for progress information
 * @param progress_data: pointer to pass to C{progress_func}
 *
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
          gpointer progress_data);

/* List all of the files matching the pseudo-glob C{PREFIX*DELIMITER*},
 * returning only that portion which matches C{PREFIX*DELIMITER}.  S3 supports
 * this particular semantics, making it quite efficient.  The returned list
 * should be freed by the caller.
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to list
 * @param prefix: the prefix
 * @param delimiter: delimiter (any length string)
 * @param list: (output) the list of files
 * @param total_size: (output) sum of size of files 
 * @returns: FALSE if an error occurs
 */
gboolean
s3_list_keys(S3Handle *hdl,
              const char *bucket,
              const char *prefix,
              const char *delimiter,
              GSList **list,
              guint64 *total_size);

/* Read an entire file, passing the contents to write_func buffer
 * by buffer.
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to read from
 * @param key: the key to read from
 * @param write_func: the callback for writing data
 * @param reset_func: the callback for to reset writing data
 * @param write_data: pointer to pass to C{write_func}
 * @param progress_func: the callback for progress information
 * @param progress_data: pointer to pass to C{progress_func}
 * @returns: FALSE if an error occurs
 */
gboolean
s3_read(S3Handle *hdl,
        const char *bucket,
        const char *key,
        s3_write_func write_func,
        s3_reset_func reset_func,
        gpointer write_data,
        s3_progress_func progress_func,
        gpointer progress_data);

/* Delete a file.
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to delete from
 * @param key: the key to delete
 * @returns: FALSE if an error occurs; a non-existent file is I{not} considered an error.
 */
gboolean
s3_delete(S3Handle *hdl,
          const char *bucket,
          const char *key);

/* Delete multiple file.
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to delete from
 * @param key: the key array to delete
 * @returns: 0 on sucess, 1 if multi_delete is not supported, 2 if an error
 *           occurs; a non-existent file is I{not} considered an error.
 */
int
s3_multi_delete(S3Handle *hdl,
                const char *bucket,
                const char **key);

/* Create a bucket.
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to create
 * @returns: FALSE if an error occurs
 */
gboolean
s3_make_bucket(S3Handle *hdl,
               const char *bucket,
	       const char *project_id);

/* Check if a bucket exists.
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to create
 * @returns: FALSE if an error occur
 */
gboolean
s3_is_bucket_exists(S3Handle *hdl,
                    const char *bucket,
		    const char *project_id);

/* Delete a bucket
 *
 * @note A bucket can not be deleted if it still contains keys
 *
 * @param hdl: the S3Handle object
 * @param bucket: the bucket to delete
 * @returns: FALSE if an error occurs
 */
gboolean
s3_delete_bucket(S3Handle *hdl,
                 const char *bucket);

/* Attempt a RefreshAWSSecurityToken on a token; if it succeeds, the old
 * token will be freed and replaced by the new. If it fails, the old
 * token is left unchanged and FALSE is returned. */
gboolean sts_refresh_token(char ** token, const char * directory);

/* These functions are for if you want to use curl on your own. You get more
 * control, but it's a lot of work that way: */
typedef struct {
    char *buffer;
    guint buffer_len;
    guint buffer_pos;
    guint max_buffer_size;
} CurlBuffer;

#define S3_BUFFER_READ_FUNCS s3_buffer_read_func, s3_buffer_reset_func, s3_buffer_size_func, s3_buffer_md5_func

#define S3_BUFFER_WRITE_FUNCS s3_buffer_write_func, s3_buffer_reset_func

/* a CURLOPT_READFUNCTION to read data from a buffer. */
size_t
s3_buffer_read_func(void *ptr, size_t size, size_t nmemb, void * stream);

size_t
s3_buffer_size_func(void *stream);

GByteArray*
s3_buffer_md5_func(void *stream);

void
s3_buffer_reset_func(void *stream);

#define S3_EMPTY_READ_FUNCS s3_empty_read_func, NULL, s3_empty_size_func, s3_empty_md5_func

/* a CURLOPT_WRITEFUNCTION to write data to a buffer. */
size_t
s3_buffer_write_func(void *ptr, size_t size, size_t nmemb, void *stream);

/* a CURLOPT_READFUNCTION that writes nothing. */
size_t
s3_empty_read_func(void *ptr, size_t size, size_t nmemb, void * stream);

size_t
s3_empty_size_func(void *stream);

GByteArray*
s3_empty_md5_func(void *stream);

#define S3_COUNTER_WRITE_FUNCS s3_counter_write_func, s3_counter_reset_func

/* a CURLOPT_WRITEFUNCTION to write data that just counts data.
 * s3_write_data should be NULL or a pointer to an gint64.
 */
size_t
s3_counter_write_func(void *ptr, size_t size, size_t nmemb, void *stream);

void
s3_counter_reset_func(void *stream);

#ifdef _WIN32
/* a CURLOPT_READFUNCTION to read data from a file. */
size_t
s3_file_read_func(void *ptr, size_t size, size_t nmemb, void * stream);

size_t
s3_file_size_func(void *stream);

GByteArray*
s3_file_md5_func(void *stream);

size_t
s3_file_reset_func(void *stream);

/* a CURLOPT_WRITEFUNCTION to write data to a file. */
size_t
s3_file_write_func(void *ptr, size_t size, size_t nmemb, void *stream);
#endif

/* Adds a null termination to a buffer. */
void terminate_buffer(CurlBuffer *);

#endif
