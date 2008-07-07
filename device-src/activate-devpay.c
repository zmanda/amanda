/* This program creates the token and certificate files for Amazon Devpay's
 * Simple Token Service (STS). Right now you can then use those files with
 * the S3 device. */

#include "amanda.h"

#include <curl/curl.h>
#include <glib.h>

#include <errno.h>
#include <getopt.h>

#include "base64.h"
#include "s3.h"

#define MAX_RESPONSE_SIZE (1024*1024)

typedef struct {
    GString * user_token;
    GString * access_key;
    GString * secret_key;
} Credentials;

static void usage(void) {
    g_fprintf(stderr,
"USAGE: activate-devpay KEY [ >> amanda.conf ]\n"
"  This tool uses an Amazon Devpay activation key to retrieve an\n"
"  user token, access key, and secret key for use with Amazon S3. Output\n"
"  is in a form suitable for placement in an Amanda configuration file\n");

    exit(EXIT_FAILURE);
}

/* This function is **not** thread-safe. Sorry. */
static const char * parse_commandline(int argc, char ** argv) {
    if (argc != 2) {
        usage();
        return NULL;
    } else {
        return argv[1];
    }
}

static char * activation_url(const char *key) {
    char * url;
    char * encoded_key;
    
    encoded_key = curl_escape(key, 0);
    url = g_strdup_printf(STS_BASE_URL "?Action=ActivateDesktopProduct&ActivationKey=%s&ProductToken=" STS_PRODUCT_TOKEN "&Version=2007-06-05", encoded_key);
    curl_free(encoded_key);

    return url;
}

/* This function is a CURLOPT_WRITEFUNCTION and a wrapper for
   g_markup_parse_context_parse(). It's not very smart about errors. */
static size_t libcurl_gmarkup_glue(void *ptr, size_t size1, size_t size2,
                                   void *stream) {
    GMarkupParseContext *context = stream;
    /* If this overflows, we have real problems, because we are expected to
     * return the result of this multiplication in a size_t. */
    size_t read_size = size1 * size2;
    GError * error = NULL;

    read_size = size1 * size2;

    if (g_markup_parse_context_parse(context, ptr, read_size, &error)) {
        return read_size;
    } else {
        if (error == NULL) {
            g_fprintf(stderr, "Internal error parsing XML.\n");
        } else {
            g_fprintf(stderr, "Error parsing XML: %s\n",
                    error->message);
            g_error_free(error);
        }
        exit(EXIT_FAILURE);
    }
}

static void do_server_stuff(const char * key, GMarkupParseContext * parser) {
    char * url;
    CURL* handle;
    char curl_error_buffer[CURL_ERROR_SIZE];

    handle = curl_easy_init();
    
    curl_easy_setopt(handle, CURLOPT_NOPROGRESS, TRUE);
    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, TRUE);
    curl_easy_setopt(handle, CURLOPT_AUTOREFERER, TRUE);
    curl_easy_setopt(handle, CURLOPT_ENCODING, ""); /* Support everything. */
#ifdef CURLOPT_MAXFILESIZE
    curl_easy_setopt(handle, CURLOPT_MAXFILESIZE, MAX_RESPONSE_SIZE);
#endif
    curl_easy_setopt(handle, CURLOPT_ERRORBUFFER, curl_error_buffer);

    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, libcurl_gmarkup_glue);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, parser);
    url = activation_url(key);
    curl_easy_setopt(handle, CURLOPT_URL, url);

    if (curl_easy_perform(handle) != 0) {
        g_error("Problem fetching data from server:\n%s",
                curl_error_buffer);
        exit(EXIT_FAILURE);
    }
    
    g_free(url);
}

static void parser_got_text(GMarkupParseContext * context,
                            const char * text,
                            size_t text_len,
                            gpointer user_data,
                            GError ** error) {
    Credentials * rval = user_data;

    const char * current_tag = g_markup_parse_context_get_element(context);

    g_assert(rval != NULL);
    g_assert(*error == NULL);

    /* We use strrstr instead of strcmp because Amazon uses namespaces
     * that I don't want to deal with. */
    if (g_strrstr(current_tag, "UserToken")) {
        g_string_append_len(rval->user_token, text, text_len);
        return;
    } else if (g_strrstr(current_tag, "AWSAccessKeyId")) {
        g_string_append_len(rval->access_key, text, text_len);
        return;
    } else if (g_strrstr(current_tag, "SecretAccessKey")) {
        g_string_append_len(rval->secret_key, text, text_len);
        return;
    } else if (g_strrstr(current_tag, "Code")) {
        /* Is it a code we know? */
        if (strncmp(text, "ExpiredActivationKey", text_len) == 0) {
            g_set_error(error, G_MARKUP_ERROR, -1,
                        "Activation key has expired; get a new one.");
        } else if (strncmp(text, "InvalidActivationKey", text_len) == 0) {
            g_set_error(error, G_MARKUP_ERROR, -1,
                        "Activation key is not valid; double-check.");
        } else {
            /* Do nothing; wait for the message. */
        }
    } else if (g_strrstr(current_tag, "Message")) {
        g_set_error(error, G_MARKUP_ERROR, -1, "%.*s", (int)text_len, text);
    }
}               

static void parser_got_error(GMarkupParseContext * context G_GNUC_UNUSED,
                             GError * error,
                             gpointer user_data G_GNUC_UNUSED) {
    g_fprintf (stderr, "Problem with Amazon response: %s\n", error->message);
    exit(EXIT_FAILURE);
}

static GMarkupParseContext * parser_init(Credentials * credentials) {
    static const GMarkupParser parser_settings = {
        NULL, /* start_element */
        NULL, /* end_element */
        parser_got_text, /* text */
        NULL, /* passthrough */
        parser_got_error /* error */
    };
    bzero(credentials, sizeof(*credentials));

    credentials->user_token = g_string_new("");
    credentials->access_key = g_string_new("");
    credentials->secret_key = g_string_new("");

    return g_markup_parse_context_new(&parser_settings, 0, credentials, NULL);
}

static void parser_cleanup(GMarkupParseContext * context) {
    GError * error = NULL;
    g_markup_parse_context_end_parse(context, &error);
    
    if (error != NULL) {
        g_fprintf (stderr, "Unexpected end of Amazon response: %s\n",
                 error->message);
        exit(EXIT_FAILURE);
    }

    g_markup_parse_context_free(context);
}

/* This function is responsible for the whole output thing. */
static void do_output(Credentials * rare) {
    if (rare == NULL ||
        rare->user_token == NULL || !rare->user_token->len ||
        rare->access_key == NULL || !rare->access_key->len ||
        rare->secret_key == NULL || !rare->secret_key->len) {
        g_fprintf(stderr, "Missing authentication data in response!\n");
        exit(EXIT_FAILURE);
    }

    g_printf("device_property \"S3_USER_TOKEN\" \"%s\"\n"
             "device_property \"S3_ACCESS_KEY\" \"%s\"\n"
             "device_property \"S3_SECRET_KEY\" \"%s\"\n",
             rare->user_token->str, rare->access_key->str,
             rare->secret_key->str);
}

int main(int argc, char ** argv) {
    const char * key;
    GMarkupParseContext * parser;
    Credentials credentials;

    key = parse_commandline(argc, argv);

    curl_global_init(CURL_GLOBAL_ALL);
    parser = parser_init(&credentials);

    do_server_stuff(key, parser);

    curl_global_cleanup();
    parser_cleanup(parser);

    do_output(&credentials);
    
    return 0;
}
