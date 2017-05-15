/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2017-2017 Carbonite, Inc.  All Rights Reserved.
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 */

#include "amanda.h"
#include "amutil.h"
#include "conffile.h"
#include "amjson.h"

static char *string_encode_json(char *str);

static void free_json_value_full(gpointer);
static void
free_json_value_full(
    gpointer pointer)
{
    amjson_t *json = pointer;
    if (json->type == JSON_STRING) {
	g_free(json->string);
	json->string = NULL;
    } else if (json->type == JSON_ARRAY) {
	guint i;
	for (i = 0; i < json->array->len; i++) {
	    free_json_value_full(g_ptr_array_index(json->array, i));
	}
	g_ptr_array_free(json->array, TRUE);
	json->array = NULL;
    } else if (json->type == JSON_HASH) {
	g_hash_table_destroy(json->hash);
	json->hash = NULL;
    }
    json->type = JSON_NULL;
    g_free(json);
}

void
delete_json(
    amjson_t *json)
{
    free_json_value_full(json);
}

static char *json_value_to_string(amjson_t *json, int first, int indent);

char *
json_to_string(
    amjson_t *json)
{
    return json_value_to_string(json, 1, 0);
}

typedef struct message_hash_s {
    GString *r;
    int first;
    int indent;
} message_hash_t;

static void
json_hash_to_string(
    gpointer gkey,
    gpointer gvalue,
    gpointer user_data)
{
    char *key = gkey;
    amjson_t *value = gvalue;
    message_hash_t *mh = user_data;
    char *result_value = json_value_to_string(value, 1, mh->indent);

    if (mh->first) {
	g_string_append_printf(mh->r,"%*c\"%s\": %s", mh->indent, ' ', (char *)key, result_value);
	mh->first = 0;
    } else {
	g_string_append_printf(mh->r,",\n%*c\"%s\": %s", mh->indent, ' ', (char *)key, result_value);
    }
    g_free(result_value);
}

static char *
json_value_to_string(
    amjson_t *json,
    int       first,
    int       indent)
{
    char *result = NULL;

    switch (json->type) {
    case JSON_TRUE:
	    result = g_strdup_printf("%s", "true");
	break;
    case JSON_FALSE:
	    result = g_strdup_printf("%s", "false");
	break;
    case JSON_NULL:
	    result = g_strdup_printf("%s", "null");
	break;
    case JSON_STRING:
	{
	    char *encoded = string_encode_json(json->string);
	    result = g_strdup_printf("\"%s\"", encoded);
	    g_free(encoded);
	}
	break;
    case JSON_NUMBER:
	{
	    result = g_strdup_printf("%lld", (long long)json->number);
	}
	break;
    case JSON_ARRAY:
	if (json->array->len == 0) {
	    result = g_strdup_printf("[ ]");
        } else {
	    GString *r = g_string_sized_new(512);
	    guint i;
	    int lfirst = 1;
	    if (indent == 0) {
		g_string_append_printf(r, "[\n");
	    } else {
		g_string_append_printf(r, "[\n%*c", indent+2, ' ');
	    }
	    for (i = 0; i < json->array->len; i++) {
		amjson_t *value = g_ptr_array_index(json->array, i);
		char *result_value = json_value_to_string(value, lfirst, indent+2);
		lfirst = 0;
		if (i>0) {
		    g_string_append(r, ",\n");
		}
		g_string_append(r, result_value);
		g_free(result_value);
	    }
	    g_string_append_printf(r, "\n%*c]", indent, ' ');
	    result = g_string_free(r, FALSE);
	}
	break;

    case JSON_HASH:
	if (g_hash_table_size(json->hash) == 0) {
	    result = g_strdup("{ }");
	} else {
	    GString *r = g_string_sized_new(512);
	    message_hash_t mh = {r, 1, indent+2};;
	    if (first) {
		g_string_append_printf(r, "{\n");
	    } else {
		g_string_append_printf(r, "%*c{\n", indent, ' ');
	    }
	    g_hash_table_foreach(json->hash, json_hash_to_string, &mh);
	    g_string_append_printf(r, "\n%*c}", indent, ' ');
	    result = g_string_free(r, FALSE);
	}
	break;

    case JSON_BAD:
	g_critical("JSON_BAD");
	break;
    }


    return result;
}

amjson_type_t
parse_json_primitive(
    char *s,
    int  *i,
    int   len G_GNUC_UNUSED)
{

    if (strncmp(&s[*i], "null", 4) == 0) {
	*i += 4;
	return JSON_NULL;
    } else if (strncmp(&s[*i], "true", 4) == 0) {
	*i += 4;
	return JSON_TRUE;
    } else if (strncmp(&s[*i], "false", 5) == 0) {
	*i += 5;
	return JSON_FALSE;
    }
    return JSON_BAD;
}

char *
json_parse_string(
    char *s,
    int  *i,
    int   len)
{
    char *string = g_malloc(len);
    char *sp = string;

    (*i)++;
    for (; *i < len && s[*i] != '\0'; (*i)++) {
	char c = s[*i];

	if (c == '"') {
	    *sp = '\0';
	    return string;
	} else if (c == '\\') {
	    (*i)++;
	    c = s[*i];

	    switch (c) {
		case '"': case '/': case '\\':
		    *sp++ = c;
		    break;
		case 'b':
		case 'f': case 'r': case 'n': case 't':
		    *sp++ = '\\';
		    *sp++ = c;
		    break;
		case 'u':
		    break;
		default:
		    break;
	    }
	} else {
	    *sp++ = c;
	}
    }
    g_free(string);
    return NULL;
}

static uint64_t json_parse_number(char *s, int *i, int len);
static uint64_t
json_parse_number(
    char *s,
    int  *i,
    int   len G_GNUC_UNUSED)
{
    gboolean negate = FALSE;
    guint64 result = 0;
    char c = s[*i];

    if (s[*i] == '-') {
	negate = TRUE;
	(*i)++;
	c = s[*i];
    }
    if (c >= '0' && c <= '9') {
	result = c-'0';
    } else {
	g_critical("json not a number");
    }
    (*i)++;
    c = s[*i];
    while (c >= '0' && c <= '9') {
	result = result*10 + c-'0';
	(*i)++;
	c = s[*i];
    }
    (*i)--;
    if (negate)
	return -result;
    else 
	return result;
}

static amjson_t *parse_json_hash(char *s, int *i);
static amjson_t *parse_json_array(char *s, int *i);
static amjson_t *
parse_json_array(
    char *s,
    int  *i)
{
    int len = strlen(s);
    char *token;
    uint64_t itoken;
    amjson_type_t json_token;
    amjson_t *json = g_new0(amjson_t, 1);
    json->type = JSON_ARRAY;
    json->array = g_ptr_array_sized_new(10);

    (*i)++;
    for (; *i < len && s[*i] != '\0'; (*i)++) {
	char c =  s[*i];

	switch (c) {
	    case '[':
		{
		    amjson_t *value = parse_json_array(s, i);
		    g_ptr_array_add(json->array, value);
		}
		break;
	    case ']':
		return json;
		break;
	    case '{':
		{
		    amjson_t *value = parse_json_hash(s, i);
		    g_ptr_array_add(json->array, value);
		}
		break;
	    case '}':
		break;
		//return;
	    case '"':
		token = json_parse_string(s, i, len);
		assert(token);
		{
		    amjson_t *value = g_new0(amjson_t, 1);
		    value->type = JSON_STRING;
		    value->string = token;
		    g_ptr_array_add(json->array, value);
		}
		token = NULL;

		break;
	    case '\t':
	    case '\r':
	    case '\n':
	    case ':':
	    case ',':
	    case ' ':
		break;

	    case '-':
	    case '0':
	    case '1':
	    case '2':
	    case '3':
	    case '4':
	    case '5':
	    case '6':
	    case '7':
	    case '8':
	    case '9':
		itoken = json_parse_number(s, i, len);
		{
		    amjson_t *value = g_new0(amjson_t, 1);
		    value->type = JSON_NUMBER;
		    value->number = itoken;
		    g_ptr_array_add(json->array, value);
		}
		break;

	    default:
		json_token = parse_json_primitive(s, i, len);
		if (json_token != JSON_BAD) {
		    amjson_t *value = g_new(amjson_t, 1);
		    value->type = json_token;
		    value->string = NULL;
		    g_ptr_array_add(json->array, value);
		}
		token = NULL;
		break;
	}
    }

    return json;
}

static amjson_t *
parse_json_hash(
    char *s,
    int  *i)
{
    int len = strlen(s);
    char *token;
    uint64_t itoken;
    amjson_type_t json_token;
    gboolean expect_key = TRUE;
    char *key = NULL;
    amjson_t *json = g_new0(amjson_t, 1);
    json->type = JSON_HASH;
    json->hash = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, free_json_value_full);

    (*i)++;
    for (; *i < len && s[*i] != '\0'; (*i)++) {
	char c =  s[*i];

	switch (c) {
	    case '[':
		if (key) {
		    amjson_t *value = parse_json_array(s, i);
		    g_hash_table_insert(json->hash, key, value);
		    key = NULL;
		    expect_key = TRUE;
		}
		break;
	    case ']':
		break;
	    case '{':
		if (key) {
		    amjson_t *value = parse_json_hash(s, i);
		    g_hash_table_insert(json->hash, key, value);
		    key = NULL;
		    expect_key = TRUE;
		} else {
		}
		break;
	    case '}':
		return json;
	    case '"':
		token = json_parse_string(s, i, len);
		assert(token);
		if (expect_key) {
		    expect_key = FALSE;
		    key = token;
		} else {
		    amjson_t *value = g_new0(amjson_t, 1);
		    value->type = JSON_STRING;
		    value->string = token;
		    g_hash_table_insert(json->hash, key, value);
		    key = NULL;
		    expect_key = TRUE;
		}
		token = NULL;

		break;
	    case '\t':
	    case '\r':
	    case '\n':
	    case ':':
	    case ',':
	    case ' ':
		break;

	    case '-':
	    case '0':
	    case '1':
	    case '2':
	    case '3':
	    case '4':
	    case '5':
	    case '6':
	    case '7':
	    case '8':
	    case '9':
		itoken = json_parse_number(s, i, len);
		if (expect_key) {
		    g_critical("number as hash key");
		} else {
		    amjson_t *value = g_new0(amjson_t, 1);
		    value->type = JSON_NUMBER;
		    value->number = itoken;
		    g_hash_table_insert(json->hash, key, value);
		    key = NULL;
		    expect_key = TRUE;
		}
		token = NULL;
		break;

	    default:
		json_token = parse_json_primitive(s, i, len);
		if (expect_key) {
		    g_critical("primitive as hash key");
		} else if (json_token != JSON_BAD) {
		    amjson_t *value = g_new0(amjson_t, 1);
		    value->type = json_token;
		    value->string = NULL;
		    g_hash_table_insert(json->hash, key, value);
		    key = NULL;
		    expect_key = TRUE;
		} else {
		    g_critical("JSON_BAD");
		}
		token = NULL;
		break;
	}
    }

    return json;
}

amjson_t *
parse_json(
    char *s)
{
    int i;
    int len = strlen(s);
    char *token;
    uint64_t itoken;
    amjson_type_t json_token;
    amjson_t *json = NULL;

    for (i = 0; i < len && s[i] != '\0'; i++) {
	char c =  s[i];

	switch (c) {
	    case '[':
		json = parse_json_array(s, &i);
		break;
	    case ']':
		break;
	    case '{':
		json = parse_json_hash(s, &i);
		break;
	    case '}':
		break;
	    case '"':
		token = json_parse_string(s, &i, len);
		assert(token);
		json = g_new0(amjson_t, 1);
		json->type = JSON_STRING;
		json->string = token;
		token = NULL;

		break;
	    case '\t':
	    case '\r':
	    case '\n':
	    case ':':
	    case ',':
	    case ' ':
		break;

	    case '-':
	    case '0':
	    case '1':
	    case '2':
	    case '3':
	    case '4':
	    case '5':
	    case '6':
	    case '7':
	    case '8':
	    case '9':
		itoken = json_parse_number(s, &i, len);
		json = g_new0(amjson_t, 1);
		json->type = JSON_NUMBER;
		json->number = itoken;
		token = NULL;
		break;

	    default:
		json_token = parse_json_primitive(s, &i, len);
		if (json_token != JSON_BAD) {
		    json = g_new0(amjson_t, 1);
		    json->type = json_token;
		}
		token = NULL;
		break;
	}
    }

    return json;
}

static char *
string_encode_json(
    char *str)
{
    int i = 0;
    int len;
    unsigned char *s;
    char *encoded;
    char *e;

    if (!str) {
        return g_strdup("null");
    }
    len = strlen(str)*2;
    s = (unsigned char *)str;
    encoded = g_malloc(len+1);
    e = encoded;

    while(*s != '\0') {
        if (i++ >= len) {
            error("string_encode_json: str is too long: %s", str);
        }
        if (*s == '\\' || *s == '"') {
            *e++ = '\\';
            *e++ = *s++;
        } else if (*s == '\b') {
            *e++ = '\\';
            *e++ = 'b';
            s++;
        } else if (*s == '\f') {
            *e++ = '\\';
            *e++ = 'f';
            s++;
        } else if (*s == '\n') {
            *e++ = '\\';
            *e++ = 'n';
            s++;
        } else if (*s == '\r') {
            *e++ = '\\';
            *e++ = 'r';
            s++;
        } else if (*s == '\t') {
            *e++ = '\\';
            *e++ = 't';
            s++;
        } else if (*s == '\v') {
            *e++ = '\\';
            *e++ = 'v';
            s++;
        } else if (*s < 32) {
            *e++ = '\\';
            *e++ = 'u';
            *e++ = '0';
            *e++ = '0';
            if ((*s>>4) <= 9)
                *e++ = '0' + (*s>>4);
            else
                *e++ = 'A' + (*s>4) - 10;
            if ((*s & 0x0F) <= 9)
                *e++ = '0' + (*s & 0x0F);
            else
                *e++ = 'A' + (*s & 0x0F) - 10;
            s++;
        } else {
            *e++ = *s++;
        }
    }
    *e = '\0';
    return encoded;
}

amjson_type_t
get_json_type(
    amjson_t *json)
{
    return json->type;
}

char *
get_json_string(
    amjson_t *json)
{
    assert(json->type == JSON_STRING);
    return json->string;
}

uint64_t
get_json_number(
    amjson_t *json)
{
    assert(json->type == JSON_NUMBER);
    return json->number;
}

amjson_t *
get_json_hash_from_key(
    amjson_t *json,
    char *key)
{
    assert(json->type == JSON_HASH);
    return g_hash_table_lookup(json->hash, key);
}

void
foreach_json_array(
    amjson_t *json,
    GFunc func,
    gpointer user_data)
{
    assert(json->type == JSON_ARRAY);
    g_ptr_array_foreach(json->array, func, user_data);
}

void
foreach_json_hash(
    amjson_t *json,
    GHFunc func,
    gpointer user_data)
{
    assert(json->type == JSON_HASH);
    g_hash_table_foreach(json->hash, func, user_data);
}


