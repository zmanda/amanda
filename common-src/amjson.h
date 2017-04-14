/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2013-2017 Carbonite, Inc.  All Rights Reserved.
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

#ifndef AMJSON_H
#define AMJSON_H

typedef enum {
    JSON_STRING,
    JSON_NUMBER,
    JSON_NULL,
    JSON_TRUE,
    JSON_FALSE,
    JSON_ARRAY,
    JSON_HASH,
    JSON_BAD
} amjson_type_t;

typedef struct amjson_s amjson_t;

void delete_json(amjson_t *json);
char *json_to_string(amjson_t *json);
amjson_t *parse_json(char *s);

amjson_type_t get_json_type(amjson_t *json);
char *get_json_string(amjson_t *json);
uint64_t get_json_number(amjson_t *json);
amjson_t *get_json_hash_from_key(amjson_t *json, char *key);
void foreach_json_array(amjson_t *json, GFunc func, gpointer user_data);
void foreach_json_hash(amjson_t *json, GHFunc func, gpointer user_data);

#endif /* AMJSON_H */
