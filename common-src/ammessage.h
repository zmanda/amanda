/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-2000 University of Maryland at College Park
 * Copyright (c) 2007-2012 Zmanda, Inc.  All Rights Reserved.
 * Copyright (c) 2013-2016 Carbonite, Inc.  All Rights Reserved.
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

#ifndef AMMESSAGE_H
#define AMMESSAGE_H

typedef struct message_s message_t;
typedef GSList *messagelist_t;

#define MSG_CRITICAL	32
#define MSG_ERROR	16
#define MSG_WARNING	8
#define MSG_MESSAGE	4
#define MSG_INFO	2
#define MSG_SUCCESS	1

void
delete_message(
    message_t *message);

void
delete_message_gpointer(
    gpointer data);

message_t *
build_message(
    char *file,
    int   line,
    int   code,
    int   severity,
    int   nb,
    ...);

char *
get_message(
    message_t *message);

char *
get_quoted_message(
    message_t *message);

char *
message_get_hint(
    message_t *message);

int
message_get_code(
    message_t *message);

int
message_get_severity(
    message_t *message);

char *
message_get_argument(
    message_t *message,
    char *key);

void
message_add_argument(
    message_t *message,
    char *key,
    char *value);

char *
sprint_message(
    message_t *message);

message_t *
print_message(
    message_t *message);

message_t *
fprint_message(
    FILE      *stream,
    message_t *message);

message_t *
fdprint_message(
    int        fd,
    message_t *message);

GPtrArray *parse_json_message(char *s);

char *get_errno_string(int my_errno);
int   get_errno_number(char *errno_string);

#endif /* AMMESSAGE_H */
