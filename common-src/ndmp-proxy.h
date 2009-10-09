/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
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
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */

#ifndef NDMP_PROTOCOL_H
#define NDMP_PROTOCOL_H

#include "ipc-binary.h"

enum {
    NDMP_PROXY_CMD_SELECT_SERVICE = 1,
    NDMP_PROXY_REPLY_GENERIC = 2,
    NDMP_PROXY_CMD_TAPE_OPEN = 3,
    NDMP_PROXY_CMD_TAPE_CLOSE = 4,
    NDMP_PROXY_CMD_TAPE_MTIO = 5,
    NDMP_PROXY_CMD_TAPE_WRITE = 6,
    NDMP_PROXY_CMD_TAPE_READ = 7,
    NDMP_PROXY_REPLY_TAPE_READ = 8,
};

enum {
    NDMP_PROXY_FILENAME = 1,
    NDMP_PROXY_MODE = 2,
    NDMP_PROXY_HOST = 3,
    NDMP_PROXY_PORT = 4,
    NDMP_PROXY_ERRCODE = 5,
    NDMP_PROXY_ERROR = 6,
    NDMP_PROXY_COMMAND = 7,
    NDMP_PROXY_COUNT = 8,
    NDMP_PROXY_DATA = 9,
    NDMP_PROXY_SERVICE = 10,
    NDMP_PROXY_USERNAME = 11,
    NDMP_PROXY_PASSWORD = 12,
};

ipc_binary_proto_t *get_ndmp_proxy_proto(void);

int   connect_to_ndmp_proxy(char **errmsg);

#endif /* NDMP_PROTOCOL_H */

