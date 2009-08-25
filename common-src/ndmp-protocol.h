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

/*
 * $Id: ndmp_protocol.h,v $
 *
 * Define feature test related items.
 */

#ifndef NDMP_PROTOCOL_H
#define NDMP_PROTOCOL_H

#include "ipc-binary.h"

/* note that this will generate duplicate symbols if #included multiple times
 * in the same executable */

amprotocol_t listen_ndmp = { 0xC74F, -1, {
   { CMD_DEVICE      , 0 },  /*						*/
   { REPLY_DEVICE    , 1 },  /* 					*/
   { CMD_MAX, 0 } }};

amprotocol_t device_ndmp = { 0xD85A, -1, {
   { CMD_TAPE_OPEN   , 4 },  /* filename mode host user,password	*/
   { REPLY_TAPE_OPEN , 1 },  /* filename err-code			*/
   { CMD_TAPE_CLOSE  , 0 },  /* 					*/
   { REPLY_TAPE_CLOSE, 1 },  /* error-code				*/
   { CMD_TAPE_MTIO   , 2 },  /* command count				*/
   { REPLY_TAPE_MTIO , 1 },  /* error-code				*/
   { CMD_TAPE_WRITE  , 1 },  /* buffer					*/
   { REPLY_TAPE_WRITE, 1 },  /* error-code				*/
   { CMD_TAPE_READ   , 1 },  /* size					*/
   { REPLY_TAPE_READ , 2 },  /* error-code "DATA"			*/
   { CMD_MAX, 0 } }};

#endif /* NDMP_PROTOCOL_H */

