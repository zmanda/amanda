/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 2005 Zmanda Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public

 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */

#ifndef TAPE_OPS_H

#include <glib.h>
#include "tape-device.h"
#include "amanda.h"

#ifdef HAVE_SYS_TAPE_H
# include <sys/tape.h>
#endif
#ifdef HAVE_SYS_MTIO_H
# include <sys/mtio.h>
#endif

/* Return codes for tape_eod */
#define TAPE_OP_ERROR -1
#define TAPE_POSITION_UNKNOWN -2

/* Real Operations (always return FALSE if not implemented). These are
 * implemented in one of tape-{uware,aix,xenix,posix}.c, depending on
 * the platform. */
gboolean tape_rewind(int fd);
gboolean tape_fsf(int fd, guint count);
gboolean tape_bsf(int fd, guint count);
gboolean tape_fsr(int fd, guint count);
gboolean tape_bsr(int fd, guint count);
/* Returns tape position file number, or one of the return codes above. */
gint tape_eod(int fd);
gboolean tape_weof(int fd, guint8 count);
/* 0 means variable */
gboolean tape_setblk(int fd, int blocksize);
gboolean tape_setcompression(int fd, gboolean on);

typedef enum {
    TAPE_CHECK_SUCCESS,
    TAPE_CHECK_UNKNOWN,
    TAPE_CHECK_FAILURE
} TapeCheckResult;
TapeCheckResult tape_is_tape_device(int fd);

/* Also implemented in above files. Sets properties on the device. */
void tape_device_discover_capabilities(TapeDevice * self);

#endif

