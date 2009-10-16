/*
 * Copyright (c) 2007,2008,2009 Zmanda, Inc.  All Rights Reserved.
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

/* This file declares functions which are implemented in each of tape-*.c. The
 * appropriate C file is selected at configure time. */

/* Real Operations (always return FALSE if not implemented) */
gboolean tape_rewind(int fd);
gboolean tape_fsf(int fd, guint count);
gboolean tape_bsf(int fd, guint count);
gboolean tape_fsr(int fd, guint count);
gboolean tape_bsr(int fd, guint count);

/* Sets attributes of the device to indicate which of the above operations
 * are available in this device. */
void tape_device_detect_capabilities(TapeDevice * self);

/* Returns tape position file number, or one of these: */
#define TAPE_OP_ERROR -1
#define TAPE_POSITION_UNKNOWN -2
gint tape_eod(int fd);

gboolean tape_weof(int fd, guint8 count);
gboolean tape_setcompression(int fd, gboolean on);

DeviceStatusFlags tape_is_tape_device(int fd);
DeviceStatusFlags tape_is_ready(int fd, TapeDevice *t_self);

#endif

