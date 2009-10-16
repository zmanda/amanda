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

/* Tape operations for SVR4 systems. Most of this stuff is based on
   documentation from http://docsrv.sco.com/cgi-bin/man/man?sdi+7 */

#include <amanda.h>
#include <tape-ops.h>
#include "glib-util.h"

/* Uncomment to test on non-SYSV4 systems. */
/* ---
#undef MTIOCTOP
#define T_RWD 0
#define T_SFF 0
#define T_SFB 0
#define T_SBF 0
#define T_SBB 0
#define T_WRFILEM 0
#define T_RDBLKLEN 0
#define T_WRBLKLEN 0
#define T_SETCOMP 0

struct blklen {
    int min_blen, max_blen;
};

 --- */

gboolean tape_rewind(int fd) {
    return 0 == ioctl(fd, T_RWD);
}

gboolean tape_fsf(int fd, guint count) {
    return 0 == ioctl(fd, T_SFF, count);
}

gboolean tape_bsf(int fd, guint count) {
    return 0 == ioctl(fd, T_SFB, count);
}

gboolean tape_fsr(int fd, guint count) {
    return 0 == ioctl(fd, T_SBF, count);
}

gboolean tape_bsr(int fd, guint count) {
    return 0 == ioctl(fd, T_SBB, count);
}

gint tape_eod(int fd G_GNUC_UNUSED) {
    g_assert_not_reached();
    return TAPE_OP_ERROR;
}

gboolean tape_weof(int fd, guint8 count) {
    return 0 == ioctl(fd, T_WRFILEM, count);
}

gboolean tape_setcompression(int fd, gboolean on) {
    int cmd;
    if (on) {
        cmd = 3;
    } else {
        cmd = 2;
    }

    return 0 == ioctl(fd, T_SETCOMP, cmd);
}

DeviceStatusFlags tape_is_tape_device(int fd) {
    /* If we can read block information, it's probably a tape device. */
    struct blklen result;
    if (0 == ioctl(fd, T_RDBLKLEN, &result)) {
        return DEVICE_STATUS_SUCCESS;
    } else {
        return DEVICE_STATUS_DEVICE_ERROR;
    }
}

DeviceStatusFlags tape_is_ready(int fd G_GNUC_UNUSED, TapeDevice *t_self G_GNUC_UNUSED) {
    /* No good way to determine this, so assume it's ready */
    return DEVICE_STATUS_SUCCESS;
}

void tape_device_detect_capabilities(TapeDevice * t_self) {
    tape_device_set_capabilities(t_self,
	TRUE,  PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT, /* fsf*/
	DEFAULT_FSF_AFTER_FILEMARK, PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT, /* fsf_after_filemark*/
	TRUE,  PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT, /* bsf*/
	TRUE,  PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT, /* fsr*/
	TRUE,  PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT, /* bsr*/
	FALSE, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT, /* eom*/
	FALSE, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT, /* bsf_after_eom*/
	2,     PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT  /* final_filemarks*/
	);
}
