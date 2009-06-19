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
 * Contact information: Zmanda Inc., 465 N Mathlida Ave, Suite 300
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* Tape operations for XENIX systems. Most of this stuff is based on
   documentation from
   http://www.ifthenfi.nl:8080/cgi-bin/ssl_getmanpage?tape+HW+XNX234+tape
*/

#include <amanda.h>
#include <tape-ops.h>
#include "glib-util.h"

/* Uncomment to test compilation on non-XENIX systems. */
/* --- 
#undef MTIOCTOP
#define MT_REWIND 0
#define MT_STATUS 0
#define T_RFM 0
#define T_WFM 0
struct tape_info {};
  --- */

gboolean tape_rewind(int fd) {
    return 0 == ioctl(fd, MT_REWIND);
}

gboolean tape_fsf(int fd, guint count) {
    while (count-- > 0) {
        if (0 != ioctl(fd, T_RFM))
            return FALSE;
    }
    return TRUE;
}

gboolean tape_bsf(int fd G_GNUC_UNUSED, guint count G_GNUC_UNUSED) {
    g_assert_not_reached();
    return FALSE;
}

gboolean tape_fsr(int fd G_GNUC_UNUSED, guint count G_GNUC_UNUSED) {
    g_assert_not_reached();
    return FALSE;
}

gboolean tape_bsr(int fd G_GNUC_UNUSED, guint count G_GNUC_UNUSED) {
    g_assert_not_reached();
    return FALSE;
}

gint tape_eod(int fd G_GNUC_UNUSED) {
    g_assert_not_reached();
    return TAPE_OP_ERROR;
}

gboolean tape_weof(int fd, guint8 count) {
    while (count-- > 0) {
        if (0 != ioctl(fd, T_WFM))
            return FALSE;
    } 

    return TRUE;
}

gboolean tape_setcompression(int fd G_GNUC_UNUSED, gboolean on G_GNUC_UNUSED) {
    return FALSE;
}

DeviceStatusFlags tape_is_tape_device(int fd) {
    struct tape_info result;
    if (0 == ioctl(fd, MT_STATUS, &result)) {
        return DEVICE_STATUS_SUCCESS;
    } else {
        return DEVICE_STATUS_DEVICE_ERROR;
    }
}

DeviceStatusFlags tape_is_ready(int fd G_GNUC_UNUSED, TapeDevice *t_self G_GNUC_UNUSED) {
    /* We can probably do better. */
    return DEVICE_STATUS_SUCCESS;
}

void tape_device_detect_capabilities(TapeDevice * t_self) {
    tape_device_set_capabilities(t_self,
	TRUE,  PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT, /* fsf*/
	DEFAULT_FSF_AFTER_FILEMARK, PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT, /* fsf_after_filemark*/
	FALSE, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT, /* bsf*/
	FALSE, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT, /* fsr*/
	FALSE, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT, /* bsr*/
	FALSE, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT, /* eom*/
	FALSE, PROPERTY_SURETY_GOOD, PROPERTY_SOURCE_DEFAULT, /* bsf_after_eom*/
	2,     PROPERTY_SURETY_BAD,  PROPERTY_SOURCE_DEFAULT  /* final_filemarks*/
	);
}
