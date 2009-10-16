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

#include <amanda.h>
#include "glib-util.h"
#include "tape-ops.h"

/* Tape operations for AIX systems. Most of this stuff is based on
   documentation from 
   http://publibn.boulder.ibm.com/doc_link/Ja_JP/a_doc_lib/files/aixfiles/rmt.htm */

/* Uncomment to test compilation on non-AIX systems. */
/* ---
#undef MTIOCTOP
#define STIOCTOP 0
#define stop mtop
#define st_op mt_op
#define st_count mt_count
#define STREW MTREW
#define STFSF MTFSF
#define STRSF MTBSF
#define STFSR MTFSR
#define STRSR MTBSR
#define STWEOF MTWEOF
--- */

gboolean tape_rewind(int fd) {
    struct stop st;
    st.st_op = STREW;
    st.st_count = 1;
    return 0 == ioctl(fd, STIOCTOP, &st);
}

gboolean tape_fsf(int fd, guint count) {
    struct stop st;
    st.st_op = STFSF;
    st.st_count = count;
    return 0 == ioctl(fd, STIOCTOP, &st);
}

gboolean tape_bsf(int fd, guint count) {
    struct stop st;
    st.st_op = STRSF;
    st.st_count = count;
    return 0 == ioctl(fd, STIOCTOP, &st);
}

gboolean tape_fsr(int fd, guint count) {
    struct stop st;
    st.st_op = STFSR;
    st.st_count = count;
    return 0 == ioctl(fd, STIOCTOP, &st);
}

gboolean tape_bsr(int fd, guint count) {
    struct stop st;
    st.st_op = STRSR;
    st.st_count = count;
    return 0 == ioctl(fd, STIOCTOP, &st);
}

gint tape_eod(int fd G_GNUC_UNUSED) {
    g_assert_not_reached();
    return TAPE_OP_ERROR;
}

gboolean tape_weof(int fd, guint8 count) {
    struct stop st;
    st.st_op = STWEOF;
    st.st_count = count;
    return 0 == ioctl(fd, STIOCTOP, &st);
}

gboolean tape_setcompression(int fd G_GNUC_UNUSED, gboolean on G_GNUC_UNUSED) {
    return FALSE;
}

DeviceStatusFlags tape_is_tape_device(int fd G_GNUC_UNUSED) {
    /* AIX doesn't have a no-op, so we'll just assume this is a tape device */
    return DEVICE_STATUS_SUCCESS;
}

DeviceStatusFlags tape_is_ready(int fd G_GNUC_UNUSED, TapeDevice *t_self G_GNUC_UNUSED) {
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
