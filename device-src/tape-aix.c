/*
 * Copyright (c) 2005-2008 Zmanda Inc.  All Rights Reserved.
 * 
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version 2.1 as 
 * published by the Free Software Foundation.
 * 
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 * License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA.
 * 
 * Contact information: Zmanda Inc., 465 S Mathlida Ave, Suite 300
 * Sunnyvale, CA 94086, USA, or: http://www.zmanda.com
 */

#include <amanda.h>
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

gint tape_eod(int fd) {
    g_assert_not_reached();
    return TAPE_OP_ERROR;
}

gboolean tape_weof(int fd, guint8 count) {
    struct stop st;
    st.st_op = STWEOF;
    st.st_count = count;
    return 0 == ioctl(fd, STIOCTOP, &st);
}

gboolean tape_setcompression(int fd, gboolean on) {
    return FALSE;
}

TapeCheckResult tape_is_tape_device(int fd) {
    /* AIX doesn't have a no-op. */
    return TAPE_CHECK_UNKNOWN;
}

TapeCheckResult tape_is_ready(TapeDevice *t_self) {
    return TAPE_CHECK_UNKNOWN;
}

void tape_device_discover_capabilities(TapeDevice * t_self) {
    Device * self;
    GValue val;

    self = DEVICE(t_self);
    g_return_if_fail(self != NULL);

    bzero(&val, sizeof(val));
    g_value_init(&val, FEATURE_SUPPORT_FLAGS_TYPE);

    g_value_set_flags(&val,
                      FEATURE_STATUS_ENABLED | FEATURE_SURETY_BAD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_FSF, &val);
    
    g_value_set_flags(&val,
                      FEATURE_STATUS_ENABLED | FEATURE_SURETY_BAD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_BSF, &val);
    
    g_value_set_flags(&val,
                      FEATURE_STATUS_ENABLED | FEATURE_SURETY_BAD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_FSR, &val);
    
    g_value_set_flags(&val,
                      FEATURE_STATUS_ENABLED | FEATURE_SURETY_BAD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_BSR, &val);
    
    g_value_set_flags(&val,
                      FEATURE_STATUS_DISABLED | FEATURE_SURETY_GOOD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_EOM, &val);

    g_value_unset_init(&val, G_TYPE_UINT);
    g_value_set_uint(&val, 2);
    device_property_set(self, PROPERTY_FINAL_FILEMARKS, &val);
}
