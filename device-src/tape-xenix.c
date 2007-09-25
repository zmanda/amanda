/*
 * Copyright (c) 2005 Zmanda, Inc.  All Rights Reserved.
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
 * Contact information: Zmanda Inc., 505 N Mathlida Ave, Suite 120
 * Sunnyvale, CA 94085, USA, or: http://www.zmanda.com
 */

/* Tape operations for XENIX systems. Most of this stuff is based on
   documentation from
   http://www.ifthenfi.nl:8080/cgi-bin/ssl_getmanpage?tape+HW+XNX234+tape
*/

#include <amanda.h>
#include <tape-ops.h>

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
    while (--count >= 0) {
        if (0 != ioctl(fd, T_RFM))
            return FALSE;
    }
    return TRUE;
}

gboolean tape_bsf(int fd, guint count) {
    g_assert_not_reached();
    return FALSE;
}

gboolean tape_fsr(int fd, guint count) {
    g_assert_not_reached();
    return FALSE;
}

gboolean tape_bsr(int fd, guint count) {
    g_assert_not_reached();
    return FALSE;
}

gint tape_eod(int fd) {
    g_assert_not_reached();
    return TAPE_OP_ERROR;
}

gboolean tape_weof(int fd, guint8 count) {
    while (count -- > 0) {
        if (0 != ioctl(fd, T_WFM))
            return FALSE;
    } 

    return TRUE;
}

gboolean tape_setcompression(int fd, gboolean on) {
    return FALSE;
}

TapeCheckResult tape_is_tape_device(int fd) {
    struct tape_info result;
    if (0 == ioctl(fd, MT_STATUS, &result)) {
        return TAPE_CHECK_SUCCESS;
    } else {
        return TAPE_CHECK_FAILURE;
    }
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
                      FEATURE_STATUS_DISABLED | FEATURE_SURETY_GOOD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_BSF, &val);
    
    g_value_set_flags(&val,
                      FEATURE_STATUS_DISABLED | FEATURE_SURETY_GOOD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_FSR, &val);
    
    g_value_set_flags(&val,
                      FEATURE_STATUS_DISABLED | FEATURE_SURETY_GOOD |
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
