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

/* Tape operations for SVR4 systems. Most of this stuff is based on
   documentation from http://docsrv.sco.com/cgi-bin/man/man?sdi+7 */

#include <amanda.h>
#include <tape-ops.h>

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

int tape_eod(int fd) {
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

TapeCheckResult tape_is_tape_device(int fd) {
    /* If we can read block information, it's probably a tape device. */
    struct blklen result;
    if (0 == ioctl(fd, T_RDBLKLEN, &result)) {
        return TAPE_CHECK_SUCCESS;
    } else {
        return TAPE_CHECK_FAILURE;
    }
}

TapeCheckResult tape_is_tape_ready(int fd) {
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
