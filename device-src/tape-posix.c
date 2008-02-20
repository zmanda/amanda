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

#include "amanda.h"
#include "util.h"
#include "tape-ops.h"
#include "property.h"
#include <glob.h>

/* Having one name for every operation would be too easy. */
#if !defined(MTCOMPRESSION) && defined(MTCOMP)
# define MTCOMPRESSION MTCOMP
#endif

#if !defined(MTSETBLK) && defined(MTSETBSIZ)
# define MTSETBLK MTSETBSIZ
#endif

#if !defined(MTEOM) && defined(MTEOD)
# define MTEOM MTEOD
#endif

#ifdef HAVE_LIMITS_H
# include <limits.h>
#endif

gboolean tape_rewind(int fd) {
    int count = 5;
    time_t stop_time;

    /* We will retry this for up to 30 seconds or 5 retries,
       whichever is less, because some hardware/software combinations
       (notably EXB-8200 on FreeBSD) can fail to rewind. */
    stop_time = time(NULL) + 30;

    while (--count >= 0 && time(NULL) < stop_time) {
        struct mtop mt;
        mt.mt_op = MTREW;
        mt.mt_count = 1;

        if (0 == ioctl(fd, MTIOCTOP, &mt))
            return TRUE;

        sleep(3);
    }

    return FALSE;
}

gboolean tape_fsf(int fd, guint count) {
    struct mtop mt;
    mt.mt_op = MTFSF;
    mt.mt_count = count;
    return 0 == ioctl(fd, MTIOCTOP, &mt);
}

gboolean tape_bsf(int fd, guint count) {
    struct mtop mt;
    mt.mt_op = MTBSF;
    mt.mt_count = count;
    return 0 == ioctl(fd, MTIOCTOP, &mt);
}

gboolean tape_fsr(int fd, guint count) {
    struct mtop mt;
    mt.mt_op = MTFSR;
    mt.mt_count = count;
    return 0 == ioctl(fd, MTIOCTOP, &mt);
}

gboolean tape_bsr(int fd, guint count) {
    struct mtop mt;
    mt.mt_op = MTBSR;
    mt.mt_count = count;
    return 0 == ioctl(fd, MTIOCTOP, &mt);
}

gint tape_eod(int fd) {
    struct mtop mt;
    struct mtget get;
    mt.mt_op = MTEOM;
    mt.mt_count = 1;
    if (0 != ioctl(fd, MTIOCTOP, &mt))
        return TAPE_OP_ERROR;

    /* Ignored result. This is just to flush buffers. */
    mt.mt_op = MTNOP;
    ioctl(fd, MTIOCTOP, &mt);

    if (0 != ioctl(fd, MTIOCGET, &get))
        return TAPE_POSITION_UNKNOWN;
    if (get.mt_fileno < 0)
        return TAPE_POSITION_UNKNOWN;
    else
        return get.mt_fileno;
}

gboolean tape_weof(int fd, guint8 count) {
    struct mtop mt;
    mt.mt_op = MTWEOF;
    mt.mt_count = count;
    return 0 == ioctl(fd, MTIOCTOP, &mt);
}

gboolean tape_setcompression(int fd G_GNUC_UNUSED, 
	gboolean on G_GNUC_UNUSED) {
#ifdef MTCOMPRESSION
    struct mtop mt;
    mt.mt_op = MTCOMPRESSION;
    mt.mt_count = on;
    return 0 == ioctl(fd, MTIOCTOP, &mt);
#else
    return 0;
#endif
}

TapeCheckResult tape_is_tape_device(int fd) {
    struct mtop mt;
    mt.mt_op = MTNOP;
    mt.mt_count = 1;
    if (0 == ioctl(fd, MTIOCTOP, &mt)) {
        return TAPE_CHECK_SUCCESS;
    } else {
	dbprintf("tape_is_tape_device: ioctl(MTIOCTOP/MTNOP) failed: %s",
		 strerror(errno));
        return TAPE_CHECK_FAILURE;
    }
}

TapeCheckResult tape_is_ready(int fd) {
    struct mtget get;
    if (0 == ioctl(fd, MTIOCGET, &get)) {
#if defined(GMT_ONLINE) || defined(GMT_DR_OPEN)
        if (1
#ifdef GMT_ONLINE
            && GMT_ONLINE(get.mt_gstat)
#endif
#ifdef GMT_DR_OPEN
            && !GMT_DR_OPEN(get.mt_gstat)
#endif
            ) {
            return TAPE_CHECK_SUCCESS;
        } else {
	    dbprintf("tape_is_read: ioctl(MTIOCGET) failed: %s", strerror(errno));
            return TAPE_CHECK_FAILURE;
        }
#else /* Neither macro is defined. */
        return TAPE_CHECK_UNKNOWN;
#endif
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
                      FEATURE_STATUS_ENABLED | FEATURE_SURETY_BAD |
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_EOM, &val);

    g_value_set_flags(&val,
                      FEATURE_STATUS_DISABLED | FEATURE_SURETY_BAD | 
                      FEATURE_SOURCE_DEFAULT);
    device_property_set(self, PROPERTY_BSF_AFTER_EOM, &val);

    g_value_unset_init(&val, G_TYPE_UINT);
    g_value_set_uint(&val, 2);
    device_property_set(self, PROPERTY_FINAL_FILEMARKS, &val);
}
