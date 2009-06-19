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

DeviceStatusFlags tape_is_tape_device(int fd) {
    struct mtop mt;
    mt.mt_op = MTNOP;
    mt.mt_count = 1;
    if (0 == ioctl(fd, MTIOCTOP, &mt)) {
        return DEVICE_STATUS_SUCCESS;
#ifdef ENOMEDIUM
    } else if (errno == ENOMEDIUM) {
	return DEVICE_STATUS_VOLUME_MISSING;
#endif
    } else {
	dbprintf("tape_is_tape_device: ioctl(MTIOCTOP/MTNOP) failed: %s\n",
		 strerror(errno));
	if (errno == EIO) {
	    /* some devices return EIO while the drive is busy loading */
	    return DEVICE_STATUS_DEVICE_ERROR|DEVICE_STATUS_DEVICE_BUSY;
	} else {
	    return DEVICE_STATUS_DEVICE_ERROR;
	}
    }
}

DeviceStatusFlags tape_is_ready(int fd, TapeDevice *t_self G_GNUC_UNUSED) {
    struct mtget get;
    if (0 == ioctl(fd, MTIOCGET, &get)) {
#if defined(GMT_ONLINE) || defined(GMT_DR_OPEN)
        if (1
#ifdef GMT_ONLINE
            && (t_self->broken_gmt_online || GMT_ONLINE(get.mt_gstat))
#endif
#ifdef GMT_DR_OPEN
            && !GMT_DR_OPEN(get.mt_gstat)
#endif
            ) {
            return DEVICE_STATUS_SUCCESS;
        } else {
            return DEVICE_STATUS_VOLUME_MISSING;
        }
#else /* Neither macro is defined. */
        return DEVICE_STATUS_SUCCESS;
#endif
    } else {
        return DEVICE_STATUS_VOLUME_ERROR;
    }
}

void tape_device_detect_capabilities(TapeDevice * t_self) {
    tape_device_set_capabilities(t_self,
	TRUE,  PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT, /* fsf*/
	DEFAULT_FSF_AFTER_FILEMARK, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT, /* fsf_after_filemark*/
	TRUE,  PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT, /* bsf*/
	TRUE,  PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT, /* fsr*/
	TRUE,  PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT, /* bsr*/
	TRUE,  PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT, /* eom*/
	FALSE, PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT, /* bsf_after_eom*/
	2,     PROPERTY_SURETY_BAD, PROPERTY_SOURCE_DEFAULT  /* final_filemarks*/
	);
}
