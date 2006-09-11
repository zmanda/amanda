/*
 * Amanda, The Advanced Maryland Automatic Network Disk Archiver
 * Copyright (c) 1991-1998 University of Maryland at College Park
 * All Rights Reserved.
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of U.M. not be used in advertising or
 * publicity pertaining to distribution of the software without specific,
 * written prior permission.  U.M. makes no representations about the
 * suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 *
 * U.M. DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING ALL
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS, IN NO EVENT SHALL U.M.
 * BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION
 * OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Author: James da Silva, Systems Design and Analysis Group
 *			   Computer Science Department
 *			   University of Maryland at College Park
 */

/*
 * $Id: output-tape.c,v 1.18 2006/08/22 14:19:39 martinea Exp $
 *
 * tapeio.c virtual tape interface for normal tape drives.
 */

#ifdef NO_AMANDA
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#else
#include "amanda.h"
#include "tapeio.h"
#endif

#include "output-tape.h"

#ifndef NO_AMANDA
#include "fileheader.h"
#endif

#ifndef R_OK
#define R_OK 4
#define W_OK 2
#endif

/*
=======================================================================
** Here are the ioctl() interface routines, which are #ifdef-ed
** heavily by platform.
=======================================================================
*/

#if defined(HAVE_BROKEN_FSF)						/* { */
/*
 * tape_tapefd_fsf -- handle systems that have a broken fsf operation
 * and cannot do an fsf operation unless they are positioned at a tape
 * mark (or BOT).  Sheesh!  This shows up in amrestore as I/O errors
 * when skipping.
 */

int
tape_tapefd_fsf(
    int fd,
    off_t count)
{
    size_t buflen;
    char *buffer = NULL;
    int len = 0;

    buflen = MAX_TAPE_BLOCK_BYTES;
    buffer = alloc(buflen);

    while(--count >= 0) {
	while((len = tapefd_read(fd, buffer, buflen)) > 0) {}
	if(len < 0) {
	    break;
	}
    }
    amfree(buffer);
    return len;
}
#endif									/* } */

#ifdef UWARE_TAPEIO							/* { */

#include <sys/tape.h>

/*
 * Rewind a tape to the beginning.
 */
int
tape_tapefd_rewind(
    int fd)
{
    int st;

    return ioctl(fd, T_RWD, &st);
}

/*
 * Rewind and unload a tape.
 */
int
tape_tapefd_unload(
    int fd)
{
    int st;

    return ioctl(fd, T_OFFL, &st);
}

#if !defined(HAVE_BROKEN_FSF)
/*
 * Forward space the tape device count files.
 */
int
tape_tapefd_fsf(
    int fd,
    off_t count)
{
    int st;
    int status;

    while (--count >= 0) {
        if ((status = ioctl(fd, T_SFF, &st)) != 0) {
            break;
	}
    }

    return status;
}
#endif

/*
 * Write some number of end of file marks (a.k.a. tape marks).
 */
int
tape_tapefd_weof(
    int fd,
    off_t count)
{
    int st;
    int status;

    while (--count >= (off_t)0) {
        if ((status = ioctl(fd, T_WRFILEM, &st)) != 0) {
            break;
	}
    }

    return status;
}

#else									/* }{ */
#ifdef AIX_TAPEIO							/* { */

#include <sys/tape.h>

/*
 * Rewind a tape to the beginning.
 */
int
tape_tapefd_rewind(
    int fd)
{
    struct stop st;

    st.st_op = STREW;
    st.st_count = 1;

    return ioctl(fd, STIOCTOP, &st);
}

/*
 * Rewind and unload a tape.
 */
int
tape_tapefd_unload(
    int fd)
{
    struct stop st;

    st.st_op = STOFFL;
    st.st_count = 1;

    return ioctl(fd, STIOCTOP, &st);
}

#if !defined(HAVE_BROKEN_FSF)
/*
 * Forward space the tape device count files.
 */
int
tape_tapefd_fsf(
    int fd,
    off_t count)
{
    struct stop st;

    if ((count > (off_t)INT_MAX) || (count < (off_t)INT_MIN)) {
#ifdef EOVERFLOW
    	    errno = EOVERFLOW;
#else
	    errno = EINVAL;
#endif
	return -1;
    }

    st.st_op = STFSF;
    st.st_count = (int)count;

    return ioctl(fd, STIOCTOP, &st);
}
#endif

/*
 * Write some number of end of file marks (a.k.a. tape marks).
 */
int
tape_tapefd_weof(
    int fd,
    off_t count)
{
    struct stop st;

    if ((count > (off_t)INT_MAX) || (count < (off_t)INT_MIN)) {
#ifdef EOVERFLOW
    	    errno = EOVERFLOW;
#else
	    errno = EINVAL;
#endif
	return -1;
    }

    st.st_op = STWEOF;
    st.st_count = (int)count;

    return ioctl(fd, STIOCTOP, &st);
}

#else /* AIX_TAPEIO */							/* }{ */
#ifdef XENIX_TAPEIO							/* { */

#include <sys/tape.h>

/*
 * Rewind a tape to the beginning.
 */
int
tape_tapefd_rewind(
    int fd)
{
    int st;

    return ioctl(fd, MT_REWIND, &st);
}

/*
 * Rewind and unload a tape.
 */
int
tape_tapefd_unload(
    int fd)
{
    int st;
    int f;

#ifdef MT_OFFLINE
    f = MT_OFFLINE;
#else
#ifdef MT_UNLOAD
    f = MT_UNLOAD;
#else
    f = syntax error;
#endif
#endif
    return ioctl(fd, f, &st);
}

#if !defined(HAVE_BROKEN_FSF)
/*
 * Forward space the tape device count files.
 */
int
tape_tapefd_fsf(
    int fd,
    off_t count)
{
    int st;
    int status;

    while (--count >= 0) {
	if ((status = ioctl(fd, MT_RFM, &st)) != 0) {
	    break;
	}
    }

    return status;
}
#endif

/*
 * Write some number of end of file marks (a.k.a. tape marks).
 */
int
tape_tapefd_weof(
    int fd,
    off_t count)
{
    int st;
    int c;
    int status;

    while (--count >= 0) {
	if ((status = ioctl(fd, MT_WFM, &st)) != 0) {
	    break;
	}
    }

    return status;
}

#else	/* ! AIX_TAPEIO && !XENIX_TAPEIO */				/* }{ */

#include <sys/mtio.h>

/*
 * Rewind a tape to the beginning.
 */
int
tape_tapefd_rewind(
    int fd)
{
    struct mtop mt;
    int rc=-1, cnt;

    mt.mt_op = MTREW;
    mt.mt_count = 1;

    /*
     * EXB-8200 drive on FreeBSD can fail to rewind, but retrying won't
     * hurt, and it will usually even work!
     */
    for(cnt = 10; cnt >= 0; --cnt) {
	if ((rc = ioctl(fd, MTIOCTOP, &mt)) == 0) {
	    break;
	}
	if (cnt) {
	    sleep(3);
	}
    }
    return rc;
}

/*
 * Rewind and unload a tape.
 */
int
tape_tapefd_unload(
    int fd)
{
    struct mtop mt;
    int rc=-1, cnt;

#ifdef MTUNLOAD
    mt.mt_op = MTUNLOAD;
#else
#ifdef MTOFFL
    mt.mt_op = MTOFFL;
#else
    mt.mt_op = syntax error;
#endif
#endif
    mt.mt_count = 1;

    /*
     * EXB-8200 drive on FreeBSD can fail to unload, but retrying won't
     * hurt, and it will usually even work!
     */
    for(cnt = 10; cnt >= 0; --cnt) {
	if ((rc = ioctl(fd, MTIOCTOP, &mt)) == 0) {
	    break;
	}
	if (cnt) {
	    sleep(3);
	}
    }
    return rc;
}

#if !defined(HAVE_BROKEN_FSF)
/*
 * Forward space the tape device count files.
 */
int
tape_tapefd_fsf(
    int fd,
    off_t count)
{
    struct mtop mt;

    if ((count > (off_t)INT_MAX) || (count < (off_t)INT_MIN)) {
#ifdef EOVERFLOW
    	    errno = EOVERFLOW;
#else
	    errno = EINVAL;
#endif
	return -1;
    }

    mt.mt_op = MTFSF;
    mt.mt_count = (int)count;

    return ioctl(fd, MTIOCTOP, &mt);
}
#endif

/*
 * Write some number of end of file marks (a.k.a. tape marks).
 *
 * write <count> filemarks on the tape.
 */
int
tape_tapefd_weof(
    int fd,
    off_t count)
{
    struct mtop mt;

    if ((count > (off_t)INT_MAX) || (count < (off_t)INT_MIN)) {
#ifdef EOVERFLOW
    	    errno = EOVERFLOW;
#else
	    errno = EINVAL;
#endif
	return -1;
    }

    mt.mt_op = MTWEOF;
    mt.mt_count = (int)count;

    return ioctl(fd, MTIOCTOP, &mt);
}

#endif /* !XENIX_TAPEIO */						/* } */
#endif /* !AIX_TAPEIO */						/* } */
#endif /* !UWARE_TAPEIO */						/* } */

/*
 * At this point we have pulled in every conceivable #include file :-),
 * so now come the more general routines with minimal #ifdef-ing.
 */

#ifdef HAVE_LINUX_ZFTAPE_H
/*
 * is_zftape(filename) checks if filename is a valid ftape device name.
 */
int
is_zftape(
    const char *filename)
{
    if (strncmp(filename, "/dev/nftape", 11) == 0) return(1);
    if (strncmp(filename, "/dev/nqft",    9) == 0) return(1);
    if (strncmp(filename, "/dev/nrft",    9) == 0) return(1);
    return(0);
}
#endif /* HAVE_LINUX_ZFTAPE_H */

int
tape_tape_open(
    char *filename,
    int flags,
    mode_t mask)
{
    int ret;
    time_t timeout = 200;
    unsigned delay = 2;

    if ((flags & 3) != O_RDONLY) {
	flags &= ~3;
	flags |= O_RDWR;
    }
    ret = open(filename, flags, mask);
    while (ret < 0) {
	if ((errno != EAGAIN) && (errno != EBUSY) && (errno != EINTR)) {
	    /*
	     * Open failed completely: just return
	     */
	    fprintf(stderr, "Opening tapedev %s: got error %s.\n",
			filename, strerror(errno));
	    return -1;
	}

	/*
	 * if tape open fails with errno==EAGAIN, EBUSY or EINTR, it
	 * may be worth retrying a few seconds later.
	 */
	timeout -= delay;
	if (timeout <= 0) {
	    /* Open failed: just return */
	    fprintf(stderr, "Opening tapedev %s: not ready.\n", filename);
	    return -1;
	}

	if (delay < 16)
	    delay *= 2;

	sleep(delay);
	ret = open(filename, flags, mask);
    }

#ifdef MTIOCGET
    /* Now check that we opened a tape device. */
    {
	struct mtget mt;

	memset(&mt, 0, SIZEOF(mt));
	if (ioctl(ret, MTIOCGET, &mt) < 0) {
	    close(ret);
	    fprintf(stderr, "tapedev %s is not a tape device!\n", filename);
	    return -1;
	}

#ifdef GMT_ONLINE
	if (!GMT_ONLINE(mt.mt_gstat)) {
	    close(ret);
	    fprintf(stderr, "tapedev %s is offline or has no loaded tape.\n",
		    filename);
	    return -1;
	}
#endif /* GMT_ONLINE */
    }
#endif /* MTIOCGET */


#ifdef HAVE_LINUX_ZFTAPE_H
    /*
     * switch the block size for the zftape driver (3.04d)
     * (its default is 10kb and not 32kb)
     *        A. Gebhardt <albrecht.gebhardt@uni-klu.ac.at>
     */
    if (is_zftape(filename) == 1) {
	struct mtop mt;

	mt.mt_op = MTSETBLK;
	mt.mt_count = 32 * 1024;		/* should be blocksize ??? */
	ioctl(ret, MTIOCTOP, &mt);
    }
#endif /* HAVE_LINUX_ZFTAPE_H */
    return ret;
}

ssize_t
tape_tapefd_read(
    int fd,
    void *buffer,
    size_t count)
{
    return read(fd, buffer, count);
}

ssize_t
tape_tapefd_write(
    int fd,
    const void *buffer,
    size_t count)
{
    return write(fd, buffer, count);
}

int
tape_tapefd_close(
    int fd)
{
    return close(fd);
} 

void
tape_tapefd_resetofs(
    int fd)
{
    /*
     * this *should* be a no-op on the tape, but resets the kernel's view
     * of the file offset, preventing it from barfing should we pass the
     * filesize limit (eg OSes with 2 GB filesize limits) on a long tape.
     */
    if (lseek(fd, (off_t)0, SEEK_SET) < 0) {
	dbprintf(("tape_tapefd_resetofs: lseek failed: <%s>\n",
		  strerror(errno)));
    }
}

int
tape_tapefd_status(
    int fd,
    struct am_mt_status *stat)
{
    int res;
    int anything_valid = 0;
#if defined(MTIOCGET)
    struct mtget buf;
#endif

    memset((void *)stat, 0, SIZEOF(*stat));

#if defined(MTIOCGET)							/* { */
    res = ioctl(fd,MTIOCGET,&buf);
    if (res >= 0) {
	/*@ignore@*/
#ifdef MT_ONL								/* { */
        /* IRIX-ish system */
	anything_valid = 1;
	stat->online_valid = 1;
	stat->online = (0 != (buf.mt_dposn & MT_ONL));
	stat->bot_valid = 1;
	stat->bot = (0 != (buf.mt_dposn & MT_BOT));
	stat->eot_valid = 1;
	stat->eot = (0 != (buf.mt_dposn & MT_EOT));
	stat->protected_valid = 1;
	stat->protected = (0 != (buf.mt_dposn & MT_WPROT));
#else									/* }{ */
#ifdef GMT_ONLINE							/* { */
        /* Linux-ish system */
	anything_valid = 1;
	stat->online_valid = 1;
	stat->online = (0 != GMT_ONLINE(buf.mt_gstat));
	stat->bot_valid = 1;
	stat->bot = (0 != GMT_BOT(buf.mt_gstat));
	stat->eot_valid = 1;
	stat->eot = (0 != GMT_EOT(buf.mt_gstat));
	stat->protected_valid = 1;
	stat->protected = (0 != GMT_WR_PROT(buf.mt_gstat));
#else									/* }{ */
#ifdef DEV_BOM								/* { */
        /* OSF1-ish system */
	anything_valid = 1;
	stat->online_valid = 1;
	stat->online = (0 == (DEV_OFFLINE & buf.mt_dsreg));
	stat->bot_valid = 1;
	stat->bot = (0 != (DEV_BOM & buf.mt_dsreg));
	stat->protected_valid = 1;
	stat->protected = (0 != (DEV_WRTLCK & buf.mt_dsreg));
#else									/* }{ */
        /* Solaris, minix, etc. */
	anything_valid = 1;
	stat->online_valid = 1;
	stat->online = 1;			/* ioctl fails otherwise */
#ifdef HAVE_MT_DSREG
	stat->device_status_valid = 1;
	stat->device_status_size = SIZEOF(buf.mt_dsreg);
	stat->device_status = (unsigned long)buf.mt_dsreg;
#endif
#ifdef HAVE_MT_ERREG
	stat->error_status_valid = 1;
	stat->error_status_size = SIZEOF(buf.mt_erreg);
	stat->error_status = (unsigned long)buf.mt_erreg;
#endif
#if defined(HAVE_MT_FLAGS) && defined(MTF_SCSI)			/* { */
	/* 
	 * On Solaris, the file/block number fields are only valid if
	 * the driver is SCSI.  And in that case, the dsreg value is
	 * not useful (it is a retry count).
	 */
	if(buf.mt_flags & MTF_SCSI) {
	    stat->device_status_valid = 0;
#ifdef HAVE_MT_FILENO
	    stat->fileno_valid = 1;
	    stat->fileno = (long)buf.mt_fileno;
#endif
#ifdef HAVE_MT_BLKNO
	    stat->blkno_valid = 1;
	    stat->blkno = (long)buf.mt_blkno;
#endif
	}
#endif									/* } */
#endif									/* } */
#endif									/* } */
#endif									/* } */
	/*@end@*/
    }
#endif									/* } */

    /*
     * If we did not find any valid information, do a stat on the device
     * and if that returns successfully, assume it is at least online.
     */
    if(!anything_valid) {
	struct stat sbuf;

	res = fstat(fd, &sbuf);
	stat->online_valid = 1;
	stat->online = (char)(res == 0);
    }

    return res;
}

int
tape_tape_stat(
     char *filename,
     struct stat *buf)
{
     return stat(filename, buf);
}

int
tape_tape_access(
     char *filename,
     int mode)
{
     return access(filename, mode);
}

int 
tape_tapefd_can_fork(
    int fd)
{
    (void)fd;	/* Quiet unused parameter warning */

    return 1;
}

