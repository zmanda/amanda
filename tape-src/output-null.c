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
 * $Id: output-null.c,v 1.9 2006/06/02 00:56:06 paddy_s Exp $
 *
 * tapeio.c virtual tape interface for a null device.
 */

#include "amanda.h"

#include "tapeio.h"
#include "output-null.h"
#include "fileheader.h"
#ifndef R_OK
#define R_OK 4
#define W_OK 2
#endif

static off_t *amount_written = NULL;
static size_t open_count = 0;

int
null_tape_open(
    char *	filename,
    int		flags,
    mode_t	mask)
{
    int fd;
    off_t **amount_written_p = &amount_written;

    (void)filename;	/* Quiet unused parameter warning */

    if ((flags & 3) != O_RDONLY) {
	flags &= ~3;
	flags |= O_RDWR;
    }
    if ((fd = open("/dev/null", flags, mask)) >= 0) {
	tapefd_setinfo_fake_label(fd, 1);
	amtable_alloc((void **)amount_written_p,
		      &open_count,
		      SIZEOF(*amount_written),
		      (size_t)(fd + 1),
		      10,
		      NULL);
	amount_written[fd] = (off_t)0;
    }
    return fd;
}

ssize_t
null_tapefd_read(
    int		fd,
    void *	buffer,
    size_t	count)
{
    return read(fd, buffer, count);
}

ssize_t
null_tapefd_write(
    int		fd,
    const void *buffer,
    size_t	count)
{
    ssize_t write_count = (ssize_t)count;
    off_t length;
    off_t kbytes_left;
    ssize_t r;

    if (write_count <= 0) {
	return 0;				/* special case */
    }

    if ((length = tapefd_getinfo_length(fd)) > (off_t)0) {
	kbytes_left = length - amount_written[fd];
	if ((off_t)(write_count / 1024) > kbytes_left) {
	    write_count = (ssize_t)kbytes_left * 1024;
	}
    }
    amount_written[fd] += (off_t)((write_count + 1023) / 1024);
    if (write_count <= 0) {
	errno = ENOSPC;
	r = -1;
    } else {
	r = write(fd, buffer, (size_t)write_count);
    }
    return r;
}

int
null_tapefd_close(
    int	fd)
{
    return close(fd);
}

void
null_tapefd_resetofs(
    int	fd)
{
    (void)fd;	/* Quiet unused parameter warning */
}

int
null_tapefd_status(
    int			 fd,
    struct am_mt_status *stat)
{
    (void)fd;	/* Quiet unused parameter warning */

    memset((void *)stat, 0, SIZEOF(*stat));
    stat->online_valid = 1;
    stat->online = 1;
    return 0;
}

int
null_tape_stat(
     char *	  filename,
     struct stat *buf)
{
    (void)filename;	/* Quiet unused parameter warning */

     return stat("/dev/null", buf);
}

int
null_tape_access(
     char *	filename,
     int	mode)
{
    (void)filename;	/* Quiet unused parameter warning */

     return access("/dev/null", mode);
}

int
null_tapefd_rewind(
    int	fd)
{
    amount_written[fd] = (off_t)0;
    return 0;
}

int
null_tapefd_unload(
    int	fd)
{
    amount_written[fd] = (off_t)0;
    return 0;
}

int
null_tapefd_fsf(
    int		fd,
    off_t	count)
{
    (void)fd;		/* Quiet unused parameter warning */
    (void)count;	/* Quiet unused parameter warning */

    return 0;
}

int
null_tapefd_weof(
    int		fd,
    off_t	count)
{
    (void)fd;		/* Quiet unused parameter warning */
    (void)count;	/* Quiet unused parameter warning */

    return 0;
}

int 
null_tapefd_can_fork(
    int	fd)
{
    (void)fd;		/* Quiet unused parameter warning */

    return 0;
}

