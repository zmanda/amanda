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
 * Authors: the Amanda Development Team.  Its members are listed in a
 * file named AUTHORS, in the root directory of this distribution.
 */
/*
 * $Id: writev.c,v 1.2 1999/03/04 22:40:08 kashmir Exp $
 *
 * emulation of writev that simply copies all iovecs into a contiguous
 * buffer and writes that out in one write.
 */

#include "amanda.h"
#ifndef HAVE_WRITEV

ssize_t
writev(fd, iov, iovcnt)
    int fd;
    const struct iovec *iov;
    int iovcnt;
{
    size_t bufsize;
    int i;
    ssize_t ret;
    char *buf, *writeptr;

    /*
     * Get the sum of the iovecs, and allocate a suitably sized buffer
     */
    for (bufsize = 0, i = 0; i < iovcnt; i++)
	bufsize += iov[i].iov_len;
    buf = alloc(bufsize);

    /*
     * Copy each iovec back to back into the buffer
     */
    for (writeptr = buf, i = 0; i < iovcnt; i++) {
	memcpy(writeptr, iov[i].iov_base, iov[i].iov_len);
	writeptr += iov[i].iov_len;
    }

    /*
     * Write the buffer, free it, and return write's result
     */
    ret = write(fd, buf, bufsize);
    amfree(buf);
    return (ret);
}
#endif	/* HAVE_WRITEV */
