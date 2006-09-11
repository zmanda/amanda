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
 * $Id: output-null.h,v 1.6 2006/05/25 01:47:27 johnfranks Exp $
 *
 * tapeio.c virtual tape interface for a null device.
 */

#ifndef OUTPUT_NULL_H
#define OUTPUT_NULL_H

#include "amanda.h"

int null_tape_access(char *, int);
int null_tape_open(char *, int, mode_t);
int null_tape_stat(char *, struct stat *);
int null_tapefd_close(int);
int null_tapefd_fsf(int, off_t);
ssize_t null_tapefd_read(int, void *, size_t);
int null_tapefd_rewind(int);
void null_tapefd_resetofs(int);
int null_tapefd_unload(int);
int null_tapefd_status(int, struct am_mt_status *);
int null_tapefd_weof(int, off_t);
ssize_t null_tapefd_write(int, const void *, size_t);
int null_tapefd_can_fork(int);

#endif /* OUTPUT_NULL_H */
