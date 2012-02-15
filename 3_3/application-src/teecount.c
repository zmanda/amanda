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
 * $Id: teecount.c $
 *
 * read stdin and write to to stdout
 * print on stderr the number of bytes copied
 */

#include "amanda.h"

#define BUFFER 262144

int main(int argc, char **argv);
int
main(
    int argc G_GNUC_UNUSED,
    char **argv G_GNUC_UNUSED)
{
    char  buffer[BUFFER];
    off_t total = 0;
    off_t size;
    off_t sizew;

    while ((size = safe_read(0, buffer, BUFFER)) > 0) {
	if ((sizew = full_write(1, buffer, size)) < size) {
	    total += sizew;
	    fprintf(stderr, "%ju", (uintmax_t)total);
	    exit (-1);
	}
	total += size;
    }
    fprintf(stderr, "%ju", (uintmax_t)total);
    if (size < 0) {
	exit(-1);
    }
    return 0;
}
