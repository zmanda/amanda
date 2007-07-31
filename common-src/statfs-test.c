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
 * $Id: statfs.c 6512 2007-05-24 17:00:24Z ian $
 *
 * test scaffolding for statfs
 */
#include "amanda.h"
#include "statfs.h"

int
main(
    int		argc,
    char **	argv)
{
    generic_fs_stats_t statbuf;
    (void)argc;
    (void)argv;

    printf(
"name                            total     free    avail   files   ffree  favail\n"
	   );
    printf(
"---------------------------- -------- -------- -------- ------- ------- -------\n"
	   );

    if(get_fs_stats("/tmp", &statbuf) == -1) {
	perror("get_fs_stats");
	return 1;
    }
    printf("%-28.28s %8ld %8ld %8ld %7ld %7ld %7ld\n", "/tmp",
	   statbuf.total, statbuf.free, statbuf.avail,
	   statbuf.files, statbuf.ffree, statbuf.favail);
    return 0;
}
