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
 * $Id: statfs.c,v 1.16 2006/08/24 17:05:35 martinea Exp $
 *
 * a generic statfs-like routine
 */
#include "amanda.h"
#include "statfs.h"

/*
 * You are in a maze of twisty passages, all alike.
 * Begin retching now.
 */

#ifdef STATFS_ULTRIX
# include <sys/param.h>
# include <sys/mount.h>
# define STATFS_STRUCT	struct fs_data
# define STATFS_TOTAL(buf)	(buf).fd_btot
# define STATFS_AVAIL(buf)	(buf).fd_bfreen
# define STATFS_FREE(buf)	(buf).fd_bfree
# define STATFS_FILES(buf)	(buf).fd_gtot
# define STATFS_FAVAIL(buf)	(buf).fd_gfree
# define STATFS_FFREE(buf)	(buf).fd_gfree
# define STATFS_SCALE(buf)	1024
# define STATFS(path, buffer)	statfs(path, &buffer)
#else
# if defined(HAVE_SYS_STATVFS_H) && !defined(STATFS_SCO_OS5)
/*
** System V.4 (STATFS_SVR4)
*/
#  include <sys/statvfs.h>
#  define STATFS_TYP		"SVR4 (Irix-5+, Solaris-2, Linux glibc 2.1)"
#  define STATFS_STRUCT	struct statvfs
#  define STATFS_TOTAL(buf)	(buf).f_blocks
#  define STATFS_AVAIL(buf)	(buf).f_bavail
#  define STATFS_FREE(buf)	(buf).f_bfree
#  define STATFS_FILES(buf)	(buf).f_files
#  define STATFS_FAVAIL(buf)	(buf).f_favail
#  define STATFS_FFREE(buf)	(buf).f_ffree
#  define STATFS_SCALE(buf)	((buf).f_frsize?(buf).f_frsize:(buf).f_bsize)
#  define STATFS(path, buffer)	statvfs(path, &buffer)
# else
#  if HAVE_SYS_VFS_H
/*
** (STATFS_AIX, STATFS_VFS, STATFS_NEXT)
*/
#   ifdef HAVE_SYS_STATFS_H /* AIX */
#    include <sys/statfs.h>
#   endif
#   include <sys/vfs.h>
#   define STATFS_TYP		"Posix (NeXTstep, AIX, Linux, HP-UX)"
#   define STATFS_STRUCT	struct statfs
#   define STATFS_TOTAL(buf)	(buf).f_blocks
#   define STATFS_AVAIL(buf)	(buf).f_bavail
#   define STATFS_FREE(buf)	(buf).f_bfree
#   define STATFS_FILES(buf)	(buf).f_files
#   define STATFS_FAVAIL(buf)	(buf).f_ffree
#   define STATFS_FFREE(buf)	(buf).f_ffree
#   define STATFS_SCALE(buf)	(buf).f_bsize
#   define STATFS(path, buffer)	statfs(path, &buffer)
#  else
#   if HAVE_SYS_STATFS_H
/*
** System V.3 (STATFS_SVR3)
*/
#    include <sys/statfs.h>
#    define STATFS_TYP		"SVR3 (Irix-3, Irix-4)"
#    define STATFS_STRUCT	struct statfs
#    define STATFS_TOTAL(buf)	(buf).f_blocks
#    define STATFS_AVAIL(buf)	(buf).f_bfree
#    define STATFS_FREE(buf)	(buf).f_bfree
#    define STATFS_FILES(buf)	(buf).f_files
#    define STATFS_FAVAIL(buf)	(buf).f_ffree
#    define STATFS_FFREE(buf)	(buf).f_ffree
#    define STATFS_SCALE(buf)	(buf).f_bsize
#    define STATFS(path, buffer)	statfs(path, &buffer, SIZEOF(STATFS_STRUCT), 0)
#   else
#    if HAVE_SYS_MOUNT_H
/*
** BSD (STATFS_BSD43, STATFS_BSD44)
*/
#     ifdef HAVE_SYS_PARAM_H /* BSD-4.4 */
#      include <sys/param.h>
#     endif
#     include <sys/mount.h>
#     define STATFS_TYP		"BSD43/44"
#     define STATFS_STRUCT	struct statfs
#     define STATFS_TOTAL(buf)	(buf).f_blocks
#     define STATFS_AVAIL(buf)	(buf).f_bavail
#     define STATFS_FREE(buf)	(buf).f_bfree
#     define STATFS_FILES(buf)	(buf).f_files
#     define STATFS_FAVAIL(buf)	(buf).f_ffree
#     define STATFS_FFREE(buf)	(buf).f_ffree
#     define STATFS_SCALE(buf)	(buf).f_bsize
#     define STATFS(path, buffer)	statfs(path, &buffer)
#     ifdef STATFS_OSF1
#      define STATFS(path, buffer)	statfs(path, &buffer, SIZEOF(STATFS_STRUCT))
#     endif
#    endif
#   endif
#  endif
# endif
#endif


off_t scale(off_t r, off_t s);

off_t
scale(
    off_t r,
    off_t s)
{
    if (r == (off_t)-1)
	return (off_t)-1;
    return r*(s/(off_t)1024);
}

int
get_fs_stats(
    char *		dir,
    generic_fs_stats_t *sp)
{
    STATFS_STRUCT statbuf;

    if(STATFS(dir, statbuf) == -1)
	return -1;

    /* total, avail, free: converted to kbytes, rounded down */

    sp->total = scale((off_t)STATFS_TOTAL(statbuf),
		      (off_t)STATFS_SCALE(statbuf));
    sp->avail = scale((off_t)STATFS_AVAIL(statbuf),
		      (off_t)STATFS_SCALE(statbuf));
    sp->free  = scale((off_t)STATFS_FREE(statbuf),
		      (off_t)STATFS_SCALE(statbuf));

    /* inode stats */

    sp->files  = (off_t)STATFS_FILES(statbuf);
    sp->favail = (off_t)STATFS_FAVAIL(statbuf);
    sp->ffree  = (off_t)STATFS_FFREE(statbuf);

    return 0;
}

#ifdef TEST
/* ----- test scaffolding ----- */

int
main(
    int		argc,
    char **	argv)
{
    generic_fs_stats_t statbuf;

    safe_fd(-1, 0);

    set_pname(argv[0]);

    dbopen(NULL);

    if(argc < 2) {
	fprintf(stderr, "Usage: %s files ...\n", get_pname());
	return 1;
    }

    printf("statfs (%s)\n",STATFS_TYP);
    printf(
"name                             total    free   avail  files  ffree favail\n"
	   );
    printf(
"------------------------------ ------- ------- ------- ------ ------ ------\n"
	   );

    do {
	argc--,argv++;
	if(get_fs_stats(*argv, &statbuf) == -1) {
	    perror(*argv);
	    continue;
	}
	printf("%-30.30s %7ld %7ld %7ld %6ld %6ld %6ld\n", *argv,
	       statbuf.total, statbuf.free, statbuf.avail,
	       statbuf.files, statbuf.ffree, statbuf.favail);
    } while(argc > 1);
    return 0;
}
#endif
