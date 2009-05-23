# SYNOPSIS
#
#   AMANDA_WITH_MAXTAPEBLOCKSIZE
#
# OVERVIEW
#
#   Implement the deprecated --with-maxtapeblocksize option.
#
AC_DEFUN([AMANDA_WITH_MAXTAPEBLOCKSIZE], [
    AC_ARG_WITH(maxtapeblocksize, [(deprecated)],
	[ AMANDA_MSG_WARN([--with-maxtapeblocksize is no longer needed]) ]
    )
])

# SYNOPSIS
#
#   AMANDA_TAPE_DEVICE
#
# OVERVIEW
#
#   Set up for the 'tape' device.  One of the conditionals WANT_TAPE_XENIX,
#   WANT_TAPE_AIX, WANT_TAPE_UWARE, and WANT_TAPE_POSIX will be true; the
#   corresponding symbols are also DEFINEd.  Finally, WANT_TAPE_DEVICE is
#   defined nad AM_CONDITIONAL'd if the tape device should be supported (if
#   at least one of the backends is available).
#
#   If 'struct mtget' fields mt_flags, mt_fileno, mt_blkno, mt_dsreg, and 
#   mt_erreg, the corresponding HAVE_MT_* is DEFINEd.
#
AC_DEFUN([AMANDA_TAPE_DEVICE], [
    AC_CHECK_HEADERS( \
	linux/zftape.h \
	sys/tape.h \
	sys/mtio.h \
	)

    # check for MTIOCTOP, an indicator of POSIX tape support
    AC_CACHE_CHECK([for MTIOCTOP], amanda_cv_HAVE_MTIOCTOP,[
	AC_TRY_COMPILE([
#ifdef HAVE_SYS_TAPE_H
# include <sys/tape.h>
#endif
#ifdef HAVE_SYS_MTIO_H
# include <sys/mtio.h>
#endif
#ifndef MTIOCTOP
#error MTIOCTOP not defined
#endif
	    ],
	    [ int dummy = 0; ],
	    amanda_cv_HAVE_MTIOCTOP=yes,
	    amanda_cv_HAVE_MTIOCTOP=no,
	    amanda_cv_HAVE_MTIOCTOP=no)]

	HAVE_MTIOCTOP=$amanda_cv_HAVE_MTIOCTOP
    )

    # decide which tape device to compile (arranged in such a way that
    # only one actually gets compiled)
    case "$host" in
      *-ibm-aix*) aix_tapeio=yes ;;
      *-sysv4.2uw2*) uware_tapeio=yes ;;
      *-sco3.2v5*) xenix_tapeio=yes ;;
      i386-pc-isc4*) xenix_tapeio=yes ;;
    esac

    # maybe we have no tape device at all (e.g., Mac OS X)?
    if test -n "$xenix_tapeio" ||
       test -n "$aix_tapeio" ||
       test -n "$uware_tapeio" ||
       test -n "$HAVE_MTIOCTOP"; then
	want_tape_device=yes
	AC_DEFINE(WANT_TAPE_DEVICE, 1, [Define if the tape-device will be built])
    fi

    AM_CONDITIONAL(WANT_TAPE_XENIX, test -n "$xenix_tapeio")
    AM_CONDITIONAL(WANT_TAPE_AIX, test -n "$aix_tapeio")
    AM_CONDITIONAL(WANT_TAPE_UWARE, test -n "$uware_tapeio")
    AM_CONDITIONAL(WANT_TAPE_POSIX, test -n "$HAVE_MTIOCTOP")
    AM_CONDITIONAL(WANT_TAPE_DEVICE, test -n "$want_tape_device")

    if test -n "$xenix_tapeio"; then
      AC_DEFINE(WANT_TAPE_XENIX,1,[Define on XENIX/ISC. ])
    fi

    if test -n "$aix_tapeio"; then
      AC_DEFINE(WANT_TAPE_AIX,1,[Define on AIX. ])
    fi

    if test -n "$uware_tapeio"; then
      AC_DEFINE(WANT_TAPE_UWARE,1,[Define on UnixWare. ])
    fi

    #
    # Check for various "mt status" related structure elements.
    #
    AC_MSG_CHECKING([for mt_flags mtget structure element])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
#include <sys/types.h>
#include <sys/mtio.h>
	]], [[
	    struct mtget buf;
	    long ds;

	    ds = buf.mt_flags;
	]])],[
	    AC_MSG_RESULT(yes)
	    AC_DEFINE(HAVE_MT_FLAGS,1,
		[Define if the mtget structure has an mt_flags field])
	],[
	    AC_MSG_RESULT(no)
	])

    AC_MSG_CHECKING([for mt_fileno mtget structure element])
    mt_fileno_result="found"
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
#include <sys/types.h>
#include <sys/mtio.h>
	]], [[
	    struct mtget buf;
	    long ds;

	    ds = buf.mt_fileno;
	]])],[
	    AC_MSG_RESULT(yes)
	    AC_DEFINE(HAVE_MT_FILENO,1,
		[Define if the mtget structure has an mt_fileno field])
	],[
	    AC_MSG_RESULT(no)
	])

    AC_MSG_CHECKING(for mt_blkno mtget structure element)
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
#include <sys/types.h>
#include <sys/mtio.h>
	]], [[
	    struct mtget buf;
	    long ds;

	    ds = buf.mt_blkno;
	]])],[
	    AC_MSG_RESULT(yes)
	    AC_DEFINE(HAVE_MT_BLKNO,1,
		[Define if the mtget structure has an mt_blkno field])
	],[
	    AC_MSG_RESULT(no)
	])

    AC_MSG_CHECKING(for mt_dsreg mtget structure element)
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
#include <sys/types.h>
#include <sys/mtio.h>
	]], [[
	    struct mtget buf;
	    long ds;

	    ds = buf.mt_dsreg;
	]])],[
	    AC_MSG_RESULT(yes)
	    AC_DEFINE(HAVE_MT_DSREG,1,
		[Define if the mtget structure has an mt_dsreg field])
	],[
	    AC_MSG_RESULT(no)
	])

    AC_MSG_CHECKING(for mt_erreg mtget structure element)
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
#include <sys/types.h>
#include <sys/mtio.h>
	]], [[
	    struct mtget buf;
	    long ds;

	    ds = buf.mt_erreg;
	]])],[
	    AC_MSG_RESULT(yes)
	    AC_DEFINE(HAVE_MT_ERREG,1,
		[Define if the mtget structure has an mt_erreg field])
	],[
	    AC_MSG_RESULT(no)
	])

    case "$host" in
	*linux*) AC_DEFINE(DEFAULT_TAPE_NON_BLOCKING_OPEN,1,
			[Define if open of tape device require O_NONBLOCK]);;
    esac
])
