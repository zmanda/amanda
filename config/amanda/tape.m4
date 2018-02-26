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
#   Set up for the 'tape' device.  WANT_TAPE_DEVICE is defined and
#   AM_CONDITIONAL'd if the tape device should be supported.
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
#include <sys/types.h>
#ifdef HAVE_SYS_TIME_H
# include <sys/time.h>
# endif
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

    # maybe we have no tape device at all (e.g., Mac OS X)?
    if test x"$HAVE_MTIOCTOP" = x"yes"; then
	want_tape_device=yes
	AC_DEFINE(WANT_TAPE_DEVICE, 1, [Define if the tape-device will be built])
    fi
    AM_CONDITIONAL(WANT_TAPE_DEVICE, test -n "$want_tape_device")

    #
    # Check for various "mt status" related structure elements.
    #
    AC_MSG_CHECKING([for mt_flags mtget structure element])
    AC_COMPILE_IFELSE([AC_LANG_PROGRAM([[
#include <stdio.h>
#include <sys/types.h>
#ifdef HAVE_SYS_TIME_H
# include <sys/time.h>
#endif
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
#ifdef HAVE_SYS_TIME_H
# include <sys/time.h>
#endif
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
#ifdef HAVE_SYS_TIME_H
# include <sys/time.h>
#endif
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
