# SYNOPSIS
#
#   AMANDA_SETUP_FILE_LOCKING
#
# OVERVIEW
#
#   Set up file locking support.  Four locking mechanisms are available:
#     POSIX_FCNTL - use fcntl().  The full job.
#     FLOCK       - use flock().  Does just as well.
#     LOCKF       - use lockf().  Only handles advisory, exclusive,
#                       blocking file locks as used by Amanda.
#     LNLOCK      - Home brew exclusive, blocking file lock.
#
#   For the chosen method, WANT_AMFLOCK_mech is defined and set up as an
#   AM_CONDITIONAL.  Also, LOCKING, which contains the mechanism, is substituted.
#
AC_DEFUN([AMANDA_SETUP_FILE_LOCKING],
[
    AC_CHECK_HEADERS(
        fcntl.h \
        sys/fcntl.h \
	sys/types.h \
	sys/file.h \
	unistd.h \
    )

    # find a working file-locking mechanism.
    # Note: these all use AC_TRY_LINK to make sure that we can compile
    # and link each variant.  They do not try to test the variants --
    # that is left to runtime.
    LOCKING="no"

    # check POSIX locking
    AC_CACHE_CHECK(
       [whether POSIX locking (with fcntl(2)) is available],
       amanda_cv_posix_filelocking,
       [
	    AC_TRY_LINK([
#if HAVE_SYS_TYPES_H
# include <sys/types.h>
#endif
#if HAVE_UNISTD_H
# include <unistd.h>
#endif
#if HAVE_FCNTL_H
# include <fcntl.h>
#endif
	    ], [
	    struct flock lock;

	    lock.l_type = F_RDLCK;
	    lock.l_start = 0;
	    lock.l_whence = SEEK_CUR;
	    lock.l_len = 0;
	    return fcntl(1, F_SETLK, &lock);
	    ], [
	amanda_cv_posix_filelocking="yes"
	    ],[
	amanda_cv_posix_filelocking="no"
	    ])
	])
    if test "x$amanda_cv_posix_filelocking" = xyes; then
	AC_DEFINE(WANT_AMFLOCK_POSIX,1,[Define to use POSIX (fcntl()) for file locking])
	WANT_AMFLOCK_POSIX="yes"
	LOCKING="POSIX_FCNTL"
    fi
    AM_CONDITIONAL(WANT_AMFLOCK_POSIX, test x"$WANT_AMFLOCK_POSIX" = x"yes")

    # check flock-based (BSD) locking
    AC_CACHE_CHECK(
       [whether flock locking is available],
       amanda_cv_flock_filelocking,
       [
	    AC_TRY_LINK([
#if HAVE_SYS_FILE_H
# include <sys/file.h>
#endif
	    ], [
	    return flock(1, LOCK_SH);
	    ], [
	amanda_cv_flock_filelocking="yes"
	    ],[
	amanda_cv_flock_filelocking="no"
	    ])
	])
    if test "x$amanda_cv_flock_filelocking" = xyes; then
	AC_DEFINE(WANT_AMFLOCK_FLOCK,1,[Define to use flock(2) for file locking])
	WANT_AMFLOCK_FLOCK="yes"
	LOCKING="FLOCK"
    fi
    AM_CONDITIONAL(WANT_AMFLOCK_FLOCK, test x"$WANT_AMFLOCK_FLOCK" = x"yes")

    # check lockf-based (SVR2, SVR3, SVR4) locking
    AC_CACHE_CHECK(
       [whether lockf(3) locking is available],
       amanda_cv_lockf_filelocking,
       [
	    AC_TRY_LINK([
#if HAVE_UNISTD_H
# include <unistd.h>
#endif
	    ], [
	    return lockf(1, F_LOCK, 0);
	    ], [
	amanda_cv_lockf_filelocking="yes"
	    ],[
	amanda_cv_lockf_filelocking="no"
	    ])
	])
    if test "x$amanda_cv_lockf_filelocking" = xyes; then
	AC_DEFINE(WANT_AMFLOCK_LOCKF,1,[Define to use lockf(3) for file locking.])
	WANT_AMFLOCK_LOCKF="yes"
	LOCKING="LOCKF"
    fi
    AM_CONDITIONAL(WANT_AMFLOCK_LOCKF, test x"$WANT_AMFLOCK_LOCKF" = x"yes")

    # check our homebrew hardlink-based locking (requires hardlinks)
    AC_CACHE_CHECK(
       [whether link(2) is available for locking],
       amanda_cv_lnlock_filelocking,
       [
	    AC_TRY_LINK([
#if HAVE_UNISTD_H
# include <unistd.h>
#endif
	    ], [
	    return link("/tmp/foo", "/tmp/bar");
	    ], [
	amanda_cv_lnlock_filelocking="yes"
	    ],[
	amanda_cv_lnlock_filelocking="no"
	    ])
	])
    if test "x$amanda_cv_lnlock_filelocking" = xyes; then
	AC_DEFINE(WANT_AMFLOCK_LNLOCK,1,[Define to use link(2) to emulate file locking.])
	WANT_AMFLOCK_LNLOCK="yes"
	LOCKING="LNLOCK"
    fi
    AM_CONDITIONAL(WANT_AMFLOCK_LNLOCK, test x"$WANT_AMFLOCK_LNLOCK" = x"yes")

    if test x"$LOCKING" = "no"; then
	# this shouldn't happen, and is *bad* if it does
	AC_MSG_ERROR([*** No working file locking capability found!])
    fi

    AC_SUBST(LOCKING)
])
