# OVERVIEW
#
#   Code to handle searches for programs Amanda needs.
#
#   Because Amanda uses a customized search path, many macros which are standard
#   in autoconf have been wrapped here.  Where this is the only change, the description
#   of those macros has been omitted.
#
#   All of these macros indicate their requirements using AC_REQUIRE, so the order in
#   which they are called in configure.in is inconsequential.

# SYNOPSIS
#
#   AMANDA_INIT_PROGS
#
# OVERVIEW
#
#   Set up some amanda-specific path directories.  This should be AC_REQUIRE()d by
#   any macros which need to search for a program.
#
#   SYSPATH is a list of likely system locations for a file, while
#   LOCPATH is a list of likely local locations.  The two are combined
#   in different orders in SYSLOCPATH and LOCSYSPATH.  These path differences
#   are known to affect Solaris 8.
#
AC_DEFUN([AMANDA_INIT_PROGS],
[
    SYSPATH="/bin:/usr/bin:/sbin:/usr/sbin:/opt/SUNWspro/bin:/usr/ucb:/usr/sfw/bin:/usr/bsd:/etc:/usr/etc"
    # expand prefix or exec_prefix in LOCPATH
    LOCPATH=`(
	test "x$prefix" = xNONE && prefix=$ac_default_prefix
	test "x$exec_prefix" = xNONE && exec_prefix=${prefix}
	eval echo "$libexecdir:$PATH:/usr/local/sbin:/usr/local/bin:/usr/ccs/bin"
    )`
    SYSLOCPATH="$SYSPATH:$LOCPATH"
    LOCSYSPATH="$LOCPATH:$SYSPATH"
])

# SYNOPSIS
#
#   AMANDA_PROG_LINT
#
# OVERVIEW
#
#   Find a lint binary (either lint or splint) and record its name in AMLINT.
#   Set up appropriate flags for the discovered binary in AMLINTFLAGS
#
AC_DEFUN([AMANDA_PROG_LINT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_REQUIRE([AMANDA_PROG_GREP])

    AC_PATH_PROG(AMLINT,lint,,/opt/SUNWspro/bin:$SYSLOCPATH)
    if test ! -z "$AMLINT"; then
      $AMLINT -flags 2>&1 | $GREP -- '-errfmt=' > /dev/null
      if test $? -eq 0; then
	AMLINTFLAGS="-n -s -u -m -x"
	AMLINTFLAGS="$AMLINTFLAGS -errchk=%all"
	AMLINTFLAGS="$AMLINTFLAGS -errfmt=macro"
	AMLINTFLAGS="$AMLINTFLAGS -errhdr=no%/usr/include"
	AMLINTFLAGS="$AMLINTFLAGS -errhdr=%user"
	AMLINTFLAGS="$AMLINTFLAGS -errsecurity=extended"
	AMLINTFLAGS="$AMLINTFLAGS -errtags=yes"
	AMLINTFLAGS="$AMLINTFLAGS -Ncheck=%all"
	AMLINTFLAGS="$AMLINTFLAGS -Nlevel=2"
	AMLINTFLAGS="$AMLINTFLAGS -erroff=E_ASGN_NEVER_USED"
	AMLINTFLAGS="$AMLINTFLAGS,E_ASGN_RESET"
	AMLINTFLAGS="$AMLINTFLAGS,E_CAST_INT_CONST_TO_SMALL_INT"
	AMLINTFLAGS="$AMLINTFLAGS,E_CAST_INT_TO_SMALL_INT"
	AMLINTFLAGS="$AMLINTFLAGS,E_CAST_UINT_TO_SIGNED_INT"
	AMLINTFLAGS="$AMLINTFLAGS,E_CONSTANT_CONDITION"
	AMLINTFLAGS="$AMLINTFLAGS,E_ENUM_UNUSE"
	AMLINTFLAGS="$AMLINTFLAGS,E_EXPR_NULL_EFFECT"
	AMLINTFLAGS="$AMLINTFLAGS,E_FUNC_RET_ALWAYS_IGNOR"
	AMLINTFLAGS="$AMLINTFLAGS,E_FUNC_RET_MAYBE_IGNORED"
	AMLINTFLAGS="$AMLINTFLAGS,E_H_C_CHECK0"
	AMLINTFLAGS="$AMLINTFLAGS,E_H_C_CHECK1"
	AMLINTFLAGS="$AMLINTFLAGS,E_H_C_CHECK2"
	AMLINTFLAGS="$AMLINTFLAGS,E_INCL_MNUSD"
	AMLINTFLAGS="$AMLINTFLAGS,E_INCL_NUSD"
	AMLINTFLAGS="$AMLINTFLAGS,E_MCR_NODIFF"
	AMLINTFLAGS="$AMLINTFLAGS,E_NAME_MULTIPLY_DEF"
	AMLINTFLAGS="$AMLINTFLAGS,E_P_REF_NULL_PSBL"
	AMLINTFLAGS="$AMLINTFLAGS,E_P_REF_SUSP"
	AMLINTFLAGS="$AMLINTFLAGS,E_PTRDIFF_OVERFLOW"
	AMLINTFLAGS="$AMLINTFLAGS,E_P_USE_NULL_PSBL"
	AMLINTFLAGS="$AMLINTFLAGS,E_P_USE_SUSP"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_ACCESS_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_CHDIR_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_CHMOD_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_CREAT_WITHOUT_EXCL"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_EXEC_PATH"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_EXEC_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_FOPEN_MODE"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_GETENV_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_MKDIR_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_PRINTF_VAR_FMT"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_RAND_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_SCANF_VAR_FMT"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_SELECT_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_SHELL_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_STRNCPY_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_UMASK_WARN"
	AMLINTFLAGS="$AMLINTFLAGS,E_SEC_USE_AFTER_STAT"
	AMLINTFLAGS="$AMLINTFLAGS,E_SIGN_EXTENSION_PSBL"
	AMLINTFLAGS="$AMLINTFLAGS,E_TYPEDEF_UNUSE"
	AMLINTFLAGS="$AMLINTFLAGS,E_UNCAL_F"
      else
	AMLINTFLAGS=""
      fi
    else
      AC_PATH_PROG(AMLINT,splint,,$SYSLOCPATH)
      if test ! -z "$AMLINT"; then
	AMLINT="splint"
        AMLINTFLAGS='+show-scan +unixlib -weak -globs +usedef +usereleased +impouts -paramimptemp -varuse -warnposix -redef -preproc -fixedformalarray -retval -unrecog -usevarargs -formatcode'
      else
	AMLINT='echo "Error: LINT is not installed" ; false'
        AMLINTFLAGS=''
      fi
    fi
    AC_SUBST(AMLINTFLAGS)
])

# SYNOPSIS
#
#   AMANDA_PROG_LPR
#
# OVERVIEW
#
#   Search for a binary for printing, usually either 'lp' or 'lpr', and put its
#   path in LPR.
#
#   LPRFLAG is substituted as the appropriate command-line flag to use 
#   to select a printer; either -P or -d.
#
AC_DEFUN([AMANDA_PROG_LPR],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])

    AC_PATH_PROGS(LPR, lpr lp)
    if test ! -z "$LPR"; then
	AC_DEFINE([HAVE_LPR_CMD], [1],
	    [Set to 1 if an LPR command was found at configure time])

	AC_CACHE_CHECK([which flag to use to select a printer],
	    amanda_cv_printer_flag, [
	    amanda_cv_printer_flag=$LPRFLAG
	    case "$LPR" in
		lpr|*/lpr) amanda_cv_printer_flag="-P";;
		lp|*/lp) amanda_cv_printer_flag="-d";;
	    esac
	])
	if test -z "$amanda_cv_printer_flag"; then
	    AMANDA_MSG_WARN([WARNING: amanda will always print to the default printer])
	fi
    fi

    AC_SUBST([LPR])
    AC_SUBST([LPRFLAG])
])

# SYNOPSIS
#
#   AMANDA_PROG_GNUPLOT
#
# OVERVIEW
#
#   Search for a 'gnuplot' binary, placing the result in the precious 
#   variable GNUPLOT.  Also accepts --with-gnuplot to indicate the location
#   of the binary.
#
AC_DEFUN([AMANDA_PROG_GNUPLOT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])

    AC_ARG_WITH(gnuplot,
    AS_HELP_STRING([--with-gnuplot=PATH],
		   [use gnuplot executable at PATH in amplot]),
	[
	    case "$withval" in
		y | ye | yes) : ;;
		n | no) GNUPLOT=no ;;
		*) GNUPLOT="$withval" ;;
	    esac
	])
    if test "x$GNUPLOT" = "xno"; then
	GNUPLOT=
    else
	AC_PATH_PROG(GNUPLOT,gnuplot,,$LOCSYSPATH)
    fi

    AC_ARG_VAR(GNUPLOT, [Location of the 'gnuplot' binary])
    AC_SUBST(GNUPLOT)
])

## simple macros needing no description; some add AC_DEFINE_UNQUOTED

AC_DEFUN([AMANDA_PROG_GREP],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(GREP,grep,grep,$LOCSYSPATH)
    AC_DEFINE_UNQUOTED(GREP,"$GREP",
	    [Define the location of the grep program. ])
])

AC_DEFUN([AMANDA_PROG_CAT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(CAT,cat,cat,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_COMPRESS],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(COMPRESS,compress,,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_DD],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(DD,dd,,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_GETCONF],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(GETCONF,getconf,,$SYSPATH)
])

AC_DEFUN([AMANDA_PROG_GZIP],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(GZIP,gzip,,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_SORT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_REQUIRE([AMANDA_CHECK_COMPONENTS])

    AC_PATH_PROG(SORT,sort,NONE,$LOCSYSPATH)

    # sort is only needed in the server build
    if test x"$SORT" = x"NONE" && $WANT_SERVER; then
        AC_MSG_ERROR([Set SORT to the path of the sort program.])
    fi

    AC_DEFINE_UNQUOTED(SORT_PATH,"$SORT",
	    [Define to the exact path to the sort program. ])
])

AC_DEFUN([AMANDA_PROG_MAILER],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROGS(MAILER,Mail mailx mail,NONE)
    if test x"$MAILER" = x"NONE"; then
        AMANDA_MSG_WARN([WARNING: Amanda cannot send mail reports without a mailer.])
	DEFAULT_MAILER=""
    else
	DEFAULT_MAILER="$MAILER"
    fi
    AC_DEFINE_UNQUOTED(DEFAULT_MAILER,"$DEFAULT_MAILER",
                [A program that understands -s "subject" user < message_file])
    AC_SUBST(DEFAULT_MAILER)
])

# SYNOPSIS
#
#   AMANDA_PROG_MT
#
# OVERVIEW
#   
#   Find and SUBST 'mt', and additionally calculate the proper flag to use
#   to identify the tape device (usually -f) and DEFINE and SUBST that value
#   as MT_FILE_FLAG.
#
AC_DEFUN([AMANDA_PROG_MT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(MT,mt,mt,$SYSLOCPATH)

    case "$host" in
	*-hp-*) MT_FILE_FLAG="-t" ;;
	*) MT_FILE_FLAG="-f" ;;
    esac

    AC_SUBST(MT_FILE_FLAG)
    AC_DEFINE_UNQUOTED(MT_FILE_FLAG, "$MT_FILE_FLAG",
  [The switch to be used when invoking mt to specify the
 * tape device. ])
])


AC_DEFUN([AMANDA_PROG_MTX],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(MTX,mtx,mtx,$LOCSYSPATH)
    AC_ARG_VAR([MTX], [Path to the 'mtx' binary])
])

AC_DEFUN([AMANDA_PROG_MOUNT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(MOUNT,mount,mount,$LOCSYSPATH)
    AC_ARG_VAR([MOUNT], [Path to the 'mount' binary])
])

AC_DEFUN([AMANDA_PROG_UMOUNT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(UMOUNT,umount,umount,$LOCSYSPATH)
    AC_ARG_VAR([UMOUNT], [Path to the 'umount' binary])
])

AC_DEFUN([AMANDA_PROG_UNAME],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(UNAME,uname,,$LOCSYSPATH)
    AC_DEFINE_UNQUOTED(UNAME_PATH,"$UNAME",
	    [Define the location of the uname program. ])
])

AC_DEFUN([AMANDA_PROG_PCAT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(PCAT,pcat,,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_PERL],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROGS(PERL,perl5 perl,,$LOCSYSPATH)
    AC_ARG_VAR([PERL], [Path to the 'perl' binary])
    AC_PROG_PERL_VERSION([5.6.0], [], [
	AC_MSG_ERROR([Amanda requires at least perl 5.6.0])
    ])
])

AC_DEFUN([AMANDA_PROG_SWIG],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROGS(SWIG,swig,,$LOCSYSPATH)
    AC_ARG_VAR([SWIG], [Path to the 'swig' binary (developers only)])
    # 1.3.32 introduces a change in the way empty strings are handled (old versions
    # returned undef in Perl, while new versions return an empty Perl string)
    # 1.3.39 is required for the %begin block
    AC_PROG_SWIG([1.3.39])
])

AC_DEFUN([AMANDA_PROG_AR],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(AR,ar,,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_BASH],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(BASH,bash,,$SYSPATH)
])

AC_DEFUN([AMANDA_PROG_SSH],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROGS(SSH, ssh, , $LOCSYSPATH)
    AC_DEFINE_UNQUOTED(SSH, "$SSH", [Path to the SSH binary])
])

AC_DEFUN([AMANDA_PROG_GETTEXT],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(GETTEXT,gettext,,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_RPCGEN],
[
    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(RPCGEN,rpcgen,,$LOCSYSPATH)
])

AC_DEFUN([AMANDA_PROG_LEX],
[
    AC_REQUIRE([AM_PROG_LEX])
    AC_REQUIRE([AMANDA_PROG_GREP])
    if test x"$LEX" != x""; then
	AC_MSG_CHECKING([whether lex is broken Solaris (SGU) lex])
	$LEX -V < /dev/null >/dev/null 2>conftest.out
	if grep SGU conftest.out >/dev/null; then
	    AC_MSG_RESULT([yes - disabled (set LEX=/path/to/lex to use a specific binary)])
	    LEX='echo no lex equivalent available; false'
	else
	    AC_MSG_RESULT([no])
	fi
	rm conftest.out
    fi
])

AC_DEFUN([AMANDA_PROG_NC],
[
    AC_PATH_PROG(NC,nc,,$LOCSYSPATH)
    AC_PATH_PROG(NC6,nc6,,$LOCSYSPATH)
    AC_PATH_PROG(NETCAT,netcat,,$LOCSYSPATH)
])
