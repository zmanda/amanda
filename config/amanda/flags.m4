# OVERVIEW/BACKGROUND
#
#   This file manages flags in CFLAGS, CPPFLAGS, LDFLAGS, and LIBS.
#
#   Flags can come from two several sources:
#    - entry to ./configure or on the configure command line (they are
#      `precious' variables)
#    - added by autoconf tests during the execution of configure
#
#   Although Automake supports overriding variables when invoking 'make',
#   we don't support it (mostly because autoconf doesn't).  Instead, users
#   should specify such variables when invoking ./configure.
#
#   CFLAGS are a little bit more complicated: Amanda has two categories,
#   mandatory CFLAGS, which should be used everywhere, and warning CFLAGS,
#   which are only used on Amanda code (not gnulib or yacc-generated code).
#   To accomplish this, mandatory CFLAGS go directl into CFLAGS, while
#   warwnings go in AMANDA_WARNING_CFLAGS; these are then added to 
#   AM_CFLAGS by the Makefiles.

# SYNOPSIS
#
#   AMANDA_INIT_FLAGS()
#
# DESCRIPTION
#
#   Process variables given by the user on the command line,
#   either as environment variables:
#	CPPFLAGS=-Dfoo ./configure ...
#   as assignments in the configure command line:
#	./configure LIBS=-lfoo ...
#   or with the deprecated flags --with-cflags, --with-includes, and
#   --with-libraries
#
AC_DEFUN([AMANDA_INIT_FLAGS],
[
    # support deprecated ./configure flags to set various compiler flags

    AC_ARG_WITH(cflags,
	AS_HELP_STRING([--with-cflags=FLAGS],
		       [deprecated; use ./configure CFLAGS=... ]),
	[
	    case "$withval" in
	    "" | y | ye | yes | n | no)
		AC_MSG_ERROR([*** You must supply an argument to the --with-cflags option.])
		;;
	    esac

	    CFLAGS="$withval"
	])

    AC_ARG_WITH(includes,
	AS_HELP_STRING([--with-includes=INCLUDE-DIRS],
		       [deprecated; use ./configure CPPFLAGS='-I.. -I..']),
	[
	    case "$withval" in
	    "" | y | ye | yes | n | no)
		AC_MSG_ERROR([*** You must supply an argument to the --with-includes option.])
	      ;;
	    esac

	    for dir in $withval; do
		if test -d "$dir"; then
		    CPPFLAGS="$CPPFLAGS -I$dir"
		else
		    AMANDA_MSG_WARN([Include directory $dir does not exist.])
		fi
	    done
	])

    AC_ARG_WITH(libraries,
	AS_HELP_STRING([--with-libraries=LIBRARY-DIRS],
		       [deprecated; use ./configure LDFLAGS='-L.. -L..' (add -R on Solaris, NetBSD)]),
	[
	    case "$withval" in
	    "" | y | ye | yes | n | no)
		AC_MSG_ERROR([*** You must supply an argument to the --with-libraries option.])
	      ;;
	    esac

	    for dir in $withval; do
		if test -d "$dir"; then
		    case "$host" in
		      *-solaris2*,*-netbsd*)
			    LDFLAGS="$LDFLAGS -R$dir"
			    ;;
		    esac
		    LDFLAGS="$LDFLAGS -L$dir"
		else
		    AMANDA_MSG_WARN([Library directory $dir does not exist.])
		fi
	    done
	])

    # Disable strict-aliasing optimizations
    AMANDA_DISABLE_GCC_FEATURE(strict-aliasing)

    # Warn for just about everything
    AMANDA_ENABLE_GCC_WARNING(all)
    
    # And add any extra warnings too
    AMANDA_TEST_GCC_FLAG(-Wextra, [
	AMANDA_ADD_WARNING_CFLAG(-Wextra)
    ], [
	AMANDA_TEST_GCC_FLAG(-W, [
	    AMANDA_ADD_WARNING_CFLAG(-W)
	])
    ])
    AC_SUBST([AMANDA_WARNING_CFLAGS])
])

# SYNOPSIS
#
#   AMANDA_STATIC_FLAGS(new_flags)
#
# DESCRIPTION
#
#   Set AMANDA_STATIC_LDFLAGS to -static if --enable-static-binary
#
AC_DEFUN([AMANDA_STATIC_FLAGS],
[
    AC_ARG_ENABLE(static-binary,
	AS_HELP_STRING([--enable-static-binary],
		       [To build statically linked binaries]),
	[
	    case "$enableval" in
	    "" | y | ye | yes)
		AMANDA_STATIC_LDFLAGS=-static
		if test x"$enable_static" = x"no"; then
			AC_MSG_ERROR([*** --enable-static-binary is incompatible with --disable-static])
		fi
		;;
	    *n | no)
		AMANDA_STATIC_LDFLAGS=
		;;
	    esac
	])
    AC_SUBST([AMANDA_STATIC_LDFLAGS])
])

# SYNOPSIS
#
#   AMANDA_WERROR_FLAGS
#
# DESCRIPTION
#
#   Set AMANDA_WERROR_FLAGS
#
AC_DEFUN([AMANDA_WERROR_FLAGS],
[
    enable_werror=no
    AC_ARG_ENABLE(werror,
	AS_HELP_STRING([--enable-werror],
		       [To compile with -Werror compiler flag]),
	[
	    case "$enableval" in
	    "" | y | ye | yes)
		enable_werror=yes
		;;
	    *n | no)
		;;
	    esac
	])
    if test x"$enable_werror" eq x"yes"; then
	AMANDA_ENABLE_GCC_WARNING(error)
    fi
])

# SYNOPSIS
#
#   AMANDA_SWIG_ERROR
#
# DESCRIPTION
#
#   Set AMANDA_SWIG_ERROR
#
AC_DEFUN([AMANDA_SWIG_ERROR],
[
    AC_ARG_ENABLE(swig-error,
	AS_HELP_STRING([--enable-swig-error],
		       [To compile swiged C file with -Werror compiler flag]),
	[
	    case "$enableval" in
	    "" | y | ye | yes)
		AMANDA_SWIG_PERL_CFLAGS=-Werror
		;;
	    *n | no)
		AMANDA_SWIG_PERL_CFLAGS=
		;;
	    esac
	])
    AC_SUBST([AMANDA_SWIG_PERL_CFLAGS])
])

# SYNOPSIS
#
#   AMANDA_ADD_CFLAGS(new_flags)
#
# DESCRIPTION
#
#   Add 'new_flags' to CFLAGS.
#
#   'new_flags' will be enclosed in double quotes in the resulting
#   shell assignment.
#
AC_DEFUN([AMANDA_ADD_CFLAGS],
    [CFLAGS="$CFLAGS $1"]
)

# SYNOPSIS
#
#   AMANDA_ADD_CPPFLAGS(new_flags)
#
# DESCRIPTION
#
#   Add 'new_flags' to CPPFLAGS.
#
#   'new_flags' will be enclosed in double quotes in the resulting
#   shell assignment.
#
AC_DEFUN([AMANDA_ADD_CPPFLAGS],
    [CPPFLAGS="$CPPFLAGS $1"]
)

# SYNOPSIS
#
#   AMANDA_ADD_LDFLAGS(new_flags)
#
# DESCRIPTION
#
#   Add 'new_flags' to LDFLAGS.
#
#   'new_flags' will be enclosed in double quotes in the resulting
#   shell assignment.
#
AC_DEFUN([AMANDA_ADD_LDFLAGS],
    [LDFLAGS="$LDFLAGS $1"]
)

# SYNOPSIS
#
#   AMANDA_ADD_LIBS(new_flags)
#
# DESCRIPTION
#
#   Add 'new_flags' to LIBS.
#
#   'new_flags' will be enclosed in double quotes in the resulting
#   shell assignment.
#
AC_DEFUN([AMANDA_ADD_LIBS],
    [LIBS="$1 $LIBS"]
)

# SYNOPSIS
#
#   AMANDA_DISABLE_GCC_FEATURE(feature)
#
# OVERVIEW
#
#   Disable feature 'feature' by adding flag -Wno-'feature' to 
#   AMANDA_FEATURE_CFLAGS.
#
AC_DEFUN([AMANDA_DISABLE_GCC_FEATURE],
[
    # test for -W'feature', then add the 'no-' version.
    AMANDA_TEST_GCC_FLAG(-f$1,
    [
	AMANDA_ADD_CFLAGS(-fno-$1)
	AMANDA_ADD_CPPFLAGS(-fno-$1)
    ])
])

# SYNOPSIS
#
#   AMANDA_ADD_WARNING_CFLAG(flag)
#
# DESCRIPTION
#
#   Add 'flag' to AMANDA_WARNING_CFLAGS
#
AC_DEFUN([AMANDA_ADD_WARNING_CFLAG],
    [AMANDA_WARNING_CFLAGS="$AMANDA_WARNING_CFLAGS $1"]
)

# SYNOPSIS
#
#   AMANDA_ENABLE_GCC_WARNING(warning)
#
# OVERVIEW
#
#   Enable warning 'warning' by adding flag -W'warning' to 
#   AMANDA_WARNING_CFLAGS.
#
AC_DEFUN([AMANDA_ENABLE_GCC_WARNING],
[
    AMANDA_TEST_GCC_FLAG(-W$1,
    [
	AMANDA_ADD_WARNING_CFLAG(-W$1)
    ])
])

# SYNOPSIS
#
#   AMANDA_DISABLE_GCC_WARNING(warning)
#
# OVERVIEW
#
#   Disable warning 'warning' by adding flag -Wno-'warning' to 
#   AMANDA_WARNING_CFLAGS.
#
AC_DEFUN([AMANDA_DISABLE_GCC_WARNING],
[
    # test for -W'warning', then add the 'no-' version.
    AMANDA_TEST_GCC_FLAG(-W$1,
    [
	AMANDA_ADD_WARNING_CFLAG(-Wno-$1)
    ])
])

# SYNOPSIS
#
#   AMANDA_TEST_GCC_FLAG(flag, action-if-found, action-if-not-found)
#
# OVERVIEW
#
#   See if CC is gcc, and if gcc -v --help contains the given flag.  If so,
#   run action-if-found; otherwise, run action-if-not-found.
#
#   Intended for internal use in this file.
#
AC_DEFUN([AMANDA_TEST_GCC_FLAG],
[
    AC_REQUIRE([AC_PROG_CC])
    AC_REQUIRE([AC_PROG_EGREP])
    AC_MSG_CHECKING(for gcc flag $1)
    if test "x$GCC" = "xyes"; then
	changequote(,)dnl
	(gcc --help={target,optimizers,warnings,undocumented,params,c} 2>&1 || 
           $CC -v --help 2>&1) | 
         $EGREP -- '[^[:alnum:]]$1[^[:alnum:]-]' 2>&1 > /dev/null
	changequote([,])dnl
	if test $? -eq 0; then
	    found_warning=yes
	    AC_MSG_RESULT(yes)
	else
	    found_warning=no
	    AC_MSG_RESULT(no)
	fi
    else
	found_warning=no
	AC_MSG_RESULT(no (not using gcc))
    fi

    if test x"$found_warning" = x"yes"; then
	ifelse($2, [], [:], $2)
    else
	ifelse($3, [], [:], $3)
    fi
])

# SYNOPSIS
#
#   AMANDA_SHOW_FLAGS_SUMMARY
#
# OVERVIEW
#
#   Show a summary of the flags with which Amanda was configured
#
AC_DEFUN([AMANDA_SHOW_FLAGS_SUMMARY],
[
    echo "Compiler Flags:"
    echo "  CFLAGS: ${CFLAGS-(none)}"
    echo "  CPPFLAGS: ${CPPFLAGS-(none)}"
    echo "  LDFLAGS: ${LDFLAGS-(none)}"
    echo "  LIBS: ${LIBS-(none)}"
])
