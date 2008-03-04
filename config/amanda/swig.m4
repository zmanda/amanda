# OVERVIEW
#
#   Set up for building SWIG bindings.  Note that shipped tarballs contain pre-built
#   SWIG bindings, so there should be no need for SWIG on non-developer machines.
# SYNOPSIS
#
#   Find perl and SWIG, and substitute PERL_INC, the -I command that will lead the compiler
#   to perl.h and friends.
#
#   Supports --with-perlextlibs, for adding extra LIBS declarations to perl extensions.
#
AC_DEFUN([AMANDA_SETUP_SWIG],
[
    AC_REQUIRE([AMANDA_PROG_SWIG])
    AC_REQUIRE([AMANDA_PROG_PERL])

    # get the include path for building perl extensions
    PERL_INC=`$PERL -MExtUtils::Embed -e perl_inc`
    AC_SUBST(PERL_INC)

    if test x"$enable_shared" = x"no"; then
	AC_MSG_ERROR([*** Amanda cannot be compiled without shared-library support (do not use --disable-shared)])
    fi

    case "$target" in
	i386-unknown-freebsd6.2) # FreeBSD 6.2 only
	    # FreeBSD 6.2 systems don't include a DT_NEEDS segment in libgthread to automatically
	    # pull in the desired threading library.  Instead, they assume that any application
	    # linking against libgthread will pull in the threading library.  This is fine for
	    # Amanda C applications, but for Perl applications this assumption means that the
	    # perl binary would pull in the threading library.  But perl is compiled without
	    # threading by default.  The easiest solution for the "default" case is to link all
	    # perl extension libraries against the threading library, so it is loaded when perl
	    # loads the extension library.  The default threading library is libpthread.  This
	    # can be overridden with --with-perlextlibs.  We can't use -pthread because gcc on
	    # FreeBSD ignores -pthread in combination with -shared.  See
	    #   http://lists.freebsd.org/pipermail/freebsd-stable/2006-June/026229.html
	    PERLEXTLIBS="-lpthread"
	    ;;
    esac

    AC_ARG_WITH(perlextlibs,
	AC_HELP_STRING([--with-perlextlibs=libs],[extra LIBS for Perl extensions]),
	[
	    case "$withval" in
		y|ye|yes) AC_MSG_ERROR([*** You must specify a value for --with-perlextlibs]);;
		n|no) PERLEXTLIBS='';;
		*) PERLEXTLIBS="$withval" ;;
	    esac
	])
    AC_SUBST(PERLEXTLIBS)
])
