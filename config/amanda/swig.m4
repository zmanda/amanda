# OVERVIEW
#
#   Set up for building SWIG bindings.  Note that shipped tarballs contain pre-built
#   SWIG bindings, so there should be no need for SWIG on non-developer machines.
# SYNOPSIS
#
#   Find perl and SWIG, and substitute PERL_INC, the -I command that will lead the compiler
#   to perl.h and friends.
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
])
