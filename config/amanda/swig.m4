# OVERVIEW
#
#   Set up for building SWIG bindings.  Note that shipped tarballs contain pre-built
#   SWIG bindings, so there should be no need for SWIG on non-developer machines.

# SYNOPSIS
#
#   Substitute PERL_EXT_CFLAGS and PERL_EXT_LDFLAGS, the flags needed to produce
#   Perl extension modules.
#
AC_DEFUN([AMANDA_SETUP_SWIG],
[
    AC_REQUIRE([AMANDA_PROG_PERL])
    
    PERL_EXT_CFLAGS=`$PERL -MExtUtils::Embed -e ccopts`
    PERL_EXT_LDFLAGS=`$PERL -MExtUtils::Embed -e ldopts`
    AC_SUBST(PERL_EXT_CFLAGS)
    AC_SUBST(PERL_EXT_LDFLAGS)
])
