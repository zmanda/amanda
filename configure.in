AMANDA_INIT_VERSION
AC_INIT([amanda], [AMANDA_F_VERSION])
AC_CONFIG_SRCDIR([common-src/amanda.h])
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_MACRO_DIR(config)
AC_CANONICAL_HOST

PACKAGE=amanda
AC_SUBST(PACKAGE)

AMANDA_VERSION

gl_EARLY
gl_INIT

AMANDA_CONFIGURE

AC_DEFINE([USE_GETTEXT], 1, [Define to 1 if files will be processed with gettextize])

#
# Internationalization
#
AMANDA_SETUP_I18N
dnl These lines *must* be in configure.in, and not indented
AM_GNU_GETTEXT_VERSION([0.15])
AM_GNU_GETTEXT([external])
AMANDA_PROG_GETTEXT


dnl
dnl Set up files for autoconf substitution.  All files are listed here, in
dnl order by subdirectory, as a central location for this list.
dnl
AC_CONFIG_FILES([
    Makefile
    amandad-src/Makefile
    amar-src/Makefile
    amplot/Makefile
    application-src/Makefile
    client-src/Makefile
    common-src/Makefile
    config/Makefile
    device-src/Makefile
    example/Makefile
    gnulib/Makefile
    installcheck/Makefile
    man/Makefile
    ndmp-src/Makefile
    oldrecover-src/Makefile
    packaging/Makefile
    perl/amglue/Makefile
    perl/Makefile
    po/Makefile
    recover-src/Makefile
    rest-server/Makefile
    server-src/Makefile
    xfer-src/Makefile
])

AC_OUTPUT
AMANDA_SHOW_SUMMARY
