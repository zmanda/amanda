# SYNOPSIS
#
#   AMANDA_SETUP_I18N
#
# OVERVIEW
#
#   Set up Amanda's internationalization support.  Note that configure.in
#   itself must contain (not indented):
#
#   AM_GNU_GETTEXT_VERSION([0.15])
#   AM_GNU_GETTEXT([external])
#
AC_DEFUN([AMANDA_SETUP_I18N], [
    # FreeBSD needs to link libxpg4
    AC_CHECK_LIB(xpg4, setlocale)

    # ------------------------------------------------------------------
    # All list of languages for which a translation exist. Each
    #  language is separated by a space.
    # ------------------------------------------------------------------
    ALL_LINGUAS=""

    AC_REQUIRE([AMANDA_INIT_PROGS])
    AC_PATH_PROG(MSGFMT, msgfmt,,$LOCSYSPATH)
    AC_PATH_PROG(GETTEXT,gettext,,$LOCSYSPATH)
])
