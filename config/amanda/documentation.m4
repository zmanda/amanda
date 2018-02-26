# OVERVIEW
#
#   Configuration for the docbook-based manpages.
#
#   We require XSLTPROC, as well as specific versions of Docbook and the
#   Docbook DTD.

AC_DEFUN([AMANDA_SETUP_DOCUMENTATION],
[
    AC_ARG_ENABLE(manpage-build,
    AS_HELP_STRING([--enable-manpage-build],
		   [Build the manpages from their XML source (shipped manpages are usually sufficient)]),
	[ ENABLE_MANPAGE_BUILD=$enableval ],
	[ ENABLE_MANPAGE_BUILD=no ])

    # and ensure that everything docbook-related is OK if we'll be using it
    if test "x$ENABLE_MANPAGE_BUILD" = "xyes"; then
	DOC_BUILD_DATE=`date '+%d-%m-%Y'`

	AC_PROG_XSLTPROC([--nonet])
	AC_CHECK_DOCBOOK_XSLT([1.72.0])
	AC_CHECK_DOCBOOK_XSLT_MIN([1.72.0])
	AC_CHECK_DOCBOOK_DTD([4.1.2])
	AC_CHECK_DOCBOOK_DTD([4.2])

	if test -z "$XSLTPROC"; then
	    AC_MSG_ERROR([Cannot build manpages: 'xsltproc' was not found.])
	fi

	# if the 'current' Docbook revision is good enough, use that; otherwise,
	# if 1.72.0 is available, use that.
	XSLREL=current
	if test "x$DOCBOOK_XSLT_CURRENT_VERSION" = "xno"; then
	    if test "x$HAVE_DOCBOOK_XSLT_1_72_0" = "xno"; then
		AC_MSG_ERROR([Cannot build manpages: docbook version 1.72.0 or higher required.])
	    else
		XSLREL=1.72.0
	    fi
	fi

	# disable validation if the correct DTDs are not available
	if test "x$HAVE_DOCBOOK_DTD_4_1_2" = "xno" || test "x$HAVE_DOCBOOK_DTD_4_2" = "xno"; then
	    AMANDA_MSG_WARN([Docbook DTD versions 4.1.2 and 4.2 are required for manpage validation; disabling validation])
	    XSLTPROC_FLAGS="$XSLTPROC_FLAGS --novalid"
	fi
    fi

    AM_CONDITIONAL(ENABLE_MANPAGE_BUILD, test "x$ENABLE_MANPAGE_BUILD" = "xyes")
    AC_SUBST(XSLREL)
    AC_SUBST(DOC_BUILD_DATE)
])

# SYNOPSIS
#
#   AMANDA_SHOW_DOCUMENTATION_SUMMARY
#
# OVERVIEW
#
#   Show a summary of the settings from this file.
#
AC_DEFUN([AMANDA_SHOW_DOCUMENTATION_SUMMARY],
[
    echo "Build documentation:" $ENABLE_MANPAGE_BUILD
])
