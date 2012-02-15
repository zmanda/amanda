# OVERVIEW
#
#   'amplot' is largely pieced together by the instantiation phase of 
#   configure; that is handled here.

# SYNOPSIS
#
#   AMANDA_SETUP_AMPLOT
#
# DESCRIPTION
#
#   Check for the requirements for amplot, and set the Automake conditional
#   WANT_AMPLOT appropriately.  If amplot is to be built, then also set up
#   the required substitutions to build it correctly.
#
AC_DEFUN([AMANDA_SETUP_AMPLOT],
[
    AC_REQUIRE([AMANDA_PROG_GNUPLOT])
    AC_REQUIRE([AMANDA_PROG_PCAT])
    AC_REQUIRE([AMANDA_PROG_COMPRESS])
    AC_REQUIRE([AMANDA_PROG_GZIP])
    AC_REQUIRE([AC_PROG_AWK])

    if test "x$GNUPLOT" != "x"; then
	WANT_AMPLOT=true

	# variable substitutions for amcat.awk
	if test "$PCAT"; then
	    AMPLOT_CAT_PACK="if(o==\"z\")print \"$PCAT\"; else"
	else
	    AMPLOT_CAT_PACK=
	fi
	if test "$COMPRESS"; then
	    AMPLOT_COMPRESS=$COMPRESS
	    AMPLOT_CAT_COMPRESS="if(o==\"Z\")print \"$COMPRESS -dc\"; else"
	else
	    AMPLOT_CAT_COMPRESS=
	fi
	if test "$GZIP"; then
	    AMPLOT_COMPRESS=$GZIP
	    AMPLOT_CAT_GZIP="if(o==\"gz\")print \"$GZIP -dc\"; else"
	else
	    AMPLOT_CAT_GZIP=
	fi

	AC_SUBST(AMPLOT_COMPRESS)
	AC_SUBST(AMPLOT_CAT_GZIP)
	AC_SUBST(AMPLOT_CAT_COMPRESS)
	AC_SUBST(AMPLOT_CAT_PACK)
    else
	WANT_AMPLOT=false
	AMANDA_MSG_WARN([Not building 'amplot', because gnuplot was not found])
    fi

    AM_CONDITIONAL(WANT_AMPLOT, test x"$WANT_AMPLOT" = x"true")
])
