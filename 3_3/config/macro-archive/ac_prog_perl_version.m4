##### http://autoconf-archive.cryp.to/ac_prog_perl_version.html
#
# SYNOPSIS
#
#   AC_PROG_PERL_VERSION(VERSION, [ACTION-IF-TRUE], [ACTION-IF-FALSE])
#
# DESCRIPTION
#
#   Makes sure that perl supports the version indicated. If true the
#   shell commands in ACTION-IF-TRUE are executed. If not the shell
#   commands in ACTION-IF-FALSE are run. Note if $PERL is not set (for
#   example by running AC_CHECK_PROG or AC_PATH_PROG),
#   AC_CHECK_PROG(PERL, perl, perl) will be run.
#
#   Example:
#
#     AC_PROG_PERL_VERSION(5.6.0)
#
#   This will check to make sure that the perl you have supports at
#   least version 5.6.0.
#
# LAST MODIFICATION
#
#   2002-09-25
#
# COPYLEFT
#
#   Copyright (c) 2002 Dean Povey <povey@wedgetail.com>
#
#   Copying and distribution of this file, with or without
#   modification, are permitted in any medium without royalty provided
#   the copyright notice and this notice are preserved.

AC_DEFUN([AC_PROG_PERL_VERSION],[dnl
# Make sure we have perl
if test -z "$PERL"; then
AC_CHECK_PROG(PERL,perl,perl)
fi

# Check if version of Perl is sufficient
ac_perl_version="$1"

if test "x$PERL" != "x"; then
  AC_MSG_CHECKING(for perl version greater than or equal to $ac_perl_version)
  # NB: It would be nice to log the error if there is one, but we cannot rely
  # on autoconf internals
  $PERL -e "use $ac_perl_version;" > /dev/null 2>&1
  if test $? -ne 0; then
    AC_MSG_RESULT(no);
    $3
  else
    AC_MSG_RESULT(ok);
    $2
  fi
else
  AC_MSG_WARN(could not find perl)
fi
])dnl

