
# SYNOPSIS
#
#   AMANDA_SYSTEMD
#
# OVERVIEW
#
#   This macro encapsulates any systemd-specific macros
#
AC_DEFUN([AMANDA_SYSTEMD], [
   read x SYSTEMD_VERSION x <<<"$(systemctl --version | head -1)"
   AM_CONDITIONAL([SYSTEMD], [test "$SYSTEMD_VERSION" -gt 100])
   AM_CONDITIONAL([SYSTEMD_AT_220], [test "$SYSTEMD_VERSION" -gt 220])
])
