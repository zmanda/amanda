# SYNOPSIS
#
#   AMANDA_CHECK_PRINTF_FORMATS
#
# OVERVIEW
#
#   Determine the printf format characters to use when printing
#   values of type long long. This will normally be "ll", but where
#   the compiler treats "long long" as a alias for "long" and printf
#   doesn't know about "long long" use "l".  Hopefully the sprintf
#   will produce a inconsistant result in the later case.  If the compiler
#   fails due to seeing "%lld" we fall back to "l".
#
#   Win32 uses "%I64d", but that's defined elsewhere since we don't use
#   configure on Win32.
#
#   Defines LL_FMT (e.g., "%lld") and LL_RFMT (e.g., "lld").
#
AC_DEFUN([AMANDA_CHECK_PRINTF_FORMATS],
[
    AC_MSG_CHECKING(printf format modifier for 64-bit integers)
    AC_RUN_IFELSE([AC_LANG_SOURCE([[
#include <stdio.h>
main() {
    long long int j = 0;
    char buf[100];
    buf[0] = 0;
    sprintf(buf, "%lld", j);
    exit((sizeof(long long int) != sizeof(long int))? 0 :
         (strcmp(buf, "0") != 0));
} 
]])
    ],[
	AC_MSG_RESULT(ll)
	LL_FMT="%lld"; LL_RFMT="lld"
    ],[
	AC_MSG_RESULT(l)
	LL_FMT="%ld"; LL_RFMT="ld"
    ],[
	AC_MSG_RESULT(assuming target platform uses ll)
        LL_FMT="%lld"; LL_RFMT="lld"
    ])

    AC_DEFINE_UNQUOTED(LL_FMT,"$LL_FMT",
      [Format for a long long printf. ])
    AC_DEFINE_UNQUOTED(LL_RFMT,"$LL_RFMT",
      [Format for a long long printf. ])
])
