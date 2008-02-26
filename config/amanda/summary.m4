# OVERVIEW/BACKGROUND
#
#   This file creates an end-of-run summary of the Amanda configuration.
#

# SYNOPSIS
#
#   AMANDA_INIT_SUMMARY()
#
# DESCRIPTION
#
#   Set up for producing a summary.  This should be called early in the configure
#   process
#
AC_DEFUN([AMANDA_INIT_SUMMARY],
[
    # initialize warnings file
    rm -f config.warnings
])

# SYNOPSIS
#
#   AMANDA_MSG_WARN()
#
# DESCRIPTION
#
#   Like AC_MSG_WARN, but also adds the message to the summary
#
AC_DEFUN([AMANDA_MSG_WARN], [
    AC_MSG_WARN([$1])
    AMANDA_ADD_WARNING([$1])
])

# SYNOPSIS
#
#   AMANDA_ADD_WARNING(warning-text)
#
# DESCRIPTION
#
#   Add the given text to the warnings summary
#
AC_DEFUN([AMANDA_ADD_WARNING], [
    cat <<AAW_EOF >>config.warnings
$1
AAW_EOF])

# SYNOPSIS
#
#   AMANDA_SHOW_SUMMARY()
#
# DESCRIPTION
#
#   Output the configuration summary.
#
AC_DEFUN([AMANDA_SHOW_SUMMARY], [
    AMANDA_SHOW_FLAGS_SUMMARY
    AMANDA_SHOW_COMPONENTS_SUMMARY
    AMANDA_SHOW_IPV6_SUMMARY
    AMANDA_SHOW_DOCUMENTATION_SUMMARY
    AMANDA_SHOW_DIRS_SUMMARY
    if test -f config.warnings; then
	echo "WARNINGS:"
	cat config.warnings | sed -e 's/^/  /g'
	rm config.warnings
    fi
])
