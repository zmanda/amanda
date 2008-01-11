# SYNOPSIS
#
#   AMANDA_WITH_DUMPERDIR
#
# OVERVIEW
#
#   Allow user to specify the dumperdir, defaulting to ${exec_prefix}/dumper.
#   Define and substitute DUMPER_DIR with the result.
#
AC_DEFUN([AMANDA_WITH_DUMPERDIR],
[
    AC_ARG_WITH(dumperdir,
	AS_HELP_STRING([--with-dumperdir=DIR],
	    [where we install the dumpers (default: exec_prefix/dumper)]),
	[
	    case "$withval" in
	    "" | y | ye | yes | n | no)
		AC_MSG_ERROR([*** You must supply an argument to the --with-dumperdir option.])
	      ;;
	    esac
	    DUMPER_DIR="$withval"
	], [
	    DUMPER_DIR='${exec_prefix}/dumper' # (variable will be evaluated below)
	]
    )
    AC_DEFINE_DIR([DUMPER_DIR],[DUMPER_DIR],
	    [Directory in which dumper interfaces should be installed and searched. ])
])

# SYNOPSIS
#
#   AMANDA_WITH_CONFIGDIR
#
# OVERVIEW
#
#   Allow user to specify the dumperdir, defaulting to ${exec_prefix}/dumper.
#   Define and substitute DUMPER_DIR with the result.
#
AC_DEFUN([AMANDA_WITH_CONFIGDIR],
[
    AC_ARG_WITH(configdir,
	AS_HELP_STRING([--with-configdir=DIR],
	    [runtime config files in DIR @<:@sysconfdir/amanda@:>@]),
	[
	    case "$withval" in
	    "" | y | ye | yes | n | no)
		AC_MSG_ERROR([*** You must supply an argument to the --with-configdir option.])
	      ;;
	    *) CONFIG_DIR="$withval"
	      ;;
	    esac
	], [
	    : ${CONFIG_DIR='${sysconfdir}/amanda'} # (variable will be evaluated below)
	]
    )
    AC_DEFINE_DIR([CONFIG_DIR], [CONFIG_DIR],
      [The directory in which configuration directories should be created. ])
])

## deprecated --with-*dir options

AC_DEFUN([AMANDA_WITH_INDEXDIR],
[
    AC_ARG_WITH(indexdir,
	AS_HELP_STRING([--with-indexdir], [deprecated: use indexdir in amanda.conf]),
	[   AC_MSG_ERROR([*** --with-indexdir is deprecated; use indexdir in amanda.conf instead.])
	],)
])

AC_DEFUN([AMANDA_WITH_DBDIR],
[
    AC_ARG_WITH(dbdir,
	AS_HELP_STRING([--with-dbdir], [deprecated: use infofile in amanda.conf]),
	[   AC_MSG_ERROR([*** --with-dbdir is deprecated; use infofile in amanda.conf instead.])
	],)
])

AC_DEFUN([AMANDA_WITH_LOGDIR],
[
    AC_ARG_WITH(logdir,
	AS_HELP_STRING([--with-logdir], [deprecated: use logfile in amanda.conf]),
	[   AC_MSG_ERROR([*** --with-logdir is deprecated; use logfile in amanda.conf instead.])
	],)
])

# SYNOPSIS
#
#   AMANDA_WITH_GNUTAR_LISTDIR
#
# OVERVIEW
#
#   Implements --with-gnutar-listdir.  Defines GNUTAR_LISTED_INCREMENTAL_DIR to the
#   value given or $localstatedir/amanda/gnutar-lists by default.  Any $xxxdir variables
#   are fully evaluated in the value.
#
AC_DEFUN([AMANDA_WITH_GNUTAR_LISTDIR], 
[
    AC_ARG_WITH(gnutar-listdir,
       AS_HELP_STRING([--with-gnutar-listdir=DIR],
        [put gnutar directory lists in DIR (default: localstatedir/amanda/gnutar-lists)]),
       [
            case "$withval" in
                n | no) GNUTAR_LISTDIR= ;;
                y | ye | yes) GNUTAR_LISTDIR='${localstatedir}/amanda/gnutar-lists' ;;
                *) GNUTAR_LISTDIR="$withval" ;;
            esac
        ], [
            GNUTAR_LISTDIR='${localstatedir}/amanda/gnutar-lists'
        ]
    )

    # substitute $prefix, etc. if necessary
    AC_DEFINE_DIR([GNUTAR_LISTED_INCREMENTAL_DIR], [GNUTAR_LISTDIR],
	[The directory in which GNU tar should store directory lists for incrementals. ])
    
    # handle deprecated option
    AC_ARG_WITH(gnutar-listed-incremental,
        AS_HELP_STRING([--with-gnutar-listed-incremental], 
            [deprecated; use --with-gnutar-listdir]),
        [
            AC_MSG_ERROR([*** The gnutar-listed-incremental option is deprecated; use --with-gnutar-listdir instead])
        ]
    )
])

# SYNOPSIS
#
#   AMANDA_WITH_TMPDIR
#
# OVERVIEW
#
#   Implement --with-tmpdir.  Defines and substitutes AMANDA_TMPDIR.
#
AC_DEFUN([AMANDA_WITH_TMPDIR],
[
    AC_ARG_WITH(tmpdir,
        AS_HELP_STRING([--with-tmpdir],
            [directory for temporary and debugging files (default: /tmp/amanda)]),
        [
            tmpdir="$withval"
        ], [
            tmpdir=yes
        ]
    )

    case "$tmpdir" in
        n | no) AC_MSG_ERROR([*** --without-tmpdir is not allowed.]);;
        y |  ye | yes) AMANDA_TMPDIR="/tmp/amanda";;
        *) AMANDA_TMPDIR="$tmpdir";;
    esac

    AC_DEFINE_DIR([AMANDA_TMPDIR], [AMANDA_TMPDIR],
        [The directory in which Amanda should create temporary files. ])
])

# SYNOPSIS
#
#   AMANDA_EXPAND_DIRS
#
# OVERVIEW
#
#   Expand any variable references in the following variables, then define them:
#   - bindir
#   - sbindir
#   - libexecdir
#   - mandir
#
#   Also defines the following directories and expands any variable references:
#   - amlibdir = ${libdir}/amanda
#   - amincludedir = ${includedir}/amanda
#   - amperldir = ${amlibdir}/perl
#
AC_DEFUN([AMANDA_EXPAND_DIRS],
[
    AC_DEFINE_DIR([bindir], [bindir],
        [Directory in which user binaries should be installed. ])

    AC_DEFINE_DIR([sbindir], [sbindir],
        [Directory in which administrator binaries should be installed. ])

    AC_DEFINE_DIR([libexecdir], [libexecdir], 
        [Directory in which internal binaries should be installed. ])

    AC_DEFINE_DIR([mandir], [mandir],
        [Directory in which man-pages should be installed])

    # amanda-specific directories
    amlibdir="${libdir}/amanda"
    AC_DEFINE_DIR([amlibdir], [amlibdir],
	[Directory in which Amanda libraries should be installed])

    amlibexecdir="${libexecdir}/amanda"
    AC_DEFINE_DIR([amlibexecdir], [amlibexecdir],
	[Directory in which Amanda own programs should be installed])

    amincludedir="${includedir}/amanda"
    AC_DEFINE_DIR([amincludedir], [amincludedir],
	[Directory in which Amanda header files should be installed])

    amperldir="${amlibdir}/perl"
    AC_DEFINE_DIR([amperldir], [amperldir],
	[Directory in which Amanda perl libraries should be installed])
])

# SYNOPSIS
#
#   AMANDA_SHOW_DIRS_SUMMARY
#
# OVERVIEW
#
#   Show a summary of the settings from this file.
#
AC_DEFUN([AMANDA_SHOW_DIRS_SUMMARY],
[
    echo "Directories:"
    echo "  Dumper: $DUMPER_DIR"
    echo "  Configuration: $CONFIG_DIR"
    echo "  GNU Tar lists: $GNUTAR_LISTED_INCREMENTAL_DIR"
    echo "  Temporary: $AMANDA_TMPDIR"
])
