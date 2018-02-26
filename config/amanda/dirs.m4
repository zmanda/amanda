# SYNOPSIS
#
#   AMANDA_WITH_APPLICATIONDIR
#
# OVERVIEW
#
#   Define and substitute APPLICATION_DIR with the result.
#
AC_DEFUN([AMANDA_WITH_APPLICATIONDIR],
[
    AC_ARG_WITH(dumperdir,
	AS_HELP_STRING([--with-dumperdir=DIR],
	    [where we install the dumpers (deprecated)]),
	[
            AMANDA_MSG_WARN([--with-dumperdir is no longer used.])
	]
    )
])

# SYNOPSIS
#
#   AMANDA_WITH_CONFIGDIR
#
# OVERVIEW
#
#   Allow user to specify the dumperdir, defaulting to ${exec_prefix}/dumper.
#   Define and substitute APPLICATION_DIR with the result.
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
#   - amdatadir = --with-amdatadir or ${datadir}/amanda
#   - amlibdir = --with-amlibdir or ${libdir}/amanda
#   - amlibexecdir = --with-amlibexecdir or ${libexecdir}/amanda
#   - amincludedir = ${includedir}/amanda
#   - amperldir = --with-amperldir or `perl -V:installsitearch`
#   - APPLICATION_DIR = ${amlibexecdir}/application
#
AC_DEFUN([AMANDA_EXPAND_DIRS],
[
    AC_REQUIRE([AMANDA_PROG_PERL])

    AC_DEFINE_DIR([bindir], [bindir],
        [Directory in which user binaries should be installed. ])

    AC_DEFINE_DIR([sbindir], [sbindir],
        [Directory in which administrator binaries should be installed. ])

    AC_DEFINE_DIR([libexecdir], [libexecdir], 
        [Directory in which internal binaries should be installed. ])

    AC_DEFINE_DIR([mandir], [mandir],
        [Directory in which man-pages should be installed])


    # amanda-specific directories
    AMLIBDIR=$libdir/amanda
    AC_ARG_WITH(amlibdir,
	AS_HELP_STRING([--with-amlibdir[[[[[=PATH]]]]]],
		[Where library are installed, default: $libdir/amanda])
	AS_HELP_STRING([--without-amlibdir],
		[Library are installed in $libdir]),
	[
	    case "$withval" in
		n | no) AMLIBDIR=$libdir ;;
		y | ye | yes) AMLIBDIR=$libdir/amanda ;;
		*) AMLIBDIR=$withval ;;
	    esac
	]
    )
    AC_DEFINE_DIR([amlibdir], [AMLIBDIR],
	[Directory in which Amanda libraries should be installed])

    AMLIBEXECDIR=$libexecdir/amanda
    AC_ARG_WITH(amlibexecdir,
	AS_HELP_STRING([--with-amlibexecdir[[[[[=PATH]]]]]],
		[Where amanda own programs are installed, default: $libexecdir/amanda])
	AS_HELP_STRING([--without-amlibexecdir],
		[Amanda own programs are installed in $libexecdir]),
	[
	    case "$withval" in
		n | no) AMLIBEXECDIR=$libexecdir ;;
		y | ye | yes) AMLIBEXECDIR=$libexecdir/amanda ;;
		*) AMLIBEXECDIR=$withval ;;
	    esac
	]
    )
    AC_DEFINE_DIR([amlibexecdir], [AMLIBEXECDIR],
	[Directory in which Amanda own programs should be installed])

    amincludedir="${includedir}/amanda"
    AC_DEFINE_DIR([amincludedir], [amincludedir],
	[Directory in which Amanda header files should be installed])

    AC_ARG_WITH(amperldir,
	AS_HELP_STRING([--with-amperldir[[[[[=PATH]]]]]],
		[Where amanda's perl modules are installed; default: installsitelib])
	AS_HELP_STRING([--without-amperldir],
		[Install amanda's perl modules in $amlibdir/perl]),
	[
	    case "$withval" in
		y | ye | yes) AMPERLLIB=DEFAULT ;;
		n | no) AMPERLLIB=$amlibdir/perl ;;
		*) AMPERLLIB=$withval ;;
	    esac
	], [
	    AMPERLLIB=DEFAULT
	]
    )
    # apply the default if no value was given.
    if test x"$AMPERLLIB" = x"DEFAULT"; then
	eval `$PERL -V:installsitelib`
	AMPERLLIB=$installsitelib
    fi
    AC_DEFINE_DIR([amperldir], [AMPERLLIB],
	[Directory in which perl modules should be installed])

    APPLICATION_DIR='${amlibexecdir}/application'
    AC_DEFINE_DIR([APPLICATION_DIR],[APPLICATION_DIR],
           [Directory in which dumper interfaces should be installed and searched. ])

    AC_ARG_WITH(amdatadir,
	AS_HELP_STRING([--with-amdatadir[[[[[=PATH]]]]]],
		[Where amanda's templates and examples are installed; default: $datadir/amanda]),
	[
	    AMDATADIR=$withval
	], [
	    AMDATADIR=$datadir/amanda
	]
    )
    AC_DEFINE_DIR([amdatadir], [AMDATADIR],
	[Directory in which amanda's templates and examples are installed. ])
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
    echo "  Application: $APPLICATION_DIR"
    echo "  Configuration: $CONFIG_DIR"
    echo "  GNU Tar lists: $GNUTAR_LISTED_INCREMENTAL_DIR"
    echo "  Perl modules (amperldir): $amperldir"
    echo "  Template and example data files (amdatadir): $amdatadir"
    echo "  Temporary: $AMANDA_TMPDIR"
])
