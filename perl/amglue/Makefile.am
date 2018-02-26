include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am

##
## libamglue -- helper functions for gluing amanda to perl
##

# add the SWIG symbol to avoid conflicts with perl definitions.
# $AMANDA_WARNING_CFLAGS is omitted because SWIG-generated files tend
# to trigger warnings.
AM_CFLAGS = -DSWIG $(AMANDA_SWIG_PERL_CFLAGS)

# Appropriate INCLUDES depends on which components (server or client) are
# being built
AM_CPPFLAGS =  \
            -I$(top_builddir)/common-src \
            -I$(top_srcdir)/common-src \
            -I$(top_srcdir)/xfer-src \
            -I$(top_srcdir)/gnulib \
            -I$(top_srcdir)/ndmp-src \
            $(PERL_INC)

amlib_LTLIBRARIES = libamglue.la
libamglue_la_SOURCES = \
	ghashtable.c \
	gerror.c \
	bigint.c \
	source.c \
	objwrap.c \
	xferwrap.c \
	amglue.h
libamglue_la_LDFLAGS = $(PERL_EXT_LDFLAGS) -release $(VERSION)
libamglue_la_LIBADD = \
	$(top_builddir)/xfer-src/libamxfer.la
EXTRA_DIST += $(libamglue_la_SOURCES)
