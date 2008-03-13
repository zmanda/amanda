# Makefile for Amanda library.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =      -I$(top_srcdir)/gnulib \
		-I$(top_srcdir)/common-src

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

amlib_LTLIBRARIES =	libamxfer.la

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

libamxfer_la_SOURCES = \
	dest-null.c \
	filter-xor.c \
	source-random.c \
	xfer-element.c \
	xfer.c \
	xmsg.c

noinst_HEADERS = \
	amxfer.h \
	xfer-element.h \
	xfer.h \
	xmsg.h

# automake-style tests

TESTS = xfer-test
noinst_PROGRAMS = $(TESTS)

xfer_test_SOURCES = xfer-test.c
xfer_test_LDADD = \
	../common-src/libamanda.la \
	../common-src/libtestutils.la \
	libamxfer.la

# lint support

lint:
	@echo $(LINT) $(libamxfer_la_SOURCES)
	@$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config $(INCLUDES) $(libamxfer_la_SOURCES)
	@echo $(LINT) $(genversion_SOURCES)
	@$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config $(INCLUDES) $(genversion_SOURCES)

listlibsrc:
	@ for p in $(libamxfer_la_SOURCES); do	\
		listlibsrcs="$$listlibsrcs `pwd`/$$p";		\
	done;							\
	echo $$listlibsrcs >listlibsrc.output
