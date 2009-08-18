# Makefile for Amanda library.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =      -I$(top_srcdir)/gnulib \
		-I$(top_srcdir)/device-src \
		-I$(top_srcdir)/common-src

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

amlib_LTLIBRARIES =	libamxfer.la

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

libamxfer_la_SOURCES = \
	dest-fd.c \
	dest-null.c \
	element-glue.c \
	filter-xor.c \
	filter-process.c \
	source-random.c \
	source-fd.c \
	source-pattern.c \
	xfer-element.c \
	xfer.c \
	xmsg.c

libamxfer_la_LDFLAGS = -release $(VERSION)
libamxfer_la_LIBADD = \
	../common-src/libamanda.la

noinst_HEADERS = \
	amxfer.h \
	element-glue.h \
	xfer-element.h \
	xfer.h \
	xmsg.h

# automake-style tests

TESTS = xfer-test
noinst_PROGRAMS = $(TESTS)

xfer_test_SOURCES = xfer-test.c
xfer_test_LDADD = \
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
