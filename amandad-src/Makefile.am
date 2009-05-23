# Makefile for Amanda client programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

amlib_LTLIBRARIES =       libamandad.la

amlibexec_PROGRAMS =	amandad

libamandad_la_SOURCES= amandad_util.c
libamandad_la_LDFLAGS = -release $(VERSION)
libamandad_la_LIBADD = ../common-src/libamanda.la

noinst_HEADERS  = amandad.h

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.la \
	libamandad.la \
	../common-src/libamanda.la \
	../gnulib/libgnu.la

INSTALLPERMS_exec = \
	dest=$(amlibexecdir) chown=amanda \
		$(amlibexec_PROGRAMS)

INSTALLPERMS_data = \
	dest=$(amlibexecdir) chown=amanda \
		$(amlibexec_SCRIPTS)

lint:
	@ for p in $(amlibexec_PROGRAMS); do					\
		p=`basename $$p $(EXEEXT)`;					\
		f="$$p.c $(libamandad_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		(cd ../server-src; make listlibsrc);				\
		f="$$f "`cat ../server-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config	\
		    $(INCLUDES) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0

