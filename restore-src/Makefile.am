# Makefile for Amanda restore programs.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/device-src   \
		-I$(top_srcdir)/server-src   \
		-I$(top_srcdir)/amandad-src  \
		-I$(top_srcdir)/gnulib

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)
AM_LDFLAGS = $(AMANDA_STATIC_LDFLAGS)

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

amlib_LTLIBRARIES =	librestore.la
LIB_EXTENSION = la

sbin_PROGRAMS = 	amrestore amfetchdump

amlibexec_PROGRAMS = 	amidxtaped

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = librestore.$(LIB_EXTENSION)              \
      ../common-src/libamanda.$(LIB_EXTENSION) \
      $(READLINE_LIBS)

amidxtaped_LDADD = $(LDADD) \
		   ../gnulib/libgnu.$(LIB_EXTENSION) \
		   ../amandad-src/libamandad.$(LIB_EXTENSION)

amidxtaped_SOURCES = 	amidxtaped.c

amfetchdump_SOURCES = 	amfetchdump.c

librestore_la_SOURCES	=	restore.c
librestore_la_LDFLAGS	=	-release $(VERSION)
librestore_la_LIBADD	=	../common-src/libamanda.$(LIB_EXTENSION) \
				../server-src/libamserver.$(LIB_EXTENSION) \
				../device-src/libamdevice.$(LIB_EXTENSION)

noinst_HEADERS = restore.h

INSTALLPERMS_exec = \
	dest=$(sbindir) chown=amanda $(sbin_PROGRAMS) \
	dest=$(amlibexecdir) chown=amanda $(amlibexec_PROGRAMS)

lint:
	@ for p in $(amlibexec_PROGRAMS) $(sbin_PROGRAMS); do			\
		p=`basename $$p $(EXEEXT)`;					\
		f="$$p.c $(librestore_la_SOURCES)";				\
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

