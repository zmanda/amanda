# Makefile for Amanda client programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

lib_LTLIBRARIES =       libamandad.la
LIB_EXTENSION = la

libexec_PROGRAMS =	amandad

if WANT_RUNTIME_PSEUDO_RELOC
AM_LDFLAGS = -Wl,-enable-runtime-pseudo-reloc
endif

libamandad_la_SOURCES= amandad_util.c
libamandad_la_LDFLAGS = -release $(VERSION)

noinst_HEADERS  = amandad.h

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	libamandad.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION)

install-exec-hook:
	@list="$(libexec_PROGRAMS) $(libexec_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(libexecdir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done

lint:
	@ for p in $(libexec_PROGRAMS); do					\
		f="$$p.c $(libamandad_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		(cd ../server-src; make listlibsrc);				\
		f="$$f "`cat ../server-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I../config	\
		    $(INCLUDES) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0

