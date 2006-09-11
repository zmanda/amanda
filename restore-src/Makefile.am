# Makefile for Amanda restore programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/tape-src     \
		-I$(top_srcdir)/server-src   \
		-I$(top_srcdir)/amandad-src

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

lib_LTLIBRARIES =	librestore.la
LIB_EXTENSION = la

sbin_PROGRAMS = 	amrestore amfetchdump

libexec_PROGRAMS = 	amidxtaped

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = librestore.$(LIB_EXTENSION)              \
      ../common-src/libamanda.$(LIB_EXTENSION) \
      $(READLINE_LIBS)

amidxtaped_LDADD = $(LDADD) ../amandad-src/libamandad.$(LIB_EXTENSION)
amidxtaped_SOURCES = 	amidxtaped.c

amfetchdump_SOURCES = 	amfetchdump.c

librestore_la_SOURCES	=	restore.c
librestore_la_LDFLAGS	=	-release $(VERSION)
librestore_la_LIBADD	=	../common-src/libamanda.$(LIB_EXTENSION) \
                   ../tape-src/libamtape.$(LIB_EXTENSION) \
                  ../server-src/libamserver.$(LIB_EXTENSION)

noinst_HEADERS = restore.h

install-exec-hook:
	@list="$(sbin_PROGRAMS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done
	@list="$(libexec_PROGRAMS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(libexecdir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done

lint:
	@ for p in $(libexec_PROGRAMS) $(sbin_PROGRAMS); do			\
		f="$$p.c $(librestore_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		(cd ../server-src; make listlibsrc);				\
		f="$$f "`cat ../server-src/listlibsrc.output`;			\
		(cd ../tape-src; make listlibsrc);				\
		f="$$f "`cat ../tape-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I../config	\
		    $(INCLUDES) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0

