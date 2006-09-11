# Makefile for Amanda tape library.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

lib_LTLIBRARIES = 	libamtape.la
LIB_EXTENSION = la

sbin_PROGRAMS=          ammt amdd amtapetype

libamtape_la_SOURCES = 	output-file.c \
			output-null.c \
			output-rait.c \
			output-tape.c \
			tapeio.c

libamtape_la_LDFLAGS =  -release $(VERSION)

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD =	../common-src/libamanda.$(LIB_EXTENSION) \
	libamtape.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION)

# used for testing only

TEST_PROGS =		amtapeio

EXTRA_PROGRAMS = 	$(TEST_PROGS)

CLEANFILES = *.test.c

amtapetype_SOURCES = tapetype.c

noinst_HEADERS = 	\
			output-file.h \
			output-null.h \
			output-rait.h \
			output-tape.h \
			tapeio.h

install-exec-hook:
	@list="$(sbin_PROGRAMS) $(sbin_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done
	@list="$(libexec_PROGRAMS) $(libexec_SCRIPTS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(libexecdir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
	done

amtapeio_SOURCES = amtapeio.test.c
amtapeio_LDADD =	../common-src/libamanda.$(LIB_EXTENSION) \
			libamtape.$(LIB_EXTENSION) \
			../common-src/libamanda.$(LIB_EXTENSION)

amtapeio.test.c: $(srcdir)/tapeio.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@

%.test.c: $(srcdir)/%.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@

tapetype:
	@echo "Use amtapetype instead"

lint:
	@ for p in $(sbin_PROGRAMS); do						\
		if [ $$p = "amtapetype" ]; then					\
			s="$(amtapetype_SOURCES)";				\
		else								\
			s=$$p.c;						\
		fi; 								\
		f="$$s $(libamandad_la_SOURCES)";				\
		f="$$f "`(cd ../common-src; make listlibsrc 2>&1 > /dev/null)`;	\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
		echo $(LINT) $$f;						\
		$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I../config	\
		    $(INCLUDES) $$f;						\
		if [ $$? -ne 0 ]; then						\
		    exit 1;							\
		fi;								\
	done;									\
        exit 0

listlibsrc:
	@ for p in $(libamtape_la_SOURCES); do	\
		listlibsrcs="$$listlibsrcs `pwd`/$$p";		\
	done;							\
	echo $$listlibsrcs > listlibsrc.output
