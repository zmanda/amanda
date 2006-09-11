# Makefile for Amanda file recovery programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/client-src

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

LIB_EXTENSION = la

sbin_PROGRAMS =		amrecover

if WANT_RUNTIME_PSEUDO_RELOC
AM_LDFLAGS = -Wl,-enable-runtime-pseudo-reloc
endif

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	@LEXLIB@ \
	../client-src/libamclient.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION) \
	$(READLINE_LIBS)

amrecover_CSRC =	amrecover.c					\
			display_commands.c		extract_list.c	\
			help.c				set_commands.c

amrecover_SOURCES =	$(amrecover_CSRC)	uparse.y	uscan.l

noinst_HEADERS = 	amrecover.h uparse.h

AM_YFLAGS =		-d

# so that uscan.c is never generated before uparse.h
# otherwise we might have makedepend problems
$(srcdir)/uscan.c: $(srcdir)/uparse.h

uscan.$(OBJEXT): $(srcdir)/uscan.c
	$(CC) $(CFLAGS) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(CPPFLAGS) -c $<

uparse.$(OBJEXT): $(srcdir)/uparse.c
	$(CC) $(CFLAGS) $(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) $(CPPFLAGS) -c $<


install-exec-hook:
	@list="$(sbin_PROGRAMS)"; \
	for p in $$list; do \
		pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
		echo chown $(BINARY_OWNER) $$pa; \
		chown $(BINARY_OWNER) $$pa; \
		echo chgrp $(SETUID_GROUP) $$pa; \
		chgrp $(SETUID_GROUP) $$pa; \
		echo chmod o-rwx $$pa; \
		chmod o-rwx $$pa; \
	done


lint:
	@f="$(amrecover_CSRC)";							\
	(cd ../common-src; make listlibsrc);					\
	f="$$f "`cat ../common-src/listlibsrc.output`;				\
	(cd ../server-src; make listlibsrc);					\
	f="$$f "`cat ../server-src/listlibsrc.output`;				\
	echo $(LINT) $$f;							\
	$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I../config $(INCLUDES) $$f;\
	if [ $$? -ne 0 ]; then							\
	    exit 1;								\
	fi;									\
        exit 0

