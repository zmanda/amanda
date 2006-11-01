# Makefile for Amanda client programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src \
		-I$(top_srcdir)/amandad-src

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

lib_LTLIBRARIES =	libamclient.la
LIB_EXTENSION = la

libexec_PROGRAMS =	noop calcsize killpgrp rundump runtar selfcheck sendbackup sendsize versionsuffix

sbin_SCRIPTS =		@CLIENT_SCRIPTS_OPT@

libexec_SCRIPTS =	patch-system

if WANT_SAMBA
samba_sources = findpass.c
endif

if WANT_RUNTIME_PSEUDO_RELOC
AM_LDFLAGS = -Wl,-enable-runtime-pseudo-reloc
endif

libamclient_la_SOURCES=	amandates.c		getfsent.c	\
			unctime.c		client_util.c	\
			$(samba_sources)

libamclient_la_LDFLAGS = -release $(VERSION)

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	libamclient.$(LIB_EXTENSION) \
	../amandad-src/libamandad.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION)

SUFFIXES =		.sh .pl

.pl:
			cat $< > $@
			chmod a+x $@
			-test -z "$(PERL)" || $(PERL) -c $@

.sh:
			cat $< > $@
			chmod a+x $@

# these are used for testing only:
TEST_PROGS = getfsent

EXTRA_PROGRAMS =	$(TEST_PROGS)

CLEANFILES = *.test.c patch-system

EXTRA_SCRIPTS =		amhpfixdevs		amsinixfixdevs

DISTCLEANFILES = 	$(EXTRA_SCRIPTS)

EXTRA_DIST =		amhpfixdevs.sh		amsinixfixdevs.sh

sendbackup_SOURCES = 	sendbackup.c		sendbackup.h	  \
			sendbackup-dump.c	sendbackup-gnutar.c

noinst_HEADERS	= 	amandates.h	getfsent.h	\
			findpass.h	client_util.h
			

install-exec-hook:
	@list="$(sbin_SCRIPTS)"; \
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
if WANT_SETUID_CLIENT
	@list="calcsize killpgrp rundump runtar"; \
	for p in $$list; do \
		if echo "$(libexec_PROGRAMS)" | grep $$p >/dev/null 2>&1; then \
			pa=$(DESTDIR)$(libexecdir)/`echo $$p|sed 's/$(EXEEXT)$$//'|sed '$(transform)'|sed 's/$$/$(EXEEXT)/'`; \
			echo chown root $$pa; \
			chown root $$pa; \
			echo chmod u+s,o-rwx $$pa; \
			chmod u+s,o-rwx $$pa; \
		else true; \
		fi; \
	done
endif

lint:
	@ for p in $(libexec_PROGRAMS); do					\
		f="$$p.c $(libamclient_la_SOURCES)";				\
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

getfsent_SOURCES = getfsent.test.c

%.test.c: $(srcdir)/%.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@
