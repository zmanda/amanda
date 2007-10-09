# Makefile for Amanda library.

INCLUDES =      -I$(top_srcdir)/gnulib   

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)

lib_LTLIBRARIES =	libamanda.la

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@
EXTRA_DIST =

LIB_EXTENSION = la

SCRIPTS_PERL =
SCRIPTS_SHELL =	amcrypt amaespipe	\
	amcrypt-ossl	amcrypt-ossl-asym
sbin_SCRIPTS = $(SCRIPTS_PERL) $(SCRIPTS_SHELL)


libamanda_la_SOURCES =		\
	alloc.c			\
	amfeatures.c		\
	amflock.c		\
	clock.c			\
	conffile.c		\
	debug.c			\
	dgram.c			\
	event.c			\
	file.c			\
	fileheader.c		\
	glib-util.c		\
	match.c			\
	packet.c		\
	pipespawn.c		\
	protocol.c		\
	security.c		\
	security-util.c		\
	sl.c			\
	sockaddr-util.c		\
	stream.c		\
	tapelist.c		\
	timestamp.c		\
	token.c			\
	util.c			\
	versuff.c
# version.c is generated; see below
nodist_libamanda_la_SOURCES = version.c

libamanda_la_LIBADD =		\
	../gnulib/libgnu.$(LIB_EXTENSION)

if WANT_AMFLOCK_POSIX
libamanda_la_SOURCES += amflock-posix.c
endif

if WANT_AMFLOCK_FLOCK
libamanda_la_SOURCES += amflock-flock.c
endif

if WANT_AMFLOCK_LOCKF
libamanda_la_SOURCES += amflock-lockf.c
endif

if WANT_AMFLOCK_LNLOCK
libamanda_la_SOURCES += amflock-lnlock.c
endif


if WANT_RSH_SECURITY
libamanda_la_SOURCES += rsh-security.c
endif
if WANT_SSH_SECURITY
libamanda_la_SOURCES += ssh-security.c
endif
if WANT_BSD_SECURITY
libamanda_la_SOURCES += bsd-security.c
endif
if WANT_BSDTCP_SECURITY
libamanda_la_SOURCES += bsdtcp-security.c
endif
if WANT_BSDUDP_SECURITY
libamanda_la_SOURCES += bsdudp-security.c
endif
if WANT_KRB4_SECURITY
libamanda_la_SOURCES += krb4-security.c	
endif
if WANT_KRB5_SECURITY
libamanda_la_SOURCES += krb5-security.c
endif

libamanda_la_LDFLAGS =  -release $(VERSION)

noinst_HEADERS =		\
	amanda.h		\
	amfeatures.h		\
	arglist.h		\
	clock.h			\
	amflock.h		\
	conffile.h		\
	debug.h			\
	dgram.h			\
	event.h			\
	file.h			\
	fileheader.h		\
	glib-util.h		\
	packet.h		\
	pipespawn.h		\
	protocol.h		\
	queue.h			\
	security.h		\
	security-util.h		\
	sl.h			\
	sockaddr-util.h		\
	stream.h		\
	tapelist.h		\
	timestamp.h		\
	token.h			\
	util.h			\
	version.h

SUFFIXES =		.sh .pl

.pl:
	cat $< > $@
	chmod a+x $@

.sh:
	cat $< > $@
	chmod a+x $@

EXTRA_PROGRAMS = genversion $(TEST_PROGS)

genversion_SOURCES = genversion.c
genversion_LDADD = $(libamanda_la_LIBADD)	\
    	versuff.lo				\
	../gnulib/libgnu.$(LIB_EXTENSION)

genversion.@OBJEXT@: genversion.h
genversion.h: $(top_builddir)/config.status
	-rm -f $@ $@.new
	echo '#define CC "$(CC)"' > $@.new
	echo '#define BUILT_DATE "'`date`'"' >> $@.new
	echo '#define BUILT_MACH "'`uname -a || echo UNKNOWN HOST`'"' >> $@.new
	updir=`cd .. && pwd` ; \
	if svn info "$$updir" > $@.svn; then \
		rev=`grep Revision: $@.svn|cut -d: -f 2|cut -c2-`; \
		echo '#define BUILT_REV "'$$rev'"' >> $@.new; \
		url=`grep URL: $@.svn|cut -d: -f 2-|cut -c2-`; \
		echo '#define BUILT_BRANCH "'`basename "$$url"`'"' >> $@.new;  \
	fi
	-rm $@.svn
	mv $@.new $@

version.c:	genversion$(EXEEXT)
	-rm -f version.c
	./genversion > version.c

# these are used for testing only:
TEST_PROGS = token file bsdsecurity amfeatures

CLEANFILES = *.test.c

DISTCLEANFILES = $(sbin_SCRIPTS) version.c genversion.h genversion amanda-int.h

# used for testing only

STANDARD_COMMON_STUFF_NOT_FILE = \
	alloc.$(OBJEXT) \
	clock.$(OBJEXT) \
	debug.$(OBJEXT) \
	util.$(OBJEXT) \
	match.$(OBJEXT) \
	sl.$(OBJEXT)

STANDARD_COMMON_STUFF = \
	$(STANDARD_COMMON_STUFF_NOT_FILE) \
	file.$(OBJEXT)

token_SOURCES = token.test.c
token_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF)

file_SOURCES = file.test.c
file_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF_NOT_FILE)

bsdsecurity_SOURCES = bsd-security.test.c
bsdsecurity_LDADD = $(libamanda_a_LIBADD) \
		    alloc.$(OBJEXT) \
		    clock.$(OBJEXT) \
		    debug.$(OBJEXT) \
		    dgram.$(OBJEXT) \
		    event.$(OBJEXT) \
		    file.$(OBJEXT) \
		    packet.$(OBJEXT) \
		    security.$(OBJEXT) \
		    ssh-security.$(OBJEXT) \
		    versuff.$(OBJEXT)

amfeatures_SOURCES = amfeatures.test.c
amfeatures_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF)

# automake-style tests

TESTS = amflock-test
noinst_PROGRAMS = $(TESTS)

amflock_test_SOURCES = amflock-test.c
amflock_test_LDADD = libamanda.la

include $(top_srcdir)/config/automake/check-perl.am
CHECK_PERL=$(SCRIPTS_PERL)
check-local: check-perl

include $(top_srcdir)/config/automake/check-shell.am
CHECK_SHELL=$(SCRIPTS_SHELL)
check-local: check-shell

# shell library

libexec_SCRIPTS = amanda-sh-lib.sh

# installation

install-exec-hook:
	@list="$(sbin_SCRIPTS)"; \
	for p in $$list; do \
	pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
	echo chown \"$(BINARY_OWNER)\" $$pa;    \
	chown "$(BINARY_OWNER)" $$pa;           \
	echo chgrp \"$(SETUID_GROUP)\" $$pa;    \
	chgrp "$(SETUID_GROUP)" $$pa;           \
	done


lint:
	@echo $(LINT) $(libamanda_la_SOURCES)
	@$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config $(INCLUDES) $(libamanda_la_SOURCES)
	@echo $(LINT) $(genversion_SOURCES)
	@$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I$(top_builddir)/config $(INCLUDES) $(genversion_SOURCES)

listlibsrc:
	@ for p in $(libamanda_la_SOURCES) $(REGEXCSRC); do	\
		listlibsrcs="$$listlibsrcs `pwd`/$$p";		\
	done;							\
	echo $$listlibsrcs >listlibsrc.output

%.test.c: $(srcdir)/%.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@
