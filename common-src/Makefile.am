# Makefile for Amanda library.

include $(top_srcdir)/config/automake/vars.am
include $(top_srcdir)/config/automake/scripts.am
include $(top_srcdir)/config/automake/installperms.am
include $(top_srcdir)/config/automake/precompile.am

INCLUDES =      -I$(top_srcdir)/gnulib   

AM_CFLAGS = $(AMANDA_WARNING_CFLAGS)

amlib_LTLIBRARIES =	libamanda.la

LINT=$(AMLINT)
LINTFLAGS=$(AMLINTFLAGS)

libamanda_la_SOURCES =		\
	alloc.c			\
	amfeatures.c		\
	amflock.c		\
	clock.c			\
	columnar.c		\
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
	../gnulib/libgnu.la

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
	columnar.h		\
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

EXTRA_PROGRAMS = genversion $(TEST_PROGS)

# Version-building steps:
#
# 1. genversion.c gets subversion information via svn keywords
# 2. Makefile creates genversion.h with build-time information
# 3. Makefile builds genversion
# 4. Makefile runs genversion to create version.c
# 5. version.c is compiled into libamanda like any other C file

genversion_SOURCES = genversion.c genversion.h
genversion_LDADD = $(libamanda_la_LIBADD)	\
    	versuff.lo				\
	../gnulib/libgnu.la

genversion.h: $(top_builddir)/config.status
	-rm -f $@ $@.new
	echo '#define CC "$(CC)"' > $@.new
	echo '#define BUILT_DATE "'`date`'"' >> $@.new
	echo '#define BUILT_MACH "$(target)"' >> $@.new
	mv $@.new $@

version.c: genversion$(EXEEXT)
	-rm -f version.c
	./genversion$(EXEEXT) > version.c

BUILT_SOURCES = genversion.h version.c

# these are used for testing only:
TEST_PROGS = token file bsdsecurity amfeatures

CLEANFILES += *.test.c
DISTCLEANFILES += version.c genversion genversion.h

# used for testing only

STANDARD_COMMON_STUFF_NOT_FILE = \
	alloc.lo \
	clock.lo \
	debug.lo \
	util.lo \
	match.lo \
	sl.lo

STANDARD_COMMON_STUFF = \
	$(STANDARD_COMMON_STUFF_NOT_FILE) \
	file.lo

token_SOURCES = token.test.c
token_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF)

file_SOURCES = file.test.c
file_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF_NOT_FILE)

bsdsecurity_SOURCES = bsd-security.test.c
bsdsecurity_LDADD = $(libamanda_a_LIBADD) \
		    alloc.lo \
		    clock.lo \
		    debug.lo \
		    dgram.lo \
		    event.lo \
		    file.lo \
		    packet.lo \
		    security.lo \
		    ssh-security.lo \
		    versuff.lo

amfeatures_SOURCES = amfeatures.test.c
amfeatures_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF)

# automake-style tests

TESTS = amflock-test
noinst_PROGRAMS = $(TESTS)

amflock_test_SOURCES = amflock-test.c
amflock_test_LDADD = libamanda.la

# scripts

# divide scripts up both by language and by destination directory.
sbin_SCRIPTS_PERL =

sbin_SCRIPTS_SHELL = \
	    amcrypt \
	    amaespipe \
	    amcrypt-ossl \
	    amcrypt-ossl-asym

libexec_SCRIPTS_PERL = 

libexec_SCRIPTS_SHELL = \
	    amanda-sh-lib.sh

SCRIPTS_PERL = $(sbin_SCRIPTS_PERL) $(libexec_SCRIPTS_PERL)
SCRIPTS_SHELL = $(sbin_SCRIPTS_SHELL) $(libexec_SCRIPTS_SHELL)
libexec_SCRIPTS = $(libexec_SCRIPTS_PERL) $(libexec_SCRIPTS_SHELL)
sbin_SCRIPTS = $(sbin_SCRIPTS_PERL) $(sbin_SCRIPTS_SHELL)

# installation

INSTALLPERMS_data = dest=$(sbindir) chown=amanda $(sbin_SCRIPTS)

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
