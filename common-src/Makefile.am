# Makefile for Amanda library.

INCLUDES =      -I$(top_srcdir)/gnulib   

lib_LTLIBRARIES =	libamanda.la

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

sbin_SCRIPTS =	amcrypt amaespipe	\
	amcrypt-ossl	amcrypt-ossl-asym


libamanda_la_SOURCES =		\
	alloc.c			\
	amfeatures.c		\
	amflock.c		\
	clock.c			\
	conffile.c		\
	debug.c			\
	dgram.c			\
	error.c			\
	event.c			\
	file.c			\
	fileheader.c		\
	match.c			\
	packet.c		\
	pipespawn.c		\
	protocol.c		\
	security.c		\
	security-util.c		\
	sl.c			\
	sockaddr-util.c		\
	statfs.c		\
	stream.c		\
	tapelist.c		\
	token.c			\
	util.c			\
	version.c		\
	versuff.c

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

#libamanda_la_LIBADD =	@LTLIBOBJS@

libamanda_la_LDFLAGS =  -release $(VERSION)

noinst_HEADERS =		\
	amanda.h		\
	amfeatures.h		\
	arglist.h		\
	clock.h			\
	conffile.h		\
	dgram.h			\
	event.h			\
	fileheader.h		\
	packet.h		\
	pipespawn.h		\
	protocol.h		\
	queue.h			\
	security.h		\
	security-util.h		\
	sl.h			\
	statfs.h		\
	sockaddr-util.h		\
	stream.h		\
	tapelist.h		\
	token.h			\
	util.h			\
	version.h

SUFFIXES =		.sh .pl

.pl:
	cat $< > $@
	chmod a+x $@
	-test -z "$(PERL)" || $(PERL) -c $@

.sh:
	cat $< > $@
	chmod a+x $@

EXTRA_PROGRAMS = genversion $(TEST_PROGS)

genversion_SOURCES = genversion.c
genversion_LDADD = $(libamanda_la_LIBADD) versuff.lo

genversion.@OBJEXT@: genversion.h
genversion.h: $(top_builddir)/config.status
	-rm -f $@ $@.new
	echo '#define CC "$(CC)"' > $@.new
	echo '#define BUILT_DATE "'`date`'"' >> $@.new
	echo '#define BUILT_MACH "'`uname -a || echo UNKNOWN HOST`'"' >> $@.new
	mv $@.new $@

version.c:	genversion$(EXEEXT)
	-rm -f version.c
	./genversion > version.c

# these are used for testing only:
TEST_PROGS = statfs token file bsdsecurity amfeatures

CLEANFILES = *.test.c

DISTCLEANFILES = version.c genversion.h genversion amanda-int.h

# used for testing only

STANDARD_COMMON_STUFF_NOT_FILE = \
	alloc.$(OBJEXT) \
	clock.$(OBJEXT) \
	debug.$(OBJEXT) \
	error.$(OBJEXT) \
	util.$(OBJEXT) \
	match.$(OBJEXT) \
	sl.$(OBJEXT)

STANDARD_COMMON_STUFF = \
	$(STANDARD_COMMON_STUFF_NOT_FILE) \
	file.$(OBJEXT)

statfs_SOURCES = statfs.test.c
statfs_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF)

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
		    error.$(OBJEXT) \
		    event.$(OBJEXT) \
		    file.$(OBJEXT) \
		    packet.$(OBJEXT) \
		    security.$(OBJEXT) \
		    ssh-security.$(OBJEXT) \
		    versuff.$(OBJEXT)

amfeatures_SOURCES = amfeatures.test.c
amfeatures_LDADD = $(libamanda_la_LIBADD) $(STANDARD_COMMON_STUFF)

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
	@$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I../config $(INCLUDES) $(libamanda_la_SOURCES)
	@echo $(LINT) $(genversion_SOURCES)
	@$(LINT) $(LINTFLAGS) $(CPPFLAGS) $(DEFS) -I. -I../config $(INCLUDES) $(genversion_SOURCES)

listlibsrc:
	@ for p in $(libamanda_la_SOURCES) $(REGEXCSRC); do	\
		listlibsrcs="$$listlibsrcs `pwd`/$$p";		\
	done;							\
	echo $$listlibsrcs >listlibsrc.output

%.test.c: $(srcdir)/%.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@
