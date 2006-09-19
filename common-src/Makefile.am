# Makefile for Amanda library.

lib_LTLIBRARIES =	libamanda.la

REGDIR = ../regex-src
REGsrcdir = $(srcdir)/$(REGDIR)

INCLUDES = -I$(REGsrcdir)

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

libamanda_la_SOURCES =		\
	alloc.c			\
	amfeatures.c		\
	amflock.c		\
	bsd-security.c		\
	bsdtcp-security.c	\
	bsdudp-security.c	\
	clock.c			\
	conffile.c		\
	debug.c			\
	dgram.c			\
	error.c			\
	event.c			\
	file.c			\
	fileheader.c		\
	krb4-security.c		\
	krb5-security.c		\
	match.c			\
	packet.c		\
	pipespawn.c		\
	protocol.c		\
	regcomp.c		\
	regerror.c		\
	regexec.c		\
	regfree.c		\
	rsh-security.c		\
	security.c		\
	security-util.c		\
	sl.c			\
	ssh-security.c		\
	statfs.c		\
	stream.c		\
	tapelist.c		\
	token.c			\
	util.c			\
	version.c		\
	versuff.c

libamanda_la_LIBADD =	@LTLIBOBJS@ @LTALLOCA@
libamanda_la_LDFLAGS =  -release $(VERSION)

noinst_HEADERS =		\
	amanda.h		\
	amfeatures.h		\
	amregex.h		\
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
	stream.h		\
	tapelist.h		\
	token.h			\
	util.h			\
	version.h

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

match.@OBJEXT@ match.lo: regex.h
regcomp.@OBJEXT@ regcomp.lo: regex.h regcomp.ih
regexec.@OBJEXT@ regexec.lo: regex.h engine.ih
regerror.@OBJEXT@ regerror.lo: regex.h regerror.ih
regfree.@OBJEXT@ regfree.lo: regex.h

REGEXCSRC =	$(REGsrcdir)/regcomp.c	\
		$(REGsrcdir)/regexec.c	\
		$(REGsrcdir)/regerror.c	\
		$(REGsrcdir)/regfree.c

REGEXHSRC =	$(REGsrcdir)/regex2.h	\
		$(REGEXCSRC)

# these are used for testing only:
TEST_PROGS = statfs token file bsdsecurity amfeatures

CLEANFILES = regex.h regcomp.ih engine.ih regerror.ih *.test.c

DISTCLEANFILES = version.c genversion.h genversion amanda-int.h

regex.h: $(REGEXHSRC) $(REGsrcdir)/mkh
	sh $(REGsrcdir)/mkh -i _REGEX_H_ $(REGEXHSRC) >$@

regcomp.ih: $(REGsrcdir)/regcomp.c $(REGsrcdir)/mkh
	sh $(REGsrcdir)/mkh -p $(REGsrcdir)/regcomp.c >$@

engine.ih: $(REGsrcdir)/engine.c $(REGsrcdir)/mkh
	sh $(REGsrcdir)/mkh -p $(REGsrcdir)/engine.c >$@

regerror.ih: $(REGsrcdir)/regerror.c $(REGsrcdir)/mkh
	sh $(REGsrcdir)/mkh -p $(REGsrcdir)/regerror.c >$@

# used for testing only

STANDARD_COMMON_STUFF_NOT_FILE = \
	alloc.$(OBJEXT) \
	clock.$(OBJEXT) \
	debug.$(OBJEXT) \
	error.$(OBJEXT) \
	util.$(OBJEXT) \
	match.$(OBJEXT)

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
