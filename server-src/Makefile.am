# Makefile for Amanda server programs.

INCLUDES =	-I$(top_builddir)/common-src \
		-I$(top_srcdir)/common-src   \
		-I$(top_srcdir)/restore-src  \
		-I$(top_srcdir)/tape-src     \
		-I$(top_srcdir)/amandad-src

LINT=@AMLINT@
LINTFLAGS=@AMLINTFLAGS@

lib_LTLIBRARIES = 	libamserver.la
LIB_EXTENSION = la

sbin_PROGRAMS =		amadmin		amcheck		amflush		\
			amgetconf	amlabel		amtape		\
			amreport

libexec_PROGRAMS =	amindexd	amlogroll	amtrmidx	\
			amtrmlog	driver		dumper		\
			planner		taper		amcleanupdisk	\
			chunker

sbin_SCRIPTS =		amcheckdb	amcleanup	amdump		\
			amoverview	amrmtape	amtoc		\
			amverify	amverifyrun	amstatus	\
			amcrypt		amaespipe       amcrypt-ossl    \
			amcrypt-ossl-asym

libamserver_la_SOURCES=	amindex.c	changer.c			\
			diskfile.c	driverio.c	\
			holding.c	infofile.c	logfile.c	\
			tapefile.c	find.c		server_util.c   \
                        taperscan.c

libamserver_la_LDFLAGS= -release $(VERSION)

###
# Because libamanda includes routines (e.g. regex) provided by some system
# libraries, and because of the way libtool sets up the command line, we
# need to list libamanda twice here, first to override the system library
# routines, and second to pick up any references in the other libraries.
###

LDADD = ../common-src/libamanda.$(LIB_EXTENSION)   \
	libamserver.$(LIB_EXTENSION)               \
	../tape-src/libamtape.$(LIB_EXTENSION)     \
	../common-src/libamanda.$(LIB_EXTENSION)   \
	../tape-src/libamtape.$(LIB_EXTENSION)     \
	../common-src/libamanda.$(LIB_EXTENSION)   \
        $(READLINE_LIBS)

amindexd_LDADD = ../common-src/libamanda.$(LIB_EXTENSION) \
	libamserver.$(LIB_EXTENSION) \
	../amandad-src/libamandad.$(LIB_EXTENSION) \
	../tape-src/libamtape.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION) \
	../tape-src/libamtape.$(LIB_EXTENSION) \
	../common-src/libamanda.$(LIB_EXTENSION)

SUFFIXES =		.sh .pl

.pl:
			cat $< > $@
			chmod a+x $@
			-test -z "$(PERL)" || $(PERL) -c $@

.sh:
			cat $< > $@
			chmod a+x $@

# there are used for testing only:
TEST_PROGS = diskfile infofile

EXTRA_PROGRAMS =	$(TEST_PROGS)

CLEANFILES = *.test.c $(sbin_SCRIPTS)

amindexd_CSRC =		amindexd.c	disk_history.c	list_dir.c
amindexd_SOURCES =	disk_history.h	list_dir.h	$(amindexd_CSRC)

amreport_SOURCES =	reporter.c

amgetconf_SOURCES =	getconf.c

noinst_HEADERS = 	amindex.h	changer.h			\
			diskfile.h	driverio.h	\
			holding.h	infofile.h	logfile.h	\
			tapefile.h	find.h		server_util.h	\
			taperscan.h

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
	@list="amcheck"; \
	for p in $$list; do \
		if echo "$(sbin_PROGRAMS)" | grep $$p >/dev/null 2>&1; then \
			pa=$(DESTDIR)$(sbindir)/`echo $$p|sed '$(transform)'`; \
			echo chown root $$pa; \
			chown root $$pa; \
			echo chmod u+s,o-rwx $$pa; \
			chmod u+s,o-rwx $$pa; \
		else true; \
		fi; \
	done
##						       ##
## enterprise version will install dumper/planner suid ##
##						       ##

	@list="dumper planner"; \
	for p in $$list; do \
		if echo "$(libexec_PROGRAMS)" | grep $$p >/dev/null 2>&1; then \
			pa=$(DESTDIR)$(libexecdir)/`echo $$p|sed '$(transform)'`; \
			echo chown root $$pa; \
			chown root $$pa; \
			echo chmod u+s,o-rwx $$pa; \
			chmod u+s,o-rwx $$pa; \
		else true; \
		fi; \
	done

lint:
	@ for p in $(libexec_PROGRAMS) $(sbin_PROGRAMS); do			\
		if [ $$p = "amindexd" ]; then					\
			s="$(amindexd_CSRC)";					\
		elif [ $$p = "amreport" ]; then					\
			s="$(amreport_SOURCES)";				\
		elif [ $$p = "amgetconf" ]; then				\
			s="$(getconf_SOURCES)";					\
		else								\
			s=$$p.c;						\
		fi; 								\
		f="$$s $(libamserver_la_SOURCES)";				\
		(cd ../common-src; make listlibsrc);				\
		f="$$f "`cat ../common-src/listlibsrc.output`;			\
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

listlibsrc:
	@ for p in $(libamserver_la_SOURCES); do		\
		listlibsrcs="$$listlibsrcs `pwd`/$$p";		\
	done;							\
	echo $$listlibsrcs >listlibsrc.output


diskfile_SOURCES = diskfile.test.c
infofile_SOURCES = infofile.test.c

%.test.c: $(srcdir)/%.c
	echo '#define TEST' >$@
	echo '#include "$<"' >>$@
